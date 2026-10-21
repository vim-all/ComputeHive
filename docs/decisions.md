# Decision log

A running record of the important decisions made while taking ComputeHive from prototype to a
provable distributed task runner: what was decided, the alternatives, and why. It is updated at
every phase boundary and whenever a significant choice is made. Newest decisions are appended to
their phase; corrections are marked **Revised**, never silently rewritten.

Format: **Decision** / **Alternatives** / **Why** / **Evidence** (the test or command that backs it).

---

## Process decisions

### D-01: Audit first, fix discrepancies in code, never in wording
- **Decision:** Wrote `docs/AUDIT.md` before changing anything; every gap between the resume claims and the
  code became a work item, tracked in its table.
- **Why:** The resume claims must be provable. Softening the claims would hide the gaps instead of closing them.
- **Evidence:** `docs/AUDIT.md` sections 2 and 7.

### D-02: One phase at a time, each ending green and committed
- **Decision:** Vet, staticcheck and `go test -race` must pass before a phase is committed, and the next phase starts from green.
- **Why:** Your rules of engagement. It also keeps `git bisect` useful.

### D-03: Measure the "authoritative" fact on the server, not on the observer
- **Decision:** Exactly-once is audited from a `commits` counter that the Lua `complete` script increments inside Redis,
  not from fake-worker ledgers.
- **Alternatives:** Count accepted submissions in the test workers.
- **Why:** A worker SIGKILLed after the coordinator committed never sees the reply, so a worker-side ledger under-counts and
  produced false failures. The server's own counter is what actually proves the property: a double commit would show `commits=2`.
- **Evidence:** `auditCommits` in `coordinator/internal/chaos/harness_test.go`.

### D-04: Treat an intermittent test failure as a bug until explained
- **Decision:** When `TestRedisRestartKeepsState` failed once in a full run, I looped it until it reproduced instead of retrying.
- **Why:** It was a real bug (D-12), not a flake. Retrying would have hidden a hole in the zero-loss claim.

---

## Phase 1: foundation and one-command bring-up

### D-05: Self-contained artifact store in the coordinator
- **Decision:** The coordinator serves `PUT/GET /artifacts/<sha256>` (content-addressed, size-capped, hash-verified on upload).
  Jobs reference `computehive-artifact://<sha256>`, which each worker resolves against its own coordinator address.
- **Alternatives:** Require S3/R2 (the original design); MinIO in compose; ship the image tar through gRPC.
- **Why:** A self-hosted cluster should run a job with zero cloud credentials. A relative reference means the submitter doesn't
  need to know the worker's network (compose, LAN, NAT). `https://` S3/R2 URLs still work for the desktop client.
- **Evidence:** `coordinator/internal/httpapi`, `worker/internal/artifact/resolve.go`, `make demo`.

### D-06: The CLI is a separate Go module sharing the generated protobuf code
- **Decision:** `cli/` is its own module (joined by `go.work`), importing `coordinator/pkg/pb` through a `replace`, same as the worker.
- **Alternatives:** Put the CLI in the coordinator module; duplicate the proto code.
- **Why:** It uses the same gRPC API as the Tauri client with no copies, and doesn't pull coordinator internals.

### D-07: Redis in compose is published on an ephemeral loopback port
- **Decision:** `127.0.0.1::6379` rather than a fixed 6379/6380.
- **Why:** Both fixed ports were already in use on the development machine (a local Valkey and another project's container).
  Tests and tools discover the port with `docker compose port`.

### D-08: Pin staticcheck to v0.8.2 and run it in CI
- **Why:** The older pinned release could not read Go 1.27 export data; v0.8.2 works with both local Go and CI's Go 1.25. It also
  found real issues (dead `resp.go`, deprecated gRPC dial) that were fixed rather than suppressed.

### D-09: `make chaos` and `make bench` fail loudly until implemented
- **Why:** A target that silently succeeds would imply coverage that doesn't exist.

---

## Phase 2: scheduler correctness

### D-10: Every state transition is one atomic Lua script over a purpose-built schema
- **Decision:** New `coordinator/internal/queue` package. Pending jobs live in a ZSET scored by a monotonically increasing sequence
  number; each job is a hash; leases and workers have their own indexes. The old `LPOP`-then-write store and the dead round-robin
  scheduler were deleted.
- **Alternatives:** Keep the list queue and patch it; use Redis Streams with consumer groups.
- **Why:** The original claim script popped a job atomically but the follow-up writes were separate, so a crash between them could
  strand a job, and a requeue went to the tail (`RPUSH`), losing FIFO position. A ZSET keyed by the original sequence number gives
  FIFO *and* lets a requeued job keep its place. Streams would have given at-least-once delivery but not our eligibility rules
  (overload, requirements, capacity) inside the same atomic step.
- **Evidence:** `TestConcurrentClaimNeverDoubleAssigns` (3000 jobs, 48 concurrent workers, 0 double assignments),
  `TestRequeueKeepsFIFOPositionAndCountsAttempt`.

### D-11: Time comes from Redis `TIME` inside the scripts
- **Why:** One clock authority, so several coordinator replicas with skewed clocks agree on lease and heartbeat deadlines.
- **Trade-off:** Scripts that call `TIME` and write are non-deterministic; this is fine on Redis 5+ (effects replication) but
  means we depend on a single-node Redis. Redis Cluster would require hash tags (documented in `scripts.go`).

### D-12: Lease tokens and attempt numbers fence every result
- **Decision:** A claim returns a fresh random token. `complete`, `fail` and `mark_running` only act if the token matches the job's
  current one; otherwise the result is `stale` (discarded). A terminal job answers `duplicate` to everything, making submission idempotent.
- **Why:** This is what stops a presumed-dead worker from corrupting state when it wakes up.
- **Evidence:** `TestLateResultFromDeadWorkerIsDiscarded`, `TestPartitionedWorkerNeverCommitsOverRequeuedJob` (3 scenarios).

### D-13: Heartbeat reconciliation of orphaned leases (found by chaos testing)
- **Problem found:** After `SIGKILL` of Redis, one job stayed `ASSIGNED` for 90s+. The atomic claim had been persisted but its reply to
  the worker was lost, so the job was leased to a *live* worker that didn't know it had it. The dead-worker reaper never fires for a
  live worker and the lease deadline is 30 minutes.
- **Decision:** Each heartbeat reports the jobs the worker is running. A lease the coordinator holds for that worker but the worker
  does not report, older than `OrphanGrace`, is requeued **without burning an attempt**.
- **Alternatives:** Make claim two-phase with an explicit ack (more round trips and a new in-between state); shorten lease deadlines
  (would kill legitimately long jobs).
- **Why the grace:** a claim reply may simply still be in flight; `OrphanGrace` is at least 5 heartbeat intervals.
- **Evidence:** `TestLostClaimReplyIsReconciledByHeartbeat`; the Redis-restart chaos test went from failing intermittently to 25/25 passes.

### D-14: Which failures are retried
- **Decision:** `job_exit` and `timeout` are the job's own fault: terminal `FAILED`, never retried. `infra` is retried and counts an
  attempt. `evicted` (worker shed load or is shutting down) is retried and does **not** count. After `max_attempts` a job is `DEAD`.
- **Alternatives:** Retry every failure.
- **Why:** Retrying a deterministic crash wastes the cluster and delays the user's real error. Infrastructure and eviction problems
  say nothing about the job.

### D-15: Failure-detection timings
- **Decision:** Heartbeat every 2s, worker declared dead after 6s of silence (must be at least 2x the interval; enforced at startup),
  reaper every 1s, retry backoff 1s doubling to 30s.
- **Why:** Worst case from last heartbeat to requeue is about 7s, leaving margin under the 15s target.
- **Measured (Docker, this machine):** kill -> requeued 5.6-6.7s, kill -> completed on another worker 6.9-8.1s (3 iterations).

### D-16: Workers keep a stable identity
- **Decision:** `RegisterWorker` accepts the worker's configured ID and keeps it across restarts and reconnects; the old behaviour
  (a new random UUID on every NotFound) orphaned its jobs.
- **Trade-off:** Two machines with the same ID would collide. Per-worker credentials in the security phase will bind an ID to a key.

### D-17: Coordinator is stateless; startup recovery is just the first reaper tick
- **Evidence:** `TestCoordinatorCrashAndRestartLosesNothing` (killed mid-run for 1.5s, 300/300 jobs complete exactly once).

### D-18: A worker returns its job on shutdown, and the executor kills the container by name
- **Why:** Cancelling a context only kills the `docker run` client, not the container, so timeouts and cancellations leaked running
  containers. On SIGTERM the worker now hands the job back as `evicted` instead of making the coordinator wait out the dead window.

### D-19: Test infrastructure uses real Redis in Docker, with in-process fake workers
- **Alternatives:** An in-memory fake Redis (miniredis).
- **Why:** The guarantees live in Lua and in Redis persistence semantics; a fake would test our assumptions about Redis, not Redis.
  Fake workers speak the real gRPC protocol so the real handlers are exercised. Tests skip (not fail) if Docker is unavailable.

### D-20: Test timing must not depend on a healthy machine
- **Problem:** `TestCompleteVersusReapRace` failed about 1 run in 18 even with a 1s liveness window, because the test Redis fsyncs
  every write (`appendfsync always`) and the dev machine stalls for seconds under load.
- **Decision:** Tests that need a worker to be "dead" backdate its heartbeat in Redis instead of sleeping, and the shared test
  `DeadAfter` was widened. Production timings are unaffected.
- **Why:** A test that is only reliable on an idle machine proves nothing in CI. 40/40 consecutive passes after the change.

### D-21: Two independent chaos layers
- **Decision:** (1) in-process suite: real Redis in Docker + real gRPC coordinator + fake workers that can be killed or partitioned;
  (2) `scripts/chaos-cluster.sh`: `docker kill` of real worker containers in the compose cluster, using the real CLI.
- **Why:** Layer 1 is fast and deterministic enough for CI and can do things Docker can't (partition a worker that keeps working,
  restart the coordinator on the same port). Layer 2 proves the same behaviour with the real binaries and real containers.
- **Measured:** layer 1 time-to-requeue 5.6-6.7s; layer 2 (3 real kills) 4.7-5.9s. Both under the 15s target.

### D-22: In the cluster test the restart policy is disabled before the kill
- **Why:** `restart: unless-stopped` brings a killed worker back in about a second, which would turn a node failure into a blip and
  make the measurement meaningless. The script runs `docker update --restart=no` first and restores it afterwards.

### D-23: Missing lease token is rejected, not defaulted
- **Decision:** `SubmitResult` without `lease_token` returns InvalidArgument. Old workers must be upgraded.
- **Alternatives:** Accept token-less results for backward compatibility.
- **Why:** Accepting them would reopen the exact hole leases close. The *wire* format stays compatible (new fields only);
  the *behaviour* is intentionally stricter.

### Phase 2 outcome
Verified: 0 double assignments (3000 jobs, 48 concurrent claimers); requeue keeps FIFO position and counts attempts; late results
from dead workers are discarded; coordinator crash/restart, Redis SIGKILL with AOF, partition (3 scenarios), and 1000 jobs with
63 random worker kills all end with every job committed exactly once (server-side audit).
Not done in Phase 2: per-user concurrency limits and aging (Phase 5); graceful drain of in-flight jobs (Phase 6).

---

## Phase 3: resource monitoring and the 90% rule

### D-24: The rule is enforced in two independent places, and the coordinator does not trust the worker's flag
- **Decision:** (a) the worker stops calling `RequestJob` while overloaded; (b) the coordinator re-derives overload from the raw
  numbers in every heartbeat, inside the atomic heartbeat script, and the claim script refuses overloaded, stale or metric-less workers.
- **Alternatives:** Trust the worker's `overloaded` boolean; enforce only on the worker.
- **Why:** The task requires that "a misbehaving worker can't bypass it". A worker that lies about its flag still reports the
  numbers (or reports none, and then gets no work: `REQUIRE_USAGE`). Omitting metrics also erases the stored ones so old healthy
  values cannot be replayed.
- **Evidence:** `TestCoordinatorDerivesOverloadFromMetricsNotFromTheWorkersFlag`, `TestWorkersWithoutMetricsAreRefusedWhenRequired`
  (coordinator side); `TestWorkerStopsRequestingJobsAtThresholdAndResumesAfterHysteresis` (worker side);
  `scripts/pressure-cluster.sh` (real cluster: the overloaded worker got 0 of 6 jobs).

### D-25: Hysteresis: enter at >= 90%, leave only after N seconds below one band under each limit
- **Decision:** Enter the moment any of RAM/CPU/disk reaches its limit (inclusive). Leave only after *every* metric has stayed
  below `limit - band` for `RELEASE_AFTER` (10s). Readings between the two thresholds change nothing, and a bounce back into the
  band restarts the timer.
- **Revised during testing:** I first required every metric below one global `RELEASE_PERCENT`. A real-memory test failed to recover:
  a disk at 85% (limit raised to 100%) blocked recovery forever although it could never have tripped anything. Now each metric's
  release level is its own limit minus the band, where band = lowest limit - release percent. With the 90/80 defaults that is exactly
  "below 80%" for all three, so the specified behaviour is unchanged.
- **Evidence:** `TestEntersAtLimitAndOnlyLeavesAfterSustainedRecovery`, `TestBouncingBack...`, `TestMetricFarFromItsOwnLimitDoesNotBlockRecovery`,
  and the real-container test below.

### D-26: Sampling details
- RAM "used" is total minus *available* (reclaimable page cache does not count), the same figure the kernel uses to decide to OOM-kill.
- CPU is averaged over 3 samples so one spike cannot trip the 90% limit; RAM and disk are instantaneous.
- Disk is the worse of the job work directory and Docker's data root.
- Docker health is a hard condition without hysteresis (no Docker, no jobs).
- If sampling fails 3 times in a row, or has never succeeded, the worker fails safe (overloaded). Nothing is assumed healthy.
- Temperature, GPU and network egress are reported but are not admission limits.

### D-27: Memory headroom is reserved for the host OS at assignment time
- **Decision:** The claim script refuses a job whose declared memory (default 512 MB if undeclared) would push projected usage to the
  limit: `used + (memory promised to running jobs but not yet used) + this job >= 90%`.
- **Why the middle term:** a job that has been placed but has not ramped up yet is invisible in host usage; ignoring it lets two jobs
  be placed into the same free memory.
- **Evidence:** `TestMemoryHeadroomIsReservedForTheHostOS`, `TestHeadroomCountsMemoryPromisedToRunningJobsNotYetUsed`.

### D-28: Over-pressure policy
1. Stop accepting new work (immediate). 2. If still over a limit after `PRESSURE_GRACE` (15s) while a job runs, stop that job
gracefully (SIGTERM, then SIGKILL after `STOP_GRACE`), report it `evicted`, and log `event=job_evicted_pressure`.
- **Why `evicted` is separate from failure:** it requeues immediately and does *not* burn an attempt; the job did nothing wrong.
- **Limit of the design:** with one job per worker "lowest priority / most recent" is trivially that job; the selection function
  needs revisiting if `max_jobs > 1` is ever enabled. The pressure may have been caused by something other than the job.
- **Evidence:** `TestPressureEvictsRunningJobAfterGraceAndRequeuesItWithoutBlame` (not evicted before the grace, evicted exactly once after).

### D-29: Hard limits on every container, always
- **Decision:** `--memory` and `--memory-swap` equal (no swap), `--cpus`, `--pids-limit`, with defaults (512 MB, 1 CPU, 256 PIDs) when a
  job declares nothing, so no container is ever unbounded.
- **Evidence (real Docker):** the running container reports Memory = MemorySwap = 64 MB, PidsLimit 32, NanoCpus 0.5, network none;
  an over-allocating job is OOM-killed (exit 137) and classed as the job's fault; a fork bomb got 30 of 400 processes.

### D-30: Real bug found by the real-container tests: `--rm` strands "Dead" containers
- **Problem:** Cancelling a job ran `docker stop` and then `docker rm -f`, racing with `--rm`'s own removal. Docker left the
  container in the `Dead` state, which holds disk and cannot be restarted.
- **Decision:** Drop `--rm`; the executor removes every container itself, deterministically, and a startup sweep removes containers
  left by a crashed worker (only this worker's, by label).
- **Why it matters for the project:** "no leaks, no zombie containers" is a contributor-safety requirement (Phase 4).

### D-31: Verifying against the real kernel, not only fakes
- Fake metrics prove the state machine; they cannot prove the sampler reads the right thing. So there is also a test where a real
  container fills tmpfs memory: RAM went from 69.0% to 76.8% against a 73% limit, the monitor tripped, and 2s after the memory was
  freed the worker resumed. Thresholds in that test are relative to the measured baseline so it is valid on any machine.

### Phase 3 outcome
Verified: unit (monitor, agent, queue, config), real containers (limits, OOM, fork bomb, timeout, orphan sweep, real memory pressure),
and the running cluster (`make pressure`). Not done: per-job disk quota and ulimits (Phase 4 sandboxing); GPU admission limits;
`max_jobs > 1`. The macOS memory path of the old reporter is no longer used by the monitor (gopsutil handles it) but was not tested.
