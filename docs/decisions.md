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

---

## Phase 4: sandboxing and trust for contributor mode

### D-32: Threat model first; every mitigation names the test that proves it
- `docs/THREAT_MODEL.md` was written before the implementation (assets, six attackers, trust boundaries, mitigations with
  evidence, residual risks). Anything not demonstrated is listed as residual rather than claimed.

### D-33: Defence in depth at the point of `docker run`
- **Decision:** (1) strict builder: non-root user, `--cap-drop ALL`, `no-new-privileges`, `--read-only`, noexec `/tmp` tmpfs,
  `--init`, ulimits (`nofile`, `core=0`, `fsize`), memory=swap, PIDs, CPUs, `--network none`, `--pull never`; (2) an **argument
  audit** that parses the final flags with a whitelist and refuses anything else before Docker runs (`--privileged`, `--cap-add`,
  `--device`, host namespaces, extra mounts, the Docker socket, unconfined seccomp/AppArmor, root); (3) worker-side
  re-validation of image refs, env names and command, because the coordinator may be compromised.
- **Why an audit as well as a careful builder:** the builder is the thing most likely to gain a bug later; the audit is a
  second, simpler implementation of the invariant. 30+ unsafe variants are tested to be refused.
- **Evidence:** `TestAuditRefusesEveryUnsafeVariant`, `TestHostile` (real containers).

### D-34: The hostile suite tests behaviour, and it found problems in my own attack scripts
- Real malicious scripts (network, host files, writes outside the sandbox, privilege escalation, Docker socket, metadata, fork
  bomb, memory, CPU spin, disk fill, log spam) run through the production executor; each prints BLOCKED/ESCAPED and the host is
  checked afterwards (no containers left, secret file untouched, Docker alive, memory and disk deltas small).
- Several early "failures" were bugs in the attacks, not escapes (grep walking `/proc`, a `nc` listener that never exits,
  `/proc/kcore` being a writable mask over `/dev/null`). Each was fixed by making the *attack* meaningful, never by weakening the
  assertion. Measured: fork bomb 29 of 500 processes; infinite loop killed at 3.3 s; 4 busy threads held to 0.48 of 0.5 CPU;
  OOM-kill exit 137; disk fill stopped at 119-125 MB against a 50 MB cap.

### D-35: Disk quota by watcher, not filesystem (an honest limitation)
- Docker cannot size-limit a bind mount without root-level filesystem features, so a watcher polls the work directory and
  kills the job; `RLIMIT_FSIZE` caps single files. Overshoot is measured (about 2.4x at memory speed) and listed as a residual
  risk, with the 90% host-disk rule as a second line.

### D-36: Uploaded image archives cannot retag the contributor's images
- `docker load` honours whatever tags a tar carries. The worker reads `manifest.json` first and refuses any tag outside
  `computehive-job:` / `computehive/`. The worker also runs Docker with an **empty `DOCKER_CONFIG`** (so stored registry logins are
  never used for a stranger) and `--pull never`. Images that are not shipped as artifacts need an explicit contributor allow list.

### D-37: Network egress redesigned after a real-Docker test disproved the first design
- **First design:** proxy listening on the per-job network's gateway address. **Test result:** Docker `--internal` networks cannot
  reach the gateway, so nothing worked (the test failed for every check).
- **Shipped design:** per-job proxy **sidecar** (the worker's own static binary mounted read-only into an empty image, non-root,
  all caps dropped, read-only, small limits) attached to the internal network and to the default bridge. The job can reach only the
  sidecar. This also removed the earlier residual risk (host services on wildcard addresses): measured unreachable.
- **Proxy rules:** names only (never IP literals), resolve itself, refuse private/loopback/link-local/CGNAT/NAT64/reserved
  after resolution, dial the validated IP (no rebinding), try each validated address (a v6-first answer on a v4-only bridge),
  ports 80/443 only, concurrency bounded.
- **Also fixed here:** my CONNECT probes in the test used `nc` that exited before reading the reply, which would have made the
  denial checks pass vacuously; the allowed tunnel now has to succeed to prove the probe works.

### D-38: gVisor is implemented and surfaced, but NOT verified here
- Auto-detected via `docker info`, `--runtime runsc`, isolation level reported in heartbeats and enforceable by both submitter
  (`--isolation gvisor`) and contributor (`require_gvisor`). No `runsc` exists on the development machine, so no container ever ran
  under it; documented in `docs/GVISOR.md`.

### D-39: The contributor, not the cluster, has the last word
- A local policy (`policy.json`, 0600) is enforced **on the worker**: donated CPU/RAM/disk, schedule window, idle-only, pause,
  kill switch, network allow-list, image allow/deny, gVisor requirement. The agent re-checks every job against it, so a
  compromised coordinator cannot override it; refused jobs are handed back as `evicted` (not the job's fault). The control API is a
  Unix socket (dir 0700, socket 0600). History records what ran (image, command, env **names only**, outcome).
- Policy updates are validated and applied atomically; unknown fields are rejected (a typo must not leave a protection off).

### D-40: Secure by default, with explicit, loud development opt-outs
- The coordinator refuses to start without an API token and TLS (`AUTH_DISABLED` and `ALLOW_INSECURE` exist for development and
  log warnings). `make up` generates a random API token, a private CA and server certificate under `.computehive/`, so the
  five-minute quickstart is still two commands.
- **Enrollment:** operator mints an expiring, limited-use token; a worker presents it once and receives a per-worker credential
  (hashed in Redis, revocable). A token holder **cannot take over an existing identity**; revoked ids cannot re-enrol; revocation
  requeues the worker's jobs without burning attempts.
- **TLS 1.3**, verified server name; **mTLS** supported and tested (client cert from another CA is refused).
- **Rate limiting** runs on source address before any secret is examined (throttles token guessing), then per worker / per token.

### D-41: Output files are returned through the coordinator, not only S3
- Found by the first verified job: any worker without an S3 bucket failed every job that wrote a file (A22). Outputs are now
  uploaded to the coordinator by the lease holder only (re-checked after the upload), SHA-256 verified, stored per attempt, and
  downloaded with `result --download` (hash re-verified, hostile file names rejected). Retention/cleanup is not implemented yet.

### D-42: Redundant execution ("verify"), simple and honest
- **Decision:** a verified job is two replicas created atomically; the claim script never gives both to one worker; each replica
  waits in `VERIFYING` until the other finishes; equal output digests (exit code + full stdout + every output file) -> both
  `SUCCEEDED`, otherwise both `FAILED (verification_mismatch)`. A replica whose peer fails, is cancelled, dies or never runs (for
  example a one-worker cluster) fails with a clear reason; a timeout (default 10 min) stops it waiting forever.
- **Tradeoff (documented):** 2x cost; only for deterministic jobs (a non-deterministic job mismatches - measured with `examples/hello`);
  detects disagreement but cannot say which worker lied; two colluding workers can agree on a wrong answer.
- **Evidence:** 6 queue tests plus the real cluster run (`verified: match` for `examples/deterministic`).

### Phase 4 outcome
Verified: hostile suite on real containers, argument audit, image policy, auth/TLS/mTLS/rate-limit/revocation end to end, contributor
controls, network allow-list on real Docker, verification, and the secured cluster (`make up`, `demo`, `chaos`, `pressure`).
Not verified: gVisor; Windows/macOS Docker Desktop behaviour; network mode when the worker itself runs in a container (unsupported).
Not done: output retention; per-user quotas (Phase 5); the Tauri client's contributor UI (Phase 7).

---

## Phase 5: best-fit worker selection

### D-43: Keep pull-based workers; make the coordinator decide whether *this* asker is the best fit
- **Options:** (a) push assignment (the coordinator picks a worker and sends the job); (b) keep workers pulling but let the coordinator
  withhold a job from a worker that is not its best fit; (c) leave claim first-come-first-served and just add filters.
- **Decision:** (b). Workers keep calling `RequestJob`, which works behind NAT with outbound-only connections and needs no new
  coordinator-to-worker channel; the coordinator evaluates all workers and hands the job over only to the best idle fit.
- **Tradeoff:** a job can wait for its best worker instead of starting immediately on a mediocre one; bounded by the patience window (D-46).
  Rejected (a): needs inbound connectivity to workers. Rejected (c): filters alone cannot express "prefer the emptier machine".

### D-44: The scheduler is pure logic; the Lua claim stays the safety net
- `internal/scheduler` does no I/O, so every decision is reproducible from a snapshot and unit-testable in microseconds.
- The `claim` script re-checks overload, capacity, requirements, isolation, headroom and the per-user limit atomically, so a stale
  snapshot (up to 250 ms) can only make a placement worse, never violate a rule. This keeps the no-double-assignment proof from Phase 2 intact.

### D-45: A transparent weighted score, deterministic, stored with the job
- Eight components, each 0..1 times a published weight (free RAM, reliability, free CPU, load, locality, fit, uptime, preference).
  The breakdown is stored on the job at assignment time and shown by `computehive explain`; a waiting job is evaluated live.
- **Why not a learned or opaque model:** nothing to train on yet, and an unexplainable placement is a support burden. Weights are hand-set
  and visible so they can be tuned later against measurements.
- **Bug found by the tests:** summing the score components by iterating a Go map made identical workers differ in the last bit
  (float addition is not associative), which broke the deterministic tie-break. Components are now summed in a fixed order.

### D-46: Patience instead of reservations
- A job is held for its best idle worker; after `SCHED_PATIENCE` (5 s) any eligible idle worker may take it. No reservation state to
  leak when a worker vanishes, and starvation is bounded. Patience relaxes the preference only, never a filter (tested with an
  overloaded worker and an infinite wait).

### D-47: Reliability is a decayed, smoothed success rate with a conservative blacklist
- `(ok + 2) / (ok + fail + 4)` with a 1-hour half-life: new workers start at 0.5, old failures fade, and blacklisting needs both
  reliability < 0.2 and at least 5 recent samples, so one bad job cannot exile a worker.
- **Known side effect (documented):** a worker with more history scores higher and wins ties (observed 0.88 vs 0.50 in the live cluster).

### D-48: Artifact locality needs byte-identical artifacts, so the CLI caches the saved image by directory hash
- **Finding (live):** the CLI tagged each build with a timestamp, so every submit produced a different tarball and a worker's cache could
  never match. Tagging by image ID did not help either: this Docker embeds the build time in the image, so two builds of identical
  sources differ (verified by diffing the blobs).
- **Decision:** cache the saved tarball by a hash of the job directory (paths, modes, contents; `.git` skipped). An unchanged directory
  reuses the exact tarball (no rebuild, same digest); `--rebuild` forces a build. Workers keep verified artifacts in an LRU cache keyed
  by SHA-256, re-hash on every read, and advertise digests for the locality score.
- **Tradeoff:** a changed base image is not noticed until `--rebuild`. Measured: repeat artifact ready in 4 ms from the worker cache.

### D-49: Job requirements from a tiny manifest; unknown keys are errors
- `computehive.yaml` is flat `key: value` (no YAML dependency) and Dockerfile `LABEL computehive.<key>=` is accepted too. Precedence: flags,
  manifest, Dockerfile labels, defaults. A typo (`mem_mb`) is a hard error rather than a job silently running with the wrong resources.

### D-50: Defects Phase 5 surfaced (kept here because they are the lesson)
1. **Claim script read job fields at the wrong HMGET indexes** (an off-by-one after adding fields), which broke OS/arch/label/disk constraints
   (a Lua error) and made the per-user limit treat the user name as a required label. The unit tests of the scripts did not exercise those paths;
   the gRPC-level heterogeneous-cluster test on a real Redis did. Fixed, and that test now guards it.
2. **Timestamped image tags made caching impossible** (D-48), found by looking at `explain` on the live cluster, not by any test.
3. **A heartbeat edit was lost** (the worker did not send OS/arch/cached digests) and nothing failed: the fields silently stayed empty.
   Found only because the live `explain` showed `locality 0.000`. Gap: no automated test covers the worker-to-coordinator plumbing of these
   fields; the live check is the evidence.

### Phase 5 outcome
Verified: scheduler filters and scoring (table-driven, deterministic over 20 runs under `-race`); heterogeneous cluster on a real Redis
(GPU job never lands on a CPU-only worker, overloaded workers never chosen even after waiting, patience releases aged jobs, OS/arch/labels/disk/
isolation, per-user limit); artifact cache (LRU, tamper detection); manifest parsing; full `make lint` and `make test` including the chaos suite;
live cluster: `explain` with score breakdown, locality 0.100 on the worker holding the artifact.
Measured: scheduler cost 5.2 us / 60 us / 0.77 ms per decision at 10 / 100 / 1000 workers (pure CPU, i7-13620H).
Not verified: behaviour with more than 2 live workers on real containers (end-to-end scheduling latency is Phase 8); weights are not tuned.

---

## Phase 6: observability and operations

### D-51: Metrics from two sources: Redis at scrape time, events as they happen
- **Options:** (a) maintain gauges incrementally in memory; (b) read cluster state from Redis on every scrape; (c) a separate exporter process.
- **Decision:** both (b) and event counters. Queue depth, jobs by state and per-worker utilisation are read from Redis when scraped, so they
  cannot drift from the data (an in-memory gauge would be wrong after a coordinator restart or with two coordinators). Durations and counts
  (time in queue, runtime, requeues, RPCs) can only be observed as they happen, so those are recorded inline.
- **Tradeoff:** each scrape costs a few Redis reads (bounded by a 3 s timeout; failure exports `computehive_redis_up 0` instead of stale data).
  Per-worker series are capped at 200 to bound label cardinality. Rejected (c): another process to deploy and secure for no gain.
- All metric methods are nil-safe, so tests and code that do not care about metrics need no plumbing.

### D-52: `/metrics` is authenticated and TLS-only, like everything else
- It lists worker ids and utilisation, so it requires the API token (not the worker credential) and shares the source-address rate limit.
  Prometheus presents the token from a mounted file and verifies the coordinator against the cluster CA.
- **Consequence:** Prometheus must read a 0600 file, so it runs as the host user and keeps its TSDB on a tmpfs: metrics history is not persisted in
  Compose (documented). A deployment beyond a laptop should use a service account and a volume.

### D-53: Alerts describe symptoms and carry the first action
- Twelve rules (down, no workers, worker lost, heartbeat late, RAM/disk at 90%, overloaded for 10 min, queue stuck, dead-letter growing, requeue
  rate, slow scheduling). `QueueStuck` fires only when the head job is old **and healthy workers exist**, because "queue is long" while the cluster
  is saturated is normal, but "old job, idle workers" means nobody can run it, and the annotation points at `computehive explain`.
- Validated with `promtool`; one was fired for real by killing a worker. No Alertmanager is configured, so alerts are visible in Prometheus and
  Grafana but not delivered anywhere.

### D-54: One dashboard, generated from a short script, verified by rendering
- Grafana JSON is verbose, so `deploy/grafana/dashboard.py` is the source and the JSON is generated. It is provisioned (nothing to import) and doubles
  as the "live cluster view" (a one-row-per-worker table plus the 90% rule per worker), which avoids building a second UI.
- **Verification:** every panel query was run against Prometheus (31 of 34 returned data; the 3 empty ones are failure-only panels that stayed empty
  until a failure happened), then the page was rendered in a browser. That caught a real defect the queries could not: the table was cluttered with
  `__name__/instance/job` columns (fixed with `max by (worker)`).
- **Bug found live:** pinning the datasource UID made Grafana refuse to start on an existing stack (it cannot change a provisioned datasource's UID).
  The provisioning file now deletes the datasource by name first, so upgrades work without wiping Grafana.

### D-55: Correlation by request id and job id; JSON logs everywhere
- An outermost gRPC interceptor assigns or accepts an `x-request-id`, returns it in the response, records RPC metrics and writes one structured line
  per call with `method`, `code`, `duration_ms`, `job_id`, `worker_id`. A caller-supplied id is accepted only if it matches `[A-Za-z0-9._-]{1,64}`, so a client cannot
  inject newlines or fake log lines (tested). Polling is logged at debug, otherwise an idle cluster writes a line per worker per 2 s.
- The worker now logs JSON by default (`LOG_FORMAT=text` for terminals) with `component` and `worker_id` on every line, and `slog.SetDefault` so package-level logs match.
- **Not done:** distributed tracing. Correlation is by ids in logs.

### D-56: Graceful drain, with a second signal as the escape hatch
- **Before:** SIGTERM cancelled the agent immediately: a running job was killed and handed back, even if seconds from finishing.
- **Now:** SIGTERM starts a drain. The worker stops pulling, heartbeats `draining` (so the coordinator stops assigning), lets the current job finish for up to
  `DRAIN_TIMEOUT` (30 s), then exits; on timeout the job is handed back as `evicted` (no attempt burned). A second signal terminates immediately; the contributor
  kill switch is still immediate. The coordinator reports not-ready for `SHUTDOWN_GRACE` (3 s) while still serving, then drains in-flight RPCs (forced stop after 15 s).
- **Why:** a rolling restart or `docker stop` should not waste work. Compose `stop_grace_period` is raised (30 s/45 s) because Docker's 10 s default would SIGKILL a draining worker.
- **Measured live:** a 15 s job survived a SIGTERM to its worker and finished on attempt 1 of 3; `/readyz` went 200, 503 for about 3 s, then closed.

### D-57: Honest limits in the metrics
- `jobs_killed_total{kind="kill"}` is exit codes 137/143: it includes OOM, disk and PID quota kills and a manual kill. The worker reports only four failure reasons,
  so a precise sandbox-kill reason would need a protocol addition. Documented as an approximation rather than presented as exact.
- Overload transitions are detected in coordinator memory, so a restart can double-count one.

### Phase 6 outcome
Verified: metrics content against a real Redis (exact values), the authenticated endpoint, nil-safety, overload-transition counting, correlation-id handling including hostile ids,
worker drain (finish, no more work, timeout hand-back, idle exit; run 3x under `-race`), `make lint` and `make test` (chaos 145 s), alert rules with `promtool`, live: Prometheus scraping
over TLS with the token, 12 rules loaded, `WorkerLost` fired by a real kill, Grafana datasource and dashboard provisioned and rendered, SIGTERM drain of a worker and of the coordinator.
Not verified: behaviour with more than two workers; alert delivery (no Alertmanager); metrics persistence across Prometheus restarts.
