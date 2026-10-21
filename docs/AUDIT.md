# ComputeHive audit (baseline at commit 6b618d2)

This is a snapshot of the repo *before* the hardening work. It exists so every later change can be
checked against what was actually there. Discrepancies are fixed in code, never in wording.
Status of each finding is tracked in the "Resolved in" column and updated as phases land.

## 1. What exists

| Component | Path | State |
|---|---|---|
| Coordinator (Go, gRPC :50051) | `coordinator/` | Builds and vets clean. No tests. Redis via go-redis v9. |
| Worker (Go) | `worker/` | Builds. Unit tests pass for artifact, config, executor args and output manager. No tests for agent, store, resources. |
| Desktop client (Rust/Tauri + TS) | `client/` | Builds a Docker image from a project dir, uploads artifact to S3/R2, submits jobs, polls results, "contributor mode" spawns a local worker. Not exercised headless. |
| Protos | `shared/proto/` | `WorkerService` (RegisterWorker, Heartbeat, RequestJob, SubmitResult) and `ClientService` (SubmitJob, GetJobStatus, GetJobResult). Generated Go code is checked in under `coordinator/pkg/pb`. |
| Compose / Make / CI / CLI | - | `docker-compose.yml` is empty. No Makefile, no CI, no CLI. |

Module wiring: the worker imports the coordinator module's generated `pb` via `replace coordinator => ../coordinator`.
The coordinator module is named plain `coordinator`, which cannot be imported from outside the repo.

## 2. Resume claims vs. code

### 2.1 "Redis-backed FIFO queues with atomic Lua job assignment (no double-assignment)"
- **Exists:** `ClaimNextQueuedJobForWorker` (`coordinator/internal/store/redis.go:119`) runs a Lua script that does
  `LPOP job_queue` and `SET` of status and worker key in one step. That part is atomic: two concurrent callers cannot get the same ID from LPOP.
- **Gaps:**
  - The script does not check worker liveness, capacity or overload; `RequestJob` checks liveness *before* the script, a TOCTOU gap.
  - After the script, `RequestJob` (`handlers.go:103-117`) performs non-atomic writes (`GetJob`, `SaveJob`). A crash between the
    script and `SaveJob` leaves the job popped from the queue with status `running`, owned by a worker that may never have received it.
  - Fallback `RequeueJobID` uses `RPUSH`, so a requeued job loses its FIFO position.
  - There is no sequence number, attempt counter or lease, and no test of any of it.
- **Verdict:** atomic pop exists; "no double-assignment" is plausible but **unproven**, and "FIFO" is not preserved on requeue.

### 2.2 "Heartbeat-based failure detection with automatic requeue in under 15 seconds"
- **Exists:** worker liveness is a `worker_alive:<id>` key with a hard-coded 10s TTL set on register and heartbeat
  (`handlers.go:31,57`). A monitor goroutine runs every 3s (`server.go:42`) and requeues jobs of workers whose key expired.
- **Gaps:**
  - Worst case is TTL 10s + tick 3s = 13s, which is under 15s only by accident. Neither value is configurable and no test or script measured it.
  - `JobsForWorker` uses `SCAN job_worker:*` (O(keyspace)) on every dead-worker check.
  - `RequeueRunningJob` reads status and then writes in a separate step; it is not atomic, so a result landing in between can be overwritten by a requeue.
  - Requeue goes to the queue tail and increments no attempt counter. There is no max-retry cap or dead state, so a poison job loops forever.
  - A worker that re-registers (e.g. after the monitor removed it) gets a new random UUID (`handlers.go:17`), orphaning the old identity.
- **Verdict:** a mechanism exists; the <15s claim is **unmeasured** and the requeue semantics are weaker than required.

### 2.3 "Zero job loss under single-node failure"
- **Exists:** job records and results persist in Redis keys with no TTL.
- **Gaps:**
  - No Redis persistence config exists anywhere (compose is empty), so a Redis restart loses everything unless the operator configured AOF.
  - `SubmitResult` (`handlers.go:138`) authorizes by worker-ID match only. There are no attempt numbers or lease tokens, so a late result from a
    presumed-dead worker is accepted if the job happens to be `running` again under the same worker ID, and after a finalize the handler returns `Accepted: true` without comparing results.
  - A worker mid-job whose heartbeat stops (partition) can still commit its result after requeue.
  - The coordinator does no recovery pass on startup.
  - Cancellation, per-job timeouts enforced by the coordinator, and retry backoff do not exist.
- **Verdict:** **not backed by the code.** Needs leases, attempt tokens, idempotent completion and chaos tests.

## 3. Stubbed or dead code

- `scheduler.PickWorker` / `AssignJob` (`coordinator/internal/scheduler/scheduler.go`) are never called. Real assignment is pull-based FCFS in `RequestJob`; job resource requirements are never compared to the pulling worker.
- Worker `store.PublishJobStatus` and `PublishAssignment` are no-ops (`worker/internal/store/redis.go:115-125`). The file is named `redis.go` but speaks gRPC.
- Worker config still carries Redis fields (`RedisAddr`, `QueueKey`, `RedisIOTimeout`) that are only used as timeouts.
- `JobSchema`, `WorkerSchema`, `AssignmentSchema` and the "v2" lifecycle enums in `worker.proto` are defined but not populated by the coordinator.
- Heartbeat sends only CPU core count, total memory and a GPU boolean; it reports **no usage** (`worker/internal/resources/reporter.go`). `HeartbeatRequest.current_load` is declared but never filled. The 90% safety rule therefore cannot exist today.

## 4. Security posture of the executor (`worker/internal/executor/docker.go`)

Present: `--rm`, `--network none`, `--cpus`, `--memory`, labelled containers, SHA-256 verification of the downloaded artifact (in `artifact/fetcher.go`), output size/file caps (in `output/manager.go`).

Missing:
- No `--user`, `--cap-drop=ALL`, `--security-opt no-new-privileges`, `--read-only`, tmpfs, `--pids-limit`, ulimits, or `--memory-swap` (swap is therefore unrestricted).
- Output dir is a writable host bind mount with no size limit.
- stdout/stderr are captured into unbounded in-memory buffers (a job can OOM the worker by printing).
- Timeout kills the `docker run` client process via `exec.CommandContext`, which does not stop the container; there is no `docker kill`/`rm` by name.
- Image ref is whatever the submitter says. No allow/deny policy; a job without an artifact can make the daemon pull any image.
- `docker load` of an untrusted tar and `image rm` of its tag happen on the contributor's daemon with no policy check.
- gRPC is plaintext (`insecure.NewCredentials()`), with no enrollment token, no worker credential, no rate limit, and thin input validation in `SubmitJob` (no size/length limits).

## 5. Operability gaps

No metrics, no `/healthz`/`/readyz`, plain `log.Printf` in the coordinator (worker uses `slog` text), no graceful drain of in-flight jobs, no CI, no lint config, no proto breaking-change check, no CLI (`status`, `logs`, `workers`, `explain` do not exist), `README.md` says "prototype".

## 6. Environment limits for verification (this machine)

Docker 29.8 (runc only), Go 1.27, cargo 1.94, 16 cores / 15 GB RAM. Not verifiable here: gVisor (`runsc` not installed), GPU, a headless run of the Tauri GUI, and GitHub Actions itself (workflows can be linted, not executed).

## 7. Tracking

| # | Finding | Resolved in |
|---|---|---|
| A1 | Claim script ignores liveness/overload; follow-up writes non-atomic | **Phase 2 (done)**: single atomic claim script checks liveness, overload, drain, capacity, requirements. 90% rule wiring in Phase 3 |
| A2 | Requeue loses FIFO position; no attempt counter; no DEAD state | **Phase 2 (done)**: ZSET keyed by original sequence; attempts, backoff, DEAD set, revive |
| A3 | Liveness timings hard-coded and unmeasured | **Phase 2 (done)**: configurable and validated; measured 5.6-6.7s in-process, 4.7-5.9s on real containers. Full percentiles in Phase 8 |
| A4 | No lease/attempt tokens; late result can commit | **Phase 2 (done)**: lease tokens; stale/duplicate results discarded |
| A5 | No Redis persistence or coordinator recovery | **Phase 1+2 (done)**: AOF in compose; stateless coordinator; Redis SIGKILL and coordinator restart tests |
| A6 | Heartbeat carries no usage; no 90% rule | Phase 3 |
| A7 | Executor not hardened; unbounded output; container survives timeout | Phase 4 |
| A8 | No auth/TLS/rate limit | Phase 4 |
| A9 | Dead scheduler; no requirement matching | Phase 5 |
| A10 | No metrics/health/drain | Phase 6 |
| A11 | No compose/Makefile/CI/CLI; README says prototype | Phase 1 (compose, Makefile, CI, CLI, quickstart) / 7 (full README) |
| A12 | Coordinator config rejected any localhost Redis ("cloud redis address required"), so local bring-up was impossible | Phase 1 (removed; defaults to 127.0.0.1:6379, validated) |
| A13 | `worker/Dockerfile` could not build (copied only `go.mod`; `replace ../coordinator` is outside its context) | Phase 1 (repo-root build context) |
| A14 | Dead `worker/internal/store/resp.go` (raw-Redis client) and deprecated `grpc.DialContext/WithBlock`, found by staticcheck | Phase 1 (deleted; `grpc.NewClient`) |
| A15 | Artifacts required external S3/R2, so a self-hosted cluster could not run a job without cloud credentials | Phase 1 (coordinator artifact store + `computehive-artifact://` references) |
| A16 | `SubmitJob` required a command even when the image has a `CMD` | Phase 1 (allowed when an artifact is attached) |
| A17 | A claim whose reply is lost (crash between the atomic claim and the worker hearing about it) strands the job on a live worker | **Phase 2 (done)**: found by the Redis-restart chaos test; fixed by heartbeat reconciliation (see decisions.md D-13) |
| A18 | `docker run` timeout/cancel left the container running (only the CLI client was killed) | **Phase 2 (done)**: container removed by name; Phase 4 hardens further |
| A19 | Worker polled `RequestJob` in a tight loop with no delay | **Phase 2 (done)**: `POLL_INTERVAL` (500ms) |
