# ComputeHive

ComputeHive is a self-hosted distributed task runner. You give it a directory with a
`Dockerfile`; it builds the image, ships it to a worker, runs it in a container with
`--network none`, and hands you the result.

> **Status: under active hardening.** The end-to-end flow below works and is tested.
> Scheduler guarantees (exactly-once assignment, requeue timing, zero loss), resource
> limits, sandboxing and auth are being built phase by phase; see
> [`docs/AUDIT.md`](docs/AUDIT.md) for what is and isn't backed by code today.

## Quickstart (about 5 minutes)

Prerequisites: Docker (with Compose v2), Go 1.25+, `make`.

```bash
git clone <this repo> && cd ComputeHive
make up        # Redis (AOF) + coordinator + 2 workers + Prometheus + Grafana
make demo      # builds the CLI, submits examples/hello, prints its output
```

Expected tail of `make demo`:

```text
submitted job <uuid>
status:    succeeded
exit code: 0
--- stdout (excerpt) ---
hello from <container id> at <timestamp>
```

Then explore:

```bash
./bin/computehive workers                       # registered workers + liveness
./bin/computehive submit examples/hello --wait  # your own job: any dir with a Dockerfile
./bin/computehive status <job-id>
./bin/computehive result <job-id> --wait
make down                                       # stop (data volumes are kept)
```

| Endpoint | URL |
|---|---|
| Coordinator gRPC | `localhost:50051` |
| Coordinator HTTP (artifacts, `/healthz`, `/readyz`) | `http://localhost:8080` |
| Grafana (admin / admin) | `http://localhost:3000` |
| Prometheus | `http://localhost:9090` |

Redis is published on an ephemeral loopback port (`docker compose port redis 6379`)
so it never clashes with a Redis you already run.

## Monorepo layout

```text
ComputeHive/
├── cli/             # `computehive` CLI (Go) - same gRPC API as the desktop app
├── client/          # Tauri desktop app (Vite + TypeScript + Rust)
├── coordinator/     # gRPC + HTTP coordinator (Go)
├── worker/          # worker runtime (Go)
├── shared/proto/    # protobuf contracts (source of truth)
├── deploy/          # Prometheus / Grafana provisioning
├── examples/        # example jobs
├── docs/            # AUDIT.md (and, as phases land, THREAT_MODEL / BENCHMARKS)
├── docker-compose.yml
└── Makefile
```

## How it works

- **Coordinator** (`coordinator/`): `WorkerService` + `ClientService` over gRPC (`:50051`),
  state in Redis, plus an HTTP server (`:8080`) with a content-addressed artifact store
  (`PUT/GET /artifacts/<sha256>`) and `/healthz` / `/readyz`.
- **Worker** (`worker/`): registers, heartbeats, pulls a job with `RequestJob`, downloads the
  artifact and verifies its SHA-256, `docker load`s it, runs it with `--network none`, and
  submits the result.
- **CLI** (`cli/`): `submit` (build, `docker save`, hash, upload, `SubmitJob`), `status`, `result`,
  `logs` (captured stdout/stderr excerpts), `workers`, and `worker join`.
- **Desktop client** (`client/`): Tauri UI for the same flow, with a contributor mode that
  starts a local worker.

Artifacts are referenced as `computehive-artifact://<sha256>`; each worker resolves that
against its own coordinator HTTP address, so the same job works inside Compose, on a LAN,
or behind NAT. `https://` artifact URLs (S3/R2, as used by the desktop client) still work.

## Joining a worker from another machine

On the coordinator host, mint a one-hour, single-use enrollment token (the secrets live in `.computehive/`):

```bash
make enroll                      # prints a token like chet_...
cp .computehive/ca.crt /path/to/the/contributor/   # the cluster CA; it is public
```

On the contributor's machine:

```bash
make build
./bin/computehive worker join --coordinator <coordinator-host>:50051 --ca ca.crt --token chet_...
```

`worker join` checks Docker, coordinator reachability and the worker binary, enrols the worker once (it then keeps its own
revocable credential in `~/.local/state/computehive/<id>/credential`), and runs it in the foreground. Revoke a worker with
`computehive admin revoke <worker-id>`.

## Contributor controls

The person who owns the machine decides what runs on it. These talk to the worker over a private Unix socket:

```bash
computehive worker status                                  # state, why it is not taking jobs, resources, policy
computehive worker pause [--now]                           # stop taking jobs (--now also hands back the running job)
computehive worker resume
computehive worker kill                                    # kill switch: stop everything immediately and exit
computehive worker history                                 # exactly what ran here (image, command, env NAMES, outcome)
computehive worker policy set max_cpus=2 max_memory_mb=4096 schedule=22:00-07:00 idle_only=true
computehive worker policy set allow_network=true allowed_hosts=pypi.org     # off by default
```

The policy is enforced on the worker itself, so the cluster cannot override it.

## Security model (summary)

Jobs are treated as untrusted and possibly malicious; see [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md) for the full model,
including residual risks.

- **Sandbox per job:** non-root, all capabilities dropped, `no-new-privileges`, read-only root filesystem, default seccomp
  profile, no network, no Docker socket, only its own work directory mounted, memory/swap/CPU/PID/file limits, wall-clock timeout
  with forced kill, disk quota and capped logs. A final audit refuses to start any container whose flags weaken this.
- **No surprises on your Docker:** images never pulled, registry logins never used, uploaded image archives may not retag your
  images, and everything a job created is removed afterwards.
- **Optional network:** only when the job asks, your policy allows the host, and traffic goes through a filtering proxy that
  refuses private, loopback and cloud-metadata addresses.
- **gVisor** (`ISOLATION=auto|runc|gvisor`) when available: [`docs/GVISOR.md`](docs/GVISOR.md) (implemented, not verified here).
- **Trust:** TLS 1.3 (mTLS optional), expiring enrollment tokens, hashed revocable per-worker credentials, an API token for
  submitters, per-identity rate limits, validated inputs. `AUTH_DISABLED` / `ALLOW_INSECURE` exist for development and log loudly.
- **Result integrity:** `computehive submit DIR --verify` runs the job on two different workers and accepts it only if their
  outputs match. Costs 2x, needs a deterministic job, and detects disagreement without knowing who lied.
- **Prove it yourself:** `make hostile` runs the hostile-job suite (needs Docker).

## Make targets

| Target | What it does |
|---|---|
| `make up` / `make down` / `make logs` | run the local cluster |
| `make demo` | end-to-end job on the running cluster |
| `make test` | unit tests with the race detector, all modules |
| `make lint` | `go vet` + `staticcheck`, all modules |
| `make build` | `bin/computehive`, `bin/computehive-worker`, `bin/computehive-coordinator` |
| `make proto` | regenerate protobuf Go code |
| `make chaos` | fault injection: in-process suite plus `docker kill` of real workers |
| `make pressure` | overloaded-worker check on the running cluster |
| `make hostile` | hostile-job suite: every attack must be contained and the host must stay healthy |
| `make enroll` | print a one-hour, single-use token for joining a worker |
| `make reset` | stop the cluster and delete all its data and secrets |
| `make bench` | not implemented yet; exits non-zero with a message |

CI (`.github/workflows/ci.yml`) runs gofmt, `go vet`, `staticcheck`, `go test -race`, an
integration job (`make up` + `make demo` on a Docker-equipped runner) and a `buf breaking`
protobuf compatibility check on pull requests.

## Configuration

Coordinator (flags override env, env overrides `.env`): `REDIS_ADDR` (default `127.0.0.1:6379`),
`REDIS_URL`, `REDIS_PASSWORD`, `REDIS_DB`, `REDIS_TLS`, `GRPC_PORT` (50051), `HTTP_PORT` (8080),
`ARTIFACT_DIR`, `ARTIFACT_MAX_BYTES`. Invalid values fail at startup with a message naming the
flag and env var. Worker: see [`worker/README.md`](worker/README.md); notable is
`COORDINATOR_ADDR` and `COORDINATOR_HTTP_URL` (default `http://<coordinator host>:8080`).

## Protocol

`shared/proto/worker.proto` and `client.proto`. `WorkerService`: `RegisterWorker`, `Heartbeat`,
`RequestJob`, `SubmitResult`. `ClientService`: `SubmitJob`, `GetJobStatus`, `GetJobResult`,
`ListWorkers`. Changes are additive; CI rejects wire-breaking changes. Generated Go code is
checked in under `coordinator/pkg/pb`.

## Desktop client

```bash
cd client && pnpm install && pnpm tauri dev
```

See [`client/README.md`](client/README.md). The desktop client is not exercised by CI.

## Reliability guarantees (and how they are tested)

| Guarantee | Mechanism | Test |
|---|---|---|
| A job is assigned to exactly one worker | one atomic Lua script moves it from pending to leased | `TestConcurrentClaimNeverDoubleAssigns` |
| FIFO, preserved on retry | pending set scored by the job's original sequence number | `TestRequeueKeepsFIFOPositionAndCountsAttempt` |
| Dead workers' jobs are requeued in under 15s | 2s heartbeats, dead after 6s, reaper every 1s (about 7s worst case) | `TestWorkerKilledMidJobIsRequeuedUnder15s...`, `scripts/chaos-cluster.sh` |
| A late result from a presumed-dead worker cannot corrupt state | per-attempt lease tokens; stale and duplicate results are discarded | `TestPartitionedWorkerNeverCommitsOverRequeuedJob` |
| No job lost or run to completion twice | all state in Redis (AOF); stateless coordinator | `TestCoordinatorCrashAndRestartLosesNothing`, `TestRedisRestartKeepsState`, `TestThousandJobsWithRandomWorkerKills` |
| Lost assignment replies are recovered | heartbeat reconciliation of unreported leases | `TestLostClaimReplyIsReconciledByHeartbeat` |

Retries: a job's own failure (non-zero exit, timeout) is terminal; infrastructure failures are retried with exponential backoff
up to `MAX_ATTEMPTS` (default 3), after which the job is parked as `DEAD`. Evictions do not count against the job.
Run the fault-injection suite yourself with `make chaos`. Design rationale: [`docs/decisions.md`](docs/decisions.md).

Note: "exactly once" means **committed** exactly once. A worker killed mid-job means the job body may run again elsewhere, so
jobs should be safe to re-run (idempotent) or write only to their output directory.

## Resource limits (the 90% rule)

Workers sample RAM, CPU, load, disk (job work dir and Docker root), swap, temperature, Docker health, GPU and network egress
every second and send them with every heartbeat (`computehive workers` shows RAM/CPU/disk and state).

- **Enter** "overloaded" the moment RAM, CPU or disk reaches **90%** (configurable: `RAM_LIMIT_PERCENT`, `CPU_LIMIT_PERCENT`, `DISK_LIMIT_PERCENT`).
- **Leave** only after every metric has stayed below **80%** for **10s** (`RELEASE_PERCENT`, `RELEASE_AFTER`). Readings in between change nothing.
- **Enforced twice:** the worker stops calling `RequestJob`; independently the coordinator derives overload from the reported numbers
  and refuses overloaded, stale, or metric-less workers, so a misbehaving worker cannot opt out.
- **Headroom:** a job is not placed if its declared memory (default 512 MB) would push the host to the limit.
- **If the host stays critical for `PRESSURE_GRACE` (15s) while a job runs**, the job is stopped gracefully and requeued elsewhere
  without counting as a failure, with an `event=job_evicted_pressure` log line.
- **Every job container** gets `--memory` = `--memory-swap` (no swap), `--cpus`, `--pids-limit` and `--network none`, with defaults
  (512 MB, 1 CPU, 256 PIDs) if the job declares nothing.

Prove it on your machine: `make pressure` (a worker with a 1% RAM limit must show as overloaded and receive no jobs) and
`cd worker && go test ./internal/monitor/ -run RealContainer -v` (a container fills memory; the worker stops, then resumes).

## Known limitations (today)

- Job output files are kept on the coordinator until you delete them (no retention policy yet).
- gVisor support is implemented but was not verified (no `runsc` on the development machine).
- Network jobs need the worker to run directly on the host, not in a container.
- Containers share the host kernel (unless gVisor is used): a kernel vulnerability is the main residual risk.
- Scheduling is first-come-first-served pull with capability filtering (no best-fit scoring yet).
- Single-node Redis only (the Lua scripts address keys dynamically; Redis Cluster would need hash tags).
- Metrics and the Grafana dashboard are not wired yet (Prometheus scrape target is down by design).
