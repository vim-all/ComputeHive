# Benchmarks and evidence

Every number here was measured by the commands shown, on the machine below, and the raw output is committed in
[`benchmark-results/`](benchmark-results/). Nothing is estimated. Where something could not be measured, it says so.

```bash
make bench                      # everything (about 45 minutes); writes bench-results/<timestamp>/
make bench SECTION=control      # control-plane limits only (in-process workers, real Redis)
make bench SECTION=reliability  # requeue timing + zero-loss under random kills
make bench SECTION=safety       # 90% rule on a real container + hostile-job suite
make bench SECTION=cluster      # live cluster (needs `make up`): e2e latency, real throughput, real docker kill
BENCH_QUICK=1 make bench        # same code paths, smaller counts, noisier numbers
```

## The machine, and why the numbers are noisy

Intel i7-13620H (16 threads), 15.3 GB RAM, Linux 7.2.4, Docker 29.8.0, Go 1.27.1, `redis:7-alpine`.
**The machine is shared**: other people's containers run on it, so the 1-minute load average was 1.7 to 12 during these
runs (recorded in each result file). Repetitions report min / median / max. Treat differences under about 30% as noise.
Anything that needs more than one machine (many hosts, real networks, WAN latency) was **not** measured.

## 1. The resume claims

| Claim | Result | How it was measured |
|---|---|---|
| No double assignment (atomic Lua claim) | **0 double assignments** with 3000 jobs and 48 competing claimers | the no-double-assignment tests in `coordinator/internal/queue` (real Redis) |
| Failure detected and job requeued in < 15 s | **4.7 to 6.7 s** in-process (30 kills: p50 5.67 s, p95 6.45 s, max 6.65 s); **5.0 to 6.6 s** on the live cluster with a real `docker kill` (10 kills: p50 5.5 s) | `TestWorkerKilledMidJobIsRequeuedUnder15s...` (`CHAOS_ITER=30`); `scripts/chaos-cluster.sh` |
| Zero job loss under single-node failure | **0 lost, every job committed exactly once**: 3 runs of 1000 jobs (133, 110 and 125 worker kills) and one of 5000 jobs (805 kills) | `TestThousandJobsWithRandomWorkerKills` (`CHAOS_JOBS`); exactly-once is audited from the server's own `commits` counter |

The ~5.5 s is the 6 s "dead after" limit minus where in the 2 s heartbeat cycle the kill landed, plus the 1 s reaper tick.
It is configuration (`WORKER_DEAD_AFTER`), not a hard floor; the claim to defend is "under 15 s", not "5 s".

## 2. Control-plane performance

In-process workers that do no work (Docker is **not** in these numbers), real Redis, production scheduling settings
(5 s patience, 250 ms snapshot).

### Assignment latency (job submitted to job assigned, idle cluster, one arrival every 25 ms, 300 jobs)

With Redis as deployed (`appendfsync everysec`, as in `docker-compose.yml`):

| workers | p50 | p95 | p99 | max |
|---|---|---|---|---|
| 1 | 9 ms | 16 ms | 16 ms | 17 ms |
| 8 | 8 ms | 15 ms | 16 ms | 16 ms |
| 32 | 8 ms | 15 ms | 16 ms | 17 ms |

With the test default `appendfsync always` (fsync on every write) the median is the same (10 to 11 ms) but runs on this
shared disk showed **stalls of 2 to 4 s** (p99 2.2 s, max 3.3 s at 32 workers in the last run; at every worker count
in an earlier one), appearing as a block of consecutive jobs waiting while the backlog drains. Turning persistence off
removed them (max 17 ms). So the tail is disk fsync latency on a shared machine, not the scheduler. That is why the
deployment uses `everysec` (decision D-66). See `benchmark-results/assign-latency.txt`.

### Cost of one `RequestJob` (queue empty, or assigning one job; snapshot cache off, the worst case)

| pool size | empty p50 / p99 | assign p50 / p99 |
|---|---|---|
| 1 | 0.46 / 0.72 ms | 10.1 / 14.6 ms |
| 8 | 0.29 / 0.75 ms | 10.1 / 14.6 ms |
| 64 | 0.86 / 1.33 ms | 11.6 / 16.5 ms |

### Scheduler decision cost (pure CPU, no Redis; `go test -bench Evaluate ./internal/scheduler`)

6.8 µs, 82 µs and 1.08 ms per decision at 10, 100 and 1000 workers (an earlier, quieter run gave 5.2 µs / 60 µs / 0.77 ms).

### Throughput versus workers (1000 jobs, 3 repetitions, median jobs/second)

| workers | before cache work | after (final) | Redis cmds/job before -> after |
|---|---|---|---|
| 1 | 49.0 | 45.5 | 89 -> 48 |
| 2 | 43.4 | 60.7 | 110 -> 40 |
| 4 | 51.4 | 95.8 | 118 -> 32 |
| 8 | 95.2 | 94.6 | 118 -> 48 |
| 16 | 162.8 | 139.3 | 106 -> 86 |
| 32 | 195.9 | 137.5 | 149 -> 157 |
| 64 | 92.6 | 77.4 | 204 -> 320 |

What this shows, honestly:

* The baseline exposed a real problem: with a few workers, **idle pollers made the control plane spend ~100 Redis
  commands per job** (each poll re-read the first 50 pending jobs), and Redis runs commands one at a time. The floor for
  a job is about 4 commands.
* The fix (a shared 250 ms snapshot, updated in place on claim and on completion, an empty queue re-read cheaply) cut that
  to 32 to 48 commands per job at 1 to 8 workers and **raised throughput at 2 and 4 workers by about 40% and 90%**. It is
  neutral at 8.
* It did **not** help, and probably hurt, at 16 to 64 workers (162.8 -> 139.3, 195.9 -> 137.5, 92.6 -> 77.4 jobs/s;
  commands per job rose at 32 and 64). Run-to-run noise on this shared machine is large (the baseline's own 64-worker runs
  ranged 87 to 194), so I can't separate noise from regression there, but I am not claiming an improvement at those sizes.
  With many workers, a worker whose cached view says "busy" triggers a whole-cluster re-read; the likely next step is a
  per-worker re-read. **Not done.**
* The intermediate attempts are kept as evidence: `throughput-first-cache-attempt.txt` (a regression I introduced: a
  worker that finished a job still looked busy for 250 ms, so one worker managed 3.8 jobs/s) and
  `throughput-second-attempt-retry-only.txt` (a re-read on every stale poll, which was worse at high worker counts).
* Peak measured control-plane capacity is therefore **roughly 100 to 190 jobs/s with no-op jobs** on this laptop. Real
  jobs are bound by Docker start-up (about 1 to 2 s each), not by the coordinator.

## 3. Live cluster (3 real worker containers)

| Measurement | Result |
|---|---|
| End-to-end latency of `examples/hello` (submit to result, warm image) | **2.05 to 2.07 s** typical; 2 of 20 runs (and 1 of 9 in a second batch) took 7 to 8 s. The cause of those outliers was not isolated. |
| Throughput, 30 `hello` jobs submitted in parallel | 49.3 / 48.0 / 50.7 jobs per minute with 1 / 2 / 3 workers: **flat**. This test is limited by the CLI submit path, not by worker count, so it does **not** show scaling and should not be quoted as such. |
| Real `docker kill` of the worker running a job | requeue observed after 4.97 to 6.58 s (10 kills); every job finished on attempt 2 |
| `make demo` end to end | 82 s, exit 0 |
| Clone to first finished job (fresh `git clone`, `make up`, `submit examples/hello`) | **360 s**: `make up` 355 s, the job 5.2 s. 295 s of that is a single `go mod download` inside the Docker build (cold module cache, run by three identical worker image builds at once). Docker's base-image layers were already cached on this machine, so a first-ever run on a clean machine would be longer. Building the worker image once is the likely fix; **not done or tested**. |

## 4. Safety evidence

* **90% rule on real memory pressure** (`TestRealContainerMemoryPressureTripsAndClearsOverload`): a container allocated
  1411 MB, RAM rose from about 79.4% to 86.5% (the effective limit was 83.4% after the headroom rule), the worker stopped
  taking jobs, and resumed 0 s after the memory was freed (release timer 2 s).
* **Hostile jobs** (`TestHostile`, `examples/hostile`): network, host files, writes outside the sandbox, privilege
  escalation, Docker socket, cloud metadata, fork bomb (stopped at the PID limit), memory fill, CPU spin, disk fill and
  unbounded output are all contained; 12 of 12 subtests pass in 60 s.
* gVisor support is implemented but **not verified** on this machine (`GVISOR.md`).

## 5. What was not measured

* More than one physical host, WAN latency, or GPU workers.
* Whether the live-cluster 7 to 8 s latency outliers are image loading on a cold worker.
* Throughput scaling with real jobs (the parallel-submit test is submit-bound).
* Behaviour at 16 to 64 workers after the cache change beyond the single noisy sweep above.
* macOS and Windows.
