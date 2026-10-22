#!/usr/bin/env bash
# Reproducible benchmarks and evidence. Every number in docs/BENCHMARKS.md comes from running this script.
#
#   scripts/bench.sh                 # everything below (about 40 minutes)
#   BENCH_QUICK=1 scripts/bench.sh   # smaller counts (about 10 minutes): same code paths, noisier numbers
#   scripts/bench.sh control         # only the in-process control-plane benchmarks
#   scripts/bench.sh reliability     # only requeue timing + zero-loss runs
#   scripts/bench.sh safety          # only the 90% rule and sandbox suites
#   scripts/bench.sh cluster         # only the live-cluster measurements (needs `make up`)
#
# What is measured, and what it means:
#   control      the coordinator's own limits: jobs/second vs number of workers, assignment latency, RequestJob cost,
#                with in-process workers that do no work (so Docker is NOT in these numbers) on a real Redis.
#   reliability  time from killing a worker to its job being requeued; N jobs with K random worker kills and how many
#                were lost (must be 0) or committed twice (must be 0).
#   safety       the 90% RAM rule on a real container, and the hostile-job suite.
#   cluster      real containers on the live cluster: end-to-end job latency, throughput with 1..3 real workers, and
#                a real `docker kill` requeue time.
#
# Results go to bench-results/<timestamp>/ (raw lines + the environment). The machine is shared with whatever else runs
# on it: the load average is recorded because it changes the numbers, and repetitions report min / median / max.
set -uo pipefail
cd "$(dirname "$0")/.."
SECTION="${1:-all}"
QUICK="${BENCH_QUICK:-}"
OUT="bench-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
RAW="$OUT/raw.txt"
: > "$RAW"

q() { if [ -n "$QUICK" ]; then echo "$2"; else echo "$1"; fi; }   # q <full> <quick>
log() { printf '\n== %s\n' "$*" | tee -a "$RAW"; }

# ---------------------------------------------------------------------------------------------------- environment
{
  echo "date:        $(date -u +%FT%TZ)"
  echo "commit:      $(git rev-parse --short HEAD 2>/dev/null) $(git diff --quiet 2>/dev/null || echo '(with uncommitted changes)')"
  echo "os:          $(uname -sr)"
  echo "cpu:         $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //') ($(nproc) threads)"
  echo "memory:      $(awk '/MemTotal/ {printf "%.1f GB", $2/1048576}' /proc/meminfo)"
  echo "load (1/5/15 min, before): $(cut -d' ' -f1-3 /proc/loadavg)"
  echo "go:          $(go version 2>/dev/null)"
  echo "docker:      $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  echo "redis image: redis:7-alpine"
  echo "mode:        $([ -n "$QUICK" ] && echo quick || echo full)"
} | tee "$OUT/environment.txt" | tee -a "$RAW"

# ---------------------------------------------------------------------------------------------------- control plane
control() {
  log "control plane: throughput vs workers (jobs=$(q 1000 300), reps=$(q 3 2))"
  (cd coordinator && BENCH=1 BENCH_JOBS=$(q 1000 300) BENCH_REPS=$(q 3 2) BENCH_WORKERS=$(q 1,2,4,8,16,32,64 1,4,16) \
     go test -count=1 -timeout 60m -run TestBenchThroughput -v ./internal/chaos 2>&1) | grep -E '^BENCH|^(--- FAIL|FAIL|panic)' | tee -a "$RAW"
  for fsync in everysec always; do
    log "control plane: submit-to-assign latency on an idle cluster (Redis appendfsync=$fsync; the deployment uses everysec)"
    (cd coordinator && COMPUTEHIVE_TEST_REDIS_FSYNC=$fsync BENCH=1 BENCH_LAT_JOBS=$(q 300 100) \
       go test -count=1 -timeout 30m -run TestBenchAssignLatency -v ./internal/chaos 2>&1) | grep -E '^BENCH|slow assignment|^(--- FAIL|FAIL|panic)' | tee -a "$RAW"
  done
  log "control plane: cost of one RequestJob (no snapshot cache: worst case)"
  (cd coordinator && BENCH=1 BENCH_RPC_CALLS=$(q 1000 300) \
     go test -count=1 -timeout 30m -run TestBenchRequestJobRPC -v ./internal/chaos 2>&1) | grep -E '^BENCH|^(--- FAIL|FAIL|panic)' | tee -a "$RAW"
  log "control plane: scheduler decision cost (pure CPU, no Redis)"
  (cd coordinator && go test -count=1 -run xxx -bench Evaluate -benchtime 2s ./internal/scheduler 2>&1) | grep -E '^Benchmark' | tee -a "$RAW"
}

# ---------------------------------------------------------------------------------------------------- reliability
reliability() {
  local report="$OUT/chaos-report.txt"; : > "$report"
  log "reliability: time from killing a worker to its job being requeued ($(q 30 8) kills, production timings)"
  (cd coordinator && CHAOS_REPORT="$OLDPWD/$report" CHAOS_ITER=$(q 30 8) \
     go test -count=1 -timeout 30m -run TestWorkerKilledMidJobIsRequeuedUnder15s -v ./internal/chaos 2>&1) | grep -E 'iteration|^(--- |ok|FAIL|panic)' | tee -a "$RAW"
  log "reliability: zero loss under random worker kills"
  for jobs in $(q "1000 1000 1000 5000" "1000 1000"); do
    (cd coordinator && CHAOS_REPORT="$OLDPWD/$report" CHAOS_JOBS=$jobs \
       go test -count=1 -timeout 30m -run TestThousandJobsWithRandomWorkerKills -v ./internal/chaos 2>&1) | grep -E 'worker kills|^(--- |ok|FAIL|panic)' | tee -a "$RAW"
  done
}

# ---------------------------------------------------------------------------------------------------- safety
safety() {
  log "safety: the 90% rule against real memory pressure (a container fills RAM; the worker must stop, then resume)"
  (cd worker && go test -count=1 -timeout 15m ./internal/monitor/ -run RealContainer -v 2>&1) | grep -E 'RAM|ram|percent|resum|limit|^(--- |ok|FAIL)' | tee -a "$RAW"
  log "safety: hostile-job suite (every attack must be contained)"
  (cd worker && go test -count=1 -timeout 20m ./internal/executor/ -run Hostile -v 2>&1) | grep -E '^(=== RUN|--- |\s+--- |ok|FAIL)|hostile_test.go' | tee -a "$RAW"
}

# ---------------------------------------------------------------------------------------------------- live cluster
cluster() {
  CLI=./bin/computehive
  make build >/dev/null 2>&1
  docker compose ps --status running --services 2>/dev/null | grep -q '^coordinator$' || { echo "the cluster is not running: run 'make up' first" | tee -a "$RAW"; return; }
  source scripts/env.sh >/dev/null 2>&1 || true
  docker compose up -d worker-1 worker-2 worker-3 >/dev/null 2>&1
  for _ in $(seq 1 40); do [ "$($CLI workers | grep -cE ' (ready|running) ')" -ge 3 ] && break; sleep 1; done

  log "cluster: end-to-end latency of one job (submit to result), $(q 20 8) sequential runs on 3 real workers"
  $CLI submit examples/hello --wait >/dev/null 2>&1   # warm: image build + worker cache
  local lat=()
  for _ in $(seq 1 "$(q 20 8)"); do
    s=$(date +%s.%N); $CLI submit examples/hello --wait >/dev/null 2>&1; e=$(date +%s.%N)
    lat+=("$(awk -v a="$s" -v b="$e" 'BEGIN{printf "%.2f", b-a}')")
  done
  printf '%s\n' "${lat[@]}" | sort -n | awk '{a[NR]=$1} END{printf "BENCH e2e_job_latency runs=%d min_s=%s p50_s=%s p95_s=%s max_s=%s\n", NR, a[1], a[int((NR+1)/2)], a[int(NR*0.95+0.5)], a[NR]}' | tee -a "$RAW"

  log "cluster: throughput with 1, 2 and 3 real workers ($(q 30 12) hello jobs each, submitted in parallel)"
  local jobs; jobs=$(q 30 12)
  for n in 1 2 3; do
    docker compose stop $(printf 'worker-%s ' $(seq $((n+1)) 3)) >/dev/null 2>&1 || true
    docker compose up -d $(printf 'worker-%s ' $(seq 1 "$n")) >/dev/null 2>&1
    for _ in $(seq 1 30); do [ "$($CLI workers | grep -cE ' (ready|running) ')" -ge "$n" ] && break; sleep 1; done
    sleep 3
    s=$(date +%s.%N)
    ids=$(seq 1 "$jobs" | xargs -P 8 -I{} sh -c "$CLI submit examples/hello --env RUN={} 2>/dev/null | sed -n 's/^submitted job //p'")
    for id in $ids; do $CLI result "$id" --wait >/dev/null 2>&1; done
    e=$(date +%s.%N)
    awk -v n="$n" -v j="$jobs" -v a="$s" -v b="$e" 'BEGIN{d=b-a; printf "BENCH real_throughput workers=%d jobs=%d seconds=%.1f jobs_per_minute=%.1f\n", n, j, d, j/d*60}' | tee -a "$RAW"
  done
  docker compose up -d worker-1 worker-2 worker-3 >/dev/null 2>&1

  log "cluster: real docker kill -> requeue ($(q 10 3) iterations)"
  CHAOS_SLEEP=15 CHAOS_REPORT="$OUT/cluster-chaos.txt" scripts/chaos-cluster.sh "$(q 10 3)" 2>&1 | grep -E 'killed|PASS|FAIL' | tee -a "$RAW"
}

case "$SECTION" in
  control) control ;;
  reliability) reliability ;;
  safety) safety ;;
  cluster) cluster ;;
  all) control; reliability; safety; cluster ;;
  *) echo "usage: scripts/bench.sh [all|control|reliability|safety|cluster]" >&2; exit 2 ;;
esac
{ echo; echo "load (1/5/15 min, after): $(cut -d' ' -f1-3 /proc/loadavg)"; } | tee -a "$RAW"
echo; echo "raw results: $RAW"
