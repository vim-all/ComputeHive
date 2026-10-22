#!/usr/bin/env bash
# A guided, real run (about 90 seconds): nothing here is canned, every number printed is measured now.
#
#   1. the cluster          3 workers, their resources
#   2. one job              build, upload, schedule, run, result
#   3. a burst of jobs      spread across the workers; which worker ran each
#   4. live output          a job's output streamed while it runs
#   5. a worker dies        kill -9 the worker running a long job; watch it be requeued and finish elsewhere
#   6. recovery             the dead worker rejoins
#
# Needs `make up` first.   scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh >/dev/null 2>&1 || true
CLI="${CLI:-./bin/computehive}"
make build >/dev/null

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   \033[2m%s\033[0m\n' "$*"; }
now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.1f", b-a}'; }
field() { sed -n "s/^$1: *//p" | head -1; }   # field <name>: extract "name:   value" from CLI output

docker compose ps --status running --services | grep -q '^coordinator$' || { echo "the cluster is not running: run 'make up' first" >&2; exit 1; }
# The demo uses three workers; start any that are missing and give them a moment to register.
docker compose up -d worker-1 worker-2 worker-3 >/dev/null 2>&1 || true
alive=0
for _ in $(seq 1 40); do
  alive=$("$CLI" workers 2>/dev/null | grep -cE ' (ready|running) ' || true)
  [ "$alive" -ge 3 ] && break
  sleep 1
done
[ "$alive" -ge 3 ] || { echo "need 3 live workers, found $alive: check 'docker compose ps'" >&2; exit 1; }

step "1. The cluster: three workers, each a Docker host that pulls work"
"$CLI" workers

step "2. One job: build the directory into an image, upload it, schedule it, run it in a sandbox"
"$CLI" submit examples/hello --wait

step "3. A burst: six jobs at once, spread across the workers"
ids=()
for i in 1 2 3 4 5 6; do
  ids+=("$("$CLI" submit examples/hello --env "RUN=$i" | sed -n 's/^submitted job //p')")
done
for id in "${ids[@]}"; do "$CLI" result "$id" --wait >/dev/null; done
"$CLI" jobs --limit 6 | cut -c1-110
note "'computehive explain <job-id>' shows why each job went where it did (scores per worker)."

step "4. Live output: a job that prints as it runs, followed from here"
out=$("$CLI" submit examples/sleep --timeout 60 -- sh -c 'for i in 1 2 3 4 5; do echo "working... step $i of 5"; sleep 2; done; echo finished')
job=$(echo "$out" | sed -n 's/^submitted job //p')
"$CLI" logs "$job" --follow | sed -n '1,/^--- final result/p' | grep -v '^--- final result'

step "5. A worker dies mid-job: kill -9 it and watch the cluster recover"
out=$("$CLI" submit examples/sleep --env SLEEP_SECONDS=30 --timeout 120)
job=$(echo "$out" | sed -n 's/^submitted job //p')
echo "job $job"
victim=""
for _ in $(seq 1 60); do
  st=$("$CLI" status "$job")
  if [ "$(echo "$st" | field status)" = "running" ]; then victim=$(echo "$st" | field worker); break; fi
  sleep 0.5
done
[ -n "$victim" ] || { echo "the job never started running" >&2; exit 1; }
sleep 3
echo "running on $victim: killing its container now"
t_kill=$(now)
docker kill "computehive-${victim}-1" >/dev/null
t_requeue=""
for _ in $(seq 1 120); do
  st=$("$CLI" status "$job")
  if [ "$(echo "$st" | field attempt | cut -d' ' -f1)" -ge 2 ] 2>/dev/null; then t_requeue=$(now); break; fi
  sleep 0.2
done
[ -n "$t_requeue" ] || { echo "the job was not requeued in time" >&2; exit 1; }
echo "  -> re-assigned to another worker $(elapsed "$t_kill" "$t_requeue")s after the kill (missed heartbeats noticed after 6s, then requeue, retry backoff and a new claim)"
"$CLI" result "$job" --wait | head -4
t_done=$(now)
st=$("$CLI" status "$job")
echo "  -> finished $(elapsed "$t_kill" "$t_done")s after the kill, on attempt $(echo "$st" | field attempt), worker $(echo "$st" | field worker)"
note "no job lost, and it was committed exactly once: that is what 'make chaos' checks over and over."

step "6. Recovery: the killed worker rejoins"
docker compose up -d "$victim" >/dev/null 2>&1
for _ in $(seq 1 40); do
  [ "$("$CLI" workers | grep -cE ' (ready|running) ')" -ge 3 ] && break
  sleep 1
done
"$CLI" workers

step "Where to look next"
echo "  Grafana (live cluster view)  http://localhost:3000   (admin / admin)"
echo "  Why did a job go there?      $CLI explain <job-id>"
echo "  Hostile job / sandbox demo   $CLI submit examples/hostile --wait"
echo "  The reliability test suite   make chaos"
