#!/usr/bin/env bash
# Real-cluster chaos: SIGKILL the worker container that is running a job and measure how
# long the cluster takes to requeue it and finish it elsewhere. Needs `make up` first.
#
#   scripts/chaos-cluster.sh [iterations]      (default 3)
#
# Writes "kill_to_requeue_seconds N" / "kill_to_complete_seconds N" lines to $CHAOS_REPORT if set.
set -euo pipefail

ITER="${1:-3}"
CLI="${CLI:-./bin/computehive}"
LIMIT=15
cd "$(dirname "$0")/.."
make build >/dev/null   # always: a stale CLI cannot parse the newer states

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }
field() { sed -n "s/^$1: *//p" | head -1; }   # field <name>: extract "name:   value" from CLI output
report() { [ -n "${CHAOS_REPORT:-}" ] && echo "$1 $2" >> "$CHAOS_REPORT" || true; }

docker compose ps --status running --services | grep -q '^worker-' || { echo "cluster is not running: run 'make up'" >&2; exit 1; }
alive=$("$CLI" workers | grep -cE ' (ready|waiting|running) ' || true)
[ "$alive" -ge 2 ] || { echo "need at least 2 live workers, found $alive" >&2; exit 1; }

fail=0
for i in $(seq 1 "$ITER"); do
  echo "== iteration $i/$ITER"
  out=$("$CLI" submit examples/sleep --env SLEEP_SECONDS=40 --timeout 120)
  job=$(echo "$out" | sed -n 's/^submitted job //p')
  echo "job $job"

  # Wait until a worker is actually running it.
  victim=""
  for _ in $(seq 1 60); do
    st=$("$CLI" status "$job")
    if [ "$(echo "$st" | field status)" = "running" ]; then victim=$(echo "$st" | field worker); break; fi
    sleep 0.5
  done
  [ -n "$victim" ] || { echo "job never started running" >&2; fail=1; continue; }
  container="computehive-${victim}-1"
  echo "running on $victim; waiting a random moment, then docker kill $container"
  sleep $((RANDOM % 4))

  docker update --restart=no "$container" >/dev/null   # a real node failure: it must not come back by itself
  t0=$(now)
  docker kill "$container" >/dev/null

  t_requeue=""
  t_done=""
  for _ in $(seq 1 400); do
    st=$("$CLI" status "$job")
    s=$(echo "$st" | field status)
    attempt=$(echo "$st" | field attempt | awk '{print $1}')
    w=$(echo "$st" | field worker)
    if [ -z "$t_requeue" ] && { [ "$s" = "requeued" ] || [ "${attempt:-1}" -ge 2 ]; }; then t_requeue=$(now); fi
    if [ "$s" = "succeeded" ]; then t_done=$(now); break; fi
    if [ "$s" = "failed" ] || [ "$s" = "dead" ] || [ "$s" = "canceled" ]; then echo "job ended $s" >&2; fail=1; break; fi
    sleep 0.25
  done
  docker start "$container" >/dev/null; docker update --restart=unless-stopped "$container" >/dev/null

  if [ -z "$t_requeue" ] || [ -z "$t_done" ]; then echo "FAIL: job did not recover" >&2; fail=1; continue; fi
  rq=$(elapsed "$t0" "$t_requeue"); dn=$(elapsed "$t0" "$t_done")
  final=$("$CLI" status "$job")
  echo "killed $victim -> requeue observed after ${rq}s -> job succeeded after ${dn}s (attempt $(echo "$final" | field attempt))"
  report kill_to_requeue_seconds "$rq"; report kill_to_complete_seconds_cluster "$dn"
  # job duration is 40s, so "complete" includes re-running it; the requeue figure is the failure-detection latency.
  if awk -v v="$rq" -v l="$LIMIT" 'BEGIN{exit !(v>=l)}'; then echo "FAIL: requeue took ${rq}s (limit ${LIMIT}s)" >&2; fail=1; fi
  fa=$(echo "$final" | field attempt | awk '{print $1}')
  [ "${fa:-1}" -ge 2 ] || { echo "FAIL: job finished on attempt ${fa:-?}, expected a retry" >&2; fail=1; }
done
[ "$fail" = 0 ] && echo "cluster chaos: PASS ($ITER kills, every job recovered, requeue < ${LIMIT}s)" || { echo "cluster chaos: FAIL" >&2; exit 1; }
