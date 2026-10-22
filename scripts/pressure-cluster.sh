#!/usr/bin/env bash
# Cluster-level check of the resource-safety rule. Starts "worker-hot" (RAM limit 1%, so it is
# permanently overloaded), then submits jobs and verifies that the overloaded worker is shown as
# overloaded and is never assigned any of them, while the healthy workers finish all of them.
# Needs `make up` first.
set -euo pipefail
cd "$(dirname "$0")/.."
CLI="${CLI:-./bin/computehive}"
. scripts/env.sh
JOBS="${1:-6}"
make build >/dev/null

docker compose ps --status running --services | grep -q '^worker-1$' || { echo "cluster is not running: run 'make up'" >&2; exit 1; }
cleanup() { docker compose --profile pressure rm -sf worker-hot >/dev/null 2>&1 || true; }
trap cleanup EXIT

# worker-hot is a brand-new identity, so it needs a fresh enrollment token.
"$CLI" admin enroll --ttl 10m --uses 1 --note "pressure test" > .computehive/enroll.token 2>/dev/null
echo "== starting worker-hot (RAM_LIMIT_PERCENT=1: permanently over its limit)"
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose --profile pressure up -d --build worker-hot >/dev/null

for _ in $(seq 1 30); do
  "$CLI" workers | grep -q '^worker-hot' && break
  sleep 1
done
sleep 4   # a few heartbeats with usage
echo "== cluster view"
"$CLI" workers
state=$("$CLI" workers | awk '$1=="worker-hot"{print $2}')
case "$state" in
  "overloaded(ram>=1%)") echo "OK: worker-hot is reported as $state" ;;
  *) echo "FAIL: worker-hot state is '$state', expected exactly overloaded(ram>=1%) (any other reason means the test is not measuring the RAM rule)" >&2; exit 1 ;;
esac

echo "== submitting $JOBS jobs"
ids=()
for i in $(seq 1 "$JOBS"); do
  out=$("$CLI" submit examples/hello --timeout 60)
  ids+=("$(echo "$out" | sed -n 's/^submitted job //p')")
done
for id in "${ids[@]}"; do
  "$CLI" result "$id" --wait --timeout 3m >/dev/null || { echo "FAIL: job $id did not finish" >&2; exit 1; }
done
echo "OK: all $JOBS jobs finished on the healthy workers"

assigned_hot=$(docker compose logs --no-color --since "$since" coordinator | grep '"job assigned"' | grep -c '"worker_id":"worker-hot"' || true)
assigned_all=$(docker compose logs --no-color --since "$since" coordinator | grep -c '"job assigned"' || true)
echo "jobs assigned since start: $assigned_all total, $assigned_hot to worker-hot"
if [ "$assigned_hot" != "0" ]; then echo "FAIL: the overloaded worker received $assigned_hot job(s)" >&2; exit 1; fi

hot_pulls=$(docker compose logs --no-color --since "$since" worker-hot | grep -c 'job received' || true)
paused=$(docker compose logs --no-color --since "$since" worker-hot | grep -c 'intake_paused' || true)
echo "worker-hot: $hot_pulls jobs received, intake paused event logged $paused time(s)"
[ "$hot_pulls" = "0" ] && [ "$paused" -ge 1 ] || { echo "FAIL: worker-hot should have paused intake and received nothing" >&2; exit 1; }
echo "pressure check: PASS"
