#!/usr/bin/env bash
# Brings the local cluster up securely:
#   1. generates secrets once (.computehive/: API token, private CA, server certificate)
#   2. starts Redis and the coordinator (TLS + authentication on)
#   3. mints a short-lived, limited-use enrollment token
#   4. starts the workers, which enrol with it and then keep their own credential
# Idempotent: safe to run again; existing secrets and worker credentials are kept.
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/env.sh
. scripts/env.sh

DIR=.computehive
mkdir -p "$DIR/certs" "$DIR/artifacts" "$COMPUTEHIVE_WORK_DIR"
chmod 700 "$DIR" "$DIR/certs"
chmod 1777 "$COMPUTEHIVE_WORK_DIR" 2>/dev/null || true

if [ ! -s "$DIR/api.token" ]; then
  head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$DIR/api.token"
  chmod 600 "$DIR/api.token"
  echo "generated a new API token in $DIR/api.token"
fi
if [ ! -s "$DIR/certs/ca.crt" ] || [ ! -s "$DIR/certs/server.crt" ]; then
  ./bin/computehive-coordinator certs --out "$DIR/certs" --hosts localhost,127.0.0.1,coordinator
fi
cp "$DIR/certs/ca.crt" "$DIR/ca.crt"          # the CLI finds this automatically
chmod 644 "$DIR/certs/ca.crt" "$DIR/certs/server.crt"

echo "starting redis and the coordinator ..."
docker compose up -d --build --wait redis coordinator

# A fresh one-hour token for up to 10 workers. Workers that already have a credential ignore it.
./bin/computehive admin enroll --ttl 1h --uses 10 --note "make up" > "$DIR/enroll.token" 2>/dev/null
chmod 600 "$DIR/enroll.token"

echo "starting the workers and the metrics stack ..."
docker compose up -d --build --wait worker-1 worker-2 worker-3 prometheus grafana
