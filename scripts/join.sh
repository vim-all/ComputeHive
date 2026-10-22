#!/usr/bin/env bash
# Join THIS machine to a ComputeHive cluster as a worker. The operator runs `computehive admin enroll`, which
# prints the exact command, including a single-use token and the cluster CA's fingerprint:
#
#   scripts/join.sh --coordinator HOST:50051 --token TOKEN --ca-sha256 FINGERPRINT [--worker-id NAME]
#
# What it does: checks Docker, builds the worker once if it is not built yet, then runs `computehive worker join`,
# which downloads the cluster CA and REFUSES to continue unless its SHA-256 equals FINGERPRINT, enrols with the
# token (which works once), and runs a worker in the foreground. Ctrl-C drains: the current job finishes first.
#
# Your machine, your rules: the worker obeys your local policy (CPU/RAM caps, schedule, pause, kill switch):
#   computehive worker status | pause | resume | kill | policy
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -eq 0 ] || [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
  exit 0
fi

command -v docker >/dev/null 2>&1 || { echo "Docker is required (https://docs.docker.com/get-docker/): the worker runs jobs in containers." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "The Docker daemon is not reachable. Is it running, and is your user allowed to use it (docker group)?" >&2; exit 1; }

if [ ! -x bin/computehive ] || [ ! -x bin/computehive-worker ]; then
  command -v go >/dev/null 2>&1 || { echo "Go is needed once, to build the worker (https://go.dev/dl/). Or copy prebuilt bin/computehive and bin/computehive-worker into this directory." >&2; exit 1; }
  echo "building the worker (first run only) ..."
  mkdir -p bin
  (cd cli && go build -o ../bin/computehive ./cmd/computehive)
  (cd worker && go build -o ../bin/computehive-worker ./cmd/worker)
fi

exec ./bin/computehive worker join "$@"
