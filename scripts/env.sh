#!/usr/bin/env bash
# Environment shared by every script that runs `docker compose` (source it, do not execute it).
#
#   DOCKER_GID           group of the Docker socket, so non-root worker containers can use it
#   COMPUTEHIVE_UID/GID  the host user: the coordinator runs as you so it can read the (private) TLS key
#   COMPUTEHIVE_WORK_DIR directory the workers share with the Docker daemon for job output
export DOCKER_GID="${DOCKER_GID:-$(stat -c %g /var/run/docker.sock 2>/dev/null || echo 999)}"
export COMPUTEHIVE_UID="${COMPUTEHIVE_UID:-$(id -u)}"
export COMPUTEHIVE_GID="${COMPUTEHIVE_GID:-$(id -g)}"
export COMPUTEHIVE_WORK_DIR="${COMPUTEHIVE_WORK_DIR:-/tmp/computehive-work}"
