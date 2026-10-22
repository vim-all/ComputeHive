FROM alpine:3.21
RUN adduser -D app
USER app
# Burns CPU on two busy loops for 15 seconds, then reports. Watch the worker's CPU% in
# `computehive workers` or the Grafana "CPU used % per worker" panel while it runs.
# The container is capped at the requested 2 CPUs, so it cannot take more of the host than it asked for.
LABEL computehive.cpu=2 computehive.mem=128 computehive.timeout=60
CMD ["sh", "-c", "timeout 15 sh -c 'while :; do :; done' & timeout 15 sh -c 'while :; do :; done' & wait; echo \"burned 2 CPUs for 15s on $(hostname)\""]
