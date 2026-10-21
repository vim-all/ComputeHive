FROM alpine:3.21
RUN adduser -D app
USER app
# Runs for SLEEP_SECONDS (default 30): long enough to kill the worker underneath it.
CMD ["sh", "-c", "sleep ${SLEEP_SECONDS:-30}; echo \"slept ${SLEEP_SECONDS:-30}s on $(hostname)\""]
