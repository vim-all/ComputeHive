FROM alpine:3.21
RUN adduser -D app
USER app
CMD ["sh", "-c", "echo \"hello from $(hostname) at $(date -u +%Y-%m-%dT%H:%M:%SZ)\""]
