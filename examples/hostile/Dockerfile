FROM alpine:3.21
RUN adduser -D app
COPY attack.sh /attack.sh
RUN chmod 755 /attack.sh
USER app
# Safe to run: it only *tries* hostile things and reports whether the sandbox stopped them (see attack.sh).
#   computehive submit examples/hostile --wait
# Expected: every line says BLOCKED and the job succeeds. If anything says ESCAPED the job fails.
LABEL computehive.cpu=1 computehive.mem=256 computehive.timeout=120
CMD ["/attack.sh"]
