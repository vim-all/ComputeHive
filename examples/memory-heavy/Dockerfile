FROM alpine:3.21
RUN adduser -D app
USER app
# Allocates ALLOCATE_MB megabytes (default 200), holds them for 5 seconds, then reports.
#
#   computehive submit examples/memory-heavy --wait                      # fits its 512 MB limit: succeeds
#   computehive submit examples/memory-heavy --mem 128 --env ALLOCATE_MB=600 --wait
#                                                                        # exceeds the limit: the kernel OOM-kills it
#                                                                        # (exit 137), the worker and host are unaffected
#
# Memory is a hard limit on every job (no swap), and the coordinator will not place a job on a worker
# whose RAM would cross 90% once it is added.
LABEL computehive.cpu=1 computehive.mem=512 computehive.timeout=60
ENV ALLOCATE_MB=200
CMD ["sh", "-c", "awk -v n=$ALLOCATE_MB 'BEGIN { for (i = 0; i < n; i++) a[i] = sprintf(\"%1048576s\", \"x\"); print \"allocated \" n \" MB\"; system(\"sleep 5\"); print \"released\" }'"]
