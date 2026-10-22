FROM alpine:3.21
RUN adduser -D app
USER app
# A job that fails: prints to stderr and exits 3. The job's own failure is terminal (it is not retried,
# because retrying would only repeat it), so the result is FAILED with failure_reason job_exit and the exit
# code and stderr are shown by `computehive result`.
#
#   computehive submit examples/failing --wait        # exits 1 and prints the failure
#
# Contrast with an *infrastructure* failure (kill the worker mid-job: see `make chaos`), which IS retried.
CMD ["sh", "-c", "echo 'starting' ; echo 'error: this job always fails' >&2 ; exit 3"]
