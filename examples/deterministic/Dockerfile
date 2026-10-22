FROM alpine:3.21
RUN adduser -D app
USER app
# A deterministic job: the same input always gives byte-identical output on any machine, so two
# workers can be compared (`computehive submit examples/deterministic --verify --wait`).
# Writes only to the work directory; prints nothing machine-specific (no hostname, no timestamps).
CMD ["sh", "-c", "i=1; s=0; while [ $i -le 100000 ]; do s=$((s + i * i % 97)); i=$((i + 1)); done; echo \"sum=$s\"; printf 'sum,%s\\n' \"$s\" > \"$COMPUTEHIVE_OUTPUT_DIR/result.csv\""]
