# Build context is the repository root:  docker build -f worker/Dockerfile .
# (the worker module imports the coordinator module's generated protobuf code)
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY coordinator ./coordinator
COPY worker ./worker
WORKDIR /src/worker
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker

FROM docker:29-cli

RUN adduser -D -h /app worker && mkdir -p /state && chown worker /state
USER worker
WORKDIR /app

COPY --from=build /out/worker /usr/local/bin/worker

ENTRYPOINT ["worker"]
