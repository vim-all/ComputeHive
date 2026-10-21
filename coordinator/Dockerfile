# Build context is the repository root:  docker build -f coordinator/Dockerfile .
FROM golang:1.25-alpine AS build
WORKDIR /src/coordinator
COPY coordinator/go.mod coordinator/go.sum ./
RUN go mod download
COPY coordinator ./
RUN CGO_ENABLED=0 go build -trimpath -o /out/coordinator ./cmd/server

FROM alpine:3.21
RUN adduser -D -h /app coordinator && mkdir -p /data/artifacts && chown -R coordinator /data
USER coordinator
WORKDIR /app
COPY --from=build /out/coordinator /usr/local/bin/coordinator
ENV ARTIFACT_DIR=/data/artifacts
EXPOSE 50051 8080
ENTRYPOINT ["coordinator"]
