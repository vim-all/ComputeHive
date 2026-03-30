# ComputeHive

ComputeHive is a decentralized compute-sharing prototype.

Current implementation focus:

- Coordinator backend in Go
- gRPC contracts in shared proto files
- Redis-backed state, queueing, and worker liveness tracking

This README documents what is implemented so far.

## Implemented So Far

### Phase 1: Coordinator server skeleton

- gRPC server bootstrapped on configurable port (default 50051)
- Redis connection initialized at startup using cloud configuration only (no localhost fallback)
- Worker and Client gRPC services registered
- Project structured with cmd, internal, and generated pb packages

### Phase 2: Core pipeline

- Worker registration
- Worker heartbeats with TTL-based liveness key
- Job submission and Redis queueing
- Worker job pull flow
- Result submission and job completion updates
- Job status and result queries from client side

End-to-end flow implemented:
Client submit -> queued in Redis -> worker pulls -> worker submits result -> client reads status and result

### Phase 3: Resilience and fairness

- Fair scheduler (round-robin with basic resource eligibility checks)
- Background worker monitor that detects dropped workers by heartbeat key expiry
- Automatic requeue of running jobs assigned to dropped workers
- Job to worker assignment mapping in Redis
- Partial result checkpoint support (minimal)
- Structured logs for important events (registration, submission, assignment, completion, disconnect)

## Repository Layout

    ComputeHive/
    ├── coordinator/
    │   ├── cmd/server/main.go
    │   ├── internal/
    │   │   ├── config/config.go
    │   │   ├── grpc/
    │   │   │   ├── server.go
    │   │   │   └── handlers.go
    │   │   ├── scheduler/scheduler.go
    │   │   └── store/redis.go
    │   ├── pkg/pb/
    │   │   ├── client.pb.go
    │   │   ├── client_grpc.pb.go
    │   │   ├── worker.pb.go
    │   │   └── worker_grpc.pb.go
    │   ├── go.mod
    │   └── go.sum
    ├── shared/
    │   └── proto/
    │       ├── client.proto
    │       └── worker.proto
    ├── worker/
    ├── client/
    ├── infra/
    ├── examples/
    └── scripts/

## gRPC Contracts

Source of truth:

- shared/proto/worker.proto
- shared/proto/client.proto

Services currently available:

- compute.v1.WorkerService
  - RegisterWorker
  - Heartbeat
  - RequestJob
  - SubmitResult
- compute.v1.ClientService
  - SubmitJob
  - GetJobStatus
  - GetJobResult

## Redis Data Model In Use

Keys used by coordinator:

- workers (SET)
- worker:{id} (STRING JSON)
- worker_alive:{id} (STRING with TTL)

- job_queue (LIST)
- job:{id} (STRING JSON)
- job_status:{id} (STRING: queued | running | completed | failed)
- job_worker:{id} (STRING: assigned worker id)
- result:{id} (STRING JSON)

## Prerequisites

- Go 1.24+
- Docker
- Cloud Redis endpoint with TLS and password
- protoc, protoc-gen-go, protoc-gen-go-grpc
- grpcurl for manual API testing

## Run Coordinator (Cloud Redis)

1. Set environment variables

   export REDIS_ADDR="<cloud-redis-host>:<port>"
   export REDIS_PASSWORD="<cloud-redis-password>"
   export REDIS_TLS=true
   export REDIS_DB=0

2. Start Coordinator

   cd coordinator
   go mod tidy
   go run cmd/server/main.go

Expected log:

- coordinator gRPC server listening on :50051

## Manual API Smoke Test

From repository root:

1. Register a worker

   grpcurl -plaintext -import-path shared/proto -proto worker.proto \
    -d '{"availableResources":{"cpuCores":4,"gpuCount":0,"memoryMb":8192},"workerVersion":"v0.1.0"}' \
    localhost:50051 compute.v1.WorkerService/RegisterWorker

2. Submit a job

   grpcurl -plaintext -import-path shared/proto -proto client.proto -proto worker.proto \
    -d '{"containerImage":"python:3.11","command":["python","-c","print(\"hello\")"],"requiredResources":{"cpuCores":1,"gpuCount":0,"memoryMb":512},"environment":{"ENV":"dev"},"maxRuntimeSeconds":30}' \
    localhost:50051 compute.v1.ClientService/SubmitJob

3. Worker requests a job

   grpcurl -plaintext -import-path shared/proto -proto worker.proto \
    -d '{"workerId":{"value":"<worker-id>"}}' \
    localhost:50051 compute.v1.WorkerService/RequestJob

4. Worker submits result

   grpcurl -plaintext -import-path shared/proto -proto worker.proto \
    -d '{"workerId":{"value":"<worker-id>"},"result":{"jobId":{"value":"<job-id>"},"status":"STATUS_SUCCEEDED","exitCode":0,"stdoutExcerpt":"done","finishedAtUnix":1735689600}}' \
    localhost:50051 compute.v1.WorkerService/SubmitResult

5. Client checks status and result

   grpcurl -plaintext -import-path shared/proto -proto client.proto -proto worker.proto \
    -d '{"jobId":{"value":"<job-id>"}}' \
    localhost:50051 compute.v1.ClientService/GetJobStatus

   grpcurl -plaintext -import-path shared/proto -proto client.proto -proto worker.proto \
    -d '{"jobId":{"value":"<job-id>"}}' \
    localhost:50051 compute.v1.ClientService/GetJobResult

## Testing Worker Drop-off Requeue

1. Register and heartbeat worker, submit and assign a job.
2. Stop sending heartbeat for that worker for more than 10 seconds.
3. Scheduler monitor detects expired worker_alive key.
4. Running jobs assigned to that worker are requeued to job_queue.
5. Another alive worker can pull the requeued job.

Check coordinator logs for lines similar to:

- worker disconnected: worker_id=... requeued_jobs=...

## Current Status

Coordinator MVP is robust enough for hackathon demos:

- Fair worker selection
- Heartbeat-based failure detection
- Requeue safety for dropped workers
- End-to-end client and worker APIs over gRPC

## Next Recommended Steps

- Add idempotency for result submission
- Add retry controls and dead-letter handling
- Add authentication and authorization
- Add metrics and dashboard service endpoints
- Implement worker execution node and integrate client UX
