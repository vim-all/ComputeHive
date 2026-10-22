SHELL := /bin/bash
export DOCKER_GID := $(shell stat -c %g /var/run/docker.sock 2>/dev/null || echo 999)
export COMPUTEHIVE_UID := $(shell id -u)
export COMPUTEHIVE_GID := $(shell id -g)
export COMPUTEHIVE_WORK_DIR ?= /tmp/computehive-work
MODULES := coordinator worker cli
BIN := bin

.PHONY: help build proto up enroll reset hostile down logs test vet lint demo chaos bench clean

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

build: ## build computehive (CLI) and computehive-worker into ./bin
	@mkdir -p $(BIN)
	cd cli && go build -o ../$(BIN)/computehive ./cmd/computehive
	cd worker && go build -o ../$(BIN)/computehive-worker ./cmd/worker
	cd coordinator && go build -o ../$(BIN)/computehive-coordinator ./cmd/server

proto: ## regenerate Go protobuf code from shared/proto
	protoc -I shared/proto --go_out=coordinator --go_opt=module=coordinator \
		--go-grpc_out=coordinator --go-grpc_opt=module=coordinator \
		shared/proto/worker.proto shared/proto/client.proto shared/proto/admin.proto

up: build ## start redis, coordinator, 3 workers, prometheus, grafana (TLS + auth on; secrets in .computehive/)
	chmod +x scripts/*.sh && scripts/bootstrap.sh
	@echo
	@echo "cluster is up:  gRPC localhost:50051   HTTP localhost:8080   Grafana http://localhost:3000   Prometheus http://localhost:9090"
	@echo "secrets:        .computehive/  (API token and CA; the CLI finds them automatically)"
	@echo "try:            make demo"

enroll: build ## print a one-hour, single-use enrollment token for joining another worker
	./$(BIN)/computehive admin enroll --ttl 1h --uses 1 --note "make enroll"

reset: ## stop the cluster and DELETE all its data and secrets
	docker compose --profile pressure down -v
	rm -rf .computehive

down: ## stop the cluster (data volumes are kept; use 'docker compose down -v' to wipe)
	docker compose down

logs: ## follow cluster logs
	docker compose logs -f --tail=50

vet: ## go vet in every module
	@for m in $(MODULES); do echo "== vet $$m"; (cd $$m && go vet ./...) || exit 1; done

lint: vet ## vet + staticcheck
	@for m in $(MODULES); do echo "== staticcheck $$m"; (cd $$m && go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...) || exit 1; done

test: ## unit tests with the race detector in every module
	@for m in $(MODULES); do echo "== test $$m"; (cd $$m && go test -race -count=1 ./...) || exit 1; done

demo: build ## guided real run on the cluster: jobs, live output, kill a worker mid-job, recovery (~90s)
	chmod +x scripts/demo.sh && scripts/demo.sh

chaos: build ## fault injection: in-process suite (Redis in Docker), then SIGKILL workers in the running cluster
	cd coordinator && go test -race -count=1 -timeout 20m -v ./internal/chaos/
	chmod +x scripts/chaos-cluster.sh && scripts/chaos-cluster.sh 3

hostile: ## run the hostile-job suite (needs Docker): network, host files, fork bomb, memory, CPU, disk, socket, metadata, escalation
	cd worker && go test -race -count=1 -v -timeout 15m ./internal/executor/ -run 'Hostile|Limits|Running|Exceeding|ForkBomb|TimedOut|Cleanup' && go test -race -count=1 -v -timeout 10m ./internal/netproxy/

pressure: build ## prove an overloaded worker is shown as overloaded and never receives jobs (needs `make up`)
	chmod +x scripts/pressure-cluster.sh && scripts/pressure-cluster.sh 6

bench: ## throughput / latency benchmarks
	@echo "benchmarks are implemented in the benchmarking phase" >&2; exit 1

clean:
	rm -rf $(BIN)
