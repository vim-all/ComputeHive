SHELL := /bin/bash
export DOCKER_GID := $(shell stat -c %g /var/run/docker.sock 2>/dev/null || echo 999)
export COMPUTEHIVE_WORK_DIR ?= /tmp/computehive-work
MODULES := coordinator worker cli
BIN := bin

.PHONY: help build proto up down logs test vet lint demo chaos bench clean

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
		shared/proto/worker.proto shared/proto/client.proto

up: ## start redis, coordinator, 2 workers, prometheus, grafana
	mkdir -p $(COMPUTEHIVE_WORK_DIR) && chmod 1777 $(COMPUTEHIVE_WORK_DIR)
	docker compose up -d --build --wait redis coordinator worker-1 worker-2 prometheus grafana
	@echo
	@echo "cluster is up:  gRPC localhost:50051   HTTP localhost:8080   Grafana http://localhost:3000   Prometheus http://localhost:9090"
	@echo "try:            make demo"

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

demo: build ## run examples/hello on the running cluster and print its result
	./$(BIN)/computehive workers
	./$(BIN)/computehive submit examples/hello --wait

chaos: ## fault-injection suite (worker/coordinator/redis kills)
	@echo "chaos suite is implemented in the scheduler-correctness phase" >&2; exit 1

bench: ## throughput / latency benchmarks
	@echo "benchmarks are implemented in the benchmarking phase" >&2; exit 1

clean:
	rm -rf $(BIN)
