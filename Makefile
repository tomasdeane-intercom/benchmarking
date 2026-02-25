# Kafka vs Sidekiq Benchmark Harness
#
# Quick reference:
#   make build              Build all Docker images
#   make smoke              Quick smoke test (100 msgs each, ~2 min)
#   make bench              Full benchmark suite (baseline mode)
#   make bench-optimized    Full benchmark suite (optimized mode)
#   make report             Re-generate report from existing results
#   make clean              Tear down all containers and volumes
#   make nuke               clean + delete result files
#
# Configuration (override via env or make args):
#   make bench MESSAGES=20000 RUNS=5 KAFKA_CONSUMERS=3

.DEFAULT_GOAL := help
SHELL := /bin/bash

# Tunables — only baseline knobs that are passed as CLI args to the orchestrator
# by the individual bench targets (kafka-bench, sidekiq-bench).
# Optimization knobs (WORKER_THREADS, SIDEKIQ_BATCH_SIZE, etc.) are owned
# entirely by run_all.sh's BENCH_MODE preset logic and must NOT be exported
# here, or Make's defaults will shadow the optimized presets.
MESSAGES        ?= 5000
WARMUP          ?= 500
RUNS            ?= 3
PARTITIONS      ?= 6
CONCURRENCY     ?= 10
KAFKA_CONSUMERS ?= 1
SIDEKIQ_WORKERS ?= 1

export MESSAGES WARMUP RUNS PARTITIONS CONCURRENCY KAFKA_CONSUMERS SIDEKIQ_WORKERS

.PHONY: help build infra smoke bench bench-optimized report clean nuke logs kafka-bench sidekiq-bench

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build:  ## Build all Docker images
	docker compose build

infra:  ## Start infrastructure only (Kafka, Redis, stub server)
	docker compose up -d zookeeper kafka redis stub-server
	@echo "Waiting for healthy services..."
	@for svc in kafka redis stub-server; do \
		printf "  $$svc: "; \
		t=120; \
		while ! docker compose ps --format json "$$svc" 2>/dev/null | grep -q '"healthy"'; do \
			sleep 2; t=$$((t-2)); \
			if [ $$t -le 0 ]; then echo "TIMEOUT"; exit 1; fi; \
		done; \
		echo "ready"; \
	done

smoke: build  ## Quick smoke test (~2 min)
	./scripts/smoke_test.sh

bench: build  ## Full benchmark suite (baseline mode)
	./scripts/run_all.sh

bench-optimized: build  ## Full benchmark suite (optimized mode)
	unset MESSAGES WARMUP RUNS PARTITIONS CONCURRENCY KAFKA_CONSUMERS SIDEKIQ_WORKERS && \
	BENCH_MODE=optimized ./scripts/run_all.sh

kafka-bench: build infra  ## Run Kafka benchmarks only
	docker compose --profile kafka up -d kafka-consumer && sleep 5
	@for wl in cpu_light io_bound fanout delayed; do \
		docker compose --profile orchestrator run --rm orchestrator \
			benchmark.py --system kafka --workload $$wl \
			--messages $(MESSAGES) --warmup $(WARMUP) --runs $(RUNS) \
			--partitions $(PARTITIONS); \
	done
	docker compose --profile kafka stop kafka-consumer

sidekiq-bench: build infra  ## Run Sidekiq benchmarks only
	docker compose --profile sidekiq up -d sidekiq-worker && sleep 5
	@for wl in cpu_light io_bound fanout delayed; do \
		docker compose --profile orchestrator run --rm orchestrator \
			benchmark.py --system sidekiq --workload $$wl \
			--messages $(MESSAGES) --warmup $(WARMUP) --runs $(RUNS) \
			--concurrency $(CONCURRENCY); \
	done
	docker compose --profile sidekiq stop sidekiq-worker

report:  ## Generate comparison report from existing results
	docker compose --profile orchestrator run --rm orchestrator report.py --dir /app/results

logs:  ## Tail logs from all running services
	docker compose logs -f --tail=50

clean:  ## Stop and remove all containers and volumes
	docker compose --profile kafka --profile sidekiq --profile orchestrator down -v --remove-orphans

nuke: clean  ## clean + delete all result files
	rm -f results/*.json results/*.md
