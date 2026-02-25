#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# ──────────────────────────────────────────────────────────────────────────
# Quick smoke test — 100 messages through each system, 1 run, no warmup.
# Useful for verifying everything works before launching a full suite.
# ──────────────────────────────────────────────────────────────────────────

banner() { printf '\n\033[1;36m══ %s ══\033[0m\n\n' "$*"; }

cleanup() {
    banner "Cleaning up"
    docker compose --profile kafka --profile sidekiq --profile orchestrator down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

banner "Building images"
docker compose build

banner "Starting infrastructure"
docker compose up -d zookeeper kafka redis stub-server

echo "Waiting for services to become healthy..."
for svc in kafka redis stub-server; do
    echo -n "  $svc: "
    timeout=120
    while ! docker compose ps --format json "$svc" 2>/dev/null | grep -q '"healthy"'; do
        sleep 2
        timeout=$((timeout - 2))
        if [ "$timeout" -le 0 ]; then
            echo "TIMEOUT — logs:"
            docker compose logs "$svc" | tail -20
            exit 1
        fi
    done
    echo "healthy"
done

# Kafka smoke
banner "Kafka smoke test (100 cpu_light messages)"
docker compose --profile kafka up -d kafka-consumer
sleep 5
docker compose --profile orchestrator run --rm orchestrator \
    benchmark.py --system kafka --workload cpu_light --messages 100 --warmup 0 --runs 1
docker compose --profile kafka stop kafka-consumer

# Sidekiq smoke
banner "Sidekiq smoke test (100 cpu_light messages)"
docker compose --profile sidekiq up -d sidekiq-worker
sleep 5
docker compose --profile orchestrator run --rm orchestrator \
    benchmark.py --system sidekiq --workload cpu_light --messages 100 --warmup 0 --runs 1
docker compose --profile sidekiq stop sidekiq-worker

# Report
banner "Generating report"
docker compose --profile orchestrator run --rm orchestrator report.py --dir /app/results

banner "Smoke test passed!"
