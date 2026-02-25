#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# ──────────────────────────────────────────────────────────────────────────
# Run a single benchmark scenario.
#
# Usage:
#   ./scripts/run_scenario.sh kafka   cpu_light  [extra args...]
#   ./scripts/run_scenario.sh sidekiq io_bound   --messages 2000 --runs 1
# ──────────────────────────────────────────────────────────────────────────

SYSTEM="${1:?Usage: $0 <kafka|sidekiq> <workload> [extra args...]}"
WORKLOAD="${2:?Usage: $0 <kafka|sidekiq> <workload> [extra args...]}"
shift 2

banner() { printf '\n\033[1;36m══ %s ══\033[0m\n\n' "$*"; }

# Ensure infrastructure is up
if ! docker compose ps kafka --format json 2>/dev/null | grep -q '"running"'; then
    banner "Starting infrastructure"
    docker compose up -d zookeeper kafka redis stub-server
    echo "Waiting for services..."
    sleep 30
fi

# Start appropriate workers
if [ "$SYSTEM" = "kafka" ]; then
    banner "Ensuring Kafka consumer is running"
    docker compose --profile kafka up -d kafka-consumer
    sleep 3
elif [ "$SYSTEM" = "sidekiq" ]; then
    banner "Ensuring Sidekiq worker is running"
    docker compose --profile sidekiq up -d sidekiq-worker
    sleep 3
fi

# Run benchmark
banner "$SYSTEM / $WORKLOAD"
docker compose --profile orchestrator run --rm orchestrator \
    benchmark.py \
    --system "$SYSTEM" \
    --workload "$WORKLOAD" \
    "$@"
