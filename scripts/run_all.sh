#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Kafka vs Sidekiq — Full Benchmark Suite
#
# Usage:
#   ./scripts/run_all.sh                              # baseline defaults
#   BENCH_MODE=optimized ./scripts/run_all.sh         # optimized presets
#   MESSAGES=20000 RUNS=5 ./scripts/run_all.sh        # custom overrides

BENCH_MODE="${BENCH_MODE:-baseline}"

# Mode presets
# Optimized presets can be overridden individually via env vars.
if [ "$BENCH_MODE" = "optimized" ]; then
    MESSAGES="${MESSAGES:-10000}"
    WARMUP="${WARMUP:-500}"
    RUNS="${RUNS:-3}"
    PARTITIONS="${PARTITIONS:-12}"
    CONCURRENCY="${CONCURRENCY:-25}"
    KAFKA_CONSUMERS="${KAFKA_CONSUMERS:-4}"
    SIDEKIQ_WORKERS="${SIDEKIQ_WORKERS:-4}"
    WORKER_THREADS="${WORKER_THREADS:-25}"
    MAX_PENDING="${MAX_PENDING:-50}"
    FETCH_MIN_BYTES="${FETCH_MIN_BYTES:-16384}"
    FETCH_MAX_WAIT_MS="${FETCH_MAX_WAIT_MS:-100}"
    AUTO_COMMIT_INTERVAL_MS="${AUTO_COMMIT_INTERVAL_MS:-5000}"
    SIDEKIQ_BATCH_SIZE="${SIDEKIQ_BATCH_SIZE:-100}"
    STUB_WORKERS="${STUB_WORKERS:-32}"
    REDIS_IO_THREADS="${REDIS_IO_THREADS:-4}"
    KAFKA_NUM_IO_THREADS="${KAFKA_NUM_IO_THREADS:-16}"
    KAFKA_NUM_NETWORK_THREADS="${KAFKA_NUM_NETWORK_THREADS:-6}"
    KAFKA_COMPRESSION="${KAFKA_COMPRESSION:-snappy}"
    KAFKA_ACKS="${KAFKA_ACKS:-1}"
    METRICS_POOL_SIZE="${METRICS_POOL_SIZE:-10}"
    HTTP_POOL_SIZE="${HTTP_POOL_SIZE:-25}"
else
    MESSAGES="${MESSAGES:-5000}"
    WARMUP="${WARMUP:-500}"
    RUNS="${RUNS:-3}"
    PARTITIONS="${PARTITIONS:-6}"
    CONCURRENCY="${CONCURRENCY:-10}"
    KAFKA_CONSUMERS="${KAFKA_CONSUMERS:-1}"
    SIDEKIQ_WORKERS="${SIDEKIQ_WORKERS:-1}"
    WORKER_THREADS="${WORKER_THREADS:-1}"
    MAX_PENDING="${MAX_PENDING:-2}"
    FETCH_MIN_BYTES="${FETCH_MIN_BYTES:-1}"
    FETCH_MAX_WAIT_MS="${FETCH_MAX_WAIT_MS:-500}"
    AUTO_COMMIT_INTERVAL_MS="${AUTO_COMMIT_INTERVAL_MS:-1000}"
    SIDEKIQ_BATCH_SIZE="${SIDEKIQ_BATCH_SIZE:-1}"
    STUB_WORKERS="${STUB_WORKERS:-4}"
    REDIS_IO_THREADS="${REDIS_IO_THREADS:-1}"
    KAFKA_NUM_IO_THREADS="${KAFKA_NUM_IO_THREADS:-8}"
    KAFKA_NUM_NETWORK_THREADS="${KAFKA_NUM_NETWORK_THREADS:-3}"
    KAFKA_COMPRESSION="${KAFKA_COMPRESSION:-none}"
    KAFKA_ACKS="${KAFKA_ACKS:-all}"
    METRICS_POOL_SIZE="${METRICS_POOL_SIZE:-5}"
    HTTP_POOL_SIZE="${HTTP_POOL_SIZE:-10}"
fi

export BENCH_MODE MESSAGES WARMUP RUNS PARTITIONS CONCURRENCY
export KAFKA_CONSUMERS SIDEKIQ_WORKERS
export WORKER_THREADS MAX_PENDING FETCH_MIN_BYTES FETCH_MAX_WAIT_MS AUTO_COMMIT_INTERVAL_MS
export SIDEKIQ_BATCH_SIZE STUB_WORKERS REDIS_IO_THREADS
export KAFKA_NUM_IO_THREADS KAFKA_NUM_NETWORK_THREADS KAFKA_COMPRESSION KAFKA_ACKS
export METRICS_POOL_SIZE HTTP_POOL_SIZE

STATS_FILE="results/docker_stats_${BENCH_MODE}_$(date +%Y%m%d_%H%M%S).csv"
STATS_PID=""

banner() { printf '\n\033[1;36m══ %s ══\033[0m\n\n' "$*"; }

wait_healthy() {
    local svc="$1" max_wait="${2:-120}"
    echo -n "  $svc: "
    local elapsed=0
    while ! docker compose ps --format json "$svc" 2>/dev/null | grep -q '"healthy"'; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$max_wait" ]; then
            echo "TIMEOUT after ${max_wait}s"
            docker compose logs "$svc" 2>&1 | tail -20
            exit 1
        fi
    done
    echo "ready"
}

cleanup() {
    banner "Cleaning up"
    [ -n "$STATS_PID" ] && kill "$STATS_PID" 2>/dev/null && wait "$STATS_PID" 2>/dev/null || true
    docker compose --profile kafka --profile sidekiq --profile orchestrator down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

run_bench() {
    local system="$1" workload="$2"
    shift 2
    banner "$system / $workload"
    docker compose --profile orchestrator run --rm orchestrator \
        benchmark.py \
        --system "$system" \
        --workload "$workload" \
        --messages "$MESSAGES" \
        --warmup "$WARMUP" \
        --runs "$RUNS" \
        --partitions "$PARTITIONS" \
        --concurrency "$CONCURRENCY" \
        "$@"
}

# 0. Print configuration
banner "Configuration (BENCH_MODE=${BENCH_MODE})"
cat <<EOF
  Messages:        ${MESSAGES}   Warmup: ${WARMUP}   Runs: ${RUNS}
  Kafka consumers: ${KAFKA_CONSUMERS}   Partitions: ${PARTITIONS}
  Worker threads:  ${WORKER_THREADS}   Max pending: ${MAX_PENDING}
  Sidekiq workers: ${SIDEKIQ_WORKERS}   Concurrency: ${CONCURRENCY}
  Batch size:      ${SIDEKIQ_BATCH_SIZE}
  Stub workers:    ${STUB_WORKERS}   Redis IO threads: ${REDIS_IO_THREADS}
  Kafka compress:  ${KAFKA_COMPRESSION}   Kafka acks: ${KAFKA_ACKS}
EOF

# 1. Build images
banner "Building images"
docker compose --profile kafka --profile sidekiq --profile orchestrator build

# 2. Start infrastructure 
banner "Starting infrastructure (Kafka, Redis, stub server)"
docker compose up -d zookeeper kafka redis stub-server

echo "Waiting for services to become healthy..."
wait_healthy redis 30
wait_healthy stub-server 30
wait_healthy kafka 120

# 3. Start resource monitoring 
banner "Starting resource monitoring → $STATS_FILE"
./scripts/collect_stats.sh "$STATS_FILE" &
STATS_PID=$!
sleep 2

# 4. Kafka benchmarks 
banner "Starting Kafka consumers (×${KAFKA_CONSUMERS}, threads=${WORKER_THREADS})"
docker compose --profile kafka up -d kafka-consumer
sleep 5

for wl in cpu_light io_bound fanout delayed; do
    run_bench kafka "$wl"
done

docker compose --profile kafka stop kafka-consumer

# 5. Sidekiq benchmarks 
banner "Starting Sidekiq workers (×${SIDEKIQ_WORKERS}, concurrency=${CONCURRENCY})"
docker compose --profile sidekiq up -d sidekiq-worker
sleep 5

for wl in cpu_light io_bound fanout delayed; do
    run_bench sidekiq "$wl"
done

docker compose --profile sidekiq stop sidekiq-worker

# 6. Stop stats, generate report 
kill "$STATS_PID" 2>/dev/null && wait "$STATS_PID" 2>/dev/null || true
STATS_PID=""

banner "Generating comparison report"
docker compose --profile orchestrator run --rm orchestrator report.py --dir /app/results

banner "Done!  Results in ./results/"
ls -lh results/
