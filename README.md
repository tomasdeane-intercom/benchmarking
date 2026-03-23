# Kafka vs Sidekiq Single-Host Benchmark Harness

This repository benchmarks the Kafka KRaft and Sidekiq stacks configured here on a single host under controlled workloads and matched nominal operating points. Redis and the stub server stay fixed across benchmark conditions.

Kafka runs as a single-node KRaft deployment with replication factor `1`. Sidekiq jobs use `retry: false`. Resource usage is system-under-test context only and is omitted from report conclusions when paired comparative runs do not meet the minimum sample threshold.

## Scope

- CPU-light work is highly sensitive to worker/process topology under the chosen nominal targets
- I/O-bound work converges once the external 50 ms dependency dominates
- shock-burst drain behaviour separates the systems more clearly than gentler ingest shapes
- delayed execution is an asymmetric architectural case study, not a symmetric throughput contest

## Suites

`cpu_light_matched_concurrency` compares matched nominal concurrency in a single-container `1 x N` layout. `parallelism_decomposition` shows how redistributing that target across replicas and in process threads changes the result.

`io_bound_matched_concurrency` compares matched nominal concurrency against the calibrated stub ceiling. At this scale the external 50 ms dependency is the main limit.

`bursty_ingest_shape` compares producer shapes using drain time and wait-to-start.

`delayed_case_study` compares Kafka consumer-side sleep-until-target behaviour with Sidekiq native scheduled jobs. Target times are distributed across the 30 s window.

## Operator Workflow

The supported operator surface is the `Makefile`. It uses host Python via `uv` and Docker for infrastructure and workers.

Prerequisites:

- Docker with Compose plugin
- `uv`

Fresh clone workflow:

```bash
make run
```

Supported commands:

- `make setup` validates Docker and Compose access
- `make run` runs the full benchmark suite
- `make calibrate` runs stub calibration and writes artifacts to `results/calibration/`
- `make verify` verifies stored benchmark results in `results/final/`
- `make report` generates `results/report/report.md` and chart PNGs in `results/report/charts/`
- `make smoke` runs the fast smoke benchmark
- `make test` runs `pytest`
- `make logs` tails benchmark logs
- `make clean` stops benchmark containers
- `make reset` removes benchmark containers and volumes
- `make purge` removes containers, volumes, images, and benchmark artifacts

## Repository Shape

Most logic lives in the `benchkit` Python package:

- manifest loading and deterministic suite expansion
- canonical workload contract and message generation
- Kafka and Sidekiq producers
- statistics, schemas, persistence, resource aggregation, reporting, and verification

Ruby is limited to the Sidekiq worker adapter in [`sidekiq_worker/worker.rb`](sidekiq_worker/worker.rb). The `Makefile` provides the operator interface.

## Results Layout

Measured runs write to `results/final/`:

- raw result JSON files
- trace gzip JSONL files in `results/final/traces/`
- aggregate repeat files `agg-*.json`

Stub calibration writes to `results/calibration/`. Reports write to `results/report/report.md` and `results/report/charts/`.

Smoke runs write to `results/smoke/` and are excluded from the main report by default.

Within each result, `statistics.total_messages` means all terminal non-error trace records for the run. `successful` and `logical_failures` partition that set, while `exception_failures` counts separate `_type: "error"` trace records.

## Canonical Contracts

Canonical contracts live in:

1. `canonical/experiment_manifest.yaml`
2. `canonical/*.json`

The benchmark runner and suite config reader use the canonical manifest directly. Raw results validate against the canonical `3.5.0` schema.

## Tests

Run tests with:

```bash
make test
```

Equivalent direct command:

```bash
uv run --extra test pytest
```

The tests focus on manifest expansion, workload semantics, schemas, statistics, report generation, verification, and resource aggregation.
