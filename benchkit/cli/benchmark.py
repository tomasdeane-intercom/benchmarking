from __future__ import annotations

import argparse
import json
from pathlib import Path

from benchkit.logging import configure_logging
from benchkit.suite_execution import prepare_condition, run_condition


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run benchmark conditions")
    parser.add_argument("--suite-id", required=True)
    parser.add_argument("--condition-id", required=True)
    parser.add_argument("--variable-under-test", required=True)
    parser.add_argument("--system", choices=["kafka", "sidekiq"], required=True)
    parser.add_argument("--workload", choices=["cpu_light", "io_bound", "bursty_ingest", "delayed"], required=True)
    parser.add_argument("--messages", type=int, required=True)
    parser.add_argument("--warmup", type=int, required=True)
    parser.add_argument("--topic", default="benchmark")
    parser.add_argument("--consumer-group", default="benchmark-group")
    parser.add_argument("--queue-name", default="benchmark")
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--replicas", type=int, required=True)
    parser.add_argument("--worker-threads", type=int, required=True)
    parser.add_argument("--burst-size", type=int)
    parser.add_argument("--inter-burst-ms", type=float)
    parser.add_argument("--produce-shape", default="sequential")
    parser.add_argument("--delay-seconds", type=float)
    parser.add_argument("--kafka-compression", default="none")
    parser.add_argument("--kafka-acks", default="all")
    parser.add_argument("--clean-state-mode", choices=["full", "skip"], default="full")
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument("--output-dir", default="results/final")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--repeat-index", type=int, default=1)
    return parser


def main() -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    if args.prepare_only:
        prepare_condition(args)
        return 0
    result = run_condition(args, output_root=Path(args.output_dir))
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
