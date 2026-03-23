from __future__ import annotations

import argparse
import sys
import time

import requests

from benchkit.config import RuntimeConfig
from benchkit.kafka.admin import KafkaAdmin
from benchkit.metrics import redis_client


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for benchmark infrastructure readiness")
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    parser.add_argument(
        "--require",
        nargs="+",
        choices=["kafka", "redis", "metrics-redis", "stub-server"],
        default=["kafka", "redis", "metrics-redis", "stub-server"],
    )
    args = parser.parse_args()

    runtime = RuntimeConfig()
    kafka_admin = KafkaAdmin(runtime.kafka_bootstrap_servers) if "kafka" in args.require else None
    deadline = time.time() + args.timeout_seconds
    last_error = "unknown error"

    while time.time() < deadline:
        try:
            if kafka_admin is not None and not kafka_admin.cluster_ready(timeout=2):
                raise RuntimeError("Kafka cluster metadata not ready")
            if "redis" in args.require:
                redis_client(runtime.redis_url).ping()
            if "metrics-redis" in args.require:
                redis_client(runtime.metrics_redis_url).ping()
            if "stub-server" in args.require:
                response = requests.get(f"{runtime.stub_server_url}/delay/0", timeout=5)
                response.raise_for_status()
            print("infrastructure ready")
            return 0
        except Exception as exc:
            last_error = str(exc)
            time.sleep(1.0)

    print(f"infrastructure not ready: {last_error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
