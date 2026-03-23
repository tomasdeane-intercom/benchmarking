from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class RuntimeConfig:
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: _env_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    redis_url: str = field(default_factory=lambda: _env_str("REDIS_URL", "redis://redis:6379/0"))
    metrics_redis_url: str = field(
        default_factory=lambda: _env_str("METRICS_REDIS_URL", "redis://metrics-redis:6379/0")
    )
    stub_server_url: str = field(
        default_factory=lambda: _env_str("STUB_SERVER_URL", "http://stub-server:8080")
    )
    stub_server_workers: int = field(
        default_factory=lambda: _env_int("STUB_SERVER_WORKERS", 20)
    )
    topic: str = field(default_factory=lambda: _env_str("KAFKA_TOPIC", "benchmark"))
    consumer_group: str = field(
        default_factory=lambda: _env_str("KAFKA_GROUP_ID", "benchmark-group")
    )
    queue_name: str = field(default_factory=lambda: _env_str("BENCHMARK_QUEUE", "benchmark"))
    compose_project: str = field(
        default_factory=lambda: _env_str("COMPOSE_PROJECT_NAME", "benchmarking")
    )
