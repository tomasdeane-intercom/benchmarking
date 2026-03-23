from __future__ import annotations

import csv
import json
import re
from bisect import bisect_left
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median

from benchkit.docker_stats import DEFAULT_DOCKER_STATS_INTERVAL_SECONDS
from benchkit.result_store import list_result_files
from benchkit.timing import parse_iso


ROLE_PATTERNS = (
    (re.compile(r".*-kafka-\d+$"), "kafka_broker"),
    (re.compile(r".*-kafka-consumer-\d+$"), "kafka_consumer"),
    (re.compile(r".*-redis-\d+$"), "redis_broker"),
    (re.compile(r".*-sidekiq-worker-\d+$"), "sidekiq_worker"),
)
SYSTEM_UNDER_TEST_ROLES = {
    "kafka": {"kafka_broker", "kafka_consumer"},
    "sidekiq": {"redis_broker", "sidekiq_worker"},
}
FALLBACK_PAD_MULTIPLIER = 1.25


def load_stats_rows(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def infer_role(container_name: str) -> str | None:
    for pattern, role in ROLE_PATTERNS:
        if pattern.fullmatch(container_name):
            return role
    return None


def aggregate_resource_usage(
    rows: list[dict],
    run_start: datetime,
    run_end: datetime,
    *,
    system: str,
    workload: str,
    compose_project: str | None = None,
) -> dict | None:
    del workload
    relevant_roles = SYSTEM_UNDER_TEST_ROLES[system]
    buckets = _relevant_buckets(rows, relevant_roles, compose_project)
    if not buckets:
        return None

    sample_times = sorted(buckets)
    selected = [timestamp for timestamp in sample_times if run_start <= timestamp <= run_end]
    if not selected:
        selected = _fallback_sample_times(sample_times, run_start, run_end)
    if not selected:
        return None

    samples: dict[str, dict[str, list[float]]] = defaultdict(lambda: {"cpu_pct": [], "mem_mb": []})
    for timestamp in selected:
        for role, values in buckets[timestamp].items():
            samples[role]["cpu_pct"].append(values["cpu_pct"])
            samples[role]["mem_mb"].append(values["mem_mb"])
    return {
        "samples": len(selected),
        "by_role": {
            role: {
                "cpu_pct": _metric_summary(values["cpu_pct"]),
                "mem_mb": _metric_summary(values["mem_mb"]),
            }
            for role, values in samples.items()
        },
    }


def enrich_results_from_csv(
    csv_path: Path,
    results_dir: Path,
    compose_project: str | None = None,
) -> list[Path]:
    rows = load_stats_rows(csv_path)
    enriched: list[Path] = []
    for path in list_result_files(results_dir):
        data = json.loads(path.read_text(encoding="utf-8"))
        usage = aggregate_resource_usage(
            rows,
            parse_iso(data["run_start_utc"]),
            parse_iso(data["run_end_utc"]),
            system=data["system"],
            workload=data["workload"],
            compose_project=compose_project,
        )
        if usage is None:
            continue
        data["resource_usage"] = usage
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        enriched.append(path)
    return enriched


def _relevant_buckets(
    rows: list[dict],
    relevant_roles: set[str],
    compose_project: str | None,
) -> dict[datetime, dict[str, dict[str, float]]]:
    buckets: dict[datetime, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"cpu_pct": 0.0, "mem_mb": 0.0})
    )
    prefix = f"{compose_project}-" if compose_project else None
    for row in rows:
        container = row["container"]
        if prefix and not container.startswith(prefix):
            continue
        role = infer_role(container)
        if role not in relevant_roles:
            continue
        slot = buckets[_parse_stats_time(row["timestamp"])][role]
        slot["cpu_pct"] += float(row["cpu_pct"])
        slot["mem_mb"] += float(row["mem_usage_mb"])
    return dict(buckets)


def _metric_summary(values: list[float]) -> dict[str, float]:
    return {
        "mean": round(sum(values) / len(values), 2),
        "peak": round(max(values), 2),
    }


def _fallback_sample_times(
    sample_times: list[datetime],
    run_start: datetime,
    run_end: datetime,
) -> list[datetime]:
    if not sample_times:
        return []
    pad_seconds = _sampling_pad_seconds(sample_times)
    selected: list[datetime] = []

    start_index = bisect_left(sample_times, run_start)
    if start_index > 0:
        previous = sample_times[start_index - 1]
        if (run_start - previous).total_seconds() <= pad_seconds:
            selected.append(previous)

    end_index = bisect_left(sample_times, run_end)
    if end_index < len(sample_times):
        following = sample_times[end_index]
        if (following - run_end).total_seconds() <= pad_seconds:
            selected.append(following)

    return sorted(set(selected))


def _sampling_pad_seconds(sample_times: list[datetime]) -> float:
    deltas = [
        (right - left).total_seconds()
        for left, right in zip(sample_times, sample_times[1:])
        if right > left
    ]
    if not deltas:
        return DEFAULT_DOCKER_STATS_INTERVAL_SECONDS
    return max(DEFAULT_DOCKER_STATS_INTERVAL_SECONDS, median(deltas) * FALLBACK_PAD_MULTIPLIER)


def _parse_stats_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
