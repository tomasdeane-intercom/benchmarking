from __future__ import annotations

from math import floor
from statistics import mean, median, stdev
from typing import Iterable


def percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    n = len(sorted_values)
    k = (n - 1) * (pct / 100.0)
    f = floor(k)
    c = f + 1
    if c >= n:
        return sorted_values[f]
    return sorted_values[f] + (k - f) * (sorted_values[c] - sorted_values[f])


def summary(values: Iterable[float], include_stddev: bool = False) -> dict[str, float | None]:
    series = sorted(float(value) for value in values)
    result = {
        "min": None,
        "max": None,
        "mean": None,
        "median": None,
        "p95": None,
        "p99": None,
    }
    if include_stddev:
        result["stddev"] = None
    if not series:
        return result
    result.update(
        {
            "min": _round(series[0]),
            "max": _round(series[-1]),
            "mean": _round(mean(series)),
            "median": _round(median(series)),
            "p95": _round(percentile(series, 95)),
            "p99": _round(percentile(series, 99)),
        }
    )
    if include_stddev:
        result["stddev"] = _round(sample_stddev(series))
    return result


def sample_stddev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    return float(stdev(values))


def compute_run_statistics(
    events: list[dict],
    error_records: list[dict],
    expected_messages: int,
    produce_start: float,
    produce_end: float,
    workload: str,
) -> dict:
    successful = sum(1 for event in events if event.get("success", False))
    logical_failures = sum(1 for event in events if not event.get("success", False))
    exception_failures = len(error_records)
    failed = logical_failures + exception_failures
    accounted_messages = len(events) + exception_failures
    latency = [float(event["latency_ms"]) for event in events]
    wait_to_start = [
        (float(event["dequeue_time"]) - float(event["enqueue_time"])) * 1000.0 for event in events
    ]
    processing = [
        (
            float(event["complete_time"])
            - float(event.get("work_start_time", event["dequeue_time"]))
        )
        * 1000.0
        for event in events
    ]

    if events:
        first_enqueue = min(float(event["enqueue_time"]) for event in events)
        last_complete = max(float(event["complete_time"]) for event in events)
    else:
        first_enqueue = produce_start
        last_complete = produce_end

    total_wall = last_complete - first_enqueue
    drain = last_complete - produce_end
    produce = produce_end - produce_start

    statistics = {
        "expected_messages": expected_messages,
        "accounted_messages": accounted_messages,
        "total_messages": len(events),
        "successful": successful,
        "logical_failures": logical_failures,
        "exception_failures": exception_failures,
        "failed": failed,
        "error_rate_pct": _round(100.0 * failed / max(accounted_messages, 1)),
        "latency_ms": summary(latency, include_stddev=True),
        "wait_to_start_ms": summary(wait_to_start, include_stddev=True),
        "processing_ms": summary(processing, include_stddev=False),
        "throughput": {
            "consumed_msg_per_sec": _round(successful / total_wall) if total_wall > 0 else 0.0,
            "produce_msg_per_sec": _round(successful / produce) if produce > 0 else 0.0,
        },
        "timing_s": {
            "total_wall": _round_timing(total_wall),
            "produce": _round_timing(produce),
            "drain": _round_timing(drain),
        },
    }

    if workload == "delayed":
        start_lateness = [
            (
                float(event.get("work_start_time", event["dequeue_time"]))
                - float(event["target_time"])
            )
            * 1000.0
            for event in events
            if event.get("target_time") is not None
        ]
        completion_lateness = [
            (float(event["complete_time"]) - float(event["target_time"])) * 1000.0
            for event in events
            if event.get("target_time") is not None
        ]
        statistics["start_lateness_ms"] = summary(start_lateness, include_stddev=True)
        statistics["completion_lateness_ms"] = summary(
            completion_lateness, include_stddev=True
        )
        statistics["scheduling"] = {
            "total_scheduled": len(start_lateness),
            "early_start_count": sum(1 for value in start_lateness if value < 0),
            "on_time_pct_100ms": _round(_pct_within(start_lateness, 100.0)),
            "on_time_pct_500ms": _round(_pct_within(start_lateness, 500.0)),
            "jitter_ms": _round(sample_stddev(start_lateness)),
        }

    return statistics


def _pct_within(values: list[float], threshold_ms: float) -> float:
    if not values:
        return 0.0
    within = sum(1 for value in values if abs(value) <= threshold_ms)
    return 100.0 * within / len(values)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _round_timing(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 4)
