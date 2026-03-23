from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Iterable, Literal

import requests


@dataclass(frozen=True)
class WorkloadSpec:
    operation: Literal["sha256_rounds", "http_delay_get"]
    seed_prefix: str = ""
    iterations: int = 0
    delay_ms: int = 0
    sleep_until_target: bool = False


WORKLOAD_SPECS = {
    "cpu_light": WorkloadSpec(operation="sha256_rounds", seed_prefix="benchmark", iterations=1000),
    "io_bound": WorkloadSpec(operation="http_delay_get", delay_ms=50),
    "bursty_ingest": WorkloadSpec(
        operation="sha256_rounds",
        seed_prefix="bursty_ingest",
        iterations=100,
    ),
    "delayed": WorkloadSpec(
        operation="sha256_rounds",
        seed_prefix="delayed",
        iterations=100,
        sleep_until_target=True,
    ),
}


def spec_for(workload: str) -> WorkloadSpec:
    return WORKLOAD_SPECS[workload]


def build_message(
    run_id: str,
    workload: str,
    message_index: int,
    enqueue_time: float,
    target_time: float | None = None,
) -> dict:
    message = {
        "id": f"{run_id}-{message_index}",
        "workload": workload,
        "enqueue_time": enqueue_time,
        "run_id": run_id,
    }
    if target_time is not None:
        message["target_time"] = target_time
    return message


def iter_produce_plan(
    run_id: str,
    workload: str,
    messages: int,
    produce_shape: str,
    burst_size: int | None = None,
    inter_burst_ms: float | None = None,
    delay_seconds: float | None = None,
) -> Iterable[tuple[dict, float]]:
    now = time.time()
    targets = _delayed_targets(now, messages, delay_seconds) if workload == "delayed" else [None] * messages
    if produce_shape in {"sequential", "sequential_scheduled"}:
        for index, target in enumerate(targets):
            yield build_message(run_id, workload, index, time.time(), target), 0.0
        return

    burst = burst_size or 1
    gap_seconds = (inter_burst_ms or 0.0) / 1000.0
    for index, target in enumerate(targets):
        sleep_after = gap_seconds if (index + 1) % burst == 0 and index + 1 < messages else 0.0
        yield build_message(run_id, workload, index, time.time(), target), sleep_after


def execute_message(message: dict, stub_server_url: str, timeout: float = 30.0) -> dict:
    dequeue_time = time.time()
    spec = spec_for(message["workload"])
    work_start_time = None
    if spec.sleep_until_target and message.get("target_time") is not None:
        remaining = float(message["target_time"]) - time.time()
        if remaining > 0:
            time.sleep(remaining)
        work_start_time = time.time()

    match spec.operation:
        case "sha256_rounds":
            _sha256_rounds(spec.seed_prefix, message["id"], spec.iterations)
        case "http_delay_get":
            response = requests.get(f"{stub_server_url}/delay/{spec.delay_ms}", timeout=timeout)
            response.raise_for_status()
        case _:
            raise AssertionError(spec.operation)

    complete_time = time.time()
    record = {
        "id": message["id"],
        "system": message.get("system", ""),
        "workload": message["workload"],
        "run_id": message["run_id"],
        "enqueue_time": float(message["enqueue_time"]),
        "dequeue_time": dequeue_time,
        "complete_time": complete_time,
        "latency_ms": (complete_time - float(message["enqueue_time"])) * 1000.0,
        "processing_ms": (complete_time - (work_start_time or dequeue_time)) * 1000.0,
        "success": True,
    }
    if message.get("target_time") is not None:
        record["target_time"] = float(message["target_time"])
    if work_start_time is not None:
        record["work_start_time"] = work_start_time
    return record


def make_error_record(exc: Exception) -> dict:
    return {"_type": "error", "error": str(exc), "time": time.time()}


def message_json(message: dict) -> str:
    return json.dumps(message, sort_keys=True)


def _sha256_rounds(seed_prefix: str, message_id: str, iterations: int) -> None:
    data = f"{seed_prefix}-{message_id}".encode("utf-8")
    for _ in range(iterations):
        data = hashlib.sha256(data).hexdigest().encode("utf-8")


def _delayed_targets(now: float, messages: int, delay_seconds: float | None) -> list[float]:
    spread = float(delay_seconds or 0.0)
    return [now + spread * index / max(messages - 1, 1) for index in range(messages)]
