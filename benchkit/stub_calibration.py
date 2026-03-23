from __future__ import annotations

import concurrent.futures
import json
from pathlib import Path
from time import perf_counter

import requests

from benchkit.schema_validation import validate_stub_calibration
from benchkit.statistics import summary
from benchkit.timing import utc_now


def run_stub_calibration(
    stub_url: str,
    output_path: Path,
    concurrency_sweep: list[int],
    requests_per_worker: int,
    delay_ms: int,
    stub_workers: int,
) -> dict:
    results = []
    for concurrency in concurrency_sweep:
        samples = _run_calibration_batch(stub_url, concurrency, requests_per_worker, delay_ms)
        latencies = [sample["latency_ms"] for sample in samples]
        wall_time = max(sample["complete_s"] for sample in samples)
        successful = sum(1 for sample in samples if sample["ok"])
        errors = len(samples) - successful
        results.append(
            {
                "concurrency": concurrency,
                "total_requests": concurrency * requests_per_worker,
                "successful": successful,
                "errors": errors,
                "throughput_rps": round(successful / max(wall_time, 1e-9), 2),
                "wall_time_s": round(wall_time, 3),
                "latency_ms": summary(latencies, include_stddev=False),
            }
        )
    artifact = {
        "type": "stub_capacity_calibration",
        "timestamp": utc_now().isoformat(),
        "config": {
            "stub_url": stub_url,
            "stub_workers": stub_workers,
            "delay_ms": delay_ms,
            "requests_per_worker": requests_per_worker,
        },
        "results": results,
        "theoretical_max_rps": round((1000.0 / max(delay_ms, 1)) * stub_workers, 2),
    }
    validate_stub_calibration(artifact)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    return artifact


def _run_calibration_batch(
    stub_url: str, concurrency: int, requests_per_worker: int, delay_ms: int
) -> list[dict]:
    samples = []
    start = perf_counter()

    def one_request() -> dict:
        request_start = perf_counter()
        ok = False
        try:
            response = requests.get(f"{stub_url}/delay/{delay_ms}", timeout=30)
            response.raise_for_status()
            ok = True
        finally:
            request_end = perf_counter()
        return {
            "ok": ok,
            "latency_ms": (request_end - request_start) * 1000.0,
            "complete_s": request_end - start,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(one_request) for _ in range(concurrency * requests_per_worker)]
        for future in concurrent.futures.as_completed(futures):
            samples.append(future.result())
    return samples
