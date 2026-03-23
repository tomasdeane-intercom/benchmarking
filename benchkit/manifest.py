from __future__ import annotations

from typing import Any

import yaml

from benchkit.paths import MANIFEST_PATH


def load_manifest(path=MANIFEST_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def list_suites(path=MANIFEST_PATH) -> list[dict[str, Any]]:
    suites = load_manifest(path)["suites"]
    return [
        {
            "suite_id": suite_id,
            "type": suite["type"],
            "workload": suite["workload"],
            "systems": suite["systems"],
            "description": " ".join(suite["description"].split()),
        }
        for suite_id, suite in suites.items()
    ]


def expand_suite(suite_id: str, path=MANIFEST_PATH) -> list[dict[str, Any]]:
    manifest = load_manifest(path)
    suite = manifest["suites"][suite_id]
    base = manifest["defaults"] | suite.get("overrides", {})
    rows: list[tuple[str, str, dict[str, Any]]] = []

    match suite_id:
        case "kafka_partition_adequacy":
            for partitions in suite["matrix"]["partitions"]:
                rows.append(
                    (
                        "kafka",
                        f"partitions={partitions}",
                        {
                            "partitions": partitions,
                            "concurrency": base["kafka_consumers"] * base["worker_threads"],
                        },
                    )
                )
        case "parallelism_decomposition":
            for system in suite["systems"]:
                for profile in suite["profiles"]:
                    rows.append(
                        (
                            system,
                            f"concurrency_profile={profile['name']}",
                            {
                                "worker_threads": profile["threads"],
                                "concurrency": profile["replicas"] * profile["threads"],
                                "kafka_consumers": profile["replicas"] if system == "kafka" else base["kafka_consumers"],
                                "sidekiq_workers": profile["replicas"] if system == "sidekiq" else base["sidekiq_workers"],
                            },
                        )
                    )
        case "cpu_light_matched_concurrency" | "io_bound_matched_concurrency":
            for concurrency in suite["matrix"]["concurrency"]:
                for system in suite["systems"]:
                    rows.append(
                        (
                            system,
                            f"concurrency={concurrency}",
                            {
                                "concurrency": concurrency,
                                "worker_threads": concurrency,
                                "kafka_consumers": 1,
                                "sidekiq_workers": 1,
                            },
                        )
                    )
        case "bursty_ingest_shape":
            for shape in suite["shapes"]:
                for system in suite["systems"]:
                    rows.append(
                        (
                            system,
                            f"produce_shape={shape['name']}",
                            {
                                "produce_shape": shape["name"],
                                "burst_size": shape["burst_size"],
                                "inter_burst_ms": shape["inter_burst_ms"],
                            },
                        )
                    )
        case "delayed_case_study":
            for system in suite["systems"]:
                rows.append((system, "delayed_case", {"produce_shape": "sequential_scheduled"}))
        case _:
            raise KeyError(f"Unknown suite: {suite_id}")

    return [_condition(suite_id, suite, system, condition_id, base | overrides) for system, condition_id, overrides in rows]


def _condition(
    suite_id: str,
    suite: dict[str, Any],
    system: str,
    condition_id: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    return {
        "suite_id": suite_id,
        "variable_under_test": suite["variable_under_test"],
        "system": system,
        "workload": suite["workload"],
        "condition_id": condition_id,
        "messages": int(cfg["messages"]),
        "warmup": int(cfg["warmup"]),
        "repetitions": int(cfg["repetitions"]),
        "partitions": int(cfg["partitions"]),
        "kafka_consumers": int(cfg["kafka_consumers"]),
        "sidekiq_workers": int(cfg["sidekiq_workers"]),
        "worker_threads": int(cfg["worker_threads"]),
        "concurrency": int(cfg["concurrency"]),
        "fetch_min_bytes": int(cfg["fetch_min_bytes"]),
        "fetch_max_wait_ms": int(cfg["fetch_max_wait_ms"]),
        "kafka_compression": str(cfg["kafka_compression"]),
        "kafka_acks": str(cfg["kafka_acks"]),
        "produce_shape": cfg.get("produce_shape", "sequential"),
        "burst_size": None if cfg.get("burst_size") is None else int(cfg["burst_size"]),
        "inter_burst_ms": None if cfg.get("inter_burst_ms") is None else float(cfg["inter_burst_ms"]),
        "delay_seconds": None if cfg.get("delay_seconds") is None else float(cfg["delay_seconds"]),
    }
