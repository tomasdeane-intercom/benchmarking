from __future__ import annotations

import gzip
import json
import uuid
from pathlib import Path
from typing import Iterable

from benchkit.paths import FINAL_RESULTS_DIR, FINAL_TRACES_DIR
from benchkit.schema_validation import SchemaError, load_json, validate_result, validate_trace_record
from benchkit.timing import iso_compact, utc_now


def ensure_results_dirs(output_root: Path = FINAL_RESULTS_DIR) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "traces").mkdir(parents=True, exist_ok=True)


def generate_run_id(system: str, workload: str) -> str:
    return f"{system}-{workload}-{iso_compact(utc_now())}-{uuid.uuid4().hex[:8]}"


def write_trace(records: Iterable[dict], run_id: str, output_root: Path = FINAL_RESULTS_DIR) -> str:
    ensure_results_dirs(output_root)
    trace_path = output_root / "traces" / f"{run_id}.jsonl.gz"
    ordered_records: list[tuple[float, int, dict]] = []
    for index, record in enumerate(records):
        validate_trace_record(record)
        ordered_records.append((_trace_terminal_time(record), index, record))
    with gzip.open(trace_path, "wt", encoding="utf-8") as handle:
        for _, _, record in sorted(ordered_records):
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")
    return str(Path("traces") / trace_path.name)


def _trace_terminal_time(record: dict) -> float:
    if record.get("_type") == "error":
        return float(record["time"])
    return float(record["complete_time"])


def write_result(result: dict, output_root: Path = FINAL_RESULTS_DIR) -> Path:
    ensure_results_dirs(output_root)
    validate_result(result)
    path = output_root / f"{result['run_id']}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=False)
        handle.write("\n")
    return path


def write_aggregate(results: list[dict], output_root: Path = FINAL_RESULTS_DIR) -> Path:
    ensure_results_dirs(output_root)
    timestamp = iso_compact(utc_now())
    first = results[0]
    path = output_root / f"agg-{first['system']}-{first['workload']}-{timestamp}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2, sort_keys=False)
        handle.write("\n")
    return path


def list_result_files(results_dir: Path) -> list[Path]:
    if not results_dir.exists():
        return []
    return sorted(
        path
        for path in results_dir.glob("*.json")
        if not path.name.startswith("agg-")
    )


def load_results(results_dir: Path) -> tuple[list[dict], list[str]]:
    results: list[dict] = []
    warnings: list[str] = []
    for path in list_result_files(results_dir):
        try:
            data = load_json(path)
            if not isinstance(data, dict):
                raise SchemaError("Result file is not a JSON object")
            validate_result(data)
            results.append(data)
        except (OSError, json.JSONDecodeError, SchemaError) as exc:
            warnings.append(f"{path.name}: {exc}")
    return results, warnings
