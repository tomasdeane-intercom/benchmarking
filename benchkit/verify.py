from __future__ import annotations

import gzip
import json
from pathlib import Path

from benchkit.manifest import expand_suite, list_suites
from benchkit.result_store import list_result_files
from benchkit.schema_validation import SchemaError, validate_result, validate_trace_record


def verify_results(
    results_dir: Path,
    *,
    require_manifest_coverage: bool = False,
) -> list[str]:
    failures: list[str] = []
    valid_results: list[dict] = []

    for path in list_result_files(results_dir):
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
            validate_result(result)
        except (OSError, json.JSONDecodeError, SchemaError) as exc:
            failures.append(f"{path.name}: {exc}")
            continue

        valid_results.append(result)
        failures.extend(_verify_result(path, result))

    if require_manifest_coverage:
        failures.extend(_verify_manifest_coverage(valid_results))
    return failures


def _verify_result(path: Path, result: dict) -> list[str]:
    failures = [f"{path.name}: {message}" for message in _verify_statistics(result["statistics"])]
    trace_name = result.get("trace_file")
    if not trace_name:
        return [*failures, f"{path.name}: missing trace_file"]

    trace_path = path.parent / trace_name
    if not trace_path.exists():
        return [*failures, f"{path.name}: missing trace file {trace_path.name}"]

    counts, trace_failures = _verify_trace(trace_path, result)
    failures.extend(f"{path.name}: {failure}" for failure in trace_failures)
    expected_counts = {
        "total_messages": result["statistics"]["total_messages"],
        "successful": result["statistics"]["successful"],
        "logical_failures": result["statistics"]["logical_failures"],
        "exception_failures": result["statistics"]["exception_failures"],
        "accounted_messages": result["statistics"]["accounted_messages"],
    }
    for key, expected in expected_counts.items():
        if counts[key] != expected:
            failures.append(f"{path.name}: trace {key} does not match stored statistics")
    return failures


def _verify_statistics(stats: dict) -> list[str]:
    checks = {
        "accounted_messages != expected_messages": stats["accounted_messages"] == stats["expected_messages"],
        "successful + failed != accounted_messages": stats["successful"] + stats["failed"] == stats["accounted_messages"],
        "logical_failures + exception_failures != failed": stats["logical_failures"] + stats["exception_failures"] == stats["failed"],
        "failed != 0": stats["failed"] == 0,
        "error_rate_pct != 0": stats["error_rate_pct"] == 0,
    }
    return [message for message, ok in checks.items() if not ok]


def _verify_trace(trace_path: Path, result: dict) -> tuple[dict[str, int], list[str]]:
    counts = {
        "total_messages": 0,
        "successful": 0,
        "logical_failures": 0,
        "exception_failures": 0,
        "accounted_messages": 0,
    }
    failures: list[str] = []
    seen_ids: dict[str, int] = {}

    try:
        with gzip.open(trace_path, "rt", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    record = json.loads(line)
                    validate_trace_record(record)
                except json.JSONDecodeError as exc:
                    failures.append(f"trace line {line_number}: invalid JSON ({exc.msg})")
                    continue
                except SchemaError as exc:
                    failures.append(f"trace line {line_number}: {exc}")
                    continue

                if record.get("_type") == "error":
                    counts["exception_failures"] += 1
                    counts["accounted_messages"] += 1
                    continue

                counts["total_messages"] += 1
                counts["accounted_messages"] += 1
                counts["successful" if record["success"] else "logical_failures"] += 1
                failures.extend(_trace_metadata_failures(record, result, line_number))

                first_line = seen_ids.setdefault(record["id"], line_number)
                if first_line != line_number:
                    failures.append(
                        f"duplicate trace message ids detected: {record['id']!r} (lines {first_line} and {line_number})"
                    )
    except OSError as exc:
        failures.append(f"unable to read trace file {trace_path.name}: {exc}")

    return counts, failures


def _trace_metadata_failures(record: dict, result: dict, line_number: int) -> list[str]:
    mismatches = [
        f"{field}={record[field]!r} != {result[field]!r}"
        for field in ("run_id", "system", "workload")
        if record[field] != result[field]
    ]
    return [] if not mismatches else [f"trace line {line_number}: {'; '.join(mismatches)}"]


def _verify_manifest_coverage(results: list[dict]) -> list[str]:
    suite_ids = [suite["suite_id"] for suite in list_suites()]
    actual = {
        (result["suite_id"], result["condition_id"], result["system"], int(result["repeat_index"]))
        for result in results
        if result["suite_id"] in suite_ids
    }
    failures: list[str] = []
    for suite_id in suite_ids:
        expected = {
            (condition["suite_id"], condition["condition_id"], condition["system"], repeat_index)
            for condition in expand_suite(suite_id)
            for repeat_index in range(1, int(condition["repetitions"]) + 1)
        }
        missing = sorted(expected - actual)
        if missing:
            preview = ", ".join(
                f"{condition_id}/{system}/repeat={repeat_index}"
                for _, condition_id, system, repeat_index in missing[:4]
            )
            suffix = ", ..." if len(missing) > 4 else ""
            failures.append(f"{suite_id}: missing {len(missing)} manifest-defined run(s): {preview}{suffix}")
    return failures
