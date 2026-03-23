import gzip
import json
from pathlib import Path

from tests.support import result_payload, success_record
from benchkit.verify import verify_results


def _write_trace(path: Path, records: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")

def _write_fixture(tmp_path: Path, *, records: list[dict], result: dict | None = None) -> Path:
    results_dir = tmp_path / "results"
    traces_dir = results_dir / "traces"
    traces_dir.mkdir(parents=True)
    _write_trace(traces_dir / "run-1.jsonl.gz", records)
    (results_dir / "run-1.json").write_text(json.dumps(result or _result()))
    return results_dir


def test_verify_accepts_matching_result(tmp_path: Path):
    assert verify_results(_write_fixture(tmp_path, records=[success_record()])) == []


def test_verify_rejects_bad_accounting_and_trace_counts(tmp_path: Path):
    result = _result()
    result["statistics"]["accounted_messages"] = 2

    failures = verify_results(_write_fixture(tmp_path, records=[success_record()], result=result))

    assert any("accounted_messages != expected_messages" in failure for failure in failures)
    assert any("trace accounted_messages does not match stored statistics" in failure for failure in failures)


def test_verify_rejects_invalid_trace_schema(tmp_path: Path):
    invalid = success_record()
    invalid.pop("success")

    failures = verify_results(_write_fixture(tmp_path, records=[invalid]))

    assert any("trace line 1" in failure and "not valid under any of the given schemas" in failure for failure in failures)


def test_verify_rejects_trace_metadata_mismatch(tmp_path: Path):
    bad = success_record()
    bad["run_id"] = "run-2"

    failures = verify_results(_write_fixture(tmp_path, records=[bad]))

    assert any("trace line 1" in failure and "run_id='run-2' != 'run-1'" in failure for failure in failures)


def test_verify_rejects_duplicate_message_ids(tmp_path: Path):
    first = success_record()
    second = {**success_record(), "enqueue_time": 2.0, "dequeue_time": 3.0, "complete_time": 4.0}
    result = _result()
    result["statistics"]["expected_messages"] = 2
    result["statistics"]["accounted_messages"] = 2
    result["statistics"]["total_messages"] = 2
    result["statistics"]["successful"] = 2

    failures = verify_results(_write_fixture(tmp_path, records=[first, second], result=result))

    assert any("duplicate trace message ids detected" in failure for failure in failures)


def test_verify_can_require_manifest_coverage(tmp_path: Path):
    result = _result()
    result["suite_id"] = "cpu_light_matched_concurrency"
    result["condition_id"] = "concurrency=1"
    result["config"]["messages"] = 10000
    result["config"]["warmup"] = 500
    result["config"]["requested_partitions"] = 6
    result["config"]["observed_partitions"] = 6

    failures = verify_results(
        _write_fixture(tmp_path, records=[success_record()], result=result),
        require_manifest_coverage=True,
    )

    assert any("missing" in failure for failure in failures)


def _result() -> dict:
    return result_payload(throughput=0.5, trace_file="traces/run-1.jsonl.gz")
