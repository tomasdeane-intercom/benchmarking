import gzip
import json
from pathlib import Path

from benchkit.result_store import write_trace


def _success_record(message_id: str, *, complete_time: float) -> dict:
    return {
        "id": message_id,
        "system": "kafka",
        "workload": "cpu_light",
        "run_id": "run-1",
        "enqueue_time": complete_time - 2.0,
        "dequeue_time": complete_time - 1.0,
        "complete_time": complete_time,
        "latency_ms": 2000.0,
        "processing_ms": 1000.0,
        "success": True,
    }


def test_write_trace_sorts_records_by_terminal_time(tmp_path: Path):
    trace_name = write_trace(
        [
            _success_record("late-success", complete_time=4.0),
            {"_type": "error", "error": "boom", "time": 3.0},
            _success_record("early-success", complete_time=2.0),
        ],
        "run-1",
        output_root=tmp_path,
    )

    with gzip.open(tmp_path / trace_name, "rt", encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle]

    assert [record["id"] if "id" in record else record["_type"] for record in records] == [
        "early-success",
        "error",
        "late-success",
    ]
