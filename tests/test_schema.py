from benchkit.schema_validation import validate_result, validate_stub_calibration, validate_trace_record
from tests.support import result_payload, success_record


def test_trace_record_variants_validate():
    validate_trace_record(success_record(enqueue_time=1.0, dequeue_time=2.0, complete_time=3.0))
    validate_trace_record({"_type": "error", "error": "boom", "time": 1.0})


def test_inline_result_and_stub_calibration_validate():
    validate_result(
        result_payload(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=1",
            throughput=0.5,
            trace_file="traces/run-1.jsonl.gz",
        )
    )
    validate_stub_calibration(
        {
            "type": "stub_capacity_calibration",
            "timestamp": "2026-03-20T14:01:12.906173",
            "config": {
                "stub_url": "http://stub-server:8080",
                "stub_workers": 20,
                "delay_ms": 50,
                "requests_per_worker": 100,
            },
            "results": [
                {
                    "concurrency": 1,
                    "total_requests": 100,
                    "successful": 100,
                    "errors": 0,
                    "throughput_rps": 17.79,
                    "wall_time_s": 5.62,
                    "latency_ms": {
                        "min": 51.89,
                        "mean": 55.93,
                        "median": 55.1,
                        "p95": 59.24,
                        "p99": 68.05,
                        "max": 69.11,
                    },
                }
            ],
            "theoretical_max_rps": 400.0,
        }
    )
