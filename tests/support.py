def success_record(**overrides) -> dict:
    record = {
        "id": "1",
        "system": "kafka",
        "workload": "cpu_light",
        "run_id": "run-1",
        "enqueue_time": 0.0,
        "dequeue_time": 1.0,
        "complete_time": 2.0,
        "latency_ms": 2000.0,
        "processing_ms": 1000.0,
        "success": True,
    }
    record.update(overrides)
    return record


def result_payload(
    *,
    run_id: str = "run-1",
    suite_id: str = "smoke",
    condition_id: str = "smoke",
    system: str = "kafka",
    workload: str = "cpu_light",
    repeat_index: int = 1,
    variable_under_test: str = "system",
    messages: int = 1,
    warmup: int = 0,
    throughput: float = 1.0,
    total_messages: int = 1,
    successful: int = 1,
    logical_failures: int = 0,
    exception_failures: int = 0,
    trace_file: str | None = None,
    resource_usage: dict | None = None,
    nominal_concurrency_target: int = 1,
    partitions: int = 1,
    consumer_instances: int = 1,
    stub_delay_ms: int | None = None,
) -> dict:
    failed = logical_failures + exception_failures
    result = {
        "schema_version": "3.5.0",
        "run_id": run_id,
        "suite_id": suite_id,
        "condition_id": condition_id,
        "repeat_index": repeat_index,
        "variable_under_test": variable_under_test,
        "system": system,
        "workload": workload,
        "timestamp": "2026-03-20T14:11:38.354920",
        "run_start_utc": "2026-03-20T14:11:33Z",
        "run_end_utc": "2026-03-20T14:11:38Z",
        "trace_file": trace_file or f"traces/{run_id}.jsonl.gz",
        "config": {
            "messages": messages,
            "warmup": warmup,
            "produce_pattern": "sequential",
            "stub_delay_ms": stub_delay_ms,
            "stub_capacity_workers": 20,
            "nominal_concurrency_target": nominal_concurrency_target,
            "burst_size": None,
            "inter_burst_ms": None,
            "delay_seconds": None,
        },
        "statistics": {
            "expected_messages": messages,
            "accounted_messages": total_messages + exception_failures,
            "total_messages": total_messages,
            "successful": successful,
            "logical_failures": logical_failures,
            "exception_failures": exception_failures,
            "failed": failed,
            "error_rate_pct": 0.0 if not messages else round(100.0 * failed / messages, 2),
            "latency_ms": {"min": 1.0, "max": 1.0, "mean": 1.0, "median": 1.0, "p95": 1.0, "p99": 1.0, "stddev": 0.0},
            "wait_to_start_ms": {"min": 1.0, "max": 1.0, "mean": 1.0, "median": 1.0, "p95": 1.0, "p99": 1.0, "stddev": 0.0},
            "processing_ms": {"min": 1.0, "max": 1.0, "mean": 1.0, "median": 1.0, "p95": 1.0, "p99": 1.0},
            "throughput": {"consumed_msg_per_sec": throughput, "produce_msg_per_sec": throughput},
            "timing_s": {"total_wall": 1.0, "produce": 1.0, "drain": 0.0},
        },
    }
    if system == "kafka":
        result["config"].update(
            {
                "concurrency_model": "consumer_instances",
                "semantics_profile": "acks=all, single-replica topic, partition-ordered consumption",
                "requested_partitions": partitions,
                "observed_partitions": partitions,
                "kafka_consumer_replicas": 1,
                "kafka_consumer_instances_per_container": consumer_instances,
                "kafka_configured_consumer_instances": consumer_instances,
                "kafka_assignment_parallelism_upper_bound": min(partitions, consumer_instances),
                "kafka_compression": "none",
                "kafka_acks": "all",
            }
        )
    else:
        result["config"].update(
            {
                "concurrency_model": "processor_slots",
                "semantics_profile": "retry=false",
                "sidekiq_worker_replicas": 1,
                "sidekiq_threads_per_worker": consumer_instances,
                "sidekiq_configured_processor_slots": consumer_instances,
                "sidekiq_processor_parallelism_upper_bound": consumer_instances,
            }
        )
    if resource_usage is not None:
        result["resource_usage"] = resource_usage
    return result
