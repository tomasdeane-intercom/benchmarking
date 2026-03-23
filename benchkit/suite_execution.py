from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path

from redis import Redis

from benchkit.config import RuntimeConfig
from benchkit.kafka.admin import KafkaAdmin
from benchkit.kafka.producer import KafkaProducerAdapter
from benchkit.metrics import MetricsStore
from benchkit.result_store import generate_run_id, write_result, write_trace
from benchkit.statistics import compute_run_statistics
from benchkit.timing import iso_z, utc_now
from benchkit.workloads import spec_for
from benchkit.sidekiq.producer import SidekiqProducerAdapter


LOGGER = logging.getLogger(__name__)


def prepare_condition(args, runtime: RuntimeConfig | None = None) -> None:
    runtime = runtime or RuntimeConfig()
    metrics = MetricsStore(runtime.metrics_redis_url)
    _clean_state(args, runtime, metrics)


def run_condition(
    args,
    output_root: Path,
    runtime: RuntimeConfig | None = None,
) -> dict:
    runtime = runtime or RuntimeConfig()
    metrics = MetricsStore(runtime.metrics_redis_url)
    repeat_index = getattr(args, "repeat_index", 1)
    run_id = generate_run_id(args.system, args.workload)
    nominal_concurrency = int(args.replicas) * int(args.worker_threads)
    LOGGER.info(
        "run suite=%s condition=%s system=%s workload=%s messages=%s warmup=%s repeat=%s concurrency=%s partitions=%s",
        args.suite_id,
        args.condition_id,
        args.system,
        args.workload,
        args.messages,
        args.warmup,
        repeat_index,
        nominal_concurrency,
        args.partitions,
    )
    if args.warmup > 0:
        warmup_id = f"warmup-{run_id}"
        _produce_and_wait(
            args=args,
            runtime=runtime,
            metrics=metrics,
            run_id=warmup_id,
            messages=args.warmup,
            timeout_seconds=args.timeout_seconds,
        )
        metrics.clear_run(warmup_id)
    metrics.clear_run(run_id)
    run_start = utc_now()
    produce_start, produce_end = _produce_and_wait(
        args=args,
        runtime=runtime,
        metrics=metrics,
        run_id=run_id,
        messages=args.messages,
        timeout_seconds=args.timeout_seconds,
    )
    run_end = utc_now()
    events = metrics.load_completed(run_id)
    errors = metrics.load_errors(run_id)
    stats = compute_run_statistics(
        events=events,
        error_records=errors,
        expected_messages=args.messages,
        produce_start=produce_start,
        produce_end=produce_end,
        workload=args.workload,
    )
    if stats["accounted_messages"] != args.messages:
        raise RuntimeError(
            f"Incomplete run {run_id}: accounted {stats['accounted_messages']} / {args.messages}"
        )
    trace_file = write_trace([*events, *errors], run_id, output_root=output_root)
    result = {
        "schema_version": "3.5.0",
        "run_id": run_id,
        "suite_id": args.suite_id,
        "condition_id": args.condition_id,
        "repeat_index": repeat_index,
        "variable_under_test": args.variable_under_test,
        "system": args.system,
        "workload": args.workload,
        "timestamp": utc_now().isoformat(),
        "run_start_utc": iso_z(run_start),
        "run_end_utc": iso_z(run_end),
        "config": _result_config(args, runtime),
        "statistics": stats,
        "trace_file": trace_file,
    }
    write_result(result, output_root=output_root)
    return result


def _clean_state(args, runtime: RuntimeConfig, metrics: MetricsStore) -> None:
    metrics.flush()
    if args.clean_state_mode == "skip":
        return
    if args.system == "kafka":
        admin = KafkaAdmin(runtime.kafka_bootstrap_servers)
        admin.delete_consumer_group(args.consumer_group)
        observed = admin.reset_topic(args.topic, args.partitions)
        if observed != args.partitions:
            raise RuntimeError(
                f"Observed {observed} partitions for {args.topic}, expected {args.partitions}"
            )
    else:
        producer = SidekiqProducerAdapter(runtime.redis_url, args.queue_name)
        producer.cleanup()


def _produce_and_wait(args, runtime: RuntimeConfig, metrics: MetricsStore, run_id: str, messages: int, timeout_seconds: float) -> tuple[float, float]:
    if args.system == "kafka":
        producer = KafkaProducerAdapter(
            runtime.kafka_bootstrap_servers,
            acks=args.kafka_acks,
            compression=args.kafka_compression,
        )
        produce_start, produce_end = producer.produce_run(
            topic=args.topic,
            run_id=run_id,
            workload=args.workload,
            messages=messages,
            produce_shape=args.produce_shape,
            burst_size=args.burst_size,
            inter_burst_ms=args.inter_burst_ms,
            delay_seconds=args.delay_seconds,
        )
    else:
        producer = SidekiqProducerAdapter(runtime.redis_url, args.queue_name)
        produce_start, produce_end = producer.produce_run(
            run_id=run_id,
            workload=args.workload,
            messages=messages,
            produce_shape=args.produce_shape,
            burst_size=args.burst_size,
            inter_burst_ms=args.inter_burst_ms,
            delay_seconds=args.delay_seconds,
        )
    if not metrics.wait_for_accounted(run_id, messages, timeout_seconds=timeout_seconds):
        raise RuntimeError(f"Timed out waiting for {messages} terminal outcomes for {run_id}")
    return produce_start, produce_end


def _result_config(args, runtime: RuntimeConfig) -> dict:
    workload_spec = spec_for(args.workload)
    nominal_concurrency = int(args.replicas) * int(args.worker_threads)
    config = {
        "messages": args.messages,
        "warmup": args.warmup,
        "produce_pattern": args.produce_shape,
        "stub_delay_ms": workload_spec.delay_ms,
        "stub_capacity_workers": runtime.stub_server_workers,
        "nominal_concurrency_target": nominal_concurrency,
        "semantics_profile": _semantics_profile(args.system, args.kafka_acks),
        "burst_size": args.burst_size,
        "inter_burst_ms": args.inter_burst_ms,
        "delay_seconds": args.delay_seconds,
    }
    if args.system == "kafka":
        admin = KafkaAdmin(runtime.kafka_bootstrap_servers)
        observed_partitions = admin.observed_partitions(args.topic)
        configured_consumer_instances = int(args.replicas) * int(args.worker_threads)
        config.update(
            {
                "concurrency_model": "consumer_instances",
                "requested_partitions": args.partitions,
                "observed_partitions": observed_partitions,
                "kafka_consumer_replicas": args.replicas,
                "kafka_consumer_instances_per_container": args.worker_threads,
                "kafka_configured_consumer_instances": configured_consumer_instances,
                "kafka_assignment_parallelism_upper_bound": min(
                    observed_partitions,
                    configured_consumer_instances,
                ),
                "kafka_compression": args.kafka_compression,
                "kafka_acks": args.kafka_acks,
            }
        )
    else:
        config.update(
            {
                "concurrency_model": "processor_slots",
                "sidekiq_worker_replicas": args.replicas,
                "sidekiq_threads_per_worker": args.worker_threads,
                "sidekiq_configured_processor_slots": args.replicas * args.worker_threads,
                "sidekiq_processor_parallelism_upper_bound": args.replicas * args.worker_threads,
            }
        )
    return config


def _semantics_profile(system: str, kafka_acks: str) -> str:
    if system == "kafka":
        return f"acks={kafka_acks}, single-replica topic, partition-ordered consumption"
    return "retry=false, Redis queue, worker-thread execution"
