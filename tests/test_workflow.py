import subprocess
from pathlib import Path

from benchkit.cli import workflow
from benchkit.config import RuntimeConfig


def test_run_group_preserves_manifest_order_and_optional_calibration(monkeypatch, tmp_path: Path):
    events: list[str] = []
    monkeypatch.setattr(
        workflow,
        "list_suites",
        lambda: [
            {"suite_id": "kafka_partition_adequacy", "type": "calibration"},
            {"suite_id": "parallelism_decomposition", "type": "calibration"},
            {"suite_id": "cpu_light_matched_concurrency", "type": "comparative"},
        ],
    )
    monkeypatch.setattr(
        workflow,
        "expand_suite",
        lambda suite_name: [{"suite_id": suite_name, "condition_id": f"{suite_name}-condition", "repetitions": 1}],
    )
    monkeypatch.setattr(
        workflow,
        "_run_suite",
        lambda suite_name, results_dir, **kwargs: events.append(f"{suite_name}:{results_dir.name}"),
    )
    monkeypatch.setattr(workflow, "_run_stub_calibration", lambda *args, **kwargs: events.append("stub"))

    workflow._run_group("calibration", tmp_path / "results", with_stub_calibration=False)
    assert events == [
        "kafka_partition_adequacy:results",
        "parallelism_decomposition:results",
    ]

    events.clear()
    workflow._run_group("all", tmp_path / "results", with_stub_calibration=True)
    assert events == [
        "stub",
        "kafka_partition_adequacy:results",
        "parallelism_decomposition:results",
        "cpu_light_matched_concurrency:results",
    ]


def test_ensure_scaled_service_tolerates_timeout_when_enough_instances_are_running(monkeypatch):
    monkeypatch.setattr(
        workflow,
        "run_compose",
        lambda *args, **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired(cmd=["docker", "compose"], timeout=30)),
    )
    monkeypatch.setattr(workflow, "_running_service_count", lambda *args, **kwargs: 1)

    workflow._ensure_scaled_service(
        service="sidekiq-worker",
        replicas=1,
        compose_project="benchmarking",
        env={"SIDEKIQ_THREADS": "10"},
    )


def test_stop_benchmark_workers_raises_when_timeout_leaves_workers_running(monkeypatch):
    monkeypatch.setattr(
        workflow,
        "run_compose",
        lambda *args, **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired(cmd=["docker", "compose"], timeout=10)),
    )
    monkeypatch.setattr(
        workflow,
        "_running_service_count",
        lambda compose_project, service: 1 if service == "kafka-consumer" else 0,
    )

    try:
        workflow._stop_benchmark_workers("benchmarking")
    except RuntimeError as exc:
        assert "kafka-consumer still running" in str(exc)
    else:
        raise AssertionError("expected cleanup timeout to raise")


def test_run_condition_prepares_then_runs_each_repeat(monkeypatch, tmp_path: Path):
    events: list[tuple[str, int | None]] = []
    aggregate_results: list[dict] = []

    condition = {
        "suite_id": "suite-a",
        "condition_id": "cond-a",
        "variable_under_test": "system",
        "system": "kafka",
        "workload": "cpu_light",
        "messages": 25,
        "warmup": 5,
        "repetitions": 2,
        "partitions": 2,
        "concurrency": 1,
        "kafka_consumers": 2,
        "sidekiq_workers": 1,
        "worker_threads": 3,
        "fetch_min_bytes": 1,
        "fetch_max_wait_ms": 500,
        "kafka_compression": "none",
        "kafka_acks": "all",
        "produce_shape": "sequential",
    }

    monkeypatch.setattr(workflow, "_stop_benchmark_workers", lambda compose_project: events.append(("stop", None)))
    monkeypatch.setattr(
        workflow,
        "run_orchestrator",
        lambda module, module_args, **kwargs: events.append(("prepare", None))
        if module == "benchkit.cli.benchmark" and "--prepare-only" in module_args
        else None,
    )
    monkeypatch.setattr(
        workflow,
        "_start_condition_workers",
        lambda current_condition, runtime: events.append(("start", None)),
    )
    monkeypatch.setattr(
        workflow,
        "_benchmark_json",
        lambda module_args: {"run_id": f"run-{module_args[module_args.index('--repeat-index') + 1]}", "repeat_index": int(module_args[module_args.index("--repeat-index") + 1])},
    )
    monkeypatch.setattr(
        workflow,
        "write_aggregate",
        lambda results, *, output_root: aggregate_results.extend(results),
    )

    workflow._run_condition(condition, results_dir=tmp_path, runtime=RuntimeConfig())

    assert events == [
        ("stop", None),
        ("prepare", None),
        ("start", None),
        ("stop", None),
        ("prepare", None),
        ("start", None),
    ]
    assert aggregate_results == [
        {"run_id": "run-1", "repeat_index": 1},
        {"run_id": "run-2", "repeat_index": 2},
    ]


def test_workflow_progress_counts_runs_from_repetitions():
    progress = workflow._workflow_progress(
        [
            [{"repetitions": 2}, {"repetitions": 1}],
            [{"repetitions": 3}],
        ]
    )

    assert progress.suite_total == 2
    assert progress.condition_total == 3
    assert progress.run_total == 6
