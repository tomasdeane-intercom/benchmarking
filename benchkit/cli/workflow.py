from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from benchkit.config import RuntimeConfig
from benchkit.docker_stats import DEFAULT_DOCKER_STATS_INTERVAL_SECONDS, DockerStatsCollector
from benchkit.host_compose import run_compose, run_orchestrator
from benchkit.manifest import expand_suite, list_suites
from benchkit.paths import RESULTS_DIR

BENCHMARK_WORKER_SERVICES = ("kafka-consumer", "sidekiq-worker")
BENCHMARK_WORKER_STOP_TIMEOUT_SECONDS = 10.0
SERVICE_UP_TIMEOUT_SECONDS = 30.0
SMOKE_BASE_CONDITION = {
    "suite_id": "smoke",
    "variable_under_test": "system",
    "workload": "cpu_light",
    "messages": 25,
    "warmup": 5,
    "repetitions": 1,
    "partitions": 2,
    "concurrency": 1,
    "kafka_consumers": 1,
    "sidekiq_workers": 1,
    "worker_threads": 1,
    "fetch_min_bytes": 1,
    "fetch_max_wait_ms": 500,
    "kafka_compression": "none",
    "kafka_acks": "all",
    "produce_shape": "sequential",
}


@dataclass
class _WorkflowProgress:
    suite_total: int
    condition_total: int
    run_total: int
    suite_index: int = 0
    condition_index: int = 0
    run_index: int = 0

    def announce_plan(self) -> None:
        _print_progress(
            f"planned {self.suite_total} suites, {self.condition_total} conditions, {self.run_total} runs"
        )

    def start_suite(self, suite_name: str, conditions: list[dict]) -> None:
        self.suite_index += 1
        suite_runs = sum(int(condition["repetitions"]) for condition in conditions)
        _print_progress(
            f"suite {self.suite_index}/{self.suite_total}: {suite_name} "
            f"({len(conditions)} conditions, {suite_runs} runs)"
        )

    def start_run(self, condition: dict, *, repeat_index: int) -> None:
        if repeat_index == 1:
            self.condition_index += 1
        self.run_index += 1
        total_repeats = int(condition["repetitions"])
        repeat_suffix = f", repeat {repeat_index}/{total_repeats}" if total_repeats > 1 else ""
        _print_progress(
            f"run {self.run_index}/{self.run_total}: "
            f"condition {self.condition_index}/{self.condition_total}, "
            f"{condition['suite_id']}/{condition['system']}/{condition['condition_id']}{repeat_suffix}"
        )


def write_aggregate(results: list[dict], *, output_root: Path) -> None:
    from benchkit.result_store import write_aggregate as write_aggregate_impl

    write_aggregate_impl(results, output_root=output_root)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Host-side benchmark workflow runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup", help="Validate Docker tooling")
    subparsers.add_parser("build", help="Build images")
    subparsers.add_parser("infra", help="Start core benchmark infrastructure")

    suite = subparsers.add_parser("suite", help="Run a suite")
    suite.add_argument("suite_name")
    suite.add_argument("--results-dir", default=os.getenv("BENCHMARK_RESULTS_DIR", "results/final"))

    group = subparsers.add_parser("group", help="Run a suite group")
    group.add_argument("group_name", choices=["all", "calibration", "comparative", "case_study"])
    group.add_argument("--results-dir", default=os.getenv("BENCHMARK_RESULTS_DIR", "results/final"))
    group.add_argument("--with-stub-calibration", action="store_true")

    stub = subparsers.add_parser("stub-calibration", help="Run stub calibration")
    stub.add_argument("--output-dir", default="results/calibration")

    smoke = subparsers.add_parser("smoke", help="Run smoke benchmark")
    smoke.add_argument("--results-dir", default=os.getenv("SMOKE_RESULTS_DIR", "results/smoke"))

    report = subparsers.add_parser("report", help="Generate report from stored results")
    report.add_argument("--results-dir", default="results/final")
    report.add_argument("--output-dir", default="results/report")

    verify = subparsers.add_parser("verify", help="Verify stored results")
    verify.add_argument("--dir", default="results/final")
    verify.add_argument("--require-manifest-coverage", action="store_true")

    wait = subparsers.add_parser("wait-for-infra", help="Wait for benchmark infrastructure")
    wait.add_argument("extra_args", nargs=argparse.REMAINDER)

    subparsers.add_parser("logs", help="Tail docker compose logs")
    subparsers.add_parser("clean", help="Stop compose services")
    subparsers.add_parser("reset", help="Stop compose services and remove volumes")
    subparsers.add_parser("purge", help="Remove compose state, local images, and benchmark artifacts")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        match args.command:
            case "setup":
                subprocess.run(["docker", "version"], check=True)
                run_compose(["version"])
            case "build":
                run_compose(["build"])
            case "infra":
                _ensure_full_infra()
            case "suite":
                conditions = expand_suite(args.suite_name)
                progress = _workflow_progress([conditions])
                progress.announce_plan()
                _run_suite(args.suite_name, Path(args.results_dir), conditions=conditions, progress=progress)
            case "group":
                _run_group(args.group_name, Path(args.results_dir), with_stub_calibration=args.with_stub_calibration)
            case "stub-calibration":
                _run_stub_calibration(output_dir=args.output_dir)
            case "smoke":
                _run_smoke(Path(args.results_dir))
            case "report":
                run_orchestrator(
                    "benchkit.cli.report",
                    ["--results-dir", args.results_dir, "--output-dir", args.output_dir],
                )
            case "verify":
                verify_args = ["--dir", args.dir]
                if args.require_manifest_coverage:
                    verify_args.append("--require-manifest-coverage")
                run_orchestrator("benchkit.cli.verify_results", verify_args)
            case "wait-for-infra":
                extra_args = args.extra_args[1:] if args.extra_args[:1] == ["--"] else args.extra_args
                run_orchestrator("benchkit.cli.wait_for_infra", extra_args)
            case "logs":
                return run_compose(["logs", "--tail=200", "-f"], check=False).returncode
            case "clean":
                run_compose(["down", "--remove-orphans"])
            case "reset":
                run_compose(["down", "-v", "--remove-orphans"])
            case "purge":
                run_compose(["down", "-v", "--rmi", "local", "--remove-orphans"])
                shutil.rmtree(RESULTS_DIR, ignore_errors=True)
        return 0
    except subprocess.CalledProcessError as exc:
        return exc.returncode


def _run_group(group_name: str, results_dir: Path, *, with_stub_calibration: bool) -> None:
    if with_stub_calibration:
        _run_stub_calibration()
    selected_suites = [suite for suite in list_suites() if group_name in {"all", suite["type"]}]
    suite_conditions = [(suite["suite_id"], expand_suite(suite["suite_id"])) for suite in selected_suites]
    progress = _workflow_progress([conditions for _, conditions in suite_conditions])
    progress.announce_plan()
    for suite_name, conditions in suite_conditions:
        _run_suite(suite_name, results_dir, conditions=conditions, progress=progress)


def _run_stub_calibration(*, output_dir: str = "results/calibration") -> None:
    run_compose(["up", "-d", "stub-server"])
    run_orchestrator("benchkit.cli.wait_for_infra", ["--require", "stub-server"])
    run_orchestrator("benchkit.cli.stub_calibration", ["--output-dir", output_dir])


def _ensure_full_infra() -> None:
    run_compose(["up", "-d", "kafka", "redis", "metrics-redis", "stub-server"])
    run_orchestrator("benchkit.cli.wait_for_infra", [])


def _run_suite(
    suite_name: str,
    results_dir: Path,
    *,
    conditions: list[dict] | None = None,
    progress: _WorkflowProgress | None = None,
) -> None:
    runtime = RuntimeConfig()
    results_dir.mkdir(parents=True, exist_ok=True)
    _ensure_full_infra()
    conditions = list(conditions or expand_suite(suite_name))
    if progress is not None:
        progress.start_suite(suite_name, conditions)

    stats_csv = results_dir.parent / f"docker_stats_{_utc_timestamp()}.csv"
    collector = DockerStatsCollector(
        stats_csv,
        compose_project=runtime.compose_project,
        interval_seconds=float(
            os.getenv("DOCKER_STATS_INTERVAL_SECONDS", str(DEFAULT_DOCKER_STATS_INTERVAL_SECONDS))
        ),
    )
    collector.start()
    try:
        for condition in conditions:
            _run_condition(condition, results_dir=results_dir, runtime=runtime, progress=progress)
    finally:
        collector.stop()

    run_orchestrator(
        "benchkit.cli.resource_stats",
        [
            "--csv",
            str(stats_csv),
            "--results-dir",
            str(results_dir),
            "--compose-project",
            runtime.compose_project,
        ],
    )


def _run_smoke(results_dir: Path) -> None:
    runtime = RuntimeConfig(
        topic="benchmark",
        consumer_group="benchmark-smoke",
        queue_name="benchmark",
    )
    results_dir.mkdir(parents=True, exist_ok=True)
    _ensure_full_infra()
    conditions = [
        {**SMOKE_BASE_CONDITION, "system": system, "condition_id": f"smoke-{system}"}
        for system in ("kafka", "sidekiq")
    ]
    progress = _workflow_progress([conditions])
    progress.announce_plan()
    progress.start_suite("smoke", conditions)
    for condition in conditions:
        _run_condition(condition, results_dir=results_dir, runtime=runtime, progress=progress)
    run_orchestrator("benchkit.cli.verify_results", ["--dir", str(results_dir)])


def _run_condition(
    condition: dict,
    *,
    results_dir: Path,
    runtime: RuntimeConfig,
    progress: _WorkflowProgress | None = None,
) -> None:
    benchmark_args = _benchmark_args(condition, results_dir=results_dir, runtime=runtime)
    results = []
    for repeat_index in range(1, int(condition["repetitions"]) + 1):
        if progress is not None:
            progress.start_run(condition, repeat_index=repeat_index)
        _stop_benchmark_workers(runtime.compose_project)
        run_orchestrator(
            "benchkit.cli.benchmark",
            [*benchmark_args, "--prepare-only"],
            stdin=subprocess.DEVNULL,
        )
        _start_condition_workers(condition, runtime)
        results.append(
            _benchmark_json(
                [*benchmark_args, "--clean-state-mode", "skip", "--repeat-index", str(repeat_index)]
            )
        )
    if len(results) > 1:
        write_aggregate(results, output_root=results_dir)


def _benchmark_json(args: list[str]) -> dict:
    completed = run_orchestrator(
        "benchkit.cli.benchmark",
        args,
        capture_output=True,
        stdin=subprocess.DEVNULL,
    )
    result = json.loads(completed.stdout)
    assert isinstance(result, dict)
    return result


def _benchmark_args(condition: dict, *, results_dir: Path, runtime: RuntimeConfig) -> list[str]:
    args = [
        "--suite-id",
        condition["suite_id"],
        "--condition-id",
        condition["condition_id"],
        "--variable-under-test",
        condition["variable_under_test"],
        "--system",
        condition["system"],
        "--workload",
        condition["workload"],
        "--messages",
        str(condition["messages"]),
        "--warmup",
        str(condition["warmup"]),
        "--topic",
        runtime.topic,
        "--consumer-group",
        runtime.consumer_group,
        "--queue-name",
        runtime.queue_name,
        "--partitions",
        str(condition["partitions"]),
        "--replicas",
        str(_condition_replicas(condition)),
        "--worker-threads",
        str(condition["worker_threads"]),
        "--kafka-compression",
        condition["kafka_compression"],
        "--kafka-acks",
        condition["kafka_acks"],
        "--produce-shape",
        condition["produce_shape"],
        "--output-dir",
        str(results_dir),
    ]
    for flag, key in (
        ("--burst-size", "burst_size"),
        ("--inter-burst-ms", "inter_burst_ms"),
        ("--delay-seconds", "delay_seconds"),
    ):
        if condition.get(key) is not None:
            args.extend([flag, str(condition[key])])
    return args


def _start_condition_workers(condition: dict, runtime: RuntimeConfig) -> None:
    if condition["system"] == "kafka":
        _ensure_scaled_service(
            service="kafka-consumer",
            replicas=int(condition["kafka_consumers"]),
            compose_project=runtime.compose_project,
            env={
                "KAFKA_TOPIC": runtime.topic,
                "KAFKA_GROUP_ID": runtime.consumer_group,
                "KAFKA_THREADS": str(condition["worker_threads"]),
                "FETCH_MIN_BYTES": str(condition["fetch_min_bytes"]),
                "FETCH_MAX_WAIT_MS": str(condition["fetch_max_wait_ms"]),
            },
        )
        return

    _ensure_scaled_service(
        service="sidekiq-worker",
        replicas=int(condition["sidekiq_workers"]),
        compose_project=runtime.compose_project,
        env={
            "BENCHMARK_QUEUE": runtime.queue_name,
            "SIDEKIQ_THREADS": str(condition["worker_threads"]),
        },
    )


def _condition_replicas(condition: dict) -> int:
    return int(condition["kafka_consumers"] if condition["system"] == "kafka" else condition["sidekiq_workers"])


def _stop_benchmark_workers(compose_project: str) -> None:
    try:
        run_compose(
            ["stop", "-t", "0", *BENCHMARK_WORKER_SERVICES],
            check=False,
            timeout=BENCHMARK_WORKER_STOP_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        stalled = [service for service in BENCHMARK_WORKER_SERVICES if _running_service_count(compose_project, service)]
        if stalled:
            raise RuntimeError(
                f"Timed out stopping benchmark workers after {BENCHMARK_WORKER_STOP_TIMEOUT_SECONDS:.0f}s: {', '.join(stalled)} still running"
            ) from exc


def _ensure_scaled_service(
    *,
    service: str,
    replicas: int,
    compose_project: str,
    env: dict[str, str],
) -> None:
    try:
        run_compose(
            ["up", "-d", "--force-recreate", "--scale", f"{service}={replicas}", service],
            env=env,
            timeout=SERVICE_UP_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        running = _running_service_count(compose_project, service)
        if running < replicas:
            raise RuntimeError(
                f"Timed out bringing up {service}: {running}/{replicas} running after {SERVICE_UP_TIMEOUT_SECONDS:.0f}s"
            ) from exc


def _running_service_count(compose_project: str, service: str) -> int:
    completed = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={compose_project}",
            "--filter",
            f"label=com.docker.compose.service={service}",
            "--filter",
            "status=running",
            "--format",
            "{{.Names}}",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    return 0 if completed.returncode else len([line for line in completed.stdout.splitlines() if line.strip()])


def _utc_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def _workflow_progress(suites: list[list[dict]]) -> _WorkflowProgress:
    return _WorkflowProgress(
        suite_total=len(suites),
        condition_total=sum(len(conditions) for conditions in suites),
        run_total=sum(int(condition["repetitions"]) for conditions in suites for condition in conditions),
    )


def _print_progress(message: str) -> None:
    print(f"[progress] {message}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
