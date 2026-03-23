from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev
from typing import Literal, TypedDict

import matplotlib

matplotlib.use("Agg")

from matplotlib import pyplot as plt, ticker

from benchkit.manifest import expand_suite, list_suites
from benchkit.paths import CALIBRATION_DIR
from benchkit.resource_usage import SYSTEM_UNDER_TEST_ROLES
from benchkit.result_store import load_results

MIN_RESOURCE_USAGE_SAMPLES = 3
PARTITION_ADEQUACY_PLATEAU_FRACTION = 0.95
APPENDIX_RESOURCE_MEMORY_CHART_KEY = "appendix_resource_memory"
APPENDIX_RESOURCE_MEMORY_SUITES = (
    "io_bound_matched_concurrency",
    "delayed_case_study",
)
COMPARABILITY_FIELDS = (
    "messages",
    "warmup",
    "produce_pattern",
    "stub_delay_ms",
    "burst_size",
    "inter_burst_ms",
    "delay_seconds",
    "nominal_concurrency_target",
)

SystemName = Literal["kafka", "sidekiq"]
MetricName = Literal[
    "consumed_msg_per_sec",
    "wait_to_start_mean_ms",
    "drain",
    "drain_ms",
    "start_lateness_p95",
]


class SuiteReport(TypedDict):
    title: str
    metric: MetricName
    first_column: str


class LineChartSpec(TypedDict):
    kind: Literal["line"]
    title: str
    metric: MetricName
    axis_title: str
    y_axis: str
    y_scale: Literal["linear", "log"]
    systems: list[SystemName]


class BarChartSpec(TypedDict):
    kind: Literal["bar"]
    title: str
    metric: MetricName
    axis_title: str
    y_axis: str
    y_scale: Literal["linear", "log"]
    systems: list[SystemName]


class RenderedChart(TypedDict):
    title: str
    path: str


ChartSpec = LineChartSpec | BarChartSpec

SUITE_REPORTS: dict[str, SuiteReport] = {
    "kafka_partition_adequacy": {
        "title": "Kafka throughput by partition",
        "metric": "consumed_msg_per_sec",
        "first_column": "Partitions",
    },
    "parallelism_decomposition": {
        "title": "Throughput",
        "metric": "consumed_msg_per_sec",
        "first_column": "Profile",
    },
    "cpu_light_matched_concurrency": {
        "title": "CPU-light throughput",
        "metric": "consumed_msg_per_sec",
        "first_column": "Nominal target",
    },
    "io_bound_matched_concurrency": {
        "title": "I/O-bound throughput",
        "metric": "consumed_msg_per_sec",
        "first_column": "Nominal target",
    },
    "bursty_ingest_shape": {
        "title": "Bursty ingest drain time",
        "metric": "drain_ms",
        "first_column": "Shape",
    },
    "delayed_case_study": {
        "title": "Delayed start lateness p95",
        "metric": "start_lateness_p95",
        "first_column": "Condition",
    },
}


def generate_report(results_dir: Path, output_dir: Path) -> tuple[Path, list[str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results, warnings = load_results(results_dir)
    results.sort(key=lambda row: (row["suite_id"], row["condition_id"], int(row["repeat_index"])))
    warnings.extend(check_comparability(results))
    charts = _render_charts(results, output_dir / "charts")
    report_path = output_dir / "report.md"
    report_path.write_text(
        build_markdown(results, load_calibrations(CALIBRATION_DIR), warnings, charts),
        encoding="utf-8",
    )
    return report_path, warnings


def load_calibrations(calibration_dir: Path) -> list[dict]:
    if not calibration_dir.exists():
        return []
    calibrations: list[dict] = []
    for path in sorted(calibration_dir.glob("*.json")):
        calibration = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(calibration, dict)
        calibrations.append(calibration)
    return calibrations


def check_comparability(results: list[dict]) -> list[str]:
    warnings: list[str] = []
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for result in results:
        grouped[(result["suite_id"], result["condition_id"])].append(result)

    for suite_id, condition_id in grouped:
        rows = grouped[(suite_id, condition_id)]
        if {row["system"] for row in rows} != {"kafka", "sidekiq"}:
            continue
        for field in COMPARABILITY_FIELDS:
            values = {row["config"].get(field) for row in rows}
            if len(values) > 1:
                warnings.append(
                    f"Comparability warning for {suite_id}/{condition_id}: {field} differs across systems"
                )

    warnings.extend(_kafka_partition_cap_warnings(results))
    warnings.extend(_kafka_operating_point_warnings(results))
    return warnings


def build_markdown(
    results: list[dict],
    calibrations: list[dict],
    warnings: list[str],
    charts: dict[str, list[RenderedChart]],
) -> str:
    present_suite_ids = {result["suite_id"] for result in results}
    lines = [
        "# Kafka vs Sidekiq Benchmark Report",
        "",
        "This report summarizes validated stored results from `results/final/`.",
        "",
        "Kafka runs as single-node KRaft with replication factor `1`.",
        "Sidekiq uses `retry: false`.",
        "Reliability, failure recovery, retries, fan-out, and fault injection are out of scope.",
        "",
        "## Suites",
        "",
    ]
    for suite in list_suites():
        if suite["suite_id"] in present_suite_ids:
            lines.append(f"- `{suite['suite_id']}`")

    lines.extend(["", "## Calibration", ""])
    if calibrations:
        for calibration in calibrations:
            config = calibration["config"]
            lines.append(
                f"- {calibration['timestamp']}: theoretical ceiling {float(calibration['theoretical_max_rps']):.2f} rps with {config['stub_workers']} workers at {config['delay_ms']} ms"
            )
    else:
        lines.append("- No stub calibration artifacts were loaded.")
    lines.extend(["", _kafka_operating_point_text(results)])

    for suite in list_suites():
        suite_id = suite["suite_id"]
        report = SUITE_REPORTS.get(suite_id)
        if report is None or suite_id not in present_suite_ids:
            continue
        lines.extend(
            [
                "",
                f"## `{suite_id}`",
                "",
                suite["description"],
                "",
                _suite_section(results, calibrations, suite, report, charts),
            ]
        )

    lines.extend(
        [
            "",
            "## Appendix",
            "",
            "### Resource Usage",
            "",
            "Resource usage is coarse SUT-only context.",
            "Mean SUT memory is charted here because the gap is large and stable; CPU is noisier and harder to interpret from coarse Docker stats.",
            "Kafka totals include `kafka_broker` and `kafka_consumer`. Sidekiq totals include `redis_broker` and `sidekiq_worker`.",
            f"Only paired workflow runs with at least {MIN_RESOURCE_USAGE_SAMPLES} samples and complete role coverage are included.",
            "",
            _appendix_resource_memory_chart(charts),
            "",
            _resource_appendix(results),
        ]
    )
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
    lines.append("")
    return "\n".join(lines)


def _suite_section(
    results: list[dict],
    calibrations: list[dict],
    suite: dict[str, str],
    report: SuiteReport,
    charts: dict[str, list[RenderedChart]],
) -> str:
    suite_id = suite["suite_id"]
    parts: list[str] = []
    if suite_id == "io_bound_matched_concurrency":
        parts.append(_io_bound_calibration_table(results, calibrations))
        parts.append(_suite_charts(charts, suite_id))
        parts.append(_metric_table(results, suite_id, suite["systems"], report["first_column"], report["title"], "consumed_msg_per_sec"))
        parts.append(
            _metric_table(
                results,
                suite_id,
                suite["systems"],
                report["first_column"],
                "I/O-bound wait to start",
                "wait_to_start_mean_ms",
            )
        )
    elif suite_id == "bursty_ingest_shape":
        parts.append(_suite_charts(charts, suite_id))
        parts.append(_metric_table(results, suite_id, suite["systems"], report["first_column"], report["title"], "drain_ms"))
        parts.append(
            _metric_table(
                results,
                suite_id,
                suite["systems"],
                report["first_column"],
                "Bursty ingest wait to start",
                "wait_to_start_mean_ms",
            )
        )
    elif suite_id == "delayed_case_study":
        parts.append(_delayed_table(results))
    else:
        parts.append(_suite_charts(charts, suite_id))
        parts.append(_suite_table(results, suite_id, suite["systems"], report))
    return "\n\n".join(part for part in parts if part)


def _suite_table(results: list[dict], suite_id: str, systems: list[str], report: SuiteReport) -> str:
    return _metric_table(results, suite_id, systems, report["first_column"], report["title"], report["metric"])


def _metric_table(
    results: list[dict],
    suite_id: str,
    systems: list[str],
    first_column: str,
    title: str,
    metric: MetricName,
) -> str:
    grouped = _group_metric(results, suite_id, metric)
    headers = " | ".join(system.title() for system in systems)
    lines = [
        f"### {title}",
        "",
        f"| {first_column} | {headers} |",
        f"|---|{'---:|' * len(systems)}",
    ]
    for condition_id in _ordered_condition_ids(suite_id, grouped):
        values = " | ".join(_format_metric(metric, grouped[condition_id].get(system)) for system in systems)
        lines.append(f"| `{_condition_label(condition_id)}` | {values} |")
    return "\n".join(lines)


def _suite_charts(charts: dict[str, list[RenderedChart]], suite_id: str) -> str:
    lines: list[str] = []
    for chart in charts.get(suite_id, []):
        lines.extend([f"### {chart['title']}", "", f"![{chart['title']}]({chart['path']})", ""])
    return "\n".join(lines).strip()


def _render_charts(results: list[dict], chart_dir: Path) -> dict[str, list[RenderedChart]]:
    chart_dir.mkdir(parents=True, exist_ok=True)
    rendered: dict[str, list[RenderedChart]] = {}
    for suite in list_suites():
        suite_id = suite["suite_id"]
        charts: list[RenderedChart] = []
        for index, spec in enumerate(_chart_specs(suite_id), start=1):
            chart = _chart_data(results, suite_id, spec)
            if chart is None:
                continue
            file_name = f"{suite_id}-{index:02d}.png"
            _save_chart(chart_dir / file_name, spec, chart["labels"], chart["series"])
            charts.append({"title": spec["title"], "path": f"charts/{file_name}"})
        if charts:
            rendered[suite_id] = charts
    appendix_chart = _render_appendix_resource_memory_chart(results, chart_dir)
    if appendix_chart is not None:
        rendered[APPENDIX_RESOURCE_MEMORY_CHART_KEY] = [appendix_chart]
    return rendered


def _chart_specs(suite_id: str) -> list[ChartSpec]:
    match suite_id:
        case "kafka_partition_adequacy":
            return [
                {
                    "kind": "line",
                    "title": "Kafka partition adequacy",
                    "metric": "consumed_msg_per_sec",
                    "axis_title": "Partitions",
                    "y_axis": "Throughput (msg/s)",
                    "y_scale": "linear",
                    "systems": ["kafka"],
                }
            ]
        case "parallelism_decomposition":
            return [
                {
                    "kind": "bar",
                    "title": "Kafka parallelism decomposition",
                    "metric": "consumed_msg_per_sec",
                    "axis_title": "Profile",
                    "y_axis": "Throughput (msg/s)",
                    "y_scale": "linear",
                    "systems": ["kafka"],
                },
                {
                    "kind": "bar",
                    "title": "Sidekiq parallelism decomposition",
                    "metric": "consumed_msg_per_sec",
                    "axis_title": "Profile",
                    "y_axis": "Throughput (msg/s)",
                    "y_scale": "linear",
                    "systems": ["sidekiq"],
                },
            ]
        case "cpu_light_matched_concurrency":
            return [
                {
                    "kind": "line",
                    "title": "CPU-light throughput",
                    "metric": "consumed_msg_per_sec",
                    "axis_title": "Nominal target",
                    "y_axis": "Throughput (msg/s)",
                    "y_scale": "linear",
                    "systems": ["kafka", "sidekiq"],
                },
                {
                    "kind": "line",
                    "title": "CPU-light wait to start",
                    "metric": "wait_to_start_mean_ms",
                    "axis_title": "Nominal target",
                    "y_axis": "Mean wait to start (ms)",
                    "y_scale": "linear",
                    "systems": ["kafka", "sidekiq"],
                },
            ]
        case "io_bound_matched_concurrency":
            return [
                {
                    "kind": "line",
                    "title": "I/O-bound throughput",
                    "metric": "consumed_msg_per_sec",
                    "axis_title": "Nominal target",
                    "y_axis": "Throughput (msg/s)",
                    "y_scale": "linear",
                    "systems": ["kafka", "sidekiq"],
                },
                {
                    "kind": "line",
                    "title": "I/O-bound wait to start",
                    "metric": "wait_to_start_mean_ms",
                    "axis_title": "Nominal target",
                    "y_axis": "Mean wait to start (ms)",
                    "y_scale": "linear",
                    "systems": ["kafka", "sidekiq"],
                },
            ]
        case "bursty_ingest_shape":
            return [
                {
                    "kind": "bar",
                    "title": "Bursty ingest drain",
                    "metric": "drain_ms",
                    "axis_title": "Shape",
                    "y_axis": "Drain (ms)",
                    "y_scale": "log",
                    "systems": ["kafka", "sidekiq"],
                },
                {
                    "kind": "bar",
                    "title": "Bursty ingest wait to start",
                    "metric": "wait_to_start_mean_ms",
                    "axis_title": "Shape",
                    "y_axis": "Mean wait to start (ms)",
                    "y_scale": "log",
                    "systems": ["kafka", "sidekiq"],
                }
            ]
        case "delayed_case_study":
            return []
    raise AssertionError(suite_id)


def _chart_data(
    results: list[dict],
    suite_id: str,
    spec: ChartSpec,
) -> dict[str, object] | None:
    grouped = _group_metric(results, suite_id, spec["metric"])
    condition_ids = [
        condition_id
        for condition_id in _ordered_condition_ids(suite_id, grouped)
        if all(grouped[condition_id].get(system) is not None for system in spec["systems"])
    ]
    if not condition_ids:
        return None
    return {
        "labels": [_condition_label(condition_id) for condition_id in condition_ids],
        "series": {
            system: [grouped[condition_id][system][0] for condition_id in condition_ids]
            for system in spec["systems"]
        },
    }


def _save_chart(
    path: Path,
    spec: ChartSpec,
    labels: list[str],
    series: dict[str, list[float]],
) -> None:
    figure, axis = plt.subplots(figsize=(7.2, 4.0), layout="constrained")
    positions = list(range(len(labels)))

    match spec["kind"]:
        case "line":
            for system in spec["systems"]:
                axis.plot(
                    positions,
                    series[system],
                    color=_chart_series_color(system),
                    label=system.title(),
                    linewidth=2.0,
                    marker="o",
                )
        case "bar":
            width = 0.8 / len(spec["systems"])
            center = (len(spec["systems"]) - 1) / 2
            for index, system in enumerate(spec["systems"]):
                offset = (index - center) * width
                axis.bar(
                    [position + offset for position in positions],
                    series[system],
                    width=width,
                    color=_chart_series_color(system),
                    label=system.title(),
                )
        case _:
            raise AssertionError(spec["kind"])

    axis.set_title(spec["title"])
    axis.set_xlabel(spec["axis_title"])
    axis.set_ylabel(spec["y_axis"])
    axis.set_xticks(positions, labels)
    lower_bound, upper_bound = _chart_bounds(spec, series)
    if spec["y_scale"] == "log":
        axis.set_yscale("log")
        axis.yaxis.set_major_locator(ticker.LogLocator(base=10.0))
        axis.yaxis.set_major_formatter(ticker.FuncFormatter(_format_log_tick))
        axis.yaxis.set_minor_locator(ticker.NullLocator())
    axis.set_ylim(lower_bound, upper_bound)
    axis.grid(axis="y", alpha=0.25)
    if len(spec["systems"]) > 1:
        axis.legend()
    figure.savefig(path, dpi=150)
    plt.close(figure)


def _chart_series_color(system: str) -> str:
    match system:
        case "kafka":
            return "#1f77b4"
        case "sidekiq":
            return "#d62728"
    raise AssertionError(system)


def _group_metric(
    results: list[dict],
    suite_id: str,
    metric: MetricName,
) -> dict[str, dict[str, tuple[float, float]]]:
    grouped: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for result in results:
        if result["suite_id"] != suite_id:
            continue
        grouped[result["condition_id"]][result["system"]].append(_metric_value(result, metric))
    return {
        condition_id: {
            system: (mean(values), 0.0 if len(values) == 1 else stdev(values))
            for system, values in systems.items()
        }
        for condition_id, systems in grouped.items()
    }


def _metric_value(result: dict, metric: MetricName) -> float:
    match metric:
        case "consumed_msg_per_sec":
            return float(result["statistics"]["throughput"]["consumed_msg_per_sec"])
        case "wait_to_start_mean_ms":
            return float(result["statistics"]["wait_to_start_ms"]["mean"])
        case "drain":
            return float(result["statistics"]["timing_s"]["drain"])
        case "drain_ms":
            return float(result["statistics"]["timing_s"]["drain"]) * 1000.0
        case "start_lateness_p95":
            return float(result["statistics"]["start_lateness_ms"]["p95"])
    raise AssertionError(metric)


def _format_metric(metric: MetricName, value: tuple[float, float] | None) -> str:
    if value is None:
        return "n/a"
    avg, spread = value
    if metric == "drain" and abs(avg) < 1.0:
        return f"{avg * 1000.0:.2f} +/- {spread * 1000.0:.2f} ms"
    suffix = " s" if metric == "drain" else " ms" if metric in {"drain_ms", "start_lateness_p95"} else ""
    return f"{avg:.2f} +/- {spread:.2f}{suffix}"


def _ordered_condition_ids(
    suite_id: str,
    rows: dict[str, dict[str, tuple[float, float]]],
) -> list[str]:
    manifest_order = list(dict.fromkeys(row["condition_id"] for row in expand_suite(suite_id)))
    index = {condition_id: position for position, condition_id in enumerate(manifest_order)}
    return sorted(rows, key=lambda condition_id: (index.get(condition_id, len(index)), _sort_key(condition_id)))


def _sort_key(condition_id: str) -> tuple[int, str, int | str]:
    prefix, separator, value = condition_id.partition("=")
    if separator and value.isdigit():
        return (0, prefix, int(value))
    return (1, prefix, value)


def _condition_label(condition_id: str) -> str:
    return condition_id.split("=", 1)[1] if "=" in condition_id else condition_id


def _resource_appendix(results: list[dict]) -> str:
    present_suite_ids = {result["suite_id"] for result in results}
    sections = []
    for suite in list_suites():
        if suite["suite_id"] not in present_suite_ids or len(suite["systems"]) < 2:
            continue
        sections.extend([f"#### `{suite['suite_id']}`", "", _resource_totals_table(results, suite["suite_id"])])
    return "\n".join(sections)


def _resource_totals_table(results: list[dict], suite_id: str) -> str:
    summary = _summarize_resource_runs(results, suite_id)
    if not summary:
        return f"_Resource usage is omitted from interpretation for `{suite_id}` because paired SUT samples were insufficient or incomplete._"
    lines = ["| System | Included runs | Mean CPU % | Mean memory MB |", "|---|---:|---:|---:|"]
    for system in ("kafka", "sidekiq"):
        values = summary[system]
        lines.append(f"| `{system}` | {values['runs']} | {values['cpu_pct_mean']:.2f} | {values['mem_mb_mean']:.2f} |")
    return "\n".join(lines)


def _appendix_resource_memory_chart(charts: dict[str, list[RenderedChart]]) -> str:
    rendered = charts.get(APPENDIX_RESOURCE_MEMORY_CHART_KEY, [])
    if not rendered:
        return ""
    chart = rendered[0]
    return "\n".join(
        [
            f"### {chart['title']}",
            "",
            f"![{chart['title']}]({chart['path']})",
            "",
            "_Supporting context only._",
        ]
    )


def _render_appendix_resource_memory_chart(results: list[dict], chart_dir: Path) -> RenderedChart | None:
    labels: list[str] = []
    series: dict[str, list[float]] = {"kafka": [], "sidekiq": []}
    for suite_id in APPENDIX_RESOURCE_MEMORY_SUITES:
        summary = _summarize_resource_runs(results, suite_id)
        if not summary:
            continue
        labels.append(suite_id)
        for system in ("kafka", "sidekiq"):
            series[system].append(float(summary[system]["mem_mb_mean"]))

    if not labels:
        return None

    file_name = "appendix_resource_memory-01.png"
    _save_chart(
        chart_dir / file_name,
        {
            "kind": "bar",
            "title": "Mean SUT memory by suite",
            "metric": "consumed_msg_per_sec",
            "axis_title": "Suite",
            "y_axis": "Mean memory (MB)",
            "y_scale": "linear",
            "systems": ["kafka", "sidekiq"],
        },
        labels,
        series,
    )
    return {"title": "Mean SUT memory by suite", "path": f"charts/{file_name}"}


def _summarize_resource_runs(results: list[dict], suite_id: str) -> dict[str, dict[str, float | int]]:
    paired: dict[tuple[str, str, int], dict[str, dict]] = defaultdict(dict)
    for result in results:
        if result["suite_id"] != suite_id:
            continue
        paired[(result["suite_id"], result["condition_id"], int(result["repeat_index"]))][result["system"]] = result

    eligible: list[dict] = []
    for rows in paired.values():
        if set(rows) == {"kafka", "sidekiq"} and all(_resource_run_is_eligible(row) for row in rows.values()):
            eligible.extend([rows["kafka"], rows["sidekiq"]])
    if not eligible:
        return {}

    summary: dict[str, dict[str, float | int]] = {}
    for system in ("kafka", "sidekiq"):
        totals = [_resource_total(row) for row in eligible if row["system"] == system]
        summary[system] = {
            "runs": len(totals),
            "cpu_pct_mean": mean(cpu for cpu, _ in totals),
            "mem_mb_mean": mean(mem for _, mem in totals),
        }
    return summary


def _resource_run_is_eligible(result: dict) -> bool:
    usage = result.get("resource_usage")
    return bool(
        usage
        and int(usage.get("samples", 0)) >= MIN_RESOURCE_USAGE_SAMPLES
        and SYSTEM_UNDER_TEST_ROLES[result["system"]].issubset(usage["by_role"])
    )


def _resource_total(result: dict) -> tuple[float, float]:
    usage = result["resource_usage"]["by_role"]
    cpu = sum(float(usage[role]["cpu_pct"]["mean"]) for role in SYSTEM_UNDER_TEST_ROLES[result["system"]])
    mem = sum(float(usage[role]["mem_mb"]["mean"]) for role in SYSTEM_UNDER_TEST_ROLES[result["system"]])
    return cpu, mem


def _kafka_partition_cap_warnings(results: list[dict]) -> list[str]:
    warnings: list[str] = []
    seen: set[tuple[str, str]] = set()
    for result in results:
        if result["system"] != "kafka":
            continue
        key = (result["suite_id"], result["condition_id"])
        if key in seen:
            continue
        seen.add(key)
        configured = int(result["config"].get("kafka_configured_consumer_instances") or 0)
        cap = int(result["config"].get("kafka_assignment_parallelism_upper_bound") or 0)
        if configured and cap < configured:
            warnings.append(
                f"Partition-cap warning for {result['suite_id']}/{result['condition_id']}: Kafka configures {configured} consumer instances but partitions cap assignment parallelism at {cap}"
            )
    return warnings


def _kafka_operating_point_text(results: list[dict]) -> str:
    analysis = _kafka_operating_point(results)
    if analysis is None:
        return "No Kafka comparative operating-point analysis was possible."
    prefix = (
        f"Kafka comparative runs use {analysis['comparative_partitions_text']} partitions and up to {analysis['max_instances']} consumer instances."
    )
    if not analysis["means_by_partition"]:
        return f"{prefix} No adequacy sweep is loaded to defend that choice."
    adequacy = ", ".join(
        f"{partitions} -> {throughput:.2f}"
        for partitions, throughput in sorted(analysis["means_by_partition"].items())
    )
    plateau = _format_ints(analysis["supported_partitions"])
    if analysis["unsupported_partitions"] or analysis["below_instance_floor"]:
        return (
            f"{prefix} Adequacy sweep throughput by partition: {adequacy} msg/s. "
            f"The {int(PARTITION_ADEQUACY_PLATEAU_FRACTION * 100)}% plateau is {plateau}, so the current comparative operating point is not fully defended."
        )
    return (
        f"{prefix} Adequacy sweep throughput by partition: {adequacy} msg/s. "
        f"The {int(PARTITION_ADEQUACY_PLATEAU_FRACTION * 100)}% plateau is {plateau}, and the comparative runs stay on it."
    )


def _kafka_operating_point_warnings(results: list[dict]) -> list[str]:
    analysis = _kafka_operating_point(results)
    if analysis is None:
        return []
    if not analysis["means_by_partition"]:
        return [
            f"Operating-point warning: comparative Kafka runs use {analysis['comparative_partitions_text']} partitions and up to {analysis['max_instances']} consumer instances, but no adequacy sweep is loaded"
        ]
    reasons = []
    if analysis["unsupported_partitions"]:
        reasons.append(f"{_format_ints(analysis['unsupported_partitions'])} is outside the adequacy plateau")
    if analysis["below_instance_floor"]:
        reasons.append(
            f"{_format_ints(analysis['below_instance_floor'])} is below the maximum configured instance count of {analysis['max_instances']}"
        )
    return [] if not reasons else [f"Operating-point warning: {' and '.join(reasons)}"]


def _kafka_operating_point(results: list[dict]) -> dict[str, object] | None:
    comparative_suite_ids = {suite["suite_id"] for suite in list_suites() if suite["type"] == "comparative"}
    comparative = [
        row
        for row in results
        if row["system"] == "kafka" and row["suite_id"] in comparative_suite_ids
    ]
    if not comparative:
        return None

    comparative_partitions = sorted(
        {
            int(row["config"]["requested_partitions"])
            for row in comparative
            if row["config"].get("requested_partitions") is not None
        }
    )
    if not comparative_partitions:
        return None

    means_by_partition = {
        partitions: mean(values)
        for partitions, values in _group_values(
            results,
            suite_id="kafka_partition_adequacy",
            system="kafka",
            key="requested_partitions",
            metric="consumed_msg_per_sec",
        ).items()
    }
    supported_partitions: list[int] = []
    if means_by_partition:
        best = max(means_by_partition.values())
        supported_partitions = [
            partitions
            for partitions, throughput in means_by_partition.items()
            if throughput >= best * PARTITION_ADEQUACY_PLATEAU_FRACTION
        ]

    max_instances = max(int(row["config"]["kafka_configured_consumer_instances"]) for row in comparative)
    return {
        "comparative_partitions_text": _format_ints(comparative_partitions),
        "means_by_partition": means_by_partition,
        "supported_partitions": sorted(supported_partitions),
        "unsupported_partitions": [
            partitions
            for partitions in comparative_partitions
            if means_by_partition and partitions not in supported_partitions
        ],
        "below_instance_floor": [partitions for partitions in comparative_partitions if partitions < max_instances],
        "max_instances": max_instances,
    }


def _group_values(
    results: list[dict],
    *,
    suite_id: str,
    system: str,
    key: str,
    metric: MetricName,
) -> dict[int, list[float]]:
    grouped: dict[int, list[float]] = defaultdict(list)
    for result in results:
        if result["suite_id"] == suite_id and result["system"] == system:
            grouped[int(result["config"][key])].append(_metric_value(result, metric))
    return grouped


def _format_ints(values: list[int]) -> str:
    return ", ".join(str(value) for value in values) if values else "none"


def _delayed_table(results: list[dict]) -> str:
    grouped: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for result in results:
        if result["suite_id"] != "delayed_case_study":
            continue
        grouped[result["system"]]["mean"].append(float(result["statistics"]["start_lateness_ms"]["mean"]))
        grouped[result["system"]]["p95"].append(float(result["statistics"]["start_lateness_ms"]["p95"]))
        grouped[result["system"]]["p99"].append(float(result["statistics"]["start_lateness_ms"]["p99"]))
        grouped[result["system"]]["jitter"].append(float(result["statistics"]["scheduling"]["jitter_ms"]))
        grouped[result["system"]]["on_time_500"].append(float(result["statistics"]["scheduling"]["on_time_pct_500ms"]))

    lines = [
        "### Delayed start lateness",
        "",
        "| System | Mean lateness ms | P95 ms | P99 ms | Jitter ms | On-time <= 500 ms % |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for system in ("kafka", "sidekiq"):
        values = grouped.get(system)
        if not values:
            lines.append(f"| `{system}` | n/a | n/a | n/a | n/a | n/a |")
            continue
        lines.append(
            f"| `{system}` | {mean(values['mean']):.2f} | {mean(values['p95']):.2f} | {mean(values['p99']):.2f} | {mean(values['jitter']):.2f} | {mean(values['on_time_500']):.2f} |"
        )
    return "\n".join(lines)


def _io_bound_calibration_table(results: list[dict], calibrations: list[dict]) -> str:
    calibration = _matching_stub_calibration(results, "io_bound_matched_concurrency", calibrations)
    if calibration is None:
        return "_No matching stub calibration artifact was loaded for `io_bound_matched_concurrency`, so the suite-local ceiling comparison is unavailable._"

    ceiling = float(calibration["theoretical_max_rps"])
    grouped = _group_metric(results, "io_bound_matched_concurrency", "consumed_msg_per_sec")
    lines = [
        "### I/O-bound calibrated stub ceiling",
        "",
        (
            "Latest matching stub calibration reports a theoretical ceiling of "
            f"{ceiling:.2f} rps with {int(calibration['config']['stub_workers'])} workers "
            f"at {int(calibration['config']['delay_ms'])} ms."
        ),
        "",
        "| Nominal target | Kafka % of ceiling | Sidekiq % of ceiling |",
        "|---|---:|---:|",
    ]
    for condition_id in _ordered_condition_ids("io_bound_matched_concurrency", grouped):
        values = []
        for system in ("kafka", "sidekiq"):
            metric = grouped[condition_id].get(system)
            if metric is None:
                values.append("n/a")
                continue
            values.append(f"{(metric[0] / ceiling) * 100.0:.1f}%")
        lines.append(f"| `{_condition_label(condition_id)}` | {values[0]} | {values[1]} |")
    return "\n".join(lines)


def _matching_stub_calibration(results: list[dict], suite_id: str, calibrations: list[dict]) -> dict | None:
    suite_delays = {
        int(delay_ms)
        for result in results
        if result["suite_id"] == suite_id
        for delay_ms in [result["config"].get("stub_delay_ms")]
        if delay_ms is not None
    }
    matching = [
        calibration
        for calibration in calibrations
        if not suite_delays or int(calibration["config"]["delay_ms"]) in suite_delays
    ]
    if not matching:
        return None
    return max(matching, key=lambda calibration: str(calibration["timestamp"]))


def _chart_upper_bound(value: float) -> int | float:
    if value <= 0:
        return 1
    return round(value * 1.1, 2)


def _chart_bounds(spec: ChartSpec, series: dict[str, list[float]]) -> tuple[float, float]:
    values = [value for values in series.values() for value in values]
    max_value = max(values)
    if spec["y_scale"] == "linear":
        return 0.0, _chart_upper_bound(max_value)

    positive_values = [value for value in values if value > 0]
    if not positive_values:
        return 0.0, _chart_upper_bound(max_value)

    min_positive = min(positive_values)
    lower_bound = 10 ** math.floor(math.log10(min_positive))
    if math.isclose(lower_bound, min_positive):
        lower_bound /= 10.0
    return lower_bound, _log_chart_upper_bound(max_value)


def _format_log_tick(value: float, _: object) -> str:
    if value <= 0:
        return ""
    return f"{value:g}"


def _log_chart_upper_bound(value: float) -> float:
    if value <= 0:
        return 1.0
    magnitude = 10 ** math.floor(math.log10(value))
    normalized = value / magnitude
    for step in (1.5, 2.0, 3.0, 5.0, 10.0):
        if normalized <= step:
            return step * magnitude
    return 10.0 * magnitude
