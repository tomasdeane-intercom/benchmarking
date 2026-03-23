import json
from pathlib import Path

from benchkit.reporting import (
    APPENDIX_RESOURCE_MEMORY_CHART_KEY,
    _chart_specs,
    build_markdown,
    check_comparability,
    generate_report,
)
from tests.support import result_payload


def test_generate_report_writes_markdown_renders_charts_and_surfaces_load_warnings(tmp_path: Path):
    results_dir = tmp_path / "results"
    report_dir = tmp_path / "report"
    results_dir.mkdir()

    (results_dir / "kafka.json").write_text(
        json.dumps(
            _sample_result(
                suite_id="cpu_light_matched_concurrency",
                condition_id="concurrency=1",
                system="kafka",
                throughput=100.0,
                partitions=1,
            )
        )
    )
    (results_dir / "sidekiq.json").write_text(
        json.dumps(
            _sample_result(
                suite_id="cpu_light_matched_concurrency",
                condition_id="concurrency=1",
                system="sidekiq",
                throughput=90.0,
            )
        )
    )
    (results_dir / "bad.json").write_text('{"schema_version":"9.9.9"}')

    report_path, warnings = generate_report(results_dir, report_dir)
    markdown = report_path.read_text()

    assert "## `cpu_light_matched_concurrency`" in markdown
    assert "![CPU-light throughput](charts/cpu_light_matched_concurrency-01.png)" in markdown
    assert "![CPU-light wait to start](charts/cpu_light_matched_concurrency-02.png)" in markdown
    assert "```mermaid" not in markdown
    assert (report_dir / "charts" / "cpu_light_matched_concurrency-01.png").exists()
    assert (report_dir / "charts" / "cpu_light_matched_concurrency-02.png").exists()
    assert "## Warnings" in markdown
    assert any("bad.json" in warning for warning in warnings)


def test_build_markdown_reports_operating_point_gaps():
    results = [
        _sample_result(
            suite_id="kafka_partition_adequacy",
            condition_id="partitions=4",
            system="kafka",
            throughput=8200.0,
            partitions=4,
            consumer_instances=4,
        ),
        _sample_result(
            suite_id="kafka_partition_adequacy",
            condition_id="partitions=6",
            system="kafka",
            throughput=6200.0,
            partitions=6,
            consumer_instances=4,
        ),
        _sample_result(
            suite_id="kafka_partition_adequacy",
            condition_id="partitions=16",
            system="kafka",
            throughput=8400.0,
            partitions=16,
            consumer_instances=4,
        ),
        _sample_result(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=16",
            system="kafka",
            throughput=2300.0,
            partitions=6,
            consumer_instances=16,
        ),
        _sample_result(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=16",
            system="sidekiq",
            throughput=1000.0,
            consumer_instances=16,
        ),
    ]

    warnings = check_comparability(results)
    markdown = build_markdown(results, [], warnings, {})

    assert any("Operating-point warning" in warning for warning in warnings)
    assert "not fully defended" in markdown
    assert "## `kafka_partition_adequacy`" in markdown


def test_build_markdown_uses_resource_pairs_and_formats_bursty_metrics_in_ms():
    resource_usage = {
        "samples": 3,
        "by_role": {
            "kafka_broker": {"cpu_pct": {"mean": 10.0, "peak": 10.0}, "mem_mb": {"mean": 100.0, "peak": 100.0}},
            "kafka_consumer": {"cpu_pct": {"mean": 20.0, "peak": 20.0}, "mem_mb": {"mean": 30.0, "peak": 30.0}},
            "redis_broker": {"cpu_pct": {"mean": 5.0, "peak": 5.0}, "mem_mb": {"mean": 20.0, "peak": 20.0}},
            "sidekiq_worker": {"cpu_pct": {"mean": 7.0, "peak": 7.0}, "mem_mb": {"mean": 25.0, "peak": 25.0}},
        },
    }
    results = [
        _sample_result(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=1",
            system="kafka",
            throughput=100.0,
            partitions=1,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=1",
            system="sidekiq",
            throughput=90.0,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if not k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="bursty_ingest_shape",
            condition_id="produce_shape=steady",
            system="kafka",
            throughput=10.0,
        ),
        _sample_result(
            suite_id="bursty_ingest_shape",
            condition_id="produce_shape=steady",
            system="sidekiq",
            throughput=10.0,
        ),
    ]
    results[2]["statistics"]["timing_s"]["drain"] = 0.0042
    results[3]["statistics"]["timing_s"]["drain"] = 0.0051
    results[2]["statistics"]["wait_to_start_ms"]["mean"] = 6.25
    results[3]["statistics"]["wait_to_start_ms"]["mean"] = 7.50

    markdown = build_markdown(
        results,
        [],
        [],
        {
            "bursty_ingest_shape": [
                {"title": "Bursty ingest drain", "path": "charts/bursty_ingest_shape-01.png"},
                {"title": "Bursty ingest wait to start", "path": "charts/bursty_ingest_shape-02.png"},
            ]
        },
    )

    assert "### Bursty ingest drain" in markdown
    assert "![Bursty ingest drain](charts/bursty_ingest_shape-01.png)" in markdown
    assert "### Bursty ingest wait to start" in markdown
    assert "![Bursty ingest wait to start](charts/bursty_ingest_shape-02.png)" in markdown
    assert "#### `cpu_light_matched_concurrency`" in markdown
    assert "| `kafka` | 1 | 30.00 | 130.00 |" in markdown
    assert "| `sidekiq` | 1 | 12.00 | 45.00 |" in markdown
    assert "| `steady` | 4.20 +/- 0.00 ms | 5.10 +/- 0.00 ms |" in markdown
    assert "| `steady` | 6.25 +/- 0.00 | 7.50 +/- 0.00 |" in markdown


def test_build_markdown_places_appendix_resource_memory_chart_before_resource_tables():
    resource_usage = {
        "samples": 3,
        "by_role": {
            "kafka_broker": {"cpu_pct": {"mean": 10.0, "peak": 10.0}, "mem_mb": {"mean": 100.0, "peak": 100.0}},
            "kafka_consumer": {"cpu_pct": {"mean": 20.0, "peak": 20.0}, "mem_mb": {"mean": 30.0, "peak": 30.0}},
            "redis_broker": {"cpu_pct": {"mean": 5.0, "peak": 5.0}, "mem_mb": {"mean": 20.0, "peak": 20.0}},
            "sidekiq_worker": {"cpu_pct": {"mean": 7.0, "peak": 7.0}, "mem_mb": {"mean": 25.0, "peak": 25.0}},
        },
    }
    results = [
        _sample_result(
            suite_id="io_bound_matched_concurrency",
            condition_id="concurrency=4",
            system="kafka",
            throughput=80.0,
            consumer_instances=4,
            stub_delay_ms=50,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="io_bound_matched_concurrency",
            condition_id="concurrency=4",
            system="sidekiq",
            throughput=100.0,
            consumer_instances=4,
            stub_delay_ms=50,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if not k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="delayed_case_study",
            condition_id="delayed_case",
            system="kafka",
            throughput=10.0,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="delayed_case_study",
            condition_id="delayed_case",
            system="sidekiq",
            throughput=10.0,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if not k.startswith("kafka")}},
        ),
    ]
    results[2]["statistics"]["start_lateness_ms"] = {"min": 10.0, "max": 30.0, "mean": 15.0, "median": 15.0, "p95": 25.0, "p99": 29.0, "stddev": 4.0}
    results[2]["statistics"]["scheduling"] = {"total_scheduled": 1000, "early_start_count": 0, "on_time_pct_100ms": 100.0, "on_time_pct_500ms": 100.0, "jitter_ms": 4.0}
    results[3]["statistics"]["start_lateness_ms"] = {"min": 20.0, "max": 40.0, "mean": 22.0, "median": 22.0, "p95": 35.0, "p99": 39.0, "stddev": 6.0}
    results[3]["statistics"]["scheduling"] = {"total_scheduled": 1000, "early_start_count": 0, "on_time_pct_100ms": 100.0, "on_time_pct_500ms": 100.0, "jitter_ms": 6.0}

    markdown = build_markdown(
        results,
        [],
        [],
        {
            APPENDIX_RESOURCE_MEMORY_CHART_KEY: [
                {"title": "Mean SUT memory by suite", "path": "charts/appendix_resource_memory-01.png"}
            ]
        },
    )

    assert "Mean SUT memory is charted here because the gap is large and stable" in markdown
    assert "### Mean SUT memory by suite" in markdown
    assert "![Mean SUT memory by suite](charts/appendix_resource_memory-01.png)" in markdown
    assert "_Supporting context only._" in markdown
    assert markdown.index("### Mean SUT memory by suite") < markdown.index("#### `io_bound_matched_concurrency`")


def test_bursty_ingest_chart_uses_log_scale():
    specs = _chart_specs("bursty_ingest_shape")

    assert specs[0]["metric"] == "drain_ms"
    assert specs[0]["y_scale"] == "log"
    assert specs[0]["y_axis"] == "Drain (ms)"
    assert specs[1]["metric"] == "wait_to_start_mean_ms"
    assert specs[1]["y_scale"] == "log"
    assert specs[1]["y_axis"] == "Mean wait to start (ms)"


def test_build_markdown_surfaces_io_calibration_inside_suite_section():
    calibration = {
        "timestamp": "2026-03-22T18:29:45.183089+00:00",
        "type": "stub_capacity_calibration",
        "config": {"stub_workers": 20, "delay_ms": 50},
        "theoretical_max_rps": 400.0,
    }
    results = [
        _sample_result(
            suite_id="io_bound_matched_concurrency",
            condition_id="concurrency=4",
            system="kafka",
            throughput=80.0,
            consumer_instances=4,
            stub_delay_ms=50,
        ),
        _sample_result(
            suite_id="io_bound_matched_concurrency",
            condition_id="concurrency=4",
            system="sidekiq",
            throughput=100.0,
            consumer_instances=4,
            stub_delay_ms=50,
        ),
    ]
    results[0]["statistics"]["wait_to_start_ms"]["mean"] = 12.0
    results[1]["statistics"]["wait_to_start_ms"]["mean"] = 10.0

    markdown = build_markdown(
        results,
        [calibration],
        [],
        {
            "io_bound_matched_concurrency": [
                {"title": "I/O-bound throughput", "path": "charts/io_bound_matched_concurrency-01.png"},
                {"title": "I/O-bound wait to start", "path": "charts/io_bound_matched_concurrency-02.png"},
            ]
        },
    )

    assert "### I/O-bound calibrated stub ceiling" in markdown
    assert "theoretical ceiling of 400.00 rps with 20 workers at 50 ms" in markdown
    assert "| `4` | 20.0% | 25.0% |" in markdown
    assert "### I/O-bound wait to start" in markdown
    assert "| `4` | 12.00 +/- 0.00 | 10.00 +/- 0.00 |" in markdown


def test_generate_report_renders_matplotlib_charts_in_manifest_order(tmp_path: Path):
    results_dir = tmp_path / "results"
    report_dir = tmp_path / "report"
    results_dir.mkdir()

    rows = []
    for concurrency, kafka_throughput, sidekiq_throughput, kafka_wait, sidekiq_wait in (
        (1, 100.0, 90.0, 2.0, 3.0),
        (4, 380.0, 340.0, 4.0, 5.0),
        (8, 640.0, 610.0, 8.0, 9.0),
        (16, 900.0, 880.0, 16.0, 18.0),
    ):
        for system, throughput, wait in (("kafka", kafka_throughput, kafka_wait), ("sidekiq", sidekiq_throughput, sidekiq_wait)):
            result = _sample_result(
                suite_id="cpu_light_matched_concurrency",
                condition_id=f"concurrency={concurrency}",
                system=system,
                throughput=throughput,
                consumer_instances=concurrency,
            )
            result["statistics"]["wait_to_start_ms"]["mean"] = wait
            rows.append(result)

    for index, row in enumerate(rows):
        (results_dir / f"{index}.json").write_text(json.dumps(row))

    report_path, _ = generate_report(results_dir, report_dir)
    markdown = report_path.read_text()
    throughput_chart = "![CPU-light throughput](charts/cpu_light_matched_concurrency-01.png)"
    wait_chart = "![CPU-light wait to start](charts/cpu_light_matched_concurrency-02.png)"

    assert throughput_chart in markdown
    assert wait_chart in markdown
    assert markdown.index(throughput_chart) < markdown.index(wait_chart)
    assert (report_dir / "charts" / "cpu_light_matched_concurrency-01.png").stat().st_size > 0
    assert (report_dir / "charts" / "cpu_light_matched_concurrency-02.png").stat().st_size > 0


def test_build_markdown_uses_delayed_table_and_workflow_omission_note():
    results = [
        _sample_result(
            suite_id="delayed_case_study",
            condition_id="delayed_case",
            system="kafka",
            throughput=10.0,
        ),
        _sample_result(
            suite_id="delayed_case_study",
            condition_id="delayed_case",
            system="sidekiq",
            throughput=10.0,
        ),
        _sample_result(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=1",
            system="kafka",
            throughput=100.0,
            resource_usage={"samples": 2, "by_role": {"kafka_broker": {"cpu_pct": {"mean": 1.0, "peak": 1.0}, "mem_mb": {"mean": 1.0, "peak": 1.0}}, "kafka_consumer": {"cpu_pct": {"mean": 2.0, "peak": 2.0}, "mem_mb": {"mean": 2.0, "peak": 2.0}}}},
        ),
        _sample_result(
            suite_id="cpu_light_matched_concurrency",
            condition_id="concurrency=1",
            system="sidekiq",
            throughput=90.0,
            resource_usage={"samples": 2, "by_role": {"redis_broker": {"cpu_pct": {"mean": 1.0, "peak": 1.0}, "mem_mb": {"mean": 1.0, "peak": 1.0}}, "sidekiq_worker": {"cpu_pct": {"mean": 2.0, "peak": 2.0}, "mem_mb": {"mean": 2.0, "peak": 2.0}}}},
        ),
    ]
    results[0]["statistics"]["start_lateness_ms"] = {"min": 10.0, "max": 30.0, "mean": 15.0, "median": 15.0, "p95": 25.0, "p99": 29.0, "stddev": 4.0}
    results[0]["statistics"]["scheduling"] = {"total_scheduled": 1000, "early_start_count": 0, "on_time_pct_100ms": 100.0, "on_time_pct_500ms": 100.0, "jitter_ms": 4.0}
    results[1]["statistics"]["start_lateness_ms"] = {"min": 20.0, "max": 40.0, "mean": 22.0, "median": 22.0, "p95": 35.0, "p99": 39.0, "stddev": 6.0}
    results[1]["statistics"]["scheduling"] = {"total_scheduled": 1000, "early_start_count": 0, "on_time_pct_100ms": 100.0, "on_time_pct_500ms": 100.0, "jitter_ms": 6.0}

    markdown = build_markdown(results, [], [], {})

    assert "### Delayed start lateness" in markdown
    assert "| System | Mean lateness ms | P95 ms | P99 ms | Jitter ms | On-time <= 500 ms % |" in markdown
    assert "| `kafka` | 15.00 | 25.00 | 29.00 | 4.00 | 100.00 |" in markdown
    assert "_Resource usage is omitted from interpretation for `cpu_light_matched_concurrency` because paired SUT samples were insufficient or incomplete._" in markdown


def test_generate_report_renders_appendix_resource_memory_chart_from_resource_summaries(tmp_path: Path):
    results_dir = tmp_path / "results"
    report_dir = tmp_path / "report"
    results_dir.mkdir()

    resource_usage = {
        "samples": 3,
        "by_role": {
            "kafka_broker": {"cpu_pct": {"mean": 10.0, "peak": 10.0}, "mem_mb": {"mean": 100.0, "peak": 100.0}},
            "kafka_consumer": {"cpu_pct": {"mean": 20.0, "peak": 20.0}, "mem_mb": {"mean": 30.0, "peak": 30.0}},
            "redis_broker": {"cpu_pct": {"mean": 5.0, "peak": 5.0}, "mem_mb": {"mean": 20.0, "peak": 20.0}},
            "sidekiq_worker": {"cpu_pct": {"mean": 7.0, "peak": 7.0}, "mem_mb": {"mean": 25.0, "peak": 25.0}},
        },
    }
    rows = [
        _sample_result(
            suite_id="io_bound_matched_concurrency",
            condition_id="concurrency=4",
            system="kafka",
            throughput=80.0,
            consumer_instances=4,
            stub_delay_ms=50,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="io_bound_matched_concurrency",
            condition_id="concurrency=4",
            system="sidekiq",
            throughput=100.0,
            consumer_instances=4,
            stub_delay_ms=50,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if not k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="delayed_case_study",
            condition_id="delayed_case",
            system="kafka",
            throughput=10.0,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if k.startswith("kafka")}},
        ),
        _sample_result(
            suite_id="delayed_case_study",
            condition_id="delayed_case",
            system="sidekiq",
            throughput=10.0,
            resource_usage={"samples": 3, "by_role": {k: v for k, v in resource_usage["by_role"].items() if not k.startswith("kafka")}},
        ),
    ]
    rows[2]["statistics"]["start_lateness_ms"] = {"min": 10.0, "max": 30.0, "mean": 15.0, "median": 15.0, "p95": 25.0, "p99": 29.0, "stddev": 4.0}
    rows[2]["statistics"]["scheduling"] = {"total_scheduled": 1000, "early_start_count": 0, "on_time_pct_100ms": 100.0, "on_time_pct_500ms": 100.0, "jitter_ms": 4.0}
    rows[3]["statistics"]["start_lateness_ms"] = {"min": 20.0, "max": 40.0, "mean": 22.0, "median": 22.0, "p95": 35.0, "p99": 39.0, "stddev": 6.0}
    rows[3]["statistics"]["scheduling"] = {"total_scheduled": 1000, "early_start_count": 0, "on_time_pct_100ms": 100.0, "on_time_pct_500ms": 100.0, "jitter_ms": 6.0}

    for index, row in enumerate(rows):
        (results_dir / f"{index}.json").write_text(json.dumps(row))

    report_path, _ = generate_report(results_dir, report_dir)
    markdown = report_path.read_text()

    assert "![Mean SUT memory by suite](charts/appendix_resource_memory-01.png)" in markdown
    assert "_Supporting context only._" in markdown
    assert (report_dir / "charts" / "appendix_resource_memory-01.png").stat().st_size > 0


def _sample_result(
    *,
    suite_id: str,
    condition_id: str,
    system: str,
    throughput: float,
    partitions: int | None = None,
    consumer_instances: int = 1,
    resource_usage: dict | None = None,
    stub_delay_ms: int | None = None,
) -> dict:
    return result_payload(
        run_id=f"{system}-{suite_id}-{condition_id}",
        suite_id=suite_id,
        condition_id=condition_id,
        system=system,
        messages=1000,
        warmup=100,
        throughput=throughput,
        total_messages=1000,
        successful=1000,
        trace_file=f"traces/{system}-{suite_id}-{condition_id}.jsonl.gz",
        resource_usage=resource_usage,
        nominal_concurrency_target=consumer_instances,
        partitions=partitions or 1,
        consumer_instances=consumer_instances,
        stub_delay_ms=stub_delay_ms,
    )
