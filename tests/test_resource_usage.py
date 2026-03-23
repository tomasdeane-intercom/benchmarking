import json
from datetime import UTC, datetime
from pathlib import Path

from benchkit.resource_usage import aggregate_resource_usage, enrich_results_from_csv
from tests.support import result_payload


def test_resource_usage_scopes_to_relevant_project_and_roles(tmp_path: Path):
    csv_path = tmp_path / "stats.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,container,cpu_pct,mem_usage_mb,mem_limit_mb,mem_pct,net_in_mb,net_out_mb",
                "2026-03-20T14:11:34Z,benchmarking-kafka-1,10,100,1000,10,0,0",
                "2026-03-20T14:11:34Z,benchmarking-kafka-consumer-1,20,30,1000,3,0,0",
                "2026-03-20T14:11:34Z,benchmarking-stub-server-1,30,40,1000,4,0,0",
                "2026-03-20T14:11:34Z,benchmarking-redis-1,50,60,1000,6,0,0",
                "2026-03-20T14:11:34Z,otherproject-kafka-1,99,999,1000,99,0,0",
            ]
        )
    )
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    result = result_payload(suite_id="suite", condition_id="condition")
    path = results_dir / "run-1.json"
    path.write_text(json.dumps(result))

    enrich_results_from_csv(csv_path, results_dir, compose_project="benchmarking")

    enriched = json.loads(path.read_text())
    assert enriched["resource_usage"]["samples"] == 1
    assert set(enriched["resource_usage"]["by_role"]) == {"kafka_broker", "kafka_consumer"}


def test_resource_usage_matches_precise_subsecond_run_windows():
    rows = [
        {
            "timestamp": "2026-03-20T14:11:34.090000Z",
            "container": "benchmarking-kafka-1",
            "cpu_pct": "5",
            "mem_usage_mb": "100",
            "mem_limit_mb": "1000",
            "mem_pct": "10",
            "net_in_mb": "0",
            "net_out_mb": "0",
        },
        {
            "timestamp": "2026-03-20T14:11:34.150000Z",
            "container": "benchmarking-kafka-1",
            "cpu_pct": "15",
            "mem_usage_mb": "120",
            "mem_limit_mb": "1000",
            "mem_pct": "12",
            "net_in_mb": "0",
            "net_out_mb": "0",
        },
        {
            "timestamp": "2026-03-20T14:11:34.150000Z",
            "container": "benchmarking-kafka-consumer-1",
            "cpu_pct": "20",
            "mem_usage_mb": "30",
            "mem_limit_mb": "1000",
            "mem_pct": "3",
            "net_in_mb": "0",
            "net_out_mb": "0",
        },
        {
            "timestamp": "2026-03-20T14:11:34.280000Z",
            "container": "benchmarking-kafka-1",
            "cpu_pct": "25",
            "mem_usage_mb": "140",
            "mem_limit_mb": "1000",
            "mem_pct": "14",
            "net_in_mb": "0",
            "net_out_mb": "0",
        },
    ]

    usage = aggregate_resource_usage(
        rows,
        datetime(2026, 3, 20, 14, 11, 34, 120000, tzinfo=UTC),
        datetime(2026, 3, 20, 14, 11, 34, 220000, tzinfo=UTC),
        system="kafka",
        workload="cpu_light",
        compose_project="benchmarking",
    )

    assert usage == {
        "samples": 1,
        "by_role": {
            "kafka_broker": {
                "cpu_pct": {"mean": 15.0, "peak": 15.0},
                "mem_mb": {"mean": 120.0, "peak": 120.0},
            },
            "kafka_consumer": {
                "cpu_pct": {"mean": 20.0, "peak": 20.0},
                "mem_mb": {"mean": 30.0, "peak": 30.0},
            },
        },
    }


def test_short_shock_burst_run_gets_nearest_sample_fallback(tmp_path: Path):
    csv_path = tmp_path / "stats.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,container,cpu_pct,mem_usage_mb,mem_limit_mb,mem_pct,net_in_mb,net_out_mb",
                "2026-03-20T14:11:34.000000Z,benchmarking-kafka-1,10,100,1000,10,0,0",
                "2026-03-20T14:11:34.000000Z,benchmarking-kafka-consumer-1,20,30,1000,3,0,0",
                "2026-03-20T14:11:34.250000Z,benchmarking-kafka-1,30,110,1000,11,0,0",
                "2026-03-20T14:11:34.250000Z,benchmarking-kafka-consumer-1,40,35,1000,3.5,0,0",
            ]
        )
    )
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    result = result_payload(
        run_id="run-shock-burst-1",
        suite_id="suite",
        condition_id="shock-burst",
        variable_under_test="produce_pattern",
        messages=25,
        warmup=0,
        throughput=500.0,
        total_messages=25,
        successful=25,
        trace_file="traces/run-shock-burst-1.jsonl.gz",
    )
    result["timestamp"] = "2026-03-20T14:11:34.300000"
    result["run_start_utc"] = "2026-03-20T14:11:34.080000Z"
    result["run_end_utc"] = "2026-03-20T14:11:34.120000Z"
    result["config"]["produce_pattern"] = "shock_burst"
    result["config"]["burst_size"] = 25
    result["config"]["inter_burst_ms"] = 0
    result["statistics"]["latency_ms"] = {"min": 1.0, "max": 2.0, "mean": 1.5, "median": 1.5, "p95": 2.0, "p99": 2.0, "stddev": 0.2}
    result["statistics"]["wait_to_start_ms"] = {"min": 0.1, "max": 0.2, "mean": 0.15, "median": 0.15, "p95": 0.2, "p99": 0.2, "stddev": 0.03}
    result["statistics"]["processing_ms"] = {"min": 0.8, "max": 1.8, "mean": 1.2, "median": 1.2, "p95": 1.8, "p99": 1.8}
    result["statistics"]["throughput"]["produce_msg_per_sec"] = 1000.0
    result["statistics"]["timing_s"] = {"total_wall": 0.04, "produce": 0.02, "drain": 0.02}
    path = results_dir / "run-shock-burst-1.json"
    path.write_text(json.dumps(result))

    enrich_results_from_csv(csv_path, results_dir, compose_project="benchmarking")

    enriched = json.loads(path.read_text())
    assert enriched["resource_usage"]["samples"] == 2
    assert enriched["resource_usage"]["by_role"]["kafka_broker"]["cpu_pct"]["mean"] == 20.0
    assert enriched["resource_usage"]["by_role"]["kafka_consumer"]["cpu_pct"]["peak"] == 40.0
