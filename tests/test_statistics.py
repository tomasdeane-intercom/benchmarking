from benchkit.statistics import _pct_within, compute_run_statistics, percentile


def test_percentile_uses_canonical_interpolation():
    values = [1.0, 2.0, 3.0, 4.0]
    assert percentile(values, 95) == 3.8499999999999996
    assert percentile(values, 50) == 2.5


def test_delayed_statistics_use_work_start_time_and_sample_stddev():
    events = [
        {
            "id": "a",
            "enqueue_time": 0.0,
            "dequeue_time": 1.0,
            "work_start_time": 2.0,
            "complete_time": 3.0,
            "latency_ms": 3000.0,
            "success": True,
            "target_time": 1.9,
        },
        {
            "id": "b",
            "enqueue_time": 0.0,
            "dequeue_time": 1.0,
            "work_start_time": 2.2,
            "complete_time": 3.3,
            "latency_ms": 3300.0,
            "success": True,
            "target_time": 2.0,
        },
    ]
    stats = compute_run_statistics(
        events=events,
        error_records=[],
        expected_messages=2,
        produce_start=0.0,
        produce_end=0.5,
        workload="delayed",
    )
    assert stats["processing_ms"]["mean"] == 1050.0
    assert stats["start_lateness_ms"]["median"] == 150.0
    assert stats["scheduling"]["total_scheduled"] == 2
    assert stats["scheduling"]["early_start_count"] == 0


def test_timing_seconds_preserve_subsecond_precision():
    events = [
        {
            "id": "a",
            "enqueue_time": 0.0,
            "dequeue_time": 0.01,
            "complete_time": 0.1234,
            "latency_ms": 123.4,
            "success": True,
        },
    ]
    stats = compute_run_statistics(
        events=events,
        error_records=[],
        expected_messages=1,
        produce_start=0.0,
        produce_end=0.02,
        workload="cpu_light",
    )

    assert stats["timing_s"]["total_wall"] == 0.1234
    assert stats["timing_s"]["produce"] == 0.02
    assert stats["timing_s"]["drain"] == 0.1034


def test_pct_within_uses_absolute_lateness():
    assert _pct_within([80.0], 100.0) == 100.0
    assert _pct_within([-80.0], 100.0) == 100.0
    assert _pct_within([120.0], 100.0) == 0.0
    assert _pct_within([-120.0], 100.0) == 0.0


def test_pct_within_counts_exact_threshold_as_on_time():
    values = [100.0, -100.0, 100.01, -100.01]

    assert _pct_within(values, 100.0) == 50.0
