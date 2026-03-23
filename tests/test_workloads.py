from benchkit.workloads import iter_produce_plan, spec_for


def test_workload_specs_are_canonical():
    assert spec_for("cpu_light").iterations == 1000
    assert spec_for("bursty_ingest").iterations == 100
    assert spec_for("delayed").iterations == 100
    assert spec_for("io_bound").delay_ms == 50


def test_delayed_targets_are_linearly_spread_and_bursts_sleep_between_batches():
    plan = list(
        iter_produce_plan(
            run_id="run-1",
            workload="delayed",
            messages=3,
            produce_shape="sequential_scheduled",
            delay_seconds=30,
        )
    )
    targets = [message["target_time"] for message, _ in plan]
    assert round(targets[1] - targets[0], 1) == 15.0
    assert round(targets[2] - targets[1], 1) == 15.0

    burst_plan = list(
        iter_produce_plan(
            run_id="run-1",
            workload="bursty_ingest",
            messages=5,
            produce_shape="burst",
            burst_size=2,
            inter_burst_ms=50,
        )
    )
    assert [round(delay, 2) for _, delay in burst_plan] == [0.0, 0.05, 0.0, 0.05, 0.0]
