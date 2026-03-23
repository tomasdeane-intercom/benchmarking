from benchkit.manifest import expand_suite, list_suites


def test_suite_list_matches_canonical_names():
    suite_ids = [suite["suite_id"] for suite in list_suites()]
    assert suite_ids == [
        "kafka_partition_adequacy",
        "parallelism_decomposition",
        "cpu_light_matched_concurrency",
        "io_bound_matched_concurrency",
        "bursty_ingest_shape",
        "delayed_case_study",
    ]


def test_cpu_light_matched_concurrency_expansion():
    conditions = expand_suite("cpu_light_matched_concurrency")
    assert len(conditions) == 8
    assert {condition["system"] for condition in conditions} == {"kafka", "sidekiq"}
    assert {condition["concurrency"] for condition in conditions} == {1, 4, 8, 16}
    assert {condition["partitions"] for condition in conditions} == {16}
    kafka_16 = next(
        condition
        for condition in conditions
        if condition["system"] == "kafka" and condition["condition_id"] == "concurrency=16"
    )
    assert kafka_16["messages"] == 10000
    assert kafka_16["kafka_consumers"] == 1
    assert kafka_16["worker_threads"] == 16


def test_bursty_shapes_and_delayed_defaults_are_canonical():
    bursty = expand_suite("bursty_ingest_shape")
    assert {condition["partitions"] for condition in bursty} == {16}
    shape_map = {
        condition["condition_id"]: (condition["burst_size"], condition["inter_burst_ms"])
        for condition in bursty
        if condition["system"] == "kafka"
    }
    assert shape_map == {
        "produce_shape=steady": (1, 0.5),
        "produce_shape=microburst": (10, 5.0),
        "produce_shape=burst": (100, 50.0),
        "produce_shape=shock_burst": (5000, 0.0),
    }

    delayed = expand_suite("delayed_case_study")
    assert {condition["partitions"] for condition in delayed} == {16}
    assert all(condition["delay_seconds"] == 30.0 for condition in delayed)
    assert all(condition["warmup"] == 200 for condition in delayed)
    assert all(condition["produce_shape"] == "sequential_scheduled" for condition in delayed)
