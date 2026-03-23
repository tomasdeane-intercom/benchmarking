from __future__ import annotations

import json
import logging
import os
import threading
import time

from confluent_kafka import Consumer

from benchkit.config import RuntimeConfig
from benchkit.kafka.worker_common import handle_message
from benchkit.logging import configure_logging
from benchkit.metrics import MetricsStore


LOGGER = logging.getLogger(__name__)


def main() -> int:
    configure_logging()
    runtime = RuntimeConfig()
    metrics = MetricsStore(runtime.metrics_redis_url)
    # `KAFKA_THREADS` is a legacy env var name. In this worker it configures
    # additional consumer instances within the container, not processor slots.
    consumer_instances = int(os.getenv("KAFKA_THREADS", "1"))
    workers = []
    for index in range(consumer_instances):
        thread = threading.Thread(
            target=_consume_loop,
            args=(runtime, metrics, index),
            daemon=False,
        )
        thread.start()
        workers.append(thread)
    for thread in workers:
        thread.join()
    return 0


def _consume_loop(runtime: RuntimeConfig, metrics: MetricsStore, index: int) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": runtime.kafka_bootstrap_servers,
            "group.id": runtime.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.min.bytes": int(os.getenv("FETCH_MIN_BYTES", "1")),
            "fetch.wait.max.ms": int(os.getenv("FETCH_MAX_WAIT_MS", "500")),
        }
    )
    consumer.subscribe([runtime.topic])
    LOGGER.info("Kafka consumer instance %s subscribed to %s", index, runtime.topic)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                continue
            handle_message(message.value().decode("utf-8"), metrics, runtime.stub_server_url, "kafka")
            consumer.commit(message)
    finally:
        consumer.close()

if __name__ == "__main__":
    raise SystemExit(main())
