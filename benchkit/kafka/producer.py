from __future__ import annotations

import time

from confluent_kafka import Producer

from benchkit.workloads import iter_produce_plan, message_json


class KafkaProducerAdapter:
    def __init__(self, bootstrap_servers: str, acks: str, compression: str) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": acks,
                "compression.type": compression,
            }
        )

    def produce_run(
        self,
        topic: str,
        run_id: str,
        workload: str,
        messages: int,
        produce_shape: str,
        burst_size: int | None,
        inter_burst_ms: float | None,
        delay_seconds: float | None,
    ) -> tuple[float, float]:
        produce_start = time.time()
        for message, sleep_after in iter_produce_plan(
            run_id=run_id,
            workload=workload,
            messages=messages,
            produce_shape=produce_shape,
            burst_size=burst_size,
            inter_burst_ms=inter_burst_ms,
            delay_seconds=delay_seconds,
        ):
            message["system"] = "kafka"
            self.producer.produce(topic, key=message["id"], value=message_json(message))
            self.producer.poll(0)
            if sleep_after > 0:
                time.sleep(sleep_after)
        self.producer.flush()
        produce_end = time.time()
        return produce_start, produce_end
