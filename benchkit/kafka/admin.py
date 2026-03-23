from __future__ import annotations

import time

from confluent_kafka.admin import AdminClient, NewTopic


class KafkaAdmin:
    def __init__(self, bootstrap_servers: str) -> None:
        self.client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def cluster_ready(self, timeout: float = 5.0) -> bool:
        try:
            metadata = self.client.list_topics(timeout=timeout)
        except Exception:
            return False
        return bool(metadata.brokers)

    def reset_topic(self, topic: str, partitions: int) -> int:
        self.wait_until_cluster_ready()
        self.delete_topic(topic)
        self.wait_until_topic_absent(topic)
        self.create_topic(topic, partitions)
        return self.observed_partitions(topic)

    def delete_topic(self, topic: str) -> None:
        futures = self.client.delete_topics([topic], operation_timeout=10)
        for future in futures.values():
            try:
                future.result()
            except Exception:
                pass

    def create_topic(self, topic: str, partitions: int) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            futures = self.client.create_topics(
                [NewTopic(topic, num_partitions=partitions, replication_factor=1)],
                operation_timeout=10,
            )
            create_error = None
            for future in futures.values():
                try:
                    future.result()
                except Exception as exc:
                    create_error = exc
            observed = self.observed_partitions(topic)
            if observed == partitions:
                return
            if create_error is None and observed > 0 and observed != partitions:
                raise RuntimeError(
                    f"Kafka topic {topic} exists with {observed} partitions, expected {partitions}"
                )
            time.sleep(1.0)
        raise RuntimeError(f"Kafka topic {topic} did not reach {partitions} partitions")

    def delete_consumer_group(self, group_id: str) -> None:
        try:
            futures = self.client.delete_consumer_groups([group_id], request_timeout=10)
            for future in futures.values():
                future.result()
        except Exception:
            pass

    def wait_until_cluster_ready(self) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.cluster_ready(timeout=5):
                return
            time.sleep(1.0)
        raise RuntimeError("Kafka cluster did not become ready")

    def wait_until_topic_absent(self, topic: str) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            observed = self.observed_partitions(topic)
            if observed == 0:
                return
            time.sleep(1.0)
        raise RuntimeError(f"Kafka topic {topic} was not deleted before recreate")

    def observed_partitions(self, topic: str) -> int:
        metadata = self.client.list_topics(topic=topic, timeout=10)
        topic_md = metadata.topics.get(topic)
        if topic_md is None or topic_md.error is not None:
            return 0
        return len(topic_md.partitions)
