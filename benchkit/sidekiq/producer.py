from __future__ import annotations

import json
import time

from redis import Redis

from benchkit.sidekiq.job_contract import build_job
from benchkit.workloads import iter_produce_plan, message_json


class SidekiqProducerAdapter:
    def __init__(self, redis_url: str, queue_name: str) -> None:
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.queue_name = queue_name

    def cleanup(self) -> None:
        queue_key = f"queue:{self.queue_name}"
        self.redis.delete(queue_key, "schedule", "retry", "dead")
        self.redis.srem("queues", self.queue_name)

    def produce_run(
        self,
        run_id: str,
        workload: str,
        messages: int,
        produce_shape: str,
        burst_size: int | None,
        inter_burst_ms: float | None,
        delay_seconds: float | None,
    ) -> tuple[float, float]:
        produce_start = time.time()
        self.redis.sadd("queues", self.queue_name)
        queue_key = f"queue:{self.queue_name}"
        for message, sleep_after in iter_produce_plan(
            run_id=run_id,
            workload=workload,
            messages=messages,
            produce_shape=produce_shape,
            burst_size=burst_size,
            inter_burst_ms=inter_burst_ms,
            delay_seconds=delay_seconds,
        ):
            message["system"] = "sidekiq"
            payload = build_job(message_json(message), self.queue_name)
            encoded = json.dumps(payload, sort_keys=True)
            if message.get("target_time") is not None:
                self.redis.zadd("schedule", {encoded: float(message["target_time"])})
            else:
                self.redis.lpush(queue_key, encoded)
            if sleep_after > 0:
                time.sleep(sleep_after)
        produce_end = time.time()
        return produce_start, produce_end
