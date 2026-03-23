from __future__ import annotations

import json
import time

from redis import Redis


def redis_client(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)


class MetricsStore:
    def __init__(self, redis_url: str) -> None:
        self.redis = redis_client(redis_url)

    def flush(self) -> None:
        self.redis.flushdb()

    def clear_run(self, run_id: str) -> None:
        self.redis.delete(
            self._completed_key(run_id),
            self._errors_key(run_id),
            self._accounted_key(run_id),
        )

    def record_success(self, run_id: str, record: dict) -> None:
        payload = json.dumps(record, sort_keys=True)
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.rpush(self._completed_key(run_id), payload)
            pipe.incr(self._accounted_key(run_id))
            pipe.execute()

    def record_error(self, run_id: str, error: dict) -> None:
        payload = json.dumps(error, sort_keys=True)
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.rpush(self._errors_key(run_id), payload)
            pipe.incr(self._accounted_key(run_id))
            pipe.execute()

    def accounted(self, run_id: str) -> int:
        value = self.redis.get(self._accounted_key(run_id))
        return int(value or 0)

    def wait_for_accounted(self, run_id: str, expected: int, timeout_seconds: float) -> bool:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if self.accounted(run_id) >= expected:
                return True
            time.sleep(0.25)
        return self.accounted(run_id) >= expected

    def load_completed(self, run_id: str) -> list[dict]:
        return [json.loads(item) for item in self.redis.lrange(self._completed_key(run_id), 0, -1)]

    def load_errors(self, run_id: str) -> list[dict]:
        return [json.loads(item) for item in self.redis.lrange(self._errors_key(run_id), 0, -1)]

    @staticmethod
    def _completed_key(run_id: str) -> str:
        return f"bench:completed:{run_id}"

    @staticmethod
    def _errors_key(run_id: str) -> str:
        return f"bench:errors:{run_id}"

    @staticmethod
    def _accounted_key(run_id: str) -> str:
        return f"bench:accounted:{run_id}"
