from __future__ import annotations

import time
import uuid


def build_job(message_json: str, queue: str) -> dict:
    now = time.time()
    return {
        "class": "BenchmarkWorker",
        "args": [message_json],
        "queue": queue,
        "jid": uuid.uuid4().hex[:24],
        "created_at": now,
        "enqueued_at": now,
        "retry": False,
    }
