#!/usr/bin/env python3
"""Kafka consumer that processes benchmark workloads and records metrics to Redis.

When WORKER_THREADS > 1, messages are dispatched to a ThreadPoolExecutor with
a semaphore-based back-pressure mechanism so consumption doesn't outpace
processing.  Thread-local requests.Sessions reuse TCP connections.
"""

import hashlib
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import redis
import requests
from confluent_kafka import Consumer, KafkaError

# Configuration from environment
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/15")
STUB_URL = os.environ.get("STUB_SERVER_URL", "http://stub-server:8080")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "bench-group")
TOPIC = os.environ.get("TOPIC", "benchmark")
POLL_TIMEOUT = float(os.environ.get("POLL_TIMEOUT", "1.0"))

# Optimization knobs
WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "1"))
MAX_PENDING = int(os.environ.get("MAX_PENDING", str(WORKER_THREADS * 2)))
FETCH_MIN_BYTES = int(os.environ.get("FETCH_MIN_BYTES", "1"))
FETCH_MAX_WAIT_MS = int(os.environ.get("FETCH_MAX_WAIT_MS", "500"))
AUTO_COMMIT_INTERVAL_MS = int(os.environ.get("AUTO_COMMIT_INTERVAL_MS", "1000"))

# Globals
running = True
metrics_redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Thread-local storage for HTTP sessions (one persistent connection per thread)
_tls = threading.local()

def _get_session():
    """Return a per-thread requests.Session for HTTP connection reuse."""
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
    return _tls.session

# Single session used in sequential (WORKER_THREADS=1) mode
http_session = requests.Session()

def _shutdown(sig, frame):
    global running
    print(f"Received signal {sig}, shutting down...", flush=True)
    running = False

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)

# Workloads
def workload_cpu_light(msg, session=None):
    data = f"benchmark-{msg['id']}"
    iterations = msg.get("payload", {}).get("iterations", 1000)
    for _ in range(iterations):
        data = hashlib.sha256(data.encode()).hexdigest()
    return True

def workload_io_bound(msg, session=None):
    delay_ms = msg.get("payload", {}).get("delay_ms", 50)
    s = session or http_session
    resp = s.get(f"{STUB_URL}/delay/{delay_ms}", timeout=15)
    return resp.status_code == 200

def workload_fanout(msg, session=None):
    data = f"fanout-{msg['id']}"
    for _ in range(100):
        data = hashlib.sha256(data.encode()).hexdigest()
    return True

def workload_delayed(msg, session=None):
    target_time = msg.get("target_time")
    if target_time:
        now = time.time()
        if now < target_time:
            time.sleep(target_time - now)
    data = f"delayed-{msg['id']}"
    for _ in range(100):
        data = hashlib.sha256(data.encode()).hexdigest()
    return True

WORKLOADS = {
    "cpu_light": workload_cpu_light,
    "io_bound": workload_io_bound,
    "fanout": workload_fanout,
    "delayed": workload_delayed,
}

# Message processing (used by both sequential and threaded paths)
def process_message(raw_value, session=None):
    """Deserialise, execute workload, record metrics.  Returns on success."""
    data = json.loads(raw_value)
    dequeue_time = time.time()

    handler = WORKLOADS.get(data.get("workload", "cpu_light"), workload_cpu_light)
    success = handler(data, session=session)

    complete_time = time.time()
    run_id = data.get("run_id", "default")

    metric = json.dumps({
        "id": data["id"],
        "system": "kafka",
        "workload": data.get("workload", "unknown"),
        "run_id": run_id,
        "enqueue_time": data["enqueue_time"],
        "dequeue_time": dequeue_time,
        "complete_time": complete_time,
        "latency_ms": round((complete_time - data["enqueue_time"]) * 1000, 2),
        "processing_ms": round((complete_time - dequeue_time) * 1000, 2),
        "success": success,
    })

    pipe = metrics_redis.pipeline()
    pipe.lpush(f"bench:completed:{run_id}", metric)
    pipe.incr(f"bench:count:{run_id}")
    pipe.execute()

def _threaded_handler(raw_value, semaphore, counter_lock, counter_ref):
    """Target for ThreadPoolExecutor tasks.  Releases semaphore when done."""
    try:
        session = _get_session()
        process_message(raw_value, session=session)
        with counter_lock:
            counter_ref[0] += 1
            if counter_ref[0] % 1000 == 0:
                print(f"Processed {counter_ref[0]} messages", flush=True)
    except Exception as exc:
        print(f"Error processing message: {exc}", flush=True)
        try:
            run_id = json.loads(raw_value).get("run_id", "default")
        except Exception:
            run_id = "default"
        metrics_redis.lpush(
            f"bench:errors:{run_id}",
            json.dumps({"error": str(exc), "time": time.time()}),
        )
    finally:
        semaphore.release()

# Main consumer loop
def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": AUTO_COMMIT_INTERVAL_MS,
        "fetch.min.bytes": FETCH_MIN_BYTES,
        "fetch.wait.max.ms": FETCH_MAX_WAIT_MS,
    })
    consumer.subscribe([TOPIC])
    print(
        f"Kafka consumer started: group={GROUP_ID} topic={TOPIC} "
        f"threads={WORKER_THREADS} max_pending={MAX_PENDING}",
        flush=True,
    )

    if WORKER_THREADS > 1:
        _run_threaded(consumer)
    else:
        _run_sequential(consumer)

def _run_sequential(consumer):
    """Original single-threaded path — preserves baseline behaviour."""
    processed = 0
    while running:
        raw = consumer.poll(POLL_TIMEOUT)
        if raw is None:
            continue
        if raw.error():
            if raw.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {raw.error()}", flush=True)
            continue

        try:
            process_message(raw.value().decode())
            processed += 1
            if processed % 1000 == 0:
                print(f"Processed {processed} messages", flush=True)
        except Exception as exc:
            print(f"Error processing message: {exc}", flush=True)
            run_id = "default"
            try:
                run_id = json.loads(raw.value().decode()).get("run_id", "default")
            except Exception:
                pass
            metrics_redis.lpush(
                f"bench:errors:{run_id}",
                json.dumps({"error": str(exc), "time": time.time()}),
            )

    consumer.close()
    print(f"Consumer stopped after processing {processed} messages", flush=True)

def _run_threaded(consumer):
    """Threaded path — dispatches to a pool with semaphore back-pressure."""
    semaphore = threading.Semaphore(MAX_PENDING)
    counter_lock = threading.Lock()
    counter = [0]  # mutable container for closure

    pool = ThreadPoolExecutor(max_workers=WORKER_THREADS)

    try:
        while running:
            raw = consumer.poll(POLL_TIMEOUT)
            if raw is None:
                continue
            if raw.error():
                if raw.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Consumer error: {raw.error()}", flush=True)
                continue

            # Back-pressure: block if too many messages are in-flight
            semaphore.acquire()
            pool.submit(_threaded_handler, raw.value().decode(), semaphore, counter_lock, counter)
    finally:
        pool.shutdown(wait=True)
        consumer.close()
        print(f"Consumer stopped after processing {counter[0]} messages", flush=True)

if __name__ == "__main__":
    main()
