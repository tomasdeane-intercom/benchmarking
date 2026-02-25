#!/usr/bin/env python3
"""Benchmark orchestrator for Kafka vs Sidekiq comparison.

Produces messages to either Kafka or Redis/Sidekiq, waits for workers to
process them, and collects latency / throughput metrics.

Usage:
    python benchmark.py --system kafka   --workload cpu_light --messages 5000
    python benchmark.py --system sidekiq --workload io_bound  --messages 2000
"""

import argparse
import json
import os
import statistics
import time
import uuid
from datetime import datetime

import redis
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Environment-driven defaults (overridden inside Docker network)
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
METRICS_REDIS_URL = os.environ.get("METRICS_REDIS_URL", "redis://redis:6379/15")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/app/results")

# Optimization knobs (set via env in optimized mode)
BENCH_MODE = os.environ.get("BENCH_MODE", "baseline")
SIDEKIQ_BATCH_SIZE = int(os.environ.get("SIDEKIQ_BATCH_SIZE", "1"))
KAFKA_COMPRESSION = os.environ.get("KAFKA_COMPRESSION", "none")
KAFKA_ACKS = os.environ.get("KAFKA_ACKS", "all")
WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "1"))

# Workload payloads
PAYLOADS = {
    "cpu_light": {"iterations": 1000},
    "io_bound":  {"delay_ms": 50},
    "fanout":    {"iterations": 100},
    "delayed":   {"iterations": 100},
}

# Producer helpers
class BenchmarkProducer:
    """Unified producer that can push work to Kafka or Sidekiq."""

    def __init__(self, system: str):
        self.system = system
        self.metrics = redis.Redis.from_url(METRICS_REDIS_URL, decode_responses=True)

        if system == "kafka":
            kafka_conf = {
                "bootstrap.servers": KAFKA_SERVERS,
                "queue.buffering.max.messages": 500000,
                "queue.buffering.max.ms": 5,
                "batch.num.messages": 10000,
                "linger.ms": 5,
                "acks": KAFKA_ACKS,
            }
            if KAFKA_COMPRESSION != "none":
                kafka_conf["compression.type"] = KAFKA_COMPRESSION
            self._kp = Producer(kafka_conf)
            self._admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
        else:
            self._redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            self._sidekiq_buffer = []  # buffer for batched enqueuing

    # Kafka
    def ensure_topic(self, topic: str, partitions: int):
        existing = self._admin.list_topics(timeout=10).topics
        if topic in existing:
            return
        fut = self._admin.create_topics(
            [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
        )
        for t, f in fut.items():
            try:
                f.result()
                print(f"  Created topic '{t}' with {partitions} partitions")
            except Exception as exc:
                print(f"  Topic '{t}': {exc}")

    def _produce_kafka(self, topic: str, msg: dict):
        self._kp.produce(topic, value=json.dumps(msg).encode())

    # Sidekiq

    def _produce_sidekiq(self, msg: dict, queue: str = "benchmark"):
        job_json = self._build_sidekiq_job(msg, queue)
        if SIDEKIQ_BATCH_SIZE <= 1:
            # Original: one pipeline per job
            pipe = self._redis.pipeline()
            pipe.lpush(f"queue:{queue}", job_json)
            pipe.sadd("queues", queue)
            pipe.execute()
        else:
            # Batched: buffer and flush when full
            self._sidekiq_buffer.append((queue, job_json))
            if len(self._sidekiq_buffer) >= SIDEKIQ_BATCH_SIZE:
                self._flush_sidekiq_batch()

    def _flush_sidekiq_batch(self):
        """Flush buffered Sidekiq jobs using multi-value LPUSH."""
        if not self._sidekiq_buffer:
            return
        # Group by queue (in practice all go to "benchmark")
        by_queue = {}
        for queue, job_json in self._sidekiq_buffer:
            by_queue.setdefault(queue, []).append(job_json)

        pipe = self._redis.pipeline()
        queues_seen = set()
        for queue, jobs in by_queue.items():
            pipe.lpush(f"queue:{queue}", *jobs)
            queues_seen.add(queue)
        for q in queues_seen:
            pipe.sadd("queues", q)
        pipe.execute()
        self._sidekiq_buffer.clear()

    def _build_sidekiq_job(self, msg: dict, queue: str) -> str:
        jid = uuid.uuid4().hex[:24]
        job = {
            "class": "BenchmarkWorker",
            "args": [json.dumps(msg)],
            "queue": queue,
            "jid": jid,
            "created_at": time.time(),
            "enqueued_at": time.time(),
            "retry": False,
        }
        return json.dumps(job)

    def _produce_sidekiq_scheduled(self, msg: dict, target_time: float, queue: str = "benchmark"):
        jid = uuid.uuid4().hex[:24]
        job = {
            "class": "BenchmarkWorker",
            "args": [json.dumps(msg)],
            "queue": queue,
            "jid": jid,
            "created_at": time.time(),
            "enqueued_at": time.time(),
            "retry": False,
        }
        self._redis.zadd("schedule", {json.dumps(job): target_time})

    # Unified

    def produce(self, msg: dict, topic: str = "benchmark", queue: str = "benchmark"):
        if self.system == "kafka":
            self._produce_kafka(topic, msg)
        else:
            if msg.get("workload") == "delayed" and msg.get("target_time"):
                self._produce_sidekiq_scheduled(msg, msg["target_time"], queue)
            else:
                self._produce_sidekiq(msg, queue)

    def flush(self):
        if self.system == "kafka":
            remaining = self._kp.flush(timeout=60)
            if remaining:
                print(f"  WARNING: {remaining} messages not delivered after flush")
        else:
            # Flush any remaining buffered Sidekiq jobs
            self._flush_sidekiq_batch()

# Metrics collection
def clear_metrics(mr: redis.Redis, run_id: str):
    for key in mr.scan_iter(f"bench:*:{run_id}"):
        mr.delete(key)

def wait_and_collect(mr: redis.Redis, run_id: str, expected: int,
                     timeout: float = 300, poll_interval: float = 0.5):
    """Block until *expected* messages are recorded or timeout elapses."""
    t0 = time.time()
    last_print = 0
    while True:
        done = int(mr.get(f"bench:count:{run_id}") or 0)
        if done >= expected:
            break
        elapsed = time.time() - t0
        if elapsed > timeout:
            print(f"  TIMEOUT after {elapsed:.0f}s: {done}/{expected} processed")
            break
        if done - last_print >= 1000 or (elapsed > 5 and done != last_print):
            print(f"  ... {done}/{expected} processed ({elapsed:.1f}s)", flush=True)
            last_print = done
        time.sleep(poll_interval)

    # Drain completed events
    events = []
    while True:
        raw = mr.rpop(f"bench:completed:{run_id}")
        if raw is None:
            break
        events.append(json.loads(raw))

    errors = []
    while True:
        raw = mr.rpop(f"bench:errors:{run_id}")
        if raw is None:
            break
        errors.append(json.loads(raw))

    return events, errors

# Statistics
def percentile(sorted_data, pct):
    """Return the value at the given percentile from sorted data."""
    if not sorted_data:
        return 0
    k = (len(sorted_data) - 1) * (pct / 100)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def compute_stats(events, errors, produce_start, produce_end):
    if not events:
        return {"error": "No events collected", "failed": len(errors)}

    latencies = sorted(e["latency_ms"] for e in events)
    processing = sorted(e["processing_ms"] for e in events)

    first_enqueue = min(e["enqueue_time"] for e in events)
    last_complete = max(e["complete_time"] for e in events)
    total_wall = last_complete - first_enqueue
    drain_time = last_complete - produce_end

    ok = sum(1 for e in events if e.get("success", True))

    return {
        "total_messages": len(events) + len(errors),
        "successful": ok,
        "failed": len(errors),
        "error_rate_pct": round(100 * len(errors) / max(len(events) + len(errors), 1), 2),
        "latency_ms": {
            "min":    round(latencies[0], 2),
            "max":    round(latencies[-1], 2),
            "mean":   round(statistics.mean(latencies), 2),
            "median": round(percentile(latencies, 50), 2),
            "p95":    round(percentile(latencies, 95), 2),
            "p99":    round(percentile(latencies, 99), 2),
            "stddev": round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0,
        },
        "processing_ms": {
            "min":    round(processing[0], 2),
            "mean":   round(statistics.mean(processing), 2),
            "median": round(percentile(processing, 50), 2),
            "p95":    round(percentile(processing, 95), 2),
            "p99":    round(percentile(processing, 99), 2),
        },
        "throughput": {
            "consumed_msg_per_sec": round(len(events) / total_wall, 2) if total_wall > 0 else 0,
            "produce_msg_per_sec":  round(len(events) / (produce_end - produce_start), 2)
                                    if produce_end > produce_start else 0,
        },
        "timing_s": {
            "total_wall": round(total_wall, 3),
            "produce":    round(produce_end - produce_start, 3),
            "drain":      round(drain_time, 3),
        },
    }

# Single benchmark run
def run_once(args, run_tag: str = ""):
    run_id = (
        f"{args.system}-{args.workload}"
        f"-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        f"-{uuid.uuid4().hex[:8]}"
    )

    print(f"\n{'=' * 64}")
    print(f"  System:   {args.system}")
    print(f"  Workload: {args.workload}")
    print(f"  Messages: {args.messages}  (warmup {args.warmup})")
    if args.system == "kafka":
        print(f"  Topic:    {args.topic}  Partitions: {args.partitions}")
    else:
        print(f"  Concurrency: {args.concurrency}")
    print(f"  Mode:     {BENCH_MODE}")
    print(f"  Run ID:   {run_id}  {run_tag}")
    print(f"{'=' * 64}")

    producer = BenchmarkProducer(args.system)
    mr = redis.Redis.from_url(METRICS_REDIS_URL, decode_responses=True)

    if args.system == "kafka":
        producer.ensure_topic(args.topic, args.partitions)
        time.sleep(2)  # let topic propagate

    # warmup 
    if args.warmup > 0:
        wid = f"warmup-{run_id}"
        clear_metrics(mr, wid)
        print(f"\n  Warmup: producing {args.warmup} messages ...")
        for _ in range(args.warmup):
            msg = _make_msg(args.workload, wid)
            producer.produce(msg, topic=args.topic)
        producer.flush()
        wait_and_collect(mr, wid, args.warmup, timeout=120)
        clear_metrics(mr, wid)
        print("  Warmup complete.\n")

    # benchmark 
    clear_metrics(mr, run_id)
    print(f"  Producing {args.messages} messages ...")

    produce_start = time.time()

    if args.workload == "fanout":
        bursts = args.messages // args.fanout_factor
        for b in range(bursts):
            for _ in range(args.fanout_factor):
                producer.produce(_make_msg("fanout", run_id, burst_id=b), topic=args.topic)
            if b % 50 == 0:
                producer.flush() if args.system == "kafka" else None
                time.sleep(0.005)
    elif args.workload == "delayed":
        for i in range(args.messages):
            target = time.time() + args.delay_seconds * (i / max(args.messages - 1, 1))
            msg = _make_msg("delayed", run_id)
            msg["target_time"] = target
            producer.produce(msg, topic=args.topic)
    else:
        for _ in range(args.messages):
            producer.produce(_make_msg(args.workload, run_id), topic=args.topic)

    producer.flush()
    produce_end = time.time()
    print(f"  Produced in {produce_end - produce_start:.2f}s "
          f"({args.messages / max(produce_end - produce_start, 0.001):.0f} msg/s)")

    # collection
    timeout = 300
    if args.workload == "io_bound":
        timeout = max(300, args.messages * 0.1 + 60)
    elif args.workload == "delayed":
        timeout = args.delay_seconds + 120

    events, errors = wait_and_collect(mr, run_id, args.messages, timeout=timeout)
    stats = compute_stats(events, errors, produce_start, produce_end)

    # summary
    print(f"\n  {'─' * 56}")
    lat = stats.get("latency_ms", {})
    tp = stats.get("throughput", {})
    tm = stats.get("timing_s", {})
    print(f"  Messages : {stats.get('successful', 0)} ok / "
          f"{stats.get('failed', 0)} err  "
          f"({stats.get('error_rate_pct', 0)}% error)")
    print(f"  Latency  : p50={lat.get('median')}  p95={lat.get('p95')}  "
          f"p99={lat.get('p99')}  max={lat.get('max')} ms")
    print(f"  Throughput: {tp.get('consumed_msg_per_sec')} msg/s consumed, "
          f"{tp.get('produce_msg_per_sec')} msg/s produced")
    print(f"  Timing   : total={tm.get('total_wall')}s  "
          f"produce={tm.get('produce')}s  drain={tm.get('drain')}s")
    print(f"  {'─' * 56}\n")

    # persist
    result = {
        "run_id": run_id,
        "system": args.system,
        "workload": args.workload,
        "timestamp": datetime.now().isoformat(),
        "bench_mode": BENCH_MODE,
        "config": {
            "messages": args.messages,
            "warmup": args.warmup,
            "partitions": args.partitions if args.system == "kafka" else None,
            "concurrency": args.concurrency,
            "fanout_factor": args.fanout_factor if args.workload == "fanout" else None,
            "delay_seconds": args.delay_seconds if args.workload == "delayed" else None,
            "worker_threads": WORKER_THREADS,
            "sidekiq_batch_size": SIDEKIQ_BATCH_SIZE,
            "kafka_compression": KAFKA_COMPRESSION if args.system == "kafka" else None,
            "kafka_acks": KAFKA_ACKS if args.system == "kafka" else None,
        },
        "statistics": stats,
    }
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, f"{run_id}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  Saved → {path}")
    return result

# Helpers
def _make_msg(workload: str, run_id: str, **extra) -> dict:
    msg = {
        "id": uuid.uuid4().hex,
        "workload": workload,
        "enqueue_time": time.time(),
        "run_id": run_id,
        "payload": PAYLOADS.get(workload, {}),
    }
    msg.update(extra)
    return msg

# CLI
def main():
    p = argparse.ArgumentParser(description="Kafka vs Sidekiq benchmark harness")
    p.add_argument("--system", choices=["kafka", "sidekiq"], required=True)
    p.add_argument("--workload", choices=["cpu_light", "io_bound", "fanout", "delayed"],
                   required=True)
    p.add_argument("--messages", type=int, default=10000,
                   help="Number of messages to produce (default 10 000)")
    p.add_argument("--warmup", type=int, default=1000,
                   help="Warmup messages before measurement (default 1 000)")
    p.add_argument("--runs", type=int, default=3,
                   help="Repeated measurement runs (default 3)")
    p.add_argument("--topic", default="benchmark",
                   help="Kafka topic name (default 'benchmark')")
    p.add_argument("--partitions", type=int, default=6,
                   help="Kafka topic partition count (default 6)")
    p.add_argument("--concurrency", type=int, default=10,
                   help="Sidekiq thread concurrency (default 10)")
    p.add_argument("--fanout-factor", type=int, default=10,
                   help="Messages per burst in fanout workload (default 10)")
    p.add_argument("--delay-seconds", type=float, default=5.0,
                   help="Max delay for scheduled workload (default 5.0)")
    args = p.parse_args()

    all_results = []
    for i in range(1, args.runs + 1):
        tag = f"[run {i}/{args.runs}]"
        result = run_once(args, run_tag=tag)
        all_results.append(result)
        if i < args.runs:
            print("  Cooldown 5 s ...\n")
            time.sleep(5)

    # Save aggregate
    if len(all_results) > 1:
        agg_path = os.path.join(
            RESULTS_DIR,
            f"agg-{args.system}-{args.workload}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        with open(agg_path, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\n  Aggregate results → {agg_path}")


if __name__ == "__main__":
    main()
