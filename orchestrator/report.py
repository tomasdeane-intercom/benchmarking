#!/usr/bin/env python3
"""Generate a Markdown comparison report from benchmark result JSON files.

Usage:
    python report.py                       # reads all results/*.json
    python report.py --dir /app/results    # custom directory
"""

import argparse
import json
import os
import statistics
from collections import defaultdict
from datetime import datetime

def load_results(results_dir: str):
    """Load all non-aggregate result JSON files."""
    results = []
    for fname in sorted(os.listdir(results_dir)):
        if fname.startswith("agg-") or not fname.endswith(".json"):
            continue
        path = os.path.join(results_dir, fname)
        with open(path) as f:
            results.append(json.load(f))
    return results

def group_results(results: list):
    """Group results by (system, workload)."""
    groups = defaultdict(list)
    for r in results:
        key = (r["system"], r["workload"])
        groups[key].append(r)
    return groups

def avg_stat(runs: list, *path):
    """Average a nested stat across runs."""
    vals = []
    for r in runs:
        obj = r.get("statistics", {})
        for p in path:
            obj = obj.get(p, {}) if isinstance(obj, dict) else 0
        if isinstance(obj, (int, float)):
            vals.append(obj)
    return round(statistics.mean(vals), 2) if vals else "N/A"

def make_comparison_table(groups: dict, workload: str):
    """Build a Markdown table comparing kafka vs sidekiq for one workload."""
    kafka = groups.get(("kafka", workload), [])
    sidekiq = groups.get(("sidekiq", workload), [])

    if not kafka and not sidekiq:
        return ""

    def col(runs):
        if not runs:
            return ["—"] * 9
        return [
            str(avg_stat(runs, "throughput", "consumed_msg_per_sec")),
            str(avg_stat(runs, "throughput", "produce_msg_per_sec")),
            str(avg_stat(runs, "latency_ms", "median")),
            str(avg_stat(runs, "latency_ms", "p95")),
            str(avg_stat(runs, "latency_ms", "p99")),
            str(avg_stat(runs, "latency_ms", "max")),
            str(avg_stat(runs, "timing_s", "drain")),
            str(avg_stat(runs, "error_rate_pct")),
            f"{len(runs)} run(s)",
        ]

    kc = col(kafka)
    sc = col(sidekiq)

    rows = [
        ("Consumed msg/s", kc[0], sc[0]),
        ("Produce msg/s", kc[1], sc[1]),
        ("Latency p50 (ms)", kc[2], sc[2]),
        ("Latency p95 (ms)", kc[3], sc[3]),
        ("Latency p99 (ms)", kc[4], sc[4]),
        ("Latency max (ms)", kc[5], sc[5]),
        ("Drain time (s)", kc[6], sc[6]),
        ("Error rate (%)", kc[7], sc[7]),
        ("Runs", kc[8], sc[8]),
    ]

    lines = [
        f"### {workload}\n",
        "| Metric | Kafka | Sidekiq |",
        "|--------|------:|--------:|",
    ]
    for label, kv, sv in rows:
        lines.append(f"| {label} | {kv} | {sv} |")
    lines.append("")
    return "\n".join(lines)

def generate_report(results_dir: str):
    results = load_results(results_dir)
    if not results:
        print("No result files found.")
        return

    groups = group_results(results)
    workloads_seen = sorted({r["workload"] for r in results})

    lines = [
        "# Kafka vs Sidekiq — Benchmark Report",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"\nTotal runs: {len(results)}\n",
        "---\n",
        "## Summary Comparison\n",
    ]

    for wl in workloads_seen:
        table = make_comparison_table(groups, wl)
        if table:
            lines.append(table)

    # Per-run detail
    lines.append("---\n")
    lines.append("## Detailed Run Results\n")

    for r in results:
        s = r.get("statistics", {})
        lat = s.get("latency_ms", {})
        tp = s.get("throughput", {})
        tm = s.get("timing_s", {})
        lines.append(f"### {r['run_id']}")
        lines.append(f"- **System**: {r['system']}  **Workload**: {r['workload']}")
        lines.append(f"- **Messages**: {s.get('successful', '?')} ok / "
                      f"{s.get('failed', '?')} err")
        lines.append(f"- **Throughput**: {tp.get('consumed_msg_per_sec', '?')} msg/s consumed")
        lines.append(f"- **Latency**: p50={lat.get('median')} p95={lat.get('p95')} "
                      f"p99={lat.get('p99')} max={lat.get('max')} ms")
        lines.append(f"- **Timing**: total={tm.get('total_wall')}s "
                      f"produce={tm.get('produce')}s drain={tm.get('drain')}s")
        cfg = r.get("config", {})
        cfg_parts = [f"{k}={v}" for k, v in cfg.items() if v is not None]
        lines.append(f"- **Config**: {', '.join(cfg_parts)}")
        lines.append("")

    report = "\n".join(lines)

    out_path = os.path.join(results_dir, "report.md")
    with open(out_path, "w") as f:
        f.write(report)
    print(report)
    print(f"\n\nReport saved → {out_path}")

def main():
    p = argparse.ArgumentParser(description="Generate benchmark comparison report")
    p.add_argument("--dir", default=os.environ.get("RESULTS_DIR", "/app/results"),
                   help="Results directory")
    args = p.parse_args()
    generate_report(args.dir)

if __name__ == "__main__":
    main()
