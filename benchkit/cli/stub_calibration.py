from __future__ import annotations

import argparse
from pathlib import Path

from benchkit.config import RuntimeConfig
from benchkit.logging import configure_logging
from benchkit.stub_calibration import run_stub_calibration
from benchkit.timing import iso_compact, utc_now


def main() -> int:
    configure_logging()
    runtime = RuntimeConfig()
    parser = argparse.ArgumentParser(description="Run stub capacity calibration")
    parser.add_argument("--concurrency-sweep", nargs="+", type=int, default=[1, 4, 8, 16, 32])
    parser.add_argument("--requests-per-worker", type=int, default=100)
    parser.add_argument("--delay-ms", type=int, default=50)
    parser.add_argument("--stub-workers", type=int, default=runtime.stub_server_workers)
    parser.add_argument("--output-dir", default="results/calibration")
    args = parser.parse_args()
    output_path = Path(args.output_dir) / f"stub-calibration-{iso_compact(utc_now())}.json"
    run_stub_calibration(
        stub_url=runtime.stub_server_url,
        output_path=output_path,
        concurrency_sweep=args.concurrency_sweep,
        requests_per_worker=args.requests_per_worker,
        delay_ms=args.delay_ms,
        stub_workers=args.stub_workers,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
