from __future__ import annotations

import argparse
from pathlib import Path

from benchkit.logging import configure_logging
from benchkit.reporting import generate_report


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Generate report from stored results")
    parser.add_argument("--results-dir", default="results/final")
    parser.add_argument("--output-dir", default="results/report")
    args = parser.parse_args()
    generate_report(Path(args.results_dir), Path(args.output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
