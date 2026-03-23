from __future__ import annotations

import argparse
from pathlib import Path

from benchkit.logging import configure_logging
from benchkit.resource_usage import enrich_results_from_csv


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Attach resource usage summaries to result files")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--compose-project")
    args = parser.parse_args()
    enrich_results_from_csv(
        csv_path=Path(args.csv),
        results_dir=Path(args.results_dir),
        compose_project=args.compose_project,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
