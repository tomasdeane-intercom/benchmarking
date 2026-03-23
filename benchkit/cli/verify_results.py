from __future__ import annotations

import argparse
import sys
from pathlib import Path

from benchkit.logging import configure_logging
from benchkit.verify import verify_results


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Verify stored benchmark results")
    parser.add_argument("--dir", required=True)
    parser.add_argument("--require-manifest-coverage", action="store_true")
    args = parser.parse_args()
    failures = verify_results(
        Path(args.dir),
        require_manifest_coverage=args.require_manifest_coverage,
    )
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    print("verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
