#!/usr/bin/env python3
"""Run the temporary SQLite business-profile bulk-write regression benchmark."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_bulk_benchmark import (
    run_business_profile_bulk_write_benchmark,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--minimum-rows-per-second", type=float, default=200.0)
    parser.add_argument("--maximum-elapsed-seconds", type=float, default=10.0)
    args = parser.parse_args(argv)
    result = run_business_profile_bulk_write_benchmark(
        row_count=args.rows,
        minimum_rows_per_second=args.minimum_rows_per_second,
        maximum_elapsed_seconds=args.maximum_elapsed_seconds,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
