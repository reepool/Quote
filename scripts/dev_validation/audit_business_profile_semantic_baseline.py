#!/usr/bin/env python3
"""Build a read-only hash-bound business-profile semantic production baseline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_semantic_baseline import (
    build_business_profile_semantic_baseline,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    parser.add_argument(
        "--financials-db", type=Path, default=Path("data/financials.db")
    )
    parser.add_argument(
        "--archive-root", type=Path, default=Path("data/filings/business_profile")
    )
    parser.add_argument(
        "--research-config", type=Path, default=Path("config/10_research.json")
    )
    parser.add_argument(
        "--scheduler-config", type=Path, default=Path("config/05_scheduler.json")
    )
    parser.add_argument(
        "--fact-catalog",
        type=Path,
        default=Path("config/business_profile_fact_catalog.json"),
    )
    parser.add_argument(
        "--product-catalog",
        type=Path,
        default=Path("config/business_profile_product_catalog.json"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = build_business_profile_semantic_baseline(
        research_db_path=args.research_db,
        financials_db_path=args.financials_db,
        archive_root=args.archive_root,
        research_config_path=args.research_config,
        scheduler_config_path=args.scheduler_config,
        fact_catalog_path=args.fact_catalog,
        product_catalog_path=args.product_catalog,
    )
    rendered = f"{json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)}\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
