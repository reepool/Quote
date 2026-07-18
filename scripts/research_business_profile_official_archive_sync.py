#!/usr/bin/env python3
"""Discover or archive official reports required by product-label review."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_official_archive_sync import (
    OFFICIAL_ARCHIVE_WRITE_SWITCH,
    BusinessProfileOfficialArchiveSyncService,
)
from research.storage import ResearchStorageManager
from utils.config_manager import UnifiedConfigManager


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-research-db",
        type=Path,
        default=Path("data/research.db"),
    )
    parser.add_argument("--instrument", action="append")
    parser.add_argument("--report-period")
    parser.add_argument("--minimum-revenue-share", type=float, default=0.01)
    parser.add_argument("--max-instruments", type=int, default=5)
    parser.add_argument("--max-documents-per-instrument", type=int, default=30)
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--as-of-date")
    parser.add_argument("--checkpoint-root", type=Path)
    parser.add_argument("--archive-write", action="store_true")
    parser.add_argument(
        "--operator-switch",
        default="",
        help=f"Archive writes require the literal {OFFICIAL_ARCHIVE_WRITE_SWITCH}",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    if args.archive_write and args.operator_switch != OFFICIAL_ARCHIVE_WRITE_SWITCH:
        raise PermissionError(
            "official archive writes require operator switch "
            f"{OFFICIAL_ARCHIVE_WRITE_SWITCH}"
        )
    research_config = UnifiedConfigManager("config").get_research_config()
    storage = ResearchStorageManager(research_config)
    if args.archive_write:
        storage.initialize()
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=research_config,
    )
    report = service.sync(
        target_research_db=args.target_research_db,
        instrument_ids=_split_values(args.instrument),
        report_period=args.report_period,
        minimum_revenue_share=args.minimum_revenue_share,
        max_instruments=args.max_instruments,
        max_documents_per_instrument=args.max_documents_per_instrument,
        page_size=args.page_size,
        max_pages=args.max_pages,
        start_date=args.start_date,
        end_date=args.end_date,
        as_of_date=args.as_of_date,
        archive_write=args.archive_write,
        operator_switch=args.operator_switch,
        checkpoint_root=args.checkpoint_root,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is None:
        print(rendered)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    return 0 if report["status"] == "success" else 3


def _split_values(values: Optional[Sequence[str]]) -> Optional[list[str]]:
    if not values:
        return None
    output = [
        part.strip()
        for value in values
        for part in str(value).split(",")
        if part.strip()
    ]
    return output or None


if __name__ == "__main__":
    raise SystemExit(main())
