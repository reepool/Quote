#!/usr/bin/env python3
"""Run a bounded free structured A-share business-profile sync."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path
from typing import Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_structured_sync import (
    WRITE_OPERATOR_SWITCH,
    StructuredBusinessProfileSyncService,
)
from research.storage import ResearchStorageManager
from utils.config_manager import UnifiedConfigManager


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument(
        "--source",
        action="append",
        help="Configured source id; repeat or use comma-separated values",
    )
    parser.add_argument(
        "--industry-group",
        action="append",
        help="First-wave industry group; repeat or use comma-separated values",
    )
    parser.add_argument(
        "--instrument",
        action="append",
        help="Canonical A-share instrument id; repeat or use comma-separated values",
    )
    parser.add_argument("--max-instruments", type=int)
    parser.add_argument("--max-elapsed-seconds", type=float)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--cache-raw-snapshots", action="store_true")
    parser.add_argument(
        "--raw-cache-root",
        type=Path,
        help="Dry-run cache override, normally an isolated /tmp directory",
    )
    parser.add_argument(
        "--probe-disabled-config",
        action="store_true",
        help="Allow a read-only dry-run while production sync remains disabled",
    )
    parser.add_argument("--candidate-write", action="store_true")
    parser.add_argument(
        "--operator-switch",
        default="",
        help=f"Candidate writes require the literal {WRITE_OPERATOR_SWITCH}",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    research_config = UnifiedConfigManager("config").get_research_config()
    storage = ResearchStorageManager(research_config)
    if args.candidate_write:
        storage.initialize()
    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=research_config,
    )
    report = asyncio.run(
        service.sync(
            as_of_date=args.as_of_date,
            sources=_split_values(args.source),
            industry_groups=_split_values(args.industry_group),
            instrument_ids=_split_values(args.instrument),
            max_instruments=args.max_instruments,
            max_elapsed_seconds=args.max_elapsed_seconds,
            dry_run=not args.candidate_write,
            candidate_write=args.candidate_write,
            operator_switch=args.operator_switch,
            allow_disabled_dry_run=args.probe_disabled_config,
            cache_raw_snapshots=args.cache_raw_snapshots,
            raw_cache_root=args.raw_cache_root,
            checkpoint_path=args.checkpoint,
            resume=args.resume,
        )
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
