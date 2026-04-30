#!/usr/bin/env python
"""Rebuild strict Shenwan industry memberships from the official classification source."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import (
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)
from research.industry_standard_operations import rebuild_official_industry_standard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild strict Shenwan industry standard rows from the official source.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE,BSE. Defaults to research_config markets.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange instrument limit for a smoke rebuild.",
    )
    parser.add_argument("--budget-mode", help="Research source budget mode override.")
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument(
        "--allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_true",
        default=None,
        help="Allow paid proxy sources.",
    )
    proxy_group.add_argument(
        "--no-allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_false",
        help="Disable paid proxy sources.",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Clear rebuildable strict industry rows before syncing.",
    )
    parser.add_argument(
        "--drop-source-files",
        action="store_true",
        help="Also clear cached official source-file manifests; implies a non-conditional download.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Bypass conditional source-file refresh for this run.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    await initialize_manager_for_research_cli(data_manager)
    try:
        result = await rebuild_official_industry_standard(
            data_manager,
            exchanges=parse_exchanges(args.exchanges),
            limit_per_exchange=args.limit_per_exchange,
            budget_mode=args.budget_mode,
            allow_paid_proxy=args.allow_paid_proxy,
            drop_existing=bool(args.drop_existing),
            drop_source_files=bool(args.drop_source_files),
            force_refresh=bool(args.force_refresh),
        )
        print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result.get("status") == "success" else 2
    finally:
        await data_manager.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
