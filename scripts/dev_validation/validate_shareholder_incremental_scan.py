#!/usr/bin/env python
"""
Dry-run validator for CNInfo announcement-driven shareholder incremental sync.

Default mode does not write shareholder snapshots, raw payloads, manifests, or
announcement scan checkpoints.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_manager import data_manager
from research.shareholder_incremental_sync import ShareholderIncrementalSyncService


def _parse_exchanges(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    if data_manager.research_storage is None:
        await data_manager.initialize(include_data_sources=False, load_progress=False)
    if data_manager.research_storage is None:
        raise RuntimeError("research storage is not initialized")
    service = ShareholderIncrementalSyncService(
        db_ops=data_manager.db_ops,
        storage=data_manager.research_storage,
        research_config=data_manager.research_config,
    )
    return await service.sync(
        exchanges=_parse_exchanges(args.exchanges),
        lookback_days=args.lookback_days,
        overlap_days=args.overlap_days,
        page_size=args.page_size,
        max_pages_per_market=args.max_pages_per_market,
        max_candidates=args.max_candidates,
        pending_recheck_days=args.pending_recheck_days,
        budget_mode=args.budget_mode,
        allow_paid_proxy=args.allow_paid_proxy,
        dry_run=not args.write,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate shareholder incremental announcement scan and candidate refresh.",
    )
    parser.add_argument("--exchanges", default="SSE,SZSE,BSE")
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--overlap-days", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--max-pages-per-market", type=int, default=2)
    parser.add_argument("--max-candidates", type=int, default=20)
    parser.add_argument("--pending-recheck-days", type=int, default=1)
    parser.add_argument("--budget-mode", default="availability_first")
    parser.add_argument("--allow-paid-proxy", action="store_true")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist snapshots/manifests/checkpoints. Default is dry-run/no-write.",
    )
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    result = asyncio.run(_run(args))
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    print(payload)
    return 0 if result.get("status") in {"success", "degraded"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
