#!/usr/bin/env python
"""Run a bounded financial disclosure incremental smoke for selected instruments."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import initialize_manager_for_research_cli, json_ready, parse_exchanges  # noqa: E402


def _csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run targeted financial disclosure incremental smoke."
    )
    parser.add_argument("--instrument-ids", required=True)
    parser.add_argument("--exchanges", default="SSE,SZSE")
    parser.add_argument("--report-periods", default="2025-12-31,2026-03-31")
    parser.add_argument("--lookback-days", type=int, default=120)
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--max-pages-per-market", type=int, default=20)
    parser.add_argument("--pending-recheck-days", type=int, default=7)
    parser.add_argument("--db-path", default="data/financials.db")
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--write-enabled", action="store_true")
    return parser


async def async_main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    instrument_ids = _csv(args.instrument_ids)
    report_periods = _csv(args.report_periods)
    exchanges = parse_exchanges(args.exchanges)

    from data_manager import data_manager

    await initialize_manager_for_research_cli(data_manager)
    results: List[Dict[str, Any]] = []
    try:
        for instrument_id in instrument_ids:
            symbol = instrument_id.split(".")[0]
            result = await data_manager.run_financial_disclosure_incremental_sync(
                exchanges=exchanges,
                lookback_days=args.lookback_days,
                overlap_days=args.lookback_days,
                page_size=args.page_size,
                max_pages_per_market=args.max_pages_per_market,
                max_candidates=20,
                pending_recheck_days=args.pending_recheck_days,
                target_instrument_ids=[instrument_id],
                target_symbols=[symbol],
                announcement_search_key=symbol,
                report_periods=report_periods,
                db_path=args.db_path,
                dry_run=not args.write_enabled,
            )
            results.append(result)
    finally:
        close = getattr(data_manager, "close", None)
        if close is not None:
            await close()

    payload = {
        "status": (
            "success"
            if all(result.get("status") in {"success", "degraded"} for result in results)
            else "failed"
        ),
        "write_enabled": bool(args.write_enabled),
        "instrument_ids": instrument_ids,
        "report_periods": report_periods,
        "db_path": args.db_path,
        "results": results,
    }
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(
        json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["status"] == "success" else 2


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
