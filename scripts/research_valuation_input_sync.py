#!/usr/bin/env python
"""Run valuation input synchronization as a repeatable repository command."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def _parse_exchanges(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    exchanges = [part.strip().upper() for part in raw.split(",") if part.strip()]
    return exchanges or None


def _parse_csv(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    values = [part.strip() for part in raw.split(",") if part.strip()]
    return values or None


def _json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


async def run_valuation_input_sync(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    source: Optional[str] = None,
    source_mode: Optional[str] = None,
    sync_mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    target_instrument_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run valuation input sync and return a structured result."""
    return await manager.run_valuation_input_sync(
        exchanges=exchanges,
        limit_per_exchange=limit_per_exchange,
        source=source,
        source_mode=source_mode,
        sync_mode=sync_mode,
        start_date=start_date,
        end_date=end_date,
        target_instrument_ids=target_instrument_ids,
    )


async def run_valuation_input_sync_with_lifecycle(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    source: Optional[str] = None,
    source_mode: Optional[str] = None,
    sync_mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    target_instrument_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Initialize manager, run input sync, and always close resources."""
    await _initialize_manager(manager)
    try:
        return await run_valuation_input_sync(
            manager,
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            source=source,
            source_mode=source_mode,
            sync_mode=sync_mode,
            start_date=start_date,
            end_date=end_date,
            target_instrument_ids=target_instrument_ids,
        )
    finally:
        close = getattr(manager, "close", None)
        if close is not None:
            await close()


async def _initialize_manager(manager: Any) -> None:
    initialize = getattr(manager, "initialize")
    try:
        parameters = inspect.signature(initialize).parameters
    except (TypeError, ValueError):
        parameters = {}

    supports_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    if (
        "include_data_sources" in parameters
        or "load_progress" in parameters
        or supports_kwargs
    ):
        await initialize(
            include_data_sources=False,
            load_progress=False,
        )
        return

    await initialize()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run valuation input synchronization.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE. Defaults to research_config markets.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange instrument limit.",
    )
    parser.add_argument(
        "--target-instrument-ids",
        help="Comma-separated instrument IDs for bounded sync, for example 600000.SH,001233.SZ,920003.BJ.",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Valuation input source. Defaults to valuation.input_sync.primary_source.",
    )
    parser.add_argument(
        "--source-mode",
        default=None,
        help="Valuation input source mode. Defaults to valuation.input_sync.source_mode.",
    )
    parser.add_argument(
        "--sync-mode",
        default="incremental",
        choices=["incremental", "full", "history", "backfill"],
        help="incremental uses all-market CNInfo snapshot; full/history/backfill fetch per-symbol capital-change events.",
    )
    parser.add_argument("--start-date", help="Optional YYYY-MM-DD lower bound.")
    parser.add_argument("--end-date", help="Optional YYYY-MM-DD upper bound.")
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    result = await run_valuation_input_sync_with_lifecycle(
        data_manager,
        exchanges=_parse_exchanges(args.exchanges),
        limit_per_exchange=args.limit_per_exchange,
        source=args.source,
        source_mode=args.source_mode,
        sync_mode=args.sync_mode,
        start_date=args.start_date,
        end_date=args.end_date,
        target_instrument_ids=_parse_csv(args.target_instrument_ids),
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("status") in {"success", "degraded"} else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
