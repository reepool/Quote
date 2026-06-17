#!/usr/bin/env python
"""Run valuation history rebuild without readiness validation overhead."""

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


async def run_valuation_history_rebuild_with_lifecycle(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    target_instrument_ids: Optional[List[str]] = None,
    quote_limit_days: Optional[int] = None,
    window_mode: str = "trading_days",
    write_policy: str = "missing_only",
    progress_log_every: int = 200,
    allow_disabled_module: bool = False,
) -> Dict[str, Any]:
    await _initialize_manager(manager)
    try:
        return await manager.run_valuation_history_rebuild(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            target_instrument_ids=target_instrument_ids,
            allow_disabled_module=allow_disabled_module,
            quote_limit_days=quote_limit_days,
            window_mode=window_mode,
            write_policy=write_policy,
            progress_log_every=progress_log_every,
        )
    finally:
        close = getattr(manager, "close", None)
        if close is not None:
            await close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run valuation_history_rebuild directly.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE,BSE.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange instrument limit.",
    )
    parser.add_argument(
        "--target-instrument-ids",
        help="Comma-separated instrument IDs for bounded rebuild.",
    )
    parser.add_argument(
        "--quote-limit-days",
        type=int,
        help="Optional quote window for valuation_history_rebuild.",
    )
    parser.add_argument(
        "--window-mode",
        default="trading_days",
        choices=["trading_days", "last_12_quarters"],
        help="Valuation history window mode.",
    )
    parser.add_argument(
        "--write-policy",
        default="missing_only",
        choices=["missing_only", "overwrite"],
        help="Write policy for valuation_history_rebuild.",
    )
    parser.add_argument(
        "--progress-log-every",
        type=int,
        default=200,
        help="Log progress every N instruments.",
    )
    parser.add_argument(
        "--allow-disabled-module",
        action="store_true",
        help="Allow rebuild while valuation.enabled remains false.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    result = await run_valuation_history_rebuild_with_lifecycle(
        data_manager,
        exchanges=_parse_exchanges(args.exchanges),
        limit_per_exchange=args.limit_per_exchange,
        target_instrument_ids=_parse_csv(args.target_instrument_ids),
        quote_limit_days=args.quote_limit_days,
        window_mode=args.window_mode,
        write_policy=args.write_policy,
        progress_log_every=args.progress_log_every,
        allow_disabled_module=bool(args.allow_disabled_module),
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("status") in {"success", "degraded"} else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
