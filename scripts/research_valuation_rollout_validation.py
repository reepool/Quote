#!/usr/bin/env python
"""Run valuation rollout validation as a repeatable repository command."""

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


async def run_rollout_validation(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    target_instrument_ids: Optional[List[str]] = None,
    sync_inputs: bool = False,
    input_sync_mode: str = "incremental",
    skip_sync: bool = False,
    allow_disabled_module: bool = False,
) -> Dict[str, Any]:
    """Run valuation_history rebuild -> readiness and return a structured summary."""
    input_sync_result: Dict[str, Any]
    if sync_inputs:
        input_sync_result = await manager.run_valuation_input_sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            target_instrument_ids=target_instrument_ids,
            sync_mode=input_sync_mode,
        )
    else:
        input_sync_result = {"status": "skipped", "reason": "sync_inputs=false"}

    sync_result: Dict[str, Any]
    if skip_sync:
        sync_result = {"status": "skipped", "reason": "skip_sync=true"}
    else:
        sync_result = await manager.run_valuation_history_rebuild(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            target_instrument_ids=target_instrument_ids,
            allow_disabled_module=allow_disabled_module,
        )

    readiness = await manager.get_research_valuation_readiness()
    ready_for_rollout = bool(readiness.get("ready_for_rollout"))
    relative_valuation = readiness.get("relative_valuation") or {}
    return {
        "status": "ready" if ready_for_rollout else "not_ready",
        "requested": {
            "exchanges": exchanges,
            "limit_per_exchange": limit_per_exchange,
            "target_instrument_ids": target_instrument_ids,
            "sync_inputs": sync_inputs,
            "input_sync_mode": input_sync_mode,
            "skip_sync": skip_sync,
            "allow_disabled_module": allow_disabled_module,
        },
        "input_sync": input_sync_result,
        "sync": sync_result,
        "readiness": readiness,
        "summary": {
            "ready_for_rollout": ready_for_rollout,
            "module_enabled": bool(readiness.get("module_enabled")),
            "valuation_history_total": int(readiness.get("valuation_history_total", 0)),
            "missing_valuation_history_count": int(
                readiness.get("missing_valuation_history_count", 0)
            ),
            "relative_valuation_ready": bool(relative_valuation.get("ready")),
            "relative_valuation_blockers": relative_valuation.get("blockers", []),
            "blockers": readiness.get("blockers", []),
        },
    }


async def run_rollout_validation_with_lifecycle(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    target_instrument_ids: Optional[List[str]] = None,
    sync_inputs: bool = False,
    input_sync_mode: str = "incremental",
    skip_sync: bool = False,
    allow_disabled_module: bool = False,
) -> Dict[str, Any]:
    """Initialize manager, run validation, and always close resources."""
    await _initialize_manager(manager)
    try:
        return await run_rollout_validation(
            manager,
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            target_instrument_ids=target_instrument_ids,
            sync_inputs=sync_inputs,
            input_sync_mode=input_sync_mode,
            skip_sync=skip_sync,
            allow_disabled_module=allow_disabled_module,
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


def exit_code_for_result(
    result: Dict[str, Any],
    *,
    fail_on_not_ready: bool,
) -> int:
    if fail_on_not_ready and not bool(
        result.get("summary", {}).get("ready_for_rollout")
    ):
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run valuation rollout validation.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE. Defaults to research_config markets.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange instrument limit for valuation_history_rebuild.",
    )
    parser.add_argument(
        "--target-instrument-ids",
        help="Comma-separated instrument IDs for bounded validation, for example 600000.SH,001233.SZ,920003.BJ.",
    )
    parser.add_argument(
        "--sync-inputs",
        action="store_true",
        help="Run valuation_input_sync before valuation_history_rebuild.",
    )
    parser.add_argument(
        "--input-sync-mode",
        default="incremental",
        choices=["incremental", "full", "history", "backfill"],
        help="Input sync mode when --sync-inputs is used.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip valuation_history_rebuild and only query readiness.",
    )
    parser.add_argument(
        "--allow-disabled-module",
        action="store_true",
        help="Allow bounded validation rebuild while valuation.enabled remains false.",
    )
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Exit with code 2 when valuation readiness is not ready.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    result = await run_rollout_validation_with_lifecycle(
        data_manager,
        exchanges=_parse_exchanges(args.exchanges),
        limit_per_exchange=args.limit_per_exchange,
        target_instrument_ids=_parse_csv(args.target_instrument_ids),
        sync_inputs=bool(args.sync_inputs),
        input_sync_mode=args.input_sync_mode,
        skip_sync=args.skip_sync,
        allow_disabled_module=bool(args.allow_disabled_module),
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return exit_code_for_result(
        result,
        fail_on_not_ready=bool(args.fail_on_not_ready),
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
