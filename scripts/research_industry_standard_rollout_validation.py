#!/usr/bin/env python
"""Run strict Shenwan rollout validation as a repeatable repository command."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import (
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


async def run_rollout_validation(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    budget_mode: Optional[str] = None,
    allow_paid_proxy: Optional[bool] = None,
    skip_refresh: bool = True,
    skip_sync: bool = False,
) -> Dict[str, Any]:
    """Run current membership sync -> readiness and return a structured summary."""
    refresh_result: Dict[str, Any]
    if skip_refresh:
        refresh_result = {"status": "skipped", "reason": "skip_refresh=true"}
    else:
        refresh_result = await manager.run_industry_official_mapping_refresh(
            exchanges=exchanges,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )

    sync_result: Dict[str, Any]
    if skip_sync:
        sync_result = {"status": "skipped", "reason": "skip_sync=true"}
    else:
        sync_result = await manager.run_industry_standard_sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )

    readiness = await manager.get_research_industry_standard_readiness()
    industry_ready = bool(readiness.get("industry_standard_ready"))
    relative_valuation = readiness.get("relative_valuation") or {}
    return {
        "status": "ready" if industry_ready else "not_ready",
        "requested": {
            "exchanges": exchanges,
            "limit_per_exchange": limit_per_exchange,
            "budget_mode": budget_mode,
            "allow_paid_proxy": allow_paid_proxy,
            "skip_refresh": skip_refresh,
            "skip_sync": skip_sync,
        },
        "refresh": refresh_result,
        "sync": sync_result,
        "readiness": readiness,
        "summary": {
            "industry_standard_ready": industry_ready,
            "blockers": readiness.get("blockers", []),
            "relative_valuation_ready": bool(relative_valuation.get("ready")),
            "relative_valuation_blockers": relative_valuation.get("blockers", []),
        },
    }


async def run_rollout_validation_with_lifecycle(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    budget_mode: Optional[str] = None,
    allow_paid_proxy: Optional[bool] = None,
    skip_refresh: bool = True,
    skip_sync: bool = False,
) -> Dict[str, Any]:
    """Initialize manager, run validation, and always close resources."""
    await initialize_manager_for_research_cli(manager)
    try:
        return await run_rollout_validation(
            manager,
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            skip_refresh=skip_refresh,
            skip_sync=skip_sync,
        )
    finally:
        close = getattr(manager, "close", None)
        if close is not None:
            await close()


def exit_code_for_result(
    result: Dict[str, Any],
    *,
    fail_on_not_ready: bool,
) -> int:
    if fail_on_not_ready and not bool(
        result.get("summary", {}).get("industry_standard_ready")
    ):
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run strict Shenwan rollout validation.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE. Defaults to research_config markets.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange instrument limit for industry_standard_sync.",
    )
    parser.add_argument("--budget-mode", help="Research source budget mode override.")
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument(
        "--allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_true",
        default=None,
        help="Allow paid proxy sources for this validation run.",
    )
    proxy_group.add_argument(
        "--no-allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_false",
        help="Disable paid proxy sources for this validation run.",
    )
    refresh_group = parser.add_mutually_exclusive_group()
    refresh_group.add_argument(
        "--include-official-refresh",
        dest="skip_refresh",
        action="store_false",
        default=True,
        help="Also run audit-only industry_official_mapping_refresh before current sync.",
    )
    refresh_group.add_argument(
        "--skip-refresh",
        dest="skip_refresh",
        action="store_true",
        help="Skip audit-only industry_official_mapping_refresh. This is the default.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip industry_standard_sync.",
    )
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Exit with code 2 when readiness is not ready.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    result = await run_rollout_validation_with_lifecycle(
        data_manager,
        exchanges=parse_exchanges(args.exchanges),
        limit_per_exchange=args.limit_per_exchange,
        budget_mode=args.budget_mode,
        allow_paid_proxy=args.allow_paid_proxy,
        skip_refresh=args.skip_refresh,
        skip_sync=args.skip_sync,
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
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
