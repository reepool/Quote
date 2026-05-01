#!/usr/bin/env python
"""Run financial statement backfill/readiness validation as a repository command."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_statements_sync import build_financial_report_periods  # noqa: E402
from scripts.research_cli_support import (  # noqa: E402
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


def parse_report_periods(raw: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated report periods such as 2024Q1,2024-06-30."""
    if raw is None:
        return None
    periods = [part.strip() for part in raw.split(",") if part.strip()]
    return periods or None


def build_configured_report_periods(manager: Any) -> List[str]:
    """Resolve financial report periods from in-memory research config."""
    module_cfg = manager.research_config.modules.get("financial_statements", {})
    history_cfg = module_cfg.get("history", {})
    storage_cfg = module_cfg.get("storage", {})
    hot_anchor_policy = storage_cfg.get("hot_anchor_policy", {})
    return build_financial_report_periods(
        baseline_report_period=str(history_cfg.get("baseline_report_period", "2024Q1")),
        rolling_min_quarters=int(history_cfg.get("rolling_min_quarters", 8)),
        optional_anchor_period=history_cfg.get("optional_ttm_anchor_period"),
        include_optional_anchor=bool(
            hot_anchor_policy.get("include_ttm_anchor_period", False)
        ),
    )


async def collect_target_instruments(
    manager: Any,
    *,
    exchanges: Optional[List[str]],
    limit_per_exchange: Optional[int],
    lookup_timeout_seconds: float = 15.0,
) -> List[str]:
    """Collect active stock instrument ids for repository readiness validation."""
    target_exchanges = exchanges or manager.research_config.markets
    instrument_ids: List[str] = []
    for exchange in target_exchanges:
        getter = manager.db_ops.get_instruments_by_exchange
        if inspect.iscoroutinefunction(getter):
            instruments = await asyncio.wait_for(
                getter(exchange),
                timeout=lookup_timeout_seconds,
            )
        else:
            instruments = await asyncio.wait_for(
                asyncio.to_thread(getter, exchange),
                timeout=lookup_timeout_seconds,
            )
        stocks = [
            item
            for item in instruments
            if item.get("type") == "stock" and item.get("is_active", True)
        ]
        if limit_per_exchange is not None:
            stocks = stocks[:limit_per_exchange]
        instrument_ids.extend(
            str(item["instrument_id"]) for item in stocks if item.get("instrument_id")
        )
    return instrument_ids


async def run_rollout_validation(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = 1,
    budget_mode: Optional[str] = "availability_first",
    allow_paid_proxy: Optional[bool] = False,
    report_periods: Optional[List[str]] = None,
    sync_mode: str = "backfill",
    force_full: bool = False,
    skip_sync: bool = False,
    enable_module: bool = False,
) -> Dict[str, Any]:
    """Run small-sample financial statement sync and repository readiness."""
    module_cfg = manager.research_config.modules.setdefault("financial_statements", {})
    before_enabled = module_cfg.get("enabled")
    if enable_module:
        module_cfg["enabled"] = True

    target_periods = report_periods or build_configured_report_periods(manager)
    instrument_lookup_error: Optional[str] = None
    try:
        target_instruments = await collect_target_instruments(
            manager,
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
        )
    except Exception as exc:
        target_instruments = []
        instrument_lookup_error = str(exc)
    sync_result: Dict[str, Any]
    if skip_sync:
        sync_result = {"status": "skipped", "reason": "skip_sync=true"}
    else:
        sync_result = await manager.run_financial_statements_shadow_sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            report_periods=target_periods,
            sync_mode=sync_mode,
            force_full=force_full,
        )

    storage = getattr(manager, "research_storage", None)
    if storage is None:
        readiness = {
            "status": "unavailable",
            "ready_for_rollout": False,
            "blockers": ["research_storage_unavailable"],
        }
    else:
        required_core_facts = list(
            module_cfg.get("readiness", {}).get(
                "required_core_facts",
                ["revenue", "net_income", "equity", "total_assets", "total_liabilities"],
            )
        )
        fallback_sources = list(
            module_cfg.get("fallback_policy", {}).get(
                "fallback_source_priority",
                ["akshare"],
            )
        )
        readiness = storage.financial_statements.validate_readiness(
            expected_periods=target_periods,
            instrument_ids=target_instruments,
            required_core_facts=required_core_facts,
            fallback_sources=fallback_sources,
        )

    return {
        "status": "ready" if readiness.get("ready_for_rollout") else "not_ready",
        "requested": {
            "exchanges": exchanges,
            "limit_per_exchange": limit_per_exchange,
            "budget_mode": budget_mode,
            "allow_paid_proxy": allow_paid_proxy,
            "report_periods": target_periods,
            "sync_mode": sync_mode,
            "force_full": force_full,
            "skip_sync": skip_sync,
            "enable_module": enable_module,
        },
        "runtime_overrides": {
            "financial_statements_enabled_before": before_enabled,
            "financial_statements_enabled_after": module_cfg.get("enabled"),
            "instrument_lookup_error": instrument_lookup_error,
        },
        "sync": sync_result,
        "readiness": readiness,
        "summary": {
            "ready_for_rollout": bool(readiness.get("ready_for_rollout")),
            "target_instrument_count": len(target_instruments),
            "target_period_count": len(target_periods),
            "blockers": readiness.get("blockers", []),
        },
    }


async def run_rollout_validation_with_lifecycle(
    manager: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Initialize manager, run validation, and always close resources."""
    await initialize_manager_for_research_cli(manager)
    try:
        return await run_rollout_validation(manager, **kwargs)
    finally:
        close = getattr(manager, "close", None)
        if close is not None:
            await close()


def exit_code_for_result(result: Dict[str, Any], *, fail_on_not_ready: bool) -> int:
    if fail_on_not_ready and not bool(
        result.get("summary", {}).get("ready_for_rollout")
    ):
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run financial statement backfill and rollout validation.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE,BSE. Defaults to research_config markets.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        default=1,
        help="Small-sample instrument limit per exchange. Defaults to 1.",
    )
    parser.add_argument(
        "--budget-mode",
        default="availability_first",
        help="Research source budget mode override. Defaults to availability_first.",
    )
    parser.add_argument(
        "--allow-paid-proxy",
        action="store_true",
        default=False,
        help="Allow paid proxy sources for this validation run.",
    )
    parser.add_argument(
        "--report-periods",
        help="Comma-separated report periods. Defaults to financial_statements history config.",
    )
    parser.add_argument(
        "--sync-mode",
        choices=["backfill", "catchup"],
        default="backfill",
        help="Financial statement sync mode. Defaults to backfill.",
    )
    parser.add_argument(
        "--force-full",
        action="store_true",
        help="Ignore checkpoint unchanged-file skips for this run.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip sync and only validate repository readiness.",
    )
    parser.add_argument(
        "--enable-module",
        action="store_true",
        help="Temporarily set financial_statements.enabled=true in memory.",
    )
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Exit with code 2 when readiness does not pass.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    from data_manager import data_manager

    result = asyncio.run(
        run_rollout_validation_with_lifecycle(
            data_manager,
            exchanges=parse_exchanges(args.exchanges),
            limit_per_exchange=args.limit_per_exchange,
            budget_mode=args.budget_mode,
            allow_paid_proxy=args.allow_paid_proxy,
            report_periods=parse_report_periods(args.report_periods),
            sync_mode=args.sync_mode,
            force_full=args.force_full,
            skip_sync=args.skip_sync,
            enable_module=args.enable_module,
        )
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return exit_code_for_result(result, fail_on_not_ready=args.fail_on_not_ready)


if __name__ == "__main__":
    raise SystemExit(main())
