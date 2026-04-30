#!/usr/bin/env python
"""Run shareholders rollout validation as a repeatable repository command."""

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


def _apply_shareholder_runtime_overrides(
    manager: Any,
    *,
    exchanges: Optional[List[str]],
    enable_module: bool,
    delivery_mode: Optional[str],
    snapshot_api_requires_mode: Optional[str],
) -> Dict[str, Any]:
    """Apply in-memory shareholder rollout gates for this validation run only."""
    research_config = getattr(manager, "research_config")
    module_cfg = research_config.modules.setdefault("shareholders", {})
    before = {
        "markets": list(getattr(research_config, "markets", []) or []),
        "enabled": module_cfg.get("enabled"),
        "delivery_mode": module_cfg.get("delivery_mode"),
        "snapshot_api_requires_mode": module_cfg.get("snapshot_api_requires_mode"),
    }

    if exchanges:
        research_config.markets = list(exchanges)
    if enable_module:
        module_cfg["enabled"] = True
    if delivery_mode:
        module_cfg["delivery_mode"] = delivery_mode
    if snapshot_api_requires_mode:
        module_cfg["snapshot_api_requires_mode"] = snapshot_api_requires_mode

    after = {
        "markets": list(getattr(research_config, "markets", []) or []),
        "enabled": module_cfg.get("enabled"),
        "delivery_mode": module_cfg.get("delivery_mode"),
        "snapshot_api_requires_mode": module_cfg.get("snapshot_api_requires_mode"),
    }
    return {"before": before, "after": after}


async def run_rollout_validation(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    budget_mode: Optional[str] = "availability_first",
    allow_paid_proxy: Optional[bool] = True,
    skip_sync: bool = False,
    enable_module: bool = False,
    delivery_mode: Optional[str] = None,
    snapshot_api_requires_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Run shareholder shadow sync -> readiness and return a structured summary."""
    overrides = _apply_shareholder_runtime_overrides(
        manager,
        exchanges=exchanges,
        enable_module=enable_module,
        delivery_mode=delivery_mode,
        snapshot_api_requires_mode=snapshot_api_requires_mode,
    )

    sync_result: Dict[str, Any]
    if skip_sync:
        sync_result = {"status": "skipped", "reason": "skip_sync=true"}
    else:
        sync_result = await manager.run_shareholder_shadow_sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )

    readiness = await manager.get_research_shareholder_readiness()
    ready = bool(readiness.get("ready_for_paid_high_availability_rollout"))
    return {
        "status": "ready" if ready else "not_ready",
        "requested": {
            "exchanges": exchanges,
            "limit_per_exchange": limit_per_exchange,
            "budget_mode": budget_mode,
            "allow_paid_proxy": allow_paid_proxy,
            "skip_sync": skip_sync,
            "enable_module": enable_module,
            "delivery_mode": delivery_mode,
            "snapshot_api_requires_mode": snapshot_api_requires_mode,
        },
        "runtime_overrides": overrides,
        "sync": sync_result,
        "readiness": readiness,
        "summary": {
            "ready_for_paid_high_availability_rollout": ready,
            "module_enabled": bool(readiness.get("module_enabled")),
            "snapshot_api_enabled": bool(readiness.get("snapshot_api_enabled")),
            "delivery_mode": readiness.get("delivery_mode"),
            "target_instrument_count": int(readiness.get("target_instrument_count", 0)),
            "snapshot_total": int(readiness.get("snapshot_total", 0)),
            "missing_snapshot_count": int(
                readiness.get("missing_snapshot_count", 0)
            ),
            "scope_counts": readiness.get("scope_counts", {}),
            "blockers": readiness.get("blockers", []),
        },
    }


async def run_rollout_validation_with_lifecycle(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    limit_per_exchange: Optional[int] = None,
    budget_mode: Optional[str] = "availability_first",
    allow_paid_proxy: Optional[bool] = True,
    skip_sync: bool = False,
    enable_module: bool = False,
    delivery_mode: Optional[str] = None,
    snapshot_api_requires_mode: Optional[str] = None,
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
            skip_sync=skip_sync,
            enable_module=enable_module,
            delivery_mode=delivery_mode,
            snapshot_api_requires_mode=snapshot_api_requires_mode,
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
        result.get("summary", {}).get("ready_for_paid_high_availability_rollout")
    ):
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run shareholders rollout validation.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE. Defaults to research_config markets.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange instrument limit for shareholder_shadow_sync.",
    )
    parser.add_argument(
        "--budget-mode",
        default="availability_first",
        help="Research source budget mode override. Defaults to availability_first.",
    )
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument(
        "--allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_true",
        default=True,
        help="Allow paid proxy sources for this validation run. This is the default.",
    )
    proxy_group.add_argument(
        "--no-allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_false",
        help="Disable paid proxy sources for this validation run.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip shareholder_shadow_sync and only query readiness.",
    )
    parser.add_argument(
        "--enable-module",
        action="store_true",
        help="Temporarily set shareholders.enabled=true in memory for this run.",
    )
    parser.add_argument(
        "--delivery-mode",
        choices=["free_best_effort", "paid_high_availability"],
        help="Temporarily override shareholders.delivery_mode in memory.",
    )
    parser.add_argument(
        "--snapshot-api-requires-mode",
        choices=["free_best_effort", "paid_high_availability"],
        help="Temporarily override shareholders.snapshot_api_requires_mode in memory.",
    )
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Exit with code 2 when shareholder readiness is not ready.",
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
        skip_sync=bool(args.skip_sync),
        enable_module=bool(args.enable_module),
        delivery_mode=args.delivery_mode,
        snapshot_api_requires_mode=args.snapshot_api_requires_mode,
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
