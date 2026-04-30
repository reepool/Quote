#!/usr/bin/env python
"""Run strict Shenwan full maintenance as a repeatable repository command."""

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


async def run_maintenance(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    taxonomy_system: Optional[str] = None,
    taxonomy_version: Optional[str] = None,
    limit_per_exchange: Optional[int] = None,
    missing_limit_per_exchange: Optional[int] = None,
    budget_mode: Optional[str] = None,
    allow_paid_proxy: Optional[bool] = None,
    include_official_refresh: bool = False,
    force_component_refresh: bool = False,
    skip_sync: bool = False,
    skip_gap_fill: bool = False,
) -> Dict[str, Any]:
    """Run full sync -> optional gap fill -> readiness for strict Shenwan."""
    coverage_before = await manager.get_research_industry_standard_coverage_gaps(
        exchanges=exchanges,
        taxonomy_system=taxonomy_system,
        taxonomy_version=taxonomy_version,
        missing_limit_per_exchange=missing_limit_per_exchange,
        include_missing_instrument_ids=True,
    )

    if include_official_refresh:
        refresh_result = await manager.run_industry_official_mapping_refresh(
            exchanges=exchanges,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
    else:
        refresh_result = {
            "status": "skipped",
            "reason": "include_official_refresh=false",
        }

    if skip_sync:
        sync_result = {"status": "skipped", "reason": "skip_sync=true"}
        coverage_after_sync = coverage_before
    else:
        sync_result = await manager.run_industry_standard_sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            force_component_refresh=force_component_refresh,
        )
        coverage_after_sync = await manager.get_research_industry_standard_coverage_gaps(
            exchanges=exchanges,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            missing_limit_per_exchange=missing_limit_per_exchange,
            include_missing_instrument_ids=True,
        )

    if skip_gap_fill:
        gap_fill_result = {
            "status": "skipped",
            "reason": "skip_gap_fill=true",
            "coverage_before": coverage_after_sync,
            "coverage_after": coverage_after_sync,
            "repaired_instrument_count": 0,
        }
    else:
        gap_fill_result = await manager.run_industry_standard_gap_fill_sync(
            exchanges=exchanges,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            missing_limit_per_exchange=missing_limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )

    readiness = await manager.get_research_industry_standard_readiness(
        taxonomy_system=taxonomy_system,
        taxonomy_version=taxonomy_version,
    )
    industry_ready = bool(readiness.get("industry_standard_ready"))
    coverage_after_gap_fill = gap_fill_result.get("coverage_after") or coverage_after_sync

    return {
        "status": "ready" if industry_ready else "not_ready",
        "requested": {
            "exchanges": exchanges,
            "taxonomy_system": taxonomy_system,
            "taxonomy_version": taxonomy_version,
            "limit_per_exchange": limit_per_exchange,
            "missing_limit_per_exchange": missing_limit_per_exchange,
            "budget_mode": budget_mode,
            "allow_paid_proxy": allow_paid_proxy,
            "include_official_refresh": include_official_refresh,
            "force_component_refresh": force_component_refresh,
            "skip_sync": skip_sync,
            "skip_gap_fill": skip_gap_fill,
        },
        "coverage_before": coverage_before,
        "refresh": refresh_result,
        "sync": sync_result,
        "coverage_after_sync": coverage_after_sync,
        "gap_fill": gap_fill_result,
        "readiness": readiness,
        "summary": {
            "industry_standard_ready": industry_ready,
            "blockers": readiness.get("blockers", []),
            "missing_before_sync": coverage_before.get(
                "missing_authoritative_membership_count", 0
            ),
            "missing_after_sync": coverage_after_sync.get(
                "missing_authoritative_membership_count", 0
            ),
            "missing_after_gap_fill": coverage_after_gap_fill.get(
                "missing_authoritative_membership_count", 0
            ),
            "full_sync_memberships_written": sync_result.get(
                "total_memberships_written", 0
            ),
            "gap_fill_repaired_instrument_count": gap_fill_result.get(
                "repaired_instrument_count", 0
            ),
        },
    }


async def run_maintenance_with_lifecycle(
    manager: Any,
    *,
    exchanges: Optional[List[str]] = None,
    taxonomy_system: Optional[str] = None,
    taxonomy_version: Optional[str] = None,
    limit_per_exchange: Optional[int] = None,
    missing_limit_per_exchange: Optional[int] = None,
    budget_mode: Optional[str] = None,
    allow_paid_proxy: Optional[bool] = None,
    include_official_refresh: bool = False,
    force_component_refresh: bool = False,
    skip_sync: bool = False,
    skip_gap_fill: bool = False,
) -> Dict[str, Any]:
    """Initialize manager, run maintenance, and always close resources."""
    await initialize_manager_for_research_cli(manager)
    try:
        return await run_maintenance(
            manager,
            exchanges=exchanges,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            limit_per_exchange=limit_per_exchange,
            missing_limit_per_exchange=missing_limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            include_official_refresh=include_official_refresh,
            force_component_refresh=force_component_refresh,
            skip_sync=skip_sync,
            skip_gap_fill=skip_gap_fill,
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
        description="Run strict Shenwan full maintenance.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges, for example SSE,SZSE. Defaults to research_config markets.",
    )
    parser.add_argument("--taxonomy-system", help="Override taxonomy system, for example sw.")
    parser.add_argument("--taxonomy-version", help="Override taxonomy version.")
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange cap for the full sync phase. Omit for full-market sync.",
    )
    parser.add_argument(
        "--missing-limit-per-exchange",
        type=int,
        help="Optional cap on missing instrument ids repaired per exchange during gap fill.",
    )
    parser.add_argument("--budget-mode", help="Research source budget mode override.")
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument(
        "--allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_true",
        default=None,
        help="Allow paid proxy sources for this run.",
    )
    proxy_group.add_argument(
        "--no-allow-paid-proxy",
        dest="allow_paid_proxy",
        action="store_false",
        help="Disable paid proxy sources for this run.",
    )
    parser.add_argument(
        "--include-official-refresh",
        action="store_true",
        help="Also refresh audit-only official mapping cache before full sync.",
    )
    parser.add_argument(
        "--force-component-refresh",
        action="store_true",
        help="Bypass the cached Shenwan leaf-industry component sets and rebuild them from the upstream source.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip the full sync phase.",
    )
    parser.add_argument(
        "--skip-gap-fill",
        action="store_true",
        help="Skip the targeted gap-fill phase.",
    )
    parser.add_argument(
        "--fail-on-not-ready",
        action="store_true",
        help="Exit with code 2 when readiness is still not ready.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    result = await run_maintenance_with_lifecycle(
        data_manager,
        exchanges=parse_exchanges(args.exchanges),
        taxonomy_system=args.taxonomy_system,
        taxonomy_version=args.taxonomy_version,
        limit_per_exchange=args.limit_per_exchange,
        missing_limit_per_exchange=args.missing_limit_per_exchange,
        budget_mode=args.budget_mode,
        allow_paid_proxy=args.allow_paid_proxy,
        include_official_refresh=bool(args.include_official_refresh),
        force_component_refresh=bool(args.force_component_refresh),
        skip_sync=bool(args.skip_sync),
        skip_gap_fill=bool(args.skip_gap_fill),
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
