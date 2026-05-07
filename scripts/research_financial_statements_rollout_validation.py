#!/usr/bin/env python
"""Run financial statement backfill/readiness validation as a repository command."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sqlite3
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


def parse_official_sources(raw_values: Optional[List[str]]) -> List[str]:
    """Parse one or more comma-separated official source names."""
    if not raw_values:
        return []
    sources: List[str] = []
    for raw in raw_values:
        for part in str(raw).split(","):
            source = part.strip().lower()
            if source and source not in sources:
                sources.append(source)
    return sources


def normalize_report_periods(report_periods: List[str]) -> List[str]:
    """Normalize report-period aliases such as 2024Q1 to quarter-end dates."""
    normalized: List[str] = []
    for value in report_periods:
        text = str(value).strip()
        if len(text) == 6 and text[4].upper() == "Q":
            year = int(text[:4])
            quarter = int(text[5])
            month, day = {
                1: (3, 31),
                2: (6, 30),
                3: (9, 30),
                4: (12, 31),
            }[quarter]
            normalized.append(f"{year:04d}-{month:02d}-{day:02d}")
            continue
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            normalized.append(text[:10])
            continue
        normalized.append(text)
    return sorted(set(normalized))


def enable_official_source_config(
    research_config: Any,
    source_name: str,
) -> Dict[str, Any]:
    """Temporarily enable one official structured source in an in-memory config."""
    normalized_source = str(source_name or "").strip().lower()
    if not normalized_source:
        raise ValueError("Official source name is required")
    if normalized_source not in research_config.sources:
        raise ValueError(
            f"Missing research source config for official source: {normalized_source}"
        )

    module_cfg = research_config.modules.setdefault("financial_statements", {})
    official_cfg = module_cfg.setdefault("official_structured_sources", {})
    source_cfg = research_config.sources[normalized_source]
    source_financial_cfg = source_cfg.setdefault("financial_statements", {})

    source_enabled_before = source_cfg.get("enabled")
    source_financial_enabled_before = source_financial_cfg.get("enabled")
    official_enabled_before = official_cfg.get("enabled")

    source_cfg["enabled"] = True
    source_financial_cfg["enabled"] = True
    official_cfg["enabled"] = True

    official_candidate_states: List[Dict[str, Any]] = []
    for candidate in official_cfg.get("candidates", []):
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("source") or "").strip().lower() != normalized_source:
            continue
        state = {
            "source": normalized_source,
            "enabled_before": candidate.get("enabled"),
        }
        candidate["enabled"] = True
        state["enabled_after"] = candidate.get("enabled")
        official_candidate_states.append(state)

    endpoint_candidate_states: List[Dict[str, Any]] = []
    for candidate in source_financial_cfg.get("endpoint_candidates", []):
        if not isinstance(candidate, dict):
            continue
        state = {
            "key": str(candidate.get("key") or candidate.get("url") or ""),
            "kind": str(candidate.get("kind") or candidate.get("artifact_kind") or ""),
            "enabled_before": candidate.get("enabled"),
        }
        candidate["enabled"] = True
        state["enabled_after"] = candidate.get("enabled")
        endpoint_candidate_states.append(state)

    return {
        "source_enabled_before": source_enabled_before,
        "source_enabled_after": source_cfg.get("enabled"),
        "source_financial_statements_enabled_before": source_financial_enabled_before,
        "source_financial_statements_enabled_after": source_financial_cfg.get("enabled"),
        "official_structured_sources_enabled_before": official_enabled_before,
        "official_structured_sources_enabled_after": official_cfg.get("enabled"),
        "official_candidate_states": official_candidate_states,
        "endpoint_candidate_states": endpoint_candidate_states,
    }


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
        sqlite_instruments = _collect_target_instruments_from_quotes_db(
            manager,
            exchange=exchange,
            limit_per_exchange=limit_per_exchange,
        )
        if sqlite_instruments is not None:
            instrument_ids.extend(
                str(item["instrument_id"])
                for item in sqlite_instruments
                if item.get("instrument_id")
            )
            continue

        sync_research_getter = getattr(
            manager.db_ops,
            "get_research_target_instruments_by_exchange_sync",
            None,
        )
        async_research_getter = getattr(
            manager.db_ops,
            "get_research_target_instruments_by_exchange",
            None,
        )
        legacy_getter = manager.db_ops.get_instruments_by_exchange

        if sync_research_getter is not None:
            instruments = await asyncio.wait_for(
                asyncio.to_thread(sync_research_getter, exchange),
                timeout=lookup_timeout_seconds,
            )
        elif async_research_getter is not None and inspect.iscoroutinefunction(
            async_research_getter
        ):
            instruments = await asyncio.wait_for(
                async_research_getter(exchange),
                timeout=lookup_timeout_seconds,
            )
        elif inspect.iscoroutinefunction(legacy_getter):
            instruments = await asyncio.wait_for(
                legacy_getter(exchange),
                timeout=lookup_timeout_seconds,
            )
        else:
            instruments = await asyncio.wait_for(
                asyncio.to_thread(legacy_getter, exchange),
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


def _collect_target_instruments_from_quotes_db(
    manager: Any,
    *,
    exchange: str,
    limit_per_exchange: Optional[int],
) -> Optional[List[Dict[str, Any]]]:
    """Read a small stock sample from quotes DB without opening ORM sessions."""
    storage_cfg = getattr(getattr(manager, "research_config", None), "storage", None)
    quotes_db_path = getattr(storage_cfg, "quotes_db_path", None)
    if not quotes_db_path:
        return None

    db_path = Path(str(quotes_db_path))
    if not db_path.is_absolute():
        db_path = ROOT_DIR / db_path
    if not db_path.exists():
        return None

    sql = (
        "SELECT instrument_id, symbol, exchange, type, is_active "
        "FROM instruments "
        "WHERE exchange = ? AND type = 'stock' AND COALESCE(is_active, 1) = 1 "
        "ORDER BY symbol"
    )
    params: List[Any] = [exchange]
    if limit_per_exchange is not None:
        sql += " LIMIT ?"
        params.append(int(limit_per_exchange))

    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
    except sqlite3.Error:
        return None

    return [dict(row) for row in rows]


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
    enable_official_sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run small-sample financial statement sync and repository readiness."""
    module_cfg = manager.research_config.modules.setdefault("financial_statements", {})
    before_enabled = module_cfg.get("enabled")
    if enable_module:
        module_cfg["enabled"] = True

    official_source_overrides: Dict[str, Any] = {}
    for source_name in enable_official_sources or []:
        official_source_overrides[source_name] = enable_official_source_config(
            manager.research_config,
            source_name,
        )

    target_periods = (
        normalize_report_periods(report_periods)
        if report_periods
        else build_configured_report_periods(manager)
    )
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
            "enable_official_sources": enable_official_sources or [],
        },
        "runtime_overrides": {
            "financial_statements_enabled_before": before_enabled,
            "financial_statements_enabled_after": module_cfg.get("enabled"),
            "official_source_overrides": official_source_overrides,
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
        "--enable-official-source",
        action="append",
        dest="enable_official_sources",
        help=(
            "Temporarily enable one official structured source in memory, "
            "for example --enable-official-source sse. Can be repeated or comma-separated."
        ),
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
            enable_official_sources=parse_official_sources(
                args.enable_official_sources
            ),
        )
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return exit_code_for_result(result, fail_on_not_ready=args.fail_on_not_ready)


if __name__ == "__main__":
    raise SystemExit(main())
