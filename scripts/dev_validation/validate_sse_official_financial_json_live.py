#!/usr/bin/env python
"""Validate live official structured financial JSON ingestion.

The command uses an isolated temporary SQLite database and enables one
official source only in memory. It does not mutate production config or
production research data.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_statements_sync import (  # noqa: E402
    FinancialStatementsShadowSyncService,
)
from research.official_financial_source_profiles import (  # noqa: E402
    default_official_source_for_exchange,
    parser_profile_for,
    source_profile_for,
    source_profile_metadata,
)
from research.storage import ResearchStorageManager  # noqa: E402
from scripts.research_cli_support import json_ready  # noqa: E402
from scripts.research_financial_statements_rollout_validation import (  # noqa: E402
    enable_official_source_config,
    normalize_report_periods,
)
from utils.config_manager import config_manager  # noqa: E402


DEFAULT_REQUIRED_CORE_FACTS = [
    "revenue",
    "net_income",
    "total_assets",
    "total_liabilities",
    "equity",
    "operating_cf",
]


class _InstrumentListDbOps:
    """Minimal db_ops adapter for isolated provider/sync validation."""

    def __init__(self, instruments: List[Dict[str, Any]]):
        self.instruments = instruments

    async def get_instruments_by_exchange(self, exchange: str) -> List[Dict[str, Any]]:
        return [
            instrument
            for instrument in self.instruments
            if str(instrument.get("exchange") or "").upper() == exchange.upper()
        ]


def _default_db_path() -> Path:
    return Path("/tmp") / f"quote_official_financial_json_live_{os.getpid()}.db"


def _instrument_from_id(instrument_id: str, exchange: str) -> Dict[str, Any]:
    symbol = str(instrument_id).split(".")[0]
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "type": "stock",
        "is_active": True,
    }


def _parse_instrument_ids(raw: str) -> List[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def _parse_report_periods(raw: str) -> List[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def _prepare_config(
    db_path: Path,
    *,
    official_source: str = "sse",
    request_timeout_seconds: Optional[float] = None,
    request_interval_seconds: Optional[float] = None,
) -> Any:
    research_config = copy.deepcopy(config_manager.get_research_config())
    research_config.storage.db_path = str(db_path)
    research_config.storage.attach_quotes_db = False
    research_config.storage.quotes_db_path = ""
    research_config.storage.financials_db_path = str(db_path)

    module_cfg = research_config.modules.setdefault("financial_statements", {})
    module_cfg["enabled"] = True
    enable_official_source_config(research_config, official_source)
    source_financial_cfg = research_config.sources[official_source]["financial_statements"]
    if request_timeout_seconds is not None:
        source_financial_cfg["request_timeout_seconds"] = float(request_timeout_seconds)
    if request_interval_seconds is not None:
        source_financial_cfg["request_interval_seconds"] = float(request_interval_seconds)

    research_config.routing["financial_statements"] = {
        "free_chain": [{"source": official_source, "mode": "direct"}],
        "paid_chain": [],
        "fallback_chain": [],
    }
    return research_config


async def run_validation(
    *,
    instrument_ids: List[str],
    exchange: str,
    report_period: Optional[str] = None,
    report_periods: Optional[List[str]] = None,
    db_path: Path,
    official_source: str = "sse",
    required_core_facts: Optional[List[str]] = None,
    request_timeout_seconds: Optional[float] = None,
    request_interval_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """Run live official download, parse, and isolated DB write."""
    required = required_core_facts or DEFAULT_REQUIRED_CORE_FACTS
    if not instrument_ids:
        raise ValueError("At least one instrument id is required")
    resolved_periods = list(report_periods or [])
    if not resolved_periods and report_period:
        resolved_periods = [report_period]
    if not resolved_periods:
        raise ValueError("At least one report period is required")
    normalized_periods = normalize_report_periods(resolved_periods)
    research_config = _prepare_config(
        db_path,
        official_source=official_source,
        request_timeout_seconds=request_timeout_seconds,
        request_interval_seconds=request_interval_seconds,
    )
    parser_cfg = research_config.modules.get("financial_statements", {}).get(
        "parser",
        {},
    )
    source_profile = source_profile_for(exchange, official_source, strict=True)
    structured_parser_profile = parser_profile_for(
        exchange,
        official_source,
        fallback=str(
            parser_cfg.get(
                "structured_json_fact_parser",
                "structured_financial_json.v1",
            )
        ),
    )
    reported_structured_json_parser = (
        str(
            parser_cfg.get(
                "structured_json_fact_parser",
                structured_parser_profile,
            )
        )
        if source_profile == "sse_commonquery"
        else structured_parser_profile
    )
    source_financial_cfg = (
        research_config.sources.get(official_source, {}).get("financial_statements", {})
    )
    request_policy = {
        "source": official_source,
        "source_profile": source_profile,
        "parser_profile": structured_parser_profile,
        "request_timeout_seconds": (
            request_timeout_seconds
            if request_timeout_seconds is not None
            else source_financial_cfg.get("request_timeout_seconds")
        ),
        "request_interval_seconds": (
            request_interval_seconds
            if request_interval_seconds is not None
            else source_financial_cfg.get("request_interval_seconds")
        ),
        "retry_attempts": source_financial_cfg.get("retry_attempts"),
        "retry_backoff_seconds": source_financial_cfg.get("retry_backoff_seconds"),
        "concurrency_assumption": "single_process_sequential",
    }
    instruments = [
        _instrument_from_id(instrument_id, exchange)
        for instrument_id in instrument_ids
    ]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_InstrumentListDbOps(instruments),
        storage=storage,
        research_config=research_config,
    )
    started_at = time.perf_counter()
    period_sync_results: List[Dict[str, Any]] = []
    for period_input, normalized_period in zip(resolved_periods, normalized_periods):
        period_started_at = time.perf_counter()
        period_sync = await service.sync(
            exchanges=[exchange],
            limit_per_exchange=len(instruments),
            budget_mode="free_only",
            allow_paid_proxy=False,
            report_periods=[period_input],
            sync_mode="backfill",
            force_full=True,
        )
        period_sync_results.append(
            {
                "input_report_period": period_input,
                "report_period": normalized_period,
                "elapsed_seconds": round(time.perf_counter() - period_started_at, 3),
                "sync": period_sync,
            }
        )
    elapsed_seconds = time.perf_counter() - started_at
    instrument_results = [
        _validate_instrument_core_facts(
            storage=storage,
            instrument_id=instrument_id,
            report_period=normalized_period,
            required_core_facts=required,
        )
        for instrument_id in instrument_ids
        for normalized_period in normalized_periods
    ]
    failed_instruments = [
        item for item in instrument_results if item["status"] != "passed"
    ]
    failed_instrument_periods = [
        {
            "instrument_id": item["instrument_id"],
            "report_period": item["report_period"],
            "blockers": item["blockers"],
        }
        for item in failed_instruments
    ]
    first_result = instrument_results[0]
    total_numeric_facts_written = sum(
        int(item["sync"].get("total_numeric_facts_written") or 0)
        for item in period_sync_results
    )
    total_core_facts_written = sum(
        int(item["sync"].get("total_core_facts_written") or 0)
        for item in period_sync_results
    )
    total_source_manifests_written = sum(
        int(item["sync"].get("total_source_manifests_written") or 0)
        for item in period_sync_results
    )

    blockers: List[str] = []
    if failed_instruments:
        blockers.append("instrument_core_fact_validation_failed")
    if total_numeric_facts_written <= 0:
        blockers.append("no_numeric_facts_written")
    if total_core_facts_written <= 0:
        blockers.append("no_core_facts_written")
    for item in period_sync_results:
        if int(item["sync"].get("total_numeric_facts_written") or 0) <= 0:
            blockers.append(f"no_numeric_facts_written:{item['report_period']}")
        if int(item["sync"].get("total_core_facts_written") or 0) <= 0:
            blockers.append(f"no_core_facts_written:{item['report_period']}")

    return {
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "db_path": str(db_path),
        "instrument_id": instrument_ids[0] if len(instrument_ids) == 1 else None,
        "instrument_ids": instrument_ids,
        "instrument_count": len(instrument_ids),
        "exchange": exchange,
        "source": official_source,
        "source_mode": "direct",
        "official_source": official_source,
        "source_profile": source_profile,
        "source_profile_metadata": source_profile_metadata(
            exchange,
            official_source,
            strict=False,
        ),
        "parser_version": parser_cfg.get(
            "parser_version",
            "financial_structured_filing.v1",
        ),
        "numeric_fact_parser": parser_cfg.get(
            "numeric_fact_parser",
            "xbrl_numeric_facts.v1",
        ),
        "structured_json_fact_parser": reported_structured_json_parser,
        "parser_profile": structured_parser_profile,
        "alias_mapping_version": parser_cfg.get(
            "alias_mapping_version",
            "core_financial_facts.v1",
        ),
        "report_period": normalized_periods[0] if len(normalized_periods) == 1 else None,
        "report_periods": normalized_periods,
        "instrument_period_count": len(instrument_ids) * len(normalized_periods),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "request_timeout_seconds": request_timeout_seconds,
        "request_interval_seconds": request_interval_seconds,
        "request_policy": request_policy,
        "throughput_instruments_per_minute": round(
            (len(instrument_ids) / elapsed_seconds * 60.0)
            if elapsed_seconds > 0
            else 0.0,
            3,
        ),
        "throughput_instrument_periods_per_minute": round(
            (len(instrument_ids) * len(normalized_periods) / elapsed_seconds * 60.0)
            if elapsed_seconds > 0
            else 0.0,
            3,
        ),
        "required_core_facts": required,
        "present_required_core_facts": first_result["present_required_core_facts"],
        "missing_required_core_facts": first_result["missing_required_core_facts"],
        "sample_core_facts": first_result["sample_core_facts"],
        "core_row_count": sum(item["core_row_count"] for item in instrument_results),
        "failed_instrument_periods": failed_instrument_periods,
        "instrument_results": instrument_results,
        "period_results": [
            {
                "input_report_period": item["input_report_period"],
                "report_period": item["report_period"],
                "elapsed_seconds": item["elapsed_seconds"],
                "sync": item["sync"],
                "failed_instrument_periods": [
                    failed
                    for failed in failed_instrument_periods
                    if failed["report_period"] == item["report_period"]
                ],
            }
            for item in period_sync_results
        ],
        "sync": {
            "status": "success" if not blockers else "degraded",
            "total_source_manifests_written": total_source_manifests_written,
            "total_numeric_facts_written": total_numeric_facts_written,
            "total_core_facts_written": total_core_facts_written,
            "successful_exchanges": sum(
                int(item["sync"].get("successful_exchanges") or 0)
                for item in period_sync_results
            ),
            "attempted_exchanges": sum(
                int(item["sync"].get("attempted_exchanges") or 0)
                for item in period_sync_results
            ),
        },
    }


def _validate_instrument_core_facts(
    *,
    storage: ResearchStorageManager,
    instrument_id: str,
    report_period: str,
    required_core_facts: List[str],
) -> Dict[str, Any]:
    core_rows = storage.financial_statements.get_core_facts(
        instrument_id,
        include_history=True,
        report_period=report_period,
        limit=1,
    )
    core_facts = (
        _extract_core_fact_values(core_rows[0], required_core_facts)
        if core_rows
        else {}
    )
    present_required = [
        field for field in required_core_facts if core_facts.get(field) is not None
    ]
    missing_required = [
        field for field in required_core_facts if field not in present_required
    ]

    blockers: List[str] = []
    if not core_rows:
        blockers.append("missing_core_fact_row")
    if missing_required:
        blockers.append("missing_required_core_facts")

    return {
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "instrument_id": instrument_id,
        "report_period": report_period,
        "required_core_facts": required_core_facts,
        "present_required_core_facts": present_required,
        "missing_required_core_facts": missing_required,
        "sample_core_facts": {
            field: core_facts.get(field)
            for field in required_core_facts
            if core_facts.get(field) is not None
        },
        "core_row_count": len(core_rows),
    }


def _extract_core_fact_values(
    row: Dict[str, Any],
    required_core_facts: List[str],
) -> Dict[str, Any]:
    values = dict(row.get("facts") or {})
    for field in required_core_facts:
        if row.get(field) is not None:
            values[field] = row.get(field)
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate live official financial structured JSON ingestion.",
    )
    parser.add_argument("--instrument-id", default="600000.SH")
    parser.add_argument(
        "--instrument-ids",
        help="Comma-separated instrument ids. Overrides --instrument-id.",
    )
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument(
        "--official-source",
        help="Official source to enable in memory. Defaults to sse for SSE, cninfo for SZSE/BSE.",
    )
    parser.add_argument("--report-period", default="2023Q4")
    parser.add_argument(
        "--report-periods",
        help="Comma-separated report periods. Overrides --report-period.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=_default_db_path(),
        help="Isolated SQLite DB path. Defaults to a /tmp path with the current pid.",
    )
    parser.add_argument(
        "--required-core-facts",
        help="Comma-separated core facts required for pass/fail.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        help="Override SSE official request timeout in the in-memory validation config.",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        help="Override SSE official request interval in the in-memory validation config.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    required = (
        [part.strip() for part in args.required_core_facts.split(",") if part.strip()]
        if args.required_core_facts
        else None
    )
    result = asyncio.run(
        run_validation(
            instrument_ids=(
                _parse_instrument_ids(args.instrument_ids)
                if args.instrument_ids
                else [args.instrument_id]
            ),
            exchange=args.exchange,
            report_period=args.report_period if not args.report_periods else None,
            report_periods=(
                _parse_report_periods(args.report_periods)
                if args.report_periods
                else None
            ),
            db_path=args.db_path,
            official_source=args.official_source
            or default_official_source_for_exchange(args.exchange),
            required_core_facts=required,
            request_timeout_seconds=args.request_timeout_seconds,
            request_interval_seconds=args.request_interval_seconds,
        )
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
