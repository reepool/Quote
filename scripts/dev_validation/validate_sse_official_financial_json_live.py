#!/usr/bin/env python
"""Validate live SSE official commonQuery financial JSON ingestion.

The command uses an isolated temporary SQLite database and enables the SSE
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
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_statements_sync import (  # noqa: E402
    FinancialStatementsShadowSyncService,
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


class _SingleInstrumentDbOps:
    """Minimal db_ops adapter for isolated provider/sync validation."""

    def __init__(self, instrument: Dict[str, Any]):
        self.instrument = instrument

    async def get_instruments_by_exchange(self, exchange: str) -> List[Dict[str, Any]]:
        if str(self.instrument.get("exchange") or "").upper() != exchange.upper():
            return []
        return [self.instrument]


def _default_db_path() -> Path:
    return Path("/tmp") / f"quote_sse_financial_json_live_{os.getpid()}.db"


def _instrument_from_id(instrument_id: str, exchange: str) -> Dict[str, Any]:
    symbol = str(instrument_id).split(".")[0]
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
        "type": "stock",
        "is_active": True,
    }


def _prepare_config(db_path: Path) -> Any:
    research_config = copy.deepcopy(config_manager.get_research_config())
    research_config.storage.db_path = str(db_path)
    research_config.storage.attach_quotes_db = False
    research_config.storage.quotes_db_path = ""
    research_config.storage.financials_db_path = str(db_path)

    module_cfg = research_config.modules.setdefault("financial_statements", {})
    module_cfg["enabled"] = True
    enable_official_source_config(research_config, "sse")

    research_config.routing["financial_statements"] = {
        "free_chain": [{"source": "sse", "mode": "direct"}],
        "paid_chain": [],
        "fallback_chain": [],
    }
    return research_config


async def run_validation(
    *,
    instrument_id: str,
    exchange: str,
    report_period: str,
    db_path: Path,
    required_core_facts: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run one live SSE official download, parse, and isolated DB write."""
    required = required_core_facts or DEFAULT_REQUIRED_CORE_FACTS
    normalized_period = normalize_report_periods([report_period])[0]
    research_config = _prepare_config(db_path)
    instrument = _instrument_from_id(instrument_id, exchange)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = FinancialStatementsShadowSyncService(
        db_ops=_SingleInstrumentDbOps(instrument),
        storage=storage,
        research_config=research_config,
    )
    sync_result = await service.sync(
        exchanges=[exchange],
        limit_per_exchange=1,
        budget_mode="free_only",
        allow_paid_proxy=False,
        report_periods=[report_period],
        sync_mode="backfill",
        force_full=True,
    )
    core_rows = storage.financial_statements.get_core_facts(
        instrument_id,
        include_history=True,
        report_period=normalized_period,
        limit=1,
    )
    core_facts = _extract_core_fact_values(core_rows[0], required) if core_rows else {}
    present_required = [
        field for field in required if core_facts.get(field) is not None
    ]
    missing_required = [field for field in required if field not in present_required]

    blockers: List[str] = []
    if not core_rows:
        blockers.append("missing_core_fact_row")
    if int(sync_result.get("total_numeric_facts_written") or 0) <= 0:
        blockers.append("no_numeric_facts_written")
    if int(sync_result.get("total_core_facts_written") or 0) <= 0:
        blockers.append("no_core_facts_written")
    if missing_required:
        blockers.append("missing_required_core_facts")

    return {
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "db_path": str(db_path),
        "instrument_id": instrument_id,
        "exchange": exchange,
        "report_period": normalized_period,
        "required_core_facts": required,
        "present_required_core_facts": present_required,
        "missing_required_core_facts": missing_required,
        "sample_core_facts": {
            field: core_facts.get(field)
            for field in required
            if core_facts.get(field) is not None
        },
        "core_row_count": len(core_rows),
        "sync": sync_result,
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
        description="Validate live SSE official financial structured JSON ingestion.",
    )
    parser.add_argument("--instrument-id", default="600000.SH")
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument("--report-period", default="2023Q4")
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
            instrument_id=args.instrument_id,
            exchange=args.exchange,
            report_period=args.report_period,
            db_path=args.db_path,
            required_core_facts=required,
        )
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
