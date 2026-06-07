#!/usr/bin/env python
"""
Dry-run or backfill broker risk-control reports from CNInfo announcements.

Default mode is a no-write dry-run over 5 broker instruments and the past
12-quarter announcement window.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_manager import data_manager
from research.broker_risk_control import (
    BROKER_RISK_CONTROL_SOURCE_PROFILE,
    BrokerRiskControlReportSyncService,
    infer_broker_risk_control_report_period,
    is_broker_risk_control_instrument,
    is_broker_risk_control_title,
)
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanner,
)
from scripts.research_cli_support import (
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


_DEFAULT_MARKET_CONFIGS: Dict[str, Dict[str, Any]] = {
    "SSE": {"market": "SSE", "column": "sse", "plate": "sh"},
    "SZSE": {"market": "SZSE", "column": "szse", "plate": "sz"},
    "BSE": {"market": "BSE", "column": "neeq", "plate": "bj"},
}


def build_default_announcement_window(
    *,
    as_of_date: Optional[str] = None,
    quarters: int = 12,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, str]:
    """Build an inclusive announcement-date window for the last N quarters."""
    resolved_end = _parse_date(end_date or as_of_date) if (end_date or as_of_date) else date.today()
    if start_date:
        resolved_start = _parse_date(start_date)
    else:
        quarter_index = resolved_end.year * 4 + ((resolved_end.month - 1) // 3)
        start_index = quarter_index - max(1, int(quarters)) + 1
        start_year = start_index // 4
        start_quarter = start_index % 4
        resolved_start = date(start_year, start_quarter * 3 + 1, 1)
    return {"start_date": resolved_start.isoformat(), "end_date": resolved_end.isoformat()}


def build_candidate_report_periods(*, as_of_date: Optional[str] = None, quarters: int = 12) -> List[str]:
    """Build the latest completed quarter-end report periods."""
    as_of = _parse_date(as_of_date) if as_of_date else date.today()
    current_quarter = (as_of.month - 1) // 3 + 1
    completed_quarter = current_quarter - 1
    year = as_of.year
    if completed_quarter == 0:
        completed_quarter = 4
        year -= 1
    periods: List[str] = []
    cursor_year = year
    cursor_quarter = completed_quarter
    for _ in range(max(1, int(quarters))):
        periods.append(_quarter_end_period(cursor_year, cursor_quarter))
        cursor_quarter -= 1
        if cursor_quarter == 0:
            cursor_quarter = 4
            cursor_year -= 1
    return sorted(periods)


def select_broker_instruments(
    db_ops: Any,
    *,
    exchanges: Sequence[str],
    limit: int,
    instrument_ids: Optional[Sequence[str]] = None,
    storage: Optional[Any] = None,
    candidate_symbols: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Select broker instruments from local master data."""
    requested = {str(item).strip() for item in (instrument_ids or []) if str(item).strip()}
    symbol_scope = {str(item).strip() for item in (candidate_symbols or []) if str(item).strip()}
    selected: List[Dict[str, Any]] = []
    seen = set()
    for exchange in exchanges:
        rows = db_ops.get_research_target_instruments_by_exchange_sync(exchange)
        for row in rows:
            instrument_id = str(row.get("instrument_id") or "")
            if not instrument_id or instrument_id in seen:
                continue
            if requested and instrument_id not in requested:
                continue
            if symbol_scope and str(row.get("symbol") or "") not in symbol_scope:
                continue
            if not requested and not is_broker_risk_control_instrument(row):
                continue
            selected.append(row)
            seen.add(instrument_id)
            if limit and len(selected) >= limit:
                return selected
    if storage is not None:
        for row in _select_broker_instruments_from_industry_memberships(
            storage,
            exchanges=exchanges,
            limit=limit,
            requested=requested,
            symbol_scope=symbol_scope,
            seen=seen,
        ):
            selected.append(row)
            seen.add(str(row.get("instrument_id") or ""))
            if limit and len(selected) >= limit:
                return selected
    return selected


def _select_broker_instruments_from_industry_memberships(
    storage: Any,
    *,
    exchanges: Sequence[str],
    limit: int,
    requested: set[str],
    symbol_scope: set[str],
    seen: set[str],
) -> List[Dict[str, Any]]:
    if not hasattr(storage, "get_connection"):
        return []
    placeholders = ",".join("?" for _ in exchanges)
    params: List[Any] = list(exchanges)
    requested_clause = ""
    if requested:
        requested_clause = f"AND instrument_id IN ({','.join('?' for _ in requested)})"
        params.extend(sorted(requested))
    symbol_clause = ""
    if symbol_scope:
        symbol_clause = f"AND symbol IN ({','.join('?' for _ in symbol_scope)})"
        params.extend(sorted(symbol_scope))
    try:
        with storage.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT
                    instrument_id,
                    symbol,
                    exchange,
                    industry_name,
                    sw_l1_name,
                    sw_l2_name,
                    sw_l3_name
                FROM industry_memberships
                WHERE exchange IN ({placeholders})
                  AND (
                    industry_name LIKE '%证券%'
                    OR sw_l2_name LIKE '%证券%'
                    OR sw_l3_name LIKE '%证券%'
                  )
                  {requested_clause}
                  {symbol_clause}
                ORDER BY exchange, symbol
                """,
                params,
            ).fetchall()
    except Exception:
        return []
    result: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        instrument_id = str(item.get("instrument_id") or "")
        if not instrument_id or instrument_id in seen:
            continue
        result.append(
            {
                "instrument_id": instrument_id,
                "symbol": item.get("symbol"),
                "exchange": item.get("exchange"),
                "industry": item.get("industry_name"),
                "industry_name": item.get("industry_name"),
                "sw_l1_name": item.get("sw_l1_name"),
                "sw_l2_name": item.get("sw_l2_name"),
                "sw_l3_name": item.get("sw_l3_name"),
                "selection_source": "industry_memberships",
            }
        )
        if limit and len(result) >= limit:
            break
    return result


def scan_broker_risk_control_announcements(
    scanner: CninfoAnnouncementScanner,
    *,
    exchanges: Sequence[str],
    start_date: str,
    end_date: str,
    page_size: int,
    max_pages: int,
    market_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    title_patterns: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Scan CNInfo announcements for broker risk-control report candidates."""
    configs = {**_DEFAULT_MARKET_CONFIGS, **(market_configs or {})}
    selected: List[CninfoAnnouncementRecord] = []
    scan_results: List[Dict[str, Any]] = []
    for exchange in exchanges:
        market_cfg = configs.get(exchange)
        if not market_cfg:
            scan_results.append({"exchange": exchange, "status": "market_config_missing"})
            continue
        result = scanner.scan(
            CninfoAnnouncementScanConfig(
                purpose_key=BROKER_RISK_CONTROL_SOURCE_PROFILE,
                market=str(market_cfg.get("market") or exchange),
                column=str(market_cfg.get("column") or ""),
                plate=market_cfg.get("plate"),
                search_key="风险控制指标",
                start_date=start_date,
                end_date=end_date,
                page_size=page_size,
                max_pages=max_pages,
            ),
            filters=[
                lambda record: (
                    ["broker_risk_control_title"]
                    if is_broker_risk_control_title(record.title, title_patterns=title_patterns)
                    else []
                )
            ],
        )
        selected.extend(result.selected_records)
        scan_results.append(
            {
                "exchange": exchange,
                "market": result.config.market,
                "column": result.config.column,
                "pages_scanned": result.pages_scanned,
                "announcements_seen": result.announcements_seen,
                "selected_announcements": len(result.selected_records),
                "max_announcement_time": result.max_announcement_time,
                "errors": list(result.errors),
            }
        )
    return {"selected_records": selected, "scan_results": scan_results}


def run_broker_risk_control_backfill(
    *,
    db_ops: Any,
    storage: Any,
    exchanges: Sequence[str],
    as_of_date: Optional[str] = None,
    quarters: int = 12,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit_instruments: int = 5,
    instrument_ids: Optional[Sequence[str]] = None,
    write: bool = False,
    scan_only: bool = False,
    scanner: Optional[CninfoAnnouncementScanner] = None,
    payload_fetcher: Optional[Any] = None,
    page_size: int = 30,
    max_pages: int = 20,
    archive_root: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """Run a broker risk-control report dry-run/backfill and return JSON-ready data."""
    window = build_default_announcement_window(
        as_of_date=as_of_date,
        quarters=quarters,
        start_date=start_date,
        end_date=end_date,
    )
    periods = build_candidate_report_periods(
        as_of_date=end_date or as_of_date,
        quarters=quarters,
    )
    active_scanner = scanner or CninfoAnnouncementScanner()
    scan = scan_broker_risk_control_announcements(
        active_scanner,
        exchanges=exchanges,
        start_date=window["start_date"],
        end_date=window["end_date"],
        page_size=page_size,
        max_pages=max_pages,
    )
    candidate_symbols = _ordered_candidate_symbols(scan["selected_records"])
    selected_instruments = select_broker_instruments(
        db_ops,
        exchanges=exchanges,
        limit=limit_instruments,
        instrument_ids=instrument_ids,
        storage=storage,
        candidate_symbols=None if instrument_ids else candidate_symbols,
    )
    service_result: Dict[str, Any]
    if scan_only:
        service_result = {
            "status": "scan_only",
            "mode": "dry_run" if not write else "write_skipped_by_scan_only",
            "target_instruments": len(selected_instruments),
            "target_periods": len(periods),
            "reports_discovered": len(scan["selected_records"]),
            "reports_parsed": 0,
            "facts_parsed": 0,
            "facts_written": 0,
        }
    else:
        service = BrokerRiskControlReportSyncService(
            storage=storage,
            scanner=active_scanner,
            payload_fetcher=payload_fetcher,
            archive_root=archive_root,
        )
        service_result = service.backfill(
            instruments=selected_instruments,
            report_periods=periods,
            announcement_records=scan["selected_records"],
            tier="history",
            dry_run=not write,
        )
    return {
        "status": service_result.get("status"),
        "dry_run": not write,
        "scan_only": scan_only,
        "date_window": window,
        "report_periods": periods,
        "exchanges": list(exchanges),
        "target_instruments": [
            {
                "instrument_id": item.get("instrument_id"),
                "symbol": item.get("symbol"),
                "exchange": item.get("exchange"),
                "name": item.get("name") or item.get("short_name"),
                "industry": item.get("industry") or item.get("industry_name"),
                "selection_source": item.get("selection_source") or "instrument_master",
            }
            for item in selected_instruments
        ],
        "announcement_scan": {
            "selected_announcements": len(scan["selected_records"]),
            "scan_results": scan["scan_results"],
            "selected_preview": [
                {
                    "announcement_id": record.announcement_id,
                    "title": record.title,
                    "report_period": infer_broker_risk_control_report_period(record),
                    "announcement_time": record.announcement_time,
                    "market": record.market,
                    "column": record.column,
                    "symbols": list(record.symbols),
                    "adjunct_url": record.adjunct_url,
                }
                for record in scan["selected_records"][:30]
            ],
        },
        "backfill": service_result,
    }


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    if data_manager.research_storage is None:
        await initialize_manager_for_research_cli(data_manager)
    if data_manager.research_storage is None:
        raise RuntimeError("research storage is not initialized")
    exchanges = parse_exchanges(args.exchanges) or ["SSE", "SZSE", "BSE"]
    return run_broker_risk_control_backfill(
        db_ops=data_manager.db_ops,
        storage=data_manager.research_storage,
        exchanges=exchanges,
        as_of_date=args.as_of_date,
        quarters=args.quarters,
        start_date=args.start_date,
        end_date=args.end_date,
        limit_instruments=args.limit_instruments,
        instrument_ids=_parse_csv(args.instrument_ids),
        write=args.write,
        scan_only=args.scan_only,
        page_size=args.page_size,
        max_pages=args.max_pages,
        archive_root=args.archive_root,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill broker risk-control reports through the financial disclosure chain.",
    )
    parser.add_argument("--exchanges", default="SSE,SZSE,BSE")
    parser.add_argument("--as-of-date", default="")
    parser.add_argument("--quarters", type=int, default=12)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--limit-instruments", type=int, default=5)
    parser.add_argument("--instrument-ids", default="")
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--archive-root", default="data/filings/financial_statements/broker_risk_control")
    parser.add_argument("--scan-only", action="store_true")
    parser.add_argument("--write", action="store_true", help="Persist manifests, archived PDFs, and numeric facts.")
    parser.add_argument("--output", default="")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = asyncio.run(_run(args))
    payload = json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    print(payload)
    return 0 if result.get("status") in {"success", "partial", "scan_only"} else 1


def _parse_csv(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _parse_date(raw: str) -> date:
    return datetime.strptime(str(raw), "%Y-%m-%d").date()


def _quarter_end_period(year: int, quarter: int) -> str:
    suffix = {
        1: "03-31",
        2: "06-30",
        3: "09-30",
        4: "12-31",
    }[quarter]
    return f"{year}-{suffix}"


def _ordered_candidate_symbols(records: Iterable[CninfoAnnouncementRecord]) -> List[str]:
    symbols: List[str] = []
    for record in records:
        for symbol in record.symbols:
            clean = str(symbol).strip()
            if clean and clean not in symbols:
                symbols.append(clean)
    return symbols


if __name__ == "__main__":
    raise SystemExit(main())
