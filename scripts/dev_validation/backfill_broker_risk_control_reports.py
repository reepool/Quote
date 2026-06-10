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
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_manager import data_manager
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BROKER_RISK_CONTROL_SOURCE_PROFILE,
    BrokerRiskControlReportSyncService,
    infer_broker_annual_report_period,
    infer_broker_risk_control_report_period,
    is_formal_broker_annual_or_semiannual_report_title,
    is_broker_risk_control_instrument,
    is_broker_risk_control_title,
)
from research.listed_broker_dealer_scope import (
    enrich_instrument_with_broker_scope,
    resolve_listed_broker_dealer_scope,
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
_CNINFO_TOP_SEARCH_URL = "https://www.cninfo.com.cn/new/information/topSearch/query"
_CNINFO_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.cninfo.com.cn",
    "Referer": "https://www.cninfo.com.cn/new/disclosure/stock",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}
LOGGER = logging.getLogger(__name__)


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


def build_candidate_report_periods(
    *,
    as_of_date: Optional[str] = None,
    quarters: int = 12,
    report_period_types: Optional[Sequence[str]] = None,
) -> List[str]:
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
    allowed = {str(item).strip().lower() for item in (report_period_types or []) if str(item).strip()}
    if allowed:
        suffix_by_type = {
            "q1": "03-31",
            "quarterly": "",
            "semiannual": "06-30",
            "halfyear": "06-30",
            "annual": "12-31",
            "fy": "12-31",
            "q3": "09-30",
        }
        selected_suffixes = {
            suffix
            for key, suffix in suffix_by_type.items()
            if key in allowed and suffix
        }
        include_all_quarters = "quarterly" in allowed or "all" in allowed
        if not include_all_quarters:
            periods = [period for period in periods if period[-5:] in selected_suffixes]
    return sorted(periods)


def select_broker_instruments(
    db_ops: Any,
    *,
    exchanges: Sequence[str],
    limit: int,
    instrument_ids: Optional[Sequence[str]] = None,
    storage: Optional[Any] = None,
    candidate_symbols: Optional[Sequence[str]] = None,
    require_confirmed_scope: bool = True,
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
            enriched = enrich_instrument_with_broker_scope(row)
            if require_confirmed_scope and not _broker_scope_eligible(enriched):
                continue
            selected.append(enriched)
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
            enriched = enrich_instrument_with_broker_scope(row)
            if require_confirmed_scope and not _broker_scope_eligible(enriched):
                continue
            selected.append(enriched)
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
    instruments: Optional[Sequence[Dict[str, Any]]] = None,
    start_date: str,
    end_date: str,
    page_size: int,
    max_pages: int,
    per_instrument_scan: bool = True,
    per_instrument_page_size: int = 30,
    per_instrument_max_pages: int = 6,
    market_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    title_patterns: Optional[Sequence[str]] = None,
    source_profile: str = BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
) -> Dict[str, Any]:
    """Scan CNInfo announcements for broker regulatory report candidates."""
    configs = {**_DEFAULT_MARKET_CONFIGS, **(market_configs or {})}
    selected: List[CninfoAnnouncementRecord] = []
    seen_announcement_ids: set[str] = set()
    market_scan_results: List[Dict[str, Any]] = []
    per_instrument_results: List[Dict[str, Any]] = []

    def _append_selected(records: Sequence[CninfoAnnouncementRecord]) -> int:
        added = 0
        for record in records:
            key = str(record.announcement_id or "").strip()
            if key and key in seen_announcement_ids:
                continue
            if key:
                seen_announcement_ids.add(key)
            selected.append(record)
            added += 1
        return added

    def _filters() -> List[Any]:
        if source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return [
                lambda record: (
                    ["formal_annual_or_semiannual_report"]
                    if is_formal_broker_annual_or_semiannual_report_title(record.title)
                    else []
                )
            ]
        return [
            lambda record: (
                ["broker_risk_control_title"]
                    if is_broker_risk_control_title(record.title, title_patterns=title_patterns)
                    else []
            )
        ]

    search_key = (
        "年度报告"
        if source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
        else "风险控制指标"
    )
    skip_market_scan = (
        source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
        and bool(instruments)
    )
    for exchange in exchanges:
        market_cfg = configs.get(exchange)
        if not market_cfg:
            LOGGER.warning("broker risk-control market scan skipped: exchange=%s reason=market_config_missing", exchange)
            market_scan_results.append({"exchange": exchange, "status": "market_config_missing"})
            continue
        if skip_market_scan:
            LOGGER.info(
                "broker risk-control market scan skipped: exchange=%s reason=instrument_scoped_formal_report_source",
                exchange,
            )
            market_scan_results.append(
                {
                    "exchange": exchange,
                    "status": "skipped_for_instrument_scoped_formal_report_source",
                    "reason": "formal annual/semiannual reports must be scanned by confirmed broker instrument",
                }
            )
            continue
        LOGGER.info(
            "broker risk-control market scan start: exchange=%s source_profile=%s window=%s..%s search_key=%s",
            exchange,
            source_profile,
            start_date,
            end_date,
            search_key,
        )
        result = scanner.scan(
            CninfoAnnouncementScanConfig(
                purpose_key=source_profile,
                market=str(market_cfg.get("market") or exchange),
                column=str(market_cfg.get("column") or ""),
                plate=market_cfg.get("plate"),
                search_key=search_key,
                start_date=start_date,
                end_date=end_date,
                page_size=page_size,
                max_pages=max_pages,
            ),
            filters=_filters(),
        )
        added = _append_selected(result.selected_records)
        LOGGER.info(
            "broker risk-control market scan done: exchange=%s pages=%s seen=%s selected=%s added=%s errors=%s",
            exchange,
            result.pages_scanned,
            result.announcements_seen,
            len(result.selected_records),
            added,
            len(result.errors),
        )
        market_scan_results.append(
            {
                "exchange": exchange,
                "market": result.config.market,
                "column": result.config.column,
                "pages_scanned": result.pages_scanned,
                "announcements_seen": result.announcements_seen,
                "selected_announcements": len(result.selected_records),
                "selected_announcements_added": added,
                "max_announcement_time": result.max_announcement_time,
                "errors": list(result.errors),
            }
        )
    attempted = 0
    instruments_with_matches = 0
    if per_instrument_scan:
        for instrument in instruments or []:
            exchange = str(instrument.get("exchange") or "").strip()
            if exchange not in exchanges:
                continue
            market_cfg = configs.get(exchange)
            if not market_cfg:
                LOGGER.warning(
                    "broker risk-control instrument scan skipped: instrument_id=%s symbol=%s exchange=%s reason=market_config_missing",
                    instrument.get("instrument_id"),
                    instrument.get("symbol"),
                    exchange,
                )
                per_instrument_results.append(
                    {
                        "instrument_id": instrument.get("instrument_id"),
                        "symbol": instrument.get("symbol"),
                        "exchange": exchange,
                        "status": "market_config_missing",
                    }
                )
                continue
            stock_param = _cninfo_stock_param(instrument)
            if not stock_param:
                LOGGER.warning(
                    "broker risk-control instrument scan skipped: instrument_id=%s symbol=%s exchange=%s reason=missing_stock_param",
                    instrument.get("instrument_id"),
                    instrument.get("symbol"),
                    exchange,
                )
                per_instrument_results.append(
                    {
                        "instrument_id": instrument.get("instrument_id"),
                        "symbol": instrument.get("symbol"),
                        "exchange": exchange,
                        "status": "missing_stock_param",
                    }
                )
                continue
            attempted += 1
            LOGGER.info(
                "broker risk-control instrument scan start: instrument_id=%s symbol=%s exchange=%s source_profile=%s stock=%s org_id=%s window=%s..%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                exchange,
                source_profile,
                stock_param,
                _cninfo_org_id(instrument),
                start_date,
                end_date,
            )
            result = scanner.scan(
                CninfoAnnouncementScanConfig(
                    purpose_key=source_profile,
                    market=str(market_cfg.get("market") or exchange),
                    column=str(market_cfg.get("column") or ""),
                    plate=market_cfg.get("plate"),
                    search_key=search_key,
                    stock=stock_param,
                    org_id=_cninfo_org_id(instrument),
                    start_date=start_date,
                    end_date=end_date,
                    page_size=per_instrument_page_size,
                    max_pages=per_instrument_max_pages,
                ),
                filters=_filters(),
            )
            added = _append_selected(result.selected_records)
            if result.selected_records:
                instruments_with_matches += 1
            LOGGER.info(
                "broker risk-control instrument scan done: instrument_id=%s symbol=%s exchange=%s pages=%s seen=%s selected=%s added=%s errors=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                exchange,
                result.pages_scanned,
                result.announcements_seen,
                len(result.selected_records),
                added,
                len(result.errors),
            )
            per_instrument_results.append(
                {
                    "instrument_id": instrument.get("instrument_id"),
                    "symbol": instrument.get("symbol"),
                    "exchange": exchange,
                    "stock_param": stock_param,
                    "pages_scanned": result.pages_scanned,
                    "announcements_seen": result.announcements_seen,
                    "selected_announcements": len(result.selected_records),
                    "selected_announcements_added": added,
                    "max_announcement_time": result.max_announcement_time,
                    "errors": list(result.errors),
                }
            )
    return {
        "selected_records": selected,
        "scan_results": market_scan_results,
        "market_scan_results": market_scan_results,
        "per_instrument_scan": {
            "enabled": per_instrument_scan,
            "attempted_instruments": attempted,
            "instruments_with_matches": instruments_with_matches,
            "selected_announcements_added": sum(
                int(item.get("selected_announcements_added") or 0)
                for item in per_instrument_results
            ),
            "results": per_instrument_results,
        },
    }


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
    per_instrument_scan: bool = True,
    per_instrument_page_size: int = 30,
    per_instrument_max_pages: int = 6,
    report_period_types: Optional[Sequence[str]] = None,
    source_profile: str = BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    include_standalone_supplement: bool = False,
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
        report_period_types=report_period_types or ("annual", "semiannual"),
    )
    LOGGER.info(
        "broker risk-control backfill start: mode=%s source_profile=%s exchanges=%s window=%s..%s periods=%s limit_instruments=%s instrument_ids=%s scan_only=%s",
        "write" if write else "dry_run",
        source_profile,
        ",".join(exchanges),
        window["start_date"],
        window["end_date"],
        len(periods),
        limit_instruments,
        ",".join(instrument_ids or []),
        scan_only,
    )
    active_scanner = scanner or CninfoAnnouncementScanner()
    selected_instruments = select_broker_instruments(
        db_ops,
        exchanges=exchanges,
        limit=limit_instruments,
        instrument_ids=instrument_ids,
        storage=storage,
        require_confirmed_scope=True,
    )
    LOGGER.info(
        "broker risk-control instruments selected: count=%s symbols=%s",
        len(selected_instruments),
        ",".join(str(item.get("symbol") or "") for item in selected_instruments[:20]),
    )
    selected_instruments, org_resolution = enrich_cninfo_stock_params(
        active_scanner,
        selected_instruments,
    )
    LOGGER.info(
        "broker risk-control cninfo org resolution done: attempted=%s resolved=%s errors=%s",
        org_resolution.get("attempted"),
        org_resolution.get("resolved"),
        len(org_resolution.get("errors") or []),
    )
    scan = scan_broker_risk_control_announcements(
        active_scanner,
        exchanges=exchanges,
        instruments=selected_instruments,
        start_date=window["start_date"],
        end_date=window["end_date"],
        page_size=page_size,
        max_pages=max_pages,
        per_instrument_scan=per_instrument_scan,
        per_instrument_page_size=per_instrument_page_size,
        per_instrument_max_pages=per_instrument_max_pages,
        source_profile=source_profile,
    )
    LOGGER.info(
        "broker risk-control announcement scan done: selected_announcements=%s per_instrument_attempted=%s matched_instruments=%s",
        len(scan["selected_records"]),
        (scan.get("per_instrument_scan") or {}).get("attempted_instruments"),
        (scan.get("per_instrument_scan") or {}).get("instruments_with_matches"),
    )
    standalone_scan: Optional[Dict[str, Any]] = None
    if include_standalone_supplement:
        standalone_scan = scan_broker_risk_control_announcements(
            active_scanner,
            exchanges=exchanges,
            instruments=selected_instruments,
            start_date=window["start_date"],
            end_date=window["end_date"],
            page_size=page_size,
            max_pages=max_pages,
            per_instrument_scan=per_instrument_scan,
            per_instrument_page_size=per_instrument_page_size,
            per_instrument_max_pages=per_instrument_max_pages,
            source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
        )
    service_result: Dict[str, Any]
    if scan_only:
        LOGGER.info(
            "broker risk-control scan-only complete: target_instruments=%s reports_discovered=%s",
            len(selected_instruments),
            len(scan["selected_records"]),
        )
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
        LOGGER.info(
            "broker risk-control parse stage start: reports=%s dry_run=%s tier=history",
            len(scan["selected_records"]),
            not write,
        )
        service = BrokerRiskControlReportSyncService(
            storage=storage,
            scanner=active_scanner,
            payload_fetcher=payload_fetcher,
            archive_root=archive_root,
            source_profile=source_profile,
        )
        service_result = service.backfill(
            instruments=selected_instruments,
            report_periods=periods,
            announcement_records=scan["selected_records"],
            tier="history",
            dry_run=not write,
        )
        LOGGER.info(
            "broker risk-control parse stage done: status=%s reports_parsed=%s facts_parsed=%s facts_written=%s parse_failures=%s retryable_pending=%s",
            service_result.get("status"),
            service_result.get("reports_parsed"),
            service_result.get("facts_parsed"),
            service_result.get("facts_written"),
            service_result.get("parse_failures"),
            service_result.get("retryable_pending_reports"),
        )
        if standalone_scan is not None:
            LOGGER.info(
                "broker risk-control standalone supplement parse start: reports=%s dry_run=%s tier=history",
                len(standalone_scan["selected_records"]),
                not write,
            )
            supplement_service = BrokerRiskControlReportSyncService(
                storage=storage,
                scanner=active_scanner,
                payload_fetcher=payload_fetcher,
                archive_root=archive_root,
                source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
            )
            supplement_result = supplement_service.backfill(
                instruments=selected_instruments,
                report_periods=periods,
                announcement_records=standalone_scan["selected_records"],
                tier="history",
                dry_run=not write,
            )
            LOGGER.info(
                "broker risk-control standalone supplement parse done: status=%s reports_parsed=%s facts_parsed=%s facts_written=%s parse_failures=%s retryable_pending=%s",
                supplement_result.get("status"),
                supplement_result.get("reports_parsed"),
                supplement_result.get("facts_parsed"),
                supplement_result.get("facts_written"),
                supplement_result.get("parse_failures"),
                supplement_result.get("retryable_pending_reports"),
            )
            service_result["supplementary_standalone"] = supplement_result
            for key in (
                "reports_discovered",
                "reports_parsed",
                "facts_parsed",
                "facts_written",
                "unchanged_reports",
                "parse_failures",
                "retryable_pending_reports",
            ):
                service_result[key] = int(service_result.get(key) or 0) + int(
                    supplement_result.get(key) or 0
                )
            if supplement_result.get("status") == "partial":
                service_result["status"] = "partial"
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
                "cninfo_org_id": item.get("cninfo_org_id"),
                "listed_broker_dealer_scope": item.get("listed_broker_dealer_scope"),
            }
            for item in selected_instruments
        ],
        "announcement_scan": {
            "cninfo_org_id_resolution": org_resolution,
            "selected_announcements": len(scan["selected_records"]),
            "scan_results": scan["scan_results"],
            "market_scan_results": scan.get("market_scan_results", scan["scan_results"]),
            "per_instrument_scan": scan.get("per_instrument_scan"),
            "selected_preview": [
                {
                    "announcement_id": record.announcement_id,
                    "title": record.title,
                    "report_period": (
                        infer_broker_annual_report_period(record)
                        if source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                        and is_formal_broker_annual_or_semiannual_report_title(record.title)
                        else infer_broker_risk_control_report_period(record)
                    ),
                    "announcement_time": record.announcement_time,
                    "market": record.market,
                    "column": record.column,
                    "symbols": list(record.symbols),
                    "adjunct_url": record.adjunct_url,
                }
                for record in scan["selected_records"][:30]
            ],
            "source_profile": source_profile,
            "standalone_supplement": (
                None
                if standalone_scan is None
                else {
                    "enabled": True,
                    "selected_announcements": len(standalone_scan["selected_records"]),
                    "fallback_reason": "supplementary_or_validation_source",
                    "scan_results": standalone_scan["scan_results"],
                    "per_instrument_scan": standalone_scan.get("per_instrument_scan"),
                }
            ),
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
        per_instrument_scan=not args.no_per_instrument_scan,
        per_instrument_page_size=args.per_instrument_page_size,
        per_instrument_max_pages=args.per_instrument_max_pages,
        report_period_types=_parse_csv(args.report_period_types),
        source_profile=args.source_profile,
        include_standalone_supplement=args.include_standalone_supplement,
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
    parser.add_argument("--no-per-instrument-scan", action="store_true")
    parser.add_argument("--per-instrument-page-size", type=int, default=30)
    parser.add_argument("--per-instrument-max-pages", type=int, default=6)
    parser.add_argument("--report-period-types", default="annual,semiannual")
    parser.add_argument("--source-profile", default=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE)
    parser.add_argument("--include-standalone-supplement", action="store_true")
    parser.add_argument("--archive-root", default="data/filings/financial_statements/broker_risk_control")
    parser.add_argument("--scan-only", action="store_true")
    parser.add_argument("--write", action="store_true", help="Persist manifests, archived PDFs, and numeric facts.")
    parser.add_argument("--output", default="")
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
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


def enrich_cninfo_stock_params(
    scanner: Any,
    instruments: Sequence[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Resolve CNInfo orgId values so per-stock announcement scans are precise."""
    if not instruments:
        return [], {"attempted": 0, "resolved": 0, "skipped": 0, "errors": []}
    if not hasattr(scanner, "session"):
        return (
            [dict(item) for item in instruments],
            {
                "attempted": 0,
                "resolved": 0,
                "skipped": len(instruments),
                "errors": ["scanner_session_unavailable"],
            },
        )

    enriched: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    attempted = 0
    resolved = 0
    for instrument in instruments:
        item = dict(instrument)
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            enriched.append(item)
            continue
        existing_org_id = _cninfo_org_id(item)
        if existing_org_id:
            item["cninfo_org_id"] = existing_org_id
            item["cninfo_stock_param"] = f"{symbol},{existing_org_id}"
            enriched.append(item)
            resolved += 1
            continue
        attempted += 1
        try:
            org_id = _resolve_cninfo_org_id(scanner, symbol)
        except Exception as exc:
            org_id = None
            errors.append({"symbol": symbol, "error": str(exc)})
        if org_id:
            item["cninfo_org_id"] = org_id
            item["cninfo_stock_param"] = f"{symbol},{org_id}"
            resolved += 1
        enriched.append(item)
        interval = float(getattr(scanner, "request_interval_seconds", 0.0) or 0.0)
        if interval > 0:
            time.sleep(interval)
    return (
        enriched,
        {
            "attempted": attempted,
            "resolved": resolved,
            "skipped": len(instruments) - attempted,
            "errors": errors,
        },
    )


def _resolve_cninfo_org_id(scanner: Any, symbol: str) -> Optional[str]:
    response = scanner.session.post(
        _CNINFO_TOP_SEARCH_URL,
        data={"keyWord": symbol, "maxNum": "10"},
        headers=_CNINFO_HEADERS,
        timeout=float(getattr(scanner, "request_timeout_seconds", 20.0) or 20.0),
    )
    response.raise_for_status()
    payload = response.json()
    rows = _extract_cninfo_top_search_rows(payload)
    for row in rows:
        code = str(row.get("code") or row.get("stockCode") or "").strip()
        org_id = str(row.get("orgId") or row.get("org_id") or "").strip()
        if code == symbol and org_id:
            return org_id
    return None


def _extract_cninfo_top_search_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "rows", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("rows", "records", "list"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _cninfo_stock_param(instrument: Dict[str, Any]) -> Optional[str]:
    stock_param = str(instrument.get("cninfo_stock_param") or "").strip()
    if stock_param:
        return stock_param
    symbol = str(instrument.get("symbol") or "").strip()
    if not symbol:
        return None
    org_id = _cninfo_org_id(instrument)
    if org_id:
        return f"{symbol},{org_id}"
    return symbol


def _cninfo_org_id(instrument: Dict[str, Any]) -> Optional[str]:
    for key in ("cninfo_org_id", "org_id", "orgId"):
        value = str(instrument.get(key) or "").strip()
        if value:
            return value
    return None


def _broker_scope_eligible(instrument: Dict[str, Any]) -> bool:
    scope = instrument.get("listed_broker_dealer_scope")
    if isinstance(scope, dict):
        return bool(scope.get("eligible"))
    return resolve_listed_broker_dealer_scope(instrument).eligible


if __name__ == "__main__":
    raise SystemExit(main())
