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
from contextlib import nullcontext
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_manager import data_manager
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BROKER_RISK_CONTROL_PARSER_VERSION,
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
from research.announcements import (
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementScope,
    build_announcement_key,
    load_announcement_acquisition_config,
)
from research.providers.registry import OfficialAnnouncementProviderRegistry
from scripts.research_cli_support import (
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


LOGGER = logging.getLogger(__name__)


def _announcement_id(record: AnnouncementRecord) -> str:
    return str(record.source_announcement_id or "")


def _announcement_time(record: AnnouncementRecord) -> Optional[str]:
    return record.published_at


def _announcement_url(record: AnnouncementRecord) -> Optional[str]:
    if not record.attachments:
        return None
    attachment = record.attachments[0]
    return attachment.resolved_url or attachment.source_url


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
    announcement_service: AnnouncementAcquisitionService,
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
    title_patterns: Optional[Sequence[str]] = None,
    source_profile: str = BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
) -> Dict[str, Any]:
    """Scan official announcements for broker regulatory report candidates."""
    selected: List[AnnouncementRecord] = []
    seen_announcement_ids: set[str] = set()
    market_scan_results: List[Dict[str, Any]] = []
    per_instrument_results: List[Dict[str, Any]] = []

    def _append_selected(records: Sequence[AnnouncementRecord]) -> int:
        added = 0
        for record in records:
            key = _announcement_id(record)
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
        if exchange not in {"SSE", "SZSE", "BSE"}:
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
        route_result = announcement_service.acquire(
            AnnouncementQuery(
                purpose_key=source_profile,
                scope=AnnouncementScope(
                    exchange=exchange,
                    market=exchange,
                    keyword=search_key,
                    start_date=start_date,
                    end_date=end_date,
                    page_size=page_size,
                    max_pages=max_pages,
                ),
            ),
            selectors=_filters(),
        )
        result = route_result.scan_result
        if result is None:
            market_scan_results.append(
                {"exchange": exchange, "status": "announcement_route_returned_no_result"}
            )
            continue
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
                "market": result.query.scope.market,
                "source": result.source,
                "pages_scanned": result.pages_scanned,
                "announcements_seen": result.announcements_seen,
                "selected_announcements": len(result.selected_records),
                "selected_announcements_added": added,
                "max_announcement_time": result.max_published_at,
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
            if exchange not in {"SSE", "SZSE", "BSE"}:
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
            attempted += 1
            LOGGER.info(
                "broker risk-control instrument scan start: instrument_id=%s symbol=%s exchange=%s source_profile=%s window=%s..%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                exchange,
                source_profile,
                start_date,
                end_date,
            )
            route_result = announcement_service.acquire(
                AnnouncementQuery(
                    purpose_key=source_profile,
                    scope=AnnouncementScope(
                        exchange=exchange,
                        market=exchange,
                        instrument_id=str(instrument.get("instrument_id") or ""),
                        symbol=str(instrument.get("symbol") or ""),
                        keyword=search_key,
                        start_date=start_date,
                        end_date=end_date,
                        page_size=per_instrument_page_size,
                        max_pages=per_instrument_max_pages,
                    ),
                ),
                selectors=_filters(),
            )
            result = route_result.scan_result
            if result is None:
                per_instrument_results.append(
                    {
                        "instrument_id": instrument.get("instrument_id"),
                        "symbol": instrument.get("symbol"),
                        "exchange": exchange,
                        "status": "announcement_route_returned_no_result",
                    }
                )
                continue
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
                    "source": result.source,
                    "pages_scanned": result.pages_scanned,
                    "announcements_seen": result.announcements_seen,
                    "selected_announcements": len(result.selected_records),
                    "selected_announcements_added": added,
                    "max_announcement_time": result.max_published_at,
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


def load_shared_broker_annual_report_records(
    shared_asset_access: Any,
    *,
    instruments: Sequence[Dict[str, Any]],
    report_periods: Sequence[str],
) -> Dict[str, Any]:
    """Project locally valid shared annual assets into the existing parser input."""

    allowed_periods = {str(item) for item in report_periods}
    records: list[AnnouncementRecord] = []
    seen_assets: set[str] = set()
    for instrument in instruments:
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            continue
        projection = shared_asset_access.list_assets(
            instrument_id=instrument_id,
            limit=1000,
        )
        for asset in projection.get("items", ()):
            if (
                asset.get("document_family") != "annual_report"
                or asset.get("availability") != "local_valid"
                or str(asset.get("report_period") or "") not in allowed_periods
            ):
                continue
            asset_id = str(asset.get("asset_id") or "")
            if not asset_id or asset_id in seen_assets:
                continue
            seen_assets.add(asset_id)
            source = str(asset.get("source") or "").strip().lower()
            source_announcement_id = str(
                asset.get("source_announcement_id") or ""
            ).strip()
            attachment_id = str(asset.get("attachment_id") or asset_id)
            records.append(
                AnnouncementRecord(
                    source=source,
                    source_announcement_id=source_announcement_id,
                    announcement_key=build_announcement_key(
                        source, source_announcement_id
                    ),
                    title=(
                        f"{str(asset['report_period'])[:4]}年年度报告"
                        + ("（修订版）" if asset.get("is_correction") else "")
                    ),
                    published_at=asset.get("published_at"),
                    exchange=str(instrument.get("exchange") or "").upper() or None,
                    market=str(instrument.get("exchange") or "").upper() or None,
                    symbols=(str(instrument.get("symbol") or "").strip(),),
                    attachments=(
                        AnnouncementAttachment(
                            source_url=f"shared-asset://{asset_id}",
                            attachment_id=attachment_id,
                            media_type="application/pdf",
                            file_extension="pdf",
                            raw_metadata={
                                "shared_asset_id": asset_id,
                                "observation_version": asset.get(
                                    "observation_version"
                                ),
                                "content_hash": asset.get("content_hash"),
                            },
                        ),
                    ),
                    raw_payload={
                        "shared_asset_id": asset_id,
                        "shared_asset_projection": dict(asset),
                    },
                    selection_reasons=("shared_annual_report_asset",),
                )
            )
    records.sort(
        key=lambda item: (
            str(item.published_at or ""),
            item.source,
            item.source_announcement_id,
        )
    )
    return {
        "selected_records": records,
        "scan_results": [],
        "market_scan_results": [],
        "per_instrument_scan": {
            "enabled": False,
            "attempted_instruments": 0,
            "instruments_with_matches": len(
                {symbol for record in records for symbol in record.symbols}
            ),
            "selected_announcements_added": len(records),
            "results": [],
        },
        "source_mode": "shared_announcement_asset",
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
    announcement_service: Optional[AnnouncementAcquisitionService] = None,
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
    tier: str = "history",
    repair_existing: bool = False,
    shared_asset_access: Any | None = None,
    annual_report_asset_mode: str | None = None,
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
    broker_cfg = data_manager.research_config.modules.get(
        "broker_risk_control_reports", {}
    )
    dependency_cfg = broker_cfg.get("annual_report_asset_dependency", {})
    dependency_mode = str(
        annual_report_asset_mode
        or dependency_cfg.get("mode", "legacy")
    ).strip().lower()
    if dependency_mode not in {"legacy", "dual_read", "shared_only"}:
        raise ValueError("invalid broker annual-report asset mode")
    shared_only_annual = (
        source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
        and dependency_mode == "shared_only"
    )
    active_announcement_service = announcement_service
    if not shared_only_annual or include_standalone_supplement:
        acquisition_config = load_announcement_acquisition_config(
            data_manager.research_config
        )
        active_announcement_service = (
            active_announcement_service
            or AnnouncementAcquisitionService(
                registry=OfficialAnnouncementProviderRegistry(
                    research_config=data_manager.research_config
                ),
                config=acquisition_config,
            )
        )
    effective_limit = 0 if instrument_ids else limit_instruments
    selected_instruments = select_broker_instruments(
        db_ops,
        exchanges=exchanges,
        limit=effective_limit,
        instrument_ids=instrument_ids,
        storage=storage,
        require_confirmed_scope=True,
    )
    LOGGER.info(
        "broker risk-control instruments selected: count=%s symbols=%s",
        len(selected_instruments),
        ",".join(str(item.get("symbol") or "") for item in selected_instruments[:20]),
    )
    org_resolution = {
        "attempted": 0,
        "resolved": 0,
        "skipped": len(selected_instruments),
        "errors": [],
        "owner": "announcement_provider",
    }
    if shared_only_annual:
        if shared_asset_access is None:
            raise RuntimeError(
                "shared-only broker annual-report backfill requires asset access"
            )
        scan = load_shared_broker_annual_report_records(
            shared_asset_access,
            instruments=selected_instruments,
            report_periods=periods,
        )
        LOGGER.info(
            "broker shared annual-report assets loaded: selected_assets=%s",
            len(scan["selected_records"]),
        )
    elif repair_existing:
        scan = load_existing_broker_risk_control_records(
            storage,
            instruments=selected_instruments,
            report_periods=periods,
            source_profile=source_profile,
        )
        payload_fetcher = payload_fetcher or scan.get("payload_fetcher")
        LOGGER.info(
            "broker risk-control repair records loaded: selected_announcements=%s missing_archives=%s",
            len(scan["selected_records"]),
            (scan.get("repair_existing") or {}).get("missing_archives"),
        )
    else:
        if active_announcement_service is None:
            raise RuntimeError("broker announcement service is unavailable")
        scan = scan_broker_risk_control_announcements(
            active_announcement_service,
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
        if active_announcement_service is None:
            raise RuntimeError("supplementary announcement service is unavailable")
        standalone_scan = scan_broker_risk_control_announcements(
            active_announcement_service,
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
    standalone_gap_filter: Optional[Dict[str, Any]] = None
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
            announcement_service=active_announcement_service,
            payload_fetcher=payload_fetcher,
            archive_root=archive_root,
            source_profile=source_profile,
            force_reparse_existing=repair_existing,
            replace_existing_facts=repair_existing,
            shared_asset_access=shared_asset_access,
            annual_report_asset_mode=dependency_mode,
        )
        with _financial_storage_scope(storage):
            service_result = service.backfill(
                instruments=selected_instruments,
                report_periods=periods,
                announcement_records=scan["selected_records"],
                tier=tier,
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
            standalone_gap_filter = filter_standalone_supplement_records_for_primary_gaps(
                standalone_scan["selected_records"],
                instruments=selected_instruments,
                report_periods=periods,
                primary_result=service_result,
                primary_records=scan["selected_records"],
                storage=storage,
            )
            LOGGER.info(
                "broker risk-control standalone supplement parse start: candidate_reports=%s gap_fill_reports=%s dry_run=%s tier=history",
                len(standalone_scan["selected_records"]),
                len(standalone_gap_filter["selected_records"]),
                not write,
            )
            supplement_service = BrokerRiskControlReportSyncService(
                storage=storage,
                announcement_service=active_announcement_service,
                payload_fetcher=payload_fetcher,
                archive_root=archive_root,
                source_profile=BROKER_RISK_CONTROL_SOURCE_PROFILE,
            )
            with _financial_storage_scope(storage):
                supplement_result = supplement_service.backfill(
                    instruments=selected_instruments,
                    report_periods=periods,
                    announcement_records=standalone_gap_filter["selected_records"],
                    tier=tier,
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
            supplement_result["primary_gap_filter"] = {
                key: value
                for key, value in standalone_gap_filter.items()
                if key != "selected_records"
            }
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
        "tier": tier,
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
                "listed_broker_dealer_scope": item.get("listed_broker_dealer_scope"),
            }
            for item in selected_instruments
        ],
        "announcement_scan": {
            "source_mode": scan.get("source_mode", "provider_discovery"),
            "cninfo_org_id_resolution": org_resolution,
            "selected_announcements": len(scan["selected_records"]),
            "scan_results": scan["scan_results"],
            "market_scan_results": scan.get("market_scan_results", scan["scan_results"]),
            "per_instrument_scan": scan.get("per_instrument_scan"),
            "selected_preview": [
                {
                    "announcement_id": _announcement_id(record),
                    "title": record.title,
                    "report_period": (
                        infer_broker_annual_report_period(record)
                        if source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                        and is_formal_broker_annual_or_semiannual_report_title(record.title)
                        else infer_broker_risk_control_report_period(record)
                    ),
                    "announcement_time": _announcement_time(record),
                    "market": record.market,
                    "source": record.source,
                    "symbols": list(record.symbols),
                    "adjunct_url": _announcement_url(record),
                }
                for record in scan["selected_records"][:30]
            ],
            "source_profile": source_profile,
            "repair_existing": scan.get("repair_existing"),
            "standalone_supplement": (
                None
                if standalone_scan is None
                else {
                    "enabled": True,
                    "selected_announcements": len(standalone_scan["selected_records"]),
                    "gap_fill_announcements": None
                    if standalone_gap_filter is None
                    else len(standalone_gap_filter["selected_records"]),
                    "fallback_reason": "primary_missing_net_capital_only",
                    "primary_gap_filter": None
                    if standalone_gap_filter is None
                    else {
                        key: value
                        for key, value in standalone_gap_filter.items()
                        if key != "selected_records"
                    },
                    "scan_results": standalone_scan["scan_results"],
                    "per_instrument_scan": standalone_scan.get("per_instrument_scan"),
                }
            ),
        },
        "backfill": service_result,
    }


def filter_standalone_supplement_records_for_primary_gaps(
    records: Sequence[AnnouncementRecord],
    *,
    instruments: Sequence[Dict[str, Any]],
    report_periods: Sequence[str],
    primary_result: Dict[str, Any],
    primary_records: Optional[Sequence[AnnouncementRecord]] = None,
    storage: Optional[Any] = None,
) -> Dict[str, Any]:
    """Select standalone reports only for primary annual/semiannual net-capital gaps."""
    instrument_by_symbol = {
        str(item.get("symbol") or "").strip(): item
        for item in instruments
        if item.get("symbol")
    }
    primary_pairs = {
        (str(summary.get("instrument_id") or ""), str(summary.get("report_period") or ""))
        for summary in primary_result.get("report_summaries", []) or []
        if summary.get("instrument_id") and summary.get("report_period")
    }
    primary_record_pairs = {
        (str((instrument or {}).get("instrument_id") or ""), str(report_period or ""))
        for record in primary_records or []
        for instrument, report_period in [
            (
                _resolve_record_instrument_by_symbol(record, instrument_by_symbol),
                infer_broker_annual_report_period(record),
            )
        ]
        if instrument is not None and report_period
    }
    all_candidate_pairs = {
        (str(item.get("instrument_id") or ""), str(period))
        for item in instruments
        for period in report_periods
        if item.get("instrument_id") and str(period)
    }
    expected_pairs = primary_pairs or primary_record_pairs or all_candidate_pairs
    covered_pairs = {
        (str(summary.get("instrument_id") or ""), str(summary.get("report_period") or ""))
        for summary in primary_result.get("report_summaries", []) or []
        if summary.get("net_capital")
        or "net_capital" in (summary.get("matched_canonical_facts") or [])
    }
    covered_pairs.update(
        _load_existing_net_capital_pairs(
            storage,
            instruments=instruments,
            report_periods=[period for _, period in expected_pairs],
        )
    )
    missing_pairs = expected_pairs - covered_pairs
    selected: List[AnnouncementRecord] = []
    ignored: List[Dict[str, Any]] = []
    for record in records:
        instrument = _resolve_record_instrument_by_symbol(record, instrument_by_symbol)
        report_period = infer_broker_risk_control_report_period(record)
        pair = (
            str((instrument or {}).get("instrument_id") or ""),
            str(report_period or ""),
        )
        if instrument is not None and pair in missing_pairs:
            selected.append(record)
            continue
        ignored.append(
            {
                "announcement_id": _announcement_id(record),
                "symbols": list(record.symbols),
                "report_period": report_period,
                "reason": "primary_net_capital_already_present_or_not_target_gap",
            }
        )
    return {
        "selected_records": selected,
        "candidate_records": len(records),
        "selected_records_count": len(selected),
        "ignored_records_count": len(ignored),
        "primary_pairs_count": len(primary_pairs),
        "primary_record_pairs_count": len(primary_record_pairs),
        "covered_net_capital_pairs_count": len(covered_pairs),
        "expected_pairs_source": (
            "primary_report_summaries"
            if primary_pairs
            else "primary_announcement_records"
            if primary_record_pairs
            else "candidate_report_periods"
        ),
        "missing_primary_pairs": [
            {"instrument_id": instrument_id, "report_period": period}
            for instrument_id, period in sorted(missing_pairs)
        ],
        "ignored_preview": ignored[:20],
    }


def _resolve_record_instrument_by_symbol(
    record: AnnouncementRecord,
    instrument_by_symbol: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    for symbol in record.symbols:
        clean = str(symbol).strip()
        if clean in instrument_by_symbol:
            return instrument_by_symbol[clean]
    return None


def _load_existing_net_capital_pairs(
    storage: Optional[Any],
    *,
    instruments: Sequence[Dict[str, Any]],
    report_periods: Sequence[str],
) -> set[tuple[str, str]]:
    """Read local canonical net_capital coverage for unchanged primary reports."""
    if storage is None or not hasattr(storage, "get_financial_numeric_facts"):
        return set()
    periods = {str(period) for period in report_periods if str(period)}
    pairs: set[tuple[str, str]] = set()
    for instrument in instruments:
        instrument_id = str(instrument.get("instrument_id") or "")
        if not instrument_id:
            continue
        try:
            rows = storage.get_financial_numeric_facts(
                instrument_id,
                include_history=True,
                canonical_fact_name="net_capital",
            )
        except Exception as exc:
            LOGGER.warning(
                "broker risk-control existing net_capital coverage read failed: instrument_id=%s error=%s",
                instrument_id,
                exc,
            )
            continue
        for row in rows:
            period = str(row.get("report_period") or "")
            value = row.get("fact_value")
            if period in periods and value is not None:
                pairs.add((instrument_id, period))
    return pairs


def load_existing_broker_risk_control_records(
    storage: Any,
    *,
    instruments: Sequence[Dict[str, Any]],
    report_periods: Sequence[str],
    source_profile: str,
) -> Dict[str, Any]:
    """Load already archived broker risk-control PDFs for parser repair."""
    parser_version = (
        BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
        if source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
        else BROKER_RISK_CONTROL_PARSER_VERSION
    )
    instrument_ids = [str(item.get("instrument_id") or "") for item in instruments if item.get("instrument_id")]
    periods = [str(period) for period in report_periods if str(period)]
    if not instrument_ids or not periods or not hasattr(storage, "get_connection"):
        return {
            "selected_records": [],
            "scan_results": [],
            "market_scan_results": [],
            "per_instrument_scan": {
                "enabled": False,
                "attempted_instruments": len(instrument_ids),
                "instruments_with_matches": 0,
                "selected_announcements_added": 0,
                "results": [],
            },
            "repair_existing": {
                "enabled": True,
                "loaded_manifests": 0,
                "missing_archives": 0,
            },
            "payload_fetcher": lambda record: None,
        }
    params: List[Any] = [parser_version, *instrument_ids, *periods]
    instrument_placeholders = ",".join("?" for _ in instrument_ids)
    period_placeholders = ",".join("?" for _ in periods)
    with _financial_storage_scope(storage), storage.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM financial_source_files
            WHERE parser_version = ?
              AND instrument_id IN ({instrument_placeholders})
              AND report_period IN ({period_placeholders})
              AND status IN ('downloaded', 'parsed', 'parse_failed')
            ORDER BY instrument_id, report_period, updated_at DESC
            """,
            params,
        ).fetchall()

    records: List[AnnouncementRecord] = []
    payload_by_announcement_id: Dict[str, Path] = {}
    missing_archives = 0
    for row in rows:
        item = dict(row)
        archive_path = Path(str(item.get("archive_path") or ""))
        if not archive_path.is_absolute():
            archive_path = REPO_ROOT / archive_path
        if not archive_path.exists():
            missing_archives += 1
            continue
        metadata = _json_dict(item.get("metadata_json"))
        announcement_record = metadata.get("announcement_record")
        if not isinstance(announcement_record, dict):
            announcement_record = {}
        announcement_id = str(item.get("filing_id") or item.get("source_file_id") or "")
        if not announcement_id:
            announcement_id = str(item.get("source_file_id") or "")
        source = str(item.get("source") or "cninfo").strip().lower()
        source_url = str(item.get("source_url") or "").strip()
        record = AnnouncementRecord(
            source=source,
            source_announcement_id=announcement_id,
            announcement_key=build_announcement_key(source, announcement_id),
            title=str(
                metadata.get("announcement_title")
                or f"archived broker report {announcement_id}"
            ),
            published_at=str(item.get("published_at") or "") or None,
            market=str(announcement_record.get("market") or ""),
            exchange=str(item.get("exchange") or "") or None,
            symbols=tuple(
                announcement_record.get("symbols") or [item.get("symbol")]
            ),
            attachments=(
                AnnouncementAttachment(
                    source_url=source_url,
                    resolved_url=(
                        source_url
                        if source_url.startswith(("http://", "https://"))
                        else None
                    ),
                    file_extension="PDF",
                ),
            )
            if source_url
            else (),
            selection_reasons=tuple(
                announcement_record.get("selection_reasons")
                or ["repair_existing_manifest"]
            ),
        )
        records.append(record)
        payload_by_announcement_id[announcement_id] = archive_path

    def _payload_fetcher(record: AnnouncementRecord) -> Optional[bytes]:
        path = payload_by_announcement_id.get(_announcement_id(record))
        return None if path is None else path.read_bytes()

    instruments_with_matches = {
        str(row["instrument_id"])
        for row in rows
        if str(row["instrument_id"] or "") in instrument_ids
    }
    return {
        "selected_records": records,
        "scan_results": [],
        "market_scan_results": [],
        "per_instrument_scan": {
            "enabled": False,
            "attempted_instruments": len(instrument_ids),
            "instruments_with_matches": len(instruments_with_matches),
            "selected_announcements_added": len(records),
            "results": [],
        },
        "repair_existing": {
            "enabled": True,
            "loaded_manifests": len(records),
            "missing_archives": missing_archives,
            "parser_version": parser_version,
        },
        "payload_fetcher": _payload_fetcher,
    }


def _financial_storage_scope(storage: Any):
    if hasattr(storage, "financial_database_scope"):
        return storage.financial_database_scope()
    return nullcontext()


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
        tier=args.tier,
        repair_existing=args.repair_existing,
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
    parser.add_argument("--tier", default="history", choices=["hot", "history"])
    parser.add_argument("--scan-only", action="store_true")
    parser.add_argument(
        "--repair-existing",
        action="store_true",
        help="Reparse existing archived manifests and replace facts instead of scanning CNInfo.",
    )
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


def _json_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        payload = json.loads(str(raw or "{}"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


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


def _broker_scope_eligible(instrument: Dict[str, Any]) -> bool:
    scope = instrument.get("listed_broker_dealer_scope")
    if isinstance(scope, dict):
        return bool(scope.get("eligible"))
    return resolve_listed_broker_dealer_scope(instrument).eligible


if __name__ == "__main__":
    raise SystemExit(main())
