"""
Financial statement maintenance driven by CNInfo disclosure announcements.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from research.financial_disclosure_events import (
    ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS,
    FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION,
    FINANCIAL_PERIODIC_REPORT_CLASSIFICATION,
    PENDING_DELISTING_RISK_CLASSIFICATION,
    FinancialDisclosureEvent,
    build_financial_disclosure_events,
    build_financial_symbol_index,
    financial_disclosure_event_filter,
    infer_report_periods_from_title,
    is_financial_disclosure_like_title,
)
from research.financial_source_field_mapping import MAPPING_VERSION
from research.financial_statement_maintenance_repair import (
    FinancialMaintenanceRepairRouter,
    FinancialMaintenanceRepairTarget,
)
from research.financial_statement_profile import resolve_financial_statement_profile
from research.financial_statements_sync import build_financial_report_periods
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanner,
)
from research.storage import ResearchStorageManager
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (
    DEFAULT_REQUIRED_CANONICAL_FACTS,
)
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager
from utils.date_utils import get_shanghai_time


MAPPING_POLICY_GAP_REASONS = {"mapping_catalog_empty", "outside_approved_local_core"}
ACCEPTED_LIFECYCLE_GAP_CLASSIFICATIONS = frozenset(
    {
        "pre_listing_period",
        "post_delisting_or_no_disclosure",
    }
)
ACCEPTED_MAINTENANCE_GAP_CLASSIFICATIONS = (
    ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS
    | ACCEPTED_LIFECYCLE_GAP_CLASSIFICATIONS
)


@dataclass
class FinancialDisclosureMaintenanceCandidate:
    instrument_id: str
    symbol: str
    exchange: str
    report_period: str
    profile: str
    reasons: List[str] = field(default_factory=list)
    events: List[FinancialDisclosureEvent] = field(default_factory=list)
    lifecycle_classification: Optional[str] = None

    @property
    def key(self) -> Tuple[str, str]:
        return (self.instrument_id, self.report_period)

    @property
    def classification(self) -> str:
        if self.lifecycle_classification:
            return self.lifecycle_classification
        if any(
            event.classification == PENDING_DELISTING_RISK_CLASSIFICATION
            or "pending_delisting_risk" in event.reasons
            for event in self.events
        ):
            return PENDING_DELISTING_RISK_CLASSIFICATION
        if any(
            event.classification == FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
            or "periodic_report_delayed" in event.reasons
            for event in self.events
        ):
            return FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
        return FINANCIAL_PERIODIC_REPORT_CLASSIFICATION


class FinancialDisclosureIncrementalSyncService:
    """Run targeted Financial L1 maintenance from CNInfo disclosure events."""

    purpose_key = "financial_disclosure_incremental_sync"

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        announcement_scanner: Optional[CninfoAnnouncementScanner] = None,
        repair_router: Optional[FinancialMaintenanceRepairRouter] = None,
    ) -> None:
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.announcement_scanner = announcement_scanner or self._build_scanner()
        self.repair_router = repair_router or FinancialMaintenanceRepairRouter(
            storage=storage,
            research_config=self.research_config,
        )
        self._last_repair_source_summary: Dict[str, Any] = {}
        self._last_candidate_source_summary: Dict[str, int] = {}
        self._last_candidate_unlimited_count: int = 0
        self._last_candidate_limit: int = 0

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages_per_market: Optional[int] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
        target_symbols: Optional[List[str]] = None,
        announcement_search_key: Optional[str] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        db_path: Optional[str] = None,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        dry_run: bool = False,
        reconciliation: bool = False,
    ) -> Dict[str, Any]:
        started_at = time.monotonic()
        module_cfg = self.research_config.modules.get("financial_statements", {})
        maintenance_cfg = module_cfg.get("disclosure_incremental_sync", {})
        target_exchanges = exchanges or list(self.research_config.markets)
        lookback = int(maintenance_cfg.get("lookback_days", 14) if lookback_days is None else lookback_days)
        overlap = int(maintenance_cfg.get("overlap_days", 3) if overlap_days is None else overlap_days)
        scan_page_size = int(maintenance_cfg.get("page_size", 30) if page_size is None else page_size)
        max_pages = int(
            maintenance_cfg.get("max_pages_per_market", 40)
            if max_pages_per_market is None
            else max_pages_per_market
        )
        candidate_limit = int(
            maintenance_cfg.get("max_candidates", 500)
            if max_candidates is None
            else max_candidates
        )
        recheck_days = int(
            maintenance_cfg.get("pending_recheck_days", 7)
            if pending_recheck_days is None
            else pending_recheck_days
        )
        required_facts = list(
            module_cfg.get("readiness", {}).get(
                "required_core_facts",
                DEFAULT_REQUIRED_CANONICAL_FACTS,
            )
        )
        target_periods = self._resolve_report_periods(
            report_periods=report_periods,
            period_window=period_window,
            rolling_quarters=rolling_quarters,
            baseline_report_period=baseline_report_period,
            latest_report_period=latest_report_period,
        )
        financial_db_path = Path(
            db_path
            or getattr(self.research_config.storage, "financials_db_path", None)
            or "data/financials.db"
        )

        run_id: Optional[int] = None
        if not dry_run:
            with self.storage.financial_database_scope():
                run_id = self.storage.start_ingestion_run(
                    domain="financial_statements",
                    job_name=(
                        "financial_disclosure_reconciliation_sync"
                        if reconciliation
                        else self.purpose_key
                    ),
                    market=",".join(target_exchanges),
                    metadata={
                        "exchanges": target_exchanges,
                        "report_periods": target_periods,
                        "target_instrument_ids": list(target_instrument_ids or []),
                        "target_symbols": list(target_symbols or []),
                        "announcement_search_key": announcement_search_key,
                        "dry_run": dry_run,
                        "reconciliation": reconciliation,
                    },
                )

        try:
            instruments = await self._load_active_instruments(
                target_exchanges,
                target_instrument_ids=target_instrument_ids,
                target_symbols=target_symbols,
            )
            scan_result = (
                {
                    "events": [],
                    "announcements_scanned": 0,
                    "selected_announcements": 0,
                    "financial_like_announcements": 0,
                    "filtered_financial_like_announcements": 0,
                    "selected_without_event_count": 0,
                    "selected_announcements_preview": [],
                    "event_count": 0,
                    "pages_scanned": 0,
                    "errors": [],
                }
                if reconciliation
                else self._scan_announcements(
                    exchanges=target_exchanges,
                    instruments=instruments,
                    lookback_days=lookback,
                    overlap_days=overlap,
                    page_size=scan_page_size,
                    max_pages_per_market=max_pages,
                    search_key=announcement_search_key,
                    run_id=run_id,
                    dry_run=dry_run,
                )
            )
            candidates = self._build_candidates(
                instruments=instruments,
                events=scan_result["events"],
                report_periods=target_periods if reconciliation else None,
                required_core_facts=required_facts,
                mapping_version=MAPPING_VERSION,
                max_candidates=candidate_limit,
                run_id=run_id,
                dry_run=dry_run,
            )
            write_result = await self._apply_candidates(
                candidates=list(candidates.values()),
                required_core_facts=required_facts,
                mapping_version=MAPPING_VERSION,
                db_path=financial_db_path,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
                pending_recheck_days=recheck_days,
                run_id=run_id,
                dry_run=dry_run,
            )
            elapsed = round(time.monotonic() - started_at, 3)
            status = self._derive_status(
                candidate_count=len(candidates),
                failed_count=write_result["failed_count"],
                blocking_count=write_result["blocking_gap_count"],
                mapping_policy_gap_count=write_result["mapping_policy_gap_count"],
                scan_errors=scan_result["errors"],
            )
            result = {
                "status": status,
                "job_name": (
                    "financial_disclosure_reconciliation_sync"
                    if reconciliation
                    else self.purpose_key
                ),
                "dry_run": dry_run,
                "reconciliation": reconciliation,
                "db_path": str(financial_db_path),
                "exchanges": target_exchanges,
                "report_periods": target_periods,
                "target_instrument_ids": list(target_instrument_ids or []),
                "target_symbols": list(target_symbols or []),
                "announcement_search_key": announcement_search_key,
                "announcements_scanned": scan_result["announcements_scanned"],
                "selected_announcements": scan_result["selected_announcements"],
                "financial_like_announcements": scan_result.get(
                    "financial_like_announcements",
                    0,
                ),
                "filtered_financial_like_announcements": scan_result.get(
                    "filtered_financial_like_announcements",
                    0,
                ),
                "selected_without_event_count": scan_result.get(
                    "selected_without_event_count",
                    0,
                ),
                "selected_announcements_preview": scan_result.get(
                    "selected_announcements_preview",
                    [],
                ),
                "event_count": scan_result.get("event_count", 0),
                "pages_scanned": scan_result["pages_scanned"],
                "candidate_count": len(candidates),
                "candidate_unlimited_count": self._last_candidate_unlimited_count,
                "candidate_limit": self._last_candidate_limit,
                "candidate_sources": dict(self._last_candidate_source_summary),
                "scan_errors": scan_result["errors"][:10],
                "elapsed_seconds": elapsed,
                **write_result,
            }
            if run_id is not None:
                with self.storage.financial_database_scope():
                    self.storage.finish_ingestion_run(
                        run_id,
                        status=status,
                        rows_written=write_result["written_count"],
                        error_message=(
                            "; ".join(scan_result["errors"][:3])
                            if status == "failed"
                            else None
                        ),
                        metadata=result,
                    )
            return result
        except Exception as exc:
            if run_id is not None:
                with self.storage.financial_database_scope():
                    self.storage.finish_ingestion_run(
                        run_id,
                        status="failed",
                        rows_written=0,
                        error_message=str(exc),
                        metadata={
                            "exchanges": target_exchanges,
                            "dry_run": dry_run,
                            "elapsed_seconds": round(time.monotonic() - started_at, 3),
                        },
                    )
            raise

    def _build_scanner(self) -> CninfoAnnouncementScanner:
        cninfo_cfg = self.research_config.sources.get("cninfo", {})
        scan_cfg = cninfo_cfg.get("announcement_scan", {})
        return CninfoAnnouncementScanner(
            request_timeout_seconds=float(scan_cfg.get("request_timeout_seconds", 20.0)),
            request_interval_seconds=float(scan_cfg.get("request_interval_seconds", 0.2)),
            retry_attempts=int(scan_cfg.get("retry_attempts", 2)),
            retry_backoff_seconds=float(scan_cfg.get("retry_backoff_seconds", 0.5)),
        )

    async def _load_active_instruments(
        self,
        exchanges: Sequence[str],
        *,
        target_instrument_ids: Optional[Sequence[str]] = None,
        target_symbols: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        target_ids = {
            str(item).strip().upper()
            for item in target_instrument_ids or []
            if str(item).strip()
        }
        target_symbol_set = {
            str(item).strip()
            for item in target_symbols or []
            if str(item).strip()
        }
        instruments: List[Dict[str, Any]] = []
        for exchange in exchanges:
            rows = await self.db_ops.get_instruments_by_exchange(exchange)
            for row in rows:
                instrument_id = str(row.get("instrument_id") or "").strip().upper()
                symbol = str(row.get("symbol") or "").strip()
                if target_ids and instrument_id not in target_ids:
                    continue
                if target_symbol_set and symbol not in target_symbol_set:
                    continue
                if row.get("type") == "stock" and row.get("is_active", True):
                    instruments.append(dict(row))
        return instruments

    def _scan_announcements(
        self,
        *,
        exchanges: Sequence[str],
        instruments: Sequence[Mapping[str, Any]],
        lookback_days: int,
        overlap_days: int,
        page_size: int,
        max_pages_per_market: int,
        search_key: Optional[str],
        run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[str, Any]:
        now = get_shanghai_time()
        end_date = now.date().isoformat()
        symbol_index = build_financial_symbol_index(instruments)
        market_configs = self._announcement_market_configs()
        all_selected = []
        pages_scanned = 0
        announcements_scanned = 0
        selected_announcements = 0
        financial_like_announcements = 0
        errors: List[str] = []
        for exchange in exchanges:
            cfg = market_configs.get(exchange, {})
            column = str(cfg.get("column") or exchange.lower()).strip()
            market = str(cfg.get("market") or exchange).strip()
            with self.storage.financial_database_scope():
                state = self.storage.get_cninfo_announcement_scan_state(
                    purpose_key=self.purpose_key,
                    market=market,
                    column=column,
                )
            watermark = None if state is None else state.get("last_watermark")
            config = CninfoAnnouncementScanConfig(
                purpose_key=self.purpose_key,
                market=market,
                column=column,
                plate=cfg.get("plate"),
                tab_name=str(cfg.get("tab_name") or "fulltext"),
                category=cfg.get("category"),
                search_key=search_key or cfg.get("search_key"),
                start_date=(now - timedelta(days=max(lookback_days, overlap_days))).date().isoformat(),
                end_date=end_date,
                page_size=page_size,
                max_pages=max_pages_per_market,
                stop_at_watermark=watermark,
            )
            scan_started_at = get_shanghai_time().isoformat()
            result = self.announcement_scanner.scan(
                config,
                filters=[financial_disclosure_event_filter],
            )
            scan_completed_at = get_shanghai_time().isoformat()
            pages_scanned += result.pages_scanned
            announcements_scanned += result.announcements_seen
            selected_announcements += len(result.selected_records)
            financial_like_announcements += sum(
                1 for record in result.records if is_financial_disclosure_like_title(record.title)
            )
            all_selected.extend(result.selected_records)
            errors.extend(result.errors)
            if not dry_run:
                with self.storage.financial_database_scope():
                    noisy_filtered = max(
                        0,
                        sum(
                            1
                            for record in result.records
                            if is_financial_disclosure_like_title(record.title)
                        )
                        - len(result.selected_records),
                    )
                    self.storage.upsert_cninfo_announcement_scan_state(
                        purpose_key=self.purpose_key,
                        market=market,
                        column=column,
                        last_watermark=result.max_announcement_time or watermark,
                        last_scan_started_at=scan_started_at,
                        last_scan_completed_at=scan_completed_at,
                        pages_scanned=result.pages_scanned,
                        announcements_seen=result.announcements_seen,
                        selected_announcements=len(result.selected_records),
                        status="success" if not result.errors else "degraded",
                        metadata={
                            "errors": result.errors[:5],
                            "financial_like_announcements": sum(
                                1
                                for record in result.records
                                if is_financial_disclosure_like_title(record.title)
                            ),
                            "filtered_financial_like_announcements": noisy_filtered,
                        },
                    )
                    for record in result.selected_records:
                        for symbol in record.symbols or [""]:
                            instrument = symbol_index.get(symbol)
                            self.storage.store_cninfo_announcement_audit(
                                purpose_key=self.purpose_key,
                                announcement_id=record.announcement_id,
                                instrument_id=(
                                    None
                                    if instrument is None
                                    else str(instrument.get("instrument_id") or "")
                                ),
                                symbol=symbol or None,
                                market=record.market,
                                column=record.column,
                                announcement_time=record.announcement_time,
                                title=record.title,
                                adjunct_url=record.adjunct_url,
                                selection_reasons=record.selection_reasons,
                                raw_payload=record.raw_payload,
                                ingestion_run_id=run_id,
                            )
        events = build_financial_disclosure_events(all_selected, symbol_index)
        selected_event_ids = {
            str(event.announcement_id)
            for event in events
            if event.announcement_id
        }
        selected_without_event = max(
            0,
            len(
                {
                    str(record.announcement_id)
                    for record in all_selected
                    if record.announcement_id
                }
            )
            - len(selected_event_ids),
        )
        return {
            "events": events,
            "pages_scanned": pages_scanned,
            "announcements_scanned": announcements_scanned,
            "selected_announcements": selected_announcements,
            "financial_like_announcements": financial_like_announcements,
            "filtered_financial_like_announcements": max(
                0, financial_like_announcements - selected_announcements
            ),
            "selected_without_event_count": selected_without_event,
            "selected_announcements_preview": [
                {
                    "announcement_id": record.announcement_id,
                    "announcement_time": record.announcement_time,
                    "market": record.market,
                    "symbols": list(record.symbols),
                    "title": record.title,
                    "selection_reasons": list(record.selection_reasons),
                    "mapped_event": any(
                        event.announcement_id == record.announcement_id
                        for event in events
                    ),
                }
                for record in all_selected[:20]
            ],
            "event_count": len(events),
            "errors": errors,
        }

    def _announcement_market_configs(self) -> Dict[str, Dict[str, Any]]:
        cninfo_cfg = self.research_config.sources.get("cninfo", {})
        scan_cfg = cninfo_cfg.get("announcement_scan", {})
        configured = scan_cfg.get("markets") or {}
        defaults = {
            "SSE": {"market": "SSE", "column": "sse", "plate": "sh"},
            "SZSE": {"market": "SZSE", "column": "szse", "plate": "sz"},
            "BSE": {"market": "BSE", "column": "neeq", "plate": "bj"},
        }
        for exchange, value in configured.items():
            if isinstance(value, dict):
                defaults[str(exchange)] = {**defaults.get(str(exchange), {}), **value}
        return defaults

    def _build_candidates(
        self,
        *,
        instruments: Sequence[Mapping[str, Any]],
        events: Sequence[FinancialDisclosureEvent],
        report_periods: Optional[Sequence[str]],
        required_core_facts: Sequence[str],
        mapping_version: str,
        max_candidates: int,
        run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[Tuple[str, str], FinancialDisclosureMaintenanceCandidate]:
        instruments_by_id = {
            str(instrument.get("instrument_id") or ""): instrument
            for instrument in instruments
            if instrument.get("instrument_id")
        }
        candidates: Dict[Tuple[str, str], FinancialDisclosureMaintenanceCandidate] = {}
        filtered_stale_pending = 0
        risk_audits_by_instrument = self._load_disclosure_risk_audits_by_instrument(
            instruments_by_id.keys()
        )
        for event in events:
            instrument = instruments_by_id.get(event.instrument_id)
            if instrument is None:
                continue
            candidate = self._candidate_for_event(instrument, event)
            is_new = candidate.key not in candidates
            candidates.setdefault(candidate.key, candidate).events.append(event)
            candidates[candidate.key].reasons.extend(
                reason for reason in event.reasons if reason not in candidates[candidate.key].reasons
            )
        with self.storage.financial_database_scope():
            pending_states = self.storage.list_financial_disclosure_event_states(
                statuses=[
                    "pending_recheck",
                    "pending_delisting_risk",
                    "accepted_disclosure_gap",
                ],
                limit=max_candidates if max_candidates > 0 else None,
            )
        for state in pending_states:
            instrument = instruments_by_id.get(str(state.get("instrument_id") or ""))
            if instrument is None:
                continue
            state_status = str(state.get("status") or "")
            if (
                state_status != "accepted_disclosure_gap"
                and self._is_stale_filtered_pending_state(state)
            ):
                filtered_stale_pending += 1
                self._record_filtered_stale_pending_state(
                    state,
                    instrument=instrument,
                    run_id=run_id,
                    dry_run=dry_run,
                )
                continue
            event = FinancialDisclosureEvent(
                instrument_id=str(state.get("instrument_id") or ""),
                report_period=str(state.get("report_period") or ""),
                classification=str(
                    state.get("classification") or FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
                ),
                reasons=list(state.get("selection_reasons") or ["pending_recheck"]),
                announcement_id=str(state.get("announcement_id") or "pending"),
                announcement_time=state.get("announcement_time"),
                title=state.get("title"),
            )
            candidate = self._candidate_for_event(instrument, event)
            candidates.setdefault(candidate.key, candidate).events.append(event)
        if report_periods:
            for instrument in instruments:
                for report_period in report_periods:
                    if max_candidates > 0 and len(candidates) >= max_candidates:
                        return dict(list(candidates.items())[:max_candidates])
                    candidate = self._candidate_for_period(instrument, report_period)
                    if candidate.key in candidates:
                        continue
                    readiness = self._readiness_for_candidate(
                        candidate,
                        required_core_facts=self._required_core_facts_for_profile(
                            candidate.profile,
                            required_core_facts,
                        ),
                        mapping_version=mapping_version,
                    )
                    if not readiness.get("ready"):
                        audit_event = self._disclosure_risk_event_for_candidate(
                            candidate,
                            risk_audits_by_instrument.get(candidate.instrument_id, []),
                        )
                        if audit_event is not None:
                            candidate.events.append(audit_event)
                            candidate.reasons.extend(
                                reason
                                for reason in audit_event.reasons
                                if reason not in candidate.reasons
                            )
                        elif candidate.lifecycle_classification:
                            candidate.reasons.append(
                                f"lifecycle:{candidate.lifecycle_classification}"
                            )
                        else:
                            candidate.reasons.append("missing_or_incomplete_local_core")
                        candidates[candidate.key] = candidate
        self._last_candidate_unlimited_count = len(candidates)
        self._last_candidate_limit = int(max_candidates)
        limited = self._limit_candidates_balanced(candidates, max_candidates)
        self._last_candidate_source_summary = self._summarize_candidate_sources(
            limited.values(),
            filtered_stale_pending=filtered_stale_pending,
        )
        return limited

    @staticmethod
    def _limit_candidates_balanced(
        candidates: Mapping[Tuple[str, str], FinancialDisclosureMaintenanceCandidate],
        max_candidates: int,
    ) -> Dict[Tuple[str, str], FinancialDisclosureMaintenanceCandidate]:
        if max_candidates <= 0 or len(candidates) <= max_candidates:
            return dict(candidates)
        grouped: Dict[Tuple[str, str, str], List[FinancialDisclosureMaintenanceCandidate]] = {}
        for candidate in sorted(
            candidates.values(),
            key=lambda item: (
                item.exchange,
                item.profile,
                item.report_period,
                item.instrument_id,
            ),
        ):
            grouped.setdefault(
                (candidate.exchange, candidate.profile, candidate.report_period),
                [],
            ).append(candidate)
        selected: Dict[Tuple[str, str], FinancialDisclosureMaintenanceCandidate] = {}
        keys = sorted(grouped)
        while keys and len(selected) < max_candidates:
            next_keys: List[Tuple[str, str, str]] = []
            for key in keys:
                bucket = grouped[key]
                if bucket and len(selected) < max_candidates:
                    candidate = bucket.pop(0)
                    selected[candidate.key] = candidate
                if bucket:
                    next_keys.append(key)
            keys = next_keys
        return selected

    @staticmethod
    def _summarize_candidate_sources(
        candidates: Sequence[FinancialDisclosureMaintenanceCandidate],
        *,
        filtered_stale_pending: int,
    ) -> Dict[str, int]:
        summary = {
            "new_event": 0,
            "pending_state": 0,
            "local_gap": 0,
            "filtered_stale_pending": filtered_stale_pending,
        }
        for candidate in candidates:
            if candidate.events:
                if any(
                    event.announcement_id == "pending"
                    or "pending_recheck" in event.reasons
                    or "pending_delisting_risk" in event.reasons
                    for event in candidate.events
                ):
                    summary["pending_state"] += 1
                else:
                    summary["new_event"] += 1
            elif "missing_or_incomplete_local_core" in candidate.reasons:
                summary["local_gap"] += 1
        return summary

    @staticmethod
    def _is_stale_filtered_pending_state(state: Mapping[str, Any]) -> bool:
        title = str(state.get("title") or "")
        if not title:
            return False
        current_reasons = financial_disclosure_event_filter(
            CninfoAnnouncementRecord(
                announcement_id=str(state.get("announcement_id") or "pending"),
                title=title,
                announcement_time=state.get("announcement_time"),
                market=str(state.get("exchange") or ""),
                column=str(state.get("exchange") or "").lower(),
                symbols=[str(state.get("symbol") or "")],
            )
        )
        if not current_reasons:
            return True
        if any(
            reason in current_reasons
            for reason in ("pending_delisting_risk", "periodic_report_delayed")
        ):
            return False
        report_period = str(state.get("report_period") or "")
        return report_period not in infer_report_periods_from_title(title)

    def _record_filtered_stale_pending_state(
        self,
        state: Mapping[str, Any],
        *,
        instrument: Mapping[str, Any],
        run_id: Optional[int],
        dry_run: bool,
    ) -> None:
        if dry_run:
            return
        now = get_shanghai_time().isoformat()
        reasons = list(state.get("selection_reasons") or [])
        if "filtered_by_current_announcement_rules" not in reasons:
            reasons.append("filtered_by_current_announcement_rules")
        with self.storage.financial_database_scope():
            self.storage.upsert_financial_disclosure_event_state(
                instrument_id=str(state.get("instrument_id") or ""),
                report_period=str(state.get("report_period") or ""),
                announcement_id=str(state.get("announcement_id") or "pending"),
                symbol=str(state.get("symbol") or instrument.get("symbol") or ""),
                exchange=str(state.get("exchange") or instrument.get("exchange") or ""),
                status="filtered_stale_noise",
                classification=str(state.get("classification") or "stale_noise"),
                title=state.get("title"),
                announcement_time=state.get("announcement_time"),
                selection_reasons=reasons,
                missing_fields=list(state.get("missing_fields") or []),
                first_pending_at=state.get("first_pending_at"),
                pending_recheck_until=None,
                processed_at=now,
                metadata={
                    **dict(state.get("metadata") or {}),
                    "filtered_stale_pending": True,
                    "filter_version": "financial_disclosure_event_filter.current",
                },
                ingestion_run_id=run_id,
            )

    def _candidate_for_event(
        self,
        instrument: Mapping[str, Any],
        event: FinancialDisclosureEvent,
    ) -> FinancialDisclosureMaintenanceCandidate:
        candidate = self._candidate_for_period(instrument, event.report_period)
        candidate.reasons = list(event.reasons)
        candidate.events = [event]
        return candidate

    def _candidate_for_period(
        self,
        instrument: Mapping[str, Any],
        report_period: str,
    ) -> FinancialDisclosureMaintenanceCandidate:
        instrument_id = str(instrument.get("instrument_id") or "")
        resolution = resolve_financial_statement_profile(instrument=instrument)
        return FinancialDisclosureMaintenanceCandidate(
            instrument_id=instrument_id,
            symbol=str(instrument.get("symbol") or instrument_id.split(".")[0]),
            exchange=str(instrument.get("exchange") or ""),
            report_period=report_period,
            profile=resolution.profile,
            lifecycle_classification=self._classify_report_period_lifecycle(
                instrument,
                report_period,
            ),
        )

    def _load_disclosure_risk_audits_by_instrument(
        self,
        instrument_ids: Sequence[str],
    ) -> Dict[str, List[Mapping[str, Any]]]:
        loader = getattr(self.storage, "list_cninfo_announcement_audit", None)
        if loader is None:
            return {}
        with self.storage.financial_database_scope():
            rows = loader(
                purpose_key=self.purpose_key,
                instrument_ids=[str(item) for item in instrument_ids if str(item)],
            )
        result: Dict[str, List[Mapping[str, Any]]] = {}
        accepted_reasons = {"pending_delisting_risk", "periodic_report_delayed"}
        for row in rows:
            reasons = {str(item) for item in row.get("selection_reasons") or []}
            if not reasons & accepted_reasons:
                continue
            instrument_id = str(row.get("instrument_id") or "")
            if not instrument_id:
                continue
            result.setdefault(instrument_id, []).append(row)
        return result

    def _disclosure_risk_event_for_candidate(
        self,
        candidate: FinancialDisclosureMaintenanceCandidate,
        audits: Sequence[Mapping[str, Any]],
    ) -> Optional[FinancialDisclosureEvent]:
        period_end = self._parse_date_text(candidate.report_period)
        if period_end is None:
            return None
        for audit in audits:
            announcement_time = self._parse_date_text(audit.get("announcement_time"))
            if announcement_time is None:
                continue
            days_after_period = (announcement_time.date() - period_end.date()).days
            if days_after_period < 0 or days_after_period > 180:
                continue
            reasons = [str(item) for item in audit.get("selection_reasons") or []]
            classification = (
                PENDING_DELISTING_RISK_CLASSIFICATION
                if "pending_delisting_risk" in reasons
                else FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
            )
            return FinancialDisclosureEvent(
                instrument_id=candidate.instrument_id,
                report_period=candidate.report_period,
                classification=classification,
                reasons=reasons,
                announcement_id=str(audit.get("announcement_id") or "audit-risk"),
                announcement_time=audit.get("announcement_time"),
                title=audit.get("title"),
            )
        return None

    @staticmethod
    def _parse_date_text(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text[:26], fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @classmethod
    def _classify_report_period_lifecycle(
        cls,
        instrument: Mapping[str, Any],
        report_period: str,
    ) -> Optional[str]:
        period_end = cls._parse_date_text(report_period)
        if period_end is None:
            return None
        listed_at = cls._parse_date_text(instrument.get("listed_date"))
        if listed_at is not None and period_end.date() < listed_at.date():
            return "pre_listing_period"
        delisted_at = cls._parse_date_text(instrument.get("delisted_date"))
        if delisted_at is not None and period_end.date() > delisted_at.date():
            return "post_delisting_or_no_disclosure"
        return None

    async def _apply_candidates(
        self,
        *,
        candidates: Sequence[FinancialDisclosureMaintenanceCandidate],
        required_core_facts: Sequence[str],
        mapping_version: str,
        db_path: Path,
        request_interval_seconds: float,
        request_timeout_seconds: float,
        pending_recheck_days: int,
        run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[str, Any]:
        before_ready = {
            candidate.key: self._readiness_for_candidate(
                candidate,
                required_core_facts=self._required_core_facts_for_profile(
                    candidate.profile,
                    required_core_facts,
                ),
                mapping_version=mapping_version,
            )
            for candidate in candidates
        }
        to_fetch = [
            candidate
            for candidate in candidates
            if not before_ready[candidate.key].get("ready")
            and not self._has_mapping_policy_gap(before_ready[candidate.key])
            and candidate.classification
            not in ACCEPTED_MAINTENANCE_GAP_CLASSIFICATIONS
        ]
        unchanged = sum(
            1
            for candidate in candidates
            if before_ready[candidate.key].get("ready")
        )
        written = 0
        failed = 0
        blocking = 0
        accepted = 0
        pending = 0
        pending_delisting = 0
        mapping_policy = 0
        source_missing = 0
        changed = 0
        lifecycle_summary = {
            "pre_listing": 0,
            "post_delisting": 0,
            "disclosure_events": 0,
        }
        outcomes: List[Dict[str, Any]] = []
        if to_fetch and not dry_run:
            try:
                repair_summary = await self._run_targeted_import(
                    candidates=to_fetch,
                    required_core_facts=required_core_facts,
                    mapping_version=mapping_version,
                    db_path=db_path,
                    request_interval_seconds=request_interval_seconds,
                    request_timeout_seconds=request_timeout_seconds,
                )
                self._last_repair_source_summary = repair_summary
            except Exception as exc:
                dm_logger.warning(
                    "[FinancialDisclosureIncremental] targeted import failed: %s",
                    exc,
                )
                failed = len(to_fetch)
                self._last_repair_source_summary = self.repair_router.default_summary()
                self._last_repair_source_summary["errors"] = [str(exc)]
        for candidate in candidates:
            before = before_ready[candidate.key]
            after = self._readiness_for_candidate(
                candidate,
                required_core_facts=self._required_core_facts_for_profile(
                    candidate.profile,
                    required_core_facts,
                ),
                mapping_version=mapping_version,
            )
            status = "unchanged" if before.get("ready") else "pending_recheck"
            if after.get("ready") and not before.get("ready"):
                status = "changed"
                changed += 1
                written += 1
            elif after.get("ready"):
                status = "unchanged"
            elif failed and candidate in to_fetch:
                status = "failed"
            elif candidate.classification in ACCEPTED_LIFECYCLE_GAP_CLASSIFICATIONS:
                status = "accepted_disclosure_gap"
                accepted += 1
                if candidate.classification == "pre_listing_period":
                    lifecycle_summary["pre_listing"] += 1
                elif candidate.classification == "post_delisting_or_no_disclosure":
                    lifecycle_summary["post_delisting"] += 1
            elif candidate.classification == PENDING_DELISTING_RISK_CLASSIFICATION:
                status = "pending_delisting_risk"
                pending_delisting += 1
                accepted += 1
                lifecycle_summary["disclosure_events"] += 1
            elif candidate.classification in ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS:
                status = "accepted_disclosure_gap"
                accepted += 1
                lifecycle_summary["disclosure_events"] += 1
            elif self._has_mapping_policy_gap(after):
                status = "mapping_policy_gap"
                mapping_policy += 1
            elif candidate.events:
                status = "pending_recheck"
                pending += 1
            else:
                status = "blocking_gap"
                blocking += 1
                if self._has_source_missing_gap(after):
                    source_missing += 1
            if status == "failed":
                failed += 0
            outcome = self._record_candidate_state(
                candidate=candidate,
                status=status,
                readiness=after,
                pending_recheck_days=pending_recheck_days,
                run_id=run_id,
                dry_run=dry_run,
            )
            outcomes.append(outcome)
        return {
            "changed_count": changed,
            "unchanged_count": unchanged,
            "pending_recheck_count": pending,
            "pending_delisting_risk_count": pending_delisting,
            "accepted_gap_count": accepted,
            "mapping_policy_gap_count": mapping_policy,
            "source_missing_gap_count": source_missing,
            "blocking_gap_count": blocking,
            "failed_count": failed,
            "written_count": written,
            "report_period_lifecycle_summary": lifecycle_summary,
            "source_routing": self._last_repair_source_summary
            or self.repair_router.default_summary(),
            "outcomes": outcomes[:50],
        }

    async def _run_targeted_import(
        self,
        *,
        candidates: Sequence[FinancialDisclosureMaintenanceCandidate],
        required_core_facts: Sequence[str],
        mapping_version: str,
        db_path: Path,
        request_interval_seconds: float,
        request_timeout_seconds: float,
    ) -> Dict[str, Any]:
        merged = self.repair_router.default_summary()
        grouped: Dict[Tuple[str, ...], List[FinancialDisclosureMaintenanceCandidate]] = {}
        for candidate in candidates:
            profile_required = tuple(
                self._required_core_facts_for_profile(
                    candidate.profile,
                    required_core_facts,
                )
            )
            grouped.setdefault(profile_required, []).append(candidate)
        for profile_required, group in grouped.items():
            summary = await self.repair_router.repair_targets(
                targets=[
                    self._repair_target_for_candidate(candidate)
                    for candidate in group
                ],
                required_core_facts=list(profile_required),
                mapping_version=mapping_version,
                db_path=db_path,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
                accepted_gap_classifications=ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS,
            )
            merged = self._merge_repair_summaries(merged, summary)
        return merged

    def _readiness_for_candidate(
        self,
        candidate: FinancialDisclosureMaintenanceCandidate,
        *,
        required_core_facts: Sequence[str],
        mapping_version: str,
    ) -> Dict[str, Any]:
        return self.repair_router.readiness_for_target(
            self._repair_target_for_candidate(candidate),
            required_core_facts=required_core_facts,
            mapping_version=mapping_version,
        )

    def _required_core_facts_for_profile(
        self,
        profile: str,
        default_required_core_facts: Sequence[str],
    ) -> List[str]:
        module_cfg = self.research_config.modules.get("financial_statements", {})
        readiness_cfg = module_cfg.get("readiness", {})
        by_profile = readiness_cfg.get("required_core_facts_by_profile")
        if isinstance(by_profile, Mapping):
            profile_required = by_profile.get(profile)
            if isinstance(profile_required, Sequence) and not isinstance(profile_required, str):
                return [str(item) for item in profile_required if str(item)]
        return [str(item) for item in default_required_core_facts if str(item)]

    @staticmethod
    def _missing_reasons(readiness: Mapping[str, Any]) -> set[str]:
        return {
            str(item.get("reason") or "")
            for item in readiness.get("missing_fields") or []
            if str(item.get("reason") or "")
        }

    @classmethod
    def _has_mapping_policy_gap(cls, readiness: Mapping[str, Any]) -> bool:
        return bool(cls._missing_reasons(readiness) & MAPPING_POLICY_GAP_REASONS)

    @classmethod
    def _has_source_missing_gap(cls, readiness: Mapping[str, Any]) -> bool:
        reasons = cls._missing_reasons(readiness)
        return bool(reasons) and not bool(reasons - {"missing_local_core_fact"})

    @staticmethod
    def _merge_repair_summaries(
        left: Dict[str, Any],
        right: Dict[str, Any],
    ) -> Dict[str, Any]:
        merged = dict(left)
        for key in (
            "cninfo_attempts",
            "cninfo_successes",
            "cninfo_batch_successes",
            "cninfo_failed_instrument_periods",
            "cninfo_missing_or_ambiguous",
            "fallback_attempts",
            "fallback_successes",
        ):
            merged[key] = int(merged.get(key, 0) or 0) + int(right.get(key, 0) or 0)
        merged["errors"] = list(merged.get("errors") or []) + list(right.get("errors") or [])
        merged["source_order"] = list(right.get("source_order") or merged.get("source_order") or [])
        merged["fallback_sources"] = list(
            right.get("fallback_sources") or merged.get("fallback_sources") or []
        )
        return merged

    @staticmethod
    def _repair_target_for_candidate(
        candidate: FinancialDisclosureMaintenanceCandidate,
    ) -> FinancialMaintenanceRepairTarget:
        return FinancialMaintenanceRepairTarget(
            instrument_id=candidate.instrument_id,
            symbol=candidate.symbol,
            exchange=candidate.exchange,
            report_period=candidate.report_period,
            profile=candidate.profile,
            classification=candidate.classification if candidate.events else "local_core_gap",
        )

    def _record_candidate_state(
        self,
        *,
        candidate: FinancialDisclosureMaintenanceCandidate,
        status: str,
        readiness: Mapping[str, Any],
        pending_recheck_days: int,
        run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[str, Any]:
        now = get_shanghai_time()
        pending_until = None
        first_pending = None
        if status in {"pending_recheck", "pending_delisting_risk"}:
            first_pending = now.isoformat()
            pending_until = (now + timedelta(days=pending_recheck_days)).isoformat()
        event = candidate.events[0] if candidate.events else None
        local_gap_announcement_id = f"local-gap:{candidate.instrument_id}:{candidate.report_period}"
        announcement_id = (
            event.announcement_id
            if event and event.announcement_id
            else local_gap_announcement_id
        )
        outcome = {
            "instrument_id": candidate.instrument_id,
            "report_period": candidate.report_period,
            "status": status,
            "classification": (
                candidate.classification
                if candidate.events or candidate.lifecycle_classification
                else "local_core_gap"
            ),
            "missing_field_count": len(readiness.get("missing_fields") or []),
        }
        if not dry_run:
            with self.storage.financial_database_scope():
                self.storage.upsert_financial_disclosure_event_state(
                    instrument_id=candidate.instrument_id,
                    report_period=candidate.report_period,
                    announcement_id=str(announcement_id),
                    symbol=candidate.symbol,
                    exchange=candidate.exchange,
                    status=status,
                    classification=outcome["classification"],
                    title=None if event is None else event.title,
                    announcement_time=None if event is None else event.announcement_time,
                    selection_reasons=list(candidate.reasons),
                    missing_fields=list(readiness.get("missing_fields") or []),
                    first_pending_at=first_pending,
                    pending_recheck_until=pending_until,
                    processed_at=now.isoformat(),
                    metadata={
                        "event_count": len(candidate.events),
                        "lifecycle_classification": candidate.lifecycle_classification,
                    },
                    ingestion_run_id=run_id,
                )
                if (
                    str(announcement_id) != local_gap_announcement_id
                    and status
                    in {
                        "accepted_disclosure_gap",
                        "pending_delisting_risk",
                        "changed",
                        "unchanged",
                    }
                ):
                    self.storage.delete_financial_disclosure_event_state(
                        instrument_id=candidate.instrument_id,
                        report_period=candidate.report_period,
                        announcement_id=local_gap_announcement_id,
                        statuses=["blocking_gap", "mapping_policy_gap", "source_missing"],
                    )
        return outcome

    @staticmethod
    def _resolve_report_periods(
        *,
        report_periods: Optional[Sequence[str]],
        period_window: str,
        rolling_quarters: int,
        baseline_report_period: str,
        latest_report_period: Optional[str],
    ) -> List[str]:
        if report_periods:
            return [str(item) for item in report_periods if str(item)]
        if period_window != "latest":
            return []
        return build_financial_report_periods(
            baseline_report_period=baseline_report_period,
            rolling_min_quarters=rolling_quarters,
            latest_report_period=latest_report_period,
        )

    @staticmethod
    def _derive_status(
        *,
        candidate_count: int,
        failed_count: int,
        blocking_count: int,
        mapping_policy_gap_count: int = 0,
        scan_errors: Sequence[str],
    ) -> str:
        if scan_errors and candidate_count <= 0:
            return "failed"
        if failed_count or blocking_count or mapping_policy_gap_count or scan_errors:
            return "degraded"
        return "success"
