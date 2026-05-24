"""
Daily incremental shareholder sync driven by reusable CNInfo announcements.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from research.providers import ShareholderProviderRegistry
from research.providers.base import ShareholderSnapshot
from research.providers.cninfo_announcements import (
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanner,
)
from research.shareholder_announcement_filters import (
    ShareholderAnnouncementCandidate,
    build_shareholder_announcement_candidates,
    build_shareholder_symbol_index,
    shareholder_announcement_filter,
)
from research.shareholder_sync import ShareholderExchangeSyncResult, ShareholderShadowSyncService
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager
from utils.date_utils import get_shanghai_time


class ShareholderIncrementalSyncService:
    """Run shareholder incremental sync using CNInfo announcement candidates."""

    job_name = "shareholder_incremental_sync"
    purpose_key = "shareholder_incremental_sync"

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        resolver: Optional[ResearchSourcePolicyResolver] = None,
        registry: Optional[ShareholderProviderRegistry] = None,
        announcement_scanner: Optional[CninfoAnnouncementScanner] = None,
    ) -> None:
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.resolver = resolver or ResearchSourcePolicyResolver(self.research_config)
        self.registry = registry or ShareholderProviderRegistry(
            research_config=self.research_config,
        )
        self.announcement_scanner = announcement_scanner or self._build_scanner()
        self.shadow_helper = ShareholderShadowSyncService(
            db_ops=db_ops,
            storage=storage,
            research_config=self.research_config,
            resolver=self.resolver,
            registry=self.registry,
        )

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
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        started_at = time.monotonic()
        module_cfg = self.research_config.modules.get("shareholders", {})
        incremental_cfg = module_cfg.get("incremental_sync", {})
        required_scope = {
            str(scope).strip()
            for scope in module_cfg.get("allowed_scope", [])
            if str(scope).strip()
        }
        target_exchanges = exchanges or self.research_config.markets
        lookback = int(
            incremental_cfg.get("lookback_days", 7)
            if lookback_days is None
            else lookback_days
        )
        overlap = int(
            incremental_cfg.get("overlap_days", 2)
            if overlap_days is None
            else overlap_days
        )
        scan_page_size = int(
            incremental_cfg.get("page_size", 30) if page_size is None else page_size
        )
        max_pages = int(
            incremental_cfg.get("max_pages_per_market", 20)
            if max_pages_per_market is None
            else max_pages_per_market
        )
        candidate_limit = int(
            incremental_cfg.get("max_candidates", 300)
            if max_candidates is None
            else max_candidates
        )
        recheck_days = int(
            incremental_cfg.get("pending_recheck_days", 2)
            if pending_recheck_days is None
            else pending_recheck_days
        )

        run_id: Optional[int] = None
        if not dry_run:
            run_id = self.storage.start_ingestion_run(
                domain="shareholders",
                job_name=self.job_name,
                market=",".join(target_exchanges),
                metadata={
                    "exchanges": target_exchanges,
                    "lookback_days": lookback,
                    "overlap_days": overlap,
                    "page_size": scan_page_size,
                    "max_pages_per_market": max_pages,
                    "max_candidates": candidate_limit,
                    "dry_run": dry_run,
                },
            )
        dm_logger.info(
            "[ShareholderIncremental] Run started: exchanges=%s run_id=%s dry_run=%s",
            target_exchanges,
            run_id,
            dry_run,
        )

        try:
            instruments_by_exchange = await self._load_active_instruments(target_exchanges)
            all_instruments = [
                instrument
                for instruments in instruments_by_exchange.values()
                for instrument in instruments
            ]
            instrument_ids = [
                str(instrument.get("instrument_id"))
                for instrument in all_instruments
                if instrument.get("instrument_id")
            ]
            existing_snapshots = self.storage.get_shareholder_snapshots(instrument_ids)
            existing_manifests = self.storage.get_shareholder_change_manifests(instrument_ids)

            scan_result = self._scan_announcements(
                exchanges=target_exchanges,
                lookback_days=lookback,
                overlap_days=overlap,
                page_size=scan_page_size,
                max_pages_per_market=max_pages,
                all_instruments=all_instruments,
                run_id=run_id,
                dry_run=dry_run,
            )

            candidates = dict(scan_result["candidates"])
            self._add_missing_and_pending_candidates(
                candidates=candidates,
                instruments=all_instruments,
                existing_snapshots=existing_snapshots,
                required_scope=required_scope,
                max_candidates=candidate_limit,
            )
            candidates = self._limit_candidates(candidates, candidate_limit)
            candidates_by_exchange = self._group_candidates_by_exchange(
                candidates,
                instruments_by_exchange,
            )

            exchange_results: List[ShareholderExchangeSyncResult] = []
            merged_snapshots: Dict[str, ShareholderSnapshot] = {}
            attempted_sources: Set[str] = set()
            successful_sources: Set[str] = set()
            for exchange, exchange_candidates in candidates_by_exchange.items():
                exchange_result, exchange_snapshots = await self._fetch_candidate_snapshots(
                    exchange=exchange,
                    candidate_instruments=exchange_candidates,
                    required_scope=required_scope,
                    budget_mode=budget_mode,
                    allow_paid_proxy=allow_paid_proxy,
                )
                exchange_results.append(exchange_result)
                merged_snapshots.update(exchange_snapshots)
                attempted_sources.update(exchange_result.attempted_sources)
                successful_sources.update(exchange_result.successful_sources)

            write_result = self._apply_candidate_snapshots(
                candidates=candidates,
                snapshots=merged_snapshots,
                existing_snapshots=existing_snapshots,
                existing_manifests=existing_manifests,
                required_scope=required_scope,
                pending_recheck_days=recheck_days,
                run_id=run_id,
                dry_run=dry_run,
            )

            status = self._derive_status(
                candidate_count=len(candidates),
                failure_count=write_result["failed_instruments"],
                scan_errors=scan_result["errors"],
            )
            metadata = {
                "exchanges": target_exchanges,
                "dry_run": dry_run,
                "announcements_scanned": scan_result["announcements_scanned"],
                "selected_announcements": scan_result["selected_announcements"],
                "pages_scanned": scan_result["pages_scanned"],
                "candidate_instruments": len(candidates),
                "changed_instruments": write_result["changed_instruments"],
                "unchanged_instruments": write_result["unchanged_instruments"],
                "pending_rechecks": write_result["pending_rechecks"],
                "failed_instruments": write_result["failed_instruments"],
                "attempted_sources": sorted(attempted_sources),
                "successful_sources": sorted(successful_sources),
                "scan_errors": scan_result["errors"][:10],
                "elapsed_seconds": round(time.monotonic() - started_at, 3),
            }
            if run_id is not None:
                self.storage.finish_ingestion_run(
                    run_id,
                    status=status,
                    rows_written=write_result["snapshots_written"],
                    error_message=(
                        "; ".join(scan_result["errors"][:3])
                        if status == "failed"
                        else None
                    ),
                    metadata=metadata,
                )
            dm_logger.info(
                "[ShareholderIncremental] Run finished: status=%s candidates=%s changed=%s unchanged=%s pending=%s failed=%s elapsed=%.1fs",
                status,
                len(candidates),
                write_result["changed_instruments"],
                write_result["unchanged_instruments"],
                write_result["pending_rechecks"],
                write_result["failed_instruments"],
                time.monotonic() - started_at,
            )
            return {
                "status": status,
                "job_name": self.job_name,
                "dry_run": dry_run,
                "exchanges": [asdict(result) for result in exchange_results],
                **metadata,
                "snapshots_written": write_result["snapshots_written"],
                "would_write_snapshots": write_result["would_write_snapshots"],
                "failed_instrument_ids": write_result["failed_instrument_ids"],
            }
        except Exception as exc:
            if run_id is not None:
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
        shareholder_cfg = cninfo_cfg.get("shareholders", {})
        return CninfoAnnouncementScanner(
            request_timeout_seconds=float(
                scan_cfg.get(
                    "request_timeout_seconds",
                    shareholder_cfg.get("request_timeout_seconds", 20.0),
                )
            ),
            request_interval_seconds=float(
                scan_cfg.get(
                    "request_interval_seconds",
                    shareholder_cfg.get("request_interval_seconds", 0.2),
                )
            ),
            retry_attempts=int(
                scan_cfg.get("retry_attempts", shareholder_cfg.get("retry_attempts", 2))
            ),
            retry_backoff_seconds=float(
                scan_cfg.get(
                    "retry_backoff_seconds",
                    shareholder_cfg.get("retry_backoff_seconds", 0.5),
                )
            ),
        )

    async def _load_active_instruments(
        self,
        exchanges: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        result: Dict[str, List[Dict[str, Any]]] = {}
        for exchange in exchanges:
            instruments = await self.db_ops.get_instruments_by_exchange(exchange)
            result[exchange] = [
                instrument
                for instrument in instruments
                if instrument.get("type") == "stock" and instrument.get("is_active", True)
            ]
        return result

    def _scan_announcements(
        self,
        *,
        exchanges: List[str],
        lookback_days: int,
        overlap_days: int,
        page_size: int,
        max_pages_per_market: int,
        all_instruments: List[Dict[str, Any]],
        run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[str, Any]:
        now = get_shanghai_time()
        start_date = (now - timedelta(days=lookback_days)).date().isoformat()
        end_date = now.date().isoformat()
        symbol_index = build_shareholder_symbol_index(all_instruments)
        market_configs = self._announcement_market_configs()
        all_selected = []
        pages_scanned = 0
        announcements_scanned = 0
        selected_announcements = 0
        errors: List[str] = []

        for exchange in exchanges:
            cfg = market_configs.get(exchange, {})
            column = str(cfg.get("column") or exchange.lower()).strip()
            market = str(cfg.get("market") or exchange).strip()
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
                search_key=cfg.get("search_key"),
                start_date=(
                    now - timedelta(days=max(lookback_days, overlap_days))
                ).date().isoformat(),
                end_date=end_date,
                page_size=page_size,
                max_pages=max_pages_per_market,
                stop_at_watermark=watermark,
            )
            scan_started_at = get_shanghai_time().isoformat()
            result = self.announcement_scanner.scan(
                config,
                filters=[shareholder_announcement_filter],
            )
            scan_completed_at = get_shanghai_time().isoformat()
            pages_scanned += result.pages_scanned
            announcements_scanned += result.announcements_seen
            selected_announcements += len(result.selected_records)
            all_selected.extend(result.selected_records)
            errors.extend(result.errors)
            if not dry_run:
                new_watermark = result.max_announcement_time or watermark
                self.storage.upsert_cninfo_announcement_scan_state(
                    purpose_key=self.purpose_key,
                    market=market,
                    column=column,
                    last_watermark=new_watermark,
                    last_scan_started_at=scan_started_at,
                    last_scan_completed_at=scan_completed_at,
                    pages_scanned=result.pages_scanned,
                    announcements_seen=result.announcements_seen,
                    selected_announcements=len(result.selected_records),
                    status="success" if not result.errors else "degraded",
                    metadata={
                        "start_date": start_date,
                        "end_date": end_date,
                        "stopped_at_watermark": result.stopped_at_watermark,
                        "errors": result.errors[:5],
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

        candidates = build_shareholder_announcement_candidates(
            all_selected,
            symbol_index,
        )
        return {
            "candidates": candidates,
            "pages_scanned": pages_scanned,
            "announcements_scanned": announcements_scanned,
            "selected_announcements": selected_announcements,
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

    def _add_missing_and_pending_candidates(
        self,
        *,
        candidates: Dict[str, ShareholderAnnouncementCandidate],
        instruments: List[Dict[str, Any]],
        existing_snapshots: Dict[str, Dict[str, Any]],
        required_scope: Set[str],
        max_candidates: int,
    ) -> None:
        for instrument in instruments:
            if max_candidates > 0 and len(candidates) >= max_candidates:
                break
            instrument_id = str(instrument.get("instrument_id") or "").strip()
            if not instrument_id or instrument_id in candidates:
                continue
            snapshot = existing_snapshots.get(instrument_id)
            if not self._snapshot_dict_covers_scope(snapshot, required_scope):
                candidates[instrument_id] = ShareholderAnnouncementCandidate(
                    instrument_id=instrument_id,
                    symbol=str(instrument.get("symbol") or "").strip(),
                    exchange=str(instrument.get("exchange") or "").strip(),
                    reasons=["missing_required_scope"],
                )

        pending = self.storage.list_pending_shareholder_rechecks(
            limit=max_candidates if max_candidates > 0 else None
        )
        for manifest in pending:
            if max_candidates > 0 and len(candidates) >= max_candidates:
                break
            instrument_id = str(manifest.get("instrument_id") or "").strip()
            if not instrument_id or instrument_id in candidates:
                continue
            instrument = next(
                (
                    item
                    for item in instruments
                    if str(item.get("instrument_id") or "").strip() == instrument_id
                ),
                None,
            )
            if instrument is None:
                continue
            candidates[instrument_id] = ShareholderAnnouncementCandidate(
                instrument_id=instrument_id,
                symbol=str(instrument.get("symbol") or "").strip(),
                exchange=str(instrument.get("exchange") or "").strip(),
                reasons=["pending_recheck"],
                latest_announcement_time=manifest.get("last_announcement_time"),
            )

    @staticmethod
    def _limit_candidates(
        candidates: Dict[str, ShareholderAnnouncementCandidate],
        max_candidates: int,
    ) -> Dict[str, ShareholderAnnouncementCandidate]:
        if max_candidates <= 0 or len(candidates) <= max_candidates:
            return candidates
        ordered = sorted(
            candidates.values(),
            key=lambda item: (
                "missing_required_scope" not in item.reasons,
                item.latest_announcement_time or "",
                item.instrument_id,
            ),
        )
        return {item.instrument_id: item for item in ordered[:max_candidates]}

    @staticmethod
    def _group_candidates_by_exchange(
        candidates: Dict[str, ShareholderAnnouncementCandidate],
        instruments_by_exchange: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        candidate_ids = set(candidates)
        result: Dict[str, List[Dict[str, Any]]] = {}
        for exchange, instruments in instruments_by_exchange.items():
            selected = [
                instrument
                for instrument in instruments
                if str(instrument.get("instrument_id") or "") in candidate_ids
            ]
            if selected:
                result[exchange] = selected
        return result

    async def _fetch_candidate_snapshots(
        self,
        *,
        exchange: str,
        candidate_instruments: List[Dict[str, Any]],
        required_scope: Set[str],
        budget_mode: Optional[str],
        allow_paid_proxy: Optional[bool],
    ) -> tuple[ShareholderExchangeSyncResult, Dict[str, ShareholderSnapshot]]:
        if not candidate_instruments:
            return (
                ShareholderExchangeSyncResult(exchange=exchange, status="skipped"),
                {},
            )
        plan = self.resolver.resolve(
            "shareholders",
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        remaining_ids = {
            str(instrument.get("instrument_id") or "")
            for instrument in candidate_instruments
        }
        merged_snapshots: Dict[str, ShareholderSnapshot] = {}
        attempted_sources: List[str] = []
        successful_sources: List[str] = []
        primary_source: Optional[str] = None
        primary_mode: Optional[str] = None

        for candidate in plan.candidates:
            candidate_key = f"{candidate.source}:{candidate.mode}"
            if not remaining_ids:
                break
            provider = self.registry.get(candidate.source)
            if provider is None or not provider.supports_mode(candidate.mode):
                continue
            fetch_instruments = [
                instrument
                for instrument in candidate_instruments
                if str(instrument.get("instrument_id") or "") in remaining_ids
            ]
            if not fetch_instruments:
                continue
            attempted_sources.append(candidate_key)
            try:
                snapshots = await provider.fetch_shareholder_snapshots(
                    instruments=fetch_instruments,
                    exchange=exchange,
                    mode=candidate.mode,
                    limit=len(fetch_instruments),
                )
            except Exception as exc:
                dm_logger.warning(
                    "[ShareholderIncremental] Provider failed: exchange=%s source=%s mode=%s error=%s",
                    exchange,
                    candidate.source,
                    candidate.mode,
                    exc,
                )
                continue
            accepted = 0
            for snapshot in snapshots or []:
                instrument_id = str(snapshot.instrument_id)
                if instrument_id not in remaining_ids:
                    continue
                merged_snapshots[instrument_id] = self.shadow_helper._merge_snapshots(
                    merged_snapshots.get(instrument_id),
                    snapshot,
                )
                accepted += 1
            if accepted:
                if primary_source is None:
                    primary_source = candidate.source
                    primary_mode = candidate.mode
                successful_sources.append(candidate_key)
                remaining_ids = {
                    instrument_id
                    for instrument_id in remaining_ids
                    if not self.shadow_helper._snapshot_covers_required_scope(
                        merged_snapshots.get(instrument_id),
                        required_scope,
                    )
                }

        missing_ids = sorted(remaining_ids)
        status = "success" if not missing_ids else "degraded"
        return (
            ShareholderExchangeSyncResult(
                exchange=exchange,
                status=status,
                source=primary_source,
                mode=primary_mode,
                attempted_sources=attempted_sources,
                successful_sources=successful_sources,
                requested_instruments=len(candidate_instruments),
                resolved_instruments=len(candidate_instruments) - len(missing_ids),
                missing_instruments=len(missing_ids),
                missing_instrument_ids=missing_ids[:20],
                snapshots_written=0,
                error_message=(
                    None
                    if not missing_ids
                    else f"Missing incremental shareholder snapshots for {len(missing_ids)} instruments"
                ),
            ),
            merged_snapshots,
        )

    def _apply_candidate_snapshots(
        self,
        *,
        candidates: Dict[str, ShareholderAnnouncementCandidate],
        snapshots: Dict[str, ShareholderSnapshot],
        existing_snapshots: Dict[str, Dict[str, Any]],
        existing_manifests: Dict[str, Dict[str, Any]],
        required_scope: Set[str],
        pending_recheck_days: int,
        run_id: Optional[int],
        dry_run: bool,
    ) -> Dict[str, Any]:
        now = get_shanghai_time()
        changed = 0
        unchanged = 0
        pending = 0
        failed = 0
        written = 0
        would_write = 0
        failed_ids: List[str] = []
        for instrument_id, candidate in candidates.items():
            snapshot = snapshots.get(instrument_id)
            if snapshot is None:
                failed += 1
                failed_ids.append(instrument_id)
                if not dry_run:
                    self.storage.upsert_shareholder_change_manifest(
                        instrument_id=instrument_id,
                        symbol=candidate.symbol,
                        exchange=candidate.exchange,
                        content_hash=None,
                        top_holders_hash=None,
                        holder_count_hash=None,
                        ownership_hash=None,
                        latest_report_date=None,
                        coverage_scope=[],
                        status="failed",
                        reasons=candidate.reasons,
                        metadata={"announcement_ids": candidate.announcement_ids},
                        ingestion_run_id=run_id,
                    )
                continue

            hashes = compute_shareholder_content_hashes(snapshot.snapshot_json)
            existing_hash = self._existing_content_hash(
                instrument_id,
                existing_snapshots,
                existing_manifests,
            )
            existing_snapshot = existing_snapshots.get(instrument_id)
            existing_complete = self._snapshot_dict_covers_scope(
                existing_snapshot,
                required_scope,
            )
            content_changed = hashes["content_hash"] != existing_hash
            should_write = content_changed or not existing_complete
            has_announcement = bool(candidate.announcement_ids)
            status = "changed" if should_write else "unchanged"
            pending_until = None
            if not should_write and has_announcement and pending_recheck_days > 0:
                deadline = self._pending_recheck_deadline(
                    existing_manifests.get(instrument_id),
                    candidate,
                    now,
                    pending_recheck_days,
                )
                if deadline is not None and deadline >= now:
                    status = "pending_recheck"
                    pending_until = deadline.isoformat()
                    pending += 1
                else:
                    unchanged += 1
            elif should_write:
                changed += 1
            else:
                unchanged += 1

            if should_write:
                would_write += 1
                if not dry_run:
                    payload_hash = ShareholderShadowSyncService._hash_payload(
                        snapshot.raw_payload
                    )
                    self.storage.store_raw_payload(
                        domain="shareholders",
                        instrument_id=snapshot.instrument_id,
                        source=snapshot.source,
                        source_mode=snapshot.source_mode,
                        payload=snapshot.raw_payload,
                        payload_hash=payload_hash,
                        ingestion_run_id=run_id,
                    )
                    self.storage.upsert_shareholder_snapshot(
                        snapshot,
                        ingestion_run_id=run_id,
                    )
                    written += 1
            if not dry_run:
                self.storage.upsert_shareholder_change_manifest(
                    instrument_id=instrument_id,
                    symbol=snapshot.symbol,
                    exchange=snapshot.exchange,
                    content_hash=hashes["content_hash"],
                    top_holders_hash=hashes["top_holders_hash"],
                    holder_count_hash=hashes["holder_count_hash"],
                    ownership_hash=hashes["ownership_hash"],
                    latest_report_date=hashes.get("latest_report_date"),
                    coverage_scope=hashes["coverage_scope"],
                    status=status,
                    last_changed_at=now.isoformat() if should_write else None,
                    pending_recheck_until=pending_until,
                    last_announcement_time=candidate.latest_announcement_time,
                    reasons=candidate.reasons,
                    metadata=self._build_manifest_metadata(
                        existing_manifests.get(instrument_id),
                        candidate,
                        now,
                        pending_until,
                    ),
                    ingestion_run_id=run_id,
                )

        return {
            "changed_instruments": changed,
            "unchanged_instruments": unchanged,
            "pending_rechecks": pending,
            "failed_instruments": failed,
            "failed_instrument_ids": failed_ids[:20],
            "snapshots_written": written,
            "would_write_snapshots": would_write,
        }

    @staticmethod
    def _existing_content_hash(
        instrument_id: str,
        existing_snapshots: Dict[str, Dict[str, Any]],
        existing_manifests: Dict[str, Dict[str, Any]],
    ) -> Optional[str]:
        manifest = existing_manifests.get(instrument_id)
        if manifest and manifest.get("content_hash"):
            return str(manifest["content_hash"])
        snapshot = existing_snapshots.get(instrument_id)
        snapshot_json = None if snapshot is None else snapshot.get("snapshot")
        if isinstance(snapshot_json, dict):
            return compute_shareholder_content_hashes(snapshot_json)["content_hash"]
        return None

    @staticmethod
    def _build_manifest_metadata(
        existing_manifest: Optional[Dict[str, Any]],
        candidate: ShareholderAnnouncementCandidate,
        now: datetime,
        pending_until: Optional[str],
    ) -> Dict[str, Any]:
        announcement_ids = [str(item) for item in candidate.announcement_ids]
        metadata: Dict[str, Any] = {"announcement_ids": announcement_ids}
        if pending_until is None:
            return metadata

        existing_metadata = (
            existing_manifest.get("metadata", {})
            if isinstance(existing_manifest, dict)
            else {}
        )
        previous_ids = {
            str(item)
            for item in existing_metadata.get("announcement_ids", []) or []
        }
        current_ids = set(announcement_ids)
        if current_ids and current_ids == previous_ids:
            first_pending_at = existing_metadata.get("first_pending_at")
        else:
            first_pending_at = None
        metadata["first_pending_at"] = first_pending_at or now.isoformat()
        return metadata

    @classmethod
    def _pending_recheck_deadline(
        cls,
        existing_manifest: Optional[Dict[str, Any]],
        candidate: ShareholderAnnouncementCandidate,
        now: datetime,
        pending_recheck_days: int,
    ) -> Optional[datetime]:
        existing_metadata = (
            existing_manifest.get("metadata", {})
            if isinstance(existing_manifest, dict)
            else {}
        )
        previous_ids = {
            str(item)
            for item in existing_metadata.get("announcement_ids", []) or []
        }
        current_ids = {str(item) for item in candidate.announcement_ids}
        first_pending_at = None
        if current_ids and current_ids == previous_ids:
            first_pending_at = cls._parse_manifest_time(
                existing_metadata.get("first_pending_at")
            )
        anchor = first_pending_at or now
        return anchor + timedelta(days=pending_recheck_days)

    @staticmethod
    def _parse_manifest_time(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=get_shanghai_time().tzinfo)
        return parsed

    @staticmethod
    def _snapshot_dict_covers_scope(
        snapshot: Optional[Dict[str, Any]],
        required_scope: Set[str],
    ) -> bool:
        if snapshot is None:
            return False
        if not required_scope:
            return True
        snapshot_json = snapshot.get("snapshot")
        if not isinstance(snapshot_json, dict):
            return False
        scope = {
            str(item).strip()
            for item in snapshot_json.get("coverage_scope", []) or []
            if str(item).strip()
        }
        return required_scope.issubset(scope)

    @staticmethod
    def _derive_status(
        *,
        candidate_count: int,
        failure_count: int,
        scan_errors: List[str],
    ) -> str:
        if scan_errors and candidate_count <= 0:
            return "failed"
        if failure_count > 0 or scan_errors:
            return "degraded"
        return "success"


def compute_shareholder_content_hashes(snapshot_json: Dict[str, Any]) -> Dict[str, Any]:
    """Compute normalized shareholder content hashes for incremental change checks."""
    holder_count = _normalize_holder_count(snapshot_json.get("holder_count") or {})
    top_holders = _normalize_top_holders(snapshot_json.get("top_holders") or [])
    ownership = _normalize_ownership(snapshot_json.get("ownership_clues") or {})
    coverage_scope = sorted(
        {
            str(item).strip()
            for item in snapshot_json.get("coverage_scope", []) or []
            if str(item).strip()
        }
    )
    latest_report_date = max(
        [
            str(value)
            for value in [
                holder_count.get("report_date"),
                *(item.get("report_date") for item in top_holders),
                ownership.get("report_date"),
            ]
            if value
        ],
        default=None,
    )
    return {
        "holder_count_hash": _hash_json(holder_count),
        "top_holders_hash": _hash_json(top_holders),
        "ownership_hash": _hash_json(ownership),
        "content_hash": _hash_json(
            {
                "coverage_scope": coverage_scope,
                "holder_count": holder_count,
                "top_holders": top_holders,
                "ownership_clues": ownership,
            }
        ),
        "latest_report_date": latest_report_date,
        "coverage_scope": coverage_scope,
    }


def _normalize_holder_count(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "value": _to_int(value.get("value")),
        "report_date": _clean_text(value.get("report_date")),
    }


def _normalize_top_holders(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for row in values:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "rank": _to_int(row.get("rank")),
                "holder_name": _clean_text(row.get("holder_name")),
                "holding_shares": _to_int(row.get("holding_shares")),
                "holding_ratio": _to_float(row.get("holding_ratio")),
                "holder_type": _clean_text(row.get("holder_type")),
                "change": _clean_text(row.get("change")),
                "report_date": _clean_text(row.get("report_date")),
            }
        )
    rows.sort(
        key=lambda item: (
            item.get("rank") is None,
            item.get("rank") or 999,
            item.get("holder_name") or "",
        )
    )
    return rows


def _normalize_ownership(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "control_owner_name": _clean_text(value.get("control_owner_name")),
        "control_owner_ratio": _to_float(value.get("control_owner_ratio")),
        "report_date": _clean_text(value.get("report_date")),
    }


def _hash_json(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _clean_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 8)
    except (TypeError, ValueError):
        return None
