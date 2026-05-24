"""
Shareholder summary shadow sync service.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set

from research.providers import ShareholderProviderRegistry
from research.providers.base import ShareholderSnapshot
from research.empty_support import allows_optional_empty_exchange
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager


@dataclass(frozen=True)
class ShareholderExchangeSyncResult:
    """Per-exchange result for shareholder summary shadow sync."""

    exchange: str
    status: str
    source: Optional[str] = None
    mode: Optional[str] = None
    attempted_sources: List[str] = field(default_factory=list)
    successful_sources: List[str] = field(default_factory=list)
    requested_instruments: int = 0
    resolved_instruments: int = 0
    missing_instruments: int = 0
    missing_instrument_ids: List[str] = field(default_factory=list)
    snapshots_written: int = 0
    unchanged_instruments: int = 0
    error_message: Optional[str] = None


class ShareholderShadowSyncService:
    """Run shareholder summary shadow sync into research.db."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        resolver: Optional[ResearchSourcePolicyResolver] = None,
        registry: Optional[ShareholderProviderRegistry] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.resolver = resolver or ResearchSourcePolicyResolver(self.research_config)
        self.registry = registry or ShareholderProviderRegistry(
            research_config=self.research_config,
        )

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        write_policy: str = "refresh_all",
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        results: List[ShareholderExchangeSyncResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    budget_mode=budget_mode,
                    allow_paid_proxy=allow_paid_proxy,
                    write_policy=write_policy,
                )
            )

        total_written = sum(result.snapshots_written for result in results)
        success_count = sum(1 for result in results if result.status == "success")

        return {
            "status": "success" if success_count else "degraded",
            "exchanges": [asdict(result) for result in results],
            "total_snapshots_written": total_written,
            "write_policy": write_policy,
            "successful_exchanges": success_count,
            "attempted_exchanges": len(results),
        }

    async def _sync_exchange(
        self,
        *,
        exchange: str,
        limit_per_exchange: Optional[int],
        budget_mode: Optional[str],
        allow_paid_proxy: Optional[bool],
        write_policy: str,
    ) -> ShareholderExchangeSyncResult:
        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]
        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return ShareholderExchangeSyncResult(
                exchange=exchange,
                status="skipped",
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="shareholders",
            job_name="shareholder_shadow_sync",
            market=exchange,
            metadata={"instrument_count": len(stock_instruments)},
        )
        dm_logger.info(
            "[ShareholderSync] Exchange sync started: exchange=%s instruments=%s run_id=%s",
            exchange,
            len(stock_instruments),
            run_id,
        )

        attempted_sources: List[str] = []
        successful_sources: List[str] = []
        skipped_sources: List[str] = []
        optional_empty_exchange = allows_optional_empty_exchange(
            self.research_config,
            "shareholders",
            exchange,
        )
        try:
            plan = self.resolver.resolve(
                "shareholders",
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )
            dm_logger.info(
                "[ShareholderSync] Source plan resolved: exchange=%s budget_mode=%s allow_paid_proxy=%s candidates=%s",
                exchange,
                plan.budget_mode,
                plan.allow_paid_proxy,
                [f"{candidate.source}:{candidate.mode}" for candidate in plan.candidates],
            )
            module_cfg = self.research_config.modules.get("shareholders", {})
            required_scope = {
                str(scope).strip()
                for scope in module_cfg.get("allowed_scope", [])
                if str(scope).strip()
            }
            recovery_candidate_keys = {
                str(candidate_key).strip()
                for candidate_key in module_cfg.get("same_source_recovery_candidates", [])
                if str(candidate_key).strip()
            }
            force_merge_candidate_keys = {
                str(candidate_key).strip()
                for candidate_key in module_cfg.get("force_merge_candidates", [])
                if str(candidate_key).strip()
            }
            skip_same_source_full_fallback_after_success = bool(
                module_cfg.get("skip_same_source_full_fallback_after_success", False)
            )
            recovery_batch_size = max(
                1,
                int(module_cfg.get("same_source_recovery_batch_size", 1)),
            )
            recovery_max_instruments = max(
                0,
                int(module_cfg.get("same_source_recovery_max_instruments", 0)),
            )
            remaining_instruments = list(stock_instruments)
            remaining_ids = {
                str(instrument["instrument_id"])
                for instrument in stock_instruments
            }
            merged_snapshots: Dict[str, ShareholderSnapshot] = {}
            primary_source: Optional[str] = None
            primary_mode: Optional[str] = None
            recovery_attempted_instruments = 0
            recovery_resolved_instruments = 0
            recovery_runs = 0
            successful_source_names: Set[str] = set()

            all_instrument_ids = {
                str(instrument["instrument_id"])
                for instrument in stock_instruments
            }

            for candidate in plan.candidates:
                candidate_key = f"{candidate.source}:{candidate.mode}"
                force_merge_candidate = candidate_key in force_merge_candidate_keys
                candidate_instruments = (
                    stock_instruments if force_merge_candidate else remaining_instruments
                )
                if not candidate_instruments:
                    continue

                if (
                    skip_same_source_full_fallback_after_success
                    and candidate.source in successful_source_names
                    and not force_merge_candidate
                ):
                    skipped_sources.append(candidate_key)
                    dm_logger.info(
                        "[ShareholderSync] Candidate skipped: exchange=%s source=%s mode=%s reason=same_source_already_successful remaining=%s successful_sources=%s",
                        exchange,
                        candidate.source,
                        candidate.mode,
                        len(candidate_instruments),
                        successful_sources,
                    )
                    continue

                attempted_sources.append(candidate_key)
                provider = self.registry.get(candidate.source)
                if provider is None or not provider.supports_mode(candidate.mode):
                    dm_logger.info(
                        "[ShareholderSync] Candidate skipped: exchange=%s source=%s mode=%s reason=unsupported",
                        exchange,
                        candidate.source,
                        candidate.mode,
                    )
                    continue

                try:
                    candidate_started_at = time.monotonic()
                    dm_logger.info(
                        "[ShareholderSync] Candidate fetch started: exchange=%s source=%s mode=%s instruments=%s",
                        exchange,
                        candidate.source,
                        candidate.mode,
                        len(candidate_instruments),
                    )
                    snapshots = await provider.fetch_shareholder_snapshots(
                        instruments=candidate_instruments,
                        exchange=exchange,
                        mode=candidate.mode,
                        limit=len(candidate_instruments),
                    )
                    dm_logger.info(
                        "[ShareholderSync] Candidate fetch finished: exchange=%s source=%s mode=%s snapshots=%s elapsed=%.1fs",
                        exchange,
                        candidate.source,
                        candidate.mode,
                        len(snapshots or []),
                        time.monotonic() - candidate_started_at,
                    )
                except Exception as exc:
                    dm_logger.warning(
                        "[ShareholderSync] Provider %s (%s) failed for %s: %s",
                        candidate.source,
                        candidate.mode,
                        exchange,
                        exc,
                    )
                    continue

                if not snapshots:
                    dm_logger.info(
                        "[ShareholderSync] Candidate returned no snapshots: exchange=%s source=%s mode=%s remaining=%s",
                        exchange,
                        candidate.source,
                        candidate.mode,
                        len(remaining_ids),
                    )
                    continue

                accepted_snapshots = []
                accepted_ids = set()
                eligible_ids = all_instrument_ids if force_merge_candidate else remaining_ids
                for snapshot in snapshots:
                    instrument_id = str(snapshot.instrument_id)
                    if instrument_id not in eligible_ids or instrument_id in accepted_ids:
                        continue
                    accepted_ids.add(instrument_id)
                    accepted_snapshots.append(snapshot)

                if not accepted_snapshots:
                    dm_logger.info(
                        "[ShareholderSync] Candidate snapshots rejected by eligibility/dedup: exchange=%s source=%s mode=%s snapshots=%s eligible=%s",
                        exchange,
                        candidate.source,
                        candidate.mode,
                        len(snapshots),
                        len(eligible_ids),
                    )
                    continue

                dm_logger.debug(
                    "[ShareholderSync] Candidate accepted snapshots: exchange=%s source=%s mode=%s accepted=%s first_ids=%s",
                    exchange,
                    candidate.source,
                    candidate.mode,
                    len(accepted_snapshots),
                    [snapshot.instrument_id for snapshot in accepted_snapshots[:10]],
                )
                for snapshot in accepted_snapshots:
                    existing_snapshot = merged_snapshots.get(snapshot.instrument_id)
                    merged_snapshot = self._merge_snapshots(
                        existing_snapshot,
                        snapshot,
                    )
                    merged_snapshots[snapshot.instrument_id] = merged_snapshot
                    if write_policy != "changed_only":
                        payload_hash = self._hash_payload(snapshot.raw_payload)
                        self.storage.store_raw_payload(
                            domain="shareholders",
                            instrument_id=snapshot.instrument_id,
                            source=snapshot.source,
                            source_mode=snapshot.source_mode,
                            payload=snapshot.raw_payload,
                            payload_hash=payload_hash,
                            ingestion_run_id=run_id,
                        )

                if primary_source is None:
                    primary_source = candidate.source
                    primary_mode = candidate.mode
                successful_sources.append(candidate_key)
                successful_source_names.add(candidate.source)
                remaining_ids = {
                    instrument_id
                    for instrument_id in remaining_ids
                    if not self._snapshot_covers_required_scope(
                        merged_snapshots.get(instrument_id),
                        required_scope,
                    )
                }
                dm_logger.info(
                    "[ShareholderSync] Candidate processed: exchange=%s source=%s mode=%s merged=%s remaining_missing_scope=%s",
                    exchange,
                    candidate.source,
                    candidate.mode,
                    len(merged_snapshots),
                    len(remaining_ids),
                )
                if (
                    recovery_max_instruments > 0
                    and candidate_key in recovery_candidate_keys
                    and remaining_ids
                ):
                    recovery_result = await self._run_same_source_recovery(
                        provider=provider,
                        candidate_source=candidate.source,
                        candidate_mode=candidate.mode,
                        exchange=exchange,
                        stock_instruments=stock_instruments,
                        remaining_ids=remaining_ids,
                        merged_snapshots=merged_snapshots,
                        run_id=run_id,
                        batch_size=recovery_batch_size,
                        max_instruments=recovery_max_instruments,
                        required_scope=required_scope,
                        write_policy=write_policy,
                    )
                    recovery_attempted_instruments += recovery_result["attempted_instruments"]
                    recovery_resolved_instruments += recovery_result["resolved_instruments"]
                    recovery_runs += recovery_result["runs"]
                    remaining_ids = {
                        instrument_id
                        for instrument_id in remaining_ids
                        if not self._snapshot_covers_required_scope(
                            merged_snapshots.get(instrument_id),
                            required_scope,
                        )
                    }
                remaining_instruments = [
                    instrument
                    for instrument in stock_instruments
                    if str(instrument["instrument_id"]) in remaining_ids
                ]

            total_written = 0
            unchanged_instruments = 0
            existing_snapshots: Dict[str, Dict[str, Any]] = {}
            if write_policy == "changed_only" and merged_snapshots:
                existing_snapshots = self.storage.get_shareholder_snapshots(
                    list(merged_snapshots.keys())
                )
            for snapshot in merged_snapshots.values():
                if write_policy == "changed_only":
                    existing_snapshot = existing_snapshots.get(snapshot.instrument_id)
                    existing_json = (
                        existing_snapshot.get("snapshot")
                        if existing_snapshot
                        else None
                    )
                    existing_hash = (
                        self._hash_payload(existing_json)
                        if isinstance(existing_json, dict)
                        else None
                    )
                    incoming_hash = self._hash_payload(snapshot.snapshot_json)
                    existing_scope = {
                        str(scope).strip()
                        for scope in (
                            (existing_json or {}).get("coverage_scope", [])
                            if isinstance(existing_json, dict)
                            else []
                        )
                        if str(scope).strip()
                    }
                    if existing_hash == incoming_hash and required_scope.issubset(existing_scope):
                        unchanged_instruments += 1
                        continue
                    payload_hash = self._hash_payload(snapshot.raw_payload)
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
                total_written += 1

            missing_ids = [
                str(instrument["instrument_id"])
                for instrument in stock_instruments
                if not self._snapshot_covers_required_scope(
                    merged_snapshots.get(str(instrument["instrument_id"])),
                    required_scope,
                )
            ]
            missing_ids = sorted(set(missing_ids))
            if optional_empty_exchange:
                resolved_instruments = len(stock_instruments)
                missing_ids = []
                status = "success"
                error_message = None
            else:
                resolved_instruments = len(stock_instruments) - len(missing_ids)
                status = (
                    "success"
                    if resolved_instruments == len(stock_instruments)
                    and (
                        total_written > 0
                        or (write_policy == "changed_only" and bool(merged_snapshots))
                    )
                    else "degraded"
                )
                error_message = None
                if total_written <= 0:
                    error_message = "No provider returned shareholder snapshots"
                elif missing_ids:
                    error_message = (
                        f"Missing required shareholder scope for {len(missing_ids)} instruments"
                    )

            self.storage.finish_ingestion_run(
                run_id,
                status=status,
                rows_written=total_written,
                error_message=error_message,
                metadata={
                    "exchange": exchange,
                    "source": primary_source,
                    "mode": primary_mode,
                    "required_scope": sorted(required_scope),
                    "force_merge_candidates": sorted(force_merge_candidate_keys),
                    "attempted_sources": attempted_sources,
                    "successful_sources": successful_sources,
                    "skipped_sources": skipped_sources,
                    "skip_same_source_full_fallback_after_success": (
                        skip_same_source_full_fallback_after_success
                    ),
                    "requested_instruments": len(stock_instruments),
                    "resolved_instruments": resolved_instruments,
                    "missing_instruments": len(missing_ids),
                    "missing_instrument_ids": missing_ids[:20],
                    "write_policy": write_policy,
                    "unchanged_instruments": unchanged_instruments,
                    "same_source_recovery_runs": recovery_runs,
                    "same_source_recovery_attempted_instruments": recovery_attempted_instruments,
                    "same_source_recovery_resolved_instruments": recovery_resolved_instruments,
                    "optional_empty_exchange": optional_empty_exchange,
                },
            )
            dm_logger.info(
                "[ShareholderSync] Exchange sync finished: exchange=%s status=%s written=%s resolved=%s/%s run_id=%s",
                exchange,
                status,
                total_written,
                resolved_instruments,
                len(stock_instruments),
                run_id,
            )
            return ShareholderExchangeSyncResult(
                exchange=exchange,
                status=status,
                source=primary_source,
                mode=primary_mode,
                attempted_sources=attempted_sources,
                successful_sources=successful_sources,
                requested_instruments=len(stock_instruments),
                resolved_instruments=resolved_instruments,
                missing_instruments=len(missing_ids),
                missing_instrument_ids=missing_ids[:20],
                snapshots_written=total_written,
                unchanged_instruments=unchanged_instruments,
                error_message=error_message,
            )

        except asyncio.CancelledError:
            error_message = (
                "Shareholder shadow sync was cancelled before exchange completion; "
                "the scheduler max_runtime_seconds timeout was likely reached."
            )
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=error_message,
                metadata={
                    "exchange": exchange,
                    "attempted_sources": attempted_sources,
                    "skipped_sources": skipped_sources,
                    "requested_instruments": len(stock_instruments),
                    "cancelled": True,
                },
            )
            dm_logger.error(
                "[ShareholderSync] Exchange sync cancelled: exchange=%s run_id=%s attempted_sources=%s",
                exchange,
                run_id,
                attempted_sources,
            )
            raise
        except Exception as exc:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=str(exc),
                metadata={
                    "exchange": exchange,
                    "attempted_sources": attempted_sources,
                    "skipped_sources": skipped_sources,
                },
            )
            return ShareholderExchangeSyncResult(
                exchange=exchange,
                status="failed",
                attempted_sources=attempted_sources,
                requested_instruments=len(stock_instruments),
                error_message=str(exc),
            )

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    async def _run_same_source_recovery(
        self,
        *,
        provider: Any,
        candidate_source: str,
        candidate_mode: str,
        exchange: str,
        stock_instruments: List[Dict[str, Any]],
        remaining_ids: Set[str],
        merged_snapshots: Dict[str, ShareholderSnapshot],
        run_id: int,
        batch_size: int,
        max_instruments: int,
        required_scope: Set[str],
        write_policy: str,
    ) -> Dict[str, int]:
        recovery_targets = [
            instrument
            for instrument in stock_instruments
            if str(instrument["instrument_id"]) in remaining_ids
        ][:max_instruments]
        if not recovery_targets:
            return {
                "runs": 0,
                "attempted_instruments": 0,
                "resolved_instruments": 0,
            }

        unresolved_before = sum(
            1
            for instrument in recovery_targets
            if not self._snapshot_covers_required_scope(
                merged_snapshots.get(str(instrument["instrument_id"])),
                required_scope,
            )
        )
        runs = 0
        for batch_start in range(0, len(recovery_targets), batch_size):
            batch = recovery_targets[batch_start : batch_start + batch_size]
            runs += 1
            try:
                snapshots = await provider.fetch_shareholder_snapshots(
                    instruments=batch,
                    exchange=exchange,
                    mode=candidate_mode,
                    limit=len(batch),
                )
            except Exception as exc:
                dm_logger.warning(
                    "[ShareholderSync] Same-source recovery %s (%s) failed for %s: %s",
                    candidate_source,
                    candidate_mode,
                    exchange,
                    exc,
                )
                continue

            accepted_ids: Set[str] = set()
            for snapshot in snapshots or []:
                instrument_id = str(snapshot.instrument_id)
                if instrument_id not in remaining_ids or instrument_id in accepted_ids:
                    continue
                accepted_ids.add(instrument_id)
                merged_snapshots[instrument_id] = self._merge_snapshots(
                    merged_snapshots.get(instrument_id),
                    snapshot,
                )
                if write_policy != "changed_only":
                    payload_hash = self._hash_payload(snapshot.raw_payload)
                    self.storage.store_raw_payload(
                        domain="shareholders",
                        instrument_id=snapshot.instrument_id,
                        source=snapshot.source,
                        source_mode=snapshot.source_mode,
                        payload=snapshot.raw_payload,
                        payload_hash=payload_hash,
                        ingestion_run_id=run_id,
                    )

        unresolved_after = sum(
            1
            for instrument in recovery_targets
            if not self._snapshot_covers_required_scope(
                merged_snapshots.get(str(instrument["instrument_id"])),
                required_scope,
            )
        )
        return {
            "runs": runs,
            "attempted_instruments": len(recovery_targets),
            "resolved_instruments": max(0, unresolved_before - unresolved_after),
        }

    @staticmethod
    def _extract_scope_set(snapshot: Optional[ShareholderSnapshot]) -> Set[str]:
        if snapshot is None:
            return set()
        scope_values = snapshot.snapshot_json.get("coverage_scope", []) or []
        return {
            str(scope).strip()
            for scope in scope_values
            if str(scope).strip()
        }

    def _snapshot_covers_required_scope(
        self,
        snapshot: Optional[ShareholderSnapshot],
        required_scope: Set[str],
    ) -> bool:
        if snapshot is None:
            return False
        if not required_scope:
            return True
        return required_scope.issubset(self._extract_scope_set(snapshot))

    def _merge_snapshots(
        self,
        existing: Optional[ShareholderSnapshot],
        incoming: ShareholderSnapshot,
    ) -> ShareholderSnapshot:
        if existing is None:
            incoming_scope = self._extract_scope_set(incoming)
            incoming_snapshot_json = dict(incoming.snapshot_json)
            incoming_snapshot_json["coverage_scope"] = sorted(incoming_scope)
            incoming_snapshot_json["scope_sources"] = {
                scope: f"{incoming.source}:{incoming.source_mode}"
                for scope in sorted(incoming_scope)
            }
            return ShareholderSnapshot(
                instrument_id=incoming.instrument_id,
                symbol=incoming.symbol,
                exchange=incoming.exchange,
                coverage_status=incoming.coverage_status,
                holder_count=incoming.holder_count,
                holder_count_report_date=incoming.holder_count_report_date,
                top_holders_report_date=incoming.top_holders_report_date,
                top_holders_count=incoming.top_holders_count,
                top_holders_total_ratio=incoming.top_holders_total_ratio,
                control_owner_name=incoming.control_owner_name,
                control_owner_ratio=incoming.control_owner_ratio,
                schema_version=incoming.schema_version,
                source=incoming.source,
                source_mode=incoming.source_mode,
                snapshot_json=incoming_snapshot_json,
                raw_payload=incoming.raw_payload,
            )

        existing_scope = self._extract_scope_set(existing)
        incoming_scope = self._extract_scope_set(incoming)
        merged_scope = sorted(existing_scope | incoming_scope)
        existing_scope_sources = dict(existing.snapshot_json.get("scope_sources", {}) or {})
        incoming_holder_count_source = f"{incoming.source}:{incoming.source_mode}"
        should_replace_holder_count = (
            "holder_count" in incoming_scope
            and incoming.holder_count is not None
            and (
                existing.holder_count is None
                or existing_scope_sources.get("holder_count") == incoming_holder_count_source
            )
        )

        existing_top_holders = list(existing.snapshot_json.get("top_holders", []) or [])
        incoming_top_holders = list(incoming.snapshot_json.get("top_holders", []) or [])
        merged_top_holders = existing_top_holders or incoming_top_holders

        existing_ownership = dict(existing.snapshot_json.get("ownership_clues", {}) or {})
        incoming_ownership = dict(incoming.snapshot_json.get("ownership_clues", {}) or {})
        merged_ownership = dict(existing_ownership)
        incoming_has_authoritative_control = (
            incoming.source == "cninfo"
            and incoming_ownership.get("control_owner_name") not in (None, "")
        )
        for key, value in incoming_ownership.items():
            if incoming_has_authoritative_control or merged_ownership.get(key) in (None, "", []):
                merged_ownership[key] = value

        scope_sources = dict(existing_scope_sources)
        for scope in incoming_scope:
            if incoming_has_authoritative_control and scope == "reference_only_ownership_clues":
                scope_sources[scope] = f"{incoming.source}:{incoming.source_mode}"
            else:
                scope_sources.setdefault(scope, f"{incoming.source}:{incoming.source_mode}")

        merged_snapshot_json = dict(existing.snapshot_json)
        merged_snapshot_json["coverage_scope"] = merged_scope
        merged_snapshot_json["top_holders"] = merged_top_holders
        merged_snapshot_json["ownership_clues"] = merged_ownership
        merged_snapshot_json["scope_sources"] = scope_sources
        if should_replace_holder_count:
            merged_snapshot_json["holder_count"] = incoming.snapshot_json.get("holder_count")

        return ShareholderSnapshot(
            instrument_id=existing.instrument_id,
            symbol=existing.symbol,
            exchange=existing.exchange,
            coverage_status=existing.coverage_status,
            holder_count=incoming.holder_count
            if should_replace_holder_count
            else existing.holder_count if existing.holder_count is not None else incoming.holder_count,
            holder_count_report_date=(
                incoming.holder_count_report_date
                if should_replace_holder_count
                else existing.holder_count_report_date or incoming.holder_count_report_date
            ),
            top_holders_report_date=existing.top_holders_report_date
            or incoming.top_holders_report_date,
            top_holders_count=existing.top_holders_count
            if existing.top_holders_count not in (None, 0)
            else incoming.top_holders_count,
            top_holders_total_ratio=existing.top_holders_total_ratio
            if existing.top_holders_total_ratio is not None
            else incoming.top_holders_total_ratio,
            control_owner_name=(
                incoming.control_owner_name
                if incoming_has_authoritative_control
                else existing.control_owner_name or incoming.control_owner_name
            ),
            control_owner_ratio=(
                incoming.control_owner_ratio
                if incoming_has_authoritative_control and incoming.control_owner_ratio is not None
                else (
                    existing.control_owner_ratio
                    if existing.control_owner_ratio is not None
                    else incoming.control_owner_ratio
                )
            ),
            schema_version=existing.schema_version,
            source=existing.source,
            source_mode=existing.source_mode,
            snapshot_json=merged_snapshot_json,
            raw_payload=existing.raw_payload,
        )
