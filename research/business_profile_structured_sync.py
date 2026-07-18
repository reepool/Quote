"""Bounded sync service for free structured A-share business-profile evidence."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from research.business_profile_corpus import (
    FIRST_WAVE_INDUSTRY_GROUPS,
    apply_instrument_lifecycle,
    list_first_wave_universe,
    load_instrument_lifecycle,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_product_catalog import load_business_product_catalog
from research.business_profile_structured_ingestion import (
    StructuredBusinessProfileCandidateWriter,
)
from research.providers.akshare_business_profile import (
    COMPOSITION_SOURCE,
    STRUCTURED_BUSINESS_PROFILE_SOURCES,
    AkshareStructuredBusinessProfileProvider,
    StructuredBusinessProfileSnapshot,
    StructuredSourceResult,
)
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager
from utils.date_utils import get_shanghai_time


WRITE_OPERATOR_SWITCH = "BUSINESS_PROFILE_CANDIDATE_WRITE"


@dataclass(frozen=True)
class StructuredBusinessProfileSyncConfig:
    enabled: bool
    candidate_only: bool
    sources: tuple[str, ...]
    possible_row_cap: int
    request_timeout_seconds: float
    request_interval_seconds: float
    retry_attempts: int
    retry_backoff_seconds: float
    max_instruments_per_run: int
    max_elapsed_seconds: float
    raw_cache_root: Path
    checkpoint_root: Path

    @classmethod
    def from_research_config(
        cls,
        research_config: ResearchConfig,
    ) -> "StructuredBusinessProfileSyncConfig":
        module = research_config.modules.get("business_profile_evidence", {})
        source_config = module.get("free_structured_sources", {})
        runtime = source_config.get("runtime", {})
        configured_sources = [
            str(item.get("source") or "").strip()
            for item in source_config.get("sources", [])
            if isinstance(item, Mapping) and item.get("enabled") is True
        ]
        unsupported = set(configured_sources) - STRUCTURED_BUSINESS_PROFILE_SOURCES
        if unsupported:
            raise ValueError(
                f"unsupported configured business-profile sources: "
                f"{sorted(unsupported)}"
            )
        return cls(
            enabled=source_config.get("enabled") is True,
            candidate_only=source_config.get("candidate_only") is True,
            sources=tuple(dict.fromkeys(configured_sources)),
            possible_row_cap=max(
                1,
                int(
                    next(
                        (
                            item.get("possible_row_cap")
                            for item in source_config.get("sources", [])
                            if isinstance(item, Mapping)
                            and item.get("source") == COMPOSITION_SOURCE
                        ),
                        200,
                    )
                    or 200
                ),
            ),
            request_timeout_seconds=max(
                1.0,
                float(runtime.get("request_timeout_seconds") or 20.0),
            ),
            request_interval_seconds=max(
                0.0,
                float(runtime.get("request_interval_seconds") or 0.5),
            ),
            retry_attempts=max(1, int(runtime.get("retry_attempts") or 2)),
            retry_backoff_seconds=max(
                0.0,
                float(runtime.get("retry_backoff_seconds") or 1.0),
            ),
            max_instruments_per_run=max(
                1,
                int(runtime.get("max_instruments_per_run") or 30),
            ),
            max_elapsed_seconds=max(
                1.0,
                float(runtime.get("max_elapsed_seconds") or 900.0),
            ),
            raw_cache_root=Path(
                runtime.get("raw_cache_root")
                or "data/cache/research/business_profile_structured"
            ),
            checkpoint_root=Path(
                runtime.get("checkpoint_root")
                or "data/checkpoints/business_profile_structured"
            ),
        )


class StructuredBusinessProfileSyncService:
    """Fetch bounded source snapshots and optionally write governed candidates."""

    def __init__(
        self,
        *,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        repository: Optional[BusinessProfileRepository] = None,
        provider: Optional[AkshareStructuredBusinessProfileProvider] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.config = StructuredBusinessProfileSyncConfig.from_research_config(
            self.research_config
        )
        self.repository = repository or BusinessProfileRepository(storage)
        self.catalog = load_business_product_catalog()
        self.writer = StructuredBusinessProfileCandidateWriter(
            self.repository,
            product_catalog=self.catalog,
        )
        self.provider = provider or AkshareStructuredBusinessProfileProvider(
            possible_row_cap=self.config.possible_row_cap,
            request_timeout_seconds=self.config.request_timeout_seconds,
            request_interval_seconds=self.config.request_interval_seconds,
            retry_attempts=self.config.retry_attempts,
            retry_backoff_seconds=self.config.retry_backoff_seconds,
        )
        self._clock = clock

    async def sync(
        self,
        *,
        as_of_date: Optional[str] = None,
        sources: Optional[Sequence[str]] = None,
        industry_groups: Optional[Sequence[str]] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        max_instruments: Optional[int] = None,
        max_elapsed_seconds: Optional[float] = None,
        dry_run: bool = True,
        candidate_write: bool = False,
        operator_switch: str = "",
        allow_disabled_dry_run: bool = False,
        cache_raw_snapshots: bool = False,
        raw_cache_root: Optional[Path] = None,
        checkpoint_path: Optional[Path] = None,
        resume: bool = False,
        universe: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> dict[str, Any]:
        """Run one bounded batch and return a compact governance report."""
        cutoff = date.fromisoformat(
            str(as_of_date or date.today().isoformat())[:10]
        ).isoformat()
        selected_sources = self._resolve_sources(sources)
        selected_groups = self._resolve_industry_groups(industry_groups)
        selected_ids = _normalize_instrument_ids(instrument_ids)
        limit = self._resolve_instrument_limit(max_instruments)
        elapsed_limit = self._resolve_elapsed_limit(max_elapsed_seconds)
        self._validate_write_controls(
            dry_run=dry_run,
            candidate_write=candidate_write,
            operator_switch=operator_switch,
            allow_disabled_dry_run=allow_disabled_dry_run,
            raw_cache_root=raw_cache_root,
        )
        effective_raw_cache_root = raw_cache_root or self.config.raw_cache_root
        eligible_universe = self._resolve_universe(
            as_of_date=cutoff,
            industry_groups=selected_groups,
            instrument_ids=selected_ids,
            universe=universe,
        )
        universe_identity = [
            {
                "instrument_id": str(item["instrument_id"]),
                "industry_group": str(item["industry_group"]),
            }
            for item in eligible_universe
        ]
        scope = {
            "as_of_date": cutoff,
            "sources": list(selected_sources),
            "industry_groups": list(selected_groups),
            "instrument_ids": list(selected_ids),
            "catalog_version": self.catalog.catalog_version,
            "universe_hash": _canonical_hash({"instruments": universe_identity}),
        }
        scope_hash = _canonical_hash(scope)
        resolved_checkpoint = checkpoint_path or (
            self.config.checkpoint_root / f"{scope_hash[:20]}.json"
        )
        checkpoint = _load_checkpoint(
            resolved_checkpoint,
            scope=scope,
            scope_hash=scope_hash,
            resume=resume,
        )
        _write_json_atomic(resolved_checkpoint, checkpoint)
        completed_keys = set(checkpoint.get("completed_source_keys", []))
        pending = [
            (
                item,
                tuple(
                    source
                    for source in selected_sources
                    if _source_key(str(item["instrument_id"]), source)
                    not in completed_keys
                ),
            )
            for item in eligible_universe
        ]
        pending = [
            (item, source_scope) for item, source_scope in pending if source_scope
        ]
        batch = pending[:limit]
        started = self._clock()
        started_at = get_shanghai_time().isoformat()
        run_token = hashlib.sha256(
            f"{scope_hash}:{started_at}".encode("utf-8")
        ).hexdigest()[:24]
        report = _new_report(
            scope=scope,
            scope_hash=scope_hash,
            started_at=started_at,
            eligible_count=len(eligible_universe),
            pending_count=len(pending),
            checkpoint_path=resolved_checkpoint,
            dry_run=dry_run,
            candidate_write=candidate_write,
            cache_raw_snapshots=(cache_raw_snapshots or candidate_write),
            raw_cache_root=effective_raw_cache_root,
        )
        audited_instrument_ids = [
            str(item["instrument_id"]) for item, _source_scope in batch
        ]
        governance_before = (
            _profile_governance_counts(self.storage, audited_instrument_ids)
            if candidate_write
            else None
        )
        raw_manifest_rows: list[dict[str, Any]] = []
        run_id: Optional[int] = None
        if candidate_write:
            run_id = self.storage.start_ingestion_run(
                domain="business_profile_structured",
                job_name="business_profile_structured_sync",
                market="A_SHARE",
                source=",".join(selected_sources),
                mode="direct",
                metadata={
                    "scope": scope,
                    "scope_hash": scope_hash,
                    "max_instruments": limit,
                    "max_elapsed_seconds": elapsed_limit,
                },
            )

        run_error: Optional[Exception] = None
        try:
            for item, source_scope in batch:
                remaining_seconds = elapsed_limit - (self._clock() - started)
                if remaining_seconds <= 0:
                    report["stopped_reason"] = "max_elapsed_seconds"
                    break
                instrument_id = str(item["instrument_id"])
                industry_group = str(item["industry_group"])
                dm_logger.info(
                    "[BusinessProfileStructuredSync] Fetching %s "
                    "(industry=%s, sources=%s)",
                    instrument_id,
                    industry_group,
                    source_scope,
                )
                instrument_started = self._clock()
                try:
                    snapshot = await asyncio.wait_for(
                        self.provider.fetch(
                            instrument_id,
                            observed_at=get_shanghai_time().isoformat(),
                            sources=source_scope,
                            deadline_monotonic=(time.monotonic() + remaining_seconds),
                        ),
                        timeout=remaining_seconds,
                    )
                    references = {}
                    if cache_raw_snapshots or candidate_write:
                        references = _cache_snapshot_sources(
                            snapshot,
                            cache_root=effective_raw_cache_root,
                            run_token=run_token,
                            manifest_rows=raw_manifest_rows,
                        )
                    if references:
                        report["raw_manifest_path"] = str(
                            _write_raw_manifest(
                                cache_root=effective_raw_cache_root,
                                run_token=run_token,
                                scope_hash=scope_hash,
                                started_at=started_at,
                                entries=raw_manifest_rows,
                            )
                        )
                    write_result = None
                    if candidate_write:
                        write_result = self.writer.write(
                            snapshot,
                            industry_group=industry_group,
                            raw_snapshot_references=references,
                        )
                    self._record_snapshot(
                        report,
                        snapshot,
                        industry_group=industry_group,
                        write_result=write_result,
                        references=references,
                    )
                    successful_sources = 0
                    for source_result in _source_results(snapshot):
                        if source_result.source not in source_scope:
                            continue
                        if source_result.status in {"success", "empty"}:
                            completed_keys.add(
                                _source_key(instrument_id, source_result.source)
                            )
                            successful_sources += 1
                    if successful_sources == 0:
                        report["failed_instruments"].append(
                            {
                                "instrument_id": instrument_id,
                                "reason": "all_requested_sources_failed",
                            }
                        )
                    report["attempted_instruments"] += 1
                    report["instrument_elapsed_seconds"] += (
                        self._clock() - instrument_started
                    )
                except Exception as exc:
                    dm_logger.warning(
                        "[BusinessProfileStructuredSync] %s failed: %s",
                        instrument_id,
                        exc,
                    )
                    report["attempted_instruments"] += 1
                    report["failed_instruments"].append(
                        {
                            "instrument_id": instrument_id,
                            "reason": f"{type(exc).__name__}:{exc}",
                        }
                    )
                checkpoint["completed_source_keys"] = sorted(completed_keys)
                checkpoint["updated_at"] = get_shanghai_time().isoformat()
                _write_json_atomic(resolved_checkpoint, checkpoint)
                if self._clock() - started >= elapsed_limit and len(
                    completed_keys
                ) < len(eligible_universe) * len(selected_sources):
                    report["stopped_reason"] = "max_elapsed_seconds"
                    break
        except Exception as exc:
            run_error = exc
            report["run_error"] = f"{type(exc).__name__}:{exc}"
            raise
        finally:
            report["elapsed_seconds"] = self._clock() - started
            report["completed_source_count"] = len(completed_keys)
            report["remaining_source_count"] = max(
                0,
                len(eligible_universe) * len(selected_sources) - len(completed_keys),
            )
            report["completed"] = report["remaining_source_count"] == 0
            if governance_before is not None:
                try:
                    governance_after = _profile_governance_counts(
                        self.storage,
                        audited_instrument_ids,
                    )
                    _record_dcf_leakage(
                        report,
                        before=governance_before,
                        after=governance_after,
                    )
                except Exception as exc:
                    report["dcf_leakage"] = {
                        "approved_records_written": None,
                        "value_chain_roles_written": None,
                        "company_commodity_exposures_written": None,
                        "dcf_inputs_written": None,
                        "status": "measurement_failed",
                        "measurement_basis": "database_delta",
                        "error": f"{type(exc).__name__}:{exc}",
                    }
            _finalize_report(report)
            if run_error is not None:
                report["status"] = "failed"
            if run_id is not None:
                status = report["status"]
                self.storage.finish_ingestion_run(
                    run_id,
                    status=(
                        status
                        if status in {"success", "degraded", "failed"}
                        else "degraded"
                    ),
                    rows_written=(
                        report["candidate_evidence_written"]
                        + report["candidate_segments_written"]
                    ),
                    error_message=(
                        report.get("run_error")
                        or (
                            "one or more instruments or sources failed"
                            if report["failed_instruments"]
                            else (
                                "business-profile leakage measurement failed"
                                if report["dcf_leakage"]["status"] != "pass"
                                else None
                            )
                        )
                    ),
                    metadata=report,
                )
        return report

    def _resolve_sources(
        self,
        sources: Optional[Sequence[str]],
    ) -> tuple[str, ...]:
        selected = tuple(
            dict.fromkeys(
                str(source or "").strip()
                for source in (sources or self.config.sources)
                if str(source or "").strip()
            )
        )
        if not selected:
            raise ValueError("no enabled structured business-profile source configured")
        unsupported = set(selected) - set(self.config.sources)
        if unsupported:
            raise ValueError(
                f"sources are not enabled in configuration: {sorted(unsupported)}"
            )
        return selected

    @staticmethod
    def _resolve_industry_groups(
        industry_groups: Optional[Sequence[str]],
    ) -> tuple[str, ...]:
        groups = tuple(
            dict.fromkeys(
                str(group or "").strip()
                for group in (industry_groups or tuple(FIRST_WAVE_INDUSTRY_GROUPS))
                if str(group or "").strip()
            )
        )
        unsupported = set(groups) - set(FIRST_WAVE_INDUSTRY_GROUPS)
        if unsupported:
            raise ValueError(
                f"unsupported first-wave industry groups: {sorted(unsupported)}"
            )
        return groups

    def _resolve_instrument_limit(self, value: Optional[int]) -> int:
        limit = self.config.max_instruments_per_run if value is None else int(value)
        if limit < 1 or limit > self.config.max_instruments_per_run:
            raise ValueError(
                "max_instruments must be between 1 and configured "
                f"limit {self.config.max_instruments_per_run}"
            )
        return limit

    def _resolve_elapsed_limit(self, value: Optional[float]) -> float:
        limit = self.config.max_elapsed_seconds if value is None else float(value)
        if limit <= 0 or limit > self.config.max_elapsed_seconds:
            raise ValueError(
                "max_elapsed_seconds must be positive and not exceed configured "
                f"limit {self.config.max_elapsed_seconds}"
            )
        return limit

    def _validate_write_controls(
        self,
        *,
        dry_run: bool,
        candidate_write: bool,
        operator_switch: str,
        allow_disabled_dry_run: bool,
        raw_cache_root: Optional[Path],
    ) -> None:
        if not self.config.enabled:
            if not (dry_run and allow_disabled_dry_run and not candidate_write):
                raise RuntimeError("free structured business-profile sync is disabled")
        if candidate_write:
            if dry_run:
                raise ValueError("candidate_write is incompatible with dry_run")
            if not self.config.candidate_only:
                raise RuntimeError("configured source must remain candidate_only")
            if operator_switch != WRITE_OPERATOR_SWITCH:
                raise PermissionError(
                    f"candidate write requires operator switch "
                    f"{WRITE_OPERATOR_SWITCH}"
                )
            if raw_cache_root is not None:
                raise ValueError(
                    "candidate writes must use the configured raw_cache_root"
                )

    def _resolve_universe(
        self,
        *,
        as_of_date: str,
        industry_groups: Sequence[str],
        instrument_ids: Sequence[str],
        universe: Optional[Sequence[Mapping[str, Any]]],
    ) -> list[dict[str, Any]]:
        if universe is None:
            universe = load_structured_business_profile_universe(
                research_db=Path(self.research_config.storage.db_path),
                quotes_db=Path(self.research_config.storage.quotes_db_path),
                as_of_date=as_of_date,
            )
        selected_groups = set(industry_groups)
        selected_ids = set(instrument_ids)
        rows = [
            dict(item)
            for item in universe
            if str(item.get("industry_group") or "") in selected_groups
            and (
                not selected_ids or str(item.get("instrument_id") or "") in selected_ids
            )
        ]
        if selected_ids:
            found = {str(item.get("instrument_id") or "") for item in rows}
            missing = sorted(selected_ids - found)
            if missing:
                raise ValueError(
                    "requested instruments are outside the point-in-time "
                    f"first-wave universe: {missing}"
                )
        return sorted(
            rows,
            key=lambda item: (
                str(item.get("industry_group") or ""),
                str(item.get("instrument_id") or ""),
            ),
        )

    def _record_snapshot(
        self,
        report: dict[str, Any],
        snapshot: StructuredBusinessProfileSnapshot,
        *,
        industry_group: str,
        write_result: Optional[Mapping[str, Any]],
        references: Mapping[str, Mapping[str, Any]],
    ) -> None:
        for source_result in _source_results(snapshot):
            if source_result.status == "skipped":
                continue
            source_report = report["sources"].setdefault(
                source_result.source,
                _new_source_report(),
            )
            source_report[f"{source_result.status}_count"] += 1
            source_report["latency_seconds"] += source_result.elapsed_seconds
            source_report["raw_row_count"] += len(source_result.raw_payload)
            source_report["normalized_row_count"] += len(source_result.rows)
            if source_result.introduction is not None:
                source_report["introduction_count"] += 1
            for raw_row in source_result.raw_payload:
                for field_name, value in raw_row.items():
                    field = str(field_name)
                    source_report["raw_field_names"].add(field)
                    target = (
                        "raw_field_empty_counts"
                        if value is None or str(value).strip() in {"", "--", "nan"}
                        else "raw_field_non_empty_counts"
                    )
                    source_report[target][field] = (
                        source_report[target].get(field, 0) + 1
                    )
            source_report["report_periods"].update(
                row.report_period for row in source_result.rows
            )
            source_report["diagnostics"].update(source_result.diagnostics)
            for diagnostic in source_result.diagnostics:
                source_report["diagnostic_counts"][diagnostic] = (
                    source_report["diagnostic_counts"].get(diagnostic, 0) + 1
                )
            if source_result.source in references:
                cache_status = references[source_result.source].get("cache_status")
                source_report[f"cache_{cache_status}_count"] += 1
            if write_result:
                write_summary = write_result["source_results"].get(
                    source_result.source,
                    {},
                )
                write_status = str(write_summary.get("status") or "")
                if write_status:
                    source_report[f"payload_{write_status}_count"] += 1

        for row in snapshot.composition.rows:
            if row.classification_type != "product":
                continue
            report["product_label_rows"] += 1
            resolution = self.catalog.resolve_alias(
                row.item_name,
                industry_group=industry_group,
            )
            if not resolution.product_ids:
                report["unmatched_label_rows"] += 1
            elif len(resolution.product_ids) > 1:
                report["ambiguous_label_rows"] += 1
            else:
                report["resolved_label_rows"] += 1
        if write_result:
            report["candidate_evidence_written"] += int(
                write_result.get("evidence_written") or 0
            )
            report["candidate_segments_written"] += int(
                write_result.get("segment_candidates_written") or 0
            )


def load_structured_business_profile_universe(
    *,
    research_db: Path,
    quotes_db: Path,
    as_of_date: str,
) -> list[dict[str, Any]]:
    """Load current listed first-wave A shares using point-in-time industry data."""
    if not research_db.exists():
        raise FileNotFoundError(research_db)
    if not quotes_db.exists():
        raise FileNotFoundError(quotes_db)
    with sqlite3.connect(
        f"file:{research_db.resolve()}?mode=ro",
        uri=True,
    ) as research_conn:
        universe = list_first_wave_universe(
            research_conn,
            as_of_date=as_of_date,
        )
    with sqlite3.connect(
        f"file:{quotes_db.resolve()}?mode=ro",
        uri=True,
    ) as quotes_conn:
        lifecycle = load_instrument_lifecycle(
            quotes_conn,
            [str(item["instrument_id"]) for item in universe],
        )
    return apply_instrument_lifecycle(
        universe,
        lifecycle,
        as_of_date=as_of_date,
    )


def _new_report(
    *,
    scope: Mapping[str, Any],
    scope_hash: str,
    started_at: str,
    eligible_count: int,
    pending_count: int,
    checkpoint_path: Path,
    dry_run: bool,
    candidate_write: bool,
    cache_raw_snapshots: bool,
    raw_cache_root: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "business_profile_structured_sync_report.v1",
        "status": "running",
        "scope": dict(scope),
        "scope_hash": scope_hash,
        "started_at": started_at,
        "dry_run": dry_run,
        "candidate_write": candidate_write,
        "cache_raw_snapshots": cache_raw_snapshots,
        "raw_cache_root": str(raw_cache_root) if cache_raw_snapshots else None,
        "eligible_instruments": eligible_count,
        "pending_instruments_before_run": pending_count,
        "attempted_instruments": 0,
        "instrument_elapsed_seconds": 0.0,
        "elapsed_seconds": 0.0,
        "stopped_reason": None,
        "failed_instruments": [],
        "sources": {},
        "product_label_rows": 0,
        "resolved_label_rows": 0,
        "unmatched_label_rows": 0,
        "ambiguous_label_rows": 0,
        "candidate_evidence_written": 0,
        "candidate_segments_written": 0,
        "completed_source_count": 0,
        "remaining_source_count": 0,
        "completed": False,
        "checkpoint_path": str(checkpoint_path),
        "raw_manifest_path": None,
        "dcf_leakage": {
            "approved_records_written": None,
            "value_chain_roles_written": None,
            "company_commodity_exposures_written": None,
            "dcf_inputs_written": None,
            "status": "not_measured",
            "measurement_basis": None,
        },
        "run_error": None,
    }


def _new_source_report() -> dict[str, Any]:
    return {
        "success_count": 0,
        "empty_count": 0,
        "failed_count": 0,
        "latency_seconds": 0.0,
        "raw_row_count": 0,
        "normalized_row_count": 0,
        "introduction_count": 0,
        "raw_field_names": set(),
        "raw_field_empty_counts": {},
        "raw_field_non_empty_counts": {},
        "report_periods": set(),
        "diagnostics": set(),
        "diagnostic_counts": {},
        "cache_new_count": 0,
        "cache_existing_count": 0,
        "payload_written_count": 0,
        "payload_unchanged_count": 0,
        "payload_empty_count": 0,
        "payload_failed_count": 0,
        "payload_invalid_count": 0,
    }


def _finalize_report(report: dict[str, Any]) -> None:
    for source_report in report["sources"].values():
        requests = (
            source_report["success_count"]
            + source_report["empty_count"]
            + source_report["failed_count"]
        )
        source_report["average_latency_seconds"] = (
            source_report["latency_seconds"] / requests if requests else 0.0
        )
        source_report["success_rate"] = (
            (source_report["success_count"] + source_report["empty_count"]) / requests
            if requests
            else 0.0
        )
        source_report["raw_field_names"] = sorted(source_report["raw_field_names"])
        source_report["raw_field_empty_counts"] = dict(
            sorted(source_report["raw_field_empty_counts"].items())
        )
        source_report["raw_field_non_empty_counts"] = dict(
            sorted(source_report["raw_field_non_empty_counts"].items())
        )
        source_report["report_periods"] = sorted(source_report["report_periods"])
        source_report["diagnostics"] = sorted(source_report["diagnostics"])
        source_report["diagnostic_counts"] = dict(
            sorted(source_report["diagnostic_counts"].items())
        )
    if report["dcf_leakage"]["status"] not in {"pass", "not_measured"}:
        report["status"] = "failed"
    elif report["stopped_reason"]:
        report["status"] = "interrupted"
    elif report["failed_instruments"] or any(
        source["failed_count"] for source in report["sources"].values()
    ):
        report["status"] = "degraded"
    else:
        report["status"] = "success"


def _profile_governance_counts(
    storage: ResearchStorageManager,
    instrument_ids: Sequence[str],
) -> dict[str, int]:
    if not instrument_ids:
        return {
            "approved_records": 0,
            "value_chain_roles": 0,
            "company_commodity_exposures": 0,
        }
    placeholders = ",".join("?" for _item in instrument_ids)
    approved_tables = (
        "business_profile_evidence",
        "company_business_profile_events",
        "company_business_profile_regimes",
        "company_business_segments",
        "company_operating_facts",
        "company_value_chain_roles",
        "company_commodity_exposures",
    )
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        approved_records = sum(
            int(
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE instrument_id IN ({placeholders})
                      AND review_status = 'approved'
                    """,
                    tuple(instrument_ids),
                ).fetchone()[0]
            )
            for table in approved_tables
        )
        value_chain_roles = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM company_value_chain_roles
                WHERE instrument_id IN ({placeholders})
                """,
                tuple(instrument_ids),
            ).fetchone()[0]
        )
        company_exposures = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM company_commodity_exposures
                WHERE instrument_id IN ({placeholders})
                """,
                tuple(instrument_ids),
            ).fetchone()[0]
        )
    return {
        "approved_records": approved_records,
        "value_chain_roles": value_chain_roles,
        "company_commodity_exposures": company_exposures,
    }


def _record_dcf_leakage(
    report: dict[str, Any],
    *,
    before: Mapping[str, int],
    after: Mapping[str, int],
) -> None:
    approved_delta = after["approved_records"] - before["approved_records"]
    role_delta = after["value_chain_roles"] - before["value_chain_roles"]
    exposure_delta = (
        after["company_commodity_exposures"] - before["company_commodity_exposures"]
    )
    deltas = (approved_delta, role_delta, exposure_delta)
    report["dcf_leakage"] = {
        "approved_records_written": approved_delta,
        "value_chain_roles_written": role_delta,
        "company_commodity_exposures_written": exposure_delta,
        "dcf_inputs_written": 0,
        "status": "pass" if all(value == 0 for value in deltas) else "fail",
        "measurement_basis": ("database_delta_and_no_dcf_writer_invocation"),
    }


def _write_raw_manifest(
    *,
    cache_root: Path,
    run_token: str,
    scope_hash: str,
    started_at: str,
    entries: Sequence[Mapping[str, Any]],
) -> Path:
    manifest_path = cache_root / "manifests" / f"{run_token}.json"
    _write_json_atomic(
        manifest_path,
        {
            "schema_version": "business_profile_structured_raw_manifest.v1",
            "run_token": run_token,
            "scope_hash": scope_hash,
            "started_at": started_at,
            "entries": list(entries),
        },
    )
    return manifest_path


def _cache_snapshot_sources(
    snapshot: StructuredBusinessProfileSnapshot,
    *,
    cache_root: Path,
    run_token: str,
    manifest_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    references: dict[str, dict[str, Any]] = {}
    for result in _source_results(snapshot):
        if result.status not in {"success", "empty"} or not result.payload_hash:
            continue
        instrument_token = snapshot.instrument_id.replace(".", "_")
        path = (
            cache_root
            / result.source
            / instrument_token
            / f"{result.payload_hash}.json.gz"
        )
        existed = path.exists()
        if not existed:
            payload = {
                "schema_version": "business_profile_structured_raw_snapshot.v1",
                "instrument_id": snapshot.instrument_id,
                "observed_at": snapshot.observed_at,
                "source": result.source,
                "status": result.status,
                "payload_hash": result.payload_hash,
                "diagnostics": list(result.diagnostics),
                "raw_payload": list(result.raw_payload),
            }
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            compressed = gzip.compress(encoded, mtime=0)
            _write_bytes_atomic(path, compressed)
        cached_payload, cache_file_hash = _read_cached_snapshot(path)
        if (
            cached_payload.get("instrument_id") != snapshot.instrument_id
            or cached_payload.get("source") != result.source
            or cached_payload.get("payload_hash") != result.payload_hash
            or _raw_payload_hash(cached_payload.get("raw_payload"))
            != result.payload_hash
        ):
            raise ValueError(f"raw snapshot cache identity mismatch: {path}")
        reference = {
            "schema_version": "business_profile_structured_raw_reference.v1",
            "source": result.source,
            "payload_hash": result.payload_hash,
            "cache_path": str(path),
            "cache_status": "existing" if existed else "new",
            "cache_file_hash": cache_file_hash,
            "run_token": run_token,
        }
        references[result.source] = reference
        manifest_rows.append(
            {
                "instrument_id": snapshot.instrument_id,
                "observed_at": snapshot.observed_at,
                **reference,
            }
        )
    return references


def _source_results(
    snapshot: StructuredBusinessProfileSnapshot,
) -> tuple[StructuredSourceResult, StructuredSourceResult]:
    return snapshot.composition, snapshot.introduction


def _load_checkpoint(
    path: Path,
    *,
    scope: Mapping[str, Any],
    scope_hash: str,
    resume: bool,
) -> dict[str, Any]:
    if resume:
        if not path.exists():
            raise FileNotFoundError(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("scope_hash") != scope_hash or payload.get("scope") != dict(
            scope
        ):
            raise ValueError("checkpoint scope does not match requested sync scope")
        return payload
    return {
        "schema_version": "business_profile_structured_checkpoint.v1",
        "scope": dict(scope),
        "scope_hash": scope_hash,
        "completed_source_keys": [],
        "created_at": get_shanghai_time().isoformat(),
        "updated_at": get_shanghai_time().isoformat(),
    }


def _normalize_instrument_ids(
    values: Optional[Sequence[str]],
) -> tuple[str, ...]:
    output = []
    for value in values or ():
        instrument_id = str(value or "").strip().upper()
        if (
            len(instrument_id) != 9
            or instrument_id[6:] not in {".SH", ".SZ", ".BJ"}
            or not instrument_id[:6].isdigit()
        ):
            raise ValueError(f"unsupported A-share instrument_id: {value}")
        output.append(instrument_id)
    return tuple(dict.fromkeys(output))


def _source_key(instrument_id: str, source: str) -> str:
    return f"{instrument_id}|{source}"


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _raw_payload_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(
            f"{json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)}\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_bytes_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_cached_snapshot(path: Path) -> tuple[Mapping[str, Any], str]:
    compressed = path.read_bytes()
    file_hash = hashlib.sha256(compressed).hexdigest()
    try:
        payload = json.loads(gzip.decompress(compressed).decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid raw snapshot cache file: {path}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"raw snapshot cache root must be an object: {path}")
    return payload, file_hash
