"""Bounded official-report archive sync for product-label review candidates."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from research.business_profile_archive import BusinessProfileDocumentArchiveService
from research.business_profile_discovery import (
    BusinessProfileDocumentCandidate,
    BusinessProfileAnnouncementDiscoveryAdapter,
)
from research.business_profile_documents import (
    business_profile_document_family,
    infer_business_profile_report_period,
)
from research.business_profile_exchange_discovery import (
    BusinessProfileDiscoveryCoordinator,
    BusinessProfileDiscoveryResolution,
)
from research.business_profile_precision_review import (
    load_product_catalog_issue_review_rows,
    load_product_label_review_rows,
)
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time


OFFICIAL_ARCHIVE_WRITE_SWITCH = "BUSINESS_PROFILE_OFFICIAL_ARCHIVE_WRITE"
OFFICIAL_ARCHIVE_TARGET_SCOPES = {"precision_exact", "catalog_issues"}
PERIODIC_REPORT_FAMILIES = {"annual_report", "semiannual_report"}
FAMILY_SEARCH_KEYS = {
    "annual_report": "年度报告",
    "semiannual_report": "半年度报告",
}


class BusinessProfileOfficialArchiveSyncService:
    """Discover and archive only reports needed by the precision review corpus."""

    def __init__(
        self,
        *,
        storage: ResearchStorageManager,
        research_config: ResearchConfig,
        coordinator: Optional[BusinessProfileDiscoveryCoordinator] = None,
        archive_service: Optional[BusinessProfileDocumentArchiveService] = None,
    ) -> None:
        self.storage = storage
        self.research_config = research_config
        module = research_config.modules.get("business_profile_evidence", {})
        archive_config = (
            module.get("archive", {}) if isinstance(module, Mapping) else {}
        )
        self.checkpoint_root = Path(
            archive_config.get(
                "checkpoint_root",
                "data/checkpoints/business_profile_official_archive",
            )
        )
        self.coordinator = coordinator or (
            BusinessProfileDiscoveryCoordinator.from_research_config(
                research_config,
                primary_adapter=BusinessProfileAnnouncementDiscoveryAdapter(),
            )
        )
        self.archive_service = archive_service or (
            BusinessProfileDocumentArchiveService.from_research_config(
                storage=storage,
                research_config=research_config,
            )
        )

    def sync(
        self,
        *,
        target_research_db: Path,
        target_scope: str = "precision_exact",
        instrument_ids: Optional[Sequence[str]] = None,
        report_period: Optional[str] = None,
        minimum_revenue_share: float = 0.01,
        max_instruments: int = 5,
        max_documents_per_instrument: int = 30,
        page_size: int = 30,
        max_pages: int = 5,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        as_of_date: Optional[str] = None,
        archive_write: bool = False,
        operator_switch: str = "",
        checkpoint_root: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """Run one bounded metadata probe or explicit official archive batch."""
        self._validate_bounds(
            max_instruments=max_instruments,
            max_documents_per_instrument=max_documents_per_instrument,
            page_size=page_size,
            max_pages=max_pages,
            start_date=start_date,
            end_date=end_date,
        )
        if archive_write and operator_switch != OFFICIAL_ARCHIVE_WRITE_SWITCH:
            raise PermissionError(
                "official archive writes require operator switch "
                f"{OFFICIAL_ARCHIVE_WRITE_SWITCH}"
            )
        if target_scope not in OFFICIAL_ARCHIVE_TARGET_SCOPES:
            raise ValueError(
                "target_scope must be one of "
                f"{sorted(OFFICIAL_ARCHIVE_TARGET_SCOPES)}"
            )
        cutoff = date.fromisoformat(
            str(as_of_date or get_shanghai_time().date().isoformat())[:10]
        ).isoformat()
        effective_checkpoint_root = checkpoint_root or self.checkpoint_root
        row_loader = (
            load_product_label_review_rows
            if target_scope == "precision_exact"
            else load_product_catalog_issue_review_rows
        )
        rows = row_loader(
            research_db=target_research_db,
            instrument_ids=instrument_ids,
            report_period=report_period,
            minimum_revenue_share=minimum_revenue_share,
        )
        targets = _select_instrument_targets(rows, limit=max_instruments)
        candidate_counts_before = _candidate_table_counts(target_research_db)
        report: Dict[str, Any] = {
            "schema_version": "business_profile_official_archive_sync.v1",
            "status": "running",
            "mode": "archive_write" if archive_write else "metadata_only",
            "started_at": get_shanghai_time().isoformat(),
            "target_research_db": str(target_research_db),
            "scope": {
                "target_scope": target_scope,
                "instrument_ids": sorted(
                    {
                        str(item).strip()
                        for item in (instrument_ids or ())
                        if str(item).strip()
                    }
                ),
                "report_period": report_period,
                "minimum_revenue_share": minimum_revenue_share,
                "max_instruments": max_instruments,
                "max_documents_per_instrument": max_documents_per_instrument,
                "page_size": page_size,
                "max_pages": max_pages,
                "start_date": start_date,
                "end_date": end_date,
                "as_of_date": cutoff,
            },
            "eligible_review_rows": len(rows),
            "eligible_instruments": len({str(item["instrument_id"]) for item in rows}),
            "selected_instruments": len(targets),
            "target_instrument_periods": sum(
                len(item["report_periods"]) for item in targets
            ),
            "matched_instrument_periods": 0,
            "missing_instrument_periods": 0,
            "archived_documents": 0,
            "unchanged_documents": 0,
            "failed_documents": 0,
            "failed_instruments": 0,
            "discovery_error_count": 0,
            "incomplete_archive_batches": 0,
            "candidate_table_counts_before": candidate_counts_before,
            "candidate_table_counts_after": None,
            "candidate_rows_written": None,
            "parent_ingestion_run_id": None,
            "results": [],
        }
        if not targets:
            report["status"] = "empty"
            report["candidate_table_counts_after"] = candidate_counts_before
            report["candidate_rows_written"] = 0
            return report

        parent_run_id: Optional[int] = None
        fatal_error: Optional[str] = None
        if archive_write:
            parent_run_id = self.storage.start_ingestion_run(
                domain="business_profile",
                job_name="business_profile_official_archive_sync",
                market="A_SHARE",
                source="official_discovery_chain",
                mode="direct",
                metadata={
                    "target_research_db": str(target_research_db),
                    "target_scope": target_scope,
                    "selected_instruments": len(targets),
                    "target_instrument_periods": report["target_instrument_periods"],
                },
            )
            report["parent_ingestion_run_id"] = parent_run_id

        try:
            for target in targets:
                try:
                    result = self._sync_instrument(
                        target,
                        page_size=page_size,
                        max_pages=max_pages,
                        start_date=start_date,
                        end_date=end_date,
                        as_of_date=cutoff,
                        archive_write=archive_write,
                        max_documents=max_documents_per_instrument,
                        checkpoint_root=effective_checkpoint_root,
                        parent_ingestion_run_id=parent_run_id,
                    )
                except Exception as exc:
                    report["failed_instruments"] += 1
                    result = {
                        **_best_effort_instrument_identity(
                            str(target["instrument_id"])
                        ),
                        "industry_groups": list(target["industry_groups"]),
                        "material_review_rows": int(target["row_count"]),
                        "max_revenue_share": target["max_revenue_share"],
                        "requested_report_periods": list(target["report_periods"]),
                        "matched_report_periods": [],
                        "missing_report_periods": list(target["report_periods"]),
                        "unsupported_report_periods": [],
                        "selected_documents": [],
                        "attempts": [],
                        "discovery_errors": [f"{type(exc).__name__}: {exc}"],
                        "archive": None,
                    }
                report["results"].append(result)
                report["matched_instrument_periods"] += len(
                    result["matched_report_periods"]
                )
                report["missing_instrument_periods"] += len(
                    result["missing_report_periods"]
                )
                archive = result.get("archive") or {}
                report["archived_documents"] += int(archive.get("archived") or 0)
                report["unchanged_documents"] += int(archive.get("unchanged") or 0)
                report["failed_documents"] += int(archive.get("failed") or 0)
                report["discovery_error_count"] += len(result["discovery_errors"])
                if archive and not archive.get("checkpoint_complete"):
                    report["incomplete_archive_batches"] += 1
        except BaseException as exc:
            fatal_error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            candidate_counts_after = _candidate_table_counts(target_research_db)
            report["candidate_table_counts_after"] = candidate_counts_after
            report["candidate_rows_written"] = sum(
                candidate_counts_after.get(key, 0) - candidate_counts_before.get(key, 0)
                for key in candidate_counts_before
            )
            if fatal_error is not None:
                report["status"] = "failed"
            elif (
                report["missing_instrument_periods"]
                or report["failed_documents"]
                or report["failed_instruments"]
                or report["discovery_error_count"]
                or report["incomplete_archive_batches"]
                or report["candidate_rows_written"] != 0
            ):
                report["status"] = "degraded"
            else:
                report["status"] = "success"
            if parent_run_id is not None:
                self.storage.finish_ingestion_run(
                    parent_run_id,
                    status=report["status"],
                    rows_written=report["archived_documents"],
                    error_message=(fatal_error or _ingestion_error_message(report)),
                    metadata=report,
                )
        return report

    def _sync_instrument(
        self,
        target: Mapping[str, Any],
        *,
        page_size: int,
        max_pages: int,
        start_date: Optional[str],
        end_date: Optional[str],
        as_of_date: str,
        archive_write: bool,
        max_documents: int,
        checkpoint_root: Optional[Path],
        parent_ingestion_run_id: Optional[int],
    ) -> Dict[str, Any]:
        instrument = _instrument_identity(str(target["instrument_id"]))
        periods_by_family = _periods_by_family(target["report_periods"])
        unsupported_periods = sorted(
            set(target["report_periods"])
            - {period for periods in periods_by_family.values() for period in periods}
        )
        candidates: Dict[tuple[str, str], BusinessProfileDocumentCandidate] = {}
        attempts: List[Dict[str, Any]] = []
        discovery_errors: List[str] = []
        for family, periods in sorted(periods_by_family.items()):
            query_start, query_end = _query_window(
                periods,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
            resolution = self.coordinator.discover_instrument(
                instrument,
                start_date=query_start,
                end_date=query_end,
                search_key=FAMILY_SEARCH_KEYS[family],
                page_size=page_size,
                max_pages=max_pages,
                dry_run=True,
            )
            _merge_discovery_resolution(
                resolution,
                family=family,
                periods=periods,
                candidates=candidates,
                attempts=attempts,
                discovery_errors=discovery_errors,
            )
        selected = _select_active_candidates(candidates.values())
        matched_periods = sorted(
            {
                infer_business_profile_report_period(
                    candidate.title,
                    candidate.announcement_time,
                )
                for candidate in selected
            }
        )
        requested_periods = sorted(target["report_periods"])
        missing_periods = sorted(set(requested_periods) - set(matched_periods))
        archive_payload: Optional[Dict[str, Any]] = None
        if archive_write and selected:
            checkpoint_path = (
                None
                if checkpoint_root is None
                else Path(checkpoint_root) / f"{instrument['instrument_id']}.json"
            )
            archive_result = self.archive_service.archive_candidates(
                instrument,
                selected,
                max_documents=max_documents,
                checkpoint_path=checkpoint_path,
                parent_ingestion_run_id=parent_ingestion_run_id,
            )
            archive_payload = archive_result.to_dict()

        return {
            **instrument,
            "industry_groups": list(target["industry_groups"]),
            "material_review_rows": int(target["row_count"]),
            "max_revenue_share": target["max_revenue_share"],
            "requested_report_periods": requested_periods,
            "matched_report_periods": matched_periods,
            "missing_report_periods": missing_periods,
            "unsupported_report_periods": unsupported_periods,
            "selected_documents": [
                {
                    "announcement_id": item.announcement_id,
                    "title": item.title,
                    "announcement_time": item.announcement_time,
                    "document_type": item.classification.document_type,
                    "source": item.source,
                    "source_tier": item.source_tier,
                    "report_period": infer_business_profile_report_period(
                        item.title,
                        item.announcement_time,
                    ),
                }
                for item in selected
            ],
            "attempts": attempts,
            "discovery_errors": discovery_errors,
            "archive": archive_payload,
        }

    @staticmethod
    def _validate_bounds(
        *,
        max_instruments: int,
        max_documents_per_instrument: int,
        page_size: int,
        max_pages: int,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        if max_instruments < 1 or max_instruments > 20:
            raise ValueError("max_instruments must be between 1 and 20")
        if max_documents_per_instrument < 1 or max_documents_per_instrument > 50:
            raise ValueError("max_documents_per_instrument must be between 1 and 50")
        if page_size < 1 or page_size > 100:
            raise ValueError("page_size must be between 1 and 100")
        if max_pages < 1 or max_pages > 10:
            raise ValueError("max_pages must be between 1 and 10")
        if bool(start_date) != bool(end_date):
            raise ValueError("start_date and end_date must be provided together")
        if start_date and (
            date.fromisoformat(start_date) > date.fromisoformat(str(end_date))
        ):
            raise ValueError("start_date must not be after end_date")


def _select_instrument_targets(
    rows: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        instrument_id = str(row["instrument_id"])
        metadata = row.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        target = grouped.setdefault(
            instrument_id,
            {
                "instrument_id": instrument_id,
                "report_periods": set(),
                "industry_groups": set(),
                "row_count": 0,
                "max_revenue_share": None,
            },
        )
        target["report_periods"].add(str(row["report_period"]))
        industry_group = str(metadata.get("industry_group") or "").strip()
        if industry_group:
            target["industry_groups"].add(industry_group)
        target["row_count"] += 1
        revenue_share = row.get("revenue_share")
        if revenue_share is not None:
            share = float(revenue_share)
            current = target["max_revenue_share"]
            target["max_revenue_share"] = (
                share if current is None else max(float(current), share)
            )
    output = [
        {
            **item,
            "report_periods": sorted(item["report_periods"]),
            "industry_groups": sorted(item["industry_groups"]),
        }
        for item in grouped.values()
    ]
    output.sort(
        key=lambda item: (
            -float(item["max_revenue_share"] or 0),
            -int(item["row_count"]),
            str(item["instrument_id"]),
        )
    )
    return output[:limit]


def _instrument_identity(instrument_id: str) -> Dict[str, str]:
    normalized = str(instrument_id or "").strip().upper()
    if len(normalized) != 9 or normalized[6] != ".":
        raise ValueError(f"unsupported A-share instrument_id: {instrument_id}")
    symbol, suffix = normalized.split(".", 1)
    exchanges = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
    if not symbol.isdigit() or len(symbol) != 6 or suffix not in exchanges:
        raise ValueError(f"unsupported A-share instrument_id: {instrument_id}")
    return {
        "instrument_id": normalized,
        "symbol": symbol,
        "exchange": exchanges[suffix],
    }


def _best_effort_instrument_identity(instrument_id: str) -> Dict[str, str]:
    normalized = str(instrument_id or "").strip().upper()
    try:
        return _instrument_identity(normalized)
    except ValueError:
        return {
            "instrument_id": normalized,
            "symbol": "",
            "exchange": "",
        }


def _ingestion_error_message(report: Mapping[str, Any]) -> Optional[str]:
    reasons = []
    counters = (
        ("missing_target_periods", "missing_instrument_periods"),
        ("failed_documents", "failed_documents"),
        ("failed_instruments", "failed_instruments"),
        ("discovery_errors", "discovery_error_count"),
        ("incomplete_archive_batches", "incomplete_archive_batches"),
        ("candidate_rows_changed", "candidate_rows_written"),
    )
    for label, key in counters:
        value = int(report.get(key) or 0)
        if value:
            reasons.append(f"{label}={value}")
    return "; ".join(reasons) or None


def _periods_by_family(periods: Sequence[str]) -> Dict[str, List[str]]:
    output: Dict[str, List[str]] = defaultdict(list)
    for raw in periods:
        period = str(raw)
        if period.endswith("-12-31"):
            output["annual_report"].append(period)
        elif period.endswith("-06-30"):
            output["semiannual_report"].append(period)
    return {key: sorted(set(values)) for key, values in output.items()}


def _query_window(
    periods: Sequence[str],
    *,
    as_of_date: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[str, str]:
    if start_date and end_date:
        return start_date, end_date
    earliest = min(date.fromisoformat(period) for period in periods)
    return earliest.isoformat(), as_of_date


def _select_active_candidates(
    candidates: Sequence[BusinessProfileDocumentCandidate],
) -> List[BusinessProfileDocumentCandidate]:
    by_period: Dict[str, List[BusinessProfileDocumentCandidate]] = defaultdict(list)
    for candidate in candidates:
        period = infer_business_profile_report_period(
            candidate.title,
            candidate.announcement_time,
        )
        by_period[period].append(candidate)
    output = []
    for period in sorted(by_period):
        output.append(
            max(
                by_period[period],
                key=lambda item: (
                    int(item.classification.is_correction),
                    str(item.announcement_time or ""),
                    item.announcement_id,
                ),
            )
        )
    return output


def _merge_discovery_resolution(
    resolution: BusinessProfileDiscoveryResolution,
    *,
    family: str,
    periods: Sequence[str],
    candidates: Dict[tuple[str, str], BusinessProfileDocumentCandidate],
    attempts: List[Dict[str, Any]],
    discovery_errors: List[str],
) -> set[str]:
    attempts.extend(asdict(item) for item in resolution.attempts)
    for attempt in resolution.attempts:
        discovery_errors.extend(attempt.errors)
    matched_periods = set()
    for candidate in resolution.candidates:
        if (
            business_profile_document_family(candidate.classification.document_type)
            != family
        ):
            continue
        try:
            candidate_period = infer_business_profile_report_period(
                candidate.title,
                candidate.announcement_time,
            )
        except ValueError:
            continue
        if candidate_period not in periods:
            continue
        key = (candidate_period, candidate.announcement_id)
        candidates[key] = candidate
        matched_periods.add(candidate_period)
    return matched_periods


def _candidate_table_counts(path: Path) -> Dict[str, int]:
    if not path.exists():
        raise FileNotFoundError(path)
    tables = ("business_profile_evidence", "company_business_segments")
    output: Dict[str, int] = {}
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as conn:
        known = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        for table in tables:
            output[table] = (
                int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                if table in known
                else 0
            )
    return output
