"""Immutable archive and manifest governance for business-profile disclosures."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from string import Formatter
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from research.announcements import (
    AnnouncementAttachment,
    AnnouncementAttachmentRetriever,
    load_announcement_acquisition_config,
)
from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import (
    business_profile_document_family,
    infer_business_profile_report_period,
)
from research.providers.base import FinancialSourceFileManifest
from utils.date_utils import get_shanghai_time
from utils.config_manager import config_manager


BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION = "business_profile_source_file_manifest.v1"
BUSINESS_PROFILE_ARCHIVE_VERSION = "business_profile_pdf_archive.v2"
BUSINESS_PROFILE_SOURCE_TIER = "official_primary"
BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES = frozenset(
    {"archived", "archived_unchanged_content", "verified", "success"}
)
DEFAULT_BUSINESS_PROFILE_ARCHIVE_ROOT = "data/filings/business_profile"
DEFAULT_BUSINESS_PROFILE_DIRECTORY_TEMPLATE = "{year}/{market}"
DEFAULT_BUSINESS_PROFILE_FILENAME_TEMPLATE = (
    "{instrument_id}_{period_label}_{announcement_id}_{content_hash}.pdf"
)
LOGGER = logging.getLogger(__name__)
_SAFE_PATH_RE = re.compile(r"[^0-9A-Za-z_.-]+")
_ARCHIVE_TEMPLATE_FIELDS = {
    "announcement_id",
    "content_hash",
    "document_type",
    "instrument_id",
    "market",
    "period_label",
    "report_period",
    "symbol",
    "year",
}


def _build_shared_annual_report_access(research_config: Any) -> Any | None:
    """Build the shared asset access facade when a real research DB is available."""

    storage_config = getattr(research_config, "storage", None)
    db_path = getattr(storage_config, "db_path", None)
    if not isinstance(db_path, (str, os.PathLike)) or not str(db_path).strip():
        # Small compatibility fixtures may only provide business-profile
        # archive settings; they intentionally retain the old adapter.
        return None
    from research.announcement_assets import (
        AnnouncementAssetAccess,
        AnnouncementAssetConfig,
        AnnouncementAssetRepository,
        AnnouncementAssetService,
    )

    asset_config = AnnouncementAssetConfig.from_research_config(
        research_config,
        project_root=Path.cwd(),
    )
    path = Path(str(db_path))
    if not path.is_absolute():
        path = Path.cwd() / path
    repository = AnnouncementAssetRepository(path)
    acquisition_config = load_announcement_acquisition_config(research_config)
    retriever = AnnouncementAttachmentRetriever.from_provider_configs(
        acquisition_config.provider_configs
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=asset_config,
        attachment_retriever=retriever,
    )
    return AnnouncementAssetAccess(
        repository=repository,
        config=asset_config,
        service=service,
    )


def download_business_profile_candidate(
    candidate: BusinessProfileDocumentCandidate,
    *,
    retriever: Optional[AnnouncementAttachmentRetriever] = None,
) -> bytes:
    """Download one attachment through the common governed retrieval service."""
    source = str(candidate.source or "cninfo").strip().lower()
    if not candidate.adjunct_url:
        raise ValueError("candidate attachment URL is missing")
    if retriever is None:
        acquisition_config = load_announcement_acquisition_config(
            config_manager.get_research_config()
        )
        retriever = AnnouncementAttachmentRetriever.from_provider_configs(
            acquisition_config.provider_configs
        )
    result = retriever.retrieve(
        source,
        AnnouncementAttachment(
            source_url=candidate.adjunct_url,
            resolved_url=(
                candidate.adjunct_url
                if candidate.adjunct_url.startswith(("http://", "https://"))
                else None
            ),
            file_extension=candidate.adjunct_type,
        ),
        require_pdf=True,
    )
    if result.status != "success":
        raise RuntimeError(
            "business-profile attachment retrieval failed: " + "; ".join(result.errors)
        )
    return result.content


def _resolved_business_profile_source_url(
    candidate: BusinessProfileDocumentCandidate,
) -> str:
    """Resolve manifest lineage through the same governed source policy."""
    if not candidate.adjunct_url:
        raise ValueError("candidate attachment URL is missing")
    source = str(candidate.source or "cninfo").strip().lower()
    acquisition_config = load_announcement_acquisition_config(
        config_manager.get_research_config()
    )
    retriever = AnnouncementAttachmentRetriever.from_provider_configs(
        acquisition_config.provider_configs
    )
    attachment = retriever.resolve_attachment(
        source,
        AnnouncementAttachment(
            source_url=candidate.adjunct_url,
            resolved_url=(
                candidate.adjunct_url
                if candidate.adjunct_url.startswith(("http://", "https://"))
                else None
            ),
            file_extension=candidate.adjunct_type,
        ),
    )
    if not attachment.resolved_url:
        raise ValueError("candidate attachment URL could not be resolved")
    return attachment.resolved_url


@dataclass(frozen=True)
class BusinessProfileArchiveRecord:
    """Outcome for one announcement attachment."""

    announcement_id: str
    report_period: str
    document_type: str
    content_hash: str
    archive_path: str
    source_file_id: str
    status: str
    supersedes_source_file_id: Optional[str] = None


@dataclass
class BusinessProfileArchiveBatchResult:
    """Bounded archive run summary."""

    attempted: int = 0
    archived: int = 0
    unchanged: int = 0
    skipped_checkpoint: int = 0
    failed: int = 0
    checkpoint_complete: bool = False
    parent_ingestion_run_id: Optional[int] = None
    manifest_ingestion_run_id: Optional[int] = None
    records: List[BusinessProfileArchiveRecord] = field(default_factory=list)
    errors: List[Dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["records"] = [asdict(item) for item in self.records]
        return payload


class BusinessProfileDocumentArchiveService:
    """Download official PDFs into an immutable, resumable local archive."""

    def __init__(
        self,
        *,
        storage: Any,
        archive_root: str | Path = DEFAULT_BUSINESS_PROFILE_ARCHIVE_ROOT,
        directory_template: str = DEFAULT_BUSINESS_PROFILE_DIRECTORY_TEMPLATE,
        filename_template: str = DEFAULT_BUSINESS_PROFILE_FILENAME_TEMPLATE,
        downloader: Optional[
            Callable[[BusinessProfileDocumentCandidate], bytes]
        ] = None,
        shared_asset_access: Any | None = None,
        shared_asset_service: Any | None = None,
        shared_annual_report_enabled: bool | None = None,
        legacy_annual_report_fallback_enabled: bool | None = None,
        annual_report_asset_mode: str | None = None,
    ) -> None:
        self.storage = storage
        self.archive_root = Path(archive_root)
        self.directory_template = self._validate_layout_template(
            directory_template,
            field_name="directory_template",
        )
        self.filename_template = self._validate_layout_template(
            filename_template,
            field_name="filename_template",
        )
        layout_fields = self._template_fields(
            self.directory_template
        ) | self._template_fields(self.filename_template)
        if "content_hash" not in layout_fields:
            raise ValueError(
                "business-profile archive layout must include {content_hash}"
            )
        self.downloader = downloader or download_business_profile_candidate
        if shared_asset_access is not None and shared_asset_service is not None:
            raise ValueError(
                "provide shared_asset_access or shared_asset_service, not both"
            )
        if shared_asset_access is None and shared_asset_service is not None:
            from research.announcement_assets import AnnouncementAssetAccess

            shared_asset_access = AnnouncementAssetAccess(
                repository=shared_asset_service.repository,
                config=shared_asset_service.config,
                service=shared_asset_service,
            )
        inferred_mode = "shared_only" if shared_asset_access is not None else "legacy"
        mode = str(annual_report_asset_mode or inferred_mode).strip().lower()
        if mode not in {"legacy", "dual_read", "shared_only"}:
            raise ValueError("invalid business-profile annual-report asset mode")
        expected_shared = mode in {"dual_read", "shared_only"}
        expected_legacy = mode in {"legacy", "dual_read"}
        if (
            shared_annual_report_enabled is not None
            and bool(shared_annual_report_enabled) != expected_shared
        ):
            raise ValueError("annual-report asset mode conflicts with enabled flag")
        if (
            legacy_annual_report_fallback_enabled is not None
            and bool(legacy_annual_report_fallback_enabled) != expected_legacy
        ):
            raise ValueError("annual-report asset mode conflicts with legacy fallback")
        self.shared_asset_access = shared_asset_access
        self.annual_report_asset_mode = mode
        self.shared_annual_report_enabled = expected_shared
        self.legacy_annual_report_fallback_enabled = expected_legacy
        if self.shared_annual_report_enabled and self.shared_asset_access is None:
            raise ValueError(
                "shared annual-report dependency requires shared asset access"
            )

    @classmethod
    def from_research_config(
        cls,
        *,
        storage: Any,
        research_config: Any,
        downloader: Optional[
            Callable[[BusinessProfileDocumentCandidate], bytes]
        ] = None,
        shared_asset_access: Any | None = None,
        shared_asset_service: Any | None = None,
    ) -> "BusinessProfileDocumentArchiveService":
        """Build the archive service from the governed research module config."""
        modules = getattr(research_config, "modules", {}) or {}
        if not isinstance(modules, Mapping):
            raise ValueError("research_config.modules must be a mapping")
        module_cfg = modules.get("business_profile_evidence", {})
        if not isinstance(module_cfg, Mapping):
            raise ValueError("business_profile_evidence config must be a mapping")
        archive_cfg = module_cfg.get("archive", {})
        if not isinstance(archive_cfg, Mapping):
            raise ValueError("business_profile_evidence.archive must be a mapping")
        dependency_cfg = module_cfg.get("annual_report_asset_dependency", {})
        if not isinstance(dependency_cfg, Mapping):
            raise ValueError(
                "business_profile_evidence.annual_report_asset_dependency "
                "must be a mapping"
            )
        dependency_enabled = bool(dependency_cfg.get("enabled", False))
        legacy_fallback_enabled = bool(
            dependency_cfg.get("legacy_fallback_enabled", not dependency_enabled)
        )
        dependency_mode = str(
            dependency_cfg.get(
                "mode", "shared_only" if dependency_enabled else "legacy"
            )
        )
        if dependency_mode == "shared_only" and (
            not str(
                dependency_cfg.get("reconciliation_evidence_id") or ""
            ).strip()
            or dependency_cfg.get("legacy_writer_disabled") is not True
        ):
            raise ValueError(
                "business-profile shared-only cutover requires reconciliation "
                "evidence and legacy writer disablement"
            )
        if shared_asset_access is None and shared_asset_service is None and dependency_enabled:
            shared_asset_access = _build_shared_annual_report_access(research_config)
        return cls(
            storage=storage,
            archive_root=archive_cfg.get(
                "archive_root",
                DEFAULT_BUSINESS_PROFILE_ARCHIVE_ROOT,
            ),
            directory_template=archive_cfg.get(
                "directory_template",
                DEFAULT_BUSINESS_PROFILE_DIRECTORY_TEMPLATE,
            ),
            filename_template=archive_cfg.get(
                "filename_template",
                DEFAULT_BUSINESS_PROFILE_FILENAME_TEMPLATE,
            ),
            downloader=downloader,
            shared_asset_access=shared_asset_access,
            shared_asset_service=shared_asset_service,
            shared_annual_report_enabled=dependency_enabled,
            legacy_annual_report_fallback_enabled=legacy_fallback_enabled,
            annual_report_asset_mode=dependency_mode,
        )

    def archive_candidates(
        self,
        instrument: Dict[str, Any],
        candidates: Sequence[BusinessProfileDocumentCandidate],
        *,
        max_documents: int = 20,
        checkpoint_path: Optional[str | Path] = None,
        parent_ingestion_run_id: Optional[int] = None,
        manifest_ingestion_run_id: Optional[int] = None,
    ) -> BusinessProfileArchiveBatchResult:
        """Archive a bounded batch and retain progress only while work is incomplete."""
        if max_documents < 1:
            raise ValueError("max_documents must be at least 1")
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError("instrument_id is required")
        checkpoint = self._load_checkpoint(checkpoint_path, instrument_id)
        completed = set(checkpoint.get("completed") or [])
        owns_manifest_run = manifest_ingestion_run_id is None
        if owns_manifest_run:
            manifest_ingestion_run_id = self._start_manifest_ingestion_run(
                instrument,
                parent_ingestion_run_id=parent_ingestion_run_id,
            )
        result = BusinessProfileArchiveBatchResult(
            parent_ingestion_run_id=parent_ingestion_run_id,
            manifest_ingestion_run_id=manifest_ingestion_run_id,
        )
        interrupted_error: Optional[str] = None
        try:
            for candidate in candidates:
                fingerprint = self._candidate_fingerprint(candidate)
                if fingerprint in completed:
                    result.skipped_checkpoint += 1
                    continue
                if result.attempted >= max_documents:
                    break
                result.attempted += 1
                try:
                    managed_annual_report = self._is_managed_annual_report(candidate)
                    record = None
                    if managed_annual_report and self.shared_annual_report_enabled:
                        record = self._reuse_shared_annual_report_asset(
                            instrument, candidate
                        )
                        if (
                            record is None
                            and not self.legacy_annual_report_fallback_enabled
                        ):
                            raise RuntimeError(
                                "shared annual-report asset is not locally ready"
                            )
                    if (
                        record is None
                        and managed_annual_report
                        and self.legacy_annual_report_fallback_enabled
                    ):
                        record = self._reuse_annual_report_asset(instrument, candidate)
                    if record is None:
                        if managed_annual_report and not self.legacy_annual_report_fallback_enabled:
                            raise RuntimeError(
                                "legacy annual-report acquisition is disabled"
                            )
                        content = self.downloader(candidate)
                        record = self.archive_content(
                            instrument,
                            candidate,
                            content,
                            manifest_ingestion_run_id=manifest_ingestion_run_id,
                            parent_ingestion_run_id=parent_ingestion_run_id,
                        )
                except Exception as exc:
                    result.failed += 1
                    result.errors.append(
                        {
                            "announcement_id": candidate.announcement_id,
                            "error": str(exc),
                        }
                    )
                    LOGGER.warning(
                        "business-profile document archive failed: instrument_id=%s announcement_id=%s error=%s",
                        instrument_id,
                        candidate.announcement_id,
                        exc,
                    )
                    continue
                result.records.append(record)
                if record.status == "unchanged":
                    result.unchanged += 1
                else:
                    result.archived += 1
                completed.add(fingerprint)
                self._write_checkpoint(checkpoint_path, instrument_id, completed)

            all_fingerprints = {
                self._candidate_fingerprint(item) for item in candidates
            }
            result.checkpoint_complete = (
                result.failed == 0 and all_fingerprints <= completed
            )
            if result.checkpoint_complete:
                self._remove_checkpoint(checkpoint_path)
            elif completed:
                self._write_checkpoint(checkpoint_path, instrument_id, completed)
            return result
        except BaseException as exc:
            interrupted_error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            if owns_manifest_run and manifest_ingestion_run_id is not None:
                self._finish_manifest_ingestion_run(
                    manifest_ingestion_run_id,
                    result,
                    interrupted_error=interrupted_error,
                )

    def _reuse_shared_annual_report_asset(
        self,
        instrument: Mapping[str, Any],
        candidate: BusinessProfileDocumentCandidate,
    ) -> BusinessProfileArchiveRecord | None:
        """Acquire annual PDFs through shared custody and project a manifest row."""

        if self.shared_asset_access is None:
            return None
        document_type = candidate.classification.document_type
        if document_type not in {"annual_report", "annual_report_correction"}:
            return None
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError("shared annual-report identity is incomplete")
        source = str(candidate.source or "cninfo").strip().lower()
        source_announcement_id = str(candidate.announcement_id)
        from research.announcement_assets import EnsureRequest

        access_config = getattr(self.shared_asset_access, "config", None)
        wait_seconds = float(
            getattr(access_config, "wait_seconds_maximum", 30.0)
        )
        ensured = self.shared_asset_access.ensure(
            EnsureRequest(
                instrument_id=instrument_id,
                source=source,
                source_announcement_id=source_announcement_id,
                allow_network=True,
                wait_seconds=wait_seconds,
                consumer="business_profile",
                principal="business-profile",
            )
        )
        asset = ensured.get("asset")
        if not asset or ensured.get("availability") != "local_valid":
            return None
        if (
            str(asset.get("source") or "").strip().lower() != source
            or str(asset.get("source_announcement_id") or "")
            != source_announcement_id
        ):
            raise RuntimeError(
                "shared annual-report selector resolved a different legal filing"
            )
        content = self.shared_asset_access.content_handle(str(asset["asset_id"]))
        handle = content["file_handle"]
        try:
            payload = handle.read()
        finally:
            handle.close()
        content_hash = str(content["content_hash"])
        if hashlib.sha256(payload).hexdigest() != content_hash:
            raise RuntimeError("shared annual-report content handle hash mismatch")
        report_period = infer_business_profile_report_period(
            candidate.title,
            candidate.announcement_time,
        )
        source_file_id = f"shared-asset:{asset['asset_id']}"
        manifest = FinancialSourceFileManifest(
            source=source,
            source_mode="shared_announcement_asset",
            source_tier=str(
                candidate.source_tier or BUSINESS_PROFILE_SOURCE_TIER
            ),
            instrument_id=instrument_id,
            symbol=str(instrument.get("symbol") or "").strip(),
            exchange=str(instrument.get("exchange") or "").strip().upper(),
            report_period=report_period,
            report_type=document_type,
            filing_id=source_announcement_id,
            source_url=str(candidate.adjunct_url),
            archive_path=str(content["path"]),
            content_hash=content_hash,
            content_length=int(content["content_length"]),
            published_at=candidate.announcement_time,
            downloaded_at=get_shanghai_time().isoformat(),
            parser_version=BUSINESS_PROFILE_ARCHIVE_VERSION,
            source_file_id=source_file_id,
            status="archived",
            schema_version=BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
            metadata_json={
                "shared_asset_id": asset["asset_id"],
                "shared_asset_observation_version": asset.get("observation_version"),
                "shared_asset_variant": asset.get("variant"),
                "shared_asset_mode": "read_through_compatibility_manifest",
            },
        )
        existing = self._find_exact_manifest(
            self._get_manifests(
                instrument_id=instrument_id,
                report_period=report_period,
            ),
            source_announcement_id,
            content_hash,
        )
        if existing is None:
            self.storage.upsert_financial_source_file_manifest(manifest)
        return BusinessProfileArchiveRecord(
            announcement_id=source_announcement_id,
            report_period=report_period,
            document_type=document_type,
            content_hash=content_hash,
            archive_path=str(content["path"]),
            source_file_id=str(
                existing["source_file_id"] if existing else source_file_id
            ),
            status="unchanged" if existing else "archived",
        )

    @staticmethod
    def _is_managed_annual_report(
        candidate: BusinessProfileDocumentCandidate,
    ) -> bool:
        return candidate.classification.document_type in {
            "annual_report",
            "annual_report_correction",
        }

    def _reuse_annual_report_asset(
        self,
        instrument: Mapping[str, Any],
        candidate: BusinessProfileDocumentCandidate,
    ) -> BusinessProfileArchiveRecord | None:
        document_type = candidate.classification.document_type
        if document_type not in {"annual_report", "annual_report_correction"}:
            return None
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            return None
        # Compatibility adapter for callers that have not yet supplied the
        # shared service; production construction binds shared custody above.
        from research.annual_report_assets import AnnualReportAssetCatalog

        asset_catalog = AnnualReportAssetCatalog(self.storage)
        report_period = infer_business_profile_report_period(
            candidate.title,
            candidate.announcement_time,
        )
        asset = asset_catalog.find_reusable_filing(
            instrument_id=instrument_id,
            report_period=report_period,
            source=str(candidate.source or "cninfo").strip().lower(),
            filing_id=candidate.announcement_id,
        )
        if asset is None:
            return None
        return BusinessProfileArchiveRecord(
            announcement_id=candidate.announcement_id,
            report_period=report_period,
            document_type=document_type,
            content_hash=str(asset["content_hash"]),
            archive_path=str(asset["archive_path"]),
            source_file_id=str(asset["source_file_id"]),
            status="unchanged",
            supersedes_source_file_id=asset.get("supersedes_source_file_id"),
        )

    def archive_content(
        self,
        instrument: Dict[str, Any],
        candidate: BusinessProfileDocumentCandidate,
        content: bytes,
        *,
        manifest_ingestion_run_id: Optional[int] = None,
        parent_ingestion_run_id: Optional[int] = None,
    ) -> BusinessProfileArchiveRecord:
        """Persist one verified PDF and its versioned source manifest."""
        if not isinstance(content, bytes) or not content:
            raise ValueError("downloaded attachment is empty")
        if not content.lstrip().startswith(b"%PDF-"):
            raise ValueError("downloaded attachment is not a PDF")
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip().upper()
        if not instrument_id or not symbol or not exchange:
            raise ValueError("instrument_id, symbol, and exchange are required")

        report_period = infer_business_profile_report_period(
            candidate.title,
            candidate.announcement_time,
        )
        document_type = candidate.classification.document_type
        document_family = business_profile_document_family(document_type)
        source = str(candidate.source or "cninfo").strip().lower()
        source_tier = str(candidate.source_tier or BUSINESS_PROFILE_SOURCE_TIER).strip()
        if not source:
            raise ValueError("business-profile document source is required")
        if source_tier not in {"official_primary", "official_backup"}:
            raise ValueError(
                f"unsupported business-profile source tier: {candidate.source_tier}"
            )
        content_hash = hashlib.sha256(content).hexdigest()
        rows = self._get_manifests(
            instrument_id=instrument_id,
            report_period=report_period,
        )
        exact = self._find_exact_manifest(rows, candidate.announcement_id, content_hash)
        if exact is not None:
            existing_archive_path = Path(str(exact.get("archive_path") or ""))
            if existing_archive_path.is_file():
                existing_hash = hashlib.sha256(
                    existing_archive_path.read_bytes()
                ).hexdigest()
                if existing_hash != content_hash:
                    raise RuntimeError(
                        "registered business-profile archive hash mismatch: "
                        f"{existing_archive_path}"
                    )
                return BusinessProfileArchiveRecord(
                    announcement_id=candidate.announcement_id,
                    report_period=report_period,
                    document_type=document_type,
                    content_hash=content_hash,
                    archive_path=str(existing_archive_path),
                    source_file_id=str(exact["source_file_id"]),
                    status="unchanged",
                    supersedes_source_file_id=exact.get("supersedes_source_file_id"),
                )
        archive_path = self._archive_path(
            instrument_id=instrument_id,
            exchange=exchange,
            symbol=symbol,
            report_period=report_period,
            document_type=document_type,
            announcement_id=candidate.announcement_id,
            content_hash=content_hash,
        )
        archive_created = self._write_immutable(archive_path, content, content_hash)
        if exact is not None and str(exact.get("archive_path") or "") == str(
            archive_path
        ):
            return BusinessProfileArchiveRecord(
                announcement_id=candidate.announcement_id,
                report_period=report_period,
                document_type=document_type,
                content_hash=content_hash,
                archive_path=str(archive_path),
                source_file_id=str(exact["source_file_id"]),
                status="unchanged",
                supersedes_source_file_id=exact.get("supersedes_source_file_id"),
            )

        superseded = self._find_superseded_manifest(
            rows,
            candidate=candidate,
            document_family=document_family,
            content_hash=content_hash,
        )
        supersedes_source_file_id = (
            None if superseded is None else str(superseded["source_file_id"])
        )
        same_content = any(row.get("content_hash") == content_hash for row in rows)
        manifest = FinancialSourceFileManifest(
            source=source,
            source_mode="direct",
            source_tier=source_tier,
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            report_type=document_type,
            filing_id=candidate.announcement_id,
            source_url=_resolved_business_profile_source_url(candidate),
            archive_path=str(archive_path),
            content_hash=content_hash,
            content_length=len(content),
            published_at=candidate.announcement_time,
            downloaded_at=get_shanghai_time().isoformat(),
            parser_version=BUSINESS_PROFILE_ARCHIVE_VERSION,
            source_file_id=None if exact is None else str(exact["source_file_id"]),
            supersedes_source_file_id=supersedes_source_file_id,
            status="archived_unchanged_content" if same_content else "archived",
            schema_version=BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
            metadata_json={
                "artifact_kind": "business_profile_official_pdf",
                "announcement_title": candidate.title,
                "document_family": document_family,
                "is_correction": candidate.classification.is_correction,
                "profile_event_hints": candidate.classification.profile_event_hints,
                "selection_reasons": candidate.selection_reasons,
                "discovery_source": source,
                "discovery_source_tier": source_tier,
                "parent_ingestion_run": {
                    "domain": "business_profile",
                    "ingestion_run_id": parent_ingestion_run_id,
                },
            },
        )
        try:
            source_file_id = self._upsert_manifest(
                manifest,
                ingestion_run_id=manifest_ingestion_run_id,
            )
        except Exception:
            if archive_created:
                archive_path.unlink(missing_ok=True)
            raise
        return BusinessProfileArchiveRecord(
            announcement_id=candidate.announcement_id,
            report_period=report_period,
            document_type=document_type,
            content_hash=content_hash,
            archive_path=str(archive_path),
            source_file_id=source_file_id,
            status=manifest.status,
            supersedes_source_file_id=supersedes_source_file_id,
        )

    def _get_manifests(self, **kwargs: Any) -> List[Dict[str, Any]]:
        repository = getattr(self.storage, "financial_statements", None)
        if repository is not None and hasattr(repository, "get_source_file_manifests"):
            return repository.get_source_file_manifests(**kwargs)
        return self.storage.get_financial_source_file_manifests(**kwargs)

    def _upsert_manifest(
        self,
        manifest: FinancialSourceFileManifest,
        *,
        ingestion_run_id: Optional[int],
    ) -> str:
        repository = getattr(self.storage, "financial_statements", None)
        if repository is not None and hasattr(
            repository, "upsert_source_file_manifest"
        ):
            return repository.upsert_source_file_manifest(
                manifest,
                ingestion_run_id=ingestion_run_id,
            )
        return self.storage.upsert_financial_source_file_manifest(
            manifest,
            ingestion_run_id=ingestion_run_id,
        )

    @staticmethod
    def _find_exact_manifest(
        rows: Sequence[Dict[str, Any]],
        announcement_id: str,
        content_hash: str,
    ) -> Optional[Dict[str, Any]]:
        for row in rows:
            if (
                row.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
                and str(row.get("filing_id") or "") == announcement_id
                and row.get("content_hash") == content_hash
            ):
                return row
        return None

    @staticmethod
    def _find_superseded_manifest(
        rows: Sequence[Dict[str, Any]],
        *,
        candidate: BusinessProfileDocumentCandidate,
        document_family: str,
        content_hash: str,
    ) -> Optional[Dict[str, Any]]:
        eligible = []
        for row in rows:
            metadata = row.get("metadata") or {}
            same_family = metadata.get("document_family") == document_family
            same_filing = str(row.get("filing_id") or "") == candidate.announcement_id
            if (
                row.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
                and row.get("content_hash") != content_hash
                and same_family
                and (same_filing or candidate.classification.is_correction)
            ):
                eligible.append(row)
        if not eligible:
            return None
        return max(
            eligible,
            key=lambda row: str(row.get("published_at") or row.get("updated_at") or ""),
        )

    def _archive_path(
        self,
        *,
        instrument_id: str,
        exchange: str,
        symbol: str,
        report_period: str,
        document_type: str,
        announcement_id: str,
        content_hash: str,
    ) -> Path:
        context = self._archive_template_context(
            instrument_id=instrument_id,
            exchange=exchange,
            symbol=symbol,
            report_period=report_period,
            document_type=document_type,
            announcement_id=announcement_id,
            content_hash=content_hash,
        )
        directory = self.directory_template.format_map(context)
        filename = self.filename_template.format_map(context)
        relative_directory = Path(directory) if directory else Path()
        if relative_directory.is_absolute() or ".." in relative_directory.parts:
            raise ValueError("archive directory template rendered an unsafe path")
        filename_path = Path(filename)
        if (
            not filename
            or filename_path.name != filename
            or filename_path.suffix.lower() != ".pdf"
        ):
            raise ValueError(
                "archive filename template must render one relative PDF filename"
            )
        return self.archive_root / relative_directory / filename

    @classmethod
    def _archive_template_context(
        cls,
        *,
        instrument_id: str,
        exchange: str,
        symbol: str,
        report_period: str,
        document_type: str,
        announcement_id: str,
        content_hash: str,
    ) -> Dict[str, str]:
        period_date = date.fromisoformat(report_period)
        return {
            "instrument_id": cls._safe_component(
                str(instrument_id).replace(".", "_"),
                field_name="instrument_id",
            ),
            "market": cls._safe_component(exchange, field_name="market"),
            "symbol": cls._safe_component(symbol, field_name="symbol"),
            "year": str(period_date.year),
            "report_period": period_date.isoformat(),
            "period_label": cls._period_label(period_date),
            "document_type": cls._safe_component(
                document_type,
                field_name="document_type",
            ),
            "announcement_id": cls._safe_component(
                announcement_id,
                field_name="announcement_id",
            ),
            "content_hash": cls._safe_component(
                content_hash,
                field_name="content_hash",
            ),
        }

    @staticmethod
    def _period_label(period_date: date) -> str:
        quarter_ends = {
            (3, 31): "Q1",
            (6, 30): "Q2",
            (9, 30): "Q3",
            (12, 31): "Q4",
        }
        quarter = quarter_ends.get((period_date.month, period_date.day))
        return (
            f"{period_date.year}{quarter}"
            if quarter
            else period_date.strftime("%Y%m%d")
        )

    @staticmethod
    def _safe_component(value: str, *, field_name: str) -> str:
        normalized = _SAFE_PATH_RE.sub("_", str(value)).strip("._")
        if not normalized:
            raise ValueError(f"{field_name} is invalid for archive layout")
        return normalized

    @classmethod
    def _validate_layout_template(
        cls,
        value: str,
        *,
        field_name: str,
    ) -> str:
        template = str(value or "")
        if field_name == "filename_template" and not template:
            raise ValueError("filename_template must not be empty")
        cls._template_fields(template)
        return template

    @staticmethod
    def _template_fields(template: str) -> set[str]:
        fields: set[str] = set()
        for _, field_name, format_spec, conversion in Formatter().parse(template):
            if field_name is None:
                continue
            if field_name not in _ARCHIVE_TEMPLATE_FIELDS or format_spec or conversion:
                raise ValueError(
                    f"unsupported business-profile archive template field: "
                    f"{field_name}"
                )
            fields.add(field_name)
        return fields

    @staticmethod
    def _write_immutable(path: Path, content: bytes, content_hash: str) -> bool:
        if path.exists():
            existing_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            if existing_hash != content_hash:
                raise RuntimeError(f"immutable archive hash mismatch: {path}")
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_bytes(content)
        if hashlib.sha256(temporary.read_bytes()).hexdigest() != content_hash:
            temporary.unlink(missing_ok=True)
            raise RuntimeError("archive write hash verification failed")
        os.replace(temporary, path)
        return True

    def _start_manifest_ingestion_run(
        self,
        instrument: Dict[str, Any],
        *,
        parent_ingestion_run_id: Optional[int],
    ) -> Optional[int]:
        if not hasattr(self.storage, "start_ingestion_run"):
            return None
        kwargs = {
            "domain": "financial_business_profile_documents",
            "job_name": "business_profile_document_archive",
            "market": str(instrument.get("exchange") or "").upper() or None,
            "source": "cninfo",
            "mode": "archive",
            "metadata": {
                "instrument_id": instrument.get("instrument_id"),
                "parent_domain": "business_profile",
                "parent_ingestion_run_id": parent_ingestion_run_id,
            },
        }
        financial_scope = getattr(self.storage, "financial_database_scope", None)
        if financial_scope is None:
            return self.storage.start_ingestion_run(**kwargs)
        with financial_scope():
            run_id = self.storage.start_ingestion_run(**kwargs)
        route_ids = getattr(self.storage, "_financial_ingestion_run_ids", None)
        if route_ids is not None:
            route_ids.discard(run_id)
        return run_id

    def _finish_manifest_ingestion_run(
        self,
        run_id: int,
        result: BusinessProfileArchiveBatchResult,
        *,
        interrupted_error: Optional[str],
    ) -> None:
        if not hasattr(self.storage, "finish_ingestion_run"):
            return
        status = (
            "failed" if interrupted_error else "partial" if result.failed else "success"
        )
        try:
            kwargs = {
                "status": status,
                "rows_written": result.archived,
                "error_message": interrupted_error,
                "metadata": {
                    "parent_ingestion_run_id": result.parent_ingestion_run_id,
                    "attempted": result.attempted,
                    "archived": result.archived,
                    "unchanged": result.unchanged,
                    "failed": result.failed,
                    "checkpoint_complete": result.checkpoint_complete,
                },
            }
            financial_scope = getattr(self.storage, "financial_database_scope", None)
            if financial_scope is None:
                self.storage.finish_ingestion_run(run_id, **kwargs)
            else:
                with financial_scope():
                    self.storage.finish_ingestion_run(run_id, **kwargs)
        finally:
            route_ids = getattr(self.storage, "_financial_ingestion_run_ids", None)
            if route_ids is not None:
                route_ids.discard(run_id)

    @staticmethod
    def _candidate_fingerprint(candidate: BusinessProfileDocumentCandidate) -> str:
        payload = {
            "announcement_id": candidate.announcement_id,
            "announcement_time": candidate.announcement_time,
            "adjunct_url": candidate.adjunct_url,
            "document_type": candidate.classification.document_type,
            "source": candidate.source,
            "source_tier": candidate.source_tier,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _load_checkpoint(
        checkpoint_path: Optional[str | Path],
        instrument_id: str,
    ) -> Dict[str, Any]:
        if checkpoint_path is None or not Path(checkpoint_path).exists():
            return {"instrument_id": instrument_id, "completed": []}
        payload = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
        if payload.get("instrument_id") != instrument_id:
            raise ValueError(
                "checkpoint instrument_id does not match requested instrument"
            )
        return payload

    @staticmethod
    def _write_checkpoint(
        checkpoint_path: Optional[str | Path],
        instrument_id: str,
        completed: set[str],
    ) -> None:
        if checkpoint_path is None:
            return
        path = Path(checkpoint_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_text(
            json.dumps(
                {
                    "version": 1,
                    "instrument_id": instrument_id,
                    "completed": sorted(completed),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        os.replace(temporary, path)

    @staticmethod
    def _remove_checkpoint(checkpoint_path: Optional[str | Path]) -> None:
        if checkpoint_path is not None:
            Path(checkpoint_path).unlink(missing_ok=True)
