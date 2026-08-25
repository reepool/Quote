"""Local-first orchestration over common announcement discovery and retrieval."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock

from research.announcements import (
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementAttachmentRetriever,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRouteResult,
    AnnouncementScope,
)

from .classifier import (
    AnnualReportCandidate,
    AnnualReportClassification,
    AnnualReportClassifier,
    WinnerSelection,
    refine_classification_from_pdf,
    select_effective_candidate,
)
from .config import AnnouncementAssetConfig
from .models import (
    AnnualReportVariant,
    AssetAvailability,
    BatchOutcome,
    DocumentFamily,
    EffectiveAnnualReport,
    EffectiveDecisionState,
    EnsureDisposition,
    EnsureRequest,
    EnsureResult,
    IntegrityStatus,
    OfficialAnnouncement,
    OfficialAnnouncementAttachment,
    OfficialAttachmentVersion,
    OfficialDocumentBlob,
    OperationStage,
    OperationStatus,
    ResultOrigin,
    canonical_json,
    normalize_instrument_id,
    stable_id,
    utc_now_iso,
)
from .repository import AnnouncementAssetRepository
from .storage import ContentAddressedBlobStore


@dataclass(frozen=True)
class RegisteredAttachment:
    attachment_id: str
    classification: AnnualReportClassification


ACQUISITION_WORK_FINGERPRINT_VERSION = "annual_report_acquisition_work.v1"
_ENSURE_EXECUTOR = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="annual-report-ensure",
)
_ENSURE_FUTURES: dict[str, Future] = {}
_ENSURE_FUTURES_LOCK = Lock()


def acquisition_work_fingerprint(
    *,
    operation_type: str,
    scope: Mapping[str, object],
    config: AnnouncementAssetConfig,
    accepted_bounds: Mapping[str, object] | None = None,
    integrity_policy: str = "hash",
    retention_policy_version: str = "asset_request_retention.v1",
    configuration_version: str | None = None,
    acquisition_service: AnnouncementAcquisitionService | None = None,
) -> str:
    """Build the global single-flight identity for compatible acquisition work."""
    payload = {
        "fingerprint_version": ACQUISITION_WORK_FINGERPRINT_VERSION,
        "operation_type": str(operation_type).strip(),
        "scope": _canonical_work_scope(scope),
        "accepted_bounds": _canonical_work_value(accepted_bounds or {}),
        "acquisition_policy": _canonical_work_value(asdict(config.acquisition)),
        "retention_policy_version": str(retention_policy_version),
        "configuration_version": (
            None if configuration_version is None else str(configuration_version)
        ),
        "provider_route_capability_matrix": _provider_route_capability_matrix(
            acquisition_service
        ),
        "classifier_integrity_policy": {
            "classifier_version": config.classifier_version,
            "asset_policy_version": config.policy_version,
            "integrity_policy": str(integrity_policy).strip().lower(),
        },
        "relevant_configuration": {
            "exchanges": list(config.exchanges),
            "instrument_type": config.instrument_type,
            "active_only": config.active_only,
            "universe_policy_version": config.universe_policy_version,
            "timezone": config.timezone,
            "archive_root": str(config.archive_root),
            "temp_root": str(config.temp_root),
            "quarantine_root": str(config.quarantine_root),
            "discovery": _canonical_work_value(asdict(config.discovery)),
            "storage": _canonical_work_value(asdict(config.storage)),
            "retry": _canonical_work_value(asdict(config.retry)),
        },
    }
    return stable_id("acquisition-work", canonical_json(payload))


def _provider_route_capability_matrix(
    acquisition_service: AnnouncementAcquisitionService | None,
) -> Mapping[str, object]:
    if acquisition_service is None:
        return {"binding": "not_bound"}
    routes = _canonical_work_value(asdict(acquisition_service.config))
    capabilities = {
        source: _canonical_work_value(asdict(provider.capabilities))
        for source, provider in sorted(
            acquisition_service.registry.providers.items(), key=lambda item: item[0]
        )
    }
    return {
        "binding": "bound",
        "routes": routes,
        "provider_capabilities": capabilities,
    }


def _canonical_work_scope(scope: Mapping[str, object]) -> object:
    normalized = dict(scope)
    if "exchanges" in normalized:
        normalized["exchanges"] = sorted(
            {str(item).strip().upper() for item in normalized["exchanges"] or ()}
        )
    if "sources" in normalized:
        normalized["sources"] = sorted(
            {str(item).strip().lower() for item in normalized["sources"] or ()}
        )
    return _canonical_work_value(normalized)


def _canonical_work_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return _canonical_work_value(asdict(value))
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_work_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (set, frozenset)):
        return sorted((_canonical_work_value(item) for item in value), key=str)
    if isinstance(value, (list, tuple)):
        return [_canonical_work_value(item) for item in value]
    return value


class AnnouncementAssetService:
    """Own metadata, attachment bytes, and effective annual-report decisions."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        blob_store: ContentAddressedBlobStore | None = None,
        classifier: AnnualReportClassifier | None = None,
        acquisition_service: AnnouncementAcquisitionService | None = None,
        attachment_retriever: AnnouncementAttachmentRetriever | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.blob_store = blob_store or ContentAddressedBlobStore(config)
        self.classifier = classifier or AnnualReportClassifier(
            config.classifier_version
        )
        self.acquisition_service = acquisition_service
        self.attachment_retriever = attachment_retriever

    def reconcile_stale_parts(self, *, now: datetime | None = None) -> int:
        """Explicitly reclaim abandoned `.part` files using durable lease fences."""

        timestamp = now or datetime.now(timezone.utc)
        return self.blob_store.cleanup_expired_parts(
            now=timestamp,
            lease_is_active=lambda evidence: self.repository.artifact_lease_is_active(
                evidence,
                now=timestamp.isoformat(),
                safety_grace_seconds=self.config.storage.part_safety_grace_seconds,
            ),
        )

    def register_discovered_record(
        self,
        record: AnnouncementRecord,
        *,
        instrument_id: str | None = None,
    ) -> tuple[RegisteredAttachment, ...]:
        """Persist normalized metadata and attachment-level classifications."""
        announcement = self.repository.upsert_announcement(
            record, instrument_id=instrument_id
        )
        registered: list[RegisteredAttachment] = []
        for attachment in record.attachments:
            classification = self.classifier.classify(record, attachment)
            metadata = dict(attachment.raw_metadata)
            mirror_chain_id = _shared_official_mirror_chain_id(
                source=record.source,
                exchange=record.exchange,
                instrument_id=instrument_id,
                source_announcement_id=record.source_announcement_id,
                published_at=record.published_at,
            )
            if mirror_chain_id is not None:
                metadata.setdefault("legal_chain_id", mirror_chain_id)
            metadata["asset_classification"] = _classification_payload(classification)
            enriched = AnnouncementAttachment(
                source_url=attachment.source_url,
                resolved_url=attachment.resolved_url,
                attachment_id=attachment.attachment_id,
                name=attachment.name,
                media_type=attachment.media_type,
                file_extension=attachment.file_extension,
                raw_metadata=metadata,
            )
            canonical_attachment = self.repository.upsert_attachment(
                announcement.announcement_id, enriched
            )
            registered.append(
                RegisteredAttachment(
                    attachment_id=canonical_attachment.attachment_id,
                    classification=classification,
                )
            )
        return tuple(registered)

    def acquire_attachment(
        self,
        attachment_id: str,
        *,
        attempt: int = 1,
        wait_seconds: float = 0.0,
        force_refresh: bool = False,
        lease_owner: str | None = None,
        knowledge_cutoff: str | None = None,
        operation_id: str | None = None,
        scheduled_write: bool = False,
    ) -> EffectiveAnnualReport | None:
        """Acquire one attachment under a durable attachment-scoped lease."""
        owner = lease_owner or f"asset-worker-{uuid.uuid4().hex}"
        deadline = time.monotonic() + max(0.0, float(wait_seconds))
        while True:
            reusable = (
                None
                if force_refresh
                else self.repository.get_latest_valid_attachment_version(attachment_id)
            )
            if reusable and reusable.content_hash:
                blob = self.repository.get_blob(reusable.content_hash)
                if blob is not None:
                    validation = self.blob_store.validate_blob(
                        blob.canonical_path,
                        expected_hash=blob.content_hash,
                        expected_length=blob.content_length,
                    )
                    if validation.status is IntegrityStatus.VALID:
                        attachment = self.repository.get_attachment(attachment_id)
                        announcement = (
                            None
                            if attachment is None
                            else self.repository.get_announcement(
                                attachment.announcement_id
                            )
                        )
                        classification = (
                            None
                            if attachment is None
                            else _classification_from_metadata(
                                dict(attachment.metadata)
                            )
                        )
                        if attachment is not None and classification is not None:
                            classification = self._refine_attachment_classification(
                                attachment,
                                classification,
                                blob.canonical_path,
                            )
                        if (
                            announcement is not None
                            and announcement.instrument_id
                            and classification is not None
                            and classification.fiscal_year
                        ):
                            return self._resolve_after_acquisition(
                                instrument_id=announcement.instrument_id,
                                classification=classification,
                                preferred_version_id=reusable.version_id,
                                knowledge_cutoff=knowledge_cutoff,
                            )
            if self.config.dry_run:
                raise RuntimeError("annual_report_asset_dry_run_blocks_acquisition")
            lease_expires_at = (
                datetime.now(timezone.utc)
                + timedelta(seconds=self.config.retry.lease_seconds)
            ).isoformat()
            if self.repository.acquire_attachment_lease(
                attachment_id,
                lease_owner=owner,
                lease_expires_at=lease_expires_at,
            ):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("attachment acquisition is already in progress")
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        lease_generation: int | None = None
        try:
            lease = self.repository.get_attachment_lease(attachment_id)
            if lease is None or lease["lease_owner"] != owner:
                raise RuntimeError(
                    "attachment lease evidence disappeared before download"
                )
            lease_generation = int(lease["lease_generation"])
            return self._acquire_attachment_under_lease(
                attachment_id,
                attempt=attempt,
                lease_owner=owner,
                lease_generation=lease_generation,
                force_refresh=force_refresh,
                knowledge_cutoff=knowledge_cutoff,
                operation_id=operation_id,
                scheduled_write=scheduled_write,
            )
        finally:
            if lease_generation is not None:
                self.repository.release_attachment_lease(
                    attachment_id,
                    lease_owner=owner,
                    lease_generation=lease_generation,
                )

    def verify_candidate_attachment(
        self,
        attachment_id: str,
        *,
        operation_id: str,
        max_bytes: int,
        policy_version: str = "bootstrap_candidate_verification.v1",
        attempt: int = 1,
    ) -> OfficialAttachmentVersion:
        """Hash a competing candidate without publishing canonical bytes."""

        if not str(operation_id or "").strip():
            raise ValueError("candidate verification requires an operation id")
        if policy_version != "bootstrap_candidate_verification.v1":
            raise ValueError("unsupported candidate verification policy")
        if int(max_bytes) <= 0:
            raise ValueError("candidate verification byte bound must be positive")
        if int(max_bytes) < self.config.storage.max_attachment_bytes:
            raise ValueError(
                "candidate verification remaining budget is below the attachment bound"
            )
        if self.attachment_retriever is None:
            raise RuntimeError("announcement attachment retriever is not configured")
        existing = self.repository.get_latest_attachment_version(attachment_id)
        if (
            existing is not None
            and existing.retrieval_status == "candidate_verified"
            and existing.integrity_status is IntegrityStatus.VALID
            and existing.content_hash_observed
        ):
            return existing
        if self.config.dry_run:
            raise RuntimeError("annual_report_asset_dry_run_blocks_acquisition")

        attachment = self.repository.get_attachment(attachment_id)
        if attachment is None:
            raise KeyError(f"attachment not found: {attachment_id}")
        announcement = self.repository.get_announcement(attachment.announcement_id)
        if announcement is None:
            raise KeyError(f"announcement not found: {attachment.announcement_id}")
        owner = f"candidate-verifier-{uuid.uuid4().hex}"
        lease_expires_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=self.config.retry.lease_seconds)
        ).isoformat()
        if not self.repository.acquire_attachment_lease(
            attachment_id,
            lease_owner=owner,
            lease_expires_at=lease_expires_at,
        ):
            raise RuntimeError("attachment acquisition is already in progress")
        generation: int | None = None
        try:
            lease = self.repository.get_attachment_lease(attachment_id)
            if lease is None or lease["lease_owner"] != owner:
                raise RuntimeError("candidate verification lease evidence disappeared")
            generation = int(lease["lease_generation"])
            source_attachment = AnnouncementAttachment(
                source_url=attachment.source_url,
                resolved_url=attachment.normalized_source_url,
                attachment_id=attachment.source_attachment_id,
                name=attachment.name,
                media_type=attachment.media_type,
                file_extension=(
                    "pdf"
                    if str(attachment.name or "").lower().endswith(".pdf")
                    else None
                ),
                raw_metadata=dict(attachment.metadata),
            )
            result = self.attachment_retriever.retrieve(
                announcement.source,
                source_attachment,
                require_pdf=True,
            )
            observed_at = result.retrieved_at or utc_now_iso()
            diagnostics = dict(result.diagnostics)
            version_available_at = str(
                diagnostics.get("version_available_at")
                or diagnostics.get("available_at")
                or observed_at
            )
            observation_key = stable_id(
                "candidate-verification-observation",
                attachment_id,
                result.final_url or attachment.normalized_source_url,
                result.content_hash or result.status,
                policy_version,
            )
            if result.status != "success" or not result.content:
                return self.repository.add_attachment_version(
                    OfficialAttachmentVersion(
                        version_id=stable_id(
                            "candidate-verification-version",
                            attachment_id,
                            observation_key,
                        ),
                        attachment_id=attachment_id,
                        observation_key=observation_key,
                        content_hash=None,
                        final_url=result.final_url,
                        retrieval_status="candidate_verification_failed",
                        integrity_status=IntegrityStatus.MISSING,
                        attempt=attempt,
                        next_retry_at=None,
                        error_code=(
                            result.errors[0]
                            if result.errors
                            else "candidate_retrieval_failed"
                        ),
                        observed_at=observed_at,
                        version_available_at=version_available_at,
                        response_evidence={
                            "policy_version": policy_version,
                            "status": result.status,
                            "errors": list(result.errors),
                            "diagnostics": diagnostics,
                        },
                        lease_owner=owner,
                        lease_generation=generation,
                        metadata={"cleanup_outcome": "not_applicable_no_bytes"},
                    )
                )

            actual_length = len(result.content)
            if actual_length > int(max_bytes):
                raise ValueError("candidate verification exceeds remaining byte budget")
            if actual_length > self.config.storage.max_attachment_bytes:
                raise ValueError("candidate exceeds configured annual-report limit")
            planned_hint = int(
                attachment.content_length_hint
                or self.config.storage.unknown_length_reservation_bytes
            )
            if planned_hint > int(max_bytes):
                raise ValueError(
                    "candidate verification reservation exceeds remaining byte budget"
                )
            with self._storage_reservation(
                attachment,
                attempt=attempt,
                operation_id=operation_id,
            ) as reservation:
                self.blob_store.preflight_capacity(actual_length)
                temporary_path, mount = self.blob_store.write_candidate_part(
                    artifact_identity=observation_key,
                    content=result.content,
                    metadata={
                        "owner": owner,
                        "lease_owner": owner,
                        "generation": generation,
                        "lease_generation": generation,
                        "operation_id": operation_id,
                        "attachment_id": attachment_id,
                        "reason": "candidate_verification",
                    },
                )
                validation = self.blob_store.validate_blob(
                    temporary_path,
                    expected_hash=result.content_hash,
                    expected_length=actual_length,
                )
                quarantine_path = None
                cleanup_outcome = "deleted"
                try:
                    self.blob_store.remove_candidate_part(
                        temporary_path, expected_mount=mount
                    )
                except (OSError, RuntimeError, ValueError):
                    cleanup_outcome = "quarantined"
                    quarantine_path = self.blob_store.quarantine_candidate_part(
                        temporary_path,
                        expected_mount=mount,
                        reason="candidate_cleanup_failed",
                        metadata={
                            "owner": owner,
                            "operation_id": operation_id,
                            "attachment_id": attachment_id,
                            "content_hash": validation.content_hash,
                        },
                    )
                version = self.repository.add_attachment_version(
                    OfficialAttachmentVersion(
                        version_id=stable_id(
                            "candidate-verification-version",
                            attachment_id,
                            observation_key,
                        ),
                        attachment_id=attachment_id,
                        observation_key=observation_key,
                        content_hash=None,
                        final_url=result.final_url,
                        retrieval_status=(
                            "candidate_verified"
                            if validation.status is IntegrityStatus.VALID
                            else "candidate_rejected"
                        ),
                        integrity_status=validation.status,
                        attempt=attempt,
                        next_retry_at=None,
                        error_code=(
                            None
                            if validation.status is IntegrityStatus.VALID
                            else validation.status.value
                        ),
                        observed_at=observed_at,
                        version_available_at=version_available_at,
                        response_evidence={
                            "policy_version": policy_version,
                            "status": result.status,
                            "final_url": result.final_url,
                            "response_media_type": result.response_media_type,
                            "signature_status": result.signature_status,
                            "diagnostics": diagnostics,
                        },
                        content_length_observed=validation.content_length,
                        content_hash_observed=validation.content_hash,
                        lease_owner=owner,
                        lease_generation=generation,
                        temporary_path=str(temporary_path),
                        temporary_bytes=actual_length,
                        quarantine_path=(
                            None if quarantine_path is None else str(quarantine_path)
                        ),
                        metadata={
                            "candidate_verification_policy_version": policy_version,
                            "cleanup_outcome": cleanup_outcome,
                            "canonical_blob_published": False,
                        },
                    )
                )
                reservation["status"] = "completed"
                return version
        finally:
            if generation is not None:
                self.repository.release_attachment_lease(
                    attachment_id,
                    lease_owner=owner,
                    lease_generation=generation,
                )

    def _acquire_attachment_under_lease(
        self,
        attachment_id: str,
        *,
        attempt: int = 1,
        lease_owner: str,
        lease_generation: int,
        force_refresh: bool,
        knowledge_cutoff: str | None,
        operation_id: str | None,
        scheduled_write: bool,
    ) -> EffectiveAnnualReport | None:
        """Retrieve one registered attachment, publish it, and recompute policy."""
        if self.attachment_retriever is None:
            raise RuntimeError("announcement attachment retriever is not configured")
        attachment = self.repository.get_attachment(attachment_id)
        if attachment is None:
            raise KeyError(f"attachment not found: {attachment_id}")
        announcement = self.repository.get_announcement(attachment.announcement_id)
        if announcement is None:
            raise KeyError(f"announcement not found: {attachment.announcement_id}")
        reusable = (
            None
            if force_refresh
            else self.repository.get_latest_valid_attachment_version(attachment_id)
        )
        if reusable and reusable.content_hash:
            blob = self.repository.get_blob(reusable.content_hash)
            if blob is not None:
                validation = self.blob_store.validate_blob(
                    blob.canonical_path,
                    expected_hash=blob.content_hash,
                    expected_length=blob.content_length,
                )
                if validation.status is IntegrityStatus.VALID:
                    classification = self._refine_attachment_classification(
                        attachment,
                        _classification_from_metadata(attachment.metadata),
                        blob.canonical_path,
                    )
                    if announcement.instrument_id and classification.fiscal_year:
                        return self._resolve_after_acquisition(
                            instrument_id=announcement.instrument_id,
                            classification=classification,
                            preferred_version_id=reusable.version_id,
                            knowledge_cutoff=knowledge_cutoff,
                        )
                else:
                    self.repository.update_blob_integrity(
                        blob.content_hash, validation.status
                    )
                    if validation.status is not IntegrityStatus.MISSING:
                        self.blob_store.quarantine_blob(
                            blob.content_hash,
                            reason=validation.status.value,
                            artifact_metadata={
                                "owner": lease_owner,
                                "generation": lease_generation,
                                "attachment_id": attachment_id,
                            },
                        )
                        self.repository.update_blob_integrity(
                            blob.content_hash, IntegrityStatus.QUARANTINED
                        )
        with self._storage_reservation(
            attachment,
            attempt=attempt,
            operation_id=operation_id,
        ) as reservation:
            return self._retrieve_publish_and_recompute(
                attachment=attachment,
                announcement=announcement,
                attempt=attempt,
                reservation=reservation,
                lease_owner=lease_owner,
                lease_generation=lease_generation,
                knowledge_cutoff=knowledge_cutoff,
                operation_id=operation_id,
                suppress_unchanged_event=force_refresh,
                scheduled_write=scheduled_write,
            )

    def _retrieve_publish_and_recompute(
        self,
        *,
        attachment: OfficialAnnouncementAttachment,
        announcement: OfficialAnnouncement,
        attempt: int,
        reservation: dict[str, object],
        lease_owner: str,
        lease_generation: int,
        knowledge_cutoff: str | None = None,
        operation_id: str | None = None,
        suppress_unchanged_event: bool = False,
        scheduled_write: bool = False,
    ) -> EffectiveAnnualReport | None:
        source_attachment = AnnouncementAttachment(
            source_url=attachment.source_url,
            resolved_url=attachment.normalized_source_url,
            attachment_id=attachment.source_attachment_id,
            name=attachment.name,
            media_type=attachment.media_type,
            file_extension="pdf"
            if str(attachment.name or "").lower().endswith(".pdf")
            else None,
            raw_metadata=dict(attachment.metadata),
        )
        result = self.attachment_retriever.retrieve(
            announcement.source,
            source_attachment,
            require_pdf=True,
        )
        observed_at = result.retrieved_at or utc_now_iso()
        diagnostics = dict(result.diagnostics)
        version_available_at = str(
            diagnostics.get("version_available_at")
            or diagnostics.get("available_at")
            or observed_at
        )
        available_time_source = str(
            diagnostics.get("available_time_source") or "first_observed"
        )
        available_time_precision = str(
            diagnostics.get("available_time_precision") or "instant"
        )
        observation_key = stable_id(
            "obs",
            attachment.attachment_id,
            result.final_url or attachment.normalized_source_url,
            result.content_hash or "failed",
        )
        if result.status != "success" or not result.content_hash:
            self.repository.add_attachment_version(
                OfficialAttachmentVersion(
                    version_id=stable_id(
                        "ver", attachment.attachment_id, observation_key
                    ),
                    attachment_id=attachment.attachment_id,
                    observation_key=observation_key,
                    content_hash=None,
                    final_url=result.final_url,
                    retrieval_status=result.status,
                    integrity_status=IntegrityStatus.MISSING,
                    attempt=attempt,
                    next_retry_at=None,
                    error_code=(
                        result.errors[0] if result.errors else "retrieval_failed"
                    ),
                    observed_at=observed_at,
                    version_available_at=version_available_at,
                    available_time_source=available_time_source,
                    available_time_precision=available_time_precision,
                    metadata=diagnostics,
                )
            )
            classification = _classification_from_metadata(attachment.metadata)
            if announcement.instrument_id and classification.fiscal_year:
                return self._resolve_after_acquisition(
                    instrument_id=announcement.instrument_id,
                    classification=classification,
                    knowledge_cutoff=knowledge_cutoff,
                    suppress_unchanged_event=suppress_unchanged_event,
                )
            return None

        actual_length = len(result.content)
        if (
            result.content_length is not None
            and int(result.content_length) != actual_length
        ):
            raise ValueError(
                "attachment reported content length does not match streamed bytes"
            )
        if actual_length > self.config.storage.max_attachment_bytes:
            raise ValueError("attachment exceeds configured annual-report limit")
        self.blob_store.preflight_capacity(actual_length)

        published = self.blob_store.publish_bytes(
            result.content,
            expected_hash=result.content_hash,
            artifact_metadata={
                "owner": lease_owner,
                "lease_generation": lease_generation,
                "attachment_id": attachment.attachment_id,
                "operation_id": operation_id,
            },
        )
        self.repository.register_blob(
            OfficialDocumentBlob(
                content_hash=published.content_hash,
                content_length=published.content_length,
                canonical_path=str(published.path),
                signature_status="valid_pdf",
                integrity_status=IntegrityStatus.VALID,
                first_available_at=observed_at,
                last_verified_at=observed_at,
            )
        )
        version = self.repository.add_attachment_version(
            OfficialAttachmentVersion(
                version_id=stable_id("ver", attachment.attachment_id, observation_key),
                attachment_id=attachment.attachment_id,
                observation_key=observation_key,
                content_hash=published.content_hash,
                final_url=result.final_url,
                retrieval_status="success",
                integrity_status=IntegrityStatus.VALID,
                attempt=attempt,
                next_retry_at=None,
                error_code=None,
                observed_at=observed_at,
                version_available_at=version_available_at,
                available_time_source=available_time_source,
                available_time_precision=available_time_precision,
                metadata=diagnostics,
            )
        )
        classification = self._refine_attachment_classification(
            attachment,
            _classification_from_metadata(attachment.metadata),
            result.content,
        )
        if not announcement.instrument_id or not classification.fiscal_year:
            reservation["status"] = "completed"
            return None
        effective = self._resolve_after_acquisition(
            instrument_id=announcement.instrument_id,
            classification=classification,
            preferred_version_id=version.version_id,
            knowledge_cutoff=knowledge_cutoff,
            suppress_unchanged_event=suppress_unchanged_event,
        )
        reservation["status"] = "completed"
        return effective

    def _refine_attachment_classification(
        self,
        attachment: OfficialAnnouncementAttachment,
        classification: AnnualReportClassification,
        pdf: bytes | str,
    ) -> AnnualReportClassification:
        refined = refine_classification_from_pdf(
            classification,
            Path(pdf) if isinstance(pdf, str) else pdf,
        )
        if refined == classification:
            return classification
        metadata = dict(attachment.metadata)
        metadata["asset_classification"] = _classification_payload(refined)
        self.repository.update_attachment_metadata(
            attachment.attachment_id,
            metadata,
        )
        return refined

    @contextmanager
    def _storage_reservation(
        self,
        attachment: OfficialAnnouncementAttachment,
        *,
        attempt: int,
        operation_id: str | None,
    ) -> Iterator[dict[str, object]]:
        """Apply the configured local storage bound before writing bytes."""
        del attempt, operation_id
        planned = int(
            attachment.content_length_hint
            or self.config.storage.unknown_length_reservation_bytes
        )
        if planned > self.config.storage.max_attachment_bytes:
            raise ValueError("attachment length hint exceeds configured limit")
        self.blob_store.preflight_capacity(planned)
        state: dict[str, object] = {"status": "failed"}
        yield state

    def _resolve_after_acquisition(
        self,
        *,
        instrument_id: str,
        classification: AnnualReportClassification,
        preferred_version_id: str | None = None,
        knowledge_cutoff: str | None = None,
        suppress_unchanged_event: bool = False,
    ) -> EffectiveAnnualReport | None:
        if classification.fiscal_year is None or classification.document_family is None:
            return None
        if classification.document_family == DocumentFamily.ANNUAL_REPORT.value:
            return self.recompute_effective_report(
                instrument_id,
                classification.fiscal_year,
                preferred_version_id=preferred_version_id,
                knowledge_cutoff=knowledge_cutoff,
                suppress_unchanged_event=suppress_unchanged_event,
            )
        if classification.document_family == DocumentFamily.SEMIANNUAL_REPORT.value:
            return self.resolve_effective_report(
                instrument_id,
                fiscal_year=classification.fiscal_year,
                document_family=classification.document_family,
                knowledge_cutoff=(knowledge_cutoff or utc_now_iso()),
            )
        return None

    def recompute_effective_report(
        self,
        instrument_id: str,
        fiscal_year: int,
        *,
        preferred_version_id: str | None = None,
        policy_migration: Mapping[str, object] | None = None,
        knowledge_cutoff: str | None = None,
        suppress_unchanged_event: bool = False,
    ) -> EffectiveAnnualReport | None:
        del preferred_version_id  # Selection always considers all committed evidence.
        evidence_cutoff = (
            None
            if knowledge_cutoff is None
            else _inclusive_knowledge_cutoff(knowledge_cutoff)
        )
        for _ in range(8):
            rows = self.repository.list_candidate_rows(
                instrument_id=instrument_id,
                fiscal_year=fiscal_year,
                observation_cutoff=evidence_cutoff,
            )
            rows = [
                row
                for row in rows
                if _classification_from_payload(
                    row.get("classification") or {}
                ).document_family
                == DocumentFamily.ANNUAL_REPORT.value
            ]
            if evidence_cutoff is not None:
                rows = [
                    row
                    for row in rows
                    if _row_evidence_visible_at(row, evidence_cutoff)
                ]
            candidates = _apply_withdrawal_relations(
                rows,
                tuple(_candidate_from_row(row) for row in rows),
            )
            current_report = self.repository.get_effective_report(
                instrument_id, fiscal_year
            )
            current_candidate = None
            if current_report is not None:
                current_candidate = next(
                    (
                        item
                        for item in candidates
                        if item.candidate_id == current_report.version_id
                    ),
                    None,
                )
            selection = select_effective_candidate(
                candidates,
                current=current_candidate,
            )
            migration_evidence = self._validate_provisional_policy_migration(
                current_report=current_report,
                policy_migration=policy_migration,
            )
            projection_candidate = selection.winner
            if selection.winner is None:
                if (
                    current_report is not None
                    and current_candidate is not None
                    and current_candidate.withdrawn
                    and current_candidate.withdrawal_target_id
                    and current_candidate.withdrawal_evidence_type
                ):
                    _, withdrawn = (
                        self.repository.withdraw_effective_report_without_replacement(
                            instrument_id,
                            fiscal_year,
                            expected_current_asset_id=current_report.asset_id,
                            classifier_version=(current_report.classifier_version),
                            decision_policy_version=(
                                current_report.canonical_projection_policy_version
                            ),
                            decision_reasons=selection.reasons
                            + ("withdrawal_target_bound",),
                            decision_evidence={
                                "withdrawal_target_id": (
                                    current_candidate.withdrawal_target_id
                                ),
                                "withdrawal_evidence_type": (
                                    current_candidate.withdrawal_evidence_type
                                ),
                                "selection_reasons": list(selection.reasons),
                            },
                            activated_at=utc_now_iso(),
                        )
                    )
                    if withdrawn:
                        return None
                if (
                    current_report is None
                    and selection.state is EffectiveDecisionState.AMBIGUOUS
                    and selection.pending_candidate is not None
                ):
                    projection_candidate = selection.pending_candidate
                else:
                    return current_report
            winner_row = next(
                row
                for row in rows
                if (row.get("version_id") or row["attachment_id"])
                == projection_candidate.candidate_id
            )
            classification = _classification_from_payload(winner_row["classification"])
            canonical_filing = selection.canonical_source_filing
            content_hash = winner_row.get("content_hash")
            now = utc_now_iso()
            conflicting_candidate_ids = tuple(
                sorted(
                    item.candidate_id
                    for item in candidates
                    if item.integrity_valid
                    and item.classification.is_eligible
                    and not item.withdrawn
                )
            )
            conflicting_observations = tuple(
                {
                    "candidate_id": item.candidate_id,
                    "source": item.source,
                    "source_announcement_id": item.source_announcement_id,
                    "attachment_id": item.attachment_id,
                    "content_hash": item.content_hash,
                    "version_available_at": item.version_available_at,
                }
                for item in sorted(
                    (
                        item
                        for item in candidates
                        if item.integrity_valid
                        and item.classification.is_eligible
                        and not item.withdrawn
                    ),
                    key=lambda item: item.candidate_id,
                )
            )
            provisional_policy_version = (
                self.config.provisional_result.policy_version
                if selection.state
                in {
                    EffectiveDecisionState.AMBIGUOUS,
                    EffectiveDecisionState.PROVISIONAL,
                }
                else None
            )
            # A newly observed legal mirror with identical verified bytes may
            # change the canonical source projection, but it must not change
            # consumer processing identity. Only a content change creates a
            # new effective asset id for the same instrument and fiscal year.
            asset_id = (
                current_report.asset_id
                if current_report is not None
                and current_report.content_hash
                and current_report.content_hash == content_hash
                else stable_id(
                    "asset",
                    instrument_id,
                    fiscal_year,
                    winner_row["attachment_id"],
                    winner_row.get("version_id") or "metadata",
                )
            )
            report = EffectiveAnnualReport(
                asset_id=asset_id,
                instrument_id=instrument_id,
                fiscal_year=int(fiscal_year),
                report_period=(classification.report_period or f"{fiscal_year}-12-31"),
                announcement_id=winner_row["announcement_id"],
                attachment_id=winner_row["attachment_id"],
                version_id=winner_row.get("version_id")
                or stable_id("metadata", winner_row["attachment_id"]),
                content_hash=content_hash,
                source=(
                    canonical_filing.source
                    if canonical_filing is not None
                    else winner_row["source"]
                ),
                source_announcement_id=(
                    canonical_filing.source_announcement_id
                    if canonical_filing is not None
                    else winner_row["source_announcement_id"]
                ),
                published_at=winner_row.get("published_at"),
                variant=classification.variant or AnnualReportVariant.ORIGINAL,
                classifier_version=classification.policy_version,
                decision_state=selection.state,
                availability=(
                    AssetAvailability.AMBIGUOUS
                    if selection.winner is None
                    and selection.state is EffectiveDecisionState.AMBIGUOUS
                    else AssetAvailability.BLOCKED
                    if selection.state
                    in {
                        EffectiveDecisionState.AMBIGUOUS,
                        EffectiveDecisionState.PROVISIONAL,
                    }
                    and not self.config.provisional_result.enabled
                    else AssetAvailability.LOCAL_VALID
                    if content_hash
                    and winner_row.get("blob_integrity_status")
                    == IntegrityStatus.VALID.value
                    else AssetAvailability.METADATA_ONLY
                ),
                predecessor_asset_id=(
                    current_report.asset_id
                    if current_report is not None
                    and current_report.asset_id != asset_id
                    else current_report.predecessor_asset_id
                    if current_report
                    else None
                ),
                pending_candidate_id=(
                    selection.pending_candidate.candidate_id
                    if selection.pending_candidate
                    else None
                ),
                activated_at=(
                    current_report.activated_at
                    if current_report is not None
                    and current_report.asset_id == asset_id
                    else now
                ),
                last_checked_at=now,
                decision_reasons=selection.reasons,
                equivalent_source_filings=selection.equivalent_source_filings,
                canonical_projection_policy_version=(
                    selection.canonical_projection_policy_version
                ),
                evidence_set_hash=selection.evidence_set_hash,
                decision_evidence={
                    "winner_version_id": (
                        None
                        if selection.winner is None
                        else selection.winner.candidate_id
                    ),
                    "canonical_source_filing": (
                        canonical_filing.as_dict()
                        if canonical_filing is not None
                        else None
                    ),
                    "selection_state": selection.state.value,
                    "conflicting_candidate_ids": (
                        list(conflicting_candidate_ids)
                        if selection.state is EffectiveDecisionState.AMBIGUOUS
                        else []
                    ),
                    "conflicting_observations": (
                        list(conflicting_observations)
                        if selection.state is EffectiveDecisionState.AMBIGUOUS
                        else []
                    ),
                    "pending_observation": (
                        {
                            "candidate_id": selection.pending_candidate.candidate_id,
                            "source": selection.pending_candidate.source,
                            "source_announcement_id": (
                                selection.pending_candidate.source_announcement_id
                            ),
                            "attachment_id": selection.pending_candidate.attachment_id,
                            "content_hash": selection.pending_candidate.content_hash,
                            "version_available_at": (
                                selection.pending_candidate.version_available_at
                            ),
                        }
                        if selection.pending_candidate is not None
                        else None
                    ),
                    "provisional_result_enabled": (
                        self.config.provisional_result.enabled
                        if provisional_policy_version
                        else None
                    ),
                    "provisional_result_policy_version": (provisional_policy_version),
                    "decision_policy_version": (
                        provisional_policy_version
                        or selection.canonical_projection_policy_version
                    ),
                    "policy_migration": migration_evidence,
                },
            )
            if (
                suppress_unchanged_event
                and current_report is not None
                and replace(
                    current_report,
                    last_checked_at=report.last_checked_at,
                )
                == report
            ):
                # A byte verification that resolves to the exact same immutable
                # observation is a check, not an effective-asset change. Avoid
                # manufacturing a repaired event/decision for unchanged bytes.
                return current_report
            committed, _, activated = self.repository.activate_effective_report(
                report,
                expected_current_asset_id=(
                    None if current_report is None else current_report.asset_id
                ),
            )
            if activated:
                return committed
        raise RuntimeError("effective annual-report activation remained contended")

    def resolve_effective_reports(
        self,
        *,
        knowledge_cutoff: str,
        document_family: str = "annual_report",
        instrument_id: str | None = None,
        fiscal_year: int | None = None,
        source: str | None = None,
        availability: AssetAvailability | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EffectiveAnnualReport]:
        """Select immutable report assets from evidence visible at a cutoff.

        This is deliberately read-only. Historical consumers must not rewrite
        today's effective projection while reconstructing what was knowable at
        an earlier date.
        """

        family = str(document_family or "").strip().lower()
        if family not in {
            DocumentFamily.ANNUAL_REPORT.value,
            DocumentFamily.SEMIANNUAL_REPORT.value,
        }:
            raise ValueError(f"unsupported effective report family: {family}")
        cutoff = _inclusive_knowledge_cutoff(knowledge_cutoff)
        rows = self.repository.list_candidate_rows(
            instrument_id=instrument_id,
            fiscal_year=fiscal_year,
            observation_cutoff=cutoff,
        )
        visible_rows = [
            row
            for row in rows
            if _row_evidence_visible_at(row, cutoff)
            and bool(str(row.get("canonical_path") or "").strip())
            and _classification_from_payload(
                row.get("classification") or {}
            ).document_family
            == family
        ]
        grouped: dict[tuple[str, int, str], list[dict[str, object]]] = {}
        for row in visible_rows:
            classification = _classification_from_payload(
                row.get("classification") or {}
            )
            if classification.fiscal_year is None:
                continue
            key = (
                str(row.get("instrument_id") or ""),
                int(classification.fiscal_year),
                str(classification.report_period or ""),
            )
            grouped.setdefault(key, []).append(row)

        reports: list[EffectiveAnnualReport] = []
        for (group_instrument_id, group_fiscal_year, _), group_rows in grouped.items():
            candidates = _apply_withdrawal_relations(
                group_rows,
                tuple(_candidate_from_row(row) for row in group_rows),
            )
            selection = select_effective_candidate(candidates)
            candidate = selection.winner
            if (
                candidate is None
                and selection.state is EffectiveDecisionState.AMBIGUOUS
            ):
                candidate = selection.pending_candidate
            if candidate is None:
                continue
            winner_row = next(
                row
                for row in group_rows
                if (row.get("version_id") or row["attachment_id"])
                == candidate.candidate_id
            )
            report = self._historical_effective_projection(
                instrument_id=group_instrument_id,
                fiscal_year=group_fiscal_year,
                document_family=family,
                row=winner_row,
                selection=selection,
                cutoff=cutoff,
            )
            if source and report.source != str(source).strip().lower():
                continue
            if availability is not None and report.availability is not availability:
                continue
            reports.append(report)

        reports.sort(
            key=lambda item: (
                item.instrument_id,
                -item.fiscal_year,
                item.report_period,
                item.published_at or "",
                item.asset_id,
            )
        )
        bounded_offset = max(0, int(offset))
        bounded_limit = max(1, min(int(limit), 1000))
        return reports[bounded_offset : bounded_offset + bounded_limit]

    def resolve_effective_report(
        self,
        instrument_id: str,
        *,
        fiscal_year: int | None = None,
        document_family: str = "annual_report",
        knowledge_cutoff: str,
    ) -> EffectiveAnnualReport | None:
        reports = self.resolve_effective_reports(
            knowledge_cutoff=knowledge_cutoff,
            document_family=document_family,
            instrument_id=instrument_id,
            fiscal_year=fiscal_year,
            limit=1,
        )
        return reports[0] if reports else None

    def _historical_effective_projection(
        self,
        *,
        instrument_id: str,
        fiscal_year: int,
        document_family: str,
        row: Mapping[str, object],
        selection: WinnerSelection,
        cutoff: str,
    ) -> EffectiveAnnualReport:
        classification = _classification_from_payload(row.get("classification") or {})
        canonical_filing = selection.canonical_source_filing
        content_hash = row.get("content_hash")
        candidate = selection.winner or selection.pending_candidate
        if candidate is None:
            raise ValueError("historical effective projection has no candidate")
        availability = (
            AssetAvailability.AMBIGUOUS
            if selection.winner is None
            and selection.state is EffectiveDecisionState.AMBIGUOUS
            else AssetAvailability.BLOCKED
            if selection.state
            in {EffectiveDecisionState.AMBIGUOUS, EffectiveDecisionState.PROVISIONAL}
            and not self.config.provisional_result.enabled
            else AssetAvailability.LOCAL_VALID
            if content_hash
            and row.get("blob_integrity_status") == IntegrityStatus.VALID.value
            else AssetAvailability.METADATA_ONLY
        )
        asset_id = stable_id(
            "asset",
            instrument_id,
            fiscal_year,
            row["attachment_id"],
            row.get("version_id") or "metadata",
        )
        return EffectiveAnnualReport(
            asset_id=asset_id,
            instrument_id=instrument_id,
            fiscal_year=fiscal_year,
            report_period=(
                classification.report_period
                or (
                    f"{fiscal_year}-06-30"
                    if document_family == DocumentFamily.SEMIANNUAL_REPORT.value
                    else f"{fiscal_year}-12-31"
                )
            ),
            announcement_id=str(row["announcement_id"]),
            attachment_id=str(row["attachment_id"]),
            version_id=str(
                row.get("version_id") or stable_id("metadata", row["attachment_id"])
            ),
            content_hash=None if content_hash is None else str(content_hash),
            source=(
                canonical_filing.source
                if canonical_filing is not None
                else str(row["source"])
            ),
            source_announcement_id=(
                canonical_filing.source_announcement_id
                if canonical_filing is not None
                else str(row["source_announcement_id"])
            ),
            published_at=None
            if row.get("published_at") is None
            else str(row["published_at"]),
            variant=classification.variant or AnnualReportVariant.ORIGINAL,
            classifier_version=classification.policy_version,
            decision_state=selection.state,
            availability=availability,
            predecessor_asset_id=None,
            pending_candidate_id=(
                None
                if selection.pending_candidate is None
                else selection.pending_candidate.candidate_id
            ),
            activated_at=(
                None
                if row.get("version_available_at") is None
                else str(row["version_available_at"])
            ),
            last_checked_at=cutoff,
            decision_reasons=selection.reasons,
            equivalent_source_filings=selection.equivalent_source_filings,
            canonical_projection_policy_version=(
                selection.canonical_projection_policy_version
            ),
            evidence_set_hash=selection.evidence_set_hash,
            decision_evidence={
                "projection_kind": "knowledge_cutoff_read",
                "knowledge_cutoff": cutoff,
                "winner_version_id": candidate.candidate_id,
            },
            document_family=document_family,
        )

    def _validate_provisional_policy_migration(
        self,
        *,
        current_report: EffectiveAnnualReport | None,
        policy_migration: Mapping[str, object] | None,
    ) -> dict[str, object] | None:
        if current_report is None or current_report.decision_state not in {
            EffectiveDecisionState.AMBIGUOUS,
            EffectiveDecisionState.PROVISIONAL,
        }:
            return None
        evidence = dict(current_report.decision_evidence)
        current_version = str(
            evidence.get("provisional_result_policy_version")
            or evidence.get("decision_policy_version")
            or ""
        )
        target_version = self.config.provisional_result.policy_version
        current_enabled = evidence.get("provisional_result_enabled")
        target_enabled = self.config.provisional_result.enabled
        if current_version == target_version and current_enabled in {
            None,
            target_enabled,
        }:
            return None
        migration = dict(policy_migration or {})
        required = {
            "from_policy_version": current_version,
            "to_policy_version": target_version,
            "actor": str(migration.get("actor") or "").strip(),
            "reason": str(migration.get("reason") or "").strip(),
        }
        if (
            migration.get("from_policy_version") != current_version
            or migration.get("to_policy_version") != target_version
            or not required["actor"]
            or not required["reason"]
        ):
            raise ValueError(
                "provisional result policy change requires explicit migration evidence"
            )
        return {
            **required,
            "from_enabled": current_enabled,
            "to_enabled": target_enabled,
            "migrated_at": utc_now_iso(),
        }

    def ensure_annual_report(self, request: EnsureRequest) -> EnsureResult:
        """Resolve locally first and create one durable bounded operation if needed."""
        # An exact source-filing selector may include instrument identity as
        # context.  Branch on fiscal-year presence, otherwise an exact request
        # could accidentally return the stock's unrelated current report.
        if request.fiscal_year is not None:
            asset = (
                self.resolve_effective_report(
                    request.instrument_id,
                    fiscal_year=request.fiscal_year,
                    document_family=DocumentFamily.ANNUAL_REPORT.value,
                    knowledge_cutoff=request.knowledge_cutoff,
                )
                if request.knowledge_cutoff is not None
                else self.repository.get_effective_report(
                    request.instrument_id,
                    request.fiscal_year,
                )
            )
            if asset and self._asset_is_local_valid(asset):
                return EnsureResult(
                    disposition=EnsureDisposition.LOCAL_HIT,
                    availability=AssetAvailability.LOCAL_VALID,
                    asset=asset,
                )
            candidates = self.repository.list_candidate_rows(
                instrument_id=request.instrument_id,
                fiscal_year=request.fiscal_year,
                observation_cutoff=(
                    None
                    if request.knowledge_cutoff is None
                    else _inclusive_knowledge_cutoff(request.knowledge_cutoff)
                ),
            )
            candidates = [
                row
                for row in candidates
                if _classification_from_payload(
                    row.get("classification") or {}
                ).document_family
                == DocumentFamily.ANNUAL_REPORT.value
            ]
        else:
            all_candidates = self.repository.list_candidate_rows(
                source=request.source,
                source_announcement_id=request.source_announcement_id,
            )
            candidates = self._filter_exact_candidates(all_candidates, request)
            asset = self._effective_for_exact_candidates(candidates, request=request)
            if asset and self._asset_is_local_valid(asset):
                return EnsureResult(
                    disposition=EnsureDisposition.LOCAL_HIT,
                    availability=AssetAvailability.LOCAL_VALID,
                    asset=asset,
                )
            if all_candidates and not candidates:
                return EnsureResult(
                    disposition=EnsureDisposition.LOCAL_MISS,
                    availability=AssetAvailability.METADATA_ONLY,
                    reason_code="exact_filing_pin_unavailable",
                )
            if candidates and asset is None:
                return EnsureResult(
                    disposition=EnsureDisposition.LOCAL_MISS,
                    availability=AssetAvailability.METADATA_ONLY,
                    reason_code=(
                        "retained_internal_only"
                        if self._has_retained_exact_content(candidates)
                        else "non_effective_exact_filing_content_unavailable"
                    ),
                )

        availability = (
            AssetAvailability.METADATA_ONLY if candidates else AssetAvailability.MISSING
        )
        if not request.allow_network:
            return EnsureResult(
                disposition=EnsureDisposition.LOCAL_MISS,
                availability=availability,
                asset=asset,
                reason_code="network_disabled",
            )
        if self.config.dry_run:
            return EnsureResult(
                disposition=EnsureDisposition.LOCAL_MISS,
                availability=availability,
                asset=asset,
                reason_code="dry_run_blocks_network_acquisition",
            )
        scope = request.normalized_scope
        principal = str(request.principal or request.consumer or "internal").strip()
        accepted_bounds = {
            "max_pages": self.config.discovery.max_pages,
            "max_requests": self.config.discovery.max_requests,
            "max_windows": self.config.discovery.max_windows,
            "max_instruments": self.config.discovery.max_instruments,
            "max_elapsed_seconds": self.config.discovery.max_elapsed_seconds,
            "max_attachment_bytes": self.config.storage.max_attachment_bytes,
            "max_task_download_bytes": self.config.acquisition.max_task_download_bytes,
        }
        operation_work_fingerprint = acquisition_work_fingerprint(
            operation_type="ensure_annual_report",
            scope=scope,
            config=self.config,
            accepted_bounds=accepted_bounds,
            integrity_policy=request.integrity_level,
            acquisition_service=self.acquisition_service,
        )
        operation_key = stable_id("ensure-scope", operation_work_fingerprint)
        request_fingerprint = canonical_json(
            {
                "scope": dict(scope),
                "policy_version": self.config.policy_version,
                "integrity_level": request.integrity_level,
                "consumer": request.consumer,
                "acquisition_work_fingerprint": operation_work_fingerprint,
            }
        )
        request_key = request.idempotency_key or stable_id(
            "asset-request",
            principal,
            operation_key,
            request_fingerprint,
        )
        asset_request, operation, subscription_created, _ = (
            self.repository.create_or_reuse_asset_request(
                operation_type="ensure_annual_report",
                operation_idempotency_key=operation_key,
                scope={
                    **scope,
                    "acquisition_work_fingerprint": operation_work_fingerprint,
                    "accepted_bounds": accepted_bounds,
                },
                policy_version=self.config.policy_version,
                principal=principal,
                request_idempotency_key=request_key,
                request_fingerprint=request_fingerprint,
                consumer=request.consumer,
                consumer_continuation_id=request.consumer_continuation_id,
                authorized_projection={
                    "schema_version": "asset_request_projection.v1",
                    "scope": dict(scope),
                    "integrity_level": request.integrity_level,
                    "allowed_progress_fields": [
                        "candidate_count",
                        "asset_id",
                        "current_stage",
                    ],
                    "allowed_diagnostics_fields": [
                        "error_type",
                        "retry_item_status",
                        "failure_class",
                        "operator_action_required",
                    ],
                },
                stage=(
                    OperationStage.DOWNLOADING
                    if candidates
                    else OperationStage.DISCOVERING
                ),
            )
        )
        disposition = (
            EnsureDisposition.OPERATION_CREATED
            if subscription_created
            else EnsureDisposition.OPERATION_REUSED
        )
        result = EnsureResult(
            disposition=disposition,
            availability=availability,
            asset=asset,
            operation=operation,
            asset_request=asset_request,
        )
        future = self._submit_ensure_operation(operation.operation_id)
        wait_seconds = min(
            float(
                self.config.wait_seconds_default
                if request.wait_seconds is None
                else request.wait_seconds
            ),
            float(self.config.wait_seconds_maximum),
        )
        if wait_seconds <= 0:
            return result
        try:
            terminal = future.result(timeout=wait_seconds)
        except FutureTimeoutError:
            return result
        except RuntimeError:
            # Another subscriber may already own the globally single-flight
            # operation. Its durable state remains the polling authority.
            terminal = self.repository.get_operation(operation.operation_id)
        if terminal is None:
            return result
        if terminal.status is OperationStatus.COMPLETED and terminal.result_asset_id:
            completed_asset = self.repository.get_effective_report_by_asset_id(
                terminal.result_asset_id
            )
            if completed_asset is not None and self._asset_is_local_valid(
                completed_asset
            ):
                return EnsureResult(
                    disposition=EnsureDisposition.LOCAL_HIT,
                    availability=AssetAvailability.LOCAL_VALID,
                    asset=completed_asset,
                    operation=terminal,
                    asset_request=asset_request,
                )
        if terminal.status is OperationStatus.MISSING:
            return EnsureResult(
                disposition=EnsureDisposition.LOCAL_MISS,
                availability=AssetAvailability.MISSING,
                operation=terminal,
                asset_request=asset_request,
                reason_code=terminal.reason_code or "annual_report_not_found",
            )
        return replace(result, operation=terminal)

    def _submit_ensure_operation(self, operation_id: str):
        """Best-effort in-process dispatch over durable database truth."""
        operation = self.repository.get_operation(operation_id)
        if operation is None:
            raise KeyError(operation_id)
        if operation.status not in {OperationStatus.QUEUED, OperationStatus.RUNNING}:
            from concurrent.futures import Future

            completed = Future()
            completed.set_result(operation)
            return completed
        with _ENSURE_FUTURES_LOCK:
            existing = _ENSURE_FUTURES.get(operation_id)
            if existing is not None and not existing.done():
                return existing
            future = _ENSURE_EXECUTOR.submit(
                self.execute_ensure_operation,
                operation_id,
                lease_owner=f"ensure-worker:{operation_id}",
            )
            _ENSURE_FUTURES[operation_id] = future

        def release(completed):
            with _ENSURE_FUTURES_LOCK:
                if _ENSURE_FUTURES.get(operation_id) is completed:
                    _ENSURE_FUTURES.pop(operation_id, None)

        future.add_done_callback(release)
        return future

    def resume_pending_ensure_operations(
        self,
        *,
        limit: int = 100,
        now: str | None = None,
    ) -> tuple[str, ...]:
        """Explicitly dispatch durable queued or stale-running ensure work.

        Callers invoke this from a governed worker/startup recovery phase.  It
        is intentionally not called by module import or service construction.
        """
        bounded = max(1, min(int(limit), 1000))
        now_time = (
            datetime.now(timezone.utc) if now is None else _ensure_discovery_cutoff(now)
        )
        pending = self.repository.list_operations(
            operation_type="ensure_annual_report",
            status=OperationStatus.QUEUED,
            limit=bounded,
        )
        remaining = max(0, bounded - len(pending))
        if remaining:
            for operation in self.repository.list_operations(
                operation_type="ensure_annual_report",
                status=OperationStatus.RUNNING,
                limit=remaining,
            ):
                lease = operation.lease_expires_at
                if lease and _ensure_discovery_cutoff(lease) > now_time:
                    continue
                pending.append(operation)
        dispatched: list[str] = []
        for operation in pending[:bounded]:
            self._submit_ensure_operation(operation.operation_id)
            dispatched.append(operation.operation_id)
        return tuple(dispatched)

    def execute_ensure_operation(
        self,
        operation_id: str,
        *,
        lease_owner: str,
    ):
        """Execute one durable bounded ensure operation after HTTP returns."""
        operation = self.repository.get_operation(operation_id)
        if operation is None or operation.operation_type != "ensure_annual_report":
            raise KeyError("annual-report ensure operation was not found")
        if self.config.dry_run:
            raise RuntimeError("annual_report_asset_dry_run_blocks_job_execution")
        claimed = self.repository.claim_operation(
            operation_id,
            lease_owner=lease_owner,
            lease_expires_at=(
                datetime.now(timezone.utc)
                + timedelta(seconds=self.config.retry.lease_seconds)
            ).isoformat(),
            stage=operation.stage or OperationStage.DISCOVERING,
        )
        scope = dict(claimed.scope)
        try:
            rows = self._operation_candidates(scope)
            if not rows:
                rows = self._discover_operation_candidates(scope)
            if not rows:
                return self.repository.transition_operation(
                    operation_id,
                    OperationStatus.MISSING,
                    outcome=BatchOutcome.PARTIAL,
                    reason_code="annual_report_not_found",
                    progress={"candidate_count": 0},
                    expected_lease_owner=lease_owner,
                    expected_lease_generation=claimed.lease_generation,
                )
            prospective = self._prospective_operation_candidate(rows, scope)
            last_asset = None
            reused = True
            if prospective is not None:
                row = prospective
                attachment_id = str(row["attachment_id"])
                had_valid = (
                    self.repository.get_latest_valid_attachment_version(attachment_id)
                    is not None
                )
                last_asset = self.acquire_attachment(
                    attachment_id,
                    wait_seconds=0,
                    lease_owner=f"{lease_owner}:{operation_id}",
                    knowledge_cutoff=scope.get("knowledge_cutoff"),
                    operation_id=operation_id,
                )
                reused = reused and had_valid
            if last_asset is None:
                return self.repository.transition_operation(
                    operation_id,
                    OperationStatus.BLOCKED,
                    outcome=BatchOutcome.BLOCKED,
                    reason_code="candidate_not_effective",
                    progress={"candidate_count": len(rows)},
                    expected_lease_owner=lease_owner,
                    expected_lease_generation=claimed.lease_generation,
                )
            return self.repository.transition_operation(
                operation_id,
                OperationStatus.COMPLETED,
                outcome=BatchOutcome.SUCCESS,
                result_asset_id=last_asset.asset_id,
                result_origin=(
                    ResultOrigin.ADOPTED if reused else ResultOrigin.DOWNLOADED
                ),
                progress={
                    "candidate_count": len(rows),
                    "asset_id": last_asset.asset_id,
                },
                expected_lease_owner=lease_owner,
                expected_lease_generation=claimed.lease_generation,
            )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            latest = self.repository.get_operation(operation_id)
            if latest is not None and (
                latest.status is not OperationStatus.RUNNING
                or latest.lease_owner != lease_owner
                or latest.lease_generation != claimed.lease_generation
            ):
                return latest
            return self.repository.transition_operation(
                operation_id,
                OperationStatus.FAILED,
                outcome=BatchOutcome.FAILED,
                reason_code="ensure_execution_failed",
                diagnostics={"error_type": type(exc).__name__, "error": str(exc)},
                expected_lease_owner=lease_owner,
                expected_lease_generation=claimed.lease_generation,
            )

    def _operation_candidates(self, scope: dict) -> list[dict]:
        if not scope.get("source_announcement_id"):
            return self.repository.list_candidate_rows(
                instrument_id=str(scope["instrument_id"]),
                fiscal_year=int(scope["fiscal_year"]),
            )
        rows = self.repository.list_candidate_rows(
            source=str(scope.get("source") or ""),
            source_announcement_id=str(scope.get("source_announcement_id") or ""),
        )
        request = EnsureRequest(
            instrument_id=scope.get("instrument_id"),
            source=str(scope.get("source") or ""),
            source_announcement_id=str(scope.get("source_announcement_id") or ""),
            attachment_id=scope.get("attachment_id"),
            expected_content_hash=scope.get("expected_content_hash"),
            observation_version=scope.get("observation_version"),
            knowledge_cutoff=scope.get("knowledge_cutoff"),
        )
        return self._filter_exact_candidates(rows, request)

    def _prospective_operation_candidate(
        self, rows: Iterable[dict], scope: Mapping[str, object]
    ) -> dict | None:
        """Choose one metadata winner before any network acquisition.

        Ordinary ensure is deliberately single-shot: an invalid prospective
        correction must not cause the worker to download an older original.
        Metadata is ranked with the same classifier policy as effective
        projection, while treating rows as provisionally integrity-valid only
        for this in-memory decision.  No verification state is persisted.
        """
        candidates = [
            row for row in rows if (row.get("classification") or {}).get("is_eligible")
        ]
        if not candidates:
            return None
        # A cross-source choice without hash/legal-chain evidence is unsafe.
        sources = {str(row.get("source") or "") for row in candidates}
        if len(sources) > 1:
            hashes = {str(row.get("content_hash") or "") for row in candidates}
            chains = {
                str((row.get("attachment_metadata") or {}).get("legal_chain_id") or "")
                for row in candidates
            }
            same_hash = len(hashes) == 1 and "" not in hashes
            same_chain = len(chains) == 1 and "" not in chains
            if not same_hash and not same_chain:
                return None
        current = None
        instrument_id = scope.get("instrument_id")
        fiscal_year = scope.get("fiscal_year")
        if instrument_id and fiscal_year is not None:
            effective = self.repository.get_effective_report(
                str(instrument_id), int(fiscal_year)
            )
            if effective is not None:
                current_row = next(
                    (
                        row
                        for row in candidates
                        if str(row.get("attachment_id")) == str(effective.attachment_id)
                    ),
                    None,
                )
                if current_row is not None:
                    current = _candidate_from_row(
                        {
                            **current_row,
                            "integrity_status": IntegrityStatus.VALID.value,
                            "blob_integrity_status": IntegrityStatus.VALID.value,
                        }
                    )
        selection = select_effective_candidate(
            [
                _candidate_from_row(
                    {
                        **row,
                        "integrity_status": IntegrityStatus.VALID.value,
                        "blob_integrity_status": IntegrityStatus.VALID.value,
                    }
                )
                for row in candidates
            ],
            current=current,
        )
        if (
            selection.winner is None
            or selection.state is EffectiveDecisionState.AMBIGUOUS
        ):
            return None
        winner_id = selection.winner.attachment_id
        return next(
            (
                row
                for row in candidates
                if str(row.get("attachment_id")) == str(winner_id)
            ),
            None,
        )

    def _has_retained_exact_content(self, candidates: Iterable[dict]) -> bool:
        """Whether an exact non-effective observation still has valid local bytes."""
        for row in candidates:
            content_hash = str(row.get("content_hash") or "").strip().lower()
            if (
                not content_hash
                or row.get("integrity_status") != IntegrityStatus.VALID.value
            ):
                continue
            blob = self.repository.get_blob(content_hash)
            if blob is None:
                continue
            validation = self.blob_store.validate_blob(
                blob.canonical_path,
                expected_hash=blob.content_hash,
                expected_length=blob.content_length,
            )
            if validation.status is IntegrityStatus.VALID:
                return True
        return False

    def _discover_operation_candidates(self, scope: dict) -> list[dict]:
        if self.acquisition_service is None:
            raise RuntimeError("announcement discovery service is not configured")
        if scope.get("source_announcement_id"):
            return self._discover_exact_operation_candidates(scope)
        instrument_id = str(scope["instrument_id"])
        symbol, _, suffix = instrument_id.partition(".")
        exchange = {
            "SH": "SSE",
            "SSE": "SSE",
            "SZ": "SZSE",
            "SZSE": "SZSE",
            "BJ": "BSE",
            "BSE": "BSE",
        }.get(suffix.upper())
        if exchange is None:
            raise ValueError("unsupported A-share instrument exchange")
        cutoff = datetime.now(timezone.utc)
        start = cutoff - timedelta(days=self.config.discovery.initial_lookback_days)
        route = self.acquisition_service.config.route_for(
            "official_announcement_assets", exchange
        )
        records: list[AnnouncementRecord] = []
        for source in route.sources:
            result = self.acquisition_service.acquire(
                AnnouncementQuery(
                    purpose_key="official_announcement_assets",
                    source=source,
                    scope=AnnouncementScope(
                        exchange=exchange,
                        instrument_id=instrument_id,
                        symbol=symbol,
                        start_date=start.isoformat(),
                        end_date=cutoff.isoformat(),
                        category="annual_report",
                        page_size=self.config.discovery.page_size,
                        max_pages=self.config.discovery.max_pages,
                    ),
                )
            )
            if not (
                isinstance(result, AnnouncementRouteResult)
                and result.scan_result is not None
                and result.scan_result.cursor_commit_allowed
            ):
                raise RuntimeError(
                    f"instrument discovery did not complete for source {source}"
                )
            scan = result.scan_result
            records.extend(scan.selected_records or scan.records)
        self._validate_discovered_record_instruments(records, instrument_id)
        for record in records:
            self.register_discovered_record(record, instrument_id=instrument_id)
        return self.repository.list_candidate_rows(
            instrument_id=instrument_id,
            fiscal_year=int(scope["fiscal_year"]),
        )

    def _discover_exact_operation_candidates(self, scope: dict) -> list[dict]:
        instrument_id = str(scope.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError(
                "bounded exact-filing discovery requires instrument identity"
            )
        source = str(scope.get("source") or "").strip().lower()
        source_announcement_id = str(scope.get("source_announcement_id") or "").strip()
        symbol, _, suffix = instrument_id.partition(".")
        exchange = {
            "SH": "SSE",
            "SSE": "SSE",
            "SZ": "SZSE",
            "SZSE": "SZSE",
            "BJ": "BSE",
            "BSE": "BSE",
        }.get(suffix.upper())
        if exchange is None:
            raise ValueError("unsupported A-share instrument exchange")
        cutoff = _ensure_discovery_cutoff(scope.get("knowledge_cutoff"))
        start = cutoff - timedelta(days=self.config.discovery.initial_lookback_days)

        def exact_selector(record: AnnouncementRecord) -> tuple[str, ...]:
            if (
                record.source == source
                and record.source_announcement_id == source_announcement_id
            ):
                return ("exact_source_announcement_id",)
            return ()

        result = self.acquisition_service.acquire(
            AnnouncementQuery(
                purpose_key="official_announcement_assets",
                source=source,
                scope=AnnouncementScope(
                    exchange=exchange,
                    instrument_id=instrument_id,
                    symbol=symbol,
                    start_date=start.isoformat(),
                    end_date=cutoff.isoformat(),
                    category="annual_report",
                    page_size=self.config.discovery.page_size,
                    max_pages=self.config.discovery.max_pages,
                    source_options={
                        "exact_source_announcement_id": source_announcement_id,
                    },
                ),
            ),
            selectors=(exact_selector,),
        )
        records: tuple[AnnouncementRecord, ...] = ()
        complete = False
        if (
            isinstance(result, AnnouncementRouteResult)
            and result.scan_result is not None
        ):
            scan = result.scan_result
            records = tuple(scan.selected_records)
            complete = scan.cursor_commit_allowed
        if not complete:
            raise RuntimeError("exact-filing discovery did not complete")
        self._validate_discovered_record_instruments(records, instrument_id)
        for record in records:
            if (
                record.source != source
                or record.source_announcement_id != source_announcement_id
            ):
                raise RuntimeError("exact-filing discovery returned another identity")
            self.register_discovered_record(record, instrument_id=instrument_id)
        request = EnsureRequest(
            instrument_id=instrument_id,
            source=source,
            source_announcement_id=source_announcement_id,
            attachment_id=scope.get("attachment_id"),
            expected_content_hash=scope.get("expected_content_hash"),
            observation_version=scope.get("observation_version"),
            knowledge_cutoff=scope.get("knowledge_cutoff"),
        )
        return self._filter_exact_candidates(
            self.repository.list_candidate_rows(
                source=source,
                source_announcement_id=source_announcement_id,
            ),
            request,
        )

    @staticmethod
    def _validate_discovered_record_instruments(
        records: Iterable[AnnouncementRecord],
        instrument_id: str,
    ) -> None:
        """Reject an instrument-scoped provider response before catalog mutation."""
        normalized_instrument = normalize_instrument_id(instrument_id)
        target_symbol = normalized_instrument.split(".", 1)[0]
        for record in records:
            record_symbols = {
                str(value).strip().upper()
                for value in record.symbols
                if str(value).strip()
            }
            normalized_symbols = {value.split(".", 1)[0] for value in record_symbols}
            if (
                normalized_instrument not in record_symbols
                and target_symbol not in normalized_symbols
            ):
                raise RuntimeError(
                    "instrument-scoped discovery returned a record for another instrument"
                )

    def _asset_is_local_valid(self, asset: EffectiveAnnualReport) -> bool:
        if (
            asset.availability is not AssetAvailability.LOCAL_VALID
            or not asset.content_hash
        ):
            return False
        blob = self.repository.get_blob(asset.content_hash)
        if blob is None:
            return False
        validation = self.blob_store.validate_blob(
            blob.canonical_path,
            expected_hash=blob.content_hash,
            expected_length=blob.content_length,
        )
        return validation.status is IntegrityStatus.VALID

    def _effective_for_exact_candidates(
        self,
        candidates: Iterable[dict],
        *,
        request: EnsureRequest,
    ) -> EffectiveAnnualReport | None:
        for row in candidates:
            classification = row.get("classification") or {}
            fiscal_year = classification.get("fiscal_year")
            instrument_id = row.get("instrument_id")
            if not fiscal_year or not instrument_id:
                continue
            effective = self.repository.get_effective_report(
                instrument_id, int(fiscal_year)
            )
            if effective is None or effective.attachment_id != row.get("attachment_id"):
                continue
            if request.observation_version and str(request.observation_version) not in {
                str(effective.version_id),
                str(row.get("version_id") or ""),
                str(row.get("observation_key") or ""),
            }:
                continue
            if (
                request.expected_content_hash
                and str(effective.content_hash or "").lower()
                != str(request.expected_content_hash).lower()
            ):
                continue
            return effective
        return None

    @staticmethod
    def _filter_exact_candidates(
        candidates: Iterable[dict], request: EnsureRequest
    ) -> list[dict]:
        cutoff = (
            None
            if request.knowledge_cutoff is None
            else datetime.fromisoformat(
                str(request.knowledge_cutoff).replace("Z", "+00:00")
            )
        )
        if cutoff is not None and cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        output: list[dict] = []
        for row in candidates:
            if request.attachment_id and str(request.attachment_id) not in {
                str(row.get("attachment_id") or ""),
                str(row.get("source_attachment_id") or ""),
            }:
                continue
            if request.observation_version and str(request.observation_version) not in {
                str(row.get("version_id") or ""),
                str(row.get("observation_key") or ""),
            }:
                continue
            if (
                request.expected_content_hash
                and str(row.get("content_hash") or "").lower()
                != str(request.expected_content_hash).lower()
            ):
                continue
            if cutoff is not None:
                available_at = row.get("version_available_at") or row.get(
                    "published_at"
                )
                if available_at:
                    parsed = datetime.fromisoformat(
                        str(available_at).replace("Z", "+00:00")
                    )
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    if parsed.astimezone(timezone.utc) > cutoff.astimezone(
                        timezone.utc
                    ):
                        continue
            output.append(row)
        return output


def _ensure_discovery_cutoff(value: object) -> datetime:
    if value in (None, ""):
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _inclusive_knowledge_cutoff(value: object) -> str:
    """Treat a date-only financial cutoff as the complete Shanghai day."""

    text = str(value or "").strip()
    if len(text) == 10:
        return f"{text}T23:59:59.999999+08:00"
    return text


def _row_evidence_visible_at(row: Mapping[str, object], cutoff: str) -> bool:
    availability_time = row.get("published_at") or row.get(
        "announcement_first_observed_at"
    )
    if not availability_time:
        return False
    try:
        available = datetime.fromisoformat(
            str(availability_time).replace("Z", "+00:00")
        )
        bound = datetime.fromisoformat(str(cutoff).replace("Z", "+00:00"))
    except ValueError:
        return False
    if available.tzinfo is None:
        available = available.replace(tzinfo=timezone.utc)
    if bound.tzinfo is None:
        bound = bound.replace(tzinfo=timezone.utc)
    return available.astimezone(timezone.utc) <= bound.astimezone(timezone.utc)


def _classification_payload(
    classification: AnnualReportClassification,
) -> dict[str, object]:
    return {
        "document_family": classification.document_family,
        "fiscal_year": classification.fiscal_year,
        "report_period": classification.report_period,
        "variant": (
            None if classification.variant is None else classification.variant.value
        ),
        "is_full_report": classification.is_full_report,
        "is_eligible": classification.is_eligible,
        "correction_evidence": classification.correction_evidence,
        "reasons": list(classification.reasons),
        "policy_version": classification.policy_version,
        "vocabulary_version": classification.vocabulary_version,
    }


def _classification_from_metadata(
    metadata: dict | object,
) -> AnnualReportClassification:
    raw = dict(metadata) if isinstance(metadata, dict) else {}
    return _classification_from_payload(raw.get("asset_classification") or {})


def _classification_from_payload(raw: dict | object) -> AnnualReportClassification:
    value = dict(raw) if isinstance(raw, dict) else {}
    variant = value.get("variant")
    return AnnualReportClassification(
        document_family=value.get("document_family"),
        fiscal_year=(int(value["fiscal_year"]) if value.get("fiscal_year") else None),
        report_period=value.get("report_period"),
        variant=AnnualReportVariant(str(variant)) if variant else None,
        is_full_report=bool(value.get("is_full_report", False)),
        is_eligible=bool(value.get("is_eligible", False)),
        correction_evidence=bool(value.get("correction_evidence", False)),
        reasons=tuple(str(item) for item in value.get("reasons", ())),
        # A missing version denotes a legacy payload.  It must not inherit the
        # current classifier identity without actually being reclassified.
        policy_version=str(value.get("policy_version") or "formal_annual_report.v1"),
        vocabulary_version=str(
            value.get("vocabulary_version") or "official_document_classification.v1"
        ),
    )


def _candidate_from_row(row: dict) -> AnnualReportCandidate:
    classification = _classification_from_payload(row.get("classification") or {})
    metadata = row.get("attachment_metadata") or {}
    announcement_metadata = row.get("announcement_metadata") or {}
    withdrawal_target = _withdrawal_target_id(metadata, announcement_metadata)
    withdrawal_evidence_type = _withdrawal_evidence_type(
        metadata, announcement_metadata
    )
    candidate_ids = {
        str(value)
        for value in (
            row.get("version_id"),
            row.get("attachment_id"),
            row.get("source_attachment_id"),
            row.get("announcement_id"),
            row.get("source_announcement_id"),
        )
        if value
    }
    withdrawn = bool(
        withdrawal_target
        and withdrawal_target in candidate_ids
        and withdrawal_evidence_type
    )
    return AnnualReportCandidate(
        candidate_id=row.get("version_id") or row["attachment_id"],
        source=row["source"],
        source_announcement_id=row["source_announcement_id"],
        attachment_id=row["attachment_id"],
        content_hash=row.get("content_hash"),
        published_at=row.get("published_at"),
        classification=classification,
        integrity_valid=(
            row.get("integrity_status") == IntegrityStatus.VALID.value
            and row.get("blob_integrity_status") == IntegrityStatus.VALID.value
        ),
        version_available_at=row.get("version_available_at"),
        withdrawn=withdrawn,
        withdrawal_target_id=withdrawal_target,
        withdrawal_evidence_type=withdrawal_evidence_type,
        legal_chain_id=(
            metadata.get("legal_chain_id")
            or _shared_official_mirror_chain_id(
                source=row.get("source"),
                exchange=row.get("exchange"),
                instrument_id=row.get("instrument_id"),
                source_announcement_id=row.get("source_announcement_id"),
                published_at=row.get("published_at"),
            )
            or _cninfo_same_title_revision_chain_id(
                source=row.get("source"),
                exchange=row.get("exchange"),
                instrument_id=row.get("instrument_id"),
                title=row.get("title"),
                fiscal_year=classification.fiscal_year,
                variant=classification.variant,
                published_at=row.get("published_at"),
                source_announcement_id=row.get("source_announcement_id"),
            )
            or _same_source_filing_chain_id(
                source=row.get("source"),
                exchange=row.get("exchange"),
                instrument_id=row.get("instrument_id"),
                source_announcement_id=row.get("source_announcement_id"),
                published_at=row.get("published_at"),
            )
        ),
        legal_precedence=(
            int(metadata["legal_precedence"])
            if metadata.get("legal_precedence") is not None
            else _cninfo_announcement_precedence(
                source=row.get("source"),
                source_announcement_id=row.get("source_announcement_id"),
                title=row.get("title"),
                published_at=row.get("published_at"),
            )
        ),
    )


def _shared_official_mirror_chain_id(
    *,
    source: object,
    exchange: object,
    instrument_id: object,
    source_announcement_id: object,
    published_at: object,
) -> str | None:
    """Bind CNInfo and exchange mirrors carrying the same official filing id."""

    normalized_source = str(source or "").strip().lower()
    normalized_exchange = str(exchange or "").strip().upper()
    normalized_instrument = str(instrument_id or "").strip().upper()
    announcement_id = str(source_announcement_id or "").strip()
    published_date = str(published_at or "").strip()[:10]
    official_source = {
        "SSE": "sse",
        "SZSE": "szse",
        "BSE": "bse",
    }.get(normalized_exchange)
    if (
        official_source is None
        or normalized_source not in {"cninfo", official_source}
        or not normalized_instrument
        or not announcement_id
        or announcement_id.lower().startswith("derived-")
        or len(published_date) != 10
    ):
        return None
    return stable_id(
        f"{official_source}-cninfo-mirror-chain",
        normalized_instrument,
        announcement_id,
        published_date,
    )


def _same_source_filing_chain_id(
    *,
    source: object,
    exchange: object,
    instrument_id: object,
    source_announcement_id: object,
    published_at: object,
) -> str | None:
    """Bind URL variants of the same source-qualified legal filing."""

    normalized_source = str(source or "").strip().lower()
    normalized_exchange = str(exchange or "").strip().upper()
    normalized_instrument = str(instrument_id or "").strip().upper()
    announcement_id = str(source_announcement_id or "").strip()
    published_date = str(published_at or "").strip()[:10]
    if (
        not all(
            (
                normalized_source,
                normalized_exchange,
                normalized_instrument,
                announcement_id,
            )
        )
        or len(published_date) != 10
    ):
        return None
    return stable_id(
        "same-source-filing-chain",
        normalized_source,
        normalized_exchange,
        normalized_instrument,
        announcement_id,
        published_date,
    )


def _cninfo_same_title_revision_chain_id(
    *,
    source: object,
    exchange: object,
    instrument_id: object,
    title: object,
    fiscal_year: int | None,
    variant: AnnualReportVariant | None,
    published_at: object,
    source_announcement_id: object,
) -> str | None:
    """Order exact-title same-day CNInfo republications by provider identity."""

    normalized_source = str(source or "").strip().lower()
    normalized_exchange = str(exchange or "").strip().upper()
    normalized_instrument = str(instrument_id or "").strip().upper()
    normalized_title = "".join(str(title or "").split())
    published_date = str(published_at or "").strip()[:10]
    announcement_id = str(source_announcement_id or "").strip()
    if (
        normalized_source != "cninfo"
        or not normalized_exchange
        or not normalized_instrument
        or not normalized_title
        or fiscal_year is None
        or variant is None
        or len(published_date) != 10
        or not announcement_id.isdigit()
    ):
        return None
    return stable_id(
        "cninfo-same-title-revision-chain",
        normalized_exchange,
        normalized_instrument,
        int(fiscal_year),
        variant.value,
        normalized_title,
        published_date,
    )


def _cninfo_announcement_precedence(
    *,
    source: object,
    source_announcement_id: object,
    title: object,
    published_at: object,
) -> int | None:
    announcement_id = str(source_announcement_id or "").strip()
    if (
        str(source or "").strip().lower() != "cninfo"
        or not announcement_id.isdigit()
        or not "".join(str(title or "").split())
        or len(str(published_at or "").strip()[:10]) != 10
    ):
        return None
    return int(announcement_id)


def _apply_withdrawal_relations(
    rows: list[dict],
    candidates: tuple[AnnualReportCandidate, ...],
) -> tuple[AnnualReportCandidate, ...]:
    """Bind source-qualified withdrawal evidence to its target candidate."""

    resolved = list(candidates)
    for relation_index, relation_row in enumerate(rows):
        relation_metadata = relation_row.get("attachment_metadata") or {}
        relation_announcement_metadata = relation_row.get("announcement_metadata") or {}
        target = _withdrawal_target_id(
            relation_metadata,
            relation_announcement_metadata,
        )
        evidence_type = _withdrawal_evidence_type(
            relation_metadata,
            relation_announcement_metadata,
        )
        if not target or not evidence_type:
            continue
        # A withdrawal notice/state-bearing observation is evidence, never a
        # replacement report in its own right.
        resolved[relation_index] = replace(
            resolved[relation_index],
            withdrawn=True,
            withdrawal_target_id=target,
            withdrawal_evidence_type=evidence_type,
        )
        for candidate_index, candidate_row in enumerate(rows):
            if not _withdrawal_target_matches(
                target,
                relation_row=relation_row,
                candidate_row=candidate_row,
            ):
                continue
            resolved[candidate_index] = replace(
                resolved[candidate_index],
                withdrawn=True,
                withdrawal_target_id=target,
                withdrawal_evidence_type=evidence_type,
            )
    return tuple(resolved)


def _withdrawal_target_matches(
    target: str,
    *,
    relation_row: dict,
    candidate_row: dict,
) -> bool:
    canonical_ids = {
        str(value)
        for value in (
            candidate_row.get("version_id"),
            candidate_row.get("attachment_id"),
            candidate_row.get("announcement_id"),
        )
        if value
    }
    if target in canonical_ids:
        return True
    if relation_row.get("source") != candidate_row.get("source"):
        return False
    provider_ids = {
        str(value)
        for value in (
            candidate_row.get("source_attachment_id"),
            candidate_row.get("source_announcement_id"),
        )
        if value
    }
    return target in provider_ids


def _withdrawal_target_id(
    attachment_metadata: dict,
    announcement_metadata: dict,
) -> str | None:
    for metadata in (attachment_metadata, announcement_metadata):
        for key in (
            "withdrawal_target_id",
            "withdrawn_announcement_id",
            "withdrawn_attachment_id",
            "target_announcement_id",
            "target_attachment_id",
        ):
            value = metadata.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return None


def _withdrawal_evidence_type(
    attachment_metadata: dict,
    announcement_metadata: dict,
) -> str | None:
    for metadata in (attachment_metadata, announcement_metadata):
        for key in (
            "withdrawal_evidence_type",
            "official_relation",
            "withdrawal_relation",
            "withdrawal_rule_version",
        ):
            value = metadata.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return None
