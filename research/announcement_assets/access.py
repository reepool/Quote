"""Business-neutral local access facade for announcement assets."""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

from .config import AnnouncementAssetConfig
from .models import (
    AssetAvailability,
    DocumentFamily,
    EnsureRequest,
    IntegrityStatus,
    normalize_source,
    stable_id,
    utc_now_iso,
)
from .repository import AnnouncementAssetRepository
from .service import AnnouncementAssetService
from .storage import MountIdentity, probe_mount_identity


class AssetContentGoneError(FileNotFoundError):
    """A stable asset id is known but is no longer publicly streamable."""

    def __init__(self, asset_id: str, lifecycle_state: str) -> None:
        self.asset_id = str(asset_id)
        self.lifecycle_state = str(lifecycle_state)
        super().__init__(
            f"annual-report asset content is gone:{self.lifecycle_state}:{self.asset_id}"
        )


class AssetContentIntegrityError(RuntimeError):
    """A current or retained blob failed controlled-stream integrity checks."""

    def __init__(self, integrity_status: str) -> None:
        self.integrity_status = str(integrity_status)
        super().__init__(f"annual-report blob integrity failed:{self.integrity_status}")


class AssetContentMountError(RuntimeError):
    """The catalog path no longer resolves to the approved filings mount."""


class ControlledContentHandle:
    """Verified file stream whose close releases a durable read lease."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        lease: dict[str, Any],
        ttl_seconds: int,
        audit_access: bool = False,
    ) -> None:
        self._repository = repository
        self._lease_id = str(lease["pin_id"])
        self._lease_owner = str(lease["owner"])
        metadata = dict(lease.get("metadata") or {})
        self._lease_generation = int(metadata.get("lease_generation") or 1)
        self._ttl_seconds = max(1, int(ttl_seconds))
        self._audit_access = bool(audit_access)
        self._heartbeat_interval = max(1.0, self._ttl_seconds / 3.0)
        self._next_heartbeat = time.monotonic() + self._heartbeat_interval
        self._lease_lock = threading.RLock()
        self._released = False
        self._closed = False
        self._file: BinaryIO | None = None

    @property
    def closed(self) -> bool:
        return self._closed

    def attach_verified_file(self, file_handle: BinaryIO) -> None:
        with self._lease_lock:
            if self._closed or self._released or self._file is not None:
                file_handle.close()
                raise RuntimeError("annual-report controlled stream is not attachable")
            self._file = file_handle

    @property
    def lease_id(self) -> str:
        return self._lease_id

    @property
    def lease_generation(self) -> int:
        return self._lease_generation

    def heartbeat(self) -> bool:
        with self._lease_lock:
            if self.closed or self._released:
                return False
            refreshed = self._repository.heartbeat_read_lease(
                self._lease_id,
                owner=self._lease_owner,
                expected_generation=self._lease_generation,
                ttl_seconds=self._ttl_seconds,
            )
            if refreshed is None:
                return False
            metadata = dict(refreshed.get("metadata") or {})
            self._lease_generation = int(metadata["lease_generation"])
            self._next_heartbeat = time.monotonic() + self._heartbeat_interval
            return True

    def heartbeat_if_due(self) -> bool:
        return time.monotonic() < self._next_heartbeat or self.heartbeat()

    def read(self, size: int = -1) -> bytes:
        if not self.heartbeat_if_due():
            raise RuntimeError("annual-report read lease heartbeat was lost")
        if self._file is None:
            raise RuntimeError("annual-report controlled stream is not ready")
        return self._file.read(size)

    def close(self) -> None:
        with self._lease_lock:
            try:
                if self._file is not None:
                    self._file.close()
                    self._file = None
                if not self._released:
                    self._repository.release_read_lease(
                        self._lease_id,
                        owner=self._lease_owner,
                        expected_generation=self._lease_generation,
                        retain_audit=self._audit_access,
                    )
                    self._released = True
            finally:
                self._closed = True


class AnnouncementAssetAccess:
    """Expose stable local-first operations without consumer-owned storage."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        service: AnnouncementAssetService | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.service = service or AnnouncementAssetService(
            repository=repository,
            config=config,
        )

    def list_assets(
        self,
        *,
        instrument_id: str | None = None,
        fiscal_year: int | None = None,
        document_family: str = "annual_report",
        source: str | None = None,
        source_announcement_id: str | None = None,
        integrity: str | None = None,
        acquisition_status: str | None = None,
        effective_state: str | None = None,
        asset_availability: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        bounded_limit = max(1, min(int(limit), 1000))
        bounded_offset = max(0, int(offset))
        records = self.repository.list_annual_report_asset_records(
            instrument_id=instrument_id,
            fiscal_year=fiscal_year,
            document_family=document_family,
            source=source,
            source_announcement_id=source_announcement_id,
            integrity=integrity,
            acquisition_status=acquisition_status,
            effective_state=effective_state,
            asset_availability=asset_availability,
            limit=bounded_limit,
            offset=bounded_offset,
        )
        return {
            "items": [self._asset_record_projection(item) for item in records],
            "returned": len(records),
            "limit": bounded_limit,
            "offset": bounded_offset,
        }

    @staticmethod
    def _asset_record_projection(record: dict[str, Any]) -> dict[str, Any]:
        source = str(record["source"])
        filing_id = str(record["source_announcement_id"])
        attachment_id = str(record["attachment_id"])
        observation_version = record.get("observation_version")
        availability = str(record["asset_availability"])
        exact_content_state = (
            "local_valid"
            if record.get("asset_id") and availability == "local_valid"
            else (
                "retained_internal_only"
                if record.get("content_hash") and record.get("integrity") == "valid"
                else "local_content_unavailable"
            )
        )
        asset_id = record.get("asset_id")
        content_url = (
            f"/api/v1/research/annual-report-assets/{asset_id}/content"
            if asset_id and exact_content_state == "local_valid"
            else None
        )
        return {
            "asset_record_id": stable_id(
                "asset-record",
                source,
                filing_id,
                attachment_id,
                str(observation_version or "metadata"),
            ),
            "asset_id": asset_id,
            "instrument_id": record["instrument_id"],
            "fiscal_year": int(record["fiscal_year"]),
            "report_period": record["report_period"],
            "source": source,
            "source_announcement_id": filing_id,
            "filing_id": filing_id,
            "attachment_id": attachment_id,
            "observation_version": observation_version,
            "version_available_at": record.get("version_available_at"),
            "published_at": record.get("published_at"),
            "document_family": record.get("document_family") or "annual_report",
            "variant": record.get("variant") or "original",
            "is_full_report": True,
            "is_correction": record.get("variant") == "correction",
            "classification_vocabulary_version": (
                record.get("classification_vocabulary_version")
                or "official_document_classification.v1"
            ),
            "content_hash": record.get("content_hash"),
            "content_length": record.get("content_length"),
            "content_url": content_url,
            "integrity": record.get("integrity"),
            "availability": availability,
            "asset_availability": availability,
            "acquisition_status": record.get("acquisition_status"),
            "effective_state": record.get("effective_state"),
            "effective_decision_state": (
                record.get("effective_state")
                if record.get("effective_state")
                in {"current", "provisional", "ambiguous", "blocked", "withdrawn"}
                else None
            ),
            "exact_content_state": exact_content_state,
            "predecessor_asset_id": record.get("predecessor_asset_id"),
            "pending_candidate_id": record.get("pending_candidate_id"),
            "activated_at": record.get("activated_at"),
            "last_checked_at": record.get("last_checked_at"),
            "decision_reasons": list(record.get("decision_reasons") or []),
            "canonical_source_filing": {
                "source": source,
                "source_announcement_id": filing_id,
                "attachment_id": attachment_id,
            },
            "equivalent_source_filings": list(
                record.get("equivalent_source_filings") or []
            ),
            "canonical_projection_policy_version": record.get(
                "canonical_projection_policy_version"
            ),
            "evidence_set_hash": record.get("evidence_set_hash"),
        }

    def get_effective_asset(
        self,
        instrument_id: str,
        *,
        fiscal_year: int | None = None,
        document_family: str = "annual_report",
        knowledge_cutoff: str | None = None,
    ) -> dict[str, Any] | None:
        if knowledge_cutoff is not None:
            report = self.service.resolve_effective_report(
                instrument_id,
                fiscal_year=fiscal_year,
                document_family=document_family,
                knowledge_cutoff=knowledge_cutoff,
            )
        elif document_family == DocumentFamily.ANNUAL_REPORT.value:
            report = self.repository.get_effective_report(
                instrument_id,
                fiscal_year,
                document_family=document_family,
            )
        else:
            report = self.service.resolve_effective_report(
                instrument_id,
                fiscal_year=fiscal_year,
                document_family=document_family,
                knowledge_cutoff=utc_now_iso(),
            )
        return None if report is None else self._asset_projection(report)

    def get_asset(self, asset_id: str) -> dict[str, Any] | None:
        """Return one production effective asset by its immutable asset id."""

        report = self.repository.get_effective_report_by_asset_id(str(asset_id))
        return None if report is None else self._asset_projection(report)

    def list_effective_assets(
        self,
        *,
        instrument_id: str | None = None,
        document_family: str = "annual_report",
        knowledge_cutoff: str | None = None,
        source: str | None = None,
        availability: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List authoritative effective annual reports without historical candidates."""

        normalized_availability = (
            None if availability is None else AssetAvailability(str(availability))
        )
        bounded_limit = max(1, min(int(limit), 1000))
        bounded_offset = max(0, int(offset))
        if (
            knowledge_cutoff is None
            and document_family == DocumentFamily.ANNUAL_REPORT.value
        ):
            reports = self.repository.list_effective_reports(
                instrument_id=instrument_id,
                document_family=document_family,
                source=source,
                availability=normalized_availability,
                limit=bounded_limit,
                offset=bounded_offset,
            )
        else:
            reports = self.service.resolve_effective_reports(
                knowledge_cutoff=(knowledge_cutoff or utc_now_iso()),
                document_family=document_family,
                instrument_id=instrument_id,
                source=source,
                availability=normalized_availability,
                limit=bounded_limit,
                offset=bounded_offset,
            )
        return {
            "items": [self._asset_projection(report) for report in reports],
            "returned": len(reports),
            "limit": bounded_limit,
            "offset": bounded_offset,
        }

    def ensure(self, request: EnsureRequest) -> dict[str, Any]:
        result = self.service.ensure_annual_report(request)
        return {
            "disposition": result.disposition.value,
            "asset_availability": result.availability.value,
            "availability": result.availability.value,
            "asset": (
                None if result.asset is None else self._asset_projection(result.asset)
            ),
            "asset_request_id": (
                None
                if result.asset_request is None
                else result.asset_request.asset_request_id
            ),
            "request": (
                None
                if result.asset_request is None
                else self._asset_request_projection(
                    result.asset_request,
                    result.operation,
                )
            ),
            "reason_code": result.reason_code,
        }

    def get_asset_request(
        self,
        asset_request_id: str,
        *,
        principal: str,
    ) -> dict[str, Any] | None:
        subscription = self.repository.get_asset_request(
            asset_request_id,
            principal=principal,
        )
        if subscription is None:
            return None
        operation = self.repository.get_operation(subscription.operation_id)
        return self._asset_request_projection(subscription, operation)

    def cancel_asset_request(
        self,
        asset_request_id: str,
        *,
        principal: str,
    ) -> dict[str, Any]:
        subscription = self.repository.cancel_asset_request(
            asset_request_id,
            principal=principal,
        )
        operation = self.repository.get_operation(subscription.operation_id)
        return self._asset_request_projection(subscription, operation)

    def readiness(self, *, operator: bool = False) -> dict[str, Any]:
        total = 0
        local_valid = 0
        offset = 0
        while True:
            page = self.repository.list_effective_reports(limit=1000, offset=offset)
            total += len(page)
            local_valid += sum(
                report.availability is AssetAvailability.LOCAL_VALID for report in page
            )
            if len(page) < 1000:
                break
            offset += len(page)
        recent_daily = self.repository.list_operations(
            operation_type="annual_report_asset_daily_update",
            limit=1,
        )
        latest = recent_daily[0] if recent_daily else None
        ready_for_daily = bool(
            self.config.enabled
            and self.config.scheduled_enabled
            and self.config.jobs.daily_enabled
            and not self.config.dry_run
        )
        blockers = [] if ready_for_daily else ["daily_job_disabled"]
        summary = {
            "effective_asset_count": total,
            "local_valid_asset_count": local_valid,
            "latest_daily_status": (None if latest is None else latest.status.value),
            "latest_daily_outcome": (
                None
                if latest is None or latest.outcome is None
                else latest.outcome.value
            ),
        }
        return {
            "status": "ready" if ready_for_daily else "disabled",
            "ready_for_reads": True,
            "ready_for_daily": ready_for_daily,
            "ready_for_deletion": False,
            "blockers": blockers,
            "warnings": [],
            "summary": summary,
            "operator_diagnostics": summary if operator else None,
        }

    def content_handle(
        self, asset_id: str, *, audit_access: bool = False
    ) -> dict[str, Any]:
        """Open a verified current asset under a deletion-blocking read lease."""

        report = self.repository.get_effective_report_by_asset_id(asset_id)
        if report is None:
            lifecycle_state = self.repository.get_asset_content_lifecycle_state(
                asset_id
            )
            if lifecycle_state in {"superseded", "withdrawn", "deleted"}:
                raise AssetContentGoneError(asset_id, lifecycle_state)
            raise KeyError("annual-report asset was not found")
        if report.availability is AssetAvailability.CORRUPT:
            raise AssetContentIntegrityError("catalog_corrupt")
        if (
            report.asset_id != asset_id
            or report.availability is not AssetAvailability.LOCAL_VALID
        ):
            raise FileNotFoundError(
                "annual-report asset content is not locally available"
            )
        if not report.content_hash:
            raise FileNotFoundError("annual-report asset has no content hash")
        blob = self.repository.get_blob(report.content_hash)
        if blob is None:
            raise FileNotFoundError("annual-report blob metadata is missing")

        def revalidate_current() -> None:
            current = self.repository.get_effective_report_by_asset_id(asset_id)
            if current is None or current.content_hash != report.content_hash:
                state = self.repository.get_asset_content_lifecycle_state(asset_id)
                raise AssetContentGoneError(asset_id, state or "superseded")
            if current.availability is AssetAvailability.CORRUPT:
                raise AssetContentIntegrityError("catalog_corrupt")
            if current.availability is not AssetAvailability.LOCAL_VALID:
                raise FileNotFoundError(
                    "annual-report asset content is not locally available"
                )

        snapshot, path, lease = self._controlled_blob_snapshot(
            blob,
            owner_prefix=f"public-asset:{asset_id}",
            lease_metadata={"asset_id": asset_id, "scope": "public_current"},
            revalidate_reference=revalidate_current,
            audit_access=audit_access,
        )
        return {
            "asset_id": report.asset_id,
            "content_hash": blob.content_hash,
            "content_length": blob.content_length,
            "media_type": "application/pdf",
            "filename": f"{report.instrument_id}-{report.fiscal_year}-annual-report.pdf",
            "path": path,
            "file_handle": snapshot,
            "read_lease_id": lease["pin_id"],
        }

    def exact_observation_handle(
        self,
        request: EnsureRequest,
        *,
        authorized: bool = False,
        audit_access: bool = False,
    ) -> dict[str, Any]:
        """Read one retained exact observation for an authorized internal caller.

        This path is intentionally incapable of discovery or acquisition.  All
        three immutable pins are required so a caller cannot accidentally read
        a different observation of the same filing.
        """
        if not authorized:
            raise PermissionError("exact observation handle is internal-only")
        if not request.source or not request.source_announcement_id:
            raise ValueError("exact observation handle requires source-filing scope")
        if (
            not request.attachment_id
            or not request.observation_version
            or not request.expected_content_hash
        ):
            raise ValueError(
                "exact observation handle requires attachment, version, and hash pins"
            )
        expected_source = normalize_source(request.source)
        expected_filing_id = str(request.source_announcement_id).strip()
        expected_attachment_id = str(request.attachment_id).strip()
        expected_content_hash = str(request.expected_content_hash).strip().lower()
        version = self.repository.get_attachment_version(
            str(request.observation_version).strip()
        )
        if version is None:
            raise FileNotFoundError("exact observation is not retained")
        if version.visibility_state != "production":
            raise FileNotFoundError("exact observation is not production-visible")
        attachment = self.repository.get_attachment(version.attachment_id)
        if attachment is None or expected_attachment_id not in {
            attachment.attachment_id,
            str(attachment.source_attachment_id or ""),
        }:
            raise FileNotFoundError("exact observation attachment pin failed")
        announcement = self.repository.get_announcement(attachment.announcement_id)
        if (
            announcement is None
            or announcement.source != expected_source
            or announcement.source_announcement_id != expected_filing_id
        ):
            raise FileNotFoundError("exact observation filing pin failed")
        if request.knowledge_cutoff is not None:
            available_at = version.version_available_at or announcement.published_at
            if available_at:
                cutoff = datetime.fromisoformat(
                    str(request.knowledge_cutoff).replace("Z", "+00:00")
                )
                available = datetime.fromisoformat(
                    str(available_at).replace("Z", "+00:00")
                )
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=timezone.utc)
                if available.tzinfo is None:
                    available = available.replace(tzinfo=timezone.utc)
                if available.astimezone(timezone.utc) > cutoff.astimezone(timezone.utc):
                    raise FileNotFoundError(
                        "exact observation was unavailable at knowledge cutoff"
                    )
        if (
            version.integrity_status is not IntegrityStatus.VALID
            or str(version.content_hash or "").lower() != expected_content_hash
        ):
            raise FileNotFoundError("exact observation integrity pin failed")
        blob = self.repository.get_blob(str(version.content_hash))
        if blob is None or blob.integrity_status is not IntegrityStatus.VALID:
            raise FileNotFoundError("exact observation blob is missing")

        def revalidate_exact() -> None:
            current_version = self.repository.get_attachment_version(version.version_id)
            current_attachment = self.repository.get_attachment(version.attachment_id)
            current_announcement = (
                None
                if current_attachment is None
                else self.repository.get_announcement(
                    current_attachment.announcement_id
                )
            )
            if (
                current_version is None
                or current_version.visibility_state != "production"
                or current_version.attachment_id != attachment.attachment_id
                or current_version.integrity_status is not IntegrityStatus.VALID
                or str(current_version.content_hash or "").lower()
                != expected_content_hash
                or current_attachment is None
                or expected_attachment_id
                not in {
                    current_attachment.attachment_id,
                    str(current_attachment.source_attachment_id or ""),
                }
                or current_announcement is None
                or current_announcement.source != expected_source
                or current_announcement.source_announcement_id != expected_filing_id
            ):
                raise FileNotFoundError(
                    "exact observation is no longer retained and valid"
                )

        snapshot, path, lease = self._controlled_blob_snapshot(
            blob,
            owner_prefix=f"internal-observation:{version.version_id}",
            lease_metadata={
                "attachment_id": attachment.attachment_id,
                "observation_version": version.version_id,
                "scope": "internal_exact_observation",
            },
            revalidate_reference=revalidate_exact,
            missing_as_unavailable=True,
            audit_access=audit_access,
        )
        return {
            "source": request.source,
            "source_announcement_id": request.source_announcement_id,
            "attachment_id": attachment.attachment_id,
            "observation_version": version.version_id,
            "content_hash": blob.content_hash,
            "content_length": blob.content_length,
            "media_type": "application/pdf",
            "filename": f"{attachment.attachment_id}.pdf",
            "path": path,
            "file_handle": snapshot,
            "read_lease_id": lease["pin_id"],
        }

    def _controlled_blob_snapshot(
        self,
        blob: Any,
        *,
        owner_prefix: str,
        lease_metadata: dict[str, Any],
        revalidate_reference: Callable[[], None],
        missing_as_unavailable: bool = False,
        audit_access: bool = False,
    ) -> tuple[ControlledContentHandle, Path, dict[str, Any]]:
        ttl_seconds = max(1, int(self.config.retry.lease_seconds))
        lease_owner = f"{owner_prefix}:{uuid.uuid4().hex}"
        lease = self.repository.acquire_read_lease(
            blob_hash=blob.content_hash,
            owner=lease_owner,
            ttl_seconds=ttl_seconds,
            metadata={**lease_metadata, "audit_access": bool(audit_access)},
        )
        snapshot = ControlledContentHandle(
            repository=self.repository,
            lease=lease,
            ttl_seconds=ttl_seconds,
            audit_access=audit_access,
        )
        source: BinaryIO | None = None
        try:
            initial_mount = self._validated_filings_mount()
            try:
                path = self.service.blob_store.resolve_readable_asset_path(
                    blob.canonical_path
                )
            except ValueError as exc:
                raise AssetContentMountError(
                    "annual-report content path is outside governed roots"
                ) from exc
            self._validate_content_mount(initial_mount, path)
            revalidate_reference()
            digest = hashlib.sha256()
            actual_length = 0
            validation_status = "valid"
            try:
                source = path.open("rb")
                if source.read(5) != b"%PDF-":
                    raise ValueError("not_pdf")
                source.seek(0)
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    if not snapshot.heartbeat_if_due():
                        raise RuntimeError(
                            "annual-report read lease heartbeat was lost"
                        )
                    digest.update(chunk)
                    actual_length += len(chunk)
            except FileNotFoundError:
                validation_status = "missing"
            except OSError:
                validation_status = "unreadable"
            except ValueError as exc:
                validation_status = (
                    str(exc)
                    if str(exc) in {"not_pdf", "size_mismatch", "hash_mismatch"}
                    else "unreadable"
                )
            if validation_status == "valid" and actual_length != int(
                blob.content_length
            ):
                validation_status = "size_mismatch"
            if (
                validation_status == "valid"
                and digest.hexdigest() != str(blob.content_hash).lower()
            ):
                validation_status = "hash_mismatch"
            if validation_status != "valid":
                self.repository.mark_content_hash_invalid(
                    blob.content_hash,
                    integrity_status=IntegrityStatus(validation_status),
                    reason=f"external_content_mutation:{validation_status}",
                )
                if validation_status == "missing" and missing_as_unavailable:
                    raise FileNotFoundError("exact observation bytes are unavailable")
                raise AssetContentIntegrityError(validation_status)
            final_mount = self._validated_filings_mount()
            if not self._same_mount(initial_mount, final_mount):
                raise AssetContentMountError(
                    "filings mount identity changed while opening content"
                )
            self._validate_content_mount(initial_mount, path)
            revalidate_reference()
            if source is None:
                raise AssetContentIntegrityError("unreadable")
            source.seek(0)
            snapshot.attach_verified_file(source)
            source = None
            return snapshot, path, lease
        except Exception:
            snapshot.close()
            raise
        finally:
            if source is not None:
                source.close()

    @staticmethod
    def _same_mount(left: MountIdentity, right: MountIdentity) -> bool:
        return (
            left.filesystem_key == right.filesystem_key
            and left.fs_type == right.fs_type
        )

    def _validate_content_mount(
        self, expected: MountIdentity, path: Path
    ) -> MountIdentity:
        actual = probe_mount_identity(path)
        if not self._same_mount(expected, actual):
            raise AssetContentMountError(
                "annual-report content is not on the approved filings mount"
            )
        return actual

    def _validated_filings_mount(self) -> MountIdentity:
        try:
            return self.service.blob_store.validate_mount()
        except RuntimeError as exc:
            raise AssetContentMountError(
                "approved filings mount is unavailable"
            ) from exc

    def _asset_projection(self, report: Any) -> dict[str, Any]:
        blob = (
            self.repository.get_blob(report.content_hash)
            if report.content_hash
            else None
        )
        version = self.repository.get_attachment_version(report.version_id)
        equivalent_source_filings = [
            asdict(item) for item in report.equivalent_source_filings
        ]
        content_url = (
            f"/api/v1/research/annual-report-assets/{report.asset_id}/content"
            if report.availability is AssetAvailability.LOCAL_VALID
            and report.decision_evidence.get("projection_kind")
            != "knowledge_cutoff_read"
            else None
        )
        return {
            "asset_id": report.asset_id,
            "instrument_id": report.instrument_id,
            "fiscal_year": report.fiscal_year,
            "report_period": report.report_period,
            "source": report.source,
            "source_announcement_id": report.source_announcement_id,
            "filing_id": report.source_announcement_id,
            "attachment_id": report.attachment_id,
            "observation_version": report.version_id,
            "version_available_at": (
                None if version is None else version.version_available_at
            ),
            "published_at": report.published_at,
            "document_family": report.document_family,
            "variant": report.variant.value,
            "is_full_report": report.is_full_report,
            "is_correction": report.variant.value == "correction",
            "classification_vocabulary_version": (
                report.classification_vocabulary_version
            ),
            "content_hash": report.content_hash,
            "content_length": None if blob is None else blob.content_length,
            "content_url": content_url,
            "integrity": None if blob is None else blob.integrity_status.value,
            "asset_availability": report.availability.value,
            "availability": report.availability.value,
            "acquisition_status": (
                "success"
                if blob is not None and blob.integrity_status is IntegrityStatus.VALID
                else "metadata_only"
            ),
            "effective_state": report.decision_state.value,
            "effective_decision_state": report.decision_state.value,
            "exact_content_state": (
                "local_valid"
                if report.availability is AssetAvailability.LOCAL_VALID
                else "local_content_unavailable"
            ),
            "predecessor_asset_id": report.predecessor_asset_id,
            "pending_candidate_id": report.pending_candidate_id,
            "activated_at": report.activated_at,
            "last_checked_at": report.last_checked_at,
            "decision_reasons": list(report.decision_reasons),
            "canonical_source_filing": {
                "source": report.source,
                "source_announcement_id": report.source_announcement_id,
                "attachment_id": report.attachment_id,
            },
            "equivalent_source_filings": equivalent_source_filings,
            "canonical_projection_policy_version": (
                report.canonical_projection_policy_version
            ),
            "evidence_set_hash": report.evidence_set_hash,
        }

    @staticmethod
    def _asset_request_projection(subscription: Any, operation: Any) -> dict[str, Any]:
        authorization = dict(subscription.authorized_projection or {})
        allowed_progress = {
            str(item) for item in authorization.get("allowed_progress_fields", ())
        }
        allowed_diagnostics = {
            str(item) for item in authorization.get("allowed_diagnostics_fields", ())
        }
        progress = (
            {}
            if operation is None
            else {
                key: value
                for key, value in operation.progress.items()
                if key in allowed_progress
            }
        )
        diagnostics = (
            {}
            if operation is None
            else {
                key: value
                for key, value in operation.diagnostics.items()
                if key in allowed_diagnostics
            }
        )
        return {
            "asset_request_id": subscription.asset_request_id,
            "asset_request_status": subscription.status.value,
            "status": subscription.status.value,
            "consumer": subscription.consumer,
            "created_at": subscription.created_at,
            "updated_at": subscription.updated_at,
            "cancelled_at": subscription.cancelled_at,
            "operation_status": None if operation is None else operation.status.value,
            "operation_stage": (
                None
                if operation is None or operation.stage is None
                else operation.stage.value
            ),
            "batch_outcome": (
                None
                if operation is None or operation.outcome is None
                else operation.outcome.value
            ),
            "attempt": None if operation is None else operation.attempt,
            "next_retry_at": None if operation is None else operation.next_retry_at,
            "progress": progress,
            "result_asset_id": None if operation is None else operation.result_asset_id,
            "result_origin": (
                None
                if operation is None or operation.result_origin is None
                else operation.result_origin.value
            ),
            "reason_code": None if operation is None else operation.reason_code,
            "diagnostics": diagnostics,
            "expires_at": subscription.expires_at,
            "expired_at": subscription.expired_at,
            "tombstone_until": subscription.tombstone_until,
            "retention_policy_version": subscription.retention_policy_version,
        }
