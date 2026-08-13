"""Crash-recoverable effective replacement and predecessor deletion."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .capacity_artifact import CapacityArtifactNotReadyError, validate_capacity_artifact
from .models import DeletionStatus, IntegrityStatus
from .repository import AnnouncementAssetRepository, DeletionLeaseFenceError
from .storage import ContentAddressedBlobStore, MountIdentity


class _DeletionFinalizationBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class DeletionExecutionResult:
    deletion_id: str
    status: DeletionStatus
    deleted: bool
    reason_code: str | None = None


class AnnouncementAssetLifecycleManager:
    """Execute persisted deletion intentions only after every safety gate passes."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        blob_store: ContentAddressedBlobStore,
        primary_failure_domain: str | None,
        deletion_lease_seconds: int = 300,
        lease_safety_grace_seconds: int = 30,
        after_mark_deleting: Callable[[str], None] | None = None,
        after_unlink: Callable[[str], None] | None = None,
    ) -> None:
        if int(deletion_lease_seconds) < 0:
            raise ValueError("deletion_lease_seconds cannot be negative")
        if int(lease_safety_grace_seconds) < 0:
            raise ValueError("lease_safety_grace_seconds cannot be negative")
        self.repository = repository
        self.blob_store = blob_store
        self.primary_failure_domain = primary_failure_domain
        self.deletion_lease_seconds = int(deletion_lease_seconds)
        self.lease_safety_grace_seconds = int(lease_safety_grace_seconds)
        self.after_mark_deleting = after_mark_deleting
        self.after_unlink = after_unlink

    def execute_deletion(
        self,
        deletion_id: str,
        *,
        actor: str = "announcement_asset_lifecycle",
    ) -> DeletionExecutionResult:
        intent = self.repository.get_deletion(deletion_id)
        if intent is None:
            raise KeyError(f"deletion intent not found: {deletion_id}")
        current = DeletionStatus(intent["status"])
        if current is DeletionStatus.DELETED:
            return DeletionExecutionResult(deletion_id, current, True)
        if self.blob_store.config.dry_run:
            return DeletionExecutionResult(
                deletion_id,
                current,
                False,
                "dry_run_blocks_deletion",
            )
        if not self.repository.deletion_recovery_pair_satisfies_unlink(deletion_id):
            return DeletionExecutionResult(
                deletion_id,
                current,
                False,
                "recovery_pair_not_closed",
            )
        if not self._recovery_pair_artifacts_are_valid(intent):
            return DeletionExecutionResult(
                deletion_id,
                current,
                False,
                "recovery_pair_artifacts_invalid",
            )
        lease_check_at = datetime.now(timezone.utc).isoformat()
        if _intent_has_active_lease(intent, as_of=lease_check_at):
            return DeletionExecutionResult(
                deletion_id,
                current,
                False,
                "deletion_lease_active",
            )
        pin_as_of = (
            datetime.now(timezone.utc)
            - timedelta(seconds=self.lease_safety_grace_seconds)
        ).isoformat()
        if (
            self.repository.active_retention_pin_count(
                intent["blob_hash"], as_of=pin_as_of
            )
            > 0
        ):
            return DeletionExecutionResult(
                deletion_id,
                current,
                False,
                "retention_pin_active",
            )
        replacement_hash = intent.get("replacement_blob_hash")
        withdrawal_without_replacement = (
            intent.get("reason") == "withdrawn_without_replacement"
        )
        if withdrawal_without_replacement and (
            replacement_hash is not None or intent.get("replacement_asset_id") is not None
        ):
            return DeletionExecutionResult(
                deletion_id, current, False, "withdrawal_replacement_forbidden"
            )
        if not replacement_hash and not withdrawal_without_replacement:
            return DeletionExecutionResult(
                deletion_id, current, False, "replacement_blob_missing"
            )
        if replacement_hash:
            replacement_blob = self.repository.get_blob(replacement_hash)
            if replacement_blob is None:
                return DeletionExecutionResult(
                    deletion_id, current, False, "replacement_blob_unregistered"
                )
            replacement_validation = self.blob_store.validate_blob(
                replacement_blob.canonical_path,
                expected_hash=replacement_blob.content_hash,
                expected_length=replacement_blob.content_length,
            )
            if replacement_validation.status is not IntegrityStatus.VALID:
                return DeletionExecutionResult(
                    deletion_id, current, False, "replacement_blob_invalid"
                )
            if not self.repository.backup_satisfies_deletion_gate(
                replacement_hash,
                primary_failure_domain=self.primary_failure_domain,
            ):
                return DeletionExecutionResult(
                    deletion_id, current, False, "replacement_backup_not_ready"
                )
        old_blob = self.repository.get_blob(intent["blob_hash"])
        if old_blob is None or old_blob.canonical_path != intent["managed_path"]:
            return DeletionExecutionResult(
                deletion_id, current, False, "managed_path_evidence_mismatch"
            )
        if not self.repository.backup_satisfies_deletion_gate(
            old_blob.content_hash,
            primary_failure_domain=self.primary_failure_domain,
        ):
            return DeletionExecutionResult(
                deletion_id, current, False, "predecessor_backup_not_ready"
            )

        try:
            validate_capacity_artifact(self.blob_store.config)
        except CapacityArtifactNotReadyError:
            return DeletionExecutionResult(
                deletion_id, current, False, "capacity_artifact_not_ready"
            )

        try:
            operation_mount = self.blob_store.validate_mount()
        except RuntimeError as exc:
            if current in {DeletionStatus.DELETING, DeletionStatus.FAILED}:
                try:
                    self.repository.block_deletion_finalization(
                        deletion_id,
                        actor=actor,
                        error_code="deletion_mount_unavailable",
                        details={"error": str(exc)},
                    )
                except DeletionLeaseFenceError:
                    return _fenced_preclaim_result(
                        self.repository,
                        deletion_id,
                        fallback_status=current,
                    )
            return DeletionExecutionResult(
                deletion_id,
                DeletionStatus.DELETING
                if current in {DeletionStatus.DELETING, DeletionStatus.FAILED}
                else current,
                False,
                "deletion_mount_unavailable",
            )
        stored_mount_key = intent.get("operation_mount_filesystem_key")
        if stored_mount_key and not _intent_mount_matches(intent, operation_mount):
            if current in {DeletionStatus.DELETING, DeletionStatus.FAILED}:
                try:
                    self.repository.block_deletion_finalization(
                        deletion_id,
                        actor=actor,
                        error_code="deletion_mount_identity_changed",
                        details={
                            "expected_filesystem_key": stored_mount_key,
                            "actual_filesystem_key": operation_mount.filesystem_key,
                        },
                    )
                except DeletionLeaseFenceError:
                    return _fenced_preclaim_result(
                        self.repository,
                        deletion_id,
                        fallback_status=current,
                    )
            return DeletionExecutionResult(
                deletion_id,
                DeletionStatus.DELETING,
                False,
                "deletion_mount_identity_changed",
            )

        lease_owner = f"{actor}-{uuid.uuid4().hex}"
        lease_expires_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=self.deletion_lease_seconds)
        ).isoformat()
        if not self.repository.claim_deletion(
            deletion_id,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            actor=actor,
            mount_evidence=_mount_evidence(operation_mount),
        ):
            latest = self.repository.get_deletion(deletion_id)
            latest_status = DeletionStatus(latest["status"]) if latest else current
            return DeletionExecutionResult(
                deletion_id,
                latest_status,
                latest_status is DeletionStatus.DELETED,
                None
                if latest_status is DeletionStatus.DELETED
                else "deletion_lease_active",
            )
        claimed = self.repository.get_deletion(deletion_id)
        if claimed is None or claimed.get("lease_owner") != lease_owner:
            return DeletionExecutionResult(
                deletion_id,
                DeletionStatus.DELETING,
                False,
                "deletion_lease_fenced",
            )
        lease_generation = int(claimed.get("lease_generation") or 0)

        try:
            if self.after_mark_deleting is not None:
                self.after_mark_deleting(deletion_id)
            try:
                self._heartbeat_deletion(
                    deletion_id,
                    lease_owner=lease_owner,
                    lease_generation=lease_generation,
                )
                self.blob_store.revalidate_mount(operation_mount)
            except RuntimeError as exc:
                raise _DeletionFinalizationBlocked(
                    "deletion mount changed before unlink"
                ) from exc
            path = Path(intent["managed_path"])
            path_existed = path.exists()
            if path_existed:
                try:
                    self.blob_store.unlink_blob(
                        intent["blob_hash"], expected_mount=operation_mount
                    )
                except RuntimeError as exc:
                    raise _DeletionFinalizationBlocked(
                        "deletion mount changed at unlink boundary"
                    ) from exc
            if self.after_unlink is not None:
                self.after_unlink(deletion_id)
            try:
                self._heartbeat_deletion(
                    deletion_id,
                    lease_owner=lease_owner,
                    lease_generation=lease_generation,
                )
                self.blob_store.revalidate_mount(operation_mount)
            except RuntimeError as exc:
                raise _DeletionFinalizationBlocked(
                    "deletion mount changed before finalization"
                ) from exc
            if path.exists():
                raise _DeletionFinalizationBlocked(
                    "managed path is still present on the captured mount"
                )
            self.repository.update_blob_integrity(
                intent["blob_hash"], IntegrityStatus.MISSING
            )
            self.repository.transition_deletion(
                deletion_id,
                DeletionStatus.DELETED,
                actor=actor,
                retention_evidence={
                    "active_pin_count": 0,
                    "replacement_backup_verified": bool(replacement_hash),
                    "predecessor_backup_verified": True,
                    "recovery_pair_closed": True,
                    "required_set_hold_retained": True,
                    "primary_failure_domain": self.primary_failure_domain,
                },
                details={"path_existed_before_unlink": path_existed},
                expected_lease_owner=lease_owner,
                expected_lease_generation=lease_generation,
            )
        except DeletionLeaseFenceError:
            return DeletionExecutionResult(
                deletion_id,
                DeletionStatus.DELETING,
                False,
                "deletion_lease_fenced",
            )
        except _DeletionFinalizationBlocked as exc:
            try:
                self.repository.block_deletion_finalization(
                    deletion_id,
                    actor=actor,
                    error_code="deletion_mount_finalization_blocked",
                    details={
                        "error": str(exc),
                        "operation_mount": _mount_evidence(operation_mount),
                    },
                    expected_lease_owner=lease_owner,
                    expected_lease_generation=lease_generation,
                )
            except DeletionLeaseFenceError:
                return DeletionExecutionResult(
                    deletion_id,
                    DeletionStatus.DELETING,
                    False,
                    "deletion_lease_fenced",
                )
            return DeletionExecutionResult(
                deletion_id,
                DeletionStatus.DELETING,
                False,
                "deletion_mount_finalization_blocked",
            )
        except Exception as exc:  # noqa: BLE001 - durable failure state must capture worker errors
            latest = self.repository.get_deletion(deletion_id)
            if latest and latest["status"] in {
                DeletionStatus.PLANNED.value,
                DeletionStatus.DELETING.value,
            }:
                try:
                    self.repository.transition_deletion(
                        deletion_id,
                        DeletionStatus.FAILED,
                        actor=actor,
                        error_code=f"{type(exc).__name__}:{exc}",
                        expected_lease_owner=lease_owner,
                        expected_lease_generation=lease_generation,
                    )
                except DeletionLeaseFenceError:
                    return DeletionExecutionResult(
                        deletion_id,
                        DeletionStatus.DELETING,
                        False,
                        "deletion_lease_fenced",
                    )
            return DeletionExecutionResult(
                deletion_id,
                DeletionStatus.FAILED,
                False,
                "unlink_or_finalize_failed",
            )
        return DeletionExecutionResult(deletion_id, DeletionStatus.DELETED, True)

    def _heartbeat_deletion(
        self,
        deletion_id: str,
        *,
        lease_owner: str,
        lease_generation: int,
    ) -> None:
        lease_expires_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=self.deletion_lease_seconds)
        ).isoformat()
        if not self.repository.heartbeat_deletion(
            deletion_id,
            lease_owner=lease_owner,
            lease_generation=lease_generation,
            lease_expires_at=lease_expires_at,
        ):
            raise DeletionLeaseFenceError(
                "deletion lease owner or generation changed"
            )

    def reconcile_pending(self, *, limit: int = 100) -> list[DeletionExecutionResult]:
        return [
            self.execute_deletion(item["deletion_id"], actor="deletion_reconciler")
            for item in self.repository.list_deletions(limit=limit)
        ]

    def _recovery_pair_artifacts_are_valid(self, intent: dict[str, object]) -> bool:
        pair_id = str(intent.get("recovery_pair_id") or "")
        manifest = self.repository.get_recovery_manifest_by_pair(pair_id)
        closure = self.repository.get_recovery_pair_closure(pair_id)
        if manifest is None or closure is None:
            return False
        if not _file_matches_hash(
            Path(closure.catalog_snapshot_identity),
            closure.catalog_snapshot_hash,
        ):
            return False
        destination = self.blob_store.config.backup.destination_root
        if destination is None:
            return False
        file_manifest_path = (
            destination / "manifests" / f"{manifest.file_manifest_watermark}.json"
        )
        try:
            manifest_bytes = file_manifest_path.read_bytes()
            if hashlib.sha256(manifest_bytes).hexdigest() != manifest.file_manifest_watermark:
                return False
            file_manifest = json.loads(manifest_bytes.decode("utf-8"))
            entries = {
                str(item["content_hash"]): item
                for item in file_manifest.get("blobs", ())
            }
        except (OSError, UnicodeError, ValueError, KeyError, TypeError):
            return False
        predecessor = self.repository.get_blob(manifest.content_hash)
        predecessor_entry = entries.get(manifest.content_hash)
        if (
            predecessor is None
            or predecessor_entry is None
            or predecessor_entry.get("backup_path") != manifest.backup_object
            or not _verified_pdf(
                Path(manifest.backup_object),
                expected_hash=manifest.content_hash,
                expected_length=predecessor.content_length,
            )
        ):
            return False
        if manifest.replacement_content_hash is None:
            return manifest.manifest_kind == "withdrawal_tombstone"
        replacement = self.repository.get_blob(manifest.replacement_content_hash)
        replacement_path = manifest.evidence.get("replacement_backup_object")
        replacement_entry = entries.get(manifest.replacement_content_hash)
        return bool(
            replacement is not None
            and replacement_path
            and replacement_entry is not None
            and replacement_entry.get("backup_path") == replacement_path
            and _verified_pdf(
                Path(str(replacement_path)),
                expected_hash=manifest.replacement_content_hash,
                expected_length=replacement.content_length,
            )
        )


def _mount_evidence(identity: MountIdentity) -> dict[str, str | int]:
    return {
        "source": identity.source,
        "mount_point": str(identity.mount_point),
        "fs_type": identity.fs_type,
        "device_id": identity.device_id,
        "filesystem_key": identity.filesystem_key,
    }


def _file_matches_hash(path: Path, expected_hash: str) -> bool:
    if not path.is_file():
        return False
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return False
    return digest.hexdigest() == expected_hash


def _verified_pdf(
    path: Path,
    *,
    expected_hash: str,
    expected_length: int,
) -> bool:
    try:
        if not path.is_file() or path.stat().st_size != int(expected_length):
            return False
        with path.open("rb") as handle:
            if handle.read(5) != b"%PDF-":
                return False
    except OSError:
        return False
    return _file_matches_hash(path, expected_hash)


def _intent_mount_matches(
    intent: dict[str, object], identity: MountIdentity
) -> bool:
    return {
        "source": intent.get("operation_mount_source"),
        "mount_point": intent.get("operation_mount_point"),
        "fs_type": intent.get("operation_mount_fs_type"),
        "device_id": intent.get("operation_mount_device_id"),
        "filesystem_key": intent.get("operation_mount_filesystem_key"),
    } == _mount_evidence(identity)


def _intent_has_active_lease(intent: dict[str, object], *, as_of: str) -> bool:
    owner = str(intent.get("lease_owner") or "").strip()
    expires_at = str(intent.get("lease_expires_at") or "").strip()
    if not owner or not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > datetime.fromisoformat(as_of)
    except ValueError:
        return True


def _fenced_preclaim_result(
    repository: AnnouncementAssetRepository,
    deletion_id: str,
    *,
    fallback_status: DeletionStatus,
) -> DeletionExecutionResult:
    latest = repository.get_deletion(deletion_id)
    latest_status = (
        DeletionStatus(latest["status"]) if latest is not None else fallback_status
    )
    return DeletionExecutionResult(
        deletion_id,
        latest_status,
        latest_status is DeletionStatus.DELETED,
        None if latest_status is DeletionStatus.DELETED else "deletion_lease_active",
    )
