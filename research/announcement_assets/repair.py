"""Production-bound handlers for explicitly authorized integrity repairs."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

from .config import AnnouncementAssetConfig
from .integrity import IntegrityFinding, RepairHandler
from .lifecycle import AnnouncementAssetLifecycleManager
from .models import DeletionStatus, IntegrityStatus
from .repository import AnnouncementAssetRepository
from .service import AnnouncementAssetService
from .storage import MountIdentity, probe_mount_identity


class ProductionIntegrityRepairHandlers:
    """Bind repair verbs to exact catalog identities and managed paths."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        service: AnnouncementAssetService,
        operation_id: str,
        actor: str,
        request_fingerprint: str,
    ) -> None:
        self.repository = repository
        self.config = config
        self.service = service
        self.blob_store = service.blob_store
        self.operation_id = str(operation_id)
        self.actor = str(actor)
        self.request_fingerprint = str(request_fingerprint)
        if not self.operation_id or not self.actor or not self.request_fingerprint:
            raise ValueError(
                "repair handlers require operation, actor, and request fingerprint"
            )

    def as_mapping(self) -> dict[str, RepairHandler]:
        return {
            "network_repair": self._network_repair,
            "quarantine": self._quarantine,
            "link": self._link,
            "move": self._move,
            "delete": self._delete,
        }

    def _network_repair(
        self, action: str, content_hash: str, finding: IntegrityFinding | None
    ) -> None:
        def mutate() -> None:
            attachment_ids = self.repository.list_attachment_ids_for_content_hash(
                content_hash, limit=1
            )
            if not attachment_ids:
                raise RuntimeError("network repair target has no attachment lineage")
            self.service.acquire_attachment(
                attachment_ids[0],
                force_refresh=True,
                lease_owner=f"integrity:{self.operation_id}",
                operation_id=self.operation_id,
            )
            blob = self.repository.get_blob(content_hash)
            if blob is None:
                raise RuntimeError("network repair target blob disappeared")
            validation = self.blob_store.validate_blob(
                blob.canonical_path,
                expected_hash=content_hash,
                expected_length=blob.content_length,
            )
            if validation.status is not IntegrityStatus.VALID:
                raise RuntimeError(
                    "network repair did not restore the requested content hash"
                )

        self._run_audited(action, content_hash, mutate)

    def _quarantine(
        self, action: str, content_hash: str, finding: IntegrityFinding | None
    ) -> None:
        def mutate() -> None:
            blob, source = self._blob_source_path(content_hash)
            target = self.config.quarantine_root / (
                f"{content_hash}.operator_integrity_quarantine.pdf"
            )
            target = target.resolve(strict=False)
            self._require_beneath(target, self.config.quarantine_root)
            if source == target:
                self._require_valid_file(target, content_hash, blob.content_length)
                self.repository.mark_content_hash_invalid(
                    content_hash,
                    integrity_status=IntegrityStatus.QUARANTINED,
                    reason="operator_integrity_quarantine",
                )
                return
            if not source.is_file() and target.is_file():
                self._require_valid_file(target, content_hash, blob.content_length)
                self.repository.update_blob_path(content_hash, str(target))
                self.repository.mark_content_hash_invalid(
                    content_hash,
                    integrity_status=IntegrityStatus.QUARANTINED,
                    reason="operator_integrity_quarantine",
                )
                return
            self._require_controlled_source(source)
            mount = self.blob_store.validate_mount()
            if target.exists():
                raise RuntimeError("quarantine target already exists")
            self._revalidate(mount)
            published = self.blob_store.quarantine_readable_blob(
                source,
                content_hash=content_hash,
                expected_length=blob.content_length,
                reason="operator_integrity_quarantine",
                artifact_metadata={
                    "owner": self.actor,
                    "operation_id": self.operation_id,
                    "request_fingerprint": self.request_fingerprint,
                },
            )
            if published.resolve(strict=False) != target:
                raise RuntimeError("quarantine target identity changed")
            self.repository.update_blob_path(content_hash, str(target))
            self.repository.mark_content_hash_invalid(
                content_hash,
                integrity_status=IntegrityStatus.QUARANTINED,
                reason="operator_integrity_quarantine",
            )

        self._run_audited(action, content_hash, mutate)

    def _link(
        self, action: str, content_hash: str, finding: IntegrityFinding | None
    ) -> None:
        self._run_audited(
            action,
            content_hash,
            lambda: self._canonicalize(content_hash, move=False),
        )

    def _move(
        self, action: str, content_hash: str, finding: IntegrityFinding | None
    ) -> None:
        self._run_audited(
            action,
            content_hash,
            lambda: self._canonicalize(content_hash, move=True),
        )

    def _delete(
        self, action: str, deletion_id: str, finding: IntegrityFinding | None
    ) -> None:
        def mutate() -> None:
            mount = self.blob_store.validate_mount()
            lifecycle = AnnouncementAssetLifecycleManager(
                repository=self.repository,
                blob_store=self.blob_store,
                primary_failure_domain=mount.filesystem_key,
                deletion_lease_seconds=self.config.retry.lease_seconds,
                lease_safety_grace_seconds=(
                    self.config.retry.lease_safety_grace_seconds
                ),
            )
            result = lifecycle.execute_deletion(deletion_id, actor=self.actor)
            if result.status is not DeletionStatus.DELETED:
                raise RuntimeError(
                    "governed deletion did not complete: "
                    + str(result.reason or result.status.value)
                )

        self._run_audited(action, deletion_id, mutate)

    def _canonicalize(self, content_hash: str, *, move: bool) -> None:
        blob, source = self._blob_source_path(content_hash)
        target = self.blob_store.blob_path(content_hash).resolve(strict=False)
        if source == target:
            self._require_valid_file(target, content_hash, blob.content_length)
            return
        if not source.is_file() and target.is_file():
            self._require_valid_file(target, content_hash, blob.content_length)
            self.repository.update_blob_path(content_hash, str(target))
            return
        self._require_controlled_source(source)
        if not any(
            source == root.resolve(strict=False)
            or root.resolve(strict=False) in source.parents
            for root in self.config.adoption_roots
        ):
            raise RuntimeError("link/move source is not a registered adoption path")
        if move and any(
            self.repository.get_active_retention_pin(
                blob_hash=content_hash,
                pin_type=pin_type,
                pin_key=str(source),
            )
            is not None
            for pin_type in ("legacy_alias", "managed_alias")
        ):
            raise RuntimeError("move source has an active legacy alias pin")
        validation = self.blob_store.validate_blob(
            source,
            expected_hash=content_hash,
            expected_length=blob.content_length,
        )
        if validation.status is not IntegrityStatus.VALID:
            raise RuntimeError("link/move source is invalid")
        mount = self.blob_store.validate_mount()
        if probe_mount_identity(source).filesystem_key != mount.filesystem_key:
            raise RuntimeError("link/move source is outside the approved filesystem")
        self._revalidate(mount)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._revalidate(mount)
        if target.exists():
            target_validation = self.blob_store.validate_blob(
                target,
                expected_hash=content_hash,
                expected_length=blob.content_length,
            )
            if target_validation.status is not IntegrityStatus.VALID:
                raise RuntimeError("canonical repair target already exists but is invalid")
        elif move:
            os.replace(source, target)
            self._fsync_directory(target.parent)
        else:
            os.link(source, target)
            self._fsync_directory(target.parent)
        self._revalidate(mount)
        final = self.blob_store.validate_blob(
            target,
            expected_hash=content_hash,
            expected_length=blob.content_length,
        )
        if final.status is not IntegrityStatus.VALID:
            raise RuntimeError("canonical repair publication verification failed")
        self.repository.update_blob_path(content_hash, str(target))

    def _blob_source_path(self, content_hash: str):
        blob = self.repository.get_blob(content_hash)
        if blob is None:
            raise KeyError(f"blob not found: {content_hash}")
        source = Path(blob.canonical_path).resolve(strict=False)
        return blob, source

    def _require_controlled_source(self, source: Path) -> None:
        if source.is_symlink():
            raise RuntimeError("repair source must not be a symlink")
        controlled = (self.config.archive_root, *self.config.adoption_roots)
        if not any(
            source == root.resolve(strict=False)
            or root.resolve(strict=False) in source.parents
            for root in controlled
        ):
            raise RuntimeError("repair source is outside controlled roots")
        if not source.is_file():
            raise FileNotFoundError(source)

    def _require_valid_file(
        self, path: Path, content_hash: str, content_length: int
    ) -> None:
        validation = self.blob_store.validate_blob(
            path,
            expected_hash=content_hash,
            expected_length=content_length,
        )
        if validation.status is not IntegrityStatus.VALID:
            raise RuntimeError("repair recovery target is invalid")

    def _run_audited(
        self, action: str, target: str, mutation: Callable[[], None]
    ) -> None:
        self._audit(action, target, "planned")
        try:
            mutation()
        except Exception as exc:
            self._audit(
                action,
                target,
                "failed",
                error=f"{type(exc).__name__}:{exc}",
            )
            raise
        self._audit(action, target, "completed")

    def _audit(
        self, action: str, target: str, status: str, *, error: str | None = None
    ) -> None:
        self.repository.append_job_command_audit(
            operation_id=self.operation_id,
            command=f"integrity_{action}_{status}",
            principal=self.actor,
            effective_permission=self.config.operator_permission,
            trigger_kind="manual",
            config_version=self.config.config_fingerprint,
            request_fingerprint=self.request_fingerprint,
            payload={"action": action, "target": target, "status": status, "error": error},
        )

    def _revalidate(self, expected: MountIdentity) -> None:
        current = self.blob_store.validate_mount()
        if current.filesystem_key != expected.filesystem_key:
            raise RuntimeError("filings mount identity changed at repair boundary")

    @staticmethod
    def _require_beneath(path: Path, root: Path) -> None:
        resolved_root = root.resolve(strict=False)
        if path != resolved_root and resolved_root not in path.parents:
            raise RuntimeError("repair target escapes its controlled root")

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
