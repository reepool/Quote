"""Governed content-addressed file storage for announcement attachments."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AnnouncementAssetConfig
from .models import IntegrityStatus
from .path_segments import normalize_reason_segment, validate_path_segment

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MountIdentity:
    requested_path: Path
    mount_point: Path
    source: str
    fs_type: str
    device_id: int
    filesystem_id: str | None = None
    read_write: bool = True

    @property
    def filesystem_key(self) -> str:
        return f"{self.source}|{self.mount_point}|{self.device_id}"


@dataclass(frozen=True)
class BlobValidation:
    status: IntegrityStatus
    content_hash: str | None
    content_length: int | None
    path: Path


@dataclass(frozen=True)
class BlobPublishResult:
    content_hash: str
    content_length: int
    path: Path
    created: bool


@dataclass(frozen=True)
class StorageCapacitySnapshot:
    filesystem_key: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    projected_free_bytes: int
    projected_utilization: float
    warning: bool


@dataclass(frozen=True)
class StorageCapacityAdmission:
    """A service-validated, target-bound capacity watermark admission."""

    authorization_id: str
    operation_id: str
    target_attachment_id: str
    filesystem_key: str
    max_bytes: int


@dataclass(frozen=True)
class StorageArtifactMetrics:
    part_count: int
    part_bytes: int
    oldest_part_age_seconds: float
    part_invalid_sidecar_count: int
    part_invalid_sidecar_bytes: int
    quarantine_count: int
    quarantine_bytes: int
    oldest_quarantine_age_seconds: float
    quarantine_invalid_sidecar_count: int
    quarantine_invalid_sidecar_bytes: int


def probe_mount_identity(path: str | Path) -> MountIdentity:
    """Resolve the longest Linux mountinfo entry that contains ``path``."""
    requested = Path(path).resolve(strict=False)
    probe = requested
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    stat = probe.stat()
    entries: list[tuple[Path, str, str, bool, int]] = []
    try:
        with Path("/proc/self/mountinfo").open("r", encoding="utf-8") as handle:
            for line in handle:
                left, separator, right = line.rstrip("\n").partition(" - ")
                if not separator:
                    continue
                left_parts = left.split()
                right_parts = right.split()
                if len(left_parts) < 6 or len(right_parts) < 2:
                    continue
                try:
                    mount_id = int(left_parts[0])
                except ValueError:
                    continue
                mount_point = Path(_unescape_mountinfo(left_parts[4])).resolve(
                    strict=False
                )
                try:
                    requested.relative_to(mount_point)
                except ValueError:
                    continue
                options = set(left_parts[5].split(";" if ";" in left_parts[5] else ","))
                entries.append(
                    (
                        mount_point,
                        right_parts[1],
                        right_parts[0],
                        "rw" in options,
                        mount_id,
                    )
                )
    except OSError:
        entries = []
    if entries:
        mount_point, source, fs_type, read_write, _mount_id = max(
            entries,
            key=lambda item: (
                len(item[0].parts),
                item[2].lower() != "autofs",
                item[4],
            ),
        )
    else:
        mount_point, source, fs_type, read_write = (
            Path("/"),
            "unknown",
            "unknown",
            os.access(probe, os.W_OK),
        )
    return MountIdentity(
        requested_path=requested,
        mount_point=mount_point,
        source=source,
        fs_type=fs_type,
        device_id=int(stat.st_dev),
        filesystem_id=f"{stat.st_dev}:{stat.st_ino}",
        read_write=read_write,
    )


def validate_backup_mount(config: AnnouncementAssetConfig) -> MountIdentity | None:
    """Fail closed when an enabled backup resolves to an unsafe local fallback."""
    backup = config.backup
    if not backup.enabled:
        return None
    if backup.mount_root is None or backup.destination_root is None:
        raise RuntimeError("enabled backup has no configured mount or destination")
    mount_root = backup.mount_root.resolve(strict=False)
    destination = backup.destination_root.resolve(strict=False)
    try:
        destination.relative_to(mount_root)
    except ValueError as exc:
        raise RuntimeError("backup destination escapes configured mount root") from exc
    identity = probe_mount_identity(mount_root)
    if identity.mount_point == Path("/"):
        raise RuntimeError("backup root is not a dedicated mounted filesystem")
    if backup.expected_mount_source and identity.source != backup.expected_mount_source:
        raise RuntimeError(
            "backup mount source mismatch: "
            f"expected={backup.expected_mount_source} actual={identity.source}"
        )
    primary = probe_mount_identity(config.filings_root)
    if identity.filesystem_key == primary.filesystem_key or (
        _mount_host(identity.source)
        and _mount_host(identity.source) == _mount_host(primary.source)
    ):
        raise RuntimeError("backup does not use an independent storage failure domain")
    if mount_root.exists() and not os.access(mount_root, os.R_OK | os.W_OK):
        raise RuntimeError("backup mount is not readable and writable")
    resolved_destination = destination.resolve(strict=False)
    try:
        resolved_destination.relative_to(mount_root.resolve(strict=False))
    except ValueError as exc:
        raise RuntimeError("backup destination resolves outside mounted root") from exc
    return identity


class ContentAddressedBlobStore:
    """Publish and validate immutable PDF blobs beneath the configured root."""

    def __init__(self, config: AnnouncementAssetConfig) -> None:
        self.config = config

    def prepare(self) -> MountIdentity:
        """Explicitly validate the filings mount and create managed directories."""
        identity = self.validate_mount()
        for path in (
            self.config.archive_root,
            self.config.blob_root,
            self.config.temp_root,
            self.config.quarantine_root,
        ):
            path.mkdir(parents=True, exist_ok=True)
        return identity

    def validate_mount(self) -> MountIdentity:
        identity = probe_mount_identity(self.config.filings_root)
        expected = self.config.expected_filings_mount_source
        if expected and identity.source != expected:
            raise RuntimeError(
                f"filings mount source mismatch: expected={expected} actual={identity.source}"
            )
        if self.config.require_filings_mount and identity.mount_point == Path("/"):
            raise RuntimeError("filings root is not a dedicated mounted filesystem")
        if self.config.filings_root.exists() and not os.access(
            self.config.filings_root, os.R_OK | os.W_OK
        ):
            raise RuntimeError("filings mount is not readable and writable")
        return identity

    def blob_path(self, content_hash: str) -> Path:
        digest = _validated_hash(content_hash)
        path = self.config.blob_root / digest[:2] / f"{digest}.pdf"
        return self._require_managed(path)

    def preflight_capacity(self, planned_bytes: int) -> StorageCapacitySnapshot:
        """Validate the mount and return a hard-gated filesystem snapshot."""
        snapshot = self.inspect_capacity(planned_bytes)
        if snapshot.projected_free_bytes < self.config.storage.free_space_reserve_bytes:
            raise RuntimeError("filings hard free-space reserve would be violated")
        if snapshot.projected_utilization >= self.config.storage.hard_stop_utilization:
            raise RuntimeError("filings hard utilization threshold would be violated")
        return snapshot

    def inspect_capacity(self, planned_bytes: int) -> StorageCapacitySnapshot:
        """Return a mount-qualified capacity snapshot without relaxing any caller gate."""
        planned = int(planned_bytes)
        if planned <= 0:
            raise ValueError("planned_bytes must be positive")
        identity = self.validate_mount()
        usage = shutil.disk_usage(self.config.filings_root)
        projected_free = usage.free - planned
        projected_utilization = (usage.used + planned) / max(usage.total, 1)
        return StorageCapacitySnapshot(
            filesystem_key=identity.filesystem_key,
            total_bytes=usage.total,
            used_bytes=usage.used,
            free_bytes=usage.free,
            projected_free_bytes=projected_free,
            projected_utilization=projected_utilization,
            warning=(
                projected_utilization >= self.config.storage.warning_utilization
            ),
        )

    def publish_bytes(
        self,
        content: bytes,
        *,
        expected_hash: str | None = None,
        artifact_metadata: Mapping[str, Any] | None = None,
        capacity_admission: StorageCapacityAdmission | None = None,
    ) -> BlobPublishResult:
        """Validate, fsync, atomically publish, reopen, and revalidate bytes."""
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if len(content) > self.config.storage.max_attachment_bytes:
            raise ValueError("attachment exceeds configured annual-report limit")
        if not content.startswith(b"%PDF-"):
            raise ValueError("attachment does not have a valid PDF signature")
        digest = hashlib.sha256(content).hexdigest()
        if expected_hash and digest != _validated_hash(expected_hash):
            raise ValueError("attachment hash does not match expected hash")
        target = self.blob_path(digest)
        initial_mount = self.validate_mount()
        if capacity_admission is None:
            self._check_capacity(len(content))
        else:
            snapshot = self.inspect_capacity(len(content))
            if snapshot.filesystem_key != capacity_admission.filesystem_key:
                raise RuntimeError("capacity admission filesystem identity changed")
            if len(content) > int(capacity_admission.max_bytes):
                raise RuntimeError("capacity admission byte bound exceeded")
            if snapshot.projected_free_bytes < 0:
                raise RuntimeError("capacity admission cannot exceed physical free space")
        if target.exists():
            validation = self.validate_blob(
                target,
                expected_hash=digest,
                expected_length=len(content),
            )
            if validation.status is not IntegrityStatus.VALID:
                raise RuntimeError(
                    f"canonical blob exists but is invalid: {validation.status.value}"
                )
            return BlobPublishResult(digest, len(content), target, False)

        self._check_artifact_write_gate()

        target.parent.mkdir(parents=True, exist_ok=True)
        publish_generation = uuid.uuid4().hex
        temporary = target.parent / f".{digest}.{publish_generation}.part"
        sidecar = Path(f"{temporary}.json")
        supplied_metadata = dict(artifact_metadata or {})
        supplied_metadata.setdefault("owner", "direct-blob-publisher")
        supplied_metadata.setdefault("lease_generation", publish_generation)
        try:
            self._write_artifact_metadata(
                sidecar,
                {
                    "artifact_type": "part",
                    "managed_path": str(temporary),
                    "content_hash": digest,
                    "planned_bytes": len(content),
                    "actual_bytes": 0,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    **supplied_metadata,
                },
            )
            with temporary.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            metadata = self._read_artifact_metadata(sidecar) or {}
            metadata["actual_bytes"] = temporary.stat().st_size
            metadata["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write_artifact_metadata(sidecar, metadata)
            temporary_validation = self.validate_blob(
                temporary,
                expected_hash=digest,
                expected_length=len(content),
            )
            if temporary_validation.status is not IntegrityStatus.VALID:
                raise RuntimeError(
                    f"temporary blob validation failed: {temporary_validation.status.value}"
                )
            self._revalidate_mutation_mount(initial_mount)
            os.replace(temporary, target)
            _fsync_directory(target.parent)
            published = self.validate_blob(
                target,
                expected_hash=digest,
                expected_length=len(content),
            )
            if published.status is not IntegrityStatus.VALID:
                raise RuntimeError(
                    f"published blob validation failed: {published.status.value}"
                )
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                sidecar.unlink(missing_ok=True)
            except OSError:
                pass
        return BlobPublishResult(digest, len(content), target, True)

    def validate_blob(
        self,
        path: str | Path,
        *,
        expected_hash: str | None = None,
        expected_length: int | None = None,
    ) -> BlobValidation:
        candidate = self._require_readable_asset(Path(path))
        if not candidate.is_file():
            return BlobValidation(IntegrityStatus.MISSING, None, None, candidate)
        try:
            stat = candidate.stat()
            if expected_length is not None and stat.st_size != int(expected_length):
                return BlobValidation(
                    IntegrityStatus.SIZE_MISMATCH, None, stat.st_size, candidate
                )
            digest = hashlib.sha256()
            with candidate.open("rb") as handle:
                if handle.read(5) != b"%PDF-":
                    return BlobValidation(
                        IntegrityStatus.NOT_PDF, None, stat.st_size, candidate
                    )
                handle.seek(0)
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            actual_hash = digest.hexdigest()
            if expected_hash and actual_hash != _validated_hash(expected_hash):
                return BlobValidation(
                    IntegrityStatus.HASH_MISMATCH,
                    actual_hash,
                    stat.st_size,
                    candidate,
                )
            return BlobValidation(
                IntegrityStatus.VALID, actual_hash, stat.st_size, candidate
            )
        except OSError:
            return BlobValidation(IntegrityStatus.UNREADABLE, None, None, candidate)

    def write_candidate_part(
        self,
        *,
        artifact_identity: str,
        content: bytes,
        metadata: Mapping[str, Any],
    ) -> tuple[Path, MountIdentity]:
        """Write a lease-owned candidate file without publishing a blob."""

        if not content:
            raise ValueError("candidate verification content must not be empty")
        mount = self.validate_mount()
        self.config.temp_root.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(str(artifact_identity).encode("utf-8")).hexdigest()
        target = self._require_managed(
            self.config.temp_root / f"{digest}.candidate.part"
        )
        sidecar = Path(f"{target}.json")
        if target.exists() or sidecar.exists():
            raise FileExistsError(target)
        self._revalidate_mutation_mount(mount)
        try:
            with target.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            self._write_artifact_metadata(
                sidecar,
                {
                    "artifact_type": "part",
                    "managed_path": str(target),
                    "actual_bytes": len(content),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    **dict(metadata),
                },
            )
            _fsync_directory(target.parent)
        except Exception:
            target.unlink(missing_ok=True)
            sidecar.unlink(missing_ok=True)
            raise
        return target, mount

    def remove_candidate_part(
        self,
        path: str | Path,
        *,
        expected_mount: MountIdentity,
    ) -> None:
        """Remove a verified candidate part and its recovery sidecar."""

        target = self._require_managed(Path(path))
        try:
            target.relative_to(self.config.temp_root.resolve(strict=False))
        except ValueError as exc:
            raise ValueError("candidate part is outside the managed temporary root") from exc
        if not target.name.endswith(".candidate.part"):
            raise ValueError("candidate part has an unexpected filename")
        sidecar = Path(f"{target}.json")
        self._revalidate_mutation_mount(expected_mount)
        target.unlink(missing_ok=True)
        self._revalidate_mutation_mount(expected_mount)
        sidecar.unlink(missing_ok=True)
        _fsync_directory(target.parent)

    def quarantine_candidate_part(
        self,
        path: str | Path,
        *,
        expected_mount: MountIdentity,
        reason: str,
        metadata: Mapping[str, Any],
    ) -> Path:
        """Move an uncleared candidate part into governed quarantine."""

        source = self._require_managed(Path(path))
        safe_reason = normalize_reason_segment(
            reason, field_name="candidate_verification.quarantine_reason"
        )
        digest = hashlib.sha256(str(source).encode("utf-8")).hexdigest()
        target = self._require_managed(
            self.config.quarantine_root
            / f"{digest}.{safe_reason}.candidate.pdf"
        )
        self._revalidate_mutation_mount(expected_mount)
        target.parent.mkdir(parents=True, exist_ok=True)
        source_sidecar = Path(f"{source}.json")
        target_sidecar = Path(f"{target}.json")
        self._revalidate_mutation_mount(expected_mount)
        os.replace(source, target)
        source_sidecar.unlink(missing_ok=True)
        self._write_artifact_metadata(
            target_sidecar,
            {
                "artifact_type": "quarantine",
                "managed_path": str(target),
                "actual_bytes": target.stat().st_size,
                "reason": safe_reason,
                "created_at": datetime.now(timezone.utc).isoformat(),
                **dict(metadata),
            },
        )
        _fsync_directory(target.parent)
        return target

    def quarantine_blob(
        self,
        content_hash: str,
        *,
        reason: str,
        artifact_metadata: Mapping[str, Any] | None = None,
    ) -> Path:
        initial_mount = self.validate_mount()
        source = self.blob_path(content_hash)
        if not source.exists():
            raise FileNotFoundError(source)
        safe_reason = normalize_reason_segment(
            reason,
            field_name="quarantine.reason",
        )
        target = self._require_managed(
            self.config.quarantine_root
            / f"{_validated_hash(content_hash)}.{safe_reason}.pdf"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        self._revalidate_mutation_mount(initial_mount)
        os.replace(source, target)
        _fsync_directory(target.parent)
        supplied_metadata = dict(artifact_metadata or {})
        supplied_metadata.setdefault("owner", "announcement-asset-integrity")
        supplied_metadata.setdefault("generation", uuid.uuid4().hex)
        self._write_artifact_metadata(
            Path(f"{target}.json"),
            {
                "artifact_type": "quarantine",
                "managed_path": str(target),
                "content_hash": _validated_hash(content_hash),
                "reason": safe_reason,
                "actual_bytes": target.stat().st_size,
                "created_at": datetime.now(timezone.utc).isoformat(),
                **supplied_metadata,
            },
        )
        return target

    def quarantine_readable_blob(
        self,
        path: str | Path,
        *,
        content_hash: str,
        expected_length: int,
        reason: str,
        artifact_metadata: Mapping[str, Any] | None = None,
    ) -> Path:
        """Quarantine one verified catalog path, including a governed legacy alias."""
        source = self._require_readable_asset(Path(path))
        if source.is_symlink():
            raise RuntimeError("quarantine source must not be a symlink")
        validation = self.validate_blob(
            source,
            expected_hash=content_hash,
            expected_length=expected_length,
        )
        if validation.status is not IntegrityStatus.VALID:
            raise RuntimeError("quarantine source validation failed")
        mount = self.validate_mount()
        target = self._require_managed(
            self.config.quarantine_root
            / f"{_validated_hash(content_hash)}.{normalize_reason_segment(reason, field_name='quarantine.reason')}.pdf"
        )
        if target.exists():
            raise RuntimeError("quarantine target already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        self._revalidate_mutation_mount(mount)
        os.replace(source, target)
        _fsync_directory(target.parent)
        metadata = dict(artifact_metadata or {})
        self._write_artifact_metadata(
            Path(f"{target}.json"),
            {
                "artifact_type": "quarantine",
                "managed_path": str(target),
                "content_hash": _validated_hash(content_hash),
                "reason": normalize_reason_segment(
                    reason, field_name="quarantine.reason"
                ),
                "actual_bytes": target.stat().st_size,
                "created_at": datetime.now(timezone.utc).isoformat(),
                **metadata,
            },
        )
        return target

    def unlink_blob(
        self,
        content_hash: str,
        *,
        expected_mount: MountIdentity | None = None,
    ) -> bool:
        """Remove one canonical blob; caller owns retention and audit decisions."""
        initial_mount = expected_mount or self.validate_mount()
        self._revalidate_mutation_mount(initial_mount)
        target = self.blob_path(content_hash)
        if not target.exists():
            return False
        self._revalidate_mutation_mount(initial_mount)
        target.unlink()
        _fsync_directory(target.parent)
        return True

    def cleanup_expired_parts(
        self,
        *,
        now: datetime | None = None,
        lease_is_active: Callable[[Mapping[str, Any]], bool] | None = None,
        lease_cleanup_claim: Callable[[Mapping[str, Any]], bool] | None = None,
        after_unlink: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> int:
        """Reclaim only evidenced stale parts whose lease generation is inactive.

        Both callbacks are deliberately required. A released byte reservation
        or missing sidecar cannot prove owner death. ``lease_is_active`` screens
        recent heartbeats; ``lease_cleanup_claim`` atomically fences the exact
        owner/generation immediately before unlink.
        """
        timestamp = now or datetime.now(timezone.utc)
        if lease_is_active is None or lease_cleanup_claim is None:
            return 0
        # Treat reconciliation itself as a filesystem operation.  The identity
        # captured here is only a starting fence; every unlink below performs
        # another check because a NAS can be remounted during the scan.
        initial_mount = self.validate_mount()
        count = 0
        for root in (self.config.blob_root, self.config.temp_root):
            if not root.exists():
                continue
            for path in root.rglob("*.part"):
                managed = self._require_managed(path)
                sidecar = Path(f"{managed}.json")
                metadata = self._read_artifact_metadata(sidecar)
                if not self._artifact_metadata_valid(
                    metadata, managed, artifact_type="part", require_lease=True
                ):
                    continue
                created_at = _parse_datetime(str(metadata["created_at"]))
                age = (timestamp - created_at).total_seconds()
                if age < (
                    self.config.storage.stale_part_max_age_seconds
                    + self.config.storage.part_safety_grace_seconds
                ):
                    continue
                try:
                    if lease_is_active(metadata):
                        continue
                except Exception:
                    # Lease evidence is a safety boundary: callback errors
                    # must never turn into destructive cleanup.
                    LOGGER.exception("stale part lease reconciliation failed")
                    continue
                # Fence a heartbeat/generation update racing with the check.
                latest = self._read_artifact_metadata(sidecar)
                if latest != metadata:
                    continue
                try:
                    if lease_is_active(latest):
                        continue
                except Exception:
                    LOGGER.exception("stale part lease fence recheck failed")
                    continue
                try:
                    if not lease_cleanup_claim(latest):
                        continue
                except Exception:
                    LOGGER.exception("stale part cleanup claim failed")
                    continue
                if self._read_artifact_metadata(sidecar) != latest:
                    continue
                try:
                    self._revalidate_mutation_mount(initial_mount)
                    managed.unlink(missing_ok=True)
                    _fsync_directory(managed.parent)
                    if after_unlink is not None:
                        after_unlink(latest)
                    self._revalidate_mutation_mount(initial_mount)
                    sidecar.unlink(missing_ok=True)
                except OSError:
                    continue
                except RuntimeError:
                    # A remount race must leave evidence in place for a later
                    # lease-safe reconciliation pass.
                    continue
                count += 1
            count += self._cleanup_orphan_part_sidecars(
                root=root,
                timestamp=timestamp,
                lease_is_active=lease_is_active,
                lease_cleanup_claim=lease_cleanup_claim,
                expected_mount=initial_mount,
            )
        return count

    def artifact_metrics(
        self,
        *,
        now: datetime | None = None,
    ) -> StorageArtifactMetrics:
        timestamp = now or datetime.now(timezone.utc)
        parts = [
            path
            for root in (self.config.blob_root, self.config.temp_root)
            if root.exists()
            for path in root.rglob("*.part")
            if path.is_file()
        ]
        quarantined = (
            [path for path in self.config.quarantine_root.rglob("*.pdf") if path.is_file()]
            if self.config.quarantine_root.exists()
            else []
        )
        part_invalid = [
            path
            for path in parts
            if not self._artifact_metadata_valid(
                self._read_artifact_metadata(Path(f"{path}.json")),
                path,
                artifact_type="part",
                require_lease=True,
            )
        ]
        quarantine_invalid = [
            path
            for path in quarantined
            if not self._artifact_metadata_valid(
                self._read_artifact_metadata(Path(f"{path}.json")),
                path,
                artifact_type="quarantine",
                require_lease=False,
            )
        ]
        return StorageArtifactMetrics(
            part_count=len(parts),
            part_bytes=_sum_sizes(parts),
            oldest_part_age_seconds=self._oldest_artifact_age(parts, timestamp),
            part_invalid_sidecar_count=len(part_invalid),
            part_invalid_sidecar_bytes=_sum_sizes(part_invalid),
            quarantine_count=len(quarantined),
            quarantine_bytes=_sum_sizes(quarantined),
            oldest_quarantine_age_seconds=self._oldest_artifact_age(
                quarantined, timestamp
            ),
            quarantine_invalid_sidecar_count=len(quarantine_invalid),
            quarantine_invalid_sidecar_bytes=_sum_sizes(quarantine_invalid),
        )

    def artifact_evidence(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Return bounded operator evidence; public readiness uses totals only."""

        timestamp = now or datetime.now(timezone.utc)
        parts = [
            path
            for root in (self.config.blob_root, self.config.temp_root)
            if root.exists()
            for path in root.rglob("*.part")
            if path.is_file()
        ]
        quarantined = (
            [path for path in self.config.quarantine_root.rglob("*.pdf") if path.is_file()]
            if self.config.quarantine_root.exists()
            else []
        )
        return {
            "parts": tuple(
                self._artifact_evidence_entry(path, "part", timestamp)
                for path in sorted(parts)
            ),
            "quarantine": tuple(
                self._artifact_evidence_entry(path, "quarantine", timestamp)
                for path in sorted(quarantined)
            ),
        }

    def cleanup_quarantine(
        self,
        *,
        authorized: bool,
        actor: str,
        audit: Callable[[Mapping[str, Any]], None],
        max_items: int = 100,
        older_than_seconds: int | None = None,
        now: datetime | None = None,
        after_unlink: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> int:
        """Delete bounded quarantine evidence only through an audited operator path."""
        if not authorized:
            raise PermissionError("quarantine cleanup requires operator authorization")
        if not str(actor or "").strip():
            raise ValueError("quarantine cleanup actor is required")
        if not callable(audit):
            raise TypeError("quarantine cleanup requires an audit callback")
        if self.config.storage.quarantine_cleanup_policy != "operator_audited_only":
            raise RuntimeError("unsupported quarantine cleanup policy")
        if int(max_items) <= 0:
            raise ValueError("max_items must be positive")
        timestamp = now or datetime.now(timezone.utc)
        initial_mount = self.validate_mount()
        minimum_age = int(
            older_than_seconds
            if older_than_seconds is not None
            else self.config.storage.quarantine_max_age_seconds
        )
        deleted = self._recover_quarantine_cleanup_sidecars(
            actor=actor,
            audit=audit,
            max_items=int(max_items),
            timestamp=timestamp,
            expected_mount=initial_mount,
        )
        if not self.config.quarantine_root.exists():
            return deleted
        for path in sorted(self.config.quarantine_root.rglob("*.pdf")):
            if deleted >= int(max_items):
                break
            managed = self._require_managed(path)
            metadata = self._read_artifact_metadata(Path(f"{managed}.json"))
            if not self._artifact_metadata_valid(
                metadata, managed, artifact_type="quarantine", require_lease=False
            ):
                continue
            created_at = _parse_datetime(str(metadata["created_at"]))
            if (timestamp - created_at).total_seconds() < minimum_age:
                continue
            sidecar = Path(f"{managed}.json")
            try:
                size = managed.stat().st_size
            except OSError:
                continue
            evidence = {
                **metadata,
                "actual_bytes": size,
                "actor": actor,
                "cleanup_id": str(metadata.get("cleanup_id") or uuid.uuid4().hex),
                "cleanup_requested_at": timestamp.isoformat(),
            }
            self._write_artifact_metadata(
                sidecar,
                {**evidence, "cleanup_state": "planned"},
            )
            audit({**evidence, "status": "planned"})
            try:
                self._revalidate_mutation_mount(initial_mount)
                managed.unlink()
                _fsync_directory(managed.parent)
            except (OSError, RuntimeError) as exc:
                audit({**evidence, "status": "failed", "error": str(exc)})
                continue
            deleted_evidence = {
                **evidence,
                "cleanup_state": "unlinked",
                "deleted_at": timestamp.isoformat(),
            }
            self._write_artifact_metadata(sidecar, deleted_evidence)
            if after_unlink is not None:
                after_unlink(deleted_evidence)
            audit({**deleted_evidence, "status": "deleted"})
            try:
                self._revalidate_mutation_mount(initial_mount)
                sidecar.unlink(missing_ok=True)
            except RuntimeError:
                # The payload is already unlinked, while the sidecar is kept as
                # crash-recovery evidence until the next safe cleanup pass.
                continue
            deleted += 1
        return deleted

    def _check_artifact_write_gate(self) -> None:
        """Stop new physical writes when temporary evidence is unsafe/full."""
        metrics = self.artifact_metrics()
        storage = self.config.storage
        if metrics.part_invalid_sidecar_count or metrics.quarantine_invalid_sidecar_count:
            raise RuntimeError("announcement artifact sidecar evidence is invalid")
        if metrics.part_bytes >= storage.stale_part_max_bytes:
            raise RuntimeError("temporary .part bytes exceed configured hard limit")
        if metrics.oldest_part_age_seconds >= storage.stale_part_max_age_seconds:
            raise RuntimeError("temporary .part age exceeds configured hard limit")
        if metrics.quarantine_bytes >= storage.quarantine_max_bytes:
            raise RuntimeError("quarantine bytes exceed configured hard limit")
        if metrics.oldest_quarantine_age_seconds >= storage.quarantine_max_age_seconds:
            raise RuntimeError("quarantine age exceeds configured hard limit")

    @staticmethod
    def _artifact_metadata_valid(
        metadata: Mapping[str, Any] | None,
        path: Path,
        *,
        artifact_type: str,
        require_lease: bool,
    ) -> bool:
        if not metadata or metadata.get("artifact_type") != artifact_type:
            return False
        try:
            if Path(str(metadata.get("managed_path"))).resolve(strict=False) != path.resolve(
                strict=False
            ):
                return False
            _parse_datetime(str(metadata["created_at"]))
            actual = int(metadata["actual_bytes"])
            stat_size = path.stat().st_size
            if actual < 0 or actual > stat_size:
                return False
            if artifact_type == "quarantine":
                return bool(metadata.get("content_hash")) and bool(metadata.get("reason"))
            return not require_lease or bool(
                str(metadata.get("owner") or metadata.get("lease_owner") or "").strip()
                and str(
                    metadata.get("lease_generation") or metadata.get("generation") or ""
                ).strip()
            )
        except (KeyError, TypeError, ValueError, OSError):
            return False

    @staticmethod
    def _write_artifact_metadata(path: Path, payload: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(dict(payload), handle, ensure_ascii=True, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)

    @staticmethod
    def _read_artifact_metadata(path: Path) -> dict[str, Any] | None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return None
        return dict(payload) if isinstance(payload, dict) else None

    def _check_capacity(self, planned_bytes: int) -> None:
        self.preflight_capacity(planned_bytes)

    def _cleanup_orphan_part_sidecars(
        self,
        *,
        root: Path,
        timestamp: datetime,
        lease_is_active: Callable[[Mapping[str, Any]], bool],
        lease_cleanup_claim: Callable[[Mapping[str, Any]], bool],
        expected_mount: MountIdentity,
    ) -> int:
        cleaned = 0
        for sidecar in root.rglob("*.part.json"):
            managed = Path(str(sidecar)[: -len(".json")])
            if managed.exists():
                continue
            metadata = self._read_artifact_metadata(sidecar)
            if not metadata or metadata.get("artifact_type") != "part":
                continue
            try:
                created_at = _parse_datetime(metadata["created_at"])
            except (KeyError, TypeError, ValueError):
                continue
            if (timestamp - created_at).total_seconds() < (
                self.config.storage.stale_part_max_age_seconds
                + self.config.storage.part_safety_grace_seconds
            ):
                continue
            try:
                if lease_is_active(metadata):
                    continue
            except Exception:
                LOGGER.exception("orphan part sidecar lease reconciliation failed")
                continue
            latest = self._read_artifact_metadata(sidecar)
            if latest != metadata:
                continue
            try:
                if lease_is_active(latest) or not lease_cleanup_claim(latest):
                    continue
            except Exception:
                LOGGER.exception("orphan part sidecar cleanup claim failed")
                continue
            if self._read_artifact_metadata(sidecar) != latest:
                continue
            try:
                self._revalidate_mutation_mount(expected_mount)
                sidecar.unlink(missing_ok=True)
            except RuntimeError:
                continue
            cleaned += 1
        return cleaned

    def _recover_quarantine_cleanup_sidecars(
        self,
        *,
        actor: str,
        audit: Callable[[Mapping[str, Any]], None],
        max_items: int,
        timestamp: datetime,
        expected_mount: MountIdentity,
    ) -> int:
        if not self.config.quarantine_root.exists():
            return 0
        recovered = 0
        for sidecar in sorted(self.config.quarantine_root.rglob("*.pdf.json")):
            if recovered >= max_items:
                break
            metadata = self._read_artifact_metadata(sidecar)
            if not metadata or metadata.get("cleanup_state") not in {
                "planned",
                "unlinked",
            }:
                continue
            managed = Path(str(sidecar)[: -len(".json")])
            if managed.exists():
                continue
            evidence = {
                **metadata,
                "actor": actor,
                "deleted_at": metadata.get("deleted_at") or timestamp.isoformat(),
                "recovered_after_crash": True,
                "status": "deleted",
            }
            audit(evidence)
            try:
                self._revalidate_mutation_mount(expected_mount)
                sidecar.unlink(missing_ok=True)
            except RuntimeError:
                continue
            recovered += 1
        return recovered

    def _oldest_artifact_age(self, paths: list[Path], now: datetime) -> float:
        ages = []
        for path in paths:
            metadata = self._read_artifact_metadata(Path(f"{path}.json"))
            try:
                created_at = _parse_datetime(metadata["created_at"] if metadata else None)
                ages.append(max(0.0, (now - created_at).total_seconds()))
            except (TypeError, ValueError, KeyError):
                try:
                    ages.append(max(0.0, now.timestamp() - path.stat().st_mtime))
                except OSError:
                    continue
        return max(ages, default=0.0)

    def _artifact_evidence_entry(
        self, path: Path, artifact_type: str, now: datetime
    ) -> dict[str, Any]:
        sidecar = self._read_artifact_metadata(Path(f"{path}.json"))
        valid = self._artifact_metadata_valid(
            sidecar,
            path,
            artifact_type=artifact_type,
            require_lease=artifact_type == "part",
        )
        try:
            actual_bytes = int(path.stat().st_size)
        except OSError:
            actual_bytes = 0
        try:
            created_at = _parse_datetime(sidecar["created_at"] if sidecar else None)
            age = max(0.0, (now - created_at).total_seconds())
        except (TypeError, ValueError, KeyError):
            age = 0.0
        return {
            "managed_path": str(path),
            "actual_bytes": actual_bytes,
            "age_seconds": age,
            "sidecar_valid": valid,
            "owner": None
            if sidecar is None
            else sidecar.get("owner") or sidecar.get("lease_owner"),
            "lease_generation": None
            if sidecar is None
            else sidecar.get("lease_generation") or sidecar.get("generation"),
            "operation_id": None if sidecar is None else sidecar.get("operation_id"),
            "attachment_id": None if sidecar is None else sidecar.get("attachment_id"),
            "reason": None if sidecar is None else sidecar.get("reason"),
        }

    def _require_managed(self, path: Path) -> Path:
        candidate = path.resolve(strict=False)
        allowed = self.config.archive_root.resolve(strict=False)
        try:
            candidate.relative_to(allowed)
        except ValueError as exc:
            raise ValueError("path escapes the managed announcement archive") from exc
        return candidate

    def _revalidate_mutation_mount(self, expected: MountIdentity) -> MountIdentity:
        """Fence filesystem mutations against a mount swap after preflight."""
        current = self.validate_mount()
        if (
            current.filesystem_key != expected.filesystem_key
            or current.fs_type != expected.fs_type
        ):
            raise RuntimeError("filings mount identity changed before file mutation")
        return current

    def revalidate_mount(self, expected: MountIdentity) -> MountIdentity:
        """Public finalization fence for lifecycle operations spanning DB state."""

        return self._revalidate_mutation_mount(expected)

    def is_managed_path(self, path: str | Path) -> bool:
        try:
            self._require_managed(Path(path))
        except ValueError:
            return False
        return True

    def resolve_readable_asset_path(self, path: str | Path) -> Path:
        """Resolve a catalog path beneath canonical or governed adoption roots."""

        return self._require_readable_asset(Path(path))

    def _require_readable_asset(self, path: Path) -> Path:
        candidate = path.resolve(strict=False)
        roots = (self.config.archive_root, *self.config.adoption_roots)
        for root in roots:
            try:
                candidate.relative_to(root.resolve(strict=False))
            except ValueError:
                continue
            return candidate
        raise ValueError("path escapes managed and read-only adoption roots")


def _validated_hash(value: str) -> str:
    return validate_path_segment(
        value,
        kind="sha256",
        field_name="content hash",
    )


def _parse_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _oldest_age(paths: list[Path], now: datetime) -> float:
    if not paths:
        return 0.0
    mtimes: list[float] = []
    for path in paths:
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            continue
    return max(0.0, now.timestamp() - min(mtimes)) if mtimes else 0.0


def _sum_sizes(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        try:
            total += int(path.stat().st_size)
        except OSError:
            continue
    return total


def _unescape_mountinfo(value: str) -> str:
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _mount_host(source: str) -> str | None:
    text = str(source or "").strip()
    if not text or text in {"unknown", "none"}:
        return None
    if ":" in text:
        return text.split(":", 1)[0].lower()
    if text.startswith("//"):
        return text[2:].split("/", 1)[0].lower()
    return None


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
