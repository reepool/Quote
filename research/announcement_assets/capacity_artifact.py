"""Fail-closed validation for production capacity approval evidence."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AnnouncementAssetConfig
from .storage import MountIdentity, probe_mount_identity, validate_backup_mount

CAPACITY_ARTIFACT_SCHEMA_VERSION = "official_announcement_asset_capacity_artifact.v4"
REQUIRED_SET_EVIDENCE_SCHEMA_VERSION = (
    "official_announcement_asset_required_set_evidence.v1"
)


class CapacityArtifactNotReadyError(RuntimeError):
    """The configured production capacity approval is absent or invalid."""


@dataclass(frozen=True)
class CapacityArtifactApproval:
    path: Path
    generated_at: str
    approver: str
    planning_horizon_years: int
    primary_required_set_actual_bytes: int
    backup_required_set_actual_bytes: int
    permanently_retained_recovery_manifest_bytes: int
    budget_basis: str
    primary_headroom_bytes: int
    backup_headroom_bytes: int


def validate_capacity_artifact(
    config: AnnouncementAssetConfig,
    *,
    now: datetime | None = None,
    artifact_path: Path | None = None,
) -> CapacityArtifactApproval | None:
    """Validate the configured approval before a production download or unlink."""

    if not config.capacity_artifact_required:
        return None
    path = artifact_path or config.capacity_artifact_path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CapacityArtifactNotReadyError("capacity_artifact_missing") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CapacityArtifactNotReadyError("capacity_artifact_unreadable") from exc
    if not isinstance(payload, Mapping):
        raise CapacityArtifactNotReadyError("capacity_artifact_not_mapping")
    if payload.get("schema_version") != CAPACITY_ARTIFACT_SCHEMA_VERSION:
        raise CapacityArtifactNotReadyError("capacity_artifact_schema_mismatch")
    if payload.get("configuration_fingerprint") != config.config_fingerprint:
        raise CapacityArtifactNotReadyError("capacity_artifact_config_mismatch")
    if payload.get("read_only") is not True:
        raise CapacityArtifactNotReadyError("capacity_artifact_not_read_only")
    generated_at = _timestamp(payload.get("generated_at"), "generated_at")
    observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age_seconds = (observed - generated_at).total_seconds()
    if age_seconds < 0 or age_seconds > config.capacity_artifact_max_age_hours * 3600:
        raise CapacityArtifactNotReadyError("capacity_artifact_expired")

    active_universe = _mapping(payload.get("active_universe"), "active_universe")
    if active_universe.get("status") != "complete":
        raise CapacityArtifactNotReadyError("capacity_artifact_universe_incomplete")
    _positive_int(active_universe.get("total"), "active_universe.total")

    primary = _mapping(payload.get("primary_archive"), "primary_archive")
    backup = _mapping(payload.get("backup_target"), "backup_target")
    _validate_storage_target(
        primary,
        name="primary_archive",
        expected_source=config.expected_filings_mount_source,
    )
    if backup.get("status") not in {None, "available"}:
        raise CapacityArtifactNotReadyError("capacity_artifact_backup_unavailable")
    _validate_storage_target(
        backup,
        name="backup_target",
        expected_source=config.backup.expected_mount_source,
    )
    if config.require_filings_mount:
        try:
            _validate_runtime_mount_identity(
                primary,
                name="primary_archive",
                actual=probe_mount_identity(config.filings_root),
            )
            actual_backup = validate_backup_mount(config)
        except CapacityArtifactNotReadyError:
            raise
        except (OSError, RuntimeError) as exc:
            raise CapacityArtifactNotReadyError(
                "capacity_artifact_runtime_mount_unavailable"
            ) from exc
        if actual_backup is None:
            raise CapacityArtifactNotReadyError(
                "capacity_artifact_backup_runtime_mount_missing"
            )
        _validate_runtime_mount_identity(
            backup,
            name="backup_target",
            actual=actual_backup,
        )
    primary_failure_domain = str(primary.get("failure_domain_identity") or "").strip()
    backup_failure_domain = str(backup.get("failure_domain_identity") or "").strip()
    if not primary_failure_domain or not backup_failure_domain:
        raise CapacityArtifactNotReadyError("capacity_artifact_failure_domain_missing")
    if primary_failure_domain == backup_failure_domain:
        raise CapacityArtifactNotReadyError("capacity_artifact_failure_domain_not_independent")
    if primary_failure_domain != _measured_failure_domain(primary):
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_primary_failure_domain_unverified"
        )
    if backup_failure_domain != _measured_failure_domain(backup):
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_backup_failure_domain_unverified"
        )
    if (
        config.backup.expected_failure_domain
        and backup.get("configured_failure_domain_label")
        != config.backup.expected_failure_domain
    ):
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_backup_failure_domain_label_mismatch"
        )
    distribution = _mapping(primary.get("pdf_distribution"), "pdf_distribution")
    if distribution.get("scope") != "manifest_verified_annual_report_candidates":
        raise CapacityArtifactNotReadyError("capacity_artifact_distribution_scope_invalid")
    file_count = _positive_int(
        distribution.get("file_count"), "pdf_distribution.file_count"
    )
    total_pdf_bytes = _positive_int(
        distribution.get("total_bytes"), "pdf_distribution.total_bytes"
    )
    if total_pdf_bytes < file_count:
        raise CapacityArtifactNotReadyError("capacity_artifact_distribution_invalid")
    p95 = _positive_int(distribution.get("p95_bytes"), "pdf_distribution.p95_bytes")
    p99 = _positive_int(distribution.get("p99_bytes"), "pdf_distribution.p99_bytes")
    maximum = _positive_int(distribution.get("max_bytes"), "pdf_distribution.max_bytes")
    if not p95 <= p99 <= maximum:
        raise CapacityArtifactNotReadyError("capacity_artifact_distribution_invalid")

    planning = _mapping(payload.get("planning"), "planning")
    if planning.get("status") != "approved":
        raise CapacityArtifactNotReadyError("capacity_artifact_not_approved")
    if planning.get("attachment_limit_within_observed_max") is not True:
        raise CapacityArtifactNotReadyError("capacity_artifact_attachment_limit_unapproved")
    attachment_limit = _positive_int(
        planning.get("attachment_limit_bytes"), "planning.attachment_limit_bytes"
    )
    if attachment_limit != config.storage.max_attachment_bytes:
        raise CapacityArtifactNotReadyError("capacity_artifact_attachment_limit_mismatch")
    if maximum > attachment_limit:
        raise CapacityArtifactNotReadyError("capacity_artifact_attachment_limit_unapproved")
    reservation = _positive_int(
        planning.get("unknown_length_reservation_bytes"),
        "planning.unknown_length_reservation_bytes",
    )
    if reservation != config.storage.unknown_length_reservation_bytes:
        raise CapacityArtifactNotReadyError("capacity_artifact_reservation_mismatch")
    if reservation < p99 or reservation > attachment_limit:
        raise CapacityArtifactNotReadyError("capacity_artifact_reservation_uncalibrated")
    horizon = _positive_int(
        planning.get("planning_horizon_years"), "planning.planning_horizon_years"
    )
    reported_primary_headroom = _non_negative_int(
        planning.get("estimated_primary_headroom_bytes"),
        "planning.estimated_primary_headroom_bytes",
    )
    reported_backup_headroom = _non_negative_int(
        planning.get("estimated_backup_headroom_bytes"),
        "planning.estimated_backup_headroom_bytes",
    )
    expected_growth = _positive_int(
        planning.get("expected_annual_growth_bytes"),
        "planning.expected_annual_growth_bytes",
    )
    stress_growth = _positive_int(
        planning.get("stress_annual_growth_bytes"),
        "planning.stress_annual_growth_bytes",
    )
    if stress_growth < expected_growth:
        raise CapacityArtifactNotReadyError("capacity_artifact_growth_invalid")
    budget_basis = str(planning.get("approved_budget_basis") or "").strip()
    if budget_basis not in {"expected", "stress"}:
        raise CapacityArtifactNotReadyError("capacity_artifact_budget_basis_invalid")
    annual_growth = expected_growth if budget_basis == "expected" else stress_growth
    temporary_peak = _positive_int(
        planning.get("estimated_temporary_peak_bytes"),
        "planning.estimated_temporary_peak_bytes",
    )
    replacement_peak = _positive_int(
        planning.get("estimated_old_plus_new_replacement_peak_bytes"),
        "planning.estimated_old_plus_new_replacement_peak_bytes",
    )
    full_market_bytes = _positive_int(
        planning.get("estimated_full_market_required_bytes"),
        "planning.estimated_full_market_required_bytes",
    )
    if (
        planning.get("old_plus_new_replacement_peak_basis")
        != "two_distinct_attachment_versions"
        or replacement_peak != 2 * full_market_bytes
    ):
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_replacement_peak_invalid"
        )
    primary_required = _non_negative_int(
        planning.get("primary_required_set_actual_bytes"),
        "planning.primary_required_set_actual_bytes",
    )
    backup_required = _non_negative_int(
        planning.get("backup_required_set_actual_bytes"),
        "planning.backup_required_set_actual_bytes",
    )
    recovery_bytes = _non_negative_int(
        planning.get("permanently_retained_recovery_manifest_bytes"),
        "planning.permanently_retained_recovery_manifest_bytes",
    )
    required_set_evidence = _mapping(
        payload.get("required_set_evidence"), "required_set_evidence"
    )
    measured_required_set = measure_required_set_evidence(config)
    if dict(required_set_evidence) != measured_required_set:
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_required_set_evidence_mismatch"
        )
    if primary_required != measured_required_set["primary_required_set"]["bytes"]:
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_primary_required_set_mismatch"
        )
    if backup_required != measured_required_set["backup_verified_set"]["bytes"]:
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_backup_required_set_mismatch"
        )
    if recovery_bytes != measured_required_set["permanent_recovery_set"]["bytes"]:
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_recovery_required_set_mismatch"
        )
    primary_free = _storage_free_bytes(primary, "primary_archive")
    backup_free = _storage_free_bytes(backup, "backup_target")
    horizon_growth = annual_growth * horizon
    replacement_overhead = replacement_peak - full_market_bytes
    primary_future_need = (
        horizon_growth
        + replacement_overhead
        + temporary_peak
        + config.storage.free_space_reserve_bytes
    )
    backup_current_shortfall = max(
        0,
        primary_required + recovery_bytes - backup_required,
    )
    backup_future_need = (
        backup_current_shortfall
        + horizon_growth
        + replacement_overhead
        + temporary_peak
        + config.backup.free_space_reserve_bytes
    )
    primary_headroom = primary_free - primary_future_need
    backup_headroom = backup_free - backup_future_need
    if primary_headroom < 0:
        raise CapacityArtifactNotReadyError("capacity_artifact_primary_capacity_insufficient")
    if backup_headroom < 0:
        raise CapacityArtifactNotReadyError("capacity_artifact_backup_capacity_insufficient")
    if reported_primary_headroom != primary_headroom:
        raise CapacityArtifactNotReadyError("capacity_artifact_primary_headroom_mismatch")
    if reported_backup_headroom != backup_headroom:
        raise CapacityArtifactNotReadyError("capacity_artifact_backup_headroom_mismatch")
    if config.require_filings_mount:
        if _current_free_bytes(config.filings_root) < primary_future_need:
            raise CapacityArtifactNotReadyError(
                "capacity_artifact_primary_runtime_capacity_insufficient"
            )
        backup_root = config.backup.mount_root
        if backup_root is None or _current_free_bytes(backup_root) < backup_future_need:
            raise CapacityArtifactNotReadyError(
                "capacity_artifact_backup_runtime_capacity_insufficient"
            )
    approver = str(planning.get("explicit_approver") or "").strip()
    if not approver:
        raise CapacityArtifactNotReadyError("capacity_artifact_approver_missing")
    return CapacityArtifactApproval(
        path=path,
        generated_at=generated_at.isoformat(),
        approver=approver,
        planning_horizon_years=horizon,
        primary_required_set_actual_bytes=primary_required,
        backup_required_set_actual_bytes=backup_required,
        permanently_retained_recovery_manifest_bytes=recovery_bytes,
        budget_basis=budget_basis,
        primary_headroom_bytes=primary_headroom,
        backup_headroom_bytes=backup_headroom,
    )


def _validate_storage_target(
    value: Mapping[str, Any], *, name: str, expected_source: str | None
) -> None:
    identity = _mapping(value.get("identity"), f"{name}.identity")
    usage = _mapping(value.get("usage"), f"{name}.usage")
    source = _mount_source(identity)
    if expected_source and source != expected_source:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_mount_mismatch")
    backing = identity.get("backing_mount")
    if not isinstance(backing, Mapping):
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_backing_mount_missing")
    required_identity = (
        "path",
        "device",
        "filesystem_id",
        "mount_target",
        "filesystem_type",
    )
    if any(identity.get(field) in {None, ""} for field in required_identity):
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_identity_incomplete")
    required_backing = ("mount_source", "mount_target", "filesystem_type")
    if any(backing.get(field) in {None, ""} for field in required_backing):
        raise CapacityArtifactNotReadyError(
            f"capacity_artifact_{name}_backing_mount_incomplete"
        )
    if backing.get("read_write") is not True:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_not_read_write")
    total = _positive_int(usage.get("total_bytes"), f"{name}.total_bytes")
    used = _non_negative_int(usage.get("used_bytes"), f"{name}.used_bytes")
    free = _non_negative_int(usage.get("free_bytes"), f"{name}.free_bytes")
    if used > total or free > total or used + free > total:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_usage_invalid")


def _storage_free_bytes(value: Mapping[str, Any], name: str) -> int:
    usage = _mapping(value.get("usage"), f"{name}.usage")
    return _non_negative_int(usage.get("free_bytes"), f"{name}.free_bytes")


def _current_free_bytes(path: Path) -> int:
    try:
        usage = os.statvfs(path)
    except OSError as exc:
        raise CapacityArtifactNotReadyError(
            "capacity_artifact_runtime_capacity_unavailable"
        ) from exc
    return int(usage.f_bavail) * int(usage.f_frsize or usage.f_bsize)


def measure_required_set_evidence(
    config: AnnouncementAssetConfig,
) -> dict[str, Any]:
    """Recompute catalog and backup required-set bytes without mutating SQLite."""

    catalog_path = (config.project_root / "data/research.db").resolve(strict=False)
    if not catalog_path.is_file():
        raise CapacityArtifactNotReadyError("capacity_required_set_catalog_missing")
    try:
        with sqlite3.connect(f"file:{catalog_path}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN")
            required = _hash_set(
                connection,
                """SELECT content_hash FROM effective_annual_reports
                   WHERE content_hash IS NOT NULL
                   UNION SELECT blob_hash FROM official_asset_retention_pins
                   WHERE released_at IS NULL
                   UNION SELECT blob_hash FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                   UNION SELECT replacement_blob_hash
                   FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                     AND replacement_blob_hash IS NOT NULL
                   UNION SELECT content_hash FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                   UNION SELECT replacement_content_hash
                   FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                     AND replacement_content_hash IS NOT NULL""",
            )
            recovery = _hash_set(
                connection,
                """SELECT content_hash FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                   UNION SELECT replacement_content_hash
                   FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                     AND replacement_content_hash IS NOT NULL""",
            )
            lengths = _blob_lengths(connection, required)
            backup = {
                str(row[0])
                for row in connection.execute(
                    """SELECT state.content_hash
                       FROM official_asset_backup_state AS state
                       JOIN official_document_blobs AS blob
                         ON blob.content_hash=state.content_hash
                       WHERE state.status='verified'
                         AND state.config_fingerprint=?
                         AND state.content_length=blob.content_length
                         AND state.verified_at IS NOT NULL
                         AND state.file_manifest_watermark IS NOT NULL
                         AND state.catalog_snapshot_watermark IS NOT NULL""",
                    (config.config_fingerprint,),
                ).fetchall()
                if str(row[0]) in required
            }
    except sqlite3.Error as exc:
        raise CapacityArtifactNotReadyError(
            "capacity_required_set_catalog_unreadable"
        ) from exc
    if set(lengths) != required:
        raise CapacityArtifactNotReadyError(
            "capacity_required_set_blob_metadata_incomplete"
        )
    return {
        "schema_version": REQUIRED_SET_EVIDENCE_SCHEMA_VERSION,
        "catalog_path": "data/research.db",
        "configuration_fingerprint": config.config_fingerprint,
        "primary_required_set": _set_measurement(required, lengths),
        "backup_verified_set": _set_measurement(backup, lengths),
        "permanent_recovery_set": _set_measurement(recovery, lengths),
    }


def _hash_set(connection: sqlite3.Connection, query: str) -> set[str]:
    return {str(row[0]) for row in connection.execute(query).fetchall() if row[0]}


def _blob_lengths(
    connection: sqlite3.Connection, hashes: set[str]
) -> dict[str, int]:
    if not hashes:
        return {}
    placeholders = ",".join("?" for _ in hashes)
    rows = connection.execute(
        f"SELECT content_hash, content_length FROM official_document_blobs "
        f"WHERE content_hash IN ({placeholders})",
        tuple(sorted(hashes)),
    ).fetchall()
    output: dict[str, int] = {}
    for row in rows:
        length = int(row[1])
        if length <= 0:
            raise CapacityArtifactNotReadyError(
                "capacity_required_set_blob_length_invalid"
            )
        output[str(row[0])] = length
    return output


def _set_measurement(
    hashes: set[str], lengths: Mapping[str, int]
) -> dict[str, int | str]:
    ordered = sorted(hashes)
    digest = hashlib.sha256("\n".join(ordered).encode("ascii")).hexdigest()
    return {
        "count": len(ordered),
        "bytes": sum(int(lengths[item]) for item in ordered),
        "fingerprint": digest,
    }


def _validate_runtime_mount_identity(
    value: Mapping[str, Any], *, name: str, actual: MountIdentity
) -> None:
    identity = _mapping(value.get("identity"), f"{name}.identity")
    backing = _mapping(identity.get("backing_mount"), f"{name}.backing_mount")
    recorded = (
        str(backing.get("mount_source") or ""),
        str(backing.get("mount_target") or ""),
        str(backing.get("filesystem_type") or ""),
        _non_negative_int(identity.get("device"), f"{name}.identity.device"),
    )
    current = (
        actual.source,
        str(actual.mount_point),
        actual.fs_type,
        actual.device_id,
        actual.filesystem_id,
    )
    recorded = (*recorded, str(identity.get("filesystem_id") or ""))
    if not actual.read_write:
        raise CapacityArtifactNotReadyError(
            f"capacity_artifact_{name}_runtime_not_read_write"
        )
    if recorded != current:
        raise CapacityArtifactNotReadyError(
            f"capacity_artifact_{name}_runtime_identity_mismatch"
        )


def _mount_source(identity: Mapping[str, Any]) -> str | None:
    backing = identity.get("backing_mount")
    if isinstance(backing, Mapping) and backing.get("mount_source"):
        return str(backing["mount_source"])
    source = identity.get("mount_source")
    return None if source in (None, "") else str(source)


def _measured_failure_domain(value: Mapping[str, Any]) -> str:
    identity = _mapping(value.get("identity"), "storage.identity")
    source = _mount_source(identity) or ""
    host = source.partition(":")[0].strip()
    if not host:
        raise CapacityArtifactNotReadyError("capacity_artifact_failure_domain_unresolved")
    return f"mount_host:{host}"


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_missing")
    return value


def _timestamp(value: Any, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_invalid") from exc
    if parsed.tzinfo is None:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_timezone_missing")
    return parsed.astimezone(timezone.utc)


def _positive_int(value: Any, name: str) -> int:
    result = _non_negative_int(value, name)
    if result <= 0:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_not_positive")
    return result


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CapacityArtifactNotReadyError(f"capacity_artifact_{name}_invalid")
    return value
