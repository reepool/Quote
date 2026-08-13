"""Incremental verified backup and restore gates for announcement blobs."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from .config import AnnouncementAssetConfig
from .models import (
    BACKUP_RECOVERY_JOURNAL_SCHEMA_VERSION,
    OfficialAssetBackupRecoveryJournalEntry,
    OfficialAssetRecoveryManifestEntry,
    OfficialAssetRecoveryPairClosure,
    canonical_json,
    stable_id,
    utc_now_iso,
)
from .operation_control import operation_stop_reason
from .path_segments import validate_path_segment
from .repository import AnnouncementAssetRepository
from .schema import OWNED_TABLES
from .storage import MountIdentity, validate_backup_mount

RECOVERY_JOURNAL_CAPTURE_TABLES = (
    "official_announcements",
    "official_announcement_attachments",
    "official_document_blobs",
    "official_attachment_versions",
    "official_asset_acquisition_leases",
    "effective_annual_reports",
    "official_asset_adoption_promotion_gates",
    "official_asset_operations",
    "official_asset_operation_subscriptions",
    "official_asset_consumer_requests",
    "official_asset_change_events",
    "official_annual_report_decisions",
    "official_asset_consumer_checkpoints",
    "official_asset_deletion_intents",
    "official_asset_deletion_audit",
    "official_asset_recovery_manifest",
    "official_asset_recovery_pair_closures",
    "official_asset_retention_pins",
    "official_asset_consumer_processing",
    "official_asset_discovery_state",
    "official_asset_attachment_retries",
    "official_asset_period_reconciliation",
    "official_asset_job_command_audit",
    "official_asset_operational_reports",
    "official_asset_coverage",
    "official_asset_bootstrap_runs",
    "official_asset_universe_snapshots",
    "official_asset_listed_security_census_snapshots",
    "official_asset_storage_reservations",
    "official_asset_capacity_override_audit",
    "official_asset_backup_state",
    "official_asset_storage_artifact_audit",
    "official_asset_legacy_path_manifest",
)
if set(RECOVERY_JOURNAL_CAPTURE_TABLES) != set(OWNED_TABLES).difference(
    {
        "official_asset_schema_versions",
        "official_asset_backup_recovery_journal",
    }
):
    raise RuntimeError("recovery-journal table scope is out of sync with schema")
RECOVERY_JOURNAL_CHANGESET_SCHEMA_VERSION = (
    "official_asset_recovery_changeset.v1"
)


@dataclass(frozen=True)
class BackupItemResult:
    content_hash: str
    content_length: int
    status: str
    backup_path: str | None
    copied: bool = False
    error_code: str | None = None


@dataclass(frozen=True)
class BackupRunResult:
    status: str
    destination_identity: str
    failure_domain: str
    catalog_snapshot_path: str | None
    catalog_snapshot_watermark: str | None
    file_manifest_path: str | None
    file_manifest_watermark: str | None
    total_blobs: int
    verified_blobs: int
    copied_blobs: int
    copied_bytes: int
    unprotected_blobs: int
    items: tuple[BackupItemResult, ...] = ()
    errors: tuple[str, ...] = ()
    recovery_pairs_closed: int = 0
    recovery_pairs_pending: int = 0
    recovery_journal_path: str | None = None
    recovery_journal_watermark: str | None = None
    recovery_journal_snapshot_sequence: int = 0
    recovery_journal_terminal_sequence: int = 0
    recovery_journal_terminal_watermark: str | None = None
    recovery_journal_source_generation: str | None = None


@dataclass(frozen=True)
class RestoreReadiness:
    ready: bool
    required_blobs: int
    verified_blobs: int
    missing_hashes: tuple[str, ...] = ()
    invalid_hashes: tuple[str, ...] = ()
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecoveryJournalReadiness:
    """Result of validating one declared post-snapshot journal interval."""

    ready: bool
    snapshot_sequence: int
    terminal_sequence: int
    verified_entries: int
    terminal_coverage_watermark: str | None
    errors: tuple[str, ...] = ()


class AnnouncementAssetBackupService:
    """Copy immutable blobs and pair them with a recoverable catalog snapshot."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        mount_validator: Callable[[AnnouncementAssetConfig], MountIdentity | None]
        = validate_backup_mount,
        recovery_crash_hook: Callable[[str, str], None] | None = None,
        backup_crash_hook: Callable[[str, str], None] | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.mount_validator = mount_validator
        self.recovery_crash_hook = recovery_crash_hook
        self.backup_crash_hook = backup_crash_hook

    def run(
        self,
        *,
        operator_repair: bool = False,
        operator_authorized: bool = False,
    ) -> BackupRunResult:
        """Back up every registered blob without mutating mismatched targets."""
        backup = self.config.backup
        if not backup.enabled:
            raise RuntimeError("announcement archive backup is disabled")
        if operator_repair and not operator_authorized:
            raise PermissionError("backup target repair requires operator authorization")
        identity = self.mount_validator(self.config)
        if identity is None or backup.destination_root is None:
            raise RuntimeError("backup mount identity is unavailable")
        destination = backup.destination_root.resolve(strict=False)
        destination.mkdir(parents=True, exist_ok=True)
        blobs = self._list_blobs()
        planned_blob_bytes = self._planned_blob_copy_bytes(
            destination,
            blobs,
            operator_repair=operator_repair,
        )
        planned_bytes = (
            planned_blob_bytes
            + self._catalog_snapshot_planned_bytes()
            + self._recovery_metadata_planned_bytes(blobs)
        )
        self._preflight_capacity(destination, max(1, planned_bytes))

        self._assert_mount_identity(identity)
        snapshot_path, snapshot_watermark = self._create_catalog_snapshot(
            destination,
            expected_identity=identity,
        )
        self._backup_crash_boundary("after_catalog_snapshot", snapshot_watermark)
        results: list[BackupItemResult] = []
        errors: list[str] = []
        stopped_reason: str | None = None
        for blob in blobs:
            if stopped_reason := operation_stop_reason():
                errors.append(stopped_reason)
                break
            result = self._backup_blob(
                destination,
                blob,
                identity=identity,
                operator_repair=operator_repair,
            )
            results.append(result)
            if result.error_code:
                errors.append(f"{result.content_hash}:{result.error_code}")

        verified = [item for item in results if item.status == "verified"]
        manifest_path: Path | None = None
        manifest_watermark: str | None = None
        journal_path: Path | None = None
        journal_watermark: str | None = None
        journal_metadata: dict[str, Any] = {}
        if not stopped_reason and len(verified) == len(results) == len(blobs):
            captured_increment = self._capture_post_snapshot_recovery_increment(
                catalog_snapshot_path=snapshot_path,
                catalog_snapshot_watermark=snapshot_watermark,
            )
            snapshot_sequence, snapshot_coverage, snapshot_generation = (
                _snapshot_recovery_journal_tail(snapshot_path)
            )
            terminal_sequence = (
                snapshot_sequence
                if captured_increment is None
                else int(captured_increment.journal_sequence)
            )
            terminal_coverage = (
                snapshot_coverage
                if captured_increment is None
                else captured_increment.coverage_watermark
            )
            terminal_generation = (
                snapshot_generation
                if captured_increment is None
                else captured_increment.source_catalog_generation
            )
            journal_path, journal_watermark, journal_metadata = (
                self._persist_recovery_journal_bundle(
                    destination,
                    catalog_snapshot_path=snapshot_path,
                    catalog_snapshot_watermark=snapshot_watermark,
                    terminal_sequence=terminal_sequence,
                    terminal_coverage_watermark=terminal_coverage,
                    source_catalog_generation=terminal_generation,
                    expected_identity=identity,
                )
            )
            manifest_payload = {
                "schema_version": "official_asset_backup_manifest.v1",
                "catalog_snapshot_watermark": snapshot_watermark,
                "destination_identity": identity.filesystem_key,
                "failure_domain": backup.expected_failure_domain,
                "created_at": utc_now_iso(),
                "recovery_journal": {
                    "path": str(journal_path),
                    "watermark": journal_watermark,
                    **journal_metadata,
                },
                "blobs": [
                    {
                        "content_hash": item.content_hash,
                        "content_length": item.content_length,
                        "backup_path": item.backup_path,
                    }
                    for item in sorted(verified, key=lambda value: value.content_hash)
                ],
            }
            manifest_content = canonical_json(manifest_payload).encode("utf-8")
            manifest_watermark = hashlib.sha256(manifest_content).hexdigest()
            manifest_path = destination / "manifests" / f"{manifest_watermark}.json"
            self._atomic_write(
                manifest_path,
                manifest_content,
                expected_identity=identity,
            )
            self._backup_crash_boundary("after_file_manifest", manifest_watermark)

        for item in results:
            self._assert_mount_identity(identity)
            paired = item.status == "verified" and manifest_watermark is not None
            self.repository.upsert_backup_state(
                content_hash=item.content_hash,
                config_fingerprint=self.config.config_fingerprint,
                destination_identity=identity.filesystem_key,
                failure_domain=backup.expected_failure_domain,
                backup_path=item.backup_path,
                content_length=item.content_length,
                status="verified" if paired else item.status,
                file_manifest_watermark=manifest_watermark if paired else None,
                catalog_snapshot_watermark=snapshot_watermark if paired else None,
                verified_at=utc_now_iso() if paired else None,
                error_code=item.error_code,
            )

        recovery_closed = 0
        recovery_pending = 0
        if manifest_watermark is not None and not stopped_reason:
            recovery_closed, recovery_pending = self.close_pending_recovery_pairs(
                actor="announcement_asset_backup",
            )

        return BackupRunResult(
            status="success" if not errors else "partial",
            destination_identity=identity.filesystem_key,
            failure_domain=str(backup.expected_failure_domain or ""),
            catalog_snapshot_path=str(snapshot_path),
            catalog_snapshot_watermark=snapshot_watermark,
            file_manifest_path=None if manifest_path is None else str(manifest_path),
            file_manifest_watermark=manifest_watermark,
            total_blobs=len(blobs),
            verified_blobs=len(verified),
            copied_blobs=sum(1 for item in results if item.copied),
            copied_bytes=sum(item.content_length for item in results if item.copied),
            unprotected_blobs=len(blobs) - len(verified),
            items=tuple(results),
            errors=tuple(errors),
            recovery_pairs_closed=recovery_closed,
            recovery_pairs_pending=recovery_pending,
            recovery_journal_path=None if journal_path is None else str(journal_path),
            recovery_journal_watermark=journal_watermark,
            recovery_journal_snapshot_sequence=int(
                journal_metadata.get("snapshot_sequence", 0)
            ),
            recovery_journal_terminal_sequence=int(
                journal_metadata.get("terminal_sequence", 0)
            ),
            recovery_journal_terminal_watermark=journal_metadata.get(
                "terminal_coverage_watermark"
            ),
            recovery_journal_source_generation=journal_metadata.get(
                "source_catalog_generation"
            ),
        )

    def close_pending_recovery_pairs(
        self,
        *,
        actor: str,
        limit: int = 100,
    ) -> tuple[int, int]:
        """Close backed deletion pairs and hand their pins to the required set."""

        pending = self.repository.list_deletions(limit=limit)
        closed = 0
        still_pending = 0
        for intent in pending:
            if not intent.get("recovery_pair_id") or not intent.get("recovery_pin_id"):
                still_pending += 1
                continue
            if self.repository.deletion_recovery_pair_satisfies_unlink(
                intent["deletion_id"]
            ):
                continue
            try:
                if self._close_recovery_pair(intent, actor=actor):
                    closed += 1
                else:
                    still_pending += 1
            except Exception:
                # Every completed boundary is durable and idempotent.  Preserve
                # the blocker so a later backup run resumes at the next step.
                still_pending += 1
                raise
        return closed, still_pending

    def required_backup_hashes(self) -> set[str]:
        """Enumerate all blobs that must survive reads, recovery, or deletion."""

        with self.repository.connection() as conn:
            rows = conn.execute(
                """SELECT content_hash FROM effective_annual_reports
                   WHERE content_hash IS NOT NULL
                   UNION
                   SELECT blob_hash FROM official_asset_retention_pins
                   WHERE released_at IS NULL
                   UNION
                   SELECT blob_hash FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                   UNION
                   SELECT replacement_blob_hash FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                     AND replacement_blob_hash IS NOT NULL
                   UNION
                   SELECT content_hash FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                   UNION
                   SELECT replacement_content_hash
                   FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                     AND replacement_content_hash IS NOT NULL"""
            ).fetchall()
        return {str(row[0]) for row in rows if row[0]}

    def verify_recovery_manifest_entry(
        self, manifest: OfficialAssetRecoveryManifestEntry
    ) -> bool:
        """Verify immutable recovery objects against their paired file manifest."""

        return self._manifest_backup_objects_are_valid(manifest)

    def _close_recovery_pair(
        self,
        intent: dict[str, Any],
        *,
        actor: str,
    ) -> bool:
        pair_id = str(intent["recovery_pair_id"])
        existing_closure = self.repository.get_recovery_pair_closure(pair_id)
        manifest = self.repository.get_recovery_manifest_by_pair(pair_id)
        if existing_closure is not None:
            return self.repository.complete_recovery_pair_handoff(
                intent["deletion_id"], recovery_id=existing_closure.recovery_id
            ) or self.repository.deletion_recovery_pair_satisfies_unlink(
                intent["deletion_id"]
            )

        if manifest is None:
            self._crash_boundary("after_file_backup", intent["deletion_id"])
            manifest = self._build_recovery_manifest(intent, actor=actor)
            if manifest is None:
                return False
            manifest = self.repository.register_recovery_manifest_entry(manifest)
            bound = self.repository.bind_deletion_recovery_manifest(
                intent["deletion_id"], recovery_manifest_id=manifest.recovery_id
            )
            if not bound:
                current = self.repository.get_deletion(intent["deletion_id"])
                if current is None or current.get("recovery_manifest_id") != manifest.recovery_id:
                    raise RuntimeError("deletion intent is bound to another recovery manifest")
            self._crash_boundary("after_manifest", intent["deletion_id"])
        elif intent.get("recovery_manifest_id") != manifest.recovery_id:
            bound = self.repository.bind_deletion_recovery_manifest(
                intent["deletion_id"], recovery_manifest_id=manifest.recovery_id
            )
            if not bound:
                current = self.repository.get_deletion(intent["deletion_id"])
                if current is None or current.get("recovery_manifest_id") != manifest.recovery_id:
                    raise RuntimeError("deletion intent manifest binding is inconsistent")

        if not self._manifest_backup_objects_are_valid(manifest):
            return False
        destination = self.config.backup.destination_root
        if destination is None:
            return False
        snapshot_path, snapshot_identity = self._create_catalog_snapshot(destination)
        snapshot_hash, _ = _hash_file(snapshot_path)
        self._verify_snapshot_contains_recovery_pair(
            snapshot_path,
            deletion_id=intent["deletion_id"],
            manifest=manifest,
        )
        self._crash_boundary("after_catalog_snapshot", intent["deletion_id"])
        closure = OfficialAssetRecoveryPairClosure(
            closure_id=stable_id(
                "recovery-closure",
                pair_id,
                manifest.recovery_id,
                snapshot_hash,
                manifest.file_manifest_watermark,
            ),
            recovery_pair_id=pair_id,
            recovery_id=manifest.recovery_id,
            catalog_snapshot_identity=str(snapshot_path),
            catalog_snapshot_hash=snapshot_hash,
            file_manifest_watermark=manifest.file_manifest_watermark,
            verified_at=utc_now_iso(),
            verified_by=actor,
            evidence={
                "bidirectional_check": True,
                "catalog_snapshot_watermark": snapshot_identity,
                "deletion_id": intent["deletion_id"],
            },
        )
        self.repository.register_recovery_pair_closure(closure)
        self._crash_boundary("after_pair_closure", intent["deletion_id"])
        handed_off = self.repository.complete_recovery_pair_handoff(
            intent["deletion_id"], recovery_id=manifest.recovery_id
        )
        self._crash_boundary("after_pin_handoff", intent["deletion_id"])
        return handed_off or self.repository.deletion_recovery_pair_satisfies_unlink(
            intent["deletion_id"]
        )

    def _build_recovery_manifest(
        self,
        intent: dict[str, Any],
        *,
        actor: str,
    ) -> OfficialAssetRecoveryManifestEntry | None:
        predecessor_state = self.repository.get_backup_state(intent["blob_hash"])
        if not self._backup_state_is_usable(predecessor_state):
            return None
        replacement_hash = intent.get("replacement_blob_hash")
        replacement_state = (
            None
            if not replacement_hash
            else self.repository.get_backup_state(str(replacement_hash))
        )
        if replacement_hash and not self._backup_state_is_usable(replacement_state):
            return None
        if (
            replacement_state is not None
            and replacement_state.get("file_manifest_watermark")
            != predecessor_state.get("file_manifest_watermark")
        ):
            return None
        decision = (
            None
            if not intent.get("decision_id")
            else self.repository.get_effective_decision(str(intent["decision_id"]))
        )
        manifest_kind = (
            "withdrawal_tombstone"
            if intent.get("reason") == "withdrawn_without_replacement"
            else "correction_predecessor"
        )
        if manifest_kind == "withdrawal_tombstone" and replacement_hash is not None:
            raise RuntimeError("withdrawal recovery cannot fabricate a replacement blob")
        pair_id = str(intent["recovery_pair_id"])
        recovery_id = stable_id("recovery-manifest", pair_id)
        return OfficialAssetRecoveryManifestEntry(
            recovery_id=recovery_id,
            manifest_kind=manifest_kind,
            manifest_version=1,
            predecessor_asset_id=intent.get("predecessor_asset_id"),
            source=None if decision is None else decision.predecessor_source,
            source_announcement_id=(
                None if decision is None else decision.predecessor_source_announcement_id
            ),
            attachment_id=(
                None if decision is None else decision.predecessor_attachment_id
            ),
            version_id=None if decision is None else decision.predecessor_version_id,
            prior_path=str(intent["managed_path"]),
            content_hash=str(intent["blob_hash"]),
            replacement_asset_id=intent.get("replacement_asset_id"),
            replacement_content_hash=(
                None if replacement_hash is None else str(replacement_hash)
            ),
            backup_object=str(predecessor_state["backup_path"]),
            file_manifest_watermark=str(
                predecessor_state["file_manifest_watermark"]
            ),
            recovery_pair_id=pair_id,
            consumer=None,
            active_indefinitely=True,
            created_at=utc_now_iso(),
            created_by=actor,
            evidence={
                "deletion_id": intent["deletion_id"],
                "decision_id": intent.get("decision_id"),
                "outbox_event_key": intent.get("outbox_event_key"),
                "predecessor_backup_verified": True,
                "replacement_backup_object": (
                    None if replacement_state is None else replacement_state["backup_path"]
                ),
                "replacement_backup_verified": replacement_state is not None,
            },
        )

    def _manifest_backup_objects_are_valid(
        self, manifest: OfficialAssetRecoveryManifestEntry
    ) -> bool:
        destination = self.config.backup.destination_root
        if destination is None:
            return False
        file_manifest_path = (
            destination
            / "manifests"
            / f"{manifest.file_manifest_watermark}.json"
        )
        try:
            manifest_bytes = file_manifest_path.read_bytes()
            if hashlib.sha256(manifest_bytes).hexdigest() != manifest.file_manifest_watermark:
                return False
            file_manifest = json.loads(manifest_bytes.decode("utf-8"))
            manifest_blobs = {
                str(item["content_hash"]): item
                for item in file_manifest.get("blobs", ())
            }
        except (OSError, UnicodeError, ValueError, KeyError, TypeError):
            return False
        predecessor = self.repository.get_blob(manifest.content_hash)
        predecessor_entry = manifest_blobs.get(manifest.content_hash)
        if (
            predecessor is None
            or predecessor_entry is None
            or predecessor_entry.get("backup_path") != manifest.backup_object
            or _validate_pdf_file(
                Path(manifest.backup_object),
                expected_hash=manifest.content_hash,
                expected_length=predecessor.content_length,
            )
            != "valid"
        ):
            return False
        if manifest.manifest_kind == "legacy_path_rollback":
            return (
                manifest.replacement_content_hash == manifest.content_hash
                and manifest.replacement_asset_id == manifest.predecessor_asset_id
            )
        if manifest.replacement_content_hash is None:
            return manifest.manifest_kind == "withdrawal_tombstone"
        replacement = self.repository.get_blob(manifest.replacement_content_hash)
        replacement_path = manifest.evidence.get("replacement_backup_object")
        replacement_entry = manifest_blobs.get(manifest.replacement_content_hash)
        return bool(
            replacement is not None
            and replacement_path
            and replacement_entry is not None
            and replacement_entry.get("backup_path") == replacement_path
            and _validate_pdf_file(
                Path(str(replacement_path)),
                expected_hash=manifest.replacement_content_hash,
                expected_length=replacement.content_length,
            )
            == "valid"
        )

    def _backup_state_is_usable(self, state: dict[str, Any] | None) -> bool:
        return bool(
            state
            and state.get("status") == "verified"
            and state.get("verified_at")
            and state.get("backup_path")
            and state.get("file_manifest_watermark")
            and state.get("catalog_snapshot_watermark")
            and state.get("failure_domain")
            and state.get("failure_domain")
            == self.config.backup.expected_failure_domain
        )

    @staticmethod
    def _verify_snapshot_contains_recovery_pair(
        snapshot_path: Path,
        *,
        deletion_id: str,
        manifest: OfficialAssetRecoveryManifestEntry,
    ) -> None:
        with sqlite3.connect(f"file:{snapshot_path}?mode=ro", uri=True) as conn:
            manifest_row = conn.execute(
                """SELECT recovery_pair_id, file_manifest_watermark
                   FROM official_asset_recovery_manifest WHERE recovery_id=?""",
                (manifest.recovery_id,),
            ).fetchone()
            intent_row = conn.execute(
                """SELECT recovery_pair_id, recovery_manifest_id, decision_id,
                          outbox_event_key, reason, replacement_asset_id,
                          replacement_blob_hash
                   FROM official_asset_deletion_intents WHERE deletion_id=?""",
                (deletion_id,),
            ).fetchone()
            decision_row = conn.execute(
                """SELECT decision_kind, predecessor_asset_id,
                          predecessor_content_hash, replacement_asset_id,
                          replacement_content_hash, instrument_id, fiscal_year,
                          outbox_event_key
                   FROM official_annual_report_decisions WHERE decision_id=?""",
                (manifest.evidence.get("decision_id"),),
            ).fetchone()
            event_row = conn.execute(
                """SELECT event_key FROM official_asset_change_events
                   WHERE event_key=?""",
                (manifest.evidence.get("outbox_event_key"),),
            ).fetchone()
        if manifest_row != (
            manifest.recovery_pair_id,
            manifest.file_manifest_watermark,
        ) or intent_row is None:
            raise RuntimeError("catalog snapshot does not contain the recovery pair")
        if (
            intent_row[0] != manifest.recovery_pair_id
            or intent_row[1] != manifest.recovery_id
            or intent_row[2] != manifest.evidence.get("decision_id")
            or intent_row[3] != manifest.evidence.get("outbox_event_key")
            or event_row is None
            or decision_row is None
            or decision_row[1] != manifest.predecessor_asset_id
            or decision_row[2] != manifest.content_hash
            or decision_row[7] != manifest.evidence.get("outbox_event_key")
        ):
            raise RuntimeError("catalog snapshot recovery lineage is inconsistent")
        if manifest.manifest_kind == "withdrawal_tombstone":
            if (
                decision_row[0] != "withdrawn_without_replacement"
                or decision_row[3] is not None
                or decision_row[4] is not None
                or intent_row[5] is not None
                or intent_row[6] is not None
            ):
                raise RuntimeError("withdrawal snapshot contains a replacement")
            with sqlite3.connect(f"file:{snapshot_path}?mode=ro", uri=True) as conn:
                current = conn.execute(
                    """SELECT 1 FROM effective_annual_reports
                       WHERE instrument_id=? AND fiscal_year=?""",
                    (decision_row[5], decision_row[6]),
                ).fetchone()
            if current is not None:
                raise RuntimeError("withdrawal snapshot still contains a current winner")
        elif (
            decision_row[3] != manifest.replacement_asset_id
            or decision_row[4] != manifest.replacement_content_hash
            or intent_row[5] != manifest.replacement_asset_id
            or intent_row[6] != manifest.replacement_content_hash
        ):
            raise RuntimeError("replacement snapshot lineage is inconsistent")

    def _crash_boundary(self, boundary: str, deletion_id: str) -> None:
        if self.recovery_crash_hook is not None:
            self.recovery_crash_hook(boundary, deletion_id)

    def verify_restore_readiness(self) -> RestoreReadiness:
        """Verify every blob required for safe reads and destructive maintenance."""
        required = self._required_restore_hashes()
        missing: list[str] = []
        invalid: list[str] = []
        for content_hash in sorted(required):
            blob = self.repository.get_blob(content_hash)
            if blob is None:
                missing.append(content_hash)
                continue
            status = _validate_pdf_file(
                Path(blob.canonical_path),
                expected_hash=content_hash,
                expected_length=blob.content_length,
            )
            if status == "missing":
                missing.append(content_hash)
            elif status != "valid":
                invalid.append(content_hash)
        verified = len(required) - len(missing) - len(invalid)
        return RestoreReadiness(
            ready=not missing and not invalid,
            required_blobs=len(required),
            verified_blobs=verified,
            missing_hashes=tuple(missing),
            invalid_hashes=tuple(invalid),
            diagnostics={"verification": "full_required_set", "sampled": False},
        )

    def verify_recovery_journal(
        self,
        *,
        snapshot_sequence: int,
        snapshot_coverage_watermark: str | None,
        terminal_sequence: int,
        terminal_coverage_watermark: str | None,
        source_catalog_generation: str,
        entries: list[OfficialAssetBackupRecoveryJournalEntry] | None = None,
    ) -> RecoveryJournalReadiness:
        """Verify a complete ordered post-snapshot recovery-journal interval.

        The declared terminal sequence and watermark are independent restore
        inputs (for example from the paired file manifest or a persisted write
        freeze record).  They are required to distinguish a valid prefix from a
        silently truncated tail.
        """

        snapshot = int(snapshot_sequence)
        terminal = int(terminal_sequence)
        generation = str(source_catalog_generation or "").strip()
        errors: list[str] = []
        if snapshot < 0 or terminal < snapshot:
            errors.append("invalid_sequence_bounds")
        if not generation:
            errors.append("source_catalog_generation_missing")

        interval = (
            list(entries)
            if entries is not None
            else [
                entry
                for entry in self.repository.list_backup_recovery_journal_entries()
                if snapshot < int(entry.journal_sequence) <= terminal
            ]
        )
        expected_sequence = snapshot + 1
        expected_predecessor = snapshot_coverage_watermark
        seen_watermarks: set[str] = set()
        verified = 0
        for entry in interval:
            if entry.schema_version != BACKUP_RECOVERY_JOURNAL_SCHEMA_VERSION:
                errors.append(
                    f"journal_schema_mismatch:{entry.journal_sequence}"
                )
            if int(entry.journal_sequence) != expected_sequence:
                errors.append(
                    "journal_sequence_mismatch:"
                    f"expected={expected_sequence}:actual={entry.journal_sequence}"
                )
            if entry.predecessor_watermark != expected_predecessor:
                errors.append(
                    f"journal_predecessor_mismatch:{entry.journal_sequence}"
                )
            if entry.source_catalog_generation != generation:
                errors.append(
                    f"journal_generation_mismatch:{entry.journal_sequence}"
                )
            if not entry.coverage_watermark or entry.coverage_watermark in seen_watermarks:
                errors.append(
                    f"journal_coverage_watermark_invalid:{entry.journal_sequence}"
                )
            if (
                entry.integrity_hash
                != self.repository.recovery_journal_integrity_hash(entry)
            ):
                errors.append(f"journal_integrity_mismatch:{entry.journal_sequence}")
            if not errors or not any(
                error.endswith(f":{entry.journal_sequence}")
                or f"actual={entry.journal_sequence}" in error
                for error in errors
            ):
                verified += 1
            seen_watermarks.add(entry.coverage_watermark)
            expected_sequence = int(entry.journal_sequence) + 1
            expected_predecessor = entry.coverage_watermark

        expected_count = max(0, terminal - snapshot)
        if len(interval) != expected_count:
            errors.append(
                f"journal_interval_length_mismatch:expected={expected_count}:actual={len(interval)}"
            )
        actual_terminal = (
            snapshot_coverage_watermark if not interval else interval[-1].coverage_watermark
        )
        if actual_terminal != terminal_coverage_watermark:
            errors.append("journal_terminal_watermark_mismatch")
        if terminal == snapshot and terminal_coverage_watermark != snapshot_coverage_watermark:
            errors.append("empty_journal_terminal_mismatch")

        return RecoveryJournalReadiness(
            ready=not errors,
            snapshot_sequence=snapshot,
            terminal_sequence=terminal,
            verified_entries=verified,
            terminal_coverage_watermark=actual_terminal,
            errors=tuple(dict.fromkeys(errors)),
        )

    def _capture_post_snapshot_recovery_increment(
        self,
        *,
        catalog_snapshot_path: Path,
        catalog_snapshot_watermark: str,
    ) -> OfficialAssetBackupRecoveryJournalEntry | None:
        """Capture one deterministic post-snapshot catalog changeset.

        ``BEGIN IMMEDIATE`` fences the terminal source view and the journal
        append in one commit.  Required attachment membership must remain
        unchanged: otherwise the initial blob enumeration cannot protect the
        newly referenced bytes and the whole backup must be retried.
        """

        snapshot_uri = f"file:{catalog_snapshot_path}?mode=ro"
        with sqlite3.connect(snapshot_uri, uri=True) as baseline:
            baseline.row_factory = sqlite3.Row
            baseline_required = _required_blob_hashes_from_connection(baseline)
            snapshot_tail = _recovery_journal_tail_from_connection(baseline)
            with self.repository.transaction() as live:
                live_tail = _recovery_journal_tail_from_connection(live)
                if live_tail != snapshot_tail:
                    raise RuntimeError(
                        "recovery journal advanced after catalog snapshot; retry"
                    )
                live_required = _required_blob_hashes_from_connection(live)
                if live_required != baseline_required:
                    raise RuntimeError(
                        "catalog required blob set changed during backup; retry"
                    )
                tables = _build_catalog_changeset(
                    baseline,
                    live,
                    tables=RECOVERY_JOURNAL_CAPTURE_TABLES,
                )
                if not tables:
                    return None
                sequence = int(live_tail[0]) + 1
                predecessor = live_tail[1]
                generation = (
                    str(live_tail[2])
                    if live_tail[2] is not None
                    else stable_id(
                        "catalog-generation",
                        catalog_snapshot_watermark,
                    )
                )
                payload = {
                    "schema_version": RECOVERY_JOURNAL_CHANGESET_SCHEMA_VERSION,
                    "baseline_catalog_snapshot_watermark": (
                        catalog_snapshot_watermark
                    ),
                    "tables": tables,
                }
                payload_hash = hashlib.sha256(
                    canonical_json(payload).encode("utf-8")
                ).hexdigest()
                coverage = stable_id(
                    "recovery-journal-coverage",
                    generation,
                    sequence,
                    predecessor or "root",
                    payload_hash,
                )
                created_at = utc_now_iso()
                entry = OfficialAssetBackupRecoveryJournalEntry(
                    journal_entry_id=stable_id(
                        "backup-recovery-journal",
                        generation,
                        sequence,
                        coverage,
                    ),
                    journal_sequence=sequence,
                    increment_kind="catalog_changeset",
                    increment_identity=stable_id(
                        "catalog-changeset",
                        catalog_snapshot_watermark,
                        payload_hash,
                    ),
                    source_catalog_generation=generation,
                    predecessor_watermark=predecessor,
                    coverage_watermark=coverage,
                    integrity_hash="",
                    payload=payload,
                    created_at=created_at,
                    created_by="announcement_asset_backup",
                )
                entry = replace(
                    entry,
                    integrity_hash=(
                        self.repository.recovery_journal_integrity_hash(entry)
                    ),
                )
                return (
                    self.repository.append_backup_recovery_journal_entry_in_transaction(
                        live,
                        entry,
                    )
                )

    def load_recovery_journal_bundle(
        self,
        path: str | Path,
        *,
        expected_watermark: str | None = None,
    ) -> tuple[dict[str, Any], list[OfficialAssetBackupRecoveryJournalEntry]]:
        """Load and authenticate one independently persisted journal bundle."""

        artifact = Path(path)
        content = artifact.read_bytes()
        watermark = hashlib.sha256(content).hexdigest()
        if expected_watermark is not None and watermark != expected_watermark:
            raise ValueError("recovery journal bundle watermark mismatch")
        if artifact.stem != watermark:
            raise ValueError("recovery journal bundle path identity mismatch")
        try:
            payload = json.loads(content.decode("utf-8"))
        except (UnicodeError, ValueError, TypeError) as exc:
            raise ValueError("recovery journal bundle is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise TypeError("recovery journal bundle root is invalid")
        if payload.get("schema_version") != "official_asset_recovery_journal_bundle.v1":
            raise ValueError("recovery journal bundle schema mismatch")
        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, list):
            raise TypeError("recovery journal bundle entries are invalid")
        entries: list[OfficialAssetBackupRecoveryJournalEntry] = []
        for raw in raw_entries:
            if not isinstance(raw, dict):
                raise TypeError("recovery journal bundle entry is invalid")
            try:
                entries.append(
                    OfficialAssetBackupRecoveryJournalEntry(
                        journal_entry_id=str(raw["journal_entry_id"]),
                        journal_sequence=int(raw["journal_sequence"]),
                        increment_kind=str(raw["increment_kind"]),
                        increment_identity=str(raw["increment_identity"]),
                        source_catalog_generation=str(
                            raw["source_catalog_generation"]
                        ),
                        predecessor_watermark=raw.get("predecessor_watermark"),
                        coverage_watermark=str(raw["coverage_watermark"]),
                        integrity_hash=str(raw["integrity_hash"]),
                        payload=dict(raw["payload"]),
                        created_at=str(raw["created_at"]),
                        created_by=str(raw["created_by"]),
                        schema_version=str(raw["schema_version"]),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("recovery journal bundle entry is incomplete") from exc
        metadata = {
            key: payload.get(key)
            for key in (
                "catalog_snapshot_watermark",
                "snapshot_sequence",
                "snapshot_coverage_watermark",
                "terminal_sequence",
                "terminal_coverage_watermark",
                "source_catalog_generation",
            )
        }
        terminal_sequence = int(metadata.get("terminal_sequence") or 0)
        terminal_watermark = metadata.get("terminal_coverage_watermark")
        source_generation = str(metadata.get("source_catalog_generation") or "")
        full_chain = self.verify_recovery_journal(
            snapshot_sequence=0,
            snapshot_coverage_watermark=None,
            terminal_sequence=terminal_sequence,
            terminal_coverage_watermark=terminal_watermark,
            source_catalog_generation=source_generation,
            entries=entries,
        )
        if not full_chain.ready:
            raise ValueError(
                "recovery journal bundle chain is invalid: "
                + ",".join(full_chain.errors)
            )
        return metadata, entries

    def _persist_recovery_journal_bundle(
        self,
        destination: Path,
        *,
        catalog_snapshot_path: Path,
        catalog_snapshot_watermark: str,
        terminal_sequence: int,
        terminal_coverage_watermark: str | None,
        source_catalog_generation: str | None,
        expected_identity: MountIdentity,
    ) -> tuple[Path, str, dict[str, Any]]:
        snapshot_sequence, snapshot_coverage, snapshot_generation = (
            _snapshot_recovery_journal_tail(catalog_snapshot_path)
        )
        terminal_sequence = int(terminal_sequence)
        entries = [
            entry
            for entry in self.repository.list_backup_recovery_journal_entries()
            if int(entry.journal_sequence) <= terminal_sequence
        ]
        terminal = entries[-1] if entries else None
        terminal_coverage = (
            None if terminal is None else terminal.coverage_watermark
        )
        if terminal_coverage != terminal_coverage_watermark:
            raise RuntimeError("recovery journal terminal watermark changed")
        generations = {entry.source_catalog_generation for entry in entries}
        if len(generations) > 1:
            raise RuntimeError("recovery journal spans multiple catalog generations")
        source_generation = (
            str(source_catalog_generation)
            if source_catalog_generation
            else next(iter(generations))
            if generations
            else snapshot_generation
            or stable_id("catalog-generation", catalog_snapshot_watermark)
        )
        if generations and generations != {source_generation}:
            raise RuntimeError("recovery journal terminal generation changed")
        if snapshot_sequence > terminal_sequence:
            raise RuntimeError("recovery journal is truncated behind catalog snapshot")
        if snapshot_generation and snapshot_generation != source_generation:
            raise RuntimeError("recovery journal catalog generation changed")
        payload = {
            "schema_version": "official_asset_recovery_journal_bundle.v1",
            "catalog_snapshot_watermark": catalog_snapshot_watermark,
            "snapshot_sequence": snapshot_sequence,
            "snapshot_coverage_watermark": snapshot_coverage,
            "terminal_sequence": terminal_sequence,
            "terminal_coverage_watermark": terminal_coverage,
            "source_catalog_generation": source_generation,
            "created_at": utc_now_iso(),
            "entries": [_recovery_journal_entry_mapping(entry) for entry in entries],
        }
        content = canonical_json(payload).encode("utf-8")
        self._preflight_capacity(destination, max(1, len(content)))
        watermark = hashlib.sha256(content).hexdigest()
        path = destination / "recovery-journal" / f"{watermark}.json"
        self._atomic_write(path, content, expected_identity=expected_identity)
        metadata = {
            key: payload[key]
            for key in (
                "catalog_snapshot_watermark",
                "snapshot_sequence",
                "snapshot_coverage_watermark",
                "terminal_sequence",
                "terminal_coverage_watermark",
                "source_catalog_generation",
            )
        }
        return path, watermark, metadata

    def _list_blobs(self) -> list[dict[str, Any]]:
        required = sorted(self.required_backup_hashes())
        if not required:
            return []
        placeholders = ",".join("?" for _ in required)
        with self.repository.connection() as conn:
            rows = conn.execute(
                """SELECT content_hash, content_length, canonical_path
                   FROM official_document_blobs
                   WHERE content_hash IN ("""
                + placeholders
                + ") ORDER BY content_hash",
                tuple(required),
            ).fetchall()
        by_hash = {str(row["content_hash"]): dict(row) for row in rows}
        return [
            by_hash.get(
                content_hash,
                {
                    "content_hash": content_hash,
                    "content_length": 0,
                    "canonical_path": "",
                    "metadata_missing": True,
                },
            )
            for content_hash in required
        ]

    def _required_restore_hashes(self) -> set[str]:
        with self.repository.connection() as conn:
            rows = conn.execute(
                """SELECT content_hash FROM effective_annual_reports
                   WHERE content_hash IS NOT NULL
                   UNION
                   SELECT blob_hash FROM official_asset_retention_pins
                   WHERE released_at IS NULL
                   UNION
                   SELECT replacement_blob_hash FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                     AND replacement_blob_hash IS NOT NULL
                   UNION
                   SELECT blob_hash FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                   UNION
                   SELECT content_hash FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                   UNION
                   SELECT replacement_content_hash
                   FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                     AND replacement_content_hash IS NOT NULL"""
            ).fetchall()
        return {str(row[0]) for row in rows if row[0]}

    def _create_catalog_snapshot(
        self,
        destination: Path,
        *,
        expected_identity: MountIdentity | None = None,
    ) -> tuple[Path, str]:
        identity = expected_identity
        if identity is None:
            identity = self.mount_validator(self.config)
            if identity is None:
                raise RuntimeError("backup mount identity is unavailable")
        self._assert_mount_identity(identity)
        target_dir = destination / "catalog"
        target_dir.mkdir(parents=True, exist_ok=True)
        self._assert_mount_identity(identity)
        temporary = target_dir / f".catalog.{uuid.uuid4().hex}.part"
        try:
            source = sqlite3.connect(self.repository.db_path)
            target = sqlite3.connect(temporary)
            try:
                source.backup(target)
                target.commit()
            finally:
                target.close()
                source.close()
            digest, length = _hash_file(temporary)
            watermark = stable_id("catalog-snapshot", digest, length)
            snapshot = target_dir / f"{watermark}.sqlite"
            if snapshot.exists():
                existing_digest, existing_length = _hash_file(snapshot)
                if (existing_digest, existing_length) != (digest, length):
                    raise RuntimeError("catalog snapshot target identity mismatch")
                self._cleanup_temporary(temporary, identity)
            else:
                self._assert_mount_identity(identity)
                os.replace(temporary, snapshot)
                _fsync_directory(snapshot.parent)
            return snapshot, watermark
        finally:
            self._cleanup_temporary(temporary, identity)

    def _backup_blob(
        self,
        destination: Path,
        blob: dict[str, Any],
        *,
        identity: MountIdentity,
        operator_repair: bool,
    ) -> BackupItemResult:
        content_hash = str(blob["content_hash"])
        content_length = int(blob["content_length"])
        if blob.get("metadata_missing"):
            return BackupItemResult(
                content_hash,
                content_length,
                "source_invalid",
                None,
                error_code="blob_metadata_missing",
            )
        source = Path(str(blob["canonical_path"]))
        target = self._target_path(destination, content_hash)
        if target.exists():
            target_status = _validate_pdf_file(
                target,
                expected_hash=content_hash,
                expected_length=content_length,
            )
            if target_status == "valid":
                return BackupItemResult(
                    content_hash, content_length, "verified", str(target)
                )
            if not operator_repair:
                return BackupItemResult(
                    content_hash,
                    content_length,
                    "target_mismatch",
                    str(target),
                    error_code=f"target_{target_status}",
                )
        source_status = _validate_pdf_file(
            source,
            expected_hash=content_hash,
            expected_length=content_length,
        )
        if source_status != "valid":
            return BackupItemResult(
                content_hash,
                content_length,
                "source_invalid",
                None,
                error_code=f"source_{source_status}",
            )
        if target.exists():
            self._assert_mount_identity(identity)
            quarantine = destination / "quarantine" / (
                f"{content_hash}.{stable_id('repair', utc_now_iso())}.invalid"
            )
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            original_evidence = _file_identity_evidence(target)
            self.repository.append_storage_artifact_audit(
                {
                    "artifact_type": "quarantine",
                    "managed_path": str(target),
                    "status": "planned",
                    "actor": "announcement_asset_backup_operator_repair",
                    "content_hash": content_hash,
                    "actual_bytes": original_evidence.get("content_length"),
                    "reason": "backup_target_identity_mismatch",
                    "quarantine_path": str(quarantine),
                    "original_evidence": original_evidence,
                }
            )
            self._assert_mount_identity(identity)
            os.replace(target, quarantine)
            _fsync_directory(quarantine.parent)
            self.repository.append_storage_artifact_audit(
                {
                    "artifact_type": "quarantine",
                    "managed_path": str(target),
                    "status": "deleted",
                    "actor": "announcement_asset_backup_operator_repair",
                    "content_hash": content_hash,
                    "actual_bytes": original_evidence.get("content_length"),
                    "reason": "backup_target_quarantined_for_repair",
                    "quarantine_path": str(quarantine),
                    "original_evidence": original_evidence,
                }
            )

        recovered = self._recover_complete_part(
            target,
            content_hash,
            content_length,
            expected_identity=identity,
        )
        if not recovered:
            self._assert_mount_identity(identity)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.parent / f".{content_hash}.{uuid.uuid4().hex}.part"
            try:
                with source.open("rb") as input_handle, temporary.open("xb") as output:
                    shutil.copyfileobj(input_handle, output, length=1024 * 1024)
                    output.flush()
                    os.fsync(output.fileno())
                status = _validate_pdf_file(
                    temporary,
                    expected_hash=content_hash,
                    expected_length=content_length,
                )
                if status != "valid":
                    raise RuntimeError(f"backup temporary validation failed: {status}")
                self._assert_mount_identity(identity)
                self._backup_crash_boundary("before_blob_publication", content_hash)
                os.replace(temporary, target)
                _fsync_directory(target.parent)
                self._backup_crash_boundary("after_blob_publication", content_hash)
            finally:
                self._cleanup_temporary(temporary, identity)
        final_status = _validate_pdf_file(
            target,
            expected_hash=content_hash,
            expected_length=content_length,
        )
        if final_status != "valid":
            return BackupItemResult(
                content_hash,
                content_length,
                "failed",
                str(target),
                error_code=f"published_{final_status}",
            )
        return BackupItemResult(
            content_hash,
            content_length,
            "verified",
            str(target),
            copied=not recovered,
        )

    @staticmethod
    def _target_path(destination: Path, content_hash: str) -> Path:
        _validate_hash(content_hash)
        return destination / "blobs" / content_hash[:2] / f"{content_hash}.pdf"

    def _recover_complete_part(
        self,
        target: Path,
        content_hash: str,
        content_length: int,
        *,
        expected_identity: MountIdentity,
    ) -> bool:
        if target.exists() or not target.parent.exists():
            return False
        for part in sorted(target.parent.glob(f".{content_hash}.*.part")):
            if _validate_pdf_file(
                part,
                expected_hash=content_hash,
                expected_length=content_length,
            ) == "valid":
                self._assert_mount_identity(expected_identity)
                os.replace(part, target)
                _fsync_directory(target.parent)
                return True
            self._assert_mount_identity(expected_identity)
            part.unlink(missing_ok=True)
        return False

    def _preflight_capacity(self, destination: Path, planned_bytes: int) -> None:
        usage = shutil.disk_usage(destination)
        projected_free = usage.free - int(planned_bytes)
        projected_utilization = (usage.used + int(planned_bytes)) / max(usage.total, 1)
        backup = self.config.backup
        if projected_free < backup.free_space_reserve_bytes:
            raise RuntimeError("backup hard free-space reserve would be violated")
        if projected_utilization >= backup.hard_stop_utilization:
            raise RuntimeError("backup hard utilization threshold would be violated")

    def _planned_blob_copy_bytes(
        self,
        destination: Path,
        blobs: list[dict[str, Any]],
        *,
        operator_repair: bool,
    ) -> int:
        planned = 0
        for blob in blobs:
            content_hash = str(blob["content_hash"])
            content_length = int(blob["content_length"])
            target = self._target_path(destination, content_hash)
            if not target.exists():
                planned += content_length
                continue
            if operator_repair and _validate_pdf_file(
                target,
                expected_hash=content_hash,
                expected_length=content_length,
            ) != "valid":
                planned += content_length
        return planned

    def _catalog_snapshot_planned_bytes(self) -> int:
        with self.repository.connection() as conn:
            page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
            page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        return max(1, page_count * page_size)

    def _recovery_metadata_planned_bytes(
        self,
        blobs: list[dict[str, Any]],
    ) -> int:
        entries = self.repository.list_backup_recovery_journal_entries()
        journal_bytes = sum(
            len(canonical_json(_recovery_journal_entry_mapping(entry)).encode("utf-8"))
            for entry in entries
        )
        # Fixed keys plus one content-addressed manifest entry per required blob.
        manifest_bytes = 2048 + sum(
            256 + len(str(blob.get("canonical_path") or "")) for blob in blobs
        )
        return max(1, journal_bytes + manifest_bytes)

    def _atomic_write(
        self,
        path: Path,
        content: bytes,
        *,
        expected_identity: MountIdentity,
    ) -> None:
        self._assert_mount_identity(expected_identity)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() == content:
                return
            raise RuntimeError("content-addressed artifact identity mismatch")
        temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.part"
        try:
            with temporary.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            self._assert_mount_identity(expected_identity)
            os.replace(temporary, path)
            _fsync_directory(path.parent)
        finally:
            self._cleanup_temporary(temporary, expected_identity)

    def _assert_mount_identity(self, expected: MountIdentity) -> None:
        current = self.mount_validator(self.config)
        if current is None or current.filesystem_key != expected.filesystem_key:
            raise RuntimeError("backup mount identity changed during mutation")

    def _backup_crash_boundary(self, boundary: str, identity: str) -> None:
        if self.backup_crash_hook is not None:
            self.backup_crash_hook(boundary, identity)

    def _cleanup_temporary(
        self,
        path: Path,
        expected_identity: MountIdentity,
    ) -> None:
        """Never unlink a same-named fallback path after a remount race."""

        try:
            self._assert_mount_identity(expected_identity)
        except RuntimeError:
            return
        path.unlink(missing_ok=True)


def _validate_hash(value: str) -> None:
    validate_path_segment(value, kind="sha256", field_name="content hash")


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            length += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), length


def _file_identity_evidence(path: Path) -> dict[str, Any]:
    """Capture the mismatched target before an authorized repair moves it."""

    try:
        stat = path.stat()
        digest, length = _hash_file(path)
    except OSError as exc:
        return {
            "path": str(path),
            "read_error": f"{type(exc).__name__}:{exc}",
        }
    return {
        "path": str(path),
        "content_hash": digest,
        "content_length": length,
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _snapshot_recovery_journal_tail(
    snapshot: Path,
) -> tuple[int, str | None, str | None]:
    with sqlite3.connect(f"file:{snapshot}?mode=ro", uri=True) as conn:
        return _recovery_journal_tail_from_connection(conn)


def _recovery_journal_tail_from_connection(
    conn: sqlite3.Connection,
) -> tuple[int, str | None, str | None]:
    row = conn.execute(
        """SELECT journal_sequence, coverage_watermark,
                  source_catalog_generation
           FROM official_asset_backup_recovery_journal
           ORDER BY journal_sequence DESC LIMIT 1"""
    ).fetchone()
    if row is None:
        return 0, None, None
    return int(row[0]), str(row[1]), str(row[2])


def _required_blob_hashes_from_connection(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """SELECT content_hash FROM effective_annual_reports
           WHERE content_hash IS NOT NULL
           UNION
           SELECT blob_hash FROM official_asset_retention_pins
           WHERE released_at IS NULL
           UNION
           SELECT blob_hash FROM official_asset_deletion_intents
           WHERE status IN ('planned', 'deleting', 'failed')
           UNION
           SELECT replacement_blob_hash FROM official_asset_deletion_intents
           WHERE status IN ('planned', 'deleting', 'failed')
             AND replacement_blob_hash IS NOT NULL
           UNION
           SELECT content_hash FROM official_asset_recovery_manifest
           WHERE active_indefinitely=1
           UNION
           SELECT replacement_content_hash
           FROM official_asset_recovery_manifest
           WHERE active_indefinitely=1
             AND replacement_content_hash IS NOT NULL"""
    ).fetchall()
    return {str(row[0]) for row in rows if row[0]}


def _build_catalog_changeset(
    baseline: sqlite3.Connection,
    terminal: sqlite3.Connection,
    *,
    tables: tuple[str, ...],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for table in tables:
        baseline_rows, primary_key = _table_rows_by_primary_key(baseline, table)
        terminal_rows, terminal_primary_key = _table_rows_by_primary_key(
            terminal,
            table,
        )
        if terminal_primary_key != primary_key:
            raise RuntimeError(f"recovery journal schema changed for {table}")
        upserts: list[dict[str, Any]] = []
        deletes: list[dict[str, Any]] = []
        for identity in sorted(
            set(baseline_rows).union(terminal_rows),
            key=lambda value: json.dumps(
                list(value),
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ),
        ):
            before = baseline_rows.get(identity)
            after = terminal_rows.get(identity)
            if before == after:
                continue
            key = dict(zip(primary_key, identity, strict=True))
            if after is None:
                deletes.append(
                    {
                        "key": key,
                        "before_hash": _catalog_row_hash(before),
                    }
                )
            else:
                upserts.append(
                    {
                        "key": key,
                        "before_hash": (
                            None if before is None else _catalog_row_hash(before)
                        ),
                        "row": after,
                    }
                )
        if upserts or deletes:
            changes.append(
                {
                    "table": table,
                    "primary_key": list(primary_key),
                    "upserts": upserts,
                    "deletes": deletes,
                }
            )
    return changes


def _table_rows_by_primary_key(
    conn: sqlite3.Connection,
    table: str,
) -> tuple[dict[tuple[Any, ...], dict[str, Any]], tuple[str, ...]]:
    if table not in RECOVERY_JOURNAL_CAPTURE_TABLES:
        raise ValueError(f"table is outside recovery-journal scope: {table}")
    columns = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    primary_key = tuple(
        str(row[1])
        for row in sorted(columns, key=lambda value: int(value[5]))
        if int(row[5]) > 0
    )
    if not primary_key:
        raise RuntimeError(f"recovery journal table has no primary key: {table}")
    names = tuple(str(row[1]) for row in columns)
    rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for raw in conn.execute(f'SELECT * FROM "{table}"').fetchall():
        row = {name: raw[name] for name in names}
        identity = tuple(row[name] for name in primary_key)
        rows[identity] = row
    return rows, primary_key


def _catalog_row_hash(row: dict[str, Any] | None) -> str:
    if row is None:
        raise ValueError("cannot hash a missing catalog row")
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()


def _recovery_journal_entry_mapping(
    entry: OfficialAssetBackupRecoveryJournalEntry,
) -> dict[str, Any]:
    return {
        "schema_version": entry.schema_version,
        "journal_entry_id": entry.journal_entry_id,
        "journal_sequence": int(entry.journal_sequence),
        "increment_kind": entry.increment_kind,
        "increment_identity": entry.increment_identity,
        "source_catalog_generation": entry.source_catalog_generation,
        "predecessor_watermark": entry.predecessor_watermark,
        "coverage_watermark": entry.coverage_watermark,
        "integrity_hash": entry.integrity_hash,
        "payload": dict(entry.payload),
        "created_at": entry.created_at,
        "created_by": entry.created_by,
    }


def _validate_pdf_file(
    path: Path,
    *,
    expected_hash: str,
    expected_length: int,
) -> str:
    if not path.is_file():
        return "missing"
    try:
        if path.stat().st_size != int(expected_length):
            return "size_mismatch"
        with path.open("rb") as handle:
            if handle.read(5) != b"%PDF-":
                return "not_pdf"
        digest, _ = _hash_file(path)
    except OSError:
        return "unreadable"
    return "valid" if digest == expected_hash else "hash_mismatch"


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
