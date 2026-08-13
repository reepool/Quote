"""Restore verification and legacy-path reconstruction for announcement assets."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .backup import (
    RECOVERY_JOURNAL_CAPTURE_TABLES,
    RECOVERY_JOURNAL_CHANGESET_SCHEMA_VERSION,
    AnnouncementAssetBackupService,
    RecoveryJournalReadiness,
    RestoreReadiness,
)
from .config import AnnouncementAssetConfig
from .models import (
    OfficialAssetBackupRecoveryJournalEntry,
    canonical_json,
    stable_id,
)
from .repository import AnnouncementAssetRepository


@dataclass(frozen=True)
class LegacyPathRestoreItem:
    legacy_path: str
    content_hash: str
    status: str
    reconstructed_path: str | None = None
    error_code: str | None = None


@dataclass(frozen=True)
class LegacyPathRestoreReport:
    status: str
    dry_run: bool
    planned: int
    reconstructed: int
    failed: int
    items: tuple[LegacyPathRestoreItem, ...]


@dataclass(frozen=True)
class PairedRestoreReadiness:
    """Fail-closed projection for a true data-loss paired restore."""

    ready: bool
    blob_readiness: RestoreReadiness
    journal_readiness: RecoveryJournalReadiness
    catalog_errors: tuple[str, ...] = ()
    gate_errors: tuple[str, ...] = ()
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BackupPairReadiness:
    """Verification result for one content-addressed catalog/file pair."""

    ready: bool
    required_blobs: int
    verified_blobs: int
    catalog_snapshot_watermark: str | None = None
    file_manifest_watermark: str | None = None
    missing_hashes: tuple[str, ...] = ()
    invalid_hashes: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    catalog_errors: tuple[str, ...] = ()
    journal_snapshot_sequence: int = 0
    journal_snapshot_coverage_watermark: str | None = None
    journal_terminal_sequence: int = 0
    journal_terminal_coverage_watermark: str | None = None
    journal_source_catalog_generation: str | None = None
    journal_entries: tuple[OfficialAssetBackupRecoveryJournalEntry, ...] = ()


@dataclass(frozen=True)
class RecoveryJournalReplayResult:
    """Outcome of applying an authenticated journal to an isolated catalog."""

    ready: bool
    applied_entries: int
    terminal_sequence: int
    terminal_coverage_watermark: str | None
    errors: tuple[str, ...] = ()


class AnnouncementAssetRestoreService:
    """Gate restore enablement and reconstruct verified legacy projections."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        backup_service: AnnouncementAssetBackupService,
    ) -> None:
        self.repository = repository
        self.config = config
        self.backup_service = backup_service

    def verify_enablement(self) -> RestoreReadiness:
        """Require the complete current/pinned/pending-replacement blob set."""
        return self.backup_service.verify_restore_readiness()

    def verify_paired_restore(
        self,
        *,
        writes_frozen: bool,
        declared_snapshot_rpo_seconds: int | None,
        application_version: str,
        expected_application_version: str,
        snapshot_sequence: int,
        snapshot_coverage_watermark: str | None,
        terminal_sequence: int,
        terminal_coverage_watermark: str | None,
        source_catalog_generation: str,
        catalog_snapshot_path: str | Path | None = None,
        file_manifest_path: str | Path | None = None,
        journal_entries: list[OfficialAssetBackupRecoveryJournalEntry] | None = None,
    ) -> PairedRestoreReadiness:
        """Verify every non-destructive gate before a live catalog overwrite.

        This method intentionally performs no replay and no catalog or file
        mutation.  The caller must supply the independently persisted terminal
        journal watermark (or an equal snapshot/terminal watermark captured
        under a write freeze) so a valid prefix cannot pass as a complete tail.
        """

        gate_errors: list[str] = []
        if not writes_frozen:
            gate_errors.append("write_freeze_required")
        if (
            declared_snapshot_rpo_seconds is None
            or int(declared_snapshot_rpo_seconds) < 0
        ):
            gate_errors.append("snapshot_rpo_required")
        if not str(application_version or "").strip():
            gate_errors.append("application_version_missing")
        elif application_version != expected_application_version:
            gate_errors.append("application_version_mismatch")

        pair = self.verify_backup_pair(
            catalog_snapshot_path=catalog_snapshot_path,
            file_manifest_path=file_manifest_path,
        )
        if not pair.ready:
            gate_errors.append("backup_pair_invalid")
        declared_journal = (
            int(snapshot_sequence),
            snapshot_coverage_watermark,
            int(terminal_sequence),
            terminal_coverage_watermark,
            str(source_catalog_generation),
        )
        paired_journal = (
            pair.journal_snapshot_sequence,
            pair.journal_snapshot_coverage_watermark,
            pair.journal_terminal_sequence,
            pair.journal_terminal_coverage_watermark,
            str(pair.journal_source_catalog_generation or ""),
        )
        if declared_journal != paired_journal:
            gate_errors.append("recovery_journal_declaration_mismatch")
        paired_interval = [
            entry
            for entry in pair.journal_entries
            if pair.journal_snapshot_sequence < int(entry.journal_sequence)
            <= pair.journal_terminal_sequence
        ]
        if journal_entries is not None and list(journal_entries) != paired_interval:
            gate_errors.append("recovery_journal_entries_mismatch")
        blobs = RestoreReadiness(
            ready=pair.ready and not pair.missing_hashes and not pair.invalid_hashes,
            required_blobs=pair.required_blobs,
            verified_blobs=pair.verified_blobs,
            missing_hashes=pair.missing_hashes,
            invalid_hashes=pair.invalid_hashes,
            diagnostics={
                "verification": "full_independent_backup_required_set",
                "sampled": False,
                "catalog_snapshot_watermark": pair.catalog_snapshot_watermark,
                "file_manifest_watermark": pair.file_manifest_watermark,
                "pair_errors": pair.errors,
            },
        )
        journal = self.backup_service.verify_recovery_journal(
            snapshot_sequence=pair.journal_snapshot_sequence,
            snapshot_coverage_watermark=pair.journal_snapshot_coverage_watermark,
            terminal_sequence=pair.journal_terminal_sequence,
            terminal_coverage_watermark=pair.journal_terminal_coverage_watermark,
            source_catalog_generation=str(
                pair.journal_source_catalog_generation or ""
            ),
            entries=paired_interval,
        )
        catalog_errors = pair.catalog_errors
        return PairedRestoreReadiness(
            ready=(
                not gate_errors
                and blobs.ready
                and journal.ready
                and not catalog_errors
            ),
            blob_readiness=blobs,
            journal_readiness=journal,
            catalog_errors=catalog_errors,
            gate_errors=tuple(gate_errors),
            diagnostics={
                "verification": "full_paired_restore",
                "sampled": False,
                "declared_snapshot_rpo_seconds": declared_snapshot_rpo_seconds,
                "source_catalog_generation": source_catalog_generation,
                "catalog_snapshot_watermark": pair.catalog_snapshot_watermark,
                "file_manifest_watermark": pair.file_manifest_watermark,
            },
        )

    def verify_backup_pair(
        self,
        *,
        catalog_snapshot_path: str | Path | None,
        file_manifest_path: str | Path | None,
    ) -> BackupPairReadiness:
        """Verify one immutable catalog snapshot and its exact file manifest."""

        errors: list[str] = []
        missing: list[str] = []
        invalid: list[str] = []
        journal_metadata: dict[str, Any] = {}
        journal_entries: list[OfficialAssetBackupRecoveryJournalEntry] = []
        destination = self.config.backup.destination_root
        if destination is None:
            errors.append("backup_destination_missing")
        if catalog_snapshot_path is None:
            errors.append("catalog_snapshot_missing")
        if file_manifest_path is None:
            errors.append("file_manifest_missing")
        if errors:
            return BackupPairReadiness(
                ready=False,
                required_blobs=0,
                verified_blobs=0,
                errors=tuple(errors),
            )

        assert destination is not None
        snapshot = Path(catalog_snapshot_path).resolve(strict=False)
        manifest_path = Path(file_manifest_path).resolve(strict=False)
        root = destination.resolve(strict=False)
        if not _is_relative_to(snapshot, root / "catalog"):
            errors.append("catalog_snapshot_outside_backup_root")
        if not _is_relative_to(manifest_path, root / "manifests"):
            errors.append("file_manifest_outside_backup_root")
        try:
            snapshot_hash, snapshot_length = _hash_length(snapshot)
            snapshot_watermark = stable_id(
                "catalog-snapshot", snapshot_hash, snapshot_length
            )
        except OSError:
            snapshot_watermark = None
            errors.append("catalog_snapshot_unreadable")
        try:
            manifest_bytes = manifest_path.read_bytes()
            manifest_watermark = hashlib.sha256(manifest_bytes).hexdigest()
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (OSError, UnicodeError, ValueError, TypeError):
            manifest_watermark = None
            manifest = {}
            errors.append("file_manifest_unreadable")
        if not isinstance(manifest, dict):
            manifest = {}
            errors.append("file_manifest_root_invalid")

        if snapshot_watermark is not None and snapshot.stem != snapshot_watermark:
            errors.append("catalog_snapshot_identity_mismatch")
        if manifest_watermark is not None and manifest_path.stem != manifest_watermark:
            errors.append("file_manifest_identity_mismatch")
        if manifest.get("schema_version") != "official_asset_backup_manifest.v1":
            errors.append("file_manifest_schema_mismatch")
        if manifest.get("catalog_snapshot_watermark") != snapshot_watermark:
            errors.append("catalog_file_pair_watermark_mismatch")
        if manifest.get("failure_domain") != self.config.backup.expected_failure_domain:
            errors.append("backup_failure_domain_mismatch")

        journal_ref = manifest.get("recovery_journal")
        if not isinstance(journal_ref, dict):
            errors.append("recovery_journal_reference_missing")
        else:
            journal_path = Path(str(journal_ref.get("path") or "")).resolve(
                strict=False
            )
            if not _is_relative_to(journal_path, root / "recovery-journal"):
                errors.append("recovery_journal_outside_backup_root")
            else:
                try:
                    journal_metadata, journal_entries = (
                        self.backup_service.load_recovery_journal_bundle(
                            journal_path,
                            expected_watermark=str(
                                journal_ref.get("watermark") or ""
                            ),
                        )
                    )
                    if journal_metadata != {
                        key: journal_ref.get(key)
                        for key in journal_metadata
                    }:
                        errors.append("recovery_journal_manifest_binding_mismatch")
                    if (
                        journal_metadata.get("catalog_snapshot_watermark")
                        != snapshot_watermark
                    ):
                        errors.append("recovery_journal_snapshot_binding_mismatch")
                    snapshot_sequence = int(
                        journal_metadata.get("snapshot_sequence") or 0
                    )
                    terminal_sequence = int(
                        journal_metadata.get("terminal_sequence") or 0
                    )
                    interval = [
                        entry
                        for entry in journal_entries
                        if snapshot_sequence < int(entry.journal_sequence)
                        <= terminal_sequence
                    ]
                    journal_readiness = self.backup_service.verify_recovery_journal(
                        snapshot_sequence=snapshot_sequence,
                        snapshot_coverage_watermark=journal_metadata.get(
                            "snapshot_coverage_watermark"
                        ),
                        terminal_sequence=terminal_sequence,
                        terminal_coverage_watermark=journal_metadata.get(
                            "terminal_coverage_watermark"
                        ),
                        source_catalog_generation=str(
                            journal_metadata.get("source_catalog_generation") or ""
                        ),
                        entries=interval,
                    )
                    if not journal_readiness.ready:
                        errors.extend(
                            "recovery_journal_invalid:" + error
                            for error in journal_readiness.errors
                        )
                except (OSError, TypeError, ValueError):
                    errors.append("recovery_journal_artifact_invalid")

        try:
            identity = self.backup_service.mount_validator(self.config)
        except (OSError, RuntimeError, TypeError, ValueError):
            identity = None
        if identity is None:
            errors.append("backup_mount_identity_unavailable")
        elif manifest.get("destination_identity") != identity.filesystem_key:
            errors.append("backup_mount_identity_mismatch")

        required: dict[str, int] = {}
        catalog_errors: tuple[str, ...] = ()
        if snapshot_watermark is not None:
            try:
                required = _required_blob_lengths(snapshot)
                snapshot_repository = AnnouncementAssetRepository(snapshot)
                catalog_errors = self.verify_catalog_recovery_invariants(
                    repository=snapshot_repository
                )
            except (OSError, sqlite3.DatabaseError, KeyError, TypeError, ValueError):
                errors.append("catalog_snapshot_invalid")

        manifest_blobs: dict[str, dict[str, Any]] = {}
        raw_blobs = manifest.get("blobs", ())
        if not isinstance(raw_blobs, list):
            errors.append("file_manifest_blob_set_invalid")
            raw_blobs = []
        for item in raw_blobs:
            if not isinstance(item, dict) or not str(item.get("content_hash") or ""):
                errors.append("file_manifest_blob_entry_invalid")
                continue
            content_hash = str(item["content_hash"])
            if content_hash in manifest_blobs:
                errors.append(f"file_manifest_blob_duplicate:{content_hash}")
            manifest_blobs[content_hash] = item

        for content_hash, content_length in sorted(required.items()):
            item = manifest_blobs.get(content_hash)
            if item is None:
                missing.append(content_hash)
                continue
            expected_path = self.backup_service._target_path(root, content_hash)
            actual_path = Path(str(item.get("backup_path") or "")).resolve(strict=False)
            try:
                listed_length = int(item.get("content_length", -1))
            except (TypeError, ValueError):
                listed_length = -1
            if (
                actual_path != expected_path.resolve(strict=False)
                or listed_length != content_length
                or not _matches_pdf(actual_path, content_hash, content_length)
            ):
                invalid.append(content_hash)
        extras = sorted(set(manifest_blobs).difference(required))
        if extras:
            errors.append("file_manifest_contains_nonrequired_blobs:" + ",".join(extras))

        verified = len(required) - len(missing) - len(invalid)
        return BackupPairReadiness(
            ready=(
                not errors
                and not missing
                and not invalid
                and not catalog_errors
            ),
            required_blobs=len(required),
            verified_blobs=verified,
            catalog_snapshot_watermark=snapshot_watermark,
            file_manifest_watermark=manifest_watermark,
            missing_hashes=tuple(missing),
            invalid_hashes=tuple(invalid),
            errors=tuple(dict.fromkeys(errors)),
            catalog_errors=catalog_errors,
            journal_snapshot_sequence=int(
                journal_metadata.get("snapshot_sequence") or 0
            ),
            journal_snapshot_coverage_watermark=journal_metadata.get(
                "snapshot_coverage_watermark"
            ),
            journal_terminal_sequence=int(
                journal_metadata.get("terminal_sequence") or 0
            ),
            journal_terminal_coverage_watermark=journal_metadata.get(
                "terminal_coverage_watermark"
            ),
            journal_source_catalog_generation=journal_metadata.get(
                "source_catalog_generation"
            ),
            journal_entries=tuple(journal_entries),
        )

    def replay_recovery_journal(
        self,
        *,
        staged_catalog_path: str | Path,
        snapshot_sequence: int,
        snapshot_coverage_watermark: str | None,
        terminal_sequence: int,
        terminal_coverage_watermark: str | None,
        source_catalog_generation: str,
        entries: list[OfficialAssetBackupRecoveryJournalEntry],
    ) -> RecoveryJournalReplayResult:
        """Replay a verified changeset chain into an isolated catalog copy.

        The live repository path is categorically rejected.  Each row's
        pre-image hash is checked before mutation, journal entries are appended
        in the same SQLite transaction as their changes, and foreign-key plus
        catalog recovery invariants must pass before the staged copy is ready.
        """

        staged = Path(staged_catalog_path).resolve(strict=False)
        live = self.repository.db_path.resolve(strict=False)
        if staged == live:
            raise PermissionError("recovery journal replay requires an isolated catalog")
        if not staged.is_file():
            raise FileNotFoundError(staged)
        readiness = self.backup_service.verify_recovery_journal(
            snapshot_sequence=snapshot_sequence,
            snapshot_coverage_watermark=snapshot_coverage_watermark,
            terminal_sequence=terminal_sequence,
            terminal_coverage_watermark=terminal_coverage_watermark,
            source_catalog_generation=source_catalog_generation,
            entries=entries,
        )
        if not readiness.ready:
            return RecoveryJournalReplayResult(
                ready=False,
                applied_entries=0,
                terminal_sequence=int(snapshot_sequence),
                terminal_coverage_watermark=snapshot_coverage_watermark,
                errors=readiness.errors,
            )

        staged_repository = AnnouncementAssetRepository(staged)
        applied = 0
        try:
            with staged_repository.transaction() as conn:
                for entry in entries:
                    self._apply_recovery_changeset(conn, entry)
                    staged_repository.append_backup_recovery_journal_entry_in_transaction(
                        conn,
                        entry,
                    )
                    applied += 1
                foreign_key_errors = conn.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall()
                if foreign_key_errors:
                    raise RuntimeError("replayed catalog has foreign-key violations")
        except (KeyError, sqlite3.DatabaseError, TypeError, ValueError, RuntimeError) as exc:
            return RecoveryJournalReplayResult(
                ready=False,
                applied_entries=0,
                terminal_sequence=int(snapshot_sequence),
                terminal_coverage_watermark=snapshot_coverage_watermark,
                errors=(f"recovery_journal_replay_failed:{type(exc).__name__}:{exc}",),
            )

        catalog_errors = self.verify_catalog_recovery_invariants(
            repository=staged_repository
        )
        return RecoveryJournalReplayResult(
            ready=not catalog_errors,
            applied_entries=applied,
            terminal_sequence=int(terminal_sequence),
            terminal_coverage_watermark=terminal_coverage_watermark,
            errors=catalog_errors,
        )

    @staticmethod
    def _apply_recovery_changeset(
        conn: sqlite3.Connection,
        entry: OfficialAssetBackupRecoveryJournalEntry,
    ) -> None:
        if entry.increment_kind != "catalog_changeset":
            raise ValueError(
                f"unsupported recovery increment kind: {entry.increment_kind}"
            )
        payload = entry.payload
        if (
            payload.get("schema_version")
            != RECOVERY_JOURNAL_CHANGESET_SCHEMA_VERSION
        ):
            raise ValueError("unsupported recovery changeset schema")
        raw_tables = payload.get("tables")
        if not isinstance(raw_tables, list):
            raise TypeError("recovery changeset tables must be a list")
        seen_tables: set[str] = set()
        for change in raw_tables:
            if not isinstance(change, dict):
                raise TypeError("recovery changeset table entry must be an object")
            table = str(change.get("table") or "")
            if table not in RECOVERY_JOURNAL_CAPTURE_TABLES or table in seen_tables:
                raise ValueError(f"invalid recovery changeset table: {table}")
            seen_tables.add(table)
            actual_columns = conn.execute(
                f'PRAGMA table_info("{table}")'
            ).fetchall()
            names = tuple(str(row[1]) for row in actual_columns)
            actual_primary_key = tuple(
                str(row[1])
                for row in sorted(actual_columns, key=lambda value: int(value[5]))
                if int(row[5]) > 0
            )
            declared_primary_key = tuple(change.get("primary_key") or ())
            if declared_primary_key != actual_primary_key:
                raise ValueError(f"recovery primary key mismatch: {table}")
            for mutation in change.get("upserts") or ():
                _apply_catalog_upsert(
                    conn,
                    table=table,
                    columns=names,
                    primary_key=actual_primary_key,
                    mutation=mutation,
                )
            for mutation in change.get("deletes") or ():
                _apply_catalog_delete(
                    conn,
                    table=table,
                    primary_key=actual_primary_key,
                    mutation=mutation,
                )

    def verify_catalog_recovery_invariants(
        self,
        *,
        repository: AnnouncementAssetRepository | None = None,
    ) -> tuple[str, ...]:
        """Reconcile authoritative restore projections without changing them."""

        catalog = repository or self.repository
        errors: list[str] = []
        with catalog.connection() as conn:
            for row in conn.execute("PRAGMA foreign_key_check").fetchall():
                errors.append(f"foreign_key_violation:{row[0]}:{row[1]}")

            effective_rows = conn.execute(
                """SELECT effective.asset_id
                   FROM effective_annual_reports effective
                   LEFT JOIN official_attachment_versions version
                     ON version.version_id=effective.version_id
                   LEFT JOIN official_document_blobs blob
                     ON blob.content_hash=effective.content_hash
                   WHERE version.version_id IS NULL
                      OR (effective.content_hash IS NOT NULL AND blob.content_hash IS NULL)
                      OR COALESCE(version.content_hash, '')
                         <> COALESCE(effective.content_hash, '')"""
            ).fetchall()
            errors.extend(
                f"effective_projection_dangling:{row[0]}" for row in effective_rows
            )

            decision_rows = conn.execute(
                """SELECT decision_sequence, decision_id, instrument_id, fiscal_year,
                          decision_kind, decision_state, predecessor_asset_id,
                          predecessor_content_hash, replacement_asset_id,
                          replacement_content_hash, outbox_event_key
                   FROM official_annual_report_decisions
                   ORDER BY instrument_id, fiscal_year, decision_sequence"""
            ).fetchall()
            event_keys = {
                str(row[0])
                for row in conn.execute(
                    "SELECT event_key FROM official_asset_change_events"
                ).fetchall()
            }
            blob_hashes = {
                str(row[0])
                for row in conn.execute(
                    "SELECT content_hash FROM official_document_blobs"
                ).fetchall()
            }
            effective_by_scope = {
                (str(row[0]), int(row[1])): (str(row[2]), row[3])
                for row in conn.execute(
                    """SELECT instrument_id, fiscal_year, asset_id, content_hash
                       FROM effective_annual_reports"""
                ).fetchall()
            }
            latest_by_scope: dict[tuple[str, int], Any] = {}
            previous_replacement: dict[tuple[str, int], str | None] = {}
            for row in decision_rows:
                sequence = int(row[0])
                decision_id = str(row[1])
                scope = (str(row[2]), int(row[3]))
                kind = str(row[4])
                predecessor_asset_id = row[6]
                predecessor_hash = row[7]
                replacement_asset_id = row[8]
                replacement_hash = row[9]
                if str(row[10]) not in event_keys:
                    errors.append(f"decision_outbox_missing:{decision_id}")
                for role, content_hash in (
                    ("predecessor", predecessor_hash),
                    ("replacement", replacement_hash),
                ):
                    if content_hash is not None and str(content_hash) not in blob_hashes:
                        errors.append(
                            f"decision_{role}_blob_missing:{decision_id}:{content_hash}"
                        )
                previous = previous_replacement.get(scope)
                if (
                    previous is not None
                    and kind in {"replacement", "projection_update", "withdrawn_without_replacement"}
                    and predecessor_asset_id != previous
                ):
                    errors.append(f"decision_edge_dangling:{decision_id}:{sequence}")
                previous_replacement[scope] = (
                    None
                    if kind == "withdrawn_without_replacement"
                    else None if replacement_asset_id is None else str(replacement_asset_id)
                )
                latest_by_scope[scope] = row

            for scope, row in latest_by_scope.items():
                kind = str(row[4])
                state = str(row[5])
                current = effective_by_scope.get(scope)
                if kind == "withdrawn_without_replacement":
                    if current is not None:
                        errors.append(
                            f"withdrawn_scope_has_current:{scope[0]}:{scope[1]}"
                        )
                elif state == "current" and (
                    current is None
                    or current != (str(row[8]), row[9])
                ):
                    errors.append(
                        f"current_projection_mismatch:{scope[0]}:{scope[1]}"
                    )

            deletion_rows = conn.execute(
                """SELECT deletion_id, blob_hash, replacement_blob_hash,
                          decision_id, outbox_event_key, recovery_pair_id,
                          recovery_manifest_id, predecessor_asset_id,
                          replacement_asset_id, reason
                   FROM official_asset_deletion_intents"""
            ).fetchall()
            decision_ids = {str(row[1]) for row in decision_rows}
            decision_by_id = {str(row[1]): row for row in decision_rows}
            manifest_rows = {
                str(row[0]): {
                    "pair_id": str(row[1]),
                    "kind": str(row[2]),
                    "content_hash": str(row[3]),
                    "replacement_content_hash": row[4],
                    "predecessor_asset_id": row[5],
                    "replacement_asset_id": row[6],
                    "file_manifest_watermark": str(row[7]),
                }
                for row in conn.execute(
                    """SELECT recovery_id, recovery_pair_id, manifest_kind,
                              content_hash, replacement_content_hash,
                              predecessor_asset_id, replacement_asset_id,
                              file_manifest_watermark
                       FROM official_asset_recovery_manifest"""
                ).fetchall()
            }
            closure_pairs = {
                str(row[0]): {
                    "recovery_id": str(row[1]),
                    "file_manifest_watermark": str(row[2]),
                }
                for row in conn.execute(
                    """SELECT recovery_pair_id, recovery_id,
                              file_manifest_watermark
                       FROM official_asset_recovery_pair_closures"""
                ).fetchall()
            }
            for row in deletion_rows:
                deletion_id = str(row[0])
                if str(row[1]) not in blob_hashes:
                    errors.append(f"deletion_predecessor_blob_missing:{deletion_id}")
                if row[2] is not None and str(row[2]) not in blob_hashes:
                    errors.append(f"deletion_replacement_blob_missing:{deletion_id}")
                if row[3] is not None and str(row[3]) not in decision_ids:
                    errors.append(f"deletion_decision_missing:{deletion_id}")
                if row[4] is not None and str(row[4]) not in event_keys:
                    errors.append(f"deletion_outbox_missing:{deletion_id}")
                decision = (
                    None if row[3] is None else decision_by_id.get(str(row[3]))
                )
                if decision is not None and (
                    decision[6] != row[7]
                    or decision[7] != row[1]
                    or decision[8] != row[8]
                    or decision[9] != row[2]
                    or decision[10] != row[4]
                ):
                    errors.append(f"deletion_decision_edge_mismatch:{deletion_id}")
                if (
                    decision is not None
                    and row[9] == "withdrawn_without_replacement"
                    and (
                        decision[4] != "withdrawn_without_replacement"
                        or row[2] is not None
                        or row[8] is not None
                    )
                ):
                    errors.append(f"withdrawal_deletion_has_replacement:{deletion_id}")
                if row[6] is not None:
                    manifest = manifest_rows.get(str(row[6]))
                    if manifest is None or manifest["pair_id"] != str(row[5]):
                        errors.append(f"deletion_recovery_manifest_mismatch:{deletion_id}")

            active_holds = {
                str(row[0])
                for row in conn.execute(
                    """SELECT blob_hash FROM official_asset_retention_pins
                       WHERE released_at IS NULL AND required_set_hold=1"""
                ).fetchall()
            }
            for recovery_id, manifest in manifest_rows.items():
                pair_id = str(manifest["pair_id"])
                content_hash = str(manifest["content_hash"])
                replacement_hash = manifest["replacement_content_hash"]
                closure = closure_pairs.get(pair_id)
                if (
                    closure is None
                    or closure["recovery_id"] != recovery_id
                    or closure["file_manifest_watermark"]
                    != manifest["file_manifest_watermark"]
                ):
                    errors.append(f"recovery_pair_unclosed:{recovery_id}")
                if content_hash not in blob_hashes:
                    errors.append(f"recovery_predecessor_blob_missing:{recovery_id}")
                if replacement_hash is not None and str(replacement_hash) not in blob_hashes:
                    errors.append(f"recovery_replacement_blob_missing:{recovery_id}")
                if content_hash not in active_holds:
                    errors.append(f"recovery_required_set_hold_missing:{recovery_id}")
                kind = str(manifest["kind"])
                predecessor_asset = manifest["predecessor_asset_id"]
                replacement_asset = manifest["replacement_asset_id"]
                if kind == "withdrawal_tombstone" and (
                    replacement_hash is not None or replacement_asset is not None
                ):
                    errors.append(f"withdrawal_manifest_has_replacement:{recovery_id}")
                elif kind == "correction_predecessor" and (
                    replacement_hash is None or replacement_asset is None
                ):
                    errors.append(f"correction_manifest_missing_replacement:{recovery_id}")
                elif kind == "legacy_path_rollback" and (
                    replacement_hash != content_hash
                    or replacement_asset != predecessor_asset
                ):
                    errors.append(f"legacy_manifest_identity_mismatch:{recovery_id}")

            known_assets = {
                str(row[0]) for row in conn.execute(
                    "SELECT asset_id FROM effective_annual_reports"
                ).fetchall()
            }
            known_assets.update(
                str(value)
                for row in decision_rows
                for value in (row[6], row[8])
                if value is not None
            )
            processing_rows = conn.execute(
                """SELECT processing_id, asset_id, status, derived_identity,
                          metadata_json
                   FROM official_asset_consumer_processing
                   WHERE status='current'"""
            ).fetchall()
            for row in processing_rows:
                processing_id = str(row[0])
                asset_id = str(row[1])
                if asset_id not in known_assets:
                    errors.append(f"consumer_asset_missing:{processing_id}")
                if not str(row[3] or "").strip():
                    errors.append(f"consumer_current_result_missing:{processing_id}")
                try:
                    metadata = json.loads(str(row[4] or "{}"))
                except (TypeError, ValueError):
                    errors.append(f"consumer_metadata_invalid:{processing_id}")
                    continue
                selector_kind = str(
                    metadata.get("selector_kind")
                    or metadata.get("selector_mode")
                    or "default_effective"
                )
                if selector_kind == "default_effective" and asset_id not in {
                    current[0] for current in effective_by_scope.values()
                }:
                    errors.append(f"consumer_default_result_stale:{processing_id}")

        return tuple(dict.fromkeys(errors))

    def reconstruct_legacy_paths(
        self,
        *,
        dry_run: bool = True,
        root_override: str | Path | None = None,
        use_hardlinks: bool = True,
        publish_live: bool = False,
    ) -> LegacyPathRestoreReport:
        """Rebuild legacy aliases from immutable, hash-verified recovery entries.

        An applied drill defaults to an isolated root. Publishing directly back
        to a legacy live path requires a separate explicit flag after that drill.
        Recovery-only predecessor bytes are read from backup when the canonical
        primary path has already been removed; they are never registered as a
        second canonical shared attachment.
        """

        if not dry_run and root_override is None and not publish_live:
            raise PermissionError(
                "live legacy-path publication requires explicit authorization"
            )
        items: list[LegacyPathRestoreItem] = []
        manifests = self.repository.list_recovery_manifest_entries(
            manifest_kind="legacy_path_rollback"
        )
        for manifest in manifests:
            legacy_path = Path(manifest.prior_path)
            target = self._target_path(legacy_path, root_override=root_override)
            content_hash = manifest.content_hash
            blob = self.repository.get_blob(content_hash)
            if blob is None:
                items.append(
                    LegacyPathRestoreItem(
                        str(legacy_path),
                        content_hash,
                        "failed",
                        error_code="blob_metadata_missing",
                    )
                )
                continue
            if not self.backup_service.verify_recovery_manifest_entry(manifest):
                items.append(
                    LegacyPathRestoreItem(
                        str(legacy_path),
                        content_hash,
                        "failed",
                        error_code="recovery_manifest_backup_invalid",
                    )
                )
                continue
            primary = Path(blob.canonical_path)
            backup = Path(manifest.backup_object)
            source = (
                primary
                if _matches(primary, content_hash, blob.content_length)
                else backup
            )
            if not _matches(source, content_hash, blob.content_length):
                items.append(
                    LegacyPathRestoreItem(
                        str(legacy_path),
                        content_hash,
                        "failed",
                        error_code="recovery_blob_integrity_failed",
                    )
                )
                continue
            if dry_run:
                items.append(
                    LegacyPathRestoreItem(
                        str(legacy_path),
                        content_hash,
                        "planned",
                        reconstructed_path=str(target),
                    )
                )
                continue
            try:
                self._publish_projection(
                    source,
                    target,
                    content_hash=content_hash,
                    content_length=blob.content_length,
                    use_hardlink=use_hardlinks,
                )
            except OSError as exc:
                items.append(
                    LegacyPathRestoreItem(
                        str(legacy_path),
                        content_hash,
                        "failed",
                        reconstructed_path=str(target),
                        error_code=f"{type(exc).__name__}:{exc}",
                    )
                )
            else:
                items.append(
                    LegacyPathRestoreItem(
                        str(legacy_path),
                        content_hash,
                        "reconstructed",
                        reconstructed_path=str(target),
                    )
                )
        failed = sum(item.status == "failed" for item in items)
        reconstructed = sum(item.status == "reconstructed" for item in items)
        return LegacyPathRestoreReport(
            status="failed" if failed else "success",
            dry_run=dry_run,
            planned=len(items),
            reconstructed=reconstructed,
            failed=failed,
            items=tuple(items),
        )

    def _target_path(
        self,
        legacy_path: Path,
        *,
        root_override: str | Path | None,
    ) -> Path:
        project = self.config.project_root.resolve(strict=False)
        candidate = legacy_path.resolve(strict=False)
        try:
            relative = candidate.relative_to(project)
        except ValueError as exc:
            raise ValueError("legacy path escapes configured project root") from exc
        if root_override is None:
            target = candidate
        else:
            target = Path(root_override).resolve(strict=False) / relative
        allowed_relative = any(
            _is_relative_to(candidate, root.resolve(strict=False))
            for root in self.config.adoption_roots
        )
        if not allowed_relative:
            raise ValueError("legacy path is outside configured adoption roots")
        return target

    @staticmethod
    def _publish_projection(
        source: Path,
        target: Path,
        *,
        content_hash: str,
        content_length: int,
        use_hardlink: bool,
    ) -> None:
        if target.exists():
            if _matches(target, content_hash, content_length):
                return
            raise FileExistsError("legacy path exists with different content")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.parent / f".{target.name}.{uuid.uuid4().hex}.part"
        try:
            if use_hardlink:
                try:
                    os.link(source, temporary)
                except OSError:
                    shutil.copy2(source, temporary)
            else:
                shutil.copy2(source, temporary)
            if not _matches(temporary, content_hash, content_length):
                raise OSError("reconstructed temporary path failed verification")
            os.replace(temporary, target)
            _fsync_directory(target.parent)
        finally:
            temporary.unlink(missing_ok=True)


def _matches(path: Path, content_hash: str, content_length: int) -> bool:
    if not path.is_file() or path.stat().st_size != int(content_length):
        return False
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return False
    return digest.hexdigest() == content_hash


def _matches_pdf(path: Path, content_hash: str, content_length: int) -> bool:
    try:
        with path.open("rb") as handle:
            if handle.read(5) != b"%PDF-":
                return False
    except OSError:
        return False
    return _matches(path, content_hash, content_length)


def _hash_length(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            length += len(chunk)
    return digest.hexdigest(), length


def _required_blob_lengths(snapshot: Path) -> dict[str, int]:
    """Read the unsampled required set from an immutable catalog snapshot."""

    with sqlite3.connect(f"file:{snapshot}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            """WITH required(content_hash) AS (
                   SELECT content_hash FROM effective_annual_reports
                   WHERE content_hash IS NOT NULL
                   UNION
                   SELECT blob_hash FROM official_asset_retention_pins
                   WHERE released_at IS NULL
                   UNION
                   SELECT blob_hash FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                   UNION
                   SELECT replacement_blob_hash
                   FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                     AND replacement_blob_hash IS NOT NULL
                   UNION
                   SELECT content_hash FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                   UNION
                   SELECT replacement_content_hash
                   FROM official_asset_recovery_manifest
                   WHERE active_indefinitely=1
                     AND replacement_content_hash IS NOT NULL
               )
               SELECT required.content_hash, blob.content_length
               FROM required
               LEFT JOIN official_document_blobs blob
                 ON blob.content_hash=required.content_hash
               ORDER BY required.content_hash"""
        ).fetchall()
    missing_metadata = [str(row[0]) for row in rows if row[1] is None]
    if missing_metadata:
        raise KeyError("required blob metadata missing: " + ",".join(missing_metadata))
    return {str(row[0]): int(row[1]) for row in rows}


def _apply_catalog_upsert(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
    mutation: Any,
) -> None:
    if not isinstance(mutation, dict):
        raise TypeError("recovery upsert must be an object")
    key = mutation.get("key")
    row = mutation.get("row")
    if not isinstance(key, dict) or set(key) != set(primary_key):
        raise ValueError(f"recovery upsert key mismatch: {table}")
    if not isinstance(row, dict) or set(row) != set(columns):
        raise ValueError(f"recovery upsert row shape mismatch: {table}")
    if any(row[name] != key[name] for name in primary_key):
        raise ValueError(f"recovery upsert row identity mismatch: {table}")
    existing = _select_catalog_row(
        conn,
        table=table,
        columns=columns,
        primary_key=primary_key,
        key=key,
    )
    before_hash = mutation.get("before_hash")
    if existing is None:
        if before_hash is not None:
            raise ValueError(f"recovery upsert missing pre-image: {table}")
        quoted_columns = ", ".join(f'"{name}"' for name in columns)
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f'INSERT INTO "{table}"({quoted_columns}) VALUES({placeholders})',
            tuple(row[name] for name in columns),
        )
        return
    if before_hash != _catalog_row_hash(existing):
        raise ValueError(f"recovery upsert pre-image mismatch: {table}")
    mutable_columns = tuple(name for name in columns if name not in primary_key)
    assignments = ", ".join(f'"{name}"=?' for name in mutable_columns)
    where = " AND ".join(f'"{name}"=?' for name in primary_key)
    conn.execute(
        f'UPDATE "{table}" SET {assignments} WHERE {where}',
        tuple(row[name] for name in mutable_columns)
        + tuple(key[name] for name in primary_key),
    )


def _apply_catalog_delete(
    conn: sqlite3.Connection,
    *,
    table: str,
    primary_key: tuple[str, ...],
    mutation: Any,
) -> None:
    if not isinstance(mutation, dict):
        raise TypeError("recovery delete must be an object")
    key = mutation.get("key")
    if not isinstance(key, dict) or set(key) != set(primary_key):
        raise ValueError(f"recovery delete key mismatch: {table}")
    columns = tuple(
        str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')
    )
    existing = _select_catalog_row(
        conn,
        table=table,
        columns=columns,
        primary_key=primary_key,
        key=key,
    )
    if existing is None or mutation.get("before_hash") != _catalog_row_hash(existing):
        raise ValueError(f"recovery delete pre-image mismatch: {table}")
    where = " AND ".join(f'"{name}"=?' for name in primary_key)
    conn.execute(
        f'DELETE FROM "{table}" WHERE {where}',
        tuple(key[name] for name in primary_key),
    )


def _select_catalog_row(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
    key: dict[str, Any],
) -> dict[str, Any] | None:
    where = " AND ".join(f'"{name}"=?' for name in primary_key)
    raw = conn.execute(
        f'SELECT * FROM "{table}" WHERE {where}',
        tuple(key[name] for name in primary_key),
    ).fetchone()
    if raw is None:
        return None
    return {name: raw[name] for name in columns}


def _catalog_row_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
