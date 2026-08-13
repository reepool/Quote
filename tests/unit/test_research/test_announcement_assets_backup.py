from __future__ import annotations

import hashlib
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from research.announcement_assets.backup import (
    AnnouncementAssetBackupService,
)
from research.announcement_assets.config import AnnouncementAssetConfig
from research.announcement_assets.models import (
    IntegrityStatus,
    OfficialAssetBackupRecoveryJournalEntry,
    OfficialAssetRecoveryManifestEntry,
    OfficialDocumentBlob,
)
from research.announcement_assets.repository import AnnouncementAssetRepository
from research.announcement_assets.restore import AnnouncementAssetRestoreService
from research.announcement_assets.storage import MountIdentity

PDF_BYTES = b"%PDF-1.4\nbackup annual report\n%%EOF\n"


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "storage": {
                "warning_utilization": 0.98,
                "hard_stop_utilization": 0.999,
                "free_space_reserve_bytes": 1,
                "max_attachment_bytes": 1024 * 1024,
                "unknown_length_reservation_bytes": 4096,
            },
            "backup": {
                "enabled": True,
                "mount_root": "backup-mount",
                "destination_root": "backup-mount/annual-reports",
                "expected_mount_source": "backup.example:/quote",
                "expected_failure_domain": "backup-array-1",
                "warning_utilization": 0.98,
                "hard_stop_utilization": 0.999,
                "free_space_reserve_bytes": 1,
                "freshness_hours": 48,
            },
        },
        project_root=tmp_path,
    )


def _mount(config: AnnouncementAssetConfig) -> MountIdentity:
    assert config.backup.mount_root is not None
    config.backup.mount_root.mkdir(parents=True, exist_ok=True)
    return MountIdentity(
        requested_path=config.backup.mount_root,
        mount_point=config.backup.mount_root,
        source="backup.example:/quote",
        fs_type="nfs4",
        device_id=99,
    )


def _register_blob(
    repository: AnnouncementAssetRepository,
    config: AnnouncementAssetConfig,
    *,
    payload: bytes = PDF_BYTES,
    required: bool = True,
) -> tuple[str, Path]:
    digest = hashlib.sha256(payload).hexdigest()
    path = config.blob_root / digest[:2] / f"{digest}.pdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(payload),
            canonical_path=str(path),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-10T00:00:00+00:00",
            last_verified_at="2026-08-10T00:00:00+00:00",
        )
    )
    if required:
        repository.add_retention_pin(
            blob_hash=digest,
            pin_type="backup-test",
            pin_key=digest,
        )
    return digest, path


def _journal_entry(
    repository: AnnouncementAssetRepository,
    *,
    sequence: int,
    predecessor: str | None,
    coverage: str,
    generation: str = "catalog-generation-1",
) -> OfficialAssetBackupRecoveryJournalEntry:
    entry = OfficialAssetBackupRecoveryJournalEntry(
        journal_entry_id=f"journal-{sequence}",
        journal_sequence=sequence,
        increment_kind="outbox",
        increment_identity=f"event-{sequence}",
        source_catalog_generation=generation,
        predecessor_watermark=predecessor,
        coverage_watermark=coverage,
        integrity_hash="",
        payload={"event_id": sequence},
        created_at=f"2026-08-10T00:00:0{sequence}+00:00",
        created_by="backup-test",
    )
    return replace(
        entry,
        integrity_hash=repository.recovery_journal_integrity_hash(entry),
    )


def test_backup_copies_once_and_pairs_manifest_with_catalog_snapshot(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, source = _register_blob(repository, config)
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )

    first = service.run()
    assert first.status == "success"
    assert first.total_blobs == 1
    assert first.copied_blobs == 1
    assert first.file_manifest_watermark
    assert first.catalog_snapshot_watermark
    assert Path(first.catalog_snapshot_path or "").is_file()
    assert Path(first.file_manifest_path or "").is_file()
    target = Path(first.items[0].backup_path or "")
    assert target.read_bytes() == PDF_BYTES

    second = service.run()
    assert second.status == "success"
    assert second.copied_blobs == 0
    assert second.items[0].status == "verified"
    assert repository.backup_satisfies_deletion_gate(
        digest,
        primary_failure_domain="primary-array",
    )
    manifest_path = Path(first.file_manifest_path or "")
    assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == manifest_path.stem

    source.unlink()
    recovery_only = service.run()
    assert recovery_only.status == "success"
    assert recovery_only.items[0].status == "verified"
    assert target.read_bytes() == PDF_BYTES


def test_backup_enumerates_only_catalog_required_blobs(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    required_hash, _ = _register_blob(repository, config)
    orphan_hash, orphan_path = _register_blob(
        repository,
        config,
        payload=b"%PDF-1.4\norphan backup object\n%%EOF\n",
        required=False,
    )
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )

    result = service.run()

    assert result.total_blobs == 1
    assert result.items[0].content_hash == required_hash
    assert orphan_path.is_file()
    assert not service._target_path(
        config.backup.destination_root, orphan_hash
    ).exists()


def test_pending_predecessor_enters_required_backup_set_before_manifest(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, source = _register_blob(repository, config)
    deletion_id = repository.plan_deletion(
        blob_hash=digest,
        managed_path=str(source),
        predecessor_asset_id="asset-old",
        replacement_asset_id="asset-new",
        replacement_blob_hash="b" * 64,
        reason="effective_replacement",
    )
    assert repository.get_deletion(deletion_id)["recovery_manifest_id"] is None
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )

    assert digest in service.required_backup_hashes()


def test_all_immutable_recovery_manifest_blob_roles_enter_required_set(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    predecessor, _ = _register_blob(
        repository,
        config,
        payload=b"%PDF-1.4\ncorrection predecessor\n%%EOF\n",
        required=False,
    )
    replacement, _ = _register_blob(
        repository,
        config,
        payload=b"%PDF-1.4\ncorrection replacement\n%%EOF\n",
        required=False,
    )
    withdrawal, _ = _register_blob(
        repository,
        config,
        payload=b"%PDF-1.4\nwithdrawal predecessor\n%%EOF\n",
        required=False,
    )
    legacy_duplicate, _ = _register_blob(
        repository,
        config,
        payload=b"%PDF-1.4\nlegacy duplicate\n%%EOF\n",
        required=False,
    )
    common = {
        "manifest_version": 1,
        "source": "cninfo",
        "source_announcement_id": "announcement-1",
        "attachment_id": "attachment-1",
        "version_id": "version-1",
        "file_manifest_watermark": "f" * 64,
        "active_indefinitely": True,
        "created_at": "2026-08-10T00:00:00+00:00",
        "created_by": "backup-test",
    }
    repository.register_recovery_manifest_entry(
        OfficialAssetRecoveryManifestEntry(
            recovery_id="recovery-correction",
            manifest_kind="correction_predecessor",
            predecessor_asset_id="asset-old",
            prior_path="/archive/old.pdf",
            content_hash=predecessor,
            replacement_asset_id="asset-new",
            replacement_content_hash=replacement,
            backup_object="/backup/old.pdf",
            recovery_pair_id="pair-correction",
            consumer=None,
            **common,
        )
    )
    repository.register_recovery_manifest_entry(
        OfficialAssetRecoveryManifestEntry(
            recovery_id="recovery-withdrawal",
            manifest_kind="withdrawal_tombstone",
            predecessor_asset_id="asset-withdrawn",
            prior_path="/archive/withdrawn.pdf",
            content_hash=withdrawal,
            replacement_asset_id=None,
            replacement_content_hash=None,
            backup_object="/backup/withdrawn.pdf",
            recovery_pair_id="pair-withdrawal",
            consumer=None,
            **common,
        )
    )
    repository.register_recovery_manifest_entry(
        OfficialAssetRecoveryManifestEntry(
            recovery_id="recovery-legacy",
            manifest_kind="legacy_path_rollback",
            predecessor_asset_id="asset-legacy",
            prior_path="/archive/legacy.pdf",
            content_hash=legacy_duplicate,
            replacement_asset_id="asset-legacy",
            replacement_content_hash=legacy_duplicate,
            backup_object="/backup/legacy.pdf",
            recovery_pair_id="pair-legacy",
            consumer="business-profile",
            **common,
        )
    )
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )

    assert service.required_backup_hashes() == {
        predecessor,
        replacement,
        withdrawal,
        legacy_duplicate,
    }


def test_backup_capacity_gate_precedes_snapshot_or_blob_publication(
    tmp_path,
    monkeypatch,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, source = _register_blob(repository, config)
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    usage = type("Usage", (), {"total": 100, "used": 99, "free": 1})()
    monkeypatch.setattr(
        "research.announcement_assets.backup.shutil.disk_usage",
        lambda _path: usage,
    )

    with pytest.raises(RuntimeError, match="free-space reserve"):
        service.run()

    destination = config.backup.destination_root
    assert destination is not None
    assert source.is_file()
    assert not service._target_path(destination, digest).exists()
    assert not (destination / "catalog").exists()
    assert not (destination / "manifests").exists()
    assert repository.get_backup_state(digest) is None


def test_operator_repair_reserves_bytes_for_mismatched_existing_target(
    tmp_path,
    monkeypatch,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    destination = config.backup.destination_root
    assert destination is not None
    target = service._target_path(destination, digest)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"wrong")
    original_mtime = target.stat().st_mtime_ns
    usage = type("Usage", (), {"total": 100, "used": 99, "free": 1})()
    monkeypatch.setattr(
        "research.announcement_assets.backup.shutil.disk_usage",
        lambda _path: usage,
    )

    with pytest.raises(RuntimeError, match="free-space reserve"):
        service.run(operator_repair=True, operator_authorized=True)

    assert target.read_bytes() == b"wrong"
    assert target.stat().st_mtime_ns == original_mtime
    assert repository.list_storage_artifact_audit(managed_path=str(target)) == []
    assert repository.get_backup_state(digest) is None


def test_backup_preserves_mismatched_target_until_operator_repair(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    destination = config.backup.destination_root
    assert destination is not None
    target = destination / "blobs" / digest[:2] / f"{digest}.pdf"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"wrong")
    original_mtime = target.stat().st_mtime_ns

    blocked = service.run()
    assert blocked.status == "partial"
    assert blocked.items[0].status == "target_mismatch"
    assert target.read_bytes() == b"wrong"
    assert target.stat().st_mtime_ns == original_mtime
    assert blocked.file_manifest_watermark is None
    assert repository.list_storage_artifact_audit(managed_path=str(target)) == []

    repaired = service.run(operator_repair=True, operator_authorized=True)
    assert repaired.status == "success"
    assert target.read_bytes() == PDF_BYTES
    quarantine = destination / "quarantine"
    assert any(path.read_bytes() == b"wrong" for path in quarantine.iterdir())
    audit = repository.list_storage_artifact_audit(managed_path=str(target))
    assert [item["status"] for item in audit] == ["planned", "deleted"]
    assert audit[0]["evidence"]["original_evidence"]["mtime_ns"] == original_mtime
    assert audit[0]["evidence"]["original_evidence"]["content_hash"] == (
        hashlib.sha256(b"wrong").hexdigest()
    )


def test_backup_remount_race_before_blob_publication_fails_closed(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)
    initial = _mount(config)
    remounted = replace(initial, device_id=initial.device_id + 1)

    def validator(active_config):
        destination = active_config.backup.destination_root
        assert destination is not None
        blob_root = destination / "blobs"
        if blob_root.exists() and any(blob_root.rglob("*.part")):
            return remounted
        return initial

    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=validator,
    )

    with pytest.raises(RuntimeError, match="mount identity changed"):
        service.run()

    destination = config.backup.destination_root
    assert destination is not None
    assert not service._target_path(destination, digest).exists()
    parts = list((destination / "blobs").rglob("*.part"))
    assert len(parts) == 1
    assert list((destination / "manifests").glob("*.json")) == []
    assert repository.get_backup_state(digest) is None

    resumed = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    ).run()
    assert resumed.status == "success"
    assert resumed.copied_blobs == 0
    assert list((destination / "blobs").rglob("*.part")) == []


def test_backup_resumes_after_crash_between_blob_and_manifest(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)

    def crash(boundary, identity):
        if boundary == "after_blob_publication" and identity == digest:
            raise RuntimeError("injected backup crash")

    interrupted = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=crash,
    )

    with pytest.raises(RuntimeError, match="injected backup crash"):
        interrupted.run()

    destination = config.backup.destination_root
    assert destination is not None
    target = interrupted._target_path(destination, digest)
    assert target.read_bytes() == PDF_BYTES
    assert repository.get_backup_state(digest) is None
    assert list((destination / "manifests").glob("*.json")) == []

    resumed = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    ).run()
    assert resumed.status == "success"
    assert resumed.copied_blobs == 0
    assert resumed.file_manifest_watermark
    assert repository.get_backup_state(digest)["status"] == "verified"


def test_backup_remount_after_manifest_does_not_commit_catalog_watermark(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)
    initial = _mount(config)
    remounted = replace(initial, device_id=initial.device_id + 1)
    state = {"identity": initial}

    def validator(_config):
        return state["identity"]

    def switch_after_manifest(boundary, _identity):
        if boundary == "after_file_manifest":
            state["identity"] = remounted

    interrupted = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=validator,
        backup_crash_hook=switch_after_manifest,
    )

    with pytest.raises(RuntimeError, match="mount identity changed"):
        interrupted.run()

    assert repository.get_backup_state(digest) is None
    destination = config.backup.destination_root
    assert destination is not None
    assert len(list((destination / "manifests").glob("*.json"))) == 1

    state["identity"] = initial
    resumed = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=validator,
    ).run()
    assert resumed.status == "success"
    assert resumed.copied_blobs == 0
    assert repository.get_backup_state(digest)["file_manifest_watermark"] == (
        resumed.file_manifest_watermark
    )


def test_restore_readiness_checks_full_required_set_not_a_sample(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, source = _register_blob(repository, config)
    repository.add_retention_pin(
        blob_hash=digest,
        pin_type="restore-test",
        pin_key="required",
    )
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )

    ready = service.verify_restore_readiness()
    assert ready.ready is True
    assert ready.required_blobs == 1
    source.unlink()
    blocked = service.verify_restore_readiness()
    assert blocked.ready is False
    assert blocked.missing_hashes == (digest,)
    assert blocked.diagnostics["sampled"] is False


def test_recovery_journal_verifies_complete_ordered_interval(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    first = _journal_entry(
        repository,
        sequence=1,
        predecessor="snapshot-watermark",
        coverage="coverage-1",
    )
    second = _journal_entry(
        repository,
        sequence=2,
        predecessor="coverage-1",
        coverage="coverage-2",
    )

    result = service.verify_recovery_journal(
        snapshot_sequence=0,
        snapshot_coverage_watermark="snapshot-watermark",
        terminal_sequence=2,
        terminal_coverage_watermark="coverage-2",
        source_catalog_generation="catalog-generation-1",
        entries=[first, second],
    )

    assert result.ready is True
    assert result.verified_entries == 2


def test_backup_persists_recovery_journal_in_independent_pair(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    first = _journal_entry(
        repository,
        sequence=1,
        predecessor=None,
        coverage="coverage-1",
    )
    second = _journal_entry(
        repository,
        sequence=2,
        predecessor="coverage-1",
        coverage="coverage-2",
    )
    repository.append_backup_recovery_journal_entry(first)
    repository.append_backup_recovery_journal_entry(second)
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )

    result = service.run()

    assert result.recovery_journal_watermark
    assert result.recovery_journal_snapshot_sequence == 2
    assert result.recovery_journal_terminal_sequence == 2
    assert result.recovery_journal_terminal_watermark == "coverage-2"
    journal_path = Path(result.recovery_journal_path or "")
    assert journal_path.is_file()
    assert hashlib.sha256(journal_path.read_bytes()).hexdigest() == journal_path.stem
    metadata, entries = service.load_recovery_journal_bundle(
        journal_path,
        expected_watermark=result.recovery_journal_watermark,
    )
    assert metadata["catalog_snapshot_watermark"] == result.catalog_snapshot_watermark
    assert entries == [first, second]


def test_paired_restore_rejects_tampered_independent_journal(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.append_backup_recovery_journal_entry(
        _journal_entry(
            repository,
            sequence=1,
            predecessor=None,
            coverage="coverage-1",
        )
    )
    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    pair = backup.run()
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )
    journal_path = Path(pair.recovery_journal_path or "")
    journal_path.write_bytes(journal_path.read_bytes() + b" ")

    readiness = restore.verify_backup_pair(
        catalog_snapshot_path=pair.catalog_snapshot_path,
        file_manifest_path=pair.file_manifest_path,
    )

    assert readiness.ready is False
    assert "recovery_journal_artifact_invalid" in readiness.errors


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("missing_middle", "journal_sequence_mismatch"),
        ("out_of_order", "journal_sequence_mismatch"),
        ("tail_truncation", "journal_interval_length_mismatch"),
        ("payload_tamper", "journal_integrity_mismatch"),
    ),
)
def test_recovery_journal_rejects_gap_order_tail_and_tampering(
    tmp_path,
    mutation,
    expected_error,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    first = _journal_entry(
        repository,
        sequence=1,
        predecessor="snapshot-watermark",
        coverage="coverage-1",
    )
    second = _journal_entry(
        repository,
        sequence=2,
        predecessor="coverage-1",
        coverage="coverage-2",
    )
    third = _journal_entry(
        repository,
        sequence=3,
        predecessor="coverage-2",
        coverage="coverage-3",
    )
    entries = [first, second]
    terminal_sequence = 2
    terminal_watermark = "coverage-2"
    if mutation == "missing_middle":
        entries = [first, third]
        terminal_sequence = 3
        terminal_watermark = "coverage-3"
    elif mutation == "out_of_order":
        entries = [second, first]
    elif mutation == "tail_truncation":
        entries = [first]
    else:
        entries = [first, replace(second, payload={"event_id": 999})]

    result = service.verify_recovery_journal(
        snapshot_sequence=0,
        snapshot_coverage_watermark="snapshot-watermark",
        terminal_sequence=terminal_sequence,
        terminal_coverage_watermark=terminal_watermark,
        source_catalog_generation="catalog-generation-1",
        entries=entries,
    )

    assert result.ready is False
    assert any(expected_error in error for error in result.errors)


def test_paired_restore_requires_freeze_rpo_and_consistent_catalog(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )
    backup_pair = backup.run()

    ready = restore.verify_paired_restore(
        writes_frozen=True,
        declared_snapshot_rpo_seconds=300,
        application_version="quote-v1",
        expected_application_version="quote-v1",
        snapshot_sequence=0,
        snapshot_coverage_watermark=None,
        terminal_sequence=0,
        terminal_coverage_watermark=None,
        source_catalog_generation=str(
            backup_pair.recovery_journal_source_generation
        ),
        catalog_snapshot_path=backup_pair.catalog_snapshot_path,
        file_manifest_path=backup_pair.file_manifest_path,
        journal_entries=[],
    )
    assert ready.ready is True

    blocked = restore.verify_paired_restore(
        writes_frozen=False,
        declared_snapshot_rpo_seconds=None,
        application_version="quote-v0",
        expected_application_version="quote-v1",
        snapshot_sequence=0,
        snapshot_coverage_watermark=None,
        terminal_sequence=0,
        terminal_coverage_watermark=None,
        source_catalog_generation=str(
            backup_pair.recovery_journal_source_generation
        ),
        catalog_snapshot_path=backup_pair.catalog_snapshot_path,
        file_manifest_path=backup_pair.file_manifest_path,
        journal_entries=[],
    )
    assert blocked.ready is False
    assert set(blocked.gate_errors) == {
        "write_freeze_required",
        "snapshot_rpo_required",
        "application_version_mismatch",
    }

    with repository.transaction() as conn:
        conn.execute(
            """INSERT INTO official_asset_consumer_processing(
                   processing_id, schema_version, asset_id, consumer,
                   parser_version, parameter_hash, status, derived_identity,
                   metadata_json, created_at, updated_at
               ) VALUES(?, ?, ?, ?, ?, ?, 'current', ?, '{}', ?, ?)""",
            (
                "processing-dangling",
                "official_asset_consumer_processing.v1",
                "missing-asset",
                "business-profile",
                "parser-v1",
                "parameters-v1",
                "result-1",
                "2026-08-10T00:00:00+00:00",
                "2026-08-10T00:00:00+00:00",
            ),
        )
    assert "consumer_asset_missing:processing-dangling" in (
        restore.verify_catalog_recovery_invariants()
    )


def test_paired_restore_rejects_missing_required_backup_blob(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)
    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    pair = backup.run()
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )
    Path(pair.items[0].backup_path or "").unlink()

    readiness = restore.verify_backup_pair(
        catalog_snapshot_path=pair.catalog_snapshot_path,
        file_manifest_path=pair.file_manifest_path,
    )

    assert readiness.ready is False
    assert readiness.required_blobs == 1
    assert readiness.verified_blobs == 0
    assert readiness.invalid_hashes == (digest,)


def test_restore_reconstructs_verified_legacy_paths_from_manifest(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, source = _register_blob(repository, config)
    repository.add_retention_pin(
        blob_hash=digest,
        pin_type="restore-test",
        pin_key="required",
    )
    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    backup_result = backup.run()
    legacy_path = config.adoption_roots[0] / "2025" / "600000.pdf"
    repository.register_recovery_manifest_entry(
        OfficialAssetRecoveryManifestEntry(
            recovery_id="legacy-recovery-1",
            manifest_kind="legacy_path_rollback",
            manifest_version=1,
            predecessor_asset_id="asset-1",
            source="cninfo",
            source_announcement_id="announcement-1",
            attachment_id="attachment-1",
            version_id="version-1",
            prior_path=str(legacy_path),
            content_hash=digest,
            replacement_asset_id="asset-1",
            replacement_content_hash=digest,
            backup_object=str(backup_result.items[0].backup_path),
            file_manifest_watermark=str(backup_result.file_manifest_watermark),
            recovery_pair_id="legacy-recovery-pair-1",
            consumer="business-profile",
            active_indefinitely=True,
            created_at="2026-08-10T00:00:00+00:00",
            created_by="backup-test",
        )
    )
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )

    # The live canonical path may already have been cleaned up.  The isolated
    # rollback drill must still reconstruct from the verified backup object.
    source.unlink()

    dry_run = restore.reconstruct_legacy_paths(
        dry_run=True,
        root_override=tmp_path / "restored",
    )
    assert dry_run.status == "success"
    assert dry_run.items[0].status == "planned"
    assert not Path(dry_run.items[0].reconstructed_path or "").exists()

    applied = restore.reconstruct_legacy_paths(
        dry_run=False,
        root_override=tmp_path / "restored",
        use_hardlinks=False,
    )
    target = Path(applied.items[0].reconstructed_path or "")
    assert applied.reconstructed == 1
    assert target.read_bytes() == PDF_BYTES


def test_restore_does_not_trust_mutable_legacy_path_projection(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest, _ = _register_blob(repository, config)
    legacy_path = config.adoption_roots[0] / "2025" / "600000.pdf"
    repository.upsert_legacy_path_manifest(
        legacy_path=str(legacy_path),
        consumer="business-profile",
        asset_id="asset-1",
        content_hash=digest,
        status="verified",
        manifest_version="legacy-path.v1",
    )
    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )

    report = restore.reconstruct_legacy_paths(
        dry_run=True,
        root_override=tmp_path / "restored",
    )

    assert report.planned == 0
    assert report.items == ()


def test_restore_live_legacy_publication_requires_explicit_authorization(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
    )
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )

    with pytest.raises(PermissionError, match="explicit authorization"):
        restore.reconstruct_legacy_paths(dry_run=False)


def test_backup_captures_and_replays_a_real_post_snapshot_operation(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    created = {"done": False}

    def create_operation_after_snapshot(boundary, _identity):
        if boundary != "after_catalog_snapshot" or created["done"]:
            return
        created["done"] = True
        repository.create_or_reuse_operation(
            operation_type="annual_report_asset_ensure",
            idempotency_key="post-snapshot-operation",
            scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
            policy_version="annual-report.v1",
            owner="backup-capture-test",
        )
        repository.append_change_event(
            event_key="post-snapshot-outbox",
            event_type="asset_observed",
            instrument_id="600000.SH",
            fiscal_year=2025,
            asset_id=None,
            predecessor_asset_id=None,
            content_hash=None,
            trigger_origin="backup_capture_test",
            dispatch_policy_version="consumer_dispatch.v1",
        )
        repository.ensure_consumer_checkpoint(
            "business-profile",
            metadata={"source": "post-snapshot-lineage"},
        )
        repository.append_storage_artifact_audit(
            {
                "artifact_type": "part",
                "managed_path": str(tmp_path / "captured.part"),
                "status": "planned",
                "actor": "backup-capture-test",
                "reason": "post_snapshot_audit",
            }
        )

    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=create_operation_after_snapshot,
    )

    pair = backup.run()

    assert pair.recovery_journal_snapshot_sequence == 0
    assert pair.recovery_journal_terminal_sequence == 1
    metadata, entries = backup.load_recovery_journal_bundle(
        pair.recovery_journal_path,
        expected_watermark=pair.recovery_journal_watermark,
    )
    assert len(entries) == 1
    assert entries[0].increment_kind == "catalog_changeset"
    operation_changes = next(
        item
        for item in entries[0].payload["tables"]
        if item["table"] == "official_asset_operations"
    )
    assert len(operation_changes["upserts"]) == 1
    assert {
        item["table"] for item in entries[0].payload["tables"]
    }.issuperset(
        {
            "official_asset_operations",
            "official_asset_change_events",
            "official_asset_consumer_checkpoints",
            "official_asset_storage_artifact_audit",
        }
    )

    staged = tmp_path / "isolated-restore.sqlite"
    shutil.copy2(pair.catalog_snapshot_path, staged)
    staged_repository = AnnouncementAssetRepository(staged)
    assert staged_repository.list_operations() == []
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )
    replay = restore.replay_recovery_journal(
        staged_catalog_path=staged,
        snapshot_sequence=int(metadata["snapshot_sequence"]),
        snapshot_coverage_watermark=metadata["snapshot_coverage_watermark"],
        terminal_sequence=int(metadata["terminal_sequence"]),
        terminal_coverage_watermark=metadata["terminal_coverage_watermark"],
        source_catalog_generation=str(metadata["source_catalog_generation"]),
        entries=entries,
    )

    assert replay.ready is True
    assert replay.applied_entries == 1
    assert len(staged_repository.list_operations()) == 1
    assert staged_repository.get_consumer_checkpoint("business-profile") is not None
    assert len(staged_repository.list_storage_artifact_audit()) == 1
    assert (
        staged_repository.list_backup_recovery_journal_entries()[0]
        == entries[0]
    )


def test_automatic_recovery_journal_chains_across_backup_runs(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    capture_round = {"value": 0}

    def mutate_catalog_after_snapshot(boundary, _identity):
        if boundary != "after_catalog_snapshot":
            return
        capture_round["value"] += 1
        sequence = capture_round["value"]
        repository.create_or_reuse_operation(
            operation_type="annual_report_asset_ensure",
            idempotency_key=f"automatic-capture-{sequence}",
            scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
            policy_version="annual-report.v1",
            owner="automatic-capture-test",
        )
        repository.append_change_event(
            event_key=f"automatic-outbox-{sequence}",
            event_type="asset_observed",
            instrument_id="600000.SH",
            fiscal_year=2025,
            asset_id=None,
            predecessor_asset_id=None,
            content_hash=None,
            trigger_origin="automatic_capture_test",
            dispatch_policy_version="consumer_dispatch.v1",
        )
        repository.ensure_consumer_checkpoint(
            "business-profile",
            metadata={"capture_round": sequence},
        )
        repository.append_storage_artifact_audit(
            {
                "artifact_type": "part",
                "managed_path": str(tmp_path / f"capture-{sequence}.part"),
                "status": "planned",
                "actor": "automatic-capture-test",
                "reason": f"post_snapshot_audit_{sequence}",
            }
        )

    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=mutate_catalog_after_snapshot,
    )

    first = backup.run()
    second = backup.run()

    first_metadata, first_entries = backup.load_recovery_journal_bundle(
        first.recovery_journal_path,
        expected_watermark=first.recovery_journal_watermark,
    )
    second_metadata, second_entries = backup.load_recovery_journal_bundle(
        second.recovery_journal_path,
        expected_watermark=second.recovery_journal_watermark,
    )
    assert [entry.journal_sequence for entry in first_entries] == [1]
    assert [entry.journal_sequence for entry in second_entries] == [1, 2]
    assert second_metadata["snapshot_sequence"] == 1
    assert second_metadata["snapshot_coverage_watermark"] == (
        first_entries[0].coverage_watermark
    )
    assert second_metadata["terminal_sequence"] == 2
    assert second_entries[1].predecessor_watermark == (
        second_entries[0].coverage_watermark
    )
    assert len({entry.journal_entry_id for entry in second_entries}) == 2
    assert len({entry.coverage_watermark for entry in second_entries}) == 2
    assert {
        entry.source_catalog_generation for entry in second_entries
    } == {str(second_metadata["source_catalog_generation"])}
    assert all(
        entry.integrity_hash == repository.recovery_journal_integrity_hash(entry)
        for entry in second_entries
    )
    assert Path(first.recovery_journal_path or "").parent == (
        config.backup.destination_root / "recovery-journal"
    )
    assert Path(second.recovery_journal_path or "").parent == (
        config.backup.destination_root / "recovery-journal"
    )

    second_interval = [second_entries[1]]
    complete = backup.verify_recovery_journal(
        snapshot_sequence=1,
        snapshot_coverage_watermark=first_entries[0].coverage_watermark,
        terminal_sequence=2,
        terminal_coverage_watermark=second_entries[1].coverage_watermark,
        source_catalog_generation=second_entries[1].source_catalog_generation,
        entries=second_interval,
    )
    truncated = backup.verify_recovery_journal(
        snapshot_sequence=1,
        snapshot_coverage_watermark=first_entries[0].coverage_watermark,
        terminal_sequence=2,
        terminal_coverage_watermark=second_entries[1].coverage_watermark,
        source_catalog_generation=second_entries[1].source_catalog_generation,
        entries=[],
    )
    assert complete.ready is True
    assert truncated.ready is False
    assert any(
        error.startswith("journal_interval_length_mismatch")
        for error in truncated.errors
    )
    assert "journal_terminal_watermark_mismatch" in truncated.errors
    assert first_metadata["terminal_coverage_watermark"] == (
        first_entries[0].coverage_watermark
    )

    staged = tmp_path / "second-run-restore.sqlite"
    shutil.copy2(second.catalog_snapshot_path, staged)
    staged_repository = AnnouncementAssetRepository(staged)
    assert len(staged_repository.list_operations()) == 1
    replay = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    ).replay_recovery_journal(
        staged_catalog_path=staged,
        snapshot_sequence=1,
        snapshot_coverage_watermark=first_entries[0].coverage_watermark,
        terminal_sequence=2,
        terminal_coverage_watermark=second_entries[1].coverage_watermark,
        source_catalog_generation=second_entries[1].source_catalog_generation,
        entries=second_interval,
    )
    assert replay.ready is True
    assert replay.applied_entries == 1
    assert len(staged_repository.list_operations()) == 2
    assert [
        entry.journal_sequence
        for entry in staged_repository.list_backup_recovery_journal_entries()
    ] == [1, 2]


def test_backup_rejects_a_catalog_snapshot_with_an_advanced_journal_tail(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    nested_started = {"value": False}

    def nested_mutation(boundary, _identity):
        if boundary != "after_catalog_snapshot":
            return
        repository.create_or_reuse_operation(
            operation_type="annual_report_asset_ensure",
            idempotency_key="nested-journal-winner",
            scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
            policy_version="annual-report.v1",
        )

    nested = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=nested_mutation,
    )

    def advance_journal_after_outer_snapshot(boundary, _identity):
        if boundary != "after_catalog_snapshot" or nested_started["value"]:
            return
        nested_started["value"] = True
        nested.run()

    outer = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=advance_journal_after_outer_snapshot,
    )

    with pytest.raises(RuntimeError, match="recovery journal advanced"):
        outer.run()

    entries = repository.list_backup_recovery_journal_entries()
    assert [entry.journal_sequence for entry in entries] == [1]
    assert entries[0].predecessor_watermark is None


def test_backup_retries_when_required_blob_set_changes_after_snapshot(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    created = {"done": False}

    def add_required_blob_after_snapshot(boundary, _identity):
        if boundary != "after_catalog_snapshot" or created["done"]:
            return
        created["done"] = True
        _register_blob(
            repository,
            config,
            payload=b"%PDF-1.4\npost snapshot required\n%%EOF\n",
        )

    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=add_required_blob_after_snapshot,
    )

    with pytest.raises(RuntimeError, match="required blob set changed"):
        backup.run()

    assert repository.list_backup_recovery_journal_entries() == []
    destination = config.backup.destination_root
    assert destination is not None
    assert list((destination / "manifests").glob("*.json")) == []


def test_recovery_replay_rejects_live_catalog_and_preimage_tampering(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    created = {"done": False}

    def create_operation_after_snapshot(boundary, _identity):
        if boundary == "after_catalog_snapshot" and not created["done"]:
            created["done"] = True
            repository.create_or_reuse_operation(
                operation_type="annual_report_asset_ensure",
                idempotency_key="tamper-operation",
                scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
                policy_version="annual-report.v1",
            )

    backup = AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=_mount,
        backup_crash_hook=create_operation_after_snapshot,
    )
    pair = backup.run()
    metadata, entries = backup.load_recovery_journal_bundle(
        pair.recovery_journal_path,
        expected_watermark=pair.recovery_journal_watermark,
    )
    restore = AnnouncementAssetRestoreService(
        repository=repository,
        config=config,
        backup_service=backup,
    )

    with pytest.raises(PermissionError, match="isolated catalog"):
        restore.replay_recovery_journal(
            staged_catalog_path=repository.db_path,
            snapshot_sequence=0,
            snapshot_coverage_watermark=None,
            terminal_sequence=1,
            terminal_coverage_watermark=metadata["terminal_coverage_watermark"],
            source_catalog_generation=str(metadata["source_catalog_generation"]),
            entries=entries,
        )

    staged = tmp_path / "tampered-stage.sqlite"
    shutil.copy2(pair.catalog_snapshot_path, staged)
    with sqlite3.connect(staged) as conn:
        raw = entries[0].payload["tables"]
        operation = next(
            item for item in raw if item["table"] == "official_asset_operations"
        )["upserts"][0]["row"]
        columns = tuple(operation)
        conn.execute(
            "INSERT INTO official_asset_operations("
            + ",".join(columns)
            + ") VALUES("
            + ",".join("?" for _ in columns)
            + ")",
            tuple(operation[name] for name in columns),
        )
    replay = restore.replay_recovery_journal(
        staged_catalog_path=staged,
        snapshot_sequence=0,
        snapshot_coverage_watermark=None,
        terminal_sequence=1,
        terminal_coverage_watermark=metadata["terminal_coverage_watermark"],
        source_catalog_generation=str(metadata["source_catalog_generation"]),
        entries=entries,
    )

    assert replay.ready is False
    assert replay.applied_entries == 0
    assert "pre-image" in replay.errors[0]
    assert AnnouncementAssetRepository(
        staged
    ).list_backup_recovery_journal_entries() == []
