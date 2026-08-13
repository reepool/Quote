from __future__ import annotations

import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from research.announcement_assets import (
    AnnouncementAssetRepository,
    BatchOutcome,
    IdempotencyConflictError,
    IntegrityStatus,
    OfficialAssetBackupRecoveryJournalEntry,
    OfficialAssetRecoveryManifestEntry,
    OfficialAssetRecoveryPairClosure,
    OfficialDocumentBlob,
    OperationStatus,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)


def _repository(tmp_path) -> AnnouncementAssetRepository:
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    return repository


def _announcement() -> AnnouncementRecord:
    source_id = "provider-filing-1"
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title="测试公司2025年年度报告",
        published_at="2026-03-20T01:00:00+00:00",
        published_at_raw="2026-03-20 09:00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/provider-filing-1.pdf",
                attachment_id="provider-attachment-1",
                name="测试公司2025年年度报告.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": source_id, "category": "annual_report"},
    )


def _register_blob(repository: AnnouncementAssetRepository, path, digest: str) -> None:
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=path.stat().st_size,
            canonical_path=str(path),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-10T00:00:00+00:00",
            last_verified_at="2026-08-10T00:00:00+00:00",
        )
    )


def test_source_qualified_upserts_are_idempotent_and_preserve_first_observation(
    tmp_path,
):
    repository = _repository(tmp_path)
    record = _announcement()

    first = repository.upsert_announcement(
        record,
        instrument_id="600000.SH",
        observed_at="2026-08-10T00:00:00+00:00",
    )
    second = repository.upsert_announcement(
        record,
        instrument_id="600000.SH",
        observed_at="2026-08-11T00:00:00+00:00",
    )
    first_attachment = repository.upsert_attachment(
        first.announcement_id,
        record.attachments[0],
        observed_at="2026-08-10T00:00:00+00:00",
    )
    second_attachment = repository.upsert_attachment(
        second.announcement_id,
        record.attachments[0],
        observed_at="2026-08-11T00:00:00+00:00",
    )

    assert second.announcement_id == first.announcement_id
    assert second.first_observed_at == "2026-08-10T00:00:00+00:00"
    assert second.last_observed_at == "2026-08-11T00:00:00+00:00"
    assert second_attachment.attachment_id == first_attachment.attachment_id
    assert second_attachment.first_observed_at == "2026-08-10T00:00:00+00:00"
    assert second_attachment.last_observed_at == "2026-08-11T00:00:00+00:00"
    with repository.connection() as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM official_announcements").fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM official_announcement_attachments"
            ).fetchone()[0]
            == 1
        )


def test_managed_alias_pin_binds_owner_hash_expiry_and_cutover_metadata(tmp_path):
    repository = _repository(tmp_path)
    canonical = tmp_path / "canonical.pdf"
    alias = tmp_path / "consumer-readable.pdf"
    canonical.write_bytes(b"%PDF-1.4\nmanaged alias\n%%EOF\n")
    os.link(canonical, alias)
    digest = "a" * 64
    _register_blob(repository, canonical, digest)
    assert canonical.stat().st_nlink == 2
    assert repository.active_retention_pin_count(digest) == 0

    cutover = {
        "consumer": "business-profile",
        "state": "dual_read",
        "policy_version": "consumer_cutover.v1",
    }
    pin_id = repository.add_managed_alias_retention_pin(
        blob_hash=digest,
        alias_path=str(alias),
        owner="business-profile",
        expires_at="2026-08-12T00:00:00+00:00",
        cutover_metadata=cutover,
    )
    repeated = repository.add_managed_alias_retention_pin(
        blob_hash=digest,
        alias_path=str(alias),
        owner="business-profile",
        expires_at="2026-08-12T00:00:00+00:00",
        cutover_metadata=cutover,
    )
    assert repeated == pin_id
    assert (
        repository.active_retention_pin_count(digest, as_of="2026-08-11T00:00:00+00:00")
        == 1
    )
    assert (
        repository.active_retention_pin_count(digest, as_of="2026-08-13T00:00:00+00:00")
        == 0
    )
    assert canonical.stat().st_nlink == 2

    with repository.connection() as conn:
        row = conn.execute(
            "SELECT * FROM official_asset_retention_pins WHERE pin_id=?", (pin_id,)
        ).fetchone()
        count = conn.execute(
            "SELECT COUNT(*) FROM official_asset_retention_pins WHERE released_at IS NULL"
        ).fetchone()[0]
    metadata = json.loads(row["metadata_json"])
    assert count == 1
    assert row["blob_hash"] == digest
    assert row["owner"] == "business-profile"
    assert row["expires_at"] == "2026-08-12T00:00:00+00:00"
    assert metadata == {
        "alias_path": str(alias),
        "content_hash": digest,
        "cutover": cutover,
    }

    with pytest.raises(IdempotencyConflictError, match="different metadata"):
        repository.add_managed_alias_retention_pin(
            blob_hash=digest,
            alias_path=str(alias),
            owner="broker",
            expires_at="2026-08-12T00:00:00+00:00",
            cutover_metadata=cutover,
        )


def test_concurrent_managed_alias_registration_creates_one_database_pin(tmp_path):
    repository = _repository(tmp_path)
    canonical = tmp_path / "canonical.pdf"
    canonical.write_bytes(b"%PDF-1.4\nconcurrent alias\n%%EOF\n")
    digest = "b" * 64
    _register_blob(repository, canonical, digest)
    kwargs = {
        "blob_hash": digest,
        "alias_path": str(tmp_path / "managed.pdf"),
        "owner": "broker",
        "expires_at": "2099-01-01T00:00:00+00:00",
        "cutover_metadata": {
            "consumer": "broker",
            "state": "legacy_compatible",
            "policy_version": "consumer_cutover.v1",
        },
    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        pin_ids = list(
            executor.map(
                lambda _: repository.add_managed_alias_retention_pin(**kwargs),
                range(8),
            )
        )

    assert len(set(pin_ids)) == 1
    with repository.connection() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM official_asset_retention_pins WHERE released_at IS NULL"
        ).fetchone()[0]
    assert count == 1


def test_active_operation_key_rejects_incompatible_work_fingerprint(tmp_path):
    repository = _repository(tmp_path)
    scope = {
        "instrument_id": "600000.SH",
        "fiscal_year": 2025,
        "acquisition_work_fingerprint": "route-cninfo-integrity-hash-bound-2",
    }
    first, created = repository.create_or_reuse_operation(
        operation_type="ensure_annual_report",
        idempotency_key="shared-active-key",
        scope=scope,
        policy_version="annual-report.v1",
    )
    repeated, repeated_created = repository.create_or_reuse_operation(
        operation_type="ensure_annual_report",
        idempotency_key="shared-active-key",
        scope=dict(reversed(tuple(scope.items()))),
        policy_version="annual-report.v1",
    )
    assert created is True
    assert repeated_created is False
    assert repeated.operation_id == first.operation_id

    with pytest.raises(IdempotencyConflictError, match="work_fingerprint"):
        repository.create_or_reuse_operation(
            operation_type="ensure_annual_report",
            idempotency_key="shared-active-key",
            scope={
                **scope,
                "acquisition_work_fingerprint": (
                    "route-sse-integrity-signature-bound-1"
                ),
            },
            policy_version="annual-report.v1",
        )


def test_operation_claim_and_terminal_transition_are_serialized(tmp_path):
    repository = _repository(tmp_path)
    operation, _ = repository.create_or_reuse_operation(
        operation_type="ensure_annual_report",
        idempotency_key="concurrent-operation",
        scope={"acquisition_work_fingerprint": "fingerprint-1"},
        policy_version="annual-report.v1",
    )

    def claim(owner: str):
        try:
            return repository.claim_operation(
                operation.operation_id,
                lease_owner=owner,
                lease_expires_at="2099-01-01T00:00:00+00:00",
            )
        except RuntimeError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        claim_results = list(executor.map(claim, ("worker-a", "worker-b")))
    claimed = [result for result in claim_results if not isinstance(result, Exception)]
    rejected = [result for result in claim_results if isinstance(result, Exception)]
    assert len(claimed) == 1
    assert len(rejected) == 1
    assert "already held" in str(rejected[0])

    def finish(status: OperationStatus):
        try:
            return repository.transition_operation(
                operation.operation_id,
                status,
                outcome=(
                    BatchOutcome.SUCCESS
                    if status is OperationStatus.COMPLETED
                    else BatchOutcome.FAILED
                ),
                expected_lease_owner=claimed[0].lease_owner,
                expected_lease_generation=claimed[0].lease_generation,
            )
        except (RuntimeError, ValueError) as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        transition_results = list(
            executor.map(finish, (OperationStatus.COMPLETED, OperationStatus.FAILED))
        )
    succeeded = [
        result for result in transition_results if not isinstance(result, Exception)
    ]
    failed = [result for result in transition_results if isinstance(result, Exception)]
    assert len(succeeded) == 1
    assert len(failed) == 1
    assert (
        "invalid operation transition" in str(failed[0])
        or "lease owner mismatch" in str(failed[0])
    )
    current = repository.get_operation(operation.operation_id)
    assert current is not None
    assert current.status is succeeded[0].status


def _recovery_manifest() -> OfficialAssetRecoveryManifestEntry:
    return OfficialAssetRecoveryManifestEntry(
        recovery_id="recovery-1",
        manifest_kind="correction_predecessor",
        manifest_version=1,
        predecessor_asset_id="asset-old",
        source="cninfo",
        source_announcement_id="filing-old",
        attachment_id="attachment-old",
        version_id="version-old",
        prior_path="data/filings/announcements/old.pdf",
        content_hash="a" * 64,
        replacement_asset_id="asset-new",
        replacement_content_hash="b" * 64,
        backup_object="sha256/aa/old.pdf",
        file_manifest_watermark="files-10",
        recovery_pair_id="recovery-pair-1",
        consumer=None,
        active_indefinitely=True,
        created_at="2026-08-10T01:00:00+00:00",
        created_by="backup-worker",
        evidence={"backup_hash_verified": True},
    )


def test_recovery_manifest_and_pair_closure_are_immutable_and_idempotent(tmp_path):
    repository = _repository(tmp_path)
    manifest = _recovery_manifest()
    assert repository.register_recovery_manifest_entry(manifest) == manifest
    assert repository.register_recovery_manifest_entry(manifest) == manifest
    with pytest.raises(ValueError, match="cannot claim a catalog snapshot closure"):
        repository.register_recovery_manifest_entry(
            replace(
                manifest,
                recovery_id="recovery-forged",
                recovery_pair_id="pair-forged",
                catalog_snapshot_watermark="db-forged",
            )
        )

    closure = OfficialAssetRecoveryPairClosure(
        closure_id="closure-1",
        recovery_pair_id=manifest.recovery_pair_id,
        recovery_id=manifest.recovery_id,
        catalog_snapshot_identity="snapshot-10",
        catalog_snapshot_hash="c" * 64,
        file_manifest_watermark=manifest.file_manifest_watermark,
        verified_at="2026-08-10T02:00:00+00:00",
        verified_by="backup-worker",
        evidence={"bidirectional_check": True},
    )
    with pytest.raises(sqlite3.IntegrityError, match="does not match manifest"):
        repository.register_recovery_pair_closure(
            replace(
                closure,
                closure_id="closure-wrong-watermark",
                file_manifest_watermark="files-other",
            )
        )
    assert repository.register_recovery_pair_closure(closure) == closure
    assert repository.register_recovery_pair_closure(closure) == closure
    assert repository.list_recovery_pair_closures() == [closure]

    with pytest.raises(IdempotencyConflictError, match="different immutable"):
        repository.register_recovery_pair_closure(
            replace(closure, catalog_snapshot_hash="d" * 64)
        )
    with repository.connection() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                "UPDATE official_asset_recovery_manifest SET created_by='other'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute("DELETE FROM official_asset_recovery_pair_closures")


def _journal_entry(
    repository: AnnouncementAssetRepository,
    *,
    sequence: int,
    predecessor: str | None,
    coverage: str,
) -> OfficialAssetBackupRecoveryJournalEntry:
    entry = OfficialAssetBackupRecoveryJournalEntry(
        journal_entry_id=f"journal-{sequence}",
        journal_sequence=sequence,
        increment_kind="outbox",
        increment_identity=f"event-{sequence}",
        source_catalog_generation="catalog-generation-7",
        predecessor_watermark=predecessor,
        coverage_watermark=coverage,
        integrity_hash="pending",
        payload={"event_id": sequence},
        created_at=f"2026-08-10T0{sequence}:00:00+00:00",
        created_by="backup-worker",
    )
    return replace(
        entry,
        integrity_hash=repository.recovery_journal_integrity_hash(entry),
    )


def test_recovery_journal_requires_a_valid_gap_free_integrity_chain(tmp_path):
    repository = _repository(tmp_path)
    first = _journal_entry(
        repository, sequence=1, predecessor=None, coverage="watermark-1"
    )
    second = _journal_entry(
        repository,
        sequence=2,
        predecessor="watermark-1",
        coverage="watermark-2",
    )
    assert repository.append_backup_recovery_journal_entry(first) == first
    assert repository.append_backup_recovery_journal_entry(first) == first
    assert repository.append_backup_recovery_journal_entry(second) == second
    assert repository.list_backup_recovery_journal_entries() == [first, second]

    bad_hash = replace(second, journal_entry_id="journal-bad", integrity_hash="0" * 64)
    with pytest.raises(ValueError, match="integrity hash mismatch"):
        repository.append_backup_recovery_journal_entry(bad_hash)
    gap = _journal_entry(
        repository,
        sequence=4,
        predecessor="watermark-2",
        coverage="watermark-4",
    )
    with pytest.raises(ValueError, match="sequence gap"):
        repository.append_backup_recovery_journal_entry(gap)
    wrong_predecessor = _journal_entry(
        repository,
        sequence=3,
        predecessor="wrong-watermark",
        coverage="watermark-3",
    )
    with pytest.raises(ValueError, match="predecessor watermark mismatch"):
        repository.append_backup_recovery_journal_entry(wrong_predecessor)

    with (
        repository.connection() as conn,
        pytest.raises(sqlite3.IntegrityError, match="append-only"),
    ):
        conn.execute(
            "UPDATE official_asset_backup_recovery_journal "
            "SET coverage_watermark='rewritten' WHERE journal_sequence=1"
        )
