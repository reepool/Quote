from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetLifecycleManager,
    AnnouncementAssetOutboxDispatcher,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AssetAvailability,
    ConsumerProcessingStatus,
    ContentAddressedBlobStore,
    DeletionStatus,
    EffectiveDecisionKind,
    EffectiveDecisionState,
    IntegrityStatus,
    OfficialAttachmentVersion,
    OfficialDocumentBlob,
    ProvisionalResultConfig,
)
from research.announcement_assets.access import AnnouncementAssetAccess
from research.announcement_assets.backup import AnnouncementAssetBackupService
from research.announcement_assets.models import stable_id
from research.announcement_assets.storage import MountIdentity
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)

PDF_ORIGINAL = b"%PDF-1.4\noriginal annual report\n%%EOF\n"
PDF_CORRECTION = b"%PDF-1.4\ncorrected annual report\n%%EOF\n"


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": False,
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
            "retry": {
                "max_attempts": 3,
                "initial_backoff_seconds": 1,
                "max_backoff_seconds": 10,
                "lease_seconds": 30,
                "heartbeat_seconds": 5,
                "lease_safety_grace_seconds": 5,
            },
            "backup": {
                "enabled": True,
                "mount_root": "backup-mount",
                "destination_root": "backup-mount/annual-reports",
                "expected_mount_source": "backup.example:/quote",
                "expected_failure_domain": "backup-nas",
                "warning_utilization": 0.98,
                "hard_stop_utilization": 0.999,
                "free_space_reserve_bytes": 1,
                "freshness_hours": 48,
            },
        },
        project_root=tmp_path,
    )


def _record(
    *,
    correction: bool = False,
    source_id: str | None = None,
    published_at: str | None = None,
) -> AnnouncementRecord:
    source_id = source_id or ("correction" if correction else "original")
    suffix = "（修订版）" if correction else ""
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=f"甲公司2025年年度报告{suffix}",
        published_at=published_at
        or (
            "2026-04-02T01:00:00+00:00"
            if correction
            else "2026-03-20T01:00:00+00:00"
        ),
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url=f"https://static.example/{source_id}.pdf",
                attachment_id=source_id,
                name=f"甲公司2025年年度报告{suffix}.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": source_id},
    )


class _MappedRetriever:
    def __init__(
        self,
        *,
        fail_correction: bool = False,
        delay: float = 0.0,
        contents: dict[str, bytes] | None = None,
    ):
        self.fail_correction = fail_correction
        self.delay = delay
        self.contents = dict(contents or {})
        self.calls = 0
        self._lock = threading.Lock()

    def retrieve(self, source, attachment, *, require_pdf=False):
        with self._lock:
            self.calls += 1
        if self.delay:
            time.sleep(self.delay)
        is_correction = "correction" in attachment.source_url
        if is_correction and self.fail_correction:
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="failed",
                errors=("attachment_http_503",),
            )
        content = self.contents.get(
            attachment.attachment_id,
            PDF_CORRECTION if is_correction else PDF_ORIGINAL,
        )
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            final_url=attachment.resolved_url or attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at=(
                "2026-04-02T02:00:00+00:00"
                if is_correction
                else "2026-03-20T02:00:00+00:00"
            ),
            signature_status="valid_pdf",
        )


def _service(tmp_path: Path, retriever: _MappedRetriever):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    return service, repository, store


def _verify_backup(repository, blob, *, name: str) -> None:
    repository.upsert_backup_state(
        content_hash=blob.content_hash,
        config_fingerprint="test-config.v1",
        destination_identity="quotebak",
        failure_domain="backup-nas",
        backup_path=name,
        content_length=blob.content_length,
        status="verified",
        file_manifest_watermark="files-1",
        catalog_snapshot_watermark="db-1",
        verified_at="2026-04-02T03:00:00+00:00",
    )


def _run_backup(repository, store, *, crash_hook=None):
    config = store.config
    assert config.backup.mount_root is not None
    config.backup.mount_root.mkdir(parents=True, exist_ok=True)

    def mount(_config):
        return MountIdentity(
            requested_path=config.backup.mount_root,
            mount_point=config.backup.mount_root,
            source="backup.example:/quote",
            fs_type="nfs4",
            device_id=99,
        )

    return AnnouncementAssetBackupService(
        repository=repository,
        config=config,
        mount_validator=mount,
        recovery_crash_hook=crash_hook,
    ).run()


def _write_stale_part(
    store,
    *,
    attachment_id: str,
    owner: str,
    generation: int,
    created_at: str,
) -> tuple[Path, Path]:
    path = store.config.temp_root / f"{attachment_id}.part"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"partial-download")
    sidecar = Path(f"{path}.json")
    sidecar.write_text(
        json.dumps(
            {
                "artifact_type": "part",
                "managed_path": str(path),
                "attachment_id": attachment_id,
                "owner": owner,
                "lease_generation": generation,
                "planned_bytes": 1024,
                "actual_bytes": path.stat().st_size,
                "created_at": created_at,
            }
        ),
        encoding="utf-8",
    )
    return path, sidecar


def _register_verified_version_without_activation(
    repository,
    store,
    *,
    attachment_id: str,
    content: bytes,
    version_available_at: str,
):
    published = store.publish_bytes(
        content,
        expected_hash=hashlib.sha256(content).hexdigest(),
    )
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=published.content_hash,
            content_length=published.content_length,
            canonical_path=str(published.path),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at=version_available_at,
            last_verified_at=version_available_at,
        )
    )
    return repository.add_attachment_version(
        OfficialAttachmentVersion(
            version_id=stable_id("manual-version", attachment_id),
            attachment_id=attachment_id,
            observation_key=stable_id("manual-observation", attachment_id),
            content_hash=published.content_hash,
            final_url=f"https://static.example/{attachment_id}.pdf",
            retrieval_status="success",
            integrity_status=IntegrityStatus.VALID,
            attempt=1,
            next_retry_at=None,
            error_code=None,
            observed_at=version_available_at,
            version_available_at=version_available_at,
        )
    )


def _pending_replacement(tmp_path: Path):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    service.acquire_attachment(correction[0].attachment_id)
    deletion = repository.list_deletions()[0]
    original_blob = repository.get_blob(original_asset.content_hash)
    return repository, store, original_blob, deletion


def _ready_replacement(tmp_path: Path):
    repository, store, original_blob, deletion = _pending_replacement(tmp_path)
    backup = _run_backup(repository, store)
    assert backup.recovery_pairs_closed == 1
    return repository, store, original_blob, deletion


def test_dry_run_blocks_predecessor_unlink_without_state_change(tmp_path):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    original_path = Path(original_blob.canonical_path)
    dry_run_store = ContentAddressedBlobStore(replace(store.config, dry_run=True))

    result = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=dry_run_store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])

    assert result.deleted is False
    assert result.reason_code == "dry_run_blocks_deletion"
    assert original_path.is_file()
    persisted = repository.get_deletion(deletion["deletion_id"])
    assert persisted is not None
    assert persisted["status"] == deletion["status"]


def test_attachment_scoped_lease_makes_concurrent_callers_single_flight(tmp_path):
    retriever = _MappedRetriever(delay=0.1)
    service, _, _ = _service(tmp_path, retriever)
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                service.acquire_attachment,
                registered[0].attachment_id,
                wait_seconds=2.0,
            )
            for _ in range(2)
        ]
        assets = [future.result() for future in futures]

    assert retriever.calls == 1
    assert assets[0] is not None and assets[1] is not None
    assert assets[0].content_hash == assets[1].content_hash


def test_successful_download_releases_reservation_with_actual_stream_size(tmp_path):
    retriever = _MappedRetriever()
    service, repository, _ = _service(tmp_path, retriever)
    record = _record()
    record = replace(
        record,
        attachments=(
            replace(record.attachments[0], raw_metadata={"content_length": 1}),
        ),
    )
    registered = service.register_discovered_record(record, instrument_id="600000.SH")
    service.acquire_attachment(registered[0].attachment_id)
    with repository.connection() as conn:
        rows = conn.execute(
            "SELECT planned_bytes, status FROM official_asset_storage_reservations"
        ).fetchall()
    assert [tuple(row) for row in rows] == [(len(PDF_ORIGINAL), "completed")]


def test_storage_hard_stop_preserves_metadata_and_avoids_network(
    tmp_path, monkeypatch
):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )

    monkeypatch.setattr(
        store,
        "preflight_capacity",
        lambda _: (_ for _ in ()).throw(RuntimeError("hard reserve")),
    )
    with pytest.raises(RuntimeError, match="hard reserve"):
        service.acquire_attachment(registered[0].attachment_id)

    assert retriever.calls == 0
    assert repository.get_attachment(registered[0].attachment_id) is not None
    assert (
        repository.get_latest_attachment_version(registered[0].attachment_id) is None
    )


def test_unknown_length_stream_over_limit_is_not_published(tmp_path):
    oversized = b"%PDF-1.4\n" + b"x" * (1024 * 1024) + b"\n%%EOF\n"
    retriever = _MappedRetriever(contents={"original": oversized})
    service, repository, store = _service(tmp_path, retriever)
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )

    with pytest.raises(ValueError, match="configured annual-report limit"):
        service.acquire_attachment(registered[0].attachment_id)

    assert retriever.calls == 1
    assert list(store.config.blob_root.rglob("*.pdf")) == []
    with repository.connection() as conn:
        statuses = conn.execute(
            "SELECT status FROM official_asset_storage_reservations"
        ).fetchall()
    assert [row["status"] for row in statuses] == ["failed"]


def test_stale_part_cleanup_fences_abandoned_owner_generation(tmp_path):
    service, repository, store = _service(tmp_path, _MappedRetriever())
    attachment = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )[0]
    now = datetime(2026, 8, 10, 3, tzinfo=timezone.utc)
    owner = "abandoned-worker"
    assert repository.acquire_attachment_lease(
        attachment.attachment_id,
        lease_owner=owner,
        lease_expires_at=(now - timedelta(hours=1)).isoformat(),
        now=(now - timedelta(hours=2)).isoformat(),
    )
    generation = repository.get_attachment_lease(attachment.attachment_id)[
        "lease_generation"
    ]
    part, sidecar = _write_stale_part(
        store,
        attachment_id=attachment.attachment_id,
        owner=owner,
        generation=generation,
        created_at=(now - timedelta(hours=2)).isoformat(),
    )
    assert repository.reserve_storage(
        reservation_id="released-reservation",
        filesystem_key="filings-fs",
        planned_bytes=part.stat().st_size,
        lease_expires_at=(now + timedelta(hours=1)).isoformat(),
        capacity_bytes=1024,
        hard_reserve_bytes=1,
    )
    assert repository.release_storage_reservation("released-reservation")
    assert store.artifact_metrics(now=now).part_bytes == part.stat().st_size

    active = lambda evidence: repository.artifact_lease_is_active(
        evidence,
        now=now.isoformat(),
        safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
    )
    claim = lambda evidence: repository.claim_stale_artifact_cleanup(
        evidence,
        now=now.isoformat(),
        safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
    )
    assert store.cleanup_expired_parts(
        now=now,
        lease_is_active=active,
        lease_cleanup_claim=claim,
    ) == 1

    assert not part.exists()
    assert not sidecar.exists()
    assert repository.get_attachment_lease(attachment.attachment_id) is None
    assert not repository.heartbeat_attachment_lease(
        attachment.attachment_id,
        lease_owner=owner,
        lease_generation=generation,
        lease_expires_at=(now + timedelta(hours=1)).isoformat(),
        now=now.isoformat(),
    )


def test_stale_part_generation_cleanup_preserves_new_worker_lease(tmp_path):
    service, repository, store = _service(tmp_path, _MappedRetriever())
    attachment = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )[0]
    now = datetime(2026, 8, 10, 3, tzinfo=timezone.utc)
    assert repository.acquire_attachment_lease(
        attachment.attachment_id,
        lease_owner="old-worker",
        lease_expires_at=(now - timedelta(hours=1)).isoformat(),
        now=(now - timedelta(hours=2)).isoformat(),
    )
    old_generation = repository.get_attachment_lease(attachment.attachment_id)[
        "lease_generation"
    ]
    part, _ = _write_stale_part(
        store,
        attachment_id=attachment.attachment_id,
        owner="old-worker",
        generation=old_generation,
        created_at=(now - timedelta(hours=2)).isoformat(),
    )
    assert repository.acquire_attachment_lease(
        attachment.attachment_id,
        lease_owner="new-worker",
        lease_expires_at=(now + timedelta(hours=1)).isoformat(),
        now=now.isoformat(),
    )
    new_generation = repository.get_attachment_lease(attachment.attachment_id)[
        "lease_generation"
    ]
    assert new_generation == old_generation + 1

    assert store.cleanup_expired_parts(
        now=now,
        lease_is_active=lambda evidence: repository.artifact_lease_is_active(
            evidence,
            now=now.isoformat(),
            safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
        ),
        lease_cleanup_claim=lambda evidence: repository.claim_stale_artifact_cleanup(
            evidence,
            now=now.isoformat(),
            safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
        ),
    ) == 1

    assert not part.exists()
    current = repository.get_attachment_lease(attachment.attachment_id)
    assert current["lease_owner"] == "new-worker"
    assert current["lease_generation"] == new_generation
    assert not repository.heartbeat_attachment_lease(
        attachment.attachment_id,
        lease_owner="old-worker",
        lease_generation=old_generation,
        lease_expires_at=(now + timedelta(hours=2)).isoformat(),
        now=now.isoformat(),
    )
    assert repository.heartbeat_attachment_lease(
        attachment.attachment_id,
        lease_owner="new-worker",
        lease_generation=new_generation,
        lease_expires_at=(now + timedelta(hours=2)).isoformat(),
        now=now.isoformat(),
    )


def test_recent_attachment_heartbeat_blocks_stale_part_cleanup(tmp_path):
    service, repository, store = _service(tmp_path, _MappedRetriever())
    attachment = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )[0]
    now = datetime(2026, 8, 10, 3, tzinfo=timezone.utc)
    assert repository.acquire_attachment_lease(
        attachment.attachment_id,
        lease_owner="recent-worker",
        lease_expires_at=(now - timedelta(seconds=10)).isoformat(),
        now=(now - timedelta(seconds=60)).isoformat(),
    )
    generation = repository.get_attachment_lease(attachment.attachment_id)[
        "lease_generation"
    ]
    part, sidecar = _write_stale_part(
        store,
        attachment_id=attachment.attachment_id,
        owner="recent-worker",
        generation=generation,
        created_at=(now - timedelta(hours=2)).isoformat(),
    )
    claims = 0

    def claim(evidence):
        nonlocal claims
        claims += 1
        return repository.claim_stale_artifact_cleanup(
            evidence,
            now=now.isoformat(),
            safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
        )

    assert store.cleanup_expired_parts(
        now=now,
        lease_is_active=lambda evidence: repository.artifact_lease_is_active(
            evidence,
            now=now.isoformat(),
            safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
        ),
        lease_cleanup_claim=claim,
    ) == 0
    assert claims == 0
    assert part.exists()
    assert sidecar.exists()


def test_stale_part_crash_after_unlink_recovers_orphan_sidecar(tmp_path):
    service, repository, store = _service(tmp_path, _MappedRetriever())
    attachment = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )[0]
    now = datetime(2026, 8, 10, 3, tzinfo=timezone.utc)
    assert repository.acquire_attachment_lease(
        attachment.attachment_id,
        lease_owner="crashed-worker",
        lease_expires_at=(now - timedelta(hours=1)).isoformat(),
        now=(now - timedelta(hours=2)).isoformat(),
    )
    generation = repository.get_attachment_lease(attachment.attachment_id)[
        "lease_generation"
    ]
    part, sidecar = _write_stale_part(
        store,
        attachment_id=attachment.attachment_id,
        owner="crashed-worker",
        generation=generation,
        created_at=(now - timedelta(hours=2)).isoformat(),
    )
    active = lambda evidence: repository.artifact_lease_is_active(
        evidence,
        now=now.isoformat(),
        safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
    )
    claim = lambda evidence: repository.claim_stale_artifact_cleanup(
        evidence,
        now=now.isoformat(),
        safety_grace_seconds=store.config.storage.part_safety_grace_seconds,
    )
    with pytest.raises(SystemExit, match="crash after part unlink"):
        store.cleanup_expired_parts(
            now=now,
            lease_is_active=active,
            lease_cleanup_claim=claim,
            after_unlink=lambda _: (_ for _ in ()).throw(
                SystemExit("crash after part unlink")
            ),
        )

    assert not part.exists()
    assert sidecar.exists()
    assert repository.get_attachment_lease(attachment.attachment_id) is None
    assert store.cleanup_expired_parts(
        now=now,
        lease_is_active=active,
        lease_cleanup_claim=claim,
    ) == 1
    assert not sidecar.exists()


def test_effective_activation_cas_prevents_older_correction_downgrade(tmp_path):
    retriever = _MappedRetriever(
        contents={
            "old": b"%PDF-1.4\nold correction\n%%EOF\n",
            "new": b"%PDF-1.4\nnew correction\n%%EOF\n",
        }
    )
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    service.acquire_attachment(original[0].attachment_id)
    old = service.register_discovered_record(
        _record(
            correction=True,
            source_id="old",
            published_at="2026-04-01T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    new = service.register_discovered_record(
        _record(
            correction=True,
            source_id="new",
            published_at="2026-04-03T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    entered = threading.Event()
    release = threading.Event()
    activate = repository.activate_effective_report

    def gated(report, **kwargs):
        if report.source_announcement_id == "old":
            entered.set()
            assert release.wait(2.0)
        return activate(report, **kwargs)

    repository.activate_effective_report = gated
    with ThreadPoolExecutor(max_workers=2) as executor:
        old_future = executor.submit(service.acquire_attachment, old[0].attachment_id)
        assert entered.wait(2.0)
        new_asset = service.acquire_attachment(new[0].attachment_id)
        release.set()
        old_asset = old_future.result()

    current = repository.get_effective_report("600000.SH", 2025)
    assert current is not None
    assert current.source_announcement_id == "new"
    assert new_asset is not None and new_asset.source_announcement_id == "new"
    assert old_asset is not None and old_asset.source_announcement_id == "new"
    deletions = repository.list_deletions()
    assert len(deletions) == 1
    assert deletions[0]["replacement_asset_id"] == current.asset_id


def test_activation_failure_before_commit_preserves_predecessor(tmp_path, monkeypatch):
    retriever = _MappedRetriever()
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    activate = repository.activate_effective_report

    def fail_correction(report, **kwargs):
        if report.variant.value == "correction":
            raise RuntimeError("injected activation failure")
        return activate(report, **kwargs)

    monkeypatch.setattr(repository, "activate_effective_report", fail_correction)
    with pytest.raises(RuntimeError, match="injected activation failure"):
        service.acquire_attachment(correction[0].attachment_id)

    current = repository.get_effective_report("600000.SH", 2025)
    assert current is not None and current.asset_id == original_asset.asset_id
    assert repository.list_deletions() == []


def test_outbox_insert_failure_rolls_back_entire_activation_transaction(tmp_path):
    retriever = _MappedRetriever()
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)
    repository.upsert_consumer_processing(
        asset_id=original_asset.asset_id,
        consumer="business-profile",
        parser_version="parser-v1",
        parameter_hash="params-v1",
        status=ConsumerProcessingStatus.CURRENT,
    )
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    with repository.connection() as conn:
        conn.executescript(
            """
            CREATE TRIGGER fail_replacement_outbox
            BEFORE INSERT ON official_asset_change_events
            WHEN NEW.event_type = 'replaced'
            BEGIN
                SELECT RAISE(ABORT, 'injected outbox failure');
            END;
            """
        )
        conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="injected outbox failure"):
        service.acquire_attachment(correction[0].attachment_id)

    current = repository.get_effective_report("600000.SH", 2025)
    assert current is not None and current.asset_id == original_asset.asset_id
    assert repository.list_deletions() == []
    assert [event["event_type"] for event in repository.list_change_events()] == [
        "added"
    ]
    assert (
        repository.list_consumer_processing(asset_id=original_asset.asset_id)[0][
            "status"
        ]
        == "current"
    )


def test_post_commit_crash_before_delivery_replays_without_rollback(tmp_path):
    retriever = _MappedRetriever()
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)
    initially_delivered: list[str] = []
    initial_dispatcher = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda event: initially_delivered.append(str(event["event_key"])),
    )
    assert initial_dispatcher.replay_until_idle().delivered == 1
    initial_checkpoint = repository.get_consumer_checkpoint("business-profile")

    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    corrected_asset = service.acquire_attachment(correction[0].attachment_id)
    assert corrected_asset.asset_id != original_asset.asset_id

    crashing = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda _: pytest.fail("delivery hook should crash first"),
        before_delivery=lambda _: (_ for _ in ()).throw(
            SystemExit("crash before delivery")
        ),
    )
    with pytest.raises(SystemExit, match="crash before delivery"):
        crashing.dispatch_once()

    after_crash = repository.get_consumer_checkpoint("business-profile")
    assert after_crash["last_event_id"] == initial_checkpoint["last_event_id"]
    assert repository.get_effective_report("600000.SH", 2025).asset_id == (
        corrected_asset.asset_id
    )

    replayed: list[dict] = []
    recovered = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda event: replayed.append(dict(event)),
    ).replay_until_idle()
    assert recovered.delivered == 1
    assert [event["event_type"] for event in replayed] == ["replaced"]
    assert replayed[0]["asset_id"] == corrected_asset.asset_id
    assert replayed[0]["predecessor_asset_id"] == original_asset.asset_id
    assert replayed[0]["trigger_origin"] == "effective_decision"
    assert replayed[0]["dispatch_policy_version"] == "consumer_dispatch.v1"


def test_post_commit_crash_before_checkpoint_redelivers_idempotently(tmp_path):
    retriever = _MappedRetriever()
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    service.acquire_attachment(original[0].attachment_id)
    noop = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="broker-risk-control",
        handler=lambda _: None,
    )
    assert noop.replay_until_idle().delivered == 1
    initial_checkpoint = repository.get_consumer_checkpoint("broker-risk-control")

    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    corrected_asset = service.acquire_attachment(correction[0].attachment_id)
    handler_calls: list[str] = []
    processed_by_key: dict[str, str] = {}

    def idempotent_handler(event):
        event_key = str(event["event_key"])
        handler_calls.append(event_key)
        processed_by_key.setdefault(event_key, str(event["asset_id"]))

    crashing = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="broker-risk-control",
        handler=idempotent_handler,
        before_checkpoint=lambda _: (_ for _ in ()).throw(
            SystemExit("crash before checkpoint")
        ),
    )
    with pytest.raises(SystemExit, match="crash before checkpoint"):
        crashing.dispatch_once()

    assert repository.get_consumer_checkpoint("broker-risk-control")[
        "last_event_id"
    ] == initial_checkpoint["last_event_id"]
    recovered = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="broker-risk-control",
        handler=idempotent_handler,
    ).replay_until_idle()

    assert recovered.delivered == 1
    assert len(handler_calls) == 2
    assert len(set(handler_calls)) == 1
    assert processed_by_key[handler_calls[0]] == corrected_asset.asset_id
    checkpoint = repository.get_consumer_checkpoint("broker-risk-control")
    assert checkpoint["last_event_key"] == handler_calls[0]
    assert checkpoint["last_event_id"] > initial_checkpoint["last_event_id"]


def test_consumer_checkpoints_are_independent_and_failure_is_retryable(tmp_path):
    retriever = _MappedRetriever()
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)

    failed = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda _: (_ for _ in ()).throw(RuntimeError("parser offline")),
    ).dispatch_once()
    assert failed.failed == 1
    failed_checkpoint = repository.get_consumer_checkpoint("business-profile")
    assert failed_checkpoint["last_event_id"] == 0
    assert failed_checkpoint["last_error_code"] == "RuntimeError:parser offline"

    broker_events: list[str] = []
    broker = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="broker-risk-control",
        handler=lambda event: broker_events.append(str(event["event_key"])),
    ).replay_until_idle()
    assert broker.delivered == 1
    assert repository.get_consumer_checkpoint("broker-risk-control")[
        "last_event_id"
    ] > failed_checkpoint["last_event_id"]

    profile_events: list[str] = []
    profile = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda event: profile_events.append(str(event["event_key"])),
    ).replay_until_idle()
    assert profile.delivered == 1
    assert profile_events == broker_events
    assert repository.get_consumer_checkpoint("business-profile")[
        "last_error_code"
    ] is None
    assert repository.get_effective_report("600000.SH", 2025).asset_id == (
        original_asset.asset_id
    )


def test_failed_correction_keeps_original_valid_and_marks_decision_provisional(
    tmp_path,
):
    retriever = _MappedRetriever(fail_correction=True)
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    result = service.acquire_attachment(correction[0].attachment_id)

    assert result is not None
    assert result.asset_id == original_asset.asset_id
    assert result.content_hash == original_asset.content_hash
    assert result.availability is AssetAvailability.LOCAL_VALID
    assert result.decision_state is EffectiveDecisionState.PROVISIONAL
    assert repository.list_deletions() == []


def test_verified_conflict_projects_ambiguity_and_isolates_historical_processing(
    tmp_path,
):
    retriever = _MappedRetriever(
        contents={
            "sse-conflict-correction-bytes": (
                b"%PDF-1.4\nconflicting official bytes\n%%EOF\n"
            )
        }
    )
    service, repository, _ = _service(tmp_path, retriever)
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    current = service.acquire_attachment(original[0].attachment_id)
    default_processing_id = repository.upsert_consumer_processing(
        asset_id=current.asset_id,
        consumer="default-effective",
        parser_version="parser-v1",
        parameter_hash="default-params",
        status=ConsumerProcessingStatus.CURRENT,
    )
    exact_processing_id = repository.upsert_consumer_processing(
        asset_id=current.asset_id,
        consumer="exact-observation",
        parser_version="parser-v1",
        parameter_hash="exact-params",
        status=ConsumerProcessingStatus.CURRENT,
        metadata={
            "selector_kind": "exact_observation",
            "observation_version": current.version_id,
        },
    )
    cutoff_processing_id = repository.upsert_consumer_processing(
        asset_id=current.asset_id,
        consumer="cutoff-consumer",
        parser_version="parser-v1",
        parameter_hash="cutoff-params",
        status=ConsumerProcessingStatus.CURRENT,
        metadata={
            "selector_kind": "knowledge_cutoff",
            "knowledge_cutoff": "2026-03-21T00:00:00+00:00",
        },
    )
    conflict_record = replace(
        _record(
            source_id="sse-conflict-correction-bytes",
            published_at="2026-03-20T01:00:00+00:00",
        ),
        source="sse",
        announcement_key=build_announcement_key(
            "sse", "sse-conflict-correction-bytes"
        ),
    )
    conflict = service.register_discovered_record(
        conflict_record,
        instrument_id="600000.SH",
    )

    ambiguous = service.acquire_attachment(conflict[0].attachment_id)

    assert ambiguous.asset_id == current.asset_id
    assert ambiguous.content_hash == current.content_hash
    assert ambiguous.availability is AssetAvailability.LOCAL_VALID
    assert ambiguous.decision_state is EffectiveDecisionState.AMBIGUOUS
    assert ambiguous.pending_candidate_id is not None
    assert repository.list_deletions() == []
    processing = {
        item["processing_id"]: item
        for item in repository.list_consumer_processing(asset_id=current.asset_id)
    }
    assert processing[default_processing_id]["status"] == "stale"
    assert processing[default_processing_id]["error_code"] == "pending_correction"
    assert processing[exact_processing_id]["status"] == "current"
    assert processing[cutoff_processing_id]["status"] == "current"
    with pytest.raises(ValueError, match="default-effective consumer processing"):
        repository.upsert_consumer_processing(
            asset_id=current.asset_id,
            consumer="new-default",
            parser_version="parser-v1",
            parameter_hash="new-default-params",
            status=ConsumerProcessingStatus.QUEUED,
        )
    with pytest.raises(ValueError, match="consumer selector evidence"):
        repository.upsert_consumer_processing(
            asset_id=current.asset_id,
            consumer="cutoff-after-conflict",
            parser_version="parser-v1",
            parameter_hash="cutoff-after-conflict-params",
            status=ConsumerProcessingStatus.QUEUED,
            metadata={
                "selector_kind": "knowledge_cutoff",
                "knowledge_cutoff": "2026-04-03T00:00:00+00:00",
            },
        )
    with pytest.raises(ValueError, match="consumer selector evidence"):
        repository.upsert_consumer_processing(
            asset_id=current.asset_id,
            consumer="forged-exact-observation",
            parser_version="parser-v1",
            parameter_hash="forged-exact-params",
            status=ConsumerProcessingStatus.QUEUED,
            metadata={
                "selector_kind": "exact_observation",
                "observation_version": "not-a-real-observation",
            },
        )
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert decisions[-1].decision_kind is EffectiveDecisionKind.PROJECTION_UPDATE
    assert decisions[-1].decision_state is EffectiveDecisionState.AMBIGUOUS
    assert decisions[-1].decision_policy_version == (
        service.config.provisional_result.policy_version
    )
    assert len(decisions[-1].decision_evidence["conflicting_candidate_ids"]) == 2
    assert repository.list_change_events()[-1]["payload"][
        "decision_state"
    ] == "ambiguous"

    service.config = replace(
        service.config,
        provisional_result=ProvisionalResultConfig(
            enabled=True,
            policy_version="provisional_effective.v2",
        ),
    )
    with pytest.raises(ValueError, match="explicit migration evidence"):
        service.recompute_effective_report("600000.SH", 2025)
    migrated = service.recompute_effective_report(
        "600000.SH",
        2025,
        policy_migration={
            "from_policy_version": "provisional_effective.v1",
            "to_policy_version": "provisional_effective.v2",
            "actor": "test-operator",
            "reason": "approved policy rollout",
        },
    )
    assert migrated.decision_state is EffectiveDecisionState.AMBIGUOUS
    assert repository.list_effective_decisions()[-1].decision_policy_version == (
        "provisional_effective.v2"
    )
    assert migrated.decision_evidence["policy_migration"]["actor"] == "test-operator"

    with repository.transaction() as conn:
        for attachment_id, precedence in (
            (original[0].attachment_id, 2),
            (conflict[0].attachment_id, 1),
        ):
            row = conn.execute(
                "SELECT metadata_json FROM official_announcement_attachments "
                "WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            metadata = json.loads(row["metadata_json"])
            metadata["legal_precedence"] = precedence
            conn.execute(
                "UPDATE official_announcement_attachments SET metadata_json=? "
                "WHERE attachment_id=?",
                (json.dumps(metadata, sort_keys=True), attachment_id),
            )
    resolved = service.recompute_effective_report("600000.SH", 2025)
    assert resolved.asset_id == current.asset_id
    assert resolved.decision_state is EffectiveDecisionState.CURRENT
    assert repository.list_effective_decisions()[-1].decision_state is (
        EffectiveDecisionState.CURRENT
    )
    assert repository.list_change_events()[-1]["payload"][
        "decision_state"
    ] == "current"
    assert repository.list_deletions() == []


def test_verified_conflict_without_prior_winner_persists_ambiguous_projection(
    tmp_path,
):
    service, repository, store = _service(tmp_path, _MappedRetriever())
    original = service.register_discovered_record(
        _record(source_id="cninfo-original"),
        instrument_id="600000.SH",
    )[0]
    conflict_record = replace(
        _record(
            source_id="sse-conflict",
            published_at="2026-03-21T01:00:00+00:00",
        ),
        source="sse",
        announcement_key=build_announcement_key("sse", "sse-conflict"),
    )
    conflict = service.register_discovered_record(
        conflict_record,
        instrument_id="600000.SH",
    )[0]
    _register_verified_version_without_activation(
        repository,
        store,
        attachment_id=original.attachment_id,
        content=PDF_ORIGINAL,
        version_available_at="2026-03-20T02:00:00+00:00",
    )
    _register_verified_version_without_activation(
        repository,
        store,
        attachment_id=conflict.attachment_id,
        content=b"%PDF-1.4\nconflicting official bytes\n%%EOF\n",
        version_available_at="2026-03-21T02:00:00+00:00",
    )

    ambiguous = service.recompute_effective_report("600000.SH", 2025)

    assert ambiguous is not None
    assert ambiguous.decision_state is EffectiveDecisionState.AMBIGUOUS
    assert ambiguous.availability is AssetAvailability.AMBIGUOUS
    assert ambiguous.pending_candidate_id is not None
    assert ambiguous.decision_evidence["winner_version_id"] is None
    assert len(ambiguous.decision_evidence["conflicting_observations"]) == 2
    assert repository.list_deletions() == []
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH",
        fiscal_year=2025,
    )
    assert len(decisions) == 1
    assert decisions[0].decision_state is EffectiveDecisionState.AMBIGUOUS
    assert repository.list_change_events()[-1]["payload"]["decision_state"] == (
        "ambiguous"
    )
    projection = AnnouncementAssetAccess(
        repository=repository,
        config=service.config,
        service=service,
    ).get_effective_asset("600000.SH", fiscal_year=2025)
    assert projection is not None
    assert projection["availability"] == "ambiguous"
    assert projection["effective_decision_state"] == "ambiguous"


def test_immutable_effective_decision_history_reconstructs_three_revision_lineage(
    tmp_path,
):
    service, repository, _ = _service(
        tmp_path,
        _MappedRetriever(
            contents={
                "correction-2": b"%PDF-1.4\nsecond corrected annual report\n%%EOF\n"
            }
        ),
    )
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction_one = service.register_discovered_record(
        _record(
            correction=True,
            source_id="correction-1",
            published_at="2026-04-02T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    correction_one_asset = service.acquire_attachment(correction_one[0].attachment_id)
    correction_two = service.register_discovered_record(
        _record(
            correction=True,
            source_id="correction-2",
            published_at="2026-04-03T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    correction_two_asset = service.acquire_attachment(correction_two[0].attachment_id)

    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert [item.decision_kind for item in decisions] == [
        EffectiveDecisionKind.INITIAL_ACTIVATION,
        EffectiveDecisionKind.REPLACEMENT,
        EffectiveDecisionKind.REPLACEMENT,
    ]
    assert decisions[0].predecessor_asset_id is None
    assert decisions[0].replacement_asset_id == original_asset.asset_id
    assert decisions[1].predecessor_asset_id == original_asset.asset_id
    assert decisions[1].replacement_asset_id == correction_one_asset.asset_id
    assert decisions[2].predecessor_asset_id == correction_one_asset.asset_id
    assert decisions[2].replacement_asset_id == correction_two_asset.asset_id
    assert [item.decision_sequence for item in decisions] == [1, 2, 3]
    assert decisions[2].predecessor_source == "cninfo"
    assert decisions[2].predecessor_source_announcement_id == "correction-1"
    assert decisions[2].replacement_source_announcement_id == "correction-2"
    assert repository.get_effective_report("600000.SH", 2025).asset_id == (
        correction_two_asset.asset_id
    )


def test_restored_catalog_preserves_decision_edges_before_appending_next_revision(
    tmp_path,
):
    contents = {
        "correction-2": b"%PDF-1.4\nsecond corrected annual report\n%%EOF\n",
        "correction-3": b"%PDF-1.4\nthird corrected annual report\n%%EOF\n",
    }
    service, repository, store = _service(
        tmp_path, _MappedRetriever(contents=contents)
    )
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    service.acquire_attachment(original[0].attachment_id)
    for source_id, published_at in (
        ("correction-1", "2026-04-02T01:00:00+00:00"),
        ("correction-2", "2026-04-03T01:00:00+00:00"),
    ):
        registered = service.register_discovered_record(
            _record(
                correction=True,
                source_id=source_id,
                published_at=published_at,
            ),
            instrument_id="600000.SH",
        )
        service.acquire_attachment(registered[0].attachment_id)
    before = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    before_edges = [
        (
            item.decision_id,
            item.decision_sequence,
            item.predecessor_asset_id,
            item.replacement_asset_id,
            item.outbox_event_key,
        )
        for item in before
    ]

    restored_path = tmp_path / "paired-catalog-restore.db"
    shutil.copy2(repository.db_path, restored_path)
    restored_repository = AnnouncementAssetRepository(restored_path)
    restored_repository.initialize_schema()
    restored_repository.initialize_schema()
    restored_before = restored_repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert [
        (
            item.decision_id,
            item.decision_sequence,
            item.predecessor_asset_id,
            item.replacement_asset_id,
            item.outbox_event_key,
        )
        for item in restored_before
    ] == before_edges

    restored_service = AnnouncementAssetService(
        repository=restored_repository,
        config=service.config,
        blob_store=store,
        attachment_retriever=_MappedRetriever(contents=contents),
    )
    correction_three = restored_service.register_discovered_record(
        _record(
            correction=True,
            source_id="correction-3",
            published_at="2026-04-04T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    correction_three_asset = restored_service.acquire_attachment(
        correction_three[0].attachment_id
    )
    restored_after = restored_repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )

    assert [
        (
            item.decision_id,
            item.decision_sequence,
            item.predecessor_asset_id,
            item.replacement_asset_id,
            item.outbox_event_key,
        )
        for item in restored_after[:3]
    ] == before_edges
    assert [item.decision_sequence for item in restored_after] == [1, 2, 3, 4]
    assert restored_after[-1].predecessor_asset_id == before[-1].replacement_asset_id
    assert restored_after[-1].replacement_asset_id == correction_three_asset.asset_id
    assert len({item.outbox_event_key for item in restored_after}) == 4


def test_same_hash_distinct_legal_filings_are_retained_as_distinct_decisions(tmp_path):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    first = service.register_discovered_record(
        _record(correction=True, source_id="legal-correction-1"),
        instrument_id="600000.SH",
    )
    first_asset = service.acquire_attachment(first[0].attachment_id)
    second = service.register_discovered_record(
        _record(
            correction=True,
            source_id="legal-correction-2",
            published_at="2026-04-03T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    second_asset = service.acquire_attachment(second[0].attachment_id)
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert len(decisions) == 2
    assert decisions[1].predecessor_asset_id == first_asset.asset_id
    assert decisions[1].replacement_asset_id == second_asset.asset_id
    assert decisions[1].predecessor_content_hash == decisions[1].replacement_content_hash
    assert decisions[1].predecessor_attachment_id != decisions[1].replacement_attachment_id
    assert decisions[1].decision_evidence["physical_unlink_outcome"] == {
        "schema_version": "official_asset_physical_unlink_outcome.v1",
        "outcome": "not_applicable_shared_blob",
        "predecessor_asset_id": first_asset.asset_id,
        "replacement_asset_id": second_asset.asset_id,
        "predecessor_source": "cninfo",
        "predecessor_source_announcement_id": "legal-correction-1",
        "predecessor_attachment_id": decisions[1].predecessor_attachment_id,
        "replacement_source": "cninfo",
        "replacement_source_announcement_id": "legal-correction-2",
        "replacement_attachment_id": decisions[1].replacement_attachment_id,
        "content_hash": first_asset.content_hash,
        "reason": "same_hash_distinct_legal_filing",
    }
    event = repository.list_change_events()[-1]
    assert event["event_key"] == decisions[1].outbox_event_key
    assert event["payload"]["physical_unlink_outcome"] == decisions[1].decision_evidence[
        "physical_unlink_outcome"
    ]
    current = repository.get_effective_report("600000.SH", 2025)
    assert current.asset_id == second_asset.asset_id
    assert current.attachment_id == decisions[1].replacement_attachment_id
    assert current.announcement_id == decisions[1].replacement_announcement_id
    assert repository.list_deletions() == []
    with repository.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM official_document_blobs WHERE content_hash=?",
            (first_asset.content_hash,),
        ).fetchone()[0] == 1


def test_later_equivalent_mirror_keeps_asset_and_consumer_processing_identity(tmp_path):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    first = service.register_discovered_record(
        _record(correction=True, source_id="z-later-projection"),
        instrument_id="600000.SH",
    )
    first_asset = service.acquire_attachment(first[0].attachment_id)
    processing_id = repository.upsert_consumer_processing(
        asset_id=first_asset.asset_id,
        consumer="test-consumer",
        parser_version="parser-v1",
        parameter_hash="params-v1",
        status=ConsumerProcessingStatus.CURRENT,
    )
    original_processing = repository.list_consumer_processing(
        asset_id=first_asset.asset_id
    )[0]
    assert original_processing["canonical_projection_policy_version"] == (
        first_asset.canonical_projection_policy_version
    )
    assert original_processing["evidence_set_hash"] == first_asset.evidence_set_hash

    mirror_record = replace(
        _record(
            correction=True,
            source_id="a-earlier-projection",
            published_at="2026-04-03T01:00:00+00:00",
        ),
        source="sse",
        announcement_key=build_announcement_key("sse", "a-earlier-projection"),
    )
    mirror = service.register_discovered_record(
        mirror_record,
        instrument_id="600000.SH",
    )
    projected = service.acquire_attachment(mirror[0].attachment_id)

    assert projected is not None
    assert projected.asset_id == first_asset.asset_id
    assert projected.content_hash == first_asset.content_hash
    assert projected.source == "cninfo"
    assert len(projected.equivalent_source_filings) == 2
    assert projected.evidence_set_hash != original_processing["evidence_set_hash"]
    processing = repository.list_consumer_processing(asset_id=projected.asset_id)
    assert len(processing) == 1
    assert processing[0]["processing_id"] == processing_id
    assert processing[0]["status"] == ConsumerProcessingStatus.CURRENT.value


def test_withdrawal_with_fallback_records_replacement_decision(tmp_path):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    correction_asset = service.acquire_attachment(correction[0].attachment_id)
    withdrawn_record = replace(
        _record(correction=True),
        attachments=(
            replace(
                _record(correction=True).attachments[0],
                raw_metadata={
                    "withdrawal_target_id": correction[0].attachment_id,
                    "withdrawal_evidence_type": "provider_withdrawal",
                },
            ),
        ),
    )
    service.register_discovered_record(withdrawn_record, instrument_id="600000.SH")
    fallback = service.acquire_attachment(correction[0].attachment_id)

    assert fallback is not None and fallback.asset_id == original_asset.asset_id
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert decisions[-1].decision_kind is EffectiveDecisionKind.REPLACEMENT
    assert decisions[-1].predecessor_asset_id == correction_asset.asset_id
    assert decisions[-1].replacement_asset_id == original_asset.asset_id
    assert repository.active_retention_pin_count(original_asset.content_hash) > 0


def test_withdrawal_without_replacement_creates_no_winner_tombstone(tmp_path):
    service, repository, store = _service(tmp_path, _MappedRetriever())
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    current = service.acquire_attachment(registered[0].attachment_id)
    withdrawn_record = replace(
        _record(),
        attachments=(
            replace(
                _record().attachments[0],
                raw_metadata={
                    "withdrawal_target_id": registered[0].attachment_id,
                    "withdrawal_evidence_type": "provider_withdrawal",
                },
            ),
        ),
    )
    service.register_discovered_record(withdrawn_record, instrument_id="600000.SH")
    assert service.acquire_attachment(registered[0].attachment_id) is None
    assert repository.get_effective_report("600000.SH", 2025) is None
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    tombstone = decisions[-1]
    assert tombstone.decision_kind is EffectiveDecisionKind.WITHDRAWN_WITHOUT_REPLACEMENT
    assert tombstone.decision_state is EffectiveDecisionState.WITHDRAWN
    assert tombstone.predecessor_asset_id == current.asset_id
    assert tombstone.replacement_asset_id is None
    assert tombstone.replacement_content_hash is None
    deletion = repository.list_deletions()[-1]
    assert deletion["reason"] == "withdrawn_without_replacement"
    assert deletion["replacement_asset_id"] is None
    assert deletion["replacement_blob_hash"] is None
    assert deletion["recovery_pair_id"]
    assert deletion["recovery_pin_id"]
    assert deletion["decision_id"] == tombstone.decision_id
    assert deletion["outbox_event_key"] == tombstone.outbox_event_key
    assert repository.list_change_events()[-1]["event_type"] == "withdrawn"

    lifecycle = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    )
    assert (
        lifecycle.execute_deletion(deletion["deletion_id"]).reason_code
        == "recovery_pair_not_closed"
    )
    backup = _run_backup(repository, store)
    assert backup.recovery_pairs_closed == 1
    manifest = repository.get_recovery_manifest_by_pair(
        deletion["recovery_pair_id"]
    )
    assert manifest is not None
    assert manifest.manifest_kind == "withdrawal_tombstone"
    assert manifest.replacement_asset_id is None
    assert manifest.replacement_content_hash is None
    deleted = lifecycle.execute_deletion(deletion["deletion_id"])
    assert deleted.deleted
    assert repository.active_required_set_hold_count(current.content_hash) == 1


def test_source_qualified_withdrawal_notice_deactivates_bound_target(tmp_path):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    current = service.acquire_attachment(registered[0].attachment_id)
    notice_base = _record(
        source_id="withdrawal-notice",
        published_at="2026-04-05T01:00:00+00:00",
    )
    notice = replace(
        notice_base,
        title="关于撤回甲公司2025年年度报告的公告",
        attachments=(
            replace(
                notice_base.attachments[0],
                name="关于撤回甲公司2025年年度报告的公告.pdf",
                raw_metadata={
                    "withdrawal_target_id": "original",
                    "withdrawal_evidence_type": "official_relation",
                },
            ),
        ),
    )
    notice_attachment = service.register_discovered_record(
        notice, instrument_id="600000.SH"
    )
    assert service.acquire_attachment(notice_attachment[0].attachment_id) is None
    assert repository.get_effective_report("600000.SH", 2025) is None
    tombstone = repository.list_effective_decisions()[-1]
    assert tombstone.decision_kind is EffectiveDecisionKind.WITHDRAWN_WITHOUT_REPLACEMENT
    assert tombstone.predecessor_asset_id == current.asset_id
    assert tombstone.decision_evidence["withdrawal_target_id"] == "original"


def test_decision_history_and_outbox_failures_roll_back_activation(tmp_path):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    with repository.connection() as conn:
        conn.executescript(
            """
            CREATE TRIGGER fail_decision_history
            BEFORE INSERT ON official_annual_report_decisions
            WHEN NEW.decision_kind='replacement'
            BEGIN
                SELECT RAISE(ABORT, 'injected decision history failure');
            END;
            """
        )
        conn.commit()
    with pytest.raises(sqlite3.IntegrityError, match="injected decision history failure"):
        service.acquire_attachment(correction[0].attachment_id)
    assert repository.get_effective_report("600000.SH", 2025).asset_id == (
        original_asset.asset_id
    )
    assert len(repository.list_effective_decisions()) == 1
    assert [event["event_type"] for event in repository.list_change_events()] == [
        "added"
    ]
    assert repository.list_deletions() == []


@pytest.mark.parametrize(
    ("withdrawal", "failure_reason"),
    [
        (False, "effective_replacement"),
        (True, "withdrawn_without_replacement"),
    ],
)
def test_deletion_intent_failure_rolls_back_decision_outbox_and_projection(
    tmp_path,
    withdrawal,
    failure_reason,
):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(registered[0].attachment_id)
    with repository.connection() as conn:
        conn.executescript(
            f"""
            CREATE TRIGGER fail_linked_deletion_intent
            BEFORE INSERT ON official_asset_deletion_intents
            WHEN NEW.reason='{failure_reason}'
            BEGIN
                SELECT RAISE(ABORT, 'injected deletion intent failure');
            END;
            """
        )
        conn.commit()
    if withdrawal:
        withdrawn_record = replace(
            _record(),
            attachments=(
                replace(
                    _record().attachments[0],
                    raw_metadata={
                        "withdrawal_target_id": registered[0].attachment_id,
                        "withdrawal_evidence_type": "provider_withdrawal",
                    },
                ),
            ),
        )
        service.register_discovered_record(
            withdrawn_record, instrument_id="600000.SH"
        )
        target_attachment_id = registered[0].attachment_id
    else:
        correction = service.register_discovered_record(
            _record(correction=True), instrument_id="600000.SH"
        )
        target_attachment_id = correction[0].attachment_id
    with pytest.raises(sqlite3.IntegrityError, match="injected deletion intent failure"):
        service.acquire_attachment(target_attachment_id)
    assert repository.get_effective_report("600000.SH", 2025).asset_id == (
        original_asset.asset_id
    )
    assert len(repository.list_effective_decisions()) == 1
    assert len(repository.list_change_events()) == 1
    assert repository.list_deletions() == []


def test_effective_decision_history_is_append_only(tmp_path):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    service.acquire_attachment(registered[0].attachment_id)
    decision = repository.list_effective_decisions()[0]
    with repository.connection() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute(
                "UPDATE official_annual_report_decisions SET decision_state='blocked' "
                "WHERE decision_id=?",
                (decision.decision_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute(
                "DELETE FROM official_annual_report_decisions WHERE decision_id=?",
                (decision.decision_id,),
            )


def test_schema_migration_snapshots_existing_effective_projection_without_dangling_edge(
    tmp_path,
):
    service, repository, _ = _service(tmp_path, _MappedRetriever())
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    current = service.acquire_attachment(registered[0].attachment_id)
    assert current is not None
    with repository.connection() as conn:
        conn.execute("DROP TABLE official_annual_report_decisions")
        conn.execute(
            "UPDATE official_asset_schema_versions SET schema_version=7 "
            "WHERE component='announcement_assets'"
        )
        conn.commit()
    repository.initialize_schema()
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert len(decisions) == 1
    assert decisions[0].decision_kind is EffectiveDecisionKind.MIGRATION_SNAPSHOT
    assert decisions[0].replacement_asset_id == current.asset_id
    assert decisions[0].outbox_event_key
    event_keys = {
        event["event_key"] for event in repository.list_change_events()
    }
    assert decisions[0].outbox_event_key in event_keys


def test_corrupt_blob_is_quarantined_and_reacquired_under_lease(tmp_path):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    first = service.acquire_attachment(registered[0].attachment_id)
    blob = repository.get_blob(first.content_hash)
    Path(blob.canonical_path).write_bytes(b"%PDF-1.4\ncorrupt bytes\n")

    repaired = service.acquire_attachment(registered[0].attachment_id)
    validation = store.validate_blob(
        blob.canonical_path,
        expected_hash=first.content_hash,
        expected_length=len(PDF_ORIGINAL),
    )
    quarantined = list(store.config.quarantine_root.glob(f"{first.content_hash}.*.pdf"))

    assert repaired.content_hash == first.content_hash
    assert retriever.calls == 2
    assert validation.status.value == "valid"
    assert len(quarantined) == 1


def test_verified_correction_plans_then_deletes_predecessor_only_after_backup(
    tmp_path,
):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)
    original_blob = repository.get_blob(original_asset.content_hash)
    repository.upsert_consumer_processing(
        asset_id=original_asset.asset_id,
        consumer="business-profile",
        parser_version="parser-v1",
        parameter_hash="params-v1",
        status=ConsumerProcessingStatus.CURRENT,
    )
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    corrected_asset = service.acquire_attachment(correction[0].attachment_id)

    assert corrected_asset.variant.value == "correction"
    assert (
        repository.list_consumer_processing(asset_id=original_asset.asset_id)[0][
            "status"
        ]
        == "stale"
    )
    deletion = repository.list_deletions()[0]
    assert deletion["status"] == DeletionStatus.PLANNED.value
    assert deletion["recovery_pair_id"]
    assert deletion["recovery_pin_id"]
    assert repository.active_required_set_hold_count(original_asset.content_hash) == 1
    assert Path(original_blob.canonical_path).is_file()

    lifecycle = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    )
    blocked = lifecycle.execute_deletion(deletion["deletion_id"])
    assert blocked.reason_code == "recovery_pair_not_closed"
    assert Path(original_blob.canonical_path).is_file()

    corrected_blob = repository.get_blob(corrected_asset.content_hash)
    backup = _run_backup(repository, store)
    assert backup.recovery_pairs_closed == 1
    closed_intent = repository.get_deletion(deletion["deletion_id"])
    assert closed_intent["recovery_manifest_id"]
    assert closed_intent["required_set_released_at"]
    assert repository.deletion_recovery_pair_satisfies_unlink(
        deletion["deletion_id"]
    )
    deleted = lifecycle.execute_deletion(deletion["deletion_id"])
    assert deleted.deleted is True
    assert not Path(original_blob.canonical_path).exists()
    assert Path(corrected_blob.canonical_path).is_file()
    assert repository.get_deletion(deletion["deletion_id"])["status"] == "deleted"
    assert repository.active_required_set_hold_count(original_asset.content_hash) == 1
    assert [
        row["status"] for row in repository.list_deletion_audit(deletion["deletion_id"])
    ] == [
        "deleting",
        "deleted",
    ]
    deleted_events = [
        event
        for event in repository.list_change_events()
        if event["event_type"] == "deleted"
    ]
    assert len(deleted_events) == 1
    assert deleted_events[0]["asset_id"] == original_asset.asset_id
    assert deleted_events[0]["content_hash"] == original_asset.content_hash
    assert deleted_events[0]["payload"]["replacement_asset_id"] == (
        corrected_asset.asset_id
    )


@pytest.mark.parametrize(
    "boundary",
    [
        "after_file_backup",
        "after_manifest",
        "after_catalog_snapshot",
        "after_pair_closure",
        "after_pin_handoff",
    ],
)
def test_recovery_pair_crash_boundaries_resume_without_unprotected_predecessor(
    tmp_path, boundary
):
    repository, store, original_blob, deletion = _pending_replacement(tmp_path)
    injected = False

    def crash(current_boundary, _deletion_id):
        nonlocal injected
        if current_boundary == boundary and not injected:
            injected = True
            raise SystemExit(f"crash:{boundary}")

    with pytest.raises(SystemExit, match=boundary):
        _run_backup(repository, store, crash_hook=crash)
    assert Path(original_blob.canonical_path).is_file()
    assert repository.active_required_set_hold_count(original_blob.content_hash) == 1
    resumed = _run_backup(repository, store)
    assert resumed.status == "success"
    assert repository.deletion_recovery_pair_satisfies_unlink(
        deletion["deletion_id"]
    )
    assert AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"]).deleted
    assert repository.active_required_set_hold_count(original_blob.content_hash) == 1


@pytest.mark.parametrize(
    "artifact",
    ["predecessor_backup", "file_manifest", "catalog_snapshot"],
)
def test_recovery_artifact_damage_blocks_primary_unlink(tmp_path, artifact):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    manifest = repository.get_recovery_manifest_by_pair(
        deletion["recovery_pair_id"]
    )
    closure = repository.get_recovery_pair_closure(deletion["recovery_pair_id"])
    assert manifest is not None and closure is not None
    if artifact == "predecessor_backup":
        target = Path(manifest.backup_object)
    elif artifact == "file_manifest":
        target = (
            store.config.backup.destination_root
            / "manifests"
            / f"{manifest.file_manifest_watermark}.json"
        )
    else:
        target = Path(closure.catalog_snapshot_identity)
    target.write_bytes(b"damaged")

    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])
    assert blocked.reason_code == "recovery_pair_artifacts_invalid"
    assert Path(original_blob.canonical_path).is_file()


@pytest.mark.parametrize("pin_type", ["active_reader", "legacy_alias"])
def test_retention_pin_prevents_predecessor_unlink(tmp_path, pin_type):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    service.acquire_attachment(correction[0].attachment_id)
    deletion = repository.list_deletions()[0]
    pin_id = repository.add_retention_pin(
        blob_hash=original_asset.content_hash,
        pin_type=pin_type,
        pin_key="pin-1",
    )
    assert _run_backup(repository, store).recovery_pairs_closed == 1
    lifecycle = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    )

    blocked = lifecycle.execute_deletion(deletion["deletion_id"])
    assert blocked.reason_code == "retention_pin_active"
    assert repository.release_retention_pin(pin_id)
    assert lifecycle.execute_deletion(deletion["deletion_id"]).deleted


def test_shared_blob_pin_prevents_physical_predecessor_deletion(tmp_path):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    original = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    original_asset = service.acquire_attachment(original[0].attachment_id)
    shared = service.register_discovered_record(
        _record(source_id="shared-original"), instrument_id="000001.SZ"
    )
    shared_asset = service.acquire_attachment(shared[0].attachment_id)
    assert shared_asset.content_hash == original_asset.content_hash

    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    service.acquire_attachment(correction[0].attachment_id)
    deletion = repository.list_deletions()[0]
    assert _run_backup(repository, store).recovery_pairs_closed == 1

    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])
    assert blocked.reason_code == "retention_pin_active"
    assert Path(repository.get_blob(original_asset.content_hash).canonical_path).is_file()


def test_deletion_reconciles_crash_after_unlink_before_finalize(tmp_path):
    retriever = _MappedRetriever()
    service, repository, store = _service(tmp_path, retriever)
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    service.acquire_attachment(original[0].attachment_id)
    correction = service.register_discovered_record(
        _record(correction=True), instrument_id="600000.SH"
    )
    service.acquire_attachment(correction[0].attachment_id)
    deletion = repository.list_deletions()[0]
    assert _run_backup(repository, store).recovery_pairs_closed == 1
    crashing = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
        after_unlink=lambda _: (_ for _ in ()).throw(SystemExit("crash")),
    )
    with pytest.raises(SystemExit, match="crash"):
        crashing.execute_deletion(deletion["deletion_id"])
    assert repository.get_deletion(deletion["deletion_id"])["status"] == "deleting"

    recovered = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])
    assert recovered.deleted
    assert repository.get_deletion(deletion["deletion_id"])["status"] == "deleted"


@pytest.mark.parametrize(
    "race_error",
    [
        "same source/export remounted with changed filesystem identity",
        "captured filesystem became read-only",
        "post-unlink path resolved to an absent fallback mount",
    ],
)
def test_deletion_finalization_mount_races_never_write_deleted_audit(
    tmp_path, monkeypatch, race_error
):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    captured = store.validate_mount()
    after_unlink = False
    original_revalidate = store.revalidate_mount

    def fail_after_unlink(expected):
        if after_unlink:
            raise RuntimeError(race_error)
        return original_revalidate(expected)

    def mark_after_unlink(_):
        nonlocal after_unlink
        after_unlink = True

    monkeypatch.setattr(store, "revalidate_mount", fail_after_unlink)
    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
        after_unlink=mark_after_unlink,
    ).execute_deletion(deletion["deletion_id"])

    intent = repository.get_deletion(deletion["deletion_id"])
    assert blocked.status is DeletionStatus.DELETING
    assert blocked.reason_code == "deletion_mount_finalization_blocked"
    assert intent["status"] == "deleting"
    assert intent["error_code"] == "deletion_mount_finalization_blocked"
    assert intent["operation_mount_source"] == captured.source
    assert intent["operation_mount_point"] == str(captured.mount_point)
    assert intent["operation_mount_fs_type"] == captured.fs_type
    assert intent["operation_mount_device_id"] == captured.device_id
    assert intent["operation_mount_filesystem_key"] == captured.filesystem_key
    assert not Path(original_blob.canonical_path).exists()
    assert "deleted" not in {
        row["status"]
        for row in repository.list_deletion_audit(deletion["deletion_id"])
    }

    monkeypatch.setattr(store, "revalidate_mount", original_revalidate)
    recovered = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
    ).execute_deletion(deletion["deletion_id"])
    assert recovered.deleted


def test_deletion_restart_rejects_same_source_with_changed_filesystem_identity(
    tmp_path, monkeypatch
):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    captured = store.validate_mount()
    crashing = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
        after_mark_deleting=lambda _: (_ for _ in ()).throw(SystemExit("crash")),
    )
    with pytest.raises(SystemExit, match="crash"):
        crashing.execute_deletion(deletion["deletion_id"])

    changed = replace(captured, device_id=captured.device_id + 1)
    monkeypatch.setattr(store, "validate_mount", lambda: changed)
    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
    ).execute_deletion(deletion["deletion_id"])

    intent = repository.get_deletion(deletion["deletion_id"])
    assert blocked.status is DeletionStatus.DELETING
    assert blocked.reason_code == "deletion_mount_identity_changed"
    assert intent["status"] == "deleting"
    assert intent["operation_mount_source"] == changed.source
    assert intent["operation_mount_device_id"] == captured.device_id
    assert Path(original_blob.canonical_path).is_file()
    assert "deleted" not in {
        row["status"]
        for row in repository.list_deletion_audit(deletion["deletion_id"])
    }


def test_deletion_restart_keeps_deleting_when_captured_mount_becomes_read_only(
    tmp_path, monkeypatch
):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    crashing = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
        after_mark_deleting=lambda _: (_ for _ in ()).throw(SystemExit("crash")),
    )
    with pytest.raises(SystemExit, match="crash"):
        crashing.execute_deletion(deletion["deletion_id"])

    monkeypatch.setattr(
        store,
        "validate_mount",
        lambda: (_ for _ in ()).throw(
            RuntimeError("filings mount is not readable and writable")
        ),
    )
    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
    ).execute_deletion(deletion["deletion_id"])

    intent = repository.get_deletion(deletion["deletion_id"])
    assert blocked.status is DeletionStatus.DELETING
    assert blocked.reason_code == "deletion_mount_unavailable"
    assert intent["status"] == "deleting"
    assert intent["error_code"] == "deletion_mount_unavailable"
    assert Path(original_blob.canonical_path).is_file()
    assert "deleted" not in {
        row["status"]
        for row in repository.list_deletion_audit(deletion["deletion_id"])
    }


def test_active_deletion_lease_blocks_concurrent_unlink(tmp_path):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    crashing = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        after_mark_deleting=lambda _: (_ for _ in ()).throw(SystemExit("crash")),
    )
    with pytest.raises(SystemExit, match="crash"):
        crashing.execute_deletion(deletion["deletion_id"])

    concurrent = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])
    assert concurrent.reason_code == "deletion_lease_active"
    assert Path(original_blob.canonical_path).is_file()


def test_deletion_heartbeat_requires_owner_and_generation_fence(tmp_path):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    captured = store.validate_mount()
    now = datetime.now(timezone.utc)
    assert repository.claim_deletion(
        deletion["deletion_id"],
        lease_owner="worker-a",
        lease_expires_at=(now + timedelta(seconds=10)).isoformat(),
        actor="worker-a",
        mount_evidence={
            "source": captured.source,
            "mount_point": str(captured.mount_point),
            "fs_type": captured.fs_type,
            "device_id": captured.device_id,
            "filesystem_key": captured.filesystem_key,
        },
        now=now.isoformat(),
    )
    claimed = repository.get_deletion(deletion["deletion_id"])
    generation = claimed["lease_generation"]
    assert not repository.heartbeat_deletion(
        deletion["deletion_id"],
        lease_owner="worker-b",
        lease_generation=generation,
        lease_expires_at=(now + timedelta(minutes=5)).isoformat(),
    )
    assert not repository.heartbeat_deletion(
        deletion["deletion_id"],
        lease_owner="worker-a",
        lease_generation=generation + 1,
        lease_expires_at=(now + timedelta(minutes=5)).isoformat(),
    )
    assert repository.heartbeat_deletion(
        deletion["deletion_id"],
        lease_owner="worker-a",
        lease_generation=generation,
        lease_expires_at=(now + timedelta(minutes=5)).isoformat(),
    )
    refreshed = repository.get_deletion(deletion["deletion_id"])
    assert refreshed["lease_owner"] == "worker-a"
    assert refreshed["lease_generation"] == generation
    assert refreshed["lease_expires_at"] == (now + timedelta(minutes=5)).isoformat()
    assert Path(original_blob.canonical_path).is_file()


@pytest.mark.parametrize("failure_mode", ["unavailable", "identity_changed"])
def test_preclaim_mount_failure_cannot_clear_concurrent_takeover_lease(
    tmp_path, monkeypatch, failure_mode
):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    captured = store.validate_mount()
    crashing = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
        after_mark_deleting=lambda _: (_ for _ in ()).throw(SystemExit("crash")),
    )
    with pytest.raises(SystemExit, match="crash"):
        crashing.execute_deletion(deletion["deletion_id"])
    expired = repository.get_deletion(deletion["deletion_id"])

    def takeover_during_mount_check():
        now = datetime.now(timezone.utc)
        assert repository.claim_deletion(
            deletion["deletion_id"],
            lease_owner="replacement-worker",
            lease_expires_at=(now + timedelta(minutes=5)).isoformat(),
            actor="replacement-worker",
            mount_evidence={
                "source": captured.source,
                "mount_point": str(captured.mount_point),
                "fs_type": captured.fs_type,
                "device_id": captured.device_id,
                "filesystem_key": captured.filesystem_key,
            },
            now=now.isoformat(),
        )
        if failure_mode == "unavailable":
            raise RuntimeError("filings mount became unavailable")
        return replace(captured, device_id=captured.device_id + 1)

    monkeypatch.setattr(store, "validate_mount", takeover_during_mount_check)
    fenced = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])

    intent = repository.get_deletion(deletion["deletion_id"])
    assert fenced.status is DeletionStatus.DELETING
    assert fenced.reason_code == "deletion_lease_active"
    assert intent["lease_owner"] == "replacement-worker"
    assert intent["lease_generation"] == expired["lease_generation"] + 1
    assert intent["lease_expires_at"] is not None
    assert intent["error_code"] is None
    assert Path(original_blob.canonical_path).is_file()
    assert "deleted" not in {
        row["status"]
        for row in repository.list_deletion_audit(deletion["deletion_id"])
    }


def test_stale_deletion_worker_cannot_finalize_after_generation_takeover(tmp_path):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    captured = store.validate_mount()

    def takeover(_):
        now = datetime.now(timezone.utc).isoformat()
        assert repository.claim_deletion(
            deletion["deletion_id"],
            lease_owner="replacement-worker",
            lease_expires_at=now,
            actor="replacement-worker",
            mount_evidence={
                "source": captured.source,
                "mount_point": str(captured.mount_point),
                "fs_type": captured.fs_type,
                "device_id": captured.device_id,
                "filesystem_key": captured.filesystem_key,
            },
            now=now,
        )

    stale = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
        after_unlink=takeover,
    ).execute_deletion(deletion["deletion_id"])

    assert stale.reason_code == "deletion_lease_fenced"
    assert stale.status is DeletionStatus.DELETING
    assert not Path(original_blob.canonical_path).exists()
    assert "deleted" not in {
        row["status"]
        for row in repository.list_deletion_audit(deletion["deletion_id"])
    }

    recovered = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        deletion_lease_seconds=0,
    ).execute_deletion(deletion["deletion_id"])
    assert recovered.deleted


@pytest.mark.parametrize(
    "pin_type", ["active_reader", "consumer_processing", "managed_alias"]
)
def test_recently_expired_reader_pin_is_held_through_safety_grace(
    tmp_path, pin_type
):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    repository.add_retention_pin(
        blob_hash=original_blob.content_hash,
        pin_type=pin_type,
        pin_key=f"{pin_type}-with-expired-heartbeat",
        expires_at=(datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
    )

    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        lease_safety_grace_seconds=30,
    ).execute_deletion(deletion["deletion_id"])
    assert blocked.reason_code == "retention_pin_active"
    assert Path(original_blob.canonical_path).is_file()

    deleted = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
        lease_safety_grace_seconds=0,
    ).execute_deletion(deletion["deletion_id"])
    assert deleted.deleted


def test_unlink_failure_is_retryable_and_reconciles(tmp_path, monkeypatch):
    repository, store, original_blob, deletion = _ready_replacement(tmp_path)
    unlink = store.unlink_blob
    calls = 0

    def fail_once(content_hash, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected unlink failure")
        return unlink(content_hash, **kwargs)

    monkeypatch.setattr(store, "unlink_blob", fail_once)
    lifecycle = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    )
    failed = lifecycle.execute_deletion(deletion["deletion_id"])
    assert failed.status is DeletionStatus.FAILED
    assert Path(original_blob.canonical_path).is_file()

    recovered = lifecycle.execute_deletion(deletion["deletion_id"])
    assert recovered.deleted
    assert not Path(original_blob.canonical_path).exists()
