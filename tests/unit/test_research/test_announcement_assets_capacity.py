from __future__ import annotations

import hashlib
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from research.announcement_assets import (
    CAPACITY_OVERRIDE_PERMISSION,
    AnnouncementAssetConfig,
    AnnouncementAssetLifecycleManager,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    CapacityOverrideAuthorization,
    ContentAddressedBlobStore,
    DeletionStatus,
    EnsureRequest,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)

PDF_BYTES = b"%PDF-1.4\ncapacity governed annual report\n%%EOF\n"


class _Retriever:
    def __init__(self, content: bytes = PDF_BYTES) -> None:
        self.content = content
        self.reported_hash: str | None = None
        self.calls = 0

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls += 1
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=self.content,
            content_hash=(
                self.reported_hash or hashlib.sha256(self.content).hexdigest()
            ),
            content_length=len(self.content),
            final_url=attachment.resolved_url or attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-04-01T02:00:00+00:00",
            signature_status=(
                "valid_pdf" if self.content.startswith(b"%PDF-") else "invalid"
            ),
        )


def _config(
    tmp_path,
    *,
    max_attachment_bytes: int = 256,
    trusted_identity_enabled: bool = True,
    capacity_artifact_required: bool = False,
    dry_run: bool = False,
):
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": dry_run,
            "capacity_artifact_required": capacity_artifact_required,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "storage": {
                "warning_utilization": 0.80,
                "hard_stop_utilization": 0.99,
                "free_space_reserve_bytes": 100,
                "max_attachment_bytes": max_attachment_bytes,
                "unknown_length_reservation_bytes": 64,
            },
            "acquisition": {"max_task_download_bytes": 256},
            "capacity_override": {
                "enabled": True,
                "max_bytes": 512,
                "max_duration_seconds": 600,
                "requires_operator": True,
                "audit_required": True,
                "scope_mode": "single_operation_and_target",
            },
            "permissions": {
                "trusted_identity_enabled": trusted_identity_enabled,
                "operator": CAPACITY_OVERRIDE_PERMISSION,
                "principals": (
                    [
                        {
                            "principal": "capacity-override-operator",
                            "token_env": "TEST_CAPACITY_OVERRIDE_TOKEN",
                            "scopes": [CAPACITY_OVERRIDE_PERMISSION],
                        }
                    ]
                    if trusted_identity_enabled
                    else []
                ),
            },
        },
        project_root=tmp_path,
    )


def _record(*, content_length: int | None = None, correction: bool = False):
    source_id = "correction" if correction else "original"
    suffix = "（修订版）" if correction else ""
    metadata = {} if content_length is None else {"content_length": content_length}
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=f"甲公司2025年年度报告{suffix}",
        published_at=(
            "2026-04-02T01:00:00+00:00"
            if correction
            else "2026-04-01T01:00:00+00:00"
        ),
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url=f"https://static.example/{source_id}.pdf",
                attachment_id=source_id,
                name=f"甲公司2025年年度报告{suffix}.pdf",
                media_type="application/pdf",
                raw_metadata=metadata,
            ),
        ),
        raw_payload={"announcementId": source_id},
    )


def _service(
    tmp_path,
    *,
    content: bytes = PDF_BYTES,
    max_attachment_bytes: int = 256,
    trusted_identity_enabled: bool = True,
    capacity_artifact_required: bool = False,
    dry_run: bool = False,
):
    config = _config(
        tmp_path,
        max_attachment_bytes=max_attachment_bytes,
        trusted_identity_enabled=trusted_identity_enabled,
        capacity_artifact_required=capacity_artifact_required,
        dry_run=dry_run,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever(content)
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    return service, repository, store, retriever


def test_required_capacity_artifact_blocks_before_network_lease_or_file_write(tmp_path):
    service, repository, store, retriever = _service(
        tmp_path,
        capacity_artifact_required=True,
    )
    registered = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]

    with pytest.raises(RuntimeError, match="capacity_artifact_missing"):
        service.acquire_attachment(registered.attachment_id)

    assert retriever.calls == 0
    assert repository.get_latest_attachment_version(registered.attachment_id) is None
    assert list(store.config.blob_root.rglob("*.pdf")) == []
    with repository.connection() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM official_asset_acquisition_leases"
        ).fetchone()[0] == 0


def test_production_dry_run_ensure_returns_blocker_without_operation_or_network(tmp_path):
    service, repository, store, retriever = _service(
        tmp_path,
        capacity_artifact_required=True,
        dry_run=True,
    )

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="test-operator",
        )
    )

    assert result.disposition.value == "local_miss"
    assert result.reason_code == "dry_run_blocks_network_acquisition"
    assert result.operation is None
    assert result.asset_request is None
    assert retriever.calls == 0
    assert repository.list_operations(limit=10) == []
    assert list(store.config.blob_root.rglob("*.pdf")) == []


def _operation(repository, attachment_id: str):
    operation, _ = repository.create_or_reuse_operation(
        operation_type="capacity_test",
        idempotency_key=f"capacity-test-{attachment_id}",
        scope={"attachment_id": attachment_id},
        policy_version="annual_report_effective.v1",
        owner="operator@example",
    )
    return operation


def _authorization(operation_id: str, attachment_id: str, *, max_bytes: int = 128):
    now = datetime.now(timezone.utc)
    return CapacityOverrideAuthorization(
        authorization_id=f"authorization-{operation_id}",
        operation_id=operation_id,
        target_attachment_id=attachment_id,
        principal="operator@example",
        permission_scope=CAPACITY_OVERRIDE_PERMISSION,
        max_bytes=max_bytes,
        issued_at=(now - timedelta(seconds=1)).isoformat(),
        expires_at=(now + timedelta(minutes=5)).isoformat(),
        reason="approved annual-report capacity exception",
    )


def _hard_stop_disk_usage(_):
    return SimpleNamespace(total=1000, used=920, free=80)


def test_concurrent_reservations_atomically_preserve_hard_reserve(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    barrier = threading.Barrier(2)

    def reserve(index: int) -> bool:
        barrier.wait()
        return repository.reserve_storage(
            reservation_id=f"reservation-{index}",
            filesystem_key="filings-fs",
            planned_bytes=60,
            lease_expires_at="2099-01-01T00:00:00+00:00",
            capacity_bytes=100,
            hard_reserve_bytes=10,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, range(2)))

    assert sorted(results) == [False, True]
    with repository.connection() as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS count, SUM(planned_bytes) AS planned
               FROM official_asset_storage_reservations WHERE status='active'"""
        ).fetchone()
    assert (row["count"], row["planned"]) == (1, 60)


def test_legacy_reservation_schema_adds_measured_bytes_and_override_audit(tmp_path):
    db_path = tmp_path / "research.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE official_asset_storage_reservations (
                   reservation_id TEXT PRIMARY KEY,
                   filesystem_key TEXT NOT NULL,
                   operation_id TEXT,
                   planned_bytes INTEGER NOT NULL,
                   status TEXT NOT NULL,
                   lease_expires_at TEXT NOT NULL,
                   created_at TEXT NOT NULL,
                   released_at TEXT,
                   metadata_json TEXT NOT NULL DEFAULT '{}'
               )"""
        )
    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()

    with repository.connection() as conn:
        columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(official_asset_storage_reservations)"
            ).fetchall()
        }
        audit_table = conn.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='official_asset_capacity_override_audit'"""
        ).fetchone()
    assert "actual_bytes" in columns
    assert audit_table is not None


def test_operation_planned_and_actual_budgets_are_cumulative(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    for reservation_id in ("r1", "r2"):
        assert repository.reserve_storage(
            reservation_id=reservation_id,
            filesystem_key="filings-fs",
            operation_id="operation-1",
            planned_bytes=40,
            lease_expires_at="2099-01-01T00:00:00+00:00",
            capacity_bytes=1000,
            hard_reserve_bytes=10,
            operation_planned_limit_bytes=100,
        )

    assert not repository.reserve_storage(
        reservation_id="r3",
        filesystem_key="filings-fs",
        operation_id="operation-1",
        planned_bytes=30,
        lease_expires_at="2099-01-01T00:00:00+00:00",
        capacity_bytes=1000,
        hard_reserve_bytes=10,
        operation_planned_limit_bytes=100,
    )
    assert not repository.resize_storage_reservation(
        "r1",
        planned_bytes=70,
        capacity_bytes=1000,
        hard_reserve_bytes=10,
        operation_actual_limit_bytes=100,
    )
    failed = repository.get_storage_reservation("r1")
    assert failed["planned_bytes"] == 40
    assert failed["actual_bytes"] == 70


def test_authorized_override_is_target_bound_audited_and_non_global(
    tmp_path, monkeypatch
):
    service, repository, store, retriever = _service(tmp_path)
    registered = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]
    operation = _operation(repository, registered.attachment_id)
    authorization = _authorization(operation.operation_id, registered.attachment_id)
    original_config = service.config.normalized_mapping()
    monkeypatch.setattr(
        "research.announcement_assets.storage.shutil.disk_usage",
        _hard_stop_disk_usage,
    )

    asset = service.acquire_attachment(
        registered.attachment_id,
        operation_id=operation.operation_id,
        capacity_override=authorization,
    )

    assert asset is not None
    assert retriever.calls == 1
    assert service.config.normalized_mapping() == original_config
    audits = repository.list_capacity_override_audit(
        operation_id=operation.operation_id
    )
    assert [row["outcome"] for row in audits] == ["admitted", "consumed"]
    assert {row["attachment_id"] for row in audits} == {registered.attachment_id}
    assert {row["authorization_id"] for row in audits} == {
        authorization.authorization_id
    }
    assert list(store.config.blob_root.rglob("*.pdf"))


def test_override_requires_trusted_operator_identity_boundary(tmp_path):
    service, repository, store, retriever = _service(
        tmp_path,
        trusted_identity_enabled=False,
    )
    registered = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]
    operation = _operation(repository, registered.attachment_id)
    authorization = _authorization(operation.operation_id, registered.attachment_id)

    with pytest.raises(PermissionError, match="authorization boundary"):
        service.acquire_attachment(
            registered.attachment_id,
            operation_id=operation.operation_id,
            capacity_override=authorization,
        )

    assert retriever.calls == 0
    assert repository.list_capacity_override_audit() == []
    assert list(store.config.blob_root.rglob("*.pdf")) == []


@pytest.mark.parametrize(
    "invalid_kind", ["missing", "wrong_operation", "wrong_target", "over_bound"]
)
def test_missing_or_out_of_scope_override_blocks_before_provider_or_asset_mutation(
    tmp_path, monkeypatch, invalid_kind
):
    service, repository, store, retriever = _service(tmp_path)
    registered = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]
    operation = _operation(repository, registered.attachment_id)
    authorization = _authorization(
        operation.operation_id,
        (
            "another-attachment"
            if invalid_kind == "wrong_target"
            else registered.attachment_id
        ),
        max_bytes=16 if invalid_kind == "over_bound" else 128,
    )
    monkeypatch.setattr(
        "research.announcement_assets.storage.shutil.disk_usage",
        _hard_stop_disk_usage,
    )

    with pytest.raises((RuntimeError, PermissionError)):
        service.acquire_attachment(
            registered.attachment_id,
            operation_id=(
                "another-operation"
                if invalid_kind == "wrong_operation"
                else operation.operation_id
            ),
            capacity_override=(None if invalid_kind == "missing" else authorization),
        )

    assert retriever.calls == 0
    assert repository.get_latest_attachment_version(registered.attachment_id) is None
    assert repository.list_capacity_override_audit() == []
    assert list(store.config.blob_root.rglob("*.pdf")) == []
    with repository.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_storage_reservations"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_acquisition_leases"
        ).fetchone()[0] == 0


def test_override_cannot_bypass_mount_or_pdf_integrity(tmp_path, monkeypatch):
    service, repository, store, retriever = _service(tmp_path, content=b"not-a-pdf")
    registered = service.register_discovered_record(
        _record(content_length=len(b"not-a-pdf")), instrument_id="600000.SH"
    )[0]
    operation = _operation(repository, registered.attachment_id)
    authorization = _authorization(operation.operation_id, registered.attachment_id)

    monkeypatch.setattr(
        store,
        "validate_mount",
        lambda: (_ for _ in ()).throw(RuntimeError("mount identity mismatch")),
    )
    with pytest.raises(RuntimeError, match="mount identity"):
        service.acquire_attachment(
            registered.attachment_id,
            operation_id=operation.operation_id,
            capacity_override=authorization,
        )
    assert retriever.calls == 0
    assert repository.list_capacity_override_audit() == []

    monkeypatch.undo()
    monkeypatch.setattr(
        "research.announcement_assets.storage.shutil.disk_usage",
        _hard_stop_disk_usage,
    )
    with pytest.raises(ValueError, match="valid PDF signature"):
        service.acquire_attachment(
            registered.attachment_id,
            operation_id=operation.operation_id,
            capacity_override=authorization,
        )
    assert retriever.calls == 1
    assert repository.get_latest_attachment_version(registered.attachment_id) is None
    assert list(store.config.blob_root.rglob("*.pdf")) == []


def test_override_cannot_bypass_per_attachment_limit(tmp_path):
    service, repository, store, retriever = _service(
        tmp_path,
        max_attachment_bytes=32,
    )
    registered = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]
    operation = _operation(repository, registered.attachment_id)
    authorization = _authorization(
        operation.operation_id,
        registered.attachment_id,
        max_bytes=128,
    )

    with pytest.raises(ValueError, match="configured limit"):
        service.acquire_attachment(
            registered.attachment_id,
            operation_id=operation.operation_id,
            capacity_override=authorization,
        )

    assert retriever.calls == 0
    assert repository.list_capacity_override_audit() == []
    assert list(store.config.blob_root.rglob("*.pdf")) == []


def test_override_cannot_bypass_hash_integrity(tmp_path, monkeypatch):
    service, repository, store, retriever = _service(tmp_path)
    retriever.reported_hash = "0" * 64
    registered = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]
    operation = _operation(repository, registered.attachment_id)
    authorization = _authorization(operation.operation_id, registered.attachment_id)
    monkeypatch.setattr(
        "research.announcement_assets.storage.shutil.disk_usage",
        _hard_stop_disk_usage,
    )

    with pytest.raises(ValueError, match="expected hash"):
        service.acquire_attachment(
            registered.attachment_id,
            operation_id=operation.operation_id,
            capacity_override=authorization,
        )

    assert retriever.calls == 1
    assert repository.get_latest_attachment_version(registered.attachment_id) is None
    assert list(store.config.blob_root.rglob("*.pdf")) == []


def test_override_config_does_not_relax_deletion_backup_gate(tmp_path):
    service, repository, store, retriever = _service(tmp_path)
    original = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES)), instrument_id="600000.SH"
    )[0]
    service.acquire_attachment(original.attachment_id)
    retriever.content = b"%PDF-1.4\ncorrected annual report\n%%EOF\n"
    correction = service.register_discovered_record(
        _record(content_length=len(PDF_BYTES), correction=True),
        instrument_id="600000.SH",
    )[0]
    service.acquire_attachment(correction.attachment_id)
    deletion = repository.list_deletions()[0]

    blocked = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=store,
        primary_failure_domain="primary-nas",
    ).execute_deletion(deletion["deletion_id"])

    assert blocked.status is DeletionStatus.PLANNED
    assert blocked.reason_code == "recovery_pair_not_closed"
