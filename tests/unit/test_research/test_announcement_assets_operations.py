from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from research.announcement_assets.commands import (
    INTEGRITY_AUDIT_JOB,
    AnnualReportSchedulerCommandService,
    CommandPrincipal,
)
from research.announcement_assets.config import AnnouncementAssetConfig
from research.announcement_assets.integrity import (
    AnnouncementAssetIntegrityAuditService,
)
from research.announcement_assets.models import (
    BatchOutcome,
    IntegrityStatus,
    OfficialDocumentBlob,
    OperationStage,
    OperationStatus,
)
from research.announcement_assets.readiness import AnnouncementAssetReadinessService
from research.announcement_assets.repair import ProductionIntegrityRepairHandlers
from research.announcement_assets.repository import AnnouncementAssetRepository
from research.announcement_assets.service import AnnouncementAssetService

PDF = b"%PDF-1.4\noperations audit\n%%EOF\n"


def _config(tmp_path: Path, *, trusted: bool = False) -> AnnouncementAssetConfig:
    permissions = {
        "trusted_identity_enabled": trusted,
        "operator": "annual_report_assets:operator",
    }
    if trusted:
        permissions["principals"] = [
            {
                "principal": "operator:test",
                "token_env": "ANNOUNCEMENT_ASSET_TEST_TOKEN",
                "scopes": ["annual_report_assets:operator"],
            }
        ]
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
                "predecessor_cleanup_warning_age_seconds": 10,
                "predecessor_cleanup_hard_age_seconds": 20,
            },
            "rollout_gates": {
                "require_bootstrap": False,
                "require_integrity": False,
                "require_storage": False,
                "require_backup": False,
                "require_consumer_migration": False,
            },
            "permissions": permissions,
        },
        project_root=tmp_path,
    )


def _register_blob(
    repository: AnnouncementAssetRepository,
    config: AnnouncementAssetConfig,
) -> tuple[str, Path]:
    digest = hashlib.sha256(PDF).hexdigest()
    path = config.blob_root / digest[:2] / f"{digest}.pdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(PDF)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF),
            canonical_path=str(path),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-01T00:00:00+00:00",
            last_verified_at="2026-08-01T00:00:00+00:00",
        )
    )
    return digest, path


def test_cleanup_thresholds_are_validated_and_enter_fingerprint(tmp_path):
    config = _config(tmp_path)
    assert config.storage.predecessor_cleanup_warning_age_seconds == 10
    assert config.storage.predecessor_cleanup_hard_age_seconds == 20
    normalized = config.normalized_mapping()["storage"]
    assert normalized["predecessor_cleanup_warning_age_seconds"] == 10
    assert normalized["predecessor_cleanup_hard_age_seconds"] == 20
    with pytest.raises(ValueError, match="warning age must be below hard age"):
        AnnouncementAssetConfig.from_mapping(
            {
                "storage": {
                    "predecessor_cleanup_warning_age_seconds": 20,
                    "predecessor_cleanup_hard_age_seconds": 20,
                }
            },
            project_root=tmp_path,
        )


def test_readiness_persists_reloadable_capacity_estimate(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-1",
            "policy_version": "a_share_active.v1",
            "snapshot_at": "2026-08-10T00:00:00+00:00",
            "freshness_limit_seconds": 36 * 3600,
                "status": "complete",
                "source_complete": True,
                "paired_census_snapshot_id": "census-complete",
                "metadata": {"census_reconciliation": {"status": "complete"}},
                "instruments": (
                {"instrument_id": "600000.SH"},
                {"instrument_id": "000001.SZ"},
            ),
            "indeterminate": (),
        }
    )
    config = _config(tmp_path)
    report = AnnouncementAssetReadinessService(
        repository=repository, config=config
    ).report(
        now="2026-08-10T01:00:00+00:00",
        persist=True,
    )
    assert report.report_id
    assert report.summary["missing_attachment_count"] == 2
    assert report.summary["estimate_state"] == "available"
    assert report.summary["estimated_required_bytes"] == (
        4 * config.storage.unknown_length_reservation_bytes
    )
    assert report.summary["configuration_fingerprint"] == config.config_fingerprint
    reloaded = repository.list_operational_reports(report_kind="readiness")
    assert reloaded[0]["report_id"] == report.report_id
    assert reloaded[0]["payload"]["summary"] == report.summary


def test_overdue_predecessor_changes_only_cleanup_gates_and_not_intent(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path)
    digest, path = _register_blob(repository, config)
    deletion_id = repository.plan_deletion(
        blob_hash=digest,
        managed_path=str(path),
        predecessor_asset_id="old-asset",
        replacement_asset_id="new-asset",
        replacement_blob_hash="b" * 64,
        reason="effective_replacement",
    )
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE official_asset_deletion_intents SET planned_at=? WHERE deletion_id=?",
            ("2026-08-10T00:00:00+00:00", deletion_id),
        )
    before = repository.get_deletion(deletion_id)
    pin_before = repository.active_retention_pin_count(digest)

    report = AnnouncementAssetReadinessService(
        repository=repository, config=config
    ).report(now="2026-08-10T00:00:21+00:00")

    assert report.status == "degraded"
    assert report.ready_for_reads is True
    assert report.ready_for_daily is True
    assert report.duplicate_cleanup_allowed is False
    assert report.summary["predecessor_cleanup_hard_crossed"] is True
    assert repository.get_deletion(deletion_id) == before
    assert repository.active_retention_pin_count(digest) == pin_before


def test_read_only_integrity_audit_preserves_database_and_file_metadata(tmp_path):
    database = tmp_path / "research.db"
    repository = AnnouncementAssetRepository(database)
    repository.initialize_schema()
    config = _config(tmp_path)
    digest, path = _register_blob(repository, config)
    before_db = hashlib.sha256(database.read_bytes()).hexdigest()
    before = path.stat()

    result = AnnouncementAssetIntegrityAuditService(
        repository=repository, config=config
    ).run(now="2026-08-10T00:00:00+00:00")

    after = path.stat()
    assert result.status == "success"
    assert result.read_only is True
    assert result.valid_count == 1
    assert result.findings == ()
    assert path.read_bytes() == PDF
    assert after.st_mtime_ns == before.st_mtime_ns
    assert after.st_mode == before.st_mode
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before_db
    assert repository.get_blob(digest).integrity_status is IntegrityStatus.VALID


@pytest.mark.parametrize("action", ["link", "move"])
def test_production_link_and_move_repairs_use_exact_canonical_hash_target(
    tmp_path, action
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, trusted=True)
    digest = hashlib.sha256(PDF).hexdigest()
    source = config.adoption_roots[0] / f"legacy-{action}.pdf"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(PDF)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF),
            canonical_path=str(source),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-01T00:00:00+00:00",
            last_verified_at="2026-08-01T00:00:00+00:00",
        )
    )
    operation, _ = repository.create_or_reuse_operation(
        operation_type=INTEGRITY_AUDIT_JOB,
        idempotency_key=f"repair-{action}",
        scope={"content_hashes": [digest]},
        policy_version=config.policy_version,
        owner="operator:test",
        stage=OperationStage.DISCOVERING,
    )
    service = AnnouncementAssetService(repository=repository, config=config)
    handlers = ProductionIntegrityRepairHandlers(
        repository=repository,
        config=config,
        service=service,
        operation_id=operation.operation_id,
        actor="operator:test",
        request_fingerprint=f"fingerprint-{action}",
    )

    result = AnnouncementAssetIntegrityAuditService(
        repository=repository,
        config=config,
        repair_handlers=handlers.as_mapping(),
    ).run(
        content_hashes=(digest,),
        action_flags={action: True},
        operator_authorized=True,
        persist=True,
        operation_id=operation.operation_id,
    )

    canonical = config.blob_root / digest[:2] / f"{digest}.pdf"
    assert result.completed_actions == 1
    assert canonical.read_bytes() == PDF
    assert repository.get_blob(digest).canonical_path == str(canonical)
    assert source.exists() is (action == "link")
    audit = repository.list_job_command_audit(operation_id=operation.operation_id)
    assert {row["command"] for row in audit} >= {
        f"integrity_{action}_planned",
        f"integrity_{action}_completed",
    }


def test_production_quarantine_repair_moves_exact_blob_and_fails_closed(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, trusted=True)
    digest, source = _register_blob(repository, config)
    operation, _ = repository.create_or_reuse_operation(
        operation_type=INTEGRITY_AUDIT_JOB,
        idempotency_key="repair-quarantine",
        scope={"content_hashes": [digest]},
        policy_version=config.policy_version,
        owner="operator:test",
        stage=OperationStage.DISCOVERING,
    )
    service = AnnouncementAssetService(repository=repository, config=config)
    handlers = ProductionIntegrityRepairHandlers(
        repository=repository,
        config=config,
        service=service,
        operation_id=operation.operation_id,
        actor="operator:test",
        request_fingerprint="fingerprint-quarantine",
    )

    AnnouncementAssetIntegrityAuditService(
        repository=repository,
        config=config,
        repair_handlers=handlers.as_mapping(),
    ).run(
        content_hashes=(digest,),
        action_flags={"quarantine": True},
        operator_authorized=True,
        persist=True,
        operation_id=operation.operation_id,
    )

    quarantined = (
        config.quarantine_root
        / f"{digest}.operator_integrity_quarantine.pdf"
    )
    assert not source.exists()
    assert quarantined.read_bytes() == PDF
    blob = repository.get_blob(digest)
    assert blob.canonical_path == str(quarantined)
    assert blob.integrity_status is IntegrityStatus.QUARANTINED


def test_production_quarantine_repair_supports_verified_legacy_alias(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, trusted=True)
    digest = hashlib.sha256(PDF).hexdigest()
    source = config.adoption_roots[0] / "legacy-quarantine.pdf"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(PDF)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF),
            canonical_path=str(source),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-01T00:00:00+00:00",
            last_verified_at="2026-08-01T00:00:00+00:00",
        )
    )
    operation, _ = repository.create_or_reuse_operation(
        operation_type=INTEGRITY_AUDIT_JOB,
        idempotency_key="repair-legacy-quarantine",
        scope={"content_hashes": [digest]},
        policy_version=config.policy_version,
        owner="operator:test",
        stage=OperationStage.DISCOVERING,
    )
    service = AnnouncementAssetService(repository=repository, config=config)
    handlers = ProductionIntegrityRepairHandlers(
        repository=repository,
        config=config,
        service=service,
        operation_id=operation.operation_id,
        actor="operator:test",
        request_fingerprint="fingerprint-legacy-quarantine",
    )

    AnnouncementAssetIntegrityAuditService(
        repository=repository,
        config=config,
        repair_handlers=handlers.as_mapping(),
    ).run(
        content_hashes=(digest,),
        action_flags={"quarantine": True},
        operator_authorized=True,
        persist=True,
        operation_id=operation.operation_id,
    )

    assert not source.exists()
    assert repository.get_blob(digest).canonical_path == str(
        config.quarantine_root / f"{digest}.operator_integrity_quarantine.pdf"
    )


@pytest.mark.parametrize("action", ("move", "quarantine"))
def test_production_file_repair_recovers_after_publish_before_catalog_update(
    tmp_path, action
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, trusted=True)
    digest = hashlib.sha256(PDF).hexdigest()
    source = config.adoption_roots[0] / f"legacy-crash-{action}.pdf"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(PDF)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF),
            canonical_path=str(source),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-01T00:00:00+00:00",
            last_verified_at="2026-08-01T00:00:00+00:00",
        )
    )
    operation, _ = repository.create_or_reuse_operation(
        operation_type=INTEGRITY_AUDIT_JOB,
        idempotency_key=f"repair-crash-{action}",
        scope={"content_hashes": [digest]},
        policy_version=config.policy_version,
        owner="operator:test",
        stage=OperationStage.DISCOVERING,
    )
    target = (
        config.quarantine_root
        / f"{digest}.operator_integrity_quarantine.pdf"
        if action == "quarantine"
        else config.blob_root / digest[:2] / f"{digest}.pdf"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    source.replace(target)
    handlers = ProductionIntegrityRepairHandlers(
        repository=repository,
        config=config,
        service=AnnouncementAssetService(repository=repository, config=config),
        operation_id=operation.operation_id,
        actor="operator:test",
        request_fingerprint=f"fingerprint-crash-{action}",
    )

    handlers.as_mapping()[action](action, digest, None)

    blob = repository.get_blob(digest)
    assert blob.canonical_path == str(target)
    assert blob.integrity_status is (
        IntegrityStatus.QUARANTINED
        if action == "quarantine"
        else IntegrityStatus.VALID
    )
    assert target.read_bytes() == PDF


def test_running_operation_lease_rejects_same_owner_reentry_until_expiry(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    operation, created = repository.create_or_reuse_operation(
        operation_type="annual_report_asset_daily_update",
        idempotency_key="same-owner-claim",
        scope={"run_cutoff": "2026-08-12T00:00:00+00:00"},
        policy_version="annual-report-v1",
        owner="service:scheduler",
        stage=OperationStage.DISCOVERING,
    )
    assert created is True
    first = repository.claim_operation(
        operation.operation_id,
        lease_owner="service:scheduler",
        lease_expires_at="2099-01-01T00:00:00+00:00",
    )

    with pytest.raises(RuntimeError, match="lease is already held"):
        repository.claim_operation(
            operation.operation_id,
            lease_owner="service:scheduler",
            lease_expires_at="2099-01-01T00:00:01+00:00",
        )

    with repository.transaction() as conn:
        conn.execute(
            "UPDATE official_asset_operations SET lease_expires_at=? "
            "WHERE operation_id=?",
            ("2000-01-01T00:00:00+00:00", first.operation_id),
        )
    reclaimed = repository.claim_operation(
        operation.operation_id,
        lease_owner="service:scheduler",
        lease_expires_at="2099-01-01T00:00:02+00:00",
    )
    assert reclaimed.attempt == first.attempt + 1
    assert reclaimed.lease_generation == first.lease_generation + 1

    with pytest.raises(RuntimeError, match="lease generation mismatch"):
        repository.heartbeat_operation(
            operation.operation_id,
            lease_owner="service:scheduler",
            lease_generation=first.lease_generation,
            lease_expires_at="2099-01-01T00:00:03+00:00",
        )
    with pytest.raises(RuntimeError, match="lease generation mismatch"):
        repository.transition_operation(
            operation.operation_id,
            OperationStatus.COMPLETED,
            outcome=BatchOutcome.SUCCESS,
            expected_lease_owner="service:scheduler",
            expected_lease_generation=first.lease_generation,
        )

    heartbeat = repository.heartbeat_operation(
        operation.operation_id,
        lease_owner="service:scheduler",
        lease_generation=reclaimed.lease_generation,
        lease_expires_at="2099-01-01T00:00:04+00:00",
    )
    completed = repository.transition_operation(
        operation.operation_id,
        OperationStatus.COMPLETED,
        outcome=BatchOutcome.SUCCESS,
        expected_lease_owner="service:scheduler",
        expected_lease_generation=heartbeat.lease_generation,
    )
    assert completed.status is OperationStatus.COMPLETED


def test_operation_lease_generation_migrates_existing_rows(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    operation, _ = repository.create_or_reuse_operation(
        operation_type="annual_report_asset_daily_update",
        idempotency_key="legacy-operation-fence",
        scope={"run_cutoff": "2026-08-12T00:00:00+00:00"},
        policy_version="annual-report-v1",
        owner="service:scheduler",
        stage=OperationStage.DISCOVERING,
    )
    with repository.transaction() as conn:
        conn.execute(
            "ALTER TABLE official_asset_operations DROP COLUMN lease_generation"
        )

    repository.initialize_schema()

    migrated = repository.get_operation(operation.operation_id)
    assert migrated is not None
    assert migrated.lease_generation == 0
    assert migrated.idempotency_key == operation.idempotency_key


def test_integrity_repair_validation_fails_before_handler_or_operation(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, trusted=True)
    digest, _ = _register_blob(repository, config)
    calls: list[tuple[str, str]] = []
    audit = AnnouncementAssetIntegrityAuditService(
        repository=repository,
        config=config,
        repair_handlers={
            "delete": lambda action, content_hash, finding: calls.append(
                (action, content_hash)
            )
        },
    )
    with pytest.raises(PermissionError, match="operator authorization"):
        audit.run(
            content_hashes=(digest,),
            action_flags={"delete": True},
            operator_authorized=False,
        )
    with pytest.raises(ValueError, match="explicit deletion_id targets"):
        audit.run(
            action_flags={"delete": True},
            operator_authorized=True,
        )
    assert calls == []

    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="announcement-assets-config.v1",
    )
    principal = CommandPrincipal(
        principal_id="operator:test",
        permissions=frozenset({"annual_report_assets:operator"}),
    )
    with pytest.raises(ValueError, match="explicit target scope"):
        command.start(
            INTEGRITY_AUDIT_JOB,
            principal=principal,
            trigger_kind="manual",
            action_flags={"delete": True},
        )
    assert repository.list_operations(limit=10) == []


def test_bounded_authorized_integrity_action_is_auditable_operation(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, trusted=True)
    digest, _ = _register_blob(repository, config)
    principal = CommandPrincipal(
        principal_id="operator:test",
        permissions=frozenset({"annual_report_assets:operator"}),
    )
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="announcement-assets-config.v1",
    )
    started = command.start(
        INTEGRITY_AUDIT_JOB,
        principal=principal,
        trigger_kind="manual",
        scope={"content_hashes": [digest]},
        action_flags={"quarantine": True},
    )
    operation = repository.get_operation(started.run_id)
    assert operation is not None
    assert operation.scope["read_only"] is False
    assert operation.scope["action_flags"]["quarantine"] is True
    audit_rows = repository.list_job_command_audit(operation_id=started.run_id)
    assert audit_rows[0]["principal"] == "operator:test"
