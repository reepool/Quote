from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

import research.announcement_assets.capacity_artifact as capacity_module
import research.announcement_assets.config as config_module
from research.announcement_assets import (
    CAPACITY_ARTIFACT_SCHEMA_VERSION,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    CapacityArtifactNotReadyError,
    IntegrityStatus,
    MountIdentity,
    OfficialDocumentBlob,
    validate_capacity_artifact,
)

_REAL_MEASURE_REQUIRED_SET_EVIDENCE = capacity_module.measure_required_set_evidence


def _config(tmp_path: Path, *, required: bool = True) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": False,
            "capacity_artifact_required": required,
            "capacity_artifact_path": (
                "config/runtime_evidence/official_announcement_asset_capacity.json"
            ),
            "capacity_artifact_max_age_hours": 24,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "storage": {
                "free_space_reserve_bytes": 100,
                "max_attachment_bytes": 200,
                "unknown_length_reservation_bytes": 160,
            },
            "backup": {"free_space_reserve_bytes": 100},
        },
        project_root=tmp_path,
    )


def _approved_artifact(config: AnnouncementAssetConfig) -> dict:
    identity = {
        "path": "/archive",
        "device": 10,
        "filesystem_id": "10:1",
        "mount_target": "/archive",
        "filesystem_type": "nfs4",
        "mount_source": "test-nfs:/archive",
        "backing_mount": {
            "mount_source": "test-nfs:/archive",
            "mount_target": "/archive",
            "filesystem_type": "nfs4",
            "read_write": True,
        },
    }
    usage = {"total_bytes": 10_000, "used_bytes": 1_000, "free_bytes": 9_000}
    required_set_evidence = _required_set_evidence(config)
    return {
        "schema_version": CAPACITY_ARTIFACT_SCHEMA_VERSION,
        "generated_at": "2026-08-12T10:00:00+00:00",
        "read_only": True,
        "configuration_fingerprint": config.config_fingerprint,
        "active_universe": {"status": "complete", "total": 3},
        "primary_archive": {
            "identity": identity,
            "failure_domain_identity": "mount_host:test-nfs",
            "usage": dict(usage),
            "pdf_distribution": {
                "scope": "manifest_verified_annual_report_candidates",
                "file_count": 3,
                "total_bytes": 430,
                "p95_bytes": 100,
                "p99_bytes": 150,
                "max_bytes": 180,
            },
        },
        "backup_target": {
            "status": "available",
            "failure_domain_identity": "mount_host:backup-nfs",
            "identity": {
                "path": "/backup",
                "device": 11,
                "filesystem_id": "11:1",
                "mount_target": "/backup",
                "filesystem_type": "nfs4",
                "mount_source": "backup-nfs:/archive",
                "backing_mount": {
                    "mount_source": "backup-nfs:/archive",
                    "mount_target": "/backup",
                    "filesystem_type": "nfs4",
                    "read_write": True,
                },
            },
            "usage": dict(usage),
        },
        "planning": {
            "status": "approved",
            "attachment_limit_bytes": 200,
            "unknown_length_reservation_bytes": 160,
            "attachment_limit_within_observed_max": True,
            "planning_horizon_years": 3,
            "approved_budget_basis": "stress",
            "estimated_primary_headroom_bytes": 6_100,
            "estimated_backup_headroom_bytes": 6_100,
            "expected_annual_growth_bytes": 300,
            "stress_annual_growth_bytes": 600,
            "estimated_temporary_peak_bytes": 400,
            "estimated_full_market_required_bytes": 600,
            "old_plus_new_replacement_peak_basis": "two_distinct_attachment_versions",
            "estimated_old_plus_new_replacement_peak_bytes": 1_200,
            "primary_required_set_actual_bytes": 700,
            "backup_required_set_actual_bytes": 700,
            "permanently_retained_recovery_manifest_bytes": 0,
            "explicit_approver": "operations:capacity-owner",
        },
        "required_set_evidence": required_set_evidence,
    }


def _required_set_evidence(config: AnnouncementAssetConfig) -> dict:
    return {
        "schema_version": "official_announcement_asset_required_set_evidence.v1",
        "catalog_path": "data/research.db",
        "configuration_fingerprint": config.config_fingerprint,
        "primary_required_set": {
            "count": 1,
            "bytes": 700,
            "fingerprint": "a" * 64,
        },
        "backup_verified_set": {
            "count": 1,
            "bytes": 700,
            "fingerprint": "a" * 64,
        },
        "permanent_recovery_set": {
            "count": 0,
            "bytes": 0,
            "fingerprint": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
    }


@pytest.fixture(autouse=True)
def _stub_required_set_measurement(monkeypatch):
    monkeypatch.setattr(
        capacity_module,
        "measure_required_set_evidence",
        lambda config: _required_set_evidence(config),
    )


def _write(config: AnnouncementAssetConfig, payload: dict) -> None:
    config.capacity_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    config.capacity_artifact_path.write_text(
        json.dumps(payload), encoding="utf-8"
    )


def test_capacity_artifact_is_optional_for_nonproduction_test_configs(tmp_path):
    assert validate_capacity_artifact(_config(tmp_path, required=False)) is None


def test_capacity_artifact_requires_fresh_approved_field_complete_evidence(tmp_path):
    config = _config(tmp_path)
    payload = _approved_artifact(config)
    _write(config, payload)

    approval = validate_capacity_artifact(
        config,
        now=datetime(2026, 8, 12, 11, tzinfo=timezone.utc),
    )

    assert approval is not None
    assert approval.approver == "operations:capacity-owner"
    assert approval.primary_required_set_actual_bytes == 700
    assert approval.backup_required_set_actual_bytes == 700


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda payload: payload.update(schema_version="v3"), "schema_mismatch"),
        (
            lambda payload: payload.update(configuration_fingerprint="0" * 64),
            "config_mismatch",
        ),
        (
            lambda payload: payload["planning"].update(status="incomplete"),
            "not_approved",
        ),
        (
            lambda payload: payload["planning"].update(explicit_approver=None),
            "approver_missing",
        ),
        (
            lambda payload: payload["planning"].update(
                backup_required_set_actual_bytes="699"
            ),
            "backup_required_set_actual_bytes_invalid",
        ),
        (
            lambda payload: payload["planning"].update(
                estimated_backup_headroom_bytes=6_099
            ),
            "backup_headroom_mismatch",
        ),
        (
            lambda payload: payload["backup_target"]["usage"].update(
                free_bytes=2_000
            ),
            "backup_capacity_insufficient",
        ),
        (
            lambda payload: payload["planning"].update(
                attachment_limit_bytes="200"
            ),
            "attachment_limit_bytes_invalid",
        ),
        (
            lambda payload: payload["backup_target"]["identity"].pop(
                "backing_mount"
            ),
            "backing_mount_missing",
        ),
        (
            lambda payload: payload["backup_target"].update(
                failure_domain_identity="backup-nas"
            ),
            "backup_failure_domain_unverified",
        ),
    ],
)
def test_capacity_artifact_fails_closed_on_invalid_approval(
    tmp_path, mutation, reason
):
    config = _config(tmp_path)
    payload = _approved_artifact(config)
    mutation(payload)
    _write(config, payload)

    with pytest.raises(CapacityArtifactNotReadyError, match=reason):
        validate_capacity_artifact(
            config,
            now=datetime(2026, 8, 12, 11, tzinfo=timezone.utc),
        )


def test_capacity_artifact_rejects_missing_and_expired_files(tmp_path):
    config = _config(tmp_path)
    with pytest.raises(CapacityArtifactNotReadyError, match="missing"):
        validate_capacity_artifact(config)

    _write(config, _approved_artifact(config))
    with pytest.raises(CapacityArtifactNotReadyError, match="expired"):
        validate_capacity_artifact(
            config,
            now=datetime(2026, 8, 14, 11, tzinfo=timezone.utc),
        )


def test_production_capacity_gate_requires_bound_rollout_evidence(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    payload = _approved_artifact(config)
    _write(config, payload)
    monkeypatch.setattr(
        capacity_module,
        "_requires_production_rollout_evidence",
        lambda config: True,
    )

    with pytest.raises(
        CapacityArtifactNotReadyError, match="approval_missing"
    ):
        validate_capacity_artifact(
            config, now=datetime(2026, 8, 12, 11, tzinfo=timezone.utc)
        )


def test_capacity_artifact_path_cannot_escape_runtime_evidence_root(tmp_path):
    with pytest.raises(ValueError, match="capacity_artifact_path"):
        AnnouncementAssetConfig.from_mapping(
            {
                "capacity_artifact_path": "data/filings/capacity.json",
                "paths": {
                    "filings_root": "data/filings",
                    "archive_root": "data/filings/announcements",
                    "temp_root": "data/filings/announcements/tmp",
                    "quarantine_root": "data/filings/announcements/quarantine",
                    "require_mount": False,
                },
            },
            project_root=tmp_path,
        )


def test_production_mount_writes_cannot_disable_capacity_artifact(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(config_module, "_is_isolated_test_root", lambda root: False)
    with pytest.raises(ValueError, match="writes require capacity artifact"):
        AnnouncementAssetConfig.from_mapping(
            {
                "enabled": True,
                "dry_run": False,
                "capacity_artifact_required": False,
                "paths": {
                    "filings_root": "data/filings",
                    "archive_root": "data/filings/announcements",
                    "temp_root": "data/filings/announcements/tmp",
                    "quarantine_root": "data/filings/announcements/quarantine",
                    "require_mount": True,
                },
            },
            project_root=tmp_path,
        )


def test_measure_required_set_evidence_recomputes_catalog_and_verified_backup(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "data/research.db")
    repository.initialize_schema()
    hashes = ("a" * 64, "b" * 64, "c" * 64)
    for digest, length in zip(hashes, (100, 200, 300), strict=True):
        repository.register_blob(
            OfficialDocumentBlob(
                content_hash=digest,
                content_length=length,
                canonical_path=str(tmp_path / f"{digest}.pdf"),
                signature_status="valid_pdf",
                integrity_status=IntegrityStatus.VALID,
                first_available_at="2026-08-12T00:00:00+00:00",
                last_verified_at="2026-08-12T00:00:00+00:00",
            )
        )
    repository.add_retention_pin(
        blob_hash=hashes[0], pin_type="consumer", pin_key="first"
    )
    repository.add_retention_pin(
        blob_hash=hashes[1], pin_type="consumer", pin_key="second"
    )
    with repository.transaction() as connection:
        connection.execute(
            """INSERT INTO official_asset_recovery_manifest(
                   recovery_id, schema_version, manifest_kind, manifest_version,
                   prior_path, content_hash, backup_object,
                   file_manifest_watermark, recovery_pair_id,
                   active_indefinitely, created_at, created_by
               ) VALUES('recovery-1', 'official_asset_recovery_manifest.v1',
                        'correction_predecessor', 1, '/legacy/third.pdf', ?,
                        'blobs/third.pdf', 'files-1', 'pair-1', 1,
                        '2026-08-12T00:00:00+00:00', 'test')""",
            (hashes[2],),
        )
        for digest, fingerprint, length in (
            (hashes[0], config.config_fingerprint, 100),
            (hashes[1], config.config_fingerprint, 199),
            (hashes[2], config.config_fingerprint, 300),
        ):
            connection.execute(
                """INSERT INTO official_asset_backup_state(
                       content_hash, config_fingerprint, destination_identity,
                       content_length, status, file_manifest_watermark,
                       catalog_snapshot_watermark, verified_at, created_at, updated_at
                   ) VALUES(?, ?, 'backup', ?, 'verified', 'files-1',
                            'catalog-1', '2026-08-12T00:00:00+00:00',
                            '2026-08-12T00:00:00+00:00',
                            '2026-08-12T00:00:00+00:00')""",
                (digest, fingerprint, length),
            )

    evidence = _REAL_MEASURE_REQUIRED_SET_EVIDENCE(config)

    required_fingerprint = hashlib.sha256("\n".join(hashes).encode("ascii")).hexdigest()
    assert evidence["primary_required_set"] == {
        "count": 3,
        "bytes": 600,
        "fingerprint": required_fingerprint,
    }
    assert evidence["backup_verified_set"] == {
        "count": 2,
        "bytes": 400,
        "fingerprint": hashlib.sha256(
            f"{hashes[0]}\n{hashes[2]}".encode("ascii")
        ).hexdigest(),
    }
    assert evidence["permanent_recovery_set"] == {
        "count": 1,
        "bytes": 300,
        "fingerprint": hashlib.sha256(hashes[2].encode("ascii")).hexdigest(),
    }


def test_capacity_artifact_rejects_runtime_mount_read_only_or_filesystem_change(
    tmp_path, monkeypatch
):
    config = AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": False,
            "capacity_artifact_required": True,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": True,
                "expected_mount_source": "test-nfs:/archive",
            },
            "storage": {
                "free_space_reserve_bytes": 100,
                "max_attachment_bytes": 200,
                "unknown_length_reservation_bytes": 160,
            },
            "backup": {
                "enabled": True,
                "mount_root": "data/backup",
                "destination_root": "data/backup/announcement_assets",
                "expected_mount_source": "backup-nfs:/archive",
                "expected_failure_domain": "backup-nas",
                "free_space_reserve_bytes": 100,
            },
        },
        project_root=tmp_path,
    )
    payload = _approved_artifact(config)
    payload["backup_target"]["configured_failure_domain_label"] = "backup-nas"
    _write(config, payload)
    primary = MountIdentity(
        requested_path=config.filings_root,
        mount_point=Path("/archive"),
        source="test-nfs:/archive",
        fs_type="nfs4",
        device_id=10,
        filesystem_id="10:1",
    )
    backup = MountIdentity(
        requested_path=config.backup.mount_root,
        mount_point=Path("/backup"),
        source="backup-nfs:/archive",
        fs_type="nfs4",
        device_id=11,
        filesystem_id="11:1",
    )
    monkeypatch.setattr(
        capacity_module,
        "validate_backup_mount",
        lambda config, *, require_enabled=True: backup,
    )
    monkeypatch.setattr(capacity_module, "_current_free_bytes", lambda path: 9_000)

    monkeypatch.setattr(
        capacity_module,
        "probe_mount_identity",
        lambda path: replace(primary, read_write=False),
    )
    with pytest.raises(CapacityArtifactNotReadyError, match="runtime_not_read_write"):
        validate_capacity_artifact(
            config, now=datetime(2026, 8, 12, 11, tzinfo=timezone.utc)
        )

    monkeypatch.setattr(
        capacity_module,
        "probe_mount_identity",
        lambda path: replace(primary, filesystem_id="10:2"),
    )
    with pytest.raises(CapacityArtifactNotReadyError, match="runtime_identity_mismatch"):
        validate_capacity_artifact(
            config, now=datetime(2026, 8, 12, 11, tzinfo=timezone.utc)
        )
