from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import research.announcement_assets.capacity_artifact as capacity_module
from research.announcement_assets import (
    CAPACITY_ARTIFACT_SCHEMA_VERSION,
    AnnouncementAssetConfig,
    validate_capacity_artifact,
)
from scripts.dev_validation import finalize_announcement_asset_capacity as finalize
from scripts.dev_validation.drill_announcement_asset_shadow_adoption import (
    SCHEMA_VERSION as SHADOW_SCHEMA_VERSION,
)


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": False,
            "capacity_artifact_required": True,
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


def _identity(host: str, path: str, device: int) -> dict[str, object]:
    return {
        "path": path,
        "device": device,
        "filesystem_id": f"{device}:1",
        "mount_source": f"{host}:/archive",
        "mount_target": path,
        "filesystem_type": "nfs4",
        "backing_mount": {
            "mount_source": f"{host}:/archive",
            "mount_target": path,
            "filesystem_type": "nfs4",
            "read_write": True,
        },
    }


def _inventory(config: AnnouncementAssetConfig) -> dict[str, object]:
    usage = {"total_bytes": 10_000, "used_bytes": 1_000, "free_bytes": 9_000}
    return {
        "schema_version": CAPACITY_ARTIFACT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
        "network_requests": 0,
        "catalog_writes": 0,
        "adoption_writes": 0,
        "archive_mutations": 0,
        "configuration_fingerprint": config.config_fingerprint,
        "inventory": {
            "inventory_fingerprint": "a" * 64,
            "counts": {"adoptable": 3},
        },
        "active_universe": {"status": "complete", "total": 3},
        "primary_archive": {
            "identity": _identity("primary", "/archive", 10),
            "failure_domain_identity": "mount_host:primary",
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
            "identity": _identity("backup", "/backup", 11),
            "failure_domain_identity": "mount_host:backup",
            "usage": dict(usage),
        },
        "planning": {
            "status": "incomplete_pending_operator_estimates_and_approval",
            "attachment_limit_bytes": 200,
            "unknown_length_reservation_bytes": 160,
            "attachment_limit_within_observed_max": True,
            "expected_annual_growth_bytes": 300,
            "stress_annual_growth_bytes": 600,
            "estimated_temporary_peak_bytes": 400,
            "estimated_full_market_required_bytes": 600,
            "old_plus_new_replacement_peak_basis": "two_distinct_attachment_versions",
            "estimated_old_plus_new_replacement_peak_bytes": 1_200,
        },
    }


def _shadow(*, fingerprint: str = "a" * 64) -> dict[str, object]:
    return {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "read_only_legacy_inputs": True,
        "network_requests": 0,
        "production_catalog_writes": 0,
        "archive_mutations": {
            "moved": 0,
            "linked": 0,
            "quarantined": 0,
            "deleted": 0,
        },
        "inventory": {
            "fingerprint": fingerprint,
            "counts": {"adoptable": 3},
        },
        "adoption": {
            "effective_scope_count": 3,
            "effective_scope_unique": True,
            "review_required_periods": [],
        },
        "catalog_counts": {"effective_reports": 3},
        "production_readiness": {"ready": True},
    }


def _required_set_evidence(config: AnnouncementAssetConfig) -> dict[str, object]:
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
        finalize,
        "measure_required_set_evidence",
        lambda config: _required_set_evidence(config),
    )
    monkeypatch.setattr(
        capacity_module,
        "measure_required_set_evidence",
        lambda config: _required_set_evidence(config),
    )


def _write(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_finalize_binds_shadow_and_operator_inputs_then_reuses_runtime_validator(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    shadow_path = tmp_path / "shadow.json"
    required_set_path = tmp_path / "required-set.json"
    output_path = tmp_path / "approved.json"
    _write(inventory_path, _inventory(config))
    _write(shadow_path, _shadow())
    _write(required_set_path, _required_set_evidence(config))

    payload = finalize.finalize_capacity_artifact(
        inventory_path=inventory_path,
        shadow_path=shadow_path,
        output_path=output_path,
        config=config,
        planning_horizon_years=3,
        budget_basis="stress",
        required_set_evidence_path=required_set_path,
        approver="operations:capacity-owner",
    )

    assert payload["planning"]["status"] == "approved"
    assert payload["planning"]["estimated_primary_headroom_bytes"] == 6_100
    approval = validate_capacity_artifact(config, artifact_path=output_path)
    assert approval is not None
    assert approval.approver == "operations:capacity-owner"
    assert payload["generated_at"] == json.loads(
        inventory_path.read_text(encoding="utf-8")
    )["generated_at"]


def test_finalize_rejects_mismatched_shadow_without_creating_output(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    shadow_path = tmp_path / "shadow.json"
    required_set_path = tmp_path / "required-set.json"
    output_path = tmp_path / "approved.json"
    _write(inventory_path, _inventory(config))
    _write(shadow_path, _shadow(fingerprint="b" * 64))
    _write(required_set_path, _required_set_evidence(config))

    with pytest.raises(ValueError, match="fingerprint_mismatch"):
        finalize.finalize_capacity_artifact(
            inventory_path=inventory_path,
            shadow_path=shadow_path,
            output_path=output_path,
            config=config,
            planning_horizon_years=3,
            budget_basis="stress",
            required_set_evidence_path=required_set_path,
            approver="operations:capacity-owner",
        )

    assert output_path.exists() is False


def test_finalize_rejects_temporary_shadow_drill_without_creating_output(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    shadow_path = tmp_path / "shadow.json"
    required_set_path = tmp_path / "required-set.json"
    output_path = tmp_path / "approved.json"
    shadow = _shadow()
    shadow["production_readiness"] = {
        "ready": False,
        "reason": "temporary drill is not production shadow adoption",
    }
    _write(inventory_path, _inventory(config))
    _write(shadow_path, shadow)
    _write(required_set_path, _required_set_evidence(config))

    with pytest.raises(ValueError, match="shadow_not_production_ready"):
        finalize.finalize_capacity_artifact(
            inventory_path=inventory_path,
            shadow_path=shadow_path,
            output_path=output_path,
            config=config,
            planning_horizon_years=3,
            budget_basis="stress",
            required_set_evidence_path=required_set_path,
            approver="operations:capacity-owner",
        )

    assert output_path.exists() is False


def test_finalize_rejects_mismatched_required_set_evidence(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    shadow_path = tmp_path / "shadow.json"
    required_set_path = tmp_path / "required-set.json"
    output_path = tmp_path / "approved.json"
    required_set = _required_set_evidence(config)
    required_set["primary_required_set"]["bytes"] = 699
    _write(inventory_path, _inventory(config))
    _write(shadow_path, _shadow())
    _write(required_set_path, required_set)

    with pytest.raises(ValueError, match="required_set_evidence_mismatch"):
        finalize.finalize_capacity_artifact(
            inventory_path=inventory_path,
            shadow_path=shadow_path,
            output_path=output_path,
            config=config,
            planning_horizon_years=3,
            budget_basis="stress",
            required_set_evidence_path=required_set_path,
            approver="operations:capacity-owner",
        )

    assert output_path.exists() is False


def test_finalize_validates_private_file_before_publication(
    tmp_path: Path, monkeypatch
) -> None:
    config = _config(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    shadow_path = tmp_path / "shadow.json"
    required_set_path = tmp_path / "required-set.json"
    output_path = tmp_path / "approved.json"
    _write(inventory_path, _inventory(config))
    _write(shadow_path, _shadow())
    _write(required_set_path, _required_set_evidence(config))
    observed_paths: list[Path] = []
    runtime_validate = validate_capacity_artifact

    def validate_before_publish(config, *, artifact_path, **kwargs):
        assert output_path.exists() is False
        assert artifact_path != output_path
        observed_paths.append(artifact_path)
        return runtime_validate(config, artifact_path=artifact_path, **kwargs)

    monkeypatch.setattr(finalize, "validate_capacity_artifact", validate_before_publish)

    finalize.finalize_capacity_artifact(
        inventory_path=inventory_path,
        shadow_path=shadow_path,
        output_path=output_path,
        config=config,
        planning_horizon_years=3,
        budget_basis="stress",
        required_set_evidence_path=required_set_path,
        approver="operations:capacity-owner",
    )

    assert len(observed_paths) == 1
    assert output_path.is_file()
