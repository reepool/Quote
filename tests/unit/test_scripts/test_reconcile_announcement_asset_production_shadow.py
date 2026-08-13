from __future__ import annotations

import json
from pathlib import Path

from tests.unit.test_scripts.test_prepare_announcement_asset_production_shadow import (
    _config,
    _fixture,
)
from scripts.dev_validation import (
    prepare_announcement_asset_production_shadow as prepare,
    reconcile_announcement_asset_production_shadow as reconcile,
)


def test_reconciliation_without_custody_evidence_stays_pending(tmp_path: Path, monkeypatch):
    config = _config(tmp_path)
    production, _report, manifest, inventory_artifact = _fixture(tmp_path, config)
    preflight = prepare.simulate_production_shadow(
        production_db=production,
        simulation_db=tmp_path / "simulation.db",
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory_artifact,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )
    preflight_path = tmp_path / "preflight.json"
    preflight_path.write_text(json.dumps(preflight), encoding="utf-8")
    monkeypatch.setattr(
        prepare,
        "probe_mount_identity",
        lambda path: type(
            "Identity",
            (),
            {
                "mount_point": Path(path),
                "read_write": True,
                "source": "test-backup",
                "filesystem_key": "test-backup|mount|1",
            },
        )(),
    )
    shadow = prepare.apply_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory_artifact,
        preflight_artifact_path=preflight_path,
        backup_path=tmp_path / "backup/research.db.bak",
        operator="ops:test",
        confirmation=prepare.CONFIRMATION_TOKEN,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )
    shadow_path = tmp_path / "shadow.json"
    shadow_path.write_text(json.dumps(shadow), encoding="utf-8")

    result = reconcile.reconcile_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory_artifact,
        shadow_artifact_path=shadow_path,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
        operator="ops:test",
        confirmation=reconcile.CONFIRMATION_TOKEN,
    )

    assert result["production_readiness"]["ready"] is False
    assert result["production_readiness"]["reconciliation_ready"] is False
    assert result["reconciliation"]["pending_custody_count"] == 1
    assert result["reconciliation"]["periods"][0]["status"] == "custody_pending"
    assert result["required_set_evidence"]["status"] == "not_measured"
    assert result["promotion"]["run"] is False


def test_reconciliation_requires_explicit_operator_confirmation(tmp_path: Path):
    config = _config(tmp_path)
    production, _report, manifest, inventory_artifact = _fixture(tmp_path, config)
    shadow_path = tmp_path / "shadow.json"
    shadow_path.write_text(
        json.dumps(
            {
                "schema_version": reconcile.SHADOW_SCHEMA_VERSION,
                "mode": "production_shadow",
                "configuration_fingerprint": config.config_fingerprint,
                "production_readiness": {"ready": False, "shadow_registration_ready": True},
                "inventory": {"fingerprint": "unused"},
            }
        ),
        encoding="utf-8",
    )
    import pytest

    with pytest.raises(PermissionError, match="confirmation"):
        reconcile.reconcile_production_shadow(
            production_db=production,
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=inventory_artifact,
            shadow_artifact_path=shadow_path,
            config=config,
            project_root=tmp_path,
            manifest_rows=[manifest],
            operator="ops:test",
            confirmation="wrong",
        )
