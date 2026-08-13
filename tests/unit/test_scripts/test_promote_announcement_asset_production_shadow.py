from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.dev_validation import (
    prepare_announcement_asset_production_shadow as prepare,
    promote_announcement_asset_production_shadow as promote,
    reconcile_announcement_asset_production_shadow as reconcile,
)
from tests.unit.test_scripts.test_prepare_announcement_asset_production_shadow import (
    _config,
    _fixture,
)


def _reconciled_fixture(tmp_path: Path, monkeypatch):
    config = _config(tmp_path)
    production, report, manifest, inventory_artifact = _fixture(tmp_path, config)
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
    identity = type(
        "Identity",
        (),
        {
            "mount_point": tmp_path / "backup",
            "read_write": True,
            "source": "test-backup",
            "filesystem_key": "test-backup|mount|1",
        },
    )()
    monkeypatch.setattr(prepare, "probe_mount_identity", lambda _path: identity)
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
    custody = {
        "by_path": {
            str(report.resolve()): {
                "path": str(report.resolve()),
                "content_hash": manifest["content_hash"],
                "mount_filesystem_key": identity.filesystem_key,
                "config_fingerprint": config.config_fingerprint,
                "custody_mode": "exact_path_excluded",
                "legacy_writer_excludes_exact_path": True,
                "legacy_cleaner_excludes_exact_path": True,
                "verified_at": datetime.now(timezone.utc).isoformat(),
                "evidence_ref": "test-exact-path-exclusion.v1",
            }
        }
    }
    custody_path = tmp_path / "custody.json"
    custody_path.write_text(json.dumps(custody), encoding="utf-8")
    monkeypatch.setattr(
        "research.announcement_assets.migration.probe_mount_identity",
        lambda _path: identity,
    )
    reconciled = reconcile.reconcile_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory_artifact,
        shadow_artifact_path=shadow_path,
        custody_evidence_path=custody_path,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
        operator="ops:test",
        confirmation=reconcile.CONFIRMATION_TOKEN,
    )
    reconciliation_path = tmp_path / "reconciliation.json"
    reconciliation_path.write_text(json.dumps(reconciled), encoding="utf-8")
    return config, production, report, manifest, inventory_artifact, reconciliation_path


def test_promotion_revalidates_and_changes_only_catalog_visibility(tmp_path, monkeypatch):
    (
        config,
        production,
        report,
        manifest,
        inventory_artifact,
        reconciliation_path,
    ) = _reconciled_fixture(tmp_path, monkeypatch)
    before_file = (report.read_bytes(), report.stat().st_mtime_ns)

    result = promote.promote_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory_artifact,
        reconciliation_artifact_path=reconciliation_path,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
        operator="ops:test",
        confirmation=promote.CONFIRMATION_TOKEN,
    )

    assert result["promotion"]["production_visible_rows_added"] == 1
    assert result["production_readiness"]["ready"] is False
    assert result["required_set_evidence"]["status"] == "not_measured"
    assert result["network_requests"] == 0
    assert set(result["archive_mutations"].values()) == {0}
    assert (report.read_bytes(), report.stat().st_mtime_ns) == before_file
    repository = prepare.AnnouncementAssetRepository(production)
    assert len(repository.list_effective_reports()) == 1

    replay = promote.promote_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory_artifact,
        reconciliation_artifact_path=reconciliation_path,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
        operator="ops:test",
        confirmation=promote.CONFIRMATION_TOKEN,
    )
    assert replay["promotion"]["production_visible_rows_added"] == 0
    assert replay["promotion"]["replayed"] is True
    assert len(repository.list_effective_reports()) == 1


def test_promotion_requires_confirmation_and_current_inventory(tmp_path, monkeypatch):
    (
        config,
        production,
        _report,
        manifest,
        inventory_artifact,
        reconciliation_path,
    ) = _reconciled_fixture(tmp_path, monkeypatch)
    with pytest.raises(PermissionError, match="confirmation"):
        promote.promote_production_shadow(
            production_db=production,
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=inventory_artifact,
            reconciliation_artifact_path=reconciliation_path,
            config=config,
            project_root=tmp_path,
            manifest_rows=[manifest],
            operator="ops:test",
            confirmation="wrong",
        )

    payload = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    payload["inventory_fingerprint"] = "0" * 64
    reconciliation_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="inventory_changed"):
        promote.promote_production_shadow(
            production_db=production,
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=inventory_artifact,
            reconciliation_artifact_path=reconciliation_path,
            config=config,
            project_root=tmp_path,
            manifest_rows=[manifest],
            operator="ops:test",
            confirmation=promote.CONFIRMATION_TOKEN,
        )
