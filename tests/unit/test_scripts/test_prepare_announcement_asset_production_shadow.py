from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from research.announcement_assets import AnnouncementAssetConfig
from scripts.dev_validation import prepare_announcement_asset_production_shadow as prepare


PDF = b"%PDF-production-shadow-fixture"


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _config(project_root: Path) -> AnnouncementAssetConfig:
    backup_root = project_root / "backup"
    backup_root.mkdir(parents=True, exist_ok=True)
    business = project_root / "data/filings/business_profile"
    broker = project_root / "data/filings/financial_statements/broker_risk_control"
    business.mkdir(parents=True, exist_ok=True)
    broker.mkdir(parents=True, exist_ok=True)
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": False,
            "scheduled_enabled": False,
            "dry_run": True,
            "capacity_artifact_required": True,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "adoption_roots": [
                    "data/filings/business_profile",
                    "data/filings/financial_statements/broker_risk_control",
                ],
                "require_mount": False,
            },
            "legacy_inventory": {
                "roots": {
                    "business_profile": {
                        "base_root": "data/filings/business_profile",
                        "path_template": "business_profile/{fiscal_year}/{exchange}/",
                    },
                    "broker_risk_control": {
                        "base_root": "data/filings/financial_statements/broker_risk_control",
                        "path_template": "broker_risk_control/{exchange}/{symbol}/",
                    },
                }
            },
            "backup": {
                "enabled": False,
                "mount_root": "backup",
                "destination_root": "backup/announcement_assets",
            },
        },
        project_root=project_root,
    )


def _fixture(project_root: Path, config: AnnouncementAssetConfig):
    production = project_root / "data/research.db"
    production.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(production) as connection:
        connection.execute("CREATE TABLE existing_business(id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO existing_business(value) VALUES('kept')")
    report = (
        project_root
        / "data/filings/business_profile/2025/SSE"
        / f"600000_SH_2025Q4_filing-1_{_digest(PDF)}.pdf"
    )
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_bytes(PDF)
    manifest = {
        "source_file_id": "source-file-1",
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "report_period": "2025-12-31",
        "report_type": "annual_report",
        "filing_id": "filing-1",
        "source": "cninfo",
        "source_url": "https://example.test/report.pdf",
        "archive_path": str(report),
        "content_hash": _digest(PDF),
        "content_length": len(PDF),
        "published_at": "2026-03-30T00:00:00+00:00",
        "downloaded_at": "2026-03-30T01:00:00+00:00",
        "schema_version": "business_profile_source_file_manifest.v1",
        "status": "verified",
    }
    inventory = prepare.AnnouncementArchiveInventory().inventory_registered(
        config=config, manifest_rows=[manifest]
    )
    artifact = project_root / "inventory.json"
    artifact.write_text(
        json.dumps(
            {
                "schema_version": prepare.CAPACITY_SCHEMA_VERSION,
                "read_only": True,
                "network_requests": 0,
                "catalog_writes": 0,
                "adoption_writes": 0,
                "archive_mutations": 0,
                "configuration_fingerprint": config.config_fingerprint,
                "inventory": {
                    "inventory_fingerprint": inventory.inventory_fingerprint,
                    "counts": dict(inventory.counts),
                },
            }
        ),
        encoding="utf-8",
    )
    return production, report, manifest, artifact


def test_simulation_copies_catalog_and_never_changes_production_or_legacy(tmp_path: Path):
    config = _config(tmp_path)
    production, report, manifest, artifact = _fixture(tmp_path, config)
    before_db = prepare._catalog_identity(production)
    before_report = (report.stat().st_mtime_ns, report.read_bytes())

    result = prepare.simulate_production_shadow(
        production_db=production,
        simulation_db=tmp_path / "simulation.db",
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=artifact,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )

    assert result["mode"] == "simulation"
    assert result["apply_eligible"] is True
    assert result["production_catalog"]["writes"] == 0
    assert result["adoption"]["effective_scope_count"] == 1
    assert result["adoption"]["effective_scope_unique"] is True
    assert result["promotion"] == {"run": False, "production_visible_rows_added": 0}
    assert result["archive_mutations"] == {
        "copied": 0,
        "moved": 0,
        "linked": 0,
        "quarantined": 0,
        "deleted": 0,
    }
    assert prepare._catalog_identity(production) == before_db
    assert (report.stat().st_mtime_ns, report.read_bytes()) == before_report
    with sqlite3.connect(production) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='official_announcements'"
        ).fetchone()[0] == 0
        assert connection.execute("SELECT value FROM existing_business").fetchone()[0] == "kept"


def test_simulation_rejects_enabled_module_and_stale_inventory(tmp_path: Path):
    config = _config(tmp_path)
    production, _report, manifest, artifact = _fixture(tmp_path, config)
    with pytest.raises(RuntimeError, match="disabled_dry_run"):
        prepare.simulate_production_shadow(
            production_db=production,
            simulation_db=tmp_path / "enabled.db",
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=artifact,
            config=replace(config, enabled=True),
            project_root=tmp_path,
            manifest_rows=[manifest],
        )
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload["inventory"]["inventory_fingerprint"] = "0" * 64
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="fingerprint_mismatch"):
        prepare.simulate_production_shadow(
            production_db=production,
            simulation_db=tmp_path / "stale.db",
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=artifact,
            config=config,
            project_root=tmp_path,
            manifest_rows=[manifest],
        )


def test_apply_requires_confirmation_and_unchanged_preflight_identity(tmp_path: Path):
    config = _config(tmp_path)
    production, _report, manifest, artifact = _fixture(tmp_path, config)
    preflight_payload = prepare.simulate_production_shadow(
        production_db=production,
        simulation_db=tmp_path / "simulation.db",
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=artifact,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )
    preflight = tmp_path / "preflight.json"
    preflight.write_text(json.dumps(preflight_payload), encoding="utf-8")
    with pytest.raises(PermissionError, match="confirmation"):
        prepare.apply_production_shadow(
            production_db=production,
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=artifact,
            preflight_artifact_path=preflight,
            backup_path=tmp_path / "backup/research.db.bak",
            operator="ops:test",
            confirmation="wrong",
            config=config,
            project_root=tmp_path,
            manifest_rows=[manifest],
        )
    with sqlite3.connect(production) as connection:
        connection.execute("INSERT INTO existing_business(value) VALUES('changed')")
    with pytest.raises(RuntimeError, match="changed_since_preflight"):
        prepare.apply_production_shadow(
            production_db=production,
            financials_db=tmp_path / "financials.db",
            inventory_artifact_path=artifact,
            preflight_artifact_path=preflight,
            backup_path=tmp_path / "backup/research.db.bak",
            operator="ops:test",
            confirmation=prepare.CONFIRMATION_TOKEN,
            config=config,
            project_root=tmp_path,
            manifest_rows=[manifest],
        )


def test_apply_writes_only_shadow_rows_without_claiming_required_set(
    tmp_path: Path, monkeypatch
):
    config = _config(tmp_path)
    production, report, manifest, artifact = _fixture(tmp_path, config)
    simulation = tmp_path / "simulation.db"
    preflight_payload = prepare.simulate_production_shadow(
        production_db=production,
        simulation_db=simulation,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=artifact,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )
    preflight = tmp_path / "preflight.json"
    preflight.write_text(json.dumps(preflight_payload), encoding="utf-8")
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
    before_report = (report.stat().st_mtime_ns, report.read_bytes())

    result = prepare.apply_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=artifact,
        preflight_artifact_path=preflight,
        backup_path=tmp_path / "backup/research.db.bak",
        operator="ops:test",
        confirmation=prepare.CONFIRMATION_TOKEN,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )

    assert result["mode"] == "production_shadow"
    assert result["production_readiness"]["ready"] is False
    assert result["production_readiness"]["shadow_registration_ready"] is True
    assert result["production_readiness"]["promotion_ready"] is False
    assert result["promotion"]["production_visible_rows_added"] == 0
    assert result["required_set_evidence"]["status"] == "not_measured"
    assert "verified backup" in result["required_set_evidence"]["reason"]
    assert (report.stat().st_mtime_ns, report.read_bytes()) == before_report
    repository = prepare.AnnouncementAssetRepository(production)
    assert repository.list_effective_reports() == []
    assert len(repository.list_effective_reports(include_shadow=True)) == 1
    with sqlite3.connect(production) as connection:
        assert connection.execute("SELECT value FROM existing_business").fetchall() == [("kept",)]
    assert (tmp_path / "backup/research.db.bak").is_file()
