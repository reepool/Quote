from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from research.announcement_assets import AnnouncementAssetRepository
from scripts.dev_validation import (
    canonicalize_announcement_asset_shadow as canonicalize,
)
from scripts.dev_validation import (
    prepare_announcement_asset_production_shadow as prepare,
)
from tests.unit.test_scripts.test_prepare_announcement_asset_production_shadow import (
    _config,
    _fixture,
)


def _shadow(tmp_path: Path, monkeypatch):
    config = _config(tmp_path)
    config = replace(
        config,
        storage=replace(config.storage, free_space_reserve_bytes=1),
    )
    production, report, manifest, inventory = _fixture(tmp_path, config)
    preflight_payload = prepare.simulate_production_shadow(
        production_db=production,
        simulation_db=tmp_path / "simulation.db",
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory,
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
    prepare.apply_production_shadow(
        production_db=production,
        financials_db=tmp_path / "financials.db",
        inventory_artifact_path=inventory,
        preflight_artifact_path=preflight,
        backup_path=tmp_path / "backup/research.db.bak",
        operator="ops:test",
        confirmation=prepare.CONFIRMATION_TOKEN,
        config=config,
        project_root=tmp_path,
        manifest_rows=[manifest],
    )
    return config, production, report


def test_plan_is_read_only_and_apply_requires_confirmation(tmp_path: Path, monkeypatch):
    config, production, report = _shadow(tmp_path, monkeypatch)
    before = (report.stat().st_mtime_ns, report.read_bytes())

    plan = canonicalize.build_plan(
        production_db=production,
        config=config,
        project_root=tmp_path,
    )

    assert plan["mode"] == "plan"
    assert plan["summary"]["unique_blob_count"] == 1
    assert plan["legacy_archive_mutations"] == 0
    assert not any(config.blob_root.rglob("*.pdf"))
    assert (report.stat().st_mtime_ns, report.read_bytes()) == before
    with pytest.raises(PermissionError, match="operator and confirmation"):
        canonicalize.apply_plan(
            production_db=production,
            plan=plan,
            config=config,
            operator="ops:test",
            confirmation="wrong",
            project_root=tmp_path,
        )


def test_apply_copy_verifies_and_preserves_legacy_file(tmp_path: Path, monkeypatch):
    config, production, report = _shadow(tmp_path, monkeypatch)
    before = (report.stat().st_mtime_ns, report.read_bytes())
    plan = canonicalize.build_plan(
        production_db=production,
        config=config,
        project_root=tmp_path,
    )

    result = canonicalize.apply_plan(
        production_db=production,
        plan=plan,
        config=config,
        operator="ops:test",
        confirmation=canonicalize.CONFIRMATION_TOKEN,
        project_root=tmp_path,
    )

    assert result["canonical_copies_created"] == 1
    assert result["legacy_archive_mutations"] == 0
    assert result["production_visible_rows_added"] == 0
    assert (report.stat().st_mtime_ns, report.read_bytes()) == before
    repository = AnnouncementAssetRepository(production)
    effective = repository.list_effective_reports(include_shadow=True)[0]
    blob = repository.get_blob(effective.content_hash)
    assert blob is not None
    canonical = Path(blob.canonical_path)
    assert canonical.is_file()
    assert canonical.read_bytes() == report.read_bytes()
    assert repository.list_effective_reports() == []


def test_apply_rejects_stale_catalog_path_plan(tmp_path: Path, monkeypatch):
    config, production, _report = _shadow(tmp_path, monkeypatch)
    plan = canonicalize.build_plan(
        production_db=production,
        config=config,
        project_root=tmp_path,
    )
    entry = plan["entries"][0]
    AnnouncementAssetRepository(production).update_blob_path(
        entry["content_hash"], str(tmp_path / "changed.pdf")
    )

    with pytest.raises((FileNotFoundError, RuntimeError, ValueError)):
        canonicalize.apply_plan(
            production_db=production,
            plan=plan,
            config=config,
            operator="ops:test",
            confirmation=canonicalize.CONFIRMATION_TOKEN,
            project_root=tmp_path,
        )


def test_apply_resumes_when_copy_and_path_switch_already_completed(
    tmp_path: Path, monkeypatch
):
    config, production, _report = _shadow(tmp_path, monkeypatch)
    plan = canonicalize.build_plan(
        production_db=production,
        config=config,
        project_root=tmp_path,
    )
    first = canonicalize.apply_plan(
        production_db=production,
        plan=plan,
        config=config,
        operator="ops:test",
        confirmation=canonicalize.CONFIRMATION_TOKEN,
        project_root=tmp_path,
    )
    second = canonicalize.apply_plan(
        production_db=production,
        plan=plan,
        config=config,
        operator="ops:test",
        confirmation=canonicalize.CONFIRMATION_TOKEN,
        project_root=tmp_path,
    )

    assert first["canonical_copies_created"] == 1
    assert second["canonical_copies_created"] == 0
    assert second["canonical_copies_reused"] == 1


def test_apply_rejects_symlink_target_parent_and_insufficient_capacity(
    tmp_path: Path, monkeypatch
):
    config, production, _report = _shadow(tmp_path, monkeypatch)
    plan = canonicalize.build_plan(
        production_db=production,
        config=config,
        project_root=tmp_path,
    )
    target_parent = Path(plan["entries"][0]["target_path"]).parent
    target_parent.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    target_parent.symlink_to(outside, target_is_directory=True)
    with pytest.raises((FileNotFoundError, RuntimeError, ValueError)):
        canonicalize.apply_plan(
            production_db=production,
            plan=plan,
            config=config,
            operator="ops:test",
            confirmation=canonicalize.CONFIRMATION_TOKEN,
            project_root=tmp_path,
        )
    target_parent.unlink()
    monkeypatch.setattr(
        canonicalize.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"free": 0})(),
    )
    with pytest.raises(RuntimeError, match="storage reserve"):
        canonicalize.apply_plan(
            production_db=production,
            plan=plan,
            config=config,
            operator="ops:test",
            confirmation=canonicalize.CONFIRMATION_TOKEN,
            project_root=tmp_path,
        )


def test_blob_path_cas_canonicalizes_a_resolving_alias(tmp_path: Path, monkeypatch):
    config, production, _report = _shadow(tmp_path, monkeypatch)
    plan = canonicalize.build_plan(
        production_db=production,
        config=config,
        project_root=tmp_path,
    )
    result = canonicalize.apply_plan(
        production_db=production,
        plan=plan,
        config=config,
        operator="ops:test",
        confirmation=canonicalize.CONFIRMATION_TOKEN,
        project_root=tmp_path,
    )
    entry = result["entries"][0]
    target = Path(entry["target_path"])
    alias = tmp_path / "blob-alias.pdf"
    alias.symlink_to(target)
    repository = AnnouncementAssetRepository(production)
    repository.update_blob_path(entry["content_hash"], str(alias))

    repository.compare_and_set_blob_path(
        entry["content_hash"],
        expected_path=alias,
        canonical_path=target,
    )

    assert repository.get_blob(entry["content_hash"]).canonical_path == str(
        target.resolve()
    )
