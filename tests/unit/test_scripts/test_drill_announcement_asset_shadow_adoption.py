from __future__ import annotations

from pathlib import Path

import pytest

from scripts.dev_validation import drill_announcement_asset_shadow_adoption as drill


def test_shadow_adoption_drill_refuses_production_catalog(tmp_path: Path) -> None:
    project_root = tmp_path
    production = project_root / "data" / "research.db"

    with pytest.raises(ValueError, match="refuses the production"):
        drill.run_drill(
            catalog_db=production,
            financials_db=project_root / "data" / "financials.db",
            project_root=project_root,
        )


def test_shadow_adoption_drill_refuses_hardlink_to_production_catalog(
    tmp_path: Path,
) -> None:
    project_root = tmp_path
    production = project_root / "data" / "research.db"
    production.parent.mkdir(parents=True)
    production.touch()
    hardlink = tmp_path / "shadow.db"
    hardlink.hardlink_to(production)

    with pytest.raises(ValueError, match="refuses the production"):
        drill.run_drill(
            catalog_db=hardlink,
            financials_db=project_root / "data" / "financials.db",
            project_root=project_root,
        )


def test_shadow_adoption_drill_table_counts_use_fixed_allowlist(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported"):
        drill._table_count(tmp_path / "catalog.db", "sqlite_master")


def test_shadow_adoption_drill_requires_temporary_catalog_root(tmp_path: Path) -> None:
    project_root = Path("/home/python/Quote")
    catalog = project_root / "data" / "filings" / "unsafe-shadow.db"

    with pytest.raises(ValueError, match="temporary root"):
        drill.run_drill(
            catalog_db=catalog,
            financials_db=tmp_path / "financials.db",
            project_root=project_root,
        )


def test_shadow_adoption_drill_fails_closed_without_manifest(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="legacy_manifest_input_unavailable"):
        drill.run_drill(
            catalog_db=tmp_path / "shadow.db",
            financials_db=tmp_path / "missing.db",
            project_root=tmp_path,
        )
