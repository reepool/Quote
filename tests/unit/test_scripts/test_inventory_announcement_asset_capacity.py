from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.dev_validation import inventory_announcement_asset_capacity as inventory


def test_capacity_artifact_marks_missing_backup_and_approval_incomplete(
    tmp_path: Path, monkeypatch
) -> None:
    filings = tmp_path / "data" / "filings"
    filings.mkdir(parents=True)
    quotes = tmp_path / "quotes.db"
    with sqlite3.connect(quotes) as connection:
        connection.execute(
            "CREATE TABLE instruments ("
            "instrument_id TEXT, exchange TEXT, is_active INTEGER, type TEXT)"
        )
        connection.executemany(
            "INSERT INTO instruments VALUES (?, ?, 1, 'stock')",
            [
                ("600000.SH", "SSE"),
                ("000001.SZ", "SZSE"),
                ("920001.BJ", "BSE"),
            ],
        )

    config = type(
        "Config",
        (),
        {
            "filings_root": filings,
            "backup": type("Backup", (), {"mount_root": tmp_path / "missing"})(),
            "storage": type(
                "Storage",
                (), {"max_attachment_bytes": 100, "unknown_length_reservation_bytes": 10},
            )(),
            "acquisition": type(
                "Acquisition",
                (),
                {"max_task_download_bytes": 1000, "download_concurrency": 2},
            )(),
            "config_fingerprint": "a" * 64,
        },
    )()
    report = type(
        "Report",
        (),
        {
            "files_seen": 0,
            "manifest_rows_seen": 0,
            "items": (),
            "inventory_fingerprint": "b" * 64,
            "root_registry_version": "test.v1",
            "out_of_scope_directories": (),
        },
    )()
    monkeypatch.setattr(
        inventory.AnnouncementAssetConfig,
        "from_research_config",
        lambda *args, **kwargs: config,
    )
    monkeypatch.setattr(
        inventory.config_manager,
        "get_research_config",
        lambda: object(),
    )
    monkeypatch.setattr(
        inventory.AnnouncementArchiveInventory,
        "inventory_registered",
        lambda self, **kwargs: report,
    )
    monkeypatch.setattr(
        inventory,
        "_read_legacy_manifest_rows",
        lambda **kwargs: ([{"source_file_id": "manifest-1"}], {
            "status": "complete",
            "rows_loaded": 1,
        }),
    )

    artifact = inventory.build_artifact(project_root=tmp_path, quotes_db=quotes)

    assert artifact["read_only"] is True
    assert artifact["catalog_writes"] == artifact["archive_mutations"] == 0
    assert artifact["active_universe"]["status"] == "complete"
    assert artifact["backup_target"] == {
        "status": "unavailable",
        "reason": "backup_mount_root_missing",
    }
    assert artifact["planning"]["status"] == (
        "incomplete_pending_operator_estimates_and_approval"
    )
    assert artifact["planning"]["primary_required_set_actual_bytes"] is None
    assert artifact["planning"]["backup_required_set_actual_bytes"] is None
    assert artifact["planning"]["explicit_approver"] is None
    assert artifact["active_universe"]["candidate_coverage"] == {
        "latest_fiscal_year": None,
        "candidate_instrument_count": 0,
        "candidate_active_instrument_count": 0,
        "candidate_missing_or_inactive_count": 0,
        "any_history_active_ratio": None,
        "latest_candidate_instrument_count": 0,
        "latest_candidate_active_instrument_count": 0,
        "latest_candidate_missing_or_inactive_count": 0,
        "full_market_coverage_ratio": 0.0,
        "basis": "manifest_verified_latest_fiscal_year_adoptable_or_duplicate_candidates",
    }
    assert json.loads(json.dumps(artifact))["schema_version"] == inventory.SCHEMA_VERSION


def test_read_legacy_manifest_rows_uses_read_only_registered_paths(tmp_path: Path) -> None:
    root = tmp_path / "data" / "filings" / "business_profile"
    root.mkdir(parents=True)
    archive_path = root / "2025" / "SSE" / (
        "600000_SH_2025Q4_1_" + "a" * 64 + ".pdf"
    )
    archive_path.parent.mkdir(parents=True)
    archive_path.write_bytes(b"%PDF-1.4\n")
    database = tmp_path / "financials.db"

    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE financial_source_files (
                source_file_id TEXT, instrument_id TEXT, symbol TEXT, exchange TEXT,
                report_period TEXT, report_type TEXT, filing_id TEXT, source_url TEXT,
                archive_path TEXT, content_hash TEXT, content_length INTEGER,
                published_at TEXT, downloaded_at TEXT, parser_version TEXT,
                parser_diagnostics_json TEXT, schema_version TEXT, source TEXT,
                source_mode TEXT, source_tier TEXT, status TEXT,
                supersedes_source_file_id TEXT, metadata_json TEXT,
                created_at TEXT, updated_at TEXT
            )
            """
        )
        connection.execute(
            "INSERT INTO financial_source_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "manifest-1", "600000.SH", "600000", "SSE", "2025-12-31",
                "annual_report", "1", "https://example.invalid/1", str(archive_path),
                "a" * 64, 9, "2026-03-01", "2026-03-01", "test", "{}",
                "business_profile_source_file_manifest.v1", "cninfo", "direct", None,
                "archived", None, '{"document_family":"annual_report"}',
                "2026-03-01", "2026-03-01",
            ),
        )

    config = type(
        "Config",
        (),
        {
            "legacy_inventory": type(
                "LegacyInventory",
                (),
                {"roots": (("business_profile", root),)},
            )()
        },
    )()

    rows, details = inventory._read_legacy_manifest_rows(
        financials_db=database,
        project_root=tmp_path,
        config=config,
    )

    assert details["status"] == "complete"
    assert details["rows_loaded"] == 1
    assert rows[0]["archive_path"] == str(archive_path)
    assert rows[0]["metadata"] == {"document_family": "annual_report"}


def test_capacity_artifact_fails_closed_without_manifest_input(
    tmp_path: Path, monkeypatch
) -> None:
    filings = tmp_path / "data" / "filings"
    filings.mkdir(parents=True)
    config = SimpleNamespace(
        filings_root=filings,
        backup=SimpleNamespace(mount_root=None),
    )
    monkeypatch.setattr(
        inventory.AnnouncementAssetConfig,
        "from_research_config",
        lambda *args, **kwargs: config,
    )
    monkeypatch.setattr(
        inventory.config_manager,
        "get_research_config",
        lambda: object(),
    )

    with pytest.raises(RuntimeError, match="legacy_manifest_input_unavailable"):
        inventory.build_artifact(
            project_root=tmp_path,
            quotes_db=tmp_path / "quotes.db",
            financials_db=tmp_path / "missing.db",
        )


def test_capacity_uses_annual_candidates_for_distribution_and_latest_coverage(
    tmp_path: Path, monkeypatch
) -> None:
    filings = tmp_path / "data" / "filings"
    filings.mkdir(parents=True)
    (filings / "corporate_actions").mkdir()
    (filings / "corporate_actions" / "small.pdf").write_bytes(b"x")
    quotes = tmp_path / "quotes.db"
    with sqlite3.connect(quotes) as connection:
        connection.execute(
            "CREATE TABLE instruments (instrument_id TEXT, exchange TEXT, "
            "is_active INTEGER, type TEXT)"
        )
        connection.executemany(
            "INSERT INTO instruments VALUES (?, ?, 1, 'stock')",
            [
                ("600000.SH", "SSE"),
                ("000001.SZ", "SZSE"),
                ("920001.BJ", "BSE"),
            ],
        )
    config = SimpleNamespace(
        filings_root=filings,
        backup=SimpleNamespace(mount_root=None),
        storage=SimpleNamespace(
            max_attachment_bytes=1000,
            unknown_length_reservation_bytes=100,
        ),
        acquisition=SimpleNamespace(
            max_task_download_bytes=10000,
            download_concurrency=1,
        ),
        config_fingerprint="a" * 64,
    )
    items = (
        SimpleNamespace(
            status="adoptable", content_length=100, instrument_id="600000.SH",
            fiscal_year=2024, path="old", consumer="test", reason="verified",
            exchange="SSE", report_period="2024-12-31", report_type="annual_report",
            source="cninfo", filing_id="old", source_file_id="old",
            content_hash="a" * 64, expected_hash="a" * 64,
        ),
        SimpleNamespace(
            status="duplicate", content_length=900, instrument_id="000001.SZ",
            fiscal_year=2025, path="latest", consumer="test", reason="verified",
            exchange="SZSE", report_period="2025-12-31", report_type="annual_report",
            source="cninfo", filing_id="latest", source_file_id="latest",
            content_hash="b" * 64, expected_hash="b" * 64,
        ),
    )
    report = SimpleNamespace(
        files_seen=3,
        manifest_rows_seen=2,
        items=items,
        inventory_fingerprint="b" * 64,
        root_registry_version="test.v1",
        out_of_scope_directories=(),
    )
    monkeypatch.setattr(
        inventory.AnnouncementAssetConfig,
        "from_research_config",
        lambda *args, **kwargs: config,
    )
    monkeypatch.setattr(inventory.config_manager, "get_research_config", lambda: object())
    monkeypatch.setattr(
        inventory.AnnouncementArchiveInventory,
        "inventory_registered",
        lambda self, **kwargs: report,
    )
    monkeypatch.setattr(
        inventory,
        "_read_legacy_manifest_rows",
        lambda **kwargs: ([{"id": 1}, {"id": 2}], {
            "status": "complete", "rows_loaded": 2,
        }),
    )

    artifact = inventory.build_artifact(project_root=tmp_path, quotes_db=quotes)

    assert artifact["primary_archive"]["pdf_distribution"] == {
        "scope": "manifest_verified_annual_report_candidates",
        "file_count": 2,
        "total_bytes": 1000,
        "p95_bytes": 900,
        "p99_bytes": 900,
        "max_bytes": 900,
    }
    assert artifact["primary_archive"]["all_filings_pdf_distribution"]["file_count"] == 1
    assert artifact["inventory"]["review_set"]["duplicate_path_count"] == 1
    assert artifact["inventory"]["review_set"]["duplicate_path_bytes"] == 900
    assert "not an approved deletion estimate" in artifact["inventory"]["review_set"][
        "duplicate_bytes_basis"
    ]
    coverage = artifact["active_universe"]["candidate_coverage"]
    assert coverage["latest_fiscal_year"] == 2025
    assert coverage["candidate_active_instrument_count"] == 2
    assert coverage["latest_candidate_active_instrument_count"] == 1
    assert coverage["full_market_coverage_ratio"] == pytest.approx(1 / 3)


def test_evidence_output_must_be_new_and_outside_project_data(tmp_path: Path) -> None:
    data_output = tmp_path / "data" / "research.db"
    data_output.parent.mkdir()
    with pytest.raises(ValueError, match="must not be created under project data"):
        inventory._validate_new_output_path(data_output, project_root=tmp_path)

    existing = tmp_path / "existing.json"
    existing.write_text("keep", encoding="utf-8")
    with pytest.raises(FileExistsError, match="already exists"):
        inventory._validate_new_output_path(existing, project_root=tmp_path)
    assert existing.read_text(encoding="utf-8") == "keep"
