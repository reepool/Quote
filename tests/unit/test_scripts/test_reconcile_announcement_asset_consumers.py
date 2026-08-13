from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.dev_validation.reconcile_announcement_asset_consumers import (
    _active_manifest,
    _broker_processing_sets_match,
    _normalize_filing_id,
    reconcile_consumers,
)

PDF = b"%PDF-1.4\nconsumer reconciliation\n%%EOF\n"


def test_filing_alias_and_active_manifest_selection_are_deterministic():
    assert _normalize_filing_id("szse:12001") == "12001"
    assert _normalize_filing_id("12001") == "12001"
    active = _active_manifest(
        [
            {
                "source_file_id": "old",
                "report_period": "2024-12-31",
                "published_at": "2025-03-01",
                "report_type": "annual_report",
            },
            {
                "source_file_id": "new",
                "report_period": "2024-12-31",
                "published_at": "2025-03-02",
                "report_type": "annual_report",
            },
        ]
    )
    assert active["source_file_id"] == "new"


def test_reconciliation_is_read_only_and_accepts_identical_consumer_bytes(tmp_path: Path):
    research_db = tmp_path / "research.db"
    financials_db = tmp_path / "financials.db"
    blob = tmp_path / "data/filings/announcements/blobs/aa/report.pdf"
    bp_path = tmp_path / "data/filings/business_profile/report.pdf"
    broker_path = tmp_path / "data/filings/financial_statements/broker_risk_control/report.pdf"
    for path in (blob, bp_path, broker_path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(PDF)
    import hashlib

    digest = hashlib.sha256(PDF).hexdigest()
    with sqlite3.connect(research_db) as conn:
        conn.executescript(
            """
            CREATE TABLE effective_annual_reports(
                asset_id TEXT, instrument_id TEXT, fiscal_year INTEGER,
                report_period TEXT, source TEXT, source_announcement_id TEXT,
                version_id TEXT, content_hash TEXT, variant TEXT,
                visibility_state TEXT, availability TEXT, decision_state TEXT
            );
            CREATE TABLE official_document_blobs(
                content_hash TEXT, canonical_path TEXT, content_length INTEGER,
                integrity_status TEXT, signature_status TEXT
            );
            CREATE TABLE official_asset_consumer_processing(
                asset_id TEXT, consumer TEXT, parser_version TEXT,
                parameter_hash TEXT, status TEXT, error_code TEXT,
                derived_identity TEXT, metadata_json TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO effective_annual_reports VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "asset-1", "600030.SH", 2025, "2025-12-31", "cninfo",
                "filing-1", "version-1", digest, "original", "production",
                "local_valid", "current",
            ),
        )
        conn.execute(
            "INSERT INTO official_document_blobs VALUES(?,?,?,?,?)",
            (digest, str(blob), len(PDF), "valid", "valid_pdf"),
        )
        conn.execute(
            "INSERT INTO official_asset_consumer_processing VALUES(?,?,?,?,?,?,?,?)",
            (
                "asset-1", "broker_risk_control",
                "broker_annual_report_embedded_risk_control_pdf.v1",
                "parameter-1", "current", None, "broker-result-1",
                json.dumps(
                    {
                        "asset_id": "asset-1",
                        "observation_version": "version-1",
                        "content_hash": digest,
                    }
                ),
            ),
        )
    with sqlite3.connect(financials_db) as conn:
        conn.executescript(
            """
            CREATE TABLE financial_source_files(
                source_file_id TEXT, instrument_id TEXT, report_period TEXT,
                report_type TEXT, source TEXT, filing_id TEXT, content_hash TEXT,
                content_length INTEGER, archive_path TEXT, published_at TEXT,
                downloaded_at TEXT, parser_version TEXT, status TEXT,
                supersedes_source_file_id TEXT, source_mode TEXT,
                metadata_json TEXT
            );
            CREATE TABLE financial_numeric_facts(
                source_file_id TEXT, fact_name TEXT, canonical_fact_name TEXT,
                unit TEXT, currency TEXT, fact_value REAL, value_text TEXT,
                report_period TEXT, report_type TEXT, statement_family TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO financial_source_files VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "bp-1", "600030.SH", "2025-12-31", "annual_report", "cninfo",
                "filing-1", digest, len(PDF), str(bp_path), "2026-03-01", None,
                "business_profile_pdf_archive.v2", "archived", None, "direct", "{}",
            ),
        )
        conn.execute(
            "INSERT INTO financial_source_files VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "broker-1", "600030.SH", "2025-12-31", "annual", "cninfo",
                "filing-1", digest, len(PDF), str(broker_path), "2026-03-01", None,
                "broker_annual_report_embedded_risk_control_pdf.v1", "parsed", None,
                "direct", "{}",
            ),
        )
        conn.execute(
            "INSERT INTO financial_source_files VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "broker-shared-1", "600030.SH", "2025-12-31", "annual", "cninfo",
                "filing-1", digest, len(PDF), str(blob), "2026-03-01", None,
                "broker_annual_report_embedded_risk_control_pdf.v1", "parsed", None,
                "shared_announcement_asset",
                json.dumps(
                    {
                        "shared_annual_report_asset": {
                            "asset_id": "asset-1",
                            "observation_version": "version-1",
                            "content_hash": digest,
                        }
                    }
                ),
            ),
        )
        conn.execute(
            "INSERT INTO financial_numeric_facts VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                "broker-1", "regulatory_net_assets", "regulatory_net_assets", "CNY", "CNY",
                1.0, "1", "2025-12-31", "annual", "broker_risk_control",
            ),
        )
        conn.execute(
            "INSERT INTO financial_numeric_facts VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                "broker-shared-1", "regulatory_net_assets", "regulatory_net_assets", "CNY", "CNY",
                1.0, "1", "2025-12-31", "annual", "broker_risk_control",
            ),
        )
    scope = tmp_path / "broker_scope.json"
    scope.write_text(
        json.dumps(
            {"entries": [{"instrument_id": "600030.SH", "scope_status": "confirmed"}]}
        ),
        encoding="utf-8",
    )
    before = (research_db.stat().st_mtime_ns, financials_db.stat().st_mtime_ns)

    result = reconcile_consumers(
        research_db=research_db,
        financials_db=financials_db,
        broker_scope_path=scope,
        project_root=tmp_path,
        config_modules={"mode": "test"},
    )

    assert result["migration_gates"]["input_reconciliation_ready"] is True
    assert result["migration_gates"]["consumer_dependency_ready"] is True
    assert result["migration_gates"]["dual_read_ready"] is True
    assert result["migration_gates"]["shared_only_ready"] is False
    assert result["migration_gates"]["dependency_blockers"] == []
    assert result["business_profile"]["active_winner_content_match_count"] == 1
    assert result["business_profile"]["dependency_handoff_ready"] is True
    assert result["business_profile"]["downstream_processing_in_rollout_gate"] is False
    broker = result["broker_risk_control"]
    assert broker["legacy_parsed_annual_scope_count"] == 1
    assert broker["legacy_exact_shared_input_match_count"] == 1
    assert broker["shared_parsed_current_scope_count"] == 1
    assert broker["processing_current_count"] == 1
    assert broker["processing_failed_count"] == 0
    assert broker["processing_accounted"] is True
    assert broker["processing_reconciliation_ready"] is True
    assert broker["business_incomplete_scope_count"] == 1
    assert result["activity"]["provider_requests"] == 0
    assert result["activity"]["database_mutations"] == 0
    assert before == (research_db.stat().st_mtime_ns, financials_db.stat().st_mtime_ns)


def test_broker_processing_requires_exact_asset_set_not_only_equal_count():
    assert _broker_processing_sets_match(
        expected_asset_ids={"asset-1", "asset-2"},
        current_asset_ids={"asset-1"},
        failed_asset_ids={"asset-extra"},
        shared_manifest_asset_ids={"asset-1"},
    ) is False
