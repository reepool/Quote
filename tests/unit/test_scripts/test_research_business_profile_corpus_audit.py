import sqlite3

from scripts.research_business_profile_corpus_audit import build_corpus_audit
from tests.unit.test_research.announcement_asset_fixtures import (
    register_shared_annual_report,
)


def test_corpus_audit_is_read_only_and_reports_local_coverage(tmp_path):
    research_db = tmp_path / "research.db"
    quotes_db = tmp_path / "quotes.db"
    announcement_assets_db = tmp_path / "announcement-assets.db"
    with sqlite3.connect(research_db) as conn:
        conn.executescript(
            """
            CREATE TABLE industry_taxonomy (
                taxonomy_system TEXT, taxonomy_version TEXT, industry_code TEXT,
                industry_name TEXT, industry_level INTEGER, parent_code TEXT
            );
            CREATE TABLE industry_classification_history (
                instrument_id TEXT, symbol TEXT, exchange TEXT,
                taxonomy_system TEXT, taxonomy_version TEXT,
                official_industry_code TEXT, official_start_date TEXT,
                official_update_time TEXT, created_at TEXT, updated_at TEXT
            );
            INSERT INTO industry_taxonomy VALUES
                ('sw','sw_2021','210000','煤炭',1,NULL),
                ('sw','sw_2021','210100','煤炭开采',2,'210000'),
                ('sw','sw_2021','210101','动力煤',3,'210100');
            INSERT INTO industry_classification_history VALUES
                ('600001.SH','600001','SSE','sw','sw_2021','210101',
                 '2020-01-01','2020-01-02','2020-01-02','2020-01-02');
            """
        )
    with sqlite3.connect(quotes_db) as conn:
        conn.execute(
            """
            CREATE TABLE instruments (
                instrument_id TEXT, name TEXT, exchange TEXT, type TEXT,
                listed_date TEXT, delisted_date TEXT, status TEXT, is_active INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO instruments VALUES (?,?,?,?,?,?,?,?)",
            (
                "600001.SH",
                "sample coal",
                "SSE",
                "stock",
                "2000-01-01",
                None,
                "active",
                1,
            ),
        )
        conn.commit()
    report_path = tmp_path / "report.pdf"
    report_path.write_bytes(b"%PDF-corpus-audit")
    register_shared_annual_report(
        announcement_assets_db,
        report_path,
        asset_id="file-1",
        instrument_id="600001.SH",
        report_period="2024-12-31",
        source_announcement_id="announcement-1",
        published_at="2025-03-20T00:00:00+08:00",
    )

    payload = build_corpus_audit(
        research_db=research_db,
        announcement_assets_db=announcement_assets_db,
        quotes_db=quotes_db,
        as_of_date="2025-12-31",
        expected_report_periods=["2024-12-31"],
    )

    assert payload["readiness"]["universe_count"] == 1
    assert payload["readiness"]["archived_document_count"] == 1
    assert payload["readiness"]["missing_expected_document_count"] == 0
    assert payload["universe"][0]["company_name"] == "sample coal"
