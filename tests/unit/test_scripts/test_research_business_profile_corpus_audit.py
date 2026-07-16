import sqlite3

from scripts.research_business_profile_corpus_audit import build_corpus_audit


def test_corpus_audit_is_read_only_and_reports_local_coverage(tmp_path):
    research_db = tmp_path / "research.db"
    quotes_db = tmp_path / "quotes.db"
    financials_db = tmp_path / "financials.db"
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
    with sqlite3.connect(financials_db) as conn:
        conn.execute(
            """
            CREATE TABLE financial_source_files (
                source_file_id TEXT, instrument_id TEXT, source TEXT,
                report_period TEXT, report_type TEXT, filing_id TEXT,
                source_url TEXT, archive_path TEXT, content_hash TEXT,
                published_at TEXT, parser_version TEXT, status TEXT,
                metadata_json TEXT, schema_version TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO financial_source_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "file-1",
                "600001.SH",
                "cninfo",
                "2024-12-31",
                "annual_report",
                "announcement-1",
                "report.pdf",
                "/archive/report.pdf",
                "hash",
                "2025-03-20",
                "parser.v1",
                "parsed",
                "{}",
                "business_profile_source_file_manifest.v1",
            ),
        )
        conn.commit()

    payload = build_corpus_audit(
        research_db=research_db,
        financials_db=financials_db,
        quotes_db=quotes_db,
        as_of_date="2025-12-31",
        expected_report_periods=["2024-12-31"],
    )

    assert payload["readiness"]["universe_count"] == 1
    assert payload["readiness"]["archived_document_count"] == 1
    assert payload["readiness"]["missing_expected_document_count"] == 0
    assert payload["universe"][0]["company_name"] == "sample coal"
