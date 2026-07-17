import json
import sqlite3

from scripts.research_business_profile_benchmark_select import build_parser_benchmark


def test_benchmark_command_builder_uses_read_only_point_in_time_universe(tmp_path):
    research_db = tmp_path / "research.db"
    financials_db = tmp_path / "financials.db"
    quotes_db = tmp_path / "quotes.db"
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
            """
        )
        conn.executemany(
            "INSERT INTO industry_classification_history VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    f"60000{index}.SH",
                    f"60000{index}",
                    "SSE",
                    "sw",
                    "sw_2021",
                    "210101",
                    "2020-01-01",
                    "2020-01-02",
                    "2020-01-02",
                    "2020-01-02",
                )
                for index in range(1, 6)
            ],
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
        conn.executemany(
            "INSERT INTO instruments VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    f"60000{index}.SH",
                    f"issuer {index}",
                    "SSE",
                    "stock",
                    "2000-01-01",
                    None,
                    "active",
                    1,
                )
                for index in range(1, 6)
            ],
        )
    with sqlite3.connect(financials_db) as conn:
        conn.execute(
            """
            CREATE TABLE financial_source_files (
                source_file_id TEXT, instrument_id TEXT, source TEXT,
                report_period TEXT, report_type TEXT, filing_id TEXT,
                source_url TEXT, archive_path TEXT, content_hash TEXT,
                published_at TEXT, parser_version TEXT, status TEXT,
                source_tier TEXT, schema_version TEXT,
                supersedes_source_file_id TEXT, metadata_json TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO financial_source_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "source-1",
                "600001.SH",
                "cninfo",
                "2024-12-31",
                "annual_report_correction",
                "announcement-1",
                "https://www.cninfo.com.cn/report.pdf",
                "/archive/report.pdf",
                "a" * 64,
                "2025-03-20",
                "business_profile_pdf_archive.v1",
                "archived",
                "official_primary",
                "business_profile_source_file_manifest.v1",
                None,
                "{}",
            ),
        )

    payload = build_parser_benchmark(
        research_db=research_db,
        financials_db=financials_db,
        quotes_db=quotes_db,
        as_of_date="2025-12-31",
    )

    assert payload["selected_issuer_count"] == 5
    assert payload["status"] == "evidence_incomplete"
    assert payload["industries"]["coal"]["candidate_count"] == 5
    assert len(payload["incomplete_industry_groups"]) == 6

    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            [
                {
                    "instrument_id": "600001.SH",
                    "verified": True,
                    "diversified_business": True,
                    "correction_report": True,
                    "complex_table": True,
                    "source_document_ids": ["source-1"],
                }
            ]
        ),
        encoding="utf-8",
    )
    validated = build_parser_benchmark(
        research_db=research_db,
        financials_db=financials_db,
        quotes_db=quotes_db,
        as_of_date="2025-12-31",
        evidence_path=evidence_path,
    )

    assert validated["industries"]["coal"]["status"] == "ready"
    assert validated["status"] == "evidence_incomplete"
