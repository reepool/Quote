import json
import sqlite3

from research.financial_available_date_backfill import backfill_financial_available_dates


def _create_core_table(conn, table_name):
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            instrument_id TEXT NOT NULL,
            report_period TEXT NOT NULL,
            exchange TEXT,
            fiscal_year INTEGER,
            fiscal_quarter INTEGER,
            data_available_date TEXT,
            publish_date TEXT,
            source_file_id TEXT,
            lineage_json TEXT NOT NULL DEFAULT '{{}}',
            updated_at TEXT,
            PRIMARY KEY (instrument_id, report_period)
        )
        """
    )


def test_backfill_financial_available_dates_uses_observed_source_file_date(tmp_path):
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_core_facts_hot")
        conn.execute(
            """
            CREATE TABLE financial_source_files (
                instrument_id TEXT,
                report_period TEXT,
                published_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_core_facts_hot (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                source_file_id, lineage_json
            ) VALUES ('600000.SH', '2025-03-31', 'SSE', 2025, 1, 'sf1', '{}')
            """
        )
        conn.execute(
            """
            INSERT INTO financial_source_files
            VALUES ('600000.SH', '2025-03-31', '2025-04-28T16:00:00+00:00')
            """
        )

    report = backfill_financial_available_dates(db_path, write_enabled=True)

    assert report.updated_rows == 1
    assert report.source_counts == {"source_file": 1}
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT data_available_date, publish_date, lineage_json
            FROM financial_core_facts_hot
            """
        ).fetchone()
    assert row[0] == "2025-04-28"
    assert row[1] == "2025-04-28"
    lineage = json.loads(row[2])
    assert lineage["data_available_date_backfill"]["quality"] == "observed"


def test_backfill_financial_available_dates_marks_estimated_deadline(tmp_path):
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_core_facts_hot")
        conn.execute(
            """
            INSERT INTO financial_core_facts_hot (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                lineage_json
            ) VALUES ('000001.SZ', '2025-12-31', 'SZSE', 2025, 4, '{}')
            """
        )

    report = backfill_financial_available_dates(db_path, write_enabled=True)

    assert report.updated_rows == 1
    assert report.source_counts == {"estimated_statutory_deadline": 1}
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT data_available_date, publish_date, lineage_json
            FROM financial_core_facts_hot
            """
        ).fetchone()
    assert row[0] == "2026-04-30"
    assert row[1] is None
    lineage = json.loads(row[2])
    assert lineage["data_available_date_backfill"]["quality"] == "estimated"
