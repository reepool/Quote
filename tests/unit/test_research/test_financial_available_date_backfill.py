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
    # 2025-04-28T16:00:00Z == 2025-04-29 00:00 CST: the Shanghai calendar
    # date is the disclosure date shown by cninfo.
    assert row[0] == "2025-04-29"
    assert row[1] == "2025-04-29"
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


def test_backfill_covers_financial_facts_wide_table(tmp_path):
    """financial_facts joins the backfill target tables (M1 of the
    historical availability change): a missing availability date on the
    wide table is filled from statements_raw evidence."""
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_facts")
        conn.execute(
            """
            CREATE TABLE financial_statements_raw (
                instrument_id TEXT,
                report_period TEXT,
                publish_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                lineage_json
            ) VALUES ('600050.SH', '2024-09-30', 'SSE', 2024, 3, '{}')
            """
        )
        conn.execute(
            """
            INSERT INTO financial_statements_raw
            VALUES ('600050.SH', '2024-09-30', '2024-10-25T08:00:00+00:00')
            """
        )

    report = backfill_financial_available_dates(db_path, write_enabled=True)

    assert report.updated_rows == 1
    assert report.source_counts == {"statements_raw": 1}
    assert report.table_counts.get("financial_facts") == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT data_available_date, publish_date FROM financial_facts"
        ).fetchone()
    assert row[0] == "2024-10-25"
    assert row[1] == "2024-10-25"


def test_backfill_is_idempotent(tmp_path):
    """Second run on already-filled rows updates nothing."""
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_facts")
        conn.execute(
            """
            CREATE TABLE financial_statements_raw (
                instrument_id TEXT,
                report_period TEXT,
                publish_date TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                lineage_json
            ) VALUES ('600050.SH', '2024-09-30', 'SSE', 2024, 3, '{}')
            """
        )
        conn.execute(
            "INSERT INTO financial_statements_raw VALUES ('600050.SH', '2024-09-30', '2024-10-25')"
        )

    first = backfill_financial_available_dates(db_path, write_enabled=True)
    assert first.updated_rows == 1

    second = backfill_financial_available_dates(db_path, write_enabled=True)
    assert second.updated_rows == 0
    assert second.scanned_rows == 0


def test_survey_audit_source_requires_inferred_period(tmp_path):
    """announcement_audit survey rows feed evidence only when the inferred
    report period is recorded; rows without it are skipped."""
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_facts")
        conn.execute(
            """
            CREATE TABLE announcement_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                purpose_key TEXT,
                instrument_id TEXT,
                published_at TEXT,
                selection_reasons_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                lineage_json
            ) VALUES ('688981.SH', '2024-09-30', 'SSE', 2024, 3, '{}')
            """
        )
        conn.execute(
            """
            INSERT INTO announcement_audit
                (purpose_key, instrument_id, published_at, selection_reasons_json)
            VALUES
                ('historical_disclosure_date_survey', '688981.SH',
                 '2024-11-05T08:00:00+00:00',
                 '{"inferred_report_period": "2024-09-30"}'),
                ('historical_disclosure_date_survey', '688981.SH',
                 '2024-10-31T08:00:00+00:00', '{"note": "no period"}'),
                ('financial_disclosure_incremental_sync', '688981.SH',
                 '2024-11-01T08:00:00+00:00',
                 '{"inferred_report_period": "2024-09-30"}')
            """
        )

    report = backfill_financial_available_dates(db_path, write_enabled=True)

    assert report.updated_rows == 1
    assert report.source_counts == {"survey_audit": 1}
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT data_available_date, publish_date, lineage_json FROM financial_facts"
        ).fetchone()
    # the earliest survey row with a resolvable period wins
    assert row[0] == "2024-11-05"
    lineage = json.loads(row[2])
    assert lineage["data_available_date_backfill"]["source"] == "survey_audit"
    # rows from other purposes never leak into the evidence set
    assert row[0] != "2024-11-01"


def test_statutory_fallback_keeps_publish_date_null_on_wide_table(tmp_path):
    """Estimated deadline fallback on financial_facts: data_available_date is
    filled, publish_date stays NULL (a deadline is a bound, not a fact)."""
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_facts")
        conn.execute(
            """
            INSERT INTO financial_facts (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                lineage_json
            ) VALUES ('000001.SZ', '2024-09-30', 'SZSE', 2024, 3, '{}')
            """
        )

    report = backfill_financial_available_dates(db_path, write_enabled=True)

    assert report.source_counts == {"estimated_statutory_deadline": 1}
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT data_available_date, publish_date FROM financial_facts"
        ).fetchone()
    assert row[0] == "2024-10-31"
    assert row[1] is None


def test_backfill_rejects_evidence_before_period_end(tmp_path):
    """Issuer title-year typos (a "2024年年度报告" filed in March 2024)
    produce evidence dated before the period end; such evidence must be
    rejected so the statutory fallback applies instead (PIT guard moved
    from acceptance-time to write-time)."""
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        _create_core_table(conn, "financial_facts")
        conn.execute(
            """
            CREATE TABLE announcement_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                purpose_key TEXT,
                instrument_id TEXT,
                published_at TEXT,
                selection_reasons_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO financial_facts (
                instrument_id, report_period, exchange, fiscal_year, fiscal_quarter,
                lineage_json
            ) VALUES ('600582.SH', '2024-12-31', 'SSE', 2024, 4, '{}')
            """
        )
        # impossible evidence: 2024 annual "report" announced in March
        conn.execute(
            """
            INSERT INTO announcement_audit
                (purpose_key, instrument_id, published_at, selection_reasons_json)
            VALUES ('historical_disclosure_date_survey', '600582.SH',
                    '2024-03-20T16:00:00+00:00',
                    '["inferred_report_period=2024-12-31"]')
            """
        )

    report = backfill_financial_available_dates(db_path, write_enabled=True)

    assert report.source_counts == {"estimated_statutory_deadline": 1}
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT data_available_date, publish_date FROM financial_facts"
        ).fetchone()
    assert row[0] == "2025-04-30"          # statutory fallback, not 2024-03-21
    assert row[1] is None
