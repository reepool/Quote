import sqlite3

from scripts.research_financial_db_storage_audit import (
    audit_financial_db,
    render_markdown,
)


def test_financial_db_storage_audit_reports_duplicate_numeric_evidence(tmp_path):
    db_path = tmp_path / "financials.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE financial_numeric_facts (
                id INTEGER PRIMARY KEY,
                raw_fact_json TEXT,
                dimensions_json TEXT
            );
            CREATE TABLE financial_numeric_facts_hot (
                id INTEGER PRIMARY KEY,
                raw_fact_json TEXT,
                dimensions_json TEXT
            );
            CREATE TABLE financial_numeric_facts_history (
                id INTEGER PRIMARY KEY,
                raw_fact_json TEXT,
                dimensions_json TEXT
            );
            CREATE TABLE financial_statements_raw (
                id INTEGER PRIMARY KEY,
                statement_json TEXT
            );
            CREATE TABLE financial_source_files (
                id INTEGER PRIMARY KEY,
                metadata_json TEXT,
                parser_diagnostics_json TEXT
            );
            CREATE TABLE financial_core_facts_hot (id INTEGER PRIMARY KEY);
            CREATE TABLE financial_core_facts_history (id INTEGER PRIMARY KEY);
            CREATE TABLE raw_payload_audit (
                id INTEGER PRIMARY KEY,
                payload_json TEXT
            );
            CREATE TABLE ingestion_runs (id INTEGER PRIMARY KEY);
            CREATE INDEX idx_hot_id ON financial_numeric_facts_hot(id);
            INSERT INTO financial_numeric_facts VALUES (1, '{"a": 1}', '{}');
            INSERT INTO financial_numeric_facts_hot VALUES (1, '{"a": 1}', '{}');
            INSERT INTO financial_statements_raw VALUES (1, '{"statement": true}');
            INSERT INTO raw_payload_audit VALUES (1, '{"payload": true}');
            """
        )

    report = audit_financial_db(db_path, sample_size=10)
    markdown = render_markdown(report)

    assert report["critical_table_rows"]["financial_numeric_facts"] == 1
    assert report["critical_table_rows"]["financial_numeric_facts_hot"] == 1
    assert report["duplicate_numeric_fact_evidence"]["numeric_equals_hot"] is True
    assert report["index_count"] == 1
    assert "Financial DB Storage Audit" in markdown
    assert "financial_numeric_facts_hot" in markdown
