import sqlite3

from scripts.research_financial_db_optimize import (
    build_plan,
    execute_optimization,
)


def test_financial_db_optimize_replaces_duplicate_table_with_view(tmp_path):
    db_path = tmp_path / "financials.db"
    backup_dir = tmp_path / "bak"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE financial_numeric_facts (
                source_file_id TEXT,
                fact_name TEXT,
                context_id TEXT,
                unit TEXT,
                dimensions_hash TEXT,
                fact_value REAL
            );
            CREATE TABLE financial_numeric_facts_hot (
                source_file_id TEXT,
                fact_name TEXT,
                context_id TEXT,
                unit TEXT,
                dimensions_hash TEXT,
                fact_value REAL
            );
            CREATE TABLE financial_numeric_facts_history (
                source_file_id TEXT,
                fact_name TEXT,
                context_id TEXT,
                unit TEXT,
                dimensions_hash TEXT,
                fact_value REAL
            );
            CREATE INDEX idx_financial_numeric_facts_instrument
            ON financial_numeric_facts(source_file_id, fact_name);
            INSERT INTO financial_numeric_facts VALUES ('sf1', 'revenue', '', 'CNY', '', 1.0);
            INSERT INTO financial_numeric_facts_hot VALUES ('sf1', 'revenue', '', 'CNY', '', 1.0);
            """
        )

    plan = build_plan(db_path, backup_dir, timestamp="20260525_010203")
    result = execute_optimization(plan)

    with sqlite3.connect(db_path) as conn:
        object_type = conn.execute(
            "SELECT type FROM sqlite_master WHERE name = 'financial_numeric_facts'"
        ).fetchone()[0]
        count = conn.execute("SELECT COUNT(*) FROM financial_numeric_facts").fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert result["status"] == "success"
    assert object_type == "view"
    assert count == 1
    assert integrity == "ok"
    assert (backup_dir / "financials.db.20260525_010203.bak").exists()
