import sqlite3

from database.connection import DatabaseManager


def test_database_initialization_creates_backtest_tables_without_data(tmp_path):
    path = tmp_path / "quotes.db"
    manager = DatabaseManager(str(path))
    manager.initialize()
    try:
        with sqlite3.connect(path) as connection:
            for table in (
                "index_composition_snapshots",
                "security_state_events",
                "daily_price_limit_revisions",
                "canonical_corporate_action_revisions",
            ):
                assert connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0] == 0
    finally:
        manager.close()
