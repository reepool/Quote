"""The real research initialization's row-version addition is race-idempotent."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

import pytest

from research.change_watermarks import ensure_row_version_column


def test_parallel_initializers_can_observe_the_same_missing_column(tmp_path):
    path = tmp_path / "initialization.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE observations (name TEXT, value INTEGER)")
        connection.execute("INSERT INTO observations VALUES ('original', 17)")
    barrier = threading.Barrier(2, timeout=5)

    class Rows:
        def __init__(self, rows):
            self.rows = rows

        def fetchall(self):
            return self.rows

    class SimultaneousObservation(sqlite3.Connection):
        observed = False

        def execute(self, sql, parameters=()):
            cursor = super().execute(sql, parameters)
            if sql == "PRAGMA table_info(observations)" and not self.observed:
                self.observed = True
                rows = cursor.fetchall()
                barrier.wait()
                return Rows(rows)
            return cursor

    def initialize():
        with sqlite3.connect(path, timeout=5, factory=SimultaneousObservation) as connection:
            ensure_row_version_column(connection, "observations")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(initialize) for _ in range(2)]
        for result in futures:
            result.result(timeout=10)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT * FROM observations").fetchall() == [("original", 17, 1)]
        assert [row[1] for row in connection.execute("PRAGMA table_info(observations)")].count("row_version") == 1


@pytest.mark.parametrize("message", ["database is locked", "duplicate column name: row_version"])
def test_unrelated_or_unconfirmed_sqlite_errors_are_not_suppressed(message):
    class BrokenConnection:
        def execute(self, sql):
            if sql.startswith("PRAGMA"):
                return self
            raise sqlite3.OperationalError(message)

        def fetchall(self):
            return [(0, "name")]

    with pytest.raises(sqlite3.OperationalError, match=message):
        ensure_row_version_column(BrokenConnection(), "observations")


def test_missing_table_and_unsafe_table_name_still_fail():
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            ensure_row_version_column(connection, "missing")
        with pytest.raises(ValueError, match="invalid SQLite table name"):
            ensure_row_version_column(connection, "observations;DROP TABLE observations")
