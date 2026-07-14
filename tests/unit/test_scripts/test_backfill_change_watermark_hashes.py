import sqlite3
from datetime import datetime

from database.operations import DatabaseOperations
from scripts.backfill_change_watermark_hashes import backfill_hashes


def _create_valuation_db(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE valuation_inputs (
                instrument_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exchange TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                currency TEXT NOT NULL,
                market_cap REAL,
                shares_outstanding REAL,
                float_market_cap REAL,
                float_shares REAL,
                source TEXT NOT NULL,
                source_mode TEXT NOT NULL,
                input_kind TEXT NOT NULL,
                unit TEXT,
                data_as_of TEXT,
                diagnostics_json TEXT NOT NULL,
                row_hash TEXT,
                row_version INTEGER NOT NULL DEFAULT 1,
                ingestion_run_id INTEGER,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE data_change_log (
                sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO valuation_inputs (
                instrument_id, symbol, exchange, as_of_date, currency,
                market_cap, source, source_mode, input_kind, data_as_of,
                diagnostics_json, row_version, ingestion_run_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "600519.SH",
                "600519",
                "SSE",
                "2026-04-18",
                "CNY",
                2000.0,
                "manual",
                "local",
                "market_cap",
                "2026-04-18",
                "{}",
                1,
                9,
                "2026-04-19T00:00:00",
                "2026-04-19T00:00:00",
            ),
        )


def test_bounded_hash_backfill_defaults_to_dry_run(tmp_path):
    db_path = tmp_path / "valuation.db"
    _create_valuation_db(db_path)

    dry_run = backfill_hashes(db_path, "valuation_inputs", limit=1)
    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            "SELECT row_hash, row_version FROM valuation_inputs"
        ).fetchone()
    assert dry_run["status"] == "dry_run"
    assert dry_run["selected"] == 1
    assert dry_run["written"] == 0
    assert before == (None, 1)

    executed = backfill_hashes(
        db_path,
        "valuation_inputs",
        start_date="2026-04-18",
        end_date="2026-04-18",
        limit=1,
        execute=True,
    )
    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            "SELECT row_hash, row_version FROM valuation_inputs"
        ).fetchone()
        change_count = conn.execute("SELECT COUNT(*) FROM data_change_log").fetchone()[0]
    assert executed["written"] == 1
    assert executed["remaining_missing_hashes"] == 0
    assert after[0]
    assert after[1] == 1
    assert change_count == 0
    assert backfill_hashes(db_path, "valuation_inputs", limit=1)["selected"] == 0


def test_hash_backfill_rejects_unbounded_limits(tmp_path):
    db_path = tmp_path / "valuation.db"
    _create_valuation_db(db_path)

    for invalid_limit in (0, 10001):
        try:
            backfill_hashes(db_path, "valuation_inputs", limit=invalid_limit)
        except ValueError as exc:
            assert "between 1 and 10000" in str(exc)
        else:
            raise AssertionError("unbounded hash backfill limit should fail")


def test_hash_backfill_does_not_create_missing_database(tmp_path):
    db_path = tmp_path / "missing.db"

    try:
        backfill_hashes(db_path, "valuation_inputs", limit=1)
    except FileNotFoundError as exc:
        assert "does not exist" in str(exc)
    else:
        raise AssertionError("missing database path should fail")

    assert not db_path.exists()


def test_quote_hash_backfill_matches_production_semantics(tmp_path):
    db_path = tmp_path / "quotes.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE daily_quotes (
                time TEXT NOT NULL, instrument_id TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL, volume INTEGER,
                amount REAL, turnover REAL, pre_close REAL, change REAL,
                pct_change REAL, tradestatus INTEGER, factor REAL,
                adjustment_type TEXT, is_complete INTEGER, quality_score REAL,
                source TEXT, batch_id TEXT, created_at TEXT, updated_at TEXT,
                row_hash TEXT, row_version INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        values = (
            "2026-04-18 00:00:00.000000",
            "600519.SH",
            1590.0,
            1610.0,
            1580.0,
            1600.0,
            100,
            160000.0,
            0.5,
            1588.0,
            12.0,
            0.75,
            1,
            1.0,
            "none",
            1,
            1.0,
            "unit",
            "batch",
            "2026-04-19",
            "2026-04-19",
        )
        conn.execute(
            """
            INSERT INTO daily_quotes VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1
            )
            """,
            values,
        )

    backfill_hashes(db_path, "daily_quotes", limit=1, execute=True)
    with sqlite3.connect(db_path) as conn:
        actual = conn.execute("SELECT row_hash FROM daily_quotes").fetchone()[0]
    expected = DatabaseOperations._daily_quote_hash(
        {
            "time": datetime(2026, 4, 18),
            "instrument_id": "600519.SH",
            "open": 1590.0,
            "high": 1610.0,
            "low": 1580.0,
            "close": 1600.0,
            "volume": 100,
            "amount": 160000.0,
            "turnover": 0.5,
            "pre_close": 1588.0,
            "change": 12.0,
            "pct_change": 0.75,
            "tradestatus": 1,
            "factor": 1.0,
            "adjustment_type": "none",
            "is_complete": True,
            "quality_score": 1.0,
            "source": "unit",
        }
    )
    assert actual == expected
