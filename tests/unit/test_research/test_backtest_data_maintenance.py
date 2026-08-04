import sqlite3

import pytest

from research.backtest_data.maintenance import (
    BacktestDataMaintenance,
    PriceLimitRuleEngine,
)
from research.backtest_data.quote_store import BacktestQuoteStore


def _create_instruments(path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE instruments (instrument_id TEXT PRIMARY KEY, symbol TEXT, exchange TEXT, "
            "type TEXT, status TEXT, is_active INTEGER, is_st INTEGER, trading_status INTEGER, "
            "source TEXT, updated_at TEXT)"
        )
        connection.execute(
            "INSERT INTO instruments VALUES ('000001.SZ', '000001', 'SZSE', 'stock', "
            "'active', 1, 0, 1, 'official_master', '2026-01-02T09:00:00+08:00')"
        )
        connection.commit()


def test_security_stage_reuses_local_master_and_emits_changed_only(tmp_path):
    path = tmp_path / "quotes.db"
    _create_instruments(path)
    service = BacktestDataMaintenance(path)

    first = service.sync_security_state_from_instruments(exchanges=["SZSE"])
    second = service.sync_security_state_from_instruments(exchanges=["SZSE"])
    assert first["network_requests"] == second["network_requests"] == 0
    assert first["inserted"] == 1
    assert second["unchanged"] == 1

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE instruments SET is_st = 1, updated_at = '2026-01-03T09:00:00+08:00'"
        )
        connection.commit()
    changed = service.sync_security_state_from_instruments(exchanges=["SZSE"])
    assert changed["changed"] == 1
    assert changed["events"] == 1
    assert BacktestQuoteStore(path).resolve_security_state(
        "000001.SZ",
        effective_date="2026-01-03",
        known_at="2026-01-03T10:00:00+08:00",
    )["state"] == "st"


def test_security_stage_dry_run_does_not_write(tmp_path):
    path = tmp_path / "quotes.db"
    _create_instruments(path)
    service = BacktestDataMaintenance(path)
    result = service.sync_security_state_from_instruments(dry_run=True)
    assert result["would_change"] == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM security_state_current_observations"
        ).fetchone()[0] == 0


def test_price_limit_stage_is_unavailable_without_source_fields(tmp_path):
    path = tmp_path / "quotes.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE daily_quotes (instrument_id TEXT, time TEXT, close REAL)"
        )
        connection.commit()
    result = BacktestDataMaintenance(path).sync_source_reported_price_limits(
        start_date="2026-01-01", end_date="2026-01-31"
    )
    assert result["status"] == "unavailable"
    assert result["network_requests"] == 0


def test_source_reported_price_limits_are_persisted_when_parent_has_fields(tmp_path):
    path = tmp_path / "quotes.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE daily_quotes (instrument_id TEXT, time TEXT, limit_up REAL, limit_down REAL, "
            "reference_price REAL, source TEXT, updated_at TEXT)"
        )
        connection.execute(
            "INSERT INTO daily_quotes VALUES ('000001.SZ', '2026-01-05', 11, 9, 10, "
            "'exchange', '2026-01-05T16:00:00+08:00')"
        )
        connection.commit()
    service = BacktestDataMaintenance(path)
    result = service.sync_source_reported_price_limits(
        start_date="2026-01-01", end_date="2026-01-31"
    )
    assert result["inserted"] == 1
    resolved = BacktestQuoteStore(path).resolve_price_limit(
        "000001.SZ",
        trade_date="2026-01-05",
        known_at="2026-01-05T17:00:00+08:00",
    )
    assert resolved["evidence"]["source_mode"] == "source_reported"


def test_index_stage_rejects_current_only_as_historical_evidence(tmp_path):
    service = BacktestDataMaintenance(tmp_path / "quotes.db")
    item = {
        "response_kind": "current_only",
        "snapshot": {
            "snapshot_id": "s1",
            "index_instrument_id": "000300.SH",
            "effective_date": "2026-01-01",
            "available_at": "2026-01-02T09:00:00+08:00",
            "source": "akshare",
            "source_profile": "akshare.current.v1",
            "completeness_state": "complete",
        },
        "members": [{"source_symbol": "000001.SZ", "weight": 1.0}],
    }
    result = service.ingest_index_snapshots([item], historical_request=True)
    assert result["status"] == "blocked"
    assert result["inserted"] == 0


def test_price_limit_rule_engine_requires_all_governed_inputs_and_rounds_to_tick():
    engine = PriceLimitRuleEngine()
    with pytest.raises(ValueError, match="inputs missing"):
        engine.calculate({"reference_price": 10})
    with pytest.raises(ValueError, match="raw prior close"):
        engine.calculate(
            {
                "reference_price": 10,
                "reference_price_basis": "raw_prior_close",
                "board": "main",
                "listing_age_days": 100,
                "st_state": "normal",
                "trading_regime": "normal",
                "tick_size": 0.01,
                "corporate_action_adjustment": "none_required",
                "rounding_mode": "half_up_to_tick",
            }
        )
    result = engine.calculate(
        {
            "reference_price": 10.03,
            "reference_price_basis": "exchange_ex_right_adjusted",
            "board": "main",
            "listing_age_days": 100,
            "st_state": "normal",
            "trading_regime": "normal",
            "tick_size": 0.01,
            "corporate_action_adjustment": "exchange_reference_applied",
            "rounding_mode": "half_up_to_tick",
        }
    )
    assert result["limit_up"] == 11.03
    assert result["limit_down"] == 9.03


def test_announcement_stage_reuses_local_official_evidence_and_keeps_pending_distinct(tmp_path):
    quotes_path = tmp_path / "quotes.db"
    research_path = tmp_path / "research.db"
    with sqlite3.connect(research_path) as connection:
        connection.execute(
            "CREATE TABLE announcement_audit (purpose_key TEXT, source TEXT, announcement_key TEXT, "
            "source_announcement_id TEXT, instrument_id TEXT, symbol TEXT, exchange TEXT, "
            "published_at TEXT, title TEXT, raw_payload_json TEXT, created_at TEXT, updated_at TEXT)"
        )
        connection.executemany(
            "INSERT INTO announcement_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("official", "cninfo", "a1", "1", "000001.SZ", "000001", "SZSE", "2026-01-02T09:00:00+08:00", "关于股票交易将被实施退市风险警示的公告", '{"effective_date":"2026-01-05"}', "2026-01-02T10:00:00+08:00", "2026-01-02T10:00:00+08:00"),
                ("official", "cninfo", "a2", "2", "000002.SZ", "000002", "SZSE", "2026-01-03T09:00:00+08:00", "关于股票可能被终止上市的风险提示公告", '{"effective_date":"2026-01-04"}', "2026-01-03T10:00:00+08:00", "2026-01-03T10:00:00+08:00"),
                ("official", "cninfo", "a3", "3", "000003.SZ", "000003", "SZSE", "2026-01-04T09:00:00+08:00", "年度股东大会公告", '{}', "2026-01-04T10:00:00+08:00", "2026-01-04T10:00:00+08:00"),
            ],
        )
        connection.commit()
    service = BacktestDataMaintenance(quotes_path)
    result = service.sync_security_events_from_announcements(research_path)
    assert result["network_requests"] == 0
    assert result["inserted"] == 2
    assert result["ignored"] == 1
    assert result["status"] == "partial"
    assert service.store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-02", known_at="2026-01-05T09:00:00+08:00"
    )["status"] == "unavailable"
    assert service.store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-05", known_at="2026-01-05T09:00:00+08:00"
    )["state"] == "st"
    pending = service.store.resolve_security_state(
        "000002.SZ", effective_date="2026-01-04", known_at="2026-01-05T09:00:00+08:00"
    )
    assert pending["status"] == "unavailable"
    assert pending["reason"] == "state_event_quality_blocked"


def test_announcement_without_explicit_effective_date_stays_unresolved(tmp_path):
    quotes_path = tmp_path / "quotes.db"
    research_path = tmp_path / "research.db"
    with sqlite3.connect(research_path) as connection:
        connection.execute(
            "CREATE TABLE announcement_audit (purpose_key TEXT, source TEXT, announcement_key TEXT, "
            "source_announcement_id TEXT, instrument_id TEXT, symbol TEXT, exchange TEXT, "
            "published_at TEXT, title TEXT, raw_payload_json TEXT, created_at TEXT, updated_at TEXT)"
        )
        connection.execute(
            "INSERT INTO announcement_audit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("official", "cninfo", "a1", "1", "000001.SZ", "000001", "SZSE", "2026-01-02T09:00:00+08:00", "关于股票交易将被实施退市风险警示的公告", '{}', "2026-01-02T10:00:00+08:00", "2026-01-02T10:00:00+08:00"),
        )
        connection.commit()

    service = BacktestDataMaintenance(quotes_path)
    result = service.sync_security_events_from_announcements(research_path)
    assert result["status"] == "partial"
    assert result["inserted"] == 1
    assert result["blockers"] == ["000001.SZ:st_started:effective_date_missing"]
    with service.store.connection() as connection:
        event = connection.execute(
            "SELECT effective_date, published_at, available_at, quality "
            "FROM security_state_events"
        ).fetchone()
    assert event["effective_date"] is None
    assert event["published_at"] == "2026-01-02T09:00:00+08:00"
    assert event["available_at"] == "2026-01-02T10:00:00+08:00"
    assert event["quality"] == "unresolved"
