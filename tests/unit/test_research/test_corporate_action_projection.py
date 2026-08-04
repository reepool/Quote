import sqlite3

from research.backtest_data.corporate_action_projection import (
    CanonicalCorporateActionProjector,
)
from research.backtest_data.quote_store import BacktestQuoteStore


def _create_evidence_db(path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE corporate_action_observations ("
            "id INTEGER PRIMARY KEY, instrument_id TEXT, source TEXT, source_profile TEXT, "
            "source_event_key TEXT, action_type TEXT, announcement_date TEXT, record_date TEXT, "
            "ex_date TEXT, pay_date TEXT, share_arrival_date TEXT, cash_dividend_per_share REAL, "
            "bonus_shares_per_share REAL, capitalization_shares_per_share REAL, rights_shares_per_share REAL, "
            "rights_price REAL, currency TEXT, event_status TEXT, quality_status TEXT, row_hash TEXT, "
            "is_current INTEGER, updated_at TEXT)"
        )
        connection.execute(
            "CREATE TABLE corporate_action_resolved_terms ("
            "id INTEGER PRIMARY KEY, instrument_id TEXT, source_event_key TEXT, "
            "cash_dividend_per_share REAL, bonus_shares_per_share REAL, capitalization_shares_per_share REAL, "
            "rights_shares_per_share REAL, rights_price REAL, currency TEXT, is_active INTEGER, updated_at TEXT)"
        )
        connection.execute(
            "CREATE TABLE corporate_action_effective_date_evidence ("
            "id INTEGER PRIMARY KEY, instrument_id TEXT, source_event_key TEXT, resolution_status TEXT, "
            "effective_date TEXT, updated_at TEXT)"
        )
        connection.execute(
            "CREATE TABLE corporate_action_resolution_states ("
            "id INTEGER PRIMARY KEY, instrument_id TEXT, source_event_key TEXT, factor_blocking INTEGER, "
            "state_reason TEXT, resolved_effective_date TEXT, updated_at TEXT)"
        )
        connection.execute(
            "CREATE TABLE corporate_action_instrument_status ("
            "id INTEGER PRIMARY KEY, instrument_id TEXT, source TEXT, coverage_status TEXT, updated_at TEXT)"
        )
        connection.execute(
            "INSERT INTO corporate_action_observations VALUES "
            "(1, '000001.SZ', 'cninfo', 'cninfo.v1', 'evt-1', 'cash_dividend', '2026-01-01', NULL, NULL, NULL, NULL, 0.1, NULL, NULL, NULL, NULL, 'CNY', 'accepted', 'accepted', 'obs-hash', 1, '2026-01-02T09:00:00+08:00')"
        )
        connection.execute(
            "INSERT INTO corporate_action_effective_date_evidence VALUES "
            "(1, '000001.SZ', 'evt-1', 'resolved', '2026-01-20', '2026-01-03T09:00:00+08:00')"
        )
        connection.execute(
            "INSERT INTO corporate_action_instrument_status VALUES "
            "(1, '000001.SZ', 'cninfo', 'complete', '2026-01-03T09:00:00+08:00')"
        )
        connection.commit()


def test_projection_reuses_evidence_and_is_ready(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    projector = CanonicalCorporateActionProjector(path)

    result = projector.project()
    assert result["network_requests"] == 0
    assert result["inserted"] == 1
    page = BacktestQuoteStore(path).list_canonical_actions(
        instrument_id="000001.SZ", ready_only=True
    )
    assert page["total"] == 1
    assert page["items"][0]["effective_date"] == "2026-01-20"
    assert page["items"][0]["cash_dividend_per_share"] == 0.1

    unchanged = projector.project()
    assert unchanged["inserted"] == 0
    assert unchanged["unchanged"] == 1


def test_projection_blocks_missing_effective_date_and_preserves_raw_evidence(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE corporate_action_effective_date_evidence SET resolution_status = 'unresolved', effective_date = NULL"
        )
        connection.execute(
            "UPDATE corporate_action_observations SET quality_status = 'conflict'"
        )
        connection.commit()
    projector = CanonicalCorporateActionProjector(path)
    result = projector.project()
    assert result["blocked"] == 1
    assert result["status"] == "degraded"
    page = BacktestQuoteStore(path).list_canonical_actions(
        instrument_id="000001.SZ", ready_only=True
    )
    assert page["total"] == 0
    all_rows = BacktestQuoteStore(path).list_canonical_actions(
        instrument_id="000001.SZ"
    )
    assert all_rows["items"][0]["backtest_ready"] == 0
    blockers = __import__("json").loads(all_rows["items"][0]["blocking_reasons_json"])
    assert "effective_date_missing" in blockers
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM corporate_action_observations"
        ).fetchone()[0] == 1


def test_projection_dry_run_does_not_write_rows_or_watermarks(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    projector = CanonicalCorporateActionProjector(path)
    result = projector.project(dry_run=True)
    assert result["would_change"] == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM data_change_log WHERE domain = 'backtest'"
        ).fetchone()[0] == 0


def test_projection_preserves_non_factor_event(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE corporate_action_observations SET action_type = 'shareholder_meeting', "
            "cash_dividend_per_share = NULL"
        )
        connection.execute(
            "UPDATE corporate_action_effective_date_evidence "
            "SET resolution_status = 'unresolved', effective_date = NULL"
        )
        connection.commit()

    CanonicalCorporateActionProjector(path).project()
    page = BacktestQuoteStore(path).list_canonical_actions(
        instrument_id="000001.SZ"
    )
    assert page["total"] == 1
    assert page["items"][0]["action_type"] == "shareholder_meeting"
    assert page["items"][0]["factor_effect"] == 0


def test_projection_ignores_retired_observation_revisions(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO corporate_action_observations VALUES "
            "(2, '000001.SZ', 'cninfo', 'cninfo.v1', 'evt-1', 'cash_dividend', "
            "'2026-01-01', NULL, NULL, NULL, NULL, 0.9, NULL, NULL, NULL, NULL, "
            "'CNY', 'retired', 'accepted', 'retired-hash', 0, "
            "'2026-02-02T09:00:00+08:00')"
        )
        connection.commit()

    CanonicalCorporateActionProjector(path).project()
    page = BacktestQuoteStore(path).list_canonical_actions(
        instrument_id="000001.SZ"
    )
    assert page["total"] == 1
    assert page["items"][0]["cash_dividend_per_share"] == 0.1
    assert page["items"][0]["backtest_ready"] == 1


def test_late_projection_revision_does_not_leak_before_known_at(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    projector = CanonicalCorporateActionProjector(path)
    projector.project()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE corporate_action_observations "
            "SET cash_dividend_per_share = 0.2, updated_at = '2026-02-02T09:00:00+08:00'"
        )
        connection.commit()
    projector.project()

    store = BacktestQuoteStore(path)
    before = store.list_canonical_actions(
        instrument_id="000001.SZ", known_at="2026-01-15T09:00:00+08:00"
    )
    after = store.list_canonical_actions(
        instrument_id="000001.SZ", known_at="2026-02-03T09:00:00+08:00"
    )
    assert before["items"][0]["cash_dividend_per_share"] == 0.1
    assert after["items"][0]["cash_dividend_per_share"] == 0.2
