import sqlite3

import pytest

from research.backtest_data.quote_store import BacktestQuoteStore


T1 = "2026-01-02T09:00:00+08:00"
T2 = "2026-02-02T09:00:00+08:00"
T3 = "2026-03-02T09:00:00+08:00"


@pytest.fixture
def store(tmp_path):
    result = BacktestQuoteStore(tmp_path / "quotes.db")
    result.initialize()
    return result


def _snapshot(snapshot_id="snap-1", *, effective_date="2026-01-01", available_at=T1):
    return {
        "snapshot_id": snapshot_id,
        "revision_id": f"revision-{snapshot_id}",
        "index_instrument_id": "000300.SH",
        "effective_date": effective_date,
        "available_at": available_at,
        "availability_quality": "source_publication",
        "source": "official_fixture",
        "source_profile": "official_fixture.v1",
        "artifact_hash": f"artifact-{snapshot_id}",
        "weight_unit": "percent",
        "completeness_state": "complete",
    }


def _members():
    return [
        {"source_symbol": "000001.SZ", "weight": 0.6},
        {"source_symbol": "600000.SH", "weight": 0.4},
    ]


def _validity(revision_id="validity-1", *, available_at=T1, valid_to=None):
    return {
        "validity_revision_id": revision_id,
        "valid_from": "2026-01-01",
        "valid_to_exclusive": valid_to,
        "decision_available_at": available_at,
        "availability_quality": "source_publication",
        "basis": "official_rebalance",
    }


def test_initialize_is_additive_and_idempotent(store):
    with sqlite3.connect(store.db_path) as connection:
        connection.execute("CREATE TABLE unrelated_user_table (value TEXT)")
        connection.execute("INSERT INTO unrelated_user_table VALUES ('preserved')")
        connection.commit()

    store.initialize()

    with sqlite3.connect(store.db_path) as connection:
        assert connection.execute("SELECT value FROM unrelated_user_table").fetchone()[0] == "preserved"
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert "index_composition_snapshots" in tables
    assert "data_change_log" in tables


def test_initialize_migrates_legacy_security_intervals(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE security_state_interval_revisions ("
            "interval_revision_id TEXT PRIMARY KEY, instrument_id TEXT NOT NULL, "
            "state TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to_exclusive TEXT, "
            "decision_available_at TEXT, availability_quality TEXT, confidence TEXT NOT NULL, "
            "status TEXT NOT NULL, input_event_ids_json TEXT NOT NULL DEFAULT '[]', "
            "evidence_json TEXT NOT NULL DEFAULT '{}', semantic_hash TEXT NOT NULL, "
            "created_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO security_state_interval_revisions VALUES "
            "('legacy-r1', '000001.SZ', 'normal', '2025-01-01', NULL, ?, "
            "'source_timestamp', 'official', 'confirmed', '[]', '{}', 'hash-1', ?)",
            (T1, T1),
        )
        connection.commit()

    migrated = BacktestQuoteStore(path)
    migrated.initialize()
    migrated.initialize()

    with sqlite3.connect(path) as connection:
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(security_state_interval_revisions)"
            )
        }
        row = connection.execute(
            "SELECT interval_key FROM security_state_interval_revisions "
            "WHERE interval_revision_id = 'legacy-r1'"
        ).fetchone()
    assert "interval_key" in columns
    assert row[0] == "000001.SZ:2025-01-01"


def test_index_snapshot_is_immutable_idempotent_and_paginated(store):
    result = store.upsert_index_snapshot(
        snapshot=_snapshot(), members=_members(), validity=_validity()
    )
    assert result == {"status": "inserted", "snapshot_id": "snap-1", "member_count": 2}
    assert store.upsert_index_snapshot(
        snapshot=_snapshot(), members=_members(), validity=_validity()
    )["status"] == "unchanged"

    with pytest.raises(ValueError, match="immutable index snapshot"):
        store.upsert_index_snapshot(
            snapshot=_snapshot(),
            members=[{"source_symbol": "000001.SZ", "weight": 1.0}],
        )

    page = store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", known_at=T1, limit=1
    )
    assert page["status"] == "success"
    assert page["total"] == 2
    assert page["items"][0]["source_symbol"] == "000001.SZ"
    second = store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", known_at=T1, limit=1, offset=1
    )
    assert second["items"][0]["source_symbol"] == "600000.SH"


def test_index_known_at_filters_snapshot_and_later_validity_independently(store):
    store.upsert_index_snapshot(snapshot=_snapshot(), members=_members())

    before = store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", known_at=T1
    )
    assert before["reason"] == "validity_evidence_missing"

    assert store.append_index_validity(
        snapshot_id="snap-1", validity=_validity(available_at=T2)
    )["status"] == "inserted"
    assert store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", known_at=T1
    )["status"] == "unavailable"
    after = store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", known_at=T2
    )
    assert after["status"] == "success"

    assert store.append_index_validity(
        snapshot_id="snap-1", validity=_validity(available_at=T2)
    )["status"] == "unchanged"
    with pytest.raises(ValueError, match="immutable index validity"):
        store.append_index_validity(
            snapshot_id="snap-1",
            validity={**_validity(available_at=T2), "basis": "changed"},
        )


def test_strict_index_read_requires_knowledge_cutoff(store):
    store.upsert_index_snapshot(
        snapshot=_snapshot(), members=_members(), validity=_validity()
    )
    result = store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", strict=True
    )
    assert result["status"] == "unavailable"
    assert result["reason"] == "known_at_required_for_strict_pit"


def test_index_validity_bounds_non_strict_reads_and_reports_readiness(store):
    store.upsert_index_snapshot(
        snapshot={**_snapshot(), "weight_unit": None},
        members=[{**item, "weight": None} for item in _members()],
        validity={**_validity(), "valid_to_exclusive": "2026-02-01", "evidence": {
            "membership_readiness": "ready", "weight_readiness": "deferred"
        }},
    )
    inside = store.list_index_constituents(
        "000300.SH", as_of_date="2026-01-15", strict=False
    )
    assert inside["status"] == "success"
    assert inside["readiness"] == {"membership": "ready", "weights": "deferred"}
    outside = store.list_index_constituents(
        "000300.SH", as_of_date="2026-02-01", strict=False
    )
    assert outside["status"] == "unavailable"


def test_security_interval_revision_selection_and_conflict_fail_closed(store):
    base = {
        "instrument_id": "000001.SZ",
        "valid_from": "2026-01-01",
        "decision_available_at": T1,
        "confidence": "official",
        "status": "confirmed",
    }
    store.append_security_interval(
        {**base, "interval_revision_id": "i-1", "state": "normal"}
    )
    store.append_security_interval(
        {
            **base,
            "interval_revision_id": "i-2",
            "state": "st",
            "decision_available_at": T2,
        }
    )
    assert store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-15", known_at=T1
    )["state"] == "normal"
    assert store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-15", known_at=T2
    )["state"] == "st"

    store.append_security_interval(
        {
            **base,
            "interval_revision_id": "overlap",
            "state": "delisting",
            "valid_from": "2026-01-10",
            "decision_available_at": T2,
        }
    )
    blocked = store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-15", known_at=T2
    )
    assert blocked["status"] == "unavailable"
    assert blocked["reason"] == "state_interval_conflict"


def test_security_event_known_at_and_current_observation_idempotency(store):
    event = {
        "event_id": "event-1",
        "instrument_id": "000001.SZ",
        "event_type": "st_started",
        "new_state": "st",
        "effective_date": "2026-01-10",
        "available_at": T2,
        "source": "exchange",
        "source_profile": "exchange.v1",
        "quality": "official",
    }
    assert store.append_security_event(event)["status"] == "inserted"
    assert store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-15", known_at=T1
    )["status"] == "unavailable"
    assert store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-15", known_at=T2
    )["state"] == "st"

    observation = {
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "exchange": "SZSE",
        "state": "normal",
        "observed_at": T1,
        "source_profile": "master.v1",
    }
    assert store.upsert_current_security_observation(observation)["status"] == "inserted"
    later_same = {**observation, "observed_at": T2, "available_at": T2}
    assert store.upsert_current_security_observation(later_same)["status"] == "unchanged"
    changed = {**later_same, "state": "st"}
    assert store.upsert_current_security_observation(changed)["transition"] == {
        "prior_state": "normal",
        "new_state": "st",
    }


def test_current_observation_is_forward_only_and_pending_event_fails_closed(store):
    store.upsert_current_security_observation(
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "state": "normal",
            "observed_at": T2,
            "source_profile": "master.v1",
        }
    )
    assert store.resolve_security_state(
        "000001.SZ", effective_date="2026-01-15", known_at=T3
    )["status"] == "unavailable"
    assert store.resolve_security_state(
        "000001.SZ", effective_date="2026-02-15", known_at=T3
    )["state"] == "normal"
    store.append_security_event(
        {
            "event_id": "pending-delisting",
            "instrument_id": "000001.SZ",
            "event_type": "delisting_risk",
            "new_state": "pending_delisting",
            "effective_date": "2026-02-10",
            "available_at": T2,
            "source": "announcement",
            "source_profile": "official_announcement.v1",
            "quality": "pending",
        }
    )
    blocked = store.resolve_security_state(
        "000001.SZ", effective_date="2026-02-15", known_at=T3
    )
    assert blocked["status"] == "unavailable"
    assert blocked["reason"] == "state_event_quality_blocked"


def test_price_limit_provenance_and_known_at(store):
    reported = {
        "revision_id": "limit-reported",
        "instrument_id": "000001.SZ",
        "trade_date": "2026-01-15",
        "limit_up": 11.0,
        "limit_down": 9.0,
        "reference_price": 10.0,
        "source_mode": "source_reported",
        "source": "exchange",
        "source_profile": "exchange.v1",
        "decision_available_at": T2,
        "quality": "official",
    }
    store.append_price_limit(reported)
    assert store.resolve_price_limit(
        "000001.SZ", trade_date="2026-01-15", known_at=T1
    )["status"] == "unavailable"
    assert store.resolve_price_limit(
        "000001.SZ", trade_date="2026-01-15", known_at=T2
    )["status"] == "success"
    with pytest.raises(ValueError, match="immutable price-limit"):
        store.append_price_limit({**reported, "limit_up": 12.0})

    with pytest.raises(ValueError, match="raw prior close"):
        store.append_price_limit(
            {
                **reported,
                "revision_id": "derived-bad",
                "source_mode": "derived_rule",
                "inputs": {"reference_price_basis": "raw_prior_close"},
            }
        )
    derived = {
        **reported,
        "revision_id": "derived-good",
        "source_mode": "derived_rule",
        "source": "rules_engine",
        "rule_version": "cn-limit.v1",
        "inputs": {
            "reference_price_basis": "exchange_ex_right_adjusted",
            "board": "main",
            "st_state": "normal",
            "tick_size": 0.01,
        },
        "quality": "derived_complete",
        "decision_available_at": T3,
    }
    assert store.append_price_limit(derived)["status"] == "inserted"
    assert store.resolve_price_limit(
        "000001.SZ", trade_date="2026-01-15", known_at=T3
    )["evidence"]["source_mode"] == "derived_rule"


def test_canonical_actions_are_changed_only_and_point_in_time(store):
    base = {
        "canonical_event_id": "ca-1",
        "instrument_id": "000001.SZ",
        "action_type": "cash_dividend",
        "effective_date": "2026-01-20",
        "cash_dividend_per_share": 0.1,
        "factor_effect": True,
        "backtest_ready": True,
        "lifecycle_applicability": "listed",
        "coverage_state": "complete",
        "quality_state": "accepted",
        "source_lineage": {"observation_ids": [1]},
        "decision_available_at": T1,
    }
    assert store.append_canonical_action(
        {**base, "projection_revision_id": "ca-r1"}
    )["status"] == "inserted"
    assert store.append_canonical_action(
        {**base, "projection_revision_id": "ca-r1"}
    )["status"] == "unchanged"
    changed = {
        **base,
        "projection_revision_id": "ca-r2",
        "cash_dividend_per_share": 0.2,
        "decision_available_at": T2,
    }
    assert store.append_canonical_action(changed)["status"] == "inserted"

    before = store.list_canonical_actions(instrument_id="000001.SZ", known_at=T1)
    after = store.list_canonical_actions(instrument_id="000001.SZ", known_at=T2)
    assert before["total"] == after["total"] == 1
    assert before["items"][0]["projection_revision_id"] == "ca-r1"
    assert after["items"][0]["projection_revision_id"] == "ca-r2"


def test_canonical_action_batch_is_atomic(store):
    base = {
        "canonical_event_id": "ca-batch",
        "projection_revision_id": "ca-batch-r1",
        "instrument_id": "000001.SZ",
        "action_type": "cash_dividend",
        "effective_date": "2026-01-20",
        "cash_dividend_per_share": 0.1,
        "factor_effect": True,
        "backtest_ready": True,
        "lifecycle_applicability": "applicable",
        "coverage_state": "complete",
        "quality_state": "accepted",
        "decision_available_at": T1,
    }
    conflicting = {**base, "cash_dividend_per_share": 0.2}

    with pytest.raises(ValueError, match="immutable canonical projection revision"):
        store.append_canonical_actions([base, conflicting])

    with sqlite3.connect(store.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM data_change_log WHERE domain = 'backtest'"
        ).fetchone()[0] == 0


def test_canonical_change_cursor_filters_business_events(store):
    first = {
        "canonical_event_id": "ca-1",
        "projection_revision_id": "r1",
        "instrument_id": "000001.SZ",
        "action_type": "cash_dividend",
        "effective_date": "2026-01-20",
        "backtest_ready": True,
        "lifecycle_applicability": "applicable",
        "coverage_state": "complete",
        "quality_state": "accepted",
        "decision_available_at": T1,
    }
    store.append_canonical_action(first)
    cursor = store.read_changes()["next_cursor"]
    store.append_canonical_action(
        {
            **first,
            "canonical_event_id": "ca-2",
            "projection_revision_id": "r2",
            "instrument_id": "600000.SH",
        }
    )
    page = store.list_canonical_actions(change_cursor=cursor)
    assert page["total"] == 1
    assert page["items"][0]["canonical_event_id"] == "ca-2"


def test_change_cursor_is_database_scoped_and_unchanged_writes_do_not_advance(store):
    store.upsert_index_snapshot(
        snapshot=_snapshot(), members=_members(), validity=_validity()
    )
    first = store.read_changes(limit=1)
    assert first["database_id"] == "quotes"
    assert len(first["items"]) == 1

    store.upsert_index_snapshot(
        snapshot=_snapshot(), members=_members(), validity=_validity()
    )
    resumed = store.read_changes(cursor=first["next_cursor"])
    assert resumed["items"] == []

    other = BacktestQuoteStore(store.db_path)
    other.database_id = "financials"
    with pytest.raises(ValueError, match="cursor scope"):
        other.read_changes(cursor=first["next_cursor"])


def test_naive_knowledge_timestamps_are_rejected(store):
    with pytest.raises(ValueError, match="timezone-aware"):
        store.list_index_constituents(
            "000300.SH", as_of_date="2026-01-01", known_at="2026-01-01T09:00:00"
        )
