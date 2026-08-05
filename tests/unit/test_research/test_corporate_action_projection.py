import sqlite3

import pytest

from research.backtest_data.corporate_action_history_backfill import (
    CanonicalCorporateActionHistoryBackfill,
    CorporateActionHistoryCheckpointStore,
    build_source_batches,
    normalize_history_backfill_parameters,
)
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
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        assert "canonical_corporate_action_revisions" not in tables
        assert "data_change_log" not in tables


def test_projection_dry_run_does_not_initialize_missing_canonical_tables(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        tables_before = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    result = CanonicalCorporateActionProjector(path).project(dry_run=True)

    assert result["would_change"] == 1
    with sqlite3.connect(path) as connection:
        tables_after = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert tables_after == tables_before
    assert "canonical_corporate_action_revisions" not in tables_after


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


def test_source_universe_is_deterministic_filtered_and_content_addressed(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO corporate_action_observations VALUES "
            "(3, '000002.SZ', 'cninfo', 'cninfo.v1', 'evt-3', 'cash_dividend', "
            "'2026-01-01', NULL, '2026-01-20', NULL, NULL, 0.3, NULL, NULL, NULL, NULL, "
            "'CNY', 'accepted', 'accepted', 'hash-3', 1, '2026-01-02T09:00:00+08:00'), "
            "(2, '000001.SZ', 'cninfo', 'cninfo.v1', 'evt-2', 'cash_dividend', "
            "'2026-01-01', NULL, '2026-01-20', NULL, NULL, 0.2, NULL, NULL, NULL, NULL, "
            "'CNY', 'accepted', 'accepted', 'hash-2', 1, '2026-01-02T09:00:00+08:00')"
        )
        connection.commit()

    projector = CanonicalCorporateActionProjector(path)
    full = projector.select_source_universe()
    repeated = projector.select_source_universe()
    filtered = projector.select_source_universe(
        instrument_ids=["000001.SZ"], source_event_keys=["evt-2"]
    )

    assert [item["observation_id"] for item in full["items"]] == [1, 2, 3]
    assert repeated["source_universe_hash"] == full["source_universe_hash"]
    assert [item["observation_id"] for item in filtered["items"]] == [2]
    assert filtered["source_universe_hash"] != full["source_universe_hash"]
    batches = build_source_batches(full["items"], batch_size=2)
    assert [batch["count"] for batch in batches] == [2, 1]
    assert batches == build_source_batches(full["items"], batch_size=2)


def test_history_dry_run_reports_without_canonical_or_checkpoint_writes(tmp_path):
    path = tmp_path / "quotes.db"
    checkpoint_root = tmp_path / "checkpoints"
    _create_evidence_db(path)
    store = BacktestQuoteStore(path)
    store.initialize()
    watermark_before = store.readiness()["latest_watermark"]

    result = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=checkpoint_root
    ).run(dry_run=True, batch_size=1)

    assert result["status"] == "dry_run"
    assert result["considered"] == 1
    assert result["ready"] == 1
    assert result["would_change"] == 1
    assert result["network_requests"] == 0
    assert not checkpoint_root.exists()
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM data_change_log WHERE domain = 'backtest'"
        ).fetchone()[0] == 0
    assert store.readiness()["latest_watermark"] == watermark_before


def test_history_dry_run_missing_database_is_read_only_and_unavailable(tmp_path):
    path = tmp_path / "missing" / "quotes.db"
    checkpoint_root = tmp_path / "checkpoints"

    result = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=checkpoint_root
    ).run(dry_run=True)

    assert result["status"] == "unavailable"
    assert result["blockers"] == ["corporate_action_observations_missing"]
    assert not path.exists()
    assert not path.parent.exists()
    assert not checkpoint_root.exists()


def test_history_write_and_explicit_rerun_are_idempotent(tmp_path):
    path = tmp_path / "quotes.db"
    checkpoint_root = tmp_path / "checkpoints"
    _create_evidence_db(path)
    backfill = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=checkpoint_root
    )

    first = backfill.run(dry_run=False, batch_size=1, resume=True)
    second = backfill.run(dry_run=False, batch_size=1, resume=False)

    assert first["inserted"] == 1
    assert first["completed_batches"] == 1
    assert second["inserted"] == 0
    assert second["unchanged"] == 1
    assert second["watermark_changed"] is False
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 1


def test_failed_batch_remains_incomplete_and_resume_retries_only_pending(tmp_path):
    path = tmp_path / "quotes.db"
    checkpoint_root = tmp_path / "checkpoints"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO corporate_action_observations VALUES "
            "(2, '000002.SZ', 'cninfo', 'cninfo.v1', 'evt-2', 'cash_dividend', "
            "'2026-01-01', NULL, '2026-01-20', NULL, NULL, 0.2, NULL, NULL, NULL, NULL, "
            "'CNY', 'accepted', 'accepted', 'hash-2', 1, '2026-01-02T09:00:00+08:00')"
        )
        connection.execute(
            "INSERT INTO corporate_action_instrument_status VALUES "
            "(2, '000002.SZ', 'cninfo', 'complete', '2026-01-03T09:00:00+08:00')"
        )
        connection.commit()
    backfill = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=checkpoint_root
    )
    original_project = backfill.projector.project
    attempted_ids = []

    def fail_second_batch(**kwargs):
        attempted_ids.append(tuple(kwargs["observation_ids"]))
        if kwargs["observation_ids"] == [2]:
            raise RuntimeError("injected batch failure")
        return original_project(**kwargs)

    backfill.projector.project = fail_second_batch
    failed = backfill.run(
        dry_run=False, batch_size=1, resume=True, checkpoint_id="resume_test"
    )
    assert failed["status"] == "failed"
    assert failed["completed_batches"] == 1
    assert attempted_ids == [(1,), (2,)]

    backfill.projector.project = original_project
    resumed = backfill.run(
        dry_run=False, batch_size=1, resume=True, checkpoint_id="resume_test"
    )
    assert resumed["status"] == "success"
    assert resumed["completed_batches"] == 2
    assert resumed["inserted"] == 2
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 2


def test_checkpoint_rejects_parameter_or_frozen_universe_mismatch(tmp_path):
    path = tmp_path / "quotes.db"
    checkpoint_root = tmp_path / "checkpoints"
    _create_evidence_db(path)
    backfill = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=checkpoint_root
    )
    backfill.run(
        dry_run=False, batch_size=1, resume=True, checkpoint_id="frozen_scope"
    )

    with pytest.raises(ValueError, match="parameters"):
        backfill.run(
            dry_run=False,
            batch_size=2,
            resume=True,
            checkpoint_id="frozen_scope",
        )

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE corporate_action_effective_date_evidence "
            "SET effective_date = '2026-01-21', "
            "updated_at = '2026-02-01T09:00:00+08:00' WHERE id = 1"
        )
        connection.commit()
    with pytest.raises(ValueError, match="source universe changed"):
        backfill.run(
            dry_run=False,
            batch_size=1,
            resume=True,
            checkpoint_id="frozen_scope",
        )


def test_batch_rejects_evidence_change_after_universe_selection(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    backfill = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=tmp_path / "checkpoints"
    )
    original_project = backfill.projector.project

    def mutate_before_projection(**kwargs):
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE corporate_action_effective_date_evidence "
                "SET effective_date = '2026-01-21', "
                "updated_at = '2026-02-01T09:00:00+08:00' WHERE id = 1"
            )
            connection.commit()
        return original_project(**kwargs)

    backfill.projector.project = mutate_before_projection
    result = backfill.run(
        dry_run=False,
        batch_size=1,
        resume=True,
        checkpoint_id="concurrent_change",
    )

    assert result["status"] == "failed"
    assert result["completed_batches"] == 0
    assert "source universe changed during batch projection" in result[
        "failed_batches"
    ][0]["error"]
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 0


def test_resume_recovers_committed_batch_before_file_checkpoint_save(tmp_path):
    path = tmp_path / "quotes.db"
    checkpoint_root = tmp_path / "checkpoints"
    _create_evidence_db(path)
    backfill = CanonicalCorporateActionHistoryBackfill(
        path, checkpoint_root=checkpoint_root
    )
    original_save = backfill.checkpoints.save
    save_calls = 0

    def fail_second_save(payload):
        nonlocal save_calls
        save_calls += 1
        if save_calls == 2:
            raise RuntimeError("injected checkpoint persistence failure")
        return original_save(payload)

    backfill.checkpoints.save = fail_second_save
    with pytest.raises(RuntimeError, match="checkpoint persistence"):
        backfill.run(
            dry_run=False,
            batch_size=1,
            resume=True,
            checkpoint_id="commit_recovery",
        )

    backfill.checkpoints.save = original_save
    resumed = backfill.run(
        dry_run=False,
        batch_size=1,
        resume=True,
        checkpoint_id="commit_recovery",
    )

    assert resumed["status"] == "success"
    assert resumed["completed_batches"] == 1
    assert resumed["inserted"] == 1
    assert resumed["unchanged"] == 0
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_revisions"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM canonical_corporate_action_backfill_commits"
        ).fetchone()[0] == 1


def test_failed_dry_run_reports_only_successful_batches(tmp_path):
    path = tmp_path / "quotes.db"
    _create_evidence_db(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO corporate_action_observations VALUES "
            "(2, '000002.SZ', 'cninfo', 'cninfo.v1', 'evt-2', 'cash_dividend', "
            "'2026-01-01', NULL, '2026-01-20', NULL, NULL, 0.2, NULL, NULL, NULL, NULL, "
            "'CNY', 'accepted', 'accepted', 'hash-2', 1, "
            "'2026-01-02T09:00:00+08:00')"
        )
        connection.commit()
    backfill = CanonicalCorporateActionHistoryBackfill(path)
    original_project = backfill.projector.project

    def fail_second_batch(**kwargs):
        if kwargs["observation_ids"] == [2]:
            raise RuntimeError("injected dry-run failure")
        return original_project(**kwargs)

    backfill.projector.project = fail_second_batch
    result = backfill.run(dry_run=True, batch_size=1)

    assert result["status"] == "failed"
    assert result["total_batches"] == 2
    assert result["completed_batches"] == 1
    assert result["considered"] == 1


def test_checkpoint_store_validates_identity_and_checkpoint_id(tmp_path):
    parameters = normalize_history_backfill_parameters(
        db_path=tmp_path / "quotes.db", batch_size=100
    )
    store = CorporateActionHistoryCheckpointStore(tmp_path / "checkpoints")
    assert store.resolve_id(parameters, "universe-hash").startswith("canonical_ca_")
    with pytest.raises(ValueError, match="checkpoint_id"):
        store.resolve_id(parameters, "universe-hash", "../unsafe")
