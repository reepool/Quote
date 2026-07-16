from datetime import date, datetime

import pytest

from utils.a_share_historical_backfill import (
    AShareBackfillCheckpointStore,
    checkpoint_parameter_hash,
    evaluate_calendar_coverage,
    normalize_a_share_backfill_parameters,
)


def test_normalize_a_share_backfill_parameters_accepts_scheduler_strings():
    result = normalize_a_share_backfill_parameters(
        start_date="2020-01-01",
        end_date="2020-12-31",
        exchanges="sse,szse,bse",
        scopes="calendar,quotes,dividends",
        dry_run="false",
        resume="true",
        chunk_size="25",
    )

    assert result["start_date"] == date(2020, 1, 1)
    assert result["end_date"] == date(2020, 12, 31)
    assert result["exchanges"] == ["SSE", "SZSE", "BSE"]
    assert result["scopes"] == ["calendar", "quotes", "dividends"]
    assert result["dry_run"] is False
    assert result["scan_sources"] is False
    assert result["resume"] is True
    assert result["chunk_size"] == 25


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start_date": "2020-02-01", "end_date": "2020-01-01"},
        {"start_date": "bad", "end_date": "2020-01-01"},
        {"start_date": "2020-01-01", "end_date": "2020-02-01", "exchanges": ["HKEX"]},
        {"start_date": "2020-01-01", "end_date": "2020-02-01", "scopes": ["unknown"]},
    ],
)
def test_normalize_a_share_backfill_parameters_rejects_invalid_input(kwargs):
    with pytest.raises(ValueError):
        normalize_a_share_backfill_parameters(**kwargs)


def test_normalize_a_share_backfill_parameters_accepts_read_only_source_scan():
    result = normalize_a_share_backfill_parameters(
        start_date="2020-01-01",
        end_date="2020-12-31",
        scopes="dividends,factors",
        dry_run=True,
        scan_sources="true",
    )

    assert result["dry_run"] is True
    assert result["scan_sources"] is True


def test_normalize_a_share_backfill_parameters_accepts_pending_quote_repair_in_write_mode():
    result = normalize_a_share_backfill_parameters(
        start_date="2020-01-01",
        end_date="2020-12-31",
        scopes="dividends,factors",
        dry_run=False,
        repair_pending_factor_quotes="true",
    )

    assert result["repair_pending_factor_quotes"] is True


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (
            {
                "dry_run": True,
                "scopes": ["factors"],
                "repair_pending_factor_quotes": True,
            },
            "requires write mode",
        ),
        (
            {
                "dry_run": False,
                "scopes": ["dividends"],
                "repair_pending_factor_quotes": True,
            },
            "requires factors scope",
        ),
    ],
)
def test_normalize_a_share_backfill_parameters_rejects_invalid_pending_quote_repair(
    kwargs,
    message,
):
    with pytest.raises(ValueError, match=message):
        normalize_a_share_backfill_parameters(
            start_date="2020-01-01",
            end_date="2020-12-31",
            **kwargs,
        )


@pytest.mark.parametrize(
    "kwargs, message",
    [
        (
            {"dry_run": False, "scan_sources": True, "scopes": ["dividends"]},
            "requires dry_run=true",
        ),
        (
            {"dry_run": True, "scan_sources": True, "scopes": ["quotes"]},
            "requires dividends or factors scope",
        ),
    ],
)
def test_normalize_a_share_backfill_parameters_rejects_invalid_source_scan(
    kwargs,
    message,
):
    with pytest.raises(ValueError, match=message):
        normalize_a_share_backfill_parameters(
            start_date="2020-01-01",
            end_date="2020-12-31",
            **kwargs,
        )


def test_calendar_coverage_respects_bse_inception():
    coverage = evaluate_calendar_coverage(
        "BSE",
        date(1990, 1, 1),
        date(2021, 11, 15),
        [{"date": datetime(2021, 11, 15), "is_trading_day": True}],
    )

    assert coverage["status"] == "success"
    assert coverage["effective_start_date"] == "2021-11-15"
    assert coverage["required_days"] == 1


def test_calendar_coverage_blocks_missing_calendar_dates():
    coverage = evaluate_calendar_coverage(
        "SSE",
        date(2026, 1, 1),
        date(2026, 1, 3),
        [{"date": date(2026, 1, 1)}],
    )

    assert coverage["status"] == "blocked"
    assert coverage["missing_days"] == 2
    assert coverage["missing_samples"] == ["2026-01-02", "2026-01-03"]


def test_checkpoint_store_is_parameter_bound_and_atomic(tmp_path):
    parameters = {
        "start_date": date(2020, 1, 1),
        "end_date": date(2020, 12, 31),
        "exchanges": ["SSE"],
        "scopes": ["quotes"],
    }
    store = AShareBackfillCheckpointStore(tmp_path)
    checkpoint_id = store.resolve_id(parameters)
    payload = store.initialize(
        checkpoint_id,
        parameters,
        [{"instrument_id": "600000.SH", "exchange": "SSE"}],
    )
    payload["stages"]["quotes"] = {"completed_chunks": ["SSE:0:test"]}

    path = store.save(payload)
    loaded = store.load(checkpoint_id, parameters)

    assert path.exists()
    assert loaded["parameter_hash"] == checkpoint_parameter_hash(parameters)
    assert loaded["stages"]["quotes"]["completed_chunks"] == ["SSE:0:test"]
    assert not list(path.parent.glob("*.tmp.*"))

    changed = dict(parameters, end_date=date(2021, 1, 1))
    with pytest.raises(ValueError, match="do not match"):
        store.load(checkpoint_id, changed)


def test_pending_quote_repair_policy_changes_checkpoint_identity(tmp_path):
    store = AShareBackfillCheckpointStore(tmp_path)
    common = {
        "start_date": date(2020, 1, 1),
        "end_date": date(2020, 12, 31),
        "exchanges": ["SSE"],
        "scopes": ["factors"],
    }

    disabled = store.resolve_id({**common, "repair_pending_factor_quotes": False})
    enabled = store.resolve_id({**common, "repair_pending_factor_quotes": True})

    assert disabled != enabled


def test_resume_control_does_not_change_checkpoint_identity(tmp_path):
    store = AShareBackfillCheckpointStore(tmp_path)
    common = {
        "start_date": date(2020, 1, 1),
        "end_date": date(2020, 12, 31),
        "exchanges": ["SSE"],
        "scopes": ["dividends", "factors"],
        "chunk_size": 100,
    }

    disabled = store.resolve_id({**common, "resume": False})
    enabled = store.resolve_id({**common, "resume": True})

    assert disabled == enabled


def test_checkpoint_store_discovers_and_migrates_compatible_legacy_checkpoint(tmp_path):
    store = AShareBackfillCheckpointStore(tmp_path)
    legacy_parameters = {
        "start_date": date(2020, 1, 1),
        "end_date": date(2020, 12, 31),
        "exchanges": ["SSE"],
        "scopes": ["quotes"],
        "resume": False,
    }
    requested_parameters = {**legacy_parameters, "resume": True}
    legacy_id = "a_share_history_legacy123456789"
    payload = store.initialize(legacy_id, legacy_parameters, [])
    payload["parameter_hash"] = "legacy-resume-sensitive-hash"
    store.save(payload)

    resolved = store.resolve_id(requested_parameters, prefer_existing=True)
    loaded = store.load(resolved, requested_parameters)

    assert resolved == legacy_id
    assert loaded["parameter_hash"] == checkpoint_parameter_hash(requested_parameters)


def test_explicit_checkpoint_id_takes_precedence_over_discovery(tmp_path):
    store = AShareBackfillCheckpointStore(tmp_path)
    parameters = {
        "start_date": date(2020, 1, 1),
        "end_date": date(2020, 12, 31),
        "exchanges": ["SSE"],
        "scopes": ["quotes"],
        "resume": True,
    }

    assert (
        store.resolve_id(
            parameters,
            "operator-checkpoint",
            prefer_existing=True,
        )
        == "operator-checkpoint"
    )
