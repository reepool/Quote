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
