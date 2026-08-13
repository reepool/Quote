import json
from datetime import date, datetime, timedelta

import pytest

from research.backtest_data.index_constituent_history_backfill import (
    CoreIndexConstituentHistoryBackfill,
    normalize_member_code,
    normalize_members,
    plan_observation_dates,
)
from research.backtest_data.quote_store import semantic_hash


def _rows(prefix: str, count: int):
    return [
        {"code": f"{prefix}.{number:06d}", "code_name": f"Member {number}"}
        for number in range(1, count + 1)
    ]


def _trading_days(start: date, end: date):
    cursor = start
    result = []
    while cursor <= end:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor += timedelta(days=1)
    return result


def test_member_code_normalization_and_guardrails():
    assert normalize_member_code("sh.600000") == "600000.SH"
    assert normalize_member_code("sz.000001") == "000001.SZ"
    with pytest.raises(ValueError, match="unsupported BaoStock constituent code"):
        normalize_member_code("bj.920001")

    members = normalize_members("000016.SH", _rows("sh", 50))
    assert len(members) == 50
    assert members[0]["weight"] is None
    assert members[0]["inclusion_metadata"]["weight_readiness"] == "deferred"
    with pytest.raises(ValueError, match="outside guardrail"):
        normalize_members("000016.SH", _rows("sh", 44))


def test_daily_and_monthly_plans_use_trading_dates():
    trading = _trading_days(date(2026, 1, 1), date(2026, 3, 31))
    daily = plan_observation_dates(
        date(2026, 1, 3),
        date(2026, 3, 8),
        trading,
    )
    assert daily[0] == date(2026, 1, 5)
    assert daily[-1] == date(2026, 3, 6)
    dates = plan_observation_dates(
        date(2026, 1, 3), date(2026, 3, 8), trading, sampling="monthly"
    )
    assert dates == [
        date(2026, 1, 5),
        date(2026, 1, 30),
        date(2026, 2, 27),
        date(2026, 3, 6),
    ]


def test_dry_run_estimates_quota_without_fetching(tmp_path):
    calls = []

    async def forbidden_fetch(*args):
        calls.append(args)
        raise AssertionError("dry-run must not fetch")

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=forbidden_fetch,
        quota_reader=lambda: {"count": 100, "limit": 40000, "remaining": 39900},
    )
    plan = service.build_plan(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        trading_dates=_trading_days(date(2026, 1, 1), date(2026, 1, 31)),
        indexes=["000300.SH", "000016.SH"],
        daily_request_reserve=5000,
    )
    result = service.dry_run(plan)
    assert result["status"] == "dry_run"
    assert result["estimated_total_requests"] == 46
    assert result["estimated_batch_requests"] == 46
    assert result["observation_date_count"] == 22
    assert len(result["observation_date_samples"]) == 10
    assert result["network_requests"] == 0
    assert calls == []
    assert not (tmp_path / "quotes.db").exists()


def test_unsupported_index_and_quota_reserve_fail_closed(tmp_path):
    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        quota_reader=lambda: {"count": 39990, "limit": 40000, "remaining": 10},
    )
    with pytest.raises(ValueError, match="unsupported historical indexes"):
        service.build_plan(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
            trading_dates=[date(2026, 1, 30)],
            indexes=["000852.SH"],
            daily_request_reserve=5,
        )
    plan = service.build_plan(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        trading_dates=[date(2026, 1, 30)],
        indexes=["000300.SH"],
        daily_request_reserve=9,
    )
    assert service.dry_run(plan)["blockers"] == [
        "insufficient_baostock_quota_headroom"
    ]


def test_plan_excludes_pre_launch_dates_per_index(tmp_path):
    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=date(2005, 4, 7),
        end_date=date(2005, 4, 8),
        trading_dates=[date(2005, 4, 7), date(2005, 4, 8)],
        indexes=["000016.SH", "000300.SH", "000905.SH"],
        daily_request_reserve=5000,
    )
    assert plan.dates_for_index("000016.SH") == (
        date(2005, 4, 7), date(2005, 4, 8)
    )
    assert plan.dates_for_index("000300.SH") == (date(2005, 4, 8),)
    assert plan.dates_for_index("000905.SH") == ()
    assert plan.query_count == 3


@pytest.mark.asyncio
async def test_backfill_collapses_unchanged_builds_intervals_and_replays(tmp_path):
    observations = {
        date(2026, 1, 5): _rows("sh", 50),
        date(2026, 1, 30): _rows("sh", 50),
        date(2026, 2, 27): _rows("sh", 49) + [{"code": "sz.000001"}],
    }
    calls = []

    async def fetch(index_id, observation_date):
        calls.append((index_id, observation_date))
        return observations[observation_date]

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 2, 27),
        trading_dates=observations,
        indexes=["000016.SH"],
        daily_request_reserve=5000,
        sampling="monthly",
    )
    first = await service.run(plan)
    assert first["status"] == "success"
    assert first["inserted"] == 2
    assert first["collapsed_observations"] == 1
    assert first["validity"]["inserted"] == 2
    assert len(calls) == 3
    with service.store.connection() as connection:
        snapshot_basis = connection.execute(
            "SELECT DISTINCT validity_basis FROM index_composition_snapshots"
        ).fetchall()
        validity_basis = connection.execute(
            "SELECT DISTINCT basis FROM index_composition_validity_revisions"
        ).fetchall()
    assert [row[0] for row in snapshot_basis] == ["monthly_source_observation"]
    assert [row[0] for row in validity_basis] == ["monthly_source_observation"]

    page = service.store.list_index_constituents(
        "000016.SH",
        as_of_date="2026-01-20",
        known_at="2099-01-01T00:00:00+08:00",
    )
    assert page["status"] == "success"
    assert page["readiness"] == {"membership": "ready", "weights": "deferred"}
    assert all(item["weight"] is None for item in page["items"])
    before_acquisition = service.store.list_index_constituents(
        "000016.SH",
        as_of_date="2026-01-20",
        known_at="2020-01-01T00:00:00+08:00",
    )
    assert before_acquisition["status"] == "unavailable"
    beyond_frontier = service.store.list_index_constituents(
        "000016.SH",
        as_of_date="2026-03-01",
        strict=False,
    )
    assert beyond_frontier["status"] == "unavailable"

    second = await service.run(plan)
    assert second["network_requests"] == 0
    assert second["inserted"] == 0
    assert second["validity"]["unchanged"] == 2
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_acquired_checkpoint_replays_without_second_network_call(tmp_path, monkeypatch):
    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=date(2026, 1, 30),
        end_date=date(2026, 1, 30),
        trading_dates=[date(2026, 1, 30)],
        indexes=["000016.SH"],
        daily_request_reserve=5000,
        sampling="monthly",
    )
    members = normalize_members("000016.SH", _rows("sh", 50))
    member_hash = semantic_hash({
        "members": [item["constituent_instrument_id"] for item in members]
    })
    service.checkpoints.save({
        "plan_hash": plan.identity,
        "plan": plan.payload(),
        "completed_units": {
            "000016.SH:2026-01-30": {
                "status": "acquired",
                "member_hash": member_hash,
                "acquired_at": "2026-08-05T12:00:00+08:00",
                "members": members,
            }
        },
        "indexes": {},
    })

    async def forbidden(*args):
        raise AssertionError("acquired checkpoint must not refetch")

    service.fetcher = forbidden
    result = await service.run(plan)
    assert result["network_requests"] == 0
    assert result["inserted"] == 1


@pytest.mark.asyncio
async def test_batch_frontier_advances_only_after_resume(tmp_path):
    trading = [date(2026, 1, day) for day in (5, 6, 7, 8)]
    calls = []

    async def fetch(index_id, observation_date):
        calls.append(observation_date)
        return _rows("sh", 50)

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=trading[0],
        end_date=trading[-1],
        trading_dates=trading,
        indexes=["000016.SH"],
        daily_request_reserve=5000,
        max_queries_per_run=2,
    )
    first = await service.run(plan)
    assert first["status"] == "partial"
    assert first["blockers"] == ["batch_query_limit_reached"]
    assert service.store.list_index_constituents(
        "000016.SH", as_of_date="2026-01-07", strict=False
    )["status"] == "unavailable"

    second = await service.run(plan)
    assert second["status"] == "success"
    assert service.store.list_index_constituents(
        "000016.SH", as_of_date="2026-01-08", strict=False
    )["status"] == "success"
    assert calls == trading


@pytest.mark.asyncio
async def test_closed_interval_evidence_stays_immutable_across_later_batches(tmp_path):
    trading = [date(2026, 1, day) for day in (5, 6, 7, 8)]
    changed = _rows("sh", 49) + [{"code": "sz.000001"}]

    async def fetch(index_id, observation_date):
        return _rows("sh", 50) if observation_date == trading[0] else changed

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=trading[0],
        end_date=trading[-1],
        trading_dates=trading,
        indexes=["000016.SH"],
        daily_request_reserve=5000,
        max_queries_per_run=2,
    )

    assert (await service.run(plan))["status"] == "partial"
    assert (await service.run(plan))["status"] == "success"

    with service.store.connection() as connection:
        rows = connection.execute(
            "SELECT valid_from, evidence_json FROM index_composition_validity_revisions "
            "ORDER BY valid_from, decision_available_at"
        ).fetchall()
    frontiers = {
        row["valid_from"]: json.loads(row["evidence_json"])["observation_frontier"]
        for row in rows
    }
    assert frontiers[trading[0].isoformat()] == trading[1].isoformat()
    assert frontiers[trading[1].isoformat()] == trading[-1].isoformat()


@pytest.mark.asyncio
async def test_database_replay_is_idempotent_after_checkpoint_loss(tmp_path):
    calls = []

    async def fetch(index_id, observation_date):
        calls.append(observation_date)
        return _rows("sh", 50)

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        trading_dates=[date(2026, 1, 5)],
        indexes=["000016.SH"],
        daily_request_reserve=5000,
    )
    first = await service.run(plan)
    assert first["inserted"] == 1
    (tmp_path / "checkpoint.json").unlink()

    replay = await service.run(plan)

    assert replay["inserted"] == 0
    assert replay["unchanged"] == 1
    assert replay["validity"]["unchanged"] == 1
    assert calls == [date(2026, 1, 5), date(2026, 1, 5)]


@pytest.mark.asyncio
async def test_checkpoint_loss_replay_reuses_unchanged_frontier_validity(tmp_path):
    calls = []

    async def fetch(index_id, observation_date):
        calls.append(observation_date)
        return _rows("sh", 50)

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    dates = [date(2026, 1, 5), date(2026, 1, 6)]
    plan = service.build_plan(
        start_date=dates[0],
        end_date=dates[-1],
        trading_dates=dates,
        indexes=["000016.SH"],
        daily_request_reserve=5000,
    )
    first = await service.run(plan)
    assert first["validity"]["inserted"] == 1
    (tmp_path / "checkpoint.json").unlink()

    replay = await service.run(plan)

    assert replay["validity"] == {"inserted": 0, "unchanged": 1}
    with service.store.connection() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM index_composition_validity_revisions"
        ).fetchone()[0] == 1
    assert calls == dates + dates


@pytest.mark.asyncio
async def test_daily_snapshot_records_post_response_acquisition_time_and_basis(tmp_path):
    response_completed_at = None

    async def fetch(index_id, observation_date):
        nonlocal response_completed_at
        response_completed_at = datetime.now().astimezone()
        return _rows("sh", 50)

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    plan = service.build_plan(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        trading_dates=[date(2026, 1, 5)],
        indexes=["000016.SH"],
        daily_request_reserve=5000,
        sampling="daily",
    )

    result = await service.run(plan)

    assert result["status"] == "success"
    with service.store.connection() as connection:
        snapshot = connection.execute(
            "SELECT available_at, validity_basis FROM index_composition_snapshots"
        ).fetchone()
        validity = connection.execute(
            "SELECT basis FROM index_composition_validity_revisions"
        ).fetchone()
    assert datetime.fromisoformat(snapshot["available_at"]) >= response_completed_at
    assert snapshot["validity_basis"] == "daily_source_observation"
    assert validity["basis"] == "daily_source_observation"


@pytest.mark.asyncio
async def test_write_rechecks_daily_reserve_before_each_query(tmp_path):
    remaining = [5003, 5002, 5001]
    calls = []

    def quota_reader():
        value = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        return {"count": 40000 - value, "limit": 40000, "remaining": value}

    async def fetch(index_id, observation_date):
        calls.append(observation_date)
        return _rows("sh", 50)

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=quota_reader,
    )
    trading_dates = [date(2026, 1, 5), date(2026, 1, 6)]
    plan = service.build_plan(
        start_date=trading_dates[0],
        end_date=trading_dates[-1],
        trading_dates=trading_dates,
        indexes=["000016.SH"],
        daily_request_reserve=5000,
    )

    result = await service.run(plan)

    assert result["status"] == "partial"
    assert result["blockers"] == ["insufficient_baostock_quota_headroom"]
    assert result["network_requests"] == 1
    assert calls == [date(2026, 1, 5)]


@pytest.mark.asyncio
async def test_quality_failure_is_checkpointed_but_retried(tmp_path):
    calls = []

    async def fetch(index_id, observation_date):
        calls.append(observation_date)
        return []

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=tmp_path / "quotes.db",
        checkpoint_path=tmp_path / "checkpoint.json",
        fetcher=fetch,
        quota_reader=lambda: {"count": 0, "limit": 40000, "remaining": 40000},
    )
    observation_date = date(2026, 1, 5)
    plan = service.build_plan(
        start_date=observation_date,
        end_date=observation_date,
        trading_dates=[observation_date],
        indexes=["000016.SH"],
        daily_request_reserve=5000,
    )

    first = await service.run(plan)
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text())
    unit = checkpoint["completed_units"]["000016.SH:2026-01-05"]
    assert first["failures"][0]["status"] == "quality_failure"
    assert unit["status"] == "quality_failure"
    assert "outside guardrail" in unit["reason"]

    second = await service.run(plan)
    assert second["network_requests"] == 1
    assert calls == [observation_date, observation_date]
