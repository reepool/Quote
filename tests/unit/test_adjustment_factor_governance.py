from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.adjustment_factor_governance import (
    build_event_product_path,
    build_canonical_series,
    compare_normalized_cumulative_paths,
    normalize_source_path,
    rebase_legacy_tail,
    reconcile_factor_events,
)


def _observation(
    instrument_id: str,
    ex_date: str,
    factor: float,
    cumulative: float,
    *,
    source: str = "akshare",
):
    return {
        "instrument_id": instrument_id,
        "ex_date": datetime.fromisoformat(ex_date),
        "source": source,
        "source_profile": "unit",
        "normalized_factor": factor,
        "provider_cumulative_factor": cumulative,
        "quality_status": "valid",
    }


def test_normalization_uses_adjacent_ratio_and_flags_provider_conflict():
    rows = [
        {
            "instrument_id": "000001.SZ",
            "ex_date": "2020-05-28",
            "factor": 1.02,
            "cumulative_factor": 10.0,
            "source": "akshare",
        },
        {
            "instrument_id": "000001.SZ",
            "ex_date": "2021-05-28",
            "factor": 1.2,
            "cumulative_factor": 12.0,
            "source": "akshare",
        },
        {
            "instrument_id": "000001.SZ",
            "ex_date": "2021-12-31",
            "factor": 1.05,
            "cumulative_factor": 9.6,
            "source": "akshare",
        },
    ]

    normalized = normalize_source_path(rows)

    assert normalized[1]["normalized_factor"] == pytest.approx(1.2)
    assert normalized[1]["quality_status"] == "valid"
    assert normalized[2]["normalized_factor"] == pytest.approx(0.8)
    assert normalized[2]["quality_status"] == "provider_factor_conflict"


def test_canonical_series_uses_unit_baseline_and_counts_completed_empty_instrument():
    observations = [
        _observation("000001.SZ", "2020-05-28", 1.1, 10.0),
        _observation("000001.SZ", "2021-05-28", 1.2, 12.0),
    ]

    rows, summary = build_canonical_series(
        observations,
        target_instruments=["000001.SZ", "000002.SZ"],
        completed_sources={"akshare": ["000001.SZ", "000002.SZ"]},
    )

    assert [row["cumulative_factor"] for row in rows] == pytest.approx([1.1, 1.32])
    assert summary["covered_instruments"] == 2
    assert summary["built_instruments"] == 1


def test_legacy_tail_rebases_new_rows_and_skips_historical_rows():
    rows = [
        _observation("000001.SZ", "2020-05-28", 1.1, 10.0),
        _observation("000001.SZ", "2026-05-28", 1.2, 12.0),
    ]

    prepared, stats = rebase_legacy_tail(
        rows,
        latest_date="2025-01-01",
        latest_cumulative_factor=8.0,
    )

    assert stats == {"rebased": 1, "historical_skipped": 1, "invalid": 0}
    assert prepared[0]["factor"] == pytest.approx(1.2)
    assert prepared[0]["cumulative_factor"] == pytest.approx(9.6)
    assert prepared[0]["source"] == "akshare"


def test_normalized_path_comparison_ignores_scale_but_detects_reset():
    candidate = [
        {"instrument_id": "000001.SZ", "ex_date": "2020-05-28", "cumulative_factor": 1.2},
        {"instrument_id": "000001.SZ", "ex_date": "2021-05-28", "cumulative_factor": 1.44},
    ]
    scale_equivalent = [
        {"instrument_id": "000001.SZ", "ex_date": "2020-05-28", "cumulative_factor": 120.0},
        {"instrument_id": "000001.SZ", "ex_date": "2021-05-28", "cumulative_factor": 144.0},
    ]
    reset_path = [
        {"instrument_id": "000001.SZ", "ex_date": "2020-05-28", "cumulative_factor": 120.0},
        {"instrument_id": "000001.SZ", "ex_date": "2021-05-28", "cumulative_factor": 99.0},
    ]

    equivalent = compare_normalized_cumulative_paths(candidate, scale_equivalent)
    reset = compare_normalized_cumulative_paths(candidate, reset_path)

    assert equivalent["max_adjusted_price_error_pct"] == pytest.approx(0.0)
    assert reset["max_adjusted_price_error_pct"] > 30.0


def test_event_product_path_ignores_stored_tdx_cumulative_reset():
    rows = [
        {
            "instrument_id": "600000.SH",
            "ex_date": "2025-07-16",
            "factor": 1.03,
            "cumulative_factor": 16.60,
        },
        {
            "instrument_id": "600000.SH",
            "ex_date": "2026-07-16",
            "factor": 1.04,
            "cumulative_factor": 1.04,
        },
    ]

    rebuilt = build_event_product_path(rows)

    assert [item["cumulative_factor"] for item in rebuilt] == pytest.approx([
        1.03,
        1.0712,
    ])


def test_event_reconciliation_accepts_one_session_provider_date_shift():
    candidate = [{
        "instrument_id": "000001.SZ",
        "ex_date": "2020-05-29",
        "factor": 1.02,
    }]
    tdx = [{
        "instrument_id": "000001.SZ",
        "ex_date": "2020-05-28",
        "factor": 1.0201,
    }]

    result = reconcile_factor_events(
        candidate,
        tdx,
        sessions_by_exchange={"SZSE": [date(2020, 5, 28), date(2020, 5, 29)]},
        factor_tolerance_pct=0.5,
    )

    assert result["exact_matches"] == 0
    assert result["shifted_matches"] == 1
    assert result["factor_conflicts"] == 0
    assert result["discrepancy_ratio"] == pytest.approx(0.0)


def test_event_reconciliation_does_not_let_exact_conflict_steal_shifted_match():
    candidate = [{
        "instrument_id": "000001.SZ",
        "ex_date": "2020-05-29",
        "factor": 1.02,
    }]
    tdx = [
        {
            "instrument_id": "000001.SZ",
            "ex_date": "2020-05-29",
            "factor": 1.50,
        },
        {
            "instrument_id": "000001.SZ",
            "ex_date": "2020-05-28",
            "factor": 1.0201,
        },
    ]

    result = reconcile_factor_events(
        candidate,
        tdx,
        sessions_by_exchange={"SZSE": [date(2020, 5, 28), date(2020, 5, 29)]},
        factor_tolerance_pct=0.5,
    )

    assert result["shifted_matches"] == 1
    assert result["factor_conflicts"] == 0
    assert result["tdx_only"] == 1


@pytest.mark.asyncio
async def test_a_share_persistence_writes_observation_before_rebased_legacy():
    manager = DataManager()
    manager.data_config = {
        "adjustment_factor_governance": {
            "write_source_observations": True,
            "rebase_legacy_appends": True,
        }
    }
    calls = []
    manager.db_ops = Mock()

    async def save_observations(rows, ingestion_run_id=None):
        calls.append(("observations", rows, ingestion_run_id))
        return {"inserted": len(rows), "changed": 0, "unchanged": 0, "failed": 0}

    async def prepare(rows):
        calls.append(("prepare", rows))
        return ([{
            "instrument_id": "000001.SZ",
            "ex_date": datetime(2026, 5, 28),
            "factor": 1.02,
            "cumulative_factor": 8.16,
            "source": "akshare",
        }], {"rebased": 1, "historical_skipped": 0, "invalid": 0})

    async def save_legacy(rows):
        calls.append(("legacy", rows))
        return len(rows)

    manager.db_ops.save_adjustment_factor_observations = AsyncMock(side_effect=save_observations)
    manager.db_ops.prepare_legacy_factor_appends = AsyncMock(side_effect=prepare)
    manager.db_ops.save_adjustment_factors = AsyncMock(side_effect=save_legacy)

    result = await manager._persist_adjustment_factor_batch(
        "SZSE",
        [{
            "instrument_id": "000001.SZ",
            "ex_date": datetime(2026, 5, 28),
            "factor": 1.02,
            "cumulative_factor": 18.2,
            "source": "akshare",
        }],
        ingestion_run_id="unit-run",
    )

    assert [call[0] for call in calls] == ["observations", "prepare", "legacy"]
    assert calls[0][1][0]["provider_cumulative_factor"] == pytest.approx(18.2)
    assert calls[2][1][0]["cumulative_factor"] == pytest.approx(8.16)
    assert result["saved"] == 1


@pytest.mark.asyncio
async def test_a_share_persistence_stops_when_observation_write_is_incomplete():
    manager = DataManager()
    manager.data_config = {"adjustment_factor_governance": {}}
    manager.db_ops = Mock()
    manager.db_ops.save_adjustment_factor_observations = AsyncMock(return_value={
        "inserted": 0,
        "changed": 0,
        "unchanged": 0,
        "failed": 1,
    })
    manager.db_ops.prepare_legacy_factor_appends = AsyncMock()
    manager.db_ops.save_adjustment_factors = AsyncMock()

    with pytest.raises(RuntimeError, match="persistence incomplete"):
        await manager._persist_adjustment_factor_batch(
            "SZSE",
            [{
                "instrument_id": "000001.SZ",
                "ex_date": datetime(2026, 5, 28),
                "factor": 1.02,
                "cumulative_factor": 18.2,
                "source": "akshare",
            }],
        )

    manager.db_ops.prepare_legacy_factor_appends.assert_not_awaited()
    manager.db_ops.save_adjustment_factors.assert_not_awaited()


@pytest.mark.asyncio
async def test_factor_cache_uses_eligible_canonical_and_explicit_fallback():
    manager = DataManager()
    manager._factor_cache = {}
    manager.data_config = {
        "adjustment_factor_governance": {
            "read_dataset": "canonical",
            "canonical_series_version": "v1",
            "allow_legacy_fallback": True,
        }
    }
    manager.db_ops = Mock()
    manager.db_ops.get_adjustment_factor_series_status = AsyncMock(
        return_value={"promotion_eligible": True}
    )
    manager.db_ops.get_adjustment_factor_instrument_status = AsyncMock(
        return_value={"coverage_status": "complete_with_events"}
    )
    manager.db_ops.get_canonical_adjustment_factors = AsyncMock(
        return_value=[{"factor": 1.1}]
    )
    manager.db_ops.get_adjustment_factors = AsyncMock(return_value=[{"factor": 9.9}])

    canonical = await manager.get_cached_adjustment_factor_bundle("000001.SZ")

    assert canonical["actual_dataset"] == "canonical"
    assert canonical["factors"] == [{"factor": 1.1}]
    manager.db_ops.get_adjustment_factors.assert_not_awaited()

    manager._factor_cache = {}
    manager.db_ops.get_adjustment_factor_series_status = AsyncMock(
        return_value={"promotion_eligible": False}
    )
    fallback = await manager.get_cached_adjustment_factor_bundle("000002.SZ")

    assert fallback["actual_dataset"] == "legacy"
    assert fallback["fallback_used"] is True
    assert fallback["availability_error"] is None


@pytest.mark.asyncio
async def test_factor_cache_distinguishes_complete_no_events_from_missing_coverage():
    manager = DataManager()
    manager._factor_cache = {}
    manager.data_config = {
        "adjustment_factor_governance": {
            "read_dataset": "canonical",
            "canonical_series_version": "v1",
            "allow_legacy_fallback": False,
        }
    }
    manager.db_ops = Mock()
    manager.db_ops.get_adjustment_factor_series_status = AsyncMock(
        return_value={"promotion_eligible": True}
    )
    manager.db_ops.get_adjustment_factor_instrument_status = AsyncMock(
        return_value={"coverage_status": "complete_no_events"}
    )
    manager.db_ops.get_canonical_adjustment_factors = AsyncMock()

    no_events = await manager.get_cached_adjustment_factor_bundle("000001.SZ")
    assert no_events["actual_dataset"] == "canonical"
    assert no_events["availability_error"] is None
    assert no_events["factors"] == []

    manager._factor_cache = {}
    manager.db_ops.get_adjustment_factor_instrument_status = AsyncMock(
        return_value=None
    )
    missing = await manager.get_cached_adjustment_factor_bundle("000002.SZ")
    assert missing["availability_error"]


@pytest.mark.asyncio
async def test_factor_rebuild_dry_run_does_not_create_checkpoint(tmp_path):
    manager = DataManager()
    manager.data_config = {
        "data_dir": str(tmp_path),
        "adjustment_factor_governance": {},
    }
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
    }])
    manager.db_ops.list_adjustment_factor_observations = AsyncMock(return_value=[])
    manager.source_factory = Mock()

    result = await manager.rebuild_a_share_adjustment_factor_governance(
        start_date="1990-12-19",
        end_date="2026-07-16",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
        resume=True,
    )

    assert result["status"] == "dry_run"
    assert result["universe"]["pending_count"] == 1
    assert not (tmp_path / "backfill_checkpoints").exists()
    manager.source_factory._find_source_by_base_name.assert_not_called()
    manager.db_ops.get_instruments_list.assert_awaited_once_with(
        exchange="SZSE", type="stock", is_active=None
    )


@pytest.mark.asyncio
async def test_factor_rebuild_write_resumes_without_second_source_request(tmp_path):
    manager = DataManager()
    manager.data_config = {
        "data_dir": str(tmp_path),
        "adjustment_factor_governance": {
            "source_priority": ["akshare", "tdx_xdxr", "baostock"],
        },
    }
    stored_observations = []
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
    }])

    async def list_observations(**_kwargs):
        return list(stored_observations)

    async def save_observations(rows, ingestion_run_id=None):
        stored_observations[:] = rows
        return {"inserted": len(rows), "changed": 0, "unchanged": 0, "failed": 0}

    async def read_query(sql, _parameters):
        if "adjustment_factors_tdx" in sql:
            return [{
                "instrument_id": "000001.SZ",
                "ex_date": "2020-05-28",
                "factor": 1.02,
                "cumulative_factor": 1.02,
                "validation_result": "computed_unvalidated",
            }]
        return [{
            "instrument_id": "000001.SZ",
            "ex_date": "2020-05-28",
            "factor": 1.02,
            "cumulative_factor": 12.0,
            "source": "baostock",
        }]

    manager.db_ops.list_adjustment_factor_observations = AsyncMock(
        side_effect=list_observations
    )
    manager.db_ops.save_adjustment_factor_observations = AsyncMock(
        side_effect=save_observations
    )
    manager.db_ops.execute_read_query = AsyncMock(side_effect=read_query)
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[
        {"date": date(2020, 5, 28), "is_trading_day": True}
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(return_value=date.today())
    manager.db_ops.replace_canonical_adjustment_factors = AsyncMock(return_value=1)
    manager.db_ops.replace_adjustment_factor_instrument_statuses = AsyncMock(
        return_value=1
    )
    manager.db_ops.upsert_adjustment_factor_series_status = AsyncMock()
    manager.db_ops.promote_canonical_adjustment_factor_series = AsyncMock()
    source = SimpleNamespace(get_adjustment_factors=AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "ex_date": datetime(2020, 5, 28),
        "factor": 1.02,
        "cumulative_factor": 10.0,
        "source": "akshare",
        "source_profile": "sina_hfq_factor",
    }]))
    manager.source_factory = Mock()
    manager.source_factory._find_source_by_base_name.return_value = source

    parameters = {
        "start_date": "1990-12-19",
        "end_date": "2026-07-16",
        "exchanges": ["SZSE"],
        "instrument_ids": ["000001.SZ"],
        "dry_run": False,
        "resume": True,
        "request_interval_seconds": 0,
    }
    first = await manager.rebuild_a_share_adjustment_factor_governance(**parameters)
    second = await manager.rebuild_a_share_adjustment_factor_governance(**parameters)

    assert first["canonical"]["event_reconciliation"]["exact_matches"] == 1
    assert first["canonical"]["status_persisted"] is True
    assert first["canonical"]["promoted"] is False
    assert first["canonical"]["staging_series_version"] != "a_share_event_product_v1"
    assert second["universe"]["pending_count"] == 0
    assert source.get_adjustment_factors.await_count == 1
    assert manager.db_ops.upsert_adjustment_factor_series_status.await_count == 2
    manager.db_ops.promote_canonical_adjustment_factor_series.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_market_rebuild_promotes_staging_version_only_after_gates_pass(tmp_path):
    manager = DataManager()
    manager.data_config = {
        "data_dir": str(tmp_path),
        "adjustment_factor_governance": {},
    }
    instruments_by_exchange = {
        "SSE": [{"instrument_id": "600000.SH", "symbol": "600000"}],
        "SZSE": [{"instrument_id": "000001.SZ", "symbol": "000001"}],
        "BSE": [{"instrument_id": "920001.BJ", "symbol": "920001"}],
    }
    stored_observations = []
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(
        side_effect=lambda exchange, type, is_active: instruments_by_exchange[exchange]
    )

    async def list_observations(**_kwargs):
        return list(stored_observations)

    async def save_observations(rows, ingestion_run_id=None):
        stored_observations.extend(rows)
        return {"inserted": len(rows), "changed": 0, "unchanged": 0, "failed": 0}

    async def read_query(sql, parameters):
        rows = []
        for value in parameters.values():
            if not isinstance(value, str) or "." not in value:
                continue
            rows.append({
                "instrument_id": value,
                "ex_date": "2020-05-28",
                "factor": 1.02,
                "cumulative_factor": 1.02,
                "validation_result": "computed_unvalidated",
                "source": "tdx_xdxr",
            })
        return rows

    manager.db_ops.list_adjustment_factor_observations = AsyncMock(
        side_effect=list_observations
    )
    manager.db_ops.save_adjustment_factor_observations = AsyncMock(
        side_effect=save_observations
    )
    manager.db_ops.execute_read_query = AsyncMock(side_effect=read_query)
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[
        {"date": date(2020, 5, 28), "is_trading_day": True}
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(return_value=date.today())
    manager.db_ops.replace_canonical_adjustment_factors = AsyncMock(return_value=3)
    manager.db_ops.replace_adjustment_factor_instrument_statuses = AsyncMock(
        return_value=3
    )
    manager.db_ops.upsert_adjustment_factor_series_status = AsyncMock()
    manager.db_ops.promote_canonical_adjustment_factor_series = AsyncMock(
        return_value={"canonical_rows": 3, "instrument_statuses": 3}
    )

    async def get_factors(instrument_id, _symbol, _start, _end):
        return [{
            "instrument_id": instrument_id,
            "ex_date": datetime(2020, 5, 28),
            "factor": 1.02,
            "cumulative_factor": 1.02,
            "source": "akshare",
        }]

    source = SimpleNamespace(get_adjustment_factors=AsyncMock(side_effect=get_factors))
    manager.source_factory = Mock()
    manager.source_factory._find_source_by_base_name.return_value = source

    result = await manager.rebuild_a_share_adjustment_factor_governance(
        start_date="1990-12-19",
        end_date=date.today().isoformat(),
        exchanges=["SSE", "SZSE", "BSE"],
        dry_run=False,
        resume=False,
        request_interval_seconds=0,
    )

    assert result["status"] == "success"
    assert result["canonical"]["promoted"] is True
    replace_kwargs = manager.db_ops.replace_canonical_adjustment_factors.await_args.kwargs
    assert "__staging__" in replace_kwargs["series_version"]
    promote_kwargs = manager.db_ops.promote_canonical_adjustment_factor_series.await_args.kwargs
    assert promote_kwargs["staging_series_version"] == replace_kwargs["series_version"]
    assert promote_kwargs["target_series_version"] == "a_share_event_product_v1"
