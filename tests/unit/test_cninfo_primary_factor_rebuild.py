from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


def _manager_with_factor_evidence(*, tdx_validation_result="computed_unvalidated"):
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "symbol": "000001",
    }])

    async def execute_read_query(query, _params):
        if "FROM corporate_action_observations" in query:
            return [{
                "instrument_id": "000001.SZ",
                "source_profile": "cninfo_dividend",
                "source_event_key": "event-1",
                "ex_date": datetime(2020, 5, 28),
                "cash_dividend_per_share": 0.218,
                "bonus_shares_per_share": None,
                "capitalization_shares_per_share": None,
                "rights_shares_per_share": None,
                "rights_price": None,
                "event_status": "implemented",
                "quality_status": "structured_complete",
                "is_current": 1,
            }]
        if "FROM adjustment_factors_tdx" in query:
            return [{
                "instrument_id": "000001.SZ",
                "ex_date": datetime(2020, 5, 28),
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "validation_result": tdx_validation_result,
                "pre_close": 13.5,
                "fenhong": 2.18,
                "songzhuangu": 0.0,
                "peigu": 0.0,
                "peigujia": 0.0,
            }]
        if "FROM adjustment_factors\n" in query:
            return [{
                "instrument_id": "000001.SZ",
                "ex_date": datetime(2020, 5, 28),
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "source": "baostock",
            }]
        if "FROM corporate_action_instrument_status" in query:
            if "source = 'tdx'" in query:
                return [{
                    "instrument_id": "000001.SZ",
                    "source_profile": "tdx_xdxr",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                }]
            return [
                {
                    "instrument_id": "000001.SZ",
                    "source_profile": "cninfo_dividend",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                },
                {
                    "instrument_id": "000001.SZ",
                    "source_profile": "cninfo_allotment",
                    "coverage_status": "complete_no_events",
                    "event_count": 0,
                },
            ]
        return []

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.get_quote_evidence_for_event_dates = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_date": date(2020, 5, 28),
        "effective_date": date(2020, 5, 28),
        "pre_close": 13.5,
        "close": 13.0,
    }])
    manager.db_ops.get_trading_calendar_records = AsyncMock(return_value=[{
        "date": date(2020, 5, 28),
        "is_trading_day": True,
    }])
    manager.db_ops.list_adjustment_factor_observations = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "ex_date": datetime(2020, 5, 28),
        "source": "akshare",
        "source_profile": "sina_hfq_factor",
        "provider_cumulative_factor": 1.01,
    }])
    manager.db_ops.save_adjustment_factor_observations = AsyncMock(return_value={
        "inserted": 1,
        "changed": 0,
        "unchanged": 0,
        "failed": 0,
    })
    manager.db_ops.replace_canonical_adjustment_factors = AsyncMock(return_value=1)
    manager.db_ops.replace_adjustment_factor_instrument_statuses = AsyncMock(
        return_value=1
    )
    manager.db_ops.upsert_adjustment_factor_series_status = AsyncMock()
    manager.invalidate_factor_cache = Mock()
    return manager


@pytest.mark.asyncio
async def test_cninfo_primary_factor_rebuild_dry_run_is_read_only():
    manager = _manager_with_factor_evidence()

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["production_isolation"] is True
    assert result["cninfo_path"]["derived_events"] == 1
    assert result["tdx_path"]["derived_events"] == 1
    assert result["reconciliation"]["totals"]["exact_matches"] == 1
    assert result["benchmark"]["source_selection_status"] == "deferred"
    assert result["benchmark"]["reference_sources"][
        "tdx_event_derived_v1"
    ]["coverage_ratio"] == pytest.approx(1.0)
    assert result["candidate"]["candidate_built"] is False
    manager.db_ops.save_adjustment_factor_observations.assert_not_awaited()
    manager.db_ops.replace_canonical_adjustment_factors.assert_not_awaited()


@pytest.mark.asyncio
async def test_cninfo_factor_rebuild_writes_paths_and_benchmark_without_candidate():
    manager = _manager_with_factor_evidence()

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["source_selection"]["status"] == "deferred"
    assert result["candidate"]["candidate_built"] is False
    assert result["candidate"]["promotion_eligible"] is False
    assert result["write_result"]["canonical_saved_rows"] == 0
    assert result["write_result"]["benchmark_status_saved"] is True
    assert manager.db_ops.save_adjustment_factor_observations.await_count == 2
    manager.db_ops.upsert_adjustment_factor_series_status.assert_awaited_once()
    manager.db_ops.replace_canonical_adjustment_factors.assert_not_awaited()
    manager.invalidate_factor_cache.assert_called_once()


@pytest.mark.asyncio
async def test_explicit_candidate_build_remains_isolated_staging():
    manager = _manager_with_factor_evidence()

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
        build_canonical=True,
    )

    assert result["status"] == "success"
    assert result["candidate"]["candidate_built"] is True
    assert result["write_result"]["canonical_saved_rows"] == 1
    assert manager.db_ops.upsert_adjustment_factor_series_status.await_count == 2
    manager.db_ops.replace_canonical_adjustment_factors.assert_awaited_once()


@pytest.mark.asyncio
async def test_incomplete_rebuild_still_persists_benchmark_without_candidate():
    manager = _manager_with_factor_evidence(
        tdx_validation_result="pending_factor_missing_pre_close"
    )

    result = await manager.rebuild_cninfo_primary_adjustment_factors(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
    )

    assert result["status"] == "partial"
    assert result["write_result"]["canonical_saved_rows"] == 0
    assert result["write_result"]["benchmark_status_saved"] is True
    manager.db_ops.replace_canonical_adjustment_factors.assert_not_awaited()
