from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


@pytest.mark.asyncio
async def test_cutoff_deferred_loader_includes_governed_effective_date_evidence():
    manager = DataManager()
    manager.db_ops = Mock()

    async def query_rows(query, params):
        if "corporate_action_effective_date_evidence" in query:
            assert "observation.is_current = 1" in query
            assert "evidence.resolution_status = 'resolved'" in query
            assert {
                value
                for key, value in params.items()
                if key.startswith("cutoff_evidence_source_")
            } >= {"cninfo_operator_attestation"}
            return [{"instrument_id": "600000.SH"}]
        return []

    manager.db_ops.execute_read_query = AsyncMock(side_effect=query_rows)

    result = await manager._load_daily_factor_cutoff_deferred_instrument_ids(
        ["600000.SH"],
        cutoff_date=date(2026, 7, 28),
        end_date=date(2026, 7, 29),
    )

    assert result == ["600000.SH"]


@pytest.mark.asyncio
async def test_daily_maintenance_excludes_bse_only_from_cninfo():
    manager = DataManager()
    manager.db_ops = Mock()

    async def active_instruments(exchange, **_kwargs):
        return [{
            "instrument_id": {
                "SSE": "600000.SH",
                "SZSE": "000001.SZ",
                "BSE": "920000.BJ",
            }[exchange]
        }]

    manager.db_ops.get_active_instruments = AsyncMock(side_effect=active_instruments)
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 17)
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["000001.SZ", "600000.SH"],
        "candidate_count": 2,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(
        return_value={
            "status": "success",
            "affected_instrument_ids": ["600000.SH"],
        }
    )
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": ["920000.BJ"],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        return_value={
            "status": "partial",
            "cninfo_path": {"pending_count": 1},
            "tdx_path": {"pending_count": 0},
            "reconciliation": {"status": "partial", "totals": {}},
            "overall_completeness": {
                "status": "partial",
                "overall_incomplete_instruments": 1,
            },
        }
    )

    result = await manager.maintain_a_share_cninfo_primary_factors(
        start_date="2026-07-01",
        end_date="2026-07-18",
        exchanges=["SSE", "SZSE", "BSE"],
        request_interval_seconds=0,
    )

    cninfo_args = manager.backfill_a_share_cninfo_corporate_actions.await_args.kwargs
    tdx_args = manager.backfill_tdx_xdxr_history.await_args.kwargs
    rebuild_args = manager.rebuild_cninfo_primary_adjustment_factors.await_args.kwargs
    assert cninfo_args["exchanges"] == ["SSE", "SZSE"]
    assert cninfo_args["instrument_ids"] == ["000001.SZ", "600000.SH"]
    assert tdx_args["exchanges"] == ["SSE", "SZSE", "BSE"]
    assert tdx_args["instrument_ids"] == ["000001.SZ", "600000.SH", "920000.BJ"]
    assert rebuild_args["exchanges"] == ["SSE", "SZSE", "BSE"]
    assert rebuild_args["instrument_ids"] == ["600000.SH", "920000.BJ"]
    assert result["status"] == "success"
    assert result["data_readiness"]["status"] == "partial"
    assert result["affected_instruments"]["count"] == 2
    assert result["parameters"]["cninfo_excluded_exchanges"] == ["BSE"]
    assert result["cninfo_refresh"]["source_coverage"]["excluded_reason"] == (
        "source_not_supported"
    )


@pytest.mark.asyncio
async def test_bse_only_daily_maintenance_skips_cninfo_but_runs_tdx():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(
        return_value=[{"instrument_id": "920000.BJ"}]
    )
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 17)
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock()
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        return_value={"status": "success"}
    )

    result = await manager.maintain_a_share_cninfo_primary_factors(
        start_date="2026-07-01",
        end_date="2026-07-18",
        exchanges=["BSE"],
    )

    manager.backfill_a_share_cninfo_corporate_actions.assert_not_awaited()
    manager.backfill_tdx_xdxr_history.assert_awaited_once()
    manager.rebuild_cninfo_primary_adjustment_factors.assert_not_awaited()
    assert result["status"] == "success"
    assert result["cninfo_refresh"]["status"] == "skipped"
    assert result["factor_rebuild"]["status"] == "skipped"
    assert result["parameters"]["tdx_exchanges"] == ["BSE"]


@pytest.mark.asyncio
async def test_daily_maintenance_propagates_discovery_partial_operational_status():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "symbol": "600000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 21)
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "partial",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
        "candidates": [{
            "instrument_id": "600000.SH",
            "reasons": ["announcement_activity"],
        }],
        "_announcement_governance_context": {
            "announcement_scan": {},
            "pending_candidate_ids": [],
            "active_instruments": {
                "600000.SH": {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                }
            },
        },
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "partial",
        "affected_instrument_ids": [],
        "errors": [{"instrument_id": "600000.SH", "reason": "timeout"}],
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={
        "scan_states_persisted": 1,
        "audits_persisted": 0,
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock()

    result = await manager.maintain_a_share_cninfo_primary_factors(
        start_date="2026-07-15",
        end_date="2026-07-22",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert result["status"] == "partial"
    assert result["factor_rebuild"]["status"] == "skipped"
    assert (
        manager._persist_cninfo_daily_announcement_activity.call_args.kwargs[
            "pending_candidate_ids"
        ]
        == ["600000.SH"]
    )


@pytest.mark.asyncio
async def test_daily_maintenance_caps_factor_end_at_latest_common_quote_date():
    manager = DataManager()
    manager.db_ops = Mock()

    async def active_instruments(exchange, **_kwargs):
        return [{
            "instrument_id": {
                "SSE": "600000.SH",
                "SZSE": "000001.SZ",
                "BSE": "920000.BJ",
            }[exchange]
        }]

    manager.db_ops.get_active_instruments = AsyncMock(
        side_effect=active_instruments
    )
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={
            "SSE": date(2026, 7, 28),
            "SZSE": date(2026, 7, 28),
            "BSE": date(2026, 7, 28),
        }
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
        "candidates": [],
        "_announcement_governance_context": {
            "announcement_scan": {
                "deferred_factor_instrument_ids": ["000001.SZ"],
            },
            "pending_candidate_ids": [],
            "active_instruments": {
                "600000.SH": {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                },
                "000001.SZ": {
                    "instrument_id": "000001.SZ",
                    "exchange": "SZSE",
                },
            },
        },
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={
        "scan_states_persisted": 2,
        "audits_persisted": 0,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(return_value={
        "status": "partial",
        "cninfo_path": {"pending_count": 0},
        "tdx_path": {"pending_count": 3},
        "reconciliation": {"status": "partial", "totals": {}},
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 1,
        },
        "source_completeness": {
            "cninfo": {
                "status": "success",
                "incomplete_instruments": 0,
            },
            "tdx_reference": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
            "reconciliation": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
        },
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE", "SZSE", "BSE"],
        request_interval_seconds=0,
    )

    rebuild_args = manager.rebuild_cninfo_primary_adjustment_factors.await_args.kwargs
    discovery_args = (
        manager.discover_a_share_cninfo_daily_candidates.await_args.kwargs
    )
    assert rebuild_args["end_date"] == date(2026, 7, 28)
    assert rebuild_args["instrument_ids"] == ["000001.SZ", "600000.SH"]
    assert discovery_args["announcement_start_date"] == date(2026, 7, 28)
    assert (
        manager.db_ops.get_latest_stock_quote_dates_by_exchange.await_args.kwargs[
            "listed_on_or_before"
        ]
        == date(2026, 7, 28)
    )
    assert (
        manager.db_ops.get_latest_stock_quote_dates_by_exchange.await_args.kwargs[
            "completed_on_or_before"
        ]
        == date(2026, 7, 28)
    )
    assert result["status"] == "success"
    assert result["data_readiness"]["status"] == "success"
    assert result["data_readiness"]["tdx_reference"]["status"] == "partial"
    assert result["factor_cutoff"]["resolved_end_date"] == "2026-07-28"
    assert (
        manager._persist_cninfo_daily_announcement_activity.call_args.kwargs[
            "pending_factor_instrument_ids"
        ]
        == []
    )


@pytest.mark.asyncio
async def test_daily_maintenance_defers_rebuild_when_quote_cutoff_is_unavailable():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "symbol": "600000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={}
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock()

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    manager.rebuild_cninfo_primary_adjustment_factors.assert_not_awaited()
    assert result["status"] == "partial"
    assert result["factor_cutoff"]["resolved_end_date"] is None
    assert result["factor_rebuild"]["reason"] == "factor_cutoff_unavailable"


@pytest.mark.asyncio
async def test_bse_only_daily_maintenance_persists_factor_retry_without_scan():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "920000.BJ",
        "symbol": "920000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={"BSE": date(2026, 7, 28)}
    )
    manager.db_ops.get_corporate_action_daily_factor_retry_instrument_ids = (
        AsyncMock(return_value=["920000.BJ"])
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock(return_value={"inserted": 0, "cleared": 0, "pending": 1})
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock()
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(return_value={
        "status": "partial",
        "cninfo_path": {
            "pending_count": 0,
            "pending_instrument_ids": [],
        },
        "tdx_path": {
            "pending_count": 1,
            "pending_instrument_ids": ["920000.BJ"],
        },
        "source_completeness": {
            "cninfo": {"status": "success", "incomplete_instruments": 0},
            "tdx_reference": {"status": "partial", "incomplete_instruments": 1},
            "reconciliation": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
        },
        "reconciliation": {"status": "partial", "totals": {}},
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 1,
        },
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["BSE"],
    )

    manager.rebuild_cninfo_primary_adjustment_factors.assert_awaited_once()
    retry_write = (
        manager.db_ops
        .replace_corporate_action_daily_factor_retry_instruments
        .await_args
    )
    assert retry_write.args == (["920000.BJ"],)
    assert retry_write.kwargs["scope_instrument_ids"] == ["920000.BJ"]
    assert result["factor_retry_state"]["status"] == "success"


@pytest.mark.asyncio
async def test_daily_maintenance_preserves_retry_queue_when_load_fails():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "920000.BJ",
        "symbol": "920000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={"BSE": date(2026, 7, 28)}
    )
    manager.db_ops.get_corporate_action_daily_factor_retry_instrument_ids = (
        AsyncMock(side_effect=RuntimeError("retry read failed"))
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock()
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock()
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock()

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["BSE"],
    )

    (
        manager.db_ops
        .replace_corporate_action_daily_factor_retry_instruments
        .assert_not_awaited()
    )
    assert result["status"] == "partial"
    assert result["factor_retry_state"] == {
        "status": "failed",
        "reason": "factor_retry_load_failed_queue_preserved",
        "error": "retry read failed",
    }


@pytest.mark.asyncio
async def test_daily_maintenance_persists_complete_new_pending_factor_queue():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "trading_status": 1,
        },
        {
            "instrument_id": "600001.SH",
            "symbol": "600001",
            "trading_status": 1,
        },
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={"SSE": date(2026, 7, 28)}
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
        "candidates": [],
        "_announcement_governance_context": {
            "announcement_scan": {
                "deferred_factor_instrument_ids": ["600001.SH"],
            },
            "pending_candidate_ids": [],
            "active_instruments": {
                "600000.SH": {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                },
                "600001.SH": {
                    "instrument_id": "600001.SH",
                    "exchange": "SSE",
                },
            },
        },
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={})
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(return_value={
        "status": "partial",
        "cninfo_path": {
            "pending_count": 2,
            "pending": [{"instrument_id": "600000.SH"}],
            "pending_instrument_ids": ["600000.SH", "600001.SH"],
        },
        "tdx_path": {
            "pending_count": 0,
            "pending": [],
            "pending_instrument_ids": [],
        },
        "source_completeness": {
            "cninfo": {"status": "partial", "incomplete_instruments": 2},
            "tdx_reference": {"status": "success", "incomplete_instruments": 0},
            "reconciliation": {
                "status": "success",
                "incomplete_instruments": 0,
            },
        },
        "reconciliation": {"status": "success", "totals": {}},
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 2,
        },
    })

    await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert (
        manager._persist_cninfo_daily_announcement_activity.call_args.kwargs[
            "pending_factor_instrument_ids"
        ]
        == ["600000.SH", "600001.SH"]
    )
