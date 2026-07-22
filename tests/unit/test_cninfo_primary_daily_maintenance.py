from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


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
