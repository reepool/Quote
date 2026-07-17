import asyncio
from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_corporate_actions import CninfoEndpointResult


async def _inline_to_thread(function, *args, **kwargs):
    return function(*args, **kwargs)


@pytest.mark.asyncio
async def test_cninfo_corporate_action_dry_run_does_not_fetch_or_write(tmp_path):
    manager = DataManager()
    manager.data_config = {"data_dir": str(tmp_path)}
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(
        return_value=[
            {
                "instrument_id": "000001.SZ",
                "symbol": "000001",
            }
        ]
    )
    manager.db_ops.save_corporate_action_observations = AsyncMock()

    result = await manager.backfill_a_share_cninfo_corporate_actions(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["universe"] == {
        "instrument_count": 1,
        "completed_count": 0,
        "pending_count": 1,
    }
    assert result["production_isolation"] is True
    assert not (tmp_path / "backfill_checkpoints").exists()
    manager.db_ops.save_corporate_action_observations.assert_not_awaited()


@pytest.mark.asyncio
async def test_cninfo_corporate_action_write_resumes_without_second_fetch(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(asyncio, "to_thread", _inline_to_thread)
    manager = DataManager()
    manager.data_config = {"data_dir": str(tmp_path)}
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(
        return_value=[
            {
                "instrument_id": "000001.SZ",
                "symbol": "000001",
            }
        ]
    )

    async def save_observations(observations, **_kwargs):
        return {
            "inserted": len(observations),
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
        }

    manager.db_ops.save_corporate_action_observations = AsyncMock(
        side_effect=save_observations
    )
    manager.db_ops.reconcile_corporate_action_observation_snapshot = AsyncMock(
        return_value=0
    )
    manager.db_ops.upsert_corporate_action_instrument_status = AsyncMock()
    observation = {
        "instrument_id": "000001.SZ",
        "source": "cninfo",
        "source_profile": "cninfo_dividend",
        "source_event_key": "event-1",
        "action_type": "dividend",
        "ex_date": date(2026, 6, 12),
        "quality_status": "structured_complete",
    }
    provider = Mock()
    provider.fetch_dividends = Mock(
        return_value=CninfoEndpointResult(
            source_profile="cninfo_dividend",
            coverage_status="complete_with_events",
            observations=[observation],
            rows_received=1,
        )
    )
    provider.fetch_allotments = Mock(
        return_value=CninfoEndpointResult(
            source_profile="cninfo_allotment",
            coverage_status="complete_no_events",
            observations=[],
            rows_received=0,
        )
    )
    monkeypatch.setattr(
        "data_sources.cninfo_corporate_actions.CninfoCorporateActionProvider",
        lambda **_: provider,
    )

    first = await manager.backfill_a_share_cninfo_corporate_actions(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
        resume=False,
        request_interval_seconds=0,
    )
    second = await manager.backfill_a_share_cninfo_corporate_actions(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
        resume=True,
        request_interval_seconds=0,
    )

    assert first["status"] == "success"
    assert first["counters"]["observations_inserted"] == 1
    assert second["universe"]["pending_count"] == 0
    assert provider.fetch_dividends.call_count == 1
    assert provider.fetch_allotments.call_count == 1


@pytest.mark.asyncio
async def test_cninfo_indeterminate_response_remains_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(asyncio, "to_thread", _inline_to_thread)
    manager = DataManager()
    manager.data_config = {"data_dir": str(tmp_path)}
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_list = AsyncMock(
        return_value=[
            {
                "instrument_id": "000003.SZ",
                "symbol": "000003",
            }
        ]
    )
    manager.db_ops.save_corporate_action_observations = AsyncMock(
        return_value={
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
        }
    )
    manager.db_ops.reconcile_corporate_action_observation_snapshot = AsyncMock(
        return_value=0
    )
    manager.db_ops.upsert_corporate_action_instrument_status = AsyncMock()
    provider = Mock()
    provider.fetch_dividends = Mock(
        return_value=CninfoEndpointResult(
            source_profile="cninfo_dividend",
            coverage_status="indeterminate",
            observations=[],
            error="malformed empty response",
        )
    )
    monkeypatch.setattr(
        "data_sources.cninfo_corporate_actions.CninfoCorporateActionProvider",
        lambda **_: provider,
    )

    result = await manager.backfill_a_share_cninfo_corporate_actions(
        start_date="1990-12-19",
        end_date="2002-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000003.SZ"],
        scopes=["dividends"],
        dry_run=False,
        resume=False,
        request_interval_seconds=0,
    )

    assert result["status"] == "partial"
    assert result["universe"]["pending_count"] == 1
    assert result["counters"]["indeterminate"] == 1
    assert result["errors"][0]["source_profile"] == "cninfo_dividend"
    manager.db_ops.reconcile_corporate_action_observation_snapshot.assert_not_awaited()
