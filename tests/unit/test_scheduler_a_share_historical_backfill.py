from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scheduler.dependencies import is_successful_task_result
from scheduler.tasks import (
    ScheduledTasks,
    _format_a_share_historical_backfill_report,
    data_manager,
)
from utils import config_manager


class _FakeDbOps:
    def __init__(self, calendar_records):
        self.calendar_records = calendar_records

    async def get_repair_universe_instruments(self, exchange, instrument_types=None):
        return [{
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": exchange,
            "type": "stock",
            "is_active": False,
        }]

    async def get_trading_calendar_records(self, exchange, start_date, end_date):
        return list(self.calendar_records)


def _task(monkeypatch, tmp_path, calendar_records):
    task = ScheduledTasks()
    task.telegram_enabled = False
    monkeypatch.setattr(data_manager, "data_config", {"data_dir": str(tmp_path)})
    monkeypatch.setattr(data_manager, "db_ops", _FakeDbOps(calendar_records))
    monkeypatch.setattr(
        data_manager,
        "filter_repair_universe",
        AsyncMock(return_value=([{
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
            "is_active": False,
            "_repair_start_date": date(2026, 1, 5),
            "_repair_end_date": date(2026, 1, 5),
        }], {"eligible_instrument_count": 1})),
    )
    return task


@pytest.mark.asyncio
async def test_a_share_historical_backfill_dry_run_has_no_business_writes(monkeypatch, tmp_path):
    task = _task(monkeypatch, tmp_path, [{"date": datetime(2026, 1, 5)}])
    quote_mock = AsyncMock()
    xdxr_mock = AsyncMock()
    monkeypatch.setattr(data_manager, "update_daily_data_range", quote_mock)
    monkeypatch.setattr(data_manager, "backfill_tdx_xdxr_history", xdxr_mock)

    result = await task.a_share_daily_data_historical_backfill(
        start_date="2026-01-05",
        end_date="2026-01-05",
        exchanges="SSE",
        scopes="master,calendar,quotes,dividends,factors",
        dry_run=True,
        chunk_size="1",
    )

    assert result["status"] == "dry_run"
    assert result["stages"]["master"]["status"] == "dry_run"
    assert result["stages"]["calendar"]["status"] == "dry_run"
    assert result["stages"]["quotes"]["status"] == "dry_run"
    quote_mock.assert_not_awaited()
    xdxr_mock.assert_not_awaited()
    assert not (tmp_path / "backfill_checkpoints").exists()
    assert is_successful_task_result(result) is True


@pytest.mark.asyncio
async def test_a_share_historical_backfill_blocks_quotes_when_calendar_missing(monkeypatch, tmp_path):
    task = _task(monkeypatch, tmp_path, [])
    quote_mock = AsyncMock()
    monkeypatch.setattr(data_manager, "update_daily_data_range", quote_mock)

    result = await task.a_share_daily_data_historical_backfill(
        start_date="2026-01-05",
        end_date="2026-01-05",
        exchanges=["SSE"],
        scopes=["quotes"],
        dry_run=False,
        chunk_size=1,
    )

    assert result["status"] == "blocked"
    assert result["stages"]["calendar"]["status"] == "blocked"
    assert result["stages"]["quotes"]["status"] == "blocked"
    quote_mock.assert_not_awaited()
    assert is_successful_task_result(result) is False


@pytest.mark.asyncio
async def test_a_share_historical_backfill_reports_partial_quote_chunk(monkeypatch, tmp_path):
    task = _task(monkeypatch, tmp_path, [{"date": date(2026, 1, 5)}])
    monkeypatch.setattr(
        data_manager,
        "update_daily_data_range",
        AsyncMock(return_value={
            "success_count": 0,
            "failure_count": 1,
            "total_quotes_added": 0,
        }),
    )

    result = await task.a_share_daily_data_historical_backfill(
        start_date="2026-01-05",
        end_date="2026-01-05",
        exchanges=["SSE"],
        scopes=["quotes"],
        dry_run=False,
        chunk_size=1,
    )

    assert result["status"] == "partial"
    assert result["stages"]["quotes"]["totals"]["failure_count"] == 1
    assert result["failure_samples"][0]["reason"] == "quote_chunk_failed"


@pytest.mark.asyncio
async def test_a_share_historical_backfill_success_resumes_completed_chunk(monkeypatch, tmp_path):
    task = _task(monkeypatch, tmp_path, [{"date": date(2026, 1, 5)}])
    quote_mock = AsyncMock(return_value={
        "success_count": 1,
        "failure_count": 0,
        "total_quotes_added": 1,
    })
    monkeypatch.setattr(data_manager, "update_daily_data_range", quote_mock)
    parameters = {
        "start_date": "2026-01-05",
        "end_date": "2026-01-05",
        "exchanges": ["SSE"],
        "scopes": ["quotes"],
        "dry_run": False,
        "resume": True,
        "chunk_size": 1,
    }

    first = await task.a_share_daily_data_historical_backfill(**parameters)
    second = await task.a_share_daily_data_historical_backfill(**parameters)

    assert first["status"] == "success"
    assert first["stages"]["quotes"]["totals"]["chunks_completed"] == 1
    assert second["status"] == "success"
    assert second["resumed"] is True
    assert second["stages"]["quotes"]["totals"]["chunks_resumed"] == 1
    assert quote_mock.await_count == 1


@pytest.mark.asyncio
async def test_a_share_historical_backfill_master_failure_blocks_write_stages(monkeypatch, tmp_path):
    task = _task(monkeypatch, tmp_path, [{"date": date(2026, 1, 5)}])
    quote_mock = AsyncMock()
    xdxr_mock = AsyncMock()
    monkeypatch.setattr(
        data_manager,
        "_run_repair_current_master_refresh",
        AsyncMock(return_value={"status": "failed", "errors": ["master failed"]}),
    )
    monkeypatch.setattr(data_manager, "update_daily_data_range", quote_mock)
    monkeypatch.setattr(data_manager, "backfill_tdx_xdxr_history", xdxr_mock)

    result = await task.a_share_daily_data_historical_backfill(
        start_date="2026-01-05",
        end_date="2026-01-05",
        exchanges=["SSE"],
        scopes=["master", "quotes", "dividends"],
        dry_run=False,
        chunk_size=1,
    )

    assert result["status"] == "blocked"
    assert "master_governance_failed" in result["blockers"]
    assert result["stages"]["quotes"]["status"] == "blocked"
    assert result["stages"]["dividends"]["status"] == "blocked"
    quote_mock.assert_not_awaited()
    xdxr_mock.assert_not_awaited()


def test_scheduler_config_registers_manual_dry_run_job():
    job = config_manager.get_nested(
        "scheduler_config.jobs.a_share_daily_data_historical_backfill",
        {},
    )

    assert job["enabled"] is True
    assert job["manual_only"] is True
    assert "trigger" not in job
    assert job["parameters"]["dry_run"] is True
    assert job["parameters"]["resume"] is True
    assert job["parameters"]["scopes"] == [
        "master", "calendar", "quotes", "dividends", "factors"
    ]


def test_historical_backfill_report_is_bounded_and_exposes_checkpoint():
    content = _format_a_share_historical_backfill_report({
        "status": "partial",
        "dry_run": False,
        "checkpoint_id": "test-checkpoint",
        "parameters": {
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "exchanges": ["SSE"],
            "scopes": ["quotes", "dividends"],
        },
        "stages": {
            "quotes": {"status": "partial", "totals": {"failure_count": 1}},
            "dividends": {"status": "success", "totals": {"raw_events": 2}},
        },
        "blockers": [],
        "failure_samples": [
            {"instrument_id": f"sample-{idx}", "reason": "failed"}
            for idx in range(50)
        ],
    })

    assert "test-checkpoint" in content
    assert "quotes: partial" in content
    assert "sample-9" in content
    assert "sample-10" not in content
