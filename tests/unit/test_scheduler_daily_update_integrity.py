from datetime import date
from unittest.mock import AsyncMock, Mock, patch

import pytest

from scheduler.tasks import ScheduledTasks


def _task() -> ScheduledTasks:
    task = ScheduledTasks.__new__(ScheduledTasks)
    task._active_tasks = set()
    task.config = Mock()
    task.config.get_nested.side_effect = lambda key, default=None: default
    task._send_task_report = AsyncMock(return_value=False)
    task._wait_for_markets_close = AsyncMock()
    return task


def _reported_data(task: ScheduledTasks):
    call = task._send_task_report.await_args
    return call.args[0] if call.args else call.kwargs["report_data"]


@pytest.mark.asyncio
async def test_daily_update_treats_explicit_closed_calendar_row_as_non_trading_day():
    task = _task()
    with patch("scheduler.tasks.data_manager") as dm:
        dm._update_trading_calendar = AsyncMock(return_value=1)
        dm.db_ops.get_trading_calendar_records = AsyncMock(return_value=[
            {"date": date.today(), "is_trading_day": False},
        ])
        dm.update_daily_data = AsyncMock()

        result = await task.daily_data_update(
            exchanges=["SSE"],
            target_date=date.today(),
            wait_for_market_close=False,
            run_factor_audit=False,
        )

    assert result is False
    dm.update_daily_data.assert_not_awaited()
    report = _reported_data(task)
    assert report["status"] == "info"
    assert report["non_trading_day"] is True


@pytest.mark.asyncio
async def test_daily_update_uses_date_utils_when_calendar_row_is_missing():
    task = _task()
    update_result = {
        "success_count": 1,
        "failure_count": 0,
        "total_quotes_added": 1,
        "exchange_stats": {"SSE": {"total_instruments": 1}},
    }
    with patch("scheduler.tasks.data_manager") as dm, patch(
        "scheduler.tasks.DateUtils.is_trading_day", return_value=True
    ) as is_trading_day, patch(
        "scheduler.tasks._run_backtest_stage", new=AsyncMock(return_value={})
    ):
        dm._update_trading_calendar = AsyncMock(return_value=0)
        dm.db_ops.get_trading_calendar_records = AsyncMock(return_value=[])
        dm.update_daily_data = AsyncMock(return_value=update_result)

        result = await task.daily_data_update(
            exchanges=["SSE"],
            target_date=date.today(),
            wait_for_market_close=False,
            run_factor_audit=False,
        )

    assert result is True
    is_trading_day.assert_called_once_with("SSE", date.today())
    dm.update_daily_data.assert_awaited_once()
    report = _reported_data(task)
    assert report["status"] == "success"
    assert report["calendar_unknown"] == []


@pytest.mark.asyncio
async def test_daily_update_reports_calendar_unknown_when_missing_row_fallback_fails():
    task = _task()
    with patch("scheduler.tasks.data_manager") as dm, patch(
        "scheduler.tasks.DateUtils.is_trading_day",
        side_effect=RuntimeError("calendar unavailable"),
    ):
        dm._update_trading_calendar = AsyncMock(return_value=0)
        dm.db_ops.get_trading_calendar_records = AsyncMock(return_value=[])
        dm.update_daily_data = AsyncMock()

        result = await task.daily_data_update(
            exchanges=["SSE"],
            target_date=date.today(),
            wait_for_market_close=False,
            run_factor_audit=False,
        )

    assert result is False
    dm.update_daily_data.assert_not_awaited()
    report = _reported_data(task)
    assert report["status"] == "warning"
    assert report["calendar_unknown"] == ["SSE"]


@pytest.mark.asyncio
async def test_daily_update_does_not_report_success_when_one_exchange_calendar_is_unknown():
    task = _task()
    update_result = {
        "success_count": 1,
        "failure_count": 0,
        "total_quotes_added": 1,
        "exchange_stats": {"SZSE": {"total_instruments": 1}},
    }

    async def calendar_rows(exchange, _start, _end):
        if exchange == "SSE":
            return []
        return [{"date": date.today(), "is_trading_day": True}]

    with patch("scheduler.tasks.data_manager") as dm, patch(
        "scheduler.tasks.DateUtils.is_trading_day",
        side_effect=RuntimeError("calendar unavailable"),
    ), patch(
        "scheduler.tasks._run_backtest_stage", new=AsyncMock(return_value={})
    ):
        dm._update_trading_calendar = AsyncMock(return_value=0)
        dm.db_ops.get_trading_calendar_records = AsyncMock(side_effect=calendar_rows)
        dm.update_daily_data = AsyncMock(return_value=update_result)

        result = await task.daily_data_update(
            exchanges=["SSE", "SZSE"],
            target_date=date.today(),
            wait_for_market_close=False,
            run_factor_audit=False,
        )

    assert result is False
    dm.update_daily_data.assert_awaited_once()
    report = _reported_data(task)
    assert report["status"] == "warning"
    assert report["calendar_unknown"] == ["SSE"]
