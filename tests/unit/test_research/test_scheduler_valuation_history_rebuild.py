import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

from scheduler.tasks import ScheduledTasks


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_valuation_history_rebuild_task_calls_data_manager_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_valuation_history_rebuild = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [{"exchange": "SSE", "status": "success", "rows_written": 10}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.valuation_history_rebuild(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result is True
    data_manager.run_valuation_history_rebuild.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        allow_disabled_module=True,
        quote_limit_days=None,
        window_mode="trading_days",
        write_policy="missing_only",
        progress_log_every=200,
    )
    assert "valuation_history_rebuild" not in task._active_tasks


def test_valuation_history_12q_rebuild_accepts_config_window_mode(monkeypatch):
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_valuation_history_rebuild = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [{"exchange": "SSE", "status": "success", "rows_written": 10}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.valuation_history_12q_rebuild(
            exchanges=["SSE"],
            limit_per_exchange=10,
            quote_limit_days=None,
            window_mode="last_12_quarters",
            write_policy="missing_only",
        )
    )

    assert result is True
    data_manager.run_valuation_history_rebuild.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        allow_disabled_module=True,
        quote_limit_days=None,
        window_mode="last_12_quarters",
        write_policy="missing_only",
        progress_log_every=200,
    )
    assert "valuation_history_rebuild" not in task._active_tasks


def test_valuation_input_sync_task_calls_data_manager_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_valuation_input_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "attempted_exchanges": 1,
            "source": "cninfo",
            "source_mode": "direct",
            "sync_mode": "incremental",
            "start_date": "2026-01-01",
            "end_date": "2026-05-29",
            "total_requested_instruments": 10,
            "total_covered_instruments": 10,
            "total_missing_instruments": 0,
            "total_snapshots_written": 10,
            "elapsed_seconds": 12.3,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "snapshots_written": 10,
                    "requested_instruments": 10,
                    "covered_instruments": 10,
                    "missing_instruments": 0,
                    "elapsed_seconds": 12.3,
                }
            ],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.valuation_input_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
            source="cninfo",
            source_mode="direct",
            sync_mode="incremental",
            start_date="2026-01-01",
            end_date="2026-05-29",
        )
    )

    assert result is True
    data_manager.run_valuation_input_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        source="cninfo",
        source_mode="direct",
        sync_mode="incremental",
        start_date="2026-01-01",
        end_date="2026-05-29",
    )
    assert "valuation_input_sync" not in task._active_tasks
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "content" in report_data
    assert "写入/更新: 10" in report_data["content"]
    assert "请求标的: 10" in report_data["content"]


def test_valuation_input_full_backfill_task_forces_full_mode_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_valuation_input_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "attempted_exchanges": 1,
            "source": "cninfo",
            "source_mode": "direct",
            "sync_mode": "full",
            "start_date": "1990-01-01",
            "end_date": "2026-05-29",
            "total_requested_instruments": 10,
            "total_covered_instruments": 10,
            "total_missing_instruments": 0,
            "total_snapshots_written": 100,
            "elapsed_seconds": 75.0,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "snapshots_written": 100,
                    "requested_instruments": 10,
                    "covered_instruments": 10,
                    "missing_instruments": 0,
                    "elapsed_seconds": 75.0,
                }
            ],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.valuation_input_full_backfill(
            exchanges=["SSE"],
            limit_per_exchange=10,
            source="cninfo",
            source_mode="direct",
            start_date="1990-01-01",
            end_date="2026-05-29",
        )
    )

    assert result is True
    data_manager.run_valuation_input_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        source="cninfo",
        source_mode="direct",
        sync_mode="full",
        start_date="1990-01-01",
        end_date="2026-05-29",
    )
    assert "valuation_input_full_backfill" not in task._active_tasks
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "content" in report_data
    assert "估值输入全量回填报告" in report_data["content"]
    assert "写入/更新: 100" in report_data["content"]
    assert "耗时: 1m15s" in report_data["content"]


def test_valuation_input_scheduler_config_keeps_daily_disabled_and_full_manual_only():
    config = json.loads(Path("config/05_scheduler.json").read_text())
    jobs = config["scheduler_config"]["jobs"]

    daily = jobs["valuation_input_sync"]
    assert daily["enabled"] is True
    assert daily["trigger"]["day_of_week"] == "mon-fri"
    assert daily["trigger"]["hour"] == 23
    assert daily["trigger"]["minute"] == 0
    assert daily["parameters"]["sync_mode"] == "incremental"
    assert daily["parameters"]["limit_per_exchange"] is None

    history = jobs["valuation_history_rebuild"]
    assert history["enabled"] is True
    assert history["trigger"]["day_of_week"] == "tue-sat"
    assert history["trigger"]["hour"] == 4
    assert history["trigger"]["minute"] == 45
    assert history["parameters"]["limit_per_exchange"] is None
    assert history["parameters"]["quote_limit_days"] == 7
    assert history["parameters"]["window_mode"] == "trading_days"
    assert history["parameters"]["write_policy"] == "missing_only"

    weekly = jobs["valuation_history_weekly_reconcile"]
    assert weekly["enabled"] is True
    assert weekly["trigger"]["day_of_week"] == "sat"
    assert weekly["trigger"]["hour"] == 5
    assert weekly["trigger"]["minute"] == 45
    assert weekly["parameters"]["quote_limit_days"] == 60
    assert weekly["parameters"]["write_policy"] == "missing_only"

    twelve_quarter_history = jobs["valuation_history_12q_rebuild"]
    assert twelve_quarter_history["enabled"] is True
    assert twelve_quarter_history["manual_only"] is True
    assert twelve_quarter_history["parameters"]["quote_limit_days"] is None
    assert twelve_quarter_history["parameters"]["window_mode"] == "last_12_quarters"
    assert twelve_quarter_history["parameters"]["write_policy"] == "missing_only"

    full_history_alias = jobs["valuation_history_full_rebuild"]
    assert full_history_alias["enabled"] is False
    assert full_history_alias["manual_only"] is True

    full = jobs["valuation_input_full_backfill"]
    assert full["enabled"] is True
    assert full["manual_only"] is True
    assert full["parameters"]["start_date"] == "1990-01-01"
    assert full["parameters"]["limit_per_exchange"] is None
