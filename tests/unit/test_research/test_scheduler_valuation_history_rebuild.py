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
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "snapshots_written": 10,
                    "missing_instruments": 0,
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


def test_valuation_input_full_backfill_task_forces_full_mode_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_valuation_input_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "snapshots_written": 100,
                    "missing_instruments": 0,
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


def test_valuation_input_scheduler_config_keeps_daily_disabled_and_full_manual_only():
    config = json.loads(Path("config/05_scheduler.json").read_text())
    jobs = config["scheduler_config"]["jobs"]

    daily = jobs["valuation_input_sync"]
    assert daily["enabled"] is False
    assert daily["trigger"]["day_of_week"] == "tue-sat"
    assert daily["trigger"]["hour"] == 4
    assert daily["trigger"]["minute"] == 30
    assert daily["parameters"]["sync_mode"] == "incremental"
    assert daily["parameters"]["limit_per_exchange"] is None

    history = jobs["valuation_history_rebuild"]
    assert history["enabled"] is False
    assert history["trigger"]["day_of_week"] == "tue-sat"
    assert history["trigger"]["hour"] == 4
    assert history["trigger"]["minute"] == 45
    assert history["parameters"]["limit_per_exchange"] is None

    full = jobs["valuation_input_full_backfill"]
    assert full["enabled"] is True
    assert full["manual_only"] is True
    assert full["parameters"]["start_date"] == "1990-01-01"
    assert full["parameters"]["limit_per_exchange"] is None
