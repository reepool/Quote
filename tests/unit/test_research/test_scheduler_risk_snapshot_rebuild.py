import asyncio
from unittest.mock import AsyncMock

from scheduler.tasks import ScheduledTasks


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_risk_snapshot_rebuild_task_calls_data_manager_and_clears_active_flag():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_risk_snapshot_rebuild = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [{"exchange": "SSE", "status": "success", "rows_written": 10}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.risk_snapshot_rebuild(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result is True
    data_manager.run_risk_snapshot_rebuild.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
    )
    assert "risk_snapshot_rebuild" not in task._active_tasks
