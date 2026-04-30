import asyncio
from unittest.mock import AsyncMock

from scheduler.tasks import ScheduledTasks


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_research_report_sync_task_calls_data_manager_and_clears_active_flag():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_research_report_shadow_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [{"exchange": "SSE", "status": "success", "reports_written": 5}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.research_report_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result is True
    data_manager.run_research_report_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        budget_mode=None,
        allow_paid_proxy=None,
    )
    assert "research_report_sync" not in task._active_tasks
