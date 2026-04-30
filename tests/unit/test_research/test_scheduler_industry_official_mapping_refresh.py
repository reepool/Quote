import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


def test_industry_official_mapping_refresh_task_calls_data_manager_and_clears_active_flag(
    monkeypatch,
):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_industry_official_mapping_refresh = AsyncMock(
        return_value={
            "status": "success",
            "source": "akshare",
            "mode": "direct",
            "mapping_cache_rows_written": 413,
            "mapped_code_count": 320,
            "total_code_count": 413,
            "component_taxonomy_count": 312,
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.industry_official_mapping_refresh(
            exchanges=["SSE"],
            budget_mode="balanced",
            allow_paid_proxy=False,
        )
    )

    assert result is True
    data_manager.run_industry_official_mapping_refresh.assert_awaited_once_with(
        exchanges=["SSE"],
        budget_mode="balanced",
        allow_paid_proxy=False,
    )
    task._send_task_report.assert_awaited_once()
    assert "industry_official_mapping_refresh" not in task._active_tasks
