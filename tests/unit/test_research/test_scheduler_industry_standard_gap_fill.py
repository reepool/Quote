import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


def test_industry_standard_gap_fill_task_calls_data_manager_and_clears_active_flag(
    monkeypatch,
):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_industry_standard_gap_fill_sync = AsyncMock(
        return_value={
            "status": "success",
            "targeted_instrument_count": 2,
            "repaired_instrument_count": 2,
            "coverage_before": {
                "target_instrument_count": 3,
                "missing_authoritative_membership_count": 2,
            },
            "coverage_after": {
                "target_instrument_count": 3,
                "missing_authoritative_membership_count": 0,
            },
            "sync": {
                "status": "success",
                "exchanges": [
                    {
                        "exchange": "SSE",
                        "status": "success",
                        "memberships_written": 2,
                    }
                ],
            },
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.industry_standard_gap_fill(
            exchanges=["SSE"],
            missing_limit_per_exchange=50,
            budget_mode="availability_first",
            allow_paid_proxy=True,
        )
    )

    assert result is True
    data_manager.run_industry_standard_gap_fill_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        missing_limit_per_exchange=50,
        budget_mode="availability_first",
        allow_paid_proxy=True,
    )
    task._send_task_report.assert_awaited_once()
    assert "industry_standard_gap_fill" not in task._active_tasks
