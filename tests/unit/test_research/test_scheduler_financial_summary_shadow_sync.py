import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


def test_financial_summary_shadow_sync_task_calls_data_manager_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    governance = {
        "status": "fresh",
        "action": "reused_fresh_master",
        "summary": {"added_instruments": 0, "deactivated_instruments": 0, "active_count": 1},
        "warnings": [],
        "errors": [],
    }
    data_manager.run_financial_summary_shadow_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "instrument_master_governance": governance,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "source": "baostock",
                }
            ],
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.financial_summary_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
            budget_mode="balanced",
            allow_paid_proxy=False,
        )
    )

    assert result is True
    data_manager.run_financial_summary_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        budget_mode="balanced",
        allow_paid_proxy=False,
    )
    task._send_task_report.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert report_data["instrument_master_governance"] == governance
    assert "状态: fresh" in report_data["instrument_master_governance_summary"]
    assert "financial_summary_shadow_sync" not in task._active_tasks
