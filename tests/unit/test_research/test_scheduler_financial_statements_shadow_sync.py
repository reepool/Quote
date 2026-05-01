import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


def test_financial_statements_shadow_sync_task_calls_data_manager_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_financial_statements_shadow_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "source": "akshare",
                }
            ],
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.financial_statements_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
            budget_mode="balanced",
            allow_paid_proxy=False,
        )
    )

    assert result is True
    data_manager.run_financial_statements_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        budget_mode="balanced",
        allow_paid_proxy=False,
    )
    task._send_task_report.assert_awaited_once()
    assert "financial_statements_shadow_sync" not in task._active_tasks


def test_financial_statements_catchup_task_passes_incremental_controls(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_financial_statements_shadow_sync = AsyncMock(
        return_value={"status": "success", "successful_exchanges": 1, "exchanges": []}
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.financial_statements_catchup_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
            budget_mode="balanced",
            allow_paid_proxy=False,
            sync_mode="catchup",
            force_full=False,
        )
    )

    assert result is True
    data_manager.run_financial_statements_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        budget_mode="balanced",
        allow_paid_proxy=False,
        sync_mode="catchup",
        force_full=False,
    )
    assert "financial_statements_catchup_sync" not in task._active_tasks


def test_financial_statements_reconciliation_task_forces_full_check(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_financial_statements_shadow_sync = AsyncMock(
        return_value={"status": "degraded", "successful_exchanges": 1, "exchanges": []}
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.financial_statements_reconciliation_sync(
            exchanges=["SSE"],
            limit_per_exchange=20,
            budget_mode="availability_first",
            allow_paid_proxy=True,
        )
    )

    assert result is True
    data_manager.run_financial_statements_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=20,
        budget_mode="availability_first",
        allow_paid_proxy=True,
        sync_mode="catchup",
        force_full=True,
    )
    assert "financial_statements_reconciliation_sync" not in task._active_tasks
