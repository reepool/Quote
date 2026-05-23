import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


def test_shareholder_shadow_sync_task_calls_data_manager_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_shareholder_shadow_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "attempted_exchanges": 1,
            "total_snapshots_written": 1,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "source": "efinance",
                    "requested_instruments": 1,
                    "resolved_instruments": 1,
                    "snapshots_written": 1,
                    "missing_instruments": 0,
                    "attempted_sources": ["cninfo:direct"],
                    "successful_sources": ["cninfo:direct"],
                }
            ],
        }
    )
    data_manager.get_research_shareholder_readiness = AsyncMock(
        return_value={
            "ready_for_paid_high_availability_rollout": True,
            "target_instrument_count": 1,
            "snapshot_total": 1,
            "missing_snapshot_count": 0,
            "scope_counts": {
                "holder_count": 1,
                "top10_holders": 1,
                "reference_only_ownership_clues": 1,
            },
            "blockers": [],
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.shareholder_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
            budget_mode="balanced",
            allow_paid_proxy=False,
        )
    )

    assert result is True
    data_manager.run_shareholder_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        budget_mode="balanced",
        allow_paid_proxy=False,
    )
    data_manager.get_research_shareholder_readiness.assert_awaited_once()
    task._send_task_report.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "结论: *成功" in report_data["content"]
    assert "本次写入/刷新快照: 1" in report_data["content"]
    assert "readiness: ready" in report_data["content"]
    assert "• SSE: success，覆盖 1/1，写入 1，缺口 0" in report_data["content"]
    assert "来源: cninfo:direct" in report_data["content"]
    assert "shareholder_shadow_sync" not in task._active_tasks


def test_scheduler_timeout_error_message_is_not_blank():
    from scheduler.scheduler import TaskScheduler

    scheduler = TaskScheduler()
    assert (
        scheduler._format_job_exception(asyncio.TimeoutError())
        == "TimeoutError: task exceeded max_runtime_seconds"
    )
