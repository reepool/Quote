import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks
from utils.task_manager.handlers import TaskManagerHandlers


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
        write_policy="refresh_all",
    )
    data_manager.get_research_shareholder_readiness.assert_awaited_once()
    task._send_task_report.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "结论: *成功" in report_data["content"]
    assert "本次写入/刷新快照: 1" in report_data["content"]
    assert "readiness: ready" in report_data["content"]
    assert "• SSE: success，覆盖 1/1，写入 1，未变 0，缺口 0" in report_data["content"]
    assert "来源: cninfo:direct" in report_data["content"]
    assert "shareholder_shadow_sync" not in task._active_tasks


def test_scheduler_timeout_error_message_is_not_blank():
    from scheduler.scheduler import TaskScheduler

    scheduler = TaskScheduler()
    assert (
        scheduler._format_job_exception(asyncio.TimeoutError())
        == "TimeoutError: task exceeded max_runtime_seconds"
    )


def test_manual_only_shareholder_shadow_sync_can_run_without_scheduler_job(monkeypatch):
    import utils

    task_manager = Mock()
    task_manager.logger = Mock()
    task_manager.task_scheduler = Mock()
    task_manager.task_scheduler.jobs = {}
    task_manager.task_scheduler.execute_job_direct = AsyncMock(return_value=True)
    task_manager.job_config_manager = Mock()
    task_manager.job_config_manager.get_job_config.return_value = Mock()

    monkeypatch.setattr(
        utils.config_manager,
        "get_nested",
        lambda path, default=None: {
            "enabled": True,
            "manual_only": True,
            "parameters": {"exchanges": ["SSE"], "max_runtime_seconds": 30},
        }
        if path == "scheduler_config.jobs.shareholder_shadow_sync"
        else default,
    )
    manual_task = AsyncMock(return_value=True)
    monkeypatch.setattr(task_module.scheduled_tasks, "shareholder_shadow_sync", manual_task)

    handler = TaskManagerHandlers(task_manager)
    result = asyncio.run(
        handler._execute_task_direct(
            chat_id=1,
            job_id="shareholder_shadow_sync",
        )
    )

    assert result is True
    task_manager.task_scheduler.execute_job_direct.assert_awaited_once_with(
        "shareholder_shadow_sync",
        include_dependencies=True,
    )
    manual_task.assert_not_awaited()


def test_shareholder_incremental_sync_task_reports_change_summary(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_shareholder_incremental_sync = AsyncMock(
        return_value={
            "status": "success",
            "dry_run": False,
            "pages_scanned": 2,
            "announcements_scanned": 20,
            "selected_announcements": 3,
            "candidate_instruments": 2,
            "changed_instruments": 1,
            "unchanged_instruments": 1,
            "pending_rechecks": 0,
            "failed_instruments": 0,
            "snapshots_written": 1,
            "would_write_snapshots": 1,
            "attempted_sources": ["cninfo:direct"],
            "successful_sources": ["cninfo:direct"],
            "failed_instrument_ids": [],
            "scan_errors": [],
        }
    )
    data_manager.get_research_shareholder_readiness = AsyncMock(
        return_value={
            "ready_for_paid_high_availability_rollout": True,
            "missing_snapshot_count": 0,
            "blockers": [],
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.shareholder_incremental_sync(
            exchanges=["SSE"],
            lookback_days=7,
            max_candidates=10,
            dry_run=True,
        )
    )

    assert result is True
    data_manager.run_shareholder_incremental_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        lookback_days=7,
        overlap_days=None,
        page_size=None,
        max_pages_per_market=None,
        max_candidates=10,
        pending_recheck_days=None,
        budget_mode=None,
        allow_paid_proxy=None,
        dry_run=True,
    )
    task._send_task_report.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "任务: `shareholder_incremental_sync`" in report_data["content"]
    assert "公告扫描: pages=2，records=20，selected=3" in report_data["content"]
    assert "候选标的: 2" in report_data["content"]
    assert "变化写入: 1" in report_data["content"]
    assert "shareholder_incremental_sync" not in task._active_tasks


def test_shareholder_reconciliation_sync_uses_changed_only_policy(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_shareholder_shadow_sync = AsyncMock(
        return_value={
            "status": "success",
            "write_policy": "changed_only",
            "successful_exchanges": 1,
            "attempted_exchanges": 1,
            "total_snapshots_written": 0,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "source": "cninfo",
                    "requested_instruments": 1,
                    "resolved_instruments": 1,
                    "snapshots_written": 0,
                    "unchanged_instruments": 1,
                    "missing_instruments": 0,
                    "attempted_sources": ["cninfo:direct"],
                    "successful_sources": ["cninfo:direct"],
                }
            ],
        }
    )
    data_manager.get_research_shareholder_readiness = AsyncMock(return_value={})
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(task.shareholder_reconciliation_sync(exchanges=["SSE"]))

    assert result is True
    data_manager.run_shareholder_shadow_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=None,
        budget_mode=None,
        allow_paid_proxy=None,
        write_policy="changed_only",
    )
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "写入策略: `changed_only`" in report_data["content"]
    assert "本次无需改写快照: 1" in report_data["content"]
    assert "shareholder_reconciliation_sync" not in task._active_tasks
