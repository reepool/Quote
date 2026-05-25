import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks
from utils.task_manager.handlers import TaskManagerHandlers


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


def test_financial_l1_full_import_task_calls_data_manager_and_reports(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_financial_l1_full_import = AsyncMock(
        return_value={
            "status": "success_with_review",
            "db_path": "data/financials.db",
            "log_dir": "log/financial_l1_full_import/test",
            "manifest_path": "log/financial_l1_full_import/test/manifest.json",
            "progress_path": "log/financial_l1_full_import/test/progress_state.json",
            "report_periods": ["2026-03-31"],
            "completed_batch_count": 1,
            "selected_batch_count": 1,
            "batch_count": 1,
            "target_count": 10,
            "review_batches": [{"batch_index": 1}],
            "failed_batches": [],
            "elapsed_seconds": 1.2,
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.financial_l1_full_import(
            exchanges=["SSE"],
            period_window="latest",
            rolling_quarters=1,
            db_path="data/financials.db",
            batch_size=2,
        )
    )

    assert result is True
    data_manager.run_financial_l1_full_import.assert_awaited_once_with(
        exchanges=["SSE"],
        report_periods=None,
        period_window="latest",
        rolling_quarters=1,
        baseline_report_period="2024Q1",
        latest_report_period=None,
        db_path="data/financials.db",
        log_dir=None,
        limit_per_exchange=None,
        batch_size=2,
        resume=False,
        request_interval_seconds=0.2,
        request_timeout_seconds=20.0,
        financial_disclosure_events_path=None,
        manifest_only=False,
        start_batch=None,
        end_batch=None,
        max_batches=None,
    )
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "财务 L1" in report_data["name"]
    assert "需复核批次" in report_data["content"]
    assert "financial_l1_full_import" not in task._active_tasks


def test_manual_only_financial_l1_full_import_can_run_without_scheduler_job(monkeypatch):
    import utils

    task_manager = Mock()
    task_manager.logger = Mock()
    task_manager.task_scheduler = Mock()
    task_manager.task_scheduler.jobs = {}
    task_manager.job_config_manager = Mock()
    task_manager.job_config_manager.get_job_config.return_value = Mock()

    monkeypatch.setattr(
        utils.config_manager,
        "get_nested",
        lambda path, default=None: {
            "enabled": True,
            "manual_only": True,
            "parameters": {
                "exchanges": ["SSE"],
                "period_window": "latest",
                "max_runtime_seconds": 30,
            },
        }
        if path == "scheduler_config.jobs.financial_l1_full_import"
        else default,
    )
    manual_task = AsyncMock(return_value=True)
    monkeypatch.setattr(task_module.scheduled_tasks, "financial_l1_full_import", manual_task)

    handler = TaskManagerHandlers(task_manager)
    result = asyncio.run(
        handler._execute_task_direct(
            chat_id=1,
            job_id="financial_l1_full_import",
        )
    )

    assert result is True
    manual_task.assert_awaited_once()
    assert manual_task.await_args.kwargs["exchanges"] == ["SSE"]
    assert "max_runtime_seconds" not in manual_task.await_args.kwargs


def test_financial_disclosure_incremental_task_reports_pending_delisting(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_financial_disclosure_incremental_sync = AsyncMock(
        return_value={
            "status": "degraded",
            "db_path": "data/financials.db",
            "reconciliation": False,
            "report_periods": ["2026-03-31"],
            "announcements_scanned": 10,
            "selected_announcements": 1,
            "pages_scanned": 1,
            "candidate_count": 1,
            "candidate_sources": {
                "new_event": 0,
                "pending_state": 1,
                "local_gap": 0,
                "filtered_stale_pending": 2,
            },
            "changed_count": 0,
            "unchanged_count": 0,
            "pending_recheck_count": 0,
            "pending_delisting_risk_count": 1,
            "accepted_gap_count": 1,
            "blocking_gap_count": 0,
            "failed_count": 0,
            "financial_like_announcements": 3,
            "filtered_financial_like_announcements": 2,
            "selected_without_event_count": 0,
            "source_routing": {
                "cninfo_attempts": 1,
                "cninfo_successes": 0,
                "cninfo_batch_successes": 1,
                "cninfo_missing_or_ambiguous": 1,
                "fallback_attempts": 1,
                "fallback_successes": 0,
                "errors": [],
            },
            "elapsed_seconds": 0.5,
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.financial_disclosure_incremental_sync(
            exchanges=["SZSE"],
            dry_run=True,
        )
    )

    assert result is True
    data_manager.run_financial_disclosure_incremental_sync.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "待退市风险 1" in report_data["content"]
    assert "过滤噪声 2" in report_data["content"]
    assert "旧噪声过滤 2" in report_data["content"]
    assert "CNInfo尝试 1" in report_data["content"]
    assert "批处理通过 1" in report_data["content"]
    assert "fallback尝试 1" in report_data["content"]
    assert "不会改写股票主数据退市状态" in report_data["content"]
    assert "financial_disclosure_incremental_sync" not in task._active_tasks
