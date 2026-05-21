import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


def test_industry_standard_sync_task_calls_data_manager_and_clears_active_flag(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_industry_standard_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "memberships_written": 1,
                }
            ],
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(
        task.industry_standard_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
            budget_mode="balanced",
            allow_paid_proxy=False,
        )
    )

    assert result is True
    data_manager.run_industry_standard_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        budget_mode="balanced",
        allow_paid_proxy=False,
        force_component_refresh=False,
    )
    task._send_task_report.assert_awaited_once()
    assert "industry_standard_sync" not in task._active_tasks


def test_industry_standard_sync_report_uses_operator_facing_content(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_industry_standard_sync = AsyncMock(
        return_value={
            "status": "success",
            "source": "swsresearch",
            "mode": "direct",
            "source_files_unchanged": True,
            "taxonomy_nodes_written": 0,
            "classification_history_rows_written": 0,
            "total_memberships_written": 0,
            "total_official_classifications_written": 0,
            "successful_exchanges": 3,
            "attempted_exchanges": 3,
            "exchanges": [
                {
                    "exchange": "SSE",
                    "status": "success",
                    "memberships_written": 0,
                    "official_classifications_written": 0,
                    "diagnostics": {
                        "source_files_unchanged": True,
                        "existing_authoritative_memberships": 100,
                        "target_instruments": 100,
                    },
                }
            ],
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(task.industry_standard_sync(exchanges=["SSE", "SZSE", "BSE"]))

    assert result is True
    report_kwargs = task._send_task_report.await_args.kwargs
    content = report_kwargs["report_data"]["content"]
    assert "研究域申万官方分类文件每日同步" in content
    assert "任务: `industry_standard_sync`" in content
    assert "结论: *成功* - 官方分类文件未变化" in content
    assert "source_files: `unchanged`" in content
    assert "memberships_written: `0`" in content
    assert "任务执行状态" not in content


def test_industry_index_analysis_sync_report_uses_operator_facing_content(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.run_industry_index_analysis_sync = AsyncMock(
        return_value={
            "status": "success",
            "operation": "latest",
            "source": "swsresearch",
            "mode": "direct",
            "rows_written": 337,
            "summary": {
                "latest_trade_date": "2026-05-20",
                "distinct_index_codes": 337,
                "index_type_counts": {
                    "一级行业": {"rows": 31, "codes": 31, "trade_dates": 1},
                    "二级行业": {"rows": 134, "codes": 134, "trade_dates": 1},
                },
            },
        }
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    result = asyncio.run(task.industry_index_analysis_sync())

    assert result is True
    report_kwargs = task._send_task_report.await_args.kwargs
    content = report_kwargs["report_data"]["content"]
    assert "研究域申万行业指数分析日频指标同步" in content
    assert "任务: `industry_index_analysis_sync`" in content
    assert "结论: *成功* - 同步成功，本次写入 337 行指数分析指标" in content
    assert "latest_trade_date: `2026-05-20`" in content
    assert "该任务只写 `industry_index_analysis_daily`，不改股票行业归属" in content
    assert "任务执行状态" not in content
