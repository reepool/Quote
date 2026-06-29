from unittest.mock import AsyncMock, Mock

import pytest

import scheduler.tasks as task_module
from scheduler.tasks import ScheduledTasks


@pytest.mark.unit
@pytest.mark.asyncio
async def test_weekly_maintenance_factor_sync_config_and_order(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._send_task_report = AsyncMock()

    order = []

    async def record(name, result=None):
        order.append(name)
        return result

    async def factor_sync_side_effect(**_):
        return await record(
            "factor_sync",
            {"HKEX": {"synced": 1, "skipped": 0, "failed": 0}},
        )

    data_manager = Mock()
    data_manager.db_ops = Mock()
    data_manager.db_ops.get_database_statistics = AsyncMock(return_value={})
    data_manager.db_ops.cleanup_ghost_instruments = AsyncMock(return_value=2)
    data_manager.backup_data = AsyncMock(return_value=True)
    data_manager.sync_all_adjustment_factors = AsyncMock(
        side_effect=factor_sync_side_effect
    )
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    monkeypatch.setattr(
        task_module.cache_manager.quote_cache,
        "clear_expired_data",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        task_module.cache_manager.general_cache,
        "_cleanup_expired",
        AsyncMock(return_value=None),
    )

    async def cleanup_logs_side_effect(*_):
        return await record("cleanup_logs")

    async def validate_side_effect():
        return await record("validate")

    async def optimize_side_effect():
        return await record("optimize")

    task._cleanup_old_logs = AsyncMock(side_effect=cleanup_logs_side_effect)
    task._validate_data_integrity = AsyncMock(side_effect=validate_side_effect)
    task._optimize_database = AsyncMock(side_effect=optimize_side_effect)

    result = await task.weekly_data_maintenance(
        backup_database=False,
        cleanup_old_logs=True,
        cleanup_ghost_stocks=True,
        sync_adjustment_factors=True,
        factor_sync_exchanges=["HKEX"],
        factor_sync_days_back=14,
        validate_data_integrity=True,
        optimize_database=True,
    )

    assert result is True
    data_manager.sync_all_adjustment_factors.assert_awaited_once_with(
        exchanges=["HKEX"],
        days_back=14,
    )
    assert order == [
        "cleanup_logs",
        "factor_sync",
        "validate",
        "optimize",
    ]
    data_manager.db_ops.cleanup_ghost_instruments.assert_not_awaited()
    data_manager.backup_data.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_weekly_maintenance_does_not_report_legacy_backup_success(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._send_task_report = AsyncMock()

    data_manager = Mock()
    data_manager.db_ops = Mock()
    data_manager.db_ops.get_database_statistics = AsyncMock(return_value={})
    data_manager.backup_data = AsyncMock(return_value=True)
    data_manager.sync_all_adjustment_factors = AsyncMock(return_value={})
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    monkeypatch.setattr(
        task_module.cache_manager.quote_cache,
        "clear_expired_data",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        task_module.cache_manager.general_cache,
        "_cleanup_expired",
        AsyncMock(return_value=None),
    )
    task._cleanup_old_logs = AsyncMock(return_value=None)
    task._validate_data_integrity = AsyncMock(return_value=None)
    task._optimize_database = AsyncMock(return_value=None)

    result = await task.weekly_data_maintenance(
        backup_database=True,
        cleanup_old_logs=False,
        sync_adjustment_factors=False,
        validate_data_integrity=False,
        optimize_database=False,
    )

    assert result is True
    data_manager.backup_data.assert_not_awaited()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    backup_entry = report_data["maintenance_tasks"][0]
    assert backup_entry == {"task_name": "数据库备份", "status": "独立任务执行"}
