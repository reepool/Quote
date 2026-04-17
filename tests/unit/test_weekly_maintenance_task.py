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

    async def cleanup_ghost_side_effect(**_):
        return await record("cleanup_ghost", 2)

    async def factor_sync_side_effect(**_):
        return await record(
            "factor_sync",
            {"HKEX": {"synced": 1, "skipped": 0, "failed": 0}},
        )

    data_manager = Mock()
    data_manager.db_ops = Mock()
    data_manager.db_ops.get_database_statistics = AsyncMock(return_value={})
    data_manager.db_ops.cleanup_ghost_instruments = AsyncMock(
        side_effect=cleanup_ghost_side_effect
    )
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
        "cleanup_ghost",
        "factor_sync",
        "validate",
        "optimize",
    ]
