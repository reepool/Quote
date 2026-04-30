from unittest.mock import AsyncMock, Mock, patch

import pytest

from utils.task_manager.task_manager import TaskManagerBot


@pytest.mark.asyncio
async def test_reload_scheduler_config_refreshes_data_manager_runtime_config():
    task_scheduler = Mock()
    task_scheduler.load_jobs_from_config = AsyncMock()
    job_config_manager = Mock()
    config_manager = Mock()

    bot = TaskManagerBot(
        telegram_bot=Mock(),
        task_scheduler=task_scheduler,
        job_config_manager=job_config_manager,
        scheduler_monitor=Mock(),
        config_manager=config_manager,
        logger=Mock(),
    )

    with patch("data_manager.data_manager.refresh_runtime_config") as refresh_runtime_config, patch(
        "utils.report.reload_report_config"
    ) as reload_report_config:
        success = await bot.reload_scheduler_config()

    assert success is True
    config_manager.reload_config.assert_called_once_with()
    refresh_runtime_config.assert_called_once_with()
    job_config_manager.load_job_configs.assert_called_once_with()
    task_scheduler.load_jobs_from_config.assert_awaited_once_with()
    reload_report_config.assert_called_once_with()
