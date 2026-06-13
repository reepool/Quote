from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from utils.task_manager.task_manager import TaskManagerBot


def _build_task_manager_bot(*, restart_cfg=None, authorized_chats=None):
    task_scheduler = Mock()
    task_scheduler.load_jobs_from_config = AsyncMock()
    task_scheduler.running_tasks = {}
    job_config_manager = Mock()
    config_manager = Mock()

    def _get_nested(path, default=None):
        if path == "telegram_config.chat_id":
            return authorized_chats if authorized_chats is not None else ["471105519"]
        if path == "telegram_config.ops.service_restart":
            return restart_cfg if restart_cfg is not None else {}
        return default

    config_manager.get_nested.side_effect = _get_nested

    bot = TaskManagerBot(
        telegram_bot=Mock(),
        task_scheduler=task_scheduler,
        job_config_manager=job_config_manager,
        scheduler_monitor=Mock(),
        config_manager=config_manager,
        logger=Mock(),
    )
    bot.send_message = AsyncMock()
    return bot, config_manager, task_scheduler, job_config_manager


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


@pytest.mark.asyncio
async def test_restart_system_command_rejects_unauthorized_chat():
    bot, *_ = _build_task_manager_bot(
        restart_cfg={"enabled": True},
        authorized_chats=["471105519"],
    )
    event = SimpleNamespace(chat_id=1, sender_id=2, text="/restart_system confirm")

    with patch("utils.task_manager.task_manager.asyncio.create_task") as create_task:
        await bot.handle_restart_system_command(event)

    create_task.assert_not_called()
    sent_message = bot.send_message.await_args.args[1]
    assert "未授权" in sent_message


@pytest.mark.asyncio
async def test_restart_system_command_requires_enabled_config():
    bot, *_ = _build_task_manager_bot(restart_cfg={"enabled": False})
    event = SimpleNamespace(chat_id=471105519, sender_id=2, text="/restart_system confirm")

    with patch("utils.task_manager.task_manager.asyncio.create_task") as create_task:
        await bot.handle_restart_system_command(event)

    create_task.assert_not_called()
    sent_message = bot.send_message.await_args.args[1]
    assert "未启用" in sent_message


@pytest.mark.asyncio
async def test_restart_system_command_requires_confirmation():
    bot, *_ = _build_task_manager_bot(restart_cfg={"enabled": True})
    event = SimpleNamespace(chat_id=471105519, sender_id=2, text="/restart_system")

    with patch("utils.task_manager.task_manager.asyncio.create_task") as create_task:
        await bot.handle_restart_system_command(event)

    create_task.assert_not_called()
    sent_message = bot.send_message.await_args.args[1]
    assert "/restart_system confirm" in sent_message


@pytest.mark.asyncio
async def test_restart_system_command_blocks_when_tasks_are_running():
    bot, _, task_scheduler, _ = _build_task_manager_bot(
        restart_cfg={
            "enabled": True,
            "mode": "self_exit",
            "service_name": "quote-system.service",
            "delay_seconds": 0,
            "exit_code": 1,
        }
    )
    task_scheduler.running_tasks = {
        "daily_data_update": {"run-1": object()},
        "index_master_governance_sync": {"run-2": object()},
    }
    event = SimpleNamespace(chat_id=471105519, sender_id=2, text="/restart_system confirm")

    with patch.object(bot, "_restart_service_by_self_exit", new=AsyncMock()) as self_exit, patch(
        "utils.task_manager.task_manager.asyncio.create_task",
    ) as create_task:
        await bot.handle_restart_system_command(event)

    self_exit.assert_not_called()
    create_task.assert_not_called()
    sent_message = bot.send_message.await_args.args[1]
    assert "暂不重启系统服务" in sent_message
    assert "`daily_data_update`" in sent_message
    assert "`index_master_governance_sync`" in sent_message


@pytest.mark.asyncio
async def test_restart_system_command_submits_fixed_service_restart():
    bot, *_ = _build_task_manager_bot(
        restart_cfg={
            "enabled": True,
            "mode": "systemctl",
            "service_name": "quote-system.service",
            "systemctl_path": "/bin/systemctl",
            "use_sudo": True,
            "delay_seconds": 0,
            "timeout_seconds": 3,
        }
    )
    event = SimpleNamespace(chat_id=471105519, sender_id=2, text="/restart_system confirm")

    with patch.object(bot, "_restart_service_after_delay", new=AsyncMock()) as restart_after_delay, patch(
        "utils.task_manager.task_manager.asyncio.create_task",
        side_effect=lambda coro: coro.close() or Mock(),
    ) as create_task:
        await bot.handle_restart_system_command(event)

    restart_after_delay.assert_called_once_with(
        chat_id=471105519,
        command=["sudo", "/bin/systemctl", "restart", "quote-system.service"],
        delay_seconds=0.0,
        timeout_seconds=3.0,
    )
    create_task.assert_called_once()
    sent_message = bot.send_message.await_args.args[1]
    assert "已提交系统服务重启请求" in sent_message


@pytest.mark.asyncio
async def test_restart_system_command_submits_self_exit_restart():
    bot, *_ = _build_task_manager_bot(
        restart_cfg={
            "enabled": True,
            "mode": "self_exit",
            "service_name": "quote-system.service",
            "delay_seconds": 0,
            "exit_code": 1,
        }
    )
    event = SimpleNamespace(chat_id=471105519, sender_id=2, text="/restart_system confirm")

    with patch.object(bot, "_restart_service_by_self_exit", new=AsyncMock()) as self_exit, patch(
        "utils.task_manager.task_manager.asyncio.create_task",
        side_effect=lambda coro: coro.close() or Mock(),
    ) as create_task:
        await bot.handle_restart_system_command(event)

    self_exit.assert_called_once_with(delay_seconds=0.0, exit_code=1)
    create_task.assert_called_once()
    sent_message = bot.send_message.await_args.args[1]
    assert "模式: `self_exit`" in sent_message
    assert "self_exit(exit_code=1)" in sent_message


def test_task_manager_authorization_accepts_string_config_chat_ids():
    bot, *_ = _build_task_manager_bot(authorized_chats=["471105519"])

    assert bot.is_authorized(471105519) is True
    assert bot.is_authorized("471105519") is True
    assert bot.is_authorized(1) is False
