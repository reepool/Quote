"""
Unit tests for Telegram task manager
Tests the task management functionality through Telegram bot
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
import asyncio

from tests.mocks import MockTelegramBot, MockScheduler
from tests.factories import ConfigFactory


@pytest.mark.unit
class TestTaskManager:
    """Test task manager core functionality"""

    @pytest.fixture
    def mock_telegram_bot(self):
        """Create mock Telegram bot"""
        return MockTelegramBot()

    @pytest.fixture
    def mock_scheduler(self):
        """Create mock scheduler"""
        return MockScheduler()

    @pytest.fixture
    def task_config(self):
        """Create task configuration"""
        return {
            "daily_data_update": {
                "enabled": True,
                "schedule": "0 20 * * 1-5",  # Weekdays 8 PM
                "description": "每日数据更新"
            },
            "system_health_check": {
                "enabled": True,
                "schedule": "0 * * * *",     # Every hour
                "description": "系统健康检查"
            },
            "database_backup": {
                "enabled": True,
                "schedule": "0 6 * * 6",     # Saturday 6 AM
                "description": "数据库备份"
            }
        }

    @pytest.fixture
    def task_manager(self, mock_telegram_bot, mock_scheduler, task_config):
        """Create task manager instance"""
        from utils.task_manager.task_manager import TaskManager

        with patch('utils.task_manager.task_manager.TelegramBot', return_value=mock_telegram_bot), \
             patch('utils.task_manager.task_manager.Scheduler', return_value=mock_scheduler):

            config = ConfigFactory.create_test_config()
            config["telegram_config"] = {
                "enabled": True,
                "chat_id": ["123456789"]
            }
            config["scheduler_config"] = {
                "enabled": True,
                "timezone": "Asia/Shanghai"
            }

            manager = TaskManager(config)
            manager.task_config = task_config
            return manager

    @pytest.mark.asyncio
    async def test_task_manager_initialization(self, task_manager):
        """Test task manager initialization"""
        # Check that bot and scheduler are initialized
        assert task_manager.bot is not None
        assert task_manager.scheduler is not None

        # Check task configuration is loaded
        assert len(task_manager.task_config) > 0
        assert "daily_data_update" in task_manager.task_config

    @pytest.mark.asyncio
    async def test_start_command_handling(self, task_manager):
        """Test handling of /start command"""
        # Mock user message
        user_id = 123456789
        message_text = "/start"

        # Create mock update and message
        mock_update = Mock()
        mock_update.message.from_user.id = user_id
        mock_update.message.text = message_text

        # Handle start command
        await task_manager.handle_start_command(mock_update)

        # Verify bot sent welcome message
        task_manager.bot.send_message.assert_called_once()
        call_args = task_manager.bot.send_message.call_args[1]

        assert "chat_id" in call_args
        assert "text" in call_args
        assert "欢迎使用" in call_args["text"] or "欢迎" in call_args["text"]

    @pytest.mark.asyncio
    async def test_status_command_handling(self, task_manager):
        """Test handling of /status command"""
        # Mock scheduler jobs
        job1 = Mock()
        job1.id = "daily_data_update"
        job1.next_run_time = datetime.now() + timedelta(hours=2)
        job1.paused = False

        job2 = Mock()
        job2.id = "system_health_check"
        job2.next_run_time = datetime.now() + timedelta(minutes=30)
        job2.paused = True

        task_manager.scheduler.jobs = {
            "daily_data_update": job1,
            "system_health_check": job2
        }

        # Mock user message
        mock_update = Mock()
        mock_update.message.from_user.id = 123456789
        mock_update.message.text = "/status"

        # Handle status command
        await task_manager.handle_status_command(mock_update)

        # Verify bot sent status message
        task_manager.bot.send_message.assert_called_once()
        call_args = task_manager.bot.send_message.call_args[1]

        status_text = call_args["text"]
        assert "任务状态" in status_text or "状态" in status_text
        assert "daily_data_update" in status_text
        assert "system_health_check" in status_text

    @pytest.mark.asyncio
    async def test_detail_command_handling(self, task_manager):
        """Test handling of /detail command"""
        # Setup specific job
        job = Mock()
        job.id = "daily_data_update"
        job.next_run_time = datetime.now() + timedelta(hours=2)
        job.paused = False
        job.kwargs = {"exchanges": ["SSE", "SZSE"]}

        task_manager.scheduler.jobs = {"daily_data_update": job}

        # Mock user message
        mock_update = Mock()
        mock_update.message.from_user.id = 123456789
        mock_update.message.text = "/detail daily_data_update"

        # Handle detail command
        await task_manager.handle_detail_command(mock_update)

        # Verify bot sent detail message
        task_manager.bot.send_message.assert_called_once()
        call_args = task_manager.bot.send_message.call_args[1]

        detail_text = call_args["text"]
        assert "daily_data_update" in detail_text
        assert "详情" in detail_text or "信息" in detail_text

    @pytest.mark.asyncio
    async def test_reload_config_command(self, task_manager):
        """Test handling of /reload_config command"""
        # Mock configuration reload
        mock_config = ConfigFactory.create_test_config()

        with patch.object(task_manager, 'reload_configuration', AsyncMock(return_value=True)) as mock_reload:
            # Mock user message
            mock_update = Mock()
            mock_update.message.from_user.id = 123456789
            mock_update.message.text = "/reload_config"

            # Handle reload command
            await task_manager.handle_reload_config_command(mock_update)

            # Verify configuration was reloaded
            mock_reload.assert_called_once()

            # Verify bot sent confirmation message
            task_manager.bot.send_message.assert_called_once()
            call_args = task_manager.bot.send_message.call_args[1]

            assert "配置" in call_args["text"] and "重新" in call_args["text"]
            assert "成功" in call_args["text"] or "完成" in call_args["text"]

    @pytest.mark.asyncio
    async def test_task_execution_control(self, task_manager):
        """Test task execution control (pause/resume/run)"""
        job_id = "daily_data_update"

        # Mock existing job
        job = Mock()
        job.id = job_id
        job.paused = False
        task_manager.scheduler.jobs = {job_id: job}

        # Test pausing a task
        mock_update = Mock()
        mock_update.message.from_user.id = 123456789
        mock_update.message.text = f"/pause {job_id}"

        await task_manager.handle_pause_command(mock_update)

        # Verify scheduler pause was called
        task_manager.scheduler.pause_job.assert_called_once_with(job_id)
        task_manager.bot.send_message.assert_called()

        # Test resuming a task
        mock_update.message.text = f"/resume {job_id}"
        task_manager.bot.send_message.reset_mock()

        await task_manager.handle_resume_command(mock_update)

        # Verify scheduler resume was called
        task_manager.scheduler.resume_job.assert_called_once_with(job_id)
        task_manager.bot.send_message.assert_called()

    @pytest.mark.asyncio
    async def test_run_task_command(self, task_manager):
        """Test running a task immediately"""
        job_id = "system_health_check"

        # Mock the task function
        mock_task_func = AsyncMock(return_value={"status": "success"})

        with patch.object(task_manager, 'execute_task', mock_task_func):
            # Mock user message
            mock_update = Mock()
            mock_update.message.from_user.id = 123456789
            mock_update.message.text = f"/run {job_id}"

            # Handle run command
            await task_manager.handle_run_command(mock_update)

            # Verify task was executed
            mock_task_func.assert_called_once_with(job_id)

            # Verify bot sent status message
            task_manager.bot.send_message.assert_called()
            call_args = task_manager.bot.send_message.call_args[1]

            assert "执行" in call_args["text"] and job_id in call_args["text"]

    @pytest.mark.asyncio
    async def test_unauthorized_access(self, task_manager):
        """Test handling of unauthorized users"""
        # Mock unauthorized user
        unauthorized_user_id = 999999999
        mock_update = Mock()
        mock_update.message.from_user.id = unauthorized_user_id
        mock_update.message.text = "/status"

        # Handle command from unauthorized user
        await task_manager.handle_status_command(mock_update)

        # Verify no response was sent (or access denied message)
        if task_manager.bot.send_message.called:
            call_args = task_manager.bot.send_message.call_args[1]
            assert "未授权" in call_args["text"] or "权限" in call_args["text"]

    @pytest.mark.asyncio
    async def test_error_handling(self, task_manager):
        """Test error handling in task commands"""
        # Configure scheduler to raise exception
        task_manager.scheduler.get_job.side_effect = Exception("Scheduler error")

        # Mock user message
        mock_update = Mock()
        mock_update.message.from_user.id = 123456789
        mock_update.message.text = "/detail nonexistent_job"

        # Handle command that will cause error
        await task_manager.handle_detail_command(mock_update)

        # Verify error message was sent
        task_manager.bot.send_message.assert_called_once()
        call_args = task_manager.bot.send_message.call_args[1]

        assert "错误" in call_args["text"] or "失败" in call_args["text"]

    @pytest.mark.asyncio
    async def test_bot_connection_management(self, task_manager):
        """Test bot connection management"""
        # Test initial connection
        assert task_manager.bot.is_connected()

        # Test connection failure handling
        task_manager.bot.configure_connected(False)

        # Mock user message
        mock_update = Mock()
        mock_update.message.from_user.id = 123456789
        mock_update.message.text = "/status"

        # Handle command with disconnected bot
        await task_manager.handle_status_command(mock_update)

        # Verify error handling for disconnected bot
        if task_manager.bot.send_message.called:
            call_args = task_manager.bot.send_message.call_args[1]
            assert "连接" in call_args["text"] or "离线" in call_args["text"]

    @pytest.mark.asyncio
    async def test_message_formatting(self, task_manager):
        """Test message formatting and markdown"""
        # Create complex status
        job1 = Mock()
        job1.id = "daily_data_update"
        job1.next_run_time = datetime.now() + timedelta(hours=1, minutes=30)
        job1.paused = False

        job2 = Mock()
        job2.id = "system_health_check"
        job2.next_run_time = datetime.now() + timedelta(minutes=45)
        job2.paused = True

        task_manager.scheduler.jobs = {
            "daily_data_update": job1,
            "system_health_check": job2
        }

        # Mock user message
        mock_update = Mock()
        mock_update.message.from_user.id = 123456789
        mock_update.message.text = "/status"

        # Handle status command
        await task_manager.handle_status_command(mock_update)

        # Verify message formatting
        task_manager.bot.send_message.assert_called_once()
        call_args = task_manager.bot.send_message.call_args[1]

        status_text = call_args["text"]

        # Check for formatted elements
        assert any(element in status_text for element in ["*", "_", "`"])  # Markdown formatting
        assert "daily_data_update" in status_text
        assert "system_health_check" in status_text

    def test_permission_validation(self, task_manager):
        """Test user permission validation"""
        # Test authorized user
        authorized_user = 123456789
        assert task_manager.is_authorized_user(authorized_user) is True

        # Test unauthorized user
        unauthorized_user = 999999999
        assert task_manager.is_authorized_user(unauthorized_user) is False

        # Test with empty authorized users list
        task_manager.authorized_users = []
        assert task_manager.is_authorized_user(authorized_user) is False

    @pytest.mark.asyncio
    async def test_configuration_hot_reload(self, task_manager):
        """Test configuration hot reload functionality"""
        # Mock new configuration
        new_config = ConfigFactory.create_test_config()
        new_config["scheduler_config"]["enabled"] = False

        with patch.object(task_manager, 'load_configuration', AsyncMock(return_value=new_config)):
            # Perform hot reload
            success = await task_manager.reload_configuration()

            # Verify configuration was updated
            assert success is True
            assert task_manager.config["scheduler_config"]["enabled"] is False

    @pytest.mark.asyncio
    async def test_task_scheduler_integration(self, task_manager):
        """Test integration with task scheduler"""
        # Test adding a new job
        job_id = "test_job"
        mock_func = AsyncMock()

        success = await task_manager.add_scheduled_job(
            job_id=job_id,
            func=mock_func,
            schedule="0 12 * * *",  # Daily at noon
            description="Test job"
        )

        # Verify job was added to scheduler
        assert success is True
        task_manager.scheduler.add_job.assert_called_once()

        # Test removing a job
        task_manager.scheduler.get_job.return_value = Mock()
        success = await task_manager.remove_scheduled_job(job_id)

        # Verify job was removed from scheduler
        assert success is True
        task_manager.scheduler.remove_job.assert_called_once_with(job_id)