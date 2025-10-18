"""
Unit tests for task scheduler
"""

import pytest
import asyncio
from datetime import datetime, date, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from scheduler.scheduler import TaskScheduler
from scheduler.tasks import TaskManager
from scheduler.job_config import JobConfig
from utils.exceptions import SchedulerError


@pytest.mark.unit
class TestTaskScheduler:
    """Test cases for TaskScheduler class"""

    @pytest.fixture
    def scheduler_config(self):
        """Configuration for task scheduler"""
        return {
            "scheduler": {
                "timezone": "Asia/Shanghai",
                "max_workers": 3,
                "job_defaults": {
                    "coalesce": True,
                    "max_instances": 1,
                    "misfire_grace_time": 300
                },
                "executors": {
                    "default": {
                        "type": "threadpool",
                        "max_workers": 3
                    }
                }
            }
        }

    @pytest.fixture
    async def task_scheduler(self, scheduler_config):
        """Create TaskScheduler instance for testing"""
        with patch('scheduler.scheduler.config_manager.get', return_value=scheduler_config):
            scheduler = TaskScheduler()
            await scheduler.initialize()
            return scheduler

    @pytest.mark.asyncio
    async def test_initialize(self, task_scheduler):
        """Test scheduler initialization"""
        assert task_scheduler is not None
        assert task_scheduler.scheduler is not None
        assert task_scheduler.scheduler.state == 1  # STATE_RUNNING

    @pytest.mark.asyncio
    async def test_start_scheduler(self, task_scheduler):
        """Test starting scheduler"""
        # Stop first
        await task_scheduler.stop()
        assert task_scheduler.scheduler.state == 0  # STATE_STOPPED

        # Start again
        await task_scheduler.start()
        assert task_scheduler.scheduler.state == 1  # STATE_RUNNING

    @pytest.mark.asyncio
    async def test_stop_scheduler(self, task_scheduler):
        """Test stopping scheduler"""
        await task_scheduler.stop()
        assert task_scheduler.scheduler.state == 0  # STATE_STOPPED

    @pytest.mark.asyncio
    async def test_add_cron_job(self, task_scheduler):
        """Test adding cron job"""
        mock_task = AsyncMock()
        job_id = "test_cron_job"

        # Add job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * 1-5",  # Weekdays at 16:00
            description="Test cron job"
        )

        # Verify job was added
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None
        assert isinstance(job.trigger, CronTrigger)

    @pytest.mark.asyncio
    async def test_add_date_job(self, task_scheduler):
        """Test adding date job"""
        mock_task = AsyncMock()
        job_id = "test_date_job"
        run_date = datetime.now() + timedelta(hours=1)

        # Add job
        await task_scheduler.add_date_job(
            func=mock_task,
            job_id=job_id,
            run_date=run_date,
            description="Test date job"
        )

        # Verify job was added
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None
        assert isinstance(job.trigger, DateTrigger)

    @pytest.mark.asyncio
    async def test_add_interval_job(self, task_scheduler):
        """Test adding interval job"""
        mock_task = AsyncMock()
        job_id = "test_interval_job"

        # Add job
        await task_scheduler.add_interval_job(
            func=mock_task,
            job_id=job_id,
            hours=1,
            description="Test interval job"
        )

        # Verify job was added
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None

    @pytest.mark.asyncio
    async def test_remove_job(self, task_scheduler):
        """Test removing job"""
        mock_task = AsyncMock()
        job_id = "test_remove_job"

        # Add job first
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Test job to remove"
        )

        # Verify job exists
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None

        # Remove job
        await task_scheduler.remove_job(job_id)

        # Verify job was removed
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is None

    @pytest.mark.asyncio
    async def test_pause_job(self, task_scheduler):
        """Test pausing job"""
        mock_task = AsyncMock()
        job_id = "test_pause_job"

        # Add job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Test job to pause"
        )

        # Pause job
        await task_scheduler.pause_job(job_id)

        # Verify job is paused
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None
        # Note: APScheduler doesn't expose pause state directly
        # This would require custom implementation

    @pytest.mark.asyncio
    async def test_resume_job(self, task_scheduler):
        """Test resuming job"""
        mock_task = AsyncMock()
        job_id = "test_resume_job"

        # Add job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Test job to resume"
        )

        # Pause and resume job
        await task_scheduler.pause_job(job_id)
        await task_scheduler.resume_job(job_id)

        # Verify job still exists
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None

    @pytest.mark.asyncio
    async def test_get_job_list(self, task_scheduler):
        """Test getting job list"""
        mock_task = AsyncMock()

        # Add multiple jobs
        for i in range(3):
            await task_scheduler.add_cron_job(
                func=mock_task,
                job_id=f"test_job_{i}",
                cron_expression="0 16 * * *",
                description=f"Test job {i}"
            )

        # Get job list
        jobs = await task_scheduler.get_job_list()

        assert isinstance(jobs, list)
        assert len(jobs) >= 3

        # Check job structure
        for job in jobs:
            assert "id" in job
            assert "name" in job
            assert "next_run_time" in job
            assert "trigger" in job

    @pytest.mark.asyncio
    async def test_get_job_info(self, task_scheduler):
        """Test getting job information"""
        mock_task = AsyncMock()
        job_id = "test_info_job"

        # Add job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * 1-5",
            description="Test job info"
        )

        # Get job info
        job_info = await task_scheduler.get_job_info(job_id)

        assert job_info is not None
        assert job_info["id"] == job_id
        assert job_info["description"] == "Test job info"
        assert "trigger" in job_info
        assert "next_run_time" in job_info

    @pytest.mark.asyncio
    async def test_modify_job(self, task_scheduler):
        """Test modifying job"""
        mock_task = AsyncMock()
        job_id = "test_modify_job"

        # Add job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Original description"
        )

        # Modify job
        await task_scheduler.modify_job(
            job_id=job_id,
            cron_expression="0 17 * * *",  # Change to 17:00
            description="Modified description"
        )

        # Verify job was modified
        job_info = await task_scheduler.get_job_info(job_id)
        assert job_info["description"] == "Modified description"

    @pytest.mark.asyncio
    async def test_run_job_now(self, task_scheduler):
        """Test running job immediately"""
        mock_task = AsyncMock()
        job_id = "test_run_now_job"

        # Add job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Test run now"
        )

        # Run job now
        await task_scheduler.run_job_now(job_id)

        # Wait a bit for job to execute
        await asyncio.sleep(0.1)

        # Verify task was called
        mock_task.assert_called()

    @pytest.mark.asyncio
    async def test_scheduler_health_check(self, task_scheduler):
        """Test scheduler health check"""
        health = await task_scheduler.health_check()

        assert isinstance(health, dict)
        assert "status" in health
        assert "running_jobs" in health
        assert "uptime" in health
        assert health["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_scheduler_error_handling(self, task_scheduler):
        """Test scheduler error handling"""
        # Test with invalid cron expression
        mock_task = AsyncMock()

        with pytest.raises(SchedulerError):
            await task_scheduler.add_cron_job(
                func=mock_task,
                job_id="invalid_cron",
                cron_expression="invalid_expression",
                description="Invalid cron"
            )

    @pytest.mark.asyncio
    async def test_duplicate_job_id(self, task_scheduler):
        """Test handling duplicate job IDs"""
        mock_task = AsyncMock()
        job_id = "duplicate_job"

        # Add first job
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="First job"
        )

        # Try to add duplicate
        with pytest.raises(SchedulerError):
            await task_scheduler.add_cron_job(
                func=mock_task,
                job_id=job_id,
                cron_expression="0 17 * * *",
                description="Duplicate job"
            )

    @pytest.mark.asyncio
    async def test_job_execution_with_exception(self, task_scheduler):
        """Test job execution with exception"""
        # Task that raises exception
        failing_task = AsyncMock(side_effect=Exception("Task failed"))

        job_id = "failing_job"

        # Add failing job
        await task_scheduler.add_cron_job(
            func=failing_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Failing job"
        )

        # Run job now
        await task_scheduler.run_job_now(job_id)

        # Wait a bit for job to execute
        await asyncio.sleep(0.1)

        # Verify task was called despite exception
        failing_task.assert_called()

    @pytest.mark.asyncio
    async def test_scheduler_shutdown_cleanup(self, task_scheduler):
        """Test scheduler shutdown cleanup"""
        # Add some jobs
        mock_task = AsyncMock()
        for i in range(3):
            await task_scheduler.add_cron_job(
                func=mock_task,
                job_id=f"cleanup_test_{i}",
                cron_expression="0 16 * * *",
                description=f"Cleanup test {i}"
            )

        # Shutdown scheduler
        await task_scheduler.shutdown()

        # Verify scheduler is stopped
        assert task_scheduler.scheduler.state == 0  # STATE_STOPPED

    @pytest.mark.asyncio
    async def test_timezone_handling(self, task_scheduler):
        """Test timezone handling"""
        mock_task = AsyncMock()
        job_id = "timezone_test"

        # Add job with specific timezone
        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            timezone="Asia/Shanghai",
            description="Timezone test"
        )

        # Verify job was added with timezone
        job = task_scheduler.scheduler.get_job(job_id)
        assert job is not None

    @pytest.mark.asyncio
    async def test_job_dependencies(self, task_scheduler):
        """Test job dependencies"""
        # Test if scheduler supports job dependencies
        mock_task1 = AsyncMock()
        mock_task2 = AsyncMock()

        # Add first job
        await task_scheduler.add_cron_job(
            func=mock_task1,
            job_id="dependency_test_1",
            cron_expression="0 16 * * *",
            description="First job"
        )

        # Add second job that depends on first
        # This depends on implementation
        await task_scheduler.add_cron_job(
            func=mock_task2,
            job_id="dependency_test_2",
            cron_expression="0 16 * * *",
            description="Dependent job"
        )

    @pytest.mark.asyncio
    async def test_job_persistence(self, task_scheduler):
        """Test job persistence across restarts"""
        # This would test if jobs are persisted and can be recovered
        # Depends on job store configuration
        mock_task = AsyncMock()
        job_id = "persistence_test"

        await task_scheduler.add_cron_job(
            func=mock_task,
            job_id=job_id,
            cron_expression="0 16 * * *",
            description="Persistence test"
        )

        # Simulate restart
        await task_scheduler.stop()
        await task_scheduler.start()

        # Check if job still exists
        job = task_scheduler.scheduler.get_job(job_id)
        # This depends on whether job persistence is configured