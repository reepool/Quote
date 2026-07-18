import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import scheduler.tasks as task_module
from data_manager import DataManager
from research.business_profile_corpus import FIRST_WAVE_INDUSTRY_GROUPS
from scheduler.job_config import JobConfig, JobConfigManager
from scheduler.scheduler import TaskScheduler
from scheduler.tasks import ScheduledTasks
from utils.config_manager import UnifiedConfigManager


def _task():
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()
    return task


def test_structured_business_profile_scheduler_preserves_degraded_result(
    monkeypatch,
):
    task = _task()
    manager = Mock()
    manager.run_business_profile_structured_sync = AsyncMock(
        return_value={
            "status": "degraded",
            "attempted_instruments": 1,
            "elapsed_seconds": 1.25,
            "sources": {
                "eastmoney_main_composition": {
                    "success_count": 1,
                    "empty_count": 0,
                    "failed_count": 0,
                },
                "ths_main_business_intro": {
                    "success_count": 0,
                    "empty_count": 0,
                    "failed_count": 1,
                },
            },
        }
    )
    monkeypatch.setattr(task_module, "data_manager", manager)

    result = asyncio.run(
        task.business_profile_structured_sync(
            industry_groups=["coal"],
            max_instruments=5,
            max_elapsed_seconds=120,
            candidate_write=True,
            operator_switch="BUSINESS_PROFILE_CANDIDATE_WRITE",
            resume=True,
        )
    )

    assert result is True
    manager.run_business_profile_structured_sync.assert_awaited_once_with(
        as_of_date=None,
        sources=None,
        industry_groups=["coal"],
        instrument_ids=None,
        max_instruments=5,
        max_elapsed_seconds=120,
        candidate_write=True,
        operator_switch="BUSINESS_PROFILE_CANDIDATE_WRITE",
        checkpoint_path=None,
        resume=True,
    )
    report = task._send_task_report.await_args.kwargs["report_data"]
    assert report["business_profile_sync"]["status"] == "degraded"
    assert "business_profile_structured_sync" not in task._active_tasks


def test_structured_business_profile_scheduler_reports_unchanged_success(
    monkeypatch,
):
    task = _task()
    manager = Mock()
    manager.run_business_profile_structured_sync = AsyncMock(
        return_value={
            "status": "success",
            "attempted_instruments": 2,
            "elapsed_seconds": 0.5,
            "candidate_evidence_written": 0,
            "candidate_segments_written": 0,
            "sources": {
                "eastmoney_main_composition": {
                    "success_count": 2,
                    "empty_count": 0,
                    "failed_count": 0,
                    "payload_unchanged_count": 2,
                }
            },
        }
    )
    monkeypatch.setattr(task_module, "data_manager", manager)

    result = asyncio.run(task.business_profile_structured_sync())

    assert result is True
    report = task._send_task_report.await_args.kwargs["report_data"]
    assert report["tasks_completed"] == 2
    assert report["business_profile_sync"]["candidate_segments_written"] == 0


def test_structured_business_profile_scheduler_forwards_resume_after_interruption(
    monkeypatch,
):
    task = _task()
    manager = Mock()
    manager.run_business_profile_structured_sync = AsyncMock(
        return_value={
            "status": "interrupted",
            "stopped_reason": "max_elapsed_seconds",
            "attempted_instruments": 1,
            "elapsed_seconds": 60.0,
            "sources": {},
        }
    )
    monkeypatch.setattr(task_module, "data_manager", manager)

    result = asyncio.run(
        task.business_profile_structured_sync(
            checkpoint_path="data/checkpoints/business_profile.json",
            resume=True,
        )
    )

    assert result is False
    call = manager.run_business_profile_structured_sync.await_args.kwargs
    assert call["checkpoint_path"] == ("data/checkpoints/business_profile.json")
    assert call["resume"] is True
    assert "business_profile_structured_sync" not in task._active_tasks


def test_structured_business_profile_job_is_disabled_and_not_scheduled(
    monkeypatch,
):
    scheduler_config = UnifiedConfigManager("config").get_scheduler_config()
    raw_job = scheduler_config.jobs["business_profile_structured_sync"]
    assert raw_job["enabled"] is False
    assert set(raw_job["parameters"]["industry_groups"]) == set(
        FIRST_WAVE_INDUSTRY_GROUPS
    )

    task_scheduler = TaskScheduler()
    job_configs = {
        "business_profile_structured_sync": JobConfig(
            job_id="business_profile_structured_sync",
            enabled=False,
            manual_only=False,
            description=raw_job["description"],
            trigger=Mock(),
            max_instances=1,
            misfire_grace_time=1800,
            coalesce=True,
            parameters=raw_job["parameters"],
        ),
    }
    add_job = AsyncMock()
    monkeypatch.setattr(task_scheduler, "job_configs", job_configs, raising=False)
    monkeypatch.setattr(task_scheduler, "_add_job_from_config", add_job)

    asyncio.run(task_scheduler._setup_jobs_from_config())

    add_job.assert_not_awaited()


def test_data_manager_disabled_module_short_circuits_before_provider_creation():
    manager = DataManager.__new__(DataManager)
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": False,
                "free_structured_sources": {"enabled": True},
            }
        },
    )
    manager.research_storage = object()

    result = asyncio.run(manager.run_business_profile_structured_sync())

    assert result == {
        "status": "disabled",
        "reason": "research business_profile_evidence module is disabled",
    }


def test_structured_business_profile_weekly_trigger_is_not_due_immediately():
    config_manager = UnifiedConfigManager("config")
    manager = task_module.config_manager
    raw_job = config_manager.get_scheduler_config().jobs[
        "business_profile_structured_sync"
    ]
    parsed = JobConfigManager(manager)._parse_job_config(
        "business_profile_structured_sync",
        raw_job,
        config_manager.get_scheduler_config(),
    )

    now = datetime(2026, 7, 18, 9, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    next_fire = parsed.trigger.get_next_fire_time(None, now)

    assert next_fire > now
