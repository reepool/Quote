import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from data_manager import DataManager
from scheduler.job_config import JobConfig
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


def test_semantic_maintenance_job_is_disabled_and_not_scheduled(monkeypatch):
    raw = UnifiedConfigManager("config").get_scheduler_config().jobs[
        "business_profile_semantic_maintenance"
    ]
    assert raw["enabled"] is False
    assert raw["parameters"]["mode"] == "resume"
    assert raw["parameters"]["instrument_ids"] == []

    scheduler = TaskScheduler()
    scheduler.job_configs = {
        "business_profile_semantic_maintenance": JobConfig(
            job_id="business_profile_semantic_maintenance",
            enabled=False,
            manual_only=False,
            description=raw["description"],
            trigger=Mock(),
            max_instances=1,
            misfire_grace_time=1800,
            coalesce=True,
            parameters=raw["parameters"],
        )
    }
    add_job = AsyncMock()
    monkeypatch.setattr(scheduler, "_add_job_from_config", add_job)
    asyncio.run(scheduler._setup_jobs_from_config())
    add_job.assert_not_awaited()


def test_data_manager_disabled_semantic_module_has_no_side_effects():
    manager = DataManager.__new__(DataManager)
    manager.research_config = Mock(
        enabled=True,
        modules={"business_profile_evidence": {"semantic_production": {"enabled": False}}},
    )
    manager.research_storage = object()

    result = asyncio.run(manager.run_business_profile_semantic_production())
    assert result == {
        "status": "disabled",
        "reason": "business profile semantic production is disabled",
    }


def test_scheduler_forwards_exact_scope_and_reports_unchanged(monkeypatch):
    task = _task()
    manager = Mock()
    manager.run_business_profile_semantic_production = AsyncMock(
        return_value={
            "status": "unchanged",
            "completed_stages": ["plan", "select", "extract", "verify", "promote"],
            "metrics": {"reused_results": 1, "elapsed_seconds": 0.01},
        }
    )
    monkeypatch.setattr(task_module, "data_manager", manager)

    result = asyncio.run(
        task.business_profile_semantic_maintenance(
            mode="resume",
            knowledge_cutoff="2026-08-01",
            instrument_ids=["601088.SH"],
            field_families=["atomic_activities"],
            runtime_identities={"model": "model.v1"},
            promotion_manifest_hashes={"atomic_activities": "manifest"},
            checkpoint_path="data/checkpoints/test.json",
            stage_payload={},
        )
    )

    assert result is True
    manager.run_business_profile_semantic_production.assert_awaited_once_with(
        mode="resume",
        knowledge_cutoff="2026-08-01",
        instrument_ids=["601088.SH"],
        field_families=["atomic_activities"],
        runtime_identities={"model": "model.v1"},
        promotion_manifest_hashes={"atomic_activities": "manifest"},
        checkpoint_path="data/checkpoints/test.json",
        stage_payload={},
    )
    assert "business_profile_semantic_maintenance" not in task._active_tasks
    report = task._send_task_report.await_args.kwargs["report_data"]
    assert report["business_profile_semantic_production"]["status"] == "unchanged"
