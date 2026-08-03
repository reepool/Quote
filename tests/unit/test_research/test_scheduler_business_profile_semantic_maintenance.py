import asyncio
from unittest.mock import AsyncMock, Mock

import scheduler.tasks as task_module
from data_manager import DataManager
from scheduler.job_config import JobConfig
from scheduler.scheduler import TaskScheduler
from scheduler.tasks import ScheduledTasks
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
    UnifiedConfigManager,
)


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


def test_data_manager_enabled_plan_builds_scope_before_default_checkpoint(tmp_path):
    research_config = ResearchConfig(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "semantic_production": {
                    "enabled": True,
                    "promotion_enabled": False,
                    "checkpoint_root": str(tmp_path / "checkpoints"),
                    "kill_switches": {
                        "all_writes": False,
                        "network_calls": True,
                        "promotion": False,
                        "scope_widening": False,
                    },
                }
            }
        },
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    manager = DataManager.__new__(DataManager)
    manager.research_config = research_config
    manager.research_storage = storage

    result = asyncio.run(
        manager.run_business_profile_semantic_production(
            mode="plan",
            knowledge_cutoff="2026-08-01",
            instrument_ids=["601088.SH"],
            field_families=["derived_value_chain_roles"],
            runtime_identities={"rules": "rules.v1", "policy": "policy.v1"},
        )
    )

    assert result["status"] == "success"
    assert result["completed_stages"] == ["plan"]
    assert len(list((tmp_path / "checkpoints").glob("*.json"))) == 1


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
            promotion_manifests={},
            max_instruments=17,
            checkpoint_path="data/checkpoints/test.json",
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
        promotion_manifests={},
        max_instruments=17,
        checkpoint_path="data/checkpoints/test.json",
    )
    assert "business_profile_semantic_maintenance" not in task._active_tasks
    report = task._send_task_report.await_args.kwargs["report_data"]
    assert report["business_profile_semantic_production"]["status"] == "unchanged"
