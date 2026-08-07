import asyncio
from unittest.mock import ANY, AsyncMock, Mock

import scheduler.tasks as task_module
import research.business_profile_semantic_runtime as runtime_module
import utils.llm as llm_module
from data_manager import DataManager
from scheduler.job_config import JobConfig
from scheduler.scheduler import TaskScheduler
from scheduler.tasks import ScheduledTasks
from research.storage import ResearchStorageManager
from research.business_profile_backfill_control import (
    BusinessProfileBackfillControlStore,
)
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


def test_single_batch_progress_uses_nested_authoritative_queue_and_readiness():
    text = task_module._format_business_profile_backfill_progress(
        {
            "state": "completed",
            "run_id": "run-1",
            "queue_health": {},
            "rollout_readiness": {},
            "latest_result": {
                "enqueue": {"inserted": 3188},
                "workers": {"publish": {"completed": 0, "retried": 0}},
                "throughput": {"enqueued": 3188, "worker_completed": 0},
                "queue_health": {
                    "claimable": 3168,
                    "running": 0,
                    "terminal": 0,
                },
                "rollout_readiness": {
                    "current_annual_coverage_ratio": 0.25,
                    "phase_ready": False,
                },
            },
            "reason_codes": ["single_batch_complete"],
        }
    )

    assert "claimable=3168" in text
    assert "当前年报覆盖率: 25.00%" in text
    assert "本批: 入队3188，完整完成0" in text
    assert task_module._business_profile_completed_items(
        {
            "throughput": {"worker_completed": 0},
            "continuous_progress": {
                "cumulative_workers": {"publish": {"completed": 7}}
            },
        }
    ) == 7


def test_daily_incremental_job_is_disabled_and_not_scheduled(monkeypatch):
    raw = (
        UnifiedConfigManager("config")
        .get_scheduler_config()
        .jobs["business_profile_daily_incremental"]
    )
    assert raw["enabled"] is False
    assert raw["parameters"]["discovery_kwargs"]["max_pages_per_market"] == 240

    scheduler = TaskScheduler()
    scheduler.job_configs = {
        "business_profile_daily_incremental": JobConfig(
            job_id="business_profile_daily_incremental",
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


def test_only_manual_business_profile_backfill_is_enabled_during_bootstrap():
    jobs = UnifiedConfigManager("config").get_scheduler_config().jobs
    assert jobs["business_profile_daily_incremental"]["enabled"] is False
    assert jobs["business_profile_daily_incremental"]["trigger"]["month"] == "1-12"
    assert jobs["business_profile_backfill"]["enabled"] is True
    assert jobs["business_profile_backfill"]["manual_only"] is True
    assert jobs["business_profile_backfill"]["parameters"]["selection_policy"] == (
        "latest_annual_only"
    )
    assert jobs["business_profile_backfill"]["parameters"]["continuous"] is False
    assert jobs["business_profile_backfill"]["parameters"]["max_runtime_seconds"] is None
    assert jobs["business_profile_backfill_control"]["enabled"] is True
    assert jobs["business_profile_backfill_control"]["manual_only"] is True
    for legacy_job_id in (
        "business_profile_index_discovery_daily",
        "business_profile_semantic_maintenance",
        "business_profile_monthly_reconciliation",
        "business_profile_semiannual_freshness",
        "business_profile_annual_coverage_reconciliation",
    ):
        assert legacy_job_id not in jobs


def test_data_manager_disabled_semantic_module_has_no_side_effects():
    manager = DataManager.__new__(DataManager)
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {"semantic_production": {"enabled": False}}
        },
    )
    manager.research_storage = object()

    result = asyncio.run(manager.run_business_profile_semantic_production())
    assert result == {
        "status": "disabled",
        "reason": "business profile semantic production is disabled",
    }


def test_data_manager_enabled_plan_builds_scope_before_default_checkpoint(
    tmp_path, monkeypatch
):
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

    report = asyncio.run(
        manager.run_business_profile_semantic_production(
            mode="report",
            knowledge_cutoff="2026-08-01",
            instrument_ids=["601088.SH"],
            field_families=["derived_value_chain_roles"],
            runtime_identities={"rules": "rules.v1", "policy": "policy.v1"},
        )
    )
    assert report["pipeline_status"] == "partial"
    assert report["completed_stages"] == ["plan"]
    assert len(list((tmp_path / "checkpoints").glob("*.json"))) == 1

    resolver_builder = Mock(side_effect=AssertionError("report mode must be read-only"))
    monkeypatch.setattr(
        runtime_module,
        "build_business_profile_counterparty_resolver",
        resolver_builder,
    )
    named_report = asyncio.run(
        manager.run_business_profile_semantic_production(
            mode="report",
            knowledge_cutoff="2026-08-01",
            instrument_ids=["601088.SH"],
            field_families=["named_relationships"],
            runtime_identities={"rules": "rules.v1"},
        )
    )
    assert named_report["status"] == "not_ready"
    resolver_builder.assert_not_called()


def test_empty_semantic_config_does_not_create_checkpoint_or_rotation(tmp_path):
    research_config = ResearchConfig(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "semantic_production": {
                    "enabled": True,
                    "promotion_enabled": False,
                    "checkpoint_root": str(tmp_path / "checkpoints"),
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

    result = asyncio.run(manager.run_business_profile_semantic_production())

    assert result["status"] == "not_ready"
    assert not (tmp_path / "checkpoints").exists()
    with storage.get_connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_operation_state"
            ).fetchone()[0]
            == 0
        )


def test_unchanged_complete_scope_builds_no_pdf_acquirer_or_llm_client(
    tmp_path, monkeypatch
):
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
                        "network_calls": False,
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
    monkeypatch.setattr(
        runtime_module,
        "discover_business_profile_semantic_scope",
        Mock(return_value=()),
    )
    acquirer_builder = Mock(
        side_effect=AssertionError("PDF acquisition is unnecessary")
    )
    monkeypatch.setattr(
        runtime_module,
        "build_business_profile_planned_disclosure_acquirer",
        acquirer_builder,
    )
    llm_client = Mock(side_effect=AssertionError("LLM construction is unnecessary"))
    monkeypatch.setattr(llm_module, "LlmClient", llm_client)

    result = asyncio.run(
        manager.run_business_profile_semantic_production(
            field_families=["structured_segments"],
            runtime_identities={"parser": "parser.v1"},
        )
    )

    assert result["status"] == "unchanged"
    acquirer_builder.assert_not_called()
    llm_client.assert_not_called()


def test_scheduler_forwards_daily_async_scope(monkeypatch):
    task = _task()
    manager = Mock()
    manager.run_business_profile_daily_incremental = AsyncMock(
        return_value={
            "status": "success",
            "enqueue": {"inserted": 2},
            "elapsed_seconds": 0.01,
        }
    )
    monkeypatch.setattr(task_module, "data_manager", manager)

    result = asyncio.run(
        task.business_profile_daily_incremental(
            knowledge_cutoff="2026-08-01",
            exchanges=["SSE"],
            field_families=["atomic_activities"],
            runtime_identities={"model": "model.v1"},
            max_attempts=4,
            discovery_kwargs={"lookback_days": 7},
            stage_budgets={"semantic": {"max_items": 2}},
        )
    )

    assert result is True
    manager.run_business_profile_daily_incremental.assert_awaited_once_with(
        knowledge_cutoff="2026-08-01",
        exchanges=["SSE"],
        field_families=["atomic_activities"],
        runtime_identities={"model": "model.v1"},
        max_attempts=4,
        discovery_kwargs={"lookback_days": 7},
        stage_budgets={"semantic": {"max_items": 2}},
    )
    assert "business_profile_daily_incremental" not in task._active_tasks
    report = task._send_task_report.await_args.kwargs["report_data"]
    assert report["business_profile_async_production"]["status"] == "success"


def test_scheduler_forwards_manual_backfill_scope(tmp_path, monkeypatch):
    log_info = Mock()
    monkeypatch.setattr(task_module.scheduler_logger, "info", log_info)
    task = _task()
    manager = Mock()
    manager.run_business_profile_backfill = AsyncMock(
        return_value={"status": "success", "enqueue": {"inserted": 1}}
    )
    monkeypatch.setattr(task_module, "data_manager", manager)
    monkeypatch.setattr(
        task_module,
        "_business_profile_backfill_control_store",
        lambda: BusinessProfileBackfillControlStore(tmp_path / "checkpoints"),
    )

    assert asyncio.run(
        task.business_profile_backfill(
            knowledge_cutoff="2026-08-01",
            rollout_phase="structured_shadow",
            selection_policy="expanded",
            instrument_ids=["601088.SH"],
            start_date="2025-01-01",
            end_date="2026-08-01",
            document_types=["resource_report"],
            field_families=["commodity_exposure_facts"],
            runtime_identities={"rules": "v1"},
            force=True,
            max_attempts=4,
            stage_budgets={"acquire": {"max_items": 2}},
        )
    )
    manager.run_business_profile_backfill.assert_awaited_once_with(
        knowledge_cutoff="2026-08-01",
        rollout_phase="structured_shadow",
        selection_policy="expanded",
        instrument_ids=["601088.SH"],
        start_date="2025-01-01",
        end_date="2026-08-01",
        document_types=["resource_report"],
        field_families=["commodity_exposure_facts"],
        runtime_identities={"rules": "v1"},
        force=True,
        max_attempts=4,
        stage_budgets={"acquire": {"max_items": 2}},
        should_stop=ANY,
    )
    assert (
        task._send_task_report.await_args.kwargs["report_data"]["tasks_completed"] == 0
    )
    formats = [str(call.args[0]) for call in log_info.call_args_list]
    assert any("Business-profile backfill start" in message for message in formats)
    assert any("Business-profile backfill end" in message for message in formats)
    assert (
        "N/A" not in task._send_task_report.await_args.kwargs["report_data"]["duration"]
    )


def test_scheduler_continuous_backfill_runs_until_phase_ready(tmp_path, monkeypatch):
    task = _task()
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    manager = Mock()
    manager.run_business_profile_backfill = AsyncMock(
        side_effect=[
            {
                "status": "success",
                "enqueue": {"inserted": 1},
                "workers": {"acquire": {"completed": 1}},
                "queue_health": {"claimable": 1, "running": 0, "terminal": 0},
                "rollout_readiness": {
                    "phase_ready": False,
                    "phase_reason_codes": ["claimable_work_remaining"],
                },
            },
            {
                "status": "success",
                "enqueue": {"inserted": 0},
                "workers": {"acquire": {"completed": 1}},
                "queue_health": {"claimable": 0, "running": 0, "terminal": 0},
                "rollout_readiness": {
                    "phase_ready": True,
                    "phase_reason_codes": [],
                },
            },
        ]
    )
    monkeypatch.setattr(task_module, "data_manager", manager)
    monkeypatch.setattr(
        task_module,
        "_business_profile_backfill_control_store",
        lambda: store,
    )

    success = asyncio.run(
        task.business_profile_backfill(
            continuous=True,
            continuous_poll_seconds=0,
            heartbeat_interval_seconds=1,
        )
    )

    assert success is True
    assert manager.run_business_profile_backfill.await_count == 2
    assert all(
        callable(call.kwargs["should_stop"])
        for call in manager.run_business_profile_backfill.await_args_list
    )
    progress = store.status()
    assert progress["state"] == "completed"
    assert progress["cycle"] == 2
    assert progress["cumulative_workers"]["acquire"]["completed"] == 2


def test_scheduler_rejects_unsafe_continuous_scope_without_active_task(monkeypatch):
    task = _task()
    store_factory = Mock()
    monkeypatch.setattr(
        task_module,
        "_business_profile_backfill_control_store",
        store_factory,
    )

    assert asyncio.run(
        task.business_profile_backfill(continuous=True, force=True)
    ) is False
    assert "business_profile_backfill" not in task._active_tasks
    store_factory.assert_not_called()


def test_scheduler_backfill_control_requests_stop(tmp_path, monkeypatch):
    task = _task()
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    store.begin(
        run_id="active-run",
        mode="continuous",
        phase="structured_shadow",
        parameters={"continuous": True},
    )
    monkeypatch.setattr(
        task_module,
        "_business_profile_backfill_control_store",
        lambda: store,
    )

    assert asyncio.run(
        task.business_profile_backfill_control(action="stop", reason="test")
    )
    assert store.status()["state"] == "stop_requested"
    assert store.should_stop("active-run")["reason"] == "test"
