import asyncio
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import Mock

from scheduler.dependencies import (
    JobDependencyConfig,
    SchedulerDependencyExecutor,
    parse_job_dependency_config,
    validate_job_dependencies,
)
import scheduler.tasks as task_module
from scheduler.scheduler import TaskScheduler


@dataclass
class FakeJobConfig:
    job_id: str
    parameters: dict[str, Any] = field(default_factory=dict)
    dependencies: JobDependencyConfig = field(default_factory=JobDependencyConfig)
    enabled: bool = True
    manual_only: bool = False
    max_instances: int = 1


def _config(job_id: str, raw_dependencies=None, **kwargs) -> FakeJobConfig:
    return FakeJobConfig(
        job_id=job_id,
        dependencies=parse_job_dependency_config(raw_dependencies),
        **kwargs,
    )


def test_dependency_config_validation_accepts_valid_parallel_post_success():
    configs = {
        "A": _config(
            "A",
            {
                "post_success": [
                    {
                        "group_id": "after_a",
                        "mode": "parallel",
                        "jobs": [
                            {"job_id": "B", "inherit": ["exchanges"], "failure_policy": "degrade_parent"},
                            {"job_id": "C", "parameters": {"dry_run": True}, "failure_policy": "ignore"},
                        ],
                    }
                ]
            },
        ),
        "B": _config("B"),
        "C": _config("C"),
    }

    assert validate_job_dependencies(configs) == []


def test_dependency_config_validation_reports_invalid_values():
    configs = {
        "A": _config(
            "A",
            {
                "post_success": [
                    {
                        "group_id": "dup",
                        "mode": "bad_mode",
                        "jobs": [
                            {
                                "job_id": "missing",
                                "inherit": "exchanges",
                                "timeout_seconds": 0,
                                "failure_policy": "bad_policy",
                            }
                        ],
                    },
                    {"group_id": "dup", "mode": "serial", "jobs": []},
                ]
            },
        )
    }

    errors = validate_job_dependencies(configs)

    assert any("invalid mode" in error for error in errors)
    assert any("unknown dependency job_id" in error for error in errors)
    assert any("invalid inherit list" in error for error in errors)
    assert any("timeout_seconds must be positive" in error for error in errors)
    assert any("invalid failure_policy" in error for error in errors)
    assert any("duplicate group_id" in error for error in errors)
    assert any("jobs must not be empty" in error for error in errors)


def test_dependency_cycle_detection_reports_cycle():
    configs = {
        "A": _config("A", {"post_success": [{"group_id": "a", "jobs": [{"job_id": "B"}]}]}),
        "B": _config("B", {"post_success": [{"group_id": "b", "jobs": [{"job_id": "A"}]}]}),
    }

    errors = validate_job_dependencies(configs)

    assert any("dependency cycle detected" in error for error in errors)


def test_successful_parent_triggers_parallel_post_dependencies():
    calls = []

    async def raw_runner(job_id, parameters, include_dependencies):
        calls.append((job_id, dict(parameters), include_dependencies))
        return True

    configs = {
        "A": _config(
            "A",
            {
                "post_success": [
                    {
                        "group_id": "after_a",
                        "mode": "parallel",
                        "jobs": [{"job_id": "B"}, {"job_id": "C"}],
                    }
                ]
            },
        ),
        "B": _config("B"),
        "C": _config("C"),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_runner, logger=Mock())

    result = asyncio.run(executor.run_job("A"))

    assert result["success"] is True
    assert {call[0] for call in calls} == {"A", "B", "C"}
    assert all(call[2] is False for call in calls)
    assert result["dependency_results"]["post_success"][0]["status"] == "success"


def test_failed_parent_skips_post_success_dependencies():
    calls = []

    async def raw_runner(job_id, parameters, include_dependencies):
        calls.append(job_id)
        return job_id != "A"

    configs = {
        "A": _config(
            "A",
            {"post_success": [{"group_id": "after_a", "jobs": [{"job_id": "B"}]}]},
        ),
        "B": _config("B"),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_runner, logger=Mock())

    result = asyncio.run(executor.run_job("A"))

    assert result["success"] is False
    assert calls == ["A"]
    assert result["dependency_results"]["post_success"] == []


def test_serial_dependency_failure_stops_following_nodes():
    calls = []

    async def raw_runner(job_id, parameters, include_dependencies):
        calls.append(job_id)
        return job_id != "B"

    configs = {
        "A": _config(
            "A",
            {
                "post_success": [
                    {
                        "group_id": "after_a",
                        "mode": "serial",
                        "jobs": [
                            {"job_id": "B", "failure_policy": "stop_chain"},
                            {"job_id": "C"},
                        ],
                    }
                ]
            },
        ),
        "B": _config("B"),
        "C": _config("C"),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_runner, logger=Mock())

    result = asyncio.run(executor.run_job("A"))

    assert calls == ["A", "B"]
    nodes = result["dependency_results"]["post_success"][0]["nodes"]
    assert nodes[1]["job_id"] == "C"
    assert nodes[1]["status"] == "skipped"
    assert result["status"] == "degraded"


def test_pre_success_dependency_controls_parent_start():
    async def raw_success(job_id, parameters, include_dependencies):
        return True

    configs = {
        "A": _config("A", {"pre_success": [{"group_id": "before_a", "jobs": [{"job_id": "B"}]}]}),
        "B": _config("B"),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_success, logger=Mock())

    success_result = asyncio.run(executor.run_job("A"))

    assert success_result["success"] is True
    assert success_result["parent_started"] is True

    calls = []

    async def raw_fail_pre(job_id, parameters, include_dependencies):
        calls.append(job_id)
        return job_id != "B"

    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_fail_pre, logger=Mock())
    failed_result = asyncio.run(executor.run_job("A"))

    assert failed_result["success"] is False
    assert failed_result["parent_started"] is False
    assert calls == ["B"]


def test_dependency_parameter_inheritance_and_node_overrides():
    seen = {}

    async def raw_runner(job_id, parameters, include_dependencies):
        seen[job_id] = dict(parameters)
        return True

    configs = {
        "A": _config(
            "A",
            {
                "post_success": [
                    {
                        "group_id": "after_a",
                        "jobs": [
                            {
                                "job_id": "B",
                                "inherit": ["exchanges", "dry_run"],
                                "parameters": {"dry_run": True, "limit": 5},
                            }
                        ],
                    }
                ]
            },
        ),
        "B": _config("B", parameters={"lookback_days": 14, "dry_run": False}),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_runner, logger=Mock())

    result = asyncio.run(executor.run_job("A", {"exchanges": ["SSE"], "dry_run": False}))

    assert result["success"] is True
    assert seen["B"] == {
        "lookback_days": 14,
        "dry_run": True,
        "exchanges": ["SSE"],
        "limit": 5,
    }
    node = result["dependency_results"]["post_success"][0]["nodes"][0]
    assert node["inherited_parameters"] == {"exchanges": ["SSE"], "dry_run": True}


def test_dependency_timeout_and_ignore_policy():
    async def raw_runner(job_id, parameters, include_dependencies):
        if job_id == "B":
            await asyncio.sleep(0.05)
        return True

    configs = {
        "A": _config(
            "A",
            {
                "post_success": [
                    {
                        "group_id": "after_a",
                        "jobs": [
                            {"job_id": "B", "timeout_seconds": 0.001, "failure_policy": "ignore"}
                        ],
                    }
                ]
            },
        ),
        "B": _config("B"),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_runner, logger=Mock())

    result = asyncio.run(executor.run_job("A"))

    node = result["dependency_results"]["post_success"][0]["nodes"][0]
    assert result["success"] is True
    assert node["status"] == "failed"
    assert node["failure_policy"] == "ignore"


def test_manual_only_dependency_node_can_be_triggered_by_dependency_executor():
    calls = []

    async def raw_runner(job_id, parameters, include_dependencies):
        calls.append(job_id)
        return True

    configs = {
        "A": _config("A", {"post_success": [{"group_id": "after_a", "jobs": [{"job_id": "B"}]}]}),
        "B": _config("B", manual_only=True),
    }
    executor = SchedulerDependencyExecutor(job_configs=configs, raw_job_runner=raw_runner, logger=Mock())

    result = asyncio.run(executor.run_job("A"))

    assert result["success"] is True
    assert calls == ["A", "B"]


def test_raw_configured_dependency_node_respects_running_task_max_instances(monkeypatch):
    scheduler = TaskScheduler()
    job_id = "dependency_probe_task"
    async_task = Mock()

    async def fake_task(job_config=None):
        async_task(job_config=job_config)
        return True

    monkeypatch.setattr(task_module.scheduled_tasks, job_id, fake_task, raising=False)
    old_job_configs = getattr(scheduler, "job_configs", None)
    old_running_tasks = getattr(scheduler, "running_tasks", None)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id, max_instances=1)}
        scheduler.running_tasks = {job_id: {"existing": object()}}

        skipped = asyncio.run(scheduler._run_configured_task_raw(job_id, {}, include_dependencies=False))

        assert skipped is False
        async_task.assert_not_called()

        scheduler.running_tasks = {}
        result = asyncio.run(scheduler._run_configured_task_raw(job_id, {}, include_dependencies=False))

        assert result is True
        async_task.assert_called_once()
        assert job_id not in scheduler.running_tasks
    finally:
        if old_job_configs is not None:
            scheduler.job_configs = old_job_configs
        if old_running_tasks is not None:
            scheduler.running_tasks = old_running_tasks


def test_raw_configured_task_strips_runtime_metadata(monkeypatch):
    scheduler = TaskScheduler()
    job_id = "metadata_probe_task"
    async_task = Mock()

    async def fake_task(*, dry_run=False, job_config=None):
        async_task(dry_run=dry_run, job_config=job_config)
        return True

    monkeypatch.setattr(task_module.scheduled_tasks, job_id, fake_task, raising=False)
    old_job_configs = getattr(scheduler, "job_configs", None)
    old_running_tasks = getattr(scheduler, "running_tasks", None)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id, max_instances=1)}
        scheduler.running_tasks = {}

        result = asyncio.run(
            scheduler._run_configured_task_raw(
                job_id,
                {
                    "dry_run": True,
                    "max_runtime_seconds": 60,
                    "note": "operator-facing description",
                },
                include_dependencies=False,
            )
        )

        assert result is True
        async_task.assert_called_once()
        assert async_task.call_args.kwargs["dry_run"] is True
        assert "note" not in async_task.call_args.kwargs
    finally:
        if old_job_configs is not None:
            scheduler.job_configs = old_job_configs
        if old_running_tasks is not None:
            scheduler.running_tasks = old_running_tasks
