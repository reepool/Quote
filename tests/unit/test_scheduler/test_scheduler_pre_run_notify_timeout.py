"""pre_run_notify 超时保护的行为验证。

背景：2026-10-02 港股日更 17:30 触发后被阻塞式开场 Telegram 通知
卡到 19:39 才开始干活。通知只是伴随信息，不允许阻塞任务主体。
"""

import asyncio
import time
from dataclasses import dataclass, field
from unittest.mock import Mock

import pytest

import scheduler.scheduler as scheduler_module
from scheduler.scheduler import TaskScheduler


@dataclass
class FakeJobConfig:
    job_id: str
    description: str = "探测任务"
    parameters: dict = field(default_factory=dict)
    pre_run_notify: bool = False
    report: bool = False
    enabled: bool = True
    manual_only: bool = False
    max_instances: int = 1


class HangingTelegramBot:
    async def send_task_notification(self, message, task_name=None, level="info"):
        await asyncio.sleep(30)
        return True


class FailingTelegramBot:
    async def send_task_notification(self, message, task_name=None, level="info"):
        raise RuntimeError("telegram unavailable")


class FastTelegramBot:
    def __init__(self):
        self.sent = []

    async def send_task_notification(self, message, task_name=None, level="info"):
        self.sent.append((task_name, message))
        return True


class _StubConfigManager:
    def __init__(self, notify_timeout_seconds):
        self._notify_timeout_seconds = notify_timeout_seconds

    def get_nested(self, path, default=None):
        assert path == "api_config.report_send_timeout_seconds"
        return self._notify_timeout_seconds


@pytest.fixture
def isolated_scheduler(monkeypatch):
    scheduler = TaskScheduler()
    monkeypatch.setattr(
        scheduler_module,
        "config_manager",
        _StubConfigManager(notify_timeout_seconds=1),
    )
    saved_executor = getattr(scheduler, "dependency_executor", None)
    scheduler.dependency_executor = None
    try:
        yield scheduler
    finally:
        scheduler.dependency_executor = saved_executor
        scheduler.running_tasks.pop("probe_pre_run_notify", None)


def _run_parameterized_task(scheduler, job_config):
    ran = []

    async def raw_runner(job_id, parameters, include_dependencies):
        ran.append((job_id, dict(parameters)))
        return True

    from scheduler.dependencies import SchedulerDependencyExecutor

    scheduler.dependency_executor = SchedulerDependencyExecutor(
        job_configs={},
        raw_job_runner=raw_runner,
        logger=Mock(),
    )
    task_func = scheduler._create_parameterized_task(
        Mock(return_value=None), job_config.parameters, job_config
    )
    result = asyncio.run(task_func())
    return result, ran


def test_hanging_pre_run_notify_times_out_and_task_proceeds(
    isolated_scheduler, monkeypatch
):
    monkeypatch.setattr(scheduler_module, "TelegramBot", HangingTelegramBot)
    job_config = FakeJobConfig(job_id="probe_pre_run_notify", pre_run_notify=True)

    started = time.monotonic()
    result, ran = _run_parameterized_task(isolated_scheduler, job_config)
    elapsed = time.monotonic() - started

    assert result is True
    assert ran and ran[0][0] == "probe_pre_run_notify"
    assert elapsed < 5


def test_failing_pre_run_notify_does_not_block_task(isolated_scheduler, monkeypatch):
    monkeypatch.setattr(scheduler_module, "TelegramBot", FailingTelegramBot)
    job_config = FakeJobConfig(job_id="probe_pre_run_notify", pre_run_notify=True)

    result, ran = _run_parameterized_task(isolated_scheduler, job_config)

    assert result is True
    assert ran and ran[0][0] == "probe_pre_run_notify"


def test_successful_pre_run_notify_still_runs_task(isolated_scheduler, monkeypatch):
    fast_bot = FastTelegramBot()
    monkeypatch.setattr(scheduler_module, "TelegramBot", lambda: fast_bot)
    job_config = FakeJobConfig(
        job_id="probe_pre_run_notify", pre_run_notify=True, description="快速任务"
    )

    result, ran = _run_parameterized_task(isolated_scheduler, job_config)

    assert result is True
    assert ran and ran[0][0] == "probe_pre_run_notify"
    assert fast_bot.sent and fast_bot.sent[0][0] == "probe_pre_run_notify"


def test_pre_run_notify_disabled_skips_notification(isolated_scheduler, monkeypatch):
    monkeypatch.setattr(
        scheduler_module,
        "TelegramBot",
        lambda: pytest.fail("TelegramBot 不应在 pre_run_notify 关闭时被构造"),
    )
    job_config = FakeJobConfig(job_id="probe_pre_run_notify", pre_run_notify=False)

    result, ran = _run_parameterized_task(isolated_scheduler, job_config)

    assert result is True
    assert ran and ran[0][0] == "probe_pre_run_notify"
