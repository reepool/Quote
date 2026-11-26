import json

import pytest

import scheduler.tasks as scheduler_tasks
from scheduler.tasks import ScheduledTasks
from utils import market_dependency_versions as versions


def test_check_market_dependency_versions_detects_updates(monkeypatch):
    monkeypatch.setattr(
        versions,
        "_get_installed_version",
        lambda distribution: {"baostock": "0.8.9", "akshare": "1.17.57"}[distribution],
    )
    monkeypatch.setattr(
        versions,
        "_fetch_pypi_metadata",
        lambda distribution, **_: {
            "baostock": {"version": "0.9.1", "package_url": "https://pypi.org/project/baostock/"},
            "akshare": {"version": "1.17.57", "package_url": "https://pypi.org/project/akshare/"},
        }[distribution],
    )

    result = versions.check_market_dependency_versions(
        [
            {"name": "baostock", "distribution": "baostock"},
            {"name": "akshare", "distribution": "akshare"},
        ]
    )

    assert [item["name"] for item in result["updates"]] == ["baostock"]
    assert result["errors"] == []
    assert "baostock: 0.8.9 -> 0.9.1" in versions.format_market_dependency_version_message(result)


def test_check_market_dependency_versions_reports_errors(monkeypatch):
    monkeypatch.setattr(versions, "_get_installed_version", lambda distribution: None)

    def fail_fetch(distribution, **_):
        raise RuntimeError("PyPI request failed")

    monkeypatch.setattr(versions, "_fetch_pypi_metadata", fail_fetch)

    result = versions.check_market_dependency_versions(
        [{"name": "missing", "distribution": "missing-package"}]
    )

    assert result["updates"] == []
    assert result["errors"][0]["name"] == "missing"
    assert "PyPI request failed" in result["errors"][0]["error"]


@pytest.mark.asyncio
async def test_scheduler_market_dependency_version_check_notifies_on_update(monkeypatch):
    result = {
        "statuses": [
            {
                "name": "baostock",
                "installed_version": "0.8.9",
                "latest_version": "0.9.1",
                "update_available": True,
                "error": None,
            }
        ],
        "updates": [
            {
                "name": "baostock",
                "installed_version": "0.8.9",
                "latest_version": "0.9.1",
                "update_available": True,
                "error": None,
            }
        ],
        "errors": [],
    }
    monkeypatch.setattr(versions, "check_market_dependency_versions", lambda *_, **__: result)

    sent = []

    class FakeBot:
        async def send_task_notification(self, message, task_name=None, level="info"):
            sent.append((message, task_name, level))
            return True

    task = ScheduledTasks.__new__(ScheduledTasks)
    task._active_tasks = set()
    task.telegram_enabled = True
    task.bot = FakeBot()

    ok = await task.market_dependency_version_check(
        packages=[{"name": "baostock", "distribution": "baostock"}]
    )

    assert ok is True
    assert sent
    assert sent[0][1] == "market_dependency_version_check"
    assert sent[0][2] == "warning"
    assert "0.8.9 -> 0.9.1" in sent[0][0]
    assert "market_dependency_version_check" not in task._active_tasks


def test_scheduler_config_contains_market_dependency_job():
    with open("config/05_scheduler.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    job = config["scheduler_config"]["jobs"]["market_dependency_version_check"]
    assert job["enabled"] is True
    assert job["trigger"]["hour"] == 12
    assert job["trigger"]["minute"] == 0
    assert job["parameters"]["notify_when_latest"] is False
    assert set(job["parameters"]["auto_upgrade_packages"]) == {
        "curl_cffi",
        "akshare_proxy_patch",
        "akshare",
        "pytdx",
        "baostock",
        "yfinance",
    }


@pytest.mark.asyncio
async def test_scheduler_launches_one_worker_for_due_auto_packages(monkeypatch):
    result = {
        "statuses": [],
        "updates": [
            {
                "name": "baostock",
                "latest_version": "0.9.5",
                "update_available": True,
            },
            {
                "name": "pytdx",
                "latest_version": "1.73",
                "update_available": True,
            },
            {
                "name": "not_auto",
                "latest_version": "9.0.0",
                "update_available": True,
            },
        ],
        "errors": [],
    }
    monkeypatch.setattr(versions, "check_market_dependency_versions", lambda *_, **__: result)
    captured = {}

    def fake_popen(command, **_kwargs):
        captured["command"] = command

        class _Process:
            pid = 321

        return _Process()

    monkeypatch.setattr(scheduler_tasks.subprocess, "Popen", fake_popen)

    class FakeBot:
        async def send_task_notification(self, message, task_name=None, level="info"):
            captured["message"] = message
            return True

    task = ScheduledTasks.__new__(ScheduledTasks)
    task._active_tasks = set()
    task.telegram_enabled = True
    task.bot = FakeBot()

    assert await task.market_dependency_version_check(
        auto_upgrade_packages=["baostock", "pytdx", "akshare"]
    )
    command = captured["command"]
    assert command[1].endswith("upgrade_market_dependency.py")
    assert "baostock==0.9.5" in command
    assert "pytdx==1.73" in command
    assert "not_auto" not in " ".join(command)
    assert "akshare==" not in " ".join(command)
    assert "已触发自动升级：baostock, pytdx" in captured["message"]
