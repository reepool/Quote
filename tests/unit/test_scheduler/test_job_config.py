import json
from pathlib import Path
from unittest.mock import Mock

from scheduler.job_config import JobConfig, JobConfigManager


def test_manual_only_job_without_trigger_has_no_next_run_time():
    manager = JobConfigManager(Mock())
    manager.job_configs["manual_job"] = JobConfig(
        job_id="manual_job",
        enabled=True,
        manual_only=True,
        description="manual test job",
        trigger=None,
        max_instances=1,
        misfire_grace_time=300,
        coalesce=True,
        parameters={},
    )

    assert manager.get_next_run_time("manual_job") is None


def test_annual_asset_production_cron_uses_shanghai_timezone():
    scheduler = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )["scheduler_config"]["jobs"]
    manager = JobConfigManager(Mock())

    trigger = manager._parse_trigger(
        scheduler["annual_report_asset_daily_update"]["trigger"]
    )

    assert trigger is not None
    assert str(trigger.timezone) == "Asia/Shanghai"


def test_annual_asset_runtime_config_matches_scheduler_and_trusts_telegram_operator():
    scheduler_jobs = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )["scheduler_config"]["jobs"]
    asset_config = json.loads(
        Path("config/10_research.json").read_text(encoding="utf-8")
    )["research_config"]["modules"]["official_announcement_assets"]

    trigger = scheduler_jobs["annual_report_asset_daily_update"]["trigger"]
    assert asset_config["jobs"]["daily_cron"] == (
        f'{trigger["minute"]} {trigger["hour"]} * * *'
    )

    principals = {
        item["principal"]: set(item["scopes"])
        for item in asset_config["permissions"]["principals"]
    }
    assert principals["telegram:471105519"] == {
        "annual_report_assets:operator"
    }


def test_high_io_weekly_jobs_use_separate_production_windows():
    jobs = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )["scheduler_config"]["jobs"]

    annual = jobs["annual_report_asset_daily_update"]["trigger"]
    tdx_weekly = jobs["a_share_tdx_corporate_action_weekly_full_refresh"]["trigger"]
    database_backup = jobs["database_backup"]["trigger"]

    assert (annual["day_of_week"], annual["hour"], annual["minute"]) == (
        "mon-sun",
        0,
        15,
    )
    assert (
        tdx_weekly["day_of_week"],
        tdx_weekly["hour"],
        tdx_weekly["minute"],
    ) == ("sun", 7, 15)
    assert (
        database_backup["day_of_week"],
        database_backup["hour"],
        database_backup["minute"],
    ) == ("mon", 1, 15)
