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


def test_annual_asset_production_cron_triggers_use_shanghai_timezone():
    scheduler = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )["scheduler_config"]["jobs"]
    manager = JobConfigManager(Mock())

    for job_name in (
        "annual_report_asset_daily_update",
        "annual_report_asset_backup",
    ):
        trigger = manager._parse_trigger(scheduler[job_name]["trigger"])
        assert trigger is not None
        assert str(trigger.timezone) == "Asia/Shanghai"
