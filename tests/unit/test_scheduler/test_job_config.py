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
