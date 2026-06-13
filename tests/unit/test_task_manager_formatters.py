from utils.task_manager.formatters import TaskManagerFormatters


def test_status_summary_formats_job_ids_as_copyable_code():
    """Status task ids should be Telegram-copyable markdown code spans."""
    running_task = {
        "job_id": "index_master_governance_sync",
        "description": "A股指数主数据治理",
        "next_run": "手工触发",
    }
    disabled_task = {
        "job_id": "technical_snapshot_refresh",
        "description": "技术快照刷新",
    }

    text = TaskManagerFormatters.format_task_status_summary(
        [running_task],
        [disabled_task],
        total_tasks=2,
    )

    assert "`index_master_governance_sync`" in text
    assert "`technical_snapshot_refresh`" in text
