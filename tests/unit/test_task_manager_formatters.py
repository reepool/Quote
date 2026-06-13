from utils.task_manager.formatters import TaskManagerFormatters
from utils.task_manager.models import TaskStatus


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

    assert "`/run index_master_governance_sync`" in text
    assert "`/run technical_snapshot_refresh`" in text


def test_status_summary_groups_tasks_by_status_and_domain():
    running_tasks = [
        {
            "job_id": "daily_data_update",
            "description": "每日数据更新任务",
            "next_run": "今天 20:00",
            "status": "running",
        },
        {
            "job_id": "shareholder_reconciliation_sync",
            "description": "研究域股东摘要周期复核与补足",
            "next_run": "周六 12:30",
            "status": "running",
        },
        {
            "job_id": "index_master_governance_sync",
            "description": "A股指数主数据治理",
            "next_run": "手工触发",
            "status": "paused",
        },
    ]
    disabled_tasks = [
        {
            "job_id": "technical_snapshot_refresh",
            "description": "技术快照刷新",
            "status": "disabled",
        }
    ]

    text = TaskManagerFormatters.format_task_status_summary(
        running_tasks,
        disabled_tasks,
        total_tasks=4,
    )

    assert "已调度 `2`" in text
    assert "手工/暂停 `1`" in text
    assert "**🟢 已调度**" in text
    assert "**🟡 手工/暂停**" in text
    assert "**🔴 已禁用**" in text
    assert "*行情与主数据*" in text
    assert "*股东与披露*" in text
    assert "*行业与指数*" in text
    assert "*研究与风控*" in text
    assert text.index("*行情与主数据*") < text.index("`/run daily_data_update`")
    assert text.index("*股东与披露*") < text.index("`/run shareholder_reconciliation_sync`")


def test_status_summary_accepts_task_status_enum():
    class _Task:
        job_id = "hkex_instrument_master_sync"
        description = "港股主数据同步/审计"
        status = TaskStatus.PAUSED
        next_run_time = None
        trigger_info = None

    text = TaskManagerFormatters.format_task_status_summary(
        [_Task()],
        [],
        total_tasks=1,
    )

    assert "手工/暂停 `1`" in text
    assert "*港美市场*" in text
    assert "`/run hkex_instrument_master_sync`" in text


def test_status_summary_stays_under_telegram_message_limit():
    running_tasks = [
        {
            "job_id": f"financial_disclosure_incremental_sync_{index:02d}",
            "description": "研究域财务报表日度增量 catch-up 与异常披露补处理",
            "next_run": "每天 21:30",
            "status": "running",
        }
        for index in range(80)
    ]

    text = TaskManagerFormatters.format_task_status_summary(
        running_tasks,
        [],
        total_tasks=len(running_tasks),
    )

    assert len(text) <= TaskManagerFormatters.STATUS_MESSAGE_MAX_CHARS
    assert "... 已省略" in text
    assert "`/run financial_disclosure_incremental_sync_00`" in text
