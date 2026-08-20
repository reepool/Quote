from scheduler.tasks import _annual_report_asset_report_data
from utils.report import generate_report


def test_annual_report_asset_partial_report_renders_outcome_and_reasons():
    report_data = _annual_report_asset_report_data(
        "annual_report_asset_daily_update",
        {
            "status": "completed",
            "outcome": "partial",
            "progress": {
                "daily_result": {
                    "metadata_registered": 6619,
                    "attachments_attempted": 200,
                    "attachments_downloaded": 162,
                    "attachments_reused": 38,
                    "attachment_failures": 0,
                    "attachment_retries_queued": 3255,
                    "errors": [
                        "universe: authoritative A-share master refresh is incomplete",
                        "cninfo/SZSE: max_pages_exhausted",
                    ],
                    "stage_log": [{"stage": "universe"}, {"stage": "discovery"}],
                    "metrics": {
                        "stage_timings_seconds": {"total": 1086.8},
                        "attachment_retry_backlog": 3440,
                    },
                }
            },
        },
    )

    rendered = generate_report("annual_report_asset_report", report_data, "telegram")

    assert "本轮已结束，工作未全部完成" in rendered
    assert "公告记录：发现 0 条，登记 6619 条" in rendered
    assert "耗时：1086.8 秒" in rendered
    assert "新下载 162" in rendered
    assert "附件队列：本轮新增 3255，当前积压 3440" in rendered
    assert "已保存下一页位置" in rendered


def test_annual_report_asset_failed_report_renders_permission_error():
    report_data = _annual_report_asset_report_data(
        "annual_report_asset_daily_update",
        {
            "status": "failed",
            "outcome": "failed",
            "diagnostics": {
                "error_type": "PermissionError",
                "error": "principal_not_registered",
            },
            "scheduler_elapsed_seconds": 1.25,
        },
    )

    rendered = generate_report("annual_report_asset_report", report_data, "telegram")

    assert "执行失败" in rendered
    assert "PermissionError: principal_not_registered" in rendered
    assert "耗时：1.2 秒" in rendered


def test_annual_report_asset_backfill_report_uses_top_level_progress():
    report_data = _annual_report_asset_report_data(
        "annual_report_asset_latest_backfill",
        {
            "status": "completed",
            "outcome": "success",
            "progress": {
                "records_seen": 300,
                "downloaded": 20,
                "local_hits": 280,
                "retryable": 2,
                "errors": [],
            },
        },
    )

    rendered = generate_report("annual_report_asset_report", report_data, "telegram")

    assert "全部完成" in rendered
    assert "公告记录：发现 300 条，登记 300 条" in rendered
    assert "队列处理 300，新下载 20，本地或等价复用 280" in rendered
    assert "附件队列：本轮新增 2，当前积压 2" in rendered
