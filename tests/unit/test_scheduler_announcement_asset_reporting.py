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
                    "stage_timings_seconds": {"total": 1086.8},
                }
            },
        },
    )

    rendered = generate_report("maintenance_report", report_data, "telegram")

    assert "completed / partial" in rendered
    assert "元数据登记 - 状态: 6619" in rendered
    assert "下载 162" in rendered
    assert "待处理附件 - 状态: 3255" in rendered
    assert "max_pages_exhausted" in rendered


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
        },
    )

    rendered = generate_report("maintenance_report", report_data, "telegram")

    assert "failed / failed" in rendered
    assert "PermissionError: principal_not_registered" in rendered


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

    rendered = generate_report("maintenance_report", report_data, "telegram")

    assert "元数据登记 - 状态: 300" in rendered
    assert "尝试 300，下载 20，复用 280" in rendered
    assert "待处理附件 - 状态: 2" in rendered
