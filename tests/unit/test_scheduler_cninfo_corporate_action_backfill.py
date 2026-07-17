import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_a_share_cninfo_corporate_action_report,
    data_manager,
)


def test_cninfo_backfill_scheduler_config_is_manual_and_conservative():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"]["a_share_cninfo_corporate_action_backfill"]

    assert job["manual_only"] is True
    assert job["parameters"]["dry_run"] is True
    assert job["parameters"]["request_interval_seconds"] >= 1.0
    assert job["max_instances"] == 1


def test_cninfo_backfill_dry_run_report_does_not_claim_execution():
    report = _format_a_share_cninfo_corporate_action_report(
        {
            "status": "dry_run",
            "dry_run": True,
            "checkpoint_id": "unit",
            "parameters": {
                "start_date": "1990-12-19",
                "end_date": "2026-07-17",
                "exchanges": ["SSE", "SZSE", "BSE"],
                "scopes": ["dividends", "allotments"],
            },
            "universe": {
                "instrument_count": 6000,
                "completed_count": 0,
                "pending_count": 6000,
            },
        }
    )

    assert "结论: *预演完成*" in report
    assert "外部请求: `0`" in report
    assert "数据库写入: `0`" in report
    assert "生产因子影响: `无`" in report


def test_cninfo_backfill_partial_report_exposes_coverage_gaps():
    report = _format_a_share_cninfo_corporate_action_report(
        {
            "status": "partial",
            "dry_run": False,
            "checkpoint_id": "unit",
            "parameters": {
                "start_date": "1990-12-19",
                "end_date": "2026-07-17",
                "exchanges": ["BSE"],
                "scopes": ["dividends"],
            },
            "universe": {
                "instrument_count": 1,
                "completed_count": 1,
                "pending_count": 0,
            },
            "counters": {
                "requested_instruments": 1,
                "requested_endpoints": 1,
                "partial_missing_fields": 1,
                "missing_ex_date_events": 7,
            },
            "announcement_recovery_required": 1,
            "production_isolation": True,
        }
    )

    assert "partial_missing_fields=1" in report
    assert "missing_ex_date_events=7" in report
    assert "需公告补证: `1`" in report


@pytest.mark.asyncio
async def test_scheduler_cninfo_backfill_delegates_manual_parameters(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    backfill = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(
        data_manager,
        "backfill_a_share_cninfo_corporate_actions",
        backfill,
    )

    result = await task.a_share_cninfo_corporate_action_backfill(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        scopes=["dividends", "allotments"],
        dry_run=True,
        request_interval_seconds=1.5,
    )

    assert result["status"] == "dry_run"
    assert backfill.await_args.kwargs["instrument_ids"] == ["000001.SZ"]
    assert backfill.await_args.kwargs["request_interval_seconds"] == 1.5
    assert "a_share_cninfo_corporate_action_backfill" not in task._active_tasks
