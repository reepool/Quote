import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_a_share_cninfo_corporate_action_report,
    _format_cninfo_special_action_discovery_report,
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
                "ignored_placeholders": 2,
            },
            "announcement_recovery_required": 1,
            "production_isolation": True,
        }
    )

    assert "partial_missing_fields=1" in report
    assert "missing_ex_date_events=7" in report
    assert "ignored_placeholders=2" in report
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


def test_cninfo_special_action_discovery_is_manual_and_candidate_only():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"][
        "a_share_cninfo_special_action_discovery"
    ]

    assert job["manual_only"] is True
    assert job["parameters"]["dry_run"] is True
    assert job["parameters"]["exchanges"] == ["SSE", "SZSE"]

    report = _format_cninfo_special_action_discovery_report({
        "status": "dry_run",
        "dry_run": True,
        "production_isolation": True,
        "parameters": {
            "start_date": "1990-12-19",
            "end_date": "2026-07-18",
            "scanned_exchanges": ["SSE", "SZSE"],
            "excluded_exchanges": ["BSE"],
        },
        "targets": {"searchable_events": 2, "events_with_candidates": 1},
        "evidence": {"candidate_count": 3},
        "announcement_governance": {
            "ingestion_run_id": None,
            "scan_states_persisted": 0,
            "audits_persisted": 0,
            "errors": 0,
        },
    })

    assert "公告候选证据: `3`" in report
    assert "run_id=None, scans=0, audits=0, errors=0" in report
    assert "不从标题推断日期" in report
    assert "生产因子影响: `无`" in report


@pytest.mark.asyncio
async def test_scheduler_special_action_discovery_delegates_parameters(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    discovery = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(
        data_manager,
        "discover_cninfo_special_action_effective_dates",
        discovery,
    )

    result = await task.a_share_cninfo_special_action_discovery(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE", "SZSE"],
        instrument_ids=["600108.SH"],
        dry_run=True,
        max_events=10,
        target_offset=20,
    )

    assert result["status"] == "dry_run"
    assert discovery.await_args.kwargs["instrument_ids"] == ["600108.SH"]
    assert discovery.await_args.kwargs["max_events"] == 10
    assert discovery.await_args.kwargs["target_offset"] == 20
    assert "a_share_cninfo_special_action_discovery" not in task._active_tasks


def test_cninfo_primary_daily_job_is_bounded_and_single_instance():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_daily_sync"
    ]

    assert job["manual_only"] is False
    assert job["max_instances"] == 1
    assert job["parameters"]["rolling_days"] <= 14
    assert job["parameters"]["build_canonical"] is False


@pytest.mark.asyncio
async def test_scheduler_cninfo_daily_sync_delegates_to_isolated_maintenance(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    maintenance = AsyncMock(return_value={"status": "success"})
    monkeypatch.setattr(
        data_manager,
        "maintain_a_share_cninfo_primary_factors",
        maintenance,
    )

    result = await task.a_share_cninfo_corporate_action_daily_sync(
        exchanges=["SZSE"],
        rolling_days=7,
        request_interval_seconds=0.5,
    )

    assert result["status"] == "success"
    assert maintenance.await_args.kwargs["exchanges"] == ["SZSE"]
    assert maintenance.await_args.kwargs["rolling_days"] == 7
    assert maintenance.await_args.kwargs["build_canonical"] is False
    assert "a_share_cninfo_corporate_action_daily_sync" not in task._active_tasks
