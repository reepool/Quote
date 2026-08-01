import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_a_share_cninfo_corporate_action_report,
    _format_cninfo_problem_detail_messages,
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
        "evidence": {"candidate_count": 3, "rejected_count": 4},
        "title_classification": {
            "enabled": True,
            "status": "success",
            "input_title_count": 7,
            "request_count": 1,
            "max_concurrency": 5,
            "peak_concurrency": 1,
            "event_errors": 0,
        },
        "announcement_governance": {
            "ingestion_run_id": None,
            "scan_states_persisted": 0,
            "audits_persisted": 0,
            "errors": 0,
        },
        "target_samples": [{
            "instrument_id": "000409.SZ",
            "source_event_key": "event-1",
            "search_windows": [{}, {}],
            "announcements_seen": 31,
            "candidate_count": 2,
            "rejected_count": 29,
            "title_classification_status": "success",
        }],
    })

    assert "titles=7, requests=1, concurrency=1/5, event_errors=0" in report
    assert "candidate=3, rejected=4" in report
    assert "000409.SZ" not in report
    assert "run_id=None, scans=0, audits=0, errors=0" in report
    assert "不从标题推断日期" in report
    assert "生产因子影响: `无`" in report

    assert _format_cninfo_problem_detail_messages(
        {
            "target_samples": [{
                "instrument_id": "000409.SZ",
                "source_event_key": "event-1",
                "title_classification_status": "success",
                "errors": [],
            }],
        },
        title="异常明细",
    ) == []


def test_cninfo_problem_details_are_split_and_only_include_failures():
    messages = _format_cninfo_problem_detail_messages(
        {
            "errors": [
                {
                    "instrument_id": f"000{index:03d}.SZ",
                    "source_event_key": f"event-{index}",
                    "error": "classification_failed",
                }
                for index in range(13)
            ],
        },
        title="异常明细",
        items_per_message=12,
    )

    assert len(messages) == 2
    assert "(1/2)" in messages[0]
    assert "(2/2)" in messages[1]
    assert "classification_failed" in messages[0]


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
    assert discovery.await_args.kwargs["classify_titles_with_llm"] is True
    assert discovery.await_args.kwargs["title_max_concurrency"] == 50
    assert discovery.await_args.kwargs["max_anchor_gap_days"] == 60
    assert "a_share_cninfo_special_action_discovery" not in task._active_tasks


def test_cninfo_primary_daily_job_is_bounded_and_single_instance():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_daily_sync"
    ]

    assert job["manual_only"] is False
    assert job["max_instances"] == 1
    assert job["parameters"]["rolling_days"] <= 14
    assert job["parameters"]["announcement_schedule_mode"] == "trading_day"
    assert job["parameters"]["announcement_overlap_days"] == 2
    assert job["parameters"]["announcement_max_pages"] >= 200
    assert job["parameters"]["candidate_limit"] <= 1000
    assert job["parameters"]["safety_sweep_size"] < 5205
    assert job["parameters"]["build_canonical"] is False
    assert job["parameters"]["anomaly_llm_enabled"] is True
    assert job["parameters"]["anomaly_llm_max_events"] == 50
    assert job["parameters"]["anomaly_llm_title_max_concurrency"] == 50
    assert job["parameters"]["anomaly_llm_pipeline_llm_concurrency"] == 50
    assert job["parameters"]["tdx_refresh_mode"] == "targeted"
    assert job["parameters"]["tdx_rotating_sample_size"] == 100
    assert config["scheduler_config"]["jobs"][
        "a_share_tdx_corporate_action_weekly_full_refresh"
    ]["trigger"]["day_of_week"] == "sun"


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
    assert maintenance.await_args.kwargs["announcement_schedule_mode"] == (
        "trading_day"
    )
    assert maintenance.await_args.kwargs["candidate_limit"] == 1000
    assert maintenance.await_args.kwargs["safety_sweep_size"] == 100
    assert maintenance.await_args.kwargs["tdx_refresh_mode"] == "targeted"
    assert maintenance.await_args.kwargs["tdx_rotating_sample_size"] == 100
    assert maintenance.await_args.kwargs["build_canonical"] is False
    assert (
        maintenance.await_args.kwargs["maintain_promoted_canonical"]
        is True
    )
    assert maintenance.await_args.kwargs["anomaly_llm_enabled"] is True
    assert maintenance.await_args.kwargs["anomaly_llm_max_events"] == 50
    assert (
        maintenance.await_args.kwargs[
            "anomaly_llm_pipeline_llm_concurrency"
        ]
        == 50
    )
    assert "a_share_cninfo_corporate_action_daily_sync" not in task._active_tasks


@pytest.mark.asyncio
async def test_scheduler_weekly_tdx_reference_refresh_uses_full_market(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    refresh = AsyncMock(return_value={
        "status": "success",
        "totals": {"processed_instruments": 10},
    })
    monkeypatch.setattr(data_manager, "backfill_tdx_xdxr_history", refresh)

    result = await task.a_share_tdx_corporate_action_weekly_full_refresh(
        end_date="2026-07-30",
        exchanges=["SSE"],
    )

    assert result["status"] == "success"
    assert result["refresh_mode"] == "full"
    assert refresh.await_args.kwargs["instrument_ids"] is None
    assert refresh.await_args.kwargs["repair_universe_mode"] == "current_repair"
    assert (
        "a_share_tdx_corporate_action_weekly_full_refresh"
        not in task._active_tasks
    )
