import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_cninfo_corporate_action_llm_report,
    _format_cninfo_resolution_governance_report,
    data_manager,
)


def test_cninfo_corporate_action_llm_job_is_manual_governed_resolution():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"]["a_share_cninfo_corporate_action_llm_resolution"]
    assert job["manual_only"] is True
    assert job["parameters"]["dry_run"] is True
    assert job["parameters"]["run_ocr"] is False
    assert job["parameters"]["refresh_documents"] is False
    assert job["parameters"]["auto_promote_validated"] is True
    assert job["parameters"]["exclude_reviewed_events"] is False
    assert job["parameters"]["pipeline"] == {
        "mode": "serial",
        "stage_queue_size": 200,
        "title_max_titles_per_request": 80,
        "download_concurrency": 8,
        "document_parse_concurrency": 8,
        "llm_concurrency": 15,
        "llm_requests_per_minute": 0,
        "writer_batch_size": 10,
        "writer_concurrency": 1,
        "progress_interval_seconds": 30,
        "verification_policy": "always",
    }
    governance = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_resolution_governance"
    ]
    assert governance["manual_only"] is True
    assert governance["parameters"]["dry_run"] is True
    assert governance["parameters"]["scopes"] == [
        "inventory", "discovery", "resolution"
    ]
    assert governance["parameters"]["retry_evidence_unavailable"] is False
    assert governance["parameters"]["classify_titles_with_llm"] is True
    assert governance["parameters"]["title_classification_profile"] == (
        "corporate_action_title_classification"
    )
    assert governance["parameters"]["title_max_concurrency"] == 50
    assert governance["parameters"]["window_before_days"] == 30
    assert governance["parameters"]["max_anchor_gap_days"] == 60
    reset = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_resolution_reset"
    ]
    assert reset["manual_only"] is True
    assert reset["parameters"]["dry_run"] is True
    assert reset["parameters"]["confirm_reset"] is False
    assert reset["parameters"]["include_unanchored"] is False
    assert reset["parameters"]["exchanges"] == ["SSE", "SZSE"]
    incremental = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_llm_incremental"
    ]
    assert incremental["enabled"] is False
    assert incremental["parameters"]["dry_run"] is True
    assert incremental["parameters"]["refresh_documents"] is False
    assert incremental["parameters"]["auto_promote_validated"] is True
    assert incremental["parameters"]["exclude_reviewed_events"] is True
    assert incremental["parameters"]["classify_titles_with_llm"] is True
    llm_config = json.loads(Path("config/11_llm.json").read_text(encoding="utf-8"))
    title_profile = llm_config["llm"]["profiles"][
        "corporate_action_title_classification"
    ]
    assert title_profile["max_concurrency"] == 50
    assert title_profile["requests_per_minute"] == 0
    resource = llm_config["llm"]["provider_resources"][
        "openai_compatible:QUOTE_LLM_API_KEY"
    ]
    assert resource["requests_per_minute"] == 58
    assert title_profile["max_retries"] == 2
    assert title_profile["retry_backoff_seconds"] == 2.0
    assert title_profile["retry_jitter_ratio"] == 0.5
    assert llm_config["llm"]["profiles"]["semantic_extraction"][
        "max_concurrency"
    ] == 50
    assert "高置信结果可写入受治理的 resolved 层" in _format_cninfo_corporate_action_llm_report({
        "status": "dry_run", "dry_run": True, "counts": {}, "targets": {},
    })
    workload_report = _format_cninfo_corporate_action_llm_report({
        "status": "partial",
        "dry_run": False,
        "counts": {"processed": 3, "analyzed": 3, "manual_required": 2},
        "targets": {"candidate_events": 10, "batch_events": 3, "next_target_offset": 3},
        "review_workload": {
            "tiers": {"machine_rework": 1, "quick_review": 1, "deep_review": 1},
            "gate_signatures": {"date_in_evidence": 1, "all_gates_passed": 1},
            "remaining_manual_review": 3,
        },
        "auto_promotion": {
            "enabled": True,
            "eligible": 1,
            "promoted": 1,
            "dry_run_eligible": 0,
            "skipped": 1,
            "failed": 0,
            "reason_counts": {"prior_event_review_exists": 1},
        },
        "llm_metrics": {
            "input_tokens": 300,
            "output_tokens": 120,
            "total_tokens": 420,
            "provider_output_budget_overruns": 1,
            "latency_ms": {"p50": 1000, "p95": 2000, "max": 2000},
        },
        "parameters": {
            "pipeline": {
                "llm_requests_per_minute": 0,
                "effective_llm_requests_per_minute": 58,
            },
        },
    })
    assert "机器返工: `1`" in workload_report
    assert "快速审核: `1`，深度审核: `1`" in workload_report
    assert "promoted=1" in workload_report
    assert "剩余人工审核: `3`" in workload_report
    assert "prior_event_review_exists: 1" in workload_report
    assert "输出预算超限: `1`" in workload_report
    assert "date_in_evidence: 1" in workload_report
    assert "llm_rpm=58 (inherited)" in workload_report
    failure_report = _format_cninfo_corporate_action_llm_report({
        "status": "partial",
        "dry_run": True,
        "counts": {"errors": 1},
        "targets": {},
        "errors": [{
            "source_event_key": "event-1",
            "code": "transient_transport_error",
            "attempt_count": 2,
            "error": "LLM provider request timed out",
        }],
    })
    assert "attempts=2" in failure_report
    assert "LLM provider request timed out" in failure_report
    governance_report = _format_cninfo_resolution_governance_report({
        "status": "dry_run",
        "dry_run": True,
        "parameters": {
            "start_date": "1990-12-19",
            "end_date": "2026-07-21",
            "exchanges": ["SSE", "SZSE"],
            "scopes": ["inventory"],
        },
        "inventory": {
            "total_events": 381,
            "actionable_events": 380,
            "terminal_events": 1,
            "factor_blocking_events": 380,
            "source_unsupported_events": 0,
            "state_counts": {"discovery_pending": 115},
            "next_action_counts": {"discover_official_announcements": 115},
        },
        "targets": {"eligible_events": 380, "batch_events": 100},
        "stages": {
            "asymmetric_review": {
                "scanned": 20,
                "eligible": 8,
                "promoted": 8,
                "updated": 0,
                "unchanged": 0,
                "skipped": 0,
                "blocked": 12,
                "failed": 0,
                "network_access": False,
                "llm_invocations": 0,
                "blocked_reason_counts": {
                    "implementation_grade_announcement_missing": 12,
                },
            },
            "tdx_asymmetric_review": {
                "scanned": 92,
                "special_events": 82,
                "eligible": 14,
                "promoted": 14,
                "skipped": 0,
                "blocked": 68,
                "failed": 0,
                "network_access": False,
                "llm_invocations": 0,
                "mismatch_reason_counts": {
                    "tdx_event_not_found_near_cninfo_dates": 60,
                    "tdx_economic_conflict": 8,
                },
            },
            "discovery": {
                "title_classification": {
                    "status": "success",
                    "input_title_count": 125,
                    "request_count": 5,
                    "max_concurrency": 5,
                    "peak_concurrency": 4,
                    "event_errors": 0,
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
            },
        },
    })
    assert "非对称旁路" in governance_report
    assert "promoted=8" in governance_report
    assert "implementation_grade_announcement_missing: 12" in governance_report
    assert "TDX非对称对账" in governance_report
    assert "matched=14" in governance_report
    assert "tdx_economic_conflict: 8" in governance_report
    assert "factor_blocking=380" in governance_report
    assert "discovery_pending: 115" in governance_report
    assert "concurrency=4/5" in governance_report
    assert "000409.SZ" not in governance_report


@pytest.mark.asyncio
async def test_scheduler_delegates_bounded_llm_resolution(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    operation = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(data_manager, "analyze_cninfo_corporate_action_candidates", operation)
    result = await task.a_share_cninfo_corporate_action_llm_resolution(
        start_date="2026-01-01", end_date="2026-12-31",
        exchanges=["SZSE"], instrument_ids=["000001.SZ"],
        source_event_keys=["event-1"], max_events=1,
        dry_run=True, auto_promote_validated=True,
        pipeline={"stage_queue_size": 25},
        pipeline_mode="async",
        pipeline_llm_concurrency=10,
        pipeline_llm_requests_per_minute=12,
    )
    assert result["status"] == "dry_run"
    assert operation.await_args.kwargs["max_events"] == 1
    assert operation.await_args.kwargs["source_event_keys"] == ["event-1"]
    assert operation.await_args.kwargs["refresh_documents"] is False
    assert operation.await_args.kwargs["auto_promote_validated"] is True
    assert operation.await_args.kwargs["pipeline"] == {
        "stage_queue_size": 25,
        "mode": "async",
        "llm_concurrency": 10,
        "llm_requests_per_minute": 12,
    }
    assert "a_share_cninfo_corporate_action_llm_resolution" not in task._active_tasks


@pytest.mark.asyncio
async def test_incremental_resolution_excludes_reviewed_events(monkeypatch):
    task = ScheduledTasks()
    delegated = AsyncMock(return_value={"status": "dry_run"})
    monkeypatch.setattr(
        task,
        "a_share_cninfo_corporate_action_resolution_governance",
        delegated,
    )

    result = await task.a_share_cninfo_corporate_action_llm_incremental()

    assert result["status"] == "dry_run"
    assert delegated.await_args.kwargs["scopes"] == [
        "inventory", "discovery", "resolution"
    ]
    assert delegated.await_args.kwargs["target_offset"] == 0
    assert delegated.await_args.kwargs["exclude_reviewed_events"] is True
    assert delegated.await_args.kwargs["classify_titles_with_llm"] is True
    assert delegated.await_args.kwargs["title_max_concurrency"] == 50


@pytest.mark.asyncio
async def test_scheduler_delegates_full_market_resolution_governance(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    operation = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(
        data_manager,
        "govern_cninfo_corporate_action_resolutions",
        operation,
    )

    result = await task.a_share_cninfo_corporate_action_resolution_governance(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE", "SZSE"],
        source_event_keys=["event-1"],
        scopes=["inventory", "discovery"],
        max_events=50,
        pipeline={"stage_queue_size": 25},
        pipeline_mode="async",
        pipeline_download_concurrency=8,
        pipeline_document_parse_concurrency=8,
        pipeline_llm_concurrency=50,
        pipeline_llm_requests_per_minute=58,
        pipeline_progress_interval_seconds=30,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert operation.await_args.kwargs["max_events"] == 50
    assert operation.await_args.kwargs["source_event_keys"] == ["event-1"]
    assert operation.await_args.kwargs["scopes"] == ["inventory", "discovery"]
    assert operation.await_args.kwargs["classify_titles_with_llm"] is True
    assert operation.await_args.kwargs["title_max_concurrency"] == 50
    assert operation.await_args.kwargs["max_anchor_gap_days"] == 60
    assert operation.await_args.kwargs["pipeline"] == {
        "stage_queue_size": 25,
        "mode": "async",
        "download_concurrency": 8,
        "document_parse_concurrency": 8,
        "llm_concurrency": 50,
        "llm_requests_per_minute": 58,
        "progress_interval_seconds": 30,
    }
    assert (
        "a_share_cninfo_corporate_action_resolution_governance"
        not in task._active_tasks
    )


@pytest.mark.asyncio
async def test_scheduler_delegates_confirmed_resolution_reset(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    operation = AsyncMock(return_value={"status": "success", "dry_run": False})
    monkeypatch.setattr(
        data_manager,
        "reset_cninfo_corporate_action_resolution_governance",
        operation,
    )

    result = await task.a_share_cninfo_corporate_action_resolution_reset(
        start_date="1990-12-19",
        end_date="2026-07-23",
        exchanges=["SSE", "SZSE"],
        include_unanchored=True,
        dry_run=False,
        confirm_reset=True,
    )

    assert result["status"] == "success"
    assert operation.await_args.kwargs["confirm_reset"] is True
    assert operation.await_args.kwargs["include_unanchored"] is True
    assert operation.await_args.kwargs["dry_run"] is False
    assert (
        "a_share_cninfo_corporate_action_resolution_reset"
        not in task._active_tasks
    )


@pytest.mark.asyncio
async def test_task_report_sends_problem_details_as_separate_messages():
    task = ScheduledTasks()
    task.telegram_enabled = True
    task.bot = SimpleNamespace(
        send_report_notification=AsyncMock(return_value=True),
        send_data_notification=AsyncMock(return_value=True),
    )

    sent = await task._send_task_report(
        {
            "content": "summary only",
            "detail_messages": ["problem batch 1", "problem batch 2"],
        },
        "maintenance_report",
        "cninfo-test",
        SimpleNamespace(report=True),
    )

    assert sent is True
    task.bot.send_report_notification.assert_awaited_once()
    assert task.bot.send_data_notification.await_count == 2
