import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_cninfo_corporate_action_llm_report,
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
    incremental = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_llm_incremental"
    ]
    assert incremental["enabled"] is False
    assert incremental["parameters"]["dry_run"] is True
    assert incremental["parameters"]["refresh_documents"] is False
    assert incremental["parameters"]["auto_promote_validated"] is True
    assert incremental["parameters"]["exclude_reviewed_events"] is True
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
    })
    assert "机器返工: `1`" in workload_report
    assert "快速审核: `1`，深度审核: `1`" in workload_report
    assert "promoted=1" in workload_report
    assert "剩余人工审核: `3`" in workload_report
    assert "prior_event_review_exists: 1" in workload_report
    assert "输出预算超限: `1`" in workload_report
    assert "date_in_evidence: 1" in workload_report
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


@pytest.mark.asyncio
async def test_scheduler_delegates_bounded_llm_resolution(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    operation = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(data_manager, "analyze_cninfo_corporate_action_candidates", operation)
    result = await task.a_share_cninfo_corporate_action_llm_resolution(
        start_date="2026-01-01", end_date="2026-12-31",
        exchanges=["SZSE"], instrument_ids=["000001.SZ"], max_events=1,
        dry_run=True, auto_promote_validated=True,
    )
    assert result["status"] == "dry_run"
    assert operation.await_args.kwargs["max_events"] == 1
    assert operation.await_args.kwargs["refresh_documents"] is False
    assert operation.await_args.kwargs["auto_promote_validated"] is True
    assert "a_share_cninfo_corporate_action_llm_resolution" not in task._active_tasks


@pytest.mark.asyncio
async def test_incremental_resolution_excludes_reviewed_events(monkeypatch):
    task = ScheduledTasks()
    delegated = AsyncMock(return_value={"status": "dry_run"})
    monkeypatch.setattr(
        task,
        "a_share_cninfo_corporate_action_llm_resolution",
        delegated,
    )

    result = await task.a_share_cninfo_corporate_action_llm_incremental()

    assert result["status"] == "dry_run"
    assert delegated.await_args.kwargs["discover_candidates"] is True
    assert delegated.await_args.kwargs["target_offset"] == 0
    assert delegated.await_args.kwargs["exclude_reviewed_events"] is True
