import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_cninfo_corporate_action_llm_report,
    data_manager,
)


def test_cninfo_corporate_action_llm_job_is_manual_candidate_only():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"]["a_share_cninfo_corporate_action_llm_resolution"]
    assert job["manual_only"] is True
    assert job["parameters"]["dry_run"] is True
    assert job["parameters"]["run_ocr"] is False
    assert job["parameters"]["refresh_documents"] is False
    incremental = config["scheduler_config"]["jobs"][
        "a_share_cninfo_corporate_action_llm_incremental"
    ]
    assert incremental["enabled"] is False
    assert incremental["parameters"]["dry_run"] is True
    assert incremental["parameters"]["refresh_documents"] is False
    assert "不会自动写入 resolved" in _format_cninfo_corporate_action_llm_report({
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
        dry_run=True,
    )
    assert result["status"] == "dry_run"
    assert operation.await_args.kwargs["max_events"] == 1
    assert operation.await_args.kwargs["refresh_documents"] is False
    assert "a_share_cninfo_corporate_action_llm_resolution" not in task._active_tasks
