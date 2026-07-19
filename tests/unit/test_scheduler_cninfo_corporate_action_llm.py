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
