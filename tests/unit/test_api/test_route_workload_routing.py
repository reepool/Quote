"""
Tests for API route workload routing helpers.
"""

import pytest
from fastapi import BackgroundTasks

from api.models import DataGapFillRequest, TaskStartResponse
from api.routes import _run_data_task_workload, fill_data_gaps
from database.connection import db_workload_context, get_current_db_workload


@pytest.mark.unit
@pytest.mark.asyncio
async def test_api_triggered_data_task_runs_on_task_workload():
    seen = []

    async def task_func(value):
        seen.append(get_current_db_workload())
        return value

    async with db_workload_context("api"):
        result = await _run_data_task_workload(task_func, "ok")
        assert get_current_db_workload() == "api"

    assert result == "ok"
    assert seen == ["task"]
    assert get_current_db_workload() == "task"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_gap_fill_route_preserves_dry_run_and_filters(monkeypatch):
    captured = {}

    async def fake_fill_data_gaps(**kwargs):
        captured.update(kwargs)
        captured["workload"] = get_current_db_workload()

    from api import routes

    monkeypatch.setattr(routes.data_manager, "fill_data_gaps", fake_fill_data_gaps)

    request = DataGapFillRequest(
        exchange="SSE",
        instrument_ids=["000001.SZ"],
        severity_filter=["high"],
        gap_type_filter=["missing_data"],
        max_gap_days=20,
        dry_run=True,
    )
    background_tasks = BackgroundTasks()

    response = await fill_data_gaps(request, background_tasks)

    TaskStartResponse(**response)
    assert response["data"]["dry_run"] is True
    assert len(background_tasks.tasks) == 1

    async with db_workload_context("api"):
        await background_tasks.tasks[0]()
        assert get_current_db_workload() == "api"

    assert captured == {
        "exchange": "SSE",
        "severity_filter": ["high"],
        "instrument_ids": ["000001.SZ"],
        "gap_type_filter": ["missing_data"],
        "max_gap_days": 20,
        "dry_run": True,
        "workload": "task",
    }
