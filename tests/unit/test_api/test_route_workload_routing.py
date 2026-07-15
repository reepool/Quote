"""
Tests for API route workload routing helpers.
"""

import pytest
from fastapi import BackgroundTasks

from api.models import DataGapFillRequest, TaskStartResponse
from api.routes import (
    _run_data_task_workload,
    fill_data_gaps,
    run_research_special_commodity_calendar_governance,
    run_research_special_commodity_price_sync,
)
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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_special_commodity_price_sync_route_uses_task_workload(monkeypatch):
    captured = {}

    async def fake_sync(**kwargs):
        captured.update(kwargs)
        captured["workload"] = get_current_db_workload()
        return {"status": "success"}

    from api import routes

    monkeypatch.setattr(routes.data_manager, "run_special_commodity_price_sync", fake_sync)

    async with db_workload_context("api"):
        result = await run_research_special_commodity_price_sync(
            scope_id="cn_coal_bspi",
            venues="CCTDA",
            categories="coal,all",
            commodity_ids="CMD.CN.COAL.BSPI",
            series_ids="CMD.CN.COAL.PORT_PRICE.BSPI.CCTDA.WEEKLY",
            frequencies="weekly",
            start_date="2026-06-01",
            end_date="2026-07-15",
            dry_run=True,
        )
        assert get_current_db_workload() == "api"

    assert result == {"status": "success"}
    assert captured == {
        "scope_id": "cn_coal_bspi",
        "venues": ["CCTDA"],
        "categories": ["coal", "all"],
        "commodity_ids": ["CMD.CN.COAL.BSPI"],
        "series_ids": ["CMD.CN.COAL.PORT_PRICE.BSPI.CCTDA.WEEKLY"],
        "frequencies": ["weekly"],
        "start_date": "2026-06-01",
        "end_date": "2026-07-15",
        "dry_run": True,
        "workload": "task",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_special_commodity_calendar_route_uses_task_workload(monkeypatch):
    captured = {}

    async def fake_governance(**kwargs):
        captured.update(kwargs)
        captured["workload"] = get_current_db_workload()
        return {"status": "success"}

    from api import routes

    monkeypatch.setattr(
        routes.data_manager,
        "run_special_commodity_calendar_governance",
        fake_governance,
    )

    async with db_workload_context("api"):
        result = await run_research_special_commodity_calendar_governance(
            scope_id="cn_coal_bspi",
            series_ids="CMD.CN.COAL.PORT_PRICE.BSPI.CCTDA.WEEKLY",
            start_date="2026-06-01",
            end_date="2026-07-15",
            dry_run=True,
        )
        assert get_current_db_workload() == "api"

    assert result == {"status": "success"}
    assert captured == {
        "scope_id": "cn_coal_bspi",
        "series_ids": ["CMD.CN.COAL.PORT_PRICE.BSPI.CCTDA.WEEKLY"],
        "start_date": "2026-06-01",
        "end_date": "2026-07-15",
        "dry_run": True,
        "workload": "task",
    }
