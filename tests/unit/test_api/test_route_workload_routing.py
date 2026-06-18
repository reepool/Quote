"""
Tests for API route workload routing helpers.
"""

import pytest

from api.routes import _run_data_task_workload
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
