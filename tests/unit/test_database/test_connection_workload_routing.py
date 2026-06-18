"""
Tests for async DB workload routing between API and task pools.
"""

import pytest

from database.connection import (
    DatabaseManager,
    db_workload_context,
    get_current_db_workload,
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_manager_routes_api_and_task_sessions(tmp_path):
    db_path = tmp_path / "workload_routing.db"
    manager = DatabaseManager(str(db_path))
    manager.initialize()

    try:
        assert manager.TaskAsyncSessionLocal is not manager.ApiAsyncSessionLocal
        assert get_current_db_workload() == "task"

        task_session = manager.get_async_session()
        try:
            assert task_session.bind is manager.task_async_engine
        finally:
            await task_session.close()

        async with db_workload_context("api"):
            api_session = manager.get_async_session()
            try:
                assert api_session.bind is manager.api_async_engine
            finally:
                await api_session.close()

        assert get_current_db_workload() == "task"
    finally:
        await manager.close_async()


@pytest.mark.unit
def test_database_package_reuses_single_database_operations_instance():
    import database
    from database.operations import database_operations

    assert database.db_ops is database_operations
    assert database.db_operations is database_operations
