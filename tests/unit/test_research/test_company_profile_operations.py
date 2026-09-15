from __future__ import annotations

import asyncio
import inspect
import json
from unittest.mock import AsyncMock, Mock

import pytest

from research.business_profile_async_production import BusinessProfileWorkRepository
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
)
from tests.unit.test_research.test_business_profile_async_production import _frontier
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
)
from tests.unit.test_research.test_company_profile_runtime import (
    SERVICE_OVERVIEW,
    _overview_page,
    _report,
    _RequestBoundOverviewProvider,
)


def _service(tmp_path, storage, **kwargs):
    from research.company_profile.operations import CompanyProfileTaskService

    return CompanyProfileTaskService(
        storage=storage,
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        **kwargs,
    )


def _second_frontier(storage):
    repository, first = _frontier(storage)
    second = {
        "instrument_id": "600036.SH",
        "symbol": "600036",
        "exchange": "SSE",
    }
    repository.upsert_record(
        instrument=second,
        record=_announcement(
            "annual-2025-600036",
            "另一家公司2025年年度报告",
            published_at="2026-03-21T08:00:00+08:00",
        ),
    )
    return repository, first, second


def test_published_catalog_lists_verified_task_not_design_or_legacy_names():
    from research.company_profile.operations import (
        PUBLISHED_ACTIONS,
        PUBLISHED_TASK_NAME,
        published_task_catalog,
    )

    catalog = published_task_catalog()

    assert catalog["task_name"] == PUBLISHED_TASK_NAME == "company_profile_common_core"
    assert tuple(catalog["actions"]) == PUBLISHED_ACTIONS == (
        "preview",
        "run",
        "status",
        "pause",
        "resume",
    )
    assert catalog["parameters"]["action"] == list(PUBLISHED_ACTIONS)
    assert catalog["storage_namespace"] == COMMON_CORE_STORAGE_NAMESPACE
    assert catalog["writer"] == COMMON_CORE_WRITER_NAME
    assert catalog["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert "company_profile_preview" not in catalog["actions"]
    assert "business_profile_backfill" in catalog["legacy_task_names_not_connected"]
    assert "business_profile_daily_incremental" in catalog[
        "legacy_task_names_not_connected"
    ]
    assert catalog["stage5_connected"] is True


def test_preview_counts_latest_annual_without_enqueue_or_provider(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    provider = _RequestBoundOverviewProvider()
    service = _service(tmp_path, storage, provider=provider)

    result = asyncio.run(
        service.execute(
            "preview",
            knowledge_cutoff="2026-08-30",
        )
    )
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    with storage.get_connection() as conn:
        work_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_work_items"
        ).fetchone()[0]

    assert result["action"] == "preview"
    assert result["eligible"] == 1
    assert result["instrument_ids"] == ["600000.SH"]
    assert result["inserted"] == 0
    assert result["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert result["task"]["task_name"] == "company_profile_common_core"
    assert work_count == 0
    assert provider.extract_calls == 0
    assert not list((tmp_path / "output").rglob("*.json"))
    assert queue.health()["total"] == 0


def test_run_uses_new_writer_and_does_not_call_legacy_semantic_writer(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    provider = _RequestBoundOverviewProvider()
    report = _report(instrument_id="600000.SH", report_id="asset-ops-run")

    def load_pages(item):
        return {
            "report": report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
    )
    result = asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-08-30",
            max_items=1,
        )
    )
    records = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )
    payload = json.loads(records[0].read_text(encoding="utf-8"))

    assert result["action"] == "run"
    assert result["state"] == "completed"
    assert result["enqueue"]["inserted"] == 1
    assert result["queue"]["completed"] == 1
    assert provider.extract_calls == 1
    assert len(records) == 1
    assert payload["storage_namespace"] == COMMON_CORE_STORAGE_NAMESPACE
    assert payload["writer"] == COMMON_CORE_WRITER_NAME
    assert payload["legacy_writers_invoked"] == []
    assert payload["production_authorization"] == PRODUCTION_AUTHORIZATION


def test_status_reports_queue_control_and_published_catalog(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    service = _service(tmp_path, storage)

    asyncio.run(service.execute("preview", knowledge_cutoff="2026-08-30"))
    result = asyncio.run(service.execute("status"))

    assert result["action"] == "status"
    assert result["task"]["task_name"] == "company_profile_common_core"
    assert result["task"]["actions"] == [
        "preview",
        "run",
        "status",
        "pause",
        "resume",
    ]
    assert "business_profile_backfill" in result["task"][
        "legacy_task_names_not_connected"
    ]
    assert result["queue"]["total"] == 0
    assert result["control"]["schema_version"] == "company_profile_task_control.v1"
    assert result["production_authorization"] == PRODUCTION_AUTHORIZATION


def test_pause_stops_further_drain_and_resume_skips_completed_scopes(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    provider = _RequestBoundOverviewProvider()
    reports = {
        "600000.SH": _report(instrument_id="600000.SH", report_id="asset-ops-a"),
        "600036.SH": _report(instrument_id="600036.SH", report_id="asset-ops-b"),
    }

    def load_pages(item):
        instrument_id = str(item["instrument_id"])
        return {
            "report": reports[instrument_id],
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
    )
    original_extract = provider.extract

    def extract_and_pause(request):
        response = original_extract(request)
        service.pause(reason="fixture_pause")
        return response

    provider.extract = extract_and_pause
    paused = asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-08-30",
            max_items=2,
        )
    )
    calls_after_pause = provider.extract_calls
    status = asyncio.run(service.execute("status"))
    provider.extract = original_extract
    resumed = asyncio.run(
        service.execute(
            "resume",
            knowledge_cutoff="2026-08-30",
            max_items=2,
        )
    )

    assert paused["state"] == "paused"
    assert status["control"]["state"] == "paused"
    assert calls_after_pause == 1
    assert resumed["action"] == "resume"
    assert resumed["state"] == "completed"
    assert resumed["enqueue"]["inserted"] == 0
    assert provider.extract_calls == 2
    assert resumed["queue"]["completed"] == 2


def test_one_unbound_company_does_not_block_the_other(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    provider = _RequestBoundOverviewProvider()
    good_report = _report(instrument_id="600000.SH", report_id="asset-ops-good")

    def load_pages(item):
        if item["instrument_id"] != "600000.SH":
            return None
        return {
            "report": good_report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
    )
    result = asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-08-30",
            max_items=2,
        )
    )

    assert result["state"] == "completed"
    assert result["queue"]["completed"] == 1
    assert result["queue"]["machine_rework"] == 1
    assert provider.extract_calls == 1


def test_unknown_action_is_rejected_with_published_names(tmp_path):
    storage = _storage(tmp_path)
    service = _service(tmp_path, storage)

    with pytest.raises(ValueError, match="preview"):
        asyncio.run(service.execute("company_profile_preview"))


def test_scheduler_job_only_forwards_to_operations_owner(monkeypatch):
    import scheduler.tasks as task_module
    from scheduler.tasks import ScheduledTasks

    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()
    execute = AsyncMock(
        return_value={
            "action": "preview",
            "state": "idle",
            "eligible": 0,
            "production_authorization": PRODUCTION_AUTHORIZATION,
        }
    )
    monkeypatch.setattr(
        "research.company_profile.operations.execute_published_task",
        execute,
    )
    data_manager = Mock()
    data_manager.research_storage = object()
    data_manager.run_business_profile_backfill = AsyncMock()
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    success = asyncio.run(
        task.company_profile_common_core(
            action="preview",
            knowledge_cutoff="2026-08-30",
        )
    )
    source = inspect.getsource(ScheduledTasks.company_profile_common_core)

    assert success is True
    execute.assert_awaited()
    assert execute.await_args.kwargs["action"] == "preview"
    assert execute.await_args.kwargs["storage"] is data_manager.research_storage
    data_manager.run_business_profile_backfill.assert_not_awaited()
    assert "_drain_stage" not in source
    assert "enqueue_latest_annual" not in source
    assert "company_profile_common_core" not in task._active_tasks


def test_scheduler_config_publishes_manual_common_core_task():
    from utils.config_manager import UnifiedConfigManager

    jobs = UnifiedConfigManager("config").get_scheduler_config().jobs
    job = jobs["company_profile_common_core"]

    assert job["enabled"] is True
    assert job["manual_only"] is True
    assert job["parameters"]["action"] == "preview"
    assert "company_profile_preview" not in jobs
    assert jobs["business_profile_backfill"]["description"].startswith("已冻结")
