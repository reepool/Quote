from __future__ import annotations

import asyncio
import csv
import json

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.reads import (
    EXPORT_SCHEMA_VERSION,
    PROFILE_SCHEMA_VERSION,
    _accepted_facts,
)
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
)
from tests.unit.test_research.test_business_profile_async_production import _frontier
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_company_profile_operations import (
    _second_frontier,
    _service,
)
from tests.unit.test_research.test_company_profile_runtime import (
    SERVICE_OVERVIEW,
    _overview_page,
    _report,
    _RequestBoundOverviewProvider,
)


def _publish_one(tmp_path, storage, instrument_id="600000.SH"):
    provider = _RequestBoundOverviewProvider()
    report = _report(instrument_id=instrument_id, report_id=f"asset-read-{instrument_id}")

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
            instrument_ids=[instrument_id],
            max_items=1,
        )
    )
    return service, result


def test_query_returns_profile_template_for_one_published_company(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    _publish_one(tmp_path, storage)
    service = _service(tmp_path, storage)

    result = asyncio.run(
        service.execute("query", instrument_ids=["600000.SH"])
    )
    profile = result["profiles"][0]
    dimensions = {
        item["dimension_id"]: item for item in profile["dimensions"]
    }

    assert result["action"] == "query"
    assert result["state"] == "found"
    assert result["delivered"] == 1
    assert result["legacy_export_used"] is False
    assert result["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert profile["schema_version"] == PROFILE_SCHEMA_VERSION
    assert profile["instrument_id"] == "600000.SH"
    assert profile["writer"] == COMMON_CORE_WRITER_NAME
    assert profile["storage_namespace"] == COMMON_CORE_STORAGE_NAMESPACE
    assert dimensions["principal_business"]["answered"] is True
    assert dimensions["principal_business"]["excerpt"]
    assert profile["freshness"]["published_at"]
    assert profile["freshness"]["report_period"]
    assert profile["accepted_facts"]
    assert any(item.get("evidence") for item in profile["accepted_facts"])
    assert isinstance(profile["gaps"], list)


def test_query_delivers_completed_company_without_waiting_for_the_batch(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    _publish_one(tmp_path, storage, "600000.SH")
    asyncio.run(
        _service(tmp_path, storage).execute(
            "run",
            knowledge_cutoff="2026-08-30",
            instrument_ids=["600036.SH"],
            max_items=1,
        )
    )
    result = asyncio.run(
        _service(tmp_path, storage).execute(
            "query",
            instrument_ids=["600000.SH"],
        )
    )

    assert result["state"] == "found"
    assert result["profiles"][0]["instrument_id"] == "600000.SH"
    assert result["missing_instrument_ids"] == []


def test_accepted_facts_exclude_all_rejected_records_when_no_id_is_accepted():
    facts = _accepted_facts(
        {
            "completed_scopes": [
                {
                    "task_result": {
                        "dispositions": [
                            {"target_id": "blocked-1", "status": "blocked"},
                            {"target_id": "unresolved-1", "status": "unresolved"},
                        ],
                        "records": [
                            {"record_id": "blocked-1", "field_id": "principal_business"},
                            {
                                "record_id": "unresolved-1",
                                "field_id": "products_services",
                            },
                        ],
                    }
                }
            ]
        }
    )

    assert facts == []


def test_accepted_facts_keep_only_accepted_for_review_from_mixed_scope():
    facts = _accepted_facts(
        {
            "accepted_records": [
                {"record_id": "top-level-1", "field_id": "principal_business"},
            ],
            "completed_scopes": [
                {
                    "task_result": {
                        "dispositions": [
                            {"target_id": "ok-1", "status": "accepted_for_review"},
                            {"target_id": "blocked-1", "status": "blocked"},
                        ],
                        "records": [
                            {"record_id": "ok-1", "field_id": "principal_business"},
                            {"record_id": "blocked-1", "field_id": "products_services"},
                        ],
                    }
                }
            ]
        }
    )

    assert [item["record_id"] for item in facts] == ["top-level-1", "ok-1"]


def test_query_does_not_treat_blocked_records_as_accepted_facts(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    _publish_one(tmp_path, storage)
    checkpoint_path = next(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE / "checkpoints").glob(
            "*.json"
        )
    )
    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    payload["accepted_records"] = []
    payload["completed_scopes"] = [
        {
            "task_result": {
                "dispositions": [
                    {"target_id": "blocked-1", "status": "blocked"},
                ],
                "records": [
                    {"record_id": "blocked-1", "field_id": "principal_business"},
                ],
            }
        }
    ]
    checkpoint_path.write_text(json.dumps(payload), encoding="utf-8")

    result = asyncio.run(
        _service(tmp_path, storage).execute("query", instrument_ids=["600000.SH"])
    )

    assert result["state"] == "found"
    assert result["profiles"][0]["accepted_facts"] == []


def test_query_missing_instrument_is_not_found(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    _publish_one(tmp_path, storage)
    result = asyncio.run(
        _service(tmp_path, storage).execute(
            "query",
            instrument_ids=["600036.SH"],
        )
    )

    assert result["state"] == "not_found"
    assert result["profiles"] == []
    assert result["missing_instrument_ids"] == ["600036.SH"]


def test_export_writes_structured_json_and_csv_for_completed_company(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    _publish_one(tmp_path, storage)
    export_dir = tmp_path / "exports"
    result = asyncio.run(
        _service(tmp_path, storage).execute(
            "export",
            instrument_ids=["600000.SH"],
            output_directory=export_dir,
        )
    )
    json_path = export_dir / "600000.SH_2025-12-31.json"
    if not json_path.is_file():
        json_files = list(export_dir.glob("600000.SH_*.json"))
        json_path = json_files[0]
    profile = json.loads(json_path.read_text(encoding="utf-8"))
    rows = list(csv.DictReader((export_dir / "profiles.csv").open(encoding="utf-8")))
    manifest = json.loads(
        (export_dir / "export_manifest.json").read_text(encoding="utf-8")
    )

    assert result["action"] == "export"
    assert result["state"] == "completed"
    assert result["legacy_export_used"] is False
    assert profile["schema_version"] == PROFILE_SCHEMA_VERSION
    assert profile["instrument_id"] == "600000.SH"
    assert rows[0]["instrument_id"] == "600000.SH"
    assert manifest["schema_version"] == EXPORT_SCHEMA_VERSION
    assert manifest["legacy_export_used"] is False
    assert "export_company_profile_research_data" not in json.dumps(result)


def test_scheduler_query_forwards_without_queue_drain(monkeypatch):
    import inspect
    from unittest.mock import AsyncMock, Mock

    import scheduler.tasks as task_module
    from scheduler.tasks import ScheduledTasks

    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task._active_tasks = set()
    task._send_task_report = AsyncMock()
    execute = AsyncMock(
        return_value={
            "action": "query",
            "state": "found",
            "delivered": 1,
            "production_authorization": PRODUCTION_AUTHORIZATION,
        }
    )
    monkeypatch.setattr(
        "research.company_profile.operations.execute_published_task",
        execute,
    )
    data_manager = Mock()
    data_manager.research_storage = object()
    data_manager._get_announcement_asset_access = Mock(return_value=None)
    monkeypatch.setattr(task_module, "data_manager", data_manager)

    success = asyncio.run(
        task.company_profile_common_core(
            action="query",
            instrument_ids=["600000.SH"],
        )
    )
    source = inspect.getsource(ScheduledTasks.company_profile_common_core)

    assert success is True
    execute.assert_awaited()
    assert execute.await_args.kwargs["action"] == "query"
    assert "_drain_stage" not in source
