"""Published-task integration for durable common-core operations.

2.6 proves repeat runs, process interrupt, failure isolation, correction
successors, frozen legacy entries, and original PDF immutability through
company_profile_common_core. It does not open a second execution loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    LEGACY_TASK_NAMES_NOT_CONNECTED,
    PUBLISHED_TASK_NAME,
    published_task_catalog,
)
from research.company_profile.runtime import COMMON_CORE_STORAGE_NAMESPACE
from tests.unit.test_research.test_business_profile_async_production import _frontier
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_pdf_artifacts import _pdf_bytes
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
)
from tests.unit.test_research.test_company_profile_operations import (
    _OfficialAssetAccess,
    _second_frontier,
    _service,
)
from tests.unit.test_research.test_company_profile_runtime import (
    SERVICE_OVERVIEW,
    _overview_page,
    _report,
    _RequestBoundOverviewProvider,
)


def _published_records(tmp_path):
    return sorted(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )


def _page_source(reports):
    def load_pages(item):
        key = str(item.get("announcement_id") or item.get("instrument_id") or "")
        report = reports.get(key)
        if report is None:
            return None
        return {
            "report": report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    return load_pages


def test_repeat_run_reuses_completed_work_without_recalling_provider(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    provider = _RequestBoundOverviewProvider()
    report = _report(instrument_id="600000.SH", report_id="asset-repeat")
    first = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=provider,
            page_source=_page_source({"600000.SH": report, "annual-2025": report}),
        ).execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )
    calls_after_first = provider.extract_calls
    first_records = [path.read_text(encoding="utf-8") for path in _published_records(tmp_path)]
    second = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=provider,
            page_source=_page_source({"600000.SH": report, "annual-2025": report}),
        ).execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )
    queried = asyncio.run(
        _service(tmp_path, storage).execute("query", instrument_ids=["600000.SH"])
    )

    assert first["state"] == "completed"
    assert second["enqueue"]["inserted"] == 0
    assert second["enqueue"]["reused"] == 1
    assert second["state"] == "completed"
    assert provider.extract_calls == calls_after_first == 1
    assert [path.read_text(encoding="utf-8") for path in _published_records(tmp_path)] == first_records
    assert queried["state"] == "found"


def test_new_process_resumes_after_interrupt_without_repeating_completed_scope(
    tmp_path,
):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    provider = _RequestBoundOverviewProvider()
    reports = {
        "600000.SH": _report(instrument_id="600000.SH", report_id="asset-int-a"),
        "annual-2025": _report(instrument_id="600000.SH", report_id="asset-int-a"),
        "600036.SH": _report(instrument_id="600036.SH", report_id="asset-int-b"),
        "annual-2025-600036": _report(instrument_id="600036.SH", report_id="asset-int-b"),
    }
    first = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=_page_source(reports),
    )
    original_extract = provider.extract

    def extract_and_pause(request):
        response = original_extract(request)
        first.pause(reason="process_interrupt")
        return response

    provider.extract = extract_and_pause
    paused = asyncio.run(
        first.execute("run", knowledge_cutoff="2026-08-30", max_items=2)
    )
    calls_after_interrupt = provider.extract_calls
    provider.extract = original_extract
    resumed = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=provider,
            page_source=_page_source(reports),
        ).execute("resume", knowledge_cutoff="2026-08-30", max_items=2)
    )

    assert paused["state"] == "paused"
    assert calls_after_interrupt == 1
    assert resumed["action"] == "resume"
    assert resumed["state"] == "completed"
    assert resumed["enqueue"]["inserted"] == 0
    assert provider.extract_calls == 2
    assert resumed["queue"]["completed"] == 2


def test_one_company_failure_does_not_block_the_other_published_delivery(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    provider = _RequestBoundOverviewProvider()
    good = _report(instrument_id="600000.SH", report_id="asset-iso-good")
    result = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=provider,
            page_source=_page_source(
                {
                    "600000.SH": good,
                    "annual-2025": good,
                }
            ),
        ).execute("run", knowledge_cutoff="2026-08-30", max_items=2)
    )
    queried = asyncio.run(
        _service(tmp_path, storage).execute(
            "query",
            instrument_ids=["600000.SH", "600036.SH"],
        )
    )

    assert result["state"] == "completed"
    assert result["queue"]["completed"] == 1
    assert result["queue"]["machine_rework"] == 1
    assert queried["state"] == "found"
    assert queried["profiles"][0]["instrument_id"] == "600000.SH"
    assert queried["missing_instrument_ids"] == ["600036.SH"]


def test_correction_creates_successor_without_rewriting_published_history(tmp_path):
    storage = _storage(tmp_path)
    frontier, instrument = _frontier(storage)
    provider = _RequestBoundOverviewProvider()
    original = _report(
        instrument_id="600000.SH",
        report_id="asset-original",
        document_version="ver-1",
        published_at="2026-03-20T08:00:00+08:00",
    )
    first = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=provider,
            page_source=_page_source(
                {"600000.SH": original, "annual-2025": original}
            ),
        ).execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )
    original_payloads = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in _published_records(tmp_path)
    ]
    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-2025-corrected",
            "某公司2025年年度报告（更正后）",
            published_at="2026-04-02T08:00:00+08:00",
        ),
    )
    correction = _report(
        instrument_id="600000.SH",
        report_id="asset-correction",
        document_version="ver-2",
        published_at="2026-04-02T08:00:00+08:00",
    )
    correction_pages = {
        "annual-2025": original,
        "annual-2025-corrected": correction,
    }

    def load_correction_pages(item):
        loaded = _page_source(correction_pages)(item)
        if loaded is None or str(item.get("announcement_id") or "") != (
            "annual-2025-corrected"
        ):
            return loaded
        return {
            **loaded,
            "pages": (
                _overview_page(
                    "公司主要从事财富管理和投行服务。"
                    "主要服务包括资产管理、投行承销和托管，"
                    "通过向客户提供金融服务收取手续费。"
                ),
            ),
        }

    second = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=provider,
            page_source=load_correction_pages,
        ).execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )
    records = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in _published_records(tmp_path)
    ]
    queried = asyncio.run(
        _service(tmp_path, storage).execute("query", instrument_ids=["600000.SH"])
    )

    assert first["state"] == "completed"
    assert second["enqueue"]["inserted"] == 1
    assert second["state"] == "completed"
    assert provider.extract_calls == 2
    assert len(records) == 2
    assert original_payloads[0] in records
    assert original_payloads[0]["report"]["document_version"] == "ver-1"
    assert queried["profiles"][0]["freshness"]["document_version"] == "ver-2"
    assert queried["profiles"][0]["report"]["report_id"] == "asset-correction"


def test_legacy_entries_stay_frozen_and_disconnected_from_published_task():
    import scheduler.tasks as task_module
    from utils.config_manager import UnifiedConfigManager

    catalog = published_task_catalog()
    jobs = UnifiedConfigManager("config").get_scheduler_config().jobs
    source = inspect.getsource(task_module.ScheduledTasks.company_profile_common_core)

    assert catalog["task_name"] == PUBLISHED_TASK_NAME
    assert catalog["production_authorization"] == PRODUCTION_AUTHORIZATION
    for name in LEGACY_TASK_NAMES_NOT_CONNECTED:
        assert name in catalog["legacy_task_names_not_connected"]
    assert jobs["business_profile_backfill"]["description"].startswith("已冻结")
    assert "run_business_profile_backfill" not in source
    assert "_drain_stage" not in source
    assert "export_company_profile_research_data" not in source
    assert "enqueue_latest_annual" not in source


def test_official_source_pdf_bytes_are_unchanged_after_published_run(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    content = _pdf_bytes(
        [
            (
                "Principal Business and business model with enough native text "
                "for company-profile page binding."
            )
        ]
    )
    pdf_path = tmp_path / "annual-2025.pdf"
    pdf_path.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    access = _OfficialAssetAccess(
        pdf_path=pdf_path,
        digest=digest,
        announcement_id="annual-2025",
    )
    result = asyncio.run(
        _service(
            tmp_path,
            storage,
            provider=_RequestBoundOverviewProvider(),
            shared_asset_access=access,
        ).execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )

    assert result["state"] == "completed"
    assert pdf_path.read_bytes() == content
    assert hashlib.sha256(pdf_path.read_bytes()).hexdigest() == digest
    assert access.exact_calls >= 1
