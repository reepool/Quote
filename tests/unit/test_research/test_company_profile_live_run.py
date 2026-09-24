from __future__ import annotations

import asyncio
import inspect
import json

import pytest
from pydantic import ValidationError

from research.business_profile_async_production import BusinessProfileWorkRepository
from research.business_profile_production_operations import (
    BusinessProfileAnnouncementFrontierRepository,
)
from research.company_profile.candidate_registry import (
    build_a_share_candidate_registry,
    load_official_task_candidate_registry,
)
from research.company_profile.execution import default_processing_identity
from research.company_profile.live_plan import record_company_profile_live_plan
from research.company_profile.live_run import (
    LIVE_RUN_SCHEMA_VERSION,
    CompanyProfileLiveRunReport,
    live_run_schema_manifest,
    record_live_run_report,
    select_live_run_targets,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import execute_published_task
from research.company_profile.runtime import COMMON_CORE_STORAGE_NAMESPACE
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
)
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


def _registry():
    return build_a_share_candidate_registry(
        as_of="2026-08-30",
        universe_coverage_guarantee="full_market",
        eligible_instruments=(
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "name": "浦发银行",
            },
            {
                "instrument_id": "600036.SH",
                "exchange": "SSE",
                "name": "招商银行",
            },
            {
                "instrument_id": "000001.SZ",
                "exchange": "SZSE",
                "name": "平安银行",
            },
        ),
        asset_coverage={
            "600000.SH": {"status": "available", "fiscal_year": 2025},
            "600036.SH": {"status": "available", "fiscal_year": 2025},
            "000001.SZ": {"status": "confirmed_missing"},
        },
        industry_memberships={
            "600000.SH": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
            "600036.SH": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
            "000001.SZ": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
        },
        effective_reports={
            "600000.SH": {
                "asset_id": "asset-600000-2025",
                "fiscal_year": 2025,
                "report_period": "2025-12-31",
                "availability": "local_valid",
                "decision_state": "effective",
                "published_at": "2026-03-20T00:00:00+08:00",
            },
            "600036.SH": {
                "asset_id": "asset-600036-2025",
                "fiscal_year": 2025,
                "report_period": "2025-12-31",
                "availability": "local_valid",
                "decision_state": "effective",
                "published_at": "2026-03-21T00:00:00+08:00",
            },
        },
    )


def test_select_live_run_targets_keeps_missing_assets_out_of_budget():
    plan = record_company_profile_live_plan()
    registry = _registry()

    selected = select_live_run_targets(registry, plan)

    assert selected == ("600000.SH", "600036.SH")
    assert "000001.SZ" not in selected
    assert registry.counts["asset_not_available"] == 1
    assert registry.counts["total"] == 3


def test_select_live_run_targets_deduplicates_repeated_instrument_ids():
    plan = record_company_profile_live_plan()
    registry = _registry()

    selected = select_live_run_targets(
        registry,
        plan,
        instrument_ids=("600000.SH", "600000.SH"),
    )

    assert selected == ("600000.SH",)
    report = record_live_run_report(
        plan=plan,
        registry=registry,
        selected_instrument_ids=selected,
        delivered_instrument_ids=("600000.SH",),
        knowledge_cutoff="2026-08-30",
    )
    assert report.universe.selected_for_run == 1
    assert report.universe.completed == 1
    payload = json.loads(report.model_dump_json())
    payload["selected_instrument_ids"] = ["600000.SH", "600000.SH"]
    payload["selected_strata"] = [
        payload["selected_strata"][0],
        payload["selected_strata"][0],
    ]
    payload["universe"]["selected_for_run"] = 2
    payload["universe"]["completed"] = 2
    payload["company_outcomes"] = [
        payload["company_outcomes"][0],
        payload["company_outcomes"][0],
    ]
    with pytest.raises(ValidationError, match="duplicate"):
        CompanyProfileLiveRunReport.model_validate_json(json.dumps(payload))


def test_record_live_run_keeps_universe_denominator_and_rejects_batch_rerun():
    plan = record_company_profile_live_plan()
    registry = _registry()
    selected = select_live_run_targets(registry, plan)
    report = record_live_run_report(
        plan=plan,
        registry=registry,
        selected_instrument_ids=selected,
        delivered_instrument_ids=("600000.SH",),
        knowledge_cutoff="2026-08-30",
        incomplete_supplement_ids=("600036.SH",),
    )

    assert report.schema_version == LIVE_RUN_SCHEMA_VERSION
    assert report.production_authorization == PRODUCTION_AUTHORIZATION
    assert report.universe.total == 3
    assert report.universe.missing_asset == 1
    assert report.universe.selected_for_run == 2
    assert report.universe.completed == 1
    assert report.universe.failed == 1
    assert report.whole_batch_rerun is False
    assert report.scale_quality_claim_allowed is False
    assert report.company_outcomes[0].delivered is True
    assert report.company_outcomes[1].supplement_incomplete is True
    assert tuple(
        (item.instrument_id, item.exchange, item.disclosure_form)
        for item in report.selected_strata
    ) == (
        ("600000.SH", "SSE", "finance"),
        ("600036.SH", "SSE", "finance"),
    )
    assert report.company_outcomes[1].batch_rerun_triggered is False

    payload = json.loads(report.model_dump_json())
    payload["whole_batch_rerun"] = True
    with pytest.raises(ValidationError):
        CompanyProfileLiveRunReport.model_validate_json(json.dumps(payload))
    with pytest.raises(ValueError, match="official report"):
        record_live_run_report(
            plan=plan,
            registry=registry,
            selected_instrument_ids=("000001.SZ",),
            delivered_instrument_ids=(),
            knowledge_cutoff="2026-08-30",
        )
    legacy = json.loads(report.model_dump_json())
    del legacy["selected_strata"]
    loaded = CompanyProfileLiveRunReport.model_validate_json(json.dumps(legacy))
    assert loaded.selected_instrument_ids == report.selected_instrument_ids
    assert loaded.selected_strata == ()
    baseline = json.loads(report.model_dump_json())
    baseline.pop("first_expansion_plan_id", None)
    baseline.pop("frozen_report_references", None)
    loaded_baseline = CompanyProfileLiveRunReport.model_validate_json(
        json.dumps(baseline)
    )
    assert loaded_baseline.first_expansion_plan_id is None
    assert loaded_baseline.frozen_report_references == ()
    incomplete = json.loads(report.model_dump_json())
    incomplete["selected_strata"] = [incomplete["selected_strata"][0]]
    with pytest.raises(ValidationError, match="selected strata"):
        CompanyProfileLiveRunReport.model_validate_json(json.dumps(incomplete))


def test_budget_limited_live_run_saves_one_company_without_batch_rerun(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    registry = _registry()
    provider = _RequestBoundOverviewProvider()
    good_report = _report(instrument_id="600000.SH", report_id="asset-live-good")

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
        candidate_registry=registry,
        live_plan=record_company_profile_live_plan(),
    )
    first = asyncio.run(
        service.execute("run", knowledge_cutoff="2026-08-30")
    )
    records = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )
    first_calls = provider.extract_calls
    second = asyncio.run(
        service.execute("run", knowledge_cutoff="2026-08-30")
    )

    live_run = first["live_run"]
    assert first["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert live_run["universe"]["total"] == 3
    assert live_run["universe"]["missing_asset"] == 1
    assert live_run["selected_instrument_ids"] == ["600000.SH", "600036.SH"]
    assert "000001.SZ" not in live_run["selected_instrument_ids"]
    assert live_run["universe"]["completed"] == 1
    assert live_run["universe"]["failed"] == 1
    assert live_run["whole_batch_rerun"] is False
    assert live_run["company_outcomes"][1]["supplement_incomplete"] is True
    assert len(records) == 1
    assert json.loads(records[0].read_text(encoding="utf-8"))["report"][
        "instrument_id"
    ] == "600000.SH"
    assert first_calls == 1
    assert provider.extract_calls == 1
    assert second["live_run"]["whole_batch_rerun"] is False
    assert second["enqueue"]["inserted"] == 0
    control = json.loads(
        (tmp_path / "checkpoints" / "control" / "task_control.json").read_text(
            encoding="utf-8"
        )
    )
    report_path = (
        tmp_path / "checkpoints" / "reports" / f"{LIVE_RUN_SCHEMA_VERSION}.json"
    )
    assert control["latest_result"]["live_run"]["schema_version"] == (
        LIVE_RUN_SCHEMA_VERSION
    )
    assert first["control"]["latest_result"]["live_run"]["selected_instrument_ids"] == [
        "600000.SH",
        "600036.SH",
    ]
    assert report_path.is_file()
    assert json.loads(report_path.read_text(encoding="utf-8"))[
        "schema_version"
    ] == LIVE_RUN_SCHEMA_VERSION


def test_published_run_loads_official_registry_without_caller_injection(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    registry = _registry()
    loaded = []

    def fake_load(**kwargs):
        loaded.append(kwargs)
        return registry

    monkeypatch.setattr(
        "research.company_profile.operations.load_official_task_candidate_registry",
        fake_load,
    )
    provider = _RequestBoundOverviewProvider()
    good_report = _report(instrument_id="600000.SH", report_id="asset-live-entry")

    def load_pages(item):
        if item["instrument_id"] != "600000.SH":
            return None
        return {
            "report": good_report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    result = asyncio.run(
        execute_published_task(
            action="run",
            storage=storage,
            output_root=tmp_path / "output",
            checkpoint_root=tmp_path / "checkpoints",
            page_source=load_pages,
            provider=provider,
            knowledge_cutoff="2026-08-30",
            shared_asset_access=object(),
        )
    )
    source = inspect.getsource(execute_published_task)

    assert loaded
    assert loaded[0]["as_of"] == "2026-08-30"
    assert "load_official_task_candidate_registry" in source
    assert result["live_run"]["selected_instrument_ids"] == [
        "600000.SH",
        "600036.SH",
    ]
    assert result["control"]["latest_result"]["live_run"]["universe"]["total"] == 3
    assert (
        tmp_path / "checkpoints" / "reports" / f"{LIVE_RUN_SCHEMA_VERSION}.json"
    ).is_file()


def test_official_registry_loader_uses_existing_universe_and_report_ports():
    class _Universe:
        def get_latest_full_market_universe_snapshot(self):
            return {
                "snapshot_id": "snap-live",
                "policy_version": "a_share_active.v1",
                "snapshot_at": "2026-08-30T00:00:00+00:00",
                "instrument_rows": {
                    "items": [
                        {
                            "instrument_id": "600000.SH",
                            "exchange": "SSE",
                            "name": "浦发银行",
                        }
                    ]
                },
            }

        def get_latest_complete_universe_snapshot(self):
            raise AssertionError("full-market snapshot already present")

        def list_asset_coverage(self, universe_snapshot_id: str):
            assert universe_snapshot_id == "snap-live"
            return [{"instrument_id": "600000.SH", "status": "available"}]

    class _Access:
        repository = _Universe()

        def get_effective_asset(self, instrument_id: str, **kwargs):
            assert instrument_id == "600000.SH"
            return {
                "asset_id": "asset-600000-2025",
                "fiscal_year": 2025,
                "report_period": "2025-12-31",
                "availability": "local_valid",
                "decision_state": "effective",
                "published_at": "2026-03-20T00:00:00+08:00",
            }

    class _Storage:
        def get_industry_membership_as_of(self, instrument_id: str, as_of: str):
            assert instrument_id == "600000.SH"
            assert as_of == "2026-08-30"
            return {"sw_l1_name": "银行", "taxonomy_system": "sw"}

    registry = load_official_task_candidate_registry(
        as_of="2026-08-30",
        storage=_Storage(),
        shared_asset_access=_Access(),
    )
    assert registry.counts["total"] == 1
    assert registry.candidate("600000.SH").latest_effective_annual_report is not None
    with pytest.raises(ValueError, match="announcement asset access"):
        load_official_task_candidate_registry(
            as_of="2026-08-30",
            storage=_Storage(),
            shared_asset_access=None,
        )


def test_live_run_does_not_drain_preexisting_queue_work(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    leftover = {
        "instrument_id": "601000.SH",
        "symbol": "601000",
        "exchange": "SSE",
    }
    BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument=leftover,
        record=_announcement(
            "annual-2025-601000",
            "排队公司2025年年度报告",
            published_at="2026-03-22T08:00:00+08:00",
        ),
    )
    leftover_queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    leftover_enqueue = leftover_queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=default_processing_identity(),
        instrument_ids=("601000.SH",),
    )
    processed: list[str] = []
    provider = _RequestBoundOverviewProvider()

    def load_pages(item):
        processed.append(item["instrument_id"])
        if item["instrument_id"] == "601000.SH":
            return {
                "report": _report(
                    instrument_id="601000.SH", report_id="asset-live-leftover"
                ),
                "pages": (_overview_page(SERVICE_OVERVIEW),),
            }
        if item["instrument_id"] != "600000.SH":
            return None
        return {
            "report": _report(
                instrument_id="600000.SH", report_id="asset-live-selected"
            ),
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
        candidate_registry=_registry(),
        live_plan=record_company_profile_live_plan(),
    )
    result = asyncio.run(service.execute("run", knowledge_cutoff="2026-08-30"))
    leftover_item = leftover_queue.get(leftover_enqueue["work_ids"][0])

    assert leftover_enqueue["inserted"] == 1
    assert leftover_item["status"] == "pending"
    assert "601000.SH" not in processed
    assert "601000.SH" not in result["live_run"]["selected_instrument_ids"]
    assert result["live_run"]["selected_instrument_ids"] == [
        "600000.SH",
        "600036.SH",
    ]
    assert result["live_run"]["universe"]["completed"] == 1
    assert provider.extract_calls == 1


def test_live_run_outcomes_ignore_historical_profiles_and_parser_crashes(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    output = tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE
    output.mkdir(parents=True, exist_ok=True)
    (output / "old-600036.json").write_text(
        json.dumps({"report": {"instrument_id": "600036.SH"}}),
        encoding="utf-8",
    )
    provider = _RequestBoundOverviewProvider()

    def load_pages(item):
        if item["instrument_id"] == "600000.SH":
            return {
                "report": _report(
                    instrument_id="600000.SH", report_id="asset-live-crash"
                ),
                "pages": (_overview_page(SERVICE_OVERVIEW),),
            }
        raise RuntimeError("pdf parser crashed")

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
        candidate_registry=_registry(),
        live_plan=record_company_profile_live_plan(),
    )
    result = asyncio.run(service.execute("run", knowledge_cutoff="2026-08-30"))
    outcomes = {
        item["instrument_id"]: item for item in result["live_run"]["company_outcomes"]
    }

    assert result["live_run"]["universe"]["completed"] == 1
    assert result["live_run"]["universe"]["failed"] == 1
    assert outcomes["600000.SH"]["delivered"] is True
    assert outcomes["600036.SH"]["outcome"] == "failed"
    assert outcomes["600036.SH"]["delivered"] is False
    assert outcomes["600036.SH"]["supplement_incomplete"] is False


def test_parse_failed_artifact_is_not_supplement_incomplete(tmp_path):
    from research.company_profile.operations import (
        OfficialAnnualReportPageSource,
        _supplement_incomplete,
    )
    from research.company_profile.runtime import (
        CompanyProfileResearchWriter,
        CompanyProfileStageRuntime,
    )

    class _Artifact:
        status = "parse_failed"
        pages: tuple = ()
        recovery_state = "source_unrecoverable"

    class _Repository:
        shared_asset_access = object()
        checkpoint_root = tmp_path

        def get_bound_source_asset(self, item):
            return {
                "archive_path": str(tmp_path / "report.pdf"),
                "content_hash": "a" * 64,
                "published_at": "2026-03-31T00:00:00+08:00",
                "source_asset_id": "filing-1",
            }

    source = OfficialAnnualReportPageSource(_Repository())

    def _failed_extract(asset, **kwargs):
        return {"artifact": _Artifact()}

    import research.business_profile_pdf_artifacts as artifacts

    original = artifacts.ensure_archived_pdf_page_artifact
    artifacts.ensure_archived_pdf_page_artifact = _failed_extract
    try:
        loaded = source({"work_id": "bp-work-frozen", "instrument_id": "600004.SH"})
    finally:
        artifacts.ensure_archived_pdf_page_artifact = original
    assert loaded == {"pdf_parse_failed": True}

    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(
            tmp_path / "output",
            checkpoint_root=tmp_path / "checkpoints",
        ),
        page_source=lambda item: {"pdf_parse_failed": True},
    )
    blocked = runtime._acquire(runtime._bind({"work_id": "bp-work-frozen"}), {"work_id": "bp-work-frozen"})
    assert blocked["reason"] == "pdf_parse_failed"
    assert _supplement_incomplete(
        {
            "metadata": {
                "stage_results": {
                    "acquire": blocked,
                }
            }
        }
    ) is False


def test_empty_live_selection_does_not_enqueue_the_full_frontier(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    empty_registry = build_a_share_candidate_registry(
        as_of="2026-08-30",
        universe_coverage_guarantee="full_market",
        eligible_instruments=(
            {
                "instrument_id": "000001.SZ",
                "exchange": "SZSE",
                "name": "平安银行",
            },
        ),
        asset_coverage={"000001.SZ": {"status": "confirmed_missing"}},
        industry_memberships={
            "000001.SZ": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
        },
    )
    provider = _RequestBoundOverviewProvider()

    def load_pages(item):
        return {
            "report": _report(
                instrument_id=item["instrument_id"],
                report_id=f"asset-empty-{item['instrument_id']}",
            ),
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
        candidate_registry=empty_registry,
        live_plan=record_company_profile_live_plan(),
    )
    result = asyncio.run(service.execute("run", knowledge_cutoff="2026-08-30"))
    records = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )

    assert result["live_run"]["selected_instrument_ids"] == []
    assert result["live_run"]["universe"]["selected_for_run"] == 0
    assert result["live_run"]["universe"]["completed"] == 0
    assert result["enqueue"]["inserted"] == 0
    assert result["enqueue"]["eligible"] == 0
    assert provider.extract_calls == 0
    assert records == []


def test_duplicate_named_ids_count_as_one_live_company(tmp_path):
    storage = _storage(tmp_path)
    _second_frontier(storage)
    provider = _RequestBoundOverviewProvider()

    def load_pages(item):
        if item["instrument_id"] != "600000.SH":
            return None
        return {
            "report": _report(
                instrument_id="600000.SH", report_id="asset-live-dup"
            ),
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    service = _service(
        tmp_path,
        storage,
        provider=provider,
        page_source=load_pages,
        candidate_registry=_registry(),
        live_plan=record_company_profile_live_plan(),
    )
    result = asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-08-30",
            instrument_ids=("600000.SH", "600000.SH"),
        )
    )
    records = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )

    assert result["live_run"]["selected_instrument_ids"] == ["600000.SH"]
    assert result["live_run"]["universe"]["selected_for_run"] == 1
    assert result["live_run"]["universe"]["completed"] == 1
    assert result["live_run"]["universe"]["failed"] == 0
    assert provider.extract_calls == 1
    assert len(records) == 1


def test_live_run_module_has_no_llm_or_legacy_writer_entry():
    from research.company_profile import live_run

    source = inspect.getsource(live_run)
    assert "openai" not in source.lower()
    assert "CompanyProfileTaskService" not in source
    assert "execute_published_task" not in source
    manifest = live_run_schema_manifest()
    assert manifest["production_authorization"] == "not_authorized"
    assert manifest["whole_batch_rerun"] is False
