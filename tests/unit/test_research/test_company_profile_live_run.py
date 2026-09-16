from __future__ import annotations

import asyncio
import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.candidate_registry import build_a_share_candidate_registry
from research.company_profile.live_plan import record_company_profile_live_plan
from research.company_profile.live_run import (
    LIVE_RUN_SCHEMA_VERSION,
    CompanyProfileLiveRunReport,
    live_run_schema_manifest,
    record_live_run_report,
    select_live_run_targets,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.runtime import COMMON_CORE_STORAGE_NAMESPACE
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


def test_live_run_module_has_no_llm_or_legacy_writer_entry():
    from research.company_profile import live_run

    source = inspect.getsource(live_run)
    assert "openai" not in source.lower()
    assert "CompanyProfileTaskService" not in source
    assert "execute_published_task" not in source
    manifest = live_run_schema_manifest()
    assert manifest["production_authorization"] == "not_authorized"
    assert manifest["whole_batch_rerun"] is False
