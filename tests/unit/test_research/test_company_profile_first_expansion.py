from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from research.company_profile.candidate_registry import (
    CANDIDATE_REGISTRY_SCHEMA_VERSION,
    build_a_share_candidate_registry,
)
from research.company_profile.live_run import LIVE_RUN_SCHEMA_VERSION, persist_live_run_report
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operator_closure import (
    OPERATOR_CLOSURE_SCHEMA_VERSION,
    load_operator_closure_report,
)
from research.company_profile.source_review import SOURCE_REVIEW_SCHEMA_VERSION


_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_CUTOFF = "2026-09-17"


def _effective_report(*, asset_id: str, content_hash: str) -> dict[str, object]:
    return {
        "asset_id": asset_id,
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "content_hash": content_hash,
        "availability": "local_valid",
        "decision_state": "effective",
        "published_at": "2026-03-31T00:00:00+00:00",
    }


def _registry(*, service: bool = True, snapshot_id: str = "snap-full"):
    instruments = [
        {"instrument_id": "000878.SZ", "exchange": "SZSE", "name": "云南铜业"},
        {"instrument_id": "601888.SH", "exchange": "SSE", "name": "中国中免"},
        {"instrument_id": "600000.SH", "exchange": "SSE", "name": "浦发银行"},
    ]
    coverage = {
        "000878.SZ": {"status": "available", "fiscal_year": 2025},
        "601888.SH": {"status": "available", "fiscal_year": 2025},
        "600000.SH": {"status": "available", "fiscal_year": 2025},
    }
    memberships = {
        "000878.SZ": {"sw_l1_name": "有色金属", "taxonomy_system": "sw"},
        "601888.SH": {
            "sw_l1_name": "社会服务" if service else "有色金属",
            "taxonomy_system": "sw",
        },
        "600000.SH": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
    }
    reports = {
        "000878.SZ": _effective_report(asset_id="asset-000878", content_hash=_HASH_A),
        "601888.SH": _effective_report(asset_id="asset-601888", content_hash=_HASH_B),
        "600000.SH": _effective_report(asset_id="asset-600000", content_hash=_HASH_C),
    }
    return build_a_share_candidate_registry(
        as_of=_CUTOFF,
        universe_snapshot_id=snapshot_id,
        universe_coverage_guarantee="full_market",
        eligible_instruments=instruments,
        asset_coverage=coverage,
        industry_memberships=memberships,
        effective_reports=reports,
    )


def _bindings() -> dict[str, dict[str, str]]:
    return {
        "000878.SZ": {"filing_id": "filing-000878"},
        "601888.SH": {"filing_id": "filing-601888"},
        "600000.SH": {"filing_id": "filing-600000"},
    }


class _FakeAccess:
    def __init__(self, registry, bindings=None):
        self._registry = registry
        self._bindings = bindings or _bindings()

    def get_effective_asset(self, instrument_id, **kwargs):
        candidate = self._registry.candidate(instrument_id)
        report = candidate.latest_effective_annual_report
        binding = self._bindings[instrument_id]
        return {
            "asset_id": report.asset_id,
            "source_asset_id": binding.get("report_id") or binding.get("filing_id"),
            "filing_id": binding.get("filing_id") or binding.get("report_id"),
            "source_announcement_id": binding.get("filing_id") or binding.get("report_id"),
            "content_hash": (
                binding.get("document_version") or report.content_hash
            ),
            "report_period": report.report_period,
        }


def test_mode_defaults_to_inactive_without_a_snapshot(tmp_path):
    from research.company_profile.first_expansion import load_first_expansion_mode

    state = load_first_expansion_mode(tmp_path)
    assert state.mode == "inactive"
    assert state.production_authorization == PRODUCTION_AUTHORIZATION
    assert state.plan_id is None


def test_activation_requires_service_stratum_and_official_report_versions(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        record_first_expansion_plan,
    )

    with pytest.raises(ValueError, match="service"):
        record_first_expansion_plan(
            registry=_registry(service=False),
            knowledge_cutoff=_CUTOFF,
            official_bindings=_bindings(),
        )
    with pytest.raises(ValueError, match="document_version|unknown|report_id"):
        record_first_expansion_plan(
            registry=_registry(),
            knowledge_cutoff=_CUTOFF,
            official_bindings={
                "000878.SZ": {"filing_id": "filing-000878"},
                "601888.SH": {"document_version": "unknown"},
                "600000.SH": {"filing_id": "filing-600000"},
            },
        )

    plan = record_first_expansion_plan(
        registry=_registry(),
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    assert "service" in {item.disclosure_form for item in plan.selected_strata}
    assert plan.knowledge_cutoff == _CUTOFF
    assert plan.registry.schema_version == CANDIDATE_REGISTRY_SCHEMA_VERSION
    assert plan.registry.as_of == _CUTOFF
    assert plan.registry.universe_snapshot_id == "snap-full"
    assert all(item.document_version != "unknown" for item in plan.reports)
    assert {item.instrument_id: item.report_id for item in plan.reports}[
        "601888.SH"
    ] == "filing-601888"

    state = activate_first_expansion(tmp_path, plan)
    assert state.mode == "active"
    assert state.plan_id == plan.plan_id
    stored = json.loads(
        (tmp_path / "reports" / "company_profile_first_expansion_plan.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert stored["plan_id"] == plan.plan_id


def test_active_mode_refuses_cutoff_registry_or_report_drift(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        record_first_expansion_plan,
        refuse_first_expansion_drift,
    )

    plan = record_first_expansion_plan(
        registry=_registry(),
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path, plan)
    with pytest.raises(ValueError, match="knowledge_cutoff"):
        refuse_first_expansion_drift(
            plan,
            knowledge_cutoff="2026-09-18",
            registry=_registry(),
            official_bindings=_bindings(),
        )
    with pytest.raises(ValueError, match="universe_snapshot"):
        refuse_first_expansion_drift(
            plan,
            knowledge_cutoff=_CUTOFF,
            registry=_registry(snapshot_id="snap-other"),
            official_bindings=_bindings(),
        )
    drifted = dict(_bindings())
    drifted["601888.SH"] = {"filing_id": "filing-601888-corrected"}
    with pytest.raises(ValueError, match="report"):
        refuse_first_expansion_drift(
            plan,
            knowledge_cutoff=_CUTOFF,
            registry=_registry(),
            official_bindings=drifted,
        )


def test_inactive_run_does_not_require_expansion_snapshot(tmp_path):
    from research.company_profile.first_expansion import (
        first_expansion_should_constrain_run,
        load_first_expansion_mode,
    )

    assert load_first_expansion_mode(tmp_path).mode == "inactive"
    assert first_expansion_should_constrain_run(tmp_path) is False


@pytest.mark.asyncio
async def test_active_resume_stays_on_frozen_work_and_rerun_is_idempotent(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        load_first_expansion_mode,
        mark_first_expansion_delivery,
        record_first_expansion_plan,
        remember_first_expansion_work_ids,
    )
    from research.company_profile.operations import CompanyProfileTaskService
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )

    plan = record_first_expansion_plan(
        registry=_registry(),
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path / "checkpoints", plan)
    remember_first_expansion_work_ids(
        tmp_path / "checkpoints",
        work_ids=("work-frozen-a", "work-frozen-b"),
    )
    from research.company_profile.operations import apply_published_publication

    apply_published_publication(
        action="enable",
        checkpoint_root=tmp_path / "checkpoints",
    )
    storage = _storage(tmp_path)
    registry = _registry()
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        candidate_registry=registry,
        shared_asset_access=_FakeAccess(registry),
    )
    seen: list[tuple[str, ...] | None] = []

    async def fake_run(self, **kwargs):
        seen.append(kwargs.get("include_work_ids") or kwargs.get("instrument_ids"))
        return {"action": kwargs.get("enqueue") and "run" or "resume", "state": "idle"}

    service._run = fake_run.__get__(service, CompanyProfileTaskService)
    await service.execute(
        "resume",
        knowledge_cutoff=_CUTOFF,
        candidate_registry=_registry(),
    )
    state = load_first_expansion_mode(tmp_path / "checkpoints")
    assert state.work_ids == ("work-frozen-a", "work-frozen-b")
    mark_first_expansion_delivery(tmp_path / "checkpoints")
    again = await service.execute(
        "run",
        knowledge_cutoff=_CUTOFF,
        candidate_registry=_registry(),
    )
    assert again["state"] == "idle"
    assert again.get("first_expansion_idempotent") is True


def test_expansion_snapshots_do_not_overwrite_baseline_v1(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        persist_first_expansion_live_run,
        persist_first_expansion_source_review,
        record_first_expansion_plan,
    )
    from research.company_profile.live_plan import record_company_profile_live_plan
    from research.company_profile.live_run import record_live_run_report
    from research.company_profile.source_review import record_source_review_report

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path, plan)
    live_plan = record_company_profile_live_plan(
        max_companies_this_round=len(plan.selected_instrument_ids)
    )
    baseline = record_live_run_report(
        plan=live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids[:1],
        delivered_instrument_ids=plan.selected_instrument_ids[:1],
        knowledge_cutoff=_CUTOFF,
    )
    persist_live_run_report(baseline, tmp_path)
    expansion = record_live_run_report(
        plan=plan.live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids,
        delivered_instrument_ids=plan.selected_instrument_ids,
        knowledge_cutoff=_CUTOFF,
        first_expansion_plan_id=plan.plan_id,
        frozen_report_references=plan.reports,
    )
    expansion_path = persist_first_expansion_live_run(expansion, tmp_path, plan)
    review = record_source_review_report(live_run=expansion)
    review_path = persist_first_expansion_source_review(review, tmp_path, plan)
    baseline_live = tmp_path / "reports" / f"{LIVE_RUN_SCHEMA_VERSION}.json"
    baseline_review = tmp_path / "reports" / f"{SOURCE_REVIEW_SCHEMA_VERSION}.json"
    assert expansion_path != baseline_live
    assert review_path != baseline_review
    assert json.loads(baseline_live.read_text(encoding="utf-8"))[
        "selected_instrument_ids"
    ] == list(plan.selected_instrument_ids[:1])
    assert not baseline_review.exists()
    assert expansion.first_expansion_plan_id == plan.plan_id


def test_operator_closure_v2_keeps_v1_readable_and_uses_post_execution_text(
    tmp_path,
):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        complete_first_expansion,
        load_first_expansion_mode,
        persist_first_expansion_source_review,
        record_first_expansion_plan,
    )
    from research.company_profile.live_run import record_live_run_report
    from research.company_profile.operator_closure import (
        persist_operator_closure_report,
        record_operator_closure_report,
    )
    from research.company_profile.publication import record_publication_control
    from research.company_profile.source_review import record_source_review_report

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path, plan)
    publication = record_publication_control("enable")
    v1 = record_operator_closure_report(publication)
    persist_operator_closure_report(v1, tmp_path)
    expansion = record_live_run_report(
        plan=plan.live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids,
        delivered_instrument_ids=plan.selected_instrument_ids,
        knowledge_cutoff=_CUTOFF,
        first_expansion_plan_id=plan.plan_id,
        frozen_report_references=plan.reports,
    )
    persist_first_expansion_source_review(
        record_source_review_report(live_run=expansion),
        tmp_path,
        plan,
    )
    v2 = complete_first_expansion(tmp_path, publication=publication)
    loaded_v1 = load_operator_closure_report(tmp_path)
    assert loaded_v1 is not None
    assert loaded_v1.schema_version == OPERATOR_CLOSURE_SCHEMA_VERSION
    assert any(
        item.item_id == "first_expansion_gates_unmet" for item in loaded_v1.m4_backlog
    )
    assert (tmp_path / "reports" / f"{OPERATOR_CLOSURE_SCHEMA_VERSION}.json").is_file()
    assert v2.schema_version == "company_profile_operator_closure.v2"
    assert all(
        item.item_id != "first_expansion_gates_unmet" for item in v2.m4_backlog
    )
    replacement = next(
        item
        for item in v2.m4_backlog
        if item.item_id == "first_expansion_executed_not_next_round"
    )
    assert "has been executed" in replacement.reason
    assert "does not authorize the next expansion" in replacement.reason
    assert load_first_expansion_mode(tmp_path).mode == "completed"
    from research.company_profile.first_expansion import (
        first_expansion_should_constrain_run,
    )

    assert first_expansion_should_constrain_run(tmp_path) is False
    assert v2.production_authorization == PRODUCTION_AUTHORIZATION
    assert v2.legacy_writer_enabled is False
    assert v2.dcf_authorized is False
    assert v2.trading_authorized is False


def test_completed_mode_does_not_reactivate_from_ordinary_run(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        complete_first_expansion,
        first_expansion_should_constrain_run,
        load_first_expansion_mode,
        persist_first_expansion_source_review,
        record_first_expansion_plan,
    )
    from research.company_profile.live_run import record_live_run_report
    from research.company_profile.publication import record_publication_control
    from research.company_profile.source_review import record_source_review_report

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path, plan)
    expansion = record_live_run_report(
        plan=plan.live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids,
        delivered_instrument_ids=plan.selected_instrument_ids,
        knowledge_cutoff=_CUTOFF,
        first_expansion_plan_id=plan.plan_id,
        frozen_report_references=plan.reports,
    )
    persist_first_expansion_source_review(
        record_source_review_report(live_run=expansion),
        tmp_path,
        plan,
    )
    complete_first_expansion(tmp_path, publication=record_publication_control("enable"))
    with pytest.raises(ValueError, match="completed"):
        activate_first_expansion(tmp_path, plan)
    assert load_first_expansion_mode(tmp_path).mode == "completed"
    assert first_expansion_should_constrain_run(tmp_path) is False


@pytest.mark.asyncio
async def test_active_run_refuses_missing_snapshot_or_report_drift(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        record_first_expansion_plan,
    )
    from research.company_profile.operations import (
        CompanyProfileTaskService,
        apply_published_publication,
    )
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path / "checkpoints", plan)
    apply_published_publication(
        action="enable",
        checkpoint_root=tmp_path / "checkpoints",
    )
    (
        tmp_path
        / "checkpoints"
        / "reports"
        / "company_profile_first_expansion_plan.v1.json"
    ).unlink()
    service = CompanyProfileTaskService(
        storage=_storage(tmp_path),
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        candidate_registry=registry,
        shared_asset_access=_FakeAccess(registry),
    )
    with pytest.raises(ValueError, match="snapshot"):
        await service.execute(
            "run",
            knowledge_cutoff=_CUTOFF,
            candidate_registry=registry,
        )

    activate_first_expansion(tmp_path / "checkpoints", plan)
    drifted = dict(_bindings())
    drifted["601888.SH"] = {"filing_id": "filing-601888-corrected"}
    drifted_service = CompanyProfileTaskService(
        storage=_storage(tmp_path / "drift"),
        output_root=tmp_path / "drift-output",
        checkpoint_root=tmp_path / "checkpoints",
        candidate_registry=registry,
        shared_asset_access=_FakeAccess(registry, drifted),
    )
    with pytest.raises(ValueError, match="report"):
        await drifted_service.execute(
            "run",
            knowledge_cutoff=_CUTOFF,
            candidate_registry=registry,
        )


@pytest.mark.asyncio
async def test_completed_run_does_not_use_frozen_enqueue(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        complete_first_expansion,
        first_expansion_should_constrain_run,
        persist_first_expansion_source_review,
        record_first_expansion_plan,
    )
    from research.company_profile.live_run import record_live_run_report
    from research.company_profile.operations import (
        CompanyProfileTaskService,
        apply_published_publication,
    )
    from research.company_profile.publication import record_publication_control
    from research.company_profile.source_review import record_source_review_report
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path / "checkpoints", plan)
    persist_first_expansion_source_review(
        record_source_review_report(
            live_run=record_live_run_report(
                plan=plan.live_plan,
                registry=registry,
                selected_instrument_ids=plan.selected_instrument_ids,
                delivered_instrument_ids=plan.selected_instrument_ids,
                knowledge_cutoff=_CUTOFF,
                first_expansion_plan_id=plan.plan_id,
                frozen_report_references=plan.reports,
            )
        ),
        tmp_path / "checkpoints",
        plan,
    )
    apply_published_publication(
        action="enable",
        checkpoint_root=tmp_path / "checkpoints",
    )
    complete_first_expansion(
        tmp_path / "checkpoints",
        publication=record_publication_control("enable"),
    )
    service = CompanyProfileTaskService(
        storage=_storage(tmp_path),
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        candidate_registry=registry,
        shared_asset_access=_FakeAccess(registry),
    )
    seen: list[str] = []

    async def fake_run_live(self, **kwargs):
        seen.append("ordinary")
        return {"action": "run", "state": "idle", "ordinary_path": True}

    service._run_live = fake_run_live.__get__(service, CompanyProfileTaskService)
    result = await service.execute(
        "run",
        knowledge_cutoff=_CUTOFF,
        candidate_registry=registry,
    )
    assert first_expansion_should_constrain_run(tmp_path / "checkpoints") is False
    assert seen == ["ordinary"]
    assert result.get("ordinary_path") is True
    assert result.get("first_expansion_idempotent") is not True


def test_official_two_company_baseline_json_still_loads_without_expansion_fields():
    from research.company_profile.live_run import CompanyProfileLiveRunReport
    from research.company_profile.source_review import CompanyProfileSourceReviewReport

    live_path = Path(
        "data/checkpoints/company_profile_common_core/reports/"
        "company_profile_live_run.v1.json"
    )
    review_path = Path(
        "data/checkpoints/company_profile_common_core/reports/"
        "company_profile_source_review.v1.json"
    )
    if not live_path.is_file() or not review_path.is_file():
        pytest.skip("official two-company baseline files are not in this workspace")
    live_run = CompanyProfileLiveRunReport.model_validate_json(
        live_path.read_text(encoding="utf-8")
    )
    review = CompanyProfileSourceReviewReport.model_validate_json(
        review_path.read_text(encoding="utf-8")
    )
    assert live_run.selected_instrument_ids == ("302132.SZ", "600000.SH")
    assert live_run.first_expansion_plan_id is None
    assert live_run.frozen_report_references == ()
    assert live_run.scale_quality_claim_allowed is False
    assert review.live_run.selected_instrument_ids == live_run.selected_instrument_ids
    assert review.expansion_gates_met is True
    assert review.scale_quality_claim_allowed is False
