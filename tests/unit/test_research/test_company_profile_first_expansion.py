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
from research.company_profile.source_review import (
    SOURCE_REVIEW_SCHEMA_VERSION,
    SemanticFinding,
)


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
            "asset_id": binding.get("asset_id") or report.asset_id,
            "source_asset_id": binding.get("report_id") or binding.get("filing_id"),
            "filing_id": binding.get("filing_id") or binding.get("report_id"),
            "source_announcement_id": binding.get("filing_id") or binding.get("report_id"),
            "content_hash": (
                binding.get("document_version") or report.content_hash
            ),
            "report_period": binding.get("report_period") or report.report_period,
        }


def _persist_assessed_expansion(root, plan, registry):
    from research.company_profile.first_expansion import (
        persist_first_expansion_live_run,
        persist_first_expansion_source_review,
    )
    from research.company_profile.live_run import record_live_run_report
    from research.company_profile.source_review import record_source_review_report

    expansion = record_live_run_report(
        plan=plan.live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids,
        delivered_instrument_ids=plan.selected_instrument_ids,
        knowledge_cutoff=_CUTOFF,
        first_expansion_plan_id=plan.plan_id,
        frozen_report_references=plan.reports,
    )
    persist_first_expansion_live_run(expansion, root, plan)
    persist_first_expansion_source_review(
        record_source_review_report(
            live_run=expansion,
            semantic_findings=_assessed_findings(plan.selected_instrument_ids),
        ),
        root,
        plan,
    )
    return expansion


def _assessed_findings(instrument_ids) -> tuple[SemanticFinding, ...]:
    findings: list[SemanticFinding] = []
    for instrument_id in instrument_ids:
        findings.extend(
            SemanticFinding(
                instrument_id=instrument_id,
                aspect=aspect,
                kind="semantic",
                source="independently_read_official_report",
                disclosure_id=f"{instrument_id}-{aspect}",
                disclosed_in_source=True,
                present_in_delivery=True,
                fact_accurate=True,
                critical_numeric_error=False,
            )
            for aspect in (
                "core_skeleton",
                "important_disclosure",
                "commodity_role",
            )
        )
    return tuple(findings)


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


def test_first_expansion_refuses_when_service_is_beyond_two_company_cap():
    from research.company_profile.first_expansion import record_first_expansion_plan
    from research.company_profile.live_plan import (
        assign_disclosure_form,
        record_company_profile_live_plan,
    )
    from research.company_profile.live_run import select_live_run_targets

    instruments = [
        {"instrument_id": "600001.SH", "exchange": "SSE", "name": "制造甲"},
        {"instrument_id": "000001.SZ", "exchange": "SZSE", "name": "制造乙"},
        {"instrument_id": "430001.BJ", "exchange": "BSE", "name": "制造丙"},
        {"instrument_id": "600002.SH", "exchange": "SSE", "name": "服务丁"},
    ]
    coverage = {
        item["instrument_id"]: {"status": "available", "fiscal_year": 2025}
        for item in instruments
    }
    memberships = {
        "600001.SH": {"sw_l1_name": "有色金属", "taxonomy_system": "sw"},
        "000001.SZ": {"sw_l1_name": "有色金属", "taxonomy_system": "sw"},
        "430001.BJ": {"sw_l1_name": "有色金属", "taxonomy_system": "sw"},
        "600002.SH": {"sw_l1_name": "商贸零售", "taxonomy_system": "sw"},
    }
    reports = {
        "600001.SH": _effective_report(asset_id="asset-600001", content_hash=_HASH_A),
        "000001.SZ": _effective_report(asset_id="asset-000001", content_hash=_HASH_B),
        "430001.BJ": _effective_report(asset_id="asset-430001", content_hash=_HASH_C),
        "600002.SH": _effective_report(asset_id="asset-600002", content_hash="d" * 64),
    }
    registry = build_a_share_candidate_registry(
        as_of=_CUTOFF,
        universe_snapshot_id="snap-overflow",
        universe_coverage_guarantee="full_market",
        eligible_instruments=instruments,
        asset_coverage=coverage,
        industry_memberships=memberships,
        effective_reports=reports,
    )
    oversized = select_live_run_targets(
        registry,
        record_company_profile_live_plan(max_companies_this_round=4),
    )
    assert oversized == ("600001.SH", "000001.SZ", "430001.BJ", "600002.SH")
    assert [
        assign_disclosure_form(registry.candidate(item)) for item in oversized
    ] == ["manufacturing", "manufacturing", "manufacturing", "service"]

    with pytest.raises(ValueError, match="service"):
        record_first_expansion_plan(
            registry=registry,
            knowledge_cutoff=_CUTOFF,
            official_bindings={
                "600001.SH": {"filing_id": "filing-600001"},
                "000001.SZ": {"filing_id": "filing-000001"},
                "430001.BJ": {"filing_id": "filing-430001"},
                "600002.SH": {"filing_id": "filing-600002"},
            },
        )


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
        record_first_expansion_plan,
    )
    from research.company_profile.operator_closure import (
        persist_operator_closure_report,
        record_operator_closure_report,
    )
    from research.company_profile.publication import record_publication_control

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
    _persist_assessed_expansion(tmp_path, plan, registry)
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
        record_first_expansion_plan,
    )
    from research.company_profile.publication import record_publication_control

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path, plan)
    _persist_assessed_expansion(tmp_path, plan, registry)
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
        record_first_expansion_plan,
    )
    from research.company_profile.operations import (
        CompanyProfileTaskService,
        apply_published_publication,
    )
    from research.company_profile.publication import record_publication_control
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
    _persist_assessed_expansion(tmp_path / "checkpoints", plan, registry)
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


@pytest.mark.asyncio
async def test_active_run_refuses_when_immutable_snapshot_is_missing_or_diverges(
    tmp_path,
):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        persist_first_expansion_live_run,
        persist_first_expansion_source_review,
        record_first_expansion_plan,
    )
    from research.company_profile.live_run import record_live_run_report
    from research.company_profile.operations import (
        CompanyProfileTaskService,
        apply_published_publication,
    )
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
    apply_published_publication(
        action="enable",
        checkpoint_root=tmp_path / "checkpoints",
    )
    (
        tmp_path
        / "checkpoints"
        / "reports"
        / "first_expansion"
        / f"company_profile_first_expansion_plan.v1.{plan.plan_id}.json"
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

    drifted_reports = tuple(
        item.model_copy(update={"document_version": "d" * 64})
        if item.instrument_id == "601888.SH"
        else item
        for item in plan.reports
    )
    expansion = record_live_run_report(
        plan=plan.live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids,
        delivered_instrument_ids=plan.selected_instrument_ids,
        knowledge_cutoff=_CUTOFF,
        first_expansion_plan_id=plan.plan_id,
        frozen_report_references=drifted_reports,
    )
    with pytest.raises(ValueError, match="report"):
        persist_first_expansion_live_run(expansion, tmp_path / "checkpoints", plan)
    with pytest.raises(ValueError, match="report"):
        persist_first_expansion_source_review(
            record_source_review_report(live_run=expansion),
            tmp_path / "checkpoints",
            plan,
        )


def test_drift_refuses_when_current_asset_id_or_report_period_changes(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        official_bindings_from_shared_access,
        record_first_expansion_plan,
        refuse_first_expansion_drift,
    )

    registry = _registry()
    plan = record_first_expansion_plan(
        registry=registry,
        knowledge_cutoff=_CUTOFF,
        official_bindings=_bindings(),
    )
    activate_first_expansion(tmp_path, plan)
    drifted = dict(_bindings())
    drifted["601888.SH"] = {
        "filing_id": "filing-601888",
        "asset_id": "asset-601888-corrected",
        "report_period": "2024-12-31",
    }
    with pytest.raises(ValueError, match="report"):
        refuse_first_expansion_drift(
            plan,
            knowledge_cutoff=_CUTOFF,
            registry=registry,
            official_bindings=official_bindings_from_shared_access(
                _FakeAccess(registry, drifted),
                plan.selected_instrument_ids,
                knowledge_cutoff=_CUTOFF,
            ),
        )


@pytest.mark.asyncio
async def test_frozen_work_survives_interrupt_and_resume_marks_delivery(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        load_first_expansion_mode,
        record_first_expansion_plan,
        remember_first_expansion_work_ids,
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
    checkpoint = tmp_path / "checkpoints"
    activate_first_expansion(checkpoint, plan)
    apply_published_publication(action="enable", checkpoint_root=checkpoint)
    service = CompanyProfileTaskService(
        storage=_storage(tmp_path),
        output_root=tmp_path / "output",
        checkpoint_root=checkpoint,
        candidate_registry=registry,
        shared_asset_access=_FakeAccess(registry),
    )

    def fake_enqueue(**kwargs):
        return {
            "eligible": 2,
            "inserted": 2,
            "reused": 0,
            "work_ids": ["work-frozen-a", "work-frozen-b"],
        }

    async def boom(*args, **kwargs):
        raise RuntimeError("drain interrupted")

    service.repository.enqueue_latest_annual = fake_enqueue
    service.production._drain_stage = boom
    with pytest.raises(RuntimeError, match="interrupted"):
        await service.execute(
            "run",
            knowledge_cutoff=_CUTOFF,
            candidate_registry=registry,
        )
    assert load_first_expansion_mode(checkpoint).work_ids == (
        "work-frozen-a",
        "work-frozen-b",
    )

    remember_first_expansion_work_ids(
        checkpoint,
        work_ids=("work-frozen-a", "work-frozen-b"),
    )

    async def fake_resume(self, **kwargs):
        return {
            "action": "resume",
            "state": "completed",
            "live_run": {
                "company_outcomes": [
                    {"delivered": True} for _ in plan.selected_instrument_ids
                ]
            },
        }

    service._run = fake_resume.__get__(service, CompanyProfileTaskService)
    await service.execute(
        "resume",
        knowledge_cutoff=_CUTOFF,
        candidate_registry=registry,
    )
    assert load_first_expansion_mode(checkpoint).delivered is True
    again = await service.execute(
        "run",
        knowledge_cutoff=_CUTOFF,
        candidate_registry=registry,
    )
    assert again.get("first_expansion_idempotent") is True


@pytest.mark.asyncio
async def test_active_run_keeps_run_accounting_and_does_not_report_idle(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        load_first_expansion_mode,
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
    checkpoint = tmp_path / "checkpoints"
    activate_first_expansion(checkpoint, plan)
    apply_published_publication(action="enable", checkpoint_root=checkpoint)
    service = CompanyProfileTaskService(
        storage=_storage(tmp_path),
        output_root=tmp_path / "output",
        checkpoint_root=checkpoint,
        candidate_registry=registry,
        shared_asset_access=_FakeAccess(registry),
    )
    enqueue_calls: list[dict[str, object]] = []

    def fake_enqueue(**kwargs):
        enqueue_calls.append(dict(kwargs))
        return {
            "eligible": 2,
            "inserted": 2,
            "reused": 0,
            "work_ids": ["work-frozen-a", "work-frozen-b"],
        }

    async def boom_drain(*args, **kwargs):
        raise RuntimeError("drain interrupted")

    seen_enqueue: list[bool | None] = []
    original_run = service._run

    async def spy_run(self, **kwargs):
        seen_enqueue.append(kwargs.get("enqueue"))
        return await original_run(**kwargs)

    service.repository.enqueue_latest_annual = fake_enqueue
    service.production._drain_stage = boom_drain
    service._run = spy_run.__get__(service, CompanyProfileTaskService)
    with pytest.raises(RuntimeError, match="interrupted"):
        await service.execute(
            "run",
            knowledge_cutoff=_CUTOFF,
            candidate_registry=registry,
        )
    assert seen_enqueue == [True]
    assert len(enqueue_calls) == 1
    assert load_first_expansion_mode(checkpoint).work_ids == (
        "work-frozen-a",
        "work-frozen-b",
    )
    control = service.control.read()
    latest = control["latest_result"]
    assert control["action"] == "run"
    assert latest["action"] == "run"
    assert latest["enqueue"]["eligible"] == 2
    assert latest["enqueue"]["inserted"] == 2
    assert latest["enqueue"]["reused"] == 0
    assert latest["enqueue"]["work_ids"] == ["work-frozen-a", "work-frozen-b"]
    assert latest["state"] == "failed"
    assert latest["state"] not in {"idle", "completed"}

    async def empty_drain(*args, **kwargs):
        return {"completed": 0, "status": "ok"}

    service.production._drain_stage = empty_drain
    result = await service.execute(
        "run",
        knowledge_cutoff=_CUTOFF,
        candidate_registry=registry,
    )
    assert seen_enqueue == [True, True]
    assert result["action"] == "run"
    assert result["control"]["action"] == "run"
    assert result["enqueue"]["eligible"] == 2
    assert result["enqueue"]["inserted"] == 2
    assert result["enqueue"]["reused"] == 0
    assert result["enqueue"]["work_ids"] == ["work-frozen-a", "work-frozen-b"]
    assert result["state"] == "incomplete"
    assert result["state"] not in {"idle", "completed"}


def test_closure_v2_refuses_unassessed_or_undelivered_review(tmp_path):
    from research.company_profile.first_expansion import (
        activate_first_expansion,
        complete_first_expansion,
        persist_first_expansion_live_run,
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
    undelivered = record_live_run_report(
        plan=plan.live_plan,
        registry=registry,
        selected_instrument_ids=plan.selected_instrument_ids,
        delivered_instrument_ids=(),
        knowledge_cutoff=_CUTOFF,
        first_expansion_plan_id=plan.plan_id,
        frozen_report_references=plan.reports,
    )
    persist_first_expansion_live_run(undelivered, tmp_path, plan)
    persist_first_expansion_source_review(
        record_source_review_report(live_run=undelivered),
        tmp_path,
        plan,
    )
    with pytest.raises(ValueError, match="delivered|assessed|cover"):
        complete_first_expansion(
            tmp_path,
            publication=record_publication_control("enable"),
        )
