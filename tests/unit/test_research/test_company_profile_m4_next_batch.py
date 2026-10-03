from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.company_profile.candidate_registry import (
    AShareCandidateRegistry,
    AShareProfileCandidate,
    ClassificationInfo,
    LatestAnnualReport,
)
from research.company_profile.contracts import SemanticProviderError
from research.company_profile.execution import (
    DEFAULT_TOTAL_TOKEN_BUDGET,
    ledger_budget_exhausted,
)
from research.company_profile.m4_next_batch import (
    M4NextBatchOutcome,
    M4NextBatchPlan,
    M4NextBatchReport,
    drift_reason,
    historical_snapshot_paths,
    load_m4_next_batch_observation,
    load_m4_next_batch_observation_for_source_review,
    load_m4_next_batch_plan,
    merge_outcome,
    save_m4_next_batch_observation,
    save_m4_next_batch_plan,
    snapshot_bytes,
    tokens_consumed_by_call,
)
from research.company_profile.operations import record_published_source_review
from research.company_profile.runtime import CompanyProfileStageRuntime
from research.company_profile.source_review import SemanticFinding
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_company_profile_operations import _service
from tests.unit.test_research.test_company_profile_runtime import (
    _RequestBoundOverviewProvider,
)


def _plan() -> M4NextBatchPlan:
    return M4NextBatchPlan(
        plan_id="batch-fixture",
        knowledge_cutoff="2026-08-30",
        token_budget=50_000,
        reports=(
            M4NextBatchReport(
                instrument_id="600100.SH",
                asset_id="asset-service",
                report_id="report-service",
                report_period="2025-12-31",
                document_version="ver-service",
                disclosure_form="service",
            ),
            M4NextBatchReport(
                instrument_id="000100.SZ",
                asset_id="asset-mfg",
                report_id="report-mfg",
                report_period="2025-12-31",
                document_version="ver-mfg",
                disclosure_form="manufacturing",
            ),
        ),
    )


def _binding(report: M4NextBatchReport, **overrides: str) -> dict[str, str]:
    payload = {
        "asset_id": report.asset_id,
        "report_id": report.report_id,
        "report_period": report.report_period,
        "document_version": report.document_version,
    }
    payload.update(overrides)
    return payload


class _Assets:
    def __init__(self, bindings: dict[str, dict[str, str]]) -> None:
        self.bindings = bindings

    def get_effective_asset(self, instrument_id: str, knowledge_cutoff: str):
        del knowledge_cutoff
        return self.bindings.get(instrument_id)


def _seed_history(root: Path) -> dict[str, bytes]:
    reports = root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    payloads = {
        "company_profile_first_expansion_mode.v1.json": b'{"mode":"inactive"}\n',
        "company_profile_first_expansion_plan.v1.json": b'{"plan":"old"}\n',
        "company_profile_operator_closure.v2.json": b'{"closure":"v2"}\n',
        "company_profile_live_run.v1.json": b'{"live":"baseline"}\n',
        "company_profile_source_review.v1.json": b'{"review":"8/9"}\n',
    }
    for name, payload in payloads.items():
        (reports / name).write_bytes(payload)
    return snapshot_bytes(historical_snapshot_paths(root))


def _prepare(tmp_path, plan: M4NextBatchPlan, bindings: dict[str, dict[str, str]]):
    storage = _storage(tmp_path)
    service = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
    )
    save_m4_next_batch_plan(service.checkpoint_root, plan)
    service.repository.shared_asset_access = _Assets(bindings)
    seen: dict[str, object] = {}

    def enqueue(**kwargs):
        seen["instrument_ids"] = tuple(kwargs.get("instrument_ids") or ())
        return {"eligible": 0, "inserted": 0, "reused": 0, "work_ids": []}

    service.repository.enqueue_latest_annual = enqueue
    return service, seen


def test_drift_refuses_one_security_and_keeps_the_other(tmp_path):
    plan = _plan()
    service_report, manufacturing = plan.reports
    service, seen = _prepare(
        tmp_path,
        plan,
        {
            service_report.instrument_id: _binding(service_report),
            manufacturing.instrument_id: _binding(
                manufacturing,
                document_version="ver-drifted",
            ),
        },
    )
    observation = load_m4_next_batch_observation(service.checkpoint_root, plan)
    observation = merge_outcome(
        observation,
        M4NextBatchOutcome(
            instrument_id=service_report.instrument_id,
            status="completed",
            tokens_consumed=80,
            reused_scope=False,
        ),
    )
    save_m4_next_batch_observation(service.checkpoint_root, observation)
    before = _seed_history(service.checkpoint_root)

    asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff=plan.knowledge_cutoff,
            instrument_ids=[manufacturing.instrument_id],
            token_budget=50_000,
        )
    )
    merged = load_m4_next_batch_observation_for_source_review(service.checkpoint_root)

    assert "instrument_ids" not in seen
    assert merged.outcome_for(service_report.instrument_id).status == "completed"
    assert merged.outcome_for(service_report.instrument_id).tokens_consumed == 80
    refused = merged.outcome_for(manufacturing.instrument_id)
    assert refused.status == "refused"
    assert refused.reason == "document_version_drift"
    assert snapshot_bytes(historical_snapshot_paths(service.checkpoint_root)) == before


def test_two_runs_keep_both_outcomes_including_a_failure(tmp_path):
    plan = _plan()
    service_report, manufacturing = plan.reports
    service, _seen = _prepare(
        tmp_path,
        plan,
        {
            service_report.instrument_id: _binding(service_report),
            manufacturing.instrument_id: _binding(manufacturing),
        },
    )

    async def fail_drain(*_args, **_kwargs):
        from research.company_profile.runtime import _WorkState

        ledger = _WorkState(work_id="work")
        ledger.tokens_used = 120
        service.runtime._states["work"] = ledger
        raise RuntimeError("provider failed")

    service.production._drain_stage = fail_drain
    service.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 1,
        "inserted": 1,
        "reused": 0,
        "work_ids": ["work"],
    }
    with pytest.raises(RuntimeError, match="provider failed"):
        asyncio.run(
            service.execute(
                "run",
                knowledge_cutoff=plan.knowledge_cutoff,
                instrument_ids=[service_report.instrument_id],
            )
        )

    service.runtime._states.clear()

    async def noop_drain(*_args, **_kwargs):
        return {"status": "completed"}

    service.production._drain_stage = noop_drain
    service.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 1,
        "inserted": 0,
        "reused": 1,
        "work_ids": [],
    }
    asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff=plan.knowledge_cutoff,
            instrument_ids=[manufacturing.instrument_id],
        )
    )
    merged = load_m4_next_batch_observation_for_source_review(service.checkpoint_root)

    assert merged.outcome_for(service_report.instrument_id).status == "failed"
    assert merged.outcome_for(service_report.instrument_id).tokens_consumed == 120
    assert merged.outcome_for(manufacturing.instrument_id).status == "incomplete"
    assert merged.outcome_for(manufacturing.instrument_id).reused_scope is True
    assert merged.outcome_for(manufacturing.instrument_id).tokens_consumed == 0
    assert merged.tokens_consumed == 120
    assert service.runtime._total_token_budget == 49_880
    assert service.runtime.provider._total_token_budget == 49_880


def test_paused_run_keeps_paused_status(tmp_path):
    plan = _plan()
    report = plan.reports[0]
    service, _seen = _prepare(
        tmp_path,
        plan,
        {report.instrument_id: _binding(report)},
    )
    service._should_stop_run = lambda: True
    service.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 1,
        "inserted": 1,
        "reused": 0,
        "work_ids": ["paused-work"],
    }
    asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff=plan.knowledge_cutoff,
            instrument_ids=[report.instrument_id],
        )
    )
    observation = load_m4_next_batch_observation(service.checkpoint_root, plan)
    assert observation.outcome_for(report.instrument_id).status == "paused"


def test_drift_reason_covers_cutoff_and_each_identity_field():
    plan = _plan()
    report = plan.reports[0]
    binding = _binding(report)
    assert (
        drift_reason(
            plan,
            report,
            knowledge_cutoff="2026-01-01",
            binding=binding,
        )
        == "cutoff_drift"
    )
    for field in ("asset_id", "report_id", "report_period", "document_version"):
        drifted = dict(binding)
        drifted[field] = "other"
        assert (
            drift_reason(
                plan,
                report,
                knowledge_cutoff=plan.knowledge_cutoff,
                binding=drifted,
            )
            == f"{field}_drift"
        )


def test_reused_scope_adds_no_consumption_and_failure_does():
    assert tokens_consumed_by_call(tokens_used=300, reused_scope=False) == 300
    assert tokens_consumed_by_call(tokens_used=300, reused_scope=True) == 0


def test_remaining_budget_reaches_runtime_and_provider():
    runtime = CompanyProfileStageRuntime(
        writer=object(),
        provider=_RequestBoundOverviewProvider(),
        token_budget=50_000,
    )
    runtime.apply_token_budget(49_880)

    assert runtime._total_token_budget == 49_880
    assert runtime.provider._total_token_budget == 49_880


def test_ordinary_run_without_a_batch_plan_keeps_the_runtime_limit(tmp_path):
    storage = _storage(tmp_path)
    service = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
    )
    service.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 0,
        "inserted": 0,
        "reused": 0,
        "work_ids": [],
    }

    async def noop_drain(*_args, **_kwargs):
        return {"status": "completed"}

    service.production._drain_stage = noop_drain

    asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-08-30",
            instrument_ids=["600000.SH"],
            token_budget=10,
        )
    )

    assert service.token_budget == 10
    assert service.runtime._total_token_budget == DEFAULT_TOTAL_TOKEN_BUDGET
    assert service.runtime.provider._total_token_budget == DEFAULT_TOTAL_TOKEN_BUDGET
    assert load_m4_next_batch_plan(service.checkpoint_root) is None


def _candidate(instrument_id: str, exchange: str, industry: str, asset_id: str):
    return AShareProfileCandidate(
        instrument_id=instrument_id,
        exchange=exchange,
        company_name=instrument_id,
        universe_status="eligible",
        classification_status="present",
        classification=ClassificationInfo(sw_l1_name=industry),
        asset_status="available",
        latest_effective_annual_report=LatestAnnualReport(
            asset_id=asset_id,
            fiscal_year=2025,
            report_period="2025-12-31",
            availability="available",
            decision_state="effective",
            published_at="2026-03-20T00:00:00+00:00",
        ),
    )


def _registry(plan: M4NextBatchPlan) -> AShareCandidateRegistry:
    industries = {"service": "交通运输", "manufacturing": "汽车"}
    exchanges = {"SH": "SSE", "SZ": "SZSE"}
    candidates = tuple(
        _candidate(
            report.instrument_id,
            exchanges[report.instrument_id.split(".")[-1]],
            industries[report.disclosure_form],
            report.asset_id,
        )
        for report in plan.reports
    )
    return AShareCandidateRegistry(
        as_of=plan.knowledge_cutoff,
        candidates=candidates,
        counts={
            "total": 2,
            "eligible": 2,
            "asset_not_available": 0,
            "classification_missing": 0,
        },
    )


def test_registry_runs_keep_both_companies_and_source_review_reads_them(tmp_path):
    plan = _plan()
    service_report, manufacturing = plan.reports
    service, _seen = _prepare(
        tmp_path,
        plan,
        {
            service_report.instrument_id: _binding(service_report),
            manufacturing.instrument_id: _binding(manufacturing),
        },
    )
    before = _seed_history(service.checkpoint_root)
    registry = _registry(plan)

    def enqueue(**kwargs):
        return {"eligible": 0, "inserted": 0, "reused": 0, "work_ids": []}

    service.repository.enqueue_latest_annual = enqueue
    for report in plan.reports:
        asyncio.run(
            service.execute(
                "run",
                knowledge_cutoff=plan.knowledge_cutoff,
                instrument_ids=[report.instrument_id],
                candidate_registry=registry,
            )
        )
    observation = load_m4_next_batch_observation_for_source_review(
        service.checkpoint_root
    )
    live_path = (
        service.checkpoint_root
        / "reports"
        / "m4_next_small_batch"
        / plan.plan_id
        / "company_profile_live_run.v1.json"
    )
    assert live_path.is_file()
    assert [item.status for item in observation.outcomes] == ["idle", "idle"]
    review = record_published_source_review(
        checkpoint_root=service.checkpoint_root,
        semantic_findings=[
            SemanticFinding(
                instrument_id=service_report.instrument_id,
                aspect="core_skeleton",
                kind="semantic",
                source="independently_read_official_report",
                disclosure_id="service-overview",
                disclosed_in_source=True,
                present_in_delivery=False,
                fact_accurate=None,
                critical_numeric_error=False,
            ),
            SemanticFinding(
                instrument_id=manufacturing.instrument_id,
                aspect="core_skeleton",
                kind="semantic",
                source="independently_read_official_report",
                disclosure_id="manufacturing-overview",
                disclosed_in_source=True,
                present_in_delivery=False,
                fact_accurate=None,
                critical_numeric_error=False,
            ),
        ],
    )
    assert set(review["source_review"]["live_run"]["selected_instrument_ids"]) == {
        service_report.instrument_id,
        manufacturing.instrument_id,
    }
    assert review["source_review_path"].endswith(
        f"m4_next_small_batch/{plan.plan_id}/company_profile_source_review.v1.json"
    )
    assert snapshot_bytes(historical_snapshot_paths(service.checkpoint_root)) == before

    other = tmp_path / "missing-company"
    only_one = _service(other, _storage(other))
    save_m4_next_batch_plan(only_one.checkpoint_root, plan)
    with pytest.raises(ValueError, match="both company outcomes"):
        record_published_source_review(checkpoint_root=only_one.checkpoint_root)


def test_cumulative_tokens_survive_retry_reuse_and_a_restored_ledger(tmp_path):
    plan = _plan()
    report = plan.reports[0]
    service, _seen = _prepare(
        tmp_path,
        plan,
        {report.instrument_id: _binding(report)},
    )

    def spend(amount: int) -> None:
        added = False

        async def drain(*_args, **_kwargs):
            nonlocal added
            state = service.runtime._states.get("work")
            if state is None:
                from research.company_profile.runtime import _WorkState

                state = _WorkState(work_id="work")
                service.runtime._states["work"] = state
            if not added:
                state.tokens_used += amount
                added = True
            return {"status": "completed"}

        service.production._drain_stage = drain
        service.repository.enqueue_latest_annual = lambda **_kwargs: {
            "eligible": 1,
            "inserted": 1,
            "reused": 0,
            "work_ids": ["work"],
        }
        asyncio.run(
            service.execute(
                "run",
                knowledge_cutoff=plan.knowledge_cutoff,
                instrument_ids=[report.instrument_id],
            )
        )

    spend(120)
    spend(50)
    observation = load_m4_next_batch_observation(service.checkpoint_root, plan)
    assert observation.outcome_for(report.instrument_id).tokens_consumed == 170

    restored = _service(
        tmp_path,
        _storage(tmp_path / "restored"),
        provider=_RequestBoundOverviewProvider(),
    )
    save_m4_next_batch_plan(restored.checkpoint_root, plan)
    save_m4_next_batch_observation(restored.checkpoint_root, observation)
    restored.repository.shared_asset_access = service.repository.shared_asset_access
    restored.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 1,
        "inserted": 0,
        "reused": 0,
        "work_ids": ["old"],
    }

    async def restore_ledger(*_args, **_kwargs):
        from research.company_profile.runtime import _WorkState

        ledger = _WorkState(work_id="old")
        ledger.tokens_restored = 170
        ledger.tokens_used = 170
        restored.runtime._states["old"] = ledger
        return {"status": "completed"}

    restored.production._drain_stage = restore_ledger
    asyncio.run(
        restored.execute(
            "run",
            knowledge_cutoff=plan.knowledge_cutoff,
            instrument_ids=[report.instrument_id],
        )
    )
    kept = load_m4_next_batch_observation(restored.checkpoint_root, plan)
    assert kept.tokens_consumed == 170
    assert restored.runtime._total_token_budget == 49_830
    assert restored.runtime.provider._total_token_budget == 49_830


def _named(instrument_id: str, exchange: str, industry: str, *, readable: bool = True):
    return SimpleNamespace(
        instrument_id=instrument_id,
        exchange=exchange,
        universe_status="eligible",
        asset_status="available" if readable else "confirmed_missing",
        classification_status="present",
        classification=SimpleNamespace(sw_l1_name=industry),
        latest_effective_annual_report=SimpleNamespace(
            content_hash=f"hash-{instrument_id}",
            asset_id=f"asset-{instrument_id}",
            report_period="2025-12-31",
        ),
    )


def test_freeze_selects_in_exchange_order_and_keeps_the_same_bytes(tmp_path):
    candidates = (
        _named("600004.SH", "SSE", "交通运输"),
        _named("600100.SH", "SSE", "交通运输"),
        _named("600200.SH", "SSE", "交通运输"),
        _named("000001.SZ", "SZSE", "社会服务"),
        _named("600050.SH", "SSE", "汽车", readable=False),
        _named("600300.SH", "SSE", "汽车"),
        _named("830001.BJ", "BSE", "机械设备"),
    )
    bindings = {
        item.instrument_id: {
            "asset_id": f"asset-{item.instrument_id}",
            "report_id": f"report-{item.instrument_id}",
            "report_period": "2025-12-31",
            "document_version": f"ver-{item.instrument_id}",
        }
        for item in candidates
    }
    storage = _storage(tmp_path)
    service = _service(tmp_path, storage, provider=_RequestBoundOverviewProvider())
    before = _seed_history(service.checkpoint_root)
    registry = SimpleNamespace(candidates=candidates)

    def readable(candidate) -> bool:
        return candidate.asset_status == "available"

    plan = service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=registry,
        delivered_ids={"600100.SH"},
        readable=readable,
        binding_for=bindings.get,
    )
    loaded = load_m4_next_batch_plan(service.checkpoint_root)
    path = (
        service.checkpoint_root
        / "reports"
        / "m4_next_small_batch"
        / plan.plan_id
        / "plan.json"
    )
    raw = path.read_bytes()

    again = service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=registry,
        delivered_ids={"600100.SH"},
        readable=readable,
        binding_for=bindings.get,
    )

    assert loaded == plan == again
    assert path.read_bytes() == raw
    assert [item.instrument_id for item in plan.reports] == ["600200.SH", "600300.SH"]
    assert [item.disclosure_form for item in plan.reports] == ["service", "manufacturing"]
    assert plan.max_companies_this_round == 2
    assert plan.token_budget == 50_000
    assert plan.knowledge_cutoff == "2026-09-17"
    assert not (
        service.checkpoint_root / "reports" / "m4_next_small_batch" / plan.plan_id / "observation.json"
    ).exists()
    assert snapshot_bytes(historical_snapshot_paths(service.checkpoint_root)) == before
    with pytest.raises(ValueError, match="cannot be replaced"):
        save_m4_next_batch_plan(
            service.checkpoint_root,
            plan.model_copy(update={"knowledge_cutoff": "2026-10-02"}),
        )
    assert path.read_bytes() == raw


def test_explicit_directory_freeze_does_not_replace_the_old_plan(tmp_path):
    candidates = (
        _named("600100.SH", "SSE", "房地产"),
        _named("600010.SH", "SSE", "钢铁"),
        _named("600200.SH", "SSE", "社会服务"),
        _named("000300.SZ", "SZSE", "钢铁"),
    )
    bindings = {
        item.instrument_id: {
            "asset_id": f"asset-{item.instrument_id}",
            "report_id": f"report-{item.instrument_id}",
            "report_period": "2025-12-31",
            "document_version": f"ver-{item.instrument_id}",
        }
        for item in candidates
    }
    storage = _storage(tmp_path)
    old_service = _service(tmp_path, storage, provider=_RequestBoundOverviewProvider())
    old = old_service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids=set(),
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    old_path = (
        old_service.checkpoint_root
        / "reports"
        / "m4_next_small_batch"
        / old.plan_id
        / "plan.json"
    )
    before = old_path.read_bytes()
    new_dir = tmp_path / "m4-v3-next"
    service = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
        plan_directory=new_dir,
    )
    plan = service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids={"600100.SH", "600010.SH"},
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    again = service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids={"600100.SH", "600010.SH"},
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    loaded = load_m4_next_batch_plan(
        service.checkpoint_root, plan_directory=new_dir
    )
    assert plan == again == loaded
    assert plan.plan_id != old.plan_id
    assert {item.instrument_id for item in plan.reports} == {
        "600200.SH",
        "000300.SZ",
    }
    assert (new_dir / "plan.json").is_file()
    assert old_path.read_bytes() == before
    assert load_m4_next_batch_plan(service.checkpoint_root).plan_id == old.plan_id


def test_v6_unseen_freeze_skips_companies_observed_under_older_identities(tmp_path):
    """Same-identity delivery misses 600007 and 600010. The caller passes them."""

    candidates = (
        _named("600000.SH", "SSE", "银行"),
        _named("600007.SH", "SSE", "环保"),
        _named("600008.SH", "SSE", "环保"),
        _named("600011.SH", "SSE", "环保"),
        _named("600010.SH", "SSE", "钢铁"),
        _named("600019.SH", "SSE", "钢铁"),
        _named("600022.SH", "SSE", "钢铁"),
        _named("302132.SZ", "SZSE", "钢铁"),
        _named("000011.SZ", "SZSE", "环保"),
    )
    bindings = {
        item.instrument_id: {
            "asset_id": f"asset-{item.instrument_id}",
            "report_id": f"report-{item.instrument_id}",
            "report_period": "2025-12-31",
            "document_version": f"ver-{item.instrument_id}",
        }
        for item in candidates
    }
    storage = _storage(tmp_path)
    service = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
        plan_directory=tmp_path / "m4-v6-unseen",
    )
    same_identity = service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids={"600008.SH", "600019.SH"},
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    assert [item.instrument_id for item in same_identity.reports] == [
        "600007.SH",
        "600010.SH",
    ]
    observed = {
        "302132.SZ",
        "600000.SH",
        "600004.SH",
        "600006.SH",
        "600007.SH",
        "600008.SH",
        "600010.SH",
        "600019.SH",
    }
    unseen_dir = tmp_path / "m4-v6-unseen-pass"
    unseen = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
        plan_directory=unseen_dir,
    )
    plan = unseen.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids=observed,
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    again = unseen.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids=observed,
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    loaded = load_m4_next_batch_plan(
        unseen.checkpoint_root, plan_directory=unseen_dir
    )
    assert plan == again == loaded
    assert [item.instrument_id for item in plan.reports] == [
        "600011.SH",
        "600022.SH",
    ]
    assert plan.knowledge_cutoff == "2026-09-17"
    assert plan.token_budget == 50_000
    assert {item.disclosure_form for item in plan.reports} == {
        "service",
        "manufacturing",
    }
    assert observed.isdisjoint(item.instrument_id for item in plan.reports)


def test_v7_unseen_freeze_also_skips_the_latest_observed_pair(tmp_path):
    """The v6 observed set still selects 600009 and 600022. This round passes them."""

    candidates = (
        _named("600009.SH", "SSE", "环保"),
        _named("600011.SH", "SSE", "环保"),
        _named("600022.SH", "SSE", "钢铁"),
        _named("600023.SH", "SSE", "钢铁"),
        _named("000011.SZ", "SZSE", "环保"),
    )
    bindings = {
        item.instrument_id: {
            "asset_id": f"asset-{item.instrument_id}",
            "report_id": f"report-{item.instrument_id}",
            "report_period": "2025-12-31",
            "document_version": f"ver-{item.instrument_id}",
        }
        for item in candidates
    }
    prior = {
        "302132.SZ",
        "600000.SH",
        "600004.SH",
        "600006.SH",
        "600007.SH",
        "600008.SH",
        "600010.SH",
        "600019.SH",
    }
    storage = _storage(tmp_path)
    service = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
        plan_directory=tmp_path / "m4-v7-without-latest",
    )
    without_latest = service.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids=prior,
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    assert [item.instrument_id for item in without_latest.reports] == [
        "600009.SH",
        "600022.SH",
    ]
    observed = prior | {"600009.SH", "600022.SH"}
    unseen_dir = tmp_path / "m4-v7-unseen"
    unseen = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
        plan_directory=unseen_dir,
    )
    plan = unseen.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids=observed,
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    again = unseen.freeze_m4_next_batch_plan(
        knowledge_cutoff="2026-09-17",
        registry=SimpleNamespace(candidates=candidates),
        delivered_ids=observed,
        readable=lambda candidate: candidate.asset_status == "available",
        binding_for=bindings.get,
    )
    loaded = load_m4_next_batch_plan(
        unseen.checkpoint_root, plan_directory=unseen_dir
    )
    assert plan == again == loaded
    assert [item.instrument_id for item in plan.reports] == [
        "600011.SH",
        "600023.SH",
    ]
    assert plan.token_budget == 50_000
    assert observed.isdisjoint(item.instrument_id for item in plan.reports)


def test_freeze_refuses_when_a_disclosure_form_has_no_candidate(tmp_path):
    storage = _storage(tmp_path)
    service = _service(tmp_path, storage, provider=_RequestBoundOverviewProvider())
    registry = SimpleNamespace(
        candidates=(_named("600200.SH", "SSE", "交通运输"),)
    )
    with pytest.raises(ValueError, match="no legal manufacturing"):
        service.freeze_m4_next_batch_plan(
            knowledge_cutoff="2026-09-17",
            registry=registry,
            delivered_ids=set(),
            readable=lambda candidate: True,
            binding_for=lambda instrument_id: {
                "asset_id": "asset",
                "report_id": "report",
                "report_period": "2025-12-31",
                "document_version": "ver",
            },
        )
    assert load_m4_next_batch_plan(service.checkpoint_root) is None


def test_second_company_failure_is_in_the_live_run_before_source_review(tmp_path):
    plan = _plan()
    service_report, manufacturing = plan.reports
    service, _seen = _prepare(
        tmp_path,
        plan,
        {
            service_report.instrument_id: _binding(service_report),
            manufacturing.instrument_id: _binding(manufacturing),
        },
    )
    before = _seed_history(service.checkpoint_root)
    registry = _registry(plan)
    service.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 0,
        "inserted": 0,
        "reused": 0,
        "work_ids": [],
    }
    asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff=plan.knowledge_cutoff,
            instrument_ids=[service_report.instrument_id],
            candidate_registry=registry,
        )
    )

    async def fail_drain(*_args, **_kwargs):
        raise RuntimeError("second company failed")

    service.production._drain_stage = fail_drain
    service.repository.enqueue_latest_annual = lambda **_kwargs: {
        "eligible": 1,
        "inserted": 1,
        "reused": 0,
        "work_ids": ["second"],
    }
    with pytest.raises(RuntimeError, match="second company failed"):
        asyncio.run(
            service.execute(
                "run",
                knowledge_cutoff=plan.knowledge_cutoff,
                instrument_ids=[manufacturing.instrument_id],
                candidate_registry=registry,
            )
        )
    observation = load_m4_next_batch_observation_for_source_review(
        service.checkpoint_root
    )
    live_path = (
        service.checkpoint_root
        / "reports"
        / "m4_next_small_batch"
        / plan.plan_id
        / "company_profile_live_run.v1.json"
    )
    live_ids = set(
        json.loads(live_path.read_text(encoding="utf-8"))["selected_instrument_ids"]
    )
    assert {item.instrument_id for item in observation.outcomes} == live_ids
    assert observation.outcome_for(manufacturing.instrument_id).status == "failed"
    review = record_published_source_review(
        checkpoint_root=service.checkpoint_root,
        semantic_findings=[
            SemanticFinding(
                instrument_id=service_report.instrument_id,
                aspect="core_skeleton",
                kind="semantic",
                source="independently_read_official_report",
                disclosure_id="service-overview",
                disclosed_in_source=True,
                present_in_delivery=False,
                fact_accurate=None,
                critical_numeric_error=False,
            ),
            SemanticFinding(
                instrument_id=manufacturing.instrument_id,
                aspect="core_skeleton",
                kind="semantic",
                source="independently_read_official_report",
                disclosure_id="manufacturing-overview",
                disclosed_in_source=True,
                present_in_delivery=False,
                fact_accurate=None,
                critical_numeric_error=False,
            ),
        ],
    )
    assert set(review["source_review"]["live_run"]["selected_instrument_ids"]) == {
        service_report.instrument_id,
        manufacturing.instrument_id,
    }
    assert snapshot_bytes(historical_snapshot_paths(service.checkpoint_root)) == before


def test_restored_ledger_does_not_exhaust_the_remaining_budget():
    class _Caller:
        def __init__(self) -> None:
            self.calls = 0

        def extract(self, _request):
            self.calls += 1
            return {"ok": True}

    caller = _Caller()
    runtime = CompanyProfileStageRuntime(
        writer=object(),
        provider=caller,
        token_budget=50_000,
    )
    from research.company_profile.runtime import _WorkState

    ledger = _WorkState(work_id="restored")
    ledger.tokens_restored = 30_000
    ledger.tokens_used = 30_000
    runtime._active_state = ledger
    runtime.apply_token_budget(20_000)
    runtime.provider.extract(None)
    assert caller.calls == 1
    assert not ledger_budget_exhausted(ledger, runtime._total_token_budget)
    ledger.tokens_used = 50_000
    assert ledger_budget_exhausted(ledger, runtime._total_token_budget)
    with pytest.raises(SemanticProviderError, match="token budget exhausted"):
        runtime.provider.extract(None)
    assert caller.calls == 1
