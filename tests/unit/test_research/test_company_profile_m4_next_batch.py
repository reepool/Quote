from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from research.company_profile.execution import DEFAULT_TOTAL_TOKEN_BUDGET
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
from research.company_profile.runtime import CompanyProfileStageRuntime
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
        service.runtime._states["work"] = type(
            "Ledger",
            (),
            {"tokens_used": 120},
        )()
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
    assert merged.outcome_for(manufacturing.instrument_id).status == "completed"
    assert merged.outcome_for(manufacturing.instrument_id).reused_scope is True
    assert merged.outcome_for(manufacturing.instrument_id).tokens_consumed == 0
    assert merged.tokens_consumed == 120
    assert service.runtime._total_token_budget == 49_880
    assert service.runtime.provider._total_token_budget == 49_880


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
