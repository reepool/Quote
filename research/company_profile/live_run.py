"""Budget-limited live-run accounting for company-profile common-core.

This is the unique owner for company_profile_live_run.v1. The published task
service still executes the queue. This module records the universe denominator
and per-company outcomes. It does not call LLM, enable production, or rerun a
batch because one company is missing supplemental information.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.candidate_registry import (
    PRODUCTION_SCOPE_POLICY,
    AShareCandidateRegistry,
    AShareProfileCandidate,
)
from research.company_profile.live_plan import (
    CompanyProfileLivePlan,
    StratifiedSamplingRule,
    record_company_profile_live_plan,
    select_stratified_review_sample,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION

LIVE_RUN_SCHEMA_VERSION = "company_profile_live_run.v1"
_SELECTED_OUTCOMES = frozenset({"completed", "failed"})


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class UniverseRunDenominator(_StrictModel):
    policy_version: Literal["a_share_full_market.v1"]
    as_of: str = Field(min_length=1)
    total: int = Field(ge=0)
    eligible: int = Field(ge=0)
    missing_asset: int = Field(ge=0)
    missing_classification: int = Field(ge=0)
    selected_for_run: int = Field(ge=0)
    completed: int = Field(ge=0)
    failed: int = Field(ge=0)
    includes_missing_assets: Literal[True]

    @model_validator(mode="after")
    def _counts_are_consistent(self) -> UniverseRunDenominator:
        if self.policy_version != PRODUCTION_SCOPE_POLICY:
            raise ValueError("live-run denominator must stay full-market")
        if not self.includes_missing_assets:
            raise ValueError("live-run denominator must keep missing assets")
        if self.completed + self.failed != self.selected_for_run:
            raise ValueError("completed and failed must cover the selected run set")
        if self.missing_asset > self.total or self.eligible > self.total:
            raise ValueError("denominator counts cannot exceed the universe")
        return self


class CompanyRunOutcome(_StrictModel):
    instrument_id: str = Field(min_length=1)
    outcome: Literal["completed", "failed"]
    supplement_incomplete: bool
    delivered: bool
    batch_rerun_triggered: Literal[False]

    @model_validator(mode="after")
    def _delivery_matches_outcome(self) -> CompanyRunOutcome:
        if self.outcome == "completed" and not self.delivered:
            raise ValueError("completed company must be delivered")
        if self.outcome == "failed" and self.delivered:
            raise ValueError("failed company cannot be marked delivered")
        if self.batch_rerun_triggered:
            raise ValueError("incomplete supplement cannot rerun the batch")
        return self


class CompanyProfileLiveRunReport(_StrictModel):
    schema_version: Literal["company_profile_live_run.v1"] = LIVE_RUN_SCHEMA_VERSION
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    knowledge_cutoff: str = Field(min_length=1)
    plan: CompanyProfileLivePlan
    selected_instrument_ids: tuple[str, ...]
    universe: UniverseRunDenominator
    company_outcomes: tuple[CompanyRunOutcome, ...]
    whole_batch_rerun: Literal[False]
    scale_quality_claim_allowed: Literal[False]

    @model_validator(mode="after")
    def _report_matches_plan_and_selection(self) -> CompanyProfileLiveRunReport:
        if self.whole_batch_rerun:
            raise ValueError("live run cannot rerun the whole batch")
        if self.scale_quality_claim_allowed:
            raise ValueError("live run cannot claim scale quality")
        if self.plan.live_task_started:
            raise ValueError("pre-live plan snapshot cannot start the task itself")
        if len(self.selected_instrument_ids) > self.plan.budget.max_companies_this_round:
            raise ValueError("selected run set cannot exceed the live-plan budget")
        if len(self.selected_instrument_ids) != self.universe.selected_for_run:
            raise ValueError("selected ids must match the denominator")
        if len(self.company_outcomes) != len(self.selected_instrument_ids):
            raise ValueError("outcomes are recorded only for the selected run set")
        outcome_ids = tuple(item.instrument_id for item in self.company_outcomes)
        if outcome_ids != self.selected_instrument_ids:
            raise ValueError("outcome order must follow the selected run set")
        return self


def select_live_run_targets(
    registry: AShareCandidateRegistry,
    plan: CompanyProfileLivePlan,
    *,
    instrument_ids: Sequence[str] = (),
) -> tuple[str, ...]:
    """Choose budget-limited official-report names. Missing assets stay out."""

    if instrument_ids:
        by_id = {item.instrument_id: item for item in registry.candidates}
        selected: list[str] = []
        for instrument_id in instrument_ids:
            candidate = by_id.get(instrument_id)
            if candidate is None:
                continue
            if not _review_eligible(candidate, plan.sampling):
                continue
            selected.append(instrument_id)
            if len(selected) >= plan.budget.max_companies_this_round:
                break
        return tuple(selected)
    return select_stratified_review_sample(registry.candidates, rule=plan.sampling)


def record_live_run_report(
    *,
    plan: CompanyProfileLivePlan,
    registry: AShareCandidateRegistry,
    selected_instrument_ids: Sequence[str],
    delivered_instrument_ids: Sequence[str],
    knowledge_cutoff: str,
    incomplete_supplement_ids: Sequence[str] = (),
) -> CompanyProfileLiveRunReport:
    """Record universe counts and selected outcomes after one bounded run."""

    selected = tuple(selected_instrument_ids)
    delivered = set(delivered_instrument_ids)
    incomplete = set(incomplete_supplement_ids)
    by_id = {item.instrument_id: item for item in registry.candidates}
    for instrument_id in selected:
        candidate = by_id.get(instrument_id)
        if candidate is None or not _review_eligible(candidate, plan.sampling):
            raise ValueError("live run cannot select a name without an official report")
    outcomes = tuple(
        CompanyRunOutcome(
            instrument_id=instrument_id,
            outcome="completed" if instrument_id in delivered else "failed",
            supplement_incomplete=instrument_id in incomplete,
            delivered=instrument_id in delivered,
            batch_rerun_triggered=False,
        )
        for instrument_id in selected
    )
    completed = sum(item.outcome == "completed" for item in outcomes)
    failed = sum(item.outcome == "failed" for item in outcomes)
    return CompanyProfileLiveRunReport(
        knowledge_cutoff=knowledge_cutoff,
        plan=plan,
        selected_instrument_ids=selected,
        universe=UniverseRunDenominator(
            policy_version=PRODUCTION_SCOPE_POLICY,
            as_of=knowledge_cutoff,
            total=registry.counts["total"],
            eligible=registry.counts["eligible"],
            missing_asset=registry.counts["asset_not_available"],
            missing_classification=registry.counts["classification_missing"],
            selected_for_run=len(selected),
            completed=completed,
            failed=failed,
            includes_missing_assets=True,
        ),
        company_outcomes=outcomes,
        whole_batch_rerun=False,
        scale_quality_claim_allowed=False,
    )


def live_run_schema_manifest() -> dict[str, Any]:
    """Register the live-run report schema without enabling production."""

    return {
        "schema_version": LIVE_RUN_SCHEMA_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "whole_batch_rerun": False,
        "plan_schema_version": record_company_profile_live_plan().schema_version,
        "report_schema": CompanyProfileLiveRunReport.model_json_schema(),
    }


def _review_eligible(
    candidate: AShareProfileCandidate,
    rule: StratifiedSamplingRule,
) -> bool:
    return bool(select_stratified_review_sample((candidate,), rule=rule))
