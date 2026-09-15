"""Pre-live run plan for company-profile source review.

This is the unique owner for recording budget, sampling, recall denominator
and expansion gates before a live task. It does not execute the task, call
LLM, or fit thresholds from observed results.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.candidate_registry import PRODUCTION_SCOPE_POLICY
from research.company_profile.models import PRODUCTION_AUTHORIZATION

# A priori operator cost caps. Copied from operations/execution so this owner
# does not import the task-service graph before a live run.
DEFAULT_LIVE_MAX_COMPANIES = 2
DEFAULT_LIVE_TOKEN_BUDGET = 50_000
DEFAULT_LIVE_MAX_ELAPSED_SECONDS = 300.0

LIVE_PLAN_SCHEMA_VERSION = "company_profile_live_plan.v1"
LIVE_PLAN_STATUS = "recorded_before_live"
_FORBIDDEN_RECALL_SOURCES = (
    "model_listed_fields",
    "evidence_id_existence",
    "gold24_equality",
)
_EXCHANGES = ("SSE", "SZSE", "BSE")
_DISCLOSURE_FORMS = ("manufacturing", "service", "finance", "other")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class LiveRunBudget(_StrictModel):
    max_companies_this_round: int = Field(ge=1)
    min_companies_this_round: int = Field(ge=0)
    token_budget: int = Field(ge=0)
    max_elapsed_seconds: float = Field(gt=0)
    require_fifty_companies: Literal[False] = False

    @model_validator(mode="after")
    def _no_forced_fifty(self) -> LiveRunBudget:
        if self.min_companies_this_round >= 50:
            raise ValueError("live plan cannot require fifty companies per round")
        if self.min_companies_this_round > self.max_companies_this_round:
            raise ValueError("minimum company quota cannot exceed this-round budget")
        return self


class StratifiedSamplingRule(_StrictModel):
    method: Literal["predeclared_stratified_sample"] = "predeclared_stratified_sample"
    purpose: Literal["independent_source_review"] = "independent_source_review"
    exchange_strata: tuple[str, ...] = _EXCHANGES
    disclosure_form_strata: tuple[str, ...] = _DISCLOSURE_FORMS
    excludes_from_universe: Literal[False] = False
    used_to_fit_thresholds: Literal[False] = False


class UniverseDenominator(_StrictModel):
    policy_version: Literal["a_share_full_market.v1"] = PRODUCTION_SCOPE_POLICY
    exchanges: tuple[str, ...] = _EXCHANGES
    as_of: Literal["declared_at_run_from_knowledge_cutoff"] = (
        "declared_at_run_from_knowledge_cutoff"
    )
    includes_missing_assets: Literal[True] = True
    includes_missing_classification: Literal[True] = True


class RecallDenominator(_StrictModel):
    definition: Literal[
        "independently_read_important_disclosures_from_sampled_official_reports"
    ] = "independently_read_important_disclosures_from_sampled_official_reports"
    forbidden_sources: tuple[str, ...] = _FORBIDDEN_RECALL_SOURCES

    @model_validator(mode="after")
    def _not_model_listed(self) -> RecallDenominator:
        forbidden = set(self.forbidden_sources)
        if "model_listed_fields" not in forbidden:
            raise ValueError("recall denominator cannot use model-listed fields")
        return self


class ExpansionQualityThresholds(_StrictModel):
    numeric_thresholds_status: Literal["unset_before_first_live_observation"] = (
        "unset_before_first_live_observation"
    )
    scale_quality_claim_allowed: Literal[False] = False
    fitted_from_observed_results: Literal[False] = False
    fixture_guards_are_not_live_quality: Literal[True] = True
    gold24_equality_is_not_a_threshold: Literal[True] = True
    untriggered_negatives_are_not_passes: Literal[True] = True
    expansion_requires_independent_source_review: Literal[True] = True
    expansion_requires_new_plan_revision: Literal[True] = True


class CompanyProfileLivePlan(_StrictModel):
    schema_version: Literal["company_profile_live_plan.v1"] = LIVE_PLAN_SCHEMA_VERSION
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    plan_status: Literal["recorded_before_live"] = LIVE_PLAN_STATUS
    live_task_started: Literal[False] = False
    budget: LiveRunBudget
    sampling: StratifiedSamplingRule
    universe_denominator: UniverseDenominator
    recall_denominator: RecallDenominator
    expansion_thresholds: ExpansionQualityThresholds

    @model_validator(mode="after")
    def _recorded_before_observation(self) -> CompanyProfileLivePlan:
        if self.expansion_thresholds.fitted_from_observed_results:
            raise ValueError("live plan cannot fit thresholds from observed results")
        if self.expansion_thresholds.scale_quality_claim_allowed:
            raise ValueError("pre-live plan cannot claim scale quality")
        return self


def record_company_profile_live_plan(
    *,
    max_companies_this_round: int = DEFAULT_LIVE_MAX_COMPANIES,
    token_budget: int = DEFAULT_LIVE_TOKEN_BUDGET,
    max_elapsed_seconds: float = DEFAULT_LIVE_MAX_ELAPSED_SECONDS,
) -> CompanyProfileLivePlan:
    """Record the operator budget and review rules before any live observation."""

    return CompanyProfileLivePlan(
        budget=LiveRunBudget(
            max_companies_this_round=max_companies_this_round,
            min_companies_this_round=0,
            token_budget=token_budget,
            max_elapsed_seconds=max_elapsed_seconds,
        ),
        sampling=StratifiedSamplingRule(),
        universe_denominator=UniverseDenominator(),
        recall_denominator=RecallDenominator(),
        expansion_thresholds=ExpansionQualityThresholds(),
    )


def live_plan_schema_manifest() -> dict[str, Any]:
    """Register the pre-live plan schema without starting a live task."""

    return {
        "schema_version": LIVE_PLAN_SCHEMA_VERSION,
        "plan_status": LIVE_PLAN_STATUS,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "live_task_started": False,
        "forbidden_recall_sources": _FORBIDDEN_RECALL_SOURCES,
        "plan_schema": CompanyProfileLivePlan.model_json_schema(),
    }
