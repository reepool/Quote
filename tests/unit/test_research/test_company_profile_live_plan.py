from __future__ import annotations

import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.execution import DEFAULT_TOTAL_TOKEN_BUDGET
from research.company_profile.live_plan import (
    DEFAULT_LIVE_MAX_COMPANIES,
    DEFAULT_LIVE_MAX_ELAPSED_SECONDS,
    DEFAULT_LIVE_TOKEN_BUDGET,
    LIVE_PLAN_SCHEMA_VERSION,
    CompanyProfileLivePlan,
    ExpansionQualityThresholds,
    LiveRunBudget,
    RecallDenominator,
    live_plan_schema_manifest,
    record_company_profile_live_plan,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    DEFAULT_MAX_ELAPSED_SECONDS,
    DEFAULT_MAX_ITEMS,
)


def test_record_live_plan_reuses_operator_budget_and_does_not_require_fifty():
    plan = record_company_profile_live_plan()

    assert plan.schema_version == LIVE_PLAN_SCHEMA_VERSION
    assert plan.production_authorization == PRODUCTION_AUTHORIZATION == "not_authorized"
    assert plan.plan_status == "recorded_before_live"
    assert plan.live_task_started is False
    assert plan.budget.max_companies_this_round == DEFAULT_LIVE_MAX_COMPANIES == DEFAULT_MAX_ITEMS == 2
    assert plan.budget.min_companies_this_round == 0
    assert (
        plan.budget.token_budget
        == DEFAULT_LIVE_TOKEN_BUDGET
        == DEFAULT_TOTAL_TOKEN_BUDGET
        == 50_000
    )
    assert (
        plan.budget.max_elapsed_seconds
        == DEFAULT_LIVE_MAX_ELAPSED_SECONDS
        == DEFAULT_MAX_ELAPSED_SECONDS
        == 300.0
    )
    assert plan.budget.require_fifty_companies is False
    assert plan.budget.max_companies_this_round != 50


def test_sampling_is_stratified_for_independent_review_not_threshold_fitting():
    plan = record_company_profile_live_plan()

    assert plan.sampling.method == "predeclared_stratified_sample"
    assert plan.sampling.purpose == "independent_source_review"
    assert plan.sampling.exchange_strata == ("SSE", "SZSE", "BSE")
    assert plan.sampling.disclosure_form_strata == (
        "manufacturing",
        "service",
        "finance",
        "other",
    )
    assert plan.sampling.excludes_from_universe is False
    assert plan.sampling.used_to_fit_thresholds is False


def test_universe_denominator_keeps_missing_assets_and_classification():
    plan = record_company_profile_live_plan()

    assert plan.universe_denominator.policy_version == "a_share_full_market.v1"
    assert plan.universe_denominator.exchanges == ("SSE", "SZSE", "BSE")
    assert plan.universe_denominator.as_of == "declared_at_run_from_knowledge_cutoff"
    assert plan.universe_denominator.includes_missing_assets is True
    assert plan.universe_denominator.includes_missing_classification is True


def test_recall_denominator_is_independent_source_read_not_model_listed_fields():
    plan = record_company_profile_live_plan()

    assert (
        plan.recall_denominator.definition
        == "independently_read_important_disclosures_from_sampled_official_reports"
    )
    assert plan.recall_denominator.forbidden_sources == (
        "model_listed_fields",
        "evidence_id_existence",
        "gold24_equality",
    )

    with pytest.raises(ValidationError):
        RecallDenominator.model_validate({"forbidden_sources": ("gold24_equality",)})


def test_expansion_thresholds_are_unset_and_cannot_claim_scale_quality():
    plan = record_company_profile_live_plan()
    thresholds = plan.expansion_thresholds

    assert thresholds.numeric_thresholds_status == "unset_before_first_live_observation"
    assert thresholds.scale_quality_claim_allowed is False
    assert thresholds.fitted_from_observed_results is False
    assert thresholds.fixture_guards_are_not_live_quality is True
    assert thresholds.gold24_equality_is_not_a_threshold is True
    assert thresholds.untriggered_negatives_are_not_passes is True
    assert thresholds.expansion_requires_independent_source_review is True
    assert thresholds.expansion_requires_new_plan_revision is True

    with pytest.raises(ValidationError):
        ExpansionQualityThresholds.model_validate(
            {"fitted_from_observed_results": True}
        )
    with pytest.raises(ValidationError):
        ExpansionQualityThresholds.model_validate(
            {"scale_quality_claim_allowed": True}
        )
    with pytest.raises(ValidationError):
        ExpansionQualityThresholds.model_validate(
            {"numeric_thresholds_status": "fitted_from_gold24"}
        )


def test_live_plan_rejects_forced_fifty_company_quota():
    with pytest.raises(ValidationError):
        LiveRunBudget.model_validate(
            {
                "max_companies_this_round": 50,
                "min_companies_this_round": 0,
                "token_budget": 50_000,
                "max_elapsed_seconds": 300.0,
                "require_fifty_companies": True,
            }
        )
    with pytest.raises(ValidationError):
        LiveRunBudget(
            max_companies_this_round=50,
            min_companies_this_round=50,
            token_budget=50_000,
            max_elapsed_seconds=300.0,
        )


def test_budget_cap_of_fifty_is_allowed_when_it_is_not_a_required_quota():
    budget = LiveRunBudget(
        max_companies_this_round=50,
        min_companies_this_round=0,
        token_budget=50_000,
        max_elapsed_seconds=300.0,
    )

    assert budget.max_companies_this_round == 50
    assert budget.require_fifty_companies is False


def test_recorded_plan_round_trips_json_and_rejects_extra_fields():
    plan = record_company_profile_live_plan()
    restored = CompanyProfileLivePlan.model_validate_json(plan.model_dump_json())

    assert restored == plan
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate(
            {**json.loads(plan.model_dump_json()), "observed_pass_rate": 0.8}
        )


def test_live_plan_records_before_run_and_has_no_execution_or_llm_entry():
    from research.company_profile import live_plan

    source = inspect.getsource(live_plan)
    assert "execute_published_task" not in source
    assert "CompanyProfileTaskService" not in source
    assert "openai" not in source.lower()
    assert "chat.completions" not in source
    assert record_company_profile_live_plan().live_task_started is False

    manifest = live_plan_schema_manifest()
    assert manifest["schema_version"] == LIVE_PLAN_SCHEMA_VERSION
    assert manifest["live_task_started"] is False
    assert manifest["production_authorization"] == "not_authorized"
    assert "model_listed_fields" in manifest["forbidden_recall_sources"]
