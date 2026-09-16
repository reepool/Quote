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
    StratifiedSamplingRule,
    UniverseDenominator,
    assign_disclosure_form,
    live_plan_schema_manifest,
    record_company_profile_live_plan,
    select_stratified_review_sample,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    DEFAULT_MAX_ELAPSED_SECONDS,
    DEFAULT_MAX_ITEMS,
)


def _plan_payload() -> dict:
    return json.loads(record_company_profile_live_plan().model_dump_json())


def test_record_live_plan_reuses_operator_budget_and_does_not_require_fifty():
    plan = record_company_profile_live_plan()

    assert plan.schema_version == LIVE_PLAN_SCHEMA_VERSION
    assert plan.production_authorization == PRODUCTION_AUTHORIZATION == "not_authorized"
    assert plan.plan_status == "recorded_before_live"
    assert plan.live_task_started is False
    assert (
        plan.budget.max_companies_this_round
        == DEFAULT_LIVE_MAX_COMPANIES
        == DEFAULT_MAX_ITEMS
        == 2
    )
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


def test_sampling_rule_is_executable_and_not_used_to_fit_thresholds():
    plan = record_company_profile_live_plan()
    sampling = plan.sampling

    assert sampling.method == "predeclared_stratified_sample"
    assert sampling.purpose == "independent_source_review"
    assert sampling.allocation == "priority_round_robin_then_instrument_id"
    assert sampling.selection_key == "instrument_id_ascending"
    assert sampling.missing_classification_stratum == "other"
    assert sampling.empty_stratum_policy == "keep_in_universe_skip_draw"
    assert sampling.exchange_strata == ("SSE", "SZSE", "BSE")
    assert sampling.disclosure_form_strata == (
        "manufacturing",
        "service",
        "finance",
        "other",
    )
    assert sampling.stratum_priority[0] == ("SSE", "manufacturing")
    assert sampling.stratum_priority[1] == ("SZSE", "manufacturing")
    assert sampling.per_stratum_first_draw == 1
    assert sampling.max_sample_size == plan.budget.max_companies_this_round == 2
    assert sampling.excludes_from_universe is False
    assert sampling.used_to_fit_thresholds is False


def test_select_stratified_sample_follows_priority_and_instrument_id():
    plan = record_company_profile_live_plan()
    selected = select_stratified_review_sample(
        (
            {
                "instrument_id": "000001.SZ",
                "exchange": "SZSE",
                "sw_l1_name": "银行",
            },
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "sw_l1_name": "银行",
            },
            {
                "instrument_id": "000878.SZ",
                "exchange": "SZSE",
                "sw_l1_name": "有色金属",
            },
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "classification_status": "missing",
            },
        ),
        rule=plan.sampling,
    )

    assert assign_disclosure_form({"sw_l1_name": "有色金属"}) == "manufacturing"
    assert assign_disclosure_form({"sw_l1_name": "银行"}) == "finance"
    assert (
        assign_disclosure_form({"classification_status": "missing"}) == "other"
    )
    assert selected == ("000878.SZ", "600000.SH")


def test_empty_stratum_is_skipped_and_missing_classification_stays_drawable():
    plan = record_company_profile_live_plan()
    selected = select_stratified_review_sample(
        (
            {
                "instrument_id": "601398.SH",
                "exchange": "SSE",
                "classification_status": "missing",
            },
            {
                "instrument_id": "600036.SH",
                "exchange": "SSE",
                "sw_l1_name": "银行",
            },
        ),
        rule=plan.sampling,
    )

    assert selected == ("600036.SH", "601398.SH")


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


def test_expansion_thresholds_are_numeric_and_bound_to_operator_budget():
    plan = record_company_profile_live_plan()
    thresholds = plan.expansion_thresholds

    assert thresholds.numeric_thresholds_basis == "operator_cost_constraint"
    assert thresholds.min_independently_reviewed_reports == 2
    assert thresholds.min_occupied_strata_reviewed == 2
    assert thresholds.min_source_recall_ratio == 1.0
    assert thresholds.min_source_accuracy_ratio == 1.0
    assert thresholds.max_critical_numeric_errors == 0
    assert thresholds.max_tokens == 50_000
    assert thresholds.max_elapsed_seconds == 300.0
    assert thresholds.max_companies_this_expansion == 2
    assert thresholds.scale_quality_claim_allowed is False
    assert thresholds.fitted_from_observed_results is False
    assert thresholds.fixture_guards_are_not_live_quality is True
    assert thresholds.gold24_equality_is_not_a_threshold is True
    assert thresholds.untriggered_negatives_are_not_passes is True
    assert thresholds.expansion_requires_independent_source_review is True


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
    plan = record_company_profile_live_plan(max_companies_this_round=50)

    assert plan.budget.max_companies_this_round == 50
    assert plan.budget.require_fifty_companies is False
    assert plan.sampling.max_sample_size == 50
    assert plan.expansion_thresholds.max_companies_this_expansion == 50
    assert plan.expansion_thresholds.min_independently_reviewed_reports == 50


def test_schema_rejects_expansion_json_without_numeric_thresholds():
    with pytest.raises(ValidationError):
        ExpansionQualityThresholds.model_validate(
            {
                "numeric_thresholds_status": "unset_before_first_live_observation",
                "scale_quality_claim_allowed": False,
                "fitted_from_observed_results": False,
                "fixture_guards_are_not_live_quality": True,
                "gold24_equality_is_not_a_threshold": True,
                "untriggered_negatives_are_not_passes": True,
                "expansion_requires_independent_source_review": True,
            }
        )
    with pytest.raises(ValidationError):
        ExpansionQualityThresholds.model_validate(
            {
                "numeric_thresholds_basis": "operator_cost_constraint",
                "min_independently_reviewed_reports": 2,
                "min_occupied_strata_reviewed": 2,
                "min_source_recall_ratio": 0.8,
                "min_source_accuracy_ratio": 1.0,
                "max_critical_numeric_errors": 0,
                "max_tokens": 50_000,
                "max_elapsed_seconds": 300.0,
                "max_companies_this_expansion": 2,
                "scale_quality_claim_allowed": False,
                "fitted_from_observed_results": False,
                "fixture_guards_are_not_live_quality": True,
                "gold24_equality_is_not_a_threshold": True,
                "untriggered_negatives_are_not_passes": True,
                "expansion_requires_independent_source_review": True,
            }
        )


def test_schema_rejects_sampling_json_without_executable_selection_rule():
    with pytest.raises(ValidationError):
        StratifiedSamplingRule.model_validate(
            {
                "method": "predeclared_stratified_sample",
                "purpose": "independent_source_review",
                "exchange_strata": ["SSE", "SZSE", "BSE"],
                "disclosure_form_strata": [
                    "manufacturing",
                    "service",
                    "finance",
                    "other",
                ],
                "excludes_from_universe": False,
                "used_to_fit_thresholds": False,
            }
        )


def test_schema_rejects_plans_that_contradict_registered_denominators():
    payload = _plan_payload()
    payload["sampling"]["exchange_strata"] = ["SSE"]
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate_json(json.dumps(payload))

    payload = _plan_payload()
    payload["universe_denominator"]["exchanges"] = ["SSE", "SZSE"]
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate_json(json.dumps(payload))

    payload = _plan_payload()
    payload["recall_denominator"]["forbidden_sources"] = ["model_listed_fields"]
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate_json(json.dumps(payload))
    with pytest.raises(ValidationError):
        RecallDenominator.model_validate_json(
            json.dumps(
                {
                    "definition": (
                        "independently_read_important_disclosures_"
                        "from_sampled_official_reports"
                    ),
                    "forbidden_sources": ["model_listed_fields"],
                }
            )
        )

    with pytest.raises(ValidationError):
        UniverseDenominator.model_validate(
            {
                "policy_version": "a_share_full_market.v1",
                "exchanges": [],
                "as_of": "declared_at_run_from_knowledge_cutoff",
                "includes_missing_assets": True,
                "includes_missing_classification": True,
            }
        )


def test_schema_rejects_budget_and_threshold_mismatch():
    payload = _plan_payload()
    payload["expansion_thresholds"]["min_source_recall_ratio"] = 0.8
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate_json(json.dumps(payload))

    payload = _plan_payload()
    payload["expansion_thresholds"]["min_independently_reviewed_reports"] = 1
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate_json(json.dumps(payload))

    payload = _plan_payload()
    payload["sampling"]["max_sample_size"] = 1
    with pytest.raises(ValidationError):
        CompanyProfileLivePlan.model_validate_json(json.dumps(payload))


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
    assert manifest["registered_expansion_thresholds"][
        "min_independently_reviewed_reports"
    ] == 2
    assert manifest["registered_sampling"]["max_sample_size"] == 2
    assert "model_listed_fields" in manifest["forbidden_recall_sources"]
