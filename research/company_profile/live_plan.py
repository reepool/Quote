"""Pre-live run plan for company-profile source review.

This is the unique owner for recording budget, executable sampling, recall
denominator and first-expansion numeric thresholds before a live task. It does
not execute the task, call LLM, or fit thresholds from observed results.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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
_STRATUM_PRIORITY = tuple(
    (exchange, form) for form in _DISCLOSURE_FORMS for exchange in _EXCHANGES
)
_FINANCE_L1 = frozenset({"银行", "非银金融"})
_SERVICE_L1 = frozenset(
    {
        "交通运输",
        "房地产",
        "商贸零售",
        "社会服务",
        "计算机",
        "传媒",
        "通信",
        "综合",
        "美容护理",
        "建筑装饰",
        "公用事业",
        "环保",
    }
)
_MANUFACTURING_L1 = frozenset(
    {
        "农林牧渔",
        "基础化工",
        "钢铁",
        "有色金属",
        "电子",
        "家用电器",
        "食品饮料",
        "纺织服饰",
        "轻工制造",
        "医药生物",
        "建筑材料",
        "电力设备",
        "机械设备",
        "国防军工",
        "煤炭",
        "石油石化",
        "汽车",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _exchange_of(candidate: Any) -> str:
    if isinstance(candidate, Mapping):
        return str(candidate.get("exchange") or "")
    return str(getattr(candidate, "exchange", "") or "")


def _instrument_id_of(candidate: Any) -> str:
    if isinstance(candidate, Mapping):
        return str(candidate.get("instrument_id") or "")
    return str(getattr(candidate, "instrument_id", "") or "")


def _sw_l1_name_of(candidate: Any) -> str | None:
    if isinstance(candidate, Mapping):
        direct = candidate.get("sw_l1_name")
        if direct:
            return str(direct)
        classification = candidate.get("classification")
        if isinstance(classification, Mapping):
            name = classification.get("sw_l1_name")
            return str(name) if name else None
        return None
    classification = getattr(candidate, "classification", None)
    name = getattr(classification, "sw_l1_name", None)
    return str(name) if name else None


def _classification_status_of(candidate: Any) -> str | None:
    if isinstance(candidate, Mapping):
        status = candidate.get("classification_status")
        return str(status) if status is not None else None
    status = getattr(candidate, "classification_status", None)
    return str(status) if status is not None else None


def assign_disclosure_form(candidate: Any) -> str:
    """Map a candidate onto a predeclared disclosure-form stratum."""

    if isinstance(candidate, Mapping):
        explicit = candidate.get("disclosure_form")
        if explicit in _DISCLOSURE_FORMS:
            return str(explicit)
    explicit = getattr(candidate, "disclosure_form", None)
    if explicit in _DISCLOSURE_FORMS:
        return str(explicit)
    if _classification_status_of(candidate) == "missing":
        return "other"
    name = _sw_l1_name_of(candidate)
    if name in _FINANCE_L1:
        return "finance"
    if name in _SERVICE_L1:
        return "service"
    if name in _MANUFACTURING_L1:
        return "manufacturing"
    return "other"


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
    method: Literal["predeclared_stratified_sample"]
    purpose: Literal["independent_source_review"]
    allocation: Literal["priority_round_robin_then_instrument_id"]
    selection_key: Literal["instrument_id_ascending"]
    missing_classification_stratum: Literal["other"]
    empty_stratum_policy: Literal["keep_in_universe_skip_draw"]
    exchange_strata: tuple[str, ...]
    disclosure_form_strata: tuple[str, ...]
    stratum_priority: tuple[tuple[str, str], ...]
    per_stratum_first_draw: Literal[1]
    max_sample_size: int = Field(ge=1)
    excludes_from_universe: Literal[False]
    used_to_fit_thresholds: Literal[False]

    @model_validator(mode="after")
    def _executable_and_registered(self) -> StratifiedSamplingRule:
        if self.exchange_strata != _EXCHANGES:
            raise ValueError("sampling exchange strata must be SSE, SZSE, BSE")
        if self.disclosure_form_strata != _DISCLOSURE_FORMS:
            raise ValueError(
                "sampling disclosure-form strata must be manufacturing, "
                "service, finance, other"
            )
        expected_priority = tuple(
            (exchange, form)
            for form in self.disclosure_form_strata
            for exchange in self.exchange_strata
        )
        if self.stratum_priority != expected_priority:
            raise ValueError(
                "stratum_priority must be form-then-exchange over declared strata"
            )
        if self.used_to_fit_thresholds:
            raise ValueError("sampling cannot be used to fit thresholds")
        if self.excludes_from_universe:
            raise ValueError("sampling cannot exclude names from the universe")
        return self


class UniverseDenominator(_StrictModel):
    policy_version: Literal["a_share_full_market.v1"]
    exchanges: tuple[str, ...]
    as_of: Literal["declared_at_run_from_knowledge_cutoff"]
    includes_missing_assets: Literal[True]
    includes_missing_classification: Literal[True]

    @model_validator(mode="after")
    def _full_market(self) -> UniverseDenominator:
        if self.exchanges != _EXCHANGES:
            raise ValueError("universe exchanges must be SSE, SZSE, BSE")
        if not self.includes_missing_assets:
            raise ValueError("universe denominator must keep missing assets")
        if not self.includes_missing_classification:
            raise ValueError("universe denominator must keep missing classification")
        return self


class RecallDenominator(_StrictModel):
    definition: Literal[
        "independently_read_important_disclosures_from_sampled_official_reports"
    ]
    forbidden_sources: tuple[str, ...]

    @model_validator(mode="after")
    def _independent_source_only(self) -> RecallDenominator:
        if frozenset(self.forbidden_sources) != frozenset(_FORBIDDEN_RECALL_SOURCES):
            raise ValueError(
                "recall denominator must forbid model-listed fields, "
                "evidence-id existence and gold24 equality"
            )
        if len(self.forbidden_sources) != len(set(self.forbidden_sources)):
            raise ValueError("recall forbidden sources cannot be duplicated")
        return self


class ExpansionQualityThresholds(_StrictModel):
    numeric_thresholds_basis: Literal["operator_cost_constraint"]
    min_independently_reviewed_reports: int = Field(ge=1)
    min_occupied_strata_reviewed: int = Field(ge=1)
    min_source_recall_ratio: float = Field(ge=1.0, le=1.0)
    min_source_accuracy_ratio: float = Field(ge=1.0, le=1.0)
    max_critical_numeric_errors: int = Field(ge=0, le=0)
    max_tokens: int = Field(ge=0)
    max_elapsed_seconds: float = Field(gt=0)
    max_companies_this_expansion: int = Field(ge=1)
    scale_quality_claim_allowed: Literal[False]
    fitted_from_observed_results: Literal[False]
    fixture_guards_are_not_live_quality: Literal[True]
    gold24_equality_is_not_a_threshold: Literal[True]
    untriggered_negatives_are_not_passes: Literal[True]
    expansion_requires_independent_source_review: Literal[True]

    @model_validator(mode="after")
    def _a_priori_numeric_gates(self) -> ExpansionQualityThresholds:
        if self.fitted_from_observed_results:
            raise ValueError("live plan cannot fit thresholds from observed results")
        if self.scale_quality_claim_allowed:
            raise ValueError("pre-live plan cannot claim scale quality")
        if self.min_source_recall_ratio != 1.0:
            raise ValueError("first-expansion recall threshold must be complete")
        if self.min_source_accuracy_ratio != 1.0:
            raise ValueError("first-expansion accuracy threshold must be complete")
        if self.max_critical_numeric_errors != 0:
            raise ValueError("first-expansion critical numeric errors must be zero")
        return self


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
    def _plan_is_internally_consistent(self) -> CompanyProfileLivePlan:
        if self.sampling.max_sample_size != self.budget.max_companies_this_round:
            raise ValueError("sample size must equal this-round company budget")
        if self.sampling.exchange_strata != self.universe_denominator.exchanges:
            raise ValueError("sampling exchanges must match the universe denominator")
        thresholds = self.expansion_thresholds
        if (
            thresholds.min_independently_reviewed_reports
            != self.budget.max_companies_this_round
        ):
            raise ValueError("reviewed-report threshold must equal this-round budget")
        if thresholds.min_occupied_strata_reviewed != min(
            self.budget.max_companies_this_round, len(self.sampling.stratum_priority)
        ):
            raise ValueError("occupied-strata threshold must follow this-round budget")
        if thresholds.max_companies_this_expansion != self.budget.max_companies_this_round:
            raise ValueError("first-expansion company cap must equal this-round budget")
        if thresholds.max_tokens != self.budget.token_budget:
            raise ValueError("first-expansion token cap must equal the run budget")
        if thresholds.max_elapsed_seconds != self.budget.max_elapsed_seconds:
            raise ValueError("first-expansion time cap must equal the run budget")
        return self


def _sampling_rule(*, max_sample_size: int) -> StratifiedSamplingRule:
    return StratifiedSamplingRule(
        method="predeclared_stratified_sample",
        purpose="independent_source_review",
        allocation="priority_round_robin_then_instrument_id",
        selection_key="instrument_id_ascending",
        missing_classification_stratum="other",
        empty_stratum_policy="keep_in_universe_skip_draw",
        exchange_strata=_EXCHANGES,
        disclosure_form_strata=_DISCLOSURE_FORMS,
        stratum_priority=_STRATUM_PRIORITY,
        per_stratum_first_draw=1,
        max_sample_size=max_sample_size,
        excludes_from_universe=False,
        used_to_fit_thresholds=False,
    )


def _universe_denominator() -> UniverseDenominator:
    return UniverseDenominator(
        policy_version=PRODUCTION_SCOPE_POLICY,
        exchanges=_EXCHANGES,
        as_of="declared_at_run_from_knowledge_cutoff",
        includes_missing_assets=True,
        includes_missing_classification=True,
    )


def _recall_denominator() -> RecallDenominator:
    return RecallDenominator(
        definition=(
            "independently_read_important_disclosures_from_sampled_official_reports"
        ),
        forbidden_sources=_FORBIDDEN_RECALL_SOURCES,
    )


def _expansion_thresholds(
    *,
    max_companies_this_round: int,
    token_budget: int,
    max_elapsed_seconds: float,
) -> ExpansionQualityThresholds:
    return ExpansionQualityThresholds(
        numeric_thresholds_basis="operator_cost_constraint",
        min_independently_reviewed_reports=max_companies_this_round,
        min_occupied_strata_reviewed=min(
            max_companies_this_round, len(_STRATUM_PRIORITY)
        ),
        min_source_recall_ratio=1.0,
        min_source_accuracy_ratio=1.0,
        max_critical_numeric_errors=0,
        max_tokens=token_budget,
        max_elapsed_seconds=max_elapsed_seconds,
        max_companies_this_expansion=max_companies_this_round,
        scale_quality_claim_allowed=False,
        fitted_from_observed_results=False,
        fixture_guards_are_not_live_quality=True,
        gold24_equality_is_not_a_threshold=True,
        untriggered_negatives_are_not_passes=True,
        expansion_requires_independent_source_review=True,
    )


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
        sampling=_sampling_rule(max_sample_size=max_companies_this_round),
        universe_denominator=_universe_denominator(),
        recall_denominator=_recall_denominator(),
        expansion_thresholds=_expansion_thresholds(
            max_companies_this_round=max_companies_this_round,
            token_budget=token_budget,
            max_elapsed_seconds=max_elapsed_seconds,
        ),
    )


def select_stratified_review_sample(
    candidates: Sequence[Any],
    *,
    rule: StratifiedSamplingRule,
) -> tuple[str, ...]:
    """Pick instrument IDs with the predeclared rule. Ignores model output."""

    buckets: dict[tuple[str, str], list[str]] = {
        stratum: [] for stratum in rule.stratum_priority
    }
    for candidate in candidates:
        instrument_id = _instrument_id_of(candidate)
        exchange = _exchange_of(candidate)
        if not instrument_id or exchange not in rule.exchange_strata:
            continue
        form = assign_disclosure_form(candidate)
        buckets[(exchange, form)].append(instrument_id)
    ordered = {
        stratum: sorted(set(members)) for stratum, members in buckets.items()
    }
    selected: list[str] = []
    cursors = dict.fromkeys(rule.stratum_priority, 0)
    progressed = True
    while len(selected) < rule.max_sample_size and progressed:
        progressed = False
        for stratum in rule.stratum_priority:
            if len(selected) >= rule.max_sample_size:
                break
            members = ordered[stratum]
            index = cursors[stratum]
            if index < len(members):
                selected.append(members[index])
                cursors[stratum] = index + 1
                progressed = True
    return tuple(selected)


def live_plan_schema_manifest() -> dict[str, Any]:
    """Register the pre-live plan schema without starting a live task."""

    plan = record_company_profile_live_plan()
    return {
        "schema_version": LIVE_PLAN_SCHEMA_VERSION,
        "plan_status": LIVE_PLAN_STATUS,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "live_task_started": False,
        "forbidden_recall_sources": _FORBIDDEN_RECALL_SOURCES,
        "registered_budget": plan.budget.model_dump(mode="json"),
        "registered_sampling": plan.sampling.model_dump(mode="json"),
        "registered_expansion_thresholds": plan.expansion_thresholds.model_dump(
            mode="json"
        ),
        "plan_schema": CompanyProfileLivePlan.model_json_schema(),
    }
