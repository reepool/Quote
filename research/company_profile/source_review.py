"""Independent source review for a budget-limited company-profile live run.

This is the unique owner for company_profile_source_review.v1. It records
sampled core-skeleton, important-disclosure and commodity-role checks against
official reports. Structural existence is not semantic correctness. Unassessed
metrics stay unassessed and are not filled with zero. It does not call LLM or
enable production.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.live_run import CompanyProfileLiveRunReport
from research.company_profile.models import PRODUCTION_AUTHORIZATION

SOURCE_REVIEW_SCHEMA_VERSION = "company_profile_source_review.v1"
ReviewAspect = Literal["core_skeleton", "important_disclosure", "commodity_role"]
_REQUIRED_ASPECTS = frozenset(
    {"core_skeleton", "important_disclosure", "commodity_role"}
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class UnassessedValue(_StrictModel):
    status: Literal["unassessed"] = "unassessed"
    value: None = None


class AssessedRatio(_StrictModel):
    status: Literal["assessed"] = "assessed"
    numerator: int = Field(ge=0)
    denominator: int = Field(ge=1)
    value: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def _ratio_matches_counts(self) -> AssessedRatio:
        expected = self.numerator / self.denominator
        if abs(self.value - expected) > 1e-12:
            raise ValueError("assessed ratio must equal numerator/denominator")
        return self


class AssessedCount(_StrictModel):
    status: Literal["assessed"] = "assessed"
    value: int = Field(ge=0)


class AssessedDuration(_StrictModel):
    status: Literal["assessed"] = "assessed"
    value: float = Field(ge=0)


QualityRatio = Annotated[
    AssessedRatio | UnassessedValue,
    Field(discriminator="status"),
]
QualityCount = Annotated[
    AssessedCount | UnassessedValue,
    Field(discriminator="status"),
]
QualityDuration = Annotated[
    AssessedDuration | UnassessedValue,
    Field(discriminator="status"),
]


class StructuralCheck(_StrictModel):
    instrument_id: str = Field(min_length=1)
    aspect: ReviewAspect
    kind: Literal["structural"]
    method: Literal["evidence_id_existence", "hash_presence", "page_reference"]
    passed: bool


class SemanticFinding(_StrictModel):
    instrument_id: str = Field(min_length=1)
    aspect: ReviewAspect
    kind: Literal["semantic"]
    source: Literal["independently_read_official_report"]
    disclosure_id: str = Field(min_length=1)
    disclosed_in_source: bool
    present_in_delivery: bool
    fact_accurate: bool | None
    critical_numeric_error: bool

    @model_validator(mode="after")
    def _semantic_finding_is_independent(self) -> SemanticFinding:
        if self.kind != "semantic":
            raise ValueError("semantic findings cannot be structural checks")
        if self.fact_accurate is True and not self.present_in_delivery:
            raise ValueError("absent delivery cannot be marked fact-accurate")
        if self.critical_numeric_error and self.fact_accurate is True:
            raise ValueError("a critical numeric error cannot be fact-accurate")
        return self


class FixtureGuardResult(_StrictModel):
    channel: Literal["fixture_guard"]
    name: str = Field(min_length=1)
    passed: bool


class FreshnessObservation(_StrictModel):
    instrument_id: str = Field(min_length=1)
    report_period: str = Field(min_length=1)
    published_at: str = Field(min_length=1)
    knowledge_cutoff: str = Field(min_length=1)
    available_at_cutoff: bool


class DeliveryCoverage(_StrictModel):
    universe_total: int = Field(ge=0)
    selected_for_run: int = Field(ge=0)
    completed: int = Field(ge=0)
    failed: int = Field(ge=0)
    missing_asset: int = Field(ge=0)
    semantic_status: Literal["not_a_source_metric"]


class SourceReviewWorkload(_StrictModel):
    tokens_used: QualityCount
    elapsed_seconds: QualityDuration
    human_review_minutes: QualityDuration


class CompanyProfileSourceReviewReport(_StrictModel):
    schema_version: Literal["company_profile_source_review.v1"] = (
        SOURCE_REVIEW_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    live_run: CompanyProfileLiveRunReport
    structural_checks: tuple[StructuralCheck, ...]
    semantic_findings: tuple[SemanticFinding, ...]
    fixture_guards: tuple[FixtureGuardResult, ...]
    independently_reviewed_reports: int = Field(ge=0)
    occupied_strata_reviewed: QualityCount
    delivery_coverage: DeliveryCoverage
    source_recall: QualityRatio
    source_accuracy: QualityRatio
    critical_numeric_errors: QualityCount
    freshness: tuple[FreshnessObservation, ...]
    workload: SourceReviewWorkload
    expansion_gates_met: bool
    scale_quality_claim_allowed: Literal[False]
    zero_recurrence_claimed: Literal[False]

    @model_validator(mode="after")
    def _review_matches_live_sample_and_assessment(self) -> (
        CompanyProfileSourceReviewReport
    ):
        if self.scale_quality_claim_allowed:
            raise ValueError("source review cannot claim scale quality")
        if self.zero_recurrence_claimed:
            raise ValueError("source review cannot claim zero recurrence")
        selected = set(self.live_run.selected_instrument_ids)
        for item in (
            *self.structural_checks,
            *self.semantic_findings,
            *self.freshness,
        ):
            if item.instrument_id not in selected:
                raise ValueError("source review can only cover the live-run sample")
        finding_keys = [
            (item.instrument_id, item.aspect, item.disclosure_id)
            for item in self.semantic_findings
        ]
        if len(finding_keys) != len(set(finding_keys)):
            raise ValueError("semantic findings cannot repeat the same disclosure")
        derived = _derived_metrics(
            live_run=self.live_run,
            findings=self.semantic_findings,
            workload=self.workload,
        )
        if self.independently_reviewed_reports != derived["independently_reviewed_reports"]:
            raise ValueError("reviewed-report count must follow semantic findings")
        if self.occupied_strata_reviewed != derived["occupied_strata_reviewed"]:
            raise ValueError("occupied strata must follow the live-run sample layers")
        if self.source_recall != derived["source_recall"]:
            raise ValueError("source recall must follow semantic findings")
        if self.source_accuracy != derived["source_accuracy"]:
            raise ValueError("source accuracy must follow semantic findings")
        if self.critical_numeric_errors != derived["critical_numeric_errors"]:
            raise ValueError("critical numeric errors must follow semantic findings")
        if self.expansion_gates_met != derived["expansion_gates_met"]:
            raise ValueError("expansion gates must follow findings and the live plan")
        if not self.semantic_findings:
            if self.source_recall.status != "unassessed":
                raise ValueError("structure-only review cannot assess source recall")
            if self.source_accuracy.status != "unassessed":
                raise ValueError("structure-only review cannot assess source accuracy")
            if self.critical_numeric_errors.status != "unassessed":
                raise ValueError(
                    "structure-only review cannot assess critical numeric errors"
                )
            if self.occupied_strata_reviewed.status != "unassessed":
                raise ValueError("structure-only review cannot assess occupied strata")
            if self.expansion_gates_met:
                raise ValueError("unassessed source review cannot meet expansion gates")
        coverage = self.delivery_coverage
        universe = self.live_run.universe
        if (
            coverage.universe_total != universe.total
            or coverage.selected_for_run != universe.selected_for_run
            or coverage.completed != universe.completed
            or coverage.failed != universe.failed
            or coverage.missing_asset != universe.missing_asset
        ):
            raise ValueError("delivery coverage must follow the live-run denominator")
        return self


def record_source_review_report(
    *,
    live_run: CompanyProfileLiveRunReport,
    structural_checks: Sequence[StructuralCheck] = (),
    semantic_findings: Sequence[SemanticFinding] = (),
    fixture_guards: Sequence[FixtureGuardResult] = (),
    freshness: Sequence[FreshnessObservation] = (),
    tokens_used: int | None = None,
    elapsed_seconds: float | None = None,
    human_review_minutes: float | None = None,
) -> CompanyProfileSourceReviewReport:
    """Record independent source review without filling unassessed metrics as 0."""

    findings = tuple(semantic_findings)
    workload = SourceReviewWorkload(
        tokens_used=_count(tokens_used),
        elapsed_seconds=_duration(elapsed_seconds),
        human_review_minutes=_duration(human_review_minutes),
    )
    derived = _derived_metrics(
        live_run=live_run,
        findings=findings,
        workload=workload,
    )
    universe = live_run.universe
    return CompanyProfileSourceReviewReport(
        live_run=live_run,
        structural_checks=tuple(structural_checks),
        semantic_findings=findings,
        fixture_guards=tuple(fixture_guards),
        independently_reviewed_reports=derived["independently_reviewed_reports"],
        occupied_strata_reviewed=derived["occupied_strata_reviewed"],
        delivery_coverage=DeliveryCoverage(
            universe_total=universe.total,
            selected_for_run=universe.selected_for_run,
            completed=universe.completed,
            failed=universe.failed,
            missing_asset=universe.missing_asset,
            semantic_status="not_a_source_metric",
        ),
        source_recall=derived["source_recall"],
        source_accuracy=derived["source_accuracy"],
        critical_numeric_errors=derived["critical_numeric_errors"],
        freshness=tuple(freshness),
        workload=workload,
        expansion_gates_met=derived["expansion_gates_met"],
        scale_quality_claim_allowed=False,
        zero_recurrence_claimed=False,
    )


def persist_source_review_report(
    report: CompanyProfileSourceReviewReport,
    root: str | Path,
) -> Path:
    """Write company_profile_source_review.v1 next to the live-run report."""

    path = Path(root) / "reports" / f"{SOURCE_REVIEW_SCHEMA_VERSION}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def source_review_schema_manifest() -> dict[str, Any]:
    """Register the source-review schema without enabling production."""

    return {
        "schema_version": SOURCE_REVIEW_SCHEMA_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "scale_quality_claim_allowed": False,
        "zero_recurrence_claimed": False,
        "unassessed_is_not_zero": True,
        "report_schema": CompanyProfileSourceReviewReport.model_json_schema(),
    }


def _derived_metrics(
    *,
    live_run: CompanyProfileLiveRunReport,
    findings: Sequence[SemanticFinding],
    workload: SourceReviewWorkload,
) -> dict[str, Any]:
    reviewed = {item.instrument_id for item in findings}
    recall = _source_recall(findings)
    accuracy = _source_accuracy(findings)
    critical = _critical_errors(findings)
    occupied = _occupied_strata(live_run, findings)
    return {
        "independently_reviewed_reports": len(reviewed),
        "occupied_strata_reviewed": occupied,
        "source_recall": recall,
        "source_accuracy": accuracy,
        "critical_numeric_errors": critical,
        "expansion_gates_met": _expansion_gates_met(
            live_run=live_run,
            findings=findings,
            reviewed=reviewed,
            occupied=occupied,
            recall=recall,
            accuracy=accuracy,
            critical=critical,
            workload=workload,
        ),
    }


def _occupied_strata(
    live_run: CompanyProfileLiveRunReport,
    findings: Sequence[SemanticFinding],
) -> AssessedCount | UnassessedValue:
    reviewed = {item.instrument_id for item in findings}
    if not reviewed:
        return UnassessedValue()
    by_id = {
        item.instrument_id: (item.exchange, item.disclosure_form)
        for item in live_run.selected_strata
    }
    sampled = [instrument_id for instrument_id in reviewed if instrument_id in by_id]
    if not sampled:
        return UnassessedValue()
    return AssessedCount(value=len({by_id[instrument_id] for instrument_id in sampled}))


def _covers_required_aspects(findings: Sequence[SemanticFinding]) -> bool:
    reviewed: dict[str, set[str]] = {}
    for item in findings:
        reviewed.setdefault(item.instrument_id, set()).add(item.aspect)
    return bool(reviewed) and all(
        aspects >= _REQUIRED_ASPECTS for aspects in reviewed.values()
    )


def _expansion_gates_met(
    *,
    live_run: CompanyProfileLiveRunReport,
    findings: Sequence[SemanticFinding],
    reviewed: set[str],
    occupied: AssessedCount | UnassessedValue,
    recall: AssessedRatio | UnassessedValue,
    accuracy: AssessedRatio | UnassessedValue,
    critical: AssessedCount | UnassessedValue,
    workload: SourceReviewWorkload,
) -> bool:
    thresholds = live_run.plan.expansion_thresholds
    tokens = workload.tokens_used
    elapsed = workload.elapsed_seconds
    return (
        bool(findings)
        and _covers_required_aspects(findings)
        and len(reviewed) >= thresholds.min_independently_reviewed_reports
        and occupied.status == "assessed"
        and occupied.value >= thresholds.min_occupied_strata_reviewed
        and recall.status == "assessed"
        and recall.value >= thresholds.min_source_recall_ratio
        and accuracy.status == "assessed"
        and accuracy.value >= thresholds.min_source_accuracy_ratio
        and critical.status == "assessed"
        and critical.value <= thresholds.max_critical_numeric_errors
        and tokens.status == "assessed"
        and tokens.value <= thresholds.max_tokens
        and elapsed.status == "assessed"
        and elapsed.value <= thresholds.max_elapsed_seconds
    )


def _source_recall(
    findings: Sequence[SemanticFinding],
) -> AssessedRatio | UnassessedValue:
    disclosed = tuple(item for item in findings if item.disclosed_in_source)
    if not disclosed:
        return UnassessedValue()
    recalled = sum(item.present_in_delivery for item in disclosed)
    return AssessedRatio(
        numerator=recalled,
        denominator=len(disclosed),
        value=recalled / len(disclosed),
    )


def _source_accuracy(
    findings: Sequence[SemanticFinding],
) -> AssessedRatio | UnassessedValue:
    delivered = tuple(item for item in findings if item.present_in_delivery)
    if not delivered:
        return UnassessedValue()
    if any(item.fact_accurate is None for item in delivered):
        return UnassessedValue()
    accurate = sum(bool(item.fact_accurate) for item in delivered)
    return AssessedRatio(
        numerator=accurate,
        denominator=len(delivered),
        value=accurate / len(delivered),
    )


def _critical_errors(
    findings: Sequence[SemanticFinding],
) -> AssessedCount | UnassessedValue:
    if not findings:
        return UnassessedValue()
    return AssessedCount(
        value=sum(item.critical_numeric_error for item in findings)
    )


def _count(value: int | None) -> AssessedCount | UnassessedValue:
    if value is None:
        return UnassessedValue()
    return AssessedCount(value=int(value))


def _duration(value: float | None) -> AssessedDuration | UnassessedValue:
    if value is None:
        return UnassessedValue()
    return AssessedDuration(value=float(value))
