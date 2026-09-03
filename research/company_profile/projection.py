"""Pure research-facing projection for stage-four company-profile records."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .contracts import CompanyProfileTaskResult, DispositionStatus
from .models import (
    PRODUCTION_AUTHORIZATION,
    RESEARCH_VIEW_SCHEMA_VERSION,
    BusinessEvent,
    BusinessOverview,
    BusinessRegime,
    Evidence,
    IndustryPackageAssignment,
    Measurement,
    Relationship,
    RelationshipType,
    ReportIdentity,
    SemanticRecord,
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ResearchViewItem(_StrictModel):
    record_id: str
    field_id: str
    object_type: str
    assertion_class: str
    subject_scope: str
    reported_period: str
    source_native: dict[str, Any]
    evidence_ids: tuple[str, ...]
    details: dict[str, Any]


class ResearchBoundary(_StrictModel):
    status: Literal["not_authorized", "not_assessed", "insufficient_evidence"]
    facts: tuple[ResearchViewItem, ...] = ()
    supported_statements: tuple[str, ...] = ()


class CompanyProfileResearchView(_StrictModel):
    schema_version: Literal["company_profile_research_view.v1"] = (
        RESEARCH_VIEW_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    data_status: Literal["research_fixture"] = "research_fixture"
    company: dict[str, str]
    as_of: dict[str, str]
    business_overview: ResearchViewItem | None = None
    business_regime: tuple[ResearchViewItem, ...] = ()
    segments: tuple[ResearchViewItem, ...] = ()
    activities: tuple[ResearchViewItem, ...] = ()
    operating_measurements: tuple[ResearchViewItem, ...] = ()
    disclosed_inputs: tuple[ResearchViewItem, ...] = ()
    counterparties: tuple[ResearchViewItem, ...] = ()
    business_events: tuple[ResearchViewItem, ...] = ()
    coverage: tuple[dict[str, Any], ...] = ()
    commodity_exposure: ResearchBoundary
    value_chain_position: ResearchBoundary
    evidence_index: tuple[dict[str, Any], ...] = ()


def project_research_view(
    *,
    company_name: str,
    report: ReportIdentity,
    task_results: Sequence[CompanyProfileTaskResult],
) -> CompanyProfileResearchView:
    """Project accepted research fixtures without creating new facts.

    Stage four has no production approval state.  Only records with the explicit
    ``accepted_for_review`` disposition and ``research_fixture`` data status are
    displayable.
    """

    accepted: dict[str, SemanticRecord] = {}
    coverage_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for result in task_results:
        statuses = {item.target_id: item.status for item in result.dispositions}
        for record in result.records:
            if record.report != report:
                raise ValueError("research view cannot mix records from another report")
            if (
                statuses.get(record.record_id) == DispositionStatus.ACCEPTED_FOR_REVIEW
                and record.data_status == "research_fixture"
            ):
                accepted[record.record_id] = record
        for coverage in result.coverage:
            if any(item.report != report for item in coverage.evidence):
                raise ValueError("research view cannot mix coverage from another report")
            coverage_by_identity[(coverage.chapter_task.value, coverage.field_id)] = (
                coverage.model_dump(mode="json")
            )

    records = sorted(
        accepted.values(),
        key=lambda item: (
            min(evidence.page for evidence in item.evidence),
            item.object_type,
            item.record_id,
        ),
    )
    items = {record.record_id: _project_record(record) for record in records}

    overviews = [item for item in records if isinstance(item, BusinessOverview)]
    regimes = [
        item
        for item in records
        if isinstance(item, (BusinessRegime, IndustryPackageAssignment))
    ]
    events = [item for item in records if isinstance(item, BusinessEvent)]
    measurements = [item for item in records if isinstance(item, Measurement)]
    relationships = [item for item in records if isinstance(item, Relationship)]
    disclosed_inputs = [
        item
        for item in relationships
        if item.relation_type == RelationshipType.MATERIAL_INPUT
    ]
    counterparties = [item for item in relationships if item not in disclosed_inputs]

    evidence_index: dict[str, Evidence] = {}
    for record in records:
        for evidence in record.evidence:
            evidence_index[evidence.evidence_id] = evidence

    material_facts = tuple(items[item.record_id] for item in disclosed_inputs)
    return CompanyProfileResearchView(
        company={
            "instrument_id": report.instrument_id,
            "name": company_name,
        },
        as_of={
            "report_id": report.report_id,
            "report_period": report.report_period,
            "published_at": report.published_at,
            "document_version": report.document_version,
        },
        business_overview=items[overviews[0].record_id] if overviews else None,
        business_regime=tuple(items[item.record_id] for item in regimes),
        segments=tuple(
            items[item.record_id] for item in records if item.object_type == "Segment"
        ),
        activities=tuple(
            items[item.record_id] for item in records if item.object_type == "Activity"
        ),
        operating_measurements=tuple(items[item.record_id] for item in measurements),
        disclosed_inputs=material_facts,
        counterparties=tuple(items[item.record_id] for item in counterparties),
        business_events=tuple(items[item.record_id] for item in events),
        coverage=tuple(
            coverage_by_identity[key] for key in sorted(coverage_by_identity)
        ),
        commodity_exposure=ResearchBoundary(
            status="not_assessed",
            facts=material_facts,
            supported_statements=(),
        ),
        value_chain_position=ResearchBoundary(
            status="insufficient_evidence",
            facts=(),
            supported_statements=(),
        ),
        evidence_index=tuple(
            evidence_index[key].model_dump(mode="json")
            for key in sorted(evidence_index)
        ),
    )


def _project_record(record: SemanticRecord) -> ResearchViewItem:
    common = {
        "schema_version",
        "record_id",
        "field_id",
        "chapter_task",
        "report",
        "subject_scope",
        "reported_period",
        "period_type",
        "knowledge_time",
        "assertion_class",
        "evidence",
        "source_native",
        "uncertainty",
        "data_status",
        "object_type",
    }
    payload = record.model_dump(mode="json")
    details = {key: value for key, value in payload.items() if key not in common}
    if record.uncertainty:
        details["uncertainty"] = list(record.uncertainty)
    if record.knowledge_time:
        details["knowledge_time"] = record.knowledge_time
    return ResearchViewItem(
        record_id=record.record_id,
        field_id=record.field_id,
        object_type=record.object_type,
        assertion_class=record.assertion_class.value,
        subject_scope=record.subject_scope.value,
        reported_period=record.reported_period,
        source_native=record.source_native.model_dump(mode="json"),
        evidence_ids=tuple(item.evidence_id for item in record.evidence),
        details=details,
    )
