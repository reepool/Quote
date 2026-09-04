"""Single application owner for the isolated manufacturing/materials slice."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator

from .contracts import (
    ChecklistItem,
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
    HumanReviewItem,
    PackageManifest,
    PreparedEvidence,
    SemanticProvider,
    SemanticTaskRequest,
)
from .models import (
    ActivityAction,
    ChapterTask,
    CoverageResult,
    CoverageStatus,
    MetricType,
    ObjectType,
    Relationship,
    RequirementLevel,
    SemanticRecord,
)
from .projection import project_research_view
from .stage5 import (
    APPROVED_STAGE5_SAMPLES,
    PreparedRequestScope,
    Stage5EvidencePlan,
    Stage5EvidencePreparer,
    Stage5SampleManifest,
)
from .stage5_bundle import (
    Stage5BenchmarkDimension,
    Stage5BenchmarkResult,
    Stage5FailureDiagnostic,
    Stage5OverallStatus,
    Stage5PreparationBundle,
    Stage5PreparedScopeSummary,
    Stage5ReportBundle,
    Stage5ReportStatus,
    Stage5ReviewAction,
    Stage5ReviewDecision,
    Stage5RunBundle,
    Stage5RunBundleStore,
    Stage5ScopeResult,
    stage5_evidence_plan_hash,
)
from .workflow import CompanyProfileSemanticService

_ALL_COVERAGE = tuple(CoverageStatus)
_ACTIVITY_ACTIONS = tuple(ActivityAction)
_REVIEW_ACTIONS = (
    Stage5ReviewAction.ACCEPT_FOR_RESEARCH_REVIEW.value,
    Stage5ReviewAction.REJECT.value,
    Stage5ReviewAction.HOLD.value,
    Stage5ReviewAction.REQUEST_REPAIR.value,
)
_FIELD_CONTRACT: dict[
    str,
    tuple[ObjectType, RequirementLevel, tuple[MetricType, ...], tuple[ActivityAction, ...]],
] = {
    "business_overview_source": (
        ObjectType.BUSINESS_OVERVIEW,
        RequirementLevel.REQUIRED,
        (),
        (),
    ),
    "explicit_activity": (
        ObjectType.ACTIVITY,
        RequirementLevel.CONDITIONAL,
        (),
        _ACTIVITY_ACTIONS,
    ),
    "business_regime": (
        ObjectType.BUSINESS_EVENT,
        RequirementLevel.REQUIRED,
        (),
        (),
    ),
    "segment_dimension": (
        ObjectType.SEGMENT,
        RequirementLevel.REQUIRED,
        (),
        (),
    ),
    "operating_revenue": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.OPERATING_REVENUE,),
        (),
    ),
    "operating_cost": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.OPERATING_COST,),
        (),
    ),
    "gross_margin_reported": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.GROSS_MARGIN_REPORTED,),
        (),
    ),
    "production_capacity": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.PRODUCTION_CAPACITY,),
        (),
    ),
    "capacity_under_construction": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.CAPACITY_UNDER_CONSTRUCTION,),
        (),
    ),
    "capacity_utilization": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.CAPACITY_UTILIZATION,),
        (),
    ),
    "production_volume": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.PRODUCTION_VOLUME,),
        (),
    ),
    "sales_volume": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.SALES_VOLUME,),
        (),
    ),
    "inventory_volume": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.INVENTORY_VOLUME,),
        (),
    ),
    "processing_volume": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.PROCESSING_VOLUME,),
        (),
    ),
    "material_input": (
        ObjectType.RELATIONSHIP,
        RequirementLevel.CONDITIONAL,
        (),
        (),
    ),
    "counterparty_relationship": (
        ObjectType.RELATIONSHIP,
        RequirementLevel.CONDITIONAL,
        (),
        (),
    ),
    "customer_concentration": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.CUSTOMER_SALES_AMOUNT, MetricType.DISCLOSED_SHARE),
        (),
    ),
    "supplier_concentration": (
        ObjectType.MEASUREMENT,
        RequirementLevel.CONDITIONAL,
        (MetricType.SUPPLIER_PURCHASE_AMOUNT, MetricType.DISCLOSED_SHARE),
        (),
    ),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Stage5SemanticInput(_StrictModel):
    deterministic_candidates: tuple[SemanticRecord, ...] = ()
    provided_coverage: tuple[CoverageResult, ...] = ()
    unresolved_field_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _targets_are_unique(self) -> Stage5SemanticInput:
        if len(self.unresolved_field_ids) != len(set(self.unresolved_field_ids)):
            raise ValueError("stage-five unresolved fields must be unique")
        return self


class Stage5SliceExecution(_StrictModel):
    run_id: str
    mode: Literal["preparation_only", "semantic_run"]
    output_path: Path
    overall_status: str
    report_statuses: dict[str, str]
    production_authorization: Literal["not_authorized"] = "not_authorized"


SemanticInputFactory = Callable[[PreparedRequestScope], Stage5SemanticInput]
ProviderFactory = Callable[[PreparedRequestScope], SemanticProvider | None]


class ManufacturingMaterialsProfileSliceService:
    """The only stage-five owner for preparation, semantics, and persistence."""

    def __init__(
        self,
        *,
        evidence_preparer: Stage5EvidencePreparer | None = None,
        semantic_service: CompanyProfileSemanticService | None = None,
    ) -> None:
        self._evidence_preparer = evidence_preparer or Stage5EvidencePreparer()
        self._semantic_service = semantic_service or CompanyProfileSemanticService()

    def run_preparation_only(
        self,
        *,
        run_id: str,
        manifest: Stage5SampleManifest,
        evidence_plan: Stage5EvidencePlan,
        evidence_plan_path: str | Path,
        store: Stage5RunBundleStore,
        sample_ids: Iterable[str] | None = None,
    ) -> Stage5SliceExecution:
        selected = self._selected_sample_ids(sample_ids)
        prepared = self._prepare_selected(manifest, evidence_plan, selected)
        scopes = tuple(
            Stage5PreparedScopeSummary(
                sample_id=scope.sample_id,
                scope_id=scope.scope_id,
                chapter_task=scope.chapter_task.value,
                field_ids=scope.field_ids,
                physical_pages=tuple(item.page for item in scope.page_contexts),
                evidence_ids=tuple(
                    item.evidence.evidence_id for item in scope.evidence_bundle
                ),
                page_text_hashes=tuple(item.text_hash for item in scope.page_contexts),
            )
            for sample_id in selected
            for scope in prepared[sample_id]
        )
        bundle = Stage5PreparationBundle(
            run_id=run_id,
            sample_manifest_revision=manifest.manifest_revision,
            evidence_plan_version=evidence_plan.plan_version,
            evidence_plan_hash=stage5_evidence_plan_hash(evidence_plan_path),
            scopes=scopes,
            created_at=_utc_now(),
        )
        destination = store.commit_preparation(bundle)
        return Stage5SliceExecution(
            run_id=run_id,
            mode="preparation_only",
            output_path=destination,
            overall_status="prepared",
            report_statuses={sample_id: "prepared" for sample_id in selected},
        )

    def run_semantic_slice(
        self,
        *,
        run_id: str,
        manifest: Stage5SampleManifest,
        evidence_plan: Stage5EvidencePlan,
        evidence_plan_path: str | Path,
        store: Stage5RunBundleStore,
        provider_factory: ProviderFactory,
        semantic_input_factory: SemanticInputFactory | None = None,
        sample_ids: Iterable[str] | None = None,
        review_decisions: Mapping[str, tuple[Stage5ReviewDecision, ...]] | None = None,
    ) -> Stage5SliceExecution:
        selected = self._selected_sample_ids(sample_ids)
        try:
            prepared = self._prepare_selected(manifest, evidence_plan, selected)
            reports = tuple(
                self._run_report(
                    run_id=run_id,
                    manifest=manifest,
                    evidence_plan=evidence_plan,
                    evidence_plan_path=evidence_plan_path,
                    sample_id=sample_id,
                    prepared_scopes=prepared[sample_id],
                    provider_factory=provider_factory,
                    semantic_input_factory=semantic_input_factory,
                    review_decisions=(review_decisions or {}).get(sample_id, ()),
                )
                for sample_id in selected
            )
            overall = _overall_status(reports, selected)
            bundle = Stage5RunBundle(
                run_id=run_id,
                sample_manifest_revision=manifest.manifest_revision,
                evidence_plan_version=evidence_plan.plan_version,
                reports=reports,
                overall_status=overall,
                retained_bundle_ids=(),
                created_at=_utc_now(),
            )
            destination = store.commit(bundle)
        except Exception as exc:
            store.record_failure(
                run_id,
                (
                    Stage5FailureDiagnostic(
                        code=type(exc).__name__,
                        message=str(exc)[:2000] or "stage-five semantic run failed",
                    ),
                ),
            )
            raise
        return Stage5SliceExecution(
            run_id=run_id,
            mode="semantic_run",
            output_path=destination,
            overall_status=overall.value,
            report_statuses={
                item.sample_id: item.report_status.value for item in reports
            },
        )

    def _run_report(
        self,
        *,
        run_id: str,
        manifest: Stage5SampleManifest,
        evidence_plan: Stage5EvidencePlan,
        evidence_plan_path: str | Path,
        sample_id: str,
        prepared_scopes: tuple[PreparedRequestScope, ...],
        provider_factory: ProviderFactory,
        semantic_input_factory: SemanticInputFactory | None,
        review_decisions: tuple[Stage5ReviewDecision, ...],
    ) -> Stage5ReportBundle:
        asset = manifest.report_by_id(sample_id)
        scope_results: list[Stage5ScopeResult] = []
        task_results: list[CompanyProfileTaskResult] = []
        for scope in prepared_scopes:
            semantic_input = (
                semantic_input_factory(scope)
                if semantic_input_factory is not None
                else Stage5SemanticInput(unresolved_field_ids=scope.field_ids)
            )
            request = _semantic_request(run_id, asset, scope, semantic_input)
            provider = provider_factory(scope) if request.unresolved_field_ids else None
            task_result = self._semantic_service.run_task(request, provider=provider)
            task_result = _normalize_review_actions(task_result)
            task_result = _suppress_same_scope_legal_empty_relationships(task_result)
            traces = tuple(getattr(provider, "traces", ())) if provider is not None else ()
            scope_result = Stage5ScopeResult(
                scope_id=scope.scope_id,
                request_id=request.request_id,
                prepared_scope=scope,
                task_result=task_result,
                provider_call_types=tuple(task_result.provider_calls),
                provider_traces=traces,
            )
            scope_results.append(scope_result)
            task_results.append(task_result)
        view = project_research_view(
            company_name=asset.company_name,
            report=asset.report,
            task_results=task_results,
        )
        benchmark = _contract_benchmark(task_results)
        report_status = (
            Stage5ReportStatus.COMPLETE
            if benchmark.decision == "pass"
            else Stage5ReportStatus.HOLD
        )
        return Stage5ReportBundle(
            run_id=run_id,
            sample_id=sample_id,
            company_name=asset.company_name,
            report=asset.report,
            sample_manifest_revision=manifest.manifest_revision,
            evidence_plan_version=evidence_plan.plan_version,
            evidence_plan_hash=stage5_evidence_plan_hash(evidence_plan_path),
            scope_results=tuple(scope_results),
            review_decisions=review_decisions,
            research_view=view,
            report_status=report_status,
            benchmark=benchmark,
            created_at=_utc_now(),
        )

    def _prepare_selected(
        self,
        manifest: Stage5SampleManifest,
        evidence_plan: Stage5EvidencePlan,
        selected: tuple[str, ...],
    ) -> dict[str, tuple[PreparedRequestScope, ...]]:
        return {
            sample_id: self._evidence_preparer.prepare_report(
                manifest=manifest,
                evidence_plan=evidence_plan,
                sample_id=sample_id,
            )
            for sample_id in selected
        }

    @staticmethod
    def _selected_sample_ids(sample_ids: Iterable[str] | None) -> tuple[str, ...]:
        selected = tuple(sample_ids or APPROVED_STAGE5_SAMPLES)
        if not selected or len(selected) != len(set(selected)):
            raise ValueError("stage-five sample selection must be non-empty and unique")
        unknown = set(selected) - set(APPROVED_STAGE5_SAMPLES)
        if unknown:
            raise ValueError(f"unapproved stage-five samples: {sorted(unknown)}")
        return selected


def _semantic_request(
    run_id: str,
    asset: Any,
    scope: PreparedRequestScope,
    semantic_input: Stage5SemanticInput,
) -> SemanticTaskRequest:
    if set(semantic_input.unresolved_field_ids) - set(scope.field_ids):
        raise ValueError("semantic input requests a field outside its request scope")
    active_fields = tuple(dict.fromkeys(scope.field_ids))
    checklist = tuple(_checklist_item(field_id, scope.chapter_task) for field_id in active_fields)
    manifest = PackageManifest(
        package_name="manufacturing_materials",
        package_version="v1",
        report=asset.report,
        checklist=checklist,
    )
    allowed_objects = tuple(
        dict.fromkeys(_FIELD_CONTRACT[field_id][0] for field_id in active_fields)
    )
    if "business_regime" in active_fields:
        allowed_objects += (
            ObjectType.BUSINESS_REGIME,
            ObjectType.INDUSTRY_PACKAGE_ASSIGNMENT,
        )
    allowed_metrics = tuple(
        dict.fromkeys(
            metric
            for field_id in active_fields
            for metric in _FIELD_CONTRACT[field_id][2]
        )
    )
    allowed_actions = tuple(
        dict.fromkeys(
            action
            for field_id in active_fields
            for action in _FIELD_CONTRACT[field_id][3]
        )
    )
    return SemanticTaskRequest(
        request_id=f"{run_id}:{scope.sample_id}:{scope.scope_id}",
        report=asset.report,
        package_manifest=manifest,
        chapter_task=scope.chapter_task,
        evidence_bundle=_field_bound_evidence(scope),
        allowed_object_types=allowed_objects,
        allowed_metric_types=allowed_metrics,
        allowed_actions=allowed_actions,
        prohibited_inferences=(
            "industry-knowledge completion",
            "commodity exposure or price sensitivity",
            "value-chain role",
            "production approval or publication eligibility",
            "cross-report anonymous identity merge",
            "current regime retroactively overwrites history",
        ),
        deterministic_candidates=semantic_input.deterministic_candidates,
        provided_coverage=semantic_input.provided_coverage,
        unresolved_field_ids=semantic_input.unresolved_field_ids,
    )


def _field_bound_evidence(scope: PreparedRequestScope) -> tuple[PreparedEvidence, ...]:
    return tuple(
        item.model_copy(update={"field_id": field_id})
        for field_id in scope.field_ids
        for item in scope.evidence_bundle
    )


def _checklist_item(field_id: str, chapter_task: ChapterTask) -> ChecklistItem:
    try:
        object_type, requirement, metrics, actions = _FIELD_CONTRACT[field_id]
    except KeyError as exc:
        raise ValueError(f"unknown stage-five checklist field: {field_id}") from exc
    return ChecklistItem(
        field_id=field_id,
        object_type=object_type,
        chapter_task=chapter_task,
        requirement_level=requirement,
        allowed_coverage_statuses=_ALL_COVERAGE,
        allowed_metric_types=metrics,
        allowed_actions=actions,
    )


def _normalize_review_actions(
    result: CompanyProfileTaskResult,
) -> CompanyProfileTaskResult:
    return result.model_copy(
        update={
            "human_review_items": tuple(
                item.model_copy(update={"allowed_actions": _REVIEW_ACTIONS})
                for item in result.human_review_items
            )
        }
    )


def _suppress_same_scope_legal_empty_relationships(
    result: CompanyProfileTaskResult,
) -> CompanyProfileTaskResult:
    legal_empty = any(
        item.field_id == "counterparty_relationship"
        and item.status == CoverageStatus.NOT_DISCLOSED
        for item in result.coverage
    )
    if not legal_empty:
        return result
    dispositions = {item.target_id: item for item in result.dispositions}
    reviews = list(result.human_review_items)
    changed = False
    for record in result.records:
        disposition = dispositions.get(record.record_id)
        if (
            isinstance(record, Relationship)
            and record.field_id == "counterparty_relationship"
            and disposition is not None
            and disposition.status == DispositionStatus.ACCEPTED_FOR_REVIEW
        ):
            blocked = Disposition(
                target_id=record.record_id,
                field_id=record.field_id,
                status=DispositionStatus.BLOCKED,
                reason_codes=(ContractErrorCode.PROHIBITED_INFERENCE,),
            )
            dispositions[record.record_id] = blocked
            reviews.append(
                HumanReviewItem(
                    review_id=f"{result.request_id}:legal-empty:{record.record_id}",
                    field_id=record.field_id,
                    candidate=record,
                    evidence=record.evidence,
                    reason_codes=(ContractErrorCode.PROHIBITED_INFERENCE,),
                    conflicting_interpretations=(
                        "same request scope declares counterparty names not_disclosed",
                    ),
                    allowed_actions=_REVIEW_ACTIONS,
                )
            )
            changed = True
    if not changed:
        return result
    return result.model_copy(
        update={
            "dispositions": tuple(dispositions[key] for key in sorted(dispositions)),
            "human_review_items": tuple(reviews),
            "task_complete": False,
        }
    )


def _contract_benchmark(
    task_results: list[CompanyProfileTaskResult],
) -> Stage5BenchmarkResult:
    incomplete = [item.request_id for item in task_results if not item.task_complete]
    production_boundary_ok = all(
        item.production_authorization == "not_authorized" for item in task_results
    )
    dimensions = (
        Stage5BenchmarkDimension(
            name="task_completion",
            passed=not incomplete,
            blocker_codes=("incomplete_request_scope",) if incomplete else (),
            details={"incomplete_request_ids": incomplete},
        ),
        Stage5BenchmarkDimension(
            name="production_isolation",
            passed=production_boundary_ok,
            blocker_codes=() if production_boundary_ok else ("production_authorized",),
        ),
        Stage5BenchmarkDimension(
            name="bounded_provider_calls",
            passed=all(
                item.provider_calls.count("repair") <= 1 for item in task_results
            ),
            blocker_codes=tuple(
                "unbounded_repair"
                for item in task_results
                if item.provider_calls.count("repair") > 1
            ),
        ),
    )
    return Stage5BenchmarkResult(
        decision="pass" if all(item.passed for item in dimensions) else "hold",
        dimensions=dimensions,
    )


def _overall_status(
    reports: tuple[Stage5ReportBundle, ...],
    selected: tuple[str, ...],
) -> Stage5OverallStatus:
    if any(item.report_status == Stage5ReportStatus.FAILED for item in reports):
        return Stage5OverallStatus.FAILED
    accepted = {
        item.sample_id
        for item in reports
        if any(
            decision.action == Stage5ReviewAction.ACCEPT_FOR_RESEARCH_REVIEW
            for decision in item.review_decisions
        )
    }
    if (
        set(selected) == set(APPROVED_STAGE5_SAMPLES)
        and all(item.report_status == Stage5ReportStatus.COMPLETE for item in reports)
        and accepted == set(APPROVED_STAGE5_SAMPLES)
    ):
        return Stage5OverallStatus.RESEARCH_SLICE_PASS
    return Stage5OverallStatus.HOLD


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
