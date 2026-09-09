"""Single application owner for the isolated manufacturing/materials slice."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator

from .acceptance_policy import (
    CORE_CHAPTERS,
    accepted_has_illegal_group_promotion,
)
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
    Activity,
    ActivityAction,
    ChapterTask,
    CoverageResult,
    CoverageStatus,
    MetricType,
    ObjectType,
    Relationship,
    RequirementLevel,
    SemanticRecord,
    SubjectBasis,
    SubjectScope,
)
from .projection import project_research_view
from .stage5 import (
    PreparedRequestScope,
    Stage5EvidencePlan,
    Stage5EvidencePreparer,
    Stage5SampleManifest,
)
from .stage5_bundle import (
    Stage5ActivityReviewDecision,
    Stage5BenchmarkDimension,
    Stage5BenchmarkResult,
    Stage5FailureDiagnostic,
    Stage5OfflineActivityReviewRequest,
    Stage5OfflineActivityReviewResult,
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
    tuple[
        ObjectType, RequirementLevel, tuple[MetricType, ...], tuple[ActivityAction, ...]
    ],
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
        scope_ids: Iterable[str] | None = None,
    ) -> Stage5SliceExecution:
        selected = self._selected_sample_ids(manifest, sample_ids)
        prepared = self._prepare_selected(
            manifest,
            evidence_plan,
            selected,
            scope_ids=scope_ids,
        )
        _validate_prepared_field_contract(prepared)
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
        scope_ids: Iterable[str] | None = None,
        review_decisions: Mapping[str, tuple[Stage5ReviewDecision, ...]] | None = None,
    ) -> Stage5SliceExecution:
        selected = self._selected_sample_ids(manifest, sample_ids)
        try:
            prepared = self._prepare_selected(
                manifest,
                evidence_plan,
                selected,
                scope_ids=scope_ids,
            )
            _validate_prepared_field_contract(prepared)
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
            traces = (
                tuple(getattr(provider, "traces", ())) if provider is not None else ()
            )
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
        benchmark = _contract_benchmark(task_results, scope_results)
        report_status = _derive_report_status(
            task_results=task_results,
            scope_results=scope_results,
            benchmark=benchmark,
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

    def apply_committed_activity_reviews(
        self,
        *,
        run_directory: str | Path,
        review_request: Stage5OfflineActivityReviewRequest,
    ) -> Stage5OfflineActivityReviewResult:
        """Apply source-bound Activity decisions without invoking a provider."""

        run_path = Path(run_directory).resolve()
        if not run_path.is_dir() or not run_path.name.startswith("run-"):
            raise ValueError("offline Activity review requires a committed run directory")
        manifest_path = run_path / "manifest.json"
        report_path = run_path / "reports" / f"{review_request.sample_id}.json"
        manifest_hash = _sha256_file(manifest_path)
        report_hash = _sha256_file(report_path)
        if manifest_hash != review_request.source_manifest_sha256:
            raise ValueError("offline Activity review manifest hash mismatch")
        if report_hash != review_request.source_report_sha256:
            raise ValueError("offline Activity review report hash mismatch")
        try:
            bundle = Stage5RunBundle.model_validate_json(
                manifest_path.read_text(encoding="utf-8")
            )
            report_file = Stage5ReportBundle.model_validate_json(
                report_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise ValueError("offline Activity review source bundle is unreadable") from exc
        if bundle.run_id != review_request.source_run_id:
            raise ValueError("offline Activity review run identity mismatch")
        matches = [
            item for item in bundle.reports if item.sample_id == review_request.sample_id
        ]
        if len(matches) != 1 or matches[0] != report_file:
            raise ValueError("offline Activity review report identity mismatch")

        source_report = matches[0]
        derived_report = _apply_activity_review_decisions(
            source_report,
            review_request.decisions,
        )
        accepted_count = sum(
            len(scope.task_result.accepted_records())
            for scope in derived_report.scope_results
        )
        review_count = sum(
            len(scope.task_result.human_review_items)
            for scope in derived_report.scope_results
        )
        return Stage5OfflineActivityReviewResult(
            adjudication_id=review_request.adjudication_id,
            source_run_id=review_request.source_run_id,
            source_manifest_sha256=manifest_hash,
            source_report_sha256=report_hash,
            decisions=review_request.decisions,
            original_report_status=source_report.report_status,
            derived_report_status=derived_report.report_status,
            accepted_record_count=accepted_count,
            remaining_human_review_count=review_count,
            report=derived_report,
            created_at=_utc_now(),
        )

    def _prepare_selected(
        self,
        manifest: Stage5SampleManifest,
        evidence_plan: Stage5EvidencePlan,
        selected: tuple[str, ...],
        *,
        scope_ids: Iterable[str] | None = None,
    ) -> dict[str, tuple[PreparedRequestScope, ...]]:
        prepared = {
            sample_id: self._evidence_preparer.prepare_report(
                manifest=manifest,
                evidence_plan=evidence_plan,
                sample_id=sample_id,
            )
            for sample_id in selected
        }
        if scope_ids is None:
            return prepared
        requested = tuple(scope_ids)
        if not requested or len(requested) != len(set(requested)):
            raise ValueError("stage-five scope selection must be non-empty and unique")
        available_scope_ids = {
            scope.scope_id for scopes in prepared.values() for scope in scopes
        }
        unknown = set(requested) - available_scope_ids
        if unknown:
            raise ValueError(
                f"unknown stage-five scopes for selected samples: {sorted(unknown)}"
            )
        selected_scopes: dict[str, tuple[PreparedRequestScope, ...]] = {}
        for sample_id in selected:
            available = {scope.scope_id: scope for scope in prepared[sample_id]}
            matched = tuple(
                available[scope_id] for scope_id in requested if scope_id in available
            )
            if not matched:
                raise ValueError(
                    "stage-five scope selection matched no scope for selected sample: "
                    f"{sample_id}"
                )
            selected_scopes[sample_id] = matched
        return selected_scopes

    @staticmethod
    def _selected_sample_ids(
        manifest: Stage5SampleManifest,
        sample_ids: Iterable[str] | None,
    ) -> tuple[str, ...]:
        allowed = tuple(item.sample_id for item in manifest.reports)
        selected = tuple(sample_ids or allowed)
        if not selected or len(selected) != len(set(selected)):
            raise ValueError("stage-five sample selection must be non-empty and unique")
        unknown = set(selected) - set(allowed)
        if unknown:
            raise ValueError(
                f"samples are outside the loaded stage-five manifest: {sorted(unknown)}"
            )
        return selected


def _validate_prepared_field_contract(
    prepared: Mapping[str, tuple[PreparedRequestScope, ...]],
) -> None:
    """Reject unknown checklist fields before constructing any provider request."""

    for sample_id, scopes in prepared.items():
        for scope in scopes:
            unknown = sorted(set(scope.field_ids) - set(_FIELD_CONTRACT))
            if unknown:
                raise ValueError(
                    "unknown stage-five checklist fields before provider execution: "
                    f"{sample_id}:{scope.scope_id}:{unknown}"
                )


def _semantic_request(
    run_id: str,
    asset: Any,
    scope: PreparedRequestScope,
    semantic_input: Stage5SemanticInput,
) -> SemanticTaskRequest:
    if set(semantic_input.unresolved_field_ids) - set(scope.field_ids):
        raise ValueError("semantic input requests a field outside its request scope")
    active_fields = _active_scope_fields(scope)
    checklist = tuple(
        _checklist_item(field_id, scope.chapter_task) for field_id in active_fields
    )
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
    unresolved_fields = tuple(
        field_id
        for field_id in semantic_input.unresolved_field_ids
        if field_id in active_fields
    )
    return SemanticTaskRequest(
        request_id=f"{run_id}:{scope.sample_id}:{scope.scope_id}",
        report=asset.report,
        package_manifest=manifest,
        chapter_task=scope.chapter_task,
        evidence_bundle=_field_bound_evidence(scope, active_fields=active_fields),
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
        unresolved_field_ids=unresolved_fields,
    )


def _active_scope_fields(scope: PreparedRequestScope) -> tuple[str, ...]:
    """Keep disclosure-specific scopes inside their frozen semantic boundary."""

    fields = tuple(dict.fromkeys(scope.field_ids))
    if scope.scope_id == "related_party_sales_purchases_and_services":
        return tuple(
            field_id for field_id in fields if field_id == "counterparty_relationship"
        )
    return fields


def _field_bound_evidence(
    scope: PreparedRequestScope,
    *,
    active_fields: tuple[str, ...] | None = None,
) -> tuple[PreparedEvidence, ...]:
    fields = scope.field_ids if active_fields is None else active_fields
    return tuple(
        item.model_copy(update={"field_id": field_id})
        for field_id in fields
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


def _apply_activity_review_decisions(
    report: Stage5ReportBundle,
    decisions: tuple[Stage5ActivityReviewDecision, ...],
) -> Stage5ReportBundle:
    source_review_counts = {
        decision.review_id: sum(
            item.review_id == decision.review_id
            for scope in report.scope_results
            for item in scope.task_result.human_review_items
        )
        for decision in decisions
    }
    missing = sorted(
        review_id for review_id, count in source_review_counts.items() if count == 0
    )
    duplicate = sorted(
        review_id for review_id, count in source_review_counts.items() if count > 1
    )
    if missing:
        raise ValueError(f"offline Activity review targets are missing: {missing}")
    if duplicate:
        raise ValueError(f"offline Activity review targets are not unique: {duplicate}")

    pending = {item.review_id: item for item in decisions}
    updated_scopes: list[Stage5ScopeResult] = []
    recorded_decisions = list(report.review_decisions)

    for scope in report.scope_results:
        result = scope.task_result
        reviews_by_id = {item.review_id: item for item in result.human_review_items}
        applicable = [
            decision for decision in decisions if decision.review_id in reviews_by_id
        ]
        if not applicable:
            updated_scopes.append(scope)
            continue

        records = {item.record_id: item for item in result.records}
        dispositions = {item.target_id: item for item in result.dispositions}
        removed_review_ids: set[str] = set()
        for decision in applicable:
            review = reviews_by_id[decision.review_id]
            candidate = _validated_activity_review_candidate(
                decision=decision,
                review=review,
                records=records,
                dispositions=dispositions,
            )
            records[candidate.record_id] = candidate.model_copy(
                update={"actor_basis": SubjectBasis.DIRECT_GRAMMATICAL_ACTOR}
            )
            dispositions[candidate.record_id] = Disposition(
                target_id=candidate.record_id,
                field_id=candidate.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            )
            removed_review_ids.add(decision.review_id)
            pending.pop(decision.review_id)
            recorded_decisions.append(
                Stage5ReviewDecision(
                    review_id=decision.review_id,
                    action=Stage5ReviewAction.ACCEPT_FOR_RESEARCH_REVIEW,
                    reason=decision.reason,
                )
            )

        remaining_reviews = [
            item
            for item in result.human_review_items
            if item.review_id not in removed_review_ids
        ]
        unresolved_activity = any(
            item.field_id == "explicit_activity" and item.candidate is not None
            for item in remaining_reviews
        )
        accepted_activities = [
            record
            for record in records.values()
            if isinstance(record, Activity)
            and record.field_id == "explicit_activity"
            and dispositions.get(record.record_id) is not None
            and dispositions[record.record_id].status
            == DispositionStatus.ACCEPTED_FOR_REVIEW
        ]
        coverage = list(result.coverage)
        if accepted_activities and not unresolved_activity:
            evidence_by_id = {
                evidence.evidence_id: evidence
                for record in accepted_activities
                for evidence in record.evidence
            }
            coverage = [
                item.model_copy(
                    update={
                        "status": CoverageStatus.OBSERVED,
                        "reason_code": None,
                        "reason": None,
                        "evidence": tuple(evidence_by_id.values()),
                        "reason_evidence_text": None,
                    }
                )
                if item.field_id == "explicit_activity"
                else item
                for item in coverage
            ]
            remaining_reviews = [
                item
                for item in remaining_reviews
                if not (
                    item.field_id == "explicit_activity"
                    and item.candidate is None
                    and ContractErrorCode.REQUIRED_COVERAGE_MISSING
                    in item.reason_codes
                )
            ]

        updated_result = result.model_copy(
            update={
                "records": tuple(records[item.record_id] for item in result.records),
                "dispositions": tuple(
                    dispositions[item.target_id] for item in result.dispositions
                ),
                "coverage": tuple(coverage),
                "human_review_items": tuple(remaining_reviews),
                "task_complete": (
                    not remaining_reviews
                    and all(
                        item.status == DispositionStatus.ACCEPTED_FOR_REVIEW
                        for item in dispositions.values()
                    )
                    and all(
                        item.status
                        not in {
                            CoverageStatus.EXTRACTION_FAILED,
                            CoverageStatus.UNCLEAR,
                        }
                        for item in coverage
                    )
                ),
            }
        )
        updated_scopes.append(scope.model_copy(update={"task_result": updated_result}))

    if pending:
        raise ValueError(
            f"offline Activity review targets are missing: {sorted(pending)}"
        )
    task_results = [item.task_result for item in updated_scopes]
    view = project_research_view(
        company_name=report.company_name,
        report=report.report,
        task_results=task_results,
    )
    benchmark = _contract_benchmark(task_results, updated_scopes)
    status = _derive_report_status(
        task_results=task_results,
        scope_results=updated_scopes,
        benchmark=benchmark,
    )
    return report.model_copy(
        update={
            "scope_results": tuple(updated_scopes),
            "review_decisions": tuple(recorded_decisions),
            "research_view": view,
            "report_status": status,
            "benchmark": benchmark,
            "created_at": _utc_now(),
        }
    )


def _validated_activity_review_candidate(
    *,
    decision: Stage5ActivityReviewDecision,
    review: HumanReviewItem,
    records: Mapping[str, SemanticRecord],
    dispositions: Mapping[str, Disposition],
) -> Activity:
    candidate = review.candidate
    if not isinstance(candidate, Activity) or candidate.field_id != "explicit_activity":
        raise ValueError("offline Activity review accepts only Activity candidates")
    if candidate.record_id != decision.target_id or records.get(candidate.record_id) != candidate:
        raise ValueError("offline Activity review target mismatch")
    disposition = dispositions.get(candidate.record_id)
    if (
        disposition is None
        or disposition.status != DispositionStatus.BLOCKED
        or ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED
        not in disposition.reason_codes
        or ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED not in review.reason_codes
    ):
        raise ValueError("offline Activity review prior blocker mismatch")
    if candidate.actor_basis != SubjectBasis.EXPLICIT_ECONOMIC_RELATIONSHIP:
        raise ValueError("offline Activity review prior actor basis mismatch")
    if (
        candidate.activity_actor != decision.activity_actor
        or candidate.source_actor != decision.source_actor
        or candidate.activity_actor != candidate.source_actor
    ):
        raise ValueError("offline Activity review actor mismatch")
    if candidate.subject_scope != SubjectScope.UNCLEAR:
        raise ValueError("offline Activity review cannot promote subject scope")
    evidence = {item.evidence_id: item for item in candidate.evidence}
    if set(evidence) != {decision.evidence_id}:
        raise ValueError("offline Activity review Evidence mismatch")
    bounded_quote = evidence[decision.evidence_id].anchor.bounded_quote
    if _normalized_text(decision.source_text) not in _normalized_text(bounded_quote):
        raise ValueError("offline Activity review source text mismatch")
    return candidate


def _normalized_text(value: str) -> str:
    return re.sub(r"\s+", "", value)


def _sha256_file(path: Path) -> str:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ValueError("offline Activity review source file is missing") from exc
    return hashlib.sha256(payload).hexdigest()


def _contract_benchmark(
    task_results: list[CompanyProfileTaskResult],
    scope_results: list[Stage5ScopeResult] | None = None,
) -> Stage5BenchmarkResult:
    incomplete = [item.request_id for item in task_results if not item.task_complete]
    production_boundary_ok = all(
        item.production_authorization == "not_authorized" for item in task_results
    )
    unclear_subject_ids = [
        record.record_id
        for task_result in task_results
        for record in task_result.records
        if record.subject_scope == SubjectScope.UNCLEAR
        and any(
            disposition.target_id == record.record_id
            and disposition.status == DispositionStatus.ACCEPTED_FOR_REVIEW
            for disposition in task_result.dispositions
        )
    ]
    chapter_dimensions = _core_chapter_dimensions(scope_results or [])
    illegal_promotion = accepted_has_illegal_group_promotion(task_results)
    dimensions = (
        Stage5BenchmarkDimension(
            name="task_completion",
            passed=not incomplete,
            blocker_codes=("incomplete_request_scope",) if incomplete else (),
            details={"incomplete_request_ids": incomplete},
        ),
        Stage5BenchmarkDimension(
            name="subject_resolution",
            passed=not illegal_promotion,
            blocker_codes=("subject_scope_unsupported_promotion",)
            if illegal_promotion
            else (),
            details={
                "unclear_subject_record_ids": unclear_subject_ids,
                "unclear_is_non_blocking": True,
            },
        ),
        *chapter_dimensions,
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
    # Final research-slice usability is a post-run decision because the real-report
    # negative cases are evaluated only after the immutable bundle is committed.
    return Stage5OverallStatus.HOLD


def _core_chapter_dimensions(
    scope_results: list[Stage5ScopeResult],
) -> tuple[Stage5BenchmarkDimension, ...]:
    dimensions: list[Stage5BenchmarkDimension] = []
    required_by_chapter: dict[ChapterTask, set[str]] = {}
    for scope in scope_results:
        for field_id in scope.prepared_scope.field_ids:
            contract = _FIELD_CONTRACT.get(field_id)
            if contract and contract[1] == RequirementLevel.REQUIRED:
                required_by_chapter.setdefault(
                    scope.prepared_scope.chapter_task, set()
                ).add(field_id)
    for chapter in CORE_CHAPTERS:
        chapter_enum = ChapterTask(chapter)
        scopes = [
            item
            for item in scope_results
            if item.prepared_scope.chapter_task == chapter_enum
        ]
        required = required_by_chapter.get(chapter_enum, set())
        seen: set[str] = set()
        failures: list[str] = []
        for scope in scopes:
            if not scope.task_result.task_complete:
                failures.append(scope.scope_id)
            for coverage in scope.task_result.coverage:
                if coverage.field_id in required and coverage.status in {
                    CoverageStatus.OBSERVED,
                    CoverageStatus.NOT_DISCLOSED,
                    CoverageStatus.NOT_APPLICABLE,
                }:
                    seen.add(coverage.field_id)
                elif coverage.field_id in required:
                    failures.append(
                        f"{scope.scope_id}:{coverage.field_id}:{coverage.status.value}"
                    )
        missing = sorted(required - seen)
        if not scopes:
            failures.append("missing_scope")
        if missing:
            failures.extend(f"missing:{item}" for item in missing)
        dimensions.append(
            Stage5BenchmarkDimension(
                name=f"core_chapter:{chapter}",
                passed=not failures,
                blocker_codes=("required_core_chapter_incomplete",) if failures else (),
                details={"failures": failures, "required_fields": sorted(required)},
            )
        )
    return tuple(dimensions)


def _derive_report_status(
    *,
    task_results: list[CompanyProfileTaskResult],
    scope_results: list[Stage5ScopeResult],
    benchmark: Stage5BenchmarkResult,
) -> Stage5ReportStatus:
    if any(
        any(
            code
            in {
                ContractErrorCode.DEADLINE_EXCEEDED,
                ContractErrorCode.PROVIDER_UNAVAILABLE,
            }
            for item in result.human_review_items
            for code in item.reason_codes
        )
        for result in task_results
    ):
        return Stage5ReportStatus.FAILED
    if benchmark.decision != "pass":
        return Stage5ReportStatus.HOLD
    accepted = [
        record for result in task_results for record in result.accepted_records()
    ]
    if any(record.subject_scope == SubjectScope.UNCLEAR for record in accepted):
        return Stage5ReportStatus.USABLE_WITH_CAVEATS
    return Stage5ReportStatus.USABLE


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
