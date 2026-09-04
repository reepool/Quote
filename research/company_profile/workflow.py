"""Single in-memory semantic workflow owner for stage four."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from .contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
    ExtractResponse,
    HumanReviewItem,
    RepairRequest,
    RepairResponse,
    SemanticProvider,
    SemanticProviderError,
    SemanticTaskRequest,
    VerifyCheck,
    VerifyRequest,
    VerifyResponse,
    VerifyStatus,
)
from .models import (
    Activity,
    CapacityKind,
    ChapterTask,
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    Measurement,
    MetricType,
    ObjectType,
    RequirementLevel,
    SemanticRecord,
)

_ResponseT = TypeVar("_ResponseT", bound=BaseModel)
_NUMERIC_TASKS = {
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
    ChapterTask.EXTRACT_OPERATING_QUANTITIES,
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
}


@dataclass
class FakeSemanticProvider:
    """Scriptable, no-network provider used by stage-four fixtures and tests."""

    extract_output: ExtractResponse | Mapping[str, Any] | None = None
    repair_outputs: list[RepairResponse | Mapping[str, Any]] = field(
        default_factory=list
    )
    verify_outputs: list[VerifyResponse | Mapping[str, Any]] = field(
        default_factory=list
    )
    calls: list[str] = field(default_factory=list)

    def extract(
        self, request: SemanticTaskRequest
    ) -> ExtractResponse | Mapping[str, Any]:
        self.calls.append("extract")
        if self.extract_output is None:
            return ExtractResponse(request_id=request.request_id)
        return self.extract_output

    def repair(self, request: RepairRequest) -> RepairResponse | Mapping[str, Any]:
        self.calls.append("repair")
        if not self.repair_outputs:
            raise RuntimeError("fake repair output was not configured")
        return self.repair_outputs.pop(0)

    def verify(self, request: VerifyRequest) -> VerifyResponse | Mapping[str, Any]:
        self.calls.append("verify")
        if self.verify_outputs:
            return self.verify_outputs.pop(0)
        return VerifyResponse(
            request_id=request.request_id,
            checks=tuple(
                VerifyCheck(
                    target_type="candidate",
                    target_id=record.record_id,
                    status=VerifyStatus.PASS,
                )
                for record in request.candidates
            )
            + tuple(
                VerifyCheck(
                    target_type="coverage",
                    target_id=_coverage_target_id(coverage),
                    status=VerifyStatus.PASS,
                )
                for coverage in request.coverage
            ),
        )


class CompanyProfileSemanticService:
    """Authoritative stage-four in-memory workflow.

    The service accepts explicit prepared inputs and has no repository, network client,
    scheduler, or production writer dependency.
    """

    def run_task(
        self,
        request: SemanticTaskRequest,
        *,
        provider: SemanticProvider | None = None,
    ) -> CompanyProfileTaskResult:
        provider_calls: list[str] = []
        preparation_error = _preparation_error(request)
        if preparation_error is not None:
            return _preparation_failure(request, preparation_error)

        records = list(request.deterministic_candidates)
        explicit_coverage = list(request.provided_coverage)
        request_review_items: list[HumanReviewItem] = []

        if request.unresolved_field_ids:
            if provider is None:
                request_review_items.append(
                    HumanReviewItem(
                        review_id=f"{request.request_id}:provider-unavailable",
                        field_id=request.unresolved_field_ids[0],
                        evidence=tuple(
                            item.evidence for item in request.evidence_bundle
                        ),
                        reason_codes=(ContractErrorCode.PROVIDER_UNAVAILABLE,),
                        conflicting_interpretations=("semantic fallback required",),
                    )
                )
            else:
                provider_calls.append("extract")
                try:
                    response = _coerce_response(
                        ExtractResponse,
                        provider.extract(request),
                    )
                    if response.request_id != request.request_id:
                        raise ValueError("extract response request_id mismatch")
                    response_candidates = response.candidates()
                    response_coverage = response.coverage_results()
                    requested_fields = set(request.unresolved_field_ids)
                    returned_fields = {
                        item.field_id
                        for item in (*response_candidates, *response_coverage)
                    }
                    if not returned_fields.issubset(requested_fields):
                        raise ValueError(
                            "extract response returned an unrequested field"
                        )
                    existing_record_ids = {item.record_id for item in records}
                    if existing_record_ids.intersection(
                        item.record_id for item in response_candidates
                    ):
                        raise ValueError(
                            "extract response reused a deterministic candidate record_id"
                        )
                    existing_coverage_ids = {
                        (item.chapter_task, item.field_id)
                        for item in explicit_coverage
                    }
                    if existing_coverage_ids.intersection(
                        (item.chapter_task, item.field_id)
                        for item in response_coverage
                    ):
                        raise ValueError(
                            "extract response duplicated provided coverage"
                        )
                    records.extend(response_candidates)
                    explicit_coverage.extend(response_coverage)
                except SemanticProviderError as exc:
                    request_review_items.append(
                        HumanReviewItem(
                            review_id=f"{request.request_id}:extract-provider",
                            field_id=request.unresolved_field_ids[0],
                            evidence=tuple(
                                item.evidence for item in request.evidence_bundle
                            ),
                            reason_codes=(exc.code,),
                            conflicting_interpretations=(str(exc),),
                        )
                    )
                except (ValidationError, ValueError, TypeError, RuntimeError) as exc:
                    request_review_items.append(
                        HumanReviewItem(
                            review_id=f"{request.request_id}:extract-contract",
                            field_id=request.unresolved_field_ids[0],
                            evidence=tuple(
                                item.evidence for item in request.evidence_bundle
                            ),
                            reason_codes=(ContractErrorCode.CANDIDATE_SCHEMA_INVALID,),
                            conflicting_interpretations=(str(exc),),
                        )
                    )

        unique_records, duplicate_dispositions, conflict_ids = _reconcile_occurrences(
            records
        )
        dispositions: dict[str, Disposition] = {
            item.target_id: item for item in duplicate_dispositions
        }
        review_items = list(request_review_items)
        locally_valid: list[SemanticRecord] = []
        record_positions = {
            record.record_id: index for index, record in enumerate(unique_records)
        }

        for record in tuple(unique_records):
            if record.record_id in conflict_ids:
                disposition = Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=DispositionStatus.UNRESOLVED,
                    reason_codes=(ContractErrorCode.OCCURRENCE_SEMANTIC_CONFLICT,),
                )
                dispositions[record.record_id] = disposition
                review_items.append(_review_item(record, disposition.reason_codes))
                continue

            issue = _candidate_issue(record, request)
            if (
                issue == ContractErrorCode.CAPACITY_KIND_AMBIGUOUS
                and provider is not None
            ):
                repaired = self._repair_capacity_kind(
                    request,
                    record,
                    provider,
                    provider_calls,
                )
                if repaired is not None:
                    record = repaired
                    unique_records[record_positions[record.record_id]] = repaired
                    issue = _candidate_issue(record, request)
            if issue is not None:
                status = (
                    DispositionStatus.UNRESOLVED
                    if issue == ContractErrorCode.CAPACITY_KIND_AMBIGUOUS
                    else DispositionStatus.BLOCKED
                )
                disposition = Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=status,
                    reason_codes=(issue,),
                )
                dispositions[record.record_id] = disposition
                review_items.append(_review_item(record, disposition.reason_codes))
                continue
            locally_valid.append(record)

        verify_checks: dict[str, VerifyCheck] = {}
        coverage_verify_checks: dict[str, VerifyCheck] = {}
        if locally_valid or explicit_coverage:
            verify_request = VerifyRequest(
                request_id=f"{request.request_id}:verify",
                original_request_id=request.request_id,
                report=request.report,
                evidence_bundle=request.evidence_bundle,
                candidates=tuple(locally_valid),
                coverage=tuple(explicit_coverage),
            )
            if provider is None:
                verify_response = _deterministic_verify(verify_request)
            else:
                provider_calls.append("verify")
                try:
                    verify_response = _coerce_response(
                        VerifyResponse,
                        provider.verify(verify_request),
                    )
                    if verify_response.request_id != verify_request.request_id:
                        raise ValueError("verify response request_id mismatch")
                    candidate_targets = {
                        item.record_id for item in verify_request.candidates
                    }
                    coverage_targets = {
                        _coverage_target_id(item) for item in verify_request.coverage
                    }
                    if any(
                        (
                            check.target_type == "candidate"
                            and check.target_id not in candidate_targets
                        )
                        or (
                            check.target_type == "coverage"
                            and check.target_id not in coverage_targets
                        )
                        for check in verify_response.checks
                    ):
                        raise ValueError("verify response returned an unknown target")
                except SemanticProviderError as exc:
                    verify_response = VerifyResponse(
                        request_id=verify_request.request_id,
                        checks=tuple(
                            VerifyCheck(
                                target_type="candidate",
                                target_id=item.record_id,
                                status=VerifyStatus.BLOCK,
                                reason_codes=(exc.code,),
                            )
                            for item in verify_request.candidates
                        )
                        + tuple(
                            VerifyCheck(
                                target_type="coverage",
                                target_id=_coverage_target_id(item),
                                status=VerifyStatus.BLOCK,
                                reason_codes=(exc.code,),
                            )
                            for item in verify_request.coverage
                        ),
                    )
                except (ValidationError, ValueError, TypeError, RuntimeError):
                    verify_response = VerifyResponse(
                        request_id=verify_request.request_id,
                        checks=(),
                    )
            verify_checks = {
                check.target_id: check
                for check in verify_response.checks
                if check.target_type == "candidate"
            }
            coverage_verify_checks = {
                check.target_id: check
                for check in verify_response.checks
                if check.target_type == "coverage"
            }

        for record in locally_valid:
            check = verify_checks.get(record.record_id)
            if check is None:
                disposition = Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=DispositionStatus.BLOCKED,
                    reason_codes=(ContractErrorCode.VERIFY_TARGET_MISSING,),
                )
            elif check.status == VerifyStatus.PASS:
                disposition = Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=DispositionStatus.ACCEPTED_FOR_REVIEW,
                    reason_codes=check.reason_codes,
                )
            elif check.status == VerifyStatus.UNCLEAR:
                disposition = Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=DispositionStatus.UNRESOLVED,
                    reason_codes=check.reason_codes
                    or (ContractErrorCode.SUBJECT_UNSUPPORTED,),
                )
            else:
                disposition = Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=DispositionStatus.BLOCKED,
                    reason_codes=check.reason_codes
                    or (ContractErrorCode.PROHIBITED_INFERENCE,),
                )
            dispositions[record.record_id] = disposition
            if disposition.status != DispositionStatus.ACCEPTED_FOR_REVIEW:
                review_items.append(_review_item(record, disposition.reason_codes))

        coverage, coverage_complete, coverage_review = _resolve_coverage(
            request,
            explicit_coverage,
            tuple(dispositions.values()),
            coverage_verify_checks,
        )
        review_items.extend(coverage_review)
        all_candidates_accepted = all(
            item.status == DispositionStatus.ACCEPTED_FOR_REVIEW
            for item in dispositions.values()
        )
        task_complete = (
            coverage_complete
            and all_candidates_accepted
            and not request_review_items
            and not conflict_ids
        )
        return CompanyProfileTaskResult(
            request_id=request.request_id,
            records=tuple(unique_records),
            dispositions=tuple(dispositions[key] for key in sorted(dispositions)),
            coverage=coverage,
            human_review_items=tuple(review_items),
            task_complete=task_complete,
            provider_calls=tuple(provider_calls),
        )

    def _repair_capacity_kind(
        self,
        request: SemanticTaskRequest,
        record: SemanticRecord,
        provider: SemanticProvider,
        provider_calls: list[str],
    ) -> SemanticRecord | None:
        repair_request = RepairRequest(
            request_id=f"{request.request_id}:repair:{record.record_id}",
            original_request_id=request.request_id,
            original_candidate=record,
            error_code=ContractErrorCode.CAPACITY_KIND_AMBIGUOUS,
            writable_fields=("/capacity_kind",),
            evidence_bundle=request.evidence_bundle,
        )
        provider_calls.append("repair")
        try:
            response = _coerce_response(
                RepairResponse,
                provider.repair(repair_request),
            )
            if response.request_id != repair_request.request_id:
                return None
            if not set(response.changed_fields).issubset(
                repair_request.writable_fields
            ):
                return None
            if response.candidate.record_id != record.record_id:
                return None
            if response.candidate.occurrence_id() != record.occurrence_id():
                return None
            changed = _changed_json_pointers(record, response.candidate)
            if not changed.issubset(set(repair_request.writable_fields)):
                return None
            return response.candidate
        except (ValidationError, ValueError, TypeError, RuntimeError):
            return None


def _preparation_error(request: SemanticTaskRequest) -> ContractErrorCode | None:
    if not request.evidence_bundle:
        return ContractErrorCode.CONTEXT_INCOMPLETE
    for item in request.evidence_bundle:
        if not item.source_readable:
            return ContractErrorCode.SOURCE_UNREADABLE
        if not (
            item.context_complete
            and item.headers_complete
            and item.footnotes_complete
            and item.continuation_complete
        ):
            return ContractErrorCode.TABLE_CONTEXT_INCOMPLETE
        if request.chapter_task in _NUMERIC_TASKS and not item.unit_context_complete:
            return ContractErrorCode.UNIT_AMBIGUOUS
    return None


def _preparation_failure(
    request: SemanticTaskRequest,
    error: ContractErrorCode,
) -> CompanyProfileTaskResult:
    reason = {
        ContractErrorCode.SOURCE_UNREADABLE: CoverageReasonCode.SOURCE_UNREADABLE,
        ContractErrorCode.UNIT_AMBIGUOUS: CoverageReasonCode.UNIT_AMBIGUOUS,
    }.get(error, CoverageReasonCode.TABLE_CONTEXT_INCOMPLETE)
    coverage = tuple(
        CoverageResult(
            field_id=item.field_id,
            chapter_task=item.chapter_task,
            requirement_level=item.requirement_level,
            status=CoverageStatus.EXTRACTION_FAILED,
            reason_code=reason,
            reason=error.value,
            evidence=tuple(source.evidence for source in request.evidence_bundle),
        )
        for item in request.package_manifest.active_items(request.chapter_task)
    )
    return CompanyProfileTaskResult(
        request_id=request.request_id,
        records=(),
        dispositions=(),
        coverage=coverage,
        human_review_items=(),
        task_complete=False,
        provider_calls=(),
    )


def _candidate_issue(
    record: SemanticRecord,
    request: SemanticTaskRequest,
) -> ContractErrorCode | None:
    if record.report != request.report or record.chapter_task != request.chapter_task:
        return ContractErrorCode.REQUEST_IDENTITY_MISMATCH
    request_evidence_ids = {
        item.evidence.evidence_id for item in request.evidence_bundle
    }
    if any(item.evidence_id not in request_evidence_ids for item in record.evidence):
        return ContractErrorCode.REQUEST_IDENTITY_MISMATCH
    evidence_field_bindings = {
        (item.evidence.evidence_id, item.field_id)
        for item in request.evidence_bundle
        if item.field_id is not None
    }
    if not any(
        (evidence.evidence_id, record.field_id) in evidence_field_bindings
        for evidence in record.evidence
    ):
        return ContractErrorCode.EVIDENCE_FIELD_MISMATCH
    if ObjectType(record.object_type) not in request.allowed_object_types:
        return ContractErrorCode.OBJECT_NOT_ALLOWED
    active = {
        item.field_id: item
        for item in request.package_manifest.active_items(request.chapter_task)
    }
    checklist = active.get(record.field_id)
    if checklist is None:
        return ContractErrorCode.FIELD_NOT_IN_ACTIVE_CHECKLIST
    if isinstance(record, Measurement):
        if record.metric_type not in request.allowed_metric_types:
            return ContractErrorCode.METRIC_NOT_ALLOWED
        if (
            checklist.allowed_metric_types
            and record.metric_type not in checklist.allowed_metric_types
        ):
            return ContractErrorCode.METRIC_NOT_ALLOWED
        if (
            record.metric_type == MetricType.PRODUCTION_CAPACITY
            and record.capacity_kind == CapacityKind.UNCLEAR
        ):
            return ContractErrorCode.CAPACITY_KIND_AMBIGUOUS
    if isinstance(record, Activity):
        if record.action not in request.allowed_actions:
            return ContractErrorCode.ACTION_NOT_ALLOWED
        if checklist.allowed_actions and record.action not in checklist.allowed_actions:
            return ContractErrorCode.ACTION_NOT_ALLOWED
        if record.activity_actor != record.source_actor:
            return ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED
    source_by_identity = {
        (item.evidence.evidence_id, item.field_id): item.source_native
        for item in request.evidence_bundle
        if item.source_native is not None
    }
    for evidence in record.evidence:
        expected = source_by_identity.get((evidence.evidence_id, record.field_id))
        if expected is None:
            expected = source_by_identity.get((evidence.evidence_id, None))
        if expected is not None and expected != record.source_native:
            return ContractErrorCode.SOURCE_VALUE_MUTATION
    return None


def _deterministic_verify(request: VerifyRequest) -> VerifyResponse:
    checks: list[VerifyCheck] = []
    for record in request.candidates:
        if (
            isinstance(record, Activity)
            and record.activity_actor != record.source_actor
        ):
            checks.append(
                VerifyCheck(
                    target_type="candidate",
                    target_id=record.record_id,
                    status=VerifyStatus.BLOCK,
                    reason_codes=(ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED,),
                )
            )
        else:
            checks.append(
                VerifyCheck(
                    target_type="candidate",
                    target_id=record.record_id,
                    status=VerifyStatus.PASS,
                )
            )
    checks.extend(
        VerifyCheck(
            target_type="coverage",
            target_id=_coverage_target_id(coverage),
            status=VerifyStatus.PASS,
        )
        for coverage in request.coverage
    )
    return VerifyResponse(request_id=request.request_id, checks=tuple(checks))


def _resolve_coverage(
    request: SemanticTaskRequest,
    explicit_coverage: list[CoverageResult],
    dispositions: tuple[Disposition, ...],
    verify_checks: dict[str, VerifyCheck],
) -> tuple[tuple[CoverageResult, ...], bool, list[HumanReviewItem]]:
    explicit = {(item.chapter_task, item.field_id): item for item in explicit_coverage}
    accepted_fields = {
        item.field_id
        for item in dispositions
        if item.status == DispositionStatus.ACCEPTED_FOR_REVIEW
    }
    unresolved_fields = {
        item.field_id
        for item in dispositions
        if item.status != DispositionStatus.ACCEPTED_FOR_REVIEW
    }
    results: list[CoverageResult] = []
    reviews: list[HumanReviewItem] = []
    complete = True
    for item in request.package_manifest.active_items(request.chapter_task):
        identity = (item.chapter_task, item.field_id)
        coverage = explicit.get(identity)
        if coverage is not None:
            check = verify_checks.get(_coverage_target_id(coverage))
            if check is None or check.status != VerifyStatus.PASS:
                coverage = CoverageResult(
                    field_id=item.field_id,
                    chapter_task=item.chapter_task,
                    requirement_level=item.requirement_level,
                    status=CoverageStatus.UNCLEAR,
                    reason_code=CoverageReasonCode.CANDIDATE_UNRESOLVED,
                    reason="explicit coverage did not pass independent verification",
                    evidence=coverage.evidence,
                )
        elif item.field_id in unresolved_fields:
            coverage = CoverageResult(
                field_id=item.field_id,
                chapter_task=item.chapter_task,
                requirement_level=item.requirement_level,
                status=CoverageStatus.UNCLEAR,
                reason_code=CoverageReasonCode.CANDIDATE_UNRESOLVED,
                reason="candidate did not pass the stage-four contract",
            )
        elif item.field_id in accepted_fields:
            coverage = CoverageResult(
                field_id=item.field_id,
                chapter_task=item.chapter_task,
                requirement_level=item.requirement_level,
                status=CoverageStatus.OBSERVED,
            )
        elif coverage is None:
            coverage = CoverageResult(
                field_id=item.field_id,
                chapter_task=item.chapter_task,
                requirement_level=item.requirement_level,
                status=CoverageStatus.UNCLEAR,
                reason_code=CoverageReasonCode.REQUIRED_RESULT_MISSING,
                reason="active checklist item has no candidate or legal-empty result",
            )
        if coverage.status not in item.allowed_coverage_statuses:
            coverage = CoverageResult(
                field_id=item.field_id,
                chapter_task=item.chapter_task,
                requirement_level=item.requirement_level,
                status=CoverageStatus.UNCLEAR,
                reason_code=CoverageReasonCode.CANDIDATE_UNRESOLVED,
                reason="coverage status is not allowed by the active checklist",
            )
        results.append(coverage)
        if item.requirement_level != RequirementLevel.OPTIONAL and coverage.status in {
            CoverageStatus.EXTRACTION_FAILED,
            CoverageStatus.UNCLEAR,
        }:
            complete = False
            reviews.append(
                HumanReviewItem(
                    review_id=f"{request.request_id}:coverage:{item.field_id}",
                    field_id=item.field_id,
                    evidence=coverage.evidence,
                    reason_codes=(ContractErrorCode.REQUIRED_COVERAGE_MISSING,),
                    conflicting_interpretations=(
                        coverage.reason or coverage.status.value,
                    ),
                )
            )
    return tuple(results), complete, reviews


def _coverage_target_id(coverage: CoverageResult) -> str:
    return f"{coverage.chapter_task.value}:{coverage.field_id}"


def _reconcile_occurrences(
    records: list[SemanticRecord],
) -> tuple[list[SemanticRecord], list[Disposition], set[str]]:
    unique: list[SemanticRecord] = []
    by_occurrence: dict[str, SemanticRecord] = {}
    duplicate_dispositions: list[Disposition] = []
    conflict_ids: set[str] = set()
    processing_sales_by_source: dict[str, SemanticRecord] = {}
    for record in records:
        if isinstance(record, Measurement) and record.metric_type in {
            MetricType.PROCESSING_VOLUME,
            MetricType.SALES_VOLUME,
        }:
            source_key = _physical_source_key(record)
            existing_source = processing_sales_by_source.get(source_key)
            if existing_source is None:
                processing_sales_by_source[source_key] = record
            elif existing_source.metric_type != record.metric_type:
                conflict_ids.update({existing_source.record_id, record.record_id})
        occurrence = record.occurrence_id()
        existing = by_occurrence.get(occurrence)
        if existing is None:
            by_occurrence[occurrence] = record
            unique.append(record)
            continue
        if (
            existing.semantic_content_fingerprint()
            == record.semantic_content_fingerprint()
        ):
            duplicate_dispositions.append(
                Disposition(
                    target_id=record.record_id,
                    field_id=record.field_id,
                    status=DispositionStatus.ACCEPTED_FOR_REVIEW,
                    reason_codes=(ContractErrorCode.DUPLICATE_OCCURRENCE,),
                )
            )
            continue
        conflict_ids.update({existing.record_id, record.record_id})
        unique.append(record)
    return unique, duplicate_dispositions, conflict_ids


def _physical_source_key(record: SemanticRecord) -> str:
    evidence_material = [
        {
            "page": item.page,
            "anchor": item.anchor.model_dump(mode="json"),
        }
        for item in record.evidence
    ]
    evidence_material.sort(
        key=lambda item: json.dumps(
            item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    )
    material = {
        "instrument_id": record.report.instrument_id,
        "document_version": record.report.document_version,
        "report_period": record.report.report_period,
        "evidence": evidence_material,
    }
    return json.dumps(
        material, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _review_item(
    record: SemanticRecord,
    reason_codes: tuple[ContractErrorCode, ...],
) -> HumanReviewItem:
    return HumanReviewItem(
        review_id=f"review:{record.record_id}",
        field_id=record.field_id,
        candidate=record,
        evidence=record.evidence,
        reason_codes=reason_codes,
        conflicting_interpretations=tuple(code.value for code in reason_codes),
    )


def _changed_json_pointers(
    original: SemanticRecord,
    repaired: SemanticRecord,
) -> set[str]:
    before = original.model_dump(mode="json")
    after = repaired.model_dump(mode="json")
    keys = set(before) | set(after)
    return {f"/{key}" for key in keys if before.get(key) != after.get(key)}


def _coerce_response(
    model: type[_ResponseT],
    value: _ResponseT | Mapping[str, Any],
) -> _ResponseT:
    if isinstance(value, model):
        return model.model_validate_json(value.model_dump_json())
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
    return model.model_validate_json(encoded)
