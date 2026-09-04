"""Versioned extract, repair, verify, and workflow contracts for stage four."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Annotated, Any, Literal, Protocol, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import (
    PRODUCTION_AUTHORIZATION,
    ActivityAction,
    ChapterTask,
    CoverageResult,
    CoverageStatus,
    Evidence,
    MetricType,
    ObjectType,
    ReportIdentity,
    RequirementLevel,
    SemanticRecord,
    SourceNativeValue,
)

CONTRACT_SCHEMA_VERSION = "company_profile_semantic_contract.v1"
PACKAGE_MANIFEST_SCHEMA_VERSION = "company_profile_package_manifest.v1"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class DispositionStatus(_StringEnum):
    ACCEPTED_FOR_REVIEW = "accepted_for_review"
    BLOCKED = "blocked"
    UNRESOLVED = "unresolved"


class VerifyStatus(_StringEnum):
    PASS = "pass"
    BLOCK = "block"
    UNCLEAR = "unclear"
    NOT_APPLICABLE = "n-a"


class ContractErrorCode(_StringEnum):
    CONTEXT_INCOMPLETE = "context_incomplete"
    TABLE_CONTEXT_INCOMPLETE = "table_context_incomplete"
    SOURCE_UNREADABLE = "source_unreadable"
    UNIT_AMBIGUOUS = "unit_ambiguous"
    UNKNOWN_CHAPTER_TASK = "unknown_chapter_task"
    REQUEST_IDENTITY_MISMATCH = "request_identity_mismatch"
    FIELD_NOT_IN_ACTIVE_CHECKLIST = "field_not_in_active_checklist"
    OBJECT_NOT_ALLOWED = "object_not_allowed"
    METRIC_NOT_ALLOWED = "metric_not_allowed"
    ACTION_NOT_ALLOWED = "action_not_allowed"
    EVIDENCE_FIELD_MISMATCH = "evidence_field_mismatch"
    SOURCE_VALUE_MUTATION = "source_value_mutation"
    CANDIDATE_SCHEMA_INVALID = "candidate_schema_invalid"
    CAPACITY_KIND_AMBIGUOUS = "capacity_kind_ambiguous"
    COMPARISON_BASIS_MISSING = "comparison_basis_missing"
    ACTIVITY_ACTOR_UNSUPPORTED = "activity_actor_unsupported"
    SUBJECT_UNSUPPORTED = "subject_unsupported"
    PROHIBITED_INFERENCE = "prohibited_inference"
    DUPLICATE_OCCURRENCE = "duplicate_occurrence"
    OCCURRENCE_SEMANTIC_CONFLICT = "occurrence_semantic_conflict"
    REPAIR_FIELD_OUT_OF_SCOPE = "repair_field_out_of_scope"
    REPAIR_IDENTITY_MUTATION = "repair_identity_mutation"
    VERIFY_TARGET_MISSING = "verify_target_missing"
    REQUIRED_COVERAGE_MISSING = "required_coverage_missing"
    COVERAGE_STATUS_NOT_ALLOWED = "coverage_status_not_allowed"
    COVERAGE_REASON_UNSUPPORTED = "coverage_reason_unsupported"
    PROVIDER_UNAVAILABLE = "provider_unavailable"


class SemanticProviderError(RuntimeError):
    """Safe typed failure raised by a SemanticProvider boundary adapter."""

    def __init__(self, code: ContractErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class ChecklistItem(_StrictModel):
    field_id: str = Field(min_length=1)
    object_type: ObjectType
    chapter_task: ChapterTask
    requirement_level: RequirementLevel
    active: bool = True
    allowed_coverage_statuses: tuple[CoverageStatus, ...] = Field(min_length=1)
    allowed_metric_types: tuple[MetricType, ...] = ()
    allowed_actions: tuple[ActivityAction, ...] = ()


class PackageManifest(_StrictModel):
    schema_version: Literal["company_profile_package_manifest.v1"] = (
        PACKAGE_MANIFEST_SCHEMA_VERSION
    )
    package_name: str = Field(min_length=1)
    package_version: str = Field(min_length=1)
    report: ReportIdentity
    checklist: tuple[ChecklistItem, ...] = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _checklist_is_unique(self) -> PackageManifest:
        identities = [(item.chapter_task, item.field_id) for item in self.checklist]
        if len(identities) != len(set(identities)):
            raise ValueError("package checklist field identity must be unique")
        return self

    def active_items(self, chapter_task: ChapterTask) -> tuple[ChecklistItem, ...]:
        return tuple(
            item
            for item in self.checklist
            if item.active and item.chapter_task == chapter_task
        )


class PreparedEvidence(_StrictModel):
    evidence: Evidence
    field_id: str | None = None
    source_native: SourceNativeValue | None = None
    context_complete: bool = True
    headers_complete: bool = True
    unit_context_complete: bool = True
    footnotes_complete: bool = True
    continuation_complete: bool = True
    source_readable: bool = True


class SemanticTaskRequest(_StrictModel):
    schema_version: Literal["company_profile_extract_request.v1"] = (
        "company_profile_extract_request.v1"
    )
    request_id: str = Field(min_length=1)
    report: ReportIdentity
    package_manifest: PackageManifest
    chapter_task: ChapterTask
    evidence_bundle: tuple[PreparedEvidence, ...]
    allowed_object_types: tuple[ObjectType, ...] = Field(min_length=1)
    allowed_metric_types: tuple[MetricType, ...] = ()
    allowed_actions: tuple[ActivityAction, ...] = ()
    prohibited_inferences: tuple[str, ...] = ()
    deterministic_candidates: tuple[SemanticRecord, ...] = ()
    provided_coverage: tuple[CoverageResult, ...] = ()
    unresolved_field_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _request_identity_and_scope(self) -> SemanticTaskRequest:
        if self.package_manifest.report != self.report:
            raise ValueError("package manifest report must match request report")
        active = {
            item.field_id: item
            for item in self.package_manifest.active_items(self.chapter_task)
        }
        if not active:
            raise ValueError(
                "request requires an active checklist for its chapter task"
            )
        if any(item.evidence.report != self.report for item in self.evidence_bundle):
            raise ValueError("prepared evidence must belong to the request report")
        if any(
            item.field_id is not None and item.field_id not in active
            for item in self.evidence_bundle
        ):
            raise ValueError("prepared evidence field is outside active checklist")
        evidence_ids = {item.evidence.evidence_id for item in self.evidence_bundle}
        record_ids = [item.record_id for item in self.deterministic_candidates]
        if len(record_ids) != len(set(record_ids)):
            raise ValueError("deterministic candidate record_id must be unique")
        for record in self.deterministic_candidates:
            if record.report != self.report or record.chapter_task != self.chapter_task:
                raise ValueError("deterministic candidate identity must match request")
            if record.field_id not in active:
                raise ValueError(
                    "deterministic candidate field is outside active checklist"
                )
            if ObjectType(record.object_type) not in self.allowed_object_types:
                raise ValueError("deterministic candidate object type is not allowed")
            if any(item.evidence_id not in evidence_ids for item in record.evidence):
                raise ValueError(
                    "candidate evidence must be contained in request bundle"
                )
        coverage_identities = [
            (item.chapter_task, item.field_id) for item in self.provided_coverage
        ]
        if len(coverage_identities) != len(set(coverage_identities)):
            raise ValueError("provided coverage identity must be unique")
        for coverage in self.provided_coverage:
            if (
                coverage.chapter_task != self.chapter_task
                or coverage.field_id not in active
            ):
                raise ValueError("coverage is outside the active request checklist")
            checklist = active[coverage.field_id]
            if coverage.requirement_level != checklist.requirement_level:
                raise ValueError("coverage requirement level must match checklist")
            if coverage.status not in checklist.allowed_coverage_statuses:
                raise ValueError("coverage status is not allowed by checklist")
            if any(item.evidence_id not in evidence_ids for item in coverage.evidence):
                raise ValueError("coverage evidence must be contained in request bundle")
        if len(self.unresolved_field_ids) != len(set(self.unresolved_field_ids)):
            raise ValueError("unresolved field identity must be unique")
        if any(field_id not in active for field_id in self.unresolved_field_ids):
            raise ValueError("unresolved field is outside active checklist")
        return self


class CandidateResponseItem(_StrictModel):
    item_type: Literal["candidate"] = "candidate"
    candidate: SemanticRecord


class CoverageResponseItem(_StrictModel):
    item_type: Literal["coverage"] = "coverage"
    coverage: CoverageResult


ExtractResponseItem: TypeAlias = Annotated[
    CandidateResponseItem | CoverageResponseItem,
    Field(discriminator="item_type"),
]


class ExtractResponse(_StrictModel):
    schema_version: Literal["company_profile_extract_response.v1"] = (
        "company_profile_extract_response.v1"
    )
    request_id: str = Field(min_length=1)
    items: tuple[ExtractResponseItem, ...] = ()

    @model_validator(mode="after")
    def _response_targets_are_unique(self) -> ExtractResponse:
        candidate_ids = [item.record_id for item in self.candidates()]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("extract candidate record_id must be unique")
        coverage_ids = [
            (item.chapter_task, item.field_id) for item in self.coverage_results()
        ]
        if len(coverage_ids) != len(set(coverage_ids)):
            raise ValueError("extract coverage identity must be unique")
        return self

    def candidates(self) -> tuple[SemanticRecord, ...]:
        return tuple(
            item.candidate
            for item in self.items
            if isinstance(item, CandidateResponseItem)
        )

    def coverage_results(self) -> tuple[CoverageResult, ...]:
        return tuple(
            item.coverage
            for item in self.items
            if isinstance(item, CoverageResponseItem)
        )


class RepairRequest(_StrictModel):
    schema_version: Literal["company_profile_repair_request.v1"] = (
        "company_profile_repair_request.v1"
    )
    request_id: str = Field(min_length=1)
    original_request_id: str = Field(min_length=1)
    original_candidate: SemanticRecord
    error_code: ContractErrorCode
    writable_fields: tuple[str, ...] = Field(min_length=1)
    evidence_bundle: tuple[PreparedEvidence, ...] = Field(min_length=1)


class RepairResponse(_StrictModel):
    schema_version: Literal["company_profile_repair_response.v1"] = (
        "company_profile_repair_response.v1"
    )
    request_id: str = Field(min_length=1)
    candidate: SemanticRecord
    changed_fields: tuple[str, ...] = Field(min_length=1)


class VerifyRequest(_StrictModel):
    schema_version: Literal["company_profile_verify_request.v1"] = (
        "company_profile_verify_request.v1"
    )
    request_id: str = Field(min_length=1)
    original_request_id: str = Field(min_length=1)
    report: ReportIdentity
    evidence_bundle: tuple[PreparedEvidence, ...] = Field(min_length=1)
    candidates: tuple[SemanticRecord, ...]
    coverage: tuple[CoverageResult, ...]


class VerifyCheck(_StrictModel):
    target_type: Literal["candidate", "coverage", "request"]
    target_id: str = Field(min_length=1)
    status: VerifyStatus
    reason_codes: tuple[ContractErrorCode, ...] = ()
    explanation: str | None = None


class VerifyResponse(_StrictModel):
    schema_version: Literal["company_profile_verify_response.v1"] = (
        "company_profile_verify_response.v1"
    )
    request_id: str = Field(min_length=1)
    checks: tuple[VerifyCheck, ...]

    @model_validator(mode="after")
    def _check_targets_are_unique(self) -> VerifyResponse:
        targets = [(item.target_type, item.target_id) for item in self.checks]
        if len(targets) != len(set(targets)):
            raise ValueError("verify check target must be unique")
        return self


class Disposition(_StrictModel):
    target_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    status: DispositionStatus
    reason_codes: tuple[ContractErrorCode, ...] = ()


class HumanReviewItem(_StrictModel):
    review_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    candidate: SemanticRecord | None = None
    evidence: tuple[Evidence, ...]
    reason_codes: tuple[ContractErrorCode, ...] = Field(min_length=1)
    conflicting_interpretations: tuple[str, ...] = ()
    allowed_actions: tuple[str, ...] = ("accept", "reject", "keep_unresolved")


class CompanyProfileTaskResult(_StrictModel):
    schema_version: Literal["company_profile_task_result.v1"] = (
        "company_profile_task_result.v1"
    )
    request_id: str = Field(min_length=1)
    records: tuple[SemanticRecord, ...]
    dispositions: tuple[Disposition, ...]
    coverage: tuple[CoverageResult, ...]
    human_review_items: tuple[HumanReviewItem, ...]
    task_complete: bool
    provider_calls: tuple[str, ...] = ()
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    def accepted_records(self) -> tuple[SemanticRecord, ...]:
        accepted_ids = {
            item.target_id
            for item in self.dispositions
            if item.status == DispositionStatus.ACCEPTED_FOR_REVIEW
        }
        return tuple(item for item in self.records if item.record_id in accepted_ids)


class SemanticProvider(Protocol):
    def extract(
        self, request: SemanticTaskRequest
    ) -> ExtractResponse | Mapping[str, Any]: ...

    def repair(self, request: RepairRequest) -> RepairResponse | Mapping[str, Any]: ...

    def verify(self, request: VerifyRequest) -> VerifyResponse | Mapping[str, Any]: ...


def contract_schema_manifest() -> dict[str, Any]:
    """Generate JSON Schema from the Pydantic runtime models."""

    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "extract_request": SemanticTaskRequest.model_json_schema(),
        "extract_response": ExtractResponse.model_json_schema(),
        "repair_request": RepairRequest.model_json_schema(),
        "repair_response": RepairResponse.model_json_schema(),
        "verify_request": VerifyRequest.model_json_schema(),
        "verify_response": VerifyResponse.model_json_schema(),
    }


def contract_example_manifest() -> dict[str, Any]:
    """Return bounded examples without duplicating the runtime schema."""

    return {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "positive": {
            "extract_response": {
                "schema_version": "company_profile_extract_response.v1",
                "request_id": "request-1",
                "items": [],
            },
            "repair_scope": {
                "error_code": ContractErrorCode.CAPACITY_KIND_AMBIGUOUS.value,
                "writable_fields": ["/capacity_kind"],
                "maximum_attempts": 1,
            },
            "verify_check": {
                "target_type": "candidate",
                "target_id": "candidate-1",
                "status": VerifyStatus.PASS.value,
                "reason_codes": [],
            },
        },
        "negative": {
            "json_external_prose": {
                "prohibited": True,
                "error_code": ContractErrorCode.CANDIDATE_SCHEMA_INVALID.value,
            },
            "source_value_rewrite": {
                "source": {"value": "23.84", "unit": "%"},
                "prohibited_candidate": {"value": "0.2384", "unit": None},
                "error_code": ContractErrorCode.SOURCE_VALUE_MUTATION.value,
            },
            "commodity_direction": {
                "prohibited": True,
                "error_code": ContractErrorCode.PROHIBITED_INFERENCE.value,
            },
        },
    }
