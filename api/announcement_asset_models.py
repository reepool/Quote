"""Pydantic contracts for shared annual-report asset resources."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

AssetAvailabilityValue = Literal[
    "local_valid",
    "metadata_only",
    "missing",
    "ambiguous",
    "corrupt",
    "superseded",
    "blocked",
]
IntegrityStatusValue = Literal[
    "unchecked",
    "valid",
    "missing",
    "unreadable",
    "not_pdf",
    "size_mismatch",
    "hash_mismatch",
    "quarantined",
]
AcquisitionStatusValue = Literal[
    "metadata_only",
    "success",
    "failed",
    "adopted",
    "candidate_verified",
    "candidate_rejected",
    "candidate_verification_failed",
]
EffectiveDecisionStateValue = Literal[
    "current",
    "provisional",
    "ambiguous",
    "blocked",
    "withdrawn",
]
AnnualReportRecordStateValue = Literal[
    "current",
    "provisional",
    "ambiguous",
    "blocked",
    "withdrawn",
    "superseded",
    "historical",
]
ExactContentStateValue = Literal[
    "local_valid",
    "retained_internal_only",
    "local_content_unavailable",
]
EnsureDispositionValue = Literal[
    "local_hit",
    "local_miss",
    "operation_created",
    "operation_reused",
]
AssetRequestStatusValue = Literal["active", "cancelled", "expired"]
OperationStatusValue = Literal[
    "queued",
    "running",
    "completed",
    "missing",
    "failed",
    "blocked",
    "cancelled",
]
OperationStageValue = Literal[
    "not_applicable",
    "discovering",
    "reconciling",
    "downloading",
    "validating",
    "activating",
]
BatchOutcomeValue = Literal["success", "partial", "blocked", "failed"]
ResultOriginValue = Literal["adopted", "downloaded", "repaired"]


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AnnualReportEnsureRequestModel(_StrictContract):
    fiscal_year: int | None = Field(None, ge=1990, le=2200)
    source: str | None = None
    source_announcement_id: str | None = None
    filing_id: str | None = None
    attachment_id: str | None = None
    expected_content_hash: str | None = None
    observation_version: str | None = None
    allow_network: bool = False
    integrity_level: Literal["metadata", "size", "hash"] = "hash"
    wait_seconds: float | None = Field(None, ge=0, le=30)
    consumer: None = None
    knowledge_cutoff: str | None = None

    @model_validator(mode="after")
    def validate_selector(self):
        source_announcement_id = str(self.source_announcement_id or "").strip()
        filing_id = str(self.filing_id or "").strip()
        if source_announcement_id and filing_id and source_announcement_id != filing_id:
            raise ValueError(
                "filing_id conflicts with canonical source_announcement_id"
            )
        canonical_filing_id = source_announcement_id or filing_id
        self.source_announcement_id = canonical_filing_id or None
        self.filing_id = canonical_filing_id or None
        period = self.fiscal_year is not None
        filing = bool(self.source and canonical_filing_id)
        if period == filing:
            raise ValueError("provide exactly one fiscal-year or exact-filing selector")
        if bool(self.source) != bool(canonical_filing_id):
            raise ValueError(
                "source and source_announcement_id/filing_id are all-or-none"
            )
        has_pin = bool(
            self.attachment_id
            or self.expected_content_hash
            or self.observation_version
        )
        if has_pin and not filing:
            raise ValueError("attachment observation pins require exact-filing identity")
        return self


class AnnualReportAssetResponse(_StrictContract):
    asset_record_id: str | None = None
    asset_id: str | None = None
    instrument_id: str
    fiscal_year: int
    report_period: str
    source: str
    source_announcement_id: str
    filing_id: str
    attachment_id: str
    observation_version: str | None = None
    version_available_at: str | None = None
    published_at: str | None = None
    document_family: Literal["annual_report"] = "annual_report"
    variant: Literal["original", "correction"]
    is_full_report: Literal[True] = True
    is_correction: bool
    classification_vocabulary_version: Literal[
        "official_document_classification.v1"
    ] = "official_document_classification.v1"
    content_hash: str | None = None
    content_length: int | None = None
    content_url: str | None = None
    integrity: IntegrityStatusValue | None = None
    asset_availability: AssetAvailabilityValue
    availability: AssetAvailabilityValue
    acquisition_status: AcquisitionStatusValue | None = None
    effective_state: AnnualReportRecordStateValue | None = None
    effective_decision_state: EffectiveDecisionStateValue | None = None
    exact_content_state: ExactContentStateValue
    predecessor_asset_id: str | None = None
    pending_candidate_id: str | None = None
    activated_at: str | None = None
    last_checked_at: str | None = None
    decision_reasons: list[str] = Field(default_factory=list)
    canonical_source_filing: dict[str, Any]
    equivalent_source_filings: list[dict[str, Any]] = Field(default_factory=list)
    canonical_projection_policy_version: str | None = None
    evidence_set_hash: str | None = None


class AnnualReportAssetListResponse(_StrictContract):
    items: list[AnnualReportAssetResponse] = Field(default_factory=list)
    returned: int
    limit: int
    offset: int


class AnnualReportEffectiveResponse(_StrictContract):
    asset_availability: AssetAvailabilityValue
    availability: AssetAvailabilityValue
    asset: AnnualReportAssetResponse | None = None


class AnnualReportEnsureResponse(_StrictContract):
    disposition: EnsureDispositionValue
    asset_availability: AssetAvailabilityValue
    availability: AssetAvailabilityValue
    asset: AnnualReportAssetResponse | None = None
    asset_request_id: str | None = None
    request: AnnualReportRequestResponse | None = None
    reason_code: str | None = None


class AnnualReportRequestResponse(_StrictContract):
    asset_request_id: str
    asset_request_status: AssetRequestStatusValue
    status: AssetRequestStatusValue
    consumer: str | None = None
    created_at: str
    updated_at: str
    cancelled_at: str | None = None
    operation_status: OperationStatusValue | None = None
    operation_stage: OperationStageValue | None = None
    batch_outcome: BatchOutcomeValue | None = None
    attempt: int | None = None
    next_retry_at: str | None = None
    progress: dict[str, Any] = Field(default_factory=dict)
    result_asset_id: str | None = None
    result_origin: ResultOriginValue | None = None
    reason_code: str | None = None
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    expires_at: str | None = None
    expired_at: str | None = None
    tombstone_until: str | None = None
    retention_policy_version: str | None = None


class AnnualReportReadinessResponse(_StrictContract):
    status: str
    ready_for_reads: bool
    ready_for_daily: bool
    ready_for_deletion: bool
    generated_at: str | None = None
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    operator_diagnostics: dict[str, Any] | None = None


class AnnualReportErrorEnvelope(_StrictContract):
    schema_version: str = "annual_report_error.v1"
    error_code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)
