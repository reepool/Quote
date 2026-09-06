"""Versioned research-only adjudication records for stage-five semantic holds."""

from __future__ import annotations

import hashlib
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .stage5 import APPROVED_STAGE5_SAMPLES

STAGE55_BASELINE_RUN_ID = "stage5-final-four-luna-20260905-f"
STAGE55_ADJUDICATION_SCHEMA = "company_profile_stage55_adjudication_ledger.v1"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Stage55AdjudicationDecision(str, Enum):
    IMPLEMENTATION_CORRECTION = "implementation_correction"
    CONTRACT_ACCEPTED = "contract_accepted"
    RUNTIME_REJECTED = "runtime_rejected"
    DEFERRED_EVIDENCE = "deferred_evidence"


class Stage55AdjudicationItem(_StrictModel):
    adjudication_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    runtime_target_ids: tuple[str, ...] = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    blocker_codes: tuple[str, ...] = Field(min_length=1)
    decision: Stage55AdjudicationDecision
    contract_refs: tuple[str, ...] = Field(min_length=1)
    rationale: str = Field(min_length=1, max_length=4000)
    reviewer: str = Field(min_length=1)
    reviewed_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = "not_authorized"

    @model_validator(mode="after")
    def _sample_is_approved(self) -> Stage55AdjudicationItem:
        if self.sample_id not in APPROVED_STAGE5_SAMPLES:
            raise ValueError("adjudication sample is outside the approved four reports")
        return self


class Stage55TargetedRunResult(_StrictModel):
    run_id: str = Field(min_length=1)
    manifest_path: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    task_complete: bool
    accepted_record_count: int = Field(ge=0)
    coverage_statuses: tuple[str, ...]
    runtime_target_ids: tuple[str, ...] = Field(min_length=1)
    provider_calls: int = Field(ge=0)
    decision: Literal["scope_pass", "hold"]
    rationale: str = Field(min_length=1, max_length=4000)
    production_authorization: Literal["not_authorized"] = "not_authorized"

    @model_validator(mode="after")
    def _scope_pass_requires_complete_task(self) -> Stage55TargetedRunResult:
        if self.sample_id not in APPROVED_STAGE5_SAMPLES:
            raise ValueError("targeted run sample is outside the approved four reports")
        if self.decision == "scope_pass" and not self.task_complete:
            raise ValueError("targeted scope cannot pass an incomplete semantic task")
        return self


class Stage55AdjudicationLedger(_StrictModel):
    schema_version: Literal["company_profile_stage55_adjudication_ledger.v1"] = (
        STAGE55_ADJUDICATION_SCHEMA
    )
    baseline_run_id: Literal["stage5-final-four-luna-20260905-f"]
    baseline_manifest_path: str = Field(min_length=1)
    baseline_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_overall_status: Literal["hold"] = "hold"
    incomplete_scopes: dict[str, tuple[str, ...]]
    unclear_subject_record_counts: dict[str, int]
    items: tuple[Stage55AdjudicationItem, ...] = Field(min_length=1)
    targeted_runs: tuple[Stage55TargetedRunResult, ...] = Field(min_length=1)
    rerun_policy: Literal["new_run_id_only"] = "new_run_id_only"
    status: Literal["in_review", "accepted", "held"] = "in_review"
    production_authorization: Literal["not_authorized"] = "not_authorized"

    @model_validator(mode="after")
    def _baseline_inventory_is_closed(self) -> Stage55AdjudicationLedger:
        expected = set(APPROVED_STAGE5_SAMPLES)
        if set(self.incomplete_scopes) != expected:
            raise ValueError(
                "adjudication incomplete-scope inventory must cover four reports"
            )
        if set(self.unclear_subject_record_counts) != expected:
            raise ValueError("adjudication subject inventory must cover four reports")
        if any(value < 0 for value in self.unclear_subject_record_counts.values()):
            raise ValueError("unclear subject counts cannot be negative")
        targeted_ids = [item.run_id for item in self.targeted_runs]
        if len(targeted_ids) != len(set(targeted_ids)):
            raise ValueError("targeted run IDs must be unique")
        return self


def load_stage55_adjudication_ledger(
    path: str | Path,
    *,
    repository_root: str | Path,
) -> Stage55AdjudicationLedger:
    """Load the ledger and prove that it still points at the immutable run-f manifest."""

    ledger_path = Path(path)
    try:
        ledger = Stage55AdjudicationLedger.model_validate_json(
            ledger_path.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ValueError("stage-five adjudication ledger is unreadable") from exc
    manifest_path = Path(repository_root) / ledger.baseline_manifest_path
    if not manifest_path.is_file():
        raise ValueError("stage-five adjudication baseline manifest is missing")
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if digest != ledger.baseline_manifest_sha256:
        raise ValueError("stage-five adjudication baseline manifest hash mismatch")
    return ledger
