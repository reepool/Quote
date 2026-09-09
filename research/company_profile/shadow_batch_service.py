"""Report-local execution and immutable persistence for the shadow cohort."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .models import PRODUCTION_AUTHORIZATION, ReportIdentity
from .projection import CompanyProfileResearchView
from .shadow_batch import (
    SHADOW_REPORT_COUNT,
    ShadowCohortReport,
    ShadowSampleManifest,
    _payload_hash,
    _StrictModel,
    _utc_now,
)
from .shadow_evidence import (
    ShadowEvidencePlan,
    ShadowEvidencePreparationAudit,
    ShadowScopeRefinementAudit,
)
from .stage5 import PreparedRequestScope
from .stage5_bundle import (
    Stage5BenchmarkResult,
    Stage5FailureDiagnostic,
    Stage5ReportStatus,
    Stage5ScopeResult,
)
from .stage5_service import (
    ManufacturingMaterialsProfileSliceService,
    ProviderFactory,
    SemanticInputFactory,
)

SHADOW_REPORT_RESULT_SCHEMA = "company_profile_shadow_report_result.v1"
SHADOW_REPORT_FAILURE_SCHEMA = "company_profile_shadow_report_failure.v1"
SHADOW_BATCH_RESULT_SCHEMA = "company_profile_shadow_batch_result.v1"
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class ShadowReplayContract(_StrictModel):
    """Single-run admission contract for a controlled refined-plan replay."""

    batch_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    preparation_audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_refinement_audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    primary_logical_profile: str = Field(min_length=1)
    extract_max_output_tokens: int = Field(gt=0)
    verify_max_output_tokens: int = Field(gt=0)
    timeout_seconds: float = Field(gt=0)
    max_provider_calls: int = Field(gt=0)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION


def validate_shadow_replay_admission(
    *,
    contract: ShadowReplayContract,
    batch_id: str,
    primary_logical_profile: str,
    extract_max_output_tokens: int,
    verify_max_output_tokens: int,
    timeout_seconds: float,
    max_provider_calls: int,
    manifest: ShadowSampleManifest,
    evidence_plan: ShadowEvidencePlan,
    preparation_audit: ShadowEvidencePreparationAudit,
    scope_refinement_audit: ShadowScopeRefinementAudit,
    prepared: Mapping[str, tuple[PreparedRequestScope, ...]],
    output_root: str | Path,
) -> None:
    """Reject any replay drift before a provider client is created."""

    actual = {
        "batch_id": batch_id,
        "sample_manifest_hash": manifest.manifest_hash,
        "evidence_plan_version": evidence_plan.plan_version,
        "evidence_plan_hash": evidence_plan.plan_hash,
        "preparation_audit_hash": preparation_audit.audit_hash,
        "scope_refinement_audit_hash": scope_refinement_audit.audit_hash,
        "primary_logical_profile": primary_logical_profile,
        "extract_max_output_tokens": extract_max_output_tokens,
        "verify_max_output_tokens": verify_max_output_tokens,
        "timeout_seconds": timeout_seconds,
        "max_provider_calls": max_provider_calls,
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    if actual != contract.model_dump(mode="python"):
        drift = sorted(
            key
            for key, expected in contract.model_dump(mode="python").items()
            if actual.get(key) != expected
        )
        raise ValueError(f"refined shadow replay contract mismatch: {drift}")
    if (
        preparation_audit.sample_manifest_hash != manifest.manifest_hash
        or preparation_audit.evidence_plan_hash != evidence_plan.plan_hash
        or preparation_audit.report_count != SHADOW_REPORT_COUNT
        or preparation_audit.planned_report_count != SHADOW_REPORT_COUNT
        or preparation_audit.evidence_traceability_rate != 1.0
    ):
        raise ValueError("refined shadow preparation audit does not admit this replay")
    if (
        scope_refinement_audit.sample_manifest_hash != manifest.manifest_hash
        or scope_refinement_audit.refined_plan_version != evidence_plan.plan_version
        or scope_refinement_audit.refined_plan_hash != evidence_plan.plan_hash
        or scope_refinement_audit.report_count != SHADOW_REPORT_COUNT
        or scope_refinement_audit.unsupported_assignment_count != 0
        or scope_refinement_audit.missing_required_owner_count != 0
        or scope_refinement_audit.table_context_incomplete_count != 0
        or scope_refinement_audit.evidence_traceability_rate != 1.0
        or scope_refinement_audit.provider_calls != 0
    ):
        raise ValueError("scope refinement audit does not admit this replay")
    ManufacturingMaterialsShadowBatchService._validate_admission(
        manifest,
        evidence_plan,
        prepared,
    )
    requested_root = Path(output_root)
    if requested_root.exists() and requested_root.is_symlink():
        raise ValueError("shadow output root cannot be a symlink")
    if (requested_root.resolve() / f"batch-{batch_id}").exists():
        raise FileExistsError(f"shadow batch already exists: {batch_id}")


class ShadowReportSuccess(_StrictModel):
    schema_version: Literal["company_profile_shadow_report_result.v1"] = (
        SHADOW_REPORT_RESULT_SCHEMA
    )
    batch_id: str = Field(min_length=1)
    report_run_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    report: ReportIdentity
    sample_manifest_revision: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_results: tuple[Stage5ScopeResult, ...] = Field(min_length=1)
    research_view: CompanyProfileResearchView
    report_status: Stage5ReportStatus
    benchmark: Stage5BenchmarkResult
    created_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _result_is_report_local(self) -> ShadowReportSuccess:
        if any(
            item.prepared_scope.sample_id != self.sample_id
            or item.prepared_scope.report != self.report
            for item in self.scope_results
        ):
            raise ValueError("shadow report result contains cross-report scopes")
        if self.research_view.production_authorization != PRODUCTION_AUTHORIZATION:
            raise ValueError("shadow research view cannot authorize production")
        return self


class ShadowReportFailure(_StrictModel):
    schema_version: Literal["company_profile_shadow_report_failure.v1"] = (
        SHADOW_REPORT_FAILURE_SCHEMA
    )
    batch_id: str = Field(min_length=1)
    report_run_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    report: ReportIdentity
    sample_manifest_revision: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["failed"] = "failed"
    diagnostics: tuple[Stage5FailureDiagnostic, ...] = Field(min_length=1, max_length=20)
    created_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION


class ShadowReportReference(_StrictModel):
    sample_id: str = Field(min_length=1)
    report_run_id: str = Field(min_length=1)
    status: Literal["success", "failed"]
    relative_path: str = Field(min_length=1)
    output_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_status: Stage5ReportStatus | None = None

    @model_validator(mode="after")
    def _status_matches_payload(self) -> ShadowReportReference:
        if (self.status == "success") != (self.report_status is not None):
            raise ValueError("shadow report reference status is inconsistent")
        return self


class ShadowBatchResult(_StrictModel):
    schema_version: Literal["company_profile_shadow_batch_result.v1"] = (
        SHADOW_BATCH_RESULT_SCHEMA
    )
    batch_id: str = Field(min_length=1)
    sample_manifest_revision: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    primary_logical_profile: str = Field(min_length=1)
    reports: tuple[ShadowReportReference, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    completed_report_count: int = Field(ge=0, le=SHADOW_REPORT_COUNT)
    failed_report_count: int = Field(ge=0, le=SHADOW_REPORT_COUNT)
    created_at: str = Field(min_length=1)
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _batch_counts_are_consistent(self) -> ShadowBatchResult:
        if len({item.sample_id for item in self.reports}) != SHADOW_REPORT_COUNT:
            raise ValueError("shadow batch result requires twenty unique reports")
        completed = sum(item.status == "success" for item in self.reports)
        if self.completed_report_count != completed:
            raise ValueError("shadow batch completed count mismatch")
        if self.failed_report_count != SHADOW_REPORT_COUNT - completed:
            raise ValueError("shadow batch failed count mismatch")
        if self.result_hash != _payload_hash(self, omit={"result_hash"}):
            raise ValueError("shadow batch result hash mismatch")
        return self


class ShadowBatchStore:
    """Persist one immutable result file per report and one batch manifest."""

    def __init__(self, output_root: str | Path) -> None:
        requested = Path(output_root)
        if requested.exists() and requested.is_symlink():
            raise ValueError("shadow output root cannot be a symlink")
        self.output_root = requested.resolve()
        self.output_root.mkdir(parents=True, exist_ok=True)

    def start(self, batch_id: str) -> Path:
        _validate_run_id(batch_id)
        destination = self.output_root / f"batch-{batch_id}"
        if destination.exists():
            raise FileExistsError(f"shadow batch already exists: {batch_id}")
        destination.mkdir(parents=False)
        (destination / "reports").mkdir()
        return destination

    def commit_report(
        self,
        batch_directory: Path,
        result: ShadowReportSuccess | ShadowReportFailure,
    ) -> ShadowReportReference:
        suffix = "json" if isinstance(result, ShadowReportSuccess) else "failed.json"
        relative = Path("reports") / f"{result.sample_id}.{suffix}"
        destination = batch_directory / relative
        content = _json_bytes(result.model_dump(mode="json"))
        _write_immutable(destination, content)
        return ShadowReportReference(
            sample_id=result.sample_id,
            report_run_id=result.report_run_id,
            status="success" if isinstance(result, ShadowReportSuccess) else "failed",
            relative_path=relative.as_posix(),
            output_sha256=hashlib.sha256(content).hexdigest(),
            report_status=(result.report_status if isinstance(result, ShadowReportSuccess) else None),
        )

    def commit_batch(self, batch_directory: Path, result: ShadowBatchResult) -> Path:
        _write_immutable(
            batch_directory / "manifest.json",
            _json_bytes(result.model_dump(mode="json")),
        )
        return batch_directory


class ManufacturingMaterialsShadowBatchService:
    """Admit and execute the frozen cohort through the existing report owner."""

    def __init__(
        self,
        *,
        stage5_service: ManufacturingMaterialsProfileSliceService | None = None,
    ) -> None:
        self._stage5_service = stage5_service or ManufacturingMaterialsProfileSliceService()

    def run(
        self,
        *,
        batch_id: str,
        primary_logical_profile: str,
        manifest: ShadowSampleManifest,
        evidence_plan: ShadowEvidencePlan,
        prepared: Mapping[str, tuple[PreparedRequestScope, ...]],
        store: ShadowBatchStore,
        provider_factory: ProviderFactory,
        semantic_input_factory: SemanticInputFactory | None = None,
    ) -> tuple[ShadowBatchResult, Path]:
        self._validate_admission(manifest, evidence_plan, prepared)
        batch_directory = store.start(batch_id)
        references: list[ShadowReportReference] = []
        for asset in manifest.reports:
            report_run_id = _report_run_id(batch_id, asset)
            try:
                execution = self._stage5_service.execute_prepared_report(
                    run_id=report_run_id,
                    asset=asset,
                    prepared_scopes=prepared[asset.sample_id],
                    provider_factory=provider_factory,
                    semantic_input_factory=semantic_input_factory,
                )
                report_result: ShadowReportSuccess | ShadowReportFailure = (
                    ShadowReportSuccess(
                        batch_id=batch_id,
                        report_run_id=report_run_id,
                        sample_id=asset.sample_id,
                        company_name=asset.company_name,
                        report=asset.report,
                        sample_manifest_revision=manifest.manifest_revision,
                        sample_manifest_hash=manifest.manifest_hash,
                        evidence_plan_version=evidence_plan.plan_version,
                        evidence_plan_hash=evidence_plan.plan_hash,
                        scope_results=execution.scope_results,
                        research_view=execution.research_view,
                        report_status=execution.report_status,
                        benchmark=execution.benchmark,
                        created_at=execution.created_at,
                    )
                )
            except Exception as exc:  # noqa: BLE001 - report isolation is contractual
                report_result = ShadowReportFailure(
                    batch_id=batch_id,
                    report_run_id=report_run_id,
                    sample_id=asset.sample_id,
                    company_name=asset.company_name,
                    report=asset.report,
                    sample_manifest_revision=manifest.manifest_revision,
                    sample_manifest_hash=manifest.manifest_hash,
                    evidence_plan_version=evidence_plan.plan_version,
                    evidence_plan_hash=evidence_plan.plan_hash,
                    diagnostics=(
                        Stage5FailureDiagnostic(
                            code=type(exc).__name__,
                            message=str(exc)[:2000] or "shadow report execution failed",
                            sample_id=asset.sample_id,
                        ),
                    ),
                    created_at=_utc_now(),
                )
            references.append(store.commit_report(batch_directory, report_result))
        payload = {
            "schema_version": SHADOW_BATCH_RESULT_SCHEMA,
            "batch_id": batch_id,
            "sample_manifest_revision": manifest.manifest_revision,
            "sample_manifest_hash": manifest.manifest_hash,
            "evidence_plan_version": evidence_plan.plan_version,
            "evidence_plan_hash": evidence_plan.plan_hash,
            "primary_logical_profile": primary_logical_profile,
            "reports": tuple(references),
            "completed_report_count": sum(item.status == "success" for item in references),
            "failed_report_count": sum(item.status == "failed" for item in references),
            "created_at": _utc_now(),
            "production_authorization": PRODUCTION_AUTHORIZATION,
        }
        result = ShadowBatchResult(**payload, result_hash=_payload_hash(payload))
        return result, store.commit_batch(batch_directory, result)

    @staticmethod
    def _validate_admission(
        manifest: ShadowSampleManifest,
        plan: ShadowEvidencePlan,
        prepared: Mapping[str, tuple[PreparedRequestScope, ...]],
    ) -> None:
        if (
            plan.sample_manifest_revision != manifest.manifest_revision
            or plan.sample_manifest_hash != manifest.manifest_hash
        ):
            raise ValueError("shadow Evidence plan does not match the active manifest")
        expected_ids = {item.sample_id for item in manifest.reports}
        if set(prepared) != expected_ids:
            raise ValueError("shadow prepared report identities do not match the manifest")
        for asset in manifest.reports:
            report_plan = plan.report_by_id(asset.sample_id)
            if report_plan.content_hash != asset.content_hash:
                raise ValueError("shadow report PDF hash does not match the active manifest")
            scopes = prepared[asset.sample_id]
            expected_scopes = {
                scope.scope_id
                for task in report_plan.tasks
                for scope in task.request_scopes
            }
            if {scope.scope_id for scope in scopes} != expected_scopes:
                raise ValueError("shadow prepared scopes do not match the frozen plan")
            if any(
                scope.sample_id != asset.sample_id
                or scope.report != asset.report
                or scope.plan_version != plan.plan_version
                for scope in scopes
            ):
                raise ValueError("shadow prepared scope identity mismatch")


def load_shadow_report_result(
    path: str | Path,
) -> ShadowReportSuccess | ShadowReportFailure:
    source = Path(path).read_text(encoding="utf-8")
    raw = json.loads(source)
    if raw.get("schema_version") == SHADOW_REPORT_RESULT_SCHEMA:
        return ShadowReportSuccess.model_validate_json(source)
    if raw.get("schema_version") == SHADOW_REPORT_FAILURE_SCHEMA:
        return ShadowReportFailure.model_validate_json(source)
    raise ValueError("unknown shadow report result schema")


def load_shadow_batch_result(path: str | Path) -> ShadowBatchResult:
    return ShadowBatchResult.model_validate_json(Path(path).read_text(encoding="utf-8"))


def _report_run_id(batch_id: str, asset: ShadowCohortReport) -> str:
    instrument = asset.report.instrument_id.replace(".", "-")
    value = f"{batch_id}-{instrument}"
    _validate_run_id(value)
    return value


def _validate_run_id(value: str) -> None:
    if not _RUN_ID_PATTERN.fullmatch(value):
        raise ValueError(f"invalid shadow run identity: {value}")


def _json_bytes(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode()


def _write_immutable(destination: Path, content: bytes) -> None:
    if destination.exists():
        if destination.read_bytes() != content:
            raise RuntimeError(f"immutable shadow output mismatch: {destination}")
        return
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
    temporary.write_bytes(content)
    try:
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
