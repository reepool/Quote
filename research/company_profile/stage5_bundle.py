"""Atomic, file-only run bundles for the isolated stage-five research slice."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .contracts import CompanyProfileTaskResult
from .models import PRODUCTION_AUTHORIZATION, ReportIdentity
from .projection import CompanyProfileResearchView
from .stage5 import APPROVED_STAGE5_SAMPLES, PreparedRequestScope

STAGE5_REPORT_BUNDLE_SCHEMA = "company_profile_stage5_report_bundle.v1"
STAGE5_RUN_BUNDLE_SCHEMA = "company_profile_stage5_run_bundle.v1"
STAGE5_FAILURE_MANIFEST_SCHEMA = "company_profile_stage5_failure_manifest.v1"
STAGE5_PREPARATION_BUNDLE_SCHEMA = "company_profile_stage5_preparation_bundle.v1"
_TEMP_PREFIX = ".stage5-tmp-"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class Stage5ReviewAction(str, Enum):
    ACCEPT_FOR_RESEARCH_REVIEW = "accept_for_research_review"
    REJECT = "reject"
    HOLD = "hold"
    REQUEST_REPAIR = "request_repair"


class Stage5ReportStatus(str, Enum):
    COMPLETE = "complete"
    HOLD = "hold"
    FAILED = "failed"


class Stage5OverallStatus(str, Enum):
    RESEARCH_SLICE_PASS = "research_slice_pass"
    HOLD = "hold"
    FAILED = "failed"


class Stage5ReviewDecision(_StrictModel):
    review_id: str = Field(min_length=1)
    action: Stage5ReviewAction
    reason: str | None = Field(default=None, max_length=2000)


class Stage5BenchmarkDimension(_StrictModel):
    name: str = Field(min_length=1)
    passed: bool
    blocker_codes: tuple[str, ...] = ()
    details: dict[str, Any] = Field(default_factory=dict)


class Stage5BenchmarkResult(_StrictModel):
    decision: Literal["pass", "hold", "not_evaluated"]
    dimensions: tuple[Stage5BenchmarkDimension, ...] = ()
    gold_evaluation_only: Literal[True] = True

    @model_validator(mode="after")
    def _blockers_force_hold(self) -> Stage5BenchmarkResult:
        has_blocker = any(item.blocker_codes for item in self.dimensions)
        if has_blocker and self.decision == "pass":
            raise ValueError("benchmark blockers cannot be hidden by a pass decision")
        return self


class Stage5ProviderCallTrace(_StrictModel):
    call_type: Literal["extract", "repair", "verify"]
    semantic_request_id: str = Field(min_length=1)
    gateway_request_id: str | None = None
    status: Literal["success", "failed"]
    profile: str = Field(min_length=1)
    provider: str | None = None
    model: str | None = None
    response_hash: str | None = None
    error_code: str | None = None
    error_detail: str | None = Field(default=None, max_length=2000)


class Stage5ScopeResult(_StrictModel):
    scope_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    prepared_scope: PreparedRequestScope
    task_result: CompanyProfileTaskResult
    provider_call_types: tuple[Literal["extract", "repair", "verify"], ...] = ()
    provider_traces: tuple[Stage5ProviderCallTrace, ...] = ()

    @model_validator(mode="after")
    def _scope_identity_matches(self) -> Stage5ScopeResult:
        if self.scope_id != self.prepared_scope.scope_id:
            raise ValueError("scope result identity must match prepared scope")
        if self.request_id != self.task_result.request_id:
            raise ValueError("scope result request identity must match task result")
        if tuple(self.task_result.provider_calls) != self.provider_call_types:
            raise ValueError("provider call types must match the bounded workflow result")
        if self.provider_traces and tuple(
            item.call_type for item in self.provider_traces
        ) != self.provider_call_types:
            raise ValueError("provider traces must match the bounded workflow call order")
        return self


class Stage5ReportBundle(_StrictModel):
    schema_version: Literal["company_profile_stage5_report_bundle.v1"] = (
        STAGE5_REPORT_BUNDLE_SCHEMA
    )
    run_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    report: ReportIdentity
    sample_manifest_revision: str = Field(min_length=1)
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_results: tuple[Stage5ScopeResult, ...] = Field(min_length=1)
    review_decisions: tuple[Stage5ReviewDecision, ...] = ()
    research_view: CompanyProfileResearchView
    report_status: Stage5ReportStatus
    benchmark: Stage5BenchmarkResult
    created_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _report_identity_is_closed(self) -> Stage5ReportBundle:
        expected = APPROVED_STAGE5_SAMPLES.get(self.sample_id)
        if expected is None or self.report.instrument_id != expected[0]:
            raise ValueError("report bundle is outside the approved sample set")
        if any(
            item.prepared_scope.sample_id != self.sample_id
            or item.prepared_scope.report != self.report
            for item in self.scope_results
        ):
            raise ValueError("scope results must belong to the report bundle")
        if self.research_view.production_authorization != "not_authorized":
            raise ValueError("research view cannot authorize production")
        return self


class Stage5RunBundle(_StrictModel):
    schema_version: Literal["company_profile_stage5_run_bundle.v1"] = (
        STAGE5_RUN_BUNDLE_SCHEMA
    )
    run_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    sample_manifest_revision: str = Field(min_length=1)
    evidence_plan_version: str = Field(min_length=1)
    reports: tuple[Stage5ReportBundle, ...] = Field(min_length=1, max_length=4)
    overall_status: Stage5OverallStatus
    retained_bundle_ids: tuple[str, ...] = ()
    created_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _run_identity_and_status(self) -> Stage5RunBundle:
        sample_ids = [item.sample_id for item in self.reports]
        if len(sample_ids) != len(set(sample_ids)):
            raise ValueError("run bundle cannot contain duplicate reports")
        if any(item.run_id != self.run_id for item in self.reports):
            raise ValueError("report bundle run_id must match its parent")
        if any(item.report_status == Stage5ReportStatus.FAILED for item in self.reports):
            if self.overall_status != Stage5OverallStatus.FAILED:
                raise ValueError("a failed report makes the run failed")
        elif (
            any(item.report_status == Stage5ReportStatus.HOLD for item in self.reports)
            and self.overall_status != Stage5OverallStatus.HOLD
        ):
            raise ValueError("a held report makes the run hold")
        if (
            self.overall_status == Stage5OverallStatus.RESEARCH_SLICE_PASS
            and set(sample_ids) != set(APPROVED_STAGE5_SAMPLES)
        ):
            raise ValueError("research_slice_pass requires all four approved reports")
        return self


class Stage5FailureDiagnostic(_StrictModel):
    code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    sample_id: str | None = Field(default=None, max_length=120)
    scope_id: str | None = Field(default=None, max_length=160)


class Stage5PreparedScopeSummary(_StrictModel):
    sample_id: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    chapter_task: str = Field(min_length=1)
    field_ids: tuple[str, ...] = Field(min_length=1)
    physical_pages: tuple[int, ...] = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    page_text_hashes: tuple[str, ...] = Field(min_length=1)


class Stage5PreparationBundle(_StrictModel):
    schema_version: Literal["company_profile_stage5_preparation_bundle.v1"] = (
        STAGE5_PREPARATION_BUNDLE_SCHEMA
    )
    run_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    sample_manifest_revision: str = Field(min_length=1)
    evidence_plan_version: str = Field(min_length=1)
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    scopes: tuple[Stage5PreparedScopeSummary, ...] = Field(min_length=1)
    provider_calls: Literal[0] = 0
    status: Literal["prepared"] = "prepared"
    created_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION


class Stage5FailureManifest(_StrictModel):
    schema_version: Literal["company_profile_stage5_failure_manifest.v1"] = (
        STAGE5_FAILURE_MANIFEST_SCHEMA
    )
    run_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    status: Literal["failed"] = "failed"
    reusable: Literal[False] = False
    diagnostics: tuple[Stage5FailureDiagnostic, ...] = Field(min_length=1, max_length=20)
    created_at: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION


class Stage5GarbageAudit(_StrictModel):
    output_root: Path
    abandoned_paths: tuple[Path, ...]
    removed_paths: tuple[Path, ...]
    retained_bundle_paths: tuple[Path, ...]
    unsafe_symlinks: tuple[Path, ...] = ()


class Stage5RunBundleStore:
    """Commit immutable stage-five bundles beneath one explicit isolated root."""

    def __init__(
        self,
        output_root: str | Path,
        *,
        repository_root: str | Path,
    ) -> None:
        self.repository_root = Path(repository_root).resolve()
        requested_root = Path(output_root)
        if requested_root.exists() and requested_root.is_symlink():
            raise ValueError("stage-five output root cannot be a symlink")
        self.output_root = requested_root.resolve()
        self._validate_output_root()
        self.output_root.mkdir(parents=True, exist_ok=True)

    def commit(self, bundle: Stage5RunBundle) -> Path:
        """Atomically commit one immutable run directory."""

        destination = self._run_destination(bundle.run_id)
        if destination.exists() or self.failure_manifest_path(bundle.run_id).exists():
            raise FileExistsError(f"stage-five run already exists: {bundle.run_id}")
        temporary = self.output_root / f"{_TEMP_PREFIX}{bundle.run_id}-{uuid.uuid4().hex}"
        temporary.mkdir(parents=False, exist_ok=False)
        try:
            reports_dir = temporary / "reports"
            reports_dir.mkdir()
            for report in bundle.reports:
                _write_json(
                    reports_dir / f"{report.sample_id}.json",
                    report.model_dump(mode="json"),
                )
            _write_json(
                temporary / "manifest.json",
                bundle.model_dump(mode="json"),
            )
            os.replace(temporary, destination)
        except Exception:
            if temporary.exists():
                shutil.rmtree(temporary)
            raise
        return destination

    def commit_preparation(self, bundle: Stage5PreparationBundle) -> Path:
        """Atomically commit a provider-free preparation audit bundle."""

        destination = self.output_root / f"preparation-{bundle.run_id}"
        if destination.exists() or self.failure_manifest_path(bundle.run_id).exists():
            raise FileExistsError(f"stage-five run already exists: {bundle.run_id}")
        temporary = self.output_root / f"{_TEMP_PREFIX}{bundle.run_id}-{uuid.uuid4().hex}"
        temporary.mkdir(parents=False, exist_ok=False)
        try:
            _write_json(
                temporary / "manifest.json",
                bundle.model_dump(mode="json"),
            )
            os.replace(temporary, destination)
        except Exception:
            if temporary.exists():
                shutil.rmtree(temporary)
            raise
        return destination

    def record_failure(
        self,
        run_id: str,
        diagnostics: tuple[Stage5FailureDiagnostic, ...],
    ) -> Path:
        """Remove uncommitted paths and retain one bounded non-reusable manifest."""

        self._validate_run_id(run_id)
        for path in self.output_root.glob(f"{_TEMP_PREFIX}{run_id}-*"):
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
        failure_path = self.failure_manifest_path(run_id)
        destination = self._run_destination(run_id)
        if destination.exists() or failure_path.exists():
            raise FileExistsError(f"stage-five run already exists: {run_id}")
        manifest = Stage5FailureManifest(
            run_id=run_id,
            diagnostics=diagnostics,
            created_at=_utc_now(),
        )
        temporary = self.output_root / f"{_TEMP_PREFIX}{run_id}-failure-{uuid.uuid4().hex}"
        try:
            _write_json(temporary, manifest.model_dump(mode="json"))
            os.replace(temporary, failure_path)
        finally:
            if temporary.exists() and temporary.is_file():
                temporary.unlink()
        return failure_path

    def audit_garbage(self, *, remove: bool = False) -> Stage5GarbageAudit:
        """List or remove abandoned temp paths without touching committed bundles."""

        abandoned: list[Path] = []
        removed: list[Path] = []
        unsafe_symlinks: list[Path] = []
        for path in sorted(self.output_root.iterdir()):
            if not path.name.startswith(_TEMP_PREFIX):
                continue
            if path.is_symlink():
                unsafe_symlinks.append(path)
                continue
            abandoned.append(path)
            if remove:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                removed.append(path)
        retained = tuple(
            path
            for path in sorted(self.output_root.iterdir())
            if path.name.startswith(("run-", "preparation-"))
            or path.name.endswith(".failed.json")
        )
        return Stage5GarbageAudit(
            output_root=self.output_root,
            abandoned_paths=tuple(abandoned),
            removed_paths=tuple(removed),
            retained_bundle_paths=retained,
            unsafe_symlinks=tuple(unsafe_symlinks),
        )

    def failure_manifest_path(self, run_id: str) -> Path:
        self._validate_run_id(run_id)
        return self.output_root / f"run-{run_id}.failed.json"

    def _run_destination(self, run_id: str) -> Path:
        self._validate_run_id(run_id)
        return self.output_root / f"run-{run_id}"

    def _validate_run_id(self, run_id: str) -> None:
        if not run_id or not run_id[0].isalnum() or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for character in run_id
        ):
            raise ValueError("run_id contains unsafe path characters")

    def _validate_output_root(self) -> None:
        if self.output_root == self.repository_root:
            raise ValueError("stage-five output root cannot be the repository root")
        try:
            relative = self.output_root.relative_to(self.repository_root)
        except ValueError:
            relative = None
        if (
            relative is not None
            and relative.parts
            and relative.parts[0] in {"data", "config"}
        ):
            raise ValueError("stage-five output root cannot use production data/config")
        if "business_profile" in self.output_root.as_posix().lower():
            raise ValueError("stage-five output root cannot use a legacy profile path")


def stage5_evidence_plan_hash(path: str | Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
