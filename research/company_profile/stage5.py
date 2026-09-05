"""Isolated stage-five evidence preparation for the approved four-report slice.

This module deliberately reads only the operator-supplied local manifest, its four
PDF assets, and the versioned evidence plan.  It has no database, legacy business
profile, scheduler, API, or production writer dependency.
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.document_processing.pdf import (
    PdfParseRequest,
    PdfRouter,
    PypdfNativeAdapter,
)

from .contracts import PreparedEvidence
from .models import ChapterTask, Evidence, ReportIdentity, TextAnchor

STAGE5_SAMPLE_MANIFEST_SCHEMA = "company_profile_industry_sample_manifest.v1"
STAGE5_EVIDENCE_PLAN_SCHEMA = "company_profile_stage5_evidence_plan.v1"
STAGE5_EVIDENCE_PLAN_VERSION = "manufacturing_materials.2026-09-05.4"
STAGE5_PACKAGE = "manufacturing_materials"
STAGE5_PRODUCTION_AUTHORIZATION = "not_authorized"

APPROVED_STAGE5_SAMPLES: dict[str, tuple[str, str]] = {
    "manufacturing-materials-300750-2025": ("300750.SZ", "stable"),
    "manufacturing-materials-603659-2025": ("603659.SH", "stable"),
    "manufacturing-materials-920015-2025": ("920015.BJ", "stable"),
    "manufacturing-materials-302132-2025-regime": ("302132.SZ", "restructuring"),
}

_FROZEN_TASKS = frozenset(ChapterTask)
_PROHIBITED_PLAN_KEYS = frozenset(
    {
        "activity_actor",
        "expected_value",
        "gold",
        "semantic",
        "source_actor",
        "source_verb",
        "subject_basis",
        "subject_scope",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class PreparationFailureCode(str, Enum):
    MANIFEST_INVALID = "manifest_invalid"
    UNAPPROVED_SAMPLE = "unapproved_sample"
    ASSET_MISSING = "asset_missing"
    ASSET_PATH_INVALID = "asset_path_invalid"
    HASH_MISMATCH = "hash_mismatch"
    CONTENT_LENGTH_MISMATCH = "content_length_mismatch"
    PAGE_COUNT_MISMATCH = "page_count_mismatch"
    PLAN_INVALID = "plan_invalid"
    PLAN_IDENTITY_MISMATCH = "plan_identity_mismatch"
    PAGE_UNREADABLE = "page_unreadable"
    CONTEXT_INCOMPLETE = "context_incomplete"
    HEADER_MISSING = "header_missing"
    UNIT_MISSING = "unit_missing"
    FOOTNOTE_MISSING = "footnote_missing"
    CONTINUATION_INCOMPLETE = "continuation_incomplete"


class EvidencePreparationError(RuntimeError):
    """Typed pre-provider failure for manifest or Evidence preparation."""

    def __init__(
        self,
        code: PreparationFailureCode,
        message: str,
        *,
        sample_id: str | None = None,
        chapter_task: ChapterTask | None = None,
        scope_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.sample_id = sample_id
        self.chapter_task = chapter_task
        self.scope_id = scope_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "message": str(self),
            "sample_id": self.sample_id,
            "chapter_task": self.chapter_task.value if self.chapter_task else None,
            "scope_id": self.scope_id,
        }


class Stage5ReportAsset(_StrictModel):
    sample_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    exchange: Literal["SSE", "SZSE", "BSE"]
    report: ReportIdentity
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    local_path: Path
    content_length: int = Field(gt=0)
    page_count: int = Field(gt=0)
    industry_package: Literal["manufacturing_materials"] = STAGE5_PACKAGE
    regime_type: Literal["stable", "restructuring"]
    regime_effective_period: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = (
        STAGE5_PRODUCTION_AUTHORIZATION
    )


class Stage5SampleManifest(_StrictModel):
    schema_version: Literal["company_profile_industry_sample_manifest.v1"] = (
        STAGE5_SAMPLE_MANIFEST_SCHEMA
    )
    manifest_revision: str = Field(min_length=1)
    reports: tuple[Stage5ReportAsset, ...] = Field(min_length=4, max_length=4)
    production_authorization: Literal["not_authorized"] = (
        STAGE5_PRODUCTION_AUTHORIZATION
    )

    @model_validator(mode="after")
    def _approved_closed_set(self) -> Stage5SampleManifest:
        ids = [item.sample_id for item in self.reports]
        if len(ids) != len(set(ids)):
            raise ValueError("sample manifest contains duplicate sample_id")
        if set(ids) != set(APPROVED_STAGE5_SAMPLES):
            raise ValueError(
                "sample manifest must contain exactly the approved reports"
            )
        for item in self.reports:
            instrument, regime = APPROVED_STAGE5_SAMPLES[item.sample_id]
            if item.report.instrument_id != instrument or item.regime_type != regime:
                raise ValueError("sample identity or regime does not match approval")
            if item.report.report_period != "2025-12-31":
                raise ValueError(
                    "stage-five slice accepts only the approved 2025 reports"
                )
        return self

    def report_by_id(self, sample_id: str) -> Stage5ReportAsset:
        for report in self.reports:
            if report.sample_id == sample_id:
                return report
        raise EvidencePreparationError(
            PreparationFailureCode.UNAPPROVED_SAMPLE,
            f"sample is outside the approved stage-five manifest: {sample_id}",
            sample_id=sample_id,
        )


class EvidenceScopePlan(_StrictModel):
    scope_id: str = Field(min_length=1)
    field_ids: tuple[str, ...] = Field(min_length=1)
    pages: tuple[int, ...] = Field(min_length=1)
    section_titles: tuple[str, ...] = Field(min_length=1)
    anchor_terms: tuple[str, ...] = Field(min_length=1)
    required_headers: tuple[str, ...] = ()
    required_units: tuple[str, ...] = ()
    required_footnotes: tuple[str, ...] = ()
    continuation_required: bool = False
    printed_page_labels: dict[str, str] = Field(default_factory=dict)
    subject_pages: tuple[int, ...] = ()
    subject_anchor_terms: tuple[str, ...] = ()
    source_row_dimensions: dict[str, str] = Field(default_factory=dict)
    candidate_pages: tuple[int, ...] = ()

    @model_validator(mode="after")
    def _pages_are_continuous(self) -> EvidenceScopePlan:
        if tuple(sorted(set(self.pages))) != self.pages:
            raise ValueError("evidence scope pages must be sorted and unique")
        if any(page < 1 for page in self.pages):
            raise ValueError("evidence plan uses one-based physical pages")
        if len(self.pages) > 1 and any(
            right != left + 1 for left, right in zip(self.pages, self.pages[1:])
        ):
            raise ValueError("each request scope must use continuous physical pages")
        if self.continuation_required and len(self.pages) < 2:
            raise ValueError("continuation scope requires at least two pages")
        if any(int(page) not in self.pages for page in self.printed_page_labels):
            raise ValueError("printed page labels must belong to the scope pages")
        if tuple(sorted(set(self.subject_pages))) != self.subject_pages or any(
            page < 1 for page in self.subject_pages
        ):
            raise ValueError(
                "subject evidence pages must be sorted unique physical pages"
            )
        if set(self.pages) & set(self.subject_pages):
            raise ValueError(
                "subject evidence pages must be separate from primary pages"
            )
        if bool(self.subject_pages) != bool(self.subject_anchor_terms):
            raise ValueError(
                "subject evidence pages and anchor terms must be supplied together"
            )
        if any(
            not label.strip() or not dimension.strip()
            for label, dimension in self.source_row_dimensions.items()
        ):
            raise ValueError(
                "source row dimension mappings require non-empty labels and dimensions"
            )
        if tuple(sorted(set(self.candidate_pages))) != self.candidate_pages or any(
            page not in self.pages for page in self.candidate_pages
        ):
            raise ValueError("candidate pages must be sorted unique primary pages")
        return self


class EvidenceTaskPlan(_StrictModel):
    chapter_task: ChapterTask
    request_scopes: tuple[EvidenceScopePlan, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _scopes_are_unique(self) -> EvidenceTaskPlan:
        ids = [scope.scope_id for scope in self.request_scopes]
        if len(ids) != len(set(ids)):
            raise ValueError("request scope identity must be unique within a task")
        return self


class EvidenceReportPlan(_StrictModel):
    sample_id: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_version: str = Field(min_length=1)
    tasks: tuple[EvidenceTaskPlan, ...] = Field(min_length=6, max_length=6)

    @model_validator(mode="after")
    def _six_frozen_tasks(self) -> EvidenceReportPlan:
        tasks = [item.chapter_task for item in self.tasks]
        if len(tasks) != len(set(tasks)) or set(tasks) != _FROZEN_TASKS:
            raise ValueError(
                "each report plan must contain the six frozen chapter tasks"
            )
        return self

    def task(self, chapter_task: ChapterTask) -> EvidenceTaskPlan:
        for task in self.tasks:
            if task.chapter_task == chapter_task:
                return task
        raise EvidencePreparationError(
            PreparationFailureCode.PLAN_INVALID,
            f"missing evidence task plan: {chapter_task.value}",
            sample_id=self.sample_id,
            chapter_task=chapter_task,
        )


class Stage5EvidencePlan(_StrictModel):
    schema_version: Literal["company_profile_stage5_evidence_plan.v1"] = (
        STAGE5_EVIDENCE_PLAN_SCHEMA
    )
    plan_version: Literal["manufacturing_materials.2026-09-05.4"] = (
        STAGE5_EVIDENCE_PLAN_VERSION
    )
    sample_manifest_revision: str = Field(min_length=1)
    page_coordinate_system: Literal["one_based_pdf_physical_page"]
    production_authorization: Literal["not_authorized"] = (
        STAGE5_PRODUCTION_AUTHORIZATION
    )
    reports: tuple[EvidenceReportPlan, ...] = Field(min_length=4, max_length=4)

    @model_validator(mode="after")
    def _approved_closed_set(self) -> Stage5EvidencePlan:
        ids = [item.sample_id for item in self.reports]
        if len(ids) != len(set(ids)) or set(ids) != set(APPROVED_STAGE5_SAMPLES):
            raise ValueError("evidence plan must contain exactly the approved reports")
        return self

    def report_by_id(self, sample_id: str) -> EvidenceReportPlan:
        for report in self.reports:
            if report.sample_id == sample_id:
                return report
        raise EvidencePreparationError(
            PreparationFailureCode.UNAPPROVED_SAMPLE,
            f"sample is outside the approved evidence plan: {sample_id}",
            sample_id=sample_id,
        )


class PreparedPageContext(_StrictModel):
    page: int = Field(ge=1)
    printed_page_label: str | None = None
    text: str = Field(min_length=1)
    text_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    extraction_method: str = Field(min_length=1)
    quality_status: str = Field(min_length=1)


class PreparedRequestScope(_StrictModel):
    schema_version: Literal["company_profile_stage5_prepared_scope.v1"] = (
        "company_profile_stage5_prepared_scope.v1"
    )
    sample_id: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    chapter_task: ChapterTask
    field_ids: tuple[str, ...] = Field(min_length=1)
    report: ReportIdentity
    evidence_bundle: tuple[PreparedEvidence, ...] = Field(min_length=1)
    page_contexts: tuple[PreparedPageContext, ...] = Field(min_length=1)
    plan_version: str = Field(min_length=1)
    source_row_dimensions: dict[str, str] = Field(default_factory=dict)
    candidate_pages: tuple[int, ...] = ()
    production_authorization: Literal["not_authorized"] = (
        STAGE5_PRODUCTION_AUTHORIZATION
    )


def load_stage5_sample_manifest(
    manifest_path: str | Path,
    *,
    repository_root: str | Path,
) -> Stage5SampleManifest:
    """Load and verify the approved local manifest without consulting a database."""

    path = Path(manifest_path)
    root = Path(repository_root).resolve()
    try:
        source = path.read_text(encoding="utf-8")
        raw = json.loads(source)
    except (OSError, ValueError) as exc:
        raise EvidencePreparationError(
            PreparationFailureCode.MANIFEST_INVALID,
            f"sample manifest is unreadable: {exc}",
        ) from exc
    try:
        reports = tuple(_manifest_report(item, raw, root) for item in raw["reports"])
        manifest = Stage5SampleManifest(
            schema_version=raw.get("schema_version"),
            manifest_revision=raw.get("manifest_revision"),
            reports=reports,
            production_authorization=raw.get("production_authorization"),
        )
    except EvidencePreparationError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidencePreparationError(
            PreparationFailureCode.MANIFEST_INVALID,
            f"sample manifest violates the stage-five contract: {exc}",
        ) from exc
    return manifest


def load_stage5_evidence_plan(plan_path: str | Path) -> Stage5EvidencePlan:
    """Load a versioned evidence plan and reject embedded semantic/Gold answers."""

    path = Path(plan_path)
    try:
        source = path.read_text(encoding="utf-8")
        raw = json.loads(source)
    except (OSError, ValueError) as exc:
        raise EvidencePreparationError(
            PreparationFailureCode.PLAN_INVALID,
            f"evidence plan is unreadable: {exc}",
        ) from exc
    prohibited = _find_prohibited_plan_keys(raw)
    if prohibited:
        raise EvidencePreparationError(
            PreparationFailureCode.PLAN_INVALID,
            f"evidence plan contains evaluation-only semantic keys: {sorted(prohibited)}",
        )
    try:
        return Stage5EvidencePlan.model_validate_json(source)
    except (TypeError, ValueError) as exc:
        raise EvidencePreparationError(
            PreparationFailureCode.PLAN_INVALID,
            f"evidence plan violates the stage-five contract: {exc}",
        ) from exc


class Stage5EvidencePreparer:
    """Build bounded page contexts and stage-four PreparedEvidence from plans."""

    def __init__(self, router: PdfRouter | None = None) -> None:
        self._router = router or PdfRouter(native=PypdfNativeAdapter())

    def prepare_report(
        self,
        *,
        manifest: Stage5SampleManifest,
        evidence_plan: Stage5EvidencePlan,
        sample_id: str,
    ) -> tuple[PreparedRequestScope, ...]:
        asset = manifest.report_by_id(sample_id)
        plan = evidence_plan.report_by_id(sample_id)
        if evidence_plan.sample_manifest_revision != manifest.manifest_revision:
            raise EvidencePreparationError(
                PreparationFailureCode.PLAN_IDENTITY_MISMATCH,
                "evidence plan manifest revision does not match the approved manifest",
                sample_id=sample_id,
            )
        if plan.content_hash != asset.content_hash:
            raise EvidencePreparationError(
                PreparationFailureCode.PLAN_IDENTITY_MISMATCH,
                "evidence plan PDF hash does not match the approved manifest",
                sample_id=sample_id,
            )
        pages = tuple(
            sorted(
                {
                    page
                    for task in plan.tasks
                    for scope in task.request_scopes
                    for page in (*scope.pages, *scope.subject_pages)
                }
            )
        )
        content = asset.local_path.read_bytes()
        result = self._router.parse(
            PdfParseRequest(
                content=content,
                expected_content_hash=asset.content_hash,
                target_pages=pages,
                ocr_mode="none",
                recovery_policy="native_first",
            )
        )
        if result.page_count != asset.page_count:
            raise EvidencePreparationError(
                PreparationFailureCode.PAGE_COUNT_MISMATCH,
                f"PDF page count {result.page_count} != manifest {asset.page_count}",
                sample_id=sample_id,
            )
        page_results = {item.page_number: item for item in result.pages}
        prepared: list[PreparedRequestScope] = []
        for task in plan.tasks:
            for scope in task.request_scopes:
                prepared.append(
                    self._prepare_scope(asset, task.chapter_task, scope, page_results)
                )
        return tuple(prepared)

    def _prepare_scope(
        self,
        asset: Stage5ReportAsset,
        chapter_task: ChapterTask,
        scope: EvidenceScopePlan,
        page_results: dict[int, Any],
    ) -> PreparedRequestScope:
        contexts: list[PreparedPageContext] = []
        evidence: list[PreparedEvidence] = []
        combined_parts: list[str] = []
        for page_number in (*scope.pages, *scope.subject_pages):
            is_subject_evidence = page_number in scope.subject_pages
            page = page_results.get(page_number)
            if page is None or not page.selected_usable_for_semantic:
                raise EvidencePreparationError(
                    PreparationFailureCode.PAGE_UNREADABLE,
                    f"planned PDF page is unavailable or unreadable: {page_number}",
                    sample_id=asset.sample_id,
                    chapter_task=chapter_task,
                    scope_id=scope.scope_id,
                )
            text = page.selected_text.strip()
            if not text:
                raise EvidencePreparationError(
                    PreparationFailureCode.PAGE_UNREADABLE,
                    f"planned PDF page has no readable text: {page_number}",
                    sample_id=asset.sample_id,
                    chapter_task=chapter_task,
                    scope_id=scope.scope_id,
                )
            if not is_subject_evidence:
                combined_parts.append(text)
            contexts.append(
                PreparedPageContext(
                    page=page_number,
                    printed_page_label=scope.printed_page_labels.get(str(page_number)),
                    text=text,
                    text_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    extraction_method=page.selected_method,
                    quality_status=page.quality_status,
                )
            )
            quote = _bounded_anchor(
                text,
                scope.subject_anchor_terms
                if is_subject_evidence
                else scope.anchor_terms,
            )
            evidence.append(
                PreparedEvidence(
                    evidence=Evidence(
                        evidence_id=_evidence_id(
                            asset.sample_id,
                            chapter_task,
                            scope.scope_id,
                            page_number,
                            quote,
                        ),
                        report=asset.report,
                        page=page_number,
                        printed_page_label=scope.printed_page_labels.get(
                            str(page_number)
                        ),
                        section_title=(
                            "主体口径核对"
                            if is_subject_evidence
                            else " / ".join(scope.section_titles)
                        ),
                        continuation_pages=(
                            ()
                            if is_subject_evidence
                            else tuple(
                                item for item in scope.pages if item != page_number
                            )
                        ),
                        subject_evidence_pages=(
                            () if is_subject_evidence else scope.subject_pages
                        ),
                        anchor=TextAnchor(bounded_quote=quote),
                    ),
                    context_complete=True,
                    headers_complete=True,
                    unit_context_complete=True,
                    footnotes_complete=True,
                    continuation_complete=True,
                    source_readable=True,
                )
            )
        combined = "\n".join(combined_parts)
        if scope.subject_pages:
            subject_text = "\n".join(
                context.text
                for context in contexts
                if context.page in scope.subject_pages
            )
            self._require_terms(
                subject_text,
                scope.subject_anchor_terms,
                PreparationFailureCode.CONTEXT_INCOMPLETE,
                "subject anchor",
                asset,
                chapter_task,
                scope,
            )
        self._require_terms(
            combined,
            scope.anchor_terms,
            PreparationFailureCode.CONTEXT_INCOMPLETE,
            "anchor",
            asset,
            chapter_task,
            scope,
        )
        self._require_terms(
            combined,
            tuple(scope.source_row_dimensions),
            PreparationFailureCode.CONTEXT_INCOMPLETE,
            "source row label",
            asset,
            chapter_task,
            scope,
        )
        self._require_terms(
            combined,
            tuple(dict.fromkeys(scope.source_row_dimensions.values())),
            PreparationFailureCode.CONTEXT_INCOMPLETE,
            "source row dimension",
            asset,
            chapter_task,
            scope,
        )
        self._require_terms(
            combined,
            scope.required_headers,
            PreparationFailureCode.HEADER_MISSING,
            "header",
            asset,
            chapter_task,
            scope,
        )
        self._require_any_term(
            combined,
            scope.required_units,
            PreparationFailureCode.UNIT_MISSING,
            "unit",
            asset,
            chapter_task,
            scope,
        )
        self._require_terms(
            combined,
            scope.required_footnotes,
            PreparationFailureCode.FOOTNOTE_MISSING,
            "footnote",
            asset,
            chapter_task,
            scope,
        )
        if scope.continuation_required and len(
            [context for context in contexts if context.page in scope.pages]
        ) != len(scope.pages):
            raise EvidencePreparationError(
                PreparationFailureCode.CONTINUATION_INCOMPLETE,
                "continued table context is incomplete",
                sample_id=asset.sample_id,
                chapter_task=chapter_task,
                scope_id=scope.scope_id,
            )
        return PreparedRequestScope(
            sample_id=asset.sample_id,
            scope_id=scope.scope_id,
            chapter_task=chapter_task,
            field_ids=scope.field_ids,
            report=asset.report,
            evidence_bundle=tuple(evidence),
            page_contexts=tuple(contexts),
            plan_version=STAGE5_EVIDENCE_PLAN_VERSION,
            source_row_dimensions=scope.source_row_dimensions,
            candidate_pages=scope.candidate_pages,
        )

    @staticmethod
    def _require_terms(
        text: str,
        terms: tuple[str, ...],
        code: PreparationFailureCode,
        label: str,
        asset: Stage5ReportAsset,
        chapter_task: ChapterTask,
        scope: EvidenceScopePlan,
    ) -> None:
        normalized = _normalize_source_text(text)
        missing = [
            term for term in terms if _normalize_source_text(term) not in normalized
        ]
        if missing:
            raise EvidencePreparationError(
                code,
                f"required {label} context is missing: {missing}",
                sample_id=asset.sample_id,
                chapter_task=chapter_task,
                scope_id=scope.scope_id,
            )

    @staticmethod
    def _require_any_term(
        text: str,
        terms: tuple[str, ...],
        code: PreparationFailureCode,
        label: str,
        asset: Stage5ReportAsset,
        chapter_task: ChapterTask,
        scope: EvidenceScopePlan,
    ) -> None:
        normalized = _normalize_source_text(text)
        if terms and not any(
            _normalize_source_text(term) in normalized for term in terms
        ):
            raise EvidencePreparationError(
                code,
                f"none of the approved {label} tokens is present: {list(terms)}",
                sample_id=asset.sample_id,
                chapter_task=chapter_task,
                scope_id=scope.scope_id,
            )


def _manifest_report(
    raw_report: dict[str, Any],
    raw_manifest: dict[str, Any],
    repository_root: Path,
) -> Stage5ReportAsset:
    sample_id = str(raw_report.get("sample_id") or "")
    identity = raw_report["report_identity"]
    package = raw_report["business_regime"]
    local_path = _resolve_local_asset(
        repository_root, identity.get("local_path"), sample_id=sample_id
    )
    content = local_path.read_bytes()
    expected_hash = str(identity.get("content_hash") or "")
    actual_hash = hashlib.sha256(content).hexdigest()
    if actual_hash != expected_hash:
        raise EvidencePreparationError(
            PreparationFailureCode.HASH_MISMATCH,
            f"PDF hash mismatch for {sample_id}",
            sample_id=sample_id,
        )
    if len(content) != int(identity.get("content_length") or 0):
        raise EvidencePreparationError(
            PreparationFailureCode.CONTENT_LENGTH_MISMATCH,
            f"PDF content length mismatch for {sample_id}",
            sample_id=sample_id,
        )
    if package.get("primary_package_candidate") != STAGE5_PACKAGE:
        raise EvidencePreparationError(
            PreparationFailureCode.MANIFEST_INVALID,
            f"sample is not assigned to the approved industry package: {sample_id}",
            sample_id=sample_id,
        )
    return Stage5ReportAsset(
        sample_id=sample_id,
        company_name=identity["company_name"],
        exchange=identity["exchange"],
        report=ReportIdentity(
            instrument_id=identity["instrument_id"],
            report_id=identity["document_id"],
            document_version=identity["document_version"],
            report_period=identity["report_period"],
            published_at=identity["published_at"],
            document_type=identity["document_type"],
        ),
        content_hash=expected_hash,
        local_path=local_path,
        content_length=identity["content_length"],
        page_count=identity["page_count"],
        industry_package=package["primary_package_candidate"],
        regime_type=package["regime_type"],
        regime_effective_period=package["effective_period"],
        production_authorization=raw_manifest.get("production_authorization"),
    )


def _resolve_local_asset(
    repository_root: Path,
    raw_path: Any,
    *,
    sample_id: str,
) -> Path:
    relative = Path(str(raw_path or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise EvidencePreparationError(
            PreparationFailureCode.ASSET_PATH_INVALID,
            f"PDF path must be repository-relative: {raw_path}",
            sample_id=sample_id,
        )
    resolved = (repository_root / relative).resolve()
    try:
        resolved.relative_to(repository_root)
    except ValueError as exc:
        raise EvidencePreparationError(
            PreparationFailureCode.ASSET_PATH_INVALID,
            f"PDF path escapes the repository: {raw_path}",
            sample_id=sample_id,
        ) from exc
    if not resolved.is_file():
        raise EvidencePreparationError(
            PreparationFailureCode.ASSET_MISSING,
            f"approved PDF asset is missing: {relative}",
            sample_id=sample_id,
        )
    return resolved


def _find_prohibited_plan_keys(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _PROHIBITED_PLAN_KEYS:
                found.add(key)
            found.update(_find_prohibited_plan_keys(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_find_prohibited_plan_keys(item))
    return found


def _bounded_anchor(text: str, terms: tuple[str, ...], *, limit: int = 4000) -> str:
    positions = [text.find(term) for term in terms if term and term in text]
    if not positions:
        return text[:limit].strip()
    start = max(min(positions) - 500, 0)
    return text[start : start + limit].strip()


def _normalize_source_text(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value).strip()
    return re.sub(
        r"(?<=[\u3400-\u9fff，。；：、（）])\s+(?=[\u3400-\u9fff，。；：、（）])",
        "",
        normalized,
    )


def _evidence_id(
    sample_id: str,
    chapter_task: ChapterTask,
    scope_id: str,
    page: int,
    quote: str,
) -> str:
    payload = "\0".join(
        (sample_id, chapter_task.value, scope_id, str(page), quote)
    ).encode("utf-8")
    return f"stage5-evidence-{hashlib.sha256(payload).hexdigest()[:24]}"
