"""Automatic six-chapter Evidence planning for the frozen shadow cohort."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

from pydantic import ConfigDict, Field, model_validator

from research.business_profile_disclosure_templates import (
    ResolvedDisclosureTemplate,
    load_disclosure_template_catalog,
)
from research.business_profile_pdf_artifacts import (
    BusinessProfilePdfArtifactExtractor,
    BusinessProfilePdfArtifactStore,
    ensure_archived_pdf_page_artifact,
)
from research.business_profile_section_selection import (
    ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
    BusinessProfileSectionSelector,
)
from research.business_profile_semantic_runtime import (
    _recover_business_profile_document,
)

from .models import PRODUCTION_AUTHORIZATION, ChapterTask
from .shadow_batch import (
    SHADOW_REPORT_COUNT,
    ShadowSampleManifest,
    _payload_hash,
    _StrictModel,
    _utc_now,
)
from .stage5 import (
    EvidenceReportPlan,
    EvidenceScopePlan,
    EvidenceTaskPlan,
    PreparedRequestScope,
    Stage5EvidencePreparer,
)
from .stage5_service import stage5_field_ids

SHADOW_EVIDENCE_PLAN_SCHEMA = "company_profile_shadow_evidence_plan.v1"
SHADOW_EVIDENCE_PLAN_VERSION = "manufacturing_materials_shadow.2026-09-09.1"
SHADOW_PREPARATION_AUDIT_SCHEMA = "company_profile_shadow_preparation_audit.v1"
_PROHIBITED_KEYS = frozenset(
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
_CHAPTER_CONFIG: dict[
    ChapterTask,
    tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]],
] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: (
        ("business_overview_source", "explicit_activity"),
        ("principal_business", "products_and_applications", "business_model"),
        ("主营业务", "主要业务", "主要产品", "经营模式"),
    ),
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: (
        (
            "segment_dimension",
            "operating_revenue",
            "operating_cost",
            "gross_margin_reported",
        ),
        ("segment_information", "revenue_cost_analysis"),
        ("分部信息", "分行业", "分产品", "营业收入", "营业成本"),
    ),
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: (
        (
            "production_capacity",
            "capacity_under_construction",
            "capacity_utilization",
            "production_volume",
            "sales_volume",
            "inventory_volume",
            "processing_volume",
        ),
        (
            "production_sales_inventory",
            "resources_and_reserves",
            "major_projects",
            "coal_operations",
            "coal_resources",
        ),
        ("产销量", "生产量", "销售量", "库存量", "产能", "在建"),
    ),
    ChapterTask.EXTRACT_MATERIAL_INPUTS: (
        ("material_input",),
        ("procurement_and_costs", "cost_composition", "principal_business"),
        ("原材料", "能源", "采购", "成本构成", "铁矿石", "煤炭"),
    ),
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: (
        (
            "counterparty_relationship",
            "customer_concentration",
            "supplier_concentration",
        ),
        ("major_customers_suppliers", "orders", "business_model"),
        ("前五名客户", "前五名供应商", "客户集中度", "供应商", "客户"),
    ),
    ChapterTask.EXTRACT_BUSINESS_REGIME: (
        ("business_regime",),
        ("principal_business", "business_model", "major_projects"),
        ("经营模式", "重大变化", "业务变化", "合并范围", "重组"),
    ),
}
_CHAPTER_MAX_SCOPES = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: 1,
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: 2,
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: 2,
    ChapterTask.EXTRACT_MATERIAL_INPUTS: 1,
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: 2,
    ChapterTask.EXTRACT_BUSINESS_REGIME: 1,
}


class ShadowPlanningFailureCode(str, Enum):
    PDF_ARTIFACT_FAILED = "pdf_artifact_failed"
    PDF_IDENTITY_MISMATCH = "pdf_identity_mismatch"
    CHAPTER_EVIDENCE_NOT_FOUND = "chapter_evidence_not_found"
    PAGE_UNREADABLE = "page_unreadable"
    TABLE_CONTEXT_INCOMPLETE = "table_context_incomplete"
    FIELD_CONTRACT_INVALID = "field_contract_invalid"
    PLAN_INVALID = "plan_invalid"
    PLAN_IDENTITY_MISMATCH = "plan_identity_mismatch"


class ShadowEvidencePlanningError(RuntimeError):
    def __init__(
        self,
        code: ShadowPlanningFailureCode,
        message: str,
        *,
        sample_id: str | None = None,
        chapter_task: ChapterTask | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.sample_id = sample_id
        self.chapter_task = chapter_task


class ShadowEvidenceScopeSelection(_StrictModel):
    chapter_task: ChapterTask
    scope_id: str = Field(min_length=1)
    pages: tuple[int, ...] = Field(min_length=1, max_length=3)
    page_hashes: dict[str, str]
    section_hashes: tuple[str, ...] = Field(min_length=1)
    selector_reasons: tuple[str, ...] = Field(min_length=1)
    quality: Literal["native", "governed_ocr"]

    @model_validator(mode="after")
    def _selection_is_bound_to_scope(self) -> ShadowEvidenceScopeSelection:
        if tuple(sorted(set(self.pages))) != self.pages:
            raise ValueError("shadow Evidence selection pages must be sorted and unique")
        if set(self.page_hashes) != {str(page) for page in self.pages}:
            raise ValueError("shadow Evidence selection page hashes mismatch")
        if any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in self.page_hashes.values()):
            raise ValueError("shadow Evidence selection requires valid page hashes")
        if any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in self.section_hashes):
            raise ValueError("shadow Evidence selection requires valid section hashes")
        return self


class ShadowEvidencePlan(_StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["company_profile_shadow_evidence_plan.v1"] = (
        SHADOW_EVIDENCE_PLAN_SCHEMA
    )
    plan_version: str = Field(min_length=1)
    sample_manifest_revision: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_coordinate_system: Literal["one_based_pdf_physical_page"] = (
        "one_based_pdf_physical_page"
    )
    reports: tuple[EvidenceReportPlan, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    pdf_artifact_hashes: dict[str, str]
    selected_section_hashes: dict[str, tuple[str, ...]]
    scope_selections: dict[str, tuple[ShadowEvidenceScopeSelection, ...]]
    recovery_states: dict[str, str]
    recovered_page_numbers: dict[str, tuple[int, ...]]
    created_at: str = Field(min_length=1)
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _plan_is_frozen(self) -> ShadowEvidencePlan:
        sample_ids = [item.sample_id for item in self.reports]
        if len(sample_ids) != len(set(sample_ids)):
            raise ValueError("shadow Evidence plan contains duplicate reports")
        if set(sample_ids) != set(self.pdf_artifact_hashes):
            raise ValueError("shadow Evidence plan PDF artifact identities mismatch")
        if set(sample_ids) != set(self.selected_section_hashes):
            raise ValueError("shadow Evidence plan section identities mismatch")
        if set(sample_ids) != set(self.scope_selections):
            raise ValueError("shadow Evidence plan scope-selection identities mismatch")
        if set(sample_ids) != set(self.recovery_states):
            raise ValueError("shadow Evidence plan recovery-state identities mismatch")
        if set(sample_ids) != set(self.recovered_page_numbers):
            raise ValueError("shadow Evidence plan recovered-page identities mismatch")
        if any(not values for values in self.selected_section_hashes.values()):
            raise ValueError("shadow Evidence plan requires selected section hashes")
        reports_by_id = {item.sample_id: item for item in self.reports}
        for sample_id, selections in self.scope_selections.items():
            report = reports_by_id[sample_id]
            planned = {
                (task.chapter_task, scope.scope_id): scope.pages
                for task in report.tasks
                for scope in task.request_scopes
            }
            selected = {
                (item.chapter_task, item.scope_id): item.pages for item in selections
            }
            if selected != planned:
                raise ValueError("shadow Evidence scope selections mismatch report plan")
        if self.plan_hash != _payload_hash(self, omit={"plan_hash"}):
            raise ValueError("shadow Evidence plan hash mismatch")
        return self

    def report_by_id(self, sample_id: str) -> EvidenceReportPlan:
        for report in self.reports:
            if report.sample_id == sample_id:
                return report
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_IDENTITY_MISMATCH,
            f"sample is outside the frozen shadow Evidence plan: {sample_id}",
            sample_id=sample_id,
        )


class ShadowPreparationReport(_StrictModel):
    sample_id: str = Field(min_length=1)
    status: Literal["prepared"] = "prepared"
    scope_count: int = Field(ge=6, le=9)
    selected_page_count: int = Field(ge=1)
    evidence_count: int = Field(ge=6)
    field_contract_passed: Literal[True] = True
    pdf_artifact_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    recovery_state: str = Field(min_length=1)
    recovered_page_numbers: tuple[int, ...] = ()


class ShadowEvidencePreparationAudit(_StrictModel):
    schema_version: Literal["company_profile_shadow_preparation_audit.v1"] = (
        SHADOW_PREPARATION_AUDIT_SCHEMA
    )
    audit_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_count: int = Field(ge=SHADOW_REPORT_COUNT, le=SHADOW_REPORT_COUNT)
    planned_report_count: int = Field(ge=SHADOW_REPORT_COUNT, le=SHADOW_REPORT_COUNT)
    total_scope_count: int = Field(ge=SHADOW_REPORT_COUNT * 6)
    total_evidence_count: int = Field(ge=SHADOW_REPORT_COUNT * 6)
    evidence_traceability_rate: Literal[1.0] = 1.0
    recovered_report_count: int = Field(ge=0, le=SHADOW_REPORT_COUNT)
    reports: tuple[ShadowPreparationReport, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _audit_is_frozen(self) -> ShadowEvidencePreparationAudit:
        if len({item.sample_id for item in self.reports}) != SHADOW_REPORT_COUNT:
            raise ValueError("shadow preparation audit report identities mismatch")
        if self.total_scope_count != sum(item.scope_count for item in self.reports):
            raise ValueError("shadow preparation audit scope count mismatch")
        if self.total_evidence_count != sum(item.evidence_count for item in self.reports):
            raise ValueError("shadow preparation audit Evidence count mismatch")
        if self.recovered_report_count != sum(
            bool(item.recovered_page_numbers) for item in self.reports
        ):
            raise ValueError("shadow preparation audit recovery count mismatch")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("shadow preparation audit hash mismatch")
        return self


class ShadowEvidencePlanner:
    """Translate governed PDF sections into existing Stage 5 plan objects."""

    def __init__(
        self,
        *,
        extractor: BusinessProfilePdfArtifactExtractor | None = None,
        selector: BusinessProfileSectionSelector | None = None,
    ) -> None:
        self._uses_custom_extractor = extractor is not None
        self._extractor = extractor or BusinessProfilePdfArtifactExtractor(
            engine_profile="pypdf_native"
        )
        self._selector = selector or BusinessProfileSectionSelector(
            context_pages=1,
            max_pages=6,
        )
        self._catalog = load_disclosure_template_catalog()

    def build(self, manifest: ShadowSampleManifest) -> ShadowEvidencePlan:
        reports: list[EvidenceReportPlan] = []
        artifact_hashes: dict[str, str] = {}
        section_hashes: dict[str, tuple[str, ...]] = {}
        scope_selections: dict[str, tuple[ShadowEvidenceScopeSelection, ...]] = {}
        recovery_states: dict[str, str] = {}
        recovered_page_numbers: dict[str, tuple[int, ...]] = {}
        for report in manifest.reports:
            artifact, recovery_state, recovered_pages = self.load_artifact(report)
            if artifact.status == "parse_failed":
                failure_class = artifact.diagnostics.get("failure_class")
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_ARTIFACT_FAILED,
                    f"PDF artifact failed: {failure_class}",
                    sample_id=report.sample_id,
                )
            if (
                artifact.source_content_hash != report.content_hash
                or artifact.page_count != report.page_count
            ):
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                    "PDF artifact identity does not match the frozen manifest",
                    sample_id=report.sample_id,
                )
            templates = self._catalog.select(
                document_date=report.report.published_at[:10],
                exchange=report.exchange,
                board=report.board,
                document_type="annual_report",
                industry_group=report.industry_group,
            )
            tasks: list[EvidenceTaskPlan] = []
            hashes: list[str] = []
            selections: list[ShadowEvidenceScopeSelection] = []
            for chapter_task in ChapterTask:
                task, selected_hashes, selected_scopes = self._plan_task(
                    report=report,
                    artifact=artifact,
                    templates=templates,
                    chapter_task=chapter_task,
                )
                tasks.append(task)
                hashes.extend(selected_hashes)
                selections.extend(selected_scopes)
            reports.append(
                EvidenceReportPlan(
                    sample_id=report.sample_id,
                    content_hash=report.content_hash,
                    plan_version=SHADOW_EVIDENCE_PLAN_VERSION,
                    tasks=tuple(tasks),
                )
            )
            artifact_hashes[report.sample_id] = artifact.artifact_hash
            section_hashes[report.sample_id] = tuple(hashes)
            scope_selections[report.sample_id] = tuple(selections)
            recovery_states[report.sample_id] = recovery_state
            recovered_page_numbers[report.sample_id] = recovered_pages
        payload = {
            "schema_version": SHADOW_EVIDENCE_PLAN_SCHEMA,
            "plan_version": SHADOW_EVIDENCE_PLAN_VERSION,
            "sample_manifest_revision": manifest.manifest_revision,
            "sample_manifest_hash": manifest.manifest_hash,
            "page_coordinate_system": "one_based_pdf_physical_page",
            "reports": tuple(reports),
            "pdf_artifact_hashes": artifact_hashes,
            "selected_section_hashes": section_hashes,
            "scope_selections": scope_selections,
            "recovery_states": recovery_states,
            "recovered_page_numbers": recovered_page_numbers,
            "created_at": _utc_now(),
            "production_authorization": PRODUCTION_AUTHORIZATION,
        }
        return ShadowEvidencePlan(**payload, plan_hash=_payload_hash(payload))

    def load_artifact(
        self,
        report: Any,
        *,
        expected_artifact_hash: str | None = None,
    ) -> tuple[Any, str, tuple[int, ...]]:
        if self._uses_custom_extractor:
            artifact = self._extractor.extract_file(
                report.local_path,
                source_file_id=report.asset_id,
            )
            return artifact, str(getattr(artifact, "recovery_state", "native_ready")), ()
        document = {
            "archive_path": str(report.local_path),
            "content_hash": report.content_hash,
            "source_file_id": report.asset_id,
        }
        try:
            native_result = ensure_archived_pdf_page_artifact(
                document,
                extractor=self._extractor,
            )
            artifact = native_result["artifact"]
            if expected_artifact_hash:
                frozen = _load_frozen_artifact(
                    report=report,
                    native_artifact_path=Path(str(native_result["artifact_path"])),
                    expected_artifact_hash=expected_artifact_hash,
                )
                if frozen is not None:
                    recovered_pages = tuple(
                        sorted(
                            int(page)
                            for page in frozen.diagnostics.get(
                                "recovered_page_numbers", ()
                            )
                        )
                    )
                    state = (
                        "partial_ocr"
                        if recovered_pages
                        else str(frozen.recovery_state or "native_ready")
                    )
                    return frozen, state, recovered_pages
            result, _outline, recovery = _recover_business_profile_document(
                document,
                {
                    "artifact": artifact,
                    "artifact_hash": artifact.artifact_hash,
                    "status": artifact.status,
                },
            )
        except (OSError, RuntimeError, ValueError) as exc:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.PDF_ARTIFACT_FAILED,
                f"bounded PDF recovery failed: {exc}",
                sample_id=report.sample_id,
            ) from exc
        state = str(recovery.get("recovery_state") or "native_ready")
        resolved_artifact = result["artifact"]
        recovered_pages = tuple(
            sorted(
                int(page)
                for page in resolved_artifact.diagnostics.get(
                    "recovered_page_numbers", ()
                )
            )
        )
        return resolved_artifact, state, recovered_pages

    def _plan_task(
        self,
        *,
        report: Any,
        artifact: Any,
        templates: tuple[ResolvedDisclosureTemplate, ...],
        chapter_task: ChapterTask,
    ) -> tuple[
        EvidenceTaskPlan,
        tuple[str, ...],
        tuple[ShadowEvidenceScopeSelection, ...],
    ]:
        field_ids, allowed_keys, hint_terms = _CHAPTER_CONFIG[chapter_task]
        unknown_fields = sorted(set(field_ids) - stage5_field_ids())
        if unknown_fields:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                f"unknown Stage 5 checklist fields: {unknown_fields}",
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        try:
            selected = self._selector.select(
                artifact=artifact,
                instrument_id=report.report.instrument_id,
                source_document_id=report.asset_id,
                field_family=ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
                templates=templates,
                hint_terms=hint_terms,
                max_pages_override=6,
            )
        except ValueError as exc:
            unreadable = str(getattr(artifact, "recovery_state", "")) in {
                "partial_ocr",
                "section_ocr_required",
                "source_unrecoverable",
                "toc_unresolved",
                "toc_probe_required",
            }
            raise ShadowEvidencePlanningError(
                (
                    ShadowPlanningFailureCode.PAGE_UNREADABLE
                    if unreadable
                    else ShadowPlanningFailureCode.CHAPTER_EVIDENCE_NOT_FOUND
                ),
                str(exc),
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            ) from exc
        unreadable = [
            item.page_number
            for item in selected.sections
            if not item.text.strip()
            or item.quality not in {"native", "governed_ocr"}
        ]
        if unreadable:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.PAGE_UNREADABLE,
                f"selected Evidence pages are unreadable: {sorted(set(unreadable))}",
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        _validate_table_context(
            selected.sections,
            sample_id=report.sample_id,
            chapter_task=chapter_task,
        )
        direct_pages = {
            section.page_number
            for section in selected.sections
            if _section_matches(section, allowed_keys, hint_terms)
        }
        if not direct_pages:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.CHAPTER_EVIDENCE_NOT_FOUND,
                f"no governed Evidence selected for {chapter_task.value}",
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        available = {section.page_number for section in selected.sections}
        bounded = sorted(
            page
            for page in available
            if page in direct_pages
            or page - 1 in direct_pages
            or page + 1 in direct_pages
        )
        scopes: list[EvidenceScopePlan] = []
        scope_selections: list[ShadowEvidenceScopeSelection] = []
        chapter_name = chapter_task.value.removeprefix("extract_")
        ranges = _select_scope_ranges(
            selected.sections,
            direct_pages=direct_pages,
            bounded_pages=bounded,
            maximum_scopes=_CHAPTER_MAX_SCOPES[chapter_task],
        )
        for index, pages in enumerate(ranges, start=1):
            page_set = set(pages)
            sections = [item for item in selected.sections if item.page_number in page_set]
            combined = "\n".join(item.text for item in sections)
            scopes.append(
                EvidenceScopePlan(
                    scope_id=f"{chapter_name}-{index:02d}",
                    field_ids=field_ids,
                    pages=pages,
                    section_titles=tuple(
                        dict.fromkeys(
                            item.section_key
                            for item in sections
                            if item.section_key != "context"
                        )
                    )
                    or (chapter_task.value,),
                    anchor_terms=(_anchor_term(sections, allowed_keys, hint_terms),),
                    required_headers=_matched_headers(sections, templates, combined),
                    required_units=_source_units(combined),
                    required_footnotes=_source_footnotes(combined),
                    continuation_required=len(pages) > 1,
                    printed_page_labels=_printed_labels(artifact, pages),
                    candidate_pages=tuple(page for page in pages if page in direct_pages),
                )
            )
            scope_selections.append(
                ShadowEvidenceScopeSelection(
                    chapter_task=chapter_task,
                    scope_id=f"{chapter_name}-{index:02d}",
                    pages=pages,
                    page_hashes={str(item.page_number): item.page_hash for item in sections},
                    section_hashes=tuple(item.section_hash for item in sections),
                    selector_reasons=tuple(
                        sorted(
                            {
                                reason
                                for item in sections
                                for reason in item.selector_reasons
                            }
                        )
                    ),
                    quality=(
                        "governed_ocr"
                        if any(item.quality == "governed_ocr" for item in sections)
                        else "native"
                    ),
                )
            )
        return EvidenceTaskPlan(
            chapter_task=chapter_task,
            request_scopes=tuple(scopes),
        ), tuple(item.section_hash for item in selected.sections), tuple(scope_selections)


class ShadowEvidencePreparer:
    """Prepare a frozen shadow plan through the existing Stage 5 owner."""

    def __init__(
        self,
        *,
        planner: ShadowEvidencePlanner | None = None,
        stage5_preparer: Stage5EvidencePreparer | None = None,
    ) -> None:
        self._planner = planner or ShadowEvidencePlanner()
        self._stage5_preparer = stage5_preparer or Stage5EvidencePreparer()

    def prepare(
        self,
        *,
        manifest: ShadowSampleManifest,
        plan: ShadowEvidencePlan,
    ) -> dict[str, tuple[PreparedRequestScope, ...]]:
        if (
            plan.sample_manifest_revision != manifest.manifest_revision
            or plan.sample_manifest_hash != manifest.manifest_hash
        ):
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.PLAN_IDENTITY_MISMATCH,
                "shadow Evidence plan does not match the active manifest",
            )
        prepared: dict[str, tuple[PreparedRequestScope, ...]] = {}
        for asset in manifest.reports:
            report_plan = plan.report_by_id(asset.sample_id)
            artifact, recovery_state, recovered_pages = self._planner.load_artifact(
                asset,
                expected_artifact_hash=plan.pdf_artifact_hashes[asset.sample_id],
            )
            if (
                artifact.source_content_hash != asset.content_hash
                or artifact.page_count != asset.page_count
                or artifact.artifact_hash != plan.pdf_artifact_hashes[asset.sample_id]
                or recovery_state != plan.recovery_states[asset.sample_id]
                or recovered_pages != plan.recovered_page_numbers[asset.sample_id]
            ):
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                    "prepared PDF artifact differs from the frozen Evidence plan",
                    sample_id=asset.sample_id,
                )
            artifact_pages = {item.page_number: item for item in artifact.pages}
            for selection in plan.scope_selections[asset.sample_id]:
                for page_number, expected_hash in selection.page_hashes.items():
                    page = artifact_pages.get(int(page_number))
                    if page is None or page.page_artifact_hash != expected_hash:
                        raise ShadowEvidencePlanningError(
                            ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                            "selected page hash differs from the frozen Evidence plan",
                            sample_id=asset.sample_id,
                            chapter_task=selection.chapter_task,
                        )
            unknown = sorted(
                {
                    field_id
                    for task in report_plan.tasks
                    for scope in task.request_scopes
                    for field_id in scope.field_ids
                }
                - stage5_field_ids()
            )
            if unknown:
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                    f"unknown Stage 5 checklist fields: {unknown}",
                    sample_id=asset.sample_id,
                )
            page_results = {
                number: _artifact_page_result(page) for number, page in artifact_pages.items()
            }
            prepared[asset.sample_id] = self._stage5_preparer.prepare_asset_plan(
                asset=asset,
                plan=report_plan,
                plan_version=plan.plan_version,
                page_results=page_results,
            )
        return prepared


def build_shadow_preparation_audit(
    plan: ShadowEvidencePlan,
    *,
    audit_id: str,
    prepared: dict[str, tuple[PreparedRequestScope, ...]],
) -> ShadowEvidencePreparationAudit:
    reports: list[ShadowPreparationReport] = []
    for report in plan.reports:
        selections = plan.scope_selections[report.sample_id]
        scopes = prepared.get(report.sample_id)
        if scopes is None or len(scopes) != len(selections):
            raise ValueError("shadow preparation audit requires every prepared scope")
        evidence_count = sum(len(scope.evidence_bundle) for scope in scopes)
        if any(
            evidence.evidence.report != scope.report
            or evidence.evidence.page not in {page.page for page in scope.page_contexts}
            for scope in scopes
            for evidence in scope.evidence_bundle
        ):
            raise ValueError("shadow prepared Evidence traceability mismatch")
        reports.append(
            ShadowPreparationReport(
                sample_id=report.sample_id,
                scope_count=len(selections),
                selected_page_count=len(
                    {
                        page
                        for selection in selections
                        for page in selection.pages
                    }
                ),
                evidence_count=evidence_count,
                pdf_artifact_hash=plan.pdf_artifact_hashes[report.sample_id],
                recovery_state=plan.recovery_states[report.sample_id],
                recovered_page_numbers=plan.recovered_page_numbers[report.sample_id],
            )
        )
    payload = {
        "schema_version": SHADOW_PREPARATION_AUDIT_SCHEMA,
        "audit_id": audit_id,
        "sample_manifest_hash": plan.sample_manifest_hash,
        "evidence_plan_hash": plan.plan_hash,
        "report_count": len(reports),
        "planned_report_count": len(reports),
        "total_scope_count": sum(item.scope_count for item in reports),
        "total_evidence_count": sum(item.evidence_count for item in reports),
        "evidence_traceability_rate": 1.0,
        "recovered_report_count": sum(
            bool(item.recovered_page_numbers) for item in reports
        ),
        "reports": tuple(reports),
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowEvidencePreparationAudit(
        **payload,
        audit_hash=_payload_hash(payload),
    )


def _artifact_page_result(page: Any) -> SimpleNamespace:
    text = str(page.text or "")
    method = str(page.extraction_method or "none")
    usable = bool(text.strip()) and method in {"native_text", "alternate_native", "ocr"}
    quality = "governed_ocr" if method == "ocr" else "native"
    return SimpleNamespace(
        page_number=page.page_number,
        selected_usable_for_semantic=usable,
        selected_text=text,
        selected_method=method,
        quality_status=quality,
    )


def _load_frozen_artifact(
    *,
    report: Any,
    native_artifact_path: Path,
    expected_artifact_hash: str,
) -> Any | None:
    store = BusinessProfilePdfArtifactStore()
    pattern = f"{report.content_hash}_*.json.gz"
    for candidate in sorted(native_artifact_path.parent.glob(pattern)):
        try:
            payload = store.read(candidate)
            if payload.get("artifact_hash") != expected_artifact_hash:
                continue
            return store.read_artifact(
                candidate,
                source_file_id=report.asset_id,
                source_pdf_path=str(report.local_path),
                expected_content_hash=report.content_hash,
            )
        except (OSError, RuntimeError, ValueError):
            continue
    return None


def write_shadow_evidence_artifact(
    path: str | Path,
    value: ShadowEvidencePlan | ShadowEvidencePreparationAudit,
) -> None:
    destination = Path(path)
    content = json.dumps(
        value.model_dump(mode="json"),
        ensure_ascii=False,
        indent=2,
    ) + "\n"
    if destination.exists():
        if destination.read_text(encoding="utf-8") != content:
            raise RuntimeError(f"immutable shadow Evidence artifact mismatch: {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.part")
    temporary.write_text(content, encoding="utf-8")
    try:
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def load_shadow_evidence_plan(path: str | Path) -> ShadowEvidencePlan:
    source = Path(path).read_text(encoding="utf-8")
    raw = json.loads(source)
    prohibited = _find_keys(raw)
    if prohibited:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow Evidence plan contains prohibited keys: {sorted(prohibited)}",
        )
    try:
        return ShadowEvidencePlan.model_validate_json(source)
    except ValueError as exc:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow Evidence plan is invalid: {exc}",
        ) from exc


def load_shadow_preparation_audit(
    path: str | Path,
) -> ShadowEvidencePreparationAudit:
    try:
        return ShadowEvidencePreparationAudit.model_validate_json(
            Path(path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow preparation audit is invalid: {exc}",
        ) from exc


def _section_matches(
    section: Any,
    allowed_keys: tuple[str, ...],
    hint_terms: tuple[str, ...],
) -> bool:
    if section.section_key in allowed_keys:
        return True
    for reason in section.selector_reasons:
        if reason.startswith("heading_alias:"):
            key = reason.split(":", 2)[1]
            if key in allowed_keys:
                return True
        if reason.startswith("structured_hint:"):
            term = reason.split(":", 1)[1]
            if term in hint_terms:
                return True
    return False


def _validate_table_context(
    sections: Sequence[Any],
    *,
    sample_id: str,
    chapter_task: ChapterTask,
) -> None:
    pages = {item.page_number for item in sections}
    for section in sections:
        normalized = section.text.replace(" ", "")
        missing_previous = "续表" in normalized and section.page_number - 1 not in pages
        requires_next = any(term in normalized for term in ("续下表", "接下页"))
        missing_next = requires_next and section.page_number + 1 not in pages
        if missing_previous or missing_next:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.TABLE_CONTEXT_INCOMPLETE,
                f"selected table continuation context is incomplete at page {section.page_number}",
                sample_id=sample_id,
                chapter_task=chapter_task,
            )


def _continuous_ranges(
    pages: Sequence[int],
    *,
    maximum: int,
) -> tuple[tuple[int, ...], ...]:
    ranges: list[list[int]] = []
    for page in pages:
        if not ranges or page != ranges[-1][-1] + 1 or len(ranges[-1]) >= maximum:
            ranges.append([page])
        else:
            ranges[-1].append(page)
    return tuple(tuple(values) for values in ranges)


def _select_scope_ranges(
    sections: Sequence[Any],
    *,
    direct_pages: set[int],
    bounded_pages: Sequence[int],
    maximum_scopes: int,
) -> tuple[tuple[int, ...], ...]:
    ranges = _continuous_ranges(bounded_pages, maximum=3)
    sections_by_page = {item.page_number: item for item in sections}

    def score(pages: tuple[int, ...]) -> tuple[int, int]:
        reasons = {
            reason
            for page in pages
            for reason in sections_by_page[page].selector_reasons
        }
        value = sum(
            100
            if reason.startswith("table_signature:")
            else 40
            if reason.startswith("structured_hint:")
            else 20
            if reason.startswith("heading_alias:")
            else 1
            for reason in reasons
        )
        value += 10 * sum(page in direct_pages for page in pages)
        return value, -pages[0]

    chosen = sorted(ranges, key=score, reverse=True)[:maximum_scopes]
    return tuple(sorted(chosen, key=lambda pages: pages[0]))


def _anchor_term(
    sections: Sequence[Any],
    allowed_keys: tuple[str, ...],
    hint_terms: tuple[str, ...],
) -> str:
    combined = "\n".join(item.text for item in sections)
    for section in sections:
        for reason in section.selector_reasons:
            if reason.startswith("heading_alias:"):
                _, key, term = reason.split(":", 2)
                if key in allowed_keys and term in combined:
                    return term
            if reason.startswith("structured_hint:"):
                term = reason.split(":", 1)[1]
                if term in hint_terms and term in combined:
                    return term
    for term in hint_terms:
        if term in combined:
            return term
    for line in combined.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:120]
    raise ValueError("selected chapter Evidence contains no anchor text")


def _matched_headers(
    sections: Sequence[Any],
    templates: Sequence[ResolvedDisclosureTemplate],
    combined: str,
) -> tuple[str, ...]:
    signature_ids = {
        reason.split(":", 1)[1]
        for section in sections
        for reason in section.selector_reasons
        if reason.startswith("table_signature:")
    }
    headers: list[str] = []
    for resolved in templates:
        for signature in resolved.template.table_signatures:
            if signature.signature_id not in signature_ids:
                continue
            for header in signature.required_headers:
                if header in combined and header not in headers:
                    headers.append(header)
    return tuple(headers)


def _source_units(text: str) -> tuple[str, ...]:
    matches = re.findall(r"单位\s*[:：]\s*([^\s，。；|]{1,16})", text)
    return tuple(dict.fromkeys(item.strip() for item in matches if item.strip()))


def _source_footnotes(text: str) -> tuple[str, ...]:
    matches = re.findall(r"(?:^|\n)(注[:：][^\n]{1,100})", text)
    return tuple(dict.fromkeys(item.strip() for item in matches if item.strip()))


def _printed_labels(artifact: Any, pages: Sequence[int]) -> dict[str, str]:
    wanted = set(pages)
    return {
        str(page.page_number): str(page.printed_page_label)
        for page in artifact.pages
        if page.page_number in wanted and page.printed_page_label
    }


def _find_keys(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _PROHIBITED_KEYS:
                found.add(key)
            found.update(_find_keys(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_find_keys(item))
    return found
