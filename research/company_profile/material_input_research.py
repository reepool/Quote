"""Single-chapter research scope for the three stage-4 material-input reports.

The historical stage-five plan still admits six chapters. This module does not
call that plan or the twenty-report shadow batch. It prepares only
``extract_material_inputs`` and writes a research-isolated bundle.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .commodity_exposure import project_commodity_exposures
from .contracts import (
    ChecklistItem,
    PackageManifest,
    PreparedEvidence,
    SemanticTaskRequest,
)
from .core_evidence_selection import explicit_material_input_names
from .models import (
    Activity,
    ActivityAction,
    AssertionClass,
    ChapterTask,
    CoverageStatus,
    ObjectType,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
)
from .stage5 import (
    EvidenceScopePlan,
    Stage5EvidencePreparer,
    Stage5ReportAsset,
)
from .stage5_bundle import Stage5RunBundleStore
from .workflow import CompanyProfileSemanticService

MATERIAL_INPUT_RESEARCH_SCHEMA = "company_profile_material_input_research.v1"
MATERIAL_INPUT_RESEARCH_PLAN_VERSION = (
    "manufacturing_materials_stage4_material_inputs.2026-09-26.1"
)
MATERIAL_INPUT_CHAPTER = ChapterTask.EXTRACT_MATERIAL_INPUTS
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_PRODUCT_MENTION = re.compile(r"产品主要包括|主要产品包括|主要产品为")


class MaterialInputResearchError(RuntimeError):
    """The single-chapter research scope could not be delivered for review."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class MaterialInputResearchFact(_StrictModel):
    sample_id: str = Field(min_length=1)
    object_name: str = Field(min_length=1)
    relation_type: Literal["material_input"] = "material_input"
    role: Literal["raw_material_input"] = "raw_material_input"
    page: int = Field(ge=1)
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    quantity: None = None
    mapping_status: Literal["mapped", "pending", "ambiguous"]
    commodity_id: str | None = None
    companion_sales_role: bool = False


class MaterialInputScopeOutcome(_StrictModel):
    sample_id: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    outcome: Literal["observed", "legal_empty", "unclear", "extraction_failure"]
    names: tuple[str, ...] = ()


class MaterialInputReportBindingRecord(_StrictModel):
    sample_id: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    dossier_path: str
    dossier_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_id: str
    report_id: str
    document_version: str


class MaterialInputResearchBundle(_StrictModel):
    schema_version: Literal["company_profile_material_input_research.v1"] = (
        MATERIAL_INPUT_RESEARCH_SCHEMA
    )
    run_id: str = Field(min_length=1)
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    plan_version: str = MATERIAL_INPUT_RESEARCH_PLAN_VERSION
    reports: tuple[MaterialInputReportBindingRecord, ...] = Field(
        min_length=3, max_length=3
    )
    scope_outcomes: tuple[MaterialInputScopeOutcome, ...] = Field(min_length=1)
    facts: tuple[MaterialInputResearchFact, ...] = Field(min_length=1)


@dataclass(frozen=True)
class _ScopeBinding:
    scope_id: str
    kind: Literal[
        "named_input",
        "direct_material_cost",
        "inventory_amount",
        "outsourced_processing",
        "product_overlap",
    ]
    page: int
    section_title: str
    anchor_terms: tuple[str, ...]


@dataclass(frozen=True)
class MaterialInputReportBinding:
    sample_id: str
    company_name: str
    exchange: Literal["SSE", "SZSE", "BSE"]
    instrument_id: str
    report_id: str
    document_version: str
    published_at: str
    content_hash: str
    relative_pdf_path: str
    content_length: int
    page_count: int
    relative_dossier_path: str
    scopes: tuple[_ScopeBinding, ...]


def material_input_research_bindings() -> tuple[MaterialInputReportBinding, ...]:
    """Return the three dossier reports and their material-input pages.

    Pages and anchors locate evidence. They do not embed expected material names.
    """

    return (
        MaterialInputReportBinding(
            sample_id="manufacturing-materials-300750-2025",
            company_name="宁德时代",
            exchange="SZSE",
            instrument_id="300750.SZ",
            report_id="asset_3b09f6c831975c7177b6bb3287cab781",
            document_version="ver_09c0e677ec8192dc4fc12cb620069f29",
            published_at="2026-03-09T16:00:00+00:00",
            content_hash=(
                "c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/c1/"
                "c15272977147dee7e6935a38ea0e4fd6855370aabb106f54cfe20f7cf6048ec9.pdf"
            ),
            content_length=2043710,
            page_count=232,
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-minimum-slice/"
                "dossiers/300750-sz-2025.md"
            ),
            scopes=(
                _ScopeBinding(
                    "300750-named-input",
                    "named_input",
                    40,
                    "原材料价格波动及供应风险",
                    ("原材料价格波动",),
                ),
                _ScopeBinding(
                    "300750-direct-cost",
                    "direct_material_cost",
                    27,
                    "直接材料成本",
                    ("直接材料",),
                ),
                _ScopeBinding(
                    "300750-product-overlap",
                    "product_overlap",
                    15,
                    "电池材料产品",
                    ("电池材料产品",),
                ),
            ),
        ),
        MaterialInputReportBinding(
            sample_id="manufacturing-materials-603659-2025",
            company_name="璞泰来",
            exchange="SSE",
            instrument_id="603659.SH",
            report_id="asset_50c70429093f66b34fc57ad8f896fcee",
            document_version="ver_c867a6a692048e88fd9cb80473fbf908",
            published_at="2026-03-05T16:00:00+00:00",
            content_hash=(
                "4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/4e/"
                "4e81f5539046ba1eee733100f38442a4abd5037afe3881115ba7f48678fa35b6.pdf"
            ),
            content_length=1740667,
            page_count=203,
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-minimum-slice/"
                "dossiers/603659-sh-2025.md"
            ),
            scopes=(
                _ScopeBinding(
                    "603659-named-input",
                    "named_input",
                    33,
                    "原材料价格上涨的风险",
                    ("原材料价格上涨",),
                ),
                _ScopeBinding(
                    "603659-direct-cost",
                    "direct_material_cost",
                    20,
                    "直接材料成本",
                    ("直接材料",),
                ),
                _ScopeBinding(
                    "603659-inventory",
                    "inventory_amount",
                    125,
                    "原材料存货",
                    ("原材料",),
                ),
            ),
        ),
        MaterialInputReportBinding(
            sample_id="manufacturing-materials-920015-2025",
            company_name="锦华新材",
            exchange="BSE",
            instrument_id="920015.BJ",
            report_id="asset_b87f1d1a48e662dae376c540cd021f69",
            document_version="ver_cfdbd2d058af825b1fc39f494d7a9bd3",
            published_at="2026-04-22T16:00:00+00:00",
            content_hash=(
                "4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/4d/"
                "4d2c1612f6f62a9024b8947d7a01b70c40f8f347c2975fa1a05b908d0770695a.pdf"
            ),
            content_length=1845726,
            page_count=143,
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-minimum-slice/"
                "dossiers/920015-bj-2025.md"
            ),
            scopes=(
                _ScopeBinding(
                    "920015-named-input",
                    "named_input",
                    26,
                    "原材料价格上涨风险",
                    ("主要原材料为",),
                ),
                _ScopeBinding(
                    "920015-outsourced",
                    "outsourced_processing",
                    12,
                    "委托外部厂商加工",
                    ("委托外部厂商加工",),
                ),
                _ScopeBinding(
                    "920015-inventory",
                    "inventory_amount",
                    108,
                    "原材料存货",
                    ("原材料",),
                ),
            ),
        ),
    )


def classify_material_scope(
    kind: str, names: tuple[str, ...]
) -> Literal["observed", "legal_empty", "unclear"]:
    """Separate an observed input from a refusal that must not become a fact."""

    if kind == "named_input":
        return "observed" if names else "unclear"
    if names:
        return "unclear"
    if kind in {
        "direct_material_cost",
        "inventory_amount",
        "outsourced_processing",
        "product_overlap",
    }:
        return "legal_empty"
    raise ValueError(f"unknown material scope kind: {kind}")


def commit_material_input_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    catalog: Any | None = None,
    run_id: str = "stage4-material-inputs",
    preparer: Stage5EvidencePreparer | None = None,
) -> Path:
    """Prepare the three reports and commit one review-only bundle.

    ``output_root`` must be an isolated research directory. The stage-five
    store rejects the repository ``data`` and ``config`` trees.
    """

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    bundle = build_material_input_research_bundle(
        repository_root=root,
        catalog=catalog,
        run_id=run_id,
        preparer=preparer,
    )
    destination = store.output_root / f"material-input-{bundle.run_id}"
    if destination.exists():
        raise FileExistsError(
            f"material-input research run already exists: {bundle.run_id}"
        )
    temporary = store.output_root / f".stage5-tmp-{bundle.run_id}-{uuid.uuid4().hex}"
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        payload = bundle.model_dump(mode="json")
        encoded = json.dumps(payload, ensure_ascii=False, indent=2)
        (temporary / "result.json").write_text(encoded + "\n", encoding="utf-8")
        os.replace(temporary, destination)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    return destination


def build_material_input_research_bundle(
    *,
    repository_root: Path,
    catalog: Any | None,
    run_id: str,
    preparer: Stage5EvidencePreparer | None,
) -> MaterialInputResearchBundle:
    active_preparer = preparer or Stage5EvidencePreparer()
    outcomes: list[MaterialInputScopeOutcome] = []
    facts: list[MaterialInputResearchFact] = []
    report_records: list[MaterialInputReportBindingRecord] = []
    for binding in material_input_research_bindings():
        prepared = active_preparer.prepare_single_chapter(
            asset=_asset(binding, repository_root),
            chapter_task=MATERIAL_INPUT_CHAPTER,
            scopes=tuple(_scope_plan(item) for item in binding.scopes),
            plan_version=MATERIAL_INPUT_RESEARCH_PLAN_VERSION,
        )
        if any(item.chapter_task is not MATERIAL_INPUT_CHAPTER for item in prepared):
            raise MaterialInputResearchError("research scope prepared a second chapter")
        by_scope = {item.scope_id: item for item in prepared}
        scope_names: dict[str, tuple[str, ...]] = {}
        for spec in binding.scopes:
            text = "\n".join(
                page.text for page in by_scope[spec.scope_id].page_contexts
            )
            names = explicit_material_input_names(text)
            scope_names[spec.scope_id] = names
            outcome = classify_material_scope(spec.kind, names)
            outcomes.append(
                MaterialInputScopeOutcome(
                    sample_id=binding.sample_id,
                    scope_id=spec.scope_id,
                    kind=spec.kind,
                    outcome=outcome,
                    names=names,
                )
            )
        named = next(item for item in binding.scopes if item.kind == "named_input")
        if (
            classify_material_scope(named.kind, scope_names[named.scope_id])
            != "observed"
        ):
            raise MaterialInputResearchError(
                f"{binding.sample_id} named-input evidence did not uniquely bind"
            )
        sales_names = _sales_names(binding, by_scope, scope_names[named.scope_id])
        product_evidence = None
        if sales_names:
            product = next(
                item for item in binding.scopes if item.kind == "product_overlap"
            )
            product_evidence = by_scope[product.scope_id].evidence_bundle[0].evidence
        accepted = _accept_report(
            binding,
            by_scope[named.scope_id],
            scope_names[named.scope_id],
            sales_names,
            product_evidence,
        )
        exposures = {
            item.source_native_name: item
            for item in project_commodity_exposures(
                tuple(
                    record
                    for record in accepted
                    if getattr(record, "relation_type", None)
                    == RelationshipType.MATERIAL_INPUT
                ),
                catalog=catalog,
            )
        }
        kept_sales_names = {
            record.object_name for record in accepted if isinstance(record, Activity)
        }
        for record in accepted:
            if not isinstance(record, Relationship):
                continue
            exposure = exposures[record.object_name]
            if exposure.role != "raw_material_input":
                raise MaterialInputResearchError(
                    f"{record.object_name} lost the material-input role"
                )
            facts.append(
                MaterialInputResearchFact(
                    sample_id=binding.sample_id,
                    object_name=record.object_name,
                    page=record.evidence[0].page,
                    mapping_status=exposure.mapping_status,
                    commodity_id=exposure.commodity_id,
                    companion_sales_role=record.object_name in kept_sales_names,
                )
            )
        dossier = repository_root / binding.relative_dossier_path
        report_records.append(
            MaterialInputReportBindingRecord(
                sample_id=binding.sample_id,
                content_hash=binding.content_hash,
                dossier_path=binding.relative_dossier_path,
                dossier_sha256=hashlib.sha256(dossier.read_bytes()).hexdigest(),
                instrument_id=binding.instrument_id,
                report_id=binding.report_id,
                document_version=binding.document_version,
            )
        )
    return MaterialInputResearchBundle(
        run_id=run_id,
        reports=tuple(report_records),
        scope_outcomes=tuple(outcomes),
        facts=tuple(facts),
    )


def _sales_names(
    binding: MaterialInputReportBinding,
    by_scope: dict[str, Any],
    input_names: tuple[str, ...],
) -> tuple[str, ...]:
    found: list[str] = []
    inputs = set(input_names)
    for spec in binding.scopes:
        if spec.kind != "product_overlap":
            continue
        text = re.sub(
            r"\s+",
            "",
            "\n".join(page.text for page in by_scope[spec.scope_id].page_contexts),
        )
        for sentence in re.split(r"[。；;]", text):
            if _PRODUCT_MENTION.search(sentence) is None:
                continue
            for name in input_names:
                if name in sentence and name in inputs and name not in found:
                    found.append(name)
    return tuple(found)


def _accept_report(binding, named_scope, names, sales_names, product_evidence):
    report = _identity(binding)
    evidence_by_page = {
        item.evidence.page: item.evidence for item in named_scope.evidence_bundle
    }
    evidence = evidence_by_page[named_scope.page_contexts[0].page]
    relationships = tuple(
        _relationship(report, evidence, name, binding.sample_id) for name in names
    )
    activities = tuple(
        _sales_activity(report, product_evidence, name, binding.sample_id)
        for name in sales_names
    )
    checklist = [
        ChecklistItem(
            field_id="material_input",
            object_type=ObjectType.RELATIONSHIP,
            chapter_task=MATERIAL_INPUT_CHAPTER,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=(
                CoverageStatus.OBSERVED,
                CoverageStatus.NOT_DISCLOSED,
                CoverageStatus.NOT_APPLICABLE,
                CoverageStatus.UNCLEAR,
                CoverageStatus.EXTRACTION_FAILED,
            ),
        )
    ]
    allowed_actions: tuple[ActivityAction, ...] = ()
    if activities:
        checklist.append(
            ChecklistItem(
                field_id="explicit_activity",
                object_type=ObjectType.ACTIVITY,
                chapter_task=MATERIAL_INPUT_CHAPTER,
                requirement_level=RequirementLevel.CONDITIONAL,
                allowed_coverage_statuses=(
                    CoverageStatus.OBSERVED,
                    CoverageStatus.NOT_APPLICABLE,
                    CoverageStatus.UNCLEAR,
                ),
                allowed_actions=(ActivityAction.SELLS,),
            )
        )
        allowed_actions = (ActivityAction.SELLS,)
    prepared = tuple(
        PreparedEvidence(
            evidence=item.evidence,
            field_id="material_input",
            context_complete=item.context_complete,
            headers_complete=item.headers_complete,
            unit_context_complete=item.unit_context_complete,
            footnotes_complete=item.footnotes_complete,
            continuation_complete=item.continuation_complete,
            source_readable=item.source_readable,
        )
        for item in named_scope.evidence_bundle
    )
    if activities:
        prepared = prepared + (
            PreparedEvidence(
                evidence=product_evidence,
                field_id="explicit_activity",
                context_complete=True,
                source_readable=True,
            ),
        )
    request = SemanticTaskRequest(
        request_id=f"{binding.sample_id}:extract_material_inputs",
        report=report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version=MATERIAL_INPUT_RESEARCH_PLAN_VERSION,
            report=report,
            checklist=tuple(checklist),
        ),
        chapter_task=MATERIAL_INPUT_CHAPTER,
        evidence_bundle=prepared,
        allowed_object_types=(
            (ObjectType.RELATIONSHIP, ObjectType.ACTIVITY)
            if activities
            else (ObjectType.RELATIONSHIP,)
        ),
        allowed_actions=allowed_actions,
        prohibited_inferences=("commodity_direction", "complete_value_chain"),
        deterministic_candidates=relationships + activities,
        unresolved_field_ids=(),
    )
    result = CompanyProfileSemanticService().run_task(request, provider=None)
    if result.provider_calls:
        raise MaterialInputResearchError("material-input research called a provider")
    if not result.task_complete:
        raise MaterialInputResearchError(
            f"{binding.sample_id} did not pass provider-free verify"
        )
    refused = [
        item.status.value
        for item in result.dispositions
        if item.status.value != "accepted_for_review"
    ]
    if refused:
        raise MaterialInputResearchError(
            f"{binding.sample_id} disposition left accepted_for_review: {refused}"
        )
    accepted_ids = {
        item.target_id
        for item in result.dispositions
        if item.status.value == "accepted_for_review"
    }
    return tuple(
        record for record in result.records if record.record_id in accepted_ids
    )


def _relationship(report, evidence, name: str, sample_id: str) -> Relationship:
    return Relationship(
        record_id=f"{sample_id}:material:{name}",
        field_id="material_input",
        chapter_task=MATERIAL_INPUT_CHAPTER,
        report=report,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP,
        subject_basis=SubjectBasis.DIRECT_SOURCE_WORDING,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name=name),
        relation_type=RelationshipType.MATERIAL_INPUT,
        object_name=name,
    )


def _sales_activity(report, evidence, name: str, sample_id: str) -> Activity:
    return Activity(
        record_id=f"{sample_id}:sales:{name}",
        field_id="explicit_activity",
        chapter_task=MATERIAL_INPUT_CHAPTER,
        report=report,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP,
        subject_basis=SubjectBasis.DIRECT_SOURCE_WORDING,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name=name),
        action=ActivityAction.SELLS,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name=name,
        source_verb="产品包括",
    )


def _scope_plan(spec: _ScopeBinding) -> EvidenceScopePlan:
    return EvidenceScopePlan(
        scope_id=spec.scope_id,
        field_ids=("material_input",),
        pages=(spec.page,),
        section_titles=(spec.section_title,),
        anchor_terms=spec.anchor_terms,
    )


def _asset(
    binding: MaterialInputReportBinding, repository_root: Path
) -> Stage5ReportAsset:
    return Stage5ReportAsset(
        sample_id=binding.sample_id,
        company_name=binding.company_name,
        exchange=binding.exchange,
        report=_identity(binding),
        content_hash=binding.content_hash,
        local_path=repository_root / binding.relative_pdf_path,
        content_length=binding.content_length,
        page_count=binding.page_count,
        regime_type="stable",
        regime_effective_period="2025",
    )


def _identity(binding: MaterialInputReportBinding) -> ReportIdentity:
    return ReportIdentity(
        instrument_id=binding.instrument_id,
        report_id=binding.report_id,
        document_version=binding.document_version,
        report_period="2025-12-31",
        published_at=binding.published_at,
        document_type="annual_report",
    )
