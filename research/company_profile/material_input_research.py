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

from pydantic import BaseModel, ConfigDict, Field, model_validator

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
    EvidencePreparationError,
    EvidenceScopePlan,
    PreparationFailureCode,
    Stage5EvidencePreparer,
    Stage5ReportAsset,
)
from .stage5_bundle import Stage5RunBundleStore
from .workflow import CompanyProfileSemanticService

MATERIAL_INPUT_RESEARCH_SCHEMA = "company_profile_material_input_research.v2"
MATERIAL_INPUT_RESEARCH_PLAN_VERSION = (
    "manufacturing_materials_stage4_material_inputs.2026-09-26.1"
)
MATERIAL_INPUT_CHAPTER = ChapterTask.EXTRACT_MATERIAL_INPUTS
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_PRODUCT_MENTION = re.compile(r"产品主要包括|主要产品包括|主要产品为")
_REPLAY_INSTRUMENTS = ("300750.SZ", "603659.SH", "920015.BJ")
_ENQUEUE_SCHEMA = "company_profile_material_input_research_enqueue.v1"
_RUN_SCHEMA = "company_profile_material_input_research_run.v1"


class MaterialInputResearchError(RuntimeError):
    """The single-chapter research scope could not be delivered for review."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ResearchReportIdentity(_StrictModel):
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    report_period: str = Field(min_length=1)
    published_at: str = Field(min_length=1)
    document_type: Literal["annual_report"] = "annual_report"


class ResearchEvidenceRef(_StrictModel):
    """One Evidence binding that can be read without the report-level record."""

    binding_status: Literal["bound", "unbound"]
    evidence_id: str | None = None
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    page: int = Field(ge=1)
    section_title: str | None = None
    bounded_quote: str | None = None
    page_text_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _bound_evidence_is_complete(self) -> ResearchEvidenceRef:
        if self.binding_status == "bound":
            if (
                not self.evidence_id
                or not self.section_title
                or not self.bounded_quote
                or not self.page_text_hash
            ):
                raise ValueError(
                    "bound evidence requires an id, section, quote, and page text hash"
                )
            return self
        if self.evidence_id or self.bounded_quote or self.page_text_hash:
            raise ValueError(
                "unbound evidence must not imitate a prepared Evidence object"
            )
        if not self.section_title:
            raise ValueError("unbound evidence still cites the planned section")
        return self


class MaterialInputResearchFact(_StrictModel):
    sample_id: str = Field(min_length=1)
    record_id: str = Field(min_length=1)
    field_id: Literal["material_input"] = "material_input"
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    relation_type: Literal["material_input"] = "material_input"
    role: Literal["raw_material_input"] = "raw_material_input"
    source_native_name: str = Field(min_length=1)
    object_name: str = Field(min_length=1)
    report: ResearchReportIdentity
    reported_period: str = Field(min_length=1)
    period_type: Literal["duration"] = "duration"
    subject_scope: str = Field(min_length=1)
    subject_basis: str = Field(min_length=1)
    assertion_class: Literal["reported_fact"] = "reported_fact"
    evidence: tuple[ResearchEvidenceRef, ...] = Field(min_length=1)
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    quantity: None = None
    mapping_status: Literal["mapped", "pending", "ambiguous"]
    commodity_id: str | None = None

    @model_validator(mode="after")
    def _fact_matches_its_evidence(self) -> MaterialInputResearchFact:
        if self.source_native_name != self.object_name:
            raise ValueError("source-native name and object name must stay together")
        if self.reported_period != self.report.report_period:
            raise ValueError("fact period must match its report identity")
        if any(item.binding_status != "bound" for item in self.evidence):
            raise ValueError("an accepted input fact requires bound evidence")
        if any(item.report_id != self.report.report_id for item in self.evidence):
            raise ValueError("fact evidence must belong to the same report")
        return self


class MaterialInputSalesRole(_StrictModel):
    sample_id: str = Field(min_length=1)
    record_id: str = Field(min_length=1)
    field_id: Literal["explicit_activity"] = "explicit_activity"
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    role: Literal["product_sales"] = "product_sales"
    action: Literal["sells"] = "sells"
    source_native_name: str = Field(min_length=1)
    object_name: str = Field(min_length=1)
    report: ResearchReportIdentity
    reported_period: str = Field(min_length=1)
    period_type: Literal["duration"] = "duration"
    subject_scope: str = Field(min_length=1)
    subject_basis: str = Field(min_length=1)
    actor_basis: str = Field(min_length=1)
    activity_actor: str = Field(min_length=1)
    source_actor: str = Field(min_length=1)
    source_verb: str = Field(min_length=1)
    assertion_class: Literal["reported_fact"] = "reported_fact"
    evidence: tuple[ResearchEvidenceRef, ...] = Field(min_length=1)
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    mapping_status: Literal["mapped", "pending", "ambiguous"]
    commodity_id: str | None = None

    @model_validator(mode="after")
    def _sales_role_has_its_own_evidence(self) -> MaterialInputSalesRole:
        if self.source_native_name != self.object_name:
            raise ValueError("sales role must keep the source-native name")
        if self.activity_actor != self.source_actor:
            raise ValueError("sales actor must match the source actor")
        if self.reported_period != self.report.report_period:
            raise ValueError("sales period must match its report identity")
        if any(item.binding_status != "bound" for item in self.evidence):
            raise ValueError("a sales role requires its own bound evidence")
        return self


class MaterialInputScopeOutcome(_StrictModel):
    sample_id: str = Field(min_length=1)
    scope_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    outcome: Literal["observed", "legal_empty", "unclear", "extraction_failure"]
    names: tuple[str, ...] = ()
    reason: str = Field(min_length=1)
    failure_code: str | None = None
    evidence: tuple[ResearchEvidenceRef, ...] = ()

    @model_validator(mode="after")
    def _negative_outcomes_cite_evidence(self) -> MaterialInputScopeOutcome:
        if self.outcome == "extraction_failure":
            if not self.failure_code or self.names:
                raise ValueError(
                    "extraction failure needs a code and no material names"
                )
            if not self.evidence or any(
                item.binding_status != "unbound" for item in self.evidence
            ):
                raise ValueError("extraction failure must cite the unbound page")
            return self
        if not self.evidence or any(
            item.binding_status != "bound" for item in self.evidence
        ):
            raise ValueError("a prepared outcome must cite bound evidence")
        if self.outcome == "legal_empty" and self.names:
            raise ValueError("legal empty must not carry named inputs")
        if self.outcome == "observed" and not self.names:
            raise ValueError("an observed input scope must name the materials")
        return self


class MaterialInputReportBindingRecord(_StrictModel):
    sample_id: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    dossier_path: str
    dossier_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_id: str
    report_id: str
    document_version: str


class MaterialInputResearchBundle(_StrictModel):
    schema_version: Literal["company_profile_material_input_research.v2"] = (
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
    facts: tuple[MaterialInputResearchFact, ...] = ()
    sales_roles: tuple[MaterialInputSalesRole, ...] = ()


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


_MATERIAL_MENTION = re.compile(r"主要原材料|原材料包括|原材料为")
_EXPRESS_OMISSION = re.compile(r"未披露|不适用|无主要原材料|不涉及")
_AMBIGUOUS_SUBJECT = re.compile(r"客户|供应商|下游|销售|出售")


def classify_material_scope(
    kind: str,
    names: tuple[str, ...],
    *,
    text: str = "",
) -> Literal["observed", "legal_empty", "unclear"]:
    """Separate an observed input from legal empty and unclear binding.

    A named-input page with no names is legal empty when the report does not
    state a company input. It is unclear when the cited sentence mentions a
    material list but does not uniquely bind that list to the company.
    """

    if kind == "named_input" and names:
        return "observed"
    if names:
        return "unclear"
    if kind == "named_input":
        compact = re.sub(r"\s+", "", text)
        if _EXPRESS_OMISSION.search(compact):
            return "legal_empty"
        if _MATERIAL_MENTION.search(compact) or _AMBIGUOUS_SUBJECT.search(compact):
            return "unclear"
        return "legal_empty"
    if kind in {
        "direct_material_cost",
        "inventory_amount",
        "outsourced_processing",
        "product_overlap",
    }:
        return "legal_empty"
    raise ValueError(f"unknown material scope kind: {kind}")


def extraction_failure_outcome(
    *,
    sample_id: str,
    scope_id: str,
    instrument_id: str,
    report_id: str,
    document_version: str,
    page: int,
    section_title: str,
    code: str,
    message: str,
) -> MaterialInputScopeOutcome:
    """Record an unbound page without turning it into a material fact."""

    return MaterialInputScopeOutcome(
        sample_id=sample_id,
        scope_id=scope_id,
        kind="named_input",
        outcome="extraction_failure",
        names=(),
        reason=message,
        failure_code=code,
        evidence=(
            ResearchEvidenceRef(
                binding_status="unbound",
                chapter_task="extract_material_inputs",
                instrument_id=instrument_id,
                report_id=report_id,
                document_version=document_version,
                page=page,
                section_title=section_title,
            ),
        ),
    )


_SCOPE_PREPARATION_FAILURES = frozenset(
    {
        PreparationFailureCode.PAGE_UNREADABLE,
        PreparationFailureCode.CONTEXT_INCOMPLETE,
        PreparationFailureCode.HEADER_MISSING,
        PreparationFailureCode.UNIT_MISSING,
        PreparationFailureCode.FOOTNOTE_MISSING,
        PreparationFailureCode.CONTINUATION_INCOMPLETE,
    }
)


class MaterialInputEnqueueScope(_StrictModel):
    scope_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    page: int = Field(ge=1)
    section_title: str = Field(min_length=1)
    anchor_terms: tuple[str, ...] = Field(min_length=1)
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"


class MaterialInputEnqueueReport(_StrictModel):
    sample_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    report_period: Literal["2025-12-31"] = "2025-12-31"
    published_at: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    dossier_path: str = Field(min_length=1)
    dossier_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_version: str = MATERIAL_INPUT_RESEARCH_PLAN_VERSION
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    scopes: tuple[MaterialInputEnqueueScope, ...] = Field(min_length=1)


class MaterialInputEnqueueSnapshot(_StrictModel):
    schema_version: Literal["company_profile_material_input_research_enqueue.v1"] = (
        _ENQUEUE_SCHEMA
    )
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    production_authorization: Literal["not_authorized"] = "not_authorized"
    plan_version: str = MATERIAL_INPUT_RESEARCH_PLAN_VERSION
    reports: tuple[MaterialInputEnqueueReport, ...] = Field(min_length=3, max_length=3)


class MaterialInputRunSnapshot(_StrictModel):
    schema_version: Literal["company_profile_material_input_research_run.v1"] = (
        _RUN_SCHEMA
    )
    chapter_task: Literal["extract_material_inputs"] = "extract_material_inputs"
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    enqueue_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bundle_dirname: str = Field(min_length=1)
    run_id: str = Field(min_length=1)


def freeze_material_input_research_enqueue(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
) -> Path:
    """Record the three-report binding before any research run starts."""

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    destination = store.output_root / "enqueue.json"
    if destination.exists():
        raise FileExistsError(
            f"material-input enqueue snapshot already exists: {destination}"
        )
    snapshot = _enqueue_snapshot(root)
    _write_json_atomic(
        store.output_root, destination.name, snapshot.model_dump(mode="json")
    )
    return destination


def replay_material_input_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    catalog: Any | None = None,
    run_id: str = "stage4-material-inputs-20260926",
    preparer: Stage5EvidencePreparer | None = None,
) -> Path:
    """Freeze the approved binding, then run only those three material-input reports.

    The run snapshot records the isolated bundle. It does not record recall,
    accuracy, critical errors, or expansion gates.
    """

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    enqueue_path = freeze_material_input_research_enqueue(
        output_root, repository_root=root
    )
    bundle_dir = commit_material_input_research(
        output_root,
        repository_root=root,
        catalog=catalog,
        run_id=run_id,
        preparer=preparer,
    )
    run = MaterialInputRunSnapshot(
        enqueue_sha256=hashlib.sha256(enqueue_path.read_bytes()).hexdigest(),
        bundle_dirname=bundle_dir.name,
        run_id=run_id,
    )
    run_path = enqueue_path.parent / "run.json"
    if run_path.exists():
        raise FileExistsError(f"material-input run snapshot already exists: {run_path}")
    _write_json_atomic(enqueue_path.parent, run_path.name, run.model_dump(mode="json"))
    return run_path


def _enqueue_snapshot(repository_root: Path) -> MaterialInputEnqueueSnapshot:
    bindings = material_input_research_bindings()
    instruments = tuple(item.instrument_id for item in bindings)
    if instruments != _REPLAY_INSTRUMENTS:
        raise MaterialInputResearchError(
            "material-input replay only admits the three approved 2025 reports"
        )
    reports: list[MaterialInputEnqueueReport] = []
    for binding in bindings:
        pdf = repository_root / binding.relative_pdf_path
        dossier = repository_root / binding.relative_dossier_path
        content_hash = hashlib.sha256(pdf.read_bytes()).hexdigest()
        if content_hash != binding.content_hash:
            raise MaterialInputResearchError(
                f"{binding.instrument_id} PDF hash does not match the frozen binding"
            )
        reports.append(
            MaterialInputEnqueueReport(
                sample_id=binding.sample_id,
                instrument_id=binding.instrument_id,
                report_id=binding.report_id,
                document_version=binding.document_version,
                published_at=binding.published_at,
                content_hash=content_hash,
                dossier_path=binding.relative_dossier_path,
                dossier_sha256=hashlib.sha256(dossier.read_bytes()).hexdigest(),
                scopes=tuple(
                    MaterialInputEnqueueScope(
                        scope_id=item.scope_id,
                        kind=item.kind,
                        page=item.page,
                        section_title=item.section_title,
                        anchor_terms=item.anchor_terms,
                    )
                    for item in binding.scopes
                ),
            )
        )
    return MaterialInputEnqueueSnapshot(reports=tuple(reports))


def _write_json_atomic(directory: Path, name: str, payload: dict[str, Any]) -> None:
    temporary = directory / f".stage5-tmp-{name}-{uuid.uuid4().hex}"
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, directory / name)


def commit_material_input_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    catalog: Any | None = None,
    run_id: str = "stage4-material-inputs",
    preparer: Stage5EvidencePreparer | None = None,
    bindings: tuple[MaterialInputReportBinding, ...] | None = None,
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
        bindings=bindings,
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
    bindings: tuple[MaterialInputReportBinding, ...] | None = None,
) -> MaterialInputResearchBundle:
    selected = material_input_research_bindings() if bindings is None else bindings
    if len(selected) != 3 or len({item.sample_id for item in selected}) != 3:
        raise MaterialInputResearchError(
            "the research slice prepares the three dossier reports"
        )
    active_preparer = preparer or Stage5EvidencePreparer()
    outcomes: list[MaterialInputScopeOutcome] = []
    facts: list[MaterialInputResearchFact] = []
    sales_roles: list[MaterialInputSalesRole] = []
    report_records: list[MaterialInputReportBindingRecord] = []
    for binding in selected:
        by_scope: dict[str, Any] = {}
        scope_names: dict[str, tuple[str, ...]] = {}
        for spec in binding.scopes:
            try:
                prepared = active_preparer.prepare_single_chapter(
                    asset=_asset(binding, repository_root),
                    chapter_task=MATERIAL_INPUT_CHAPTER,
                    scopes=(_scope_plan(spec),),
                    plan_version=MATERIAL_INPUT_RESEARCH_PLAN_VERSION,
                )
            except EvidencePreparationError as exc:
                if exc.code not in _SCOPE_PREPARATION_FAILURES:
                    raise
                outcomes.append(
                    extraction_failure_outcome(
                        sample_id=binding.sample_id,
                        scope_id=spec.scope_id,
                        instrument_id=binding.instrument_id,
                        report_id=binding.report_id,
                        document_version=binding.document_version,
                        page=spec.page,
                        section_title=spec.section_title,
                        code=exc.code.value,
                        message=str(exc),
                    )
                )
                continue
            if (
                len(prepared) != 1
                or prepared[0].chapter_task is not MATERIAL_INPUT_CHAPTER
            ):
                raise MaterialInputResearchError(
                    "research scope prepared a second chapter"
                )
            by_scope[spec.scope_id] = prepared[0]
            text = _scope_text(prepared[0])
            names = explicit_material_input_names(text)
            scope_names[spec.scope_id] = names
            outcome = classify_material_scope(spec.kind, names, text=text)
            outcomes.append(
                MaterialInputScopeOutcome(
                    sample_id=binding.sample_id,
                    scope_id=spec.scope_id,
                    kind=spec.kind,
                    outcome=outcome,
                    names=names,
                    reason=_outcome_reason(spec.kind, outcome),
                    evidence=_bound_evidence(prepared[0]),
                )
            )
        named = next(item for item in binding.scopes if item.kind == "named_input")
        prepared_named = by_scope.get(named.scope_id)
        if prepared_named is None or (
            classify_material_scope(
                named.kind,
                scope_names[named.scope_id],
                text=_scope_text(prepared_named),
            )
            != "observed"
        ):
            _append_report_record(report_records, binding, repository_root)
            continue
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
        page_hashes = {
            page.page: page.text_hash
            for scope in by_scope.values()
            for page in scope.page_contexts
        }
        exposures = {
            item.source_record_ids[0]: item
            for item in project_commodity_exposures(accepted, catalog=catalog)
        }
        for record in accepted:
            exposure = exposures[record.record_id]
            if isinstance(record, Relationship):
                if exposure.role != "raw_material_input":
                    raise MaterialInputResearchError(
                        f"{record.object_name} lost the material-input role"
                    )
                facts.append(
                    _input_fact(binding.sample_id, record, exposure, page_hashes)
                )
            elif isinstance(record, Activity):
                if exposure.role != "product_sales":
                    raise MaterialInputResearchError(
                        f"{record.object_name} lost the independent sales role"
                    )
                sales_roles.append(
                    _sales_role(binding.sample_id, record, exposure, page_hashes)
                )
        _append_report_record(report_records, binding, repository_root)
    return MaterialInputResearchBundle(
        run_id=run_id,
        reports=tuple(report_records),
        scope_outcomes=tuple(outcomes),
        facts=tuple(facts),
        sales_roles=tuple(sales_roles),
    )


def _append_report_record(
    records: list, binding: MaterialInputReportBinding, repository_root: Path
) -> None:
    dossier = repository_root / binding.relative_dossier_path
    records.append(
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


def _scope_text(scope) -> str:
    return "\n".join(page.text for page in scope.page_contexts)


def _outcome_reason(kind: str, outcome: str) -> str:
    if outcome == "observed":
        return "the cited sentence binds named materials to the company's own input"
    if outcome == "unclear" and kind == "named_input":
        return (
            "the cited evidence does not uniquely bind a material "
            "to the company's own input"
        )
    if outcome == "unclear":
        return "a name on this page is not uniquely bound as the company's input"
    reasons = {
        "named_input": "the report does not state a named company input",
        "direct_material_cost": "a direct-material cost amount is not a named input",
        "inventory_amount": "a raw-material inventory amount is not a named input",
        "outsourced_processing": "outsourced processing is not a material input",
        "product_overlap": "product or sales evidence alone does not create an input role",
    }
    try:
        return reasons[kind]
    except KeyError as exc:
        raise ValueError(f"unknown material scope kind: {kind}") from exc


def _bound_evidence(scope) -> tuple[ResearchEvidenceRef, ...]:
    hashes = {page.page: page.text_hash for page in scope.page_contexts}
    return tuple(
        _evidence_ref(item.evidence, hashes[item.evidence.page])
        for item in scope.evidence_bundle
    )


def _evidence_ref(evidence, page_text_hash: str) -> ResearchEvidenceRef:
    quote = evidence.anchor.bounded_quote
    return ResearchEvidenceRef(
        binding_status="bound",
        evidence_id=evidence.evidence_id,
        instrument_id=evidence.report.instrument_id,
        report_id=evidence.report.report_id,
        document_version=evidence.report.document_version,
        page=evidence.page,
        section_title=evidence.section_title,
        bounded_quote=quote,
        page_text_hash=page_text_hash,
    )


def _report_view(report) -> ResearchReportIdentity:
    return ResearchReportIdentity(
        instrument_id=report.instrument_id,
        report_id=report.report_id,
        document_version=report.document_version,
        report_period=report.report_period,
        published_at=report.published_at,
        document_type="annual_report",
    )


def _record_evidence(
    record, page_hashes: dict[int, str]
) -> tuple[ResearchEvidenceRef, ...]:
    return tuple(
        _evidence_ref(item, page_hashes[item.page]) for item in record.evidence
    )


def _input_fact(
    sample_id: str, record: Relationship, exposure, page_hashes
) -> MaterialInputResearchFact:
    return MaterialInputResearchFact(
        sample_id=sample_id,
        record_id=record.record_id,
        source_native_name=record.source_native.name or record.object_name,
        object_name=record.object_name,
        report=_report_view(record.report),
        reported_period=record.reported_period,
        period_type=record.period_type.value,
        subject_scope=record.subject_scope.value,
        subject_basis=record.subject_basis.value,
        evidence=_record_evidence(record, page_hashes),
        mapping_status=exposure.mapping_status,
        commodity_id=exposure.commodity_id,
    )


def _sales_role(
    sample_id: str, record: Activity, exposure, page_hashes
) -> MaterialInputSalesRole:
    return MaterialInputSalesRole(
        sample_id=sample_id,
        record_id=record.record_id,
        source_native_name=record.source_native.name or record.object_name,
        object_name=record.object_name,
        report=_report_view(record.report),
        reported_period=record.reported_period,
        period_type=record.period_type.value,
        subject_scope=record.subject_scope.value,
        subject_basis=record.subject_basis.value,
        actor_basis=record.actor_basis.value,
        activity_actor=record.activity_actor,
        source_actor=record.source_actor,
        source_verb=record.source_verb,
        evidence=_record_evidence(record, page_hashes),
        mapping_status=exposure.mapping_status,
        commodity_id=exposure.commodity_id,
    )


def _sales_names(
    binding: MaterialInputReportBinding,
    by_scope: dict[str, Any],
    input_names: tuple[str, ...],
) -> tuple[str, ...]:
    found: list[str] = []
    inputs = set(input_names)
    for spec in binding.scopes:
        if spec.kind != "product_overlap" or spec.scope_id not in by_scope:
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
