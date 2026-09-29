"""Single-chapter research path for segment revenue, cost, and reported margin.

The path reuses Stage 5 page preparation and the research isolation bundle.
It reads only formal tables. A reported gross margin is the percentage printed
in the margin column. Revenue and cost are never turned into a margin, an
Activity, or a substitute for an empty elimination cell.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .contracts import (
    ChecklistItem,
    PackageManifest,
    PreparedEvidence,
    SemanticTaskRequest,
)
from .execution import default_processing_identity
from .models import (
    AssertionClass,
    ChapterTask,
    CoverageReasonCode,
    CoverageStatus,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    ObjectType,
    PeriodType,
    ReportIdentity,
    RequirementLevel,
    RowClass,
    Segment,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from .stage5 import Stage5EvidencePreparer, Stage5ReportAsset
from .stage5_bundle import Stage5RunBundleStore
from .workflow import CompanyProfileSemanticService

SEGMENT_FINANCIAL_HISTORICAL_PLAN_VERSION = (
    "manufacturing_materials_stage4_segment_financials.2026-09-28.1"
)
SEGMENT_FINANCIAL_PRIOR_PLAN_VERSION = (
    "manufacturing_materials_stage4_segment_financials.2026-09-29.2"
)
SEGMENT_FINANCIAL_PLAN_VERSION = (
    "manufacturing_materials_stage4_segment_financials.2026-09-29.3"
)
SEGMENT_FINANCIAL_CHAPTER = ChapterTask.EXTRACT_SEGMENT_FINANCIALS
_SCHEMA = "company_profile_segment_financial_research.v1"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_FIELDS = (
    "segment_dimension",
    "operating_revenue",
    "operating_cost",
    "gross_margin_reported",
)
_METRIC = {
    "operating_revenue": MetricType.OPERATING_REVENUE,
    "operating_cost": MetricType.OPERATING_COST,
    "gross_margin_reported": MetricType.GROSS_MARGIN_REPORTED,
}
_SLOT = {
    MetricType.OPERATING_REVENUE: LogicalSlot.REVENUE,
    MetricType.OPERATING_COST: LogicalSlot.COST,
    MetricType.GROSS_MARGIN_REPORTED: LogicalSlot.GROSS_MARGIN,
}
_DIMENSION_MARKS = (
    "分销售模式",
    "按产品分类",
    "按区域分类",
    "分行业",
    "分产品",
    "分地区",
    "分业务",
)
_DASHES = {"-", "—", "－", "–"}
_PLAIN = re.compile(r"-?\d+(?:\.\d+)?")
_ELIMINATION = re.compile(r"抵销|抵消")
_SINGLE_SEGMENT = re.compile(r"仅有一个经营分部")
_STOP = ("产销量", "主要客户", "主要供应商", "成本分析", "实物销售收入")


class SegmentFinancialResearchError(RuntimeError):
    """The segment-financial research path left its single-chapter boundary."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class _ScopeBinding:
    scope_id: str
    pages: tuple[int, ...]
    section_title: str
    anchor_terms: tuple[str, ...]


@dataclass(frozen=True)
class SegmentFinancialReportBinding:
    sample_id: str
    company_name: str
    exchange: Literal["SSE", "SZSE", "BSE"]
    instrument_id: str
    report_id: str
    document_version: str
    published_at: str
    content_hash: str
    relative_pdf_path: str
    relative_dossier_path: str
    content_length: int
    page_count: int
    regime_type: Literal["stable", "restructuring"]
    regime_effective_period: str
    scopes: tuple[_ScopeBinding, ...]


@dataclass
class SegmentHit:
    page: int
    field_id: str
    source_dimension: str
    label: str
    value: str | None
    unit: str | None
    quote: str
    row_class: str | None = None


@dataclass
class SegmentCoverage:
    page: int
    field_id: str
    status: CoverageStatus
    reason_code: CoverageReasonCode
    reason: str
    quote: str
    source_dimension: str = ""
    label: str = ""


@dataclass
class _PageState:
    unit: str | None = None
    dimension: str | None = None
    mode: str | None = None
    column_section: str = ""
    columns: tuple[str, ...] = ()
    parsed_row: bool = False
    saw_metric_header: bool = False
    saw_sales_mode_title: bool = False
    saw_sales_mode_row: bool = False
    revenue_only: bool = False
    roles: tuple[str, ...] = ()
    row_metric: bool = False
    column_table: bool = False
    saw_margin_row: bool = False
    single_segment_quote: str = ""
    blanks: list[tuple[str, str, str]] = field(default_factory=list)


class SegmentFinancialFact(_StrictModel):
    sample_id: str
    record_id: str
    object_type: Literal["Segment", "Measurement"]
    field_id: str
    source_dimension: str
    label: str
    value: str | None = None
    unit: str | None = None
    page: int
    row_class: str | None = None
    evidence_id: str
    bounded_quote: str
    disposition: Literal["accepted_for_review"] = "accepted_for_review"


class SegmentFinancialCoverageFact(_StrictModel):
    sample_id: str
    field_id: str
    source_dimension: str = ""
    label: str = ""
    page: int
    coverage_status: Literal[
        "not_disclosed",
        "not_applicable",
        "unclear",
        "extraction_failed",
    ]
    legal_empty: Literal[True] = True
    reason: str
    bounded_quote: str


class SegmentFinancialReportRecord(_StrictModel):
    sample_id: str
    instrument_id: str
    exchange: Literal["SSE", "SZSE", "BSE"]
    report_id: str
    document_version: str
    content_hash: str


class SegmentFinancialResearchBundle(_StrictModel):
    schema_version: Literal["company_profile_segment_financial_research.v1"] = _SCHEMA
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION
    run_id: str
    chapter_task: Literal["extract_segment_financials"] = (
        SEGMENT_FINANCIAL_CHAPTER.value
    )
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    processing_identity: dict[str, str]
    reports: tuple[SegmentFinancialReportRecord, ...]
    facts: tuple[SegmentFinancialFact, ...] = ()
    coverage: tuple[SegmentFinancialCoverageFact, ...] = ()


def segment_financial_research_bindings() -> tuple[SegmentFinancialReportBinding, ...]:
    """Return the four approved reports as one segment-financial sample."""

    return (
        _binding(
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
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-segment-financials/"
                "dossiers/300750-sz-2025.md"
            ),
            content_length=2043710,
            page_count=232,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "300750-segment-table",
                    (24, 25),
                    "收入与成本",
                    ("分产品", "毛利率"),
                ),
                _ScopeBinding(
                    "300750-revenue-cost-note",
                    (195,),
                    "营业收入和营业成本",
                    ("营业收入和营业成本", "主营业务"),
                ),
                _ScopeBinding(
                    "300750-single-segment",
                    (223,),
                    "分部信息",
                    ("仅有一个经营分部",),
                ),
            ),
        ),
        _binding(
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
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-segment-financials/"
                "dossiers/603659-sh-2025.md"
            ),
            content_length=1740667,
            page_count=203,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "603659-segment-table",
                    (18, 19),
                    "主营业务分行业、分产品、分地区、分销售模式情况",
                    ("合并抵消项", "毛利率"),
                ),
            ),
        ),
        _binding(
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
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-segment-financials/"
                "dossiers/920015-bj-2025.md"
            ),
            content_length=1845726,
            page_count=143,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "920015-income-composition",
                    (16,),
                    "收入构成",
                    ("收入构成", "主营业务收入"),
                ),
                _ScopeBinding(
                    "920015-product-region",
                    (17,),
                    "按产品分类分析",
                    ("按产品分类", "毛利率"),
                ),
                _ScopeBinding(
                    "920015-note-segments",
                    (139,),
                    "分部信息",
                    ("分部间抵销",),
                ),
            ),
        ),
        _binding(
            sample_id="manufacturing-materials-302132-2025-regime",
            company_name="中航成飞",
            exchange="SZSE",
            instrument_id="302132.SZ",
            report_id="asset_0a488da55636b09107be6d719c9ebf39",
            document_version="ver_2d20ba3aebc5fac6c562cd619695995a",
            published_at="2026-04-28T16:00:00+00:00",
            content_hash=(
                "605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020"
            ),
            relative_pdf_path=(
                "data/filings/announcements/blobs/60/"
                "605394bd0879f906a829a9fcd3a2dab037d8aad2554b741a7d95757a3a5e3020.pdf"
            ),
            relative_dossier_path=(
                "openspec/changes/scope-manufacturing-materials-stage4-segment-financials/"
                "dossiers/302132-sz-2025.md"
            ),
            content_length=1721821,
            page_count=186,
            regime_type="restructuring",
            regime_effective_period="2025-01-06 onward",
            scopes=(
                _ScopeBinding(
                    "302132-mda-table",
                    (14, 15),
                    "收入与成本",
                    ("分销售模式", "毛利率"),
                ),
                _ScopeBinding(
                    "302132-note-segments",
                    (178,),
                    "报告分部的财务信息",
                    ("分部间抵销", "营业收入"),
                ),
            ),
        ),
    )


def interpret_segment_financial_pages(
    pages: tuple[tuple[int, str], ...] | list[tuple[int, str]],
) -> tuple[tuple[SegmentHit, ...], tuple[SegmentCoverage, ...]]:
    """Read formal segment tables. Do not derive a margin or merge dimensions."""

    hits: list[SegmentHit] = []
    coverages: list[SegmentCoverage] = []
    saw_sales_mode_title = False
    saw_sales_mode_row = False
    sales_mode_quote = ""
    sales_mode_page = 0
    carried_unit: str | None = None
    carried_roles: tuple[str, ...] = ()
    carried_dimension: str | None = None
    carried_row_metric = False
    carried_revenue_only = False
    for page, text in pages:
        state = _PageState()
        state.unit = carried_unit
        state.roles = carried_roles
        state.dimension = carried_dimension
        state.row_metric = carried_row_metric
        state.revenue_only = carried_revenue_only
        if not text.strip():
            coverages.append(
                _coverage(
                    page,
                    "segment_dimension",
                    CoverageStatus.EXTRACTION_FAILED,
                    CoverageReasonCode.SOURCE_UNREADABLE,
                    "page text is empty",
                    text or " ",
                )
            )
            continue
        if _SINGLE_SEGMENT.search(re.sub(r"\s+", "", text)):
            state.single_segment_quote = _sentence(text, "仅有一个经营分部")
        for line in _prepare_lines(text):
            if _stopped(line):
                state.mode = None
                state.roles = ()
                state.row_metric = False
                continue
            _consume_line(state, page, line, hits, coverages)
        if state.saw_sales_mode_title:
            saw_sales_mode_title = True
            sales_mode_quote = sales_mode_quote or _sentence(text, "销售模式")
            sales_mode_page = sales_mode_page or page
        if state.saw_sales_mode_row:
            saw_sales_mode_row = True
        carried_unit = state.unit or carried_unit
        carried_roles = state.roles
        carried_dimension = state.dimension
        carried_row_metric = state.row_metric
        carried_revenue_only = state.revenue_only
        _finish_page(state, page, text, coverages)
    if saw_sales_mode_title and not saw_sales_mode_row:
        coverages.append(
            _coverage(
                sales_mode_page,
                "segment_dimension",
                CoverageStatus.NOT_DISCLOSED,
                CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                "sales-mode section has no data row",
                sales_mode_quote,
                source_dimension="分销售模式",
            )
        )
    return tuple(_dedupe_hits(hits)), tuple(_dedupe_coverages(coverages))


def build_segment_financial_research_bundle(
    *,
    repository_root: Path,
    run_id: str,
    preparer: Stage5EvidencePreparer | None = None,
    bindings: tuple[SegmentFinancialReportBinding, ...] | None = None,
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION,
) -> SegmentFinancialResearchBundle:
    """Prepare one chapter through Stage 5 and keep the review disposition."""

    selected = segment_financial_research_bindings() if bindings is None else bindings
    _require_known_sample(selected)
    active = preparer or Stage5EvidencePreparer()
    facts: list[SegmentFinancialFact] = []
    coverage_facts: list[SegmentFinancialCoverageFact] = []
    reports: list[SegmentFinancialReportRecord] = []
    for binding in selected:
        pages: list[tuple[int, str]] = []
        for spec in binding.scopes:
            prepared = active.prepare_single_chapter(
                asset=_asset(binding, repository_root),
                chapter_task=SEGMENT_FINANCIAL_CHAPTER,
                scopes=(_scope_plan(spec),),
                plan_version=plan_version,
            )
            if len(prepared) != 1 or prepared[0].chapter_task is not (
                SEGMENT_FINANCIAL_CHAPTER
            ):
                raise SegmentFinancialResearchError(
                    "research scope prepared a second chapter"
                )
            if prepared[0].plan_version != plan_version:
                raise SegmentFinancialResearchError(
                    "stage5 preparation returned a different plan version"
                )
            for page in prepared[0].page_contexts:
                pages.append((page.page, page.text))
        hits, coverages = interpret_segment_financial_pages(tuple(pages))
        accepted = _accept(binding, hits) if hits else ()
        facts.extend(_fact(binding.sample_id, item) for item in accepted)
        coverage_facts.extend(
            _coverage_fact(binding.sample_id, item) for item in coverages
        )
        reports.append(
            SegmentFinancialReportRecord(
                sample_id=binding.sample_id,
                instrument_id=binding.instrument_id,
                exchange=binding.exchange,
                report_id=binding.report_id,
                document_version=binding.document_version,
                content_hash=binding.content_hash,
            )
        )
    return SegmentFinancialResearchBundle(
        run_id=run_id,
        plan_version=plan_version,
        processing_identity=default_processing_identity(),
        reports=tuple(reports),
        facts=tuple(facts),
        coverage=tuple(coverage_facts),
    )


def commit_segment_financial_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    run_id: str = "stage4-segment-financials",
    preparer: Stage5EvidencePreparer | None = None,
    bindings: tuple[SegmentFinancialReportBinding, ...] | None = None,
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION,
) -> Path:
    """Write one accepted_for_review bundle outside common-core production."""

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    bundle = build_segment_financial_research_bundle(
        repository_root=root,
        run_id=run_id,
        preparer=preparer,
        bindings=bindings,
        plan_version=plan_version,
    )
    destination = store.output_root / f"segment-financial-{bundle.run_id}"
    if destination.exists():
        raise FileExistsError(
            f"segment-financial research run already exists: {bundle.run_id}"
        )
    temporary = store.output_root / f".stage5-tmp-{bundle.run_id}-{uuid.uuid4().hex}"
    temporary.mkdir(parents=False, exist_ok=False)
    try:
        payload = bundle.model_dump(mode="json")
        forbidden = {
            "recall",
            "accuracy",
            "critical_numeric_errors",
            "expansion_gates_met",
            "source_review",
        }
        if forbidden & set(payload):
            raise SegmentFinancialResearchError(
                "segment-financial bundle preset a review metric"
            )
        (temporary / "result.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, destination)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise
    return destination


class SegmentFinancialEnqueueScope(_StrictModel):
    scope_id: str = Field(min_length=1)
    pages: tuple[int, ...] = Field(min_length=1)
    section_title: str = Field(min_length=1)
    anchor_terms: tuple[str, ...] = Field(min_length=1)
    chapter_task: Literal["extract_segment_financials"] = (
        SEGMENT_FINANCIAL_CHAPTER.value
    )


class SegmentFinancialEnqueueReport(_StrictModel):
    sample_id: str = Field(min_length=1)
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    report_period: Literal["2025-12-31"] = "2025-12-31"
    published_at: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    dossier_path: str = Field(min_length=1)
    dossier_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION
    chapter_task: Literal["extract_segment_financials"] = (
        SEGMENT_FINANCIAL_CHAPTER.value
    )
    scopes: tuple[SegmentFinancialEnqueueScope, ...] = Field(min_length=1)


class SegmentFinancialEnqueueSnapshot(_StrictModel):
    schema_version: Literal["company_profile_segment_financial_research_enqueue.v1"] = (
        "company_profile_segment_financial_research_enqueue.v1"
    )
    chapter_task: Literal["extract_segment_financials"] = (
        SEGMENT_FINANCIAL_CHAPTER.value
    )
    production_authorization: Literal["not_authorized"] = "not_authorized"
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION
    reports: tuple[SegmentFinancialEnqueueReport, ...] = Field(
        min_length=4, max_length=4
    )


class SegmentFinancialRunSnapshot(_StrictModel):
    schema_version: Literal["company_profile_segment_financial_research_run.v1"] = (
        "company_profile_segment_financial_research_run.v1"
    )
    chapter_task: Literal["extract_segment_financials"] = (
        SEGMENT_FINANCIAL_CHAPTER.value
    )
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION
    enqueue_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bundle_dirname: str = Field(min_length=1)
    run_id: str = Field(min_length=1)


def freeze_segment_financial_research_enqueue(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    bindings: tuple[SegmentFinancialReportBinding, ...] | None = None,
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION,
) -> Path:
    """Record PDF identity and dossier hashes before the run starts."""

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    destination = store.output_root / "enqueue.json"
    if destination.exists():
        raise FileExistsError(
            f"segment-financial enqueue snapshot already exists: {destination}"
        )
    snapshot = _enqueue_snapshot(root, bindings, plan_version=plan_version)
    _write_json_atomic(
        store.output_root, destination.name, snapshot.model_dump(mode="json")
    )
    return destination


def replay_segment_financial_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    run_id: str = "stage4-segment-financials-20260928",
    preparer: Stage5EvidencePreparer | None = None,
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION,
) -> Path:
    """Freeze enqueue, prepare one chapter, then write run and result.

    The snapshots do not record recall, accuracy, critical errors, or a gate.
    """

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    selected = segment_financial_research_bindings()
    enqueue_path = freeze_segment_financial_research_enqueue(
        output_root,
        repository_root=root,
        bindings=selected,
        plan_version=plan_version,
    )
    bundle_dir = commit_segment_financial_research(
        output_root,
        repository_root=root,
        run_id=run_id,
        preparer=preparer,
        bindings=selected,
        plan_version=plan_version,
    )
    run = SegmentFinancialRunSnapshot(
        plan_version=plan_version,
        enqueue_sha256=hashlib.sha256(enqueue_path.read_bytes()).hexdigest(),
        bundle_dirname=bundle_dir.name,
        run_id=run_id,
    )
    run_path = enqueue_path.parent / "run.json"
    if run_path.exists():
        raise FileExistsError(
            f"segment-financial run snapshot already exists: {run_path}"
        )
    _write_json_atomic(enqueue_path.parent, run_path.name, run.model_dump(mode="json"))
    return run_path


def _enqueue_snapshot(
    repository_root: Path,
    bindings: tuple[SegmentFinancialReportBinding, ...] | None = None,
    plan_version: str = SEGMENT_FINANCIAL_PLAN_VERSION,
) -> SegmentFinancialEnqueueSnapshot:
    selected = segment_financial_research_bindings() if bindings is None else bindings
    if tuple(item.instrument_id for item in selected) != (
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    ):
        raise SegmentFinancialResearchError(
            "segment-financial replay only admits the four defining reports"
        )
    reports: list[SegmentFinancialEnqueueReport] = []
    for binding in selected:
        pdf = repository_root / binding.relative_pdf_path
        dossier = repository_root / binding.relative_dossier_path
        content_hash = hashlib.sha256(pdf.read_bytes()).hexdigest()
        if content_hash != binding.content_hash:
            raise SegmentFinancialResearchError(
                f"{binding.instrument_id} PDF hash does not match the frozen binding"
            )
        reports.append(
            SegmentFinancialEnqueueReport(
                sample_id=binding.sample_id,
                instrument_id=binding.instrument_id,
                report_id=binding.report_id,
                document_version=binding.document_version,
                published_at=binding.published_at,
                content_hash=content_hash,
                dossier_path=binding.relative_dossier_path,
                dossier_sha256=hashlib.sha256(dossier.read_bytes()).hexdigest(),
                plan_version=plan_version,
                scopes=tuple(
                    SegmentFinancialEnqueueScope(
                        scope_id=item.scope_id,
                        pages=item.pages,
                        section_title=item.section_title,
                        anchor_terms=item.anchor_terms,
                    )
                    for item in binding.scopes
                ),
            )
        )
    return SegmentFinancialEnqueueSnapshot(
        plan_version=plan_version, reports=tuple(reports)
    )


def _write_json_atomic(directory: Path, name: str, payload: dict[str, Any]) -> None:
    forbidden = {
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
        "source_review",
    }
    if forbidden & set(payload):
        raise SegmentFinancialResearchError(
            "segment-financial snapshot preset a review metric"
        )
    temporary = directory / f".stage5-tmp-{name}-{uuid.uuid4().hex}"
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, directory / name)


def _binding(**kwargs: Any) -> SegmentFinancialReportBinding:
    return SegmentFinancialReportBinding(**kwargs)


def _require_known_sample(
    bindings: tuple[SegmentFinancialReportBinding, ...],
) -> None:
    known = {item.instrument_id for item in segment_financial_research_bindings()}
    unknown = [
        item.instrument_id for item in bindings if item.instrument_id not in known
    ]
    if unknown:
        raise SegmentFinancialResearchError(
            "segment-financial research only admits the four defining reports"
        )


def _asset(
    binding: SegmentFinancialReportBinding, repository_root: Path
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
        regime_type=binding.regime_type,
        regime_effective_period=binding.regime_effective_period,
    )


def _scope_plan(spec: _ScopeBinding):
    from .stage5 import EvidenceScopePlan

    return EvidenceScopePlan(
        scope_id=spec.scope_id,
        field_ids=_FIELDS,
        pages=spec.pages,
        section_titles=(spec.section_title,),
        anchor_terms=spec.anchor_terms,
    )


def _consume_line(
    state: _PageState,
    page: int,
    line: str,
    hits: list[SegmentHit],
    coverages: list[SegmentCoverage],
) -> None:
    compact = re.sub(r"\s+", "", line)
    if "销售模式" in compact and "行业" in compact:
        state.saw_sales_mode_title = True
    declared = _unit(line)
    if declared is not None:
        state.unit = declared
        return
    if _is_column_header(line):
        state.mode = "columns"
        state.column_table = True
        state.roles = ()
        state.row_metric = False
        state.revenue_only = False
        state.columns = _column_names(line)
        return
    if _closes_open_table(line):
        state.roles = ()
        state.row_metric = False
        state.mode = None
        state.revenue_only = False
        return
    section = _table_section(line)
    if section is not None:
        state.dimension = section
        state.roles = ()
        state.row_metric = False
        state.mode = None
        state.revenue_only = False
        return
    roles = _roles_from_header(line)
    if roles is not None:
        if set(roles) <= {"ignore"} and _open_measure_roles(state.roles):
            state.roles = state.roles + roles
            return
        state.saw_metric_header = True
        state.roles = roles
        state.row_metric = False
        state.mode = "metrics"
        state.revenue_only = "revenue" in roles and "cost" not in roles
        header_dimension = _dimension_on_header(line)
        if header_dimension is not None:
            state.dimension = header_dimension
        return
    if _is_period_header(line) and not state.roles:
        state.row_metric = True
        state.mode = "metrics"
        state.revenue_only = False
        return
    ignore_roles = _ignore_roles(line)
    if ignore_roles and _open_measure_roles(state.roles):
        state.roles = state.roles + ignore_roles
        return
    column_section = _column_section(line)
    if column_section is not None:
        _bind_printed_section(state, column_section)
        return
    if _is_new_clause(line):
        state.roles = ()
        state.row_metric = False
        state.mode = None
        state.revenue_only = False
        state.columns = ()
        return
    dimension = _dimension_label(line)
    if dimension is not None:
        state.dimension = dimension
        return
    if state.mode == "columns":
        _consume_column_row(state, page, line, hits)
        return
    if not state.roles and not state.row_metric:
        return
    _consume_metric_row(state, page, line, hits, coverages)


def _bind_printed_section(state: _PageState, section: str) -> None:
    """Bind the printed section, then drop the previous table's column state."""

    if state.column_section != section:
        state.columns = ()
        state.mode = None
        state.column_table = False
        state.roles = ()
        state.row_metric = False
        state.revenue_only = False
    state.column_section = section


def _finish_page(
    state: _PageState,
    page: int,
    text: str,
    coverages: list[SegmentCoverage],
) -> None:
    if state.single_segment_quote and not state.parsed_row:
        quote = state.single_segment_quote
        for field_id in _FIELDS:
            coverages.append(
                _coverage(
                    page,
                    field_id,
                    CoverageStatus.NOT_APPLICABLE,
                    CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE,
                    "source states a single operating segment",
                    quote,
                )
            )
        return
    if state.saw_metric_header and not state.parsed_row:
        coverages.append(
            _coverage(
                page,
                "segment_dimension",
                CoverageStatus.EXTRACTION_FAILED,
                CoverageReasonCode.TABLE_CONTEXT_INCOMPLETE,
                "formal header has no data row",
                _sentence(text, "营业收入"),
            )
        )
        return
    if state.revenue_only and state.parsed_row and "cost" not in state.roles:
        for field_id in ("operating_cost", "gross_margin_reported"):
            coverages.append(
                _coverage(
                    page,
                    field_id,
                    CoverageStatus.NOT_DISCLOSED,
                    CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                    "revenue composition table has no cost or margin column",
                    _sentence(text, "营业收入"),
                )
            )
    if state.column_table and not state.saw_margin_row:
        coverages.append(
            _coverage(
                page,
                "gross_margin_reported",
                CoverageStatus.NOT_DISCLOSED,
                CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                "reportable-segment table has no margin row",
                _sentence(text, "营业收入"),
                source_dimension=state.column_section,
            )
        )
    for section, label, quote in state.blanks:
        for field_id in ("operating_revenue", "operating_cost"):
            coverages.append(
                _coverage(
                    page,
                    field_id,
                    CoverageStatus.UNCLEAR,
                    CoverageReasonCode.CANDIDATE_UNRESOLVED,
                    "elimination column is present and its amount cell is empty",
                    quote,
                    source_dimension=section,
                    label=label,
                )
            )


def _consume_metric_row(
    state: _PageState,
    page: int,
    line: str,
    hits: list[SegmentHit],
    coverages: list[SegmentCoverage],
) -> None:
    label, tail = _label_and_tail(line)
    if label is None or state.dimension is None:
        return
    if label in {
        "项目",
        "分行业",
        "分产品",
        "分地区",
        "分业务",
        "分销售模式",
        "营业收入",
        "营业成本",
        "毛利率",
    }:
        return
    values = _value_tokens(tail)
    if not values:
        return
    adjustment = _ELIMINATION.search(label) is not None
    if state.row_metric:
        _consume_labeled_metric_row(state, page, line, label, values, hits, adjustment)
        return
    if not state.roles:
        return
    assigned = _assign_roles(values, state.roles)
    revenue = assigned.get("revenue")
    cost = assigned.get("cost")
    if revenue is None and cost is None and "margin" not in assigned:
        return
    if revenue is not None or cost is not None:
        _append_row(
            hits,
            page=page,
            source_dimension=state.dimension,
            label=label,
            revenue=revenue,
            cost=cost,
            unit=state.unit or "元",
            quote=line,
            adjustment=adjustment,
        )
    state.parsed_row = True
    if state.dimension == "分销售模式":
        state.saw_sales_mode_row = True
    margin = assigned.get("margin")
    if "margin" not in state.roles:
        return
    if margin is None:
        coverages.append(
            _coverage(
                page,
                "gross_margin_reported",
                CoverageStatus.NOT_DISCLOSED,
                CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
                "margin cell is empty or not a reported percentage",
                line,
                source_dimension=state.dimension,
                label=label,
            )
        )
        return
    hits.append(
        SegmentHit(
            page=page,
            field_id="gross_margin_reported",
            source_dimension=state.dimension,
            label=label,
            value=margin,
            unit="%",
            quote=line,
            row_class="consolidation_adjustment" if adjustment else None,
        )
    )


def _consume_column_row(
    state: _PageState,
    page: int,
    line: str,
    hits: list[SegmentHit],
) -> None:
    label, tail = _label_and_tail(line)
    if label is None or label.startswith("其中") or not state.column_section:
        return
    field_id = _column_field(label)
    if field_id is None or not state.columns:
        return
    if field_id == "gross_margin_reported":
        state.saw_margin_row = True
    amounts = [token for token in tail if _is_money(token)]
    elimination = [name for name in state.columns if _ELIMINATION.search(name)]
    if elimination and len(amounts) == len(state.columns) - len(elimination):
        names = [name for name in state.columns if name not in elimination]
        pairs = list(zip(names, amounts, strict=True))
        state.blanks.extend((state.column_section, name, line) for name in elimination)
    elif len(amounts) == len(state.columns):
        pairs = list(zip(state.columns, amounts, strict=True))
    else:
        return
    for name, amount in pairs:
        adjustment = _ELIMINATION.search(name) is not None
        row_class = "consolidation_adjustment" if adjustment else None
        hits.append(
            SegmentHit(
                page=page,
                field_id="segment_dimension",
                source_dimension=state.column_section,
                label=name,
                value=None,
                unit=None,
                quote=line,
                row_class=row_class,
            )
        )
        hits.append(
            SegmentHit(
                page=page,
                field_id=field_id,
                source_dimension=state.column_section,
                label=name,
                value=amount,
                unit=state.unit or "元",
                quote=line,
                row_class=row_class,
            )
        )
    state.parsed_row = True


def _append_row(
    hits: list[SegmentHit],
    *,
    page: int,
    source_dimension: str,
    label: str,
    revenue: str | None,
    cost: str | None,
    unit: str,
    quote: str,
    adjustment: bool,
) -> None:
    row_class = "consolidation_adjustment" if adjustment else None
    hits.append(
        SegmentHit(
            page=page,
            field_id="segment_dimension",
            source_dimension=source_dimension,
            label=label,
            value=None,
            unit=None,
            quote=quote,
            row_class=row_class,
        )
    )
    if revenue is not None:
        hits.append(
            SegmentHit(
                page=page,
                field_id="operating_revenue",
                source_dimension=source_dimension,
                label=label,
                value=revenue,
                unit=unit,
                quote=quote,
                row_class=row_class,
            )
        )
    if cost is not None:
        hits.append(
            SegmentHit(
                page=page,
                field_id="operating_cost",
                source_dimension=source_dimension,
                label=label,
                value=cost,
                unit=unit,
                quote=quote,
                row_class=row_class,
            )
        )


def _reported_margin(tail: list[str]) -> tuple[str | None, str]:
    """Return a printed margin. A short row's year-over-year numbers stay out."""

    values = [token for token in tail if token not in {"增加", "减少", "个百分点"}]
    if not values:
        return None, "not_disclosed"
    if values[0] in _DASHES:
        return None, "not_disclosed"
    if values[0].endswith("%"):
        return values[0], "observed"
    plain = [token for token in values if _PLAIN.fullmatch(token)]
    if len(plain) >= 3:
        return plain[0], "observed"
    if len(plain) == 1:
        return None, "unclear"
    return None, "not_disclosed"


def _accept(
    binding: SegmentFinancialReportBinding,
    hits: tuple[SegmentHit, ...],
) -> tuple[Any, ...]:
    report = _identity(binding)
    prepared: list[PreparedEvidence] = []
    candidates: list[Segment | Measurement] = []
    for hit in hits:
        evidence = _evidence(report, hit)
        prepared.append(
            PreparedEvidence(
                evidence=evidence, field_id=hit.field_id, source_readable=True
            )
        )
        if hit.field_id == "segment_dimension":
            candidates.append(_segment(report, binding.sample_id, hit, evidence))
        else:
            candidates.append(_measurement(report, binding.sample_id, hit, evidence))
    if not candidates:
        return ()
    request = SemanticTaskRequest(
        request_id=f"{binding.sample_id}:extract_segment_financials",
        report=report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version=SEGMENT_FINANCIAL_PLAN_VERSION,
            report=report,
            checklist=tuple(_checklist(field_id) for field_id in _FIELDS),
        ),
        chapter_task=SEGMENT_FINANCIAL_CHAPTER,
        evidence_bundle=tuple(prepared),
        allowed_object_types=(ObjectType.SEGMENT, ObjectType.MEASUREMENT),
        allowed_metric_types=tuple(_METRIC.values()),
        prohibited_inferences=(
            "derive_gross_margin_from_revenue_and_cost",
            "replace_formal_table_with_narrative",
            "merge_equal_amounts_across_dimensions",
            "elimination_as_activity",
        ),
        deterministic_candidates=tuple(candidates),
        provided_coverage=(),
        unresolved_field_ids=(),
    )
    result = CompanyProfileSemanticService().run_task(request, provider=None)
    if result.provider_calls:
        raise SegmentFinancialResearchError(
            "segment-financial research called a provider"
        )
    refused = [
        item.status.value
        for item in result.dispositions
        if item.status.value != "accepted_for_review"
    ]
    if refused:
        raise SegmentFinancialResearchError(
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


def _segment(report, sample_id: str, hit: SegmentHit, evidence: Evidence) -> Segment:
    adjustment = hit.row_class == "consolidation_adjustment"
    return Segment(
        record_id=_record_id(sample_id, hit),
        field_id="segment_dimension",
        chapter_task=SEGMENT_FINANCIAL_CHAPTER,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        subject_basis=SubjectBasis.UNCLEAR,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name=hit.label, header=hit.source_dimension),
        dimension="adjustment" if adjustment else hit.source_dimension,
        label=hit.label,
        row_class=RowClass.CONSOLIDATION_ADJUSTMENT if adjustment else None,
    )


def _measurement(
    report, sample_id: str, hit: SegmentHit, evidence: Evidence
) -> Measurement:
    metric = _METRIC[hit.field_id]
    adjustment = hit.row_class == "consolidation_adjustment"
    return Measurement(
        record_id=_record_id(sample_id, hit),
        field_id=hit.field_id,
        chapter_task=SEGMENT_FINANCIAL_CHAPTER,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        subject_basis=SubjectBasis.UNCLEAR,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(
            name=hit.label,
            value=hit.value,
            unit=hit.unit,
            header=hit.source_dimension,
        ),
        metric_type=metric,
        logical_slot=_SLOT[metric],
        measured_object=hit.label,
        segment_dimension=hit.source_dimension,
        segment_label=hit.label,
        row_class=RowClass.CONSOLIDATION_ADJUSTMENT if adjustment else None,
    )


def _identity(binding: SegmentFinancialReportBinding) -> ReportIdentity:
    return ReportIdentity(
        instrument_id=binding.instrument_id,
        report_id=binding.report_id,
        document_version=binding.document_version,
        report_period="2025-12-31",
        published_at=binding.published_at,
    )


def _evidence(report: ReportIdentity, hit: SegmentHit) -> Evidence:
    return Evidence(
        evidence_id=(
            "stage5-evidence-"
            + hashlib.sha256(
                f"{hit.page}:{hit.field_id}:{hit.source_dimension}:{hit.label}:{hit.quote}".encode()
            ).hexdigest()[:24]
        ),
        report=report,
        page=hit.page,
        section_title=hit.source_dimension,
        anchor=TextAnchor(bounded_quote=hit.quote),
    )


def _checklist(field_id: str) -> ChecklistItem:
    if field_id == "segment_dimension":
        return ChecklistItem(
            field_id=field_id,
            object_type=ObjectType.SEGMENT,
            chapter_task=SEGMENT_FINANCIAL_CHAPTER,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
        )
    return ChecklistItem(
        field_id=field_id,
        object_type=ObjectType.MEASUREMENT,
        chapter_task=SEGMENT_FINANCIAL_CHAPTER,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(_METRIC[field_id],),
    )


def _fact(sample_id: str, record) -> SegmentFinancialFact:
    evidence = record.evidence[0]
    value = getattr(record.source_native, "value", None)
    unit = getattr(record.source_native, "unit", None)
    row_class = getattr(record, "row_class", None)
    if isinstance(record, Segment):
        source_dimension = record.source_native.header or record.dimension
        label = record.label
        object_type = "Segment"
    else:
        source_dimension = record.segment_dimension or ""
        label = record.segment_label or record.measured_object
        object_type = "Measurement"
    return SegmentFinancialFact(
        sample_id=sample_id,
        record_id=record.record_id,
        object_type=object_type,
        field_id=record.field_id,
        source_dimension=source_dimension,
        label=label,
        value=value,
        unit=unit,
        page=evidence.page,
        row_class=row_class.value if row_class is not None else None,
        evidence_id=evidence.evidence_id,
        bounded_quote=evidence.anchor.bounded_quote,
    )


def _coverage_fact(
    sample_id: str, item: SegmentCoverage
) -> SegmentFinancialCoverageFact:
    return SegmentFinancialCoverageFact(
        sample_id=sample_id,
        field_id=item.field_id,
        source_dimension=item.source_dimension,
        label=item.label,
        page=item.page,
        coverage_status=item.status.value,
        reason=item.reason,
        bounded_quote=item.quote,
    )


def _record_id(sample_id: str, hit: SegmentHit) -> str:
    return ":".join(
        (
            sample_id,
            hit.field_id,
            hit.source_dimension,
            hit.label,
            hit.value or "",
            str(hit.page),
        )
    )


def _coverage(
    page: int,
    field_id: str,
    status: CoverageStatus,
    reason_code: CoverageReasonCode,
    reason: str,
    quote: str,
    *,
    source_dimension: str = "",
    label: str = "",
) -> SegmentCoverage:
    return SegmentCoverage(
        page=page,
        field_id=field_id,
        status=status,
        reason_code=reason_code,
        reason=reason,
        quote=quote.strip() or " ",
        source_dimension=source_dimension,
        label=label,
    )


def _dedupe_coverages(items: list[SegmentCoverage]) -> list[SegmentCoverage]:
    unique: list[SegmentCoverage] = []
    seen: set[tuple[int, str, str, str, str]] = set()
    for item in items:
        key = (
            item.page,
            item.field_id,
            item.source_dimension,
            item.label,
            item.status.value,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _dedupe_hits(hits: list[SegmentHit]) -> list[SegmentHit]:
    unique: list[SegmentHit] = []
    seen: set[tuple[int, str, str, str, str]] = set()
    for hit in hits:
        key = (hit.page, hit.field_id, hit.source_dimension, hit.label, hit.value or "")
        if key in seen:
            continue
        seen.add(key)
        unique.append(hit)
    return unique


def _prepare_lines(text: str) -> list[str]:
    """Join PDF line wraps inside a label or a broken amount."""

    lines = [re.sub(r"[^\S\n]+", " ", line).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    lines = _join_broken_amounts(lines)
    lines = _join_header_shards(lines)
    lines = _join_split_headers(lines)
    lines = _join_wrapped_labels(lines)
    return _join_following_amounts(lines)


def _join_broken_amounts(lines: list[str]) -> list[str]:
    joined: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index]
        while index + 1 < len(lines) and _amount_continues(current, lines[index + 1]):
            nxt = lines[index + 1].strip()
            if current == "-" or current.endswith(("-", ",")) or nxt.startswith(","):
                current = f"{current}{nxt}"
            else:
                head, _, last = current.rpartition(" ")
                token = f"{last}{nxt.split()[0]}"
                rest = " ".join(nxt.split()[1:])
                current = " ".join(part for part in (head, token, rest) if part)
            index += 1
        joined.append(current)
        index += 1
    return joined


def _amount_continues(current: str, nxt: str) -> bool:
    follow = nxt.strip()
    if not follow or re.match(r"[\u4e00-\u9fff]", follow):
        return False
    if follow.startswith(","):
        return True
    if not re.match(r"\d", follow):
        return False
    token = current.split()[-1] if current.split() else current
    if token == "-":
        return True
    if token.endswith((",", ".")):
        return True
    return re.search(r"(?:,\d{1,2}|\.\d)$", token) is not None


def _join_header_shards(lines: list[str]) -> list[str]:
    """Attach a vertically wrapped 分部间抵销 / 合计 token to its header row."""

    joined: list[str] = []
    for line in lines:
        shard = re.sub(r"\s+", "", line)
        if (
            joined
            and re.fullmatch(r"[\u4e00-\u9fff]{1,3}", shard)
            and (
                "项" in joined[-1]
                or joined[-1].endswith(("分", "部", "抵", "间", "合"))
            )
        ):
            if joined[-1][-1] in "分部抵间合":
                joined[-1] = f"{joined[-1]}{shard}"
            else:
                joined[-1] = f"{joined[-1]} {shard}"
            continue
        joined.append(line)
    return joined


def _join_split_headers(lines: list[str]) -> list[str]:
    """Bind one formal header when a PDF wraps its column titles."""

    joined: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index]
        extras = 0
        while (
            extras < 6
            and index + 1 < len(lines)
            and _header_continues(current, lines[index + 1])
        ):
            current = f"{current} {lines[index + 1]}"
            index += 1
            extras += 1
        joined.append(current)
        index += 1
    return joined


def _header_continues(current: str, nxt: str) -> bool:
    if _first_money(current) is not None or _first_money(nxt) is not None:
        return False
    if _table_section(current) is not None or _table_section(nxt) is not None:
        return False
    if (
        _dimension_label(nxt) is not None
        or _is_column_header(nxt)
        or _closes_open_table(nxt)
    ):
        return False
    current_compact = re.sub(r"\s+", "", current)
    next_compact = re.sub(r"\s+", "", nxt)
    if (
        _is_role_word_line(next_compact)
        and not _is_role_word_line(current_compact)
        and "比重" not in next_compact
        and "增减" not in next_compact
    ):
        return False
    if not _looks_like_header(current_compact):
        return False
    return _looks_like_header_shard(next_compact)


def _looks_like_header(compact: str) -> bool:
    return any(
        token in compact
        for token in ("金额", "营业收入", "营业成本", "毛利率", "比重", "项目", "同比")
    )


def _looks_like_header_shard(compact: str) -> bool:
    if compact in {
        "重",
        "比重",
        "比重%",
        "的比重%",
        "增减",
        "增减%",
        "同期增减",
        "年同期增减",
        "比上",
        "比上年同期增减",
    }:
        return True
    if len(compact) > 24:
        return False
    return any(
        token in compact
        for token in ("比重", "增减", "同期", "毛利率", "营业收入", "营业成本", "金额")
    )


def _join_wrapped_labels(lines: list[str]) -> list[str]:
    joined: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index]
        nxt = lines[index + 1] if index + 1 < len(lines) else ""
        if nxt and _is_label_fragment(current) and _starts_with_label_amount(nxt):
            prefix = re.sub(r"\s+", "", current)
            joined.append(prefix + nxt.strip())
            index += 2
            continue
        joined.append(current)
        index += 1
    return joined


def _join_following_amounts(lines: list[str]) -> list[str]:
    joined: list[str] = []
    for line in lines:
        first = line.split()[0] if line.split() else ""
        if (
            joined
            and (_is_money(first) or _is_signed_money(first))
            and _can_continue_row(joined[-1])
        ):
            joined[-1] = f"{joined[-1]} {line}"
            continue
        joined.append(line)
    return joined


def _is_label_fragment(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    if (
        not compact
        or _dimension_label(line) is not None
        or _is_metric_header(line)
        or _is_column_header(line)
        or _is_role_word_line(compact)
        or compact in {"合", "计", "合计", "小计", "分", "部", "间", "抵", "销"}
        or any(
            token in compact
            for token in (
                "增减",
                "毛利率",
                "营业收入",
                "营业成本",
                "百分点",
                "同期",
                "项目",
            )
        )
    ):
        return False
    return re.fullmatch(r"[\u4e00-\u9fff]{1,24}", compact) is not None


def _starts_with_label_amount(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    return (
        re.match(r"[\u4e00-\u9fff]", compact) is not None
        and _first_money(line) is not None
    )


def _is_signed_money(token: str) -> bool:
    return token.startswith("-") and _is_money(token)


def _can_continue_row(line: str) -> bool:
    return not (
        _dimension_label(line) is not None
        or _is_metric_header(line)
        or _is_column_header(line)
        or not re.search(r"[\u4e00-\u9fff]", line)
    )


def _consume_labeled_metric_row(
    state: _PageState,
    page: int,
    line: str,
    label: str,
    values: list[str],
    hits: list[SegmentHit],
    adjustment: bool,
) -> None:
    if not values:
        return
    current = values[0]
    if current in _DASHES:
        return
    if "成本" in label:
        field = "cost"
    elif "收入" in label:
        field = "revenue"
    else:
        return
    _append_row(
        hits,
        page=page,
        source_dimension=state.dimension or "",
        label=label,
        revenue=current if field == "revenue" else None,
        cost=current if field == "cost" else None,
        unit=state.unit or "元",
        quote=line,
        adjustment=adjustment,
    )
    state.parsed_row = True


def _value_tokens(tail: list[str]) -> list[str]:
    values: list[str] = []
    for token in tail:
        if token in {"增加", "减少", "个百分点", "个", "营业成本"}:
            continue
        if (
            _is_money(token)
            or token in _DASHES
            or token.endswith("%")
            or _PLAIN.fullmatch(token)
        ):
            values.append(token)
    return values


def _assign_roles(values: list[str], roles: tuple[str, ...]) -> dict[str, str | None]:
    assigned: dict[str, str | None] = {}
    if "margin" in roles:
        margin_index = roles.index("margin")
        trailing_roles = [
            role for role in roles[margin_index + 1 :] if role == "ignore"
        ]
        if margin_index < len(values):
            candidate = values[margin_index]
            trailing_values = values[margin_index + 1 :]
            if candidate in _DASHES:
                assigned["margin"] = None
            elif str(candidate).endswith("%"):
                assigned["margin"] = str(candidate)
            elif trailing_roles and 0 < len(trailing_values) < len(trailing_roles):
                assigned["margin"] = None
            elif _PLAIN.fullmatch(str(candidate)):
                assigned["margin"] = str(candidate)
            else:
                assigned["margin"] = None
        else:
            assigned["margin"] = None
    for role, value in zip(roles, values, strict=False):
        if role in {"revenue", "cost"} and value not in _DASHES:
            assigned.setdefault(role, value)
    return assigned


def _roles_from_header(line: str) -> tuple[str, ...] | None:
    compact = re.sub(r"\s+", "", line).replace("的", "")
    if _first_money(line) is not None:
        return None
    if (
        "占营业收入比重" in compact
        and "营业成本" not in compact
        and "毛利率" not in compact
    ):
        return ("revenue", "ignore", "ignore", "ignore", "ignore")
    if (
        "占营业成本比重" in compact
        and "营业收入" not in compact
        and "毛利率" not in compact
    ):
        return ("cost", "ignore", "ignore", "ignore", "ignore")
    pair = _pair_roles(compact)
    if pair is not None:
        return pair
    if "营业收入" not in compact or "营业成本" not in compact:
        return None
    roles = _metric_roles(compact)
    if (
        roles.count("revenue") != 1
        or roles.count("cost") != 1
        or roles.count("margin") > 1
    ):
        return None
    if "毛利率" in compact and "margin" not in roles:
        return None
    return roles


def _ignore_roles(line: str) -> tuple[str, ...] | None:
    compact = re.sub(r"\s+", "", line).replace("的", "")
    if _first_money(line) is not None:
        return None
    roles = _metric_roles(compact)
    if roles and set(roles) <= {"ignore"}:
        return roles
    return None


def _closes_open_table(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    if _table_section(line) is not None:
        return False
    if "分解" in compact and "营业" in compact:
        return True
    if _roles_from_header(line) is not None or _first_money(line) is not None:
        return False
    roles = _metric_roles(compact)
    return roles.count("revenue") > 1 or roles.count("cost") > 1


def _open_measure_roles(roles: tuple[str, ...]) -> bool:
    return "revenue" in roles or "cost" in roles or "margin" in roles


def _pair_roles(compact: str) -> tuple[str, ...] | None:
    if any(token in compact for token in ("营业收入", "营业成本", "毛利率", "比重")):
        return None
    tokens = re.findall(r"收入|成本", compact)
    if not tokens or set(tokens) != {"收入", "成本"}:
        return None
    if compact.replace("收入", "").replace("成本", ""):
        return None
    roles: list[str] = []
    closed = False
    for token in tokens:
        if closed:
            roles.append("ignore")
            continue
        roles.append("revenue" if token == "收入" else "cost")
        if token == "成本" and "revenue" in roles:
            closed = True
    return tuple(roles)


def _metric_roles(compact: str) -> tuple[str, ...]:
    roles: list[str] = []
    pattern = re.compile(
        r"营业收入比|营业成本比|毛利率比|同比增减|营业收入|营业成本|毛利率"
    )
    for match in pattern.finditer(compact):
        word = match.group()
        if "比" in word or word == "同比增减":
            roles.append("ignore")
        elif word == "营业收入":
            roles.append("revenue")
        elif word == "营业成本":
            roles.append("cost")
        elif word == "毛利率":
            roles.append("margin")
    return tuple(roles)


def _is_period_header(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    if _first_money(line) is not None or "毛利率" in compact or "金额" in compact:
        return False
    current = "2025" in compact or "本期" in compact
    prior = "2024" in compact or "上期" in compact or "上年" in compact
    return current and prior


def _table_section(line: str) -> str | None:
    compact = re.sub(r"\s+", "", line)
    if _first_money(line) is not None or len(compact) > 40 or "变动" in compact:
        return None
    if "营业收入和营业成本" in compact and "分解" not in compact:
        return "营业收入和营业成本"
    if "营业收入构成" in compact:
        return "营业收入构成"
    if "收入构成" in compact and "营业收入" not in compact:
        return "收入构成"
    if "营业成本构成" in compact:
        return "营业成本构成"
    return None


def _is_new_clause(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    if (
        not compact
        or _first_money(line) is not None
        or _table_section(line) is not None
        or _dimension_label(line) is not None
        or _column_section(line) is not None
    ):
        return False
    return re.match(r"^(?:（\d+）|\(\d+\)|\d+[）)、.]|\d+、)", compact) is not None


def _is_role_word_line(compact: str) -> bool:
    remainder = compact
    for word in (
        "营业收入",
        "营业成本",
        "毛利率",
        "收入",
        "成本",
        "金额",
        "比重",
        "项目",
    ):
        remainder = remainder.replace(word, "")
    remainder = re.sub(r"[%（）()比上年同期增减发生额本期上期年月日]", "", remainder)
    return remainder == "" and bool(compact)


def _dimension_on_header(line: str) -> str | None:
    compact = re.sub(r"\s+", "", line)
    found = [mark for mark in _DIMENSION_MARKS if mark in compact]
    if len(found) == 1:
        return found[0]
    return None


def _dimension_label(line: str) -> str | None:
    compact = re.sub(r"\s+", "", line).strip("：:")
    found = [mark for mark in _DIMENSION_MARKS if mark in compact]
    if len(found) != 1:
        return None
    remainder = compact.replace(found[0], "", 1)
    remainder = re.sub(r"主营业务|情况|分析", "", remainder)
    if remainder:
        return None
    return found[0]


def _is_metric_header(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    if _first_money(line) is not None:
        return False
    if "占营业收入比重" in compact and "营业成本" not in compact:
        return True
    return "营业收入" in compact and "营业成本" in compact and "毛利率" in compact


def _is_column_header(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    if "营业收入" in compact or "营业成本" in compact or "毛利率" in compact:
        return False
    return _ELIMINATION.search(compact) is not None and "项目" in compact


def _column_names(line: str) -> tuple[str, ...]:
    normalized = re.sub(r"合\s*计", "合计", re.sub(r"项\s*目", "项目", line))
    tokens = normalized.split()
    if tokens and tokens[0] == "项目":
        tokens = tokens[1:]
    return tuple(tokens)


def _column_section(line: str) -> str | None:
    compact = re.sub(r"[\s0-9.．（）()]", "", line)
    if _first_money(line) is not None or _ELIMINATION.search(compact):
        return None
    if compact.endswith("地区分部"):
        return "地区分部"
    if compact.endswith("业务分部"):
        return "业务分部"
    if compact.endswith("报告分部的财务信息"):
        return "报告分部的财务信息"
    return None


def _column_field(label: str) -> str | None:
    compact = re.sub(r"\s+", "", label)
    if compact == "营业收入":
        return "operating_revenue"
    if compact == "营业成本":
        return "operating_cost"
    if compact == "毛利率":
        return "gross_margin_reported"
    return None


def _label_and_tail(line: str) -> tuple[str | None, list[str]]:
    tokens = line.split()
    for index, token in enumerate(tokens):
        if _is_money(token) or token in _DASHES or token.endswith("%"):
            label = "".join(tokens[:index])
            if (
                len(tokens[:index]) >= 2
                and tokens[index - 1] == "营业成本"
                and tokens[0] != "营业成本"
            ):
                label = tokens[0]
            if not label:
                return None, []
            return label, tokens[index:]
    return None, []


def _is_money(token: str) -> bool:
    if token in _DASHES or token.endswith("%"):
        return False
    if re.fullmatch(r"(?:19|20)\d{2}", token):
        return False
    if not re.fullmatch(r"-?[\d,]+(?:\.\d+)?", token):
        return False
    if "," in token:
        return True
    return abs(float(token)) >= 1000


def _first_money(line: str) -> str | None:
    for token in line.split():
        if _is_money(token):
            return token
    return None


def _unit(line: str) -> str | None:
    match = re.search(r"单位[:：]\s*(?:人民币)?(百万元|千元|亿元|万元|元)", line)
    if match is None:
        return None
    return match.group(1)


def _stopped(line: str) -> bool:
    compact = re.sub(r"\s+", "", line)
    return any(token in compact for token in _STOP)


def _sentence(text: str, token: str) -> str:
    compact = re.sub(r"\s+", "", text)
    if token not in compact:
        return text.strip()[:80] or " "
    start = max(0, compact.find(token) - 12)
    end = min(len(compact), compact.find(token) + len(token) + 24)
    return compact[start:end]
