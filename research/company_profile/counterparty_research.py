"""Single-chapter research path for customers, suppliers, and concentration.

The path reuses Stage 5 page preparation and the research isolation bundle.
It reads only the counterparty section. A top-five total or a related-party
share stays a concentration Measurement. A rank, anonymous label, or aggregate
name stays a Relationship. Equal amounts do not merge those identities.
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
from typing import Literal

from pydantic import BaseModel, ConfigDict

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
    IdentityClass,
    LogicalSlot,
    Measurement,
    MetricType,
    ObjectType,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from .stage5 import EvidenceScopePlan, Stage5EvidencePreparer, Stage5ReportAsset
from .stage5_bundle import Stage5RunBundleStore
from .workflow import CompanyProfileSemanticService

COUNTERPARTY_PLAN_VERSION = "manufacturing_materials_stage4_counterparties.2026-09-29.1"
COUNTERPARTY_CHAPTER = ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION
_SCHEMA = "company_profile_counterparty_research.v1"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_DOSSIER = "openspec/changes/hold-stage4-expansion-and-review-next-scope/design.md"
_FIELDS = (
    "counterparty_relationship",
    "customer_concentration",
    "supplier_concentration",
)
_SALE = r"销\s*售"
_BUY = r"采\s*购"
_VERB = rf"(?:{_SALE}|{_BUY})"
_MARK = r"(?:\uf052|√|☑|■)"
_RANK = r"第[一二三四五]名"
_LETTER = r"[A-Z]\s*公司"
_MASKED_CUSTOMER = r"客户\s*[A-Z](?:\(\s*\d+\s*\))?"
_MASKED_SUPPLIER = r"供应商\s*[A-Z](?:\(\s*\d+\s*\))?"
_AGGREGATE = r"[\u4e00-\u9fff（）()]{2,40}?(?:同一控制\s*下企业|及其控制的企业)"
_ROW = re.compile(
    rf"(?P<name>{_RANK}|{_MASKED_CUSTOMER}|{_MASKED_SUPPLIER}|{_LETTER}|{_AGGREGATE})"
    rf"\s+(?P<amount>\d{{1,3}}(?:,\d{{3}})+(?:\.\d+)?)"
    rf"\s+(?P<share>\d+(?:\.\d+)?)\s*%"
    rf"(?:\s+(?P<related>是|否))?"
)
_CONTRACT = re.compile(
    rf"(?P<name>{_MASKED_CUSTOMER}|{_MASKED_SUPPLIER})"
    rf"\s+-\s+(?P<amount>\d{{1,3}}(?:,\d{{3}})+)"
)
_RELATED = re.compile(
    rf"前五名(?P<side>客户|供应商)(?:合计)?(?:{_VERB})额中关联\s*方(?:{_VERB})额"
    rf"(?:（(?P<unit>亿元|万元|千元|元)）)?"
    rf"(?:(?P<amount>\d{{1,3}}(?:,\d{{3}})+(?:\.\d+)?|\d+(?:\.\d+)?)(?P<unit2>亿元|万元|千元|元))?"
    rf"[，、]?(?:占年度(?:{_SALE}|{_BUY})总额(?:比例)?\s*(?P<share>\d+(?:\.\d+)?)%)?"
)
_FIELD_AMOUNT = re.compile(
    rf"前五名(?P<side>客户|供应商)合计(?:{_VERB})金额"
    rf"（(?P<unit>亿元|万元|千元|元)）\s*"
    rf"(?P<amount>\d{{1,3}}(?:,\d{{3}})+(?:\.\d+)?)"
)
_PROSE = re.compile(
    rf"前五名(?P<side>客户|供应商)(?:{_VERB})额"
    rf"(?P<amount>\d{{1,3}}(?:,\d{{3}})+|\d+(?:\.\d+)?)"
    rf"(?P<unit>亿元|万元|千元|元)，占年度(?:{_SALE}|{_BUY})总额"
    rf"(?P<share>\d+(?:\.\d+)?)%"
)
_SHARE = re.compile(
    rf"前五名(?P<side>客户|供应商)合计(?:{_VERB})(?:金额|额)"
    rf"占年度(?:{_SALE}|{_BUY})总额比例\s*(?P<share>\d+(?:\.\d+)?)%"
)
_TOTAL_ROW = re.compile(
    r"合计\s+(?:--\s+)?(?P<amount>\d{1,3}(?:,\d{3})+(?:\.\d+)?)\s+"
    r"(?P<share>\d+(?:\.\d+)?)\s*%"
)
_BARE_AMOUNT = re.compile(
    rf"前五名(?P<side>客户|供应商)合计(?:{_VERB})金额\s+"
    rf"(?P<amount>\d{{1,3}}(?:,\d{{3}})+(?:\.\d+)?|\d+(?:\.\d+)?)"
)
_UNIT = re.compile(r"单位[：:]\s*(亿元|万元|千元|元)|（(亿元|万元|千元|元)）")
_HEADINGS: tuple[tuple[str, str], ...] = (
    ("joint_contract", "重大销售合同、重大采购合同截至本报告期的履行情况"),
    ("sales_contract", "已签订的重大销售合同截至本报告期的履行情况"),
    ("procurement_contract", "已签订的重大采购合同截至本报告期的履行情况"),
    ("combined", "主要销售客户及主要供应商情况"),
    ("combined", "主要销售客户和主要供应商情况"),
    ("customer_other", "主要客户其他情况说明"),
    ("supplier_other", "主要供应商其他情况说明"),
    ("customer", "公司主要销售客户情况"),
    ("supplier", "公司主要供应商情况"),
    ("customer", "主要客户情况"),
    ("supplier", "主要供应商情况"),
    ("trade", "贸易业务收入占营业收入比例超过"),
    ("stop", "现金流量状况"),
    ("stop", "研发投入"),
    ("stop", "信用风险"),
    ("stop", "其他应收款"),
    ("stop", "关联交易"),
    ("stop", "3、费用"),
)


class CounterpartyResearchError(RuntimeError):
    """The counterparty research path left its single-chapter boundary."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True)
class _ScopeBinding:
    scope_id: str
    pages: tuple[int, ...]
    section_title: str
    anchor_terms: tuple[str, ...]


@dataclass(frozen=True)
class CounterpartyReportBinding:
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
class CounterpartyHit:
    page: int
    object_type: Literal["Relationship", "Measurement"]
    field_id: str
    relation_type: Literal["customer", "supplier"]
    name: str
    value: str | None
    unit: str | None
    quote: str
    identity_class: str | None = None
    metric_type: str | None = None
    relationship_context: str | None = None
    related_party: str | None = None


@dataclass
class CounterpartyCoverage:
    page: int
    field_id: str
    status: CoverageStatus
    reason_code: CoverageReasonCode
    reason: str
    quote: str
    relation_type: str = ""
    label: str = ""


@dataclass
class _Open:
    mode: str | None = None
    customer_rows: int = 0
    supplier_rows: int = 0
    customer_concentration: bool = False
    supplier_concentration: bool = False
    customer_page: int = 0
    supplier_page: int = 0
    customer_quote: str = ""
    supplier_quote: str = ""
    customer_unit: str | None = None
    supplier_unit: str | None = None
    seen: set[tuple[str, str, str, str]] = field(default_factory=set)


class CounterpartyResearchFact(_StrictModel):
    sample_id: str
    record_id: str
    object_type: Literal["Relationship", "Measurement"]
    field_id: str
    relation_type: Literal["customer", "supplier"]
    name: str
    identity_class: str | None = None
    metric_type: str | None = None
    value: str | None = None
    unit: str | None = None
    relationship_context: str | None = None
    related_party: str | None = None
    page: int
    bounded_quote: str
    subject_scope: Literal["unclear"] = "unclear"
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    chapter_task: Literal["extract_counterparties_and_concentration"] = (
        COUNTERPARTY_CHAPTER.value
    )


class CounterpartyCoverageFact(_StrictModel):
    sample_id: str
    field_id: str
    relation_type: str = ""
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


class CounterpartyReportRecord(_StrictModel):
    sample_id: str
    instrument_id: str
    exchange: Literal["SSE", "SZSE", "BSE"]
    report_id: str
    document_version: str
    content_hash: str


class CounterpartyResearchBundle(_StrictModel):
    schema_version: Literal["company_profile_counterparty_research.v1"] = _SCHEMA
    plan_version: str = COUNTERPARTY_PLAN_VERSION
    run_id: str
    chapter_task: Literal["extract_counterparties_and_concentration"] = (
        COUNTERPARTY_CHAPTER.value
    )
    disposition: Literal["accepted_for_review"] = "accepted_for_review"
    provider_calls: Literal[0] = 0
    production_authorization: Literal["not_authorized"] = "not_authorized"
    processing_identity: dict[str, str]
    reports: tuple[CounterpartyReportRecord, ...]
    facts: tuple[CounterpartyResearchFact, ...] = ()
    coverage: tuple[CounterpartyCoverageFact, ...] = ()


def counterparty_research_bindings() -> tuple[CounterpartyReportBinding, ...]:
    """Return the four approved reports as one counterparty sample."""

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
            content_length=2043710,
            page_count=232,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "300750-counterparties",
                    (27, 28),
                    "主要销售客户和主要供应商情况",
                    ("重大销售合同", "主要销售客户", "前五名供应商"),
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
            content_length=1740667,
            page_count=203,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "603659-counterparties",
                    (21, 22),
                    "主要销售客户及主要供应商情况",
                    ("主要销售客户及主要供应商情况", "前五名客户"),
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
            content_length=1845726,
            page_count=143,
            regime_type="stable",
            regime_effective_period="2025",
            scopes=(
                _ScopeBinding(
                    "920015-counterparties",
                    (17, 18),
                    "主要客户情况",
                    ("主要客户情况", "主要供应商情况"),
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
            content_length=1721821,
            page_count=186,
            regime_type="restructuring",
            regime_effective_period="2025-01-06 onward",
            scopes=(
                _ScopeBinding(
                    "302132-counterparties",
                    (15, 16),
                    "主要销售客户和主要供应商情况",
                    ("重大销售合同", "前五名客户"),
                ),
            ),
        ),
    )


def interpret_counterparty_pages(
    pages: tuple[tuple[int, str], ...] | list[tuple[int, str]],
) -> tuple[tuple[CounterpartyHit, ...], tuple[CounterpartyCoverage, ...]]:
    """Read one counterparty section. Do not merge identities that share an amount."""

    state = _Open()
    hits: list[CounterpartyHit] = []
    coverages: list[CounterpartyCoverage] = []
    for page, text in pages:
        if not text.strip():
            coverages.append(
                _coverage(
                    page,
                    "counterparty_relationship",
                    CoverageStatus.EXTRACTION_FAILED,
                    CoverageReasonCode.SOURCE_UNREADABLE,
                    "page text is empty",
                    "page text is empty",
                )
            )
            continue
        _consume_page(state, page, _normalize(text), hits, coverages)
    _flush(state, coverages)
    return tuple(hits), tuple(coverages)


def build_counterparty_research_bundle(
    *,
    repository_root: Path,
    run_id: str,
    preparer: Stage5EvidencePreparer | None = None,
    bindings: tuple[CounterpartyReportBinding, ...] | None = None,
    plan_version: str = COUNTERPARTY_PLAN_VERSION,
) -> CounterpartyResearchBundle:
    """Prepare one chapter through Stage 5 and keep the review disposition."""

    selected = counterparty_research_bindings() if bindings is None else bindings
    _require_known_sample(selected)
    active = preparer or Stage5EvidencePreparer()
    facts: list[CounterpartyResearchFact] = []
    coverage_facts: list[CounterpartyCoverageFact] = []
    reports: list[CounterpartyReportRecord] = []
    for binding in selected:
        pdf = repository_root / binding.relative_pdf_path
        digest = hashlib.sha256(pdf.read_bytes()).hexdigest()
        if digest != binding.content_hash:
            raise CounterpartyResearchError(
                f"{binding.instrument_id} PDF hash does not match the frozen binding"
            )
        pages: list[tuple[int, str]] = []
        for spec in binding.scopes:
            prepared = active.prepare_single_chapter(
                asset=_asset(binding, repository_root),
                chapter_task=COUNTERPARTY_CHAPTER,
                scopes=(_scope_plan(spec),),
                plan_version=plan_version,
            )
            if len(prepared) != 1 or prepared[0].chapter_task is not (
                COUNTERPARTY_CHAPTER
            ):
                raise CounterpartyResearchError(
                    "research scope prepared a second chapter"
                )
            if prepared[0].plan_version != plan_version:
                raise CounterpartyResearchError(
                    "stage5 preparation returned a different plan version"
                )
            for page in prepared[0].page_contexts:
                pages.append((page.page, page.text))
        hits, coverages = interpret_counterparty_pages(tuple(pages))
        accepted = _accept(binding, hits) if hits else ()
        facts.extend(_fact(binding.sample_id, item) for item in accepted)
        coverage_facts.extend(
            _coverage_fact(binding.sample_id, item) for item in coverages
        )
        reports.append(
            CounterpartyReportRecord(
                sample_id=binding.sample_id,
                instrument_id=binding.instrument_id,
                exchange=binding.exchange,
                report_id=binding.report_id,
                document_version=binding.document_version,
                content_hash=binding.content_hash,
            )
        )
    return CounterpartyResearchBundle(
        run_id=run_id,
        plan_version=plan_version,
        processing_identity=default_processing_identity(),
        reports=tuple(reports),
        facts=tuple(facts),
        coverage=tuple(coverage_facts),
    )


def commit_counterparty_research(
    output_root: str | Path,
    *,
    repository_root: str | Path | None = None,
    run_id: str = "stage4-counterparties",
    preparer: Stage5EvidencePreparer | None = None,
    bindings: tuple[CounterpartyReportBinding, ...] | None = None,
    plan_version: str = COUNTERPARTY_PLAN_VERSION,
) -> Path:
    """Write one accepted_for_review bundle outside common-core production."""

    root = Path(repository_root) if repository_root is not None else _REPOSITORY_ROOT
    store = Stage5RunBundleStore(output_root, repository_root=root)
    bundle = build_counterparty_research_bundle(
        repository_root=root,
        run_id=run_id,
        preparer=preparer,
        bindings=bindings,
        plan_version=plan_version,
    )
    destination = store.output_root / f"counterparty-{bundle.run_id}"
    if destination.exists():
        raise FileExistsError(
            f"counterparty research run already exists: {bundle.run_id}"
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
            raise CounterpartyResearchError(
                "counterparty bundle preset a review metric"
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


def _binding(
    *,
    sample_id: str,
    company_name: str,
    exchange: Literal["SSE", "SZSE", "BSE"],
    instrument_id: str,
    report_id: str,
    document_version: str,
    published_at: str,
    content_hash: str,
    relative_pdf_path: str,
    content_length: int,
    page_count: int,
    regime_type: Literal["stable", "restructuring"],
    regime_effective_period: str,
    scopes: tuple[_ScopeBinding, ...],
) -> CounterpartyReportBinding:
    return CounterpartyReportBinding(
        sample_id=sample_id,
        company_name=company_name,
        exchange=exchange,
        instrument_id=instrument_id,
        report_id=report_id,
        document_version=document_version,
        published_at=published_at,
        content_hash=content_hash,
        relative_pdf_path=relative_pdf_path,
        relative_dossier_path=_DOSSIER,
        content_length=content_length,
        page_count=page_count,
        regime_type=regime_type,
        regime_effective_period=regime_effective_period,
        scopes=scopes,
    )


def _require_known_sample(bindings: tuple[CounterpartyReportBinding, ...]) -> None:
    expected = ("300750.SZ", "603659.SH", "920015.BJ", "302132.SZ")
    found = tuple(item.instrument_id for item in bindings)
    if found != expected:
        raise CounterpartyResearchError(
            "counterparty research only admits the four approved reports"
        )


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _consume_page(
    state: _Open,
    page: int,
    text: str,
    hits: list[CounterpartyHit],
    coverages: list[CounterpartyCoverage],
) -> None:
    cursor = 0
    mode = state.mode
    while cursor < len(text):
        found = _next_heading(text, cursor)
        if found is None:
            if mode:
                _parse_span(state, mode, page, text[cursor:], hits, coverages)
            break
        index, kind, phrase = found
        if index > cursor and mode:
            _parse_span(state, mode, page, text[cursor:index], hits, coverages)
        window = text[index : index + 160]
        if kind == "stop":
            _flush(state, coverages)
            mode = None
        elif kind in {
            "joint_contract",
            "sales_contract",
            "procurement_contract",
            "customer_other",
            "supplier_other",
            "trade",
        }:
            mode = _apply_special(state, kind, page, window, phrase, coverages)
        else:
            _switch(state, kind, page, phrase, coverages)
            mode = kind
        cursor = index + len(phrase)
        state.mode = mode


def _next_heading(text: str, start: int) -> tuple[int, str, str] | None:
    found: list[tuple[int, int, str, str]] = []
    for kind, phrase in _HEADINGS:
        index = text.find(phrase, start)
        if index >= 0:
            found.append((index, -len(phrase), kind, phrase))
    if not found:
        return None
    found.sort()
    index, _length, kind, phrase = found[0]
    return index, kind, phrase


def _apply_special(
    state: _Open,
    kind: str,
    page: int,
    window: str,
    phrase: str,
    coverages: list[CounterpartyCoverage],
) -> str | None:
    choice = _choice(window)
    quote = window[:80]
    if kind == "joint_contract" and choice == "no":
        _flush(state, coverages)
        _not_applicable(coverages, page, "customer", phrase, quote)
        _not_applicable(coverages, page, "supplier", phrase, quote)
        return None
    if kind == "sales_contract":
        if choice == "no":
            _not_applicable(coverages, page, "customer", phrase, quote)
            return None
        return "sales_contract"
    if kind == "procurement_contract":
        if choice == "no":
            _not_applicable(coverages, page, "supplier", phrase, quote)
            return None
        return "procurement_contract"
    if choice == "no":
        _flush(state, coverages)
        relation = "supplier" if kind == "supplier_other" else "customer"
        _not_applicable(coverages, page, relation, phrase, quote)
        return None
    return state.mode


def _switch(
    state: _Open,
    kind: str,
    page: int,
    phrase: str,
    coverages: list[CounterpartyCoverage],
) -> None:
    if state.mode in {"customer", "combined", "supplier"} and kind != state.mode:
        _flush(state, coverages)
    state.mode = kind
    if kind in {"customer", "combined"}:
        state.customer_rows = 0
        state.customer_concentration = False
        state.customer_page = page
        state.customer_quote = phrase
        state.customer_unit = None
    if kind in {"supplier", "combined"}:
        state.supplier_rows = 0
        state.supplier_concentration = False
        state.supplier_page = page
        state.supplier_quote = phrase
        state.supplier_unit = None


def _parse_span(
    state: _Open,
    mode: str,
    page: int,
    span: str,
    hits: list[CounterpartyHit],
    coverages: list[CounterpartyCoverage],
) -> None:
    if mode in {"sales_contract", "procurement_contract"}:
        _parse_contract(state, mode, page, span, hits)
        return
    if mode not in {"customer", "supplier", "combined"}:
        return
    unit = _remember_unit(state, mode, span)
    cleaned = _RELATED.sub(" ", span)
    for match in _RELATED.finditer(span):
        _related(state, page, match, hits, coverages)
    for match in _FIELD_AMOUNT.finditer(cleaned):
        _amount(state, page, match, hits, unit)
    for match in _PROSE.finditer(cleaned):
        _prose(state, page, match, hits)
    for match in _SHARE.finditer(cleaned):
        _share(state, page, match, hits)
    if mode != "combined":
        for match in _TOTAL_ROW.finditer(cleaned):
            _total_row(state, mode, page, match, hits, unit)
    for match in _BARE_AMOUNT.finditer(cleaned):
        _bare_amount(state, page, match, span, coverages)
    for match in _ROW.finditer(span):
        _rank_row(state, mode, page, match, hits, unit)


def _parse_contract(
    state: _Open,
    mode: str,
    page: int,
    span: str,
    hits: list[CounterpartyHit],
) -> None:
    relation = "supplier" if mode == "procurement_contract" else "customer"
    unit = _section_unit(span)
    seen: set[str] = set()
    for match in _CONTRACT.finditer(span):
        name = _clean_name(match.group("name"))
        if name in seen:
            continue
        seen.add(name)
        _add_relationship(
            state,
            page,
            relation,
            name,
            match.group("amount"),
            unit,
            match.group(0),
            "contract",
            None,
            hits,
        )


def _related(
    state: _Open,
    page: int,
    match: re.Match[str],
    hits: list[CounterpartyHit],
    coverages: list[CounterpartyCoverage],
) -> None:
    relation = _relation(match.group("side"))
    unit = match.group("unit") or match.group("unit2")
    amount = match.group("amount")
    share = match.group("share")
    label = "关联方销售额" if relation == "customer" else "关联方采购额"
    if amount and unit:
        _add_measurement(
            state,
            page,
            relation,
            label,
            amount,
            unit,
            "customer_sales_amount"
            if relation == "customer"
            else "supplier_purchase_amount",
            f"related_party_{relation}s",
            match.group(0),
            hits,
        )
    elif amount and not unit:
        coverages.append(
            _coverage(
                page,
                f"{relation}_concentration",
                CoverageStatus.UNCLEAR,
                CoverageReasonCode.UNIT_AMBIGUOUS,
                "related-party amount has no unit",
                match.group(0),
                relation,
                label,
            )
        )
    if share:
        _add_measurement(
            state,
            page,
            relation,
            label,
            share,
            "%",
            "disclosed_share",
            f"related_party_{relation}s",
            match.group(0),
            hits,
        )


def _amount(
    state: _Open,
    page: int,
    match: re.Match[str],
    hits: list[CounterpartyHit],
    unit: str | None,
) -> None:
    relation = _relation(match.group("side"))
    chosen = match.group("unit") or unit
    label = "前五名客户合计" if relation == "customer" else "前五名供应商合计"
    if not chosen:
        return
    _add_measurement(
        state,
        page,
        relation,
        label,
        match.group("amount"),
        chosen,
        "customer_sales_amount"
        if relation == "customer"
        else "supplier_purchase_amount",
        f"top_five_{relation}s",
        match.group(0),
        hits,
    )


def _prose(
    state: _Open,
    page: int,
    match: re.Match[str],
    hits: list[CounterpartyHit],
) -> None:
    relation = _relation(match.group("side"))
    label = "前五名客户合计" if relation == "customer" else "前五名供应商合计"
    metric = (
        "customer_sales_amount"
        if relation == "customer"
        else "supplier_purchase_amount"
    )
    _add_measurement(
        state,
        page,
        relation,
        label,
        match.group("amount"),
        match.group("unit"),
        metric,
        f"top_five_{relation}s",
        match.group(0),
        hits,
    )
    _add_measurement(
        state,
        page,
        relation,
        label,
        match.group("share"),
        "%",
        "disclosed_share",
        f"top_five_{relation}s",
        match.group(0),
        hits,
    )


def _share(
    state: _Open,
    page: int,
    match: re.Match[str],
    hits: list[CounterpartyHit],
) -> None:
    relation = _relation(match.group("side"))
    label = "前五名客户合计" if relation == "customer" else "前五名供应商合计"
    _add_measurement(
        state,
        page,
        relation,
        label,
        match.group("share"),
        "%",
        "disclosed_share",
        f"top_five_{relation}s",
        match.group(0),
        hits,
    )


def _total_row(
    state: _Open,
    mode: str,
    page: int,
    match: re.Match[str],
    hits: list[CounterpartyHit],
    unit: str | None,
) -> None:
    relation = "supplier" if mode == "supplier" else "customer"
    label = "前五名客户合计" if relation == "customer" else "前五名供应商合计"
    if unit:
        _add_measurement(
            state,
            page,
            relation,
            label,
            match.group("amount"),
            unit,
            "customer_sales_amount"
            if relation == "customer"
            else "supplier_purchase_amount",
            f"top_five_{relation}s",
            match.group(0),
            hits,
        )
    _add_measurement(
        state,
        page,
        relation,
        label,
        match.group("share"),
        "%",
        "disclosed_share",
        f"top_five_{relation}s",
        match.group(0),
        hits,
    )


def _bare_amount(
    state: _Open,
    page: int,
    match: re.Match[str],
    span: str,
    coverages: list[CounterpartyCoverage],
) -> None:
    if _FIELD_AMOUNT.search(match.group(0)) or _section_unit(match.group(0)):
        return
    if _section_unit(span):
        return
    relation = _relation(match.group("side"))
    label = "前五名客户合计" if relation == "customer" else "前五名供应商合计"
    _mark_concentration(state, relation, page, match.group(0))
    coverages.append(
        _coverage(
            page,
            f"{relation}_concentration",
            CoverageStatus.UNCLEAR,
            CoverageReasonCode.UNIT_AMBIGUOUS,
            "top-five amount has no unit",
            match.group(0),
            relation,
            label,
        )
    )


def _rank_row(
    state: _Open,
    mode: str,
    page: int,
    match: re.Match[str],
    hits: list[CounterpartyHit],
    unit: str | None,
) -> None:
    name = _clean_name(match.group("name"))
    relation = _relation_for_name(mode, name)
    _add_relationship(
        state,
        page,
        relation,
        name,
        match.group("amount"),
        unit,
        match.group(0),
        "rank",
        match.group("related"),
        hits,
    )


def _add_relationship(
    state: _Open,
    page: int,
    relation: Literal["customer", "supplier"],
    name: str,
    amount: str | None,
    unit: str | None,
    quote: str,
    context: str,
    related: str | None,
    hits: list[CounterpartyHit],
) -> None:
    if not _remember(
        state, "relationship", relation, f"{context}:{name}", amount or ""
    ):
        return
    if relation == "customer":
        state.customer_rows += 1
        state.customer_page = state.customer_page or page
        state.customer_quote = state.customer_quote or quote
    else:
        state.supplier_rows += 1
        state.supplier_page = state.supplier_page or page
        state.supplier_quote = state.supplier_quote or quote
    hits.append(
        CounterpartyHit(
            page=page,
            object_type="Relationship",
            field_id="counterparty_relationship",
            relation_type=relation,
            name=name,
            value=amount,
            unit=unit,
            quote=quote,
            identity_class=_identity_class(name),
            relationship_context=f"{context}_{relation}",
            related_party=related,
        )
    )


def _add_measurement(
    state: _Open,
    page: int,
    relation: Literal["customer", "supplier"],
    label: str,
    value: str,
    unit: str,
    metric: str,
    context: str,
    quote: str,
    hits: list[CounterpartyHit],
) -> None:
    if not _remember(
        state, "measurement", relation, f"{context}:{metric}:{label}", value
    ):
        return
    _mark_concentration(state, relation, page, quote)
    hits.append(
        CounterpartyHit(
            page=page,
            object_type="Measurement",
            field_id=f"{relation}_concentration",
            relation_type=relation,
            name=label,
            value=value,
            unit=unit,
            quote=quote,
            metric_type=metric,
            relationship_context=context,
        )
    )


def _mark_concentration(
    state: _Open,
    relation: Literal["customer", "supplier"],
    page: int,
    quote: str,
) -> None:
    if relation == "customer":
        state.customer_concentration = True
        state.customer_page = state.customer_page or page
        state.customer_quote = state.customer_quote or quote
    else:
        state.supplier_concentration = True
        state.supplier_page = state.supplier_page or page
        state.supplier_quote = state.supplier_quote or quote


def _flush(state: _Open, coverages: list[CounterpartyCoverage]) -> None:
    if state.mode in {"customer", "combined", "supplier"}:
        _flush_side(state, "customer", coverages)
        _flush_side(state, "supplier", coverages)
    state.mode = None
    state.customer_rows = 0
    state.supplier_rows = 0
    state.customer_concentration = False
    state.supplier_concentration = False


def _flush_side(
    state: _Open,
    relation: Literal["customer", "supplier"],
    coverages: list[CounterpartyCoverage],
) -> None:
    rows = state.customer_rows if relation == "customer" else state.supplier_rows
    present = (
        state.customer_concentration
        if relation == "customer"
        else state.supplier_concentration
    )
    if state.mode == "customer" and relation == "supplier":
        return
    if state.mode == "supplier" and relation == "customer":
        return
    if not present or rows:
        return
    page = state.customer_page if relation == "customer" else state.supplier_page
    quote = state.customer_quote if relation == "customer" else state.supplier_quote
    coverages.append(
        _coverage(
            page,
            "counterparty_relationship",
            CoverageStatus.NOT_DISCLOSED,
            CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
            "top-five section has no counterparty row",
            quote or "前五名",
            relation,
            "前五名客户" if relation == "customer" else "前五名供应商",
        )
    )


def _not_applicable(
    coverages: list[CounterpartyCoverage],
    page: int,
    relation: str,
    label: str,
    quote: str,
) -> None:
    coverages.append(
        _coverage(
            page,
            "counterparty_relationship",
            CoverageStatus.NOT_APPLICABLE,
            CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE,
            "section is marked inapplicable",
            quote,
            relation,
            label,
        )
    )


def _coverage(
    page: int,
    field_id: str,
    status: CoverageStatus,
    reason_code: CoverageReasonCode,
    reason: str,
    quote: str,
    relation: str = "",
    label: str = "",
) -> CounterpartyCoverage:
    return CounterpartyCoverage(
        page=page,
        field_id=field_id,
        status=status,
        reason_code=reason_code,
        reason=reason,
        quote=quote,
        relation_type=relation,
        label=label,
    )


def _choice(window: str) -> str | None:
    head = window[:80]
    negative = re.search(_MARK + "不适用", head)
    positive = re.search(_MARK + "适用", head)
    if negative and (positive is None or negative.start() <= positive.start()):
        return "no"
    if positive:
        return "yes"
    return None


def _remember_unit(state: _Open, mode: str, span: str) -> str | None:
    found = _section_unit(span)
    if mode == "supplier":
        if found:
            state.supplier_unit = found
        return found or state.supplier_unit
    if mode in {"customer", "sales_contract"}:
        if found:
            state.customer_unit = found
        return found or state.customer_unit
    if mode == "procurement_contract":
        if found:
            state.supplier_unit = found
        return found or state.supplier_unit
    return found


def _section_unit(text: str) -> str | None:
    match = _UNIT.search(text)
    if match is None:
        return None
    return match.group(1) or match.group(2)


def _relation(side: str) -> Literal["customer", "supplier"]:
    return "customer" if side == "客户" else "supplier"


def _relation_for_name(mode: str, name: str) -> Literal["customer", "supplier"]:
    compact = re.sub(r"\s+", "", name)
    if compact.startswith("供应商"):
        return "supplier"
    if compact.startswith("客户"):
        return "customer"
    if mode == "supplier":
        return "supplier"
    return "customer"


def _identity_class(name: str) -> str:
    compact = re.sub(r"\s+", "", name)
    if "同一控制" in compact or "及其控制" in compact or compact.endswith("控制的企业"):
        return "report_local_aggregate"
    if re.fullmatch(r"第[一二三四五]名", compact):
        return "report_local_anonymous"
    if re.fullmatch(r"(?:客户|供应商)[A-Z](?:\(\d+\))?", compact):
        return "report_local_anonymous"
    if re.fullmatch(r"[A-Z]公司", compact):
        return "report_local_anonymous"
    return "named"


def _clean_name(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip()


def _remember(state: _Open, kind: str, relation: str, label: str, value: str) -> bool:
    key = (kind, relation, label, value)
    if key in state.seen:
        return False
    state.seen.add(key)
    return True


def _accept(
    binding: CounterpartyReportBinding,
    hits: tuple[CounterpartyHit, ...],
) -> tuple[Relationship | Measurement, ...]:
    report = _identity(binding)
    prepared: list[PreparedEvidence] = []
    candidates: list[Relationship | Measurement] = []
    for hit in hits:
        evidence = _evidence(report, hit)
        prepared.append(
            PreparedEvidence(
                evidence=evidence,
                field_id=hit.field_id,
                source_readable=True,
            )
        )
        if hit.object_type == "Relationship":
            candidates.append(_relationship(report, binding.sample_id, hit, evidence))
        else:
            candidates.append(_measurement(report, binding.sample_id, hit, evidence))
    request = SemanticTaskRequest(
        request_id=f"{binding.sample_id}:extract_counterparties_and_concentration",
        report=report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version=COUNTERPARTY_PLAN_VERSION,
            report=report,
            checklist=tuple(_checklist(field_id) for field_id in _FIELDS),
        ),
        chapter_task=COUNTERPARTY_CHAPTER,
        evidence_bundle=tuple(prepared),
        allowed_object_types=(ObjectType.RELATIONSHIP, ObjectType.MEASUREMENT),
        allowed_metric_types=(
            MetricType.CUSTOMER_SALES_AMOUNT,
            MetricType.SUPPLIER_PURCHASE_AMOUNT,
            MetricType.DISCLOSED_SHARE,
        ),
        prohibited_inferences=(
            "merge_anonymous_identities_across_reports",
            "merge_customer_and_supplier_anonymous_identities",
            "merge_contract_and_rank_by_equal_amount",
            "concentration_creates_relationship",
            "backfill_names_from_related_party_or_credit_tables",
        ),
        deterministic_candidates=tuple(candidates),
        provided_coverage=(),
        unresolved_field_ids=(),
    )
    result = CompanyProfileSemanticService().run_task(request, provider=None)
    if result.provider_calls:
        raise CounterpartyResearchError("counterparty research called a provider")
    refused = [
        item.status.value
        for item in result.dispositions
        if item.status.value != "accepted_for_review"
    ]
    if refused:
        raise CounterpartyResearchError(
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


def _relationship(
    report: ReportIdentity,
    sample_id: str,
    hit: CounterpartyHit,
    evidence: Evidence,
) -> Relationship:
    return Relationship(
        record_id=_record_id(sample_id, hit),
        field_id="counterparty_relationship",
        chapter_task=COUNTERPARTY_CHAPTER,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        subject_basis=SubjectBasis.UNCLEAR,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(
            name=hit.name,
            value=hit.value,
            unit=hit.unit,
            header=hit.relationship_context,
            qualifier=hit.related_party,
        ),
        relation_type=(
            RelationshipType.CUSTOMER
            if hit.relation_type == "customer"
            else RelationshipType.SUPPLIER
        ),
        object_name=hit.name,
        identity_class=IdentityClass(hit.identity_class or ""),
    )


def _measurement(
    report: ReportIdentity,
    sample_id: str,
    hit: CounterpartyHit,
    evidence: Evidence,
) -> Measurement:
    metric = MetricType(hit.metric_type or "")
    slot = {
        MetricType.CUSTOMER_SALES_AMOUNT: LogicalSlot.CUSTOMER_SALES_AMOUNT,
        MetricType.SUPPLIER_PURCHASE_AMOUNT: LogicalSlot.SUPPLIER_PURCHASE_AMOUNT,
        MetricType.DISCLOSED_SHARE: LogicalSlot.DISCLOSED_SHARE,
    }[metric]
    return Measurement(
        record_id=_record_id(sample_id, hit),
        field_id=hit.field_id,
        chapter_task=COUNTERPARTY_CHAPTER,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        subject_basis=SubjectBasis.UNCLEAR,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(
            name=hit.name,
            value=hit.value,
            unit=hit.unit,
            header=hit.relationship_context,
        ),
        metric_type=metric,
        logical_slot=slot,
        measured_object=hit.name,
        relationship_context=hit.relationship_context,
    )


def _identity(binding: CounterpartyReportBinding) -> ReportIdentity:
    return ReportIdentity(
        instrument_id=binding.instrument_id,
        report_id=binding.report_id,
        document_version=binding.document_version,
        report_period="2025-12-31",
        published_at=binding.published_at,
    )


def _evidence(report: ReportIdentity, hit: CounterpartyHit) -> Evidence:
    return Evidence(
        evidence_id=(
            "stage5-evidence-"
            + hashlib.sha256(
                f"{hit.page}:{hit.field_id}:{hit.relation_type}:{hit.name}:{hit.quote}".encode()
            ).hexdigest()[:24]
        ),
        report=report,
        page=hit.page,
        section_title=hit.relationship_context or hit.field_id,
        anchor=TextAnchor(bounded_quote=hit.quote),
    )


def _checklist(field_id: str) -> ChecklistItem:
    if field_id == "counterparty_relationship":
        return ChecklistItem(
            field_id=field_id,
            object_type=ObjectType.RELATIONSHIP,
            chapter_task=COUNTERPARTY_CHAPTER,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
        )
    metrics = (
        (MetricType.CUSTOMER_SALES_AMOUNT, MetricType.DISCLOSED_SHARE)
        if field_id == "customer_concentration"
        else (MetricType.SUPPLIER_PURCHASE_AMOUNT, MetricType.DISCLOSED_SHARE)
    )
    return ChecklistItem(
        field_id=field_id,
        object_type=ObjectType.MEASUREMENT,
        chapter_task=COUNTERPARTY_CHAPTER,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=metrics,
    )


def _record_id(sample_id: str, hit: CounterpartyHit) -> str:
    material = "|".join(
        (
            sample_id,
            hit.object_type,
            hit.relation_type,
            hit.relationship_context or "",
            hit.name,
            hit.metric_type or "",
            hit.value or "",
            str(hit.page),
        )
    )
    return "counterparty-" + hashlib.sha256(material.encode()).hexdigest()[:24]


def _fact(
    sample_id: str, record: Relationship | Measurement
) -> CounterpartyResearchFact:
    evidence = record.evidence[0]
    quote = (
        evidence.anchor.bounded_quote if isinstance(evidence.anchor, TextAnchor) else ""
    )
    if isinstance(record, Relationship):
        return CounterpartyResearchFact(
            sample_id=sample_id,
            record_id=record.record_id,
            object_type="Relationship",
            field_id=record.field_id,
            relation_type=(
                "customer"
                if record.relation_type == RelationshipType.CUSTOMER
                else "supplier"
            ),
            name=record.object_name,
            identity_class=(
                record.identity_class.value if record.identity_class else None
            ),
            value=record.source_native.value,
            unit=record.source_native.unit,
            relationship_context=record.source_native.header,
            related_party=record.source_native.qualifier,
            page=evidence.page,
            bounded_quote=quote,
        )
    return CounterpartyResearchFact(
        sample_id=sample_id,
        record_id=record.record_id,
        object_type="Measurement",
        field_id=record.field_id,
        relation_type=(
            "customer" if record.field_id == "customer_concentration" else "supplier"
        ),
        name=record.measured_object,
        metric_type=record.metric_type.value,
        value=record.source_native.value,
        unit=record.source_native.unit,
        relationship_context=record.relationship_context,
        page=evidence.page,
        bounded_quote=quote,
    )


def _coverage_fact(
    sample_id: str, item: CounterpartyCoverage
) -> CounterpartyCoverageFact:
    return CounterpartyCoverageFact(
        sample_id=sample_id,
        field_id=item.field_id,
        relation_type=item.relation_type,
        label=item.label,
        page=item.page,
        coverage_status=item.status.value,
        reason=item.reason,
        bounded_quote=item.quote,
    )


def _asset(
    binding: CounterpartyReportBinding, repository_root: Path
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


def _scope_plan(spec: _ScopeBinding) -> EvidenceScopePlan:
    return EvidenceScopePlan(
        scope_id=spec.scope_id,
        field_ids=_FIELDS,
        pages=spec.pages,
        section_titles=(spec.section_title,),
        anchor_terms=spec.anchor_terms,
        continuation_required=len(spec.pages) > 1,
    )
