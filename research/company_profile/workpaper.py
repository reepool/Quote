"""Evidence-linked company workpaper projection and its researcher rendering.

This is an input/output projection, not another fact store or publication owner.
Human references and model drafts use the same content contract and stay distinct.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import Evidence, ReportIdentity, TextAnchor

WORKPAPER_SCHEMA = "company_profile_workpaper.v1"
ELEMENT_LABELS = {
    "business_structure": "业务结构",
    "products_services": "产品、服务及用途",
    "revenue_model": "怎么赚钱",
    "scale_and_mix": "规模与业务权重",
    "operations_and_inputs": "经营方式与关键投入",
    "customers_channels_markets": "客户、渠道与市场",
    "operating_drivers": "关键经营影响因素",
    "material_changes": "近期重要变化",
}
SUPPLY_CHAIN_LABELS = {
    "position": "公司所处环节",
    "upstream_inputs": "上游原材料与资源",
    "downstream_customers": "下游用途与客户领域",
}
Subject = Literal[
    "consolidated_group", "issuer", "named_subsidiary", "business_segment", "unclear"
]
Status = Literal[
    "observed", "not_disclosed", "not_applicable", "extraction_failed", "unclear"
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WorkpaperSpan(_Model):
    page: int = Field(ge=1)
    start: int = Field(ge=0)
    end: int = Field(gt=0)
    quote: str = Field(min_length=1)

    @model_validator(mode="after")
    def ordered(self):
        if self.end <= self.start or self.end - self.start != len(self.quote):
            raise ValueError("source span must use original page character coordinates")
        return self


class WorkpaperEvidence(_Model):
    evidence_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    section_path: tuple[str, ...] = Field(min_length=1)
    physical_pages: tuple[int, ...] = Field(min_length=1)
    source_spans: tuple[WorkpaperSpan, ...] = Field(min_length=1)
    table_context: str | None = None

    @model_validator(mode="after")
    def pages_match(self):
        if set(self.physical_pages) != {span.page for span in self.source_spans}:
            raise ValueError("evidence pages and source spans disagree")
        return self

    def native_evidence(self, report: ReportIdentity) -> tuple[Evidence, ...]:
        """Expose the existing Evidence type without inventing canonical facts."""
        return tuple(
            Evidence(
                evidence_id=f"{self.evidence_id}:{index}",
                report=report,
                page=span.page,
                section_title=" / ".join(self.section_path),
                anchor=TextAnchor(bounded_quote=span.quote, source_offset=span.start),
            )
            for index, span in enumerate(self.source_spans)
        )


class WorkpaperMetric(_Model):
    name: str = Field(min_length=1)
    raw_value: str = Field(min_length=1)
    value: str = Field(min_length=1)
    unit: str = Field(min_length=1)
    currency: str | None = None
    subject_scope: Subject
    period: str = Field(min_length=1)
    comparison_basis: str | None = None

    @model_validator(mode="after")
    def decimal_value(self):
        raw_number = self.raw_value.strip()
        # Some real responses repeat the separately declared unit in raw_value.
        # Preserve that source spelling, but normalize only that exact suffix;
        # this does not convert units or accept other decorations/estimates.
        if raw_number.endswith(self.unit):
            raw_number = raw_number[:-len(self.unit)].strip()
        try:
            raw = Decimal(raw_number.replace(",", "").replace("，", ""))
            normalized = Decimal(self.value)
        except InvalidOperation as exc:
            raise ValueError("metric values must be decimal strings") from exc
        if not raw.is_finite() or not normalized.is_finite() or raw != normalized:
            raise ValueError("normalized value must preserve the original value and unit")
        return self


class WorkpaperFact(_Model):
    fact_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    subject_scope: Subject
    period: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    metrics: tuple[WorkpaperMetric, ...] = ()


class WorkpaperElement(_Model):
    status: Status
    facts: tuple[WorkpaperFact, ...] = ()
    limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def substantive_or_explained(self):
        if self.status == "observed" and not self.facts:
            raise ValueError("observed element requires substantive facts")
        if not self.facts and not self.limitations:
            raise ValueError("missing element requires a scope or reason")
        return self


class WorkpaperElements(_Model):
    business_structure: WorkpaperElement
    products_services: WorkpaperElement
    revenue_model: WorkpaperElement
    scale_and_mix: WorkpaperElement
    operations_and_inputs: WorkpaperElement
    customers_channels_markets: WorkpaperElement
    operating_drivers: WorkpaperElement
    material_changes: WorkpaperElement


def _missing_supply_chain_element() -> WorkpaperElement:
    return WorkpaperElement(
        status="unclear",
        limitations=("本次未返回该项供应链信息；不代表全报告未披露。",),
    )


class WorkpaperSupplyChain(_Model):
    """Optional enrichment, never a prerequisite for the eight-element profile."""

    position: WorkpaperElement = Field(default_factory=_missing_supply_chain_element)
    upstream_inputs: WorkpaperElement = Field(default_factory=_missing_supply_chain_element)
    downstream_customers: WorkpaperElement = Field(default_factory=_missing_supply_chain_element)


def _check_fact_source(fact: WorkpaperFact, sources: Mapping[str, WorkpaperEvidence]) -> None:
    if set(fact.evidence_ids) - sources.keys():
        raise ValueError("fact cites unknown evidence")
    numbers = {
        Decimal(value.replace(",", ""))
        for evidence_id in fact.evidence_ids
        for span in sources[evidence_id].source_spans
        for value in re.findall(r"(?<![\d.])-?\d[\d,]*(?:\.\d+)?(?![\d.])", span.quote)
    }
    for metric in fact.metrics:
        if Decimal(metric.value) not in numbers:
            raise ValueError(f"metric number is absent from cited source: {metric.name}")


class WorkpaperSection(_Model):
    section_id: str = Field(min_length=1)
    section_path: tuple[str, ...] = Field(min_length=1)
    start_page: int = Field(ge=1)
    end_page: int = Field(ge=1)
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def ordered_pages(self):
        if self.end_page < self.start_page:
            raise ValueError("section end precedes its start")
        return self


class WorkpaperReadingScope(_Model):
    sections: tuple[WorkpaperSection, ...] = Field(min_length=1)
    unread_or_deferred: tuple[str, ...] = ()
    selection_rationale: str = Field(min_length=1)
    stop_reason: str = Field(min_length=1)
    transcription_version: str = "source_page_text.v1"


class CompanyProfileWorkpaper(_Model):
    schema_version: Literal["company_profile_workpaper.v1"] = WORKPAPER_SCHEMA
    origin: Literal["human_reference", "llm_generated", "controlled_fixture"]
    report: ReportIdentity
    reading_scope: WorkpaperReadingScope
    elements: WorkpaperElements
    evidence: tuple[WorkpaperEvidence, ...]
    limitations: tuple[str, ...] = ()
    supply_chain: WorkpaperSupplyChain | None = None

    @model_validator(mode="after")
    def source_integrity(self):
        sources = {item.evidence_id: item for item in self.evidence}
        if len(sources) != len(self.evidence):
            raise ValueError("duplicate evidence identity")
        if any(item.document_version != self.report.document_version for item in self.evidence):
            raise ValueError("evidence document version mismatch")
        ids: set[str] = set()
        sections = [getattr(self.elements, name) for name in ELEMENT_LABELS]
        if self.supply_chain is not None:
            sections.extend(getattr(self.supply_chain, name) for name in SUPPLY_CHAIN_LABELS)
        for section in sections:
            for fact in section.facts:
                if fact.fact_id in ids:
                    raise ValueError("duplicate fact identity")
                ids.add(fact.fact_id)
                _check_fact_source(fact, sources)
        return self

    def validate_pages(self, pages: Sequence[Mapping]) -> None:
        """Check immutable source coordinates, not semantic truth or completeness."""
        by_page = {
            int(page.get("page", page.get("page_number"))): str(page["text"])
            for page in pages
        }
        for item in self.evidence:
            for span in item.source_spans:
                if by_page.get(span.page, "")[span.start:span.end] != span.quote:
                    raise ValueError(f"source span does not match supplied page: {item.evidence_id}")


class WorkpaperDraftResponse(_Model):
    """LLM response before local evidence binding and report identity binding."""

    schema_version: Literal["company_profile_workpaper_draft.v1"] = (
        "company_profile_workpaper_draft.v1"
    )
    elements: WorkpaperElements
    limitations: tuple[str, ...] = ()
    supply_chain: WorkpaperSupplyChain | None = None


def bind_workpaper_draft(
    response: WorkpaperDraftResponse,
    *,
    report: ReportIdentity,
    reading_scope: WorkpaperReadingScope,
    evidence: Sequence[WorkpaperEvidence],
    origin: Literal["llm_generated", "controlled_fixture"] = "llm_generated",
) -> CompanyProfileWorkpaper:
    """Bind a model's evidence IDs to the immutable local source packet."""
    allowed = {item.evidence_id for item in evidence}
    for name in ELEMENT_LABELS:
        for fact in getattr(response.elements, name).facts:
            if set(fact.evidence_ids) - allowed:
                raise ValueError(f"model cited evidence outside the supplied packet: {fact.fact_id}")
    supply_chain = response.supply_chain
    if supply_chain is not None:
        sources = {item.evidence_id: item for item in evidence}
        used_ids = {fact.fact_id for name in ELEMENT_LABELS for fact in getattr(response.elements, name).facts}
        cleaned = {}
        for name in SUPPLY_CHAIN_LABELS:
            element = getattr(supply_chain, name)
            kept, rejected = [], []
            for fact in element.facts:
                try:
                    if fact.fact_id in used_ids:
                        raise ValueError("duplicate fact identity")
                    _check_fact_source(fact, sources)
                except ValueError as exc:
                    rejected.append(f"可选事实{fact.fact_id}未纳入：{exc}")
                    continue
                kept.append(fact)
                used_ids.add(fact.fact_id)
            cleaned[name] = element.model_copy(update={
                "facts": tuple(kept),
                "status": "unclear" if rejected and not kept else element.status,
                "limitations": (*element.limitations, *rejected),
            })
        supply_chain = WorkpaperSupplyChain(**cleaned)
    return CompanyProfileWorkpaper(
        origin=origin,
        report=report,
        reading_scope=reading_scope,
        elements=response.elements,
        evidence=tuple(evidence),
        limitations=response.limitations,
        supply_chain=supply_chain,
    )


def render_workpaper(workpaper: CompanyProfileWorkpaper) -> str:
    """Render only the structured workpaper; never synthesize a second answer."""
    report = workpaper.report
    origin = {
        "human_reference": "人工原文参考底稿（非模型成绩）",
        "llm_generated": "真实模型底稿（待核，不代表正式公司可用）",
        "controlled_fixture": "受控工程输入（非真实模型结果）",
    }[workpaper.origin]
    out = [
        f"# {report.instrument_id}｜{report.report_period} 公司画像底稿",
        "",
        origin,
        "",
        f"来源版本：`{report.document_version}`。发布时点：{report.published_at}。",
        "",
    ]
    for name, label in ELEMENT_LABELS.items():
        element = getattr(workpaper.elements, name)
        out.extend([f"## {label}", ""])
        for fact in element.facts:
            refs = "、".join(f"[{value}](#{value.lower()})" for value in fact.evidence_ids)
            out.append(f"- {fact.text}（{refs}）")
            for metric in fact.metrics:
                out.append(f"  - {metric.name}：{metric.raw_value} {metric.unit}；{metric.period}，{metric.subject_scope}。")
        for limitation in element.limitations:
            out.append(f"- 限制：{limitation}")
        out.append("")
    if workpaper.supply_chain is not None:
        out.extend(["## 供应链信息（可选加分项）", "", "未获取到的信息不影响基础画像交付；以下内容仍待核。", ""])
        for name, label in SUPPLY_CHAIN_LABELS.items():
            element = getattr(workpaper.supply_chain, name)
            out.extend([f"### {label}", ""])
            for fact in element.facts:
                refs = "、".join(f"[{value}](#{value.lower()})" for value in fact.evidence_ids)
                out.append(f"- {fact.text}（{refs}）")
                for metric in fact.metrics:
                    out.append(f"  - {metric.name}：{metric.raw_value} {metric.unit}；{metric.period}，{metric.subject_scope}。")
            out.extend(f"- 限制：{limit}" for limit in element.limitations)
            out.append("")
    out.extend(["## 阅读范围与取舍", "", workpaper.reading_scope.selection_rationale, ""])
    for section in workpaper.reading_scope.sections:
        out.append(f"- {' / '.join(section.section_path)}，物理页{section.start_page}—{section.end_page}：{section.rationale}")
    out.extend(["", "停止理由：" + workpaper.reading_scope.stop_reason, ""])
    for scope in workpaper.reading_scope.unread_or_deferred:
        out.append("- 未读/后置：" + scope)
    for limitation in workpaper.limitations:
        out.append("- 限制：" + limitation)
    out.extend(["", "## 来源", ""])
    for item in workpaper.evidence:
        out.extend([f"### {item.evidence_id}", "", " / ".join(item.section_path)])
        for span in item.source_spans:
            quote = " ".join(span.quote.split())
            out.extend(["", f"物理页{span.page}，字符{span.start}—{span.end}：", "", f"> {quote}"])
        if item.table_context:
            out.extend(["", "表格口径：" + item.table_context])
        out.append("")
    return "\n".join(out).rstrip() + "\n"
