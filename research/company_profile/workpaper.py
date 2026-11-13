"""Evidence-linked company workpaper projection and its researcher rendering.

This is an input/output projection, not another fact store or publication owner.
Human references and model drafts use the same content contract and stay distinct.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

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
    value: str | None = Field(default=None, min_length=1)
    unit: str | None = Field(default=None, min_length=1)
    source_unit: str | None = None
    currency: str | None = None
    subject_scope: Subject
    period: str = Field(min_length=1)
    comparison_basis: str | None = None
    qualifier: Literal["exact", "approximate", "greater_than"] = "exact"
    uncertainty_reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="before")
    @classmethod
    def preserve_source_qualifier(cls, data):
        if not isinstance(data, Mapping):
            return data
        raw = str(data.get("raw_value", "")).strip()
        qualifier = ("approximate" if raw.startswith(("近", "约")) else
                     "greater_than" if raw.startswith(("超", "突破")) else "exact")
        if "qualifier" in data and data["qualifier"] != qualifier:
            raise ValueError("metric qualifier must match original raw value")
        result = {**data, "qualifier": qualifier}
        # Preserve a real returned source phrase rather than forcing a signed
        # number onto it. Only an agreeing same-unit reference can fall back;
        # a wrong sign/unit/value is still an error, not "uncertainty".
        if (raw.startswith(("增加", "减少")) and raw.endswith("个百分点")
            and data.get("unit") == "百分点"
            and data.get("source_unit") in (None, "百分点", "%", "％") and data.get("value") is not None):
            try:
                amount = Decimal(raw[2:-len("个百分点")].strip())
                reference = Decimal(str(data["value"]))
            except InvalidOperation:
                pass
            else:
                if amount.is_finite() and amount >= 0 and reference.is_finite():
                    signed = amount.copy_negate() if raw.startswith("减少") else amount
                    if reference == signed:
                        result.update(value=None, uncertainty_reason=data.get("uncertainty_reason") or
                                      "原文带增减方向及单位；保留原文，未统一为带符号数值。")
        return result

    @model_validator(mode="after")
    def decimal_value(self):
        if self.value is None:
            if not self.uncertainty_reason:
                raise ValueError("unnormalized metric requires an uncertainty reason")
            return self
        if self.unit is None:
            raise ValueError("unknown unit requires a null normalized value")
        if self.source_unit and self.source_unit != self.unit and (
            self.source_unit, self.unit
        ) not in {("%", "百分点"), ("％", "百分点")}:
            raise ValueError("source_unit cannot relabel a numeric magnitude")
        raw_number = self.raw_value.strip()
        if self.qualifier != "exact":
            raw_number = raw_number[2 if raw_number.startswith("突破") else 1:].strip()
        # Some real responses repeat the separately declared unit in raw_value.
        # Preserve that source spelling, but normalize only that exact suffix;
        # this does not convert units; estimates retain their explicit qualifier.
        raw_unit = self.source_unit or self.unit
        if raw_number.endswith(raw_unit):
            raw_number = raw_number[:-len(raw_unit)].strip()
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
    uncertainty_reason: str | None = Field(default=None, min_length=1)


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
        if metric.value is None:
            quoted = "".join(span.quote for eid in fact.evidence_ids for span in sources[eid].source_spans)
            if "".join(metric.raw_value.split()) not in "".join(quoted.split()):
                raise ValueError(f"uncertain metric original wording is absent from cited source: {metric.name}")
            continue
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


class WorkpaperModelUsage(_Model):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)


class WorkpaperModelCall(_Model):
    purpose: Literal[
        "select_profile_sections", "extract_profile_workpaper",
        "review_profile_workpaper", "repair_profile_workpaper",
        "verify_profile_workpaper_repair",
    ]
    request_id: str = Field(min_length=1)
    request_hash: str | None = None
    response_hash: str | None = None
    source_label: str | None = None
    model: str | None = None
    selected_profile: str | None = None
    logical_profile: str | None = None
    route_fingerprint: str | None = None
    finish_reason: str | None = None
    reasoning_effort: str | None = None
    contract_status: Literal["accepted", "rejected", "unknown"] = "unknown"
    usage: WorkpaperModelUsage = Field(default_factory=WorkpaperModelUsage)
    scope_kind: Literal["report_heading_index", "selected_source_sections"]
    heading_count: int | None = Field(default=None, ge=0)
    section_ids: tuple[str, ...] = ()
    physical_pages: tuple[int, ...] = ()


class WorkpaperGenerationMetadata(_Model):
    prompt_version: str = Field(min_length=1)
    preprocessing_version: str = Field(min_length=1)
    section_guide_version: str | None = None
    selection: WorkpaperModelCall
    extraction: WorkpaperModelCall

    @model_validator(mode="after")
    def stages_match(self):
        if self.selection.purpose != "select_profile_sections":
            raise ValueError("selection model attribution has wrong purpose")
        if self.extraction.purpose != "extract_profile_workpaper":
            raise ValueError("extraction model attribution has wrong purpose")
        return self


class WorkpaperReviewFindingDraft(_Model):
    finding_id: str = Field(min_length=1)
    element: str = Field(min_length=1)
    severity: Literal["material", "minor"]
    issue_type: Literal[
        "unsupported_claim", "contradicts_source", "wrong_subject", "wrong_period",
        "wrong_column", "wrong_unit_or_denominator", "unsupported_causality",
        "material_omission", "other",
    ]
    action: Literal["quarantine", "correct", "possible_omission", "human_review"]
    affected_fact_id: str | None = None
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    explanation: str = Field(min_length=1)


class WorkpaperReviewResponse(_Model):
    reviewed_fact_ids: tuple[str, ...]
    findings: tuple[WorkpaperReviewFindingDraft, ...] = ()


class WorkpaperRepairResponse(_Model):
    repaired_facts: tuple[WorkpaperFact, ...]


class WorkpaperRepairCheck(_Model):
    fact_id: str = Field(min_length=1)
    accepted: bool
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    explanation: str = Field(min_length=1)


class WorkpaperRepairVerificationResponse(_Model):
    checked_fact_ids: tuple[str, ...]
    checks: tuple[WorkpaperRepairCheck, ...]


class WorkpaperReviewFinding(_Model):
    finding_id: str = Field(min_length=1)
    element: str = Field(min_length=1)
    severity: Literal["material", "minor"]
    issue_type: Literal[
        "unsupported_claim", "contradicts_source", "wrong_subject", "wrong_period",
        "wrong_column", "wrong_unit_or_denominator", "unsupported_causality",
        "material_omission", "other",
    ]
    affected_fact_id: str | None = None
    evidence_ids: tuple[str, ...]
    explanation: str = Field(min_length=1)
    resolution: Literal[
        "quarantined", "corrected_pending_trust", "unresolved",
        "omission_candidate", "repair_rejected", "verification_failed",
    ]
    original_fact: WorkpaperFact | None = None
    revised_fact: WorkpaperFact | None = None


class WorkpaperQualityReview(_Model):
    status: Literal["completed", "completed_with_open_findings", "incomplete", "failed"]
    policy_version: str = "company_profile_workpaper_review.v1"
    original_draft_hash: str
    findings: tuple[WorkpaperReviewFinding, ...] = ()
    checked_fact_ids: tuple[str, ...] = ()
    calls: tuple[WorkpaperModelCall, ...] = ()
    retained_request_ids: tuple[str, ...] = ()
    locally_revalidated_request_ids: tuple[str, ...] = ()
    error: str | None = None


class CompanyProfileWorkpaper(_Model):
    schema_version: Literal["company_profile_workpaper.v1"] = WORKPAPER_SCHEMA
    origin: Literal["human_reference", "llm_generated", "controlled_fixture"]
    report: ReportIdentity
    reading_scope: WorkpaperReadingScope
    elements: WorkpaperElements
    evidence: tuple[WorkpaperEvidence, ...]
    limitations: tuple[str, ...] = ()
    supply_chain: WorkpaperSupplyChain | None = None
    generation: WorkpaperGenerationMetadata | None = None
    quality_review: WorkpaperQualityReview | None = None
    standardization_notes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def source_integrity(self):
        sources = {item.evidence_id: item for item in self.evidence}
        if len(sources) != len(self.evidence):
            raise ValueError("duplicate evidence identity")
        if any(item.document_version != self.report.document_version for item in self.evidence):
            raise ValueError("evidence document version mismatch")
        if self.generation is not None:
            selected_ids = {item.section_id for item in self.reading_scope.sections}
            if set(self.generation.extraction.section_ids) != selected_ids:
                raise ValueError("model extraction attribution does not match selected sections")
            source_pages = {span.page for item in self.evidence for span in item.source_spans}
            if set(self.generation.extraction.physical_pages) != source_pages:
                raise ValueError("model extraction attribution does not match supplied pages")
        if self.quality_review is not None:
            sources_set = set(sources)
            for finding in self.quality_review.findings:
                if set(finding.evidence_ids) - sources_set:
                    raise ValueError("review finding cites unknown source evidence")
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


def workpaper_draft_wire_schema() -> dict:
    """Keep required business fields typed, without losing optional/raw extras."""
    schema = WorkpaperDraftResponse.model_json_schema()
    schema["$defs"]["WorkpaperElements"]["additionalProperties"] = True
    # Preserve the intended shape as a model hint; malformed optional content
    # must reach the adapter receipt before it is locally withheld.
    schema["properties"]["supply_chain"]["anyOf"].append({})
    return schema


def normalize_workpaper_draft(data):
    """Isolate the two observed container failures; never invent required facts."""
    if not isinstance(data, Mapping):
        return data
    payload = dict(data)
    limitations = list(payload.get("limitations") or ())
    elements = payload.get("elements")
    if isinstance(elements, Mapping):
        unknown = set(elements) - ELEMENT_LABELS.keys()
        if unknown:
            payload["elements"] = {k: v for k, v in elements.items() if k in ELEMENT_LABELS}
            limitations.append("未归类要素字段未作结论采用，原内容保留在模型回执：" + "、".join(sorted(unknown)))
    if payload.get("supply_chain") is not None:
        try:
            WorkpaperSupplyChain.model_validate(payload["supply_chain"])
        except (ValidationError, ValueError, TypeError):
            payload["supply_chain"] = None
            limitations.append("可选供应链结构无法解析，未作为结论采用；原内容保留在模型回执。")
    payload["limitations"] = limitations
    return payload


def bind_workpaper_draft(
    response: WorkpaperDraftResponse,
    *,
    report: ReportIdentity,
    reading_scope: WorkpaperReadingScope,
    evidence: Sequence[WorkpaperEvidence],
    origin: Literal["llm_generated", "controlled_fixture"] = "llm_generated",
    generation: WorkpaperGenerationMetadata | None = None,
) -> CompanyProfileWorkpaper:
    """Bind a model's evidence IDs to the immutable local source packet."""
    allowed = {item.evidence_id for item in evidence}
    for name in ELEMENT_LABELS:
        for fact in getattr(response.elements, name).facts:
            if set(fact.evidence_ids) - allowed:
                raise ValueError(f"model cited evidence outside the supplied packet: {fact.fact_id}")
    # A bad metric must not erase the other business content already returned.
    # Withhold the entire affected fact rather than guessing its sign/unit or
    # retaining a possibly wrong number in the narrative without its metric.
    sources = {item.evidence_id: item for item in evidence}
    cleaned_elements = {}
    binding_notes = []
    for name in ELEMENT_LABELS:
        element = getattr(response.elements, name)
        kept, rejected = [], []
        for fact in element.facts:
            try:
                _check_fact_source(fact, sources)
            except ValueError as exc:
                rejected.append(f"事实{fact.fact_id}已隔离，未作为结论采用：{exc}")
                continue
            kept.append(fact)
        cleaned_elements[name] = element.model_copy(update={
            "facts": tuple(kept),
            "status": "unclear" if rejected and not kept else element.status,
            "limitations": (*element.limitations, *rejected),
        })
        binding_notes.extend(rejected)
    elements = response.elements.model_copy(update=cleaned_elements)
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
            binding_notes.extend(rejected)
        supply_chain = WorkpaperSupplyChain(**cleaned)
    return standardize_workpaper_metrics(CompanyProfileWorkpaper(
        origin=origin,
        report=report,
        reading_scope=reading_scope,
        elements=elements,
        evidence=tuple(evidence),
        limitations=response.limitations,
        supply_chain=supply_chain,
        generation=generation,
        standardization_notes=tuple(binding_notes),
    ))


def standardize_workpaper_metrics(paper: CompanyProfileWorkpaper) -> CompanyProfileWorkpaper:
    """Normalize only explicit metric semantics; never rewrite legacy files on read."""
    payload = paper.model_dump(mode="json")
    notes = list(paper.standardization_notes)
    sources = {e.evidence_id: "".join("".join(s.quote.split()) for s in e.source_spans) for e in paper.evidence}
    for key in ("elements", "supply_chain"):
        for element in (payload.get(key) or {}).values():
            for fact in element["facts"]:
                context = "".join(sources[eid] for eid in fact["evidence_ids"])
                for metric in fact["metrics"]:
                    label = f"{fact['fact_id']}/{metric['name']}"
                    if metric["unit"] in {"%", "％", "百分点", "基点", "bp", "bps"} and metric["currency"] is not None:
                        notes.append(f"{label}：无量纲比例清除币种{metric['currency']}，原数值不变。")
                        metric["currency"] = None
                    is_margin_delta = "毛利率" in metric["name"] and any(
                        word in metric["name"] for word in ("比上年增减", "同比变动", "同比变化"))
                    points_in_answer = re.search(r"(-?\d+(?:\.\d+)?)\s*个百分点", fact["text"])
                    if (metric["value"] is not None and metric["unit"] == "%" and is_margin_delta and points_in_answer
                        and Decimal(points_in_answer.group(1)) == Decimal(metric["value"])
                        and all(word in context for word in ("毛利率", "比上年", "增减"))):
                        metric.update(source_unit="%", unit="百分点")
                        notes.append(f"{label}：来源表印示%，按已明确的毛利率差值规范为百分点；保留原值及来源单位。")
    payload["standardization_notes"] = list(dict.fromkeys(notes))
    return CompanyProfileWorkpaper.model_validate(payload)


def _metric_display(metric: WorkpaperMetric) -> str:
    """Display an already validated raw value without repeating its exact unit."""
    raw_value = metric.raw_value.strip()
    unit = (metric.unit or "").strip()
    if metric.value is None:
        source_unit = metric.source_unit or unit
        original = raw_value if not source_unit or raw_value.endswith(source_unit) else f"{raw_value} {source_unit}"
        return f"{original}（不确定：{metric.uncertainty_reason}；未标准化）"
    qualification = {"exact": "", "approximate": "（约数，非精确值）", "greater_than": "（下限，非精确值）"}[metric.qualifier]
    reason = metric.uncertainty_reason or ("指标主体范围未明确" if metric.subject_scope == "unclear" else None)
    if reason:
        qualification += f"（不确定：{reason}）"
    if metric.source_unit and metric.source_unit != unit:
        prefix = raw_value[:2 if raw_value.startswith("突破") else 1] if metric.qualifier != "exact" else ""
        return f"{prefix}{metric.value} {unit}（原文{metric.raw_value}）{qualification}"
    return (raw_value if raw_value.endswith(unit) else f"{raw_value} {unit}") + qualification


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
            uncertain = f"【不确定：{fact.uncertainty_reason or '主体范围未明确'}】" if fact.uncertainty_reason or fact.subject_scope == "unclear" else ""
            out.append(f"- {uncertain}{fact.text}（{refs}）")
            for metric in fact.metrics:
                out.append(f"  - {metric.name}：{_metric_display(metric)}；{metric.period}，{metric.subject_scope}。")
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
                uncertain = f"【不确定：{fact.uncertainty_reason or '主体范围未明确'}】" if fact.uncertainty_reason or fact.subject_scope == "unclear" else ""
                out.append(f"- {uncertain}{fact.text}（{refs}）")
                for metric in fact.metrics:
                    out.append(f"  - {metric.name}：{_metric_display(metric)}；{metric.period}，{metric.subject_scope}。")
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
    if workpaper.generation is not None:
        out.extend(["", "## 模型处理记录", ""])
        for label, call in (("选章", workpaper.generation.selection), ("正文抽取", workpaper.generation.extraction)):
            model = call.model or "模型未知"
            source = call.source_label or "来源未知"
            profile = call.selected_profile or call.logical_profile or "profile未知"
            if call.scope_kind == "report_heading_index":
                scope = f"报告标题索引（{call.heading_count if call.heading_count is not None else '未知'}个候选章节）"
            else:
                pages = ",".join(str(page) for page in call.physical_pages) or "未知"
                scope = f"章节 {', '.join(call.section_ids) or '未知'}；物理页 {pages}"
            usage = call.usage
            usage_text = (
                f"输入/输出/合计 {usage.input_tokens}/{usage.output_tokens}/{usage.total_tokens}"
                if usage.total_tokens is not None else "usage未知"
            )
            effort = f"；requested_reasoning={call.reasoning_effort}" if call.reasoning_effort else ""
            out.append(f"- {label}：{model}（{source}，{profile}），{scope}；{usage_text}；finish={call.finish_reason or '未知'}{effort}。")
        out.append(f"- 提示版本：`{workpaper.generation.prompt_version}`；预处理：`{workpaper.generation.preprocessing_version}`。")
    if workpaper.quality_review is not None:
        review = workpaper.quality_review
        out.extend(["", "## 原文复核（开发结果仍待正式核验）", "", f"复核状态：`{review.status}`。"])
        out.append(f"- 已检查原事实：{len(review.checked_fact_ids)}条；范围限于本次所选原文，不是全报告完整性保证。")
        out.append(f"- 原始底稿哈希：`{review.original_draft_hash}`。")
        for call in review.calls:
            out.append(
                f"- {call.purpose}：{call.model or '未知'}（{call.source_label or '未知'}）；"
                f"requested_reasoning={call.reasoning_effort or '未知'}；usage={call.usage.total_tokens}；"
                f"{'继承历史回执，非本次调用' if call.request_id in review.retained_request_ids else '本次调用'}。"
            )
            if call.request_id in review.locally_revalidated_request_ids:
                out.append("  - 历史合同状态仍为rejected；本次仅本地重校验候选有效，不是新的模型通过。")
        for finding in review.findings:
            refs = "、".join(f"[{value}](#{value.lower()})" for value in finding.evidence_ids)
            out.append(f"- {finding.severity} / {finding.issue_type} / {finding.resolution}：{finding.explanation}（{refs}）")
            if finding.original_fact is not None:
                out.append(f"  - 原事实（已保留审计）：{finding.original_fact.text}")
            if finding.revised_fact is not None:
                out.append(f"  - 修订候选（尚非可信发布）：{finding.revised_fact.text}")
        if review.error:
            out.append(f"- 复核未完成：{review.error}")
    if workpaper.standardization_notes:
        out.extend(["", "## 程序处理说明", ""])
        out.extend(f"- {note}" for note in workpaper.standardization_notes)
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
