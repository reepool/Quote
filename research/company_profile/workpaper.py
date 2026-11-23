"""Evidence-linked company workpaper projection and its researcher rendering.

This is an input/output projection, not another fact store or publication owner.
Human references and model drafts use the same content contract and stay distinct.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_serializer, model_validator

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
FEATURE_ELEMENT = "company_features"
Subject = Literal[
    "consolidated_group", "issuer", "named_subsidiary", "business_segment", "unclear"
]
SUBJECT_SCOPE_DESCRIPTION = (
    "consolidated_group means group-wide scope; plain 公司 in report-wide operating text "
    "defaults to it unless local evidence gives a narrower object. issuer means an "
    "explicitly parent-only company, not merely the reporting company. named_subsidiary "
    "means a specified subsidiary; business_segment means the specified business, product "
    "or industry row. unclear preserves scope that cannot be established."
)
WORKPAPER_SUBJECT_INSTRUCTIONS = SUBJECT_SCOPE_DESCRIPTION + (
    "\nJudge each metric's own source object independently of its enclosing fact. A business "
    "or industry row does not inherit consolidated_group from a company-wide fact or a "
    "consolidated table. Keep the source category in the metric name. Apply these meanings "
    "to original and repaired facts alike; never change a label just to pass review. "
    "A locatable amount, similar amounts or an arithmetic sum alone cannot establish "
    "subject scope or equivalence between source categories."
)
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


def _money_unit_spelling(unit: str | None) -> str | None:
    """Currency decoration is not a magnitude conversion."""
    if not isinstance(unit, str):
        return None
    bare = (unit or "").removeprefix("人民币").removesuffix("人民币").strip()
    return bare if bare in {"元", "千元", "万元", "百万元", "亿元", "万亿元"} else None


class WorkpaperMetric(_Model):
    name: str = Field(min_length=1)
    raw_value: str = Field(min_length=1)
    value: str | None = Field(default=None, min_length=1)
    unit: str | None = Field(default=None, min_length=1)
    source_unit: str | None = None
    currency: str | None = None
    subject_scope: Subject = Field(description=SUBJECT_SCOPE_DESCRIPTION)
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
        if qualifier != "exact" and data.get("value") is None and not data.get("uncertainty_reason"):
            result["uncertainty_reason"] = "程序说明：保留原文限定，未作精确数值标准化。"
        change = re.fullmatch(r"(增加|减少)\s*(\d[\d,，]*(?:\.\d+)?)\s*(.+)", raw)
        if change and data.get("value") is not None:
            unit = _money_unit_spelling(data.get("unit"))
            source_unit = _money_unit_spelling(data.get("source_unit") or data.get("unit"))
            if unit and unit == source_unit == _money_unit_spelling(change[3]):
                try:
                    amount = Decimal(change[2].replace(",", "").replace("，", ""))
                    reference = Decimal(str(data["value"]))
                except InvalidOperation:
                    pass
                else:
                    signed = -amount if change[1] == "减少" else amount
                    if amount.is_finite() and reference.is_finite() and signed == reference:
                        result.update(value=None, uncertainty_reason=data.get("uncertainty_reason") or
                                      "原文带增减方向及金额单位；保留原文，不参与精确加总。")
        # Preserve a real returned source phrase rather than forcing a signed
        # number onto it. Only an agreeing same-unit reference can fall back;
        # a wrong sign/unit/value is still an error, not "uncertainty".
        if (raw.startswith(("增加", "减少")) and raw.endswith("个百分点")
            and data.get("unit") in {"百分点", "个百分点"}
            and data.get("source_unit") in (None, "百分点", "个百分点", "%", "％") and data.get("value") is not None):
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
        decorated = any("人民币" in (unit or "") for unit in (self.unit, self.source_unit))
        if decorated and self.currency not in (None, "人民币", "CNY", "RMB"):
            raise ValueError("currency conflicts with the explicit source unit")
        if self.value is None:
            if not self.uncertainty_reason:
                raise ValueError("unnormalized metric requires an uncertainty reason")
            return self
        if self.unit is None:
            raise ValueError("unknown unit requires a null normalized value")
        if self.source_unit and self.source_unit != self.unit and (
            self.source_unit, self.unit
        ) not in {("%", "百分点"), ("％", "百分点"), ("个百分点", "百分点")} and not (
            _money_unit_spelling(self.unit)
            and _money_unit_spelling(self.unit) == _money_unit_spelling(self.source_unit)
        ):
            raise ValueError("source_unit cannot relabel a numeric magnitude")
        raw_number = self.raw_value.strip()
        if self.qualifier != "exact":
            raw_number = raw_number[2 if raw_number.startswith("突破") else 1:].strip()
        # Some real responses repeat the separately declared unit in raw_value.
        # Preserve that source spelling, but normalize only that exact suffix;
        # this does not convert units; estimates retain their explicit qualifier.
        raw_unit = self.source_unit or self.unit
        if self.unit == "百分点" and raw_unit in {"百分点", "%", "％"} and raw_number.endswith("个百分点"):
            raw_unit = "个百分点"
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
    subject_scope: Subject = Field(description=SUBJECT_SCOPE_DESCRIPTION)
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


def workpaper_elements(paper) -> dict[str, WorkpaperElement]:
    """All supplied fact groups, without making optional enrichment mandatory."""
    elements = {name: getattr(paper.elements, name) for name in ELEMENT_LABELS}
    if paper.supply_chain is not None:
        elements.update({name: getattr(paper.supply_chain, name) for name in SUPPLY_CHAIN_LABELS})
    if paper.company_features is not None:
        elements[FEATURE_ELEMENT] = paper.company_features
    return elements


def workpaper_element_payloads(payload: Mapping) -> dict[str, dict]:
    """Mutable views into a serialized workpaper for existing local projections."""
    elements = dict(payload.get("elements") or {})
    elements.update(payload.get("supply_chain") or {})
    if payload.get(FEATURE_ELEMENT) is not None:
        elements[FEATURE_ELEMENT] = payload[FEATURE_ELEMENT]
    return elements


def _wrapped_table_cells(quote: str) -> list[tuple[str, int, int]]:
    """Read explicit same-column continuations, never arbitrary whitespace joins.

    Require a monetary header and either a wrapped change-in-points marker or
    comparative columns with multiple uniquely aligned decimal continuations.
    Original quotes/coordinates are untouched; other damaged tables stay unclear.
    """
    cells = []
    lines = quote.splitlines(keepends=True)
    offset = 0
    for upper, lower in zip(lines, lines[1:]):
        change = re.search(r"(增加|减少)\s*(\d+(?:\.\d+)?)\s*$", upper.rstrip())
        unit = re.search(r"个百分点\s*$", lower.rstrip())
        header = re.search(r"单位\s*[:：]\s*(?:人民币\s*)?(亿元|万元|千元|元)", quote[:offset])
        if change and unit and header and change.start() <= unit.start() <= change.end():
            fragments = list(re.finditer(r"(?<![\d.,])-?\d[\d,]*(?:\.\d*)?(?![\d.,])", lower))
            prefixes = list(re.finditer(r"(?<![\d.,])-?\d{1,3}(?:,\d{3})+\.(\d?)(?!\d)", upper))
            joins = []
            for fragment in fragments:
                if not re.fullmatch(r"\d{1,2}", fragment.group()):
                    break
                aligned = [p for p in prefixes if abs(p.end() - fragment.end()) <= 2
                           and len(p.group(1)) + len(fragment.group()) == 2]
                if len(aligned) != 1:
                    break
                prefix = aligned[0]
                joins.append((prefix.group() + fragment.group(), offset + prefix.start(), offset + prefix.end()))
            else:
                # The points cell and every numeric fragment belong to this row.
                if len({start for _, start, _ in joins}) == len(joins):
                    cells.extend(joins)
                    cells.append((f"{change.group(1)}{change.group(2)}个百分点",
                                  offset + change.start(), offset + change.end()))
        # Accounting tables also wrap decimal suffixes without a points cell.
        # Two independent aligned columns establish a continuation; do not join
        # a lone nearby digit or regroup an incomplete integer part.
        if header and any(len(re.findall(r"\d{4}年|本期|上期|本年|上年", line)) >= 2
                          for line in quote[header.end():offset].splitlines()):
            fragments = list(re.finditer(r"(?<![\d.,])-?(?:\d[\d,]*(?:\.\d*)?|\.\d+)(?![\d.,])", lower))
            prefixes = list(re.finditer(r"(?<![\d.,])-?\d{1,3}(?:,\d{3})+(?:\.(\d?))?(?![\d.,])", upper))
            joins = []
            for fragment in fragments:
                aligned = []
                for prefix in prefixes:
                    tail = fragment.group()
                    valid = (prefix.group(1) is not None and re.fullmatch(r"\d{1,2}", tail)
                             and len(prefix.group(1)) + len(tail) == 2) or (
                                 prefix.group(1) is None and re.fullmatch(r"\.\d{2}", tail))
                    if valid and abs(prefix.end() - fragment.start()) <= 2:
                        aligned.append(prefix)
                if len(aligned) != 1:
                    break
                prefix = aligned[0]
                joins.append((prefix.group() + fragment.group(), offset + prefix.start(), offset + prefix.end()))
            else:
                if len(joins) >= 2 and len({start for _, start, _ in joins}) == len(joins):
                    cells.extend(joins)
        offset += len(upper)
    return cells


_MONEY_HEADER = re.compile(
    r"(?:(?:金额单位|单位)\s*[:：]\s*(?:人民币\s*)?|[（(]\s*人民币\s*)"
    r"(万亿元|亿元|百万元|万元|千元|元)"
)


def _source_number_cells(quote: str, *, legacy_fragments: bool = False) -> list[tuple[str, int, int]]:
    """Recognize complete numeric spellings without flattening rows/columns.

    Only comma/decimal punctuation may bridge same-line layout spaces. A
    parenthesized grouped/decimal amount needs a monetary header or an
    explicit multi-column account row; notes do not supply negatives or units.
    All ranges remain original offsets.
    Legacy deserialization retains its old fragments without rewriting files.
    """
    horizontal = r"[^\S\r\n]*"
    amount = rf"(?:\d{{1,3}}(?:,{horizontal}\d{{3}})+|\d+)(?:\.{horizontal}\d+)?"
    recovered = []
    for match in re.finditer(rf"(?<!\S)[（(]{horizontal}({amount}){horizontal}[)）](?!\S)", quote):
        text = re.sub(r"[^\S\r\n]", "", match.group(1))
        line_start = quote.rfind("\n", 0, match.start()) + 1
        line_end = quote.find("\n", match.end())
        line = quote[line_start:line_end if line_end >= 0 else len(quote)]
        # A continuation table may omit its currency caption. Multiple
        # spaced numeric columns on an explicit financial-account row still
        # establish accounting sign, but supply no missing monetary unit.
        financial_row = re.search(r"收入|支出|损益|利润|费用|成本|资产|负债|余额|合计", line[:match.start()-line_start])
        columns = re.findall(r"(?<!\S)[（(]?-?\d[\d,.]*[)）]?(?=[^\S\r\n]{2,}|$)", line)
        accounting_context = _MONEY_HEADER.search(quote[:match.start()]) or (financial_row and len(columns) >= 3)
        if ("," in text or "." in text) and accounting_context:
            recovered.append(("-" + text, match.start(), match.end()))
    # Do not bridge a bare gap between integers or a physical line break.
    spaced = rf"(?<![\d.,])-?(?:\d{{1,3}}(?:,{horizontal}\d{{3}})+(?:\.{horizontal}\d+)?|\d+\.[^\S\r\n]+\d+)(?![\d.,])"
    for match in re.finditer(spaced, quote):
        text = re.sub(r"[^\S\r\n]", "", match.group())
        if text != match.group() and not any(start <= match.start() < end for _, start, end in recovered):
            recovered.append((text, match.start(), match.end()))
    ordinary = [
        (match.group(), match.start(), match.end())
        for match in re.finditer(r"(?<![\d.])-?\d[\d,]*(?:\.\d+)?(?![\d.])", quote)
        if legacy_fragments or not any(start <= match.start() < end for _, start, end in recovered)
    ]
    return [*ordinary, *recovered]


def _check_fact_source(
    fact: WorkpaperFact, sources: Mapping[str, WorkpaperEvidence], *, check_units: bool = False,
) -> tuple[str, ...]:
    if set(fact.evidence_ids) - sources.keys():
        raise ValueError("fact cites unknown evidence")
    numbers = {
        Decimal(value.replace(",", ""))
        for evidence_id in fact.evidence_ids
        for span in sources[evidence_id].source_spans
        for value, _, _ in _source_number_cells(span.quote, legacy_fragments=not check_units)
    }
    cells = [cell for eid in fact.evidence_ids for span in sources[eid].source_spans
             for cell in _wrapped_table_cells(span.quote)]
    numbers.update(Decimal(text.replace(",", "")) for text, _, _ in cells
                   if re.fullmatch(r"-?\d[\d,]*(?:\.\d+)?", text))
    for metric in fact.metrics:
        if check_units:
            _check_explicit_money_unit(metric, fact, sources)
        if metric.value is None:
            quoted = "".join(span.quote for eid in fact.evidence_ids for span in sources[eid].source_spans)
            literal = "".join(metric.raw_value.split())
            if literal not in "".join(quoted.split()) and not any(literal in text for text, _, _ in cells):
                raise ValueError(f"uncertain metric original wording is absent from cited source: {metric.name}")
            continue
        # A signed change may be spelled as a direction-qualified rate in prose.
        # Only an explicit period-on-period clause with its rate unit supplies
        # this alternative; it never supplies a negative monetary balance.
        rate_unit = {"%": "%", "％": "%", "百分点": "百分点", "个百分点": "百分点"}
        negative_rates = {
            -Decimal(amount.replace(",", ""))
            for eid in fact.evidence_ids for span in sources[eid].source_spans
            for amount, unit in re.findall(
                r"(?:^|[，,；;。\n])\s*(?:同比|环比)\s*(?:下降|减少|降低)\s*"
                r"(\d[\d,]*(?:\.\d+)?)\s*(%|％|个百分点|百分点)", span.quote)
            if rate_unit.get(unit) == rate_unit.get(metric.unit) and metric.unit in rate_unit
        } if metric.unit in rate_unit and Decimal(metric.value) < 0 else set()
        if rate_unit.get(metric.unit) == "百分点" and Decimal(metric.value) < 0:
            negative_rates.update(-Decimal(match.group(1)) for text, _, _ in cells
                                  if (match := re.fullmatch(r"减少(\d+(?:\.\d+)?)个百分点", text)))
        # A table's signed percent and prose's explicit decline magnitude are
        # two spellings of the same change, not a positive balance or growth.
        if metric.unit in {"%", "％"} and Decimal(metric.value) > 0 and -Decimal(metric.value) in numbers:
            if any(re.search(r"(?:同比|环比|同期增减)\s*[(（]\s*[%％]\s*[)）]", span.quote)
                   for eid in fact.evidence_ids for span in sources[eid].source_spans):
                negative_rates.update(Decimal(amount.replace(",", "")) for amount in re.findall(
                    r"(?:^|[，,；;。\n])\s*(?:同比|环比)\s*(?:下降|减少|降低)\s*"
                    r"(\d[\d,]*(?:\.\d+)?)\s*[%％]", fact.text))
        if Decimal(metric.value) not in numbers | negative_rates:
            raise ValueError(f"metric number is absent from cited source: {metric.name}")
    if check_units:
        return _check_prose_quantities(fact, sources, numbers)
    return ()


def _monetary_display_note(
    fact: WorkpaperFact, sources: Mapping[str, WorkpaperEvidence], amount: Decimal, unit: str,
) -> str | None:
    """Compare display magnitudes only to already source-validated same-fact metrics."""
    scales = {"元": Decimal(1), "千元": Decimal(1000), "万元": Decimal(10000),
              "百万元": Decimal(1000000), "亿元": Decimal(100000000), "万亿元": Decimal(1000000000000)}
    for metric in fact.metrics:
        original_unit = _money_unit_spelling(metric.unit)
        if (metric.value is None or metric.qualifier != "exact" or original_unit not in scales
            or original_unit == unit or metric.period != fact.period):
            continue
        if not _check_explicit_money_unit(metric, fact, sources):
            continue  # A model-supplied unit without an explicit source unit cannot justify conversion.
        try:
            converted = Decimal(metric.value) * scales[original_unit] / scales[unit]
            matches = converted == amount or (amount != 0 and converted.quantize(
                Decimal(1).scaleb(amount.as_tuple().exponent), rounding=ROUND_HALF_UP) == amount)
        except InvalidOperation:
            continue  # Unsupported precision remains a source mismatch, not a whole-draft crash.
        if matches:
            kind = "等值换算" if converted == amount else "舍入展示，非精确值"
            return f"程序显示说明：{fact.fact_id}正文{amount}{unit}为{metric.name}{metric.raw_value}{metric.unit}的{kind}；结构化原值及来源未改，不据展示值精确加总。"
    return None


def qualify_workpaper_display(fact: WorkpaperFact, notes: Sequence[str]) -> WorkpaperFact:
    """Keep generated display qualifications stable across source rebindings."""
    original = fact.uncertainty_reason or ""
    additions = [note for note in notes if note not in original]
    if not additions:
        return fact
    return fact.model_copy(update={"uncertainty_reason": "；".join(filter(None, (original, *additions)))})


def _check_prose_quantities(
    fact: WorkpaperFact, sources: Mapping[str, WorkpaperEvidence], numbers: set[Decimal],
) -> tuple[str, ...]:
    """Bind written financial numerals too, without inventing semantic metrics.

    This is a literal/unit check, not a claim that a number belongs to the right
    subject or table column. Those still require semantic source review. Called
    only on new binding/repairs, so historical deserialization is not rewritten.
    """
    units = {"%": "%", "％": "%", "百分点": "百分点", "个百分点": "百分点"}
    money = {"元", "千元", "万元", "百万元", "亿元", "万亿元"}
    display_notes = []
    financial = r"收入|成本|利润|金额|费用|补贴|余额|资产|负债|资本|减值|毛利率|占比|现金流"
    token = r"(?<![\d.,A-Za-z])(-?\d[\d,]*(?:\.\d+)?|零(?=万亿元|亿元|百万元|万元|千元|元|个百分点|百分点|%|％|[，,；;。）、）()\s]|$))\s*(万亿元|亿元|百万元|万元|千元|元|个百分点|百分点|%|％)?(?![\d.,A-Za-z])"
    checked = {(Decimal(m.value), units.get(m.unit, m.unit)) for m in fact.metrics if m.value is not None}
    clauses = {"".join(clause.split()) for eid in fact.evidence_ids for span in sources[eid].source_spans
               for clause in re.split(r"[，,；;。\n]", span.quote)}
    # A categorical financial absence is a zero-like assertion without a digit.
    # Require its scoped source wording, rather than borrowing some other row's
    # zero or teaching this binder to infer business/column semantics.
    absence = (
        r"(?:(?:无|没有|未产生|未实现|未取得)(?:任何)?(?:营业)?收入|"
        r"(?:无|没有|未发生)(?:任何)?(?:营业)?(?:成本|费用|支出))$"
    )
    source_texts = ["".join(span.quote.split()) for eid in fact.evidence_ids for span in sources[eid].source_spans]
    for clause in re.split(r"[，,；;。\n]", fact.text):
        literal = "".join(clause.split())
        assertion = re.search(absence, literal)
        if assertion and re.search(
            r"(?:(?:不能|无法|不应|不得)(?:据此)?(?:认定|判断|确定|推断|证明|视为)(?:是否)?|"
            r"不代表|不意味着|不等于|是否)$", literal[:assertion.start()],
        ):
            continue  # An explicit warning not to infer zero is not a zero claim.
        if assertion and not any(literal in text for text in source_texts):
            raise ValueError("categorical financial absence lacks explicit scoped source wording")
    for match in re.finditer(token, fact.text):
        amount = Decimal("0" if match.group(1) == "零" else match.group(1).replace(",", ""))
        unit = match.group(2)
        prefix = re.split(r"[，,；;。\n]", fact.text[:match.start()])[-1]
        if not unit and not (amount == 0 and re.search(financial, prefix)):
            continue  # Years, model names and operating counts are not financial amounts.
        signed = -amount if unit in units and re.search(r"(?:下降|减少|降低)\s*(?:近|约|超|突破)?\s*$", prefix) else amount
        normalized_unit = units.get(unit, unit)
        literal_zero = amount == 0 and "".join((prefix + match.group(0)).split()) in clauses
        display_note = _monetary_display_note(fact, sources, amount, unit) if unit in money else None
        if display_note:
            display_notes.append(display_note)
            continue
        if amount == 0 and unit is None and not literal_zero and not any(
            value == 0 and measured_unit in money | set(units.values()) for value, measured_unit in checked
        ):
            raise ValueError("narrative financial zero lacks a corresponding metric or explicit source clause")
        if not literal_zero and amount not in numbers and signed not in numbers and not ({(amount, normalized_unit), (signed, normalized_unit)} & checked):
            raise ValueError(f"narrative number is absent from cited source: {match.group(0).strip()}")
        if unit in money:
            metric = WorkpaperMetric(name="正文金额（技术源核，非新增指标）", raw_value=str(amount),
                value=str(amount), unit=unit, source_unit=unit, currency=None,
                subject_scope=fact.subject_scope, period=fact.period)
            _check_explicit_money_unit(metric, fact, sources)
    return tuple(dict.fromkeys(display_notes))


def _check_explicit_money_unit(
    metric: WorkpaperMetric, fact: WorkpaperFact, sources: Mapping[str, WorkpaperEvidence],
) -> bool:
    """Reject only a clear cited table-unit conflict; never infer a missing unit.

    Called for new binding/repairs, not legacy deserialization. A multi-unit or
    headerless match is deliberately undecided rather than guessed by magnitude.
    """
    declared = {u for value in (metric.unit, metric.source_unit) if (u := _money_unit_spelling(value))}
    if not declared:
        return False
    reference = metric.value
    if reference is None:
        literal = re.fullmatch(r"(?:近|约|超|突破)?\s*(-?\d[\d,，]*(?:\.\d+)?)\s*(?:万亿元|亿元|百万元|万元|千元|元)?", metric.raw_value)
        if literal is None:
            return False
        reference = literal.group(1).replace(",", "").replace("，", "")
    reference = Decimal(reference)
    units, headerless = set(), False
    for eid in fact.evidence_ids:
        for span in sources[eid].source_spans:
            headers = list(_MONEY_HEADER.finditer(span.quote))
            matches = _source_number_cells(span.quote)
            matches.extend(cell for cell in _wrapped_table_cells(span.quote)
                           if re.fullmatch(r"-?\d[\d,]*(?:\.\d+)?", cell[0]))
            for text, start, end in matches:
                if Decimal(text.replace(",", "")) != reference:
                    continue
                # A paragraph may follow a table on the same source fragment.
                # Its explicit amount suffix owns the unit, not that old header.
                inline = re.match(r"\s*(万亿元|亿元|百万元|万元|千元|元)", span.quote[end:])
                if inline:
                    units.add(inline.group(1))
                    continue
                governing = [h for h in headers if h.end() <= start]
                if governing:
                    units.add(governing[-1].group(1))
                else:
                    headerless = True
    if not headerless and len(units) == 1 and declared != units:
        raise ValueError(f"source monetary unit conflict: {metric.name}; 原表{'/'.join(units)}，返回{'/'.join(sorted(declared))}；未猜测换算")
    return not headerless and len(units) == 1 and declared == units


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


class WorkpaperFormatNormalization(_Model):
    policy: Literal["terminal_object_and_root_fields.v1"] = "terminal_object_and_root_fields.v1"
    original_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalized_object_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    operations: tuple[str, ...] = Field(min_length=1)


class WorkpaperUnreadNormalization(_Model):
    policy: Literal["enrichment_empty_unread_status.v1"] = "enrichment_empty_unread_status.v1"
    original_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalized_object_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_elements: tuple[str, ...] = Field(min_length=1)
    normalized_elements: tuple[str, ...] = Field(min_length=1)


class WorkpaperGenerationMetadata(_Model):
    prompt_version: str = Field(min_length=1)
    preprocessing_version: str = Field(min_length=1)
    section_guide_version: str | None = None
    selection: WorkpaperModelCall
    extraction: WorkpaperModelCall
    local_format_normalization: WorkpaperFormatNormalization | None = None
    local_schema_normalization: WorkpaperUnreadNormalization | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_shape(self, handler):
        payload = handler(self)
        if self.local_format_normalization is None:
            payload.pop("local_format_normalization", None)
        if self.local_schema_normalization is None:
            payload.pop("local_schema_normalization", None)
        return payload

    @model_validator(mode="after")
    def stages_match(self):
        if self.selection.purpose != "select_profile_sections":
            raise ValueError("selection model attribution has wrong purpose")
        if self.extraction.purpose != "extract_profile_workpaper":
            raise ValueError("extraction model attribution has wrong purpose")
        if self.local_format_normalization is not None and (
            self.extraction.contract_status != "rejected"
            or self.extraction.response_hash != self.local_format_normalization.original_content_sha256
        ):
            raise ValueError("local format normalization must retain the rejected original hash")
        if self.local_schema_normalization is not None and (
            self.local_format_normalization is not None
            or self.extraction.contract_status != "rejected"
            or self.extraction.response_hash != self.local_schema_normalization.original_content_sha256
        ):
            raise ValueError("local schema normalization must retain its rejected original hash")
        return self


class WorkpaperReviewMetricLimit(_Model):
    """Model source-review limit on exact original positions, not a corrected metric."""

    fact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    narrative_supported: Literal[True]
    metric_indices: tuple[int, ...] = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def strict_positions(cls, value):
        if isinstance(value, Mapping):
            indices = value.get("metric_indices")
            if (value.get("narrative_supported") is not True or not isinstance(indices, (list, tuple))
                or any(type(index) is not int or index < 0 for index in indices)):
                raise ValueError("metric limit requires explicit narrative support and nonnegative integer indices")
        return value


class WorkpaperReviewFindingDraft(_Model):
    finding_id: str = Field(min_length=1)
    element: str = Field(min_length=1)
    severity: Literal["material", "minor"]
    issue_type: Literal[
        "unsupported_claim", "contradicts_source", "wrong_subject", "wrong_period",
        "wrong_column", "wrong_unit_or_denominator", "unsupported_causality",
        "material_omission", "other",
    ]
    action: Literal["quarantine", "correct", "possible_omission", "human_review", "limit_metrics"]
    affected_fact_id: str | None = None
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    explanation: str = Field(min_length=1)
    metric_limit: WorkpaperReviewMetricLimit | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_shape(self, handler):
        payload = handler(self)
        if self.metric_limit is None:
            payload.pop("metric_limit", None)
        return payload


class WorkpaperReviewResponse(_Model):
    reviewed_fact_ids: tuple[str, ...]
    findings: tuple[WorkpaperReviewFindingDraft, ...] = ()


class WorkpaperRepairResponse(_Model):
    """Raw candidates: identities first, then each full fact is validated locally."""

    repaired_facts: tuple[dict[str, Any], ...]


def workpaper_repair_wire_schema() -> dict:
    """Request full typed facts even though local consumption isolates bad ones."""
    schema = WorkpaperRepairResponse.model_json_schema()
    fact_schema = WorkpaperFact.model_json_schema()
    schema["$defs"] = fact_schema.pop("$defs", {})
    schema["$defs"]["WorkpaperFact"] = fact_schema
    schema["properties"]["repaired_facts"]["items"] = {"$ref": "#/$defs/WorkpaperFact"}
    return schema


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
    related_fact_id: str | None = None
    evidence_ids: tuple[str, ...]
    explanation: str = Field(min_length=1)
    resolution: Literal[
        "quarantined", "corrected_pending_trust", "unresolved",
        "omission_candidate", "repair_rejected", "verification_failed", "metrics_limited",
    ]
    original_fact: WorkpaperFact | None = None
    revised_fact: WorkpaperFact | None = None
    metric_limit: WorkpaperReviewMetricLimit | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_shape(self, handler):
        payload = handler(self)
        if self.metric_limit is None:
            payload.pop("metric_limit", None)
        return payload

    @model_validator(mode="after")
    def metric_limit_is_bound(self):
        if self.resolution == "metrics_limited":
            limit = self.metric_limit
            fact = self.original_fact
            if (limit is None or fact is None or self.affected_fact_id != fact.fact_id
                or limit.fact_sha256 != workpaper_fact_hash(fact)
                or len(set(limit.metric_indices)) != len(limit.metric_indices)
                or any(i >= len(fact.metrics) for i in limit.metric_indices)):
                raise ValueError("metric-only review requires exact original fact/hash and unique valid indices")
        elif self.metric_limit is not None:
            raise ValueError("metric-only support cannot accompany a whole-fact resolution")
        return self


class WorkpaperReviewExecutionFailure(_Model):
    """Local execution diagnostics; never a model-authored quality decision."""

    purpose: Literal["review_profile_workpaper", "repair_profile_workpaper", "verify_profile_workpaper_repair"]
    kind: Literal["empty_response", "gateway_error", "contract_error"]
    code: str = Field(min_length=1)
    requested_profile: str | None = None
    selected_profile: str | None = None
    source_label: str | None = None
    model: str | None = None


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
    checked_fact_hashes: dict[str, str] | None = None
    execution_failure: WorkpaperReviewExecutionFailure | None = None
    metric_limit_policy: Literal["source_review_exact_metric_limits.v1"] | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_shape(self, handler):
        payload = handler(self)
        if self.checked_fact_hashes is None:
            payload.pop("checked_fact_hashes", None)
        if self.execution_failure is None:
            payload.pop("execution_failure", None)
        if self.metric_limit_policy is None:
            payload.pop("metric_limit_policy", None)
        return payload


def workpaper_fact_hash(fact: WorkpaperFact) -> str:
    """Bind a use/review decision to exact current text, metrics and references."""
    return hashlib.sha256(json.dumps(fact.model_dump(mode="json"), ensure_ascii=False, sort_keys=True).encode()).hexdigest()


class WorkpaperMetricRestriction(_Model):
    """An independent restriction on an original metric position, never a repair."""

    metric_index: int = Field(ge=0, strict=True)
    reason: str = Field(min_length=1)


class WorkpaperUseDecision(_Model):
    fact_id: str = Field(min_length=1)
    fact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    disposition: Literal["source_checked", "source_only", "withheld", "duplicate"]
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    reason: str = Field(min_length=1)
    limitation: str | None = None
    metric_restrictions: tuple[WorkpaperMetricRestriction, ...] = ()

    @model_serializer(mode="wrap")
    def preserve_legacy_shape(self, handler):
        payload = handler(self)
        if not self.metric_restrictions:
            payload.pop("metric_restrictions", None)
        return payload


class WorkpaperUseReview(_Model):
    """Locally bound independent source review, never authored by extraction."""
    report: ReportIdentity
    reviewer: str = Field(min_length=1)
    reference: str = Field(min_length=1)
    reference_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    decisions: tuple[WorkpaperUseDecision, ...]


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
    company_features: WorkpaperElement | None = None
    use_review: WorkpaperUseReview | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_shape(self, handler):
        payload = handler(self)
        if self.company_features is None:
            payload.pop(FEATURE_ELEMENT, None)
        if self.use_review is None:
            payload.pop("use_review", None)
        return payload

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
        facts = {}
        for section in workpaper_elements(self).values():
            for fact in section.facts:
                if fact.fact_id in ids:
                    raise ValueError("duplicate fact identity")
                ids.add(fact.fact_id)
                facts[fact.fact_id] = fact
                _check_fact_source(fact, sources)
        if self.quality_review is not None:
            for finding in self.quality_review.findings:
                if finding.resolution != "metrics_limited":
                    continue
                if self.quality_review.metric_limit_policy != "source_review_exact_metric_limits.v1":
                    raise ValueError("metric-only source review requires its explicit policy")
                current = facts.get(finding.affected_fact_id)
                if current is not None and workpaper_fact_hash(current) != finding.metric_limit.fact_sha256:
                    raise ValueError("metric-only source review content is stale")
        if self.use_review is not None:
            if self.use_review.report != self.report:
                raise ValueError("independent use review belongs to another report")
            seen = set()
            for decision in self.use_review.decisions:
                if decision.fact_id in seen or decision.fact_id not in facts:
                    raise ValueError("unknown or duplicate independent use-review fact")
                seen.add(decision.fact_id)
                if workpaper_fact_hash(facts[decision.fact_id]) != decision.fact_sha256:
                    raise ValueError("independent use-review fact content is stale")
                indices = [item.metric_index for item in decision.metric_restrictions]
                if len(indices) != len(set(indices)) or any(
                    index >= len(facts[decision.fact_id].metrics) for index in indices
                ):
                    raise ValueError("unknown, duplicate or out-of-range independent metric restriction")
                if set(decision.evidence_ids) - sources.keys():
                    raise ValueError("independent use review cites unknown evidence")
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
    company_features: WorkpaperElement | None = None


def workpaper_draft_wire_schema() -> dict:
    """Keep required business fields typed, without losing optional/raw extras."""
    schema = WorkpaperDraftResponse.model_json_schema()
    schema["$defs"]["WorkpaperElements"]["additionalProperties"] = True
    # Preserve the intended shape as a model hint; malformed optional content
    # must reach the adapter receipt before it is locally withheld.
    schema["properties"]["supply_chain"]["anyOf"].append({})
    schema["properties"][FEATURE_ELEMENT]["anyOf"].append({})
    return schema


def normalize_workpaper_draft(data, *, metric_format_policy=None):
    """Withhold observed local failures, never guess missing facts or metric meaning."""
    from .workpaper_metric_format import METRIC_FORMAT_POLICY, normalize_directional_metric_format

    if metric_format_policy not in (None, METRIC_FORMAT_POLICY):
        raise ValueError(f"unknown workpaper metric format policy: {metric_format_policy!r}")
    if not isinstance(data, Mapping):
        return data
    payload = dict(data)
    if metric_format_policy is not None:
        groups = []
        for container_name in ("elements", "supply_chain"):
            container = payload.get(container_name)
            if isinstance(container, Mapping):
                payload[container_name] = dict(container)
                groups.extend((payload[container_name], name) for name in container)
        if payload.get(FEATURE_ELEMENT) is not None:
            groups.append((payload, FEATURE_ELEMENT))
        for container, name in groups:
            element = container[name]
            if not isinstance(element, Mapping) or not isinstance(element.get("facts"), (tuple, list)):
                continue
            facts, notes = [], []
            for raw_fact in element["facts"]:
                fact, local_notes = normalize_directional_metric_format(raw_fact, policy=metric_format_policy)
                facts.append(fact)
                notes.extend(local_notes)
            container[name] = {**element, "facts": facts,
                               "limitations": [*(element.get("limitations") or ()), *notes]}
    limitations = list(payload.get("limitations") or ())
    elements = payload.get("elements")
    if isinstance(elements, Mapping):
        unknown = set(elements) - ELEMENT_LABELS.keys()
        if unknown:
            payload["elements"] = {k: v for k, v in elements.items() if k in ELEMENT_LABELS}
            limitations.append("未归类要素字段未作结论采用，原内容保留在模型回执：" + "、".join(sorted(unknown)))
        cleaned = dict(payload.get("elements", elements))
        for name, element in cleaned.items():
            if not isinstance(element, Mapping) or not isinstance(element.get("facts"), (list, tuple)):
                continue  # Other contract errors still fail the typed response.
            kept, rejected = [], []
            for fact in element["facts"]:
                try:
                    WorkpaperFact.model_validate(fact)
                except ValidationError as exc:
                    errors = exc.errors(include_input=False, include_url=False)
                    if not all(error["loc"] and error["loc"][0] == "metrics" for error in errors):
                        kept.append(fact)
                        continue
                    reason = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in errors)
                    rejected.append(f"指标格式隔离：事实{fact['fact_id']}未作为结论采用；{reason}；原内容保留在模型回执。")
                    continue
                kept.append(fact)
            if rejected:
                cleaned[name] = {**element, "facts": kept,
                    "status": "unclear" if not kept else element.get("status"),
                    "limitations": [*(element.get("limitations") or ()), *rejected]}
        payload["elements"] = cleaned
    if payload.get("supply_chain") is not None:
        try:
            WorkpaperSupplyChain.model_validate(payload["supply_chain"])
        except (ValidationError, ValueError, TypeError):
            payload["supply_chain"] = None
            limitations.append("可选供应链结构无法解析，未作为结论采用；原内容保留在模型回执。")
    feature = payload.get(FEATURE_ELEMENT)
    if feature is not None:
        try:
            if isinstance(feature, Mapping) and isinstance(feature.get("facts"), (list, tuple)):
                original_groups = list(elements.values()) if isinstance(elements, Mapping) else []
                if isinstance(data.get("supply_chain"), Mapping):
                    original_groups.extend(data["supply_chain"].values())
                reserved_ids = {f.get("fact_id") for group in original_groups if isinstance(group, Mapping)
                                and isinstance(group.get("facts"), (list, tuple)) for f in group["facts"]
                                if isinstance(f, Mapping) and isinstance(f.get("fact_id"), str)}
                kept, rejected = [], []
                for fact in feature["facts"]:
                    try:
                        WorkpaperFact.model_validate(fact)
                        if fact["fact_id"] in reserved_ids:
                            raise ValueError("duplicate baseline/feature identity")
                    except (ValidationError, ValueError, TypeError):
                        identity = fact.get("fact_id", "未知编号") if isinstance(fact, Mapping) else "未知编号"
                        rejected.append(f"特色事实{identity}格式错误，未采用；原内容保留在模型回执。")
                    else:
                        kept.append(fact)
                feature = {**feature, "facts": kept,
                           "limitations": [*(feature.get("limitations") or ()), *rejected]}
                if rejected and not kept:
                    feature["status"] = "unclear"
            payload[FEATURE_ELEMENT] = WorkpaperElement.model_validate(feature).model_dump(mode="json")
        except (ValidationError, ValueError, TypeError):
            payload[FEATURE_ELEMENT] = None
            limitations.append("可选公司特色结构无法解析，未采用；基础画像保留，原内容保留在模型回执。")
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
    binding_notes = [note for name in ELEMENT_LABELS
                     for note in getattr(response.elements, name).limitations
                     if note.startswith("指标格式隔离：")]
    for name in ELEMENT_LABELS:
        element = getattr(response.elements, name)
        kept, rejected = [], []
        for fact in element.facts:
            try:
                display_notes = _check_fact_source(fact, sources, check_units=True)
            except ValueError as exc:
                rejected.append(f"事实{fact.fact_id}已隔离，未作为结论采用：{exc}")
                continue
            if display_notes:
                fact = qualify_workpaper_display(fact, display_notes)
                binding_notes.extend(display_notes)
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
                    display_notes = _check_fact_source(fact, sources, check_units=True)
                except ValueError as exc:
                    rejected.append(f"可选事实{fact.fact_id}未纳入：{exc}")
                    continue
                if display_notes:
                    fact = qualify_workpaper_display(fact, display_notes)
                    binding_notes.extend(display_notes)
                kept.append(fact)
                used_ids.add(fact.fact_id)
            cleaned[name] = element.model_copy(update={
                "facts": tuple(kept),
                "status": "unclear" if rejected and not kept else element.status,
                "limitations": (*element.limitations, *rejected),
            })
            binding_notes.extend(rejected)
        supply_chain = WorkpaperSupplyChain(**cleaned)
    company_features = response.company_features
    if company_features is not None:
        binding_notes.extend(note for note in company_features.limitations
                             if note.startswith("特色事实") and "格式错误" in note)
        used_ids = {f.fact_id for name in ELEMENT_LABELS for f in getattr(response.elements, name).facts}
        if response.supply_chain is not None:
            used_ids.update(f.fact_id for name in SUPPLY_CHAIN_LABELS for f in getattr(response.supply_chain, name).facts)
        kept, rejected = [], []
        for fact in company_features.facts:
            try:
                if fact.fact_id in used_ids:
                    raise ValueError("duplicate fact identity")
                display_notes = _check_fact_source(fact, sources, check_units=True)
            except ValueError as exc:
                rejected.append(f"特色事实{fact.fact_id}未纳入：{exc}")
                continue
            if display_notes:
                fact = qualify_workpaper_display(fact, display_notes)
                binding_notes.extend(display_notes)
            kept.append(fact)
            used_ids.add(fact.fact_id)
        company_features = company_features.model_copy(update={
            "facts": tuple(kept),
            "status": "unclear" if rejected and not kept else company_features.status,
            "limitations": (*company_features.limitations, *rejected),
        })
        binding_notes.extend(rejected)
    return standardize_workpaper_metrics(CompanyProfileWorkpaper(
        origin=origin,
        report=report,
        reading_scope=reading_scope,
        elements=elements,
        evidence=tuple(evidence),
        limitations=response.limitations,
        supply_chain=supply_chain,
        company_features=company_features,
        generation=generation,
        standardization_notes=tuple(binding_notes),
    ))


def standardize_workpaper_metrics(paper: CompanyProfileWorkpaper) -> CompanyProfileWorkpaper:
    """Normalize only explicit metric semantics; never rewrite legacy files on read."""
    payload = paper.model_dump(mode="json")
    notes = list(paper.standardization_notes)
    sources = {e.evidence_id: "".join("".join(s.quote.split()) for s in e.source_spans) for e in paper.evidence}
    for element in workpaper_element_payloads(payload).values():
        for fact in element["facts"]:
            context = "".join(sources[eid] for eid in fact["evidence_ids"])
            for metric in fact["metrics"]:
                label = f"{fact['fact_id']}/{metric['name']}"
                if metric["unit"] == "个百分点":
                    metric.update(source_unit=metric["source_unit"] or "个百分点", unit="百分点")
                    notes.append(f"{label}：个百分点与百分点为同义单位，保留原文/来源单位，未改数值。")
                if (metric.get("uncertainty_reason") or "").startswith("程序说明："):
                    notes.append(f"{label}：{metric['uncertainty_reason']}")
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


def render_workpaper(workpaper: CompanyProfileWorkpaper, *, delivery: Mapping | None = None) -> str:
    """Render only the structured workpaper; never synthesize a second answer."""
    report = workpaper.report
    origin = {
        "human_reference": "人工原文参考底稿（非模型成绩）",
        "llm_generated": "真实模型底稿（待核，不代表正式公司可用）",
        "controlled_fixture": "受控工程输入（非真实模型结果）",
    }[workpaper.origin]
    if workpaper.generation is not None and workpaper.generation.local_format_normalization is not None:
        origin = (
            "真实模型原内容，经本地格式标准化的待核候选（原抽取失败，未二次审核）"
            if workpaper.quality_review is None else
            "真实模型原内容，经本地格式标准化并接续源复核的候选（原抽取失败，仍非正式可信发布）"
        )
    out = [
        f"# {report.instrument_id}｜{report.report_period} 公司画像底稿",
        "",
        origin,
        "",
        f"来源版本：`{report.document_version}`。发布时点：{report.published_at}。",
        "",
    ]
    delivery_by_id = {entry["fact_id"]: entry for entry in delivery["facts"]} if delivery is not None else None
    if delivery is not None:
        out.extend(["研究分级不等于正式可信发布；以下普通正文不含隔离、仅源参考或重复对象。原底稿及后文复核记录保留审计。", ""])
        out.append("；".join(f"{delivery['labels'][state]} {count}" for state, count in delivery["counts"].items()))
        out.append(f"待核合计（含限用中的待核）：{delivery['pending_review_count']}。")
        out.append("")
        if workpaper.use_review is not None:
            out.extend([f"独立使用审核：{workpaper.use_review.reviewer}；依据：{workpaper.use_review.reference}；hash `{workpaper.use_review.reference_sha256}`。", ""])
    labels = dict(ELEMENT_LABELS)
    if workpaper.company_features is not None:
        labels[FEATURE_ELEMENT] = "公司特色与自主发现（可选）"
    elements = workpaper_elements(workpaper)
    for name, label in labels.items():
        element = elements[name]
        out.extend([f"## {label}", ""])
        for fact in element.facts:
            entry = delivery_by_id[fact.fact_id] if delivery_by_id is not None else None
            if entry is not None and entry["fact"] is None:
                continue
            refs = "、".join(f"[{value}](#{value.lower()})" for value in fact.evidence_ids)
            uncertain = f"【不确定：{fact.uncertainty_reason or '主体范围未明确'}】" if fact.uncertainty_reason or fact.subject_scope == "unclear" else ""
            label = f"【{entry['label']}】" if entry is not None else ""
            out.append(f"- {label}{uncertain}{fact.text}（{refs}）")
            if entry is not None and entry["state"] in {"pending", "limited"}:
                out.append("  - 使用限制：" + "；".join(entry["reasons"]))
            metrics = (WorkpaperMetric.model_validate(item) for item in entry["fact"]["metrics"]) if entry else fact.metrics
            for metric in metrics:
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
                entry = delivery_by_id[fact.fact_id] if delivery_by_id is not None else None
                if entry is not None and entry["fact"] is None:
                    continue
                refs = "、".join(f"[{value}](#{value.lower()})" for value in fact.evidence_ids)
                uncertain = f"【不确定：{fact.uncertainty_reason or '主体范围未明确'}】" if fact.uncertainty_reason or fact.subject_scope == "unclear" else ""
                label = f"【{entry['label']}】" if entry is not None else ""
                out.append(f"- {label}{uncertain}{fact.text}（{refs}）")
                if entry is not None and entry["state"] in {"pending", "limited"}:
                    out.append("  - 使用限制：" + "；".join(entry["reasons"]))
                metrics = (WorkpaperMetric.model_validate(item) for item in entry["fact"]["metrics"]) if entry else fact.metrics
                for metric in metrics:
                    out.append(f"  - {metric.name}：{_metric_display(metric)}；{metric.period}，{metric.subject_scope}。")
            out.extend(f"- 限制：{limit}" for limit in element.limitations)
            out.append("")
    if delivery is not None and delivery.get("suggested_acquisitions"):
        out.extend(["## 建议获取（不阻塞画像交付）", ""])
        for suggestion in delivery["suggested_acquisitions"]:
            element_label = ELEMENT_LABELS.get(suggestion["element"], suggestion["element"])
            out.append(f"- {element_label}：{suggestion['reason']}")
        out.append("")
    out.extend(["## 阅读范围与取舍", "", workpaper.reading_scope.selection_rationale, ""])
    for section in workpaper.reading_scope.sections:
        out.append(f"- {' / '.join(section.section_path)}，物理页{section.start_page}—{section.end_page}：{section.rationale}")
    out.extend(["", "停止理由：" + workpaper.reading_scope.stop_reason, ""])
    for scope in workpaper.reading_scope.unread_or_deferred:
        out.append("- 未读/后置：" + scope)
    for limitation in workpaper.limitations:
        out.append("- 限制：" + limitation)
    if delivery is not None:
        restricted = [e for e in delivery["facts"] if e["fact"] is None]
        if restricted:
            out.extend(["", "## 限用与隔离（不作普通结论）", ""])
            for entry in restricted:
                out.append(f"- {entry['fact_id']} / {entry['label']}：{'；'.join(entry['reasons'])}")
    if workpaper.generation is not None:
        out.extend(["", "## 模型处理记录", ""])
        if workpaper.generation.local_format_normalization is not None:
            normalization = workpaper.generation.local_format_normalization
            out.append(
                "本地格式标准化候选：未新增模型调用，原抽取合同仍为rejected，尚未进行二次审核。"
                if workpaper.quality_review is None else
                "本地格式标准化步骤未调用模型，原抽取合同仍为rejected；后续源复核调用及结果单列如下。"
            )
            out.append(f"- 标准化策略：`{normalization.policy}`；操作：{'、'.join(normalization.operations)}。")
            out.append(f"- 原始正文hash：`{normalization.original_content_sha256}`；标准化对象hash：`{normalization.normalized_object_sha256}`。")
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
            if finding.related_fact_id is not None:
                out.append(f"  - 补充关联原事实：{finding.related_fact_id}；本意见不撤回或替换该原事实，其他纠错意见仍有效。")
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
