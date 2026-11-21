"""Human-readable rendering of the existing safe annual-profile projection."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from html import escape
import re
from typing import Any

from .workpaper import ELEMENT_LABELS, FEATURE_ELEMENT, SUPPLY_CHAIN_LABELS, WorkpaperMetric, _metric_display


def _text(value: Any) -> str:
    """Keep source/model text literal, never an active image, HTML or link."""
    value = escape(str(value if value is not None else "未知"), quote=False)
    return re.sub(r"([\\`*_\[\]{}#!|])", r"\\\1", value).replace("\n", "<br>")


def render_annual_profiles(profiles: Sequence[Mapping[str, Any]]) -> str:
    """Render only API-visible content; do not reclassify, reconcile or infer."""
    out = ["# 当前公司画像（同报告多轮研究投影）", "",
           "与 annual_profiles.json 使用同一内容。保留待核、保真限用及未获取项；不代表正式可信发布或生产授权。", "",
           "只覆盖已选章节；全报告完整性、跨轮一致性未评估。不同阅读批次的相似断言保留归属，不自动去重、加总或判定缺项已补齐。", ""]
    for number, profile in enumerate(profiles, 1):
        report = profile["report"]
        out.append(f"- [{_text(profile['instrument_id'])} / {_text(report['report_period'])}](#company-{number})")
    for number, profile in enumerate(profiles, 1):
        out.extend(_profile_lines(profile, number))
    return "\n".join(out) + "\n"


def _profile_lines(profile: Mapping[str, Any], number: int) -> list[str]:
    report, readings = profile["report"], profile["readings"]
    visible = [entry for entry in profile["facts"] if entry["fact"] is not None]
    used = {(entry["reading_work_id"], eid) for entry in visible for eid in entry["evidence_ids"]}
    references = {}
    for reading in readings:
        for evidence in reading["evidence"]:
            key = (reading["work_id"], evidence["evidence_id"])
            if key in used:
                references[key] = (f"source-{number}-{len(references)+1}", evidence)
    out = ["", f'<a id="company-{number}"></a>', "",
           f"## {_text(profile['instrument_id'])}｜{_text(report['report_period'])}", "",
           f"报告：{_text(report['report_id'])}；文档版本：{_text(report['document_version'])}；发布时点：{_text(report['published_at'])}。", "",
           f"本报告阅读 {len(readings)} 轮，当前可读 {len(visible)} 条；是内容计数，不是准确率或完整性评分。", ""]
    labels = {**ELEMENT_LABELS, **SUPPLY_CHAIN_LABELS, FEATURE_ELEMENT: "公司特色与自主发现（可选）"}
    for name, label in labels.items():
        entries = [entry for entry in visible if entry["element"] == name]
        if name not in ELEMENT_LABELS and not entries:
            continue  # Optional empty sections are not mandatory profile failures.
        if name in SUPPLY_CHAIN_LABELS:
            label = "供应链（可选）／" + label
        out.extend([f"### {label}", ""])
        if not entries:
            out.extend(["未获取成功；详见下方缺项与建议，不表示报告未披露或该业务不存在。", ""])
        for entry in entries:
            fact, work_id = entry["fact"], entry["reading_work_id"]
            out.extend([f"【{_text(entry['label'])}】{_text(fact['text'])}", "",
                        f"主体：{_text(fact['subject_scope'])}；期间：{_text(fact['period'])}；事实标识：{_text(work_id)} / {_text(entry['fact_id'])}。", ""])
            refs = []
            for eid in entry["evidence_ids"]:
                source = references.get((work_id, eid))
                refs.append(f"[{_text(eid)}](#{source[0]})" if source else f"{_text(eid)}（来源未附）")
            out.extend(["来源：" + "、".join(refs) + "。", ""])
            restrictions = [*entry.get("reasons", ())]
            if fact.get("uncertainty_reason"):
                restrictions.append(fact["uncertainty_reason"])
            if restrictions:
                out.extend(["使用限制：" + "；".join(_text(item) for item in dict.fromkeys(restrictions)), ""])
            for raw in fact["metrics"]:
                metric = WorkpaperMetric.model_validate(raw)
                out.append(f"- {_text(metric.name)}：{_text(_metric_display(metric))}；主体 {_text(metric.subject_scope)}；期间 {_text(metric.period)}。")
                if metric.currency:
                    out.append(f"  币种：{_text(metric.currency)}。")
                if metric.comparison_basis:
                    out.append(f"  比较口径：{_text(metric.comparison_basis)}。")
            if fact["metrics"]:
                out.append("")
    for field, title in (("information_gaps", "缺项与不确定"), ("suggested_acquisitions", "建议获取（不阻塞交付）")):
        out.extend([f"### {title}", ""])
        items = profile[field]
        if not items:
            out.extend(["本次投影无记录，不代表全报告无遗漏。", ""])
        for item in items:
            status = "历史，已被复核后继替代" if item.get("active") is False else "当前记录"
            detail = "；".join(item.get("reasons", ())) if field == "information_gaps" else item["reason"]
            out.append(f"- {status}／{_text(labels.get(item['element'], item['element']))}：{_text(detail or item.get('status'))}；阅读 {_text(item['reading_work_id'])}。")
            if item.get("supplemental_reading_work_ids"):
                out.append("  已有相关补读但未全面重评：" + "、".join(_text(wid) for wid in item["supplemental_reading_work_ids"]) + "。")
        out.append("")
    out.extend(["### 逐轮阅读与实际模型", ""])
    for reading in readings:
        out.extend([f"#### {_text(reading['work_id'])}", "",
                    f"内容来源类型：{_text(reading['origin'])}；源复核状态：{_text(reading['review_status'])}。", ""])
        if reading.get("superseded_by_review_work_id"):
            out.extend(["历史轮次已被复核后继替代：" + _text(reading["superseded_by_review_work_id"]) + "；不重复展示旧正文。", ""])
        scope = reading["reading_scope"]
        out.extend(["阅读取舍：" + _text(scope["selection_rationale"]), ""])
        for section in scope["sections"]:
            out.append(f"- {_text(' / '.join(section['section_path']))}；物理页 {section['start_page']}—{section['end_page']}。")
        out.extend(["", "停止理由：" + _text(scope["stop_reason"]), ""])
        for item in (*scope["unread_or_deferred"], *reading["limitations"]):
            out.append("- 未读/限制：" + _text(item))
        out.append("")
        generation = reading.get("generation") or {}
        for stage, call in (("选章", generation.get("selection")), ("抽取", generation.get("extraction"))):
            if call:
                out.append(f"- {stage}：{_call_label(call)}")
        for call in reading["review_calls"]:
            out.append(f"- 保存的复核记录（可能继承，非新增调用声明）：{_text(call['purpose'])}；{_call_label(call)}")
        out.append("")
    hidden = [entry for entry in profile["facts"] if entry["fact"] is None]
    if hidden:
        out.extend(["### 未展示记录（不含原正文及独占引文）", ""])
        for entry in hidden:
            out.append(f"- {_text(entry['reading_work_id'])} / {_text(entry['fact_id'])}：{_text(entry['state'])}。")
        out.append("")
    out.extend(["### 当前可读内容的原文来源", ""])
    for (work_id, eid), (anchor, evidence) in references.items():
        out.extend([f'<a id="{anchor}"></a>', "",
                    f"#### {_text(work_id)} / {_text(eid)}", "",
                    _text(" / ".join(evidence["section_path"])), ""])
        for span in evidence["source_spans"]:
            out.extend([f"物理页 {span['page']}；原页字符 {span['start']}—{span['end']}：", ""])
            # Indented code preserves tables/newlines and neutralizes embedded markup.
            out.extend("    " + line for line in span["quote"].splitlines())
            out.append("")
    return out


def _call_label(call: Mapping[str, Any]) -> str:
    return (f"模型 {_text(call.get('model'))}；来源 {_text(call.get('source_label'))}；"
            f"profile {_text(call.get('selected_profile') or call.get('logical_profile'))}；"
            f"思考强度 {_text(call.get('reasoning_effort'))}；合同状态 {_text(call.get('contract_status'))}。")
