#!/usr/bin/env python
"""Export local-standard financial field candidates for manual review."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import json_ready  # noqa: E402
from scripts.dev_validation.audit_sina_ths_financial_mapping import (  # noqa: E402
    _apply_machine_review_statuses,
)


CSV_COLUMNS = [
    "instrument_id",
    "report_period",
    "profile",
    "statement_type",
    "standard_field_key_candidate",
    "review_status",
    "unit_review_status",
    "known_units",
    "unknown_unit_sources",
    "sina_fields",
    "sina_values",
    "ths_fields",
    "ths_values",
    "eastmoney_fields",
    "eastmoney_values",
    "canonical_fact_candidates",
    "machine_review_status",
    "machine_review_confidence",
    "machine_review_reasons",
    "machine_review_blockers",
    "review_reason",
    "review_decision",
    "approved_local_field",
    "approved_semantic",
    "approved_canonical_unit",
    "approved_source_unit",
    "unit_multiplier",
    "relationship",
    "approved_sina_field",
    "approved_ths_metric",
    "approved_eastmoney_field",
    "review_notes",
]

GROUP_CSV_COLUMNS = [
    "group_id",
    "statement_type",
    "issue_type",
    "issue_name_cn",
    "recommended_action",
    "accepted_preliminary_rule",
    "what_to_verify_cn",
    "candidate_summary",
]

STATEMENT_CN = {
    "balance_sheet": "资产负债表",
    "profit_sheet": "利润表",
    "cash_flow_sheet": "现金流量表",
}

ISSUE_CN = {
    "per_share_basic_vs_diluted": "基本 EPS 与稀释 EPS 同值",
    "multi_source_same_value_duplicate": "多源同值但字段标签不同",
    "same_value_label_variant": "基础项与合计项同值",
    "missing_third_source": "缺少第三数据源佐证",
    "unit_review": "单位需要确认",
    "manual_semantic_review": "语义需要人工确认",
}

PRELIMINARY_RULES_CN = {
    "per_share_basic_vs_diluted": (
        "已采纳用户初步意见：基本 EPS 只能映射基本 EPS，稀释 EPS 只能映射稀释 EPS；"
        "即使两者数值相同，也不能交叉合并。"
    ),
    "same_value_label_variant": (
        "已采纳用户初步意见：在建工程/在建工程合计、其他应收款/其他应收款合计等通常是表述差异；"
        "本地标准优先采用简短字段名，合计字段可作为同义别名保留。"
    ),
    "multi_source_same_value_duplicate": (
        "已采纳部分用户意见：应付账款若与应付票据及应付账款同值，优先按更完整口径"
        "“应付票据及应付账款”处理；资产总计与负债和权益总计理论相等但语义不同，不能互相覆盖。"
    ),
}


def export_financial_mapping_review(
    evidence: Dict[str, Any],
    *,
    include_known: bool = False,
    include_machine_approved: bool = False,
) -> Dict[str, Any]:
    rows = []
    for sample in _iter_samples(evidence):
        audit = sample.get("audit", sample)
        cluster_payload = audit.get("local_standard_field_candidates", {})
        candidates = cluster_payload.get("candidates", [])
        if candidates and not any(candidate.get("machine_review") for candidate in candidates):
            candidates = _apply_machine_review_statuses(candidates)
        for candidate in candidates:
            machine_status = candidate.get("machine_review", {}).get("status")
            if not include_machine_approved and machine_status in {
                "machine_approved_candidate",
                "machine_rejected_candidate",
            }:
                continue
            if not include_known and candidate.get("review_status") == "known_canonical_candidate":
                continue
            rows.append(_review_row(sample, candidate))
    rows.sort(
        key=lambda row: (
            row["profile"],
            row["statement_type"],
            row["standard_field_key_candidate"],
            row["sina_fields"],
            row["ths_fields"],
        )
    )
    return {
        "summary": {
            "row_count": len(rows),
            "include_known": include_known,
            "review_status_counts": _counts(row["review_status"] for row in rows),
            "unit_review_status_counts": _counts(row["unit_review_status"] for row in rows),
            "machine_review_status_counts": _counts(
                row["machine_review_status"] for row in rows
            ),
        },
        "rows": rows,
    }


def _iter_samples(evidence: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    samples = evidence.get("samples")
    if isinstance(samples, list):
        yield from (sample for sample in samples if isinstance(sample, dict))
        return
    yield evidence


def _review_row(sample: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    sources = candidate.get("sources", {})
    unit_review = candidate.get("unit_review", {})
    machine_review = candidate.get("machine_review", {})
    return {
        "instrument_id": sample.get("instrument_id"),
        "report_period": sample.get("report_period"),
        "profile": candidate.get("profile") or sample.get("profile"),
        "statement_type": candidate.get("statement_type"),
        "standard_field_key_candidate": candidate.get("standard_field_key_candidate"),
        "review_status": candidate.get("review_status"),
        "unit_review_status": unit_review.get("status"),
        "known_units": ",".join(unit_review.get("known_units", []) or []),
        "unknown_unit_sources": ",".join(unit_review.get("unknown_unit_sources", []) or []),
        "sina_fields": _join_source_items(sources, "sina_report", "field_name"),
        "sina_values": _join_source_items(sources, "sina_report", "value"),
        "ths_fields": _join_source_items(sources, "ths_report", "field_name"),
        "ths_values": _join_source_items(sources, "ths_report", "value"),
        "eastmoney_fields": _join_source_items(sources, "eastmoney_report", "field_name"),
        "eastmoney_values": _join_source_items(sources, "eastmoney_report", "value"),
        "canonical_fact_candidates": ",".join(candidate.get("canonical_fact_candidates", []) or []),
        "machine_review_status": machine_review.get("status"),
        "machine_review_confidence": machine_review.get("confidence"),
        "machine_review_reasons": ",".join(machine_review.get("reasons", []) or []),
        "machine_review_blockers": ",".join(machine_review.get("blockers", []) or []),
        "review_reason": _review_reason(candidate),
        "review_decision": machine_review.get("suggested_decision") or "",
        "approved_local_field": machine_review.get("suggested_local_field") or "",
        "approved_semantic": machine_review.get("suggested_semantic") or "",
        "approved_canonical_unit": machine_review.get("suggested_canonical_unit") or "",
        "approved_source_unit": machine_review.get("suggested_source_unit") or "",
        "unit_multiplier": machine_review.get("suggested_unit_multiplier") or "",
        "relationship": machine_review.get("suggested_relationship") or "",
        "approved_sina_field": machine_review.get("suggested_sina_field") or "",
        "approved_ths_metric": machine_review.get("suggested_ths_metric") or "",
        "approved_eastmoney_field": machine_review.get("suggested_eastmoney_field") or "",
        "review_notes": "",
    }


def _join_source_items(
    sources: Dict[str, Any],
    source_name: str,
    field_name: str,
) -> str:
    values = [
        item.get(field_name)
        for item in sources.get(source_name, [])
        if isinstance(item, dict) and item.get(field_name) is not None
    ]
    return " | ".join(str(value) for value in values)


def _review_reason(candidate: Dict[str, Any]) -> str:
    reasons = []
    if candidate.get("review_status") != "known_canonical_candidate":
        reasons.append("semantic_review_required")
    unit_status = candidate.get("unit_review", {}).get("status")
    if unit_status != "known_units_match":
        reasons.append(f"unit_review:{unit_status}")
    machine_blockers = candidate.get("machine_review", {}).get("blockers") or []
    if machine_blockers:
        reasons.append("machine_blockers:" + "|".join(machine_blockers))
    if not candidate.get("sources", {}).get("eastmoney_report"):
        reasons.append("missing_eastmoney_enrichment")
    return ",".join(reasons) or "known_canonical_candidate"


def _counts(values: Iterable[Optional[str]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        key = str(value or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def write_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})


def write_group_csv(result: Dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    groups = _build_validation_groups(result["rows"])
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=GROUP_CSV_COLUMNS)
        writer.writeheader()
        for group in groups:
            writer.writerow({column: group.get(column, "") for column in GROUP_CSV_COLUMNS})


def write_markdown(result: Dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validation_groups = _build_validation_groups(result["rows"])
    lines = [
        "# 财务字段映射人工复核说明",
        "",
        "## 摘要",
        "",
        f"- 需要人工复核的明细行数：`{result['summary']['row_count']}`",
        f"- 机器复核状态：`{json.dumps(result['summary']['machine_review_status_counts'], ensure_ascii=False, sort_keys=True)}`",
        f"- 单位复核状态：`{json.dumps(result['summary']['unit_review_status_counts'], ensure_ascii=False, sort_keys=True)}`",
        "",
        "## 审核方法",
        "",
        "- 优先看“分组复核表”，不要逐行看明细。",
        "- 如果分组中的“已采纳初步规则”符合你的判断，该组后续可按规则批量处理。",
        "- 如果分组里存在你认为不能合并的字段，只需要指出字段名和原因。",
        "- 不能只因为数值相同就合并；必须同时满足报表类型、字段含义、期间属性、合并口径和单位一致。",
        "",
        "## 分组复核",
        "",
    ]
    for group in validation_groups:
        lines.extend(
            [
                f"### {group['group_id']} {group['statement_name_cn']}：{group['issue_name_cn']}",
                "",
                f"- 为什么需要确认：{group['what_to_verify_cn']}",
                f"- 已采纳初步规则：{group['accepted_preliminary_rule']}",
                f"- 建议处理：{group['recommended_action']}",
                "- 候选映射：",
                *[f"  - {item}" for item in group.get("candidate_items", [])],
                "",
            ]
        )
    lines.extend(
        [
            "",
            "## 明细证据",
            "",
            "下面是分组表的底层证据，通常不需要逐行审核；只有当分组摘要看不清时再看这里。",
            "",
        ]
    )
    lines.extend([
        "| profile | 报表 | 本地候选字段 | 新浪字段 | 同花顺字段 | 东财字段 | 单位 | 进入人工复核的原因 |",
        "|---|---|---|---|---|---|---|---|",
    ])
    for row in result["rows"]:
        lines.append(
            "| {profile} | {statement_type} | {standard_field_key_candidate} | "
            "{sina_fields} | {ths_fields} | {eastmoney_fields} | "
            "{unit_review_status}:{known_units} | {review_reason} |".format(
                **{key: _markdown_cell(row.get(key)) for key in CSV_COLUMNS}
            )
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_validation_groups(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        blockers = row.get("machine_review_blockers") or ""
        issue_type = _issue_type_from_blockers(blockers)
        key = (str(row.get("statement_type") or ""), issue_type)
        grouped.setdefault(key, []).append(row)

    groups = []
    for index, ((statement_type, issue_type), group_rows) in enumerate(
        sorted(grouped.items()),
        start=1,
    ):
        candidate_items = sorted(
            {
                f"`{row.get('standard_field_key_candidate')}`：新浪 `{row.get('sina_fields')}`；"
                f"同花顺 `{row.get('ths_fields')}`；东财 `{row.get('eastmoney_fields')}`"
                for row in group_rows
            }
        )
        candidate_summary = "; ".join(candidate_items)
        groups.append(
            {
                "group_id": f"G{index:02d}",
                "statement_type": statement_type,
                "statement_name_cn": STATEMENT_CN.get(statement_type, statement_type),
                "issue_type": issue_type,
                "issue_name_cn": ISSUE_CN.get(issue_type, issue_type),
                "recommended_action": _recommended_action_for_issue(issue_type),
                "accepted_preliminary_rule": _preliminary_rule_for_group(
                    issue_type,
                    statement_type=statement_type,
                ),
                "what_to_verify_cn": _what_to_verify_for_issue(
                    issue_type,
                    statement_type=statement_type,
                ),
                "candidate_items": candidate_items,
                "candidate_summary": candidate_summary,
            }
        )
    return groups


def _issue_type_from_blockers(blockers: str) -> str:
    blocker_set = {part.strip() for part in blockers.split(",") if part.strip()}
    if "per_share_same_value_ambiguity" in blocker_set:
        return "per_share_basic_vs_diluted"
    if "ambiguous_eastmoney_field" in blocker_set and (
        "duplicate_local_field_candidate" in blocker_set
        or "duplicate_ths_field_candidate" in blocker_set
    ):
        return "multi_source_same_value_duplicate"
    if "duplicate_local_field_candidate" in blocker_set or "duplicate_ths_field_candidate" in blocker_set:
        return "same_value_label_variant"
    if "missing_eastmoney_match" in blocker_set:
        return "missing_third_source"
    if "unit_conflict" in blocker_set or "unresolved_unit" in blocker_set:
        return "unit_review"
    return "manual_semantic_review"


def _recommended_action_for_issue(issue_type: str) -> str:
    actions = {
        "per_share_basic_vs_diluted": "只批准基本对基本、稀释对稀释；同值不是合并理由。",
        "multi_source_same_value_duplicate": "选择语义最精确的字段；总项、附注项或等式另一边只作相关字段。",
        "same_value_label_variant": "确认基础项与合计项是否只是同一字段的不同写法。",
        "missing_third_source": "缺第三源佐证时暂不批准，先补样本或公告证据。",
        "unit_review": "先确认单位和换算倍率，再决定是否批准。",
        "manual_semantic_review": "确认字段含义、期间属性和合并口径。",
    }
    return actions.get(issue_type, actions["manual_semantic_review"])


def _preliminary_rule_for_group(issue_type: str, *, statement_type: str = "") -> str:
    if issue_type == "multi_source_same_value_duplicate" and statement_type == "cash_flow_sheet":
        return (
            "暂无用户确认规则。我的建议是优先采用现金流量表主表字段，例如 "
            "`NETCASH_OPERATE`、`CCE_ADD`；带 `NOTE` 的字段作为附注/补充资料字段，不与主表字段合并。"
        )
    if issue_type == "multi_source_same_value_duplicate" and statement_type == "profit_sheet":
        return (
            "暂无用户确认规则。我的建议是按会计项目拆分：`净利润` 不等同于 `持续经营净利润`；"
            "`少数股东损益` 不等同于 `归属于少数股东的综合收益总额`；"
            "`其他综合收益` 不等同于外币折算差额或归母其他综合收益。"
        )
    return PRELIMINARY_RULES_CN.get(issue_type, "暂无已采纳规则，需要本轮确认。")


def _what_to_verify_for_issue(issue_type: str, *, statement_type: str = "") -> str:
    if issue_type == "multi_source_same_value_duplicate" and statement_type == "cash_flow_sheet":
        return (
            "现金流量表里东财同时给出主表字段和附注/补充资料字段，例如 "
            "`NETCASH_OPERATE` 与 `NETCASH_OPERATENOTE` 数值相同。"
            "本地标准字段应优先映射主表项目，附注字段只能作为相关字段或单独字段，不能混入同一个 canonical 字段。"
        )
    if issue_type == "multi_source_same_value_duplicate" and statement_type == "profit_sheet":
        return (
            "利润表里多个不同层级的项目在该样本中恰好同值，例如“净利润”和“持续经营净利润”、"
            "“少数股东损益”和“归属于少数股东的综合收益总额”、"
            "“其他综合收益”和其中的外币折算/归母部分。"
            "这些字段即使同值也不一定同义，需要按会计含义拆开。"
        )
    descriptions = {
        "per_share_basic_vs_diluted": (
            "该样本中基本 EPS 和稀释 EPS 数值相同，导致三源按数值匹配时互相撞在一起。"
            "需要确认的是映射关系，而不是数值是否相等。"
        ),
        "multi_source_same_value_duplicate": (
            "不同数据源存在多个字段同值，但这些字段可能是简写、完整字段、附注字段或会计等式另一边。"
            "需要确认哪个字段才是本地标准字段的精确语义。"
        ),
        "same_value_label_variant": (
            "字段名只差“合计”等修饰词，数值也相同。需要确认在当前报表 profile 下是否可以作为同义别名。"
        ),
        "missing_third_source": "Sina 和 THS 可以互相匹配，但缺少 Eastmoney 或官方字段辅助解释，需要补证据。",
        "unit_review": "字段数值可能一致，但单位或倍率尚不明确。需要确认单位。",
        "manual_semantic_review": "机器规则无法判断字段层级或会计含义，需要人工确认语义。",
    }
    return descriptions.get(issue_type, descriptions["manual_semantic_review"])


def _markdown_cell(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export financial field mapping candidates for manual review."
    )
    parser.add_argument("--evidence-path", required=True, help="Audit/live-audit JSON path")
    parser.add_argument("--output-csv", help="Optional CSV review output path")
    parser.add_argument("--output-md", help="Optional Markdown review output path")
    parser.add_argument("--output-json", help="Optional normalized JSON review output path")
    parser.add_argument("--output-group-csv", help="Optional grouped review CSV output path")
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Include known canonical candidates; default exports only uncertain rows.",
    )
    parser.add_argument(
        "--include-machine-approved",
        action="store_true",
        help="Include machine-approved candidates; default exports only fields needing review.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    evidence_path = Path(args.evidence_path)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    result = export_financial_mapping_review(
        evidence,
        include_known=args.include_known,
        include_machine_approved=args.include_machine_approved,
    )
    if args.output_csv:
        write_csv(result["rows"], Path(args.output_csv))
    if args.output_md:
        write_markdown(result, Path(args.output_md))
    if args.output_group_csv:
        write_group_csv(result, Path(args.output_group_csv))
    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(
            json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if not any([args.output_csv, args.output_md, args.output_json, args.output_group_csv]):
        print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
