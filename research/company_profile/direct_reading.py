"""Core-chapter reading for one report.

A body candidate is verified as its own evidence unit and saved before the
next unit. The report-level source check lists what the text supports and
does not include those candidates. It cannot block a unit already verified.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

from research.company_profile.contracts import (
    ReportSemanticRequestV3,
    ReportSemanticResponseV3,
    ReportSourceCondition,
    ReportViewCheck,
    ReportViewClaim,
    SemanticProviderError,
)
from research.company_profile.models import Evidence, ReportIdentity, SemanticRecord, TextAnchor
from research.company_profile.reading_budget import (
    request_payload_bytes,
    request_reservation,
)
from research.company_profile.report_interpretation import (
    VIEWS,
    ReportInterpretation,
    _bind_source_evidence,
    _located_conditions,
    _pages,
    fits_card_reservation,
    cited_quote_pages,
    governing_subject_heading,
    pack_fitting_batches,
)
from utils.llm.budget import LlmRunBudget
from utils.llm.errors import LlmDeadlineExceededError, LlmError

NAV_OUTPUT = 1000
BODY_OUTPUT = 2500
VERIFY_OUTPUT = 2000
SOURCE_OUTPUT = 4000
RESERVATION_LIMIT = 24_000
LEDGER_LIMIT = 79_000


def _nav_output(identity) -> int:
    return int((identity or {}).get("nav_output_tokens") or NAV_OUTPUT)


def _source_output(identity) -> int:
    return int((identity or {}).get("source_output_tokens") or SOURCE_OUTPUT)


def _body_output(identity) -> int:
    return int((identity or {}).get("body_output_tokens") or BODY_OUTPUT)


def _verify_output(identity) -> int:
    return int((identity or {}).get("verify_output_tokens") or VERIFY_OUTPUT)


def _fit(pages, *, purpose, source_scope, report, identity, output, limit):
    return fits_card_reservation(
        pages,
        reservation_limit=limit,
        purpose=purpose,
        source_scope=source_scope,
        report=report,
        identity=identity,
        max_output_tokens=output,
    )


def _business_groups(conditions: Sequence[ReportSourceCondition]) -> list[list[ReportSourceCondition]]:
    """Interleave current core dimensions, and do not draft the non-current tail."""
    order = (
        "principal_business",
        "revenue_model",
        "products_services",
        "input_exposure",
        "supply_chain_role",
        "upstream_downstream",
        "output_exposure",
    )
    buckets: dict[str, list[ReportSourceCondition]] = {name: [] for name in order}
    for condition in conditions:
        if condition.statement_status == "current" and condition.dimension in buckets:
            buckets[condition.dimension].append(condition)
    groups: list[list[ReportSourceCondition]] = []
    while any(buckets.values()):
        group: list[ReportSourceCondition] = []
        for name in order:
            if not buckets[name]:
                continue
            group.append(buckets[name].pop(0))
            if len(group) == 6:
                break
        if group:
            groups.append(group)
    return groups


_UNTRUSTED_LIMITATIONS = {
    "pending_verification",
    "claim_or_dependency_not_verified",
    "evidence_not_located",
    "subject_scope_omitted",
    "retracted_pending_correction",
    "not_verified",
    "pending",
}
_CONCENTRATION_MARKS = (
    "前五名客户",
    "前五名供应商",
    "占年度销售总额",
    "占年度采购总额",
)
_COST_ONLY_MARKS = ("成本构成", "原辅材料价格")
_ROLE_STATEMENT_MARKS = (
    "生产线",
    "林浆纸",
    "林-浆-纸",
    "一体化",
    "产业链",
    "上游",
    "下游",
    "制浆",
    "自行生产",
    "延伸服务",
    "链主",
)
_UNIT_TO_YUAN = {
    "元": 1.0,
    "万元": 10_000.0,
    "百万元": 1_000_000.0,
    "亿元": 100_000_000.0,
}
_ANSWER_AMOUNT = re.compile(
    r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(亿元|万元|百万元|元)?"
)
_METRIC_SUFFIXES = ("规模", "收入", "余额", "总额", "业务")
_METRIC_STOP = frozenset(
    {
        "增长",
        "增速",
        "增幅",
        "变动",
        "增加",
        "减少",
        "同比",
        "分别",
        "其中",
        "达到",
        "实现",
        "报告",
        "报告期",
        "报告期内",
        "报告期末",
        "较上",
        "较上年",
        "上年末",
        "较上年末增加",
        "较上年末减少",
        "增加",
        "减少",
        "本集团",
        "本公司",
        "公司",
        "亿元",
        "万元",
        "百万元",
        "万亿元",
    }
)


def _latest_check(checks, target_type: str, target_id: str):
    found = None
    for check in checks:
        if check.target_type == target_type and check.target_id == target_id:
            found = check
    return found


def _verified_claims(claims, checks):
    """Claims a later pass still stands on. A label alone is not enough."""
    trusted = []
    for item in claims:
        if item.status != "observed" or item.limitation in _UNTRUSTED_LIMITATIONS:
            continue
        latest = _latest_check(checks, "claim", item.claim_id)
        if latest is not None and latest.status == "pass":
            trusted.append(item)
    return trusted


def _has_operating_role_statement(text: str) -> bool:
    """Locate candidate pages. Words here never complete a dimension."""
    return any(mark in str(text or "") for mark in _ROLE_STATEMENT_MARKS)


def _is_concentration_or_cost_substitute(text: str) -> bool:
    """Customer/supplier share or a cost table is not the company's operating role."""
    body = str(text or "")
    return any(mark in body for mark in (*_CONCENTRATION_MARKS, *_COST_ONLY_MARKS))


def _latest_named_condition_check(checks, needle: str):
    """Last check whose target is this condition. An older pass does not count."""
    found = None
    for check in checks:
        if check.target_type != "condition":
            continue
        target = str(check.target_id or "")
        tail = needle.split(":")[-1]
        if target == needle or (
            target.endswith(":" + tail) and target.split(":")[-1] == tail
        ):
            found = check
    return found


def _is_operating_role_question(condition_id: str) -> bool:
    """Which question this is. A matching id does not itself pass."""
    tail = str(condition_id or "").split(":")[-1]
    return tail in {"operating-role", "operating_roles"} or tail.endswith(
        ("operating-role", "operating_roles")
    )


def _operating_role_established(claims, checks) -> bool:
    """Latest covering pass on the role answer. The question id may be native."""
    named = _latest_named_condition_check(checks, "core-question:operating-role")
    if _condition_covers_verified_claim(named, claims, checks):
        return True
    verified = {item.claim_id: item for item in _verified_claims(claims, checks)}
    latest_for: dict[str, object] = {}
    for check in checks:
        if check.target_type == "condition" and check.target_id:
            latest_for[str(check.target_id)] = check
    for claim in verified.values():
        if _is_concentration_or_cost_substitute(str(claim.answer or "")):
            continue
        for condition_id in claim.condition_ids or ():
            if not _is_operating_role_question(str(condition_id)):
                continue
            check = latest_for.get(str(condition_id))
            if check is None:
                tail = str(condition_id).split(":")[-1]
                check = next(
                    (
                        item
                        for target, item in latest_for.items()
                        if target.endswith(":" + tail)
                    ),
                    None,
                )
            if (
                check is not None
                and check.status == "pass"
                and check.covers_target_type == "claim"
                and check.covers_target_id == claim.claim_id
            ):
                return True
    return False


def _amount_in_yuan(amount: str, unit: str) -> float | None:
    try:
        value = float(str(amount or "").replace(",", ""))
    except ValueError:
        return None
    scale = _UNIT_TO_YUAN.get(unit or "元")
    if scale is None:
        return None
    return value * scale


def _amounts_equivalent(left: str, left_unit: str, right: str, right_unit: str) -> bool:
    """万元/亿元/元 舍入差别，在主体期间一致时视为同一总额。"""
    first = _amount_in_yuan(left, left_unit or "元")
    second = _amount_in_yuan(right, right_unit or "元")
    if first is None or second is None:
        return False
    coarse = max(
        _UNIT_TO_YUAN.get(left_unit or "元", 1.0),
        _UNIT_TO_YUAN.get(right_unit or "元", 1.0),
    )
    return abs(first - second) <= max(1.0, 0.01 * coarse)


def _condition_covers_verified_claim(latest, claims, checks) -> bool:
    """A bare pass, a missing object, or a later deny does not answer the question."""
    if latest is None or latest.status != "pass":
        return False
    verified = {item.claim_id for item in _verified_claims(claims, checks)}
    return (
        latest.covers_target_type == "claim"
        and latest.covers_target_id in verified
    )


def _period_revenue_verified(claims, checks) -> bool:
    latest = _latest_named_condition_check(
        checks, "core-question:period-operating-revenue"
    )
    return _condition_covers_verified_claim(latest, claims, checks)


def _completeness_gaps(claims, conditions, checks, records=()) -> list[str]:
    """Three views and covered major conditions decide completeness."""
    def latest_check(target_type: str, target_id: str):
        return _latest_check(checks, target_type, target_id)

    verified_claims = {item.claim_id for item in _verified_claims(claims, checks)}
    observed = {
        item.dimension for item in claims if item.claim_id in verified_claims
    }
    found = []
    for view, dimensions in VIEWS.items():
        if not observed & set(dimensions):
            found.append(f"view_not_substantive:{view}")
    for dimension in ("principal_business", "products_services", "revenue_model"):
        if dimension not in observed:
            found.append(f"core_dimension_missing:{dimension}")
    if not _operating_role_established(claims, checks):
        found.append("core_dimension_missing:operating_role")
    claims_by_id = {item.claim_id: item for item in claims}
    accepted_records = {item.record_id for item in records}
    record_verdicts = {
        check.target_id: check.status
        for check in checks
        if check.target_type == "record"
    }
    for condition in conditions:
        if not condition.major or condition.statement_status != "current":
            continue
        latest = latest_check("condition", condition.condition_id)
        covered = False
        check = latest
        if check is not None and check.status == "pass":
            if (
                check.covers_target_type == "claim"
                and check.covers_target_id in verified_claims
            ):
                claim = claims_by_id[check.covers_target_id]
                dependencies = set(claim.supporting_record_ids)
                if not dependencies or (
                    dependencies <= accepted_records
                    and all(record_verdicts.get(key) in {None, "pass"} for key in dependencies)
                ):
                    covered = True
            elif (
                check.covers_target_type == "record"
                and check.covers_target_id in accepted_records
                and record_verdicts.get(check.covers_target_id) in {None, "pass"}
            ):
                covered = True
        if covered:
            continue
        if latest is not None and latest.status == "supplementary":
            found.append(f"supplementary_gap:{condition.condition_id}")
            continue
        found.append(f"major_condition_not_verified:{condition.condition_id}")
    return found


def operating_revenue_statement_pages(sources) -> list[int]:
    """Income-statement pages, not the pages an older product-line claim happened to cite."""
    found = []
    for source in sources:
        text = str(source.get("text") or "")
        if "利润表" not in text or "营业收入" not in text or "本期" not in text:
            continue
        found.append(int(source["page"]))
    return found[:1]


def _condition_id_names(item: str, cid: str, *, tail_unique: bool) -> bool:
    """Full source-request identity. A shared short tail is not enough."""
    if item == cid:
        return True
    shorter, longer = (item, cid) if len(item) <= len(cid) else (cid, item)
    if not longer.endswith(":" + shorter):
        return False
    tail = cid.split(":")[-1]
    if shorter == tail:
        return tail_unique
    return True


def claims_covering_condition(condition, candidates) -> list:
    """Claims whose saved condition ids actually name this condition.

    A same-dimension product claim is not a cover for a different question.
    Two businesses that share only a short tail stay separate.
    """
    cid = str(condition.condition_id)
    tail = cid.split(":")[-1]
    named_ids = []
    for claim in candidates:
        named_ids.extend(str(item) for item in (getattr(claim, "condition_ids", ()) or ()))
    tail_hits = {
        item
        for item in named_ids
        if item == tail or item.endswith(":" + tail)
    }
    tail_unique = len(tail_hits) == 1
    matched = []
    for claim in candidates:
        if getattr(claim, "status", None) not in {None, "observed"}:
            continue
        ids = [str(item) for item in (getattr(claim, "condition_ids", ()) or ())]
        if any(_condition_id_names(item, cid, tail_unique=tail_unique) for item in ids):
            matched.append(claim)
    return matched


def recheck_claims_for(condition, candidates) -> list:
    """Claims that can be shown to a coverage check. A link is not itself a pass.

    Page, quote window and amount only nominate candidates, including across
    pages. They do not decide coverage.
    """
    linked = [
        claim
        for claim in claims_covering_condition(condition, candidates)
        if getattr(claim, "status", None) == "observed"
    ]
    left = "".join(
        "".join(str(item.anchor.bounded_quote or "").split())
        for item in condition.evidence
    )
    amounts = re.findall(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?", left)
    description = "".join(str(getattr(condition, "description", "") or "").split())

    def related(claim) -> bool:
        if getattr(claim, "status", None) != "observed":
            return False
        if claim in linked:
            return False
        right = "".join(str(claim.answer or "").split())
        if any(amount in right for amount in amounts):
            return True
        if len(left) < 16 and len(description) < 16:
            return False
        source = left if len(left) >= 16 else description
        return any(
            source[index : index + 16] in right
            for index in range(0, len(source) - 15)
            if "报告期内" not in source[index : index + 16]
            and "同比增长" not in source[index : index + 16]
            and "较上年末" not in source[index : index + 16]
        )

    found = [claim for claim in candidates if related(claim)]
    return [*linked, *found]


def compatible_evidence_targets(claims, conditions) -> list:
    """Give a coverage request one body per evidence id.

    A saved claim and a rebound condition can share an evidence id after the
    quote is located. The wire refuses that conflict and the condition would
    never be checked. The stored objects stay unchanged.
    """
    catalog: dict[str, str] = {}

    def body(evidence) -> str:
        return json.dumps(
            evidence.model_dump(mode="json", exclude={"report"}),
            ensure_ascii=False,
            sort_keys=True,
        )

    for claim in claims:
        for evidence in claim.evidence:
            catalog.setdefault(evidence.evidence_id, body(evidence))
    fixed = []
    for condition in conditions:
        updated = []
        changed = False
        for evidence in condition.evidence:
            dumped = body(evidence)
            prior = catalog.get(evidence.evidence_id)
            if prior is None or prior == dumped:
                catalog.setdefault(evidence.evidence_id, dumped)
                updated.append(evidence)
                continue
            number = 2
            new_id = f"{evidence.evidence_id}__{number}"
            while new_id in catalog:
                number += 1
                new_id = f"{evidence.evidence_id}__{number}"
            renamed = evidence.model_copy(update={"evidence_id": new_id})
            catalog[new_id] = body(renamed)
            updated.append(renamed)
            changed = True
        fixed.append(
            condition.model_copy(update={"evidence": tuple(updated)})
            if changed
            else condition
        )
    return fixed


def _current_gaps(accumulated, claims, conditions, checks, records=()) -> list[str]:
    """Publish the latest coverage, and keep live operational gaps.

    A resolved condition gap is not revived from the historical list. A later
    block or a still-missing core question remains.
    """
    dynamic = _completeness_gaps(claims, conditions, checks, records)
    live = []
    for gap in accumulated:
        if gap.startswith(
            (
                "major_condition_not_verified:",
                "major_condition_not_in_synthesis:",
                "supplementary_gap:",
            )
        ):
            continue
        if gap.startswith(
            (
                "necessary_unread:",
                "necessary_fragment",
                "navigation_incomplete:",
                "direct_core_",
                "isolated_unreliable_claim:",
                "chapter_",
                "evidence_not_located:",
                "major_evidence_not_located:",
                "claim_pending:",
                "verify_unit_not_admitted:",
                "company_synthesis_context_truncated",
                "retracted_for_correction:",
                "read_but_missed:",
            )
        ) and gap not in live:
            if gap == "read_but_missed:period-operating-revenue" and _period_revenue_verified(
                claims, checks
            ):
                continue
            if gap.startswith("evidence_not_located:"):
                target = gap.split(":", 1)[1]
                if any(
                    item.claim_id == target
                    or target.endswith(":" + item.claim_id)
                    or item.claim_id.endswith(target.rsplit(":", 1)[-1])
                    for item in _verified_claims(claims, checks)
                ):
                    continue
                if _period_revenue_verified(claims, checks) and "period-operating-revenue" in target:
                    continue
            if gap.startswith("verify_unit_not_admitted:"):
                target = gap.split(":", 1)[1]
                if any(item.claim_id == target for item in _verified_claims(claims, checks)):
                    continue
            if gap.startswith("claim_pending:"):
                target = gap.split(":", 1)[1]
                claim = next((item for item in claims if item.claim_id == target), None)
                latest = _latest_check(checks, "claim", target)
                if (
                    claim is not None
                    and claim.limitation not in _UNTRUSTED_LIMITATIONS
                    and latest is not None
                    and latest.status == "pass"
                ):
                    continue
            live.append(gap)
    for gap in dynamic:
        if gap not in live:
            live.append(gap)
    return live


def _core_answers_ready(claims, conditions, checks, records=()) -> bool:
    """Stop only when the current major conditions and core claims are covered.

    Dimension presence alone does not retire another unanswered major business.
    """
    return not any(
        gap.startswith(
            (
                "view_not_substantive:",
                "core_dimension_missing:",
                "major_condition_not_verified:",
                "core_question_unanswered:",
            )
        )
        for gap in _completeness_gaps(claims, conditions, checks, records)
    )


_CORE_CLOSEOUT_DIMENSIONS = {
    "principal_business",
    "products_services",
    "revenue_model",
    "input_exposure",
    "supply_chain_role",
    "upstream_downstream",
    "output_exposure",
}


def _core_closeout_ready(claims, conditions, checks, records=()) -> bool:
    """Core business is verified, so leftover unread pages are not a core gap.

    A pending, unclear, or not_reliable claim does not satisfy a dimension.
    An unverified major condition in a core dimension still blocks.
    """
    trusted = []
    for claim in claims:
        if claim.status != "observed" or claim.limitation in _UNTRUSTED_LIMITATIONS:
            continue
        latest = None
        for check in checks:
            if check.target_type == "claim" and check.target_id == claim.claim_id:
                latest = check
        if latest is None or latest.status != "pass":
            continue
        trusted.append(claim)
    dimensions = {claim.dimension for claim in trusted}
    if not {
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
    } <= dimensions:
        return False
    if not _operating_role_established(claims, checks):
        return False
    for gap in _completeness_gaps(claims, conditions, checks, records):
        if not gap.startswith("major_condition_not_verified:"):
            if gap.startswith(("view_not_substantive:", "core_dimension_missing:")):
                return False
            continue
        condition_id = gap.split(":", 1)[1]
        condition = next(
            (item for item in conditions if item.condition_id == condition_id),
            None,
        )
        if condition is None or condition.dimension in _CORE_CLOSEOUT_DIMENSIONS:
            return False
    return True


def _structure_preview(text: str) -> str:
    """Headings a reader can recognize, plus a short body sample. Not an 8-character stub."""
    headings = _heading_lines(text, limit=2)
    compact = " ".join(text.split())
    sample = compact[:48]
    parts = [*headings, sample]
    joined = " ".join(parts)
    marker = next((mark for mark in _BUSINESS_MARKERS if mark in compact and mark not in joined), "")
    if marker:
        index = compact.find(marker)
        parts.append(compact[max(0, index - 8) : index + len(marker) + 12])
    return " / ".join(part for part in parts if part)[:180]


_DOTTED_TOC = re.compile(r"^(?P<title>.+?)\s*[\.·．…\s]{2,}(?P<page>\d{1,3})$")
_SECTION_TITLE = re.compile(r"^第[一二三四五六七八九十百零0-9]+节\s+\S+")
_NUMBERED_TITLE = re.compile(r"^\d+(?:\.\d+)+\s+\S+")
_PRINTED_NUM = re.compile(r"^0*(\d{1,3})$")
_HEADER_PAGE = re.compile(r"(?<!\d)(\d{1,3})\s*/\s*\d{2,4}(?!\d)")
_BUSINESS_MARKERS = (
    "管理层讨论",
    "经营情况",
    "经营讨论",
    "主营业务",
    "主要业务",
    "业务情况",
    "经营模式",
    "利润表",
    "营业收入",
    "营业总收入",
    "分部",
    "财务报告",
    "采购",
    "供应商",
    "主要客户",
    "前五名",
    "手续费",
    "佣金净收入",
    "利息净收入",
)
_SKIP_MARKERS = (
    "重要提示",
    "释义",
    "公司治理",
    "环境和社会",
    "重要事项",
    "股份变动",
    "股东情况",
    "债券相关",
    "备查文件",
    "目录和释义",
    "公司简介和主要财务指标",
    "会计数据和财务指标",
)


def _heading_lines(text: str, limit: int = 8) -> list[str]:
    headings: list[str] = []
    for raw in str(text).splitlines():
        line = " ".join(raw.split())
        if not line or len(line) > 40 or line.endswith(("。", "！", "？", "；")):
            continue
        headings.append(line)
        if len(headings) >= limit:
            break
    return headings


def _looks_like_toc(text: str) -> bool:
    head = str(text)[:800]
    return ("目录" in head or "目 录" in head) and ("第" in head and "节" in head)


def _is_business_title(title: str) -> bool:
    if any(skip in title for skip in _SKIP_MARKERS) and not any(
        mark in title for mark in _BUSINESS_MARKERS
    ):
        return False
    if any(mark in title for mark in _BUSINESS_MARKERS):
        return True
    return bool(
        _NUMBERED_TITLE.match(title)
        and any(token in title for token in ("业务", "经营", "利润", "收入", "采购", "客户"))
    )


def _parse_toc_entries(text: str) -> list[tuple[str, int | None]]:
    entries: list[tuple[str, int | None]] = []
    titles: list[str] = []
    numbers: list[int] = []
    for raw in str(text).splitlines():
        line = " ".join(raw.split())
        if not line:
            continue
        dotted = _DOTTED_TOC.match(line)
        if dotted:
            entries.append((dotted.group("title").strip(), int(dotted.group("page"))))
            continue
        printed = _PRINTED_NUM.match(line)
        if printed and len(line) <= 3:
            numbers.append(int(printed.group(1)))
            continue
        if _SECTION_TITLE.match(line) or _NUMBERED_TITLE.match(line):
            titles.append(line)
    if not entries and titles:
        if numbers and len(numbers) >= len(titles):
            entries.extend(zip(titles, numbers))
        else:
            entries.extend((title, None) for title in titles)
    return entries


def _printed_to_physical(pages) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for source in pages:
        text = str(source.get("text") or "")
        match = _HEADER_PAGE.search(text[:120]) or _HEADER_PAGE.search(text[-80:])
        if match is None:
            continue
        mapping.setdefault(int(match.group(1)), int(source["page"]))
    return mapping


def _is_title_cross_reference(source, title: str) -> bool:
    needle = " ".join(str(title).split())
    text = " ".join(str(source.get("text") or "").split())
    index = text.find(needle)
    if index < 0:
        return False
    window = text[max(0, index - 8) : index]
    return any(mark in window for mark in ("查阅", "详见", "参见", "见本"))


def _page_has_title(source, title: str) -> bool:
    if _looks_like_toc(str(source.get("text") or "")):
        return False
    needle = " ".join(str(title).split())
    if not needle:
        return False
    text = " ".join(str(source.get("text") or "").split())
    if needle in text and not _is_title_cross_reference(source, title):
        return True
    short = needle[:16]
    return any(short in line or needle in line for line in _heading_lines(str(source.get("text") or "")))


def chapter_candidate_pages(pages, limit: int = 16) -> list[int]:
    """Locate business subsections from TOC and headings. Not every page opening."""
    readable = [
        source
        for source in pages
        if source.get("readable", True) and str(source.get("text") or "").strip()
    ]
    chosen: list[int] = []
    seen: set[int] = set()

    def add(page_no: int) -> None:
        page_no = int(page_no)
        if page_no in seen:
            return
        seen.add(page_no)
        chosen.append(page_no)

    toc_entries: list[tuple[str, int | None]] = []
    for source in readable[:24]:
        text = str(source.get("text") or "")
        if not _looks_like_toc(text):
            continue
        add(int(source["page"]))
        toc_entries.extend(_parse_toc_entries(text))
    printed_map = _printed_to_physical(readable)
    available = {int(source["page"]) for source in readable}
    for title, printed in toc_entries:
        if not _is_business_title(title):
            continue
        located = None
        if printed is not None:
            candidate = printed_map.get(int(printed))
            if candidate is None and int(printed) in available:
                candidate = int(printed)
            source = next(
                (item for item in readable if int(item["page"]) == candidate),
                None,
            ) if candidate is not None else None
            if source is not None and _page_has_title(source, title):
                located = int(candidate)
        if located is None:
            located = next(
                (
                    int(source["page"])
                    for source in readable
                    if _page_has_title(source, title)
                ),
                None,
            )
        if located is not None:
            add(located)
    seen_heading: set[str] = set()
    for source in readable:
        for heading in _heading_lines(str(source.get("text") or "")):
            if not _is_business_title(heading):
                continue
            if not (
                _SECTION_TITLE.match(heading)
                or _NUMBERED_TITLE.match(heading)
                or heading.startswith(("一、", "二、", "三、", "四、", "(一)", "（一）"))
                or heading.endswith("续")
                or any(heading.startswith(mark) for mark in _BUSINESS_MARKERS)
            ):
                continue
            key = re.sub(r"\d+", "N", heading)[:28]
            if key in seen_heading:
                continue
            seen_heading.add(key)
            add(int(source["page"]))
            break
    if not chosen and readable:
        add(int(readable[0]["page"]))
    return chosen[: max(1, int(limit))]


def _directed_operating_role_pages(sources, by_page, limit: int = 8) -> list[int]:
    """Unread pages after a TOC business start. Reservation, not semantics, slices.

    A first window is only the first send. Later requested pages still follow.
    Already-read cost or concentration pages are not substitutes.
    """
    by_id = {int(page["page"]): page for page in sources}
    starts = []
    for page_no in chapter_candidate_pages(sources):
        source = by_id.get(int(page_no))
        if source is None:
            continue
        text = str(source.get("text") or "")
        if by_page.get(int(page_no), {}).get("status") != "read" or any(
            mark in text[:1200]
            for mark in ("管理层讨论", "经营情况", "经营模式", "核心竞争力")
        ):
            starts.append(int(page_no))
    role_hits: list[int] = []
    section_hits: list[int] = []
    for start in starts:
        for page_no in range(start, start + 16):
            if page_no in role_hits or page_no in section_hits or page_no not in by_id:
                continue
            if by_page.get(page_no, {}).get("status") == "read":
                continue
            text = str(by_id[page_no].get("text") or "")
            preview = " ".join(
                [*_heading_lines(text, limit=4), " ".join(text.split())[:400]]
            )
            if _has_operating_role_statement(text):
                role_hits.append(page_no)
            elif any(
                mark in preview
                for mark in ("经营情况", "核心竞争力", "管理层讨论", "经营模式")
            ):
                section_hits.append(page_no)
    chosen = role_hits + section_hits
    return chosen[: max(1, int(limit))] if chosen else chosen


def compact_source_conditions(
    conditions: Sequence[ReportSourceCondition],
) -> list[ReportSourceCondition]:
    """Drop exact-quote+name repeats. Keep distinct majors on the same quote."""
    kept: list[ReportSourceCondition] = []
    quotes: set[tuple[Any, ...]] = set()
    names: set[tuple[Any, ...]] = set()
    for item in conditions:
        description = " ".join(str(item.description or "").split())
        quote_key = None
        if item.evidence:
            evidence = item.evidence[0]
            quote = str(getattr(getattr(evidence, "anchor", None), "bounded_quote", "") or "")
            quote_key = (int(evidence.page), quote, item.dimension, description)
            if quote_key in quotes:
                continue
        name_key = (
            item.dimension,
            description,
            item.statement_status,
            item.major,
        )
        if item.major and name_key in names:
            continue
        if quote_key is not None:
            quotes.add(quote_key)
        if item.major:
            names.add(name_key)
        kept.append(item)
    return kept


def _norm_source_text(text: str) -> str:
    return re.sub(r"\s+", "", str(text or ""))


def _whitespace_span(context: str, quote: str) -> tuple[int, int] | None:
    """Locate quote inside context when only whitespace differs."""
    compact_quote = _norm_source_text(quote)
    if not compact_quote:
        return None
    length = len(context)
    cursor = 0
    while cursor < length:
        while cursor < length and context[cursor].isspace():
            cursor += 1
        if cursor >= length:
            break
        matched = 0
        end = cursor
        while end < length and matched < len(compact_quote):
            if context[end].isspace():
                end += 1
                continue
            if context[end] != compact_quote[matched]:
                break
            matched += 1
            end += 1
        if matched == len(compact_quote):
            return cursor, end
        cursor += 1
    return None


def unique_context_prefix(context: str, quote: str) -> str:
    """Keep subject, period, unit and negation around the quote."""
    context = str(context or "")
    quote = str(quote or "")
    if not context:
        return ""
    if not quote:
        return context.strip()
    index = context.find(quote)
    end = index + len(quote) if index >= 0 else -1
    if index < 0:
        span = _whitespace_span(context, quote)
        if span is None:
            return context.strip()
        index, end = span
    prefix = context[:index].strip()
    suffix = context[end:].strip()
    kept = [part for part in (prefix, suffix) if part and _norm_source_text(part) != _norm_source_text(quote)]
    return "\n".join(kept)


def compact_evidence_item(evidence: Evidence) -> Evidence:
    """Drop quote copies from context_before and an identical model_quote."""
    anchor = getattr(evidence, "anchor", None)
    if not isinstance(anchor, TextAnchor):
        return evidence
    quote = str(anchor.bounded_quote or "")
    context = unique_context_prefix(str(anchor.context_before or ""), quote)
    model = str(anchor.model_quote or "")
    if model and _norm_source_text(model) == _norm_source_text(quote):
        model = ""
    if context == str(anchor.context_before or "") and model == str(anchor.model_quote or ""):
        return evidence
    return evidence.model_copy(
        update={
            "anchor": anchor.model_copy(
                update={
                    "context_before": context or None,
                    "model_quote": model or None,
                }
            )
        }
    )


def _compact_object_evidence(item):
    if not getattr(item, "evidence", None):
        return item
    compacted = tuple(compact_evidence_item(evidence) for evidence in item.evidence)
    if compacted == item.evidence:
        return item
    return item.model_copy(update={"evidence": compacted})


def compact_source_pages(
    pages: Sequence[Mapping[str, Any]],
    objects: Sequence[Any],
) -> list[dict[str, Any]]:
    """Keep page text that is not already an exact quote or its context."""
    quotes: dict[int, list[str]] = {}
    for item in objects:
        for evidence in getattr(item, "evidence", ()) or ():
            quote = str(getattr(getattr(evidence, "anchor", None), "bounded_quote", "") or "")
            if not quote:
                continue
            quotes.setdefault(int(evidence.page), []).append(quote)
            context = unique_context_prefix(
                str(getattr(getattr(evidence, "anchor", None), "context_before", "") or ""),
                quote,
            )
            if context:
                quotes[int(evidence.page)].append(context)
            heading = str(getattr(evidence, "section_title", "") or "")
            if heading:
                quotes[int(evidence.page)].append(heading)
    compacted = []
    seen: set[tuple[int, str]] = set()
    for page in pages:
        page_no = int(page["page"])
        text = str(page.get("text") or "")
        for quote in quotes.get(page_no, ()):
            if quote and quote in text:
                text = text.replace(quote, "\n")
        text = re.sub(r"\n{2,}", "\n", text).strip()
        if not text:
            continue
        key = (page_no, text)
        if key in seen:
            continue
        seen.add(key)
        compacted.append({**dict(page), "page": page_no, "text": text, "readable": True})
    if compacted:
        return compacted
    return [
        {"page": int(page["page"]), "text": str(page.get("text") or "")[:80], "readable": True}
        for page in pages[:1]
        if page.get("text")
    ]


def compact_verify_targets(claims, conditions, records=()):
    """One wire copy of each exact location for a verify or coverage request."""
    claims = [_compact_object_evidence(item) for item in claims]
    conditions = [_compact_object_evidence(item) for item in conditions]
    records = [_compact_object_evidence(item) for item in records]
    return claims, conditions, records


_REVENUE_TOTAL = re.compile(
    r"(?:本集团|本公司|合并)?营业收入[^。\n]{0,32}?"
    r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(亿元|万元|百万元|元)"
)
_NET_INCOME_LINES = (
    ("利息净收入", "net_interest_income"),
    ("手续费及佣金净收入", "net_fee_income"),
    ("其他净收入", "other_net_income"),
)
_LINE_AMOUNT = re.compile(r"(\d{1,3}(?:,\d{3})+(?:\.\d+)?)")


def disclosed_core_revenue_facts(sources: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Group operating-revenue totals and the three net-income lines on read pages."""
    found: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for source in sources:
        text = str(source.get("text") or "")
        page = int(source["page"])
        for match in _REVENUE_TOTAL.finditer(text):
            window = text[max(0, match.start() - 24) : match.end() + 16]
            if any(mark in window for mark in ("分产品", "分行业", "分地区")):
                continue
            amount = match.group(1)
            unit = match.group(2)
            if unit != "亿元" and "," not in amount:
                continue
            key = (amount.replace(",", ""), unit)
            if key in seen:
                continue
            seen.add(key)
            found.append(
                {
                    "kind": "operating_revenue_total",
                    "page": page,
                    "amount": amount,
                    "unit": unit,
                    "quote": match.group(0)[:180],
                }
            )
        for match in re.finditer(r"营业收入\s+(\d{1,3}(?:,\d{3})+(?:\.\d+)?)", text):
            window = text[max(0, match.start() - 40) : match.end() + 16]
            if any(mark in window for mark in ("分产品", "分行业", "分地区", "占")):
                continue
            # A column header such as 变动比例（%） is not this line's rate.
            if "%" in re.sub(r"[（(]\s*%\s*[）)]", "", window):
                continue
            amount = match.group(1)
            unit = ""
            header = text[max(0, match.start() - 240) : match.start()]
            unit_marks = list(re.finditer(r"单位\s*[:：]\s*(?:人民币)?(亿元|万元|元)", header))
            if unit_marks:
                unit = unit_marks[-1].group(1)
            quote = match.group(0)[:180]
            if unit and unit not in quote:
                quote = f"单位：{unit} {quote}"[:180]
            key = (amount.replace(",", ""), unit)
            if key in seen:
                continue
            seen.add(key)
            found.append(
                {
                    "kind": "operating_revenue_total",
                    "page": page,
                    "amount": amount,
                    "unit": unit,
                    "quote": quote,
                }
            )
        for label, kind in _NET_INCOME_LINES:
            for match in re.finditer(re.escape(label) + r"[^\n]{0,40}", text):
                window = match.group(0)
                if "占" in window or "%" in window:
                    continue
                number = _LINE_AMOUNT.search(window)
                if number is None:
                    continue
                amount = number.group(1)
                key = (kind, amount.replace(",", ""))
                if key in seen:
                    continue
                seen.add(key)
                found.append(
                    {
                        "kind": kind,
                        "page": page,
                        "amount": amount,
                        "unit": "",
                        "quote": window[:180],
                    }
                )
    return found


def _answer_covers_revenue_fact(answers: str, fact: Mapping[str, Any]) -> bool:
    """A number in a quote or another object does not answer this fact."""
    amount = str(fact.get("amount") or "").replace(",", "")
    unit = str(fact.get("unit") or "")
    display = _norm_source_text(str(fact.get("amount") or "") + unit)
    compact = _norm_source_text(answers)
    labels = {
        "operating_revenue_total": ("营业收入", "营业总收入"),
        "net_interest_income": ("利息净收入",),
        "net_fee_income": ("手续费及佣金净收入", "手续费"),
        "other_net_income": ("其他净收入",),
    }
    if not any(label in answers for label in labels.get(str(fact.get("kind") or ""), ("营业收入",))):
        return False
    if amount and (amount in compact or display in compact):
        return True
    if not amount:
        return False
    for match in _ANSWER_AMOUNT.finditer(answers):
        if _amounts_equivalent(amount, unit or "元", match.group(1), match.group(2) or "元"):
            return True
    return False


def missed_core_revenue_facts(
    sources: Sequence[Mapping[str, Any]],
    conditions=(),
    claims=(),
) -> list[dict[str, Any]]:
    """Facts already printed on read pages but absent from delivered answers."""
    answers = "\n".join(str(getattr(item, "answer", "") or "") for item in claims)
    missed = []
    for fact in disclosed_core_revenue_facts(sources):
        if _answer_covers_revenue_fact(answers, fact):
            continue
        missed.append(fact)
    return missed


def _metric_core(label: str) -> tuple[str, str]:
    for suffix in _METRIC_SUFFIXES:
        if label.endswith(suffix) and len(label) > len(suffix):
            return label[: -len(suffix)], suffix
    return label, ""


def _metric_rate_pairs(text: str) -> list[tuple[str, str]]:
    """Named metric next to a printed rate. A bare word like 托管 is not a match."""
    pairs: list[tuple[str, str]] = []
    for match in re.finditer(r"(\d{1,2}\.\d{2})\s*%", str(text or "")):
        window = re.split(r"[。；;]", str(text or "")[max(0, match.start() - 36) : match.start()])[-1]
        prefix = re.sub(
            r"(?:较上年末|较上年|同比)?(?:增长|增速|增幅|变动|增加)\s*$",
            "",
            window,
        )
        names = re.findall(r"[\u4e00-\u9fff]{2,12}", prefix)
        label = next(
            (
                name
                for name in reversed(names)
                if name not in _METRIC_STOP and not name.startswith("较上")
            ),
            None,
        )
        if label is None:
            continue
        pairs.append((label, match.group(1)))
    return pairs


def _same_metric_label(left: str, right: str) -> bool:
    if left == right:
        return True
    core_left, suffix_left = _metric_core(left)
    core_right, suffix_right = _metric_core(right)
    if suffix_left and suffix_right and suffix_left != suffix_right:
        return False
    if core_left == core_right:
        return True
    shorter, longer = (
        (core_left, core_right)
        if len(core_left) <= len(core_right)
        else (core_right, core_left)
    )
    return len(shorter) >= 2 and shorter not in _METRIC_STOP and shorter in longer


def related_conflict_pages(claim, sources: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Other read pages that print a different rate for the same named metric."""
    answer = str(getattr(claim, "answer", "") or "")
    quotes = "\n".join(
        str(getattr(getattr(evidence, "anchor", None), "bounded_quote", "") or "")
        for evidence in getattr(claim, "evidence", ()) or ()
    )
    cited = {int(evidence.page) for evidence in getattr(claim, "evidence", ()) or ()}
    body = answer + "\n" + quotes
    claim_pairs = _metric_rate_pairs(body)
    if not claim_pairs:
        return []
    claimed: dict[str, set[str]] = {}
    for label, rate in claim_pairs:
        claimed.setdefault(label, set()).add(rate)
    extra = []
    seen: set[int] = set()
    for source in sources:
        page_no = int(source["page"])
        if page_no in cited or page_no in seen:
            continue
        text = str(source.get("text") or "")
        conflict = False
        for label, rate in _metric_rate_pairs(text):
            for claim_label, rates in claimed.items():
                if _same_metric_label(claim_label, label) and rate not in rates:
                    conflict = True
                    break
            if conflict:
                break
        if not conflict:
            continue
        extra.append({"page": page_no, "text": text, "readable": True})
        seen.add(page_no)
    return extra


def make_operating_revenue_question(report, facts: Sequence[Mapping[str, Any]]):
    """Ask for the printed group total and the three net-income amounts."""
    evidence = []
    for fact in facts:
        evidence.append(
            Evidence(
                evidence_id=f"read-missed-{fact['kind']}-{fact['page']}",
                report=report,
                page=int(fact["page"]),
                section_title="营业收入",
                anchor=TextAnchor(bounded_quote=str(fact["quote"])[:180]),
            )
        )
    return ReportSourceCondition(
        condition_id="core-question:period-operating-revenue",
        dimension="revenue_model",
        description=(
            "State the reporting entity's current-period operating revenue "
            "total when the supplied text states it, using only the line, "
            "period and unit printed there. If the same statement prints "
            "net interest income, net fee and commission income, and other "
            "net income amounts, state those amounts too. A share, "
            "principal-business, product-line, region, or sales-channel "
            "subtotal does not replace that total."
        ),
        evidence=tuple(evidence),
        major=True,
        statement_status="current",
    )


def make_operating_role_question(report, evidence) -> ReportSourceCondition:
    """Ask what the company produces, processes, sells or serves, and its chain role."""
    return ReportSourceCondition(
        condition_id="core-question:operating-role",
        dimension="supply_chain_role",
        description=(
            "根据所供原文说明本公司当前如何生产、加工、销售或提供服务，"
            "以及有据的上下游角色。客户或供应商集中度、成本构成不能代替经营角色。"
            "不要把计划目标写成已经实现的身份，也不要编造原文没有的客户。"
        ),
        evidence=tuple(evidence),
        major=True,
        statement_status="current",
    )


def _printed_amount_needles(amount: str, unit: str) -> list[str]:
    yuan = _amount_in_yuan(amount, unit or "元")
    if yuan is None:
        return []
    needles = []
    for name, scale in _UNIT_TO_YUAN.items():
        value = yuan / scale
        if value < 0.01:
            continue
        rendered = f"{value:,.2f}"
        compact = rendered.rstrip("0").rstrip(".")
        needles.append(f"{rendered}{name}")
        needles.append(f"{compact}{name}")
        needles.append(f"{rendered} {name}")
    digits = str(amount or "").replace(",", "")
    if digits:
        needles.append(digits)
    return list(dict.fromkeys(needles))


def _bind_equivalent_amount_evidence(evidence, report, pages):
    """Locate the printed original when the model sent a converted amount."""
    quote = str(getattr(getattr(evidence, "anchor", None), "bounded_quote", "") or "")
    model_quote = str(getattr(getattr(evidence, "anchor", None), "model_quote", "") or "")
    body = model_quote or quote
    matches = list(_ANSWER_AMOUNT.finditer(body))
    if not matches:
        return None
    found: list[tuple[int, str]] = []
    seen: set[int] = set()
    for page in pages:
        if page.get("readable", True) is False:
            continue
        text = str(page.get("text") or "")
        compact = text.replace(",", "").replace(" ", "")
        hit = False
        for match in matches:
            for needle in _printed_amount_needles(match.group(1), match.group(2) or "元"):
                if needle.replace(",", "").replace(" ", "") in compact or needle in text:
                    hit = True
                    break
            if hit:
                break
        if not hit:
            continue
        page_no = int(page["page"])
        if page_no in seen:
            continue
        seen.add(page_no)
        snippet = next(
            (
                line.strip()
                for line in text.splitlines()
                if any(
                    token in line
                    for token in ("营业收入", "利息净收入", "手续费", "其他净收入")
                )
            ),
            text[:180],
        )
        found.append((page_no, snippet[:180]))
    preferred = int(getattr(evidence, "page", 0) or 0)
    chosen = next((item for item in found if item[0] == preferred), None)
    if chosen is None and len(found) == 1:
        chosen = found[0]
    if chosen is None:
        return None
    page_no, snippet = chosen
    return evidence.model_copy(
        update={
            "page": page_no,
            "anchor": evidence.anchor.model_copy(
                update={
                    "bounded_quote": snippet,
                    "model_quote": None if snippet == body else body,
                }
            ),
        }
    )


PREPARED_NAV: list[list[int]] | None = None


def navigation_batches(pages, report, identity, limit: int):
    """Pack short previews of already located candidate pages."""
    previews = [
        {
            "page": source["page"],
            "readable": bool(source.get("readable", True)),
            "text": _structure_preview(str(source.get("text") or "")),
        }
        for source in pages
    ]
    if PREPARED_NAV:
        by_page = {int(item["page"]): item for item in previews}
        grouped = []
        for group in PREPARED_NAV:
            batch = [by_page[page] for page in group if page in by_page]
            if batch:
                grouped.append(batch)
        if grouped:
            return grouped
    return pack_fitting_batches(
        tuple(previews),
        lambda batch: _fit(
            batch,
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            output=_nav_output(identity),
            limit=limit,
        ),
        split_mode="boundary",
    )


def _boundary_header(text: str) -> str:
    """Carry the previous slice's leading line so a table or paragraph is not cut blind."""
    for raw in str(text).splitlines():
        line = raw.strip()
        if line:
            return line[:120]
    return ""


def source_discovery_slices(pages, report, identity, limit: int):
    """Pack every readable page. Navigation does not choose which text is sent."""
    fragments = tuple(
        {
            "page": page["page"],
            "text": page["text"],
            "readable": True,
            "offset": 0,
            "scope": "full_page",
        }
        for page in pages
        if page.get("readable", True) and str(page.get("text") or "").strip()
    )

    def fits(batch) -> bool:
        return _fit(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_source_output(identity),
            limit=limit,
        )

    batches = pack_fitting_batches(fragments, fits, split_mode="boundary")
    overlapped = []
    previous_text = ""
    previous_page = None
    for batch in batches:
        header = _boundary_header(previous_text)
        if header and previous_page is not None:
            context = {
                "page": previous_page,
                "text": header,
                "readable": True,
                "offset": 0,
                "scope": "boundary_context",
            }
            trial = [context, *batch]
            if fits(trial):
                batch = trial
        overlapped.append(batch)
        previous_text = str(batch[-1].get("text") or "")
        previous_page = batch[-1].get("page")
    return overlapped


def fulltext_entry_plan(
    pages: Sequence[Mapping[str, Any]],
    report: ReportIdentity,
    identity: Mapping[str, Any],
    *,
    ledger: int,
    reservation_limit: int,
    request_cap: int,
) -> dict[str, Any]:
    """Measure every source slice plus one draft, one verify, one supplement and two recoveries."""
    slices = source_discovery_slices(pages, report, identity, reservation_limit)
    if not slices:
        raise ValueError("direct_core_fulltext_has_no_slice")
    source_units = [
        _units(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_source_output(identity),
        )
        for batch in slices
    ]
    sample = slices[0]
    synthesis_units = _units(
        sample,
        purpose="synthesize_views",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=_body_output(identity),
    )
    verification_units = _nonempty_verify_units(sample, report=report, identity=identity)
    largest = max(source_units)
    supplement_units = largest
    recovery_units = 2 * largest
    total_units = (
        sum(source_units)
        + synthesis_units
        + verification_units
        + supplement_units
        + recovery_units
    )
    physical_requests = len(slices) + 4
    covered = sorted({int(page["page"]) for batch in slices for page in batch})
    readable = sorted(
        int(page["page"])
        for page in pages
        if page.get("readable", True) and str(page.get("text") or "").strip()
    )
    return {
        "source_slices": len(slices),
        "source_units": source_units,
        "synthesis_units": synthesis_units,
        "verification_units": verification_units,
        "supplement_units": supplement_units,
        "recovery_units": recovery_units,
        "total_units": total_units,
        "physical_requests": physical_requests,
        "covered_pages": covered,
        "readable_pages": readable,
        "fits_entry": total_units <= ledger and physical_requests <= request_cap,
    }


def body_packages(pages, report, identity, limit: int):
    fragments = tuple(
        {
            "page": page["page"],
            "text": page["text"],
            "readable": True,
            "offset": 0,
            "scope": "full_page",
        }
        for page in pages
        if page.get("readable", True) and str(page.get("text") or "").strip()
    )
    return pack_fitting_batches(
        fragments,
        lambda batch: _fit(
            batch,
            purpose="synthesize_views",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_body_output(identity),
            limit=limit,
        )
        and _fit(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_body_output(identity),
            limit=limit,
        )
        and _fit(
            batch,
            purpose="verify_views",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_verify_output(identity),
            limit=limit,
        ),
        split_mode="boundary",
    )


def joint_verify_reservation(
    report,
    identity,
    pages,
    claims=(),
    conditions=(),
    records=(),
) -> int:
    """Size the verify request that will actually be sent."""
    if not pages:
        return 10**9
    return request_reservation(
        ReportSemanticRequestV3(
            request_id="direct-core-joint-verify",
            purpose="verify_views",
            report=report,
            processing_identity=dict(identity),
            source_pages=tuple(pages),
            source_scope="full_context",
            source_conditions=tuple(conditions),
            claims=tuple(claims),
            records=tuple(records),
            max_output_tokens=_verify_output(identity),
            timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
        )
    )


def _units(pages, *, purpose, source_scope, report, identity, output) -> int:
    return request_reservation(
        ReportSemanticRequestV3(
            request_id="direct-core-size",
            purpose=purpose,
            report=report,
            processing_identity=dict(identity),
            source_pages=tuple(pages),
            source_scope=source_scope,
            max_output_tokens=output,
            timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
        )
    )


def _first_delivery_tail(pages, *, report, identity, limit: int) -> int:
    """Reserve the first real source+draft+verify object, not a two-page stub."""
    if not pages:
        return 0
    pack_limit = min(int(limit), RESERVATION_LIMIT)
    try:
        slices = list(source_discovery_slices(pages, report, identity, pack_limit))
    except ValueError:
        slices = [list(pages[:1])]
    first = slices[0] if slices else list(pages[:1])
    return (
        _units(
            first,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_source_output(identity),
        )
        + _units(
            first,
            purpose="synthesize_views",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_body_output(identity),
        )
        + _nonempty_verify_units(first, report=report, identity=identity)
    )


def _compose_tail(pages, *, report, identity) -> int:
    if not pages:
        return 0
    return (
        _units(
            pages,
            purpose="synthesize_views",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=_body_output(identity),
        )
        + _nonempty_verify_units(pages, report=report, identity=identity)
    )


def _nonempty_verify_units(piece, *, report, identity) -> int:
    """Size verification with a real claim and condition, not an empty target list."""
    from research.company_profile.models import Evidence, TextAnchor

    quote = str(piece[0].get("text") or "")[:40] or "正文"
    evidence = Evidence(
        evidence_id="direct-core-size",
        report=report,
        page=int(piece[0]["page"]),
        section_title="size",
        anchor=TextAnchor(bounded_quote=quote),
    )
    claim = ReportViewClaim(
        claim_id="size-claim",
        dimension="principal_business",
        status="observed",
        answer=quote,
        evidence=(evidence,),
    )
    condition = ReportSourceCondition(
        condition_id="size-condition",
        dimension="principal_business",
        description="用于计量的非空条件",
        evidence=(evidence,),
        major=True,
        statement_status="current",
    )
    return request_reservation(
        ReportSemanticRequestV3(
            request_id="direct-core-nonempty-verify",
            purpose="verify_views",
            report=report,
            processing_identity=dict(identity),
            source_pages=tuple(piece),
            source_scope="full_context",
            source_conditions=(condition,),
            claims=(claim,),
            max_output_tokens=_verify_output(identity),
            timeout_seconds=float(
                identity.get("logical_request_timeout_seconds") or 120
            ),
        )
    )


def first_package_plan(
    pages: Sequence[Mapping[str, Any]],
    report: ReportIdentity,
    identity: Mapping[str, Any],
    *,
    ledger: int = LEDGER_LIMIT,
    reservation_limit: int = RESERVATION_LIMIT,
) -> dict[str, Any]:
    """Admit navigation, one chapter package, its review and one recovery."""
    nav = navigation_batches(pages, report, identity, reservation_limit)
    if not nav:
        raise ValueError("direct_core_navigation_has_no_batch")
    readable = [page for page in pages if page.get("readable", True)]
    if not readable:
        raise ValueError("direct_core_has_no_readable_page")
    sample = max(readable, key=lambda page: len(str(page.get("text") or "")))
    packages = body_packages([sample], report, identity, reservation_limit)
    if not packages:
        raise ValueError("direct_core_first_package_does_not_fit")
    piece = packages[0]
    nav_units = [
        _units(
            batch,
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            output=_nav_output(identity),
        )
        for batch in nav
    ]
    body_units = _units(
        piece,
        purpose="synthesize_views",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=_body_output(identity),
    )
    gap_units = _units(
        piece,
        purpose="source_inventory",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=_body_output(identity),
    )
    verify_units = _units(
        piece,
        purpose="verify_views",
        source_scope="full_context",
        report=report,
        identity=identity,
        output=_verify_output(identity),
    )
    nonempty_verify = _nonempty_verify_units(
        piece, report=report, identity=identity
    )
    recovery = max([*nav_units, body_units, gap_units, nonempty_verify])
    known_total = sum(nav_units) + body_units + gap_units + verify_units + max(
        [*nav_units, body_units, gap_units, verify_units]
    )
    unknown_total = sum(nav_units) + body_units + gap_units + nonempty_verify + recovery
    return {
        "navigation_batches": len(nav),
        "navigation_pages": [[int(page["page"]) for page in batch] for batch in nav],
        "navigation_units": nav_units,
        "body_units": body_units,
        "gap_units": gap_units,
        "empty_verify_units": verify_units,
        "nonempty_verify_units": nonempty_verify,
        "recovery_units": recovery,
        "known_total_units": known_total,
        "unknown_total_units": unknown_total,
        "fits_ledger": unknown_total <= ledger,
    }


def interpret_direct_core(
    *,
    report: ReportIdentity,
    pages: Sequence[Mapping[str, Any]],
    processing_identity: Mapping[str, Any],
    provider: Any,
    work_id: str,
    token_budget: int,
    started_at: float,
    structured_records: Sequence[SemanticRecord] = (),
    execution_budget: LlmRunBudget | None = None,
    on_progress: Any = None,
) -> ReportInterpretation:
    """Publish each verified chapter package before reading the next one."""
    identity = dict(processing_identity)
    mode = identity.get("execution_mode", "offline_fake")
    if mode not in {"offline_fake", "provider"}:
        raise ValueError("unknown report execution mode")
    if provider is None or not callable(getattr(provider, "interpret", None)):
        raise ValueError("semantic_provider_unavailable")
    if mode == "provider" and (
        execution_budget is None
        or getattr(provider, "run_budget", None) is not execution_budget
        or getattr(provider, "execution_authorization", {}).get("status") != "authorized"
    ):
        raise ValueError("a0_real_execution_binding_required")
    if mode == "offline_fake" and not getattr(provider, "offline_only", False):
        raise ValueError("offline_identity_requires_fake_provider")
    if identity.get("resumed_receipts") or identity.get("navigation_query"):
        raise ValueError("direct_core_refuses_semantic_continuation")

    sources = _pages(pages)
    limit = int(identity.get("request_reservation_limit") or RESERVATION_LIMIT)
    ledger = int(identity.get("ledger_tokens") or token_budget or LEDGER_LIMIT)
    request_cap = int(identity.get("max_physical_requests") or 16)
    gaps: list[str] = []
    conditions: list[ReportSourceCondition] = []
    conditions_to_draft: list[ReportSourceCondition] = conditions
    claims: list[ReportViewClaim] = []
    checks: list[ReportViewCheck] = []
    records: list[SemanticRecord] = []
    dispositions: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for claim_id in identity.get("isolated_claim_ids") or ():
        gap = f"isolated_unreliable_claim:{claim_id}"
        if gap not in gaps:
            gaps.append(gap)
    coverage = [
        {
            "page": page["page"],
            "status": "not_read",
            "chunks": [],
            "structure_indexed": False,
            "map_checked": False,
        }
        for page in sources
    ]
    by_page = {item["page"]: item for item in coverage}
    structured = {
        item.record_id: item for item in structured_records if item.report == report
    }

    def snapshot() -> dict[str, Any]:
        if execution_budget is None:
            return {}
        return execution_budget.snapshot()

    def recovery_used() -> bool:
        counts: dict[str, int] = {}
        for item in snapshot().get("attempts") or ():
            logical = str(item.get("logical_request_id") or item.get("request_id") or "")
            if logical:
                counts[logical] = counts.get(logical, 0) + 1
        return any(count >= 2 for count in counts.values())

    def gateway_budget():
        """Public gateway ledger. A reservation simulator must not override it."""
        budget = getattr(provider, "run_budget", None)
        if budget is not None and hasattr(budget, "snapshot"):
            return budget
        client = getattr(provider, "client", None)
        budget = getattr(client, "physical_budget", None)
        if budget is not None and hasattr(budget, "snapshot"):
            return budget
        return None

    def charged() -> int:
        prior = int(identity.get("prior_charged_tokens") or 0)
        gateway = gateway_budget()
        if gateway is not None:
            return prior + int(gateway.snapshot().get("charged_tokens") or 0)
        live = int(snapshot().get("charged_tokens") or 0)
        simulated = int(identity.get("offline_charged_tokens") or 0)
        return prior + max(live, simulated)

    def pack_room() -> int:
        remaining = ledger - charged() - tail_hold
        return min(int(limit), max(0, remaining))

    def physical_count() -> int:
        return sum(1 for item in receipts if item.get("physical_request", True))

    def publish() -> None:
        if on_progress is None:
            return
        on_progress(_result())

    def _result() -> ReportInterpretation:
        observed = {item.dimension for item in claims if item.status == "observed"}
        published = _current_gaps(gaps, claims, conditions, checks, records)
        blocking_gaps = [
            gap for gap in published if not gap.startswith("supplementary_gap:")
        ]
        blocking = any(
            gap.startswith(
                (
                    "necessary_unread:",
                    "necessary_fragment",
                    "major_condition_not_verified:",
                    "major_condition_not_in_synthesis:",
                    "navigation_incomplete:",
                    "view_not_substantive:",
                    "core_dimension_missing:",
                    "direct_core_gap_check_stopped",
                    "claim_pending:",
                    "company_synthesis_context_truncated",
                    "core_question_unanswered:",
                )
            )
            for gap in blocking_gaps
        )
        draft_complete = bool(observed) and not blocking
        return ReportInterpretation(
            report=report,
            processing_identity=identity,
            execution_mode=mode if mode in {"offline_fake", "provider"} else "offline_fake",
            page_coverage=tuple(coverage),
            source_conditions=tuple(conditions),
            claims=tuple(claims),
            records=tuple(records),
            checks=tuple(checks),
            record_dispositions=tuple(dispositions),
            call_receipts=tuple(receipts),
            gaps=tuple(published),
            verified_dimensions=tuple(sorted(observed)),
            draft_complete=draft_complete,
            semantic_complete=draft_complete,
            real_semantic_complete=draft_complete and mode == "provider",
            tokens_charged=charged(),
            budget_ledger=snapshot(),
            elapsed_seconds=max(0.0, time.monotonic() - started_at),
        )

    tail_hold = 0

    def admit(units: int, later: int) -> bool:
        if charged() + units + later + tail_hold > ledger:
            gaps.append("direct_core_ledger_hold")
            return False
        if physical_count() + 1 > request_cap:
            gaps.append("direct_core_request_cap")
            return False
        if execution_budget is not None and execution_budget.remaining() <= 0:
            gaps.append("direct_core_deadline")
            return False
        return True

    def invoke(purpose: str, source_pages, *, source_scope: str, output: int, later: int, extra: dict | None = None):
        extra = dict(extra or {})
        if purpose == "verify_views":
            claims = list(extra.get("claims") or ())
            conditions = list(extra.get("source_conditions") or ())
            records = list(extra.get("records") or ())
            claims, conditions, records = compact_verify_targets(
                claims, conditions, records
            )
            source_pages = compact_source_pages(
                source_pages, [*claims, *conditions, *records]
            )
            extra["claims"] = tuple(claims)
            extra["source_conditions"] = tuple(conditions)
            extra["records"] = tuple(records)
        request = ReportSemanticRequestV3(
            request_id=f"{work_id}:report:{len(receipts) + 1}",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(source_pages),
            source_scope=source_scope,
            max_output_tokens=output,
            timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
            **extra,
        )
        units = request_reservation(request)
        if units > limit:
            raise ValueError("card_request_reservation_exceeded")
        if not admit(units, later):
            raise ValueError("direct_core_admission_stopped")
        response = provider.interpret(request)
        if not isinstance(response, ReportSemanticResponseV3):
            response = ReportSemanticResponseV3.model_validate_json(
                json.dumps(response, ensure_ascii=False)
            )
        if getattr(provider, "offline_only", False) and gateway_budget() is None:
            identity["offline_charged_tokens"] = int(
                identity.get("offline_charged_tokens") or 0
            ) + int(units)
        request_targets = []
        for claim in request.claims:
            request_targets.append(
                {
                    "target_type": "claim",
                    "target_id": claim.claim_id,
                    "answer": claim.answer,
                    "supporting_record_ids": list(claim.supporting_record_ids),
                    "evidence": [
                        {
                            "page": evidence.page,
                            "section_title": evidence.section_title,
                            "anchor": {"bounded_quote": evidence.anchor.bounded_quote},
                        }
                        for evidence in claim.evidence
                    ],
                }
            )
        for condition in request.source_conditions:
            request_targets.append(
                {
                    "target_type": "condition",
                    "target_id": condition.condition_id,
                }
            )
        receipts.append(
            {
                "request_id": request.request_id,
                "purpose": purpose,
                "status": "success",
                "source_scope": source_scope,
                "source_pages": [int(page["page"]) for page in request.source_pages],
                "request_targets": request_targets,
                "response_payload": response.model_dump(mode="json"),
                "physical_request": True,
            }
        )
        return response

    selected: set[int] = set()
    selected_order: list[int] = []
    supplementary: set[int] = set()
    covered_nav: set[int] = set()
    queued_pages: set[int] = set()
    supplement_pages: list[int] = []
    completed_pages: set[int] = set()
    discovered_scopes: set[frozenset[int]] = set()

    def remember_map(batch, requested) -> None:
        requested_ids = []
        for page in requested:
            page_no = int(page)
            if page_no not in requested_ids:
                requested_ids.append(page_no)
            if page_no not in selected:
                selected_order.append(page_no)
            selected.add(page_no)
        requested_set = set(requested_ids)
        for page in batch:
            page_no = int(page["page"] if isinstance(page, dict) else page)
            covered_nav.add(page_no)
            if page_no not in by_page:
                continue
            by_page[page_no].update(
                structure_indexed=True,
                map_checked=True,
                map_selected=page_no in requested_set,
                status="partially_checked",
            )

    def context_page(evidence) -> dict | None:
        page_no = int(evidence.page)
        source = next((item for item in sources if int(item["page"]) == page_no), None)
        if source is None or not source.get("readable", True):
            return None
        paragraph = str(getattr(evidence.anchor, "context_before", "") or "").strip()
        quote = str(getattr(evidence.anchor, "bounded_quote", "") or "").strip()
        heading = str(getattr(evidence, "section_title", "") or "").strip()
        unique = unique_context_prefix(paragraph, quote) if quote else paragraph
        parts = []
        if heading and heading not in unique and heading not in quote:
            parts.append(heading)
        if unique:
            parts.append(unique)
        if parts:
            return {"page": page_no, "text": "\n".join(parts), "readable": True}
        if quote:
            return {"page": page_no, "text": heading or quote[:40], "readable": True}
        return source

    def pages_for(items) -> list[dict]:
        pages = []
        seen = set()
        for item in items:
            for evidence in getattr(item, "evidence", ()):
                fragment = context_page(evidence)
                if fragment is None:
                    continue
                key = (int(fragment["page"]), fragment["text"])
                if key not in seen:
                    seen.add(key)
                    pages.append(fragment)
                for page_no in getattr(evidence, "continuation_pages", ()):
                    source = next(
                        (row for row in sources if int(row["page"]) == int(page_no)),
                        None,
                    )
                    if source is None:
                        continue
                    key = (int(source["page"]), source["text"])
                    if key not in seen:
                        seen.add(key)
                        pages.append(source)
        return pages

    def claim_pages(claim) -> list[dict]:
        return pages_for([claim])

    def group_pages(group, related) -> list[dict]:
        return pages_for([*group, *related])

    def related_records(group):
        wanted = {key for claim in group for key in claim.supporting_record_ids}
        return tuple(record for record in structured.values() if record.record_id in wanted)

    def verify_units(group, pages, related) -> int:
        return request_reservation(
            ReportSemanticRequestV3(
                request_id=f"{work_id}:verify-size",
                purpose="verify_views",
                report=report,
                processing_identity=identity,
                source_pages=tuple(pages),
                source_scope="full_context",
                claims=tuple(group),
                records=tuple(related),
                max_output_tokens=_verify_output(identity),
                timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
            )
        )

    def upsert(claim):
        """Keep one object per id. A different answer is a new object with a stable id."""
        for index, earlier in enumerate(claims):
            if earlier.claim_id != claim.claim_id:
                continue
            if earlier.answer != claim.answer:
                taken = {item.claim_id for item in claims}
                number = 2
                renamed = f"{claim.claim_id}__{number}"
                while renamed in taken:
                    number += 1
                    renamed = f"{claim.claim_id}__{number}"
                claim = claim.model_copy(update={"claim_id": renamed})
                break
            verified = earlier.status == "observed" and earlier.limitation not in {
                "pending_verification",
                "claim_or_dependency_not_verified",
                "evidence_not_located",
            }
            if verified and claim.limitation == "pending_verification":
                return earlier
            claims[index] = claim
            return claim
        claims.append(claim)
        return claim

    def keep_pending(claim, reason: str) -> None:
        gaps.append(f"{reason}:{claim.claim_id}")
        upsert(
            claim.model_copy(update={"limitation": "pending_verification"})
            if claim.status == "observed"
            else claim
        )

    def apply_verdict(group, verified, pages) -> None:
        verdicts = {
            (item.target_type, item.target_id): item.status for item in verified.checks
        }
        quarantined = {
            item.target_id
            for item in verified.checks
            if item.target_type == "record" and item.status != "pass"
        }
        for key in quarantined:
            if key in structured:
                dispositions.append(
                    {
                        "record_id": key,
                        "status": "quarantined",
                        "reason": "record_semantic_conflict",
                    }
                )
                gaps.append(f"record_semantic_conflict:{key}")
                for claim_index, earlier in enumerate(claims):
                    if key not in earlier.supporting_record_ids or earlier.status != "observed":
                        continue
                    claims[claim_index] = earlier.model_copy(
                        update={
                            "status": "unclear",
                            "limitation": "claim_or_dependency_not_verified",
                        }
                    )
                    gaps.append(f"claim_not_verified:{earlier.claim_id}")
        kept_records = [item for item in related_records(group) if item.record_id not in quarantined]
        records[:] = [
            item
            for item in records
            if item.record_id not in {row.record_id for row in related_records(group)}
        ]
        records.extend(kept_records)
        accepted_ids = {item.record_id for item in records}
        for claim in group:
            dependency_blocked = bool(claim.supporting_record_ids) and (
                not set(claim.supporting_record_ids) <= accepted_ids
                or any(
                    verdicts.get(("record", key)) != "pass"
                    for key in claim.supporting_record_ids
                )
            )
            verdict = verdicts.get(("claim", claim.claim_id))
            evidence_ok = all(
                bool(getattr(getattr(evidence, "anchor", None), "bounded_quote", ""))
                and (
                    any(
                        evidence.anchor.bounded_quote in str(page.get("text") or "")
                        and int(page["page"]) == int(evidence.page)
                        for page in pages
                    )
                    or any(
                        evidence.anchor.bounded_quote in str(source.get("text") or "")
                        and int(source["page"]) == int(evidence.page)
                        for source in sources
                    )
                )
                for evidence in claim.evidence
            )
            if verdict != "pass" or not evidence_ok or dependency_blocked:
                if verdict is None:
                    keep_pending(claim, "claim_pending")
                else:
                    upsert(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "claim_or_dependency_not_verified",
                            }
                        )
                    )
                    gaps.append(f"claim_not_verified:{claim.claim_id}")
                continue
            upsert(
                claim.model_copy(
                    update={"status": "observed", "limitation": None}
                )
            )
        checks.extend(verified.checks)

    def retract_rejected(sent_claims, verified) -> None:
        """A block or a missing check retracts only the objects in this request."""
        verdicts = {
            (item.target_type, item.target_id): item.status for item in verified.checks
        }
        for claim in sent_claims:
            verdict = verdicts.get(("claim", claim.claim_id))
            if verdict == "pass":
                continue
            for index, earlier in enumerate(claims):
                if earlier.claim_id != claim.claim_id or earlier.status != "observed":
                    continue
                claims[index] = earlier.model_copy(
                    update={
                        "status": "unclear",
                        "limitation": "claim_or_dependency_not_verified",
                    }
                )
                gaps.append(f"claim_not_verified:{claim.claim_id}")
        for record in records:
            verdict = verdicts.get(("record", record.record_id))
            if verdict in (None, "pass"):
                continue
            dispositions.append(
                {
                    "record_id": record.record_id,
                    "status": "quarantined",
                    "reason": "record_semantic_conflict",
                }
            )
            gaps.append(f"record_semantic_conflict:{record.record_id}")
            for index, earlier in enumerate(claims):
                if record.record_id not in earlier.supporting_record_ids or earlier.status != "observed":
                    continue
                claims[index] = earlier.model_copy(
                    update={
                        "status": "unclear",
                        "limitation": "claim_or_dependency_not_verified",
                    }
                )
                gaps.append(f"claim_not_verified:{earlier.claim_id}")

    def settle_claims(package_claims, package_conditions=()) -> None:
        observable = []
        for claim in package_claims:
            if claim.status != "observed":
                upsert(claim)
                continue
            if not claim_pages(claim):
                gaps.append(f"evidence_not_located:{claim.claim_id}")
                upsert(
                    claim.model_copy(
                        update={
                            "status": "unclear",
                            "limitation": "evidence_not_located",
                        }
                    )
                )
                continue
            observable.append(claim)
        observable.sort(
            key=lambda claim: (
                {
                    "principal_business": 0,
                    "revenue_model": 1,
                    "products_services": 2,
                    "input_exposure": 3,
                    "supply_chain_role": 4,
                    "upstream_downstream": 5,
                    "output_exposure": 6,
                }.get(claim.dimension, 50),
                claim.claim_id,
            )
        )
        def already_trusted(claim) -> bool:
            if claim.status != "observed" or claim.limitation in {
                "pending_verification",
                "claim_or_dependency_not_verified",
                "evidence_not_located",
                "retracted_pending_correction",
                "subject_scope_omitted",
            }:
                return False
            latest = None
            for check in checks:
                if check.target_type == "claim" and check.target_id == claim.claim_id:
                    latest = check
            return latest is not None and latest.status == "pass"

        def measure(group, conditions):
            conditions = compatible_evidence_targets(group, conditions)
            related = related_records(group)
            group, conditions, related = compact_verify_targets(
                group, conditions, related
            )
            pages = pages_for([*group, *conditions, *related])
            for claim in group:
                for extra in related_conflict_pages(claim, sources):
                    if int(extra["page"]) not in {int(page["page"]) for page in pages}:
                        pages.append(extra)
            pages = compact_source_pages(pages, [*group, *conditions, *related])
            room = pack_room()
            try:
                units = joint_verify_reservation(
                    report, identity, pages, group, conditions, related
                )
                if pages:
                    sized = ReportSemanticRequestV3(
                        request_id=f"{work_id}:report:{len(receipts) + 1}",
                        purpose="verify_views",
                        report=report,
                        processing_identity=identity,
                        source_pages=tuple(pages),
                        source_scope="full_context",
                        claims=tuple(group),
                        records=tuple(related),
                        source_conditions=tuple(conditions),
                        max_output_tokens=_verify_output(identity),
                        timeout_seconds=float(
                            identity.get("logical_request_timeout_seconds") or 120
                        ),
                    )
                    cap = int(identity.get("max_payload_utf8_bytes") or 60_000)
                    if request_payload_bytes(sized) > cap or units > room:
                        return room + 1, pages, related, conditions
            except ValueError:
                return room + 1, pages, related, conditions
            return units, pages, related, conditions

        sent_conditions: set[str] = set()

        def attach(group, pool):
            chosen = []
            for condition in pool:
                if condition.condition_id in sent_conditions:
                    continue
                if not claims_covering_condition(condition, group):
                    continue
                if measure(group, chosen + [condition])[0] <= pack_room():
                    chosen.append(condition)
            return chosen

        def send(group, sent, *, support: bool) -> bool:
            units, pages, related, sent = measure(group, sent)
            if units > pack_room() or not pages:
                if support:
                    for claim in group:
                        keep_pending(claim, "verify_unit_not_admitted")
                    publish()
                else:
                    for condition in sent:
                        gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                return False
            try:
                verified = invoke(
                    "verify_views",
                    pages,
                    source_scope="full_context",
                    output=_verify_output(identity),
                    later=0,
                    extra={
                        "claims": tuple(group),
                        "records": related,
                        "source_conditions": tuple(sent),
                    },
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_verify_stopped:{exc}")
                if support:
                    for claim in group:
                        latest = None
                        for check in checks:
                            if (
                                check.target_type == "claim"
                                and check.target_id == claim.claim_id
                            ):
                                latest = check
                        if latest is not None and latest.status == "pass":
                            continue
                        keep_pending(claim, "claim_pending")
                publish()
                return False
            if support:
                apply_verdict(group, verified, pages)
                retract_rejected(group, verified)
            else:
                blocked = {
                    item.target_id
                    for item in verified.checks
                    if item.target_type == "claim" and item.status != "pass"
                }
                if blocked:
                    retract_rejected(
                        [claim for claim in group if claim.claim_id in blocked],
                        verified,
                    )
                checks.extend(
                    item for item in verified.checks if item.target_type == "condition"
                )
            checks[:] = [
                check
                for check in checks
                if not (
                    check.target_type == "condition"
                    and check.status == "pass"
                    and check.covers_target_type == "claim"
                    and not any(
                        item.claim_id == check.covers_target_id and item.status == "observed"
                        for item in claims
                    )
                )
            ]
            for page in pages:
                by_page[int(page["page"])]["status"] = "read"
            for condition in sent:
                sent_conditions.add(condition.condition_id)
            publish()
            return True

        remaining = [claim for claim in observable if not already_trusted(claim)]
        while remaining:
            room = pack_room()
            group: list = []
            next_round: list = []
            overflow: list = []
            for claim in remaining:
                if measure([claim], [])[0] > room:
                    overflow.append(claim)
                    continue
                trial = group + [claim]
                if group and measure(trial, [])[0] > room:
                    next_round.append(claim)
                else:
                    group = trial
            if not group:
                for claim in overflow + next_round:
                    keep_pending(claim, "verify_unit_not_admitted")
                    publish()
                break
            if not send(group, attach(group, package_conditions), support=True):
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
                if any(gap.startswith("direct_core_verify_stopped:") for gap in gaps):
                    for claim in next_round:
                        keep_pending(claim, "claim_pending")
                    for claim in overflow:
                        keep_pending(claim, "verify_unit_not_admitted")
                    return
            remaining = next_round
            for claim in overflow:
                keep_pending(claim, "verify_unit_not_admitted")
                publish()
        pending_conditions = [
            condition
            for condition in package_conditions
            if condition.condition_id not in sent_conditions
        ]

        def usable_claims(item):
            return [
                claim
                for claim in recheck_claims_for(item, observable)
                if claim.status == "observed"
                and claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                    "subject_scope_omitted",
                }
            ]

        pack_limit = [10**9]
        while pending_conditions:
            condition = pending_conditions.pop(0)
            chosen = list(usable_claims(condition))
            while chosen and measure(chosen, [condition])[0] > pack_room():
                chosen.pop()
            if not chosen:
                gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                sent_conditions.add(condition.condition_id)
                continue
            batch = [condition]
            later = []
            for other in pending_conditions:
                if len(batch) >= pack_limit[0]:
                    later.append(other)
                    continue
                extra = [claim for claim in usable_claims(other) if claim not in chosen]
                if not extra and not claims_covering_condition(other, chosen):
                    later.append(other)
                    continue
                trial = [*chosen, *extra]
                if measure(trial, batch + [other])[0] <= pack_room():
                    chosen = trial
                    batch.append(other)
                else:
                    later.append(other)
            pending_conditions = later
            if not send(chosen, batch, support=False):
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
                if len(batch) > 1 and any(
                    "payload_not_admitted" in gap for gap in gaps
                ):
                    gaps[:] = [
                        gap for gap in gaps if "payload_not_admitted" not in gap
                    ]
                    pack_limit[0] = max(1, len(batch) // 2)
                    pending_conditions = [*batch, *pending_conditions]
                    continue
                for item in batch:
                    if item.condition_id not in sent_conditions:
                        gaps.append(f"major_condition_not_verified:{item.condition_id}")
                        sent_conditions.add(item.condition_id)
            else:
                pack_limit[0] = 10**9

    def report_gap(page_ids) -> None:
        by_id = {int(page["page"]): page for page in sources}
        pages = [
            by_id[int(page_no)]
            for page_no in page_ids
            if int(page_no) in by_id and by_id[int(page_no)]["readable"]
        ]
        if not pages:
            gaps.append("direct_core_gap_check_stopped:no_core_context")
            return
        batches: list[list] = []
        batch: list = []
        for page in pages:
            trial = batch + [page]
            units = _units(
                trial,
                purpose="source_inventory",
                source_scope="full_context",
                report=report,
                identity=identity,
                output=_body_output(identity),
            )
            if units <= limit:
                batch = trial
                continue
            if batch:
                batches.append(batch)
            single = _units(
                [page],
                purpose="source_inventory",
                source_scope="full_context",
                report=report,
                identity=identity,
                output=_body_output(identity),
            )
            if single > limit:
                gaps.append(f"gap_page_not_admitted:{page['page']}")
                batch = []
            else:
                batch = [page]
        if batch:
            batches.append(batch)
        found = []
        for piece in batches:
            try:
                reviewed = invoke(
                    "source_inventory",
                    piece,
                    source_scope="full_context",
                    output=_body_output(identity),
                    later=_nonempty_verify_units(piece, report=report, identity=identity),
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_gap_check_stopped:{exc}")
                return
            accepted, location_gaps = _located_conditions(
                reviewed.source_conditions,
                report,
                piece,
                request_id=receipts[-1]["request_id"],
                seen=seen,
            )
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            found.extend(item for item in accepted if item.major)
            for page_no in reviewed.requested_pages:
                page_no = int(page_no)
                if page_no in by_page and by_page[page_no]["status"] != "read" and page_no not in queued_pages:
                    supplement_pages.append(page_no)
        if found:
            settle_conditions(found)
        if supplement_pages:
            extra = list(supplement_pages)
            supplement_pages.clear()
            read_selected(extra)

    def settle_conditions(found) -> None:
        def verified_claims():
            return [
                item
                for item in claims
                if item.status == "observed"
                and item.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                }
            ]

        def relevant(condition):
            pages = {int(evidence.page) for evidence in condition.evidence}
            picked = []
            for claim in verified_claims():
                claim_pages = {int(evidence.page) for evidence in claim.evidence}
                if claim.dimension == condition.dimension or claim_pages & pages:
                    picked.append(claim)
            return picked

        def sized(group, delivered):
            pages = pages_for([*group, *delivered])
            units = request_reservation(
                ReportSemanticRequestV3(
                    request_id=f"{work_id}:condition-size",
                    purpose="verify_views",
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(pages),
                    source_scope="full_context",
                    source_conditions=tuple(group),
                    claims=tuple(delivered),
                    max_output_tokens=_verify_output(identity),
                    timeout_seconds=float(identity.get("logical_request_timeout_seconds") or 120),
                )
            ) if pages else limit + 1
            return units, pages

        current = []
        current_claims = []
        groups = []
        for condition in found:
            delivered = relevant(condition)
            if not delivered:
                gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                continue
            units, _pages = sized([condition], delivered)
            if units > limit:
                gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                continue
            if current:
                merged_claims = list({item.claim_id: item for item in current_claims + delivered}.values())
                units, _pages = sized(current + [condition], merged_claims)
                if units > limit:
                    groups.append((current, current_claims))
                    current = [condition]
                    current_claims = delivered
                else:
                    current.append(condition)
                    current_claims = merged_claims
            else:
                current = [condition]
                current_claims = delivered
        if current:
            groups.append((current, current_claims))
        for group, delivered in groups:
            _units_now, pages = sized(group, delivered)
            if not pages:
                for condition in group:
                    gaps.append(f"major_condition_not_verified:{condition.condition_id}")
                continue
            try:
                verified = invoke(
                    "verify_views",
                    pages,
                    source_scope="full_context",
                    output=_verify_output(identity),
                    later=0,
                    extra={
                        "source_conditions": tuple(group),
                        "claims": tuple(delivered),
                    },
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_verify_stopped:{exc}")
                continue
            retract_rejected(delivered, verified)
            checks.extend(
                check
                for check in verified.checks
                if not (
                    check.target_type == "condition"
                    and check.covers_target_type == "claim"
                    and any(
                        item.claim_id == check.covers_target_id and item.status != "observed"
                        for item in claims
                    )
                )
            )

    def read_selected(page_ids) -> None:
        ordered = []
        for page in page_ids:
            page_no = int(page)
            if page_no in completed_pages or page_no in queued_pages or page_no in ordered:
                continue
            ordered.append(page_no)
        if not ordered:
            return
        queued_pages.update(ordered)
        by_id = {int(page["page"]): page for page in sources}
        chosen = [
            by_id[page_no]
            for page_no in ordered
            if page_no in by_id and by_id[page_no]["readable"]
        ]
        try:
            packages = list(body_packages(chosen, report, identity, limit))
        except ValueError as exc:
            gaps.append(str(exc))
            return
        index = 0
        while index < len(packages) and index < 8:
            package = packages[index]
            package_pages = {int(page["page"]) for page in package}
            scope = frozenset(package_pages)
            package_conditions = [
                item
                for item in conditions
                if scope & {int(evidence.page) for evidence in item.evidence}
            ]
            if scope not in discovered_scopes:
                try:
                    reviewed = invoke(
                        "source_inventory",
                        package,
                        source_scope="full_context",
                        output=_body_output(identity),
                        later=_nonempty_verify_units(package, report=report, identity=identity),
                    )
                    discovered_scopes.add(scope)
                    accepted, location_gaps = _located_conditions(
                        reviewed.source_conditions,
                        report,
                        package,
                        request_id=receipts[-1]["request_id"],
                        seen=seen,
                    )
                    gaps.extend(location_gaps)
                    conditions.extend(accepted)
                    package_conditions = accepted
                    for page_no in reviewed.requested_pages:
                        page_no = int(page_no)
                        if (
                            page_no in by_page
                            and by_page[page_no]["status"] != "read"
                            and page_no not in queued_pages
                        ):
                            supplement_pages.append(page_no)
                except (ValueError, LlmError, SemanticProviderError) as exc:
                    gaps.append(f"direct_core_gap_check_stopped:{exc}")
            try:
                drafted = invoke(
                    "synthesize_views",
                    package,
                    source_scope="full_context",
                    output=_body_output(identity),
                    later=_nonempty_verify_units(package, report=report, identity=identity),
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_package_stopped:{exc}")
                for page_no in package_pages:
                    gap = f"necessary_unread:{page_no}"
                    if gap not in gaps:
                        gaps.append(gap)
                if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                    return
                index += 1
                continue
            package_claims: list[ReportViewClaim] = []
            for claim in drafted.claims:
                bound = []
                failed = False
                for evidence in claim.evidence:
                    located = _bind_source_evidence(evidence, report, package)
                    if located is None:
                        located = _bind_equivalent_amount_evidence(
                            evidence, report, package
                        )
                    if located is None:
                        failed = True
                        break
                    bound.append(located)
                if failed or not bound:
                    gaps.append(f"evidence_not_located:{claim.claim_id}")
                    package_claims.append(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "evidence_not_located",
                            }
                        )
                    )
                    continue
                package_claims.append(claim.model_copy(update={"evidence": tuple(bound)}))
            settle_claims(package_claims, package_conditions)
            index += 1
            if any(gap.startswith("direct_core_ledger_hold") for gap in gaps):
                return
        if supplement_pages:
            extra = list(supplement_pages)
            supplement_pages.clear()
            read_selected(extra)

    def priority_order(page_ids):
        """Ask which already selected pages are the core reading order."""
        previews = []
        for page_no in page_ids:
            source = next((item for item in sources if int(item["page"]) == int(page_no)), None)
            if source is None:
                continue
            previews.append(
                {
                    "page": int(page_no),
                    "text": str(source.get("text") or ""),
                    "readable": True,
                }
            )
        if not previews:
            return [], list(page_ids)
        try:
            batches = navigation_batches(previews, report, identity, limit)
        except ValueError as exc:
            gaps.append(str(exc))
            gaps.append("chapter_selection_limited")
            return [], list(page_ids)
        core = []
        for batch in batches:
            full_pages = [
                source
                for source in sources
                if int(source["page"]) in {int(item["page"]) for item in batch}
            ]
            try:
                reserve_pages = full_pages
                if identity.get("chapter_select"):
                    reserve_pages = [
                        {
                            "page": int(source["page"]),
                            "text": " ".join(str(source.get("text") or "").split())[:700],
                            "readable": True,
                        }
                        for source in full_pages[:2]
                    ]
                response = invoke(
                    "source_inventory",
                    batch,
                    source_scope="page_map",
                    output=_nav_output(identity),
                    later=(
                        _first_delivery_tail(
                            reserve_pages,
                            report=report,
                            identity=identity,
                            limit=limit,
                        )
                        if identity.get("chapter_select")
                        else 0
                    ),
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                gaps.append(f"direct_core_navigation_incomplete:{exc}")
                gaps.append("chapter_selection_limited")
                return core, [page_no for page_no in page_ids if page_no not in core]
            for page_no in response.requested_pages:
                page_no = int(page_no)
                if page_no in page_ids and page_no not in core:
                    core.append(page_no)
        return core, [page_no for page_no in page_ids if page_no not in core]

    def bind_reused_candidates() -> list[ReportViewClaim]:
        bound_claims: list[ReportViewClaim] = []
        for item in identity.get("accepted_candidate_receipts") or ():
            payload = item.get("response_payload") or {}
            if item.get("purpose") != "synthesize_views" or payload.get("checks"):
                raise ValueError("reused_candidate_is_not_an_unverified_body_reading")
            response = ReportSemanticResponseV3.model_validate_json(
                json.dumps(payload, ensure_ascii=False)
            )
            page_ids = [int(page) for page in item.get("source_pages") or []]
            package = [page for page in sources if int(page["page"]) in page_ids]
            for claim in response.claims:
                located = []
                failed = False
                for evidence in claim.evidence:
                    bound = _bind_source_evidence(evidence, report, package)
                    if bound is None:
                        failed = True
                        break
                    located.append(bound)
                if failed or not located:
                    gaps.append(f"evidence_not_located:{claim.claim_id}")
                    claims.append(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "evidence_not_located",
                            }
                        )
                    )
                    continue
                bound_claims.append(claim.model_copy(update={"evidence": tuple(located)}))
            receipts.append({**item, "physical_request": False, "reused_candidate": True})
            for page_no in page_ids:
                if page_no in by_page:
                    by_page[page_no]["status"] = "read"
                    by_page[page_no]["full_text_sent"] = True
        return bound_claims

    inventoried: set[int] = set()
    source_later = 0

    def inventory_pages(page_ids, prepared=None) -> list[int]:
        """Read source text without candidate answers. Navigation is not body reading."""
        follow: list[int] = []
        ordered: list[int] = []
        for page in page_ids:
            page_no = int(page)
            if page_no in inventoried or page_no in ordered or page_no not in by_page:
                continue
            ordered.append(page_no)
        if not ordered:
            return follow
        by_id = {int(page["page"]): page for page in sources}
        chosen = [
            by_id[page_no]
            for page_no in ordered
            if by_id[page_no].get("readable", True)
        ]
        try:
            prepared_pages = {
                int(page["page"])
                for package in prepared or ()
                for page in package
                if page.get("scope") != "boundary_context"
            }
            if prepared and prepared_pages == {int(page["page"]) for page in chosen}:
                packages = list(prepared)
            else:
                room = ledger - charged() - source_later
                slice_limit = max(1024, min(limit, room))
                packages = list(
                    source_discovery_slices(chosen, report, identity, slice_limit)
                )
        except ValueError as exc:
            gaps.append(str(exc))
            for page_no in ordered:
                gap = f"necessary_unread:{page_no}"
                if gap not in gaps:
                    gaps.append(gap)
            return follow
        already = set(inventoried)
        for package in packages:
            try:
                reviewed = invoke(
                    "source_inventory",
                    package,
                    source_scope="full_context",
                    output=_source_output(identity),
                    later=source_later,
                )
            except (ValueError, LlmError, SemanticProviderError) as exc:
                text = str(exc)
                recovery_stopped = (
                    "card_recovery_exhausted" in text
                    or "logical_recovery_already_used" in text
                )
                if (
                    inventoried - already
                    and "direct_core_admission_stopped" in text
                ):
                    while "direct_core_ledger_hold" in gaps:
                        gaps.remove("direct_core_ledger_hold")
                    return follow
                if recovery_stopped:
                    gaps.append(f"direct_core_recovery_refused:{text}")
                else:
                    gaps.append(f"direct_core_gap_check_stopped:{text}")
                if recovery_stopped or any(
                    gap.startswith("direct_core_ledger_hold") for gap in gaps
                ):
                    return follow
                if len(package) > 1:
                    middle = len(package) // 2
                    follow.extend(
                        inventory_pages(int(page["page"]) for page in package[:middle])
                    )
                    follow.extend(
                        inventory_pages(int(page["page"]) for page in package[middle:])
                    )
                    continue
                for page in package:
                    gap = f"necessary_unread:{int(page['page'])}"
                    if gap not in gaps:
                        gaps.append(gap)
                continue
            for page in package:
                if page.get("scope") == "boundary_context":
                    continue
                page_no = int(page["page"])
                inventoried.add(page_no)
                by_page[page_no]["status"] = "read"
                by_page[page_no]["full_text_sent"] = True
            accepted, location_gaps = _located_conditions(
                reviewed.source_conditions,
                report,
                package,
                request_id=receipts[-1]["request_id"],
                seen=seen,
            )
            accepted = compact_source_conditions(accepted)
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            for page_no in reviewed.requested_pages:
                page_no = int(page_no)
                if page_no in by_page and page_no not in inventoried and page_no not in follow:
                    follow.append(page_no)
            publish()
        return follow

    def note_missed_core_revenue():
        """Turn already-read totals into a core question, not a page whitelist."""
        read_sources = [
            page
            for page in sources
            if by_page.get(int(page["page"]), {}).get("status") == "read"
        ]
        if not read_sources:
            cited = {
                int(evidence.page)
                for item in (*conditions, *claims)
                for evidence in getattr(item, "evidence", ()) or ()
            }
            read_sources = [page for page in sources if int(page["page"]) in cited]
        missed = missed_core_revenue_facts(read_sources, conditions, claims)
        if not missed:
            return None
        gap = "read_but_missed:period-operating-revenue"
        if gap not in gaps:
            gaps.append(gap)
        existing = next(
            (
                item
                for item in conditions
                if item.condition_id.startswith("core-question:period-operating-revenue")
            ),
            None,
        )
        if existing is not None:
            return existing
        question = make_operating_revenue_question(report, missed)
        conditions.append(question)
        return question

    composed: set[int] = set()

    def compose(page_ids) -> None:
        """Draft from pages that already have a successful source read."""
        by_id = {int(page["page"]): page for page in sources}
        chosen = []
        seen_pages: set[int] = set()
        for page in page_ids:
            page_no = int(page)
            if (
                page_no in seen_pages
                or page_no in composed
                or page_no not in by_id
                or not by_id[page_no].get("readable", True)
                or by_page.get(page_no, {}).get("status") != "read"
            ):
                continue
            seen_pages.add(page_no)
            chosen.append(by_id[page_no])
        if not chosen:
            return
        quote_pages = [
            dict(page)
            for page in cited_quote_pages(
                evidence
                for condition in conditions
                for evidence in condition.evidence
                if int(evidence.page) in {int(item["page"]) for item in chosen}
            )
        ]
        draft_input = quote_pages or chosen
        try:
            packages = list(body_packages(draft_input, report, identity, limit))
        except ValueError as exc:
            gaps.append(str(exc))
            return
        if not packages:
            return
        if len(packages) > 1:
            gaps.append("company_synthesis_context_truncated")
            kept_pages = {int(page["page"]) for page in packages[0]}
            for condition in conditions:
                if kept_pages & {int(evidence.page) for evidence in condition.evidence}:
                    continue
                gap = f"major_condition_not_in_synthesis:{condition.condition_id}"
                if gap not in gaps:
                    gaps.append(gap)
        package = packages[0]
        page_set = {int(page["page"]) for page in chosen}
        rank = {
            "principal_business": 0,
            "revenue_model": 1,
            "products_services": 2,
            "input_exposure": 3,
            "supply_chain_role": 4,
            "upstream_downstream": 5,
            "output_exposure": 6,
        }
        related = sorted(
            (
                item
                for item in conditions
                if page_set & {int(evidence.page) for evidence in item.evidence}
            ),
            key=lambda item: (rank.get(item.dimension, 50), item.condition_id),
        )

        pending = [
            item
            for item in related
            if item.condition_id not in drafted_condition_ids
            and item.major
            and item.statement_status == "current"
            and item.dimension in _CORE_CLOSEOUT_DIMENSIONS
        ]
        if pending:
            deliver_group(pending)
        for page in package:
            page_no = int(page["page"])
            composed.add(page_no)
            if page_no in by_page:
                by_page[page_no]["status"] = "read"
                by_page[page_no]["full_text_sent"] = True

    def draft_reused_sources() -> None:
        """Write pending drafts from saved source conditions, then verify."""
        rank = {
            "principal_business": 0,
            "revenue_model": 1,
            "products_services": 2,
            "input_exposure": 3,
            "supply_chain_role": 4,
            "upstream_downstream": 5,
            "output_exposure": 6,
        }
        payload_cap = int(identity.get("max_payload_utf8_bytes") or 60_000)
        draft_cap = min(payload_cap, 32_000)

        def synthesis_request(pages, selected):
            return ReportSemanticRequestV3(
                request_id=f"{work_id}:synthesis-size",
                purpose="synthesize_views",
                report=report,
                processing_identity=identity,
                source_pages=tuple(pages),
                source_scope="full_context",
                source_conditions=tuple(selected),
                max_output_tokens=_body_output(identity),
                timeout_seconds=float(
                    identity.get("logical_request_timeout_seconds") or 120
                ),
            )

        def fits_synthesis(pages, selected) -> bool:
            if not pages:
                return False
            request = synthesis_request(pages, selected)
            return (
                request_payload_bytes(request) <= draft_cap
                and request_reservation(request) <= limit
            )

        ordered = [
            condition
            for condition in sorted(
                conditions_to_draft,
                key=lambda item: (rank.get(item.dimension, 50), item.condition_id),
            )
            if condition.condition_id not in drafted_condition_ids
        ]
        if identity.get("saved_source_draft"):
            themes = (
                ("principal_business", "products_services", "supply_chain_role"),
                ("revenue_model",),
                ("input_exposure", "output_exposure", "upstream_downstream"),
            )
            grouped = []
            for dims in themes:
                group = [condition for condition in ordered if condition.dimension in dims]
                if group:
                    grouped.append(group)
        else:
            grouped = _business_groups(ordered)
        for group in grouped:
            if _core_answers_ready(claims, conditions, checks, records) or (
                identity.get("successor_core_closeout")
                and _core_closeout_ready(claims, conditions, checks, records)
            ):
                return
            deliver_group(group)

    drafted_condition_ids: set[str] = set()
    pending_coverage: list[tuple[ReportSourceCondition, list]] = []

    def consume_coverage(drafted, draft_conditions) -> None:
        """Attach leftover coverage checks to this verify. Page hints are not a pass."""
        extra_claims = []
        extra_conds = []
        seen_ids = {claim.claim_id for claim in drafted}
        while pending_coverage:
            condition, chosen = pending_coverage.pop(0)
            extra_conds.append(condition)
            for claim in chosen:
                if claim.claim_id in seen_ids:
                    continue
                extra_claims.append(claim)
                seen_ids.add(claim.claim_id)
        settle_claims([*drafted, *extra_claims], [*draft_conditions, *extra_conds])
        publish()

    def deliver_group(group: list[ReportSourceCondition], *, recovered: bool = False) -> None:
        """Save one business group, verify it, and publish before the next group."""
        pending = [
            condition
            for condition in group
            if condition.condition_id not in drafted_condition_ids
        ]
        if not pending:
            return
        pending = compatible_evidence_targets([], pending)
        pages = [
            dict(page)
            for page in cited_quote_pages(
                evidence for item in pending for evidence in item.evidence
            )
        ]
        if identity.get("core_gap_closeout"):
            full_pages = []
            for page in pages:
                source = next(
                    (
                        item
                        for item in sources
                        if int(item["page"]) == int(page["page"]) and str(item.get("text") or "").strip()
                    ),
                    None,
                )
                if source is None:
                    continue
                full_pages.append(
                    {"page": int(page["page"]), "text": str(source["text"]), "readable": True}
                )
            if full_pages:
                pages = full_pages
        for evidence in (item for condition in pending for item in condition.evidence):
            offset = getattr(getattr(evidence, "anchor", None), "source_offset", None)
            heading = governing_subject_heading(
                sources,
                int(evidence.page),
                before_offset=offset if offset else None,
            )
            if not heading:
                continue
            for page in pages:
                if int(page["page"]) == int(evidence.page) and heading not in str(
                    page.get("text") or ""
                ):
                    page["text"] = f"{heading}\n{page.get('text') or ''}"
        sized = ReportSemanticRequestV3(
            request_id=f"{work_id}:report:{len(receipts) + 1}",
            purpose="synthesize_views",
            report=report,
            processing_identity=identity,
            source_pages=tuple(pages),
            source_scope="full_context",
            source_conditions=tuple(pending),
            max_output_tokens=_body_output(identity),
            timeout_seconds=float(
                identity.get("logical_request_timeout_seconds") or 120
            ),
        )
        cap = int(identity.get("max_payload_utf8_bytes") or 60_000)
        room = pack_room()
        draft_units = request_reservation(sized)
        verify_units = _nonempty_verify_units(pages, report=report, identity=identity)
        survives_one_unknown = charged() + draft_units * 2 + verify_units <= ledger
        too_big = (
            request_payload_bytes(sized) > cap
            or request_reservation(sized) > room
            or (
                identity.get("saved_source_draft")
                and not survives_one_unknown
            )
        )
        if too_big and len(pending) > 1:
            middle = max(1, len(pending) // 2)
            deliver_group(pending[:middle], recovered=recovered)
            deliver_group(pending[middle:], recovered=recovered)
            return
        if too_big:
            gap = f"necessary_fragment:{pending[0].condition_id}"
            if gap not in gaps:
                gaps.append(gap)
            return
        try:
            drafted = invoke(
                "synthesize_views",
                pages,
                source_scope="full_context",
                output=_body_output(identity),
                later=0,
                extra={"source_conditions": tuple(pending)},
            )
        except (ValueError, LlmError, SemanticProviderError) as exc:
            text = str(exc)
            retryable = (
                "deadline" in text.lower()
                or "timed out" in text.lower()
                or "timeout" in text.lower()
                or "transport callback failed" in text.lower()
                or "card_request_reservation_exceeded" in text
                or "direct_core_admission_stopped" in text
            )
            budget_stopped = "budget_exhausted" in text or "physical_request_token" in text
            if budget_stopped:
                gaps.append(f"direct_core_package_stopped:{exc}")
                return
            if not recovered and retryable:
                deliver_group(pending, recovered=True)
                return
            if len(pending) > 1 and retryable:
                middle = max(1, len(pending) // 2)
                deliver_group(pending[:middle], recovered=True)
                deliver_group(pending[middle:], recovered=True)
            else:
                gaps.append(f"direct_core_package_stopped:{exc}")
            return
        drafted_group: list[ReportViewClaim] = []
        for claim in drafted.claims:
            bound = []
            failed = False
            for evidence in claim.evidence:
                located = _bind_source_evidence(evidence, report, sources)
                if located is None:
                    failed = True
                    break
                bound.append(located)
            if failed or not bound:
                gaps.append(f"evidence_not_located:{claim.claim_id}")
                claim = claim.model_copy(
                    update={
                        "status": "unclear",
                        "limitation": "evidence_not_located",
                        "evidence": tuple(bound),
                    }
                )
                upsert(claim)
            else:
                claim = claim.model_copy(update={"evidence": tuple(bound)})
                stored = upsert(
                    claim.model_copy(update={"limitation": "pending_verification"})
                )
                drafted_group.append(stored.model_copy(update={"limitation": None}))
            publish()
        for condition in pending:
            drafted_condition_ids.add(condition.condition_id)
        located_conditions = []
        for condition in pending:
            bound_evidence = []
            failed = False
            for evidence in condition.evidence:
                located = _bind_source_evidence(evidence, report, sources)
                if located is None:
                    failed = True
                    break
                bound_evidence.append(located)
            located_conditions.append(
                condition
                if failed
                else condition.model_copy(update={"evidence": tuple(bound_evidence)})
            )
        if drafted_group or pending_coverage:
            consume_coverage(drafted_group, located_conditions)
        returned_ids = {
            str(item)
            for claim in drafted.claims
            for item in (claim.condition_ids or ())
        }
        quiet = [
            condition
            for condition in located_conditions
            if condition.condition_id not in returned_ids
            and not any(
                _condition_id_names(
                    item,
                    condition.condition_id,
                    tail_unique=sum(
                        1
                        for other in returned_ids
                        if other.split(":")[-1] == condition.condition_id.split(":")[-1]
                    )
                    == 1,
                )
                for item in returned_ids
            )
        ]
        if quiet:
            related = []
            for condition in quiet:
                for claim in recheck_claims_for(condition, claims):
                    if claim.limitation in {
                        "pending_verification",
                        "claim_or_dependency_not_verified",
                        "evidence_not_located",
                        "retracted_pending_correction",
                        "subject_scope_omitted",
                    }:
                        continue
                    if claim.claim_id in set(identity.get("isolated_claim_ids") or ()):
                        continue
                    if claim not in related:
                        related.append(claim)
            if related:
                settle_claims(related, quiet)
                publish()

    def draft_ready_core() -> None:
        """Draft the next core group once principal and revenue evidence both exist."""
        pending = [
            condition
            for condition in conditions
            if condition.condition_id not in drafted_condition_ids
            and condition.statement_status == "current"
            and condition.major
            and condition.dimension
            in {
                "principal_business",
                "revenue_model",
                "products_services",
                "input_exposure",
                "supply_chain_role",
                "upstream_downstream",
            }
        ]
        dims = {condition.dimension for condition in pending}
        if "principal_business" not in dims or not (
            {"revenue_model", "products_services"} & dims
        ):
            return
        deliver_group(_business_groups(pending)[0])

    chapter_required_pages: set[int] | None = None
    isolated_condition_ids: set[str] = set()
    reused_receipts = list(identity.get("accepted_source_receipts") or ())
    if (
        identity.get("chapter_select")
        and (reused_receipts or identity.get("accepted_prior_draft"))
        and not identity.get("compatible_continue")
    ):
        raise ValueError("chapter_select_refuses_saved_semantics")
    if reused_receipts:
        for item in reused_receipts:
            payload = item.get("response_payload") or {}
            response = ReportSemanticResponseV3.model_validate_json(
                json.dumps(payload, ensure_ascii=False)
            )
            sent = {int(page) for page in item.get("source_pages") or []}
            locate_pages = [
                page
                for page in sources
                if not sent or int(page["page"]) in sent
            ] or list(sources)
            accepted, location_gaps = _located_conditions(
                response.source_conditions,
                report,
                locate_pages,
                request_id=str(item.get("request_id") or work_id),
                seen=seen,
            )
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            receipts.append(
                {**item, "physical_request": False, "reused_source": True}
            )
            for page_no in item.get("source_pages") or []:
                page_no = int(page_no)
                if page_no not in by_page:
                    continue
                by_page[page_no]["status"] = "read"
                by_page[page_no]["reused_source"] = True
        conditions_to_draft = []
        pending_core: list[ReportViewClaim] = []
        prior = identity.get("accepted_prior_draft") or {}
        if prior:
            retract = set(identity.get("retract_claim_ids") or ())
            defer = set(identity.get("defer_claim_ids") or ())
            kept_answers: list[str] = []
            for raw in prior.get("claims") or []:
                claim = ReportViewClaim.model_validate_json(
                    json.dumps(raw, ensure_ascii=False)
                )
                if claim.claim_id in retract:
                    gaps.append(f"retracted_for_correction:{claim.claim_id}")
                    upsert(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "retracted_pending_correction",
                            }
                        )
                    )
                    conditions.append(
                        ReportSourceCondition(
                            condition_id=f"correction:{claim.claim_id}",
                            dimension=claim.dimension,
                            description=(
                                "只根据这些引文重写。一般税收、收费或价格政策不是公司实际收取的费用。"
                                "为对方销售不是向对方销售。期末应付款或应收款余额不是本期采购或本期费用。"
                                "分开表述的金额不要相加。"
                                "同页连续标题不能当作后面每一段的业务名；无法区分时保留上层已写明的业务语境，不要猜最近标题。"
                                "只写核心服务或角色，不要把增长率、品牌口号或地区名次写成新的主业。"
                            ),
                            evidence=claim.evidence,
                            major=True,
                            statement_status="current",
                        )
                    )
                    continue
                if claim.limitation == "subject_scope_omitted" and claim.claim_id not in defer:
                    rebound = []
                    for evidence in claim.evidence:
                        located = _bind_source_evidence(evidence, report, sources)
                        rebound.append(located or evidence)
                    claim = claim.model_copy(
                        update={
                            "evidence": tuple(rebound),
                            "status": "observed",
                            "limitation": "pending_verification",
                        }
                    )
                    upsert(claim)
                    if claim.dimension in _CORE_CLOSEOUT_DIMENSIONS:
                        pending_core.append(claim.model_copy(update={"limitation": None}))
                    continue
                if claim.claim_id in set(identity.get("isolated_claim_ids") or ()):
                    isolated_condition_ids.update(claim.condition_ids or ())
                    upsert(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "retracted_pending_correction",
                            }
                        )
                    )
                    continue
                kept_answers.append(claim.answer)
                upsert(claim)
                if (
                    claim.claim_id not in defer
                    and claim.limitation == "pending_verification"
                    and claim.dimension in _CORE_CLOSEOUT_DIMENSIONS
                ):
                    pending_core.append(claim)
                    drafted_condition_ids.update(str(item) for item in (claim.condition_ids or ()))
            kept_checks = []
            for raw_check in prior.get("checks") or []:
                check = ReportViewCheck.model_validate(raw_check)
                if check.target_id in retract:
                    continue
                kept_checks.append(check)
            checks.extend(kept_checks)
            for item in prior.get("verify_receipts") or []:
                receipts.append(
                    {**item, "physical_request": False, "reused_verification": True}
                )
            cited_ids = {
                condition_id
                for claim in claims
                if claim.status == "observed"
                and claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                }
                and any(
                    check.target_type == "claim"
                    and check.target_id == claim.claim_id
                    and check.status == "pass"
                    for check in checks
                )
                for condition_id in (claim.condition_ids or [])
            }
            answer_text = "\n".join(
                claim.answer
                for claim in claims
                if claim.status == "observed"
                and claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                    "subject_scope_omitted",
                }
            )
            selected_conditions = []
            if identity.get("core_gap_closeout"):
                role_evidence = []
                for claim in claims:
                    if claim.dimension not in {
                        "principal_business",
                        "products_services",
                        "revenue_model",
                    }:
                        continue
                    if claim.status != "observed" or claim.limitation:
                        continue
                    role_evidence.extend(claim.evidence)
                selected_conditions = []
                if role_evidence:
                    selected_conditions.append(
                        ReportSourceCondition(
                            condition_id="operating-role-from-current-business",
                            dimension="supply_chain_role",
                            description=(
                                "只根据所供原文归纳公司当前生产或提供的产品与服务、服务对象和上下游位置。"
                                "期末余额不是经营角色。不要把计划中的目标写成已经实现的身份，也不要编造原文没有的客户。"
                            ),
                            evidence=tuple(role_evidence[:12]),
                            major=True,
                            statement_status="current",
                        )
                    )
            else:
                for condition in conditions:
                    if condition.condition_id.startswith("correction:"):
                        selected_conditions.append(condition)
                        continue
                    quote_amounts = re.findall(
                        r"\d{1,3}(?:,\d{3})+(?:\.\d+)?",
                        "\n".join(
                            evidence.anchor.bounded_quote for evidence in condition.evidence
                        ),
                    )
                    if (
                        condition.major
                        and condition.statement_status == "current"
                        and condition.dimension in _CORE_CLOSEOUT_DIMENSIONS
                    ):
                        selected_conditions.append(condition)
            if not identity.get("core_gap_closeout"):
                selected_conditions.sort(
                    key=lambda item: (
                        item.condition_id.startswith("correction:"),
                        {
                            "principal_business": 0,
                            "revenue_model": 1,
                            "products_services": 2,
                            "input_exposure": 3,
                            "supply_chain_role": 4,
                            "upstream_downstream": 5,
                        }.get(item.dimension, 50),
                        item.condition_id,
                    )
                )
            terms = [
                str(term)
                for term in identity.get("supplement_context_terms") or ()
                if str(term).strip()
            ]
            quoted = "\n".join(
                evidence.anchor.bounded_quote
                for condition in conditions
                for evidence in condition.evidence
            )
            supplement_pages = []
            for page in sources:
                text = str(page.get("text") or "")
                if any(term in text and term not in quoted for term in terms):
                    supplement_pages.append(page)
            if (
                supplement_pages
                and not identity.get("successor_core_closeout")
                and not identity.get("read_remaining_sources")
            ):
                try:
                    slices = list(
                        source_discovery_slices(
                            supplement_pages[:2], report, identity, limit
                        )
                    )
                    package = next(
                        (
                            item
                            for item in slices
                            if any(
                                term in "".join(str(page.get("text") or "") for page in item)
                                for term in terms
                            )
                        ),
                        slices[0] if slices else None,
                    )
                    if package:
                        reviewed = invoke(
                            "source_inventory",
                            package,
                            source_scope="full_context",
                            output=_source_output(identity),
                            later=0,
                        )
                        extra, extra_gaps = _located_conditions(
                            reviewed.source_conditions,
                            report,
                            package,
                            request_id=receipts[-1]["request_id"],
                            seen=seen,
                        )
                        gaps.extend(extra_gaps)
                        selected_conditions.extend(extra)
                except (ValueError, LlmError, SemanticProviderError) as exc:
                    gaps.append(f"direct_core_gap_check_stopped:{exc}")
            if identity.get("continue_unread"):
                fresh_ids = set()
                for item in identity.get("accepted_source_receipts") or []:
                    if not item.get("physical_request"):
                        continue
                    request_id = str(item.get("request_id") or "")
                    for condition in (item.get("response_payload") or {}).get("source_conditions") or []:
                        condition_id = str(condition.get("condition_id") or "")
                        if request_id and condition_id:
                            fresh_ids.add(f"{request_id}:{condition_id}")
                conditions_to_draft = [
                    condition
                    for condition in selected_conditions
                    if condition.condition_id in fresh_ids
                    or "funding_and_capital" in condition.condition_id
                ]
            else:
                conditions_to_draft = list(selected_conditions)
        elif identity.get("saved_source_draft"):
            conditions_to_draft = [
                condition
                for condition in conditions
                if condition.major
                and condition.statement_status == "current"
                and condition.dimension in _CORE_CLOSEOUT_DIMENSIONS
            ]
        if identity.get("saved_source_draft"):
            for index, claim in enumerate(list(claims)):
                if claim.limitation == "pending_verification" and not (
                    claim.condition_ids or ()
                ):
                    claims[index] = claim.model_copy(
                        update={"status": "unclear", "limitation": None}
                    )
        unread = [
            int(page["page"])
            for page in sources
            if (identity.get("continue_unread") or identity.get("read_remaining_sources"))
            and page.get("readable", True)
            and str(page.get("text") or "").strip()
            and by_page[int(page["page"])]["status"] != "read"
        ]
        if unread and not identity.get("core_repair_pages"):
            inventory_pages(unread)
        repair_pages = {int(page) for page in identity.get("core_repair_pages") or ()}
        saved_follow = []
        for item in reused_receipts:
            if item.get("purpose") != "source_inventory" or item.get("status") != "success":
                continue
            requested_here = {
                int(page) for page in item.get("source_pages") or []
            }
            for page in (item.get("response_payload") or {}).get("requested_pages") or []:
                page_no = int(page)
                if (
                    page_no in by_page
                    and by_page[page_no]["status"] != "read"
                    and page_no not in requested_here
                    and page_no not in saved_follow
                ):
                    saved_follow.append(page_no)
        if (repair_pages or saved_follow) and not identity.get("saved_source_draft"):
            known_ids = {condition.condition_id for condition in conditions}
            chapter_required_pages = {
                int(item["page"]) for item in coverage if item["status"] == "read"
            } | repair_pages | set(saved_follow)
            follow = inventory_pages(repair_pages) if repair_pages else []
            expand = [
                int(page)
                for page in [*saved_follow, *follow]
                if int(page) in by_page and int(page) not in repair_pages
            ]
            if expand and not any(
                gap.startswith("direct_core_ledger_hold") for gap in gaps
            ):
                gaps.append("chapter_gap_expansion")
                chapter_required_pages.update(expand)
                inventory_pages(expand)
            touched = repair_pages | set(saved_follow) | set(expand)
            fresh = [
                condition
                for condition in conditions
                if condition.condition_id not in known_ids
                and condition.condition_id not in drafted_condition_ids
                and condition.major
                and condition.statement_status == "current"
                and condition.dimension in _CORE_CLOSEOUT_DIMENSIONS
                and any(int(evidence.page) in touched for evidence in condition.evidence)
            ]
            drafted_ids = {condition.condition_id for condition in conditions_to_draft}
            conditions_to_draft.extend(
                condition for condition in fresh if condition.condition_id not in drafted_ids
            )
        elif reused_receipts and not identity.get("read_remaining_sources"):
            chapter_required_pages = {
                int(item["page"]) for item in coverage if item["status"] == "read"
            }
        unread_role_this_turn: list[int] = []
        if identity.get("compatible_continue"):
            read_sources = [
                page
                for page in sources
                if by_page.get(int(page["page"]), {}).get("status") == "read"
            ]
            for index, claim in enumerate(list(claims)):
                if claim.limitation != "evidence_not_located":
                    continue
                bound = []
                failed = False
                for evidence in claim.evidence:
                    located = _bind_source_evidence(evidence, report, read_sources)
                    if located is None:
                        located = _bind_equivalent_amount_evidence(
                            evidence, report, read_sources
                        )
                    if located is None:
                        failed = True
                        break
                    bound.append(located)
                if failed or not bound:
                    continue
                rebound_claim = claim.model_copy(
                    update={
                        "status": "observed",
                        "limitation": None,
                        "evidence": tuple(bound),
                    }
                )
                claims[index] = rebound_claim
                if rebound_claim.dimension in _CORE_CLOSEOUT_DIMENSIONS:
                    pending_core.append(rebound_claim)
            if (
                not identity.get("saved_source_draft")
                and not _operating_role_established(claims, checks)
            ):
                unread = _directed_operating_role_pages(sources, by_page)
                unread_role_this_turn = list(unread)
                if unread:
                    if chapter_required_pages is None:
                        chapter_required_pages = {
                            int(item["page"])
                            for item in coverage
                            if item["status"] == "read"
                        }
                    inventory_pages(unread)
                    chapter_required_pages.update(
                        page for page in unread if page in inventoried
                    )
                    unread_role_this_turn = [
                        page for page in unread if page in inventoried
                    ]
                role_pages = [
                    page
                    for page in sources
                    if int(page["page"]) in set(unread_role_this_turn)
                ]
                role_evidence = []
                for page in role_pages[:4]:
                    snippet = " ".join(str(page.get("text") or "").split())[:180]
                    if not snippet:
                        continue
                    role_evidence.append(
                        Evidence(
                            evidence_id=f"operating-role-{page['page']}",
                            report=report,
                            page=int(page["page"]),
                            section_title="经营角色",
                            anchor=TextAnchor(bounded_quote=snippet),
                        )
                    )
                existing_role = next(
                    (
                        item
                        for item in conditions
                        if str(item.condition_id).split(":")[-1] == "operating-role"
                    ),
                    None,
                )
                if role_evidence:
                    question = existing_role or make_operating_role_question(
                        report, role_evidence[:8]
                    )
                    if existing_role is None:
                        conditions.append(question)
                    else:
                        question = existing_role.model_copy(
                            update={"evidence": tuple(role_evidence[:8])}
                        )
                        conditions[conditions.index(existing_role)] = question
                    if question not in conditions_to_draft:
                        conditions_to_draft.append(question)
                elif existing_role is not None:
                    gaps.append("direct_core_no_new_input:operating_role")
        def shown_claim_ids(condition) -> set[str]:
            """Claims in the latest verification that already judged this condition."""
            tail = condition.condition_id.split(":")[-1]
            shown: set[str] = set()
            for item in receipts:
                if item.get("purpose") != "verify_views":
                    continue
                targets = item.get("request_targets") or []
                cond_ids = [
                    str(target.get("target_id") or "")
                    for target in targets
                    if target.get("target_type") == "condition"
                ]
                named = any(
                    item_id == condition.condition_id
                    or _condition_id_names(
                        item_id,
                        condition.condition_id,
                        tail_unique=sum(
                            1
                            for other in cond_ids
                            if other.split(":")[-1] == tail
                        )
                        == 1,
                    )
                    for item_id in cond_ids
                )
                if condition.condition_id not in cond_ids and not named:
                    continue
                shown = {
                    str(target.get("target_id"))
                    for target in targets
                    if target.get("target_type") == "claim" and target.get("target_id")
                }
            return shown

        def trusted_for(condition) -> list:
            return [
                claim
                for claim in recheck_claims_for(condition, claims)
                if claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                    "subject_scope_omitted",
                }
                and claim.claim_id not in set(identity.get("isolated_claim_ids") or ())
            ]

        def pending_for(condition) -> list:
            return [
                claim
                for claim in recheck_claims_for(condition, claims)
                if claim.status == "observed"
                and claim.limitation == "pending_verification"
                and claim.claim_id not in set(identity.get("isolated_claim_ids") or ())
            ]

        def gap_copy(condition, referenced) -> ReportSourceCondition:
            lines = [condition.description]
            if referenced:
                lines.append(
                    "已有正文如下。只补充这些正文没有写出、且所供引文里确实写出的业务事实。"
                    "战略口号、地区名次和增长率不是新的主业。引文没有的分句不要写。"
                    "若没有需要新增的事实，不要返回主张。"
                )
                lines.extend(claim.answer for claim in referenced[:3])
            if condition.condition_id in isolated_condition_ids or any(
                condition.condition_id.endswith(":" + item)
                for item in isolated_condition_ids
            ):
                lines.append(
                    "被撤回的旧句不要照抄，不要写入该句的增长率；只写服务和本期水平金额。"
                )
            return condition.model_copy(update={"description": "\n".join(lines)})

        recheck_pairs: list[tuple[ReportSourceCondition, list]] = []
        core_answers = []
        for condition in list(conditions_to_draft):
            latest = None
            tail = condition.condition_id.split(":")[-1]
            for check in checks:
                if check.target_type == "condition" and check.target_id in {
                    condition.condition_id,
                    tail,
                }:
                    latest = check
            if latest is not None and latest.status == "supplementary":
                continue
            if latest is not None and latest.status == "pass":
                cover = next(
                    (claim for claim in claims if claim.claim_id == latest.covers_target_id),
                    None,
                )
                if (
                    cover is not None
                    and cover.status == "observed"
                    and cover.limitation
                    not in {
                        "pending_verification",
                        "claim_or_dependency_not_verified",
                        "evidence_not_located",
                        "retracted_pending_correction",
                        "subject_scope_omitted",
                    }
                    and cover.claim_id not in set(identity.get("isolated_claim_ids") or ())
                ):
                    continue
            pending = pending_for(condition)
            referenced = trusted_for(condition)
            if pending:
                merged = {claim.claim_id: claim for claim in (*pending, *referenced)}
                recheck_pairs.append((condition, list(merged.values())))
                continue
            if latest is not None and latest.status in {"unclear", "block"}:
                if str(condition.condition_id).split(":")[-1] == "operating-role":
                    if unread_role_this_turn:
                        core_answers.append(condition)
                    else:
                        gap = "direct_core_no_new_input:operating_role"
                        if gap not in gaps:
                            gaps.append(gap)
                    continue
                fresh = [
                    claim
                    for claim in referenced
                    if claim.claim_id not in shown_claim_ids(condition)
                ]
                if fresh:
                    recheck_pairs.append((condition, fresh))
                elif condition.dimension in _CORE_CLOSEOUT_DIMENSIONS:
                    core_answers.append(gap_copy(condition, referenced))
                continue
            if referenced:
                recheck_pairs.append((condition, referenced))
                continue
            if condition.dimension in _CORE_CLOSEOUT_DIMENSIONS:
                core_answers.append(gap_copy(condition, referenced))
        missed_question = note_missed_core_revenue()
        if missed_question is not None:
            core_answers = [
                item
                for item in core_answers
                if item.condition_id != missed_question.condition_id
            ]
            core_answers.append(missed_question)
        if reused_receipts and not _period_revenue_verified(claims, checks):
            read_for_missed = [
                page
                for page in sources
                if by_page.get(int(page["page"]), {}).get("status") == "read"
            ]
            if not missed_core_revenue_facts(read_for_missed, conditions, claims):
                statement_pages = []
            else:
                statement_pages = operating_revenue_statement_pages(sources)
            evidence = []
            for page_no in statement_pages:
                source = next(
                    (item for item in sources if int(item["page"]) == page_no),
                    None,
                )
                if source is None:
                    continue
                quote = next(
                    (
                        line.strip()
                        for line in str(source.get("text") or "").splitlines()
                        if "营业收入" in line and "分产品" not in line and line.strip()
                    ),
                    "",
                )
                if not quote:
                    continue
                evidence.append(
                    Evidence(
                        evidence_id=f"statement-page-{page_no}",
                        report=report,
                        page=page_no,
                        section_title="利润表",
                        anchor=TextAnchor(bounded_quote=quote[:180]),
                    )
                )
            if not evidence and statement_pages:
                for claim in claims:
                    if claim.dimension == "revenue_model" and claim.status == "observed" and claim.evidence:
                        evidence.append(claim.evidence[0])
                        break
            existing_question = next(
                (
                    condition
                    for condition in conditions
                    if condition.condition_id.startswith("core-question:period-operating-revenue")
                ),
                None,
            )
            question_passed = False
            if existing_question is not None:
                for check in checks:
                    if (
                        check.target_type == "condition"
                        and check.target_id == existing_question.condition_id
                        and check.status == "pass"
                    ):
                        question_passed = True
            verified_revenue = [
                claim
                for claim in claims
                if claim.dimension == "revenue_model"
                and claim.status == "observed"
                and claim.limitation
                not in {
                    "pending_verification",
                    "claim_or_dependency_not_verified",
                    "evidence_not_located",
                    "retracted_pending_correction",
                    "subject_scope_omitted",
                }
                and any(
                    check.target_type == "claim"
                    and check.target_id == claim.claim_id
                    and check.status == "pass"
                    for check in checks
                )
            ]
            total_already_verified = question_passed or any(
                "period-operating-revenue" in str(item)
                for claim in verified_revenue
                for item in (claim.condition_ids or ())
            )
            if existing_question is not None and not question_passed and verified_revenue:
                recheck_pairs.append((existing_question, verified_revenue))
            elif (
                evidence
                and not question_passed
                and existing_question is None
                and not total_already_verified
            ):
                question = ReportSourceCondition(
                    condition_id="core-question:period-operating-revenue",
                    dimension="revenue_model",
                    description=(
                        "State the reporting entity's current-period operating revenue "
                        "total when the supplied text states it, using only the line, "
                        "period and unit printed there. A principal-business, product-line, "
                        "region, or sales-channel subtotal does not replace that total. "
                        "Do not add a statement name the text does not use. If the text "
                        "does not state the total, say so."
                    ),
                    evidence=tuple(evidence),
                    major=True,
                    statement_status="current",
                )
                if existing_question is None:
                    conditions.append(question)
                else:
                    conditions[:] = [
                        question if item.condition_id == existing_question.condition_id else item
                        for item in conditions
                    ]
                core_answers = [
                    item
                    for item in core_answers
                    if item.condition_id != question.condition_id
                ]
                core_answers.append(question)
        question_items = [
            condition
            for condition in core_answers
            if condition.condition_id.startswith("core-question:")
            or str(condition.condition_id).split(":")[-1]
            in {"operating-role", "period-operating-revenue"}
        ]
        other_items = [
            condition for condition in core_answers if condition not in question_items
        ]
        grouped = [
            condition
            for group in _business_groups(other_items)
            for condition in group
        ]
        conditions_to_draft = [*grouped, *question_items]
        pending_coverage.extend(recheck_pairs)
        if identity.get("read_remaining_sources"):
            conditions_to_draft = [
                condition
                for condition in conditions
                if condition.condition_id not in drafted_condition_ids
                and condition.major
                and condition.statement_status == "current"
                and condition.dimension in _CORE_CLOSEOUT_DIMENSIONS
            ]
        draft_reused_sources()
        if pending_coverage:
            leftover = list(pending_coverage)
            pending_coverage.clear()
            grouped_pairs: dict[tuple[str, ...], list] = {}
            grouped_claims: dict[tuple[str, ...], list] = {}
            for condition, chosen in leftover:
                key = tuple(claim.claim_id for claim in chosen)
                grouped_pairs.setdefault(key, []).append(condition)
                grouped_claims[key] = chosen
            for key, paired in grouped_pairs.items():
                settle_claims(grouped_claims[key], paired)
                publish()
        this_entry_unclear = set()
        for item in receipts:
            if not item.get("physical_request") or item.get("purpose") != "verify_views":
                continue
            for raw_check in (item.get("response_payload") or {}).get("checks") or []:
                if (
                    raw_check.get("target_type") == "condition"
                    and raw_check.get("status") in {"unclear", "block"}
                ):
                    this_entry_unclear.add(str(raw_check.get("target_id") or ""))
        more = []
        for condition in list(conditions):
            if condition.condition_id in drafted_condition_ids:
                continue
            if condition.dimension not in _CORE_CLOSEOUT_DIMENSIONS:
                continue
            if condition.condition_id not in this_entry_unclear and not any(
                _condition_id_names(
                    item,
                    condition.condition_id,
                    tail_unique=sum(
                        1
                        for other in this_entry_unclear
                        if other.split(":")[-1] == condition.condition_id.split(":")[-1]
                    )
                    == 1,
                )
                for item in this_entry_unclear
            ):
                continue
            latest = None
            for check in checks:
                if check.target_type == "condition" and (
                    check.target_id == condition.condition_id
                    or _condition_id_names(
                        check.target_id, condition.condition_id, tail_unique=True
                    )
                ):
                    latest = check
            if latest is None or latest.status not in {"unclear", "block"}:
                continue
            referenced = trusted_for(condition)
            if any(
                claim.claim_id not in shown_claim_ids(condition) for claim in referenced
            ):
                continue
            more.append(gap_copy(condition, referenced))
        if more:
            conditions_to_draft = more
            draft_reused_sources()
        if prior:
            fresh_ids = {item.claim_id for item in claims}
            still_pending = [
                item.model_copy(update={"limitation": None})
                for item in pending_core
                if item.claim_id in fresh_ids
            ]
            if still_pending:
                related_conditions = [
                    item
                    for item in conditions
                    if not item.condition_id.startswith("correction:")
                    and claims_covering_condition(item, still_pending)
                ]
                if related_conditions:
                    settle_claims(still_pending, related_conditions)
    else:
        readable_order = [
            int(page["page"])
            for page in sources
            if page.get("readable", True) and str(page.get("text") or "").strip()
        ]
        readable_pages = [
            page for page in sources if int(page["page"]) in set(readable_order)
        ]
        planned = []
        if not identity.get("chapter_select"):
            try:
                planned = list(
                    source_discovery_slices(readable_pages, report, identity, limit)
                )
            except ValueError as exc:
                gaps.append(str(exc))
                planned = []
        if planned:
            slice_units = [
                _units(
                    batch,
                    purpose="source_inventory",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=_source_output(identity),
                )
                for batch in planned
            ]
            sample = planned[0]
            tail_hold = (
                _units(
                    sample,
                    purpose="synthesize_views",
                    source_scope="full_context",
                    report=report,
                    identity=identity,
                    output=_body_output(identity),
                )
                + _nonempty_verify_units(sample, report=report, identity=identity)
                + 2 * max(slice_units)
            )
        if identity.get("chapter_select"):
            candidates = chapter_candidate_pages(sources)
            candidate_pages = [
                page
                for page in sources
                if int(page["page"]) in set(candidates)
            ]
            selected, _deferred = priority_order(candidates)
            if not selected:
                gaps.append("chapter_selection_empty")
            chapter_required_pages = {int(page) for page in selected}
            for page_no in candidates:
                if page_no in by_page:
                    by_page[page_no]["map_checked"] = True
                    by_page[page_no]["structure_indexed"] = True
            hold_pages = [
                page
                for page in sources
                if int(page["page"]) in (chapter_required_pages or set(candidates))
            ]
            if hold_pages:
                try:
                    hold_slices = list(
                        source_discovery_slices(
                            hold_pages,
                            report,
                            identity,
                            min(int(limit), RESERVATION_LIMIT),
                        )
                    )
                except ValueError:
                    hold_slices = [hold_pages[:1]]
                first = hold_slices[0] if hold_slices else hold_pages[:1]
                windowed = [
                    {
                        "page": int(page["page"]),
                        "text": " ".join(str(page.get("text") or "").split())[:700],
                        "readable": True,
                    }
                    for page in first[:2]
                ]
                source_later = _compose_tail(
                    windowed,
                    report=report,
                    identity=identity,
                )
            elif candidate_pages:
                source_later = _first_delivery_tail(
                    candidate_pages, report=report, identity=identity, limit=limit
                )
            follow = inventory_pages(selected)
            expand = [
                int(page)
                for page in follow
                if int(page) not in chapter_required_pages and int(page) in set(readable_order)
            ]
            if expand and not any(
                gap.startswith("direct_core_ledger_hold") for gap in gaps
            ):
                gaps.append("chapter_gap_expansion")
                chapter_required_pages.update(expand)
                inventory_pages(expand)
            tail_hold = 0
            note_missed_core_revenue()
            compose([*selected, *expand])
        else:
            follow = inventory_pages(readable_order, prepared=planned)
            tail_hold = 0
            note_missed_core_revenue()
            compose(readable_order)
            if (
                follow
                and not any(gap.startswith("direct_core_ledger_hold") for gap in gaps)
                and any(
                    gap.startswith(("core_dimension_missing:", "view_not_substantive:"))
                    for gap in _completeness_gaps(claims, conditions, checks, records)
                )
            ):
                inventory_pages(follow)
                compose(follow)
    read_pages = {int(item["page"]) for item in coverage if item["status"] == "read"}
    for page in sources:
        page_no = int(page["page"])
        if page_no in read_pages:
            continue
        if not page.get("readable", True) or not str(page.get("text") or "").strip():
            continue
        if chapter_required_pages is not None and page_no not in chapter_required_pages:
            continue
        gap = f"necessary_unread:{page_no}"
        if gap not in gaps:
            gaps.append(gap)
    for page_no in sorted(supplementary - {int(item["page"]) for item in coverage if item["status"] == "read"}):
        gap = f"supplementary_unread:{page_no}"
        if gap not in gaps:
            gaps.append(gap)
    if checks:
        receipts.append(
            {
                "request_id": f"{work_id}:carried-checks",
                "purpose": "verify_views",
                "status": "success",
                "physical_request": False,
                "carried_verification": True,
                "source_pages": [],
                "response_payload": {
                    "request_id": f"{work_id}:carried-checks",
                    "purpose": "verify_views",
                    "checks": [item.model_dump(mode="json") for item in checks],
                },
            }
        )
    for gap in _completeness_gaps(claims, conditions, checks, records):
        if gap not in gaps:
            gaps.append(gap)
    return _result()
