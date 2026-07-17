"""Classify official disclosures used by the company business-profile pipeline."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional


_SPACE_RE = re.compile(r"\s+")
_TAG_RE = re.compile(r"<[^>]+>")
_PERIODIC_REPORT_TITLE_RE = re.compile(
    r"(?P<year>20\d{2})(?:年年度报告|年度报告|年半年度报告|年中期报告)"
)
_FULL_REPORT_SUFFIX_RE = re.compile(
    r"^(?:全文|[（(](?:更正后|修订(?:版|稿)?|更新后|更新版|补充后|补充版|补充修订版)[）)])?$"
)


@dataclass(frozen=True)
class BusinessProfileDocumentClassification:
    """Deterministic classification for one disclosure title."""

    document_type: str
    selected: bool
    is_full_report: bool = False
    is_correction: bool = False
    selection_reasons: List[str] = field(default_factory=list)
    exclusion_reason: Optional[str] = None
    profile_event_hints: List[str] = field(default_factory=list)


def normalize_announcement_title(title: str) -> str:
    """Normalize harmless title markup without changing Chinese semantics."""
    text = _TAG_RE.sub("", str(title or ""))
    return _SPACE_RE.sub("", text).strip()


def infer_profile_change_event_hints(title: str) -> List[str]:
    """Return review-only profile-change hints from an announcement title."""
    normalized = normalize_announcement_title(title)
    hints: List[str] = []
    patterns = (
        ("reverse_merger", ("借壳", "重组上市", "重大资产置换")),
        (
            "major_asset_restructuring",
            ("重大资产重组", "发行股份购买资产", "重大资产购买"),
        ),
        (
            "business_disposal",
            ("出售重大资产", "重大资产出售", "出售主营", "剥离主营"),
        ),
        (
            "business_acquisition",
            ("重大收购", "收购控股权", "收购资产", "收购股权"),
        ),
        ("control_change", ("控制权变更", "实际控制人变更", "控股股东变更")),
        (
            "principal_business_change",
            ("主营业务变更", "变更主营业务", "新增主营业务", "拓展第二主业"),
        ),
        ("company_name_change", ("变更公司名称", "公司名称变更", "证券简称变更")),
    )
    for event_type, keywords in patterns:
        if any(keyword in normalized for keyword in keywords):
            hints.append(event_type)
    return hints


def business_profile_document_family(document_type: str) -> str:
    """Return the stable family shared by an original report and corrections."""
    normalized = str(document_type or "").strip()
    return normalized.removesuffix("_correction")


def infer_business_profile_report_period(
    title: str,
    announcement_time: Optional[str],
) -> str:
    """Infer a filing period, falling back to the publication date for events."""
    normalized = normalize_announcement_title(title)
    match = _PERIODIC_REPORT_TITLE_RE.search(normalized)
    if match and ("半年度报告" in normalized or "中期报告" in normalized):
        return f"{match.group('year')}-06-30"
    if match and "年度报告" in normalized:
        return f"{match.group('year')}-12-31"
    published = str(announcement_time or "").strip()
    if len(published) >= 10 and published[4] == "-" and published[7] == "-":
        return published[:10]
    raise ValueError("report period cannot be inferred from title or announcement_time")


def classify_business_profile_document(
    title: str,
    *,
    adjunct_type: Optional[str] = None,
) -> BusinessProfileDocumentClassification:
    """Classify source documents without making profile changes."""
    normalized = normalize_announcement_title(title)
    hints = infer_profile_change_event_hints(normalized)
    type_text = str(adjunct_type or "").strip().lower()
    if type_text and "pdf" not in type_text:
        return BusinessProfileDocumentClassification(
            document_type="unsupported_attachment",
            selected=False,
            exclusion_reason="attachment_not_pdf",
            profile_event_hints=hints,
        )

    correction = any(
        keyword in normalized
        for keyword in (
            "更正",
            "修订",
            "更新后",
            "更新版",
            "补充",
        )
    )
    correction_notice = (
        correction
        and "公告" in normalized
        and not any(
            keyword in normalized
            for keyword in (
                "更正后",
                "修订版",
                "修订稿",
                "更新后",
                "更新版",
                "补充后",
                "补充版",
                "补充修订版",
            )
        )
    )
    summary = "摘要" in normalized
    translation = any(
        keyword in normalized for keyword in ("英文版", "英文翻译", "外文版")
    )

    periodic = _classify_periodic_report(
        normalized,
        correction=correction,
        correction_notice=correction_notice,
        summary=summary,
        translation=translation,
        profile_event_hints=hints,
    )
    if periodic is not None:
        return periodic

    category_patterns = (
        (
            "operating_data",
            ("主要经营数据", "经营情况公告", "产销数据", "月度经营数据"),
        ),
        (
            "resource_report",
            ("资源储量", "资源量", "储量报告", "矿业权评估", "矿产资源"),
        ),
        (
            "major_contract",
            ("重大合同", "长期协议", "长协", "重大采购合同", "重大销售合同"),
        ),
        (
            "hedging_disclosure",
            ("套期保值", "商品衍生品", "期货和衍生品", "衍生品交易"),
        ),
        (
            "profile_change_event",
            (
                "重大资产重组",
                "发行股份购买资产",
                "重大资产置换",
                "控制权变更",
                "主营业务变更",
                "重大资产出售",
            ),
        ),
    )
    for document_type, keywords in category_patterns:
        matched = [keyword for keyword in keywords if keyword in normalized]
        if matched:
            return BusinessProfileDocumentClassification(
                document_type=document_type,
                selected=True,
                is_correction=correction,
                selection_reasons=[f"title_keyword:{keyword}" for keyword in matched],
                profile_event_hints=hints,
            )

    if hints:
        return BusinessProfileDocumentClassification(
            document_type="profile_change_event",
            selected=True,
            is_correction=correction,
            selection_reasons=[
                f"profile_event_hint:{event_type}" for event_type in hints
            ],
            profile_event_hints=hints,
        )

    return BusinessProfileDocumentClassification(
        document_type="audit_report" if "审计报告" in normalized else "other",
        selected=False,
        exclusion_reason="unsupported_document_class",
        profile_event_hints=hints,
    )


def _classify_periodic_report(
    normalized: str,
    *,
    correction: bool,
    correction_notice: bool,
    summary: bool,
    translation: bool,
    profile_event_hints: List[str],
) -> Optional[BusinessProfileDocumentClassification]:
    report_match = _PERIODIC_REPORT_TITLE_RE.search(normalized)
    if report_match is None:
        return None
    report_type: Optional[str] = None
    if "半年度报告" in normalized or "中期报告" in normalized:
        report_type = "semiannual_report"
    elif "年度报告" in normalized:
        report_type = "annual_report"
    if report_type is None:
        return None
    if correction_notice:
        return BusinessProfileDocumentClassification(
            document_type=f"{report_type}_correction_notice",
            selected=True,
            is_full_report=False,
            is_correction=True,
            selection_reasons=["official_periodic_report_correction_notice"],
            profile_event_hints=profile_event_hints,
        )
    if summary:
        return BusinessProfileDocumentClassification(
            document_type=f"{report_type}_summary",
            selected=False,
            is_correction=correction,
            exclusion_reason="summary_not_full_report",
            profile_event_hints=profile_event_hints,
        )
    if translation:
        return BusinessProfileDocumentClassification(
            document_type=f"{report_type}_translation",
            selected=False,
            is_correction=correction,
            exclusion_reason="translation_not_primary_report",
            profile_event_hints=profile_event_hints,
        )
    suffix = normalized[report_match.end() :]
    if _FULL_REPORT_SUFFIX_RE.fullmatch(suffix) is None:
        return BusinessProfileDocumentClassification(
            document_type=f"{report_type}_related",
            selected=False,
            is_correction=correction,
            exclusion_reason="periodic_report_related_not_full_report",
            profile_event_hints=profile_event_hints,
        )
    return BusinessProfileDocumentClassification(
        document_type=f"{report_type}_correction" if correction else report_type,
        selected=True,
        is_full_report=True,
        is_correction=correction,
        selection_reasons=["official_periodic_full_report"],
        profile_event_hints=profile_event_hints,
    )
