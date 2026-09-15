"""Form the all-industry three-dimension skeleton from existing core owners.

This module does not add ChapterTask values or manufacturing-only required
fields. Quantity and material tasks stay enhancements and activate only when
the source owns that disclosure.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .contracts import CompanyProfileTaskResult
from .core_assessment_projection import (
    COMMON_CORE_MAPPING_VERSION,
    CompanyProfileCoreAssessment,
    project_core_assessment,
)
from .core_evidence_selection import (
    CORE_SEGMENT_HEADINGS,
    CoreEvidenceSelection,
    ReportPageText,
    owned_section_heading,
    select_core_evidence,
)
from .models import (
    PRODUCTION_AUTHORIZATION,
    ChapterTask,
    ReportIdentity,
)

COMMON_CORE_CHAPTERS = (
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
)
_QUANTITY_HEADINGS = (
    "主要产品的产销量情况",
    "主要产品产销量情况",
    "产销量情况分析表",
    "公司实物销售收入是否大于劳务收入",
)
_QUANTITY_NEGATED = re.compile(
    r"公司实物销售收入是否大于劳务收入.{0,80}(?:[√☑])?(?:否|不适用)"
)
_SLICE_DISABLED_CHAPTERS = (
    ChapterTask.EXTRACT_MATERIAL_INPUTS,
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
    ChapterTask.EXTRACT_BUSINESS_REGIME,
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ActivatedChapter(_StrictModel):
    chapter_task: ChapterTask
    role: Literal["common_core", "enhancement"]
    status: Literal["activated", "not_applicable", "not_activated"]
    reason: Literal[
        "common_core_overview",
        "owned_heading",
        "no_owned_heading",
        "explicit_negation",
        "not_enabled_in_this_slice",
    ]


class CoreSkeletonResult(_StrictModel):
    mapping_version: Literal["company_profile_common_core_mapping.v1"] = (
        COMMON_CORE_MAPPING_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    report: ReportIdentity
    evidence: CoreEvidenceSelection
    assessment: CompanyProfileCoreAssessment
    activated_chapters: tuple[ActivatedChapter, ...]


def form_core_skeleton(
    *,
    report: ReportIdentity,
    pages: Sequence[ReportPageText | Mapping[str, Any]],
    task_results: Sequence[CompanyProfileTaskResult],
) -> CoreSkeletonResult:
    """Select common-core Evidence, assess the three dimensions, and name tasks."""

    accepted = tuple(
        record
        for result in task_results
        for record in result.accepted_records()
    )
    evidence = select_core_evidence(
        report=report,
        pages=pages,
        accepted_records=accepted,
    )
    return CoreSkeletonResult(
        report=report,
        evidence=evidence,
        assessment=project_core_assessment(
            report=report,
            task_results=task_results,
        ),
        activated_chapters=select_activated_chapters(pages),
    )


def select_activated_chapters(
    pages: Sequence[ReportPageText | Mapping[str, Any]],
) -> tuple[ActivatedChapter, ...]:
    """Return which existing chapter tasks the source actually supports."""

    texts = tuple(
        item.text if isinstance(item, ReportPageText) else str(item.get("text") or "")
        for item in pages
    )
    has_segment = any(
        owned_section_heading(text, CORE_SEGMENT_HEADINGS) for text in texts
    )
    quantity_negated = any(
        _QUANTITY_NEGATED.search(re.sub(r"\s+", "", text)) for text in texts
    )
    has_quantity = any(
        owned_section_heading(text, _QUANTITY_HEADINGS) for text in texts
    )
    activated: list[ActivatedChapter] = []
    for chapter in ChapterTask:
        activated.append(
            _activate_chapter(
                chapter,
                has_segment=has_segment,
                has_quantity=has_quantity,
                quantity_negated=quantity_negated,
            )
        )
    return tuple(activated)


def _activate_chapter(
    chapter: ChapterTask,
    *,
    has_segment: bool,
    has_quantity: bool,
    quantity_negated: bool,
) -> ActivatedChapter:
    if chapter in _SLICE_DISABLED_CHAPTERS:
        return ActivatedChapter(
            chapter_task=chapter,
            role="enhancement",
            status="not_activated",
            reason="not_enabled_in_this_slice",
        )
    if chapter is ChapterTask.EXTRACT_BUSINESS_OVERVIEW:
        return ActivatedChapter(
            chapter_task=chapter,
            role="common_core",
            status="activated",
            reason="common_core_overview",
        )
    if chapter is ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
        return ActivatedChapter(
            chapter_task=chapter,
            role="common_core",
            status="activated" if has_segment else "not_applicable",
            reason="owned_heading" if has_segment else "no_owned_heading",
        )
    if quantity_negated:
        status: Literal["activated", "not_applicable"] = "not_applicable"
        reason: Literal["owned_heading", "no_owned_heading", "explicit_negation"] = (
            "explicit_negation"
        )
    elif has_quantity:
        status = "activated"
        reason = "owned_heading"
    else:
        status = "not_applicable"
        reason = "no_owned_heading"
    return ActivatedChapter(
        chapter_task=chapter,
        role="enhancement",
        status=status,
        reason=reason,
    )
