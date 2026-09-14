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
    CoreEvidenceSelection,
    ReportPageText,
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
_QUANTITY_OWNING = re.compile(
    r"主要产品(?:的)?产销(?:量|存)|"
    r"生产量.{0,40}销售量.{0,40}库存量|"
    r"公司实物销售收入是否大于劳务收入"
)
_SEGMENT_OWNING = re.compile(
    r"占公司营业收入或营业利润10%以上|"
    r"主营业务分(?:行业|产品)|"
    r"分部(?:报告|信息)|"
    r"(?:^|\n)\s*分(?:产品|行业)\b"
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ActivatedChapter(_StrictModel):
    chapter_task: ChapterTask
    role: Literal["common_core", "enhancement"]
    status: Literal["activated", "not_applicable"]


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

    text = "\n".join(
        item.text if isinstance(item, ReportPageText) else str(item.get("text") or "")
        for item in pages
    )
    has_segment = bool(_SEGMENT_OWNING.search(text))
    has_quantity = bool(_QUANTITY_OWNING.search(text))
    activated: list[ActivatedChapter] = []
    for chapter in ChapterTask:
        if chapter in COMMON_CORE_CHAPTERS:
            role: Literal["common_core", "enhancement"] = "common_core"
            if chapter == ChapterTask.EXTRACT_BUSINESS_OVERVIEW:
                status: Literal["activated", "not_applicable"] = "activated"
            else:
                status = "activated" if has_segment else "not_applicable"
        else:
            role = "enhancement"
            status = (
                "activated"
                if chapter == ChapterTask.EXTRACT_OPERATING_QUANTITIES
                and has_quantity
                else "not_applicable"
            )
        activated.append(
            ActivatedChapter(chapter_task=chapter, role=role, status=status)
        )
    return tuple(activated)
