"""Locate the bounded management-discussion chapter in an annual-report PDF."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


OUTLINE_SCHEMA_VERSION = "business_profile_report_outline.v1"
MANAGEMENT_DISCUSSION_TITLES = (
    "管理层讨论与分析",
    "管理层讨论和分析",
    "经营情况讨论与分析",
    "经营情况讨论和分析",
    "经营分析",
)
_MAJOR_HEADING_RE = re.compile(
    r"^\s*第(?P<number>[一二三四五六七八九十百]+)节\s*(?P<title>[^\n]{1,80})\s*$"
)
_TOC_ENTRY_RE = re.compile(
    r"第(?P<number>[一二三四五六七八九十百]+)节\s*"
    r"(?P<title>.*?)(?P<page>\d{1,4})\s*$"
)
_DOT_NOISE_RE = re.compile(r"[.。·…_\-—]+")


@dataclass(frozen=True)
class BusinessProfileReportOutline:
    """The page scope and confidence used by chapter-aware selection."""

    start_page: int
    end_page: int
    source: str
    confidence: str
    chapter_title: str
    diagnostics: tuple[str, ...] = ()

    @property
    def page_numbers(self) -> tuple[int, ...]:
        return tuple(range(self.start_page, self.end_page + 1))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OUTLINE_SCHEMA_VERSION,
            "start_page": self.start_page,
            "end_page": self.end_page,
            "source": self.source,
            "confidence": self.confidence,
            "chapter_title": self.chapter_title,
            "diagnostics": list(self.diagnostics),
        }


def locate_business_profile_outline(artifact: Mapping[str, Any]) -> BusinessProfileReportOutline:
    """Resolve the management-discussion page range from TOC or headings."""

    pages = _pages(artifact)
    page_numbers = [int(item["page_number"]) for item in pages]
    if not page_numbers:
        return _unavailable_outline()
    page_set = set(page_numbers)
    page_count = max(page_numbers)
    toc_entries = _toc_entries(pages)
    management = _find_management_full_entry(toc_entries)
    if management is not None:
        section_number, title, start_printed = management
        following = next(
            (entry for entry in toc_entries if entry[0] > section_number),
            None,
        )
        heading_entries = _heading_entries(pages)
        actual_start = next(
            (entry[2] for entry in heading_entries if entry[0] == section_number),
            None,
        )
        actual_end_boundary = (
            next(
                (entry[2] for entry in heading_entries if following and entry[0] == following[0]),
                None,
            )
            if following
            else None
        )
        if actual_start is not None:
            return BusinessProfileReportOutline(
                start_page=actual_start,
                end_page=min((actual_end_boundary or page_count + 1) - 1, page_count),
                source="table_of_contents",
                confidence="high" if actual_end_boundary else "medium",
                chapter_title=title,
                diagnostics=("toc_entry_mapped_to_major_headings",),
            )
        next_pages = [following[2]] if following is not None else []
        end_printed = min(next_pages, default=page_count + 1) - 1
        if start_printed in page_set and end_printed >= start_printed:
            return BusinessProfileReportOutline(
                start_page=start_printed,
                end_page=min(end_printed, page_count),
                source="table_of_contents",
                confidence="medium",
                chapter_title=title,
                diagnostics=("toc_printed_page_assumed_without_heading",),
            )

    heading_entries = _heading_entries(pages)
    management_heading = _find_management_entry(heading_entries)
    if management_heading is not None:
        start_page, title = management_heading
        next_pages = [
            page for _number, _title, page in heading_entries if page > start_page
        ]
        end_page = min(next_pages, default=page_count + 1) - 1
        if start_page in page_set and end_page >= start_page:
            return BusinessProfileReportOutline(
                start_page=start_page,
                end_page=min(end_page, page_count),
                source="major_heading_fallback",
                confidence="medium",
                chapter_title=title,
                diagnostics=("toc_unavailable_or_unmapped",),
            )

    return BusinessProfileReportOutline(
        start_page=min(page_numbers),
        end_page=page_count,
        source="bounded_full_document_fallback",
        confidence="low",
        chapter_title="",
        diagnostics=("management_discussion_boundary_unavailable",),
    )


def _pages(artifact: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = (
        artifact.get("pages")
        if isinstance(artifact, Mapping)
        else getattr(artifact, "pages", ())
    )
    return sorted(
        (
            item.to_dict() if hasattr(item, "to_dict") else dict(item)
            for item in values or []
        ),
        key=lambda item: int(item.get("page_number") or 0),
    )


def _toc_entries(pages: Sequence[Mapping[str, Any]]) -> list[tuple[int, str, int]]:
    entries: list[tuple[int, str, int]] = []
    for page in pages[: min(12, len(pages))]:
        text = str(page.get("text") or "")
        if "目录" not in text:
            continue
        for line in text.splitlines():
            match = _TOC_ENTRY_RE.search(line.strip())
            if not match:
                continue
            title = _clean_title(match.group("title"))
            if title:
                entries.append(
                    (
                        _chinese_section_number(match.group("number")),
                        title,
                        int(match.group("page")),
                    )
                )
    return _dedupe_entries(entries)


def _heading_entries(pages: Sequence[Mapping[str, Any]]) -> list[tuple[int, str, int]]:
    entries: list[tuple[int, str, int]] = []
    for page in pages:
        page_number = int(page.get("page_number") or 0)
        text = str(page.get("text") or "")
        if "目录" in text[:200]:
            continue
        for line in text.splitlines():
            match = _MAJOR_HEADING_RE.match(line)
            if match is None:
                continue
            title = _clean_title(match.group("title"))
            if title:
                entries.append(
                    (
                        _chinese_section_number(match.group("number")),
                        title,
                        page_number,
                    )
                )
    return _dedupe_entries(entries)


def _find_management_entry(
    entries: Iterable[tuple[int, str, int]],
) -> tuple[int, str] | None:
    for _number, title, page in entries:
        normalized = re.sub(r"\s+", "", title)
        if any(candidate in normalized for candidate in MANAGEMENT_DISCUSSION_TITLES):
            return page, title
    return None


def _find_management_full_entry(
    entries: Iterable[tuple[int, str, int]],
) -> tuple[int, str, int] | None:
    for number, title, page in entries:
        normalized = re.sub(r"\s+", "", title)
        if any(candidate in normalized for candidate in MANAGEMENT_DISCUSSION_TITLES):
            return number, title, page
    return None


def _dedupe_entries(
    entries: Iterable[tuple[int, str, int]],
) -> list[tuple[int, str, int]]:
    seen: set[tuple[int, int]] = set()
    output: list[tuple[int, str, int]] = []
    for entry in sorted(entries, key=lambda item: (item[2], item[0])):
        key = (entry[0], entry[2])
        if key not in seen:
            seen.add(key)
            output.append(entry)
    return output


def _clean_title(value: str) -> str:
    return _DOT_NOISE_RE.sub(" ", str(value or "")).strip(" :：\t")


def _chinese_section_number(value: str) -> int:
    digits = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
    if value == "十":
        return 10
    if "十" in value:
        left, _, right = value.partition("十")
        return (digits.get(left, 1) * 10 if left else 10) + digits.get(right, 0)
    if "百" in value:
        left, _, right = value.partition("百")
        return digits.get(left, 1) * 100 + digits.get(right, 0)
    return digits.get(value, 0)


def _unavailable_outline() -> BusinessProfileReportOutline:
    return BusinessProfileReportOutline(
        start_page=1,
        end_page=1,
        source="unavailable",
        confidence="low",
        chapter_title="",
        diagnostics=("empty_pdf_artifact",),
    )
