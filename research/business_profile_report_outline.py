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
    r"^\s*第(?P<number>[一二三四五六七八九十百]+)(?P<kind>章|节)\s*(?P<title>[^\n]{1,80})\s*$"
)
_TOC_ENTRY_RE = re.compile(
    r"第(?P<number>[一二三四五六七八九十百]+)(?:章|节)\s*"
    r"(?P<title>.*?)(?P<page>\d{1,4})\s*$"
)
_TOC_ENTRY_LEADING_PAGE_RE = re.compile(
    r"^(?P<page>\d{1,4})\s+第(?P<number>[一二三四五六七八九十百]+)(?:章|节)\s*"
    r"(?P<title>[^\n]{1,100})\s*$"
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


@dataclass(frozen=True)
class BusinessProfileRecoveryDecision:
    """Bounded business-layer decision before semantic processing."""

    state: str
    outline: BusinessProfileReportOutline
    toc_probe_pages: tuple[int, ...] = ()
    section_pages: tuple[int, ...] = ()
    diagnostics: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "outline": self.outline.to_dict(),
            "toc_probe_pages": list(self.toc_probe_pages),
            "section_pages": list(self.section_pages),
            "diagnostics": list(self.diagnostics),
        }


def locate_business_profile_outline(
    artifact: Mapping[str, Any],
    *,
    allow_full_document_fallback: bool = True,
) -> BusinessProfileReportOutline:
    """Resolve the management-discussion page range from TOC or headings."""

    pages = _pages(artifact)
    page_numbers = [int(item["page_number"]) for item in pages]
    if not page_numbers:
        return _unavailable_outline()
    page_set = set(page_numbers)
    page_count = max(page_numbers)
    bookmark = _bookmark_management_entry(pages)
    if bookmark is not None:
        start_page, title = bookmark
        following = next(
            (
                int(page.get("page_number") or 0)
                for page in pages
                if int(page.get("page_number") or 0) > start_page
                and str(page.get("bookmark_title") or "").strip()
                and not any(
                    candidate
                    in re.sub(r"\s+", "", str(page.get("bookmark_title") or ""))
                    for candidate in MANAGEMENT_DISCUSSION_TITLES
                )
            ),
            page_count + 1,
        )
        return BusinessProfileReportOutline(
            start_page=start_page,
            end_page=min(page_count, following - 1),
            source="bookmark",
            confidence="high",
            chapter_title=title,
            diagnostics=("bookmark_management_heading",),
        )
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

    if not allow_full_document_fallback:
        return _unresolved_outline(page_count)
    return BusinessProfileReportOutline(
        start_page=min(page_numbers),
        end_page=page_count,
        source="bounded_full_document_fallback",
        confidence="low",
        chapter_title="",
        diagnostics=("management_discussion_boundary_unavailable",),
    )


def assess_business_profile_recovery(
    artifact: Mapping[str, Any],
    *,
    toc_probe_max_pages: int = 5,
    section_max_pages: int = 20,
) -> BusinessProfileRecoveryDecision:
    """Classify a report and return only bounded pages eligible for recovery."""
    pages = _pages(artifact)
    page_numbers = tuple(int(item["page_number"]) for item in pages)
    if not page_numbers:
        return BusinessProfileRecoveryDecision(
            "source_unrecoverable", _unavailable_outline(), diagnostics=("empty_pdf_artifact",)
        )
    outline = locate_business_profile_outline(
        artifact, allow_full_document_fallback=False
    )
    probe_pages = tuple(page_numbers[: max(1, int(toc_probe_max_pages))])
    if outline.source == "unavailable":
        return BusinessProfileRecoveryDecision(
            "source_unrecoverable", outline, toc_probe_pages=probe_pages,
            diagnostics=outline.diagnostics,
        )
    if outline.source == "toc_unresolved":
        return BusinessProfileRecoveryDecision(
            "toc_probe_required", outline, toc_probe_pages=probe_pages,
            diagnostics=("bounded_toc_probe_required",),
        )
    by_number = {int(page["page_number"]): page for page in pages}
    scope = tuple(
        number for number in range(outline.start_page, outline.end_page + 1)
        if number in by_number
    )
    unusable = tuple(
        number for number in scope
        if not _usable_page(by_number[number])
    )
    if unusable:
        target = unusable[: max(1, int(section_max_pages))]
        return BusinessProfileRecoveryDecision(
            "section_ocr_required", outline, section_pages=target,
            diagnostics=("business_section_has_unusable_pages",),
        )
    return BusinessProfileRecoveryDecision(
        "native_ready", outline, section_pages=scope,
        diagnostics=("business_sections_native_ready",),
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
            leading_page = _TOC_ENTRY_LEADING_PAGE_RE.search(line.strip())
            if match is None and leading_page is not None:
                match = leading_page
            if not match:
                continue
            title = _clean_title(match.group("title"))
            if title:
                entries.append(
                    (
                        _chinese_section_number(match.group("number")),
                        title,
                        int(match.groupdict().get("page") or match.groupdict().get("page")),
                    )
                )
    return _dedupe_entries(entries)


def _bookmark_management_entry(
    pages: Sequence[Mapping[str, Any]],
) -> tuple[int, str] | None:
    for page in pages:
        title = str(page.get("bookmark_title") or "").strip()
        normalized = re.sub(r"\s+", "", title)
        if title and any(candidate in normalized for candidate in MANAGEMENT_DISCUSSION_TITLES):
            return int(page.get("page_number") or 0), title
    return None


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
            title = re.sub(r"\s+\d{1,4}$", "", title).strip()
            if title:
                entries.append(
                    (
                        _chinese_section_number(match.group("number")),
                        title,
                        page_number,
                    )
                )
    # Page headers repeat the major chapter title on every page.  Keep the
    # first physical page for each chapter number so the next chapter forms a
    # real boundary rather than ending the current chapter on its first page.
    first_by_number: dict[int, tuple[int, str, int]] = {}
    for entry in sorted(entries, key=lambda item: (item[2], item[0])):
        first_by_number.setdefault(entry[0], entry)
    return list(first_by_number.values())


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


def _unresolved_outline(page_count: int) -> BusinessProfileReportOutline:
    return BusinessProfileReportOutline(
        start_page=1,
        end_page=max(1, page_count),
        source="toc_unresolved",
        confidence="low",
        chapter_title="",
        diagnostics=("management_discussion_boundary_unavailable",),
    )


def _usable_page(page: Mapping[str, Any]) -> bool:
    text = str(page.get("text") or "").strip()
    method = str(page.get("extraction_method") or "native_text")
    status = str(page.get("native_text_status") or "")
    return bool(text) and method in {"native_text", "alternate_native", "ocr"} and status not in {
        "empty", "glyph_decoding_error", "extraction_error"
    }
