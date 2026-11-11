"""Report-local section boundaries and concentrated source packets for v9.

The index recognizes typography/numbering, not business meanings. The model
selects sections by the questions; physical pages only locate the source.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import Field

from .models import ReportIdentity
from .workpaper import (
    WorkpaperEvidence,
    WorkpaperReadingScope,
    WorkpaperSection,
    WorkpaperSpan,
    _Model,
)

_CN = "一二三四五六七八九十百零〇"
_HEADING = re.compile(
    rf"^(第[{_CN}0-9]+[章节]\s*.+|[{_CN}]+[、．.]\s*.+|"
    rf"[（(][{_CN}]+[）)]\s*.+|[0-9]{{1,3}}[、．.]\s*[^0-9].+|"
    r"[（(][0-9]{1,3}[）)][.．]?\s*.+)$"
)


class IndexedSection(_Model):
    section_id: str
    section_path: tuple[str, ...]
    start_page: int
    start_offset: int
    end_page: int
    end_offset: int
    characters: int
    level: int
    next_title: str | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "section_id": self.section_id,
            "section_path": list(self.section_path),
            "physical_pages": [self.start_page, self.end_page],
            "characters": self.characters,
        }


class SectionSelection(_Model):
    selected_section_ids: tuple[str, ...] = Field(min_length=1)
    rationale: str = Field(min_length=1)
    deferred: tuple[str, ...] = ()
    stop_reason: str = Field(min_length=1)


class SectionPacket(_Model):
    report: ReportIdentity
    reading_scope: WorkpaperReadingScope
    evidence: tuple[WorkpaperEvidence, ...]
    transcription_version: str = "source_page_text.v1"
    limitations: tuple[str, ...] = ()

    def model_text(self) -> str:
        out = [
            "以下仅为本报告选定章节原文，不是候选答案。",
            "表格以等宽文本保留布局；相邻原页片段可能属于同一续表，金额须同时引用表头/单位所在片段。",
        ]
        for evidence in self.evidence:
            out.extend([
                "",
                f"[[{evidence.evidence_id}]] {' / '.join(evidence.section_path)}",
            ])
            for span in evidence.source_spans:
                out.extend([f"物理页 {span.page}", "```text", span.quote, "```"])
        return "\n".join(out)


def _level(title: str) -> int:
    if title.startswith("第"):
        return 0
    if re.match(rf"^[{_CN}]+[、．.]", title):
        return 1
    if re.match(rf"^[（(][{_CN}]+[）)]", title):
        return 2
    if re.match(r"^[0-9]", title):
        return 3
    return 4


def _source_pages(pages: Sequence[Mapping]) -> dict[int, str]:
    result = {}
    for page in pages:
        number = int(page.get("page", page.get("page_number")))
        if number < 1 or number in result:
            raise ValueError("source pages must have unique positive identities")
        result[number] = str(page.get("text") or "")
    return dict(sorted(result.items()))


def index_sections(pages: Sequence[Mapping]) -> tuple[IndexedSection, ...]:
    """Index real headings; directory entries locate structure, not answers."""
    by_page = _source_pages(pages)
    headings: list[dict[str, Any]] = []
    stack: list[dict[str, Any]] = []
    for page, text in by_page.items():
        # A TOC is navigation metadata, never a second definition of its chapters.
        if ("目录" in text or "目 录" in text) and len(re.findall(r"\.{3,}|…{2,}", text)) >= 2:
            continue
        offset = 0
        for line in text.splitlines(keepends=True):
            stripped = line.strip()
            match = _HEADING.match(stripped)
            if match and len(stripped) <= 150 and not re.search(r"\.{3,}|…{2,}", stripped):
                title = re.split(r"[。；]", stripped, maxsplit=1)[0].strip()
                level = _level(title)
                start = offset + len(line) - len(line.lstrip())
                while stack and stack[-1]["level"] >= level:
                    closed = stack.pop()
                    closed.update(end_page=page, end_offset=start, next_title=title)
                path = tuple(item["title"] for item in stack) + (title,)
                entry = {
                    "section_id": f"section-{len(headings) + 1:04d}",
                    "section_path": path,
                    "start_page": page,
                    "start_offset": start,
                    "level": level,
                    "title": title,
                }
                headings.append(entry)
                stack.append(entry)
            offset += len(line)
    if not headings:
        raise ValueError("report section headings are unavailable; no full-report fallback")
    last_page = max(by_page)
    for entry in stack:
        entry.update(end_page=last_page, end_offset=len(by_page[last_page]), next_title=None)
    output = []
    for item in headings:
        length = sum(
            len(text[
                item["start_offset"] if page == item["start_page"] else 0:
                item["end_offset"] if page == item["end_page"] else len(text)
            ])
            for page, text in by_page.items()
            if item["start_page"] <= page <= item["end_page"]
        )
        output.append(IndexedSection(**{key: value for key, value in item.items() if key != "title"}, characters=length))
    return tuple(output)


def _position(text: str, title: str) -> int | None:
    chars, positions = [], []
    for offset, char in enumerate(text):
        if not char.isspace():
            chars.append(char)
            positions.append(offset)
    at = "".join(chars).find(re.sub(r"\s+", "", title))
    return positions[at] if at >= 0 else None


def selected_pages(
    sections: Sequence[IndexedSection], selection: SectionSelection
) -> tuple[int, ...]:
    by_id = {item.section_id: item for item in sections}
    unknown = set(selection.selected_section_ids) - by_id.keys()
    if unknown:
        raise ValueError(f"selection cites unknown section IDs: {sorted(unknown)}")
    return tuple(sorted({
        page for sid in selection.selected_section_ids
        for page in range(by_id[sid].start_page, by_id[sid].end_page + 1)
    }))


def build_section_packet(
    report: ReportIdentity,
    pages: Sequence[Mapping],
    sections: Sequence[IndexedSection],
    selection: SectionSelection,
    *,
    remap_layout: bool = False,
    max_characters: int = 160_000,
) -> SectionPacket:
    by_page = _source_pages(pages)
    by_id = {item.section_id: item for item in sections}
    selected_pages(sections, selection)
    chosen = sorted((by_id[sid] for sid in set(selection.selected_section_ids)), key=lambda e: (e.start_page, e.start_offset, -e.characters))
    # An ancestor already contains its child; don't send its text twice.
    chosen = [item for index, item in enumerate(chosen) if not any(
        (other.start_page, other.start_offset) <= (item.start_page, item.start_offset)
        and (other.end_page, other.end_offset) >= (item.end_page, item.end_offset)
        for other in chosen[:index]
    )]
    evidence, limitations = [], []
    for section in chosen:
        for page in range(section.start_page, section.end_page + 1):
            text = by_page.get(page)
            if text is None:
                raise ValueError(f"selected section page was not supplied: {page}")
            start = section.start_offset if page == section.start_page else 0
            end = section.end_offset if page == section.end_page else len(text)
            if remap_layout:
                start = (_position(text, section.section_path[-1]) if page == section.start_page else 0)
                end = (_position(text, section.next_title) if page == section.end_page and section.next_title else len(text))
                if start is None or end is None:
                    raise ValueError(f"layout section boundary cannot be located: {section.section_id}:{page}")
            if end <= start or not text[start:end].strip():
                continue
            quote = text[start:end]
            evidence.append(WorkpaperEvidence(
                evidence_id=f"source-{len(evidence) + 1:04d}",
                document_version=report.document_version,
                section_path=section.section_path,
                physical_pages=(page,),
                source_spans=(WorkpaperSpan(page=page, start=start, end=end, quote=quote),),
                table_context="相邻片段按原页顺序保留；续表的表头、单位与数据须共同引用。",
            ))
    if not evidence:
        raise ValueError("selected sections contain no readable source text")
    if sum(len(span.quote) for item in evidence for span in item.source_spans) > max_characters:
        raise ValueError("selected sections are too broad; select smaller complete subsections")
    return SectionPacket(
        report=report,
        reading_scope=WorkpaperReadingScope(
            sections=tuple(WorkpaperSection(
                section_id=item.section_id, section_path=item.section_path,
                start_page=item.start_page, end_page=item.end_page,
                rationale=selection.rationale,
            ) for item in chosen),
            unread_or_deferred=selection.deferred,
            selection_rationale=selection.rationale,
            stop_reason=selection.stop_reason,
            transcription_version="shared-pdf-pypdf-layout.v1" if remap_layout else "source_page_text.v1",
        ),
        evidence=tuple(evidence),
        transcription_version="shared-pdf-pypdf-layout.v1" if remap_layout else "source_page_text.v1",
        limitations=tuple(limitations),
    )


def extract_selected_layout(content: bytes, page_numbers: Sequence[int]) -> tuple[dict, ...]:
    """Use the shared native PDF owner in opt-in fixed-width layout mode."""
    from research.document_processing.pdf import PypdfNativeAdapter, detect_text_quality

    result = PypdfNativeAdapter(extraction_mode="layout").extract(content, target_pages=page_numbers)
    requested = set(page_numbers)
    if set(page.page_number for page in result.pages) != requested:
        raise ValueError("native layout extraction did not return every selected page")
    pages = []
    for page in result.pages:
        quality, _diagnostics = detect_text_quality(page.text)
        # A native chapter divider can legitimately contain only its heading.
        # Keep that text and its coordinates: low character count is not a
        # decoding failure, and must not discard the following business pages.
        if quality not in {"usable", "low_text"} or page.diagnostics:
            raise ValueError(f"selected page has unreadable native layout: {page.page_number}")
        pages.append({
            "page": page.page_number, "text": page.text, "readable": True,
            "text_hash": hashlib.sha256(page.text.encode()).hexdigest(),
            "extraction_method": "shared-pdf-pypdf-layout.v1",
            "layout_quality": quality,
        })
    return tuple(pages)
