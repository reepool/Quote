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
    rf"^(\d{{1,3}}(?:\.\d{{1,3}})+\s+[^\d\s].+|第[{_CN}0-9]+[章节]\s*.+|[{_CN}]+[、．.]\s*.+|"
    rf"[（(][{_CN}]+[）)]\s*.+|[0-9]{{1,3}}[、．.]\s*[^0-9].+|"
    r"[（(][0-9]{1,3}[）)][.．]?\s*.+)$"
)
_INLINE_HEADING = re.compile(rf"[。；]\s+([{_CN}]+[、．.]\s*[^。；\r\n]{{2,80}})\s*$")
_PLAIN_HEADING = re.compile(
    r"^[^\d\W][^。；;：:，,!?！？\d]{1,37}"
    r"(?:情况|分析|概览|综述|回顾|业务|经营模式|业务模式|财务报告|公司治理|重要事项|未来展望)$"
)
_REPORT_PAGE_FOOTER = re.compile(r"(?:年度报告|年报|annual\s+report).*\d\s*$", re.IGNORECASE)


def _numeric_table_row(title: str, *, legacy: bool = False) -> bool:
    """Do not let decimal amounts/ratios terminate a real section.

    Multi-level headings such as 3.7 / 3.6.2 remain valid. Financial
    continuations can start with 000.00 or 1.47 -69.51; retain that text
    as source, not as a new heading/boundary.
    """
    if not legacy and re.match(
        r"^\d+(?:\.\d+)+\s*(?:[万亿千百]?(?:元|台|吨|只|辆|人|户)|[%％])(?:[，,。；;\s]|$)", title
    ):
        return True
    match = re.match(r"^(\d+(?:\.\d+)+)\s+(.+)$", title)
    if not match:
        return False
    return any(len(part) > 1 and part.startswith("0") for part in match[1].split(".")) or bool(
        re.match(r"^[+−\-]?\d[\d,]*\.\d+(?:%|％)?(?:\s|$)", match[2])
    ) or bool(
        re.fullmatch(r"[\d\s.,，%％+−—–\-()（）/]+", match[2])
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
            "正常页以等宽文本保留表格布局；若页内标明布局降级或未读，不能据此猜列。相邻片段的金额须共同引用表头/单位。",
        ]
        for evidence in self.evidence:
            out.extend([
                "",
                f"[[{evidence.evidence_id}]] {' / '.join(evidence.section_path)}",
            ])
            for span in evidence.source_spans:
                if evidence.table_context and ("布局未恢复" in evidence.table_context
                    or "structured-notes" in self.reading_scope.transcription_version):
                    out.append(evidence.table_context)
                out.extend([f"物理页 {span.page}", "```text", span.quote, "```"])
        return "\n".join(out)


def _level(title: str) -> int:
    decimal = re.match(r"^(\d+(?:\.\d+)+)\s", title)
    if decimal:
        return len(decimal.group(1).split(".")) - 1
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


def index_sections(
    pages: Sequence[Mapping], *, legacy_numbered_only: bool = False,
    exclude_footer_labels: bool = True,
) -> tuple[IndexedSection, ...]:
    """Index real headings; directory entries locate structure, not answers."""
    by_page = _source_pages(pages)
    footer_counts: dict[str, int] = {}
    for text in by_page.values():
        for line in text.splitlines()[-2:]:
            title = line.strip()
            footer_counts[title] = footer_counts.get(title, 0) + 1
    headings: list[dict[str, Any]] = []
    stack: list[dict[str, Any]] = []
    for page, text in by_page.items():
        # A TOC is navigation metadata, never a second definition of its chapters.
        if ("目录" in text or "目 录" in text) and len(re.findall(r"\.{3,}|…{2,}", text)) >= 2:
            continue
        offset = 0
        lines = text.splitlines(keepends=True)
        for line_number, line in enumerate(lines):
            stripped = line.strip()
            inline = _INLINE_HEADING.search(stripped)
            leading = len(line) - len(line.lstrip())
            if inline:
                # Some official native text appends the next heading to the
                # previous paragraph. Keep the original character position.
                leading += inline.start(1)
                stripped = inline.group(1).strip()
            match = _HEADING.match(stripped)
            plain = not legacy_numbered_only and bool(_PLAIN_HEADING.fullmatch(stripped))
            if plain and line_number >= len(lines) - 2 and footer_counts.get(stripped, 0) >= 3:
                plain = False  # Repeated report/page footer, not a new boundary.
            if plain and exclude_footer_labels and not any(t.strip() for t in lines[line_number + 1:]):
                previous = next((t.strip() for t in reversed(lines[:line_number]) if t.strip()), "")
                if _REPORT_PAGE_FOOTER.search(previous):
                    # Running chapter labels can follow the report's page
                    # footer even once. A real heading above it is preserved.
                    plain = False
            if (match or plain) and not _numeric_table_row(stripped, legacy=legacy_numbered_only) and len(stripped) <= 150 and not re.search(r"\.{3,}|…{2,}", stripped):
                title = re.split(r"[。；]", stripped, maxsplit=1)[0].strip()
                level = _level(title) if match else 1
                start = offset + leading
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
    skip_unlocatable_sections: bool = False,
    preserve_note_markers: bool = False,
) -> SectionPacket:
    by_page = _source_pages(pages)
    page_metadata = {int(p.get("page", p.get("page_number"))): p for p in pages}
    by_id = {item.section_id: item for item in sections}
    selected_pages(sections, selection)
    chosen = sorted((by_id[sid] for sid in set(selection.selected_section_ids)), key=lambda e: (e.start_page, e.start_offset, -e.characters))
    evidence, limitations = [], []
    if remap_layout and skip_unlocatable_sections:
        locatable = []
        for section in chosen:
            boundaries = ((section.start_page, section.section_path[-1]), (section.end_page, section.next_title))
            for page, title in boundaries:
                text = by_page.get(page)
                if (title and text is not None and page_metadata[page].get("readable") is not False
                    and _position(text, title) is None):
                    limitations.append(
                        f"章节{section.section_id}（{' / '.join(section.section_path)}）边界无法定位："
                        f"物理页{page}未找到标题“{title}”；本候选未读，不等于未披露，未猜测偏移或扩读全文。"
                    )
                    break
            else:
                locatable.append(section)
        chosen = locatable
    # An ancestor already contains its child; don't send its text twice.
    chosen = [item for index, item in enumerate(chosen) if not any(
        (other.start_page, other.start_offset) <= (item.start_page, item.start_offset)
        and (other.end_page, other.end_offset) >= (item.end_page, item.end_offset)
        for other in chosen[:index]
    )]
    for section in chosen:
        for page in range(section.start_page, section.end_page + 1):
            text = by_page.get(page)
            if text is None:
                raise ValueError(f"selected section page was not supplied: {page}")
            metadata = page_metadata[page]
            if metadata.get("readable") is False:
                limitation = str(metadata.get("layout_limitation") or f"物理页{page}原文不可读，本页未读，不等于未披露。")
                if limitation not in limitations:
                    limitations.append(limitation)
                continue
            if metadata.get("layout_limitation") and metadata["layout_limitation"] not in limitations:
                limitations.append(metadata["layout_limitation"])
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
                table_context=metadata.get("layout_limitation") or "相邻片段按原页顺序保留；续表的表头、单位与数据须共同引用。",
            ))
    if not evidence:
        raise ValueError("selected sections contain no readable source text")
    if sum(len(span.quote) for item in evidence for span in item.source_spans) > max_characters:
        raise ValueError("selected sections are too broad; select smaller complete subsections")
    transcription = (
        "shared-pdf-layout-with-native-fallback.v2" if remap_layout and limitations
        else "shared-pdf-pypdf-layout.v1" if remap_layout else "source_page_text.v1"
    )
    if remap_layout and preserve_note_markers:
        transcription = ("shared-pdf-layout-with-native-fallback.structured-notes.v3" if limitations
                         else "shared-pdf-pypdf-layout.structured-notes.v1")
    return SectionPacket(
        report=report,
        reading_scope=WorkpaperReadingScope(
            sections=tuple(WorkpaperSection(
                section_id=item.section_id, section_path=item.section_path,
                start_page=item.start_page, end_page=item.end_page,
                rationale=selection.rationale,
            ) for item in chosen),
            unread_or_deferred=(*selection.deferred, *limitations),
            selection_rationale=selection.rationale,
            stop_reason=selection.stop_reason,
            transcription_version=transcription,
        ),
        evidence=tuple(evidence),
        transcription_version=transcription,
        limitations=tuple(limitations),
    )


def extract_selected_layout(
    content: bytes, page_numbers: Sequence[int], *, native_pages: Sequence[Mapping] | None = None,
    section_bounds: Sequence[IndexedSection] = (), preserve_note_markers: bool = False,
) -> tuple[dict, ...]:
    """Use the shared native PDF owner in opt-in fixed-width layout mode."""
    from research.document_processing.pdf import PypdfNativeAdapter, detect_text_quality

    options = {"preserve_note_markers": True} if preserve_note_markers else {}
    result = PypdfNativeAdapter(extraction_mode="layout", **options).extract(content, target_pages=page_numbers)
    requested = set(page_numbers)
    if native_pages is None and set(page.page_number for page in result.pages) != requested:
        raise ValueError("native layout extraction did not return every selected page")
    fallback = _source_pages(native_pages) if native_pages is not None else {}
    by_number = {page.page_number: page for page in result.pages}
    pages = []
    for number in sorted(requested):
        page = by_number.get(number)
        text = page.text if page else ""
        quality, _diagnostics = detect_text_quality(text)
        # A page may contain plenty of readable text while layout extraction
        # loses a heading needed to remap the native index. Keep this local:
        # use the already supplied shared native text, never guessed offsets.
        native_titles = (
            line.strip() for line in fallback.get(number, "").splitlines()[:-2]
            if (_HEADING.fullmatch(line.strip()) or _PLAIN_HEADING.fullmatch(line.strip()))
            and not _numeric_table_row(line.strip())
        )
        lost_heading = any(_position(text, title) is None for title in native_titles)
        empty_remap = False
        for section in section_bounds:
            if not section.start_page <= number <= section.end_page:
                continue
            native_start = section.start_offset if number == section.start_page else 0
            native_end = section.end_offset if number == section.end_page else len(fallback.get(number, ""))
            if not fallback.get(number, "")[native_start:native_end].strip():
                continue
            start = _position(text, section.section_path[-1]) if number == section.start_page else 0
            end = (_position(text, section.next_title)
                   if number == section.end_page and section.next_title else len(text))
            if start is None or end is None or end <= start:
                empty_remap = True
                break
        # A native chapter divider can legitimately contain only its heading.
        # Keep that text and its coordinates: low character count is not a
        # decoding failure, and must not discard the following business pages.
        metadata = {}
        fatal_diagnostics = bool(page and any(d.code != "layout_note_unresolved" for d in page.diagnostics))
        if quality not in {"usable", "low_text"} or page is None or fatal_diagnostics or lost_heading or empty_remap:
            if native_pages is None:
                raise ValueError(f"selected page has unreadable native layout: {number}")
            text = fallback.get(number, "")
            quality, _diagnostics = detect_text_quality(text)
            readable = quality in {"usable", "low_text"}
            metadata = {
                "layout_limitation": f"物理页{number}布局未恢复；" + (
                    "选中章节的布局边界为空或顺序异常；" if empty_remap else ""
                ) + (
                    "仅保留共享原生文本，表格列关系未核定，不猜列、金额或表头对应。" if readable
                    else "原生文本也不可读，本页未读，不等于未披露。"
                ),
                "extraction_method": "shared-native-text-fallback.v1" if readable else "unreadable",
            }
            if not readable:
                text = ""
        elif preserve_note_markers and page.diagnostics:
            metadata["layout_limitation"] = "；".join(d.message for d in page.diagnostics)
        if preserve_note_markers and not metadata.get("extraction_method"):
            metadata["extraction_method"] = "shared-pdf-pypdf-layout.structured-notes.v1"
        pages.append({
            "page": number, "text": text, "readable": quality in {"usable", "low_text"},
            "text_hash": hashlib.sha256(text.encode()).hexdigest(),
            "extraction_method": "shared-pdf-pypdf-layout.v1",
            "layout_quality": quality,
            **metadata,
        })
    return tuple(pages)
