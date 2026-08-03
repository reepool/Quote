"""Deterministic table and keyword span extraction from selected sections."""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from research.business_profile_disclosure_templates import ResolvedDisclosureTemplate
from research.business_profile_pdf_artifacts import (
    BusinessProfileParserDiagnostic,
    build_table_parse_failure_diagnostic,
)
from research.business_profile_section_selection import SelectedSectionArtifact


TABLE_PARSER_VERSION = "business_profile_text_table_parser.v1"
KEYWORD_SELECTOR_VERSION = "business_profile_action_object_spans.v1"

_ACTION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("produces", re.compile(r"(?:生产|制造|产出)(?P<object>[\u4e00-\u9fffA-Za-z0-9·-]{2,24})")),
    ("processes", re.compile(r"(?:加工|冶炼|精炼)(?P<object>[\u4e00-\u9fffA-Za-z0-9·-]{2,24})")),
    ("purchases", re.compile(r"(?:采购|购入)(?P<object>[\u4e00-\u9fffA-Za-z0-9·-]{2,24})")),
    ("consumes", re.compile(r"(?:消耗|使用)(?P<object>[\u4e00-\u9fffA-Za-z0-9·-]{2,24})")),
    ("sells", re.compile(r"(?:销售|出售)(?P<object>[\u4e00-\u9fffA-Za-z0-9·-]{2,24})")),
    ("hedges", re.compile(r"(?:套期保值|开展套保)(?P<object>[\u4e00-\u9fffA-Za-z0-9·-]{0,24})")),
)


@dataclass(frozen=True)
class ParsedTable:
    table_id: str
    signature_id: str
    page_numbers: tuple[int, ...]
    headers: tuple[str, ...]
    unit: str | None
    rows: tuple[Mapping[str, Any], ...]
    source_section_ids: tuple[str, ...]
    parser_version: str = TABLE_PARSER_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["page_numbers"] = list(self.page_numbers)
        payload["headers"] = list(self.headers)
        payload["rows"] = [dict(item) for item in self.rows]
        payload["source_section_ids"] = list(self.source_section_ids)
        return payload


@dataclass(frozen=True)
class CandidateSpan:
    span_id: str
    section_id: str
    page_number: int
    action_hint: str
    object_hint: str
    quote: str
    normalized_start: int
    normalized_end: int
    quote_hash: str
    selector_version: str = KEYWORD_SELECTOR_VERSION


def parse_selected_tables(
    selected: SelectedSectionArtifact,
    *,
    templates: Sequence[ResolvedDisclosureTemplate],
) -> tuple[list[ParsedTable], list[BusinessProfileParserDiagnostic]]:
    """Parse simple governed native-text tables and fail closed on conflicts."""

    signatures = [
        signature
        for resolved in templates
        for signature in resolved.template.table_signatures
    ]
    tables: list[ParsedTable] = []
    diagnostics: list[BusinessProfileParserDiagnostic] = []
    claimed_sections: set[str] = set()
    for signature in signatures:
        matched_sections = [
            section
            for section in selected.sections
            if section.section_key in signature.section_keys
            or _header_match_count(section.text, signature.required_headers)
            >= signature.min_required_header_matches
        ]
        if not matched_sections:
            continue
        try:
            parsed = _parse_signature_sections(signature, matched_sections)
        except ValueError as exc:
            diagnostics.append(
                build_table_parse_failure_diagnostic(
                    page_numbers=[item.page_number for item in matched_sections],
                    detail=str(exc),
                    field_name=signature.fact_group,
                )
            )
            continue
        if parsed is not None:
            tables.append(parsed)
            claimed_sections.update(parsed.source_section_ids)
    return tables, diagnostics


def locate_action_object_spans(
    selected: SelectedSectionArtifact,
    *,
    context_characters: int = 80,
) -> list[CandidateSpan]:
    """Locate bounded candidate spans without asserting a governed activity."""

    if context_characters < 0 or context_characters > 500:
        raise ValueError("context_characters must be between 0 and 500")
    output: list[CandidateSpan] = []
    for section in selected.sections:
        text = section.normalized_text
        for action, pattern in _ACTION_PATTERNS:
            for match in pattern.finditer(text):
                start = max(0, match.start() - context_characters)
                end = min(len(text), match.end() + context_characters)
                quote = text[start:end]
                object_hint = str(match.groupdict().get("object") or "").strip()
                core = {
                    "section_id": section.section_id,
                    "page_number": section.page_number,
                    "action_hint": action,
                    "object_hint": object_hint,
                    "quote": quote,
                    "normalized_start": section.normalized_start + start,
                    "normalized_end": section.normalized_start + end,
                    "quote_hash": hashlib.sha256(quote.encode("utf-8")).hexdigest(),
                }
                output.append(CandidateSpan(span_id=_stable_id(core), **core))
    unique = {item.span_id: item for item in output}
    return [unique[key] for key in sorted(unique)]


def _parse_signature_sections(signature: Any, sections: Sequence[Any]) -> ParsedTable | None:
    headers: list[str] | None = None
    rows_by_key: dict[str, dict[str, Any]] = {}
    page_numbers: set[int] = set()
    source_ids: list[str] = []
    unit: str | None = None
    for section in sections:
        lines = [line.strip() for line in section.text.splitlines() if line.strip()]
        local_unit = next(
            (
                match.group(1).strip()
                for line in lines[:4]
                for match in [re.search(r"单位\s*[:：]\s*([^\s|]+)", line)]
                if match
            ),
            None,
        )
        if local_unit:
            if unit is not None and unit != local_unit:
                raise ValueError(f"conflicting table units: {unit} vs {local_unit}")
            unit = local_unit
        for index, line in enumerate(lines):
            cells = _split_cells(line)
            if len(cells) < 2:
                continue
            required_matches = _header_match_count(" ".join(cells), signature.required_headers)
            if required_matches >= signature.min_required_header_matches:
                if headers is None:
                    headers = cells
                    header_unit = _header_unit(cells)
                    if header_unit:
                        if unit is not None and unit != header_unit:
                            raise ValueError(
                                f"conflicting table units: {unit} vs {header_unit}"
                            )
                        unit = header_unit
                elif [_normalize(item) for item in headers] != [
                    _normalize(item) for item in cells
                ]:
                    raise ValueError("cross-page table header changed")
                continue
            if headers is None or len(cells) != len(headers):
                continue
            row_label = _strip_footnote(cells[0])
            if not row_label:
                continue
            role = _row_role(row_label, signature.row_role_markers)
            row = {
                "row_label": row_label,
                "row_role": role,
                "cells": {
                    _strip_footnote(header): _strip_footnote(value)
                    for header, value in zip(headers, cells)
                },
                "page_number": section.page_number,
                "section_id": section.section_id,
            }
            key = _normalize(row_label)
            previous = rows_by_key.get(key)
            if previous is not None:
                previous_cells = {
                    key: value
                    for key, value in previous["cells"].items()
                    if key != headers[0]
                }
                current_cells = {
                    key: value for key, value in row["cells"].items() if key != headers[0]
                }
                if previous_cells != current_cells:
                    raise ValueError(f"conflicting duplicate table row: {row_label}")
                continue
            rows_by_key[key] = row
            page_numbers.add(section.page_number)
            source_ids.append(section.section_id)
    if headers is None or not rows_by_key:
        return None
    core = {
        "signature_id": signature.signature_id,
        "page_numbers": sorted(page_numbers),
        "headers": headers,
        "unit": unit,
        "rows": list(rows_by_key.values()),
        "source_section_ids": sorted(set(source_ids)),
    }
    return ParsedTable(
        table_id=_stable_id(core),
        signature_id=signature.signature_id,
        page_numbers=tuple(core["page_numbers"]),
        headers=tuple(headers),
        unit=unit,
        rows=tuple(core["rows"]),
        source_section_ids=tuple(core["source_section_ids"]),
    )


def _split_cells(line: str) -> list[str]:
    if "|" in line:
        return [item.strip() for item in line.strip("|").split("|")]
    if "\t" in line:
        return [item.strip() for item in line.split("\t")]
    return [item.strip() for item in re.split(r"\s{2,}", line)]


def _header_match_count(text: str, headers: Sequence[str]) -> int:
    normalized = _normalize(text)
    return sum(_normalize(header) in normalized for header in headers)


def _row_role(label: str, markers: Sequence[tuple[str, Sequence[str]]]) -> str:
    normalized = _normalize(label)
    for role, values in markers:
        if any(_normalize(value) in normalized for value in values):
            return role
    return "detail"


def _strip_footnote(value: str) -> str:
    return re.sub(r"(?:[（(]?注\s*\d+[）)]?|[①②③④⑤*]+)$", "", str(value)).strip()


def _header_unit(headers: Sequence[str]) -> str | None:
    for header in headers:
        match = re.search(r"[（(]([^（）()]{1,20})[）)]", str(header))
        if match and any(
            marker in match.group(1)
            for marker in ("元", "吨", "克", "立方米", "千瓦", "%")
        ):
            return match.group(1).strip()
    return None


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


def _stable_id(value: Any) -> str:
    encoded = repr(sorted(value.items())).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
