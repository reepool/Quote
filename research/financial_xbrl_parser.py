"""
Minimal namespace-aware parser for XBRL numeric facts.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from research.providers.base import FinancialNumericFactSnapshot


_XBRLI_NS = "http://www.xbrl.org/2003/instance"


@dataclass(frozen=True)
class FinancialXbrlParseResult:
    """Parsed numeric facts and parser diagnostics."""

    numeric_facts: List[FinancialNumericFactSnapshot]
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class FinancialXbrlNumericFactParser:
    """Parse numeric facts from an XBRL instance document."""

    def __init__(self, *, parser_version: str = "xbrl_numeric_facts.v1"):
        self.parser_version = parser_version

    def parse(
        self,
        payload: bytes | str,
        *,
        source_file_id: str,
        instrument_id: str,
        symbol: str,
        exchange: str,
        report_period: str,
        source: str,
        source_mode: str = "direct",
        report_type: Optional[str] = None,
    ) -> FinancialXbrlParseResult:
        raw_text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
        root = ET.fromstring(raw_text)
        contexts = self._parse_contexts(root)
        units = self._parse_units(root)
        facts: List[FinancialNumericFactSnapshot] = []
        skipped_non_numeric = 0

        for element in root.iter():
            attributes = dict(element.attrib)
            context_id = attributes.get("contextRef")
            if not context_id:
                continue
            value_text = (element.text or "").strip()
            fact_value = self._to_float(value_text)
            if fact_value is None:
                skipped_non_numeric += 1
                continue
            namespace, fact_name = self._split_tag(element.tag)
            context = contexts.get(context_id, {})
            unit_id = attributes.get("unitRef")
            facts.append(
                FinancialNumericFactSnapshot(
                    source_file_id=source_file_id,
                    instrument_id=instrument_id,
                    symbol=symbol,
                    exchange=exchange,
                    report_period=report_period,
                    report_type=report_type,
                    statement_family=None,
                    fact_name=fact_name,
                    taxonomy_namespace=namespace,
                    context_id=context_id,
                    unit=units.get(unit_id, unit_id),
                    decimals=attributes.get("decimals"),
                    precision=attributes.get("precision"),
                    period_start=context.get("period_start"),
                    period_end=context.get("period_end"),
                    instant=context.get("instant"),
                    fact_value=fact_value,
                    value_text=value_text,
                    dimensions_json=context.get("dimensions", {}),
                    raw_fact_json={"attributes": attributes},
                    source=source,
                    source_mode=source_mode,
                    parser_version=self.parser_version,
                )
            )

        return FinancialXbrlParseResult(
            numeric_facts=facts,
            diagnostics={
                "numeric_fact_count": len(facts),
                "context_count": len(contexts),
                "unit_count": len(units),
                "skipped_non_numeric_facts": skipped_non_numeric,
                "parser_version": self.parser_version,
            },
        )

    def _parse_contexts(self, root: ET.Element) -> Dict[str, Dict[str, Any]]:
        contexts: Dict[str, Dict[str, Any]] = {}
        for context in root.findall(f".//{{{_XBRLI_NS}}}context"):
            context_id = context.attrib.get("id")
            if not context_id:
                continue
            period = context.find(f"{{{_XBRLI_NS}}}period")
            details: Dict[str, Any] = {"dimensions": {}}
            if period is not None:
                instant = period.find(f"{{{_XBRLI_NS}}}instant")
                start = period.find(f"{{{_XBRLI_NS}}}startDate")
                end = period.find(f"{{{_XBRLI_NS}}}endDate")
                details["instant"] = None if instant is None else (instant.text or "").strip()
                details["period_start"] = None if start is None else (start.text or "").strip()
                details["period_end"] = None if end is None else (end.text or "").strip()
            for member in context.findall(".//{*}explicitMember"):
                dimension = member.attrib.get("dimension")
                if dimension:
                    details["dimensions"][dimension] = (member.text or "").strip()
            contexts[context_id] = details
        return contexts

    @staticmethod
    def _parse_units(root: ET.Element) -> Dict[str, str]:
        units: Dict[str, str] = {}
        for unit in root.findall(f".//{{{_XBRLI_NS}}}unit"):
            unit_id = unit.attrib.get("id")
            measure = unit.find(f"{{{_XBRLI_NS}}}measure")
            if unit_id and measure is not None:
                units[unit_id] = (measure.text or "").strip()
        return units

    @staticmethod
    def _split_tag(tag: str) -> tuple[Optional[str], str]:
        if tag.startswith("{"):
            namespace, local_name = tag[1:].split("}", 1)
            return namespace, local_name
        return None, tag

    @staticmethod
    def _to_float(value: str) -> Optional[float]:
        text = value.strip().replace(",", "")
        if text in {"", "-", "--"}:
            return None
        if not re.match(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$", text):
            return None
        return float(text)
