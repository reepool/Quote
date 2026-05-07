"""
Minimal namespace-aware parser for XBRL numeric facts.
"""

from __future__ import annotations

import io
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from research.providers.official_financial_filings import (
    classify_official_filing_response,
)
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


class FinancialSseStructuredJsonFactParser:
    """Parse SSE official XBRL commonQuery structured JSON into numeric facts."""

    _SQL_STATEMENT_FAMILIES = {
        "COMMON_MAP_INCOMESTATEMENT_C": "income_statement",
        "COMMON_MAP_BALANCESHEET_C": "balance_sheet",
        "COMMON_MAP_CASHFLOW_C": "cash_flow",
    }

    def __init__(self, *, parser_version: str = "sse_commonquery_structured_json_facts.v1"):
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
        payload_obj = self._loads(payload)
        sql_id = str(payload_obj.get("sqlId") or "").upper()
        statement_family = self._SQL_STATEMENT_FAMILIES.get(sql_id)
        rows = payload_obj.get("result")
        if statement_family is None or not isinstance(rows, list):
            return FinancialXbrlParseResult(
                numeric_facts=[],
                diagnostics={
                    "numeric_fact_count": 0,
                    "parser_version": self.parser_version,
                    "sql_id": sql_id,
                    "parse_status": "unsupported_structured_json_payload",
                },
            )

        facts: List[FinancialNumericFactSnapshot] = []
        skipped_non_numeric = 0
        skipped_non_fact_fields = 0
        for row_index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            for key, value in row.items():
                fact_name = str(key)
                if not re.match(r"^S\d{4}_\d{4}$", fact_name):
                    skipped_non_fact_fields += 1
                    continue
                value_text = str(value).strip()
                fact_value = self._to_float(value_text)
                if fact_value is None:
                    skipped_non_numeric += 1
                    continue
                facts.append(
                    FinancialNumericFactSnapshot(
                        source_file_id=source_file_id,
                        instrument_id=instrument_id,
                        symbol=symbol,
                        exchange=exchange,
                        report_period=report_period,
                        report_type=report_type,
                        statement_family=statement_family,
                        fact_name=fact_name,
                        taxonomy_namespace=f"sse:{sql_id.lower()}",
                        context_id=f"{sql_id}:{row_index}",
                        unit="CNY",
                        fact_value=fact_value,
                        value_text=value_text,
                        raw_fact_json={
                            "sql_id": sql_id,
                            "row_index": row_index,
                            "stock_id": row.get("STOCK_ID"),
                            "report_year": row.get("REPORT_YEAR"),
                        },
                        source=source,
                        source_mode=source_mode,
                        parser_version=self.parser_version,
                    )
                )

        return FinancialXbrlParseResult(
            numeric_facts=facts,
            diagnostics={
                "numeric_fact_count": len(facts),
                "parser_version": self.parser_version,
                "sql_id": sql_id,
                "statement_family": statement_family,
                "result_row_count": len(rows),
                "skipped_non_numeric_facts": skipped_non_numeric,
                "skipped_non_fact_fields": skipped_non_fact_fields,
                "parse_status": "parsed" if facts else "no_numeric_facts",
            },
        )

    @staticmethod
    def _loads(payload: bytes | str) -> Dict[str, Any]:
        text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
        text = text.lstrip("\ufeff\r\n\t ")
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError:
            match = re.match(r"^[A-Za-z_$][\w$.]*\s*\((.*)\)\s*;?\s*$", text, flags=re.S)
            if not match:
                match = re.match(r"^null\s*\((.*)\)\s*;?\s*$", text, flags=re.S)
            if not match:
                raise
            loaded = json.loads(match.group(1))
        if not isinstance(loaded, dict):
            raise ValueError("SSE structured JSON payload must be a JSON object")
        return loaded

    @staticmethod
    def _to_float(value: str) -> Optional[float]:
        text = value.strip().replace(",", "")
        if text in {"", "-", "--", "null", "None"}:
            return None
        if not re.match(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$", text):
            return None
        return float(text)


class FinancialStructuredFilingParserDispatcher:
    """Dispatch official structured filing artifacts to parser implementations."""

    def __init__(
        self,
        *,
        parser_version: str = "financial_structured_filing.v1",
        xbrl_parser: Optional[FinancialXbrlNumericFactParser] = None,
        structured_json_parser: Optional[FinancialSseStructuredJsonFactParser] = None,
    ):
        self.parser_version = parser_version
        self.xbrl_parser = xbrl_parser or FinancialXbrlNumericFactParser()
        self.structured_json_parser = (
            structured_json_parser or FinancialSseStructuredJsonFactParser()
        )

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
        artifact_kind: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> FinancialXbrlParseResult:
        raw_payload = payload.encode("utf-8", errors="replace") if isinstance(payload, str) else payload
        resolved_kind = artifact_kind or classify_official_filing_response(
            raw_payload,
            content_type=content_type,
        ).artifact_kind

        if resolved_kind == "xbrl_xml":
            return self._parse_xbrl_payload(
                raw_payload,
                source_file_id=source_file_id,
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange,
                report_period=report_period,
                source=source,
                source_mode=source_mode,
                report_type=report_type,
                artifact_kind=resolved_kind,
            )
        if resolved_kind == "xbrl_zip":
            entry_name, entry_payload = self._extract_xbrl_from_zip(raw_payload)
            return self._parse_xbrl_payload(
                entry_payload,
                source_file_id=source_file_id,
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange,
                report_period=report_period,
                source=source,
                source_mode=source_mode,
                report_type=report_type,
                artifact_kind=resolved_kind,
                archive_entry=entry_name,
            )
        if resolved_kind == "structured_json":
            result = self.structured_json_parser.parse(
                raw_payload,
                source_file_id=source_file_id,
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange,
                report_period=report_period,
                source=source,
                source_mode=source_mode,
                report_type=report_type,
            )
            return FinancialXbrlParseResult(
                numeric_facts=result.numeric_facts,
                diagnostics={
                    **result.diagnostics,
                    "dispatcher_version": self.parser_version,
                    "artifact_kind": resolved_kind,
                },
            )
        if resolved_kind == "structured_html":
            return FinancialXbrlParseResult(
                numeric_facts=[],
                diagnostics={
                    "numeric_fact_count": 0,
                    "parser_version": self.parser_version,
                    "artifact_kind": resolved_kind,
                    "parse_status": "unsupported_artifact_kind",
                },
            )
        raise ValueError(f"Unsupported official financial artifact kind: {resolved_kind or 'unknown'}")

    def _parse_xbrl_payload(
        self,
        payload: bytes,
        *,
        source_file_id: str,
        instrument_id: str,
        symbol: str,
        exchange: str,
        report_period: str,
        source: str,
        source_mode: str,
        report_type: Optional[str],
        artifact_kind: str,
        archive_entry: Optional[str] = None,
    ) -> FinancialXbrlParseResult:
        result = self.xbrl_parser.parse(
            payload,
            source_file_id=source_file_id,
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            source=source,
            source_mode=source_mode,
            report_type=report_type,
        )
        return FinancialXbrlParseResult(
            numeric_facts=result.numeric_facts,
            diagnostics={
                **result.diagnostics,
                "dispatcher_version": self.parser_version,
                "artifact_kind": artifact_kind,
                "archive_entry": archive_entry,
                "parse_status": "parsed",
            },
        )

    @staticmethod
    def _extract_xbrl_from_zip(payload: bytes) -> tuple[str, bytes]:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            for name in archive.namelist():
                lower_name = name.lower()
                if lower_name.endswith((".xbrl", ".xml")) and not lower_name.endswith("/"):
                    return name, archive.read(name)
        raise ValueError("No XML/XBRL entry found in official filing ZIP")
