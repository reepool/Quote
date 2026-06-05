"""Professional DCF xlsx workbook builder.

The project does not currently depend on openpyxl/xlsxwriter. This builder
therefore writes the small subset of Office Open XML needed for auditable DCF
workbooks by using the Python standard library.
"""

from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from xml.sax.saxutils import escape


REQUIRED_SHEETS: Tuple[str, ...] = (
    "Cover",
    "Assumptions",
    "Financials",
    "Forecast",
    "WACC",
    "Valuation",
    "Scenarios",
    "Sensitivity",
    "Diagnostics",
    "Lineage",
)


@dataclass(frozen=True)
class DcfWorkbookArtifact:
    artifact_id: str
    path: Path
    generated_at: str
    expires_at: str
    style: str
    warnings: Tuple[str, ...]
    sheets: Tuple[str, ...]


class DcfWorkbookBuilder:
    """Build a professional DCF workbook artifact."""

    required_sheets = REQUIRED_SHEETS

    def __init__(
        self,
        *,
        artifact_dir: Path | str,
        style: str = "consulting_clean",
        ttl_hours: int = 24,
    ) -> None:
        self.artifact_dir = Path(artifact_dir)
        self.style = style or "consulting_clean"
        self.ttl_hours = int(ttl_hours or 24)

    def build(self, result: Dict[str, Any]) -> DcfWorkbookArtifact:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        generated_at_dt = datetime.now(timezone.utc).replace(microsecond=0)
        expires_at_dt = generated_at_dt + timedelta(hours=self.ttl_hours)
        artifact_id = self._artifact_id(result)
        path = self.artifact_dir / f"{artifact_id}.xlsx"

        sheets = self._build_sheet_rows(result)
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            self._write_package(archive, sheets)

        return DcfWorkbookArtifact(
            artifact_id=artifact_id,
            path=path,
            generated_at=generated_at_dt.isoformat(),
            expires_at=expires_at_dt.isoformat(),
            style=self.style,
            warnings=(),
            sheets=tuple(name for name, _rows in sheets),
        )

    def _build_sheet_rows(
        self,
        result: Dict[str, Any],
    ) -> List[Tuple[str, List[List[Any]]]]:
        assumptions = result.get("assumptions") or {}
        diagnostics = result.get("diagnostics") or {}
        lineage = result.get("lineage") or {}
        net_debt = result.get("net_debt_adjustment") or {}

        return [
            (
                "Cover",
                [
                    ["Professional DCF Model"],
                    ["Instrument", result.get("instrument_id")],
                    ["Symbol", result.get("symbol")],
                    ["Exchange", result.get("exchange")],
                    ["Valuation Date", result.get("valuation_date")],
                    ["Model Profile", result.get("model_profile")],
                    ["Calculation Version", result.get("calc_version")],
                    ["Status", result.get("status")],
                    ["Readiness", (result.get("readiness") or {}).get("level")],
                    ["Formula Mode", "Python-calculated values with formula lineage markers"],
                    ["Disclaimer", "For research review only; not investment advice."],
                ],
            ),
            ("Assumptions", self._assumption_rows(assumptions)),
            (
                "Financials",
                [
                    ["Field", "Value"],
                    ["Base Cash Flow", result.get("base_cash_flow")],
                    ["Base Cash Flow Source", result.get("base_cash_flow_source")],
                    ["Shares Outstanding", result.get("shares_outstanding")],
                    ["Latest Close", result.get("latest_close")],
                    ["Data Available Cutoff", result.get("data_available_cutoff")],
                    ["Financial Lineage", self._json(lineage.get("financial"))],
                    ["Formula Source", "Financial statement facts and configured/manual overrides"],
                ],
            ),
            ("Forecast", self._table_rows(result.get("forecast_rows") or [])),
            ("WACC", self._key_value_rows((assumptions.get("wacc") or {}))),
            (
                "Valuation",
                [
                    ["Metric", "Value"],
                    ["Enterprise Value", result.get("enterprise_value")],
                    ["Equity Value", result.get("equity_value")],
                    ["Terminal Value PV", result.get("terminal_value")],
                    ["Terminal Value %", result.get("terminal_value_pct")],
                    ["Intrinsic Value / Share", self._base_intrinsic_value(result)],
                    ["Net Debt", net_debt.get("net_debt")],
                    ["Cash And Equivalents", net_debt.get("cash_and_equivalents")],
                    ["Total Debt", net_debt.get("total_debt")],
                    ["Lease Liabilities", net_debt.get("lease_liabilities")],
                    ["Preferred Equity", net_debt.get("preferred_equity")],
                    ["Minority Interest", net_debt.get("minority_interest")],
                    ["Non Operating Assets", net_debt.get("non_operating_assets")],
                    ["Formula Bridge", "Equity Value = Enterprise Value - Net Debt"],
                ],
            ),
            ("Scenarios", self._table_rows(self._scenario_rows(result.get("scenarios") or []))),
            ("Sensitivity", self._table_rows(result.get("sensitivity") or [])),
            (
                "Diagnostics",
                [
                    ["Type", "Value"],
                    ["Blockers", ", ".join(diagnostics.get("blockers") or [])],
                    ["Warnings", ", ".join(result.get("warnings") or [])],
                    ["Input Gaps", self._json(diagnostics.get("input_gaps") or [])],
                    ["Workbook Style", self.style],
                    ["Formula Mode", "Python-calculated values; source formulas documented in Lineage"],
                ],
            ),
            (
                "Lineage",
                [
                    ["Field", "Value"],
                    ["Input Hash", result.get("input_hash")],
                    ["Parameter Hash", result.get("parameter_hash")],
                    ["Model Profile", result.get("model_profile")],
                    ["Calculation Method", result.get("calc_method")],
                    ["Calculation Version", result.get("calc_version")],
                    ["Assumption Lineage", self._json(lineage.get("assumptions"))],
                    ["Beta Lineage", self._json(lineage.get("beta"))],
                    ["Company Characteristics", self._json(lineage.get("company_characteristics"))],
                    ["Formula Notes", "FCFF = NOPAT + D&A - Capex - Change in Working Capital"],
                    ["Terminal Value Formula", "FCFF_n * (1 + g) / (WACC - g)"],
                    ["Equity Bridge Formula", "Enterprise Value - Net Debt"],
                ],
            ),
        ]

    def _write_package(
        self,
        archive: zipfile.ZipFile,
        sheets: Sequence[Tuple[str, List[List[Any]]]],
    ) -> None:
        archive.writestr("[Content_Types].xml", self._content_types_xml(len(sheets)))
        archive.writestr("_rels/.rels", self._root_rels_xml())
        archive.writestr("xl/workbook.xml", self._workbook_xml(sheets))
        archive.writestr("xl/_rels/workbook.xml.rels", self._workbook_rels_xml(len(sheets)))
        archive.writestr("xl/styles.xml", self._styles_xml())
        archive.writestr("docProps/core.xml", self._core_xml())
        archive.writestr("docProps/app.xml", self._app_xml(sheets))
        for index, (_name, rows) in enumerate(sheets, start=1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", self._worksheet_xml(rows))

    def _worksheet_xml(self, rows: List[List[Any]]) -> str:
        row_xml = []
        for row_idx, row in enumerate(rows, start=1):
            cells = []
            for col_idx, value in enumerate(row, start=1):
                style_id = self._style_for_cell(row_idx, col_idx, value)
                cells.append(self._cell_xml(self._cell_ref(row_idx, col_idx), value, style_id))
            row_xml.append(f'<row r="{row_idx}">{"".join(cells)}</row>')
        max_col = max((len(row) for row in rows), default=1)
        last_cell = self._cell_ref(max(len(rows), 1), max(max_col, 1))
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetViews><sheetView workbookViewId="0">'
            '<pane xSplit="1" ySplit="1" topLeftCell="B2" activePane="bottomRight" state="frozen"/>'
            "</sheetView></sheetViews>"
            '<cols><col min="1" max="1" width="24" customWidth="1"/>'
            '<col min="2" max="20" width="18" customWidth="1"/></cols>'
            f'<sheetData>{"".join(row_xml)}</sheetData>'
            f'<autoFilter ref="A1:{last_cell}"/>'
            '<printOptions horizontalCentered="1"/>'
            '<pageMargins left="0.4" right="0.4" top="0.6" bottom="0.6" header="0.3" footer="0.3"/>'
            "</worksheet>"
        )

    def _cell_xml(self, ref: str, value: Any, style_id: int) -> str:
        if value is None:
            return f'<c r="{ref}" s="{style_id}"/>'
        if isinstance(value, bool):
            return f'<c r="{ref}" s="{style_id}" t="b"><v>{1 if value else 0}</v></c>'
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return f'<c r="{ref}" s="{style_id}"><v>{value}</v></c>'
        return (
            f'<c r="{ref}" s="{style_id}" t="inlineStr">'
            f"<is><t>{escape(str(value))}</t></is></c>"
        )

    def _style_for_cell(self, row_idx: int, col_idx: int, value: Any) -> int:
        if row_idx == 1:
            return 1
        if row_idx == 2 or (row_idx == 1 and col_idx > 1):
            return 2
        text = str(value or "").lower()
        if "warning" in text or "blocker" in text or "unavailable" in text:
            return 5
        if col_idx == 2:
            return 3
        if isinstance(value, (int, float)):
            return 4
        return 0

    def _table_rows(self, items: Iterable[Dict[str, Any]]) -> List[List[Any]]:
        items = list(items)
        if not items:
            return [["No data"]]
        keys: List[str] = []
        for item in items:
            for key in item.keys():
                if key not in keys and not isinstance(item.get(key), (list, dict)):
                    keys.append(key)
        return [keys] + [[item.get(key) for key in keys] for item in items]

    def _scenario_rows(self, scenarios: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = []
        for scenario in scenarios:
            rows.append(
                {
                    key: value
                    for key, value in scenario.items()
                    if key not in {"forecast_rows", "projected_cash_flows"}
                }
            )
        return rows

    def _assumption_rows(self, assumptions: Dict[str, Any]) -> List[List[Any]]:
        rows = [["Assumption", "Value", "Source", "Primary Source", "As Of", "Quality", "Fallback"]]
        for key, value in assumptions.items():
            if key == "wacc" or not isinstance(value, dict):
                continue
            rows.append(
                [
                    key,
                    value.get("value"),
                    value.get("source"),
                    value.get("primary_source"),
                    value.get("as_of_date"),
                    value.get("quality_flag"),
                    value.get("fallback_used"),
                ]
            )
        return rows

    def _key_value_rows(self, mapping: Dict[str, Any]) -> List[List[Any]]:
        rows = [["Field", "Value"]]
        for key, value in mapping.items():
            if isinstance(value, (dict, list)):
                value = self._json(value)
            rows.append([key, value])
        return rows

    def _base_intrinsic_value(self, result: Dict[str, Any]) -> Optional[float]:
        for scenario in result.get("scenarios") or []:
            if scenario.get("scenario") == "base":
                return scenario.get("intrinsic_value_per_share")
        return None

    def _artifact_id(self, result: Dict[str, Any]) -> str:
        raw = "_".join(
            str(value or "")
            for value in (
                result.get("instrument_id"),
                result.get("valuation_date"),
                result.get("model_profile"),
                result.get("input_hash"),
                result.get("parameter_hash"),
            )
        )
        slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("_")[:140]
        return f"dcf_{slug or 'run'}"

    @staticmethod
    def _json(value: Any) -> str:
        if value in (None, ""):
            return ""
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    @staticmethod
    def _cell_ref(row_idx: int, col_idx: int) -> str:
        letters = ""
        col = col_idx
        while col:
            col, remainder = divmod(col - 1, 26)
            letters = chr(65 + remainder) + letters
        return f"{letters}{row_idx}"

    @staticmethod
    def _content_types_xml(sheet_count: int) -> str:
        sheet_overrides = "".join(
            f'<Override PartName="/xl/worksheets/sheet{i}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            for i in range(1, sheet_count + 1)
        )
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/styles.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
            '<Override PartName="/docProps/core.xml" '
            'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
            '<Override PartName="/docProps/app.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
            f"{sheet_overrides}</Types>"
        )

    @staticmethod
    def _root_rels_xml() -> str:
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>'
            '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>'
            "</Relationships>"
        )

    @staticmethod
    def _workbook_xml(sheets: Sequence[Tuple[str, List[List[Any]]]]) -> str:
        sheet_xml = "".join(
            f'<sheet name="{escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
            for idx, (name, _rows) in enumerate(sheets, start=1)
        )
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f"<sheets>{sheet_xml}</sheets></workbook>"
        )

    @staticmethod
    def _workbook_rels_xml(sheet_count: int) -> str:
        rels = "".join(
            f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
            for i in range(1, sheet_count + 1)
        )
        rels += (
            f'<Relationship Id="rId{sheet_count + 1}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
            'Target="styles.xml"/>'
        )
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            f"{rels}</Relationships>"
        )

    @staticmethod
    def _styles_xml() -> str:
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<fonts count="3"><font><sz val="10"/><name val="Aptos"/></font>'
            '<font><b/><sz val="14"/><name val="Aptos"/></font>'
            '<font><b/><sz val="10"/><name val="Aptos"/></font></fonts>'
            '<fills count="6"><fill><patternFill patternType="none"/></fill>'
            '<fill><patternFill patternType="gray125"/></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/></patternFill></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/></patternFill></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FFE2F0D9"/></patternFill></fill>'
            '<fill><patternFill patternType="solid"><fgColor rgb="FFFFE699"/></patternFill></fill></fills>'
            '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
            '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
            '<cellXfs count="7">'
            '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
            '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFill="1" applyFont="1"/>'
            '<xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyFill="1" applyFont="1"/>'
            '<xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1"/>'
            '<xf numFmtId="4" fontId="0" fillId="4" borderId="0" xfId="0" applyFill="1" applyNumberFormat="1"/>'
            '<xf numFmtId="0" fontId="2" fillId="5" borderId="0" xfId="0" applyFill="1" applyFont="1"/>'
            '<xf numFmtId="10" fontId="0" fillId="4" borderId="0" xfId="0" applyFill="1" applyNumberFormat="1"/>'
            '</cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
            "</styleSheet>"
        )

    @staticmethod
    def _core_xml() -> str:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/" '
            'xmlns:dcterms="http://purl.org/dc/terms/" '
            'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
            "<dc:creator>Quote Research DCF</dc:creator>"
            "<dc:title>Professional DCF Workbook</dc:title>"
            f'<dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created>'
            f'<dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>'
            "</cp:coreProperties>"
        )

    @staticmethod
    def _app_xml(sheets: Sequence[Tuple[str, List[List[Any]]]]) -> str:
        titles = "".join(f"<vt:lpstr>{escape(name)}</vt:lpstr>" for name, _rows in sheets)
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
            'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
            "<Application>Quote Research</Application>"
            f'<TitlesOfParts><vt:vector size="{len(sheets)}" baseType="lpstr">{titles}</vt:vector></TitlesOfParts>'
            "</Properties>"
        )
