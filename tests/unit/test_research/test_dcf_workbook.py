import zipfile
from pathlib import Path

from research.dcf_workbook import REQUIRED_SHEETS
from research.valuation_service import ResearchValuationService


def _run_workbook_dcf(tmp_path):
    service = ResearchValuationService(
        {
            "dcf": {
                "professional": {
                    "workbook": {
                        "artifact_dir": str(tmp_path),
                        "artifact_ttl_hours": 1,
                        "default_style": "consulting_clean",
                    }
                }
            }
        }
    )
    return service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "depreciation_and_amortization": 40.0,
            "change_in_working_capital": 10.0,
            "cash_and_equivalents": 100.0,
            "total_debt": 200.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18", "include_workbook": True},
    )


def test_dcf_workbook_builder_creates_required_xlsx_sheets_and_metadata(tmp_path):
    result = _run_workbook_dcf(tmp_path)

    workbook = result["workbook"]
    assert workbook["workbook_available"] is True
    assert workbook["workbook_artifact_id"].startswith("dcf_")
    assert workbook["style"] == "consulting_clean"
    assert workbook["generated_at"] < workbook["expires_at"]
    assert set(REQUIRED_SHEETS).issubset(set(workbook["sheets"]))

    artifact_path = Path(tmp_path) / f"{workbook['workbook_artifact_id']}.xlsx"
    assert "artifact_path" not in workbook
    with zipfile.ZipFile(artifact_path) as archive:
        names = set(archive.namelist())
        assert "[Content_Types].xml" in names
        assert "xl/workbook.xml" in names
        assert "xl/styles.xml" in names
        assert "xl/worksheets/sheet1.xml" in names
        workbook_xml = archive.read("xl/workbook.xml").decode("utf-8")
        styles_xml = archive.read("xl/styles.xml").decode("utf-8")
        cover_xml = archive.read("xl/worksheets/sheet1.xml").decode("utf-8")

    for sheet_name in REQUIRED_SHEETS:
        assert f'name="{sheet_name}"' in workbook_xml
    assert "FFE2F0D9" in styles_xml
    assert "FFFFE699" in styles_xml
    assert 'state="frozen"' in cover_xml
    assert "Professional DCF Model" in cover_xml
    assert "Formula Mode" in cover_xml


def test_dcf_workbook_contains_diagnostics_for_unavailable_result(tmp_path):
    service = ResearchValuationService(
        {"dcf": {"professional": {"workbook": {"artifact_dir": str(tmp_path)}}}}
    )

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18", "include_workbook": True},
    )

    assert result["status"] == "unavailable"
    artifact_path = Path(tmp_path) / f"{result['workbook']['workbook_artifact_id']}.xlsx"
    with zipfile.ZipFile(artifact_path) as archive:
        workbook_xml = archive.read("xl/workbook.xml").decode("utf-8")
        diagnostics_xml = archive.read("xl/worksheets/sheet9.xml").decode("utf-8")

    assert 'name="Diagnostics"' in workbook_xml
    assert "capital_expenditure_required" in diagnostics_xml
