import csv
import json

from scripts.dev_validation.export_financial_mapping_review import (
    export_financial_mapping_review,
    main,
)


def _evidence_payload():
    return {
        "samples": [
            {
                "instrument_id": "600519.SH",
                "report_period": "2024-12-31",
                "profile": "nonbank",
                "audit": {
                    "local_standard_field_candidates": {
                        "candidates": [
                            {
                                "standard_field_key_candidate": "revenue",
                                "statement_type": "profit_sheet",
                                "profile": "nonbank",
                                "review_status": "known_canonical_candidate",
                                "canonical_fact_candidates": ["revenue"],
                                "unit_review": {
                                    "status": "known_units_match",
                                    "known_units": ["CNY"],
                                    "unknown_unit_sources": [],
                                },
                                "sources": {
                                    "sina_report": [
                                        {"field_name": "营业收入", "value": 100.0}
                                    ],
                                    "ths_report": [
                                        {"field_name": "operating_income", "value": 100.0}
                                    ],
                                    "eastmoney_report": [
                                        {"field_name": "TOTAL_OPERATE_INCOME", "value": 100.0}
                                    ],
                                },
                            },
                            {
                                "standard_field_key_candidate": "balance_sheet.cash",
                                "statement_type": "balance_sheet",
                                "profile": "nonbank",
                                "review_status": "requires_semantic_review",
                                "canonical_fact_candidates": [],
                                "unit_review": {
                                    "status": "requires_unit_review",
                                    "known_units": [],
                                    "unknown_unit_sources": ["sina_report", "ths_report"],
                                },
                                "sources": {
                                    "sina_report": [
                                        {"field_name": "货币资金", "value": 200.0}
                                    ],
                                    "ths_report": [
                                        {"field_name": "cash", "value": 200.0}
                                    ],
                                    "eastmoney_report": [
                                        {"field_name": "MONETARYFUNDS", "value": 200.0}
                                    ],
                                },
                            },
                        ]
                    }
                },
            }
        ]
    }


def test_export_financial_mapping_review_defaults_to_uncertain_rows():
    result = export_financial_mapping_review(_evidence_payload())

    assert result["summary"]["row_count"] == 1
    row = result["rows"][0]
    assert row["profile"] == "nonbank"
    assert row["unit_review_status"] == "requires_unit_review"
    assert row["sina_fields"] == "货币资金"
    assert row["ths_fields"] == "cash"
    assert row["eastmoney_fields"] == "MONETARYFUNDS"
    assert "semantic_review_required" in row["review_reason"]
    assert row["review_decision"] == ""
    assert row["approved_local_field"] == ""


def test_export_financial_mapping_review_can_include_known_rows():
    result = export_financial_mapping_review(
        _evidence_payload(),
        include_known=True,
        include_machine_approved=True,
    )

    assert result["summary"]["row_count"] == 2
    assert result["summary"]["review_status_counts"] == {
        "known_canonical_candidate": 1,
        "requires_semantic_review": 1,
    }


def test_export_financial_mapping_review_cli_writes_csv_and_markdown(tmp_path):
    evidence_path = tmp_path / "evidence.json"
    output_csv = tmp_path / "review.csv"
    output_group_csv = tmp_path / "review_groups.csv"
    output_md = tmp_path / "review.md"
    output_json = tmp_path / "review.json"
    evidence_path.write_text(
        json.dumps(_evidence_payload(), ensure_ascii=False),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--evidence-path",
            str(evidence_path),
            "--output-csv",
            str(output_csv),
            "--output-md",
            str(output_md),
            "--output-group-csv",
            str(output_group_csv),
            "--output-json",
            str(output_json),
        ]
    )

    assert exit_code == 0
    rows = list(csv.DictReader(output_csv.open(encoding="utf-8")))
    assert len(rows) == 1
    assert rows[0]["standard_field_key_candidate"] == "balance_sheet.cash"
    group_rows = list(csv.DictReader(output_group_csv.open(encoding="utf-8")))
    assert group_rows[0]["issue_type"]
    assert "MONETARYFUNDS" in output_md.read_text(encoding="utf-8")
    assert json.loads(output_json.read_text(encoding="utf-8"))["summary"]["row_count"] == 1
