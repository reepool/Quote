import csv
import json

from scripts.dev_validation.import_financial_mapping_review import (
    import_financial_mapping_review,
    main,
)


def _review_row(**overrides):
    row = {
        "profile": "nonbank",
        "statement_type": "balance_sheet",
        "standard_field_key_candidate": "balance_sheet.cash",
        "unit_review_status": "requires_unit_review",
        "sina_fields": "货币资金",
        "ths_fields": "cash",
        "eastmoney_fields": "MONETARYFUNDS",
        "review_decision": "",
        "approved_local_field": "",
        "approved_semantic": "",
        "approved_canonical_unit": "",
        "approved_source_unit": "",
        "unit_multiplier": "",
        "relationship": "",
        "approved_sina_field": "",
        "approved_ths_metric": "",
        "approved_eastmoney_field": "",
        "review_notes": "",
    }
    row.update(overrides)
    return row


def test_import_financial_mapping_review_approves_complete_core_row():
    result = import_financial_mapping_review(
        [
            _review_row(
                review_decision="approve_core",
                approved_local_field="cash_and_cash_equivalents",
                approved_semantic="cash_and_cash_equivalents",
                approved_canonical_unit="CNY",
                relationship="exact_equivalent",
            )
        ],
        mapping_version="sina_ths_core_financial_facts.review_draft",
    )

    assert result["summary"]["approved_count"] == 1
    assert result["summary"]["error_count"] == 0
    mapping = result["approved_mappings"][0]
    assert mapping["canonical_fact"] == "cash_and_cash_equivalents"
    assert mapping["statement_family"] == "balance_sheet"
    assert mapping["sina_field"] == "货币资金"
    assert mapping["ths_metric"] == "cash"
    assert mapping["canonical_unit"] == "CNY"
    assert mapping["approved_for_core"] is True


def test_import_financial_mapping_review_rejects_incomplete_approval():
    result = import_financial_mapping_review(
        [_review_row(review_decision="approve_core")],
        mapping_version="sina_ths_core_financial_facts.review_draft",
    )

    assert result["summary"]["approved_count"] == 0
    assert result["summary"]["error_count"] == 1
    assert "missing required approval fields" in result["errors"][0]["errors"][0]


def test_import_financial_mapping_review_requires_explicit_source_field_when_ambiguous():
    result = import_financial_mapping_review(
        [
            _review_row(
                review_decision="approve_core",
                approved_local_field="cash_and_cash_equivalents",
                approved_semantic="cash_and_cash_equivalents",
                approved_canonical_unit="CNY",
                sina_fields="货币资金 | 现金及现金等价物",
                ths_fields="cash",
            )
        ],
        mapping_version="sina_ths_core_financial_facts.review_draft",
    )

    assert result["summary"]["error_count"] == 1
    assert "approved_sina_field" in result["errors"][0]["errors"][0]


def test_import_financial_mapping_review_cli_writes_json_and_fails_on_errors(tmp_path):
    review_csv = tmp_path / "review.csv"
    output_json = tmp_path / "draft.json"
    rows = [_review_row(review_decision="approve_core")]
    with review_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    exit_code = main(
        [
            "--review-csv",
            str(review_csv),
            "--mapping-version",
            "sina_ths_core_financial_facts.review_draft",
            "--output-json",
            str(output_json),
            "--fail-on-errors",
        ]
    )

    assert exit_code == 1
    assert json.loads(output_json.read_text(encoding="utf-8"))["summary"]["error_count"] == 1
