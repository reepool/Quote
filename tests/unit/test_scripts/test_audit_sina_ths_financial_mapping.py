import json
from copy import deepcopy

from scripts.dev_validation.audit_sina_ths_financial_mapping import (
    audit_sina_ths_financial_mapping_sample,
    main,
)


def _sample_payload():
    return {
        "instrument_id": "600000.SH",
        "report_period": "2025-12-31",
        "sources": {
            "sina_report": {
                "balance_sheet": [
                    {
                        "报告日": "20251231",
                        "资产总计": 1200.0,
                        "负债合计": 420.0,
                        "归属于母公司所有者权益合计": 760.0,
                        "股东权益合计": 780.0,
                    }
                ],
                "profit_sheet": [
                    {
                        "报告日": "20251231",
                        "营业收入": 1000.0,
                        "营业利润": 230.0,
                        "利润总额": 220.0,
                        "归属于母公司所有者的净利润": 180.0,
                    }
                ],
                "cash_flow_sheet": [
                    {
                        "报告日": "20251231",
                        "经营活动产生的现金流量净额": 210.0,
                    }
                ],
            },
            "ths_report": {
                "balance_sheet": [
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "assets_total",
                        "value": 1200.0,
                        "single": 1200.0,
                        "yoy": 1.0,
                        "mom": 0.1,
                        "single_yoy": 1.0,
                    },
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "total_debt",
                        "value": 420.0,
                    },
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "parent_holder_equity_total",
                        "value": 760.0,
                    },
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "holder_equity_total",
                        "value": 780.0,
                    },
                ],
                "profit_sheet": [
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "operating_income",
                        "value": 1000.0,
                    },
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "operating_profit",
                        "value": 230.0,
                    },
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "profit_total",
                        "value": 220.0,
                    },
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "parent_holder_net_profit",
                        "value": 180.0,
                    },
                ],
                "cash_flow_sheet": [
                    {
                        "report_date": "2025-12-31",
                        "metric_name": "act_cash_flow_net",
                        "value": 210.0,
                    }
                ],
            },
            "cninfo_data20": {
                "numeric_facts": [
                    {
                        "fact_name": "营业收入",
                        "canonical_fact_name": "revenue",
                        "fact_value": 1000.0,
                        "unit": "CNY",
                    },
                    {
                        "fact_name": "资产总计",
                        "canonical_fact_name": "total_assets",
                        "fact_value": 1200.0,
                        "unit": "CNY",
                    },
                ]
            },
            "eastmoney_report": {
                "balance_sheet": [
                    {
                        "REPORT_DATE": "2025-12-31",
                        "TOTAL_ASSETS": 1200.0,
                        "TOTAL_LIABILITIES": 420.0,
                        "TOTAL_EQUITY": 780.0,
                    }
                ],
                "profit_sheet": [
                    {
                        "REPORT_DATE": "2025-12-31",
                        "TOTAL_OPERATE_INCOME": 1000.0,
                        "OPERATE_PROFIT": 230.0,
                        "TOTAL_PROFIT": 220.0,
                        "PARENT_NETPROFIT": 180.0,
                    }
                ],
                "cash_flow_sheet": [
                    {
                        "REPORT_DATE": "2025-12-31",
                        "NETCASH_OPERATE": 210.0,
                    }
                ],
            },
        },
    }


def test_audit_sina_ths_financial_mapping_sample_reports_counts_and_passed_mappings():
    result = audit_sina_ths_financial_mapping_sample(
        _sample_payload(),
        profile="nonbank",
    )

    assert result["status"] == "passed"
    assert result["field_counts"]["sina_report"] == 9
    assert result["field_counts"]["ths_report"] == 9
    assert result["field_counts"]["cninfo_data20"] == 2
    assert result["summary"]["approved_mapping_count"] >= 10
    assert result["summary"]["approved_mapping_passed_count"] == 8
    assert result["summary"]["missing_source_value_count"] >= 2
    assert result["summary"]["blocking_issue_count"] == 0
    assert result["identity_checks"]["sina_report"][
        "assets_equals_liabilities_plus_equity_total"
    ]["status"] == "passed"
    revenue_mapping = next(
        item
        for item in result["mapping_audit"]
        if item["canonical_fact"] == "revenue" and item["sina_field"] == "营业收入"
    )
    assert revenue_mapping["approved_for_core"] is True
    assert revenue_mapping["comparison"]["absolute_diff"] == 0.0
    assert result["canonical_field_matches"]["sina_report"]["equity_parent"][0][
        "field_name"
    ] == "归属于母公司所有者权益合计"
    assert any(
        item["field_name"] == "营业收入"
        for item in result["source_field_values"]["sina_report"]
    )
    sina_ths_candidates = result["generated_mapping_candidates"]["candidate_pairs"][
        "sina_report__ths_report"
    ]
    assert any(
        item["left_field_name"] == "营业收入"
        and item["right_field_name"] == "operating_income"
        and item["relationship_candidate"] == "exact_equivalent"
        for item in sina_ths_candidates
    )
    local_candidates = result["local_standard_field_candidates"]
    assert local_candidates["basis"] == "sina_ths_intersection_with_eastmoney_enrichment"
    assert local_candidates["summary"]["candidate_count"] >= 8
    revenue_candidate = next(
        item
        for item in local_candidates["candidates"]
        if item["standard_field_key_candidate"] == "revenue"
        and item["sources"]["sina_report"][0]["field_name"] == "营业收入"
    )
    assert revenue_candidate["review_status"] == "known_canonical_candidate"
    assert revenue_candidate["approved_for_local_standard"] is False
    assert revenue_candidate["profile"] == "nonbank"
    assert revenue_candidate["unit_review"]["status"] == "known_units_match"
    assert revenue_candidate["unit_review"]["known_units"] == ["CNY"]
    assert revenue_candidate["sources"]["ths_report"][0]["field_name"] == "operating_income"
    assert revenue_candidate["sources"]["eastmoney_report"][0]["field_name"] == (
        "TOTAL_OPERATE_INCOME"
    )


def test_audit_sina_ths_financial_mapping_sample_reports_candidate_label_variants():
    payload = deepcopy(_sample_payload())
    balance_row = payload["sources"]["sina_report"]["balance_sheet"][0]
    balance_row["归属于母公司股东权益合计"] = balance_row.pop("归属于母公司所有者权益合计")

    result = audit_sina_ths_financial_mapping_sample(payload, profile="nonbank")

    exact_mapping = next(
        item
        for item in result["mapping_audit"]
        if item["canonical_fact"] == "equity_parent"
        and item["sina_field"] == "归属于母公司所有者权益合计"
    )
    assert exact_mapping["status"] == "missing_source_value"
    assert result["canonical_field_matches"]["sina_report"]["equity_parent"][0][
        "field_name"
    ] == "归属于母公司股东权益合计"


def test_audit_sina_ths_financial_mapping_rejects_oci_component_as_total():
    payload = deepcopy(_sample_payload())
    profit_row = payload["sources"]["sina_report"]["profit_sheet"][0]
    profit_row["外币财务报表折算差额"] = 10.0
    ths_profit = payload["sources"]["ths_report"]["profit_sheet"]
    ths_profit.append(
        {
            "report_date": "2025-12-31",
            "metric_name": "other_common_profit",
            "value": 10.0,
        }
    )

    result = audit_sina_ths_financial_mapping_sample(payload, profile="nonbank")

    candidate = next(
        item
        for item in result["local_standard_field_candidates"]["candidates"]
        if item["sources"]["sina_report"][0]["field_name"] == "外币财务报表折算差额"
        and item["sources"]["ths_report"][0]["field_name"] == "other_common_profit"
    )
    assert candidate["machine_review"]["status"] == "machine_rejected_candidate"
    assert candidate["machine_review"]["suggested_decision"] == "reject"


def test_audit_sina_ths_financial_mapping_allows_cninfo_summary_rounding():
    payload = deepcopy(_sample_payload())
    for source_name in ("sina_report", "ths_report", "eastmoney_report"):
        if source_name == "sina_report":
            payload["sources"][source_name]["profit_sheet"][0]["营业收入"] = 24795539.62
        elif source_name == "ths_report":
            revenue_row = next(
                row
                for row in payload["sources"][source_name]["profit_sheet"]
                if row["metric_name"] == "operating_income"
            )
            revenue_row["value"] = 24795539.62
        else:
            payload["sources"][source_name]["profit_sheet"][0][
                "TOTAL_OPERATE_INCOME"
            ] = 24795539.62
    payload["sources"]["cninfo_data20"]["numeric_facts"][0]["fact_value"] = 24795500.0

    result = audit_sina_ths_financial_mapping_sample(payload, profile="nonbank")

    cninfo_revenue = next(
        item
        for item in result["canonical_comparisons"]
        if item["other_source"] == "cninfo_data20"
        and item["canonical_fact"] == "revenue"
    )
    assert cninfo_revenue["status"] == "passed"
    assert cninfo_revenue["absolute_tolerance"] == 100.0
    assert cninfo_revenue["relative_tolerance"] == 1e-5


def test_audit_sina_ths_financial_mapping_keeps_cninfo_mismatch_observational():
    payload = deepcopy(_sample_payload())
    payload["sources"]["cninfo_data20"]["numeric_facts"][0]["fact_value"] = 1200.0

    result = audit_sina_ths_financial_mapping_sample(payload, profile="nonbank")

    cninfo_revenue = next(
        item
        for item in result["canonical_comparisons"]
        if item["other_source"] == "cninfo_data20"
        and item["canonical_fact"] == "revenue"
    )
    assert cninfo_revenue["status"] == "value_mismatch"
    assert cninfo_revenue["blocking_for_local_core"] is False
    assert result["summary"]["canonical_mismatch_count"] == 1
    assert result["summary"]["blocking_canonical_mismatch_count"] == 0
    assert result["summary"]["blocking_issue_count"] == 0
    assert result["status"] == "passed"


def test_audit_sina_ths_financial_mapping_generates_review_candidates_for_unmapped_fields():
    payload = deepcopy(_sample_payload())
    payload["sources"]["sina_report"]["profit_sheet"][0]["未登记新浪字段"] = 777.0
    payload["sources"]["ths_report"]["profit_sheet"].append(
        {
            "report_date": "2025-12-31",
            "metric_name": "unmapped_ths_metric",
            "value": 777.0,
        }
    )

    result = audit_sina_ths_financial_mapping_sample(payload, profile="nonbank")

    candidates = result["generated_mapping_candidates"]["candidate_pairs"][
        "sina_report__ths_report"
    ]
    candidate = next(
        item
        for item in candidates
        if item["left_field_name"] == "未登记新浪字段"
        and item["right_field_name"] == "unmapped_ths_metric"
    )
    assert candidate["relationship_candidate"] == "unknown_candidate"
    assert candidate["approved_for_core"] is False
    local_candidate = next(
        item
        for item in result["local_standard_field_candidates"]["candidates"]
        if item["sources"]["sina_report"][0]["field_name"] == "未登记新浪字段"
    )
    assert local_candidate["standard_field_key_candidate"] == "profit_sheet.unmapped_ths_metric"
    assert local_candidate["review_status"] == "requires_semantic_review"
    assert local_candidate["unit_review"]["status"] == "known_units_match"
    assert local_candidate["machine_review"]["status"] == "needs_more_evidence"
    assert "missing_eastmoney_match" in local_candidate["machine_review"]["blockers"]


def test_audit_sina_ths_financial_mapping_sample_keeps_rejected_bank_mapping_out_of_core():
    result = audit_sina_ths_financial_mapping_sample(
        _sample_payload(),
        profile="bank",
    )
    rejected = next(
        item for item in result["mapping_audit"] if item["ths_metric"] == "total_cash"
    )

    assert rejected["approved_for_core"] is False
    assert rejected["status"] == "not_approved_for_core"
    assert rejected["rejection_reason"]


def test_audit_sina_ths_financial_mapping_cli_writes_json(tmp_path):
    sample_path = tmp_path / "sample.json"
    output_path = tmp_path / "audit.json"
    sample_path.write_text(json.dumps(_sample_payload(), ensure_ascii=False), encoding="utf-8")

    exit_code = main(
        [
            "--sample-path",
            str(sample_path),
            "--output-path",
            str(output_path),
            "--profile",
            "nonbank",
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert payload["summary"]["approved_mapping_passed_count"] == 8
