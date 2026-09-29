"""Directed tests for the stage-4 counterparty research path."""

from __future__ import annotations

import json
from pathlib import Path

from research.company_profile.counterparty_research import (
    COUNTERPARTY_PLAN_VERSION,
    build_counterparty_research_bundle,
    commit_counterparty_research,
    interpret_counterparty_pages,
)

_ROOT = Path(__file__).resolve().parents[3]


def test_contract_party_stays_apart_from_the_same_amount_rank():
    hits, coverages = interpret_counterparty_pages(
        (
            (
                2,
                """
已签订的重大销售合同截至本报告期的履行情况
适用 □不适用
单位：千元
锂电池供应 客户 A(1) - 58,159,202 - 58,159,202
已签订的重大采购合同截至本报告期的履行情况
□适用 不适用
主要销售客户和主要供应商情况
公司主要销售客户情况
前五名客户合计销售金额（千元） 165,061,533
前五名客户合计销售金额占年度销售总额比例 38.96%
1 第一名 58,159,202 13.73%
公司主要供应商情况
前五名供应商合计采购金额（千元） 59,938,203
1 第一名 23,318,360 4.04%
""",
            ),
        )
    )
    customers = [
        item
        for item in hits
        if item.object_type == "Relationship" and item.relation_type == "customer"
    ]
    assert [item.name for item in customers] == ["客户 A(1)", "第一名"]
    assert {item.relationship_context for item in customers} == {
        "contract_customer",
        "rank_customer",
    }
    assert all(item.value == "58,159,202" for item in customers)
    suppliers = [
        item.name
        for item in hits
        if item.object_type == "Relationship" and item.relation_type == "supplier"
    ]
    assert suppliers == ["第一名"]
    assert not any(
        item.name == "合计" for item in hits if item.object_type == "Relationship"
    )
    assert any(
        item.status.value == "not_applicable" and item.relation_type == "supplier"
        for item in coverages
    )
    assert all(
        item.object_type != "Relationship" or item.identity_class for item in hits
    )


def test_totals_only_do_not_create_relationships():
    hits, coverages = interpret_counterparty_pages(
        (
            (
                4,
                """
主要销售客户及主要供应商情况
前五名客户销售额913,511万元，占年度销售总额58.14%；其中前五名客户销售额中关联方销售额0万元，占年度销售总额0%。
前五名供应商采购额115,002万元，占年度采购总额13.98%。
3、费用
""",
            ),
        )
    )
    assert not any(item.object_type == "Relationship" for item in hits)
    assert any(
        item.name == "前五名客户合计"
        and item.value == "913,511"
        and item.unit == "万元"
        for item in hits
    )
    assert any(
        item.relation_type == "customer" and item.status.value == "not_disclosed"
        for item in coverages
    )
    assert any(
        item.relation_type == "supplier" and item.status.value == "not_disclosed"
        for item in coverages
    )


def test_aggregate_and_letter_identities_keep_the_related_party_column():
    hits, _coverages = interpret_counterparty_pages(
        (
            (
                7,
                """
主要客户情况
单位：元
1 浙江衢州硅宝化工有限公司同一控制 下企业 178,099,027.98 17.25% 是
2 J 公司 102,138,447.77 9.89% 否
合计 518,958,619.32 50.27%
主要供应商情况
单位：元
1 巨化集团有限公司及其控制的企业 176,687,834.41 23.42% 是
2 A 公司 64,084,947.81 8.49% 否
合计 387,793,549.7 51.40%
""",
            ),
        )
    )
    relationships = [item for item in hits if item.object_type == "Relationship"]
    assert [item.identity_class for item in relationships] == [
        "report_local_aggregate",
        "report_local_anonymous",
        "report_local_aggregate",
        "report_local_anonymous",
    ]
    assert [item.related_party for item in relationships] == ["是", "否", "是", "否"]
    assert [item.relation_type for item in relationships] == [
        "customer",
        "customer",
        "supplier",
        "supplier",
    ]
    assert not any(item.name == "合计" for item in relationships)
    assert any(item.value == "387,793,549.7" and item.unit == "元" for item in hits)
    assert any(item.value == "518,958,619.32" for item in hits)


def test_later_tables_do_not_backfill_top_five_names():
    hits, _coverages = interpret_counterparty_pages(
        (
            (
                9,
                """
主要销售客户和主要供应商情况
公司主要销售客户情况
前五名客户合计销售金额（元） 72,672,513,444.91
前五名客户合计销售金额占年度销售总额比例 96.44%
前五名客户销售额中关联方销售额占年度销售总额比例 4.93%
3、费用
信用风险集中按照客户进行管理。应收账款的94.72%源于余额前五名客户。
按欠款方归集的期末余额前五名的其他应收款情况
1 某往来单位 11,117,775.42 48.38%
关联交易
1 集团所属单位 3,000,000.00 10.00%
""",
            ),
        )
    )
    assert not any(item.object_type == "Relationship" for item in hits)
    assert not any(item.value == "11,117,775.42" for item in hits)
    assert not any(item.value == "94.72" for item in hits)
    assert any(item.value == "4.93" and item.unit == "%" for item in hits)


def test_coverage_statuses_stay_distinct():
    hits, coverages = interpret_counterparty_pages(
        (
            (1, "   "),
            (
                2,
                """
重大销售合同、重大采购合同截至本报告期的履行情况
□适用 不适用
公司主要销售客户情况
前五名客户合计销售金额 100
前五名客户合计销售金额占年度销售总额比例 10%
""",
            ),
        )
    )
    statuses = {item.status.value for item in coverages}
    assert {
        "extraction_failed",
        "not_applicable",
        "not_disclosed",
        "unclear",
    } <= statuses
    assert not any(item.value == "100" for item in hits)
    assert any(item.value == "10" and item.unit == "%" for item in hits)


def test_runtime_rules_do_not_hardcode_sample_identity():
    source = Path(interpret_counterparty_pages.__code__.co_filename).read_text(
        encoding="utf-8"
    )
    start = source.index("def interpret_counterparty_pages")
    end = source.index("def build_counterparty_research_bundle")
    rules = source[start:end]
    for token in (
        "300750",
        "603659",
        "920015",
        "302132",
        "宁德时代",
        "璞泰来",
        "锦华新材",
        "中航成飞",
        "客户A",
        "客户 A",
        "第一名",
        "J公司",
        "巨化",
        "衢州",
    ):
        assert token not in rules


def test_four_reports_stay_in_one_review_bundle(tmp_path):
    destination = commit_counterparty_research(
        tmp_path / "counterparty-replay",
        repository_root=_ROOT,
        run_id="stage4-counterparties-unit",
    )
    payload = json.loads((destination / "result.json").read_text(encoding="utf-8"))
    assert payload["plan_version"] == COUNTERPARTY_PLAN_VERSION
    assert payload["chapter_task"] == "extract_counterparties_and_concentration"
    assert payload["disposition"] == "accepted_for_review"
    assert payload["provider_calls"] == 0
    assert payload["production_authorization"] == "not_authorized"
    assert payload["processing_identity"]["rules"] == "company_profile_common_core.v1"
    for key in (
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
        "source_review",
    ):
        assert key not in payload
    facts = payload["facts"]
    coverage = payload["coverage"]
    assert all(item["subject_scope"] == "unclear" for item in facts)
    assert "consolidated_group" not in json.dumps(payload, ensure_ascii=False)

    catl = [
        item
        for item in facts
        if item["sample_id"] == "manufacturing-materials-300750-2025"
        and item["object_type"] == "Relationship"
    ]
    assert {
        (item["relation_type"], item["name"], item["relationship_context"])
        for item in catl
    } >= {
        ("customer", "客户 A(1)", "contract_customer"),
        ("customer", "第一名", "rank_customer"),
        ("supplier", "第一名", "rank_supplier"),
    }
    assert any(
        item["sample_id"] == "manufacturing-materials-300750-2025"
        and item["coverage_status"] == "not_applicable"
        and item["relation_type"] == "supplier"
        for item in coverage
    )

    putailai = [
        item
        for item in facts
        if item["sample_id"] == "manufacturing-materials-603659-2025"
    ]
    assert not any(item["object_type"] == "Relationship" for item in putailai)
    assert any(
        item["value"] == "913,511" and item["unit"] == "万元" for item in putailai
    )
    assert any(
        item["sample_id"] == "manufacturing-materials-603659-2025"
        and item["coverage_status"] == "not_disclosed"
        for item in coverage
    )

    jinhua = [
        item
        for item in facts
        if item["sample_id"] == "manufacturing-materials-920015-2025"
        and item["object_type"] == "Relationship"
    ]
    assert any(
        item["identity_class"] == "report_local_aggregate"
        and item["related_party"] == "是"
        for item in jinhua
    )
    assert any(
        item["name"] == "A 公司" and item["relation_type"] == "supplier"
        for item in jinhua
    )
    assert any(
        item["name"] == "J 公司" and item["relation_type"] == "customer"
        for item in jinhua
    )
    assert any(
        item["sample_id"] == "manufacturing-materials-920015-2025"
        and item["value"] == "387,793,549.7"
        for item in facts
    )

    avic = [
        item
        for item in facts
        if item["sample_id"] == "manufacturing-materials-302132-2025-regime"
    ]
    assert not any(item["object_type"] == "Relationship" for item in avic)
    assert any(item["value"] == "72,672,513,444.91" for item in avic)
    assert any(item["value"] == "4.93" for item in avic)
    assert any(
        item["sample_id"] == "manufacturing-materials-302132-2025-regime"
        and item["coverage_status"] == "not_disclosed"
        for item in coverage
    )

    bundle = build_counterparty_research_bundle(
        repository_root=_ROOT,
        run_id="stage4-counterparties-unit",
    )
    assert bundle.disposition == "accepted_for_review"
    assert bundle.provider_calls == 0
