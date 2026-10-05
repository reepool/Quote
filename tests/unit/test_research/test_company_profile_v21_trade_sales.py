"""Frozen p19 company trade revenue supports a native sale, never a quantity."""

import json

import pytest

from tests.unit.test_research.test_company_profile_v19_closure import FIXTURES, _drive


def _fixture_page():
    fixture = json.loads(
        (FIXTURES / "company_profile_v20_unseen_frozen_pages.json").read_text()
    )["600021.SH"]
    return fixture, next(p for p in fixture["pages"] if p["page"] == 19)


def test_frozen_company_trade_revenue_delivers_pending_fuel_sale(tmp_path):
    fixture, page = _fixture_page()
    for profile in _drive(
        tmp_path,
        instrument="600021.SH",
        fixture=fixture,
        pages=[page],
        repair_version="v21",
    ):
        fact = next(
            f for f in profile["accepted_facts"] if f["source_native_name"] == "燃料"
        )
        assert fact["object_type"] == "Activity" and fact["source_actor"] == "公司"
        assert fact["reported_period"] == "2025-12-31"
        assert (
            fact["source_native_value"] is None and fact["source_native_unit"] is None
        )
        evidence = fact["evidence"][0]
        assert evidence["page"] == 19
        assert (
            "11,193.86" in evidence["bounded_quote"]
            and "单位：万元" in evidence["bounded_quote"]
        )
        role = next(
            x
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
            if x["source_native_name"] == "燃料"
        )
        assert role["role"] == "product_sales" and role["mapping_status"] == "pending"
        assert (
            role["commodity_id"] is None and role["market_link_status"] == "not_linked"
        )
        assert not role["measurement_record_ids"]


@pytest.mark.parametrize(
    "old,new",
    [
        ("报告期内公司存在贸易业务收入", "报告期内子公司存在贸易业务收入"),
        ("报告期内公司存在贸易业务收入", "报告期内第三方公司存在贸易业务收入"),
        ("报告期内公司存在贸易业务收入", "报告期内公司拟开展贸易业务并取得收入"),
        ("报告期内公司存在贸易业务收入", "报告期内公司不存在贸易业务收入"),
        ("贸易业务开展情况 本期营业收入", "子公司贸易业务开展情况 本期营业收入"),
        ("贸易业务开展情况 本期营业收入", "第三方贸易业务开展情况 本期营业收入"),
        ("贸易业务开展情况 本期营业收入", "计划贸易业务开展情况 本期营业收入"),
    ],
)
def test_frozen_trade_table_does_not_lift_external_planned_or_denied_sales(
    tmp_path, old, new
):
    fixture, page = _fixture_page()
    assert old in page["text"]
    text = page["text"].replace(old, new, 1)
    for profile in _drive(
        tmp_path,
        instrument="600021.SH",
        fixture=fixture,
        pages=[{**page, "text": text}],
        repair_version="v21",
    ):
        assert not any(
            x["source_native_name"] == "燃料"
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        )
