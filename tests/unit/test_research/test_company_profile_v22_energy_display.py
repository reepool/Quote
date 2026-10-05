"""Frozen complete source pages deliver substantive answers, rows and native roles."""

import json
import re

import pytest

from research.company_profile.core_evidence_selection import (
    _company_coal_risk_source,
    _company_trade_sales_product_names,
    _current_shipped_product_bindings,
)
from tests.unit.test_research.test_company_profile_v19_closure import (
    FIXTURES,
    _answer,
    _drive,
)


def _fixture(instrument):
    return json.loads(
        (FIXTURES / "company_profile_v21_unseen_frozen_pages.json").read_text()
    )[instrument]


def _page(instrument, number):
    return next(p for p in _fixture(instrument)["pages"] if p["page"] == number)


@pytest.mark.parametrize("instrument", ["600023.SH", "600060.SH"])
@pytest.mark.parametrize("page_variant", ["pages", "owner_parser_pages"])
def test_full_frozen_pages_accept_query_export_substantive_answers_rows_roles(
    tmp_path, instrument, page_variant
):
    fixture = _fixture(instrument)
    for profile in _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[page_variant],
        repair_version="v22",
    ):
        facts = profile["accepted_facts"]
        for dimension in ("principal_business", "products_services"):
            text = _answer(profile, dimension)
            terms = (
                [
                    "煤电",
                    "气电",
                    "核电",
                    "热电联产",
                    "综合能源",
                    "公司取得中来股份控制权",
                    "中来股份为",
                    "光伏辅材",
                    "高效电池及组件",
                    "光伏应用",
                ]
                if instrument == "600023.SH"
                else [
                    "公司坚持",
                    "公司持续深耕",
                    "智慧显示终端",
                    "激光显示",
                    "商用显示",
                    "芯片",
                    "云服务",
                    "同时积极布局",
                    "机器人",
                ]
            )
            assert all(term in text for term in terms)
            assert any(
                text == re.sub(r"\s+", "", f.get("source_text") or "")
                for f in facts
                if f["object_type"] == "BusinessOverview"
            )
        rows = [f for f in facts if f["object_type"] == "Segment"]
        assert len(rows) == 8
        for row in rows:
            measurement = next(
                f
                for f in facts
                if f["object_type"] == "Measurement"
                and f["source_native_name"] == row["source_native_name"]
            )
            assert (row["source_native_value"], row["source_native_unit"]) == (
                measurement["source_native_value"],
                measurement["source_native_unit"],
            )
            assert row["source_native_unit"] == (
                "元" if instrument == "600023.SH" else "万元"
            )
        revenue = _answer(profile, "revenue_model")
        if instrument == "600023.SH":
            row = next(
                f for f in rows if f["source_native_name"] == "电力、热力生产及供应"
            )
            assert row["source_native_value"] == "72,870,145,425.91"
            assert "在某一时点确认" in {f["source_native_name"] for f in rows}
            assert all(
                term in revenue
                for term in [
                    "电力",
                    "蒸汽",
                    "光伏产品",
                    "国家电网",
                    "市场化交易",
                    "收入确认：在某一时点确认",
                ]
            )
            assert "销售模式：在某一时点确认" not in revenue
            assert not any(
                "管理及控股" in f["source_native_name"]
                for f in facts
                if f["object_type"] == "Activity"
            )
            expected = {
                (n, "product_sales")
                for n in ["电力", "热力", "背板", "电池", "组件", "市场煤"]
            } | {("煤炭", "raw_material_input")}
            market = next(f for f in facts if f["source_native_name"] == "市场煤")
            assert (
                market["source_native_value"] is None
                and market["source_native_unit"] is None
            )
            assert any(
                "286,336.52" in e["bounded_quote"] and e["page"] == 13
                for e in market["evidence"]
            )
            coal = next(f for f in facts if f["source_native_name"] == "煤炭")
            assert all(
                "公司装机结构中煤电占比高" in re.sub(r"\s+", "", e["bounded_quote"])
                and "狠抓" not in e["bounded_quote"]
                for e in coal["evidence"]
            )
        else:
            assert all(
                term in revenue
                for term in ["智慧显示终端", "新显示新业务", "直销", "经销"]
            )
            expected = {(n, "product_sales") for n in ["显示产品", "芯片", "AI耳机"]}
            chip = next(f for f in facts if f["source_native_name"] == "芯片")
            assert "战略控股" in chip["source_actor"]
        roles = profile["commodity_exposure"]["assessment"]["exposures"]
        assert {(x["source_native_name"], x["role"]) for x in roles} == expected
        assert len(roles) == len(expected)
        if instrument == "600060.SH":
            assert (
                next(x for x in roles if x["source_native_name"] == "芯片")[
                    "subject_scope"
                ]
                == "consolidated_group"
            )
        assert all(x["market_link_status"] == "not_linked" for x in roles)
        assert not any(
            x["source_native_name"]
            in {"面板", "原材料", "机器人", "电力万千瓦时", "显示产品万台"}
            for x in roles
        )


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_current_business_narrative_rejects_other_subjects_and_plans(tmp_path, subject):
    page = _page("600060.SH", 12)
    # The complete business section is kept, including its own boundary and future layout.
    end = page["text"].index("二、报告期内公司所处行业情况")
    text = (
        page["text"][:end]
        .replace("公司坚持", subject + "坚持")
        .replace("公司持\n续深耕", subject + "持续深耕")
    )
    for profile in _drive(
        tmp_path,
        instrument="600060.SH",
        fixture=_fixture("600060.SH"),
        pages=[{**page, "text": text}],
        repair_version="v22",
    ):
        assert not _answer(profile, "principal_business")
        assert not _answer(profile, "products_services")


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_market_coal_trade_rejects_other_subjects_and_plans(subject):
    page = _page("600023.SH", 13)
    text = page["text"].replace(
        "报告期内公司存在贸易业务收入", "报告期内" + subject + "存在贸易业务收入"
    )
    assert not _company_trade_sales_product_names(text)


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_coal_risk_requires_own_current_company_fleet(subject):
    page = _page("600023.SH", 26)
    text = page["text"].replace(
        "公司装机结构中煤电占比高", subject + "装机结构中煤电占比高"
    )
    assert not _company_coal_risk_source(text)
    assert not _company_coal_risk_source("煤炭成本在燃煤发电企业的发电成本中占比较大。")


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_shipped_products_keep_group_and_company_context_without_external_plans(
    subject,
):
    chip = _page("600060.SH", 23)["text"].replace("公司战略控股", subject + "战略控股")
    wearable = _page("600060.SH", 24)["text"].replace(
        "报告期内，公司凭借", "报告期内，" + subject + "凭借"
    )
    assert not _current_shipped_product_bindings(chip)
    assert not _current_shipped_product_bindings(wearable)


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟", "公司计划"])
@pytest.mark.parametrize("instrument", ["600023.SH", "600060.SH"])
def test_full_page_negative_role_contexts_do_not_enter_query_export(
    tmp_path, subject, instrument
):
    fixture = _fixture(instrument)
    replacements = (
        [
            ("报告期内公司存在贸易业务收入", "报告期内" + subject + "存在贸易业务收入"),
            ("公司装机结构中煤电占比高", subject + "装机结构中煤电占比高"),
            ("公司电力销\n售客户主要为", subject + "电力销售客户主要为"),
            (
                "公司煤机全年市场化交易累计成交电量",
                subject + "煤机全年市场化交易累计成交电量",
            ),
        ]
        if instrument == "600023.SH"
        else [
            ("公司战略控股", subject + "战略控股"),
            ("报告期内，公司凭借", "报告期内，" + subject + "凭借"),
            (
                "主要产品 单位 生产量 销售量",
                subject + "产销量\n主要产品 单位 生产量 销售量",
            ),
        ]
    )
    pages = []
    for page in fixture["pages"]:
        text = page["text"]
        for old, new in replacements:
            text = text.replace(old, new)
        pages.append({**page, "text": text})
    for profile in _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=pages,
        repair_version="v22",
    ):
        names = {
            x["source_native_name"]
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        }
        assert not names & (
            {"市场煤", "煤炭"}
            if instrument == "600023.SH"
            else {"芯片", "AI耳机", "显示产品"}
        )
        if instrument == "600023.SH":
            assert "国家电网" not in _answer(profile, "revenue_model")
            assert "市场化交易" not in _answer(profile, "revenue_model")
