"""Regress the business misses in the first frozen v20 unseen delivery."""

import json

import pytest

from research.company_profile.core_evidence_selection import (
    explicit_material_input_names,
)
from tests.unit.test_research.test_company_profile_v19_closure import (
    FIXTURES,
    _answer,
    _drive,
)


@pytest.mark.parametrize("instrument", ["600021.SH", "600059.SH"])
def test_frozen_full_pages_deliver_native_business_and_roles(tmp_path, instrument):
    fixture = json.loads(
        (FIXTURES / "company_profile_v20_unseen_frozen_pages.json").read_text()
    )[instrument]
    for profile in _drive(
        tmp_path / instrument,
        instrument=instrument,
        fixture=fixture,
        pages=fixture["pages"],
        repair_version="v20",
    ):
        roles = {
            (x["source_native_name"], x["role"])
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        }
        if instrument == "600021.SH":
            facts = profile["accepted_facts"]
            assert {
                f["source_native_name"] for f in facts if f["object_type"] == "Segment"
            } == {
                "电力行业",
                "其他行业",
                "电力",
                "热力",
                "其他",
                "上海地区",
                "江苏地区",
                "土耳其",
                "安徽地区",
                "浙江地区",
                "马耳他地区",
                "日本地区",
                "其他地区",
                "直销",
            }
            assert not any(
                f["source_native_name"] in {"了超临界", "超超临界参数"} for f in facts
            )
            rows = [f for f in facts if f["source_native_name"] == "上海地区"]
            assert {f["object_type"] for f in rows} == {"Segment", "Measurement"}
            assert all(f["source_native_value"] == "13,671,762,626.89" for f in rows)
            assert {
                ("电力", "product_sales"),
                ("热力", "product_sales"),
                ("标煤", "raw_material_input"),
            } <= roles
            assert all(
                name in _answer(profile, "revenue_model")
                for name in ("电力", "热力", "直销")
            )
        else:
            assert (
                len(
                    [
                        f
                        for f in profile["accepted_facts"]
                        if f["object_type"] == "Segment"
                    ]
                )
                == 10
            )
            assert all(
                name in _answer(profile, "products_services")
                for name in (
                    "国酿",
                    "青花醉",
                    "库藏",
                    "金系列",
                    "清醇",
                    "无高低",
                    "果酒",
                    "露酒",
                    "米酒",
                    "坛装酒",
                    "海外产品",
                )
            )
            revenue = _answer(profile, "revenue_model")
            assert (
                "黄酒销售" in revenue
                and "99.98%" in revenue
                and "少量对外销售" in revenue
            )
            assert {
                ("黄酒", "product_sales"),
                ("玻璃瓶", "product_sales"),
                ("糯米", "raw_material_input"),
                ("小麦", "raw_material_input"),
            } <= roles
        assert all(
            x["market_link_status"] == "not_linked"
            for x in profile["commodity_exposure"]["assessment"]["exposures"]
        )


@pytest.mark.parametrize(
    "sentence",
    [
        "子公司合并口径入炉标煤单价（含税）966.98元/吨。",
        "第三方公司合并口径入炉标煤单价（含税）966.98元/吨。",
        "公司计划生产黄酒，酿造的主要原材料（如糯米、小麦）可能对公司的盈利造成影响。",
        "黄酒酿造的主要原材料（如糯米、小麦）可能对供应商公司的盈利造成影响。",
        "客户酿造的主要原材料（如糯米、小麦）可能对公司的盈利造成影响。",
    ],
)
def test_new_input_shapes_do_not_lift_other_subjects_or_plans(sentence):
    assert not explicit_material_input_names(sentence, source_delivery_repair=True)


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司拟"])
def test_native_product_sales_do_not_lift_other_subjects_or_plans(tmp_path, subject):
    fixture = {"document_version": "native-sales-negative"}
    profiles = _drive(
        tmp_path,
        instrument="600059.SH",
        fixture=fixture,
        repair_version="v20",
        pages=[
            {
                "page": 7,
                "readable": True,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    + subject
                    + "主要从事黄酒的研发、酿造、生产和销售等业务。\n"
                    + subject
                    + "有玻璃瓶的生产，自用较多，少量对外销售。"
                ),
            }
        ],
    )
    for profile in profiles:
        assert not profile["commodity_exposure"]["assessment"]["exposures"]


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "公司计划"])
def test_income_mechanism_keeps_subject_and_current_state(tmp_path, subject):
    fixture = json.loads(
        (FIXTURES / "company_profile_v20_unseen_frozen_pages.json").read_text()
    )["600059.SH"]
    page = next(p for p in fixture["pages"] if p["page"] == 13)
    text = page["text"].replace(
        "公司主营业务收入主要来自于", subject + "主营业务收入主要来自于"
    )
    for profile in _drive(
        tmp_path,
        instrument="600059.SH",
        fixture=fixture,
        pages=[{**page, "text": text}],
        repair_version="v20",
    ):
        assert "99.98%" not in _answer(profile, "revenue_model")


@pytest.mark.parametrize("subject", ["子公司", "第三方公司"])
def test_product_matrix_does_not_lift_other_subjects(tmp_path, subject):
    fixture = json.loads(
        (FIXTURES / "company_profile_v20_unseen_frozen_pages.json").read_text()
    )["600059.SH"]
    pages = [
        {
            **p,
            "text": p["text"].replace(
                "公司旗下不同产品系列", subject + "旗下不同产品系列"
            ),
        }
        for p in fixture["pages"]
        if p["page"] in {7, 8}
    ]
    for profile in _drive(
        tmp_path,
        instrument="600059.SH",
        fixture=fixture,
        pages=pages,
        repair_version="v20",
    ):
        assert "国酿系列" not in _answer(profile, "products_services")


@pytest.mark.parametrize("ending", ["没有对外销售", "计划在下一年对外销售"])
def test_external_sales_must_be_established(tmp_path, ending):
    for profile in _drive(
        tmp_path,
        instrument="600059.SH",
        fixture={"document_version": "external-sales-negative"},
        pages=[
            {
                "page": 7,
                "readable": True,
                "text": "报告期内公司从事的业务情况\n公司有玻璃瓶的生产，自用较多，"
                + ending
                + "。",
            }
        ],
        repair_version="v20",
    ):
        assert not profile["commodity_exposure"]["assessment"]["exposures"]
