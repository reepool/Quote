"""Official complete pages preserve native columns, actor scope and directions."""

import json
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v58_energy_automotive_pages.json"
)


def _records(root):
    checkpoint = next(
        (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
    )
    return list(_iter_accepted_raw(json.loads(checkpoint.read_text())))


@pytest.mark.parametrize("instrument", ["600098.SH", "600104.SH"])
def test_official_complete_pages_accept_query_export(tmp_path, instrument):
    fixture = json.loads(FIXTURE.read_text())[instrument]
    root = tmp_path / instrument
    query, exported = _drive(
        root,
        instrument=instrument,
        fixture=fixture,
        pages=fixture["pages"],
        repair_version="v58",
    )
    assert query == exported
    records = _records(root)
    facts = [(r["object_type"], r["source_native"], r) for r in records]
    if instrument == "600098.SH":
        for name, value in [
            ("燃煤发电及供热业务", "4,349,947,127.70"),
            ("天然气发电及供热业务", "3,204,831,500.79"),
        ]:
            assert any(
                k == "Segment"
                and n["name"].removeprefix("(1)").removeprefix("(2)") == name
                and n["value"] == value
                for k, n, r in facts
            )
        assert not any(n["name"] in {"供热业务", "及供热业务"} for k, n, r in facts)
        assert not any(
            n.get("value")
            in {"92,182,690.31", "94,945,119.03", "770,121.24", "337,904.69"}
            for k, n, r in facts
            if k in {"Segment", "Measurement"}
        )
        for name in ["广东红海湾发电有限公司", "广东珠海金湾液化天然气有限公司"]:
            matching = [r for k, n, r in facts if k == "Segment" and n["name"] == name]
            assert matching
            assert all(
                "联营企业" in r["source_native"]["header"]
                and "原栏目冲突" in r["source_native"]["qualifier"]
                and r["subject_scope"] == "business_segment"
                for r in matching
            )
        assert not any(
            r.get("action") == "sells" and n["name"] in {"油品仓储", "燃气管网建设"}
            for k, n, r in facts
        )
        main = next(
            a["excerpt"]
            for a in query["dimensions"]
            if a["dimension_id"] == "principal_business"
        )
        products = next(
            a["excerpt"]
            for a in query["dimensions"]
            if a["dimension_id"] == "products_services"
        )
        assert "佛山高明独立储能项目" in main and "项目投产" in main
        assert "河北邯郸大名独立储能项目" in main and "处于调试阶段" in main
        assert "危化品仓储" in products
        required = ["上网电量", "抄表", "扣罚", "实际利率", "佣金"]
    else:
        for actor, expected in [
            ("上汽大众汽车有限公司", "business_segment"),
            ("上汽通用汽车有限公司", "business_segment"),
            ("上汽大通汽车有限公司", "named_subsidiary"),
            ("智己汽车科技有限公司", "named_subsidiary"),
        ]:
            matching = [
                r
                for k, n, r in facts
                if k == "Activity"
                and n["name"] == "汽车"
                and r["source_actor"] == actor
            ]
            assert matching
            assert all(r["subject_scope"] == expected for r in matching)
        for basis, value in [
            ("主营业务收入", "632,007,457,472.74"),
            ("其他业务收入", "14,144,644,416.56"),
        ]:
            assert any(
                k == "Segment"
                and n["name"] == "合计"
                and n["value"] == value
                and n["header"] == basis
                for k, n, r in facts
            )
        research = [
            r for k, n, r in facts if k == "Activity" and n["name"] == "研发劳务"
        ]
        assert len(research) == 5
        assert all(r["source_native"]["header"] == "本期采购服务" for r in research)
        assert not any(
            n["name"] in {"配件的", "和研发", "数量等因素的销售折扣"}
            for k, n, r in facts
        )
        assert not any(
            n.get("value") in {"250,550,820.65", "28,476.00", "768,363,630.07"}
            for k, n, r in facts
            if k in {"Segment", "Measurement"}
        )
        main = next(
            a["excerpt"]
            for a in query["dimensions"]
            if a["dimension_id"] == "principal_business"
        )
        products = next(
            a["excerpt"]
            for a in query["dimensions"]
            if a["dimension_id"] == "products_services"
        )
        assert "经销商电子交易平台" in main
        assert "SUV" in products and "MPV" in products
        required = ["电子交易平台", "售后代管", "销售折扣", "已发生的成本", "代理人"]
    revenue = next(
        a["excerpt"]
        for a in query["dimensions"]
        if a["dimension_id"] == "revenue_model"
    )
    assert all(word in revenue for word in required)


@pytest.mark.parametrize(
    "business, expected, rejected",
    [
        (
            "燃气管网建设、燃气销售",
            {("燃气管网建设", "provides_service"), ("燃气", "sells")},
            {("燃气管网建设", "sells")},
        ),
        (
            "汽车零部件总成的设计、生产和销售",
            {("汽车零部件总成", "produces"), ("汽车零部件总成", "sells")},
            {("汽车零部件总成的设计", "sells")},
        ),
        ("电力供应", {("电力供应", "provides_service")}, {("电力", "sells")}),
        ("拟生产销售电池", set(), {("电池", "sells")}),
    ],
)
def test_noun_and_verb_lists_keep_independent_directions(business, expected, rejected):
    actual = {
        (r["name"], r["action"].value)
        for r in selection._business_nature_action_bindings("原具名公司", business)
    }
    assert expected <= actual
    assert not (rejected & actual)


def test_discount_policy_is_not_a_sale_object():
    text = (
        "本集团向客户提供基于销售数量等因素的销售折扣，本集团根据历史经验确定折扣金额。"
    )
    assert not selection._owned_goods_bindings(text)
