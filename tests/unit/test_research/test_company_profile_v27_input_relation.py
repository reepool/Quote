"""Full owned tables distinguish material use from explicit external purchase."""

import json

import pytest

from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research import (
    test_company_profile_v26_shipping_materials as frozen,
)
from tests.unit.test_research.test_company_profile_v19_closure import _drive

UPSTREAM = {
    "电石",
    "煤炭",
    "醋酸",
    "VAC",
    "甲醇",
    "乙烯",
    "糖蜜",
    "酒精",
    "PVA",
    "丁醛",
    "盐酸",
    "芒硝",
    "硫酸",
    "精对苯二甲酸",
    "乙二醇",
    "石灰石",
    "煤",
    "PVA光学膜",
    "TAC膜",
    "PVB树脂",
    "增塑剂",
    "VAE乳液",
    "甘油",
    "聚乙二醇",
}


@pytest.mark.parametrize("instrument", ["600026.SH", "600063.SH"])
@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
def test_full_source_delivery_preserves_roles_and_input_actions(
    tmp_path, instrument, page_kind
):
    frozen.test_complete_owner_pages_deliver_source_substance_rows_and_sales(
        tmp_path, instrument, page_kind, "v27"
    )
    checkpoint = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    records = list(_iter_accepted_raw(checkpoint))
    if instrument != "600063.SH":
        return
    upstream = [
        r
        for r in records
        if r["object_type"] in {"Activity", "Relationship"}
        and any(e["page"] == 27 for e in r["evidence"])
    ]
    assert len(upstream) == len(UPSTREAM)
    assert {r["source_native"]["name"] for r in upstream} == UPSTREAM
    for record in upstream:
        assert record["object_type"] == "Relationship"
        assert record["relation_type"] == "material_input"
        assert record["field_id"] == "material_input"
        assert record["chapter_task"] == "extract_material_inputs"
        assert record["reported_period"] == "2025-12-31"
        assert record["subject_scope"] == "consolidated_group"
        assert record["source_native"]["header"] == "主要上游原材料"
        assert not record.get("action")
    purchased = [r for r in records if any(e["page"] == 33 for e in r["evidence"])]
    assert len(purchased) == 5
    for record in purchased:
        assert record["object_type"] == "Activity"
        assert record["action"] == "purchases"
        name = record["source_native"]["name"]
        assert name in {"煤", "醋酸", "甲醇", "乙烯", "电"}
        assert record["source_verb"] == ("耗用" if name == "电" else "采购")
        assert record["source_native"]["header"] == (
            "能源" if name == "电" else "原材料"
        )
    profile = json.loads(
        next((tmp_path / "export").glob("600063.SH_*.json")).read_text()
    )
    roles = profile["commodity_exposure"]["assessment"]["exposures"]
    for name in ["PVA", "PVB树脂", "VAE乳液"]:
        assert {r["role"] for r in roles if r["source_native_name"] == name} == {
            "product_sales",
            "raw_material_input",
        }


@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("prefix", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_full_input_pages_keep_subject_and_plan_boundaries(tmp_path, page_kind, prefix):
    fixture = frozen._fixture("600063.SH")
    pages = [
        {
            **p,
            "text": p["text"]
            .replace("主要产品情况", prefix + "主要产品情况")
            .replace("主要原材料的基本情况", prefix + "主要原材料的基本情况")
            .replace("主要能源的基本情况", prefix + "主要能源的基本情况"),
        }
        for p in fixture[page_kind]
        if p["page"] in {27, 33}
    ]
    for profile in _drive(
        tmp_path,
        instrument="600063.SH",
        fixture=fixture,
        pages=pages,
        repair_version="v27",
    ):
        assert not profile["commodity_exposure"]["assessment"]["exposures"]
        assert not any(
            f["object_type"] == "Relationship" for f in profile["accepted_facts"]
        )
