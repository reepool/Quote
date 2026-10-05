"""Directed repair of actual misses; never replace the first formal observation."""

import json
from pathlib import Path

import pytest

from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _profiles(tmp_path, instrument, text_replace=None):
    fixture = json.loads(
        (FIXTURES / f"company_profile_unseen_{instrument[:6]}_frozen.json").read_text()
    )
    pages = fixture["pages"]
    if text_replace:
        pages = [{**p, "text": text_replace(p["text"])} for p in pages]
    return _drive(
        tmp_path / instrument, instrument=instrument, fixture=fixture, pages=pages
    )


def test_complete_highway_pages_deliver_every_long_road_and_bound_company_actor(
    tmp_path,
):
    for profile in _profiles(tmp_path, "600020.SH"):
        rows = [f for f in profile["accepted_facts"] if f["object_type"] == "Segment"]
        assert len(rows) == 12
        names = {f["source_native_name"]: f["source_native_value"] for f in rows}
        assert names["京港澳高速郑州至漯河高速公路"] == "1,994,414,866.70"
        assert names["京港澳高速漯河至驻马店高速公路"] == "439,759,072.91"
        assert names["郑州至民权高速公路郑州至开封段"] == "409,448,078.73"
        assert names["郑州至民权高速公路开封至民权段"] == "114,476,254.01"
        activities = [
            f["source_native_name"]
            for f in profile["accepted_facts"]
            if f["object_type"] == "Activity"
        ]
        assert not {"项目投资", "投资管理", "投资咨询", "房地产"} & set(activities)
        assert "运营管理" in activities
        assert all("利润" not in name for name in activities)


def test_complete_pharma_pages_deliver_owned_overview_matrix_and_industry_mix(tmp_path):
    for profile in _profiles(tmp_path, "600056.SH"):
        principal = _answer(profile, "principal_business")
        products = _answer(profile, "products_services")
        for label in (
            "医药工业业务",
            "医药商业业务",
            "医疗器械业务",
            "国际贸易业务",
            "大健康和电商业务",
        ):
            assert label in principal
        for product in ("化学制剂", "化学原料药", "中成药", "中药饮片"):
            assert product in products
        revenue = _answer(profile, "revenue_model")
        for label in ("医药工业", "医药商业", "国际贸易", "大健康和电商"):
            assert label in revenue
        facts = profile["accepted_facts"]
        raw_drug = [f for f in facts if f["source_native_name"] == "原料药"]
        assert len(raw_drug) == 2
        assert all(
            f["source_native_value"] == "820,511,443.08"
            and f["source_native_unit"] == "元"
            for f in raw_drug
        )
        assert len([f for f in facts if f["object_type"] == "Segment"]) == 7
        assert not any("抵消" in (f["source_native_name"] or "") for f in facts)
        activities = [
            f["source_native_name"] for f in facts if f["object_type"] == "Activity"
        ]
        assert "中成药及大健康产品" in activities
        assert not any("建立" in name or "公司在中药" in name for name in activities)


@pytest.mark.parametrize(
    "replacement", ["子公司业务：子公司", "第三方业务：第三方", "医药工业业务：公司拟"]
)
def test_business_blocks_do_not_promote_an_external_or_planned_position(
    tmp_path, replacement
):
    def change(text):
        return text.replace("医药工业业务：公司", replacement)

    for profile in _profiles(tmp_path, "600056.SH", change):
        assert not _answer(profile, "principal_business")
