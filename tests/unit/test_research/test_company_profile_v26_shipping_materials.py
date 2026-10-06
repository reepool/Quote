"""Full frozen owner/PDF pages: business substance, native rows and sold objects."""

import json
import re
from pathlib import Path

import pytest

from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v26_shipping_materials_frozen_pages.json"
)


def _fixture(instrument):
    return json.loads(FIXTURE.read_text())[instrument]


@pytest.mark.parametrize("instrument", ["600026.SH", "600063.SH"])
@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("repair_version", ["v26"])
def test_complete_owner_pages_deliver_source_substance_rows_and_sales(
    tmp_path, instrument, page_kind, repair_version
):
    fixture = _fixture(instrument)
    profiles = _drive(
        tmp_path,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[page_kind],
        repair_version=repair_version,
    )
    assert profiles[0] == profiles[1]
    for profile in profiles:
        for dim, condition in fixture["answers"].items():
            text = _answer(profile, dim)
            assert all(
                any(re.sub(r"\s+", "", alias) in text for alias in group)
                for group in condition["required_groups"]
            ), (dim, text)
        facts = profile["accepted_facts"]
        from research.company_profile.reads import _iter_accepted_raw

        checkpoint = json.loads(
            next(
                (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
            ).read_text()
        )
        raw = {record["record_id"]: record for record in _iter_accepted_raw(checkpoint)}
        for row in fixture["source_rows"]:
            for field in ["segment_dimension", "operating_revenue"]:
                matching = [
                    f
                    for f in facts
                    if f["field_id"] == field and f["source_native_name"] == row["name"]
                ]
                assert len(matching) == 1, (field, row, matching)
                assert matching[0]["source_native_value"] == row["value"]
                assert matching[0]["source_native_unit"] == row["unit"]
                record = raw[matching[0]["record_id"]]
                assert (
                    record.get("dimension", record.get("segment_dimension"))
                    == row["dimension"]
                )
                assert record.get("label", record.get("segment_label")) == row["name"]
        roles = profile["commodity_exposure"]["assessment"]["exposures"]
        assert all(
            any(
                re.sub(r"\s+", "", r["source_native_name"])
                in expected["native_aliases"]
                and r["role"] == expected["role"]
                for expected in fixture["native_roles"]
            )
            for r in roles
        ), [(r["source_native_name"], r["role"]) for r in roles]
        for role in fixture["native_roles"]:
            matching = [
                r
                for r in roles
                if re.sub(r"\s+", "", r["source_native_name"]) in role["native_aliases"]
                and r["role"] == role["role"]
            ]
            assert matching, (
                role["name"],
                [r["source_native_name"] for r in roles],
            )
        assert not any(r["source_native_name"] == "钢铁" for r in roles)
        assert not any(
            "衍生产品" in (f["source_native_name"] or "")
            or f["source_native_name"] in {"水力", "建设", "运营与管理"}
            or "领域核心产品为" in (f["source_native_name"] or "")
            for f in facts
        )
        assert not any(
            "衍生产品" in (f["source_native_name"] or "")
            or f["source_native_name"] in {"利用", "高强高模PVA纤维醋酸乙烯", "贸易"}
            for f in facts
        )


@pytest.mark.parametrize("prefix", ["子公司", "第三方公司", "公司拟", "公司计划"])
def test_complete_input_pages_do_not_raise_other_subjects_or_plans(tmp_path, prefix):
    fixture = _fixture("600063.SH")
    pages = []
    for p in fixture["pages"]:
        if p["page"] not in {27, 33}:
            continue
        text = (
            p["text"]
            .replace("主要产品情况", prefix + "主要产品情况")
            .replace("主要原材料的基本情况", prefix + "主要原材料的基本情况")
            .replace("主要能源的基本情况", prefix + "主要能源的基本情况")
        )
        pages.append({**p, "text": text})
    for profile in _drive(
        tmp_path,
        instrument="600063.SH",
        fixture=fixture,
        pages=pages,
        repair_version="v26",
    ):
        assert not profile["commodity_exposure"]["assessment"]["exposures"]


@pytest.mark.parametrize(
    "modifier", ["子公司", "第三方公司", "公司拟", "公司计划", "公司尚未"]
)
def test_complete_film_page_requires_current_affirmative_disclosure(tmp_path, modifier):
    fixture = _fixture("600063.SH")
    p = next(p for p in fixture["pages"] if p["page"] == 15)
    text = p["text"].replace("可降解洗衣凝珠膜", modifier + "可降解洗衣凝珠膜")
    for profile in _drive(
        tmp_path,
        instrument="600063.SH",
        fixture=fixture,
        pages=[{**p, "text": text}],
        repair_version="v26",
    ):
        assert not any(
            r["source_native_name"] in {"洗衣凝珠膜", "农药包装膜"}
            for r in profile["commodity_exposure"]["assessment"]["exposures"]
        )


def test_complete_combined_table_preserves_subtotals_and_independent_unit(tmp_path):
    fixture = _fixture("600026.SH")
    pages = [p for p in fixture["pages"] if p["page"] in {19, 20}]
    tail = "\n主营业务分产品情况\n单位：万元\n独立服务 100.00 60.00 40.00\n"
    pages[-1] = {
        **pages[-1],
        "text": pages[-1]["text"].split("主营业务分行业、分产品、分地区模式情况的说明")[
            0
        ]
        + tail,
    }
    for profile in _drive(
        tmp_path,
        instrument="600026.SH",
        fixture=fixture,
        pages=pages,
        repair_version="v26",
    ):
        for name in ["内贸油品小计", "外贸油品小计"]:
            rows = [
                f for f in profile["accepted_facts"] if f["source_native_name"] == name
            ]
            assert len(rows) == 2
            assert all(f["source_native_unit"] == "千元" for f in rows)
            assert name not in _answer(profile, "revenue_model")
            assert name not in _answer(profile, "products_services")
        rows = [
            f
            for f in profile["accepted_facts"]
            if f["source_native_name"] == "独立服务"
        ]
        assert len(rows) == 2 and all(f["source_native_unit"] == "万元" for f in rows)


def test_missing_combined_table_continuation_is_not_completed(tmp_path):
    fixture = _fixture("600026.SH")
    p = next(p for p in fixture["pages"] if p["page"] == 19)
    for profile in _drive(
        tmp_path,
        instrument="600026.SH",
        fixture=fixture,
        pages=[p],
        repair_version="v26",
    ):
        assert not any(
            f["field_id"] in {"segment_dimension", "operating_revenue"}
            for f in profile["accepted_facts"]
        )
