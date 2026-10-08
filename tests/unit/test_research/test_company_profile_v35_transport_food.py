"""Full PDF/owner deliveries retain native actors, actions and income columns."""

import json
import re
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v35_transport_food_frozen_pages.json"
)


def compact(text):
    return re.sub(r"\s+", "", text or "").replace("（", "(").replace("）", ")")


def fixture(i):
    return json.loads(FIXTURE.read_text())[i]


def delivered(root, i, pages, repair_version="v36"):
    q, e = _drive(
        root,
        instrument=i,
        fixture=fixture(i),
        pages=pages,
        repair_version=repair_version,
    )
    assert q == e
    checkpoint = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return q, list(_iter_accepted_raw(checkpoint))


@pytest.mark.parametrize("repair_version", ["v35", "v36"])
@pytest.mark.parametrize("i", ["600035.SH", "600073.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_full_owned_sources_deliver_all_substance_cells_and_roles(
    tmp_path, i, kind, repair_version
):
    f = fixture(i)
    q, raw = delivered(tmp_path, i, f[kind], repair_version)
    for dim, rule in f["answers"].items():
        assert all(
            any(compact(s) in compact(_answer(q, dim)) for s in group)
            for group in rule["required_groups"]
        ), dim
    for row in f["source_rows"]:
        for model in ["Segment", "Measurement"]:
            candidates = [
                r
                for r in raw
                if r["object_type"] == model
                and compact(r["source_native"]["name"])
                in set(map(compact, row["native_aliases"]))
                and r["source_native"]["value"] == row["value"]
                and r["source_native"]["unit"] == row["unit"]
                and row["page"]
                in {
                    p
                    for evidence in r["evidence"]
                    for p in [
                        evidence.get("page"),
                        *evidence.get("continuation_pages", []),
                    ]
                }
                and (
                    (r.get("dimension") or r.get("segment_dimension"))
                    == row["dimension"]
                    or (
                        r.get("dimension") or r.get("segment_dimension"),
                        row["dimension"],
                    )
                    in {("business_segment", "report_segment"), ("lease", "service")}
                )
            ]
            assert candidates, row
            if row["header"] == "主营业务分地区":
                assert any(
                    r["source_native"]["header"] == "主营业务收入" for r in candidates
                ), row
            assert any(
                (r["subject_scope"] == "issuer") == (row["source_actor"] == "母公司")
                for r in candidates
            ), row
            if row.get("counterparty"):
                assert any(
                    compact(row["counterparty"])
                    in compact(r["source_native"].get("qualifier"))
                    for r in candidates
                ), row
            if row["dimension"] == "sales_mode":
                assert any(
                    (r.get("dimension") or r.get("segment_dimension")) == "sales_mode"
                    for r in candidates
                ), row
            if row["dimension"] == "report_segment" and (
                "交易收入" in row["column"] or row["column"] == "分部营业收入合计"
            ):
                assert any(
                    r["source_native"]["header"] == row["column"] for r in candidates
                ), row
    raw_ids = {r["record_id"]: r for r in raw}
    for role in f["native_roles"]:
        exposures = [
            x
            for x in q["commodity_exposure"]["assessment"]["exposures"]
            if x["role"] == role["role"]
            and x["source_native_name"] in role["native_aliases"]
        ]
        assert exposures, role
        assert any(
            raw_ids[rid].get("action") in role["source_actions"]
            or raw_ids[rid].get("relation_type") in role["source_relation_types"]
            for x in exposures
            for rid in x["source_record_ids"]
        ), role
    assert not any(
        r.get("object_name")
        in {
            "银蕨",
            "苏食",
            "爱森",
            "联豪",
            "肉制品，及糖果",
            "饮用水等，主要品牌包括：梅林B2",
        }
        for r in raw
    )
    if i == "600073.SH":
        veg = [r for r in raw if r.get("object_name") == "蔬菜及番茄沙司类罐头"]
        assert veg and all(
            r["source_actor"] == "上海梅林食品有限公司/上海梅林正广和（绵阳）有限公司"
            for r in veg
        )
        assert all(r["subject_scope"] == "named_subsidiary" for r in veg)
        assert any(
            r.get("object_name") == "饲料"
            and r.get("relation_type") == "material_input"
            for r in raw
        )
        assert all(
            r["source_native"]["header"] == "全部营业收入/销售渠道"
            for r in raw
            if r["subject_scope"] == "issuer" and r["source_native"]["name"] == "直营"
        )


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("replacement", ["不销售生猪", "拟销售生猪", "尚未销售生猪"])
def test_negative_subsidiary_sales_leave_other_current_goods(
    tmp_path, kind, replacement
):
    i = "600073.SH"
    pages = fixture(i)[kind]
    for p in pages:
        if p["page"] == 11:
            p["text"] = re.sub(r"销\s*售生猪", replacement, p["text"])
    q, raw = delivered(tmp_path, i, pages)
    assert not any(
        r.get("object_name") == "生猪"
        and r.get("action") == "sells"
        and r.get("source_actor") == "光明农牧科技有限公司"
        for r in raw
    )
    assert any(
        r.get("object_name") == "生猪" and r.get("action") == "purchases" for r in raw
    )
    assert any(
        r.get("object_name") == "羔羊肉" and r.get("action") == "sells" for r in raw
    )
    assert not any(
        x["source_native_name"] == "生猪" and x["role"] == "product_sales"
        for x in q["commodity_exposure"]["assessment"]["exposures"]
    )


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_customer_purchase_project_is_issuer_sale_not_material_purchase(tmp_path, kind):
    i = "600035.SH"
    q, raw = delivered(
        tmp_path, i, [p for p in fixture(i)[kind] if p["page"] in {69, 70}]
    )
    for name in ["车道自助发卡机", "车道自助缴费机", "高速公路机电系统设备"]:
        actions = {r.get("action") for r in raw if r.get("object_name") == name}
        assert actions == {"sells"}
    assert len(q["commodity_exposure"]["assessment"]["exposures"]) == 3


@pytest.mark.parametrize("repair_version", ["v35", "v36"])
def test_successor_activates_five_cumulative_repairs_without_changing_default(
    repair_version,
):
    identity = default_processing_identity()
    assert "revenue_sentence_repair" not in identity
    for flag in [
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ]:
        assert getattr(selection, flag)(
            {**identity, "revenue_sentence_repair": repair_version}
        )


@pytest.mark.parametrize("location", ["current", "prior", "unreconciled"])
def test_sparse_income_uses_both_year_totals_without_layout(tmp_path, location):
    pages = fixture("600073.SH")["pages"]
    p = next(p for p in pages if p["page"] == 241)
    assert "layout_text" not in p
    original = (
        "合计 1,303,874,406.70 1,134,480,974.41 1,390,550,886.48 1,226,712,859.00"
    )
    if location == "prior":
        p["text"] = p["text"].replace(
            original,
            "合计 1,303,873,935.00 1,134,480,974.41 1,390,551,358.18 1,226,712,859.00",
            1,
        )
    elif location == "unreconciled":
        p["text"] = p["text"].replace(original, original.replace("406.70", "405.70"), 1)
    q, raw = delivered(tmp_path, "600073.SH", pages)
    records = [
        r
        for r in raw
        if r["object_type"] in {"Segment", "Measurement"}
        and r["subject_scope"] == "issuer"
        and r["source_native"]["name"] == "其他业务"
        and r["source_native"]["value"] == "471.70"
    ]
    if location == "current":
        assert {r["object_type"] for r in records} == {"Segment", "Measurement"}
        assert {r["record_id"] for r in records} <= {
            r["record_id"] for r in q["accepted_facts"]
        }
    else:
        assert not records


@pytest.mark.parametrize("replacement", ["计划完成", "尚未完成", "第三方完成"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_planned_or_third_party_launch_does_not_borrow_current_sale(
    tmp_path, replacement, kind
):
    i = "600073.SH"
    pages = fixture(i)[kind]
    for p in pages:
        if p["page"] == 22:
            p["text"] = (
                p["text"]
                .replace("完成\n多口味", replacement + "\n多口味")
                .replace("完成\r\n多口味", replacement + "\r\n多口味")
            )
    q, raw = delivered(tmp_path, i, pages)
    assert not any(
        r.get("object_name")
        in {"大白兔花生太妃糖", "巴旦木太妃糖", "软牛轧糖", "脆牛轧糖", "棒棒糖"}
        and r.get("action") == "sells"
        for r in raw
    )
    assert any(
        r.get("object_name") == "大白兔奶糖" and r.get("action") == "sells" for r in raw
    )
    assert q["production_authorization"] == "not_authorized"
