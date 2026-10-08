"""Full owner/PDF regression for current income, row identity and action bounds."""

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
    / "fixtures/company_profile_v33_highway_wind_frozen_pages.json"
)


def _compact(s):
    return re.sub(r"\s+", "", s or "")


def _fixture(i):
    return json.loads(FIXTURE.read_text())[i]


def _delivered(tmp_path, i, pages):
    q, e = _drive(
        tmp_path, instrument=i, fixture=_fixture(i), pages=pages, repair_version="v33"
    )
    assert q == e
    checkpoint = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return q, {r["record_id"]: r for r in _iter_accepted_raw(checkpoint)}


@pytest.mark.parametrize("i", ["600033.SH", "600072.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_owned_pages_deliver_income_bodies_and_actions(tmp_path, i, kind):
    f = _fixture(i)
    q, raw = _delivered(tmp_path, i, f[kind])
    for dim, rule in f["answers"].items():
        assert all(
            any(_compact(s) in _answer(q, dim) for s in group)
            for group in rule["required_groups"]
        ), dim
    for row in f["source_rows"]:
        for record_type in ["Segment", "Measurement"]:
            matches = [
                r
                for r in raw.values()
                if r["object_type"] == record_type
                and _compact(r["source_native"]["name"])
                in set(map(_compact, row["native_aliases"]))
                and r["source_native"]["value"] == row["value"]
                and r["source_native"]["unit"] == row["unit"]
            ]
            assert matches, row
            assert any(
                (r["subject_scope"] == "issuer") == (row["source_actor"] == "母公司")
                for r in matches
            ), row
            if row.get("counterparty"):
                if row["header"].startswith("本公司作为出租方"):
                    assert any(
                        r["source_native"].get("qualifier")
                        == "承租方：" + row["counterparty"]
                        for r in matches
                    ), row
                else:
                    assert any(
                        _compact(row["counterparty"])
                        in _compact(r["source_native"].get("qualifier"))
                        for r in matches
                    ), row
    for role in f["native_roles"]:
        matches = [
            r
            for r in q["commodity_exposure"]["assessment"]["exposures"]
            if r["role"] == role["role"]
            and r["source_native_name"] in role["native_aliases"]
        ]
        assert matches, role
        assert any(
            raw[oid].get("action") in role["source_actions"]
            for m in matches
            for oid in m["source_record_ids"]
        )
    assert not any(
        r["source_native"]["value"] == "759,601,816.76" for r in raw.values()
    )
    assert not any(
        r["object_type"] in {"Segment", "Measurement"}
        and r["source_native"]["name"]
        in {"印花税", "房产税", "职工薪酬", "差旅费", "招标费用"}
        for r in raw.values()
    )
    assert not any(
        r["object_type"] == "Activity" and "叶片的设计" in r.get("object_name", "")
        for r in raw.values()
    )
    if i == "600072.SH":
        leaves = [
            r
            for r in raw.values()
            if r["object_type"] == "Activity" and r.get("object_name") == "叶片"
        ]
        assert leaves and all(
            r["source_actor"] == "双瑞复材" and r["action"] == "produces"
            for r in leaves
        )
    else:
        rents = [
            r
            for r in raw.values()
            if r["object_type"] == "Segment"
            and r["source_native"]["name"] == "房屋租赁"
        ]
        assert len({r["record_id"] for r in rents}) == 13
        assert not any(
            r["source_native"]["value"] == "788,834.86" for r in raw.values()
        )


@pytest.mark.parametrize(
    "replacement",
    ["公司不从事风机配件销售", "公司拟开展风机配件销售", "第三方公司风机配件销售"],
)
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_negative_sales_do_not_borrow_other_page_actions(tmp_path, replacement, kind):
    i = "600072.SH"
    pages = _fixture(i)[kind]
    for p in pages:
        p["text"] = p["text"].replace("公司风机配件销售", replacement)
    q, raw = _delivered(tmp_path, i, pages)
    assert not any(
        r.get("object_name") == "风机配件" and r.get("action") == "sells"
        for r in raw.values()
    )
    assert not any(
        r["source_native_name"] == "风机配件" and r["role"] == "product_sales"
        for r in q["commodity_exposure"]["assessment"]["exposures"]
    )
    assert any(
        r.get("object_name") == "电力" and r.get("action") == "sells"
        for r in raw.values()
    )


def test_policy_orders_and_costs_do_not_create_sales(tmp_path):
    i = "600072.SH"
    pages = [p for p in _fixture(i)["pages"] if p["page"] in {165, 235, 296, 297}]
    q, raw = _delivered(tmp_path, i, pages)
    assert not any(
        r.get("object_name") in {"船舶配件", "电站产品", "绿色甲醇", "电力"}
        and r.get("action") == "sells"
        for r in raw.values()
    )
    assert not q["commodity_exposure"]["assessment"]["exposures"]


def test_successor_flags_leave_default_identity_unchanged():
    identity = default_processing_identity()
    assert "revenue_sentence_repair" not in identity
    repaired = {**identity, "revenue_sentence_repair": "v33"}
    for name in [
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ]:
        assert getattr(selection, name)(repaired)


def test_equal_rent_values_for_distinct_tenants_remain_distinct(tmp_path):
    i = "600033.SH"
    pages = _fixture(i)["pages"]
    for p in pages:
        for field in ["text", "layout_text"]:
            if p.get(field):
                p[field] = p[field].replace("2,336,513.76", "5,527,173.17")
    _, raw = _delivered(tmp_path, i, pages)
    for kind in ["Segment", "Measurement"]:
        rows = [
            r
            for r in raw.values()
            if r["object_type"] == kind
            and r["source_native"]["name"] == "房屋租赁"
            and r["source_native"]["value"] == "5,527,173.17"
        ]
        assert len(rows) == 2
        assert len({r["source_native"]["qualifier"] for r in rows}) == 2


@pytest.mark.parametrize("modifier", ["不从事", "拟开展", "将开展"])
def test_main_sale_and_procurement_keep_local_action_status(tmp_path, modifier):
    i = "600072.SH"
    pages = _fixture(i)["pages"]
    for p in pages:
        p["text"] = (
            p["text"]
            .replace("主机销售等", modifier + "主机销售等")
            .replace("供应商采购", "供应商" + modifier + "采购")
        )
    q, raw = _delivered(tmp_path, i, pages)
    assert not any(
        r.get("object_name") == "风电主机" and r.get("action") == "sells"
        for r in raw.values()
    )
    assert not any(
        r.get("object_name") == "风机零部件" and r.get("action") == "purchases"
        for r in raw.values()
    )
    assert any(
        r["source_native_name"] == "塔筒" and r["role"] == "product_sales"
        for r in q["commodity_exposure"]["assessment"]["exposures"]
    )
