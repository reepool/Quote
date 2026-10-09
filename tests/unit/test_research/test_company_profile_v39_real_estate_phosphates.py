"""Real owner and official pages preserve native business content and direction."""

import copy
import json
import re
from pathlib import Path

import pytest

from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

PRIOR_ONLY_SERVICES = {
    ("湖北迈睿达供应链股份有限公司", "26.28"),
    ("浙江吉帅数据科技有限公司", "8.22"),
    ("靖江手拉手物业管理有限公司", "0.08"),
}

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v39_real_estate_phosphates_frozen_pages.json"
)


def compact(s):
    return re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")


def fixture(i):
    return json.loads(FIXTURE.read_text())[i]


def page_numbers(record):
    return {
        page
        for evidence in record["evidence"]
        for page in [evidence["page"], *evidence.get("continuation_pages", [])]
    }


def delivered(root, i, pages):
    q, e = _drive(
        root, instrument=i, fixture=fixture(i), pages=pages, repair_version="v39"
    )
    assert q == e
    cp = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return q, list(_iter_accepted_raw(cp))


@pytest.mark.parametrize("i", ["600048.SH", "600078.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_bodies_preserve_original_substance_and_limits(tmp_path, i, kind):
    f = fixture(i)
    q, raw = delivered(tmp_path, i, f[kind])
    for dim, rule in f["answers"].items():
        text = compact(_answer(q, dim))
        missing = [
            g for g in rule["required_groups"] if not any(compact(t) in text for t in g)
        ]
        assert not missing, (dim, missing)
    assert any(r["object_type"] == "BusinessOverview" for r in raw)
    for row in f["source_rows"]:
        matches = [
            r
            for r in raw
            if r["object_type"] == "Segment"
            and r["dimension"] == row["dimension"]
            and compact(r["source_native"]["name"])
            in set(map(compact, row["native_aliases"]))
            and compact(r["source_native"]["value"]) == compact(row["value"])
            and r["source_native"]["unit"] == row["unit"]
            and row["page"] in page_numbers(r)
            and (
                not row.get("counterparty")
                or compact(r["source_native"].get("qualifier"))
                .replace("：", ":")
                .split(":")[-1]
                == compact(row["counterparty"])
            )
        ]
        if (
            i == "600078.SH"
            and (row.get("counterparty"), row["value"]) in PRIOR_ONLY_SERVICES
        ):
            # The preserved first contract conflicts with the printed year columns.
            # Current-year output must follow the official layout, never that error.
            assert not matches, row
            assert not any(
                r["object_type"] in {"Segment", "Measurement"}
                and compact(r["source_native"]["name"])
                in set(map(compact, row["native_aliases"]))
                and compact(r["source_native"].get("qualifier"))
                .replace("：", ":")
                .split(":")[-1]
                == compact(row["counterparty"])
                for r in raw
            ), row  # Includes shifted prior values and invented current zeros.
            continue
        assert matches, (
            row["page"],
            row["name"],
            row["value"],
            row.get("counterparty"),
        )
        assert any(
            r["object_type"] == "Measurement"
            and r["source_native"] == matches[0]["source_native"]
            and row["page"] in page_numbers(r)
            for r in raw
        )
        if row["source_actor"] == "母公司":
            assert all(r["subject_scope"] == "issuer" for r in matches)
    for role in f["native_roles"]:
        candidates = [
            r
            for r in raw
            if r["object_type"] in {"Activity", "Relationship"}
            and compact(r["source_native"]["name"])
            in set(map(compact, role["native_aliases"]))
            and role["page"] in page_numbers(r)
            and (
                r.get("action") in role["source_actions"]
                if role["source_actions"]
                else r.get("relation_type") == "material_input"
            )
            and (
                r["subject_scope"] == "consolidated_group"
                if role["source_actor"] == "公司及合并子公司"
                else r.get("source_actor") == role["source_actor"]
            )
        ]
        assert candidates, (role["name"], role["source_actor"], role["source_actions"])
        assert any(
            x["source_record_ids"]
            and any(r["record_id"] in x["source_record_ids"] for r in candidates)
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        )
    assert not any(
        r["object_type"] == "Activity"
        and (
            r["source_native"]["name"] == "半导体"
            or "在内的" in r["source_native"]["name"]
            or "有限公司采购" in r["source_native"]["name"]
        )
        for r in raw
    )
    assert not any(
        r["object_type"] == "Segment"
        and r["dimension"] == "sales_mode"
        and r["label"] in {"第一名", "第二名", "第三名", "第四名", "第五名"}
        for r in raw
    )


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("modifier", ["不从事", "拟开展"])
def test_subsidiary_coal_actions_reject_negative_and_plans(tmp_path, kind, modifier):
    f = fixture("600078.SH")
    pages = copy.deepcopy([p for p in f[kind] if p["page"] in {183, 184}])
    for p in pages:
        p["text"] = p["text"].replace(
            "主要从事原煤、焦煤及矿用设备购销", modifier + "原煤、焦煤及矿用设备购销"
        )
    q, raw = delivered(tmp_path, "600078.SH", pages)
    activities = [r for r in raw if r["object_type"] == "Activity"]
    assert not any(r["source_actor"] == "宣威市荣昌煤磷有限公司" for r in activities)
    assert any(
        r["source_actor"] == "会泽龙威矿业有限公司" and r["action"] == "sells"
        for r in activities
    )
    assert not any(
        x["source_record_ids"]
        and x["source_native_name"] in {"原煤", "焦煤", "矿用设备"}
        for x in q["commodity_exposure"]["assessment"]["exposures"]
    )


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_purchase_and_use_columns_are_distinct_and_uses_not_products(tmp_path, kind):
    f = fixture("600078.SH")
    _q, raw = delivered(
        tmp_path, "600078.SH", [p for p in f[kind] if p["page"] in {25, 27}]
    )
    assert any(
        r["object_type"] == "Relationship" and r["object_name"] == "黄磷" for r in raw
    )
    assert not any(
        r["object_type"] == "Activity"
        and r["source_native"]["name"] in {"黄磷", "食品工业", "正极材料", "三氯化磷"}
        for r in raw
    )
    for name in ["磷矿", "焦丁", "电极", "电煤", "电力"]:
        assert any(
            r["object_type"] == "Activity"
            and r["source_native"]["name"] == name
            and r["action"] == "purchases"
            for r in raw
        )
        assert any(
            r["object_type"] == "Relationship"
            and r["source_native"]["name"] == name
            and r["relation_type"] == "material_input"
            for r in raw
        )


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_current_rental_rows_and_sparse_parent_income(tmp_path, kind):
    f = fixture("600048.SH")
    _q, raw = delivered(
        tmp_path,
        "600048.SH",
        [p for p in f[kind] if p["page"] in {12, 13, 128, 276, 277, 278, 341, 348}],
    )
    leases = [
        r for r in raw if r["object_type"] == "Segment" and r["dimension"] == "lease"
    ]
    assert len(leases) == 24
    assert len({r["record_id"] for r in leases}) == 24
    assert not any(
        r["source_native"]["value"]
        in {"686,963.83", "609,798.10", "1,609,158.17", "900,243.09"}
        for r in leases
    )
    parent = [
        r
        for r in raw
        if r["object_type"] == "Segment" and r["source_native"]["name"] == "其他业务"
    ]
    assert len(parent) == 1
    assert parent[0]["subject_scope"] == "issuer"
    assert parent[0]["source_native"]["value"] == "477,781,402.44"


def test_v39_identity_inherits_all_five_switches_and_default_unchanged():
    from research.company_profile import core_evidence_selection as selection
    from research.company_profile.execution import default_processing_identity

    before = default_processing_identity()
    identity = {**before, "revenue_sentence_repair": "v39"}
    assert all(
        check(identity)
        for check in [
            selection.revenue_sentence_repair_requested,
            selection.named_role_repair_requested,
            selection.service_operating_energy_requested,
            selection.core_answer_repair_requested,
            selection.source_delivery_repair_requested,
        ]
    )
    assert default_processing_identity() == before
    assert before.get("revenue_sentence_repair") != "v39"


@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_blank_deduction_item_does_not_take_following_subtotal(tmp_path, kind):
    f = fixture("600078.SH")
    q, raw = delivered(tmp_path, "600078.SH", f[kind])
    blank_name = "未形成或难以形成稳定业务模式的业务所产生的收入"
    assert not any(
        r["object_type"] in {"Segment", "Measurement"}
        and r["source_native"]["name"] == blank_name
        for r in raw
    )
    assert blank_name not in _answer(q, "revenue_model")
    for model in ["Segment", "Measurement"]:
        assert any(
            r["object_type"] == model
            and r["source_native"]["name"] == "正常经营之外的其他业务收入"
            and r["source_native"]["value"] == "26,044,735.71"
            and r["source_native"]["unit"] == "元"
            and 9 in page_numbers(r)
            for r in raw
        )
    for dimension, rule in f["answers"].items():
        text = compact(_answer(q, dimension))
        assert all(
            any(compact(token) in text for token in group)
            for group in rule["required_groups"]
        ), dimension
