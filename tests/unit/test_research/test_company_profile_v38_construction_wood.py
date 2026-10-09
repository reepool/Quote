"""Complete owner/PDF pages deliver native income, actors and current actions."""

import copy
import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v38_construction_wood_frozen_pages.json"
)


def compact(s):
    return (
        re.sub(r"\s+", "", s or "")
        .replace("（", "(")
        .replace("）", ")")
        .replace("：", ":")
    )


def fixture(i):
    return json.loads(FIXTURE.read_text())[i]


def delivered(root, i, pages):
    q, e = _drive(
        root, instrument=i, fixture=fixture(i), pages=pages, repair_version="v38"
    )
    assert q == e
    c = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return q, list(_iter_accepted_raw(c))


def page_numbers(r):
    return {
        p for e in r["evidence"] for p in [e["page"], *e.get("continuation_pages", [])]
    }


def decimal(s):
    return Decimal(str(s).replace(",", ""))


@pytest.mark.parametrize("i", ["600039.SH", "600076.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_pages_deliver_substance_income_and_roles(tmp_path, i, kind):
    f = fixture(i)
    q, raw = delivered(tmp_path, i, f[kind])
    for dim, rule in f["answers"].items():
        body = compact(_answer(q, dim))
        assert all(
            any(compact(t) in body for t in g) for g in rule["required_groups"]
        ), dim
        if i == "600039.SH":
            for text in [
                "成德绵",
                "成自泸",
                "内威荣",
                "自隆",
                "江习古",
                "2024年末完成出表",
                "参股",
                "植物租摆",
            ]:
                if (
                    text in {"2024年末完成出表", "参股", "植物租摆"}
                    or dim != "revenue_model"
                ):
                    assert text in body, (dim, text)
        else:
            for text in [
                "湖北天欣木结构房制造有限公司",
                "2025/9/30",
                "丧失控制权",
                "不再纳入公司财务报表合并范围",
            ]:
                assert text in body, (dim, text)
    if i == "600039.SH":
        body = compact(_answer(q, "revenue_model"))
        for text in [
            "主要责任人",
            "代理人",
            "PPP",
            "BOT",
            "BT",
            "长期应收款",
            "实际利率法",
            "实际完成的工作量和结算单价",
        ]:
            assert text in body
    else:
        body = compact(_answer(q, "revenue_model"))
        for text in [
            "报价",
            "投标",
            "验收",
            "先付款后提货",
            "先发货后收款",
            "购买人自行办理运输",
            "共同确认销售数量",
            "装车交付",
            "投入法",
            "直线法",
        ]:
            assert text in body
    for row in f["source_rows"]:
        names = set(map(compact, row["native_aliases"]))
        dims = {
            row["dimension"],
            {"report_segment": "business_segment", "service": "lease"}.get(
                row["dimension"], row["dimension"]
            ),
        }
        for model in ["Segment", "Measurement"]:
            matches = [
                r
                for r in raw
                if r["object_type"] == model
                and compact(r["source_native"]["name"]) in names
                and r["source_native"]["value"] is not None
                and decimal(r["source_native"]["value"]) == decimal(row["value"])
                and r["source_native"]["unit"] == row["unit"]
                and row["page"] in page_numbers(r)
                and (r.get("dimension") or r.get("segment_dimension")) in dims
            ]
            assert matches, (model, row)
            if row["counterparty"]:
                assert any(
                    compact(r["source_native"].get("qualifier")).split(":")[-1]
                    == compact(row["counterparty"])
                    for r in matches
                ), row
            if row["source_actor"] == "母公司":
                assert all(r["subject_scope"] == "issuer" for r in matches)
            if row["dimension"] == "adjustment":
                assert all(
                    r["row_class"] == "consolidation_adjustment" for r in matches
                )
    for role in f["native_roles"]:
        candidates = [
            r
            for r in raw
            if compact(r["source_native"]["name"])
            in set(map(compact, role["native_aliases"]))
            and (r.get("action") or r.get("relation_type"))
            in role["source_actions"] + role["source_relation_types"]
            and role["page"] in page_numbers(r)
        ]
        assert candidates, role
        if role["source_actor"] != "公司及合并子公司":
            assert all(
                r.get("source_actor") == role["source_actor"] for r in candidates
            )
        assert any(
            x["role"] == role["role"]
            and any(r["record_id"] in x["source_record_ids"] for r in candidates)
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        ), role
    assert not any(r.get("object_name") == "业务规模保持在合理区间" for r in raw)
    if i == "600039.SH":
        rentals = [
            r
            for r in raw
            if r["object_type"] == "Segment" and r["dimension"] == "lease"
        ]
        assert len(rentals) == 9  # Twenty prior-only rows stay absent.
        assert all(
            not any(n in r["source_native"]["name"] for n in ["销售费用", "研发投入"])
            for r in raw
        )
    else:
        assert not any(
            r.get("action") == "purchases"
            and r["source_native"]["name"] in {"意杨", "竹材"}
            for r in raw
        )
        assert not any(
            r.get("relation_type") == "material_input"
            and r["source_native"]["name"] == "箱板"
            for r in raw
        )


@pytest.mark.parametrize("modifier", ["尚未销售", "拟销售"])
@pytest.mark.parametrize("branch", ["products", "deduction", "subsidiary", "input"])
def test_complete_page_action_modifiers_do_not_borrow_other_branches(
    tmp_path, modifier, branch
):
    pages = copy.deepcopy(fixture("600076.SH")["pages"])
    if branch == "products":
        p = next(p for p in pages if p["page"] == 14)
        p["text"] = p["text"].replace("研发、生产和销", "研发、生产和" + modifier)
        names, action = (
            {"全木复合集装箱地板", "COSB复合集装箱地板", "竹木复合集装箱地板"},
            "sells",
        )
    elif branch == "deduction":
        p = next(p for p in pages if p["page"] == 12)
        # Only the current explanation changes; prior-year affirmative text stays.
        p["text"], changed = re.subn(
            r"废料\s*销\s*售", "废料" + modifier, p["text"], count=1
        )
        assert changed == 1
        names, action = {"废料"}, "sells"
    elif branch == "subsidiary":
        p = next(p for p in pages if p["page"] == 27)
        p["text"] = p["text"].replace("生产销售：", "生产" + modifier + "：")
        names, action = {"五金配件", "胶合板"}, "sells"
    else:
        p = next(p for p in pages if p["page"] == 18)
        p["text"] = p["text"].replace(
            "提升意杨", ("尚未提升" if modifier == "尚未销售" else "拟提升") + "意杨"
        )
        names, action = {"意杨", "竹材"}, "material_input"
    q, raw = delivered(tmp_path, "600076.SH", pages)
    assert not any(
        compact(r["source_native"]["name"]) in names
        and (r.get("action") or r.get("relation_type")) == action
        for r in raw
    )
    ids = {
        r["record_id"]
        for r in raw
        if compact(r["source_native"]["name"]) in names
        and (r.get("action") or r.get("relation_type")) == action
    }
    assert not any(
        ids.intersection(x["source_record_ids"])
        for x in q["commodity_exposure"]["assessment"]["exposures"]
    )
    assert any(
        r.get("action") == "sells" and r["source_native"]["name"] == "建筑模板"
        for r in raw
    )
    if branch != "deduction":
        assert any(
            r.get("action") == "sells" and r["source_native"]["name"] == "电力"
            for r in raw
        )


def test_successor_enables_five_flags_without_changing_default():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v38"}
    for name in [
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ]:
        assert getattr(selection, name)(identity)
    assert default_processing_identity().get("revenue_sentence_repair") != "v38"
