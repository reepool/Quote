"""Complete official media/machinery pages retain identity and native directions."""

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v54_media_machinery_pages.json"
)
co = lambda s: re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")
num = lambda s: Decimal(co(s).replace(",", ""))


def pages(r):
    return {
        p
        for e in r["evidence"]
        for p in [
            e.get("page"),
            *e.get("continuation_pages", []),
            *e.get("subject_evidence_pages", []),
        ]
        if p is not None
    }


def match(r, v):
    n = r["source_native"]
    h = n.get("header") or ""
    q = co(n.get("qualifier"))
    d = r.get("dimension", r.get("segment_dimension"))
    rh = v["header"]
    name = co(n["name"])
    expected = co(v["name"])
    if (
        v["page"] not in pages(r)
        or n.get("unit") != v["unit"]
        or num(n.get("value")) != num(v["value"])
        or r["subject_scope"]
        != (
            "business_segment"
            if v["subject_scope"] == "group_associated_entity"
            else v["subject_scope"]
        )
    ):
        return False
    # The native mother-company prefix is a row marker; it does not relax other names.
    if name.removeprefix("其中：") != expected:
        return False
    if (
        v["source_actor"] != "公司及合并子公司"
        and v["subject_scope"] not in ["issuer", "consolidated_group"]
        and q
        not in {
            co("子公司：" + v["source_actor"]),
            co("原子公司：" + v["source_actor"]),
            co("参股公司：" + v["source_actor"]),
            co("联营公司：" + v["source_actor"]),
        }
    ):
        return False
    if rh == "出售商品/提供劳务":
        return (
            d in ["product", "service"]
            and h == "出售商品/提供劳务/本期发生额"
            and q == co("交易对方：" + (v["counterparty"] or "合计"))
        )
    if rh == "主要控股参股公司分析":
        return (
            d in ["business_segment", "business_type"] and h == "控股参股公司/营业收入"
        )
    if rh == "重要非全资子公司的主要财务信息":
        return (
            d in ["business_segment", "business_type"]
            and h == "重要非全资子公司/本期营业收入"
        )
    if rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        return d == "business_type" and h == "营业收入"
    if rh.startswith("主营业务分"):
        return d == v["dimension"] and h == "主营业务收入"
    if rh.startswith("合同收入分解/"):
        if not q.startswith("原收入列：") or not q.endswith("营业收入"):
            return False
        if rh.endswith("/商品类型"):
            return d == "product" and h == "合同收入/商品类型"
        if rh.endswith("/经营地区"):
            return (
                d == "region"
                and h in ["合同收入/按经营地区分类", "合同收入/经营地区"]
                or name == "合计"
                and d == "business_segment"
                and h == "合同收入/合同分解合计"
            )
        if rh.endswith("/商品转让时间"):
            return d == "revenue_timing" and h in [
                "合同收入/按商品转让的时间分类",
                "合同收入/收入确认时间",
                "合同收入/商品转让时间",
            ]
        if rh.endswith("/合计"):
            return name == "合计" and (
                d == "revenue_timing"
                and h == "合同收入/按商品转让的时间分类"
                or d == "business_segment"
                and h == "合同收入/合同分解合计"
            )
    if rh == "作为出租人的经营租赁/租赁收入":
        return d in ["service", "lease", "business_type"] and h == rh
    return False


def action_match(r, v):
    if co(r["source_native"]["name"]) not in {co(x) for x in v["native_aliases"]} or r[
        "subject_scope"
    ] != (
        "business_segment"
        if v["subject_scope"] == "group_associated_entity"
        else v["subject_scope"]
    ):
        return False
    if v["source_actor"] != "公司及合并子公司" and co(r.get("source_actor")) not in {
        co(x) for x in v["actor_aliases"]
    }:
        return False
    return (
        r.get("relation_type") == "material_input"
        if v["source_actions"] == ["material_input"]
        else r.get("action") in v["source_actions"]
    )


def negative_violations(n, rs, ds):
    fin = [r for r in rs if r["object_type"] in ["Segment", "Measurement"]]
    acts = [r for r in rs if r["object_type"] in ["Activity", "Relationship"]]
    name = n["name"]
    bad = []
    relevant = [r for r in rs if n["page"] in pages(r)]
    if name == "仅上期关联销售":
        bad = [
            r
            for r in relevant
            if r in fin and num(r["source_native"]["value"]) == num(n["prior_value"])
        ]
    elif "forbidden_headers" in n:
        bad = [
            r
            for r in relevant
            if r in fin
            and any(
                h in (r["source_native"].get("header") or "")
                for h in n["forbidden_headers"]
            )
        ]
    elif name == "两联营营业收入本期空":
        bad = [
            r
            for r in relevant
            if r in fin
            and any(a in co(str(r["source_native"])) for a in n["actors"])
            and "营业收入" in (r["source_native"].get("header") or "")
        ]
    elif name == "受托无费用":
        bad = [
            r
            for r in relevant
            if r in fin and re.search("佣金|管理费", r["source_native"]["name"])
        ]
    elif name in [
        "未履约非本期收入",
        "母公司未履约非收入",
        "承租费用非收入",
        "行业销量非公司销量",
    ]:
        bad = [
            r
            for r in relevant
            if r in fin
            and num(r["source_native"]["value"]) in [num(v) for v in n["values"]]
        ]
    elif name in ["联营注销边界", "注销采购打印零"]:
        bad = [
            r
            for r in relevant
            if r in acts
            and n["actor"]
            in str(r.get("source_actor", ""))
            + str(r["source_native"].get("qualifier", ""))
            and r.get("action") in ["sells", "produces", "purchases", "operates"]
        ]
    elif name == "零销售不推当前销售":
        bad = [
            r
            for r in relevant
            if r in acts
            and r.get("action") == "sells"
            and n["counterparty"] in str(r["source_native"].get("qualifier", ""))
        ]
    elif name == "零采购不推动作":
        # The same supplier has positive current labour and zero goods on
        # separate native rows; the zero-goods condition cannot reject labour.
        bad = [
            r
            for r in relevant
            if r in acts
            and r.get("action") == "purchases"
            and r["source_native"]["name"] in {"商品", "采购商品"}
            and any(
                p in str(r["source_native"].get("qualifier", ""))
                for p in n["counterparties"]
            )
        ]
    elif name == "接受劳务不成商品":
        bad = [
            r
            for r in relevant
            if r in acts
            and r["source_native"]["name"] == "接受劳务"
            and (
                r.get("action") in ["produces", "sells"]
                or r.get("relation_type") == "material_input"
            )
        ]
    elif name == "时段打印零不是空":
        bad = [
            r for r in relevant if r in acts and "时段" in r["source_native"]["name"]
        ]
    elif name == "参股不集团化":
        bad = [
            r
            for r in relevant
            if any(
                a
                in str(r.get("source_actor", ""))
                + str(r["source_native"].get("qualifier", ""))
                + str(r["source_native"]["name"])
                for a in n["actors"]
            )
            and r["subject_scope"]
            in ["issuer", "consolidated_group", "named_subsidiary"]
        ]
    elif "names" in n:
        bad = [
            r
            for r in relevant
            if r in acts and any(x == r["source_native"]["name"] for x in n["names"])
        ]
    else:
        raise ValueError(name)
    return [r["record_id"] for r in bad]


@pytest.mark.parametrize("instrument", ["600088.SH", "600099.SH"])
def test_complete_official_contract(tmp_path, instrument):
    f = json.loads(FIXTURE.read_text())[instrument]
    q, e = _drive(
        tmp_path,
        instrument=instrument,
        fixture=f,
        pages=f["pages"],
        repair_version="v54",
    )
    assert q == e
    raw = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    tmp_path.glob("company_profile_common_core.v1/checkpoints/*.json")
                ).read_text()
            )
        )
    )
    for kind in ("Segment", "Measurement"):
        assert all(
            any(r["object_type"] == kind and match(r, v) for r in raw)
            for v in f["source_rows"]
        )
    assert all(
        any(
            r["object_type"] in ["Activity", "Relationship"] and action_match(r, v)
            for r in raw
        )
        for v in f["native_roles"]
    )
    assert all(
        not negative_violations(n, raw, q["dimensions"])
        for n in f["negative_conditions"]
    )
    for d in q["dimensions"]:
        assert d["answered"]
        assert all(
            any(co(t) in co(d["excerpt"]) for t in g)
            for g in f["answers"][d["dimension_id"]]["required_groups"]
        )
    assert not any(
        "371,136,597.51308,563,992.74" in r["source_native"]["name"]
        or "机械摩托车" == r["source_native"]["name"]
        for r in raw
    )
    if instrument == "600088.SH":
        parties = {
            r["source_native"].get("qualifier")
            for r in raw
            if r["object_type"] == "Segment"
            and r["source_native"]["name"] == "基金业务"
        }
        assert "交易对方：中视融合（上海）企业管理合伙企业（有限合伙）" in parties
        assert "交易对方：宁波梅山保税港区琦林股权投资合伙企业（有限合伙）" in parties
    else:
        actors = [
            r
            for r in raw
            if r["object_type"] in ["Activity", "Segment"]
            and "联海动力" in str(r["source_native"]) + str(r.get("source_actor", ""))
        ]
        assert actors and all(
            r["subject_scope"] == "business_segment" for r in actors if 31 in pages(r)
        )


@pytest.mark.parametrize("version", ["v54", "v55", "v56"])
def test_successor_retains_default_and_five_switches(version):
    default = default_processing_identity()
    successor = {**default, "revenue_sentence_repair": version}
    for name in (
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ):
        assert getattr(selection, name)(successor)
        assert not getattr(selection, name)(default)
    assert default_processing_identity() == default


@pytest.mark.parametrize("state", ["拟", "尚未"])
def test_future_product_matrix_does_not_create_current_actions(state):
    text = f"公司{state}从事的主营业务是机械等及以上产品配件的制造和销售。主要产品如下：消防泵"
    assert not selection._owned_product_action_bindings(text)
