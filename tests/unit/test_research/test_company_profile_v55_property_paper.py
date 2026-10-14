"""Actual property/paper sources preserve empty columns and business identities."""

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v55_property_paper_pages.json"
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
    if r.get("reported_period") != v["reported_period"]:
        return False
    if (
        v["page"] not in pages(r)
        or n.get("unit") != v["unit"]
        or n.get("value") is None
        or num(n["value"]) != num(v["value"])
    ):
        return False
    if co(n["name"]).removeprefix("其中：") != co(v["name"]):
        return False
    if r["subject_scope"] != (
        "business_segment"
        if v["subject_scope"] == "group_associated_entity"
        else v["subject_scope"]
    ):
        return False
    rh = v["header"]
    if rh.startswith("主营业务分"):
        return d == v["dimension"] and h == "主营业务收入"
    if rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        return h == "营业收入" and d == "business_type"
    if rh == "主要控股参股公司分析":
        return h == "控股参股公司/营业收入" and q == co("子公司：" + v["source_actor"])
    if rh == "重要非全资子公司的主要财务信息":
        return h == "重要非全资子公司/本期营业收入" and q == co(
            "原子公司：" + v["source_actor"]
        )
    if rh == "重要联营企业的主要财务信息":
        return (
            h in ["重要联营企业/营业收入", "重要联营企业/本期营业收入"]
            and co(v["source_actor"]) in q
        )
    if rh == "作为出租人的经营租赁":
        return h == "作为出租人的经营租赁/租赁收入" and d in [
            "service",
            "lease",
            "business_type",
        ]
    if rh == "作为出租人的经营租赁/可变租赁收入":
        return "可变" in h
    if rh == "关联租赁/本公司作为出租方":
        return h == "本公司作为出租方/本期确认的租赁收入" and q == co(
            "承租方：" + v["counterparty"]
        )
    if rh == "关联交易/出售商品及提供劳务":
        return h == "出售商品/提供劳务/本期发生额" and q == co(
            "交易对方：" + v["counterparty"]
        )
    if rh == "报告期内房地产销售和结转情况":
        return h == "房地产销售和结转/结转收入金额"
    if "合同收入分解/" in rh:
        return (
            h.startswith("合同收入/")
            and ("原收入列：" + co(v.get("source_segment") or ""))
            in q.replace("其他-分部", "其他分部")
            and (
                "经营地区" in h
                if v["dimension"] == "region"
                else "商品类型" in h
                if v["dimension"] == "product"
                else True
            )
        )
    if rh == "报告分部的财务信息":
        return "分部" in h and d in ["industry", "business_segment"]
    return False


def rolematch(r, v):
    if co(r["source_native"]["name"]) not in {co(a) for a in v["native_aliases"]}:
        return False
    expected = (
        "business_segment"
        if v["subject_scope"] == "group_associated_entity"
        else v["subject_scope"]
    )
    if r["subject_scope"] != expected:
        return False
    if v["state"] == "disclosed_report_date_associated_business":
        q = co(r["source_native"].get("qualifier"))
        if not all(
            term in q for term in ["报告披露日", "未纳入合并", "不视为集团本期经营"]
        ):
            return False
    elif r.get("reported_period") != v["reported_period"]:
        return False
    if v["source_actor"] not in ["公司及合并子公司", "公司子公司"]:
        if expected == "issuer":
            if r.get("source_actor") not in [
                None,
                "本公司",
                "母公司",
                v["source_actor"],
            ]:
                return False
        elif co(r.get("source_actor")) not in {co(a) for a in v["actor_aliases"]}:
            return False
    acts = [
        {"procures": "purchases", "manufactures": "produces"}.get(a, a)
        for a in v["source_actions"]
    ]
    if not (
        r.get("relation_type") == "material_input"
        if acts == ["uses_material"]
        else r.get("action") in acts
    ):
        return False
    if not ({v["page"], *v.get("context_pages", [])} & pages(r)):
        return False
    cp = re.search(
        r"(?:原供应方|供应方|向)(.+?公司(?:厦门分公司)?)(?=实际|采购|；|$)",
        v.get("qualifier") or "",
    )
    return not (
        cp
        and ("实际本期出售" in v["qualifier"] or "供应方" in v["qualifier"])
        and co(cp[1]) not in co(r["source_native"].get("qualifier"))
    )


def negative_violations(n, rs, ds):
    relevant = [r for r in rs if n["page"] in pages(r)]
    fin = [r for r in relevant if r["object_type"] in ["Segment", "Measurement"]]
    acts = [r for r in relevant if r["object_type"] in ["Activity", "Relationship"]]
    name = n["name"]
    bad = []
    if name in ["仅上期关联销售", "仅上期商标使用费", "仅上期采购", "污水设备本期空"]:
        bad = [
            r
            for r in fin + acts
            if co(n["content"]) == co(r["source_native"]["name"])
            and (
                co("交易对方：" + n["counterparty"])
                == co(r["source_native"].get("qualifier") or "")
                or n["name"] == "仅上期采购"
                and not r["source_native"].get("qualifier")
            )
            and (
                r in acts
                or n.get("prior_value")
                and r["source_native"].get("value")
                and num(r["source_native"]["value"]) == num(n["prior_value"])
            )
        ]
    elif n.get("values"):
        bad = [
            r
            for r in fin
            if r["source_native"].get("value")
            and num(r["source_native"]["value"]) in [num(v) for v in n["values"]]
        ]
    elif name in ["本期收入空", "本期结转收入空", "研究分部收入空"]:
        bad = [r for r in fin if co(n["content"]) == co(r["source_native"]["name"])]
    elif name in ["子公司本期营业收入空", "杭州普润星融本期收入空", "金控营业收入空"]:
        bad = [r for r in fin if co(n["actor"]) in co(str(r["source_native"]))]
    elif name in [
        "合营章节不适用",
        "合营联营财务不适用",
        "集团合同分解不适用",
        "母公司合同分解不适用",
        "关联销售不适用",
    ]:
        cue = {
            "合营章节不适用": "合营企业/营业收入",
            "合营联营财务不适用": "企业/营业收入",
            "集团合同分解不适用": "合同收入/",
            "母公司合同分解不适用": "合同收入/",
            "关联销售不适用": "出售商品/提供劳务/本期发生额",
        }[name]
        bad = [r for r in fin if cue in (r["source_native"].get("header") or "")]
    elif name == "注销边界":
        bad = [
            r
            for r in acts
            if any(co(a) in co(r.get("source_actor") or "") for a in n["actors"])
            and not re.search("注销|退出", co(r["source_native"].get("qualifier")))
        ]
    elif name == "经营范围与计划非当前动作":
        bad = [
            r
            for r in acts
            if co(n["actor"]) in co(r.get("source_actor") or "")
            and r.get("action") in ["produces", "sells"]
        ]
    elif name == "参股及筹备边界":
        bad = [
            r
            for r in acts
            if any(co(a) in co(r.get("source_actor") or "") for a in n["actors"])
            and r["subject_scope"] == "consolidated_group"
        ]
    elif name == "下游用途不是公司制造":
        bad = [
            r
            for r in acts
            if n["content"] in r["source_native"]["name"]
            and r.get("action") == "produces"
        ]
    elif name == "研发注册非销售":
        bad = [
            r
            for r in acts
            if n["content"] in r["source_native"]["name"] and r.get("action") == "sells"
        ]
    elif name == "采购不反推投入":
        bad = [
            r
            for r in acts
            if r.get("relation_type") == "material_input"
            and re.search("燃料|设备", r["source_native"]["name"])
        ]
    elif name == "冲突单位不得确定收入化":
        bad = [
            r
            for r in fin
            if r["source_native"].get("header") == "房地产出租/租金收入"
            and not any("unclear" in u for u in r.get("uncertainty", []))
        ]
    else:
        raise AssertionError(name)
    return [r["record_id"] for r in bad]


@pytest.mark.parametrize("instrument", ["600094.SH", "600103.SH"])
def test_actual_official_input_delivers_frozen_income_actions_and_boundaries(
    tmp_path, instrument
):
    bundle = json.loads(FIXTURE.read_text())
    scope = bundle["contract"]
    source = bundle["official_inputs"][instrument]
    queried, exported = _drive(
        tmp_path / instrument,
        instrument=instrument,
        fixture=source["report"],
        pages=source["pages"],
        repair_version="v55",
    )
    assert queried == exported
    ck = json.loads(
        next(
            (tmp_path / instrument / "company_profile_common_core.v1/checkpoints").glob(
                "*.json"
            )
        ).read_text()
    )
    rs = list(_iter_accepted_raw(ck))
    for condition in scope["source_rows"][instrument]:
        assert any(r["object_type"] == "Segment" and match(r, condition) for r in rs), (
            condition
        )
    for condition in scope["native_roles"]:
        if condition["instrument_id"] == instrument:
            assert any(rolematch(r, condition) for r in rs), condition
    ds = queried["dimensions"]
    for condition in scope["negative_conditions"]:
        if condition["instrument_id"] == instrument:
            assert not negative_violations(condition, rs, ds), condition
    for answer in ds:
        text = co(answer["excerpt"])
        for group in scope["answers"][instrument][answer["dimension_id"]][
            "required_groups"
        ]:
            # Original concrete policy "服务提供数量" states the service quantity
            # condition; this semantic match preserves the frozen requirement.
            assert any(
                co(term) in text or term == "服务数量" and "服务提供数量" in text
                for term in group
            ), group
    for condition in scope["source_uncertainty_conditions"]:
        if condition["instrument_id"] != instrument:
            continue
        pair = [
            r
            for r in rs
            if r["object_type"] in ["Segment", "Measurement"]
            and r["source_native"]["name"] == condition["name"]
            and r["source_native"].get("qualifier") == "原地区：" + condition["region"]
        ]
        assert {r["object_type"] for r in pair} == {"Segment", "Measurement"}
        for record in pair:
            assert num(record["source_native"]["value"]) == num(
                condition["printed_value"]
            )
            assert record["source_native"]["unit"] == condition["printed_unit"]
            assert any(
                "official_income_unit_conflict:unclear" in u
                for u in record["uncertainty"]
            )
            assert all(
                record["record_id"] not in a["supporting_record_ids"] for a in ds
            )
            from research.company_profile.acceptance_policy import usage_policy
            from research.company_profile.models import SEMANTIC_RECORD_ADAPTER

            policy = usage_policy(
                SEMANTIC_RECORD_ADAPTER.validate_json(json.dumps(record))
            )
            assert policy == {
                "display": True,
                "consolidated_aggregation": False,
                "ranking": False,
                "cross_company_comparison": False,
                "restriction_reason": "unclear_official_income_unit",
            }
            published = next(
                f
                for f in queried["accepted_facts"]
                if f["record_id"] == record["record_id"]
            )
            assert published["uncertainty"] == record["uncertainty"]
    if instrument == "600094.SH":
        assert not any(r["source_native"]["name"] == "映辰（毛家" for r in rs)
        notes = [
            r
            for r in rs
            if r["object_type"] == "BusinessOverview"
            and r["source_text"].startswith("注3：")
        ]
        assert notes
        for note in notes:
            assert note["subject_scope"] == "business_segment"
            assert "未纳入合并" in note["source_text"]
            assert "不视为集团本期经营" in note["source_native"]["qualifier"]
            assert "重大的非股权投资" not in note["source_text"]
    else:
        assert not any(
            r.get("action") == "produces"
            and r["source_native"]["name"]
            in ["森林培育", "商品流通", "发展", "超声波技术研究"]
            for r in rs
        )


def test_wrapped_trade_row_keeps_original_identity():
    from research.company_profile import core_evidence_selection as selection

    bundle = json.loads(FIXTURE.read_text())
    page = next(
        p for p in bundle["official_inputs"]["600103.SH"]["pages"] if p["page"] == 22
    )
    rows = selection._native_note_income_rows(page["text"], "贸易业务收入")
    assert any(r[1:3] == ("木竹片及木浆产品贸易业务", "60.15") for r in rows)
    assert not any(r[1] == "易业务" for r in rows)
    assert set(selection._company_trade_sales_product_names(page["text"])) == {
        "木竹片",
        "木浆产品",
    }


def test_positive_labour_and_zero_goods_remain_separate():
    from tests.unit.test_research.test_company_profile_v54_media_machinery import (
        negative_violations as old_negative,
    )

    condition = {
        "name": "零采购不推动作",
        "page": 176,
        "counterparties": ["江苏林海智能装备有限公司"],
    }
    labour = {
        "record_id": "current-labour",
        "object_type": "Activity",
        "action": "purchases",
        "evidence": [{"page": 176}],
        "source_native": {
            "name": "接受劳务",
            "qualifier": "交易对方：江苏林海智能装备有限公司",
        },
    }
    goods = {
        **labour,
        "record_id": "forbidden-zero-goods",
        "source_native": {**labour["source_native"], "name": "商品"},
    }
    assert not old_negative(condition, [labour], [])
    assert old_negative(condition, [goods], []) == ["forbidden-zero-goods"]


def test_wrapped_purchase_services_do_not_become_goods():
    from research.company_profile import core_evidence_selection as selection

    bundle = json.loads(FIXTURE.read_text())
    ps = bundle["official_inputs"]["600103.SH"]["pages"]
    text = "\n".join(
        p.get("layout_text") or p["text"] for p in ps if p["page"] in [199, 200]
    )
    bindings = selection._related_current_goods_bindings(text)
    service = [b for b in bindings if "工程项目承包及其他咨询" in b["name"]]
    assert service and all(b["header"] == "本期采购服务" for b in service)
    soaps = [b for b in bindings if b["name"] == "手工皂、洗衣液"]
    assert soaps and all(b["header"] == "本期采购原内容" for b in soaps)
