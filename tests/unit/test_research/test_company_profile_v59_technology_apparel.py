"""Complete official inputs retain native income cells, actors, directions and answers."""

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v59_technology_apparel_pages.json"
)


def co(s):
    return re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")


def num(s):
    return Decimal(co(str(s)).replace(",", ""))


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
    rh = v["header"]
    d = r.get("dimension", r.get("segment_dimension"))
    sc = r["subject_scope"]
    expected = {
        "parent_company": "issuer",
        "group_associated_entity": "business_segment",
    }.get(v["subject_scope"], v["subject_scope"])
    if (
        r.get("reported_period") != v["reported_period"]
        or v["page"] not in pages(r)
        or n.get("unit") != v["unit"]
        or n.get("value") is None
        or num(n["value"]) != num(v["value"])
    ):
        return False
    if co(n["name"]) not in {
        co(x)
        for x in [
            *v["native_aliases"],
            *([v["source_segment"]] if rh == "报告分部财务信息" else []),
        ]
    }:
        return False
    if sc != expected and not (rh == "合并利润表" and sc == "unclear"):
        return False
    if v.get("counterparty") and co(v["counterparty"]) not in q:
        return False
    if rh.startswith("主营业务分"):
        return d == v["dimension"] and h in ["主营业务收入", rh]
    if rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        return h == "营业收入" and d == "business_type"
    if rh in ["合并利润表", "母公司利润表"]:
        return h in [(rh + "/" + v["name"]), v["name"], rh]
    if rh in ["合并收入分解", "母公司收入分解"] or "收入分解/" in rh:
        target = "合同收入/" + {"product": "商品类型", "region": "按经营地区分类"}.get(
            v["dimension"], "合同分解合计"
        )
        return h == target and q == co("原收入列：" + v["source_segment"] + "营业收入")
    if rh == "报告分部财务信息":
        return h == "对外营业收入" and co(v["source_segment"]) == co(n["name"])
    if rh == "作为出租人的经营租赁":
        return h == "作为出租人的经营租赁/租赁收入"
    if rh == "关联交易/出售商品及提供劳务":
        return h == "出售商品/提供劳务/本期发生额" and q == co(
            "交易对方：" + v["counterparty"]
        )
    if rh == "关联租赁/本公司作为出租方":
        return h == "本公司作为出租方/本期确认的租赁收入" and q == co(
            "承租方：" + v["counterparty"]
        )
    if rh.startswith("重要") and rh.endswith("主要财务信息"):
        relation = (
            "合营" if "合营" in rh else "联营" if "联营" in rh else "非全资子公司"
        )
        return (
            h == "重要" + relation + "企业/本期营业收入"
            and co(v["source_actor"]) in co(n["name"] + " " + q)
            if relation in ["合营", "联营"]
            else h == "重要非全资子公司/本期营业收入"
            and co(v["source_actor"]) in co(n["name"] + " " + q)
        )
    if rh == "重要非全资子公司主要财务信息":
        return h == "重要非全资子公司/本期营业收入" and co(v["source_actor"]) in q
    if rh in ["主要控股参股公司分析", "主要控股参股公司财务情况"]:
        return h == "控股参股公司/" + v["source_column"] and co(v["source_actor"]) in q
    if rh == "主要销售客户":
        return h in ["前五名客户/销售额", "前五名客户/营业收入"] and d == "customer"
    if rh == "关联受托管理":
        return h == rh and expected == "issuer"
    if rh == "终止经营营业收入":
        return h == rh and d == v["dimension"] and sc == expected
    # Any later recognized native table must retain its source heading / column.
    return (
        h == rh
        and (not v.get("source_segment") or co(v["source_segment"]) in q)
        and (v["source_actor"] == "公司及合并子公司" or co(v["source_actor"]) in q)
        and (not v.get("native_unit") or "冲突" in q and "万元" in q and "元" in q)
    )


ACTION_MAP = {
    "生产": "produces",
    "制造": "produces",
    "组装": "produces",
    "加工": "processes",
    "销售": "sells",
    "零售": "sells",
    "经销": "sells",
    "分销": "sells",
    "转售": "sells",
    "开发": "develops",
    "提供": "provides_service",
    "服务": "provides_service",
    "安装": "provides_service",
    "维护": "provides_service",
    "修理": "provides_service",
    "技术支持": "provides_service",
    "安调": "provides_service",
    "升级": "provides_service",
    "租赁": "provides_service",
    "经营": "operates",
    "投运": "operates",
    "应用": "operates",
    "挂牌": "operates",
    "种植": "produces",
}


def rolematch(r, v):
    n = r["source_native"]
    q = co(n.get("qualifier"))
    expected = {
        "group_associated_entity": "business_segment",
        "parent_company": "issuer",
    }.get(v["subject_scope"], v["subject_scope"])
    if (
        co(n["name"]) not in {co(x) for x in v["native_aliases"]}
        or r["subject_scope"] != expected
        or r.get("reported_period") != v["reported_period"]
        or v["page"] not in pages(r)
    ):
        return False
    if v["source_actor"] != "公司及合并子公司" and co(r.get("source_actor")) not in {
        co(a) for a in v["actor_aliases"]
    }:
        return False
    if r.get("action") not in {ACTION_MAP[a] for a in v["source_actions"]}:
        return False
    native_services = {"安装", "维护", "修理", "租赁", "技术支持", "安调"}
    required_services = set(v["source_actions"]) & native_services
    if required_services and r.get("source_verb") not in required_services:
        return False
    if v.get("counterparty") and co(v["counterparty"]) not in q:
        return False
    if v.get("category") == "service" and r["action"] not in [
        "provides_service",
        "operates",
        "develops",
    ]:
        return False
    if (
        v.get("category") == "goods"
        and r["action"] == "provides_service"
        and not any(a in ["租赁"] for a in v["source_actions"])
    ):
        return False
    return not (
        v.get("qualifier")
        and any(t in v["qualifier"] for t in ["开发", "投运", "应用", "参股"])
        and not n.get("qualifier")
    )


@pytest.mark.parametrize("instrument", ["600100.SH", "600107.SH"])
def test_actual_complete_pages_accept_query_export(tmp_path, instrument):
    bundle = json.loads(FIXTURE.read_text())
    scope = bundle["contract"]
    source = bundle["official_inputs"][instrument]
    queried, exported = _drive(
        tmp_path,
        instrument=instrument,
        fixture=source["report"],
        pages=source["pages"],
        repair_version="v59",
    )
    assert queried == exported
    ck = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    records = list(_iter_accepted_raw(ck))
    for condition in scope["revenue_conditions"][instrument]:
        for kind in ("Segment", "Measurement"):
            assert any(
                r["object_type"] == kind and match(r, condition) for r in records
            ), condition
    for condition in scope["action_conditions"]:
        if condition["instrument_id"] == instrument:
            assert any(
                r["object_type"] == "Activity" and rolematch(r, condition)
                for r in records
            ), condition
    bodies = {a["dimension_id"]: co(a["excerpt"]) for a in queried["dimensions"]}
    if instrument == "600100.SH":
        for term in [
            "公共安全",
            "数智产业",
            "智慧能源",
            "上年计算机硬件业务出表",
            "2025.6.30",
            "重要联营企业",
        ]:
            assert term in bodies["principal_business"]
        for term in [
            "货物车辆检查",
            "行李及包裹成像检查系统",
            "人体",
            "CNKI",
            "华知",
            "热泵",
            "消防系统",
            "大温差",
            "蒙古国订单",
            "研发",
            "全部投运",
        ]:
            assert term in bodies["products_services"]
        for term in [
            "机构订阅",
            "个人按需",
            "技术授权",
            "取得了收款凭证",
            "产出法或投入法",
            "PPP",
            "代理人",
            "日常维护管理费",
            "直线法",
            "资产托管",
        ]:
            assert term in bodies["revenue_model"]
        assert not any(
            r.get("action") == "sells"
            and r["source_native"]["name"]
            in {"弱电及节能工程实施及产品", "合同能源管理"}
            for r in records
        )
    else:
        for term in [
            "服装服饰",
            "来料加工",
            "进料加工",
            "青海惠嘉",
            "2024年4月26日",
            "2025年6月13日",
            "51%",
        ]:
            assert term in bodies["principal_business"]
        for term in ["男装", "女装", "西服", "衬衣", "蒸汽", "转售水、电", "运输服务"]:
            assert term in bodies["products_services"]
        for term in [
            "专卖店",
            "与商场结算",
            "消费者确认收货",
            "FOB",
            "装船并办理报关",
            "投入法",
            "指定交货地点",
            "预收",
            "租赁",
            "原表单位：万元",
            "金额按元解释",
        ]:
            assert term in bodies["revenue_model"]
        for r in records:
            n = r["source_native"]
            if r["object_type"] in {"Segment", "Measurement"}:
                assert n["name"] != "医疗"
                if 240 in pages(r):
                    assert n.get("value") is None or num(n["value"]) not in {
                        num("58591137.65"),
                        num("63019092.53"),
                    }
                if n.get("header") in {
                    "各品牌盈利",
                    "直营店和加盟店盈利",
                    "线上线下销售渠道盈利",
                    "各地区盈利",
                }:
                    assert n["unit"] == "元"
                    assert "原表单位：万元" in n["qualifier"]
            if (
                r["object_type"] == "Activity"
                and r.get("action") == "purchases"
                and 215 in pages(r)
            ):
                assert n["name"] == "采购商品/接受劳务"
                assert n["header"] == "本期采购原内容"


@pytest.mark.parametrize(
    "business,forbidden",
    [
        ("燃气管网建设、燃气销售", "燃气管网建设"),
        ("弱电及节能工程实施及产品销售", "弱电及节能工程实施及产品"),
        ("合同能源管理、城市热网节能方案的开发、生产、销售", "合同能源管理"),
    ],
)
def test_independent_service_clause_does_not_become_sold_goods(business, forbidden):
    assert not any(
        name == forbidden and action.value == "sells"
        for name, action, verb in selection._native_business_verb_objects(business)
    )


def test_wrapped_header_is_not_a_company_name():
    text = "本期发生额 上期发生额\n营业收\n入\n净利润\n综合收\n益总额\n漳州水仙药业\n股份有限公司 30,377.59 4,992.95 4,992.95 4,682.53 31,022.67 4,241.66 4,241.66 8,615.42"
    rows = selection._native_current_company_income_rows(text)
    assert len(rows) == 1
    assert rows[0][1:3] == ("漳州水仙药业股份有限公司", "30,377.59")


@pytest.mark.parametrize("approved", [True, False])
def test_unit_ruling_is_limited_to_approved_official_version(approved):
    from tests.unit.test_research.test_company_profile_runtime import _report

    source = json.loads(FIXTURE.read_text())["official_inputs"]["600107.SH"]
    report = _report(instrument_id="600107.SH").model_copy(
        update={
            "document_version": source["report"]["document_version"]
            if approved
            else "another-official-version"
        }
    )
    evidence = selection.select_core_evidence(
        report=report,
        pages=[p for p in source["pages"] if p["page"] in [15, 16, 23, 24]],
        source_delivery_repair=True,
    )
    records = selection.project_owned_page_facts(evidence, source_delivery_repair=True)
    ruled = [
        r
        for r in records
        if r.source_native.header
        in {"各品牌盈利", "直营店和加盟店盈利", "线上线下销售渠道盈利", "各地区盈利"}
    ]
    assert bool(ruled) == approved
    assert all(
        r.source_native.unit == "元" and "原表单位：万元" in r.source_native.qualifier
        for r in ruled
    )


def test_income_match_rejects_wrong_column_unit_and_amount():
    from copy import deepcopy

    contract = json.loads(FIXTURE.read_text())["contract"]
    v = next(
        v
        for v in contract["revenue_conditions"]["600107.SH"]
        if v["header"].startswith("主营业务分")
    )
    r = {
        "source_native": {
            "name": v["native_aliases"][0],
            "value": v["value"],
            "unit": v["unit"],
            "header": "主营业务收入",
        },
        "dimension": v["dimension"],
        "subject_scope": v["subject_scope"],
        "reported_period": v["reported_period"],
        "evidence": [{"page": v["page"]}],
    }
    assert match(r, v)
    for key, wrong in [("header", "主营业务成本"), ("unit", "万元"), ("value", "1.00")]:
        bad = deepcopy(r)
        bad["source_native"][key] = wrong
        assert not match(bad, v)
