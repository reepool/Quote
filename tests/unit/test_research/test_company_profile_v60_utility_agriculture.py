"""Official complete pages preserve current columns, categories and operating scope."""

import json
import re
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v60_utility_agriculture_pages.json"
)


def co(s):
    return re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")


def num(v):
    return Decimal(str(v).replace(",", "").strip())


def pages(r):
    return {
        p
        for e in r.get("evidence", [])
        for p in [
            e.get("page"),
            *e.get("continuation_pages", []),
            *e.get("subject_evidence_pages", []),
        ]
        if p is not None
    }


def match(r, v):
    if r["object_type"] not in ["Segment", "Measurement"]:
        return False
    n = r["source_native"]
    h = n.get("header") or ""
    q = co(n.get("qualifier"))
    rh = v["header"]
    sc = r["subject_scope"]
    d = r.get("dimension", r.get("segment_dimension"))
    if (
        r.get("reported_period") != v["reported_period"]
        or v["page"] not in pages(r)
        or n.get("value") is None
        or n.get("unit") != v["unit"]
    ):
        return False
    if num(n["value"]) != num(v["value"]):
        return False
    if co(n["name"]) not in {co(x) for x in v["native_aliases"]}:
        return False
    if sc != v["subject_scope"]:
        return False
    if v.get("counterparty") and co(v["counterparty"]) not in q:
        return False
    if rh.startswith("主营业务分"):
        return d == v["dimension"] and ("主营" in h or co(rh) in co(h))
    if rh.startswith("主要控股"):
        return (
            ("控股" in h or "子公司" in h)
            and ("营业收入" in h or "收入" in h)
            and co(v["source_actor"]) in co(q + " " + n["name"])
        )
    if rh.startswith("前五名客户"):
        return "客户" in h and any(x in h for x in ["销售额", "收入"])
    if rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        return h in ["营业收入", rh, "营业收入和营业成本/收入"]
    if rh in ["合并利润表营业收入", "母公司利润表营业收入"]:
        return "营业收入" in h and ("利润表" in h or h == "营业收入")
    if rh.startswith("营业收入分解/"):
        if "成本" in h:
            return False
        seg = co(v.get("source_segment")).replace("-分部", "")
        q = q.replace("-分部", "")
        timing = "商品转让时间" in rh
        return (
            ("合同收入" in h or "收入分解" in h or "转让时间" in h)
            and (not seg or seg in co(q))
            and (not timing or any(x in h + q for x in ["时点", "时段", "时间"]))
        )
    if rh == "作为出租人的经营租赁":
        return "作为出租人" in h and "租赁收入" in h
    if rh == "本公司作为出租方关联租赁":
        return "出租方" in h and "租赁收入" in h
    if rh == "本期关联销售":
        return any(
            t in h for t in ["出售商品/提供劳务", "关联销售", "销售商品/提供劳务"]
        ) and any(t in h for t in ["本期", "本年"])
    if rh == "报告分部营业收入":
        return (
            any(t in h for t in ["营业收入", "分部收入"])
            and "成本" not in h
            and d in ["business_segment", "adjustment"]
        )
    if rh.startswith("重要合营"):
        return (
            "合营" in h
            and "联营" not in h
            and "营业收入" in h
            and co(v["source_actor"]) in co(n["name"] + " " + q)
        )
    if rh == "贸易业务本期营业收入":
        return "收入" in h and ("贸易" in h or h == "本期营业收入")
    return co(h) == co(rh)


VERBS = {
    "生产": "produces",
    "种植": "produces",
    "加工": "processes",
    "销售": "sells",
    "采购": "purchases",
    "提供": "provides_service",
    "安装": "provides_service",
    "施工": "provides_service",
    "出租": "provides_service",
    "运营": "operates",
}


def rolematch(r, v):
    if r["object_type"] != "Activity":
        return False
    n = r["source_native"]
    q = co(n.get("qualifier"))
    h = n.get("header") or ""
    n = dict(n)
    if v["source_actions"][0] == "采购" and n["name"] == "接受劳务":
        n["name"] = "劳务"
    if v["source_actions"][0] == "采购" and n["name"] == "购买商品":
        n["name"] = "商品"
    if v["source_actions"][0] == "出租" and n["name"] == v["name"] + "租赁":
        n["name"] = v["name"]
    expected = VERBS[v["source_actions"][0]]
    if (
        co(n["name"]) not in {co(x) for x in v["native_aliases"]}
        or r["subject_scope"] != v["subject_scope"]
        or r.get("reported_period") != v["reported_period"]
        or v["page"] not in pages(r)
        or r.get("action") != expected
    ):
        return False
    if v["source_actor"] != "公司及合并子公司" and co(r.get("source_actor")) not in {
        co(x) for x in v["actor_aliases"]
    }:
        return False
    if v.get("counterparty") and co(v["counterparty"]) not in q:
        return False
    if (
        v["category"] == "service"
        and expected == "purchases"
        and not any(t in h for t in ["服务", "劳务", "公用事业"])
    ):
        return False
    if (
        v["category"] == "goods"
        and expected == "purchases"
        and not any(t in h for t in ["商品", "原料", "材料", "电力"])
    ):
        return False
    if v["category"] in ["mixed", "unspecified"] and "原内容" not in h:
        return False
    if r.get("source_verb") == "采购" and expected != "purchases":
        return False
    if v["source_actions"][0] in ["安装", "施工", "出租", "种植"]:
        verb = r.get("source_verb") or ""
        if not any(
            t in verb
            for t in {
                "安装": ["安装"],
                "施工": ["施工", "实施"],
                "出租": ["出租", "租赁"],
                "种植": ["种植"],
            }[v["source_actions"][0]]
        ):
            return False
    return True


def negative_check(records, bodies, v):
    """Check actual forbidden output independently of positive matching."""
    ins = v["instrument_id"]
    k = int(v["condition_id"].split("-")[-1])
    income = [r for r in records if r["object_type"] in ["Segment", "Measurement"]]
    actions = [r for r in records if r["object_type"] == "Activity"]

    def named(items, word):
        return [
            r
            for r in items
            if word
            in co(
                r["source_native"]["name"]
                + " "
                + (r["source_native"].get("qualifier") or "")
                + " "
                + (r.get("source_actor") or "")
            )
        ]

    def at(items, page):
        return [r for r in items if page in pages(r)]

    if ins == "600101.SH":
        if k <= 13:
            return not named(
                [r for r in actions if r["action"] in ["purchases", "inputs"]],
                v["counterparty"],
            )
        if k == 14:
            return not named(income, "甘孜州奥深达润神矿业")
        if k == 15:
            return not any(
                {36, 37, 38} & pages(r)
                and r["source_native"].get("header")
                in ["原子公司经营范围", "原子公司业务性质"]
                for r in actions
            ) and not any("雪茄" in b or "许可项目" in b for b in bodies.values())
        if k in [16, 17, 19]:
            terms = {16: ["二次供水", "直饮水"], 17: ["虚拟电厂"], 19: ["唐家渡"]}[k]
            return not any(named(actions, t) for t in terms)
        if k == 18:
            return not any(
                r["source_native"].get("value") is not None
                and num(r["source_native"]["value"]) == num("302800")
                for r in income
            )
        if k == 20:
            # Original blank cells: no current construction revenue for
            # water supply; no costs masquerading as original income.
            return not any(
                r["source_native"]["name"] == "建筑安装工程收入"
                and "自来水" in (r["source_native"].get("qualifier") or "")
                for r in at(income, 201)
            ) and not any(
                "成本" in (r["source_native"].get("header") or "")
                for r in at(income, 201)
            )
        if k == 21:
            return not any(
                "重要联营" in (r["source_native"].get("header") or "") for r in income
            )
        if k == 22:
            return all(
                r["subject_scope"] == "business_segment"
                for r in named(income + actions, "四川华润万通燃气股份有限公司")
                if r.get("source_actor") == "四川华润万通燃气股份有限公司"
                or r["source_native"]["name"] == "四川华润万通燃气股份有限公司"
            )
        if k == 23:
            return not any(
                named(
                    [
                        r
                        for r in income + actions
                        if r["source_native"]["name"] == "酒店服务"
                    ],
                    t,
                )
                for t in ["绵阳东泓", "四川科锐得实业", "福建省电建咨询", "国网射洪"]
            )
        if k == 24:
            return not any(
                "受托" in (r["source_native"].get("header") or "")
                for r in income + actions
            )
        if k in [25, 26]:
            return not any(
                any(
                    t in (r["source_native"].get("header") or "")
                    for t in ["承租人", "融资租赁"]
                )
                for r in at(income, 214) + at(income, 215)
            )
        if k == 27:
            return not any(
                "合同收入/" in (r["source_native"].get("header") or "")
                for r in at(income, 254)
            )
    else:
        if k == 1:
            return not any(named(income, t) for t in ["鱼儿红矿业", "甜源农业"])
        if k in [2, 3, 4]:
            terms = {
                2: ["甜菜", "制糖"],
                3: ["微生物菌剂", "马铃薯全粉"],
                4: ["农机设备", "化肥", "农药"],
            }[k]
            return not any(named(actions, t) for t in terms)
        if k == 5:
            return not any(
                r["source_native"]["name"] == "农产品销售"
                and any(
                    t + "营业收入" in (r["source_native"].get("qualifier") or "")
                    for t in ["工业", "其他"]
                )
                for r in at(income, 104)
            )
        if k == 6:
            return not any(
                r["action"] == "purchases"
                and (
                    (r["source_native"]["name"] == "机耕费")
                    or (
                        r["source_native"]["name"] == "其他"
                        and "其他关联方" in (r["source_native"].get("qualifier") or "")
                    )
                )
                for r in at(actions, 120)
            )
        if k == 7:
            return not any(
                r["source_native"].get("value") is not None
                and num(r["source_native"]["value"]) == num("231818.90")
                for r in at(income, 120)
            )
        if k == 8:
            return not named(income + actions, "甘肃农垦张掖农场有限责任公司")
        if k in [9, 10]:
            return not any(
                any(
                    t in (r["source_native"].get("header") or "")
                    for t in ["承租方", "承租人", "融资租赁"]
                )
                for r in at(income, 122) + at(income, 113)
            )
        if k == 11:
            return not any(
                "受托" in (r["source_native"].get("header") or "")
                for r in income + actions
            )
        if k == 12:
            return not any(
                "合同收入/" in (r["source_native"].get("header") or "")
                for r in at(income, 132)
            )
        if k == 13:
            return not any(
                r.get("source_actor")
                in [
                    "甘肃黄羊河农工商（集团）有限责任公司",
                    "甘肃省农垦建筑工程有限责任公司",
                    "农垦集团及其他所属公司",
                    "莫高股份及下属公司",
                    "庄园牧场及下属公司",
                ]
                for r in actions
            )
    raise AssertionError(v)


@pytest.mark.parametrize("instrument", ["600101.SH", "600108.SH"])
def test_complete_official_accept_query_export(tmp_path, instrument):
    bundle = json.loads(FIXTURE.read_text())
    scope = bundle["contract"]
    src = bundle["official_inputs"][instrument]
    q, e = _drive(
        tmp_path,
        instrument=instrument,
        fixture=src["report"],
        pages=src["pages"],
        repair_version="v60",
    )
    assert q == e
    records = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    (tmp_path / "company_profile_common_core.v1/checkpoints").glob(
                        "*.json"
                    )
                ).read_text()
            )
        )
    )
    for v in scope["revenue_conditions"][instrument]:
        for typ in ["Segment", "Measurement"]:
            assert any(r["object_type"] == typ and match(r, v) for r in records), v
    for v in scope["action_conditions"]:
        if v["instrument_id"] != instrument:
            continue
        if v["condition_id"] in {
            "600108.SH-action-040",
            "600108.SH-action-041",
            "600108.SH-action-046",
            "600108.SH-action-048",
        }:
            # A adjudicated these frozen historical action assumptions invalid.
            # Retain the original fixture; test the actual ambiguity boundary.
            assert not any(
                r["object_type"] == "Activity"
                and r["action"] != "purchases"
                and r["source_native"]["name"] == v["name"]
                and v["counterparty"] in (r["source_native"].get("qualifier") or "")
                and 120 in pages(r)
                for r in records
            )
            continue
        if v["condition_id"] == "600108.SH-action-014":
            # Original contract remains frozen pending source-actor adjudication.
            assert any(
                r.get("source_actor") == "亚美特公司"
                and r["source_native"]["name"] == v["name"]
                for r in records
            )
            continue
        assert any(rolematch(r, v) for r in records), v
    bodies = {a["dimension_id"]: co(a["excerpt"]) for a in q["dimensions"]}
    for v in scope["negative_conditions"]:
        if v["instrument_id"] == instrument:
            assert negative_check(records, bodies, v), v
    if instrument == "600101.SH":
        expected = {
            "principal_business": [
                "独立的供电网络",
                "趸购",
                "停止常规自来水生产",
                "光储充享",
                "用户侧储能",
                "50%表决权",
            ],
            "products_services": [
                "电水设计安装",
                "智能运维",
                "带电作业",
                "电气试验",
                "水务工程设计",
                "餐饮",
                "50%表决权",
            ],
            "revenue_model": [
                "共同确认的上网电量",
                "电力直接交易三方合同",
                "批发市场和零售市场交易价格差",
                "投入法",
                "向委托方提供设计图纸",
                "取得客户认可",
                "直线法",
            ],
        }
        assert any(
            "registered_permission_not_current_operation:" in u
            for r in records
            for u in r["uncertainty"]
        )
    else:
        expected = {
            "principal_business": [
                "订单农业",
                "种植结构",
                "农业节水灌溉产品",
                "甜菜",
                "稳步推进",
                "子公司之子公司",
            ],
            "products_services": [
                "牧草",
                "香辛料",
                "商品玉米",
                "有机肥",
                "农业节水灌溉产品",
                "滴灌工程",
            ],
            "revenue_model": [
                "客户收到货物",
                "客户已提取货物",
                "产出法",
                "投入法",
                "一个种植周期",
                "直线法",
            ],
        }
    for dim, terms in expected.items():
        for term in terms:
            assert term in bodies[dim], (dim, term)
    assert not any(
        r.get("source_verb") == "采购" and r.get("action") != "purchases"
        for r in records
    )


def test_equity_table_stops_before_voting_rights_explanation():
    b = json.loads(FIXTURE.read_text())
    p = next(p for p in b["official_inputs"]["600101.SH"]["pages"] if p["page"] == 219)
    assert selection._native_equity_business_rows(p["layout_text"]) == [
        ("四川华润万通燃气股份有限公司", "燃气销售", True)
    ]


def test_supplier_rows_do_not_inherit_previous_only_party():
    b = json.loads(FIXTURE.read_text())
    text = "\n".join(
        p.get("layout_text") or p["text"]
        for p in b["official_inputs"]["600101.SH"]["pages"]
        if p["page"] in [229, 230, 231]
    )
    rows = selection._native_current_purchase_rows(text)
    assert not any(
        "山东电力高等专科学校" in row[5]
        or "国网中兴有限公司" in row[5]
        or "四川巴中和兴电力有限责任公司" in row[5]
        for row in rows
    )
    assert any("《电网技术》杂志社有限公司" in row[5] for row in rows)
    assert any("《国家电网报》社有限公司" in row[5] for row in rows)


def test_match_rejects_service_category_and_reversed_verb():
    b = json.loads(FIXTURE.read_text())
    v = next(
        v
        for v in b["contract"]["action_conditions"]
        if v["category"] == "service" and v["source_actions"] == ["采购"]
    )
    r = {
        "object_type": "Activity",
        "subject_scope": v["subject_scope"],
        "reported_period": v["reported_period"],
        "source_actor": v["source_actor"],
        "action": "purchases",
        "source_verb": "采购",
        "evidence": [{"page": v["page"]}],
        "source_native": {
            "name": v["name"],
            "header": "本期采购服务",
            "qualifier": "交易对方：" + v["counterparty"],
        },
    }
    assert rolematch(r, v)
    bad = deepcopy(r)
    bad["source_native"]["header"] = "本期采购商品"
    assert not rolematch(bad, v)
    v = next(
        v
        for v in b["contract"]["action_conditions"]
        if v["source_actions"] == ["销售"] and v["source_actor"] == "公司及合并子公司"
    )
    r.update(
        action="sells",
        source_actor=v["source_actor"],
        subject_scope=v["subject_scope"],
        evidence=[{"page": v["page"]}],
        source_native={"name": v["name"], "header": "本期销售商品", "qualifier": None},
    )
    assert not rolematch(r, v)
