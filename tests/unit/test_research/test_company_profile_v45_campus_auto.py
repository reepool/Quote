"""Native campus/automotive owner pages: bodies, actors and revenue columns."""

import json
import re
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive
from tests.unit.test_research.test_company_profile_v43_energy_pharma import (
    action_matches,
)

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v45_campus_auto_frozen_pages.json"
)
compact = lambda s: re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")
num = lambda s: compact(s).replace(",", "")
aliases = {
    "正蓝节能": "浙江正蓝节能科技股份有限公司",
    "汇贤优策": "重庆汇贤优策科技有限公司",
    "东风延锋": "东风延锋汽车座舱系统有限公司",
    "湛江德利": "湛江德利车辆部件有限公司",
    "东风汤姆森": "东风富士汤姆森调温器有限公司",
    "东科克诺尔制动": "东科克诺尔商用车制动系统（十堰）有限公司",
    "苏州精冲": "苏州东风精冲工程有限公司",
    "东风电驱动": "东风电驱动系统有限公司",
    "东风马勒": "东风马勒热系统有限公司",
    "上海弗列加": "上海弗列加滤清器有限公司",
    "伟世通电子": "上海伟世通汽车电子系统有限公司",
    "广州座椅": "东科延锋（广州）座椅系统有限公司",
    "东科克诺尔制动技术": "东科克诺尔商用车制动技术有限公司",
}


def native_match(r, row):
    n = r["source_native"]
    ep = {
        p
        for e in r["evidence"]
        for p in [e.get("page"), *e.get("continuation_pages", [])]
        if p
    }
    if (
        row["page"] not in ep
        or compact(n["name"]) not in set(map(compact, row["native_aliases"]))
        or num(n["value"]) != num(row["value"])
        or n["unit"] != row["unit"]
    ):
        return False
    dim = r.get("dimension", r.get("segment_dimension"))
    expected = row["dimension"]
    h = n.get("header") or ""
    qual = n.get("qualifier") or ""
    if dim != expected and not (
        expected == "service" and dim == "lease" and "出租方" in h
    ):
        return False
    if row.get("segment_column"):
        if compact(qual) != compact("原收入列：" + row["column"]):
            return False
        expectedheader = "合同收入/" + (
            "合同分解合计" if row["header"] == "合同分解合计" else row["header"]
        )
        if h != expectedheader:
            return False
    elif row["header"] in [
        "合并营业收入和营业成本",
        "母公司营业收入和营业成本",
        "报告分部财务信息",
    ]:
        if h != "营业收入":
            return False
    elif row["header"].startswith("主营业务分"):
        if h not in ["", "主营业务收入"]:
            return False
    elif row["header"] == "主要控股参股公司分析":
        if h != "控股参股公司/营业收入":
            return False
    elif row["header"] == "重要非全资子公司的主要财务信息":
        if h != "重要非全资子公司/本期营业收入":
            return False
    elif row["header"] in ["重要合营企业的主要财务信息", "重要联营企业的主要财务信息"]:
        relation = "合营企业" if "合营" in row["header"] else "联营企业"
        if h != "重要" + relation + "/本期营业收入" or compact(qual) != compact(
            relation + "：" + row["source_actor"]
        ):
            return False
    elif row["header"] == "出售商品/提供劳务":
        if h != "出售商品/提供劳务/本期发生额":
            return False
    elif row["header"] == "作为出租人的经营租赁":
        if h != "作为出租人的经营租赁/租赁收入":
            return False
    elif row["header"] == "本公司作为出租方":
        if h != "本公司作为出租方/本期确认的租赁收入":
            return False
    if (
        row["header"] == "临时公告未披露关联交易"
        and h != "临时公告未披露关联交易/本期收入"
    ):
        return False
    if (
        row["header"] == "本公司受托管理/承包"
        and h != "本公司受托管理/承包/本期确认的托管收益"
    ):
        return False
    prefix = (
        "委托方："
        if row["header"] == "本公司受托管理/承包"
        else "承租方："
        if dim == "lease"
        else "交易对方："
    )
    if row["counterparty"] and compact(qual) != compact(prefix + row["counterparty"]):
        return False
    actor = row["source_actor"]
    if actor == "公司及合并子公司":
        return r["subject_scope"] == "consolidated_group"
    if "（母公司）" in actor:
        return r["subject_scope"] == "issuer"
    found = aliases.get(n["name"], n["name"])
    subject = aliases.get(r.get("subject_name"), r.get("subject_name"))
    if compact(actor) not in map(
        compact, [found, subject or "", r.get("source_actor") or ""]
    ):
        return False
    expectedscope = (
        "business_segment"
        if row["header"] in ["重要合营企业的主要财务信息", "重要联营企业的主要财务信息"]
        or n["name"]
        in ["东风马勒", "上海弗列加", "伟世通电子", "广州座椅", "东科克诺尔制动技术"]
        else "named_subsidiary"
    )
    return r["subject_scope"] == expectedscope


def negative_wrong(raw, n):
    rr = raw
    financial = [r for r in rr if r["object_type"] in ["Segment", "Measurement"]]
    acts = [r for r in rr if r["object_type"] in ["Activity", "Relationship"]]
    wrong = []
    rule = n["name"]
    if rule == "排气无销售生产":
        wrong = [
            r
            for r in acts
            if r["source_native"]["name"] == "排气系统"
            and r.get("action") in ["sells", "produces"]
        ]
    if rule == "库存销售非生产":
        wrong = [
            r
            for r in acts
            if r["source_native"]["name"] == "发动机和变速箱业务"
            and r.get("action") == "produces"
        ]
    if rule == "母公司主营空不造零":
        wrong = [
            r
            for r in financial
            if r["subject_scope"] == "issuer"
            and r["source_native"]["name"] == "主营业务"
        ]
    if rule == "母公司无收入表":
        wrong = [r for r in financial if r["subject_scope"] == "issuer"]
    if rule == "关系章节不串表":
        wrong = [
            r
            for r in financial
            if "重要合营企业" in (r["source_native"].get("header") or "")
            and any(
                t in (r["source_native"].get("qualifier") or "")
                for t in ["伟世通", "座椅系统", "制动技术"]
            )
        ]
    if rule == "合营联营不适用":
        wrong = [
            r
            for r in financial
            if any(
                t in (r["source_native"].get("header") or "")
                for t in ["重要合营", "重要联营"]
            )
        ]
    if rule in [
        "采购不收入",
        "采购上期空不借列",
        "资产转让非营业收入",
        "租赁现金流非收入",
        "承租支出非出租收入",
        "费用表终止收入",
        "成本表非收入",
    ]:
        forbidden = {
            "采购不收入": [
                "939,488.80",
                "776,479.83",
                "477,580.24",
                "694,983.17",
                "9,589,079.53",
                "1.00",
            ],
            "采购上期空不借列": [
                "42,392,943.50",
                "1,197,123.71",
                "286,079.28",
                "170,788.52",
            ],
            "资产转让非营业收入": ["270,532,800.00", "25,334,367.50", "1,392,880.50"],
            "租赁现金流非收入": ["1,265,926.68", "2,741,151.86", "23,734,039.84"],
            "承租支出非出租收入": [
                "9,619,465.61",
                "2,729,102.95",
                "2,631,096.00",
                "463,675.22",
                "33,361.00",
            ],
            "费用表终止收入": [
                "11,379,618.28",
                "8,693,041.18",
                "8,190,537.83",
                "34,343,932.66",
                "30,694,955.13",
            ],
            "成本表非收入": [
                "106,125,173.35",
                "9,131,720.52",
                "5,902,988.18",
                "8,723,865.76",
            ],
        }[rule]
        wrong = [r for r in financial if r["source_native"]["value"] in forbidden]
    if rule == "跃动仅上期采购":
        wrong = [r for r in financial if r["source_native"]["value"] == "21,516,371.68"]
    if rule == "关联预算非实际收入":
        wrong = [
            r
            for r in financial
            if r["source_native"]["value"]
            in ["50.00", "5,000,000,000", "5,000,000,000.00"]
        ]
    if rule == "客户供应商非渠道":
        wrong = [
            r
            for r in financial
            if r.get("dimension", r.get("segment_dimension")) == "sales_mode"
        ]
    if rule == "参股电池非集团制造销售":
        wrong = [
            r
            for r in acts
            if r["subject_scope"] == "consolidated_group"
            and any(t in r["source_native"]["name"] for t in ["动力电池", "储能电池"])
        ]
    if rule == "政策非当前业务":
        wrong = [
            r
            for r in acts
            if any(t in r["source_native"]["name"] for t in ["智能快递柜", "无人超市"])
        ]
    if rule == "影视无新项目投资":
        wrong = [
            r
            for r in acts
            if r.get("action") in ["develops", "produces"]
            and any(t in r["source_native"]["name"] for t in ["影视", "电影", "电视剧"])
        ]
    if rule == "支付渠道出表":
        wrong = [
            r
            for r in acts
            if "支付渠道" in r["source_native"]["name"]
            and not any(
                t in (r["source_native"].get("qualifier") or "")
                for t in ["1-7", "7月", "退出"]
            )
        ]
    return wrong


@pytest.mark.parametrize("instrument", ["600052.SH", "600081.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_native_business_delivery(tmp_path, instrument, kind):
    f = json.loads(FIXTURE.read_text())[instrument]
    q, ex = _drive(
        tmp_path, instrument=instrument, fixture=f, pages=f[kind], repair_version="v45"
    )
    assert q == ex
    cp = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    raw = list(_iter_accepted_raw(cp))
    assert len({r["record_id"] for r in raw}) == len(raw)
    for dim, rule in f["answers"].items():
        txt = _answer(q, dim)
        assert all(
            any(compact(t) in txt for t in g) for g in rule["required_groups"]
        ), dim
    for row in f["source_rows"]:
        matched = [
            r for r in raw if r["object_type"] == "Segment" and native_match(r, row)
        ]
        assert matched, (row["condition_id"], row["page"], row["name"], row["value"])
        assert any(
            r["object_type"] == "Measurement"
            and r["source_native"] == m["source_native"]
            and r["subject_scope"] == m["subject_scope"]
            for r in raw
            for m in matched
        ), row["condition_id"]
    for role in f["native_roles"]:
        matches = action_matches(raw, role)
        assert matches, (role["name"], role["source_actor"], role["page"])
        if role["role"] in {"product_production", "service_provision"}:
            assert any(
                m["record_id"] == x["record_id"]
                for m in matches
                for x in q["accepted_facts"]
            )
            continue
        assert any(
            m["record_id"] in x["source_record_ids"]
            and x["role"]
            == {"raw_material_procurement": "raw_material_input"}.get(
                role["role"], role["role"]
            )
            for m in matches
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        ), role["name"]
    for n in f["negative_conditions"]:
        assert not negative_wrong(raw, n), n["name"]
    assert not any(
        r["object_type"] == "Activity"
        and compact(r["source_native"]["name"]) == "商品不适用否"
        for r in raw
    )
    if instrument == "600081.SH":
        assert not any(
            "本期发生额" in r["source_native"]["name"]
            or "上期发生额" in r["source_native"]["name"]
            for r in raw
            if r["object_type"] in {"Segment", "Measurement"}
        )
        original_parent = [
            r
            for r in raw
            if r["object_type"] == "Activity"
            and r["action"] in {"operates", "produces"}
            and r["source_native"]["name"]
            in {
                "汽车零部件",
                "座舱与车身系统",
                "制动及智能驾驶系统",
                "电驱动系统",
                "热管理系统",
                "动力总成部件系统",
            }
        ]
        assert len(original_parent) == 6


def test_successor_inherits_five_repairs_without_changing_default():
    default = default_processing_identity()
    assert default.get("revenue_sentence_repair") != "v45"
    identity = {**default, "revenue_sentence_repair": "v45"}
    for requested in [
        selection.core_answer_repair_requested,
        selection.named_role_repair_requested,
        selection.revenue_sentence_repair_requested,
        selection.service_operating_energy_requested,
        selection.source_delivery_repair_requested,
    ]:
        assert requested(identity)


def test_wrapped_multiple_aliases_keep_current_legal_actor():
    text = "汇贤优策、重庆汇贤 指\n重庆汇贤优策科技有限公司，原重庆汇贤优策智慧校园有限公司"
    definitions = selection._native_actor_definitions(text)
    assert [(alias, name) for alias, name, _ in definitions] == [
        ("汇贤优策", "重庆汇贤优策科技有限公司"),
        ("重庆汇贤", "重庆汇贤优策科技有限公司"),
    ]


def test_current_blank_contract_cell_cannot_shift_cost_into_income():
    # The current source's column geometry is retained, including empty cells.
    table = "合同分类          分部 1              合计\n"
    table += (
        " " * 16
        + "".join(
            h.center(20) for h in ["营业收入", "营业成本", "营业收入", "营业成本"]
        )
        + "\n商品类型\n"
    )
    table += (
        "商品甲".ljust(16)
        + "".join(v.center(20) for v in ["", "12.00", "34.00", "12.00"])
        + "\n其他说明\n费用 99.00 99.00 99.00\n"
    )
    rows = selection._native_contract_column_rows(table)
    assert rows == [
        (
            "product",
            "商品甲",
            "34.00",
            "合同收入/商品类型",
            None,
            "原收入列：合计营业收入",
        )
    ]
