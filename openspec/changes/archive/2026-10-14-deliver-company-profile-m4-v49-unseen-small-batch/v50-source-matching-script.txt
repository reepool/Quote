"""Full official inputs retain native business actions, table cells and entities."""

import json
from pathlib import Path

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v50_realestate_chemical_frozen_pages.json"
)
import re
from decimal import Decimal

co = lambda x: re.sub(r"\s+", "", x or "").replace("（", "(").replace("）", ")")
num = lambda x: Decimal(co(x).replace(",", ""))


def pages(r):
    return {
        p
        for e in r["evidence"]
        for p in [e.get("page"), *e.get("continuation_pages", [])]
        if p is not None
    }


def actor_ok(r, row):
    sc = row["subject_scope"]
    actual = r["subject_scope"]
    n = r["source_native"]
    q = co(n.get("qualifier"))
    actor = co(row["source_actor"])
    if sc == "consolidated_group":
        return actual == sc
    if sc == "issuer":
        return actual == "issuer"
    if sc == "named_associate":
        return (
            actual == "business_segment"
            and q
            in {
                co("联营企业：" + a)
                for a in [row["name"], row["source_actor"], *row["native_aliases"]]
            }
            if row["header"] == "重要联营企业的主要财务信息"
            else actual in ["business_segment", "named_subsidiary"]
            and q
            in {
                co(prefix + a)
                for prefix in ["子公司：", "参股公司："]
                for a in row["native_aliases"]
            }
        )
    return actual == "named_subsidiary" and (
        actor in q
        or any(co(a) in q for a in row["native_aliases"])
        or co(r.get("source_actor")) == actor
    )


def match(r, row):
    n = r["source_native"]
    h = n.get("header") or ""
    q = co(n.get("qualifier"))
    d = r.get("dimension", r.get("segment_dimension"))
    rh = row["header"]
    name = co(n["name"])
    if (
        row["page"] not in pages(r)
        or n.get("unit") != row["unit"]
        or num(n.get("value")) != num(row["value"])
    ):
        return False
    if name not in {co(s) for s in row["native_aliases"]} and not (
        rh == "贸易业务开展情况" and name == "商贸业务" and row["name"] == "贸易业务"
    ):
        return False
    if rh == "出售商品/提供劳务":
        return (
            d in ["product", "service"]
            and h == "出售商品/提供劳务/本期发生额"
            and q == co("交易对方：" + row["counterparty"])
            and r["subject_scope"] == "consolidated_group"
        )
    if rh == "本公司作为出租方":
        return (
            d == "lease"
            and h == "本公司作为出租方/本期确认的租赁收入"
            and q == co("承租方：" + row["counterparty"])
            and r["subject_scope"] == "consolidated_group"
        )
    if rh == "受托管理/承包":
        return (
            d == "service"
            and "托管" in h
            and co(row["counterparty"]) in q
            and co(row["source_actor"]) in q
            and r["subject_scope"] == row["subject_scope"]
        )
    if rh == "重要联营企业的主要财务信息":
        return h == "重要联营企业/本期营业收入" and actor_ok(r, row)
    if d != row["dimension"] and not (
        rh in ["主要控股参股公司分析", "重要非全资子公司的主要财务信息"]
        and d == "business_segment"
    ):
        return False
    if rh.startswith("主营业务分"):
        ok = h in ["", "主营业务收入"]
    elif rh == "主营业务收入构成":
        ok = h == "主营业务收入构成"
    elif rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        ok = h == "营业收入"
    elif rh == "贸易业务开展情况":
        ok = h == "本期营业收入"
    elif rh == "主要控股参股公司分析":
        ok = h == "控股参股公司/营业收入"
    elif rh == "重要非全资子公司的主要财务信息":
        ok = h == "重要非全资子公司/本期营业收入"
    elif rh.endswith("营业收入明细"):
        ok = h in ["营业收入", "营业收入明细", "主营业务收入"]
    elif rh.startswith("报告分部/"):
        ok = h in ["主营业务收入", rh.split("/")[-1], rh, "报告分部/主营业务收入"]
    elif rh == "按销售渠道划分的公司主营业务基本情况":
        ok = h in [
            "主营业务收入",
            "主营业务收入/销售渠道",
            "营业收入",
            "渠道收入/主营业务",
        ]
    elif rh == "房地产销售和结转情况":
        ok = "结转" in h
    elif rh == "房地产出租情况":
        ok = "租金" in h or "租赁" in h
    elif rh.startswith("经营讨论/"):
        ok = "营业收入" in h or "结转" in h
    else:
        ok = False
    return ok and actor_ok(r, row)


def action_match(r, role):
    n = r["source_native"]
    if role["page"] not in pages(r) or co(n["name"]) not in {
        co(a) for a in role["native_aliases"]
    }:
        return False
    if r["subject_scope"] != role["subject_scope"]:
        return False
    if role["source_actor"] != "公司及合并子公司" and co(r.get("source_actor")) not in {
        co(a) for a in role.get("actor_aliases", [role["source_actor"]])
    }:
        return False
    return (
        r.get("relation_type") == "material_input"
        if role["source_actions"] == ["material_input"]
        else r.get("action") in role["source_actions"]
    )


def negative_violations(raw, condition, answers):
    """Check the original negative condition against actual delivered identities."""
    n = condition
    name = n["name"]
    fin = [r for r in raw if r["object_type"] in {"Segment", "Measurement"}]
    acts = [r for r in raw if r["object_type"] in {"Activity", "Relationship"}]
    if name == "重要合营不适用":
        return [
            r["record_id"]
            for r in fin
            if "重要合营" in (r["source_native"].get("header") or "")
        ]
    if name == "仅上期关联":
        return [
            r["record_id"]
            for r in fin
            if co(r["source_native"].get("qualifier"))
            == co("交易对方：" + n["counterparty"])
        ]
    if name in {
        "项目结转空位",
        "新创营业收入空位",
        "联营成长创信收入空",
        "联营创新收入空",
        "合并其他业务本期空",
        "母公司其他业务本期空",
    }:
        return [
            r["record_id"]
            for r in fin
            if n["page"] in pages(r)
            and co(r["source_native"]["name"]) in {co(v) for v in n["names"]}
        ]
    if name == "受托不适用委托费非收入":
        return [
            r["record_id"]
            for r in fin
            if n["page"] in pages(r)
            and any(
                h in (r["source_native"].get("header") or "")
                for h in n["forbidden_headers"]
            )
        ]
    if name in {"股权收益非营业收入", "承租租金非收入", "投资收益非收入"}:
        return [
            r["record_id"]
            for r in fin
            if any(num(r["source_native"]["value"]) == num(v) for v in n["values"])
        ]
    if name in {
        "被投行业非集团具名生产销售",
        "联营生产不升格集团",
        "参股氟化产品非集团",
        "子公司产能非当前销售",
        "下游用途非自产商品",
        "医用聚甲醛研发非销售",
    }:
        targets = {co(v) for v in n["names"]}
        return [
            r["record_id"]
            for r in acts
            if co(r["source_native"]["name"]) in targets
            and r.get("action") in {"produces", "sells"}
            and (
                name
                not in {
                    "参股氟化产品非集团",
                    "被投行业非集团具名生产销售",
                    "联营生产不升格集团",
                }
                or r["subject_scope"] == "consolidated_group"
            )
            and (name != "子公司产能非当前销售" or r.get("action") == "sells")
        ]
    if name == "费用成本非营业收入":
        return [
            r["record_id"]
            for r in fin
            if any(
                v in (r["source_native"].get("header") or "")
                for v in n["forbidden_headers"]
            )
        ]
    if name == "报告分部主营不升级全部外内抵销":
        return [
            r["record_id"]
            for r in fin
            if pages(r) & {258, 259}
            and any(
                v in (r["source_native"].get("header") or "")
                for v in ["对外交易", "内部交易", "分部间抵销", "全部营业"]
            )
        ]
    if name == "打印零采购不推动作":
        return [
            r["record_id"]
            for r in acts
            if co(r["source_native"]["name"]) in {co(v) for v in n["names"]}
            and r.get("action") == "purchases"
            and any(
                co(v) in co(r["source_native"].get("qualifier"))
                for v in n["counterparties"]
            )
        ]
    if name == "零收入不推销售动作":
        return [
            r["record_id"]
            for r in acts
            if r.get("action") == "sells"
            and co(n["counterparty"]) in co(r["source_native"].get("qualifier"))
            and co(r["source_native"]["name"])
            in {co(v).removeprefix("销售") for v in n["content"].split("、")}
        ]
    if name in {
        "退出合并状态",
        "红海40仍控制",
        "大豆9月后不开展",
        "供应链探索不升格全面现有服务",
        "海口关联口径期间",
    }:
        t = co("".join(answers.values()))
        if name == "退出合并状态":
            assert all(
                x in t
                for x in [
                    "河南云天化",
                    "云农科技",
                    "不再纳入公司合并范围",
                    "上海云天化国际贸易有限公司完成清算注销",
                ]
            )
            return [
                r["record_id"]
                for r in acts
                if co(r.get("source_actor")) in {co(v) for v in n["names"]}
            ]
        if name == "红海40仍控制":
            assert "能够对其控制" in t
        if name == "大豆9月后不开展":
            return [
                r["record_id"]
                for r in acts
                if co(r["source_native"]["name"]) == "大豆"
                and "9月" not in co(r["source_native"].get("qualifier"))
            ]
        if name == "供应链探索不升格全面现有服务":
            assert "探索" in t
        if name == "海口关联口径期间":
            assert "3月后不再是公司信息披露口径的关联方" in t
        return []
    raise AssertionError(name)


@pytest.mark.parametrize("instrument", ["600064.SH", "600096.SH"])
@pytest.mark.parametrize("input_kind", ["pages", "independent_pages"])
def test_complete_official_contract(tmp_path, instrument, input_kind):
    f = json.loads(FIXTURE.read_text())[instrument]
    q, e = _drive(
        tmp_path,
        instrument=instrument,
        fixture=f,
        pages=f[input_kind],
        repair_version="v50",
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
    if input_kind == "pages":
        Path("/tmp/v50raw_" + instrument + ".json").write_text(
            json.dumps(raw, ensure_ascii=False, indent=2)
        )
        Path("/tmp/v50profile_" + instrument + ".json").write_text(
            json.dumps(q, ensure_ascii=False, indent=2)
        )

    for kind in ["Segment", "Measurement"]:
        missing = [
            v["condition_id"]
            for v in f["source_rows"]
            if not any(r["object_type"] == kind and match(r, v) for r in raw)
        ]
        assert not missing, missing
    missing = [
        v["condition_id"]
        for v in f["native_roles"]
        if not any(action_match(r, v) for r in raw)
    ]
    assert not missing, missing
    answers = {dim: _answer(q, dim) for dim in f["answers"]}
    for dim, rule in f["answers"].items():
        assert all(
            any(co(t) in answers[dim] for t in g) for g in rule["required_groups"]
        )
    bad_names = {
        "天然气为主要原料",
        "商贸",
        "新材料的研发及产品",
        "；国际货",
        "维修费",
        "消防费用及消防器材",
        "电费",
        "检测费",
        "租金",
    }
    assert not any(
        r["object_type"] == "Activity" and r["source_native"]["name"] in bad_names
        for r in raw
    )
    for n in f["negative_conditions"]:
        assert not negative_violations(raw, n, answers), n["condition_id"]
    if instrument == "600096.SH":
        assert any(
            r["object_type"] == "Segment"
            and r["source_native"]["value"] == "88,139,460.13"
            and r["source_native"]["unit"] == "元"
            and r["subject_scope"] == "consolidated_group"
            for r in raw
        )


def test_successor_identity_retains_all_five_switches():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v50"}
    for name in [
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ]:
        assert getattr(selection, name)(identity)
    assert default_processing_identity().get("revenue_sentence_repair") != "v50"


@pytest.mark.parametrize(
    "business", ["房地产开发、经营、销售等", "房地产开发、经营、销售"]
)
def test_shared_realestate_object_keeps_operating_and_sales(business):
    text = (
        "主要控股参股公司分析\n"
        f"{'公司名称':<16}{'公司类型':<8}{'主要业务':<26}注册资本 总资产 净资产 营业收入 营业利润 净利润\n"
        f"{'甲置业有限公司':<16}{'子公司':<8}{business:<26}100.00 200.00 150.00 60.00 10.00 9.00"
    )
    bindings = selection._owned_product_action_bindings(text)
    assert all(b["name"] for b in bindings)
    assert {(b["name"], b["action"].value) for b in bindings} >= {
        ("房地产", "operates"),
        ("房地产", "sells"),
    }


def test_related_sales_title_does_not_filter_native_goods_row():
    rows = selection._native_related_sales_columns(
        "关联方    关联交易内容    本期发生额    上期发生额\n甲公司    出售商品    12.34    56.78"
    )
    assert rows[0][1:3] == ("出售商品", "12.34")


@pytest.mark.parametrize("drift", [False, True, "between_reads"])
def test_successor_exact_asset_binding(tmp_path, drift):
    from tests.unit.test_research.test_company_profile_v41_telecom_pharma import (
        test_frozen_asset_enters_real_owner_without_an_existing_frontier,
    )

    test_frozen_asset_enters_real_owner_without_an_existing_frontier(
        tmp_path, drift, "v50"
    )


def test_purchase_enumeration_keeps_complete_mineral_name():
    text = (
        "采购商品/接受劳务情况表\n√适用 □不适用\n"
        "关联方    关联交易内容    本期发生额    上期发生额\n"
        "甲公司    炸药及爆破器材    12.34    56.78\n"
        "乙公司    购买铬矿砂及精矿    90.12    34.56\n"
        "出售商品/提供劳务情况表\n"
    )
    names = {b["name"] for b in selection._related_current_goods_bindings(text)}
    assert names == {"炸药", "爆破器材", "铬矿砂及精矿"}
