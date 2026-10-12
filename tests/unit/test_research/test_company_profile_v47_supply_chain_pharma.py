"""Complete original supply-chain/pharma pages through the business owner."""

import json
import re
from decimal import Decimal
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
    / "fixtures/company_profile_v47_supply_chain_pharma_frozen_pages.json"
)


def compact(value):
    return re.sub(r"\s+", "", value or "").replace("（", "(").replace("）", ")")


def num(value):
    return Decimal(compact(value).replace(",", ""))


def match(r, row):
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
    h = n.get("header") or ""
    q = compact(n.get("qualifier"))
    expected = row["dimension"]
    rh = row["header"]
    actor = row["source_actor"]
    if rh == "本公司作为出租方":
        return (
            dim == "lease"
            and h == "本公司作为出租方/本期确认的租赁收入"
            and q == compact("承租方：" + row["counterparty"])
            and r["subject_scope"] == "consolidated_group"
        )
    if rh == "出售商品/提供劳务":
        return (
            dim in [expected, "service"]
            and h == "出售商品/提供劳务/本期发生额"
            and q == compact("交易对方：" + row["counterparty"])
            and r["subject_scope"] == "consolidated_group"
        )
    alias = (
        row["name"] == "合计" and expected == "region" and dim == "business_type"
    ) or (
        row["name"] == "行业间抵消" and expected == "industry" and dim == "adjustment"
    )
    if dim != expected and not alias:
        return False
    if rh.startswith("主营业务分"):
        ok = h in ["", "主营业务收入"] or (
            row["name"] == "行业间抵消"
            and h == "主营业务分行业/分部抵消"
            and q == "分部间抵销"
        )
    elif rh in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        ok = h == "营业收入"
    elif rh == "贸易业务开展情况":
        ok = h == "本期营业收入"
    elif rh == "主要控股参股公司分析":
        ok = h == "控股参股公司/营业收入" and q == compact("子公司：" + row["name"])
    elif rh == "重要非全资子公司的主要财务信息":
        ok = h == "重要非全资子公司/本期营业收入" and q == compact(
            "原子公司：" + row["name"]
        )
    elif rh == "作为出租人的经营租赁":
        ok = h == "作为出租人的经营租赁/租赁收入"
    elif rh == "按治疗领域分类的经营数据":
        ok = h == "治疗领域收入/主营业务"
    elif rh.startswith("报告分部财务信息/"):
        return h == rh.split("/")[-1] and r["subject_scope"] == "consolidated_group"
    elif rh == "母公司合同分类/商品类型":
        ok = h == "全部营业收入/商品类型"
    elif rh in ["母公司合同分类/按经营地区分类", "合同分类/按经营地区分类"]:
        ok = h in (
            ["主营业务收入/经营地区", "营业收入/主营业务收入/经营地区"]
            if r["report"]["instrument_id"] == "600085.SH"
            else ["全部营业收入/经营地区", "营业收入/全部营业收入/经营地区"]
        )
    elif rh == "合同分类/商品类型":
        ok = h == "全部营业收入/商品类型"
    elif rh in [
        "合同收入/商品转让时间",
        "作为承租人/转租收入",
        "其他关联交易/收取商标使用费",
    ]:
        ok = h == rh
    elif rh == "分部地理信息/对外主营业务收入":
        ok = h == "对外主营业务收入"
    elif rh.startswith("年度主要业务数据/"):
        ok = h == rh
    elif rh == "本公司受托管理/承包":
        ok = (
            h == "本公司受托管理/承包/本期确认的托管收益"
            and compact(row["counterparty"]) in q
            and compact(row["source_actor"]) in q
        )
    else:
        ok = False
    if not ok:
        return False
    if actor == "公司及合并子公司":
        return r["subject_scope"] == "consolidated_group"
    if "（母公司）" in actor:
        return r["subject_scope"] == "issuer"
    return r["subject_scope"] == "named_subsidiary" and (
        rh in ["主要控股参股公司分析", "重要非全资子公司的主要财务信息"]
        or compact(actor) in q
    )


def delivered(root, instrument, input_kind="pages"):
    fixture = json.loads(FIXTURE.read_text())[instrument]
    q, e = _drive(
        root,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[input_kind],
        repair_version="v47",
    )
    assert q == e
    raw = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    root.glob("company_profile_common_core.v1/checkpoints/*.json")
                ).read_text()
            )
        )
    )
    return fixture, q, raw


@pytest.mark.parametrize("instrument", ["600057.SH", "600085.SH"])
@pytest.mark.parametrize("input_kind", ["pages", "independent_pages"])
def test_complete_original_contract(tmp_path, instrument, input_kind):
    f, q, raw = delivered(tmp_path, instrument, input_kind)
    missing = [
        row["condition_id"]
        for row in f["source_rows"]
        if not any(r["object_type"] == "Segment" and match(r, row) for r in raw)
    ]
    assert not missing, missing
    missing_roles = [
        role["condition_id"]
        for role in f["native_roles"]
        if not action_matches(raw, role)
    ]
    assert not missing_roles, missing_roles
    for row in f["source_rows"]:
        assert any(r["object_type"] == "Measurement" and match(r, row) for r in raw), (
            row["condition_id"]
        )
    for record in raw:
        if record["object_type"] in {"Segment", "Measurement"}:
            assert any(match(record, row) for row in f["source_rows"]), record[
                "source_native"
            ]
    for role in f["native_roles"]:
        if role["role"] in {
            "product_production",
            "product_processing",
            "native_operating_action",
            "service_provision",
        }:
            continue  # Native operating/service/production conditions use accepted Activities.
        exported_role = {
            "raw_material_procurement": "raw_material_input",
            "operating_energy_input": "energy_consumption",
        }.get(role["role"], role["role"])
        ids = {r["record_id"] for r in action_matches(raw, role)}
        assert any(
            ids.intersection(x["source_record_ids"]) and x["role"] == exported_role
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        ), role["condition_id"]
    negatives = evaluate_negatives(f, raw, q)
    assert len(negatives) == len(f["negative_conditions"])
    assert all(n["passed"] for n in negatives), [
        n for n in negatives if not n["passed"]
    ]
    check_answer_substance(q, instrument)
    for d, rule in f["answers"].items():
        text = _answer(q, d)
        assert all(
            any(compact(t) in text for t in group) for group in rule["required_groups"]
        )


def evaluate_negatives(fixture, records, profile):
    """Check each original frozen negative against accepted facts and answers."""
    co = lambda x: compact(x).replace(",", "")
    out = []
    for n in fixture["negative_conditions"]:
        i = n["instrument_id"]
        rr = records
        f = [r for r in rr if r["object_type"] in ["Segment", "Measurement"]]
        a = [r for r in rr if r["object_type"] in ["Activity", "Relationship"]]
        rule = n["name"]
        wrong = []
        wrong_answers = []
        note = ""
        if rule.startswith("仅上期关联:"):
            wrong = [
                r
                for r in f
                if co(r["source_native"].get("qualifier"))
                == co("交易对方：" + n["counterparty"])
                and "出售商品" in (r["source_native"].get("header") or "")
                and co(r["source_native"]["name"]) == co(n["content"])
            ]
            note = "独立layout原本期空，逐核原交易对手＋内容；正文只引用原双年列表和空本期，无肯定本期收入，不凭同额否定其他合法交易。"
        elif rule.startswith("仅上期出租:"):
            wrong = [
                r
                for r in f
                if co(r["source_native"].get("qualifier"))
                == co("承租方：" + n["counterparty"])
                and "本公司作为出租方" in (r["source_native"].get("header") or "")
            ]
            note = "原三仅上期出租未生成本期事实/零；正文原表保留双年格，未把上期改成本期。"
        elif rule == "空分部格不借相邻":
            wrong = [
                r
                for r in f
                if r["source_native"]["name"] == "分部间抵销"
                and co(r["source_native"]["value"]) == "1725618.51"
                and any(e["page"] == 195 for e in r["evidence"])
            ]
            body = profile["dimensions"]
            wrong_answers = [
                d["dimension_id"]
                for d in body
                if "分部间抵销（非同级业务，不累计）" in d["excerpt"]
                and "营业收入 1,725,618.51万元" in d["excerpt"]
            ]
            note = "对外收入原空抵销格不得借合计；内部交易512028.94原抵销额必须保留。"
        elif rule == "母公司境外收入空":
            wrong = [
                r
                for r in f
                if r["subject_scope"] == "issuer"
                and r.get("dimension", r.get("segment_dimension")) == "region"
                and "境外" in r["source_native"]["name"]
            ]
            note = "象屿母公司境外空未造零、未借境内；实际境内及地区合计保留。"
        elif rule == "重要合营联营不适用":
            wrong = [
                r
                for r in f
                if any(
                    x in (r["source_native"].get("header") or "")
                    for x in ["重要合营", "重要联营"]
                )
            ]
            note = "原不适用状态无合营联营收入事实或肯定正文；非重要投资价值/利润不当营收。"
        elif rule == "报告分部不适用":
            wrong = [
                r for r in f if "报告分部" in (r["source_native"].get("header") or "")
            ]
            note = "象屿母公司收入与子公司不同原表，未生成报告分部。"
        elif rule == "费用表边界":
            wrong = [
                r
                for r in f
                if any(
                    x in (r["source_native"].get("header") or "")
                    for x in ["税金及附加", "销售费用", "管理费用", "财务费用"]
                )
            ]
            note = "实际本期收入原格逐核；原费用章节未生成营收事实或肯定正文。"
        elif rule in [
            "承租费用非收入",
            "关联承租非出租",
            "未来租金非当期营收",
            "支付品牌费非营业收入",
            "母公司投资收益非收入",
            "投资收益非营业收入",
            "成本表非收入",
            "期现毛利非营业收入",
        ]:
            forbidden = {
                "承租费用非收入": ["70047647.01", "16275050.04", "243174411.00"],
                "关联承租非出租": (
                    ["59923512.04", "3253157.61"] if i == "600085.SH" else []
                ),
                "未来租金非当期营收": [],
                "支付品牌费非营业收入": ["35579378.85", "1023.88"],
                "母公司投资收益非收入": ["286094507.50"],
                "投资收益非营业收入": ["801302014.92"],
                "成本表非收入": ["399316.53", "12168.72", "762494.02"],
                "期现毛利非营业收入": ["-18.03"],
            }[rule]
            wrong = [
                r
                for r in f
                if co(r["source_native"]["value"]) in forbidden
                or any(
                    x in (r["source_native"].get("header") or "")
                    for x in [
                        "作为承租人/租金",
                        "承租支出",
                        "投资收益",
                        "资产处置收益",
                        "营业成本",
                        "期现毛利",
                        "未折现租金收款",
                    ]
                )
            ]
            # The unrelated genuine rental cells were matched to their exact original rows in the object review.
            note = "逐核原非收入数字/章节与全量实际收入格，未把承租、未来收款、成本、投资/处置收益或毛利肯定为本期营业收入；合法出租收入独立保留。"
        elif rule == "客户排名非销售渠道":
            wrong = [
                r
                for r in f
                if r.get("dimension", r.get("segment_dimension")) == "sales_mode"
            ]
            note = "象屿客户/供应商排名未生成销售渠道或采购营收；贸易营收3852亿元为独立真实原列。"
        elif rule == "订单计划非已销售":
            wrong = [
                r
                for r in a
                if any(
                    t in r["source_native"]["name"]
                    for t in ["53艘", "114艘", "2029", "待满产"]
                )
            ]
            note = "未把订单/排产数量肯定为本期已交付；正文保留53艘订单、28艘交付、114艘在手及2029排产/待满产原状态。"
        elif rule == "控股股东制造非发行人制造":
            wrong = [
                r
                for r in a
                if r.get("action") in ["produces", "processes"]
                and any(
                    t in r["source_native"]["name"] for t in ["不锈钢", "铝", "玉米"]
                )
            ]
            note = "原控股股东制造协同限定在原经营片段/正文保留，未转发行人具名制造。"
        elif rule == "未来五年盈利模式不当前化":
            wrong = [
                r
                for r in a
                if any(
                    t in r["source_native"]["name"]
                    for t in ["2026", "2030", "未来五年"]
                )
            ]
            note = "未来盈利结构未作为本期新收入；当前p13服务收益/规模集拼/价差机制按原状态独立交付。"
        elif rule == "主要子公司表无收入列":
            wrong = [r for r in f if any(e["page"] == 32 for e in r["evidence"])]
            note = "p32原总资产/净资产/净利润未生成收入；正文原完整表引用有列名，不肯定为营业收入，p179本期三子公司收入保留。"
        elif rule == "受托不适用":
            wrong = [
                r
                for r in f
                if any(
                    t in (r["source_native"].get("header") or "")
                    for t in ["受托", "托管"]
                )
            ]
            note = "同仁堂不适用受托章节未借出租表生成托管收入；象屿有真实受托不跨公司拒绝。"
        elif rule == "采购不推投入":
            wrong = [r for r in a if r.get("relation_type") == "material_input"]
            note = "零售采购未推公司实际制造投入；原药品类别占比未作营收金额。"
        elif rule == "注册许可非销售":
            wrong = [
                r
                for r in a
                if r.get("action") == "sells"
                and any(
                    t in r["source_native"]["name"]
                    for t in ["金钗石斛散", "芪参颗粒", "加拿大许可", "香港注册"]
                )
            ]
            note = (
                "研发长表保留上市后研究/临床状态，不将注册许可或Ⅱ期入组转为当前销售。"
            )
        elif rule == "注销不维持当前子公司":
            names = [
                "宝鸡达吉康",
                "安国）中药材物流",
                "圣盖博加洲",
                "本草药业科技",
                "同仁九州",
            ]
            wrong = [
                r
                for r in a
                if any(
                    t in ((r.get("source_actor") or "") + r["source_native"]["name"])
                    for t in names
                )
            ]
            note = "已交付企业集团表无五注销主体当前经营；正文保留天成元9月28日取得控制和购买资产的原限定，不肯定全年控制。"
        else:
            raise AssertionError(rule)
        out.append(
            {
                "condition": n,
                "evaluation_status": "evaluated",
                "actual_profile_exists": True,
                "accepted_facts_and_all_answered_texts_checked": True,
                "answered_dimension_ids": [
                    d["dimension_id"] for d in profile["dimensions"] if d["answered"]
                ],
                "passed": not wrong and not wrong_answers,
                "wrong_accepted_object_ids": [r["record_id"] for r in wrong],
                "wrong_answer_dimension_ids": wrong_answers,
                "reason": note,
            }
        )
    return out


def check_answer_substance(profile, instrument):
    """Verify concrete states and mechanisms beyond the coarse keyword groups."""
    principal = compact(_answer(profile, "principal_business"))
    products = compact(_answer(profile, "products_services"))
    revenue = compact(_answer(profile, "revenue_model"))
    if instrument == "600057.SH":
        for text in [
            "铁路物流",
            "新能源物流",
            "铝产业物流",
            "农产品物流",
            "象道物流集团有限公司",
            "象屿海装",
            "订单53艘",
            "交付28艘",
            "114艘",
            "2029",
        ]:
            assert text in principal, text
        for text in ["钢坯", "铝板", "硅铁", "无烟煤", "选矿", "油脂加工", "租船"]:
            assert text in products, text
        for text in [
            "赚取服务费",
            "规模集拼收益",
            "价差收益",
            "货权已转移",
            "收款的凭据",
            "代理人",
            "按照履约进度",
            "转租使用权资产取得的收入",
        ]:
            assert text in revenue, text
    else:
        for text in [
            "北京同仁堂科技发展股份有限公司",
            "同仁堂国药",
            "零售",
            "电商",
            "2025年9月28日",
            "按照购买资产进行会计处理",
            "注销",
            "实质控制",
            "Ⅱ期",
        ]:
            assert text in principal, text
        for text in ["加拿大", "香港", "注册"]:
            assert text in principal, text
        for text in [
            "愈风宁心滴丸",
            "偏瘫复原丸",
            "柏子养心系列",
            "调经促孕丸",
            "人参",
            "山茱萸",
            "甘草",
            "黄芪",
            "医疗器械",
        ]:
            assert text in products, text
        for text in [
            "收到货物并验收",
            "扣除估计的折扣",
            "系统显示商品转出",
            "销售流水单",
            "现金缴款单",
            "刷卡记录清单",
            "提供服务的会计期间",
            "商标使用费",
        ]:
            assert text in revenue, text


def test_v47_identity_keeps_five_repairs_and_default():
    identity = {**default_processing_identity(), "revenue_sentence_repair": "v47"}
    for name in [
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ]:
        assert getattr(selection, name)(identity)
    assert default_processing_identity().get("revenue_sentence_repair") != "v47"


@pytest.mark.parametrize("drift", [False, True, "between_reads"])
def test_successor_exact_asset_binding(tmp_path, drift):
    from tests.unit.test_research.test_company_profile_v41_telecom_pharma import (
        test_frozen_asset_enters_real_owner_without_an_existing_frontier,
    )

    test_frozen_asset_enters_real_owner_without_an_existing_frontier(
        tmp_path, drift, "v47"
    )


@pytest.mark.parametrize("instrument", ["600050.SH"])
def test_successor_keeps_prior_telecom_units_and_subjects(tmp_path, instrument):
    from tests.unit.test_research.test_company_profile_v41_telecom_pharma import (
        test_full_source_business_bodies_income_and_named_actions,
    )

    test_full_source_business_bodies_income_and_named_actions(
        tmp_path, instrument, "pages", "v47"
    )
