"""Tourism/wine native owner disclosure: original bodies, rows and actors."""

import json
from pathlib import Path

import pytest

from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive
from tests.unit.test_research.test_company_profile_v45_campus_auto import compact

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v46_tourism_wine_frozen_pages.json"
)


def delivered(root, instrument, kind):
    fixture = json.loads(FIXTURE.read_text())[instrument]
    query, exported = _drive(
        root,
        instrument=instrument,
        fixture=fixture,
        pages=fixture[kind],
        repair_version="v46",
    )
    assert query == exported
    checkpoint = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    raw = list(_iter_accepted_raw(checkpoint))
    assert len({r["record_id"] for r in raw}) == len(raw)
    return fixture, query, raw


def test_successor_identity_inherits_five_repairs_and_keeps_default():
    from research.company_profile import core_evidence_selection as selection
    from research.company_profile.execution import default_processing_identity

    identity = {**default_processing_identity(), "revenue_sentence_repair": "v46"}
    assert "revenue_sentence_repair" not in default_processing_identity()
    assert all(
        fn(identity)
        for fn in (
            selection.revenue_sentence_repair_requested,
            selection.named_role_repair_requested,
            selection.service_operating_energy_requested,
            selection.core_answer_repair_requested,
            selection.source_delivery_repair_requested,
        )
    )


def check_bodies(fixture, query, instrument):
    for dim, rule in fixture["answers"].items():
        text = _answer(query, dim)
        assert all(
            any(compact(token) in text for token in group)
            for group in rule["required_groups"]
        ), dim
    expected = {
        "600054.SH": {
            "principal_business": [
                "投入运营",
                "前期工作",
                "公开测试",
                "本公司没有重要的合营或联营企业",
            ],
            "products_services": [
                "SaaS平台已成功对外拓展外部部分景区",
                "部分产品已投入租赁或出售",
                "旅游产品和活动策划",
            ],
            "revenue_model": [
                "扣减代黄山风景区管理委员会",
                "扣减代扣款项后的索道票收入",
                "取得收款权利",
                "定期向黄山管委会结算支付",
                "计入营业成本",
            ],
        },
        "600084.SH": {
            "principal_business": [
                "集葡萄种植、生产加工、贸易、科研为一体",
                "科研平台",
            ],
            "products_services": [
                "西域系列",
                "尼雅高端系列",
                "新天系列",
                "生产管理及技术指导服务",
                "包装材料",
            ],
            "revenue_model": [
                "每年签订一次",
                "先款后货或现款现货",
                "提供适当账期",
                "提货单、入库单或商品货权确认单",
                "完成发货并收到货款",
                "完成对账",
                "货运单",
                "累计已确认收入极可能不会发生重大转回",
            ],
        },
    }[instrument]
    for dim, tokens in expected.items():
        text = _answer(query, dim)
        for token in tokens:
            assert compact(token) in text, (dim, token)


@pytest.mark.parametrize("instrument", ["600054.SH", "600084.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_native_bodies(tmp_path, instrument, kind):
    fixture, query, _ = delivered(tmp_path, instrument, kind)
    check_bodies(fixture, query, instrument)


num = lambda s: compact(s).replace(",", "")
aliases = {
    "玉屏索道": "黄山玉屏客运索道有限责任公司",
    "太平索道": "黄山太平索道有限公司",
    "黄山徽商故里集团": "黄山徽商故里文化发展集团有限公司",
    "太平湖文旅": "黄山太平湖文化旅游有限公司",
    "花山谜窟": "黄山市花山谜窟旅游开发有限责任公司",
    "西海饭店": "黄山西海饭店有限责任公司",
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
    h = n.get("header") or ""
    q = n.get("qualifier") or ""
    expected = row["dimension"]
    alias = (
        row["header"] == "出售商品/提供劳务"
        and expected == "product"
        and dim == "service"
    ) or (
        row["name"] == "成品酒小计"
        and expected == "product"
        and dim == "total"
        and q == "小计"
    )
    if dim != expected and not alias:
        return False
    if row["header"] in ["合并营业收入和营业成本", "母公司营业收入和营业成本"]:
        if h != "营业收入":
            return False
    elif row["header"].startswith("主营业务分"):
        if h not in ["", "主营业务收入"]:
            return False
    elif row["header"] == "主要控股参股公司分析":
        if h != "控股参股公司/营业收入" or compact(q) != compact(
            "子公司：" + n["name"]
        ):
            return False
    elif row["header"] == "重要非全资子公司的主要财务信息":
        if h != "重要非全资子公司/本期营业收入":
            return False
    elif row["header"] == "出售商品/提供劳务":
        if h != "出售商品/提供劳务/本期发生额" or compact(q) != compact(
            "交易对方：" + row["counterparty"]
        ):
            return False
    elif row["header"] == "报告分部财务信息":
        if h != "主营业务收入":
            return False
    elif row["header"].startswith(
        ("产品情况/", "销售渠道/", "区域情况/", "线上销售情况/", "主营业务构成/")
    ):
        if h != row["header"]:
            return False
        if row["counterparty"] and compact(q) != compact(
            "交易对方：" + row["counterparty"]
        ):
            return False
    elif row["header"] == "作为出租人的经营租赁":
        if h != "作为出租人的经营租赁/租赁收入":
            return False
    else:
        return False
    actor = row["source_actor"]
    if actor == "公司及合并子公司":
        return r["subject_scope"] == "consolidated_group"
    if "（母公司）" in actor:
        return r["subject_scope"] == "issuer"
    return r["subject_scope"] == "named_subsidiary" and compact(actor) == compact(
        aliases.get(n["name"], n["name"])
    )


def action_matches(raw, role):
    from tests.unit.test_research.test_company_profile_v43_energy_pharma import (
        action_matches as exact,
    )

    names = {
        "黄山旅游官方平台运营": "黄山旅游官方平台",
        "新媒体运营": "新媒体",
        "OTA业务承接": "OTA",
    }
    if role["name"] in names.values():
        # The original contract accidentally repeats the platform alias for
        # media and OTA. Keep the original file; match each real object exactly.
        role = {**role, "native_aliases": [role["name"]]}
    # Existing source-contract interpretation: operating the named travel
    # services supplies those services; the composite planning condition stays whole.
    mapped = [
        {
            **r,
            "source_native": {
                **r["source_native"],
                "name": names.get(
                    compact(r["source_native"]["name"]), r["source_native"]["name"]
                ),
            },
            "action": "provides_service"
            if r.get("action") == "operates"
            and role["name"] in {"国内旅游", "入境旅游", "出境旅游", "定制游", "疗休养"}
            else r.get("action"),
        }
        for r in raw
    ]
    return exact(mapped, role)


@pytest.mark.parametrize("instrument", ["600054.SH", "600084.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_complete_named_actions(tmp_path, instrument, kind):
    fixture, query, raw = delivered(tmp_path, instrument, kind)
    for role in fixture["native_roles"]:
        matches = action_matches(raw, role)
        assert matches, (
            role["name"],
            role["source_actor"],
            role["source_actions"],
        )
        if role["role"] in {
            "product_production",
            "product_processing",
            "native_operating_action",
            "service_provision",
        }:
            continue
        expected_role = {
            "raw_material_procurement": "raw_material_input",
            "operating_energy_input": "energy_consumption",
        }.get(role["role"], role["role"])
        assert any(
            m["record_id"] in x["source_record_ids"] and x["role"] == expected_role
            for m in matches
            for x in query["commodity_exposure"]["assessment"]["exposures"]
        ), role["condition_id"]
    assert not any(
        r["object_type"] == "Activity" and r["source_native"]["name"] == "旅游商品在线"
        for r in raw
    )
    assert not any(
        r.get("relation_type") == "material_input"
        and r["source_native"]["name"] in {"包装材料", "能源"}
        for r in raw
    )
    if instrument == "600054.SH":
        assert not any(
            r.get("action") in {"sells", "produces"}
            and any(
                t in r["source_native"]["name"]
                for t in ("登山杖", "髌骨带", "外骨骼", "机器狗")
            )
            for r in raw
        )
        assert (
            len(
                [
                    r
                    for r in raw
                    if r["object_type"] == "Activity"
                    and r.get("source_verb") in {"涵盖", "接待"}
                    and r["source_native"]["name"] != "旅游商品"
                ]
            )
            >= 12
        )


def negative_wrong(rr, n):
    i = n["instrument_id"]
    f = [r for r in rr if r["object_type"] in ["Segment", "Measurement"]]
    a = [r for r in rr if r["object_type"] in ["Activity", "Relationship"]]
    rule = n["name"]
    wrong = []
    if rule.startswith("仅上期销售:"):
        wrong = [
            r
            for r in f
            if compact(r["source_native"].get("qualifier"))
            == compact("交易对方：" + n["counterparty"])
            and "出售商品" in (r["source_native"].get("header") or "")
        ]
    elif rule in ["不适用合营联营无收入", "不适用权益无收入"]:
        wrong = [
            r
            for r in f
            if any(
                x in (r["source_native"].get("header") or "")
                for x in ["重要合营", "重要联营"]
            )
        ]
    elif rule == "关联出租不适用":
        wrong = [
            r
            for r in f
            if "本公司作为出租方" in (r["source_native"].get("header") or "")
        ]
    elif rule == "报告分部不适用":
        wrong = [r for r in f if "报告分部" in (r["source_native"].get("header") or "")]
    elif rule == "徐州收入空不借利润":
        wrong = [
            r
            for r in f
            if "徐州" in r["source_native"]["name"]
            or (
                r["source_native"]["value"] == "158.81"
                and r["source_native"]["unit"] == "万元"
            )
        ]
    elif rule == "测试装备不逐项销售":
        wrong = [
            r
            for r in a
            if any(
                x in r["source_native"]["name"]
                for x in ["登山杖", "髌骨带", "外骨骼", "机器狗"]
            )
            and r.get("action") in ["sells", "produces"]
        ]
    elif rule == "前期工程不当前酒店服务":
        wrong = [
            r
            for r in a
            if "升级改造" in r["source_native"]["name"]
            and r.get("action") in ["operates", "provides_service"]
        ]
    elif rule == "行业政策非公司动作":
        wrong = [
            r
            for r in a
            if any(x in r["source_native"]["name"] for x in ["冰雪", "低空", "银发"])
        ]
    elif rule == "品牌不伪商品动作":
        wrong = [
            r
            for r in a
            if r["source_native"]["name"] in ["西域", "尼雅", "新天", "天方夜谭"]
        ]
    elif rule == "采购不推投入":
        wrong = [
            r
            for r in a
            if r.get("relation_type") == "material_input"
            and any(x in r["source_native"]["name"] for x in ["能源", "包装"])
        ]
    elif rule == "无在建产能":
        wrong = [r for r in a if "在建产能" in r["source_native"]["name"]]
    elif rule in ["抵销前行业不混合并", "跨表口径不可混并"]:
        wrong = []
        if i == "600054.SH":
            wrong = [
                r
                for r in f
                if r["source_native"]["name"] == "合计"
                and num(r["source_native"]["value"]) == "2404938872.58"
            ]
        else:
            wrong = [
                r
                for r in f
                if r["source_native"]["name"] == "华北地区"
                and num(r["source_native"]["value"]) == "39698365.90"
                and any(e["page"] == 17 for e in r["evidence"])
                or r["source_native"]["name"] == "直销（含团购）"
                and num(r["source_native"]["value"]) == "48661019.36"
                and any(e["page"] == 17 for e in r["evidence"])
            ]
    else:
        # The declared exclusions refer to exact original non-income lines; pair/page/amount checked, not a keyword match.
        values = {
            "代收非收入": ["258273193.40", "99359311.73", "142101740.37"],
            "母公司投资收益非收入": ["2755088.52"],
            "承租支出非租赁收入": ["64352.09", "1505593.24", "60377.36", "421350.00"],
        }.get(rule, [])
        if rule in ["成本表非收入", "采购不营业收入", "投资收益非收入", "费用表不收入"]:
            wrong = [
                r
                for r in f
                if any(
                    x in (r["source_native"].get("header") or "")
                    for x in {
                        "成本表非收入": ["营业成本", "成本构成"],
                        "采购不营业收入": ["采购商品", "接受劳务"],
                        "投资收益非收入": ["投资收益"],
                        "费用表不收入": ["销售费用", "税金及附加"],
                    }[rule]
                )
            ]
        else:
            assert values, rule
            wrong = [r for r in f if num(r["source_native"]["value"]) in values]
    return wrong


@pytest.mark.parametrize("instrument", ["600054.SH", "600084.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
def test_native_income_pairs_and_negatives(tmp_path, instrument, kind):
    fixture, _query, raw = delivered(tmp_path, instrument, kind)
    missing = [
        row["condition_id"]
        for row in fixture["source_rows"]
        if not any(r["object_type"] == "Segment" and native_match(r, row) for r in raw)
    ]
    assert not missing, missing
    for row in fixture["source_rows"]:
        assert any(
            r["object_type"] == "Measurement" and native_match(r, row) for r in raw
        ), row["condition_id"]
    for n in fixture["negative_conditions"]:
        assert not negative_wrong(raw, n), n["name"]
    if instrument == "600084.SH":
        assert any(
            r["object_type"] == "Measurement"
            and r["source_native"]["name"] == "正常经营之外的其他业务收入"
            and r["source_native"]["value"] == "555.89"
            for r in raw
        )
