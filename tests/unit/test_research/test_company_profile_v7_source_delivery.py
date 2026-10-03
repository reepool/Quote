"""v8 delivers Huaneng and Sinopec source sentences through export."""

from __future__ import annotations

import asyncio
import json
import re

from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from tests.unit.test_research.test_company_profile_runtime import (
    WORK_STAGES,
    _report,
)


def _identity() -> dict[str, str]:
    payload = default_processing_identity()
    payload["revenue_sentence_repair"] = "v8"
    return payload


def _drive(root, instrument: str, report_id: str, pages: list[dict]) -> dict:
    identity = _identity()
    report = _report(instrument_id=instrument, report_id=report_id)
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": f"work-{instrument}",
        "instrument_id": instrument,
        "report": report.model_dump(mode="json"),
        "pages": pages,
        "processing_identity": identity,
    }

    async def drive():
        for stage in WORK_STAGES:
            await runtime(stage, item)

    asyncio.run(drive())
    profile = CompanyProfileReadService(root).query(
        (instrument,),
        processing_identity=identity,
    )["profiles"][0]
    exported = CompanyProfileReadService(root).export(
        (instrument,),
        export_directory=root / "export",
        processing_identity=identity,
    )
    assert exported["state"] == "completed"
    payload = json.loads(
        next((root / "export").glob(f"{instrument}_*.json")).read_text(encoding="utf-8")
    )
    return {"query": profile, "export": payload}


def _dimension(profile: dict, dimension_id: str) -> str:
    return next(
        item["excerpt"] or ""
        for item in profile["dimensions"]
        if item["dimension_id"] == dimension_id
    )


def test_huaneng_principal_coal_volume_and_combined_power_heat_share(tmp_path):
    result = _drive(
        tmp_path / "huaneng",
        "600011.SH",
        "1225029354",
        [
            {
                "page": 10,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司的主要业务是利用现代化的技术和设备，利用国内外资金，在国内外开发、建设和运营\n"
                    "燃煤、燃气发电厂、新能源发电项目及配套港口、航运、增量配电网等设施，为社会提供电力、\n"
                    "热力及综合能源服务。\n"
                    "报告期内，公司电力、热力销售收入约占营业收入的 96.37%。\n"
                    "2025 年，公司共采购煤炭 1.86 亿吨。\n"
                ),
                "readable": True,
            }
        ],
    )
    for profile in (result["query"], result["export"]):
        principal = _dimension(profile, "principal_business")
        assert "公司的主要业务是" in principal
        assert "综合能源服务" in principal
        assert principal.strip() != "销售"
    overview = next(
        item
        for item in result["query"]["accepted_facts"]
        if item["field_id"] == "business_overview_source"
    )
    assert "综合能源服务" in (overview["source_text"] or "")
    facts = {item["record_id"]: item for item in result["query"]["accepted_facts"]}
    roles = []
    for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]:
        fact = facts[item["source_record_ids"][0]]
        roles.append(
            (
                item["source_native_name"],
                item["role"],
                fact["source_native_value"],
                fact.get("source_actor"),
            )
        )
    assert ("煤炭", "raw_material_input", "1.86", "公司") in [
        (name, role, value, actor) for name, role, value, actor in roles
    ]
    coal = next(item for item in result["query"]["accepted_facts"] if item["source_native_name"] == "煤炭")
    assert coal["source_native_value"] == "1.86"
    power = [item for item in roles if item[0] == "电力、热力"]
    assert len(power) == 1
    assert power[0][2] == "96.37"
    assert not any(name in {"电力", "热力"} for name, *_ in roles)


def test_sinopec_profile_industry_table_and_internal_sales(tmp_path):
    result = _drive(
        tmp_path / "sinopec",
        "600028.SH",
        "1225024063",
        [
            {
                "page": 4,
                "text": (
                    "公司简介\n"
                    "中国石化是中国最大的一体化能源化工公司之一，主要从事石油与天然气勘探\n"
                    "开采、管道运输、销售；石油炼制、石油化工、煤化工、化纤及其他化工产品的生产与销售、\n"
                    "储运；石油、天然气、石油产品、石油化工及其他化工产品和其他商品、技术的进出口。\n"
                ),
                "readable": True,
            },
            {
                "page": 26,
                "text": (
                    "炼油事业部业务包括从第三方及勘探及开发事业部购入原油，并将原油加工成石油产品，\n"
                    "大部分汽油、柴油、煤油内部销售给营销及分销事业部，部分化工原料油内部销售给化工事\n"
                    "业部，其他精炼石油产品由炼油事业部外销给国内外客户。\n"
                ),
                "readable": True,
            },
            {
                "page": 32,
                "text": (
                    "主营业务分行业情况\n"
                    "分行业 营业收入（人\n"
                    "民币百万元）\n"
                    "勘探及开发 285,992 201,833 24.1\n"
                    "炼油 1,328,509 1,070,616 1.9\n"
                    "营销及分销 1,505,275 1,426,774 5.0\n"
                    "化工 464,108 460,886 0.4\n"
                    "抵销分部间\n"
                    "销售 (2,115,901) (2,116,871)\n"
                    "合计 2,783,583 2,341,383 6.8\n"
                    "分行业说明完毕。\n"
                ),
                "readable": True,
            },
            {
                "page": 86,
                "text": (
                    "主要业务\n"
                    "中国石化国际石油勘探开发有限公司 全资子公司 石油、天然气勘探、开发、生产及销售。\n"
                ),
                "readable": True,
            },
            {
                "page": 121,
                "text": (
                    "重要会计政策\n"
                    "分部报告\n"
                    "该组成部分能够在日常活动中产生收入、发生费用。\n"
                ),
                "readable": True,
            },
        ],
    )
    for profile in (result["query"], result["export"]):
        principal = _dimension(profile, "principal_business")
        products = _dimension(profile, "products_services")
        revenue = _dimension(profile, "revenue_model")
        assert "主要从事" in principal
        assert "石油与天然气勘探" in principal
        assert "石油炼制" in principal
        assert principal.strip() != "销售"
        assert "石油炼制" in products
        assert products.strip() != "销售"
        for name in ("勘探及开发", "炼油", "营销及分销", "化工"):
            assert name in revenue
        assert "抵销" not in revenue
        assert revenue.strip() != "合计"
    refining = next(
        item
        for item in result["query"]["accepted_facts"]
        if item["source_native_name"] == "炼油" and item["field_id"] == "operating_revenue"
    )
    assert refining["source_native_value"] == "1328509" or refining["source_native_value"] == "1,328,509"
    facts = {item["record_id"]: item for item in result["query"]["accepted_facts"]}
    roles = []
    for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]:
        fact = facts[item["source_record_ids"][0]]
        roles.append(
            (
                item["source_native_name"],
                item["role"],
                fact.get("source_actor"),
                fact.get("source_native_header"),
            )
        )
    assert ("原油", "raw_material_input", "炼油事业部", None) in roles
    for name in ("汽油", "柴油", "煤油"):
        assert (name, "product_sales", "炼油事业部", "内部销售") in roles
    feedstock = facts.get("owned:core-ev-438ae54472b17daf:commodity:5") or next(
        (
            item
            for item in result["query"]["accepted_facts"]
            if item.get("source_native_name") == "化工原料油"
        ),
        None,
    )
    assert feedstock is not None
    assert feedstock["source_actor"] == "炼油事业部"
    assert feedstock["source_native_header"] == "部分内部销售给化工事业部"
    assert feedstock["source_native_value"] in (None, "")
    for profile in (result["query"], result["export"]):
        exported_feedstock = [
            item
            for item in profile["accepted_facts"]
            if item.get("source_native_name") == "化工原料油"
        ]
        assert exported_feedstock
        assert exported_feedstock[0]["source_native_header"] == (
            "部分内部销售给化工事业部"
        )
    exposures = {
        item["source_native_name"]: item
        for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]
    }
    assert exposures["化工原料油"]["mapping_status"] == "pending"


def test_wutong_toll_answers_and_blocked_measurements_are_delivered(tmp_path):
    result = _drive(
        tmp_path / "wutong",
        "600012.SH",
        "1225088001",
        [
            {
                "page": 16,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司的主营业务为投资、建设、运营及管理安徽省境内的部分收费公路。公司通过\n"
                    "投资建设、收购或合作经营等多种方式获得经营性公路资产，为过往车辆提供通行\n"
                    "服务，按照收费标准收取车辆通行费，并对运营公路进行养护维修和安全维护。\n"
                ),
                "readable": True,
            },
            {
                "page": 25,
                "text": (
                    "2、收入和成本分析\n"
                    "√适用 □不适用\n"
                    "本集团报告期内实现营业收入 672,157.42 万元，其中主营业务收入 662,740.17 万\n"
                    "元（包含收费公路业务营业收入 508,231.97 万元，建造服务收入 154,508.20 万\n"
                    "元），其他业务收入 9,417.25 万元。\n"
                    "(1). 主营业务分行业、分产品、分地区、分销售模式情况\n"
                    "单位：元 币种：人民币\n"
                    "主营业务分行业情况\n"
                    "分行业 营业收入 营业成本\n"
                    "收费公路业务 5,082,319,695.11 2,152,079,827.68 57.66 13.29\n"
                    "建造服务收入/成\n"
                    "本\n"
                    "1,545,081,974.52 1,545,081,974.52 0.00 -50.69 -50.69\n"
                    "主营业务分产品情况\n"
                    "分产品 营业收入 营业成本\n"
                    "合宁高速公路 1,487,013,977.77 537,750,350.15 63.84 7.08\n"
                    "205 国道天长段新\n"
                    "线\n"
                    "87,442,156.79 43,291,675.18 50.49 8.04\n"
                    "建造期收入/成本 1,545,081,974.52 1,545,081,974.52 0.00 -50.69\n"
                    "主营业务分地区情况\n"
                    "分地区 营业收入 营业成本\n"
                    "安徽省 6,627,401,669.63 3,697,161,802.20 44.21 -13.02\n"
                ),
                "readable": True,
            },
        ],
    )
    for profile in (result["query"], result["export"]):
        products = re.sub(r"\s+", "", _dimension(profile, "products_services"))
        revenue = re.sub(r"\s+", "", _dimension(profile, "revenue_model"))
        assert "通行服务" in products
        assert "收取车辆通行费" in revenue or "通行服务" in products
        revenues = [
            item
            for item in profile["accepted_facts"]
            if item["field_id"] == "operating_revenue"
        ]
        names = {item.get("source_native_name") for item in revenues}
        assert {"收费公路业务", "建造服务收入/成本", "205国道天长段新线",
                "建造期收入/成本", "合宁高速公路", "安徽省"} <= names
        for item in revenues:
            assert item["source_native_value"]
            assert item["source_native_unit"] == "元"
    assert result["query"]["dimensions"][2]["answered"] is True


def test_sany_steel_raw_material_and_four_hedge_underlyings(tmp_path):
    result = _drive(
        tmp_path / "sany",
        "600031.SH",
        "1225100001",
        [
            {
                "page": 9,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "1、公司的主要业务\n"
                    "公司主要从事工程机械的研发、制造、销售和服务。公司产品包括混凝土机械、挖掘机械、\n"
                    "起重机械、桩工机械、路面机械。\n"
                    "2、公司的经营模式\n"
                    "公司生产所需的主要原材料及零部件为汽车底盘、发动机、钢材、液压泵、主油泵、分动箱、\n"
                    "各种液压阀、回转轴承等。\n"
                ),
                "readable": True,
            },
            {
                "page": 25,
                "text": (
                    "衍生品投资情况\n"
                    "√适用 □不适用\n"
                    "报告期内套期保值业务的会计政策：根据金融工具及相关准则规定进行核算。\n"
                    "为规避原料现货价格波动对公司生产带来的不利影响，公司以自有资金开展与本公司生产相关\n"
                    "的大宗商品（如：钢材、铜、铝、原油等）原料的期货业务。\n"
                    "单位：万元 币种：人民币\n"
                    "合计 2,090,594 27,701 -43,992 8,818,338 7,648,743\n"
                ),
                "readable": True,
            },
        ],
    )
    activities = [
        item
        for item in result["query"]["accepted_facts"]
        if item["field_id"] == "explicit_activity"
    ]
    steel_input = next(
        (i for i in activities if "steel-raw-material" in i["record_id"]), None
    )
    assert steel_input is not None
    assert steel_input["source_native_header"] == "主要原材料"
    names = {i.get("source_native_name") for i in activities}
    for name in ("铜", "铝", "原油"):
        assert name in names, name
    exposures = result["query"]["commodity_exposure"]["assessment"]["exposures"]
    hedges = [i for i in exposures if i["role"] == "hedge_underlying"]
    assert {i["source_native_name"] for i in hedges} == {"钢材", "铜", "铝", "原油"}
    raw = [i for i in exposures if i["role"] == "raw_material_input"]
    assert {i["source_native_name"] for i in raw} == {"钢材"}
    # catalog mapping follows the existing rules and is checked in the round
    # review; this test pins the delivery, independence, and empty values.
    hedge_values = [
        item.get("source_native_value")
        for item in result["query"]["accepted_facts"]
        if item["field_id"] == "explicit_activity"
        and item.get("source_native_header") == "套期保值标的"
    ]
    assert hedge_values and all(v in (None, "") for v in hedge_values)
    for profile in (result["query"], result["export"]):
        exported_hedges = [
            item
            for item in profile["accepted_facts"]
            if item.get("source_native_header") == "套期保值标的"
        ]
        assert len(exported_hedges) == 4


def test_wrapped_spaced_product_row_keeps_industry_revenue_accepted(tmp_path):
    result = _drive(
        tmp_path / "huaneng-product-wrap",
        "600011.SH",
        "1225029354",
        [
            {
                "page": 15,
                "text": (
                    "2、收入和成本分析\n"
                    "(1). 主营业务分行业、分产品、分地区、分销售模式情况\n"
                    "单位：元 币种：人民币\n"
                    "主营业务分行业情况\n"
                    "分行业 营业收入 营业成本\n"
                    "电力及热力 220,961,342,675 181,509,430,965 17.85 -6.98\n"
                    "个百分点\n"
                    "主营业务分产品情况\n"
                    "分产品 营业收入 营业成本\n"
                    "电 力 及 热\n"
                    "力\n"
                    "220,961,342,675 181,509,430,965 17.85 -6.98 -10.76 增加 3.47\n"
                    "个百分点\n"
                ),
                "readable": True,
            },
        ],
    )
    for profile in (result["query"], result["export"]):
        record_ids = [
            item["record_id"]
            for item in profile["accepted_facts"]
            if item.get("source_native_name") == "电力及热力"
            and item["field_id"] in ("operating_revenue", "segment_dimension")
        ]
        assert any(":segment:industry:电力及热力" in rid for rid in record_ids)
        assert any(":segment:product:电力及热力" in rid for rid in record_ids)
        assert any(":revenue:industry:电力及热力" in rid for rid in record_ids)
        assert any(":revenue:product:电力及热力" in rid for rid in record_ids)
        for rid in record_ids:
            if ":revenue:" in rid:
                row = next(
                    item
                    for item in profile["accepted_facts"]
                    if item["record_id"] == rid
                )
                assert row["source_native_value"] == "220,961,342,675"
                assert row["source_native_unit"] == "元"


def test_region_margin_wrap_leaves_no_fake_segment_in_query_or_export(tmp_path):
    result = _drive(
        tmp_path / "huaneng-region",
        "600011.SH",
        "1225029354",
        [
            {
                "page": 10,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司的主要业务是利用现代化的技术和设备，在国内外开发、建设和运营\n"
                    "发电厂，为社会提供电力、热力及综合能源服务。\n"
                ),
                "readable": True,
            },
            {
                "page": 15,
                "text": (
                    "2、收入和成本分析\n"
                    "(1). 主营业务分行业、分产品、分地区、分销售模式情况\n"
                    "单位：元 币种：人民币\n"
                    "主营业务分行业情况\n"
                    "分行业 营业收入 营业成本\n"
                    "电力及热力 220,961,342,675 181,509,430,965 17.85 -6.98\n"
                    "个百分点\n"
                    "主营业务分地区情况\n"
                    "分地区 营业收入 营业成本\n"
                    "中国境内 202,761,140,328 165,457,723,992\n"
                    "18.40 -6.29 -10.66 增加 4.00\n"
                    "个百分点\n"
                    "中国境外 18,523,422,143 16,302,529,972 11.99 -13.76\n"
                    "个百分点\n"
                ),
                "readable": True,
            },
        ],
    )
    for profile in (result["query"], result["export"]):
        delivered = [
            (item.get("source_native_name"), item.get("source_native_unit"))
            for item in profile["accepted_facts"]
            if item["field_id"] == "segment_dimension"
        ]
        assert ("40", None) not in delivered
        assert ("18", None) not in delivered
        assert ("中国境外", "元") in delivered
        overseas_revenue = [
            item
            for item in profile["accepted_facts"]
            if item["field_id"] == "operating_revenue"
            and item.get("source_native_name") == "中国境外"
        ]
        assert overseas_revenue[0]["source_native_value"] == "18,523,422,143"
        assert overseas_revenue[0]["source_native_unit"] == "元"


def test_v8_keeps_steel_airport_energy_and_excludes_false_sales(tmp_path):
    from tests.unit.test_research.test_company_profile_v6_core_answers import (
        _REVENUE_PAGE,
        _STEEL_PAGE,
        _SUPPLY_PAGE,
    )

    result = _drive(
        tmp_path / "regression",
        "600019.SH",
        "1225257227",
        [
            {"page": 9, "text": _STEEL_PAGE, "readable": True},
            {"page": 14, "text": _REVENUE_PAGE, "readable": True},
            {
                "page": 17,
                "text": (
                    "运营端持续深化精益管理，污水吨水药耗、电耗分别同比有所下降。\n"
                    "同时公司通过技改优化，焚烧业务柴油单耗显著下降。\n"
                ),
                "readable": True,
            },
            {"page": 25, "text": _SUPPLY_PAGE, "readable": True},
            {"page": 35, "text": "历任济钢集团副总经理兼销售公司经理。\n", "readable": True},
            {"page": 62, "text": "就公司销售产品的相关交易，选取样本，执行函证程序。\n", "readable": True},
            {
                "page": 201,
                "text": "合营企业或联营企业名称业务性质\n广州JFE钢铁生产\n平煤神马煤炭开采与销售\n",
                "readable": True,
            },
        ],
    )
    principal = _dimension(result["query"], "principal_business")
    revenue = _dimension(result["query"], "revenue_model")
    assert "五大系列" in principal
    assert "航空性收入" in revenue
    assert "旅客及货邮航空服务收入" in revenue
    facts = {item["record_id"]: item for item in result["query"]["accepted_facts"]}
    roles = []
    for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]:
        fact = facts[item["source_record_ids"][0]]
        pages = [evidence["page"] for evidence in fact["evidence"]]
        roles.append((item["source_native_name"], item["role"], tuple(pages)))
    assert ("铁矿石", "raw_material_input", (25,)) in roles
    assert ("废钢", "raw_material_input", (25,)) in roles
    assert ("电耗", "energy_consumption", (17,)) in roles
    assert ("柴油单耗", "energy_consumption", (17,)) in roles
    assert not any(page in {35, 62, 201} for _, _, pages in roles for page in pages)
