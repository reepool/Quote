"""v7 keeps substantive core answers and row-level commodity subjects."""

from __future__ import annotations

import asyncio
import json

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

_STEEL_PAGE = (
    "报告期内公司从事的业务情况\n"
    "公司是国内钢铁行业中具有重要地位和影响力的省级大型骨干企业，拥有钢城基地和日照基\n"
    "地两大钢铁生产基地，主要产品有中厚板、冷热轧、型钢、特钢、螺纹钢五大系列。\n"
    "冷轧板卷主要产品涵盖厚度 0.2mm~25.4mm、宽度 830mm~1900mm。\n"
)
_SUPPLY_PAGE = (
    "4、铁矿石供应情况\n"
    "铁矿石供应来源\n"
    "国内采购 421.00 476.18\n"
    "国外进口 1,928.81 2,087.87\n"
    "合计 2,349.81 2,564.05\n"
    "5、废钢供应情况\n"
    "废钢供应来源\n"
    "国内采购 264.00 251.70\n"
    "合计 264.00 251.70\n"
)
_SALES_PAGES = (
    (
        61,
        "山东钢铁公司的销售收入主要源于钢铁产品销售，属于在某一时点履行的履约义务。\n",
    ),
    (
        62,
        "就山东钢铁公司销售产品的相关交易，选取样本，对销售交易金额执行函证程序。\n",
    ),
    (
        35,
        "历任济钢集团副总经理兼销售公司经理、山东钢铁副总经理。\n",
    ),
    (
        168,
        "母公司名称 注册地 业务性质\n山东钢铁集团有限公司济南市钢铁冶炼、加工制造、销售\n",
    ),
    (
        182,
        "本公司集中于钢铁产品及其副产品的生产及销售业务，资产均在中国。\n",
    ),
)
_REVENUE_PAGE = (
    "收入和成本分析\n"
    "单位：元 币种：人民币\n"
    "航空性收入 5,989,145,946.12 44.87 5,560,487,801.05 44.96 7.71\n"
    "1、架次相关收入 2,371,942,585.20 17.77 2,289,141,806.69 18.51 3.62\n"
    "2、旅客及货邮航空\n"
    "服务收入\n"
    "3,617,203,360.92 27.10 3,271,345,994.36 26.45 10.57\n"
    "非航空性收入 7,357,046,218.00 55.13 6,808,316,302.41 55.04 8.06\n"
    "1、商业餐饮收入 2,205,613,932.38 16.53 2,055,110,226.15 16.62 7.32\n"
    "2、物流服务收入 1,829,588,881.81 13.71 1,682,672,213.63 13.60 8.73\n"
    "3、其他非航收入 3,321,843,403.81 24.89 3,070,533,862.63 24.82 8.18\n"
    "收入结构说明完毕。\n"
)
_ASSOCIATE_PAGE = (
    "主要子公司及对公司净利润影响达 10%以上的参股公司情况\n"
    "上海浦东国际机场航空油料有限责任公司参股公司汽油、煤油、柴油批发。\n"
)


def _identity() -> dict[str, str]:
    payload = default_processing_identity()
    payload["revenue_sentence_repair"] = "v7"
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


def _dimension(profile: dict, dimension_id: str) -> dict:
    return next(item for item in profile["dimensions"] if item["dimension_id"] == dimension_id)


def test_steel_page_keeps_the_business_sentence_and_five_series(tmp_path):
    result = _drive(
        tmp_path / "steel-text",
        "600022.SH",
        "1225149024",
        [{"page": 9, "text": _STEEL_PAGE, "readable": True}],
    )
    for profile in (result["query"], result["export"]):
        principal = _dimension(profile, "principal_business")["excerpt"]
        products = _dimension(profile, "products_services")["excerpt"]
        assert "钢铁" in principal
        assert "五大系列" in principal
        assert "中厚板" in principal
        assert "五大系列" in products
        assert products.strip() != "厚度 0.2mm~25.4mm"
    overview = next(
        item
        for item in result["query"]["accepted_facts"]
        if item["field_id"] == "business_overview_source"
    )
    assert "五大系列" in (overview["source_text"] or "")
    assert "厚度" not in (overview["source_text"] or "")


def test_airport_revenue_keeps_parent_lines_and_wrapped_children(tmp_path):
    result = _drive(
        tmp_path / "airport",
        "600009.SH",
        "1225264362",
        [
            {
                "page": 9,
                "text": "报告期内公司从事的业务情况\n公司的经营范围是民用机场运营。\n",
                "readable": True,
            },
            {"page": 14, "text": _REVENUE_PAGE, "readable": True},
            {"page": 21, "text": _ASSOCIATE_PAGE, "readable": True},
        ],
    )
    names = {
        item["source_native_name"]
        for item in result["query"]["accepted_facts"]
        if item["field_id"] == "operating_revenue"
    }
    assert {
        "航空性收入",
        "非航空性收入",
        "架次相关收入",
        "旅客及货邮航空服务收入",
        "商业餐饮收入",
        "物流服务收入",
        "其他非航收入",
    } <= names
    headers = {
        item["source_native_name"]: item["source_native_header"]
        for item in result["query"]["accepted_facts"]
        if item["source_native_name"] in {"架次相关收入", "旅客及货邮航空服务收入", "商业餐饮收入"}
    }
    assert headers["架次相关收入"] == "航空性收入"
    assert headers["旅客及货邮航空服务收入"] == "航空性收入"
    assert headers["商业餐饮收入"] == "非航空性收入"
    for profile in (result["query"], result["export"]):
        excerpt = _dimension(profile, "revenue_model")["excerpt"]
        assert "航空性收入" in excerpt
        assert "非航空性收入" in excerpt
        assert "旅客及货邮航空服务收入" in excerpt
        assert excerpt.strip() != "架次相关收入"
    roles = {
        item["source_native_name"]
        for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]
    }
    assert "柴油" not in roles


def test_supply_rows_and_sales_clauses_keep_their_subjects(tmp_path):
    pages = [
        {"page": 9, "text": _STEEL_PAGE, "readable": True},
        {"page": 25, "text": _SUPPLY_PAGE, "readable": True},
    ]
    pages.extend(
        {"page": page, "text": text, "readable": True} for page, text in _SALES_PAGES
    )
    result = _drive(tmp_path / "roles", "600022.SH", "1225149024", pages)
    facts = {item["record_id"]: item for item in result["query"]["accepted_facts"]}
    bindings = []
    sales_pages = []
    for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]:
        fact = facts[item["source_record_ids"][0]]
        if item["role"] == "raw_material_input":
            bindings.append((item["source_native_name"], fact["source_native_header"]))
        if item["source_native_name"] == "钢铁" and item["role"] == "product_sales":
            sales_pages.extend(evidence["page"] for evidence in fact["evidence"])
    assert ("铁矿石", "国内采购") in bindings
    assert ("铁矿石", "国外进口") in bindings
    assert ("废钢", "国内采购") in bindings
    assert 61 in sales_pages
    assert 182 in sales_pages
    assert 62 not in sales_pages
    assert 35 not in sales_pages
    assert 168 not in sales_pages
    exported_pages = []
    exported_facts = {
        item["record_id"]: item for item in result["export"]["accepted_facts"]
    }
    for item in result["export"]["commodity_exposure"]["assessment"]["exposures"]:
        if item["source_native_name"] != "钢铁":
            continue
        fact = exported_facts[item["source_record_ids"][0]]
        exported_pages.extend(evidence["page"] for evidence in fact["evidence"])
    assert exported_pages == sales_pages


def test_v7_keeps_the_previously_accepted_named_roles(tmp_path):
    result = _drive(
        tmp_path / "prior",
        "600019.SH",
        "1225257227",
        [
            {
                "page": 9,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司专注于\n"
                    "钢铁业，同时从事与钢铁主业相关的加工配送、化工及信息科技等业务。\n"
                ),
                "readable": True,
            },
            {
                "page": 15,
                "text": "主要产品单位生产量销售量库存量\n其他钢铁产品万吨128147\n",
                "readable": True,
            },
            {
                "page": 69,
                "text": (
                    "公司销售钢铁产品等市场价。\n"
                    "公司销售能源介质等市场价。\n"
                    "公司采购能源介质市场价。\n"
                ),
                "readable": True,
            },
            {
                "page": 201,
                "text": (
                    "合营企业或联营企业名称主要经营地注册地业务性质\n"
                    "（“广州JFE”）中国广州市钢铁生产\n"
                    "（“平煤神马”）中国平顶山市煤炭开采与销售\n"
                ),
                "readable": True,
            },
        ],
    )
    roles = {
        (item["source_native_name"], item["role"])
        for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]
    }
    assert ("其他钢铁产品", "product_sales") in roles
    assert ("钢铁", "product_sales") in roles
    assert ("能源介质", "product_sales") in roles
    assert ("能源介质", "raw_material_input") in roles
    principal = _dimension(result["query"], "principal_business")["excerpt"]
    assert "钢铁业" in principal
    assert "同时从事" in principal
    pages = []
    facts = {item["record_id"]: item for item in result["query"]["accepted_facts"]}
    for item in result["query"]["commodity_exposure"]["assessment"]["exposures"]:
        if item["source_native_name"] != "钢铁":
            continue
        for record_id in item["source_record_ids"]:
            pages.extend(evidence["page"] for evidence in facts[record_id]["evidence"])
    assert 201 not in pages
