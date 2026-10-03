"""v4 keeps company principal sentences and separates real commodity actions."""

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

_PAGE = (
    "报告期内公司从事的业务情况\n"
    "香格里拉业务覆盖酒店。\n"
    "公司业务覆盖水、固、气、能。\n"
    "分销售模式情况的说明。公司按内部组织机构划分为钢铁制造。\n"
    "主要产品单位生产量销售量库存量\n"
    "其他钢铁产品万吨128147\n"
    "公司与钢铁主业在主要销售区域重合。\n"
    "总经理曾任销售部总经理。\n"
    "公司销售钢铁产品。\n"
    "公司销售能源介质等市场价3,691。\n"
    "公司采购能源介质市场价3,805。\n"
    "废钢供应来源自供3886242国内采购7422863。\n"
    "公司有较大规模进口铁矿石采购需求。\n"
)


def _identity(**extra: str) -> dict[str, str]:
    payload = default_processing_identity()
    payload.update(extra)
    return payload


async def _drive(root, identity: dict[str, str]) -> None:
    report = _report(instrument_id="600019.SH", report_id="1225257227")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": "work-v4",
        "instrument_id": report.instrument_id,
        "report": report.model_dump(mode="json"),
        "pages": [{"page": 15, "text": _PAGE, "readable": True}],
        "processing_identity": identity,
    }
    for stage in WORK_STAGES:
        await runtime(stage, item)


def _profile(root, identity: dict[str, str]) -> dict:
    return CompanyProfileReadService(root).query(
        ("600019.SH",),
        processing_identity=identity,
    )["profiles"][0]


def _roles(profile: dict) -> list[tuple[str, str]]:
    exposures = (
        (profile.get("commodity_exposure") or {})
        .get("assessment", {})
        .get("exposures")
        or []
    )
    return [
        (item.get("source_native_name"), item.get("role"))
        for item in exposures
    ]


def test_v4_keeps_principal_sales_volume_and_both_energy_directions(tmp_path):
    identity = _identity(revenue_sentence_repair="v4")
    root = tmp_path / "marked"
    asyncio.run(_drive(root, identity))
    profile = _profile(root, identity)
    principal = next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "principal_business"
    )
    assert principal["answered"] is True
    assert "业务覆盖" in principal["excerpt"]
    assert "香格里拉" not in principal["excerpt"]
    roles = _roles(profile)
    assert ("其他钢铁产品", "product_sales") in roles
    assert ("钢铁", "product_sales") in roles
    assert ("能源介质", "product_sales") in roles
    assert ("能源介质", "raw_material_input") in roles
    assert ("废钢", "raw_material_input") in roles
    assert ("铁矿石", "raw_material_input") in roles
    exposures = profile["commodity_exposure"]["assessment"]["exposures"]
    energy = [
        item
        for item in exposures
        if item.get("source_native_name") == "能源介质"
    ]
    assert len(energy) == 2
    assert all(not item.get("measurement_record_ids") for item in energy)
    assert "3691" not in json.dumps(exposures, ensure_ascii=False)
    assert "3805" not in json.dumps(exposures, ensure_ascii=False)
    exported = CompanyProfileReadService(root).export(
        ("600019.SH",),
        export_directory=tmp_path / "export",
        processing_identity=identity,
    )
    assert exported["state"] == "completed"
    payload = json.loads(
        next((tmp_path / "export").rglob("600019.SH_*.json")).read_text(
            encoding="utf-8"
        )
    )
    assert ("其他钢铁产品", "product_sales") in _roles(payload)
    assert ("废钢", "raw_material_input") in _roles(payload)


def test_v4_keeps_a_focus_sentence_and_skips_internal_scrap(tmp_path):
    identity = _identity(revenue_sentence_repair="v4")
    root = tmp_path / "focus"
    report = _report(instrument_id="600008.SH", report_id="1225095393")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": "work-focus",
        "instrument_id": "600008.SH",
        "report": report.model_dump(mode="json"),
        "pages": [
            {
                "page": 10,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司专注于钢铁业。\n"
                    "废钢供应来源自供3886242。\n"
                ),
                "readable": True,
            }
        ],
        "processing_identity": identity,
    }

    async def drive():
        for stage in WORK_STAGES:
            await runtime(stage, item)

    asyncio.run(drive())
    profile = CompanyProfileReadService(root).query(
        ("600008.SH",),
        processing_identity=identity,
    )["profiles"][0]
    principal = next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "principal_business"
    )
    assert principal["answered"] is True
    assert "专注于" in principal["excerpt"]
    assert "钢铁业" in principal["excerpt"]
    assert ("废钢", "raw_material_input") not in _roles(profile)


def test_bare_focus_phrase_is_not_a_complete_principal(tmp_path):
    identity = _identity(revenue_sentence_repair="v5")
    root = tmp_path / "bare"
    report = _report(instrument_id="600019.SH", report_id="1225257227")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": "work-bare",
        "instrument_id": "600019.SH",
        "report": report.model_dump(mode="json"),
        "pages": [
            {
                "page": 9,
                "text": "报告期内公司从事的业务情况\n公司专注于\n",
                "readable": True,
            }
        ],
        "processing_identity": identity,
    }

    async def drive():
        for stage in WORK_STAGES:
            await runtime(stage, item)

    asyncio.run(drive())
    profile = CompanyProfileReadService(root).query(
        ("600019.SH",),
        processing_identity=identity,
    )["profiles"][0]
    principal = next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "principal_business"
    )
    assert principal["answered"] is False


def test_wrapped_focus_sentence_and_investee_table(tmp_path):
    identity = _identity(revenue_sentence_repair="v5")
    root = tmp_path / "wrap"
    report = _report(instrument_id="600019.SH", report_id="1225257227")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": "work-wrap",
        "instrument_id": "600019.SH",
        "report": report.model_dump(mode="json"),
        "pages": [
            {
                "page": 9,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司是中国最现代化的特大型钢铁联合企业。公司专注于\n"
                    "钢铁业，同时从事与钢铁主业相关的加工配送、化工及信息科技等业务。\n"
                ),
                "readable": True,
            },
            {
                "page": 15,
                "text": (
                    "主要产品单位生产量销售量库存量\n"
                    "其他钢铁产品万吨128147\n"
                ),
                "readable": True,
            },
            {
                "page": 69,
                "text": (
                    "公司与主要关联方发生的日常关联交易。\n"
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
        "processing_identity": identity,
    }

    async def drive():
        for stage in WORK_STAGES:
            await runtime(stage, item)

    asyncio.run(drive())
    profile = CompanyProfileReadService(root).query(
        ("600019.SH",),
        processing_identity=identity,
    )["profiles"][0]
    principal = next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "principal_business"
    )
    excerpt = principal["excerpt"] or ""
    assert principal["answered"] is True
    assert "钢铁业" in excerpt
    assert "同时从事" in excerpt
    assert excerpt.strip() != "公司专注于"
    roles = _roles(profile)
    assert ("其他钢铁产品", "product_sales") in roles
    assert ("钢铁", "product_sales") in roles
    assert ("能源介质", "product_sales") in roles
    assert ("能源介质", "raw_material_input") in roles
    pages_for_steel = []
    facts = {item["record_id"]: item for item in profile["accepted_facts"]}
    for item in profile["commodity_exposure"]["assessment"]["exposures"]:
        if item.get("source_native_name") != "钢铁":
            continue
        for record_id in item.get("source_record_ids") or []:
            for evidence in facts[record_id]["evidence"]:
                pages_for_steel.append(evidence["page"])
    assert 201 not in pages_for_steel
    exported = CompanyProfileReadService(root).export(
        ("600019.SH",),
        export_directory=tmp_path / "wrap-export",
        processing_identity=identity,
    )
    assert exported["state"] == "completed"


_PAGE_17 = (
    "处理规模达 53.8 万吨/日。运营端持续深化精益管理，污水吨水药耗、电耗分别同比有所下降，核\n"
    "心运营指标稳步优化，水务业务盈利韧性持续增强。\n"
    "剔除新加坡 ECO 公司处置影响因素后，固废处理业务核心主业营收实现稳步增长。\n"
    "累计上网电量 24.13 亿千瓦时，同比增长 6.88%，项目运营能力稳步提升。\n"
    "宁波厨余项目完成价格调整，总体产能利用率提升至 86%。\n"
    "同时公司通过技改优化、设备大修标准化、能耗管控等举措，焚烧业务柴油单耗、生物质度电燃料成本等关键指标显著下降，\n"
    "运营效率与盈利水平同步提升。\n"
    "公司面临电力价格风险。\n"
    "子公司焚烧业务柴油单耗下降。\n"
)


def _service_profile(root, identity: dict[str, str], work_id: str) -> dict:
    report = _report(instrument_id="600008.SH", report_id="1225095393")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": work_id,
        "instrument_id": "600008.SH",
        "report": report.model_dump(mode="json"),
        "pages": [
            {
                "page": 10,
                "text": (
                    "报告期内公司从事的业务情况\n"
                    "公司业务覆盖水、固、气、能环保全产业链。\n"
                ),
                "readable": True,
            },
            {"page": 17, "text": _PAGE_17, "readable": True},
        ],
        "processing_identity": identity,
    }

    async def drive():
        for stage in WORK_STAGES:
            await runtime(stage, item)

    asyncio.run(drive())
    return CompanyProfileReadService(root).query(
        ("600008.SH",),
        processing_identity=identity,
    )["profiles"][0]


def test_service_operating_energy_keeps_subject_and_business_column(tmp_path):
    identity = _identity(revenue_sentence_repair="v6")
    root = tmp_path / "energy"
    profile = _service_profile(root, identity, "work-energy")
    facts = {item["record_id"]: item for item in profile["accepted_facts"]}
    exposures = profile["commodity_exposure"]["assessment"]["exposures"]
    energy = {
        item["source_native_name"]: item
        for item in exposures
        if item["role"] == "energy_consumption"
    }
    assert set(energy) == {"电耗", "柴油单耗"}
    for name, actor, header, quote_part in (
        ("电耗", "运营端", "污水", "污水吨水药耗、电耗"),
        ("柴油单耗", "公司", "焚烧业务", "焚烧业务柴油单耗"),
    ):
        exposure = energy[name]
        assert exposure["measurement_record_ids"] == []
        record = facts[exposure["source_record_ids"][0]]
        assert record["source_actor"] == actor
        assert record["source_native_header"] == header
        assert record["source_native_value"] is None
        evidence = record["evidence"]
        assert evidence[0]["page"] == 17
        assert quote_part in evidence[0]["bounded_quote"]
        assert exposure["evidence_ids"]
    names = [item["source_native_name"] for item in exposures]
    assert "product_sales" not in {item["role"] for item in exposures}
    assert "电力" not in names
    assert names.count("柴油单耗") == 1
    exported = CompanyProfileReadService(root).export(
        ("600008.SH",),
        export_directory=tmp_path / "energy-export",
        processing_identity=identity,
    )
    assert exported["state"] == "completed"
    payload = json.loads(
        next((tmp_path / "energy-export").glob("600008.SH_*.json")).read_text(
            encoding="utf-8"
        )
    )
    exported_energy = [
        item["source_native_name"]
        for item in payload["commodity_exposure"]["assessment"]["exposures"]
        if item["role"] == "energy_consumption"
    ]
    assert exported_energy == ["电耗", "柴油单耗"] or set(exported_energy) == {
        "电耗",
        "柴油单耗",
    }


def test_earlier_repair_does_not_take_service_operating_energy(tmp_path):
    identity = _identity(revenue_sentence_repair="v5")
    profile = _service_profile(tmp_path / "v5-energy", identity, "work-v5-energy")
    assert profile["commodity_exposure"]["assessment"]["exposures"] == []


def test_unmarked_identity_does_not_adopt_the_new_principal_sentence(tmp_path):
    identity = _identity(successor="v9")
    root = tmp_path / "plain"
    asyncio.run(_drive(root, identity))
    profile = _profile(root, identity)
    principal = next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "principal_business"
    )
    assert principal["answered"] is False
    assert ("其他钢铁产品", "product_sales") not in _roles(profile)
    assert ("废钢", "raw_material_input") not in _roles(profile)
