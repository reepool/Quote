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
    assert ("废钢", "raw_material_input") not in _roles(profile)


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
