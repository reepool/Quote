"""Repair-marked commodity roles keep sales, procurement, and one energy fee."""

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

_PAGES = (
    {
        "page": 8,
        "text": (
            "报告期内公司从事的业务情况\n"
            "公司主要从事矿产资源开发利用、钢铁产品的生产与销售等。"
            "矿产品主要有稀土精矿、萤石精矿。"
            "主要产品有冶金焦炭。"
        ),
        "readable": True,
    },
    {
        "page": 140,
        "text": (
            "本公司与关联方的交易均符合正常的商业条款。"
            "支付蒸汽费、热水费及电费等 7,407,073"
        ),
        "readable": True,
    },
    {
        "page": 220,
        "text": (
            "本公司向集团公司采购主要原辅料，包括铁矿石、白灰、石灰石、进口矿。"
            "双方签署焦炭采购协议。"
        ),
        "readable": True,
    },
)


def _identity(**extra: str) -> dict[str, str]:
    payload = default_processing_identity()
    payload.update(extra)
    return payload


async def _drive(root, identity: dict[str, str]) -> None:
    report = _report(instrument_id="600010.SH", report_id="1225121984")
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root),
        provider=None,
    )
    item = {
        "work_id": "work-commodity",
        "instrument_id": report.instrument_id,
        "report": report.model_dump(mode="json"),
        "pages": list(_PAGES),
        "processing_identity": identity,
    }
    for stage in WORK_STAGES:
        await runtime(stage, item)


def _roles(profile: dict) -> list[tuple[str, str]]:
    exposures = (
        (profile.get("commodity_exposure") or {})
        .get("assessment", {})
        .get("exposures")
        or []
    )
    return [(item.get("source_native_name"), item.get("role")) for item in exposures]


def test_repair_marker_keeps_sales_procurement_and_one_energy_fee(tmp_path):
    root = tmp_path / "marked"
    asyncio.run(_drive(root, _identity(revenue_sentence_repair="v1")))
    profile = CompanyProfileReadService(root).query(("600010.SH",))["profiles"][0]
    roles = _roles(profile)
    assert ("钢铁", "product_sales") in roles
    assert ("稀土精矿", "product_sales") in roles
    assert ("萤石", "product_sales") in roles
    assert ("焦化产品", "product_sales") in roles
    for name in ("铁矿石", "白灰", "石灰石", "进口矿", "焦炭"):
        assert (name, "raw_material_input") in roles
    for name in ("蒸汽", "热水", "电"):
        assert (name, "energy_consumption") in roles
    exposures = profile["commodity_exposure"]["assessment"]["exposures"]
    assert "pending" in [item.get("mapping_status") for item in exposures]
    energy = [
        item
        for item in exposures
        if item.get("source_native_name") in {"蒸汽", "热水", "电"}
    ]
    assert energy
    assert all(not item.get("measurement_record_ids") for item in energy)
    quotes = json.dumps(profile["accepted_facts"], ensure_ascii=False)
    assert "7,407,073" in quotes
    assert "7407073" not in json.dumps(exposures, ensure_ascii=False)
    exported = CompanyProfileReadService(root).export(
        ("600010.SH",),
        export_directory=tmp_path / "export",
    )
    assert exported["state"] == "completed"


def test_unmarked_identity_does_not_add_repair_commodity_roles(tmp_path):
    root = tmp_path / "plain"
    asyncio.run(_drive(root, _identity(successor="v9")))
    profile = CompanyProfileReadService(root).query(("600010.SH",))["profiles"][0]
    names = {name for name, _role in _roles(profile)}
    assert "铁矿石" not in names
    assert "蒸汽" not in names
    assert "7407073" not in json.dumps(profile, ensure_ascii=False)
