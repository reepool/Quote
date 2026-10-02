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
            "公司主要从事矿产资源开发利用、钢铁产品的生产与销售等。"
            "本公司主要产品为稀土精矿、萤石精矿的生产和销售。"
            "公司销售冶金焦炭等焦化产品。"
        ),
        "readable": True,
    },
    {
        "page": 9,
        "text": (
            "包钢股份煤焦化工分公司有焦炉，焦炭产能580万吨，主要产品有冶金焦炭。"
            "采购模式：公司设有采购中心。"
            "伴生萤石资源量国内最大。"
        ),
        "readable": True,
    },
    {
        "page": 13,
        "text": (
            "公司拥有的白云鄂博西矿，铁矿石累计查明储量7.5亿吨。"
            "公司主要从事钢铁产品的生产与销售。"
        ),
        "readable": True,
    },
    {
        "page": 140,
        "text": (
            "国贸有限公司 接受劳务 支付蒸汽费、热水费及电费等 7,407,073 7,382,121"
        ),
        "readable": True,
    },
    {
        "page": 220,
        "text": (
            "集团公司保证将依照本公司发出的订单向本公司供应主要原、辅料包括"
            "铁矿石、白灰、石灰石、进口矿等，但本公司所订购的主要原、辅料品种"
            "必须为集团公司现生产的品种。"
            "白灰、石灰石：交易价格按照区内采购的市场价格执行。"
        ),
        "readable": True,
    },
    {
        "page": 223,
        "text": (
            "焦炭采购协议。本公司生产过程中需要焦炭作为原料。"
            "本公司可按需要发出订单以订购焦炭。"
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


def _associations(profile: dict) -> list[tuple[str, str, tuple[int, ...], str]]:
    facts = {item["record_id"]: item for item in profile["accepted_facts"]}
    rows = []
    exposures = (
        (profile.get("commodity_exposure") or {})
        .get("assessment", {})
        .get("exposures")
        or []
    )
    for item in exposures:
        pages: list[int] = []
        quotes: list[str] = []
        for record_id in item.get("source_record_ids") or []:
            for evidence in facts[record_id]["evidence"]:
                pages.append(int(evidence["page"]))
                quotes.append(evidence.get("bounded_quote") or "")
        rows.append(
            (
                item.get("source_native_name"),
                item.get("role"),
                tuple(pages),
                "".join(quotes),
            )
        )
    return rows


def _assert_sentence_bindings(profile: dict) -> None:
    rows = _associations(profile)
    assert ("钢铁", "product_sales") in _roles(profile)
    assert ("稀土精矿", "product_sales") in _roles(profile)
    assert ("萤石", "product_sales") in _roles(profile)
    assert ("焦化产品", "product_sales") in _roles(profile)
    assert any(
        name == "铁矿石" and role == "raw_material_input" and 220 in pages
        for name, role, pages, _quote in rows
    )
    assert any(
        name == "焦炭" and role == "raw_material_input" and 223 in pages
        for name, role, pages, _quote in rows
    )
    assert not any(
        name == "焦炭" and role == "raw_material_input" and 9 in pages
        for name, role, pages, _quote in rows
    )
    assert not any(
        name == "铁矿石" and role == "raw_material_input" and 13 in pages
        for name, role, pages, _quote in rows
    )
    assert not any("萤石资源" in quote and role == "product_sales" for _name, role, _pages, quote in rows)
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


def test_repair_marker_keeps_sales_procurement_and_one_energy_fee(tmp_path):
    root = tmp_path / "marked"
    asyncio.run(_drive(root, _identity(revenue_sentence_repair="v2")))
    profile = CompanyProfileReadService(root).query(
        ("600010.SH",),
        processing_identity=_identity(revenue_sentence_repair="v2"),
    )["profiles"][0]
    _assert_sentence_bindings(profile)
    exported = CompanyProfileReadService(root).export(
        ("600010.SH",),
        export_directory=tmp_path / "export",
        processing_identity=_identity(revenue_sentence_repair="v2"),
    )
    assert exported["state"] == "completed"
    payload = json.loads(
        next((tmp_path / "export").rglob("*.json")).read_text(encoding="utf-8")
    )
    _assert_sentence_bindings(payload)


def test_unmarked_identity_does_not_add_repair_commodity_roles(tmp_path):
    root = tmp_path / "plain"
    asyncio.run(_drive(root, _identity(successor="v9")))
    profile = CompanyProfileReadService(root).query(("600010.SH",))["profiles"][0]
    names = {name for name, _role in _roles(profile)}
    assert "铁矿石" not in names
    assert "蒸汽" not in names
    assert "7407073" not in json.dumps(profile, ensure_ascii=False)
