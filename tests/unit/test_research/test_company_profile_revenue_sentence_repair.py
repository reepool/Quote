"""Same-evidence revenue sentences stay behind the repair marker."""

from __future__ import annotations

import asyncio

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

_PAGE = """报告期内公司从事的业务情况
报告期内，公司主要从事写字楼、商城、公寓等投资性物业的出租和管理以及酒店
经营等。
公司除所拥有的酒店委托香格里拉国际饭店管理有限公司进行管理和经营外，其他主营业务均由公司自行管理和经营。
公司属于房地产业。公司的营业
收入主要来源于写字楼、商城、公寓等投资性物业的出租和管理以及酒店的经营；公司
的经营规模处于领先水平。
香格里拉营业收入主要来源于客房及餐饮。
国贸有限公司的营业收入主要来源于物业管理。
公司并未形成新的营业收入来源。
报告期内公司新增重要非主营业务的说明
不适用。
"""


def _identity(**extra: str) -> dict[str, str]:
    payload = default_processing_identity()
    payload.update(extra)
    return payload


async def _publish(tmp_path, identity: dict[str, str]) -> None:
    report = _report(instrument_id="600007.SH", report_id="1225071290")
    writer = CompanyProfileResearchWriter(tmp_path)
    runtime = CompanyProfileStageRuntime(writer=writer, provider=None)
    item = {
        "work_id": "work-revenue-sentence",
        "instrument_id": report.instrument_id,
        "report": report.model_dump(mode="json"),
        "pages": [{"page": 14, "text": _PAGE, "readable": True}],
        "processing_identity": identity,
    }
    for stage in WORK_STAGES:
        await runtime(stage, item)


def _revenue(profile: dict) -> dict:
    return next(
        item
        for item in profile["dimensions"]
        if item["dimension_id"] == "revenue_model"
    )


def test_repair_marker_delivers_the_company_revenue_sentence(tmp_path):
    root = tmp_path / "marked"
    asyncio.run(
        _publish(root, _identity(revenue_sentence_repair="v1"))
    )
    reads = CompanyProfileReadService(root)
    queried = reads.query(("600007.SH",))
    exported = reads.export(
        ("600007.SH",),
        export_directory=tmp_path / "export",
    )
    profile = queried["profiles"][0]
    revenue = _revenue(profile)

    excerpt = "".join(revenue["excerpt"].split())
    assert queried["state"] == "found"
    assert exported["state"] == "completed"
    assert revenue["answered"] is True
    assert revenue["missing_reason"] is None
    assert revenue["evidence_ids"]
    assert "营业收入主要来源于写字楼、商城、公寓" in excerpt
    assert "香格里拉营业收入" not in excerpt
    assert "国贸有限公司的营业收入" not in excerpt
    assert "并未形成" not in excerpt
    assert "不适用" not in excerpt
    assert profile["production_authorization"] == "not_authorized"
    assert exported["production_authorization"] == "not_authorized"


def test_unmarked_identity_keeps_the_revenue_gap(tmp_path):
    marked_root = tmp_path / "other-identity"
    plain_root = tmp_path / "plain"
    asyncio.run(
        _publish(marked_root, _identity(successor="v9"))
    )
    asyncio.run(_publish(plain_root, default_processing_identity()))

    for root in (marked_root, plain_root):
        profile = CompanyProfileReadService(root).query(("600007.SH",))["profiles"][0]
        revenue = _revenue(profile)
        assert revenue["answered"] is False
        assert revenue["missing_reason"] == "overview_lacks_dimension"
        assert "营业收入主要来源于" not in "".join((revenue["excerpt"] or "").split())
