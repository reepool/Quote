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


_SANDWICH = """报告期内公司从事的业务情况
公司主要从事写字楼、商城和公寓出租。
香格里拉营业收入主要来源于客房及餐饮。
公司并未形成新的营业收入来源。
公司的营业
收入主要来源于租金。
国贸有限公司的营业收入主要来源于物业管理。
公司的营业收入主要来源于物业出租。
"""

_COMMA_SENTENCE = (
    "报告期内公司从事的业务情况\n"
    "公司主要从事写字楼出租，公司的营业收入主要来源于租金。\n"
)


async def _drive(runtime, item) -> None:
    for stage in WORK_STAGES:
        await runtime(stage, item)


def test_intervening_third_party_and_denial_stay_out_of_the_answer(tmp_path):
    identities = {
        "marked": _identity(revenue_sentence_repair="v1"),
        "plain": default_processing_identity(),
        "other": _identity(successor="v9"),
    }
    for name, identity in identities.items():
        root = tmp_path / name
        asyncio.run(_drive(
            CompanyProfileStageRuntime(
                writer=CompanyProfileResearchWriter(root),
                provider=None,
            ),
            {
                "work_id": f"work-{name}",
                "instrument_id": "600007.SH",
                "report": _report(
                    instrument_id="600007.SH",
                    report_id="1225071290",
                ).model_dump(mode="json"),
                "pages": [{"page": 14, "text": _SANDWICH, "readable": True}],
                "processing_identity": identity,
            },
        ))
        revenue = _revenue(
            CompanyProfileReadService(root).query(("600007.SH",))["profiles"][0]
        )
        excerpt = "".join((revenue["excerpt"] or "").split())
        if name == "marked":
            assert revenue["answered"] is True
            assert "营业收入主要来源于租金" in excerpt
            assert "营业收入主要来源于物业出租" in excerpt
            assert "香格里拉营业收入" not in excerpt
            assert "国贸有限公司的营业收入" not in excerpt
            assert "并未形成" not in excerpt
            assert len(revenue["evidence_ids"]) >= 1
            assert len(revenue["supporting_record_ids"]) >= 2
        else:
            assert revenue["answered"] is False
            assert revenue["missing_reason"] == "overview_lacks_dimension"


def test_same_sentence_revenue_clause_stays_on_the_parent_rule(tmp_path):
    text = _COMMA_SENTENCE
    for name, identity, answered in (
        ("marked", _identity(revenue_sentence_repair="v1"), True),
        ("plain", default_processing_identity(), False),
        ("other", _identity(owned_page_facts="v8", successor="v9"), False),
    ):
        root = tmp_path / name
        asyncio.run(_drive(
            CompanyProfileStageRuntime(
                writer=CompanyProfileResearchWriter(root),
                provider=None,
            ),
            {
                "work_id": f"work-comma-{name}",
                "instrument_id": "600007.SH",
                "report": _report(
                    instrument_id="600007.SH",
                    report_id="1225071290",
                ).model_dump(mode="json"),
                "pages": [{"page": 14, "text": text, "readable": True}],
                "processing_identity": identity,
            },
        ))
        revenue = _revenue(
            CompanyProfileReadService(root).query(("600007.SH",))["profiles"][0]
        )
        excerpt = "".join((revenue["excerpt"] or "").split())
        assert revenue["answered"] is answered
        if answered:
            assert "营业收入主要来源于租金" in excerpt
        else:
            assert revenue["missing_reason"] == "overview_lacks_dimension"
            assert "营业收入主要来源于" not in excerpt


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
