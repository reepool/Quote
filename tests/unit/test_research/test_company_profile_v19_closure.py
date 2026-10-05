"""A-role closure: frozen full pages, formal identity, and delivered text."""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

import pytest

from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from tests.unit.test_research.test_company_profile_runtime import WORK_STAGES, _report

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"
FEES = ("包干费", "库场使用费", "港口其他收费")


def _drive(root, *, instrument, fixture, pages, repair_version="v19"):
    identity = {**default_processing_identity(), "revenue_sentence_repair": repair_version}
    report = _report(instrument_id=instrument).model_copy(
        update={"document_version": fixture["document_version"]}
    )
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(root), provider=None
    )
    item = {
        "work_id": f"work-{instrument}",
        "instrument_id": instrument,
        "report": report.model_dump(mode="json"),
        "pages": pages,
        "processing_identity": identity,
    }

    async def run():
        for stage in WORK_STAGES:
            await runtime(stage, item)

    asyncio.run(run())
    reads = CompanyProfileReadService(root)
    query = reads.query((instrument,), processing_identity=identity)["profiles"][0]
    reads.export(
        (instrument,), export_directory=root / "export", processing_identity=identity
    )
    exported = json.loads(
        next((root / "export").glob(f"{instrument}_*.json")).read_text()
    )
    assert query["production_authorization"] == "not_authorized"
    return query, exported


def _answer(profile, dimension):
    return re.sub(
        r"\s+",
        "",
        next(
            d["excerpt"] or ""
            for d in profile["dimensions"]
            if d["dimension_id"] == dimension
        ),
    )


@pytest.mark.parametrize("subject", ["公司", "公司的", "本公司", "本公司的"])
def test_frozen_complete_sipg_page_delivers_service_and_fees(tmp_path, subject):
    fixture = json.loads(
        (FIXTURES / "company_profile_sipg_frozen_p12.json").read_text()
    )
    text = fixture["text"].replace("公司经营模式", f"{subject}经营模式")
    profiles = _drive(
        tmp_path / "sipg",
        instrument="600018.SH",
        fixture=fixture,
        pages=[{"page": 12, "text": text, "readable": True}, *fixture["segment_pages"]],
    )
    for profile in profiles:
        overview = "".join(
            re.sub(r"\s+", "", f.get("source_text") or "")
            for f in profile["accepted_facts"]
            if f["field_id"] == "business_overview_source"
        )
        revenue = _answer(profile, "revenue_model")
        for delivered in (overview, revenue):
            assert "为客户提供港口及相关服务" in delivered
            assert all(fee in delivered for fee in FEES)
        assert any(
            f["field_id"] == "operating_revenue" for f in profile["accepted_facts"]
        )


@pytest.mark.parametrize(
    "sentence",
    [
        "公司经营模式主要为：为客户提供港口及相关服务，计划收取港口作业包干费、库场使用费和港口其他收费。",
        "公司经营模式主要为：为客户提供港口及相关服务，不收取港口作业包干费、库场使用费和港口其他收费。",
        "第三方公司经营模式主要为：为客户提供港口及相关服务，收取港口作业包干费、库场使用费和港口其他收费。",
        "公司经营模式主要为：为客户提供港口及相关服务，第三方收取港口作业包干费、库场使用费和港口其他收费。",
    ],
)
def test_port_fee_negatives_do_not_answer_revenue(tmp_path, sentence):
    fixture = json.loads(
        (FIXTURES / "company_profile_sipg_frozen_p12.json").read_text()
    )
    profiles = _drive(
        tmp_path / "negative",
        instrument="600018.SH",
        fixture=fixture,
        pages=[
            {
                "page": 12,
                "text": "报告期内公司从事的业务情况\n" + sentence,
                "readable": True,
            }
        ],
    )
    for profile in profiles:
        assert not _answer(profile, "revenue_model")


def test_port_fee_free_add_on_preserves_paid_service(tmp_path):
    fixture = json.loads(
        (FIXTURES / "company_profile_sipg_frozen_p12.json").read_text()
    )
    sentence = (
        "公司经营模式主要为：为客户提供港口及相关服务，收取港口作业包干费、"
        "库场使用费和港口其他收费，同时免费提供附带咨询服务。"
    )
    profiles = _drive(
        tmp_path / "free",
        instrument="600018.SH",
        fixture=fixture,
        pages=[
            {
                "page": 12,
                "text": "报告期内公司从事的业务情况\n" + sentence,
                "readable": True,
            }
        ],
    )
    for profile in profiles:
        assert all(fee in _answer(profile, "revenue_model") for fee in FEES)


def test_frozen_wandong_positioning_and_development_remain_delivered(tmp_path):
    fixture = json.loads(
        (FIXTURES / "company_profile_wandong_frozen_p11.json").read_text()
    )
    profiles = _drive(
        tmp_path / "wandong",
        instrument="600055.SH",
        fixture=fixture,
        pages=[{"page": 11, "text": fixture["text"], "readable": True}],
    )
    for profile in profiles:
        for dimension in ("principal_business", "products_services"):
            text = _answer(profile, dimension)
            assert text.startswith("2025年，作为国产医学影像装备")
            assert "智慧医疗解决方案的核心提供商，公司在" in text
            assert "谋篇布局“十五五”发展的关键之年。" in text


@pytest.mark.parametrize(
    "sentence",
    [
        "作为国产医学影像装备与智慧医疗解决方案的核心提供商，子公司在医疗设备领域持续发展。",
        "作为国产医学影像装备与智慧医疗解决方案的核心提供商，第三方公司在医疗设备领域持续发展。",
        "公司拟作为国产医学影像装备与智慧医疗解决方案的核心提供商，公司计划进入医疗设备领域。",
        "子公司作为国产医学影像装备与智慧医疗解决方案的核心提供商，公司在医疗设备领域持续发展。",
    ],
)
def test_provider_positioning_negative_does_not_answer_company_business(
    tmp_path, sentence
):
    fixture = json.loads(
        (FIXTURES / "company_profile_wandong_frozen_p11.json").read_text()
    )
    profiles = _drive(
        tmp_path / "provider-negative",
        instrument="600055.SH",
        fixture=fixture,
        pages=[
            {
                "page": 11,
                "text": "报告期内公司从事的业务情况\n" + sentence,
                "readable": True,
            }
        ],
    )
    for profile in profiles:
        assert not _answer(profile, "principal_business")
        assert not _answer(profile, "products_services")
        assert not any(
            f["field_id"] == "business_overview_source"
            for f in profile["accepted_facts"]
        )


@pytest.mark.parametrize("subject", ["子公司", "第三方公司", "planned"])
def test_real_wandong_page_with_rejected_subject_or_plan(tmp_path, subject):
    fixture = json.loads(
        (FIXTURES / "company_profile_wandong_frozen_p11.json").read_text()
    )
    text = fixture["text"]
    if subject == "planned":
        text = text.replace("作为国产", "公司拟作为国产", 1).replace(
            "核心提供商，公司在", "核心提供商，公司计划进入", 1
        )
    else:
        text = text.replace("核心提供商，公司在", f"核心提供商，{subject}在", 1)
    profiles = _drive(
        tmp_path / "real-page-negative",
        instrument="600055.SH",
        fixture=fixture,
        pages=[{"page": 11, "text": text, "readable": True}],
    )
    for profile in profiles:
        assert not _answer(profile, "principal_business")
        assert not _answer(profile, "products_services")
        assert not any(
            f["field_id"] == "business_overview_source"
            for f in profile["accepted_facts"]
        )
