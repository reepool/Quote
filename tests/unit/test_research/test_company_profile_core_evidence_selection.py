from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from research.company_profile import ChapterTask
from research.company_profile.core_evidence_selection import (
    CORE_EVIDENCE_SCHEMA_VERSION,
    core_evidence_schema_manifest,
    select_core_evidence,
)
from research.company_profile.models import ReportIdentity, SemanticRecord

ROOT = Path(__file__).resolve().parents[3]
REFERENCE_INPUT = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_input.json"
)
RECORD_ADAPTER = TypeAdapter(SemanticRecord)


def _report() -> ReportIdentity:
    return ReportIdentity(
        instrument_id="600000.SH",
        report_id="asset-core-evidence",
        document_version="ver-core-evidence",
        report_period="2025-12-31",
        published_at="2026-03-31T00:00:00+00:00",
    )


def _record(payload):
    return RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))


def _reference_report() -> ReportIdentity:
    payload = json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))
    return ReportIdentity.model_validate(payload["report"])


def _reference_records(*record_ids: str):
    payload = json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))
    wanted = set(record_ids)
    return [
        _record(deepcopy(item))
        for item in payload["records"]
        if item["record_id"] in wanted
    ]


def test_schema_keeps_six_chapter_tasks_and_existing_field_ids():
    assert [item.value for item in ChapterTask] == [
        "extract_business_overview",
        "extract_segment_financials",
        "extract_operating_quantities",
        "extract_material_inputs",
        "extract_counterparties_and_concentration",
        "extract_business_regime",
    ]
    manifest = core_evidence_schema_manifest()
    assert manifest["schema_version"] == CORE_EVIDENCE_SCHEMA_VERSION
    assert manifest["chapter_tasks"] == (
        "extract_business_overview",
        "extract_segment_financials",
    )
    assert "extract_revenue_model" not in json.dumps(manifest)


def test_selects_owned_overview_and_keeps_necessary_context():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 2,
                "text": "目录\n第三节 管理层讨论与分析 主要业务 14",
            },
            {
                "page": 14,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    "公司主要从事动力电池、储能电池的研发、生产和销售，"
                    "并通过向客户销售电池系统取得货款。\n"
                    "二、风险因素\n公司主要业务面临原材料价格波动风险。"
                ),
            },
        ),
    )
    assert selected.production_authorization == "not_authorized"
    assert selected.spans[0].page == 14
    assert selected.spans[0].section_title == "报告期内公司从事的主要业务"
    assert selected.spans[0].context_complete is True
    assert "取得货款" in selected.spans[0].excerpt
    assert "主要从事" in selected.spans[0].excerpt
    assert "风险因素" not in selected.spans[0].excerpt
    assert "principal_business" in selected.spans[0].dimension_ids
    assert "revenue_model" in selected.spans[0].dimension_ids
    assert "business_overview_source" in selected.unresolved_field_ids
    assert {item.field_id for item in selected.prepared_evidence} == {
        "business_overview_source",
        "explicit_activity",
    }
    assert all(
        "取得货款" in item.evidence.anchor.bounded_quote
        for item in selected.prepared_evidence
    )
    assert selected.prepared_evidence[0].evidence.page == 14
    assert selected.gaps == ()


def test_keyword_only_fee_page_is_not_an_owned_section():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 80,
                "text": "财务报表附注\n本期支付银行手续费100万元，佣金支出20万元。",
            },
        ),
    )
    assert selected.spans == ()
    assert selected.unresolved_field_ids == ()
    assert any(gap.code == "chapter_missing" for gap in selected.gaps)


def test_reuses_accepted_structured_facts_and_skips_those_fields():
    records = _reference_records(
        "cp-300750-overview",
        "cp-300750-produces",
        "cp-300750-segment",
        "cp-300750-revenue",
    )
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 14,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    "公司主要从事动力电池、储能电池的研发、生产、销售。\n"
                    "二、风险因素"
                ),
            },
            {
                "page": 25,
                "text": "分产品\n动力电池系统 营业收入 316506369 千元\n三、主要销售客户",
            },
        ),
        accepted_records=records,
    )
    reused_fields = {item.field_id for item in selected.reused_facts}
    assert reused_fields >= {
        "business_overview_source",
        "segment_dimension",
        "operating_revenue",
    }
    assert {item.record_id for item in selected.reused_facts} >= {
        "cp-300750-overview",
        "cp-300750-segment",
        "cp-300750-revenue",
    }
    assert all(item.requires_llm is False for item in selected.reused_facts)
    assert "business_overview_source" not in selected.unresolved_field_ids
    assert "segment_dimension" not in selected.unresolved_field_ids
    assert "operating_revenue" not in selected.unresolved_field_ids


def test_unreadable_continuation_is_an_explicit_gap():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 14,
                "text": "报告期内公司从事的主要业务\n具体如下：",
            },
            {
                "page": 15,
                "text": "",
                "readable": False,
            },
        ),
    )
    assert selected.spans == ()
    assert any(
        gap.code == "page_unreadable" and gap.page == 15 for gap in selected.gaps
    )


def test_foreign_report_records_cannot_be_reused():
    records = _reference_records("cp-300750-overview")
    with pytest.raises(ValueError, match="another report"):
        select_core_evidence(
            report=_report(),
            pages=(
                {
                    "page": 14,
                    "text": (
                        "报告期内公司从事的主要业务\n"
                        "公司主要从事动力电池系统的研发、生产和销售。"
                    ),
                },
            ),
            accepted_records=records,
        )


def test_missing_continuation_page_is_an_explicit_gap():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 14,
                "text": "报告期内公司从事的主要业务\n具体如下：",
            },
        ),
    )
    assert selected.spans == ()
    assert any(
        gap.code == "page_unreadable" and gap.page == 15 for gap in selected.gaps
    )


def test_toc_and_inline_mentions_are_not_owned_headings():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 3,
                "text": "目录\n第五节 重要事项 分部信息 120",
            },
            {
                "page": 40,
                "text": "公司主要业务面临宏观经济波动及原材料价格上涨的风险。",
            },
        ),
    )
    assert selected.spans == ()
    assert any(gap.code == "chapter_missing" for gap in selected.gaps)


def test_reads_through_section_and_stops_at_next_heading():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 14,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    "公司主要从事动力电池、储能电池的研发、生产和销售。"
                    "相关产品已形成完整业务体系，并持续向下游客户交付。"
                ),
            },
            {
                "page": 15,
                "text": (
                    "公司通过向客户销售电池系统取得货款。\n"
                    "二、风险因素\n公司主要业务面临原材料价格波动风险。"
                ),
            },
        ),
    )
    assert selected.spans[0].continuation_pages == (15,)
    assert selected.spans[0].context_complete is True
    assert "取得货款" in selected.spans[0].excerpt
    assert "风险因素" not in selected.spans[0].excerpt
    assert selected.gaps == ()


def test_reuse_requires_matching_period():
    revenue = _reference_records("cp-300750-revenue")[0]
    stale = _record(
        {
            **json.loads(revenue.model_dump_json()),
            "record_id": "cp-300750-revenue-2024",
            "reported_period": "2024",
        }
    )
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 25,
                "text": "分产品\n动力电池系统 营业收入 316506369 千元\n三、主要销售客户",
            },
        ),
        accepted_records=(stale,),
    )
    assert selected.reused_facts == ()
    assert "operating_revenue" in selected.unresolved_field_ids


def test_header_only_revenue_table_stays_unresolved():
    revenue = _reference_records("cp-300750-revenue")[0]
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 25,
                "text": (
                    "分产品\n"
                    "项目          营业收入\n"
                    "动力电池系统  316506369\n"
                    "储能电池系统  100000\n"
                    "三、主要销售客户"
                ),
            },
        ),
        accepted_records=(revenue,),
    )
    assert any(item.record_id == "cp-300750-revenue" for item in selected.reused_facts)
    assert "operating_revenue" in selected.unresolved_field_ids


def test_subject_evidence_page_is_not_a_fact_source():
    revenue = _reference_records("cp-300750-revenue")[0]
    payload = json.loads(revenue.model_dump_json())
    payload["evidence"][0]["subject_evidence_pages"] = [70]
    cited = _record(payload)
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 70,
                "text": (
                    "分产品\n动力电池系统 营业收入 316506369 千元\n"
                    "三、主要销售客户"
                ),
            },
        ),
        accepted_records=(cited,),
    )
    assert selected.reused_facts == ()
    assert "operating_revenue" in selected.unresolved_field_ids


def test_wider_evidence_quote_does_not_close_partial_overview():
    overview = _reference_records("cp-300750-overview")[0]
    payload = json.loads(overview.model_dump_json())
    payload["evidence"][0]["anchor"]["bounded_quote"] = (
        "主要从事动力电池、储能电池的研发、生产、销售，"
        "并通过向客户收取技术服务费取得收入"
    )
    wider = _record(payload)
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 14,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    "公司主要从事动力电池、储能电池的研发、生产、销售，"
                    "并通过向客户收取技术服务费取得收入。\n"
                    "二、风险因素"
                ),
            },
        ),
        accepted_records=(wider,),
    )
    assert any(item.record_id == "cp-300750-overview" for item in selected.reused_facts)
    assert "business_overview_source" in selected.unresolved_field_ids


def test_accepted_overview_source_text_covers_matching_span():
    overview = _reference_records("cp-300750-overview")[0]
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 14,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    "公司主要从事动力电池、储能电池的研发、生产、销售。\n"
                    "二、风险因素"
                ),
            },
        ),
        accepted_records=(overview,),
    )
    assert any(item.record_id == "cp-300750-overview" for item in selected.reused_facts)
    assert "business_overview_source" not in selected.unresolved_field_ids


def test_partial_overview_does_not_close_added_revenue_sentence():
    overview = _reference_records("cp-300750-overview")[0]
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 14,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    "公司主要从事动力电池、储能电池的研发、生产、销售。"
                    "公司通过向客户收取技术服务费。\n"
                    "二、风险因素"
                ),
            },
        ),
        accepted_records=(overview,),
    )
    assert any(item.record_id == "cp-300750-overview" for item in selected.reused_facts)
    assert "business_overview_source" in selected.unresolved_field_ids


def test_same_object_on_other_page_is_not_reused():
    revenue = _reference_records("cp-300750-revenue")[0]
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 70,
                "text": (
                    "分产品\n动力电池系统 营业收入 316506369 千元\n"
                    "三、主要销售客户"
                ),
            },
        ),
        accepted_records=(revenue,),
    )
    assert selected.reused_facts == ()
    assert "operating_revenue" in selected.unresolved_field_ids


def test_interim_and_midyear_periods_do_not_match_annual_report():
    revenue = _reference_records("cp-300750-revenue")[0]
    interim = _record(
        {
            **json.loads(revenue.model_dump_json()),
            "record_id": "cp-300750-revenue-h1",
            "reported_period": "2025年1-6月",
        }
    )
    midyear = _record(
        {
            **json.loads(revenue.model_dump_json()),
            "record_id": "cp-300750-revenue-midyear",
            "reported_period": "2025-06-30",
        }
    )
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 25,
                "text": "分产品\n动力电池系统 营业收入 316506369 千元\n三、主要销售客户",
            },
        ),
        accepted_records=(interim, midyear),
    )
    assert selected.reused_facts == ()
    assert "operating_revenue" in selected.unresolved_field_ids


def test_one_revenue_fact_does_not_close_other_objects():
    revenue = _reference_records("cp-300750-revenue")[0]
    total = _record(
        {
            **json.loads(revenue.model_dump_json()),
            "record_id": "cp-300750-revenue-total",
            "segment_label": "合计",
            "measured_object": "合计",
            "source_native": {
                **json.loads(revenue.model_dump_json())["source_native"],
                "name": "合计",
            },
        }
    )
    selected = select_core_evidence(
        report=_reference_report(),
        pages=(
            {
                "page": 25,
                "text": (
                    "分产品\n"
                    "动力电池系统 营业收入 316506369 千元\n"
                    "储能电池系统 营业收入 57350891 千元\n"
                    "三、主要销售客户"
                ),
            },
        ),
        accepted_records=(revenue, total),
    )
    reused_ids = {item.record_id for item in selected.reused_facts}
    assert "cp-300750-revenue" in reused_ids
    assert "cp-300750-revenue-total" not in reused_ids
    assert "operating_revenue" in selected.unresolved_field_ids
    assert "segment_dimension" in selected.unresolved_field_ids


def test_prepared_evidence_keeps_full_context_and_binds_each_field():
    filler = "该等产品广泛应用于新能源汽车及储能电站等领域。"
    overview = (
        "报告期内公司从事的主要业务\n"
        "公司主要从事动力电池、储能电池的研发、生产和销售。"
        f"{filler * 8}"
        "公司通过向客户销售电池系统取得货款。\n"
        "二、风险因素"
    )
    assert len(overview) > 180
    selected = select_core_evidence(
        report=_report(),
        pages=({"page": 14, "text": overview},),
    )
    assert "取得货款" in selected.spans[0].excerpt
    assert "取得货款" not in selected.spans[0].bounded_quote
    prepared = selected.prepared_evidence
    assert {item.field_id for item in prepared} == {
        "business_overview_source",
        "explicit_activity",
    }
    assert all("取得货款" in item.evidence.anchor.bounded_quote for item in prepared)
    assert all(item.context_complete is True for item in prepared)


def test_heading_without_usable_context_is_extraction_failed():
    selected = select_core_evidence(
        report=_report(),
        pages=({"page": 14, "text": "主要业务"},),
    )
    assert selected.spans == ()
    assert any(gap.code == "extraction_failed" for gap in selected.gaps)


def test_bank_title_3_6_locates_overview_and_is_not_chapter_missing():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 4,
                "text": "目录\n3.6 公司主要业务情况 62",
            },
            {
                "page": 62,
                "text": (
                    "3.6 公司主要业务情况\n"
                    "公司致力于为客户提供全面而专业的金融服务，"
                    "涵盖商业信贷、交易银行、投资银行、电子银行、"
                    "跨境业务、离岸业务等多个领域。\n"
                    "二、风险因素\n宏观风险。"
                ),
            },
        ),
    )
    assert selected.spans[0].section_title == "公司主要业务情况"
    assert selected.spans[0].page == 62
    assert "商业信贷" in selected.spans[0].excerpt
    assert selected.spans[0].context_complete is True
    assert not any(gap.code == "chapter_missing" for gap in selected.gaps)


def test_stated_bank_overview_stays_closed_across_open_continuation_pages():
    pages = [
        {
            "page": 62,
            "text": (
                "3.6 公司主要业务情况\n"
                "公司致力于为客户提供全面而专业的金融服务，"
                "涵盖商业信贷、交易银行、投资银行、电子银行、"
                "跨境业务、离岸业务等多个领域。"
            ),
        }
    ]
    pages.extend(
        {"page": page, "text": f"续表正文第{page}页。"} for page in range(63, 71)
    )
    selected = select_core_evidence(report=_report(), pages=pages)
    assert selected.spans[0].section_title == "公司主要业务情况"
    assert selected.spans[0].context_complete is True
    assert selected.spans[0].continuation_pages == ()
    assert "商业信贷" in selected.spans[0].excerpt
    assert "续表正文" not in selected.spans[0].excerpt
    assert not any(gap.code == "page_unreadable" for gap in selected.gaps)


def test_official_business_scope_line_owns_same_line_value():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 22,
                "text": (
                    "经营范围 银行业务；证券投资基金托管；"
                    "公募证券投资基金销售；经批准的其它业务。\n"
                    "注册资本 293.52亿元"
                ),
            },
        ),
    )
    assert selected.spans[0].section_title == "经营范围"
    assert "银行业务" in selected.spans[0].excerpt
    assert not any(gap.code == "chapter_missing" for gap in selected.gaps)


def test_incidental_business_scope_mention_stays_unowned():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 80,
                "text": (
                    "本公司在报告期内未改变经营范围，"
                    "亦未因经营范围与同业产生诉讼。"
                ),
            },
        ),
    )
    assert selected.spans == ()
    assert any(gap.code == "chapter_missing" for gap in selected.gaps)


def test_standalone_title_is_preferred_over_business_scope_label():
    selected = select_core_evidence(
        report=_report(),
        pages=(
            {
                "page": 22,
                "text": "经营范围 银行业务；证券投资基金托管。",
            },
            {
                "page": 62,
                "text": (
                    "3.6 公司主要业务情况\n"
                    "公司致力于为客户提供全面而专业的金融服务。\n"
                    "二、风险因素"
                ),
            },
        ),
    )
    assert selected.spans[0].section_title == "公司主要业务情况"
    assert selected.spans[0].page == 62
