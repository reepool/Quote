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
                    "并通过向客户销售电池系统取得货款。"
                ),
            },
        ),
    )
    assert selected.production_authorization == "not_authorized"
    assert selected.spans[0].page == 14
    assert selected.spans[0].section_title == "报告期内公司从事的主要业务"
    assert "取得货款" in selected.spans[0].excerpt
    assert "主要从事" in selected.spans[0].excerpt
    assert "principal_business" in selected.spans[0].dimension_ids
    assert "revenue_model" in selected.spans[0].dimension_ids
    assert "business_overview_source" in selected.unresolved_field_ids
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
                    "公司主要从事动力电池系统的研发、生产和销售。"
                ),
            },
            {
                "page": 25,
                "text": "分产品\n动力电池系统 营业收入 316506369 千元",
            },
        ),
        accepted_records=records,
    )
    reused_fields = {item.field_id for item in selected.reused_facts}
    assert reused_fields >= {
        "business_overview_source",
        "explicit_activity",
        "segment_dimension",
        "operating_revenue",
    }
    assert all(item.requires_llm is False for item in selected.reused_facts)
    assert selected.unresolved_field_ids == ()


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


def test_heading_without_usable_context_is_extraction_failed():
    selected = select_core_evidence(
        report=_report(),
        pages=({"page": 14, "text": "主要业务"},),
    )
    assert selected.spans == ()
    assert any(gap.code == "extraction_failed" for gap in selected.gaps)
