"""Provider-free segment-financial research for the four defining reports."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.company_profile.models import ChapterTask
from research.company_profile.segment_financial_research import (
    SEGMENT_FINANCIAL_PLAN_VERSION,
    SegmentFinancialResearchError,
    build_segment_financial_research_bundle,
    commit_segment_financial_research,
    interpret_segment_financial_pages,
    segment_financial_research_bindings,
)
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope

_ROOT = Path(__file__).resolve().parents[3]


def test_bindings_are_the_four_approved_reports():
    bindings = segment_financial_research_bindings()
    assert tuple(item.instrument_id for item in bindings) == (
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    )
    assert {item.exchange for item in bindings} == {"SZSE", "SSE", "BSE"}
    assert all(
        term
        for binding in bindings
        for scope in binding.scopes
        for term in scope.anchor_terms
    )
    assert SEGMENT_FINANCIAL_PLAN_VERSION.endswith("2026-09-28.1")


def test_same_amount_keeps_source_dimension_labels():
    hits, _ = interpret_segment_financial_pages(
        (
            (
                15,
                """
单位：元
营业收入 营业成本 毛利率
分行业
航空制造业 73,551,376,777.21 67,921,791,741.91 7.65%
分产品
航空产品 73,551,376,777.21 67,921,791,741.91 7.65%
""",
            ),
        )
    )
    revenues = [item for item in hits if item.field_id == "operating_revenue"]
    assert {(item.source_dimension, item.label) for item in revenues} == {
        ("分行业", "航空制造业"),
        ("分产品", "航空产品"),
    }
    assert len(revenues) == 2


def test_single_segment_note_and_missing_sales_mode_stay_empty():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                25,
                """
单位：千元
占公司营业收入或营业利润10%以上的行业、产品、地区、销售模式的情况
营业收入 营业成本 毛利率
分业务
电气机械及器材制造业 417,723,738 307,077,698 26.49%
分产品
动力电池系统 316,506,369 241,064,397 23.84%
其他业务只有收入表
分产品
其他业务 16,916,612
""",
            ),
            (
                223,
                "管理层认为本公司仅有一个经营分部，无需按经营分部披露分类业绩。",
            ),
        )
    )
    assert any(
        item.label == "动力电池系统" and item.value == "23.84%"
        for item in hits
        if item.field_id == "gross_margin_reported"
    )
    assert not any(
        item.label == "其他业务" and item.field_id == "operating_cost" for item in hits
    )
    assert not any(item.source_dimension == "分销售模式" for item in hits)
    assert any(
        item.page == 25
        and item.source_dimension == "分销售模式"
        and item.status.value == "not_disclosed"
        for item in coverages
    )
    assert {
        item.field_id
        for item in coverages
        if item.page == 223 and item.status.value == "not_applicable"
    } == {
        "segment_dimension",
        "operating_revenue",
        "operating_cost",
        "gross_margin_reported",
    }
    assert not any(
        item.page == 223 and item.field_id == "operating_revenue" for item in hits
    )


def test_elimination_change_columns_are_not_margin():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                19,
                """
单位：元
营业收入 营业成本 毛利率（%） 营业收入比上年增减（%） 营业成本比上年增减（%） 毛利率比上年增减（%）
分产品
新能源电池材料与服务 11,792,842,608.70 7,909,390,929.81 32.93 20.69 11.19 增加 5.73 个百分点
合并抵消项 -2,098,859,323.96 -2,070,706,126.81 118.30 104.19
""",
            ),
        )
    )
    elimination = [item for item in hits if item.label == "合并抵消项"]
    assert {(item.field_id, item.value, item.row_class) for item in elimination} == {
        ("segment_dimension", None, "consolidation_adjustment"),
        ("operating_revenue", "-2,098,859,323.96", "consolidation_adjustment"),
        ("operating_cost", "-2,070,706,126.81", "consolidation_adjustment"),
    }
    assert not any(item.value in {"118.30", "104.19"} for item in hits)
    assert any(
        item.label == "合并抵消项" and item.status.value == "not_disclosed"
        for item in coverages
        if item.field_id == "gross_margin_reported"
    )
    materials = [
        item
        for item in hits
        if item.label == "新能源电池材料与服务"
        and item.field_id == "gross_margin_reported"
    ]
    assert [item.value for item in materials] == ["32.93"]
    assert all(item.row_class != "Activity" for item in hits)


def test_dash_total_company_margin_and_blank_elimination_stay_distinct():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                16,
                """
单位：元
营业收入 1,032,297,576.85
营业成本 722,864,879.01
毛利率 29.98%
""",
            ),
            (
                17,
                """
单位：元
按产品分类
营业收入 营业成本 毛利率%
硅烷交联剂 575,652,405.05 456,934,716.38 20.62%
合计 1,032,297,576.85 722,864,879.01 - - - -
收入构成变动的原因：高毛利率产品收入占比提高。
按区域分类
营业收入 营业成本 毛利率%
内销客户 895,512,828.11 641,113,535.22 28.41%
""",
            ),
            (
                139,
                """
单位：元
地区分部
项目 境内 境外 分部间抵销 合计
营业收入 895,512,828.11 136,784,748.74 1,032,297,576.85
营业成本 641,113,535.22 81,751,343.79 722,864,879.01
""",
            ),
        )
    )
    assert not any(item.value == "29.98%" for item in hits)
    assert not any(
        item.label == "合计" and item.field_id == "gross_margin_reported"
        for item in hits
    )
    assert not any(item.value in {"0", "0.00"} for item in hits)
    assert any(
        item.page == 17 and item.label == "硅烷交联剂" and item.value == "20.62%"
        for item in hits
    )
    assert any(
        item.page == 17
        and item.label == "合计"
        and item.field_id == "gross_margin_reported"
        and item.status.value == "not_disclosed"
        for item in coverages
    )
    assert any(
        item.page == 139
        and item.label == "分部间抵销"
        and item.status.value == "unclear"
        for item in coverages
    )
    assert not any(
        item.page == 139 and item.label == "分部间抵销" and item.value for item in hits
    )
    assert any(
        item.page == 139
        and item.label == "境内"
        and item.field_id == "operating_revenue"
        for item in hits
    )
    assert any(item.page == 17 and item.label == "内销客户" for item in hits)


def test_two_pages_keep_note_margin_not_disclosed():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                15,
                """
单位：元
营业收入 营业成本 毛利率
分行业
航空制造业 73,551,376,777.21 67,921,791,741.91 7.65%
分销售模式
直销 75,358,958,001.86 68,984,052,885.87 8.46%
""",
            ),
            (
                178,
                """
单位：元
报告分部的财务信息
项目 西南分部 华中分部 分部间抵销 合计
营业收入 75,978,283,261.22 625,322,175.89 -3,606,586,716.08 75,358,958,001.86
营业成本 70,230,919,218.85 561,218,695.77 -3,536,259,900.08 68,984,052,885.87
""",
            ),
        )
    )
    page_15 = [
        item
        for item in hits
        if item.page == 15 and item.field_id == "gross_margin_reported"
    ]
    page_178 = [item for item in hits if item.page == 178]
    assert {item.value for item in page_15} == {"7.65%", "8.46%"}
    assert {item.page for item in page_178} == {178}
    assert not any(item.field_id == "gross_margin_reported" for item in page_178)
    assert any(
        item.page == 178
        and item.label == "分部间抵销"
        and item.field_id == "operating_revenue"
        and item.value == "-3,606,586,716.08"
        and item.row_class == "consolidation_adjustment"
        for item in hits
    )
    assert any(
        item.page == 178
        and item.field_id == "gross_margin_reported"
        and item.status.value == "not_disclosed"
        for item in coverages
    )
    assert {item.page for item in page_15}.isdisjoint({item.page for item in page_178})


def test_printed_margin_is_not_recalculated_from_revenue_and_cost():
    hits, _ = interpret_segment_financial_pages(
        (
            (
                1,
                """
单位：元
分产品
营业收入 营业成本 毛利率
甲产品 100,000.00 40,000.00 7.65%
""",
            ),
        )
    )
    margin = next(item for item in hits if item.field_id == "gross_margin_reported")
    assert margin.value == "7.65%"


def test_header_without_rows_is_extraction_failed():
    hits, coverages = interpret_segment_financial_pages(
        ((4, "单位：元\n营业收入 营业成本 毛利率\n"),)
    )
    assert hits == ()
    assert {item.status.value for item in coverages} == {"extraction_failed"}


def test_isolated_bundle_stays_accepted_for_review(tmp_path):
    class _Preparer:
        def prepare_single_chapter(
            self, *, asset, chapter_task, scopes, plan_version, page_results=None
        ):
            assert chapter_task is ChapterTask.EXTRACT_SEGMENT_FINANCIALS
            assert plan_version == SEGMENT_FINANCIAL_PLAN_VERSION
            spec = scopes[0]
            text = _fixture_for(asset.sample_id, spec.scope_id)
            pages = tuple(
                PreparedPageContext(
                    page=page,
                    text=text,
                    text_hash="a" * 64,
                    extraction_method="fixture",
                    quality_status="readable",
                )
                for page in spec.pages
            )
            evidence = pages[0]
            from research.company_profile.contracts import PreparedEvidence
            from research.company_profile.models import Evidence, TextAnchor

            prepared = PreparedEvidence(
                evidence=Evidence(
                    evidence_id=f"fixture-{spec.scope_id}",
                    report=asset.report,
                    page=evidence.page,
                    section_title=spec.section_titles[0],
                    anchor=TextAnchor(bounded_quote=text.strip().splitlines()[0]),
                ),
                field_id="segment_dimension",
            )
            return (
                PreparedRequestScope(
                    sample_id=asset.sample_id,
                    scope_id=spec.scope_id,
                    chapter_task=chapter_task,
                    field_ids=spec.field_ids,
                    report=asset.report,
                    evidence_bundle=(prepared,),
                    page_contexts=pages,
                    plan_version=plan_version,
                ),
            )

    destination = commit_segment_financial_research(
        tmp_path / "segment-financials",
        repository_root=_ROOT,
        run_id="segment-financials-fixture",
        preparer=_Preparer(),
    )
    payload = json.loads((destination / "result.json").read_text(encoding="utf-8"))
    assert payload["disposition"] == "accepted_for_review"
    assert payload["provider_calls"] == 0
    assert payload["production_authorization"] == "not_authorized"
    assert payload["processing_identity"] == {
        "rules": "company_profile_common_core.v1",
        "owned_page_facts": "v8",
        "material_input_facts": "v1",
    }
    assert {item["instrument_id"] for item in payload["reports"]} == {
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    }
    assert not {
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
        "source_review",
    } & set(payload)
    assert all(item["legal_empty"] is True for item in payload["coverage"])
    assert {item["coverage_status"] for item in payload["coverage"]} <= {
        "not_disclosed",
        "not_applicable",
        "unclear",
        "extraction_failed",
    }
    assert "data" not in destination.relative_to(tmp_path).parts


def _fixture_for(sample_id: str, scope_id: str) -> str:
    fixtures = {
        ("manufacturing-materials-300750-2025", "300750-segment-table"): """
单位：千元
占公司营业收入或营业利润10%以上的行业、产品、地区、销售模式的情况
营业收入 营业成本 毛利率
分产品
动力电池系统 316,506,369 241,064,397 23.84%
""",
        ("manufacturing-materials-300750-2025", "300750-single-segment"): (
            "管理层认为本公司仅有一个经营分部，无需披露分类业绩。"
        ),
        ("manufacturing-materials-603659-2025", "603659-segment-table"): """
单位：元
分产品
营业收入 营业成本 毛利率（%）
新能源电池材料与服务 11,792,842,608.70 7,909,390,929.81 32.93 20.69 11.19 增加 5.73 个百分点
合并抵消项 -2,098,859,323.96 -2,070,706,126.81 118.30 104.19
""",
        ("manufacturing-materials-920015-2025", "920015-product-region"): """
单位：元
按产品分类
营业收入 营业成本 毛利率%
硅烷交联剂 575,652,405.05 456,934,716.38 20.62%
合计 1,032,297,576.85 722,864,879.01 - - - -
""",
        ("manufacturing-materials-920015-2025", "920015-note-segments"): """
单位：元
地区分部
项目 境内 境外 分部间抵销 合计
营业收入 895,512,828.11 136,784,748.74 1,032,297,576.85
营业成本 641,113,535.22 81,751,343.79 722,864,879.01
""",
        ("manufacturing-materials-302132-2025-regime", "302132-mda-table"): """
单位：元
分销售模式
营业收入 营业成本 毛利率
直销 75,358,958,001.86 68,984,052,885.87 8.46%
""",
        ("manufacturing-materials-302132-2025-regime", "302132-note-segments"): """
单位：元
报告分部的财务信息
项目 西南分部 分部间抵销 合计
营业收入 75,978,283,261.22 -3,606,586,716.08 75,358,958,001.86
营业成本 70,230,919,218.85 -3,536,259,900.08 68,984,052,885.87
""",
    }
    return fixtures[(sample_id, scope_id)]


def test_bundle_builder_refuses_a_second_chapter():
    binding = segment_financial_research_bindings()[0]

    class _Wrong:
        def prepare_single_chapter(self, **kwargs):
            scope = kwargs["scopes"][0]
            page = PreparedPageContext(
                page=scope.pages[0],
                text="单位：元\n分产品\n营业收入 营业成本 毛利率\n甲 1,000.00 400.00 10%",
                text_hash="b" * 64,
                extraction_method="fixture",
                quality_status="readable",
            )
            from research.company_profile.contracts import PreparedEvidence
            from research.company_profile.models import Evidence, TextAnchor

            prepared = PreparedEvidence(
                evidence=Evidence(
                    evidence_id="fixture",
                    report=kwargs["asset"].report,
                    page=page.page,
                    section_title="分产品",
                    anchor=TextAnchor(bounded_quote="分产品"),
                ),
                field_id="segment_dimension",
            )
            return (
                PreparedRequestScope(
                    sample_id=kwargs["asset"].sample_id,
                    scope_id=scope.scope_id,
                    chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
                    field_ids=("sales_volume",),
                    report=kwargs["asset"].report,
                    evidence_bundle=(prepared,),
                    page_contexts=(page,),
                    plan_version=kwargs["plan_version"],
                ),
            )

    with pytest.raises(SegmentFinancialResearchError, match="second chapter"):
        build_segment_financial_research_bundle(
            repository_root=_ROOT,
            run_id="wrong-chapter",
            preparer=_Wrong(),
            bindings=(binding,),
        )
