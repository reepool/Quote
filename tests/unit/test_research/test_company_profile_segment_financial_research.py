"""Provider-free segment-financial research for the four defining reports."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.models import ChapterTask
from research.company_profile.segment_financial_research import (
    SEGMENT_FINANCIAL_HISTORICAL_PLAN_VERSION,
    SEGMENT_FINANCIAL_PLAN_VERSION,
    SEGMENT_FINANCIAL_PRIOR_PLAN_VERSION,
    SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION,
    SegmentFinancialResearchError,
    build_segment_financial_research_bundle,
    commit_segment_financial_research,
    freeze_segment_financial_research_enqueue,
    interpret_segment_financial_pages,
    replay_segment_financial_research,
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
    assert SEGMENT_FINANCIAL_PLAN_VERSION.endswith("2026-09-29.3")
    assert SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION.endswith("2026-10-01.4")
    assert SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION != SEGMENT_FINANCIAL_PLAN_VERSION
    assert SEGMENT_FINANCIAL_PRIOR_PLAN_VERSION.endswith("2026-09-29.2")
    assert SEGMENT_FINANCIAL_HISTORICAL_PLAN_VERSION.endswith("2026-09-28.1")
    dossier_hashes = {
        "300750-sz-2025.md": "d97587dbe5a14ee086b54ade6ef59ce8d4cf112e02d7367046913a4a4e2f9cb8",
        "603659-sh-2025.md": "f1fdfc75ccf24ba2581fbf4731b9b9bc8974198825f2e5310075c33fc78e4cb5",
        "920015-bj-2025.md": "e09f80661e03235d46cb3de8a1c525f7c53d6dd24770fb9e749d55e0d173c06b",
        "302132-sz-2025.md": "726cc7554016e85ae3158490bc15789105d8087a30698cbacfd4a94c0ce3445c",
    }
    for binding in bindings:
        dossier = _ROOT / binding.relative_dossier_path
        assert dossier.is_file()
        assert "changes/archive/" in binding.relative_dossier_path
        assert (
            hashlib.sha256(dossier.read_bytes()).hexdigest()
            == dossier_hashes[dossier.name]
        )
    pages = {
        scope.scope_id: scope.pages for binding in bindings for scope in binding.scopes
    }
    assert pages["300750-revenue-cost-note"] == (195,)
    assert pages["920015-income-composition"] == (16,)
    assert pages["302132-mda-table"] == (14, 15)


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


def test_wrapped_pdf_lines_keep_the_source_label_and_amount():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                25,
                """
单位：千元
占公司营业收入或营业利润10%以上的行业、产品、地区、销售模式的情况
营业收入 营业成本 毛利率
分业务
电气机械及器材
制造业 417,723,738
307,077,698 26.49% 17.17% 14.37% 1.80%
""",
            ),
            (
                19,
                """
分产品 营业收入 营业成本 毛利率（%）
新能源电池材
料与服务 11,792,842,608.70 7,909,390,929.81 32.93 20.69 11.19 增加 5.73 个
百分点
境外 936,450,803.90 821,701,248.84 12.25
""",
            ),
        )
    )
    assert any(
        item.label == "电气机械及器材制造业" and item.value == "26.49%" for item in hits
    )
    assert any(
        item.label == "新能源电池材料与服务" and item.value == "32.93" for item in hits
    )
    assert not any(item.label.startswith("百分点") for item in hits)
    assert not any(item.value == "118.30" for item in hits)
    assert coverages or hits


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
        ("manufacturing-materials-300750-2025", "300750-revenue-cost-note"): """
单位：千元
营业收入和营业成本
本期发生额 上期发生额
收入 成本 收入 成本
主营业务 406,785,222 308,033,498 344,524,735 265,082,813
""",
        ("manufacturing-materials-920015-2025", "920015-income-composition"): """
单位：元
收入构成
项目 2025 年 2024 年 变动比例%
主营业务收入 1,014,715,549.55 1,232,762,645.51 -17.69%
主营业务成本 708,496,694.98 887,317,477.02 -20.15%
""",
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


def test_page25_composition_does_not_fill_cost_or_margin():
    hits, _coverages = interpret_segment_financial_pages(
        (
            (
                25,
                """
单位：千元
营业收入构成
金额 占营业收入比重 金额 占营业收入比重 同比增减
分地区
境内 294,060,576 69.40% 251,677,045 69.52% 16.84%
境外 129,641,258 30.60% 110,335,509 30.48% 17.50%
项目 营业收入 营业成本 毛利率 营业收入比上年同期增减 营业成本比上年同期增减 毛利率比上年同期增减
分地区
境内 294,060,576 223,497,885 24.00% 16.84% 14.22% 1.75%
境外 129,641,258 88,885,412 31.44% 17.50% 14.19% 1.99%
""",
            ),
        )
    )
    values = {(item.field_id, item.label, item.value) for item in hits}
    assert ("operating_cost", "境内", "251,677,045") not in values
    assert ("gross_margin_reported", "境内", "69.52%") not in values
    assert ("operating_cost", "境外", "110,335,509") not in values
    assert ("gross_margin_reported", "境外", "30.48%") not in values
    assert ("operating_cost", "境内", "223,497,885") in values
    assert ("gross_margin_reported", "境内", "24.00%") in values
    assert ("operating_cost", "境外", "88,885,412") in values
    assert ("gross_margin_reported", "境外", "31.44%") in values


def test_ten_current_cells_become_measurements_without_merging_repeats():
    hits, _coverages = interpret_segment_financial_pages(
        (
            (
                24,
                """
单位：千元
营业收入构成
金额 占营业收入比重 金额 占营业收入比重 同比增减
营业收入合计 423,701,834 100.00% 362,012,554 100.00% 17.04%
分产品
其他业务 16,916,612 3.99% 17,487,818 4.83% -3.27%
""",
            ),
            (
                195,
                """
单位：千元
营业收入和营业成本
项目
本期发生额 上期发生额
收入 成本 收入 成本
主营业务 406,785,222 308,033,498 344,524,735 265,082,813
其他业务 16,916,612 4,349,799 17,487,818 8,436,146
合计 423,701,834 312,383,297 362,012,554 273,518,959
营业收入、营业成本的分解信息
电气机械及器材制造业 采选冶炼行业 合计
营业收入 营业成本 营业收入 营业成本 营业收入 营业成本
境内 288,841,747 218,857,074 5,218,829 4,640,812 294,060,576 223,497,885
税金及附加
项目 本期发生额 上期发生额
合计 2,832,322 2,057,466
""",
            ),
            (
                16,
                """
单位：元
营业收入 1,032,297,576.85
毛利率 29.98%
收入构成
项目 2025 年 2024 年 变动比例%
主营业务收入 1,014,715,549.55 1,232,762,645.51 -17.69%
主营业务成本 708,496,694.98 887,317,477.02 -20.15%
""",
            ),
            (
                14,
                """
单位：元
营业收入构成
金额 占营业收入比重 金额 占营业收入比重 同比增减
营业收入合计 75,358,958,001.86 100% 65,054,925,106.17 100% 15.84%
分销售模式
直销 75,358,958,001.86 100.00% 65,054,925,106.17 100.00% 15.84%
""",
            ),
            (
                15,
                """
单位：元
营业成本构成
金额 占营业成本比
重 金额 占营业成本比
重
航空制造业 营业成本 67,921,791,741.91 98.46% 56,870,679,297.66 98.01% 19.43%
其他 营业成本 1,062,261,143.96 1.54% 1,156,035,387.54 1.99% -8.11%
""",
            ),
        )
    )
    measurements = {
        (item.page, item.field_id, item.source_dimension, item.label, item.value)
        for item in hits
        if item.field_id != "segment_dimension"
    }
    expected = {
        (24, "operating_revenue", "营业收入构成", "营业收入合计", "423,701,834"),
        (24, "operating_revenue", "分产品", "其他业务", "16,916,612"),
        (195, "operating_revenue", "营业收入和营业成本", "主营业务", "406,785,222"),
        (195, "operating_cost", "营业收入和营业成本", "主营业务", "308,033,498"),
        (195, "operating_cost", "营业收入和营业成本", "其他业务", "4,349,799"),
        (195, "operating_cost", "营业收入和营业成本", "合计", "312,383,297"),
        (16, "operating_revenue", "收入构成", "主营业务收入", "1,014,715,549.55"),
        (16, "operating_cost", "收入构成", "主营业务成本", "708,496,694.98"),
        (14, "operating_revenue", "营业收入构成", "营业收入合计", "75,358,958,001.86"),
        (15, "operating_cost", "营业成本构成", "其他", "1,062,261,143.96"),
    }
    assert expected <= measurements
    totals = {
        item.label
        for item in hits
        if item.field_id == "operating_revenue" and item.value == "75,358,958,001.86"
    }
    assert totals == {"营业收入合计", "直销"}
    company_totals = [
        item
        for item in hits
        if item.field_id == "operating_revenue" and item.value == "423,701,834"
    ]
    assert {(item.page, item.label) for item in company_totals} == {
        (24, "营业收入合计"),
        (195, "合计"),
    }
    assert not any(
        item.value
        in {
            "362,012,554",
            "344,524,735",
            "265,082,813",
            "218,857,074",
            "2,832,322",
            "1,232,762,645.51",
            "887,317,477.02",
            "29.98%",
        }
        for item in hits
    )
    assert any(
        item.page == 15
        and item.source_dimension == "营业成本构成"
        and item.label == "航空制造业"
        and item.field_id == "operating_cost"
        and item.value == "67,921,791,741.91"
        for item in hits
    )


def test_split_change_header_keeps_118_30_out_of_margin():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                19,
                """
单位：元
分产品 营业收入 营业成本 毛利率
（%）
营业收入比上年增减（%） 营业成本比上年增减（%） 毛利率比上年增减（%）
合并抵消项 -2,098,859,323.96 -2,070,706,126.81 118.30 104.19
""",
            ),
        )
    )
    assert not any(item.value in {"118.30", "104.19"} for item in hits)
    assert any(
        item.label == "合并抵消项"
        and item.row_class == "consolidation_adjustment"
        and item.field_id == "operating_revenue"
        for item in hits
    )
    assert any(
        item.label == "合并抵消项" and item.status.value == "not_disclosed"
        for item in coverages
        if item.field_id == "gross_margin_reported"
    )


def test_runtime_rules_do_not_hardcode_sample_identity():
    source = Path(segment_financial_research_bindings.__code__.co_filename).read_text(
        encoding="utf-8"
    )
    start = source.index("def interpret_segment_financial_pages")
    end = source.index("def build_segment_financial_research_bundle")
    rules = source[start:end]
    for token in (
        "300750",
        "603659",
        "920015",
        "302132",
        "宁德时代",
        "璞泰来",
        "锦华新材",
        "中航成飞",
        "动力电池",
        "硅烷",
        "航空产品",
        "195",
    ):
        assert token not in rules


def test_printed_footnote_sections_stay_separate():
    hits, coverages = interpret_segment_financial_pages(
        (
            (
                139,
                """
单位：元
(1) 地区分部
项目 境内 境外 分部间抵销 合计
营业收入 895,512,828.11 136,784,748.74 1,032,297,576.85
营业成本 641,113,535.22 81,751,343.79 722,864,879.01
(2) 业务分部
项目 硅烷交联剂 羟胺盐 其他主营产品 其他 分部间抵销 合计
营业收入 575,652,405.05 285,241,030.19 153,822,114.31 17,582,027.30 1,032,297,576.85
营业成本 456,934,716.38 159,734,692.43 91,827,286.17 14,368,184.03 722,864,879.01
""",
            ),
            (
                178,
                """
单位：元
（2） 报告分部的财务信息
项目 西南分部 分部间抵销 合计
营业收入 75,978,283,261.22 -3,606,586,716.08 75,358,958,001.86
营业成本 70,230,919,218.85 -3,536,259,900.08 68,984,052,885.87
""",
            ),
        )
    )
    totals = [
        item
        for item in hits
        if item.page == 139
        and item.field_id == "operating_revenue"
        and item.label == "合计"
    ]
    assert {(item.source_dimension, item.value) for item in totals} == {
        ("地区分部", "1,032,297,576.85"),
        ("业务分部", "1,032,297,576.85"),
    }
    assert not any(item.source_dimension == "报告分部" for item in hits)
    for section in ("地区分部", "业务分部"):
        assert any(
            item.page == 139
            and item.source_dimension == section
            and item.label == "分部间抵销"
            and item.status.value == "unclear"
            for item in coverages
        )
    note = [
        item
        for item in hits
        if item.page == 178 and item.field_id != "segment_dimension"
    ]
    assert note
    assert {item.source_dimension for item in note} == {"报告分部的财务信息"}
    assert any(
        item.label == "分部间抵销"
        and item.field_id == "operating_revenue"
        and item.value == "-3,606,586,716.08"
        and item.row_class == "consolidation_adjustment"
        for item in note
    )
    assert not any(item.value in {"0", "0.00"} for item in hits)


def test_prior_successor_replay_bytes_stay_unchanged():
    root = (
        _ROOT
        / "openspec/changes/archive"
        / "2026-09-29-repair-segment-financial-column-binding-and-cell-coverage"
        / "replay/20260929.2"
    )
    expected = {
        "enqueue.json": "2a4c778790678197eda9472a907b8fc1c132be7f08fdbd5d95a3a0cc86c70f19",
        "run.json": "b5aad9e75ea4200a385ad107f6c044d7d14787f231156b077888018a68708b5e",
        "segment-financial-stage4-segment-financials-20260929.2/result.json": (
            "6ca3e2960e11f90a13aaaa75e71dc43452344864b78d10db51566f9bff6912b1"
        ),
    }
    for name, digest in expected.items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest


def test_original_replay_bytes_stay_unchanged():
    root = (
        _ROOT
        / "openspec/changes/archive"
        / "2026-09-29-scope-manufacturing-materials-stage4-segment-financials"
        / "replay/20260928"
    )
    expected = {
        "enqueue.json": "935578cc64a58a84d84224443354c8a6e5f0d9a6167fd1753cbf156e7079ffe3",
        "run.json": "974dbbfda8608cd1de535215390a9b1accf67b8e7810a9770966620e2e45634b",
        "segment-financial-stage4-segment-financials-20260928/result.json": (
            "7c2d5ea6e6433586b46545ad2c597a3c7cc60016ca1023559e7a137c8117d859"
        ),
        "source_review.json": "ff2f01fd4365cc621dcfeac2b2778d97b446bcf3d73c1d7f3bb06cb7b9035c2d",
    }
    for name, digest in expected.items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest


def test_sf3_replay_bytes_stay_unchanged():
    root = (
        _ROOT
        / "openspec/changes/archive"
        / "2026-09-29-repair-segment-financial-footnote-dimension-binding"
        / "replay/20260929.3"
    )
    expected = {
        "enqueue.json": "0f2ebfa087f8aa4327d15d4e0366af568bcd06a41ab923ba595899ec4e3cdd9b",
        "run.json": "2ab053d1b3e80b52351ea7963e45f323b97a2706971514c99486eaefd3234d1f",
        "segment-financial-stage4-segment-financials-20260929.3/result.json": (
            "92ec1e2a8be5d4a6ee8bc5b492d7dc09a26dbc84bfbd0ad592ca5aab6cdc1eeb"
        ),
        "source_review.json": "4446fb7619f39b49e865bc563cf7673b98d2288d481c0b229de67394ee9c3c74",
    }
    for name, digest in expected.items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest


def test_default_enqueue_keeps_the_current_plan_and_readable_dossiers(tmp_path):
    destination = freeze_segment_financial_research_enqueue(
        tmp_path / "default",
        repository_root=_ROOT,
    )
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["plan_version"] == SEGMENT_FINANCIAL_PLAN_VERSION
    assert payload["production_authorization"] == "not_authorized"
    assert [item["instrument_id"] for item in payload["reports"]] == [
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    ]
    for report in payload["reports"]:
        dossier = _ROOT / report["dossier_path"]
        assert dossier.is_file()
        assert (
            hashlib.sha256(dossier.read_bytes()).hexdigest() == report["dossier_sha256"]
        )


def test_explicit_successor_plan_stays_isolated_from_the_default(tmp_path):
    class _Preparer:
        def prepare_single_chapter(
            self, *, asset, chapter_task, scopes, plan_version, page_results=None
        ):
            assert plan_version == SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION
            spec = scopes[0]
            text = _fixture_for(asset.sample_id, spec.scope_id)
            pages = tuple(
                PreparedPageContext(
                    page=page,
                    text=text,
                    text_hash="c" * 64,
                    extraction_method="fixture",
                    quality_status="readable",
                )
                for page in spec.pages
            )
            from research.company_profile.contracts import PreparedEvidence
            from research.company_profile.models import Evidence, TextAnchor

            prepared = PreparedEvidence(
                evidence=Evidence(
                    evidence_id=f"fixture-{spec.scope_id}",
                    report=asset.report,
                    page=pages[0].page,
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

    official = (
        _ROOT
        / "openspec/changes"
        / "scope-stage4-segment-financial-successor-after-failed-observations"
        / "replay/20261001"
    )
    official_before = _artifact_fingerprint(official)
    present = tmp_path / "already-present"
    present.mkdir()
    (present / "enqueue.json").write_text('{"kept": true}\n', encoding="utf-8")
    present_before = _artifact_fingerprint(present)
    output = tmp_path / "successor"
    replay_segment_financial_research(
        output,
        repository_root=_ROOT,
        run_id="stage4-segment-financials-20261001",
        preparer=_Preparer(),
        plan_version=SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION,
    )
    enqueue = json.loads((output / "enqueue.json").read_text(encoding="utf-8"))
    run = json.loads((output / "run.json").read_text(encoding="utf-8"))
    result = json.loads(
        (
            output
            / "segment-financial-stage4-segment-financials-20261001"
            / "result.json"
        ).read_text(encoding="utf-8")
    )
    assert enqueue["plan_version"] == SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION
    assert run["plan_version"] == SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION
    assert result["plan_version"] == SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION
    assert result["production_authorization"] == "not_authorized"
    assert [item["instrument_id"] for item in enqueue["reports"]] == [
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    ]
    for report in enqueue["reports"]:
        dossier = _ROOT / report["dossier_path"]
        assert dossier.is_file()
        assert (
            hashlib.sha256(dossier.read_bytes()).hexdigest() == report["dossier_sha256"]
        )
        assert report["plan_version"] == SEGMENT_FINANCIAL_SUCCESSOR_PLAN_VERSION
    elimination = [
        item
        for item in result["facts"]
        if item["label"] == "合并抵消项" and item["field_id"] == "gross_margin_reported"
    ]
    assert elimination
    assert {item["row_class"] for item in elimination} == {"consolidation_adjustment"}
    assert not any(item["value"] in {"0", "0.00"} for item in result["facts"])
    note = [
        item
        for item in result["facts"]
        if item["page"] == 178 and item["field_id"] != "segment_dimension"
    ]
    assert note
    assert {item["source_dimension"] for item in note} == {"报告分部的财务信息"}
    assert not {
        "recall",
        "accuracy",
        "critical_numeric_errors",
        "expansion_gates_met",
        "source_review",
    } & set(result)
    official = (
        _ROOT
        / "openspec/changes"
        / "scope-stage4-segment-financial-successor-after-failed-observations"
        / "replay/20261001"
    )
    assert output.resolve() != official.resolve()
    assert _artifact_fingerprint(official) == official_before
    assert _artifact_fingerprint(present) == present_before
    assert not (present / "run.json").exists()


def _artifact_fingerprint(path: Path) -> tuple[tuple[str, str], ...] | None:
    """Return file hashes, or None when the directory does not exist."""

    if not path.exists():
        return None
    rows = []
    for item in sorted(
        candidate for candidate in path.rglob("*") if candidate.is_file()
    ):
        rows.append(
            (
                str(item.relative_to(path)),
                hashlib.sha256(item.read_bytes()).hexdigest(),
            )
        )
    return tuple(rows)


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
