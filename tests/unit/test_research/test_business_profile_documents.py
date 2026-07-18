from research.business_profile_documents import (
    business_profile_document_family,
    classify_business_profile_document,
    infer_business_profile_report_period,
    infer_profile_change_event_hints,
)


def test_classifies_full_periodic_reports_and_excludes_summaries():
    annual = classify_business_profile_document("万华化学2025年年度报告")
    summary = classify_business_profile_document("万华化学2025年年度报告摘要")
    corrected = classify_business_profile_document("万华化学2025年年度报告（修订版）")

    assert annual.document_type == "annual_report"
    assert annual.selected is True
    assert annual.is_full_report is True
    assert summary.document_type == "annual_report_summary"
    assert summary.selected is False
    assert summary.exclusion_reason == "summary_not_full_report"
    assert corrected.document_type == "annual_report_correction"
    assert corrected.is_correction is True


def test_periodic_report_keyword_in_governance_policy_is_not_a_full_report():
    result = classify_business_profile_document(
        "年度报告信息披露重大差错责任追究管理办法（2025年修订）"
    )

    assert result.selected is False
    assert result.document_type == "other"
    assert result.exclusion_reason == "unsupported_document_class"


def test_correction_notice_is_selected_but_not_classified_as_corrected_full_report():
    result = classify_business_profile_document("关于《2023年年度报告》的补充更正公告")

    assert result.selected is True
    assert result.document_type == "annual_report_correction_notice"
    assert result.is_correction is True
    assert result.is_full_report is False


def test_periodic_report_meeting_notice_is_not_a_full_report():
    result = classify_business_profile_document("2025年年度报告业绩说明会预告公告")

    assert result.selected is False
    assert result.document_type == "annual_report_related"
    assert result.exclusion_reason == "periodic_report_related_not_full_report"


def test_supplemented_full_report_is_kept_as_a_correction():
    result = classify_business_profile_document("2025年年度报告（补充后）")

    assert result.selected is True
    assert result.document_type == "annual_report_correction"
    assert result.is_full_report is True
    assert result.is_correction is True


def test_periodic_title_and_report_period_use_the_same_year_contract():
    unsupported = classify_business_profile_document("2025半年度报告")
    supported = classify_business_profile_document("2025年半年度报告")

    assert unsupported.selected is False
    assert supported.selected is True
    assert (
        infer_business_profile_report_period(
            "2025年半年度报告",
            "2026-08-01T00:00:00+08:00",
        )
        == "2025-06-30"
    )


def test_accepts_observed_official_semiannual_title_variants():
    titles = (
        "宝泰隆新材料股份有限公司2022年度半年度报告全文",
        "中国石化2021年半年报",
    )

    for title in titles:
        result = classify_business_profile_document(title, adjunct_type="PDF")
        assert result.document_type == "semiannual_report"
        assert result.selected is True

    assert (
        infer_business_profile_report_period(titles[0], "2026-01-01")
        == "2022-06-30"
    )
    assert (
        infer_business_profile_report_period(titles[1], "2026-01-01")
        == "2021-06-30"
    )


def test_classifies_operating_resource_contract_and_hedging_disclosures():
    cases = {
        "2026年6月份主要经营数据公告": "operating_data",
        "关于矿产资源储量更新的公告": "resource_report",
        "关于签署重大销售合同的公告": "major_contract",
        "关于开展商品期货套期保值业务的公告": "hedging_disclosure",
    }

    for title, expected in cases.items():
        result = classify_business_profile_document(title)
        assert result.document_type == expected
        assert result.selected is True


def test_restructuring_title_creates_review_hint_only():
    result = classify_business_profile_document(
        "关于重大资产置换及发行股份购买资产暨关联交易的公告"
    )

    assert result.document_type == "profile_change_event"
    assert result.selected is True
    assert result.profile_event_hints == [
        "reverse_merger",
        "major_asset_restructuring",
    ]


def test_profile_change_hints_cover_long_term_business_changes():
    hints = infer_profile_change_event_hints(
        "关于控制权变更并新增主营业务及变更公司名称的公告"
    )

    assert hints == [
        "control_change",
        "principal_business_change",
        "company_name_change",
    ]


def test_all_profile_change_hints_are_selected_for_discovery():
    cases = {
        "关于新增主营业务的公告": "principal_business_change",
        "关于出售重大资产的公告": "business_disposal",
        "关于重大资产购买的公告": "major_asset_restructuring",
        "关于重大收购事项的公告": "business_acquisition",
        "关于证券简称变更的公告": "company_name_change",
    }

    for title, event_type in cases.items():
        result = classify_business_profile_document(title, adjunct_type="PDF")
        assert result.selected is True
        assert result.document_type == "profile_change_event"
        assert event_type in result.profile_event_hints


def test_non_pdf_attachment_is_not_selected():
    result = classify_business_profile_document(
        "2025年年度报告",
        adjunct_type="DOCX",
    )

    assert result.selected is False
    assert result.exclusion_reason == "attachment_not_pdf"


def test_period_and_document_family_are_stable_across_corrections():
    assert (
        infer_business_profile_report_period(
            "某公司2025年半年度报告（修订版）",
            "2026-04-21T08:00:00+08:00",
        )
        == "2025-06-30"
    )
    assert (
        business_profile_document_family("semiannual_report_correction")
        == "semiannual_report"
    )
    assert (
        infer_business_profile_report_period(
            "某公司重大资产出售公告",
            "2026-04-21T08:00:00+08:00",
        )
        == "2026-04-21"
    )
