from research.financial_disclosure_events import (
    build_financial_disclosure_event_index,
    build_financial_disclosure_events,
    build_financial_symbol_index,
    financial_disclosure_event_filter,
    infer_report_periods_from_title,
    is_non_primary_financial_announcement_title,
)
from research.providers.cninfo_announcements import CninfoAnnouncementRecord


def test_financial_disclosure_filter_detects_delayed_periodic_report():
    record = CninfoAnnouncementRecord(
        announcement_id="a1",
        title="关于无法按期披露2025年年度报告暨股票停牌的公告",
        announcement_time="2026-05-06",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )

    assert financial_disclosure_event_filter(record) == [
        "periodic_report_delayed",
        "periodic_report_related_trading_risk",
        "pending_delisting_risk",
    ]


def test_financial_disclosure_filter_detects_regular_periodic_report():
    record = CninfoAnnouncementRecord(
        announcement_id="a2",
        title="2026年第一季度报告",
        announcement_time="2026-04-30",
        market="SSE",
        column="sse",
        symbols=["600000"],
    )

    assert financial_disclosure_event_filter(record) == ["periodic_report"]


def test_financial_disclosure_filter_detects_formal_correction_and_revision():
    correction = CninfoAnnouncementRecord(
        announcement_id="a3",
        title="关于2025年年度报告的更正公告",
        announcement_time="2026-05-10",
        market="SSE",
        column="sse",
        symbols=["600000"],
    )
    revision = CninfoAnnouncementRecord(
        announcement_id="a4",
        title="2025年年度报告修订公告",
        announcement_time="2026-05-11",
        market="SSE",
        column="sse",
        symbols=["600000"],
    )

    assert financial_disclosure_event_filter(correction) == [
        "periodic_report_correction"
    ]
    assert financial_disclosure_event_filter(revision) == [
        "periodic_report_correction"
    ]


def test_financial_disclosure_filter_excludes_non_primary_report_related_titles():
    excluded_titles = [
        "2025年年度报告业绩说明会预告公告",
        "2025年度报告（英文版）",
        "2025年年度报告（图文版）",
        "关于2025年年度报告的信息披露监管问询函回复",
        "会计师事务所关于2025年年度报告信息披露监管问询函回复的专项说明",
        "关于参加山西辖区上市公司2026年投资者网上集体接待日暨年报说明会预告公告",
        "2025年年度报告摘要",
    ]
    for index, title in enumerate(excluded_titles, start=1):
        record = CninfoAnnouncementRecord(
            announcement_id=f"excluded-{index}",
            title=title,
            announcement_time="2026-05-20",
            market="BSE",
            column="neeq",
            symbols=["920001"],
        )
        assert financial_disclosure_event_filter(record) == []
        assert is_non_primary_financial_announcement_title(title) is True


def test_financial_disclosure_filter_keeps_delayed_notice_even_if_title_mentions_inquiry():
    record = CninfoAnnouncementRecord(
        announcement_id="delay-1",
        title="关于问询函事项导致无法按期披露2025年年度报告的公告",
        announcement_time="2026-05-20",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )

    assert financial_disclosure_event_filter(record) == ["periodic_report_delayed"]


def test_infer_report_periods_from_chinese_periodic_report_title():
    assert infer_report_periods_from_title("关于无法按期披露2025年年度报告的公告") == [
        "2025-12-31"
    ]
    assert infer_report_periods_from_title("2026年第一季度报告无法按期披露公告") == [
        "2026-03-31"
    ]
    assert infer_report_periods_from_title("2025年度报告（英文版）") == [
        "2025-12-31"
    ]


def test_infer_report_periods_does_not_use_activity_year_for_annual_meeting_notice():
    assert (
        infer_report_periods_from_title(
            "关于参加山西辖区上市公司2026年投资者网上集体接待日暨年报说明会预告公告"
        )
        == []
    )


def test_build_financial_disclosure_events_maps_symbols_to_instruments():
    record = CninfoAnnouncementRecord(
        announcement_id="a1",
        title="关于无法按期披露2025年年度报告暨股票停牌的公告",
        announcement_time="2026-05-06",
        market="SSE",
        column="sse",
        symbols=["688121"],
    )

    events = build_financial_disclosure_events(
        [record],
        {"688121": {"instrument_id": "688121.SH", "exchange": "SSE"}},
    )

    assert len(events) == 1
    assert events[0].instrument_id == "688121.SH"
    assert events[0].report_period == "2025-12-31"
    assert events[0].classification == "pending_delisting_risk"


def test_build_financial_disclosure_events_excludes_noisy_selected_records():
    record = CninfoAnnouncementRecord(
        announcement_id="briefing-1",
        title="2025年年度报告业绩说明会预告公告",
        announcement_time="2026-05-10",
        market="BSE",
        column="neeq",
        symbols=["920001"],
    )

    events = build_financial_disclosure_events(
        [record],
        {"920001": {"instrument_id": "920001.BJ", "exchange": "BSE"}},
    )

    assert events == []


def test_build_financial_symbol_index_includes_instrument_id_prefix():
    index = build_financial_symbol_index(
        [{"instrument_id": "688121.SH", "symbol": "688121", "exchange": "SSE"}]
    )

    assert index["688121"]["instrument_id"] == "688121.SH"


def test_build_financial_disclosure_event_index_ignores_unapproved_classification():
    index = build_financial_disclosure_event_index(
        [
            {
                "instrument_id": "002731.SZ",
                "report_period": "2025-12-31",
                "classification": "pending_delisting_risk",
            },
            {
                "instrument_id": "002731.SZ",
                "report_period": "2026-03-31",
                "classification": "ordinary_periodic_report",
            },
        ]
    )

    assert sorted(index["002731.SZ"]) == ["2025-12-31"]
