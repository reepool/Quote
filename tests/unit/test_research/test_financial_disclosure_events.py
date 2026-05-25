from research.financial_disclosure_events import (
    build_financial_disclosure_event_index,
    build_financial_disclosure_events,
    build_financial_symbol_index,
    financial_disclosure_event_filter,
    infer_report_periods_from_title,
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


def test_infer_report_periods_from_chinese_periodic_report_title():
    assert infer_report_periods_from_title("关于无法按期披露2025年年度报告的公告") == [
        "2025-12-31"
    ]
    assert infer_report_periods_from_title("2026年第一季度报告无法按期披露公告") == [
        "2026-03-31"
    ]


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
