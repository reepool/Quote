from datetime import date

from data_sources.cninfo_special_action_resolution import (
    announcement_match_reasons,
    build_candidate_evidence,
    build_search_target,
    build_search_targets,
    classify_special_action,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)


def _share_reform_row():
    return {
        "instrument_id": "600108.SH",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "announcement_date": date(2006, 6, 9),
        "record_date": date(2006, 6, 12),
        "ex_date": None,
        "quality_status": "partial_missing_ex_date",
        "capitalization_shares_per_share": 0.34,
        "bonus_shares_per_share": 0.68,
        "description": "10送6.8转增3.4股派0.3581058元",
        "raw_payload": {"分红类型": "股改分红"},
    }


def test_special_action_target_uses_structured_bounded_window():
    row = _share_reform_row()

    target = build_search_target(row)

    assert classify_special_action(row) == "share_reform"
    assert target is not None
    assert target.start_date == date(2006, 5, 30)
    assert target.end_date == date(2006, 7, 12)
    assert target.search_basis == "role_cluster:announcement_date+record_date"


def test_outlier_announcement_date_creates_separate_role_windows():
    row = {
        **_share_reform_row(),
        "instrument_id": "000007.SZ",
        "announcement_date": date(1993, 5, 16),
        "record_date": date(1992, 11, 7),
        "share_arrival_date": date(1992, 11, 10),
        "description": "10送2股派0.5元",
        "raw_payload": {"分红类型": "年度分红"},
    }
    trading_days = [
        date(1992, 11, 6),
        date(1992, 11, 9),
        date(1992, 11, 10),
    ]

    targets = build_search_targets(row, trading_days=trading_days)

    assert len(targets) == 2
    assert targets[0].search_basis == (
        "role_cluster:record_date+share_arrival_date"
    )
    assert targets[0].candidate_effective_dates == (date(1992, 11, 9),)
    assert targets[1].search_basis == "role_cluster:announcement_date"


def test_record_date_candidate_cannot_fall_after_share_arrival():
    row = {
        **_share_reform_row(),
        "record_date": date(2006, 6, 12),
        "share_arrival_date": date(2006, 6, 12),
    }

    target = build_search_target(
        row,
        trading_days=[date(2006, 6, 12), date(2006, 6, 13)],
    )

    assert target is not None
    assert target.candidate_effective_dates == ()


def test_announcement_metadata_creates_candidate_without_effective_date():
    target = build_search_target(_share_reform_row())
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="120220001",
        announcement_key=build_announcement_key("cninfo", "120220001"),
        title="<em>股权分置改革</em>方案实施公告",
        published_at="2006-06-09T08:00:00+08:00",
        market="SSE",
        exchange="SSE",
        symbols=("600108",),
        attachments=(
            AnnouncementAttachment(
                source_url="finalpage/2006-06-09/120220001.PDF",
                resolved_url=(
                    "https://static.cninfo.com.cn/"
                    "finalpage/2006-06-09/120220001.PDF"
                ),
            ),
        ),
        raw_payload={"announcementId": "120220001"},
    )

    reasons = announcement_match_reasons(target, record.title)
    evidence = build_candidate_evidence(target, [record])

    assert "event_class:share_reform" in reasons
    assert evidence[0]["resolution_status"] == "candidate"
    assert evidence[0]["effective_date"] is None
    assert evidence[0]["evidence_url"].startswith(
        "https://static.cninfo.com.cn/"
    )


def test_meaningful_cash_only_missing_date_is_discoverable():
    row = {
        **_share_reform_row(),
        "description": "每10股派1元",
        "raw_payload": {"分红类型": "年度分红"},
        "capitalization_shares_per_share": None,
        "bonus_shares_per_share": None,
        "cash_dividend_per_share": 0.1,
    }

    assert classify_special_action(row) == "missing_date_distribution"
    assert build_search_target(row) is not None


def test_empty_missing_date_row_is_not_discoverable():
    row = {
        **_share_reform_row(),
        "description": None,
        "raw_payload": {},
        "capitalization_shares_per_share": None,
        "bonus_shares_per_share": None,
        "cash_dividend_per_share": None,
    }

    assert classify_special_action(row) is None
    assert build_search_target(row) is None
