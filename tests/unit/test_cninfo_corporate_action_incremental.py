import json
from datetime import date, datetime
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import pytest

from data_manager import DataManager
from data_sources.cninfo_corporate_action_incremental import (
    associate_exceptional_announcements,
    build_incremental_refresh_candidates,
    build_targeted_tdx_refresh_instruments,
    classify_daily_corporate_action_title,
    normalize_active_instruments,
    resolve_daily_announcement_window,
    resolve_tdx_refresh_mode,
    select_daily_semantic_anomalies,
    select_rotating_safety_instruments,
    select_rotating_safety_targets,
)
from research.announcements import (
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRouteAttempt,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
)


def _active(count=6):
    return normalize_active_instruments([
        {
            "instrument_id": f"{index:06d}.SZ",
            "symbol": f"{index:06d}",
            "exchange": "SZSE",
        }
        for index in range(1, count + 1)
    ])


def test_calendar_daily_announcement_window_includes_previous_day_and_overnight():
    run_at = datetime(
        2026, 7, 29, 3, 30,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )

    result = resolve_daily_announcement_window(
        run_at=run_at,
        schedule_mode="calendar_daily",
    )

    assert result["start_date"] == date(2026, 7, 28)
    assert result["end_date"] == date(2026, 7, 29)
    assert result["run_at"] == run_at


def test_trading_day_announcement_window_spans_long_holiday():
    result = resolve_daily_announcement_window(
        run_at=datetime(
            2026, 10, 9, 3, 30,
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ),
        schedule_mode="trading_day",
        previous_trading_day=date(2026, 9, 30),
    )

    assert result["start_date"] == date(2026, 9, 30)
    assert result["end_date"] == date(2026, 10, 9)


@pytest.mark.parametrize(
    "title",
    [
        "2025年度权益分派实施公告",
        "股权分置改革方案实施公告",
        "重整计划资本公积金转增股本实施公告",
        "业绩承诺补偿股份赠与完成公告",
        "业绩承诺补偿股份回购注销完成公告",
        "2025年度利润分配实施公告",
        "2025年度现金红利发放公告",
    ],
)
def test_daily_title_trigger_accepts_implemented_corporate_actions(title):
    decision = classify_daily_corporate_action_title(title)

    assert decision["selected"] is True
    assert decision["subject_markers"]
    assert decision["implementation_markers"]


@pytest.mark.parametrize(
    "title",
    [
        "2026年半年度报告",
        "第五届董事会第三次会议决议公告",
        "关于控股股东部分股份质押的公告",
        "关于为全资子公司提供担保的公告",
        "关于回购公司股份方案的公告",
        "限制性股票归属结果暨股份上市公告",
        "关于向特定对象发行股票不存在直接或间接财务资助或补偿的公告",
        "关于部分A股限制性股票回购注销实施公告",
        "关于回购股份完成注销的公告",
        "关于已回购股份完成注销暨股份变动的公告",
        "库存股注销完成暨股份变动公告",
        "关于减少注册资本实施完成的公告",
        "关于权益分派后调整限制性股票回购价格的公告",
    ],
)
def test_daily_title_trigger_rejects_unrelated_disclosures(title):
    assert classify_daily_corporate_action_title(title)["selected"] is False


def test_distribution_implementation_takes_precedence_over_exclusion_words():
    decision = classify_daily_corporate_action_title(
        "2025年度权益分派实施暨回购价格调整公告"
    )

    assert decision["selected"] is True
    assert decision["source_profiles"] == ["cninfo_dividend"]


def test_exceptional_title_requires_semantic_review():
    decision = classify_daily_corporate_action_title(
        "重整计划资本公积金转增股本实施公告"
    )

    assert decision["selected"] is True
    assert decision["requires_semantic_review"] is True
    assert "重整" in decision["exceptional_markers"]


def test_ordinary_complete_event_bypasses_semantic_selection():
    result = select_daily_semantic_anomalies([{
        "instrument_id": "000001.SZ",
        "source_event_key": "ordinary",
        "quality_status": "structured_complete",
    }])

    assert result["candidate_count"] == 0
    assert result["source_event_keys"] == []


def test_semantic_selector_merges_reasons_and_bounds_work():
    events = [
        {
            "instrument_id": "000001.SZ",
            "source_event_key": "partial",
            "quality_status": "partial_missing_ex_date",
        },
        {
            "instrument_id": "000002.SZ",
            "source_event_key": "special",
            "quality_status": "structured_complete",
        },
        {
            "instrument_id": "000003.SZ",
            "source_event_key": "conflict",
            "quality_status": "structured_complete",
        },
    ]

    result = select_daily_semantic_anomalies(
        events,
        exceptional_markers_by_event={"special": ["重整"]},
        conflict_event_keys=["conflict"],
        changed_event_keys=["conflict"],
        max_events=2,
    )

    assert result["source_event_keys"] == ["partial", "special"]
    assert result["deferred_source_event_keys"] == ["conflict"]
    assert result["reason_counts"] == {
        "current_run_tdx_conflict": 1,
        "exceptional_implementation_title": 1,
        "incomplete_structured_event": 1,
    }


def test_historical_conflict_is_not_selected_without_current_change():
    result = select_daily_semantic_anomalies(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "historical",
            "quality_status": "structured_complete",
        }],
        conflict_event_keys=["historical"],
        changed_event_keys=[],
    )

    assert result["candidate_count"] == 0


def test_deferred_event_keys_are_prioritized_before_new_candidates():
    result = select_daily_semantic_anomalies(
        [
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "new-event",
                "quality_status": "partial_missing_ex_date",
            },
            {
                "instrument_id": "000002.SZ",
                "source_event_key": "deferred-event",
                "quality_status": "partial_missing_ex_date",
            },
        ],
        priority_event_keys=["deferred-event"],
        max_events=1,
    )

    assert result["source_event_keys"] == ["deferred-event"]
    assert result["deferred_source_event_keys"] == ["new-event"]


def test_deferred_conflict_event_keeps_reason_without_new_source_change():
    result = select_daily_semantic_anomalies(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "deferred-conflict",
            "quality_status": "structured_complete",
        }],
        conflict_event_keys=["deferred-conflict"],
        changed_event_keys=[],
        priority_event_keys=["deferred-conflict"],
    )

    assert result["source_event_keys"] == ["deferred-conflict"]
    assert result["candidates"][0]["reason_codes"] == [
        "current_run_tdx_conflict"
    ]


def test_terminal_event_is_not_reselected_for_semantic_governance():
    result = select_daily_semantic_anomalies([{
        "instrument_id": "000001.SZ",
        "source_event_key": "resolved-event",
        "quality_status": "partial_missing_ex_date",
        "resolution_is_terminal": 1,
    }])

    assert result["candidate_count"] == 0


def test_changed_terminal_event_is_reselected_for_semantic_governance():
    result = select_daily_semantic_anomalies(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "changed-event",
            "quality_status": "partial_missing_ex_date",
            "resolution_is_terminal": 1,
        }],
        changed_event_keys=["changed-event"],
    )

    assert result["source_event_keys"] == ["changed-event"]
    assert result["candidates"][0]["reason_codes"] == [
        "incomplete_structured_event"
    ]


def test_exceptional_terminal_event_is_reselected_for_semantic_governance():
    result = select_daily_semantic_anomalies(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "exceptional-terminal",
            "quality_status": "structured_complete",
            "resolution_is_terminal": 1,
        }],
        exceptional_markers_by_event={
            "exceptional-terminal": ["重整"],
        },
    )

    assert result["source_event_keys"] == ["exceptional-terminal"]
    assert result["candidates"][0]["reason_codes"] == [
        "exceptional_implementation_title"
    ]


def test_exceptional_association_reports_unmatched_instrument():
    result = associate_exceptional_announcements(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "quality_status": "structured_complete",
            "announcement_date": date(2026, 7, 28),
        }],
        exceptional_announcements_by_instrument={
            "000001.SZ": [{
                "announcement_key": "announcement-1",
                "announcement_date": "2026-07-28",
                "exceptional_markers": ["股改"],
            }],
            "000002.SZ": [{
                "announcement_key": "announcement-2",
                "announcement_date": "2026-07-28",
                "exceptional_markers": ["重整"],
            }],
        },
    )

    assert result["exceptional_markers_by_event"] == {
        "event-1": ["股改"]
    }
    assert result["unmatched_instrument_ids"] == ["000002.SZ"]


def test_exceptional_association_selects_only_unique_nearby_event():
    result = associate_exceptional_announcements(
        [
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "ordinary-old",
                "announcement_date": date(2026, 6, 1),
            },
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "special-current",
                "announcement_date": date(2026, 7, 28),
            },
        ],
        exceptional_announcements_by_instrument={
            "000001.SZ": [{
                "announcement_key": "announcement-1",
                "announcement_date": "2026-07-29",
                "exceptional_markers": ["重整"],
            }],
        },
    )

    assert result["exceptional_markers_by_event"] == {
        "special-current": ["重整"]
    }
    assert result["announcement_keys_by_event"] == {
        "special-current": ["announcement-1"]
    }


def test_exceptional_association_uses_implementation_date_after_old_announcement():
    result = associate_exceptional_announcements(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "announcement_date": date(2026, 6, 1),
            "record_date": date(2026, 7, 28),
            "ex_date": date(2026, 7, 29),
        }],
        exceptional_announcements_by_instrument={
            "000001.SZ": [{
                "announcement_key": "announcement-1",
                "announcement_date": "2026-07-29",
                "exceptional_markers": ["重整"],
            }],
        },
    )

    assert result["exceptional_markers_by_event"] == {
        "event-1": ["重整"]
    }
    assert result["unmatched_announcements"] == []


def test_exceptional_association_defers_ambiguous_same_date_events():
    result = associate_exceptional_announcements(
        [
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "event-1",
                "announcement_date": date(2026, 7, 28),
            },
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "event-2",
                "announcement_date": date(2026, 7, 28),
            },
        ],
        exceptional_announcements_by_instrument={
            "000001.SZ": [{
                "announcement_key": "announcement-1",
                "announcement_date": "2026-07-28",
                "exceptional_markers": ["重整"],
            }],
        },
    )

    assert result["exceptional_markers_by_event"] == {}
    assert result["unmatched_instrument_ids"] == ["000001.SZ"]


def test_candidate_priority_and_explicit_ids_bypass_automatic_limit():
    result = build_incremental_refresh_candidates(
        active_instruments=_active(),
        explicit_ids=["000006.SZ"],
        retry_ids=["000005.SZ"],
        recent_event_ids=["000004.SZ"],
        announcement_ids=["000003.SZ", "000002.SZ"],
        safety_ids=["000001.SZ"],
        max_candidates=2,
    )

    assert result["candidate_ids"] == [
        "000006.SZ",
        "000005.SZ",
        "000004.SZ",
    ]
    assert result["explicit_count"] == 1
    assert result["automatic_count"] == 2
    assert result["deferred_count"] == 3
    assert result["deferred_reason_counts"] == {
        "announcement_activity": 2,
        "safety_sweep": 1,
    }
    json.dumps(result)


def test_candidate_reasons_are_merged_without_duplicates():
    result = build_incremental_refresh_candidates(
        active_instruments=_active(1),
        retry_ids=["000001.SZ"],
        recent_event_ids=["000001.SZ"],
        announcement_ids=["000001.SZ"],
        max_candidates=10,
    )

    candidate = result["candidates"][0]
    assert candidate["reasons"] == [
        "retry_indeterminate",
        "recent_event",
        "announcement_activity",
    ]
    assert candidate["priority"] == 10


def test_rotating_safety_selection_is_deterministic_and_bounded():
    instrument_ids = [f"{index:06d}.SZ" for index in range(1, 11)]

    first = select_rotating_safety_instruments(
        instrument_ids,
        as_of_date=date(2026, 7, 22),
        sample_size=3,
    )
    repeated = select_rotating_safety_instruments(
        list(reversed(instrument_ids)),
        as_of_date=date(2026, 7, 22),
        sample_size=3,
    )
    next_day = select_rotating_safety_instruments(
        instrument_ids,
        as_of_date=date(2026, 7, 23),
        sample_size=3,
    )

    assert first == repeated
    assert len(first) <= 3
    assert first != next_day


def test_endpoint_profiles_merge_without_adding_unrelated_retry_target():
    result = build_incremental_refresh_candidates(
        active_instruments=_active(2),
        retry_ids=["000001.SZ"],
        retry_profiles={"000001.SZ": ["cninfo_allotment"]},
        announcement_ids=["000001.SZ", "000002.SZ"],
        announcement_profiles={
            "000001.SZ": ["cninfo_dividend"],
            "000002.SZ": ["cninfo_dividend"],
        },
        max_candidates=10,
    )

    assert result["endpoint_target_count"] == 3
    assert result["endpoint_target_counts"] == {
        "cninfo_allotment": 1,
        "cninfo_dividend": 2,
    }
    assert result["candidates"][0]["source_profiles"] == [
        "cninfo_dividend",
        "cninfo_allotment",
    ]
    assert result["candidates"][1]["source_profiles"] == [
        "cninfo_dividend"
    ]


def test_special_title_routes_both_profiles_and_plain_rights_routes_one():
    special = classify_daily_corporate_action_title(
        "重整计划资本公积转增股本实施公告"
    )
    rights = classify_daily_corporate_action_title("配股发行实施公告")

    assert special["source_profiles"] == [
        "cninfo_dividend",
        "cninfo_allotment",
    ]
    assert rights["source_profiles"] == ["cninfo_allotment"]


def test_safety_and_tdx_reference_rotations_are_bounded():
    instrument_ids = [f"{index:06d}.SZ" for index in range(1, 21)]
    safety = select_rotating_safety_targets(
        instrument_ids,
        as_of_date=date(2026, 7, 30),
        sample_size=5,
    )
    plan = build_targeted_tdx_refresh_instruments(
        active_instrument_ids=instrument_ids,
        cninfo_candidate_ids=["000001.SZ"],
        announcement_ids=["000002.SZ"],
        retry_or_carryover_ids=["000003.SZ"],
        rotating_sample_size=4,
        as_of_date=date(2026, 7, 30),
    )

    assert sum(len(values) for values in safety.values()) == 5
    assert set(safety) == {"cninfo_dividend", "cninfo_allotment"}
    assert plan["rotating_sample_count"] == 4
    assert plan["instrument_count"] <= 7
    assert {"000001.SZ", "000002.SZ", "000003.SZ"} <= set(
        plan["instrument_ids"]
    )


def test_tdx_refresh_mode_resolution_is_explicit():
    assert resolve_tdx_refresh_mode("targeted") == "targeted"
    assert resolve_tdx_refresh_mode("full") == "full"
    assert resolve_tdx_refresh_mode("auto", periodic_full_due=True) == "full"
    assert resolve_tdx_refresh_mode("auto", periodic_full_due=False) == "targeted"
    with pytest.raises(ValueError, match="tdx_refresh_mode"):
        resolve_tdx_refresh_mode("fast")


@pytest.mark.asyncio
async def test_market_announcement_scan_maps_activity_and_persists_governance():
    manager = DataManager()
    storage = Mock()
    storage.get_announcement_scan_state.return_value = {
        "metadata": {
            "pending_candidate_ids": ["600001.SH"],
            "pending_factor_instrument_ids": ["920000.BJ"],
            "pending_special_announcements_by_instrument": {
                "600001.SH": [{
                    "announcement_key": "special-1",
                    "announcement_date": "2026-07-21",
                    "exceptional_markers": ["重整"],
                }]
            },
            "pending_semantic_event_keys_by_instrument": {
                "600001.SH": ["event-1"]
            },
        },
    }
    manager.research_config = Mock(enabled=True)
    manager.research_storage = storage
    active_instruments = {
        "600000.SH": {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
        },
        "600001.SH": {
            "instrument_id": "600001.SH",
            "symbol": "600001",
            "exchange": "SSE",
        },
    }
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="announcement-1",
        announcement_key=build_announcement_key("cninfo", "announcement-1"),
        title="2025年度权益分派实施公告",
        published_at="2026-07-22T01:00:00+00:00",
        exchange="SSE",
        market="SSE",
        symbols=("600000",),
    )
    future_record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="announcement-2",
        announcement_key=build_announcement_key("cninfo", "announcement-2"),
        title="2026年度权益分派实施公告",
        published_at="2026-07-22T02:00:00+00:00",
        exchange="SSE",
        market="SSE",
        symbols=("600000",),
    )

    def acquire(query: AnnouncementQuery):
        scan = AnnouncementScanResult(
            source="cninfo",
            query=query,
            status="success",
            records=(record, future_record),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=2,
            max_published_at="2026-07-22T02:00:00+00:00",
            provider_cursor=ProviderCursor(
                kind="published_at",
                value="2026-07-22T02:00:00+00:00",
            ),
            is_complete=True,
            stop_reason="last_page",
        )
        return AnnouncementRouteResult(
            query=query,
            status="success",
            selected_source="cninfo",
            scan_result=scan,
            attempts=(AnnouncementRouteAttempt(
                source="cninfo",
                status="success",
                record_count=1,
                selected_count=1,
                pages_scanned=1,
            ),),
        )

    manager._build_official_announcement_acquisition_service = Mock(
        return_value=Mock(acquire=acquire)
    )
    discovery = await manager._scan_cninfo_daily_announcement_activity(
        active_instruments=active_instruments,
        exchanges=["SSE"],
        start_date=date(2026, 7, 15),
        end_date=date(2026, 7, 22),
        run_at=datetime(
            2026, 7, 22, 9, 30,
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ),
        overlap_days=3,
        page_size=30,
        max_pages=60,
        request_interval_seconds=0,
    )
    persisted = manager._persist_cninfo_daily_announcement_activity(
        discovery,
        pending_candidate_ids=["600001.SH"],
        pending_special_announcements_by_instrument={
            "600001.SH": [{
                "announcement_key": "special-1",
                "announcement_date": "2026-07-21",
                "exceptional_markers": ["重整"],
            }]
        },
        pending_semantic_event_keys_by_instrument={
            "600001.SH": ["event-1"]
        },
        pending_factor_instrument_ids=["600000.SH"],
        active_instruments=active_instruments,
    )

    assert discovery["announcement_instrument_ids"] == ["600000.SH"]
    assert discovery["deferred_announcement_instrument_ids"] == ["600001.SH"]
    assert discovery["deferred_factor_instrument_ids"] == ["920000.BJ"]
    assert discovery["deferred_special_announcements_by_instrument"] == {
        "600001.SH": [{
            "announcement_key": "special-1",
            "announcement_date": "2026-07-21",
            "exceptional_markers": ["重整"],
        }]
    }
    assert discovery["deferred_semantic_event_keys_by_instrument"] == {
        "600001.SH": ["event-1"]
    }
    assert discovery["matched_announcements"] == 1
    assert persisted == {"scan_states_persisted": 1, "audits_persisted": 1}
    saved_scan = storage.upsert_announcement_scan_state.call_args.kwargs[
        "scan_result"
    ]
    assert saved_scan.selected_records[0].selection_reasons == (
        "corporate_action_announcement",
    )
    assert saved_scan.provider_cursor == ProviderCursor(
        kind="published_at",
        value="2026-07-22T01:30:00+00:00",
    )
    assert saved_scan.max_published_at == "2026-07-22T01:30:00+00:00"
    assert saved_scan.diagnostics["provider_cursor_bounded_by_run_at"] is True
    assert saved_scan.diagnostics["observed_max_published_at"] == (
        "2026-07-22T02:00:00+00:00"
    )
    assert storage.upsert_announcement_scan_state.call_args.kwargs["metadata"][
        "pending_candidate_ids"
    ] == ["600001.SH"]
    assert storage.upsert_announcement_scan_state.call_args.kwargs["metadata"][
        "pending_factor_instrument_ids"
    ] == ["600000.SH"]
    assert storage.upsert_announcement_scan_state.call_args.kwargs["metadata"][
        "pending_special_announcements_by_instrument"
    ] == {
        "600001.SH": [{
            "announcement_key": "special-1",
            "announcement_date": "2026-07-21",
            "exceptional_markers": ["重整"],
        }]
    }
    assert storage.upsert_announcement_scan_state.call_args.kwargs["metadata"][
        "pending_semantic_event_keys_by_instrument"
    ] == {"600001.SH": ["event-1"]}
    storage.store_announcement_audit.assert_called_once()


def test_deferred_announcement_queue_is_prioritized_on_next_run():
    result = build_incremental_refresh_candidates(
        active_instruments=_active(3),
        deferred_announcement_ids=["000003.SZ"],
        announcement_ids=["000001.SZ", "000002.SZ"],
        max_candidates=1,
    )

    assert result["candidate_ids"] == ["000003.SZ"]
    assert result["deferred_by_reason"]["announcement_activity"] == [
        "000001.SZ",
        "000002.SZ",
    ]


@pytest.mark.asyncio
async def test_discovery_merges_retry_event_announcement_and_safety_candidates():
    manager = DataManager()
    manager._load_cninfo_daily_retry_instrument_ids = AsyncMock(
        return_value=["000001.SZ"]
    )
    manager._load_cninfo_daily_event_instrument_ids = AsyncMock(
        return_value=["000002.SZ"]
    )
    manager._scan_cninfo_daily_announcement_activity = AsyncMock(return_value={
        "status": "success",
        "announcement_instrument_ids": ["000003.SZ"],
        "pages_scanned": 1,
        "announcements_seen": 1,
        "matched_announcements": 1,
        "errors": [],
        "route_results": {},
        "matched_records_by_exchange": {},
        "matched_instruments_by_record": {},
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={})

    result = await manager.discover_a_share_cninfo_daily_candidates(
        active_instruments=[
            {
                "instrument_id": f"00000{index}.SZ",
                "symbol": f"00000{index}",
                "exchange": "SZSE",
            }
            for index in range(1, 6)
        ],
        exchanges=["SZSE"],
        start_date=date(2026, 7, 15),
        end_date=date(2026, 7, 22),
        candidate_limit=4,
        safety_sweep_size=1,
        request_interval_seconds=0,
    )

    assert result["status"] == "success"
    assert result["candidate_ids"][:3] == [
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
    ]
    assert result["candidate_count"] == 4
    assert result["reason_counts"]["safety_sweep"] == 1
    manager._persist_cninfo_daily_announcement_activity.assert_not_called()
