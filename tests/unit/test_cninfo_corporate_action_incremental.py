import json
from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_corporate_action_incremental import (
    build_incremental_refresh_candidates,
    normalize_active_instruments,
    select_rotating_safety_instruments,
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


@pytest.mark.asyncio
async def test_market_announcement_scan_maps_activity_and_persists_governance():
    manager = DataManager()
    storage = Mock()
    storage.get_announcement_scan_state.return_value = None
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
        title="测试公告",
        published_at="2026-07-22T01:00:00+00:00",
        exchange="SSE",
        market="SSE",
        symbols=("600000",),
    )

    def acquire(query: AnnouncementQuery):
        scan = AnnouncementScanResult(
            source="cninfo",
            query=query,
            status="success",
            records=(record,),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=1,
            provider_cursor=ProviderCursor(
                kind="published_at",
                value="2026-07-22T01:00:00+00:00",
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
        overlap_days=3,
        page_size=30,
        max_pages=60,
        request_interval_seconds=0,
    )
    persisted = manager._persist_cninfo_daily_announcement_activity(
        discovery,
        pending_candidate_ids=["600001.SH"],
        active_instruments=active_instruments,
    )

    assert discovery["announcement_instrument_ids"] == ["600000.SH"]
    assert discovery["matched_announcements"] == 1
    assert persisted == {"scan_states_persisted": 1, "audits_persisted": 1}
    saved_scan = storage.upsert_announcement_scan_state.call_args.kwargs[
        "scan_result"
    ]
    assert saved_scan.selected_records[0].selection_reasons == (
        "instrument_activity_trigger",
    )
    assert storage.upsert_announcement_scan_state.call_args.kwargs["metadata"][
        "pending_candidate_ids"
    ] == ["600001.SH"]
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
