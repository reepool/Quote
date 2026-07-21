from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementScanResult,
    build_announcement_key,
)


class _FakeAnnouncementService:
    def acquire(self, query, *, selectors=None):
        record = AnnouncementRecord(
            source="cninfo",
            source_announcement_id="120220001",
            announcement_key=build_announcement_key("cninfo", "120220001"),
            title="股权分置改革方案实施公告",
            published_at="2006-06-09T08:00:00+08:00",
            market=query.scope.market,
            exchange=query.scope.exchange,
            symbols=(query.scope.symbol or "600108",),
            attachments=(
                AnnouncementAttachment(
                    source_url="finalpage/2006-06-09/120220001.PDF",
                    resolved_url=(
                        "https://static.cninfo.com.cn/"
                        "finalpage/2006-06-09/120220001.PDF"
                    ),
                ),
            ),
        )
        reasons = []
        for selector in selectors or ():
            reasons.extend(selector(record) or ())
        selected = (record.with_selection_reasons(reasons),) if reasons else ()
        scan_result = AnnouncementScanResult(
            source="cninfo",
            query=query.for_source("cninfo"),
            status="success",
            records=(record,),
            selected_records=selected,
            announcements_seen=1,
            pages_scanned=1,
            requests_made=1,
            is_complete=True,
        )
        return SimpleNamespace(scan_result=scan_result)


def _event_row(
    instrument_id="600108.SH",
    source_event_key="event-1",
    announcement_date=date(2006, 6, 9),
    record_date=date(2006, 6, 12),
):
    return {
        "instrument_id": instrument_id,
        "source_profile": "cninfo_dividend",
        "source_event_key": source_event_key,
        "announcement_date": announcement_date,
        "record_date": record_date,
        "ex_date": None,
        "capitalization_shares_per_share": 0.34,
        "bonus_shares_per_share": 0.68,
        "cash_dividend_per_share": 0.03581058,
        "description": "10送6.8转增3.4股派0.3581058元",
        "quality_status": "partial_missing_ex_date",
        "raw_payload_json": '{"分红类型": "股改分红"}',
    }


def _manager(event_rows=None, adjacent_rows=None):
    manager = DataManager()
    manager.db_ops = Mock()
    normalized_event_rows = list(event_rows or [_event_row()])
    normalized_adjacent_rows = list(adjacent_rows or [{
        "instrument_id": "600108.SH",
        "announcement_date": date(2006, 6, 9),
        "record_date": date(2006, 6, 12),
    }])

    async def execute_read_query(query, _params):
        if "quality_status = 'partial_missing_ex_date'" in query:
            return normalized_event_rows
        return normalized_adjacent_rows

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.save_corporate_action_effective_date_evidence = AsyncMock(
        return_value={
            "inserted": 1,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
        }
    )
    manager._build_official_announcement_acquisition_service = Mock(
        return_value=_FakeAnnouncementService()
    )
    return manager


@pytest.mark.asyncio
async def test_discovery_dry_run_scans_candidates_but_does_not_write():
    manager = _manager()

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE", "BSE"],
        instrument_ids=["600108.SH"],
        dry_run=True,
        max_events=10,
    )

    assert result["status"] == "dry_run"
    assert result["parameters"]["scanned_exchanges"] == ["SSE"]
    assert result["parameters"]["excluded_exchanges"] == ["BSE"]
    assert result["evidence"]["candidate_count"] == 1
    assert result["evidence"]["resolved_count"] == 0
    manager.db_ops.save_corporate_action_effective_date_evidence.assert_not_awaited()


@pytest.mark.asyncio
async def test_discovery_write_persists_candidate_only():
    manager = _manager()

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        instrument_ids=["600108.SH"],
        dry_run=False,
        max_events=10,
    )

    assert result["status"] == "success"
    rows = manager.db_ops.save_corporate_action_effective_date_evidence.await_args.args[0]
    assert rows[0]["resolution_status"] == "candidate"
    assert rows[0]["effective_date"] is None


@pytest.mark.asyncio
async def test_discovery_exposes_deterministic_continuation_offset():
    event_rows = [
        _event_row(),
        _event_row(
            instrument_id="600109.SH",
            source_event_key="event-2",
            announcement_date=date(2007, 6, 9),
            record_date=date(2007, 6, 12),
        ),
    ]
    manager = _manager(event_rows=event_rows)

    first = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=False,
        max_events=1,
        target_offset=0,
    )

    assert first["status"] == "partial"
    assert first["targets"]["searchable_events"] == 2
    assert first["targets"]["batch_events"] == 1
    assert first["targets"]["next_target_offset"] == 1

    manager = _manager(event_rows=event_rows)
    second = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=False,
        max_events=1,
        target_offset=1,
    )

    assert second["status"] == "success"
    assert second["targets"]["batch_events"] == 1
    assert second["targets"]["has_more"] is False
    saved = manager.db_ops.save_corporate_action_effective_date_evidence.await_args.args[0]
    assert saved[0]["source_event_key"] == "event-2"


@pytest.mark.asyncio
async def test_discovery_excludes_unanchored_window_outside_requested_range(
):
    row = {
        **_event_row(announcement_date=None, record_date=None),
        "fiscal_period": "1995年度",
    }
    manager = _manager(
        event_rows=[row],
        adjacent_rows=[
            {"instrument_id": "600108.SH", "ex_date": date(1995, 10, 1)},
            {"instrument_id": "600108.SH", "ex_date": date(1996, 2, 1)},
        ],
    )

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="2020-01-01",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=True,
    )

    assert result["targets"]["searchable_events"] == 0
    assert result["targets"]["skipped_outside_range"] == 1
    assert result["evidence"]["candidate_count"] == 0
