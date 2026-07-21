from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_announcement_title_llm import (
    TITLE_CLASSIFICATION_SCHEMA_VERSION,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    build_announcement_key,
)


class _FakeAnnouncementService:
    def __init__(
        self,
        *,
        status="success",
        is_complete=True,
        errors=(),
        stop_reason=None,
        title="股权分置改革方案实施公告",
    ):
        self.status = status
        self.is_complete = is_complete
        self.errors = tuple(errors)
        self.stop_reason = stop_reason
        self.title = title

    def acquire(self, query, *, selectors=None):
        record = AnnouncementRecord(
            source="cninfo",
            source_announcement_id="120220001",
            announcement_key=build_announcement_key("cninfo", "120220001"),
            title=self.title,
            published_at="2006-06-09T08:00:00+08:00",
            market=query.scope.market,
            exchange=query.scope.exchange,
            symbols=(query.scope.symbol or "600108",),
            raw_payload={"announcementId": "120220001"},
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
            status=self.status,
            records=(record,),
            selected_records=selected,
            announcements_seen=1,
            pages_scanned=1,
            requests_made=1,
            is_complete=self.is_complete,
            stop_reason=self.stop_reason,
            errors=self.errors,
        )
        return AnnouncementRouteResult(
            query=query,
            status=self.status,
            selected_source="cninfo",
            scan_result=scan_result,
            attempts=(),
            fallback_used=False,
            fallback_reason=None,
        )


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


def _manager(event_rows=None, adjacent_rows=None, announcement_service=None):
    manager = DataManager()
    manager.db_ops = Mock()
    normalized_event_rows = list(event_rows or [_event_row()])
    normalized_adjacent_rows = list(
        adjacent_rows
        if adjacent_rows is not None
        else [{
            "instrument_id": "600108.SH",
            "announcement_date": date(2006, 6, 9),
            "record_date": date(2006, 6, 12),
        }]
    )

    async def execute_read_query(query, _params):
        if "quality_status = 'partial_missing_ex_date'" in query:
            return normalized_event_rows
        return normalized_adjacent_rows

    manager.db_ops.execute_read_query = AsyncMock(side_effect=execute_read_query)
    manager.db_ops.get_trading_days = AsyncMock(
        return_value=[
            date(2006, 6, 9),
            date(2006, 6, 12),
            date(2006, 6, 13),
            date(2006, 6, 14),
        ]
    )
    manager.db_ops.save_corporate_action_effective_date_evidence = AsyncMock(
        return_value={
            "inserted": 1,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
        }
    )
    manager._build_official_announcement_acquisition_service = Mock(
        return_value=announcement_service or _FakeAnnouncementService()
    )
    storage = Mock()
    storage.start_ingestion_run = Mock(return_value=42)
    storage.finish_ingestion_run = Mock()
    storage.upsert_announcement_scan_state = Mock(return_value={"status": "success"})
    storage.store_announcement_audit = Mock()
    manager._require_research_storage = Mock(return_value=storage)
    manager._announcement_test_storage = storage
    return manager


def _title_llm_response(*, announcement_id="120220001", relevance="relevant"):
    role = (
        "compensation_share_distribution"
        if relevance != "unrelated"
        else "periodic_report"
    )
    return SimpleNamespace(
        data={
            "schema_version": TITLE_CLASSIFICATION_SCHEMA_VERSION,
            "events": [{
                "source_event_key": "event-1",
                "event_applicability": "effectful",
                "applicability_reason": "The structured event has share effects",
                "classifications": [{
                    "announcement_id": announcement_id,
                    "relevance": relevance,
                    "announcement_role": role,
                    "confidence": 0.97,
                    "reason": "Semantic title decision",
                }],
            }],
        },
        model="fake-model",
        request_hash="request-hash",
        response_hash="response-hash",
        request_id="request-id",
        latency_ms=10,
        attempt_count=1,
    )


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
    manager._announcement_test_storage.upsert_announcement_scan_state.assert_not_called()
    manager._announcement_test_storage.store_announcement_audit.assert_not_called()
    manager._announcement_test_storage.start_ingestion_run.assert_not_called()
    manager._announcement_test_storage.finish_ingestion_run.assert_not_called()


@pytest.mark.asyncio
async def test_discovery_filters_exact_source_event_keys():
    manager = _manager()

    await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        source_event_keys=["event-1"],
        dry_run=True,
        max_events=10,
    )

    query, params = manager.db_ops.execute_read_query.await_args_list[0].args
    assert "source_event_key IN" in query
    assert params["source_event_key_0"] == "event-1"


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
    storage = manager._announcement_test_storage
    storage.upsert_announcement_scan_state.assert_called_once()
    state_kwargs = storage.upsert_announcement_scan_state.call_args.kwargs
    assert state_kwargs["scan_result"].query.purpose_key == (
        "a_share_cninfo_special_action_discovery"
    )
    assert state_kwargs["metadata"]["business_domain"] == "corporate_action"
    assert state_kwargs["metadata"]["business_run_id"] == result[
        "ingestion_run_id"
    ]
    storage.store_announcement_audit.assert_called_once()
    audit_kwargs = storage.store_announcement_audit.call_args.kwargs
    assert audit_kwargs["purpose_key"] == "a_share_cninfo_special_action_discovery"
    assert audit_kwargs["instrument_id"] == "600108.SH"
    assert audit_kwargs["symbol"] == "600108"
    assert audit_kwargs["ingestion_run_id"] == 42
    assert audit_kwargs["record"].announcement_key == "cninfo:120220001"
    assert audit_kwargs["record"].raw_payload == {
        "announcementId": "120220001"
    }
    assert "event_class:share_reform" in audit_kwargs[
        "record"
    ].selection_reasons
    assert result["announcement_governance"] == {
        "ingestion_run_id": 42,
        "scan_states_persisted": 1,
        "audits_persisted": 1,
        "errors": 0,
    }
    storage.finish_ingestion_run.assert_called_once()
    assert storage.finish_ingestion_run.call_args.kwargs["status"] == "success"


@pytest.mark.asyncio
async def test_discovery_governance_failure_is_partial_but_keeps_domain_write():
    manager = _manager()
    manager._announcement_test_storage.upsert_announcement_scan_state.side_effect = (
        RuntimeError("research storage unavailable")
    )

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        instrument_ids=["600108.SH"],
        dry_run=False,
        max_events=10,
    )

    assert result["status"] == "partial"
    assert result["announcement_governance"]["errors"] == 1
    assert result["evidence"]["candidate_count"] == 1
    manager.db_ops.save_corporate_action_effective_date_evidence.assert_awaited_once()
    assert any(
        "announcement_governance_persistence_failed" in item["error"]
        for item in result["errors"]
    )


@pytest.mark.asyncio
async def test_discovery_incomplete_scan_persists_diagnostics_and_is_partial():
    manager = _manager(
        announcement_service=_FakeAnnouncementService(
            status="degraded",
            is_complete=False,
            stop_reason="max_pages_reached",
        )
    )

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        instrument_ids=["600108.SH"],
        dry_run=False,
        max_events=10,
    )

    assert result["status"] == "partial"
    state_result = manager._announcement_test_storage.upsert_announcement_scan_state.call_args.kwargs[
        "scan_result"
    ]
    assert state_result.status == "degraded"
    assert state_result.cursor_commit_allowed is False
    assert any(
        "announcement_scan_incomplete" in item["error"]
        for item in result["errors"]
    )


@pytest.mark.asyncio
async def test_discovery_domain_write_failure_closes_governance_run():
    manager = _manager()
    manager.db_ops.save_corporate_action_effective_date_evidence.side_effect = (
        RuntimeError("domain database unavailable")
    )

    with pytest.raises(RuntimeError, match="domain database unavailable"):
        await manager.discover_cninfo_special_action_effective_dates(
            start_date="1990-12-19",
            end_date="2026-07-18",
            exchanges=["SSE"],
            instrument_ids=["600108.SH"],
            dry_run=False,
            max_events=10,
        )

    finish = manager._announcement_test_storage.finish_ingestion_run
    finish.assert_called_once()
    assert finish.call_args.kwargs["status"] == "failed"
    assert "corporate_action_evidence_write_failed" in finish.call_args.kwargs[
        "error_message"
    ]


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


@pytest.mark.asyncio
async def test_exact_event_key_can_resolve_a_late_observation_outside_range():
    manager = _manager()

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="2026-07-07",
        end_date="2026-07-21",
        exchanges=["SSE"],
        source_event_keys=["event-1"],
        dry_run=True,
        max_events=1,
    )

    assert result["targets"]["searchable_events"] == 1
    assert result["targets"]["skipped_outside_range"] == 0


@pytest.mark.asyncio
async def test_discovery_reports_event_key_for_unbounded_anchor():
    row = {
        **_event_row(announcement_date=None, record_date=None),
        "fiscal_period": None,
        "share_arrival_date": None,
    }
    manager = _manager(event_rows=[row], adjacent_rows=[])

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=True,
    )

    assert result["targets"]["skipped_without_bounded_anchor"] == 1
    assert result["skipped_samples"] == [{
        "instrument_id": "600108.SH",
        "source_event_key": "event-1",
        "reason": "unbounded_anchor",
    }]


@pytest.mark.asyncio
async def test_llm_title_discovery_accepts_compensation_share_without_keywords():
    manager = _manager(
        announcement_service=_FakeAnnouncementService(
            title="重大资产重组业绩承诺补偿股份赠与实施完成公告"
        )
    )
    client = SimpleNamespace(
        complete=AsyncMock(return_value=_title_llm_response())
    )

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=True,
        classify_titles_with_llm=True,
        title_llm_client=client,
    )

    assert result["status"] == "dry_run"
    assert result["evidence"]["candidate_count"] == 1
    assert result["title_classification"]["status"] == "success"
    assert result["target_samples"][0]["classification_samples"][0][
        "announcement_role"
    ] == "compensation_share_distribution"
    manager.db_ops.save_corporate_action_effective_date_evidence.assert_not_awaited()


@pytest.mark.asyncio
async def test_llm_title_discovery_persists_unrelated_as_rejected_evidence():
    manager = _manager(
        announcement_service=_FakeAnnouncementService(title="年度报告摘要")
    )
    client = SimpleNamespace(
        complete=AsyncMock(
            return_value=_title_llm_response(relevance="unrelated")
        )
    )

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=False,
        classify_titles_with_llm=True,
        title_llm_client=client,
    )

    assert result["status"] == "success"
    assert result["evidence"]["candidate_count"] == 0
    assert result["evidence"]["rejected_count"] == 1
    rows = manager.db_ops.save_corporate_action_effective_date_evidence.await_args.args[0]
    assert rows[0]["resolution_status"] == "rejected"
    assert rows[0]["raw_payload"]["title_classification"]["relevance"] == (
        "unrelated"
    )
    manager._announcement_test_storage.store_announcement_audit.assert_not_called()


@pytest.mark.asyncio
async def test_llm_title_coverage_error_is_retryable_and_writes_no_evidence():
    manager = _manager()
    client = SimpleNamespace(
        complete=AsyncMock(
            return_value=_title_llm_response(announcement_id="invented")
        )
    )

    result = await manager.discover_cninfo_special_action_effective_dates(
        start_date="1990-12-19",
        end_date="2026-07-18",
        exchanges=["SSE"],
        dry_run=True,
        classify_titles_with_llm=True,
        title_llm_client=client,
    )

    assert result["status"] == "partial"
    assert result["evidence"]["classified_count"] == 0
    assert result["title_classification"]["status"] == "partial"
    assert result["title_classification"]["event_errors"] == 1
    assert "title_classification_failed" in result["errors"][0]["error"]
    manager.db_ops.save_corporate_action_effective_date_evidence.assert_not_awaited()
