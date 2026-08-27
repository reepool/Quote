from contextlib import contextmanager
import asyncio

from research.announcements import (
    AnnouncementRecord,
    AnnouncementRouteAttempt,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
)
from research.financial_disclosure_incremental_sync import (
    FinancialDisclosureMaintenanceCandidate,
    FinancialDisclosureIncrementalSyncService,
)
from research.financial_disclosure_events import financial_disclosure_anomaly_filter
from research.financial_statement_maintenance_repair import (
    FinancialMaintenanceRepairRouter,
    FinancialMaintenanceRepairTarget,
)
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


class _FakeDbOps:
    async def get_instruments_by_exchange(self, exchange):
        return [
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "type": "stock",
                "is_active": True,
            },
            {
                "instrument_id": "688121.SH",
                "symbol": "688121",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            },
        ]


class _FakeLifecycleDbOps:
    async def get_instruments_by_exchange(self, exchange):
        return [
            {
                "instrument_id": "001237.SZ",
                "symbol": "001237",
                "exchange": "SZSE",
                "type": "stock",
                "is_active": True,
                "listed_date": "2026-05-22 00:00:00.000000",
            }
        ]


class _FakeFinancialStatements:
    def __init__(self, *, ready=False, numeric_rows=None, missing_fields=None):
        self.ready = ready
        self.numeric_rows = list(numeric_rows or [])
        self.missing_fields = missing_fields

    def get_local_core_facts(self, *args, **kwargs):
        return {
            "ready": self.ready,
            "missing_fields": []
            if self.ready
            else (
                self.missing_fields
                if self.missing_fields is not None
                else [{"canonical_fact": "total_assets"}]
            ),
            "facts": {},
        }

    def get_numeric_facts(self, *args, **kwargs):
        return list(self.numeric_rows)


class _FakeStorage:
    def __init__(
        self,
        *,
        ready=False,
        numeric_rows=None,
        pending_states=None,
        missing_fields=None,
        audit_rows=None,
        industry_memberships=None,
        company_profiles=None,
        stale_runs=None,
    ):
        self.financial_statements = _FakeFinancialStatements(
            ready=ready,
            numeric_rows=numeric_rows,
            missing_fields=missing_fields,
        )
        self.states = []
        self.deleted_states = []
        self.pending_states = list(pending_states or [])
        self.audit_rows = list(audit_rows or [])
        self.industry_memberships = dict(industry_memberships or {})
        self.company_profiles = dict(company_profiles or {})
        self.stale_runs = list(stale_runs or [])
        self.generic_scan_states = []
        self.generic_audits = []

    @contextmanager
    def financial_database_scope(self):
        yield

    def list_financial_disclosure_event_states(self, **kwargs):
        statuses = set(kwargs.get("statuses") or [])
        rows = [
            row
            for row in self.pending_states
            if not statuses or str(row.get("status") or "pending_recheck") in statuses
        ]
        limit = kwargs.get("limit")
        return rows if limit is None else rows[: int(limit)]

    def get_announcement_scan_state(self, **kwargs):
        return None

    def upsert_announcement_scan_state(self, **kwargs):
        self.generic_scan_states.append(kwargs)

    def store_announcement_audit(self, **kwargs):
        self.generic_audits.append(kwargs)

    def list_announcement_audit(self, **kwargs):
        ids = set(kwargs.get("instrument_ids") or [])
        return [
            row for row in self.audit_rows
            if not ids or row.get("instrument_id") in ids
        ]

    def get_industry_membership(self, instrument_id, **kwargs):
        return self.industry_memberships.get(instrument_id)

    def get_company_profile(self, instrument_id, **kwargs):
        return self.company_profiles.get(instrument_id)

    def start_ingestion_run(self, **kwargs):
        return 1

    def finalize_stale_ingestion_runs(self, **kwargs):
        self.stale_run_query = kwargs
        return list(self.stale_runs)

    def finish_ingestion_run(self, *args, **kwargs):
        self.finished_run = {"args": args, "kwargs": kwargs}

    def upsert_financial_disclosure_event_state(self, **kwargs):
        self.states.append(kwargs)

    def delete_financial_disclosure_event_state(self, **kwargs):
        self.deleted_states.append(kwargs)
        return 1


def _record(
    *,
    announcement_id,
    title,
    announcement_time,
    market,
    symbols,
    **_kwargs,
):
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=announcement_id,
        announcement_key=build_announcement_key("cninfo", announcement_id),
        title=title,
        published_at=announcement_time,
        exchange=market,
        market=market,
        symbols=tuple(symbols),
    )


class _FakeAnnouncementService:
    def __init__(
        self,
        records,
        *,
        status="success",
        is_complete=True,
        stop_reason="completed",
        errors=(),
    ):
        self.records = tuple(records)
        self.status = status
        self.is_complete = is_complete
        self.stop_reason = stop_reason
        self.errors = tuple(errors)
        self.queries = []

    def acquire(self, query, *, selectors=None):
        self.queries.append(query)
        records = tuple(
            record
            for record in self.records
            if str(record.exchange or record.market or "").upper()
            == query.scope.exchange
        )
        if query.scope.category == "periodic_report":
            records = tuple(
                record
                for record in records
                if not financial_disclosure_anomaly_filter(record)
            )
        if query.scope.keyword:
            records = tuple(
                record
                for record in records
                if query.scope.keyword in record.title
            )
        selected = []
        for record in records:
            reasons = []
            for selector in selectors or ():
                reasons.extend(selector(record) or ())
            if reasons:
                selected.append(record.with_selection_reasons(reasons))
        source_query = query.for_source("cninfo")
        scan_result = AnnouncementScanResult(
            source="cninfo",
            query=source_query,
            status=self.status,
            records=records,
            selected_records=tuple(selected),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=len(records),
            max_published_at=max(
                (record.published_at for record in records if record.published_at),
                default=None,
            ),
            provider_cursor=ProviderCursor(kind="published_at", value="2026-05-06"),
            is_complete=self.is_complete,
            stop_reason=self.stop_reason,
            errors=self.errors,
        )
        attempt = AnnouncementRouteAttempt(
            source="cninfo",
            status=self.status,
            record_count=len(records),
            selected_count=len(selected),
            pages_scanned=1,
            stop_reason=self.stop_reason,
            errors=self.errors,
        )
        return AnnouncementRouteResult(
            query=query,
            status=self.status,
            selected_source="cninfo",
            scan_result=scan_result,
            attempts=(attempt,),
        )


def _research_config(tmp_path):
    return ResearchConfig(
        enabled=True,
        modules={
            "financial_statements": {
                "enabled": True,
                "readiness": {"required_core_facts": ["total_assets"]},
            }
        },
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            financials_db_path=str(tmp_path / "financials.db"),
            attach_quotes_db=False,
        ),
        budget=ResearchBudgetConfig(),
    )


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())


def test_incremental_sync_classifies_pending_delisting_risk(tmp_path):
    record = _record(
        announcement_id="ann-1",
        title="关于无法按期披露2025年年度报告暨股票停牌的公告",
        announcement_time="2026-05-06",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=False),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["status"] == "success"
    assert result["candidate_count"] == 1
    assert result["pending_delisting_risk_count"] == 1
    assert result["accepted_gap_count"] == 1
    assert result["blocking_gap_count"] == 0


def test_incremental_sync_uses_common_announcement_service_and_generic_audit(tmp_path):
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="ann-common-risk",
        announcement_key=build_announcement_key("cninfo", "ann-common-risk"),
        title="关于无法按期披露2025年年度报告暨股票停牌的公告",
        published_at="2026-05-06T09:00:00+08:00",
        published_at_raw="2026-05-06",
        exchange="SZSE",
        market="SZSE",
        symbols=("002731",),
        raw_payload={"announcementId": "ann-common-risk"},
    )
    storage = _FakeStorage(
        ready=False,
        stale_runs=[
            {
                "run_id": 1102,
                "job_name": "financial_disclosure_incremental_sync",
                "started_at": "2026-08-13T21:45:11+08:00",
            }
        ],
    )
    announcement_service = _FakeAnnouncementService([record])
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=announcement_service,
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=False,
        )
    )

    assert result["status"] == "success"
    assert result["pending_delisting_risk_count"] == 1
    assert result["selected_announcements"] == 1
    assert result["stale_run_count"] == 1
    assert result["stale_run_samples"][0]["run_id"] == 1102
    assert storage.stale_run_query["job_names"] == (
        "financial_disclosure_incremental_sync",
        "financial_disclosure_reconciliation_sync",
        "financial_statements_shadow_sync",
    )
    assert storage.generic_scan_states[0]["scan_result"].source == "cninfo"
    assert storage.generic_audits[0]["record"].source_announcement_id == "ann-common-risk"
    assert announcement_service.queries[0].purpose_key == service.purpose_key


def test_incremental_sync_uses_periodic_and_narrow_anomaly_scopes(tmp_path):
    records = [
        _record(
            announcement_id="formal",
            title="2026年第一季度报告",
            announcement_time="2026-04-30",
            market="SZSE",
            symbols=["002731"],
        ),
        _record(
            announcement_id="delayed",
            title="关于延期披露2026年半年度报告的公告",
            announcement_time="2026-08-14",
            market="SZSE",
            symbols=["002731"],
        ),
    ]
    announcement_service = _FakeAnnouncementService(records)
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=announcement_service,
    )

    result = service._scan_announcements(
        exchanges=["SZSE"],
        instruments=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
            }
        ],
        lookback_days=14,
        overlap_days=2,
        page_size=30,
        max_pages_per_market=40,
        search_key=None,
        run_id=None,
        dry_run=True,
    )

    assert [
        (query.scope.category, query.scope.keyword)
        for query in announcement_service.queries
    ] == [
        ("periodic_report", None),
        (None, "披露"),
        (None, "定期报告"),
    ]
    assert result["selected_announcements"] == 2
    assert result["event_count"] == 2
    assert result["errors"] == []


def test_incremental_sync_uses_bse_official_periodic_scopes(tmp_path):
    announcement_service = _FakeAnnouncementService([])
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=announcement_service,
    )

    service._scan_announcements(
        exchanges=["BSE"],
        instruments=[],
        lookback_days=14,
        overlap_days=2,
        page_size=30,
        max_pages_per_market=40,
        search_key=None,
        run_id=None,
        dry_run=True,
    )

    assert [
        (query.scope.category, query.scope.keyword)
        for query in announcement_service.queries
    ] == [
        ("periodic_report", None),
        ("periodic_report_anomaly", None),
    ]


def test_incremental_sync_reports_incomplete_announcement_stream_as_degraded(tmp_path):
    record = _record(
        announcement_id="formal-incomplete",
        title="2026年第一季度报告",
        announcement_time="2026-04-30",
        market="SZSE",
        symbols=["002731"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService(
            [record],
            status="degraded",
            is_complete=False,
            stop_reason="max_pages_exhausted",
        ),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["status"] == "degraded"
    assert result["candidate_count"] == 1
    assert any("max_pages_exhausted" in item for item in result["scan_errors"])


def test_incremental_sync_accepts_delayed_report_without_source_retry(tmp_path):
    record = _record(
        announcement_id="ann-delay",
        title="收到《关于公司2025年年度报告预计无法在法定期限内披露的监管工作函》的公告",
        announcement_time="2026-05-06",
        market="SSE",
        column="sse",
        symbols=["688121"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=False),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    result = _run(
        service.sync(
            exchanges=["SSE"],
            latest_report_period="2026Q1",
            dry_run=False,
        )
    )

    assert result["candidate_count"] == 1
    assert result["accepted_gap_count"] == 1
    assert result["pending_recheck_count"] == 0
    assert result["source_routing"]["cninfo_attempts"] == 0
    assert result["source_routing"]["fallback_attempts"] == 0
    assert service.storage.states[0]["status"] == "accepted_disclosure_gap"


def test_incremental_sync_skips_ready_regular_report_candidate(tmp_path):
    record = _record(
        announcement_id="ann-2",
        title="2026年第一季度报告",
        announcement_time="2026-04-30",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["status"] == "success"
    assert result["candidate_count"] == 1
    assert result["unchanged_count"] == 1
    assert result["pending_recheck_count"] == 0
    assert result["blocking_gap_count"] == 0


def test_incremental_sync_records_source_failure(tmp_path):
    record = _record(
        announcement_id="ann-3",
        title="2026年第一季度报告",
        announcement_time="2026-04-30",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    storage = _FakeStorage(ready=False)
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    async def _fail_import(**kwargs):
        raise RuntimeError("source unavailable")

    service._run_targeted_import = _fail_import

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=False,
        )
    )

    assert result["status"] == "degraded"
    assert result["failed_count"] == 1
    assert storage.states[0]["status"] == "failed"


def test_incremental_sync_target_filter_limits_candidates(tmp_path):
    records = [
        _record(
            announcement_id="ann-1",
            title="关于无法按期披露2025年年度报告暨股票停牌的公告",
            announcement_time="2026-05-06",
            market="SZSE",
            column="szse",
            symbols=["002731"],
        ),
        _record(
            announcement_id="ann-2",
            title="关于无法按期披露2025年年度报告暨股票停牌的公告",
            announcement_time="2026-05-06",
            market="SSE",
            column="sse",
            symbols=["688121"],
        ),
    ]
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=False),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService(records),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE", "SSE"],
            latest_report_period="2026Q1",
            target_instrument_ids=["002731.SZ"],
            dry_run=True,
        )
    )

    assert result["candidate_count"] == 1
    assert result["outcomes"][0]["instrument_id"] == "002731.SZ"
    assert result["target_instrument_ids"] == ["002731.SZ"]


def test_incremental_sync_reports_filtered_financial_like_announcements(tmp_path):
    records = [
        _record(
            announcement_id="ann-noisy",
            title="2025年年度报告业绩说明会预告公告",
            announcement_time="2026-05-06",
            market="SZSE",
            column="szse",
            symbols=["002731"],
        ),
        _record(
            announcement_id="ann-formal",
            title="2026年第一季度报告",
            announcement_time="2026-05-06",
            market="SZSE",
            column="szse",
            symbols=["002731"],
        ),
    ]
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService(records),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["financial_like_announcements"] == 2
    assert result["filtered_financial_like_announcements"] == 1
    assert result["selected_without_event_count"] == 0
    assert result["candidate_count"] == 1


def test_incremental_sync_skips_stale_pending_noise_from_old_filter(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": "2025-12-31",
                "announcement_id": "old-noise",
                "announcement_time": "2026-05-06",
                "title": "2025年年度报告业绩说明会预告公告",
                "classification": "periodic_report_available",
                "selection_reasons": ["periodic_report"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["candidate_count"] == 0
    assert result["candidate_sources"]["pending_state"] == 0
    assert result["candidate_sources"]["filtered_stale_pending"] == 1


def test_incremental_sync_marks_stale_pending_noise_when_not_dry_run(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": "2025-12-31",
                "announcement_id": "old-noise",
                "announcement_time": "2026-05-06",
                "title": "2025年年度报告（英文版）",
                "classification": "periodic_report_available",
                "selection_reasons": ["periodic_report"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=False,
        )
    )

    assert result["candidate_count"] == 0
    assert storage.states[0]["status"] == "filtered_stale_noise"
    assert (
        "filtered_by_current_announcement_rules"
        in storage.states[0]["selection_reasons"]
    )


def test_incremental_sync_excludes_accepted_state_from_daily_candidates(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": "2025-12-31",
                "announcement_id": "accepted-delay",
                "status": "accepted_disclosure_gap",
                "classification": "periodic_report_delayed_or_suspended",
                "selection_reasons": ["periodic_report_delayed"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["candidate_count"] == 0
    assert result["accepted_gap_count"] == 0
    assert result["candidate_sources"]["new_event"] == 0


def test_incremental_sync_expires_pending_state_without_retry(tmp_path):
    first_pending = "2026-07-15T21:47:14+08:00"
    pending_until = "2026-07-22T21:47:14+08:00"
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": "2026-06-30",
                "announcement_id": "expired-formal-report",
                "announcement_time": "2026-07-15T16:00:00+08:00",
                "title": "2026年半年度报告",
                "status": "pending_recheck",
                "classification": "periodic_report_available",
                "selection_reasons": ["periodic_report"],
                "missing_fields": [{"canonical_fact": "total_assets"}],
                "first_pending_at": first_pending,
                "pending_recheck_until": pending_until,
                "metadata": {"event_count": 1},
            }
        ],
    )
    rescanned_record = _record(
        announcement_id="expired-formal-report",
        title="2026年半年度报告",
        announcement_time="2026-07-15T16:00:00+08:00",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([rescanned_record]),
    )

    async def _unexpected_import(**kwargs):
        raise AssertionError("expired pending state must not call source repair")

    service._run_targeted_import = _unexpected_import

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=False,
        )
    )

    assert result["candidate_count"] == 0
    assert result["expired_pending_count"] == 1
    assert result["candidate_sources"]["expired_pending"] == 1
    assert storage.states[-1]["status"] == "pending_recheck_expired"
    assert storage.states[-1]["first_pending_at"] == first_pending
    assert storage.states[-1]["pending_recheck_until"] == pending_until
    assert storage.states[-1]["metadata"]["terminal_reason"] == (
        "pending_recheck_horizon_expired"
    )


def test_incremental_sync_keeps_active_pending_state(tmp_path):
    storage = _FakeStorage(
        ready=True,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": "2026-06-30",
                "announcement_id": "active-formal-report",
                "announcement_time": "2026-08-03T16:00:00+08:00",
                "title": "2026年半年度报告",
                "status": "pending_recheck",
                "classification": "periodic_report_available",
                "selection_reasons": ["periodic_report"],
                "pending_recheck_until": "2999-08-10T21:45:00+08:00",
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["candidate_count"] == 1
    assert result["expired_pending_count"] == 0
    assert result["candidate_sources"]["pending_state"] == 1


def test_incremental_candidate_limit_is_not_consumed_by_accepted_states(tmp_path):
    accepted_states = [
        {
            "instrument_id": "002731.SZ",
            "symbol": "002731",
            "exchange": "SZSE",
            "report_period": f"202{i}-12-31",
            "announcement_id": f"accepted-{i}",
            "status": "accepted_disclosure_gap",
            "classification": "pre_listing_period",
            "selection_reasons": ["lifecycle:pre_listing_period"],
        }
        for i in range(1, 6)
    ]
    record = _record(
        announcement_id="new-q2-report",
        title="2026年半年度报告",
        announcement_time="2026-08-03",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True, pending_states=accepted_states),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            max_candidates=1,
            dry_run=True,
        )
    )

    assert result["candidate_count"] == 1
    assert result["candidate_unlimited_count"] == 1
    assert result["outcomes"][0]["report_period"] == "2026-06-30"
    assert result["report_periods"][-1] == "2026-03-31"


def test_incremental_sync_default_candidate_limit_keeps_all_announcements(tmp_path):
    records = [
        _record(
            announcement_id=f"h1-{symbol}",
            title="2026年半年度报告",
            announcement_time="2026-08-27",
            market="SZSE",
            symbols=[symbol],
        )
        for symbol in ("002731", "000006", "000007")
    ]

    class _MultiInstrumentDbOps:
        async def get_instruments_by_exchange(self, exchange):
            return [
                {
                    "instrument_id": f"{symbol}.SZ",
                    "symbol": symbol,
                    "exchange": "SZSE",
                    "type": "stock",
                    "is_active": True,
                }
                for symbol in ("002731", "000006", "000007")
            ]

    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_MultiInstrumentDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService(records),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["candidate_limit"] == 0
    assert result["candidate_count"] == 3
    assert result["candidate_unlimited_count"] == 3
    assert {item["instrument_id"] for item in result["outcomes"]} == {
        "002731.SZ",
        "000006.SZ",
        "000007.SZ",
    }


def test_incremental_sync_succeeds_when_cninfo_fails_but_fallback_writes(tmp_path):
    record = _record(
        announcement_id="fallback-q2-report",
        title="2026年半年度报告",
        announcement_time="2026-08-03",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    storage = _FakeStorage(ready=False)
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    async def _fallback_write(**kwargs):
        storage.financial_statements.ready = True
        return {
            **service.repair_router.default_summary(),
            "cninfo_attempts": 1,
            "cninfo_missing_or_ambiguous": 1,
            "fallback_attempts": 1,
            "fallback_successes": 1,
            "final_source": "fallback",
            "final_source_counts": {"cninfo": 0, "fallback": 1},
            "source_collection_complete": True,
            "errors": ["cninfo_data20:SZSE:2026-06-30:degraded:failed=1/1"],
        }

    service._run_targeted_import = _fallback_write

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=False,
        )
    )

    assert result["changed_count"] == 1
    assert result["failed_count"] == 0
    assert result["source_routing"]["fallback_successes"] == 1
    assert result["source_routing"]["final_source"] == "fallback"
    assert result["status"] == "success"


def test_incremental_sync_remains_degraded_when_fallback_is_incomplete(tmp_path):
    source_routing = {
        "cninfo_attempts": 1,
        "cninfo_successes": 0,
        "fallback_attempts": 1,
        "fallback_successes": 0,
        "errors": ["cninfo_data20:SZSE:2026-06-30:degraded:failed=1/1"],
    }

    assert (
        FinancialDisclosureIncrementalSyncService._derive_status(
            candidate_count=1,
            failed_count=0,
            blocking_count=0,
            mapping_policy_gap_count=0,
            pending_recheck_count=0,
            source_routing=source_routing,
            scan_errors=[],
        )
        == "degraded"
    )


def test_repair_summary_classifies_cninfo_and_mixed_sources(tmp_path):
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=True),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    cninfo_only = service.repair_router.default_summary()
    cninfo_only.update({"cninfo_attempts": 2, "cninfo_successes": 2})
    service.repair_router._finalize_source_summary(cninfo_only)
    assert cninfo_only["final_source"] == "cninfo"
    assert cninfo_only["source_collection_complete"] is True

    mixed = service.repair_router.default_summary()
    mixed.update(
        {
            "cninfo_attempts": 2,
            "cninfo_successes": 1,
            "fallback_attempts": 1,
            "fallback_successes": 1,
        }
    )
    service.repair_router._finalize_source_summary(mixed)
    assert mixed["final_source"] == "mixed"
    assert mixed["source_collection_complete"] is True


def test_incremental_sync_is_degraded_for_unresolved_pending_recheck(tmp_path):
    record = _record(
        announcement_id="pending-q2-report",
        title="2026年半年度报告",
        announcement_time="2026-08-03",
        market="SZSE",
        column="szse",
        symbols=["002731"],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(ready=False),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([record]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["pending_recheck_count"] == 1
    assert result["status"] == "degraded"


def test_incremental_sync_filters_progress_pending_delisting_risk(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": "2026-03-31",
                "announcement_id": "risk-without-period",
                "announcement_time": "2026-04-28",
                "title": "关于定期报告披露进展暨股票交易可能被实施退市风险警示的风险提示公告",
                "classification": "pending_delisting_risk",
                "selection_reasons": ["pending_delisting_risk"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            latest_report_period="2026Q1",
            dry_run=True,
        )
    )

    assert result["candidate_sources"]["filtered_stale_pending"] == 1
    assert result["candidate_sources"]["pending_state"] == 0


def test_readiness_accepts_cninfo_data20_official_fact_for_missing_core(tmp_path):
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(
            ready=False,
            numeric_rows=[
                {
                    "canonical_fact_name": "total_assets",
                    "source": "cninfo",
                    "parser_version": "cninfo_data20_structured_json_facts.v1",
                    "raw_fact": {"source_profile": "cninfo_data20"},
                    "value": 100.0,
                }
            ],
        ),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )
    candidate = service._candidate_for_period(
        {
            "instrument_id": "002731.SZ",
            "symbol": "002731",
            "exchange": "SZSE",
        },
        "2026-03-31",
    )

    readiness = service._readiness_for_candidate(
        candidate,
        required_core_facts=["total_assets"],
        mapping_version="test",
    )

    assert readiness["ready"] is True
    assert readiness["missing_fields"] == []
    assert readiness["facts"]["total_assets"]["raw_fact"]["maintenance_source_routing"][
        "source"
    ] == "cninfo_data20"


def test_targeted_import_uses_cninfo_before_fallback(tmp_path):
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=_FakeStorage(
            ready=False,
            numeric_rows=[
                {
                    "canonical_fact_name": "total_assets",
                    "source": "cninfo",
                    "parser_version": "cninfo_data20_structured_json_facts.v1",
                    "raw_fact": {"source_profile": "cninfo_data20"},
                    "value": 100.0,
                }
            ],
        ),
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )
    candidate = service._candidate_for_period(
        {
            "instrument_id": "002731.SZ",
            "symbol": "002731",
            "exchange": "SZSE",
        },
        "2026-03-31",
    )

    async def _fake_cninfo(**kwargs):
        return {"attempts": 1, "successes": 1, "missing_or_ambiguous": 0, "errors": []}

    service.repair_router._run_cninfo_data20_import = _fake_cninfo

    result = _run(
        service._run_targeted_import(
            candidates=[candidate],
            required_core_facts=["total_assets"],
            mapping_version="test",
            db_path=tmp_path / "financials.db",
            request_interval_seconds=0.0,
            request_timeout_seconds=1.0,
        )
    )

    assert result["source_order"] == ["cninfo_data20", "ths_report", "sina_report"]
    assert result["cninfo_attempts"] == 1
    assert result["cninfo_successes"] == 1
    assert result["fallback_attempts"] == 0


def test_repair_router_uses_fresh_readiness_after_external_write(tmp_path, monkeypatch):
    current_storage = _FakeStorage(ready=False)
    fresh_storage = _FakeStorage(ready=True)
    config = _research_config(tmp_path)
    router = FinancialMaintenanceRepairRouter(
        storage=current_storage,
        research_config=config,
    )
    target = FinancialMaintenanceRepairTarget(
        instrument_id="601187.SH",
        symbol="601187",
        exchange="SSE",
        report_period="2026-03-31",
        profile="bank",
    )

    async def _fake_cninfo(**kwargs):
        return {
            "attempts": 1,
            "batch_successes": 0,
            "failed_instrument_periods": 1,
            "errors": ["cninfo_data20:SSE:2026-03-31:degraded:failed=1/1"],
        }

    router._run_cninfo_data20_import = _fake_cninfo
    monkeypatch.setattr(
        "research.financial_statement_maintenance_repair.ResearchStorageManager",
        lambda research_config: fresh_storage,
    )

    result = _run(
        router.repair_targets(
            targets=[target],
            required_core_facts=["total_assets"],
            mapping_version="test",
            db_path=tmp_path / "financials.db",
            request_interval_seconds=0.0,
            request_timeout_seconds=1.0,
        )
    )

    assert result["cninfo_attempts"] == 1
    assert result["cninfo_successes"] == 1
    assert result["cninfo_missing_or_ambiguous"] == 0
    assert result["fallback_attempts"] == 0


def test_repair_router_keeps_partial_cninfo_and_falls_back_for_missing_fact(
    tmp_path,
    monkeypatch,
):
    storage = _FakeStorage(
        ready=False,
        numeric_rows=[
            {
                "canonical_fact_name": "total_assets",
                "source": "cninfo",
                "parser_version": "cninfo_data20_structured_json_facts.v1",
                "raw_fact": {"source_profile": "cninfo_data20"},
                "value": 100.0,
            }
        ],
        missing_fields=[{"canonical_fact": "equity_parent"}],
    )
    router = FinancialMaintenanceRepairRouter(
        storage=storage,
        research_config=_research_config(tmp_path),
    )
    target = FinancialMaintenanceRepairTarget(
        instrument_id="002731.SZ",
        symbol="002731",
        exchange="SZSE",
        report_period="2026-06-30",
        profile="nonbank",
    )

    async def _fake_cninfo(**kwargs):
        assert kwargs["required_core_facts"] == ["total_assets", "equity_parent"]
        return {
            "attempts": 1,
            "batch_successes": 0,
            "failed_instrument_periods": 1,
            "source_failures": 0,
            "parsed_instrument_periods": 1,
            "partial_instrument_periods": 1,
            "numeric_facts": 24,
            "missing_required_core_facts": ["equity_parent"],
            "errors": [],
        }

    async def _fake_fallback(**kwargs):
        storage.financial_statements.ready = True
        storage.financial_statements.missing_fields = []

    router._run_cninfo_data20_import = _fake_cninfo
    monkeypatch.setattr(
        "research.financial_statement_maintenance_repair.ResearchStorageManager",
        lambda research_config: storage,
    )
    monkeypatch.setattr(
        "scripts.dev_validation.validate_sina_ths_local_core_dryrun.run_local_core_dryrun",
        _fake_fallback,
    )

    result = _run(
        router.repair_targets(
            targets=[target],
            required_core_facts=["total_assets", "equity_parent"],
            mapping_version="test",
            db_path=tmp_path / "financials.db",
            request_interval_seconds=0.0,
            request_timeout_seconds=1.0,
        )
    )

    assert result["cninfo_parsed_instrument_periods"] == 1
    assert result["cninfo_source_failures"] == 0
    assert result["cninfo_missing_required_core_facts"] == ["equity_parent"]
    assert result["fallback_attempts"] == 1
    assert result["fallback_successes"] == 1
    assert result["final_source"] == "mixed"
    assert result["errors"] == []


def test_official_validation_uses_cninfo_numeric_facts_for_readiness():
    from scripts.dev_validation.validate_sse_official_financial_json_live import (
        _validate_instrument_core_facts,
    )

    class _FinancialStatements:
        @staticmethod
        def get_core_facts(*args, **kwargs):
            return [
                {
                    "facts": {
                        "total_assets": 100.0,
                        "net_income_parent": None,
                    }
                }
            ]

        @staticmethod
        def get_numeric_facts(*args, **kwargs):
            return [
                {
                    "canonical_fact_name": "net_income_parent",
                    "fact_value": 12.0,
                    "source": "cninfo",
                    "parser_version": "cninfo_data20_structured_json_facts.v1",
                }
            ]

    storage = type("_Storage", (), {"financial_statements": _FinancialStatements()})()

    result = _validate_instrument_core_facts(
        storage=storage,
        instrument_id="600519.SH",
        report_period="2026-06-30",
        required_core_facts=["total_assets", "net_income_parent", "equity_parent"],
        official_source="cninfo",
        parser_profile="cninfo_data20_structured_json_facts.v1",
    )

    assert result["present_required_core_facts"] == [
        "total_assets",
        "net_income_parent",
    ]
    assert result["missing_required_core_facts"] == ["equity_parent"]
    assert result["official_parsed"] is True


def test_candidate_profile_uses_storage_industry_membership(tmp_path):
    storage = _FakeStorage(
        ready=True,
        industry_memberships={
            "601187.SH": {
                "instrument_id": "601187.SH",
                "taxonomy_system": "sw",
                "sw_l1_name": "银行",
                "sw_l2_name": "城商行Ⅱ",
                "sw_l3_name": "城商行Ⅲ",
            }
        },
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    candidate = service._candidate_for_period(
        {
            "instrument_id": "601187.SH",
            "symbol": "601187",
            "exchange": "SSE",
        },
        "2026-03-31",
    )

    assert candidate.profile == "bank"


def test_apply_candidates_uses_fresh_readiness_for_final_status(tmp_path, monkeypatch):
    current_storage = _FakeStorage(ready=False)
    fresh_storage = _FakeStorage(ready=True)
    config = _research_config(tmp_path)
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=current_storage,
        research_config=config,
        announcement_service=_FakeAnnouncementService([]),
    )
    candidate = FinancialDisclosureMaintenanceCandidate(
        instrument_id="601187.SH",
        symbol="601187",
        exchange="SSE",
        report_period="2026-03-31",
        profile="bank",
        reasons=["missing_or_incomplete_local_core"],
    )

    async def _fake_import(**kwargs):
        return service.repair_router.default_summary()

    service._run_targeted_import = _fake_import
    monkeypatch.setattr(
        "research.financial_disclosure_incremental_sync.ResearchStorageManager",
        lambda research_config: fresh_storage,
    )

    result = _run(
        service._apply_candidates(
            candidates=[candidate],
            required_core_facts=["total_assets"],
            mapping_version="test",
            db_path=tmp_path / "financials.db",
            request_interval_seconds=0.0,
            request_timeout_seconds=1.0,
            pending_recheck_days=5,
            run_id=1,
            dry_run=False,
        )
    )

    assert result["changed_count"] == 1
    assert result["blocking_gap_count"] == 0
    assert result["source_missing_gap_count"] == 0
    assert current_storage.states[-1]["status"] == "changed"


def test_reconciliation_mapping_policy_gap_does_not_retry_sources(tmp_path):
    storage = _FakeStorage(
        ready=False,
        missing_fields=[
            {
                "canonical_fact": "net_income",
                "reason": "outside_approved_local_core",
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    async def _unexpected_import(**kwargs):
        raise AssertionError("mapping policy gaps must not call source repair")

    service._run_targeted_import = _unexpected_import

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            report_periods=["2026-03-31"],
            max_candidates=1,
            dry_run=False,
            reconciliation=True,
        )
    )

    assert result["status"] == "degraded"
    assert result["mapping_policy_gap_count"] == 1
    assert result["source_missing_gap_count"] == 0
    assert result["source_routing"]["cninfo_attempts"] == 0
    assert result["source_routing"]["fallback_attempts"] == 0
    assert storage.states[0]["status"] == "mapping_policy_gap"


def test_reconciliation_accepts_pre_listing_period_without_source_retry(tmp_path):
    storage = _FakeStorage(ready=False)
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeLifecycleDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    async def _unexpected_import(**kwargs):
        raise AssertionError("pre-listing gaps must not call source repair")

    service._run_targeted_import = _unexpected_import

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            report_periods=["2026-03-31"],
            max_candidates=1,
            dry_run=False,
            reconciliation=True,
        )
    )

    assert result["status"] == "success"
    assert result["accepted_gap_count"] == 1
    assert result["blocking_gap_count"] == 0
    assert result["source_routing"]["cninfo_attempts"] == 0
    assert result["source_routing"]["fallback_attempts"] == 0
    assert result["report_period_lifecycle_summary"]["pre_listing"] == 1
    assert storage.states[0]["status"] == "accepted_disclosure_gap"
    assert storage.states[0]["classification"] == "pre_listing_period"


def test_reconciliation_converts_pre_listing_pending_state_without_source_retry(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "001237.SZ",
                "symbol": "001237",
                "exchange": "SZSE",
                "report_period": "2026-03-31",
                "announcement_id": "local-gap:001237.SZ:2026-03-31",
                "status": "pending_recheck",
                "classification": "periodic_report_available",
                "selection_reasons": ["pending_recheck"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeLifecycleDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    async def _unexpected_import(**kwargs):
        raise AssertionError("pre-listing pending states must not call source repair")

    service._run_targeted_import = _unexpected_import

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            target_instrument_ids=["001237.SZ"],
            report_periods=["2026-03-31"],
            max_candidates=5,
            dry_run=False,
            reconciliation=True,
        )
    )

    assert result["status"] == "success"
    assert result["accepted_gap_count"] == 1
    assert result["pending_recheck_count"] == 0
    assert result["source_routing"]["cninfo_attempts"] == 0
    assert result["source_routing"]["fallback_attempts"] == 0
    assert result["report_period_lifecycle_summary"]["pre_listing"] == 1
    assert storage.states[-1]["status"] == "accepted_disclosure_gap"
    assert storage.states[-1]["classification"] == "pre_listing_period"
    assert storage.states[-1]["pending_recheck_until"] is None
    assert storage.states[-1]["metadata"]["event_count"] == 1
    assert storage.states[-1]["metadata"]["lifecycle_classification"] == "pre_listing_period"
    assert "lifecycle:pre_listing_period" in storage.states[-1]["selection_reasons"]


def test_reconciliation_reuses_accepted_disclosure_state_without_source_retry(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "688121.SH",
                "symbol": "688121",
                "exchange": "SSE",
                "report_period": "2025-12-31",
                "announcement_id": "accepted-delay",
                "announcement_time": "2026-05-06",
                "title": "收到《关于公司2025年年度报告预计无法在法定期限内披露的监管工作函》的公告",
                "status": "accepted_disclosure_gap",
                "classification": "periodic_report_delayed_or_suspended",
                "selection_reasons": ["periodic_report_delayed"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    async def _unexpected_import(**kwargs):
        raise AssertionError("accepted disclosure gaps must not call source repair")

    service._run_targeted_import = _unexpected_import

    result = _run(
        service.sync(
            exchanges=["SSE"],
            target_instrument_ids=["688121.SH"],
            report_periods=["2025-12-31"],
            max_candidates=5,
            dry_run=False,
            reconciliation=True,
        )
    )

    assert result["status"] == "success"
    assert result["candidate_count"] == 1
    assert result["accepted_gap_count"] == 1
    assert result["blocking_gap_count"] == 0
    assert result["source_routing"]["cninfo_attempts"] == 0
    assert result["source_routing"]["fallback_attempts"] == 0
    assert storage.states[-1]["status"] == "accepted_disclosure_gap"


def test_reconciliation_ignores_recent_generic_risk_audit_without_report_period(tmp_path):
    storage = _FakeStorage(
        ready=False,
        audit_rows=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "market": "SZSE",
                "source_announcement_id": "risk-generic",
                "published_at": "2026-05-05T16:00:00+00:00",
                "title": "关于无法在法定期限内披露定期报告暨股票停牌的公告",
                "selection_reasons": ["pending_delisting_risk"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = service._load_disclosure_risk_audits_by_instrument(
        ["002731.SZ"]
    )

    assert result == {}


def test_reconciliation_candidate_limit_is_balanced_across_groups():
    candidates = {
        (f"60000{i}.SH", "2026-03-31"): FinancialDisclosureMaintenanceCandidate(
            instrument_id=f"60000{i}.SH",
            symbol=f"60000{i}",
            exchange="SSE",
            report_period="2026-03-31",
            profile="nonbank",
        )
        for i in range(4)
    }
    candidates.update(
        {
            (f"00000{i}.SZ", "2026-03-31"): FinancialDisclosureMaintenanceCandidate(
                instrument_id=f"00000{i}.SZ",
                symbol=f"00000{i}",
                exchange="SZSE",
                report_period="2026-03-31",
                profile="nonbank",
            )
            for i in range(4)
        }
    )

    limited = FinancialDisclosureIncrementalSyncService._limit_candidates_balanced(
        candidates,
        max_candidates=2,
    )

    assert {candidate.exchange for candidate in limited.values()} == {"SSE", "SZSE"}


def test_reconciliation_persisted_states_still_report_balanced_limit(tmp_path):
    storage = _FakeStorage(
        ready=False,
        pending_states=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "exchange": "SZSE",
                "report_period": period,
                "announcement_id": f"accepted-{period}",
                "status": "accepted_disclosure_gap",
                "classification": "periodic_report_delayed_or_suspended",
                "selection_reasons": ["periodic_report_delayed"],
            }
            for period in ("2025-12-31", "2026-03-31")
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_service=_FakeAnnouncementService([]),
    )

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            report_periods=["2026-03-31"],
            max_candidates=1,
            dry_run=True,
            reconciliation=True,
        )
    )

    assert result["candidate_count"] == 1
    assert result["candidate_unlimited_count"] == 2
    assert result["candidate_limit"] == 1
    assert result["candidate_sources"]["accepted_state"] == 1
