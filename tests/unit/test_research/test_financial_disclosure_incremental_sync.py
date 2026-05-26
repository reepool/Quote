from contextlib import contextmanager
import asyncio

from research.financial_disclosure_incremental_sync import (
    FinancialDisclosureMaintenanceCandidate,
    FinancialDisclosureIncrementalSyncService,
)
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanResult,
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
    ):
        self.financial_statements = _FakeFinancialStatements(
            ready=ready,
            numeric_rows=numeric_rows,
            missing_fields=missing_fields,
        )
        self.states = []
        self.pending_states = list(pending_states or [])
        self.audit_rows = list(audit_rows or [])

    @contextmanager
    def financial_database_scope(self):
        yield

    def get_cninfo_announcement_scan_state(self, **kwargs):
        return None

    def list_financial_disclosure_event_states(self, **kwargs):
        return list(self.pending_states)

    def list_cninfo_announcement_audit(self, **kwargs):
        ids = set(kwargs.get("instrument_ids") or [])
        return [
            row for row in self.audit_rows
            if not ids or row.get("instrument_id") in ids
        ]

    def start_ingestion_run(self, **kwargs):
        return 1

    def finish_ingestion_run(self, *args, **kwargs):
        self.finished_run = {"args": args, "kwargs": kwargs}

    def upsert_cninfo_announcement_scan_state(self, **kwargs):
        self.scan_state = kwargs

    def store_cninfo_announcement_audit(self, **kwargs):
        self.audit = kwargs

    def upsert_financial_disclosure_event_state(self, **kwargs):
        self.states.append(kwargs)


class _FakeScanner:
    def __init__(self, records):
        self.records = records

    def scan(self, config, *, filters=None):
        selected = []
        for record in self.records:
            reasons = []
            for filter_func in filters or []:
                reasons.extend(filter_func(record))
            if reasons:
                selected.append(
                    CninfoAnnouncementRecord(
                        announcement_id=record.announcement_id,
                        title=record.title,
                        announcement_time=record.announcement_time,
                        market=record.market,
                        column=record.column,
                        symbols=record.symbols,
                        selection_reasons=reasons,
                    )
                )
        return CninfoAnnouncementScanResult(
            config=config,
            records=self.records,
            selected_records=selected,
            pages_scanned=1,
            announcements_seen=len(self.records),
            max_announcement_time="2026-05-06",
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
    record = CninfoAnnouncementRecord(
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
        announcement_scanner=_FakeScanner([record]),
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


def test_incremental_sync_accepts_delayed_report_without_source_retry(tmp_path):
    record = CninfoAnnouncementRecord(
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
        announcement_scanner=_FakeScanner([record]),
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
    record = CninfoAnnouncementRecord(
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
        announcement_scanner=_FakeScanner([record]),
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
    record = CninfoAnnouncementRecord(
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
        announcement_scanner=_FakeScanner([record]),
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
        CninfoAnnouncementRecord(
            announcement_id="ann-1",
            title="关于无法按期披露2025年年度报告暨股票停牌的公告",
            announcement_time="2026-05-06",
            market="SZSE",
            column="szse",
            symbols=["002731"],
        ),
        CninfoAnnouncementRecord(
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
        announcement_scanner=_FakeScanner(records),
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
        CninfoAnnouncementRecord(
            announcement_id="ann-noisy",
            title="2025年年度报告业绩说明会预告公告",
            announcement_time="2026-05-06",
            market="SZSE",
            column="szse",
            symbols=["002731"],
        ),
        CninfoAnnouncementRecord(
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
        announcement_scanner=_FakeScanner(records),
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
        announcement_scanner=_FakeScanner([]),
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
        announcement_scanner=_FakeScanner([]),
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
        announcement_scanner=_FakeScanner([]),
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
        announcement_scanner=_FakeScanner([]),
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
        announcement_scanner=_FakeScanner([]),
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
        announcement_scanner=_FakeScanner([]),
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
        announcement_scanner=_FakeScanner([]),
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


def test_reconciliation_accepts_recent_generic_risk_audit_without_source_retry(tmp_path):
    storage = _FakeStorage(
        ready=False,
        audit_rows=[
            {
                "instrument_id": "002731.SZ",
                "symbol": "002731",
                "market": "SZSE",
                "announcement_id": "risk-generic",
                "announcement_time": "2026-05-05T16:00:00+00:00",
                "title": "关于无法在法定期限内披露定期报告暨股票停牌的公告",
                "selection_reasons": ["pending_delisting_risk"],
            }
        ],
    )
    service = FinancialDisclosureIncrementalSyncService(
        db_ops=_FakeDbOps(),
        storage=storage,
        research_config=_research_config(tmp_path),
        announcement_scanner=_FakeScanner([]),
    )

    async def _unexpected_import(**kwargs):
        raise AssertionError("recent disclosure risk audits must not call source repair")

    service._run_targeted_import = _unexpected_import

    result = _run(
        service.sync(
            exchanges=["SZSE"],
            target_instrument_ids=["002731.SZ"],
            report_periods=["2026-03-31"],
            max_candidates=5,
            dry_run=False,
            reconciliation=True,
        )
    )

    assert result["status"] == "success"
    assert result["pending_delisting_risk_count"] == 1
    assert result["accepted_gap_count"] == 1
    assert result["blocking_gap_count"] == 0
    assert result["source_routing"]["cninfo_attempts"] == 0
    assert result["source_routing"]["fallback_attempts"] == 0
    assert storage.states[-1]["status"] == "pending_delisting_risk"
    assert storage.states[-1]["announcement_id"] == "risk-generic"


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
