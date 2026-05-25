from contextlib import contextmanager
import asyncio

from research.financial_disclosure_incremental_sync import (
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


class _FakeFinancialStatements:
    def __init__(self, *, ready=False):
        self.ready = ready

    def get_local_core_facts(self, *args, **kwargs):
        return {
            "ready": self.ready,
            "missing_fields": [] if self.ready else [{"canonical_fact": "total_assets"}],
        }


class _FakeStorage:
    def __init__(self, *, ready=False):
        self.financial_statements = _FakeFinancialStatements(ready=ready)
        self.states = []

    @contextmanager
    def financial_database_scope(self):
        yield

    def get_cninfo_announcement_scan_state(self, **kwargs):
        return None

    def list_financial_disclosure_event_states(self, **kwargs):
        return []

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
