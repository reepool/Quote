import sqlite3
from types import SimpleNamespace

from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementScanResult,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_production_operations import (
    BusinessProfileAnnouncementFrontierRepository,
    BusinessProfileIndexDiscoveryService,
    build_business_profile_reconciliation_report,
    discover_business_profile_shared_annual_reports,
)
from research.business_profile_semantic_runtime import (
    discover_business_profile_semantic_scope,
)
from tests.unit.test_research.test_business_profile_exposure_components import _storage


def _quotes(storage, rows):
    with sqlite3.connect(storage.quotes_db_path) as conn:
        conn.execute(
            "CREATE TABLE instruments ("
            "instrument_id TEXT PRIMARY KEY, symbol TEXT, name TEXT, exchange TEXT, "
            "type TEXT, listed_date TEXT, delisted_date TEXT, status TEXT, is_active INTEGER)"
        )
        conn.executemany(
            "INSERT INTO instruments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def _announcement(announcement_id, title, *, published_at):
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=announcement_id,
        announcement_key=f"cninfo:{announcement_id}",
        title=title,
        published_at=published_at,
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url=f"/finalpage/{announcement_id}.PDF",
                resolved_url=f"https://static.cninfo.com.cn/{announcement_id}.PDF",
                file_extension="PDF",
            ),
        ),
    )


def test_frontier_persists_source_qualified_annual_report_without_pdf(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="annual-2025",
        announcement_key="cninfo:annual-2025",
        title="浦发银行2025年年度报告",
        published_at="2026-03-30T08:00:00+08:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="/finalpage/2026-03-30/annual-2025.PDF",
                resolved_url="https://static.cninfo.com.cn/annual-2025.PDF",
                file_extension="PDF",
            ),
        ),
        selection_reasons=("business_profile_document:annual_report",),
    )

    status = BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
        },
        record=record,
    )

    assert status == "pending"
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT announcement_id, report_period, document_type, status "
            "FROM business_profile_announcement_frontier"
        ).fetchone()
    assert dict(row) == {
        "announcement_id": "annual-2025",
        "report_period": "2025-12-31",
        "document_type": "annual_report",
        "status": "pending",
    }
    assert not list(tmp_path.rglob("*.pdf"))


def test_shared_only_business_profile_discovery_uses_effective_assets_zero_provider(
    tmp_path,
):
    storage = _storage(tmp_path)

    class _SharedAccess:
        def __init__(self):
            self.calls = []

        def list_effective_assets(self, **kwargs):
            limit = kwargs["limit"]
            offset = kwargs["offset"]
            assert kwargs["document_family"] == "annual_report"
            assert kwargs["knowledge_cutoff"] == "2026-04-30"
            assert kwargs["availability"] == "local_valid"
            self.calls.append((limit, offset))
            if offset:
                return {"items": []}
            return {
                "items": [
                    {
                        "asset_id": "asset-2025",
                        "instrument_id": "600000.SH",
                        "fiscal_year": 2025,
                        "report_period": "2025-12-31",
                        "document_family": "annual_report",
                        "availability": "local_valid",
                        "source": "cninfo",
                        "source_announcement_id": "annual-2025",
                        "attachment_id": "attachment-2025",
                        "observation_version": "observation-2025",
                        "content_hash": "a" * 64,
                        "published_at": "2026-03-30T08:00:00+08:00",
                        "is_correction": False,
                    },
                ]
            }

    shared = _SharedAccess()
    result = discover_business_profile_shared_annual_reports(
        storage=storage,
        shared_asset_access=shared,
        knowledge_cutoff="2026-04-30",
        page_size=100,
        max_pages=2,
    )

    assert result["status"] == "success"
    assert result["provider_requests"] == 0
    assert result["attachment_downloads"] == 0
    assert result["selected_announcements"] == 1
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT announcement_id, source_url, document_type, metadata_json "
            "FROM business_profile_announcement_frontier"
        ).fetchone()
    assert row["announcement_id"] == "annual-2025"
    assert row["source_url"] == "shared-asset://asset-2025"
    assert row["document_type"] == "annual_report"
    assert not list(tmp_path.rglob("*.pdf"))


def test_shared_discovery_filters_each_explicit_instrument_without_market_paging(
    tmp_path,
):
    storage = _storage(tmp_path)

    class _SharedAccess:
        def __init__(self):
            self.calls = []

        def list_effective_assets(self, **kwargs):
            self.calls.append(dict(kwargs))
            instrument_id = kwargs["instrument_id"]
            return {
                "items": [
                    {
                        "asset_id": f"asset-{instrument_id}",
                        "instrument_id": instrument_id,
                        "fiscal_year": 2025,
                        "report_period": "2025-12-31",
                        "document_family": "annual_report",
                        "availability": "local_valid",
                        "source": "cninfo",
                        "source_announcement_id": f"annual-{instrument_id}",
                        "attachment_id": f"attachment-{instrument_id}",
                        "observation_version": f"observation-{instrument_id}",
                        "content_hash": "a" * 64,
                        "published_at": "2026-03-30T08:00:00+08:00",
                        "is_correction": False,
                    }
                ]
            }

    shared = _SharedAccess()
    result = discover_business_profile_shared_annual_reports(
        storage=storage,
        shared_asset_access=shared,
        knowledge_cutoff="2026-08-20",
        instrument_ids=("601088.SH", "000001.SZ", "601088.SH"),
        page_size=100,
        max_pages=100,
    )

    assert result["status"] == "success"
    assert result["filter_mode"] == "targeted_shared_effective_assets"
    assert result["pages_scanned"] == 2
    assert result["selected_announcements"] == 2
    assert [call["instrument_id"] for call in shared.calls] == [
        "000001.SZ",
        "601088.SH",
    ]
    assert all(call["offset"] == 0 for call in shared.calls)
    assert all(call["limit"] == 100 for call in shared.calls)


def test_targeted_shared_discovery_paginates_within_the_requested_instrument(
    tmp_path,
):
    storage = _storage(tmp_path)
    calls = []

    class _SharedAccess:
        def list_effective_assets(self, **kwargs):
            calls.append(dict(kwargs))
            if kwargs["offset"]:
                return {"items": []}
            return {
                "items": [
                    {
                        "asset_id": "asset-601088-2025",
                        "instrument_id": "601088.SH",
                        "fiscal_year": 2025,
                        "report_period": "2025-12-31",
                        "document_family": "annual_report",
                        "availability": "local_valid",
                        "source": "cninfo",
                        "source_announcement_id": "annual-601088-2025",
                        "attachment_id": "attachment-601088-2025",
                        "observation_version": "observation-601088-2025",
                        "content_hash": "b" * 64,
                        "published_at": "2026-03-30T08:00:00+08:00",
                        "is_correction": False,
                    }
                ]
            }

    result = discover_business_profile_shared_annual_reports(
        storage=storage,
        shared_asset_access=_SharedAccess(),
        knowledge_cutoff="2026-08-20",
        instrument_ids=("601088.SH",),
        page_size=1,
        max_pages=2,
    )

    assert result["status"] == "success"
    assert [call["instrument_id"] for call in calls] == [
        "601088.SH",
        "601088.SH",
    ]
    assert [call["offset"] for call in calls] == [0, 1]


def test_scope_rotation_covers_active_issuers_without_manifests(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "甲",
                "SSE",
                "stock",
                "2000-01-01",
                None,
                "active",
                1,
            ),
            (
                "000001.SZ",
                "000001",
                "乙",
                "SZSE",
                "stock",
                "2000-01-01",
                None,
                "active",
                1,
            ),
            (
                "920001.BJ",
                "920001",
                "丙",
                "BSE",
                "stock",
                "2021-01-01",
                None,
                "active",
                1,
            ),
        ],
    )
    repository = BusinessProfileRepository(storage)
    kwargs = {
        "knowledge_cutoff": "2026-08-04",
        "max_instruments": 2,
        "field_families": ("derived_value_chain_roles",),
        "runtime_identities": {"rules": "rules.v1"},
        "source_asset_loader": lambda _instrument_id: (),
    }

    first = discover_business_profile_semantic_scope(repository, **kwargs)
    second = discover_business_profile_semantic_scope(repository, **kwargs)

    assert len(first) == 2
    assert len(second) == 2
    assert set(first) | set(second) == {"600000.SH", "000001.SZ", "920001.BJ"}


def test_historical_discovery_end_date_drives_default_start_date(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )

    class _Config:
        @staticmethod
        def route_for(purpose_key, exchange):
            assert purpose_key == "business_profile_evidence:index"
            assert exchange == "SSE"
            return SimpleNamespace(sources=())

    class _AnnouncementService:
        config = _Config()
        query = None

        def acquire(self, query, *, selectors, provider_cursors):
            self.query = query
            assert selectors
            assert provider_cursors == {}
            return SimpleNamespace(
                scan_result=SimpleNamespace(
                    pages_scanned=0,
                    announcements_seen=0,
                    selected_records=(),
                    errors=(),
                    source="fake",
                    status="success",
                ),
                attempts=(),
            )

    announcement_service = _AnnouncementService()
    report = BusinessProfileIndexDiscoveryService(
        storage=storage,
        announcement_service=announcement_service,
    ).discover(
        exchanges=("SSE",),
        end_date="2024-04-30",
        lookback_days=10,
        dry_run=True,
    )

    assert report["start_date"] == "2024-04-20"
    assert report["end_date"] == "2024-04-30"
    assert announcement_service.query.scope.start_date == "2024-04-20"
    assert announcement_service.query.scope.end_date == "2024-04-30"
    assert announcement_service.query.scope.category == "annual_report"


def test_page_bound_discovery_splits_and_persists_date_windows(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )

    class _Config:
        @staticmethod
        def route_for(_purpose_key, _exchange):
            return SimpleNamespace(sources=())

    class _AnnouncementService:
        config = _Config()

        @staticmethod
        def acquire(query, *, selectors, provider_cursors):
            assert selectors
            assert provider_cursors == {}
            scan = AnnouncementScanResult(
                source="fake",
                query=query,
                status="degraded",
                pages_scanned=query.scope.max_pages,
                is_complete=False,
                stop_reason="max_pages_reached",
            )
            return SimpleNamespace(scan_result=scan, attempts=())

    report = BusinessProfileIndexDiscoveryService(
        storage=storage,
        announcement_service=_AnnouncementService(),
    ).discover(
        exchanges=("SSE",),
        start_date="2026-04-01",
        end_date="2026-04-30",
        max_pages_per_market=2,
        resumable_windows=True,
        max_windows_per_market=1,
    )

    assert report["status"] == "degraded"
    assert report["discovery_window_backlog"] == 2
    assert report["incomplete_windows"] == [
        {
            "exchange": "SSE",
            "start_date": "2026-04-01",
            "end_date": "2026-04-30",
            "stop_reason": "max_pages_reached",
            "splittable": True,
        }
    ]
    state = BusinessProfileAnnouncementFrontierRepository(storage).get_state(
        "business_profile_discovery_windows:SSE"
    )
    assert {
        (item["start_date"], item["end_date"]) for item in state["pending_windows"]
    } == {("2026-04-01", "2026-04-15"), ("2026-04-16", "2026-04-30")}


def test_dense_window_preflight_splits_after_first_page(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )

    class _Config:
        @staticmethod
        def route_for(_purpose_key, _exchange):
            return SimpleNamespace(sources=())

    class _AnnouncementService:
        config = _Config()
        query = None

        def acquire(self, query, *, selectors, provider_cursors):
            self.query = query
            assert selectors
            assert provider_cursors == {}
            assert query.scope.preflight_page_bound is True
            scan = AnnouncementScanResult(
                source="fake",
                query=query,
                status="degraded",
                pages_scanned=1,
                is_complete=False,
                stop_reason="estimated_pages_exceed_bound",
                diagnostics={
                    "total_pages": 500,
                    "start_page": 1,
                    "last_page_scanned": 1,
                    "next_page": 2,
                    "preflight_page_bound": True,
                },
            )
            return SimpleNamespace(scan_result=scan, attempts=())

    announcement_service = _AnnouncementService()
    report = BusinessProfileIndexDiscoveryService(
        storage=storage,
        announcement_service=announcement_service,
    ).discover(
        exchanges=("SSE",),
        start_date="2026-04-01",
        end_date="2026-04-30",
        max_pages_per_market=240,
        resumable_windows=True,
        max_windows_per_market=1,
    )

    assert report["pages_scanned"] == 1
    assert report["preflight_splits"] == 1
    assert report["incomplete_windows"][0]["total_pages"] == 500
    state = BusinessProfileAnnouncementFrontierRepository(storage).get_state(
        "business_profile_discovery_windows:SSE"
    )
    assert {
        (item["start_date"], item["end_date"]) for item in state["pending_windows"]
    } == {("2026-04-01", "2026-04-15"), ("2026-04-16", "2026-04-30")}
    assert all("next_page" not in item for item in state["pending_windows"])


def test_closed_single_day_checkpoint_resumes_and_completes(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    frontier.set_state(
        "business_profile_discovery_windows:SSE",
        {
            "pending_windows": [
                {
                    "start_date": "2026-04-30",
                    "end_date": "2026-04-30",
                    "kind": "unsplittable",
                }
            ]
        },
    )

    class _Config:
        @staticmethod
        def route_for(_purpose_key, _exchange):
            return SimpleNamespace(sources=())

    class _AnnouncementService:
        config = _Config()

        def __init__(self):
            self.queries = []

        def acquire(self, query, *, selectors, provider_cursors):
            self.queries.append(query)
            assert selectors
            if query.scope.start_date != query.scope.end_date:
                scan = AnnouncementScanResult(
                    source="fake",
                    query=query,
                    status="success_empty",
                    pages_scanned=1,
                    is_complete=True,
                    stop_reason="empty_page",
                )
            elif query.scope.start_page == 1:
                scan = AnnouncementScanResult(
                    source="fake",
                    query=query,
                    status="degraded",
                    pages_scanned=2,
                    is_complete=False,
                    stop_reason="max_pages_exhausted",
                    diagnostics={
                        "total_pages": 3,
                        "start_page": 1,
                        "last_page_scanned": 2,
                        "next_page": 3,
                    },
                )
            else:
                scan = AnnouncementScanResult(
                    source="fake",
                    query=query,
                    status="success",
                    pages_scanned=1,
                    is_complete=True,
                    stop_reason="reported_last_page",
                    diagnostics={
                        "total_pages": 3,
                        "start_page": 3,
                        "last_page_scanned": 3,
                        "next_page": None,
                    },
                )
            return SimpleNamespace(scan_result=scan, attempts=())

    announcement_service = _AnnouncementService()
    discovery = BusinessProfileIndexDiscoveryService(
        storage=storage,
        announcement_service=announcement_service,
    )
    kwargs = {
        "exchanges": ("SSE",),
        "start_date": "2026-01-01",
        "end_date": "2026-08-07",
        "max_pages_per_market": 2,
        "resumable_windows": True,
        "max_windows_per_market": 2,
    }

    first = discovery.discover(**kwargs)
    first_state = frontier.get_state("business_profile_discovery_windows:SSE")
    assert first["incomplete_windows"][0]["page_checkpointed"] is True
    assert first_state["pending_windows"] == [
        {
            "end_date": "2026-04-30",
            "kind": "unsplittable",
            "next_page": 3,
            "start_date": "2026-04-30",
        }
    ]

    second = discovery.discover(**kwargs)
    second_state = frontier.get_state("business_profile_discovery_windows:SSE")
    assert second["page_resumes"] == 1
    assert second_state["pending_windows"] == []
    single_day_queries = [
        query
        for query in announcement_service.queries
        if query.scope.start_date == query.scope.end_date == "2026-04-30"
    ]
    assert [query.scope.start_page for query in single_day_queries] == [1, 3]
    assert all(
        query.scope.preflight_page_bound is False for query in single_day_queries
    )


def test_current_day_window_ignores_persisted_page_offset(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    frontier.set_state(
        "business_profile_discovery_windows:SSE",
        {
            "pending_windows": [
                {
                    "start_date": "2026-08-07",
                    "end_date": "2026-08-07",
                    "kind": "unsplittable",
                    "next_page": 241,
                }
            ]
        },
    )

    class _Config:
        @staticmethod
        def route_for(_purpose_key, _exchange):
            return SimpleNamespace(sources=())

    class _AnnouncementService:
        config = _Config()
        query = None

        def acquire(self, query, *, selectors, provider_cursors):
            self.query = query
            scan = AnnouncementScanResult(
                source="fake",
                query=query,
                status="success_empty",
                pages_scanned=1,
                is_complete=True,
                stop_reason="empty_page",
            )
            return SimpleNamespace(scan_result=scan, attempts=())

    announcement_service = _AnnouncementService()
    report = BusinessProfileIndexDiscoveryService(
        storage=storage,
        announcement_service=announcement_service,
    ).discover(
        exchanges=("SSE",),
        start_date="2026-08-07",
        end_date="2026-08-07",
        overlap_days=0,
        resumable_windows=True,
        max_windows_per_market=1,
    )

    assert announcement_service.query.scope.start_page == 1
    assert report["page_resumes"] == 0
    assert (
        frontier.get_state("business_profile_discovery_windows:SSE")["pending_windows"]
        == []
    )


def test_complete_market_scan_runs_bounded_rotating_missing_company_repair(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )

    class _Config:
        @staticmethod
        def route_for(_purpose_key, _exchange):
            return SimpleNamespace(sources=())

    class _AnnouncementService:
        config = _Config()

        def __init__(self):
            self.queries = []

        def acquire(
            self,
            query,
            *,
            selectors,
            provider_cursors=None,
        ):
            assert selectors
            self.queries.append(query)
            scan = AnnouncementScanResult(
                source="fake",
                query=query,
                status="success_empty",
                pages_scanned=1,
                is_complete=True,
                stop_reason="empty_page",
            )
            return SimpleNamespace(scan_result=scan, attempts=())

    announcement_service = _AnnouncementService()
    report = BusinessProfileIndexDiscoveryService(
        storage=storage,
        announcement_service=announcement_service,
    ).discover(
        exchanges=("SSE",),
        start_date="2026-01-01",
        end_date="2026-05-01",
        resumable_windows=True,
        max_windows_per_market=1,
        max_targeted_repairs=1,
        targeted_repair_lookback_years=3,
        targeted_repair_max_pages=4,
    )

    assert report["targeted_repair"]["attempted"] == 1
    assert report["targeted_repair"]["expected_annual_period"] == "2025-12-31"
    assert len(announcement_service.queries) == 2
    market_scope, repair_scope = [item.scope for item in announcement_service.queries]
    assert market_scope.symbol is None
    assert market_scope.category == "annual_report"
    assert repair_scope.symbol == "600000"
    assert repair_scope.category == "annual_report"
    assert repair_scope.start_date == "2023-01-01"
    assert repair_scope.max_pages == 4


def test_reconciliation_does_not_require_prior_year_annual_before_may(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "浦发银行",
                "SSE",
                "stock",
                "1999-11-10",
                None,
                "active",
                1,
            )
        ],
    )
    shared_access = SimpleNamespace(
        list_effective_assets=lambda **_kwargs: {
            "returned": 1,
            "items": [
                {
                    "instrument_id": "600000.SH",
                    "report_period": "2024-12-31",
                    "published_at": "2025-03-30",
                }
            ],
        }
    )

    before_deadline = build_business_profile_reconciliation_report(
        storage,
        shared_asset_access=shared_access,
        frequency="monthly",
        knowledge_cutoff="2026-04-30",
    )
    after_deadline = build_business_profile_reconciliation_report(
        storage,
        shared_asset_access=shared_access,
        frequency="monthly",
        knowledge_cutoff="2026-05-01",
    )

    assert before_deadline["current_annual_period"] == "2024-12-31"
    assert before_deadline["missing_current_annual_count"] == 0
    assert after_deadline["current_annual_period"] == "2025-12-31"
    assert after_deadline["missing_current_annual_count"] == 1


def test_correction_frontier_supersedes_prior_annual_report(tmp_path):
    storage = _storage(tmp_path)
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }
    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-original",
            "浦发银行2025年年度报告",
            published_at="2026-03-30T08:00:00+08:00",
        ),
    )
    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-correction",
            "浦发银行2025年年度报告（更正后）",
            published_at="2026-04-02T08:00:00+08:00",
        ),
    )

    with storage.get_connection() as conn:
        rows = conn.execute(
            "SELECT announcement_id, status, supersedes_frontier_id, frontier_id "
            "FROM business_profile_announcement_frontier ORDER BY published_at"
        ).fetchall()

    assert rows[0]["status"] == "superseded"
    assert rows[1]["status"] == "pending"
    assert rows[1]["supersedes_frontier_id"] == rows[0]["frontier_id"]


def test_correction_frontier_converges_when_source_returns_newest_first(tmp_path):
    storage = _storage(tmp_path)
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }
    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-correction",
            "浦发银行2025年年度报告（修订版）",
            published_at="2026-04-02T08:00:00+08:00",
        ),
    )
    status = frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-original",
            "浦发银行2025年年度报告",
            published_at="2026-03-30T08:00:00+08:00",
        ),
    )

    assert status == "superseded"
    with storage.get_connection() as conn:
        rows = {
            row["announcement_id"]: dict(row)
            for row in conn.execute(
                "SELECT announcement_id, frontier_id, status, "
                "supersedes_frontier_id FROM business_profile_announcement_frontier"
            ).fetchall()
        }
    assert rows["annual-original"]["status"] == "superseded"
    assert rows["annual-correction"]["status"] == "pending"
    assert (
        rows["annual-correction"]["supersedes_frontier_id"]
        == (rows["annual-original"]["frontier_id"])
    )


def test_frontier_marks_only_shared_asset_announcements_processed(tmp_path):
    storage = _storage(tmp_path)
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }
    for announcement_id, title in (
        ("annual-2025", "浦发银行2025年年度报告"),
        ("semiannual-2026", "浦发银行2026年半年度报告"),
    ):
        frontier.upsert_record(
            instrument=instrument,
            record=_announcement(
                announcement_id,
                title,
                published_at="2026-04-02T08:00:00+08:00",
            ),
        )
    shared_access = SimpleNamespace(
        list_effective_assets=lambda **_kwargs: {
            "items": [
                {
                    "instrument_id": "600000.SH",
                    "source": "cninfo",
                    "source_announcement_id": "annual-2025",
                }
            ]
        }
    )

    assert (
        frontier.mark_shared_assets_processed(
            ["600000.SH"], shared_asset_access=shared_access
        )
        == 1
    )
    with storage.get_connection() as conn:
        statuses = {
            row["announcement_id"]: row["status"]
            for row in conn.execute(
                "SELECT announcement_id, status "
                "FROM business_profile_announcement_frontier"
            ).fetchall()
        }
    assert statuses == {"annual-2025": "processed", "semiannual-2026": "pending"}
