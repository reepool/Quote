import hashlib
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
    audit_business_profile_archive,
    build_business_profile_reconciliation_report,
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
        (item["start_date"], item["end_date"])
        for item in state["pending_windows"]
    } == {("2026-04-01", "2026-04-15"), ("2026-04-16", "2026-04-30")}


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
    storage.financial_statements = SimpleNamespace(
        get_source_file_manifests=lambda: [
            {
                "schema_version": "business_profile_source_file_manifest.v1",
                "instrument_id": "600000.SH",
                "report_period": "2024-12-31",
                "report_type": "annual_report",
                "published_at": "2025-03-30",
                "status": "verified",
                "content_hash": "a" * 64,
                "archive_path": "data/filings/business_profile/2024/SSE/report.pdf",
            }
        ]
    )

    before_deadline = build_business_profile_reconciliation_report(
        storage,
        frequency="monthly",
        knowledge_cutoff="2026-04-30",
    )
    after_deadline = build_business_profile_reconciliation_report(
        storage,
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


def test_frontier_marks_only_manifested_announcements_processed(tmp_path):
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
    storage.financial_statements = SimpleNamespace(
        get_source_file_manifests=lambda: [
            {
                "schema_version": "business_profile_source_file_manifest.v1",
                "instrument_id": "600000.SH",
                "source": "cninfo",
                "filing_id": "annual-2025",
                "status": "archived_unchanged_content",
                "content_hash": "a" * 64,
                "archive_path": "data/filings/business_profile/2025/SSE/a.pdf",
            }
        ]
    )

    assert frontier.mark_manifested_processed(["600000.SH"]) == 1
    with storage.get_connection() as conn:
        statuses = {
            row["announcement_id"]: row["status"]
            for row in conn.execute(
                "SELECT announcement_id, status "
                "FROM business_profile_announcement_frontier"
            ).fetchall()
        }
    assert statuses == {"annual-2025": "processed", "semiannual-2026": "pending"}


def test_archive_audit_resolves_relative_paths_and_never_allows_deletion(tmp_path):
    archive_root = tmp_path / "filings"
    archive_root.mkdir()
    pdf = archive_root / "annual.pdf"
    pdf.write_bytes(b"%PDF-1.7\narchive")
    digest = hashlib.sha256(pdf.read_bytes()).hexdigest()
    financials_db = tmp_path / "financials.db"
    with sqlite3.connect(financials_db) as conn:
        conn.execute("CREATE TABLE financial_source_files (source_file_id TEXT)")
    storage = SimpleNamespace(
        financials_db_path=str(financials_db),
        financial_statements=SimpleNamespace(
            get_source_file_manifests=lambda: [
                {
                    "schema_version": "business_profile_source_file_manifest.v1",
                    "source_file_id": "source-1",
                    "archive_path": "annual.pdf",
                    "content_hash": digest,
                    "supersedes_source_file_id": None,
                }
            ]
        ),
    )

    report = audit_business_profile_archive(storage, archive_root=archive_root)

    assert report["automatic_deletion_allowed"] is False
    assert report["classifications"]["active"] == [str(pdf.resolve())]
    assert report["classifications"]["unreferenced"] == []


def test_archive_audit_without_manifest_schema_is_ungoverned(tmp_path):
    archive_root = tmp_path / "filings"
    archive_root.mkdir()
    (archive_root / "annual.pdf").write_bytes(b"%PDF-1.7\narchive")
    storage = SimpleNamespace(financials_db_path=str(tmp_path / "missing.db"))

    report = audit_business_profile_archive(storage, archive_root=archive_root)

    assert report["status"] == "ungoverned_archive"
    assert report["manifest_table_exists"] is False
    assert report["automatic_deletion_allowed"] is False
