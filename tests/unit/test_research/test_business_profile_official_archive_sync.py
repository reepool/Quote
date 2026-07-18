import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

import research.business_profile_official_archive_sync as archive_sync_module
from research.business_profile_archive import BusinessProfileDocumentArchiveService
from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import classify_business_profile_document
from research.business_profile_exchange_discovery import (
    BusinessProfileDiscoveryResolution,
    BusinessProfileSourceAttempt,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_official_archive_sync import (
    OFFICIAL_ARCHIVE_WRITE_SWITCH,
    BusinessProfileOfficialArchiveSyncService,
)
from tests.unit.test_research.test_business_profile_governance import (
    _approved_evidence,
    _storage,
)


def _candidate_segment(
    *,
    instrument_id: str = "601088.SH",
    report_period: str = "2025-12-31",
    evidence_id: str = "evidence-2025-ar",
    revenue_share: float = 0.8,
):
    return {
        "record_id": f"segment-{instrument_id}-{report_period}",
        "instrument_id": instrument_id,
        "report_period": report_period,
        "segment_id": "coal",
        "segment_name_raw": "煤炭",
        "segment_type": "product",
        "revenue": 100.0,
        "revenue_share": revenue_share,
        "evidence_id": evidence_id,
        "data_available_date": "2026-03-28",
        "confidence": 0.95,
        "review_status": "candidate",
        "metadata": {
            "source_name": "eastmoney_main_composition",
            "source_row_key": f"coal-{report_period}",
            "industry_group": "coal",
            "product_resolution": {
                "product_ids": ["coal"],
                "matched_alias_ids": ["coal-exact"],
                "normalized_alias": "煤炭",
            },
        },
    }


def _official_candidate(
    *,
    title: str = "中国神华2025年年度报告",
    announcement_id: str = "ann-2025-ar",
    source: str = "cninfo",
):
    return BusinessProfileDocumentCandidate(
        announcement_id=announcement_id,
        title=title,
        announcement_time="2026-03-28 18:00:00",
        symbols=["601088"],
        adjunct_url="/finalpage/2026-03-28/ann-2025-ar.PDF",
        adjunct_type="PDF",
        classification=classify_business_profile_document(
            title,
            adjunct_type="PDF",
        ),
        selection_reasons=["periodic_report"],
        source=source,
        source_tier=("official_primary" if source == "cninfo" else "official_backup"),
    )


class _FakeCoordinator:
    def __init__(self, candidates, *, errors=None):
        self.candidates = list(candidates)
        self.errors = list(errors or [])
        self.calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        return BusinessProfileDiscoveryResolution(
            status="degraded" if self.errors else "success",
            selected_source="cninfo",
            selected_source_tier="official_primary",
            fallback_used=False,
            fallback_reason=None,
            candidates=list(self.candidates),
            attempts=[
                BusinessProfileSourceAttempt(
                    source="cninfo",
                    source_tier="official_primary",
                    status="degraded" if self.errors else "success",
                    candidate_count=len(self.candidates),
                    pages_scanned=1,
                    announcements_seen=len(self.candidates),
                    errors=list(self.errors),
                )
            ],
        )


class _FailIfArchived:
    def archive_candidates(self, *_args, **_kwargs):
        raise AssertionError("metadata-only sync must not archive documents")


class _PartialBackupCoordinator:
    def __init__(self, primary_candidates, backup_candidates):
        self.primary_candidates = list(primary_candidates)
        self.backup_candidates = list(backup_candidates)
        self.primary_calls = []
        self.backup_calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.primary_calls.append((instrument, kwargs))
        return _resolution(self.primary_candidates, source="cninfo")

    def discover_backup_instrument(self, instrument, **kwargs):
        self.backup_calls.append((instrument, kwargs))
        return _resolution(self.backup_candidates, source="sse")


class _PartialPrimaryRetryCoordinator(_PartialBackupCoordinator):
    def __init__(self, primary_candidates, retry_candidates, backup_candidates):
        super().__init__(primary_candidates, backup_candidates)
        self.retry_candidates = list(retry_candidates)
        self.primary_retry_calls = []

    def discover_primary_instrument(self, instrument, **kwargs):
        self.primary_retry_calls.append((instrument, kwargs))
        return _resolution(self.retry_candidates, source="cninfo")


class _InitialPartialBackupCoordinator:
    def __init__(self, initial_candidates, retry_candidates):
        self.initial_candidates = list(initial_candidates)
        self.retry_candidates = list(retry_candidates)
        self.initial_calls = []
        self.backup_calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.initial_calls.append((instrument, kwargs))
        return _resolution(self.initial_candidates, source="sse")

    def discover_backup_instrument(self, instrument, **kwargs):
        self.backup_calls.append((instrument, kwargs))
        return _resolution(self.retry_candidates, source="sse")


def _resolution(candidates, *, source):
    source_tier = "official_primary" if source == "cninfo" else "official_backup"
    return BusinessProfileDiscoveryResolution(
        status="success",
        selected_source=source,
        selected_source_tier=source_tier,
        fallback_used=source != "cninfo",
        fallback_reason="explicit_backup" if source != "cninfo" else None,
        candidates=list(candidates),
        attempts=[
            BusinessProfileSourceAttempt(
                source=source,
                source_tier=source_tier,
                status="success",
                candidate_count=len(candidates),
                pages_scanned=1,
                announcements_seen=len(candidates),
            )
        ],
    )


def _seed_review_candidate(storage):
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _approved_evidence())
    repository.upsert("segments", _candidate_segment())


def _seed_additional_review_period(storage, report_period):
    repository = BusinessProfileRepository(storage)
    evidence_id = f"evidence-{report_period}"
    evidence = _approved_evidence()
    evidence["evidence_id"] = evidence_id
    evidence["source_document_id"] = f"source-{report_period}"
    evidence["report_period"] = report_period
    repository.upsert("evidence", evidence)
    repository.upsert(
        "segments",
        _candidate_segment(
            report_period=report_period,
            evidence_id=evidence_id,
            revenue_share=0.7,
        ),
    )


def _seed_invalid_review_candidate(storage):
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence("BAD-ID")
    evidence["evidence_id"] = "evidence-bad-id"
    evidence["source_document_id"] = "source-bad-id"
    repository.upsert("evidence", evidence)
    repository.upsert(
        "segments",
        _candidate_segment(
            instrument_id="BAD-ID",
            evidence_id="evidence-bad-id",
            revenue_share=0.9,
        ),
    )


def test_metadata_probe_matches_period_without_writing_candidates_or_archive(
    tmp_path,
):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    coordinator = _FakeCoordinator([_official_candidate()])
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        report_period="2025-12-31",
        max_instruments=1,
        as_of_date="2026-07-18",
    )

    assert report["status"] == "success"
    assert report["mode"] == "metadata_only"
    assert report["matched_instrument_periods"] == 1
    assert report["candidate_rows_written"] == 0
    assert report["archived_documents"] == 0
    assert report["results"][0]["matched_report_periods"] == ["2025-12-31"]
    assert len(coordinator.calls) == 1
    assert coordinator.calls[0][1]["dry_run"] is True
    assert coordinator.calls[0][1]["search_key"] == "年度报告"


def test_default_cutoff_uses_shanghai_calendar_date(tmp_path, monkeypatch):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    coordinator = _FakeCoordinator([_official_candidate()])
    monkeypatch.setattr(
        archive_sync_module,
        "get_shanghai_time",
        lambda: datetime.fromisoformat("2026-07-19T00:30:00+08:00"),
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        max_instruments=1,
    )

    assert report["scope"]["as_of_date"] == "2026-07-19"
    assert coordinator.calls[0][1]["end_date"] == "2026-07-19"


def test_partial_primary_result_queries_backup_for_missing_periods(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    _seed_additional_review_period(storage, "2024-12-31")
    coordinator = _PartialBackupCoordinator(
        [_official_candidate()],
        [
            _official_candidate(
                title="中国神华2024年年度报告",
                announcement_id="sse-2024-ar",
                source="sse",
            )
        ],
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        max_instruments=1,
        as_of_date="2026-07-18",
    )

    assert report["status"] == "success"
    assert report["matched_instrument_periods"] == 2
    assert report["missing_instrument_periods"] == 0
    assert [item["source"] for item in report["results"][0]["attempts"]] == [
        "cninfo",
        "sse",
    ]
    assert len(coordinator.primary_calls) == 1
    assert len(coordinator.backup_calls) == 1
    assert coordinator.backup_calls[0][1]["start_date"] == "2024-12-31"


def test_partial_primary_result_retries_narrow_period_before_backup(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    _seed_additional_review_period(storage, "2021-06-30")
    coordinator = _PartialPrimaryRetryCoordinator(
        [_official_candidate()],
        [
            _official_candidate(
                title="中国神华2021年半年度报告",
                announcement_id="cninfo-2021-semi",
            )
        ],
        [],
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        max_instruments=1,
        as_of_date="2026-07-18",
    )

    assert report["status"] == "success"
    assert report["matched_instrument_periods"] == 2
    assert len(coordinator.primary_retry_calls) == 1
    assert coordinator.primary_retry_calls[0][1]["start_date"] == "2021-06-30"
    assert coordinator.primary_retry_calls[0][1]["end_date"] == "2021-12-31"
    assert coordinator.primary_retry_calls[0][1]["search_key"] == "2021年"
    assert coordinator.backup_calls == []


def test_partial_initial_backup_retries_missing_period_with_narrow_window(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    _seed_additional_review_period(storage, "2024-12-31")
    coordinator = _InitialPartialBackupCoordinator(
        [_official_candidate()],
        [
            _official_candidate(
                title="中国神华2024年年度报告",
                announcement_id="sse-2024-ar",
                source="sse",
            )
        ],
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        max_instruments=1,
        as_of_date="2026-07-18",
    )

    assert report["status"] == "success"
    assert report["matched_instrument_periods"] == 2
    assert report["missing_instrument_periods"] == 0
    assert len(coordinator.initial_calls) == 1
    assert len(coordinator.backup_calls) == 1
    assert coordinator.backup_calls[0][1]["start_date"] == "2024-12-31"
    assert coordinator.backup_calls[0][1]["end_date"] == "2025-06-30"


def test_archive_write_requires_exact_operator_switch(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    coordinator = _FakeCoordinator([_official_candidate()])
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    with pytest.raises(PermissionError, match=OFFICIAL_ARCHIVE_WRITE_SWITCH):
        service.sync(
            target_research_db=research_db,
            archive_write=True,
            operator_switch="wrong",
        )

    assert coordinator.calls == []


def test_explicit_archive_write_creates_manifest_and_parent_child_runs(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    coordinator = _FakeCoordinator([_official_candidate()])
    archive_root = tmp_path / "official_reports"
    archive_service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=archive_root,
        downloader=lambda _candidate: b"%PDF-1.7 official report fixture",
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=archive_service,
    )
    checkpoint_root = tmp_path / "checkpoints"

    report = service.sync(
        target_research_db=research_db,
        report_period="2025-12-31",
        max_instruments=1,
        as_of_date="2026-07-18",
        archive_write=True,
        operator_switch=OFFICIAL_ARCHIVE_WRITE_SWITCH,
        checkpoint_root=checkpoint_root,
    )

    assert report["status"] == "success"
    assert report["archived_documents"] == 1
    assert report["candidate_rows_written"] == 0
    assert report["parent_ingestion_run_id"] is not None
    assert not (checkpoint_root / "601088.SH.json").exists()
    with sqlite3.connect(storage.financials_db_path) as conn:
        manifest = conn.execute(
            """
            SELECT instrument_id, report_period, report_type, status, archive_path
            FROM financial_source_files
            """
        ).fetchone()
    assert manifest[:4] == (
        "601088.SH",
        "2025-12-31",
        "annual_report",
        "archived",
    )
    assert Path(manifest[4]).is_file()
    with sqlite3.connect(research_db) as conn:
        parent_run = conn.execute(
            """
            SELECT id, job_name, status
            FROM ingestion_runs
            WHERE id = ?
            """,
            (report["parent_ingestion_run_id"],),
        ).fetchone()
    assert parent_run[1:3] == (
        "business_profile_official_archive_sync",
        "success",
    )
    with sqlite3.connect(storage.financials_db_path) as conn:
        child_run = conn.execute(
            """
            SELECT job_name, status, metadata_json
            FROM ingestion_runs
            WHERE job_name = 'business_profile_document_archive'
            """
        ).fetchone()
    assert child_run[:2] == ("business_profile_document_archive", "success")
    assert json.loads(child_run[2])["parent_ingestion_run_id"] == parent_run[0]


def test_discovery_error_records_accurate_parent_run_reason(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    coordinator = _FakeCoordinator(
        [_official_candidate()],
        errors=["cninfo_primary_partial_failure"],
    )
    archive_service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "official_reports",
        downloader=lambda _candidate: b"%PDF-1.7 official report fixture",
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=archive_service,
    )

    report = service.sync(
        target_research_db=research_db,
        report_period="2025-12-31",
        max_instruments=1,
        as_of_date="2026-07-18",
        archive_write=True,
        operator_switch=OFFICIAL_ARCHIVE_WRITE_SWITCH,
        checkpoint_root=tmp_path / "checkpoints",
    )

    assert report["status"] == "degraded"
    assert report["archived_documents"] == 1
    assert report["missing_instrument_periods"] == 0
    with sqlite3.connect(research_db) as conn:
        error_message = conn.execute(
            "SELECT error_message FROM ingestion_runs WHERE id = ?",
            (report["parent_ingestion_run_id"],),
        ).fetchone()[0]
    assert error_message == "discovery_errors=1"


def test_invalid_instrument_is_isolated_from_later_targets(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    _seed_invalid_review_candidate(storage)
    coordinator = _FakeCoordinator([_official_candidate()])
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        report_period="2025-12-31",
        max_instruments=2,
        as_of_date="2026-07-18",
    )

    assert report["status"] == "degraded"
    assert report["failed_instruments"] == 1
    assert report["matched_instrument_periods"] == 1
    assert [item["instrument_id"] for item in report["results"]] == [
        "BAD-ID",
        "601088.SH",
    ]
    assert report["results"][0]["exchange"] == ""
    assert len(coordinator.calls) == 1


def test_unmatched_report_period_is_degraded_without_archiving(tmp_path):
    storage, research_db = _storage(tmp_path)
    _seed_review_candidate(storage)
    coordinator = _FakeCoordinator(
        [_official_candidate(title="中国神华2024年年度报告")]
    )
    service = BusinessProfileOfficialArchiveSyncService(
        storage=storage,
        research_config=storage.research_config,
        coordinator=coordinator,
        archive_service=_FailIfArchived(),
    )

    report = service.sync(
        target_research_db=research_db,
        report_period="2025-12-31",
        max_instruments=1,
        as_of_date="2026-07-18",
    )

    assert report["status"] == "degraded"
    assert report["matched_instrument_periods"] == 0
    assert report["missing_instrument_periods"] == 1
    assert report["results"][0]["selected_documents"] == []
