import asyncio
from dataclasses import replace
from pathlib import Path

from data_manager import DataManager
from research.annual_report_assets import AnnualReportAssetCatalog
from research.business_profile_archive import BusinessProfileDocumentArchiveService
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_official_archive_sync import (
    _official_candidate,
)


INSTRUMENT = {
    "instrument_id": "601088.SH",
    "symbol": "601088",
    "exchange": "SSE",
}


def test_archive_reuses_verified_annual_report_before_download(tmp_path):
    storage = _storage(tmp_path)
    calls = []

    def downloader(_candidate):
        calls.append("download")
        return b"%PDF-1.7 reusable annual report"

    service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "reports",
        downloader=downloader,
    )
    candidate = _official_candidate()

    first = service.archive_candidates(INSTRUMENT, [candidate], max_documents=1)
    second = service.archive_candidates(INSTRUMENT, [candidate], max_documents=1)

    assert first.archived == 1
    assert second.unchanged == 1
    assert calls == ["download"]
    assert first.records[0].source_file_id == second.records[0].source_file_id


def test_catalog_keeps_history_and_switches_active_correction(tmp_path):
    storage = _storage(tmp_path)
    payloads = iter(
        (
            b"%PDF-1.7 original annual report",
            b"%PDF-1.7 corrected annual report",
        )
    )
    service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "reports",
        downloader=lambda _candidate: next(payloads),
    )
    original = _official_candidate()
    correction = replace(
        _official_candidate(
            title="中国神华2025年年度报告（更正后）",
            announcement_id="ann-2025-ar-correction",
        ),
        announcement_time="2026-04-02 18:00:00",
    )

    service.archive_candidates(INSTRUMENT, [original], max_documents=1)
    service.archive_candidates(INSTRUMENT, [correction], max_documents=1)
    catalog = AnnualReportAssetCatalog(storage)
    history = catalog.list_assets(
        instrument_id="601088.SH",
        report_period="2025-12-31",
        validate_files=True,
    )

    assert len(history) == 2
    assert all(item["integrity_status"] == "valid" for item in history)
    active = [item for item in history if item["is_active"]]
    assert [item["filing_id"] for item in active] == ["ann-2025-ar-correction"]
    assert active[0]["supersedes_source_file_id"] == next(
        item["source_file_id"] for item in history if not item["is_active"]
    )
    before_correction = catalog.get_asset(
        "601088.SH",
        knowledge_cutoff="2026-03-30",
    )
    assert before_correction is not None
    assert before_correction["filing_id"] == "ann-2025-ar"


def test_catalog_rejects_missing_file_and_data_manager_lists_history(tmp_path):
    storage = _storage(tmp_path)
    service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "reports",
        downloader=lambda _candidate: b"%PDF-1.7 annual report",
    )
    record = service.archive_candidates(
        INSTRUMENT,
        [_official_candidate()],
        max_documents=1,
    ).records[0]
    Path(record.archive_path).unlink()

    catalog = AnnualReportAssetCatalog(storage)
    assert catalog.get_asset("601088.SH", validate_file=True) is None
    rows = catalog.list_assets(instrument_id="601088.SH", validate_files=True)
    assert rows[0]["integrity_status"] == "missing"

    manager = DataManager.__new__(DataManager)
    manager.research_config = storage.research_config
    manager.research_storage = storage
    listed = asyncio.run(
        manager.get_annual_report_assets(
            instrument_id="601088.SH",
            validate_files=True,
        )
    )
    assert listed[0]["source_file_id"] == record.source_file_id
    assert listed[0]["integrity_status"] == "missing"
