from dataclasses import asdict
from pathlib import Path

import pytest

from research.business_profile_archive import (
    BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
    BusinessProfileDocumentArchiveService,
)
from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import classify_business_profile_document


class _Storage:
    def __init__(self):
        self.rows = []
        self.upsert_calls = 0

    def get_financial_source_file_manifests(self, **filters):
        return [
            row
            for row in self.rows
            if all(row.get(key) == value for key, value in filters.items() if value)
        ]

    def upsert_financial_source_file_manifest(self, manifest, *, ingestion_run_id=None):
        self.upsert_calls += 1
        source_file_id = manifest.source_file_id or f"source-{self.upsert_calls}"
        row = asdict(manifest)
        row["source_file_id"] = source_file_id
        row["metadata"] = row.pop("metadata_json")
        row["ingestion_run_id"] = ingestion_run_id
        self.rows = [
            item for item in self.rows if item["source_file_id"] != source_file_id
        ]
        self.rows.append(row)
        return source_file_id


def _candidate(announcement_id, title, *, content_url=None):
    classification = classify_business_profile_document(title, adjunct_type="PDF")
    return BusinessProfileDocumentCandidate(
        announcement_id=announcement_id,
        title=title,
        announcement_time="2026-04-21T08:00:00+08:00",
        symbols=["600309"],
        adjunct_url=content_url or f"finalpage/{announcement_id}.PDF",
        adjunct_type="PDF",
        classification=classification,
        selection_reasons=[f"business_profile_document:{classification.document_type}"],
    )


def _instrument():
    return {"instrument_id": "600309.SH", "symbol": "600309", "exchange": "SSE"}


def test_archive_writes_immutable_hash_path_and_manifest(tmp_path):
    storage = _Storage()
    service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "filings",
    )
    candidate = _candidate("annual-1", "万华化学2025年年度报告")
    content = b"%PDF-1.7\nfixture"

    record = service.archive_content(
        _instrument(),
        candidate,
        content,
        ingestion_run_id=7,
    )

    path = Path(record.archive_path)
    assert record.status == "archived"
    assert record.report_period == "2025-12-31"
    assert record.content_hash in record.archive_path
    assert path.name.endswith(".pdf")
    assert path.parent.name == "original"
    assert path.read_bytes() == content
    assert storage.rows[0]["schema_version"] == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
    assert storage.rows[0]["source_tier"] == "official_primary"
    assert storage.rows[0]["filing_id"] == "annual-1"
    assert storage.rows[0]["published_at"] == candidate.announcement_time
    assert storage.rows[0]["metadata"]["document_family"] == "annual_report"


def test_exact_rerun_short_circuits_manifest_write(tmp_path):
    storage = _Storage()
    service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "filings",
    )
    candidate = _candidate("annual-1", "万华化学2025年年度报告")
    content = b"%PDF-1.7\nsame"

    first = service.archive_content(_instrument(), candidate, content)
    second = service.archive_content(_instrument(), candidate, content)

    assert first.status == "archived"
    assert second.status == "unchanged"
    assert second.source_file_id == first.source_file_id
    assert storage.upsert_calls == 1


def test_corrected_report_supersedes_original_without_overwrite(tmp_path):
    storage = _Storage()
    service = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "filings",
    )
    original = service.archive_content(
        _instrument(),
        _candidate("annual-1", "万华化学2025年年度报告"),
        b"%PDF-1.7\noriginal",
    )
    correction = service.archive_content(
        _instrument(),
        _candidate("annual-2", "万华化学2025年年度报告（修订版）"),
        b"%PDF-1.7\ncorrected",
    )

    assert correction.supersedes_source_file_id == original.source_file_id
    assert correction.archive_path != original.archive_path
    assert len(storage.rows) == 2
    assert storage.rows[1]["report_type"] == "annual_report_correction"
    assert storage.rows[1]["supersedes_source_file_id"] == original.source_file_id
    assert storage.rows[0]["status"] == "archived"


def test_checkpoint_resumes_after_interruption_and_is_removed_when_complete(tmp_path):
    storage = _Storage()
    candidates = [
        _candidate("annual-1", "万华化学2024年年度报告"),
        _candidate("annual-2", "万华化学2025年年度报告"),
    ]
    checkpoint = tmp_path / "archive-checkpoint.json"

    def interrupting_downloader(candidate):
        if candidate.announcement_id == "annual-2":
            raise KeyboardInterrupt
        return b"%PDF-1.7\nfirst"

    interrupted = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "filings",
        downloader=interrupting_downloader,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.archive_candidates(
            _instrument(),
            candidates,
            checkpoint_path=checkpoint,
        )

    assert checkpoint.exists()
    resumed_downloads = []

    def resumed_downloader(candidate):
        resumed_downloads.append(candidate.announcement_id)
        return b"%PDF-1.7\nsecond"

    resumed = BusinessProfileDocumentArchiveService(
        storage=storage,
        archive_root=tmp_path / "filings",
        downloader=resumed_downloader,
    ).archive_candidates(
        _instrument(),
        candidates,
        checkpoint_path=checkpoint,
    )

    assert resumed.skipped_checkpoint == 1
    assert resumed_downloads == ["annual-2"]
    assert resumed.checkpoint_complete is True
    assert not checkpoint.exists()


def test_archive_rejects_non_pdf_payload(tmp_path):
    service = BusinessProfileDocumentArchiveService(
        storage=_Storage(),
        archive_root=tmp_path / "filings",
    )

    with pytest.raises(ValueError, match="not a PDF"):
        service.archive_content(
            _instrument(),
            _candidate("annual-1", "万华化学2025年年度报告"),
            b"<html>rate limited</html>",
        )
