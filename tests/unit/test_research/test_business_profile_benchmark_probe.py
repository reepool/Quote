from pathlib import Path

import pytest

from research.business_profile_benchmark_probe import (
    probe_benchmark_documents,
    select_probe_issuers,
)
from research.business_profile_discovery import (
    BusinessProfileDiscoveryResult,
    BusinessProfileDocumentCandidate,
)
from research.business_profile_documents import classify_business_profile_document


def _benchmark():
    return {
        "industries": {
            "coal": {
                "selected_issuers": [
                    {
                        "instrument_id": "600001.SH",
                        "symbol": "600001",
                        "company_name": "甲公司",
                        "exchange": "SSE",
                    },
                    {
                        "instrument_id": "000001.SZ",
                        "symbol": "000001",
                        "company_name": "乙公司",
                        "exchange": "SZSE",
                    },
                ]
            },
            "steel": {
                "selected_issuers": [
                    {
                        "instrument_id": "920001.BJ",
                        "symbol": "920001",
                        "company_name": "丙公司",
                        "exchange": "BSE",
                    }
                ]
            },
        }
    }


def _candidate(
    announcement_id,
    title,
    announcement_time,
    *,
    adjunct_url="finalpage/report.PDF",
):
    return BusinessProfileDocumentCandidate(
        announcement_id=announcement_id,
        title=title,
        announcement_time=announcement_time,
        symbols=["600001"],
        adjunct_url=adjunct_url,
        adjunct_type="PDF",
        classification=classify_business_profile_document(
            title,
            adjunct_type="PDF",
        ),
        selection_reasons=["business_profile_document:annual_report"],
    )


class _Adapter:
    def __init__(self, candidates):
        self.candidates = candidates
        self.calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        return BusinessProfileDiscoveryResult(
            status="success",
            purpose_key="probe",
            instrument_id=instrument["instrument_id"],
            symbol=instrument["symbol"],
            exchange=instrument["exchange"],
            pages_scanned=1,
            announcements_seen=len(self.candidates),
            candidates=list(self.candidates),
            max_announcement_time="2026-04-30T00:00:00+00:00",
            stopped_at_watermark=False,
        )


class _Artifact:
    status = "parsed"
    artifact_hash = "artifact-hash"
    page_count = 10
    heading_index = [object(), object()]
    low_text_pages = [1]
    ocr_required_pages = []
    parser_diagnostics = []
    diagnostics = {"native_text_page_count": 10}


class _Extractor:
    def __init__(self):
        self.calls = []

    def extract_bytes(self, content, *, source_pdf_path):
        self.calls.append((content, source_pdf_path))
        return _Artifact()


def test_select_probe_issuers_is_deterministic_and_bounded():
    selected = select_probe_issuers(
        _benchmark(),
        industry_groups=["coal"],
        max_issuers=1,
    )

    assert [item["instrument_id"] for item in selected] == ["600001.SH"]
    assert selected[0]["industry_group"] == "coal"


def test_select_probe_issuers_rejects_unknown_explicit_instrument():
    with pytest.raises(ValueError, match="unavailable after benchmark filters"):
        select_probe_issuers(
            _benchmark(),
            instrument_ids=["600999.SH"],
        )


def test_select_probe_issuers_does_not_silently_truncate_explicit_instruments():
    with pytest.raises(ValueError, match="000001.SZ"):
        select_probe_issuers(
            _benchmark(),
            instrument_ids=["600001.SH", "000001.SZ"],
            max_issuers=1,
        )


def test_metadata_probe_passes_bounded_query_and_never_downloads():
    adapter = _Adapter(
        [
            _candidate(
                "annual",
                "甲公司2025年年度报告",
                "2026-04-20T00:00:00+00:00",
            )
        ]
    )
    downloads = []

    result = probe_benchmark_documents(
        _benchmark(),
        adapter=adapter,
        downloader=lambda candidate: downloads.append(candidate),
        instrument_ids=["600001.SH"],
        start_date="2026-01-01",
        end_date="2026-07-17",
        search_key="年度报告",
        category="category_ndbg_szsh",
        page_size=10,
        max_pages=2,
    )

    assert result["status"] == "success"
    assert result["mode"] == "metadata_only"
    assert result["periodic_report_candidate_count"] == 1
    assert downloads == []
    _, kwargs = adapter.calls[0]
    assert kwargs["dry_run"] is True
    assert kwargs["search_key"] == "年度报告"
    assert kwargs["category"] == "category_ndbg_szsh"
    assert kwargs["page_size"] == 10
    assert kwargs["max_pages"] == 2


def test_probe_requires_a_complete_date_range():
    with pytest.raises(ValueError, match="must be provided together"):
        probe_benchmark_documents(
            _benchmark(),
            adapter=_Adapter([]),
            start_date="2026-01-01",
        )


def test_probe_rejects_production_archive_as_download_root():
    with pytest.raises(ValueError, match="must not use the production archive"):
        probe_benchmark_documents(
            _benchmark(),
            adapter=_Adapter([]),
            download_root=Path("data/filings/business_profile/probe"),
        )


def test_pdf_probe_prefers_latest_correction_and_writes_content_addressed_file(
    tmp_path,
):
    original = _candidate(
        "annual-original",
        "甲公司2025年年度报告",
        "2026-04-20T00:00:00+00:00",
    )
    correction = _candidate(
        "annual-correction",
        "甲公司2025年年度报告（修订版）",
        "2026-04-30T00:00:00+00:00",
    )
    adapter = _Adapter([original, correction])
    extractor = _Extractor()

    result = probe_benchmark_documents(
        _benchmark(),
        adapter=adapter,
        downloader=lambda candidate: b"%PDF-fixture",
        extractor=extractor,
        instrument_ids=["600001.SH"],
        download_root=tmp_path,
        max_documents_per_issuer=1,
    )

    issuer = result["results"][0]
    document = issuer["documents"][0]
    assert result["status"] == "success"
    assert result["mode"] == "pdf_diagnostics"
    assert issuer["correction_candidate_count"] == 1
    assert document["announcement_id"] == "annual-correction"
    assert document["is_correction"] is True
    assert document["artifact_status"] == "parsed"
    assert document["local_path"].startswith(str(tmp_path))
    assert extractor.calls[0][0] == b"%PDF-fixture"
    assert (tmp_path / "600001.SH").is_dir()


def test_pdf_probe_rejects_non_pdf_download_before_writing(tmp_path):
    adapter = _Adapter(
        [
            _candidate(
                "annual",
                "甲公司2025年年度报告",
                "2026-04-20T00:00:00+00:00",
            )
        ]
    )

    result = probe_benchmark_documents(
        _benchmark(),
        adapter=adapter,
        downloader=lambda candidate: b"<html>blocked</html>",
        instrument_ids=["600001.SH"],
        download_root=tmp_path,
    )

    assert result["status"] == "degraded"
    assert result["diagnosed_document_count"] == 0
    assert result["results"][0]["errors"][0].endswith(
        "ValueError: downloaded attachment is not a PDF"
    )
    assert list(tmp_path.rglob("*")) == []


def test_pdf_probe_reports_parser_failure_as_degraded(tmp_path):
    class _FailedArtifact(_Artifact):
        status = "parse_failed"
        diagnostics = {"failure_class": "malformed_pdf"}

    class _FailedExtractor:
        def extract_bytes(self, content, *, source_pdf_path):
            return _FailedArtifact()

    adapter = _Adapter(
        [
            _candidate(
                "annual",
                "甲公司2025年年度报告",
                "2026-04-20T00:00:00+00:00",
            )
        ]
    )

    result = probe_benchmark_documents(
        _benchmark(),
        adapter=adapter,
        downloader=lambda candidate: b"%PDF-malformed",
        extractor=_FailedExtractor(),
        instrument_ids=["600001.SH"],
        download_root=tmp_path,
    )

    assert result["status"] == "degraded"
    assert result["failure_count"] == 1
    assert result["results"][0]["errors"] == [
        "artifact:annual:parse_failed:malformed_pdf"
    ]


def test_probe_reports_discovery_failure_without_aborting_other_issuers():
    class _PartiallyFailingAdapter(_Adapter):
        def discover_instrument(self, instrument, **kwargs):
            if instrument["instrument_id"] == "600001.SH":
                raise RuntimeError("source unavailable")
            return super().discover_instrument(instrument, **kwargs)

    adapter = _PartiallyFailingAdapter(
        [
            _candidate(
                "annual",
                "乙公司2025年年度报告",
                "2026-04-20T00:00:00+00:00",
            )
        ]
    )

    result = probe_benchmark_documents(
        _benchmark(),
        adapter=adapter,
        industry_groups=["coal"],
    )

    assert result["status"] == "degraded"
    assert result["failure_count"] == 1
    assert result["missing_report_issuer_count"] == 1
    assert result["results"][0]["errors"][0].startswith("discovery:RuntimeError")
