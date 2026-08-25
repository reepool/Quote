from __future__ import annotations

import hashlib
import json

from research.document_processing.pdf import PdfDiagnostic, PdfDocumentResult, PdfPageResult, PdfProfile, PdfParseRequest
from research.document_processing.pdf.evaluation import (
    PdfAcceptanceGates,
    assess_report,
    build_archive_manifest,
    evaluate_cases,
    load_manifest,
    probe_ocr_components,
    run_bounded_canary,
    stratify_cases,
    write_report,
)


def test_manifest_requires_explicit_hash_and_write_report(tmp_path) -> None:
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4\nfixture")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"cases": [{"case_id": "one", "pdf_path": str(pdf), "content_hash": hashlib.sha256(pdf.read_bytes()).hexdigest()}]}), encoding="utf-8")
    cases = load_manifest(manifest)
    assert cases[0].case_id == "one"
    report_path = tmp_path / "report.md"
    write_report({"schema_version": "x", "read_only": True, "profiles": []}, report_path)
    assert "Read-only" in report_path.read_text(encoding="utf-8")


def test_archive_manifest_is_bounded_and_read_only(tmp_path) -> None:
    first = tmp_path / "one.pdf"
    second = tmp_path / "two.pdf"
    first.write_bytes(b"%PDF-1.4\nfirst")
    second.write_bytes(b"%PDF-1.4\nsecond")
    cases = build_archive_manifest([first, second, first], max_cases=1)
    assert len(cases) == 1
    assert cases[0].content_hash
    strata = stratify_cases(cases)
    assert strata["case_count"] == 1
    assert strata["bounded"] is True


def test_evaluation_reports_accuracy_efficiency_and_resource_metrics(tmp_path) -> None:
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4\nfixture")
    case = load_manifest(_manifest_for(pdf, tmp_path / "manifest.json", gold={
        "expected_text": "招商银行 600036",
        "expected_numeric": "600036",
        "expected_headings": ["招商银行"],
        "expected_table_cells": ["600036"],
    }))[0]

    def router_factory(profile):
        class FakeRouter:
            def parse(self, request: PdfParseRequest):
                text = "招商银行 600036"
                page = PdfPageResult(1, text, "native", "usable", "text-hash", "page-hash", confidence=None)
                return PdfDocumentResult("shared_pdf.v1", "test", profile.name, request.content_hash, request.parameter_hash, 1, (page,), "success")
        return FakeRouter()

    report = evaluate_cases([case], [PdfProfile(name="test")], router_factory=router_factory)
    profile = report["profiles"][0]
    assert report["schema_version"] == "pdf-evaluation.v3"
    assert profile["documents_per_minute"] > 0
    assert profile["chinese_exact_match"] == 1.0
    assert profile["numeric_exact_match"] == 1.0
    assert profile["heading_match"] == 1.0
    assert profile["table_structure_match"] == 1.0


def test_acceptance_gates_reject_accuracy_and_queue_bottlenecks() -> None:
    report = {"profiles": [{
        "profile": "slow-ocr", "cases": 1, "success_rate": 1.0,
        "p95_seconds": 1.0, "ocr_pages_per_second": 0.2,
        "queue_wait_seconds": 40.0, "rss_delta_bytes": 1,
        "chinese_exact_match": 0.8, "numeric_exact_match": 0.7,
        "results": [{"status": "success"}],
    }]}
    assessed = assess_report(report, PdfAcceptanceGates(min_chinese_exact_match=0.9, min_numeric_exact_match=0.9, min_ocr_pages_per_second=1.0))
    assert assessed["eligible_profiles"] == []
    checks = assessed["decisions"][0]["checks"]
    assert checks["chinese_accuracy"] is False
    assert checks["ocr_throughput"] is False
    assert checks["queue_wait"] is False


def test_bounded_canary_is_fail_closed_and_component_probe_is_local(tmp_path) -> None:
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(b"%PDF-1.4\nfixture")
    case = load_manifest(_manifest_for(pdf, tmp_path / "manifest.json"))[0]

    class FakeRouter:
        def parse(self, request):
            page = PdfPageResult(1, "ok", "native", "usable", "hash", "page-hash")
            return PdfDocumentResult("shared_pdf.v1", "test", request.profile.name, request.content_hash, request.parameter_hash, 1, (page,), "success")

    result = run_bounded_canary([case], PdfProfile(name="canary"), router_factory=lambda _: FakeRouter(), max_cases=1, max_pages=1)
    assert result["status"] == "passed"
    assert result["bounded"] is True
    components = probe_ocr_components()
    assert set(components) == {"paddleocr_ppocr", "paddleocr_pp_structure", "pdf_inspector_ocr", "tesseract_ocrmypdf"}


def _manifest_for(pdf, manifest, *, gold=None):
    manifest.write_text(json.dumps({"cases": [{
        "case_id": "one", "pdf_path": str(pdf),
        "content_hash": hashlib.sha256(pdf.read_bytes()).hexdigest(),
        "gold": gold or {},
    }]}), encoding="utf-8")
    return manifest
