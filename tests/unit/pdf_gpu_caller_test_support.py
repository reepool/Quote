"""Shared fixtures for caller-level GPU PDF readiness regressions."""

from __future__ import annotations

import json


def configure_approved_gpu_profile_with_unavailable_worker(monkeypatch, tmp_path):
    """Enable static GPU approval and fail immediately if native parsing reaches OCR."""
    from research.document_processing.pdf import profiles as pdf_profiles
    from research.document_processing.pdf.adapters import PaddleOcrAdapter
    from research.document_processing.pdf.profiles import GPU_CANARY_REQUIRED_CHECKS

    corpus = "c" * 64
    report = tmp_path / "gpu-canary.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "pdf-gpu-canary-approval.v1",
                "profile": "pdfium_paddleocr_gpu",
                "corpus_hash": corpus,
                "gpu_canary_approved": True,
                "checks": {name: True for name in GPU_CANARY_REQUIRED_CHECKS},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pdf_profiles, "GPU_CANARY_CORPUS_HASH", corpus)
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_APPROVED", "1")
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_REPORT", str(report))
    monkeypatch.setenv("QUOTE_PDF_GPU_OCR_WORKER", "/definitely-missing/quote-pdf-gpu-worker")
    monkeypatch.setenv("QUOTE_PDF_CPU_OCR_WORKER", "/definitely-missing/quote-pdf-cpu-worker")
    pdf_profiles.clear_gpu_runtime_probe_cache()

    calls: list[str] = []

    def unexpected_probe(*_args, **_kwargs):
        calls.append("probe")
        raise AssertionError("native-only caller must not probe the unavailable GPU worker")

    def unexpected_ocr(*_args, **_kwargs):
        calls.append("ocr")
        raise AssertionError("native-only caller must not invoke GPU OCR or CPU fallback")

    monkeypatch.setattr(PaddleOcrAdapter, "probe_runtime", staticmethod(unexpected_probe))
    monkeypatch.setattr(PaddleOcrAdapter, "extract_pages", unexpected_ocr)
    return calls


def text_pdf_bytes(*lines: str) -> bytes:
    """Build a small valid text PDF without a test-only rendering dependency."""
    encoded_lines = []
    for line in lines:
        value = str(line).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        encoded_lines.append(f"({value}) Tj T*")
    stream = ("BT\n/F1 12 Tf\n14 TL\n72 720 Td\n" + "\n".join(encoded_lines) + "\nET\n").encode("latin-1")
    objects = (
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"endstream",
    )
    body = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for number, value in enumerate(objects, start=1):
        offsets.append(len(body))
        body.extend(f"{number} 0 obj\n".encode("ascii"))
        body.extend(value)
        body.extend(b"\nendobj\n")
    xref_offset = len(body)
    body.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    body.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        body.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    body.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("ascii"))
    return bytes(body)
