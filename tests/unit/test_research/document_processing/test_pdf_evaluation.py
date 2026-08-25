from __future__ import annotations

import hashlib
import json

from research.document_processing.pdf.evaluation import load_manifest, write_report


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
