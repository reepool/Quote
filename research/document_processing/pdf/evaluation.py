"""Read-only, explicit-corpus PDF engine evaluation helpers."""

from __future__ import annotations

import json
import statistics
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .core import PdfDocumentResult, PdfParseRequest, PdfProfile, PdfRouter, compute_content_hash


@dataclass(frozen=True)
class PdfEvaluationCase:
    case_id: str
    pdf_path: str
    content_hash: str
    document_class: str = "unknown"
    announcement_id: str | None = None
    instrument: str | None = None
    page_count: int | None = None
    gold: Mapping[str, Any] = field(default_factory=dict)
    target_pages: tuple[int, ...] = ()


@dataclass(frozen=True)
class PdfEvaluationResult:
    case_id: str
    profile: str
    status: str
    elapsed_seconds: float
    page_count: int
    processed_pages: int
    ocr_pages: int
    diagnostics: tuple[str, ...] = ()
    page_text_hashes: tuple[str, ...] = ()
    native_seconds: float = 0.0
    ocr_seconds: float = 0.0
    warnings: int = 0
    mapping_error_pages: int = 0
    expected_mapping_error_pages: int | None = None
    mapping_error_recall: float | None = None


def load_manifest(path: str | Path) -> tuple[PdfEvaluationCase, ...]:
    """Load an explicit JSON manifest; no discovery or downloading is allowed."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("cases") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list) or not rows:
        raise ValueError("evaluation manifest must contain a non-empty cases list")
    cases: list[PdfEvaluationCase] = []
    for row in rows:
        case = PdfEvaluationCase(**row)
        file_path = Path(case.pdf_path)
        if not file_path.is_file():
            raise FileNotFoundError(case.pdf_path)
        content = file_path.read_bytes()
        if not content.lstrip().startswith(b"%PDF-"):
            raise ValueError(f"{case.case_id}: invalid PDF signature")
        actual = compute_content_hash(content)
        if actual != case.content_hash:
            raise ValueError(f"{case.case_id}: content hash mismatch")
        cases.append(case)
    return tuple(cases)


def evaluate_cases(cases: Sequence[PdfEvaluationCase], profiles: Sequence[PdfProfile], *, router_factory=None) -> dict[str, Any]:
    """Run profiles over identical ordered bytes and return bounded metrics."""
    if not cases or not profiles:
        raise ValueError("at least one case and profile are required")
    results: list[PdfEvaluationResult] = []
    for profile in profiles:
        for case in cases:
            content = Path(case.pdf_path).read_bytes()
            started = time.perf_counter()
            if router_factory is None:
                from .profiles import build_router
                router = build_router(profile)
            else:
                router = router_factory(profile)
            result: PdfDocumentResult = router.parse(PdfParseRequest(content=content, expected_content_hash=case.content_hash, profile=profile, target_pages=case.target_pages))
            elapsed = time.perf_counter() - started
            mapping_pages = sum(any(diag.code == "native_text_mapping_error" for diag in page.diagnostics) for page in result.pages)
            expected_mapping = case.gold.get("expected_mapping_corrupt_pages")
            results.append(PdfEvaluationResult(case.case_id, profile.name, result.status, elapsed, result.page_count, len(result.pages), sum(page.extraction_method == "ocr" for page in result.pages), tuple(item.code for item in result.diagnostics), tuple(page.text_hash for page in result.pages), sum(page.elapsed_seconds for page in result.pages if page.extraction_method != "ocr"), sum(page.elapsed_seconds for page in result.pages if page.extraction_method == "ocr"), sum(len(page.diagnostics) for page in result.pages), mapping_pages, expected_mapping, mapping_pages / expected_mapping if expected_mapping else None))
    grouped: dict[str, list[PdfEvaluationResult]] = {}
    for item in results:
        grouped.setdefault(item.profile, []).append(item)
    profiles_report = []
    for name, items in grouped.items():
        latencies = [item.elapsed_seconds for item in items]
        profiles_report.append({"profile": name, "cases": len(items), "success_rate": sum(item.status == "success" for item in items) / len(items), "p50_seconds": statistics.median(latencies), "p95_seconds": sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)], "ocr_pages": sum(item.ocr_pages for item in items), "native_seconds": sum(item.native_seconds for item in items), "ocr_seconds": sum(item.ocr_seconds for item in items), "mapping_error_pages": sum(item.mapping_error_pages for item in items), "mapping_error_recall": statistics.mean(item.mapping_error_recall for item in items if item.mapping_error_recall is not None) if any(item.mapping_error_recall is not None for item in items) else None, "results": [asdict(item) for item in items]})
    corpus_hash = compute_content_hash("\n".join(f"{case.case_id}:{case.content_hash}" for case in cases).encode())
    return {"schema_version": "pdf-evaluation.v2", "read_only": True, "corpus_hash": corpus_hash, "case_count": len(cases), "profiles": profiles_report}


@dataclass(frozen=True)
class PdfAcceptanceGates:
    min_success_rate: float = 0.95
    max_p95_seconds: float = 900.0
    max_failure_rate: float = 0.05
    max_ocr_pages_deferred: int = 0


def assess_report(report: Mapping[str, Any], gates: PdfAcceptanceGates = PdfAcceptanceGates()) -> dict[str, Any]:
    """Apply explicit fidelity/latency gates without selecting by speed alone."""
    decisions = []
    for profile in report.get("profiles", []):
        failures = sum(item.get("status") == "failed" for item in profile.get("results", []))
        deferred = sum(item.get("status") in {"partial", "failed"} and item.get("ocr_pages", 0) > 0 for item in profile.get("results", []))
        checks = {
            "success_rate": profile.get("success_rate", 0.0) >= gates.min_success_rate,
            "p95_latency": profile.get("p95_seconds", float("inf")) <= gates.max_p95_seconds,
            "failure_rate": failures / max(profile.get("cases", 1), 1) <= gates.max_failure_rate,
            "ocr_budget": deferred <= gates.max_ocr_pages_deferred,
        }
        decisions.append({"profile": profile.get("profile"), "eligible": all(checks.values()), "checks": checks})
    eligible = [item["profile"] for item in decisions if item["eligible"]]
    return {"gates": asdict(gates), "decisions": decisions, "eligible_profiles": eligible, "recommendation": eligible[0] if eligible else None}


def write_report(report: Mapping[str, Any], path: str | Path) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.suffix.lower() == ".json":
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return
    lines = ["# PDF Engine Evaluation", "", f"- Schema: `{report.get('schema_version')}`", f"- Read-only: `{report.get('read_only')}`", ""]
    for item in report.get("profiles", []):
        lines.extend([f"## {item['profile']}", "", f"- Cases: {item['cases']}", f"- Success rate: {item['success_rate']:.3f}", f"- P50/P95 seconds: {item['p50_seconds']:.4f}/{item['p95_seconds']:.4f}", f"- OCR pages: {item['ocr_pages']}", ""])
    destination.write_text("\n".join(lines), encoding="utf-8")


MANDATORY_600036_CASE = {
    "case_id": "600036.SH-2025-annual-report",
    "announcement_id": "ann_e9a7df3862148a4699fd3a36284fe1c7",
    "instrument": "600036.SH",
    "content_hash": "abe612a273468072b176dd51ea460c1e1596f8ca729cbc6db3fa28ba9a57ea79",
    "page_count": 350,
    "document_class": "viewer_readable_native_mapping_corrupt",
}
