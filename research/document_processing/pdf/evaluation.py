"""Read-only, explicit-corpus PDF engine evaluation helpers."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import math
import os
import platform
import resource
import shutil
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .core import (
    PdfDocumentResult,
    PdfParseRequest,
    PdfProfile,
    PdfResourceLimits,
    compute_content_hash,
)

NATIVE_WORKER_WIDTHS = (1, 2, 4, 6, 8, 10)


def benchmark_native_parallelism(
    cases: Sequence[PdfEvaluationCase],
    *,
    widths: Sequence[int] = NATIVE_WORKER_WIDTHS,
    rounds: int = 20,
    target_pages: Sequence[int] = (),
    timeout_seconds: float = 900.0,
) -> dict[str, Any]:
    """Run a read-only native-worker width comparison over explicit cases."""
    if not cases or not widths or rounds < 1:
        raise ValueError("cases, widths, and rounds must be non-empty/positive")
    from concurrent.futures import ThreadPoolExecutor
    from .adapters import IsolatedNativeAdapter
    from .native_worker import NativeWorkerPool

    reports: list[dict[str, Any]] = []
    for raw_width in widths:
        width = int(raw_width)
        if width < 1:
            raise ValueError("native worker widths must be positive")
        pool = NativeWorkerPool(max_workers=width, queue_size=max(32, width * 2), task_timeout_seconds=timeout_seconds)
        adapter = IsolatedNativeAdapter("pypdfium2", pool=pool, timeout_seconds=timeout_seconds)
        latencies: list[float] = []
        page_counts: list[int] = []
        diagnostics: list[str] = []
        completed_pages = 0
        expected_pages_total = 0
        expected_pages_known = True
        started = time.perf_counter()
        try:
            for _ in range(rounds):
                def run_case(case: PdfEvaluationCase):
                    content = Path(case.pdf_path).read_bytes()
                    case_pages = tuple(target_pages) or case.target_pages
                    call_started = time.perf_counter()
                    result = adapter.extract(content, target_pages=case_pages)
                    return result, time.perf_counter() - call_started

                with ThreadPoolExecutor(max_workers=min(width, len(cases)), thread_name_prefix="pdf-native-benchmark") as executor:
                    outputs = list(executor.map(run_case, cases))
                for case, (result, elapsed) in zip(cases, outputs):
                    latencies.append(elapsed)
                    page_counts.append(len(result.pages))
                    completed_pages += len(result.pages)
                    requested = tuple(target_pages) or case.target_pages
                    if requested:
                        expected_pages_total += len(requested)
                    elif case.page_count is not None:
                        expected_pages_total += int(case.page_count)
                    elif result.page_count:
                        expected_pages_total += int(result.page_count)
                    else:
                        expected_pages_known = False
                    diagnostics.extend(item.code for item in result.diagnostics)
        finally:
            pool.close()
        reports.append({
            "width": width,
            "rounds": rounds,
            "cases": len(cases),
            "elapsed_seconds": time.perf_counter() - started,
            "p50_seconds": statistics.median(latencies) if latencies else None,
            "p95_seconds": _p95(latencies) if latencies else None,
            "throughput_documents_per_second": len(latencies) / max(time.perf_counter() - started, 0.001),
            "completed_pages": completed_pages,
            "expected_pages": expected_pages_total,
            "expected_pages_known": expected_pages_known,
            "diagnostics": sorted(set(diagnostics)),
            "worker_restarts": pool.restart_count,
            "queue_wait_seconds": pool.queue_wait_total_seconds,
            "rss_max_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024),
            "parent_process_alive": True,
        })
    eligible = [item for item in reports if item["parent_process_alive"] and item["expected_pages_known"] and item["completed_pages"] >= item["expected_pages"] and not any(code in {"native_worker_crashed", "native_worker_timeout", "native_worker_protocol_error"} for code in item["diagnostics"])]
    throughput_optimal = min(eligible, key=lambda item: item["elapsed_seconds"])["width"] if eligible else 1
    highest_safe = max((item["width"] for item in eligible), default=1)
    return {"schema_version": "pdf-native-parallel-benchmark.v1", "read_only": True, "widths": list(widths), "rounds": rounds, "reports": reports, "throughput_optimal_width": throughput_optimal, "highest_safe_width": highest_safe, "recommended_width": throughput_optimal}


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
    ocr_mode: str = "none"
    recovery_policy: str = "native_first"
    mode_budget: Mapping[str, Any] = field(default_factory=dict)


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
    chinese_exact_match: float | None = None
    numeric_exact_match: float | None = None
    cpu_seconds: float | None = None
    rss_delta_bytes: int | None = None
    queue_wait_seconds: float = 0.0
    model_load_seconds: float = 0.0
    confidence_coverage: float | None = None
    heading_match: float | None = None
    table_structure_match: float | None = None
    low_quality_recall: float | None = None
    ocr_page_seconds: tuple[float, ...] = ()
    native_selection_match: float | None = None
    negative_ocr_match: bool | None = None


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


def build_archive_manifest(paths: Sequence[str | Path], *, max_cases: int = 24) -> tuple[PdfEvaluationCase, ...]:
    """Create a bounded, read-only manifest from already archived local PDFs.

    This helper never queries providers or downloads data. Classification is
    best-effort and remains evidence metadata, not a production decision.
    """
    if max_cases < 1:
        raise ValueError("max_cases must be positive")
    cases: list[PdfEvaluationCase] = []
    seen: set[str] = set()
    for raw_path in paths:
        path = Path(raw_path)
        if not path.is_file() or path.suffix.lower() != ".pdf":
            continue
        content = path.read_bytes()
        if not content.lstrip().startswith(b"%PDF-"):
            continue
        digest = compute_content_hash(content)
        if digest in seen:
            continue
        seen.add(digest)
        document_class = "native_or_mixed"
        try:
            import pdf_inspector
            classification = pdf_inspector.detect_pdf_bytes(content)
            document_class = str(getattr(classification, "pdf_type", document_class))
            if bool(getattr(classification, "has_encoding_issues", False)):
                document_class = "viewer_readable_native_mapping_corrupt"
        except Exception:
            pass
        cases.append(PdfEvaluationCase(path.stem, str(path), digest, document_class))
        if len(cases) >= max_cases:
            break
    if not cases:
        raise ValueError("archive paths contained no valid PDF cases")
    return tuple(cases)


REQUIRED_CORPUS_CLASSES = (
    "native",
    "scanned",
    "mixed",
    "low_quality",
    "glyph_encoding",
    "viewer_readable_native_mapping_corrupt",
    "table",
    "chinese_announcement",
)


def stratify_cases(cases: Sequence[PdfEvaluationCase]) -> dict[str, Any]:
    """Summarize the bounded corpus and make unavailable strata explicit."""
    counts: dict[str, int] = {}
    aliases = {"text_based": "native", "image_only": "scanned", "encoding_issue": "glyph_encoding"}
    for case in cases:
        category = aliases.get(case.document_class, case.document_class)
        counts[category] = counts.get(category, 0) + 1
    return {
        "case_count": len(cases),
        "counts": counts,
        "required_classes": list(REQUIRED_CORPUS_CLASSES),
        "missing_classes": [name for name in REQUIRED_CORPUS_CLASSES if not counts.get(name)],
        "bounded": True,
    }


def evaluate_cases(
    cases: Sequence[PdfEvaluationCase],
    profiles: Sequence[PdfProfile],
    *,
    router_factory=None,
    max_cases: int | None = None,
    max_pages: int | None = None,
    max_elapsed_seconds: float | None = None,
) -> dict[str, Any]:
    """Run profiles over identical ordered bytes and return bounded metrics."""
    if not cases or not profiles:
        raise ValueError("at least one case and profile are required")
    results: list[PdfEvaluationResult] = []
    bounded_cases = tuple(cases[:max_cases] if max_cases is not None else cases)
    if not bounded_cases:
        raise ValueError("max_cases removed every evaluation case")
    for profile in profiles:
        for case in bounded_cases:
            if max_elapsed_seconds is not None and sum(item.elapsed_seconds for item in results) >= max_elapsed_seconds:
                break
            content = Path(case.pdf_path).read_bytes()
            started = time.perf_counter()
            cpu_started = time.process_time()
            rss_started = None
            try:
                import psutil
                rss_started = psutil.Process().memory_info().rss
            except ImportError:
                pass
            if router_factory is None:
                from .profiles import _require_isolated_gpu_runtime, build_router
                if profile.ocr_device.startswith("gpu"):
                    _require_isolated_gpu_runtime(profile)
                router = build_router(profile, allow_unapproved_gpu_canary=profile.ocr_device.startswith("gpu"))
            else:
                router = router_factory(profile)
            target_pages = case.target_pages
            if max_pages is not None:
                target_pages = target_pages[:max_pages] if target_pages else tuple(range(1, max_pages + 1))
            mode_budget = PdfResourceLimits(**dict(case.mode_budget)) if case.mode_budget else None
            result: PdfDocumentResult = router.parse(PdfParseRequest(content=content, expected_content_hash=case.content_hash, profile=profile, target_pages=target_pages, ocr_mode=case.ocr_mode, recovery_policy=case.recovery_policy, mode_budget=mode_budget))
            elapsed = time.perf_counter() - started
            cpu_elapsed = time.process_time() - cpu_started
            rss_delta = None
            if rss_started is not None:
                rss_delta = __import__("psutil").Process().memory_info().rss - rss_started
            mapping_pages = sum(any(diag.code == "native_text_mapping_error" for diag in page.diagnostics) for page in result.pages)
            expected_mapping = case.gold.get("expected_mapping_corrupt_pages")
            actual_text = "\n".join(page.text for page in result.pages)
            expected_text = str(case.gold.get("expected_text", ""))
            raw_numeric = case.gold.get("expected_numeric", ())
            expected_numeric = (str(raw_numeric),) if isinstance(raw_numeric, (str, int, float)) else tuple(str(value) for value in raw_numeric)
            chinese_exact = _character_recall(actual_text, expected_text) if expected_text else None
            numeric_exact = _fraction_present(actual_text, expected_numeric) if expected_numeric else None
            expected_headings = tuple(str(item) for item in case.gold.get("expected_headings", ()))
            expected_table_cells = tuple(str(item) for item in case.gold.get("expected_table_cells", ()))
            expected_low_quality = {int(item) for item in case.gold.get("expected_low_quality_pages", ())}
            expected_native = {int(item) for item in case.gold.get("native_selected_pages", ())}
            negative_ocr = {int(item) for item in case.gold.get("negative_ocr_pages", ())}
            heading_match = _fraction_present(actual_text, expected_headings) if expected_headings else None
            table_match = _fraction_present(actual_text, expected_table_cells) if expected_table_cells else None
            low_quality_pages = {page.page_number for page in result.pages if page.quality_status in {"empty", "low_text", "native_text_mapping_error", "ocr_low_quality", "ocr_failure"}}
            low_quality_recall = len(low_quality_pages & expected_low_quality) / len(expected_low_quality) if expected_low_quality else None
            selected_native = {page.page_number for page in result.pages if page.selected_method in {"native_text", "alternate_native"}}
            ocr_pages_selected = {page.page_number for page in result.pages if page.selected_method == "ocr"}
            native_selection_match = len(selected_native & expected_native) / len(expected_native) if expected_native else None
            negative_ocr_match = not bool(ocr_pages_selected & negative_ocr) if negative_ocr else None
            confidence_values = [page.confidence for page in result.pages if page.extraction_method == "ocr"]
            confidence_coverage = sum(value is not None for value in confidence_values) / len(confidence_values) if confidence_values else None
            model_load_seconds = max((float(item.get("warmup_seconds", 0.0)) for page in result.pages for item in page.provenance if isinstance(item, Mapping)), default=0.0)
            ocr_page_seconds = tuple(page.elapsed_seconds for page in result.pages if page.extraction_method == "ocr")
            results.append(PdfEvaluationResult(case.case_id, profile.name, result.status, elapsed, result.page_count, len(result.pages), sum(page.extraction_method == "ocr" for page in result.pages), tuple(item.code for item in result.diagnostics), tuple(page.text_hash for page in result.pages), sum(page.elapsed_seconds for page in result.pages if page.extraction_method != "ocr"), sum(ocr_page_seconds), sum(len(page.diagnostics) for page in result.pages), mapping_pages, expected_mapping, mapping_pages / expected_mapping if expected_mapping else None, chinese_exact, numeric_exact, cpu_elapsed, rss_delta, 0.0, model_load_seconds, confidence_coverage, heading_match, table_match, low_quality_recall, ocr_page_seconds, native_selection_match, negative_ocr_match))
    grouped: dict[str, list[PdfEvaluationResult]] = {}
    for item in results:
        grouped.setdefault(item.profile, []).append(item)
    profiles_report = []
    for name, items in grouped.items():
        latencies = [item.elapsed_seconds for item in items]
        ocr_page_latencies = [value for item in items for value in item.ocr_page_seconds]
        ocr_seconds = sum(item.ocr_seconds for item in items)
        profiles_report.append({"profile": name, "cases": len(items), "success_rate": sum(item.status == "success" for item in items) / len(items), "p50_seconds": statistics.median(latencies), "p95_seconds": _p95(latencies), "ocr_page_p50_seconds": statistics.median(ocr_page_latencies) if ocr_page_latencies else None, "ocr_page_p95_seconds": _p95(ocr_page_latencies) if ocr_page_latencies else None, "ocr_pages": sum(item.ocr_pages for item in items), "native_seconds": sum(item.native_seconds for item in items), "ocr_seconds": ocr_seconds, "ocr_time_share": ocr_seconds / max(sum(latencies), 0.001), "ocr_pages_per_second": sum(item.ocr_pages for item in items) / max(ocr_seconds, 0.001), "documents_per_minute": len(items) / max(sum(latencies), 0.001) * 60.0, "pages_per_minute": sum(item.processed_pages for item in items) / max(sum(latencies), 0.001) * 60.0, "cpu_seconds": sum(item.cpu_seconds or 0.0 for item in items), "rss_delta_bytes": max((item.rss_delta_bytes or 0 for item in items), default=0), "queue_wait_seconds": sum(item.queue_wait_seconds for item in items), "model_load_seconds": sum(item.model_load_seconds for item in items), "confidence_coverage": statistics.mean(item.confidence_coverage for item in items if item.confidence_coverage is not None) if any(item.confidence_coverage is not None for item in items) else None, "heading_match": statistics.mean(item.heading_match for item in items if item.heading_match is not None) if any(item.heading_match is not None for item in items) else None, "table_structure_match": statistics.mean(item.table_structure_match for item in items if item.table_structure_match is not None) if any(item.table_structure_match is not None for item in items) else None, "low_quality_recall": statistics.mean(item.low_quality_recall for item in items if item.low_quality_recall is not None) if any(item.low_quality_recall is not None for item in items) else None, "native_selection_match": statistics.mean(item.native_selection_match for item in items if item.native_selection_match is not None) if any(item.native_selection_match is not None for item in items) else None, "negative_ocr_match": all(item.negative_ocr_match is not False for item in items), "mapping_error_pages": sum(item.mapping_error_pages for item in items), "mapping_error_recall": statistics.mean(item.mapping_error_recall for item in items if item.mapping_error_recall is not None) if any(item.mapping_error_recall is not None for item in items) else None, "chinese_exact_match": statistics.mean(item.chinese_exact_match for item in items if item.chinese_exact_match is not None) if any(item.chinese_exact_match is not None for item in items) else None, "numeric_exact_match": statistics.mean(item.numeric_exact_match for item in items if item.numeric_exact_match is not None) if any(item.numeric_exact_match is not None for item in items) else None, "results": [asdict(item) for item in items]})
    corpus_contract = json.dumps(
        [asdict(case) for case in bounded_cases],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    corpus_hash = compute_content_hash(corpus_contract)
    return {"schema_version": "pdf-evaluation.v5", "read_only": True, "corpus_hash": corpus_hash, "case_count": len(bounded_cases), "strata": stratify_cases(bounded_cases), "capabilities": probe_ocr_components(profiles), "profiles": profiles_report}


@dataclass(frozen=True)
class PdfAcceptanceGates:
    min_success_rate: float = 0.95
    max_p95_seconds: float = 900.0
    max_failure_rate: float = 0.05
    max_ocr_pages_deferred: int = 0
    min_chinese_exact_match: float = 0.0
    min_numeric_exact_match: float = 0.0
    min_ocr_pages_per_second: float = 0.0
    max_rss_delta_bytes: int | None = None
    max_queue_wait_seconds: float = 30.0
    require_fidelity: bool = True


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
            "chinese_accuracy": _metric_at_least(profile, "chinese_exact_match", gates.min_chinese_exact_match),
            "numeric_accuracy": _metric_at_least(profile, "numeric_exact_match", gates.min_numeric_exact_match),
            "ocr_throughput": profile.get("ocr_pages_per_second", 0.0) >= gates.min_ocr_pages_per_second,
            "queue_wait": profile.get("queue_wait_seconds", 0.0) <= gates.max_queue_wait_seconds,
            "resource": gates.max_rss_delta_bytes is None or profile.get("rss_delta_bytes", 0) <= gates.max_rss_delta_bytes,
            "fidelity": not gates.require_fidelity or not any(item.get("status") == "fidelity_mismatch" for item in profile.get("results", [])),
        }
        decisions.append({"profile": profile.get("profile"), "eligible": all(checks.values()), "checks": checks})
    eligible = [item["profile"] for item in decisions if item["eligible"]]
    return {"gates": asdict(gates), "decisions": decisions, "eligible_profiles": eligible, "recommendation": eligible[0] if eligible else None}


def run_bounded_canary(
    cases: Sequence[PdfEvaluationCase],
    profile: PdfProfile,
    *,
    router_factory=None,
    max_cases: int = 3,
    max_pages: int = 5,
) -> dict[str, Any]:
    """Run a small, fail-closed canary without mutating production state."""
    report = evaluate_cases(cases, [profile], router_factory=router_factory, max_cases=max_cases, max_pages=max_pages)
    profile_report = report["profiles"][0] if report["profiles"] else {}
    failures = [item for item in profile_report.get("results", []) if item.get("status") not in {"success", "partial"}]
    return {"status": "failed" if failures else "passed", "bounded": True, "max_cases": max_cases, "max_pages": max_pages, "report": report, "failures": failures}


def probe_ocr_components(profiles: Sequence[PdfProfile] = ()) -> dict[str, dict[str, Any]]:
    """Return capability evidence without importing Paddle into Quote.

    CUDA capability is worker-owned. Distribution metadata is sufficient for a
    local inventory; configured GPU profiles invoke their external worker's
    probe command instead of importing a potentially conflicting wheel here.
    """
    paddle_version = _distribution_version("paddlepaddle") or _distribution_version("paddlepaddle-gpu")
    worker_probes: dict[str, Any] = {}
    for profile in profiles:
        if profile.ocr_device.startswith("gpu") and profile.ocr_worker_command:
            from .adapters import PaddleOcrAdapter

            worker_probes[profile.name] = PaddleOcrAdapter.probe_runtime(profile)
    gpu_probe = next((probe for probe in worker_probes.values() if probe.get("cuda_available")), None)
    runtime = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "paddle_version": paddle_version,
        "paddle_cuda_compiled": None,
        "cuda_available": bool(gpu_probe),
        "cuda_device_count": int(gpu_probe.get("cuda_device_count", 0)) if gpu_probe else 0,
        "device": "gpu" if gpu_probe else "unprobed",
        "paddleocr_version": _distribution_version("paddleocr"),
        "pypdfium2_version": _distribution_version("pypdfium2"),
        "pdf_inspector_version": _distribution_version("pdf-inspector"),
        "model_cache_dir": os.environ.get("PADDLE_PDX_CACHE_HOME") or os.environ.get("QUOTE_PDF_OCR_CACHE_DIR"),
        "nvidia_smi": _nvidia_smi_summary(),
        "worker_probes": worker_probes,
    }
    return {
        "runtime": runtime,
        "paddleocr_ppocr": {"available": _module_available("paddleocr"), "component": "pp-ocr", **runtime},
        "paddleocr_pp_structure": {"available": _module_available("paddleocr"), "component": "pp-structure", "selection": "table/layout pages only", **runtime},
        "pdf_inspector_ocr": {"available": _module_available("pdf_inspector"), "component": "pdf-inspector-ocr", "offline": True, **runtime},
        "tesseract_ocrmypdf": {"available": bool(shutil.which("tesseract") or shutil.which("ocrmypdf")), "component": "lightweight-baseline", **runtime},
    }


def _character_recall(actual: str, expected: str) -> float:
    expected_chars = [char for char in expected if not char.isspace()]
    if not expected_chars:
        return 1.0
    return sum(char in actual for char in expected_chars) / len(expected_chars)


def _fraction_present(actual: str, expected: Sequence[str]) -> float:
    compact_actual = "".join(actual.split())
    return sum(item in actual or "".join(item.split()) in compact_actual for item in expected) / max(len(expected), 1)


def _metric_at_least(profile: Mapping[str, Any], key: str, minimum: float) -> bool:
    value = profile.get(key)
    return minimum <= 0.0 or (value is not None and float(value) >= minimum)


def assess_gpu_canary(
    report: Mapping[str, Any],
    *,
    max_document_p95_seconds: float = 120.0,
    max_page_p95_seconds: float = 60.0,
    min_chinese_exact_match: float = 1.0,
    min_numeric_exact_match: float = 1.0,
    min_heading_match: float = 1.0,
    min_confidence_coverage: float = 1.0,
    min_gpu_memory_mib: float = 4096.0,
) -> dict[str, Any]:
    """Produce the explicit approval artifact required by the GPU profile."""
    capabilities = report.get("capabilities", {}).get("runtime", {})
    profiles = [item for item in report.get("profiles", ()) if "gpu" in str(item.get("profile", ""))]
    profile = profiles[0] if profiles else {}
    nvidia_smi = capabilities.get("nvidia_smi") or {}
    visible_gpus = nvidia_smi.get("gpus") or ()
    gpu_memory = [float(item.get("memory_total_mib", 0.0)) for item in visible_gpus if isinstance(item, Mapping)]
    expected_case_count = int(report.get("case_count", 0) or 0)
    evaluated_case_count = int(profile.get("cases", 0) or 0) if profile else 0
    checks = {
        "cuda_runtime": capabilities.get("cuda_available") is True,
        "gpu_resource": nvidia_smi.get("available") is True and int(capabilities.get("cuda_device_count", 0)) > 0 and any(value >= min_gpu_memory_mib for value in gpu_memory),
        "gpu_profile_evaluated": bool(profile),
        "complete_corpus": expected_case_count > 0 and evaluated_case_count == expected_case_count,
        "all_cases_success": bool(profile) and all(item.get("status") == "success" for item in profile.get("results", ())),
        "document_p95": bool(profile) and float(profile.get("p95_seconds", float("inf"))) <= max_document_p95_seconds,
        "page_p95": bool(profile) and profile.get("ocr_page_p95_seconds") is not None and float(profile["ocr_page_p95_seconds"]) <= max_page_p95_seconds,
        "chinese_quality": bool(profile) and profile.get("chinese_exact_match") is not None and float(profile["chinese_exact_match"]) >= min_chinese_exact_match,
        "numeric_quality": bool(profile) and profile.get("numeric_exact_match") is not None and float(profile["numeric_exact_match"]) >= min_numeric_exact_match,
        "heading_quality": bool(profile) and profile.get("heading_match") is not None and float(profile["heading_match"]) >= min_heading_match,
        "confidence_coverage": bool(profile) and profile.get("confidence_coverage") is not None and float(profile["confidence_coverage"]) >= min_confidence_coverage,
        "no_undiagnosed_failures": bool(profile) and all(item.get("status") == "success" or item.get("diagnostics") for item in profile.get("results", ())),
    }
    return {"schema_version": "pdf-gpu-canary-approval.v1", "read_only": True, "corpus_hash": report.get("corpus_hash"), "profile": profile.get("profile"), "checks": checks, "gpu_canary_approved": all(checks.values())}


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _distribution_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _nvidia_smi_summary() -> Mapping[str, Any]:
    try:
        completed = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version,memory.total,compute_cap", "--format=csv,noheader,nounits"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {"available": False}
    if completed.returncode != 0:
        return {"available": False, "diagnostic": completed.stderr.strip()[:300]}
    gpus = []
    for line in completed.stdout.splitlines():
        fields = [item.strip() for item in line.split(",")]
        if len(fields) != 4:
            continue
        name, driver_version, memory_total_mib, compute_capability = fields
        try:
            memory = float(memory_total_mib)
        except ValueError:
            memory = 0.0
        gpus.append({
            "name": name,
            "driver_version": driver_version,
            "memory_total_mib": memory,
            "compute_capability": compute_capability,
        })
    return {"available": bool(gpus), "gpus": gpus}


def _p95(values: Sequence[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


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
