"""Configuration helpers for named PDF engine profiles."""

from __future__ import annotations

import json
import logging
import os
import shlex
import threading
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, Mapping

from .core import DEFAULT_MODE_BUDGETS, PdfProfile, PdfResourceLimits

logger = logging.getLogger(__name__)
_GPU_PROBE_LOCK = threading.Lock()
_GPU_PROBE_CACHE: dict[tuple[Any, ...], Mapping[str, Any]] = {}


def profile_from_mapping(name: str, raw: Mapping[str, Any]) -> PdfProfile:
    """Build and validate a profile from JSON/YAML-like configuration."""
    limits_raw = dict(raw.get("limits") or {})
    allowed_limits = {item.name for item in fields(PdfResourceLimits)}
    unknown_limits = set(limits_raw) - allowed_limits
    if unknown_limits:
        raise ValueError(f"unknown PDF resource limits: {sorted(unknown_limits)}")
    limits = PdfResourceLimits(**limits_raw)
    mode_budgets_raw = dict(raw.get("mode_budgets") or {})
    unknown_modes = set(mode_budgets_raw) - set(DEFAULT_MODE_BUDGETS)
    if unknown_modes:
        raise ValueError(f"unknown PDF OCR modes: {sorted(unknown_modes)}")
    mode_budgets = {
        mode: PdfResourceLimits(**dict(values))
        for mode, values in mode_budgets_raw.items()
    }
    allowed = {item.name for item in fields(PdfProfile)} - {"name"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown PDF profile keys: {sorted(unknown)}")
    profile = PdfProfile(name=name, limits=limits, mode_budgets=mode_budgets, **{key: raw[key] for key in raw if key not in {"limits", "mode_budgets"}})
    if profile.rollout_state not in {"shadow", "canary", "active", "disabled"}:
        raise ValueError("rollout_state must be shadow, canary, active, or disabled")
    return profile


PDFIUM_ENGINE_VERSIONS = {
    "pypdfium2": "5.13.0",
    "paddlepaddle": "3.3.1",
    "paddleocr": "3.7.0",
    "paddleocr_model": "PP-OCRv6",
}


def _worker_command(env_name: str) -> tuple[str, ...]:
    value = os.environ.get(env_name, "").strip()
    return tuple(shlex.split(value)) if value else ()

# The default profile is native-only: OCR remains an explicit recovery action.
# ``pypdf_native`` is retained as a configuration-only rollback profile.
DEFAULT_PROFILES = {
    "pdfium_native": PdfProfile(
        name="pdfium_native",
        native_engines=("pypdfium2", "pypdf"),
        native_max_concurrency=4,
        expected_script="auto",
        min_text_characters=4,
        engine_versions=PDFIUM_ENGINE_VERSIONS,
        fallback_profile="pypdf_native",
    ),
    "pypdf_native": PdfProfile(
        name="pypdf_native",
        native_engines=("pypdf",),
        native_max_concurrency=4,
        expected_script="auto",
        min_text_characters=4,
        engine_versions=PDFIUM_ENGINE_VERSIONS,
    ),
    "pdfium_paddleocr_cpu": PdfProfile(
        name="pdfium_paddleocr_cpu",
        native_engines=("pypdfium2", "pypdf"),
        native_max_concurrency=4,
        ocr_engine="paddleocr",
        expected_script="auto",
        min_text_characters=4,
        engine_versions=PDFIUM_ENGINE_VERSIONS,
        fallback_profile="pdfium_native",
        ocr_runtime="isolated-cpu-paddle-3.3.1",
        ocr_worker_command=_worker_command("QUOTE_PDF_CPU_OCR_WORKER"),
    ),
    "pdfium_paddleocr_gpu": PdfProfile(
        name="pdfium_paddleocr_gpu",
        native_engines=("pypdfium2", "pypdf"),
        native_max_concurrency=4,
        ocr_engine="paddleocr",
        expected_script="auto",
        min_text_characters=4,
        engine_versions=PDFIUM_ENGINE_VERSIONS,
        rollout_state="canary",
        fallback_profile="pdfium_paddleocr_cpu",
        ocr_device="gpu:0",
        ocr_runtime="isolated-gpu-paddle-3.3.1",
        ocr_fallback_runtime="isolated-cpu-paddle-3.3.1",
        ocr_worker_command=_worker_command("QUOTE_PDF_GPU_OCR_WORKER"),
        ocr_fallback_worker_command=_worker_command("QUOTE_PDF_CPU_OCR_WORKER"),
    ),
}

GPU_CANARY_REQUIRED_CHECKS = frozenset({
    "all_cases_success",
    "chinese_quality",
    "complete_corpus",
    "confidence_coverage",
    "cuda_runtime",
    "document_p95",
    "gpu_profile_evaluated",
    "gpu_resource",
    "heading_quality",
    "no_undiagnosed_failures",
    "numeric_quality",
    "page_p95",
})
# A new PDFium-rendered expanded corpus approval is required. The old
# pypdf_paddleocr canary only establishes runtime viability.
GPU_CANARY_CORPUS_HASH = os.environ.get("QUOTE_PDF_GPU_CANARY_CORPUS_HASH", "")


def resolve_profile(name: str | None = None) -> PdfProfile:
    """Resolve a named profile from an explicit value or rollout environment."""
    selected = str(name or os.environ.get("QUOTE_PDF_ENGINE_PROFILE", "pdfium_native")).strip()
    try:
        profile = DEFAULT_PROFILES[selected]
    except KeyError as exc:
        raise ValueError(f"unknown PDF engine profile: {selected}") from exc
    if not profile.enabled or profile.rollout_state == "disabled":
        raise ValueError(f"PDF engine profile is disabled: {selected}")
    cache_dir = os.environ.get("QUOTE_PDF_OCR_CACHE_DIR") or profile.ocr_model_cache_dir
    gpu_worker = _worker_command("QUOTE_PDF_GPU_OCR_WORKER")
    cpu_worker = _worker_command("QUOTE_PDF_CPU_OCR_WORKER")
    if profile.ocr_device.startswith("gpu"):
        profile = replace(profile, ocr_model_cache_dir=cache_dir, ocr_worker_command=gpu_worker or profile.ocr_worker_command, ocr_fallback_worker_command=cpu_worker or profile.ocr_fallback_worker_command)
    else:
        profile = replace(profile, ocr_model_cache_dir=cache_dir, ocr_worker_command=cpu_worker or profile.ocr_worker_command)
    if profile.ocr_device.startswith("gpu"):
        _require_gpu_canary_approval(profile)
    return profile


def build_router(profile: PdfProfile, *, allow_unapproved_gpu_canary: bool = False):
    """Resolve adapters from a profile without vendor branches in consumers."""
    from .adapters import IsolatedNativeAdapter, PaddleOcrAdapter
    from .core import PdfRouter
    from .native_worker import get_shared_native_worker_pool

    if profile.ocr_device.startswith("gpu"):
        if not allow_unapproved_gpu_canary:
            _require_gpu_canary_approval(profile)
    native_pool = get_shared_native_worker_pool(
        max_workers=profile.native_max_concurrency,
        queue_size=profile.native_queue_size,
        queue_wait_seconds=profile.native_queue_wait_seconds,
        task_timeout_seconds=profile.limits.max_document_seconds,
        start_method=profile.native_start_method,
        max_restarts=profile.native_worker_restart_limit,
    )
    native_chain = tuple(
        IsolatedNativeAdapter(name, pool=native_pool, timeout_seconds=profile.limits.max_document_seconds)
        for name in profile.native_engines
    )
    ocr = (
        PaddleOcrAdapter(
            structure=profile.structure_pages,
            model_cache_dir=profile.ocr_model_cache_dir,
            device=profile.ocr_device,
            worker_command=profile.ocr_worker_command,
            fallback_worker_command=profile.ocr_fallback_worker_command,
            native_pool=native_pool,
        )
        if profile.ocr_engine == "paddleocr"
        else None
    )
    if profile.ocr_engine not in (None, "paddleocr"):
        raise ValueError(f"unsupported PDF OCR engine: {profile.ocr_engine}")
    return PdfRouter(native_chain=native_chain, ocr=ocr)


def _require_gpu_canary_approval(profile: PdfProfile) -> None:
    if os.environ.get("QUOTE_PDF_GPU_CANARY_APPROVED") != "1":
        raise ValueError(f"GPU PDF profile requires QUOTE_PDF_GPU_CANARY_APPROVED=1: {profile.name}")
    report_path = os.environ.get("QUOTE_PDF_GPU_CANARY_REPORT")
    if not report_path:
        raise ValueError("GPU PDF profile requires QUOTE_PDF_GPU_CANARY_REPORT")
    try:
        report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"GPU PDF canary report is unreadable: {report_path}") from exc
    checks = report.get("checks") or {}
    report_hash = report.get("corpus_hash")
    report_valid = (
        report.get("schema_version") == "pdf-gpu-canary-approval.v1"
        and report.get("profile") == profile.name
        and bool(GPU_CANARY_CORPUS_HASH)
        and report_hash == GPU_CANARY_CORPUS_HASH
        and GPU_CANARY_REQUIRED_CHECKS <= set(checks)
        and all(checks.get(name) is True for name in GPU_CANARY_REQUIRED_CHECKS)
        and report.get("gpu_canary_approved") is True
    )
    if not report_valid:
        raise ValueError("GPU PDF canary report has not passed every gate")


def _require_visible_cuda_runtime() -> None:
    """Compatibility helper retained for tests; GPU probing is worker-only."""
    raise ValueError("GPU PDF runtime must be probed through an isolated OCR worker")


def clear_gpu_runtime_probe_cache() -> None:
    """Drop the in-process GPU worker probe cache."""
    with _GPU_PROBE_LOCK:
        _GPU_PROBE_CACHE.clear()


def _gpu_runtime_probe_key(profile: PdfProfile) -> tuple[Any, ...]:
    return (
        tuple(profile.ocr_worker_command or ()),
        str(profile.ocr_runtime or ""),
        str(profile.ocr_model_cache_dir or ""),
        str(profile.ocr_device or ""),
    )


def _gpu_probe_failure_diagnostic(probe: Mapping[str, Any]) -> str:
    diagnostic = str(probe.get("diagnostic") or "").strip()
    if diagnostic:
        return diagnostic
    parts: list[str] = []
    if not probe.get("healthy"):
        parts.append(f"healthy={probe.get('healthy')}")
        if probe.get("paddleocr_version") is not None:
            parts.append(f"paddleocr={probe.get('paddleocr_version')}")
    if not probe.get("cuda_available"):
        parts.append(f"cuda_available={probe.get('cuda_available')}")
        if probe.get("device") is not None:
            parts.append(f"device={probe.get('device')}")
        if probe.get("cuda_device_count") is not None:
            parts.append(f"cuda_device_count={probe.get('cuda_device_count')}")
    if probe.get("model_cache_writable") is False:
        parts.append(f"model_cache_writable=false dir={probe.get('model_cache_dir')}")
    return "; ".join(parts) or "unspecified probe failure"


def _require_isolated_gpu_runtime(profile: PdfProfile, *, timeout_seconds: float | None = None) -> Mapping[str, Any]:
    from .adapters import PaddleOcrAdapter

    key = _gpu_runtime_probe_key(profile)
    with _GPU_PROBE_LOCK:
        cached = _GPU_PROBE_CACHE.get(key)
        if cached is None:
            probe = PaddleOcrAdapter.probe_runtime(profile, timeout_seconds=timeout_seconds)
            healthy = (
                bool(probe.get("healthy"))
                and bool(probe.get("cuda_available"))
                and probe.get("model_cache_writable") is not False
            )
            if healthy:
                _GPU_PROBE_CACHE[key] = probe
        else:
            probe = cached
            healthy = True

    if not healthy:
        diagnostic = _gpu_probe_failure_diagnostic(probe)
        logger.error("GPU PDF OCR worker probe failed: %s", diagnostic)
        raise ValueError(
            f"GPU PDF profile requires a healthy isolated CUDA OCR worker: {diagnostic}"
        )
    return probe
