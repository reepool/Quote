"""Configuration helpers for named PDF engine profiles."""

from __future__ import annotations

import json
import os
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, Mapping

from .core import DEFAULT_MODE_BUDGETS, PdfProfile, PdfResourceLimits


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


DEFAULT_PROFILES = {
    "pypdf_native": PdfProfile(name="pypdf_native"),
    "pypdf_paddleocr": PdfProfile(name="pypdf_paddleocr", alternate_native_engine="pdf-inspector", ocr_engine="paddleocr", fallback_profile="pypdf_native"),
    "pdf_inspector_paddleocr": PdfProfile(name="pdf_inspector_paddleocr", native_engine="pdf-inspector", alternate_native_engine="pypdf", ocr_engine="paddleocr", rollout_state="shadow", fallback_profile="pypdf_paddleocr"),
    "pypdf_paddleocr_gpu_canary": PdfProfile(name="pypdf_paddleocr_gpu_canary", alternate_native_engine="pdf-inspector", ocr_engine="paddleocr", rollout_state="canary", fallback_profile="pypdf_paddleocr", ocr_device="gpu:0"),
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
GPU_CANARY_CORPUS_HASH = "55ae9d356e82e40d5dc65065ae7c25b31b7e51b250cfd9672312c8c09883e52a"


def resolve_profile(name: str | None = None) -> PdfProfile:
    """Resolve a named profile from an explicit value or rollout environment."""
    selected = str(name or os.environ.get("QUOTE_PDF_ENGINE_PROFILE", "pypdf_native")).strip()
    try:
        profile = DEFAULT_PROFILES[selected]
    except KeyError as exc:
        raise ValueError(f"unknown PDF engine profile: {selected}") from exc
    if not profile.enabled or profile.rollout_state == "disabled":
        raise ValueError(f"PDF engine profile is disabled: {selected}")
    cache_dir = os.environ.get("QUOTE_PDF_OCR_CACHE_DIR") or profile.ocr_model_cache_dir
    profile = replace(profile, ocr_model_cache_dir=cache_dir)
    if profile.ocr_device.startswith("gpu"):
        _require_gpu_canary_approval(profile)
    return profile


def build_router(profile: PdfProfile, *, allow_unapproved_gpu_canary: bool = False):
    """Resolve adapters from a profile without vendor branches in consumers."""
    from .adapters import PaddleOcrAdapter, PdfInspectorNativeAdapter
    from .core import PdfRouter, PypdfNativeAdapter

    def native_adapter(name: str):
        if name == "pypdf":
            return PypdfNativeAdapter()
        if name == "pdf-inspector":
            return PdfInspectorNativeAdapter()
        raise ValueError(f"unsupported PDF native engine: {name}")

    if profile.ocr_device.startswith("gpu"):
        if allow_unapproved_gpu_canary:
            _require_visible_cuda_runtime()
        else:
            _require_gpu_canary_approval(profile)
    native = native_adapter(profile.native_engine)
    alternate = native_adapter(profile.alternate_native_engine) if profile.alternate_native_engine else None
    ocr = (
        PaddleOcrAdapter(
            structure=profile.structure_pages,
            model_cache_dir=profile.ocr_model_cache_dir,
            device=profile.ocr_device,
        )
        if profile.ocr_engine == "paddleocr"
        else None
    )
    if profile.ocr_engine not in (None, "paddleocr"):
        raise ValueError(f"unsupported PDF OCR engine: {profile.ocr_engine}")
    return PdfRouter(native=native, alternate_native=alternate, ocr=ocr)


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
        and report_hash == GPU_CANARY_CORPUS_HASH
        and GPU_CANARY_REQUIRED_CHECKS <= set(checks)
        and all(checks.get(name) is True for name in GPU_CANARY_REQUIRED_CHECKS)
        and report.get("gpu_canary_approved") is True
    )
    if not report_valid:
        raise ValueError("GPU PDF canary report has not passed every gate")
    _require_visible_cuda_runtime()


def _require_visible_cuda_runtime() -> None:
    try:
        import paddle

        if not paddle.is_compiled_with_cuda() or paddle.device.cuda.device_count() < 1:
            raise ValueError("GPU PDF profile requires a CUDA-enabled Paddle runtime and visible device")
    except ImportError as exc:
        raise ValueError("GPU PDF profile requires PaddlePaddle GPU runtime") from exc
