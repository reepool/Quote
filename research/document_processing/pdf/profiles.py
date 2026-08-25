"""Configuration helpers for named PDF engine profiles."""

from __future__ import annotations

from dataclasses import fields
import os
from typing import Any, Mapping

from .core import PdfProfile, PdfResourceLimits


def profile_from_mapping(name: str, raw: Mapping[str, Any]) -> PdfProfile:
    """Build and validate a profile from JSON/YAML-like configuration."""
    limits_raw = dict(raw.get("limits") or {})
    allowed_limits = {item.name for item in fields(PdfResourceLimits)}
    unknown_limits = set(limits_raw) - allowed_limits
    if unknown_limits:
        raise ValueError(f"unknown PDF resource limits: {sorted(unknown_limits)}")
    limits = PdfResourceLimits(**limits_raw)
    allowed = {item.name for item in fields(PdfProfile)} - {"name"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown PDF profile keys: {sorted(unknown)}")
    profile = PdfProfile(name=name, limits=limits, **{key: raw[key] for key in raw if key != "limits"})
    if profile.rollout_state not in {"shadow", "canary", "active", "disabled"}:
        raise ValueError("rollout_state must be shadow, canary, active, or disabled")
    return profile


DEFAULT_PROFILES = {
    "pypdf_native": PdfProfile(name="pypdf_native"),
    "pypdf_paddleocr": PdfProfile(name="pypdf_paddleocr", alternate_native_engine="pdf-inspector", ocr_engine="paddleocr", fallback_profile="pypdf_native"),
    "pdf_inspector_paddleocr": PdfProfile(name="pdf_inspector_paddleocr", native_engine="pdf-inspector", alternate_native_engine="pypdf", ocr_engine="paddleocr", rollout_state="shadow", fallback_profile="pypdf_paddleocr"),
}


def resolve_profile(name: str | None = None) -> PdfProfile:
    """Resolve a named profile from an explicit value or rollout environment."""
    selected = str(name or os.environ.get("QUOTE_PDF_ENGINE_PROFILE", "pypdf_native")).strip()
    try:
        profile = DEFAULT_PROFILES[selected]
    except KeyError as exc:
        raise ValueError(f"unknown PDF engine profile: {selected}") from exc
    if not profile.enabled or profile.rollout_state == "disabled":
        raise ValueError(f"PDF engine profile is disabled: {selected}")
    return profile


def build_router(profile: PdfProfile):
    """Resolve adapters from a profile without vendor branches in consumers."""
    from .adapters import PaddleOcrAdapter, PdfInspectorNativeAdapter
    from .core import PdfRouter, PypdfNativeAdapter

    def native_adapter(name: str):
        if name == "pypdf":
            return PypdfNativeAdapter()
        if name == "pdf-inspector":
            return PdfInspectorNativeAdapter()
        raise ValueError(f"unsupported PDF native engine: {name}")

    native = native_adapter(profile.native_engine)
    alternate = native_adapter(profile.alternate_native_engine) if profile.alternate_native_engine else None
    ocr = (
        PaddleOcrAdapter(
            structure=profile.structure_pages,
            model_cache_dir=profile.ocr_model_cache_dir,
        )
        if profile.ocr_engine == "paddleocr"
        else None
    )
    if profile.ocr_engine not in (None, "paddleocr"):
        raise ValueError(f"unsupported PDF OCR engine: {profile.ocr_engine}")
    return PdfRouter(native=native, alternate_native=alternate, ocr=ocr)
