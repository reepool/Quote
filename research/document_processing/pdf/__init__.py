"""Versioned, profile-driven PDF extraction shared by research consumers."""

from .core import (
    PDF_PARSER_SCHEMA_VERSION,
    PdfDiagnostic,
    PdfDocumentResult,
    PdfPageResult,
    PdfParseRequest,
    PdfProfile,
    PdfResourceLimits,
    PdfRouter,
    PypdfNativeAdapter,
    compute_content_hash,
    detect_text_quality,
)
from .adapters import PaddleOcrAdapter, PdfInspectorNativeAdapter
from .profiles import DEFAULT_PROFILES, build_router, profile_from_mapping, resolve_profile
from .evaluation import MANDATORY_600036_CASE, PdfAcceptanceGates, PdfEvaluationCase, assess_report, evaluate_cases, load_manifest, write_report

__all__ = [
    "PDF_PARSER_SCHEMA_VERSION",
    "PdfDiagnostic",
    "PdfDocumentResult",
    "PdfPageResult",
    "PdfParseRequest",
    "PdfProfile",
    "PdfResourceLimits",
    "PdfRouter",
    "PypdfNativeAdapter",
    "compute_content_hash",
    "detect_text_quality",
    "PaddleOcrAdapter",
    "PdfInspectorNativeAdapter",
    "DEFAULT_PROFILES",
    "profile_from_mapping",
    "build_router",
    "resolve_profile",
    "PdfEvaluationCase",
    "PdfAcceptanceGates",
    "load_manifest",
    "evaluate_cases",
    "assess_report",
    "write_report",
    "MANDATORY_600036_CASE",
]
