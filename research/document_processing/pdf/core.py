"""Technical PDF extraction contract and bounded native-first router.

Business modules should consume these page results and retain ownership of
classification, table interpretation, and persistence. Optional OCR and
alternate-native adapters implement the small protocols below, so importing
this module never requires heavyweight OCR runtimes.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import time
import unicodedata
import warnings
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterable, Mapping, Optional, Protocol, Sequence

PDF_PARSER_SCHEMA_VERSION = "shared_pdf.v1"
PDF_PARSER_VERSION = "shared-pdf-pypdf.v1"


def compute_content_hash(content: bytes) -> str:
    return hashlib.sha256(bytes(content)).hexdigest()


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PdfDiagnostic:
    code: str
    message: str = ""
    page_number: Optional[int] = None
    severity: str = "warning"
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PdfResourceLimits:
    max_pages: Optional[int] = None
    max_ocr_pages: Optional[int] = None
    max_concurrency: int = 1
    max_queue_size: int = 32
    max_queue_wait_seconds: float = 30.0
    max_page_seconds: float = 120.0
    max_document_seconds: float = 900.0
    ocr_batch_size: int = 1
    render_dpi: int = 150

    def __post_init__(self) -> None:
        if self.max_concurrency < 1 or self.max_queue_size < 1:
            raise ValueError("OCR concurrency and queue size must be positive")
        if self.ocr_batch_size < 1 or self.render_dpi < 72:
            raise ValueError("invalid OCR batch size or render DPI")


@dataclass(frozen=True)
class PdfProfile:
    name: str = "pypdf_native"
    native_engine: str = "pypdf"
    alternate_native_engine: Optional[str] = None
    ocr_engine: Optional[str] = None
    enabled: bool = True
    rollout_state: str = "active"
    min_text_characters: int = 20
    mapping_check: bool = True
    ocr_on_low_text: bool = True
    structure_pages: bool = False
    limits: PdfResourceLimits = field(default_factory=PdfResourceLimits)
    parser_config_version: str = "pdf-profile.v1"
    engine_versions: Mapping[str, str] = field(default_factory=dict)
    fallback_profile: Optional[str] = None
    ocr_model_cache_dir: Optional[str] = None


@dataclass(frozen=True)
class PdfParseRequest:
    content: bytes
    expected_content_hash: Optional[str] = None
    target_pages: tuple[int, ...] = ()
    profile: PdfProfile = field(default_factory=lambda: _resolve_default_profile())
    parameter_overrides: Mapping[str, Any] = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        return compute_content_hash(self.content)

    @property
    def parameter_hash(self) -> str:
        return _stable_hash(
            {
                "profile": asdict(self.profile),
                "target_pages": self.target_pages,
                "overrides": dict(self.parameter_overrides),
            }
        )

    def __post_init__(self) -> None:
        if self.expected_content_hash and self.expected_content_hash != self.content_hash:
            raise ValueError("expected_content_hash does not match PDF bytes")
        pages = tuple(sorted({int(page) for page in self.target_pages}))
        if any(page < 1 for page in pages):
            raise ValueError("target_pages must be one-based positive integers")
        object.__setattr__(self, "target_pages", pages)


def _resolve_default_profile() -> PdfProfile:
    """Resolve the configured rollout profile without importing it at module load."""
    try:
        from .profiles import resolve_profile
    except ImportError:
        return PdfProfile()
    return resolve_profile()


@dataclass(frozen=True)
class PdfPageResult:
    page_number: int
    text: str
    extraction_method: str
    quality_status: str
    text_hash: str
    page_result_hash: str
    diagnostics: tuple[PdfDiagnostic, ...] = ()
    confidence: Optional[float] = None
    provenance: tuple[Mapping[str, Any], ...] = ()
    elapsed_seconds: float = 0.0
    ocr_required: bool = False
    width_points: Optional[float] = None
    height_points: Optional[float] = None


@dataclass(frozen=True)
class PdfDocumentResult:
    schema_version: str
    parser_version: str
    profile_name: str
    content_hash: str
    parameter_hash: str
    page_count: int
    pages: tuple[PdfPageResult, ...]
    status: str
    diagnostics: tuple[PdfDiagnostic, ...] = ()
    elapsed_seconds: float = 0.0
    engine_versions: Mapping[str, str] = field(default_factory=dict)


class NativeAdapter(Protocol):
    name: str

    def extract(self, content: bytes, *, target_pages: Sequence[int] = ()) -> "NativeResult": ...


class OcrAdapter(Protocol):
    name: str

    def extract_pages(
        self,
        content: bytes,
        pages: Sequence[int],
        *,
        request: PdfParseRequest,
    ) -> Mapping[int, "OcrPage"]: ...


@dataclass(frozen=True)
class NativePage:
    page_number: int
    text: str
    elapsed_seconds: float
    diagnostics: tuple[PdfDiagnostic, ...] = ()
    width_points: Optional[float] = None
    height_points: Optional[float] = None


@dataclass(frozen=True)
class NativeResult:
    page_count: int
    pages: tuple[NativePage, ...]
    diagnostics: tuple[PdfDiagnostic, ...] = ()
    engine_version: str = PDF_PARSER_VERSION


@dataclass(frozen=True)
class OcrPage:
    text: str
    confidence: Optional[float] = None
    elapsed_seconds: float = 0.0
    diagnostics: tuple[PdfDiagnostic, ...] = ()
    provenance: Mapping[str, Any] = field(default_factory=dict)


def detect_text_quality(text: str, *, min_characters: int = 20) -> tuple[str, tuple[PdfDiagnostic, ...]]:
    """Classify native text, including valid-Unicode ToUnicode mojibake.

    The mapping check intentionally does not depend on U+FFFD or heading
    matches: broken CMaps commonly decode to legal Kannada/Georgian/etc.
    characters. It is a conservative routing signal, not a language detector.
    """
    value = str(text or "")
    compact = re.sub(r"\s+", "", value)
    if not compact:
        return "empty", (PdfDiagnostic("empty_page", "native extraction returned no text"),)
    controls = sum(1 for ch in compact if unicodedata.category(ch).startswith("C"))
    replacement = value.count("\ufffd")
    cjk = sum(1 for ch in compact if "CJK UNIFIED" in unicodedata.name(ch, ""))
    letters = sum(1 for ch in compact if unicodedata.category(ch).startswith("L"))
    suspicious_scripts = sum(
        1
        for ch in compact
        if unicodedata.category(ch).startswith("L")
        and not ("CJK UNIFIED" in unicodedata.name(ch, "") or ch.isascii())
    )
    diagnostics: list[PdfDiagnostic] = []
    if replacement or controls:
        diagnostics.append(PdfDiagnostic("suspicious_glyph_decoding", "replacement/control glyphs detected", details={"replacement": replacement, "controls": controls}))
    # Chinese/English announcement pages normally contain CJK or ASCII. A
    # dense run of unrelated valid scripts is a strong ToUnicode signal.
    mapping_ratio = suspicious_scripts / max(letters, 1)
    if (mapping_ratio >= 0.20 and cjk == 0 and len(compact) >= max(20, min_characters)) or replacement or controls:
        diagnostics.append(PdfDiagnostic("native_text_mapping_error", "native text likely has a broken ToUnicode/CMap mapping", details={"suspicious_script_ratio": round(mapping_ratio, 4)}))
        return "native_text_mapping_error", tuple(diagnostics)
    if len(compact) < min_characters:
        diagnostics.append(PdfDiagnostic("low_text_page", "native text below configured quality threshold", details={"characters": len(compact), "threshold": min_characters}))
        return "low_text", tuple(diagnostics)
    return "usable", tuple(diagnostics)


class PypdfNativeAdapter:
    name = "pypdf"
    version = PDF_PARSER_VERSION

    def extract(self, content: bytes, *, target_pages: Sequence[int] = ()) -> NativeResult:
        started = time.perf_counter()
        raw = bytes(content)
        if not raw.lstrip().startswith(b"%PDF-"):
            return NativeResult(0, (), (PdfDiagnostic("invalid_pdf_signature", "input does not start with %PDF-", severity="error"),), self.version)
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            return NativeResult(0, (), (PdfDiagnostic("native_runtime_unavailable", str(exc), severity="error"),), self.version)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                reader = PdfReader(io.BytesIO(raw), strict=False)
                if reader.is_encrypted and not reader.decrypt(""):
                    return NativeResult(0, (), (PdfDiagnostic("encrypted_password_required", "PDF requires a password", severity="error"),), self.version)
                count = len(reader.pages)
                if count < 1:
                    return NativeResult(0, (), (PdfDiagnostic("empty_pdf", "PDF contains no pages", severity="error"),), self.version)
                requested = set(int(p) for p in target_pages) if target_pages else set(range(1, count + 1))
                pages: list[NativePage] = []
                document_diags: list[PdfDiagnostic] = []
                for warning in caught:
                    document_diags.append(PdfDiagnostic("pypdf_warning", str(warning.message)))
                for number in sorted(requested):
                    if number < 1 or number > count:
                        document_diags.append(PdfDiagnostic("target_page_out_of_range", f"page {number} is outside 1..{count}", number, "error"))
                        continue
                    page_started = time.perf_counter()
                    try:
                        text = reader.pages[number - 1].extract_text() or ""
                        try:
                            width = float(reader.pages[number - 1].mediabox.width)
                            height = float(reader.pages[number - 1].mediabox.height)
                        except (AttributeError, TypeError, ValueError):
                            width = height = None
                        pages.append(NativePage(number, text, time.perf_counter() - page_started, width_points=width, height_points=height))
                    except Exception as exc:
                        pages.append(NativePage(number, "", time.perf_counter() - page_started, (PdfDiagnostic("native_extraction_error", f"{type(exc).__name__}: {exc}", number, "error"),)))
                return NativeResult(count, tuple(pages), tuple(document_diags), self.version)
        except Exception as exc:
            return NativeResult(0, (), (PdfDiagnostic("malformed_pdf", f"{type(exc).__name__}: {exc}", severity="error"),), self.version)


class PdfRouter:
    """Native-first router with optional alternate-native and OCR adapters."""

    def __init__(self, *, native: Optional[NativeAdapter] = None, alternate_native: Optional[NativeAdapter] = None, ocr: Optional[OcrAdapter] = None, quality_detector: Callable[..., tuple[str, tuple[PdfDiagnostic, ...]]] = detect_text_quality) -> None:
        self.native = native or PypdfNativeAdapter()
        self.alternate_native = alternate_native
        self.ocr = ocr
        self.quality_detector = quality_detector

    def parse(self, request: PdfParseRequest) -> PdfDocumentResult:
        started = time.perf_counter()
        profile = request.profile
        if not profile.enabled or profile.rollout_state == "disabled":
            return self._failed(request, "profile_disabled", "PDF profile is disabled", started)
        native = self.native.extract(request.content, target_pages=request.target_pages)
        if native.page_count == 0:
            return PdfDocumentResult(PDF_PARSER_SCHEMA_VERSION, PDF_PARSER_VERSION, profile.name, request.content_hash, request.parameter_hash, 0, (), "failed", native.diagnostics, time.perf_counter() - started)
        limit = profile.limits.max_pages
        pages = list(native.pages[:limit]) if limit else list(native.pages)
        results: dict[int, PdfPageResult] = {}
        ocr_targets: list[int] = []
        alternate_targets: list[int] = []
        for item in pages:
            quality, diags = self.quality_detector(item.text, min_characters=profile.min_text_characters)
            all_diags = tuple(item.diagnostics) + tuple(diags)
            requested = item.page_number in request.target_pages
            needs_ocr = requested or quality in {"empty", "low_text", "native_text_mapping_error"}
            if quality == "native_text_mapping_error" and self.alternate_native and profile.alternate_native_engine:
                alternate_targets.append(item.page_number)
            elif needs_ocr and profile.ocr_engine:
                ocr_targets.append(item.page_number)
            page_hash = _stable_hash({"page": item.page_number, "method": "native_text", "text_hash": compute_content_hash(item.text.encode())})
            results[item.page_number] = PdfPageResult(item.page_number, item.text, "native_text", quality, compute_content_hash(item.text.encode()), page_hash, all_diags, elapsed_seconds=item.elapsed_seconds, ocr_required=needs_ocr, width_points=item.width_points, height_points=item.height_points)
        if alternate_targets and self.alternate_native:
            alternate = self.alternate_native.extract(request.content, target_pages=alternate_targets)
            for item in alternate.pages:
                quality, diags = self.quality_detector(item.text, min_characters=profile.min_text_characters)
                if quality == "usable":
                    previous = results[item.page_number]
                    results[item.page_number] = self._page(item.page_number, item.text, "alternate_native", quality, previous.diagnostics + tuple(diags), item.elapsed_seconds, previous.provenance + (({"engine": getattr(self.alternate_native, "name", "alternate_native")},)), width_points=previous.width_points, height_points=previous.height_points)
                elif profile.ocr_engine:
                    ocr_targets.append(item.page_number)
        all_ocr_targets = sorted(set(ocr_targets))
        queue_targets = all_ocr_targets[: profile.limits.max_queue_size]
        queue_deferred = all_ocr_targets[len(queue_targets):]
        ocr_targets = queue_targets[: profile.limits.max_ocr_pages or None]
        deferred_targets = queue_deferred + queue_targets[len(ocr_targets):]
        for number in deferred_targets:
            previous = results[number]
            code = "ocr_queue_full" if number in queue_deferred else "ocr_page_budget_exceeded"
            message = "OCR queue bound reached" if code == "ocr_queue_full" else "maximum OCR pages exceeded"
            results[number] = self._page(number, previous.text, "ocr_deferred", "ocr_budget_exceeded", previous.diagnostics + (PdfDiagnostic(code, message, number, "error"),), previous.elapsed_seconds, previous.provenance, width_points=previous.width_points, height_points=previous.height_points)
        if ocr_targets and self.ocr and profile.ocr_engine:
            try:
                ocr_pages = self.ocr.extract_pages(request.content, ocr_targets, request=request)
            except Exception as exc:
                ocr_pages = {}
                for number in ocr_targets:
                    previous = results[number]
                    results[number] = self._page(number, previous.text, "ocr_failed", "ocr_failure", previous.diagnostics + (PdfDiagnostic("ocr_runtime_failure", f"{type(exc).__name__}: {exc}", number, "error"),), previous.elapsed_seconds, previous.provenance, width_points=previous.width_points, height_points=previous.height_points)
            for number in ocr_targets:
                item = ocr_pages.get(number)
                if item is None:
                    continue
                previous = results[number]
                quality, diags = self.quality_detector(item.text, min_characters=profile.min_text_characters)
                status = "ocr_low_confidence" if item.confidence is not None and item.confidence < 0.6 else ("ocr_success" if quality == "usable" else "ocr_low_quality")
                results[number] = self._page(number, item.text, "ocr", status, previous.diagnostics + tuple(item.diagnostics) + tuple(diags), item.elapsed_seconds, previous.provenance + (({"engine": getattr(self.ocr, "name", profile.ocr_engine), **dict(item.provenance)},)), item.confidence, width_points=previous.width_points, height_points=previous.height_points)
        elif ocr_targets:
            for number in ocr_targets:
                previous = results[number]
                results[number] = self._page(number, previous.text, "ocr_deferred", "ocr_deferred", previous.diagnostics + (PdfDiagnostic("ocr_unavailable", "configured OCR adapter is unavailable", number, "error"),), previous.elapsed_seconds, previous.provenance, width_points=previous.width_points, height_points=previous.height_points)
        elapsed = time.perf_counter() - started
        document_diags = list(native.diagnostics)
        if elapsed > profile.limits.max_document_seconds:
            document_diags.append(PdfDiagnostic("document_time_budget_exceeded", "PDF processing exceeded configured document budget", severity="error", details={"elapsed_seconds": elapsed}))
        status = "partial" if (limit and len(pages) < native.page_count) or deferred_targets or elapsed > profile.limits.max_document_seconds else ("success" if all(page.quality_status not in {"ocr_failure", "ocr_deferred", "ocr_budget_exceeded"} for page in results.values()) else "partial")
        return PdfDocumentResult(PDF_PARSER_SCHEMA_VERSION, PDF_PARSER_VERSION, profile.name, request.content_hash, request.parameter_hash, native.page_count, tuple(results[number] for number in sorted(results)), status, tuple(document_diags), elapsed, {"native": native.engine_version, **dict(profile.engine_versions)})

    @staticmethod
    def _page(number: int, text: str, method: str, status: str, diagnostics: tuple[PdfDiagnostic, ...], elapsed: float, provenance: tuple[Mapping[str, Any], ...], confidence: Optional[float] = None, *, width_points: Optional[float] = None, height_points: Optional[float] = None) -> PdfPageResult:
        text_hash = compute_content_hash(text.encode("utf-8"))
        return PdfPageResult(number, text, method, status, text_hash, _stable_hash({"page": number, "method": method, "text_hash": text_hash}), diagnostics, confidence, provenance, elapsed, True, width_points, height_points)

    @staticmethod
    def _failed(request: PdfParseRequest, code: str, message: str, started: float) -> PdfDocumentResult:
        return PdfDocumentResult(PDF_PARSER_SCHEMA_VERSION, PDF_PARSER_VERSION, request.profile.name, request.content_hash, request.parameter_hash, 0, (), "failed", (PdfDiagnostic(code, message, severity="error"),), time.perf_counter() - started)
