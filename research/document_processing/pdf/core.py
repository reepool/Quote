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
from dataclasses import asdict, dataclass, field, replace
from typing import (
    Any,
    Callable,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
)

PDF_PARSER_SCHEMA_VERSION = "shared_pdf.v3"
PDF_PARSER_VERSION = "shared-pdf-router.v3"


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
    enforce_hard_timeout: bool = True

    def __post_init__(self) -> None:
        if self.max_concurrency < 1 or self.max_queue_size < 1:
            raise ValueError("OCR concurrency and queue size must be positive")
        if self.ocr_batch_size < 1 or self.render_dpi < 72:
            raise ValueError("invalid OCR batch size or render DPI")


PdfOcrMode = Literal["none", "toc_probe", "section_extract", "table_extract"]
PdfRecoveryPolicy = Literal["native_first", "selective_recovery", "force_ocr"]


DEFAULT_MODE_BUDGETS: Mapping[str, "PdfResourceLimits"] = {
    "toc_probe": PdfResourceLimits(max_ocr_pages=5, max_page_seconds=120.0, max_document_seconds=180.0),
    "section_extract": PdfResourceLimits(max_ocr_pages=20, max_page_seconds=120.0, max_document_seconds=900.0),
    "table_extract": PdfResourceLimits(max_ocr_pages=8, max_page_seconds=120.0, max_document_seconds=600.0),
}


@dataclass(frozen=True)
class PdfProfile:
    name: str = "pypdf_native"
    native_engines: tuple[str, ...] = ("pypdf",)
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
    mode_budgets: Mapping[str, PdfResourceLimits] = field(default_factory=dict)
    ocr_min_confidence: float = 0.60
    ocr_device: str = "cpu"
    expected_script: str = "auto"
    min_expected_script_characters: int = 1
    ocr_runtime: str = "quote-local"
    ocr_fallback_runtime: Optional[str] = None
    ocr_worker_command: tuple[str, ...] = ()
    ocr_fallback_worker_command: tuple[str, ...] = ()
    ocr_fallback_failure_codes: tuple[str, ...] = (
        "ocr_unavailable",
        "ocr_runtime_unavailable",
        "ocr_worker_startup_failed",
        "ocr_model_unhealthy",
    )
    ocr_inference_config: Mapping[str, Any] = field(default_factory=dict)
    native_max_concurrency: int = 1
    native_queue_size: int = 32
    native_queue_wait_seconds: float = 30.0
    native_worker_restart_limit: int = 3
    native_start_method: str = "spawn"

    def __post_init__(self) -> None:
        engines = tuple(str(engine).strip() for engine in self.native_engines if str(engine).strip())
        if not engines:
            raise ValueError("native_engines must contain at least one engine")
        if len(set(engines)) != len(engines):
            raise ValueError("native_engines must not contain duplicates")
        if self.min_expected_script_characters < 1:
            raise ValueError("min_expected_script_characters must be positive")
        if self.native_max_concurrency < 1 or self.native_queue_size < 1 or self.native_worker_restart_limit < 0:
            raise ValueError("native worker limits must be positive")
        object.__setattr__(self, "native_engines", engines)
        object.__setattr__(self, "ocr_worker_command", tuple(self.ocr_worker_command))
        object.__setattr__(self, "ocr_fallback_worker_command", tuple(self.ocr_fallback_worker_command))


@dataclass(frozen=True)
class PdfParseRequest:
    content: bytes
    expected_content_hash: Optional[str] = None
    target_pages: tuple[int, ...] = ()
    profile: PdfProfile = field(default_factory=lambda: _resolve_default_profile())
    ocr_mode: PdfOcrMode = "none"
    recovery_policy: PdfRecoveryPolicy = "native_first"
    mode_budget: Optional[PdfResourceLimits] = None
    cache_backend: Optional["PdfPageCacheBackend"] = None
    parameter_overrides: Mapping[str, Any] = field(default_factory=dict)

    requested_pages: tuple[int, ...] = field(init=False, default=())

    @property
    def content_hash(self) -> str:
        return compute_content_hash(self.content)

    @property
    def parameter_hash(self) -> str:
        return _stable_hash(
            {
                "profile": asdict(self.profile),
                "target_pages": self.target_pages,
                "ocr_mode": self.ocr_mode,
                "recovery_policy": self.recovery_policy,
                "mode_budget": asdict(self.mode_budget) if self.mode_budget else None,
                "overrides": dict(self.parameter_overrides),
            }
        )

    def __post_init__(self) -> None:
        if self.expected_content_hash and self.expected_content_hash != self.content_hash:
            raise ValueError("expected_content_hash does not match PDF bytes")
        if self.ocr_mode not in {"none", "toc_probe", "section_extract", "table_extract"}:
            raise ValueError(f"unsupported ocr_mode: {self.ocr_mode}")
        if self.recovery_policy not in {"native_first", "selective_recovery", "force_ocr"}:
            raise ValueError(f"unsupported recovery_policy: {self.recovery_policy}")
        raw_pages = tuple(int(page) for page in self.target_pages)
        pages = tuple(sorted(set(raw_pages)))
        if any(page < 1 for page in pages):
            raise ValueError("target_pages must be one-based positive integers")
        if self.recovery_policy == "force_ocr" and not pages:
            raise ValueError("force_ocr requires explicit target_pages")
        object.__setattr__(self, "requested_pages", raw_pages)
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
    selected_text: str = ""
    selected_method: str = "none"
    selected_text_hash: str = ""
    selected_usable_for_semantic: bool = False
    candidates: tuple["PdfCandidate", ...] = ()
    cache_identity: Mapping[str, Any] = field(default_factory=dict)
    cache_status: str = "cache_miss"
    structured_payload: Any = None
    structured_format: Optional[str] = None
    pdf_page_label: Optional[str] = None
    printed_page_label: Optional[str] = None
    bookmark_title: Optional[str] = None


@dataclass(frozen=True)
class PdfCandidate:
    method: str
    text: str
    text_hash: str
    quality_status: str
    usable_for_semantic: bool
    confidence: Optional[float] = None
    elapsed_seconds: float = 0.0
    diagnostics: tuple[PdfDiagnostic, ...] = ()
    provenance: Mapping[str, Any] = field(default_factory=dict)


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
    requested_pages: tuple[int, ...] = ()
    returned_pages: tuple[int, ...] = ()
    page_diagnostics: Mapping[int, tuple[PdfDiagnostic, ...]] = field(default_factory=dict)


class NativeAdapter(Protocol):
    name: str

    def extract(self, content: bytes, *, target_pages: Sequence[int] = (), progress_callback: Optional[Callable[["NativePage"], None]] = None, page_count_callback: Optional[Callable[[int], None]] = None) -> "NativeResult": ...


class OcrAdapter(Protocol):
    name: str

    def extract_pages(
        self,
        content: bytes,
        pages: Sequence[int],
        *,
        request: PdfParseRequest,
    ) -> Mapping[int, "OcrPage"]: ...


class PdfPageCacheBackend(Protocol):
    def get(self, cache_identity: Mapping[str, Any]) -> Optional["PdfPageResult | Mapping[str, Any]"]: ...

    def put(self, cache_identity: Mapping[str, Any], page_result: "PdfPageResult") -> None: ...


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
    structured_payload: Any = None
    structured_format: Optional[str] = None


def detect_text_quality(
    text: str,
    *,
    min_characters: int = 20,
    expected_script: str = "auto",
    min_expected_script_characters: int = 1,
) -> tuple[str, tuple[PdfDiagnostic, ...]]:
    """Classify native text, including valid-Unicode ToUnicode mojibake.

    The mapping check intentionally does not depend on U+FFFD or heading
    matches: broken CMaps commonly decode to legal Kannada/Georgian/etc.
    characters. It is a conservative routing signal, not a language detector.
    """
    value = str(text or "")
    compact = re.sub(r"\s+", "", value)
    if not compact:
        return "empty", (PdfDiagnostic("empty_page", "native extraction returned no text"),)
    # Private-use glyphs commonly encode checkboxes in normal filings. Only
    # actual control/format code points are a decoding failure signal.
    controls = sum(1 for ch in compact if unicodedata.category(ch) in {"Cc", "Cf"})
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
    if expected_script.lower() in {"cjk", "chinese", "zh"} and cjk < min_expected_script_characters:
        diagnostics.append(
            PdfDiagnostic(
                "native_text_expected_script_missing",
                "native text does not contain the configured Chinese-script evidence",
                details={
                    "expected_script": expected_script,
                    "cjk_characters": cjk,
                    "threshold": min_expected_script_characters,
                },
            )
        )
        return "native_text_expected_script_missing", tuple(diagnostics)
    return "usable", tuple(diagnostics)


class PypdfNativeAdapter:
    name = "pypdf"
    version = PDF_PARSER_VERSION

    def extract(self, content: bytes, *, target_pages: Sequence[int] = (), progress_callback: Optional[Callable[[NativePage], None]] = None, page_count_callback: Optional[Callable[[int], None]] = None) -> NativeResult:
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
                if page_count_callback is not None:
                    page_count_callback(count)
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
                        item = NativePage(number, text, time.perf_counter() - page_started, width_points=width, height_points=height)
                        pages.append(item)
                        if progress_callback is not None:
                            progress_callback(item)
                    except Exception as exc:
                        item = NativePage(number, "", time.perf_counter() - page_started, (PdfDiagnostic("native_extraction_error", f"{type(exc).__name__}: {exc}", number, "error"),))
                        pages.append(item)
                        if progress_callback is not None:
                            progress_callback(item)
                return NativeResult(count, tuple(pages), tuple(document_diags), self.version)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            code = "encrypted_password_required" if "password" in message.lower() else "malformed_pdf"
            return NativeResult(0, (), (PdfDiagnostic(code, message, severity="error"),), self.version)


class PdfiumNativeAdapter:
    """PDFium text extractor using one open document per adapter request."""

    name = "pypdfium2"

    @property
    def version(self) -> str:
        try:
            from importlib.metadata import version

            return f"pypdfium2-{version('pypdfium2')}"
        except Exception:
            return "pypdfium2-unknown"

    def extract(self, content: bytes, *, target_pages: Sequence[int] = (), progress_callback: Optional[Callable[[NativePage], None]] = None, page_count_callback: Optional[Callable[[int], None]] = None) -> NativeResult:
        raw = bytes(content)
        if not raw.lstrip().startswith(b"%PDF-"):
            return NativeResult(0, (), (PdfDiagnostic("invalid_pdf_signature", "input does not start with %PDF-", severity="error"),), self.version)
        try:
            import pypdfium2 as pdfium
        except ImportError as exc:
            return NativeResult(0, (), (PdfDiagnostic("native_runtime_unavailable", str(exc), severity="error"),), self.version)
        document = None
        try:
            document = pdfium.PdfDocument(raw)
            count = len(document)
            if page_count_callback is not None:
                page_count_callback(count)
            if count < 1:
                return NativeResult(0, (), (PdfDiagnostic("empty_pdf", "PDF contains no pages", severity="error"),), self.version)
            requested = tuple(sorted(set(int(page) for page in target_pages))) if target_pages else tuple(range(1, count + 1))
            pages: list[NativePage] = []
            diagnostics: list[PdfDiagnostic] = []
            for number in requested:
                if number < 1 or number > count:
                    diagnostics.append(PdfDiagnostic("target_page_out_of_range", f"page {number} is outside 1..{count}", number, "error"))
                    continue
                started = time.perf_counter()
                page = text_page = None
                try:
                    page = document.get_page(number - 1)
                    text_page = page.get_textpage()
                    text = text_page.get_text_range() or ""
                    width, height = page.get_size()
                    item = NativePage(number, text, time.perf_counter() - started, width_points=float(width), height_points=float(height))
                    pages.append(item)
                    if progress_callback is not None:
                        progress_callback(item)
                except Exception as exc:
                    item = NativePage(number, "", time.perf_counter() - started, (PdfDiagnostic("native_extraction_error", f"{type(exc).__name__}: {exc}", number, "error"),))
                    pages.append(item)
                    if progress_callback is not None:
                        progress_callback(item)
                finally:
                    if text_page is not None:
                        text_page.close()
                    if page is not None:
                        page.close()
            return NativeResult(count, tuple(pages), tuple(diagnostics), self.version)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            code = "encrypted_password_required" if "password" in message.lower() else "malformed_pdf"
            return NativeResult(0, (), (PdfDiagnostic(code, message, severity="error"),), self.version)
        finally:
            if document is not None:
                document.close()


class PdfRouter:
    """Page-level native chain with bounded optional OCR recovery.

    ``native`` and ``alternate_native`` remain accepted during the migration so
    existing consumers can move to ``native_chain`` without a flag day.
    """

    def __init__(
        self,
        *,
        native_chain: Optional[Sequence[NativeAdapter]] = None,
        native: Optional[NativeAdapter] = None,
        alternate_native: Optional[NativeAdapter] = None,
        ocr: Optional[OcrAdapter] = None,
        quality_detector: Callable[..., tuple[str, tuple[PdfDiagnostic, ...]]] = detect_text_quality,
    ) -> None:
        chain = tuple(native_chain or ())
        if not chain:
            chain = (native or PdfiumNativeAdapter(),)
            if alternate_native is not None:
                chain += (alternate_native,)
        if not chain:
            raise ValueError("native_chain must contain at least one adapter")
        self.native_chain = chain
        self.native = chain[0]
        self.alternate_native = chain[1] if len(chain) > 1 else None
        self.ocr = ocr
        self.quality_detector = quality_detector

    def parse(self, request: PdfParseRequest) -> PdfDocumentResult:
        started = time.perf_counter()
        profile = request.profile
        if not profile.enabled or profile.rollout_state == "disabled":
            return self._failed(request, "profile_disabled", "PDF profile is disabled", started)
        native = self.native_chain[0].extract(request.content, target_pages=request.target_pages)
        if native.page_count == 0:
            return PdfDocumentResult(PDF_PARSER_SCHEMA_VERSION, PDF_PARSER_VERSION, profile.name, request.content_hash, request.parameter_hash, 0, (), "failed", native.diagnostics, time.perf_counter() - started, requested_pages=request.requested_pages)
        limit = profile.limits.max_pages
        pages = list(native.pages[:limit]) if limit else list(native.pages)
        results: dict[int, PdfPageResult] = {}
        explicit_targets = set(request.target_pages)
        allow_ocr = request.ocr_mode != "none" and request.recovery_policy != "native_first"
        pending = {item.page_number for item in pages}
        native_attempts: list[NativeResult] = [native]
        for index, adapter in enumerate(self.native_chain):
            if not pending or (index and request.recovery_policy == "force_ocr"):
                break
            attempt = native_attempts[0] if index == 0 else adapter.extract(request.content, target_pages=tuple(sorted(pending)))
            if index:
                native_attempts.append(attempt)
            by_page = {item.page_number: item for item in attempt.pages}
            for number in tuple(sorted(pending)):
                item = by_page.get(number)
                method = "native_text" if index == 0 else "alternate_native"
                engine = getattr(adapter, "name", profile.native_engines[min(index, len(profile.native_engines) - 1)])
                if item is None:
                    diagnostic = PdfDiagnostic("native_adapter_missing_page", "native adapter returned no page result", number, "error", {"engine": engine})
                    candidate = self._candidate(method, "", "native_extraction_failure", 0.0, (diagnostic,), provenance={"engine": engine, "engine_version": attempt.engine_version, "chain_index": index})
                    previous = results.get(number)
                    candidates = (previous.candidates if previous else ()) + (candidate,)
                    results[number] = self._page_from_candidates(number, candidates, "native_extraction_failure", (previous.diagnostics if previous else ()) + (diagnostic,), previous.elapsed_seconds if previous else 0.0, True, previous.width_points if previous else None, previous.height_points if previous else None, cache_identity=self._cache_identity(request, number, attempt.engine_version))
                    continue
                quality, detected = self._quality(item.text, profile)
                diagnostics = tuple(item.diagnostics) + tuple(detected)
                candidate = self._candidate(method, item.text, quality, item.elapsed_seconds, diagnostics, provenance={"engine": engine, "engine_version": attempt.engine_version, "chain_index": index})
                previous = results.get(number)
                candidates = (previous.candidates if previous else ()) + (candidate,)
                selected = (method, item.text) if quality == "usable" and request.recovery_policy != "force_ocr" else None
                results[number] = self._page_from_candidates(number, candidates, quality, (previous.diagnostics if previous else ()) + diagnostics, item.elapsed_seconds, quality != "usable" or request.recovery_policy == "force_ocr", item.width_points, item.height_points, selected=selected, provenance=(candidate.provenance,), cache_identity=self._cache_identity(request, number, attempt.engine_version))
                if quality == "usable" and request.recovery_policy != "force_ocr":
                    pending.discard(number)
            # Later adapters receive only pages failed by all preceding engines.
            if index == 0 and request.recovery_policy == "force_ocr":
                break
        if request.recovery_policy == "force_ocr":
            ocr_targets = sorted(explicit_targets & set(results))
        elif allow_ocr:
            ocr_targets = sorted(number for number in pending if number in explicit_targets)
        else:
            ocr_targets = []
        all_ocr_targets = sorted(set(ocr_targets))
        limits = self._effective_limits(request)
        queue_targets = all_ocr_targets[: limits.max_queue_size]
        queue_deferred = all_ocr_targets[len(queue_targets):]
        ocr_targets = queue_targets[: limits.max_ocr_pages or None]
        deferred_targets = queue_deferred + queue_targets[len(ocr_targets):]
        for number in deferred_targets:
            previous = results[number]
            code = "ocr_queue_full" if number in queue_deferred else "ocr_page_budget_exceeded"
            message = "OCR queue bound reached" if code == "ocr_queue_full" else "maximum OCR pages exceeded"
            diagnostic = PdfDiagnostic(code, message, number, "error")
            results[number] = self._page_from_candidates(number, previous.candidates, "ocr_budget_exceeded", previous.diagnostics + (diagnostic,), previous.elapsed_seconds, previous.ocr_required, previous.width_points, previous.height_points, cache_identity=previous.cache_identity)
        cache_hits: set[int] = set()
        uncached_ocr_targets: list[int] = []
        for number in ocr_targets:
            previous = results[number]
            identity = self._cache_identity(request, number, getattr(self.ocr, "version", profile.engine_versions.get(profile.ocr_engine or "", "unknown")))
            cached = self._read_cache(request.cache_backend, identity)
            if cached is None:
                uncached_ocr_targets.append(number)
                results[number] = self._with_cache(previous, identity, "cache_miss")
            else:
                cache_hits.add(number)
                results[number] = self._merge_cached(previous, cached, identity)
        ocr_targets = uncached_ocr_targets
        if ocr_targets and self.ocr and profile.ocr_engine:
            try:
                ocr_request = replace(request, profile=replace(profile, limits=limits))
                ocr_pages = self.ocr.extract_pages(request.content, ocr_targets, request=ocr_request)
            except Exception as exc:
                ocr_pages = {}
                for number in ocr_targets:
                    previous = results[number]
                    diagnostic = PdfDiagnostic("ocr_failure", f"{type(exc).__name__}: {exc}", number, "error")
                    results[number] = self._page_from_candidates(number, previous.candidates, "ocr_failure", previous.diagnostics + (diagnostic,), previous.elapsed_seconds, previous.ocr_required, previous.width_points, previous.height_points, cache_identity=previous.cache_identity)
            for number in ocr_targets:
                item = ocr_pages.get(number)
                if item is None:
                    previous = results[number]
                    diagnostic = PdfDiagnostic("ocr_failure", "OCR adapter returned no page result", number, "error")
                    results[number] = self._page_from_candidates(number, previous.candidates, "ocr_failure", previous.diagnostics + (diagnostic,), previous.elapsed_seconds, previous.ocr_required, previous.width_points, previous.height_points, cache_identity=previous.cache_identity)
                    continue
                previous = results[number]
                quality, diags = self._quality(item.text, profile)
                provenance = {"engine": getattr(self.ocr, "name", profile.ocr_engine), "mode": request.ocr_mode, **dict(item.provenance)}
                diagnostics = previous.diagnostics + tuple(item.diagnostics) + tuple(diags)
                diagnostic_codes = {item.code for item in item.diagnostics}
                if "ocr_document_timeout" in diagnostic_codes:
                    status = "ocr_timeout"
                elif "ocr_timeout" in diagnostic_codes or item.elapsed_seconds > limits.max_page_seconds:
                    status = "ocr_timeout"
                    if "ocr_timeout" not in diagnostic_codes:
                        diagnostics += (PdfDiagnostic("ocr_timeout", "OCR page exceeded its time budget", number, "error", {"elapsed_seconds": item.elapsed_seconds, "max_page_seconds": limits.max_page_seconds}),)
                elif "ocr_failure" in diagnostic_codes:
                    status = "ocr_failure"
                elif item.confidence is not None and item.confidence < profile.ocr_min_confidence:
                    status = "ocr_low_confidence"
                elif not item.text.strip():
                    status = "ocr_empty"
                    diagnostics += (PdfDiagnostic("ocr_empty", "OCR returned empty text", number, "error"),)
                else:
                    status = "ocr_success" if quality == "usable" else "ocr_low_quality"
                candidate = self._candidate("ocr", item.text, status, item.elapsed_seconds, diagnostics, item.confidence, provenance)
                page = self._page_from_candidates(number, previous.candidates + (candidate,), status, diagnostics, item.elapsed_seconds, previous.ocr_required, previous.width_points, previous.height_points, selected=("ocr", item.text) if status == "ocr_success" else None, confidence=item.confidence, provenance=(provenance,), cache_identity=self._cache_identity(request, number, provenance.get("engine_version", getattr(self.ocr, "version", profile.engine_versions.get(profile.ocr_engine or "", "unknown")))), structured_payload=item.structured_payload, structured_format=item.structured_format)
                results[number] = page
                self._write_cache(request.cache_backend, page.cache_identity, page)
        elif ocr_targets:
            for number in ocr_targets:
                previous = results[number]
                diagnostic = PdfDiagnostic("ocr_unavailable", "configured OCR adapter is unavailable", number, "error")
                results[number] = self._page_from_candidates(number, previous.candidates, "ocr_unavailable", previous.diagnostics + (diagnostic,), previous.elapsed_seconds, previous.ocr_required, previous.width_points, previous.height_points, cache_identity=previous.cache_identity)
        elapsed = time.perf_counter() - started
        document_diags = list(native.diagnostics)
        if elapsed > limits.max_document_seconds and request.ocr_mode != "none":
            document_diags.append(PdfDiagnostic("document_time_budget_exceeded", "PDF processing exceeded configured document budget", severity="error", details={"elapsed_seconds": elapsed, "max_document_seconds": limits.max_document_seconds}))
        ordered_pages = tuple(results[number] for number in sorted(results))
        returned_pages = tuple(page.page_number for page in ordered_pages)
        failed_statuses = {
            "ocr_failure", "ocr_unavailable", "ocr_timeout", "ocr_empty", "ocr_low_quality", "ocr_low_confidence", "ocr_budget_exceeded",
            "native_worker_crashed", "native_worker_timeout", "native_worker_protocol_error", "native_extraction_failure",
        }
        native_failed = any(item.code in failed_statuses for item in native.diagnostics)
        status = "partial" if (limit and len(pages) < native.page_count) or deferred_targets or elapsed > limits.max_document_seconds or native_failed or any(page.quality_status in failed_statuses for page in ordered_pages) else "success"
        if not ordered_pages and native.page_count:
            status = "failed"
        page_diagnostics = {page.page_number: page.diagnostics for page in ordered_pages}
        return PdfDocumentResult(PDF_PARSER_SCHEMA_VERSION, PDF_PARSER_VERSION, profile.name, request.content_hash, request.parameter_hash, native.page_count, ordered_pages, status, tuple(document_diags), elapsed, {"native": native.engine_version, **dict(profile.engine_versions)}, request.requested_pages, returned_pages, page_diagnostics)

    @staticmethod
    def _candidate(method: str, text: str, quality: str, elapsed: float, diagnostics: tuple[PdfDiagnostic, ...], confidence: Optional[float] = None, provenance: Optional[Mapping[str, Any]] = None) -> PdfCandidate:
        text_hash = compute_content_hash(text.encode("utf-8"))
        return PdfCandidate(method, text, text_hash, quality, quality in {"usable", "ocr_success"}, confidence, elapsed, diagnostics, dict(provenance or {}))

    def _quality(self, text: str, profile: PdfProfile) -> tuple[str, tuple[PdfDiagnostic, ...]]:
        """Invoke legacy injected detectors while supplying the new policy."""
        try:
            return self.quality_detector(
                text,
                min_characters=profile.min_text_characters,
                expected_script=profile.expected_script,
                min_expected_script_characters=profile.min_expected_script_characters,
            )
        except TypeError:
            return self.quality_detector(text, min_characters=profile.min_text_characters)

    @classmethod
    def _page_from_candidates(cls, number: int, candidates: tuple[PdfCandidate, ...], status: str, diagnostics: tuple[PdfDiagnostic, ...], elapsed: float, ocr_required: bool, width_points: Optional[float] = None, height_points: Optional[float] = None, *, selected: Optional[tuple[str, str]] = None, confidence: Optional[float] = None, provenance: tuple[Mapping[str, Any], ...] = (), cache_identity: Optional[Mapping[str, Any]] = None, cache_status: str = "cache_miss", structured_payload: Any = None, structured_format: Optional[str] = None) -> PdfPageResult:
        method, text = selected if selected else ("none", "")
        text_hash = compute_content_hash(text.encode("utf-8"))
        selected_usable = method != "none"
        return PdfPageResult(number, text, method, status, text_hash, _stable_hash({"page": number, "method": method, "text_hash": text_hash}), diagnostics, confidence, provenance, elapsed, ocr_required, width_points, height_points, text, method, text_hash, selected_usable, candidates, dict(cache_identity or {}), cache_status, structured_payload, structured_format)

    @classmethod
    def _cache_identity(cls, request: PdfParseRequest, page_number: int, engine_version: str) -> Mapping[str, Any]:
        profile = request.profile
        limits = cls._effective_limits(request)
        model_version = profile.engine_versions.get("paddleocr_model_version", profile.engine_versions.get("paddleocr_model", "unknown"))
        return {
            "content_hash": request.content_hash,
            "physical_page_number": page_number,
            "profile": profile.name,
            "ocr_mode": request.ocr_mode,
            "recovery_policy": request.recovery_policy,
            "dpi": limits.render_dpi,
            "batch_size": limits.ocr_batch_size,
            "engine_version": engine_version,
            "model_version": model_version,
            "parser_config_version": profile.parser_config_version,
            "native_chain": profile.native_engines,
            "expected_script": profile.expected_script,
            "renderer": "pypdfium2",
            "renderer_version": profile.engine_versions.get("pypdfium2", "5.13.0"),
            "ocr_runtime": profile.ocr_runtime,
            "ocr_device": profile.ocr_device,
            "ocr_fallback_runtime": profile.ocr_fallback_runtime,
            "inference_config_hash": _stable_hash(dict(profile.ocr_inference_config)),
        }

    @staticmethod
    def _effective_limits(request: PdfParseRequest) -> PdfResourceLimits:
        profile = request.profile
        mode_limits = profile.mode_budgets.get(request.ocr_mode) if request.ocr_mode in profile.mode_budgets else DEFAULT_MODE_BUDGETS.get(request.ocr_mode)
        values = [profile.limits, mode_limits, request.mode_budget]
        values = [item for item in values if item is not None]
        if not values:
            return profile.limits
        def minimum(name: str):
            entries = [getattr(item, name) for item in values]
            non_null = [item for item in entries if item is not None]
            return min(non_null) if non_null else None
        ocr_caps = []
        for item in values:
            cap = item.max_ocr_pages
            if item is not profile.limits and cap is None:
                cap = item.max_pages
            if cap is not None:
                ocr_caps.append(cap)
        return PdfResourceLimits(
            max_pages=profile.limits.max_pages,
            max_ocr_pages=min(ocr_caps) if ocr_caps else None,
            max_concurrency=minimum("max_concurrency") or 1,
            max_queue_size=minimum("max_queue_size") or 1,
            max_queue_wait_seconds=minimum("max_queue_wait_seconds") or 0.0,
            max_page_seconds=minimum("max_page_seconds") or profile.limits.max_page_seconds,
            max_document_seconds=minimum("max_document_seconds") or profile.limits.max_document_seconds,
            ocr_batch_size=minimum("ocr_batch_size") or 1,
            render_dpi=minimum("render_dpi") or profile.limits.render_dpi,
            enforce_hard_timeout=all(item.enforce_hard_timeout for item in values),
        )

    @staticmethod
    def _read_cache(backend: Optional[PdfPageCacheBackend], identity: Mapping[str, Any]) -> Optional[PdfPageResult]:
        if backend is None:
            return None
        value = backend.get(identity)
        if isinstance(value, PdfPageResult):
            return value
        if isinstance(value, Mapping):
            try:
                diagnostics = tuple(PdfDiagnostic(**item) for item in value.get("diagnostics", ()))
                candidates = tuple(
                    PdfCandidate(
                        method=str(item.get("method", "none")),
                        text=str(item.get("text", "")),
                        text_hash=str(item.get("text_hash", "")),
                        quality_status=str(item.get("quality_status", "unknown")),
                        usable_for_semantic=bool(item.get("usable_for_semantic", False)),
                        confidence=item.get("confidence"),
                        elapsed_seconds=float(item.get("elapsed_seconds", 0.0)),
                        diagnostics=tuple(PdfDiagnostic(**diag) for diag in item.get("diagnostics", ())),
                        provenance=dict(item.get("provenance", {})),
                    )
                    for item in value.get("candidates", ())
                )
                return PdfPageResult(**{**dict(value), "diagnostics": diagnostics, "candidates": candidates})
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def _write_cache(backend: Optional[PdfPageCacheBackend], identity: Mapping[str, Any], page: PdfPageResult) -> None:
        if backend is not None and page.selected_method == "ocr":
            backend.put(identity, page)

    @staticmethod
    def _with_cache(page: PdfPageResult, identity: Mapping[str, Any], status: str) -> PdfPageResult:
        return PdfPageResult(**{**asdict(page), "diagnostics": page.diagnostics, "candidates": page.candidates, "cache_identity": dict(identity), "cache_status": status})

    @classmethod
    def _merge_cached(cls, previous: PdfPageResult, cached: PdfPageResult, identity: Mapping[str, Any]) -> PdfPageResult:
        candidates = previous.candidates + tuple(item for item in cached.candidates if item.method not in {candidate.method for candidate in previous.candidates})
        return cls._page_from_candidates(cached.page_number, candidates, cached.quality_status, previous.diagnostics + cached.diagnostics, cached.elapsed_seconds, previous.ocr_required, previous.width_points, previous.height_points, selected=(cached.selected_method, cached.selected_text) if cached.selected_method != "none" else None, confidence=cached.confidence, provenance=cached.provenance, cache_identity=identity, cache_status="cache_hit", structured_payload=cached.structured_payload, structured_format=cached.structured_format)

    @staticmethod
    def _failed(request: PdfParseRequest, code: str, message: str, started: float) -> PdfDocumentResult:
        return PdfDocumentResult(PDF_PARSER_SCHEMA_VERSION, PDF_PARSER_VERSION, request.profile.name, request.content_hash, request.parameter_hash, 0, (), "failed", (PdfDiagnostic(code, message, severity="error"),), time.perf_counter() - started, requested_pages=request.requested_pages)
