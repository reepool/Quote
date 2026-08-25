"""Optional PDF/OCR adapter boundaries.

Heavy runtimes are deliberately imported lazily. This keeps native parsing
usable on workers that do not install PaddleOCR or pdf-inspector.
"""

from __future__ import annotations

import importlib.util
import os
import tempfile
import time
import threading
from typing import Any, Mapping, Sequence

from .core import OcrPage, PdfDiagnostic, PdfParseRequest


class PaddleOcrAdapter:
    """Page-addressable PaddleOCR adapter with one lazily-created session.

    The adapter accepts an injected ``page_renderer`` so deployments can use
    PDFium, PyMuPDF, or another renderer without coupling the shared contract.
    """

    name = "paddleocr"

    def __init__(self, *, page_renderer: Any = None, ocr_instance: Any = None, structure: bool = False) -> None:
        self.page_renderer = page_renderer or _render_pdfium_page
        self._ocr = ocr_instance
        self.structure = structure
        self._warmup_seconds = 0.0
        self._session_lock = threading.Lock()

    @property
    def available(self) -> bool:
        return self._ocr is not None or importlib.util.find_spec("paddleocr") is not None

    def _session(self) -> Any:
        if self._ocr is None:
            started = time.perf_counter()
            os.environ.setdefault("PADDLE_PDX_CACHE_HOME", os.path.join(tempfile.gettempdir(), "quote_paddlex"))
            try:
                from paddleocr import PaddleOCR
            except ImportError as exc:
                raise RuntimeError("PaddleOCR runtime is not installed") from exc
            # Avoid constructing a model at import time; the session is warm
            # and reused for every page handled by this adapter instance.
            self._ocr = PaddleOCR(use_doc_orientation_classify=False, use_doc_unwarping=False, use_textline_orientation=False, enable_mkldnn=False)
            self._warmup_seconds = time.perf_counter() - started
        return self._ocr

    def extract_pages(self, content: bytes, pages: Sequence[int], *, request: PdfParseRequest) -> Mapping[int, OcrPage]:
        if not pages:
            return {}
        with self._session_lock:
            session = self._session()
            output: dict[int, OcrPage] = {}
            batch_size = max(1, int(request.profile.limits.ocr_batch_size))
            for offset in range(0, len(pages), batch_size):
                batch_pages = list(pages[offset: offset + batch_size])
                started = time.perf_counter()
                images = []
                for page_number in batch_pages:
                    image = self.page_renderer(content, page_number, request.profile.limits.render_dpi)
                    if not isinstance(image, str):
                        try:
                            import numpy as np
                            image = np.asarray(image)
                        except ImportError as exc:
                            raise RuntimeError("numpy is required for PaddleOCR input") from exc
                    images.append(image)
                if hasattr(session, "predict"):
                    raw_results = list(session.predict(images if len(images) > 1 else images[0]))
                else:
                    raw_results = [session.ocr(image, cls=True) for image in images]
                if len(raw_results) != len(batch_pages):
                    raise RuntimeError(f"PaddleOCR returned {len(raw_results)} results for {len(batch_pages)} pages")
                elapsed = time.perf_counter() - started
                for page_number, result in zip(batch_pages, raw_results):
                    text, confidence, blocks = _normalise_paddle_result(result)
                    diagnostics = () if text else (PdfDiagnostic("ocr_empty", "PaddleOCR returned no text", page_number, "error"),)
                    output[page_number] = OcrPage(text, confidence, elapsed / len(batch_pages), diagnostics, {"component": "pp-structure" if self.structure else "pp-ocr", "blocks": blocks, "batch_size": len(batch_pages), "warmup_seconds": self._warmup_seconds})
        return output


class PdfInspectorNativeAdapter:
    """Optional capability probe for firecrawl/pdf-inspector."""

    name = "pdf-inspector"

    @staticmethod
    def available() -> bool:
        return importlib.util.find_spec("pdf_inspector") is not None

    def extract(self, content: bytes, *, target_pages: Sequence[int] = ()):
        from .core import NativePage, NativeResult

        if not self.available():
            return NativeResult(0, (), (PdfDiagnostic("alternate_native_runtime_unavailable", "pdf-inspector is not installed", severity="error"),), "pdf-inspector-unavailable")
        try:
            import pdf_inspector
        except Exception as exc:
            return NativeResult(0, (), (PdfDiagnostic("alternate_native_runtime_unavailable", str(exc), severity="error"),), "pdf-inspector-unavailable")
        started = time.perf_counter()
        try:
            classification = pdf_inspector.detect_pdf_bytes(content)
            page_count = int(getattr(classification, "page_count", getattr(classification, "pages", 0)) or 0)
            requested = tuple(target_pages) if target_pages else tuple(range(1, page_count + 1))
            pages: list[NativePage] = []
            diagnostics: list[PdfDiagnostic] = []
            for page_number in requested:
                page_started = time.perf_counter()
                items = pdf_inspector.extract_text_with_positions_bytes(content, pages=[page_number])
                # TextItem exposes page/x/y; sorting by visual reading order is
                # preferable to relying on PDF content-stream order.
                ordered = sorted((item for item in items if int(getattr(item, "page", page_number)) == page_number), key=lambda item: (-float(getattr(item, "y", 0.0)), float(getattr(item, "x", 0.0))))
                text = " ".join(str(getattr(item, "text", "") or "") for item in ordered).strip()
                pages.append(NativePage(page_number, text, time.perf_counter() - page_started))
            return NativeResult(page_count, tuple(pages), tuple(diagnostics), "pdf-inspector-1.17.0")
        except Exception as exc:
            return NativeResult(0, (), (PdfDiagnostic("alternate_native_extraction_error", f"{type(exc).__name__}: {exc}", severity="error"),), "pdf-inspector-error")


class PdfInspectorOcrAdapter:
    """Optional pdf-inspector OCR boundary; offline mode never downloads models."""

    name = "pdf-inspector-ocr"

    def __init__(self, *, offline: bool = True, model_directory: str | None = None) -> None:
        self.offline = offline
        self.model_directory = model_directory

    @staticmethod
    def available() -> bool:
        return importlib.util.find_spec("pdf_inspector") is not None

    def extract_pages(self, content: bytes, pages: Sequence[int], *, request: PdfParseRequest) -> Mapping[int, OcrPage]:
        if not self.available():
            raise RuntimeError("pdf-inspector OCR runtime is not installed")
        import pdf_inspector

        result = pdf_inspector.process_pdf_with_ocr_bytes(
            content,
            mode="force",
            page_numbers=list(pages),
            dpi=float(request.profile.limits.render_dpi),
            model_directory=self.model_directory,
            offline=self.offline,
        )
        output: dict[int, OcrPage] = {}
        by_page = {int(item.page_number): item for item in result.pages}
        for page_number in pages:
            item = by_page.get(page_number)
            text = str(getattr(item, "markdown", "") or "").strip() if item else ""
            diagnostics = () if text else (PdfDiagnostic("ocr_empty", "pdf-inspector OCR returned no text", page_number, "error"),)
            output[page_number] = OcrPage(text, None, float(getattr(result, "ocr_time_ms", 0.0) or 0.0) / 1000.0, diagnostics, {"component": "pdf-inspector-ocr", "processing_time_ms": getattr(result, "processing_time_ms", None)})
        return output


def _normalise_paddle_result(result: Any) -> tuple[str, float | None, list[dict[str, Any]]]:
    """Accept common PaddleOCR 2.x/3.x result shapes without leaking them."""
    texts: list[str] = []
    scores: list[float] = []
    blocks: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, str):
            if value.strip():
                texts.append(value.strip())
        elif isinstance(value, (list, tuple)):
            if len(value) == 2 and isinstance(value[1], (int, float)) and isinstance(value[0], str):
                texts.append(value[0].strip())
                scores.append(float(value[1]))
                return
            for item in value:
                visit(item)
        elif isinstance(value, Mapping):
            rec_texts = value.get("rec_texts")
            rec_scores = value.get("rec_scores")
            if isinstance(rec_texts, (list, tuple)):
                texts.extend(str(item).strip() for item in rec_texts if str(item).strip())
                if isinstance(rec_scores, (list, tuple)):
                    scores.extend(float(item) for item in rec_scores if isinstance(item, (int, float)))
            text = value.get("text") or value.get("rec_text")
            score = value.get("score") or value.get("rec_score")
            if text:
                texts.append(str(text).strip())
            if isinstance(score, (int, float)):
                scores.append(float(score))
            blocks.append({key: value[key] for key in ("text", "rec_text", "score", "rec_score") if key in value})
    visit(result)
    confidence = sum(scores) / len(scores) if scores else None
    return "\n".join(item for item in texts if item), confidence, blocks


def _render_pdfium_page(content: bytes, page_number: int, dpi: int) -> Any:
    """Render one page without retaining the whole PDF as images."""
    try:
        import pypdfium2 as pdfium
    except ImportError as exc:
        raise RuntimeError("pypdfium2 is required for PaddleOCR page rendering") from exc
    document = pdfium.PdfDocument(content)
    try:
        page = document.get_page(page_number - 1)
        bitmap = page.render(scale=float(dpi) / 72.0)
        return bitmap.to_pil()
    finally:
        try:
            page.close()
        except Exception:
            pass
        document.close()
