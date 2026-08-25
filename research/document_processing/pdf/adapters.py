"""Optional PDF/OCR adapter boundaries.

Heavy runtimes are deliberately imported lazily. This keeps native parsing
usable on workers that do not install PaddleOCR or pdf-inspector.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import logging
import multiprocessing
import os
import pickle
import queue
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Mapping, Sequence

from .core import OcrPage, PdfDiagnostic, PdfParseRequest

logger = logging.getLogger(__name__)


class _OcrWorkerError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _paddle_device() -> str:
    """Return the active Paddle device without making runtime availability fatal."""
    try:
        import paddle

        return str(paddle.device.get_device())
    except Exception:
        return "unknown"


class PaddleOcrAdapter:
    """Page-addressable PaddleOCR adapter with bounded worker sessions.

    Quote-side rendering uses PDFium. Production Paddle imports stay in an
    explicitly configured external worker command; the in-process path is
    retained only for injected test sessions and local compatibility.
    """

    name = "paddleocr"
    version = "paddleocr-adapter.v2"
    worker_protocol_version = "quote-pdf-ocr-worker.v1"

    def __init__(self, *, page_renderer: Any = None, ocr_instance: Any = None, structure: bool = False, model_cache_dir: str | None = None, device: str = "cpu", process_worker: Any = None, worker_command: Sequence[str] = (), fallback_worker_command: Sequence[str] = ()) -> None:
        self.page_renderer = page_renderer or _render_pdfium_page
        self._default_renderer = page_renderer is None
        self._ocr = ocr_instance
        self._process_worker = process_worker
        self.model_cache_dir = model_cache_dir
        self.device = str(device or "cpu")
        self.structure = structure
        self.worker_command = tuple(str(item) for item in worker_command)
        self.fallback_worker_command = tuple(str(item) for item in fallback_worker_command)
        self._warmup_seconds = 0.0
        self._session_lock = threading.Lock()
        self._session_local = threading.local()

    @property
    def available(self) -> bool:
        return self._ocr is not None or bool(self.worker_command) or importlib.util.find_spec("paddleocr") is not None

    @classmethod
    def probe_runtime(cls, profile) -> Mapping[str, Any]:
        """Probe an isolated worker without importing Paddle in Quote."""
        command = tuple(profile.ocr_worker_command)
        if not command:
            return {"healthy": False, "cuda_available": False, "diagnostic": "ocr_worker_command is not configured"}
        try:
            completed = subprocess.run(
                [*command, "--probe"],
                input=json.dumps({"protocol": cls.worker_protocol_version, "runtime": profile.ocr_runtime, "model_cache_dir": profile.ocr_model_cache_dir}),
                capture_output=True,
                text=True,
                timeout=20.0,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"healthy": False, "cuda_available": False, "diagnostic": f"{type(exc).__name__}: {exc}"}
        if completed.returncode != 0:
            return {"healthy": False, "cuda_available": False, "diagnostic": completed.stderr.strip()[:500] or "worker exited non-zero"}
        try:
            payload = json.loads(completed.stdout)
        except ValueError:
            return {"healthy": False, "cuda_available": False, "diagnostic": "worker returned malformed probe response"}
        if payload.get("protocol") != cls.worker_protocol_version:
            return {"healthy": False, "cuda_available": False, "diagnostic": "worker protocol mismatch"}
        return dict(payload)

    def _session(self) -> Any:
        if self._ocr is not None:
            return self._ocr
        cached = getattr(self._session_local, "session", None)
        if cached is not None:
            return cached
        with self._session_lock:
            started = time.perf_counter()
            cache_dir = self.model_cache_dir or os.environ.get("PADDLE_PDX_CACHE_HOME")
            if not cache_dir:
                cache_dir = os.path.join(tempfile.gettempdir(), "quote_paddlex")
                # Keep local development usable, but make an unconfigured
                # production cache explicit in provenance and logs.
                logger.warning("PADDLE_PDX_CACHE_HOME is not configured; using temporary OCR cache %s", cache_dir)
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            if not os.access(cache_dir, os.W_OK):
                raise RuntimeError(f"PaddleOCR model cache is not writable: {cache_dir}")
            os.environ["PADDLE_PDX_CACHE_HOME"] = cache_dir
            try:
                from paddleocr import PaddleOCR
            except ImportError as exc:
                raise RuntimeError("PaddleOCR runtime is not installed") from exc
            # Avoid constructing a model at import time; the session is warm
            # and reused for every page handled by this adapter instance.
            cached = PaddleOCR(device=self.device, use_doc_orientation_classify=False, use_doc_unwarping=False, use_textline_orientation=False, enable_mkldnn=False)
            self._session_local.session = cached
            self._warmup_seconds = max(self._warmup_seconds, time.perf_counter() - started)
            return cached

    def extract_pages(self, content: bytes, pages: Sequence[int], *, request: PdfParseRequest) -> Mapping[int, OcrPage]:
        if not pages:
            return {}
        if self.worker_command:
            return self._extract_pages_with_external_worker(content, pages, request=request)
        if self._ocr is None and request.profile.ocr_runtime.startswith("isolated-"):
            return self._worker_error_pages(
                pages,
                _OcrWorkerError("ocr_worker_startup_failed", "isolated OCR worker command is not configured"),
                request,
            )
        if self._ocr is None and request.profile.limits.enforce_hard_timeout:
            if self._process_worker is None:
                try:
                    pickle.dumps(self.page_renderer)
                except (pickle.PickleError, TypeError, AttributeError) as exc:
                    raise TypeError(
                        "custom page_renderer must be picklable when hard OCR timeouts are enabled"
                    ) from exc
            return self._extract_pages_with_process_timeouts(content, pages, request=request)
        batch_size = max(1, int(request.profile.limits.ocr_batch_size))
        batches = [list(pages[offset: offset + batch_size]) for offset in range(0, len(pages), batch_size)]
        workers = min(max(1, int(request.profile.limits.max_concurrency)), len(batches))
        if self._ocr is not None:
            workers = 1
        if workers == 1:
            outputs = [self._extract_batch(content, batch, request=request) for batch in batches]
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pdf-ocr") as pool:
                outputs = list(pool.map(lambda batch: self._extract_batch(content, batch, request=request), batches))
        merged: dict[int, OcrPage] = {}
        for output in outputs:
            merged.update(output)
        return merged

    def _extract_pages_with_external_worker(self, content: bytes, pages: Sequence[int], *, request: PdfParseRequest) -> Mapping[int, OcrPage]:
        """Render once in Quote and send image-only work to an isolated runtime."""
        started = time.monotonic()
        try:
            images = _render_pdfium_batch(content, pages, request.profile.limits.render_dpi)
        except Exception as exc:
            diagnostic = PdfDiagnostic("ocr_render_failure", f"{type(exc).__name__}: {exc}", severity="error")
            return {int(page): OcrPage("", None, 0.0, (diagnostic,), {"runtime": request.profile.ocr_runtime}) for page in pages}
        try:
            output = self._invoke_external_worker(self.worker_command, images, request, timeout=max(0.1, request.profile.limits.max_document_seconds))
        except _OcrWorkerError as exc:
            if self.fallback_worker_command and exc.code in request.profile.ocr_fallback_failure_codes:
                remaining = request.profile.limits.max_document_seconds - (time.monotonic() - started)
                if remaining > 0:
                    try:
                        output = self._invoke_external_worker(self.fallback_worker_command, images, request, timeout=remaining, runtime=request.profile.ocr_fallback_runtime)
                        output = {
                            number: OcrPage(
                                item.text,
                                item.confidence,
                                item.elapsed_seconds,
                                item.diagnostics + (PdfDiagnostic("ocr_primary_runtime_failed", exc.message, number, "warning"),),
                                {**dict(item.provenance), "fallback_from_runtime": request.profile.ocr_runtime, "fallback_reason": exc.code},
                                item.structured_payload,
                                item.structured_format,
                            )
                            for number, item in output.items()
                        }
                    except _OcrWorkerError as fallback_exc:
                        return self._worker_error_pages(pages, fallback_exc, request, attempted_runtime=request.profile.ocr_fallback_runtime)
                else:
                    return self._worker_error_pages(pages, _OcrWorkerError("ocr_document_timeout", "OCR document budget exhausted before CPU fallback"), request)
            else:
                return self._worker_error_pages(pages, exc, request)
        return output

    def _invoke_external_worker(self, command: Sequence[str], images: Mapping[int, bytes], request: PdfParseRequest, *, timeout: float, runtime: str | None = None) -> Mapping[int, OcrPage]:
        payload = {
            "protocol": self.worker_protocol_version,
            "runtime": runtime or request.profile.ocr_runtime,
            "device": self.device,
            "structure": self.structure,
            "model_cache_dir": self.model_cache_dir,
            "inference_config": dict(request.profile.ocr_inference_config),
            "pages": [
                {
                    "page_number": number,
                    "image_png_base64": base64.b64encode(image).decode("ascii"),
                    "image_sha256": hashlib.sha256(image).hexdigest(),
                }
                for number, image in sorted(images.items())
            ],
        }
        try:
            completed = subprocess.run(
                [*command, "--ocr"], input=json.dumps(payload), capture_output=True, text=True, timeout=timeout, check=False
            )
        except subprocess.TimeoutExpired as exc:
            raise _OcrWorkerError("ocr_timeout", f"worker exceeded original timeout: {exc}") from exc
        except OSError as exc:
            raise _OcrWorkerError("ocr_worker_startup_failed", f"{type(exc).__name__}: {exc}") from exc
        if completed.returncode != 0:
            code = "ocr_worker_crashed" if completed.returncode < 0 else "ocr_worker_failure"
            raise _OcrWorkerError(code, completed.stderr.strip()[:500] or "worker exited non-zero")
        try:
            response = json.loads(completed.stdout)
        except ValueError as exc:
            raise _OcrWorkerError("ocr_worker_malformed_response", "worker did not return JSON") from exc
        if response.get("protocol") != self.worker_protocol_version:
            raise _OcrWorkerError("ocr_worker_malformed_response", "worker protocol mismatch")
        items = response.get("pages")
        if not isinstance(items, list):
            raise _OcrWorkerError("ocr_worker_malformed_response", "worker response has no pages list")
        expected = set(images)
        output: dict[int, OcrPage] = {}
        for item in items:
            if not isinstance(item, Mapping) or int(item.get("page_number", 0)) not in expected:
                raise _OcrWorkerError("ocr_worker_malformed_response", "worker returned invalid page identity")
            number = int(item["page_number"])
            if number in output:
                raise _OcrWorkerError("ocr_worker_malformed_response", "worker returned duplicate page")
            text = str(item.get("text", ""))
            confidence = item.get("confidence")
            if confidence is not None:
                confidence = float(confidence)
            diagnostics = tuple(PdfDiagnostic(**diag) for diag in item.get("diagnostics", ()) if isinstance(diag, Mapping))
            output[number] = OcrPage(text, confidence, float(item.get("elapsed_seconds", 0.0)), diagnostics, {
                "engine": "paddleocr",
                "engine_version": item.get("paddleocr_version"),
                "paddle_version": item.get("paddle_version"),
                "runtime": response.get("runtime"),
                "device": response.get("device"),
                "renderer": "pypdfium2",
                "renderer_version": "5.13.0",
                "render_dpi": request.profile.limits.render_dpi,
                "image_sha256": item.get("image_sha256"),
                "model": item.get("model"),
                "model_version": item.get("model_version"),
                "inference_config": response.get("inference_config", {}),
            })
        if set(output) != expected:
            raise _OcrWorkerError("ocr_worker_malformed_response", "worker omitted requested page")
        return output

    @staticmethod
    def _worker_error_pages(pages: Sequence[int], error: "_OcrWorkerError", request: PdfParseRequest, *, attempted_runtime: str | None = None) -> Mapping[int, OcrPage]:
        return {
            int(page): OcrPage("", None, 0.0, (PdfDiagnostic(error.code, error.message, int(page), "error"),), {
                "runtime": attempted_runtime or request.profile.ocr_runtime,
                "worker_error": error.code,
            })
            for page in pages
        }

    def _extract_pages_with_process_timeouts(self, content: bytes, pages: Sequence[int], *, request: PdfParseRequest) -> Mapping[int, OcrPage]:
        """Run bounded persistent workers that can be terminated on page timeout."""
        context = multiprocessing.get_context("spawn")
        output_queue = context.Queue()
        pending = list(dict.fromkeys(int(page) for page in pages))
        workers = min(max(1, int(request.profile.limits.max_concurrency)), len(pending))
        worker_state: dict[int, dict[str, Any]] = {}
        output: dict[int, OcrPage] = {}
        request_without_cache = PdfParseRequest(
            content=request.content,
            expected_content_hash=request.expected_content_hash,
            target_pages=request.target_pages,
            profile=request.profile,
            ocr_mode=request.ocr_mode,
            recovery_policy=request.recovery_policy,
            mode_budget=request.mode_budget,
            parameter_overrides=request.parameter_overrides,
        )
        document_deadline = time.monotonic() + request.profile.limits.max_document_seconds

        def start_worker(worker_id: int) -> None:
            input_queue = context.Queue(maxsize=1)
            worker_args = (
                worker_id,
                input_queue,
                output_queue,
                self.structure,
                self.model_cache_dir,
                self.device,
            )
            if self._process_worker is None:
                worker_args += (self.page_renderer,)
            process = context.Process(
                target=self._process_worker or _paddle_process_worker,
                args=worker_args,
                name=f"pdf-ocr-{worker_id}",
                daemon=True,
            )
            process.start()
            worker_state[worker_id] = {"process": process, "queue": input_queue, "page": None, "deadline": None}

        def assign(worker_id: int) -> None:
            if not pending:
                return
            page = pending.pop(0)
            state = worker_state[worker_id]
            state["page"] = page
            state["deadline"] = min(
                time.monotonic() + request.profile.limits.max_page_seconds,
                document_deadline,
            )
            state["queue"].put((bytes(content), page, request_without_cache))

        def stop_worker(worker_id: int, *, terminate: bool = False) -> None:
            state = worker_state[worker_id]
            process = state["process"]
            if terminate and process.is_alive():
                process.terminate()
            elif process.is_alive():
                try:
                    state["queue"].put_nowait(None)
                except queue.Full:
                    pass
            process.join(timeout=0.5 if terminate else 5.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)
            state["queue"].close()

        for worker_id in range(workers):
            start_worker(worker_id)
            assign(worker_id)
        try:
            while any(state["page"] is not None for state in worker_state.values()):
                now = time.monotonic()
                active_deadlines = [state["deadline"] for state in worker_state.values() if state["page"] is not None]
                wait_seconds = max(0.0, min(active_deadlines) - now) if active_deadlines else 0.0
                try:
                    worker_id, page, result, error = output_queue.get(timeout=wait_seconds)
                except queue.Empty:
                    worker_id = page = result = error = None
                if worker_id is not None:
                    state = worker_state.get(worker_id)
                    if state and state["page"] == page:
                        if error:
                            diagnostic = PdfDiagnostic("ocr_failure", error, page, "error")
                            output[page] = OcrPage("", None, 0.0, (diagnostic,), {"device": self.device})
                        else:
                            output[page] = result
                        state["page"] = None
                        state["deadline"] = None
                        assign(worker_id)
                now = time.monotonic()
                for current_id, state in list(worker_state.items()):
                    if state["page"] is None or now < state["deadline"]:
                        continue
                    timed_out_page = int(state["page"])
                    document_timeout = now >= document_deadline
                    code = "ocr_document_timeout" if document_timeout else "ocr_timeout"
                    diagnostic = PdfDiagnostic(code, "OCR worker exceeded its hard time budget", timed_out_page, "error")
                    output[timed_out_page] = OcrPage("", None, request.profile.limits.max_page_seconds, (diagnostic,), {"device": self.device, "worker_terminated": True})
                    stop_worker(current_id, terminate=True)
                    if document_timeout:
                        state["page"] = None
                        state["deadline"] = None
                    elif pending:
                        start_worker(current_id)
                        assign(current_id)
                    else:
                        state["page"] = None
                        state["deadline"] = None
                if time.monotonic() >= document_deadline:
                    for remaining_page in pending:
                        diagnostic = PdfDiagnostic("ocr_document_timeout", "OCR document budget exhausted before page dispatch", remaining_page, "error")
                        output[remaining_page] = OcrPage("", None, 0.0, (diagnostic,), {"device": self.device})
                    pending.clear()
                    break
        finally:
            for worker_id in list(worker_state):
                state = worker_state[worker_id]
                deadline_expired = time.monotonic() >= document_deadline
                stop_worker(
                    worker_id,
                    terminate=deadline_expired and state["page"] is not None,
                )
            output_queue.close()
        return output

    def _extract_batch(self, content: bytes, batch_pages: Sequence[int], *, request: PdfParseRequest) -> Mapping[int, OcrPage]:
        session = self._session()
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
        output: dict[int, OcrPage] = {}
        for page_number, result in zip(batch_pages, raw_results):
            text, confidence, blocks = _normalise_paddle_result(result)
            diagnostics = () if text else (PdfDiagnostic("ocr_empty", "PaddleOCR returned no text", page_number, "error"),)
            output[page_number] = OcrPage(
                text,
                confidence,
                elapsed / len(batch_pages),
                diagnostics,
                {
                    "component": "pp-structure" if self.structure else "pp-ocr",
                    "engine": "paddleocr",
                    "engine_version": self.version,
                    "model": request.profile.engine_versions.get("paddleocr_model", "PP-OCRv6"),
                    "model_version": request.profile.engine_versions.get("paddleocr_model_version"),
                    "blocks": blocks,
                    "batch_size": len(batch_pages),
                    "warmup_seconds": self._warmup_seconds,
                    "model_cache_dir": self.model_cache_dir or os.environ.get("PADDLE_PDX_CACHE_HOME"),
                    "device": _paddle_device(),
                },
            )
        return output


def _paddle_process_worker(worker_id: int, input_queue: Any, output_queue: Any, structure: bool, model_cache_dir: str | None, device: str, page_renderer: Any) -> None:
    adapter = PaddleOcrAdapter(page_renderer=page_renderer, structure=structure, model_cache_dir=model_cache_dir, device=device)
    while True:
        task = input_queue.get()
        if task is None:
            return
        content, page, request = task
        try:
            result = adapter._extract_batch(content, [page], request=request).get(page)
            if result is None:
                raise RuntimeError("PaddleOCR worker returned no page result")
            output_queue.put((worker_id, page, result, None))
        except Exception as exc:
            output_queue.put((worker_id, page, None, f"{type(exc).__name__}: {exc}"))


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


def _render_pdfium_batch(content: bytes, pages: Sequence[int], dpi: int) -> Mapping[int, bytes]:
    """Render a bounded OCR batch once with the authoritative PDFium input."""
    try:
        import pypdfium2 as pdfium
    except ImportError as exc:
        raise RuntimeError("pypdfium2 is required for PaddleOCR page rendering") from exc
    import io

    document = pdfium.PdfDocument(bytes(content))
    output: dict[int, bytes] = {}
    try:
        count = len(document)
        for number in sorted(set(int(page) for page in pages)):
            if number < 1 or number > count:
                raise ValueError(f"page {number} is outside 1..{count}")
            page = bitmap = image = None
            try:
                page = document.get_page(number - 1)
                bitmap = page.render(scale=float(dpi) / 72.0)
                image = bitmap.to_pil()
                buffer = io.BytesIO()
                image.save(buffer, format="PNG")
                output[number] = buffer.getvalue()
            finally:
                if image is not None:
                    image.close()
                if page is not None:
                    page.close()
    finally:
        document.close()
    return output
