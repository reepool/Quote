from __future__ import annotations

import io
import json
import os
import signal
import sys
import time
from pathlib import Path

import pytest
from pypdf import PdfWriter

from research.document_processing.pdf import (
    DEFAULT_PROFILES,
    PdfParseRequest,
    PdfProfile,
    PdfResourceLimits,
    PdfRouter,
    PdfiumNativeAdapter,
    PypdfNativeAdapter,
    detect_text_quality,
    profile_from_mapping,
    resolve_profile,
    benchmark_native_parallelism,
)
from research.document_processing.pdf.adapters import IsolatedNativeAdapter, PaddleOcrAdapter
from research.document_processing.pdf.core import NativePage, NativeResult, OcrPage
from research.document_processing.pdf.native_worker import NativeWorkerPool
from research.document_processing.pdf.evaluation import NATIVE_WORKER_WIDTHS
from research.document_processing.pdf.profiles import GPU_CANARY_REQUIRED_CHECKS


def _sleeping_process_worker(worker_id, input_queue, output_queue, structure, model_cache_dir, device):
    while True:
        task = input_queue.get()
        if task is None:
            return
        time.sleep(10)


def _picklable_page_renderer(*_):
    return "image"


def _trap_native_worker(worker_id, input_queue, output_queue):
    os.kill(os.getpid(), signal.SIGTRAP)


def _hang_native_worker(worker_id, input_queue, output_queue):
    input_queue.get()
    time.sleep(10)


def _blank_pdf(page_count: int = 1) -> bytes:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=595, height=842)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def test_pypdf_valid_pdf_preserves_page_identity_and_hash() -> None:
    content = _blank_pdf(2)
    request = PdfParseRequest(content=content)
    result = PdfRouter().parse(request)
    assert result.status == "success"
    assert result.page_count == 2
    assert [page.page_number for page in result.pages] == [1, 2]
    assert all(len(page.text_hash) == 64 for page in result.pages)
    assert result.content_hash == request.content_hash


def test_invalid_and_hash_mismatch_fail_closed() -> None:
    result = PdfRouter().parse(PdfParseRequest(content=b"not a pdf"))
    assert result.status == "failed"
    assert result.diagnostics[0].code == "invalid_pdf_signature"
    with pytest.raises(ValueError, match="content_hash"):
        PdfParseRequest(content=_blank_pdf(), expected_content_hash="0" * 64)


def test_empty_native_page_is_explicitly_not_successful_disclosure() -> None:
    result = PdfRouter().parse(PdfParseRequest(content=_blank_pdf()))
    assert result.pages[0].quality_status == "empty"
    assert result.pages[0].diagnostics[0].code == "empty_page"


def test_valid_unicode_mapping_corruption_is_detected_without_replacement() -> None:
    text = "".join(chr(code) for code in range(0x0c80, 0x0c80 + 80))
    quality, diagnostics = detect_text_quality(text)
    assert quality == "native_text_mapping_error"
    assert any(item.code == "native_text_mapping_error" for item in diagnostics)


def test_600036_fixture_is_mapping_corrupt_when_archived() -> None:
    path = Path("data/filings/announcements/blobs/ab/abe612a273468072b176dd51ea460c1e1596f8ca729cbc6db3fa28ba9a57ea79.pdf")
    if not path.exists():
        pytest.skip("optional archived 600036.SH fixture is not present")
    # Keep this regression focused on the known pypdf defect; PDFium-first is
    # expected to recover these same pages and is covered by the promotion gold.
    result = PdfRouter(native=PypdfNativeAdapter()).parse(PdfParseRequest(content=path.read_bytes(), target_pages=(19, 41), profile=PdfProfile(name="fixture-diagnostic", native_engines=("pypdf",))))
    assert result.page_count == 350
    assert all(page.quality_status == "native_text_mapping_error" for page in result.pages)


class _FakeNative:
    name = "fake-native"

    def extract(self, content: bytes, *, target_pages=()):
        return NativeResult(2, (NativePage(1, "招商银行 2025 年度报告 经营情况 " * 3, 0.001), NativePage(2, "garbled " + "".join(chr(0x0c80 + i) for i in range(80)), 0.001)))


class _FakeAlternate:
    name = "pdf-inspector"

    def extract(self, content: bytes, *, target_pages=()):
        return NativeResult(2, (NativePage(2, "主要业务 营业收入和营业成本 经营情况分析 " * 2, 0.002),))


class _FakeOcr:
    name = "paddleocr"

    def extract_pages(self, content, pages, *, request):
        return {page: OcrPage("OCR recovered page", 0.95, 0.01) for page in pages}


def test_router_prefers_alternate_native_then_selective_ocr() -> None:
    profile = PdfProfile(name="test", native_engines=("first", "second"), ocr_engine="paddleocr", limits=PdfResourceLimits(max_ocr_pages=1))
    result = PdfRouter(native_chain=(_FakeNative(), _FakeAlternate()), ocr=_FakeOcr()).parse(PdfParseRequest(content=_blank_pdf(2), profile=profile))
    assert result.pages[0].extraction_method == "native_text"
    assert result.pages[1].extraction_method == "alternate_native"
    assert not any(page.extraction_method == "ocr" for page in result.pages)


def test_native_first_does_not_create_ocr_work_and_preserves_failed_candidate():
    profile = PdfProfile(name="native-first", native_engines=("first",), ocr_engine="paddleocr")
    ocr = _FakeOcr()
    result = PdfRouter(native=_FakeNative(), alternate_native=None, ocr=ocr).parse(
        PdfParseRequest(content=_blank_pdf(2), profile=profile, target_pages=(2,), ocr_mode="section_extract", recovery_policy="native_first")
    )
    assert result.pages[1].selected_method == "none"
    assert result.pages[1].selected_text == ""
    assert result.pages[1].selected_usable_for_semantic is False
    assert result.pages[1].candidates[0].method == "native_text"
    assert not any(page.extraction_method == "ocr" for page in result.pages)


def test_force_ocr_requires_explicit_pages():
    with pytest.raises(ValueError, match="force_ocr"):
        PdfParseRequest(content=_blank_pdf(), recovery_policy="force_ocr", ocr_mode="section_extract")


def test_requested_recovery_without_ocr_runtime_is_typed():
    profile = PdfProfile(name="no-ocr-runtime", ocr_engine=None, min_text_characters=20)
    result = PdfRouter(native=_FakeNative()).parse(
        PdfParseRequest(content=_blank_pdf(2), profile=profile, target_pages=(2,), ocr_mode="section_extract", recovery_policy="selective_recovery")
    )
    assert result.pages[1].selected_method == "none"
    assert any(item.code == "ocr_unavailable" for item in result.pages[1].diagnostics)


def test_page_contract_keeps_requested_order_and_reports_out_of_range():
    request = PdfParseRequest(content=_blank_pdf(2), target_pages=(3, 2, 2, 1), ocr_mode="none")
    result = PdfRouter().parse(request)
    assert result.requested_pages == (3, 2, 2, 1)
    assert result.returned_pages == (1, 2)
    assert any(item.code == "target_page_out_of_range" for item in result.diagnostics)


def test_cache_backend_reuses_successful_ocr_page():
    class Cache:
        def __init__(self):
            self.values = {}

        def get(self, identity):
            return self.values.get(json.dumps(identity, sort_keys=True, default=list))

        def put(self, identity, page_result):
            self.values[json.dumps(identity, sort_keys=True, default=list)] = page_result

    cache = Cache()
    profile = PdfProfile(name="cache", ocr_engine="paddleocr", min_text_characters=1)
    first = PdfRouter(native=_FakeNative(), ocr=_FakeOcr()).parse(PdfParseRequest(content=_blank_pdf(2), profile=profile, target_pages=(2,), ocr_mode="section_extract", recovery_policy="force_ocr", cache_backend=cache))
    second = PdfRouter(native=_FakeNative(), ocr=_FakeOcr()).parse(PdfParseRequest(content=_blank_pdf(2), profile=profile, target_pages=(2,), ocr_mode="section_extract", recovery_policy="force_ocr", cache_backend=cache))
    assert first.pages[1].cache_status == "cache_miss"
    assert second.pages[1].cache_status == "cache_hit"
    assert second.pages[1].selected_method == "ocr"


def test_mode_budget_caps_ocr_pages_and_page_timeout_is_typed():
    class SlowOcr(_FakeOcr):
        def extract_pages(self, content, pages, *, request):
            return {page: OcrPage("OCR recovered page", 0.95, 2.0) for page in pages}

    profile = PdfProfile(name="budget", ocr_engine="paddleocr", limits=PdfResourceLimits(max_ocr_pages=3, max_page_seconds=5))
    result = PdfRouter(native=_FakeNative(), ocr=SlowOcr()).parse(
        PdfParseRequest(content=_blank_pdf(2), profile=profile, target_pages=(2,), ocr_mode="toc_probe", recovery_policy="force_ocr", mode_budget=PdfResourceLimits(max_pages=1, max_page_seconds=1, max_document_seconds=10))
    )
    assert result.pages[1].quality_status == "ocr_timeout"
    assert any(item.code == "ocr_timeout" for item in result.pages[1].diagnostics)


def test_missing_ocr_runtime_is_typed_and_profile_switch_is_config_only() -> None:
    profile = profile_from_mapping("test", {"ocr_engine": "paddleocr", "ocr_model_cache_dir": "/tmp/quote-paddlex-test", "limits": {"max_ocr_pages": 1}})
    assert profile.name == "test"
    assert profile.ocr_model_cache_dir == "/tmp/quote-paddlex-test"
    assert DEFAULT_PROFILES["pdfium_paddleocr_cpu"].fallback_profile == "pdfium_native"


def test_production_profiles_use_four_native_workers() -> None:
    assert {
        name: profile.native_max_concurrency
        for name, profile in DEFAULT_PROFILES.items()
    } == {
        "pdfium_native": 4,
        "pypdf_native": 4,
        "pdfium_paddleocr_cpu": 4,
        "pdfium_paddleocr_gpu": 4,
    }


def test_profile_rollout_can_be_changed_without_consumer_code(monkeypatch) -> None:
    monkeypatch.setenv("QUOTE_PDF_ENGINE_PROFILE", "pdfium_native")
    assert resolve_profile().name == "pdfium_native"
    monkeypatch.setenv("QUOTE_PDF_ENGINE_PROFILE", "unknown")
    with pytest.raises(ValueError, match="unknown PDF engine profile"):
        resolve_profile()


def test_gpu_canary_profile_fails_closed_without_approval(monkeypatch) -> None:
    monkeypatch.delenv("QUOTE_PDF_GPU_CANARY_APPROVED", raising=False)
    with pytest.raises(ValueError, match="GPU PDF profile requires"):
        resolve_profile("pdfium_paddleocr_gpu")


def test_gpu_canary_profile_rejects_incomplete_approval_report(monkeypatch, tmp_path) -> None:
    report = tmp_path / "approval.json"
    report.write_text('{"schema_version":"pdf-gpu-canary-approval.v1","profile":"pdfium_paddleocr_gpu","corpus_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","gpu_canary_approved":true,"checks":{"cuda_runtime":true}}', encoding="utf-8")
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_APPROVED", "1")
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_REPORT", str(report))
    with pytest.raises(ValueError, match="has not passed every gate"):
        resolve_profile("pdfium_paddleocr_gpu")


def test_gpu_canary_profile_rejects_approval_for_other_corpus(monkeypatch, tmp_path) -> None:
    report = tmp_path / "approval.json"
    report.write_text(json.dumps({
        "schema_version": "pdf-gpu-canary-approval.v1",
        "profile": "pdfium_paddleocr_gpu",
        "corpus_hash": "a" * 64,
        "gpu_canary_approved": True,
        "checks": {name: True for name in GPU_CANARY_REQUIRED_CHECKS},
    }), encoding="utf-8")
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_APPROVED", "1")
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_REPORT", str(report))
    with pytest.raises(ValueError, match="has not passed every gate"):
        resolve_profile("pdfium_paddleocr_gpu")


def _enable_approved_gpu_canary(monkeypatch, tmp_path) -> None:
    from research.document_processing.pdf import profiles as pdf_profiles

    corpus = "b" * 64
    report = tmp_path / "gpu-canary.json"
    report.write_text(
        json.dumps(
            {
                "schema_version": "pdf-gpu-canary-approval.v1",
                "profile": "pdfium_paddleocr_gpu",
                "corpus_hash": corpus,
                "gpu_canary_approved": True,
                "checks": {name: True for name in GPU_CANARY_REQUIRED_CHECKS},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pdf_profiles, "GPU_CANARY_CORPUS_HASH", corpus)
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_APPROVED", "1")
    monkeypatch.setenv("QUOTE_PDF_GPU_CANARY_REPORT", str(report))
    monkeypatch.setenv("QUOTE_PDF_GPU_OCR_WORKER", "/opt/fake-gpu/bin/python /tmp/ocr_worker.py")
    pdf_profiles.clear_gpu_runtime_probe_cache()


def test_gpu_runtime_probe_is_cached_across_profile_and_router(monkeypatch, tmp_path) -> None:
    from research.document_processing.pdf import profiles as pdf_profiles
    from research.document_processing.pdf.adapters import PaddleOcrAdapter

    _enable_approved_gpu_canary(monkeypatch, tmp_path)
    calls: list[object] = []

    def fake_probe(profile):
        calls.append(tuple(profile.ocr_worker_command))
        return {"healthy": True, "cuda_available": True, "model_cache_writable": True, "device": "gpu:0"}

    monkeypatch.setattr(PaddleOcrAdapter, "probe_runtime", staticmethod(fake_probe))

    profile = resolve_profile("pdfium_paddleocr_gpu")
    pdf_profiles._require_isolated_gpu_runtime(profile)
    assert calls == [("/opt/fake-gpu/bin/python", "/tmp/ocr_worker.py")]


def test_gpu_runtime_probe_failure_includes_and_logs_diagnostic(monkeypatch, tmp_path, caplog) -> None:
    from research.document_processing.pdf.adapters import PaddleOcrAdapter

    _enable_approved_gpu_canary(monkeypatch, tmp_path)
    calls: list[int] = []

    def fake_probe(profile):
        calls.append(1)
        return {
            "healthy": False,
            "cuda_available": False,
            "model_cache_writable": True,
            "diagnostic": "TimeoutExpired: worker exceeded 60.0 seconds",
        }

    monkeypatch.setattr(PaddleOcrAdapter, "probe_runtime", staticmethod(fake_probe))
    with caplog.at_level("ERROR"):
        with pytest.raises(ValueError, match="TimeoutExpired: worker exceeded 60.0 seconds"):
            resolve_profile("pdfium_paddleocr_gpu")
        with pytest.raises(ValueError, match="TimeoutExpired: worker exceeded 60.0 seconds"):
            resolve_profile("pdfium_paddleocr_gpu")
    assert calls == [1, 1]
    assert any("TimeoutExpired: worker exceeded 60.0 seconds" in message for message in caplog.messages)


def test_gpu_probe_uses_cold_start_timeout_budget(monkeypatch) -> None:
    captured: dict[str, float] = {}

    class Completed:
        returncode = 0
        stdout = json.dumps(
            {
                "protocol": PaddleOcrAdapter.worker_protocol_version,
                "healthy": True,
                "cuda_available": True,
            }
        )
        stderr = ""

    def fake_run(*args, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        return Completed()

    monkeypatch.setattr("research.document_processing.pdf.adapters.subprocess.run", fake_run)
    profile = PdfProfile(
        name="gpu-probe",
        ocr_worker_command=("/opt/fake-gpu/bin/python", "/tmp/ocr_worker.py"),
        ocr_runtime="isolated-gpu-paddle-3.3.1",
        ocr_model_cache_dir="/var/cache/quote/paddlex",
    )
    payload = PaddleOcrAdapter.probe_runtime(profile)
    assert payload["healthy"] is True
    assert captured["timeout"] == 60.0


def test_ocr_cache_directory_can_be_supplied_by_runtime_config(monkeypatch) -> None:
    monkeypatch.setenv("QUOTE_PDF_OCR_CACHE_DIR", "/tmp/quote-pdf-cache")
    assert resolve_profile("pdfium_paddleocr_cpu").ocr_model_cache_dir == "/tmp/quote-pdf-cache"


def test_default_parse_request_uses_configured_rollout_profile(monkeypatch) -> None:
    monkeypatch.setenv("QUOTE_PDF_ENGINE_PROFILE", "pdfium_native")
    assert PdfParseRequest(content=b"%PDF-1.4").profile.name == "pdfium_native"


def test_paddle_adapter_reuses_session_and_batches_pages() -> None:
    class FakeSession:
        def __init__(self):
            self.calls = []

        def predict(self, images):
            self.calls.append(images)
            values = images if isinstance(images, list) else [images]
            return [{"rec_texts": [f"page-{index}"], "rec_scores": [0.9]} for index, _ in enumerate(values, 1)]

    session = FakeSession()
    adapter = PaddleOcrAdapter(page_renderer=lambda *_: "image", ocr_instance=session)
    profile = PdfProfile(name="batch", limits=PdfResourceLimits(ocr_batch_size=2))
    result = adapter.extract_pages(b"%PDF-1.4", [1, 2], request=PdfParseRequest(content=b"%PDF-1.4", profile=profile))
    assert [result[index].text for index in (1, 2)] == ["page-1", "page-2"]
    assert len(session.calls) == 1
    assert result[1].provenance["model"] == "PP-OCRv6"


def test_paddle_adapter_uses_configured_concurrency_for_real_sessions() -> None:
    import threading
    import time

    class FakeSession:
        def __init__(self):
            self.thread_ids = []

        def predict(self, images):
            self.thread_ids.append(threading.get_ident())
            time.sleep(0.01)
            values = images if isinstance(images, list) else [images]
            return [{"rec_texts": ["page"], "rec_scores": [0.9]} for _ in values]

    sessions = []

    class TestAdapter(PaddleOcrAdapter):
        def _session(self):
            session = getattr(self._session_local, "session", None)
            if session is None:
                session = FakeSession()
                self._session_local.session = session
                sessions.append(session)
            return session

    adapter = TestAdapter(page_renderer=lambda *_: "image")
    profile = PdfProfile(
        name="parallel",
        limits=PdfResourceLimits(ocr_batch_size=1, max_concurrency=2, enforce_hard_timeout=False),
    )
    result = adapter.extract_pages(b"%PDF-1.4", [1, 2], request=PdfParseRequest(content=b"%PDF-1.4", profile=profile))

    assert set(result) == {1, 2}
    assert len(sessions) == 2
    assert len({thread_id for session in sessions for thread_id in session.thread_ids}) == 2


def test_paddle_adapter_terminates_worker_at_hard_page_timeout() -> None:
    adapter = PaddleOcrAdapter(process_worker=_sleeping_process_worker)
    profile = PdfProfile(name="hard-timeout", limits=PdfResourceLimits(max_page_seconds=0.1, max_document_seconds=1.0))
    started = time.monotonic()
    result = adapter.extract_pages(b"%PDF-1.4", [1], request=PdfParseRequest(content=b"%PDF-1.4", profile=profile))
    assert time.monotonic() - started < 3.0
    assert result[1].diagnostics[0].code == "ocr_timeout"
    assert result[1].provenance["worker_terminated"] is True


def test_document_deadline_terminates_every_active_worker_without_cleanup_delay() -> None:
    adapter = PaddleOcrAdapter(process_worker=_sleeping_process_worker)
    profile = PdfProfile(
        name="document-timeout",
        limits=PdfResourceLimits(
            max_concurrency=2,
            max_page_seconds=0.05,
            max_document_seconds=0.1,
        ),
    )
    started = time.monotonic()
    result = adapter.extract_pages(
        b"%PDF-1.4",
        [1, 2],
        request=PdfParseRequest(content=b"%PDF-1.4", profile=profile),
    )
    assert time.monotonic() - started < 3.0
    assert set(result) == {1, 2}
    assert all(page.diagnostics[0].code in {"ocr_timeout", "ocr_document_timeout"} for page in result.values())


def test_real_paddle_with_picklable_custom_renderer_uses_hard_timeout_path(monkeypatch) -> None:
    adapter = PaddleOcrAdapter(page_renderer=_picklable_page_renderer)
    sentinel = {1: OcrPage("bounded")}
    monkeypatch.setattr(adapter, "_extract_pages_with_process_timeouts", lambda *args, **kwargs: sentinel)
    result = adapter.extract_pages(
        b"%PDF-1.4",
        [1],
        request=PdfParseRequest(content=b"%PDF-1.4", profile=PdfProfile(name="custom-renderer")),
    )
    assert result is sentinel


def test_hard_timeout_rejects_unpicklable_custom_renderer() -> None:
    adapter = PaddleOcrAdapter(page_renderer=lambda *_: "image")
    with pytest.raises(TypeError, match="page_renderer must be picklable"):
        adapter.extract_pages(
            b"%PDF-1.4",
            [1],
            request=PdfParseRequest(content=b"%PDF-1.4", profile=PdfProfile(name="custom-renderer")),
        )


def test_expected_chinese_script_rejects_numeric_residue_but_keeps_english_configurable() -> None:
    chinese = PdfProfile(name="chinese", expected_script="cjk", min_text_characters=5)
    numeric_result = PdfRouter(native=_StaticNative({1: "2025 90,676 5.0%"})).parse(
        PdfParseRequest(content=_blank_pdf(), profile=chinese, target_pages=(1,))
    )
    assert numeric_result.pages[0].selected_method == "none"
    assert numeric_result.pages[0].quality_status == "native_text_expected_script_missing"
    english = PdfProfile(name="english", expected_script="latin", min_text_characters=5)
    english_result = PdfRouter(native=_StaticNative({1: "Annual report 2025"})).parse(
        PdfParseRequest(content=_blank_pdf(), profile=english, target_pages=(1,))
    )
    assert english_result.pages[0].selected_method == "native_text"


def test_ordered_native_chain_batches_failed_pages_and_short_circuits_usable_pages() -> None:
    fallback = _RecordingNative({2: "中文恢复", 3: "中文恢复"})
    profile = PdfProfile(name="chain", native_engines=("first", "second"), expected_script="cjk", min_text_characters=2)
    result = PdfRouter(native_chain=(_StaticNative({1: "中文可用", 2: "", 3: ""}), fallback)).parse(
        PdfParseRequest(content=_blank_pdf(3), profile=profile, target_pages=(3, 1, 2))
    )
    assert fallback.calls == [(2, 3)]
    assert result.returned_pages == (1, 2, 3)
    assert result.pages[0].selected_method == "native_text"
    assert all(page.selected_method == "alternate_native" for page in result.pages[1:])


def test_force_ocr_keeps_first_native_diagnostic_and_skips_later_native_engines() -> None:
    fallback = _RecordingNative({1: "不应调用"})
    profile = PdfProfile(name="force", native_engines=("first", "second"), ocr_engine="paddleocr", expected_script="cjk", min_text_characters=3)
    class ChineseOcr(_FakeOcr):
        def extract_pages(self, content, pages, *, request):
            return {page: OcrPage("中文 OCR 恢复", 0.95, 0.01) for page in pages}

    result = PdfRouter(native_chain=(_StaticNative({1: "中文原生文本"}), fallback), ocr=ChineseOcr()).parse(
        PdfParseRequest(content=_blank_pdf(), profile=profile, target_pages=(1,), ocr_mode="section_extract", recovery_policy="force_ocr")
    )
    assert fallback.calls == []
    assert result.pages[0].selected_method == "ocr"
    assert len(result.pages[0].candidates) == 2


def test_pdfium_adapter_extracts_stable_requested_pages_once(monkeypatch) -> None:
    import research.document_processing.pdf.core as core

    calls = []
    original = core.PdfiumNativeAdapter.extract

    def wrapped(self, content, *, target_pages=()):
        calls.append(tuple(target_pages))
        return original(self, content, target_pages=target_pages)

    monkeypatch.setattr(core.PdfiumNativeAdapter, "extract", wrapped)
    result = PdfRouter(native=PdfiumNativeAdapter()).parse(
        PdfParseRequest(content=_blank_pdf(2), profile=PdfProfile(name="pdfium", native_engines=("pypdfium2",)), target_pages=(2, 1))
    )
    assert calls == [(1, 2)]
    assert result.returned_pages == (1, 2)


def test_external_worker_protocol_uses_pdfium_images_and_preserves_identity(tmp_path) -> None:
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import json,sys\n"
        "payload=json.load(sys.stdin)\n"
        "if '--probe' in sys.argv: print(json.dumps({'protocol':'quote-pdf-ocr-worker.v1','healthy':True,'cuda_available':False})); raise SystemExit(0)\n"
        "print(json.dumps({'protocol':'quote-pdf-ocr-worker.v1','runtime':payload['runtime'],'device':'cpu','inference_config':{},'pages':[{'page_number':x['page_number'],'text':'中文 OCR 文本','confidence':0.99,'elapsed_seconds':0.01,'image_sha256':x['image_sha256'],'paddle_version':'3.3.1','paddleocr_version':'3.7.0','model':'PP-OCRv6','diagnostics':[]} for x in payload['pages']]}))\n",
        encoding="utf-8",
    )
    command = (sys.executable, str(worker))
    profile = PdfProfile(name="worker", ocr_engine="paddleocr", expected_script="cjk", min_text_characters=3, ocr_worker_command=command, ocr_runtime="isolated-cpu-paddle-3.3.1")
    adapter = PaddleOcrAdapter(worker_command=command)
    result = adapter.extract_pages(_blank_pdf(), [1], request=PdfParseRequest(content=_blank_pdf(), profile=profile))
    assert result[1].text == "中文 OCR 文本"
    assert result[1].provenance["renderer"] == "pypdfium2"
    assert len(result[1].provenance["image_sha256"]) == 64


def test_external_worker_malformed_response_fails_closed(tmp_path) -> None:
    worker = tmp_path / "bad_worker.py"
    worker.write_text("print('not-json')\n", encoding="utf-8")
    command = (sys.executable, str(worker))
    profile = PdfProfile(name="worker", ocr_engine="paddleocr", ocr_worker_command=command)
    result = PaddleOcrAdapter(worker_command=command).extract_pages(
        _blank_pdf(), [1], request=PdfParseRequest(content=_blank_pdf(), profile=profile)
    )
    assert result[1].diagnostics[0].code == "ocr_worker_malformed_response"


def test_native_worker_pool_extracts_and_renders_in_isolated_process() -> None:
    pool = NativeWorkerPool(max_workers=2, task_timeout_seconds=20.0)
    try:
        content = _blank_pdf(2)
        adapter = IsolatedNativeAdapter("pypdfium2", pool=pool, timeout_seconds=20.0)
        result = adapter.extract(content, target_pages=(2, 1))
        assert result.page_count == 2
        assert [page.page_number for page in result.pages] == [1, 2]
        images = pool.render(content, (1,), dpi=100, timeout_seconds=20.0)
        assert set(images) == {1}
        assert images[1].startswith(b"\x89PNG")
    finally:
        pool.close()


def test_native_worker_signal_is_typed_and_parent_survives() -> None:
    pool = NativeWorkerPool(max_workers=1, task_timeout_seconds=5.0, max_restarts=1, worker_target=_trap_native_worker)
    try:
        result = pool.extract(_blank_pdf(), "pypdfium2", target_pages=(1,))
        assert result.diagnostics[0].code == "native_worker_crashed"
        assert result.diagnostics[0].details["worker_exitcode"] < 0
    finally:
        pool.close()


def test_native_worker_timeout_is_typed_and_worker_is_reaped() -> None:
    pool = NativeWorkerPool(max_workers=1, task_timeout_seconds=0.1, max_restarts=0, worker_target=_hang_native_worker)
    try:
        result = pool.extract(_blank_pdf(), "pypdfium2", target_pages=(1,), timeout_seconds=0.1)
        assert result.diagnostics[0].code == "native_worker_timeout"
    finally:
        pool.close()


def test_native_parallel_benchmark_width_matrix_includes_six_eight_ten(tmp_path) -> None:
    pdf = tmp_path / "sample.pdf"
    pdf.write_bytes(_blank_pdf())
    case = type("Case", (), {"pdf_path": str(pdf), "target_pages": (1,), "page_count": 1})()
    report = benchmark_native_parallelism([case], widths=(1, 2, 4, 6, 8, 10), rounds=1, timeout_seconds=20.0)
    assert report["widths"] == [1, 2, 4, 6, 8, 10]
    assert {item["width"] for item in report["reports"]} == set(NATIVE_WORKER_WIDTHS)


class _StaticNative:
    name = "static"

    def __init__(self, pages):
        self.pages = pages

    def extract(self, content, *, target_pages=()):
        requested = tuple(target_pages) or tuple(self.pages)
        return NativeResult(
            3,
            tuple(NativePage(number, self.pages.get(number, ""), 0.01) for number in requested),
            engine_version="static-v1",
        )


class _RecordingNative(_StaticNative):
    name = "recording"

    def __init__(self, pages):
        super().__init__(pages)
        self.calls = []

    def extract(self, content, *, target_pages=()):
        self.calls.append(tuple(target_pages))
        return super().extract(content, target_pages=target_pages)
