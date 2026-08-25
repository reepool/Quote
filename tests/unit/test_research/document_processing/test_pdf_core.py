from __future__ import annotations

import io
from pathlib import Path

import pytest
from pypdf import PdfWriter

from research.document_processing.pdf import (
    DEFAULT_PROFILES,
    PdfDiagnostic,
    PdfParseRequest,
    PdfProfile,
    PdfResourceLimits,
    PdfRouter,
    PypdfNativeAdapter,
    detect_text_quality,
    profile_from_mapping,
    resolve_profile,
)
from research.document_processing.pdf.adapters import PaddleOcrAdapter
from research.document_processing.pdf.core import NativePage, NativeResult, OcrPage


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
    result = PdfRouter().parse(PdfParseRequest(content=path.read_bytes(), profile=PdfProfile(name="fixture-diagnostic")))
    assert result.page_count == 350
    assert sum(page.quality_status == "native_text_mapping_error" for page in result.pages) >= 300
    assert all(page.quality_status != "usable" for page in result.pages if page.page_number > 10)


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
    profile = PdfProfile(name="test", alternate_native_engine="pdf-inspector", ocr_engine="paddleocr", limits=PdfResourceLimits(max_ocr_pages=1))
    result = PdfRouter(native=_FakeNative(), alternate_native=_FakeAlternate(), ocr=_FakeOcr()).parse(PdfParseRequest(content=_blank_pdf(2), profile=profile))
    assert result.pages[0].extraction_method == "native_text"
    assert result.pages[1].extraction_method == "alternate_native"
    assert not any(page.extraction_method == "ocr" for page in result.pages)


def test_missing_ocr_runtime_is_typed_and_profile_switch_is_config_only() -> None:
    profile = profile_from_mapping("test", {"ocr_engine": "paddleocr", "ocr_model_cache_dir": "/tmp/quote-paddlex-test", "limits": {"max_ocr_pages": 1}})
    assert profile.name == "test"
    assert profile.ocr_model_cache_dir == "/tmp/quote-paddlex-test"
    assert DEFAULT_PROFILES["pypdf_paddleocr"].fallback_profile == "pypdf_native"


def test_profile_rollout_can_be_changed_without_consumer_code(monkeypatch) -> None:
    monkeypatch.setenv("QUOTE_PDF_ENGINE_PROFILE", "pdf_inspector_paddleocr")
    assert resolve_profile().name == "pdf_inspector_paddleocr"
    monkeypatch.setenv("QUOTE_PDF_ENGINE_PROFILE", "unknown")
    with pytest.raises(ValueError, match="unknown PDF engine profile"):
        resolve_profile()


def test_default_parse_request_uses_configured_rollout_profile(monkeypatch) -> None:
    monkeypatch.setenv("QUOTE_PDF_ENGINE_PROFILE", "pdf_inspector_paddleocr")
    assert PdfParseRequest(content=b"%PDF-1.4").profile.name == "pdf_inspector_paddleocr"


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
    profile = PdfProfile(name="parallel", limits=PdfResourceLimits(ocr_batch_size=1, max_concurrency=2))
    result = adapter.extract_pages(b"%PDF-1.4", [1, 2], request=PdfParseRequest(content=b"%PDF-1.4", profile=profile))

    assert set(result) == {1, 2}
    assert len(sessions) == 2
    assert len({thread_id for session in sessions for thread_id in session.thread_ids}) == 2
