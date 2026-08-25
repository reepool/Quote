"""Official CNInfo corporate-action document archive and page extraction."""

from __future__ import annotations

import hashlib
from html import unescape
from html.parser import HTMLParser
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Protocol

from research.announcements import (
    AnnouncementAttachment,
    AnnouncementAttachmentRetriever,
    load_announcement_acquisition_config,
)
from utils.config_manager import ResearchConfig, config_manager


DOCUMENT_PARSER_VERSION = "cninfo_corporate_action_document.v2"
DEFAULT_ARCHIVE_ROOT = Path("data/filings/corporate_actions")
_SAFE_NAME = re.compile(r"[^0-9A-Za-z_.-]+")
_HTML_CHARSET = re.compile(
    br"charset\s*=\s*[\"']?\s*([A-Za-z0-9._-]+)", re.IGNORECASE
)
_HTML_BLOCK_TAGS = frozenset({
    "address", "article", "aside", "blockquote", "br", "div", "footer",
    "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main",
    "nav", "ol", "p", "pre", "section", "table", "td", "th", "tr", "ul",
})
_HTML_IGNORED_TAGS = frozenset({"script", "style", "noscript", "svg"})
_HTML_ERROR_MARKERS = (
    "404 not found", "page not found", "access denied", "forbidden",
    "页面不存在", "访问受限", "请输入验证码", "安全验证",
)


class _VisibleHtmlTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._ignored_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        del attrs
        normalized = str(tag or "").lower()
        if normalized in _HTML_IGNORED_TAGS:
            self._ignored_depth += 1
        elif self._ignored_depth == 0 and normalized in _HTML_BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized = str(tag or "").lower()
        if normalized in _HTML_IGNORED_TAGS and self._ignored_depth > 0:
            self._ignored_depth -= 1
        elif self._ignored_depth == 0 and normalized in _HTML_BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._ignored_depth == 0 and str(data or "").strip():
            self.parts.append(data)


@dataclass(frozen=True)
class CorporateActionPageText:
    page_number: int
    text: str
    text_hash: str
    announcement_id: Optional[str] = None
    extraction_method: str = "native_text"
    quality_status: str = "usable"

    def to_row(self) -> dict[str, Any]:
        return {
            "page_number": self.page_number,
            "text": self.text,
            "text_hash": self.text_hash,
            "extraction_method": self.extraction_method,
            "quality_status": self.quality_status,
            "parser_version": DOCUMENT_PARSER_VERSION,
        }


class CorporateActionOcrAdapter(Protocol):
    """Optional OCR seam; OCR implementation is deliberately owned by the caller."""

    def extract_page_text(self, pdf_bytes: bytes, page_number: int) -> str: ...


@dataclass(frozen=True)
class CorporateActionDocumentBundle:
    announcement_id: str
    source_url: str
    content_hash: str
    content_type: str
    content_length: int
    archive_path: str
    pages: tuple[CorporateActionPageText, ...]
    extraction_status: str
    error_message: Optional[str] = None
    source: str = "cninfo"

    def artifact_row(self, *, title: Optional[str] = None, announcement_time: Any = None) -> dict[str, Any]:
        return {
            "announcement_id": self.announcement_id,
            "source": self.source,
            "source_url": self.source_url,
            "announcement_title": title,
            "announcement_time": announcement_time,
            "content_hash": self.content_hash,
            "content_type": self.content_type,
            "content_length": self.content_length,
            "archive_path": self.archive_path,
            "download_status": "downloaded",
            "extraction_status": self.extraction_status,
            "parser_version": DOCUMENT_PARSER_VERSION,
            "error_message": self.error_message,
            "metadata": {"archive_policy": "content_hash_immutable"},
        }


@dataclass(frozen=True)
class CorporateActionDocumentArtifact:
    announcement_id: str
    source_url: str
    content_hash: str
    content_type: str
    content_length: int
    archive_path: str
    source: str = "cninfo"


def normalize_page_text(value: str) -> str:
    """Normalize whitespace only; preserve wording for exact-quote validation."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def page_text_cache_key(
    artifact_hash: str,
    page: CorporateActionPageText,
    *,
    parser_version: str = DOCUMENT_PARSER_VERSION,
) -> str:
    """Return the immutable reuse key for one parsed artifact page."""
    identity = "|".join((
        str(artifact_hash or "").strip(),
        str(parser_version or "").strip(),
        str(page.page_number),
        str(page.extraction_method or "").strip(),
        str(page.text_hash or "").strip(),
    ))
    if not all(identity.split("|")):
        raise ValueError("page text cache identity is incomplete")
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _safe_component(value: str) -> str:
    result = _SAFE_NAME.sub("_", str(value or "").strip())
    return result[:120] or "unknown"


def extract_pdf_pages(
    pdf_bytes: bytes,
    *,
    ocr_adapter: Optional[CorporateActionOcrAdapter] = None,
) -> tuple[CorporateActionPageText, ...]:
    """Extract pages through the shared native-first PDF router."""
    from research.document_processing.pdf import PdfParseRequest, PdfProfile, PdfResourceLimits, PdfRouter
    from research.document_processing.pdf.core import OcrPage

    if not pdf_bytes or not pdf_bytes.startswith(b"%PDF-"):
        raise ValueError("invalid_pdf_signature")

    class _CorporateActionOcr:
        name = "corporate_action_ocr"

        def extract_pages(self, content: bytes, page_numbers: Iterable[int], *, request: PdfParseRequest):
            del request
            output: dict[int, OcrPage] = {}
            for page_number in page_numbers:
                try:
                    text = normalize_page_text(ocr_adapter.extract_page_text(content, page_number)) if ocr_adapter else ""
                except Exception as exc:
                    from research.document_processing.pdf import PdfDiagnostic
                    output[page_number] = OcrPage("", diagnostics=(PdfDiagnostic("ocr_adapter_failure", str(exc), page_number, "error"),))
                    continue
                output[page_number] = OcrPage(text, provenance={"adapter": type(ocr_adapter).__name__ if ocr_adapter else None})
            return output

    profile = PdfProfile(
        name="pypdf_corporate_action",
        ocr_engine="corporate_action_ocr" if ocr_adapter is not None else None,
        limits=PdfResourceLimits(max_ocr_pages=None),
    )
    result = PdfRouter(ocr=_CorporateActionOcr() if ocr_adapter is not None else None).parse(
        PdfParseRequest(content=pdf_bytes, profile=profile)
    )
    if result.status == "failed":
        code = result.diagnostics[0].code if result.diagnostics else "pdf_parse_failed"
        raise ValueError(code)
    pages: list[CorporateActionPageText] = []
    for page in result.pages:
        text = normalize_page_text(page.text)
        if text and page.quality_status not in {"native_text_mapping_error", "ocr_low_quality", "ocr_failure", "ocr_deferred"}:
            pages.append(CorporateActionPageText(
                page_number=page.page_number,
                text=text,
                text_hash=page.text_hash,
                extraction_method=page.extraction_method,
                quality_status="usable" if page.extraction_method != "ocr" else "ocr_usable",
            ))
    if not pages:
        raise ValueError("ocr_unavailable")
    return tuple(pages)


def extract_html_pages(html_bytes: bytes) -> tuple[CorporateActionPageText, ...]:
    """Extract one immutable visible-text page from a historical HTML notice."""
    if not html_bytes:
        raise ValueError("document_empty")
    declared = _HTML_CHARSET.search(html_bytes[:4096])
    encodings = []
    if declared:
        encodings.append(declared.group(1).decode("ascii", errors="ignore"))
    encodings.extend(["utf-8-sig", "gb18030"])
    decoded: Optional[str] = None
    for encoding in dict.fromkeys(encodings):
        try:
            decoded = html_bytes.decode(encoding)
            break
        except (LookupError, UnicodeDecodeError):
            continue
    if decoded is None:
        decoded = html_bytes.decode("utf-8", errors="replace")
    parser = _VisibleHtmlTextParser()
    try:
        parser.feed(decoded)
        parser.close()
    except Exception as exc:
        raise ValueError(f"html_parse_failed:{type(exc).__name__}") from exc
    text = normalize_page_text(unescape(" ".join(parser.parts)))
    lowered = text.lower()
    if len(text) < 40:
        raise ValueError("html_text_insufficient")
    if len(text) < 1200 and any(marker in lowered for marker in _HTML_ERROR_MARKERS):
        raise ValueError("html_error_page")
    return (
        CorporateActionPageText(
            page_number=1,
            text=text,
            text_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            extraction_method="html_text",
            quality_status="usable",
        ),
    )


def select_relevant_pages(
    pages: Iterable[CorporateActionPageText],
    *,
    max_pages: int = 8,
    context_pages: int = 1,
) -> list[CorporateActionPageText]:
    """Select action/date pages and bounded neighboring context."""
    page_list = list(pages)
    if not page_list:
        return []
    terms = (
        "股权登记日", "除权日", "除息日", "除权除息日", "实施日", "股份到账日",
        "新增股份上市日", "复牌日", "对价支付日", "权益分派", "资本公积转增",
        "股权分置改革", "重整计划", "每10股", "每十股", "派发", "送股", "转增",
    )
    selected: set[int] = set()
    for position, page in enumerate(page_list):
        if any(term in page.text for term in terms):
            start = max(0, position - max(0, int(context_pages)))
            end = min(len(page_list), position + int(context_pages) + 1)
            selected.update(range(start, end))
    if not selected:
        selected.update(range(min(len(page_list), max(1, int(max_pages)))))
    return [page_list[index] for index in sorted(selected)[:max(1, int(max_pages))]]


class CninfoCorporateActionDocumentService:
    """Download and archive one official attachment with an injectable fetcher."""

    def __init__(
        self,
        *,
        archive_root: str | Path = DEFAULT_ARCHIVE_ROOT,
        fetcher: Optional[Callable[[str], bytes]] = None,
        retriever: Optional[AnnouncementAttachmentRetriever] = None,
        research_config: Optional[ResearchConfig] = None,
        ocr_adapter: Optional[CorporateActionOcrAdapter] = None,
    ) -> None:
        self.archive_root = Path(archive_root)
        self.fetcher = fetcher
        if retriever is None and fetcher is None:
            acquisition_config = load_announcement_acquisition_config(
                research_config or config_manager.get_research_config()
            )
            retriever = AnnouncementAttachmentRetriever.from_provider_configs(
                acquisition_config.provider_configs
            )
        self.retriever = retriever
        self.ocr_adapter = ocr_adapter

    def ingest(
        self,
        *,
        announcement_id: str,
        source_url: str,
        source: str = "cninfo",
        title: Optional[str] = None,
        announcement_time: Any = None,
    ) -> CorporateActionDocumentBundle:
        artifact = self.retrieve_and_archive(
            announcement_id=announcement_id,
            source_url=source_url,
            source=source,
            title=title,
            announcement_time=announcement_time,
        )
        return self.parse_artifact(artifact)

    def retrieve_and_archive(
        self,
        *,
        announcement_id: str,
        source_url: str,
        source: str = "cninfo",
        title: Optional[str] = None,
        announcement_time: Any = None,
    ) -> CorporateActionDocumentArtifact:
        """Retrieve and archive an official PDF or historical HTML document."""
        announcement_id = str(announcement_id or "").strip()
        source_url = str(source_url or "").strip()
        if not announcement_id or not source_url:
            raise ValueError("announcement_id and source_url are required")
        source = str(source or "").strip().lower()
        if not source:
            raise ValueError("source is required")
        final_url = source_url
        content_type = "application/octet-stream"
        if self.fetcher is not None:
            content = self.fetcher(source_url)
        else:
            if self.retriever is None:
                raise RuntimeError("announcement attachment retriever is not configured")
            retrieval = self.retriever.retrieve(
                source,
                AnnouncementAttachment(
                    source_url=source_url,
                    resolved_url=(
                        source_url
                        if source_url.startswith(("http://", "https://"))
                        else None
                    ),
                ),
                require_pdf=False,
            )
            if retrieval.status != "success":
                raise RuntimeError(
                    "corporate-action attachment retrieval failed: "
                    + "; ".join(retrieval.errors)
                )
            content = retrieval.content
            final_url = retrieval.final_url or source_url
            content_type = retrieval.response_media_type or content_type
        if not isinstance(content, (bytes, bytearray)) or not content:
            raise ValueError("document_empty")
        content = bytes(content)
        if content.startswith(b"%PDF-"):
            extension = ".pdf"
            content_type = "application/pdf"
        else:
            prefix = content[:2048].lstrip().lower()
            is_html = (
                "html" in str(content_type or "").lower()
                or prefix.startswith((b"<!doctype html", b"<html", b"<head", b"<body"))
            )
            if not is_html:
                raise ValueError("unsupported_document_signature")
            extract_html_pages(content)
            extension = ".html"
            content_type = "text/html"
        digest = hashlib.sha256(content).hexdigest()
        relative = Path(_safe_component(announcement_id)) / f"{digest}{extension}"
        target = self.archive_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            existing_hash = hashlib.sha256(target.read_bytes()).hexdigest()
            if existing_hash != digest:
                raise ValueError("archive_hash_mismatch")
        else:
            with tempfile.NamedTemporaryFile(
                dir=target.parent,
                prefix=f".{digest}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.replace(temporary_path, target)
            finally:
                temporary_path.unlink(missing_ok=True)
        return CorporateActionDocumentArtifact(
            announcement_id=announcement_id,
            source_url=final_url,
            content_hash=digest,
            content_type=content_type,
            content_length=len(content),
            archive_path=relative.as_posix(),
            source=source,
        )

    def parse_artifact(
        self,
        artifact: CorporateActionDocumentArtifact,
    ) -> CorporateActionDocumentBundle:
        """Parse one immutable artifact under a caller-owned CPU resource lease."""
        target = self.archive_root / artifact.archive_path
        if not target.is_file():
            raise FileNotFoundError(f"document artifact is missing: {artifact.archive_path}")
        content = target.read_bytes()
        if hashlib.sha256(content).hexdigest() != artifact.content_hash:
            raise ValueError("archive_hash_mismatch")
        if content.startswith(b"%PDF-"):
            extracted_pages = extract_pdf_pages(
                content, ocr_adapter=self.ocr_adapter
            )
        else:
            extracted_pages = extract_html_pages(content)
        pages = tuple(
            CorporateActionPageText(
                page_number=page.page_number,
                text=page.text,
                text_hash=page.text_hash,
                announcement_id=artifact.announcement_id,
                extraction_method=page.extraction_method,
                quality_status=page.quality_status,
            )
            for page in extracted_pages
        )
        return CorporateActionDocumentBundle(
            announcement_id=artifact.announcement_id,
            source_url=artifact.source_url,
            content_hash=artifact.content_hash,
            content_type=artifact.content_type,
            content_length=artifact.content_length,
            archive_path=artifact.archive_path,
            pages=pages,
            extraction_status="extracted",
            source=artifact.source,
        )
