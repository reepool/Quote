"""Official CNInfo corporate-action document archive and page extraction."""

from __future__ import annotations

import hashlib
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


DOCUMENT_PARSER_VERSION = "cninfo_corporate_action_pypdf.v1"
DEFAULT_ARCHIVE_ROOT = Path("data/filings/corporate_actions")
_SAFE_NAME = re.compile(r"[^0-9A-Za-z_.-]+")


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


def normalize_page_text(value: str) -> str:
    """Normalize whitespace only; preserve wording for exact-quote validation."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _safe_component(value: str) -> str:
    result = _SAFE_NAME.sub("_", str(value or "").strip())
    return result[:120] or "unknown"


def extract_pdf_pages(
    pdf_bytes: bytes,
    *,
    ocr_adapter: Optional[CorporateActionOcrAdapter] = None,
) -> tuple[CorporateActionPageText, ...]:
    """Extract native page text and fail explicitly if OCR would be required."""
    if not pdf_bytes or not pdf_bytes.startswith(b"%PDF-"):
        raise ValueError("invalid_pdf_signature")
    try:
        from pypdf import PdfReader
        import io

        reader = PdfReader(io.BytesIO(pdf_bytes), strict=False)
    except Exception as exc:
        raise ValueError(f"pdf_parse_failed:{type(exc).__name__}") from exc
    pages: list[CorporateActionPageText] = []
    for index, page in enumerate(reader.pages, start=1):
        try:
            text = normalize_page_text(page.extract_text() or "")
        except Exception:
            text = ""
        extraction_method = "native_text"
        quality_status = "usable"
        if not text and ocr_adapter is not None:
            try:
                text = normalize_page_text(ocr_adapter.extract_page_text(pdf_bytes, index))
            except Exception:
                text = ""
            extraction_method = "ocr"
            quality_status = "ocr_usable" if len(text) >= 20 else "ocr_low_quality"
        if text and quality_status != "ocr_low_quality":
            pages.append(CorporateActionPageText(
                page_number=index,
                text=text,
                text_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                extraction_method=extraction_method,
                quality_status=quality_status,
            ))
    if not pages:
        raise ValueError("ocr_unavailable")
    return tuple(pages)


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
        announcement_id = str(announcement_id or "").strip()
        source_url = str(source_url or "").strip()
        if not announcement_id or not source_url:
            raise ValueError("announcement_id and source_url are required")
        source = str(source or "").strip().lower()
        if not source:
            raise ValueError("source is required")
        final_url = source_url
        content_type = "application/pdf"
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
                require_pdf=True,
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
        if not content.startswith(b"%PDF-"):
            raise ValueError("invalid_pdf_signature")
        digest = hashlib.sha256(content).hexdigest()
        relative = Path(_safe_component(announcement_id)) / f"{digest}.pdf"
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
        pages = tuple(
            CorporateActionPageText(
                page_number=page.page_number,
                text=page.text,
                text_hash=page.text_hash,
                announcement_id=announcement_id,
                extraction_method=page.extraction_method,
                quality_status=page.quality_status,
            )
            for page in extract_pdf_pages(content, ocr_adapter=self.ocr_adapter)
        )
        return CorporateActionDocumentBundle(
            announcement_id=announcement_id,
            source_url=final_url,
            content_hash=digest,
            content_type=content_type,
            content_length=len(content),
            archive_path=relative.as_posix(),
            pages=pages,
            extraction_status="extracted",
            source=source,
        )
