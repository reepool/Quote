"""Auditable page artifacts for archived company business-profile PDFs."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


BUSINESS_PROFILE_PDF_ARTIFACT_SCHEMA_VERSION = "business_profile_pdf_artifact.v1"
BUSINESS_PROFILE_PDF_EXTRACTOR_VERSION = "business_profile_pypdf.v1"
DEFAULT_LOW_TEXT_CHARACTER_THRESHOLD = 40
DEFAULT_HEADING_ALIASES: Dict[str, Sequence[str]] = {
    "principal_business": ("主要业务", "主营业务", "principal business"),
    "business_model": ("经营模式", "business model"),
    "segment_information": (
        "分部信息",
        "分行业",
        "分产品",
        "segment information",
    ),
    "revenue_cost_analysis": (
        "营业收入和营业成本",
        "收入和成本分析",
        "revenue and cost",
    ),
    "production_sales_inventory": (
        "产销量",
        "生产量",
        "销售量",
        "库存量",
        "production and sales",
    ),
    "cost_composition": ("成本构成", "成本分析", "cost composition"),
    "resources_and_reserves": ("资源储量", "矿产资源", "resources and reserves"),
    "major_projects": ("重大项目", "在建工程", "major projects"),
    "derivatives_and_hedging": (
        "套期保值",
        "期货和衍生品",
        "衍生工具",
        "hedging",
        "derivatives",
    ),
}
_SAFE_COMPONENT_RE = re.compile(r"[^0-9A-Za-z_.-]+")
DERIVED_ARTIFACT_DIRECTORIES = {
    "page_text": "derived",
    "tables": "tables",
    "ocr": "ocr",
}


@dataclass(frozen=True)
class BusinessProfileHeadingMatch:
    """One deterministic heading alias match."""

    heading_type: str
    alias: str
    page_number: int
    line_number: int
    text: str


@dataclass(frozen=True)
class BusinessProfilePdfPageArtifact:
    """Native-text diagnostics for one PDF page."""

    page_number: int
    width_points: Optional[float]
    height_points: Optional[float]
    text: str
    text_hash: str
    text_character_count: int
    non_whitespace_character_count: int
    text_density_per_square_inch: Optional[float]
    native_text_status: str
    field_relevant: bool
    ocr_required: bool
    heading_matches: List[BusinessProfileHeadingMatch] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    page_artifact_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["heading_matches"] = [asdict(item) for item in self.heading_matches]
        return payload


@dataclass(frozen=True)
class BusinessProfilePdfArtifact:
    """Reproducible PDF extraction result and derived-artifact manifest."""

    schema_version: str
    extractor_version: str
    source_file_id: Optional[str]
    source_pdf_path: Optional[str]
    source_content_hash: str
    parameter_hash: str
    status: str
    encrypted: bool
    page_count: int
    pages: List[BusinessProfilePdfPageArtifact]
    heading_index: List[BusinessProfileHeadingMatch]
    low_text_pages: List[int]
    ocr_required_pages: List[int]
    diagnostics: Dict[str, Any]
    artifact_hash: str

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["pages"] = [item.to_dict() for item in self.pages]
        payload["heading_index"] = [asdict(item) for item in self.heading_index]
        return payload


@dataclass(frozen=True)
class BusinessProfilePdfArtifactWriteResult:
    """Immutable derived-artifact write outcome."""

    artifact_path: str
    artifact_hash: str
    status: str


class BusinessProfilePdfArtifactExtractor:
    """Validate a PDF and extract deterministic native page artifacts."""

    def __init__(
        self,
        *,
        extractor_version: str = BUSINESS_PROFILE_PDF_EXTRACTOR_VERSION,
        low_text_character_threshold: int = DEFAULT_LOW_TEXT_CHARACTER_THRESHOLD,
        heading_aliases: Optional[Mapping[str, Sequence[str]]] = None,
    ) -> None:
        self.extractor_version = str(extractor_version).strip()
        if not self.extractor_version:
            raise ValueError("extractor_version is required")
        self.low_text_character_threshold = max(0, int(low_text_character_threshold))
        aliases = heading_aliases or DEFAULT_HEADING_ALIASES
        self.heading_aliases = {
            str(heading_type): tuple(
                str(alias).strip() for alias in values if str(alias).strip()
            )
            for heading_type, values in aliases.items()
        }

    def extract_file(
        self,
        path: str | Path,
        *,
        source_file_id: Optional[str] = None,
        target_page_numbers: Iterable[int] = (),
    ) -> BusinessProfilePdfArtifact:
        """Read one archived PDF and extract its page artifacts."""
        source_path = Path(path)
        return self.extract_bytes(
            source_path.read_bytes(),
            source_file_id=source_file_id,
            source_pdf_path=str(source_path),
            target_page_numbers=target_page_numbers,
        )

    def extract_bytes(
        self,
        content: bytes,
        *,
        source_file_id: Optional[str] = None,
        source_pdf_path: Optional[str] = None,
        target_page_numbers: Iterable[int] = (),
    ) -> BusinessProfilePdfArtifact:
        """Extract an artifact without writing it to disk."""
        content_bytes = bytes(content or b"")
        content_hash = hashlib.sha256(content_bytes).hexdigest()
        target_pages = self._normalize_target_pages(target_page_numbers)
        parameter_hash = self._parameter_hash(target_pages)
        if not content_bytes.lstrip().startswith(b"%PDF-"):
            return self._failure_artifact(
                source_file_id=source_file_id,
                source_pdf_path=source_pdf_path,
                source_content_hash=content_hash,
                parameter_hash=parameter_hash,
                failure_class="invalid_pdf_signature",
            )
        try:
            from pypdf import PdfReader
        except ImportError:
            return self._failure_artifact(
                source_file_id=source_file_id,
                source_pdf_path=source_pdf_path,
                source_content_hash=content_hash,
                parameter_hash=parameter_hash,
                failure_class="pypdf_unavailable",
            )
        try:
            reader = PdfReader(io.BytesIO(content_bytes), strict=False)
        except Exception as exc:
            return self._failure_artifact(
                source_file_id=source_file_id,
                source_pdf_path=source_pdf_path,
                source_content_hash=content_hash,
                parameter_hash=parameter_hash,
                failure_class="malformed_pdf",
                error=exc,
            )

        encrypted = bool(reader.is_encrypted)
        if encrypted:
            try:
                decrypt_result = reader.decrypt("")
            except Exception as exc:
                return self._failure_artifact(
                    source_file_id=source_file_id,
                    source_pdf_path=source_pdf_path,
                    source_content_hash=content_hash,
                    parameter_hash=parameter_hash,
                    failure_class="encrypted_password_required",
                    error=exc,
                    encrypted=True,
                )
            if not decrypt_result:
                return self._failure_artifact(
                    source_file_id=source_file_id,
                    source_pdf_path=source_pdf_path,
                    source_content_hash=content_hash,
                    parameter_hash=parameter_hash,
                    failure_class="encrypted_password_required",
                    encrypted=True,
                )

        try:
            page_count = len(reader.pages)
        except Exception as exc:
            return self._failure_artifact(
                source_file_id=source_file_id,
                source_pdf_path=source_pdf_path,
                source_content_hash=content_hash,
                parameter_hash=parameter_hash,
                failure_class="malformed_page_tree",
                error=exc,
                encrypted=encrypted,
            )
        if page_count < 1:
            return self._failure_artifact(
                source_file_id=source_file_id,
                source_pdf_path=source_pdf_path,
                source_content_hash=content_hash,
                parameter_hash=parameter_hash,
                failure_class="empty_pdf",
                encrypted=encrypted,
            )

        pages: List[BusinessProfilePdfPageArtifact] = []
        heading_index: List[BusinessProfileHeadingMatch] = []
        extraction_error_pages: List[int] = []
        for page_number, page in enumerate(reader.pages, start=1):
            page_errors: List[str] = []
            try:
                text = page.extract_text() or ""
                native_text_status = "extracted" if text.strip() else "empty"
            except Exception as exc:
                text = ""
                native_text_status = "extraction_error"
                page_errors.append(f"{type(exc).__name__}: {exc}")
                extraction_error_pages.append(page_number)
            width, height = self._page_dimensions(page)
            non_whitespace = len(re.sub(r"\s+", "", text))
            matches = self._heading_matches(text, page_number)
            heading_index.extend(matches)
            low_text = non_whitespace < self.low_text_character_threshold
            field_relevant = bool(matches) or page_number in target_pages
            ocr_required = field_relevant and (
                low_text or native_text_status == "extraction_error"
            )
            page_payload = {
                "page_number": page_number,
                "width_points": width,
                "height_points": height,
                "text_hash": self._hash_text(text),
                "text_character_count": len(text),
                "non_whitespace_character_count": non_whitespace,
                "text_density_per_square_inch": self._text_density(
                    non_whitespace,
                    width,
                    height,
                ),
                "native_text_status": native_text_status,
                "field_relevant": field_relevant,
                "ocr_required": ocr_required,
                "heading_matches": [asdict(item) for item in matches],
                "errors": page_errors,
            }
            pages.append(
                BusinessProfilePdfPageArtifact(
                    text=text,
                    page_artifact_hash=self._stable_hash(page_payload),
                    **{
                        key: value
                        for key, value in page_payload.items()
                        if key != "heading_matches"
                    },
                    heading_matches=matches,
                )
            )

        low_text_pages = [
            item.page_number
            for item in pages
            if item.non_whitespace_character_count < self.low_text_character_threshold
        ]
        ocr_required_pages = [item.page_number for item in pages if item.ocr_required]
        status = (
            "partial"
            if extraction_error_pages
            else "ocr_required" if ocr_required_pages else "parsed"
        )
        diagnostics = {
            "failure_class": None,
            "low_text_character_threshold": self.low_text_character_threshold,
            "target_page_numbers": sorted(target_pages),
            "native_text_page_count": sum(
                item.native_text_status == "extracted" for item in pages
            ),
            "empty_text_page_count": sum(
                item.native_text_status == "empty" for item in pages
            ),
            "extraction_error_pages": extraction_error_pages,
            "heading_match_count": len(heading_index),
        }
        payload = self._artifact_payload(
            source_file_id=source_file_id,
            source_pdf_path=source_pdf_path,
            source_content_hash=content_hash,
            parameter_hash=parameter_hash,
            status=status,
            encrypted=encrypted,
            page_count=page_count,
            pages=pages,
            heading_index=heading_index,
            low_text_pages=low_text_pages,
            ocr_required_pages=ocr_required_pages,
            diagnostics=diagnostics,
        )
        return BusinessProfilePdfArtifact(
            **payload,
            artifact_hash=self._artifact_hash(payload),
        )

    def _failure_artifact(
        self,
        *,
        source_file_id: Optional[str],
        source_pdf_path: Optional[str],
        source_content_hash: str,
        parameter_hash: str,
        failure_class: str,
        error: Optional[Exception] = None,
        encrypted: bool = False,
    ) -> BusinessProfilePdfArtifact:
        diagnostics = {
            "failure_class": failure_class,
            "error": None if error is None else f"{type(error).__name__}: {error}",
            "low_text_character_threshold": self.low_text_character_threshold,
        }
        payload = self._artifact_payload(
            source_file_id=source_file_id,
            source_pdf_path=source_pdf_path,
            source_content_hash=source_content_hash,
            parameter_hash=parameter_hash,
            status="parse_failed",
            encrypted=encrypted,
            page_count=0,
            pages=[],
            heading_index=[],
            low_text_pages=[],
            ocr_required_pages=[],
            diagnostics=diagnostics,
        )
        return BusinessProfilePdfArtifact(
            **payload,
            artifact_hash=self._artifact_hash(payload),
        )

    def _artifact_payload(self, **kwargs: Any) -> Dict[str, Any]:
        return {
            "schema_version": BUSINESS_PROFILE_PDF_ARTIFACT_SCHEMA_VERSION,
            "extractor_version": self.extractor_version,
            **kwargs,
        }

    @staticmethod
    def _serializable_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
        output = dict(payload)
        output["pages"] = [item.to_dict() for item in payload.get("pages") or []]
        output["heading_index"] = [
            asdict(item) for item in payload.get("heading_index") or []
        ]
        return output

    def _artifact_hash(self, payload: Mapping[str, Any]) -> str:
        hashable = self._serializable_payload(payload)
        hashable.pop("source_pdf_path", None)
        hashable.pop("source_file_id", None)
        return self._stable_hash(hashable)

    def _parameter_hash(self, target_pages: set[int]) -> str:
        return self._stable_hash(
            {
                "extractor_version": self.extractor_version,
                "low_text_character_threshold": self.low_text_character_threshold,
                "heading_aliases": {
                    key: list(self.heading_aliases[key])
                    for key in sorted(self.heading_aliases)
                },
                "target_page_numbers": sorted(target_pages),
            }
        )

    @staticmethod
    def _normalize_target_pages(values: Iterable[int]) -> set[int]:
        pages: set[int] = set()
        for value in values:
            try:
                page = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError("target page numbers must be integers") from exc
            if page < 1:
                raise ValueError("target page numbers must be positive and one-based")
            pages.add(page)
        return pages

    def _heading_matches(
        self,
        text: str,
        page_number: int,
    ) -> List[BusinessProfileHeadingMatch]:
        matches: List[BusinessProfileHeadingMatch] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            normalized = self._normalize_text(line)
            if not normalized:
                continue
            for heading_type, aliases in self.heading_aliases.items():
                for alias in aliases:
                    if self._normalize_text(alias) in normalized:
                        matches.append(
                            BusinessProfileHeadingMatch(
                                heading_type=heading_type,
                                alias=alias,
                                page_number=page_number,
                                line_number=line_number,
                                text=line.strip(),
                            )
                        )
                        break
        return matches

    @staticmethod
    def _normalize_text(value: str) -> str:
        return re.sub(r"\s+", "", str(value or "")).lower()

    @staticmethod
    def _hash_text(value: str) -> str:
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()

    @staticmethod
    def _stable_hash(value: Any) -> str:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _page_dimensions(page: Any) -> tuple[Optional[float], Optional[float]]:
        try:
            return float(page.mediabox.width), float(page.mediabox.height)
        except (AttributeError, TypeError, ValueError):
            return None, None

    @staticmethod
    def _text_density(
        character_count: int,
        width_points: Optional[float],
        height_points: Optional[float],
    ) -> Optional[float]:
        if not width_points or not height_points:
            return None
        square_inches = (width_points * height_points) / (72.0 * 72.0)
        if square_inches <= 0:
            return None
        return round(character_count / square_inches, 6)


class BusinessProfilePdfArtifactStore:
    """Write compressed, immutable page artifacts beside the archived original."""

    def artifact_path(
        self,
        source_pdf_path: str | Path,
        *,
        source_content_hash: str,
        extractor_version: str,
        parameter_hash: Optional[str] = None,
        artifact_kind: str = "page_text",
    ) -> Path:
        source_path = Path(source_pdf_path)
        document_root = (
            source_path.parent.parent
            if source_path.parent.name == "original"
            else source_path.parent
        )
        safe_version = _SAFE_COMPONENT_RE.sub("_", extractor_version).strip("._")
        if not safe_version:
            raise ValueError("extractor_version is invalid for artifact path")
        directory = DERIVED_ARTIFACT_DIRECTORIES.get(str(artifact_kind))
        if directory is None:
            raise ValueError(f"unsupported derived artifact kind: {artifact_kind}")
        parameter_suffix = f"_{parameter_hash[:16]}" if parameter_hash else ""
        return (
            document_root
            / directory
            / safe_version
            / f"{source_content_hash}{parameter_suffix}.json.gz"
        )

    def write(
        self,
        artifact: BusinessProfilePdfArtifact,
        *,
        source_pdf_path: Optional[str | Path] = None,
    ) -> BusinessProfilePdfArtifactWriteResult:
        path_value = source_pdf_path or artifact.source_pdf_path
        if not path_value:
            raise ValueError("source_pdf_path is required")
        path = self.artifact_path(
            path_value,
            source_content_hash=artifact.source_content_hash,
            extractor_version=artifact.extractor_version,
            parameter_hash=artifact.parameter_hash,
            artifact_kind="page_text",
        )
        stored_payload = artifact.to_dict()
        stored_payload["source_file_id"] = None
        stored_payload["source_pdf_path"] = None
        raw = json.dumps(
            stored_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        compressed = gzip.compress(raw, compresslevel=9, mtime=0)
        if path.exists():
            existing = self.read(path)
            if existing.get("artifact_hash") != artifact.artifact_hash:
                raise RuntimeError(f"immutable derived artifact hash mismatch: {path}")
            return BusinessProfilePdfArtifactWriteResult(
                artifact_path=str(path),
                artifact_hash=artifact.artifact_hash,
                status="unchanged",
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_bytes(compressed)
        try:
            stored = self.read(temporary)
            if stored.get("artifact_hash") != artifact.artifact_hash:
                raise RuntimeError("derived artifact write verification failed")
            os.replace(temporary, path)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return BusinessProfilePdfArtifactWriteResult(
            artifact_path=str(path),
            artifact_hash=artifact.artifact_hash,
            status="written",
        )

    @staticmethod
    def read(path: str | Path) -> Dict[str, Any]:
        return json.loads(gzip.decompress(Path(path).read_bytes()).decode("utf-8"))
