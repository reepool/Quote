"""Immutable archive and manifest governance for business-profile disclosures."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import (
    business_profile_document_family,
    infer_business_profile_report_period,
)
from research.providers.base import FinancialSourceFileManifest
from utils.date_utils import get_shanghai_time
from utils.http_transport import HttpTlsConfig, request_get


BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION = "business_profile_source_file_manifest.v1"
BUSINESS_PROFILE_ARCHIVE_VERSION = "business_profile_pdf_archive.v1"
BUSINESS_PROFILE_SOURCE_TIER = "official_primary"
LOGGER = logging.getLogger(__name__)
_SAFE_PATH_RE = re.compile(r"[^0-9A-Za-z_.-]+")


@dataclass(frozen=True)
class BusinessProfileArchiveRecord:
    """Outcome for one announcement attachment."""

    announcement_id: str
    report_period: str
    document_type: str
    content_hash: str
    archive_path: str
    source_file_id: str
    status: str
    supersedes_source_file_id: Optional[str] = None


@dataclass
class BusinessProfileArchiveBatchResult:
    """Bounded archive run summary."""

    attempted: int = 0
    archived: int = 0
    unchanged: int = 0
    skipped_checkpoint: int = 0
    failed: int = 0
    checkpoint_complete: bool = False
    records: List[BusinessProfileArchiveRecord] = field(default_factory=list)
    errors: List[Dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["records"] = [asdict(item) for item in self.records]
        return payload


class BusinessProfileDocumentArchiveService:
    """Download official PDFs into an immutable, resumable local archive."""

    def __init__(
        self,
        *,
        storage: Any,
        archive_root: str | Path = "data/filings/business_profile",
        downloader: Optional[
            Callable[[BusinessProfileDocumentCandidate], bytes]
        ] = None,
    ) -> None:
        self.storage = storage
        self.archive_root = Path(archive_root)
        self.downloader = downloader or self._download_candidate

    def archive_candidates(
        self,
        instrument: Dict[str, Any],
        candidates: Sequence[BusinessProfileDocumentCandidate],
        *,
        max_documents: int = 20,
        checkpoint_path: Optional[str | Path] = None,
        ingestion_run_id: Optional[int] = None,
    ) -> BusinessProfileArchiveBatchResult:
        """Archive a bounded batch and retain progress only while work is incomplete."""
        if max_documents < 1:
            raise ValueError("max_documents must be at least 1")
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError("instrument_id is required")
        checkpoint = self._load_checkpoint(checkpoint_path, instrument_id)
        completed = set(checkpoint.get("completed") or [])
        result = BusinessProfileArchiveBatchResult()

        for candidate in candidates:
            fingerprint = self._candidate_fingerprint(candidate)
            if fingerprint in completed:
                result.skipped_checkpoint += 1
                continue
            if result.attempted >= max_documents:
                break
            result.attempted += 1
            try:
                content = self.downloader(candidate)
                record = self.archive_content(
                    instrument,
                    candidate,
                    content,
                    ingestion_run_id=ingestion_run_id,
                )
            except Exception as exc:
                result.failed += 1
                result.errors.append(
                    {
                        "announcement_id": candidate.announcement_id,
                        "error": str(exc),
                    }
                )
                LOGGER.warning(
                    "business-profile document archive failed: instrument_id=%s announcement_id=%s error=%s",
                    instrument_id,
                    candidate.announcement_id,
                    exc,
                )
                continue
            result.records.append(record)
            if record.status == "unchanged":
                result.unchanged += 1
            else:
                result.archived += 1
            completed.add(fingerprint)
            self._write_checkpoint(checkpoint_path, instrument_id, completed)

        all_fingerprints = {self._candidate_fingerprint(item) for item in candidates}
        result.checkpoint_complete = (
            result.failed == 0 and all_fingerprints <= completed
        )
        if result.checkpoint_complete:
            self._remove_checkpoint(checkpoint_path)
        elif completed:
            self._write_checkpoint(checkpoint_path, instrument_id, completed)
        return result

    def archive_content(
        self,
        instrument: Dict[str, Any],
        candidate: BusinessProfileDocumentCandidate,
        content: bytes,
        *,
        ingestion_run_id: Optional[int] = None,
    ) -> BusinessProfileArchiveRecord:
        """Persist one verified PDF and its versioned source manifest."""
        if not isinstance(content, bytes) or not content:
            raise ValueError("downloaded attachment is empty")
        if not content.lstrip().startswith(b"%PDF-"):
            raise ValueError("downloaded attachment is not a PDF")
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip().upper()
        if not instrument_id or not symbol or not exchange:
            raise ValueError("instrument_id, symbol, and exchange are required")

        report_period = infer_business_profile_report_period(
            candidate.title,
            candidate.announcement_time,
        )
        document_type = candidate.classification.document_type
        document_family = business_profile_document_family(document_type)
        content_hash = hashlib.sha256(content).hexdigest()
        rows = self._get_manifests(
            instrument_id=instrument_id,
            report_period=report_period,
            source="cninfo",
        )
        exact = self._find_exact_manifest(rows, candidate.announcement_id, content_hash)
        archive_path = self._archive_path(
            exchange=exchange,
            symbol=symbol,
            report_period=report_period,
            announcement_id=candidate.announcement_id,
            content_hash=content_hash,
        )
        self._write_immutable(archive_path, content, content_hash)
        if exact is not None and str(exact.get("archive_path") or "") == str(
            archive_path
        ):
            return BusinessProfileArchiveRecord(
                announcement_id=candidate.announcement_id,
                report_period=report_period,
                document_type=document_type,
                content_hash=content_hash,
                archive_path=str(archive_path),
                source_file_id=str(exact["source_file_id"]),
                status="unchanged",
                supersedes_source_file_id=exact.get("supersedes_source_file_id"),
            )

        superseded = self._find_superseded_manifest(
            rows,
            candidate=candidate,
            document_family=document_family,
            content_hash=content_hash,
        )
        supersedes_source_file_id = (
            None if superseded is None else str(superseded["source_file_id"])
        )
        same_content = any(row.get("content_hash") == content_hash for row in rows)
        manifest = FinancialSourceFileManifest(
            source="cninfo",
            source_mode="direct",
            source_tier=BUSINESS_PROFILE_SOURCE_TIER,
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            report_type=document_type,
            filing_id=candidate.announcement_id,
            source_url=self._absolute_cninfo_url(candidate.adjunct_url),
            archive_path=str(archive_path),
            content_hash=content_hash,
            content_length=len(content),
            published_at=candidate.announcement_time,
            downloaded_at=get_shanghai_time().isoformat(),
            parser_version=BUSINESS_PROFILE_ARCHIVE_VERSION,
            source_file_id=None if exact is None else str(exact["source_file_id"]),
            supersedes_source_file_id=supersedes_source_file_id,
            status="archived_unchanged_content" if same_content else "archived",
            schema_version=BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
            metadata_json={
                "artifact_kind": "business_profile_official_pdf",
                "announcement_title": candidate.title,
                "document_family": document_family,
                "is_correction": candidate.classification.is_correction,
                "profile_event_hints": candidate.classification.profile_event_hints,
                "selection_reasons": candidate.selection_reasons,
            },
        )
        source_file_id = self._upsert_manifest(
            manifest,
            ingestion_run_id=ingestion_run_id,
        )
        return BusinessProfileArchiveRecord(
            announcement_id=candidate.announcement_id,
            report_period=report_period,
            document_type=document_type,
            content_hash=content_hash,
            archive_path=str(archive_path),
            source_file_id=source_file_id,
            status=manifest.status,
            supersedes_source_file_id=supersedes_source_file_id,
        )

    def _get_manifests(self, **kwargs: Any) -> List[Dict[str, Any]]:
        repository = getattr(self.storage, "financial_statements", None)
        if repository is not None and hasattr(repository, "get_source_file_manifests"):
            return repository.get_source_file_manifests(**kwargs)
        return self.storage.get_financial_source_file_manifests(**kwargs)

    def _upsert_manifest(
        self,
        manifest: FinancialSourceFileManifest,
        *,
        ingestion_run_id: Optional[int],
    ) -> str:
        repository = getattr(self.storage, "financial_statements", None)
        if repository is not None and hasattr(
            repository, "upsert_source_file_manifest"
        ):
            return repository.upsert_source_file_manifest(
                manifest,
                ingestion_run_id=ingestion_run_id,
            )
        return self.storage.upsert_financial_source_file_manifest(
            manifest,
            ingestion_run_id=ingestion_run_id,
        )

    @staticmethod
    def _find_exact_manifest(
        rows: Sequence[Dict[str, Any]],
        announcement_id: str,
        content_hash: str,
    ) -> Optional[Dict[str, Any]]:
        for row in rows:
            if (
                row.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
                and str(row.get("filing_id") or "") == announcement_id
                and row.get("content_hash") == content_hash
            ):
                return row
        return None

    @staticmethod
    def _find_superseded_manifest(
        rows: Sequence[Dict[str, Any]],
        *,
        candidate: BusinessProfileDocumentCandidate,
        document_family: str,
        content_hash: str,
    ) -> Optional[Dict[str, Any]]:
        eligible = []
        for row in rows:
            metadata = row.get("metadata") or {}
            same_family = metadata.get("document_family") == document_family
            same_filing = str(row.get("filing_id") or "") == candidate.announcement_id
            if (
                row.get("schema_version") == BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION
                and row.get("content_hash") != content_hash
                and same_family
                and (same_filing or candidate.classification.is_correction)
            ):
                eligible.append(row)
        if not eligible:
            return None
        return max(
            eligible,
            key=lambda row: str(row.get("published_at") or row.get("updated_at") or ""),
        )

    def _archive_path(
        self,
        *,
        exchange: str,
        symbol: str,
        report_period: str,
        announcement_id: str,
        content_hash: str,
    ) -> Path:
        safe_announcement = _SAFE_PATH_RE.sub("_", announcement_id).strip("._")
        if not safe_announcement:
            raise ValueError("announcement_id is invalid for archive path")
        return (
            self.archive_root
            / exchange
            / symbol
            / report_period
            / "original"
            / f"{safe_announcement}_{content_hash}.pdf"
        )

    @staticmethod
    def _write_immutable(path: Path, content: bytes, content_hash: str) -> None:
        if path.exists():
            existing_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            if existing_hash != content_hash:
                raise RuntimeError(f"immutable archive hash mismatch: {path}")
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_bytes(content)
        if hashlib.sha256(temporary.read_bytes()).hexdigest() != content_hash:
            temporary.unlink(missing_ok=True)
            raise RuntimeError("archive write hash verification failed")
        os.replace(temporary, path)

    @staticmethod
    def _candidate_fingerprint(candidate: BusinessProfileDocumentCandidate) -> str:
        payload = {
            "announcement_id": candidate.announcement_id,
            "announcement_time": candidate.announcement_time,
            "adjunct_url": candidate.adjunct_url,
            "document_type": candidate.classification.document_type,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _load_checkpoint(
        checkpoint_path: Optional[str | Path],
        instrument_id: str,
    ) -> Dict[str, Any]:
        if checkpoint_path is None or not Path(checkpoint_path).exists():
            return {"instrument_id": instrument_id, "completed": []}
        payload = json.loads(Path(checkpoint_path).read_text(encoding="utf-8"))
        if payload.get("instrument_id") != instrument_id:
            raise ValueError(
                "checkpoint instrument_id does not match requested instrument"
            )
        return payload

    @staticmethod
    def _write_checkpoint(
        checkpoint_path: Optional[str | Path],
        instrument_id: str,
        completed: set[str],
    ) -> None:
        if checkpoint_path is None:
            return
        path = Path(checkpoint_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".part")
        temporary.write_text(
            json.dumps(
                {
                    "version": 1,
                    "instrument_id": instrument_id,
                    "completed": sorted(completed),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        os.replace(temporary, path)

    @staticmethod
    def _remove_checkpoint(checkpoint_path: Optional[str | Path]) -> None:
        if checkpoint_path is not None:
            Path(checkpoint_path).unlink(missing_ok=True)

    @staticmethod
    def _absolute_cninfo_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        if url.startswith(("http://", "https://")):
            return url
        return f"https://static.cninfo.com.cn/{url.lstrip('/')}"

    @classmethod
    def _download_candidate(cls, candidate: BusinessProfileDocumentCandidate) -> bytes:
        url = cls._absolute_cninfo_url(candidate.adjunct_url)
        if not url:
            raise ValueError("candidate attachment URL is missing")
        response = request_get(
            url,
            tls_config=HttpTlsConfig(source_name="cninfo"),
            timeout=20,
        )
        response.raise_for_status()
        return bytes(response.content or b"")
