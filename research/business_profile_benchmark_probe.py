"""Read-only official-document probes for the business-profile parser benchmark."""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from research.business_profile_archive import download_business_profile_candidate
from research.business_profile_discovery import (
    BusinessProfileDocumentCandidate,
    CninfoBusinessProfileDiscoveryAdapter,
)
from research.business_profile_pdf_artifacts import BusinessProfilePdfArtifactExtractor


BENCHMARK_PROBE_SCHEMA_VERSION = "business_profile_parser_benchmark_probe.v1"
PERIODIC_REPORT_DOCUMENT_TYPES = {
    "annual_report",
    "annual_report_correction",
    "semiannual_report",
    "semiannual_report_correction",
}
_SAFE_PATH_COMPONENT_RE = re.compile(r"[^0-9A-Za-z_.-]+")
_PRODUCTION_ARCHIVE_ROOT = Path("data/filings/business_profile")


def select_probe_issuers(
    benchmark: Mapping[str, Any],
    *,
    industry_groups: Iterable[str] = (),
    instrument_ids: Iterable[str] = (),
    max_issuers: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Select a deterministic bounded subset from a benchmark payload."""
    allowed_industries = {
        str(value).strip() for value in industry_groups if str(value).strip()
    }
    allowed_instruments = {
        str(value).strip() for value in instrument_ids if str(value).strip()
    }
    if max_issuers is not None and max_issuers < 1:
        raise ValueError("max_issuers must be at least 1")

    selected: List[Dict[str, Any]] = []
    industries = benchmark.get("industries")
    if not isinstance(industries, Mapping):
        raise ValueError("benchmark industries must be an object")
    for industry_group in sorted(industries):
        if allowed_industries and industry_group not in allowed_industries:
            continue
        industry = industries[industry_group]
        if not isinstance(industry, Mapping):
            continue
        issuers = industry.get("selected_issuers") or []
        for raw in issuers:
            if not isinstance(raw, Mapping):
                continue
            item = dict(raw)
            instrument_id = str(item.get("instrument_id") or "").strip()
            if not instrument_id:
                continue
            if allowed_instruments and instrument_id not in allowed_instruments:
                continue
            selected.append(
                {
                    **item,
                    "instrument_id": instrument_id,
                    "industry_group": industry_group,
                }
            )
    selected.sort(
        key=lambda item: (
            str(item["industry_group"]),
            str(item.get("exchange") or ""),
            str(item["instrument_id"]),
        )
    )
    bounded = selected if max_issuers is None else selected[:max_issuers]
    if allowed_instruments:
        found = {str(item["instrument_id"]) for item in bounded}
        missing = sorted(allowed_instruments - found)
        if missing:
            raise ValueError(
                "requested instruments are unavailable after benchmark filters: "
                + ", ".join(missing)
            )
    return bounded


def probe_benchmark_documents(
    benchmark: Mapping[str, Any],
    *,
    adapter: Optional[CninfoBusinessProfileDiscoveryAdapter] = None,
    downloader: Optional[Callable[[BusinessProfileDocumentCandidate], bytes]] = None,
    extractor: Optional[BusinessProfilePdfArtifactExtractor] = None,
    industry_groups: Iterable[str] = (),
    instrument_ids: Iterable[str] = (),
    max_issuers: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search_key: Optional[str] = "年度报告",
    category: Optional[str] = None,
    page_size: int = 30,
    max_pages: int = 5,
    download_root: Optional[Path] = None,
    max_documents_per_issuer: int = 1,
) -> Dict[str, Any]:
    """Probe official metadata and optionally diagnose PDFs without DB writes."""
    if page_size < 1 or max_pages < 1:
        raise ValueError("page_size and max_pages must be at least 1")
    if max_documents_per_issuer < 1:
        raise ValueError("max_documents_per_issuer must be at least 1")
    if bool(start_date) != bool(end_date):
        raise ValueError("start_date and end_date must be provided together")
    if download_root is not None and _is_within(
        Path(download_root),
        _PRODUCTION_ARCHIVE_ROOT,
    ):
        raise ValueError(
            "benchmark probe download_root must not use the production archive"
        )
    issuers = select_probe_issuers(
        benchmark,
        industry_groups=industry_groups,
        instrument_ids=instrument_ids,
        max_issuers=max_issuers,
    )
    discovery = adapter or CninfoBusinessProfileDiscoveryAdapter()
    fetch = downloader or download_business_profile_candidate
    pdf_extractor = extractor or BusinessProfilePdfArtifactExtractor()

    results: List[Dict[str, Any]] = []
    total_candidates = 0
    total_documents = 0
    total_failures = 0
    for issuer in issuers:
        item = _probe_issuer(
            issuer,
            adapter=discovery,
            downloader=fetch,
            extractor=pdf_extractor,
            start_date=start_date,
            end_date=end_date,
            search_key=search_key,
            category=category,
            page_size=page_size,
            max_pages=max_pages,
            download_root=download_root,
            max_documents=max_documents_per_issuer,
        )
        results.append(item)
        total_candidates += item["periodic_report_candidate_count"]
        total_documents += len(item["documents"])
        total_failures += len(item["errors"])

    missing_reports = sum(
        item["periodic_report_candidate_count"] == 0 for item in results
    )
    status = (
        "empty"
        if not results
        else "degraded" if total_failures or missing_reports else "success"
    )
    return {
        "schema_version": BENCHMARK_PROBE_SCHEMA_VERSION,
        "status": status,
        "mode": "pdf_diagnostics" if download_root is not None else "metadata_only",
        "query": {
            "start_date": start_date,
            "end_date": end_date,
            "search_key": search_key,
            "category": category,
            "page_size": page_size,
            "max_pages": max_pages,
            "max_documents_per_issuer": max_documents_per_issuer,
        },
        "selected_issuer_count": len(issuers),
        "periodic_report_candidate_count": total_candidates,
        "diagnosed_document_count": total_documents,
        "missing_report_issuer_count": missing_reports,
        "failure_count": total_failures,
        "results": results,
    }


def _probe_issuer(
    issuer: Mapping[str, Any],
    *,
    adapter: CninfoBusinessProfileDiscoveryAdapter,
    downloader: Callable[[BusinessProfileDocumentCandidate], bytes],
    extractor: BusinessProfilePdfArtifactExtractor,
    start_date: Optional[str],
    end_date: Optional[str],
    search_key: Optional[str],
    category: Optional[str],
    page_size: int,
    max_pages: int,
    download_root: Optional[Path],
    max_documents: int,
) -> Dict[str, Any]:
    instrument = {
        "instrument_id": issuer["instrument_id"],
        "symbol": issuer.get("symbol"),
        "exchange": issuer.get("exchange"),
    }
    errors: List[str] = []
    try:
        discovery = adapter.discover_instrument(
            instrument,
            start_date=start_date,
            end_date=end_date,
            search_key=search_key,
            category=category,
            page_size=page_size,
            max_pages=max_pages,
            dry_run=True,
        )
    except Exception as exc:
        return {
            **instrument,
            "company_name": issuer.get("company_name"),
            "industry_group": issuer.get("industry_group"),
            "discovery_status": "failed",
            "pages_scanned": 0,
            "announcements_seen": 0,
            "periodic_report_candidate_count": 0,
            "correction_candidate_count": 0,
            "candidate_titles": [],
            "documents": [],
            "errors": [f"discovery:{type(exc).__name__}: {exc}"],
        }

    periodic = [
        item
        for item in discovery.candidates
        if item.classification.document_type in PERIODIC_REPORT_DOCUMENT_TYPES
    ]
    periodic.sort(key=_candidate_sort_key, reverse=True)
    documents: List[Dict[str, Any]] = []
    if download_root is not None:
        for candidate in periodic[:max_documents]:
            try:
                content = downloader(candidate)
                if not content:
                    raise ValueError("downloaded attachment is empty")
                if not content.lstrip().startswith(b"%PDF-"):
                    raise ValueError("downloaded attachment is not a PDF")
                content_hash = hashlib.sha256(content).hexdigest()
                path = _write_probe_pdf(
                    Path(download_root),
                    instrument_id=str(instrument["instrument_id"]),
                    announcement_id=candidate.announcement_id,
                    content_hash=content_hash,
                    content=content,
                )
                artifact = extractor.extract_bytes(
                    content,
                    source_pdf_path=str(path),
                )
                if artifact.status == "parse_failed":
                    failure_class = artifact.diagnostics.get("failure_class")
                    errors.append(
                        "artifact:"
                        f"{candidate.announcement_id}:parse_failed:"
                        f"{failure_class or 'unknown'}"
                    )
                documents.append(
                    {
                        "announcement_id": candidate.announcement_id,
                        "title": candidate.title,
                        "announcement_time": candidate.announcement_time,
                        "document_type": candidate.classification.document_type,
                        "is_correction": candidate.classification.is_correction,
                        "adjunct_url": candidate.adjunct_url,
                        "content_hash": content_hash,
                        "local_path": str(path),
                        "artifact_status": artifact.status,
                        "artifact_hash": artifact.artifact_hash,
                        "page_count": artifact.page_count,
                        "heading_match_count": len(artifact.heading_index),
                        "low_text_pages": artifact.low_text_pages,
                        "ocr_required_pages": artifact.ocr_required_pages,
                        "parser_diagnostics": [
                            diagnostic.to_dict()
                            for diagnostic in artifact.parser_diagnostics
                        ],
                        "diagnostics": artifact.diagnostics,
                    }
                )
            except Exception as exc:
                errors.append(
                    "document:"
                    f"{candidate.announcement_id}:{type(exc).__name__}: {exc}"
                )
    errors.extend(discovery.errors)
    return {
        **instrument,
        "company_name": issuer.get("company_name"),
        "industry_group": issuer.get("industry_group"),
        "discovery_status": discovery.status,
        "pages_scanned": discovery.pages_scanned,
        "announcements_seen": discovery.announcements_seen,
        "periodic_report_candidate_count": len(periodic),
        "correction_candidate_count": sum(
            item.classification.is_correction for item in periodic
        ),
        "candidate_titles": [
            {
                "announcement_id": item.announcement_id,
                "title": item.title,
                "announcement_time": item.announcement_time,
                "document_type": item.classification.document_type,
                "is_correction": item.classification.is_correction,
                "adjunct_url": item.adjunct_url,
            }
            for item in periodic
        ],
        "documents": documents,
        "errors": errors,
    }


def _candidate_sort_key(
    candidate: BusinessProfileDocumentCandidate,
) -> tuple[str, int, str]:
    return (
        str(candidate.announcement_time or ""),
        int(candidate.classification.is_correction),
        candidate.announcement_id,
    )


def _write_probe_pdf(
    root: Path,
    *,
    instrument_id: str,
    announcement_id: str,
    content_hash: str,
    content: bytes,
) -> Path:
    safe_instrument = _safe_path_component(instrument_id)
    safe_announcement = _safe_path_component(announcement_id)
    path = root / safe_instrument / f"{safe_announcement}_{content_hash}.pdf"
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != content_hash:
            raise RuntimeError(f"probe PDF hash mismatch: {path}")
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_bytes(content)
    if hashlib.sha256(temporary.read_bytes()).hexdigest() != content_hash:
        temporary.unlink(missing_ok=True)
        raise RuntimeError("probe PDF write hash verification failed")
    os.replace(temporary, path)
    return path


def _safe_path_component(value: str) -> str:
    normalized = _SAFE_PATH_COMPONENT_RE.sub("_", str(value)).strip("._")
    if not normalized:
        raise ValueError("invalid empty path component")
    return normalized


def _is_within(path: Path, parent: Path) -> bool:
    resolved_path = path.resolve()
    resolved_parent = parent.resolve()
    return resolved_path == resolved_parent or resolved_parent in resolved_path.parents
