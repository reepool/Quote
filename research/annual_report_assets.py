"""Reusable annual-report PDF catalog projected from canonical source manifests."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

ANNUAL_REPORT_TYPES = ("annual_report", "annual_report_correction")
ANNUAL_REPORT_ASSET_SCHEMA_VERSION = "annual_report_asset_catalog.v1"
BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION = "business_profile_source_file_manifest.v1"
BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES = (
    "archived",
    "archived_unchanged_content",
    "verified",
    "success",
)


class AnnualReportAssetCatalog:
    """Query and validate immutable annual-report assets for cross-module reuse."""

    def __init__(self, storage: Any, *, archive_base: str | Path | None = None):
        self.storage = storage
        self.archive_base = (
            Path(archive_base) if archive_base is not None else Path.cwd()
        )

    def list_assets(
        self,
        *,
        instrument_id: str | None = None,
        report_period: str | None = None,
        filing_id: str | None = None,
        source: str | None = None,
        knowledge_cutoff: str | None = None,
        active_only: bool = False,
        validate_files: bool = False,
    ) -> list[dict[str, Any]]:
        """Return catalog rows, retaining history unless ``active_only`` is set."""

        rows = self._get_manifests(
            instrument_id=instrument_id,
            report_period=report_period,
            source=source,
            report_types=ANNUAL_REPORT_TYPES,
            filing_id=filing_id,
            schema_version=BUSINESS_PROFILE_MANIFEST_SCHEMA_VERSION,
            statuses=tuple(BUSINESS_PROFILE_USABLE_MANIFEST_STATUSES),
            published_before=knowledge_cutoff,
        )
        grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        for row in rows:
            key = (
                str(row.get("instrument_id") or ""),
                str(row.get("report_period") or ""),
            )
            grouped.setdefault(key, []).append(row)
        active_ids = set()
        for group in grouped.values():
            superseded_ids = {
                str(item.get("supersedes_source_file_id") or "")
                for item in group
                if item.get("supersedes_source_file_id")
            }
            lineage_heads = [
                item
                for item in group
                if str(item.get("source_file_id") or "") not in superseded_ids
            ]
            active = max(lineage_heads or group, key=_asset_sort_key)
            active_ids.add(str(active.get("source_file_id") or ""))
        assets = []
        for row in rows:
            source_file_id = str(row.get("source_file_id") or "")
            is_active = source_file_id in active_ids
            if active_only and not is_active:
                continue
            assets.append(
                self._catalog_row(
                    row,
                    is_active=is_active,
                    validate_file=validate_files,
                )
            )
        return sorted(assets, key=_asset_sort_key, reverse=True)

    def get_asset(
        self,
        instrument_id: str,
        *,
        report_period: str | None = None,
        knowledge_cutoff: str | None = None,
        validate_file: bool = True,
    ) -> dict[str, Any] | None:
        """Return the latest active report, optionally constrained to one period."""

        assets = self.list_assets(
            instrument_id=instrument_id,
            report_period=report_period,
            knowledge_cutoff=knowledge_cutoff,
            active_only=True,
            validate_files=validate_file,
        )
        if validate_file:
            assets = [item for item in assets if item["integrity_status"] == "valid"]
        return assets[0] if assets else None

    def find_reusable_filing(
        self,
        *,
        instrument_id: str,
        report_period: str,
        source: str,
        filing_id: str,
    ) -> dict[str, Any] | None:
        """Return an exact verified source filing suitable for download avoidance."""

        assets = self.list_assets(
            instrument_id=instrument_id,
            report_period=report_period,
            source=source,
            filing_id=filing_id,
            active_only=False,
            validate_files=True,
        )
        return next(
            (item for item in assets if item["integrity_status"] == "valid"),
            None,
        )

    def _catalog_row(
        self,
        row: Mapping[str, Any],
        *,
        is_active: bool,
        validate_file: bool,
    ) -> dict[str, Any]:
        registered_path = str(row.get("archive_path") or "")
        resolved_path = self._resolve_path(registered_path)
        integrity_status = (
            self._validate_file(row, resolved_path) if validate_file else "unchecked"
        )
        return {
            "schema_version": ANNUAL_REPORT_ASSET_SCHEMA_VERSION,
            "source_file_id": str(row.get("source_file_id") or ""),
            "instrument_id": str(row.get("instrument_id") or ""),
            "symbol": str(row.get("symbol") or ""),
            "exchange": str(row.get("exchange") or ""),
            "report_period": str(row.get("report_period") or ""),
            "report_type": str(row.get("report_type") or ""),
            "filing_id": str(row.get("filing_id") or ""),
            "source": str(row.get("source") or ""),
            "source_url": row.get("source_url"),
            "archive_path": registered_path,
            "resolved_archive_path": str(resolved_path),
            "content_hash": str(row.get("content_hash") or ""),
            "content_length": row.get("content_length"),
            "published_at": row.get("published_at"),
            "downloaded_at": row.get("downloaded_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "status": str(row.get("status") or ""),
            "supersedes_source_file_id": row.get("supersedes_source_file_id"),
            "is_active": is_active,
            "integrity_status": integrity_status,
            "metadata": dict(row.get("metadata") or {}),
        }

    def _validate_file(self, row: Mapping[str, Any], path: Path) -> str:
        if not path.is_file():
            return "missing"
        try:
            stat = path.stat()
            expected_length = row.get("content_length")
            if expected_length is not None and stat.st_size != int(expected_length):
                return "size_mismatch"
            with path.open("rb") as handle:
                if handle.read(5) != b"%PDF-":
                    return "not_pdf"
                digest = hashlib.sha256()
                handle.seek(0)
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            expected_hash = str(row.get("content_hash") or "")
            if expected_hash and digest.hexdigest() != expected_hash:
                return "hash_mismatch"
        except (OSError, TypeError, ValueError):
            return "unreadable"
        return "valid"

    def _resolve_path(self, archive_path: str) -> Path:
        path = Path(archive_path)
        return path if path.is_absolute() else self.archive_base / path

    def _get_manifests(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]:
        repository = getattr(self.storage, "financial_statements", None)
        if repository is not None and hasattr(repository, "get_source_file_manifests"):
            return repository.get_source_file_manifests(**kwargs)
        return self.storage.get_financial_source_file_manifests(**kwargs)


def _asset_sort_key(row: Mapping[str, Any]) -> tuple[str, str, int, str]:
    report_type = str(row.get("report_type") or "")
    return (
        str(row.get("report_period") or ""),
        str(row.get("published_at") or row.get("downloaded_at") or ""),
        int(report_type == "annual_report_correction"),
        str(row.get("source_file_id") or ""),
    )
