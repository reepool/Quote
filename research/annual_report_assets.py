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

    def __init__(
        self,
        storage: Any,
        *,
        archive_base: str | Path | None = None,
        shared_asset_access: Any | None = None,
        mode: str | None = None,
    ):
        self.storage = storage
        self.archive_base = (
            Path(archive_base) if archive_base is not None else Path.cwd()
        )
        research_config = getattr(storage, "research_config", None)
        modules = getattr(research_config, "modules", {}) or {}
        business_profile_cfg = (
            modules.get("business_profile_evidence", {})
            if isinstance(modules, Mapping)
            else {}
        )
        dependency_cfg = (
            business_profile_cfg.get("annual_report_asset_dependency", {})
            if isinstance(business_profile_cfg, Mapping)
            else {}
        )
        self.mode = str(mode or dependency_cfg.get("mode", "legacy")).strip().lower()
        if self.mode not in {"legacy", "dual_read", "shared_only"}:
            raise ValueError("invalid annual-report compatibility catalog mode")
        self.shared_asset_access = shared_asset_access
        if self.shared_asset_access is None and self.mode != "legacy":
            self.shared_asset_access = self._build_shared_access(research_config)

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

        if self.mode != "legacy":
            shared_assets = self._list_shared_assets(
                instrument_id=instrument_id,
                report_period=report_period,
                filing_id=filing_id,
                source=source,
                knowledge_cutoff=knowledge_cutoff,
                active_only=active_only,
                validate_files=validate_files,
            )
            if self.mode == "shared_only" or shared_assets:
                return shared_assets

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

        if self.mode != "legacy":
            shared = self._find_shared_filing(
                instrument_id=instrument_id,
                report_period=report_period,
                source=source,
                filing_id=filing_id,
            )
            if self.mode == "shared_only" or shared is not None:
                return shared

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

    def _list_shared_assets(
        self,
        *,
        instrument_id: str | None,
        report_period: str | None,
        filing_id: str | None,
        source: str | None,
        knowledge_cutoff: str | None,
        active_only: bool,
        validate_files: bool,
    ) -> list[dict[str, Any]]:
        if self.shared_asset_access is None:
            if self.mode == "shared_only":
                raise RuntimeError("shared annual-report catalog access is unavailable")
            return []
        fiscal_year = (
            int(str(report_period)[:4])
            if report_period and len(str(report_period)) >= 4
            else None
        )
        projection = self.shared_asset_access.list_assets(
            instrument_id=instrument_id,
            fiscal_year=fiscal_year,
            source=source,
            limit=1000,
        )
        rows: list[dict[str, Any]] = []
        for asset in projection.get("items", ()):
            if active_only and not asset.get("asset_id"):
                continue
            if report_period and str(asset.get("report_period") or "") != str(
                report_period
            ):
                continue
            if filing_id and str(asset.get("source_announcement_id") or "") != str(
                filing_id
            ):
                continue
            if knowledge_cutoff and str(asset.get("published_at") or "") > str(
                knowledge_cutoff
            ):
                continue
            if knowledge_cutoff and str(
                asset.get("version_available_at") or ""
            ) > str(knowledge_cutoff):
                continue
            rows.append(
                self._shared_catalog_row(asset, validate_file=validate_files)
            )
        return sorted(rows, key=_asset_sort_key, reverse=True)

    def _find_shared_filing(
        self,
        *,
        instrument_id: str,
        report_period: str,
        source: str,
        filing_id: str,
    ) -> dict[str, Any] | None:
        if self.shared_asset_access is None:
            return None
        from research.announcement_assets import EnsureRequest

        ensured = self.shared_asset_access.ensure(
            EnsureRequest(
                instrument_id=instrument_id,
                source=source,
                source_announcement_id=filing_id,
                allow_network=False,
                consumer="annual_report_catalog_compatibility",
                principal="internal",
            )
        )
        asset = ensured.get("asset")
        if not asset or ensured.get("availability") != "local_valid":
            return None
        if str(asset.get("report_period") or "") != str(report_period):
            return None
        return self._shared_catalog_row(asset, validate_file=True)

    def _shared_catalog_row(
        self,
        asset: Mapping[str, Any],
        *,
        validate_file: bool,
    ) -> dict[str, Any]:
        content = None
        integrity_status = str(asset.get("integrity") or "unchecked")
        try:
            content = self.shared_asset_access.content_handle(str(asset["asset_id"]))
        except (FileNotFoundError, KeyError, RuntimeError, ValueError):
            if validate_file:
                integrity_status = "missing"
        else:
            handle = content.get("file_handle")
            if handle is not None:
                handle.close()
            integrity_status = "valid"
        archive_path = "" if content is None else str(content.get("path") or "")
        return {
            "schema_version": ANNUAL_REPORT_ASSET_SCHEMA_VERSION,
            "source_file_id": f"shared-asset:{asset['asset_id']}",
            "shared_asset_id": str(asset["asset_id"]),
            "instrument_id": str(asset.get("instrument_id") or ""),
            "symbol": str(asset.get("instrument_id") or "").split(".", 1)[0],
            "exchange": str(asset.get("instrument_id") or "").split(".", 1)[-1],
            "report_period": str(asset.get("report_period") or ""),
            "report_type": (
                "annual_report_correction"
                if asset.get("is_correction")
                else "annual_report"
            ),
            "filing_id": str(asset.get("source_announcement_id") or ""),
            "source": str(asset.get("source") or ""),
            "source_url": None,
            "archive_path": archive_path,
            "resolved_archive_path": archive_path,
            "content_hash": str(asset.get("content_hash") or ""),
            "content_length": asset.get("content_length"),
            "published_at": asset.get("published_at"),
            "downloaded_at": None,
            "created_at": asset.get("activated_at"),
            "updated_at": asset.get("last_checked_at"),
            "status": "verified" if integrity_status == "valid" else "unavailable",
            "supersedes_source_file_id": (
                None
                if not asset.get("predecessor_asset_id")
                else f"shared-asset:{asset['predecessor_asset_id']}"
            ),
            "is_active": True,
            "integrity_status": integrity_status,
            "metadata": {
                "shared_asset_id": asset.get("asset_id"),
                "observation_version": asset.get("observation_version"),
                "effective_decision_state": asset.get(
                    "effective_decision_state"
                ),
                "compatibility_mode": self.mode,
            },
        }

    @staticmethod
    def _build_shared_access(research_config: Any) -> Any | None:
        storage_config = getattr(research_config, "storage", None)
        db_path = getattr(storage_config, "db_path", None)
        if not db_path:
            return None
        from research.announcement_assets import (
            AnnouncementAssetAccess,
            AnnouncementAssetConfig,
            AnnouncementAssetRepository,
        )

        config = AnnouncementAssetConfig.from_research_config(
            research_config,
            project_root=Path.cwd(),
        )
        path = Path(str(db_path))
        if not path.is_absolute():
            path = Path.cwd() / path
        return AnnouncementAssetAccess(
            repository=AnnouncementAssetRepository(path),
            config=config,
        )


def _asset_sort_key(row: Mapping[str, Any]) -> tuple[str, str, int, str]:
    report_type = str(row.get("report_type") or "")
    return (
        str(row.get("report_period") or ""),
        str(row.get("published_at") or row.get("downloaded_at") or ""),
        int(report_type == "annual_report_correction"),
        str(row.get("source_file_id") or ""),
    )
