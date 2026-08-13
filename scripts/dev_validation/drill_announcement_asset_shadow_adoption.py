"""Drill legacy shadow adoption in an isolated temporary announcement catalog."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import (
    AnnouncementArchiveInventory,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
)
from scripts.dev_validation.inventory_announcement_asset_capacity import (
    _is_beneath,
    _read_legacy_manifest_rows,
    _require_complete_manifest_input,
    _validate_new_output_path,
    _write_new_json,
)
from utils.config_manager import config_manager

SCHEMA_VERSION = "annual_report_asset_shadow_adoption_drill.v1"
_COUNT_TABLES = frozenset(
    {
        "official_announcements",
        "official_announcement_attachments",
        "official_document_blobs",
        "official_attachment_versions",
        "effective_annual_reports",
        "official_annual_report_decisions",
    }
)


def _candidate_stats(
    inventory: Any,
) -> dict[str, tuple[int, int]]:
    return {
        item.path: (
            int(Path(item.path).stat().st_size),
            int(Path(item.path).stat().st_mtime_ns),
        )
        for item in inventory.items
        if item.status in {"adoptable", "duplicate", "superseded"}
    }


def _table_count(db_path: Path, table: str) -> int:
    if table not in _COUNT_TABLES:
        raise ValueError(f"unsupported shadow-adoption drill table: {table}")
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def run_drill(
    *,
    catalog_db: Path,
    financials_db: Path,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, Any]:
    """Run isolated adoption and prove no legacy file mutation."""

    allowed_temp_roots = tuple(
        root.resolve(strict=True)
        for root in (Path(tempfile.gettempdir()), Path("/dev/shm"))
        if root.is_dir()
    )
    if not any(_is_beneath(catalog_db, root) for root in allowed_temp_roots):
        raise ValueError("shadow adoption drill catalog must be under a temporary root")
    production_catalog = project_root / "data/research.db"
    same_production_file = (
        catalog_db.exists()
        and production_catalog.exists()
        and catalog_db.samefile(production_catalog)
    )
    if (
        catalog_db.resolve(strict=False) == production_catalog.resolve(strict=False)
        or same_production_file
    ):
        raise ValueError("shadow adoption drill refuses the production research catalog")
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(),
        project_root=project_root,
    )
    rows, manifest_input = _read_legacy_manifest_rows(
        financials_db=financials_db,
        project_root=project_root,
        config=config,
    )
    _require_complete_manifest_input(rows, manifest_input)
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory_registered(config=config, manifest_rows=rows)
    before = _candidate_stats(inventory)
    if catalog_db.exists():
        raise FileExistsError("shadow adoption drill catalog already exists")
    repository = AnnouncementAssetRepository(catalog_db)
    repository.initialize_schema()
    adoption = migration.shadow_adopt(
        inventory,
        repository=repository,
        observed_at=datetime.now(timezone.utc).isoformat(),
    )
    after = _candidate_stats(inventory)
    if before != after:
        raise RuntimeError("shadow adoption drill changed legacy candidate files")

    reports = []
    offset = 0
    while True:
        page = repository.list_effective_reports(
            include_shadow=True, limit=1000, offset=offset
        )
        if not page:
            break
        reports.extend(page)
        offset += len(page)
    scope_counts = Counter((item.instrument_id, item.fiscal_year) for item in reports)
    duplicate_scopes = sorted(
        f"{instrument_id}:{fiscal_year}"
        for (instrument_id, fiscal_year), count in scope_counts.items()
        if count != 1
    )
    if duplicate_scopes:
        raise RuntimeError(
            f"shadow adoption drill produced non-unique effective scopes: {duplicate_scopes[:3]}"
        )
    period_statuses = Counter(item.status for item in adoption.periods)
    review_required_periods = [
        {
            "instrument_id": item.instrument_id,
            "fiscal_year": item.fiscal_year,
            "status": item.status,
            "reason": item.reason,
            "source_file_ids": list(item.source_file_ids),
        }
        for item in adoption.periods
        if item.status != "current"
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "read_only_legacy_inputs": True,
        "network_requests": adoption.network_requests,
        "production_catalog_writes": 0,
        "temporary_catalog_path": str(catalog_db),
        "temporary_catalog_writes": True,
        "archive_mutations": {
            "moved": adoption.files_moved,
            "linked": adoption.files_linked,
            "quarantined": adoption.files_quarantined,
            "deleted": adoption.files_deleted,
        },
        "manifest_input": dict(manifest_input),
        "inventory": {
            "fingerprint": inventory.inventory_fingerprint,
            "counts": dict(inventory.counts),
            "files_seen": inventory.files_seen,
            "manifest_rows_seen": inventory.manifest_rows_seen,
        },
        "adoption": {
            "files_adopted": adoption.files_adopted,
            "legal_attachments_registered": adoption.legal_attachments_registered,
            "blobs_registered": adoption.blobs_registered,
            "period_count": len(adoption.periods),
            "period_statuses": dict(sorted(period_statuses.items())),
            "review_required_periods": review_required_periods,
            "skipped_counts": dict(adoption.skipped_counts),
            "effective_scope_count": len(scope_counts),
            "effective_scope_unique": not duplicate_scopes,
        },
        "catalog_counts": {
            "announcements": _table_count(catalog_db, "official_announcements"),
            "attachments": _table_count(catalog_db, "official_announcement_attachments"),
            "blobs": _table_count(catalog_db, "official_document_blobs"),
            "attachment_versions": _table_count(
                catalog_db, "official_attachment_versions"
            ),
            "effective_reports": _table_count(
                catalog_db, "effective_annual_reports"
            ),
            "effective_decisions": _table_count(
                catalog_db, "official_annual_report_decisions"
            ),
        },
        "promotion": {
            "run": False,
            "reason": "production custody, reconciliation, approval, and backup gates are absent",
        },
        "production_readiness": {
            "ready": False,
            "reason": "temporary drill is not production shadow adoption",
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-db", type=Path, required=True)
    parser.add_argument(
        "--financials-db",
        type=Path,
        default=PROJECT_ROOT / "data/financials.db",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output_path = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    result = run_drill(
        catalog_db=args.catalog_db,
        financials_db=args.financials_db,
    )
    _write_new_json(output_path, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
