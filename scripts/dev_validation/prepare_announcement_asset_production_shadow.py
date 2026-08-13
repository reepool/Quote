#!/usr/bin/env python3
"""Prepare or explicitly apply production shadow adoption for annual reports.

The default mode writes only an isolated SQLite simulation.  Production apply
requires an exact preflight artifact, an independent SQLite backup target, an
operator identity, and an explicit confirmation token.  Neither mode performs
provider requests, attachment downloads, file convergence, or promotion.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
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
from research.announcement_assets.schema import OWNED_TABLES
from research.announcement_assets.storage import probe_mount_identity
from scripts.dev_validation.drill_announcement_asset_shadow_adoption import (
    _candidate_stats,
    _table_count,
)
from scripts.dev_validation.inventory_announcement_asset_capacity import (
    SCHEMA_VERSION as CAPACITY_SCHEMA_VERSION,
    _is_beneath,
    _read_legacy_manifest_rows,
    _require_complete_manifest_input,
    _validate_new_output_path,
    _write_new_json,
)
from utils.config_manager import config_manager

SCHEMA_VERSION = "annual_report_asset_production_shadow.v1"
CONFIRMATION_TOKEN = "WRITE_PRODUCTION_SHADOW"


def _read_mapping(path: Path, name: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name}_unreadable") from exc
    if not isinstance(payload, Mapping):
        raise TypeError(f"{name}_not_mapping")
    return payload


def _require_shadow_safe_config(config: AnnouncementAssetConfig) -> None:
    if config.enabled or config.scheduled_enabled or not config.dry_run:
        raise RuntimeError("production_shadow_requires_disabled_dry_run_module")
    enabled_jobs = (
        config.jobs.latest_backfill_enabled,
        config.jobs.daily_enabled,
        config.jobs.backup_enabled,
        config.jobs.integrity_enabled,
    )
    if any(enabled_jobs):
        raise RuntimeError("production_shadow_requires_all_asset_jobs_disabled")


def _require_production_catalog(path: Path, *, project_root: Path) -> Path:
    expected = (project_root / "data/research.db").resolve(strict=False)
    resolved = path.resolve(strict=False)
    if resolved != expected:
        raise ValueError("production shadow target must be the exact data/research.db")
    if path.is_symlink() or not path.is_file():
        raise ValueError("production shadow target must be an existing regular file")
    if path.stat().st_nlink != 1:
        raise ValueError("production shadow target must not be hard-linked")
    return resolved


def _catalog_identity(path: Path) -> dict[str, int | str]:
    state = path.stat()
    return {
        "path": str(path.resolve(strict=True)),
        "device": int(state.st_dev),
        "inode": int(state.st_ino),
        "size_bytes": int(state.st_size),
        "mtime_ns": int(state.st_mtime_ns),
    }


def _identity_matches(path: Path, expected: Mapping[str, Any]) -> bool:
    actual = _catalog_identity(path)
    return all(actual.get(key) == expected.get(key) for key in actual)


def _sqlite_backup(source: Path, destination: Path) -> None:
    if destination.exists():
        raise FileExistsError(destination)
    if not destination.parent.is_dir():
        raise FileNotFoundError(destination.parent)
    with sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True) as src:
        with sqlite3.connect(destination) as dst:
            src.backup(dst)
            result = dst.execute("PRAGMA quick_check").fetchone()
            if result is None or result[0] != "ok":
                raise RuntimeError("sqlite_backup_quick_check_failed")


def _schema_fingerprint(path: Path, *, exclude_owned: bool) -> str:
    import hashlib

    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            """SELECT type, name, tbl_name, coalesce(sql, '')
               FROM sqlite_master
               WHERE name NOT LIKE 'sqlite_%'
               ORDER BY type, name"""
        ).fetchall()
    if exclude_owned:
        rows = [row for row in rows if str(row[2]) not in OWNED_TABLES]
    body = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _production_projection(repository: AnnouncementAssetRepository) -> list[tuple[Any, ...]]:
    if not repository.schema_initialized():
        return []
    output: list[tuple[Any, ...]] = []
    offset = 0
    while True:
        page = repository.list_effective_reports(limit=1000, offset=offset)
        if not page:
            break
        output.extend(
            (
                item.instrument_id,
                item.fiscal_year,
                item.asset_id,
                item.content_hash,
                item.decision_state.value,
                item.availability.value,
                item.visibility_state,
            )
            for item in page
        )
        offset += len(page)
    return sorted(output)


def _inventory_scopes(inventory: Any) -> set[tuple[str, int]]:
    return {
        (str(item.instrument_id), int(item.fiscal_year))
        for item in inventory.items
        if item.status in {"adoptable", "duplicate", "superseded"}
        and item.instrument_id
        and item.fiscal_year is not None
    }


def _load_fresh_inventory(
    *,
    config: AnnouncementAssetConfig,
    financials_db: Path,
    project_root: Path,
    manifest_rows: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[Any, Mapping[str, Any]]:
    if manifest_rows is None:
        rows, details = _read_legacy_manifest_rows(
            financials_db=financials_db,
            project_root=project_root,
            config=config,
        )
        _require_complete_manifest_input(rows, details)
    else:
        rows = [dict(item) for item in manifest_rows]
        details = {
            "status": "complete",
            "database": str(financials_db),
            "rows_loaded": len(rows),
            "rows_outside_registered_roots": 0,
            "schema_versions": sorted(
                {str(item.get("schema_version") or "") for item in rows}
            ),
        }
        _require_complete_manifest_input(rows, details)
    inventory = AnnouncementArchiveInventory().inventory_registered(
        config=config,
        manifest_rows=rows,
    )
    return inventory, details


def _validate_inventory_artifact(
    artifact: Mapping[str, Any], *, config: AnnouncementAssetConfig, inventory: Any
) -> None:
    if artifact.get("schema_version") != CAPACITY_SCHEMA_VERSION:
        raise ValueError("inventory_artifact_schema_mismatch")
    if artifact.get("configuration_fingerprint") != config.config_fingerprint:
        raise ValueError("inventory_artifact_config_mismatch")
    details = artifact.get("inventory")
    if not isinstance(details, Mapping):
        raise ValueError("inventory_artifact_details_missing")
    if details.get("inventory_fingerprint") != inventory.inventory_fingerprint:
        raise ValueError("inventory_artifact_fingerprint_mismatch")
    if details.get("counts") != dict(inventory.counts):
        raise ValueError("inventory_artifact_counts_mismatch")
    if artifact.get("read_only") is not True or any(
        int(artifact.get(field) or 0) != 0
        for field in ("network_requests", "catalog_writes", "adoption_writes", "archive_mutations")
    ):
        raise ValueError("inventory_artifact_is_not_read_only")


def _adoption_projection(catalog_db: Path, adoption: Any) -> dict[str, Any]:
    repository = AnnouncementAssetRepository(catalog_db)
    reports = repository.list_effective_reports(include_shadow=True, limit=100000)
    scope_counts = Counter((item.instrument_id, item.fiscal_year) for item in reports)
    review = [
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
        "files_adopted": adoption.files_adopted,
        "legal_attachments_registered": adoption.legal_attachments_registered,
        "blobs_registered": adoption.blobs_registered,
        "period_count": len(adoption.periods),
        "period_statuses": dict(sorted(Counter(item.status for item in adoption.periods).items())),
        "review_required_periods": review,
        "skipped_counts": dict(adoption.skipped_counts),
        "effective_scope_count": len(scope_counts),
        "effective_scope_unique": all(count == 1 for count in scope_counts.values()),
    }


def _catalog_counts(path: Path) -> dict[str, int]:
    return {
        "announcements": _table_count(path, "official_announcements"),
        "attachments": _table_count(path, "official_announcement_attachments"),
        "blobs": _table_count(path, "official_document_blobs"),
        "attachment_versions": _table_count(path, "official_attachment_versions"),
        "effective_reports": _table_count(path, "effective_annual_reports"),
        "effective_decisions": _table_count(path, "official_annual_report_decisions"),
    }


def simulate_production_shadow(
    *,
    production_db: Path,
    simulation_db: Path,
    financials_db: Path,
    inventory_artifact_path: Path,
    config: AnnouncementAssetConfig,
    project_root: Path = PROJECT_ROOT,
    manifest_rows: Iterable[Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    """Run the exact schema/adoption path on a new SQLite copy only."""

    _require_shadow_safe_config(config)
    production = _require_production_catalog(production_db, project_root=project_root)
    simulation = simulation_db.resolve(strict=False)
    allowed = (Path("/tmp").resolve(), Path("/dev/shm").resolve())
    if not any(root == simulation or root in simulation.parents for root in allowed):
        raise ValueError("production shadow simulation must be under /tmp or /dev/shm")
    inventory, manifest_input = _load_fresh_inventory(
        config=config,
        financials_db=financials_db,
        project_root=project_root,
        manifest_rows=manifest_rows,
    )
    artifact = _read_mapping(inventory_artifact_path, "inventory_artifact")
    _validate_inventory_artifact(artifact, config=config, inventory=inventory)
    legacy_before = _candidate_stats(inventory)
    source_identity_before = _catalog_identity(production)
    source_schema = _schema_fingerprint(production, exclude_owned=True)
    source_repository = AnnouncementAssetRepository(production)
    production_before = _production_projection(source_repository)
    conflicts = sorted(set((item[0], item[1]) for item in production_before) & _inventory_scopes(inventory))
    if conflicts:
        raise RuntimeError(f"production_effective_scope_conflict:{conflicts[:3]}")

    _sqlite_backup(production, simulation)
    repository = AnnouncementAssetRepository(simulation)
    repository.initialize_schema()
    adoption = AnnouncementArchiveInventory().shadow_adopt(
        inventory,
        repository=repository,
        observed_at=datetime.now(timezone.utc).isoformat(),
    )
    legacy_after = _candidate_stats(inventory)
    if legacy_before != legacy_after:
        raise RuntimeError("production_shadow_simulation_changed_legacy_files")
    if not _identity_matches(production, source_identity_before):
        raise RuntimeError("production_catalog_changed_during_simulation")
    if _schema_fingerprint(simulation, exclude_owned=True) != source_schema:
        raise RuntimeError("production_shadow_changed_non_owned_schema")
    production_after = _production_projection(repository)
    if production_after != production_before:
        raise RuntimeError("production_shadow_simulation_changed_production_projection")
    adoption_result = _adoption_projection(simulation, adoption)
    apply_eligible = bool(
        adoption_result["effective_scope_unique"]
        and not adoption_result["review_required_periods"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "simulation",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "operator": None,
        "configuration_fingerprint": config.config_fingerprint,
        "inventory_artifact_path": str(inventory_artifact_path.resolve(strict=True)),
        "inventory": {
            "fingerprint": inventory.inventory_fingerprint,
            "counts": dict(inventory.counts),
            "files_seen": inventory.files_seen,
            "manifest_rows_seen": inventory.manifest_rows_seen,
        },
        "manifest_input": dict(manifest_input),
        "production_catalog": {
            "path": str(production),
            "identity": source_identity_before,
            "writes": 0,
            "production_projection_count": len(production_before),
            "non_owned_schema_fingerprint": source_schema,
        },
        "simulation_catalog": {
            "path": str(simulation),
            "writes": True,
            "catalog_counts": _catalog_counts(simulation),
        },
        "adoption": adoption_result,
        "network_requests": 0,
        "attachment_downloads": 0,
        "archive_mutations": {"copied": 0, "moved": 0, "linked": 0, "quarantined": 0, "deleted": 0},
        "promotion": {"run": False, "production_visible_rows_added": 0},
        "apply_eligible": apply_eligible,
        "production_readiness": {
            "ready": False,
            "reason": "simulation_only; explicit operator-authorized production shadow apply is required",
        },
    }


def apply_production_shadow(
    *,
    production_db: Path,
    financials_db: Path,
    inventory_artifact_path: Path,
    preflight_artifact_path: Path,
    backup_path: Path,
    operator: str,
    confirmation: str,
    config: AnnouncementAssetConfig,
    project_root: Path = PROJECT_ROOT,
    manifest_rows: Iterable[Mapping[str, Any]] | None = None,
) -> Mapping[str, Any]:
    """Register shadow rows in production after exact simulation and backup."""

    _require_shadow_safe_config(config)
    production = _require_production_catalog(production_db, project_root=project_root)
    if confirmation != CONFIRMATION_TOKEN:
        raise PermissionError("production_shadow_confirmation_missing")
    operator_id = str(operator or "").strip()
    if not operator_id:
        raise PermissionError("production_shadow_operator_missing")
    preflight = _read_mapping(preflight_artifact_path, "preflight_artifact")
    if (
        preflight.get("schema_version") != SCHEMA_VERSION
        or preflight.get("mode") != "simulation"
        or preflight.get("apply_eligible") is not True
        or preflight.get("configuration_fingerprint") != config.config_fingerprint
    ):
        raise ValueError("production_shadow_preflight_not_eligible")
    production_evidence = preflight.get("production_catalog")
    if not isinstance(production_evidence, Mapping) or not isinstance(
        production_evidence.get("identity"), Mapping
    ):
        raise ValueError("production_shadow_preflight_identity_missing")
    if not _identity_matches(production, production_evidence["identity"]):
        raise RuntimeError("production_catalog_changed_since_preflight")

    inventory, manifest_input = _load_fresh_inventory(
        config=config,
        financials_db=financials_db,
        project_root=project_root,
        manifest_rows=manifest_rows,
    )
    artifact = _read_mapping(inventory_artifact_path, "inventory_artifact")
    _validate_inventory_artifact(artifact, config=config, inventory=inventory)
    preflight_inventory = preflight.get("inventory")
    if not isinstance(preflight_inventory, Mapping) or preflight_inventory.get(
        "fingerprint"
    ) != inventory.inventory_fingerprint:
        raise ValueError("production_shadow_inventory_changed_since_preflight")
    repository = AnnouncementAssetRepository(production)
    production_before = _production_projection(repository)
    conflicts = sorted(set((item[0], item[1]) for item in production_before) & _inventory_scopes(inventory))
    if conflicts:
        raise RuntimeError(f"production_effective_scope_conflict:{conflicts[:3]}")
    if not config.backup.mount_root:
        raise RuntimeError("production_shadow_backup_mount_unconfigured")
    backup = backup_path.resolve(strict=False)
    if not _is_beneath(backup, config.backup.mount_root.resolve(strict=True)):
        raise ValueError("production shadow backup must use configured backup mount")
    backup_identity = probe_mount_identity(config.backup.mount_root)
    if backup_identity.mount_point == Path("/") or not backup_identity.read_write:
        raise RuntimeError("production_shadow_backup_mount_unavailable")
    if config.backup.expected_mount_source and backup_identity.source != config.backup.expected_mount_source:
        raise RuntimeError("production_shadow_backup_mount_source_mismatch")

    legacy_before = _candidate_stats(inventory)
    source_identity = _catalog_identity(production)
    non_owned_schema = _schema_fingerprint(production, exclude_owned=True)
    _sqlite_backup(production, backup)
    if _schema_fingerprint(backup, exclude_owned=True) != non_owned_schema:
        raise RuntimeError("production_shadow_backup_schema_mismatch")

    repository.initialize_schema()
    adoption = AnnouncementArchiveInventory().shadow_adopt(
        inventory,
        repository=repository,
        observed_at=datetime.now(timezone.utc).isoformat(),
    )
    if legacy_before != _candidate_stats(inventory):
        raise RuntimeError("production_shadow_apply_changed_legacy_files")
    if _schema_fingerprint(production, exclude_owned=True) != non_owned_schema:
        raise RuntimeError("production_shadow_apply_changed_non_owned_schema")
    production_after = _production_projection(repository)
    if production_after != production_before:
        raise RuntimeError("production_shadow_apply_changed_production_projection")
    adoption_result = _adoption_projection(production, adoption)
    ready = bool(
        adoption_result["effective_scope_unique"]
        and not adoption_result["review_required_periods"]
    )
    if not ready:
        raise RuntimeError("production_shadow_apply_requires_operator_review")
    result = {
        "schema_version": SCHEMA_VERSION,
        "mode": "production_shadow",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "operator": operator_id,
        "configuration_fingerprint": config.config_fingerprint,
        "inventory_artifact_path": str(inventory_artifact_path.resolve(strict=True)),
        "preflight_artifact_path": str(preflight_artifact_path.resolve(strict=True)),
        "inventory": {
            "fingerprint": inventory.inventory_fingerprint,
            "counts": dict(inventory.counts),
            "files_seen": inventory.files_seen,
            "manifest_rows_seen": inventory.manifest_rows_seen,
        },
        "manifest_input": dict(manifest_input),
        "production_catalog": {
            "path": str(production),
            "identity_before": source_identity,
            "identity_after": _catalog_identity(production),
            "writes": True,
            "catalog_counts": _catalog_counts(production),
            "production_projection_count_before": len(production_before),
            "production_projection_count_after": len(production_after),
            "non_owned_schema_fingerprint": non_owned_schema,
        },
        "database_backup": {
            "path": str(backup),
            "identity": _catalog_identity(backup),
            "mount_filesystem_key": backup_identity.filesystem_key,
            "quick_check": "ok",
        },
        "adoption": adoption_result,
        "required_set_evidence": {
            "status": "not_measured",
            "reason": (
                "required-set evidence is valid only after reconciliation, "
                "production visibility promotion, and a verified backup run"
            ),
        },
        "network_requests": 0,
        "attachment_downloads": 0,
        "archive_mutations": {"copied": 0, "moved": 0, "linked": 0, "quarantined": 0, "deleted": 0},
        "promotion": {"run": False, "production_visible_rows_added": 0},
        "production_readiness": {
            "ready": False,
            "reason": (
                "shadow_registration_only; reconciliation, promotion, verified "
                "backup, required-set measurement, and capacity approval remain"
            ),
            "shadow_registration_ready": True,
            "promotion_ready": False,
            "consumer_cutover_ready": False,
        },
    }
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--simulate", action="store_true")
    mode.add_argument("--apply-shadow", action="store_true")
    parser.add_argument("--production-db", type=Path, default=PROJECT_ROOT / "data/research.db")
    parser.add_argument("--financials-db", type=Path, default=PROJECT_ROOT / "data/financials.db")
    parser.add_argument("--inventory-artifact", type=Path, required=True)
    parser.add_argument("--preflight-artifact", type=Path)
    parser.add_argument("--simulation-db", type=Path)
    parser.add_argument("--backup-path", type=Path)
    parser.add_argument("--operator")
    parser.add_argument("--confirm")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(), project_root=PROJECT_ROOT
    )
    if args.simulate:
        if args.simulation_db is None:
            parser.error("--simulation-db is required with --simulate")
        result = simulate_production_shadow(
            production_db=args.production_db,
            simulation_db=args.simulation_db,
            financials_db=args.financials_db,
            inventory_artifact_path=args.inventory_artifact,
            config=config,
        )
    else:
        if args.preflight_artifact is None or args.backup_path is None:
            parser.error("--preflight-artifact and --backup-path are required with --apply-shadow")
        result = apply_production_shadow(
            production_db=args.production_db,
            financials_db=args.financials_db,
            inventory_artifact_path=args.inventory_artifact,
            preflight_artifact_path=args.preflight_artifact,
            backup_path=args.backup_path,
            operator=args.operator or "",
            confirmation=args.confirm or "",
            config=config,
        )
    _write_new_json(output, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
