"""Create a bounded, read-only annual-report archive inventory and capacity artifact.

This is an operator evidence tool, not a migration command.  It never opens the
announcement catalog for writing, downloads attachments, adopts legacy files, or
changes the archive.  The output is deliberately a timestamped JSON artifact so
rollout gates can reject stale or configuration-mismatched measurements.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import (
    AnnouncementArchiveInventory,
    AnnouncementAssetConfig,
)
from utils.config_manager import config_manager

SCHEMA_VERSION = "official_announcement_asset_capacity_artifact.v4"
PDF_SUFFIX = ".pdf"
_LEGACY_MANIFEST_SCHEMA_VERSIONS = frozenset(
    {
        "business_profile_source_file_manifest.v1",
        "financial_source_file_manifest.v1",
    }
)


def _is_beneath(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def _validate_new_output_path(path: Path, *, project_root: Path) -> Path:
    """Allow a new artifact only outside protected production data roots."""

    resolved = path.resolve(strict=False)
    if _is_beneath(resolved, project_root / "data"):
        raise ValueError("evidence output must not be created under project data")
    if resolved.exists():
        raise FileExistsError("evidence output already exists")
    if not resolved.parent.is_dir():
        raise FileNotFoundError("evidence output parent must already exist")
    return resolved


def _write_new_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Publish one evidence file without truncating an existing path."""

    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _require_complete_manifest_input(
    rows: list[dict[str, Any]], details: Mapping[str, Any]
) -> None:
    if details.get("status") != "complete":
        raise RuntimeError(
            f"legacy_manifest_input_unavailable:{details.get('reason') or 'unknown'}"
        )
    if not rows or int(details.get("rows_loaded") or 0) <= 0:
        raise RuntimeError("legacy_manifest_input_empty")


def _candidate_sizes(inventory: Any) -> list[int]:
    return [
        int(item.content_length)
        for item in inventory.items
        if item.status in {"adoptable", "duplicate"}
        and item.content_length is not None
        and int(item.content_length) > 0
    ]


def _quantile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile + 0.999999) - 1))
    return ordered[index]


def _mount_identity(path: Path) -> Mapping[str, Any]:
    """Return read-only filesystem identity without probing or creating paths."""

    resolved = path.resolve(strict=True)
    details: dict[str, Any] = {
        "path": str(resolved),
        "device": None,
        "filesystem_id": None,
        "mount_source": None,
        "mount_target": None,
        "filesystem_type": None,
    }
    state = resolved.stat()
    details["device"] = int(state.st_dev)
    details["filesystem_id"] = f"{state.st_dev}:{state.st_ino}"
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.exists():
        return details
    best: tuple[int, list[str], str] | None = None
    for raw in mountinfo.read_text(encoding="utf-8", errors="replace").splitlines():
        fields = raw.split()
        if "-" not in fields or len(fields) < 10:
            continue
        separator = fields.index("-")
        mount_target = fields[4].replace("\\040", " ")
        try:
            resolved.relative_to(Path(mount_target))
        except ValueError:
            continue
        candidate = (len(mount_target), fields, mount_target)
        if best is None or candidate[0] > best[0]:
            best = candidate
    if best is not None:
        _, fields, mount_target = best
        separator = fields.index("-")
        details.update(
            {
                "mount_target": mount_target,
                "filesystem_type": fields[separator + 1],
                "mount_source": fields[separator + 2],
                "mount_options": fields[separator + 3] if len(fields) > separator + 3 else None,
            }
        )
    resolved_mount = _findmnt_backing_mount(resolved)
    if resolved_mount is not None:
        details["backing_mount"] = resolved_mount
    return details


def _findmnt_backing_mount(path: Path) -> Mapping[str, Any] | None:
    """Resolve an automounter wrapper to the concrete backing mount if present."""

    try:
        result = subprocess.run(
            [
                "findmnt",
                "-T",
                str(path),
                "-J",
                "-o",
                "TARGET,SOURCE,FSTYPE,OPTIONS,FSROOT",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        payload = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return None
    filesystems = payload.get("filesystems") if isinstance(payload, Mapping) else None
    if not isinstance(filesystems, list):
        return None
    candidates = [
        item
        for item in filesystems
        if isinstance(item, Mapping)
        and str(item.get("fstype") or "").lower() not in {"autofs", ""}
    ]
    if not candidates:
        return None
    candidate = candidates[-1]
    return {
        "mount_target": candidate.get("target"),
        "mount_source": candidate.get("source"),
        "filesystem_type": candidate.get("fstype"),
        "mount_options": candidate.get("options"),
        "read_write": "rw" in str(candidate.get("options") or "").split(","),
    }


def _disk_usage(path: Path) -> Mapping[str, int]:
    usage = os.statvfs(path)
    block = int(usage.f_frsize or usage.f_bsize)
    total = int(usage.f_blocks) * block
    free = int(usage.f_bavail) * block
    available = int(usage.f_bfree) * block
    return {"total_bytes": total, "used_bytes": total - available, "free_bytes": free}


def _failure_domain(identity: Mapping[str, Any]) -> str:
    backing = identity.get("backing_mount")
    source = (
        backing.get("mount_source")
        if isinstance(backing, Mapping)
        else identity.get("mount_source")
    )
    host = str(source or "").partition(":")[0].strip()
    return f"mount_host:{host}" if host else "unresolved"


def _read_active_universe(quotes_db: Path) -> Mapping[str, Any]:
    if not quotes_db.is_file():
        return {"status": "unavailable", "reason": "quotes_db_missing", "counts": {}}
    uri = f"file:{quotes_db.resolve()}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """SELECT exchange, COUNT(*) AS count
                   FROM instruments
                   WHERE is_active = 1
                     AND exchange IN ('SSE', 'SZSE', 'BSE')
                     AND lower(coalesce(type, '')) IN ('stock', 'a_stock', 'equity')
                   GROUP BY exchange
                   ORDER BY exchange"""
            ).fetchall()
            instrument_rows = connection.execute(
                """SELECT instrument_id
                   FROM instruments
                   WHERE is_active = 1
                     AND exchange IN ('SSE', 'SZSE', 'BSE')
                     AND lower(coalesce(type, '')) IN ('stock', 'a_stock', 'equity')
                """
            ).fetchall()
    except sqlite3.Error as exc:
        return {"status": "unavailable", "reason": f"quotes_query_failed:{type(exc).__name__}", "counts": {}}
    counts = {str(row["exchange"]): int(row["count"]) for row in rows}
    return {
        "status": "complete" if all(counts.get(exchange, 0) > 0 for exchange in ("SSE", "SZSE", "BSE")) else "partial",
        "counts": counts,
        "total": sum(counts.values()),
        "instrument_ids": tuple(str(row[0]) for row in instrument_rows),
        "database": str(quotes_db),
    }


def _iter_pdf_sizes(root: Path) -> Iterable[int]:
    for path in root.rglob(f"*{PDF_SUFFIX}"):
        try:
            if path.is_file():
                yield path.stat().st_size
        except OSError:
            continue


def _read_legacy_manifest_rows(
    *,
    financials_db: Path,
    project_root: Path,
    config: AnnouncementAssetConfig,
) -> tuple[list[dict[str, Any]], Mapping[str, Any]]:
    """Load only registered legacy manifests through a read-only SQLite URI.

    The legacy stores predate the shared catalog.  Their manifests are the only
    authoritative local identity evidence available before shadow adoption, so
    capacity inventory must not classify every unbound PDF as an orphan.
    """

    if not financials_db.is_file():
        return [], {"status": "unavailable", "reason": "financials_db_missing"}
    registered_roots = tuple(
        Path(root).resolve(strict=False) for _, root in config.legacy_inventory.roots
    )
    uri = f"file:{financials_db.resolve()}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT source_file_id, instrument_id, symbol, exchange, report_period,
                       report_type, filing_id, source_url, archive_path, content_hash,
                       content_length, published_at, downloaded_at, parser_version,
                       parser_diagnostics_json, schema_version, source, source_mode,
                       source_tier, status, supersedes_source_file_id, metadata_json,
                       created_at, updated_at
                FROM financial_source_files
                WHERE schema_version IN (?, ?)
                  AND archive_path IS NOT NULL
                  AND trim(archive_path) <> ''
                ORDER BY archive_path, source_file_id
                """,
                tuple(sorted(_LEGACY_MANIFEST_SCHEMA_VERSIONS)),
            ).fetchall()
    except sqlite3.Error as exc:
        return [], {
            "status": "unavailable",
            "reason": f"financials_manifest_query_failed:{type(exc).__name__}",
        }

    manifests: list[dict[str, Any]] = []
    outside_registered_roots = 0
    for row in rows:
        item = dict(row)
        archive_path = Path(str(item["archive_path"] or ""))
        resolved = (
            archive_path if archive_path.is_absolute() else project_root / archive_path
        ).resolve(strict=False)
        if not any(
            resolved == root or root in resolved.parents for root in registered_roots
        ):
            outside_registered_roots += 1
            continue
        for key in ("parser_diagnostics_json", "metadata_json"):
            raw = item.pop(key, None)
            try:
                item["parser_diagnostics" if key.startswith("parser") else "metadata"] = (
                    json.loads(raw) if raw else {}
                )
            except (TypeError, json.JSONDecodeError):
                item["parser_diagnostics" if key.startswith("parser") else "metadata"] = {}
        item["archive_path"] = str(resolved)
        manifests.append(item)
    return manifests, {
        "status": "complete",
        "database": str(financials_db),
        "rows_loaded": len(manifests),
        "rows_outside_registered_roots": outside_registered_roots,
        "schema_versions": sorted(_LEGACY_MANIFEST_SCHEMA_VERSIONS),
    }


def _inventory_review_set(inventory: Any) -> Mapping[str, Any]:
    """Return a deterministic operator review set without exposing file bytes."""

    reviewable = {"adoptable", "duplicate", "orphan", "conflicting", "missing", "superseded"}
    items = []
    for item in sorted(inventory.items, key=lambda value: value.path):
        if item.status not in reviewable:
            continue
        items.append(
            {
                "path": item.path,
                "consumer": item.consumer,
                "status": item.status,
                "reason": item.reason,
                "instrument_id": item.instrument_id,
                "exchange": item.exchange,
                "report_period": item.report_period,
                "fiscal_year": item.fiscal_year,
                "report_type": item.report_type,
                "source": item.source,
                "filing_id": item.filing_id,
                "source_file_id": item.source_file_id,
                "content_hash": item.content_hash,
                "expected_hash": item.expected_hash,
                "content_length": item.content_length,
            }
        )
    duplicate_items = [item for item in inventory.items if item.status == "duplicate"]
    return {
        "candidate_statuses": ["adoptable", "duplicate"],
        "candidate_count": sum(
            1 for item in inventory.items if item.status in {"adoptable", "duplicate"}
        ),
        "candidate_bytes": sum(
            int(item.content_length or 0)
            for item in inventory.items
            if item.status in {"adoptable", "duplicate"}
        ),
        "duplicate_path_count": len(duplicate_items),
        "duplicate_path_bytes": sum(
            int(item.content_length or 0) for item in duplicate_items
        ),
        "duplicate_bytes_basis": (
            "sum of verified file lengths for inventory paths classified duplicate; "
            "not an approved deletion estimate"
        ),
        "items": items,
    }


def build_artifact(
    *,
    project_root: Path,
    quotes_db: Path,
    financials_db: Path | None = None,
) -> Mapping[str, Any]:
    """Measure the existing archive and return a serializable evidence artifact."""

    research_config = config_manager.get_research_config()
    config = AnnouncementAssetConfig.from_research_config(
        research_config, project_root=project_root
    )
    filings_root = config.filings_root.resolve(strict=True)
    backup_root = (
        config.backup.mount_root.resolve(strict=False)
        if config.backup.mount_root
        else None
    )
    manifest_rows, manifest_input = _read_legacy_manifest_rows(
        financials_db=financials_db or project_root / "data" / "financials.db",
        project_root=project_root,
        config=config,
    )
    _require_complete_manifest_input(manifest_rows, manifest_input)
    inventory = AnnouncementArchiveInventory().inventory_registered(
        config=config,
        manifest_rows=manifest_rows,
    )
    review_set = _inventory_review_set(inventory)
    all_pdf_sizes = list(_iter_pdf_sizes(filings_root))
    annual_candidate_sizes = _candidate_sizes(inventory)
    inventory_counts = Counter(item.status for item in inventory.items)
    inventory_bytes = sum(
        int(item.content_length or 0)
        for item in inventory.items
        if item.status == "adoptable"
    )
    now = datetime.now(timezone.utc).isoformat()
    primary_identity = _mount_identity(filings_root)
    primary = {
        "identity": primary_identity,
        "failure_domain_identity": _failure_domain(primary_identity),
        "usage": _disk_usage(filings_root),
        "pdf_distribution": {
            "scope": "manifest_verified_annual_report_candidates",
            "file_count": len(annual_candidate_sizes),
            "total_bytes": sum(annual_candidate_sizes),
            "p95_bytes": _quantile(annual_candidate_sizes, 0.95),
            "p99_bytes": _quantile(annual_candidate_sizes, 0.99),
            "max_bytes": max(annual_candidate_sizes) if annual_candidate_sizes else None,
        },
        "all_filings_pdf_distribution": {
            "scope": "all_pdf_files_beneath_filings_root",
            "file_count": len(all_pdf_sizes),
            "total_bytes": sum(all_pdf_sizes),
            "p95_bytes": _quantile(all_pdf_sizes, 0.95),
            "p99_bytes": _quantile(all_pdf_sizes, 0.99),
            "max_bytes": max(all_pdf_sizes) if all_pdf_sizes else None,
        },
    }
    backup: Mapping[str, Any]
    if backup_root is None or not backup_root.exists():
        backup = {"status": "unavailable", "reason": "backup_mount_root_missing"}
    else:
        backup_identity = _mount_identity(backup_root)
        backup = {
            "status": "available",
            "identity": backup_identity,
            "failure_domain_identity": _failure_domain(
                backup_identity,
            ),
            "configured_failure_domain_label": config.backup.expected_failure_domain,
            "usage": _disk_usage(backup_root),
        }
    active_universe = dict(_read_active_universe(quotes_db))
    active_ids = set(active_universe.pop("instrument_ids", ()))
    candidate_ids = {
        str(item.instrument_id)
        for item in inventory.items
        if item.status in {"adoptable", "duplicate"} and item.instrument_id
    }
    latest_fiscal_year = max(
        (
            int(item.fiscal_year)
            for item in inventory.items
            if item.status in {"adoptable", "duplicate"}
            and item.instrument_id
            and item.fiscal_year is not None
        ),
        default=None,
    )
    latest_candidate_ids = {
        str(item.instrument_id)
        for item in inventory.items
        if item.status in {"adoptable", "duplicate"}
        and item.instrument_id
        and item.fiscal_year == latest_fiscal_year
    }
    candidate_active_ids = candidate_ids & active_ids
    candidate_missing_ids = candidate_ids - active_ids
    latest_candidate_active_ids = latest_candidate_ids & active_ids
    latest_candidate_missing_ids = latest_candidate_ids - active_ids
    distribution = primary["pdf_distribution"]
    active_total = int(active_universe.get("total") or 0)
    p99_bytes = int(distribution.get("p99_bytes") or 0)
    max_attachment_bytes = int(config.storage.max_attachment_bytes)
    download_concurrency = max(1, int(config.acquisition.download_concurrency))
    attachment_limit_ok = (
        primary["pdf_distribution"]["max_bytes"] is None
        or int(primary["pdf_distribution"]["max_bytes"]) <= max_attachment_bytes
    )
    estimated_full_market_bytes = (
        active_total * p99_bytes if active_total and p99_bytes else None
    )
    estimated_replacement_peak_bytes = (
        estimated_full_market_bytes * 2
        if estimated_full_market_bytes is not None
        else None
    )
    estimated_temporary_peak_bytes = max_attachment_bytes * download_concurrency
    backup_usage = backup.get("usage") if isinstance(backup, Mapping) else None
    backup_free_bytes = (
        int(backup_usage.get("free_bytes") or 0)
        if isinstance(backup_usage, Mapping)
        else None
    )
    estimated_headroom_bytes = (
        backup_free_bytes - estimated_full_market_bytes
        if backup_free_bytes is not None and estimated_full_market_bytes is not None
        else None
    )
    active_universe["candidate_coverage"] = {
        "latest_fiscal_year": latest_fiscal_year,
        "candidate_instrument_count": len(candidate_ids),
        "candidate_active_instrument_count": len(candidate_active_ids),
        "candidate_missing_or_inactive_count": len(candidate_missing_ids),
        "any_history_active_ratio": (
            len(candidate_active_ids) / len(candidate_ids) if candidate_ids else None
        ),
        "latest_candidate_instrument_count": len(latest_candidate_ids),
        "latest_candidate_active_instrument_count": len(latest_candidate_active_ids),
        "latest_candidate_missing_or_inactive_count": len(latest_candidate_missing_ids),
        "full_market_coverage_ratio": (
            len(latest_candidate_active_ids) / active_total if active_total else None
        ),
        "basis": "manifest_verified_latest_fiscal_year_adoptable_or_duplicate_candidates",
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now,
        "read_only": True,
        "network_requests": 0,
        "catalog_writes": 0,
        "adoption_writes": 0,
        "archive_mutations": 0,
        "configuration_fingerprint": config.config_fingerprint,
        "inventory": {
            "files_seen": inventory.files_seen,
            "manifest_rows_seen": inventory.manifest_rows_seen,
            "manifest_input": manifest_input,
            "counts": dict(sorted(inventory_counts.items())),
            "inventory_fingerprint": inventory.inventory_fingerprint,
            "registered_root_registry_version": inventory.root_registry_version,
            "out_of_scope_directories": list(inventory.out_of_scope_directories),
            "adoptable_bytes_from_manifests": inventory_bytes,
            "review_set": review_set,
            "shadow_adoption": {"status": "not_run", "reason": "read_only_inventory_only"},
        },
        "active_universe": active_universe,
        "primary_archive": primary,
        "backup_target": backup,
        "planning": {
            "attachment_limit_bytes": config.storage.max_attachment_bytes,
            "unknown_length_reservation_bytes": config.storage.unknown_length_reservation_bytes,
            "max_task_download_bytes": config.acquisition.max_task_download_bytes,
            "old_plus_new_replacement_peak_basis": "two_distinct_attachment_versions",
            "legacy_manifest_candidate_bytes": review_set["candidate_bytes"],
            "estimated_full_market_required_bytes": estimated_full_market_bytes,
            "estimated_full_market_required_bytes_basis": (
                "active_universe_count_times_observed_p99_bytes;"
                " conservative upper planning estimate, not catalog required-set truth"
            ),
            "expected_annual_growth_bytes": (
                None
                if not annual_candidate_sizes or not active_total
                else int(
                    sum(annual_candidate_sizes)
                    / len(annual_candidate_sizes)
                    * active_total
                )
            ),
            "expected_annual_growth_basis": (
                "active universe times observed manifest-verified candidate mean"
            ),
            "stress_annual_growth_bytes": estimated_full_market_bytes,
            "stress_annual_growth_basis": "active universe times observed P99",
            "estimated_old_plus_new_replacement_peak_bytes": estimated_replacement_peak_bytes,
            "estimated_temporary_peak_bytes": estimated_temporary_peak_bytes,
            "temporary_peak_basis": (
                "configured max attachment bytes times download concurrency;"
                " excludes unknown provider overhead"
            ),
            "planning_horizon_years": None,
            "planning_horizon_status": "operator_required",
            "approved_budget_basis": None,
            "estimated_primary_headroom_bytes": None,
            "estimated_backup_headroom_bytes": estimated_headroom_bytes,
            "backup_headroom_basis": (
                "backup free bytes minus conservative full-market estimate"
            ),
            "primary_required_set_actual_bytes": None,
            "backup_required_set_actual_bytes": None,
            "permanently_retained_recovery_manifest_bytes": None,
            "explicit_approver": None,
            "attachment_limit_within_observed_max": attachment_limit_ok,
            "status": "incomplete_pending_operator_estimates_and_approval",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quotes-db", type=Path, default=PROJECT_ROOT / "data/quotes.db")
    parser.add_argument(
        "--financials-db",
        type=Path,
        default=PROJECT_ROOT / "data/financials.db",
        help="legacy manifest database opened in SQLite read-only mode",
    )
    parser.add_argument("--output-path", type=Path, required=True)
    args = parser.parse_args()
    output_path = _validate_new_output_path(
        args.output_path,
        project_root=PROJECT_ROOT,
    )
    artifact = build_artifact(
        project_root=PROJECT_ROOT,
        quotes_db=args.quotes_db,
        financials_db=args.financials_db,
    )
    _write_new_json(output_path, artifact)
    print(json.dumps(artifact, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
