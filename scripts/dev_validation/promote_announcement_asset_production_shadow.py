"""Promote an explicitly reconciled production shadow adoption.

This stage changes only catalog visibility. It revalidates the current legacy
inventory and persisted promotion gates, and it neither mutates archive files
nor claims backup, required-set, capacity, or consumer-cutover readiness.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import (
    AnnouncementArchiveInventory,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    ArchivePeriodReconciliation,
    ArchiveReconciliationReport,
)
from scripts.dev_validation.inventory_announcement_asset_capacity import (
    _validate_new_output_path,
    _write_new_json,
)
from scripts.dev_validation.prepare_announcement_asset_production_shadow import (
    _load_fresh_inventory,
    _production_projection,
    _read_mapping,
    _require_production_catalog,
    _require_shadow_safe_config,
    _validate_inventory_artifact,
)
from scripts.dev_validation.reconcile_announcement_asset_production_shadow import (
    SCHEMA_VERSION as RECONCILIATION_SCHEMA_VERSION,
)
from utils.config_manager import config_manager

SCHEMA_VERSION = "annual_report_asset_production_shadow_promotion.v1"
CONFIRMATION_TOKEN = "PROMOTE_PRODUCTION_SHADOW"


def _reconciliation_from_artifact(
    artifact: Mapping[str, Any],
) -> ArchiveReconciliationReport:
    details = artifact.get("reconciliation")
    if not isinstance(details, Mapping):
        raise ValueError("production_shadow_reconciliation_details_missing")
    periods_raw = details.get("periods")
    if not isinstance(periods_raw, list):
        raise ValueError("production_shadow_reconciliation_periods_missing")
    periods: list[ArchivePeriodReconciliation] = []
    for raw in periods_raw:
        if not isinstance(raw, Mapping):
            raise ValueError("production_shadow_reconciliation_period_invalid")
        periods.append(
            ArchivePeriodReconciliation(
                instrument_id=str(raw.get("instrument_id") or ""),
                fiscal_year=int(raw.get("fiscal_year")),
                status=str(raw.get("status") or ""),
                reason=str(raw.get("reason") or ""),
                canonical_asset_id=(
                    None
                    if raw.get("canonical_asset_id") is None
                    else str(raw["canonical_asset_id"])
                ),
                canonical_content_hash=(
                    None
                    if raw.get("canonical_content_hash") is None
                    else str(raw["canonical_content_hash"])
                ),
                legacy_source_file_ids=tuple(
                    str(value) for value in raw.get("legacy_source_file_ids", [])
                ),
                promotion_gate_id=(
                    None
                    if raw.get("promotion_gate_id") is None
                    else str(raw["promotion_gate_id"])
                ),
            )
        )
    conflict_count = int(details.get("conflict_count") or 0)
    pending_custody_count = int(details.get("pending_custody_count") or 0)
    ready = details.get("ready_for_cutover") is True
    if not ready or conflict_count or pending_custody_count:
        raise ValueError("production_shadow_reconciliation_not_ready")
    if not periods or any(
        not item.canonical_asset_id or not item.promotion_gate_id for item in periods
    ):
        raise ValueError("production_shadow_reconciliation_period_not_promotable")
    return ArchiveReconciliationReport(
        periods=tuple(periods),
        ready_for_cutover=True,
        conflict_count=0,
        inventory_fingerprint=str(artifact.get("inventory_fingerprint") or ""),
        config_fingerprint=str(artifact.get("configuration_fingerprint") or ""),
    )


def promote_production_shadow(
    *,
    production_db: Path,
    financials_db: Path,
    inventory_artifact_path: Path,
    reconciliation_artifact_path: Path,
    config: AnnouncementAssetConfig,
    project_root: Path = PROJECT_ROOT,
    manifest_rows: Iterable[Mapping[str, Any]] | None = None,
    operator: str = "",
    confirmation: str = "",
) -> Mapping[str, Any]:
    """Promote current reconciled rows without any archive-file mutation."""

    _require_shadow_safe_config(config)
    if confirmation != CONFIRMATION_TOKEN:
        raise PermissionError("production_shadow_promotion_confirmation_missing")
    operator_id = str(operator or "").strip()
    if not operator_id:
        raise PermissionError("production_shadow_promotion_operator_missing")
    production = _require_production_catalog(production_db, project_root=project_root)
    artifact = _read_mapping(reconciliation_artifact_path, "reconciliation_artifact")
    if (
        artifact.get("schema_version") != RECONCILIATION_SCHEMA_VERSION
        or artifact.get("mode") != "production_shadow_reconciliation"
        or artifact.get("configuration_fingerprint") != config.config_fingerprint
    ):
        raise ValueError("production_shadow_reconciliation_artifact_not_eligible")
    readiness = artifact.get("production_readiness")
    if (
        not isinstance(readiness, Mapping)
        or readiness.get("ready") is True
        or readiness.get("reconciliation_ready") is not True
    ):
        raise ValueError("production_shadow_reconciliation_artifact_not_ready")
    if artifact.get("promotion", {}).get("run") is not False:
        raise ValueError("production_shadow_reconciliation_already_promoted")
    if artifact.get("required_set_evidence", {}).get("status") != "not_measured":
        raise ValueError("production_shadow_reconciliation_claims_required_set")

    inventory, manifest_input = _load_fresh_inventory(
        config=config,
        financials_db=financials_db,
        project_root=project_root,
        manifest_rows=manifest_rows,
    )
    capacity_artifact = _read_mapping(inventory_artifact_path, "inventory_artifact")
    _validate_inventory_artifact(capacity_artifact, config=config, inventory=inventory)
    if artifact.get("inventory_fingerprint") != inventory.inventory_fingerprint:
        raise ValueError("production_shadow_inventory_changed_since_reconciliation")
    expected_inventory_path = str(inventory_artifact_path.resolve(strict=True))
    if artifact.get("inventory_artifact_path") != expected_inventory_path:
        raise ValueError("production_shadow_inventory_artifact_changed")

    repository = AnnouncementAssetRepository(production)
    if not repository.schema_initialized():
        raise RuntimeError("production_shadow_catalog_schema_missing")
    before = _production_projection(repository)
    before_asset_ids = {str(item[2]) for item in before}
    reconciliation = _reconciliation_from_artifact(artifact)
    promoted = AnnouncementArchiveInventory().promote_shadow_adoption(
        reconciliation,
        repository=repository,
        config=config,
    )
    after = _production_projection(repository)
    promoted_ids = sorted(item.asset_id for item in promoted)
    after_asset_ids = {str(item[2]) for item in after}
    added_ids = sorted(after_asset_ids - before_asset_ids)
    if (
        not promoted_ids
        or not set(promoted_ids).issubset(after_asset_ids)
        or len(after) != len(before) + len(added_ids)
    ):
        raise RuntimeError("production_shadow_promotion_projection_mismatch")
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "production_shadow_promotion",
        "operator": operator_id,
        "configuration_fingerprint": config.config_fingerprint,
        "inventory_fingerprint": inventory.inventory_fingerprint,
        "inventory_artifact_path": expected_inventory_path,
        "reconciliation_artifact_path": str(
            reconciliation_artifact_path.resolve(strict=True)
        ),
        "manifest_input": dict(manifest_input),
        "production_catalog": {
            "path": str(production),
            "projection_count_before": len(before),
            "projection_count_after": len(after),
            "writes": True,
        },
        "promotion": {
            "run": True,
            "production_visible_rows_added": len(added_ids),
            "asset_ids": promoted_ids,
            "replayed": not bool(added_ids),
        },
        "required_set_evidence": {
            "status": "not_measured",
            "reason": "verified backup must run before required-set measurement",
        },
        "production_readiness": {
            "ready": False,
            "promotion_ready": True,
            "reason": (
                "promotion_complete; verified backup, required-set measurement, "
                "capacity approval, bootstrap, and consumer cutover remain"
            ),
        },
        "network_requests": 0,
        "archive_mutations": {
            "copied": 0,
            "moved": 0,
            "linked": 0,
            "quarantined": 0,
            "deleted": 0,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-db", type=Path, default=PROJECT_ROOT / "data/research.db")
    parser.add_argument("--financials-db", type=Path, default=PROJECT_ROOT / "data/financials.db")
    parser.add_argument("--inventory-artifact", type=Path, required=True)
    parser.add_argument("--reconciliation-artifact", type=Path, required=True)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(), project_root=PROJECT_ROOT
    )
    result = promote_production_shadow(
        production_db=args.production_db,
        financials_db=args.financials_db,
        inventory_artifact_path=args.inventory_artifact,
        reconciliation_artifact_path=args.reconciliation_artifact,
        operator=args.operator,
        confirmation=args.confirm,
        config=config,
    )
    _write_new_json(output, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
