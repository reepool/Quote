"""Reconcile production shadow adoption without promotion or capacity approval.

This command is deliberately a separate stage from shadow registration.  It
checks the current catalog and legacy-file inventory again, records custody or
conflict outcomes, and creates promotion gates only when the existing
reconciliation service can prove them.  It never promotes assets, measures a
required set, downloads files, or mutates legacy paths.
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
)
from scripts.dev_validation.inventory_announcement_asset_capacity import (
    _validate_new_output_path,
    _write_new_json,
)
from scripts.dev_validation.prepare_announcement_asset_production_shadow import (
    SCHEMA_VERSION as SHADOW_SCHEMA_VERSION,
    _load_fresh_inventory,
    _production_projection,
    _read_mapping,
    _require_production_catalog,
    _require_shadow_safe_config,
    _validate_inventory_artifact,
)
from utils.config_manager import config_manager

SCHEMA_VERSION = "annual_report_asset_production_shadow_reconciliation.v1"
CONFIRMATION_TOKEN = "RECONCILE_PRODUCTION_SHADOW"


def _load_custody_evidence(path: Path | None) -> Mapping[str, Mapping[str, Any]]:
    if path is None:
        return {}
    payload = _read_mapping(path, "custody_evidence")
    rows = payload.get("by_path", payload)
    if not isinstance(rows, Mapping):
        raise TypeError("custody_evidence_by_path_not_mapping")
    return {
        str(key): dict(value)
        for key, value in rows.items()
        if isinstance(value, Mapping)
    }


def reconcile_production_shadow(
    *,
    production_db: Path,
    financials_db: Path,
    inventory_artifact_path: Path,
    shadow_artifact_path: Path,
    config: AnnouncementAssetConfig,
    custody_evidence_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
    manifest_rows: Iterable[Mapping[str, Any]] | None = None,
    operator: str = "",
    confirmation: str = "",
) -> Mapping[str, Any]:
    _require_shadow_safe_config(config)
    if confirmation != CONFIRMATION_TOKEN:
        raise PermissionError("production_shadow_reconciliation_confirmation_missing")
    if not str(operator or "").strip():
        raise PermissionError("production_shadow_reconciliation_operator_missing")
    production = _require_production_catalog(production_db, project_root=project_root)
    shadow = _read_mapping(shadow_artifact_path, "shadow_artifact")
    if (
        shadow.get("schema_version") != SHADOW_SCHEMA_VERSION
        or shadow.get("mode") != "production_shadow"
        or shadow.get("configuration_fingerprint") != config.config_fingerprint
    ):
        raise ValueError("production_shadow_artifact_not_eligible")
    readiness = shadow.get("production_readiness")
    if not isinstance(readiness, Mapping) or readiness.get("shadow_registration_ready") is not True:
        raise ValueError("production_shadow_registration_not_ready")
    if readiness.get("ready") is True:
        raise ValueError("production_shadow_artifact_claims_production_ready")

    inventory, manifest_input = _load_fresh_inventory(
        config=config,
        financials_db=financials_db,
        project_root=project_root,
        manifest_rows=manifest_rows,
    )
    artifact = _read_mapping(inventory_artifact_path, "inventory_artifact")
    _validate_inventory_artifact(artifact, config=config, inventory=inventory)
    shadow_inventory = shadow.get("inventory")
    if not isinstance(shadow_inventory, Mapping) or shadow_inventory.get(
        "fingerprint"
    ) != inventory.inventory_fingerprint:
        raise ValueError("production_shadow_inventory_changed_since_registration")

    repository = AnnouncementAssetRepository(production)
    if not repository.schema_initialized():
        raise RuntimeError("production_shadow_catalog_schema_missing")
    before = _production_projection(repository)
    reconciliation = AnnouncementArchiveInventory().reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
        config_fingerprint=config.config_fingerprint,
        legacy_custody_evidence_by_path=_load_custody_evidence(custody_evidence_path),
    )
    after = _production_projection(repository)
    periods = [
        {
            "instrument_id": item.instrument_id,
            "fiscal_year": item.fiscal_year,
            "status": item.status,
            "reason": item.reason,
            "canonical_asset_id": item.canonical_asset_id,
            "canonical_content_hash": item.canonical_content_hash,
            "legacy_source_file_ids": list(item.legacy_source_file_ids),
            "promotion_gate_id": item.promotion_gate_id,
        }
        for item in reconciliation.periods
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "production_shadow_reconciliation",
        "operator": str(operator).strip(),
        "production_readiness": {
            "ready": False,
            "reconciliation_ready": reconciliation.ready_for_cutover,
            "reason": (
                "reconciliation_only; promotion, verified backup, required-set "
                "measurement, and capacity approval remain"
            ),
        },
        "configuration_fingerprint": config.config_fingerprint,
        "inventory_fingerprint": reconciliation.inventory_fingerprint,
        "inventory_artifact_path": str(inventory_artifact_path.resolve(strict=True)),
        "shadow_artifact_path": str(shadow_artifact_path.resolve(strict=True)),
        "manifest_input": dict(manifest_input),
        "production_catalog": {
            "path": str(production),
            "projection_count_before": len(before),
            "projection_count_after": len(after),
            "projection_unchanged": before == after,
            "writes": True,
        },
        "reconciliation": {
            "ready_for_cutover": reconciliation.ready_for_cutover,
            "conflict_count": reconciliation.conflict_count,
            "pending_custody_count": sum(
                item["status"] == "custody_pending" for item in periods
            ),
            "period_count": len(periods),
            "periods": periods,
        },
        "required_set_evidence": {
            "status": "not_measured",
            "reason": "promotion and verified backup are required first",
        },
        "promotion": {"run": False, "production_visible_rows_added": 0},
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
    parser.add_argument("--shadow-artifact", type=Path, required=True)
    parser.add_argument("--custody-evidence", type=Path)
    parser.add_argument("--operator", required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(), project_root=PROJECT_ROOT
    )
    result = reconcile_production_shadow(
        production_db=args.production_db,
        financials_db=args.financials_db,
        inventory_artifact_path=args.inventory_artifact,
        shadow_artifact_path=args.shadow_artifact,
        custody_evidence_path=args.custody_evidence,
        operator=args.operator,
        confirmation=args.confirm,
        config=config,
    )
    _write_new_json(output, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
