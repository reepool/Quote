"""Finalize a measured capacity inventory into a validated approval candidate."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import (
    CAPACITY_ARTIFACT_SCHEMA_VERSION,
    AnnouncementAssetConfig,
    measure_required_set_evidence,
    validate_capacity_artifact,
)
from scripts.dev_validation.drill_announcement_asset_shadow_adoption import (
    SCHEMA_VERSION as SHADOW_SCHEMA_VERSION,
)
from utils.config_manager import config_manager


def _read_mapping(path: Path, name: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{name}_unreadable") from exc
    if not isinstance(payload, Mapping):
        raise TypeError(f"{name}_not_mapping")
    return payload


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name}_missing")
    return value


def _non_negative(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name}_invalid")
    return value


def _positive(value: Any, name: str) -> int:
    result = _non_negative(value, name)
    if result == 0:
        raise ValueError(f"{name}_not_positive")
    return result


def _is_beneath(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def _validate_output_path(path: Path, *, project_root: Path) -> Path:
    """Restrict approval publication to temporary or controlled runtime roots."""

    resolved = path.resolve(strict=False)
    allowed_roots = (
        Path("/tmp"),
        Path("/dev/shm"),
        project_root / "config/runtime_evidence",
    )
    if not any(_is_beneath(resolved, root) for root in allowed_roots):
        raise ValueError("capacity output must use a controlled evidence root")
    if resolved.exists():
        raise FileExistsError("capacity output already exists")
    if not resolved.parent.is_dir():
        raise FileNotFoundError("capacity output parent must already exist")
    return resolved


def _publish_validated_capacity_artifact(
    path: Path,
    payload: Mapping[str, Any],
    *,
    config: AnnouncementAssetConfig,
) -> None:
    """Validate a private inode before publishing it without replacement."""

    body = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    temporary: Path | None = None
    try:
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        validate_capacity_artifact(config, artifact_path=temporary)
        os.link(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def finalize_capacity_artifact(
    *,
    inventory_path: Path,
    shadow_path: Path,
    output_path: Path,
    config: AnnouncementAssetConfig,
    planning_horizon_years: int,
    budget_basis: str,
    required_set_evidence_path: Path,
    approver: str,
) -> Mapping[str, Any]:
    """Bind matching read-only evidence to explicit operator capacity inputs."""

    output = _validate_output_path(output_path, project_root=config.project_root)
    inventory = _read_mapping(inventory_path, "inventory")
    shadow = _read_mapping(shadow_path, "shadow")
    if inventory.get("schema_version") != CAPACITY_ARTIFACT_SCHEMA_VERSION:
        raise ValueError("inventory_schema_mismatch")
    if shadow.get("schema_version") != SHADOW_SCHEMA_VERSION:
        raise ValueError("shadow_schema_mismatch")
    if inventory.get("configuration_fingerprint") != config.config_fingerprint:
        raise ValueError("inventory_config_mismatch")
    for field in ("network_requests", "catalog_writes", "adoption_writes", "archive_mutations"):
        if inventory.get(field) != 0:
            raise ValueError(f"inventory_{field}_not_zero")
    if inventory.get("read_only") is not True:
        raise ValueError("inventory_not_read_only")
    if shadow.get("read_only_legacy_inputs") is not True:
        raise ValueError("shadow_inputs_not_read_only")
    if shadow.get("network_requests") != 0 or shadow.get("production_catalog_writes") != 0:
        raise ValueError("shadow_external_mutation_detected")
    production_readiness = _mapping(
        shadow.get("production_readiness"), "shadow.production_readiness"
    )
    if production_readiness.get("ready") is not True:
        raise ValueError("shadow_not_production_ready")
    mutations = _mapping(shadow.get("archive_mutations"), "shadow.archive_mutations")
    if any(_non_negative(value, f"shadow.archive_mutations.{name}") for name, value in mutations.items()):
        raise ValueError("shadow_archive_mutation_detected")

    inventory_details = _mapping(inventory.get("inventory"), "inventory.inventory")
    shadow_inventory = _mapping(shadow.get("inventory"), "shadow.inventory")
    inventory_fingerprint = str(
        inventory_details.get("inventory_fingerprint") or ""
    ).strip()
    if not inventory_fingerprint or shadow_inventory.get("fingerprint") != inventory_fingerprint:
        raise ValueError("inventory_shadow_fingerprint_mismatch")
    if shadow_inventory.get("counts") != inventory_details.get("counts"):
        raise ValueError("inventory_shadow_counts_mismatch")
    adoption = _mapping(shadow.get("adoption"), "shadow.adoption")
    if adoption.get("effective_scope_unique") is not True:
        raise ValueError("shadow_effective_scope_not_unique")
    if adoption.get("review_required_periods") not in ([], ()):
        raise ValueError("shadow_review_required")
    catalog_counts = _mapping(shadow.get("catalog_counts"), "shadow.catalog_counts")
    effective_scopes = _non_negative(
        adoption.get("effective_scope_count"), "shadow.effective_scope_count"
    )
    if _non_negative(catalog_counts.get("effective_reports"), "shadow.effective_reports") != effective_scopes:
        raise ValueError("shadow_effective_count_mismatch")

    horizon = _positive(planning_horizon_years, "planning_horizon_years")
    if budget_basis not in {"expected", "stress"}:
        raise ValueError("budget_basis_invalid")
    required_set_evidence = _read_mapping(
        required_set_evidence_path, "required_set_evidence"
    )
    measured_required_set = measure_required_set_evidence(config)
    if dict(required_set_evidence) != measured_required_set:
        raise ValueError("required_set_evidence_mismatch")
    primary_required = int(measured_required_set["primary_required_set"]["bytes"])
    backup_required = int(measured_required_set["backup_verified_set"]["bytes"])
    recovery_bytes = int(measured_required_set["permanent_recovery_set"]["bytes"])
    approver_name = str(approver).strip()
    if not approver_name:
        raise ValueError("approver_missing")

    payload = deepcopy(dict(inventory))
    planning = dict(_mapping(payload.get("planning"), "inventory.planning"))
    expected_growth = _positive(
        planning.get("expected_annual_growth_bytes"), "expected_annual_growth_bytes"
    )
    stress_growth = _positive(
        planning.get("stress_annual_growth_bytes"), "stress_annual_growth_bytes"
    )
    annual_growth = expected_growth if budget_basis == "expected" else stress_growth
    temporary_peak = _positive(
        planning.get("estimated_temporary_peak_bytes"), "estimated_temporary_peak_bytes"
    )
    replacement_peak = _positive(
        planning.get("estimated_old_plus_new_replacement_peak_bytes"),
        "estimated_old_plus_new_replacement_peak_bytes",
    )
    full_market_bytes = _positive(
        planning.get("estimated_full_market_required_bytes"),
        "estimated_full_market_required_bytes",
    )
    if (
        planning.get("old_plus_new_replacement_peak_basis")
        != "two_distinct_attachment_versions"
        or replacement_peak != 2 * full_market_bytes
    ):
        raise ValueError("replacement_peak_invalid")
    primary = _mapping(payload.get("primary_archive"), "primary_archive")
    backup = _mapping(payload.get("backup_target"), "backup_target")
    primary_free = _non_negative(
        _mapping(primary.get("usage"), "primary_archive.usage").get("free_bytes"),
        "primary_archive.free_bytes",
    )
    backup_free = _non_negative(
        _mapping(backup.get("usage"), "backup_target.usage").get("free_bytes"),
        "backup_target.free_bytes",
    )
    replacement_overhead = replacement_peak - full_market_bytes
    primary_need = (
        annual_growth * horizon
        + replacement_overhead
        + temporary_peak
        + config.storage.free_space_reserve_bytes
    )
    backup_need = (
        max(0, primary_required + recovery_bytes - backup_required)
        + annual_growth * horizon
        + replacement_overhead
        + temporary_peak
        + config.backup.free_space_reserve_bytes
    )
    primary_headroom = primary_free - primary_need
    backup_headroom = backup_free - backup_need
    if primary_headroom < 0 or backup_headroom < 0:
        raise ValueError("approved_capacity_insufficient")

    payload["planning"] = {
        **planning,
        "status": "approved",
        "planning_horizon_years": horizon,
        "planning_horizon_status": "operator_approved",
        "approved_budget_basis": budget_basis,
        "estimated_primary_headroom_bytes": primary_headroom,
        "estimated_backup_headroom_bytes": backup_headroom,
        "primary_required_set_actual_bytes": primary_required,
        "backup_required_set_actual_bytes": backup_required,
        "permanently_retained_recovery_manifest_bytes": recovery_bytes,
        "explicit_approver": approver_name,
    }
    payload["required_set_evidence"] = measured_required_set
    payload["approval"] = {
        "approved_at": datetime.now(timezone.utc).isoformat(),
        "inventory_path": str(inventory_path.resolve(strict=True)),
        "shadow_path": str(shadow_path.resolve(strict=True)),
        "required_set_evidence_path": str(
            required_set_evidence_path.resolve(strict=True)
        ),
        "inventory_fingerprint": inventory_fingerprint,
        "shadow_schema_version": SHADOW_SCHEMA_VERSION,
        "shadow_effective_scope_count": effective_scopes,
        "approver": approver_name,
    }

    _publish_validated_capacity_artifact(output, payload, config=config)
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--shadow", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--planning-horizon-years", type=int, required=True)
    parser.add_argument("--budget-basis", choices=("expected", "stress"), required=True)
    parser.add_argument("--required-set-evidence", type=Path, required=True)
    parser.add_argument("--approver", required=True)
    args = parser.parse_args(argv)
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(), project_root=PROJECT_ROOT
    )
    payload = finalize_capacity_artifact(
        inventory_path=args.inventory,
        shadow_path=args.shadow,
        output_path=args.output,
        config=config,
        planning_horizon_years=args.planning_horizon_years,
        budget_basis=args.budget_basis,
        required_set_evidence_path=args.required_set_evidence,
        approver=args.approver,
    )
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
