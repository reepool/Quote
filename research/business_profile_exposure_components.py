"""Conservative migration helpers for legacy mixed commodity exposures."""

from __future__ import annotations

import hashlib
import json
from typing import Any



LEGACY_DECOMPOSITION_SCHEMA_VERSION = "legacy_exposure_decomposition.v1"
LEGACY_DECOMPOSITION_POLICY_VERSION = "business_profile_legacy_decomposition.v1"


class BusinessProfileExposureComponentMigrator:
    """Create componentized successors only from uniquely proven legacy lineage."""

    def __init__(self, repository: Any):
        self.repository = repository

    def migrate(self, *, instrument_id: str | None = None) -> dict[str, Any]:
        rows = self.repository.list_records(
            "exposures",
            instrument_id=instrument_id,
            limit=10000,
        )
        result = {
            "rows_examined": len(rows),
            "componentized": 0,
            "already_componentized": 0,
            "legacy_compatible": 0,
            "failures": [],
        }
        for row in rows:
            if row.get("component_lineage_hash") and row.get("fact_ids"):
                result["already_componentized"] += 1
                continue
            if row.get("review_status") != "approved":
                result["legacy_compatible"] += 1
                result["failures"].append(
                    {
                        "exposure_id": row.get("exposure_id"),
                        "reason": "legacy_source_not_approved",
                    }
                )
                continue
            proof = (row.get("metadata") or {}).get("legacy_decomposition_proof")
            try:
                normalized = self._validate_proof(row, proof)
            except ValueError as exc:
                result["legacy_compatible"] += 1
                result["failures"].append(
                    {
                        "exposure_id": row.get("exposure_id"),
                        "reason": str(exc),
                    }
                )
                continue
            # A legacy proof can identify a replay candidate, but it does not
            # carry the current promotion manifest/gate context.  Do not use a
            # generic system reviewer to create an executable successor here.
            # The semantic repair operator reports this record and the regular
            # publication path replays it from approved local facts.
            result["legacy_compatible"] += 1
            result["failures"].append(
                {
                    "exposure_id": row.get("exposure_id"),
                    "reason": "legacy_replay_requires_current_publication_pipeline",
                    "proof": normalized,
                }
            )
            continue
        return result

    @staticmethod
    def _validate_proof(row: dict[str, Any], proof: Any) -> dict[str, Any]:
        if not isinstance(proof, dict):
            raise ValueError("legacy_lineage_incomplete")
        required = {
            "schema_version",
            "exposure_fact_type",
            "object_raw",
            "fact_scope",
            "mapping_ids",
            "direction_rule_id",
            "build_policy_version",
        }
        if set(proof) - {
            *required,
            "activity_id",
            "segment_id",
            "product_id",
            "value_raw",
            "unit_raw",
            "value_normalized",
            "unit_normalized",
            "share",
        }:
            raise ValueError("legacy_lineage_unknown_fields")
        if any(proof.get(key) in {None, ""} for key in required - {"mapping_ids"}):
            raise ValueError("legacy_lineage_incomplete")
        if proof.get("schema_version") != LEGACY_DECOMPOSITION_SCHEMA_VERSION:
            raise ValueError("legacy_lineage_schema_mismatch")
        mapping_ids = proof.get("mapping_ids")
        if (
            not isinstance(mapping_ids, list)
            or not mapping_ids
            or len(mapping_ids) != len(set(mapping_ids))
            or not all(str(item).strip() for item in mapping_ids)
        ):
            raise ValueError("legacy_mapping_lineage_ambiguous")
        if not row.get("evidence_id") or not row.get("direction"):
            raise ValueError("legacy_lineage_incomplete")
        return {**proof, "mapping_ids": [str(item).strip() for item in mapping_ids]}


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
