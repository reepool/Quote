"""Conservative migration helpers for legacy mixed commodity exposures."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from research.business_profile_review import BusinessProfileReviewService


LEGACY_DECOMPOSITION_SCHEMA_VERSION = "legacy_exposure_decomposition.v1"
LEGACY_DECOMPOSITION_POLICY_VERSION = "business_profile_legacy_decomposition.v1"


class BusinessProfileExposureComponentMigrator:
    """Create componentized successors only from uniquely proven legacy lineage."""

    def __init__(self, repository: Any):
        self.repository = repository
        self.review_service = BusinessProfileReviewService(repository)

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
            successor_id = f"{row['exposure_id']}:componentized:v1"
            existing = self.repository.list_records(
                "exposures",
                instrument_id=row["instrument_id"],
                limit=10000,
            )
            if any(item.get("exposure_id") == successor_id for item in existing):
                result["already_componentized"] += 1
                continue
            fact_id = f"{row['exposure_id']}:fact:v1"
            fact = {
                "fact_id": fact_id,
                "instrument_id": row["instrument_id"],
                "report_period": row["report_period"],
                "activity_id": normalized.get("activity_id"),
                "segment_id": normalized.get("segment_id"),
                "exposure_fact_type": normalized["exposure_fact_type"],
                "object_raw": normalized["object_raw"],
                "product_id": normalized.get("product_id"),
                "value_raw": normalized.get("value_raw"),
                "unit_raw": normalized.get("unit_raw"),
                "value_normalized": normalized.get("value_normalized"),
                "unit_normalized": normalized.get("unit_normalized"),
                "share": normalized.get("share"),
                "fact_scope": normalized["fact_scope"],
                "evidence_id": row["evidence_id"],
                "run_id": None,
                "data_available_date": row["data_available_date"],
                "confidence": row["confidence"],
                "review_status": "candidate",
                "valid_from": row.get("effective_from"),
                "valid_to": row.get("effective_to"),
                "business_regime_id": row.get("business_regime_id"),
                "knowledge_from": row.get("knowledge_from")
                or row["data_available_date"],
                "knowledge_to": row.get("knowledge_to"),
                "version": 1,
                "metadata": {
                    "migrated_from_exposure_id": row["exposure_id"],
                    "decomposition_schema_version": LEGACY_DECOMPOSITION_SCHEMA_VERSION,
                },
            }
            self.repository.upsert("exposure_facts", fact)
            self._promote(
                "exposure_facts",
                fact_id,
                field_family="commodity_exposure_facts",
                evidence_references=[row["evidence_id"]],
            )
            component_lineage_hash = _stable_hash(
                {
                    "fact_ids": [fact_id],
                    "mapping_ids": normalized["mapping_ids"],
                    "assumption_ids": [],
                    "direction_rule_id": normalized["direction_rule_id"],
                    "build_policy_version": normalized["build_policy_version"],
                }
            )
            successor = {
                **{
                    key: value
                    for key, value in row.items()
                    if key
                    not in {
                        "metadata",
                        "created_at",
                        "updated_at",
                        "lineage_hash",
                        "review_status",
                        "version",
                        "fact_ids",
                        "mapping_ids",
                        "assumption_ids",
                    }
                },
                "exposure_id": successor_id,
                "review_status": "candidate",
                "fact_ids": [fact_id],
                "mapping_ids": normalized["mapping_ids"],
                "assumption_ids": [],
                "direction_rule_id": normalized["direction_rule_id"],
                "build_policy_version": normalized["build_policy_version"],
                "build_policy_hash": _stable_hash(normalized["build_policy_version"]),
                "component_lineage_hash": component_lineage_hash,
                "legacy_compatibility_status": "componentized_successor",
                "supersedes_exposure_id": row["exposure_id"],
                "version": int(row.get("version") or 1) + 1,
                "metadata": {
                    **(row.get("metadata") or {}),
                    "migrated_from_exposure_id": row["exposure_id"],
                },
            }
            self.repository.upsert("exposures", successor)
            self._promote(
                "exposures",
                successor_id,
                field_family="commodity_exposure_publication",
                evidence_references=[row["evidence_id"], fact_id],
            )
            result["componentized"] += 1
        return result

    def _promote(
        self,
        record_type: str,
        record_id: str,
        *,
        field_family: str,
        evidence_references: list[str],
    ) -> None:
        current = next(
            item
            for item in self.repository.list_records(record_type, limit=10000)
            if item[self.repository._TABLES[record_type]["pk"]] == record_id
        )
        self.review_service.system_promote_record(
            record_type,
            record_id,
            field_family=field_family,
            policy_version=LEGACY_DECOMPOSITION_POLICY_VERSION,
            gate_manifest_hash=_stable_hash(
                {
                    "policy": LEGACY_DECOMPOSITION_POLICY_VERSION,
                    "record_type": record_type,
                    "record_id": record_id,
                }
            ),
            reviewer_version="v1",
            expected_updated_at=current["updated_at"],
            evidence_references=evidence_references,
        )

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
