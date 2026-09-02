"""Fail-closed automatic promotion and exception routing for business profiles."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Mapping, Optional

from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_semantic_schemas import (
    validate_business_profile_artifact,
)
from utils.date_utils import get_shanghai_time


PROMOTION_POLICY_VERSION = "business_profile_auto_promotion.2026.1"
PROMOTION_REVIEWER_VERSION = "v1"

_REQUIRED_GATES = frozenset(
    {
        "official_identity",
        "artifact_quality",
        "exact_evidence",
        "catalogs_current",
        "temporal_scope",
        "numeric_reconciliation",
        "no_conflicts",
        "field_family_manifest",
        "runtime_identity_match",
        "candidate_current",
        "semantic_proof",
    }
)
_MACHINE_REWORK_REASONS = frozenset(
    {
        "ocr_required",
        "planned_document_missing_or_invalid_locally",
        "selector_gap",
        "context_incomplete",
        "evidence_provenance_failed",
        "unit_normalization_failed",
        "numeric_validation_failed",
        "partial_row_rejection",
        "verification_incomplete",
        "blocked_configuration",
        "schema_failure",
        "gateway_failure",
        "catalog_proposal",
        "runtime_identity_mismatch",
        "manifest_not_promoted",
        "stale_catalog",
        "transformation_lineage_missing",
        "product_mapping_required",
        "ambiguous_or_unsupported_exposure_direction",
        "ambiguous_or_unpromoted_product_commodity_mapping",
        "ambiguous_product_commodity_mapping",
        "stale_product_commodity_catalog",
    }
)
_QUICK_REVIEW_REASONS = frozenset({"entity_ambiguity", "exact_alias_ambiguity"})
_DEEP_REVIEW_REASONS = frozenset(
    {
        "conflicting_disclosures",
        "ambiguous_issuer_scope",
        "complex_scope_change",
        "complex_restructuring",
        "ambiguous_direction",
        "unsupported_materiality",
        "pass_through_judgment",
        "hedge_effectiveness_judgment",
        "valuation_assumption_requested",
        "prior_human_decision",
    }
)


@dataclass(frozen=True)
class FieldFamilyPromotionManifest:
    field_family: str
    enabled: bool
    benchmark_passed: bool
    identities: Mapping[str, str]
    required_gates: tuple[str, ...] = tuple(sorted(_REQUIRED_GATES))
    policy_version: str = PROMOTION_POLICY_VERSION

    def __post_init__(self) -> None:
        if not self.field_family:
            raise ValueError("promotion manifest field_family is required")
        if set(self.required_gates) != _REQUIRED_GATES:
            raise ValueError("promotion manifest required gates are incomplete")
        if not self.identities or any(
            not str(key).strip() or not str(value).strip()
            for key, value in self.identities.items()
        ):
            raise ValueError("promotion manifest identities must be complete")

    @property
    def manifest_hash(self) -> str:
        return _stable_hash(
            {
                "field_family": self.field_family,
                "enabled": self.enabled,
                "benchmark_passed": self.benchmark_passed,
                "identities": dict(sorted(self.identities.items())),
                "required_gates": list(self.required_gates),
                "policy_version": self.policy_version,
            }
        )


@dataclass(frozen=True)
class PromotionContext:
    target_type: str
    target_id: str
    instrument_id: str
    field_family: str
    expected_updated_at: str
    gates: Mapping[str, Any]
    runtime_identities: Mapping[str, str]
    evidence_references: tuple[str, ...] = ()
    exception_reasons: tuple[str, ...] = ()
    ranked_choices: tuple[Mapping[str, Any], ...] = ()
    high_risk_flags: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)


class BusinessProfilePromotionClassifier:
    """Classify one candidate with complete versioned fail-closed gates."""

    def classify(
        self,
        context: PromotionContext,
        manifest: FieldFamilyPromotionManifest,
    ) -> dict[str, Any]:
        reason_codes: list[str] = []
        if context.field_family != manifest.field_family:
            reason_codes.append("field_family_manifest_mismatch")
        if not manifest.enabled or not manifest.benchmark_passed:
            reason_codes.append("manifest_not_promoted")
        if dict(context.runtime_identities) != dict(manifest.identities):
            reason_codes.append("runtime_identity_mismatch")
        gate_keys = set(context.gates)
        missing = sorted(_REQUIRED_GATES - gate_keys)
        extra = sorted(gate_keys - _REQUIRED_GATES)
        reason_codes.extend(f"missing_gate:{name}" for name in missing)
        reason_codes.extend(f"unknown_gate:{name}" for name in extra)
        for name in sorted(_REQUIRED_GATES & gate_keys):
            if context.gates[name] is not True:
                reason_codes.append(f"failed_gate:{name}")
        reason_codes.extend(str(item) for item in context.exception_reasons)
        reason_codes.extend(str(item) for item in context.high_risk_flags)
        reason_codes = list(dict.fromkeys(reason_codes))

        simple_reasons = {item.split(":", 1)[-1] for item in reason_codes}
        if not reason_codes:
            classification = "auto_promoted"
        elif simple_reasons & _DEEP_REVIEW_REASONS:
            classification = "deep_review"
        elif simple_reasons and simple_reasons.issubset(_MACHINE_REWORK_REASONS):
            classification = "machine_rework"
        elif simple_reasons and simple_reasons.issubset(_QUICK_REVIEW_REASONS):
            classification = "quick_review"
        elif (
            "semantic_proof" in simple_reasons
            and simple_reasons - {"semantic_proof"}
            and (simple_reasons - {"semantic_proof"}).issubset(
                _MACHINE_REWORK_REASONS
            )
        ):
            # A deterministic proof held by a local gate is machine-reworkable.
            # A semantic verifier rejection has no companion machine reason and
            # remains deep review.
            classification = "machine_rework"
        elif any(
            item.startswith(("missing_gate:", "unknown_gate:")) for item in reason_codes
        ):
            classification = "machine_rework"
        elif any(
            item
            in {
                "failed_gate:artifact_quality",
                "failed_gate:catalogs_current",
                "failed_gate:runtime_identity_match",
                "failed_gate:field_family_manifest",
            }
            for item in reason_codes
        ):
            classification = "machine_rework"
        else:
            classification = "deep_review"
        gate_signature = _stable_hash(
            {
                "manifest_hash": manifest.manifest_hash,
                "gates": dict(sorted(context.gates.items())),
                "reasons": reason_codes,
            }
        )
        decision = {
            "schema_version": "business_profile_promotion_decision.v1",
            "target_type": context.target_type,
            "target_id": context.target_id,
            "classification": classification,
            "policy_version": manifest.policy_version,
            "gate_manifest_hash": manifest.manifest_hash,
            "reason_codes": reason_codes,
        }
        validate_business_profile_artifact("promotion_decision", decision)
        return {**decision, "gate_signature": gate_signature}


class BusinessProfilePromotionService:
    """Promote eligible candidates or persist a reason-coded exception."""

    def __init__(
        self,
        review_service: BusinessProfileReviewService,
        *,
        classifier: Optional[BusinessProfilePromotionClassifier] = None,
        max_machine_retries: int = 3,
    ) -> None:
        self.review_service = review_service
        self.repository = review_service.repository
        self.storage = review_service.storage
        self.classifier = classifier or BusinessProfilePromotionClassifier()
        self.max_machine_retries = max(0, int(max_machine_retries))

    def process(
        self,
        context: PromotionContext,
        manifest: FieldFamilyPromotionManifest,
    ) -> dict[str, Any]:
        decision = self.classifier.classify(context, manifest)
        if decision["classification"] == "auto_promoted":
            audit = self.review_service.system_promote_record(
                context.target_type,
                context.target_id,
                field_family=context.field_family,
                policy_version=manifest.policy_version,
                gate_manifest_hash=manifest.manifest_hash,
                reviewer_version=PROMOTION_REVIEWER_VERSION,
                expected_updated_at=context.expected_updated_at,
                evidence_references=context.evidence_references,
                metadata={
                    "promotion_decision": {
                        key: value
                        for key, value in decision.items()
                        if key != "gate_signature"
                    },
                    "gate_signature": decision["gate_signature"],
                    "runtime_identities": dict(context.runtime_identities),
                    "gates": dict(context.gates),
                    **dict(context.metadata or {}),
                },
            )
            self._resolve_open_exceptions(context)
            return {"decision": decision, "promoted": True, "audit": audit}
        exception = self._upsert_exception(context, decision)
        return {"decision": decision, "promoted": False, "exception": exception}

    def list_exceptions(
        self,
        *,
        instrument_id: Optional[str] = None,
        tier: Optional[str] = None,
        status: str = "open",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        clauses = ["status = ?"]
        params: list[Any] = [status]
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if tier:
            clauses.append("tier = ?")
            params.append(tier)
        params.append(max(1, min(int(limit), 10000)))
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_exceptions "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY updated_at DESC, exception_id DESC LIMIT ?",
                params,
            ).fetchall()
        return [_decode_exception(dict(row)) for row in rows]

    def resolve_open_exceptions_for_target(
        self,
        *,
        target_id: str,
        field_family: str | None = None,
        target_type: str | None = None,
    ) -> int:
        """Resolve stale exceptions after the represented target converges."""

        clauses = ["target_id = ?", "status = 'open'"]
        params: list[Any] = [str(target_id)]
        if field_family:
            clauses.append("field_family = ?")
            params.append(str(field_family))
        if target_type:
            clauses.append("target_type = ?")
            params.append(str(target_type))
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                "UPDATE business_profile_exceptions "
                "SET status = 'resolved', resolved_at = ?, updated_at = ? "
                f"WHERE {' AND '.join(clauses)}",
                (now, now, *params),
            )
            conn.commit()
        return max(0, int(cursor.rowcount or 0))

    def _upsert_exception(
        self,
        context: PromotionContext,
        decision: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = get_shanghai_time()
        gate_signature = str(decision["gate_signature"])
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT * FROM business_profile_exceptions "
                    "WHERE target_type = ? AND target_id = ? AND gate_signature = ?",
                    (context.target_type, context.target_id, gate_signature),
                ).fetchone()
                retry_count = int(existing["retry_count"] or 0) if existing else 0
                tier = str(decision["classification"])
                reason_codes = list(decision["reason_codes"])
                next_retry_at = None
                if tier == "machine_rework" and retry_count < self.max_machine_retries:
                    retry_count += 1
                    next_retry_at = (
                        now + timedelta(minutes=2 ** (retry_count - 1))
                    ).isoformat()
                elif tier == "machine_rework":
                    reason_codes.append("machine_rework_exhausted")
                exception_id = (
                    str(existing["exception_id"])
                    if existing is not None
                    else "bp-exception-"
                    + _stable_hash(
                        {
                            "target_type": context.target_type,
                            "target_id": context.target_id,
                            "gate_signature": gate_signature,
                        }
                    )[:24]
                )
                conn.execute(
                    "UPDATE business_profile_exceptions "
                    "SET status = 'resolved', resolved_at = ?, updated_at = ? "
                    "WHERE target_type = ? AND target_id = ? "
                    "AND status = 'open' AND exception_id <> ?",
                    (
                        now.isoformat(),
                        now.isoformat(),
                        context.target_type,
                        context.target_id,
                        exception_id,
                    ),
                )
                schema_payload = {
                    "schema_version": "business_profile_exception.v1",
                    "exception_id": exception_id,
                    "target_type": context.target_type,
                    "target_id": context.target_id,
                    "tier": tier,
                    "reason_codes": reason_codes,
                    "retry_count": retry_count,
                    "next_retry_at": next_retry_at,
                    "gate_signature": gate_signature,
                    "resolved_at": None,
                }
                validate_business_profile_artifact("exception_record", schema_payload)
                created_at = (
                    str(existing["created_at"]) if existing else now.isoformat()
                )
                conn.execute(
                    """
                    INSERT INTO business_profile_exceptions (
                        exception_id, target_type, target_id, instrument_id,
                        field_family, tier, reason_codes_json, retry_count,
                        next_retry_at, gate_signature, gate_manifest_hash,
                        evidence_references_json, ranked_choices_json, status,
                        resolved_at, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', NULL, ?, ?, ?)
                    ON CONFLICT(exception_id) DO UPDATE SET
                        tier = excluded.tier,
                        reason_codes_json = excluded.reason_codes_json,
                        retry_count = excluded.retry_count,
                        next_retry_at = excluded.next_retry_at,
                        evidence_references_json = excluded.evidence_references_json,
                        ranked_choices_json = excluded.ranked_choices_json,
                        status = 'open',
                        resolved_at = NULL,
                        metadata_json = excluded.metadata_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        exception_id,
                        context.target_type,
                        context.target_id,
                        context.instrument_id,
                        context.field_family,
                        tier,
                        _json(reason_codes),
                        retry_count,
                        next_retry_at,
                        gate_signature,
                        decision["gate_manifest_hash"],
                        _json(context.evidence_references),
                        _json(context.ranked_choices),
                        _json(
                            {
                                "gates": dict(context.gates),
                                **dict(context.metadata),
                                "machine_rework_exhausted": (
                                    "machine_rework_exhausted" in reason_codes
                                ),
                            }
                        ),
                        created_at,
                        now.isoformat(),
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT * FROM business_profile_exceptions WHERE exception_id = ?",
                (exception_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError(
                f"persisted business profile exception is missing: {exception_id}"
            )
        return _decode_exception(dict(row))

    def _resolve_open_exceptions(self, context: PromotionContext) -> None:
        self.resolve_open_exceptions_for_target(
            target_id=context.target_id,
            target_type=context.target_type,
        )


def _decode_exception(row: dict[str, Any]) -> dict[str, Any]:
    for key in (
        "reason_codes_json",
        "evidence_references_json",
        "ranked_choices_json",
        "metadata_json",
    ):
        output_key = key.removesuffix("_json")
        row[output_key] = json.loads(row.pop(key) or "[]")
    return row


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()
