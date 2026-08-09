"""Persistent append-only governance for previously unknown business units."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

from research.business_profile_numeric_reconciliation import decimal_value
from research.business_profile_unit_conversions import (
    load_unit_conversion_catalog,
    normalize_unit_lexeme,
)
from utils.llm import LlmMessage, LlmRequest
from utils.date_utils import get_shanghai_time


UNIT_RULE_REGISTRY_VERSION = "business_profile_unit_rules.v1"
UNIT_RULE_STATUSES = {
    "proposed",
    "shadow_active",
    "auto_approved",
    "quarantined",
    "superseded",
}
PROHIBITED_TRANSFORMATIONS = {"fx", "non_linear", "contextual", "offset"}
GOVERNED_DIMENSIONS = {
    "currency",
    "count",
    "mass",
    "area",
    "volume",
    "liquid_volume",
    "energy",
    "power",
    "length",
    "duration",
    "ratio",
    "price_per_mass",
    "price_per_volume",
    "price_per_liquid_volume",
    "price_per_energy",
    "price_per_power",
    "price_per_count",
    "mass_capacity",
    "volume_capacity",
    "liquid_volume_rate",
    "energy_capacity",
    "power_capacity",
    "count_capacity",
}
UNIT_PROPOSAL_KEYS = {
    "source_unit",
    "normalized_lexeme",
    "dimension",
    "canonical_unit",
    "numerator",
    "denominator",
    "primitive_rule_ids",
    "factors",
    "transformation_type",
    "round_trip_vectors",
    "semantic_summary_zh",
}


@dataclass(frozen=True)
class UnitRuleProof:
    valid: bool
    status: str
    multiplier: Optional[Decimal]
    reason_codes: tuple[str, ...]
    proof_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "status": self.status,
            "multiplier": str(self.multiplier) if self.multiplier is not None else None,
            "reason_codes": list(self.reason_codes),
            "proof_hash": self.proof_hash,
        }


class BusinessProfileUnitRuleRegistry:
    def __init__(
        self,
        storage: Any,
        *,
        primitive_multipliers: Optional[Mapping[str, Any]] = None,
        corroboration_observations: int = 3,
        corroboration_models: int = 2,
        corroboration_reconciliations: int = 2,
    ) -> None:
        self.storage = storage
        self.primitive_multipliers = {
            str(key): decimal_value(value, f"primitive:{key}")
            for key, value in dict(primitive_multipliers or {}).items()
        }
        self.corroboration_observations = max(2, int(corroboration_observations))
        self.corroboration_models = max(1, int(corroboration_models))
        self.corroboration_reconciliations = max(
            1, int(corroboration_reconciliations)
        )

    def register_proposal(
        self,
        proposal: Mapping[str, Any],
        *,
        proposal_input_hash: str,
        artifact_id: Optional[str] = None,
        source_document_id: Optional[str] = None,
        context_hash: Optional[str] = None,
        model_identity: Optional[str] = None,
    ) -> dict[str, Any]:
        validated = validate_unit_proposal(proposal)
        rule_id = "bp-unit-rule-" + _stable_hash(
            {
                "normalized_lexeme": validated["normalized_lexeme"],
                "dimension": validated["dimension"],
                "canonical_unit": validated["canonical_unit"],
            }
        )[:20]
        proof = prove_unit_proposal(
            validated,
            primitive_multipliers=self.proof_primitives(),
            existing_rule_ids=self._known_rule_ids(),
            target_rule_id=rule_id,
        )
        self._append_rule_event(
            rule_id,
            "proposed",
            validated,
            proof,
            proposal_input_hash=proposal_input_hash,
        )
        lifecycle_status = proof.status
        catalog_version = None
        if lifecycle_status == "auto_approved":
            catalog_version = self._commit_catalog_version(rule_id)
        self._append_rule_event(
            rule_id,
            lifecycle_status,
            validated,
            proof,
            proposal_input_hash=proposal_input_hash,
            catalog_version=catalog_version,
        )
        if context_hash:
            self.observe(
                rule_id,
                artifact_id=artifact_id,
                source_document_id=source_document_id,
                context_hash=context_hash,
                model_identity=model_identity,
            )
        if lifecycle_status == "auto_approved":
            self._replay_affected_artifacts(rule_id, reason="unit_rule_auto_approved")
        self.queue_notification(rule_id, lifecycle_status)
        return self.get_rule(rule_id)

    def proof_primitives(self) -> dict[str, Decimal]:
        """Return governed primitives plus committed auto-approved overlays."""

        primitives = dict(self.primitive_multipliers)
        for rule in self.overlay_rules():
            multiplier = rule.get("multiplier")
            if multiplier is None:
                continue
            primitives[str(rule["rule_id"])] = decimal_value(
                multiplier, f"runtime primitive:{rule['rule_id']}"
            )
        return primitives

    def observe(
        self,
        rule_id: str,
        *,
        context_hash: str,
        artifact_id: Optional[str] = None,
        source_document_id: Optional[str] = None,
        model_identity: Optional[str] = None,
        reconciliation_status: Optional[str] = None,
    ) -> dict[str, Any]:
        now = get_shanghai_time().isoformat()
        observation_id = "bp-unit-observation-" + _stable_hash(
            {"rule_id": rule_id, "context_hash": context_hash}
        )[:24]
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT OR IGNORE INTO business_profile_unit_rule_observations (
                    observation_id, rule_id, artifact_id, source_document_id,
                    context_hash, model_identity, reconciliation_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_id,
                    rule_id,
                    artifact_id,
                    source_document_id,
                    context_hash,
                    model_identity,
                    reconciliation_status,
                    now,
                ),
            )
            counts = conn.execute(
                """
                SELECT COUNT(*) observations,
                       COUNT(DISTINCT model_identity) models,
                       SUM(CASE WHEN reconciliation_status = 'passed' THEN 1 ELSE 0 END)
                           reconciliations
                FROM business_profile_unit_rule_observations WHERE rule_id = ?
                """,
                (rule_id,),
            ).fetchone()
            conn.commit()
        rule = self.get_rule(rule_id)
        if (
            rule.get("status") == "shadow_active"
            and int(counts["observations"] or 0) >= self.corroboration_observations
            and int(counts["models"] or 0) >= self.corroboration_models
            and int(counts["reconciliations"] or 0)
            >= self.corroboration_reconciliations
        ):
            self.promote_shadow(rule_id, counts=dict(counts))
        return self.get_rule(rule_id)

    def promote_shadow(
        self, rule_id: str, *, counts: Mapping[str, Any]
    ) -> dict[str, Any]:
        current = self.get_rule(rule_id)
        if current.get("status") != "shadow_active":
            return current
        proposal = dict(current.get("proposal") or {})
        proof = UnitRuleProof(
            valid=True,
            status="auto_approved",
            multiplier=decimal_value(current["multiplier"], "multiplier"),
            reason_codes=("automated_corroboration_threshold_met",),
            proof_hash=_stable_hash(dict(counts)),
        )
        catalog_version = self._commit_catalog_version(rule_id)
        self._append_rule_event(
            rule_id,
            "auto_approved",
            proposal,
            proof,
            proposal_input_hash=str(current["proposal_input_hash"]),
            catalog_version=catalog_version,
            counters=counts,
        )
        self._replay_affected_artifacts(rule_id, reason="unit_rule_promoted")
        self.queue_notification(rule_id, "auto_approved")
        return self.get_rule(rule_id)

    def supersede(
        self,
        rule_id: str,
        replacement_proposal: Mapping[str, Any],
        *,
        proposal_input_hash: str,
    ) -> dict[str, Any]:
        current = self.get_rule(rule_id)
        replacement = self.register_proposal(
            replacement_proposal,
            proposal_input_hash=proposal_input_hash,
        )
        proof = UnitRuleProof(
            valid=False,
            status="superseded",
            multiplier=(
                decimal_value(current["multiplier"])
                if current.get("multiplier") is not None
                else None
            ),
            reason_codes=(f"superseded_by:{replacement['rule_id']}",),
            proof_hash=_stable_hash(
                {"old": rule_id, "replacement": replacement["rule_id"]}
            ),
        )
        self._append_rule_event(
            rule_id,
            "superseded",
            dict(current.get("proposal") or {}),
            proof,
            proposal_input_hash=str(current["proposal_input_hash"]),
            supersedes_rule_id=str(replacement["rule_id"]),
        )
        self._replay_affected_artifacts(
            rule_id,
            reason=f"unit_rule_superseded_by:{replacement['rule_id']}",
        )
        self.queue_notification(rule_id, "superseded")
        return replacement

    def overlay_rules(self, *, include_shadow: bool = False) -> list[dict[str, Any]]:
        statuses = ["auto_approved"] + (["shadow_active"] if include_shadow else [])
        placeholders = ",".join("?" for _ in statuses)
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT r.* FROM business_profile_unit_rules r
                WHERE r.rowid = (
                    SELECT latest.rowid FROM business_profile_unit_rules latest
                    WHERE latest.rule_id = r.rule_id
                    ORDER BY latest.created_at DESC, latest.rowid DESC LIMIT 1
                )
                AND r.status IN ({placeholders})
                ORDER BY r.normalized_lexeme, r.rule_id
                """,
                statuses,
            ).fetchall()
        return [self._decode_rule(dict(row)) for row in rows]

    def get_rule(self, rule_id: str) -> dict[str, Any]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT * FROM business_profile_unit_rules WHERE rule_id = ? "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (rule_id,),
            ).fetchone()
        if row is None:
            raise ValueError(f"unknown unit rule: {rule_id}")
        return self._decode_rule(dict(row))

    def queue_notification(
        self, rule_id: str, lifecycle_status: str, *, impact_window: Optional[str] = None
    ) -> str:
        window = impact_window or get_shanghai_time().date().isoformat()
        payload = {"rule_id": rule_id, "status": lifecycle_status, "window": window}
        payload_hash = _stable_hash(payload)
        notification_id = "bp-unit-notify-" + payload_hash[:24]
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                """
                INSERT OR IGNORE INTO business_profile_unit_rule_notifications (
                    notification_id, rule_id, lifecycle_status, impact_window,
                    payload_hash, delivery_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    notification_id,
                    rule_id,
                    lifecycle_status,
                    window,
                    payload_hash,
                    now,
                    now,
                ),
            )
            conn.commit()
        return notification_id

    async def dispatch_notifications(
        self,
        notifier: Callable[[str], Awaitable[Any]],
        *,
        limit: int = 20,
    ) -> int:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_unit_rule_notifications "
                "WHERE delivery_status IN ('pending', 'retry') "
                "ORDER BY created_at LIMIT ?",
                (max(1, min(int(limit), 100)),),
            ).fetchall()
        delivered = 0
        for raw in rows:
            row = dict(raw)
            rule = self.get_rule(str(row["rule_id"]))
            message = (
                "[公司画像单位规则] "
                f"状态={row['lifecycle_status']} 单位={rule['source_unit']} "
                f"维度={rule.get('dimension')} 倍率={rule.get('multiplier')} "
                f"规则={rule['rule_id']}"
            )
            try:
                receipt = await notifier(message)
            except Exception as exc:
                self._update_notification(
                    str(row["notification_id"]), "retry", error=type(exc).__name__
                )
            else:
                self._update_notification(
                    str(row["notification_id"]),
                    "delivered",
                    message_id=str(getattr(receipt, "id", "") or "") or None,
                )
                delivered += 1
        return delivered

    def _append_rule_event(
        self,
        rule_id: str,
        status: str,
        proposal: Mapping[str, Any],
        proof: UnitRuleProof,
        *,
        proposal_input_hash: str,
        catalog_version: Optional[str] = None,
        supersedes_rule_id: Optional[str] = None,
        counters: Optional[Mapping[str, Any]] = None,
    ) -> None:
        now = get_shanghai_time().isoformat()
        event_id = "bp-unit-event-" + _stable_hash(
            {"rule_id": rule_id, "status": status, "proof": proof.proof_hash}
        )[:24]
        counts = dict(counters or {})
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                """
                INSERT OR IGNORE INTO business_profile_unit_rules (
                    rule_event_id, rule_id, normalized_lexeme, source_unit,
                    status, dimension, canonical_unit, multiplier, numerator_json,
                    denominator_json, primitive_rule_ids_json, proposal_json,
                    proof_json, proposal_input_hash, proof_hash, catalog_version,
                    supersedes_rule_id, observation_count, independent_model_count,
                    reconciliation_pass_count, affected_fact_count, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    rule_id,
                    proposal.get("normalized_lexeme"),
                    proposal.get("source_unit"),
                    status,
                    proposal.get("dimension"),
                    proposal.get("canonical_unit"),
                    str(proof.multiplier) if proof.multiplier is not None else None,
                    _json(proposal.get("numerator") or []),
                    _json(proposal.get("denominator") or []),
                    _json(proposal.get("primitive_rule_ids") or []),
                    _json(dict(proposal)),
                    _json(proof.to_dict()),
                    proposal_input_hash,
                    proof.proof_hash,
                    catalog_version,
                    supersedes_rule_id,
                    int(counts.get("observations") or 1),
                    int(counts.get("models") or 0),
                    int(counts.get("reconciliations") or 0),
                    int(counts.get("affected_fact_count") or 0),
                    now,
                ),
            )
            conn.commit()

    def _commit_catalog_version(self, rule_id: str) -> str:
        now = get_shanghai_time().isoformat()
        version = "business_profile_runtime_units." + now[:19].replace("-", "").replace(
            ":", ""
        ).replace("T", ".") + "." + rule_id[-6:]
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            parent_row = conn.execute(
                "SELECT catalog_version FROM business_profile_unit_catalog_versions "
                "ORDER BY committed_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
            parent_version = (
                str(parent_row["catalog_version"])
                if parent_row is not None
                else load_unit_conversion_catalog().catalog_version
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO business_profile_unit_catalog_versions (
                    catalog_version, parent_catalog_version, reason, rule_ids_json,
                    committed_at, created_at
                ) VALUES (?, ?, 'runtime_rule_commit', ?, ?, ?)
                """,
                (version, parent_version, _json([rule_id]), now, now),
            )
            conn.commit()
        return version

    def _known_rule_ids(self) -> set[str]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT DISTINCT rule_id FROM business_profile_unit_rules"
            ).fetchall()
        return {str(row["rule_id"]) for row in rows}

    def _update_notification(
        self,
        notification_id: str,
        status: str,
        *,
        message_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                """
                UPDATE business_profile_unit_rule_notifications
                SET delivery_status = ?, telegram_message_id = ?, last_error = ?,
                    attempt_count = attempt_count + 1, updated_at = ?
                WHERE notification_id = ?
                """,
                (status, message_id, error, now, notification_id),
            )
            conn.commit()

    def _replay_affected_artifacts(self, rule_id: str, *, reason: str) -> int:
        from research.business_profile_semantic_artifacts import (
            BusinessProfileSemanticArtifactRepository,
        )

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT DISTINCT o.artifact_id, a.source_document_id, a.instrument_id "
                "FROM business_profile_unit_rule_observations o "
                "LEFT JOIN business_profile_semantic_artifacts a "
                "ON a.artifact_id = o.artifact_id "
                "WHERE o.rule_id = ? AND o.artifact_id IS NOT NULL",
                (rule_id,),
            ).fetchall()
        repository = BusinessProfileSemanticArtifactRepository(self.storage)
        replayed = 0
        for row in rows:
            repository.mark(
                str(row["artifact_id"]),
                "conversion_pending",
                reason_code=reason,
                metadata={"unit_rule_id": rule_id},
            )
            replayed += 1
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            for row in rows:
                if not row["source_document_id"]:
                    continue
                conn.execute(
                    """
                    UPDATE business_profile_work_items
                    SET stage = 'semantic', status = 'retry_due', attempt_count = 0,
                        lease_owner = NULL, lease_expires_at = NULL,
                        next_attempt_at = NULL, completed_at = NULL,
                        last_error = ?, updated_at = ?
                    WHERE instrument_id = ? AND status IN (
                        'completed', 'machine_rework', 'retry_due', 'terminal_failure'
                    )
                      AND metadata_json LIKE ?
                    """,
                    (
                        reason,
                        now,
                        row["instrument_id"],
                        f"%{row['source_document_id']}%",
                    ),
                )
            conn.commit()
        return replayed

    @staticmethod
    def _decode_rule(row: dict[str, Any]) -> dict[str, Any]:
        for key in (
            "numerator_json",
            "denominator_json",
            "primitive_rule_ids_json",
            "proposal_json",
            "proof_json",
        ):
            row[key.removesuffix("_json")] = json.loads(row.pop(key) or "{}")
        return row


def validate_unit_proposal(proposal: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(proposal, Mapping):
        raise ValueError("unit proposal must be an object")
    unknown = set(proposal) - UNIT_PROPOSAL_KEYS
    if unknown:
        raise ValueError(f"unit proposal has unknown fields: {sorted(unknown)}")
    required = {
        "source_unit",
        "normalized_lexeme",
        "dimension",
        "canonical_unit",
        "primitive_rule_ids",
        "factors",
        "transformation_type",
        "round_trip_vectors",
    }
    missing = sorted(key for key in required if proposal.get(key) in (None, ""))
    if missing:
        raise ValueError(f"unit proposal missing fields: {missing}")
    output = dict(proposal)
    output["source_unit"] = str(output["source_unit"]).strip()
    output["normalized_lexeme"] = normalize_unit_lexeme(output["normalized_lexeme"])
    if output["normalized_lexeme"] != normalize_unit_lexeme(output["source_unit"]):
        raise ValueError("unit proposal normalized lexeme does not match source unit")
    for key in ("numerator", "denominator", "primitive_rule_ids", "factors", "round_trip_vectors"):
        if not isinstance(output.get(key, []), list):
            raise ValueError(f"unit proposal {key} must be an array")
    return output


def prove_unit_proposal(
    proposal: Mapping[str, Any],
    *,
    primitive_multipliers: Mapping[str, Decimal],
    existing_rule_ids: set[str],
    target_rule_id: str,
) -> UnitRuleProof:
    reasons: list[str] = []
    transformation = str(proposal.get("transformation_type") or "")
    dimension = str(proposal.get("dimension") or "")
    if transformation in PROHIBITED_TRANSFORMATIONS:
        reasons.append("prohibited_transformation")
    if transformation != "linear_multiplier":
        reasons.append("non_linear_or_unknown_transformation")
    if dimension not in GOVERNED_DIMENSIONS:
        reasons.append("new_or_unknown_dimension")
    references = tuple(str(item) for item in proposal.get("primitive_rule_ids") or ())
    round_trip_vectors = tuple(proposal.get("round_trip_vectors") or ())
    if target_rule_id in references:
        reasons.append("dependency_cycle")
    unknown_references = set(references) - set(primitive_multipliers) - existing_rule_ids
    if unknown_references:
        reasons.append("unknown_primitive_reference")
    multiplier = Decimal("1")
    try:
        for factor in proposal.get("factors") or ():
            if not isinstance(factor, Mapping):
                raise ValueError("factor_not_object")
            reference = str(factor.get("primitive_rule_id") or "")
            exponent = int(factor.get("exponent", 1))
            if exponent not in {-1, 1}:
                raise ValueError("unsupported_exponent")
            primitive = primitive_multipliers.get(reference)
            if primitive is None:
                raise ValueError("unknown_factor_reference")
            multiplier = multiplier * (primitive if exponent == 1 else Decimal("1") / primitive)
    except (ValueError, ArithmeticError):
        reasons.append("multiplier_recomputation_failed")
        multiplier = None
    if references and not round_trip_vectors:
        reasons.append("round_trip_vectors_missing")
    if multiplier is not None:
        for vector in round_trip_vectors:
            try:
                source = decimal_value(vector["source"], "vector source")
                canonical = decimal_value(vector["canonical"], "vector canonical")
                if source * multiplier != canonical or canonical / multiplier != source:
                    reasons.append("round_trip_failed")
                    break
            except (KeyError, ValueError, ArithmeticError):
                reasons.append("invalid_round_trip_vector")
                break
    valid = not reasons and multiplier is not None
    if valid and references:
        status = "auto_approved"
    elif not reasons and multiplier is not None:
        status = "shadow_active"
    else:
        status = "quarantined"
    proof_payload = {
        "proposal": dict(proposal),
        "multiplier": str(multiplier) if multiplier is not None else None,
        "reasons": reasons,
        "status": status,
    }
    return UnitRuleProof(
        valid=valid,
        status=status,
        multiplier=multiplier,
        reason_codes=tuple(reasons),
        proof_hash=_stable_hash(proof_payload),
    )


def unit_proposal_response_schema() -> dict[str, Any]:
    """Closed data-only schema for the optional unit-proposal LLM profile."""

    return {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(
            UNIT_PROPOSAL_KEYS - {"numerator", "denominator", "semantic_summary_zh"}
        ),
        "properties": {
            "source_unit": {"type": "string", "minLength": 1},
            "normalized_lexeme": {"type": "string", "minLength": 1},
            "dimension": {"type": "string", "minLength": 1},
            "canonical_unit": {"type": "string", "minLength": 1},
            "numerator": {"type": "array", "items": {"type": "string"}},
            "denominator": {"type": "array", "items": {"type": "string"}},
            "primitive_rule_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "factors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["primitive_rule_id", "exponent"],
                    "properties": {
                        "primitive_rule_id": {"type": "string"},
                        "exponent": {"enum": [-1, 1]},
                    },
                },
            },
            "transformation_type": {"const": "linear_multiplier"},
            "round_trip_vectors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["source", "canonical"],
                    "properties": {
                        "source": {"type": "string"},
                        "canonical": {"type": "string"},
                    },
                },
            },
            "semantic_summary_zh": {"type": "string"},
        },
    }


async def propose_unknown_unit(
    llm_client: Any,
    *,
    source_unit: str,
    context_zh: str,
    primitive_multipliers: Mapping[str, Decimal],
    profile: str = "semantic_extraction",
) -> Mapping[str, Any]:
    """Ask for data-only decomposition; proof and writes remain local authority."""

    bounded_context = str(context_zh or "")[:1200]
    request_payload = {
        "source_unit": str(source_unit),
        "context_zh": bounded_context,
        "governed_primitives": [
            {"rule_id": key, "multiplier": str(value)}
            for key, value in sorted(primitive_multipliers.items())
        ][:300],
    }
    response = await llm_client.complete(
        LlmRequest(
            profile=profile,
            messages=(
                LlmMessage(
                    role="system",
                    is_safety_instruction=True,
                    content=(
                        "你只负责把未知原始单位拆解为给定基础规则的候选公式，使用简体中文说明。"
                        "不得换算任何公司数值，不得引入汇率、上下文公式、非线性公式、新维度，"
                        "不得批准规则或修改目录。只返回闭合 JSON。"
                    ),
                ),
                LlmMessage(role="user", content=_json(request_payload)),
            ),
            response_schema=unit_proposal_response_schema(),
            schema_name="business_profile_unit_proposal_v1",
            schema_version="business_profile_unit_proposal.v1",
            temperature=0,
            metadata={
                "workload": "business_profile_unit_proposal",
                "stage": "unit_proposal",
                "bulk": False,
            },
            content_is_untrusted=True,
        )
    )
    return validate_unit_proposal(response.data)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()
