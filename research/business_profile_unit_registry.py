"""Persistent append-only governance for previously unknown business units."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

from research.business_profile_numeric_reconciliation import decimal_value
from research.business_profile_unit_conversions import (
    UnitResolution,
    governed_canonical_units,
    governed_primitive_definitions,
    load_unit_conversion_catalog,
    normalize_unit_lexeme,
    unit_magnitude_multiplier,
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
    "electric_charge",
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
        primitive_definitions: Optional[Mapping[str, Mapping[str, Any]]] = None,
        corroboration_observations: int = 3,
        corroboration_models: int = 2,
        corroboration_reconciliations: int = 2,
    ) -> None:
        self.storage = storage
        self.primitive_multipliers = {
            str(key): decimal_value(value, f"primitive:{key}")
            for key, value in dict(primitive_multipliers or {}).items()
        }
        defaults = governed_primitive_definitions()
        supplied_definitions = dict(primitive_definitions or {})
        self.primitive_definitions: dict[str, dict[str, Any]] = {}
        for key in self.primitive_multipliers:
            raw = supplied_definitions.get(key) or defaults.get(key)
            if not raw:
                continue
            self.primitive_definitions[key] = {
                "multiplier": decimal_value(
                    raw.get("multiplier"), f"primitive definition:{key}"
                ),
                "dimension": raw.get("dimension"),
                "canonical_unit": raw.get("canonical_unit"),
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
            primitive_definitions=self.proof_primitive_definitions(),
            canonical_units=governed_canonical_units(),
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
            # Observation must be recorded before replay; otherwise the newly
            # discovered artifact is absent from the replay set and remains
            # stuck in conversion_pending until a later unrelated run.
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

    def proof_primitive_definitions(self) -> dict[str, dict[str, Any]]:
        """Return dimensioned primitives plus committed runtime overlays."""

        definitions = {
            key: dict(value) for key, value in self.primitive_definitions.items()
        }
        for rule in self.overlay_rules():
            multiplier = rule.get("multiplier")
            if multiplier is None:
                continue
            definitions[str(rule["rule_id"])] = {
                "multiplier": decimal_value(
                    multiplier, f"runtime primitive:{rule['rule_id']}"
                ),
                "dimension": rule.get("dimension"),
                "canonical_unit": rule.get("canonical_unit"),
            }
        return definitions

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

    def get_rule_history(self, rule_id: str) -> list[dict[str, Any]]:
        """Return the append-only lifecycle history for an operator-visible rule."""

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_unit_rules WHERE rule_id = ? "
                "ORDER BY created_at, rowid",
                (rule_id,),
            ).fetchall()
        if not rows:
            raise ValueError(f"unknown unit rule: {rule_id}")
        return [self._decode_rule(dict(row)) for row in rows]

    def correct_rule(
        self,
        rule_id: str,
        *,
        dimension: str,
        canonical_unit: str,
        multiplier: Any,
        reason: str = "operator_correction",
    ) -> dict[str, Any]:
        """Append a governed operator correction and replay affected artifacts.

        This is the exceptional fallback for a wrong automated proposal. It accepts
        only an exact positive multiplier into an existing governed dimension; no
        formula or executable expression crosses the operator boundary.
        """

        current = self.get_rule(rule_id)
        normalized_dimension = str(dimension or "").strip()
        normalized_canonical = str(canonical_unit or "").strip()
        allowed_canonical = governed_canonical_units()
        if normalized_dimension not in GOVERNED_DIMENSIONS:
            raise ValueError(
                f"unit correction uses unknown dimension: {normalized_dimension}"
            )
        if allowed_canonical.get(normalized_dimension) != normalized_canonical:
            raise ValueError(
                "unit correction canonical unit does not match governed dimension: "
                f"{normalized_dimension}->{normalized_canonical}"
            )
        normalized_multiplier = decimal_value(multiplier, "unit correction multiplier")
        if normalized_multiplier <= 0:
            raise ValueError("unit correction multiplier must be positive")
        multiplier_text = _canonical_decimal_text(normalized_multiplier)
        correction_reason = str(reason or "operator_correction").strip()[:240]
        source_unit = str(current.get("source_unit") or "").strip()
        normalized_lexeme = normalize_unit_lexeme(source_unit)
        replacement_rule_id = "bp-unit-rule-" + _stable_hash(
            {
                "normalized_lexeme": normalized_lexeme,
                "dimension": normalized_dimension,
                "canonical_unit": normalized_canonical,
                "multiplier": multiplier_text,
                "authority": "operator_correction",
            }
        )[:20]
        try:
            existing = self.get_rule(replacement_rule_id)
        except ValueError:
            existing = None
        if existing is not None and existing.get("status") not in {
            "proposed",
            "auto_approved",
        }:
            raise ValueError(
                "operator correction replacement is not resumable: "
                f"{replacement_rule_id}:{existing.get('status')}"
            )

        proposal = {
            "source_unit": source_unit,
            "normalized_lexeme": normalized_lexeme,
            "dimension": normalized_dimension,
            "canonical_unit": normalized_canonical,
            "numerator": [],
            "denominator": [],
            "primitive_rule_ids": [],
            "factors": [],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [
                {"source": "1", "canonical": multiplier_text}
            ],
            "semantic_summary_zh": f"人工纠正规则：{correction_reason}",
        }
        proposal_input_hash = _stable_hash(
            {
                "replaces": rule_id,
                "proposal": proposal,
                "reason": correction_reason,
            }
        )
        replacement_proof = UnitRuleProof(
            valid=True,
            status="auto_approved",
            multiplier=normalized_multiplier,
            reason_codes=("operator_governed_correction",),
            proof_hash=_stable_hash(
                {
                    "rule_id": replacement_rule_id,
                    "proposal": proposal,
                    "authority": "operator_correction",
                }
            ),
        )
        if existing is None:
            self._append_rule_event(
                replacement_rule_id,
                "proposed",
                proposal,
                replacement_proof,
                proposal_input_hash=proposal_input_hash,
            )

        old_proof = UnitRuleProof(
            valid=False,
            status="superseded",
            multiplier=(
                decimal_value(current["multiplier"], "superseded multiplier")
                if current.get("multiplier") is not None
                else None
            ),
            reason_codes=(
                f"superseded_by:{replacement_rule_id}",
                "operator_governed_correction",
            ),
            proof_hash=_stable_hash(
                {"old": rule_id, "replacement": replacement_rule_id}
            ),
        )
        old_already_superseded = current.get("status") == "superseded"
        if old_already_superseded:
            if current.get("supersedes_rule_id") != replacement_rule_id:
                raise ValueError(
                    "unit rule was superseded by a different replacement: "
                    f"{current.get('supersedes_rule_id')}"
                )
        else:
            self._append_rule_event(
                rule_id,
                "superseded",
                dict(current.get("proposal") or {}),
                old_proof,
                proposal_input_hash=str(current["proposal_input_hash"]),
                supersedes_rule_id=replacement_rule_id,
            )
        replacement_already_active = (
            existing is not None and existing.get("status") == "auto_approved"
        )
        if not replacement_already_active:
            catalog_version = self._commit_catalog_version(replacement_rule_id)
            self._append_rule_event(
                replacement_rule_id,
                "auto_approved",
                proposal,
                replacement_proof,
                proposal_input_hash=proposal_input_hash,
                catalog_version=catalog_version,
                supersedes_rule_id=rule_id,
            )
        completed_before_call = (
            old_already_superseded
            and replacement_already_active
            and self._notification_exists(replacement_rule_id, "auto_approved")
            and self._notification_exists(rule_id, "superseded")
        )
        replayed = 0
        if not completed_before_call:
            self._copy_observations(rule_id, replacement_rule_id)
            replayed = self._replay_affected_artifacts(
                replacement_rule_id,
                reason=f"unit_rule_operator_correction:{rule_id}",
            )
            self.queue_notification(replacement_rule_id, "auto_approved")
            self.queue_notification(rule_id, "superseded")
        return {
            "old_rule": self.get_rule(rule_id),
            "replacement_rule": self.get_rule(replacement_rule_id),
            "replayed_artifacts": replayed,
            "idempotent_reuse": completed_before_call,
        }

    def reconcile_deterministic_rules(self, *, limit: int = 100) -> dict[str, int]:
        """Replace quarantined rules that the current deterministic catalog resolves."""

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT r.* FROM business_profile_unit_rules r
                WHERE r.rowid = (
                    SELECT latest.rowid FROM business_profile_unit_rules latest
                    WHERE latest.rule_id = r.rule_id
                    ORDER BY latest.created_at DESC, latest.rowid DESC LIMIT 1
                )
                  AND r.status = 'quarantined'
                ORDER BY r.created_at, r.rowid LIMIT ?
                """,
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
        catalog = load_unit_conversion_catalog()
        report = {"scanned": len(rows), "resolved": 0, "superseded": 0, "replayed": 0}
        for raw in rows:
            current = self._decode_rule(dict(raw))
            resolution = catalog.resolve(
                str(current["source_unit"]),
                runtime_rules=self.overlay_rules(),
            )
            if not resolution.publishable or resolution.multiplier is None:
                continue
            replacement = self._register_deterministic_resolution(resolution)
            replacement_id = str(replacement["rule_id"])
            current_id = str(current["rule_id"])
            self._copy_observations(current_id, replacement_id)
            proof = UnitRuleProof(
                valid=False,
                status="superseded",
                multiplier=(
                    decimal_value(current["multiplier"], "superseded multiplier")
                    if current.get("multiplier") is not None
                    else None
                ),
                reason_codes=(f"superseded_by:{replacement_id}",),
                proof_hash=_stable_hash(
                    {"old": current_id, "replacement": replacement_id}
                ),
            )
            self._append_rule_event(
                current_id,
                "superseded",
                dict(current.get("proposal") or {}),
                proof,
                proposal_input_hash=str(current["proposal_input_hash"]),
                supersedes_rule_id=replacement_id,
            )
            report["replayed"] += self._replay_affected_artifacts(
                replacement_id,
                reason=f"unit_rule_superseded:{current_id}",
            )
            self.queue_notification(current_id, "superseded")
            report["resolved"] += 1
            report["superseded"] += 1
        return report

    def _register_deterministic_resolution(
        self, resolution: UnitResolution
    ) -> dict[str, Any]:
        proposal = {
            "source_unit": resolution.source_unit,
            "normalized_lexeme": resolution.normalized_lexeme,
            "dimension": resolution.dimension,
            "canonical_unit": resolution.canonical_unit,
            "numerator": list(resolution.numerator),
            "denominator": list(resolution.denominator),
            "primitive_rule_ids": list(resolution.rule_ids),
            "factors": [],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [
                {"source": "1", "canonical": str(resolution.multiplier)}
            ],
            "semantic_summary_zh": "当前版本单位目录已完成确定性解析",
        }
        rule_id = "bp-unit-rule-" + _stable_hash(
            {
                "normalized_lexeme": resolution.normalized_lexeme,
                "dimension": resolution.dimension,
                "canonical_unit": resolution.canonical_unit,
                "authority": "deterministic_catalog",
            }
        )[:20]
        try:
            existing = self.get_rule(rule_id)
        except ValueError:
            existing = None
        if existing is not None and existing.get("status") == "auto_approved":
            return existing
        proof = UnitRuleProof(
            valid=True,
            status="auto_approved",
            multiplier=resolution.multiplier,
            reason_codes=("deterministic_catalog_resolution",),
            proof_hash=_stable_hash(
                {
                    "rule_id": rule_id,
                    "catalog_version": resolution.catalog_version,
                    "multiplier": str(resolution.multiplier),
                }
            ),
        )
        proposal_input_hash = _stable_hash(
            {
                "source_unit": resolution.source_unit,
                "catalog_version": resolution.catalog_version,
            }
        )
        self._append_rule_event(
            rule_id,
            "proposed",
            proposal,
            proof,
            proposal_input_hash=proposal_input_hash,
        )
        catalog_version = self._commit_catalog_version(rule_id)
        self._append_rule_event(
            rule_id,
            "auto_approved",
            proposal,
            proof,
            proposal_input_hash=proposal_input_hash,
            catalog_version=catalog_version,
        )
        self.queue_notification(rule_id, "auto_approved")
        return self.get_rule(rule_id)

    def _copy_observations(self, source_rule_id: str, target_rule_id: str) -> None:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_unit_rule_observations "
                "WHERE rule_id = ? ORDER BY created_at",
                (source_rule_id,),
            ).fetchall()
        for raw in rows:
            row = dict(raw)
            self.observe(
                target_rule_id,
                artifact_id=row.get("artifact_id"),
                source_document_id=row.get("source_document_id"),
                context_hash=str(row["context_hash"]),
                model_identity=row.get("model_identity"),
                reconciliation_status=row.get("reconciliation_status"),
            )

    def queue_notification(
        self, rule_id: str, lifecycle_status: str, *, impact_window: Optional[str] = None
    ) -> str:
        window = impact_window or get_shanghai_time().date().isoformat()
        rule = self.get_rule(rule_id)
        payload = {
            "normalized_lexeme": rule["normalized_lexeme"],
            "status": lifecycle_status,
            "window": window,
        }
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

    def _notification_exists(self, rule_id: str, lifecycle_status: str) -> bool:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT 1 FROM business_profile_unit_rule_notifications "
                "WHERE rule_id = ? AND lifecycle_status = ? LIMIT 1",
                (rule_id, lifecycle_status),
            ).fetchone()
        return row is not None

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
            impacts = self._rule_impacts(str(row["rule_id"]))
            proof_reasons = ",".join(
                str(value) for value in (rule.get("proof") or {}).get("reason_codes", [])
            ) or "none"
            effective = rule.get("status") == "auto_approved"
            message = (
                "[公司画像单位规则] "
                f"状态={row['lifecycle_status']} 单位={rule['source_unit']} "
                f"已生效={'是' if effective else '否'} "
                f"维度={rule.get('dimension')} 规范单位={rule.get('canonical_unit')} "
                f"倍率={rule.get('multiplier')} 原因={proof_reasons} "
                f"影响公司={','.join(impacts) or '暂无'} "
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

    def _rule_impacts(self, rule_id: str, *, limit: int = 8) -> list[str]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT DISTINCT a.instrument_id
                FROM business_profile_unit_rule_observations o
                JOIN business_profile_semantic_artifacts a
                  ON a.artifact_id = o.artifact_id
                WHERE o.rule_id = ? AND a.instrument_id IS NOT NULL
                ORDER BY a.instrument_id LIMIT ?
                """,
                (rule_id, max(1, min(int(limit), 50))),
            ).fetchall()
        return [str(row["instrument_id"]) for row in rows]

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
    primitive_definitions: Optional[Mapping[str, Mapping[str, Any]]] = None,
    canonical_units: Optional[Mapping[str, str]] = None,
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
    canonical = str(proposal.get("canonical_unit") or "")
    allowed_canonical = dict(canonical_units or governed_canonical_units())
    if dimension in allowed_canonical and canonical != allowed_canonical[dimension]:
        reasons.append("canonical_unit_dimension_mismatch")
    references = tuple(str(item) for item in proposal.get("primitive_rule_ids") or ())
    factors = tuple(proposal.get("factors") or ())
    factor_references = tuple(
        str(factor.get("primitive_rule_id") or "")
        for factor in factors
        if isinstance(factor, Mapping)
    )
    if set(references) != set(factor_references):
        reasons.append("primitive_factor_reference_mismatch")
    if len(factor_references) != len(set(factor_references)):
        reasons.append("duplicate_primitive_reference")
    round_trip_vectors = tuple(proposal.get("round_trip_vectors") or ())
    if target_rule_id in references:
        reasons.append("dependency_cycle")
    unknown_references = set(references) - set(primitive_multipliers) - existing_rule_ids
    if unknown_references:
        reasons.append("unknown_primitive_reference")
    definitions = dict(primitive_definitions or {})
    dimensionful_references = {
        str(definitions[reference].get("dimension"))
        for reference in references
        if reference in definitions and definitions[reference].get("dimension")
    }
    if dimensionful_references and dimensionful_references != {dimension}:
        reasons.append("primitive_dimension_mismatch")
    magnitude_references = {
        reference for reference in references if reference.startswith("magnitude:")
    }
    if magnitude_references:
        supplied_scale = Decimal("1")
        for reference in magnitude_references:
            definition = definitions.get(reference)
            if definition is not None:
                supplied_scale *= decimal_value(
                    definition.get("multiplier"),
                    f"magnitude primitive:{reference}",
                )
        if supplied_scale != unit_magnitude_multiplier(proposal.get("source_unit")):
            reasons.append("source_magnitude_mismatch")
    multiplier = Decimal("1")
    try:
        for factor in factors:
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


def unit_proposal_response_schema(
    *,
    dimensions: Optional[Sequence[str]] = None,
    canonical_units: Optional[Sequence[str]] = None,
    primitive_rule_ids: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
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
            "dimension": {
                "type": "string",
                "enum": sorted(set(dimensions or GOVERNED_DIMENSIONS)),
            },
            "canonical_unit": {
                "type": "string",
                "enum": sorted(
                    set(canonical_units or governed_canonical_units().values())
                ),
            },
            "numerator": {"type": "array", "items": {"type": "string"}},
            "denominator": {"type": "array", "items": {"type": "string"}},
            "primitive_rule_ids": {
                "type": "array",
                "items": {
                    "type": "string",
                    **(
                        {"enum": sorted(set(primitive_rule_ids))}
                        if primitive_rule_ids
                        else {}
                    ),
                },
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
    primitive_definitions: Optional[Mapping[str, Mapping[str, Any]]] = None,
    profile: str = "semantic_extraction",
) -> Mapping[str, Any]:
    """Ask for data-only decomposition; proof and writes remain local authority."""

    bounded_context = str(context_zh or "")[:1200]
    request_payload = {
        "source_unit": str(source_unit),
        "context_zh": bounded_context,
        "governed_primitives": [
            {
                "rule_id": key,
                "multiplier": str(value),
                **dict((primitive_definitions or {}).get(key) or {}),
            }
            for key, value in sorted(primitive_multipliers.items())
        ][:300],
        "governed_dimensions": sorted(GOVERNED_DIMENSIONS),
        "governed_canonical_units": governed_canonical_units(),
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
                        "dimension 和 canonical_unit 必须逐字选择给定治理集合中的值，"
                        "primitive_rule_ids 必须与 factors 中的引用完全一致。"
                        "round_trip_vectors 的 source 和 canonical 必须是纯数字字符串，不能包含单位文字。"
                        "可以把未知计数词作为既有 count/unit 的线性别名，但不得换算任何公司数值；"
                        "不得引入汇率、上下文公式、非线性公式或新维度，不得批准规则或修改目录。"
                        "只返回闭合 JSON。"
                    ),
                ),
                LlmMessage(role="user", content=_json(request_payload)),
            ),
            response_schema=unit_proposal_response_schema(
                dimensions=GOVERNED_DIMENSIONS,
                canonical_units=governed_canonical_units().values(),
                primitive_rule_ids=primitive_multipliers.keys(),
            ),
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


def _canonical_decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()
