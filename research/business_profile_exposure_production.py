"""Governed production of commodity exposure components and publications."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from research.business_profile_product_catalog import (
    BusinessProductCatalog,
    load_business_product_catalog,
)
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_promotion import (
    BusinessProfilePromotionService,
    FieldFamilyPromotionManifest,
    PromotionContext,
)
from research.business_profile_unit_conversions import load_unit_conversion_catalog
from research.business_profile_semantic_schemas import (
    validate_business_profile_artifact,
)


EXPOSURE_FACT_POLICY_VERSION = "business_profile_exposure_facts.v1"
DIRECTION_RULE_VERSION = "business_profile_exposure_direction.v1"
ASSUMPTION_POLICY_VERSION = "business_profile_exposure_assumptions.v1"
PUBLICATION_POLICY_VERSION = "business_profile_exposure_publication.v1"

_ACTION_ROLES = {
    "sells": ("revenue", "positive", "explicit_product"),
    "produces": ("revenue", "positive", "explicit_product"),
    "purchases": ("feedstock_cost", "negative", "explicit_raw_material"),
    "consumes": ("feedstock_cost", "negative", "explicit_raw_material"),
}
_ASSUMPTION_TYPES = {
    "lag_days": "lag_days",
    "pass_through": "pass_through_score",
    "pass_through_score": "pass_through_score",
    "hedge_effectiveness": "hedge_adjustment",
    "hedge_adjustment": "hedge_adjustment",
    "spread_parameter": None,
}
_ASSUMPTION_SOURCES = {
    "disclosed_formula",
    "deterministic_calculation",
    "calibrated",
    "operator_policy",
}


@dataclass(frozen=True)
class ResolvedCommodityMapping:
    mapping_id: str
    catalog_version: str
    product_id: str
    exposure_role: str
    commodity_id: str
    price_series_id: str | None
    reference_type: str | None


class BusinessProfileExposureFactProducer:
    """Convert approved atomic activities into evidence-preserving facts."""

    def __init__(self, repository: Any):
        self.repository = repository

    def build_from_activity(self, activity: Mapping[str, Any]) -> dict[str, Any]:
        if activity.get("review_status") != "approved":
            raise ValueError("commodity exposure facts require an approved activity")
        action = str(activity.get("action") or "").strip()
        if action not in {"sells", "produces", "purchases", "consumes", "hedges"}:
            raise ValueError(f"unsupported exposure activity action: {action}")
        evidence_id = _required_text(activity, "evidence_id")
        instrument_id = _required_text(activity, "instrument_id")
        report_period = _required_text(activity, "report_period")
        object_raw = _required_text(activity, "object_raw")
        value = activity.get("value")
        unit = str(activity.get("unit") or "").strip() or None
        catalog = load_unit_conversion_catalog()
        resolution = (
            catalog.resolve(unit) if value is not None and unit else None
        )
        fact_type = _fact_type(
            action,
            value=value,
            unit=unit,
            dimension=resolution.dimension if resolution else None,
        )
        normalized_value = None
        normalized_unit = None
        period_basis = _activity_period_basis(activity)
        conversion_completed = (
            value is not None
            and resolution is not None
            and resolution.publishable
            and period_basis in catalog.period_basis_values
        )
        if conversion_completed:
            converted = catalog.convert_resolved(
                value,
                resolution,
                period_basis=period_basis,
                equity_basis="unknown",
            )
            normalized_value = float(converted.normalized_value)
            normalized_unit = converted.normalized_unit
        stable_payload = {
            "activity_id": _required_text(activity, "activity_id"),
            "fact_type": fact_type,
            "value": value,
            "unit": unit,
            "share": activity.get("share"),
            "evidence_id": evidence_id,
            "activity_lineage_hash": activity.get("lineage_hash"),
            "policy_version": EXPOSURE_FACT_POLICY_VERSION,
        }
        fact_id = "exposure-fact-" + _stable_hash(stable_payload)[:24]
        return {
            "fact_id": fact_id,
            "instrument_id": instrument_id,
            "report_period": report_period,
            "activity_id": activity["activity_id"],
            "segment_id": activity.get("segment_id"),
            "exposure_fact_type": fact_type,
            "object_raw": object_raw,
            "product_id": activity.get("object_id"),
            "value_raw": value,
            "unit_raw": unit,
            "value_normalized": normalized_value,
            "unit_normalized": normalized_unit,
            "share": activity.get("share"),
            "fact_scope": "segment" if activity.get("segment_id") else "company",
            "evidence_id": evidence_id,
            "run_id": activity.get("run_id"),
            "data_available_date": activity.get("data_available_date"),
            "confidence": activity.get("confidence"),
            "review_status": "candidate",
            "valid_from": activity.get("valid_from"),
            "valid_to": activity.get("valid_to"),
            "business_regime_id": activity.get("business_regime_id"),
            "knowledge_from": activity.get("knowledge_from"),
            "knowledge_to": activity.get("knowledge_to"),
            "version": 1,
            "metadata": {
                "policy_version": EXPOSURE_FACT_POLICY_VERSION,
                "source_activity_action": action,
                "source_activity_lineage_hash": activity.get("lineage_hash"),
                "unknown_value_preserved": value is None,
                "unknown_unit_preserved": unit is None or not bool(resolution and resolution.publishable),
                "unit_resolution": resolution.to_dict() if resolution else None,
                "period_basis": period_basis,
                "period_basis_source": (
                    "activity" if period_basis != "unknown" else "unknown"
                ),
                "numeric_reconciliation_executed": value is None or conversion_completed,
                "numeric_reconciliation_valid": value is None or conversion_completed,
                "unit_normalization_status": (
                    resolution.status if resolution else ("not_applicable" if value is None else "unit_resolution_pending")
                ),
                "publication_blocker": (
                    "unit_normalization_failed"
                    if value is not None and not (resolution and resolution.publishable)
                    else "period_basis_unknown"
                    if value is not None and period_basis not in catalog.period_basis_values
                    else None
                ),
                "llm_assumption_fields_prohibited": True,
            },
        }

    def persist_from_activity_id(self, activity_id: str) -> dict[str, Any]:
        activity = _find_record(
            self.repository, "activities", "activity_id", activity_id
        )
        fact = self.build_from_activity(activity)
        self.repository.upsert("exposure_facts", fact)
        return _find_record(
            self.repository, "exposure_facts", "fact_id", fact["fact_id"]
        )


def _activity_period_basis(activity: Mapping[str, Any]) -> str:
    """Read the disclosed measurement basis without inventing annual semantics."""

    metadata = activity.get("metadata") or {}
    value = activity.get("period_basis") or metadata.get("period_basis")
    normalized = str(value or "").strip().lower()
    return normalized or "unknown"


class GovernedCommodityMappingResolver:
    """Resolve commodity identity separately from an executable price series."""

    def __init__(self, catalog: BusinessProductCatalog | None = None):
        self.catalog = catalog or load_business_product_catalog()

    def resolve(
        self,
        *,
        product_id: str,
        exposure_role: str,
        evidence_requirement: str,
        knowledge_cutoff: str,
    ) -> ResolvedCommodityMapping:
        if knowledge_cutoff < self.catalog.document_applicable_from or (
            self.catalog.document_applicable_to
            and knowledge_cutoff > self.catalog.document_applicable_to
        ):
            raise ValueError("stale_product_commodity_catalog")
        candidates = self.catalog.commodity_candidates(
            product_id,
            exposure_role=exposure_role,
            evidence_requirement=evidence_requirement,
        )
        commodity_ids = {item.commodity_id for item in candidates}
        if len(candidates) != 1 or len(commodity_ids) != 1:
            raise ValueError("ambiguous_or_unpromoted_product_commodity_mapping")
        mapping = candidates[0]
        executable = (
            not mapping.candidate_only
            and mapping.ambiguity_policy == "single_target"
            and len(mapping.targets) == 1
            and bool(mapping.targets[0].price_series_id)
        )
        target = mapping.targets[0] if executable else None
        return ResolvedCommodityMapping(
            mapping_id=mapping.mapping_id,
            catalog_version=self.catalog.catalog_version,
            product_id=product_id,
            exposure_role=exposure_role,
            commodity_id=mapping.commodity_id,
            price_series_id=str(target.price_series_id) if target else None,
            reference_type=target.reference_type if target else None,
        )


class BusinessProfileExposureAssumptionWriter:
    """Write governed assumption candidates without accepting model judgments."""

    def __init__(self, repository: Any):
        self.repository = repository

    def write(
        self,
        *,
        instrument_id: str,
        scope_type: str,
        scope_id: str,
        assumption_type: str,
        assumption_value: float,
        unit: str,
        method: str,
        source_kind: str,
        data_available_date: str,
        effective_from: str,
        sample_start: str | None = None,
        sample_end: str | None = None,
        evidence_id: str | None = None,
        run_id: str | None = None,
        confidence: float = 1.0,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_source = str(source_kind or "").strip()
        if normalized_source not in _ASSUMPTION_SOURCES:
            raise ValueError("unsupported commodity exposure assumption source")
        metadata_payload = dict(metadata or {})
        if _contains_model_origin(metadata_payload) or _contains_model_origin(
            {"method": method, "source_kind": normalized_source}
        ):
            raise ValueError("LLM output cannot populate exposure assumptions")
        if assumption_type not in _ASSUMPTION_TYPES:
            raise ValueError(f"unsupported exposure assumption_type: {assumption_type}")
        if normalized_source == "disclosed_formula" and not evidence_id:
            raise ValueError("disclosed formula assumptions require evidence_id")
        stable_payload = {
            "instrument_id": instrument_id,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "assumption_type": assumption_type,
            "value": float(assumption_value),
            "unit": unit,
            "method": method,
            "source_kind": normalized_source,
            "sample_start": sample_start,
            "sample_end": sample_end,
            "evidence_id": evidence_id,
            "policy_version": ASSUMPTION_POLICY_VERSION,
        }
        assumption_id = "exposure-assumption-" + _stable_hash(stable_payload)[:24]
        payload = {
            "assumption_id": assumption_id,
            "instrument_id": instrument_id,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "assumption_type": assumption_type,
            "assumption_value": float(assumption_value),
            "unit": unit,
            "method": method,
            "sample_start": sample_start,
            "sample_end": sample_end,
            "evidence_id": evidence_id,
            "run_id": run_id,
            "data_available_date": data_available_date,
            "confidence": float(confidence),
            "review_status": "candidate",
            "effective_from": effective_from,
            "knowledge_from": data_available_date,
            "version": 1,
            "metadata": {
                **metadata_payload,
                "source_kind": normalized_source,
                "policy_version": ASSUMPTION_POLICY_VERSION,
            },
        }
        self.repository.upsert("exposure_assumptions", payload)
        return _find_record(
            self.repository,
            "exposure_assumptions",
            "assumption_id",
            assumption_id,
        )


class BusinessProfileExposurePublisher:
    """Assemble compatible approved components into an audited publication."""

    def __init__(
        self,
        repository: Any,
        *,
        mapping_resolver: GovernedCommodityMappingResolver | None = None,
    ):
        self.repository = repository
        self.mapping_resolver = mapping_resolver or GovernedCommodityMappingResolver()
        self.review_service = BusinessProfileReviewService(repository)

    def publish_basic(
        self,
        *,
        fact_id: str,
        knowledge_cutoff: str,
        required_assumption_types: Sequence[str] = (),
        consumer_id: str | None = None,
        promotion_manifest: FieldFamilyPromotionManifest | None = None,
        promotion_gates: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if promotion_manifest is None:
            raise ValueError("commodity publication promotion manifest is required")
        if promotion_manifest.field_family != "commodity_exposure_publication":
            raise ValueError("commodity publication promotion manifest mismatch")
        if promotion_gates is None:
            raise ValueError("commodity publication promotion gates are required")
        fact = _find_approved_as_of(
            self.repository,
            "exposure_facts",
            "fact_id",
            fact_id,
            knowledge_cutoff,
        )
        if (fact.get("metadata") or {}).get("publication_blocker"):
            return {
                "status": "fact_only",
                "fact": fact,
                "reason": str((fact.get("metadata") or {}).get("publication_blocker")),
            }
        action = str((fact.get("metadata") or {}).get("source_activity_action") or "")
        if action == "hedges":
            return {"status": "fact_only", "fact": fact, "reason": "hedge_publication_rule_unsupported"}
        if action not in _ACTION_ROLES:
            return {"status": "fact_only", "fact": fact, "reason": "unknown_exposure_role"}
        exposure_role, direction, evidence_requirement = _ACTION_ROLES[action]
        product_id = str(fact.get("product_id") or "").strip()
        if not product_id:
            return {
                "status": "fact_only",
                "fact": fact,
                "reason": "commodity_identity_unresolved",
            }
        product = self.mapping_resolver.catalog.require_product(product_id)
        if action in {"purchases", "consumes"} and product.product_kind == "energy_input":
            exposure_role = "energy_cost"
        mapping = self.mapping_resolver.resolve(
            product_id=product_id,
            exposure_role=exposure_role,
            evidence_requirement=evidence_requirement,
            knowledge_cutoff=knowledge_cutoff,
        )
        if not mapping.price_series_id:
            return {
                "status": "fact_only",
                "fact": fact,
                "mapping": {
                    "mapping_id": mapping.mapping_id,
                    "commodity_id": mapping.commodity_id,
                    "catalog_version": mapping.catalog_version,
                },
                "reason": "market_series_unresolved",
            }
        assumptions = self._resolve_assumptions(
            instrument_id=fact["instrument_id"],
            scope_ids=(product_id, fact.get("segment_id"), fact["instrument_id"]),
            assumption_types=required_assumption_types,
            knowledge_cutoff=knowledge_cutoff,
        )
        normalized_consumer_id = str(consumer_id or "").strip() or None
        if assumptions and not normalized_consumer_id:
            raise ValueError(
                "consumer_id is required for assumption-bearing publication"
            )
        assumption_ids = [item["assumption_id"] for item in assumptions]
        assumption_values = {
            _ASSUMPTION_TYPES[item["assumption_type"]]: item["assumption_value"]
            for item in assumptions
            if _ASSUMPTION_TYPES[item["assumption_type"]] is not None
        }
        direction_rule_id = (
            f"{DIRECTION_RULE_VERSION}:{action}:{exposure_role}:{direction}"
        )
        component_lineage = {
            "fact_ids": [fact_id],
            "source_activity_action": action,
            "mapping_ids": [mapping.mapping_id],
            "assumption_ids": assumption_ids,
            "assumption_lineage_hashes": [
                item.get("lineage_hash") for item in assumptions
            ],
            "direction_rule_id": direction_rule_id,
            "fact_lineage_hash": fact.get("lineage_hash"),
            "catalog_version": mapping.catalog_version,
            "build_policy_version": PUBLICATION_POLICY_VERSION,
            "consumer_id": normalized_consumer_id,
        }
        component_lineage_hash = _stable_hash(component_lineage)
        exposure_id = "commodity-exposure-" + component_lineage_hash[:24]
        existing = _find_optional_record(
            self.repository, "exposures", "exposure_id", exposure_id
        )
        if existing and existing.get("review_status") == "approved":
            if not _publication_context_allows_approved_reuse(
                promotion_manifest, promotion_gates
            ):
                return {
                    "status": "held",
                    "exposure": existing,
                    "audit": None,
                    "reason": "publication_gates_failed",
                }
            return {"status": "unchanged", "exposure": existing, "audit": None}
        build_policy_hash = _stable_hash(
            {
                "policy_version": PUBLICATION_POLICY_VERSION,
                "direction_rule_version": DIRECTION_RULE_VERSION,
                "catalog_version": mapping.catalog_version,
                "consumer_id": normalized_consumer_id,
                "required_assumption_types": list(required_assumption_types),
            }
        )
        base_scope_type = fact["fact_scope"]
        base_scope_id = fact.get("segment_id") or fact["instrument_id"]
        publication_scope_type = "model_consumer" if assumptions else base_scope_type
        publication_scope_id = (
            f"{base_scope_type}:{base_scope_id}:consumer:{normalized_consumer_id}"
            if assumptions
            else base_scope_id
        )
        publication_artifact = {
            "schema_version": "business_profile_exposure_publication.v1",
            "exposure_id": exposure_id,
            "instrument_id": fact["instrument_id"],
            "fact_ids": [fact_id],
            "mapping_ids": [mapping.mapping_id],
            "assumption_ids": assumption_ids,
            "direction_rule_id": direction_rule_id,
            "build_policy_version": PUBLICATION_POLICY_VERSION,
            "build_policy_hash": build_policy_hash,
            "component_lineage_hash": component_lineage_hash,
            "effective_from": fact.get("valid_from"),
            "effective_to": fact.get("valid_to"),
            "knowledge_from": fact.get("knowledge_from") or fact["data_available_date"],
            "knowledge_to": fact.get("knowledge_to"),
        }
        validate_business_profile_artifact("exposure_publication", publication_artifact)
        payload = {
            "exposure_id": exposure_id,
            "instrument_id": fact["instrument_id"],
            "report_period": fact["report_period"],
            "scope_type": publication_scope_type,
            "scope_id": publication_scope_id,
            "commodity_id": mapping.commodity_id,
            "exposure_role": exposure_role,
            "direction": direction,
            "materiality": None,
            "mapping_basis": (
                f"product_catalog:{mapping.catalog_version}:{mapping.mapping_id}"
            ),
            "price_series_id": mapping.price_series_id,
            "spread_definition_id": None,
            "lag_days": assumption_values.get("lag_days"),
            "pass_through_score": assumption_values.get("pass_through_score"),
            "hedge_adjustment": assumption_values.get("hedge_adjustment"),
            "evidence_id": fact["evidence_id"],
            "data_available_date": fact["data_available_date"],
            "confidence": fact.get("confidence"),
            "review_status": "candidate",
            "effective_from": fact.get("valid_from"),
            "effective_to": fact.get("valid_to"),
            "business_regime_id": fact.get("business_regime_id"),
            "knowledge_from": publication_artifact["knowledge_from"],
            "knowledge_to": fact.get("knowledge_to"),
            "fact_ids": [fact_id],
            "mapping_ids": [mapping.mapping_id],
            "assumption_ids": assumption_ids,
            "direction_rule_id": direction_rule_id,
            "build_policy_version": PUBLICATION_POLICY_VERSION,
            "build_policy_hash": build_policy_hash,
            "component_lineage_hash": component_lineage_hash,
            "legacy_compatibility_status": "componentized",
            "metadata": {
                "mapping_reference_type": mapping.reference_type,
                "market_series_status": (
                    "resolved" if mapping.price_series_id else "unresolved"
                ),
                "unknown_materiality_preserved": fact.get("share") is None,
                "basic_publication_excludes_optional_assumptions": not assumption_ids,
                "required_assumption_types": list(required_assumption_types),
                "consumer_id": normalized_consumer_id,
            },
        }
        predecessor = self._find_predecessor(payload)
        payload["supersedes_exposure_id"] = (
            predecessor.get("exposure_id") if predecessor else None
        )
        payload["version"] = (
            int(predecessor.get("version") or 1) + 1 if predecessor else 1
        )
        self.repository.upsert("exposures", payload)
        current = _find_record(self.repository, "exposures", "exposure_id", exposure_id)
        identities = dict(promotion_manifest.identities)
        required_identities = {
            "catalog_version": mapping.catalog_version,
            "publication_policy": PUBLICATION_POLICY_VERSION,
        }
        if any(
            str(identities.get(key) or "") != value
            for key, value in required_identities.items()
            if key in identities
        ):
            raise ValueError("commodity publication manifest identity mismatch")
        gates = dict(promotion_gates)
        promotion = BusinessProfilePromotionService(self.review_service).process(
            PromotionContext(
                target_type="exposures",
                target_id=exposure_id,
                instrument_id=fact["instrument_id"],
                field_family="commodity_exposure_publication",
                expected_updated_at=current["updated_at"],
                gates=gates,
                runtime_identities=identities,
                evidence_references=tuple(
                    [fact["evidence_id"], fact_id, mapping.mapping_id, *assumption_ids]
                ),
                metadata={"component_lineage_hash": component_lineage_hash},
            ),
            promotion_manifest,
        )
        return {
            "status": "published" if promotion.get("promoted") else "held",
            "exposure": _find_record(
                self.repository, "exposures", "exposure_id", exposure_id
            ),
            "audit": promotion.get("audit"),
            "promotion": promotion.get("decision"),
        }

    def _resolve_assumptions(
        self,
        *,
        instrument_id: str,
        scope_ids: Sequence[Any],
        assumption_types: Sequence[str],
        knowledge_cutoff: str,
    ) -> list[dict[str, Any]]:
        canonical_by_requested = {
            str(item).strip(): _ASSUMPTION_TYPES.get(str(item).strip())
            for item in assumption_types
        }
        unknown = sorted(key for key, value in canonical_by_requested.items() if key and value is None and key not in {"spread_parameter"})
        if unknown:
            raise ValueError(f"unsupported required exposure assumptions: {unknown}")
        requested = tuple(dict.fromkeys(value for value in canonical_by_requested.values() if value))
        if not requested:
            return []
        valid_scopes = {
            str(item).strip() for item in scope_ids if str(item or "").strip()
        }
        approved = self.repository.get_approved_as_of(
            "exposure_assumptions",
            instrument_id=instrument_id,
            cutoff=knowledge_cutoff,
        )
        selected: list[dict[str, Any]] = []
        for assumption_type in requested:
            matches = [
                item
                for item in approved
                if _ASSUMPTION_TYPES.get(str(item.get("assumption_type") or "")) == assumption_type
                and str(item.get("scope_id") or "") in valid_scopes
            ]
            values = {float(item.get("assumption_value")) for item in matches}
            if len(matches) != 1 and len(values) != 1:
                raise ValueError(
                    "required approved exposure assumption is missing or ambiguous: "
                    f"{assumption_type}"
                )
            selected.append(sorted(matches, key=lambda item: str(item.get("updated_at") or ""))[-1])
        return selected

    def _find_predecessor(self, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        matches = [
            item
            for item in self.repository.list_records(
                "exposures",
                instrument_id=payload["instrument_id"],
                review_status="approved",
                limit=10000,
            )
            if item.get("scope_type") == payload.get("scope_type")
            and item.get("scope_id") == payload.get("scope_id")
            and item.get("commodity_id") == payload.get("commodity_id")
            and item.get("exposure_role") == payload.get("exposure_role")
            and str((item.get("metadata") or {}).get("source_activity_action") or "")
            == str((payload.get("metadata") or {}).get("source_activity_action") or "")
            and str((item.get("metadata") or {}).get("consumer_id") or "")
            == str((payload.get("metadata") or {}).get("consumer_id") or "")
            and item.get("exposure_id") != payload.get("exposure_id")
            and not item.get("knowledge_to")
        ]
        if not matches:
            return None
        return max(
            matches,
            key=lambda item: (
                int(item.get("version") or 0),
                str(item.get("knowledge_from") or ""),
                str(item.get("updated_at") or ""),
            ),
        )


def _fact_type(
    action: str,
    *,
    value: Any,
    unit: str | None,
    dimension: str | None = None,
) -> str:
    if action == "produces":
        if value is None:
            return "production_activity"
        if dimension == "currency":
            return "production_value"
        if dimension is None:
            return "production_activity"
        return "production_volume"
    if action == "hedges":
        if value is None:
            return "hedge_activity"
        return "hedge_notional"
    prefix = {"sells": "sales", "purchases": "purchase", "consumes": "consumption"}[
        action
    ]
    if value is None:
        return f"{prefix}_activity"
    if dimension is None:
        # Preserve the raw assertion as an activity until the unit catalog can
        # establish its dimension; never silently classify an unknown unit as volume.
        return f"{prefix}_activity"
    if dimension == "currency" or unit in {"CNY", "HKD", "USD"}:
        return f"{prefix}_value"
    return f"{prefix}_volume"


def _publication_context_allows_approved_reuse(
    manifest: FieldFamilyPromotionManifest,
    gates: Mapping[str, Any],
) -> bool:
    required = set(manifest.required_gates)
    return bool(
        manifest.enabled
        and manifest.benchmark_passed
        and set(gates) == required
        and all(gates.get(name) is True for name in required)
    )


def _find_approved_as_of(
    repository: Any,
    record_type: str,
    pk: str,
    value: str,
    cutoff: str,
) -> dict[str, Any]:
    raw = _find_record(repository, record_type, pk, value)
    eligible = repository.get_approved_as_of(
        record_type,
        instrument_id=raw["instrument_id"],
        cutoff=cutoff,
    )
    for item in eligible:
        if item.get(pk) == value:
            return item
    raise ValueError(f"approved component unavailable at cutoff: {record_type}:{value}")


def _find_record(
    repository: Any, record_type: str, pk: str, value: str
) -> dict[str, Any]:
    item = _find_optional_record(repository, record_type, pk, value)
    if item is None:
        raise ValueError(f"business profile record not found: {record_type}:{value}")
    return item


def _find_optional_record(
    repository: Any, record_type: str, pk: str, value: str
) -> dict[str, Any] | None:
    for item in repository.list_records(record_type, limit=10000):
        if str(item.get(pk) or "") == str(value):
            return item
    return None


def _required_text(value: Mapping[str, Any], key: str) -> str:
    output = str(value.get(key) or "").strip()
    if not output:
        raise ValueError(f"{key} is required")
    return output


def _contains_model_origin(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            _contains_model_origin(key) or _contains_model_origin(item)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_contains_model_origin(item) for item in value)
    normalized = str(value or "").strip().lower()
    return any(
        token in normalized for token in ("llm", "language_model", "model_generated")
    )


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
