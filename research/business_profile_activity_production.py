"""Governed activity, relationship, and value-chain role production."""

from __future__ import annotations

import json
import hashlib
import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

ACTIVITY_PRODUCTION_SCHEMA_VERSION = "business_profile_activity_production.v1"
ROLE_RULE_VERSION = "business_profile_activity_role_rules.v1"
ENTITY_RESOLUTION_POLICY_VERSION = "business_profile_entity_resolution.v2"

ACTIVITY_ROLE_RULES = {
    "extracts": "producer",
    "cultivates": "producer",
    "produces": "producer",
    "transports": "logistics_provider",
    "stores": "storage_provider",
    "trades": "trader",
}

RELATIONSHIP_DIRECTIONS = {
    "sells_to": "outbound",
    "provides_service_to": "outbound",
    "buys_from": "inbound",
    "receives_service_from": "inbound",
}

RELATIONSHIP_IDENTITY_RESOLVED = "resolved_entity"
RELATIONSHIP_IDENTITY_DISCLOSED = "disclosed_name_only"
_LEGACY_RELATIONSHIP_IDENTITY_STATUSES = {
    "resolved": RELATIONSHIP_IDENTITY_RESOLVED,
    RELATIONSHIP_IDENTITY_RESOLVED: RELATIONSHIP_IDENTITY_RESOLVED,
    "unresolved": RELATIONSHIP_IDENTITY_DISCLOSED,
    RELATIONSHIP_IDENTITY_DISCLOSED: RELATIONSHIP_IDENTITY_DISCLOSED,
}

_ANONYMOUS_CONCENTRATION_LABEL_ALIASES = {
    "前五大客户": "top_five_customers",
    "前五名客户": "top_five_customers",
    "前五客户": "top_five_customers",
    "前五大供应商": "top_five_suppliers",
    "前五名供应商": "top_five_suppliers",
    "前五供应商": "top_five_suppliers",
}

_ANONYMOUS_CONCENTRATION_OBJECT_ALIASES = {
    "收入": "revenue",
    "营业收入": "revenue",
    "销售收入": "revenue",
    "采购": "procurement",
    "采购额": "procurement",
    "采购总额": "procurement",
}


@dataclass(frozen=True)
class EntityResolution:
    status: str
    entity_id: str | None
    normalized_name: str | None
    basis: str | None
    candidate_entity_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "entity_id": self.entity_id,
            "normalized_name": self.normalized_name,
            "basis": self.basis,
            "candidate_entity_ids": list(self.candidate_entity_ids),
            "policy_version": ENTITY_RESOLUTION_POLICY_VERSION,
        }


def canonical_relationship_identity_status(
    metadata: Mapping[str, Any] | None,
) -> str | None:
    """Normalize legacy relationship statuses at one read boundary.

    ``None`` denotes an unknown or internally conflicting legacy row.  New
    writers always emit the two canonical values and therefore never need a
    best-effort choice here.
    """

    values: set[str] = set()
    for key in ("identity_status", "resolution_status"):
        raw = str((metadata or {}).get(key) or "").strip().lower()
        if not raw:
            continue
        normalized = _LEGACY_RELATIONSHIP_IDENTITY_STATUSES.get(raw)
        if normalized is None:
            return None
        values.add(normalized)
    if len(values) > 1:
        return None
    return next(iter(values), RELATIONSHIP_IDENTITY_DISCLOSED)


class GovernedCounterpartyResolver:
    """Resolve only unique exact identifiers, legal names, or approved aliases."""

    def __init__(
        self,
        *,
        entities: Sequence[Mapping[str, Any]],
        aliases: Sequence[Mapping[str, Any]] = (),
    ):
        self.entities = tuple(dict(item) for item in entities)
        self.aliases = tuple(dict(item) for item in aliases)

    def resolve(
        self,
        raw_name: str,
        *,
        official_identifier: str | None = None,
        knowledge_cutoff: str | None = None,
    ) -> EntityResolution:
        name = str(raw_name or "").strip()
        identifier = str(official_identifier or "").strip()
        if identifier:
            matches = [
                item
                for item in self.entities
                if str(item.get("official_identifier") or "").strip() == identifier
                and _date_eligible(item, knowledge_cutoff)
            ]
            return _resolution(matches, "official_identifier")
        legal_matches = [
            item
            for item in self.entities
            if str(item.get("legal_name") or "").strip() == name
            and _date_eligible(item, knowledge_cutoff)
        ]
        if legal_matches:
            return _resolution(legal_matches, "exact_legal_name")
        alias_entity_ids = {
            str(item.get("entity_id") or "").strip()
            for item in self.aliases
            if str(item.get("alias") or "").strip() == name
            and item.get("review_status") == "approved"
            and _date_eligible(item, knowledge_cutoff)
        }
        alias_matches = [
            item
            for item in self.entities
            if str(item.get("entity_id") or "").strip() in alias_entity_ids
            and _date_eligible(item, knowledge_cutoff)
        ]
        if alias_matches:
            return _resolution(alias_matches, "approved_exact_alias")
        # A legal-looking string is evidence, not a registry identity.  Do not
        # synthesize a local entity id: without a unique master-data match the
        # relationship must remain unresolved for review.
        return EntityResolution("unresolved", None, None, None)


class BusinessProfileActivityProducer:
    """Map validated atomic assertions to governed candidates and derived roles."""

    def __init__(self, repository: Any):
        self.repository = repository

    def build_activity_candidate(
        self,
        assertion: Mapping[str, Any],
        *,
        evidence_id: str,
        run_id: str,
        data_available_date: str,
        extraction_method: str,
    ) -> dict[str, Any]:
        action = _required_choice(
            assertion,
            "action",
            set(ACTIVITY_ROLE_RULES)
            | {"processes", "purchases", "consumes", "sells", "hedges"},
        )
        instrument_id = _required_text(assertion, "instrument_id")
        report_period = _required_text(assertion, "report_period")
        object_raw = _required_text(assertion, "object_raw")
        subject_scope = _required_choice(
            assertion,
            "subject_scope",
            {"issuer", "consolidated_group", "named_subsidiary"},
        )
        if (
            subject_scope == "named_subsidiary"
            and assertion.get("issuer_scope_resolved") is not True
        ):
            raise ValueError("named subsidiary issuer scope is unresolved")
        stable_payload = {
            "instrument_id": instrument_id,
            "report_period": report_period,
            "subject_scope": subject_scope,
            "action": action,
            "object_type": _required_text(assertion, "object_type"),
            "object_raw": object_raw,
            "object_id": assertion.get("object_id"),
            "segment_id": assertion.get("segment_id"),
            "source_row_key": assertion.get("source_row_key"),
            "contract_reference_raw": assertion.get("contract_reference_raw"),
            "evidence_id": evidence_id,
        }
        activity_id = _stable_id("activity", stable_payload)
        return {
            "activity_id": activity_id,
            "instrument_id": instrument_id,
            "report_period": report_period,
            "subject_scope": subject_scope,
            "action": action,
            "object_type": stable_payload["object_type"],
            "object_raw": object_raw,
            "object_id": stable_payload["object_id"],
            "segment_id": stable_payload["segment_id"],
            "evidence_id": evidence_id,
            "run_id": run_id,
            "geography": assertion.get("geography"),
            "value": assertion.get("value"),
            "unit": assertion.get("unit"),
            "share": assertion.get("share"),
            "data_available_date": data_available_date,
            "extraction_method": extraction_method,
            "confidence": float(assertion.get("confidence") or 0.0),
            "review_status": "candidate",
            "valid_from": report_period,
            "valid_to": None,
            "business_regime_id": assertion.get("business_regime_id"),
            "knowledge_from": data_available_date,
            "knowledge_to": None,
            "version": 1,
            "metadata": {
                "schema_version": ACTIVITY_PRODUCTION_SCHEMA_VERSION,
                "semantic_verification_id": assertion.get("verification_id"),
                "source_row_key": assertion.get("source_row_key"),
                "contract_reference_raw": assertion.get("contract_reference_raw"),
            },
        }

    def build_relationship_or_concentration_candidate(
        self,
        assertion: Mapping[str, Any],
        *,
        resolution: EntityResolution,
        evidence_id: str,
        run_id: str,
        data_available_date: str,
    ) -> tuple[str, dict[str, Any]]:
        relationship_type = _required_choice(
            assertion, "relationship_type", set(RELATIONSHIP_DIRECTIONS)
        )
        instrument_id = _required_text(assertion, "instrument_id")
        report_period = _required_text(assertion, "report_period")
        anonymous_label = str(assertion.get("counterparty_name_raw") or "").strip()
        anonymous = (
            assertion.get("anonymous") is True
            or _is_anonymous_concentration_label(anonymous_label)
        )
        if anonymous:
            share = assertion.get("disclosed_share")
            if share is None:
                raise ValueError("anonymous relationship requires disclosed_share")
            normalized_share, share_rule = _normalize_disclosed_share(
                share, assertion.get("disclosed_share_unit")
            )
            anonymous_label = _required_text(assertion, "counterparty_name_raw")
            _validate_anonymous_direction(anonymous_label, relationship_type)
            object_raw = str(assertion.get("object_raw") or "").strip() or None
            anonymous_label_key = _anonymous_concentration_key(
                anonymous_label, _ANONYMOUS_CONCENTRATION_LABEL_ALIASES
            )
            object_key = _anonymous_concentration_key(
                object_raw, _ANONYMOUS_CONCENTRATION_OBJECT_ALIASES
            )
            fact_scope = _stable_id(
                "anonymous-concentration-scope",
                {
                    "scope_id": assertion.get("scope_id"),
                    "anonymous_label_key": anonymous_label_key,
                    "object_key": object_key,
                },
            )
            record = {
                "record_id": _stable_id(
                    "anonymous-concentration",
                    {
                        "instrument_id": instrument_id,
                        "report_period": report_period,
                        "relationship_type": relationship_type,
                        "fact_scope": fact_scope,
                        "evidence_id": evidence_id,
                    },
                ),
                "instrument_id": instrument_id,
                "report_period": report_period,
                "segment_id": assertion.get("segment_id"),
                "project_id": None,
                "fact_type": (
                    "customer_concentration_share"
                    if RELATIONSHIP_DIRECTIONS[relationship_type] == "outbound"
                    else "supplier_concentration_share"
                ),
                "value_raw": normalized_share,
                "unit_raw": "fraction",
                "value_normalized": normalized_share,
                "unit_normalized": "fraction",
                "fact_scope": fact_scope,
                "currency": None,
                "equity_basis": "consolidated_100_percent",
                "evidence_id": evidence_id,
                "data_available_date": data_available_date,
                "confidence": float(assertion.get("confidence") or 0.0),
                "review_status": "candidate",
                "knowledge_from": data_available_date,
                "version": 1,
                "metadata": {
                    "run_id": run_id,
                    "scope_id": assertion.get("scope_id"),
                    "anonymous_label": anonymous_label,
                    "anonymous_label_key": anonymous_label_key,
                    "object_raw": object_raw,
                    "object_key": object_key,
                    "no_relationship_edge_created": True,
                    "numeric_reconciliation": {
                        "schema_version": "business_profile_ratio_validation.v1",
                        "status": "passed",
                        "source_value": str(share),
                        "source_unit": assertion.get("disclosed_share_unit") or "fraction",
                        "normalized_value": str(normalized_share),
                        "normalized_unit": "fraction",
                        "rule": share_rule,
                    },
                    "numeric_reconciliation_status": "passed",
                    "numeric_reconciliation_executed": True,
                    "numeric_reconciliation_valid": True,
                },
            }
            return "operating_facts", record
        raw_name = _required_text(assertion, "counterparty_name_raw")
        valid_from = assertion.get("valid_from") or report_period
        normalized_share, share_rule = _normalize_disclosed_share(
            assertion.get("disclosed_share"), assertion.get("disclosed_share_unit")
        )
        contract_reference_raw = str(
            assertion.get("contract_reference_raw") or ""
        ).strip() or None
        relationship_occurrence_key = _stable_id(
            "relationship-occurrence",
            {
                "contract_reference_raw": contract_reference_raw,
                "relationship_type": relationship_type,
                "counterparty_name_raw": raw_name,
                "scope_id": assertion.get("scope_id"),
                "object_raw": assertion.get("object_raw"),
                "object_id": assertion.get("object_id"),
                "disclosed_value": assertion.get("disclosed_value"),
                "disclosed_unit": assertion.get("disclosed_unit"),
                "disclosed_share": normalized_share,
            },
        )
        relationship_id = _stable_id(
            "relationship",
            {
                "instrument_id": instrument_id,
                "relationship_type": relationship_type,
                "counterparty_entity_id": resolution.entity_id,
                "counterparty_name_raw": raw_name,
                "scope_id": assertion.get("scope_id"),
                "object_raw": assertion.get("object_raw"),
                "object_id": assertion.get("object_id"),
                "source_row_key": assertion.get("source_row_key"),
                "contract_reference_raw": contract_reference_raw,
                "evidence_id": evidence_id,
            },
        )
        return "relationships", {
            "relationship_id": relationship_id,
            "instrument_id": instrument_id,
            "report_period": report_period,
            "relationship_type": relationship_type,
            "direction": RELATIONSHIP_DIRECTIONS[relationship_type],
            "counterparty_name_raw": raw_name,
            "counterparty_name_normalized": resolution.normalized_name,
            "counterparty_entity_id": resolution.entity_id,
            "resolution_basis": resolution.basis,
            "anonymous": 0,
            "scope_type": str(assertion.get("scope_type") or "company"),
            "scope_id": str(assertion.get("scope_id") or instrument_id),
            "object_raw": assertion.get("object_raw"),
            "object_id": assertion.get("object_id"),
            "disclosed_value": assertion.get("disclosed_value"),
            "disclosed_unit": assertion.get("disclosed_unit"),
            "disclosed_share": normalized_share,
            "evidence_id": evidence_id,
            "run_id": run_id,
            "data_available_date": data_available_date,
            "confidence": float(assertion.get("confidence") or 0.0),
            "review_status": "candidate",
            "valid_from": valid_from,
            "valid_to": assertion.get("valid_to"),
            "business_regime_id": assertion.get("business_regime_id"),
            "knowledge_from": data_available_date,
            "knowledge_to": None,
            "version": 1,
            "metadata": {
                "entity_resolution": resolution.to_dict(),
                "resolution_status": (
                    RELATIONSHIP_IDENTITY_RESOLVED
                    if resolution.status == "resolved" and resolution.entity_id
                    else RELATIONSHIP_IDENTITY_DISCLOSED
                ),
                "counterparty_catalog_pending": False,
                "identity_status": (
                    RELATIONSHIP_IDENTITY_RESOLVED
                    if resolution.status == "resolved" and resolution.entity_id
                    else RELATIONSHIP_IDENTITY_DISCLOSED
                ),
                "schema_version": ACTIVITY_PRODUCTION_SCHEMA_VERSION,
                "source_row_key": assertion.get("source_row_key"),
                "contract_reference_raw": contract_reference_raw,
                "relationship_occurrence_key": relationship_occurrence_key,
                "numeric_reconciliation": {
                    "schema_version": "business_profile_ratio_validation.v1",
                    "status": "passed" if normalized_share is not None else "not_applicable",
                    "source_value": None if assertion.get("disclosed_share") is None else str(assertion.get("disclosed_share")),
                    "source_unit": assertion.get("disclosed_share_unit"),
                    "normalized_value": None if normalized_share is None else str(normalized_share),
                    "normalized_unit": "fraction" if normalized_share is not None else None,
                    "rule": share_rule,
                },
            },
        }

    def derive_role_candidates(
        self,
        activities: Iterable[Mapping[str, Any]],
        *,
        supporting_facts: Iterable[Mapping[str, Any]] = (),
    ) -> list[dict[str, Any]]:
        activity_rows = tuple(activities)
        activities_by_id, facts_by_id = _governed_support_indexes(
            activity_rows,
            supporting_facts,
        )
        output: list[dict[str, Any]] = []
        for activity in activity_rows:
            if activity.get("review_status") != "approved":
                continue
            action = str(activity.get("action") or "")
            transformation = _transformation_support(
                activity,
                activities_by_id=activities_by_id,
                facts_by_id=facts_by_id,
            )
            role = (
                "processor"
                if action == "processes" and transformation is not None
                else ACTIVITY_ROLE_RULES.get(action)
            )
            if role is None:
                continue
            activity_id = _required_text(activity, "activity_id")
            supporting_activity_ids = list(
                dict.fromkeys(
                    (
                        activity_id,
                        *(transformation[0] if transformation else ()),
                    )
                )
            )
            supporting_fact_ids = list(transformation[1] if transformation else ())
            record_id = _stable_id(
                "value-chain-role",
                {
                    "supporting_activity_ids": supporting_activity_ids,
                    "supporting_fact_ids": supporting_fact_ids,
                    "role": role,
                    "segment_id": activity.get("segment_id"),
                    "rule_version": ROLE_RULE_VERSION,
                },
            )
            output.append(
                {
                    "record_id": record_id,
                    "instrument_id": activity["instrument_id"],
                    "report_period": activity["report_period"],
                    "segment_id": activity.get("segment_id"),
                    "role": role,
                    "materiality": None,
                    "revenue_share": None,
                    "mapping_basis": "approved_atomic_activity_rule",
                    "evidence_id": activity["evidence_id"],
                    "data_available_date": activity["data_available_date"],
                    "confidence": activity["confidence"],
                    "review_status": "candidate",
                    "valid_from": activity.get("valid_from"),
                    "valid_to": activity.get("valid_to"),
                    "business_regime_id": activity.get("business_regime_id"),
                    "knowledge_from": activity.get("knowledge_from"),
                    "knowledge_to": activity.get("knowledge_to"),
                    "version": 1,
                    "metadata": {
                        "supporting_activity_ids": supporting_activity_ids,
                        "supporting_fact_ids": supporting_fact_ids,
                        "role_rule_version": ROLE_RULE_VERSION,
                        "valuation_effects": {},
                    },
                }
            )
        return output

    @staticmethod
    def role_derivation_gap(
        activity: Mapping[str, Any],
        *,
        activities: Iterable[Mapping[str, Any]] = (),
        supporting_facts: Iterable[Mapping[str, Any]] = (),
    ) -> str | None:
        activity_rows = list(activities)
        activity_id = str(activity.get("activity_id") or "")
        if activity_id and all(
            str(item.get("activity_id") or "") != activity_id for item in activity_rows
        ):
            activity_rows.append(activity)
        return BusinessProfileActivityProducer.role_derivation_gaps(
            activity_rows,
            supporting_facts=supporting_facts,
        ).get(activity_id)

    @staticmethod
    def role_derivation_gaps(
        activities: Iterable[Mapping[str, Any]],
        *,
        supporting_facts: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, str]:
        activity_rows = tuple(activities)
        activities_by_id, facts_by_id = _governed_support_indexes(
            activity_rows,
            supporting_facts,
        )
        gaps: dict[str, str] = {}
        for activity in activity_rows:
            activity_id = str(activity.get("activity_id") or "")
            if not activity_id:
                continue
            if (
                activity.get("review_status") == "approved"
                and str(activity.get("action") or "") == "processes"
                and _transformation_support(
                    activity,
                    activities_by_id=activities_by_id,
                    facts_by_id=facts_by_id,
                )
                is None
            ):
                gaps[activity_id] = "transformation_lineage_missing"
        return gaps


def classify_entity_resolution_exception(
    resolution: EntityResolution,
) -> dict[str, Any] | None:
    if resolution.status == "resolved":
        return None
    if len(resolution.candidate_entity_ids) > 1:
        return {
            "tier": "quick_review",
            "reason_code": "counterparty_exact_match_ambiguous",
            "ranked_local_choices": list(resolution.candidate_entity_ids),
        }
    # A named counterparty disclosed in the filing is useful evidence even
    # without a local listed-company entity.  It is reviewed as disclosed-name
    # evidence rather than sent back for an impossible catalog match.
    return None


def _normalize_disclosed_share(value: Any, unit: Any) -> tuple[float | None, str]:
    if value in (None, ""):
        return None, "not_applicable"
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("relationship disclosed_share is not numeric") from exc
    if not math.isfinite(numeric):
        raise ValueError("relationship disclosed_share must be finite")
    normalized_unit = str(unit or "").strip().lower()
    if normalized_unit in {"%", "percent", "percentage", "百分点"}:
        numeric /= 100.0
        rule = "percent_to_fraction"
    elif normalized_unit in {"fraction", "ratio", "", "decimal"}:
        rule = "finite_fraction_inclusive_range_0_1"
    else:
        raise ValueError("relationship disclosed_share unit is ambiguous")
    if not 0 <= numeric <= 1:
        raise ValueError("relationship disclosed_share is outside [0, 1]")
    return numeric, rule


def _validate_anonymous_direction(label: str, relationship_type: str) -> None:
    normalized = _anonymous_concentration_key(label, _ANONYMOUS_CONCENTRATION_LABEL_ALIASES)
    direction = RELATIONSHIP_DIRECTIONS[relationship_type]
    if normalized == "top_five_customers" and direction != "outbound":
        raise ValueError("anonymous concentration direction conflicts with customer label")
    if normalized == "top_five_suppliers" and direction != "inbound":
        raise ValueError("anonymous concentration direction conflicts with supplier label")


def _resolution(matches: Sequence[Mapping[str, Any]], basis: str) -> EntityResolution:
    entity_ids = tuple(
        sorted(
            {
                str(item.get("entity_id") or "").strip()
                for item in matches
                if item.get("entity_id")
            }
        )
    )
    if len(entity_ids) != 1:
        return EntityResolution(
            "ambiguous" if entity_ids else "unresolved",
            None,
            None,
            None,
            entity_ids,
        )
    entity = next(
        item for item in matches if str(item.get("entity_id")) == entity_ids[0]
    )
    return EntityResolution(
        "resolved",
        entity_ids[0],
        str(entity.get("legal_name") or "").strip() or None,
        basis,
        entity_ids,
    )


def _date_eligible(value: Mapping[str, Any], cutoff: str | None) -> bool:
    if not cutoff:
        return True
    start = str(value.get("valid_from") or "")[:10]
    end = str(value.get("valid_to") or "")[:10]
    return (not start or start <= cutoff) and (not end or cutoff < end)


def _transformation_support(
    activity: Mapping[str, Any],
    *,
    activities_by_id: Mapping[str, Mapping[str, Any]] | None = None,
    facts_by_id: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    metadata = activity.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    input_activity_ids = _metadata_id_tuple(
        metadata, "transformation_input_activity_ids"
    )
    output_activity_ids = _metadata_id_tuple(
        metadata, "transformation_output_activity_ids"
    )
    input_fact_ids = _metadata_id_tuple(metadata, "transformation_input_fact_ids")
    output_fact_ids = _metadata_id_tuple(metadata, "transformation_output_fact_ids")
    if not (input_activity_ids or input_fact_ids) or not (
        output_activity_ids or output_fact_ids
    ):
        return None
    if not _governed_links_match_scope(
        activity,
        activity_ids=(*input_activity_ids, *output_activity_ids),
        fact_ids=(*input_fact_ids, *output_fact_ids),
        activities_by_id=activities_by_id or {},
        facts_by_id=facts_by_id or {},
    ):
        return None
    return (
        tuple(dict.fromkeys((*input_activity_ids, *output_activity_ids))),
        tuple(dict.fromkeys((*input_fact_ids, *output_fact_ids))),
    )


def _governed_support_indexes(
    activities: Iterable[Mapping[str, Any]],
    supporting_facts: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    activities_by_id = {
        str(item.get("activity_id") or ""): item
        for item in activities
        if item.get("review_status") == "approved" and item.get("activity_id")
    }
    facts_by_id: dict[str, Mapping[str, Any]] = {}
    for item in supporting_facts:
        if item.get("review_status") != "approved":
            continue
        record_id = str(item.get("record_id") or item.get("fact_id") or "")
        if record_id:
            facts_by_id[record_id] = item
    return activities_by_id, facts_by_id


def _governed_links_match_scope(
    activity: Mapping[str, Any],
    *,
    activity_ids: Sequence[str],
    fact_ids: Sequence[str],
    activities_by_id: Mapping[str, Mapping[str, Any]],
    facts_by_id: Mapping[str, Mapping[str, Any]],
) -> bool:
    source_activity_id = str(activity.get("activity_id") or "")
    linked_rows: list[Mapping[str, Any]] = []
    for activity_id in activity_ids:
        if activity_id == source_activity_id or activity_id not in activities_by_id:
            return False
        linked_rows.append(activities_by_id[activity_id])
    for fact_id in fact_ids:
        if fact_id not in facts_by_id:
            return False
        linked_rows.append(facts_by_id[fact_id])
    return bool(linked_rows) and all(
        str(item.get("instrument_id") or "") == str(activity.get("instrument_id") or "")
        and str(item.get("report_period") or "")
        == str(activity.get("report_period") or "")
        and (item.get("segment_id") or None) == (activity.get("segment_id") or None)
        for item in linked_rows
    )


def _metadata_id_tuple(metadata: Mapping[str, Any], *keys: str) -> tuple[str, ...]:
    output: list[str] = []
    for key in keys:
        values = metadata.get(key)
        if not isinstance(values, Sequence) or isinstance(
            values, (str, bytes, bytearray)
        ):
            continue
        output.extend(str(item).strip() for item in values if str(item).strip())
    return tuple(dict.fromkeys(output))


def _required_text(value: Mapping[str, Any], key: str) -> str:
    text = str(value.get(key) or "").strip()
    if not text:
        raise ValueError(f"{key} is required")
    return text


def _required_choice(value: Mapping[str, Any], key: str, allowed: set[str]) -> str:
    text = _required_text(value, key)
    if text not in allowed:
        raise ValueError(f"unsupported {key}: {text}")
    return text


def _anonymous_concentration_key(
    value: Any,
    aliases: Mapping[str, str],
) -> str | None:
    normalized = "".join(
        character.lower()
        for character in str(value or "").strip()
        if character.isalnum()
    )
    if not normalized:
        return None
    return aliases.get(normalized, normalized)


def _is_anonymous_concentration_label(value: Any) -> bool:
    normalized = "".join(
        character.lower()
        for character in str(value or "").strip()
        if character.isalnum()
    )
    return bool(normalized and normalized in _ANONYMOUS_CONCENTRATION_LABEL_ALIASES)


def _stable_id(prefix: str, value: Any) -> str:
    digest = hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:{digest[:32]}"
