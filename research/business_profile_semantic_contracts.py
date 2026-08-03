"""Versioned contracts shared by business-profile semantic production stages."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


BUSINESS_PROFILE_FIELD_FAMILY_SCHEMA_VERSION = "business_profile_field_families.v1"


class BusinessProfileFieldFamily(str, Enum):
    """Stable identifiers for independently promoted production field families."""

    STRUCTURED_SEGMENTS = "structured_segments"
    TABULAR_OPERATING_FACTS = "tabular_operating_facts"
    ATOMIC_ACTIVITIES = "atomic_activities"
    NAMED_RELATIONSHIPS = "named_relationships"
    DERIVED_VALUE_CHAIN_ROLES = "derived_value_chain_roles"
    COMMODITY_EXPOSURE_FACTS = "commodity_exposure_facts"
    COMMODITY_EXPOSURE_PUBLICATION = "commodity_exposure_publication"


@dataclass(frozen=True)
class BusinessProfileFieldFamilyDefinition:
    """Immutable ownership and verification contract for one field family."""

    field_family: BusinessProfileFieldFamily
    source_stage: str
    output_record_types: tuple[str, ...]
    verification_policy: str
    requires_official_evidence: bool
    llm_allowed: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["field_family"] = self.field_family.value
        payload["output_record_types"] = list(self.output_record_types)
        return payload


FIELD_FAMILY_DEFINITIONS: tuple[BusinessProfileFieldFamilyDefinition, ...] = (
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.STRUCTURED_SEGMENTS,
        source_stage="structured_or_official_table",
        output_record_types=("segments",),
        verification_policy="deterministic_reconciliation",
        requires_official_evidence=True,
        llm_allowed=False,
    ),
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.TABULAR_OPERATING_FACTS,
        source_stage="official_table",
        output_record_types=("operating_facts",),
        verification_policy="deterministic_reconciliation",
        requires_official_evidence=True,
        llm_allowed=False,
    ),
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.ATOMIC_ACTIVITIES,
        source_stage="official_semantic",
        output_record_types=("activities",),
        verification_policy="independent_semantic_verification",
        requires_official_evidence=True,
        llm_allowed=True,
    ),
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.NAMED_RELATIONSHIPS,
        source_stage="official_semantic",
        output_record_types=("relationships",),
        verification_policy="independent_semantic_and_entity_resolution",
        requires_official_evidence=True,
        llm_allowed=True,
    ),
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.DERIVED_VALUE_CHAIN_ROLES,
        source_stage="governed_derivation",
        output_record_types=("value_chain_roles",),
        verification_policy="deterministic_rule",
        requires_official_evidence=True,
        llm_allowed=False,
    ),
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.COMMODITY_EXPOSURE_FACTS,
        source_stage="governed_derivation",
        output_record_types=("exposure_facts",),
        verification_policy="deterministic_rule",
        requires_official_evidence=True,
        llm_allowed=False,
    ),
    BusinessProfileFieldFamilyDefinition(
        field_family=BusinessProfileFieldFamily.COMMODITY_EXPOSURE_PUBLICATION,
        source_stage="governed_publication",
        output_record_types=("exposures",),
        verification_policy="approved_component_assembly",
        requires_official_evidence=True,
        llm_allowed=False,
    ),
)


def get_business_profile_field_family(
    field_family: str | BusinessProfileFieldFamily,
) -> BusinessProfileFieldFamilyDefinition:
    """Return one field-family contract or fail on an unknown identifier."""

    try:
        normalized = BusinessProfileFieldFamily(field_family)
    except ValueError as exc:
        raise ValueError(
            f"unsupported business-profile field family: {field_family}"
        ) from exc
    return next(
        definition
        for definition in FIELD_FAMILY_DEFINITIONS
        if definition.field_family == normalized
    )


def business_profile_field_family_manifest() -> dict[str, Any]:
    """Return the canonical versioned manifest used by run and promotion lineage."""

    return {
        "schema_version": BUSINESS_PROFILE_FIELD_FAMILY_SCHEMA_VERSION,
        "field_families": [item.to_dict() for item in FIELD_FAMILY_DEFINITIONS],
    }
