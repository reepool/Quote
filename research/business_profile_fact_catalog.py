"""Versioned field contract for governed company business-profile facts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


DEFAULT_BUSINESS_FACT_CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "business_profile_fact_catalog.json"
)
BUSINESS_FACT_CATALOG_SCHEMA_VERSION = "business_profile_fact_catalog.v1"

RECORD_TYPES = {
    "segments",
    "operating_facts",
    "value_chain_roles",
    "exposures",
}
VALUE_TYPES = {"decimal", "integer", "ratio", "enum", "text", "identifier"}
UNIT_DIMENSIONS = {
    "capacity",
    "classification",
    "cost_per_unit",
    "currency",
    "days",
    "identifier",
    "price_per_unit",
    "ratio",
    "resource_quantity",
    "source_reported",
    "text",
    "volume",
}
MATERIALITY_LEVELS = {"critical", "high", "medium", "low"}
CANDIDATE_POLICIES = {
    "automatic",
    "automatic_if_reconciled",
    "template_scoped",
    "review_only",
    "disabled",
}
REVIEW_POLICIES = {
    "rule_assisted_after_promotion",
    "human_required",
    "human_required_sensitive",
}
RECONCILIATION_POLICIES = {"required", "when_available", "not_applicable"}
DCF_ELIGIBILITY_POLICIES = {"approved_only", "diagnostic_only", "not_eligible"}


@dataclass(frozen=True)
class BusinessFactDefinition:
    """Immutable semantic and governance definition for one extracted field."""

    field_id: str
    record_type: str
    fact_type: str
    label_zh: str
    semantic: str
    value_type: str
    unit_dimension: str
    canonical_units: tuple[str, ...]
    allowed_values: tuple[str, ...]
    materiality: str
    candidate_policy: str
    review_policy: str
    reconciliation_policy: str
    dcf_eligibility: str
    required_dimensions: tuple[str, ...]
    prohibited_inferences: tuple[str, ...]
    notes: str = ""

    @property
    def numeric(self) -> bool:
        return self.value_type in {"decimal", "integer", "ratio"}

    @property
    def machine_candidate_enabled(self) -> bool:
        return self.candidate_policy not in {"review_only", "disabled"}

    @property
    def requires_human_review(self) -> bool:
        return self.review_policy in {"human_required", "human_required_sensitive"}

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in (
            "canonical_units",
            "allowed_values",
            "required_dimensions",
            "prohibited_inferences",
        ):
            payload[key] = list(payload[key])
        return payload


@dataclass(frozen=True)
class BusinessFactCatalog:
    """Validated catalog selected by semantic version and effective date."""

    schema_version: str
    catalog_version: str
    released_on: str
    document_applicable_from: str
    document_applicable_to: Optional[str]
    definitions: tuple[BusinessFactDefinition, ...]

    def get(self, field_id: str) -> Optional[BusinessFactDefinition]:
        target = str(field_id or "").strip()
        return next(
            (
                definition
                for definition in self.definitions
                if definition.field_id == target
            ),
            None,
        )

    def require(self, field_id: str) -> BusinessFactDefinition:
        definition = self.get(field_id)
        if definition is None:
            raise KeyError(f"unknown business fact field_id: {field_id}")
        return definition

    def list_definitions(
        self,
        *,
        record_type: Optional[str] = None,
        dcf_eligibility: Optional[str] = None,
    ) -> tuple[BusinessFactDefinition, ...]:
        if record_type is not None and record_type not in RECORD_TYPES:
            raise ValueError(f"unsupported business fact record_type: {record_type}")
        if (
            dcf_eligibility is not None
            and dcf_eligibility not in DCF_ELIGIBILITY_POLICIES
        ):
            raise ValueError(
                f"unsupported business fact dcf_eligibility: {dcf_eligibility}"
            )
        return tuple(
            definition
            for definition in self.definitions
            if (record_type is None or definition.record_type == record_type)
            and (
                dcf_eligibility is None or definition.dcf_eligibility == dcf_eligibility
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "catalog_version": self.catalog_version,
            "released_on": self.released_on,
            "document_applicable_from": self.document_applicable_from,
            "document_applicable_to": self.document_applicable_to,
            "fields": [definition.to_dict() for definition in self.definitions],
        }


@lru_cache(maxsize=8)
def load_business_fact_catalog(
    path: str | Path = DEFAULT_BUSINESS_FACT_CATALOG_PATH,
    *,
    version: Optional[str] = None,
    document_date: Optional[str] = None,
) -> BusinessFactCatalog:
    """Load and validate the configured business-fact catalog.

    ``document_date`` checks whether the source document is covered by this
    extraction catalog. ``released_on`` records when the catalog itself was
    published and does not restrict historical source documents.
    """

    catalog_path = Path(path)
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog = parse_business_fact_catalog(payload)
    if version is not None and catalog.catalog_version != str(version):
        raise ValueError(
            "unsupported business fact catalog version: "
            f"{version}; configured={catalog.catalog_version}"
        )
    if document_date is not None:
        requested_date = _parse_date(document_date, "document_date")
        applicable_from = _parse_date(
            catalog.document_applicable_from,
            "document_applicable_from",
        )
        applicable_to = (
            _parse_date(
                catalog.document_applicable_to,
                "document_applicable_to",
            )
            if catalog.document_applicable_to
            else None
        )
        if requested_date < applicable_from or (
            applicable_to is not None and requested_date > applicable_to
        ):
            raise ValueError(
                "business fact catalog is not applicable to document_date: "
                f"{document_date}"
            )
    return catalog


def parse_business_fact_catalog(payload: Mapping[str, Any]) -> BusinessFactCatalog:
    """Validate an in-memory catalog payload and return its immutable form."""

    if not isinstance(payload, Mapping):
        raise ValueError("business fact catalog root must be an object")
    schema_version = _required_text(payload, "schema_version")
    if schema_version != BUSINESS_FACT_CATALOG_SCHEMA_VERSION:
        raise ValueError(
            "unsupported business fact catalog schema_version: " f"{schema_version}"
        )
    catalog_version = _required_text(payload, "catalog_version")
    released_on = _required_text(payload, "released_on")
    _parse_date(released_on, "released_on")
    document_applicable_from = _required_text(
        payload,
        "document_applicable_from",
    )
    applicable_from = _parse_date(
        document_applicable_from,
        "document_applicable_from",
    )
    document_applicable_to = (
        str(payload.get("document_applicable_to") or "").strip() or None
    )
    if document_applicable_to is not None:
        applicable_to = _parse_date(
            document_applicable_to,
            "document_applicable_to",
        )
        if applicable_to < applicable_from:
            raise ValueError(
                "business fact catalog has invalid document applicability interval"
            )
    rows = payload.get("fields")
    if not isinstance(rows, list) or not rows:
        raise ValueError("business fact catalog fields must be a non-empty array")

    definitions: list[BusinessFactDefinition] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"business fact catalog fields[{index}] must be an object")
        definition = _parse_definition(row, index=index)
        if definition.field_id in seen:
            raise ValueError(f"duplicate business fact field_id: {definition.field_id}")
        seen.add(definition.field_id)
        definitions.append(definition)

    return BusinessFactCatalog(
        schema_version=schema_version,
        catalog_version=catalog_version,
        released_on=released_on,
        document_applicable_from=document_applicable_from,
        document_applicable_to=document_applicable_to,
        definitions=tuple(definitions),
    )


def _parse_definition(
    row: Mapping[str, Any],
    *,
    index: int,
) -> BusinessFactDefinition:
    field_id = _required_text(row, "field_id", index=index)
    record_type = _enum_value(row, "record_type", RECORD_TYPES, index=index)
    value_type = _enum_value(row, "value_type", VALUE_TYPES, index=index)
    unit_dimension = _enum_value(row, "unit_dimension", UNIT_DIMENSIONS, index=index)
    materiality = _enum_value(row, "materiality", MATERIALITY_LEVELS, index=index)
    candidate_policy = _enum_value(
        row, "candidate_policy", CANDIDATE_POLICIES, index=index
    )
    review_policy = _enum_value(row, "review_policy", REVIEW_POLICIES, index=index)
    reconciliation_policy = _enum_value(
        row,
        "reconciliation_policy",
        RECONCILIATION_POLICIES,
        index=index,
    )
    dcf_eligibility = _enum_value(
        row, "dcf_eligibility", DCF_ELIGIBILITY_POLICIES, index=index
    )
    canonical_units = _string_tuple(
        row.get("canonical_units"),
        "canonical_units",
        index,
        allow_empty=True,
    )
    allowed_values = _string_tuple(
        row.get("allowed_values", ()),
        "allowed_values",
        index,
        allow_empty=True,
    )
    required_dimensions = _string_tuple(
        row.get("required_dimensions"),
        "required_dimensions",
        index,
        allow_empty=True,
    )
    prohibited_inferences = _string_tuple(
        row.get("prohibited_inferences"),
        "prohibited_inferences",
        index,
        allow_empty=True,
    )

    numeric = value_type in {"decimal", "integer", "ratio"}
    if numeric and not canonical_units and unit_dimension != "source_reported":
        raise ValueError(
            f"business fact catalog fields[{index}] numeric field requires canonical_units"
        )
    if value_type == "enum" and not allowed_values:
        raise ValueError(
            f"business fact catalog fields[{index}] enum field requires allowed_values"
        )
    if value_type != "enum" and allowed_values:
        raise ValueError(
            f"business fact catalog fields[{index}] non-enum field "
            "must not define allowed_values"
        )
    if dcf_eligibility == "approved_only" and review_policy not in {
        "human_required",
        "human_required_sensitive",
    }:
        raise ValueError(
            f"business fact catalog fields[{index}] approved_only field "
            "requires human review"
        )
    if review_policy == "human_required_sensitive" and materiality not in {
        "critical",
        "high",
    }:
        raise ValueError(
            f"business fact catalog fields[{index}] sensitive field "
            "must have critical or high materiality"
        )

    return BusinessFactDefinition(
        field_id=field_id,
        record_type=record_type,
        fact_type=_required_text(row, "fact_type", index=index),
        label_zh=_required_text(row, "label_zh", index=index),
        semantic=_required_text(row, "semantic", index=index),
        value_type=value_type,
        unit_dimension=unit_dimension,
        canonical_units=canonical_units,
        allowed_values=allowed_values,
        materiality=materiality,
        candidate_policy=candidate_policy,
        review_policy=review_policy,
        reconciliation_policy=reconciliation_policy,
        dcf_eligibility=dcf_eligibility,
        required_dimensions=required_dimensions,
        prohibited_inferences=prohibited_inferences,
        notes=str(row.get("notes") or "").strip(),
    )


def _required_text(
    row: Mapping[str, Any],
    key: str,
    *,
    index: Optional[int] = None,
) -> str:
    value = str(row.get(key) or "").strip()
    if value:
        return value
    location = "" if index is None else f" fields[{index}]"
    raise ValueError(f"business fact catalog{location} missing {key}")


def _enum_value(
    row: Mapping[str, Any],
    key: str,
    allowed: set[str],
    *,
    index: int,
) -> str:
    value = _required_text(row, key, index=index)
    if value not in allowed:
        raise ValueError(
            f"business fact catalog fields[{index}] unsupported {key}: {value}"
        )
    return value


def _string_tuple(
    value: Any,
    key: str,
    index: int,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(
            f"business fact catalog fields[{index}] {key} must be an array"
        )
    result = tuple(str(item).strip() for item in value if str(item).strip())
    if not allow_empty and not result:
        raise ValueError(
            f"business fact catalog fields[{index}] {key} must not be empty"
        )
    if len(set(result)) != len(result):
        raise ValueError(
            f"business fact catalog fields[{index}] {key} contains duplicates"
        )
    return result


def _parse_date(value: Any, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"business fact catalog {field_name} must be an ISO date"
        ) from exc
