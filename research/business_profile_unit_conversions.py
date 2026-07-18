"""Versioned exact unit conversions for business-profile numeric facts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


DEFAULT_UNIT_CONVERSION_CATALOG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "business_profile_unit_conversion_catalog.json"
)
UNIT_CONVERSION_CATALOG_SCHEMA_VERSION = "business_profile_unit_conversion_catalog.v1"
CONVERSION_TYPES = {"fixed_multiplier", "external_rate_required"}


@dataclass(frozen=True)
class UnitDefinition:
    unit_id: str
    dimension: str
    aliases: tuple[str, ...]
    canonical_for_dimension: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["aliases"] = list(self.aliases)
        return payload


@dataclass(frozen=True)
class UnitConversionRule:
    rule_id: str
    from_unit: str
    to_unit: str
    conversion_type: str
    multiplier: Optional[Decimal]
    required_lineage_fields: tuple[str, ...]
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["multiplier"] = (
            str(self.multiplier) if self.multiplier is not None else None
        )
        payload["required_lineage_fields"] = list(self.required_lineage_fields)
        return payload


@dataclass(frozen=True)
class UnitConversionResult:
    raw_value: Decimal
    raw_unit: str
    normalized_value: Decimal
    normalized_unit: str
    multiplier: Decimal
    conversion_rule_id: str
    period_basis: str
    equity_basis: str
    catalog_version: str

    def to_dict(self) -> dict[str, str]:
        return {
            "raw_value": str(self.raw_value),
            "raw_unit": self.raw_unit,
            "normalized_value": str(self.normalized_value),
            "normalized_unit": self.normalized_unit,
            "multiplier": str(self.multiplier),
            "conversion_rule_id": self.conversion_rule_id,
            "period_basis": self.period_basis,
            "equity_basis": self.equity_basis,
            "catalog_version": self.catalog_version,
        }


@dataclass(frozen=True)
class UnitConversionCatalog:
    schema_version: str
    catalog_version: str
    fact_catalog_version: str
    released_on: str
    period_basis_values: tuple[str, ...]
    equity_basis_values: tuple[str, ...]
    prohibited_transformations: tuple[str, ...]
    units: tuple[UnitDefinition, ...]
    conversions: tuple[UnitConversionRule, ...]

    def resolve_unit(self, raw_unit: str) -> UnitDefinition:
        normalized = normalize_unit_alias(raw_unit)
        matches = [
            unit
            for unit in self.units
            if normalized == normalize_unit_alias(unit.unit_id)
            or normalized in {normalize_unit_alias(alias) for alias in unit.aliases}
        ]
        if not matches:
            raise ValueError(f"unknown business-profile unit: {raw_unit}")
        if len(matches) > 1:
            raise ValueError(f"ambiguous business-profile unit: {raw_unit}")
        return matches[0]

    def convert(
        self,
        value: Decimal | int | float | str,
        *,
        from_unit: str,
        to_unit: str,
        period_basis: str,
        equity_basis: str,
    ) -> UnitConversionResult:
        source = self.resolve_unit(from_unit)
        target = self.resolve_unit(to_unit)
        if source.dimension != target.dimension:
            raise ValueError(
                "unit dimension mismatch: "
                f"{source.unit_id}({source.dimension}) -> "
                f"{target.unit_id}({target.dimension})"
            )
        if period_basis not in self.period_basis_values:
            raise ValueError(f"unsupported period_basis: {period_basis}")
        if equity_basis not in self.equity_basis_values:
            raise ValueError(f"unsupported equity_basis: {equity_basis}")
        numeric_value = _decimal(value, "value")
        if source.unit_id == target.unit_id:
            multiplier = Decimal("1")
            rule_id = f"identity:{source.unit_id}"
        else:
            rule, inverse = self._find_rule(source.unit_id, target.unit_id)
            if rule.conversion_type != "fixed_multiplier":
                raise ValueError(
                    "unit conversion requires external lineage: "
                    f"{rule.rule_id}; fields={list(rule.required_lineage_fields)}"
                )
            if rule.multiplier is None:
                raise ValueError(
                    f"fixed unit conversion is missing multiplier: {rule.rule_id}"
                )
            multiplier = Decimal("1") / rule.multiplier if inverse else rule.multiplier
            rule_id = f"{rule.rule_id}:inverse" if inverse else rule.rule_id
        return UnitConversionResult(
            raw_value=numeric_value,
            raw_unit=source.unit_id,
            normalized_value=numeric_value * multiplier,
            normalized_unit=target.unit_id,
            multiplier=multiplier,
            conversion_rule_id=rule_id,
            period_basis=period_basis,
            equity_basis=equity_basis,
            catalog_version=self.catalog_version,
        )

    def _find_rule(
        self,
        from_unit: str,
        to_unit: str,
    ) -> tuple[UnitConversionRule, bool]:
        direct = [
            rule
            for rule in self.conversions
            if rule.from_unit == from_unit and rule.to_unit == to_unit
        ]
        inverse = [
            rule
            for rule in self.conversions
            if rule.from_unit == to_unit and rule.to_unit == from_unit
        ]
        if direct:
            return direct[0], False
        if inverse:
            return inverse[0], True
        raise ValueError(f"no governed unit conversion: {from_unit} -> {to_unit}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "catalog_version": self.catalog_version,
            "fact_catalog_version": self.fact_catalog_version,
            "released_on": self.released_on,
            "period_basis_values": list(self.period_basis_values),
            "equity_basis_values": list(self.equity_basis_values),
            "prohibited_transformations": list(self.prohibited_transformations),
            "units": [unit.to_dict() for unit in self.units],
            "conversions": [rule.to_dict() for rule in self.conversions],
        }


@lru_cache(maxsize=8)
def load_unit_conversion_catalog(
    path: str | Path = DEFAULT_UNIT_CONVERSION_CATALOG_PATH,
    *,
    version: Optional[str] = None,
) -> UnitConversionCatalog:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    catalog = parse_unit_conversion_catalog(payload)
    from research.business_profile_fact_catalog import load_business_fact_catalog

    fact_catalog = load_business_fact_catalog()
    if catalog.fact_catalog_version != fact_catalog.catalog_version:
        raise ValueError(
            "unit conversion fact catalog version mismatch: "
            f"{catalog.fact_catalog_version} != {fact_catalog.catalog_version}"
        )
    if version is not None and catalog.catalog_version != str(version):
        raise ValueError(
            "unsupported unit conversion catalog version: "
            f"{version}; configured={catalog.catalog_version}"
        )
    return catalog


def parse_unit_conversion_catalog(
    payload: Mapping[str, Any],
) -> UnitConversionCatalog:
    if not isinstance(payload, Mapping):
        raise ValueError("unit conversion catalog root must be an object")
    schema_version = _required_text(payload, "schema_version", "catalog")
    if schema_version != UNIT_CONVERSION_CATALOG_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported unit conversion catalog schema_version: {schema_version}"
        )
    catalog_version = _required_text(payload, "catalog_version", "catalog")
    fact_catalog_version = _required_text(
        payload,
        "fact_catalog_version",
        "catalog",
    )
    released_on = _required_text(payload, "released_on", "catalog")
    _validate_date(released_on, "released_on")
    period_basis_values = _string_tuple(
        payload.get("period_basis_values"),
        "period_basis_values",
        "catalog",
    )
    equity_basis_values = _string_tuple(
        payload.get("equity_basis_values"),
        "equity_basis_values",
        "catalog",
    )
    prohibited_transformations = _string_tuple(
        payload.get("prohibited_transformations"),
        "prohibited_transformations",
        "catalog",
    )
    units = _parse_units(payload.get("units"))
    conversions = _parse_conversions(
        payload.get("conversions"),
        units={unit.unit_id: unit for unit in units},
    )
    return UnitConversionCatalog(
        schema_version=schema_version,
        catalog_version=catalog_version,
        fact_catalog_version=fact_catalog_version,
        released_on=released_on,
        period_basis_values=period_basis_values,
        equity_basis_values=equity_basis_values,
        prohibited_transformations=prohibited_transformations,
        units=units,
        conversions=conversions,
    )


def normalize_unit_alias(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace(" ", "")
        .replace("公吨", "吨")
        .replace("每", "/")
    )


def _parse_units(value: Any) -> tuple[UnitDefinition, ...]:
    rows = _object_array(value, "units")
    output: list[UnitDefinition] = []
    seen_ids: set[str] = set()
    seen_aliases: dict[str, str] = {}
    canonical_dimensions: set[str] = set()
    for index, row in enumerate(rows):
        location = f"units[{index}]"
        unit_id = _required_text(row, "unit_id", location)
        if unit_id in seen_ids:
            raise ValueError(f"duplicate unit_id: {unit_id}")
        seen_ids.add(unit_id)
        dimension = _required_text(row, "dimension", location)
        aliases = _string_tuple(
            row.get("aliases", ()),
            "aliases",
            location,
            allow_empty=True,
        )
        for alias in (unit_id, *aliases):
            normalized = normalize_unit_alias(alias)
            prior = seen_aliases.get(normalized)
            if prior is not None and prior != unit_id:
                raise ValueError(f"unit alias collision: {alias}; {prior} vs {unit_id}")
            seen_aliases[normalized] = unit_id
        canonical = row.get("canonical_for_dimension")
        if not isinstance(canonical, bool):
            raise ValueError(f"{location} canonical_for_dimension must be a boolean")
        if canonical:
            if dimension in canonical_dimensions:
                raise ValueError(f"multiple canonical units for dimension: {dimension}")
            canonical_dimensions.add(dimension)
        output.append(
            UnitDefinition(
                unit_id=unit_id,
                dimension=dimension,
                aliases=aliases,
                canonical_for_dimension=canonical,
            )
        )
    dimensions = {unit.dimension for unit in output}
    missing_canonical = dimensions - canonical_dimensions
    if missing_canonical:
        raise ValueError(
            f"dimensions missing canonical units: {sorted(missing_canonical)}"
        )
    return tuple(output)


def _parse_conversions(
    value: Any,
    *,
    units: Mapping[str, UnitDefinition],
) -> tuple[UnitConversionRule, ...]:
    rows = _object_array(value, "conversions")
    output: list[UnitConversionRule] = []
    seen_ids: set[str] = set()
    seen_pairs: set[frozenset[str]] = set()
    for index, row in enumerate(rows):
        location = f"conversions[{index}]"
        rule_id = _required_text(row, "rule_id", location)
        if rule_id in seen_ids:
            raise ValueError(f"duplicate unit conversion rule_id: {rule_id}")
        seen_ids.add(rule_id)
        from_unit = _required_text(row, "from_unit", location)
        to_unit = _required_text(row, "to_unit", location)
        if from_unit not in units or to_unit not in units:
            raise ValueError(f"{location} references unknown units")
        if units[from_unit].dimension != units[to_unit].dimension:
            raise ValueError(f"{location} crosses unit dimensions")
        pair = frozenset((from_unit, to_unit))
        if pair in seen_pairs:
            raise ValueError(f"{location} duplicates a governed conversion pair")
        seen_pairs.add(pair)
        conversion_type = _enum_text(
            row,
            "conversion_type",
            CONVERSION_TYPES,
            location,
        )
        multiplier_raw = row.get("multiplier")
        multiplier = (
            _decimal(multiplier_raw, f"{location}.multiplier")
            if multiplier_raw is not None
            else None
        )
        required_lineage_fields = _string_tuple(
            row.get("required_lineage_fields", ()),
            "required_lineage_fields",
            location,
            allow_empty=True,
        )
        if conversion_type == "fixed_multiplier":
            if multiplier is None or multiplier <= 0:
                raise ValueError(
                    f"{location} fixed_multiplier requires positive multiplier"
                )
            if required_lineage_fields:
                raise ValueError(
                    f"{location} fixed_multiplier cannot require external lineage"
                )
        else:
            if multiplier is not None:
                raise ValueError(
                    f"{location} external conversion cannot define multiplier"
                )
            if not required_lineage_fields:
                raise ValueError(
                    f"{location} external conversion requires lineage fields"
                )
        output.append(
            UnitConversionRule(
                rule_id=rule_id,
                from_unit=from_unit,
                to_unit=to_unit,
                conversion_type=conversion_type,
                multiplier=multiplier,
                required_lineage_fields=required_lineage_fields,
                notes=str(row.get("notes") or "").strip(),
            )
        )
    return tuple(output)


def _object_array(value: Any, key: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"unit conversion catalog {key} must be a non-empty array")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"unit conversion catalog {key} must contain objects")
    return value


def _required_text(row: Mapping[str, Any], key: str, location: str) -> str:
    value = str(row.get(key) or "").strip()
    if not value:
        raise ValueError(f"unit conversion catalog {location} missing {key}")
    return value


def _enum_text(
    row: Mapping[str, Any],
    key: str,
    allowed: set[str],
    location: str,
) -> str:
    value = _required_text(row, key, location)
    if value not in allowed:
        raise ValueError(
            f"unit conversion catalog {location} unsupported {key}: {value}"
        )
    return value


def _string_tuple(
    value: Any,
    key: str,
    location: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"unit conversion catalog {location} {key} must be an array")
    result = tuple(str(item).strip() for item in value if str(item).strip())
    if not allow_empty and not result:
        raise ValueError(f"unit conversion catalog {location} {key} must not be empty")
    if len(set(result)) != len(result):
        raise ValueError(
            f"unit conversion catalog {location} {key} contains duplicates"
        )
    return result


def _decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc


def _validate_date(value: Any, field_name: str) -> None:
    try:
        from datetime import date

        date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"unit conversion catalog {field_name} must be an ISO date"
        ) from exc
