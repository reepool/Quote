"""Versioned exact unit conversions for business-profile numeric facts."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, localcontext
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
UNIT_RESOLUTION_STATUSES = {
    "resolved",
    "unit_resolution_pending",
    "external_rate_required",
    "shadow_active",
}
MAX_UNIT_LEXEME_LENGTH = 96
MAX_ABS_DECIMAL_EXPONENT = 100
MAX_CLASSIFIER_ALTERNATIVES = 16


@dataclass(frozen=True)
class UnitResolution:
    """Deterministic, replayable interpretation of one source-native unit."""

    source_unit: str
    normalized_lexeme: str
    dimension: Optional[str]
    canonical_unit: Optional[str]
    multiplier: Optional[Decimal]
    numerator: tuple[str, ...]
    denominator: tuple[str, ...]
    rule_ids: tuple[str, ...]
    catalog_version: str
    status: str
    reason: Optional[str] = None
    runtime_rule_id: Optional[str] = None

    @property
    def publishable(self) -> bool:
        return self.status == "resolved"

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_unit": self.source_unit,
            "normalized_lexeme": self.normalized_lexeme,
            "dimension": self.dimension,
            "canonical_unit": self.canonical_unit,
            "multiplier": (
                str(self.multiplier) if self.multiplier is not None else None
            ),
            "numerator": list(self.numerator),
            "denominator": list(self.denominator),
            "rule_ids": list(self.rule_ids),
            "catalog_version": self.catalog_version,
            "status": self.status,
            "reason": self.reason,
            "runtime_rule_id": self.runtime_rule_id,
        }


class UnitResolutionPendingError(ValueError):
    """Conversion is replayable after a unit-catalog change."""

    def __init__(self, resolution: UnitResolution) -> None:
        self.resolution = resolution
        super().__init__(
            "unknown business-profile unit; "
            f"status={resolution.status} reason={resolution.reason} "
            f"unit={resolution.source_unit}"
        )


@dataclass(frozen=True)
class _PrimitiveResolution:
    dimension: str
    canonical_unit: str
    multiplier: Decimal
    token: str
    rule_ids: tuple[str, ...]


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

    def resolve(
        self,
        raw_unit: str,
        *,
        required_dimension: Optional[str] = None,
        runtime_rules: Sequence[Mapping[str, Any]] = (),
        allow_shadow: bool = False,
    ) -> UnitResolution:
        """Resolve a source unit without turning an unknown unit into data loss.

        Runtime overlays are data-only records. Only committed ``auto_approved``
        rules are publishable; ``shadow_active`` rules are returned solely when
        explicitly requested for non-publishable calculations.
        """

        source_unit = str(raw_unit or "").strip()
        normalized = normalize_unit_lexeme(source_unit)
        if not normalized:
            return self._pending_resolution(source_unit, normalized, "empty_unit")
        if len(normalized) > MAX_UNIT_LEXEME_LENGTH:
            return self._pending_resolution(
                source_unit, normalized[:MAX_UNIT_LEXEME_LENGTH], "unit_too_long"
            )

        overlay = _resolve_runtime_overlay(
            source_unit,
            normalized,
            runtime_rules,
            catalog_version=self.catalog_version,
            allow_shadow=allow_shadow,
        )
        if overlay is not None:
            if required_dimension and overlay.dimension != required_dimension:
                return self._pending_resolution(
                    source_unit,
                    normalized,
                    "dimension_mismatch",
                    dimension=overlay.dimension,
                )
            return overlay

        try:
            source = self.resolve_unit(source_unit)
        except ValueError as exc:
            if "ambiguous" in str(exc):
                return self._pending_resolution(
                    source_unit, normalized, "ambiguous_catalog_alias"
                )
        else:
            if required_dimension and source.dimension != required_dimension:
                return self._pending_resolution(
                    source_unit,
                    normalized,
                    "dimension_mismatch",
                    dimension=source.dimension,
                )
            target = next(
                unit
                for unit in self.units
                if unit.dimension == source.dimension and unit.canonical_for_dimension
            )
            if source.unit_id == target.unit_id:
                multiplier = Decimal("1")
                rule_id = f"identity:{source.unit_id}"
                status = "resolved"
            else:
                rule, inverse = self._find_rule(source.unit_id, target.unit_id)
                if rule.conversion_type != "fixed_multiplier":
                    return UnitResolution(
                        source_unit=source_unit,
                        normalized_lexeme=normalized,
                        dimension=source.dimension,
                        canonical_unit=target.unit_id,
                        multiplier=None,
                        numerator=(source.unit_id,),
                        denominator=(),
                        rule_ids=(rule.rule_id,),
                        catalog_version=self.catalog_version,
                        status="external_rate_required",
                        reason="implicit_fx_prohibited",
                    )
                assert rule.multiplier is not None
                multiplier = (
                    Decimal("1") / rule.multiplier if inverse else rule.multiplier
                )
                rule_id = f"{rule.rule_id}:inverse" if inverse else rule.rule_id
                status = "resolved"
            return UnitResolution(
                source_unit=source_unit,
                normalized_lexeme=normalized,
                dimension=source.dimension,
                canonical_unit=target.unit_id,
                multiplier=multiplier,
                numerator=(source.unit_id,),
                denominator=(),
                rule_ids=(rule_id,),
                catalog_version=self.catalog_version,
                status=status,
            )

        composed = _compose_unit(normalized, catalog_version=self.catalog_version)
        if composed.status != "resolved":
            return composed
        if required_dimension and composed.dimension != required_dimension:
            return self._pending_resolution(
                source_unit,
                normalized,
                "dimension_mismatch",
                dimension=composed.dimension,
            )
        return UnitResolution(
            source_unit=source_unit,
            normalized_lexeme=composed.normalized_lexeme,
            dimension=composed.dimension,
            canonical_unit=composed.canonical_unit,
            multiplier=composed.multiplier,
            numerator=composed.numerator,
            denominator=composed.denominator,
            rule_ids=composed.rule_ids,
            catalog_version=self.catalog_version,
            status=composed.status,
            reason=composed.reason,
        )

    def convert_resolved(
        self,
        value: Decimal | int | float | str,
        resolution: UnitResolution,
        *,
        period_basis: str,
        equity_basis: str,
    ) -> UnitConversionResult:
        """Apply an already-proved resolution with exact decimal arithmetic."""

        if not resolution.publishable or resolution.multiplier is None:
            raise ValueError(
                "unit resolution is not publishable: "
                f"{resolution.status}:{resolution.reason or 'unresolved'}"
            )
        if period_basis not in self.period_basis_values:
            raise ValueError(f"unsupported period_basis: {period_basis}")
        if equity_basis not in self.equity_basis_values:
            raise ValueError(f"unsupported equity_basis: {equity_basis}")
        numeric_value = _bounded_decimal(value, "value")
        with localcontext() as context:
            context.prec = 50
            normalized_value = numeric_value * resolution.multiplier
        _validate_decimal_bound(normalized_value, "normalized value")
        return UnitConversionResult(
            raw_value=numeric_value,
            raw_unit=resolution.source_unit,
            normalized_value=normalized_value,
            normalized_unit=str(resolution.canonical_unit),
            multiplier=resolution.multiplier,
            conversion_rule_id="+".join(resolution.rule_ids),
            period_basis=period_basis,
            equity_basis=equity_basis,
            catalog_version=resolution.catalog_version,
        )

    def _pending_resolution(
        self,
        source_unit: str,
        normalized: str,
        reason: str,
        *,
        dimension: Optional[str] = None,
    ) -> UnitResolution:
        return UnitResolution(
            source_unit=source_unit,
            normalized_lexeme=normalized,
            dimension=dimension,
            canonical_unit=None,
            multiplier=None,
            numerator=(),
            denominator=(),
            rule_ids=(),
            catalog_version=self.catalog_version,
            status="unit_resolution_pending",
            reason=reason,
        )

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


_PUNCTUATION_TRANSLATION = str.maketrans(
    {
        "／": "/",
        "·": "*",
        "•": "*",
        "，": ",",
        "（": "(",
        "）": ")",
        "［": "[",
        "］": "]",
        "％": "%",
        "：": ":",
    }
)
_MAGNITUDES: tuple[tuple[str, Decimal], ...] = (
    ("一亿", Decimal("100000000")),
    ("亿", Decimal("100000000")),
    ("千万", Decimal("10000000")),
    ("百万", Decimal("1000000")),
    ("十万", Decimal("100000")),
    ("万", Decimal("10000")),
    ("千", Decimal("1000")),
    ("百", Decimal("100")),
    ("十", Decimal("10")),
)
_COUNT_ALIASES = {
    "pcs",
    "piece",
    "pieces",
    "个",
    "件",
    "台",
    "套",
    "项",
    "艘",
    "颗",
    "粒",
    "羽",
    "只",
    "瓶",
    "盒",
    "袋",
    "板",
    "腔",
    "辆",
    "部",
    "枚",
    "片",
    "支",
    "根",
    "块",
    "张",
    "点",
}
_EXACT_TABLE_HEADER_UNIT_ALIASES = {
    "元币种:人民币": "元",
    "单位:元币种:人民币": "元",
}
# Annual-report PDF extraction can lose the original capitalization of power
# units.  This is a narrowly governed compatibility rule for megawatt only;
# general SI prefix parsing below remains case-sensitive.
_POWER_COMPATIBILITY_ALIASES = frozenset({"MW", "mw", "mW"})
_PRIMITIVES: Mapping[str, tuple[str, str, Decimal]] = {
    # currency; cross-currency normalization is intentionally not represented.
    "元": ("currency", "CNY", Decimal("1")),
    "人民币": ("currency", "CNY", Decimal("1")),
    "人民币元": ("currency", "CNY", Decimal("1")),
    "rmb": ("currency", "CNY", Decimal("1")),
    "cny": ("currency", "CNY", Decimal("1")),
    # mass
    "吨": ("mass", "tonne", Decimal("1")),
    "公吨": ("mass", "tonne", Decimal("1")),
    "t": ("mass", "tonne", Decimal("1")),
    "ton": ("mass", "tonne", Decimal("1")),
    "tons": ("mass", "tonne", Decimal("1")),
    "tonne": ("mass", "tonne", Decimal("1")),
    "tonnes": ("mass", "tonne", Decimal("1")),
    "千克": ("mass", "tonne", Decimal("0.001")),
    "公斤": ("mass", "tonne", Decimal("0.001")),
    "kg": ("mass", "tonne", Decimal("0.001")),
    "克": ("mass", "tonne", Decimal("0.000001")),
    "g": ("mass", "tonne", Decimal("0.000001")),
    # freight transport work; this is neither a mass nor a length fact alone.
    "吨千米": ("freight_turnover", "tonne_km", Decimal("1")),
    "吨公里": ("freight_turnover", "tonne_km", Decimal("1")),
    # area
    "平方": ("area", "square_meter", Decimal("1")),
    "平方米": ("area", "square_meter", Decimal("1")),
    "m2": ("area", "square_meter", Decimal("1")),
    "㎡": ("area", "square_meter", Decimal("1")),
    "公顷": ("area", "square_meter", Decimal("10000")),
    "ha": ("area", "square_meter", Decimal("10000")),
    "平方公里": ("area", "square_meter", Decimal("1000000")),
    "km2": ("area", "square_meter", Decimal("1000000")),
    # volume
    "立方": ("volume", "cubic_meter", Decimal("1")),
    "立方米": ("volume", "cubic_meter", Decimal("1")),
    "m3": ("volume", "cubic_meter", Decimal("1")),
    "m³": ("volume", "cubic_meter", Decimal("1")),
    "升": ("volume", "cubic_meter", Decimal("0.001")),
    "l": ("volume", "cubic_meter", Decimal("0.001")),
    "毫升": ("volume", "cubic_meter", Decimal("0.000001")),
    "ml": ("volume", "cubic_meter", Decimal("0.000001")),
    "桶": ("liquid_volume", "barrel", Decimal("1")),
    "bbl": ("liquid_volume", "barrel", Decimal("1")),
    # length
    "米": ("length", "meter", Decimal("1")),
    "m": ("length", "meter", Decimal("1")),
    "公里": ("length", "meter", Decimal("1000")),
    "千米": ("length", "meter", Decimal("1000")),
    "km": ("length", "meter", Decimal("1000")),
    # power
    "瓦": ("power", "watt", Decimal("1")),
    "w": ("power", "watt", Decimal("1")),
    "千瓦": ("power", "watt", Decimal("1000")),
    "kw": ("power", "watt", Decimal("1000")),
    "兆瓦": ("power", "watt", Decimal("1000000")),
    "mw": ("power", "watt", Decimal("1000000")),
    "吉瓦": ("power", "watt", Decimal("1000000000")),
    "gw": ("power", "watt", Decimal("1000000000")),
    # energy
    "瓦时": ("energy", "kwh", Decimal("0.001")),
    "wh": ("energy", "kwh", Decimal("0.001")),
    "千瓦时": ("energy", "kwh", Decimal("1")),
    "度": ("energy", "kwh", Decimal("1")),
    "kwh": ("energy", "kwh", Decimal("1")),
    "兆瓦时": ("energy", "kwh", Decimal("1000")),
    "mwh": ("energy", "kwh", Decimal("1000")),
    "吉瓦时": ("energy", "kwh", Decimal("1000000")),
    "gwh": ("energy", "kwh", Decimal("1000000")),
    # electric charge / battery charge capacity; energy conversion needs voltage.
    "ah": ("electric_charge", "Ah", Decimal("1")),
    "安时": ("electric_charge", "Ah", Decimal("1")),
    "安培小时": ("electric_charge", "Ah", Decimal("1")),
    "mah": ("electric_charge", "Ah", Decimal("0.001")),
    "毫安时": ("electric_charge", "Ah", Decimal("0.001")),
    "毫安培小时": ("electric_charge", "Ah", Decimal("0.001")),
    "kah": ("electric_charge", "Ah", Decimal("1000")),
    "千安时": ("electric_charge", "Ah", Decimal("1000")),
    "千安培小时": ("electric_charge", "Ah", Decimal("1000")),
    # ratio and duration
    "%": ("ratio", "fraction", Decimal("0.01")),
    "百分比": ("ratio", "fraction", Decimal("0.01")),
    "日": ("duration", "day", Decimal("1")),
    "天": ("duration", "day", Decimal("1")),
    "day": ("duration", "day", Decimal("1")),
    "days": ("duration", "day", Decimal("1")),
    "小时": ("duration", "day", Decimal("0.041666666666666666666666666666666666666666666666667")),
    "h": ("duration", "day", Decimal("0.041666666666666666666666666666666666666666666666667")),
    "年": ("duration", "year", Decimal("1")),
    "年度": ("duration", "year", Decimal("1")),
    "year": ("duration", "year", Decimal("1")),
    "years": ("duration", "year", Decimal("1")),
}


def normalize_unit_lexeme(value: Any) -> str:
    """Normalize syntax while preserving the untouched source unit separately."""

    text = unicodedata.normalize("NFKC", str(value or "")).translate(
        _PUNCTUATION_TRANSLATION
    )
    text = re.sub(r"\s+", "", text).replace("每", "/")
    text = text.replace("per", "/")
    text = text.strip(".,;:")
    text = _strip_enclosing_unit_delimiters(text)
    return _EXACT_TABLE_HEADER_UNIT_ALIASES.get(text, text)


def _strip_enclosing_unit_delimiters(text: str) -> str:
    """Remove delimiters only when they enclose the complete unit lexeme."""

    pairs = {"(": ")", "[": "]", "{": "}"}
    while len(text) >= 2 and text[0] in pairs and text[-1] == pairs[text[0]]:
        opening = text[0]
        closing = pairs[opening]
        depth = 0
        encloses_all = True
        for index, character in enumerate(text):
            if character == opening:
                depth += 1
            elif character == closing:
                depth -= 1
                if depth < 0:
                    encloses_all = False
                    break
            if depth == 0 and index < len(text) - 1:
                encloses_all = False
                break
        if not encloses_all or depth != 0:
            break
        text = text[1:-1].strip()
    return text


def governed_primitive_multipliers() -> dict[str, Decimal]:
    """Expose the closed primitive multiplier set for proposal proof only."""

    output = {
        f"primitive:{token}": definition[2]
        for token, definition in _PRIMITIVES.items()
    }
    output.update(
        {f"magnitude:{prefix}": multiplier for prefix, multiplier in _MAGNITUDES}
    )
    output.update({f"classifier:{token}": Decimal("1") for token in _COUNT_ALIASES})
    return output


def governed_primitive_definitions() -> dict[str, dict[str, Any]]:
    """Expose multiplier and dimension metadata for bounded LLM proposals."""

    output = {
        f"primitive:{token}": {
            "multiplier": multiplier,
            "dimension": dimension,
            "canonical_unit": canonical,
            "source_tokens": [token],
        }
        for token, (dimension, canonical, multiplier) in _PRIMITIVES.items()
    }
    output.update(
        {
            f"magnitude:{prefix}": {
                "multiplier": multiplier,
                "dimension": None,
                "canonical_unit": None,
                "source_tokens": [prefix],
            }
            for prefix, multiplier in _MAGNITUDES
        }
    )
    output.update(
        {
            f"classifier:{token}": {
                "multiplier": Decimal("1"),
                "dimension": "count",
                "canonical_unit": "unit",
                "source_tokens": [token],
            }
            for token in _COUNT_ALIASES
        }
    )
    return output


def governed_canonical_units() -> dict[str, str]:
    """Return the single program-owned canonical unit for each catalog dimension."""

    return {
        unit.dimension: unit.unit_id
        for unit in load_unit_conversion_catalog().units
        if unit.canonical_for_dimension
    }


def unit_magnitude_multiplier(raw_unit: Any) -> Decimal:
    """Return the explicit Chinese magnitude prefix in a source unit."""

    normalized = normalize_unit_lexeme(raw_unit).lower()
    for prefix, multiplier in _MAGNITUDES:
        if normalized.startswith(prefix) and len(normalized) > len(prefix):
            return multiplier
    return Decimal("1")


def _compose_unit(normalized: str, *, catalog_version: str) -> UnitResolution:
    source = normalized
    # A parenthesized classifier denotes an alternative, e.g. 万台（套）.
    alternative_match = re.fullmatch(r"(.+?)\(([^()]+)\)", normalized)
    if alternative_match:
        primary_text, alternative_text = alternative_match.groups()
        primary = _resolve_primitive(primary_text)
        scale, unscaled_primary = _split_magnitude(primary_text)
        alternative = _resolve_primitive(alternative_text)
        if primary is None:
            primary = _resolve_primitive(unscaled_primary)
            if primary is not None:
                primary = _scale_primitive(primary, scale, f"magnitude:{scale}")
        if (
            primary is not None
            and alternative is not None
            and primary.dimension == alternative.dimension == "count"
        ):
            return _resolution_from_primitive(
                source,
                primary,
                catalog_version,
                numerator=(primary.token, alternative.token),
                extra_rules=("same_dimension_alternative",),
            )
        if (
            primary is not None
            and alternative is not None
            and primary.dimension != alternative.dimension
        ):
            return _pending_composed(
                source, catalog_version, "cross_dimension_alternative"
            )
        return _pending_composed(source, catalog_version, "ambiguous_alternative")

    # Slash between classifiers is an alternative rather than a rate.
    parts = normalized.split("/")
    alternatives = (
        [_resolve_primitive(part) for part in parts]
        if 2 <= len(parts) <= MAX_CLASSIFIER_ALTERNATIVES
        else []
    )
    if alternatives and all(
        item is not None and item.dimension == "count" for item in alternatives
    ):
        multipliers = {item.multiplier for item in alternatives if item is not None}
        if len(multipliers) != 1:
            return _pending_composed(
                source, catalog_version, "ambiguous_alternative_scale"
            )
        rules = tuple(
            dict.fromkeys(
                rule_id
                for item in alternatives
                if item is not None
                for rule_id in item.rule_ids
            )
        )
        primitive = _PrimitiveResolution(
            "count",
            "unit",
            next(iter(multipliers)),
            normalized,
            ("classifier_alternative", *rules),
        )
        return _resolution_from_primitive(
            source,
            primitive,
            catalog_version,
            numerator=tuple(parts),
        )
    if len(parts) > 2:
        return _pending_composed(source, catalog_version, "unsupported_compound_unit")
    if len(parts) == 2:
        numerator = _resolve_primitive(parts[0])
        denominator = _resolve_primitive(parts[1])
        if numerator is None or denominator is None:
            return _pending_composed(source, catalog_version, "unknown_unit_token")
        compound = _compound_primitive(numerator, denominator)
        if compound is None:
            return _pending_composed(source, catalog_version, "cross_dimension_unit")
        return UnitResolution(
            source_unit=source,
            normalized_lexeme=source,
            dimension=compound.dimension,
            canonical_unit=compound.canonical_unit,
            multiplier=compound.multiplier,
            numerator=(numerator.token,),
            denominator=(denominator.token,),
            rule_ids=compound.rule_ids,
            catalog_version=catalog_version,
            status="resolved",
        )

    primitive = _resolve_primitive(normalized)
    if primitive is None:
        return _pending_composed(source, catalog_version, "unknown_unit_token")
    return _resolution_from_primitive(source, primitive, catalog_version)


def _resolve_primitive(text: str) -> Optional[_PrimitiveResolution]:
    token = text.strip()
    if token in _POWER_COMPATIBILITY_ALIASES:
        return _PrimitiveResolution(
            "power",
            "watt",
            Decimal("1000000"),
            token,
            ("power_compatibility:megawatt",),
        )
    if token in _COUNT_ALIASES or token.lower() in _COUNT_ALIASES:
        return _PrimitiveResolution(
            "count", "unit", Decimal("1"), token, (f"classifier:{token.lower()}",)
        )
    direct = _PRIMITIVES.get(token)
    if direct is None and token not in {"M", "G", "k"}:
        direct = _PRIMITIVES.get(token.lower())
    if direct is not None:
        dimension, canonical, multiplier = direct
        return _PrimitiveResolution(
            dimension,
            canonical,
            multiplier,
            token,
            (f"primitive:{token}",),
        )
    scale, remainder = _split_magnitude(token)
    if scale != Decimal("1"):
        base = _resolve_primitive(remainder)
        if base is not None:
            return _scale_primitive(base, scale, f"magnitude:{token[:-len(remainder)]}")
    for suffix, multiplier in (
        ("hundredmillion", Decimal("100000000")),
        ("tenmillion", Decimal("10000000")),
        ("million", Decimal("1000000")),
        ("thousand", Decimal("1000")),
        ("hundred", Decimal("100")),
        ("ten", Decimal("10")),
    ):
        if token.endswith(suffix) and len(token) > len(suffix):
            base = _resolve_primitive(token[: -len(suffix)])
            if base is not None:
                return _scale_primitive(base, multiplier, f"magnitude:{suffix}")
    # Preserve SI case: m/g are base units and only a complete compound token
    # may use m/M/G/k as a prefix (for example mm, Mt, or kt).
    for prefix, multiplier in (
        ("G", Decimal("1000000000")),
        ("M", Decimal("1000000")),
        ("m", Decimal("0.001")),
        ("k", Decimal("1000")),
    ):
        if token.startswith(prefix) and len(token) > 1:
            remainder = token[len(prefix) :]
            # Prefix case is semantic; base-unit aliases retain the catalog's
            # existing ASCII case-insensitive compatibility (e.g. kW).
            base = _PRIMITIVES.get(remainder) or _PRIMITIVES.get(remainder.lower())
            if base is not None:
                dimension, canonical, base_multiplier = base
                return _PrimitiveResolution(
                    dimension,
                    canonical,
                    multiplier * base_multiplier,
                    token,
                    (f"si_prefix:{prefix}", f"primitive:{remainder}"),
                )
    return None


def _split_magnitude(text: str) -> tuple[Decimal, str]:
    for prefix, multiplier in _MAGNITUDES:
        if text.startswith(prefix) and len(text) > len(prefix):
            return multiplier, text[len(prefix) :]
    return Decimal("1"), text


def _scale_primitive(
    primitive: _PrimitiveResolution, scale: Decimal, rule_id: str
) -> _PrimitiveResolution:
    return _PrimitiveResolution(
        primitive.dimension,
        primitive.canonical_unit,
        primitive.multiplier * scale,
        primitive.token,
        (rule_id, *primitive.rule_ids),
    )


def _compound_primitive(
    numerator: _PrimitiveResolution,
    denominator: _PrimitiveResolution,
) -> Optional[_PrimitiveResolution]:
    if denominator.dimension == "duration" and denominator.canonical_unit in {
        "day",
        "year",
    }:
        dimension = {
            "mass": "mass_capacity",
            "volume": "volume_capacity",
            "liquid_volume": "liquid_volume_rate",
            "energy": "energy_capacity",
            "power": "power_capacity",
            "count": "count_capacity",
        }.get(numerator.dimension)
        if dimension is None:
            return None
        canonical = f"{numerator.canonical_unit}/{denominator.canonical_unit}"
        return _PrimitiveResolution(
            dimension,
            canonical,
            numerator.multiplier / denominator.multiplier,
            f"{numerator.token}/{denominator.token}",
            (*numerator.rule_ids, *denominator.rule_ids, "compound:rate"),
        )
    if numerator.dimension == "currency":
        dimension = {
            "mass": "price_per_mass",
            "volume": "price_per_volume",
            "liquid_volume": "price_per_liquid_volume",
            "energy": "price_per_energy",
            "power": "price_per_power",
            "count": "price_per_count",
            "area": "price_per_area",
            "length": "price_per_length",
        }.get(denominator.dimension)
        if dimension is None:
            return None
        return _PrimitiveResolution(
            dimension,
            f"{numerator.canonical_unit}/{denominator.canonical_unit}",
            numerator.multiplier / denominator.multiplier,
            f"{numerator.token}/{denominator.token}",
            (*numerator.rule_ids, *denominator.rule_ids, "compound:price"),
        )
    return None


def _resolution_from_primitive(
    source: str,
    primitive: _PrimitiveResolution,
    catalog_version: str,
    *,
    numerator: Optional[tuple[str, ...]] = None,
    extra_rules: tuple[str, ...] = (),
) -> UnitResolution:
    return UnitResolution(
        source_unit=source,
        normalized_lexeme=source,
        dimension=primitive.dimension,
        canonical_unit=primitive.canonical_unit,
        multiplier=primitive.multiplier,
        numerator=numerator or (primitive.token,),
        denominator=(),
        rule_ids=(*primitive.rule_ids, *extra_rules),
        catalog_version=catalog_version,
        status="resolved",
    )


def _pending_composed(
    source: str, catalog_version: str, reason: str
) -> UnitResolution:
    return UnitResolution(
        source_unit=source,
        normalized_lexeme=source,
        dimension=None,
        canonical_unit=None,
        multiplier=None,
        numerator=(),
        denominator=(),
        rule_ids=(),
        catalog_version=catalog_version,
        status="unit_resolution_pending",
        reason=reason,
    )


def _resolve_runtime_overlay(
    source_unit: str,
    normalized: str,
    rules: Sequence[Mapping[str, Any]],
    *,
    catalog_version: str,
    allow_shadow: bool,
) -> Optional[UnitResolution]:
    matches = [
        rule
        for rule in rules
        if normalize_unit_lexeme(rule.get("normalized_lexeme")) == normalized
        and str(rule.get("status"))
        in ({"auto_approved", "shadow_active"} if allow_shadow else {"auto_approved"})
    ]
    if len(matches) != 1:
        return None
    rule = matches[0]
    try:
        multiplier = _bounded_decimal(rule.get("multiplier"), "runtime multiplier")
    except ValueError:
        return None
    status = (
        "shadow_active" if str(rule.get("status")) == "shadow_active" else "resolved"
    )
    return UnitResolution(
        source_unit=source_unit,
        normalized_lexeme=normalized,
        dimension=str(rule.get("dimension") or "") or None,
        canonical_unit=str(rule.get("canonical_unit") or "") or None,
        multiplier=multiplier,
        numerator=tuple(str(item) for item in rule.get("numerator", ())),
        denominator=tuple(str(item) for item in rule.get("denominator", ())),
        rule_ids=(str(rule.get("rule_id")),),
        catalog_version=str(rule.get("catalog_version") or catalog_version),
        status=status,
        runtime_rule_id=str(rule.get("rule_id")),
    )


def _bounded_decimal(value: Any, field_name: str) -> Decimal:
    result = _decimal(value, field_name)
    _validate_decimal_bound(result, field_name)
    return result


def _validate_decimal_bound(value: Decimal, field_name: str) -> None:
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if value and abs(value.adjusted()) > MAX_ABS_DECIMAL_EXPONENT:
        raise ValueError(f"{field_name} exceeds supported decimal bounds")


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
