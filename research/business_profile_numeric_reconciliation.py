"""Authoritative deterministic arithmetic for business-profile facts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.business_profile_unit_conversions import normalize_unit_lexeme


NUMERIC_RECONCILIATION_VERSION = "business_profile_numeric_reconciliation.v1"
DEFAULT_TOLERANCE_FLOOR = Decimal("0.0001")
DEFAULT_TOLERANCE_CEILING = Decimal("0.001")
RECONCILIATION_STATUSES = {"passed", "failed", "derived", "not_applicable"}


@dataclass(frozen=True)
class NumericReconciliationResult:
    status: str
    applicable: bool
    passed: bool
    reported_value: Optional[Decimal]
    calculated_value: Optional[Decimal]
    tolerance: Optional[Decimal]
    difference: Optional[Decimal]
    reason: str
    revenue: Optional[Decimal]
    segment_cost: Optional[Decimal]
    version: str = NUMERIC_RECONCILIATION_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key, value in tuple(payload.items()):
            if isinstance(value, Decimal):
                payload[key] = str(value)
        return payload


def reconcile_gross_margin(
    *,
    revenue: Any,
    segment_cost: Any,
    reported_margin: Any = None,
    reported_margin_unit: str = "fraction",
    dimensions_compatible: bool = True,
    tolerance_floor: Decimal = DEFAULT_TOLERANCE_FLOOR,
    tolerance_ceiling: Decimal = DEFAULT_TOLERANCE_CEILING,
) -> NumericReconciliationResult:
    """Reconcile ``(revenue - cost) / revenue`` without overwriting source data."""

    if revenue is None or segment_cost is None:
        return _result("not_applicable", "missing_revenue_or_cost")
    if not dimensions_compatible:
        return _result("failed", "incompatible_dimensions")
    try:
        revenue_value = decimal_value(revenue, "revenue")
        cost_value = decimal_value(segment_cost, "segment_cost")
    except ValueError:
        return _result("failed", "invalid_revenue_or_cost")
    if revenue_value == 0:
        return _result(
            "not_applicable",
            "zero_revenue",
            revenue=revenue_value,
            segment_cost=cost_value,
        )
    with localcontext() as context:
        context.prec = 50
        calculated = (revenue_value - cost_value) / revenue_value
    if reported_margin is None:
        return _result(
            "derived",
            "reported_margin_missing",
            calculated_value=calculated,
            revenue=revenue_value,
            segment_cost=cost_value,
        )
    try:
        reported_source = decimal_value(reported_margin, "reported_margin")
        reported = normalize_ratio(reported_source, reported_margin_unit)
    except ValueError:
        return _result(
            "failed",
            "invalid_reported_margin",
            calculated_value=calculated,
            revenue=revenue_value,
            segment_cost=cost_value,
        )
    tolerance = precision_aware_tolerance(
        revenue_value,
        cost_value,
        reported_margin=reported_source,
        reported_margin_unit=reported_margin_unit,
        floor=tolerance_floor,
        ceiling=tolerance_ceiling,
    )
    difference = abs(reported - calculated)
    passed = difference <= tolerance
    return NumericReconciliationResult(
        status="passed" if passed else "failed",
        applicable=True,
        passed=passed,
        reported_value=reported,
        calculated_value=calculated,
        tolerance=tolerance,
        difference=difference,
        reason="within_tolerance" if passed else "gross_margin_mismatch",
        revenue=revenue_value,
        segment_cost=cost_value,
    )


def precision_aware_tolerance(
    revenue: Decimal,
    segment_cost: Decimal,
    *,
    reported_margin: Optional[Decimal] = None,
    reported_margin_unit: str = "fraction",
    floor: Decimal = DEFAULT_TOLERANCE_FLOOR,
    ceiling: Decimal = DEFAULT_TOLERANCE_CEILING,
) -> Decimal:
    if floor <= 0 or ceiling < floor:
        raise ValueError("invalid numeric reconciliation tolerance bounds")
    revenue_error = _rounding_error(revenue)
    cost_error = _rounding_error(segment_cost)
    with localcontext() as context:
        context.prec = 50
        propagated = (
            (revenue_error + cost_error) / abs(revenue)
            + abs(revenue - segment_cost) * revenue_error / (revenue * revenue)
        )
    base_tolerance = max(floor, propagated)
    if reported_margin is not None:
        base_tolerance += _reported_rounding_error(
            reported_margin, reported_margin_unit
        )
    # Keep an absolute safety bound while allowing disclosed precision to
    # explain a rounded integer percentage (for example 35% vs 35.249%).
    return min(Decimal("0.01"), min(ceiling, base_tolerance) if reported_margin is None else base_tolerance)


def normalize_ratio(value: Any, source_unit: str) -> Decimal:
    numeric = decimal_value(value, "ratio")
    unit = normalize_unit_lexeme(source_unit or "fraction").lower()
    if unit in {"fraction", "小数", "1"}:
        return numeric
    if unit in {"%", "percent", "百分比"}:
        return numeric * Decimal("0.01")
    raise ValueError(f"unsupported ratio unit: {source_unit}")


def decimal_value(value: Any, label: str = "value") -> Decimal:
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{label} must be decimal-compatible") from exc
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    if result and abs(result.adjusted()) > 100:
        raise ValueError(f"{label} exceeds supported bounds")
    return result


def authoritative_total(values: Iterable[Any]) -> Decimal:
    return sum((decimal_value(value) for value in values), Decimal("0"))


def authoritative_difference(left: Any, right: Any) -> Decimal:
    return decimal_value(left) - decimal_value(right)


def authoritative_ratio(numerator: Any, denominator: Any) -> Optional[Decimal]:
    denominator_value = decimal_value(denominator, "denominator")
    if denominator_value == 0:
        return None
    return decimal_value(numerator, "numerator") / denominator_value


def authoritative_shares(values: Sequence[Any]) -> tuple[Optional[Decimal], ...]:
    numbers = tuple(decimal_value(value) for value in values)
    total = sum(numbers, Decimal("0"))
    if total == 0:
        return tuple(None for _ in numbers)
    return tuple(value / total for value in numbers)


def authoritative_ranks(values: Sequence[Any]) -> tuple[int, ...]:
    numbers = tuple(decimal_value(value) for value in values)
    ordered = sorted(set(numbers), reverse=True)
    ranks = {value: index + 1 for index, value in enumerate(ordered)}
    return tuple(ranks[value] for value in numbers)


def authoritative_materiality(value: Any, threshold: Any) -> bool:
    return abs(decimal_value(value)) >= abs(decimal_value(threshold))


def authoritative_confidence(
    components: Mapping[str, Any], *, weights: Optional[Mapping[str, Any]] = None
) -> Decimal:
    if not components:
        return Decimal("0")
    configured = weights or {key: Decimal("1") for key in components}
    numerator = Decimal("0")
    denominator = Decimal("0")
    for key, raw_value in components.items():
        weight = decimal_value(configured.get(key, 0), f"weight:{key}")
        value = min(Decimal("1"), max(Decimal("0"), decimal_value(raw_value, key)))
        if weight < 0:
            raise ValueError("confidence weights must be non-negative")
        numerator += value * weight
        denominator += weight
    return Decimal("0") if denominator == 0 else numerator / denominator


def _rounding_error(value: Decimal) -> Decimal:
    return Decimal("0.5").scaleb(value.as_tuple().exponent)


def _reported_rounding_error(value: Decimal, source_unit: str) -> Decimal:
    error = _rounding_error(value)
    unit = normalize_unit_lexeme(source_unit or "fraction").lower()
    return error * Decimal("0.01") if unit in {"%", "percent", "百分比"} else error


def _result(
    status: str,
    reason: str,
    *,
    reported_value: Optional[Decimal] = None,
    calculated_value: Optional[Decimal] = None,
    revenue: Optional[Decimal] = None,
    segment_cost: Optional[Decimal] = None,
) -> NumericReconciliationResult:
    return NumericReconciliationResult(
        status=status,
        applicable=status in {"passed", "failed"},
        passed=status == "passed",
        reported_value=reported_value,
        calculated_value=calculated_value,
        tolerance=None,
        difference=None,
        reason=reason,
        revenue=revenue,
        segment_cost=segment_cost,
    )
