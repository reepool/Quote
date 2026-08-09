from decimal import Decimal

import pytest

from research.business_profile_numeric_reconciliation import (
    authoritative_confidence,
    authoritative_ranks,
    authoritative_shares,
    normalize_ratio,
    reconcile_gross_margin,
)


def test_matching_reported_margin_passes_with_decimal_diagnostics():
    result = reconcile_gross_margin(
        revenue="1000.00",
        segment_cost="800.00",
        reported_margin="20.00",
        reported_margin_unit="%",
    )
    assert result.status == "passed"
    assert result.passed is True
    assert result.reported_value == Decimal("0.2000")
    assert result.calculated_value == Decimal("0.2")
    assert result.tolerance is not None


def test_inconsistent_margin_fails_without_overwriting_reported_value():
    result = reconcile_gross_margin(
        revenue="1000",
        segment_cost="800",
        reported_margin="18.41",
        reported_margin_unit="%",
    )
    assert result.status == "failed"
    assert result.reported_value == Decimal("0.1841")
    assert result.calculated_value == Decimal("0.2")
    assert result.reason == "gross_margin_mismatch"


@pytest.mark.parametrize(
    ("revenue", "cost", "reported_margin"),
    [
        ("3730348528.32", "4123298999.92", "0.0237"),
        ("2771050232.64", "3505428112.68", "0.0223"),
    ],
)
def test_600403_shadow_rows_are_rejected_as_inconsistent(
    revenue, cost, reported_margin
):
    result = reconcile_gross_margin(
        revenue=revenue,
        segment_cost=cost,
        reported_margin=reported_margin,
    )
    assert result.status == "failed"
    assert result.passed is False
    assert result.difference > result.tolerance


def test_missing_margin_is_derived_but_not_claimed_reconciled():
    result = reconcile_gross_margin(revenue="100", segment_cost="125")
    assert result.status == "derived"
    assert result.passed is False
    assert result.calculated_value == Decimal("-0.25")


@pytest.mark.parametrize(
    ("kwargs", "status", "reason"),
    [
        ({"revenue": None, "segment_cost": "1"}, "not_applicable", "missing_revenue_or_cost"),
        ({"revenue": "0", "segment_cost": "0"}, "not_applicable", "zero_revenue"),
        (
            {"revenue": "1", "segment_cost": "1", "dimensions_compatible": False},
            "failed",
            "incompatible_dimensions",
        ),
    ],
)
def test_explicit_non_success_states(kwargs, status, reason):
    result = reconcile_gross_margin(**kwargs)
    assert result.status == status
    assert result.reason == reason
    assert result.passed is False


def test_program_owned_ratio_share_ranking_and_confidence():
    assert normalize_ratio("18.41", "%") == Decimal("0.1841")
    assert normalize_ratio("18.41", "（%）") == Decimal("0.1841")
    assert authoritative_shares(["2", "1"]) == (
        Decimal("2") / Decimal("3"),
        Decimal("1") / Decimal("3"),
    )
    assert authoritative_ranks(["2", "5", "5"]) == (2, 1, 1)
    assert authoritative_confidence({"evidence": "1", "parser": "0.5"}) == Decimal(
        "0.75"
    )
