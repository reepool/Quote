import pytest

from research.business_profile_temporal import (
    BUSINESS_PROFILE_TEMPORAL_POLICY_SCHEMA_VERSION,
    BusinessProfileTemporalClass,
    business_profile_temporal_policy_manifest,
    derive_report_observation_interval,
    get_business_profile_temporal_policy,
)


def test_temporal_manifest_declares_all_required_classes():
    manifest = business_profile_temporal_policy_manifest()
    classes = {item["temporal_class"] for item in manifest["policies"]}

    assert manifest["schema_version"] == BUSINESS_PROFILE_TEMPORAL_POLICY_SCHEMA_VERSION
    assert classes == {item.value for item in BusinessProfileTemporalClass}
    assert get_business_profile_temporal_policy("segments").freshness_days == 550


@pytest.mark.parametrize(
    ("report_period", "period_basis", "expected"),
    [
        ("2025-12-31", None, ("2025-01-01", "2025-12-31")),
        ("2026-06-30", None, ("2026-01-01", "2026-06-30")),
        ("2026-09-30", "quarterly", ("2026-07-01", "2026-09-30")),
    ],
)
def test_report_observation_intervals_are_bounded(report_period, period_basis, expected):
    assert derive_report_observation_interval(
        report_period,
        period_basis=period_basis,
    ) == expected


def test_report_observation_interval_rejects_non_month_end():
    with pytest.raises(ValueError, match="calendar month end"):
        derive_report_observation_interval("2026-06-29")
