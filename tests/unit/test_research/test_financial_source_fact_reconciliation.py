"""Tests for the official-versus-fallback source fact reconciliation service."""

import pytest

from research.financial_source_fact_reconciliation import (
    DEFAULT_ABSOLUTE_FLOOR_CNY,
    RECORD_KIND_SANITY_REVIEW,
    RECORD_KIND_SOURCE_DIFFERENCE,
    compare_official_and_fallback_facts,
    evaluate_single_sourced_sanity,
    summarize_comparison_records,
    values_match_within_tolerance,
)


def test_values_match_within_tolerance_cases():
    assert values_match_within_tolerance(100.0, 100.0)
    assert values_match_within_tolerance(100.0, 100.4)
    # Difference of exactly the absolute floor is allowed.
    assert values_match_within_tolerance(100.0, 101.0)
    assert not values_match_within_tolerance(100.0, 101.01)
    assert values_match_within_tolerance(0.0, 0.5, absolute_floor_cny=1.0)
    assert not values_match_within_tolerance(0.0, 5.0, absolute_floor_cny=1.0)


def test_compare_skips_one_sided_facts_and_flags_differences():
    records = compare_official_and_fallback_facts(
        instrument_id="002254.SZ",
        report_period="2026-09-30",
        official_facts={
            "revenue": 3_111_164_418.0,
            "total_assets": 16_735_362_100.0,
            "equity_total": 7_935_173_800.0,
        },
        fallback_facts={
            "revenue": 3_111_164_418.0,
            "total_assets": 16_735_362_100.0,
            "equity_parent": 7_105_855_388.0,
        },
        official_source="cninfo",
        fallback_source="akshare",
        scope=("revenue", "total_assets", "equity_parent", "total_liabilities"),
    )
    # revenue/total_assets match; equity_parent and total_liabilities are
    # one-sided and never flagged.
    assert records == []


def test_compare_flags_beyond_tolerance_with_both_values():
    records = compare_official_and_fallback_facts(
        instrument_id="002254.SZ",
        report_period="2026-09-30",
        official_facts={"revenue": 3_111_164_418.0},
        fallback_facts={"revenue": 3_300_000_000.0},
        official_source="cninfo",
        fallback_source="akshare",
        scope=("revenue",),
    )
    assert len(records) == 1
    record = records[0]
    assert record.record_kind == RECORD_KIND_SOURCE_DIFFERENCE
    assert record.official_value == 3_111_164_418.0
    assert record.fallback_value == 3_300_000_000.0
    assert record.relative_diff == pytest.approx(0.0572, abs=1e-3)


def test_sanity_passes_on_plausible_minority_interest():
    record = evaluate_single_sourced_sanity(
        instrument_id="002254.SZ",
        report_period="2026-09-30",
        filled_fact="equity_parent",
        filled_value=7_105_855_388.0,
        fallback_source="akshare",
        reference_fact="equity_total",
        reference_value=7_935_173_800.0,
        reference_source="cninfo",
    )
    assert record is None


def test_sanity_flags_negative_implied_minority():
    record = evaluate_single_sourced_sanity(
        instrument_id="002254.SZ",
        report_period="2026-09-30",
        filled_fact="equity_parent",
        filled_value=8_500_000_000.0,
        fallback_source="akshare",
        reference_fact="equity_total",
        reference_value=7_935_173_800.0,
        reference_source="cninfo",
    )
    assert record is not None
    assert record.record_kind == RECORD_KIND_SANITY_REVIEW
    assert record.detail["implied_minority_interest"] == pytest.approx(
        -564_826_200.0
    )


def test_sanity_flags_excessive_minority_share():
    record = evaluate_single_sourced_sanity(
        instrument_id="002254.SZ",
        report_period="2026-09-30",
        filled_fact="equity_parent",
        filled_value=3_000_000_000.0,
        fallback_source="akshare",
        reference_fact="equity_total",
        reference_value=7_935_173_800.0,
        reference_source="cninfo",
        minority_share_max=0.5,
    )
    assert record is not None
    assert record.record_kind == RECORD_KIND_SANITY_REVIEW


def test_summarize_counts_and_bounds_samples():
    records = compare_official_and_fallback_facts(
        instrument_id="002254.SZ",
        report_period="2026-09-30",
        official_facts={"revenue": 100.0, "total_assets": 200.0},
        fallback_facts={"revenue": 150.0, "total_assets": 200.0},
        official_source="cninfo",
        fallback_source="akshare",
        scope=("revenue", "total_assets"),
    )
    summary = summarize_comparison_records(records, report_top_n=5)
    assert summary["compared_records"] == 1
    assert summary["source_differences"] == 1
    assert summary["sanity_reviews"] == 0
    assert summary["samples"][0]["canonical_fact"] == "revenue"
    assert summary["samples"][0]["official_source"] == "cninfo"
    assert DEFAULT_ABSOLUTE_FLOOR_CNY == 1.0
