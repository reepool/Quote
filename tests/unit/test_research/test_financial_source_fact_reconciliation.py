"""Tests for the official-versus-fallback source fact reconciliation service."""

import pytest

from research.financial_source_fact_reconciliation import (
    DEFAULT_ABSOLUTE_FLOOR_CNY,
    RECORD_KIND_SANITY_REVIEW,
    RECORD_KIND_SOURCE_DIFFERENCE,
    compare_official_and_fallback_facts,
    evaluate_single_sourced_sanity,
    merge_reconciliation_summaries,
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


def _summary_fixture(**overrides):
    summary = {
        "enabled": True,
        "scope_canonical_facts": ["revenue", "equity_parent"],
        "compared_facts": 4,
        "consistent_facts": 3,
        "source_differences": 1,
        "sanity_reviews": 1,
        "merged_instruments": 2,
        "merged_official_fields": 8,
        "filled_fallback_fields": 2,
        "cleaned_fallback_facts": 8,
        "targets_with_official_rows": 2,
        "official_present_core_fact_counts": {"revenue": 2},
        "samples": [{"instrument_id": "A"}],
        "sanity_samples": [{"instrument_id": "A"}],
    }
    summary.update(overrides)
    return summary


def test_merge_summaries_adds_counts_and_caps_samples():
    merged = merge_reconciliation_summaries(
        [
            _summary_fixture(),
            _summary_fixture(
                compared_facts=2,
                consistent_facts=2,
                source_differences=0,
                official_present_core_fact_counts={"revenue": 1, "equity_parent": 1},
                samples=[{"instrument_id": "B"}, {"instrument_id": "C"}],
                sanity_samples=[],
            ),
            None,
        ],
        report_top_n=2,
    )
    assert merged["enabled"] is True
    assert merged["scope_canonical_facts"] == ["revenue", "equity_parent"]
    assert merged["compared_facts"] == 6
    assert merged["consistent_facts"] == 5
    assert merged["source_differences"] == 1
    assert merged["merged_official_fields"] == 16
    assert merged["official_present_core_fact_counts"] == {
        "revenue": 3,
        "equity_parent": 1,
    }
    assert [sample["instrument_id"] for sample in merged["samples"]] == ["A", "B"]
    assert [sample["instrument_id"] for sample in merged["sanity_samples"]] == ["A"]


def test_merge_summaries_keeps_disabled_fill_counts_and_handles_empty():
    merged = merge_reconciliation_summaries(
        [
            {"enabled": False, "merged_official_fields": 3},
            {"enabled": False, "merged_official_fields": 1},
        ]
    )
    assert merged["enabled"] is False
    assert merged["merged_official_fields"] == 4
    assert merged["compared_facts"] == 0
    assert merge_reconciliation_summaries([None, {}]) is None
