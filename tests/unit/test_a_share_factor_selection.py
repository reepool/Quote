from datetime import date

import pytest

from data_sources.a_share_factor_selection import (
    build_continuity_segments,
    build_three_source_canonical_candidate,
    compare_segment_paths,
    normalize_legacy_composite_rows,
)
from utils.adjustment import AdjustmentEngine


def _row(
    instrument_id,
    ex_date,
    factor,
    source,
    profile=None,
):
    return {
        "instrument_id": instrument_id,
        "ex_date": ex_date,
        "factor": factor,
        "source": source,
        "source_profile": profile or f"{source}_unit",
    }


def _candidate(
    *,
    cninfo,
    tdx,
    legacy,
    complete=None,
    zero_event_complete=None,
    lineage=None,
    lifecycle=None,
    special=None,
    factor_relative_tolerance=0.001,
):
    return build_three_source_canonical_candidate(
        cninfo_rows=cninfo,
        tdx_rows=tdx,
        legacy_rows=legacy,
        target_instruments=["000001.SZ"],
        series_version="unit__staging",
        start_date=date(2020, 1, 1),
        end_date=date(2022, 12, 31),
        complete_instruments_by_source=complete or {
            "cninfo": ["000001.SZ"],
            "tdx": ["000001.SZ"],
            "legacy": ["000001.SZ"] if legacy else [],
        },
        zero_event_complete_instruments_by_source=(
            zero_event_complete or {}
        ),
        lineage_by_instrument=lineage,
        lifecycle_bounds_by_instrument=lifecycle,
        special_event_dates_by_instrument=special,
        sessions_by_exchange={
            "SZSE": [
                date(2021, 5, 27),
                date(2021, 5, 28),
                date(2021, 5, 31),
            ]
        },
        factor_relative_tolerance=factor_relative_tolerance,
        cumulative_relative_tolerance=factor_relative_tolerance,
    )


def test_legacy_composite_normalizes_cumulative_levels_to_event_ratios():
    rows = normalize_legacy_composite_rows([
        {
            "instrument_id": "600018.SH",
            "ex_date": date(2006, 10, 26),
            "factor": 9.778728,
            "cumulative_factor": 9.778728,
            "source": "baostock",
        },
        {
            "instrument_id": "600018.SH",
            "ex_date": date(2007, 6, 22),
            "factor": 9.864413,
            "cumulative_factor": 9.864413,
            "source": "baostock",
        },
        {
            "instrument_id": "600018.SH",
            "ex_date": date(2008, 1, 1),
            "factor": 9.864413,
            "cumulative_factor": 9.864413,
            "source": "baostock",
        },
    ])

    assert [row["factor"] for row in rows] == pytest.approx([
        9.778728,
        9.864413 / 9.778728,
    ])
    assert rows[1]["upstream_source"] == "baostock"
    assert rows[1]["source_profile"] == "baostock_sina_legacy_composite"


def test_legacy_composite_uses_stored_factor_for_unrebased_source_switch():
    rows = normalize_legacy_composite_rows([
        {
            "instrument_id": "000636.SZ",
            "ex_date": date(2025, 7, 11),
            "factor": 9.991164,
            "cumulative_factor": 9.991164,
            "source": "baostock",
        },
        {
            "instrument_id": "000636.SZ",
            "ex_date": date(2026, 7, 10),
            "factor": 1.001539,
            "cumulative_factor": 18.551202,
            "source": "akshare",
        },
    ])

    assert rows[1]["factor"] == pytest.approx(1.001539)
    assert rows[1]["cumulative_factor"] == pytest.approx(
        9.991164 * 1.001539
    )
    assert rows[1]["provider_cumulative_factor"] == pytest.approx(18.551202)
    assert rows[1]["legacy_basis_conflict"] is True
    assert rows[1]["legacy_normalization_method"] == (
        "stored_factor_at_source_switch"
    )


def test_legacy_composite_keeps_rebased_source_switch_on_cumulative_chain():
    first_cumulative = 16.383859
    event_factor = 1.028404
    rows = normalize_legacy_composite_rows([
        {
            "instrument_id": "600018.SH",
            "ex_date": date(2025, 7, 17),
            "factor": first_cumulative,
            "cumulative_factor": first_cumulative,
            "source": "baostock",
        },
        {
            "instrument_id": "600018.SH",
            "ex_date": date(2026, 7, 23),
            "factor": event_factor,
            "cumulative_factor": first_cumulative * event_factor,
            "source": "akshare",
        },
    ])

    assert rows[1]["factor"] == pytest.approx(event_factor)
    assert rows[1]["cumulative_factor"] == pytest.approx(
        first_cumulative * event_factor
    )
    assert rows[1]["legacy_basis_conflict"] is False
    assert rows[1]["legacy_normalization_method"] == "cumulative_ratio"


def test_legacy_composite_continues_after_unrebased_source_switch():
    rows = normalize_legacy_composite_rows([
        {
            "instrument_id": "000636.SZ",
            "ex_date": date(2025, 7, 11),
            "factor": 9.991164,
            "cumulative_factor": 9.991164,
            "source": "baostock",
        },
        {
            "instrument_id": "000636.SZ",
            "ex_date": date(2026, 7, 10),
            "factor": 1.001539,
            "cumulative_factor": 18.551202,
            "source": "akshare",
        },
        {
            "instrument_id": "000636.SZ",
            "ex_date": date(2027, 7, 9),
            "factor": 1.01,
            "cumulative_factor": 18.551202 * 1.01,
            "source": "akshare",
        },
    ])

    assert [row["factor"] for row in rows[1:]] == pytest.approx([
        1.001539,
        1.01,
    ])
    assert rows[2]["cumulative_factor"] == pytest.approx(
        9.991164 * 1.001539 * 1.01
    )


def test_legacy_composite_rejects_switch_without_valid_stored_factor():
    rows = normalize_legacy_composite_rows([
        {
            "instrument_id": "000001.SZ",
            "ex_date": date(2019, 1, 1),
            "factor": 10.0,
            "cumulative_factor": 10.0,
            "source": "baostock",
        },
        {
            "instrument_id": "000001.SZ",
            "ex_date": date(2020, 1, 1),
            "factor": 0.0,
            "cumulative_factor": 18.0,
            "source": "akshare",
        },
    ])

    assert rows[1]["normalized_factor"] is None
    assert rows[1]["legacy_basis_conflict"] is True
    assert rows[1]["legacy_normalization_method"] == (
        "invalid_source_switch_factor"
    )
    assert rows[1]["legacy_cumulative_ratio"] == pytest.approx(1.8)


def test_legacy_composite_rejects_initial_non_baostock_without_event_factor():
    rows = normalize_legacy_composite_rows([{
        "instrument_id": "000001.SZ",
        "ex_date": date(2020, 1, 1),
        "factor": 0.0,
        "cumulative_factor": 18.0,
        "source": "akshare",
    }])

    assert rows[0]["normalized_factor"] is None
    assert rows[0]["legacy_basis_conflict"] is False
    assert rows[0]["legacy_normalization_method"] == (
        "invalid_initial_source_factor"
    )
    assert rows[0]["provider_cumulative_factor"] == pytest.approx(18.0)


def test_continuity_segments_split_non_continuous_transition():
    segments = build_continuity_segments(
        instrument_id="600018.SH",
        start_date=date(2001, 1, 1),
        end_date=date(2007, 1, 1),
        lineage={
            "transitions": [{
                "effective_date": "2006-10-26",
                "price_continuity": "non_continuous",
            }]
        },
    )

    assert [(item["start_date"], item["end_date"]) for item in segments] == [
        (date(2001, 1, 1), date(2006, 10, 25)),
        (date(2006, 10, 26), date(2007, 1, 1)),
    ]
    assert segments[1]["reset_at_start"] is True


def test_pairwise_agreement_accepts_trading_session_shift():
    comparison = compare_segment_paths(
        [_row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")],
        [_row("000001.SZ", date(2021, 5, 31), 1.1, "tdx")],
        market_sessions=[
            date(2021, 5, 28),
            date(2021, 5, 31),
        ],
        max_session_shift=1,
    )

    assert comparison["agrees"] is True
    assert comparison["event_matches"] == 1
    assert comparison["exact_matches"] == 0
    assert comparison["shifted_matches"] == 1
    assert comparison["factor_difference_buckets"]["le_0_01_pct"] == 1


def test_pairwise_agreement_aligns_adjacent_shifted_events_globally():
    comparison = compare_segment_paths(
        [
            _row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo"),
            _row("000001.SZ", date(2021, 5, 31), 1.2, "cninfo"),
        ],
        [
            _row("000001.SZ", date(2021, 5, 27), 1.1, "tdx"),
            _row("000001.SZ", date(2021, 5, 28), 1.2, "tdx"),
        ],
        market_sessions=[
            date(2021, 5, 27),
            date(2021, 5, 28),
            date(2021, 5, 31),
        ],
        max_session_shift=1,
    )

    assert comparison["agrees"] is True
    assert comparison["event_matches"] == 2
    assert comparison["exact_matches"] == 0
    assert comparison["shifted_matches"] == 2


def test_pairwise_agreement_rejects_offsetting_event_errors():
    comparison = compare_segment_paths(
        [
            _row("000001.SZ", date(2020, 1, 2), 1.1, "cninfo"),
            _row("000001.SZ", date(2021, 1, 2), 1 / 1.1, "cninfo"),
        ],
        [
            _row("000001.SZ", date(2020, 1, 2), 1.2, "tdx"),
            _row("000001.SZ", date(2021, 1, 2), 1 / 1.2, "tdx"),
        ],
    )

    assert comparison["agrees"] is False
    assert comparison["event_conflicts"] == 2
    assert comparison["factor_difference_buckets"]["gt_1_pct"] == 2


def test_selector_prefers_cninfo_for_three_source_consensus():
    rows = [_row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")]
    candidate, summary = _candidate(
        cninfo=rows,
        tdx=[{**rows[0], "source": "tdx"}],
        legacy=[{**rows[0], "source": "baostock"}],
    )

    assert summary["selection_counts"] == {"cninfo": 1}
    assert summary["confidence_counts"] == {"high": 1}
    assert candidate[0]["selected_source"] == "cninfo"
    assert candidate[0]["evidence_count"] == 3


def test_selector_uses_independent_consensus_for_ordinary_segment():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.05, "cninfo")
        ],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "baostock")
        ],
    )

    assert summary["selection_counts"] == {"tdx": 1}
    assert summary["confidence_counts"] == {"independent_consensus": 1}
    assert candidate[0]["selected_source"] == "tdx"


def test_selector_keeps_cninfo_for_governed_special_segment():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.05, "cninfo")
        ],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "akshare")
        ],
        special={"000001.SZ": [date(2021, 5, 28)]},
    )

    assert summary["confidence_counts"] == {"governed_special": 1}
    assert candidate[0]["selected_source"] == "cninfo"


def test_selector_uses_low_confidence_cninfo_when_all_disagree():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.05, "cninfo")
        ],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[
            _row("000001.SZ", date(2021, 5, 28), 1.2, "baostock")
        ],
    )

    assert summary["low_confidence_segment_count"] == 1
    assert summary["conflict_samples"][0]["selected_source"] == "cninfo"
    assert candidate[0]["quality_status"] == "low"


def test_selector_blocks_incomplete_cninfo():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[
            _row("000001.SZ", date(2021, 5, 28), 1.2, "baostock")
        ],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": ["000001.SZ"],
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 1
    assert summary["promotion_eligible"] is False


def test_selector_uses_independent_consensus_when_cninfo_is_incomplete():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "baostock")
        ],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": ["000001.SZ"],
        },
    )

    assert summary["blocked_segment_count"] == 0
    assert summary["selection_counts"] == {"tdx": 1}
    assert candidate[0]["selected_source"] == "tdx"


def test_selector_does_not_treat_missing_legacy_as_zero_event_vote():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")
        ],
        tdx=[],
        legacy=[],
    )

    assert len(candidate) == 1
    assert summary["blocked_segment_count"] == 0
    assert summary["selection_counts"] == {"cninfo": 1}
    assert summary["confidence_counts"] == {"low": 1}
    assert summary["decisions"][0]["reason"] == (
        "no_eligible_consensus_cninfo_fallback"
    )
    assert summary["decisions"][0]["eligible_sources"] == [
        "cninfo",
    ]


def test_selector_requires_segment_local_rows_without_zero_event_evidence():
    candidate, summary = build_three_source_canonical_candidate(
        cninfo_rows=[],
        tdx_rows=[],
        legacy_rows=[
            _row("600018.SH", date(2007, 6, 22), 1.01, "legacy")
        ],
        target_instruments=["600018.SH"],
        series_version="unit__staging",
        start_date=date(2001, 1, 1),
        end_date=date(2007, 12, 31),
        complete_instruments_by_source={
            "cninfo": [],
            "tdx": ["600018.SH"],
            "legacy": ["600018.SH"],
        },
        lineage_by_instrument={
            "600018.SH": {
                "transitions": [{
                    "effective_date": "2006-10-26",
                    "price_continuity": "non_continuous",
                }]
            }
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 2
    assert summary["decisions"][0]["eligible_sources"] == []
    assert summary["decisions"][1]["eligible_sources"] == ["legacy"]


def test_selector_accepts_explicit_complete_zero_event_source():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")
        ],
        tdx=[],
        legacy=[],
        zero_event_complete={"tdx": ["000001.SZ"]},
    )

    assert len(candidate) == 1
    assert summary["decisions"][0]["eligible_sources"] == [
        "cninfo",
        "tdx",
    ]


def test_selector_rejects_invalid_path_as_empty_consensus_vote():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 0.0, "tdx")
        ],
        legacy=[],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": [],
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 1
    assert summary["invalid_path_instruments_by_source"] == {
        "tdx": ["000001.SZ"]
    }
    assert summary["decisions"][0]["invalid_sources"] == ["tdx"]
    assert summary["decisions"][0]["eligible_sources"] == []


def test_selector_bounds_segment_to_instrument_lifecycle():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")
        ],
        tdx=[],
        legacy=[],
        complete={
            "cninfo": ["000001.SZ"],
            "tdx": [],
            "legacy": [],
        },
        lifecycle={
            "000001.SZ": {
                "listed_date": date(2021, 1, 4),
                "delisted_date": date(2021, 12, 31),
                "lifecycle_ended": True,
            }
        },
    )

    assert len(candidate) == 1
    assert summary["decisions"][0]["start_date"] == date(2021, 1, 4)
    assert summary["decisions"][0]["end_date"] == date(2021, 12, 31)


def test_selector_uses_tdx_single_source_for_completed_history():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": [],
        },
        lifecycle={
            "000001.SZ": {
                "listed_date": date(2020, 1, 1),
                "delisted_date": date(2021, 12, 31),
                "lifecycle_ended": True,
            }
        },
    )

    assert summary["blocked_segment_count"] == 0
    assert summary["historical_single_source_segment_count"] == 1
    assert summary["low_confidence_segment_count"] == 1
    assert summary["confidence_counts"] == {
        "historical_single_source": 1
    }
    assert candidate[0]["selected_source"] == "tdx"
    assert candidate[0]["quality_status"] == "historical_single_source"
    assert summary["decisions"][0]["reason"] == (
        "tdx_historical_single_source_fallback"
    )


def test_selector_treats_historical_tdx_events_as_contradicting_cninfo_zero_event():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[],
        complete={
            "cninfo": ["000001.SZ"],
            "tdx": ["000001.SZ"],
            "legacy": [],
        },
        zero_event_complete={"cninfo": ["000001.SZ"]},
        lifecycle={
            "000001.SZ": {
                "listed_date": date(2020, 1, 1),
                "delisted_date": date(2021, 12, 31),
                "lifecycle_ended": True,
            }
        },
    )

    assert summary["blocked_segment_count"] == 0
    assert summary["historical_single_source_segment_count"] == 1
    assert candidate[0]["selected_source"] == "tdx"
    assert summary["decisions"][0]["eligible_sources"] == [
        "cninfo",
        "tdx",
    ]
    assert summary["decisions"][0]["reason"] == (
        "tdx_historical_single_source_fallback"
    )


def test_selector_blocks_single_reference_for_active_lifecycle():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[],
        complete={
            "cninfo": ["000001.SZ"],
            "tdx": ["000001.SZ"],
            "legacy": [],
        },
        zero_event_complete={"cninfo": ["000001.SZ"]},
        lifecycle={
            "000001.SZ": {
                "listed_date": date(2020, 1, 1),
                "lifecycle_ended": False,
            }
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 1
    assert summary["historical_single_source_segment_count"] == 0
    assert summary["decisions"][0]["cninfo_empty_contradicted"] is True


def test_selector_does_not_use_historical_fallback_for_active_lineage_segment():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2020, 5, 28), 1.1, "tdx")
        ],
        legacy=[],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": [],
        },
        lineage={
            "000001.SZ": {
                "transitions": [{
                    "effective_date": "2022-01-01",
                    "price_continuity": "non_continuous",
                }]
            }
        },
        lifecycle={
            "000001.SZ": {
                "listed_date": date(2020, 1, 1),
                "lifecycle_ended": False,
            }
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 2
    assert summary["historical_single_source_segment_count"] == 0


def test_selector_blocks_disagreeing_historical_reference_sources():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "tdx")
        ],
        legacy=[
            _row("000001.SZ", date(2021, 5, 28), 1.2, "legacy")
        ],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": ["000001.SZ"],
        },
        lifecycle={
            "000001.SZ": {
                "listed_date": date(2020, 1, 1),
                "delisted_date": date(2021, 12, 31),
                "lifecycle_ended": True,
            }
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 1
    assert summary["historical_single_source_segment_count"] == 0


def test_selector_resets_cumulative_at_lineage_boundary():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2020, 5, 28), 1.1, "cninfo"),
            _row("000001.SZ", date(2022, 5, 28), 1.2, "cninfo"),
        ],
        tdx=[],
        legacy=[],
        complete={
            "cninfo": ["000001.SZ"],
            "tdx": [],
            "legacy": [],
        },
        lineage={
            "000001.SZ": {
                "transitions": [{
                    "effective_date": "2022-01-01",
                    "price_continuity": "non_continuous",
                }]
            }
        },
    )

    assert summary["segment_count"] == 2
    assert [row["cumulative_factor"] for row in candidate] == pytest.approx([
        1.1,
        1.2,
    ])
    assert [row["segment_id"] for row in candidate] == [
        "000001.SZ:1",
        "000001.SZ:2",
    ]


def test_selector_excludes_provider_factor_at_non_continuous_boundary():
    boundary_row = _row(
        "000001.SZ", date(2022, 1, 1), 1.5, "tdx"
    )
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[boundary_row],
        legacy=[{**boundary_row, "source": "baostock"}],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "legacy": ["000001.SZ"],
        },
        lineage={
            "000001.SZ": {
                "transitions": [{
                    "effective_date": "2022-01-01",
                    "price_continuity": "non_continuous",
                }]
            }
        },
    )

    assert candidate == []
    second_segment = summary["decisions"][1]
    assert second_segment["source_event_counts"] == {
        "cninfo": 0,
        "tdx": 0,
        "legacy": 0,
    }
    assert second_segment["excluded_boundary_event_counts"] == {
        "tdx": 1,
        "legacy": 1,
    }


def test_adjustment_engine_keeps_non_continuous_segments_independent():
    continuity_segments = [
        {
            "segment_id": "600018.SH:1",
            "start_date": "2001-01-01",
            "end_date": "2006-10-25",
        },
        {
            "segment_id": "600018.SH:2",
            "start_date": "2006-10-26",
            "end_date": "2007-12-31",
        },
    ]
    factors = [
        {
            "ex_date": "2005-01-10",
            "cumulative_factor": 2.0,
            "continuity_segments": continuity_segments,
        },
        {
            "ex_date": "2007-01-10",
            "cumulative_factor": 3.0,
            "continuity_segments": continuity_segments,
        },
    ]
    quotes = [
        {"time": "2005-01-01", "close": 10.0, "volume": 100},
        {"time": "2006-10-25", "close": 20.0, "volume": 100},
        {"time": "2006-10-26", "close": 30.0, "volume": 100},
        {"time": "2007-01-10", "close": 40.0, "volume": 100},
        {"time": "2008-01-02", "close": 50.0, "volume": 100},
    ]

    forward = AdjustmentEngine.forward_adjust(
        quotes, factors, price_fields=("close",)
    )
    backward = AdjustmentEngine.backward_adjust(
        quotes, factors, price_fields=("close",)
    )

    assert [row["factor"] for row in forward] == [
        0.5,
        1.0,
        pytest.approx(1 / 3, rel=1e-6),
        1.0,
        1.0,
    ]
    assert [row["factor"] for row in backward] == [
        1.0,
        2.0,
        1.0,
        3.0,
        3.0,
    ]
