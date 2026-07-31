from datetime import date

import pytest

from data_sources.a_share_factor_selection import (
    build_continuity_segments,
    build_three_source_canonical_candidate,
    compare_segment_paths,
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
    sina,
    complete=None,
    lineage=None,
    special=None,
):
    return build_three_source_canonical_candidate(
        cninfo_rows=cninfo,
        tdx_rows=tdx,
        sina_rows=sina,
        target_instruments=["000001.SZ"],
        series_version="unit__staging",
        start_date=date(2020, 1, 1),
        end_date=date(2022, 12, 31),
        complete_instruments_by_source=complete or {
            "cninfo": ["000001.SZ"],
            "tdx": ["000001.SZ"],
            "sina": ["000001.SZ"],
        },
        lineage_by_instrument=lineage,
        special_event_dates_by_instrument=special,
        sessions_by_exchange={
            "SZSE": [
                date(2021, 5, 27),
                date(2021, 5, 28),
                date(2021, 5, 31),
            ]
        },
    )


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


def test_selector_prefers_cninfo_for_three_source_consensus():
    rows = [_row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")]
    candidate, summary = _candidate(
        cninfo=rows,
        tdx=[{**rows[0], "source": "tdx"}],
        sina=[{**rows[0], "source": "sina"}],
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
        sina=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "sina")
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
        sina=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "sina")
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
        sina=[
            _row("000001.SZ", date(2021, 5, 28), 1.2, "sina")
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
        sina=[
            _row("000001.SZ", date(2021, 5, 28), 1.2, "sina")
        ],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "sina": ["000001.SZ"],
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
        sina=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "sina")
        ],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "sina": ["000001.SZ"],
        },
    )

    assert summary["blocked_segment_count"] == 0
    assert summary["selection_counts"] == {"tdx": 1}
    assert candidate[0]["selected_source"] == "tdx"


def test_selector_accepts_independent_zero_event_consensus():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2021, 5, 28), 1.1, "cninfo")
        ],
        tdx=[],
        sina=[],
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 0
    assert summary["selection_counts"] == {"tdx": 1}
    assert summary["decisions"][0]["reason"] == (
        "tdx_sina_consensus_over_cninfo"
    )


def test_selector_rejects_invalid_path_as_empty_consensus_vote():
    candidate, summary = _candidate(
        cninfo=[],
        tdx=[
            _row("000001.SZ", date(2021, 5, 28), 0.0, "tdx")
        ],
        sina=[],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "sina": ["000001.SZ"],
        },
    )

    assert candidate == []
    assert summary["blocked_segment_count"] == 1
    assert summary["invalid_path_instruments_by_source"] == {
        "tdx": ["000001.SZ"]
    }
    assert summary["decisions"][0]["invalid_sources"] == ["tdx"]
    assert summary["decisions"][0]["eligible_sources"] == ["sina"]


def test_selector_resets_cumulative_at_lineage_boundary():
    candidate, summary = _candidate(
        cninfo=[
            _row("000001.SZ", date(2020, 5, 28), 1.1, "cninfo"),
            _row("000001.SZ", date(2022, 5, 28), 1.2, "cninfo"),
        ],
        tdx=[],
        sina=[],
        complete={
            "cninfo": ["000001.SZ"],
            "tdx": [],
            "sina": [],
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
        sina=[{**boundary_row, "source": "sina"}],
        complete={
            "cninfo": [],
            "tdx": ["000001.SZ"],
            "sina": ["000001.SZ"],
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
        "sina": 0,
    }
    assert second_segment["excluded_boundary_event_counts"] == {
        "tdx": 1,
        "sina": 1,
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
