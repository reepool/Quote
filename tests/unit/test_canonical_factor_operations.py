from datetime import date

from data_sources.a_share_canonical_operations import (
    compact_canonical_report,
    qualify_composite_paths,
    summarize_canonical_decisions,
)


def _decision(confidence="high", source="cninfo"):
    return {
        "instrument_id": "000001.SZ",
        "segment_id": "000001.SZ:1",
        "start_date": date(1991, 4, 3),
        "end_date": date(2026, 7, 31),
        "selected_source": source,
        "confidence": confidence,
        "agreeing_pairs": ["cninfo__tdx"],
        "pairwise": {
            "cninfo__tdx": {
                "agrees": confidence != "blocked",
                "exact_matches": 2,
                "event_conflicts": int(confidence == "blocked"),
            }
        },
    }


def test_compact_report_removes_unbounded_decisions_and_keeps_samples():
    report = {
        "status": "candidate",
        "decisions": [_decision(), _decision("blocked", None)],
        "blocked_decisions": [_decision("blocked", None)],
    }

    compact = compact_canonical_report(report, sample_limit=1)

    assert "decisions" not in compact
    assert "blocked_decisions" not in compact
    assert compact["decision_count"] == 2
    assert len(compact["blocked_decision_samples"]) == 1
    assert compact["decision_storage"] == "adjustment_factor_decisions"


def test_compact_report_preserves_existing_blocked_samples_on_repeat():
    blocked_sample = _decision("blocked", None)
    report = {
        "report_format": "canonical_summary_v2",
        "decision_count": 2,
        "decision_storage": "adjustment_factor_decisions",
        "blocked_decision_samples": [blocked_sample],
    }

    compact = compact_canonical_report(report, sample_limit=1)

    assert compact["decision_count"] == 2
    assert compact["blocked_decision_samples"] == [blocked_sample]


def test_compact_report_recursively_bounds_reconciliation_details():
    report = {
        "decisions": [_decision()],
        "reconciliation": {
            "exact_matches": [{"event": index} for index in range(5)],
            "totals": {"exact_matches": 5},
        },
        "overall_completeness": {
            "all_instrument_ids": [f"{index:06d}.SZ" for index in range(5)],
        },
    }

    compact = compact_canonical_report(report, sample_limit=2)

    assert compact["reconciliation"]["exact_matches"] == [
        {"event": 0},
        {"event": 1},
    ]
    assert compact["reconciliation"]["exact_matches_total_count"] == 5
    assert compact["reconciliation"]["totals"]["exact_matches"] == 5
    assert compact["overall_completeness"][
        "all_instrument_ids_total_count"
    ] == 5


def test_summary_separates_blocked_low_and_complete_coverage():
    summary = summarize_canonical_decisions(
        [_decision(), _decision("blocked", None), _decision("low", "tdx")],
        instrument_statuses=[
            {"coverage_status": "complete_with_events"},
            {"coverage_status": "complete_no_events"},
        ],
    )

    assert summary["blocked_segment_count"] == 1
    assert summary["low_confidence_segment_count"] == 1
    assert summary["conflict_count"] == 1
    assert summary["coverage_ratio"] == 1.0
    assert summary["overall_completeness"]["status"] == "success"


def test_summary_excludes_incomplete_source_pairs_from_reconciliation():
    comparable = _decision()
    incomplete = _decision()
    incomplete["instrument_id"] = "000002.SZ"
    incomplete["segment_id"] = "000002.SZ:1"
    incomplete["pairwise"] = {
        "cninfo__tdx": {
            "reason": "source_incomplete",
            "agrees": False,
            "left_only": 3,
        }
    }

    summary = summarize_canonical_decisions([comparable, incomplete])
    reconciliation = summary["pairwise_reconciliation"]["cninfo__tdx"]

    assert reconciliation["compared_segments"] == 1
    assert reconciliation["exact_matches"] == 2
    assert reconciliation["left_only"] == 0
    assert "instruments" not in reconciliation
    assert "agreements" not in reconciliation


def test_composite_qualification_asserts_path_not_event_completeness():
    rows = [{
        "instrument_id": "000001.SZ",
        "ex_date": date(2020, 1, 1),
        "normalized_factor": 1.1,
        "cumulative_factor": 1.1,
        "upstream_source": "baostock",
        "composite_normalization_method": "initial_cumulative",
    }, {
        "instrument_id": "000002.SZ",
        "ex_date": date(2020, 1, 1),
        "normalized_factor": float("nan"),
        "cumulative_factor": 1.0,
        "upstream_source": "sina",
        "composite_normalization_method": "invalid_initial_source_factor",
    }]

    result = qualify_composite_paths(rows)

    assert result["000001.SZ"]["path_eligible"] is True
    assert result["000001.SZ"]["event_completeness"] == "not_asserted"
    assert result["000002.SZ"]["path_eligible"] is False
    assert result["000002.SZ"]["invalid_row_count"] == 1
