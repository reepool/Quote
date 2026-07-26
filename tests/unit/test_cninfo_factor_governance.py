from datetime import date

import pytest

from data_sources.cninfo_factor_governance import (
    CNINFO_FACTOR_PROFILE,
    TDX_FACTOR_PROFILE,
    build_cninfo_primary_candidate,
    build_quote_evidence_keys,
    derive_cninfo_factor_path,
    derive_tdx_factor_path,
    evaluate_coverage_intervals,
    reconcile_cninfo_tdx_events,
)


def test_segmented_coverage_intervals_merge_across_historical_and_rolling_rows():
    result = evaluate_coverage_intervals(
        [
            {
                "coverage_status": "complete_with_events",
                "requested_start_date": date(1990, 12, 19),
                "requested_end_date": date(2026, 7, 18),
            },
            {
                "coverage_status": "complete_no_events",
                "requested_start_date": date(2026, 7, 14),
                "requested_end_date": date(2026, 7, 21),
            },
        ],
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        accepted_statuses={"complete_with_events", "complete_no_events"},
    )

    assert result["covered"] is True
    assert result["gaps"] == []
    assert result["merged_intervals"] == [{
        "start_date": date(1990, 12, 19),
        "end_date": date(2026, 7, 21),
    }]


def test_coverage_intervals_preserve_real_gaps_and_ignore_failed_retries():
    result = evaluate_coverage_intervals(
        [
            {
                "coverage_status": "complete_with_events",
                "requested_start_date": date(2020, 1, 1),
                "requested_end_date": date(2020, 6, 30),
            },
            {
                "coverage_status": "indeterminate",
                "requested_start_date": date(2020, 1, 1),
                "requested_end_date": date(2020, 12, 31),
            },
            {
                "coverage_status": "complete_no_events",
                "requested_start_date": date(2020, 7, 2),
                "requested_end_date": date(2020, 12, 31),
            },
        ],
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        accepted_statuses={"complete_with_events", "complete_no_events"},
    )

    assert result["covered"] is False
    assert result["gaps"] == [{
        "start_date": date(2020, 7, 1),
        "end_date": date(2020, 7, 1),
    }]


def test_cninfo_factor_derivation_aggregates_same_day_economics():
    observations = [
        {
            "instrument_id": "000001.SZ",
            "source_event_key": "distribution",
            "ex_date": date(1993, 5, 24),
            "event_status": "implemented",
            "is_current": True,
            "cash_dividend_per_share": 0.3,
            "bonus_shares_per_share": 0.35,
            "capitalization_shares_per_share": 0.5,
        },
        {
            "instrument_id": "000001.SZ",
            "source_event_key": "rights",
            "ex_date": date(1993, 5, 24),
            "event_status": "implemented",
            "is_current": True,
            "rights_shares_per_share": 0.1,
            "rights_price": 16.0,
        },
    ]
    quote_evidence = [{
        "instrument_id": "000001.SZ",
        "source_date": date(1993, 5, 24),
        "effective_date": date(1993, 5, 24),
        "pre_close": 20.0,
        "close": 11.0,
    }]

    result = derive_cninfo_factor_path(observations, quote_evidence)

    assert result["pending"] == []
    assert len(result["events"]) == 1
    event = result["events"][0]
    assert event["cash_per_share"] == 0.3
    assert event["bonus_per_share"] == 0.85
    assert event["rights_per_share"] == 0.1
    assert event["rights_proceeds_per_share"] == 1.6
    assert event["rights_price"] == 16.0
    assert event["factor"] == pytest.approx(20 * 1.95 / 21.3)
    assert result["observations"][0]["source_profile"] == CNINFO_FACTOR_PROFILE


def test_cninfo_factor_derivation_shifts_non_trading_date_and_keeps_pending():
    observations = [
        {
            "instrument_id": "000004.SZ",
            "source_event_key": "weekend",
            "ex_date": date(1996, 7, 27),
            "event_status": "implemented",
            "is_current": True,
            "bonus_shares_per_share": 0.2,
        },
        {
            "instrument_id": "000004.SZ",
            "source_event_key": "failed",
            "ex_date": date(1996, 8, 1),
            "event_status": "failed",
            "is_current": True,
            "rights_shares_per_share": 0.3,
            "rights_price": 5.0,
        },
    ]
    quote_evidence = [{
        "instrument_id": "000004.SZ",
        "source_date": date(1996, 7, 27),
        "effective_date": date(1996, 7, 29),
        "pre_close": 7.19,
        "close": 6.3,
    }]

    result = derive_cninfo_factor_path(observations, quote_evidence)

    assert result["events"][0]["effective_date"] == date(1996, 7, 29)
    assert result["events"][0]["date_shifted"] is True
    assert result["events"][0]["factor"] == pytest.approx(1.2)
    assert result["pending"] == []


def test_unlocated_cninfo_event_blocks_factor_path():
    result = derive_cninfo_factor_path(
        [
            {
                "instrument_id": "000004.SZ",
                "source_event_key": "missing-date",
                "ex_date": None,
                "event_status": "announced_incomplete",
            },
            {
                "instrument_id": "000004.SZ",
                "source_event_key": "known-date",
                "ex_date": date(1996, 8, 1),
                "event_status": "implemented",
            },
        ],
        [{
            "instrument_id": "000004.SZ",
            "source_date": date(1996, 8, 1),
            "effective_date": date(1996, 8, 1),
            "pre_close": 10.0,
        }],
    )

    assert result["events"] == []
    assert any(
        item["reason"] == "prior_unlocated_event_pending"
        for item in result["pending"]
    )


def test_cninfo_partial_economic_fields_are_pending_not_zero_effect():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "partial-rights",
            "ex_date": date(2020, 5, 28),
            "event_status": "implemented",
            "quality_status": "partial_missing_economic_fields",
            "rights_shares_per_share": None,
            "rights_price": None,
        }],
        [{
            "instrument_id": "000001.SZ",
            "source_date": date(2020, 5, 28),
            "effective_date": date(2020, 5, 28),
            "pre_close": 10.0,
        }],
    )

    assert result["events"] == []
    assert result["pending"][0]["reason"] == "partial_missing_economic_fields"


def test_resolved_date_evidence_allows_missing_ex_date_event_only():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "600108.SH",
            "source_event_key": "special-action",
            "ex_date": None,
            "resolved_effective_date": date(2006, 6, 13),
            "resolved_date_basis": "official_resumption_date",
            "resolved_evidence_source": "cninfo_announcement_review",
            "resolved_evidence_key": "announcement-1",
            "event_status": "announced_incomplete",
            "quality_status": "partial_missing_ex_date",
            "bonus_shares_per_share": 0.68,
            "capitalization_shares_per_share": 0.34,
        }],
        [{
            "instrument_id": "600108.SH",
            "source_date": date(2006, 6, 13),
            "effective_date": date(2006, 6, 13),
            "pre_close": 10.0,
        }],
    )

    assert result["pending"] == []
    assert result["events"][0]["factor"] == pytest.approx(2.02)
    evidence = result["events"][0]["resolved_date_evidence"][0]
    assert evidence["date_basis"] == "official_resumption_date"


def test_authoritative_resolved_date_drives_quote_lookup_and_factor():
    observations = [{
        "instrument_id": "600449.SH",
        "source_event_key": "share-reform",
        "ex_date": date(2006, 7, 14),
        "resolved_effective_date": date(2006, 8, 15),
        "resolved_date_authoritative": True,
        "resolved_factor_effect": "normal",
        "event_status": "implemented",
        "quality_status": "structured_complete",
        "capitalization_shares_per_share": 0.172488,
        "is_current": True,
    }]

    assert build_quote_evidence_keys(observations, []) == [
        ("600449.SH", date(2006, 8, 15))
    ]

    result = derive_cninfo_factor_path(
        observations,
        [{
            "instrument_id": "600449.SH",
            "source_date": date(2006, 8, 15),
            "effective_date": date(2006, 8, 15),
            "pre_close": 10.0,
            "close": 8.5,
        }],
    )

    assert result["pending"] == []
    assert result["events"][0]["effective_date"] == date(2006, 8, 15)
    assert result["events"][0]["factor"] == pytest.approx(1.172488)


def test_candidate_only_metadata_does_not_resolve_missing_ex_date():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "600108.SH",
            "source_event_key": "special-action",
            "ex_date": None,
            "event_status": "announced_incomplete",
            "quality_status": "partial_missing_ex_date",
            "bonus_shares_per_share": 0.68,
            "capitalization_shares_per_share": 0.34,
        }],
        [],
    )

    assert result["events"] == []
    assert result["pending"][0]["reason"] == "missing_ex_date"


def test_pending_factor_blocks_later_cumulative_path():
    result = derive_cninfo_factor_path(
        [
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "early",
                "ex_date": date(2020, 5, 28),
                "event_status": "implemented",
            },
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "late",
                "ex_date": date(2020, 6, 28),
                "event_status": "implemented",
            },
        ],
        [
            {
                "instrument_id": "000001.SZ",
                "source_date": date(2020, 5, 28),
                "effective_date": date(2020, 5, 28),
                "pre_close": None,
            },
            {
                "instrument_id": "000001.SZ",
                "source_date": date(2020, 6, 28),
                "effective_date": date(2020, 6, 29),
                "pre_close": 10.0,
            },
        ],
    )

    assert result["events"] == []
    assert any(item["reason"] == "prior_event_pending" for item in result["pending"])


def test_tdx_pending_factor_blocks_later_cumulative_path():
    result = derive_tdx_factor_path(
        [
            {
                "instrument_id": "000001.SZ",
                "ex_date": date(2020, 5, 28),
                "factor": 1.0,
                "validation_result": "pending_factor_missing_pre_close",
            },
            {
                "instrument_id": "000001.SZ",
                "ex_date": date(2020, 6, 28),
                "factor": 1.1,
                "validation_result": "computed_unvalidated",
            },
        ],
        [
            {
                "instrument_id": "000001.SZ",
                "source_date": date(2020, 5, 28),
                "effective_date": date(2020, 5, 28),
            },
            {
                "instrument_id": "000001.SZ",
                "source_date": date(2020, 6, 28),
                "effective_date": date(2020, 6, 29),
            },
        ],
    )

    assert result["events"] == []
    assert any(item["reason"] == "prior_event_pending" for item in result["pending"])


def test_tdx_path_uses_event_product_and_effective_session():
    rows = [
        {
            "instrument_id": "000003.SZ",
            "ex_date": date(1996, 7, 27),
            "factor": 1.2,
            "validation_result": "computed_unvalidated",
            "songzhuangu": 2.0,
        }
    ]
    quote_evidence = [{
        "instrument_id": "000003.SZ",
        "source_date": date(1996, 7, 27),
        "effective_date": date(1996, 7, 29),
        "pre_close": 7.19,
        "close": 6.3,
    }]

    result = derive_tdx_factor_path(rows, quote_evidence)

    assert result["pending"] == []
    assert result["events"][0]["effective_date"] == date(1996, 7, 29)
    assert result["observations"][0]["source_profile"] == TDX_FACTOR_PROFILE
    assert result["observations"][0]["provider_cumulative_factor"] == 1.2


def test_reconciliation_and_candidate_keep_tdx_only_as_unverified_fallback():
    cninfo_events = [
        {
            "instrument_id": "000001.SZ",
            "source_ex_date": date(2020, 5, 28),
            "effective_date": date(2020, 5, 28),
            "cash_per_share": 0.218,
            "bonus_per_share": 0.0,
            "rights_per_share": 0.0,
            "rights_proceeds_per_share": 0.0,
            "factor": 1.01,
        }
    ]
    tdx_events = [
        {
            "instrument_id": "000001.SZ",
            "source_ex_date": date(2020, 5, 28),
            "effective_date": date(2020, 5, 28),
            "cash_per_share": 0.218,
            "bonus_per_share": 0.0,
            "rights_per_share": 0.0,
            "rights_price": 0.0,
            "factor": 1.01,
        },
        {
            "instrument_id": "000001.SZ",
            "source_ex_date": date(1991, 5, 2),
            "effective_date": date(1991, 5, 2),
            "cash_per_share": 0.3,
            "bonus_per_share": 0.4,
            "rights_per_share": 0.0,
            "rights_price": 0.0,
            "factor": 1.4,
        },
    ]

    reconciliation = reconcile_cninfo_tdx_events(cninfo_events, tdx_events)
    candidate, summary = build_cninfo_primary_candidate(
        cninfo_events,
        tdx_events,
        reconciliation,
        series_version="cninfo_primary_test",
    )

    assert reconciliation["totals"] == {
        "cninfo_events": 1,
        "tdx_events": 2,
        "exact_matches": 1,
        "rounded_matches": 0,
        "shifted_matches": 0,
        "accepted_authoritative_overrides": 0,
        "suppressed_reference_events": 0,
        "conflicts": 0,
        "cninfo_only": 0,
        "tdx_only": 1,
    }
    exact_match = reconciliation["exact_matches"][0]
    assert "rounded_field_tolerances" not in exact_match
    assert "precision_policy" not in exact_match
    assert len(candidate) == 2
    assert candidate[0]["quality_status"] == "tdx_fallback_unverified"
    assert candidate[1]["selected_source"] == "cninfo"
    assert summary["promotion_eligible"] is False


def test_same_date_tdx_source_rounding_is_reported_separately():
    cninfo_events = [{
        "instrument_id": "600108.SH",
        "source_ex_date": date(2006, 6, 14),
        "effective_date": date(2006, 6, 14),
        "cash_per_share": 0.03581058386488,
        "bonus_per_share": 1.02,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "rights_price": 0.0,
        "factor": 2.042366463463,
    }]
    tdx_events = [{
        "instrument_id": "600108.SH",
        "source_ex_date": date(2006, 6, 14),
        "effective_date": date(2006, 6, 14),
        "cash_per_share": 0.0360000014305115,
        "bonus_per_share": 1.01999998092651,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "rights_price": 0.0,
        "factor": 2.042486,
    }]

    result = reconcile_cninfo_tdx_events(cninfo_events, tdx_events)

    assert result["status"] == "success"
    assert result["totals"]["exact_matches"] == 0
    assert result["totals"]["rounded_matches"] == 1
    assert result["totals"]["conflicts"] == 0
    match = result["rounded_matches"][0]
    assert match["reason"] == "same_date_source_precision_match"
    assert match["differences"]["cash_per_share"] == pytest.approx(
        0.0001894175656315
    )
    assert match["factor_relative_difference"] == pytest.approx(0.00005855, rel=1e-3)
    policy = result["matching_policy"]["rounded_precision_policy"]
    assert policy["version"] == "tdx_xdxr_observed_precision_v2"
    assert match["rounded_field_tolerances"]["cash_per_share"] == 0.0005


def test_tdx_rights_ratio_and_price_use_separate_precision_allowances():
    cninfo_rights = 0.12345
    cninfo_price = 8.1234
    tdx_rights = 0.1234
    tdx_price = 8.12
    cninfo_events = [{
        "instrument_id": "000001.SZ",
        "source_ex_date": date(1993, 5, 24),
        "effective_date": date(1993, 5, 24),
        "cash_per_share": 0.0,
        "bonus_per_share": 0.0,
        "rights_per_share": cninfo_rights,
        "rights_proceeds_per_share": cninfo_rights * cninfo_price,
        "rights_price": cninfo_price,
        "factor": 1.1,
    }]
    tdx_events = [{
        "instrument_id": "000001.SZ",
        "source_ex_date": date(1993, 5, 24),
        "effective_date": date(1993, 5, 24),
        "cash_per_share": 0.0,
        "bonus_per_share": 0.0,
        "rights_per_share": tdx_rights,
        "rights_proceeds_per_share": tdx_rights * tdx_price,
        "rights_price": tdx_price,
        "factor": 1.1,
    }]

    result = reconcile_cninfo_tdx_events(cninfo_events, tdx_events)

    assert result["totals"]["rounded_matches"] == 1
    match = result["rounded_matches"][0]
    assert match["differences"]["rights_price"] == pytest.approx(0.0034)
    assert match["rounded_field_tolerances"]["rights_price"] == 0.005
    assert (
        match["differences"]["rights_proceeds_per_share"]
        <= match["rounded_field_tolerances"]["rights_proceeds_per_share"]
    )


def test_tdx_observed_decimal_precision_tightens_cash_allowance():
    cninfo_events = [{
        "instrument_id": "000001.SZ",
        "source_ex_date": date(2020, 5, 28),
        "effective_date": date(2020, 5, 28),
        "cash_per_share": 0.0336,
        "bonus_per_share": 0.0,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "factor": 1.00001,
    }]
    tdx_events = [{
        **cninfo_events[0],
        "cash_per_share": 0.0335,
        "factor": 1.00001,
    }]

    result = reconcile_cninfo_tdx_events(
        cninfo_events,
        tdx_events,
        field_tolerance=0.00001,
    )

    assert result["totals"]["rounded_matches"] == 0
    assert result["totals"]["conflicts"] == 1
    assert result["conflicts"][0]["rounded_field_tolerances"][
        "cash_per_share"
    ] == pytest.approx(0.00005)


def test_unused_rights_price_does_not_create_a_false_conflict():
    base_event = {
        "instrument_id": "000001.SZ",
        "source_ex_date": date(2020, 5, 28),
        "effective_date": date(2020, 5, 28),
        "cash_per_share": 0.218,
        "bonus_per_share": 0.0,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "factor": 1.01,
    }

    result = reconcile_cninfo_tdx_events(
        [{**base_event, "rights_price": 0.0}],
        [{**base_event, "rights_price": 10.0}],
    )

    assert result["totals"]["exact_matches"] == 1
    assert result["totals"]["conflicts"] == 0


def test_rounded_fields_with_material_factor_difference_remain_conflict():
    cninfo_events = [{
        "instrument_id": "600108.SH",
        "source_ex_date": date(2006, 6, 14),
        "effective_date": date(2006, 6, 14),
        "cash_per_share": 0.03581,
        "bonus_per_share": 1.02,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "rights_price": 0.0,
        "factor": 2.042366,
    }]
    tdx_events = [{
        **cninfo_events[0],
        "cash_per_share": 0.036,
        "factor": 2.05,
    }]

    result = reconcile_cninfo_tdx_events(cninfo_events, tdx_events)

    assert result["status"] == "partial"
    assert result["totals"]["rounded_matches"] == 0
    assert result["totals"]["conflicts"] == 1
    assert result["conflicts"][0]["factor_relative_difference"] > 0.0001


def test_shifted_date_difference_is_not_accepted_as_rounding():
    cninfo_events = [{
        "instrument_id": "000001.SZ",
        "source_ex_date": date(2020, 5, 28),
        "effective_date": date(2020, 5, 28),
        "cash_per_share": 0.2183,
        "bonus_per_share": 0.0,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "rights_price": 0.0,
        "factor": 1.01,
    }]
    tdx_events = [{
        **cninfo_events[0],
        "source_ex_date": date(2020, 5, 29),
        "effective_date": date(2020, 5, 29),
        "cash_per_share": 0.218,
    }]

    result = reconcile_cninfo_tdx_events(
        cninfo_events,
        tdx_events,
        sessions_by_exchange={
            "SZSE": [date(2020, 5, 28), date(2020, 5, 29)]
        },
    )

    assert result["totals"]["rounded_matches"] == 0
    assert result["totals"]["shifted_matches"] == 0
    assert result["totals"]["conflicts"] == 1
    assert result["conflicts"][0]["reason"] == "shifted_economic_conflict"


def test_candidate_keeps_cninfo_primary_when_tdx_only_shares_effective_date():
    cninfo_events = [{
        "instrument_id": "000001.SZ",
        "source_ex_date": date(2020, 5, 28),
        "effective_date": date(2020, 5, 28),
        "factor": 1.01,
    }]
    tdx_events = [{
        "instrument_id": "000001.SZ",
        "source_ex_date": date(2020, 5, 27),
        "effective_date": date(2020, 5, 28),
        "factor": 1.02,
    }]
    reconciliation = {
        "conflicts": [],
        "tdx_only": [{**tdx_events[0], "tdx_index": 0}],
    }

    candidate, _ = build_cninfo_primary_candidate(
        cninfo_events, tdx_events, reconciliation,
        series_version="cninfo_primary_test",
    )

    assert len(candidate) == 1
    assert candidate[0]["selected_source"] == "cninfo"


def test_candidate_does_not_restore_explicit_cninfo_no_effect_event_from_tdx():
    tdx_events = [{
        "instrument_id": "000035.SZ",
        "source_ex_date": date(2012, 11, 30),
        "effective_date": date(2012, 11, 30),
        "factor": 1.2596,
    }]
    reconciliation = {
        "conflicts": [],
        "tdx_only": [{**tdx_events[0], "tdx_index": 0}],
    }

    candidate, summary = build_cninfo_primary_candidate(
        [],
        tdx_events,
        reconciliation,
        series_version="cninfo_primary_test",
        excluded_cninfo_events=[{
            "instrument_id": "000035.SZ",
            "effective_date": "2012-11-30",
            "suppressed_dates": ["2012-11-29", "2012-11-30"],
            "reason": "resolved_factor_effect_none",
        }],
    )

    assert candidate == []
    assert summary["tdx_fallback_count"] == 0
    assert summary["cninfo_no_effect_exclusion_count"] == 2


def test_reconciliation_suppresses_reference_for_cninfo_no_effect_event():
    tdx_events = [{
        "instrument_id": "000035.SZ",
        "source_ex_date": date(2012, 11, 30),
        "effective_date": date(2012, 11, 30),
        "factor": 1.2596,
    }]

    result = reconcile_cninfo_tdx_events(
        [],
        tdx_events,
        excluded_cninfo_events=[{
            "instrument_id": "000035.SZ",
            "effective_date": "2012-11-30",
            "suppressed_dates": ["2012-11-30"],
        }],
    )

    assert result["status"] == "success"
    assert result["tdx_only"] == []
    assert result["totals"]["suppressed_reference_events"] == 1


def test_authoritative_cninfo_override_does_not_create_reconciliation_conflict():
    cninfo_events = [{
        "instrument_id": "600449.SH",
        "source_ex_date": date(2006, 8, 15),
        "effective_date": date(2006, 8, 15),
        "cash_per_share": 0.0,
        "bonus_per_share": 0.172488,
        "rights_per_share": 0.0,
        "rights_proceeds_per_share": 0.0,
        "rights_price": 0.0,
        "factor": 1.172488,
        "authoritative_override": True,
    }]
    tdx_events = [{
        **cninfo_events[0],
        "bonus_per_share": 0.442,
        "factor": 1.442,
        "authoritative_override": False,
    }]

    result = reconcile_cninfo_tdx_events(cninfo_events, tdx_events)

    assert result["status"] == "success"
    assert result["totals"]["conflicts"] == 0
    assert result["totals"]["accepted_authoritative_overrides"] == 1
    assert result["accepted_authoritative_overrides"][0][
        "reason"
    ] == "authoritative_cninfo_override"


def test_authoritative_cninfo_only_event_is_not_unresolved():
    result = reconcile_cninfo_tdx_events(
        [{
            "instrument_id": "000519.SZ",
            "source_ex_date": date(2016, 5, 11),
            "effective_date": date(2016, 5, 11),
            "cash_per_share": 0.0,
            "bonus_per_share": 0.08189,
            "rights_per_share": 0.0,
            "rights_proceeds_per_share": 0.0,
            "rights_price": 0.0,
            "factor": 1.08189,
            "authoritative_override": True,
        }],
        [],
    )

    assert result["status"] == "success"
    assert result["cninfo_only"] == []
    assert result["totals"]["accepted_authoritative_overrides"] == 1
    assert result["accepted_authoritative_overrides"][0][
        "reason"
    ] == "authoritative_cninfo_event_only"
