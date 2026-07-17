from datetime import date

import pytest

from data_sources.cninfo_factor_governance import (
    CNINFO_FACTOR_PROFILE,
    TDX_FACTOR_PROFILE,
    build_cninfo_primary_candidate,
    derive_cninfo_factor_path,
    derive_tdx_factor_path,
    reconcile_cninfo_tdx_events,
)


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
        "shifted_matches": 0,
        "conflicts": 0,
        "cninfo_only": 0,
        "tdx_only": 1,
    }
    assert len(candidate) == 2
    assert candidate[0]["quality_status"] == "tdx_fallback_unverified"
    assert candidate[1]["selected_source"] == "cninfo"
    assert summary["promotion_eligible"] is False


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
