from datetime import date

import pytest

from data_sources.cninfo_factor_governance import derive_cninfo_factor_path


def test_reviewed_economic_overlay_unblocks_partial_missing_fields_event():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
            "ex_date": date(2026, 6, 12),
            "cash_dividend_per_share": 0.236,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
            "rights_shares_per_share": None,
            "rights_price": None,
            "quality_status": "partial_missing_economic_fields",
            "resolved_economic_terms": True,
            "resolved_economic_fields": ["cash_dividend_per_share"],
            "event_status": "implemented",
            "is_current": True,
        }],
        [{
            "instrument_id": "000001.SZ",
            "source_date": date(2026, 6, 12),
            "effective_date": date(2026, 6, 12),
            "pre_close": 10.0,
            "close": 10.1,
        }],
    )
    assert result["pending"] == []
    assert len(result["events"]) == 1
    assert result["events"][0]["cash_per_share"] == 0.236


def test_partial_rights_event_stays_pending_until_ratio_and_price_are_complete():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_allotment",
            "action_type": "rights",
            "ex_date": date(1991, 8, 1),
            "rights_shares_per_share": 0.3,
            "rights_price": None,
            "quality_status": "partial_missing_economic_fields",
            "resolved_economic_terms": True,
            "resolved_economic_fields": ["rights_shares_per_share"],
            "event_status": "implemented",
            "is_current": True,
        }],
        [{
            "instrument_id": "000001.SZ",
            "source_date": date(1991, 8, 1),
            "effective_date": date(1991, 8, 1),
            "pre_close": 20.0,
        }],
    )
    assert result["events"] == []
    assert result["pending"][0]["reason"] == "partial_missing_economic_fields"


def test_reviewed_bonus_can_expand_an_incomplete_cash_event_to_mixed():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
            "ex_date": date(2026, 6, 12),
            "cash_dividend_per_share": 0.2,
            "bonus_shares_per_share": 0.1,
            "quality_status": "partial_missing_fields",
            "resolved_economic_terms": True,
            "resolved_economic_fields": ["bonus_shares_per_share"],
            "event_status": "implemented",
            "is_current": True,
        }],
        [{
            "instrument_id": "000001.SZ",
            "source_date": date(2026, 6, 12),
            "effective_date": date(2026, 6, 12),
            "pre_close": 10.0,
        }],
    )
    assert result["pending"] == []
    assert result["events"][0]["cash_per_share"] == pytest.approx(0.2)
    assert result["events"][0]["bonus_per_share"] == pytest.approx(0.1)


def test_reviewed_nonzero_term_unblocks_zero_effect_placeholder():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "distribution",
            "ex_date": date(2026, 6, 12),
            "cash_dividend_per_share": 0.236,
            "quality_status": "partial_zero_effect",
            "resolved_economic_terms": True,
            "resolved_economic_fields": ["cash_dividend_per_share"],
            "event_status": "implemented",
            "is_current": True,
        }],
        [{
            "instrument_id": "000001.SZ",
            "source_date": date(2026, 6, 12),
            "effective_date": date(2026, 6, 12),
            "pre_close": 10.0,
        }],
    )
    assert result["pending"] == []
    assert result["events"][0]["cash_per_share"] == pytest.approx(0.236)


def test_resolved_date_does_not_hide_missing_economic_terms():
    result = derive_cninfo_factor_path(
        [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "distribution",
            "ex_date": None,
            "resolved_effective_date": date(2026, 6, 12),
            "cash_dividend_per_share": 0.0,
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.0,
            "quality_status": "partial_missing_ex_date",
            "event_status": "announced_incomplete",
            "is_current": True,
        }],
        [{
            "instrument_id": "000001.SZ",
            "source_date": date(2026, 6, 12),
            "effective_date": date(2026, 6, 12),
            "pre_close": 10.0,
        }],
    )
    assert result["events"] == []
    assert result["pending"][0]["reason"] == "partial_missing_economic_fields"
