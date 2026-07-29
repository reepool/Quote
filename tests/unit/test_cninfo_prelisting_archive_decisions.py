from scripts.dev_validation import (
    apply_cninfo_prelisting_archive_decisions as batch,
)


def test_fixed_archive_manifests_are_complete_and_disjoint():
    pre_listing_keys = {
        item[1] for item in batch.PRE_LISTING_SPECS
    }
    non_effective_keys = {
        item[1] for item in batch.NON_EFFECTIVE_SPECS
    }
    review_keys = {
        item[1] for item in batch.REVIEW_SPECS
    }

    assert len(batch.PRE_LISTING_SPECS) == 45
    assert len(batch.NON_EFFECTIVE_SPECS) == 17
    assert len(batch.REVIEW_SPECS) == 10
    assert not pre_listing_keys & non_effective_keys
    assert not (pre_listing_keys | non_effective_keys) & review_keys
    assert batch._hash_lines(
        pre_listing_keys | non_effective_keys
    ) == batch.EXPECTED_DECISION_EVENT_KEYS_HASH
    assert batch._hash_lines(
        review_keys
    ) == batch.EXPECTED_REVIEW_EVENT_KEYS_HASH


def test_review_workbook_frames_keep_operator_fields_blank():
    row = {
        "instrument_id": "000055.SZ",
        "instrument_name": "Test",
        "source_event_key": "event-1",
        "fiscal_period": "1995年报",
        "listed_date": "1996-04-15",
        "announcement_date": "1996-06-08",
        "record_date": None,
        "pay_date": None,
        "share_arrival_date": None,
        "cash_dividend_per_share": 0.12,
        "bonus_shares_per_share": 0.0,
        "capitalization_shares_per_share": 0.0,
        "rights_shares_per_share": None,
        "rights_price": None,
        "description": "10派1.2元",
        "resolution_state": "official_archive_unavailable",
    }

    summary, review, nearby, fields = batch.build_workbook_frames(
        [row], {"000055.SZ": []}
    )

    assert len(summary) == 6
    assert len(review) == 1
    assert nearby.empty
    assert len(fields) == 5
    assert review.iloc[0]["用户决定"] == ""
    assert review.iloc[0]["核准除权日"] == ""
    assert review.iloc[0]["factor_effect"] == ""
    assert review.iloc[0]["用户说明"] == ""
