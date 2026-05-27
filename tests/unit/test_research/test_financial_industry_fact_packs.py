from research.financial_industry_fact_packs import (
    INDUSTRY_FACT_PACK_VERSION,
    PACK_STATUS_APPROVED,
    PACK_STATUS_NOT_YET_APPROVED,
    build_industry_pack_payload,
    get_financial_industry_fact_pack,
    get_financial_industry_fact_pack_status,
)


def test_bank_industry_pack_exposes_only_approved_bank_specific_facts():
    entries = get_financial_industry_fact_pack(profile="bank")

    facts = {entry.canonical_fact for entry in entries}
    assert "balance_sheet.loans_payments_behalf" in facts
    assert "balance_sheet.deposits_and_deposits" in facts
    assert "profit_sheet.interest_income" in facts
    assert all(entry.profile == "bank" for entry in entries)
    assert all(entry.approval_status == PACK_STATUS_APPROVED for entry in entries)
    assert all(entry.required_for_core is False for entry in entries)
    assert all(entry.pack_version == INDUSTRY_FACT_PACK_VERSION for entry in entries)


def test_securities_and_insurance_industry_packs_are_explicit_placeholders():
    securities_status = get_financial_industry_fact_pack_status(profile="securities")
    insurance_status = get_financial_industry_fact_pack_status(profile="insurance")

    assert get_financial_industry_fact_pack(profile="securities") == []
    assert get_financial_industry_fact_pack(profile="insurance") == []
    assert securities_status["status"] == PACK_STATUS_NOT_YET_APPROVED
    assert insurance_status["status"] == PACK_STATUS_NOT_YET_APPROVED


def test_industry_pack_missing_is_optional_and_not_a_core_blocker():
    payload = build_industry_pack_payload(
        instrument_id="600000.SH",
        report_period="2026-03-31",
        profile="bank",
        local_fact_result={
            "instrument_id": "600000.SH",
            "report_period": "2026-03-31",
            "profile": "bank",
            "mapping_version": "sina_ths_core_financial_facts.v5",
            "facts": {
                "balance_sheet.deposits_and_deposits": {"fact_value": 1.0},
            },
        },
    )

    assert payload["is_optional"] is True
    assert payload["status"] == "partial"
    assert payload["facts"]["balance_sheet.deposits_and_deposits"]["fact_value"] == 1.0
    assert {
        item["reason"] for item in payload["missing_fields"]
    } == {"industry_pack_missing"}


def test_not_yet_approved_profile_reports_policy_status_not_source_missing():
    payload = build_industry_pack_payload(
        instrument_id="600030.SH",
        report_period="2026-03-31",
        profile="securities",
        local_fact_result=None,
    )

    assert payload["is_optional"] is True
    assert payload["status"] == "partial"
    assert payload["facts"] == {}
    assert payload["missing_fields"] == [
        {
            "canonical_fact": None,
            "reason": "industry_pack_not_yet_approved",
            "profile": "securities",
            "pack_version": INDUSTRY_FACT_PACK_VERSION,
            "report_period": "2026-03-31",
        }
    ]
