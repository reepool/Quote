from research.financial_industry_fact_packs import (
    INDUSTRY_FACT_PACK_VERSION,
    PACK_STATUS_APPROVED,
    build_industry_pack_payload,
    get_financial_industry_fact_pack,
    get_financial_industry_fact_pack_status,
)


def test_bank_industry_pack_exposes_only_approved_bank_specific_facts():
    entries = get_financial_industry_fact_pack(profile="bank")

    facts = {entry.canonical_fact for entry in entries}
    assert "balance_sheet.loans_payments_behalf" in facts
    assert "balance_sheet.customer_deposits" in facts
    assert "balance_sheet.interbank_deposits" in facts
    assert "balance_sheet.deposits_and_deposits" in facts
    assert "profit_sheet.interest_income" in facts
    assert all(entry.profile == "bank" for entry in entries)
    assert all(entry.approval_status == PACK_STATUS_APPROVED for entry in entries)
    assert all(entry.required_for_core is False for entry in entries)
    assert all(entry.pack_version == INDUSTRY_FACT_PACK_VERSION for entry in entries)


def test_securities_and_insurance_industry_packs_are_approved_profile_scoped_packs():
    securities_status = get_financial_industry_fact_pack_status(profile="securities")
    insurance_status = get_financial_industry_fact_pack_status(profile="insurance")
    securities_facts = {
        entry.canonical_fact for entry in get_financial_industry_fact_pack(profile="securities")
    }
    insurance_facts = {
        entry.canonical_fact for entry in get_financial_industry_fact_pack(profile="insurance")
    }

    assert securities_status["status"] == PACK_STATUS_APPROVED
    assert insurance_status["status"] == PACK_STATUS_APPROVED
    assert {
        entry.profile for entry in get_financial_industry_fact_pack(profile="securities")
    } == {"securities"}
    assert {
        entry.profile for entry in get_financial_industry_fact_pack(profile="insurance")
    } == {"insurance"}
    assert "balance_sheet.agent_trading_security" in securities_facts
    assert "profit_sheet.net_fee_commission_income" in securities_facts
    assert "balance_sheet.advance_premiums" in insurance_facts
    assert "profit_sheet.withdrawal_insurance_money" in insurance_facts
    assert "balance_sheet.agent_trading_security" not in insurance_facts
    assert "balance_sheet.advance_premiums" not in securities_facts


def test_industry_pack_derives_bank_deposit_total_from_cninfo_components():
    payload = build_industry_pack_payload(
        instrument_id="600000.SH",
        report_period="2026-03-31",
        profile="bank",
        local_fact_result={
            "instrument_id": "600000.SH",
            "report_period": "2026-03-31",
            "profile": "bank",
            "mapping_version": "sina_ths_core_financial_facts.v5",
            "facts": {},
        },
        numeric_fact_rows=[
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "report_period": "2026-03-31",
                "fact_name": "吸收存款",
                "source": "cninfo",
                "source_mode": "direct",
                "fact_value": 10.0,
                "raw_fact": {},
            },
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "report_period": "2026-03-31",
                "fact_name": "同业存放及其他金融机构存放款项",
                "source": "cninfo",
                "source_mode": "direct",
                "fact_value": 3.0,
                "raw_fact": {},
            },
        ],
    )

    assert payload["is_optional"] is True
    assert payload["facts"]["balance_sheet.customer_deposits"]["fact_value"] == 10.0
    assert payload["facts"]["balance_sheet.interbank_deposits"]["fact_value"] == 3.0
    assert payload["facts"]["balance_sheet.deposits_and_deposits"]["fact_value"] == 13.0
    assert (
        payload["facts"]["balance_sheet.deposits_and_deposits"]["raw_fact"][
            "industry_pack_mapping"
        ]["relationship"]
        == "derived_equivalent"
    )


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
            "facts": {},
        },
    )

    assert payload["is_optional"] is True
    assert payload["status"] == "partial"
    assert {
        item["reason"] for item in payload["missing_fields"]
    } == {"industry_pack_missing"}


def test_securities_pack_reads_exact_raw_fields_without_polluting_common_facts():
    payload = build_industry_pack_payload(
        instrument_id="600030.SH",
        report_period="2026-03-31",
        profile="securities",
        local_fact_result={
            "instrument_id": "600030.SH",
            "report_period": "2026-03-31",
            "profile": "securities",
            "mapping_version": "sina_ths_core_financial_facts.v5",
            "facts": {
                "balance_sheet.trade_financial_assets": {"fact_value": 20.0},
            },
        },
        numeric_fact_rows=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "report_period": "2026-03-31",
                "fact_name": "代理买卖证券款",
                "source": "cninfo",
                "source_mode": "direct",
                "fact_value": 100.0,
                "raw_fact": {},
            },
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
                "report_period": "2026-03-31",
                "fact_name": "手续费及佣金净收入",
                "source": "cninfo",
                "source_mode": "direct",
                "fact_value": 8.0,
                "raw_fact": {},
            },
        ],
    )

    assert payload["is_optional"] is True
    assert payload["status"] == "partial"
    assert payload["facts"]["balance_sheet.trade_financial_assets"]["fact_value"] == 20.0
    assert payload["facts"]["balance_sheet.agent_trading_security"]["fact_value"] == 100.0
    assert payload["facts"]["profit_sheet.net_fee_commission_income"]["fact_value"] == 8.0
    assert (
        payload["facts"]["balance_sheet.agent_trading_security"]["raw_fact"][
            "industry_pack_mapping"
        ]["relationship"]
        == "exact_equivalent"
    )
    assert "industry_pack_missing" in {
        item["reason"] for item in payload["missing_fields"]
    }


def test_insurance_pack_reads_exact_raw_fields():
    payload = build_industry_pack_payload(
        instrument_id="601318.SH",
        report_period="2026-03-31",
        profile="insurance",
        local_fact_result={
            "instrument_id": "601318.SH",
            "report_period": "2026-03-31",
            "profile": "insurance",
            "mapping_version": "sina_ths_core_financial_facts.v5",
            "facts": {
                "balance_sheet.debt_investment": {"fact_value": 30.0},
            },
        },
        numeric_fact_rows=[
            {
                "instrument_id": "601318.SH",
                "symbol": "601318",
                "exchange": "SSE",
                "report_period": "2026-03-31",
                "fact_name": "预收保费",
                "source": "cninfo",
                "source_mode": "direct",
                "fact_value": 7.0,
                "raw_fact": {},
            },
            {
                "instrument_id": "601318.SH",
                "symbol": "601318",
                "exchange": "SSE",
                "report_period": "2026-03-31",
                "fact_name": "定期存款",
                "source": "cninfo",
                "source_mode": "direct",
                "fact_value": 11.0,
                "raw_fact": {},
            },
        ],
    )

    assert payload["is_optional"] is True
    assert payload["status"] == "partial"
    assert payload["facts"]["balance_sheet.debt_investment"]["fact_value"] == 30.0
    assert payload["facts"]["balance_sheet.advance_premiums"]["fact_value"] == 7.0
    assert payload["facts"]["balance_sheet.time_deposits"]["fact_value"] == 11.0


def test_unsupported_profile_reports_policy_status_not_source_missing():
    payload = build_industry_pack_payload(
        instrument_id="000001.SZ",
        report_period="2026-03-31",
        profile="nonbank",
        local_fact_result=None,
    )

    assert payload["is_optional"] is True
    assert payload["status"] == "partial"
    assert payload["facts"] == {}
    assert payload["missing_fields"] == [
        {
            "canonical_fact": None,
            "reason": "industry_pack_unsupported_profile",
            "profile": "nonbank",
            "pack_version": INDUSTRY_FACT_PACK_VERSION,
            "report_period": "2026-03-31",
        }
    ]
