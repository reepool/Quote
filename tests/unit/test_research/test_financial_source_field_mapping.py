import pytest

from research.financial_source_field_mapping import (
    APPROVED_RELATIONSHIPS,
    MAPPING_VERSION,
    MAPPING_VERSION_V1,
    MAPPING_VERSION_V2,
    MAPPING_VERSION_V3,
    MAPPING_VERSION_V4,
    RELATIONSHIP_REJECTED,
    find_financial_source_field_mapping,
    get_financial_source_field_mappings,
)
from research.financial_fact_aliases import describe_financial_numeric_fact_name


def test_financial_source_field_mapping_returns_only_approved_core_entries():
    mappings = get_financial_source_field_mappings(
        profile="nonbank",
        approved_for_core=True,
    )

    assert mappings
    assert {mapping.profile for mapping in mappings} == {"nonbank"}
    assert all(mapping.relationship in APPROVED_RELATIONSHIPS for mapping in mappings)
    assert all(mapping.source_unit == mapping.canonical_unit for mapping in mappings)
    assert {mapping.canonical_unit for mapping in mappings} <= {"CNY", "CNY_per_share"}
    assert all(mapping.mapping_version == MAPPING_VERSION for mapping in mappings)


def test_financial_source_field_mapping_rejects_bank_cash_like_false_equivalent():
    mapping = find_financial_source_field_mapping(
        profile="bank",
        sina_field="现金及存放中央银行款项",
        ths_metric="total_cash",
    )

    assert mapping is not None
    assert mapping.relationship == RELATIONSHIP_REJECTED
    assert mapping.approved_for_core is False
    assert mapping.rejection_reason
    assert "not equivalent" in mapping.rejection_reason


def test_financial_source_field_mapping_rejects_unknown_version():
    with pytest.raises(ValueError, match="Unsupported financial source-field mapping version"):
        get_financial_source_field_mappings(mapping_version="unknown.v1")


def test_financial_source_field_mapping_preserves_v1_and_v2_history():
    v1_mapping = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="equity_parent",
        sina_field="归属于母公司股东权益合计",
        mapping_version=MAPPING_VERSION_V1,
    )
    v2_mapping = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="equity_parent",
        sina_field="归属于母公司股东权益合计",
        mapping_version=MAPPING_VERSION_V3,
    )
    bank_net_income = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="net_income_parent",
        sina_field="归属于母公司的净利润",
        mapping_version=MAPPING_VERSION,
    )
    v2_variant = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="equity_parent",
        sina_field="归属于母公司股东权益合计",
        mapping_version=MAPPING_VERSION_V2,
    )

    assert v1_mapping is None
    assert v2_mapping is not None
    assert v2_mapping.approved_for_core is True
    assert v2_mapping.evidence
    assert v2_variant is not None
    assert bank_net_income is not None
    assert bank_net_income.mapping_version == MAPPING_VERSION


def test_financial_source_field_mapping_current_includes_review_confirmed_nonbank_fields():
    accounts_payable = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="balance_sheet.accounts_payable",
        sina_field="应付账款",
        ths_metric="accounts_payable",
    )
    payable_notes_total = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="balance_sheet.payable_notes_and_accounts",
        sina_field="应付票据及应付账款",
        ths_metric="payable_notes_and_accounts",
    )
    basic_eps = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="profit_sheet.basic_eps",
        sina_field="基本每股收益",
        ths_metric="basic_eps",
    )
    diluted_eps = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="profit_sheet.diluted_eps",
        sina_field="稀释每股收益",
        ths_metric="diluted_eps",
    )
    net_income_variant = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="net_income_parent",
        sina_field="归属于母公司的净利润",
        ths_metric="parent_holder_net_profit",
    )
    financial_nonbank_equity_variant = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="equity_parent",
        sina_field="归属于母公司的股东权益合计",
        ths_metric="parent_holder_equity_total",
    )
    foreign_currency_component = find_financial_source_field_mapping(
        profile="nonbank",
        canonical_fact="profit_sheet.foreign_currency_translation_diff",
        sina_field="外币财务报表折算差额",
        ths_metric="other_common_profit",
    )

    assert accounts_payable is not None
    assert accounts_payable.semantic == "accounts_payable"
    assert payable_notes_total is not None
    assert payable_notes_total.semantic == "notes_and_accounts_payable"
    assert net_income_variant is not None
    assert net_income_variant.approved_for_core is True
    assert financial_nonbank_equity_variant is not None
    assert financial_nonbank_equity_variant.approved_for_core is True
    assert foreign_currency_component is None
    assert basic_eps is not None
    assert basic_eps.canonical_unit == "CNY_per_share"
    assert diluted_eps is not None
    assert diluted_eps.canonical_unit == "CNY_per_share"


def test_financial_source_field_mapping_returns_profile_specific_financial_nonbank_fields():
    securities_mapping = find_financial_source_field_mapping(
        profile="securities",
        canonical_fact="equity_parent",
        sina_field="归属于母公司的股东权益合计",
        ths_metric="parent_holder_equity_total",
    )
    insurance_mapping = find_financial_source_field_mapping(
        profile="insurance",
        canonical_fact="net_income_parent",
        sina_field="归属于母公司的净利润",
        ths_metric="parent_holder_net_profit",
    )

    assert securities_mapping is not None
    assert securities_mapping.profile == "securities"
    assert securities_mapping.approved_for_core is True
    assert insurance_mapping is not None
    assert insurance_mapping.profile == "insurance"
    assert insurance_mapping.approved_for_core is True


def test_financial_source_field_mapping_v4_materializes_profile_specific_catalogs():
    bank_accrued_wages = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="balance_sheet.accrued_wages",
        sina_field="应付职工薪酬",
        ths_metric="accrued_wages",
    )
    securities_accrued_wages = find_financial_source_field_mapping(
        profile="securities",
        canonical_fact="balance_sheet.accrued_wages",
        sina_field="应付职工薪酬",
        ths_metric="accrued_wages",
    )
    insurance_cash_flow = find_financial_source_field_mapping(
        profile="insurance",
        canonical_fact="cash_flow_sheet.tax_payments",
        sina_field="支付的各项税费",
        ths_metric="tax_payments",
    )
    bank_v3_gap = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="balance_sheet.accrued_wages",
        sina_field="应付职工薪酬",
        ths_metric="accrued_wages",
        mapping_version=MAPPING_VERSION_V3,
    )
    bank_v4_accrued_wages = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="balance_sheet.accrued_wages",
        sina_field="应付职工薪酬",
        ths_metric="accrued_wages",
        mapping_version=MAPPING_VERSION_V4,
    )

    assert bank_accrued_wages is not None
    assert bank_accrued_wages.profile == "bank"
    assert bank_accrued_wages.mapping_version == MAPPING_VERSION
    assert bank_accrued_wages.approved_for_core is True
    assert securities_accrued_wages is not None
    assert securities_accrued_wages.profile == "securities"
    assert insurance_cash_flow is not None
    assert insurance_cash_flow.profile == "insurance"
    assert bank_v3_gap is None
    assert bank_v4_accrued_wages is not None
    assert bank_v4_accrued_wages.mapping_version == MAPPING_VERSION_V4


def test_financial_source_field_mapping_v5_corrects_bank_loans_net_mapping():
    gross_mapping = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="balance_sheet.loans_payments_behalf",
        sina_field="发放贷款及垫款",
        ths_metric="loans_payments_behalf",
    )
    net_mapping = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="balance_sheet.loans_payments_behalf",
        sina_field="发放贷款及垫款净额",
        ths_metric="loans_payments_behalf",
    )
    historical_v4_gross_mapping = find_financial_source_field_mapping(
        profile="bank",
        canonical_fact="balance_sheet.loans_payments_behalf",
        sina_field="发放贷款及垫款",
        ths_metric="loans_payments_behalf",
        mapping_version=MAPPING_VERSION_V4,
    )

    assert gross_mapping is not None
    assert gross_mapping.approved_for_core is False
    assert gross_mapping.relationship == RELATIONSHIP_REJECTED
    assert gross_mapping.rejection_reason
    assert net_mapping is not None
    assert net_mapping.approved_for_core is True
    assert net_mapping.mapping_version == MAPPING_VERSION
    assert historical_v4_gross_mapping is not None
    assert historical_v4_gross_mapping.approved_for_core is True


def test_ths_core_aliases_are_registered_in_standard_fact_catalog():
    assert (
        describe_financial_numeric_fact_name("parent_holder_net_profit")[
            "canonical_fact_name"
        ]
        == "net_income_parent"
    )
    assert (
        describe_financial_numeric_fact_name("parent_holder_equity_total")[
            "canonical_fact_name"
        ]
        == "equity_parent"
    )
    assert (
        describe_financial_numeric_fact_name("act_cash_flow_net")[
            "canonical_fact_name"
        ]
        == "operating_cf"
    )
