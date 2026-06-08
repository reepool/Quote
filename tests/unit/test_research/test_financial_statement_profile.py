import pytest

from research.financial_statement_profile import (
    resolve_financial_statement_profile,
)


def test_resolve_financial_statement_profile_prefers_explicit_profile():
    result = resolve_financial_statement_profile(
        explicit_profile="证券",
        industry_membership={"sw_l1_name": "银行"},
    )

    assert result.profile == "securities"
    assert result.confidence == "explicit"
    assert result.source == "explicit_profile"


@pytest.mark.parametrize(
    ("membership", "expected_profile"),
    [
        (
            {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "480301",
                "industry_name": "股份制银行Ⅲ",
                "sw_l1_name": "银行",
                "sw_l2_name": "股份制银行Ⅱ",
                "sw_l3_name": "股份制银行Ⅲ",
            },
            "bank",
        ),
        (
            {
                "instrument_id": "600030.SH",
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "490101",
                "industry_name": "证券Ⅲ",
                "sw_l1_name": "非银金融",
                "sw_l2_name": "证券Ⅱ",
                "sw_l3_name": "证券Ⅲ",
            },
            "securities",
        ),
        (
            {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "490201",
                "industry_name": "保险Ⅲ",
                "sw_l1_name": "非银金融",
                "sw_l2_name": "保险Ⅱ",
                "sw_l3_name": "保险Ⅲ",
            },
            "insurance",
        ),
        (
            {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "270106",
                "industry_name": "集成电路制造",
                "sw_l1_name": "电子",
                "sw_l2_name": "半导体",
                "sw_l3_name": "集成电路制造",
            },
            "nonbank",
        ),
    ],
)
def test_resolve_financial_statement_profile_from_strict_industry_membership(
    membership,
    expected_profile,
):
    result = resolve_financial_statement_profile(industry_membership=membership)

    assert result.profile == expected_profile
    assert result.confidence == "high"
    assert result.source == "industry_membership"
    assert result.evidence["taxonomy_version"] == "sw_2021"


def test_resolve_financial_statement_profile_uses_company_profile_when_industry_missing():
    result = resolve_financial_statement_profile(
        company_profile={
            "instrument_id": "600030.SH",
            "company_name": "样例证券股份有限公司",
            "short_name": "样例证券",
            "industry_raw": "证券",
            "sector_raw": "非银金融",
        }
    )

    assert result.profile == "securities"
    assert result.confidence == "medium"
    assert result.source == "company_profile"


def test_resolve_financial_statement_profile_rejects_shenwan_only_securities_candidate():
    result = resolve_financial_statement_profile(
        industry_membership={
            "instrument_id": "300059.SZ",
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "industry_code": "490101",
            "industry_name": "证券Ⅲ",
            "sw_l1_name": "非银金融",
            "sw_l2_name": "证券Ⅱ",
            "sw_l3_name": "证券Ⅲ",
        }
    )

    assert result.profile == "nonbank"
    assert result.reason == "securities_candidate_without_confirmed_broker_scope"
    assert result.evidence["listed_broker_dealer_scope"]["eligible"] is False


def test_resolve_financial_statement_profile_defaults_to_nonbank_without_evidence():
    result = resolve_financial_statement_profile(
        instrument={"instrument_id": "688981.SH", "name": "中芯国际"}
    )

    assert result.profile == "nonbank"
    assert result.confidence == "default"
    assert result.reason == "no_profile_evidence_available"


def test_resolve_financial_statement_profile_rejects_unknown_explicit_profile():
    with pytest.raises(ValueError, match="Unsupported financial statement profile"):
        resolve_financial_statement_profile(explicit_profile="trust")
