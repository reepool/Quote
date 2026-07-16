import pytest
from data_manager import DataManager
from research.dcf_input_governance import (
    derive_cash_and_debt,
    derive_capital_expenditure,
    enrich_instrument_with_industry,
    estimate_a_share_filing_deadline,
    resolve_financial_availability,
    select_financial_bundle_as_of,
)


def _financial_bundle(report_period, capex, **extra):
    return {
        "instrument_id": "601088.SH",
        "report_period": report_period,
        "facts": {
            "cash_flow_sheet": {
                "pay_fixed_assets_etc_cash": capex,
            }
        },
        **extra,
    }


def test_a_share_filing_deadline_estimates_are_conservative():
    assert estimate_a_share_filing_deadline("2025-03-31") == "2025-04-30"
    assert estimate_a_share_filing_deadline("2025-06-30") == "2025-08-31"
    assert estimate_a_share_filing_deadline("2025-09-30") == "2025-10-31"
    assert estimate_a_share_filing_deadline("2025-12-31") == "2026-04-30"


def test_financial_bundle_selection_uses_actual_date_before_estimate_and_cutoff():
    bundles = [
        _financial_bundle("2025-12-31", 100.0),
        _financial_bundle(
            "2025-09-30",
            70.0,
            publish_date="2025-10-20",
        ),
    ]

    before_annual_deadline = select_financial_bundle_as_of(
        bundles,
        valuation_date="2026-04-20",
        exchange="SSE",
    )
    after_annual_deadline = select_financial_bundle_as_of(
        bundles,
        valuation_date="2026-05-01",
        exchange="SSE",
    )

    assert before_annual_deadline["report_period"] == "2025-09-30"
    assert before_annual_deadline["data_available_date_estimated"] is False
    assert after_annual_deadline["report_period"] == "2025-12-31"
    assert after_annual_deadline["data_available_date"] == "2026-04-30"
    assert after_annual_deadline["data_available_date_estimated"] is True
    assert resolve_financial_availability(
        after_annual_deadline,
        exchange="SSE",
    )["estimated"] is True


def test_capex_derivation_supports_annual_and_ttm_bridge():
    bundles = [
        _financial_bundle("2026-06-30", 70.0),
        _financial_bundle("2025-12-31", 120.0),
        _financial_bundle("2025-06-30", 50.0),
    ]

    annual = derive_capital_expenditure(
        bundles,
        selected_report_period="2025-12-31",
    )
    ttm = derive_capital_expenditure(
        bundles,
        selected_report_period="2026-06-30",
    )

    assert annual["value"] == 120.0
    assert annual["method"] == "annual_reported_cash_outflow"
    assert ttm["value"] == 140.0
    assert ttm["method"] == "ttm_cumulative_cash_flow_bridge"
    assert ttm["lineage_hash"]


def test_capex_ttm_bridge_fails_closed_when_comparable_period_is_missing():
    result = derive_capital_expenditure(
        [
            _financial_bundle("2026-06-30", 70.0),
            _financial_bundle("2025-12-31", 120.0),
        ],
        selected_report_period="2026-06-30",
    )

    assert result["status"] == "unavailable"
    assert result["value"] is None
    assert result["missing_reason"] == "ttm_capex_bridge_incomplete"


def test_cash_and_debt_excludes_total_liabilities_alias():
    context = derive_cash_and_debt(
        {
            "facts": {
                "balance_sheet": {
                    "total_cash": "118585000000",
                    "short_term_loans": "86700000000",
                    "year_non_current_debt": "8174000000",
                    "long_term_loan": "29673000000",
                    "lease_debt": "996000000",
                    "total_debt": "227396000000",
                }
            }
        }
    )

    assert context["cash_and_equivalents"] == 118_585_000_000.0
    assert context["total_debt"] == 124_547_000_000.0
    assert context["lease_liabilities"] == 996_000_000.0
    assert context["total_debt"] != 227_396_000_000.0
    assert "balance_sheet.total_debt" in context["excluded_fields"]


def test_authoritative_shenwan_membership_enriches_dcf_instrument():
    enriched = enrich_instrument_with_industry(
        {"instrument_id": "601088.SH", "exchange": "SSE"},
        {
            "taxonomy_system": "sw",
            "taxonomy_version": "2021",
            "industry_code": "410101",
            "industry_name": "动力煤",
            "industry_level": 3,
            "mapping_status": "authoritative",
            "sw_l1_code": "410000",
            "sw_l1_name": "煤炭",
            "sw_l2_code": "410100",
            "sw_l2_name": "煤炭开采",
            "sw_l3_code": "410101",
            "sw_l3_name": "动力煤",
        },
    )

    assert enriched["sw_l1_name"] == "煤炭"
    assert enriched["sw_l3_name"] == "动力煤"
    assert enriched["dcf_industry_mapping_status"] == "authoritative"


@pytest.mark.asyncio
async def test_data_manager_enriches_point_in_time_shares_and_local_risk_free_rate(
    monkeypatch,
):
    async def sync_to_thread(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", sync_to_thread)

    class Storage:
        def get_latest_valuation_input(self, instrument_id, **kwargs):
            assert instrument_id == "601088.SH"
            assert kwargs["as_of_date"] == "2026-07-14"
            return {
                "shares_outstanding": 19_889_620_000.0,
                "as_of_date": "2026-06-30",
                "data_as_of": "2026-07-01",
                "source": "local_valuation_input",
                "source_mode": "local",
                "input_kind": "shares_outstanding",
                "unit": "shares",
            }

        def list_risk_free_rate_series(self):
            return [
                {
                    "series_id": "china_treasury_10y",
                    "unit": "percent_annual",
                    "currency": "CNY",
                    "tenor": "10Y",
                    "source_profile": "china_bond_10y",
                    "source": "akshare",
                    "updated_at": "2026-07-15T00:00:00",
                }
            ]

        def get_risk_free_rate_observations(self, series_id, **kwargs):
            assert series_id == "china_treasury_10y"
            assert kwargs["end_date"] == "2026-07-14"
            assert kwargs["start_date"] == "2026-06-13"
            return [
                {
                    "observation_date": "2026-07-14",
                    "value": 1.7402,
                    "source": "akshare",
                    "source_mode": "direct",
                }
            ]

    manager = object.__new__(DataManager)
    bundle = await manager._enrich_dcf_bundle_with_local_shares(
        Storage(),
        "601088.SH",
        {"latest_facts": {}},
        valuation_date="2026-07-14",
    )
    rate = await manager._get_dcf_risk_free_rate_context(
        Storage(),
        valuation_date="2026-07-14",
        exchange="SSE",
        currency="CNY",
    )

    assert bundle["shares_outstanding"] == 19_889_620_000.0
    assert bundle["lineage"]["shares_outstanding"]["lineage_hash"]
    assert rate["value"] == 0.017402
    assert rate["database"] == "interests.db"
    assert rate["as_of_date"] == "2026-07-14"
    assert rate["lineage_hash"]
