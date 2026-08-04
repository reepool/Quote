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


@pytest.mark.asyncio
async def test_special_commodity_dcf_context_is_company_and_valuation_date_bound(
    monkeypatch,
):
    async def sync_to_thread(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", sync_to_thread)

    class Storage:
        def get_series(self, series_id):
            assert series_id == "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY"
            return {
                "series_id": series_id,
                "commodity_id": "CN.CHEMICAL.SODA_ASH.SPOT",
                "currency": "CNY",
                "unit": "CNY/ton",
                "active": True,
            }

        def read_observations(
            self,
            *,
            series_id,
            end_date=None,
            start_date=None,
            available_at_lte=None,
        ):
            assert series_id == "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY"
            assert end_date == "2025-12-31"
            assert start_date == "2023-01-01"
            assert available_at_lte == "2025-12-31T23:59:59.999999+08:00"
            return [
                {
                    "series_id": series_id,
                    "observation_date": "2025-06-30",
                    "value": 1000.0,
                    "currency": "CNY",
                    "unit": "CNY/ton",
                    "source_profile": "100ppi",
                    "available_at": "2025-07-01T10:00:00+08:00",
                    "availability_quality": "local_first_seen_timestamp",
                },
                {
                    "series_id": series_id,
                    "observation_date": "2025-12-31",
                    "value": 1200.0,
                    "currency": "CNY",
                    "unit": "CNY/ton",
                    "source_profile": "100ppi",
                    "available_at": "2025-12-31T10:00:00+08:00",
                    "availability_quality": "local_first_seen_timestamp",
                },
                {
                    "series_id": series_id,
                    "observation_date": "2026-01-02",
                    "value": 3000.0,
                    "currency": "CNY",
                    "unit": "CNY/ton",
                    "source_profile": "100ppi",
                },
            ]

    manager = object.__new__(DataManager)
    manager.research_config = type(
        "Config",
        (),
        {
            "modules": {
                "commodity_market_data": {
                    "special_commodity_market_data": {"enabled": True}
                }
            }
        },
    )()
    manager._require_special_commodity_storage = lambda: Storage()
    context = await manager._get_dcf_special_commodity_context(
        valuation_date="2025-12-31",
        target_currency="CNY",
        business_profile_context={
            "instrument_id": "600001.SH",
            "executable_exposure_mappings": [
                {
                    "mapping_id": "business-profile:exposure-soda-ash-cost",
                    "source": "approved_company_business_profile",
                    "market_data_family": "special_commodity",
                    "exposure_role": "revenue",
                    "direction": "positive",
                    "revenue_series_id": (
                        "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY"
                    ),
                    "cost_series_ids": [],
                }
            ],
        },
    )

    assert context["status"] == "ready"
    assert context["mapping_scope_id"] == "600001.SH"
    assert context["selected_mapping"]["exposure_role"] == "revenue"
    assert context["latest_observation"]["observation_date"] == "2025-12-31"
    assert context["commodity_price_assumption"] == 1200.0
    assert context["diagnostic"]["observation_count"] == 2
    assert context["diagnostic"]["percentile"] == 1.0
    assert context["diagnostic"]["availability_quality"] == (
        "local_first_seen_timestamp"
    )
    assert context["lineage_hash"]


@pytest.mark.asyncio
async def test_special_commodity_dcf_context_blocks_without_governed_availability(
    monkeypatch,
):
    async def sync_to_thread(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", sync_to_thread)

    class Storage:
        def get_series(self, series_id):
            return {
                "series_id": series_id,
                "currency": "CNY",
                "unit": "CNY/ton",
                "active": True,
            }

        def read_observations(self, **kwargs):
            assert kwargs["available_at_lte"].startswith("2025-12-31T23:59:59")
            return []

    manager = object.__new__(DataManager)
    manager.research_config = type(
        "Config",
        (),
        {
            "modules": {
                "commodity_market_data": {
                    "special_commodity_market_data": {"enabled": True}
                }
            }
        },
    )()
    manager._require_special_commodity_storage = lambda: Storage()

    context = await manager._get_dcf_special_commodity_context(
        valuation_date="2025-12-31",
        target_currency="CNY",
        business_profile_context={
            "instrument_id": "600001.SH",
            "executable_exposure_mappings": [
                {
                    "source": "approved_company_business_profile",
                    "market_data_family": "special_commodity",
                    "exposure_role": "revenue",
                    "direction": "positive",
                    "revenue_series_id": "CMD.TEST.PIT",
                    "cost_series_ids": [],
                }
            ],
        },
    )

    assert context["status"] == "blocked"
    assert "temporal_availability_missing:CMD.TEST.PIT" in context["blockers"]
    assert context["commodity_price_assumption"] is None


@pytest.mark.asyncio
async def test_special_commodity_dcf_context_ignores_global_series_without_approval():
    manager = object.__new__(DataManager)
    manager.research_config = type(
        "Config",
        (),
        {
            "modules": {
                "commodity_market_data": {
                    "special_commodity_market_data": {"enabled": True}
                }
            }
        },
    )()

    context = await manager._get_dcf_special_commodity_context(
        valuation_date="2025-12-31",
        target_currency="CNY",
        business_profile_context={"executable_exposure_mappings": []},
    )

    assert context is None


@pytest.mark.asyncio
async def test_special_commodity_cost_leg_requires_approved_spread(monkeypatch):
    async def sync_to_thread(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", sync_to_thread)

    class Storage:
        def get_series(self, series_id):
            return {"series_id": series_id, "active": True}

        def read_observations(self, **kwargs):
            return [
                {
                    "series_id": kwargs["series_id"],
                    "observation_date": "2025-12-31",
                    "value": 1200.0,
                    "currency": "CNY",
                    "source_profile": "100ppi",
                }
            ]

    manager = object.__new__(DataManager)
    manager.research_config = type(
        "Config",
        (),
        {
            "modules": {
                "commodity_market_data": {
                    "special_commodity_market_data": {"enabled": True}
                }
            }
        },
    )()
    manager._require_special_commodity_storage = lambda: Storage()

    context = await manager._get_dcf_special_commodity_context(
        valuation_date="2025-12-31",
        target_currency="CNY",
        business_profile_context={
            "instrument_id": "600001.SH",
            "executable_exposure_mappings": [
                {
                    "source": "approved_company_business_profile",
                    "market_data_family": "special_commodity",
                    "exposure_role": "feedstock_cost",
                    "direction": "negative",
                    "revenue_series_id": None,
                    "cost_series_ids": [
                        "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY"
                    ],
                    "spread_ids": [],
                }
            ],
        },
    )

    assert context["status"] == "blocked"
    assert context["commodity_price_assumption"] is None
    assert context["cycle_index_level"] is None
    assert "cost_only_special_commodity_requires_approved_spread" in context[
        "blockers"
    ]
