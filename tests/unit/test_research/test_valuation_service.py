import pandas as pd
import pytest

from research.professional_dcf import ProfessionalDcfEngine
from research.valuation_service import ResearchValuationService


def test_valuation_service_builds_history_snapshots():
    service = ResearchValuationService({"history": {"lookback_days": 5}})

    quotes = pd.DataFrame(
        [
            {"time": "2026-04-16", "close": 10.0},
            {"time": "2026-04-17", "close": 11.0},
        ]
    )
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "exchange": "SSE",
    }
    bundle = {
        "report_period": "2025Q4",
        "data_available_date": "2026-04-15",
        "shares_outstanding": 100.0,
        "revenue": 80.0,
        "net_income": 20.0,
        "equity": 50.0,
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 2
    assert snapshots[0].market_cap == 1000.0
    assert snapshots[0].pe_ratio == 50.0
    assert snapshots[0].pe_static == 50.0
    assert snapshots[0].pe_ttm == 50.0
    assert snapshots[0].pb_mrq == 20.0
    assert snapshots[1].pb_ratio == 22.0
    assert snapshots[0].details_json["metrics"]["pe_forward"]["status"] == "unavailable"
    assert (
        snapshots[0].details_json["metrics"]["pe_forward"]["missing_reason"]
        == "analyst_forecast_disabled"
    )


def test_valuation_history_hash_excludes_window_parameters():
    base = ResearchValuationService(
        {
            "history": {
                "lookback_days": 7,
                "periodic_rebuild_window_mode": "trading_days",
                "flow_input_mode": "cumulative_ytd",
                "require_availability_date": True,
                "forward_metrics_enabled": False,
            }
        }
    )
    broad = ResearchValuationService(
        {
            "history": {
                "lookback_days": 252,
                "periodic_rebuild_window_mode": "last_12_quarters",
                "flow_input_mode": "cumulative_ytd",
                "require_availability_date": True,
                "forward_metrics_enabled": False,
            }
        }
    )
    changed_calculation = ResearchValuationService(
        {
            "history": {
                "lookback_days": 7,
                "periodic_rebuild_window_mode": "trading_days",
                "flow_input_mode": "cumulative_ytd",
                "require_availability_date": False,
                "forward_metrics_enabled": False,
            }
        }
    )

    base_identity = base.history_identity()
    broad_identity = broad.history_identity()

    assert base_identity["parameter_hash"] == broad_identity["parameter_hash"]
    assert (
        base_identity["parameter_hash"]
        != changed_calculation.history_identity()["parameter_hash"]
    )
    assert len(base_identity["compatible_parameter_hashes"]) >= 1


def test_valuation_service_builds_history_percentile_default_metrics():
    service = ResearchValuationService()
    rows = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": date_value.date().isoformat(),
            "pe_ttm": float(index),
            "pb_mrq": float(index + 100),
            "ps_ttm": float(index + 200),
            "calc_method": "valuation_history_builtin",
            "calc_version": "valuation_history.v1",
            "parameter_hash": "current",
        }
        for index, date_value in enumerate(
            pd.date_range("2026-01-01", periods=100, freq="D"),
            start=1,
        )
    ]

    result = service.build_history_percentile_response(
        rows,
        min_points=60,
        quarters=4,
    )

    assert result["status"] == "success"
    assert result["as_of_date"] == "2026-04-10"
    assert result["metric_variants"] == ["pe_ttm", "pb_mrq", "ps_ttm"]
    assert result["metrics"]["pe_ttm"]["sample_count"] == 100
    assert result["metrics"]["pe_ttm"]["current_value"] == 100.0
    assert result["metrics"]["pe_ttm"]["percentile_rank"] == 1.0
    assert result["metrics"]["pe_ttm"]["metric_median"] == 50.5


def test_valuation_service_history_percentile_handles_negative_policy_flag():
    service = ResearchValuationService()
    rows = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-01",
            "pe_ttm": -10.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-02",
            "pe_ttm": 5.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-03",
            "pe_ttm": 10.0,
        },
    ]

    result = service.build_history_percentile_response(
        rows,
        metrics=["pe_ttm"],
        min_points=1,
        negative_policy="flag",
    )

    metric = result["metrics"]["pe_ttm"]
    assert metric["status"] == "success"
    assert metric["sample_count"] == 3
    assert metric["negative_sample_count"] == 1
    assert metric["percentile_rank"] == 1.0
    assert metric["positive_only_percentile_rank"] == 1.0
    assert "negative_values_in_sample_limit_interpretation" in metric["warnings"]


def test_valuation_service_history_percentile_excludes_non_positive_values():
    service = ResearchValuationService()
    rows = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-01",
            "pb_mrq": -1.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-02",
            "pb_mrq": 0.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-03",
            "pb_mrq": 2.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-04",
            "pb_mrq": 4.0,
        },
    ]

    result = service.build_history_percentile_response(
        rows,
        metrics=["pb_mrq"],
        min_points=1,
        negative_policy="exclude",
        include_series=True,
    )

    metric = result["metrics"]["pb_mrq"]
    assert metric["sample_count"] == 2
    assert metric["excluded_count"] == 2
    assert metric["percentile_rank"] == 1.0
    assert metric["series"] == [
        {"as_of_date": "2026-01-03", "value": 2.0},
        {"as_of_date": "2026-01-04", "value": 4.0},
    ]


def test_valuation_service_history_percentile_excludes_non_positive_current_value():
    service = ResearchValuationService()
    rows = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-01",
            "pe_ttm": 2.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-02",
            "pe_ttm": 4.0,
        },
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-03",
            "pe_ttm": -1.0,
        },
    ]

    result = service.build_history_percentile_response(
        rows,
        metrics=["pe_ttm"],
        min_points=1,
        negative_policy="exclude",
    )

    metric = result["metrics"]["pe_ttm"]
    assert result["status"] == "unavailable"
    assert metric["status"] == "current_value_excluded"
    assert metric["sample_count"] == 2
    assert metric["percentile_rank"] is None
    assert "current_non_positive_excluded_from_percentile" in metric["warnings"]


def test_valuation_service_history_percentile_reports_insufficient_data():
    service = ResearchValuationService()
    rows = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-01-01",
            "ps_ttm": 2.0,
        }
    ]

    result = service.build_history_percentile_response(
        rows,
        metrics=["ps_ttm"],
        min_points=2,
    )

    metric = result["metrics"]["ps_ttm"]
    assert result["status"] == "insufficient_data"
    assert metric["status"] == "insufficient_data"
    assert metric["sample_count"] == 1
    assert metric["percentile_rank"] is None
    assert "insufficient_valid_observations" in metric["warnings"]


def test_valuation_service_writes_market_cap_only_rows_without_financial_facts():
    service = ResearchValuationService({"history": {"lookback_days": 5}})
    quotes = pd.DataFrame(
        [
            {"time": "2026-05-27", "close": 10.0},
            {"time": "2026-05-28", "close": 11.0},
        ]
    )
    instrument = {
        "instrument_id": "688635.SH",
        "symbol": "688635",
        "exchange": "SSE",
    }
    bundle = {
        "financial_history": [],
        "valuation_inputs": [
            {
                "as_of_date": "2026-05-27",
                "shares_outstanding": 100.0,
                "float_shares": 40.0,
                "source": "cninfo",
                "source_mode": "direct",
                "input_kind": "share_count",
                "unit": "share",
            }
        ],
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 2
    assert snapshots[0].market_cap == 1000.0
    assert snapshots[0].float_market_cap == 400.0
    assert snapshots[0].pe_ratio is None
    assert snapshots[0].details_json["valuation_scope"] == "market_cap_only"
    assert (
        snapshots[0].details_json["metrics"]["pe_ttm"]["missing_reason"]
        == "financial_facts_not_available"
    )
    assert service.candidate_history_dates(quotes, bundle) == [
        "2026-05-27",
        "2026-05-28",
    ]


def test_valuation_service_builds_ttm_without_future_report_data():
    service = ResearchValuationService({"history": {"lookback_days": 5}})
    quotes = pd.DataFrame(
        [
            {"time": "2025-04-29", "close": 10.0},
            {"time": "2025-05-06", "close": 11.0},
        ]
    )
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "exchange": "SSE",
    }
    bundle = {
        "financial_history": [
            {
                "report_period": "2025Q1",
                "data_available_date": "2025-04-30",
                "revenue": 130.0,
                "net_income": 15.0,
                "equity": 120.0,
                "shares_outstanding": 100.0,
            },
            {
                "report_period": "2024Q4",
                "data_available_date": "2025-04-20",
                "revenue": 400.0,
                "net_income": 80.0,
                "equity": 110.0,
                "shares_outstanding": 100.0,
            },
            {
                "report_period": "2024Q1",
                "data_available_date": "2024-04-30",
                "revenue": 100.0,
                "net_income": 10.0,
                "equity": 90.0,
                "shares_outstanding": 100.0,
            },
        ]
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 2
    assert snapshots[0].as_of_date == "2025-04-29"
    assert snapshots[0].details_json["latest_financial_report_period"] == "2024Q4"
    assert snapshots[0].pe_ttm == 12.5
    assert snapshots[1].details_json["latest_financial_report_period"] == "2025Q1"
    assert snapshots[1].pe_ttm == pytest.approx(1100.0 / 85.0)
    assert snapshots[1].details_json["metrics"]["pe_ttm"]["report_periods"] == [
        "2025Q1",
        "2024Q4",
        "2024Q1",
    ]


def test_valuation_service_builds_semiannual_ttm_from_ytd_facts():
    service = ResearchValuationService({"history": {"lookback_days": 5}})
    quotes = pd.DataFrame([{"time": "2025-09-01", "close": 10.0}])
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "exchange": "SSE",
    }
    bundle = {
        "financial_history": [
            {
                "report_period": "2025Q2",
                "data_available_date": "2025-08-31",
                "revenue": 260.0,
                "net_income": 30.0,
                "equity": 120.0,
                "shares_outstanding": 100.0,
            },
            {
                "report_period": "2024Q4",
                "data_available_date": "2025-04-20",
                "revenue": 400.0,
                "net_income": 80.0,
                "equity": 110.0,
                "shares_outstanding": 100.0,
            },
            {
                "report_period": "2024Q2",
                "data_available_date": "2024-08-31",
                "revenue": 220.0,
                "net_income": 25.0,
                "equity": 95.0,
                "shares_outstanding": 100.0,
            },
        ]
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 1
    assert snapshots[0].pe_ttm == pytest.approx(1000.0 / 85.0)
    assert snapshots[0].ps_ttm == pytest.approx(1000.0 / 440.0)


def test_valuation_service_marks_missing_ttm_periods():
    service = ResearchValuationService()
    quotes = pd.DataFrame([{"time": "2025-09-10", "close": 10.0}])
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "exchange": "SSE",
    }
    bundle = {
        "financial_history": [
            {
                "report_period": "2025Q2",
                "data_available_date": "2025-08-30",
                "revenue": 250.0,
                "net_income": -1.0,
                "equity": 100.0,
                "shares_outstanding": 100.0,
            },
            {
                "report_period": "2024Q4",
                "data_available_date": "2025-04-20",
                "revenue": 400.0,
                "net_income": 80.0,
                "equity": 90.0,
                "shares_outstanding": 100.0,
            },
        ]
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 1
    assert snapshots[0].pe_ttm is None
    assert (
        snapshots[0].details_json["metrics"]["pe_ttm"]["missing_reason"]
        == "missing_ttm_comparison_period"
    )
    assert snapshots[0].details_json["metrics"]["pe_static"]["status"] == "available"
    assert snapshots[0].details_json["metrics"]["pb_mrq"]["status"] == "available"


def test_valuation_service_keeps_negative_pe_and_rejects_zero_sales():
    service = ResearchValuationService()
    quotes = pd.DataFrame([{"time": "2026-05-10", "close": 10.0}])
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "exchange": "SSE",
    }
    bundle = {
        "financial_history": [
            {
                "report_period": "2025Q4",
                "data_available_date": "2026-04-30",
                "revenue": 0.0,
                "net_income": -5.0,
                "equity": 100.0,
                "shares_outstanding": 100.0,
            }
        ]
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 1
    assert snapshots[0].pe_static == -200.0
    assert snapshots[0].pe_ttm == -200.0
    assert snapshots[0].ps_static is None
    assert snapshots[0].details_json["metrics"]["pe_static"]["status"] == "available"
    assert snapshots[0].details_json["metrics"]["ps_static"]["missing_reason"] == "invalid_denominator"


def test_valuation_service_builds_relative_valuation():
    service = ResearchValuationService({"relative": {"min_peer_count": 1}})

    result = service.build_relative_valuation(
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        subject_row={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-04-18",
            "close_price": 1600.0,
            "market_cap": 2000.0,
            "pe_ratio": 25.0,
            "pb_ratio": 8.0,
            "ps_ratio": 10.0,
            "pe_ttm": 25.0,
            "pb_mrq": 8.0,
            "ps_ttm": 10.0,
            "data_as_of": "2026-04-18T18:30:00",
        },
        industry_membership={
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "mapping_status": "authoritative",
            "sw_l2_code": "801124.SI",
            "sw_l2_name": "饮料乳品",
        },
        peer_rows=[
            {
                "instrument_id": "000858.SZ",
                "symbol": "000858",
                "exchange": "SZSE",
                "as_of_date": "2026-04-18",
                "close_price": 150.0,
                "market_cap": 1800.0,
                "pe_ratio": 20.0,
                "pb_ratio": 6.0,
                "ps_ratio": 8.0,
                "pe_ttm": 20.0,
                "pb_mrq": 6.0,
                "ps_ttm": 8.0,
                "data_as_of": "2026-04-18T18:30:00",
            },
            {
                "instrument_id": "603369.SH",
                "symbol": "603369",
                "exchange": "SSE",
                "as_of_date": "2026-04-18",
                "close_price": 180.0,
                "market_cap": 1900.0,
                "pe_ratio": 22.0,
                "pb_ratio": 7.0,
                "ps_ratio": 9.0,
                "pe_ttm": 22.0,
                "pb_mrq": 7.0,
                "ps_ttm": 9.0,
                "data_as_of": "2026-04-18T18:30:00",
            },
        ],
    )

    assert result["status"] == "success"
    assert result["benchmark_level"] == 2
    assert result["benchmark_field"] == "sw_l2_code"
    assert result["benchmark_code"] == "801124.SI"
    assert result["benchmark_name"] == "饮料乳品"
    assert result["benchmark_sw_l2_code"] == "801124.SI"
    assert result["benchmark_summary"]["pe_ratio"]["peer_median"] == 21.0
    assert result["benchmark_summary"]["pe_ttm"]["peer_p25"] == 20.5
    assert result["benchmark_summary"]["pe_ttm"]["peer_p75"] == 21.5
    assert result["benchmark_summary"]["pe_ttm"]["valid_peer_count"] == 2
    assert result["benchmark_summary"]["pe_ttm"]["percentile_rank"] == 1.0
    assert len(result["peers"]) == 2


def test_valuation_service_relative_valuation_reports_variant_exclusions():
    service = ResearchValuationService(
        {
            "relative": {
                "min_peer_count": 1,
                "metric_variants": ["pe_static", "pe_ttm", "pe_forward"],
                "include_compatibility_metrics": False,
            }
        }
    )

    result = service.build_relative_valuation(
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        subject_row={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-04-18",
            "market_cap": 2000.0,
            "pe_static": 24.0,
            "pe_ttm": 25.0,
            "pe_forward": None,
            "data_as_of": "2026-04-18T18:30:00",
        },
        industry_membership={
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "mapping_status": "authoritative",
            "sw_l2_code": "801124.SI",
            "sw_l2_name": "饮料乳品",
        },
        peer_rows=[
            {
                "instrument_id": "000858.SZ",
                "symbol": "000858",
                "exchange": "SZSE",
                "as_of_date": "2026-04-18",
                "pe_static": 20.0,
                "pe_ttm": 21.0,
                "pe_forward": None,
            },
            {
                "instrument_id": "603369.SH",
                "symbol": "603369",
                "exchange": "SSE",
                "as_of_date": "2026-04-18",
                "pe_static": -1.0,
                "pe_ttm": None,
                "pe_forward": None,
            },
        ],
    )

    assert result["metric_variants"] == ["pe_static", "pe_ttm", "pe_forward"]
    assert result["benchmark_summary"]["pe_static"]["valid_peer_count"] == 2
    assert result["benchmark_summary"]["pe_static"]["excluded_peer_count"] == 0
    assert result["benchmark_summary"]["pe_static"]["peer_median"] == 9.5
    assert result["benchmark_summary"]["pe_forward"]["valid_peer_count"] == 0
    assert result["diagnostics"]["metric_exclusions"]["pe_ttm"][0]["reason"] == "missing_value"


def test_valuation_service_rejects_non_authoritative_relative_membership():
    service = ResearchValuationService({"relative": {"min_peer_count": 1}})

    result = service.build_relative_valuation(
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        subject_row={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-04-18",
            "close_price": 1600.0,
            "market_cap": 2000.0,
            "pe_ratio": 25.0,
            "pb_ratio": 8.0,
            "ps_ratio": 10.0,
            "data_as_of": "2026-04-18T18:30:00",
        },
        industry_membership={
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "mapping_status": "reference_only",
            "sw_l2_code": "801124.SI",
            "sw_l2_name": "饮料乳品",
        },
        peer_rows=[
            {
                "instrument_id": "000858.SZ",
                "symbol": "000858",
                "exchange": "SZSE",
                "as_of_date": "2026-04-18",
                "pe_ratio": 20.0,
                "pb_ratio": 6.0,
                "ps_ratio": 8.0,
                "data_as_of": "2026-04-18T18:30:00",
            }
        ],
    )

    assert result["status"] == "benchmark_unavailable"
    assert result["missing_reason"] == "authoritative_sw_l2_membership_required"
    assert result["benchmark_field"] == "sw_l2_code"
    assert result["benchmark_code"] == "801124.SI"
    assert result["peers"] == []


def test_valuation_service_runs_dcf():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "depreciation_and_amortization": 40.0,
            "change_in_working_capital": 10.0,
            "net_income": 80.0,
            "cash_and_equivalents": 120.0,
            "total_debt": 200.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"growth_rate": 0.08, "projection_years": 5, "valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["calc_version"] == "nonfinancial_fcff.v1"
    assert result["base_cash_flow_source"] == "fcff"
    assert len(result["scenarios"]) == 4
    assert result["scenarios"][1]["intrinsic_value_per_share"] is not None
    assert result["forecast_rows"]
    assert len(result["sensitivity"]) > 0


def test_valuation_service_dcf_records_on_demand_beta_lineage():
    service = ResearchValuationService({"dcf": {"discount_rate": None}})

    result = service.run_dcf(
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "beta": 1.2,
            "beta_source": "beta_on_demand",
            "beta_benchmark": {"benchmark_instrument_id": "000300.SH"},
        },
    )

    assert result["status"] == "success"
    assert result["beta"] == 1.2
    assert result["beta_source"] == "beta_on_demand"
    assert result["beta_benchmark"]["benchmark_instrument_id"] == "000300.SH"
    assert result["assumptions"]["wacc"]["beta"] == 1.2


def test_valuation_service_dcf_excludes_future_financial_facts():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-05-01",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["missing_reason"] == "financial_fact_after_valuation_date"
    assert "financial_fact_after_valuation_date" in result["readiness"]["blockers"]


def test_valuation_service_dcf_bank_defaults_to_residual_income_model():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry_name": "银行",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "equity": 1000.0,
            "net_income": 100.0,
            "roe": 0.10,
            "capital_adequacy_ratio": 0.13,
            "dividend_payout_ratio": 0.3,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["model_profile"] == "bank_residual_income.v1"
    assert result["calc_method"] == "professional_dcf_bank_residual_income"
    assert result["enterprise_value"] is None
    assert result["equity_value"] is not None
    assert result["implied_pb"] is not None
    assert result["ddm_cross_check"]["intrinsic_value_per_share"] is not None


def test_valuation_service_dcf_bank_missing_core_inputs_fails_closed():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry_name": "银行",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "equity": 1000.0,
            "net_income": 100.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["model_profile"] == "bank_residual_income.v1"
    assert result["missing_reason"] == "bank_core_inputs_missing"
    assert "shares_outstanding_required" in result["readiness"]["blockers"]


def test_valuation_service_dcf_bank_derives_roe_and_flags_capital_warning():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry_name": "银行",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "equity": 1000.0,
            "net_income": 80.0,
            "capital_adequacy_ratio": 0.09,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["financial_model_diagnostics"]["roe_source"] == "derived_net_income_over_equity"
    assert result["financial_model_diagnostics"]["capital"]["status"] == "below_threshold"
    assert "bank_roe_derived_from_net_income_over_equity" in result["warnings"]
    assert "bank_capital_adequacy_below_threshold" in result["warnings"]


def test_valuation_service_dcf_broker_defaults_to_excess_capital_model():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "industry_name": "证券",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "equity": 1000.0,
            "net_income": 120.0,
            "net_capital": 260.0,
            "roe": 0.12,
            "market_turnover": 90000.0,
            "index_level": 3600.0,
            "brokerage_revenue": 80.0,
            "investment_income": 45.0,
            "leverage_ratio": 2.5,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["model_profile"] == "broker_excess_capital.v1"
    assert result["calc_method"] == "professional_dcf_broker_excess_capital"
    assert result["enterprise_value"] is None
    assert result["equity_value"] is not None
    assert result["implied_pb"] is not None
    assert result["normalized_roe"] == 0.12
    assert result["excess_capital"] == 60.0
    assert result["broker_model_diagnostics"]["missing_market_cycle_inputs"] == []


def test_valuation_service_dcf_broker_missing_core_inputs_fails_closed():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "industry_name": "证券",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "equity": 1000.0,
            "net_income": 120.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["model_profile"] == "broker_excess_capital.v1"
    assert result["missing_reason"] == "broker_core_inputs_missing"
    assert "net_capital_required" in result["readiness"]["blockers"]


def test_valuation_service_dcf_broker_scope_required_for_platform_candidate():
    service = ResearchValuationService()
    instrument = {
        "instrument_id": "300059.SZ",
        "symbol": "300059",
        "exchange": "SZSE",
        "industry_name": "证券",
    }
    bundle = {
        "report_period": "2025-12-31",
        "data_available_date": "2026-03-30",
        "equity": 1000.0,
        "net_income": 120.0,
        "net_capital": 260.0,
        "shares_outstanding": 10.0,
    }

    auto_result = service.run_dcf(
        instrument=instrument,
        financial_bundle=bundle,
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )
    forced_result = service.run_dcf(
        instrument=instrument,
        financial_bundle=bundle,
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "model_profile": "broker_excess_capital.v1",
        },
    )

    assert auto_result["model_profile"] != "broker_excess_capital.v1"
    assert forced_result["status"] == "unavailable"
    assert forced_result["missing_reason"] == "broker_scope_unconfirmed"
    assert "broker_scope_unconfirmed" in forced_result["readiness"]["blockers"]


def test_valuation_service_dcf_broker_derives_and_caps_peak_roe():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "industry_name": "证券",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "equity": 1000.0,
            "net_income": 300.0,
            "net_capital": 250.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["normalized_roe"] == 0.15
    assert result["broker_model_diagnostics"]["roe_source"] == "derived_net_income_over_equity"
    assert "broker_roe_derived_from_net_income_over_equity" in result["warnings"]
    assert "broker_roe_normalized_to_cap" in result["warnings"]
    assert "broker_market_cycle_inputs_missing" in result["warnings"]


def test_valuation_service_dcf_cyclical_defaults_to_midcycle_model():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "601088.SH",
            "symbol": "601088",
            "exchange": "SSE",
            "industry_name": "煤炭",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 240.0,
            "capital_expenditure": 80.0,
            "commodity_price_assumption": 780.0,
            "cycle_index_level": 105.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["model_profile"] == "cyclical_fcff_midcycle.v1"
    assert result["calc_method"] == "professional_dcf_cyclical_midcycle_fcff"
    assert result["base_cash_flow_source"] == "midcycle_fcff"
    assert result["enterprise_value"] is not None
    assert result["cyclical_model_diagnostics"]["reported_operating_margin"] == 0.24
    assert result["cyclical_model_diagnostics"]["normalized_operating_margin"] == 0.18
    assert "cyclical_peak_margin_capped_to_midcycle" in result["warnings"]


def test_valuation_service_dcf_cyclical_uses_futures_cycle_for_margin_normalization():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "601088.SH",
            "symbol": "601088",
            "exchange": "SSE",
            "industry_name": "煤炭",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 160.0,
            "capital_expenditure": 80.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "futures_cycle_context": {
                "selected_series_id": "CNF.J.DCE.main",
                "mapping_scope": "industry",
                "mapping_scope_id": "煤炭",
                "commodity_price_assumption": 1932.0,
                "cycle_index_level": 0.92,
                "diagnostic": {
                    "percentile": 0.92,
                    "mean_price": 2166.82,
                    "mean_deviation_pct": 0.18,
                    "cycle_state": "extreme_high",
                },
                "source_policy": "local_futures_db_only",
            },
        },
    )

    diagnostics = result["cyclical_model_diagnostics"]
    assert result["status"] == "success"
    assert diagnostics["normalization_source"] == "reported_margin_adjusted_by_futures_cycle_to_midcycle_band"
    assert diagnostics["normalized_operating_margin"] < diagnostics["reported_operating_margin"]
    assert diagnostics["futures_cycle_percentile"] == 0.92
    assert diagnostics["futures_cycle_selected_series_id"] == "CNF.J.DCE.main"
    assert diagnostics["futures_market_data"]["source_policy"] == "local_futures_db_only"
    assert "cyclical_high_cycle_margin_normalized_with_futures_data" in result["warnings"]


def test_valuation_service_dcf_cyclical_explicit_midcycle_margin_overrides_cycle_context():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "601088.SH",
            "symbol": "601088",
            "exchange": "SSE",
            "industry_name": "煤炭",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 160.0,
            "capital_expenditure": 80.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "midcycle_operating_margin": 0.09,
            "futures_cycle_context": {
                "selected_series_id": "CNF.J.DCE.main",
                "cycle_index_level": 0.95,
                "diagnostic": {"percentile": 0.95, "cycle_state": "extreme_high"},
            },
        },
    )

    diagnostics = result["cyclical_model_diagnostics"]
    assert diagnostics["normalization_source"] == "explicit_midcycle_operating_margin"
    assert diagnostics["normalized_operating_margin"] == 0.09
    assert "cyclical_high_cycle_margin_normalized_with_futures_data" not in result["warnings"]


def test_valuation_service_dcf_cyclical_missing_core_inputs_fails_closed():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "601088.SH",
            "symbol": "601088",
            "exchange": "SSE",
            "industry_name": "煤炭",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 240.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["model_profile"] == "cyclical_fcff_midcycle.v1"
    assert result["missing_reason"] == "cyclical_core_inputs_missing"
    assert "capital_expenditure_required" in result["readiness"]["blockers"]


def test_valuation_service_dcf_cyclical_comparison_preserves_diagnostics():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "601088.SH",
            "symbol": "601088",
            "exchange": "SSE",
            "industry_name": "煤炭",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 20.0,
            "capital_expenditure": 80.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "model_strategy": "compare",
            "include_model_comparison": True,
        },
    )

    assert result["status"] == "success"
    assert result["cyclical_model_diagnostics"]["normalized_operating_margin"] == 0.04
    assert "cyclical_trough_margin_lifted_to_midcycle_floor" in result["warnings"]
    assert "cyclical_cycle_inputs_missing" in result["warnings"]
    comparison = result["model_comparison"]["industry_model_result"]
    assert comparison["model_profile"] == "cyclical_fcff_midcycle.v1"
    assert comparison["cyclical_model_diagnostics"]["normalized_operating_margin"] == 0.04


def test_valuation_service_dcf_model_comparison_for_close_scores():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "688001.SH",
            "symbol": "688001",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 30.0,
            "capital_expenditure": 120.0,
            "research_and_development": 200.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "research_mode": True,
            "model_strategy": "compare",
            "include_model_comparison": True,
        },
    )

    assert result["model_comparison"] is not None
    assert len(result["model_suitability_candidates"]) == 2
    comparison = result["model_comparison"]
    assert comparison["industry_model_result"]["model_profile"] == "nonfinancial_fcff.v1"
    assert comparison["industry_model_result"]["status"] in {"success", "partial"}
    assert comparison["company_characteristic_model_result"]["model_profile"] == "high_growth_staged_fcff.v1"
    assert comparison["company_characteristic_model_result"]["status"] == "unavailable"
    assert "model_profile_not_implemented" in comparison["company_characteristic_model_result"]["missing_reason"]


def test_valuation_service_dcf_selects_fcfe_for_stable_low_leverage_when_inputs_ready():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600900.SH",
            "symbol": "600900",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 250.0,
            "capital_expenditure": 50.0,
            "operating_cf": 180.0,
            "net_debt_change": 5.0,
            "total_debt": 100.0,
            "equity": 1000.0,
            "dividend_payout_ratio": 0.6,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["calc_method"] == "professional_dcf_fcfe"
    assert result["calc_version"] == "nonfinancial_fcfe.v1"
    assert result["cash_flow_model_selection"]["selected_cash_flow_model"] == "fcfe"
    assert result["selected_cash_flow_model"] == "fcfe"
    assert result["base_cash_flow_source"] == "fcfe"
    assert result["base_cash_flow"] is not None


def test_valuation_service_dcf_explicit_fcfe_calculates_when_inputs_ready():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600900.SH",
            "symbol": "600900",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 250.0,
            "capital_expenditure": 50.0,
            "operating_cf": 180.0,
            "net_debt_change": 5.0,
            "total_debt": 100.0,
            "equity": 1000.0,
            "dividend_payout_ratio": 0.6,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18", "cash_flow_model": "fcfe"},
    )

    assert result["status"] == "success"
    assert result["calc_method"] == "professional_dcf_fcfe"
    assert result["selected_cash_flow_model"] == "fcfe"
    assert result["base_cash_flow_source"] == "fcfe"
    assert result["enterprise_value"] is None
    assert result["equity_value"] is not None


def test_valuation_service_dcf_explicit_fcfe_fails_closed_when_inputs_missing():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600900.SH",
            "symbol": "600900",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 250.0,
            "capital_expenditure": 50.0,
            "total_debt": 100.0,
            "equity": 1000.0,
            "dividend_payout_ratio": 0.6,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18", "cash_flow_model": "fcfe"},
    )

    assert result["status"] == "unavailable"
    assert result["missing_reason"] == "fcfe_inputs_missing"
    assert result["selected_cash_flow_model"] == "fcfe"
    assert "fcfe_inputs_missing" in result["readiness"]["blockers"]


def test_valuation_service_dcf_keeps_high_leverage_company_on_fcff():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600900.SH",
            "symbol": "600900",
            "exchange": "SSE",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 250.0,
            "capital_expenditure": 50.0,
            "change_in_working_capital": 5.0,
            "operating_cf": 180.0,
            "net_debt_change": 5.0,
            "total_debt": 1800.0,
            "equity": 1000.0,
            "dividend_payout_ratio": 0.6,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["selected_cash_flow_model"] == "fcff"
    assert "high_leverage_fcfe_not_default" in result["warnings"]


def test_valuation_service_dcf_utility_distribution_model_succeeds():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "600900.SH",
            "symbol": "600900",
            "exchange": "SSE",
            "industry_name": "电力 公用事业",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 250.0,
            "operating_cf": 220.0,
            "capital_expenditure": 80.0,
            "net_debt_change": 0.0,
            "net_income": 160.0,
            "dividend_payout_ratio": 0.65,
            "shares_outstanding": 10.0,
            "total_debt": 500.0,
            "equity": 2000.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "model_strategy": "compare",
            "include_model_comparison": True,
        },
    )

    assert result["status"] == "success"
    assert result["model_profile"] == "utility_fcfe_or_ddm.v1"
    assert result["calc_method"] == "professional_dcf_distribution"
    assert result["selected_cash_flow_model"] == "ddm"
    assert result["base_cash_flow_source"] == "fcfe_distribution"
    assert result["enterprise_value"] is None
    assert result["equity_value"] is not None
    assert result["model_comparison"]["industry_model_result"]["model_profile"] == "utility_fcfe_or_ddm.v1"
    assert result["model_comparison"]["industry_model_result"]["status"] == "success"


def test_valuation_service_dcf_reit_distribution_model_uses_affo_or_ffo():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "508000.SH",
            "symbol": "508000",
            "exchange": "SSE",
            "industry_name": "基础设施公募 REIT",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "affo": 120.0,
            "ffo": 130.0,
            "dividend_payout_ratio": 0.9,
            "shares_outstanding": 10.0,
        },
        latest_close=8.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "success"
    assert result["model_profile"] == "reit_ffo_affo_ddm.v1"
    assert result["base_cash_flow_source"] == "affo_distribution"
    assert result["selected_cash_flow_model"] == "ddm"
    assert result["scenarios"][1]["dividend_yield"] is not None


def test_valuation_service_dcf_reit_distribution_missing_inputs_fails_closed():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={
            "instrument_id": "508000.SH",
            "symbol": "508000",
            "exchange": "SSE",
            "industry_name": "基础设施公募 REIT",
        },
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "dividend_payout_ratio": 0.9,
            "shares_outstanding": 10.0,
        },
        latest_close=8.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["missing_reason"] == "distribution_inputs_missing"
    assert "distribution_cash_flow_required" in result["readiness"]["blockers"]


def test_valuation_service_dcf_honors_detail_suppression_and_validates_methods():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "include_forecast_rows": False,
            "include_sensitivity": False,
            "include_lineage": False,
            "scenario_set": "downside_only",
            "terminal_method": "gordon_growth",
        },
    )

    assert result["status"] == "success"
    assert result["forecast_rows"] == []
    assert result["sensitivity"] == []
    assert result["lineage"] is None
    assert [item["scenario"] for item in result["scenarios"]] == ["bear", "base", "stress"]

    invalid = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18", "terminal_method": "exit_multiple"},
    )
    assert invalid["status"] == "invalid_parameters"
    assert invalid["missing_reason"] == "unsupported_terminal_method"


def test_professional_dcf_assumptions_expose_fallback_missing_and_refresh_diagnostics():
    engine = ProfessionalDcfEngine()

    assumptions = engine.get_assumptions(market="SSE", currency="CNY")
    risk_free = assumptions["risk_free_rate"]

    assert risk_free["assumption_key"] == "risk_free_rate_rmb_10y"
    assert risk_free["fallback_used"] is True
    assert risk_free["lineage_hash"]
    assert assumptions["commodity_price"]["quality_flag"] == "missing"
    assert "assumption_value_missing" in assumptions["commodity_price"]["warnings"]

    refresh = engine.refresh_assumptions(
        source_profile="china_bond_10y",
        timeout_seconds=3,
        dry_run=False,
    )

    assert refresh["status"] == "unsupported"
    assert refresh["diagnostics"]["remote_fetch_performed"] is False
    assert refresh["source_results"][0]["timeout_seconds"] == 3


def test_professional_dcf_fx_assumption_override_uses_local_fx_lineage():
    engine = ProfessionalDcfEngine()

    assumptions = engine.get_assumptions(
        market="SSE",
        currency="CNY",
        overrides={
            "fx_usd_cny": 7.2,
            "fx_usd_cny_source": "local_fx_db",
            "fx_usd_cny_quality_flag": "official",
            "fx_usd_cny_as_of_date": "2026-06-26",
            "fx_usd_cny_metadata": {
                "fx_series_id": "FX.USD_CNY.CFETS.MID.DAILY",
                "conversion_policy": "direct",
                "source_policy": "local_fx_db_only",
            },
        },
    )

    fx = assumptions["fx_usd_cny"]
    assert fx["value"] == 7.2
    assert fx["source"] == "local_fx_db"
    assert fx["quality_flag"] == "official"
    assert fx["fallback_used"] is False
    assert fx["as_of_date"] == "2026-06-26"
    assert fx["metadata"]["source_policy"] == "local_fx_db_only"
    assert fx["lineage_hash"]


def test_professional_dcf_missing_required_assumption_returns_structured_blocker():
    service = ResearchValuationService(
        {
            "dcf": {
                "professional": {
                    "assumptions": {
                        "risk_free_rate_rmb_10y": {
                            "assumption_key": "risk_free_rate_rmb_10y",
                            "market": "CN",
                            "currency": "CNY",
                            "tenor": "10Y",
                            "value": None,
                            "unit": "rate",
                            "primary_source": "china_bond_10y",
                            "fallback_sources": ["manual_config"],
                            "source": None,
                            "quality_flag": "missing",
                            "fallback_used": False,
                        }
                    }
                }
            }
        }
    )

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["missing_reason"] == "assumption_risk_free_rate_rmb_10y_missing"


def test_professional_dcf_fallback_assumptions_are_warned():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert "risk_free_rate_rmb_10y_fallback_used" in result["warnings"]
    assert "equity_risk_premium_fallback_used" in result["warnings"]
    assert "cost_of_debt_default_fallback_used" in result["warnings"]
    assert "beta_fallback_used" in result["warnings"]


def test_valuation_service_dcf_blocks_missing_availability_date():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["missing_reason"] == "missing_data_available_date"


def test_valuation_service_dcf_missing_capex_blocks_full_fcff():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["missing_reason"] == "capital_expenditure_required"
    assert "capital_expenditure_required" in result["readiness"]["blockers"]


def test_valuation_service_dcf_financial_formula_and_net_debt_bridge():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "depreciation_and_amortization": 40.0,
            "change_in_working_capital": 10.0,
            "cash_and_equivalents": 100.0,
            "total_debt": 200.0,
            "lease_liabilities": 10.0,
            "preferred_equity": 3.0,
            "minority_interest": 5.0,
            "non_operating_assets": 8.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={
            "valuation_date": "2026-04-18",
            "growth_rate": 0.0,
            "projection_years": 1,
            "discount_rate": 0.1,
            "terminal_growth": 0.03,
        },
    )

    first_row = result["forecast_rows"][0]
    assert first_row["fcff"] == pytest.approx(105.0)
    assert result["net_debt_adjustment"]["net_debt"] == pytest.approx(110.0)
    assert result["assumptions"]["wacc"]["wacc"] == pytest.approx(0.1)
    assert "discount_rate_override_used" in result["warnings"]


def test_valuation_service_dcf_terminal_growth_must_be_below_wacc():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18", "discount_rate": 0.03, "terminal_growth": 0.03},
    )

    assert result["status"] == "invalid_parameters"
    assert result["missing_reason"] == "wacc_must_exceed_terminal_growth"


def test_valuation_service_dcf_forced_financial_fcff_records_warning():
    service = ResearchValuationService()

    for instrument_id, industry_name in (("600000.SH", "银行"), ("600030.SH", "证券")):
        result = service.run_dcf(
            instrument={
                "instrument_id": instrument_id,
                "symbol": instrument_id.split(".")[0],
                "exchange": "SSE",
                "industry_name": industry_name,
            },
            financial_bundle={
                "report_period": "2025-12-31",
                "data_available_date": "2026-03-30",
                "revenue": 1000.0,
                "operating_profit": 180.0,
                "capital_expenditure": 60.0,
                "shares_outstanding": 10.0,
            },
            latest_close=12.0,
            overrides={"valuation_date": "2026-04-18", "model_profile": "nonfinancial_fcff.v1"},
        )

        assert result["status"] == "success"
        assert "forced_model_warning" in result["warnings"]
        assert "financial_sector_mismatch" in result["warnings"]


def test_valuation_service_dcf_special_company_guardrail_fails_closed():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "688001.SH", "symbol": "688001", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": -50.0,
            "capital_expenditure": 60.0,
            "net_income": -100.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    assert result["status"] == "unavailable"
    assert result["model_profile"] == "high_growth_staged_fcff.v1"
    assert result["missing_reason"] == "model_profile_not_implemented"


def test_valuation_service_dcf_adapter_records_audit_fields_and_warnings():
    service = ResearchValuationService()

    result = service.run_dcf(
        instrument={"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        financial_bundle={
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "total_debt": 2000.0,
            "equity": 500.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"valuation_date": "2026-04-18"},
    )

    selection = result["cash_flow_model_selection"]
    assert selection["selected_cash_flow_model"] == "fcff"
    assert selection["candidate_models"] == ["fcff", "fcfe"]
    assert "fcfe" in selection["rejected_models"]
    assert "operating_cf" in selection["input_gap_by_model"]["fcfe"]
    assert "high_leverage_fcfe_not_default" in selection["warnings"]
