import pandas as pd
import pytest

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
            "operating_cf": 100.0,
            "net_income": 80.0,
            "shares_outstanding": 10.0,
        },
        latest_close=12.0,
        overrides={"growth_rate": 0.08, "projection_years": 5},
    )

    assert result["status"] == "success"
    assert result["base_cash_flow_source"] == "operating_cf"
    assert len(result["scenarios"]) == 3
    assert result["scenarios"][1]["intrinsic_value_per_share"] is not None
    assert len(result["sensitivity"]) > 0
