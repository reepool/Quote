import pandas as pd

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
        "report_period": "2025-12-31",
        "shares_outstanding": 100.0,
        "revenue": 80.0,
        "net_income": 20.0,
        "equity": 50.0,
    }

    snapshots = service.build_history_snapshots(quotes, instrument, bundle)

    assert len(snapshots) == 2
    assert snapshots[0].market_cap == 1000.0
    assert snapshots[0].pe_ratio == 50.0
    assert snapshots[1].pb_ratio == 22.0


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
    assert len(result["peers"]) == 2


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
