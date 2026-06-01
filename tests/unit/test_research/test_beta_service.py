import pandas as pd

from research.beta_service import ResearchBetaService


def _quotes(start: str, stock_returns: list[float]) -> pd.DataFrame:
    values = [100.0]
    for item in stock_returns:
        values.append(values[-1] * (1.0 + item))
    return pd.DataFrame(
        [
            {"time": day.date().isoformat(), "close": value}
            for day, value in zip(pd.date_range(start, periods=len(values), freq="D"), values)
        ]
    )


def test_beta_service_calculates_beta_and_diagnostics():
    service = ResearchBetaService({"min_observations_floor": 2})
    benchmark_returns = [0.01, -0.02, 0.03, 0.01, -0.01]
    stock_returns = [item * 1.5 for item in benchmark_returns]

    result = service.build_result(
        stock_quotes=_quotes("2026-01-01", stock_returns),
        benchmark_quotes=_quotes("2026-01-01", benchmark_returns),
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
            "benchmark_name": "沪深300",
        },
        window_days=5,
        stock_adjustment="qfq",
        benchmark_adjustment="none",
    )

    assert result.status == "success"
    assert round(result.beta, 6) == 1.5
    assert result.observation_count == 5
    assert result.stock_adjustment == "qfq"
    assert result.benchmark_adjustment == "none"
    assert result.correlation > 0.99
    assert result.r_squared > 0.99
    assert result.residual_volatility < 1e-12
    assert result.tracking_error is not None
    assert result.standard_error_beta is not None
    assert result.t_stat_beta is not None
    assert result.p_value_beta is not None
    assert result.quality_flag == "high"
    assert result.interpretation_flags == []
    assert result.details_json["p_value_method"] == "normal_approximation"


def test_beta_service_marks_low_quality_when_explanatory_power_is_low():
    service = ResearchBetaService({"min_observations_floor": 4})

    result = service.build_result(
        stock_quotes=_quotes("2026-01-01", [0.05, -0.04, 0.03, -0.02, 0.01, -0.03]),
        benchmark_quotes=_quotes("2026-01-01", [0.01, 0.01, -0.01, -0.01, 0.01, -0.01]),
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
        },
        window_days=6,
    )

    assert result.status == "success"
    assert result.quality_flag in {"low", "medium"}
    assert result.interpretation_flags


def test_beta_service_build_results_uses_default_windows():
    service = ResearchBetaService({"windows": [3, 5], "min_observations_floor": 2})
    benchmark_returns = [0.01, -0.02, 0.03, 0.01, -0.01]
    stock_returns = [item * 1.2 for item in benchmark_returns]

    results = service.build_results(
        stock_quotes=_quotes("2026-01-01", stock_returns),
        benchmark_quotes=_quotes("2026-01-01", benchmark_returns),
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
        },
    )

    assert [item.window_days for item in results] == [3, 5]
    assert all(item.status == "success" for item in results)


def test_beta_service_reports_insufficient_observations():
    service = ResearchBetaService({"min_observations_floor": 4})

    result = service.build_result(
        stock_quotes=_quotes("2026-01-01", [0.01, 0.02]),
        benchmark_quotes=_quotes("2026-01-01", [0.01, 0.02]),
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
        },
        window_days=5,
    )

    assert result.status == "unavailable"
    assert result.missing_reason == "insufficient_aligned_observations"
    assert result.observation_count == 2


def test_beta_service_reports_zero_benchmark_variance():
    service = ResearchBetaService({"min_observations_floor": 2})

    result = service.build_result(
        stock_quotes=_quotes("2026-01-01", [0.01, 0.02, 0.03]),
        benchmark_quotes=_quotes("2026-01-01", [0.0, 0.0, 0.0]),
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
        },
        window_days=3,
    )

    assert result.status == "unavailable"
    assert result.missing_reason == "benchmark_return_variance_not_available"


def test_beta_service_reports_missing_benchmark_quotes():
    service = ResearchBetaService()

    result = service.build_result(
        stock_quotes=_quotes("2026-01-01", [0.01, 0.02, 0.03]),
        benchmark_quotes=None,
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
        },
        window_days=3,
    )

    assert result.status == "unavailable"
    assert result.missing_reason == "benchmark_quotes_not_available"


def test_beta_service_aligns_dates_before_calculation():
    service = ResearchBetaService({"min_observations_floor": 2, "min_observation_ratio": 0.5})
    stock_quotes = _quotes("2026-01-01", [0.02, 0.04, 0.06, 0.08])
    benchmark_quotes = _quotes("2026-01-03", [0.01, 0.02, 0.03, 0.04])

    result = service.build_result(
        stock_quotes=stock_quotes,
        benchmark_quotes=benchmark_quotes,
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        benchmark={
            "benchmark_family": "market_default",
            "benchmark_instrument_id": "000300.SH",
        },
        window_days=3,
    )

    assert result.status == "success"
    assert result.observation_count == 2
    assert result.window_start == "2026-01-04"
