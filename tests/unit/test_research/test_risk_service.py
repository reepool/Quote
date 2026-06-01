import pandas as pd

from research.risk_service import ResearchRiskService


def test_risk_service_builds_snapshot_from_quotes_and_financials():
    service = ResearchRiskService({"benchmark_instrument_id": "000300.SH"})
    times = pd.date_range("2026-01-01", periods=90, freq="B")
    quotes = pd.DataFrame(
        {
            "time": times,
            "close": [100 + i * 0.6 for i in range(len(times))],
            "amount": [2.5e8 + i * 1e6 for i in range(len(times))],
            "turnover": [1.2 + (i % 5) * 0.1 for i in range(len(times))],
        }
    )
    benchmark_quotes = pd.DataFrame(
        {
            "time": times,
            "close": [4000 + i * 8 for i in range(len(times))],
        }
    )

    snapshot = service.build_snapshot(
        quotes,
        {"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"},
        {
            "report_period": "2025-12-31",
            "total_assets": 1200.0,
            "total_liabilities": 280.0,
            "current_assets": 420.0,
            "current_liabilities": 200.0,
            "operating_cf": 260.0,
            "net_income": 220.0,
        },
        benchmark_quotes=benchmark_quotes,
        negative_event_count_30d=1,
    )

    assert snapshot is not None
    assert snapshot.volatility_20d is not None
    assert snapshot.beta_60d is not None
    assert snapshot.risk_level in {"low", "medium", "high"}
    assert snapshot.negative_event_count_30d == 1

