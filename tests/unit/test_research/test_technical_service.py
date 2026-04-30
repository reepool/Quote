import pandas as pd

from research.technical_service import ResearchTechnicalAnalysisService


def _build_quotes(rows: int) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=rows, freq="B")
    closes = [10 + i * 0.4 for i in range(rows)]

    return pd.DataFrame(
        {
            "time": dates,
            "instrument_id": ["600000.SH"] * rows,
            "open": [value - 0.2 for value in closes],
            "high": [value + 0.5 for value in closes],
            "low": [value - 0.6 for value in closes],
            "close": closes,
            "volume": [1_000_000 + i * 10_000 for i in range(rows)],
            "amount": [close * 1_000_000 for close in closes],
            "quality_score": [1.0] * rows,
        }
    )


def test_technical_service_builds_bullish_summary():
    service = ResearchTechnicalAnalysisService()
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }

    summary = service.build_summary(
        _build_quotes(90),
        instrument,
        requested_adjustment="qfq",
        applied_adjustment="qfq",
    )

    assert summary is not None
    assert summary["instrument_id"] == "600000.SH"
    assert summary["status"] == "complete"
    assert summary["signal"] == "bullish"
    assert summary["trend_score"] is not None
    assert summary["sma20"] is not None
    assert summary["macd"] is not None
    assert summary["rsi14"] is not None
    assert summary["adx"] is not None
    assert summary["stoch_k"] is not None
    assert summary["cci"] is not None
    assert summary["williams_r"] is not None
    assert summary["quote_summary"]["data_points"] == 90
    assert summary["quote_summary"]["applied_adjustment"] == "qfq"


def test_technical_service_reports_insufficient_data():
    service = ResearchTechnicalAnalysisService()
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }

    summary = service.build_summary(
        _build_quotes(20),
        instrument,
        requested_adjustment="none",
        applied_adjustment="none",
    )

    assert summary is not None
    assert summary["status"] == "insufficient_data"
    assert summary["missing_reason"] == "not_enough_quote_history"
    assert summary["signal"] == "insufficient_data"
    assert summary["trend_score"] is None


def test_technical_service_builds_indicator_series():
    service = ResearchTechnicalAnalysisService()
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }

    indicators = service.build_indicator_series(
        _build_quotes(90),
        instrument,
        requested_adjustment="qfq",
        applied_adjustment="qfq",
        limit=5,
    )

    assert indicators is not None
    assert indicators["instrument_id"] == "600000.SH"
    assert indicators["data_points"] == 5
    assert indicators["requested_adjustment"] == "qfq"
    assert indicators["applied_adjustment"] == "qfq"
    assert len(indicators["items"]) == 5
    assert indicators["items"][-1]["signal"] in {"bullish", "neutral", "bearish", "insufficient_data"}
    assert indicators["items"][-1]["macd"] is not None
    assert indicators["items"][-1]["adx"] is not None
    assert indicators["items"][-1]["stoch_k"] is not None
    assert indicators["items"][-1]["cci"] is not None
    assert indicators["items"][-1]["williams_r"] is not None
