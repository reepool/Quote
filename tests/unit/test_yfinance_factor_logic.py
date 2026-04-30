from datetime import datetime
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from data_sources.base_source import RateLimitConfig
from data_sources.yfinance_source import YFinanceSource


@pytest.mark.unit
class TestYFinanceFactorLogic:
    @pytest.mark.asyncio
    async def test_short_window_does_not_create_synthetic_first_day_event(self):
        source = YFinanceSource("yfinance_hk_stock", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()

        index = pd.to_datetime(["2026-04-09", "2026-04-10", "2026-04-11", "2026-04-12"])
        data = pd.DataFrame(
            {
                "Close": [10.0, 10.0, 10.0, 10.0],
                "Adj Close": [12.0, 12.0, 12.0, 13.2],
            },
            index=index,
        )

        source._fetch_yahoo_data = AsyncMock(return_value=data)

        factors = await source.get_adjustment_factors(
            instrument_id="00001.HK",
            symbol="00001",
            start_date=datetime(2026, 4, 10),
            end_date=datetime(2026, 4, 12),
        )

        assert len(factors) == 1
        assert factors[0]["ex_date"] == datetime(2026, 4, 12)
        assert factors[0]["factor"] == 1.1
        assert factors[0]["cumulative_factor"] == 1.32

    @pytest.mark.asyncio
    async def test_full_window_keeps_initial_anchor_event(self):
        source = YFinanceSource("yfinance_us_stock", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()

        index = pd.to_datetime(["2026-04-10", "2026-04-11", "2026-04-12"])
        data = pd.DataFrame(
            {
                "Close": [20.0, 20.0, 20.0],
                "Adj Close": [24.0, 24.0, 26.4],
            },
            index=index,
        )

        source._fetch_yahoo_data = AsyncMock(return_value=data)

        factors = await source.get_adjustment_factors(
            instrument_id="AAPL.NASDAQ",
            symbol="AAPL",
            start_date=datetime(2026, 4, 10),
            end_date=datetime(2026, 4, 12),
        )

        assert len(factors) == 2
        assert factors[0]["ex_date"] == datetime(2026, 4, 10)
        assert factors[0]["factor"] == 1.2
        assert factors[0]["cumulative_factor"] == 1.2
        assert factors[1]["ex_date"] == datetime(2026, 4, 12)
        assert factors[1]["factor"] == 1.1
        assert factors[1]["cumulative_factor"] == 1.32

    @pytest.mark.asyncio
    async def test_library_path_uses_download_when_proxy_patch_ready(self, monkeypatch):
        source = YFinanceSource("yfinance_us_stock", RateLimitConfig())
        source.proxy_patch_ready = True
        source.yf_proxy_arg = None

        captured = {}
        sample = pd.DataFrame(
            {
                "Open": [1.0],
                "High": [1.1],
                "Low": [0.9],
                "Close": [1.0],
                "Adj Close": [1.0],
                "Volume": [100],
            },
            index=pd.to_datetime(["2026-04-10"]),
        )

        def fake_download(*args, **kwargs):
            captured["download_args"] = args
            captured["download_kwargs"] = kwargs
            return sample

        def fail_ticker(*args, **kwargs):
            raise AssertionError("Ticker path should not be used when proxy patch is ready")

        monkeypatch.setattr("data_sources.yfinance_source.yf.download", fake_download)
        monkeypatch.setattr("data_sources.yfinance_source.yf.Ticker", fail_ticker)

        data = await source._fetch_yahoo_data_library(
            "AAPL",
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 30),
        )

        assert data is not None and not data.empty
        assert captured["download_args"] == ("AAPL",)
        assert captured["download_kwargs"]["progress"] is False
        assert captured["download_kwargs"]["threads"] is False
        assert "proxy" not in captured["download_kwargs"]
