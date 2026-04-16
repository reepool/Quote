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
