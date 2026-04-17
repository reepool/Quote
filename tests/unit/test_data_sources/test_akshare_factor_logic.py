from datetime import datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from data_sources.akshare_source import AkShareSource
from data_sources.base_source import RateLimitConfig


def _series(values):
    dates = pd.to_datetime(list(values.keys()))
    return pd.Series(list(values.values()), index=dates)


@pytest.mark.unit
class TestAkshareFactorLogic:
    def test_sparse_factor_events_skip_anchor_row_inside_short_window(self):
        source = AkShareSource("akshare_test", RateLimitConfig())

        events = source._build_sparse_factor_events(
            instrument_id="00001.HK",
            cum_factor=_series({
                "2026-04-10": 2.0,
                "2026-04-13": 2.0,
            }),
            requested_start=datetime(2026, 4, 13).date(),
            requested_end=datetime(2026, 4, 13).date(),
            threshold=0.06,
            source="akshare",
        )

        assert events == []

    def test_sparse_factor_events_detect_real_change_with_anchor(self):
        source = AkShareSource("akshare_test", RateLimitConfig())

        events = source._build_sparse_factor_events(
            instrument_id="00001.HK",
            cum_factor=_series({
                "2026-04-10": 2.0,
                "2026-04-13": 2.2,
            }),
            requested_start=datetime(2026, 4, 13).date(),
            requested_end=datetime(2026, 4, 13).date(),
            threshold=0.06,
            source="akshare",
        )

        assert len(events) == 1
        assert events[0]["ex_date"].date() == datetime(2026, 4, 13).date()
        assert events[0]["factor"] == 1.1
        assert events[0]["cumulative_factor"] == 2.2

    def test_sparse_factor_events_keep_first_point_only_without_anchor(self):
        source = AkShareSource("akshare_test", RateLimitConfig())

        events = source._build_sparse_factor_events(
            instrument_id="00001.HK",
            cum_factor=_series({
                "2026-04-10": 2.0,
                "2026-04-13": 2.0,
            }),
            requested_start=datetime(2026, 4, 10).date(),
            requested_end=datetime(2026, 4, 13).date(),
            threshold=0.06,
            source="akshare",
        )

        assert len(events) == 1
        assert events[0]["ex_date"].date() == datetime(2026, 4, 10).date()
        assert events[0]["factor"] == 2.0
        assert events[0]["cumulative_factor"] == 2.0

    @pytest.mark.asyncio
    async def test_hk_factor_uses_single_qfq_factor_call_with_history_anchor(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()

        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["1900-01-01", "2026-04-10", "2026-04-13"]),
            "qfq_factor": [1.0, 2.0, 2.2],
        })

        with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(return_value=factor_df)) as mock_to_thread:
            events = await source._get_hk_adjustment_factors(
                instrument_id="00001.HK",
                symbol="00001",
                start_date=datetime(2026, 4, 13),
                end_date=datetime(2026, 4, 13),
            )

        assert len(events) == 1
        assert events[0]["ex_date"].date() == datetime(2026, 4, 13).date()
        assert events[0]["factor"] == 1.1
        assert events[0]["cumulative_factor"] == 2.2
        assert mock_to_thread.await_count == 1
