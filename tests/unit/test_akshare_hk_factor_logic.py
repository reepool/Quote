from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from data_sources.akshare_source import AkShareSource
from data_sources.base_source import RateLimitConfig
from utils.adjustment import AdjustmentEngine


@pytest.mark.unit
class TestAkshareHkFactorLogic:
    def test_rounding_noise_does_not_create_factor_event(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        cum_factor = pd.Series(
            [1.0000, 1.0003, 0.9998, 1.0002],
            index=pd.to_datetime(["2026-04-10", "2026-04-11", "2026-04-12", "2026-04-13"]),
        )

        factors = source._build_sparse_factor_events(
            instrument_id="00001.HK",
            cum_factor=cum_factor,
            requested_start=date(2026, 4, 10),
            requested_end=date(2026, 4, 13),
            threshold=0.0,
            source="akshare",
            event_mode="factor_ratio",
            rel_tol=source._HK_FACTOR_RATIO_REL_TOL,
            abs_tol=source._HK_FACTOR_RATIO_ABS_TOL,
        )

        assert factors == []

    def test_real_hk_factor_change_still_detected(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        cum_factor = pd.Series(
            [1.0000, 1.0003, 1.0185],
            index=pd.to_datetime(["2026-04-10", "2026-04-11", "2026-04-12"]),
        )

        factors = source._build_sparse_factor_events(
            instrument_id="00001.HK",
            cum_factor=cum_factor,
            requested_start=date(2026, 4, 10),
            requested_end=date(2026, 4, 12),
            threshold=0.0,
            source="akshare",
            event_mode="factor_ratio",
            rel_tol=source._HK_FACTOR_RATIO_REL_TOL,
            abs_tol=source._HK_FACTOR_RATIO_ABS_TOL,
        )

        assert len(factors) == 1
        assert factors[0]["ex_date"].date() == date(2026, 4, 12)
        assert factors[0]["source"] == "akshare"
        assert factors[0]["cumulative_factor"] == 1.0185
        assert factors[0]["factor"] == round(1.0185 / 1.0003, 6)

    def test_plateau_denoise_keeps_only_persistent_shift(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        cum_factor = pd.Series(
            [
                1.0000, 1.0004, 0.9999, 1.0003,
                1.0262, 1.0265, 1.0260, 1.0264,
            ],
            index=pd.to_datetime([
                "2026-04-08", "2026-04-09", "2026-04-10", "2026-04-11",
                "2026-04-12", "2026-04-13", "2026-04-14", "2026-04-15",
            ]),
        )

        factors = source._build_hk_plateau_factor_events(
            instrument_id="00001.HK",
            cum_factor=cum_factor,
            requested_start=date(2026, 4, 8),
            requested_end=date(2026, 4, 15),
            source="akshare",
        )

        assert len(factors) == 1
        assert factors[0]["ex_date"].date() == date(2026, 4, 12)
        assert abs(factors[0]["factor"] - 1.0262) < 0.002

    def test_hk_cumulative_factor_uses_ohlc_median(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        idx = pd.to_datetime(["2026-04-10", "2026-04-11"])
        raw_df = pd.DataFrame(
            {
                "开盘": [10.0, 10.0],
                "最高": [10.2, 10.2],
                "最低": [9.8, 9.8],
                "收盘": [10.0, 10.0],
            },
            index=idx,
        )
        hfq_df = pd.DataFrame(
            {
                "开盘": [10.0, 10.1],
                "最高": [10.2, 10.302],
                "最低": [9.8, 9.898],
                "收盘": [10.0, 10.4],
            },
            index=idx,
        )

        cum = source._build_hk_cumulative_factor(raw_df, hfq_df)

        assert len(cum) == 2
        assert cum.iloc[0] == 1.0
        assert abs(cum.iloc[1] - 1.01) < 1e-9

    def test_hk_reliability_rejects_too_many_events(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        factors = [
            {
                "instrument_id": "00001.HK",
                "ex_date": pd.Timestamp("2026-01-01") + pd.Timedelta(days=i * 5),
                "factor": 1.01,
                "cumulative_factor": 1.01 + i * 0.001,
                "source": "akshare",
            }
            for i in range(10)
        ]

        reliable = source._hk_factor_result_is_reliable(
            factors,
            requested_start=date(2026, 1, 1),
            requested_end=date(2026, 2, 28),
        )

        assert reliable is False

    def test_hk_reliability_accepts_sparse_events(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        factors = [
            {
                "instrument_id": "00001.HK",
                "ex_date": pd.Timestamp("2026-01-15"),
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "source": "akshare",
            },
            {
                "instrument_id": "00001.HK",
                "ex_date": pd.Timestamp("2026-03-15"),
                "factor": 1.02,
                "cumulative_factor": 1.03,
                "source": "akshare",
            },
        ]

        reliable = source._hk_factor_result_is_reliable(
            factors,
            requested_start=date(2026, 1, 1),
            requested_end=date(2026, 4, 30),
        )

        assert reliable is True


@pytest.mark.unit
class TestAkshareHkDirectQfqFactorFetch:
    def _source(self):
        source = AkShareSource("akshare_hk_stock", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()
        return source

    @pytest.mark.asyncio
    async def test_qfq_factor_single_call_builds_multiplicative_events(self):
        source = self._source()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["1900-01-01", "2024-05-17", "2025-05-16"]),
            "qfq_factor": [0.5, 0.75, 1.0],
        })

        with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(return_value=factor_df)) as mock_thread:
            events = await source._get_hk_adjustment_factors(
                instrument_id="00700.HK",
                symbol="00700",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2025, 12, 31),
            )

        assert mock_thread.await_count == 1
        assert source.rate_limiter.acquire.await_count == 1
        assert [event["ex_date"].date() for event in events] == [
            date(2024, 5, 17),
            date(2025, 5, 16),
        ]
        assert events[0]["factor"] == 1.5
        assert events[0]["cumulative_factor"] == 1.5
        assert events[0]["event_type"] == "mixed"
        assert events[1]["factor"] == round(2.0 / 1.5, 6)
        assert events[1]["cumulative_factor"] == 2.0

    @pytest.mark.asyncio
    async def test_qfq_factor_uses_history_before_requested_start_as_anchor(self):
        source = self._source()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["1900-01-01", "2024-05-17", "2025-05-16"]),
            "qfq_factor": [0.5, 0.75, 1.0],
        })

        with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(return_value=factor_df)):
            events = await source._get_hk_adjustment_factors(
                instrument_id="00700.HK",
                symbol="00700",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 12, 31),
            )

        assert len(events) == 1
        assert events[0]["ex_date"].date() == date(2025, 5, 16)
        assert events[0]["factor"] == round(2.0 / 1.5, 6)
        assert events[0]["cumulative_factor"] == 2.0

    @pytest.mark.asyncio
    async def test_qfq_factor_sentinel_only_returns_empty_list(self):
        source = self._source()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["1900-01-01"]),
            "qfq_factor": [1.0],
        })

        with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(return_value=factor_df)):
            events = await source._get_hk_adjustment_factors(
                instrument_id="01810.HK",
                symbol="01810",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2025, 12, 31),
            )

        assert events == []

    @pytest.mark.asyncio
    async def test_qfq_factor_missing_base_row_returns_none_by_default(self):
        source = self._source()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["2024-05-17"]),
            "qfq_factor": [1.0],
        })

        with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(return_value=factor_df)):
            events = await source._get_hk_adjustment_factors(
                instrument_id="00700.HK",
                symbol="00700",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2025, 12, 31),
            )

        assert events is None

    @pytest.mark.asyncio
    async def test_invalid_response_can_be_configured_as_empty_result(self):
        source = self._source()
        cfg = {
            "api": "stock_hk_daily",
            "factor_adjust": "qfq-factor",
            "base_date": "1900-01-01",
            "require_base_date": True,
            "fallback_on_invalid_response": False,
            "rounding_tolerance": 0.0001,
            "diagnostic_hfq_check_enabled": False,
        }

        with patch("data_sources.akshare_source.config_manager.get_nested", return_value=cfg):
            with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(side_effect=Exception("network error"))):
                events = await source._get_hk_adjustment_factors(
                    instrument_id="00700.HK",
                    symbol="00700",
                    start_date=datetime(2024, 1, 1),
                    end_date=datetime(2025, 12, 31),
                )

        assert events == []

    @pytest.mark.asyncio
    async def test_project_forward_adjust_matches_sina_qfq_factor_math(self):
        source = self._source()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["1900-01-01", "2024-05-17", "2025-05-16"]),
            "qfq_factor": [0.5, 0.75, 1.0],
        })

        with patch("data_sources.akshare_source.asyncio.to_thread", new=AsyncMock(return_value=factor_df)):
            factors = await source._get_hk_adjustment_factors(
                instrument_id="00700.HK",
                symbol="00700",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2025, 12, 31),
            )

        raw_quotes = [
            {
                "time": datetime(2024, 5, 17),
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 100.0,
                "volume": 1000,
            },
            {
                "time": datetime(2025, 5, 16),
                "open": 200.0,
                "high": 210.0,
                "low": 190.0,
                "close": 200.0,
                "volume": 1000,
            },
        ]

        adjusted = AdjustmentEngine.forward_adjust(raw_quotes, factors)

        assert adjusted[0]["close"] == 75.0
        assert adjusted[0]["open"] == 75.0
        assert adjusted[1]["close"] == 200.0
        assert adjusted[1]["open"] == 200.0
