from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

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

    @pytest.mark.asyncio
    async def test_a_share_factor_fetch_failure_allows_factory_fallback(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()

        with patch(
            "data_sources.akshare_source.AkShareSource."
            "_fetch_sina_hfq_factor_frame",
            side_effect=RuntimeError("upstream unavailable"),
        ):
            events = await source.get_adjustment_factors(
                instrument_id="600000.SH",
                symbol="600000",
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 7, 15),
            )

        assert events is None

    @pytest.mark.asyncio
    async def test_a_share_factor_timeout_allows_backfill_to_continue(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()

        with (
            patch(
                "data_sources.akshare_source.config_manager.get_nested",
                return_value={"request_timeout_seconds": 0.001},
            ),
            patch(
                "data_sources.akshare_source.AkShareSource."
                "_fetch_sina_hfq_factor_frame",
                side_effect=TimeoutError("socket timeout"),
            ),
        ):
            events = await source.get_adjustment_factors(
                instrument_id="600000.SH",
                symbol="600000",
                start_date=datetime(2026, 1, 1),
                end_date=datetime(2026, 7, 15),
            )

        assert events is None

    @pytest.mark.asyncio
    async def test_a_share_factor_uses_direct_sina_hfq_factor_with_anchor(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime([
                "2019-12-31",
                "2020-05-28",
                "2020-05-29",
            ]),
            "hfq_factor": ["1.0", "1.1", "1.1"],
        })

        with patch(
            "data_sources.akshare_source.AkShareSource."
            "_fetch_sina_hfq_factor_frame",
            return_value=factor_df,
        ) as mock_fetch:
            events = await source.get_adjustment_factors(
                instrument_id="600000.SH",
                symbol="600000",
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2020, 12, 31),
            )

        assert events is not None
        assert len(events) == 1
        assert events[0]["factor"] == pytest.approx(1.1)
        assert events[0]["source"] == "akshare"
        assert events[0]["source_profile"] == "sina_hfq_factor"
        mock_fetch.assert_called_once_with("sh600000", 30.0)

    @pytest.mark.asyncio
    async def test_a_share_factor_returns_valid_empty_window(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["2019-12-31", "2020-05-28"]),
            "factor": [1.0, 1.0],
        })

        with patch(
            "data_sources.akshare_source.AkShareSource."
            "_fetch_sina_hfq_factor_frame",
            return_value=factor_df,
        ):
            events = await source.get_adjustment_factors(
                instrument_id="000001.SZ",
                symbol="000001",
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2020, 12, 31),
            )

        assert events == []

    @pytest.mark.asyncio
    async def test_a_share_factor_ignores_sub_threshold_ratio_drift(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["2019-12-31", "2020-05-28"]),
            "hfq_factor": [100.0, 100.000001],
        })

        with patch(
            "data_sources.akshare_source.AkShareSource."
            "_fetch_sina_hfq_factor_frame",
            return_value=factor_df,
        ):
            events = await source.get_adjustment_factors(
                instrument_id="000001.SZ",
                symbol="000001",
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2020, 12, 31),
            )

        assert events == []

    @pytest.mark.asyncio
    async def test_a_share_factor_accepts_base_only_history_as_zero_events(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["1900-01-01"]),
            "hfq_factor": [1.0],
        })

        with patch(
            "data_sources.akshare_source.AkShareSource."
            "_fetch_sina_hfq_factor_frame",
            return_value=factor_df,
        ):
            events = await source.get_adjustment_factors(
                instrument_id="000001.SZ",
                symbol="000001",
                start_date=datetime(1990, 1, 1),
                end_date=datetime(2026, 7, 30),
            )

        assert events == []

    @pytest.mark.asyncio
    async def test_a_share_factor_rejects_truncated_history(self):
        source = AkShareSource("akshare_test", RateLimitConfig())
        source.rate_limiter.acquire = AsyncMock()
        factor_df = pd.DataFrame({
            "date": pd.to_datetime(["2020-05-28"]),
            "hfq_factor": [1.1],
        })

        with patch(
            "data_sources.akshare_source.AkShareSource."
            "_fetch_sina_hfq_factor_frame",
            return_value=factor_df,
        ):
            events = await source.get_adjustment_factors(
                instrument_id="000001.SZ",
                symbol="000001",
                start_date=datetime(1990, 1, 1),
                end_date=datetime(2026, 7, 30),
                required_coverage_start_date=date(1991, 4, 3),
            )

        assert events is None

    def test_sina_factor_transport_applies_socket_timeout_and_row_count(self):
        response = MagicMock()
        response.text = (
            'var factor={"total":2,"data":'
            '[["1900-01-01","1"],["2020-05-28","1.1"]]};'
        )

        with patch(
            "akshare.stock.stock_zh_a_sina.requests.get",
            return_value=response,
        ) as mock_get:
            result = AkShareSource._fetch_sina_hfq_factor_frame(
                "sz000001",
                30.0,
            )

        response.raise_for_status.assert_called_once_with()
        assert result["hfq_factor"].tolist() == ["1", "1.1"]
        assert mock_get.call_args.kwargs["timeout"] == (10.0, 30.0)

    def test_sina_factor_transport_rejects_incomplete_payload(self):
        response = MagicMock()
        response.text = (
            'var factor={"total":2,"data":[["1900-01-01","1"]]};'
        )

        with (
            patch(
                "akshare.stock.stock_zh_a_sina.requests.get",
                return_value=response,
            ),
            pytest.raises(ValueError, match="response is incomplete"),
        ):
            AkShareSource._fetch_sina_hfq_factor_frame("sz000001", 30.0)
