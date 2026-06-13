"""AkShare-backed futures market-data provider."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from research.futures_market_data import (
    FUTURES_SYNC_VERSION,
    FuturesBar,
    FuturesSeries,
    normalize_provider_bars,
)
from research.providers.akshare_support import load_akshare
from utils.config_manager import ResearchConfig


class AkshareFuturesMarketDataProvider:
    """Fetch futures daily bars through AkShare free interfaces.

    The provider intentionally keeps interface names configurable. AkShare
    futures APIs have changed over time; tests use fixture payloads and live
    runs record the exact source interface in lineage.
    """

    source_name = "akshare"
    parser_version = FUTURES_SYNC_VERSION

    def __init__(self, research_config: ResearchConfig):
        self.research_config = research_config
        self.module_cfg = research_config.modules.get("commodity_market_data", {})
        self.source_cfg = (
            research_config.sources.get("akshare", {}).get("futures_market_data", {})
            if research_config.sources
            else {}
        )

    async def fetch_daily_bars(
        self,
        series: FuturesSeries,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
    ) -> List[FuturesBar]:
        return await asyncio.to_thread(
            self._fetch_daily_bars_sync,
            series,
            start_date,
            end_date,
            mode,
        )

    def _fetch_daily_bars_sync(
        self,
        series: FuturesSeries,
        start_date: Optional[str],
        end_date: Optional[str],
        mode: str,
    ) -> List[FuturesBar]:
        akshare = load_akshare(mode)
        interface_name = series.source_interface or self.source_cfg.get(
            "daily_interface",
            "futures_zh_daily_sina",
        )
        if not hasattr(akshare, interface_name):
            raise AttributeError(f"AkShare futures interface not found: {interface_name}")
        fn = getattr(akshare, interface_name)
        frame = self._call_interface(fn, series, start_date=start_date, end_date=end_date)
        rows = self._frame_rows(frame)
        bars = normalize_provider_bars(
            rows,
            series,
            source=self.source_name,
            source_mode=mode,
            source_profile=series.source_profile or "akshare_futures",
            source_interface=interface_name,
            parser_version=self.parser_version,
        )
        if start_date:
            start = str(start_date)[:10]
            bars = [bar for bar in bars if bar.trade_date >= start]
        if end_date:
            end = str(end_date)[:10]
            bars = [bar for bar in bars if bar.trade_date <= end]
        return bars

    def _call_interface(
        self,
        fn: Any,
        series: FuturesSeries,
        *,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Any:
        # AkShare futures interfaces are not perfectly uniform. Try the common
        # symbol-only shape first, then bounded signatures when available.
        call_attempts = [
            {"symbol": series.symbol},
            {"symbol": series.symbol, "start_date": _akshare_date(start_date), "end_date": _akshare_date(end_date)},
            {"symbol": series.symbol, "start_date": start_date, "end_date": end_date},
        ]
        last_error: Optional[Exception] = None
        for kwargs in call_attempts:
            clean_kwargs = {key: value for key, value in kwargs.items() if value is not None}
            try:
                return fn(**clean_kwargs)
            except TypeError as exc:
                last_error = exc
                continue
        if last_error is not None:
            raise last_error
        return fn(series.symbol)

    @staticmethod
    def _frame_rows(frame: Any) -> List[Dict[str, Any]]:
        if isinstance(frame, pd.DataFrame):
            return frame.to_dict("records")
        if isinstance(frame, list):
            return [dict(item) for item in frame if isinstance(item, dict)]
        raise ValueError(f"Unsupported AkShare futures payload type: {type(frame)!r}")


def _akshare_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value)
    return text[:10].replace("-", "")
