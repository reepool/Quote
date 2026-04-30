"""
Technical latest snapshot refresh service.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import pandas as pd

from research.providers.base import TechnicalIndicatorLatestSnapshot
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig, config_manager

from .technical_service import ResearchTechnicalAnalysisService


AdjustQuotesCallable = Callable[
    [pd.DataFrame, str, Dict[str, Any], str],
    Awaitable[Tuple[pd.DataFrame, str]],
]


@dataclass(frozen=True)
class TechnicalExchangeRefreshResult:
    """Per-exchange result for technical latest snapshot refresh."""

    exchange: str
    status: str
    instruments_processed: int = 0
    rows_written: int = 0
    skipped_instruments: int = 0
    error_message: Optional[str] = None
    missing_quotes: List[str] = field(default_factory=list)


class TechnicalIndicatorLatestRefreshService:
    """Refresh latest technical snapshots from local quotes."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        adjust_quotes: Optional[AdjustQuotesCallable] = None,
        technical_service: Optional[ResearchTechnicalAnalysisService] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        technical_config = self.research_config.modules.get("technical", {})
        summary_config = technical_config.get("summary", {})
        self.technical_service = technical_service or ResearchTechnicalAnalysisService(
            summary_config
        )
        self.adjust_quotes = adjust_quotes

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        adjustment: Optional[str] = None,
        period: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Refresh latest technical snapshots for target exchanges."""
        target_exchanges = exchanges or self.research_config.markets
        technical_config = self.research_config.modules.get("technical", {})
        default_adjustment = str(technical_config.get("default_adjustment", "qfq"))
        latest_cache_config = technical_config.get("latest_cache", {})
        requested_adjustment = str(
            adjustment
            or latest_cache_config.get("adjustment")
            or default_adjustment
        )
        target_period = str(period or latest_cache_config.get("period", "1d"))

        results: List[TechnicalExchangeRefreshResult] = []
        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    adjustment=requested_adjustment,
                    period=target_period,
                )
            )

        return {
            "status": "success" if any(item.status == "success" for item in results) else "degraded",
            "period": target_period,
            "adjustment": requested_adjustment,
            "exchanges": [asdict(item) for item in results],
            "successful_exchanges": sum(1 for item in results if item.status == "success"),
            "attempted_exchanges": len(results),
            "total_rows_written": sum(item.rows_written for item in results),
            "total_instruments_processed": sum(item.instruments_processed for item in results),
        }

    async def _sync_exchange(
        self,
        *,
        exchange: str,
        limit_per_exchange: Optional[int],
        adjustment: str,
        period: str,
    ) -> TechnicalExchangeRefreshResult:
        technical_config = self.research_config.modules.get("technical", {})
        summary_config = technical_config.get("summary", {})
        lookback_bars = int(summary_config.get("lookback_bars", 180))

        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument
            for instrument in instruments
            if str(instrument.get("type", "")).lower() == "stock"
            and instrument.get("is_active", True)
        ]
        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return TechnicalExchangeRefreshResult(
                exchange=exchange,
                status="skipped",
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="technical_indicator_latest",
            job_name="technical_snapshot_refresh",
            market=exchange,
            source="local_quotes",
            mode="derived",
            metadata={
                "instrument_count": len(stock_instruments),
                "lookback_bars": lookback_bars,
                "period": period,
                "adjustment": adjustment,
            },
        )

        rows_written = 0
        instruments_processed = 0
        skipped_instruments = 0
        missing_quotes: List[str] = []

        try:
            for instrument in stock_instruments:
                instrument_id = str(instrument["instrument_id"])
                quotes = await self.db_ops.get_daily_data(
                    instrument_id=instrument_id,
                    limit=lookback_bars,
                    return_format="pandas",
                )
                if quotes is None or quotes.empty:
                    skipped_instruments += 1
                    missing_quotes.append(instrument_id)
                    continue

                processed_quotes = quotes
                applied_adjustment = "none"
                if self.adjust_quotes is not None:
                    processed_quotes, applied_adjustment = await self.adjust_quotes(
                        quotes,
                        instrument_id,
                        instrument,
                        adjustment,
                    )

                summary = self.technical_service.build_summary(
                    processed_quotes,
                    instrument,
                    requested_adjustment=adjustment,
                    applied_adjustment=applied_adjustment,
                )
                if summary is None:
                    skipped_instruments += 1
                    continue

                snapshot = self._build_snapshot(
                    summary,
                    instrument,
                    period=period,
                    adjustment=adjustment,
                    applied_adjustment=applied_adjustment,
                )
                self.storage.upsert_technical_indicator_latest(
                    snapshot,
                    ingestion_run_id=run_id,
                )
                rows_written += 1
                instruments_processed += 1

            status = "success" if rows_written > 0 else "degraded"
            self.storage.finish_ingestion_run(
                run_id,
                status=status,
                rows_written=rows_written,
                metadata={
                    "exchange": exchange,
                    "period": period,
                    "adjustment": adjustment,
                    "lookback_bars": lookback_bars,
                    "instruments_processed": instruments_processed,
                    "skipped_instruments": skipped_instruments,
                    "missing_quotes": missing_quotes,
                },
            )
            return TechnicalExchangeRefreshResult(
                exchange=exchange,
                status=status,
                instruments_processed=instruments_processed,
                rows_written=rows_written,
                skipped_instruments=skipped_instruments,
                missing_quotes=missing_quotes,
            )
        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=rows_written,
                error_message=str(e),
                metadata={
                    "exchange": exchange,
                    "period": period,
                    "adjustment": adjustment,
                    "lookback_bars": lookback_bars,
                    "instruments_processed": instruments_processed,
                    "skipped_instruments": skipped_instruments,
                    "missing_quotes": missing_quotes,
                },
            )
            return TechnicalExchangeRefreshResult(
                exchange=exchange,
                status="failed",
                instruments_processed=instruments_processed,
                rows_written=rows_written,
                skipped_instruments=skipped_instruments,
                error_message=str(e),
                missing_quotes=missing_quotes,
            )

    @staticmethod
    def _build_snapshot(
        summary: Dict[str, Any],
        instrument: Dict[str, Any],
        *,
        period: str,
        adjustment: str,
        applied_adjustment: str,
    ) -> TechnicalIndicatorLatestSnapshot:
        data_as_of = str(summary["data_as_of"])
        as_of_date = data_as_of.split("T", 1)[0]
        quote_summary = summary.get("quote_summary") or {}
        return TechnicalIndicatorLatestSnapshot(
            instrument_id=str(summary.get("instrument_id") or instrument["instrument_id"]),
            symbol=str(summary.get("symbol") or instrument.get("symbol") or ""),
            exchange=str(summary.get("exchange") or instrument.get("exchange") or ""),
            period=period,
            as_of_date=as_of_date,
            adjustment=adjustment,
            applied_adjustment=str(quote_summary.get("applied_adjustment") or applied_adjustment),
            calc_method=str(summary.get("calc_method") or "ta_builtin"),
            calc_version=str(summary.get("calc_version") or "technical_summary.v1"),
            parameter_hash=str(summary.get("parameter_hash") or ""),
            status=str(summary.get("status") or "complete"),
            missing_reason=summary.get("missing_reason"),
            signal=str(summary.get("signal") or "neutral"),
            trend_score=summary.get("trend_score"),
            close_price=summary.get("close"),
            pct_change_1d=summary.get("pct_change_1d"),
            pct_change_20d=summary.get("pct_change_20d"),
            sma20=summary.get("sma20"),
            sma60=summary.get("sma60"),
            ema12=summary.get("ema12"),
            ema26=summary.get("ema26"),
            macd=summary.get("macd"),
            macd_signal=summary.get("macd_signal"),
            macd_hist=summary.get("macd_hist"),
            rsi14=summary.get("rsi14"),
            adx=summary.get("adx"),
            plus_di=summary.get("plus_di"),
            minus_di=summary.get("minus_di"),
            stoch_k=summary.get("stoch_k"),
            stoch_d=summary.get("stoch_d"),
            cci=summary.get("cci"),
            williams_r=summary.get("williams_r"),
            boll_upper=summary.get("boll_upper"),
            boll_middle=summary.get("boll_middle"),
            boll_lower=summary.get("boll_lower"),
            atr14=summary.get("atr14"),
            volume_ratio=summary.get("volume_ratio"),
            distance_to_sma20=summary.get("distance_to_sma20"),
            distance_to_sma60=summary.get("distance_to_sma60"),
            summary_json=summary,
        )
