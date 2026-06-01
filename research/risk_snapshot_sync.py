"""
Risk snapshot rebuild service.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig, config_manager

from .risk_service import ResearchRiskService


@dataclass(frozen=True)
class RiskExchangeRebuildResult:
    """Per-exchange result for risk snapshot rebuild."""

    exchange: str
    status: str
    instruments_processed: int = 0
    rows_written: int = 0
    skipped_instruments: int = 0
    error_message: Optional[str] = None
    missing_quotes: List[str] = field(default_factory=list)


class RiskSnapshotRebuildService:
    """Rebuild risk snapshots from local quotes and research facts."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        risk_service: Optional[ResearchRiskService] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        risk_config = self.research_config.modules.get("risk", {})
        self.risk_service = risk_service or ResearchRiskService(risk_config)

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        results: List[RiskExchangeRebuildResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                )
            )

        return {
            "status": "success" if any(item.status == "success" for item in results) else "degraded",
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
    ) -> RiskExchangeRebuildResult:
        risk_config = self.research_config.modules.get("risk", {})
        lookback_days = max(
            int(risk_config.get("drawdown_window", 252)),
            int(risk_config.get("beta_window", 60)) + 5,
            int(risk_config.get("volatility_window_long", 60)) + 5,
            int(risk_config.get("liquidity_window", 20)) + 5,
        )
        event_window_days = int(risk_config.get("event_window_days", 30))

        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]
        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return RiskExchangeRebuildResult(
                exchange=exchange,
                status="skipped",
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="risk",
            job_name="risk_snapshot_rebuild",
            market=exchange,
            metadata={"instrument_count": len(stock_instruments)},
        )

        benchmark_instrument_id = self.risk_service.parameters.get("benchmark_instrument_id")
        benchmark_quotes = None
        if benchmark_instrument_id:
            benchmark_quotes = await self.db_ops.get_daily_data(
                instrument_id=benchmark_instrument_id,
                limit=lookback_days,
                return_format="pandas",
            )

        rows_written = 0
        instruments_processed = 0
        skipped_instruments = 0
        missing_quotes: List[str] = []

        try:
            for instrument in stock_instruments:
                quotes = await self.db_ops.get_daily_data(
                    instrument_id=instrument["instrument_id"],
                    limit=lookback_days,
                    return_format="pandas",
                )
                if quotes is None or quotes.empty:
                    skipped_instruments += 1
                    missing_quotes.append(instrument["instrument_id"])
                    continue

                financial_bundle = self.storage.get_financial_statement_bundle(
                    instrument["instrument_id"],
                    include_statements=False,
                )
                latest_date = pd.to_datetime(quotes["time"]).max().date()

                event_start_date = (latest_date - timedelta(days=event_window_days)).isoformat()
                event_end_date = latest_date.isoformat()
                negative_event_count = self.storage.get_sentiment_event_count(
                    instrument["instrument_id"],
                    start_date=event_start_date,
                    end_date=event_end_date,
                    negative_only=True,
                )
                snapshot = self.risk_service.build_snapshot(
                    quotes,
                    instrument,
                    financial_bundle,
                    benchmark_quotes=benchmark_quotes,
                    negative_event_count_30d=negative_event_count,
                )
                if snapshot is None:
                    skipped_instruments += 1
                    continue

                self.storage.upsert_risk_snapshot(snapshot, ingestion_run_id=run_id)
                rows_written += 1
                instruments_processed += 1

            status = "success" if rows_written > 0 else "degraded"
            self.storage.finish_ingestion_run(
                run_id,
                status=status,
                rows_written=rows_written,
                metadata={
                    "exchange": exchange,
                    "instruments_processed": instruments_processed,
                    "skipped_instruments": skipped_instruments,
                    "missing_quotes": missing_quotes,
                    "benchmark_instrument_id": benchmark_instrument_id,
                },
            )
            return RiskExchangeRebuildResult(
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
                    "instruments_processed": instruments_processed,
                    "skipped_instruments": skipped_instruments,
                    "missing_quotes": missing_quotes,
                    "benchmark_instrument_id": benchmark_instrument_id,
                },
            )
            return RiskExchangeRebuildResult(
                exchange=exchange,
                status="failed",
                instruments_processed=instruments_processed,
                rows_written=rows_written,
                skipped_instruments=skipped_instruments,
                error_message=str(e),
                missing_quotes=missing_quotes,
            )
