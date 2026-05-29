"""
Valuation history rebuild service.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig, config_manager

from .valuation_service import ResearchValuationService


@dataclass(frozen=True)
class ValuationExchangeRebuildResult:
    """Per-exchange result for valuation history rebuild."""

    exchange: str
    status: str
    instruments_processed: int = 0
    rows_written: int = 0
    skipped_instruments: int = 0
    error_message: Optional[str] = None
    missing_financials: List[str] = field(default_factory=list)
    missing_valuation_inputs: List[str] = field(default_factory=list)


class ValuationHistoryRebuildService:
    """Rebuild valuation history from local quotes and financial facts."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        valuation_service: Optional[ResearchValuationService] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        valuation_config = self.research_config.modules.get("valuation", {})
        self.valuation_service = valuation_service or ResearchValuationService(valuation_config)

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        results: List[ValuationExchangeRebuildResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    target_instrument_ids=target_instrument_ids,
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
        target_instrument_ids: Optional[List[str]],
    ) -> ValuationExchangeRebuildResult:
        valuation_config = self.research_config.modules.get("valuation", {})
        history_config = valuation_config.get("history", {})
        lookback_days = int(history_config.get("lookback_days", 252))

        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]
        if target_instrument_ids:
            target_set = {str(item).strip() for item in target_instrument_ids if str(item).strip()}
            stock_instruments = [
                instrument
                for instrument in stock_instruments
                if str(instrument.get("instrument_id") or "").strip() in target_set
            ]
        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return ValuationExchangeRebuildResult(
                exchange=exchange,
                status="skipped",
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="valuation_history",
            job_name="valuation_history_rebuild",
            market=exchange,
            metadata={"instrument_count": len(stock_instruments)},
        )

        rows_written = 0
        instruments_processed = 0
        skipped_instruments = 0
        missing_financials: List[str] = []
        missing_valuation_inputs: List[str] = []

        try:
            for instrument in stock_instruments:
                with self.storage.financial_database_scope():
                    bundle = self.storage.get_financial_statement_bundle(
                        instrument["instrument_id"],
                        include_statements=False,
                    )
                    core_facts = self.storage.get_financial_core_facts(
                        instrument["instrument_id"],
                        include_history=True,
                        limit=12,
                    )
                if bundle is None and not core_facts:
                    skipped_instruments += 1
                    missing_financials.append(instrument["instrument_id"])
                    continue
                if bundle is None:
                    bundle = dict(core_facts[0])
                else:
                    bundle = dict(bundle)
                bundle["financial_history"] = core_facts or [bundle]
                valuation_inputs = self.storage.get_valuation_inputs(
                    instrument["instrument_id"],
                    limit=0,
                )
                bundle["valuation_inputs"] = valuation_inputs
                if not valuation_inputs:
                    missing_valuation_inputs.append(instrument["instrument_id"])

                quotes = await self.db_ops.get_daily_data(
                    instrument_id=instrument["instrument_id"],
                    limit=lookback_days,
                    return_format="pandas",
                )
                if quotes is None or quotes.empty:
                    skipped_instruments += 1
                    continue

                snapshots = self.valuation_service.build_history_snapshots(
                    quotes,
                    instrument,
                    bundle,
                )
                if not snapshots:
                    skipped_instruments += 1
                    missing_financials.append(instrument["instrument_id"])
                    continue

                for snapshot in snapshots:
                    self.storage.upsert_valuation_history(
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
                    "lookback_days": lookback_days,
                    "instruments_processed": instruments_processed,
                    "skipped_instruments": skipped_instruments,
                    "missing_financials": missing_financials,
                    "missing_valuation_inputs": missing_valuation_inputs,
                    "valuation_db_path": self.storage.valuation_db_path,
                },
            )
            return ValuationExchangeRebuildResult(
                exchange=exchange,
                status=status,
                instruments_processed=instruments_processed,
                rows_written=rows_written,
                skipped_instruments=skipped_instruments,
                missing_financials=missing_financials,
                missing_valuation_inputs=missing_valuation_inputs,
            )
        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=rows_written,
                error_message=str(e),
                metadata={
                    "exchange": exchange,
                    "lookback_days": lookback_days,
                    "instruments_processed": instruments_processed,
                    "skipped_instruments": skipped_instruments,
                    "missing_financials": missing_financials,
                    "missing_valuation_inputs": missing_valuation_inputs,
                    "valuation_db_path": self.storage.valuation_db_path,
                },
            )
            return ValuationExchangeRebuildResult(
                exchange=exchange,
                status="failed",
                instruments_processed=instruments_processed,
                rows_written=rows_written,
                skipped_instruments=skipped_instruments,
                error_message=str(e),
                missing_financials=missing_financials,
                missing_valuation_inputs=missing_valuation_inputs,
            )
