"""
Valuation history rebuild service.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from research.storage import ResearchStorageManager
from utils import dm_logger
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
        quote_limit_days: Optional[int] = None,
        progress_log_every: int = 200,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        results: List[ValuationExchangeRebuildResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    target_instrument_ids=target_instrument_ids,
                    quote_limit_days=quote_limit_days,
                    progress_log_every=progress_log_every,
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
        quote_limit_days: Optional[int],
        progress_log_every: int,
    ) -> ValuationExchangeRebuildResult:
        valuation_config = self.research_config.modules.get("valuation", {})
        history_config = valuation_config.get("history", {})
        configured_lookback_days = int(history_config.get("lookback_days", 252))
        lookback_days = int(quote_limit_days or configured_lookback_days)

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
            metadata={
                "instrument_count": len(stock_instruments),
                "lookback_days": lookback_days,
                "configured_lookback_days": configured_lookback_days,
            },
        )
        dm_logger.info(
            "[ValuationHistoryRebuild] exchange=%s start instruments=%s lookback_days=%s limit_per_exchange=%s target_count=%s",
            exchange,
            len(stock_instruments),
            lookback_days,
            limit_per_exchange,
            len(target_instrument_ids or []),
        )

        rows_written = 0
        instruments_processed = 0
        skipped_instruments = 0
        missing_financials: List[str] = []
        missing_valuation_inputs: List[str] = []

        try:
            total_instruments = len(stock_instruments)
            log_every = max(1, int(progress_log_every or 200))
            def _log_progress(index: int, instrument: Dict[str, Any]) -> None:
                if index == 1 or index == total_instruments or index % log_every == 0:
                    dm_logger.info(
                        "[ValuationHistoryRebuild] exchange=%s progress=%s/%s processed=%s skipped=%s rows=%s missing_financials=%s missing_inputs=%s current=%s",
                        exchange,
                        index,
                        total_instruments,
                        instruments_processed,
                        skipped_instruments,
                        rows_written,
                        len(missing_financials),
                        len(missing_valuation_inputs),
                        instrument.get("instrument_id"),
                    )

            for index, instrument in enumerate(stock_instruments, start=1):
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
                    _log_progress(index, instrument)
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
                    _log_progress(index, instrument)
                    continue

                snapshots = self.valuation_service.build_history_snapshots(
                    quotes,
                    instrument,
                    bundle,
                )
                if not snapshots:
                    skipped_instruments += 1
                    missing_financials.append(instrument["instrument_id"])
                    _log_progress(index, instrument)
                    continue

                for snapshot in snapshots:
                    self.storage.upsert_valuation_history(
                        snapshot,
                        ingestion_run_id=run_id,
                    )
                    rows_written += 1

                instruments_processed += 1
                _log_progress(index, instrument)

            status = "success" if rows_written > 0 else "degraded"
            dm_logger.info(
                "[ValuationHistoryRebuild] exchange=%s finished status=%s processed=%s skipped=%s rows=%s missing_financials=%s missing_inputs=%s",
                exchange,
                status,
                instruments_processed,
                skipped_instruments,
                rows_written,
                len(missing_financials),
                len(missing_valuation_inputs),
            )
            self.storage.finish_ingestion_run(
                run_id,
                status=status,
                rows_written=rows_written,
                metadata={
                    "exchange": exchange,
                    "lookback_days": lookback_days,
                    "configured_lookback_days": configured_lookback_days,
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
            dm_logger.error(
                "[ValuationHistoryRebuild] exchange=%s failed processed=%s skipped=%s rows=%s error=%s",
                exchange,
                instruments_processed,
                skipped_instruments,
                rows_written,
                e,
            )
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=rows_written,
                error_message=str(e),
                metadata={
                    "exchange": exchange,
                    "lookback_days": lookback_days,
                    "configured_lookback_days": configured_lookback_days,
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
