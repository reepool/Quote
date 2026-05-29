"""
Valuation input synchronization service.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from research.providers.base import BaseValuationInputProvider
from research.providers.registry import ValuationInputProviderRegistry
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig, config_manager


@dataclass(frozen=True)
class ValuationInputExchangeSyncResult:
    """Per-exchange result for valuation input sync."""

    exchange: str
    status: str
    source: str
    source_mode: str
    sync_mode: str
    requested_instruments: int = 0
    snapshots_written: int = 0
    covered_instruments: int = 0
    missing_instruments: int = 0
    missing_instrument_ids: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


class ValuationInputSyncService:
    """Synchronize explicit share-count and market-cap inputs into valuation.db."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        provider_registry: Optional[ValuationInputProviderRegistry] = None,
        provider: Optional[BaseValuationInputProvider] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.provider_registry = provider_registry or ValuationInputProviderRegistry(
            research_config=self.research_config,
        )
        self.provider = provider

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        sync_mode: str = "incremental",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        target_instrument_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        module_cfg = self.research_config.modules.get("valuation", {})
        input_cfg = module_cfg.get("input_sync", {})
        selected_source = source or str(input_cfg.get("primary_source", "cninfo"))
        selected_mode = source_mode or str(input_cfg.get("source_mode", "direct"))
        provider = self.provider or self.provider_registry.get(selected_source)
        if provider is None:
            return {
                "status": "unavailable",
                "reason": f"valuation input provider not found: {selected_source}",
                "source": selected_source,
                "source_mode": selected_mode,
                "sync_mode": sync_mode,
            }
        if not provider.supports_mode(selected_mode):
            return {
                "status": "unavailable",
                "reason": (
                    f"valuation input provider {selected_source} does not support "
                    f"mode {selected_mode}"
                ),
                "source": selected_source,
                "source_mode": selected_mode,
                "sync_mode": sync_mode,
            }

        target_exchanges = exchanges or self.research_config.markets
        results: List[ValuationInputExchangeSyncResult] = []
        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    provider=provider,
                    source=selected_source,
                    source_mode=selected_mode,
                    sync_mode=sync_mode,
                    start_date=start_date,
                    end_date=end_date,
                    limit_per_exchange=limit_per_exchange,
                    target_instrument_ids=target_instrument_ids,
                )
            )

        successful = sum(1 for item in results if item.status == "success")
        written = sum(item.snapshots_written for item in results)
        return {
            "status": "success" if successful else "degraded",
            "source": selected_source,
            "source_mode": selected_mode,
            "sync_mode": sync_mode,
            "start_date": start_date,
            "end_date": end_date,
            "exchanges": [asdict(item) for item in results],
            "successful_exchanges": successful,
            "attempted_exchanges": len(results),
            "total_snapshots_written": written,
            "total_covered_instruments": sum(
                item.covered_instruments for item in results
            ),
            "total_missing_instruments": sum(
                item.missing_instruments for item in results
            ),
        }

    async def _sync_exchange(
        self,
        *,
        exchange: str,
        provider: BaseValuationInputProvider,
        source: str,
        source_mode: str,
        sync_mode: str,
        start_date: Optional[str],
        end_date: Optional[str],
        limit_per_exchange: Optional[int],
        target_instrument_ids: Optional[List[str]],
    ) -> ValuationInputExchangeSyncResult:
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
            return ValuationInputExchangeSyncResult(
                exchange=exchange,
                status="skipped",
                source=source,
                source_mode=source_mode,
                sync_mode=sync_mode,
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="valuation_inputs",
            job_name="valuation_input_sync",
            market=exchange,
            metadata={
                "source": source,
                "source_mode": source_mode,
                "sync_mode": sync_mode,
                "instrument_count": len(stock_instruments),
                "start_date": start_date,
                "end_date": end_date,
                "valuation_db_path": self.storage.valuation_db_path,
            },
        )
        snapshots_written = 0
        try:
            snapshots = await provider.fetch_valuation_inputs(
                instruments=stock_instruments,
                exchange=exchange,
                mode=source_mode,
                sync_mode=sync_mode,
                start_date=start_date,
                end_date=end_date,
                limit=None,
            )
            covered_ids = {
                snapshot.instrument_id for snapshot in snapshots if snapshot.instrument_id
            }
            target_ids = {
                str(instrument.get("instrument_id") or "")
                for instrument in stock_instruments
                if instrument.get("instrument_id")
            }
            for snapshot in snapshots:
                self.storage.upsert_valuation_input(
                    snapshot,
                    ingestion_run_id=run_id,
                )
                snapshots_written += 1

            missing_ids = sorted(target_ids - covered_ids)
            status = "success" if snapshots_written > 0 else "degraded"
            self.storage.finish_ingestion_run(
                run_id,
                status=status,
                rows_written=snapshots_written,
                metadata={
                    "exchange": exchange,
                    "source": source,
                    "source_mode": source_mode,
                    "sync_mode": sync_mode,
                    "requested_instruments": len(stock_instruments),
                    "covered_instruments": len(covered_ids),
                    "missing_instrument_ids": missing_ids[:50],
                    "valuation_db_path": self.storage.valuation_db_path,
                },
            )
            return ValuationInputExchangeSyncResult(
                exchange=exchange,
                status=status,
                source=source,
                source_mode=source_mode,
                sync_mode=sync_mode,
                requested_instruments=len(stock_instruments),
                snapshots_written=snapshots_written,
                covered_instruments=len(covered_ids),
                missing_instruments=len(missing_ids),
                missing_instrument_ids=missing_ids[:20],
            )
        except Exception as exc:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=snapshots_written,
                error_message=str(exc),
                metadata={
                    "exchange": exchange,
                    "source": source,
                    "source_mode": source_mode,
                    "sync_mode": sync_mode,
                    "requested_instruments": len(stock_instruments),
                    "valuation_db_path": self.storage.valuation_db_path,
                },
            )
            return ValuationInputExchangeSyncResult(
                exchange=exchange,
                status="failed",
                source=source,
                source_mode=source_mode,
                sync_mode=sync_mode,
                requested_instruments=len(stock_instruments),
                snapshots_written=snapshots_written,
                error_message=str(exc),
            )
