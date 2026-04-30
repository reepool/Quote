"""
Financial statements shadow sync service.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from research.providers import FinancialStatementsProviderRegistry
from research.empty_support import allows_optional_empty_exchange
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager


@dataclass(frozen=True)
class FinancialStatementsExchangeSyncResult:
    """Per-exchange result for financial statements shadow sync."""

    exchange: str
    status: str
    source: Optional[str] = None
    mode: Optional[str] = None
    attempted_sources: List[str] = field(default_factory=list)
    bundles_written: int = 0
    raw_rows_written: int = 0
    error_message: Optional[str] = None


class FinancialStatementsShadowSyncService:
    """Run financial statements shadow sync into research.db."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        resolver: Optional[ResearchSourcePolicyResolver] = None,
        registry: Optional[FinancialStatementsProviderRegistry] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.resolver = resolver or ResearchSourcePolicyResolver(self.research_config)
        self.registry = registry or FinancialStatementsProviderRegistry(
            research_config=self.research_config,
        )

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        results: List[FinancialStatementsExchangeSyncResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    budget_mode=budget_mode,
                    allow_paid_proxy=allow_paid_proxy,
                )
            )

        total_bundles_written = sum(result.bundles_written for result in results)
        total_raw_rows_written = sum(result.raw_rows_written for result in results)
        success_count = sum(1 for result in results if result.status == "success")

        return {
            "status": "success" if success_count else "degraded",
            "exchanges": [asdict(result) for result in results],
            "total_bundles_written": total_bundles_written,
            "total_raw_rows_written": total_raw_rows_written,
            "successful_exchanges": success_count,
            "attempted_exchanges": len(results),
        }

    async def _sync_exchange(
        self,
        *,
        exchange: str,
        limit_per_exchange: Optional[int],
        budget_mode: Optional[str],
        allow_paid_proxy: Optional[bool],
    ) -> FinancialStatementsExchangeSyncResult:
        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]

        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return FinancialStatementsExchangeSyncResult(
                exchange=exchange,
                status="skipped",
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="financial_statements",
            job_name="financial_statements_shadow_sync",
            market=exchange,
            metadata={"instrument_count": len(stock_instruments)},
        )

        attempted_sources: List[str] = []
        optional_empty_exchange = allows_optional_empty_exchange(
            self.research_config,
            "financial_statements",
            exchange,
        )
        try:
            plan = self.resolver.resolve(
                "financial_statements",
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            for candidate in plan.candidates:
                attempted_sources.append(f"{candidate.source}:{candidate.mode}")
                provider = self.registry.get(candidate.source)
                if provider is None or not provider.supports_mode(candidate.mode):
                    continue

                try:
                    bundles = await provider.fetch_financial_statement_bundles(
                        instruments=stock_instruments,
                        exchange=exchange,
                        mode=candidate.mode,
                        limit=limit_per_exchange,
                    )
                except Exception as e:
                    dm_logger.warning(
                        "[FinancialStatementsSync] Provider %s (%s) failed for %s: %s",
                        candidate.source,
                        candidate.mode,
                        exchange,
                        e,
                    )
                    continue

                if not bundles:
                    continue

                raw_rows_written = 0
                for bundle in bundles:
                    self.storage.upsert_financial_statement_bundle(
                        bundle,
                        ingestion_run_id=run_id,
                    )
                    raw_rows_written += len(bundle.raw_statements)
                    payload_hash = self._hash_payload(bundle.raw_payload)
                    self.storage.store_raw_payload(
                        domain="financial_statements",
                        instrument_id=bundle.instrument_id,
                        source=bundle.source,
                        source_mode=bundle.source_mode,
                        payload=bundle.raw_payload,
                        payload_hash=payload_hash,
                        ingestion_run_id=run_id,
                    )

                self.storage.finish_ingestion_run(
                    run_id,
                    status="success",
                    rows_written=len(bundles) + raw_rows_written,
                    metadata={
                        "exchange": exchange,
                        "source": candidate.source,
                        "mode": candidate.mode,
                        "attempted_sources": attempted_sources,
                    },
                )
                return FinancialStatementsExchangeSyncResult(
                    exchange=exchange,
                    status="success",
                    source=candidate.source,
                    mode=candidate.mode,
                    attempted_sources=attempted_sources,
                    bundles_written=len(bundles),
                    raw_rows_written=raw_rows_written,
                )

            self.storage.finish_ingestion_run(
                run_id,
                status="success" if optional_empty_exchange else "degraded",
                rows_written=0,
                error_message=None if optional_empty_exchange else "No provider returned financial statement bundles",
                metadata={
                    "exchange": exchange,
                    "attempted_sources": attempted_sources,
                    "optional_empty_exchange": optional_empty_exchange,
                },
            )
            return FinancialStatementsExchangeSyncResult(
                exchange=exchange,
                status="success" if optional_empty_exchange else "degraded",
                attempted_sources=attempted_sources,
                error_message=None if optional_empty_exchange else "No provider returned financial statement bundles",
            )
        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=str(e),
                metadata={"exchange": exchange, "attempted_sources": attempted_sources},
            )
            return FinancialStatementsExchangeSyncResult(
                exchange=exchange,
                status="failed",
                attempted_sources=attempted_sources,
                error_message=str(e),
            )

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
