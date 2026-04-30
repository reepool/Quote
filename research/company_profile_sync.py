"""
Company profile shadow sync service.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager
from research.empty_support import allows_optional_empty_exchange
from research.providers import CompanyProfileProviderRegistry, CompanyProfileSnapshot
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager


@dataclass(frozen=True)
class CompanyProfileExchangeSyncResult:
    """Per-exchange result for shadow sync."""

    exchange: str
    status: str
    source: Optional[str] = None
    mode: Optional[str] = None
    attempted_sources: List[str] = field(default_factory=list)
    profiles_written: int = 0
    error_message: Optional[str] = None


class CompanyProfileShadowSyncService:
    """Run company profile shadow sync into research.db."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        resolver: Optional[ResearchSourcePolicyResolver] = None,
        registry: Optional[CompanyProfileProviderRegistry] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.resolver = resolver or ResearchSourcePolicyResolver(self.research_config)
        self.registry = registry or CompanyProfileProviderRegistry(
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
        results: List[CompanyProfileExchangeSyncResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    budget_mode=budget_mode,
                    allow_paid_proxy=allow_paid_proxy,
                )
            )

        total_written = sum(result.profiles_written for result in results)
        success_count = sum(1 for result in results if result.status == "success")

        return {
            "status": "success" if success_count else "degraded",
            "exchanges": [asdict(result) for result in results],
            "total_profiles_written": total_written,
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
    ) -> CompanyProfileExchangeSyncResult:
        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]

        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return CompanyProfileExchangeSyncResult(
                exchange=exchange,
                status="skipped",
                error_message="No active stock instruments found for exchange",
            )

        run_id = self.storage.start_ingestion_run(
            domain="company_profile",
            job_name="company_profile_shadow_sync",
            market=exchange,
            metadata={"instrument_count": len(stock_instruments)},
        )

        attempted_sources: List[str] = []
        optional_empty_exchange = allows_optional_empty_exchange(
            self.research_config,
            "company_profile",
            exchange,
        )
        try:
            plan = self.resolver.resolve(
                "company_profile",
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            for candidate in plan.candidates:
                attempted_sources.append(f"{candidate.source}:{candidate.mode}")
                provider = self.registry.get(candidate.source)
                if provider is None or not provider.supports_mode(candidate.mode):
                    continue

                try:
                    snapshots = await provider.fetch_company_profiles(
                        instruments=stock_instruments,
                        exchange=exchange,
                        mode=candidate.mode,
                        limit=limit_per_exchange,
                    )
                except Exception as e:
                    dm_logger.warning(
                        "[CompanyProfileSync] Provider %s (%s) failed for %s: %s",
                        candidate.source,
                        candidate.mode,
                        exchange,
                        e,
                    )
                    continue

                if not snapshots:
                    continue

                for snapshot in snapshots:
                    self.storage.upsert_company_profile(snapshot, ingestion_run_id=run_id)
                    payload_hash = self._hash_payload(snapshot.raw_payload)
                    self.storage.store_raw_payload(
                        domain="company_profile",
                        instrument_id=snapshot.instrument_id,
                        source=snapshot.source,
                        source_mode=snapshot.source_mode,
                        payload=snapshot.raw_payload,
                        payload_hash=payload_hash,
                        ingestion_run_id=run_id,
                    )

                self.storage.finish_ingestion_run(
                    run_id,
                    status="success",
                    rows_written=len(snapshots),
                    metadata={
                        "exchange": exchange,
                        "source": candidate.source,
                        "mode": candidate.mode,
                        "attempted_sources": attempted_sources,
                    },
                )
                return CompanyProfileExchangeSyncResult(
                    exchange=exchange,
                    status="success",
                    source=candidate.source,
                    mode=candidate.mode,
                    attempted_sources=attempted_sources,
                    profiles_written=len(snapshots),
                )

            self.storage.finish_ingestion_run(
                run_id,
                status="success" if optional_empty_exchange else "degraded",
                rows_written=0,
                error_message=None if optional_empty_exchange else "No provider returned company profiles",
                metadata={
                    "exchange": exchange,
                    "attempted_sources": attempted_sources,
                    "optional_empty_exchange": optional_empty_exchange,
                },
            )
            return CompanyProfileExchangeSyncResult(
                exchange=exchange,
                status="success" if optional_empty_exchange else "degraded",
                attempted_sources=attempted_sources,
                error_message=None if optional_empty_exchange else "No provider returned company profiles",
            )

        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=str(e),
                metadata={"exchange": exchange, "attempted_sources": attempted_sources},
            )
            return CompanyProfileExchangeSyncResult(
                exchange=exchange,
                status="failed",
                attempted_sources=attempted_sources,
                error_message=str(e),
            )

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
