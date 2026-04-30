"""
Authoritative Shenwan industry sync service.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Dict, List, Optional

from research.empty_support import allows_optional_empty_exchange
from research.official_shenwan_mapping import (
    OfficialShenwanCodeMapper,
    OfficialShenwanCodeMapping,
)
from research.providers import (
    BaseOfficialIndustryHistoryProvider,
    BaseIndustryStandardProvider,
    IndustryNameHintSnapshot,
    IndustrySnapshot,
    IndustryNameSupplementProviderRegistry,
    IndustryStandardProviderRegistry,
    IndustryTaxonomySnapshot,
    OfficialIndustryClassificationSnapshot,
    OfficialIndustryHistorySnapshot,
    OfficialIndustryHistoryProviderRegistry,
)
from research.providers.base import build_taxonomy_children_index, get_leaf_taxonomy_nodes
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager


@dataclass(frozen=True)
class IndustryStandardExchangeSyncResult:
    """Per-exchange result for authoritative Shenwan sync."""

    exchange: str
    status: str
    memberships_written: int = 0
    official_classifications_written: int = 0
    source: Optional[str] = None
    mode: Optional[str] = None
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass(frozen=True)
class _OfficialMappingContext:
    """Reusable mapping context for one candidate source."""

    mapping_by_code: Dict[str, OfficialShenwanCodeMapping]
    taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot]
    mapped_code_count: int
    unmapped_code_count: int
    total_code_count: int
    component_taxonomy_count: int
    mapping_source: str
    cache_row_count: int = 0
    cache_built_at: Optional[str] = None
    component_cache_source: Optional[str] = None
    component_cache_built_at: Optional[str] = None


class IndustryStandardSyncService:
    """Sync authoritative Shenwan taxonomy and stock memberships into research.db."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        resolver: Optional[ResearchSourcePolicyResolver] = None,
        registry: Optional[IndustryStandardProviderRegistry] = None,
        supplement_registry: Optional[IndustryNameSupplementProviderRegistry] = None,
        official_registry: Optional[OfficialIndustryHistoryProviderRegistry] = None,
        code_mapper: Optional[OfficialShenwanCodeMapper] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.resolver = resolver or ResearchSourcePolicyResolver(self.research_config)
        self.registry = registry or IndustryStandardProviderRegistry(
            research_config=self.research_config
        )
        self.supplement_registry = supplement_registry or IndustryNameSupplementProviderRegistry(
            research_config=self.research_config
        )
        self.official_registry = official_registry or OfficialIndustryHistoryProviderRegistry(
            research_config=self.research_config
        )
        self.code_mapper = code_mapper or self._build_code_mapper()

    async def _list_target_stock_instruments(
        self,
        exchange: str,
    ) -> List[Dict[str, Any]]:
        """Return research target stock instruments for one exchange."""
        reader = getattr(
            self.db_ops,
            "get_research_target_instruments_by_exchange",
            None,
        )
        if reader is not None:
            result = reader(exchange, is_active=True)
            if hasattr(result, "__await__"):
                result = await result
            if isinstance(result, list):
                return result

        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        return [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]

    async def refresh_official_mapping_cache(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        run_id = self.storage.start_ingestion_run(
            domain="industry_official_mapping_refresh",
            job_name="industry_official_mapping_refresh",
            market=",".join(target_exchanges),
            metadata={"requested_exchanges": target_exchanges},
        )

        attempted_sources: List[str] = []
        last_error: Optional[str] = None
        try:
            plan = self.resolver.resolve(
                "industry_standard",
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            for candidate in plan.candidates:
                attempted_sources.append(f"{candidate.source}:{candidate.mode}")
                provider = self.registry.get(candidate.source)
                official_provider = self.official_registry.get(candidate.source)
                if provider is None or official_provider is None:
                    continue
                if not provider.supports_mode(candidate.mode):
                    continue
                if not official_provider.supports_mode(candidate.mode):
                    continue

                try:
                    taxonomy_nodes = await provider.fetch_taxonomy(mode=candidate.mode)
                    if not taxonomy_nodes:
                        continue

                    for node in taxonomy_nodes:
                        self.storage.upsert_industry_taxonomy(node)

                    official_context = await self._rebuild_official_mapping_context(
                        provider=provider,
                        official_provider=official_provider,
                        taxonomy_nodes=taxonomy_nodes,
                        mode=candidate.mode,
                    )
                except Exception as e:
                    last_error = str(e)
                    dm_logger.warning(
                        "[IndustryStandardSync] Official mapping refresh failed for %s (%s): %s",
                        candidate.source,
                        candidate.mode,
                        e,
                    )
                    continue

                result = {
                    "status": "success",
                    "source": candidate.source,
                    "mode": candidate.mode,
                    "attempted_sources": attempted_sources,
                    "taxonomy_nodes_written": len(taxonomy_nodes),
                    "mapping_cache_rows_written": official_context.cache_row_count,
                    "mapped_code_count": official_context.mapped_code_count,
                    "unmapped_code_count": official_context.unmapped_code_count,
                    "total_code_count": official_context.total_code_count,
                    "component_taxonomy_count": official_context.component_taxonomy_count,
                    "mapping_source": official_context.mapping_source,
                    "cache_built_at": official_context.cache_built_at,
                    "component_cache_source": official_context.component_cache_source,
                    "component_cache_built_at": official_context.component_cache_built_at,
                }
                self.storage.finish_ingestion_run(
                    run_id,
                    status="success",
                    rows_written=len(taxonomy_nodes) + official_context.cache_row_count,
                    metadata=result,
                )
                return result

            error_message = last_error or "No provider returned official mapping refresh result"
            self.storage.finish_ingestion_run(
                run_id,
                status="degraded",
                rows_written=0,
                error_message=error_message,
                metadata={"attempted_sources": attempted_sources},
            )
            return {
                "status": "degraded",
                "source": None,
                "mode": None,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "mapping_cache_rows_written": 0,
                "mapped_code_count": 0,
                "unmapped_code_count": 0,
                "total_code_count": 0,
                "component_taxonomy_count": 0,
                "mapping_source": None,
                "cache_built_at": None,
                "error_message": error_message,
            }
        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=str(e),
                metadata={"attempted_sources": attempted_sources},
            )
            return {
                "status": "failed",
                "source": None,
                "mode": None,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "mapping_cache_rows_written": 0,
                "mapped_code_count": 0,
                "unmapped_code_count": 0,
                "total_code_count": 0,
                "component_taxonomy_count": 0,
                "mapping_source": None,
                "cache_built_at": None,
                "error_message": str(e),
            }

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        instrument_ids_by_exchange: Optional[Dict[str, List[str]]] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        force_component_refresh: bool = False,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        instruments_by_exchange: Dict[str, List[Dict[str, Any]]] = {}
        optional_empty_by_exchange: Dict[str, bool] = {}
        exchange_results: List[IndustryStandardExchangeSyncResult] = []

        total_instruments = 0
        active_required_exchanges = 0
        active_optional_empty_exchanges = 0
        for exchange in target_exchanges:
            stock_instruments = await self._list_target_stock_instruments(exchange)
            requested_instrument_ids = {
                str(instrument_id).strip()
                for instrument_id in (
                    (instrument_ids_by_exchange or {}).get(exchange) or []
                )
                if str(instrument_id).strip()
            }
            if requested_instrument_ids:
                stock_instruments = [
                    instrument
                    for instrument in stock_instruments
                    if str(instrument.get("instrument_id", "")).strip()
                    in requested_instrument_ids
                ]
            if limit_per_exchange is not None:
                stock_instruments = stock_instruments[:limit_per_exchange]

            instruments_by_exchange[exchange] = stock_instruments
            optional_empty_exchange = allows_optional_empty_exchange(
                self.research_config,
                "industry",
                exchange,
            )
            optional_empty_by_exchange[exchange] = optional_empty_exchange
            total_instruments += len(stock_instruments)
            if stock_instruments:
                if optional_empty_exchange:
                    active_optional_empty_exchanges += 1
                else:
                    active_required_exchanges += 1

        if total_instruments == 0:
            return {
                "status": "skipped",
                "source": None,
                "mode": None,
                "attempted_sources": [],
                "taxonomy_nodes_written": 0,
                "total_memberships_written": 0,
                "total_official_classifications_written": 0,
                "successful_exchanges": 0,
                "attempted_exchanges": len(target_exchanges),
                "exchanges": [
                    asdict(
                        IndustryStandardExchangeSyncResult(
                            exchange=exchange,
                            status="skipped",
                            error_message="No active stock instruments found for exchange",
                        )
                    )
                    for exchange in target_exchanges
                ],
            }

        run_id = self.storage.start_ingestion_run(
            domain="industry_standard",
            job_name="industry_standard_sync",
            market=",".join(target_exchanges),
            metadata={"instrument_count": total_instruments},
        )

        attempted_sources: List[str] = []
        best_degraded_result: Optional[Dict[str, Any]] = None
        targeted_sync = bool(instrument_ids_by_exchange)
        try:
            plan = self.resolver.resolve(
                "industry_standard",
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            for candidate in plan.candidates:
                attempted_sources.append(f"{candidate.source}:{candidate.mode}")
                provider = self.registry.get(candidate.source)
                if provider is None or not provider.supports_mode(candidate.mode):
                    continue

                if self._official_classification_primary_enabled():
                    official_result = await self._try_sync_official_classification_primary(
                        provider=provider,
                        candidate_source=candidate.source,
                        candidate_mode=candidate.mode,
                        target_exchanges=target_exchanges,
                        instruments_by_exchange=instruments_by_exchange,
                        run_id=run_id,
                        attempted_sources=attempted_sources,
                        force_refresh=force_component_refresh,
                    )
                    if official_result is not None:
                        if official_result.get("status") == "success":
                            self.storage.finish_ingestion_run(
                                run_id,
                                status="success",
                                rows_written=(
                                    int(official_result.get("taxonomy_nodes_written", 0))
                                    + int(
                                        official_result.get(
                                            "classification_history_rows_written",
                                            0,
                                        )
                                    )
                                    + int(
                                        official_result.get(
                                            "total_memberships_written",
                                            0,
                                        )
                                    )
                                    + int(
                                        official_result.get(
                                            "total_official_classifications_written",
                                            0,
                                        )
                                    )
                                ),
                                metadata={
                                    "source": candidate.source,
                                    "mode": candidate.mode,
                                    "attempted_sources": attempted_sources,
                                    "official_classification_primary": True,
                                    "exchange_results": official_result.get("exchanges", []),
                                },
                            )
                            return official_result
                        best_degraded_result = official_result
                        continue

                taxonomy_source = "live_fetch"
                taxonomy_nodes: List[IndustryTaxonomySnapshot] = []
                if targeted_sync:
                    taxonomy_nodes = self._load_cached_taxonomy_nodes()
                    if taxonomy_nodes:
                        taxonomy_source = "cache"

                if not taxonomy_nodes:
                    try:
                        taxonomy_nodes = await provider.fetch_taxonomy(mode=candidate.mode)
                    except Exception as e:
                        dm_logger.warning(
                            "[IndustryStandardSync] Provider %s (%s) failed during taxonomy fetch: %s",
                            candidate.source,
                            candidate.mode,
                            e,
                        )
                        continue
                    taxonomy_source = "live_fetch"

                if not taxonomy_nodes:
                    continue

                if taxonomy_source != "cache":
                    for node in taxonomy_nodes:
                        self.storage.upsert_industry_taxonomy(node)

                official_provider = self.official_registry.get(candidate.source)
                if official_provider is not None and not official_provider.supports_mode(
                    candidate.mode
                ):
                    official_provider = None
                if targeted_sync:
                    official_provider = None

                total_memberships_written = 0
                total_official_classifications_written = 0
                exchange_results = []

                for exchange in target_exchanges:
                    stock_instruments = instruments_by_exchange[exchange]
                    if not stock_instruments:
                        exchange_results.append(
                            IndustryStandardExchangeSyncResult(
                                exchange=exchange,
                                status="skipped",
                                source=candidate.source,
                                mode=candidate.mode,
                                error_message="No active stock instruments found for exchange",
                            )
                        )
                        continue

                    exchange_result = await self._sync_exchange_memberships(
                        provider=provider,
                        official_provider=official_provider,
                        optional_empty_exchange=optional_empty_by_exchange.get(exchange, False),
                        candidate_source=candidate.source,
                        candidate_mode=candidate.mode,
                        taxonomy_nodes=taxonomy_nodes,
                        stock_instruments=stock_instruments,
                        exchange=exchange,
                        run_id=run_id,
                        force_component_refresh=force_component_refresh,
                    )
                    exchange_results.append(exchange_result)
                    total_memberships_written += exchange_result.memberships_written
                    total_official_classifications_written += (
                        exchange_result.official_classifications_written
                    )

                successful_exchanges = sum(
                    1 for result in exchange_results if result.status == "success"
                )
                required_exchange_failures = sum(
                    1
                    for result in exchange_results
                    if (
                        instruments_by_exchange.get(result.exchange)
                        and not optional_empty_by_exchange.get(result.exchange, False)
                        and result.status != "success"
                    )
                )
                candidate_success = required_exchange_failures == 0 and (
                    successful_exchanges > 0 or active_required_exchanges == 0
                )
                current_result = {
                    "status": "success" if candidate_success else "degraded",
                    "source": candidate.source,
                    "mode": candidate.mode,
                    "attempted_sources": attempted_sources,
                    "taxonomy_source": taxonomy_source,
                    "taxonomy_nodes_written": len(taxonomy_nodes),
                    "total_memberships_written": total_memberships_written,
                    "total_official_classifications_written": total_official_classifications_written,
                    "successful_exchanges": successful_exchanges,
                    "attempted_exchanges": len(exchange_results),
                    "exchanges": [asdict(result) for result in exchange_results],
                }

                if candidate_success:
                    self.storage.finish_ingestion_run(
                        run_id,
                        status="success",
                        rows_written=(
                            len(taxonomy_nodes)
                            + total_memberships_written
                            + total_official_classifications_written
                        ),
                        metadata={
                            "source": candidate.source,
                            "mode": candidate.mode,
                            "attempted_sources": attempted_sources,
                            "taxonomy_nodes_written": len(taxonomy_nodes),
                            "total_official_classifications_written": (
                                total_official_classifications_written
                            ),
                            "exchange_results": [asdict(item) for item in exchange_results],
                        },
                    )
                    return current_result

                best_degraded_result = current_result
                dm_logger.warning(
                    "[IndustryStandardSync] Provider %s (%s) produced taxonomy but no authoritative memberships; trying next candidate if available",
                    candidate.source,
                    candidate.mode,
                )

            if active_required_exchanges == 0 and active_optional_empty_exchanges > 0:
                empty_exchange_results = []
                for exchange in target_exchanges:
                    if not instruments_by_exchange.get(exchange):
                        empty_exchange_results.append(
                            asdict(
                                IndustryStandardExchangeSyncResult(
                                    exchange=exchange,
                                    status="skipped",
                                    error_message="No active stock instruments found for exchange",
                                )
                            )
                        )
                        continue

                    empty_exchange_results.append(
                        asdict(
                            IndustryStandardExchangeSyncResult(
                                exchange=exchange,
                                status="success",
                                diagnostics={"optional_empty_exchange": True},
                            )
                        )
                    )

                self.storage.finish_ingestion_run(
                    run_id,
                    status="success",
                    rows_written=0,
                    metadata={
                        "attempted_sources": attempted_sources,
                        "all_optional_empty_exchanges": True,
                        "exchange_results": empty_exchange_results,
                    },
                )
                return {
                    "status": "success",
                    "source": None,
                    "mode": None,
                    "attempted_sources": attempted_sources,
                    "taxonomy_nodes_written": 0,
                    "total_memberships_written": 0,
                    "total_official_classifications_written": 0,
                    "successful_exchanges": active_optional_empty_exchanges,
                    "attempted_exchanges": len(target_exchanges),
                    "exchanges": empty_exchange_results,
                }

            self.storage.finish_ingestion_run(
                run_id,
                status="degraded",
                rows_written=(
                    (best_degraded_result or {}).get("taxonomy_nodes_written", 0)
                    + (best_degraded_result or {}).get(
                        "total_official_classifications_written",
                        0,
                    )
                ),
                error_message="No provider returned authoritative Shenwan taxonomy",
                metadata={
                    "attempted_sources": attempted_sources,
                    "best_degraded_result": best_degraded_result,
                },
            )
            if best_degraded_result is not None:
                return {
                    **best_degraded_result,
                    "status": "degraded",
                }

            return {
                "status": "degraded",
                "source": None,
                "mode": None,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "total_memberships_written": 0,
                "total_official_classifications_written": 0,
                "successful_exchanges": 0,
                "attempted_exchanges": len(target_exchanges),
                "exchanges": [
                    asdict(
                        IndustryStandardExchangeSyncResult(
                            exchange=exchange,
                            status="degraded",
                            error_message="No provider returned authoritative Shenwan taxonomy",
                        )
                    )
                    for exchange in target_exchanges
                ],
            }
        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=str(e),
                metadata={"attempted_sources": attempted_sources},
            )
            return {
                "status": "failed",
                "source": None,
                "mode": None,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "total_memberships_written": 0,
                "total_official_classifications_written": 0,
                "successful_exchanges": 0,
                "attempted_exchanges": len(target_exchanges),
                "exchanges": [
                    asdict(
                        IndustryStandardExchangeSyncResult(
                            exchange=exchange,
                            status="failed",
                            error_message=str(e),
                        )
                    )
                    for exchange in target_exchanges
                ],
            }

    def _official_classification_primary_enabled(self) -> bool:
        standard_cfg = self.research_config.modules.get("industry", {}).get("standard", {})
        return bool(standard_cfg.get("classification_primary_enabled", False))

    async def _try_sync_official_classification_primary(
        self,
        *,
        provider: BaseIndustryStandardProvider,
        candidate_source: str,
        candidate_mode: str,
        target_exchanges: List[str],
        instruments_by_exchange: Dict[str, List[Dict[str, Any]]],
        run_id: int,
        attempted_sources: List[str],
        force_refresh: bool,
    ) -> Optional[Dict[str, Any]]:
        bundle_fetcher = getattr(provider, "fetch_official_classification_bundle", None)
        if callable(bundle_fetcher):
            return await self._sync_swsresearch_official_bundle(
                provider=provider,
                bundle_fetcher=bundle_fetcher,
                candidate_source=candidate_source,
                candidate_mode=candidate_mode,
                target_exchanges=target_exchanges,
                instruments_by_exchange=instruments_by_exchange,
                run_id=run_id,
                attempted_sources=attempted_sources,
                force_refresh=force_refresh,
            )

        if candidate_source != "akshare":
            return None

        official_provider = self.official_registry.get(candidate_source)
        if official_provider is None or not official_provider.supports_mode(candidate_mode):
            return None

        cached_taxonomy_nodes = self._load_cached_taxonomy_nodes()
        if not cached_taxonomy_nodes or not self._looks_like_official_classification_taxonomy(
            cached_taxonomy_nodes
        ):
            return {
                "status": "degraded",
                "source": candidate_source,
                "mode": candidate_mode,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "classification_history_rows_written": 0,
                "total_memberships_written": 0,
                "total_official_classifications_written": 0,
                "successful_exchanges": 0,
                "attempted_exchanges": len(target_exchanges),
                "exchanges": [
                    asdict(
                        IndustryStandardExchangeSyncResult(
                            exchange=exchange,
                            status="degraded",
                            source=candidate_source,
                            mode=candidate_mode,
                            diagnostics={"official_classification_primary": True},
                            error_message=(
                                "Cached official Shenwan classification taxonomy is "
                                "required for AkShare stock-history fallback"
                            ),
                        )
                    )
                    for exchange in target_exchanges
                ],
            }

        return await self._sync_official_history_provider_with_cached_taxonomy(
            official_provider=official_provider,
            taxonomy_nodes=cached_taxonomy_nodes,
            candidate_source=candidate_source,
            candidate_mode=candidate_mode,
            target_exchanges=target_exchanges,
            instruments_by_exchange=instruments_by_exchange,
            run_id=run_id,
            attempted_sources=attempted_sources,
        )

    async def _sync_swsresearch_official_bundle(
        self,
        *,
        provider: BaseIndustryStandardProvider,
        bundle_fetcher: Any,
        candidate_source: str,
        candidate_mode: str,
        target_exchanges: List[str],
        instruments_by_exchange: Dict[str, List[Dict[str, Any]]],
        run_id: int,
        attempted_sources: List[str],
        force_refresh: bool,
    ) -> Dict[str, Any]:
        previous_source_files = self.storage.get_latest_industry_source_files(
            source=getattr(provider, "source_name", candidate_source),
            source_mode=candidate_mode,
            artifact_kinds=[
                getattr(provider, "STOCK_HISTORY_ARTIFACT", "shenwan_stock_classification_history"),
                getattr(provider, "CODE_TABLE_ARTIFACT", "shenwan_classification_code_table"),
            ],
        )
        try:
            bundle = await bundle_fetcher(
                mode=candidate_mode,
                previous_source_files=previous_source_files,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            dm_logger.warning(
                "[IndustryStandardSync] Official SWS classification source failed: %s",
                exc,
            )
            return {
                "status": "degraded",
                "source": candidate_source,
                "mode": candidate_mode,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "classification_history_rows_written": 0,
                "total_memberships_written": 0,
                "total_official_classifications_written": 0,
                "successful_exchanges": 0,
                "attempted_exchanges": len(target_exchanges),
                "exchanges": [
                    asdict(
                        IndustryStandardExchangeSyncResult(
                            exchange=exchange,
                            status="degraded",
                            source=candidate_source,
                            mode=candidate_mode,
                            diagnostics={"official_classification_primary": True},
                            error_message=str(exc),
                        )
                    )
                    for exchange in target_exchanges
                ],
            }

        if not bundle.changed:
            return self._build_unchanged_official_classification_result(
                candidate_source=candidate_source,
                candidate_mode=candidate_mode,
                attempted_sources=attempted_sources,
                target_exchanges=target_exchanges,
                instruments_by_exchange=instruments_by_exchange,
                diagnostics=bundle.diagnostics,
            )

        taxonomy_nodes = list(bundle.taxonomy_nodes)
        taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
        taxonomy_by_code = {node.industry_code: node for node in taxonomy_nodes}
        source_file_ids: Dict[str, int] = {}
        for source_file in bundle.source_files:
            source_file_ids[source_file.artifact_kind] = self.storage.upsert_industry_source_file(
                source_file,
                ingestion_run_id=run_id,
            )

        cleared_counts = self.storage.clear_industry_standard_slice(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
        )
        for node in taxonomy_nodes:
            self.storage.upsert_industry_taxonomy(node)

        stock_history_artifact = getattr(
            provider,
            "STOCK_HISTORY_ARTIFACT",
            "shenwan_stock_classification_history",
        )
        source_file_id = source_file_ids.get(stock_history_artifact)
        history_rows = [
            row if source_file_id is None else replace(row, source_file_id=source_file_id)
            for row in bundle.history_rows
        ]
        self.storage.replace_industry_classification_history(
            history_rows,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            ingestion_run_id=run_id,
        )

        latest_by_symbol = {
            str(snapshot.symbol).strip(): snapshot for snapshot in bundle.latest_classifications
        }
        exchange_results: List[IndustryStandardExchangeSyncResult] = []
        total_memberships_written = 0
        total_official_classifications_written = 0
        for exchange in target_exchanges:
            stock_instruments = instruments_by_exchange.get(exchange, [])
            if not stock_instruments:
                exchange_results.append(
                    IndustryStandardExchangeSyncResult(
                        exchange=exchange,
                        status="skipped",
                        source=candidate_source,
                        mode=candidate_mode,
                        error_message="No active stock instruments found for exchange",
                    )
                )
                continue

            exchange_result = self._write_official_memberships_for_exchange(
                taxonomy_nodes=taxonomy_nodes,
                taxonomy_by_code=taxonomy_by_code,
                latest_by_symbol=latest_by_symbol,
                stock_instruments=stock_instruments,
                exchange=exchange,
                source=candidate_source,
                mode=candidate_mode,
                run_id=run_id,
                diagnostics={
                    "official_classification_primary": True,
                    "cleared_counts": cleared_counts,
                    "source_file_ids": source_file_ids,
                    "bundle": bundle.diagnostics,
                },
            )
            exchange_results.append(exchange_result)
            total_memberships_written += exchange_result.memberships_written
            total_official_classifications_written += (
                exchange_result.official_classifications_written
            )

        successful_exchanges = sum(
            1 for result in exchange_results if result.status == "success"
        )
        required_failures = sum(
            1
            for result in exchange_results
            if instruments_by_exchange.get(result.exchange) and result.status != "success"
        )
        return {
            "status": "success" if required_failures == 0 and successful_exchanges > 0 else "degraded",
            "source": candidate_source,
            "mode": candidate_mode,
            "attempted_sources": attempted_sources,
            "official_classification_primary": True,
            "taxonomy_nodes_written": len(taxonomy_nodes),
            "classification_history_rows_written": len(history_rows),
            "source_files_written": len(bundle.source_files),
            "total_memberships_written": total_memberships_written,
            "total_official_classifications_written": total_official_classifications_written,
            "successful_exchanges": successful_exchanges,
            "attempted_exchanges": len(exchange_results),
            "exchanges": [asdict(result) for result in exchange_results],
        }

    async def _sync_official_history_provider_with_cached_taxonomy(
        self,
        *,
        official_provider: BaseOfficialIndustryHistoryProvider,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        candidate_source: str,
        candidate_mode: str,
        target_exchanges: List[str],
        instruments_by_exchange: Dict[str, List[Dict[str, Any]]],
        run_id: int,
        attempted_sources: List[str],
    ) -> Dict[str, Any]:
        taxonomy_by_code = {node.industry_code: node for node in taxonomy_nodes}
        latest_by_symbol: Dict[str, OfficialIndustryHistorySnapshot] = {}
        try:
            latest_classifications = await official_provider.fetch_all_latest_classifications(
                mode=candidate_mode,
            )
        except Exception as exc:
            return {
                "status": "degraded",
                "source": candidate_source,
                "mode": candidate_mode,
                "attempted_sources": attempted_sources,
                "taxonomy_nodes_written": 0,
                "classification_history_rows_written": 0,
                "total_memberships_written": 0,
                "total_official_classifications_written": 0,
                "successful_exchanges": 0,
                "attempted_exchanges": len(target_exchanges),
                "exchanges": [
                    asdict(
                        IndustryStandardExchangeSyncResult(
                            exchange=exchange,
                            status="degraded",
                            source=candidate_source,
                            mode=candidate_mode,
                            diagnostics={"official_classification_primary": True},
                            error_message=str(exc),
                        )
                    )
                    for exchange in target_exchanges
                ],
            }

        latest_by_symbol = {
            str(snapshot.symbol).strip(): snapshot for snapshot in latest_classifications
        }
        exchange_results: List[IndustryStandardExchangeSyncResult] = []
        total_memberships_written = 0
        total_official_classifications_written = 0
        for exchange in target_exchanges:
            exchange_result = self._write_official_memberships_for_exchange(
                taxonomy_nodes=taxonomy_nodes,
                taxonomy_by_code=taxonomy_by_code,
                latest_by_symbol=latest_by_symbol,
                stock_instruments=instruments_by_exchange.get(exchange, []),
                exchange=exchange,
                source=candidate_source,
                mode=candidate_mode,
                run_id=run_id,
                diagnostics={
                    "official_classification_primary": True,
                    "fallback": "akshare_stock_industry_clf_hist_sw",
                    "classification_history_source": "akshare_latest_only",
                },
            )
            exchange_results.append(exchange_result)
            total_memberships_written += exchange_result.memberships_written
            total_official_classifications_written += (
                exchange_result.official_classifications_written
            )

        successful_exchanges = sum(
            1 for result in exchange_results if result.status == "success"
        )
        required_failures = sum(
            1
            for result in exchange_results
            if instruments_by_exchange.get(result.exchange) and result.status != "success"
        )
        return {
            "status": "success" if required_failures == 0 and successful_exchanges > 0 else "degraded",
            "source": candidate_source,
            "mode": candidate_mode,
            "attempted_sources": attempted_sources,
            "official_classification_primary": True,
            "taxonomy_nodes_written": 0,
            "classification_history_rows_written": 0,
            "total_memberships_written": total_memberships_written,
            "total_official_classifications_written": total_official_classifications_written,
            "successful_exchanges": successful_exchanges,
            "attempted_exchanges": len(exchange_results),
            "exchanges": [asdict(result) for result in exchange_results],
        }

    def _write_official_memberships_for_exchange(
        self,
        *,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
        latest_by_symbol: Dict[str, OfficialIndustryHistorySnapshot],
        stock_instruments: List[Dict[str, Any]],
        exchange: str,
        source: str,
        mode: str,
        run_id: int,
        diagnostics: Dict[str, Any],
    ) -> IndustryStandardExchangeSyncResult:
        if not stock_instruments:
            return IndustryStandardExchangeSyncResult(
                exchange=exchange,
                status="skipped",
                source=source,
                mode=mode,
                error_message="No active stock instruments found for exchange",
            )

        memberships_written = 0
        official_classifications_written = 0
        unresolved_instrument_ids: List[str] = []
        for instrument in stock_instruments:
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                symbol = str(instrument.get("instrument_id") or "").split(".", 1)[0]
            official_snapshot = latest_by_symbol.get(symbol)
            if official_snapshot is None:
                unresolved_instrument_ids.append(str(instrument.get("instrument_id") or ""))
                continue
            classification_snapshot, membership_snapshot = (
                self._build_direct_official_classification_snapshots(
                    instrument=instrument,
                    official_snapshot=official_snapshot,
                    taxonomy_nodes=taxonomy_nodes,
                    taxonomy_by_code=taxonomy_by_code,
                    source=source,
                    mode=mode,
                )
            )
            self.storage.upsert_official_industry_classification(
                classification_snapshot,
                ingestion_run_id=run_id,
            )
            official_classifications_written += 1
            if membership_snapshot is None:
                unresolved_instrument_ids.append(str(instrument.get("instrument_id") or ""))
                continue
            self.storage.upsert_industry_membership(
                membership_snapshot,
                ingestion_run_id=run_id,
            )
            self.storage.store_raw_payload(
                domain="industry_standard",
                instrument_id=membership_snapshot.instrument_id,
                source=membership_snapshot.source,
                source_mode=membership_snapshot.source_mode,
                payload=membership_snapshot.raw_payload,
                payload_hash=self._hash_payload(membership_snapshot.raw_payload),
                ingestion_run_id=run_id,
            )
            memberships_written += 1

        stale_memberships_removed = 0
        if unresolved_instrument_ids:
            taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
            stale_memberships_removed = self.storage.delete_industry_memberships(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
                instrument_ids=unresolved_instrument_ids,
            )

        result_diagnostics = {
            **diagnostics,
            "target_instruments": len(stock_instruments),
            "official_classifications_written": official_classifications_written,
            "memberships_written": memberships_written,
            "unresolved_target_instruments": len(unresolved_instrument_ids),
            "stale_current_memberships_removed": stale_memberships_removed,
        }
        if unresolved_instrument_ids:
            result_diagnostics["unresolved_instrument_ids"] = unresolved_instrument_ids[:20]

        if memberships_written >= len(stock_instruments):
            status = "success"
            error_message = None
        else:
            status = "degraded"
            error_message = "Official Shenwan classification coverage incomplete"

        return IndustryStandardExchangeSyncResult(
            exchange=exchange,
            status=status,
            memberships_written=memberships_written,
            official_classifications_written=official_classifications_written,
            source=source,
            mode=mode,
            diagnostics=result_diagnostics,
            error_message=error_message,
        )

    def _build_unchanged_official_classification_result(
        self,
        *,
        candidate_source: str,
        candidate_mode: str,
        attempted_sources: List[str],
        target_exchanges: List[str],
        instruments_by_exchange: Dict[str, List[Dict[str, Any]]],
        diagnostics: Dict[str, Any],
    ) -> Dict[str, Any]:
        standard_cfg = self.research_config.modules.get("industry", {}).get("standard", {})
        taxonomy_system = str(standard_cfg.get("taxonomy_system", "sw"))
        taxonomy_version = str(standard_cfg.get("taxonomy_version", "sw_2021"))
        authoritative_by_exchange = self.storage.count_industry_memberships_by_exchange(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            mapping_status="authoritative",
        )
        exchange_results: List[IndustryStandardExchangeSyncResult] = []
        for exchange in target_exchanges:
            target_count = len(instruments_by_exchange.get(exchange, []))
            if target_count <= 0:
                exchange_results.append(
                    IndustryStandardExchangeSyncResult(
                        exchange=exchange,
                        status="skipped",
                        source=candidate_source,
                        mode=candidate_mode,
                        error_message="No active stock instruments found for exchange",
                    )
                )
                continue
            authoritative_count = int(authoritative_by_exchange.get(exchange, 0))
            status = "success" if authoritative_count >= target_count else "degraded"
            exchange_results.append(
                IndustryStandardExchangeSyncResult(
                    exchange=exchange,
                    status=status,
                    memberships_written=0,
                    official_classifications_written=0,
                    source=candidate_source,
                    mode=candidate_mode,
                    diagnostics={
                        "official_classification_primary": True,
                        "source_files_unchanged": True,
                        "existing_authoritative_memberships": authoritative_count,
                        "target_instruments": target_count,
                        **diagnostics,
                    },
                    error_message=None
                    if status == "success"
                    else "Source files unchanged but existing membership coverage is incomplete",
                )
            )
        successful_exchanges = sum(
            1 for result in exchange_results if result.status == "success"
        )
        required_failures = sum(
            1
            for result in exchange_results
            if instruments_by_exchange.get(result.exchange) and result.status != "success"
        )
        return {
            "status": "success" if required_failures == 0 and successful_exchanges > 0 else "degraded",
            "source": candidate_source,
            "mode": candidate_mode,
            "attempted_sources": attempted_sources,
            "official_classification_primary": True,
            "source_files_unchanged": True,
            "taxonomy_nodes_written": 0,
            "classification_history_rows_written": 0,
            "total_memberships_written": 0,
            "total_official_classifications_written": 0,
            "successful_exchanges": successful_exchanges,
            "attempted_exchanges": len(exchange_results),
            "exchanges": [asdict(result) for result in exchange_results],
        }

    def _build_direct_official_classification_snapshots(
        self,
        *,
        instrument: Dict[str, Any],
        official_snapshot: OfficialIndustryHistorySnapshot,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
        source: str,
        mode: str,
    ) -> tuple[OfficialIndustryClassificationSnapshot, Optional[IndustrySnapshot]]:
        taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
        mapped_node = taxonomy_by_code.get(str(official_snapshot.official_industry_code))
        l1_node, l2_node, l3_node = self._resolve_taxonomy_levels(
            node=mapped_node,
            taxonomy_by_code=taxonomy_by_code,
        )
        levels_payload = {
            "sw_l1": self._node_payload(l1_node),
            "sw_l2": self._node_payload(l2_node),
            "sw_l3": self._node_payload(l3_node),
        }
        classification_json = {
            "official": {
                "symbol": official_snapshot.symbol,
                "official_industry_code": official_snapshot.official_industry_code,
                "start_date": official_snapshot.start_date,
                "update_time": official_snapshot.update_time,
                "raw_payload": official_snapshot.raw_payload,
            },
            "identifier_namespace": "swsresearch_official_classification_code",
            "levels": levels_payload,
        }
        classification_snapshot = OfficialIndustryClassificationSnapshot(
            instrument_id=str(instrument.get("instrument_id") or official_snapshot.instrument_id),
            symbol=str(instrument.get("symbol") or official_snapshot.symbol),
            exchange=str(instrument.get("exchange") or official_snapshot.exchange),
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            official_industry_code=official_snapshot.official_industry_code,
            official_start_date=official_snapshot.start_date,
            official_update_time=official_snapshot.update_time,
            mapped_industry_code=None if mapped_node is None else mapped_node.industry_code,
            mapped_industry_name=None if mapped_node is None else mapped_node.industry_name,
            mapped_industry_level=None if mapped_node is None else mapped_node.industry_level,
            mapped_parent_code=None if mapped_node is None else mapped_node.parent_code,
            mapping_status="mapped" if mapped_node is not None else "unmapped",
            mapping_confidence="official_direct" if mapped_node is not None else None,
            source=source,
            source_mode=mode,
            classification_json=classification_json,
        )
        if mapped_node is None:
            return classification_snapshot, None
        membership_snapshot = IndustrySnapshot(
            instrument_id=classification_snapshot.instrument_id,
            symbol=classification_snapshot.symbol,
            exchange=classification_snapshot.exchange,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_code=mapped_node.industry_code,
            industry_name=mapped_node.industry_name,
            industry_level=mapped_node.industry_level,
            parent_code=mapped_node.parent_code,
            mapping_status="authoritative",
            effective_date=official_snapshot.start_date,
            source_classification="申万官方股票行业分类文件",
            source_industry_name=mapped_node.industry_name,
            sw_l1_code=None if l1_node is None else l1_node.industry_code,
            sw_l1_name=None if l1_node is None else l1_node.industry_name,
            sw_l2_code=None if l2_node is None else l2_node.industry_code,
            sw_l2_name=None if l2_node is None else l2_node.industry_name,
            sw_l3_code=None if l3_node is None else l3_node.industry_code,
            sw_l3_name=None if l3_node is None else l3_node.industry_name,
            source=source,
            source_mode=mode,
            membership_json=classification_json,
            raw_payload=classification_json,
        )
        return classification_snapshot, membership_snapshot

    @staticmethod
    def _looks_like_official_classification_taxonomy(
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
    ) -> bool:
        if not taxonomy_nodes:
            return False
        official_code_count = sum(
            1
            for node in taxonomy_nodes
            if re.fullmatch(r"\d{6}", str(node.industry_code or ""))
        )
        return official_code_count >= max(1, int(len(taxonomy_nodes) * 0.9))

    async def _build_official_mapping_context(
        self,
        *,
        provider: BaseIndustryStandardProvider,
        official_provider: BaseOfficialIndustryHistoryProvider,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        mode: str,
        force_live_rebuild: bool = False,
    ) -> _OfficialMappingContext:
        if force_live_rebuild:
            return await self._rebuild_official_mapping_context(
                provider=provider,
                official_provider=official_provider,
                taxonomy_nodes=taxonomy_nodes,
                mode=mode,
            )

        taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
        cached_context = self._load_cached_official_mapping_context(
            taxonomy_nodes=taxonomy_nodes,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
        )
        if cached_context is not None:
            return cached_context

        if not self._allow_live_rebuild_on_cache_miss():
            raise ValueError(
                "Official mapping cache unavailable or stale and live rebuild is disabled"
            )

        return await self._rebuild_official_mapping_context(
            provider=provider,
            official_provider=official_provider,
            taxonomy_nodes=taxonomy_nodes,
            mode=mode,
        )

    async def _rebuild_official_mapping_context(
        self,
        *,
        provider: BaseIndustryStandardProvider,
        official_provider: BaseOfficialIndustryHistoryProvider,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        mode: str,
    ) -> _OfficialMappingContext:
        taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
        official_snapshots = await official_provider.fetch_all_latest_classifications(mode=mode)
        if not official_snapshots:
            raise ValueError("Official source returned no latest classifications")

        component_sets, component_cache_source, component_cache_built_at = (
            self._load_cached_component_sets(
                taxonomy_nodes=taxonomy_nodes,
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
            )
        )
        if component_sets is None:
            component_sets = await provider.fetch_component_sets(
                taxonomy_nodes=taxonomy_nodes,
                mode=mode,
            )
            if not component_sets:
                raise ValueError("Industry standard provider returned no component sets")

            self.storage.replace_industry_component_sets(
                component_sets,
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
                source=provider.source_name,
                source_mode=mode,
            )
            component_cache_info = self.storage.get_latest_industry_component_set_cache_info(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
            )
            component_cache_source = "live_fetch"
            component_cache_built_at = None if component_cache_info is None else str(
                component_cache_info.get("built_at") or component_cache_info.get("updated_at") or ""
            ) or None

        mappings = self.code_mapper.infer_mappings(
            official_snapshots=official_snapshots,
            taxonomy_components=component_sets,
        )
        mappings = self._apply_official_mapping_overrides(
            mappings=mappings,
            taxonomy_by_code={node.industry_code: node for node in taxonomy_nodes},
        )
        self.storage.replace_official_industry_code_mappings(
            mappings,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            source=provider.source_name,
            source_mode=mode,
        )
        mapped_code_count = sum(1 for item in mappings if item.taxonomy_industry_code is not None)
        return _OfficialMappingContext(
            mapping_by_code={item.official_industry_code: item for item in mappings},
            taxonomy_by_code={node.industry_code: node for node in taxonomy_nodes},
            mapped_code_count=mapped_code_count,
            unmapped_code_count=len(mappings) - mapped_code_count,
            total_code_count=len(mappings),
            component_taxonomy_count=len(component_sets),
            mapping_source="live_rebuild",
            cache_row_count=len(mappings),
            component_cache_source=component_cache_source,
            component_cache_built_at=component_cache_built_at,
        )

    async def _sync_exchange_memberships(
        self,
        *,
        provider: BaseIndustryStandardProvider,
        official_provider: Optional[BaseOfficialIndustryHistoryProvider],
        optional_empty_exchange: bool,
        candidate_source: str,
        candidate_mode: str,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        stock_instruments: List[Dict[str, Any]],
        exchange: str,
        run_id: int,
        force_component_refresh: bool = False,
    ) -> IndustryStandardExchangeSyncResult:
        diagnostics: Dict[str, Any] = {}
        normalized_membership_ids: set[str] = set()
        taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
        taxonomy_by_code = {node.industry_code: node for node in taxonomy_nodes}
        component_memberships_written = 0
        component_missing_ids: set[str] = {
            str(instrument.get("instrument_id", ""))
            for instrument in stock_instruments
            if str(instrument.get("instrument_id", ""))
        }
        component_cache_source: Optional[str] = None
        component_cache_built_at: Optional[str] = None
        component_sets, component_cache_source, component_cache_built_at = await (
            self._load_or_fetch_component_sets(
                provider=provider,
                taxonomy_nodes=taxonomy_nodes,
                mode=candidate_mode,
                force_refresh=force_component_refresh,
            )
        )
        component_snapshots = self._build_current_component_memberships(
            instruments=stock_instruments,
            component_sets=component_sets,
            taxonomy_by_code=taxonomy_by_code,
            source=candidate_source,
            mode=candidate_mode,
        )
        for snapshot in component_snapshots:
            self.storage.upsert_industry_membership(
                snapshot,
                ingestion_run_id=run_id,
            )
            self.storage.store_raw_payload(
                domain="industry_standard",
                instrument_id=snapshot.instrument_id,
                source=snapshot.source,
                source_mode=snapshot.source_mode,
                payload=snapshot.raw_payload,
                payload_hash=self._hash_payload(snapshot.raw_payload),
                ingestion_run_id=run_id,
            )
            normalized_membership_ids.add(snapshot.instrument_id)
            component_missing_ids.discard(snapshot.instrument_id)
            component_memberships_written += 1

        diagnostics.update(
            {
                "component_current_target_instruments": len(stock_instruments),
                "component_current_memberships_written": component_memberships_written,
                "component_current_missing_instruments": len(component_missing_ids),
            }
        )
        if component_cache_source:
            diagnostics["component_current_cache_source"] = component_cache_source
        if component_cache_built_at:
            diagnostics["component_current_cache_built_at"] = component_cache_built_at
        if force_component_refresh:
            diagnostics["component_current_force_refresh"] = True

        official_classifications_written = 0
        official_target_records = 0
        official_unmapped_records = 0

        if official_provider is not None and official_provider.supports_mode(candidate_mode):
            try:
                official_snapshots = await official_provider.fetch_latest_classifications(
                    instruments=stock_instruments,
                    exchange=exchange,
                    mode=candidate_mode,
                )
            except Exception as e:
                diagnostics["official_fetch_error"] = str(e)
                official_snapshots = []

            official_target_records = len(official_snapshots)
            instrument_by_symbol = {
                str(instrument.get("symbol", "")).strip(): instrument
                for instrument in stock_instruments
                if str(instrument.get("symbol", "")).strip()
            }

            for official_snapshot in official_snapshots:
                instrument = instrument_by_symbol.get(str(official_snapshot.symbol).strip())
                if instrument is None:
                    continue

                classification_snapshot, membership_snapshot = self._build_official_snapshots(
                    instrument=instrument,
                    official_snapshot=official_snapshot,
                    taxonomy_nodes=taxonomy_nodes,
                    mapping=None,
                    taxonomy_by_code=taxonomy_by_code,
                )

                self.storage.upsert_official_industry_classification(
                    classification_snapshot,
                    ingestion_run_id=run_id,
                )
                self.storage.store_raw_payload(
                    domain="industry_standard_official",
                    instrument_id=classification_snapshot.instrument_id,
                    source=classification_snapshot.source,
                    source_mode=classification_snapshot.source_mode,
                    payload=classification_snapshot.classification_json,
                    payload_hash=self._hash_payload(classification_snapshot.classification_json),
                    ingestion_run_id=run_id,
                )
                official_classifications_written += 1

                if membership_snapshot is None:
                    official_unmapped_records += 1

        diagnostics.update(
            {
                "official_target_records": official_target_records,
                "official_classifications_written": official_classifications_written,
                "official_mapped_memberships": 0,
                "official_membership_source": "audit_only",
                "official_mapping_source": "not_applied_to_current_sync",
                "official_unmapped_records": official_unmapped_records,
            }
        )

        supplement_instruments = [
            instrument
            for instrument in stock_instruments
            if instrument.get("instrument_id") not in normalized_membership_ids
        ]
        supplement_snapshots, supplement_diagnostics = await (
            self._fetch_name_supplement_memberships(
                instruments=supplement_instruments,
                exchange=exchange,
                taxonomy_by_code=taxonomy_by_code,
            )
        )
        for snapshot in supplement_snapshots:
            self.storage.upsert_industry_membership(
                snapshot,
                ingestion_run_id=run_id,
            )
            self.storage.store_raw_payload(
                domain="industry_standard",
                instrument_id=snapshot.instrument_id,
                source=snapshot.source,
                source_mode=snapshot.source_mode,
                payload=snapshot.raw_payload,
                payload_hash=self._hash_payload(snapshot.raw_payload),
                ingestion_run_id=run_id,
            )
            normalized_membership_ids.add(snapshot.instrument_id)

        if supplement_diagnostics:
            diagnostics.update(supplement_diagnostics)

        fallback_instruments = [
            instrument
            for instrument in stock_instruments
            if instrument.get("instrument_id") not in normalized_membership_ids
        ]

        fallback_snapshots: List[IndustrySnapshot] = []
        fallback_diagnostics: Dict[str, Any] = {}
        fallback_error_message: Optional[str] = None
        if fallback_instruments:
            try:
                fallback_snapshots = await provider.fetch_industries(
                    instruments=fallback_instruments,
                    exchange=exchange,
                    mode=candidate_mode,
                )
            except Exception as e:
                fallback_error_message = str(e)
                fallback_diagnostics = provider.get_last_fetch_metadata()
            else:
                fallback_diagnostics = provider.get_last_fetch_metadata()
                for snapshot in fallback_snapshots:
                    self.storage.upsert_industry_membership(
                        snapshot,
                        ingestion_run_id=run_id,
                    )
                    self.storage.store_raw_payload(
                        domain="industry_standard",
                        instrument_id=snapshot.instrument_id,
                        source=snapshot.source,
                        source_mode=snapshot.source_mode,
                        payload=snapshot.raw_payload,
                        payload_hash=self._hash_payload(snapshot.raw_payload),
                        ingestion_run_id=run_id,
                    )
                    normalized_membership_ids.add(snapshot.instrument_id)

        diagnostics.update(
            {
                "fallback_attempted": bool(fallback_instruments),
                "fallback_target_instruments": len(fallback_instruments),
                "fallback_memberships_written": len(fallback_snapshots),
            }
        )
        if optional_empty_exchange:
            diagnostics["optional_empty_exchange"] = True
        if fallback_diagnostics:
            diagnostics["fallback_diagnostics"] = fallback_diagnostics
        if fallback_error_message:
            diagnostics["fallback_error"] = fallback_error_message

        unresolved_instrument_ids = sorted(
            {
                str(instrument.get("instrument_id", "")).strip()
                for instrument in stock_instruments
                if str(instrument.get("instrument_id", "")).strip()
                and str(instrument.get("instrument_id", "")).strip()
                not in normalized_membership_ids
            }
        )
        stale_memberships_removed = 0
        if unresolved_instrument_ids:
            stale_memberships_removed = self.storage.delete_industry_memberships(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
                instrument_ids=unresolved_instrument_ids,
            )
        diagnostics.update(
            {
                "unresolved_target_instruments": len(unresolved_instrument_ids),
                "stale_current_memberships_removed": stale_memberships_removed,
            }
        )
        if unresolved_instrument_ids:
            diagnostics["unresolved_instrument_ids"] = unresolved_instrument_ids[:20]

        memberships_written = (
            component_memberships_written
            + len(supplement_snapshots)
            + len(fallback_snapshots)
        )
        if memberships_written:
            return IndustryStandardExchangeSyncResult(
                exchange=exchange,
                status="success",
                memberships_written=memberships_written,
                official_classifications_written=official_classifications_written,
                source=candidate_source,
                mode=candidate_mode,
                diagnostics=diagnostics,
            )

        if optional_empty_exchange:
            return IndustryStandardExchangeSyncResult(
                exchange=exchange,
                status="success",
                official_classifications_written=official_classifications_written,
                source=candidate_source,
                mode=candidate_mode,
                diagnostics=diagnostics,
            )

        return IndustryStandardExchangeSyncResult(
            exchange=exchange,
            status="degraded",
            official_classifications_written=official_classifications_written,
            source=candidate_source,
            mode=candidate_mode,
            diagnostics=diagnostics,
            error_message=self._build_no_membership_message(diagnostics),
        )

    async def _fetch_name_supplement_memberships(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
    ) -> tuple[List[IndustrySnapshot], Dict[str, Any]]:
        supplement_cfg = self._name_supplement_config()
        diagnostics: Dict[str, Any] = {
            "name_supplement_enabled": bool(supplement_cfg.get("enabled", False)),
            "name_supplement_attempted": False,
            "name_supplement_target_instruments": len(instruments),
            "name_supplement_hints_returned": 0,
            "name_supplement_memberships_written": 0,
        }
        if not instruments or not bool(supplement_cfg.get("enabled", False)):
            return [], diagnostics

        candidates = supplement_cfg.get("candidates") or supplement_cfg.get("sources") or []
        if not isinstance(candidates, list) or not candidates:
            diagnostics["name_supplement_error"] = "no_supplement_candidates_configured"
            return [], diagnostics

        max_instruments = int(supplement_cfg.get("max_instruments_per_exchange", 200))
        target_instruments = instruments[: max(max_instruments, 0)]
        if len(target_instruments) < len(instruments):
            diagnostics["name_supplement_truncated"] = True
            diagnostics["name_supplement_truncated_from"] = len(instruments)
            diagnostics["name_supplement_truncated_to"] = len(target_instruments)
        if not target_instruments:
            return [], diagnostics

        remaining_by_id = {
            str(instrument.get("instrument_id") or ""): instrument
            for instrument in target_instruments
            if str(instrument.get("instrument_id") or "")
        }
        snapshots_by_id: Dict[str, IndustrySnapshot] = {}
        attempted_sources: List[str] = []
        source_results: List[Dict[str, Any]] = []
        unmatched_name_samples: List[Dict[str, str]] = []
        matched_level_counts: Dict[str, int] = {}
        unmatched_sample_limit = max(
            1,
            int(supplement_cfg.get("unmatched_sample_limit", 10)),
        )
        allow_level2_leaf_matches = bool(
            supplement_cfg.get("allow_level2_leaf_matches", True)
        )

        for raw_candidate in candidates:
            if not isinstance(raw_candidate, dict):
                continue
            source_name = str(raw_candidate.get("source") or "").strip()
            mode = str(raw_candidate.get("mode") or "direct").strip()
            if not source_name:
                continue
            attempted_sources.append(f"{source_name}:{mode}")
            provider = self.supplement_registry.get(source_name)
            if provider is None:
                source_results.append(
                    {
                        "source": source_name,
                        "mode": mode,
                        "status": "skipped",
                        "reason": "provider_not_registered",
                    }
                )
                continue
            if not provider.supports_mode(mode):
                source_results.append(
                    {
                        "source": source_name,
                        "mode": mode,
                        "status": "skipped",
                        "reason": "mode_not_supported",
                    }
                )
                continue

            diagnostics["name_supplement_attempted"] = True
            try:
                hints = await provider.fetch_industry_name_hints(
                    instruments=list(remaining_by_id.values()),
                    exchange=exchange,
                    mode=mode,
                )
            except Exception as exc:
                source_results.append(
                    {
                        "source": source_name,
                        "mode": mode,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                continue

            diagnostics["name_supplement_hints_returned"] += len(hints)
            source_written = 0
            for hint in hints:
                instrument_id = str(hint.instrument_id or "")
                instrument = remaining_by_id.get(instrument_id)
                if instrument is None:
                    continue
                snapshot, match_info = self._build_name_supplement_membership(
                    hint=hint,
                    instrument=instrument,
                    taxonomy_by_code=taxonomy_by_code,
                    allow_level2_leaf_matches=allow_level2_leaf_matches,
                )
                if snapshot is None:
                    if len(unmatched_name_samples) < unmatched_sample_limit:
                        unmatched_name_samples.append(
                            {
                                "instrument_id": instrument_id,
                                "industry_name": hint.industry_name,
                                "reason": match_info.get("reason", "unmatched"),
                            }
                        )
                    continue

                snapshots_by_id[instrument_id] = snapshot
                remaining_by_id.pop(instrument_id, None)
                source_written += 1
                level_key = f"level_{snapshot.industry_level}"
                matched_level_counts[level_key] = matched_level_counts.get(level_key, 0) + 1

            source_results.append(
                {
                    "source": source_name,
                    "mode": mode,
                    "status": "success",
                    "hints_returned": len(hints),
                    "memberships_written": source_written,
                }
            )
            if not remaining_by_id:
                break

        snapshots = list(snapshots_by_id.values())
        diagnostics.update(
            {
                "name_supplement_attempted_sources": attempted_sources,
                "name_supplement_source_results": source_results,
                "name_supplement_memberships_written": len(snapshots),
                "name_supplement_missing_instruments": len(remaining_by_id),
                "name_supplement_matched_level_counts": matched_level_counts,
            }
        )
        if unmatched_name_samples:
            diagnostics["name_supplement_unmatched_name_samples"] = unmatched_name_samples
        return snapshots, diagnostics

    def _build_name_supplement_membership(
        self,
        *,
        hint: IndustryNameHintSnapshot,
        instrument: Dict[str, Any],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
        allow_level2_leaf_matches: bool,
    ) -> tuple[Optional[IndustrySnapshot], Dict[str, str]]:
        node, match_info = self._match_taxonomy_node_by_name(
            industry_name=hint.industry_name,
            taxonomy_by_code=taxonomy_by_code,
            allow_level2_leaf_matches=allow_level2_leaf_matches,
        )
        if node is None:
            return None, match_info

        l1_node, l2_node, l3_node = self._resolve_taxonomy_levels(
            node=node,
            taxonomy_by_code=taxonomy_by_code,
        )
        levels_payload = {
            "sw_l1": self._node_payload(l1_node),
            "sw_l2": self._node_payload(l2_node),
            "sw_l3": self._node_payload(l3_node),
        }
        symbol = str(instrument.get("symbol") or hint.symbol or "").strip()
        raw_payload = {
            "supplement_source": "industry_name_hint",
            "source": hint.source,
            "source_mode": hint.source_mode,
            "source_industry_name": hint.industry_name,
            "matched_industry_code": node.industry_code,
            "matched_industry_name": node.industry_name,
            "matched_industry_level": node.industry_level,
            "match": match_info,
            "raw": hint.raw_payload,
        }
        return (
            IndustrySnapshot(
                instrument_id=str(instrument.get("instrument_id") or hint.instrument_id),
                symbol=symbol,
                exchange=str(instrument.get("exchange") or hint.exchange),
                taxonomy_system=node.taxonomy_system,
                taxonomy_version=node.taxonomy_version,
                industry_code=node.industry_code,
                industry_name=node.industry_name,
                industry_level=node.industry_level,
                parent_code=node.parent_code,
                mapping_status="authoritative",
                effective_date=None,
                source_classification="申万行业名称补源",
                source_industry_name=hint.industry_name,
                sw_l1_code=None if l1_node is None else l1_node.industry_code,
                sw_l1_name=None if l1_node is None else l1_node.industry_name,
                sw_l2_code=None if l2_node is None else l2_node.industry_code,
                sw_l2_name=None if l2_node is None else l2_node.industry_name,
                sw_l3_code=None if l3_node is None else l3_node.industry_code,
                sw_l3_name=None if l3_node is None else l3_node.industry_name,
                source=hint.source,
                source_mode=hint.source_mode,
                membership_json={
                    "levels": levels_payload,
                    "name_supplement": raw_payload,
                },
                raw_payload=raw_payload,
            ),
            match_info,
        )

    def _match_taxonomy_node_by_name(
        self,
        *,
        industry_name: str,
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
        allow_level2_leaf_matches: bool,
    ) -> tuple[Optional[IndustryTaxonomySnapshot], Dict[str, str]]:
        normalized_name = self._normalize_industry_name(industry_name)
        if not normalized_name:
            return None, {"reason": "empty_industry_name"}

        taxonomy_nodes = list(taxonomy_by_code.values())
        children_by_parent = build_taxonomy_children_index(taxonomy_nodes)
        exact_index: Dict[str, List[IndustryTaxonomySnapshot]] = {}
        stripped_index: Dict[str, List[IndustryTaxonomySnapshot]] = {}
        for node in taxonomy_nodes:
            node_name = self._normalize_industry_name(node.industry_name)
            if not node_name:
                continue
            exact_index.setdefault(node_name, []).append(node)
            stripped_index.setdefault(
                self._strip_industry_level_suffix(node_name),
                [],
            ).append(node)

        candidates = exact_index.get(normalized_name) or []
        match_method = "exact_name"
        if not candidates:
            stripped_name = self._strip_industry_level_suffix(normalized_name)
            candidates = stripped_index.get(stripped_name) or []
            match_method = "level_suffix_stripped_name"

        if not candidates:
            return None, {"reason": "no_taxonomy_name_match"}

        for node in sorted(candidates, key=lambda item: int(item.industry_level), reverse=True):
            if int(node.industry_level) == 3:
                return node, {
                    "method": match_method,
                    "matched_level": "3",
                    "source_industry_name": industry_name,
                }

        for node in sorted(candidates, key=lambda item: int(item.industry_level), reverse=True):
            if int(node.industry_level) != 2:
                continue
            has_children = bool(children_by_parent.get(node.industry_code))
            if allow_level2_leaf_matches and not has_children:
                return node, {
                    "method": f"{match_method}:level2_leaf",
                    "matched_level": "2",
                    "source_industry_name": industry_name,
                }

        return None, {"reason": "matched_name_is_not_usable_level3_or_leaf_level2"}

    async def _load_or_fetch_component_sets(
        self,
        *,
        provider: BaseIndustryStandardProvider,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        mode: str,
        force_refresh: bool = False,
    ) -> tuple[Dict[str, set[str]], Optional[str], Optional[str]]:
        taxonomy_system, taxonomy_version = self._resolve_taxonomy_identity(taxonomy_nodes)
        expected_leaf_nodes = get_leaf_taxonomy_nodes(taxonomy_nodes, minimum_level=2)
        expected_leaf_codes = {
            str(node.industry_code)
            for node in expected_leaf_nodes
        }
        if not force_refresh:
            component_sets, cache_source, cache_built_at = self._load_cached_component_sets(
                taxonomy_nodes=taxonomy_nodes,
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
            )
            if component_sets is not None:
                missing_leaf_codes = sorted(expected_leaf_codes.difference(component_sets))
                if not missing_leaf_codes:
                    return component_sets, cache_source, cache_built_at

                missing_leaf_nodes = [
                    node
                    for node in expected_leaf_nodes
                    if str(node.industry_code) in missing_leaf_codes
                ]
                repaired_sets = await provider.fetch_component_sets(
                    taxonomy_nodes=missing_leaf_nodes,
                    mode=mode,
                )
                merged_sets = dict(component_sets)
                for industry_code, symbols in (repaired_sets or {}).items():
                    merged_sets[str(industry_code)] = {
                        str(symbol).strip()
                        for symbol in (symbols or set())
                        if str(symbol).strip()
                    }

                remaining_missing_codes = sorted(expected_leaf_codes.difference(merged_sets))
                if not remaining_missing_codes:
                    self.storage.replace_industry_component_sets(
                        merged_sets,
                        taxonomy_system=taxonomy_system,
                        taxonomy_version=taxonomy_version,
                        source=provider.source_name,
                        source_mode=mode,
                    )
                    cache_info = self.storage.get_latest_industry_component_set_cache_info(
                        taxonomy_system=taxonomy_system,
                        taxonomy_version=taxonomy_version,
                    )
                    cache_built_at = None if cache_info is None else str(
                        cache_info.get("built_at") or cache_info.get("updated_at") or ""
                    ) or None
                    return merged_sets, "cache_gap_fill", cache_built_at

        component_sets = await provider.fetch_component_sets(
            taxonomy_nodes=taxonomy_nodes,
            mode=mode,
        )
        if component_sets:
            self.storage.replace_industry_component_sets(
                component_sets,
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
                source=provider.source_name,
                source_mode=mode,
            )
            cache_info = self.storage.get_latest_industry_component_set_cache_info(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
            )
            cache_built_at = None if cache_info is None else str(
                cache_info.get("built_at") or cache_info.get("updated_at") or ""
            ) or None
            cache_source = "forced_live_fetch" if force_refresh else "live_fetch"
            return component_sets, cache_source, cache_built_at

        return {}, None, None

    def _build_current_component_memberships(
        self,
        *,
        instruments: List[Dict[str, Any]],
        component_sets: Dict[str, set[str]],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
        source: str,
        mode: str,
    ) -> List[IndustrySnapshot]:
        leaf_nodes = get_leaf_taxonomy_nodes(list(taxonomy_by_code.values()), minimum_level=2)
        leaf_codes = {node.industry_code for node in leaf_nodes}
        symbol_to_leaf_code: Dict[str, str] = {}
        for industry_code, symbols in component_sets.items():
            node = taxonomy_by_code.get(industry_code)
            if node is None or industry_code not in leaf_codes:
                continue
            for symbol in symbols:
                normalized_symbol = str(symbol or "").strip()
                if normalized_symbol:
                    symbol_to_leaf_code.setdefault(normalized_symbol, industry_code)

        snapshots: List[IndustrySnapshot] = []
        for instrument in instruments:
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                instrument_id = str(instrument.get("instrument_id") or "")
                symbol = instrument_id.split(".", 1)[0]
            leaf_code = symbol_to_leaf_code.get(symbol)
            if not leaf_code:
                continue
            leaf_node = taxonomy_by_code.get(leaf_code)
            if leaf_node is None:
                continue
            l1_node, l2_node, resolved_l3_node = self._resolve_taxonomy_levels(
                node=leaf_node,
                taxonomy_by_code=taxonomy_by_code,
            )
            levels_payload = {
                "sw_l1": self._node_payload(l1_node),
                "sw_l2": self._node_payload(l2_node),
                "sw_l3": self._node_payload(resolved_l3_node),
            }
            raw_payload = {
                "component_source": "shenwan_leaf_components",
                "symbol": symbol,
                "industry_code": leaf_node.industry_code,
                "industry_name": leaf_node.industry_name,
                "industry_level": leaf_node.industry_level,
            }
            snapshots.append(
                IndustrySnapshot(
                    instrument_id=str(instrument.get("instrument_id") or ""),
                    symbol=symbol,
                    exchange=str(instrument.get("exchange") or ""),
                    taxonomy_system=leaf_node.taxonomy_system,
                    taxonomy_version=leaf_node.taxonomy_version,
                    industry_code=leaf_node.industry_code,
                    industry_name=leaf_node.industry_name,
                    industry_level=leaf_node.industry_level,
                    parent_code=leaf_node.parent_code,
                    mapping_status="authoritative",
                    effective_date=None,
                    source_classification="申万叶子行业成分股",
                    source_industry_name=leaf_node.industry_name,
                    sw_l1_code=None if l1_node is None else l1_node.industry_code,
                    sw_l1_name=None if l1_node is None else l1_node.industry_name,
                    sw_l2_code=None if l2_node is None else l2_node.industry_code,
                    sw_l2_name=None if l2_node is None else l2_node.industry_name,
                    sw_l3_code=None if resolved_l3_node is None else resolved_l3_node.industry_code,
                    sw_l3_name=None if resolved_l3_node is None else resolved_l3_node.industry_name,
                    source=source,
                    source_mode=mode,
                    membership_json={
                        "levels": levels_payload,
                        "component_membership": raw_payload,
                    },
                    raw_payload=raw_payload,
                )
            )
        return snapshots

    def _build_official_snapshots(
        self,
        *,
        instrument: Dict[str, Any],
        official_snapshot: Any,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        mapping: Optional[OfficialShenwanCodeMapping],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
    ) -> tuple[OfficialIndustryClassificationSnapshot, Optional[IndustrySnapshot]]:
        taxonomy_system = str(
            self.research_config.modules.get("industry", {})
            .get("standard", {})
            .get("taxonomy_system", "sw")
        )
        taxonomy_version = str(
            self.research_config.modules.get("industry", {})
            .get("standard", {})
            .get("taxonomy_version", "sw_2021")
        )

        mapped_node = None
        if mapping is not None and mapping.taxonomy_industry_code is not None:
            mapped_node = taxonomy_by_code.get(mapping.taxonomy_industry_code)

        l1_node, l2_node, l3_node = self._resolve_taxonomy_levels(
            node=mapped_node,
            taxonomy_by_code=taxonomy_by_code,
        )

        mapping_status = "mapped" if mapped_node is not None else "unmapped"
        mapping_payload = None if mapping is None else asdict(mapping)
        levels_payload = {
            "sw_l1": self._node_payload(l1_node),
            "sw_l2": self._node_payload(l2_node),
            "sw_l3": self._node_payload(l3_node),
        }

        classification_json = {
            "official": {
                "symbol": official_snapshot.symbol,
                "official_industry_code": official_snapshot.official_industry_code,
                "start_date": official_snapshot.start_date,
                "update_time": official_snapshot.update_time,
                "raw_payload": official_snapshot.raw_payload,
            },
            "mapping": mapping_payload,
            "levels": levels_payload,
        }

        classification_snapshot = OfficialIndustryClassificationSnapshot(
            instrument_id=str(instrument.get("instrument_id", "")),
            symbol=str(instrument.get("symbol", official_snapshot.symbol)),
            exchange=str(instrument.get("exchange", official_snapshot.exchange)),
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            official_industry_code=official_snapshot.official_industry_code,
            official_start_date=official_snapshot.start_date,
            official_update_time=official_snapshot.update_time,
            mapped_industry_code=None if mapped_node is None else mapped_node.industry_code,
            mapped_industry_name=None if mapped_node is None else mapped_node.industry_name,
            mapped_industry_level=None if mapped_node is None else mapped_node.industry_level,
            mapped_parent_code=None if mapped_node is None else mapped_node.parent_code,
            mapping_status=mapping_status,
            mapping_confidence=None if mapping is None else mapping.confidence,
            source=official_snapshot.source,
            source_mode=official_snapshot.source_mode,
            classification_json=classification_json,
        )

        if mapped_node is None:
            return classification_snapshot, None

        membership_snapshot = IndustrySnapshot(
            instrument_id=classification_snapshot.instrument_id,
            symbol=classification_snapshot.symbol,
            exchange=classification_snapshot.exchange,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            industry_code=mapped_node.industry_code,
            industry_name=mapped_node.industry_name,
            industry_level=mapped_node.industry_level,
            parent_code=mapped_node.parent_code,
            mapping_status="authoritative",
            effective_date=official_snapshot.start_date,
            source_classification="申万官方股票分类历史",
            source_industry_name=None,
            sw_l1_code=None if l1_node is None else l1_node.industry_code,
            sw_l1_name=None if l1_node is None else l1_node.industry_name,
            sw_l2_code=None if l2_node is None else l2_node.industry_code,
            sw_l2_name=None if l2_node is None else l2_node.industry_name,
            sw_l3_code=None if l3_node is None else l3_node.industry_code,
            sw_l3_name=None if l3_node is None else l3_node.industry_name,
            source=official_snapshot.source,
            source_mode=official_snapshot.source_mode,
            membership_json=classification_json,
            raw_payload={
                "official": official_snapshot.raw_payload,
                "mapping": mapping_payload,
            },
        )
        return classification_snapshot, membership_snapshot

    @staticmethod
    def _resolve_taxonomy_levels(
        *,
        node: Optional[IndustryTaxonomySnapshot],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
    ) -> tuple[
        Optional[IndustryTaxonomySnapshot],
        Optional[IndustryTaxonomySnapshot],
        Optional[IndustryTaxonomySnapshot],
    ]:
        if node is None:
            return None, None, None

        if int(node.industry_level) == 1:
            return node, None, None

        if int(node.industry_level) == 2:
            l1_node = taxonomy_by_code.get(str(node.parent_code or ""))
            return l1_node, node, None

        l3_node = node
        l2_node = taxonomy_by_code.get(str(l3_node.parent_code or ""))
        l1_node = None
        if l2_node is not None and l2_node.parent_code:
            l1_node = taxonomy_by_code.get(str(l2_node.parent_code))
        return l1_node, l2_node, l3_node

    @staticmethod
    def _node_payload(node: Optional[IndustryTaxonomySnapshot]) -> Optional[Dict[str, Any]]:
        if node is None:
            return None
        return {
            "industry_code": node.industry_code,
            "industry_name": node.industry_name,
            "industry_level": node.industry_level,
            "parent_code": node.parent_code,
        }

    def _load_cached_official_mapping_context(
        self,
        *,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> Optional[_OfficialMappingContext]:
        mapping_cfg = self._official_mapping_config()
        cached_rows = self.storage.get_official_industry_code_mappings(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            max_age_days=int(mapping_cfg.get("cache_max_age_days", 7)),
        )
        minimum_rows = max(1, int(mapping_cfg.get("minimum_mapping_rows", 1)))
        if len(cached_rows) < minimum_rows:
            return None

        mappings: list[OfficialShenwanCodeMapping] = []
        for row in cached_rows:
            mapping_payload = row.get("mapping") or {}
            mappings.append(
                OfficialShenwanCodeMapping(
                    official_industry_code=str(row.get("official_industry_code", "")),
                    best_taxonomy_industry_code=(
                        row.get("best_taxonomy_industry_code")
                        or mapping_payload.get("best_taxonomy_industry_code")
                    ),
                    taxonomy_industry_code=row.get("mapped_industry_code"),
                    overlap_count=int(row.get("overlap_count") or 0),
                    official_symbol_count=int(row.get("official_symbol_count") or 0),
                    taxonomy_symbol_count=int(row.get("taxonomy_symbol_count") or 0),
                    precision=float(row.get("precision") or 0.0),
                    recall=float(row.get("recall") or 0.0),
                    confidence=str(row.get("mapping_confidence") or "unmapped"),
                    mapping_source=str(mapping_payload.get("mapping_source") or "inferred"),
                    override_reason=mapping_payload.get("override_reason"),
                )
            )

        mappings = self._apply_official_mapping_overrides(
            mappings=mappings,
            taxonomy_by_code={node.industry_code: node for node in taxonomy_nodes},
        )
        mapped_code_count = sum(1 for item in mappings if item.taxonomy_industry_code is not None)
        minimum_mapped_rows = max(1, int(mapping_cfg.get("minimum_mapped_rows", 1)))
        if mapped_code_count < minimum_mapped_rows:
            return None
        built_at = None if not cached_rows else str(cached_rows[0].get("built_at") or "")
        return _OfficialMappingContext(
            mapping_by_code={item.official_industry_code: item for item in mappings},
            taxonomy_by_code={node.industry_code: node for node in taxonomy_nodes},
            mapped_code_count=mapped_code_count,
            unmapped_code_count=len(mappings) - mapped_code_count,
            total_code_count=len(mappings),
            component_taxonomy_count=0,
            mapping_source="cache",
            cache_row_count=len(mappings),
            cache_built_at=built_at or None,
        )

    def _resolve_taxonomy_identity(
        self,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
    ) -> tuple[str, str]:
        if taxonomy_nodes:
            first_node = taxonomy_nodes[0]
            return str(first_node.taxonomy_system), str(first_node.taxonomy_version or "")

        standard_cfg = self.research_config.modules.get("industry", {}).get("standard", {})
        return (
            str(standard_cfg.get("taxonomy_system", "sw")),
            str(standard_cfg.get("taxonomy_version", "sw_2021")),
        )

    def _official_mapping_config(self) -> Dict[str, Any]:
        return (
            self.research_config.modules.get("industry", {})
            .get("standard", {})
            .get("official_mapping", {})
        )

    def _official_mapping_overrides(self) -> Dict[str, Dict[str, Any]]:
        raw = self._official_mapping_config().get("manual_overrides", {})
        if not isinstance(raw, dict):
            return {}
        return {str(key).strip(): value for key, value in raw.items() if str(key).strip()}

    def _component_cache_config(self) -> Dict[str, Any]:
        return (
            self.research_config.modules.get("industry", {})
            .get("standard", {})
            .get("component_cache", {})
        )

    def _name_supplement_config(self) -> Dict[str, Any]:
        return (
            self.research_config.modules.get("industry", {})
            .get("standard", {})
            .get("name_supplement", {})
        )

    def _load_cached_taxonomy_nodes(self) -> List[IndustryTaxonomySnapshot]:
        standard_cfg = self.research_config.modules.get("industry", {}).get("standard", {})
        taxonomy_system = str(standard_cfg.get("taxonomy_system") or "sw")
        taxonomy_version = str(standard_cfg.get("taxonomy_version") or "sw_2021")
        loader = getattr(self.storage, "list_industry_taxonomy", None)
        if loader is None:
            return []
        try:
            return loader(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
            )
        except Exception as exc:
            dm_logger.warning(
                "[IndustryStandardSync] Failed to load cached taxonomy %s/%s: %s",
                taxonomy_system,
                taxonomy_version,
                exc,
            )
            return []

    @staticmethod
    def _normalize_industry_name(value: Any) -> str:
        if value is None:
            return ""
        text = unicodedata.normalize("NFKC", str(value)).strip().lower()
        text = re.sub(r"\s+", "", text)
        text = text.replace("（", "(").replace("）", ")")
        return text

    @staticmethod
    def _strip_industry_level_suffix(value: str) -> str:
        text = str(value or "").strip().lower()
        return re.sub(r"(?:i{1,3}|[123]级|一级|二级|三级)$", "", text)

    def _load_cached_component_sets(
        self,
        *,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        taxonomy_system: str,
        taxonomy_version: str,
    ) -> tuple[Optional[Dict[str, set[str]]], Optional[str], Optional[str]]:
        component_cfg = self._component_cache_config()
        cached_sets = self.storage.get_industry_component_sets(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            max_age_days=int(component_cfg.get("cache_max_age_days", 7)),
        )
        if not cached_sets:
            return None, None, None

        target_codes = {
            str(node.industry_code)
            for node in get_leaf_taxonomy_nodes(taxonomy_nodes, minimum_level=2)
        }
        filtered_sets = {
            industry_code: symbols
            for industry_code, symbols in cached_sets.items()
            if industry_code in target_codes
        }
        minimum_component_sets = max(
            1,
            int(component_cfg.get("minimum_component_sets", 1)),
        )
        if len(filtered_sets) < minimum_component_sets:
            return None, None, None

        cache_info = self.storage.get_latest_industry_component_set_cache_info(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
        )
        built_at = None if cache_info is None else str(
            cache_info.get("built_at") or cache_info.get("updated_at") or ""
        ) or None
        return filtered_sets, "cache", built_at

    def _apply_official_mapping_overrides(
        self,
        *,
        mappings: List[OfficialShenwanCodeMapping],
        taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
    ) -> List[OfficialShenwanCodeMapping]:
        overrides = self._official_mapping_overrides()
        if not overrides:
            return list(mappings)

        overridden: List[OfficialShenwanCodeMapping] = []
        for mapping in mappings:
            override_cfg = overrides.get(mapping.official_industry_code)
            if not isinstance(override_cfg, dict):
                overridden.append(mapping)
                continue

            taxonomy_code = str(override_cfg.get("taxonomy_industry_code", "")).strip()
            if not taxonomy_code:
                overridden.append(mapping)
                continue

            if taxonomy_code not in taxonomy_by_code:
                dm_logger.warning(
                    "[IndustryStandardSync] Ignoring official mapping override for %s: taxonomy code %s not found",
                    mapping.official_industry_code,
                    taxonomy_code,
                )
                overridden.append(mapping)
                continue

            override_reason = str(override_cfg.get("reason", "")).strip() or None
            confidence = str(override_cfg.get("confidence") or "high")
            overridden.append(
                OfficialShenwanCodeMapping(
                    official_industry_code=mapping.official_industry_code,
                    best_taxonomy_industry_code=(
                        mapping.best_taxonomy_industry_code or taxonomy_code
                    ),
                    taxonomy_industry_code=taxonomy_code,
                    overlap_count=mapping.overlap_count,
                    official_symbol_count=mapping.official_symbol_count,
                    taxonomy_symbol_count=mapping.taxonomy_symbol_count,
                    precision=mapping.precision,
                    recall=mapping.recall,
                    confidence=confidence,
                    mapping_source="manual_override",
                    override_reason=override_reason,
                )
            )
        return overridden

    def _allow_live_rebuild_on_cache_miss(self) -> bool:
        return bool(self._official_mapping_config().get("allow_live_rebuild_on_cache_miss", True))

    def _build_code_mapper(self) -> OfficialShenwanCodeMapper:
        mapping_cfg = self._official_mapping_config()
        return OfficialShenwanCodeMapper(
            min_overlap_count=int(mapping_cfg.get("min_overlap_count", 2)),
            min_precision=float(mapping_cfg.get("min_precision", 0.6)),
            min_recall=float(mapping_cfg.get("min_recall", 0.6)),
        )

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    @staticmethod
    def _build_no_membership_message(diagnostics: Dict[str, Any]) -> str:
        if not diagnostics:
            return "Provider returned no authoritative memberships"

        parts = ["Provider returned no authoritative memberships"]
        for key in (
            "official_fetch_error",
            "official_target_records",
            "official_unmapped_records",
            "name_supplement_target_instruments",
            "name_supplement_hints_returned",
            "name_supplement_memberships_written",
            "name_supplement_missing_instruments",
            "fallback_target_instruments",
            "fallback_memberships_written",
            "fallback_error",
            "attempted_third_codes",
            "failed_third_codes",
            "matched_instruments",
            "missing_instruments",
        ):
            value = diagnostics.get(key)
            if value is None:
                continue
            parts.append(f"{key}={value}")

        fallback_diagnostics = diagnostics.get("fallback_diagnostics") or {}
        for key in (
            "attempted_third_codes",
            "failed_third_codes",
            "matched_instruments",
            "missing_instruments",
        ):
            value = fallback_diagnostics.get(key)
            if value is None:
                continue
            parts.append(f"{key}={value}")

        return "; ".join(parts)
