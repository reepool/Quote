"""
Sync service for official Shenwan industry index-analysis rows.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from research.providers.base import INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS
from research.providers.registry import IndustryIndexAnalysisProviderRegistry
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager


class IndustryIndexAnalysisSyncService:
    """Fetch and persist official SWS index-analysis daily metrics."""

    def __init__(
        self,
        *,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        provider_registry: Optional[IndustryIndexAnalysisProviderRegistry] = None,
    ):
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.provider_registry = provider_registry or IndustryIndexAnalysisProviderRegistry(
            research_config=self.research_config
        )

    async def sync_latest(
        self,
        *,
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
        source: str = "swsresearch",
        mode: str = "direct",
    ) -> Dict[str, Any]:
        """Sync the latest official daily rows without touching stock memberships."""
        return await self._sync_index_analysis(
            index_types=index_types,
            limit_per_type=limit_per_type,
            start_date=start_date,
            end_date=end_date,
            latest_date=latest_date,
            source=source,
            mode=mode,
            job_name="industry_index_analysis_sync",
            operation="latest",
        )

    async def sync_history(
        self,
        *,
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        start_date: str,
        end_date: str,
        source: str = "akshare",
        mode: str = "direct",
    ) -> Dict[str, Any]:
        """Backfill historical SWS index-analysis rows without stock memberships."""
        return await self._sync_index_analysis(
            index_types=index_types,
            limit_per_type=limit_per_type,
            start_date=start_date,
            end_date=end_date,
            latest_date=None,
            source=source,
            mode=mode,
            job_name="industry_index_analysis_backfill",
            operation="history",
        )

    async def _sync_index_analysis(
        self,
        *,
        index_types: Optional[List[str]],
        limit_per_type: Optional[int],
        start_date: Optional[str],
        end_date: Optional[str],
        latest_date: Optional[str],
        source: str,
        mode: str,
        job_name: str,
        operation: str,
    ) -> Dict[str, Any]:
        """Sync index-analysis rows from one provider."""
        industry_cfg = self.research_config.modules.get("industry", {})
        standard_cfg = industry_cfg.get("standard", {})
        index_cfg = self._source_index_analysis_config(source)
        taxonomy_system = standard_cfg.get("taxonomy_system", "sw")
        taxonomy_version = standard_cfg.get("taxonomy_version", "sw_2021")
        configured_types = index_cfg.get("supported_index_types", [])
        target_types = list(index_types or configured_types or [])

        if not index_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": f"{source}.index_analysis.enabled is false",
            }

        provider = self.provider_registry.get(source)
        if provider is None:
            return {"status": "failed", "reason": f"provider_not_found:{source}"}

        run_id = self.storage.start_ingestion_run(
            domain="industry_index_analysis",
            job_name=job_name,
            source=source,
            mode=mode,
            metadata={
                "operation": operation,
                "index_types": target_types,
                "limit_per_type": limit_per_type,
                "start_date": start_date,
                "end_date": end_date,
                "latest_date": latest_date,
            },
        )
        try:
            dm_logger.info(
                "[IndustryIndexAnalysisSync] Sync starting "
                "(operation=%s, source=%s, mode=%s, index_types=%s, limit_per_type=%s, "
                "start_date=%s, end_date=%s, latest_date=%s)",
                operation,
                source,
                mode,
                target_types,
                limit_per_type,
                start_date,
                end_date,
                latest_date,
            )
            snapshots = await provider.fetch_latest_index_analysis(
                mode=mode,
                index_types=target_types or None,
                limit_per_type=limit_per_type,
                start_date=start_date,
                end_date=end_date,
                latest_date=latest_date,
            )
            coverage = summarize_index_analysis_snapshots(snapshots)
            dm_logger.info(
                "[IndustryIndexAnalysisSync] Provider fetch finished "
                "(rows=%s, trade_dates=%s, start=%s, end=%s, index_types=%s)",
                len(snapshots),
                coverage.get("trade_dates", 0),
                coverage.get("start_date"),
                coverage.get("end_date"),
                sorted((coverage.get("index_type_counts") or {}).keys()),
            )
            for snapshot in snapshots:
                self.storage.upsert_industry_index_analysis(
                    snapshot,
                    ingestion_run_id=run_id,
                )

            summary = self.storage.summarize_industry_index_analysis_daily(
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
            )
            dm_logger.info(
                "[IndustryIndexAnalysisSync] Rows upserted and storage summarized "
                "(rows_written=%s, latest_trade_date=%s, distinct_index_codes=%s)",
                len(snapshots),
                summary.get("latest_trade_date"),
                summary.get("distinct_index_codes", 0),
            )
            self.storage.finish_ingestion_run(
                run_id,
                status="success",
                rows_written=len(snapshots),
                metadata={
                    "operation": operation,
                    "summary": summary,
                    "coverage": coverage,
                    "field_units": INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS,
                },
            )
            return {
                "status": "success",
                "operation": operation,
                "source": source,
                "mode": mode,
                "rows_written": len(snapshots),
                "summary": summary,
                "coverage": coverage,
                "field_units": INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS,
            }
        except Exception as exc:
            dm_logger.error("[IndustryIndexAnalysisSync] Sync failed: %s", exc)
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                error_message=str(exc),
                metadata={
                    "operation": operation,
                    "index_types": target_types,
                    "limit_per_type": limit_per_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "latest_date": latest_date,
                },
            )
            return {"status": "failed", "reason": str(exc), "rows_written": 0}

    def _source_index_analysis_config(self, source: str) -> Dict[str, Any]:
        source_cfg = self.research_config.sources.get(source, {}).get(
            "index_analysis",
            {},
        )
        if source_cfg:
            return source_cfg
        if source == "akshare":
            return self.research_config.sources.get("swsresearch", {}).get(
                "index_analysis",
                {},
            )
        return source_cfg


def summarize_index_analysis_snapshots(
    snapshots: List[Any],
) -> Dict[str, Any]:
    """Summarize rows fetched in one index-analysis run."""
    metric_fields = tuple(INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS.keys())
    by_type: Dict[str, Dict[str, Any]] = {}
    trade_dates: set[str] = set()
    for snapshot in snapshots:
        index_type = snapshot.index_type or ""
        bucket = by_type.setdefault(
            index_type,
            {
                "rows": 0,
                "codes": set(),
                "trade_dates": set(),
                "missing_metrics": {field: 0 for field in metric_fields},
            },
        )
        bucket["rows"] += 1
        bucket["codes"].add(snapshot.sw_index_code)
        bucket["trade_dates"].add(snapshot.trade_date)
        trade_dates.add(snapshot.trade_date)
        for field in metric_fields:
            if getattr(snapshot, field, None) is None:
                bucket["missing_metrics"][field] += 1

    normalized_by_type: Dict[str, Dict[str, Any]] = {}
    for index_type, bucket in by_type.items():
        dates = sorted(bucket["trade_dates"])
        normalized_by_type[index_type] = {
            "rows": bucket["rows"],
            "codes": len(bucket["codes"]),
            "trade_dates": len(dates),
            "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None,
            "missing_metrics": {
                field: count
                for field, count in bucket["missing_metrics"].items()
                if count
            },
        }

    dates = sorted(trade_dates)
    return {
        "rows": len(snapshots),
        "trade_dates": len(dates),
        "start_date": dates[0] if dates else None,
        "end_date": dates[-1] if dates else None,
        "index_type_counts": normalized_by_type,
    }
