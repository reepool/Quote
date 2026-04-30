"""Operational helpers for strict Shenwan industry standard maintenance."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


INDUSTRY_STANDARD_TABLES = [
    "industry_source_files",
    "industry_taxonomy",
    "industry_classification_history",
    "industry_official_classifications",
    "industry_memberships",
    "industry_component_sets",
    "industry_official_code_mappings",
]


def count_industry_standard_tables(storage: Any) -> Dict[str, int]:
    """Return row counts for rebuild-relevant industry tables."""
    counts: Dict[str, int] = {}
    with storage.get_connection() as conn:
        for table in INDUSTRY_STANDARD_TABLES:
            try:
                counts[table] = int(
                    conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
            except Exception:
                counts[table] = -1
    return counts


async def rebuild_official_industry_standard(
    manager: Any,
    *,
    exchanges: Optional[List[str]],
    limit_per_exchange: Optional[int],
    budget_mode: Optional[str],
    allow_paid_proxy: Optional[bool],
    drop_existing: bool,
    drop_source_files: bool,
    force_refresh: bool,
) -> Dict[str, Any]:
    """Clear and rebuild strict Shenwan rows from the official classification source.

    The destructive scope is intentionally limited to the strict Shenwan industry
    standard slice. Other research domains and quote data are not touched.
    """
    standard_cfg = manager.research_config.modules.get("industry", {}).get("standard", {})
    taxonomy_system = str(standard_cfg.get("taxonomy_system", "sw"))
    taxonomy_version = str(standard_cfg.get("taxonomy_version", "sw_2021"))
    if manager.research_storage is None:
        raise RuntimeError("research storage is not initialized")

    before_counts = count_industry_standard_tables(manager.research_storage)
    cleared: Dict[str, int] = {}
    if drop_existing:
        cleared = manager.research_storage.clear_industry_standard_slice(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            include_source_files=drop_source_files,
        )

    sync_result = await manager.run_industry_standard_sync(
        exchanges=exchanges,
        limit_per_exchange=limit_per_exchange,
        budget_mode=budget_mode,
        allow_paid_proxy=allow_paid_proxy,
        force_component_refresh=force_refresh or drop_source_files,
    )
    readiness = await manager.get_research_industry_standard_readiness()
    after_counts = count_industry_standard_tables(manager.research_storage)
    return {
        "status": sync_result.get("status"),
        "requested": {
            "exchanges": exchanges,
            "limit_per_exchange": limit_per_exchange,
            "budget_mode": budget_mode,
            "allow_paid_proxy": allow_paid_proxy,
            "drop_existing": drop_existing,
            "drop_source_files": drop_source_files,
            "force_refresh": force_refresh,
        },
        "taxonomy": {
            "taxonomy_system": taxonomy_system,
            "taxonomy_version": taxonomy_version,
        },
        "cleared": cleared,
        "table_counts": {
            "before": before_counts,
            "after": after_counts,
        },
        "sync": sync_result,
        "readiness": readiness,
    }
