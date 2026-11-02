"""Load the current-version financial source-field mapping catalog into financials.db.

Post-incident operator tool (2026-11-03): the cross-validation change bumped
MAPPING_VERSION to v6 in code but the registry sync chain had not run since,
leaving the production financial_source_field_mappings table with v5 rows only
- the disclosure readiness gate then reports mapping_catalog_empty /
outside_approved_local_core for every instrument.

Mirrors FinancialStatementsShadowSyncService._ensure_local_core_mapping_catalog
exactly (code catalog -> dedupe -> upsert) with an audited ingestion run.
"""
import sys

sys.path.insert(0, "/home/python/Quote")

from utils.config_manager import config_manager
from research.financial_source_field_mapping import (
    FINANCIAL_STATEMENT_PROFILES,
    MAPPING_VERSION,
    get_financial_source_field_mappings,
)
from research.storage import ResearchStorageManager


def main() -> None:
    research_config = config_manager.get_research_config()
    storage = ResearchStorageManager(research_config)
    storage.initialize()
    run_id = storage.start_ingestion_run(
        domain="financial",
        job_name="load_mapping_catalog_v6_repair",
        source="financial_source_field_mapping",
        mode="operator_repair",
        metadata={"reason": "v6 registry never materialized after c402cd08"},
    )
    mappings = []
    seen = set()
    for profile in FINANCIAL_STATEMENT_PROFILES:
        for mapping in get_financial_source_field_mappings(
            profile=profile,
            mapping_version=MAPPING_VERSION,
        ):
            key = (
                mapping.mapping_version,
                mapping.profile,
                mapping.canonical_fact,
                mapping.sina_field,
                mapping.ths_metric,
            )
            if key in seen:
                continue
            seen.add(key)
            mappings.append(mapping)
    rows = storage.financial_statements.upsert_source_field_mappings(
        mappings,
        ingestion_run_id=run_id,
    )
    storage.finish_ingestion_run(run_id, status="success")
    print(f"mapping_version={MAPPING_VERSION} rows_synced={rows} run_id={run_id}")


if __name__ == "__main__":
    main()
