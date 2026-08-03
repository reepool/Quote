import json
import sqlite3
from pathlib import Path

from research.business_profile_semantic_baseline import (
    BUSINESS_PROFILE_SEMANTIC_BASELINE_SCHEMA_VERSION,
    build_business_profile_semantic_baseline,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_baseline_is_read_only_hash_bound_and_counts_governed_inputs(tmp_path):
    research_db = tmp_path / "research.db"
    with sqlite3.connect(research_db) as conn:
        conn.executescript(
            """
            CREATE TABLE business_profile_evidence (
                evidence_id TEXT PRIMARY KEY,
                review_status TEXT NOT NULL
            );
            INSERT INTO business_profile_evidence VALUES ('e-1', 'candidate');
            CREATE TABLE business_profile_review_audit (audit_id TEXT PRIMARY KEY);
            """
        )
    financials_db = tmp_path / "financials.db"
    with sqlite3.connect(financials_db) as conn:
        conn.executescript(
            """
            CREATE TABLE financial_source_files (
                source_file_id TEXT PRIMARY KEY,
                instrument_id TEXT,
                report_period TEXT,
                report_type TEXT,
                source TEXT,
                source_tier TEXT,
                status TEXT,
                content_hash TEXT,
                archive_path TEXT,
                supersedes_source_file_id TEXT,
                schema_version TEXT
            );
            INSERT INTO financial_source_files VALUES (
                'f-1', '000001.SZ', '2025-12-31', 'annual_report', 'cninfo',
                'official_primary', 'archived', 'abc', '2025/a.pdf', NULL,
                'business_profile_source_file_manifest.v1'
            );
            """
        )
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "a.pdf").write_bytes(b"pdf")
    (archive / "a.json.gz").write_bytes(b"artifact")
    research_config = tmp_path / "research.json"
    scheduler_config = tmp_path / "scheduler.json"
    fact_catalog = tmp_path / "facts.json"
    product_catalog = tmp_path / "products.json"
    _write_json(
        research_config,
        {
            "research_config": {
                "modules": {
                    "business_profile_evidence": {
                        "enabled": False,
                        "free_structured_sources": {"enabled": True},
                        "llm_extraction": {"enabled": False, "candidate_only": True},
                        "semantic_production": {
                            "enabled": False,
                            "promotion_enabled": False,
                            "scheduler_enabled": False,
                        },
                    }
                }
            }
        },
    )
    _write_json(
        scheduler_config,
        {
            "scheduler_config": {
                "jobs": {
                    "business_profile_structured_sync": {
                        "enabled": False,
                        "manual_only": False,
                    },
                    "business_profile_semantic_maintenance": {
                        "enabled": False,
                        "manual_only": False,
                    },
                }
            }
        },
    )
    _write_json(
        fact_catalog,
        {"schema_version": "facts.v1", "catalog_version": "facts.1", "fields": [{}]},
    )
    _write_json(
        product_catalog,
        {
            "schema_version": "products.v1",
            "catalog_version": "products.1",
            "products": [{}, {}],
            "aliases": [{}],
            "commodity_mappings": [{}, {}, {}],
        },
    )

    first = build_business_profile_semantic_baseline(
        research_db_path=research_db,
        financials_db_path=financials_db,
        archive_root=archive,
        research_config_path=research_config,
        scheduler_config_path=scheduler_config,
        fact_catalog_path=fact_catalog,
        product_catalog_path=product_catalog,
        generated_at="2026-08-03T00:00:00+00:00",
    )
    second = build_business_profile_semantic_baseline(
        research_db_path=research_db,
        financials_db_path=financials_db,
        archive_root=archive,
        research_config_path=research_config,
        scheduler_config_path=scheduler_config,
        fact_catalog_path=fact_catalog,
        product_catalog_path=product_catalog,
        generated_at="2026-08-04T00:00:00+00:00",
    )

    assert first["schema_version"] == BUSINESS_PROFILE_SEMANTIC_BASELINE_SCHEMA_VERSION
    assert first["baseline_hash"] == second["baseline_hash"]
    assert first["production_tables"]["tables"]["business_profile_evidence"] == {
        "exists": True,
        "row_count": 1,
        "review_status_counts": {"candidate": 1},
        "status_counts": {},
    }
    assert first["official_manifests"]["manifest_count"] == 1
    assert first["archive_artifacts"]["suffix_counts"] == {
        ".json.gz": 1,
        ".pdf": 1,
    }
    assert first["archive_artifacts"]["page_artifact_count"] == 1
    assert first["catalogs"]["product_catalog"]["commodity_mapping_count"] == 3
    assert first["enablement"]["structured_source_enabled"] is True
    assert first["enablement"]["semantic_production_enabled"] is False
    assert first["enablement"]["semantic_promotion_enabled"] is False
    assert first["enablement"]["semantic_scheduler_gate_enabled"] is False
    assert first["enablement"]["semantic_scheduler_enabled"] is False


def test_baseline_does_not_create_missing_databases(tmp_path):
    research_config = tmp_path / "research.json"
    scheduler_config = tmp_path / "scheduler.json"
    fact_catalog = tmp_path / "facts.json"
    product_catalog = tmp_path / "products.json"
    _write_json(research_config, {"research_config": {"modules": {}}})
    _write_json(scheduler_config, {"scheduler_config": {"jobs": {}}})
    _write_json(
        fact_catalog,
        {"schema_version": "facts.v1", "catalog_version": "facts.1", "fields": []},
    )
    _write_json(
        product_catalog,
        {
            "schema_version": "products.v1",
            "catalog_version": "products.1",
            "products": [],
            "aliases": [],
            "commodity_mappings": [],
        },
    )
    missing_research = tmp_path / "missing-research.db"
    missing_financials = tmp_path / "missing-financials.db"

    report = build_business_profile_semantic_baseline(
        research_db_path=missing_research,
        financials_db_path=missing_financials,
        archive_root=tmp_path / "missing-archive",
        research_config_path=research_config,
        scheduler_config_path=scheduler_config,
        fact_catalog_path=fact_catalog,
        product_catalog_path=product_catalog,
    )

    assert report["production_tables"]["database_exists"] is False
    assert report["official_manifests"]["database_exists"] is False
    assert not missing_research.exists()
    assert not missing_financials.exists()
