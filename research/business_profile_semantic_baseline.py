"""Read-only baseline audit for automated business-profile semantic production."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from research.business_profile_semantic_contracts import (
    business_profile_field_family_manifest,
)


BUSINESS_PROFILE_SEMANTIC_BASELINE_SCHEMA_VERSION = (
    "business_profile_semantic_production_baseline.v1"
)

GOVERNED_TABLES: tuple[str, ...] = (
    "business_profile_semantic_runs",
    "business_profile_exceptions",
    "business_profile_evidence",
    "company_business_profile_events",
    "company_business_profile_regimes",
    "company_business_segments",
    "company_operating_facts",
    "company_business_activities",
    "company_value_chain_roles",
    "company_supply_chain_relationships",
    "company_commodity_exposure_facts",
    "company_commodity_exposure_assumptions",
    "company_commodity_exposures",
    "business_profile_review_audit",
)


def build_business_profile_semantic_baseline(
    *,
    research_db_path: str | Path,
    financials_db_path: str | Path,
    archive_root: str | Path,
    research_config_path: str | Path,
    scheduler_config_path: str | Path,
    fact_catalog_path: str | Path,
    product_catalog_path: str | Path,
    generated_at: Optional[str] = None,
) -> dict[str, Any]:
    """Build a deterministic, hash-bound inventory without changing local state."""

    research_db = Path(research_db_path)
    financials_db = Path(financials_db_path)
    archive = Path(archive_root)
    research_config = _load_json(Path(research_config_path))
    scheduler_config = _load_json(Path(scheduler_config_path))
    fact_catalog = _load_json(Path(fact_catalog_path))
    product_catalog = _load_json(Path(product_catalog_path))

    payload: dict[str, Any] = {
        "schema_version": BUSINESS_PROFILE_SEMANTIC_BASELINE_SCHEMA_VERSION,
        "production_tables": _audit_production_tables(research_db),
        "official_manifests": _audit_manifests(financials_db),
        "archive_artifacts": _audit_archive(archive),
        "catalogs": {
            "fact_catalog": _catalog_summary(
                Path(fact_catalog_path),
                fact_catalog,
                collection_key="fields",
            ),
            "product_catalog": {
                **_catalog_summary(
                    Path(product_catalog_path),
                    product_catalog,
                    collection_key="products",
                ),
                "alias_count": len(product_catalog.get("aliases") or []),
                "commodity_mapping_count": len(
                    product_catalog.get("commodity_mappings") or []
                ),
            },
        },
        "field_families": business_profile_field_family_manifest(),
        "enablement": _audit_enablement(research_config, scheduler_config),
    }
    payload["baseline_hash"] = _stable_hash(payload)
    payload["generated_at"] = generated_at or datetime.now(timezone.utc).isoformat()
    return payload


def _audit_production_tables(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"database_exists": False, "tables": {}}
    tables: dict[str, Any] = {}
    with _read_only_connection(path) as conn:
        available = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        for table in GOVERNED_TABLES:
            if table not in available:
                tables[table] = {"exists": False, "row_count": None}
                continue
            row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            status_counts: dict[str, int] = {}
            columns = {
                str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")
            }
            if "review_status" in columns:
                status_counts = {
                    str(row[0]): int(row[1])
                    for row in conn.execute(
                        f"SELECT review_status, COUNT(*) FROM {table} "
                        "GROUP BY review_status ORDER BY review_status"
                    ).fetchall()
                }
            lifecycle_status_counts: dict[str, int] = {}
            if "status" in columns:
                lifecycle_status_counts = {
                    str(row[0]): int(row[1])
                    for row in conn.execute(
                        f"SELECT status, COUNT(*) FROM {table} "
                        "GROUP BY status ORDER BY status"
                    ).fetchall()
                }
            tables[table] = {
                "exists": True,
                "row_count": row_count,
                "review_status_counts": status_counts,
                "status_counts": lifecycle_status_counts,
            }
    return {
        "database_exists": True,
        "database_size_bytes": path.stat().st_size,
        "tables": tables,
    }


def _audit_manifests(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"database_exists": False, "manifest_count": 0}
    with _read_only_connection(path) as conn:
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'financial_source_files'"
        ).fetchone()
        if table_exists is None:
            return {
                "database_exists": True,
                "manifest_table_exists": False,
                "manifest_count": 0,
            }
        rows = conn.execute(
            """
            SELECT source_file_id, instrument_id, report_period, report_type,
                   source, source_tier, status, content_hash, archive_path,
                   supersedes_source_file_id
            FROM financial_source_files
            WHERE schema_version = 'business_profile_source_file_manifest.v1'
            ORDER BY source_file_id
            """
        ).fetchall()
    records = [dict(row) for row in rows]
    superseded_ids = {
        str(item.get("supersedes_source_file_id") or "")
        for item in records
        if item.get("supersedes_source_file_id")
    }
    return {
        "database_exists": True,
        "manifest_table_exists": True,
        "manifest_count": len(records),
        "active_manifest_count": sum(
            str(item.get("source_file_id") or "") not in superseded_ids
            for item in records
        ),
        "instrument_count": len(
            {str(item.get("instrument_id") or "") for item in records}
        ),
        "instrument_period_count": len(
            {
                (
                    str(item.get("instrument_id") or ""),
                    str(item.get("report_period") or ""),
                )
                for item in records
            }
        ),
        "report_type_counts": dict(
            sorted(Counter(str(item.get("report_type") or "unknown") for item in records).items())
        ),
        "source_counts": dict(
            sorted(Counter(str(item.get("source") or "unknown") for item in records).items())
        ),
        "status_counts": dict(
            sorted(Counter(str(item.get("status") or "unknown") for item in records).items())
        ),
        "manifest_inventory_hash": _stable_hash(records),
    }


def _audit_archive(root: Path) -> dict[str, Any]:
    if not root.exists():
        return {"archive_exists": False, "file_count": 0, "suffix_counts": {}}
    inventory = []
    suffix_counts: Counter[str] = Counter()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        suffix = ".json.gz" if path.name.endswith(".json.gz") else path.suffix.lower()
        suffix_counts[suffix or "<none>"] += 1
        inventory.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "suffix": suffix,
            }
        )
    return {
        "archive_exists": True,
        "file_count": len(inventory),
        "suffix_counts": dict(sorted(suffix_counts.items())),
        "page_artifact_count": sum(
            item["suffix"] in {".json", ".json.gz"} for item in inventory
        ),
        "archive_inventory_hash": _stable_hash(inventory),
    }


def _audit_enablement(
    research_config: Mapping[str, Any],
    scheduler_config: Mapping[str, Any],
) -> dict[str, Any]:
    research_root = _config_root(
        research_config,
        primary_key="research_config",
        legacy_key="research",
    )
    scheduler_root = _config_root(
        scheduler_config,
        primary_key="scheduler_config",
        legacy_key="scheduler",
    )
    module = (
        research_root
        .get("modules", {})
        .get("business_profile_evidence", {})
    )
    jobs = scheduler_root.get("jobs", {})
    structured_job = jobs.get("business_profile_structured_sync", {})
    semantic_job = jobs.get("business_profile_daily_incremental", {})
    llm = module.get("llm_extraction", {}) if isinstance(module, Mapping) else {}
    semantic = (
        module.get("semantic_production", {})
        if isinstance(module, Mapping)
        else {}
    )
    structured = (
        module.get("free_structured_sources", {})
        if isinstance(module, Mapping)
        else {}
    )
    return {
        "business_profile_module_enabled": bool(module.get("enabled")),
        "structured_source_enabled": bool(structured.get("enabled")),
        "llm_extraction_enabled": bool(llm.get("enabled")),
        "llm_candidate_only": bool(llm.get("candidate_only", True)),
        "semantic_production_enabled": bool(semantic.get("enabled")),
        "semantic_promotion_enabled": bool(semantic.get("promotion_enabled")),
        "semantic_scheduler_gate_enabled": bool(semantic.get("scheduler_enabled")),
        "structured_scheduler_enabled": bool(structured_job.get("enabled")),
        "structured_scheduler_manual_only": bool(structured_job.get("manual_only")),
        "semantic_scheduler_enabled": bool(semantic_job.get("enabled")),
        "semantic_scheduler_manual_only": bool(semantic_job.get("manual_only")),
    }


def _config_root(
    payload: Mapping[str, Any],
    *,
    primary_key: str,
    legacy_key: str,
) -> Mapping[str, Any]:
    """Return the production config root while retaining old fixture compatibility."""

    root = payload.get(primary_key)
    if not isinstance(root, Mapping):
        root = payload.get(legacy_key)
    return root if isinstance(root, Mapping) else {}


def _catalog_summary(
    path: Path,
    payload: Mapping[str, Any],
    *,
    collection_key: str,
) -> dict[str, Any]:
    return {
        "schema_version": payload.get("schema_version"),
        "catalog_version": payload.get("catalog_version"),
        "entry_count": len(payload.get(collection_key) or []),
        "file_hash": _file_hash(path),
    }


def _read_only_connection(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _load_json(path: Path) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
