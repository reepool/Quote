import json
import sqlite3
from pathlib import Path

import pytest

from research.backtest_data.catalog import (
    BacktestDataCatalog,
    CatalogValidationError,
    RuntimeReadinessAggregator,
    load_default_catalog,
)
from research.backtest_data.probes import (
    BoundedProbeScope,
    ExistingResourceProbeSuite,
    ReadOnlySQLiteInspector,
)


def _resource(decision: str) -> dict:
    is_new = decision == "new_source_required"
    return {
        "dataset": f"dataset_{decision}",
        "description": decision,
        "route_decision": decision,
        "temporal_contract": (
            "unavailable" if decision in {"manual_import_only", "unavailable"} else "knowledge_time_safe"
        ),
        "markets": ["SSE"],
        "required_history_start": "2020-01-01",
        "frequency": "daily",
        "key_fields": ["instrument_id", "available_at"],
        "quality_threshold": "strict",
        "providers": [
            {
                "name": "approved_new" if is_new else "existing",
                "existing": not is_new,
                "full_market": is_new,
                "capabilities": ["test"],
            }
        ],
        "parent_job": "daily_data_update",
        "target_universe_owner": "daily_data_update",
        "transport": "shared",
        "checkpoint": "existing",
        "store": "quotes.db",
        "watermark_domain": "test",
        "read_api": "/test",
        "forward_owner": "daily_data_update",
        "historical_backfill_owner": "a_share_daily_data_historical_backfill",
        "probe_evidence": ([{"id": "bounded", "max_requests": 1}] if is_new else []),
        "new_source_approved": is_new,
    }


def _write_catalog(path: Path, resources: list[dict]) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": "backtest-data-resources.v1",
                "catalog_version": "test.v1",
                "resources": resources,
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(
    "decision",
    ["reuse", "extend_existing", "new_source_required", "manual_import_only", "unavailable"],
)
def test_catalog_accepts_all_governed_route_decisions(tmp_path, decision):
    catalog = BacktestDataCatalog.load(
        _write_catalog(tmp_path / "catalog.json", [_resource(decision)])
    )

    assert catalog.resources[0].route_decision == decision


def test_default_catalog_reuses_existing_parent_workflows():
    catalog = load_default_catalog()

    assert catalog.get("canonical_corporate_actions").route_decision == "reuse"
    assert catalog.get("index_composition").route_decision == "extend_existing"
    assert catalog.get("industry_membership").temporal_contract == "effective_date_only"
    assert all(not item.standalone_job for item in catalog.resources)


def test_catalog_rejects_unapproved_full_market_source(tmp_path):
    resource = _resource("extend_existing")
    resource["providers"] = [
        {"name": "unapproved", "existing": False, "full_market": True}
    ]

    with pytest.raises(CatalogValidationError, match="new full-market providers"):
        BacktestDataCatalog.load(
            _write_catalog(tmp_path / "catalog.json", [resource])
        )


def test_catalog_rejects_unknown_owner_without_approved_probe(tmp_path):
    resource = _resource("reuse")
    resource["parent_job"] = "new_umbrella_cron"

    with pytest.raises(CatalogValidationError, match="unknown parent job"):
        BacktestDataCatalog.load(
            _write_catalog(tmp_path / "catalog.json", [resource])
        )


def test_readiness_is_scoped_and_effective_date_only_is_not_strict_ready():
    catalog = load_default_catalog()
    readiness = RuntimeReadinessAggregator(catalog).aggregate(
        {
            "industry_membership": {
                "target_count": 1,
                "covered_count": 1,
                "covered_start": "2010-01-01",
                "covered_end": "2026-08-04",
            }
        },
        market="SSE",
        start_date="2020-01-01",
        end_date="2025-12-31",
        strict_pit=True,
    )
    industry = next(
        item for item in readiness["resources"] if item["dataset"] == "industry_membership"
    )

    assert industry["ready"] is False
    assert "temporal_contract_effective_date_only" in industry["blockers"]


def test_probe_scope_rejects_unbounded_requests():
    with pytest.raises(ValueError, match="must not exceed 20"):
        BoundedProbeScope.build(
            identifiers=[f"id-{index}" for index in range(21)],
            start_date="2026-08-01",
            end_date="2026-08-02",
        )
    with pytest.raises(ValueError, match="must not exceed 31 days"):
        BoundedProbeScope.build(
            identifiers=["000001.SZ"],
            start_date="2026-01-01",
            end_date="2026-02-15",
        )


def test_read_only_inspector_does_not_modify_database(tmp_path):
    path = tmp_path / "quotes.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE daily_quotes (instrument_id TEXT, time TEXT)")
        connection.execute("INSERT INTO daily_quotes VALUES ('000001.SZ', '2026-08-01')")
        connection.commit()
    before = path.read_bytes()

    result = ReadOnlySQLiteInspector(path).inspect(("daily_quotes",))

    assert result["tables"]["daily_quotes"]["rows"] == 1
    assert path.read_bytes() == before


def test_probe_suite_reports_industry_as_effective_date_only(tmp_path):
    quote_path = tmp_path / "quotes.db"
    financial_path = tmp_path / "financials.db"
    research_path = tmp_path / "research.db"
    for path in (quote_path, financial_path, research_path):
        sqlite3.connect(path).close()
    with sqlite3.connect(research_path) as connection:
        connection.execute(
            "CREATE TABLE industry_classification_history ("
            "instrument_id TEXT, official_start_date TEXT, official_industry_code TEXT)"
        )
        connection.commit()
    suite = ExistingResourceProbeSuite(
        catalog=load_default_catalog(),
        quotes_db_path=quote_path,
        financials_db_path=financial_path,
        research_db_path=research_path,
    )
    scope = BoundedProbeScope.build(
        identifiers=["000001.SZ"],
        start_date="2026-08-01",
        end_date="2026-08-04",
        markets=["SZSE"],
    )

    result = suite.probe_industry_membership(scope)

    assert result.temporal_contract == "effective_date_only"
    assert result.as_dict()["no_write"] is True
