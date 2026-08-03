import hashlib
import shutil
import sqlite3
from pathlib import Path

import pytest

from research.business_profile_exposure_components import (
    BusinessProfileExposureComponentMigrator,
)
from research.business_profile_governance import (
    BusinessProfileRepository,
    BusinessProfileResolver,
)
from research.business_profile_review import BusinessProfileReviewService
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


def _storage(tmp_path, db_name="research.db"):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / db_name),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def _promote(repository, record_type, record_id, *, references):
    spec = repository._TABLES[record_type]
    current = next(
        row
        for row in repository.list_records(record_type, limit=10000)
        if row[spec["pk"]] == record_id
    )
    BusinessProfileReviewService(repository).system_promote_record(
        record_type,
        record_id,
        field_family=f"test:{record_type}",
        policy_version="test_policy.v1",
        gate_manifest_hash=f"gates:{record_type}:{record_id}",
        reviewer_version="v1",
        expected_updated_at=current["updated_at"],
        evidence_references=references,
    )


def _approved_evidence(repository):
    payload = {
        "evidence_id": "evidence-2025-ar",
        "instrument_id": "601088.SH",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "evidence-hash",
        "extraction_method": "native_table",
        "confidence": 1.0,
        "review_status": "candidate",
        "metadata": {},
    }
    repository.upsert("evidence", payload)
    _promote(repository, "evidence", payload["evidence_id"], references=[])


def test_component_tables_and_published_reference_columns_exist(tmp_path):
    storage = _storage(tmp_path)
    with sqlite3.connect(storage.db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            ).fetchall()
        }
        exposure_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(company_commodity_exposures)"
            ).fetchall()
        }

    assert "company_commodity_exposure_facts" in tables
    assert "company_commodity_exposure_assumptions" in tables
    assert "company_commodity_exposures_legacy_compatibility" in tables
    assert {
        "fact_ids_json",
        "mapping_ids_json",
        "assumption_ids_json",
        "direction_rule_id",
        "build_policy_version",
        "build_policy_hash",
        "component_lineage_hash",
    }.issubset(exposure_columns)


def test_legacy_non_null_lag_column_migrates_without_rewriting_values(tmp_path):
    storage = _storage(tmp_path)
    with sqlite3.connect(storage.db_path) as conn:
        columns = {
            row[1]: row for row in conn.execute(
                "PRAGMA table_info(company_commodity_exposures)"
            ).fetchall()
        }

    assert columns["lag_days"][3] == 0


def test_copied_production_shaped_migration_preserves_approved_history(tmp_path):
    source_storage = _storage(tmp_path, "research_source.db")
    source_repository = BusinessProfileRepository(source_storage)
    _approved_evidence(source_repository)
    exposure = {
        "exposure_id": "legacy-approved-coal",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "scope_type": "company",
        "scope_id": "601088.SH",
        "commodity_id": "coal",
        "exposure_role": "revenue",
        "direction": "positive",
        "materiality": "high",
        "mapping_basis": "legacy_official_disclosure",
        "price_series_id": "CNF.JM.DCE.main",
        "lag_days": 0,
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "effective_from": "2026-03-28",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    source_repository.upsert("exposures", exposure)
    _promote(
        source_repository,
        "exposures",
        exposure["exposure_id"],
        references=["evidence-2025-ar"],
    )
    approved_before = source_repository.list_records("exposures")[0]
    resolver_before = BusinessProfileResolver(source_repository).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )["executable_exposure_mappings"]
    with sqlite3.connect(source_storage.db_path) as conn:
        conn.row_factory = sqlite3.Row
        audits_before = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM business_profile_review_audit "
                "ORDER BY reviewed_at, audit_id"
            ).fetchall()
        ]
        create_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'company_commodity_exposures'"
        ).fetchone()[0]
        legacy_sql = create_sql.replace(
            "lag_days INTEGER,",
            "lag_days INTEGER NOT NULL,",
        )
        assert legacy_sql != create_sql
        conn.execute(
            "DROP VIEW IF EXISTS company_commodity_exposures_legacy_compatibility"
        )
        conn.execute(
            "ALTER TABLE company_commodity_exposures "
            "RENAME TO company_commodity_exposures_current"
        )
        conn.execute(legacy_sql)
        columns = [
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(company_commodity_exposures_current)"
            ).fetchall()
        ]
        column_list = ", ".join(columns)
        conn.execute(
            f"INSERT INTO company_commodity_exposures ({column_list}) "
            f"SELECT {column_list} FROM company_commodity_exposures_current"
        )
        conn.execute("DROP TABLE company_commodity_exposures_current")
        conn.commit()
        legacy_columns = {
            row[1]: row
            for row in conn.execute(
                "PRAGMA table_info(company_commodity_exposures)"
            ).fetchall()
        }
        assert legacy_columns["lag_days"][3] == 1

    source_hash = hashlib.sha256(
        Path(source_storage.db_path).read_bytes()
    ).hexdigest()
    copied_path = tmp_path / "research_production_copy.db"
    shutil.copy2(source_storage.db_path, copied_path)
    copied_storage = _storage(tmp_path, copied_path.name)
    copied_repository = BusinessProfileRepository(copied_storage)
    approved_after = copied_repository.list_records("exposures")[0]
    resolver_after = BusinessProfileResolver(copied_repository).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )["executable_exposure_mappings"]
    with sqlite3.connect(copied_storage.db_path) as conn:
        conn.row_factory = sqlite3.Row
        audits_after = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM business_profile_review_audit "
                "ORDER BY reviewed_at, audit_id"
            ).fetchall()
        ]
        columns_after = {
            row[1]: row
            for row in conn.execute(
                "PRAGMA table_info(company_commodity_exposures)"
            ).fetchall()
        }

    assert columns_after["lag_days"][3] == 0
    assert approved_after == approved_before
    assert audits_after == audits_before
    assert resolver_after == resolver_before
    assert (
        hashlib.sha256(Path(source_storage.db_path).read_bytes()).hexdigest()
        == source_hash
    )


def test_exposure_facts_and_calibrated_assumptions_are_bitemporal(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    fact = {
        "fact_id": "fact-coal-sales-2025",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "activity_id": "activity-coal-sales",
        "segment_id": "coal",
        "exposure_fact_type": "sales_revenue",
        "object_raw": "thermal coal",
        "product_id": "coal.thermal_coal",
        "value_raw": 1000.0,
        "unit_raw": "CNY",
        "value_normalized": 1000.0,
        "unit_normalized": "CNY",
        "share": 0.8,
        "fact_scope": "segment",
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2025-01-01",
        "valid_to": "2026-01-01",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    repository.upsert("exposure_facts", fact)
    _promote(
        repository,
        "exposure_facts",
        fact["fact_id"],
        references=["evidence-2025-ar"],
    )
    assumption = {
        "assumption_id": "assumption-coal-lag-v1",
        "instrument_id": "601088.SH",
        "scope_type": "product",
        "scope_id": "coal.thermal_coal",
        "assumption_type": "lag_days",
        "assumption_value": 30.0,
        "unit": "day",
        "method": "calibrated_cross_correlation",
        "sample_start": "2021-01-01",
        "sample_end": "2025-12-31",
        "data_available_date": "2026-04-15",
        "confidence": 0.9,
        "review_status": "candidate",
        "effective_from": "2026-01-01",
        "knowledge_from": "2026-04-15",
        "version": 1,
        "metadata": {},
    }
    repository.upsert("exposure_assumptions", assumption)
    _promote(
        repository,
        "exposure_assumptions",
        assumption["assumption_id"],
        references=["calibration:coal-lag-2021-2025"],
    )

    assert repository.get_approved_as_of(
        "exposure_facts",
        instrument_id="601088.SH",
        cutoff="2026-03-30",
    )
    assert repository.get_approved_as_of(
        "exposure_assumptions",
        instrument_id="601088.SH",
        cutoff="2026-04-01",
    ) == []
    current = repository.get_approved_as_of(
        "exposure_assumptions",
        instrument_id="601088.SH",
        cutoff="2026-04-30",
    )
    assert current[0]["assumption_value"] == 30.0


def test_legacy_migration_decomposes_only_uniquely_proven_rows(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    legacy = {
        "exposure_id": "legacy-coal-exposure",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "scope_type": "company",
        "scope_id": "601088.SH",
        "commodity_id": "coal",
        "exposure_role": "revenue",
        "direction": "positive",
        "materiality": "high",
        "mapping_basis": "legacy_official_disclosure",
        "price_series_id": "CNF.JM.DCE.main",
        "lag_days": 0,
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "effective_from": "2026-03-28",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {
            "legacy_decomposition_proof": {
                "schema_version": "legacy_exposure_decomposition.v1",
                "exposure_fact_type": "sales_revenue",
                "object_raw": "thermal coal",
                "fact_scope": "company",
                "product_id": "coal.thermal_coal",
                "share": 0.8,
                "mapping_ids": ["mapping.coal.thermal.v1"],
                "direction_rule_id": "direction.revenue.v1",
                "build_policy_version": "publication.v1",
            }
        },
    }
    repository.upsert("exposures", legacy)
    _promote(
        repository,
        "exposures",
        legacy["exposure_id"],
        references=["evidence-2025-ar"],
    )
    incomplete = dict(legacy)
    incomplete["exposure_id"] = "legacy-incomplete"
    incomplete["commodity_id"] = "copper"
    incomplete["metadata"] = {}
    repository.upsert("exposures", incomplete)
    _promote(
        repository,
        "exposures",
        incomplete["exposure_id"],
        references=["evidence-2025-ar"],
    )

    result = BusinessProfileExposureComponentMigrator(repository).migrate()
    exposures = {
        row["exposure_id"]: row for row in repository.list_records("exposures")
    }

    assert result["componentized"] == 1
    assert result["legacy_compatible"] == 1
    successor = exposures["legacy-coal-exposure:componentized:v1"]
    assert successor["review_status"] == "approved"
    assert successor["fact_ids"] == ["legacy-coal-exposure:fact:v1"]
    assert successor["supersedes_exposure_id"] == "legacy-coal-exposure"
    assert exposures["legacy-coal-exposure"]["review_status"] == "approved"
    assert exposures["legacy-incomplete"]["component_lineage_hash"] is None

    replay = BusinessProfileExposureComponentMigrator(repository).migrate()
    assert replay["componentized"] == 0
    assert replay["already_componentized"] >= 1


def test_legacy_migration_never_promotes_an_unapproved_source(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    candidate = {
        "exposure_id": "legacy-candidate",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "scope_type": "company",
        "scope_id": "601088.SH",
        "commodity_id": "coal",
        "exposure_role": "revenue",
        "direction": "positive",
        "mapping_basis": "legacy_official_disclosure",
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "effective_from": "2026-03-28",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {
            "legacy_decomposition_proof": {
                "schema_version": "legacy_exposure_decomposition.v1",
                "exposure_fact_type": "sales_revenue",
                "object_raw": "thermal coal",
                "fact_scope": "company",
                "product_id": "coal.thermal_coal",
                "mapping_ids": ["mapping.coal.thermal.v1"],
                "direction_rule_id": "direction.revenue.v1",
                "build_policy_version": "publication.v1",
            }
        },
    }
    repository.upsert("exposures", candidate)

    result = BusinessProfileExposureComponentMigrator(repository).migrate()

    assert result["componentized"] == 0
    assert result["failures"] == [
        {
            "exposure_id": "legacy-candidate",
            "reason": "legacy_source_not_approved",
        }
    ]
    exposures = repository.list_records("exposures")
    assert [item["exposure_id"] for item in exposures] == ["legacy-candidate"]
    assert repository.list_records("exposure_facts") == []


def test_exposure_fact_batch_rolls_back_on_foreign_key_failure(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    base = {
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "activity_id": "activity-1",
        "segment_id": "coal",
        "exposure_fact_type": "sales_volume",
        "object_raw": "thermal coal",
        "product_id": "coal.thermal_coal",
        "value_raw": 100.0,
        "unit_raw": "tonne",
        "value_normalized": 100.0,
        "unit_normalized": "tonne",
        "fact_scope": "segment",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2025-01-01",
        "valid_to": "2026-01-01",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    with pytest.raises(sqlite3.IntegrityError):
        repository.upsert_many(
            "exposure_facts",
            [
                {
                    **base,
                    "fact_id": "fact-valid",
                    "evidence_id": "evidence-2025-ar",
                },
                {
                    **base,
                    "fact_id": "fact-invalid",
                    "activity_id": "activity-2",
                    "evidence_id": "missing-evidence",
                },
            ],
        )
    assert repository.list_records("exposure_facts") == []


def test_assumption_supersession_selects_version_by_knowledge_cutoff(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    base = {
        "instrument_id": "601088.SH",
        "scope_type": "product",
        "scope_id": "coal.thermal_coal",
        "assumption_type": "lag_days",
        "unit": "day",
        "method": "calibrated_cross_correlation",
        "sample_start": "2021-01-01",
        "sample_end": "2025-12-31",
        "confidence": 0.9,
        "review_status": "candidate",
        "effective_from": "2026-01-01",
        "metadata": {},
    }
    first = {
        **base,
        "assumption_id": "lag-v1",
        "assumption_value": 30.0,
        "data_available_date": "2026-04-01",
        "knowledge_from": "2026-04-01",
        "knowledge_to": "2026-05-01",
        "version": 1,
    }
    repository.upsert("exposure_assumptions", first)
    _promote(
        repository,
        "exposure_assumptions",
        "lag-v1",
        references=["calibration:v1"],
    )
    second = {
        **base,
        "assumption_id": "lag-v2",
        "assumption_value": 20.0,
        "data_available_date": "2026-05-01",
        "knowledge_from": "2026-05-01",
        "supersedes_assumption_id": "lag-v1",
        "version": 2,
    }
    repository.upsert("exposure_assumptions", second)
    _promote(
        repository,
        "exposure_assumptions",
        "lag-v2",
        references=["calibration:v2"],
    )

    april = repository.get_approved_as_of(
        "exposure_assumptions",
        instrument_id="601088.SH",
        cutoff="2026-04-15",
    )
    may = repository.get_approved_as_of(
        "exposure_assumptions",
        instrument_id="601088.SH",
        cutoff="2026-05-15",
    )
    assert [item["assumption_id"] for item in april] == ["lag-v1"]
    assert [item["assumption_id"] for item in may] == ["lag-v2"]


def test_resolver_does_not_invent_missing_company_direction_or_materiality(tmp_path):
    class _SeriesStorage:
        @staticmethod
        def get_series(series_id):
            return {"series_id": series_id, "active": True}

    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    exposure = {
        "exposure_id": "missing-direction",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "scope_type": "company",
        "scope_id": "601088.SH",
        "commodity_id": "coal",
        "exposure_role": "revenue",
        "direction": "",
        "materiality": None,
        "mapping_basis": "legacy_incomplete",
        "price_series_id": "CNF.JM.DCE.main",
        "lag_days": 0,
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "effective_from": "2026-03-28",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    repository.upsert("exposures", exposure)
    _promote(
        repository,
        "exposures",
        exposure["exposure_id"],
        references=["evidence-2025-ar"],
    )

    context = BusinessProfileResolver(
        repository,
        futures_storage=_SeriesStorage(),
    ).resolve("601088.SH", as_of_date="2026-04-30")

    assert context["executable_exposure_mappings"] == []
    assert "exposure_direction_missing:missing-direction" in context["readiness"][
        "input_gaps"
    ]
