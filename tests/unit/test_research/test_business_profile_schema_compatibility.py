from research.business_profile_governance import BusinessProfileRepository
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


def _storage(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
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
    return ResearchStorageManager(config)


def _candidate_evidence():
    return {
        "evidence_id": "evidence-schema-test",
        "instrument_id": "601088.SH",
        "source_document_id": "report-schema-test",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "text-hash",
        "extraction_method": "native_table",
        "confidence": 1.0,
        "review_status": "candidate",
        "metadata": {},
    }


def test_schema_inventory_matches_repository_contract(tmp_path):
    storage = _storage(tmp_path)
    storage.initialize()

    inventory = BusinessProfileRepository(storage).schema_inventory()

    assert inventory["status"] == "current"
    assert all(
        not item["missing_columns"] for item in inventory["tables"].values()
    )
    segment_table = inventory["tables"]["segments"]
    assert segment_table["status"] == "current"


def test_explicit_migration_is_idempotent_and_preserves_record_identity(tmp_path):
    storage = _storage(tmp_path)
    storage.initialize()
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _candidate_evidence())
    before = repository.list_records("evidence")[0]

    storage.initialize()
    after = BusinessProfileRepository(storage).list_records("evidence")[0]

    assert after == before
    assert after["review_status"] == "candidate"
    assert after["created_at"] == before["created_at"]
    assert after["updated_at"] == before["updated_at"]


def test_schema_inventory_reports_legacy_database_without_writing(tmp_path):
    storage = _storage(tmp_path)
    database_path = tmp_path / "research.db"
    with sqlite3.connect(database_path) as conn:
        conn.execute("CREATE TABLE legacy_marker (id INTEGER PRIMARY KEY)")
        conn.commit()

    inventory = BusinessProfileRepository(storage).schema_inventory()

    assert inventory["status"] == "migration_required"
    assert all(
        item["status"] == "migration_required"
        for item in inventory["tables"].values()
    )
    with sqlite3.connect(database_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert tables == {"legacy_marker"}
import sqlite3
