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


def _repository(tmp_path):
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
    storage = ResearchStorageManager(config)
    storage.initialize()
    return BusinessProfileRepository(storage)


def _promote(repository, record_type, record_id, references):
    spec = repository._TABLES[record_type]
    current = next(
        row
        for row in repository.list_records(record_type)
        if row[spec["pk"]] == record_id
    )
    BusinessProfileReviewService(repository).system_promote_record(
        record_type,
        record_id,
        field_family=f"test:{record_type}",
        policy_version="test.v1",
        gate_manifest_hash=f"gates:{record_id}",
        reviewer_version="v1",
        expected_updated_at=current["updated_at"],
        evidence_references=references,
    )


def _evidence(repository):
    row = {
        "evidence_id": "evidence-2025-ar",
        "instrument_id": "601088.SH",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "evidence-hash",
        "extraction_method": "native_text",
        "confidence": 1.0,
        "review_status": "candidate",
        "metadata": {},
    }
    repository.upsert("evidence", row)
    _promote(repository, "evidence", row["evidence_id"], [])


def test_activity_and_relationship_storage_are_point_in_time_and_read_only(tmp_path):
    repository = _repository(tmp_path)
    _evidence(repository)
    activity = {
        "activity_id": "activity-coal-production",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "subject_scope": "issuer",
        "action": "produces",
        "object_type": "product",
        "object_raw": "动力煤",
        "object_id": "coal.thermal_coal",
        "segment_id": "coal",
        "evidence_id": "evidence-2025-ar",
        "run_id": "run-1",
        "data_available_date": "2026-03-28",
        "extraction_method": "semantic_verified",
        "confidence": 0.98,
        "review_status": "candidate",
        "valid_from": "2025-12-31",
        "valid_to": None,
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    repository.upsert("activities", activity)
    _promote(repository, "activities", activity["activity_id"], ["evidence-2025-ar"])
    relationship = {
        "relationship_id": "relationship-customer",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "relationship_type": "sells_to",
        "direction": "outbound",
        "counterparty_name_raw": "客户股份有限公司",
        "counterparty_name_normalized": "客户股份有限公司",
        "counterparty_entity_id": "entity-customer",
        "resolution_basis": "exact_legal_name",
        "anonymous": 0,
        "scope_type": "segment",
        "scope_id": "coal",
        "object_raw": "动力煤",
        "object_id": "coal.thermal_coal",
        "evidence_id": "evidence-2025-ar",
        "run_id": "run-1",
        "data_available_date": "2026-03-28",
        "confidence": 0.95,
        "review_status": "candidate",
        "valid_from": "2025-01-01",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    repository.upsert("relationships", relationship)
    _promote(
        repository,
        "relationships",
        relationship["relationship_id"],
        ["evidence-2025-ar"],
    )

    assert repository.get_approved_as_of(
        "activities", instrument_id="601088.SH", cutoff="2026-03-20"
    ) == []
    assert repository.get_approved_as_of(
        "activities", instrument_id="601088.SH", cutoff="2026-04-01"
    )[0]["activity_id"] == "activity-coal-production"
    assert repository.get_approved_as_of(
        "relationships", instrument_id="601088.SH", cutoff="2026-04-01"
    )[0]["counterparty_entity_id"] == "entity-customer"

    candidate_relationship = {
        **relationship,
        "relationship_id": "relationship-candidate",
        "counterparty_name_raw": "候选客户",
        "counterparty_name_normalized": "候选客户",
        "counterparty_entity_id": "entity-candidate",
        "review_status": "candidate",
    }
    repository.upsert("relationships", candidate_relationship)
    context = BusinessProfileResolver(repository).resolve(
        "601088.SH",
        as_of_date="2026-04-01",
        include_candidates=True,
    )
    history = repository.get_profile_history("601088.SH")

    assert context["company_specific_profile"]["activities"][0]["activity_id"] == (
        "activity-coal-production"
    )
    assert context["company_specific_profile"]["supply_chain_relationships"][0][
        "relationship_id"
    ] == "relationship-customer"
    assert context["candidate_facts"]["relationships"][0]["relationship_id"] == (
        "relationship-candidate"
    )
    assert len(history["relationships"]) == 2
