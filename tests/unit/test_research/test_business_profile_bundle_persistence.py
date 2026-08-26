import json
import sqlite3

import pytest

from research.business_profile_governance import BusinessProfileRepository
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
    return BusinessProfileRepository(storage), storage


def _run(run_id="run-1"):
    return {
        "run_id": run_id,
        "instrument_id": "601088.SH",
        "source_document_id": "annual-report-2025",
        "field_family": "atomic_activities",
        "bundle_hash": "bundle-hash",
        "fact_catalog_version": "business_profile_facts.2026.3",
        "product_catalog_version": "business_profile_products.2026.4",
        "metadata": {"document_hash": "document-hash"},
    }


def _evidence(evidence_id="evidence-1"):
    return {
        "evidence_id": evidence_id,
        "instrument_id": "601088.SH",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "evidence-hash",
        "extraction_method": "native_text",
        "parser_version": "business_profile_semantic_runtime.v6",
        "confidence": 1.0,
        "review_status": "candidate",
        "metadata": {
            "promotion_validation": {
                "catalog_versions": {
                    "product": "business_profile_products.2026.3",
                }
            }
        },
    }


def _activity(activity_id="activity-1", evidence_id="evidence-1", run_id="run-1"):
    return {
        "activity_id": activity_id,
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "subject_scope": "issuer",
        "action": "produces",
        "object_type": "product",
        "object_raw": "动力煤",
        "object_id": "coal.thermal_coal",
        "segment_id": "coal",
        "evidence_id": evidence_id,
        "run_id": run_id,
        "data_available_date": "2026-03-28",
        "extraction_method": "semantic_verified",
        "confidence": 0.98,
        "review_status": "candidate",
        "valid_from": "2025-12-31",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }


def _relationship(
    relationship_id="relationship-1",
    evidence_id="evidence-1",
    run_id="run-1",
):
    return {
        "relationship_id": relationship_id,
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
        "evidence_id": evidence_id,
        "run_id": run_id,
        "data_available_date": "2026-03-28",
        "confidence": 0.95,
        "review_status": "candidate",
        "valid_from": "2025-12-31",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }


def _operating_fact(record_id="fact-1", evidence_id="evidence-1"):
    return {
        "record_id": record_id,
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "segment_id": "coal",
        "fact_type": "production_volume",
        "project_id": None,
        "value_raw": 100.0,
        "unit_raw": "吨",
        "value_normalized": 100.0,
        "unit_normalized": "吨",
        "fact_scope": "issuer",
        "evidence_id": evidence_id,
        "data_available_date": "2026-03-28",
        "confidence": 0.95,
        "review_status": "candidate",
        "valid_from": "2025-01-01",
        "valid_to": "2025-12-31",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }


def _assert_empty(repository, storage):
    assert repository.list_records("evidence") == []
    assert repository.list_records("activities") == []
    assert repository.list_records("relationships") == []
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_semantic_runs"
            ).fetchone()[0]
            == 0
        )


def test_valid_document_field_family_bundle_commits_once(tmp_path):
    repository, storage = _repository(tmp_path)

    result = repository.persist_document_field_family_bundle(
        run=_run(),
        records_by_type={
            "evidence": [_evidence()],
            "activities": [_activity()],
            "relationships": [_relationship()],
        },
    )

    assert result == {
        "run_id": "run-1",
        "status": "completed",
        "evidence_count": 1,
        "fact_count": 0,
        "activity_count": 1,
        "relationship_count": 1,
    }
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        run = dict(
            conn.execute(
                "SELECT * FROM business_profile_semantic_runs WHERE run_id = 'run-1'"
            ).fetchone()
        )
    assert run["status"] == "completed"
    assert run["activity_count"] == 1


def test_replace_bundle_attaches_successor_and_preserves_prior_version(tmp_path):
    repository, _storage = _repository(tmp_path)
    repository.persist_document_field_family_bundle(
        run=_run(),
        records_by_type={"evidence": [_evidence()], "operating_facts": [_operating_fact()]},
    )
    _system_promote(repository, "evidence", "evidence-1")
    _system_promote(repository, "operating_facts", "fact-1")

    replacement = _operating_fact(record_id="fact-2")
    replacement["confidence"] = 0.97
    run = _run("run-replace")
    run["metadata"] = {"result_policy": "replace"}
    repository.persist_document_field_family_bundle(
        run=run,
        records_by_type={"operating_facts": [replacement]},
    )

    prior = repository.get_record("operating_facts", "fact-1")
    successor = repository.get_record("operating_facts", "fact-2")
    assert prior["review_status"] == "approved"
    assert successor["supersedes_record_id"] == "fact-1"
    assert successor["version"] == 2


def test_reuse_bundle_keeps_nonzero_report_flow_candidate(tmp_path):
    repository, _storage = _repository(tmp_path)
    positive = _operating_fact(record_id="fact-positive")
    zero = _operating_fact(record_id="fact-zero")
    zero["value_raw"] = 0.0
    zero["value_normalized"] = 0.0
    run = _run("run-reuse-conflict")
    run["metadata"] = {"result_policy": "reuse"}

    repository.persist_document_field_family_bundle(
        run=run,
        records_by_type={
            "evidence": [_evidence()],
            "operating_facts": [positive, zero],
        },
    )

    facts = repository.list_records("operating_facts")
    assert [item["record_id"] for item in facts] == ["fact-positive"]
    assert facts[0]["value_raw"] == 100.0
    with repository.storage.get_connection() as conn:
        metadata = json.loads(
            conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs "
                "WHERE run_id = ?",
                ("run-reuse-conflict",),
            ).fetchone()[0]
        )
    assert metadata["record_ids"]["operating_facts"] == ["fact-positive"]


def test_bundle_collapses_identical_primary_keys(tmp_path):
    repository, _storage_manager = _repository(tmp_path)

    result = repository.persist_document_field_family_bundle(
        run=_run(),
        records_by_type={
            "evidence": [_evidence(), _evidence()],
            "activities": [_activity(), _activity()],
        },
    )

    assert result["evidence_count"] == 1
    assert result["activity_count"] == 1
    assert len(repository.list_records("evidence")) == 1
    assert len(repository.list_records("activities")) == 1


def test_bundle_rejects_conflicting_duplicate_primary_keys(tmp_path):
    repository, storage = _repository(tmp_path)
    conflict = _activity()
    conflict["object_raw"] = "焦煤"

    with pytest.raises(ValueError, match="conflicting business profile primary key"):
        repository.persist_document_field_family_bundle(
            run=_run(),
            records_by_type={
                "evidence": [_evidence()],
                "activities": [_activity(), conflict],
            },
        )
    _assert_empty(repository, storage)


@pytest.mark.parametrize(
    "mutation,match",
    [
        (lambda run, rows: run.update(fact_catalog_version="stale"), "catalog"),
        (
            lambda run, rows: rows["evidence"][0].update(instrument_id="600000.SH"),
            "evidence instrument mismatch",
        ),
        (
            lambda run, rows: rows["activities"][0].update(
                valid_from="2026-01-01", valid_to="2025-01-01"
            ),
            "cannot be earlier",
        ),
    ],
)
def test_bundle_validation_failures_leave_no_partial_rows(
    tmp_path,
    mutation,
    match,
):
    repository, storage = _repository(tmp_path)
    run = _run()
    rows = {"evidence": [_evidence()], "activities": [_activity()]}
    mutation(run, rows)

    with pytest.raises(ValueError, match=match):
        repository.persist_document_field_family_bundle(
            run=run,
            records_by_type=rows,
        )
    _assert_empty(repository, storage)


def test_bundle_foreign_key_and_terminal_state_races_roll_back(tmp_path):
    repository, storage = _repository(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        repository.persist_document_field_family_bundle(
            run=_run(),
            records_by_type={
                "activities": [_activity(evidence_id="missing-evidence")],
            },
        )
    _assert_empty(repository, storage)

    repository.upsert("evidence", _evidence())
    candidate = repository.list_records("evidence")[0]
    from research.business_profile_review import BusinessProfileReviewService

    BusinessProfileReviewService(repository).system_promote_record(
        "evidence",
        "evidence-1",
        field_family="test",
        policy_version="test.v1",
        gate_manifest_hash="gates",
        reviewer_version="v1",
        expected_updated_at=candidate["updated_at"],
        evidence_references=[],
    )
    changed_evidence = _evidence()
    changed_evidence["document_hash"] = "changed-after-approval"
    with pytest.raises(ValueError, match="terminal-state race"):
        repository.persist_document_field_family_bundle(
            run=_run("run-2"),
            records_by_type={
                "evidence": [changed_evidence],
                "activities": [_activity(run_id="run-2")],
            },
        )
    assert repository.list_records("activities") == []


def _system_promote(repository, record_type, record_id):
    from research.business_profile_review import BusinessProfileReviewService

    candidate = repository.get_record(record_type, record_id)
    BusinessProfileReviewService(repository).system_promote_record(
        record_type,
        record_id,
        field_family="atomic_activities",
        policy_version="test.v1",
        gate_manifest_hash="gates",
        reviewer_version="v1",
        expected_updated_at=candidate["updated_at"],
        evidence_references=[],
    )


def test_terminal_bundle_replay_ignores_processing_identity_and_preserves_rows(
    tmp_path,
):
    repository, storage = _repository(tmp_path)
    repository.persist_document_field_family_bundle(
        run=_run(),
        records_by_type={
            "evidence": [_evidence()],
            "activities": [_activity()],
        },
    )
    _system_promote(repository, "evidence", "evidence-1")
    _system_promote(repository, "activities", "activity-1")
    approved_evidence = repository.get_record("evidence", "evidence-1")
    approved_activity = repository.get_record("activities", "activity-1")

    replay_evidence = _evidence()
    replay_evidence.update(
        parser_version="business_profile_semantic_runtime.v7",
        extraction_method="semantic_evidence_spans",
        confidence=0.99,
        metadata={
            "promotion_validation": {
                "catalog_versions": {
                    "product": "business_profile_products.2026.4",
                }
            }
        },
    )
    replay_activity = _activity(run_id="run-2")
    replay_activity.update(
        extraction_method="semantic_verified_v2",
        confidence=0.99,
        lineage_hash="new-runtime-lineage",
        metadata={"runtime_schema_version": "business_profile_semantic_runtime.v7"},
    )

    result = repository.persist_document_field_family_bundle(
        run=_run("run-2"),
        records_by_type={
            "evidence": [replay_evidence],
            "activities": [replay_activity],
        },
    )

    assert result["status"] == "completed"
    assert repository.get_record("evidence", "evidence-1") == approved_evidence
    assert repository.get_record("activities", "activity-1") == approved_activity
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        assert conn.execute(
            "SELECT status FROM business_profile_semantic_runs WHERE run_id = 'run-2'"
        ).fetchone()[0] == "completed"


@pytest.mark.parametrize(
    ("record_type", "mutation"),
    [
        ("evidence", lambda row: row.update(evidence_text_hash="changed-evidence")),
        ("evidence", lambda row: row.update(page_number=99)),
        ("evidence", lambda row: row.update(section_path="changed-section")),
        ("activities", lambda row: row.update(value=999.0)),
    ],
)
def test_terminal_bundle_replay_rejects_source_or_business_changes(
    tmp_path,
    record_type,
    mutation,
):
    repository, storage = _repository(tmp_path)
    repository.persist_document_field_family_bundle(
        run=_run(),
        records_by_type={
            "evidence": [_evidence()],
            "activities": [_activity()],
        },
    )
    _system_promote(repository, "evidence", "evidence-1")
    _system_promote(repository, "activities", "activity-1")
    replay_evidence = _evidence()
    replay_activity = _activity(run_id="run-2")
    mutation(replay_evidence if record_type == "evidence" else replay_activity)

    with pytest.raises(ValueError, match="terminal-state race"):
        repository.persist_document_field_family_bundle(
            run=_run("run-2"),
            records_by_type={
                "evidence": [replay_evidence],
                "activities": [replay_activity],
            },
        )

    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        assert conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_runs WHERE run_id = 'run-2'"
        ).fetchone()[0] == 0
