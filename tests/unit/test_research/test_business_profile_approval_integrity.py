import sqlite3
from contextlib import contextmanager

import pytest

from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_review import BusinessProfileReviewService
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


def _repository(tmp_path):
    research_db = tmp_path / "research.db"
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(research_db),
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
    return BusinessProfileRepository(storage), research_db


def _candidate_evidence(evidence_id="evidence-2025-ar"):
    return {
        "evidence_id": evidence_id,
        "instrument_id": "601088.SH",
        "source_document_id": f"cninfo-ann-{evidence_id}",
        "source_tier": "official_filing",
        "document_hash": "doc-hash",
        "report_period": "2025-12-31",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "text-hash",
        "extraction_method": "native_pdf_table",
        "confidence": 1.0,
        "review_status": "candidate",
    }


@pytest.mark.parametrize("status", ["held", "approved", "rejected", "superseded"])
def test_repository_rejects_direct_terminal_or_held_insert(tmp_path, status):
    repository, _ = _repository(tmp_path)
    payload = _candidate_evidence()
    payload["review_status"] = status

    with pytest.raises(ValueError, match="must be inserted as candidate"):
        repository.upsert("evidence", payload)

    assert repository.list_records("evidence") == []


def test_system_promotion_reuses_optimistic_transition_and_immutable_audit(tmp_path):
    repository, research_db = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    candidate = repository.list_records("evidence")[0]

    audit = BusinessProfileReviewService(repository).system_promote_record(
        "evidence",
        candidate["evidence_id"],
        field_family="structured_segments",
        policy_version="business_profile_auto_promotion.2026.1",
        gate_manifest_hash="gate-hash",
        reviewer_version="v1",
        expected_updated_at=candidate["updated_at"],
        evidence_references=["document:doc-hash:page:31"],
    )

    approved = repository.list_records("evidence")[0]
    assert approved["review_status"] == "approved"
    assert approved["reviewed_by"] == "system:business_profile_auto_promotion.v1"
    assert audit["reviewer"] == "system:business_profile_auto_promotion.v1"
    assert audit["metadata"]["system_promotion"]["gate_manifest_hash"] == "gate-hash"

    with sqlite3.connect(research_db) as conn:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="business_profile_review_audit is immutable",
        ):
            conn.execute(
                "UPDATE business_profile_review_audit SET reason = 'changed' "
                "WHERE audit_id = ?",
                (audit["audit_id"],),
            )


def test_human_hold_blocks_system_promotion_but_can_be_resolved_by_human(tmp_path):
    repository, _ = _repository(tmp_path)
    review = BusinessProfileReviewService(repository)
    repository.upsert("evidence", _candidate_evidence())
    candidate = repository.list_records("evidence")[0]
    hold = review.review_record(
        "evidence",
        candidate["evidence_id"],
        decision="held",
        reviewer="analyst@example",
        reason="issuer scope requires human judgment",
        expected_review_status="candidate",
        expected_updated_at=candidate["updated_at"],
    )
    held = repository.list_records("evidence")[0]
    assert held["review_status"] == "held"

    with pytest.raises(ValueError, match="prior human decision blocks automatic promotion"):
        review.system_promote_record(
            "evidence",
            candidate["evidence_id"],
            field_family="atomic_activities",
            policy_version="business_profile_auto_promotion.2026.1",
            gate_manifest_hash="gate-hash",
            reviewer_version="v1",
            expected_updated_at=held["updated_at"],
        )

    approved = review.review_record(
        "evidence",
        candidate["evidence_id"],
        decision="approved",
        reviewer="senior-analyst@example",
        reason="scope confirmed against consolidated filing",
        expected_review_status="held",
        expected_updated_at=held["updated_at"],
    )
    assert hold["new_status"] == "held"
    assert approved["prior_status"] == "held"
    assert repository.list_records("evidence")[0]["review_status"] == "approved"


class _CountingStorage:
    def __init__(self, storage):
        self._storage = storage
        self.connection_count = 0

    def _apply_pragmas(self, conn):
        self._storage._apply_pragmas(conn)

    @contextmanager
    def get_connection(self):
        self.connection_count += 1
        with self._storage.get_connection() as conn:
            yield conn


def test_bulk_upsert_validates_all_records_before_opening_transaction(tmp_path):
    repository, _ = _repository(tmp_path)
    counting_storage = _CountingStorage(repository.storage)
    bulk_repository = BusinessProfileRepository(counting_storage)
    invalid = _candidate_evidence("evidence-invalid")
    invalid.pop("instrument_id")

    with pytest.raises(ValueError, match="instrument_id is required"):
        bulk_repository.upsert_many(
            "evidence",
            [_candidate_evidence("evidence-valid"), invalid],
        )

    assert counting_storage.connection_count == 0
    assert repository.list_records("evidence") == []


def test_bulk_upsert_uses_one_connection_and_one_atomic_batch(tmp_path):
    repository, _ = _repository(tmp_path)
    counting_storage = _CountingStorage(repository.storage)
    bulk_repository = BusinessProfileRepository(counting_storage)

    assert bulk_repository.upsert_many(
        "evidence",
        [_candidate_evidence(f"evidence-{index}") for index in range(3)],
    ) == 3

    assert counting_storage.connection_count == 1
    assert len(repository.list_records("evidence")) == 3


def test_bulk_upsert_rolls_back_when_any_absent_row_requests_terminal_status(tmp_path):
    repository, _ = _repository(tmp_path)
    invalid = _candidate_evidence("evidence-approved")
    invalid["review_status"] = "approved"

    with pytest.raises(ValueError, match="must be inserted as candidate"):
        repository.upsert_many(
            "evidence",
            [_candidate_evidence("evidence-candidate"), invalid],
        )

    assert repository.list_records("evidence") == []


def test_sql_as_of_is_correct_beyond_prior_5000_row_history_cap(tmp_path):
    repository, research_db = _repository(tmp_path)
    review = BusinessProfileReviewService(repository)
    repository.upsert("evidence", _candidate_evidence())
    evidence = repository.list_records("evidence")[0]
    review.system_promote_record(
        "evidence",
        evidence["evidence_id"],
        field_family="structured_segments",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-gates",
        reviewer_version="v1",
        expected_updated_at=evidence["updated_at"],
    )
    records = [
        {
            "record_id": f"segment-{index:05d}",
            "instrument_id": "601088.SH",
            "report_period": "2025-06-30",
            "segment_id": f"segment-{index:05d}",
            "segment_name_raw": f"segment {index}",
            "segment_type": "product",
            "consolidation_scope": "consolidated",
            "evidence_id": evidence["evidence_id"],
            "data_available_date": "2025-08-30",
            "confidence": 1.0,
            "review_status": "candidate",
            "valid_from": "2025-01-01",
            "valid_to": "2025-06-30",
        }
        for index in reversed(range(5001))
    ]
    records.extend(
        [
            {
                **records[-1],
                "record_id": "segment-00000-annual-correction",
                "report_period": "2025-12-31",
                "data_available_date": "2026-03-31",
                "valid_to": "2025-12-31",
                "supersedes_record_id": "segment-00000",
                "version": 2,
            },
            {
                **records[-1],
                "record_id": "segment-future-known",
                "segment_id": "segment-future-known",
                "data_available_date": "2027-01-01",
            },
        ]
    )
    repository.upsert_many("segments", records[:5000])
    repository.upsert_many("segments", records[5000:])
    with sqlite3.connect(research_db) as conn:
        conn.execute("UPDATE company_business_segments SET review_status = 'approved'")
        conn.commit()

    selected = repository.get_approved_as_of(
        "segments",
        instrument_id="601088.SH",
        cutoff="2026-04-30",
    )

    assert len(selected) == 5001
    segment_zero = next(item for item in selected if item["segment_id"] == "segment-00000")
    assert segment_zero["record_id"] == "segment-00000-annual-correction"
    assert all(item["record_id"] != "segment-future-known" for item in selected)


def test_diagnostic_cursor_paginates_equal_timestamps_without_loss(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert_many(
        "evidence",
        [_candidate_evidence(f"evidence-{index}") for index in range(5)],
    )
    seen = []
    cursor = None
    while True:
        page = repository.list_records_page(
            "evidence",
            page_size=2,
            cursor=cursor,
        )
        seen.extend(item["evidence_id"] for item in page["records"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert len(seen) == 5
    assert len(set(seen)) == 5


def test_temporal_validation_rejects_invalid_interval_and_overlapping_state(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    evidence = repository.list_records("evidence")[0]
    BusinessProfileReviewService(repository).system_promote_record(
        "evidence",
        evidence["evidence_id"],
        field_family="test:evidence",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-evidence-gates",
        reviewer_version="v1",
        expected_updated_at=evidence["updated_at"],
        evidence_references=[],
    )
    invalid_interval = {
        "record_id": "segment-invalid-interval",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "segment_id": "coal",
        "segment_name_raw": "coal",
        "segment_type": "product",
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "knowledge_from": "2026-03-28",
        "knowledge_to": "2026-03-27",
    }
    with pytest.raises(ValueError, match="knowledge_to cannot be earlier"):
        repository.upsert("segments", invalid_interval)

    base_role = {
        "record_id": "role-v1",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "segment_id": "coal",
        "role": "processor",
        "mapping_basis": "activity_rule.v1",
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2026-01-01",
        "knowledge_from": "2026-03-28",
    }
    repository.upsert("value_chain_roles", base_role)
    current = repository.list_records("value_chain_roles")[0]
    BusinessProfileReviewService(repository).system_promote_record(
        "value_chain_roles",
        current["record_id"],
        field_family="test:value_chain_roles",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-role-gates",
        reviewer_version="v1",
        expected_updated_at=current["updated_at"],
        evidence_references=["evidence-2025-ar"],
    )
    with pytest.raises(ValueError, match="business profile temporal conflict"):
        repository.upsert(
            "value_chain_roles",
            {**base_role, "record_id": "role-v2", "version": 2},
        )


def test_promotion_rechecks_temporal_conflicts_and_allows_explicit_successor(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    evidence = repository.list_records("evidence")[0]
    review = BusinessProfileReviewService(repository)
    review.system_promote_record(
        "evidence",
        evidence["evidence_id"],
        field_family="test:evidence",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-evidence-gates",
        reviewer_version="v1",
        expected_updated_at=evidence["updated_at"],
    )
    base = {
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "segment_id": "coal",
        "role": "processor",
        "mapping_basis": "activity_rule.v1",
        "evidence_id": evidence["evidence_id"],
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2026-01-01",
        "knowledge_from": "2026-03-28",
    }
    repository.upsert_many(
        "value_chain_roles",
        [
            {**base, "record_id": "role-v1", "version": 1},
            {**base, "record_id": "role-conflict", "version": 2},
        ],
    )
    roles = {item["record_id"]: item for item in repository.list_records("value_chain_roles")}
    review.system_promote_record(
        "value_chain_roles",
        "role-v1",
        field_family="test:value_chain_roles",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-role-gates",
        reviewer_version="v1",
        expected_updated_at=roles["role-v1"]["updated_at"],
        evidence_references=[evidence["evidence_id"]],
    )
    with pytest.raises(ValueError, match="business profile temporal conflict"):
        review.system_promote_record(
            "value_chain_roles",
            "role-conflict",
            field_family="test:value_chain_roles",
            policy_version="test_policy.v1",
            gate_manifest_hash="test-role-gates",
            reviewer_version="v1",
            expected_updated_at=roles["role-conflict"]["updated_at"],
            evidence_references=[evidence["evidence_id"]],
        )

    repository.upsert(
        "value_chain_roles",
        {
            **base,
            "record_id": "role-v2",
            "supersedes_record_id": "role-v1",
            "version": 2,
        },
    )
    successor = next(
        item
        for item in repository.list_records("value_chain_roles")
        if item["record_id"] == "role-v2"
    )
    review.system_promote_record(
        "value_chain_roles",
        "role-v2",
        field_family="test:value_chain_roles",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-role-gates-v2",
        reviewer_version="v1",
        expected_updated_at=successor["updated_at"],
        evidence_references=[evidence["evidence_id"]],
    )

    selected = repository.get_approved_as_of(
        "value_chain_roles",
        instrument_id="601088.SH",
        cutoff="2026-04-30",
    )
    assert [item["record_id"] for item in selected] == ["role-v2"]
