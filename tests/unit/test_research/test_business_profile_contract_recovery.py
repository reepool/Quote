import gzip
import json

import pytest

from research.business_profile_contract_recovery import (
    BusinessProfileContractRecovery,
    obsolete_contract_reasons,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_semantic_extraction import (
    STRUCTURED_EXTRACTION_SCHEMA_VERSION,
)
from research.business_profile_semantic_runtime import RUNTIME_SCHEMA_VERSION
from tests.unit.test_research.test_business_profile_exposure_components import (
    _storage,
)


NOW = "2026-08-09T12:00:00+08:00"


def _add_evidence(repository, *, evidence_id="evidence-1", document_id="document-1"):
    repository.upsert(
        "evidence",
        {
            "evidence_id": evidence_id,
            "instrument_id": "600000.SH",
            "source_document_id": document_id,
            "source_tier": "official_filing",
            "document_hash": "d" * 64,
            "data_available_date": "2026-03-20",
            "availability_quality": "actual",
            "evidence_text_hash": "e" * 64,
            "extraction_method": "native_table",
            "confidence": 1.0,
            "review_status": "candidate",
        },
    )


def _add_segment(repository, *, record_id, review_status="candidate"):
    repository.upsert(
        "segments",
        {
            "record_id": record_id,
            "instrument_id": "600000.SH",
            "report_period": "2025-12-31",
            "segment_id": record_id,
            "segment_name_raw": "主营业务",
            "segment_type": "product",
            "source_document_id": "document-1",
            "evidence_id": "evidence-1",
            "data_available_date": "2026-03-20",
            "confidence": 1.0,
            "review_status": "candidate",
            "metadata": {
                "semantic_synthesis": True,
                "structured_schema_version": "obsolete.v1",
                "source_label_raw": "",
                "semantic_summary_zh": "English only",
                "numeric_reconciliation_executed": False,
                "numeric_reconciliation_valid": False,
            },
        },
    )
    if review_status == "approved":
        evidence = repository.get_record("evidence", "evidence-1")
        if evidence["review_status"] != "approved":
            BusinessProfileReviewService(repository).system_promote_record(
                "evidence",
                "evidence-1",
                field_family="test:evidence",
                policy_version="test_policy.v1",
                gate_manifest_hash="evidence-gates",
                reviewer_version="v1",
                expected_updated_at=evidence["updated_at"],
                evidence_references=[],
            )
        segment = repository.get_record("segments", record_id)
        BusinessProfileReviewService(repository).system_promote_record(
            "segments",
            record_id,
            field_family="test:segments",
            policy_version="test_policy.v1",
            gate_manifest_hash=f"segment-gates:{record_id}",
            reviewer_version="v1",
            expected_updated_at=segment["updated_at"],
            evidence_references=["evidence-1"],
        )


def _mark_segment_current(storage, record_id):
    metadata = {
        "semantic_synthesis": True,
        "structured_schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
        "source_label_raw": "主营业务",
        "semantic_summary_zh": "公司主营业务保持稳定",
        "numeric_reconciliation_executed": True,
        "numeric_reconciliation_valid": True,
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
    }
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE company_business_segments SET metadata_json = ? "
            "WHERE record_id = ?",
            (json.dumps(metadata, ensure_ascii=False), record_id),
        )
        conn.commit()


def _add_concentration_fact(repository, *, record_id="concentration-1"):
    repository.upsert(
        "operating_facts",
        {
            "record_id": record_id,
            "instrument_id": "600000.SH",
            "report_period": "2025-12-31",
            "fact_type": "customer_concentration_share",
            "value_raw": 0.595,
            "unit_raw": "fraction",
            "value_normalized": 0.595,
            "unit_normalized": "fraction",
            "fact_scope": "top_five_customers",
            "evidence_id": "evidence-1",
            "data_available_date": "2026-03-20",
            "confidence": 1.0,
            "review_status": "candidate",
            "metadata": {
                "semantic_synthesis": True,
                "numeric_reconciliation_executed": True,
                "numeric_reconciliation_valid": True,
            },
        },
    )


def _add_work(storage, *, work_id="work-1", status="completed"):
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_work_items ("
            "work_id, frontier_id, instrument_id, source, announcement_id, "
            "report_period, document_type, policy, processing_identity_hash, "
            "stage, status, checkpoint_path, metadata_json, created_at, updated_at"
            ") VALUES (?, 'frontier-1', '600000.SH', 'cninfo', 'annual-2025', "
            "'2025-12-31', 'annual_report', 'latest_annual_only', 'identity-1', "
            "'publish', ?, 'checkpoint.json', ?, ?, ?)",
            (
                work_id,
                status,
                json.dumps({"source_document_id": "document-1"}),
                NOW,
                NOW,
            ),
        )
        conn.commit()


def _add_unit_exception(storage, tmp_path, *, exception_id="exception-1"):
    selected_path = tmp_path / f"{exception_id}.json.gz"
    selected = {
        "bundle": {"bundle_id": "bundle-1", "document_hash": "d" * 64},
        "sections": [
            {
                "section_id": "section-1",
                "page_number": 10,
                "section_hash": "s" * 64,
            }
        ],
    }
    selected_path.write_bytes(gzip.compress(json.dumps(selected).encode("utf-8")))
    semantic_result = {
        "rows": [
            {
                "source_label_raw": "产能",
                "source_value": "10",
                "source_unit_raw": "未知单位",
            }
        ]
    }
    metadata = {
        "source_document_id": "document-1",
        "selected_artifact_path": str(selected_path),
        "diagnostics": {
            "semantic_audit": {
                "response_hash": "r" * 64,
                "profile": "semantic_extraction",
                "actual_model": "model-test",
                "usage": {"input_tokens": 100, "output_tokens": 20},
                "diagnostics": {
                    "semantic_result": semantic_result,
                    "resolved_evidence": [
                        {
                            "evidence_spans": [
                                {"evidence_span_id": "span-1"}
                            ]
                        }
                    ],
                },
            }
        },
    }
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_exceptions ("
            "exception_id, target_type, target_id, instrument_id, field_family, "
            "tier, reason_codes_json, gate_signature, gate_manifest_hash, "
            "metadata_json, created_at, updated_at"
            ") VALUES (?, 'document_field_family', 'work-1', '600000.SH', "
            "'tabular_operating_facts', 'machine_rework', ?, ?, 'manifest-1', ?, ?, ?)",
            (
                exception_id,
                json.dumps(["unit_normalization_failed", "conversion_retry"]),
                f"gate-{exception_id}",
                json.dumps(metadata),
                NOW,
                NOW,
            ),
        )
        conn.commit()


def test_contract_recovery_rejects_inconsistent_candidate_once(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_evidence(repository)
    _add_segment(repository, record_id="segment-candidate")
    _add_work(storage)

    first = BusinessProfileContractRecovery(repository).run()
    second = BusinessProfileContractRecovery(repository).run()

    assert first["rejected"] == 1
    assert first["requeued"] == 1
    assert second["rejected"] == 0
    assert repository.get_record("segments", "segment-candidate")["review_status"] == "rejected"
    with storage.get_connection() as conn:
        audit_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_review_audit "
            "WHERE record_id = 'segment-candidate'"
        ).fetchone()[0]
        work = conn.execute(
            "SELECT stage, status FROM business_profile_work_items WHERE work_id = 'work-1'"
        ).fetchone()
    assert audit_count == 1
    assert tuple(work) == ("semantic", "retry_due")


def test_relationship_concentration_fact_is_not_a_structured_contract_row():
    record = {
        "metadata": {
            "semantic_synthesis": True,
            "numeric_reconciliation_executed": True,
            "numeric_reconciliation_valid": True,
        }
    }

    assert obsolete_contract_reasons("operating_facts", record) == ()


def test_contract_recovery_preserves_human_held_record(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_evidence(repository)
    _add_segment(repository, record_id="segment-human-held")
    candidate = repository.get_record("segments", "segment-human-held")
    BusinessProfileReviewService(repository).review_record(
        "segments",
        "segment-human-held",
        decision="held",
        reviewer="analyst@example",
        reason="needs human assessment",
        expected_review_status="candidate",
        expected_updated_at=candidate["updated_at"],
        evidence_references=["evidence-1"],
    )

    result = BusinessProfileContractRecovery(repository).run()

    assert result["human_held"] == 1
    assert repository.get_record("segments", "segment-human-held")["review_status"] == "held"


def test_structured_operating_fact_still_uses_structured_contract():
    record = {
        "metadata": {
            "semantic_synthesis": True,
            "structured_schema_version": "obsolete.v1",
            "source_label_raw": "",
            "numeric_reconciliation_executed": True,
            "numeric_reconciliation_valid": True,
        }
    }

    assert obsolete_contract_reasons("operating_facts", record) == (
        "structured_schema_obsolete",
        "source_label_contract_obsolete",
    )


def test_contract_recovery_reopens_automation_rejected_concentration_fact(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_evidence(repository)
    _add_concentration_fact(repository)
    _add_work(storage)
    review = BusinessProfileReviewService(repository)
    fact = repository.get_record("operating_facts", "concentration-1")
    review.review_record(
        "operating_facts",
        "concentration-1",
        decision="rejected",
        reviewer="automation:business_profile_contract_recovery.v1",
        reason="obsolete production contract: structured_schema_obsolete",
        expected_review_status="candidate",
        expected_updated_at=fact["updated_at"],
    )

    result = BusinessProfileContractRecovery(repository).run()

    assert result["rejected"] == 0
    assert result["reopened"] == 1
    assert result["requeued"] == 1
    assert (
        repository.get_record("operating_facts", "concentration-1")[
            "review_status"
        ]
        == "candidate"
    )
    with storage.get_connection() as conn:
        work = conn.execute(
            "SELECT stage, status, lease_owner, lease_expires_at "
            "FROM business_profile_work_items WHERE work_id = 'work-1'"
        ).fetchone()
    assert tuple(work) == ("semantic", "retry_due", None, None)


def test_contract_recovery_preserves_approved_history_and_deduplicates_blocker(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_evidence(repository)
    _add_segment(repository, record_id="segment-approved", review_status="approved")

    recovery = BusinessProfileContractRecovery(repository)
    recovery.run()
    recovery.run()

    assert repository.get_record("segments", "segment-approved")["review_status"] == "approved"
    with storage.get_connection() as conn:
        blockers = conn.execute(
            "SELECT COUNT(*) FROM business_profile_readiness_blockers "
            "WHERE blocker_type = 'approved_history_conflict' "
            "AND target_id = 'segment-approved'"
        ).fetchone()[0]
    assert blockers == 1


def test_contract_recovery_limit_does_not_starve_obsolete_rows_behind_current_rows(
    tmp_path,
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_evidence(repository)
    _add_segment(repository, record_id="segment-current")
    _mark_segment_current(storage, "segment-current")
    _add_segment(repository, record_id="segment-obsolete")

    result = BusinessProfileContractRecovery(repository).run(limit=1)

    assert result["rejected"] == 1
    assert repository.get_record("segments", "segment-current")["review_status"] == "candidate"
    assert repository.get_record("segments", "segment-obsolete")["review_status"] == "rejected"


def test_contract_recovery_approved_cursor_skips_existing_blockers(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_evidence(repository)
    _add_segment(repository, record_id="segment-approved-1", review_status="approved")
    _add_segment(repository, record_id="segment-approved-2", review_status="approved")
    recovery = BusinessProfileContractRecovery(repository)

    first = recovery.run(limit=1)
    second = recovery.run(limit=1)

    assert first["approved_history_blockers"] == 1
    assert second["approved_history_blockers"] == 1
    with storage.get_connection() as conn:
        blocker_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_readiness_blockers "
            "WHERE blocker_type = 'approved_history_conflict'"
        ).fetchone()[0]
    assert blocker_count == 2


def test_unit_recovery_is_idempotent_and_preserves_exception_audit(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_work(storage, status="terminal_failure")
    _add_unit_exception(storage, tmp_path)

    recovery = BusinessProfileContractRecovery(repository)
    first = recovery.recover_unit_blocked(limit=20)
    second = recovery.recover_unit_blocked(limit=20)

    assert first == {"attempted": 1, "recovered": 1, "skipped": 0}
    assert second == {"attempted": 0, "recovered": 0, "skipped": 0}
    with storage.get_connection() as conn:
        artifact_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_artifacts"
        ).fetchone()[0]
        event_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_artifact_events"
        ).fetchone()[0]
        exception = conn.execute(
            "SELECT status, metadata_json FROM business_profile_exceptions "
            "WHERE exception_id = 'exception-1'"
        ).fetchone()
        work = conn.execute(
            "SELECT stage, status, attempt_count FROM business_profile_work_items "
            "WHERE work_id = 'work-1'"
        ).fetchone()
    metadata = json.loads(exception["metadata_json"])
    assert artifact_count == 1
    assert event_count == 2
    assert exception["status"] == "resolved"
    assert "diagnostics" in metadata
    assert metadata["contract_recovery"]["recovered_artifact_id"]
    assert tuple(work) == ("semantic", "retry_due", 0)


def test_unit_recovery_resumes_after_interruption_before_exception_close(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _add_work(storage, status="terminal_failure")
    _add_unit_exception(storage, tmp_path)
    interrupted = BusinessProfileContractRecovery(repository)
    monkeypatch.setattr(
        interrupted,
        "_resolve_exception",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("interrupted")),
    )

    with pytest.raises(RuntimeError, match="interrupted"):
        interrupted.recover_unit_blocked(limit=20)
    with storage.get_connection() as conn:
        status_after_interrupt = conn.execute(
            "SELECT status FROM business_profile_exceptions "
            "WHERE exception_id = 'exception-1'"
        ).fetchone()[0]
        work_after_interrupt = conn.execute(
            "SELECT stage, status FROM business_profile_work_items "
            "WHERE work_id = 'work-1'"
        ).fetchone()
    assert status_after_interrupt == "open"
    assert tuple(work_after_interrupt) == ("semantic", "retry_due")

    resumed = BusinessProfileContractRecovery(repository).recover_unit_blocked(limit=20)

    assert resumed == {"attempted": 1, "recovered": 1, "skipped": 0}
    with storage.get_connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_artifacts"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_artifact_events"
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT status FROM business_profile_exceptions "
            "WHERE exception_id = 'exception-1'"
        ).fetchone()[0] == "resolved"
