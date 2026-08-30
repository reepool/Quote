import json
from pathlib import Path

from research.business_profile_semantic_repair import BusinessProfileSemanticRepairService
from research.business_profile_semantic_artifacts import (
    BusinessProfileSemanticArtifactRepository,
    SemanticArtifactIdentity,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_activity_production import BusinessProfileActivityProducer
from research.providers.base import CompanyProfileSnapshot, ShareholderSnapshot
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _storage(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"), shadow_mode=True,
            attach_quotes_db=False, quotes_db_path=str(tmp_path / "quotes.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def _insert_work_item(
    storage,
    *,
    work_id,
    instrument_id,
    status,
    checkpoint_path,
    processing_identity,
):
    now = "2026-08-30T12:00:00+08:00"
    metadata = {
        "schema_version": "business_profile_work_item.v1",
        "processing_identity": processing_identity,
    }
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_work_items ("
            "work_id, frontier_id, instrument_id, source, announcement_id, "
            "report_period, document_type, policy, processing_identity_hash, "
            "stage, status, checkpoint_path, metadata_json, created_at, updated_at"
            ") VALUES (?, ?, ?, 'cninfo', ?, '2025-12-31', 'annual_report', "
            "'expanded', ?, 'acquire', ?, ?, ?, ?, ?)",
            (
                work_id,
                f"frontier-{work_id}",
                instrument_id,
                f"announcement-{work_id}",
                f"identity-{work_id}",
                status,
                str(checkpoint_path),
                json.dumps(metadata),
                now,
                now,
            ),
        )
        conn.commit()


def test_repair_deletes_failed_semantic_receipt_and_converges(tmp_path):
    storage = _storage(tmp_path)
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    identity = SemanticArtifactIdentity(
        instrument_id="600000.SH",
        source_document_id="annual-report-2025",
        document_hash="a" * 64,
        report_period="2025-12-31",
        field_family="atomic_activities",
        evidence_scope_hash="b" * 64,
        input_hash="c" * 64,
        prompt_version="prompt.v1",
        schema_version="schema.v1",
    )
    artifact = artifacts.receive(
        identity,
        response={"activities": []},
        response_hash="",
        evidence_ids=[],
    )
    artifacts.mark(
        artifact["artifact_id"],
        "conversion_pending",
        reason_code="unit_normalization_failed",
    )
    service = BusinessProfileSemanticRepairService(storage)

    audit = service.run(instrument_ids=["600000.SH"])
    assert audit["issue_counts"]["failed_semantic_artifact"] == 1
    assert audit["write_count"] == 0

    applied = service.run(instrument_ids=["600000.SH"], apply=True)
    assert applied["change_counts"]["changed"] == 1
    assert artifacts.find_replay(identity) is None
    repeated = service.run(instrument_ids=["600000.SH"], apply=True)
    assert repeated["change_counts"]["unchanged"] == 1


def test_all_scope_includes_instruments_with_only_failed_lifecycle_rows(tmp_path):
    storage = _storage(tmp_path)
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    identity = SemanticArtifactIdentity(
        instrument_id="699999.SH",
        source_document_id="annual-report-2025",
        document_hash="a" * 64,
        report_period="2025-12-31",
        field_family="atomic_activities",
        evidence_scope_hash="b" * 64,
        input_hash="c" * 64,
        prompt_version="prompt.v1",
        schema_version="schema.v1",
    )
    artifact = artifacts.receive(
        identity,
        response={"activities": []},
        response_hash="",
        evidence_ids=[],
    )
    artifacts.mark(artifact["artifact_id"], "rejected", reason_code="schema_failure")

    audit = BusinessProfileSemanticRepairService(storage).run(all_scope=True)

    assert [item["instrument_id"] for item in audit["instruments"]] == ["699999.SH"]
    assert audit["issue_counts"]["failed_semantic_artifact"] == 1


def test_repair_removes_obsolete_work_and_orphan_checkpoints(tmp_path):
    storage = _storage(tmp_path)
    checkpoint_root = tmp_path / "checkpoints"
    checkpoint_root.mkdir()
    legacy_identity = {
        "rollout_phase": "structured_shadow",
        "field_families": ["structured_segments", "tabular_operating_facts"],
    }
    current_identity = {
        "rollout_phase": "semantic_complete_targeted",
        "field_families": ["atomic_activities", "named_relationships"],
    }
    paths = {
        "legacy": checkpoint_root / "bp-work-legacy.json",
        "failed": checkpoint_root / "bp-work-failed.json",
        "superseded": checkpoint_root / "bp-work-superseded.json",
        "current": checkpoint_root / "bp-work-current.json",
        "orphan": checkpoint_root / "bp-work-orphan.json",
    }
    for path in paths.values():
        path.write_text("{}", encoding="utf-8")
    _insert_work_item(
        storage,
        work_id="bp-work-legacy",
        instrument_id="600001.SH",
        status="pending",
        checkpoint_path=paths["legacy"],
        processing_identity=legacy_identity,
    )
    _insert_work_item(
        storage,
        work_id="bp-work-failed",
        instrument_id="600002.SH",
        status="terminal_failure",
        checkpoint_path=paths["failed"],
        processing_identity=current_identity,
    )
    _insert_work_item(
        storage,
        work_id="bp-work-superseded",
        instrument_id="600003.SH",
        status="superseded",
        checkpoint_path=paths["superseded"],
        processing_identity=current_identity,
    )
    _insert_work_item(
        storage,
        work_id="bp-work-current",
        instrument_id="600004.SH",
        status="retry_due",
        checkpoint_path=paths["current"],
        processing_identity=current_identity,
    )
    control = checkpoint_root / "control" / "backfill_progress.json"
    control.parent.mkdir()
    control.write_text("{}", encoding="utf-8")
    service = BusinessProfileSemanticRepairService(
        storage, checkpoint_root=checkpoint_root
    )

    audit = service.run(all_scope=True)

    assert audit["execution_state"]["work_items"]["delete_count"] == 3
    assert audit["execution_state"]["work_items"]["work_ids_count"] == 3
    assert "_delete_work_ids" not in audit["execution_state"]["work_items"]
    assert audit["issue_counts"] == {
        "obsolete_work_items": 1,
        "orphan_checkpoint_files": 1,
    }
    applied = service.run(all_scope=True, apply=True)
    assert applied["change_counts"]["failed"] == 0
    execution_change = next(
        item for item in applied["changes"] if item.get("instrument_id") is None
    )
    assert execution_change["affected_ids_count"] == 3
    assert execution_change["checkpoint_paths_count"] == 4
    with storage.get_connection() as conn:
        remaining = conn.execute(
            "SELECT work_id FROM business_profile_work_items ORDER BY work_id"
        ).fetchall()
    assert [row[0] for row in remaining] == ["bp-work-current"]
    assert paths["current"].exists()
    assert control.exists()
    assert all(not paths[name].exists() for name in ("legacy", "failed", "superseded", "orphan"))
    repeated = service.run(all_scope=True)
    assert repeated["issue_counts"] == {}


def test_cleanup_only_does_not_apply_unrelated_business_repairs(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600009.SH",
        symbol="600009",
        exchange="SSE",
        source="local",
        holder_count=1,
        holder_count_report_date="20251231",
        snapshot_json={"coverage_scope": ["incorrect_scope"]},
    ))
    service = BusinessProfileSemanticRepairService(storage)

    audit = service.run(all_scope=True, cleanup_only=True)
    applied = service.run(all_scope=True, apply=True, cleanup_only=True)

    assert audit["cleanup_only"] is True
    assert "shareholder_scope_mismatch" not in audit["issue_counts"]
    assert applied["change_counts"]["changed"] == 0
    snapshot = storage.get_shareholder_snapshot("600009.SH", include_snapshot=True)
    assert snapshot["snapshot"]["coverage_scope"] == ["incorrect_scope"]


def test_repair_refuses_checkpoint_outside_owned_root(tmp_path):
    storage = _storage(tmp_path)
    checkpoint_root = tmp_path / "checkpoints"
    checkpoint_root.mkdir()
    outside = tmp_path / "bp-work-outside.json"
    outside.write_text("{}", encoding="utf-8")
    _insert_work_item(
        storage,
        work_id="bp-work-outside",
        instrument_id="600005.SH",
        status="terminal_failure",
        checkpoint_path=outside,
        processing_identity={"field_families": ["atomic_activities"]},
    )
    service = BusinessProfileSemanticRepairService(
        storage, checkpoint_root=checkpoint_root
    )

    audit = service.run(all_scope=True)
    assert audit["issue_counts"]["unsafe_checkpoint_paths"] == 1
    applied = service.run(all_scope=True, apply=True)

    assert outside.exists()
    assert applied["change_counts"]["failed"] == 1
    with storage.get_connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM business_profile_work_items"
        ).fetchone()[0] == 0


def test_repair_deletes_retired_shadow_artifact_and_run(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    identity = SemanticArtifactIdentity(
        instrument_id="600006.SH",
        source_document_id="annual-report-2025",
        document_hash="a" * 64,
        report_period="2025-12-31",
        field_family="structured_segments",
        evidence_scope_hash="b" * 64,
        input_hash="c" * 64,
        prompt_version="business_profile_structured_extraction.v4",
        schema_version="business_profile_structured_extraction.v4",
    )
    artifact = artifacts.receive(
        identity,
        response={"segments": []},
        response_hash="",
        evidence_ids=[],
    )
    artifacts.mark(artifact["artifact_id"], "converted")
    now = "2026-08-30T12:00:00+08:00"
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_semantic_runs ("
            "run_id, instrument_id, source_document_id, field_family, status, "
            "bundle_hash, metadata_json, started_at, completed_at, created_at, updated_at"
            ") VALUES ('legacy-run', '600006.SH', 'annual-report-2025', "
            "'structured_segments', 'completed', 'bundle', ?, ?, ?, ?, ?)",
            (
                json.dumps({"semantic_artifact_id": artifact["artifact_id"]}),
                now,
                now,
                now,
                now,
            ),
        )
        conn.commit()
    repository.upsert("evidence", {
        "evidence_id": "legacy-evidence",
        "instrument_id": "600006.SH",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "text-hash",
        "extraction_method": "native_text",
        "confidence": 1.0,
        "review_status": "candidate",
    })
    repository.upsert("exposure_facts", {
        "fact_id": "legacy-candidate-exposure-fact",
        "instrument_id": "600006.SH",
        "report_period": "2025-12-31",
        "activity_id": "legacy-activity",
        "exposure_fact_type": "sales_value",
        "object_raw": "legacy product",
        "value_raw": 1.0,
        "unit_raw": "CNY",
        "value_normalized": 1.0,
        "unit_normalized": "CNY",
        "fact_scope": "issuer",
        "evidence_id": "legacy-evidence",
        "run_id": "legacy-run",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    })

    applied = BusinessProfileSemanticRepairService(storage).run(
        all_scope=True, apply=True
    )

    assert applied["issue_counts"]["legacy_shadow_semantic_artifact"] == 1
    assert artifacts.find_replay(identity) is None
    assert repository.get_record(
        "exposure_facts", "legacy-candidate-exposure-fact"
    ) is None
    with storage.get_connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_runs"
        ).fetchone()[0] == 0


def test_repair_audit_is_read_only_and_apply_requires_explicit_scope(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600000.SH", symbol="600000", exchange="SSE",
        holder_count=10, holder_count_report_date="20260331", control_owner_name="第一大股东",
        source="efinance", snapshot_json={
            "coverage_scope": ["holder_count", "reference_only_ownership_clues"],
            "holder_count": {"value": 10, "report_date": "20260331"},
            "top_holders": [{"rank": 1, "holder_name": "第一大股东", "report_date": "20260331"}],
            "ownership_clues": {"control_owner_name": "第一大股东"},
        },
    ))
    service = BusinessProfileSemanticRepairService(storage)
    before = storage.get_shareholder_snapshot("600000.SH")

    audit = service.run(instrument_ids=["600000.SH"])

    assert audit["mode"] == "audit"
    assert audit["write_count"] == 0
    assert audit["network_access"] is False and audit["llm_access"] is False
    assert storage.get_shareholder_snapshot("600000.SH") == before
    assert {item["code"] for item in audit["instruments"][0]["issues"]} >= {
        "shareholder_noncanonical_report_date", "shareholder_inferred_controller"
    }

    try:
        service.run(apply=True)
    except ValueError as exc:
        assert "instrument_ids or all_scope" in str(exc)
    else:
        raise AssertionError("apply without an explicit scope must fail")


def test_repair_keeps_official_controller_without_control_history(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600001.SH", symbol="600001", exchange="SSE",
        control_owner_name="控制人甲", source="cninfo", snapshot_json={
            "coverage_scope": ["reference_only_ownership_clues"],
            "ownership_clues": {"control_owner_name": "控制人甲"},
            "scope_raw_provenance": {
                "reference_only_ownership_clues": {
                    "source": "cninfo", "source_mode": "direct", "payload": {},
                }
            },
        },
    ))

    audit = BusinessProfileSemanticRepairService(storage).run(
        instrument_ids=["600001.SH"]
    )

    assert "shareholder_inferred_controller" not in {
        item["code"] for item in audit["instruments"][0]["issues"]
    }


def test_repair_holds_ambiguous_controller_provenance_without_writing(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600002.SH", symbol="600002", exchange="SSE",
        control_owner_name="控制人甲", source="unknown", snapshot_json={
            "coverage_scope": ["reference_only_ownership_clues"],
            "ownership_clues": {"control_owner_name": "控制人甲"},
        },
    ))
    service = BusinessProfileSemanticRepairService(storage)

    audit = service.run(instrument_ids=["600002.SH"])
    applied = service.run(instrument_ids=["600002.SH"], apply=True)

    assert {item["code"] for item in audit["instruments"][0]["issues"]} == {
        "shareholder_controller_provenance_ambiguous"
    }
    assert applied["change_counts"]["held"] == 1


def test_repair_apply_normalizes_dates_preserves_scope_provenance_and_is_idempotent(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600000.SH", symbol="600000", exchange="SSE",
        holder_count=10, holder_count_report_date="20260331", source="composite",
        source_mode="per_scope", snapshot_json={
            "coverage_scope": ["holder_count"],
            "holder_count": {"value": 10, "report_date": "20260331"},
            "scope_raw_provenance": {
                "holder_count": {
                    "source": "cninfo", "source_mode": "direct",
                    "payload": {"raw_report_date": "20260331", "value": 10},
                }
            },
        },
    ))
    service = BusinessProfileSemanticRepairService(storage)

    applied = service.run(instrument_ids=["600000.SH"], apply=True)
    repaired = storage.get_shareholder_snapshot("600000.SH", include_snapshot=True)
    repeated = service.run(instrument_ids=["600000.SH"], apply=True)

    assert applied["change_counts"]["changed"] == 1
    assert repaired["holder_count_report_date"] == "2026-03-31"
    assert repaired["snapshot"]["holder_count"]["report_date"] == "2026-03-31"
    provenance = repaired["snapshot"]["scope_raw_provenance"]["holder_count"]
    assert provenance["source"] == "cninfo"
    assert provenance["payload"] == {"raw_report_date": "20260331", "value": 10}
    assert repeated["change_counts"]["unchanged"] == 1


def test_local_shareholder_projection_uses_snapshot_and_control_history_only(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600000.SH", symbol="600000", exchange="SSE",
        holder_count=12, holder_count_report_date="2026-03-31", top_holders_report_date="2026-03-31",
        source="cninfo", source_mode="direct", snapshot_json={
            "top_holders": [{"rank": 1, "holder_name": "股东甲", "report_date": "2026-03-31"}],
        },
    ))
    storage.upsert_shareholder_control_changes([{
        "instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE",
        "change_date": "2026-02-01", "actual_controller_name": "控制人甲",
        "control_type": "实际控制", "source": "cninfo", "source_mode": "direct",
    }])

    projection = storage.get_shareholder_profile_projection(
        "600000.SH", knowledge_cutoff="2099-01-01"
    )

    assert projection["status"] == "success"
    assert projection["top_holders"][0]["holder_name"] == "股东甲"
    assert projection["actual_controller"]["actual_controller_name"] == "控制人甲"


def _approved_relationship_with_short_name(storage, *, human_approval=False):
    repository = BusinessProfileRepository(storage)
    evidence = {
        "evidence_id": "repair-evidence",
        "instrument_id": "600010.SH",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "text-hash",
        "extraction_method": "native_text",
        "confidence": 1.0,
        "review_status": "candidate",
    }
    repository.upsert("evidence", evidence)
    current_evidence = repository.get_record("evidence", evidence["evidence_id"])
    review = BusinessProfileReviewService(repository)
    review.system_promote_record(
        "evidence",
        evidence["evidence_id"],
        field_family="test:evidence",
        policy_version="test.v1",
        gate_manifest_hash="test-evidence",
        reviewer_version="v1",
        expected_updated_at=current_evidence["updated_at"],
    )
    relationship = {
        "relationship_id": "repair-relationship",
        "instrument_id": "600010.SH",
        "report_period": "2025-12-31",
        "relationship_type": "buys_from",
        "direction": "inbound",
        "counterparty_name_raw": "供应商甲",
        "counterparty_entity_id": "600011.SH",
        "resolution_basis": "exact_legal_name",
        "anonymous": 0,
        "scope_type": "company",
        "scope_id": "600010.SH",
        "object_raw": "原材料",
        "evidence_id": evidence["evidence_id"],
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2025-01-01",
        "metadata": {
            "identity_status": "resolved_entity",
            "resolution_status": "resolved_entity",
            "entity_resolution": {"basis": "exact_legal_name"},
        },
    }
    repository.upsert("relationships", relationship)
    current_relationship = repository.get_record(
        "relationships", relationship["relationship_id"]
    )
    if human_approval:
        review.review_record(
            "relationships",
            relationship["relationship_id"],
            decision="approved",
            reviewer="analyst@example",
            reason="fixture relationship approval",
            expected_review_status="candidate",
            expected_updated_at=current_relationship["updated_at"],
            evidence_references=[evidence["evidence_id"]],
        )
    else:
        review.system_promote_record(
            "relationships",
            relationship["relationship_id"],
            field_family="test:relationships",
            policy_version="test.v1",
            gate_manifest_hash="test-relationship",
            reviewer_version="v1",
            expected_updated_at=current_relationship["updated_at"],
            evidence_references=[evidence["evidence_id"]],
        )
    storage.upsert_company_profile(
        CompanyProfileSnapshot(
            instrument_id="600011.SH",
            symbol="600011",
            company_name="供应商甲",
            short_name="供应商甲",
            exchange="SSE",
            source="baostock",
            source_mode="direct",
        )
    )
    return repository, review


def test_repair_holds_machine_approved_short_name_resolution_and_preserves_history(tmp_path):
    storage = _storage(tmp_path)
    repository, review = _approved_relationship_with_short_name(storage)
    service = BusinessProfileSemanticRepairService(storage)

    audit = service.run(instrument_ids=["600010.SH"])
    assert audit["change_counts"]["would_change"] == 1
    assert audit["write_count"] == 0
    assert audit["before_current_projections"]["600010.SH"]["relationships"] == [
        "repair-relationship"
    ]

    applied = service.run(instrument_ids=["600010.SH"], apply=True)
    repaired = repository.get_record("relationships", "repair-relationship")
    assert repaired["review_status"] == "held"
    assert applied["change_counts"]["changed"] == 1
    assert applied["after_current_projections"]["600010.SH"]["relationships"] == []
    history = review.list_review_audit(
        record_type="relationships", record_id="repair-relationship"
    )
    assert len(history) == 2
    assert history[0]["new_status"] == "held"

    repeated = service.run(instrument_ids=["600010.SH"], apply=True)
    assert repeated["change_counts"]["unchanged"] == 1


def test_repair_does_not_override_human_reviewed_short_name_resolution(tmp_path):
    storage = _storage(tmp_path)
    repository, _ = _approved_relationship_with_short_name(storage, human_approval=True)

    applied = BusinessProfileSemanticRepairService(storage).run(
        instrument_ids=["600010.SH"], apply=True
    )

    assert repository.get_record("relationships", "repair-relationship")["review_status"] == "approved"
    assert applied["change_counts"]["held"] == 1
    assert applied["change_counts"]["changed"] == 0


def test_repair_holds_legacy_internal_inventory_storage_role(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = {
        "evidence_id": "inventory-repair-evidence",
        "instrument_id": "000858.SZ",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "inventory-document-hash",
        "data_available_date": "2026-03-30",
        "availability_quality": "actual",
        "evidence_text_hash": "inventory-text-hash",
        "extraction_method": "native_text",
        "confidence": 1.0,
        "review_status": "candidate",
    }
    repository.upsert("evidence", evidence)
    review = BusinessProfileReviewService(repository)
    evidence_current = repository.get_record("evidence", evidence["evidence_id"])
    review.system_promote_record(
        "evidence", evidence["evidence_id"], field_family="test:evidence",
        policy_version="test.v1", gate_manifest_hash="inventory-evidence",
        reviewer_version="v1", expected_updated_at=evidence_current["updated_at"],
    )
    producer = BusinessProfileActivityProducer(repository)
    activity = producer.build_activity_candidate(
        {
            "instrument_id": "000858.SZ", "report_period": "2025-12-31",
            "subject_scope": "issuer", "action": "stores",
            "object_type": "inventory", "object_raw": "成品酒",
            "segment_id": "白酒", "confidence": 1.0,
        }, evidence_id=evidence["evidence_id"], run_id="repair-run",
        data_available_date="2026-03-30", extraction_method="semantic_verified",
    )
    repository.upsert("activities", activity)
    activity_current = repository.get_record("activities", activity["activity_id"])
    review.system_promote_record(
        "activities", activity["activity_id"], field_family="test:activities",
        policy_version="test.v1", gate_manifest_hash="inventory-activity",
        reviewer_version="v1", expected_updated_at=activity_current["updated_at"],
        evidence_references=[evidence["evidence_id"]],
    )
    role = {
        "record_id": "legacy-inventory-storage-role",
        "instrument_id": "000858.SZ", "report_period": "2025-12-31",
        "segment_id": "白酒", "role": "storage_provider",
        "mapping_basis": "legacy_stores_rule", "evidence_id": evidence["evidence_id"],
        "data_available_date": "2026-03-30", "confidence": 1.0,
        "review_status": "candidate", "valid_from": "2025-01-01",
        "business_regime_id": None, "knowledge_from": "2026-03-30",
        "metadata": {
            "supporting_activity_ids": [activity["activity_id"]],
            "role_business_identity": {
                "instrument_id": "000858.SZ", "segment_id": "白酒",
                "role": "storage_provider", "report_period": "2025-12-31",
                "business_regime_id": None,
            },
        },
    }
    repository.upsert("value_chain_roles", role)
    role_current = repository.get_record("value_chain_roles", role["record_id"])
    review.system_promote_record(
        "value_chain_roles", role["record_id"], field_family="test:roles",
        policy_version="test.v1", gate_manifest_hash="inventory-role",
        reviewer_version="v1", expected_updated_at=role_current["updated_at"],
        evidence_references=[evidence["evidence_id"]],
    )

    service = BusinessProfileSemanticRepairService(storage)
    audit = service.run(instrument_ids=["000858.SZ"])
    assert audit["change_counts"]["would_change"] == 1
    applied = service.run(instrument_ids=["000858.SZ"], apply=True)
    repaired = repository.get_record("value_chain_roles", role["record_id"])
    assert repaired["review_status"] == "held"
    assert applied["change_counts"]["changed"] == 1
    assert repository.get_record("evidence", evidence["evidence_id"])["review_status"] == "approved"
    assert service.run(instrument_ids=["000858.SZ"], apply=True)["change_counts"]["unchanged"] == 1
    assert service.run(instrument_ids=["000858.SZ"])["issue_counts"] == {}


def test_repair_consolidates_machine_approved_duplicate_roles(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert(
        "evidence",
        {
            "evidence_id": "role-dedup-evidence",
            "instrument_id": "300750.SZ",
            "source_document_id": "annual-report-2025",
            "source_tier": "official_filing",
            "document_hash": "role-dedup-document",
            "data_available_date": "2026-03-30",
            "availability_quality": "actual",
            "evidence_text_hash": "role-dedup-text",
            "extraction_method": "native_text",
            "confidence": 1.0,
            "review_status": "candidate",
        },
    )
    for record_id, activity_id in (
        ("legacy-producer-role-a", "activity-a"),
        ("legacy-producer-role-b", "activity-b"),
    ):
        repository.upsert(
            "value_chain_roles",
            {
                "record_id": record_id,
                "instrument_id": "300750.SZ",
                "report_period": "2025-12-31",
                "segment_id": None,
                "role": "producer",
                "mapping_basis": "approved_atomic_activity_rule",
                "evidence_id": "role-dedup-evidence",
                "data_available_date": "2026-03-30",
                "confidence": 1.0,
                "review_status": "candidate",
                "valid_from": "2025-12-31",
                "metadata": {
                    "role_rule_version": "business_profile_activity_role_rules.v1",
                    "supporting_activity_ids": [activity_id],
                },
            },
        )
    # Production-shaped legacy state: both rows were machine-approved before
    # role business identity aggregation was introduced.
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE company_value_chain_roles SET review_status = 'approved' "
            "WHERE instrument_id = '300750.SZ'"
        )
        conn.commit()

    service = BusinessProfileSemanticRepairService(storage)
    applied = service.run(instrument_ids=["300750.SZ"], apply=True)

    rows = repository.list_records(
        "value_chain_roles", instrument_id="300750.SZ", limit=10
    )
    assert {row["review_status"] for row in rows} == {"approved", "held"}
    assert applied["change_counts"] == {
        "would_change": 0,
        "changed": 1,
        "unchanged": 0,
        "held": 0,
        "failed": 0,
    }
    assert applied["changes"][0]["reason"] == "duplicate_machine_roles_consolidated"
    assert service.run(instrument_ids=["300750.SZ"])["issue_counts"] == {}


def test_repair_replays_distinct_candidate_contract_occurrences(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    base = {
        "instrument_id": "601012.SH", "report_period": "2025-12-31",
        "segment_id": "segment-polysilicon", "fact_type": "purchase_amount",
        "unit_raw": "亿元", "value_normalized": 0, "unit_normalized": "CNY",
        "fact_scope": "多晶硅料:采购金额#legacy-broad", "equity_basis": "source_reported_unknown",
        "data_available_date": "2026-03-30", "confidence": 1.0,
        "review_status": "candidate", "valid_from": "2025-01-01",
        "knowledge_from": "2026-03-30",
    }
    for evidence_id in ("evidence-contract-1", "evidence-contract-2"):
        repository.upsert(
            "evidence",
            {
                "evidence_id": evidence_id,
                "instrument_id": "601012.SH",
                "source_document_id": "annual-report-2025",
                "source_tier": "official_filing",
                "document_hash": "contract-document-hash",
                "data_available_date": "2026-03-30",
                "availability_quality": "actual",
                "evidence_text_hash": evidence_id + "-text",
                "extraction_method": "native_text",
                "confidence": 1.0,
                "review_status": "candidate",
            },
        )
    for record_id, value, evidence_id, quote in (
        ("legacy-contract-1", 4.18, "evidence-contract-1", "合同一 67.46 4.18亿元"),
        ("legacy-contract-2", 0, "evidence-contract-2", "合同二 1.25 0亿元"),
    ):
        repository.upsert(
            "operating_facts",
            {
                **base,
                "record_id": record_id,
                "value_raw": value,
                "evidence_id": evidence_id,
                "metadata": {"source_row_key": "legacy-broad", "exact_evidence": {"quote": quote}},
            },
        )
    service = BusinessProfileSemanticRepairService(storage)
    audit = service.run(instrument_ids=["601012.SH"])
    issue = next(item for item in audit["instruments"][0]["issues"] if item["code"] == "operating_fact_occurrence_conflict")
    assert issue["details"]["reconstructable"] is True
    applied = service.run(instrument_ids=["601012.SH"], apply=True)
    assert applied["change_counts"]["changed"] == 2
    rows = repository.list_records("operating_facts", instrument_id="601012.SH")
    assert {row["review_status"] for row in rows} == {"held", "candidate"}
    assert len([row for row in rows if row["review_status"] == "candidate"]) == 2
