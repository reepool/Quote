from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest

from research.announcement_assets import (
    CLASSIFICATION_VOCABULARY_VERSION,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AnnualReportCandidate,
    AnnualReportClassification,
    AnnualReportClassifier,
    AnnualReportVariant,
    AssetAvailability,
    AssetRequestStatus,
    BatchOutcome,
    ContentAddressedBlobStore,
    DocumentFamily,
    EffectiveDecisionState,
    EligibilityPolicy,
    EnsureDisposition,
    EnsureRequest,
    ExpectedPeriodCoverage,
    IdempotencyConflictError,
    IntegrityStatus,
    MountIdentity,
    OfficialAttachmentVersion,
    OfficialDocumentBlob,
    OperationStage,
    OperationStatus,
    derive_fiscal_year_search_bounds,
    effective_snapshot,
    normalize_annual_report_variant,
    normalize_document_family,
    select_effective_candidate,
)
from research.announcement_assets.classifier import (
    SAME_SOURCE_EQUIVALENT_TIE_BREAK_POLICY_VERSION,
    refine_classification_from_pdf,
)
from research.announcement_assets.schema import OWNED_TABLES, SCHEMA_VERSION
from research.announcement_assets.service import _candidate_from_row
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)

PDF_BYTES = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n"

CANONICAL_SCHEMA_FIELD_MATRIX = {
    "official_announcements": {
        "announcement_id",
        "schema_version",
        "source",
        "source_announcement_id",
        "title",
        "instrument_id",
        "exchange",
        "source_category",
        "published_at",
        "published_at_raw",
        "published_at_precision",
        "raw_payload_hash",
        "first_observed_at",
        "last_observed_at",
        "status",
        "provider_diagnostics_json",
        "metadata_json",
        "created_at",
        "updated_at",
    },
    "official_announcement_attachments": {
        "attachment_id",
        "schema_version",
        "announcement_id",
        "attachment_identity",
        "source_attachment_id",
        "source_url",
        "normalized_source_url",
        "name",
        "media_type",
        "content_length_hint",
        "first_observed_at",
        "last_observed_at",
        "metadata_json",
        "created_at",
        "updated_at",
    },
    "official_document_blobs": {
        "content_hash",
        "schema_version",
        "content_length",
        "canonical_path",
        "signature_status",
        "integrity_status",
        "first_available_at",
        "last_verified_at",
        "backup_status",
        "backup_verified_at",
        "acquisition_origin",
        "adopted_from_path",
        "verification_evidence_json",
        "backup_evidence_json",
        "created_at",
        "updated_at",
    },
    "official_attachment_versions": {
        "version_id",
        "schema_version",
        "attachment_id",
        "observation_key",
        "content_hash",
        "final_url",
        "retrieval_status",
        "integrity_status",
        "attempt",
        "max_attempts",
        "next_retry_at",
        "error_code",
        "observed_at",
        "first_observed_at",
        "last_observed_at",
        "version_available_at",
        "available_time_source",
        "available_time_precision",
        "response_evidence_json",
        "content_length_observed",
        "content_hash_observed",
        "lease_owner",
        "lease_generation",
        "temporary_path",
        "temporary_bytes",
        "quarantine_path",
        "visibility_state",
        "metadata_json",
        "created_at",
        "updated_at",
    },
    "official_asset_acquisition_leases": {
        "attachment_id",
        "lease_owner",
        "lease_generation",
        "lease_expires_at",
        "heartbeat_at",
        "attempt",
        "created_at",
        "updated_at",
    },
    "effective_annual_reports": {
        "asset_id",
        "schema_version",
        "instrument_id",
        "fiscal_year",
        "report_period",
        "announcement_id",
        "attachment_id",
        "version_id",
        "content_hash",
        "source",
        "source_announcement_id",
        "published_at",
        "document_family",
        "variant",
        "is_full_report",
        "classifier_version",
        "decision_state",
        "availability",
        "predecessor_asset_id",
        "pending_candidate_id",
        "activated_at",
        "last_checked_at",
        "decision_reasons_json",
        "decision_evidence_json",
        "equivalent_source_filings_json",
        "canonical_projection_policy_version",
        "evidence_set_hash",
        "visibility_state",
        "created_at",
        "updated_at",
    },
    "official_asset_adoption_promotion_gates": {
        "gate_id",
        "schema_version",
        "asset_id",
        "inventory_fingerprint",
        "config_fingerprint",
        "content_hash",
        "content_length",
        "canonical_path",
        "mount_filesystem_key",
        "custody_state",
        "status",
        "reconciled_at",
        "expires_at",
        "consumed_at",
        "invalidated_at",
        "invalidation_reason",
        "evidence_json",
        "created_at",
        "updated_at",
    },
    "official_annual_report_decisions": {
        "decision_sequence",
        "decision_id",
        "schema_version",
        "instrument_id",
        "fiscal_year",
        "decision_kind",
        "predecessor_asset_id",
        "predecessor_source",
        "predecessor_source_announcement_id",
        "predecessor_announcement_id",
        "predecessor_attachment_id",
        "predecessor_version_id",
        "predecessor_content_hash",
        "replacement_asset_id",
        "replacement_source",
        "replacement_source_announcement_id",
        "replacement_announcement_id",
        "replacement_attachment_id",
        "replacement_version_id",
        "replacement_content_hash",
        "decision_state",
        "classifier_version",
        "decision_policy_version",
        "decision_reasons_json",
        "decision_evidence_json",
        "activated_at",
        "outbox_event_key",
        "created_at",
    },
    "official_asset_operations": {
        "operation_id",
        "schema_version",
        "operation_type",
        "idempotency_key",
        "scope_json",
        "bounds_json",
        "policy_version",
        "config_version",
        "owner",
        "status",
        "stage",
        "stage_schema_version",
        "outcome",
        "attempt",
        "max_attempts",
        "resume_generation",
        "next_retry_at",
        "lease_owner",
        "lease_expires_at",
        "heartbeat_at",
        "progress_json",
        "checkpoint_json",
        "result_asset_id",
        "result_origin",
        "reason_code",
        "diagnostics_json",
        "created_at",
        "started_at",
        "finished_at",
        "updated_at",
    },
    "official_asset_operation_subscriptions": {
        "asset_request_id",
        "schema_version",
        "operation_id",
        "principal",
        "consumer",
        "idempotency_key",
        "request_fingerprint",
        "authorized_projection_json",
        "status",
        "consumer_continuation_id",
        "metadata_json",
        "created_at",
        "updated_at",
        "cancelled_at",
        "expires_at",
        "expired_at",
        "tombstone_until",
        "retention_policy_version",
    },
    "official_asset_consumer_requests": {
        "consumer_request_id",
        "schema_version",
        "principal",
        "consumer",
        "idempotency_key",
        "request_fingerprint",
        "processing_fingerprint",
        "selector_json",
        "asset_request_id",
        "asset_id",
        "processing_id",
        "status",
        "result_state",
        "result_identity",
        "resolved_source",
        "resolved_source_announcement_id",
        "resolved_attachment_id",
        "resolved_observation_version",
        "resolved_content_hash",
        "resolved_report_period",
        "reason_code",
        "retry_metadata_json",
        "diagnostics_json",
        "metadata_json",
        "processing_started_at",
        "finished_at",
        "stop_requested_at",
        "cancelled_at",
        "expires_at",
        "expired_at",
        "tombstone_until",
        "retention_policy_version",
        "created_at",
        "updated_at",
    },
    "official_asset_deletion_intents": {
        "deletion_id",
        "schema_version",
        "blob_hash",
        "managed_path",
        "predecessor_asset_id",
        "replacement_asset_id",
        "replacement_blob_hash",
        "decision_id",
        "outbox_event_key",
        "status",
        "reason",
        "lease_owner",
        "lease_generation",
        "lease_expires_at",
        "attempt",
        "next_retry_at",
        "error_code",
        "operation_mount_source",
        "operation_mount_point",
        "operation_mount_fs_type",
        "operation_mount_device_id",
        "operation_mount_filesystem_key",
        "operation_mount_captured_at",
        "recovery_pair_id",
        "recovery_pin_id",
        "recovery_manifest_id",
        "required_set_released_at",
        "planned_at",
        "deleting_at",
        "deleted_at",
        "updated_at",
    },
    "official_asset_deletion_audit": {
        "audit_id",
        "deletion_id",
        "status",
        "blob_hash",
        "managed_path",
        "predecessor_asset_id",
        "replacement_asset_id",
        "replacement_blob_hash",
        "reason",
        "retention_evidence_json",
        "actor",
        "details_json",
        "created_at",
    },
    "official_asset_recovery_manifest": {
        "recovery_id",
        "schema_version",
        "manifest_kind",
        "manifest_version",
        "predecessor_asset_id",
        "source",
        "source_announcement_id",
        "attachment_id",
        "version_id",
        "prior_path",
        "content_hash",
        "replacement_asset_id",
        "replacement_content_hash",
        "backup_object",
        "file_manifest_watermark",
        "catalog_snapshot_watermark",
        "consumer",
        "active_indefinitely",
        "created_at",
        "created_by",
        "evidence_json",
    },
    "official_asset_retention_pins": {
        "pin_id",
        "blob_hash",
        "pin_type",
        "pin_key",
        "owner",
        "created_at",
        "expires_at",
        "released_at",
        "blocks_primary_unlink",
        "required_set_hold",
        "required_set_released_at",
        "metadata_json",
    },
    "official_asset_change_events": {
        "event_id",
        "schema_version",
        "event_key",
        "event_type",
        "instrument_id",
        "fiscal_year",
        "asset_id",
        "predecessor_asset_id",
        "content_hash",
        "trigger_origin",
        "dispatch_policy_version",
        "payload_json",
        "created_at",
    },
    "official_asset_consumer_checkpoints": {
        "consumer",
        "schema_version",
        "last_event_id",
        "last_event_key",
        "last_attempted_event_id",
        "delivery_attempt",
        "last_attempted_at",
        "last_delivered_at",
        "last_error_code",
        "metadata_json",
        "created_at",
        "updated_at",
    },
    "official_asset_consumer_processing": {
        "processing_id",
        "schema_version",
        "asset_id",
        "consumer",
        "parser_version",
        "parameter_hash",
        "status",
        "derived_identity",
        "error_code",
        "canonical_projection_policy_version",
        "evidence_set_hash",
        "equivalent_source_filings_json",
        "metadata_json",
        "lease_owner",
        "lease_generation",
        "lease_expires_at",
        "heartbeat_at",
        "attempt",
        "max_attempts",
        "started_at",
        "finished_at",
        "created_at",
        "updated_at",
    },
    "official_asset_discovery_state": {
        "source",
        "exchange",
        "category",
        "scope_key",
        "config_fingerprint",
        "schema_version",
        "item_cursor_kind",
        "item_cursor_value",
        "covered_until",
        "run_cutoff",
        "next_page",
        "status",
        "is_complete",
        "gap_reason",
        "checkpoint_json",
        "lease_owner",
        "lease_expires_at",
        "lease_generation",
        "state_version",
        "created_at",
        "updated_at",
    },
}

PROTECTED_RESEARCH_TABLES = {
    "business_profile_evidence",
    "financial_numeric_facts_hot",
    "financial_numeric_facts_history",
}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _object_snapshot(conn: sqlite3.Connection, tables: set[str]):
    definitions = conn.execute(
        """SELECT type, name, tbl_name, sql FROM sqlite_master
           WHERE tbl_name IN ({}) AND type IN ('table', 'index', 'trigger')
           ORDER BY type, name""".format(",".join("?" for _ in tables)),
        tuple(sorted(tables)),
    ).fetchall()
    rows = {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
        for table in tables
    }
    return definitions, rows


def _seed_real_business_contracts(conn: sqlite3.Connection) -> None:
    from research.storage import ResearchStorageManager

    ResearchStorageManager._create_tables(conn)
    conn.execute(
        """INSERT INTO business_profile_evidence(
               evidence_id, instrument_id, source_document_id, source_tier,
               document_hash, data_available_date, availability_quality,
               evidence_text_hash, extraction_method, confidence, review_status,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "evidence-1",
            "600000.SH",
            "document-1",
            "official",
            "a" * 64,
            "2026-04-01",
            "exact",
            "b" * 64,
            "pdf_text",
            0.95,
            "approved",
            "2026-08-10T00:00:00+00:00",
            "2026-08-10T00:00:00+00:00",
        ),
    )
    for table in ("financial_numeric_facts_hot", "financial_numeric_facts_history"):
        conn.execute(
            f"""INSERT INTO {table}(
                   source_file_id, instrument_id, symbol, exchange, report_period,
                   report_type, statement_family, fact_name, fact_value,
                   parser_version, source, source_mode, created_at, updated_at
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                f"{table}-source",
                "600000.SH",
                "600000",
                "SSE",
                "2025-12-31",
                "annual",
                "balance_sheet",
                "total_assets",
                100.0,
                "fixture.v1",
                "official_json",
                "structured",
                "2026-08-10T00:00:00+00:00",
                "2026-08-10T00:00:00+00:00",
            ),
        )


def _config(tmp_path: Path, **overrides) -> AnnouncementAssetConfig:
    value = {
        "enabled": True,
        "scheduled_enabled": False,
        "dry_run": False,
        "paths": {
            "filings_root": "data/filings",
            "archive_root": "data/filings/announcements",
            "temp_root": "data/filings/announcements/tmp",
            "quarantine_root": "data/filings/announcements/quarantine",
            "require_mount": False,
        },
        "storage": {
            "warning_utilization": 0.98,
            "hard_stop_utilization": 0.999,
            "free_space_reserve_bytes": 1,
            "max_attachment_bytes": 1024 * 1024,
            "unknown_length_reservation_bytes": 4096,
        },
    }
    value.update(overrides)
    return AnnouncementAssetConfig.from_mapping(value, project_root=tmp_path)


def _record(
    *,
    source: str = "cninfo",
    source_id: str = "a-1",
    title: str = "甲公司2025年年度报告",
    attachment_name: str = "甲公司2025年年度报告.pdf",
    instrument_id: str = "600000.SH",
) -> tuple[AnnouncementRecord, str]:
    attachment = AnnouncementAttachment(
        source_url=f"https://static.example/{source_id}.pdf",
        attachment_id=f"file-{source_id}",
        name=attachment_name,
        media_type="application/pdf",
        raw_metadata={"content_length": len(PDF_BYTES)},
    )
    record = AnnouncementRecord(
        source=source,
        source_announcement_id=source_id,
        announcement_key=build_announcement_key(source, source_id),
        title=title,
        published_at="2026-03-20T01:00:00+00:00",
        published_at_raw="2026-03-20 09:00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(attachment,),
        raw_payload={"announcementId": source_id},
    )
    return record, instrument_id


def _classification(
    variant: AnnualReportVariant,
) -> AnnualReportClassification:
    return AnnualReportClassification(
        document_family="annual_report",
        fiscal_year=2025,
        report_period="2025-12-31",
        variant=variant,
        is_full_report=True,
        is_eligible=True,
        correction_evidence=variant is AnnualReportVariant.CORRECTION,
        reasons=("eligible",),
    )


def _candidate(
    candidate_id: str,
    *,
    source: str = "cninfo",
    content_hash: str = "a" * 64,
    published_at: str = "2026-03-20T01:00:00+00:00",
    variant: AnnualReportVariant = AnnualReportVariant.ORIGINAL,
    valid: bool = True,
    legal_chain_id: str | None = None,
    legal_precedence: int | None = None,
) -> AnnualReportCandidate:
    return AnnualReportCandidate(
        candidate_id=candidate_id,
        source=source,
        source_announcement_id=f"filing-{candidate_id}",
        attachment_id=f"attachment-{candidate_id}",
        content_hash=content_hash,
        published_at=published_at,
        classification=_classification(variant),
        integrity_valid=valid,
        legal_chain_id=legal_chain_id,
        legal_precedence=legal_precedence,
    )


def test_construction_has_zero_database_or_archive_side_effects(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "data/research.db")
    AnnouncementAssetService(repository=repository, config=config)

    assert not repository.db_path.exists()
    assert not config.archive_root.exists()


def test_job_defaults_reject_manual_only_cron_combinations(tmp_path):
    with pytest.raises(ValueError, match="latest backfill manual-only"):
        _config(
            tmp_path,
            jobs={
                "latest_backfill_manual_only": True,
                "latest_backfill_cron": "0 1 * * *",
            },
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("master_data_max_age_hours", 0),
        ("listed_security_census_max_age_hours", 0),
        ("bootstrap_max_lookback_years", 0),
        ("daily_catch_up_max_days", 0),
    ],
)
def test_config_rejects_zero_scope_and_freshness_limits(tmp_path, field, value):
    with pytest.raises(ValueError):
        _config(tmp_path, **{field: value})


@pytest.mark.parametrize(
    "storage",
    [
        {"part_max_age_seconds": 0},
        {"part_max_bytes": 0},
        {"part_safety_grace_seconds": 3600},
        {"quarantine_warning_age_seconds": 400, "quarantine_hard_age_seconds": 300},
        {"quarantine_warning_bytes": 400, "quarantine_hard_bytes": 300},
        {"quarantine_cleanup_policy": "automatic"},
    ],
)
def test_storage_sidecar_thresholds_fail_closed(tmp_path, storage):
    with pytest.raises((ValueError, TypeError)):
        _config(tmp_path, storage=storage)
    with pytest.raises(ValueError, match="every calendar day"):
        _config(
            tmp_path,
            jobs={
                "daily_enabled": False,
                "daily_cron": "15 3 * * mon-fri",
            },
        )


def test_canonical_classification_vocabulary_normalizes_legacy_labels():
    assert normalize_document_family("annual_report_correction") == (
        DocumentFamily.ANNUAL_REPORT.value
    )
    assert normalize_document_family("semiannual") == (
        DocumentFamily.SEMIANNUAL_REPORT.value
    )
    assert normalize_annual_report_variant("annual_report_correction") is (
        AnnualReportVariant.CORRECTION
    )


def test_config_rejects_path_escape(tmp_path):
    with pytest.raises(ValueError, match="safe project-relative"):
        _config(
            tmp_path,
            paths={
                "filings_root": "data/filings",
                "archive_root": "../outside",
            },
        )
@pytest.mark.parametrize(
    "archive_root",
    [
        "data/filings/announcements/has space",
        "data/filings/announcements/%2e%2e",
        "data/filings/announcements/\x00control",
        "data/filings/announcements/",
    ],
)
def test_config_rejects_noncanonical_dynamic_path_segments(tmp_path, archive_root):
    with pytest.raises(ValueError, match="path segment|path component|encoded traversal"):
        _config(
            tmp_path,
            paths={
                "filings_root": "data/filings",
                "archive_root": archive_root,
            },
        )


def test_runtime_mount_guard_rejects_local_fallback_when_mount_is_required(tmp_path):
    config = _config(tmp_path)
    guarded = replace(
        config,
        require_filings_mount=True,
        expected_filings_mount_source="nfs.example:/filings",
    )
    with pytest.raises(RuntimeError, match="mount source mismatch"):
        ContentAddressedBlobStore(guarded).prepare()


def test_clean_schema_creation_preserves_real_business_and_financial_contracts(
    tmp_path,
):
    db_path = tmp_path / "research.db"
    with sqlite3.connect(db_path) as conn:
        _seed_real_business_contracts(conn)
        before = _object_snapshot(conn, PROTECTED_RESEARCH_TABLES)
        conn.commit()

    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()
    repository.initialize_schema()

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        after = _object_snapshot(conn, PROTECTED_RESEARCH_TABLES)
    assert set(OWNED_TABLES).issubset(tables)
    assert before == after


def test_clean_schema_matches_canonical_field_matrix_and_constraints(tmp_path):
    db_path = tmp_path / "research.db"
    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()

    with sqlite3.connect(db_path) as conn:
        for table, expected in CANONICAL_SCHEMA_FIELD_MATRIX.items():
            assert expected.issubset(_table_columns(conn, table)), table
        version = conn.execute(
            """SELECT schema_version FROM official_asset_schema_versions
               WHERE component='announcement_assets'"""
        ).fetchone()[0]
        assert version == SCHEMA_VERSION
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO official_asset_operations(
                       operation_id, schema_version, operation_type, idempotency_key,
                       scope_json, policy_version, status, created_at, updated_at
                   ) VALUES('bad-status', 'v1', 'ensure', 'bad-status', '{}', 'v1',
                            'expired', '2026-08-10', '2026-08-10')"""
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO official_asset_operations(
                       operation_id, schema_version, operation_type, idempotency_key,
                       scope_json, policy_version, status, stage, created_at, updated_at
                   ) VALUES('bad-stage', 'v1', 'ensure', 'bad-stage', '{}', 'v1',
                            'queued', 'completed', '2026-08-10', '2026-08-10')"""
            )


def test_schema_initialized_distinguishes_an_unrelated_existing_database(tmp_path):
    db_path = tmp_path / "research.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE unrelated(value TEXT)")
    repository = AnnouncementAssetRepository(db_path)

    assert repository.schema_initialized() is False

    repository.initialize_schema()

    assert repository.schema_initialized() is True


def test_announcement_and_attachment_upserts_are_source_qualified_and_idempotent(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    record, instrument_id = _record()

    first = repository.upsert_announcement(
        record, instrument_id=instrument_id, observed_at="2026-03-20T02:00:00+00:00"
    )
    second = repository.upsert_announcement(
        replace(record, title="甲公司2025年年度报告（更新观察）"),
        instrument_id=instrument_id,
        observed_at="2026-03-21T02:00:00+00:00",
    )
    first_attachment = repository.upsert_attachment(
        first.announcement_id,
        record.attachments[0],
        observed_at="2026-03-20T02:00:00+00:00",
    )
    second_attachment = repository.upsert_attachment(
        first.announcement_id,
        replace(
            record.attachments[0], source_url="https://static.example/a-1.pdf?b=2&a=1"
        ),
        observed_at="2026-03-21T02:00:00+00:00",
    )

    assert first.announcement_id == second.announcement_id
    assert second.first_observed_at == first.first_observed_at
    assert second.last_observed_at == "2026-03-21T02:00:00+00:00"
    assert first_attachment.attachment_id == second_attachment.attachment_id
    assert len(repository.list_attachments(first.announcement_id)) == 1


@pytest.mark.parametrize(
    ("title", "attachment_name", "eligible", "reason"),
    [
        (
            "甲公司2025年年度报告",
            "2025年年度报告.pdf",
            True,
            "eligible_complete_original",
        ),
        ("甲公司2025年年度报告摘要", "摘要.pdf", False, "excluded:摘要"),
        (
            "甲公司2025年年度报告",
            "2025年年度报告（英文版）.pdf",
            False,
            "excluded:英文版",
        ),
        (
            "甲公司2025年年度报告（英文）",
            "2025年年度报告（英文）.pdf",
            False,
            "excluded:（英文）",
        ),
        (
            "甲公司2025年年度报告（英文简版）",
            "2025年年度报告（英文简版）.pdf",
            False,
            "excluded:英文简版",
        ),
        (
            "甲公司关于2025年年度报告的自愿性披露公告",
            "自愿性披露公告.pdf",
            False,
            "excluded:自愿性披露公告",
        ),
        (
            "甲公司2025年年度报告更正公告",
            "更正公告.pdf",
            False,
            "correction_notice_without_full_replacement",
        ),
        (
            "甲公司2025年年度报告更正公告",
            "2025年年度报告（修订版）.pdf",
            True,
            "eligible_complete_correction",
        ),
        (
            "甲公司关于更正《2025年年度报告》的公告",
            "关于更正《2025年年度报告》的公告.pdf",
            False,
            "correction_notice_without_full_replacement",
        ),
        (
            "甲公司2025年半年度报告",
            "2025年半年度报告.pdf",
            False,
            "excluded:半年度报告",
        ),
        (
            "甲公司2025年半<em>年度报告</em>（更正后）",
            "2025年半年度报告（更正后）.pdf",
            False,
            "excluded:半年度报告",
        ),
    ],
)
def test_classifier_is_attachment_level_and_fail_closed(
    title, attachment_name, eligible, reason
):
    record, _ = _record(title=title, attachment_name=attachment_name)
    result = AnnualReportClassifier().classify(record, record.attachments[0])

    assert result.is_eligible is eligible
    assert reason in result.reasons
    assert result.vocabulary_version == CLASSIFICATION_VOCABULARY_VERSION
    if "更正公告" in title:
        assert result.document_family == DocumentFamily.ANNUAL_REPORT.value
        assert result.variant is AnnualReportVariant.CORRECTION
        assert result.is_full_report is eligible


def test_pdf_title_refinement_rejects_provider_mislabeled_summary(monkeypatch):
    class Page:
        @staticmethod
        def extract_text():
            return "甲公司 2025 年年度报告摘要"

    class Reader:
        def __init__(self, *_args, **_kwargs):
            self.pages = [Page()]

    import pypdf

    monkeypatch.setattr(pypdf, "PdfReader", Reader)
    record, _ = _record(title="甲公司2025年年度报告")
    initial = AnnualReportClassifier().classify(record, record.attachments[0])

    refined = refine_classification_from_pdf(initial, PDF_BYTES)

    assert not refined.is_eligible
    assert not refined.is_full_report
    assert "excluded:pdf_annual_report_summary" in refined.reasons


def test_fiscal_year_policy_handles_january_april_deadline_and_post_period_listing():
    january = derive_fiscal_year_search_bounds(
        as_of=date(2026, 1, 15),
        listing_date=date(2020, 6, 1),
        provider_coverage_start_year=2000,
        lookback_years=5,
    )
    after_deadline = derive_fiscal_year_search_bounds(
        as_of=date(2026, 5, 1),
        listing_date=date(2020, 6, 1),
        provider_coverage_start_year=2000,
        lookback_years=5,
    )
    newly_listed = derive_fiscal_year_search_bounds(
        as_of=date(2026, 3, 1),
        listing_date=date(2026, 1, 10),
        provider_coverage_start_year=2000,
        lookback_years=5,
    )

    assert january.candidate_upper_year == 2025
    assert january.disclosure_due_year == 2024
    assert after_deadline.disclosure_due_year == 2025
    assert newly_listed.candidate_years == ()


def test_winner_selection_prefers_latest_correction_and_blocks_cross_source_conflict():
    original = _candidate("original")
    first_correction = _candidate(
        "correction-1",
        variant=AnnualReportVariant.CORRECTION,
        published_at="2026-04-01T01:00:00+00:00",
        content_hash="b" * 64,
    )
    latest_correction = _candidate(
        "correction-2",
        variant=AnnualReportVariant.CORRECTION,
        published_at="2026-04-02T01:00:00+00:00",
        content_hash="c" * 64,
    )
    selected = select_effective_candidate(
        [original, first_correction, latest_correction]
    )
    assert selected.winner == latest_correction
    assert selected.state is EffectiveDecisionState.CURRENT

    conflict = select_effective_candidate(
        [
            latest_correction,
            replace(
                latest_correction,
                candidate_id="exchange-copy",
                source="sse",
                content_hash="d" * 64,
            ),
        ]
    )
    assert conflict.winner is None
    assert conflict.state is EffectiveDecisionState.AMBIGUOUS
    assert conflict.pending_candidate is not None
    assert conflict.pending_candidate.candidate_id in {
        latest_correction.candidate_id,
        "exchange-copy",
    }


def test_same_source_same_timestamp_different_hash_fails_closed_without_precedence():
    first = _candidate(
        "same-time-a",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="2026-04-02T01:00:00+00:00",
    )
    second = _candidate(
        "same-time-b",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="b" * 64,
        published_at="2026-04-02T01:00:00+00:00",
    )
    selection = select_effective_candidate([first, second])
    assert selection.winner is None
    assert selection.state is EffectiveDecisionState.AMBIGUOUS


def test_same_source_same_timestamp_uses_unique_legal_precedence():
    lower = _candidate(
        "same-time-low",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="2026-04-02T01:00:00+00:00",
        legal_chain_id="chain",
        legal_precedence=1,
    )
    higher = _candidate(
        "same-time-high",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="b" * 64,
        published_at="2026-04-02T01:00:00+00:00",
        legal_chain_id="chain",
        legal_precedence=2,
    )
    selection = select_effective_candidate([lower, higher])
    reverse = select_effective_candidate([higher, lower])
    assert selection.winner == higher
    assert reverse.winner == higher
    assert selection.state is EffectiveDecisionState.CURRENT
    assert reverse.state is EffectiveDecisionState.CURRENT


def test_same_source_equivalent_tie_break_is_discovery_order_independent():
    earlier_identity = replace(
        _candidate(
            "candidate-z",
            variant=AnnualReportVariant.CORRECTION,
            content_hash="a" * 64,
            published_at="2026-04-02T01:00:00+00:00",
            legal_chain_id="proved-chain",
        ),
        source_announcement_id="announcement-100",
        attachment_id="attachment-900",
    )
    later_identity = replace(
        _candidate(
            "candidate-a",
            variant=AnnualReportVariant.CORRECTION,
            content_hash="a" * 64,
            published_at="2026-04-02T09:00:00+08:00",
            legal_chain_id="proved-chain",
        ),
        source_announcement_id="announcement-200",
        attachment_id="attachment-100",
    )

    forward = select_effective_candidate([earlier_identity, later_identity])
    reverse = select_effective_candidate([later_identity, earlier_identity])

    assert SAME_SOURCE_EQUIVALENT_TIE_BREAK_POLICY_VERSION.endswith(".v1")
    assert forward.state is reverse.state is EffectiveDecisionState.CURRENT
    assert forward.winner == reverse.winner == later_identity
    assert forward.canonical_source_filing == reverse.canonical_source_filing
    assert forward.equivalent_source_filings == reverse.equivalent_source_filings
    assert forward.evidence_set_hash == reverse.evidence_set_hash
    assert (
        forward.canonical_projection_policy_version
        == reverse.canonical_projection_policy_version
    )


def test_same_source_filing_url_variants_share_legal_chain():
    classification = {
        "document_family": "annual_report",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "variant": "original",
        "is_full_report": True,
        "is_eligible": True,
        "correction_evidence": False,
        "reasons": ["eligible_complete_original"],
        "policy_version": "formal_annual_report.v1",
        "vocabulary_version": CLASSIFICATION_VOCABULARY_VERSION,
    }

    def row(attachment_id: str, version_id: str):
        return {
            "version_id": version_id,
            "attachment_id": attachment_id,
            "source": "cninfo",
            "source_announcement_id": "1225132706",
            "instrument_id": "920128.BJ",
            "exchange": "BSE",
            "content_hash": "a" * 64,
            "published_at": "2026-04-19T16:00:00+00:00",
            "classification": classification,
            "integrity_status": "valid",
            "blob_integrity_status": "valid",
            "attachment_metadata": {},
            "announcement_metadata": {},
        }

    first = _candidate_from_row(row("attachment-absolute", "version-absolute"))
    second = _candidate_from_row(row("attachment-relative", "version-relative"))
    selection = select_effective_candidate((first, second))

    assert first.legal_chain_id
    assert first.legal_chain_id == second.legal_chain_id
    assert selection.state is EffectiveDecisionState.CURRENT


def test_cninfo_same_title_same_day_republication_uses_provider_id_order():
    classification = {
        "document_family": "annual_report",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "variant": "original",
        "is_full_report": True,
        "is_eligible": True,
        "correction_evidence": False,
        "reasons": ["eligible_complete_original"],
        "policy_version": "formal_annual_report.v1",
        "vocabulary_version": CLASSIFICATION_VOCABULARY_VERSION,
    }

    def row(source_announcement_id: str, content_hash: str):
        return {
            "version_id": f"version-{source_announcement_id}",
            "attachment_id": f"attachment-{source_announcement_id}",
            "source": "cninfo",
            "source_announcement_id": source_announcement_id,
            "title": "2025年年度报告",
            "instrument_id": "920445.BJ",
            "exchange": "BSE",
            "content_hash": content_hash,
            "published_at": "2026-03-26T16:00:00+00:00",
            "classification": classification,
            "integrity_status": "valid",
            "blob_integrity_status": "valid",
            "attachment_metadata": {},
            "announcement_metadata": {},
        }

    earlier = _candidate_from_row(row("1225045788", "a" * 64))
    later = _candidate_from_row(row("1225287578", "b" * 64))
    selection = select_effective_candidate((later, earlier))

    assert earlier.legal_chain_id == later.legal_chain_id
    assert later.legal_precedence > earlier.legal_precedence
    assert selection.state is EffectiveDecisionState.CURRENT
    assert selection.winner == later


@pytest.mark.parametrize("chain_ids", [(None, None), ("chain-a", "chain-b")])
def test_same_source_equivalent_timestamp_requires_proved_legal_chain(chain_ids):
    first = _candidate(
        "same-hash-a",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="2026-04-02T01:00:00+00:00",
        legal_chain_id=chain_ids[0],
    )
    second = _candidate(
        "same-hash-b",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="2026-04-02T09:00:00+08:00",
        legal_chain_id=chain_ids[1],
    )

    forward = select_effective_candidate([first, second])
    reverse = select_effective_candidate([second, first])

    assert forward.winner is reverse.winner is None
    assert forward.state is reverse.state is EffectiveDecisionState.AMBIGUOUS


def test_same_source_same_chain_timestamp_different_hash_remains_ambiguous():
    first = _candidate(
        "different-hash-a",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="2026-04-02T01:00:00+00:00",
        legal_chain_id="proved-chain",
    )
    second = _candidate(
        "different-hash-b",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="b" * 64,
        published_at="2026-04-02T09:00:00+08:00",
        legal_chain_id="proved-chain",
    )

    selection = select_effective_candidate([first, second])

    assert selection.winner is None
    assert selection.state is EffectiveDecisionState.AMBIGUOUS


def test_same_source_equivalent_identity_requires_normalized_publication_time():
    first = _candidate(
        "missing-time-a",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="",
        legal_chain_id="proved-chain",
    )
    second = _candidate(
        "missing-time-b",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="a" * 64,
        published_at="not-a-timestamp",
        legal_chain_id="proved-chain",
    )

    selection = select_effective_candidate([first, second])

    assert selection.winner is None
    assert selection.state is EffectiveDecisionState.AMBIGUOUS


def test_old_attachment_version_schema_migrates_availability_time(tmp_path):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE official_attachment_versions(
                version_id TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL,
                attachment_id TEXT NOT NULL,
                observation_key TEXT NOT NULL,
                content_hash TEXT,
                final_url TEXT,
                retrieval_status TEXT NOT NULL,
                integrity_status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                next_retry_at TEXT,
                error_code TEXT,
                observed_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(attachment_id, observation_key)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO official_attachment_versions(
                version_id, schema_version, attachment_id, observation_key,
                content_hash, final_url, retrieval_status, integrity_status,
                attempt, observed_at, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "version-legacy",
                "official_attachment_version.v1",
                "attachment-legacy",
                "observation-legacy",
                None,
                None,
                "failed",
                IntegrityStatus.MISSING.value,
                1,
                "2026-08-10T00:00:00+00:00",
                "2026-08-10T00:00:00+00:00",
                "2026-08-10T00:00:00+00:00",
            ),
        )
        conn.commit()

    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()
    migrated = repository.get_attachment_version("version-legacy")
    assert migrated is not None
    assert migrated.version_available_at == migrated.observed_at
    assert migrated.available_time_source == "first_observed"
    assert migrated.available_time_precision == "instant"


def test_v6_foundation_schema_migrates_status_stage_recovery_and_pin_contracts(
    tmp_path,
):
    db_path = tmp_path / "legacy-foundation.db"
    with sqlite3.connect(db_path) as conn:
        _seed_real_business_contracts(conn)
        conn.executescript(
            """
            CREATE TABLE official_asset_acquisition_leases(
                attachment_id TEXT PRIMARY KEY, lease_owner TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE official_asset_operations(
                operation_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL,
                operation_type TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                scope_json TEXT NOT NULL, bounds_json TEXT NOT NULL DEFAULT '{}',
                policy_version TEXT NOT NULL, config_version TEXT, owner TEXT,
                status TEXT NOT NULL, stage TEXT, outcome TEXT,
                attempt INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 1,
                resume_generation INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT,
                lease_owner TEXT, lease_expires_at TEXT, heartbeat_at TEXT,
                progress_json TEXT NOT NULL DEFAULT '{}',
                checkpoint_json TEXT NOT NULL DEFAULT '{}', result_asset_id TEXT,
                result_origin TEXT, reason_code TEXT,
                diagnostics_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL,
                started_at TEXT, finished_at TEXT, updated_at TEXT NOT NULL
            );
            CREATE TABLE official_asset_operation_subscriptions(
                asset_request_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL,
                operation_id TEXT NOT NULL, principal TEXT NOT NULL, consumer TEXT,
                idempotency_key TEXT NOT NULL, request_fingerprint TEXT NOT NULL,
                authorized_projection_json TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL,
                consumer_continuation_id TEXT, metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, cancelled_at TEXT,
                expires_at TEXT, UNIQUE(principal, idempotency_key)
            );
            CREATE TABLE official_asset_deletion_intents(
                deletion_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL,
                blob_hash TEXT NOT NULL, managed_path TEXT NOT NULL,
                predecessor_asset_id TEXT, replacement_asset_id TEXT,
                replacement_blob_hash TEXT, status TEXT NOT NULL, reason TEXT NOT NULL,
                lease_owner TEXT, lease_generation INTEGER NOT NULL DEFAULT 0,
                lease_expires_at TEXT, attempt INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT, error_code TEXT, planned_at TEXT NOT NULL,
                deleting_at TEXT, deleted_at TEXT, updated_at TEXT NOT NULL
            );
            CREATE TABLE official_asset_retention_pins(
                pin_id TEXT PRIMARY KEY, blob_hash TEXT NOT NULL, pin_type TEXT NOT NULL,
                pin_key TEXT NOT NULL, owner TEXT, created_at TEXT NOT NULL,
                expires_at TEXT, released_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            INSERT INTO official_asset_operations(
                operation_id, schema_version, operation_type, idempotency_key,
                scope_json, policy_version, status, created_at, updated_at
            ) VALUES(
                'legacy-expired', 'official_asset_operation.v1', 'ensure',
                'legacy-expired', '{}', 'v1', 'expired', '2026-08-10', '2026-08-10'
            );
            """
        )
        before = _object_snapshot(conn, PROTECTED_RESEARCH_TABLES)
        conn.commit()

    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()

    migrated = repository.get_operation("legacy-expired")
    assert migrated is not None
    assert migrated.status is OperationStatus.BLOCKED
    assert migrated.reason_code == "legacy_expired_operation"
    assert migrated.schema_version == "official_asset_operation.v2"
    assert migrated.stage_schema_version == "official_asset_operation_stage.v1"
    with sqlite3.connect(db_path) as conn:
        assert _object_snapshot(conn, PROTECTED_RESEARCH_TABLES) == before
        for table, expected in CANONICAL_SCHEMA_FIELD_MATRIX.items():
            assert expected.issubset(_table_columns(conn, table)), table
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE official_asset_operations SET status='expired' "
                "WHERE operation_id='legacy-expired'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE official_asset_operations SET stage='completed' "
                "WHERE operation_id='legacy-expired'"
            )


def test_a_share_universe_policy_covers_active_sse_szse_bse_and_excludes_non_stock():
    policy = EligibilityPolicy(max_freshness_hours=36)
    records = [
        {
            "instrument_id": "600000.SH",
            "exchange": "SSE",
            "type": "stock",
            "currency": "CNY",
            "is_active": True,
            "board": "main",
        },
        {
            "instrument_id": "688001.SH",
            "exchange": "SSE",
            "type": "stock",
            "currency": "CNY",
            "is_active": True,
            "board": "star",
        },
        {
            "instrument_id": "300001.SZ",
            "exchange": "SZSE",
            "type": "stock",
            "currency": "CNY",
            "is_active": True,
            "board": "chinext",
        },
        {
            "instrument_id": "920001.BJ",
            "exchange": "BSE",
            "type": "stock",
            "currency": "CNY",
            "is_active": True,
            "status": "suspended",
        },
        {
            "instrument_id": "900901.SH",
            "exchange": "SSE",
            "type": "stock",
            "currency": "USD",
            "is_active": True,
        },
        {
            "instrument_id": "510300.SH",
            "exchange": "SSE",
            "type": "etf",
            "currency": "CNY",
            "is_active": True,
        },
    ]
    snapshot = policy.materialize(
        records,
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence={
            "status": "complete",
            "scope": "full_refresh",
            "source": "instrument_master_refresh_state",
            "watermark": "refresh-master-v1",
            "exchanges": ("SSE", "SZSE", "BSE"),
            "completed_at": "2026-08-10T00:00:00+00:00",
        },
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    assert snapshot.is_complete
    assert {row["instrument_id"] for row in snapshot.instruments} == {
        "600000.SH",
        "688001.SH",
        "300001.SZ",
        "920001.BJ",
    }


def test_universe_refresh_falls_back_and_keeps_indeterminate_evidence(tmp_path):
    policy = EligibilityPolicy(max_freshness_hours=24)
    previous = policy.materialize(
        [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "is_active": True,
            }
        ],
        master_data_version="master-v1",
        master_data_last_success_at="2026-08-10T00:00:00+00:00",
        master_data_refresh_evidence={
            "status": "complete",
            "scope": "full_refresh",
            "source": "instrument_master_refresh_state",
            "watermark": "refresh-master-v1",
            "exchanges": ("SSE", "SZSE", "BSE"),
            "completed_at": "2026-08-10T00:00:00+00:00",
        },
        snapshot_at="2026-08-10T01:00:00+00:00",
    )
    failed = policy.materialize(
        [
            {
                "instrument_id": "000001.SZ",
                "exchange": "SZSE",
                "type": None,
                "currency": "CNY",
                "is_active": True,
            }
        ],
        master_data_version="master-v2-partial",
        master_data_last_success_at="2026-08-08T00:00:00+00:00",
        snapshot_at="2026-08-10T02:00:00+00:00",
        source_complete=False,
        previous=previous,
    )
    assert not failed.is_complete
    assert failed.status == "eligibility_indeterminate"
    assert failed.indeterminate[0]["reason"] == "missing_eligibility_fields"
    assert effective_snapshot(failed, previous).snapshot_id == previous.snapshot_id

    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(previous.to_mapping())
    repository.upsert_universe_snapshot(failed.to_mapping())
    assert (
        repository.get_latest_complete_universe_snapshot()["snapshot_id"]
        == previous.snapshot_id
    )


def test_asset_availability_and_expected_period_coverage_are_orthogonal(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    row = repository.upsert_asset_coverage(
        universe_snapshot_id="snapshot-1",
        instrument_id="600000.SH",
        fiscal_year=2024,
        status="available",
        as_of="2026-05-01T00:00:00+00:00",
        expected_fiscal_year=2025,
        earliest_search_year=2020,
        evidence={
            "expected_period_coverage": ExpectedPeriodCoverage.OVERDUE_MISSING.value,
            "daily_readiness": "degraded",
            "daily_enablement_blocked": False,
        },
    )
    assert row["status"] == "available"
    assert row["evidence"]["expected_period_coverage"] == "overdue_missing"
    assert row["evidence"]["daily_enablement_blocked"] is False


def test_knowledge_cutoff_hides_later_attachment_observation(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    record, instrument_id = _record()
    registered = service.register_discovered_record(record, instrument_id=instrument_id)
    version_id = registered[0].attachment_id
    attachment = repository.get_attachment(version_id)
    assert attachment is not None
    digest = hashlib.sha256(PDF_BYTES).hexdigest()
    published = store.publish_bytes(PDF_BYTES, expected_hash=digest)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF_BYTES),
            canonical_path=str(published.path),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-10T00:00:00+00:00",
            last_verified_at="2026-08-10T00:00:00+00:00",
        )
    )
    repository.add_attachment_version(
        OfficialAttachmentVersion(
            version_id="silent-update-version",
            attachment_id=attachment.attachment_id,
            observation_key="silent-update-observation",
            content_hash=digest,
            final_url=attachment.normalized_source_url,
            retrieval_status="success",
            integrity_status=IntegrityStatus.VALID,
            attempt=1,
            next_retry_at=None,
            error_code=None,
            observed_at="2026-08-10T00:00:00+00:00",
            version_available_at="2026-08-10T00:00:00+00:00",
            available_time_source="official_effective_time",
            available_time_precision="day",
        )
    )
    effective = service.recompute_effective_report(instrument_id, 2025)
    assert effective is not None
    assert (
        repository.get_effective_report(
            instrument_id, 2025, knowledge_cutoff="2026-08-05T00:00:00+00:00"
        )
        is None
    )
    assert (
        repository.get_effective_report(
            instrument_id, 2025, knowledge_cutoff="2026-08-10T00:00:00+00:00"
        )
        is not None
    )


def test_unverified_newer_correction_keeps_predecessor_provisional():
    original = _candidate("original")
    pending = _candidate(
        "pending",
        variant=AnnualReportVariant.CORRECTION,
        published_at="2026-04-02T01:00:00+00:00",
        content_hash="b" * 64,
        valid=False,
    )
    selection = select_effective_candidate([original, pending], current=original)
    assert selection.winner == original
    assert selection.pending_candidate == pending
    assert selection.state is EffectiveDecisionState.PROVISIONAL


def test_withdrawal_is_excluded_and_governed_legal_chain_resolves_mirrors():
    withdrawn = replace(_candidate("withdrawn"), withdrawn=True)
    original = _candidate("original")
    assert select_effective_candidate([withdrawn, original]).winner == original

    correction = _candidate(
        "cninfo-correction",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="b" * 64,
        legal_chain_id="legal-chain-1",
    )
    mirror = replace(
        correction,
        candidate_id="sse-correction",
        source="sse",
        content_hash="c" * 64,
    )
    selection = select_effective_candidate([correction, mirror])
    assert selection.state is EffectiveDecisionState.CURRENT
    assert selection.winner in {correction, mirror}


def test_equivalent_source_projection_is_discovery_order_independent():
    cninfo = _candidate(
        "cninfo-mirror",
        variant=AnnualReportVariant.CORRECTION,
        content_hash="b" * 64,
        published_at="2026-04-02T01:00:00+00:00",
    )
    exchange = replace(
        cninfo,
        candidate_id="sse-mirror",
        source="sse",
        source_announcement_id="sse-filing",
        attachment_id="sse-attachment",
    )

    forward = select_effective_candidate([cninfo, exchange])
    reverse = select_effective_candidate([exchange, cninfo])

    assert forward.canonical_source_filing == reverse.canonical_source_filing
    assert forward.equivalent_source_filings == reverse.equivalent_source_filings
    assert forward.evidence_set_hash == reverse.evidence_set_hash
    assert forward.canonical_projection_policy_version == (
        reverse.canonical_projection_policy_version
    )


def test_blob_store_atomically_publishes_and_deduplicates(tmp_path):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    digest = hashlib.sha256(PDF_BYTES).hexdigest()

    first = store.publish_bytes(PDF_BYTES, expected_hash=digest)
    second = store.publish_bytes(PDF_BYTES, expected_hash=digest)
    validation = store.validate_blob(
        first.path, expected_hash=digest, expected_length=len(PDF_BYTES)
    )

    assert first.created is True
    assert second.created is False
    assert first.path == second.path
    assert validation.status is IntegrityStatus.VALID
    assert list(config.blob_root.rglob("*.part")) == []


def test_blob_store_rejects_invalid_pdf_hash_and_path_escape(tmp_path):
    store = ContentAddressedBlobStore(_config(tmp_path))
    store.prepare()
    with pytest.raises(ValueError, match="PDF signature"):
        store.publish_bytes(b"not-pdf")
    with pytest.raises(ValueError, match="expected hash"):
        store.publish_bytes(PDF_BYTES, expected_hash="0" * 64)
    with pytest.raises(ValueError, match="escapes"):
        store.validate_blob(tmp_path / "outside.pdf")


def test_blob_publish_fails_closed_when_mount_changes_after_preflight(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    stable = MountIdentity(
        requested_path=config.filings_root,
        mount_point=config.filings_root,
        source="nfs.example:/filings",
        fs_type="nfs4",
        device_id=10,
    )
    changed = MountIdentity(
        requested_path=config.filings_root,
        mount_point=config.filings_root,
        source="local-fallback",
        fs_type="ext4",
        device_id=11,
    )
    identities = iter((stable, stable, changed))
    monkeypatch.setattr(
        "research.announcement_assets.storage.probe_mount_identity",
        lambda _: next(identities),
    )

    with pytest.raises(RuntimeError, match="identity changed"):
        store.publish_bytes(PDF_BYTES)

    digest = hashlib.sha256(PDF_BYTES).hexdigest()
    assert not store.blob_path(digest).exists()
    assert list(config.blob_root.rglob("*.part")) == []


def test_operation_single_flight_transition_and_discovery_range_watermark(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    first, created = repository.create_or_reuse_operation(
        operation_type="ensure_annual_report",
        idempotency_key="scope-1",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="v1",
        owner="business-profile",
        stage=OperationStage.DISCOVERING,
    )
    second, second_created = repository.create_or_reuse_operation(
        operation_type="ensure_annual_report",
        idempotency_key="scope-1",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="v1",
        owner="broker",
        stage=OperationStage.DISCOVERING,
    )
    assert created is True
    assert second_created is False
    assert first.operation_id == second.operation_id

    running = repository.claim_operation(
        first.operation_id,
        lease_owner="worker-1",
        lease_expires_at="2099-01-01T00:00:00+00:00",
        stage=OperationStage.DOWNLOADING,
    )
    completed = repository.transition_operation(
        running.operation_id,
        OperationStatus.COMPLETED,
        outcome=BatchOutcome.SUCCESS,
        expected_lease_owner="worker-1",
        expected_lease_generation=running.lease_generation,
    )
    assert completed.status is OperationStatus.COMPLETED
    with pytest.raises(ValueError, match="invalid operation transition"):
        repository.transition_operation(completed.operation_id, OperationStatus.RUNNING)

    empty = repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="fp-1",
        status="success_empty",
        is_complete=True,
        covered_until="2026-08-01T00:00:00+00:00",
        run_cutoff="2026-08-01T00:00:00+00:00",
    )
    incomplete = repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="fp-1",
        status="failed",
        is_complete=False,
        covered_until="2026-08-02T00:00:00+00:00",
        run_cutoff="2026-08-02T00:00:00+00:00",
    )
    assert empty["covered_until"] == "2026-08-01T00:00:00+00:00"
    assert incomplete["covered_until"] == "2026-08-01T00:00:00+00:00"


def test_operation_stage_is_versioned_and_expiry_stays_on_caller_projection(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    subscription, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="stage-scope",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="v1",
        principal="alice",
        request_idempotency_key="stage-request",
        request_fingerprint="stage-fingerprint",
        stage=None,
    )
    assert operation.stage is None
    assert operation.stage_schema_version == "official_asset_operation_stage.v1"

    completed = repository.transition_operation(
        operation.operation_id,
        OperationStatus.CANCELLED,
        stage=OperationStage.NOT_APPLICABLE,
        outcome=BatchOutcome.PARTIAL,
    )
    assert completed.stage is OperationStage.NOT_APPLICABLE
    expired = repository.expire_asset_request(
        subscription.asset_request_id,
        principal="alice",
        tombstone_until="2027-08-10T00:00:00+00:00",
    )
    assert expired.status is AssetRequestStatus.EXPIRED
    assert expired.expired_at is not None
    assert expired.tombstone_until == "2027-08-10T00:00:00+00:00"
    assert (
        repository.get_operation(operation.operation_id).status
        is OperationStatus.CANCELLED
    )
    replayed, replayed_operation, replay_created, replay_operation_created = (
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="stage-scope",
            scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
            policy_version="v1",
            principal="alice",
            request_idempotency_key="stage-request",
            request_fingerprint="stage-fingerprint",
            stage=None,
        )
    )
    assert replayed.asset_request_id == subscription.asset_request_id
    assert replayed.status is AssetRequestStatus.EXPIRED
    assert replayed_operation.operation_id == operation.operation_id
    assert replay_created is False
    assert replay_operation_created is False
    fresh, fresh_operation, fresh_created, fresh_operation_created = (
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="stage-scope",
            scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
            policy_version="v1",
            principal="alice",
            request_idempotency_key="stage-request-after-expiry",
            request_fingerprint="stage-fingerprint",
            stage=None,
        )
    )
    assert fresh.asset_request_id != subscription.asset_request_id
    assert fresh.status is AssetRequestStatus.ACTIVE
    assert fresh_operation.operation_id != operation.operation_id
    assert fresh_created is True
    assert fresh_operation_created is True


def test_retention_pin_unlink_block_and_required_set_hold_transition_independently(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest = "a" * 64
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF_BYTES),
            canonical_path=str(tmp_path / "predecessor.pdf"),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-10T00:00:00+00:00",
            last_verified_at="2026-08-10T00:00:00+00:00",
        )
    )
    pin_id = repository.add_retention_pin(
        blob_hash=digest,
        pin_type="backup_required",
        pin_key="deletion-1",
        blocks_primary_unlink=True,
        required_set_hold=True,
    )
    assert repository.active_retention_pin_count(digest) == 1
    assert repository.active_required_set_hold_count(digest) == 1

    assert repository.transition_retention_pin(pin_id, blocks_primary_unlink=False)
    assert repository.active_retention_pin_count(digest) == 0
    assert repository.active_required_set_hold_count(digest) == 1
    assert repository.transition_retention_pin(pin_id, release_required_set_hold=True)
    assert repository.active_required_set_hold_count(digest) == 0
    with repository.connection() as conn:
        row = conn.execute(
            "SELECT * FROM official_asset_retention_pins WHERE pin_id=?", (pin_id,)
        ).fetchone()
    assert row["blocks_primary_unlink"] == 0
    assert row["required_set_hold"] == 0
    assert row["required_set_released_at"] is not None


def test_recovery_predecessor_pin_cannot_bypass_pair_closure(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    digest = "b" * 64
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=digest,
            content_length=len(PDF_BYTES),
            canonical_path=str(tmp_path / "predecessor.pdf"),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-10T00:00:00+00:00",
            last_verified_at="2026-08-10T00:00:00+00:00",
        )
    )
    pin_id = repository.add_retention_pin(
        blob_hash=digest,
        pin_type="recovery_predecessor",
        pin_key="recovery-pair-1",
        blocks_primary_unlink=True,
        required_set_hold=True,
    )

    with pytest.raises(PermissionError, match="pair closure"):
        repository.transition_retention_pin(
            pin_id, blocks_primary_unlink=False
        )
    with pytest.raises(PermissionError, match="cannot be released"):
        repository.release_retention_pin(pin_id)
    assert repository.active_retention_pin_count(digest) == 1
    assert repository.active_required_set_hold_count(digest) == 1


def test_atomic_storage_reservations_do_not_cross_hard_reserve(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    assert repository.reserve_storage(
        reservation_id="r1",
        filesystem_key="fs",
        planned_bytes=60,
        lease_expires_at="2099-01-01T00:00:00+00:00",
        capacity_bytes=100,
        hard_reserve_bytes=10,
    )
    assert not repository.reserve_storage(
        reservation_id="r2",
        filesystem_key="fs",
        planned_bytes=40,
        lease_expires_at="2099-01-01T00:00:00+00:00",
        capacity_bytes=100,
        hard_reserve_bytes=10,
    )
    assert repository.release_storage_reservation("r1")
    assert repository.reserve_storage(
        reservation_id="r2",
        filesystem_key="fs",
        planned_bytes=40,
        lease_expires_at="2099-01-01T00:00:00+00:00",
        capacity_bytes=100,
        hard_reserve_bytes=10,
    )


def test_retry_taxonomy_and_exhaustion_projection_are_bounded(tmp_path):
    from research.announcement_assets.retry import (
        RetryFailureClass,
        RetryQueueStatus,
        classify_retry_failure,
    )

    config = _config(tmp_path).retry
    first = classify_retry_failure(
        "provider timeout",
        attempt=1,
        config=config,
        now="2026-08-10T00:00:00+00:00",
    )
    exhausted = classify_retry_failure(
        "provider timeout",
        attempt=config.max_attempts,
        config=config,
        now="2026-08-10T00:00:00+00:00",
    )
    storage = classify_retry_failure(
        "filings hard free-space reserve would be violated",
        attempt=1,
        config=config,
    )
    invalid_pdf = classify_retry_failure(
        "attachment does not have a valid PDF signature",
        attempt=1,
        config=config,
    )
    dense_day = classify_retry_failure(
        "unsplittable_dense_day",
        attempt=1,
        config=config,
    )

    assert first.failure_class is RetryFailureClass.TRANSIENT
    assert first.status is RetryQueueStatus.RETRYABLE
    assert first.next_retry_at == "2026-08-10T00:01:00+00:00"
    assert exhausted.status is RetryQueueStatus.EXHAUSTED
    assert exhausted.reason_code == "retry_attempts_exhausted"
    assert storage.failure_class is RetryFailureClass.STORAGE_BLOCKED
    assert storage.status is RetryQueueStatus.BLOCKED
    assert storage.consumes_retry_budget is False
    assert invalid_pdf.reason_code == "invalid_pdf"
    assert invalid_pdf.operator_action_required is True
    assert dense_day.reason_code == "unsplittable_window"

    repository = AnnouncementAssetRepository(tmp_path / "retry.db")
    repository.initialize_schema()
    record, instrument_id = _record()
    announcement = repository.upsert_announcement(
        record,
        instrument_id=instrument_id,
        observed_at="2026-08-10T00:00:00+00:00",
    )
    attachment = repository.upsert_attachment(
        announcement.announcement_id,
        record.attachments[0],
        observed_at="2026-08-10T00:00:00+00:00",
    )
    operation, created = repository.create_or_reuse_operation(
        operation_type="attachment_retry_test",
        idempotency_key="retry-operation",
        scope={"attachment_id": attachment.attachment_id},
        policy_version="v1",
    )
    assert created is True
    repository.enqueue_attachment_retry(
        attachment_id=attachment.attachment_id,
        source="cninfo",
        operation_id=operation.operation_id,
        observation_key="observation-1",
        max_attempts=1,
    )
    claimed = repository.claim_attachment_retry(
        attachment.attachment_id, now="2026-08-10T00:00:00+00:00"
    )
    decision = classify_retry_failure(
        "provider timeout",
        attempt=claimed["attempt"],
        config=replace(config, max_attempts=1),
        now="2026-08-10T00:00:00+00:00",
    )
    item = repository.finish_attachment_retry(
        attachment.attachment_id,
        success=False,
        retryable=decision.retryable,
        next_retry_at=decision.next_retry_at,
        error_code=decision.reason_code,
        failure_class=decision.failure_class.value,
        operator_action_required=decision.operator_action_required,
        consumes_retry_budget=decision.consumes_retry_budget,
        max_attempts=1,
    )
    assert item["status"] == "exhausted"
    parent = repository.get_operation(operation.operation_id)
    assert parent is not None
    assert parent.status is OperationStatus.BLOCKED
    assert parent.reason_code == "retry_exhausted"

    unchanged = repository.enqueue_attachment_retry(
        attachment_id=attachment.attachment_id,
        source="cninfo",
        observation_key="observation-1",
        max_attempts=1,
    )
    assert unchanged["status"] == "exhausted"
    reopened = repository.enqueue_attachment_retry(
        attachment_id=attachment.attachment_id,
        source="cninfo",
        observation_key="observation-2",
        max_attempts=1,
    )
    assert reopened["status"] == "queued"
    assert reopened["attempt"] == 0
    with pytest.raises(ValueError, match="requires an actor"):
        repository.enqueue_attachment_retry(
            attachment_id=attachment.attachment_id,
            source="cninfo",
            observation_key="observation-2",
            max_attempts=1,
            reopen_reason="audited_repair",
        )

    blocked = repository.finish_attachment_retry(
        attachment.attachment_id,
        success=False,
        retryable=False,
        error_code="operator_review_required",
        failure_class="operator_action",
        operator_action_required=True,
        max_attempts=1,
    )
    assert blocked["status"] == "blocked"
    due_repair = repository.enqueue_attachment_retry(
        attachment_id=attachment.attachment_id,
        source="cninfo",
        observation_key="observation-2",
        max_attempts=1,
        reopen_reason="due_repair",
    )
    assert due_repair["status"] == "queued"
    blocked_again = repository.finish_attachment_retry(
        attachment.attachment_id,
        success=False,
        retryable=False,
        error_code="operator_review_required",
        failure_class="operator_action",
        operator_action_required=True,
        max_attempts=1,
    )
    assert blocked_again["status"] == "blocked"
    audited_repair = repository.enqueue_attachment_retry(
        attachment_id=attachment.attachment_id,
        source="cninfo",
        observation_key="observation-2",
        max_attempts=1,
        reopen_reason="audited_repair",
        repair_actor="ops@example",
    )
    assert audited_repair["status"] == "queued"


def test_storage_preflight_surfaces_warning_before_hard_stop(tmp_path, monkeypatch):
    config = _config(
        tmp_path,
        storage={
            "warning_utilization": 0.80,
            "hard_stop_utilization": 0.95,
            "free_space_reserve_bytes": 1,
            "max_attachment_bytes": 1024 * 1024,
            "unknown_length_reservation_bytes": 4096,
        },
    )
    store = ContentAddressedBlobStore(config)
    store.prepare()
    monkeypatch.setattr(
        "research.announcement_assets.storage.shutil.disk_usage",
        lambda _: SimpleNamespace(total=1000, used=850, free=150),
    )

    snapshot = store.preflight_capacity(10)

    assert snapshot.warning is True
    assert snapshot.projected_utilization == pytest.approx(0.86)


class _Retriever:
    def __init__(self):
        self.calls = 0

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls += 1
        digest = hashlib.sha256(PDF_BYTES).hexdigest()
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=PDF_BYTES,
            content_hash=digest,
            content_length=len(PDF_BYTES),
            final_url=attachment.resolved_url or attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-03-20T02:00:00+00:00",
            signature_status="valid_pdf",
        )


def test_service_reuses_shared_local_asset_with_zero_second_download(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    record, instrument_id = _record()
    registered = service.register_discovered_record(record, instrument_id=instrument_id)
    asset = service.acquire_attachment(registered[0].attachment_id)
    assert asset is not None
    assert asset.availability is AssetAvailability.LOCAL_VALID
    assert retriever.calls == 1

    same_asset = service.acquire_attachment(registered[0].attachment_id)
    assert same_asset.content_hash == asset.content_hash
    assert retriever.calls == 1

    first_consumer = service.ensure_annual_report(
        EnsureRequest(
            instrument_id=instrument_id,
            fiscal_year=2025,
            allow_network=True,
            consumer="business-profile",
        )
    )
    second_consumer = service.ensure_annual_report(
        EnsureRequest(
            instrument_id=instrument_id,
            fiscal_year=2025,
            allow_network=True,
            consumer="broker",
        )
    )
    assert first_consumer.disposition is EnsureDisposition.LOCAL_HIT
    assert second_consumer.disposition is EnsureDisposition.LOCAL_HIT
    assert retriever.calls == 1


def test_service_persists_pdf_summary_refinement(tmp_path, monkeypatch):
    class Page:
        @staticmethod
        def extract_text():
            return "甲公司 2025 年年度报告摘要"

    class Reader:
        def __init__(self, *_args, **_kwargs):
            self.pages = [Page()]

    import pypdf

    monkeypatch.setattr(pypdf, "PdfReader", Reader)
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_Retriever(),
    )
    record, instrument_id = _record()
    registered = service.register_discovered_record(record, instrument_id=instrument_id)

    assert service.acquire_attachment(registered[0].attachment_id) is None
    attachment = repository.get_attachment(registered[0].attachment_id)
    classification = attachment.metadata["asset_classification"]
    assert classification["is_eligible"] is False
    assert classification["is_full_report"] is False
    assert repository.get_effective_report(instrument_id, 2025) is None


def test_asset_request_subscriptions_share_work_but_isolate_principals(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    scope = {"instrument_id": "600000.SH", "fiscal_year": 2025}

    first_request, first_operation, request_created, operation_created = (
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="global-scope",
            scope=scope,
            policy_version="v1",
            principal="alice",
            request_idempotency_key="request-1",
            request_fingerprint="fingerprint-1",
            consumer="business-profile",
            consumer_continuation_id="consumer-request-1",
            stage=OperationStage.DISCOVERING,
        )
    )
    second_request, second_operation, second_created, second_operation_created = (
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="global-scope",
            scope=scope,
            policy_version="v1",
            principal="bob",
            request_idempotency_key="request-1",
            request_fingerprint="fingerprint-1",
            consumer="broker",
            stage=OperationStage.DISCOVERING,
        )
    )

    assert request_created is True
    assert operation_created is True
    assert second_created is True
    assert second_operation_created is False
    assert first_operation.operation_id == second_operation.operation_id
    assert first_operation.owner is None
    assert first_request.asset_request_id != second_request.asset_request_id

    reused, reused_operation, reused_created, _ = (
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="global-scope",
            scope=scope,
            policy_version="v1",
            principal="alice",
            request_idempotency_key="request-1",
            request_fingerprint="fingerprint-1",
            consumer="business-profile",
            stage=OperationStage.DISCOVERING,
        )
    )
    assert reused.asset_request_id == first_request.asset_request_id
    assert reused_operation.operation_id == first_operation.operation_id
    assert reused_created is False

    with pytest.raises(IdempotencyConflictError):
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="different-global-scope",
            scope={"instrument_id": "600001.SH", "fiscal_year": 2025},
            policy_version="v1",
            principal="alice",
            request_idempotency_key="request-1",
            request_fingerprint="different-fingerprint",
            stage=OperationStage.DISCOVERING,
        )

    cancelled = repository.cancel_asset_request(
        first_request.asset_request_id,
        principal="alice",
    )
    assert cancelled.status is AssetRequestStatus.CANCELLED
    assert cancelled.consumer_continuation_id == "consumer-request-1"
    repeated = repository.cancel_asset_request(
        first_request.asset_request_id,
        principal="alice",
    )
    assert repeated.asset_request_id == first_request.asset_request_id
    assert repeated.status is AssetRequestStatus.CANCELLED
    assert (
        repository.get_asset_request(first_request.asset_request_id, principal="bob")
        is None
    )
    remaining = repository.list_asset_requests(
        operation_id=first_operation.operation_id,
        active_only=True,
    )
    assert [item.asset_request_id for item in remaining] == [
        second_request.asset_request_id
    ]
    assert (
        repository.get_operation(first_operation.operation_id).status
        is OperationStatus.QUEUED
    )
    last_cancelled = repository.cancel_asset_request(
        second_request.asset_request_id,
        principal="bob",
    )
    assert last_cancelled.status is AssetRequestStatus.CANCELLED
    assert repository.list_asset_requests(
        operation_id=first_operation.operation_id,
        active_only=True,
    ) == []
    assert (
        repository.get_operation(first_operation.operation_id).status
        is OperationStatus.QUEUED
    )


def test_change_events_require_explicit_origin_and_dispatch_policy(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    with pytest.raises(ValueError, match="origin and dispatch policy"):
        repository.append_change_event(
            event_key="invalid-origin",
            event_type="added",
            instrument_id="600000.SH",
            fiscal_year=2025,
            asset_id=None,
            predecessor_asset_id=None,
            content_hash=None,
            trigger_origin="unknown",
            dispatch_policy_version="consumer_dispatch.v1",
        )
    event_id = repository.append_change_event(
        event_key="explicit-origin",
        event_type="added",
        instrument_id="600000.SH",
        fiscal_year=2025,
        asset_id=None,
        predecessor_asset_id=None,
        content_hash=None,
        trigger_origin="targeted_repair",
        dispatch_policy_version="consumer_dispatch.v1",
    )
    event = repository.list_change_events(after_event_id=event_id - 1)[0]
    assert event["trigger_origin"] == "targeted_repair"
    assert event["dispatch_policy_version"] == "consumer_dispatch.v1"


def test_due_asset_request_lazily_expires_without_mutating_internal_operation(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    subscription, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="lazy-expiry-operation",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="v1",
        principal="alice",
        request_idempotency_key="lazy-expiry-request",
        request_fingerprint="lazy-expiry-fingerprint",
    )
    assert subscription.expires_at is not None
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE official_asset_operation_subscriptions SET expires_at=? "
            "WHERE asset_request_id=?",
            ("2000-01-01T00:00:00+00:00", subscription.asset_request_id),
        )

    expired = repository.get_asset_request(
        subscription.asset_request_id,
        principal="alice",
    )

    assert expired is not None
    assert expired.status is AssetRequestStatus.EXPIRED
    assert expired.expired_at is not None
    assert expired.tombstone_until is not None
    assert expired.retention_policy_version == "asset_request_retention.v1"
    assert repository.get_operation(operation.operation_id).status is OperationStatus.QUEUED


def test_service_ensure_returns_isolated_request_handles_for_shared_operation(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetService(repository=repository, config=config)

    first = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
            consumer="business-profile",
            idempotency_key="alice-1",
        )
    )
    second = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="bob",
            consumer="broker",
            idempotency_key="bob-1",
        )
    )
    incompatible_integrity = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            integrity_level="signature",
            principal="carol",
            consumer="broker",
            idempotency_key="carol-1",
        )
    )

    assert first.asset_request is not None
    assert second.asset_request is not None
    assert first.operation is not None
    assert second.operation is not None
    assert incompatible_integrity.operation is not None
    assert first.operation.operation_id == second.operation.operation_id
    assert incompatible_integrity.operation.operation_id != first.operation.operation_id
    assert first.operation.scope["acquisition_work_fingerprint"]
    assert first.operation.scope["accepted_bounds"]["max_pages"] > 0
    assert first.asset_request.asset_request_id != second.asset_request.asset_request_id
    assert first.asset_request.principal == "alice"
    assert second.asset_request.principal == "bob"
