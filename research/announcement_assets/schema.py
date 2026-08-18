"""SQLite schema owned by the shared announcement-asset capability."""

from __future__ import annotations

SCHEMA_VERSION = 23

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS official_asset_schema_versions (
    component TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS official_announcements (
    announcement_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    source TEXT NOT NULL,
    source_announcement_id TEXT NOT NULL,
    title TEXT NOT NULL,
    instrument_id TEXT,
    exchange TEXT,
    source_category TEXT,
    published_at TEXT,
    published_at_raw TEXT,
    published_at_precision TEXT,
    raw_payload_hash TEXT NOT NULL,
    first_observed_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    status TEXT NOT NULL,
    provider_diagnostics_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source, source_announcement_id)
);

CREATE INDEX IF NOT EXISTS idx_official_announcements_instrument_time
ON official_announcements(instrument_id, published_at);

CREATE TABLE IF NOT EXISTS official_announcement_attachments (
    attachment_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    announcement_id TEXT NOT NULL,
    attachment_identity TEXT NOT NULL,
    source_attachment_id TEXT,
    source_url TEXT NOT NULL,
    normalized_source_url TEXT NOT NULL,
    name TEXT,
    media_type TEXT,
    content_length_hint INTEGER,
    first_observed_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(announcement_id) REFERENCES official_announcements(announcement_id),
    UNIQUE(announcement_id, attachment_identity)
);

CREATE INDEX IF NOT EXISTS idx_official_attachments_announcement
ON official_announcement_attachments(announcement_id, last_observed_at);

CREATE TABLE IF NOT EXISTS official_document_blobs (
    content_hash TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    content_length INTEGER NOT NULL,
    canonical_path TEXT NOT NULL,
    signature_status TEXT NOT NULL,
    integrity_status TEXT NOT NULL,
    first_available_at TEXT NOT NULL,
    last_verified_at TEXT,
    backup_status TEXT NOT NULL DEFAULT 'unprotected',
    backup_verified_at TEXT,
    acquisition_origin TEXT,
    adopted_from_path TEXT,
    verification_evidence_json TEXT NOT NULL DEFAULT '{}',
    backup_evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(canonical_path)
);

CREATE TABLE IF NOT EXISTS official_attachment_versions (
    version_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    observation_key TEXT NOT NULL,
    content_hash TEXT,
    final_url TEXT,
    retrieval_status TEXT NOT NULL,
    integrity_status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 4,
    next_retry_at TEXT,
    error_code TEXT,
    observed_at TEXT NOT NULL,
    first_observed_at TEXT NOT NULL DEFAULT '',
    last_observed_at TEXT NOT NULL DEFAULT '',
    version_available_at TEXT NOT NULL DEFAULT '',
    available_time_source TEXT NOT NULL DEFAULT 'first_observed',
    available_time_precision TEXT NOT NULL DEFAULT 'instant',
    response_evidence_json TEXT NOT NULL DEFAULT '{}',
    content_length_observed INTEGER,
    content_hash_observed TEXT,
    lease_owner TEXT,
    lease_generation INTEGER,
    temporary_path TEXT,
    temporary_bytes INTEGER,
    quarantine_path TEXT,
    visibility_state TEXT NOT NULL DEFAULT 'production'
        CHECK(visibility_state IN ('shadow', 'production')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(attachment_id) REFERENCES official_announcement_attachments(attachment_id),
    FOREIGN KEY(content_hash) REFERENCES official_document_blobs(content_hash),
    UNIQUE(attachment_id, observation_key)
);

CREATE INDEX IF NOT EXISTS idx_official_attachment_versions_retry
ON official_attachment_versions(retrieval_status, next_retry_at);

CREATE TABLE IF NOT EXISTS official_asset_acquisition_leases (
    attachment_id TEXT PRIMARY KEY,
    lease_owner TEXT NOT NULL,
    lease_generation INTEGER NOT NULL DEFAULT 1,
    lease_expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(attachment_id) REFERENCES official_announcement_attachments(attachment_id)
);

CREATE TABLE IF NOT EXISTS effective_annual_reports (
    asset_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    report_period TEXT NOT NULL,
    announcement_id TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    content_hash TEXT,
    source TEXT NOT NULL,
    source_announcement_id TEXT NOT NULL,
    published_at TEXT,
    document_family TEXT NOT NULL DEFAULT 'annual_report',
    variant TEXT NOT NULL,
    is_full_report INTEGER NOT NULL DEFAULT 1 CHECK(is_full_report=1),
    classifier_version TEXT NOT NULL,
    decision_state TEXT NOT NULL,
    availability TEXT NOT NULL,
    predecessor_asset_id TEXT,
    pending_candidate_id TEXT,
    activated_at TEXT,
    last_checked_at TEXT NOT NULL,
    decision_reasons_json TEXT NOT NULL DEFAULT '[]',
    decision_evidence_json TEXT NOT NULL DEFAULT '{}',
    equivalent_source_filings_json TEXT NOT NULL DEFAULT '[]',
    canonical_projection_policy_version TEXT NOT NULL DEFAULT 'canonical_source_filing.v1',
    evidence_set_hash TEXT,
    visibility_state TEXT NOT NULL DEFAULT 'production'
        CHECK(visibility_state IN ('shadow', 'production')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(announcement_id) REFERENCES official_announcements(announcement_id),
    FOREIGN KEY(attachment_id) REFERENCES official_announcement_attachments(attachment_id),
    FOREIGN KEY(version_id) REFERENCES official_attachment_versions(version_id),
    FOREIGN KEY(content_hash) REFERENCES official_document_blobs(content_hash),
    UNIQUE(instrument_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_effective_annual_reports_lookup
ON effective_annual_reports(instrument_id, report_period, visibility_state, decision_state);

CREATE TABLE IF NOT EXISTS official_asset_adoption_promotion_gates (
    gate_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    inventory_fingerprint TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_length INTEGER NOT NULL CHECK(content_length > 0),
    canonical_path TEXT NOT NULL,
    mount_filesystem_key TEXT NOT NULL,
    custody_state TEXT NOT NULL
        CHECK(custody_state IN ('canonical', 'shared_controlled_legacy')),
    status TEXT NOT NULL CHECK(status IN ('ready', 'consumed', 'invalidated')),
    reconciled_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    invalidated_at TEXT,
    invalidation_reason TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(content_hash) REFERENCES official_document_blobs(content_hash)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_adoption_gates_asset
ON official_asset_adoption_promotion_gates(asset_id, status, reconciled_at);

CREATE TABLE IF NOT EXISTS official_asset_operations (
    operation_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    bounds_json TEXT NOT NULL DEFAULT '{}',
    policy_version TEXT NOT NULL,
    config_version TEXT,
    owner TEXT,
    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'completed', 'missing', 'failed', 'blocked', 'cancelled')),
    stage TEXT CHECK(stage IS NULL OR stage IN (
        'not_applicable', 'discovering', 'reconciling', 'adopting',
        'downloading', 'validating', 'activating', 'deleting',
        'backing_up', 'restoring', 'auditing'
    )),
    stage_schema_version TEXT NOT NULL DEFAULT 'official_asset_operation_stage.v1',
    outcome TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    resume_generation INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    progress_json TEXT NOT NULL DEFAULT '{}',
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    result_asset_id TEXT,
    result_origin TEXT,
    reason_code TEXT,
    diagnostics_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_official_asset_operation_active
ON official_asset_operations(idempotency_key)
WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS idx_official_asset_operations_status_retry
ON official_asset_operations(status, next_retry_at, updated_at);

CREATE TABLE IF NOT EXISTS official_asset_operation_subscriptions (
    asset_request_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    principal TEXT NOT NULL,
    consumer TEXT,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    authorized_projection_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL CHECK(status IN ('active', 'cancelled', 'expired')),
    consumer_continuation_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    cancelled_at TEXT,
    expires_at TEXT,
    expired_at TEXT,
    tombstone_until TEXT,
    retention_policy_version TEXT NOT NULL DEFAULT 'asset_request_retention.v1',
    FOREIGN KEY(operation_id) REFERENCES official_asset_operations(operation_id),
    UNIQUE(principal, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_operation_subscriptions_operation
ON official_asset_operation_subscriptions(operation_id, status, updated_at);

CREATE TABLE IF NOT EXISTS official_asset_consumer_requests (
    consumer_request_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    principal TEXT NOT NULL,
    consumer TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    processing_fingerprint TEXT NOT NULL,
    selector_json TEXT NOT NULL,
    asset_request_id TEXT,
    asset_id TEXT,
    processing_id TEXT,
    status TEXT NOT NULL CHECK(status IN (
        'pending_asset', 'not_started', 'queued', 'processing', 'completed',
        'failed', 'missing', 'blocked', 'cancelled', 'expired'
    )),
    result_state TEXT NOT NULL CHECK(result_state IN (
        'unavailable', 'current', 'stale', 'reprocessing'
    )),
    result_identity TEXT,
    resolved_source TEXT,
    resolved_source_announcement_id TEXT,
    resolved_attachment_id TEXT,
    resolved_observation_version TEXT,
    resolved_content_hash TEXT,
    resolved_report_period TEXT,
    reason_code TEXT,
    retry_metadata_json TEXT NOT NULL DEFAULT '{}',
    diagnostics_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    processing_started_at TEXT,
    finished_at TEXT,
    stop_requested_at TEXT,
    cancelled_at TEXT,
    expires_at TEXT NOT NULL,
    expired_at TEXT,
    tombstone_until TEXT,
    retention_policy_version TEXT NOT NULL DEFAULT 'consumer_request_retention.v1',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(asset_request_id)
        REFERENCES official_asset_operation_subscriptions(asset_request_id),
    UNIQUE(principal, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_consumer_requests_owner
ON official_asset_consumer_requests(principal, status, updated_at);

CREATE INDEX IF NOT EXISTS idx_official_asset_consumer_requests_asset_request
ON official_asset_consumer_requests(asset_request_id, status, updated_at);

CREATE TABLE IF NOT EXISTS official_asset_change_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    schema_version TEXT NOT NULL DEFAULT 'official_asset_change_event.v1',
    event_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    asset_id TEXT,
    predecessor_asset_id TEXT,
    content_hash TEXT,
    trigger_origin TEXT NOT NULL DEFAULT 'unknown',
    dispatch_policy_version TEXT NOT NULL DEFAULT 'consumer_dispatch.v1',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_official_asset_change_events_scope
ON official_asset_change_events(instrument_id, fiscal_year, event_id);

CREATE TABLE IF NOT EXISTS official_annual_report_decisions (
    decision_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    decision_kind TEXT NOT NULL CHECK(decision_kind IN (
        'initial_activation', 'replacement', 'projection_update',
        'migration_snapshot', 'withdrawn_without_replacement'
    )),
    predecessor_asset_id TEXT,
    predecessor_source TEXT,
    predecessor_source_announcement_id TEXT,
    predecessor_announcement_id TEXT,
    predecessor_attachment_id TEXT,
    predecessor_version_id TEXT,
    predecessor_content_hash TEXT,
    replacement_asset_id TEXT,
    replacement_source TEXT,
    replacement_source_announcement_id TEXT,
    replacement_announcement_id TEXT,
    replacement_attachment_id TEXT,
    replacement_version_id TEXT,
    replacement_content_hash TEXT,
    decision_state TEXT NOT NULL,
    classifier_version TEXT NOT NULL,
    decision_policy_version TEXT NOT NULL,
    decision_reasons_json TEXT NOT NULL DEFAULT '[]',
    decision_evidence_json TEXT NOT NULL DEFAULT '{}',
    activated_at TEXT NOT NULL,
    outbox_event_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    CHECK(
        (
            decision_kind = 'withdrawn_without_replacement'
            AND predecessor_asset_id IS NOT NULL
            AND predecessor_source IS NOT NULL
            AND predecessor_source_announcement_id IS NOT NULL
            AND predecessor_announcement_id IS NOT NULL
            AND predecessor_attachment_id IS NOT NULL
            AND predecessor_version_id IS NOT NULL
            AND predecessor_content_hash IS NOT NULL
            AND replacement_asset_id IS NULL
            AND replacement_source IS NULL
            AND replacement_source_announcement_id IS NULL
            AND replacement_announcement_id IS NULL
            AND replacement_attachment_id IS NULL
            AND replacement_version_id IS NULL
            AND replacement_content_hash IS NULL
            AND decision_state = 'withdrawn'
        ) OR (
            decision_kind IN ('initial_activation', 'migration_snapshot')
            AND predecessor_asset_id IS NULL
            AND predecessor_source IS NULL
            AND predecessor_source_announcement_id IS NULL
            AND predecessor_announcement_id IS NULL
            AND predecessor_attachment_id IS NULL
            AND predecessor_version_id IS NULL
            AND predecessor_content_hash IS NULL
            AND replacement_asset_id IS NOT NULL
            AND replacement_source IS NOT NULL
            AND replacement_source_announcement_id IS NOT NULL
            AND replacement_announcement_id IS NOT NULL
            AND replacement_attachment_id IS NOT NULL
            AND replacement_version_id IS NOT NULL
            AND replacement_content_hash IS NOT NULL
        ) OR (
            decision_kind IN ('replacement', 'projection_update')
            AND predecessor_asset_id IS NOT NULL
            AND predecessor_source IS NOT NULL
            AND predecessor_source_announcement_id IS NOT NULL
            AND predecessor_announcement_id IS NOT NULL
            AND predecessor_attachment_id IS NOT NULL
            AND predecessor_version_id IS NOT NULL
            AND predecessor_content_hash IS NOT NULL
            AND replacement_asset_id IS NOT NULL
            AND replacement_source IS NOT NULL
            AND replacement_source_announcement_id IS NOT NULL
            AND replacement_announcement_id IS NOT NULL
            AND replacement_attachment_id IS NOT NULL
            AND replacement_version_id IS NOT NULL
            AND replacement_content_hash IS NOT NULL
        )
    ),
    FOREIGN KEY(outbox_event_key)
        REFERENCES official_asset_change_events(event_key)
);

CREATE INDEX IF NOT EXISTS idx_official_annual_report_decisions_scope
ON official_annual_report_decisions(instrument_id, fiscal_year, decision_sequence);

CREATE TRIGGER IF NOT EXISTS prevent_official_annual_report_decision_update
BEFORE UPDATE ON official_annual_report_decisions
BEGIN
    SELECT RAISE(ABORT, 'official annual report decisions are append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_official_annual_report_decision_delete
BEFORE DELETE ON official_annual_report_decisions
BEGIN
    SELECT RAISE(ABORT, 'official annual report decisions are append-only');
END;

-- Pre-v10 decision tables do not carry the stronger inline CHECK above.  The
-- insert trigger therefore keeps clean and migrated catalogs on one contract.
CREATE TRIGGER IF NOT EXISTS validate_official_annual_report_decision_insert
BEFORE INSERT ON official_annual_report_decisions
WHEN NOT (
    (
        NEW.decision_kind = 'withdrawn_without_replacement'
        AND NEW.predecessor_asset_id IS NOT NULL
        AND NEW.predecessor_source IS NOT NULL
        AND NEW.predecessor_source_announcement_id IS NOT NULL
        AND NEW.predecessor_announcement_id IS NOT NULL
        AND NEW.predecessor_attachment_id IS NOT NULL
        AND NEW.predecessor_version_id IS NOT NULL
        AND NEW.predecessor_content_hash IS NOT NULL
        AND NEW.replacement_asset_id IS NULL
        AND NEW.replacement_source IS NULL
        AND NEW.replacement_source_announcement_id IS NULL
        AND NEW.replacement_announcement_id IS NULL
        AND NEW.replacement_attachment_id IS NULL
        AND NEW.replacement_version_id IS NULL
        AND NEW.replacement_content_hash IS NULL
        AND NEW.decision_state = 'withdrawn'
    ) OR (
        NEW.decision_kind IN ('initial_activation', 'migration_snapshot')
        AND NEW.predecessor_asset_id IS NULL
        AND NEW.predecessor_source IS NULL
        AND NEW.predecessor_source_announcement_id IS NULL
        AND NEW.predecessor_announcement_id IS NULL
        AND NEW.predecessor_attachment_id IS NULL
        AND NEW.predecessor_version_id IS NULL
        AND NEW.predecessor_content_hash IS NULL
        AND NEW.replacement_asset_id IS NOT NULL
        AND NEW.replacement_source IS NOT NULL
        AND NEW.replacement_source_announcement_id IS NOT NULL
        AND NEW.replacement_announcement_id IS NOT NULL
        AND NEW.replacement_attachment_id IS NOT NULL
        AND NEW.replacement_version_id IS NOT NULL
        AND NEW.replacement_content_hash IS NOT NULL
    ) OR (
        NEW.decision_kind IN ('replacement', 'projection_update')
        AND NEW.predecessor_asset_id IS NOT NULL
        AND NEW.predecessor_source IS NOT NULL
        AND NEW.predecessor_source_announcement_id IS NOT NULL
        AND NEW.predecessor_announcement_id IS NOT NULL
        AND NEW.predecessor_attachment_id IS NOT NULL
        AND NEW.predecessor_version_id IS NOT NULL
        AND NEW.predecessor_content_hash IS NOT NULL
        AND NEW.replacement_asset_id IS NOT NULL
        AND NEW.replacement_source IS NOT NULL
        AND NEW.replacement_source_announcement_id IS NOT NULL
        AND NEW.replacement_announcement_id IS NOT NULL
        AND NEW.replacement_attachment_id IS NOT NULL
        AND NEW.replacement_version_id IS NOT NULL
        AND NEW.replacement_content_hash IS NOT NULL
    )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid effective decision lineage shape');
END;

-- One durable delivery cursor per registered consumer.  The cursor is
-- intentionally independent from the global event outbox: an offline
-- consumer can resume from its own position without rediscovery.
CREATE TABLE IF NOT EXISTS official_asset_consumer_checkpoints (
    consumer TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    last_event_id INTEGER NOT NULL DEFAULT 0,
    last_event_key TEXT,
    last_attempted_event_id INTEGER,
    delivery_attempt INTEGER NOT NULL DEFAULT 0,
    last_attempted_at TEXT,
    last_delivered_at TEXT,
    last_error_code TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_official_asset_consumer_checkpoints_delivery
ON official_asset_consumer_checkpoints(last_event_id, updated_at);

CREATE TABLE IF NOT EXISTS official_asset_deletion_intents (
    deletion_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    blob_hash TEXT NOT NULL,
    managed_path TEXT NOT NULL,
    predecessor_asset_id TEXT,
    replacement_asset_id TEXT,
    replacement_blob_hash TEXT,
    decision_id TEXT,
    outbox_event_key TEXT,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    error_code TEXT,
    operation_mount_source TEXT,
    operation_mount_point TEXT,
    operation_mount_fs_type TEXT,
    operation_mount_device_id INTEGER,
    operation_mount_filesystem_key TEXT,
    operation_mount_captured_at TEXT,
    recovery_pair_id TEXT,
    recovery_pin_id TEXT,
    recovery_manifest_id TEXT,
    required_set_released_at TEXT,
    planned_at TEXT NOT NULL,
    deleting_at TEXT,
    deleted_at TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(blob_hash) REFERENCES official_document_blobs(content_hash),
    FOREIGN KEY(decision_id) REFERENCES official_annual_report_decisions(decision_id),
    FOREIGN KEY(outbox_event_key) REFERENCES official_asset_change_events(event_key)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_deletion_pending
ON official_asset_deletion_intents(status, updated_at);

CREATE TABLE IF NOT EXISTS official_asset_deletion_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    deletion_id TEXT NOT NULL,
    status TEXT NOT NULL,
    blob_hash TEXT NOT NULL,
    managed_path TEXT NOT NULL,
    predecessor_asset_id TEXT,
    replacement_asset_id TEXT,
    replacement_blob_hash TEXT,
    reason TEXT NOT NULL,
    retention_evidence_json TEXT NOT NULL DEFAULT '{}',
    actor TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY(deletion_id) REFERENCES official_asset_deletion_intents(deletion_id)
);

CREATE TABLE IF NOT EXISTS official_asset_recovery_manifest (
    recovery_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    manifest_kind TEXT NOT NULL,
    manifest_version INTEGER NOT NULL CHECK(manifest_version=1),
    predecessor_asset_id TEXT,
    source TEXT,
    source_announcement_id TEXT,
    attachment_id TEXT,
    version_id TEXT,
    prior_path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    replacement_asset_id TEXT,
    replacement_content_hash TEXT,
    backup_object TEXT NOT NULL,
    file_manifest_watermark TEXT NOT NULL,
    recovery_pair_id TEXT NOT NULL UNIQUE,
    catalog_snapshot_watermark TEXT,
    consumer TEXT,
    active_indefinitely INTEGER NOT NULL DEFAULT 1 CHECK(active_indefinitely=1),
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(manifest_kind, prior_path, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_recovery_required_blob
ON official_asset_recovery_manifest(content_hash, active_indefinitely);

CREATE TRIGGER IF NOT EXISTS prevent_official_asset_recovery_manifest_update
BEFORE UPDATE ON official_asset_recovery_manifest
BEGIN
    SELECT RAISE(ABORT, 'recovery manifest is immutable');
END;

CREATE TRIGGER IF NOT EXISTS prevent_official_asset_recovery_manifest_delete
BEFORE DELETE ON official_asset_recovery_manifest
BEGIN
    SELECT RAISE(ABORT, 'recovery manifest is immutable');
END;

CREATE TABLE IF NOT EXISTS official_asset_recovery_pair_closures (
    closure_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    recovery_pair_id TEXT NOT NULL UNIQUE,
    recovery_id TEXT NOT NULL UNIQUE,
    catalog_snapshot_identity TEXT NOT NULL,
    catalog_snapshot_hash TEXT NOT NULL,
    file_manifest_watermark TEXT NOT NULL,
    verified_at TEXT NOT NULL,
    verified_by TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(recovery_pair_id)
        REFERENCES official_asset_recovery_manifest(recovery_pair_id),
    FOREIGN KEY(recovery_id)
        REFERENCES official_asset_recovery_manifest(recovery_id)
);

CREATE TRIGGER IF NOT EXISTS validate_official_asset_recovery_pair_closure
BEFORE INSERT ON official_asset_recovery_pair_closures
WHEN NOT EXISTS (
    SELECT 1 FROM official_asset_recovery_manifest manifest
    WHERE manifest.recovery_id=NEW.recovery_id
      AND manifest.recovery_pair_id=NEW.recovery_pair_id
      AND manifest.file_manifest_watermark=NEW.file_manifest_watermark
)
BEGIN
    SELECT RAISE(ABORT, 'recovery pair closure does not match manifest');
END;

CREATE TRIGGER IF NOT EXISTS prevent_official_asset_recovery_pair_closure_update
BEFORE UPDATE ON official_asset_recovery_pair_closures
BEGIN
    SELECT RAISE(ABORT, 'recovery pair closure is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_official_asset_recovery_pair_closure_delete
BEFORE DELETE ON official_asset_recovery_pair_closures
BEGIN
    SELECT RAISE(ABORT, 'recovery pair closure is append-only');
END;

CREATE TABLE IF NOT EXISTS official_asset_backup_recovery_journal (
    journal_entry_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    journal_sequence INTEGER NOT NULL UNIQUE CHECK(journal_sequence > 0),
    increment_kind TEXT NOT NULL,
    increment_identity TEXT NOT NULL,
    source_catalog_generation TEXT NOT NULL,
    predecessor_watermark TEXT,
    coverage_watermark TEXT NOT NULL UNIQUE,
    integrity_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    UNIQUE(increment_kind, increment_identity)
);

CREATE TRIGGER IF NOT EXISTS prevent_official_asset_recovery_journal_update
BEFORE UPDATE ON official_asset_backup_recovery_journal
BEGIN
    SELECT RAISE(ABORT, 'recovery journal is append-only');
END;

CREATE TRIGGER IF NOT EXISTS prevent_official_asset_recovery_journal_delete
BEFORE DELETE ON official_asset_backup_recovery_journal
BEGIN
    SELECT RAISE(ABORT, 'recovery journal is append-only');
END;

CREATE TABLE IF NOT EXISTS official_asset_retention_pins (
    pin_id TEXT PRIMARY KEY,
    blob_hash TEXT NOT NULL,
    pin_type TEXT NOT NULL,
    pin_key TEXT NOT NULL,
    owner TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    released_at TEXT,
    blocks_primary_unlink INTEGER NOT NULL DEFAULT 1
        CHECK(blocks_primary_unlink IN (0, 1)),
    required_set_hold INTEGER NOT NULL DEFAULT 0
        CHECK(required_set_hold IN (0, 1)),
    required_set_released_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(blob_hash) REFERENCES official_document_blobs(content_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_official_asset_active_pin
ON official_asset_retention_pins(blob_hash, pin_type, pin_key)
WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS official_asset_consumer_processing (
    processing_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    consumer TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    parameter_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    derived_identity TEXT,
    error_code TEXT,
    canonical_projection_policy_version TEXT,
    evidence_set_hash TEXT,
    equivalent_source_filings_json TEXT NOT NULL DEFAULT '[]',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 4,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(asset_id, consumer, parser_version, parameter_hash)
);

CREATE TABLE IF NOT EXISTS official_asset_discovery_state (
    source TEXT NOT NULL,
    exchange TEXT NOT NULL,
    category TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    schema_version TEXT NOT NULL DEFAULT 'official_asset_discovery_state.v2',
    item_cursor_kind TEXT,
    item_cursor_value TEXT,
    covered_until TEXT,
    run_cutoff TEXT,
    next_page INTEGER,
    status TEXT NOT NULL,
    is_complete INTEGER NOT NULL DEFAULT 0,
    gap_reason TEXT,
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    lease_owner TEXT,
    lease_expires_at TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0,
    state_version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(source, exchange, category, scope_key, config_fingerprint)
);

CREATE TABLE IF NOT EXISTS official_asset_attachment_retries (
    attachment_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    operation_id TEXT,
    observation_key TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 4,
    next_retry_at TEXT,
    last_error_code TEXT,
    failure_class TEXT,
    operator_action_required INTEGER NOT NULL DEFAULT 0,
    consumes_retry_budget INTEGER NOT NULL DEFAULT 1,
    reopen_reason TEXT,
    reopened_at TEXT,
    repair_actor TEXT,
    first_queued_at TEXT NOT NULL,
    last_attempted_at TEXT,
    completed_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL,
    FOREIGN KEY(attachment_id) REFERENCES official_announcement_attachments(attachment_id),
    FOREIGN KEY(operation_id) REFERENCES official_asset_operations(operation_id)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_attachment_retries_due
ON official_asset_attachment_retries(status, next_retry_at, first_queued_at);

CREATE TABLE IF NOT EXISTS official_asset_period_reconciliation (
    instrument_id TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_reconciled_at TEXT,
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(instrument_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_period_reconciliation_due
ON official_asset_period_reconciliation(status, next_retry_at, last_reconciled_at);

CREATE TABLE IF NOT EXISTS official_asset_job_command_audit (
    command_id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT,
    command TEXT NOT NULL,
    principal TEXT NOT NULL,
    effective_permission TEXT NOT NULL,
    trigger_kind TEXT NOT NULL,
    config_version TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES official_asset_operations(operation_id)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_job_command_audit_operation
ON official_asset_job_command_audit(operation_id, command_id);

CREATE TABLE IF NOT EXISTS official_asset_operational_reports (
    report_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    report_kind TEXT NOT NULL,
    operation_id TEXT,
    scope_key TEXT NOT NULL DEFAULT 'global',
    config_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES official_asset_operations(operation_id)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_operational_reports_recent
ON official_asset_operational_reports(report_kind, scope_key, generated_at DESC);

CREATE TABLE IF NOT EXISTS official_asset_coverage (
    universe_snapshot_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    fiscal_year INTEGER,
    status TEXT NOT NULL,
    as_of TEXT NOT NULL,
    expected_fiscal_year INTEGER,
    earliest_search_year INTEGER,
    evidence_expires_at TEXT,
    last_reconciled_at TEXT,
    retry_at TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(universe_snapshot_id, instrument_id)
);

-- A bootstrap is a bounded evidence population, rather than a collection of
-- independent daily discovery cursors.  Keep its identity durable so a
-- resumed worker cannot silently mix a new cutoff or query policy with old
-- checkpoints.
CREATE TABLE IF NOT EXISTS official_asset_bootstrap_runs (
    operation_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL DEFAULT 'official_asset_bootstrap_run.v1',
    universe_snapshot_id TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    as_of TEXT NOT NULL,
    evidence_visibility_cutoff TEXT NOT NULL,
    query_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK(status IN ('running', 'partial', 'success', 'blocked', 'failed')),
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(universe_snapshot_id, as_of, query_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_bootstrap_runs_status
ON official_asset_bootstrap_runs(status, updated_at);

CREATE TABLE IF NOT EXISTS official_asset_universe_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    master_data_version TEXT,
    master_data_last_success_at TEXT,
    snapshot_at TEXT NOT NULL,
    freshness_limit_seconds INTEGER NOT NULL,
    status TEXT NOT NULL,
    source_complete INTEGER NOT NULL DEFAULT 0,
    indeterminate_count INTEGER NOT NULL DEFAULT 0,
    eligible_count INTEGER NOT NULL DEFAULT 0,
    instrument_rows_json TEXT NOT NULL DEFAULT '[]',
    indeterminate_rows_json TEXT NOT NULL DEFAULT '[]',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    paired_census_snapshot_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_official_asset_universe_latest
ON official_asset_universe_snapshots(snapshot_at DESC, status);

CREATE TABLE IF NOT EXISTS official_asset_listed_security_census_snapshots (
    census_snapshot_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    source TEXT NOT NULL,
    query_boundary_json TEXT NOT NULL DEFAULT '{}',
    completeness_watermark TEXT NOT NULL,
    source_version TEXT NOT NULL,
    snapshot_at TEXT NOT NULL,
    raw_payload_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('complete', 'partial', 'failed')),
    instrument_rows_json TEXT NOT NULL DEFAULT '[]',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_official_asset_census_latest
ON official_asset_listed_security_census_snapshots(snapshot_at DESC, status);

CREATE TABLE IF NOT EXISTS official_asset_storage_reservations (
    reservation_id TEXT PRIMARY KEY,
    filesystem_key TEXT NOT NULL,
    operation_id TEXT,
    planned_bytes INTEGER NOT NULL,
    actual_bytes INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_official_asset_reservations_active
ON official_asset_storage_reservations(filesystem_key, status, lease_expires_at);

CREATE TABLE IF NOT EXISTS official_asset_capacity_override_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    authorization_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    principal TEXT NOT NULL,
    permission_scope TEXT NOT NULL,
    max_bytes INTEGER NOT NULL,
    requested_bytes INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES official_asset_operations(operation_id),
    FOREIGN KEY(attachment_id) REFERENCES official_announcement_attachments(attachment_id)
);

CREATE INDEX IF NOT EXISTS idx_official_asset_capacity_override_operation
ON official_asset_capacity_override_audit(operation_id, attachment_id, audit_id);

CREATE TABLE IF NOT EXISTS official_asset_backup_state (
    content_hash TEXT PRIMARY KEY,
    config_fingerprint TEXT NOT NULL,
    destination_identity TEXT NOT NULL,
    failure_domain TEXT,
    backup_path TEXT,
    content_length INTEGER NOT NULL,
    status TEXT NOT NULL,
    file_manifest_watermark TEXT,
    catalog_snapshot_watermark TEXT,
    verified_at TEXT,
    error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(content_hash) REFERENCES official_document_blobs(content_hash)
);

CREATE TABLE IF NOT EXISTS official_asset_storage_artifact_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_type TEXT NOT NULL,
    managed_path TEXT NOT NULL,
    status TEXT NOT NULL,
    actor TEXT NOT NULL,
    owner TEXT,
    lease_generation TEXT,
    operation_id TEXT,
    attachment_id TEXT,
    content_hash TEXT,
    actual_bytes INTEGER,
    reason TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_official_asset_storage_artifact_audit_path
ON official_asset_storage_artifact_audit(managed_path, audit_id);

CREATE TABLE IF NOT EXISTS official_asset_legacy_path_manifest (
    legacy_path TEXT PRIMARY KEY,
    consumer TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    manifest_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    verified_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
"""


OWNED_TABLES = (
    "official_asset_schema_versions",
    "official_announcements",
    "official_announcement_attachments",
    "official_document_blobs",
    "official_attachment_versions",
    "official_asset_acquisition_leases",
    "effective_annual_reports",
    "official_asset_adoption_promotion_gates",
    "official_annual_report_decisions",
    "official_asset_operations",
    "official_asset_operation_subscriptions",
    "official_asset_consumer_requests",
    "official_asset_change_events",
    "official_asset_consumer_checkpoints",
    "official_asset_deletion_intents",
    "official_asset_deletion_audit",
    "official_asset_recovery_manifest",
    "official_asset_recovery_pair_closures",
    "official_asset_backup_recovery_journal",
    "official_asset_retention_pins",
    "official_asset_consumer_processing",
    "official_asset_discovery_state",
    "official_asset_attachment_retries",
    "official_asset_period_reconciliation",
    "official_asset_job_command_audit",
    "official_asset_operational_reports",
    "official_asset_coverage",
    "official_asset_bootstrap_runs",
    "official_asset_universe_snapshots",
    "official_asset_listed_security_census_snapshots",
    "official_asset_storage_reservations",
    "official_asset_capacity_override_audit",
    "official_asset_backup_state",
    "official_asset_storage_artifact_audit",
    "official_asset_legacy_path_manifest",
)
