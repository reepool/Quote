"""SQLite repository for canonical official announcement assets."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from research.announcements import AnnouncementAttachment, AnnouncementRecord

from .models import (
    ACTIVE_OPERATION_STATUSES,
    ANNOUNCEMENT_SCHEMA_VERSION,
    ATTACHMENT_SCHEMA_VERSION,
    ATTACHMENT_VERSION_SCHEMA_VERSION,
    BLOB_SCHEMA_VERSION,
    BOOTSTRAP_RUN_SCHEMA_VERSION,
    DISCOVERY_STATE_SCHEMA_VERSION,
    EFFECTIVE_ANNUAL_REPORT_SCHEMA_VERSION,
    EFFECTIVE_DECISION_SCHEMA_VERSION,
    OPERATION_SCHEMA_VERSION,
    OPERATION_STAGE_SCHEMA_VERSION,
    OPERATION_SUBSCRIPTION_SCHEMA_VERSION,
    AnnualReportVariant,
    AssetAvailability,
    AssetOperation,
    AssetOperationSubscription,
    AssetRequestStatus,
    BatchOutcome,
    EffectiveAnnualReport,
    EffectiveAnnualReportDecision,
    EffectiveDecisionKind,
    EffectiveDecisionState,
    IntegrityStatus,
    OfficialAnnouncement,
    OfficialAnnouncementAttachment,
    OfficialAttachmentVersion,
    OfficialDocumentBlob,
    OperationStage,
    OperationStatus,
    ResultOrigin,
    SourceFilingEvidence,
    canonical_json,
    normalize_instrument_id,
    normalize_source,
    normalize_source_url,
    stable_id,
    utc_now_iso,
)
from .schema import OBSOLETE_COLUMNS, OBSOLETE_TABLES, SCHEMA_SQL, SCHEMA_VERSION

_ALLOWED_OPERATION_TRANSITIONS = {
    OperationStatus.QUEUED: {
        OperationStatus.RUNNING,
        OperationStatus.CANCELLED,
        OperationStatus.BLOCKED,
    },
    OperationStatus.RUNNING: {
        OperationStatus.COMPLETED,
        OperationStatus.MISSING,
        OperationStatus.FAILED,
        OperationStatus.BLOCKED,
        OperationStatus.CANCELLED,
    },
    OperationStatus.BLOCKED: {OperationStatus.QUEUED},
    OperationStatus.FAILED: {OperationStatus.QUEUED},
    OperationStatus.MISSING: {OperationStatus.QUEUED},
    OperationStatus.CANCELLED: set(),
    OperationStatus.COMPLETED: set(),
}

_UNSET = object()
ASSET_REQUEST_RETENTION_POLICY_VERSION = "asset_request_retention.v1"
ASSET_REQUEST_ACTIVE_TTL_SECONDS = 7 * 24 * 3600
ASSET_REQUEST_TOMBSTONE_TTL_SECONDS = 30 * 24 * 3600
READ_LEASE_PIN_TYPE = "active_reader"
CHANGE_EVENT_POLICY_VERSION = "asset_change_event.v1"


_CONFIRMED_MISSING_REQUIRED_SCOPE_FIELDS = (
    "source",
    "exchange",
    "normalized_category",
    "query_bounds",
    "successful_empty_completion_watermark",
    "page_or_subscope_completion",
)


def _confirmed_missing_evidence_error(
    evidence: Mapping[str, Any], evidence_expires_at: str | None = None
) -> str | None:
    """Return a stable validation error for terminal missing evidence.

    This is intentionally strict.  A missing report is a negative fact and
    must be reproducible after restart; accepting a partial JSON payload here
    would turn a provider outage into permanent coverage credit.
    """
    required = (
        "required_route_scope_set",
        "listing_evidence",
        "bootstrap_as_of",
        "evidence_visibility_cutoff",
        "confirmed_at",
        "route_capability_fingerprint",
        "query_policy_fingerprint",
        "classifier_fingerprint",
        "eligibility_fingerprint",
        "underlying_evidence_references",
    )
    missing = [key for key in required if not evidence.get(key)]
    if missing:
        return "confirmed_missing evidence missing: " + ",".join(missing)
    scopes = evidence.get("required_route_scope_set")
    if not isinstance(scopes, list) or not scopes:
        return "confirmed_missing required_route_scope_set must be non-empty"
    for index, scope in enumerate(scopes):
        if not isinstance(scope, Mapping):
            return f"confirmed_missing route scope {index} is not an object"
        missing_scope = [
            key
            for key in _CONFIRMED_MISSING_REQUIRED_SCOPE_FIELDS
            if not scope.get(key)
        ]
        if missing_scope:
            return f"confirmed_missing route scope {index} missing: " + ",".join(
                missing_scope
            )
        completion = scope.get("page_or_subscope_completion")
        if not isinstance(completion, Mapping) or not completion.get("complete"):
            return f"confirmed_missing route scope {index} is not complete"
    listing = evidence.get("listing_evidence")
    if not isinstance(listing, Mapping) or not listing.get("instrument_id"):
        return "confirmed_missing listing evidence lacks instrument identity"
    references = evidence.get("underlying_evidence_references")
    if not isinstance(references, Mapping):
        return "confirmed_missing underlying_evidence_references must be an object"
    for name in ("source_responses", "coverage_checkpoints", "route_equivalence"):
        if not references.get(name):
            return f"confirmed_missing evidence references missing: {name}"
    expiry = evidence.get("evidence_expires_at") or evidence_expires_at
    if not expiry:
        return "confirmed_missing evidence expiry is required"
    return None


class IdempotencyConflictError(ValueError):
    """A principal reused an idempotency key for a different request."""


class DiscoveryStateFenceError(RuntimeError):
    """A discovery worker attempted to commit an obsolete leased state."""


class DiscoveryRetryNotDueError(RuntimeError):
    """A discovery retry was claimed before its durable due time."""


class DiscoveryRetryBlockedError(RuntimeError):
    """A terminal discovery retry requires governed reopen evidence."""


class BootstrapRunIdentityError(ValueError):
    """A bootstrap operation was resumed with a different evidence population."""


_UPSERT_EFFECTIVE_SQL = """
INSERT INTO effective_annual_reports(
    asset_id, schema_version, instrument_id, fiscal_year,
    report_period, announcement_id, attachment_id, version_id,
    content_hash, source, source_announcement_id, published_at,
    document_family, variant, is_full_report, classifier_version,
    decision_state, availability,
    predecessor_asset_id, pending_candidate_id, activated_at,
    last_checked_at, decision_reasons_json, decision_evidence_json,
    equivalent_source_filings_json, canonical_projection_policy_version,
    evidence_set_hash, visibility_state, created_at, updated_at
) VALUES(
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?
)
ON CONFLICT(instrument_id, fiscal_year) DO UPDATE SET
    asset_id=excluded.asset_id,
    report_period=excluded.report_period,
    announcement_id=excluded.announcement_id,
    attachment_id=excluded.attachment_id,
    version_id=excluded.version_id,
    content_hash=excluded.content_hash,
    source=excluded.source,
    source_announcement_id=excluded.source_announcement_id,
    published_at=excluded.published_at,
    document_family=excluded.document_family,
    variant=excluded.variant,
    is_full_report=excluded.is_full_report,
    classifier_version=excluded.classifier_version,
    decision_state=excluded.decision_state,
    availability=excluded.availability,
    predecessor_asset_id=excluded.predecessor_asset_id,
    pending_candidate_id=excluded.pending_candidate_id,
    activated_at=excluded.activated_at,
    last_checked_at=excluded.last_checked_at,
    decision_reasons_json=excluded.decision_reasons_json,
    decision_evidence_json=excluded.decision_evidence_json,
    equivalent_source_filings_json=excluded.equivalent_source_filings_json,
    canonical_projection_policy_version=excluded.canonical_projection_policy_version,
    evidence_set_hash=excluded.evidence_set_hash,
    visibility_state=excluded.visibility_state,
    updated_at=excluded.updated_at
"""


class AnnouncementAssetRepository:
    """Own shared-asset tables without mutating unrelated storage contracts."""

    def __init__(self, db_path: str | Path, *, timeout_seconds: float = 30.0) -> None:
        self.db_path = Path(db_path)
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    def initialize_schema(self) -> None:
        """Explicitly create or migrate only announcement-asset-owned tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(SCHEMA_SQL)
            for table in OBSOLETE_TABLES:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            for table, columns in OBSOLETE_COLUMNS.items():
                existing = {
                    str(row[1])
                    for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
                for column in columns:
                    if column in existing:
                        conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
            conn.execute(
                """DELETE FROM official_asset_retention_pins
                   WHERE pin_type NOT IN ('effective_asset', 'active_reader')
                      OR released_at IS NOT NULL
                      OR (pin_type='active_reader' AND expires_at<=?)""",
                (utc_now_iso(),),
            )
            self._migrate_attachment_version_temporal_columns(conn)
            self._migrate_attachment_version_visibility(conn)
            self._migrate_effective_visibility_column(conn)
            self._migrate_attachment_retry_columns(conn)
            self._migrate_canonical_contract_columns(conn)
            self._migrate_operation_status_contract(conn)
            self._migrate_operation_lease_fencing(conn)
            self._migrate_effective_decision_history(conn)
            self._migrate_discovery_state_fencing(conn)
            self._migrate_universe_census_contract(conn)
            conn.execute(
                """
                INSERT INTO official_asset_schema_versions(
                    component, schema_version, applied_at
                ) VALUES('announcement_assets', ?, ?)
                ON CONFLICT(component) DO UPDATE SET
                    schema_version=excluded.schema_version,
                    applied_at=excluded.applied_at
                """,
                (SCHEMA_VERSION, utc_now_iso()),
            )
            conn.commit()

    def schema_initialized(self) -> bool:
        """Return whether the shared catalog tables exist without mutating SQLite."""
        if not self.db_path.exists():
            return False
        with self.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='official_asset_schema_versions' LIMIT 1"
            ).fetchone()
        return row is not None

    @staticmethod
    def _migrate_universe_census_contract(conn: sqlite3.Connection) -> None:
        """Add the independent census-pair reference to legacy universe rows."""

        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(official_asset_universe_snapshots)"
            ).fetchall()
        }
        if "paired_census_snapshot_id" not in columns:
            conn.execute(
                "ALTER TABLE official_asset_universe_snapshots "
                "ADD COLUMN paired_census_snapshot_id TEXT"
            )

    @staticmethod
    def _migrate_attachment_version_temporal_columns(
        conn: sqlite3.Connection,
    ) -> None:
        """Add immutable observation availability fields to pre-v2 databases."""
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(official_attachment_versions)"
            ).fetchall()
        }
        additions = (
            ("version_available_at", "TEXT NOT NULL DEFAULT ''"),
            (
                "available_time_source",
                "TEXT NOT NULL DEFAULT 'first_observed'",
            ),
            (
                "available_time_precision",
                "TEXT NOT NULL DEFAULT 'instant'",
            ),
        )
        for name, definition in additions:
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE official_attachment_versions ADD COLUMN {name} {definition}"
                )
        conn.execute(
            """
            UPDATE official_attachment_versions
            SET version_available_at=observed_at
            WHERE version_available_at IS NULL OR version_available_at=''
            """
        )

    @staticmethod
    def _migrate_attachment_version_visibility(conn: sqlite3.Connection) -> None:
        """Keep legacy observations visible while fencing new shadow adoption."""

        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(official_attachment_versions)"
            ).fetchall()
        }
        if "visibility_state" not in columns:
            conn.execute(
                "ALTER TABLE official_attachment_versions "
                "ADD COLUMN visibility_state TEXT NOT NULL DEFAULT 'production'"
            )
        conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_official_attachment_versions_visibility
               ON official_attachment_versions(
                   attachment_id, visibility_state, version_available_at
               )"""
        )

    @staticmethod
    def _migrate_effective_visibility_column(conn: sqlite3.Connection) -> None:
        """Keep existing effective rows production-visible during v2 migration."""
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(effective_annual_reports)"
            ).fetchall()
        }
        if "visibility_state" not in columns:
            conn.execute(
                "ALTER TABLE effective_annual_reports "
                "ADD COLUMN visibility_state TEXT NOT NULL DEFAULT 'production'"
            )

    @staticmethod
    def _migrate_attachment_retry_columns(conn: sqlite3.Connection) -> None:
        """Add durable retry taxonomy evidence to pre-v5 catalogs."""

        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(official_asset_attachment_retries)"
            ).fetchall()
        }
        additions = (
            ("observation_key", "TEXT NOT NULL DEFAULT ''"),
            ("max_attempts", "INTEGER NOT NULL DEFAULT 4"),
            ("failure_class", "TEXT"),
            ("operator_action_required", "INTEGER NOT NULL DEFAULT 0"),
            ("consumes_retry_budget", "INTEGER NOT NULL DEFAULT 1"),
            ("reopen_reason", "TEXT"),
            ("reopened_at", "TEXT"),
            ("repair_actor", "TEXT"),
        )
        for name, definition in additions:
            if name not in columns:
                conn.execute(
                    "ALTER TABLE official_asset_attachment_retries "
                    f"ADD COLUMN {name} {definition}"
                )

    @staticmethod
    def _add_missing_columns(
        conn: sqlite3.Connection,
        table: str,
        additions: Sequence[tuple[str, str]],
    ) -> None:
        columns = {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for name, definition in additions:
            if name not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    @classmethod
    def _migrate_canonical_contract_columns(cls, conn: sqlite3.Connection) -> None:
        """Upgrade pre-v6 catalogs without rebuilding consumer-owned tables."""

        cls._add_missing_columns(
            conn,
            "official_announcements",
            (
                ("source_category", "TEXT"),
                ("published_at_precision", "TEXT"),
                ("provider_diagnostics_json", "TEXT NOT NULL DEFAULT '{}'"),
            ),
        )
        conn.execute(
            "UPDATE official_asset_operations SET schema_version=?",
            (OPERATION_SCHEMA_VERSION,),
        )
        conn.execute(
            "UPDATE official_asset_operation_subscriptions SET schema_version=?",
            (OPERATION_SUBSCRIPTION_SCHEMA_VERSION,),
        )
        cls._add_missing_columns(
            conn,
            "official_attachment_versions",
            (
                ("max_attempts", "INTEGER NOT NULL DEFAULT 4"),
                ("first_observed_at", "TEXT NOT NULL DEFAULT ''"),
                ("last_observed_at", "TEXT NOT NULL DEFAULT ''"),
                ("response_evidence_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("content_length_observed", "INTEGER"),
                ("content_hash_observed", "TEXT"),
                ("lease_owner", "TEXT"),
                ("lease_generation", "INTEGER"),
                ("temporary_path", "TEXT"),
                ("temporary_bytes", "INTEGER"),
                ("quarantine_path", "TEXT"),
            ),
        )
        conn.execute(
            """UPDATE official_attachment_versions
               SET first_observed_at=observed_at
               WHERE first_observed_at IS NULL OR first_observed_at=''"""
        )
        conn.execute(
            """UPDATE official_attachment_versions
               SET last_observed_at=observed_at
               WHERE last_observed_at IS NULL OR last_observed_at=''"""
        )
        cls._add_missing_columns(
            conn,
            "effective_annual_reports",
            (
                ("document_family", "TEXT NOT NULL DEFAULT 'annual_report'"),
                ("is_full_report", "INTEGER NOT NULL DEFAULT 1"),
                ("decision_evidence_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("equivalent_source_filings_json", "TEXT NOT NULL DEFAULT '[]'"),
                (
                    "canonical_projection_policy_version",
                    "TEXT NOT NULL DEFAULT 'canonical_source_filing.v1'",
                ),
                ("evidence_set_hash", "TEXT"),
            ),
        )
        cls._add_missing_columns(
            conn,
            "official_asset_acquisition_leases",
            (("lease_generation", "INTEGER NOT NULL DEFAULT 1"),),
        )
        cls._add_missing_columns(
            conn,
            "official_asset_operations",
            (
                ("bounds_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("config_version", "TEXT"),
                ("max_attempts", "INTEGER NOT NULL DEFAULT 1"),
                ("resume_generation", "INTEGER NOT NULL DEFAULT 0"),
                ("checkpoint_json", "TEXT NOT NULL DEFAULT '{}'"),
                (
                    "stage_schema_version",
                    "TEXT NOT NULL DEFAULT 'official_asset_operation_stage.v1'",
                ),
            ),
        )
        cls._add_missing_columns(
            conn,
            "official_asset_operation_subscriptions",
            (
                ("authorized_projection_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("expires_at", "TEXT"),
                ("expired_at", "TEXT"),
                ("tombstone_until", "TEXT"),
                (
                    "retention_policy_version",
                    "TEXT NOT NULL DEFAULT 'asset_request_retention.v1'",
                ),
            ),
        )
        cls._add_missing_columns(
            conn,
            "official_asset_change_events",
            (
                (
                    "schema_version",
                    "TEXT NOT NULL DEFAULT 'official_asset_change_event.v1'",
                ),
                ("trigger_origin", "TEXT NOT NULL DEFAULT 'unknown'"),
                (
                    "dispatch_policy_version",
                    "TEXT NOT NULL DEFAULT 'asset_change_event.v1'",
                ),
            ),
        )

    @staticmethod
    def _migrate_effective_decision_history(conn: sqlite3.Connection) -> None:
        """Snapshot pre-history current projections without inventing lineage."""

        missing_blob = conn.execute(
            """SELECT asset_id FROM effective_annual_reports e
               WHERE e.content_hash IS NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM official_annual_report_decisions d
                     WHERE d.instrument_id=e.instrument_id
                       AND d.fiscal_year=e.fiscal_year
                       AND d.replacement_asset_id=e.asset_id
                 )
               LIMIT 1"""
        ).fetchone()
        if missing_blob is not None:
            raise ValueError(
                "cannot migrate an effective projection without a replacement blob: "
                f"{missing_blob['asset_id']}"
            )
        rows = conn.execute(
            """SELECT * FROM effective_annual_reports e
               WHERE e.content_hash IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM official_annual_report_decisions d
                     WHERE d.instrument_id=e.instrument_id
                       AND d.fiscal_year=e.fiscal_year
                       AND d.replacement_asset_id=e.asset_id
                 )
               ORDER BY e.instrument_id, e.fiscal_year"""
        ).fetchall()
        for row in rows:
            activated_at = row["activated_at"] or row["updated_at"]
            legacy_predecessor_asset_id = row["predecessor_asset_id"]
            legal_filing = conn.execute(
                """SELECT source, source_announcement_id
                   FROM official_announcements WHERE announcement_id=?""",
                (row["announcement_id"],),
            ).fetchone()
            if legal_filing is None:
                raise ValueError(
                    "cannot migrate effective decision with an unknown legal filing"
                )
            event_key = stable_id(
                "event",
                "decision_history_migration",
                row["instrument_id"],
                row["fiscal_year"],
                row["asset_id"],
            )
            decision_id = stable_id("decision", event_key)
            conn.execute(
                """INSERT OR IGNORE INTO official_asset_change_events(
                       event_key, event_type, instrument_id, fiscal_year,
                       asset_id, predecessor_asset_id, content_hash,
                       trigger_origin, dispatch_policy_version,
                       payload_json, created_at
                   ) VALUES(?, 'repaired', ?, ?, ?, ?, ?,
                            'schema_migration', ?, ?, ?)""",
                (
                    event_key,
                    row["instrument_id"],
                    int(row["fiscal_year"]),
                    row["asset_id"],
                    None,
                    row["content_hash"],
                    CHANGE_EVENT_POLICY_VERSION,
                    canonical_json(
                        {
                            "decision_id": decision_id,
                            "decision_kind": "migration_snapshot",
                            "migration_limitation": "pre_v8_lineage_unavailable",
                            "legacy_predecessor_asset_id_hint": (
                                legacy_predecessor_asset_id
                            ),
                        }
                    ),
                    activated_at,
                ),
            )
            conn.execute(
                """INSERT OR IGNORE INTO official_annual_report_decisions(
                       decision_id, schema_version, instrument_id, fiscal_year,
                       decision_kind, predecessor_asset_id,
                       replacement_asset_id, replacement_source,
                       replacement_source_announcement_id,
                       replacement_announcement_id,
                       replacement_attachment_id, replacement_version_id,
                       replacement_content_hash, decision_state,
                       classifier_version, decision_policy_version,
                       decision_reasons_json, decision_evidence_json,
                       activated_at, outbox_event_key, created_at
                   ) VALUES(?, ?, ?, ?, 'migration_snapshot', ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    decision_id,
                    EFFECTIVE_DECISION_SCHEMA_VERSION,
                    row["instrument_id"],
                    int(row["fiscal_year"]),
                    None,
                    row["asset_id"],
                    legal_filing["source"],
                    legal_filing["source_announcement_id"],
                    row["announcement_id"],
                    row["attachment_id"],
                    row["version_id"],
                    row["content_hash"],
                    row["decision_state"],
                    row["classifier_version"],
                    row["canonical_projection_policy_version"],
                    json.dumps(
                        ["decision_history_v1_current_projection_snapshot"],
                        ensure_ascii=False,
                    ),
                    canonical_json(
                        {
                            "migration_limitation": "pre_v8_lineage_unavailable",
                            "legacy_predecessor_asset_id_hint": (
                                legacy_predecessor_asset_id
                            ),
                            "source_schema_version": row["schema_version"],
                        }
                    ),
                    activated_at,
                    event_key,
                    activated_at,
                ),
            )

    @staticmethod
    def _migrate_discovery_state_fencing(conn: sqlite3.Connection) -> None:
        """Add durable fencing and retry evidence to discovery cursors."""
        AnnouncementAssetRepository._add_missing_columns(
            conn,
            "official_asset_discovery_state",
            (
                (
                    "schema_version",
                    "TEXT NOT NULL DEFAULT 'official_asset_discovery_state.v2'",
                ),
                ("lease_owner", "TEXT"),
                ("lease_expires_at", "TEXT"),
                ("lease_generation", "INTEGER NOT NULL DEFAULT 0"),
                ("state_version", "INTEGER NOT NULL DEFAULT 0"),
                ("operation_id", "TEXT"),
                ("observation_key", "TEXT NOT NULL DEFAULT ''"),
                ("attempt", "INTEGER NOT NULL DEFAULT 0"),
                ("max_attempts", "INTEGER NOT NULL DEFAULT 4"),
                ("next_retry_at", "TEXT"),
                ("last_error_code", "TEXT"),
                ("failure_class", "TEXT"),
                ("operator_action_required", "INTEGER NOT NULL DEFAULT 0"),
                ("consumes_retry_budget", "INTEGER NOT NULL DEFAULT 1"),
                ("reopen_reason", "TEXT"),
                ("reopened_at", "TEXT"),
                ("repair_actor", "TEXT"),
            ),
        )

    @staticmethod
    def _migrate_operation_status_contract(conn: sqlite3.Connection) -> None:
        """Project legacy caller-expiry rows out of durable operation status."""
        conn.execute(
            """
            UPDATE official_asset_operations
            SET status='blocked',
                reason_code=COALESCE(reason_code, 'legacy_expired_operation'),
                diagnostics_json=CASE
                    WHEN diagnostics_json IS NULL OR diagnostics_json='' OR diagnostics_json='{}'
                    THEN '{"legacy_status":"expired","migration":"operation_status.v2"}'
                    ELSE diagnostics_json
                END,
                updated_at=COALESCE(updated_at, ?)
            WHERE status='expired'
            """,
            (utc_now_iso(),),
        )
        conn.executescript(
            """
            DROP TRIGGER IF EXISTS trg_official_asset_operation_stage_insert;
            DROP TRIGGER IF EXISTS trg_official_asset_operation_stage_update;
            DROP TRIGGER IF EXISTS trg_official_asset_retention_pin_flags_insert;
            DROP TRIGGER IF EXISTS trg_official_asset_retention_pin_flags_update;

            CREATE TRIGGER IF NOT EXISTS trg_official_asset_operation_status_insert
            BEFORE INSERT ON official_asset_operations
            WHEN NEW.status NOT IN (
                'queued', 'running', 'completed', 'missing',
                'failed', 'blocked', 'cancelled'
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid internal operation status');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_official_asset_operation_status_update
            BEFORE UPDATE OF status ON official_asset_operations
            WHEN NEW.status NOT IN (
                'queued', 'running', 'completed', 'missing',
                'failed', 'blocked', 'cancelled'
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid internal operation status');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_official_asset_operation_stage_insert
            BEFORE INSERT ON official_asset_operations
            WHEN NEW.stage IS NOT NULL AND NEW.stage NOT IN (
                'not_applicable', 'discovering', 'reconciling',
                'downloading', 'validating', 'activating'
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid operation stage');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_official_asset_operation_stage_update
            BEFORE UPDATE OF stage ON official_asset_operations
            WHEN NEW.stage IS NOT NULL AND NEW.stage NOT IN (
                'not_applicable', 'discovering', 'reconciling',
                'downloading', 'validating', 'activating'
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid operation stage');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_official_asset_subscription_status_insert
            BEFORE INSERT ON official_asset_operation_subscriptions
            WHEN NEW.status NOT IN ('active', 'cancelled', 'expired')
            BEGIN
                SELECT RAISE(ABORT, 'invalid asset request status');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_official_asset_subscription_status_update
            BEFORE UPDATE OF status ON official_asset_operation_subscriptions
            WHEN NEW.status NOT IN ('active', 'cancelled', 'expired')
            BEGIN
                SELECT RAISE(ABORT, 'invalid asset request status');
            END;

            """
        )

    @classmethod
    def _migrate_operation_lease_fencing(cls, conn: sqlite3.Connection) -> None:
        """Add a monotonic lease token without invalidating legacy rows."""
        cls._add_missing_columns(
            conn,
            "official_asset_operations",
            (("lease_generation", "INTEGER NOT NULL DEFAULT 0"),),
        )
        conn.execute(
            "UPDATE official_asset_operations SET schema_version=?",
            (OPERATION_SCHEMA_VERSION,),
        )

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, timeout=self.timeout_seconds)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(
        self, *, immediate: bool = True
    ) -> Generator[sqlite3.Connection, None, None]:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()

    def upsert_announcement(
        self,
        record: AnnouncementRecord,
        *,
        instrument_id: str | None = None,
        status: str = "observed",
        observed_at: str | None = None,
    ) -> OfficialAnnouncement:
        source = normalize_source(record.source)
        observed = observed_at or utc_now_iso()
        announcement_id = stable_id("ann", source, record.source_announcement_id)
        payload_hash = hashlib.sha256(
            canonical_json(record.raw_payload).encode("utf-8")
        ).hexdigest()
        normalized_instrument = (
            normalize_instrument_id(instrument_id) if instrument_id else None
        )
        metadata = {
            "market": record.market,
            "symbols": list(record.symbols),
            "security_names": list(record.security_names),
            "organization_ids": list(record.organization_ids),
            "diagnostics": list(record.diagnostics),
            "identity_is_derived": bool(record.identity_is_derived),
        }
        source_category = next(
            (
                str(record.raw_payload[key]).strip()
                for key in ("category", "announcement_category", "announcementType")
                if record.raw_payload.get(key) not in (None, "")
            ),
            None,
        )
        published_at_precision = (
            "date"
            if "published_at_date_only" in record.diagnostics
            else "instant"
            if record.published_at
            else None
        )
        provider_diagnostics = {
            "diagnostics": list(record.diagnostics),
            "selection_reasons": list(record.selection_reasons),
        }
        if record.provider_route_evidence:
            provider_diagnostics["provider_route"] = dict(
                record.provider_route_evidence
            )
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_announcements(
                    announcement_id, schema_version, source,
                    source_announcement_id, title, instrument_id, exchange,
                    source_category, published_at, published_at_raw,
                    published_at_precision, raw_payload_hash,
                    first_observed_at, last_observed_at, status,
                    provider_diagnostics_json, metadata_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, source_announcement_id) DO UPDATE SET
                    title=excluded.title,
                    instrument_id=COALESCE(excluded.instrument_id, official_announcements.instrument_id),
                    exchange=COALESCE(excluded.exchange, official_announcements.exchange),
                    source_category=COALESCE(excluded.source_category, official_announcements.source_category),
                    published_at=COALESCE(excluded.published_at, official_announcements.published_at),
                    published_at_raw=COALESCE(excluded.published_at_raw, official_announcements.published_at_raw),
                    published_at_precision=COALESCE(excluded.published_at_precision, official_announcements.published_at_precision),
                    raw_payload_hash=excluded.raw_payload_hash,
                    last_observed_at=excluded.last_observed_at,
                    status=excluded.status,
                    provider_diagnostics_json=excluded.provider_diagnostics_json,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    announcement_id,
                    ANNOUNCEMENT_SCHEMA_VERSION,
                    source,
                    record.source_announcement_id,
                    record.title,
                    normalized_instrument,
                    record.exchange,
                    source_category,
                    record.published_at,
                    (
                        None
                        if record.published_at_raw is None
                        else str(record.published_at_raw)
                    ),
                    published_at_precision,
                    payload_hash,
                    observed,
                    observed,
                    status,
                    canonical_json(provider_diagnostics),
                    canonical_json(metadata),
                    observed,
                    observed,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_announcements WHERE source=? AND source_announcement_id=?",
                (source, record.source_announcement_id),
            ).fetchone()
        return self._announcement_from_row(_require_row(row))

    def upsert_attachment(
        self,
        announcement_id: str,
        attachment: AnnouncementAttachment,
        *,
        observed_at: str | None = None,
    ) -> OfficialAnnouncementAttachment:
        observed = observed_at or utc_now_iso()
        normalized_url = normalize_source_url(
            attachment.resolved_url or attachment.source_url
        )
        identity = str(attachment.attachment_id or "").strip() or normalized_url
        attachment_id = stable_id("att", announcement_id, identity)
        length_hint = _optional_int(
            attachment.raw_metadata.get("content_length")
            or attachment.raw_metadata.get("fileSize")
        )
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_announcement_attachments(
                    attachment_id, schema_version, announcement_id,
                    attachment_identity, source_attachment_id, source_url,
                    normalized_source_url, name, media_type, content_length_hint,
                    first_observed_at, last_observed_at, metadata_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(announcement_id, attachment_identity) DO UPDATE SET
                    source_url=excluded.source_url,
                    normalized_source_url=excluded.normalized_source_url,
                    name=COALESCE(excluded.name, official_announcement_attachments.name),
                    media_type=COALESCE(excluded.media_type, official_announcement_attachments.media_type),
                    content_length_hint=COALESCE(excluded.content_length_hint, official_announcement_attachments.content_length_hint),
                    last_observed_at=excluded.last_observed_at,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    attachment_id,
                    ATTACHMENT_SCHEMA_VERSION,
                    announcement_id,
                    identity,
                    attachment.attachment_id,
                    attachment.source_url,
                    normalized_url,
                    attachment.name,
                    attachment.media_type,
                    length_hint,
                    observed,
                    observed,
                    canonical_json(attachment.raw_metadata),
                    observed,
                    observed,
                ),
            )
            row = conn.execute(
                """SELECT * FROM official_announcement_attachments
                   WHERE announcement_id=? AND attachment_identity=?""",
                (announcement_id, identity),
            ).fetchone()
        return self._attachment_from_row(_require_row(row))

    def update_attachment_metadata(
        self,
        attachment_id: str,
        metadata: Mapping[str, object],
        *,
        updated_at: str | None = None,
    ) -> OfficialAnnouncementAttachment:
        updated = updated_at or utc_now_iso()
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE official_announcement_attachments
                SET metadata_json=?, updated_at=?
                WHERE attachment_id=?
                """,
                (canonical_json(metadata), updated, attachment_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"attachment not found: {attachment_id}")
            row = conn.execute(
                "SELECT * FROM official_announcement_attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        return self._attachment_from_row(_require_row(row))

    def get_announcement(self, announcement_id: str) -> OfficialAnnouncement | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_announcements WHERE announcement_id=?",
                (announcement_id,),
            ).fetchone()
        return None if row is None else self._announcement_from_row(row)

    def get_attachment(
        self, attachment_id: str
    ) -> OfficialAnnouncementAttachment | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_announcement_attachments WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        return None if row is None else self._attachment_from_row(row)

    def list_attachments(
        self, announcement_id: str
    ) -> list[OfficialAnnouncementAttachment]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT * FROM official_announcement_attachments
                   WHERE announcement_id=? ORDER BY attachment_id""",
                (announcement_id,),
            ).fetchall()
        return [self._attachment_from_row(row) for row in rows]

    def list_candidate_rows(
        self,
        *,
        instrument_id: str | None = None,
        fiscal_year: int | None = None,
        source: str | None = None,
        source_announcement_id: str | None = None,
        observation_cutoff: str | None = None,
        include_shadow: bool = False,
    ) -> list[dict[str, Any]]:
        """Return attachment observations plus newest evidence visible at cutoff.

        A bootstrap must not let a later observation hide the last evidence
        visible at its fixed cutoff.  Without a cutoff this retains the legacy
        newest-observation behavior used by daily consumers.
        """
        clauses: list[str] = []
        params: list[Any] = []
        if instrument_id:
            clauses.append("a.instrument_id=?")
            params.append(normalize_instrument_id(instrument_id))
        if source:
            clauses.append("a.source=?")
            params.append(normalize_source(source))
        if source_announcement_id:
            clauses.append("a.source_announcement_id=?")
            params.append(str(source_announcement_id).strip())
        if not include_shadow:
            clauses.append(
                "(NOT EXISTS (SELECT 1 FROM official_attachment_versions av "
                "WHERE av.attachment_id=aa.attachment_id) OR EXISTS ("
                "SELECT 1 FROM official_attachment_versions av "
                "WHERE av.attachment_id=aa.attachment_id "
                "AND av.visibility_state='production'))"
            )
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.connection() as conn:
            version_cutoff_clause = (
                "" if include_shadow else " AND v2.visibility_state='production'"
            )
            version_params: list[Any] = []
            if observation_cutoff is not None:
                version_cutoff_clause += (
                    " AND julianday(v2.version_available_at)<=julianday(?)"
                )
                version_params.append(str(observation_cutoff))
            rows = conn.execute(
                f"""
                SELECT
                    a.announcement_id, a.source, a.source_announcement_id,
                    a.title, a.instrument_id, a.exchange, a.published_at,
                    a.first_observed_at AS announcement_first_observed_at,
                    a.last_observed_at AS announcement_last_observed_at,
                    a.status AS announcement_status,
                    aa.attachment_id, aa.attachment_identity,
                    aa.source_attachment_id, aa.source_url,
                    aa.normalized_source_url, aa.name, aa.media_type,
                    aa.metadata_json AS attachment_metadata_json,
                    v.version_id, v.observation_key, v.content_hash,
                    v.final_url, v.retrieval_status, v.integrity_status,
                    v.attempt, v.observed_at, v.version_available_at,
                    v.available_time_source, v.available_time_precision,
                    v.response_evidence_json AS version_response_evidence_json,
                    v.content_length_observed, v.content_hash_observed,
                    v.temporary_path, v.temporary_bytes, v.quarantine_path,
                    v.metadata_json AS version_metadata_json,
                    v.visibility_state AS version_visibility_state,
                    a.metadata_json AS announcement_metadata_json,
                    b.canonical_path, b.content_length,
                    b.integrity_status AS blob_integrity_status
                FROM official_announcements a
                JOIN official_announcement_attachments aa
                  ON aa.announcement_id=a.announcement_id
                LEFT JOIN official_attachment_versions v
                  ON v.version_id=(
                    SELECT v2.version_id FROM official_attachment_versions v2
                    WHERE v2.attachment_id=aa.attachment_id
                      {version_cutoff_clause}
                    ORDER BY v2.version_available_at DESC, v2.observed_at DESC,
                             v2.version_id DESC LIMIT 1
                  )
                LEFT JOIN official_document_blobs b ON b.content_hash=v.content_hash
                {where}
                ORDER BY a.published_at DESC, aa.attachment_id
                """,
                tuple(version_params + params),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            metadata = _json_load(item.pop("attachment_metadata_json", None), {})
            item["announcement_metadata"] = _json_load(
                item.pop("announcement_metadata_json", None), {}
            )
            item["version_response_evidence"] = _json_load(
                item.pop("version_response_evidence_json", None), {}
            )
            item["version_metadata"] = _json_load(
                item.pop("version_metadata_json", None), {}
            )
            classification = metadata.get("asset_classification") or {}
            if fiscal_year is not None and _optional_int(
                classification.get("fiscal_year")
            ) != int(fiscal_year):
                continue
            item["attachment_metadata"] = metadata
            item["classification"] = classification
            output.append(item)
        return output

    def list_annual_report_asset_records(
        self,
        *,
        document_family: str = "annual_report",
        instrument_id: str | None = None,
        fiscal_year: int | None = None,
        source: str | None = None,
        source_announcement_id: str | None = None,
        integrity: str | None = None,
        acquisition_status: str | None = None,
        effective_state: str | None = None,
        asset_availability: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List current and historical annual-report filing records locally."""
        bounded_limit = max(1, min(int(limit), 1000))
        bounded_offset = max(0, int(offset))
        clauses = ["document_family=?", "is_full_report=1"]
        params: list[Any] = [str(document_family).strip().lower()]
        for clause, value in (
            (
                "instrument_id=?",
                normalize_instrument_id(instrument_id) if instrument_id else None,
            ),
            ("fiscal_year=?", int(fiscal_year) if fiscal_year is not None else None),
            ("source=?", normalize_source(source) if source else None),
            (
                "source_announcement_id=?",
                str(source_announcement_id).strip() if source_announcement_id else None,
            ),
            ("integrity=?", str(integrity).strip() if integrity else None),
            (
                "acquisition_status=?",
                str(acquisition_status).strip() if acquisition_status else None,
            ),
            (
                "effective_state=?",
                str(effective_state).strip() if effective_state else None,
            ),
            (
                "asset_availability=?",
                str(asset_availability).strip() if asset_availability else None,
            ),
        ):
            if value is not None:
                clauses.append(clause)
                params.append(value)
        with self.connection() as conn:
            rows = conn.execute(
                """
                WITH latest_versions AS (
                    SELECT ranked.* FROM (
                        SELECT version.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY version.attachment_id
                                   ORDER BY version.version_available_at DESC,
                                            version.observed_at DESC,
                                            version.version_id DESC
                               ) AS row_number
                        FROM official_attachment_versions AS version
                        WHERE version.visibility_state='production'
                    ) AS ranked WHERE ranked.row_number=1
                ),
                base AS (
                    SELECT
                        announcement.instrument_id,
                        CAST(json_extract(
                            attachment.metadata_json,
                            '$.asset_classification.fiscal_year'
                        ) AS INTEGER) AS fiscal_year,
                        json_extract(
                            attachment.metadata_json,
                            '$.asset_classification.report_period'
                        ) AS report_period,
                        announcement.source,
                        announcement.source_announcement_id,
                        announcement.announcement_id,
                        attachment.attachment_id,
                        attachment.source_attachment_id,
                        version.version_id AS observation_version,
                        version.version_available_at,
                        announcement.published_at,
                        json_extract(
                            attachment.metadata_json,
                            '$.asset_classification.document_family'
                        ) AS document_family,
                        json_extract(
                            attachment.metadata_json,
                            '$.asset_classification.variant'
                        ) AS variant,
                        CAST(COALESCE(json_extract(
                            attachment.metadata_json,
                            '$.asset_classification.is_full_report'
                        ), 0) AS INTEGER) AS is_full_report,
                        json_extract(
                            attachment.metadata_json,
                            '$.asset_classification.vocabulary_version'
                        ) AS classification_vocabulary_version,
                        version.content_hash,
                        blob.content_length,
                        COALESCE(
                            blob.integrity_status,
                            version.integrity_status,
                            'unchecked'
                        ) AS integrity,
                        COALESCE(version.retrieval_status, 'metadata_only')
                            AS acquisition_status,
                        effective.asset_id,
                        effective.predecessor_asset_id,
                        effective.pending_candidate_id,
                        effective.activated_at,
                        COALESCE(
                            effective.last_checked_at,
                            announcement.last_observed_at
                        ) AS last_checked_at,
                        effective.decision_reasons_json,
                        effective.equivalent_source_filings_json,
                        effective.canonical_projection_policy_version,
                        effective.evidence_set_hash,
                        CASE
                            WHEN effective.asset_id IS NOT NULL
                                THEN effective.decision_state
                            WHEN announcement.status IN ('withdrawn', 'cancelled')
                                THEN 'withdrawn'
                            WHEN EXISTS (
                                SELECT 1 FROM official_annual_report_decisions decision
                                WHERE decision.predecessor_version_id=version.version_id
                            ) THEN 'superseded'
                            ELSE 'historical'
                        END AS effective_state,
                        CASE
                            WHEN effective.asset_id IS NOT NULL
                                THEN effective.availability
                            WHEN EXISTS (
                                SELECT 1 FROM official_annual_report_decisions decision
                                WHERE decision.predecessor_version_id=version.version_id
                            ) THEN 'superseded'
                            ELSE 'metadata_only'
                        END AS asset_availability
                    FROM official_announcements AS announcement
                    JOIN official_announcement_attachments AS attachment
                      ON attachment.announcement_id=announcement.announcement_id
                    LEFT JOIN latest_versions AS version
                      ON version.attachment_id=attachment.attachment_id
                    LEFT JOIN official_document_blobs AS blob
                      ON blob.content_hash=version.content_hash
                    LEFT JOIN effective_annual_reports AS effective
                      ON effective.version_id=version.version_id
                     AND effective.visibility_state='production'
                )
                SELECT * FROM base
                WHERE """
                + " AND ".join(clauses)
                + """
                ORDER BY instrument_id ASC, fiscal_year DESC,
                         published_at DESC, source ASC,
                         source_announcement_id ASC, attachment_id ASC,
                         COALESCE(observation_version, '') ASC
                LIMIT ? OFFSET ?
                """,
                tuple(params + [bounded_limit, bounded_offset]),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["decision_reasons"] = _json_load(
                item.pop("decision_reasons_json", None), []
            )
            item["equivalent_source_filings"] = _json_load(
                item.pop("equivalent_source_filings_json", None), []
            )
            output.append(item)
        return output

    def register_blob(self, blob: OfficialDocumentBlob) -> OfficialDocumentBlob:
        now = utc_now_iso()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM official_document_blobs WHERE content_hash=?",
                (blob.content_hash,),
            ).fetchone()
            if existing is not None:
                if int(existing["content_length"]) != int(blob.content_length):
                    raise ValueError(
                        "content hash already exists with a different length"
                    )
                if str(existing["canonical_path"]) != str(blob.canonical_path):
                    raise ValueError(
                        "content hash already exists at a different canonical path"
                    )
            conn.execute(
                """
                INSERT INTO official_document_blobs(
                    content_hash, schema_version, content_length, canonical_path,
                    signature_status, integrity_status, first_available_at,
                    last_verified_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(content_hash) DO UPDATE SET
                    signature_status=excluded.signature_status,
                    integrity_status=excluded.integrity_status,
                    last_verified_at=COALESCE(excluded.last_verified_at, official_document_blobs.last_verified_at),
                    updated_at=excluded.updated_at
                """,
                (
                    blob.content_hash,
                    BLOB_SCHEMA_VERSION,
                    int(blob.content_length),
                    blob.canonical_path,
                    blob.signature_status,
                    blob.integrity_status.value,
                    blob.first_available_at,
                    blob.last_verified_at,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_document_blobs WHERE content_hash=?",
                (blob.content_hash,),
            ).fetchone()
        return self._blob_from_row(_require_row(row))

    def get_blob(self, content_hash: str) -> OfficialDocumentBlob | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_document_blobs WHERE content_hash=?",
                (content_hash,),
            ).fetchone()
        return None if row is None else self._blob_from_row(row)

    def update_blob_integrity(
        self,
        content_hash: str,
        status: IntegrityStatus,
        *,
        verified_at: str | None = None,
    ) -> None:
        now = verified_at or utc_now_iso()
        with self.transaction() as conn:
            result = conn.execute(
                """UPDATE official_document_blobs
                   SET integrity_status=?, last_verified_at=?, updated_at=?
                   WHERE content_hash=?""",
                (status.value, now, now, content_hash),
            )
            if result.rowcount != 1:
                raise KeyError(f"blob not found: {content_hash}")

    def add_attachment_version(
        self, version: OfficialAttachmentVersion
    ) -> OfficialAttachmentVersion:
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_attachment_versions(
                    version_id, schema_version, attachment_id, observation_key,
                    content_hash, final_url, retrieval_status, integrity_status,
                    attempt, max_attempts, next_retry_at, error_code, observed_at,
                    first_observed_at, last_observed_at,
                    version_available_at, available_time_source,
                    available_time_precision, response_evidence_json,
                    content_length_observed, content_hash_observed,
                    lease_owner, lease_generation, temporary_path,
                    temporary_bytes, quarantine_path, metadata_json,
                    visibility_state, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attachment_id, observation_key) DO UPDATE SET
                    content_hash=COALESCE(excluded.content_hash, official_attachment_versions.content_hash),
                    final_url=COALESCE(excluded.final_url, official_attachment_versions.final_url),
                    retrieval_status=excluded.retrieval_status,
                    integrity_status=excluded.integrity_status,
                    attempt=MAX(official_attachment_versions.attempt, excluded.attempt),
                    max_attempts=excluded.max_attempts,
                    next_retry_at=excluded.next_retry_at,
                    error_code=excluded.error_code,
                    last_observed_at=MAX(official_attachment_versions.last_observed_at, excluded.last_observed_at),
                    response_evidence_json=excluded.response_evidence_json,
                    content_length_observed=COALESCE(excluded.content_length_observed, official_attachment_versions.content_length_observed),
                    content_hash_observed=COALESCE(excluded.content_hash_observed, official_attachment_versions.content_hash_observed),
                    lease_owner=excluded.lease_owner,
                    lease_generation=excluded.lease_generation,
                    temporary_path=excluded.temporary_path,
                    temporary_bytes=excluded.temporary_bytes,
                    quarantine_path=excluded.quarantine_path,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    version.version_id,
                    ATTACHMENT_VERSION_SCHEMA_VERSION,
                    version.attachment_id,
                    version.observation_key,
                    version.content_hash,
                    version.final_url,
                    version.retrieval_status,
                    version.integrity_status.value,
                    max(0, int(version.attempt)),
                    max(1, int(version.max_attempts)),
                    version.next_retry_at,
                    version.error_code,
                    version.observed_at,
                    version.first_observed_at or version.observed_at,
                    version.last_observed_at or version.observed_at,
                    version.version_available_at or version.observed_at,
                    version.available_time_source or "first_observed",
                    version.available_time_precision or "instant",
                    canonical_json(version.response_evidence),
                    version.content_length_observed,
                    version.content_hash_observed,
                    version.lease_owner,
                    version.lease_generation,
                    version.temporary_path,
                    version.temporary_bytes,
                    version.quarantine_path,
                    canonical_json(version.metadata),
                    version.visibility_state,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """SELECT * FROM official_attachment_versions
                   WHERE attachment_id=? AND observation_key=?""",
                (version.attachment_id, version.observation_key),
            ).fetchone()
        return self._version_from_row(_require_row(row))

    def get_attachment_version(
        self, version_id: str
    ) -> OfficialAttachmentVersion | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_attachment_versions WHERE version_id=?",
                (version_id,),
            ).fetchone()
        return None if row is None else self._version_from_row(row)

    def get_latest_attachment_version(
        self, attachment_id: str, *, include_shadow: bool = False
    ) -> OfficialAttachmentVersion | None:
        visibility = "" if include_shadow else " AND visibility_state='production'"
        with self.connection() as conn:
            row = conn.execute(
                """SELECT * FROM official_attachment_versions
                   WHERE attachment_id=?"""
                + visibility
                + """
                   ORDER BY version_available_at DESC, observed_at DESC,
                            version_id DESC LIMIT 1""",
                (attachment_id,),
            ).fetchone()
        return None if row is None else self._version_from_row(row)

    def get_latest_valid_attachment_version(
        self, attachment_id: str, *, include_shadow: bool = False
    ) -> OfficialAttachmentVersion | None:
        visibility = "" if include_shadow else " AND visibility_state='production'"
        with self.connection() as conn:
            row = conn.execute(
                """SELECT * FROM official_attachment_versions
                   WHERE attachment_id=? AND integrity_status='valid'
                     AND content_hash IS NOT NULL"""
                + visibility
                + """
                   ORDER BY version_available_at DESC, observed_at DESC,
                            version_id DESC LIMIT 1""",
                (attachment_id,),
            ).fetchone()
        return None if row is None else self._version_from_row(row)

    def acquire_attachment_lease(
        self,
        attachment_id: str,
        *,
        lease_owner: str,
        lease_expires_at: str,
        now: str | None = None,
    ) -> bool:
        owner = str(lease_owner or "").strip()
        if not owner:
            raise ValueError("lease_owner is required")
        timestamp = now or utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_acquisition_leases
                   WHERE attachment_id=?""",
                (attachment_id,),
            ).fetchone()
            if (
                row is not None
                and row["lease_owner"] != owner
                and _iso_after(row["lease_expires_at"], timestamp)
            ):
                return False
            attempt = 1 if row is None else int(row["attempt"]) + 1
            lease_generation = (
                1 if row is None else int(row["lease_generation"] or 0) + 1
            )
            created_at = timestamp if row is None else row["created_at"]
            conn.execute(
                """
                INSERT INTO official_asset_acquisition_leases(
                    attachment_id, lease_owner, lease_expires_at, heartbeat_at,
                    lease_generation, attempt, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(attachment_id) DO UPDATE SET
                    lease_owner=excluded.lease_owner,
                    lease_expires_at=excluded.lease_expires_at,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_generation=excluded.lease_generation,
                    attempt=excluded.attempt,
                    updated_at=excluded.updated_at
                """,
                (
                    attachment_id,
                    owner,
                    lease_expires_at,
                    timestamp,
                    lease_generation,
                    attempt,
                    created_at,
                    timestamp,
                ),
            )
        return True

    def heartbeat_attachment_lease(
        self,
        attachment_id: str,
        *,
        lease_owner: str,
        lease_generation: int,
        lease_expires_at: str,
        now: str | None = None,
    ) -> bool:
        timestamp = now or utc_now_iso()
        with self.transaction() as conn:
            result = conn.execute(
                """UPDATE official_asset_acquisition_leases
                   SET lease_expires_at=?, heartbeat_at=?, updated_at=?
                   WHERE attachment_id=? AND lease_owner=?
                     AND lease_generation=?""",
                (
                    lease_expires_at,
                    timestamp,
                    timestamp,
                    attachment_id,
                    lease_owner,
                    int(lease_generation),
                ),
            )
        return result.rowcount == 1

    def release_attachment_lease(
        self,
        attachment_id: str,
        *,
        lease_owner: str,
        lease_generation: int,
    ) -> bool:
        with self.transaction() as conn:
            result = conn.execute(
                """DELETE FROM official_asset_acquisition_leases
                   WHERE attachment_id=? AND lease_owner=?
                     AND lease_generation=?""",
                (attachment_id, lease_owner, int(lease_generation)),
            )
        return result.rowcount == 1

    def get_attachment_lease(self, attachment_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_acquisition_leases WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        return None if row is None else dict(row)

    def artifact_lease_is_active(
        self,
        metadata: Mapping[str, Any],
        *,
        now: str | None = None,
        safety_grace_seconds: int = 0,
    ) -> bool:
        """Fail closed unless a sidecar's exact owner/generation is abandoned."""

        attachment_id = str(metadata.get("attachment_id") or "").strip()
        owner = str(metadata.get("owner") or metadata.get("lease_owner") or "").strip()
        generation_text = str(
            metadata.get("lease_generation") or metadata.get("generation") or ""
        ).strip()
        if not attachment_id or not owner or not generation_text:
            return True
        try:
            generation = int(generation_text)
        except ValueError:
            return True
        timestamp = now or utc_now_iso()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_acquisition_leases WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        if row is None:
            return False
        if (
            row["lease_owner"] != owner
            or int(row["lease_generation"] or 0) != generation
        ):
            return False
        if _iso_after(row["lease_expires_at"], timestamp):
            return True
        heartbeat = datetime.fromisoformat(
            str(row["heartbeat_at"]).replace("Z", "+00:00")
        )
        cutoff = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if heartbeat.tzinfo is None:
            heartbeat = heartbeat.replace(tzinfo=timezone.utc)
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        return (cutoff - heartbeat).total_seconds() <= max(0, int(safety_grace_seconds))

    def upsert_effective_report(
        self, report: EffectiveAnnualReport
    ) -> EffectiveAnnualReport:
        committed, _, activated = self.activate_effective_report(report)
        if not activated or committed is None:
            raise RuntimeError("effective annual-report upsert did not commit")
        return committed

    def activate_effective_report(
        self,
        report: EffectiveAnnualReport,
        *,
        expected_current_asset_id: str | None | object = _UNSET,
    ) -> tuple[EffectiveAnnualReport | None, str | None, bool]:
        """Atomically project the selected report and append its decision event."""
        now = utc_now_iso()
        instrument_id = normalize_instrument_id(report.instrument_id)
        fiscal_year = int(report.fiscal_year)
        with self.transaction() as conn:
            prior_row = conn.execute(
                """SELECT e.*, b.canonical_path AS prior_canonical_path
                   FROM effective_annual_reports e
                   LEFT JOIN official_document_blobs b ON b.content_hash=e.content_hash
                   WHERE e.instrument_id=? AND e.fiscal_year=?""",
                (instrument_id, fiscal_year),
            ).fetchone()
            prior = None if prior_row is None else self._effective_from_row(prior_row)
            if expected_current_asset_id is not _UNSET:
                actual_current_asset_id = None if prior is None else prior.asset_id
                if actual_current_asset_id != expected_current_asset_id:
                    return prior, None, False
            if prior is not None and prior.asset_id != report.asset_id:
                # The replacement edge is repository-owned evidence.  Do not
                # trust a stale caller to name a different predecessor.
                report = replace(report, predecessor_asset_id=prior.asset_id)
            if report.content_hash is None:
                raise ValueError(
                    "an effective annual-report decision requires a replacement blob"
                )
            decision_kind = (
                EffectiveDecisionKind.INITIAL_ACTIVATION
                if prior is None
                else (
                    EffectiveDecisionKind.PROJECTION_UPDATE
                    if prior.asset_id == report.asset_id
                    else EffectiveDecisionKind.REPLACEMENT
                )
            )
            event_type = (
                "added"
                if decision_kind is EffectiveDecisionKind.INITIAL_ACTIVATION
                else (
                    "repaired"
                    if decision_kind is EffectiveDecisionKind.PROJECTION_UPDATE
                    else "replaced"
                )
            )
            event_key = stable_id(
                "event",
                event_type,
                instrument_id,
                fiscal_year,
                None if prior is None else prior.asset_id,
                report.asset_id,
                report.last_checked_at,
                now,
            )
            decision_id = stable_id("decision", event_key)
            self._upsert_effective_conn(conn, report, now=now)
            if report.content_hash:
                pin_id = stable_id(
                    "pin", report.content_hash, "effective_asset", report.asset_id
                )
                conn.execute(
                    """INSERT INTO official_asset_retention_pins(
                           pin_id, blob_hash, pin_type, pin_key, created_at,
                           metadata_json
                       ) VALUES(?, ?, 'effective_asset', ?, ?, '{}')
                       ON CONFLICT(pin_id) DO UPDATE SET
                           released_at=NULL,
                           metadata_json=excluded.metadata_json""",
                    (pin_id, report.content_hash, report.asset_id, now),
                )
            if (
                prior is not None
                and prior.asset_id != report.asset_id
                and prior.content_hash
            ):
                conn.execute(
                    """UPDATE official_asset_retention_pins SET released_at=?
                       WHERE blob_hash=? AND pin_type='effective_asset'
                         AND pin_key=? AND released_at IS NULL""",
                    (now, prior.content_hash, prior.asset_id),
                )
            decision_policy_version = str(
                report.decision_evidence.get("decision_policy_version")
                or report.canonical_projection_policy_version
            )
            conn.execute(
                """INSERT INTO official_asset_change_events(
                       event_key, event_type, instrument_id, fiscal_year,
                       asset_id, predecessor_asset_id, content_hash,
                       trigger_origin, dispatch_policy_version,
                       payload_json, created_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_key,
                    event_type,
                    instrument_id,
                    fiscal_year,
                    report.asset_id,
                    None if prior is None else prior.asset_id,
                    report.content_hash,
                    "effective_decision",
                    CHANGE_EVENT_POLICY_VERSION,
                    canonical_json(
                        {
                            "schema_version": "official_asset_change_event.v1",
                            "decision_id": decision_id,
                            "decision_kind": decision_kind.value,
                            "report_period": report.report_period,
                            "source": report.source,
                            "source_announcement_id": report.source_announcement_id,
                            "decision_state": report.decision_state.value,
                            "availability": report.availability.value,
                            "visibility_state": report.visibility_state,
                            "pending_candidate_id": report.pending_candidate_id,
                            "decision_policy_version": decision_policy_version,
                        }
                    ),
                    now,
                ),
            )
            self._append_effective_decision_conn(
                conn,
                decision_id=decision_id,
                decision_kind=decision_kind,
                prior=prior,
                replacement=report,
                decision_state=report.decision_state,
                classifier_version=report.classifier_version,
                decision_policy_version=decision_policy_version,
                decision_reasons=report.decision_reasons,
                decision_evidence={
                    **dict(report.decision_evidence),
                    "equivalent_source_filings": [
                        item.as_dict() for item in report.equivalent_source_filings
                    ],
                    "evidence_set_hash": report.evidence_set_hash,
                    "variant": report.variant.value,
                    "availability": report.availability.value,
                },
                activated_at=report.activated_at or now,
                outbox_event_key=event_key,
                created_at=now,
            )
            row = conn.execute(
                """SELECT * FROM effective_annual_reports
                   WHERE instrument_id=? AND fiscal_year=?""",
                (instrument_id, fiscal_year),
            ).fetchone()
        return self._effective_from_row(_require_row(row)), None, True

    def withdraw_effective_report_without_replacement(
        self,
        instrument_id: str,
        fiscal_year: int,
        *,
        expected_current_asset_id: str,
        classifier_version: str,
        decision_policy_version: str,
        decision_reasons: Sequence[str],
        decision_evidence: Mapping[str, Any],
        activated_at: str | None = None,
    ) -> tuple[str | None, bool]:
        """Atomically clear a legally withdrawn winner and append a tombstone."""

        instrument_id = normalize_instrument_id(instrument_id)
        fiscal_year = int(fiscal_year)
        now = utc_now_iso()
        with self.transaction() as conn:
            prior_row = conn.execute(
                """SELECT e.*, b.canonical_path AS prior_canonical_path
                   FROM effective_annual_reports e
                   LEFT JOIN official_document_blobs b ON b.content_hash=e.content_hash
                   WHERE e.instrument_id=? AND e.fiscal_year=?""",
                (instrument_id, fiscal_year),
            ).fetchone()
            if prior_row is None:
                return None, False
            prior = self._effective_from_row(prior_row)
            if prior.asset_id != expected_current_asset_id:
                return None, False
            if not prior.content_hash or not prior_row["prior_canonical_path"]:
                raise ValueError(
                    "withdrawal tombstone requires a retained predecessor blob"
                )
            event_key = stable_id(
                "event",
                "withdrawn_without_replacement",
                instrument_id,
                fiscal_year,
                prior.asset_id,
                now,
            )
            decision_id = stable_id("decision", event_key)
            conn.execute(
                """INSERT INTO official_asset_change_events(
                       event_key, event_type, instrument_id, fiscal_year,
                       asset_id, predecessor_asset_id, content_hash,
                       trigger_origin, dispatch_policy_version,
                       payload_json, created_at
                   ) VALUES(?, 'withdrawn', ?, ?, NULL, ?, NULL,
                            'effective_decision', ?, ?, ?)""",
                (
                    event_key,
                    instrument_id,
                    fiscal_year,
                    prior.asset_id,
                    CHANGE_EVENT_POLICY_VERSION,
                    canonical_json(
                        {
                            "schema_version": "official_asset_change_event.v1",
                            "decision_id": decision_id,
                            "decision_kind": "withdrawn_without_replacement",
                            "decision_state": "withdrawn",
                            "availability": "blocked",
                        }
                    ),
                    now,
                ),
            )
            self._append_effective_decision_conn(
                conn,
                decision_id=decision_id,
                decision_kind=(EffectiveDecisionKind.WITHDRAWN_WITHOUT_REPLACEMENT),
                prior=prior,
                replacement=None,
                decision_state=EffectiveDecisionState.WITHDRAWN,
                classifier_version=classifier_version,
                decision_policy_version=decision_policy_version,
                decision_reasons=decision_reasons,
                decision_evidence=decision_evidence,
                activated_at=activated_at or now,
                outbox_event_key=event_key,
                created_at=now,
            )
            conn.execute(
                """UPDATE official_asset_retention_pins SET released_at=?
                   WHERE blob_hash=? AND pin_type='effective_asset'
                     AND pin_key=? AND released_at IS NULL""",
                (now, prior.content_hash, prior.asset_id),
            )
            conn.execute(
                """DELETE FROM effective_annual_reports
                   WHERE instrument_id=? AND fiscal_year=? AND asset_id=?""",
                (instrument_id, fiscal_year, prior.asset_id),
            )
        return None, True

    def list_effective_decisions(
        self,
        *,
        instrument_id: str | None = None,
        fiscal_year: int | None = None,
    ) -> list[EffectiveAnnualReportDecision]:
        clauses: list[str] = []
        params: list[Any] = []
        if instrument_id is not None:
            clauses.append("instrument_id=?")
            params.append(normalize_instrument_id(instrument_id))
        if fiscal_year is not None:
            clauses.append("fiscal_year=?")
            params.append(int(fiscal_year))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_annual_report_decisions"
                + where
                + " ORDER BY decision_sequence",
                tuple(params),
            ).fetchall()
        return [self._effective_decision_from_row(row) for row in rows]

    def get_effective_decision(
        self, decision_id: str
    ) -> EffectiveAnnualReportDecision | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_annual_report_decisions WHERE decision_id=?",
                (decision_id,),
            ).fetchone()
        return None if row is None else self._effective_decision_from_row(row)

    def get_effective_report(
        self,
        instrument_id: str,
        fiscal_year: int | None = None,
        *,
        document_family: str = "annual_report",
        knowledge_cutoff: str | None = None,
        include_shadow: bool = False,
    ) -> EffectiveAnnualReport | None:
        clauses = ["e.instrument_id=?", "e.document_family=?"]
        params: list[Any] = [
            normalize_instrument_id(instrument_id),
            str(document_family).strip().lower(),
        ]
        if not include_shadow:
            clauses.append("e.visibility_state='production'")
        if fiscal_year is not None:
            clauses.append("e.fiscal_year=?")
            params.append(int(fiscal_year))
        if knowledge_cutoff is not None:
            clauses.append(
                "(e.published_at IS NULL OR julianday(e.published_at)<=julianday(?))"
            )
            params.append(knowledge_cutoff)
            clauses.append(
                "(v.version_available_at IS NULL OR v.version_available_at='' "
                "OR julianday(v.version_available_at)<=julianday(?))"
            )
            params.append(knowledge_cutoff)
        sql = (
            "SELECT e.* FROM effective_annual_reports e "
            "LEFT JOIN official_attachment_versions v ON v.version_id=e.version_id "
            "WHERE "
            + " AND ".join(clauses)
            + " ORDER BY e.fiscal_year DESC, e.published_at DESC LIMIT 1"
        )
        with self.connection() as conn:
            row = conn.execute(sql, tuple(params)).fetchone()
        return None if row is None else self._effective_from_row(row)

    def get_effective_report_by_asset_id(
        self, asset_id: str, *, include_shadow: bool = False
    ) -> EffectiveAnnualReport | None:
        visibility = "" if include_shadow else " AND visibility_state='production'"
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM effective_annual_reports WHERE asset_id=?" + visibility,
                (asset_id,),
            ).fetchone()
        return None if row is None else self._effective_from_row(row)

    def list_effective_reports(
        self,
        *,
        instrument_id: str | None = None,
        document_family: str = "annual_report",
        source: str | None = None,
        availability: AssetAvailability | None = None,
        include_shadow: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EffectiveAnnualReport]:
        clauses: list[str] = ["document_family=?"]
        params: list[Any] = [str(document_family).strip().lower()]
        if not include_shadow:
            clauses.append("visibility_state='production'")
        if instrument_id:
            clauses.append("instrument_id=?")
            params.append(normalize_instrument_id(instrument_id))
        if source:
            clauses.append("source=?")
            params.append(normalize_source(source))
        if availability:
            clauses.append("availability=?")
            params.append(availability.value)
            if availability is AssetAvailability.LOCAL_VALID:
                clauses.append(
                    "EXISTS (SELECT 1 FROM official_document_blobs b "
                    "WHERE b.content_hash=effective_annual_reports.content_hash "
                    "AND b.integrity_status='valid' "
                    "AND NULLIF(TRIM(b.canonical_path), '') IS NOT NULL)"
                )
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend((max(1, min(int(limit), 1000)), max(0, int(offset))))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM effective_annual_reports"
                + where
                + " ORDER BY fiscal_year DESC, published_at DESC LIMIT ? OFFSET ?",
                tuple(params),
            ).fetchall()
        return [self._effective_from_row(row) for row in rows]

    def mark_effective_content_invalid(
        self,
        asset_id: str,
        *,
        integrity_status: IntegrityStatus,
        reason: str,
    ) -> EffectiveAnnualReport:
        """Persist a fail-closed projection after an external file mutation."""

        if integrity_status in {IntegrityStatus.VALID, IntegrityStatus.UNCHECKED}:
            raise ValueError(
                "invalid content projection requires a failing integrity status"
            )
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM effective_annual_reports WHERE asset_id=?",
                (str(asset_id),),
            ).fetchone()
            current = self._effective_from_row(_require_row(row))
            reasons = list(current.decision_reasons)
            if reason not in reasons:
                reasons.append(str(reason))
            evidence = dict(current.decision_evidence)
            evidence["content_integrity_failure"] = {
                "status": integrity_status.value,
                "reason": str(reason),
                "observed_at": now,
            }
            if current.content_hash:
                self._mark_content_hash_invalid_conn(
                    conn,
                    content_hash=current.content_hash,
                    integrity_status=integrity_status,
                    reason=str(reason),
                    now=now,
                )
            conn.execute(
                """UPDATE effective_annual_reports
                   SET availability='corrupt', decision_reasons_json=?,
                       decision_evidence_json=?, last_checked_at=?, updated_at=?
                   WHERE asset_id=?""",
                (
                    json.dumps(reasons, ensure_ascii=False),
                    canonical_json(evidence),
                    now,
                    now,
                    current.asset_id,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM effective_annual_reports WHERE asset_id=?",
                (current.asset_id,),
            ).fetchone()
        return self._effective_from_row(_require_row(updated))

    def mark_content_hash_invalid(
        self,
        content_hash: str,
        *,
        integrity_status: IntegrityStatus,
        reason: str,
    ) -> tuple[EffectiveAnnualReport, ...]:
        """Fail closed for every asset, version, and promotion gate sharing one blob."""

        if integrity_status in {IntegrityStatus.VALID, IntegrityStatus.UNCHECKED}:
            raise ValueError(
                "invalid content projection requires a failing integrity status"
            )
        now = utc_now_iso()
        with self.transaction() as conn:
            self._mark_content_hash_invalid_conn(
                conn,
                content_hash=str(content_hash),
                integrity_status=integrity_status,
                reason=str(reason),
                now=now,
            )
            rows = conn.execute(
                """SELECT * FROM effective_annual_reports
                   WHERE content_hash=? ORDER BY instrument_id, fiscal_year""",
                (str(content_hash),),
            ).fetchall()
        return tuple(self._effective_from_row(row) for row in rows)

    @staticmethod
    def _mark_content_hash_invalid_conn(
        conn: sqlite3.Connection,
        *,
        content_hash: str,
        integrity_status: IntegrityStatus,
        reason: str,
        now: str,
    ) -> None:
        conn.execute(
            """UPDATE official_document_blobs
               SET integrity_status=?, last_verified_at=?, updated_at=?
               WHERE content_hash=?""",
            (integrity_status.value, now, now, str(content_hash)),
        )
        conn.execute(
            """UPDATE official_attachment_versions
               SET integrity_status=?, error_code=?, updated_at=?
               WHERE content_hash=?""",
            (integrity_status.value, str(reason), now, str(content_hash)),
        )
        rows = conn.execute(
            """SELECT asset_id, decision_reasons_json, decision_evidence_json
               FROM effective_annual_reports WHERE content_hash=?""",
            (str(content_hash),),
        ).fetchall()
        for row in rows:
            reasons = _json_load(row["decision_reasons_json"], [])
            if str(reason) not in reasons:
                reasons.append(str(reason))
            evidence = _json_load(row["decision_evidence_json"], {})
            evidence["content_integrity_failure"] = {
                "status": integrity_status.value,
                "reason": str(reason),
                "observed_at": now,
            }
            conn.execute(
                """UPDATE effective_annual_reports
                   SET availability='corrupt', decision_reasons_json=?,
                       decision_evidence_json=?, last_checked_at=?, updated_at=?
                   WHERE asset_id=?""",
                (
                    json.dumps(reasons, ensure_ascii=False),
                    canonical_json(evidence),
                    now,
                    now,
                    row["asset_id"],
                ),
            )

    def create_or_reuse_operation(
        self,
        *,
        operation_type: str,
        idempotency_key: str,
        scope: Mapping[str, Any],
        policy_version: str,
        owner: str | None = None,
        stage: OperationStage | None = None,
    ) -> tuple[AssetOperation, bool]:
        operation_type = str(operation_type or "").strip()
        key = str(idempotency_key or "").strip()
        policy = str(policy_version or "").strip()
        if not operation_type or not key or not policy:
            raise ValueError("operation type, idempotency key, and policy are required")
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_operations
                   WHERE idempotency_key=? AND status IN ('queued', 'running')
                   ORDER BY created_at DESC LIMIT 1""",
                (key,),
            ).fetchone()
            created = row is None
            if row is not None:
                self._assert_operation_reuse_compatible(
                    row,
                    operation_type=operation_type,
                    scope=scope,
                    policy_version=policy,
                    idempotency_key=key,
                )
            if created:
                operation_id = stable_id("op", key, now)
                conn.execute(
                    """
                    INSERT INTO official_asset_operations(
                        operation_id, schema_version, operation_type,
                        idempotency_key, scope_json, policy_version, owner,
                        status, stage, outcome, attempt, progress_json,
                        diagnostics_json, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, 'queued', ?, NULL, 0, '{}', '{}', ?, ?)
                    """,
                    (
                        operation_id,
                        OPERATION_SCHEMA_VERSION,
                        operation_type,
                        key,
                        canonical_json(scope),
                        policy,
                        owner,
                        None if stage is None else stage.value,
                        now,
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM official_asset_operations WHERE operation_id=?",
                    (operation_id,),
                ).fetchone()
        return self._operation_from_row(_require_row(row)), created

    def create_or_reuse_asset_request(
        self,
        *,
        operation_type: str,
        operation_idempotency_key: str,
        scope: Mapping[str, Any],
        policy_version: str,
        principal: str,
        request_idempotency_key: str,
        request_fingerprint: str,
        consumer: str | None = None,
        stage: OperationStage | None = None,
        metadata: Mapping[str, Any] | None = None,
        authorized_projection: Mapping[str, Any] | None = None,
        consumer_continuation_id: str | None = None,
        expires_at: str | None = None,
        retention_policy_version: str = ASSET_REQUEST_RETENTION_POLICY_VERSION,
    ) -> tuple[AssetOperationSubscription, AssetOperation, bool, bool]:
        """Create one caller subscription over one global acquisition operation."""
        operation_type = str(operation_type or "").strip()
        operation_key = str(operation_idempotency_key or "").strip()
        policy = str(policy_version or "").strip()
        normalized_principal = str(principal or "").strip()
        request_key = str(request_idempotency_key or "").strip()
        fingerprint = str(request_fingerprint or "").strip()
        retention_version = str(retention_policy_version or "").strip()
        continuation_id = str(consumer_continuation_id or "").strip() or None
        if not all(
            (
                operation_type,
                operation_key,
                policy,
                normalized_principal,
                request_key,
                fingerprint,
                retention_version,
            )
        ):
            raise ValueError("asset request identity fields are required")
        now = utc_now_iso()
        active_expiry = (
            expires_at
            or (
                _parse_iso_datetime(now)
                + timedelta(seconds=ASSET_REQUEST_ACTIVE_TTL_SECONDS)
            ).isoformat()
        )
        if not _iso_after(active_expiry, now):
            raise ValueError("asset request expiry must be in the future")
        with self.transaction() as conn:
            subscription_row = conn.execute(
                """SELECT * FROM official_asset_operation_subscriptions
                   WHERE principal=? AND idempotency_key=?""",
                (normalized_principal, request_key),
            ).fetchone()
            if subscription_row is not None:
                if subscription_row["request_fingerprint"] != fingerprint:
                    raise IdempotencyConflictError(
                        "idempotency key is already bound to another request"
                    )
                operation_row = conn.execute(
                    "SELECT * FROM official_asset_operations WHERE operation_id=?",
                    (subscription_row["operation_id"],),
                ).fetchone()
                self._assert_operation_reuse_compatible(
                    _require_row(operation_row),
                    operation_type=operation_type,
                    scope=scope,
                    policy_version=policy,
                    idempotency_key=operation_key,
                )
                return (
                    self._subscription_from_row(subscription_row),
                    self._operation_from_row(_require_row(operation_row)),
                    False,
                    False,
                )

            operation_row = conn.execute(
                """SELECT operation.* FROM official_asset_operations AS operation
                   WHERE operation.idempotency_key=?
                     AND (
                         operation.status IN ('queued', 'running')
                         OR EXISTS (
                             SELECT 1
                             FROM official_asset_operation_subscriptions AS subscription
                             WHERE subscription.operation_id=operation.operation_id
                               AND subscription.status IN ('active', 'cancelled')
                               AND (
                                   subscription.expires_at IS NULL
                                   OR subscription.expires_at > ?
                               )
                         )
                     )
                   ORDER BY
                       CASE WHEN operation.status IN ('queued', 'running')
                            THEN 0 ELSE 1 END,
                       operation.created_at DESC
                   LIMIT 1""",
                (operation_key, now),
            ).fetchone()
            operation_created = operation_row is None
            if operation_row is not None:
                self._assert_operation_reuse_compatible(
                    operation_row,
                    operation_type=operation_type,
                    scope=scope,
                    policy_version=policy,
                    idempotency_key=operation_key,
                )
            if operation_created:
                operation_id = stable_id("op", operation_key, now)
                conn.execute(
                    """
                    INSERT INTO official_asset_operations(
                        operation_id, schema_version, operation_type,
                        idempotency_key, scope_json, policy_version, owner,
                        status, stage, outcome, attempt, progress_json,
                        diagnostics_json, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, NULL, 'queued', ?, NULL, 0, '{}', '{}', ?, ?)
                    """,
                    (
                        operation_id,
                        OPERATION_SCHEMA_VERSION,
                        operation_type,
                        operation_key,
                        canonical_json(scope),
                        policy,
                        None if stage is None else stage.value,
                        now,
                        now,
                    ),
                )
                operation_row = conn.execute(
                    "SELECT * FROM official_asset_operations WHERE operation_id=?",
                    (operation_id,),
                ).fetchone()

            asset_request_id = stable_id(
                "assetreq", normalized_principal, request_key, now
            )
            conn.execute(
                """
                INSERT INTO official_asset_operation_subscriptions(
                    asset_request_id, schema_version, operation_id, principal,
                    consumer, idempotency_key, request_fingerprint, status,
                    consumer_continuation_id, metadata_json,
                    authorized_projection_json, expires_at,
                    retention_policy_version, created_at, updated_at, cancelled_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    asset_request_id,
                    OPERATION_SUBSCRIPTION_SCHEMA_VERSION,
                    _require_row(operation_row)["operation_id"],
                    normalized_principal,
                    str(consumer).strip() if consumer else None,
                    request_key,
                    fingerprint,
                    continuation_id,
                    canonical_json(metadata or {}),
                    canonical_json(authorized_projection or {}),
                    active_expiry,
                    retention_version,
                    now,
                    now,
                ),
            )
            subscription_row = conn.execute(
                """SELECT * FROM official_asset_operation_subscriptions
                   WHERE asset_request_id=?""",
                (asset_request_id,),
            ).fetchone()
        return (
            self._subscription_from_row(_require_row(subscription_row)),
            self._operation_from_row(_require_row(operation_row)),
            True,
            operation_created,
        )

    @staticmethod
    def _assert_operation_reuse_compatible(
        row: sqlite3.Row,
        *,
        operation_type: str,
        scope: Mapping[str, Any],
        policy_version: str,
        idempotency_key: str,
    ) -> None:
        """Bind an active operation key to its complete work identity.

        The service normally derives the idempotency key from the acquisition
        work fingerprint.  Keeping the check in the repository is still
        necessary because scheduled jobs, tests, and future callers can invoke
        the repository directly.  A reused key must never silently combine a
        different route, integrity policy, bound, or selector scope.
        """
        stored_scope = _json_load(row["scope_json"], {})
        mismatches: list[str] = []
        if str(row["operation_type"] or "") != str(operation_type):
            mismatches.append("operation_type")
        if str(row["policy_version"] or "") != str(policy_version):
            mismatches.append("policy_version")
        if str(row["idempotency_key"] or "") != str(idempotency_key):
            mismatches.append("idempotency_key")
        stored_fingerprint = stored_scope.get(
            "acquisition_work_fingerprint"
        ) or stored_scope.get("request_fingerprint")
        incoming_fingerprint = scope.get("acquisition_work_fingerprint") or scope.get(
            "request_fingerprint"
        )
        if stored_fingerprint is not None or incoming_fingerprint is not None:
            if str(stored_fingerprint or "") != str(incoming_fingerprint or ""):
                mismatches.append("scope_or_work_fingerprint")
        elif canonical_json(stored_scope) != canonical_json(scope):
            mismatches.append("scope_or_work_fingerprint")
        if mismatches:
            raise IdempotencyConflictError(
                "active operation identity conflict: " + ",".join(mismatches)
            )

    def get_asset_request(
        self,
        asset_request_id: str,
        *,
        principal: str | None = None,
    ) -> AssetOperationSubscription | None:
        clauses = ["asset_request_id=?"]
        params: list[Any] = [asset_request_id]
        if principal is not None:
            clauses.append("principal=?")
            params.append(str(principal).strip())
        now = utc_now_iso()
        tombstone_until = (
            _parse_iso_datetime(now)
            + timedelta(seconds=ASSET_REQUEST_TOMBSTONE_TTL_SECONDS)
        ).isoformat()
        with self.transaction() as conn:
            conn.execute(
                """UPDATE official_asset_operation_subscriptions
                   SET status='expired', expired_at=?, tombstone_until=?, updated_at=?
                   WHERE asset_request_id=? AND status='active'
                     AND expires_at IS NOT NULL AND expires_at<=?""",
                (now, tombstone_until, now, asset_request_id, now),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_operation_subscriptions WHERE "
                + " AND ".join(clauses),
                tuple(params),
            ).fetchone()
        return None if row is None else self._subscription_from_row(row)

    def cancel_asset_request(
        self,
        asset_request_id: str,
        *,
        principal: str,
    ) -> AssetOperationSubscription:
        """Cancel only the caller subscription, never the shared operation."""
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_operation_subscriptions
                   WHERE asset_request_id=? AND principal=?""",
                (asset_request_id, str(principal).strip()),
            ).fetchone()
            current = self._subscription_from_row(_require_row(row))
            if current.status is AssetRequestStatus.ACTIVE:
                conn.execute(
                    """UPDATE official_asset_operation_subscriptions
                       SET status='cancelled', cancelled_at=?, updated_at=?
                       WHERE asset_request_id=?""",
                    (now, now, asset_request_id),
                )
                row = conn.execute(
                    """SELECT * FROM official_asset_operation_subscriptions
                       WHERE asset_request_id=?""",
                    (asset_request_id,),
                ).fetchone()
        return self._subscription_from_row(_require_row(row))

    def expire_asset_request(
        self,
        asset_request_id: str,
        *,
        principal: str,
        tombstone_until: str,
    ) -> AssetOperationSubscription:
        """Expire only the caller projection; the internal operation is unchanged."""
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_operation_subscriptions
                   WHERE asset_request_id=? AND principal=?""",
                (asset_request_id, str(principal).strip()),
            ).fetchone()
            current = self._subscription_from_row(_require_row(row))
            if current.status is not AssetRequestStatus.EXPIRED:
                conn.execute(
                    """UPDATE official_asset_operation_subscriptions
                       SET status='expired', expired_at=?, tombstone_until=?,
                           updated_at=?
                       WHERE asset_request_id=?""",
                    (now, tombstone_until, now, asset_request_id),
                )
                row = conn.execute(
                    """SELECT * FROM official_asset_operation_subscriptions
                       WHERE asset_request_id=?""",
                    (asset_request_id,),
                ).fetchone()
        return self._subscription_from_row(_require_row(row))

    def list_asset_requests(
        self,
        *,
        operation_id: str | None = None,
        principal: str | None = None,
        active_only: bool = False,
    ) -> list[AssetOperationSubscription]:
        clauses: list[str] = []
        params: list[Any] = []
        if operation_id:
            clauses.append("operation_id=?")
            params.append(operation_id)
        if principal:
            clauses.append("principal=?")
            params.append(str(principal).strip())
        if active_only:
            clauses.append("status='active'")
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        now = utc_now_iso()
        tombstone_until = (
            _parse_iso_datetime(now)
            + timedelta(seconds=ASSET_REQUEST_TOMBSTONE_TTL_SECONDS)
        ).isoformat()
        with self.transaction() as conn:
            conn.execute(
                """UPDATE official_asset_operation_subscriptions
                   SET status='expired', expired_at=?, tombstone_until=?, updated_at=?
                   WHERE status='active' AND expires_at IS NOT NULL AND expires_at<=?""",
                (now, tombstone_until, now, now),
            )
            rows = conn.execute(
                "SELECT * FROM official_asset_operation_subscriptions"
                + where
                + " ORDER BY created_at, asset_request_id",
                tuple(params),
            ).fetchall()
        return [self._subscription_from_row(row) for row in rows]

    def get_operation(self, operation_id: str) -> AssetOperation | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return None if row is None else self._operation_from_row(row)

    def list_operations(
        self,
        *,
        operation_type: str | None = None,
        status: OperationStatus | None = None,
        limit: int = 100,
    ) -> list[AssetOperation]:
        clauses: list[str] = []
        params: list[Any] = []
        if operation_type:
            clauses.append("operation_type=?")
            params.append(str(operation_type).strip())
        if status is not None:
            clauses.append("status=?")
            params.append(status.value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(max(1, min(int(limit), 1000)))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_asset_operations"
                + where
                + " ORDER BY created_at DESC, operation_id DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [self._operation_from_row(row) for row in rows]

    def request_operation_stop(
        self,
        operation_id: str,
        *,
        principal: str,
    ) -> AssetOperation:
        """Request cooperative stop without discarding committed checkpoints."""
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            current = self._operation_from_row(_require_row(row))
            if current.status is OperationStatus.QUEUED:
                conn.execute(
                    """UPDATE official_asset_operations SET
                           status='cancelled', outcome='partial', reason_code='operator_stop',
                           progress_json=?, finished_at=?, updated_at=?
                       WHERE operation_id=?""",
                    (
                        canonical_json(
                            {
                                **current.progress,
                                "stop_requested": True,
                                "stop_requested_by": str(principal).strip(),
                                "stop_requested_at": now,
                            }
                        ),
                        now,
                        now,
                        operation_id,
                    ),
                )
            elif current.status is OperationStatus.RUNNING:
                conn.execute(
                    """UPDATE official_asset_operations SET progress_json=?, updated_at=?
                       WHERE operation_id=?""",
                    (
                        canonical_json(
                            {
                                **current.progress,
                                "stop_requested": True,
                                "stop_requested_by": str(principal).strip(),
                                "stop_requested_at": now,
                            }
                        ),
                        now,
                        operation_id,
                    ),
                )
            elif current.status is not OperationStatus.CANCELLED:
                raise ValueError("terminal operation cannot be stopped")
            updated = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._operation_from_row(_require_row(updated))

    def operation_stop_requested(self, operation_id: str) -> bool:
        operation = self.get_operation(operation_id)
        return bool(operation and operation.progress.get("stop_requested"))

    def resume_operation(
        self,
        operation_id: str,
        *,
        principal: str,
    ) -> AssetOperation:
        """Resume one logical run in place and increment its generation."""
        now = utc_now_iso()
        terminal_resumable = {
            OperationStatus.CANCELLED,
            OperationStatus.FAILED,
            OperationStatus.BLOCKED,
            OperationStatus.MISSING,
        }
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            current = self._operation_from_row(_require_row(row))
            stale_non_terminal = current.status in {
                OperationStatus.QUEUED,
                OperationStatus.RUNNING,
            } and (
                current.lease_expires_at is None
                or not _iso_after(current.lease_expires_at, now)
            )
            if current.status not in terminal_resumable and not stale_non_terminal:
                raise ValueError("operation is not resumable")
            conflicting = conn.execute(
                """SELECT operation_id FROM official_asset_operations
                   WHERE idempotency_key=? AND status IN ('queued', 'running')
                     AND operation_id<>? LIMIT 1""",
                (current.idempotency_key, operation_id),
            ).fetchone()
            if conflicting is not None:
                raise RuntimeError("equivalent operation is already active")
            generation = int(current.progress.get("resume_generation", 0)) + 1
            progress = dict(current.progress)
            progress.update(
                {
                    "resume_generation": generation,
                    "resumed_by": str(principal).strip(),
                    "resumed_at": now,
                    "stop_requested": False,
                }
            )
            conn.execute(
                """UPDATE official_asset_operations SET
                       status='queued', outcome=NULL, reason_code=NULL,
                       next_retry_at=NULL, lease_owner=NULL, lease_expires_at=NULL,
                       heartbeat_at=NULL, finished_at=NULL, progress_json=?, updated_at=?
                   WHERE operation_id=?""",
                (canonical_json(progress), now, operation_id),
            )
            updated = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._operation_from_row(_require_row(updated))

    def heartbeat_operation(
        self,
        operation_id: str,
        *,
        lease_owner: str,
        lease_generation: int,
        lease_expires_at: str,
        progress: Mapping[str, Any] | None = None,
    ) -> AssetOperation:
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            current = self._operation_from_row(_require_row(row))
            if current.status is not OperationStatus.RUNNING:
                raise ValueError("only running operations may heartbeat")
            if current.lease_owner != str(lease_owner).strip():
                raise RuntimeError("operation lease owner mismatch")
            if current.lease_generation != int(lease_generation):
                raise RuntimeError("operation lease generation mismatch")
            conn.execute(
                """UPDATE official_asset_operations SET
                       lease_expires_at=?, heartbeat_at=?, progress_json=?, updated_at=?
                   WHERE operation_id=?""",
                (
                    lease_expires_at,
                    now,
                    canonical_json(progress or current.progress),
                    now,
                    operation_id,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._operation_from_row(_require_row(updated))

    def claim_operation(
        self,
        operation_id: str,
        *,
        lease_owner: str,
        lease_expires_at: str,
        stage: OperationStage | None = None,
    ) -> AssetOperation:
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            current = self._operation_from_row(_require_row(row))
            if current.status not in {OperationStatus.QUEUED, OperationStatus.RUNNING}:
                raise ValueError("terminal operation cannot be claimed")
            if (
                current.status is OperationStatus.RUNNING
                and current.lease_expires_at
                and _iso_after(current.lease_expires_at, now)
            ):
                raise RuntimeError("operation lease is already held")
            conn.execute(
                """UPDATE official_asset_operations SET
                       status='running', stage=COALESCE(?, stage),
                       lease_owner=?, lease_expires_at=?, heartbeat_at=?,
                       lease_generation=lease_generation+1,
                       attempt=attempt+1, started_at=COALESCE(started_at, ?), updated_at=?
                   WHERE operation_id=?""",
                (
                    None if stage is None else stage.value,
                    lease_owner,
                    lease_expires_at,
                    now,
                    now,
                    now,
                    operation_id,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._operation_from_row(_require_row(updated))

    def transition_operation(
        self,
        operation_id: str,
        status: OperationStatus,
        *,
        stage: OperationStage | None = None,
        outcome: BatchOutcome | None = None,
        progress: Mapping[str, Any] | None = None,
        result_asset_id: str | None = None,
        result_origin: ResultOrigin | None = None,
        reason_code: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
        next_retry_at: str | None = None,
        expected_lease_owner: str | None = None,
        expected_lease_generation: int | None = None,
    ) -> AssetOperation:
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            current = self._operation_from_row(_require_row(row))
            fenced = (
                expected_lease_owner is not None
                or expected_lease_generation is not None
            )
            if fenced and (
                expected_lease_owner is None or expected_lease_generation is None
            ):
                raise ValueError(
                    "expected lease owner and generation must be provided together"
                )
            if fenced and current.lease_owner != str(expected_lease_owner).strip():
                raise RuntimeError("operation lease owner mismatch")
            if fenced and current.lease_generation != int(expected_lease_generation):
                raise RuntimeError("operation lease generation mismatch")
            if (
                current.status is OperationStatus.RUNNING
                and status.value not in ACTIVE_OPERATION_STATUSES
                and not fenced
            ):
                raise ValueError(
                    "running operation terminal transition requires lease fencing"
                )
            effective_reason_code = (
                current.reason_code
                if status is current.status and reason_code is None
                else reason_code
            )
            if (
                status != current.status
                and status not in _ALLOWED_OPERATION_TRANSITIONS[current.status]
            ):
                raise ValueError(
                    f"invalid operation transition: {current.status.value}->{status.value}"
                )
            finished_at = now if status.value not in ACTIVE_OPERATION_STATUSES else None
            conn.execute(
                """UPDATE official_asset_operations SET
                       status=?, stage=COALESCE(?, stage), outcome=?,
                       progress_json=?, result_asset_id=?, result_origin=?,
                       reason_code=?, diagnostics_json=?, next_retry_at=?,
                       lease_owner=CASE WHEN ? IS NULL THEN lease_owner ELSE NULL END,
                       lease_expires_at=CASE WHEN ? IS NULL THEN lease_expires_at ELSE NULL END,
                       finished_at=COALESCE(?, finished_at), updated_at=?
                   WHERE operation_id=?""",
                (
                    status.value,
                    None if stage is None else stage.value,
                    None if outcome is None else outcome.value,
                    canonical_json(progress or current.progress),
                    result_asset_id or current.result_asset_id,
                    None if result_origin is None else result_origin.value,
                    effective_reason_code,
                    canonical_json(diagnostics or {}),
                    next_retry_at,
                    finished_at,
                    finished_at,
                    finished_at,
                    now,
                    operation_id,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM official_asset_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._operation_from_row(_require_row(updated))

    def append_change_event(
        self,
        *,
        event_key: str,
        event_type: str,
        instrument_id: str,
        fiscal_year: int,
        asset_id: str | None,
        predecessor_asset_id: str | None,
        content_hash: str | None,
        trigger_origin: str,
        dispatch_policy_version: str,
        payload: Mapping[str, Any] | None = None,
    ) -> int:
        origin = str(trigger_origin or "").strip()
        dispatch_version = str(dispatch_policy_version or "").strip()
        if not origin or origin == "unknown" or not dispatch_version:
            raise ValueError("change event origin and dispatch policy are required")
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_asset_change_events(
                    event_key, event_type, instrument_id, fiscal_year, asset_id,
                    predecessor_asset_id, content_hash, trigger_origin,
                    dispatch_policy_version, payload_json, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO NOTHING
                """,
                (
                    event_key,
                    event_type,
                    normalize_instrument_id(instrument_id),
                    int(fiscal_year),
                    asset_id,
                    predecessor_asset_id,
                    content_hash,
                    origin,
                    dispatch_version,
                    canonical_json(payload or {}),
                    utc_now_iso(),
                ),
            )
            row = conn.execute(
                "SELECT event_id FROM official_asset_change_events WHERE event_key=?",
                (event_key,),
            ).fetchone()
        return int(_require_row(row)["event_id"])

    def list_change_events(
        self, *, after_event_id: int = 0, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT * FROM official_asset_change_events
                   WHERE event_id>? ORDER BY event_id LIMIT ?""",
                (max(0, int(after_event_id)), max(1, min(int(limit), 1000))),
            ).fetchall()
        return [_decode_row(row, json_fields=("payload_json",)) for row in rows]

    def acquire_read_lease(
        self,
        *,
        blob_hash: str,
        owner: str,
        ttl_seconds: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a deletion-blocking reader lease for one physical blob."""

        normalized_hash = str(blob_hash or "").strip().lower()
        normalized_owner = str(owner or "").strip()
        bounded_ttl = int(ttl_seconds)
        if not normalized_hash or not normalized_owner:
            raise ValueError("read lease blob hash and owner are required")
        if bounded_ttl <= 0:
            raise ValueError("read lease ttl must be positive")
        now = datetime.now(timezone.utc)
        now_text = now.isoformat()
        expires_at = (now + timedelta(seconds=bounded_ttl)).isoformat()
        lease_id = stable_id(
            "read-lease",
            normalized_hash,
            normalized_owner,
            uuid.uuid4().hex,
        )
        evidence = {
            **dict(metadata or {}),
            "lease_id": lease_id,
            "lease_generation": 1,
            "heartbeat_at": now_text,
            "lease_expires_at": expires_at,
        }
        with self.transaction() as conn:
            blob = conn.execute(
                "SELECT 1 FROM official_document_blobs WHERE content_hash=?",
                (normalized_hash,),
            ).fetchone()
            if blob is None:
                raise KeyError(f"announcement blob not found: {normalized_hash}")
            conn.execute(
                """INSERT INTO official_asset_retention_pins(
                       pin_id, blob_hash, pin_type, pin_key, owner, created_at,
                       expires_at, metadata_json
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    lease_id,
                    normalized_hash,
                    READ_LEASE_PIN_TYPE,
                    lease_id,
                    normalized_owner,
                    now_text,
                    expires_at,
                    canonical_json(evidence),
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_retention_pins WHERE pin_id=?",
                (lease_id,),
            ).fetchone()
        return _decode_row(_require_row(row), json_fields=("metadata_json",))

    def heartbeat_read_lease(
        self,
        lease_id: str,
        *,
        owner: str,
        expected_generation: int,
        ttl_seconds: int,
    ) -> dict[str, Any] | None:
        """Extend an unexpired reader lease using an owner/generation fence."""

        bounded_ttl = int(ttl_seconds)
        if bounded_ttl <= 0:
            raise ValueError("read lease ttl must be positive")
        now = datetime.now(timezone.utc)
        now_text = now.isoformat()
        expires_at = (now + timedelta(seconds=bounded_ttl)).isoformat()
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_retention_pins
                   WHERE pin_id=? AND pin_type=?""",
                (str(lease_id), READ_LEASE_PIN_TYPE),
            ).fetchone()
            if row is None or row["released_at"] is not None:
                return None
            metadata = _json_load(row["metadata_json"], {})
            if (
                row["owner"] != str(owner).strip()
                or int(metadata.get("lease_generation") or 0)
                != int(expected_generation)
                or not _iso_after(row["expires_at"], now_text)
            ):
                return None
            metadata.update(
                {
                    "lease_generation": int(expected_generation) + 1,
                    "heartbeat_at": now_text,
                    "lease_expires_at": expires_at,
                }
            )
            previous_metadata = row["metadata_json"]
            updated = conn.execute(
                """UPDATE official_asset_retention_pins
                   SET expires_at=?, metadata_json=?
                   WHERE pin_id=? AND pin_type=? AND owner=?
                     AND released_at IS NULL AND expires_at>?
                     AND metadata_json=?""",
                (
                    expires_at,
                    canonical_json(metadata),
                    str(lease_id),
                    READ_LEASE_PIN_TYPE,
                    str(owner).strip(),
                    now_text,
                    previous_metadata,
                ),
            )
            if updated.rowcount != 1:
                return None
            refreshed = conn.execute(
                "SELECT * FROM official_asset_retention_pins WHERE pin_id=?",
                (str(lease_id),),
            ).fetchone()
        return _decode_row(_require_row(refreshed), json_fields=("metadata_json",))

    def release_read_lease(
        self,
        lease_id: str,
        *,
        owner: str,
        expected_generation: int,
    ) -> bool:
        """Release one reader lease without permitting stale-owner cleanup."""

        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_retention_pins
                   WHERE pin_id=? AND pin_type=?""",
                (str(lease_id), READ_LEASE_PIN_TYPE),
            ).fetchone()
            if row is None or row["released_at"] is not None:
                return False
            metadata = _json_load(row["metadata_json"], {})
            if row["owner"] != str(owner).strip() or int(
                metadata.get("lease_generation") or 0
            ) != int(expected_generation):
                return False
            result = conn.execute(
                """UPDATE official_asset_retention_pins SET released_at=?
                   WHERE pin_id=? AND pin_type=? AND owner=?
                     AND released_at IS NULL AND metadata_json=?""",
                (
                    now,
                    str(lease_id),
                    READ_LEASE_PIN_TYPE,
                    str(owner).strip(),
                    row["metadata_json"],
                ),
            )
        return result.rowcount == 1

    def get_read_lease(self, lease_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_retention_pins
                   WHERE pin_id=? AND pin_type=?""",
                (str(lease_id), READ_LEASE_PIN_TYPE),
            ).fetchone()
        return None if row is None else _decode_row(row, json_fields=("metadata_json",))

    def get_asset_content_lifecycle_state(self, asset_id: str) -> str | None:
        """Return the public content lifecycle state for one stable asset id."""

        normalized_id = str(asset_id or "").strip()
        if not normalized_id:
            return None
        with self.connection() as conn:
            current = conn.execute(
                """SELECT availability FROM effective_annual_reports
                   WHERE asset_id=? AND visibility_state='production'""",
                (normalized_id,),
            ).fetchone()
            if current is not None:
                return str(current["availability"])
            decision = conn.execute(
                """SELECT decision_kind FROM official_annual_report_decisions
                   WHERE predecessor_asset_id=?
                   ORDER BY decision_sequence DESC LIMIT 1""",
                (normalized_id,),
            ).fetchone()
        if decision is None:
            return None
        if (
            decision is not None
            and decision["decision_kind"] == "withdrawn_without_replacement"
        ):
            return "withdrawn"
        return "superseded"

    def claim_discovery_state(
        self,
        *,
        source: str,
        exchange: str,
        category: str,
        scope_key: str,
        config_fingerprint: str,
        lease_owner: str,
        lease_expires_at: str,
        now: str | None = None,
        operation_id: str | None = None,
        observation_key: str | None = None,
        max_attempts: int = 4,
        reopen_reason: str | None = None,
        repair_actor: str | None = None,
    ) -> dict[str, Any]:
        """Claim or reclaim one source scope and return its fencing token."""
        owner = str(lease_owner or "").strip()
        if not owner:
            raise ValueError("discovery lease owner is required")
        allowed_reopen = {None, "new_observation", "due_repair", "audited_repair"}
        if reopen_reason not in allowed_reopen:
            raise ValueError("invalid discovery retry reopen reason")
        if reopen_reason == "audited_repair" and not str(repair_actor or "").strip():
            raise ValueError("audited discovery repair requires an actor")
        if int(max_attempts) < 1:
            raise ValueError("max_attempts must be positive")
        timestamp = now or utc_now_iso()
        if not _iso_after(lease_expires_at, timestamp):
            raise ValueError("discovery lease expiry must be after claim time")
        key = (
            normalize_source(source),
            exchange.upper(),
            category,
            scope_key,
            config_fingerprint,
        )
        with self.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_discovery_state
                   WHERE source=? AND exchange=? AND category=? AND scope_key=?
                     AND config_fingerprint=?""",
                key,
            ).fetchone()
            if (
                row is not None
                and row["lease_owner"]
                and row["lease_expires_at"]
                and _iso_after(row["lease_expires_at"], timestamp)
                and row["lease_owner"] != owner
            ):
                raise DiscoveryStateFenceError(
                    f"discovery scope is leased by {row['lease_owner']}"
                )
            incoming_observation = str(observation_key or "").strip()
            existing_observation = (
                "" if row is None else str(row["observation_key"] or "")
            )
            new_observation = bool(
                incoming_observation and incoming_observation != existing_observation
            )
            if new_observation:
                reopen_reason = "new_observation"
            if (
                row is not None
                and row["status"]
                in {
                    "blocked",
                    "exhausted",
                    "operator_hold",
                }
                and not (
                    new_observation or reopen_reason in {"due_repair", "audited_repair"}
                )
            ):
                raise DiscoveryRetryBlockedError(
                    "terminal discovery retry requires new observation or governed repair"
                )
            if (
                row is not None
                and row["status"] == "retryable"
                and not new_observation
                and reopen_reason not in {"due_repair", "audited_repair"}
                and row["next_retry_at"]
                and _iso_after(row["next_retry_at"], timestamp)
            ):
                raise DiscoveryRetryNotDueError(
                    f"discovery retry is not due until {row['next_retry_at']}"
                )
            reset_attempt = bool(
                row is None
                or new_observation
                or reopen_reason in {"due_repair", "audited_repair"}
            )
            attempt = 1 if reset_attempt else int(row["attempt"] or 0) + 1
            effective_observation = incoming_observation or existing_observation
            if row is None:
                conn.execute(
                    """INSERT INTO official_asset_discovery_state(
                           source, exchange, category, scope_key, config_fingerprint,
                           schema_version, status, is_complete, checkpoint_json,
                           lease_owner, lease_expires_at, lease_generation,
                           state_version, operation_id, observation_key,
                           attempt, max_attempts, consumes_retry_budget,
                           reopen_reason, reopened_at, repair_actor,
                           created_at, updated_at
                       ) VALUES(?, ?, ?, ?, ?, ?, 'leased', 0, '{}', ?, ?, 1, 1,
                                ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)""",
                    (
                        *key,
                        DISCOVERY_STATE_SCHEMA_VERSION,
                        owner,
                        lease_expires_at,
                        operation_id,
                        effective_observation,
                        attempt,
                        int(max_attempts),
                        reopen_reason,
                        timestamp if reopen_reason else None,
                        repair_actor,
                        timestamp,
                        timestamp,
                    ),
                )
            else:
                conn.execute(
                    """UPDATE official_asset_discovery_state
                       SET status='leased', lease_owner=?, lease_expires_at=?,
                           lease_generation=lease_generation+1,
                           state_version=state_version+1,
                           operation_id=COALESCE(?, operation_id),
                           observation_key=?, attempt=?, max_attempts=?,
                           next_retry_at=NULL,
                           last_error_code=CASE WHEN ? THEN NULL ELSE last_error_code END,
                           failure_class=CASE WHEN ? THEN NULL ELSE failure_class END,
                           operator_action_required=0, consumes_retry_budget=1,
                           reopen_reason=?, reopened_at=?, repair_actor=?, updated_at=?
                       WHERE source=? AND exchange=? AND category=? AND scope_key=?
                         AND config_fingerprint=?""",
                    (
                        owner,
                        lease_expires_at,
                        operation_id,
                        effective_observation,
                        attempt,
                        int(max_attempts),
                        int(reset_attempt),
                        int(reset_attempt),
                        reopen_reason,
                        timestamp if reopen_reason else row["reopened_at"],
                        repair_actor,
                        timestamp,
                        *key,
                    ),
                )
            claimed = conn.execute(
                """SELECT * FROM official_asset_discovery_state
                   WHERE source=? AND exchange=? AND category=? AND scope_key=?
                     AND config_fingerprint=?""",
                key,
            ).fetchone()
        return _decode_row(_require_row(claimed), json_fields=("checkpoint_json",))

    def upsert_discovery_state(
        self,
        *,
        source: str,
        exchange: str,
        category: str,
        scope_key: str,
        config_fingerprint: str,
        status: str,
        is_complete: bool,
        covered_until: str | None,
        run_cutoff: str | None,
        item_cursor_kind: str | None = None,
        item_cursor_value: str | None = None,
        next_page: int | None = None,
        gap_reason: str | None = None,
        checkpoint: Mapping[str, Any] | None = None,
        expected_lease_owner: str | None = None,
        expected_lease_generation: int | None = None,
        expected_state_version: int | None = None,
        release_lease: bool = True,
        next_retry_at: str | None = None,
        error_code: str | None = None,
        failure_class: str | None = None,
        operator_action_required: bool = False,
        consumes_retry_budget: bool = True,
        project_parent_block: bool = True,
    ) -> dict[str, Any]:
        """Commit a discovery checkpoint, optionally under a fencing token.

        A worker that acquired a lease must provide all three expected values.
        The conditional update then acts as a compare-and-swap, so an expired
        worker cannot regress a newer cursor, gap, or completion state.
        """
        fenced = any(
            value is not None
            for value in (
                expected_lease_owner,
                expected_lease_generation,
                expected_state_version,
            )
        )
        if fenced and (
            not str(expected_lease_owner or "").strip()
            or expected_lease_generation is None
            or expected_state_version is None
        ):
            raise ValueError(
                "fenced discovery commit requires lease owner, generation, and state version"
            )
        now = utc_now_iso()
        with self.transaction() as conn:
            existing = conn.execute(
                """SELECT * FROM official_asset_discovery_state
                   WHERE source=? AND exchange=? AND category=? AND scope_key=?
                     AND config_fingerprint=?""",
                (
                    normalize_source(source),
                    exchange.upper(),
                    category,
                    scope_key,
                    config_fingerprint,
                ),
            ).fetchone()
            if (
                not fenced
                and existing is not None
                and int(existing["lease_generation"] or 0) > 0
            ):
                raise DiscoveryStateFenceError(
                    "discovery state update requires its current fencing token"
                )
            if fenced:
                if existing is None:
                    raise DiscoveryStateFenceError("discovery state disappeared")
                mismatches = []
                if existing["lease_owner"] != str(expected_lease_owner):
                    mismatches.append("lease_owner")
                if int(existing["lease_generation"] or 0) != int(
                    expected_lease_generation
                ):
                    mismatches.append("lease_generation")
                if int(existing["state_version"] or 0) != int(expected_state_version):
                    mismatches.append("state_version")
                if mismatches:
                    raise DiscoveryStateFenceError(
                        "discovery state fence mismatch: " + ",".join(mismatches)
                    )
            prior_covered = None if existing is None else existing["covered_until"]
            committed_covered = covered_until if is_complete else prior_covered
            if (
                prior_covered
                and committed_covered
                and committed_covered < prior_covered
            ):
                raise ValueError("covered_until cannot move backwards")
            key = (
                normalize_source(source),
                exchange.upper(),
                category,
                scope_key,
                config_fingerprint,
            )
            values = (
                item_cursor_kind,
                item_cursor_value,
                committed_covered,
                run_cutoff,
                next_page,
                status,
                int(is_complete),
                gap_reason,
                canonical_json(checkpoint or {}),
                next_retry_at,
                error_code,
                failure_class,
                int(operator_action_required),
                int(consumes_retry_budget),
                now,
            )
            if fenced:
                lease_owner = None if release_lease else str(expected_lease_owner)
                cursor = conn.execute(
                    """UPDATE official_asset_discovery_state
                       SET item_cursor_kind=?, item_cursor_value=?, covered_until=?,
                           run_cutoff=?, next_page=?, status=?, is_complete=?,
                           gap_reason=?, checkpoint_json=?, next_retry_at=?,
                           last_error_code=?, failure_class=?,
                           operator_action_required=?, consumes_retry_budget=?,
                           attempt=CASE WHEN ?=0 THEN MAX(attempt-1, 0) ELSE attempt END,
                           updated_at=?,
                           state_version=state_version+1,
                           lease_owner=?, lease_expires_at=?
                       WHERE source=? AND exchange=? AND category=? AND scope_key=?
                         AND config_fingerprint=? AND lease_owner=?
                         AND lease_generation=? AND state_version=?""",
                    (
                        *values[:-1],
                        int(consumes_retry_budget),
                        values[-1],
                        lease_owner,
                        None if release_lease else existing["lease_expires_at"],
                        *key,
                        str(expected_lease_owner),
                        int(expected_lease_generation),
                        int(expected_state_version),
                    ),
                )
                if cursor.rowcount != 1:
                    raise DiscoveryStateFenceError(
                        "discovery state fence lost during commit"
                    )
            else:
                conn.execute(
                    """
                    INSERT INTO official_asset_discovery_state(
                        source, exchange, category, scope_key, config_fingerprint,
                        schema_version,
                        item_cursor_kind, item_cursor_value, covered_until,
                        run_cutoff, next_page, status, is_complete, gap_reason,
                        checkpoint_json, next_retry_at, last_error_code,
                        failure_class, operator_action_required,
                        consumes_retry_budget, lease_owner, lease_expires_at,
                        lease_generation, state_version, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, 1, ?, ?)
                    ON CONFLICT(source, exchange, category, scope_key, config_fingerprint)
                    DO UPDATE SET
                        item_cursor_kind=excluded.item_cursor_kind,
                        item_cursor_value=excluded.item_cursor_value,
                        covered_until=excluded.covered_until,
                        run_cutoff=excluded.run_cutoff,
                        next_page=excluded.next_page,
                        status=excluded.status,
                        is_complete=excluded.is_complete,
                        gap_reason=excluded.gap_reason,
                        checkpoint_json=excluded.checkpoint_json,
                        next_retry_at=excluded.next_retry_at,
                        last_error_code=excluded.last_error_code,
                        failure_class=excluded.failure_class,
                        operator_action_required=excluded.operator_action_required,
                        consumes_retry_budget=excluded.consumes_retry_budget,
                        attempt=CASE
                            WHEN excluded.consumes_retry_budget=0
                            THEN MAX(official_asset_discovery_state.attempt-1, 0)
                            ELSE official_asset_discovery_state.attempt
                        END,
                        state_version=official_asset_discovery_state.state_version+1,
                        updated_at=excluded.updated_at
                    """,
                    (*key, DISCOVERY_STATE_SCHEMA_VERSION, *values, now),
                )
            row = conn.execute(
                """SELECT * FROM official_asset_discovery_state
                   WHERE source=? AND exchange=? AND category=? AND scope_key=?
                     AND config_fingerprint=?""",
                (
                    normalize_source(source),
                    exchange.upper(),
                    category,
                    scope_key,
                    config_fingerprint,
                ),
            ).fetchone()
            current = _require_row(row)
            if (
                project_parent_block
                and current["operation_id"]
                and status in {"blocked", "exhausted"}
            ):
                parent_reason = (
                    "retry_exhausted" if status == "exhausted" else error_code
                ) or "operator_action_required"
                diagnostics = {
                    "retry_item_type": "discovery",
                    "source": normalize_source(source),
                    "exchange": exchange.upper(),
                    "scope_key": scope_key,
                    "retry_item_status": status,
                    "failure_class": failure_class,
                    "operator_action_required": bool(operator_action_required),
                }
                conn.execute(
                    """UPDATE official_asset_operations
                       SET status='blocked', reason_code=?, diagnostics_json=?,
                           next_retry_at=NULL, updated_at=?
                       WHERE operation_id=? AND status IN ('queued', 'running')""",
                    (
                        parent_reason,
                        canonical_json(diagnostics),
                        now,
                        current["operation_id"],
                    ),
                )
        return _decode_row(_require_row(row), json_fields=("checkpoint_json",))

    def get_discovery_state(
        self,
        *,
        source: str,
        exchange: str,
        category: str,
        scope_key: str,
        config_fingerprint: str,
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_discovery_state
                   WHERE source=? AND exchange=? AND category=? AND scope_key=?
                     AND config_fingerprint=?""",
                (
                    normalize_source(source),
                    exchange.upper(),
                    category,
                    scope_key,
                    config_fingerprint,
                ),
            ).fetchone()
        return (
            None if row is None else _decode_row(row, json_fields=("checkpoint_json",))
        )

    def list_discovery_states(
        self,
        *,
        category: str | None = None,
        scope_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if category:
            clauses.append("category=?")
            params.append(category)
        if scope_prefix:
            clauses.append("scope_key LIKE ?")
            params.append(f"{scope_prefix}%")
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_asset_discovery_state"
                + where
                + " ORDER BY updated_at, source, exchange, scope_key",
                tuple(params),
            ).fetchall()
        return [_decode_row(row, json_fields=("checkpoint_json",)) for row in rows]

    def enqueue_attachment_retry(
        self,
        *,
        attachment_id: str,
        source: str,
        operation_id: str | None = None,
        next_retry_at: str | None = None,
        error_code: str | None = None,
        observation_key: str | None = None,
        max_attempts: int = 4,
        failure_class: str | None = None,
        operator_action_required: bool = False,
        consumes_retry_budget: bool = True,
        reopen_reason: str | None = None,
        repair_actor: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        incoming_metadata = dict(metadata or {})
        incoming_observation = str(
            observation_key
            or incoming_metadata.get("observation_key")
            or incoming_metadata.get("candidate_id")
            or ""
        ).strip()
        allowed_reopen = {
            None,
            "new_observation",
            "due_retry",
            "due_repair",
            "audited_repair",
        }
        if reopen_reason not in allowed_reopen:
            raise ValueError("invalid attachment retry reopen reason")
        if reopen_reason == "audited_repair" and not str(repair_actor or "").strip():
            raise ValueError("audited attachment repair requires an actor")
        if int(max_attempts) < 1:
            raise ValueError("max_attempts must be positive")
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            if existing is not None:
                existing_observation = str(existing["observation_key"] or "")
                same_observation = existing_observation == incoming_observation
                new_observation = bool(incoming_observation and not same_observation)
                if new_observation:
                    reopen_reason = "new_observation"
                elif (
                    existing["status"]
                    in {"completed", "blocked", "exhausted", "operator_hold"}
                    and reopen_reason
                    not in {"new_observation", "due_repair", "audited_repair"}
                ) or (
                    existing["status"] in {"queued", "retryable", "running"}
                    and reopen_reason not in {"due_retry", "audited_repair"}
                ):
                    return _decode_row(existing, json_fields=("metadata_json",))
                if (
                    reopen_reason == "due_retry"
                    and existing["next_retry_at"]
                    and _iso_after(existing["next_retry_at"], now)
                ):
                    return _decode_row(existing, json_fields=("metadata_json",))
            attempt = (
                int(existing["attempt"] or 0)
                if existing is not None and reopen_reason == "due_retry"
                else 0
            )
            conn.execute(
                """INSERT INTO official_asset_attachment_retries(
                       attachment_id, source, operation_id, observation_key,
                       status, attempt, max_attempts, next_retry_at,
                       last_error_code, failure_class,
                       operator_action_required, consumes_retry_budget,
                       reopen_reason, reopened_at, repair_actor,
                       first_queued_at, metadata_json, updated_at
                   ) VALUES(?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(attachment_id) DO UPDATE SET
                       source=excluded.source,
                       operation_id=COALESCE(excluded.operation_id, operation_id),
                       observation_key=excluded.observation_key,
                       status='queued',
                       attempt=excluded.attempt,
                       max_attempts=excluded.max_attempts,
                       next_retry_at=excluded.next_retry_at,
                       last_error_code=excluded.last_error_code,
                       failure_class=excluded.failure_class,
                       operator_action_required=excluded.operator_action_required,
                       consumes_retry_budget=excluded.consumes_retry_budget,
                       reopen_reason=excluded.reopen_reason,
                       reopened_at=excluded.reopened_at,
                       repair_actor=excluded.repair_actor,
                       completed_at=NULL,
                       metadata_json=excluded.metadata_json,
                       updated_at=excluded.updated_at""",
                (
                    attachment_id,
                    normalize_source(source),
                    operation_id,
                    incoming_observation,
                    attempt,
                    int(max_attempts),
                    next_retry_at,
                    error_code,
                    failure_class,
                    int(operator_action_required),
                    int(consumes_retry_budget),
                    reopen_reason,
                    now if reopen_reason else None,
                    repair_actor,
                    now,
                    canonical_json(incoming_metadata),
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        return _decode_row(_require_row(row), json_fields=("metadata_json",))

    def claim_attachment_retry(
        self, attachment_id: str, *, now: str | None = None
    ) -> dict[str, Any]:
        timestamp = now or utc_now_iso()
        exhausted = False
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            current = _require_row(row)
            if current["status"] not in {"queued", "retryable"}:
                raise RuntimeError("attachment retry is not claimable")
            if current["next_retry_at"] and _iso_after(
                current["next_retry_at"], timestamp
            ):
                raise RuntimeError("attachment retry is not due")
            if int(current["attempt"]) >= int(current["max_attempts"]):
                conn.execute(
                    """UPDATE official_asset_attachment_retries
                       SET status='exhausted', next_retry_at=NULL,
                           operator_action_required=1, updated_at=?
                       WHERE attachment_id=?""",
                    (timestamp, attachment_id),
                )
                exhausted = True
            else:
                conn.execute(
                    """UPDATE official_asset_attachment_retries SET
                           status='running', attempt=attempt+1,
                           last_attempted_at=?, updated_at=?
                       WHERE attachment_id=?""",
                    (timestamp, timestamp, attachment_id),
                )
            updated = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        if exhausted:
            raise RuntimeError("attachment retry attempts are exhausted")
        return _decode_row(_require_row(updated), json_fields=("metadata_json",))

    def finish_attachment_retry(
        self,
        attachment_id: str,
        *,
        success: bool,
        retryable: bool = True,
        next_retry_at: str | None = None,
        error_code: str | None = None,
        failure_class: str | None = None,
        operator_action_required: bool = False,
        consumes_retry_budget: bool = True,
        max_attempts: int | None = None,
        project_parent_block: bool = True,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        with self.transaction() as conn:
            current = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            current = _require_row(current)
            limit = int(max_attempts or current["max_attempts"] or 1)
            attempt = int(current["attempt"])
            if not consumes_retry_budget and attempt > 0:
                attempt -= 1
            if success:
                status = "completed"
                next_retry_at = None
            elif failure_class == "transient" and attempt >= limit:
                status = "exhausted"
                next_retry_at = None
                operator_action_required = True
            elif operator_action_required or not retryable:
                status = "blocked"
                next_retry_at = None
            elif attempt >= limit:
                status = "exhausted"
                next_retry_at = None
                operator_action_required = True
            else:
                status = "queued"
            conn.execute(
                """UPDATE official_asset_attachment_retries SET
                       status=?, attempt=?, max_attempts=?, next_retry_at=?,
                       last_error_code=?, failure_class=?,
                       operator_action_required=?, consumes_retry_budget=?,
                       completed_at=CASE WHEN ? THEN ? ELSE completed_at END,
                       updated_at=? WHERE attachment_id=?""",
                (
                    status,
                    attempt,
                    limit,
                    next_retry_at,
                    error_code,
                    failure_class,
                    int(operator_action_required),
                    int(consumes_retry_budget),
                    int(success),
                    now,
                    now,
                    attachment_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
            if (
                project_parent_block
                and status in {"blocked", "exhausted"}
                and current["operation_id"]
            ):
                parent_reason = (
                    "retry_exhausted" if status == "exhausted" else error_code
                ) or "operator_action_required"
                diagnostics = {
                    "retry_item_type": "attachment",
                    "attachment_id": attachment_id,
                    "retry_item_status": status,
                    "failure_class": failure_class,
                    "operator_action_required": bool(operator_action_required),
                }
                conn.execute(
                    """UPDATE official_asset_operations
                       SET status='blocked', reason_code=?, diagnostics_json=?,
                           next_retry_at=NULL, updated_at=?
                       WHERE operation_id=? AND status IN ('queued', 'running')""",
                    (
                        parent_reason,
                        canonical_json(diagnostics),
                        now,
                        current["operation_id"],
                    ),
                )
        return _decode_row(_require_row(row), json_fields=("metadata_json",))

    def get_attachment_retry(self, attachment_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE attachment_id=?",
                (attachment_id,),
            ).fetchone()
        return None if row is None else _decode_row(row, json_fields=("metadata_json",))

    def list_attachment_retries(
        self, *, due_at: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        clauses = ["status IN ('queued', 'retryable', 'running')"]
        params: list[Any] = []
        if due_at is not None:
            clauses.append("(next_retry_at IS NULL OR next_retry_at<=?)")
            params.append(due_at)
        params.append(max(1, min(int(limit), 1000)))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_asset_attachment_retries WHERE "
                + " AND ".join(clauses)
                + " ORDER BY COALESCE(next_retry_at, first_queued_at), attachment_id LIMIT ?",
                tuple(params),
            ).fetchall()
        return [_decode_row(row, json_fields=("metadata_json",)) for row in rows]

    def upsert_period_reconciliation(
        self,
        *,
        instrument_id: str,
        fiscal_year: int,
        status: str,
        next_retry_at: str | None = None,
        last_reconciled_at: str | None = None,
        checkpoint: Mapping[str, Any] | None = None,
        error_code: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO official_asset_period_reconciliation(
                       instrument_id, fiscal_year, status, attempt, next_retry_at,
                       last_reconciled_at, checkpoint_json, last_error_code,
                       created_at, updated_at
                   ) VALUES(?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(instrument_id, fiscal_year) DO UPDATE SET
                       status=excluded.status,
                       next_retry_at=excluded.next_retry_at,
                       last_reconciled_at=excluded.last_reconciled_at,
                       checkpoint_json=excluded.checkpoint_json,
                       last_error_code=excluded.last_error_code,
                       updated_at=excluded.updated_at""",
                (
                    normalize_instrument_id(instrument_id),
                    int(fiscal_year),
                    str(status),
                    next_retry_at,
                    last_reconciled_at,
                    canonical_json(checkpoint or {}),
                    error_code,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_period_reconciliation WHERE instrument_id=? AND fiscal_year=?",
                (normalize_instrument_id(instrument_id), int(fiscal_year)),
            ).fetchone()
        return _decode_row(_require_row(row), json_fields=("checkpoint_json",))

    def get_period_reconciliation(
        self, instrument_id: str, fiscal_year: int
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_period_reconciliation WHERE instrument_id=? AND fiscal_year=?",
                (normalize_instrument_id(instrument_id), int(fiscal_year)),
            ).fetchone()
        return (
            None if row is None else _decode_row(row, json_fields=("checkpoint_json",))
        )

    def mark_period_reconciliation_attempt(
        self, instrument_id: str, fiscal_year: int
    ) -> dict[str, Any]:
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """UPDATE official_asset_period_reconciliation
                   SET attempt=attempt+1, status='running', updated_at=?
                   WHERE instrument_id=? AND fiscal_year=?""",
                (now, normalize_instrument_id(instrument_id), int(fiscal_year)),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_period_reconciliation WHERE instrument_id=? AND fiscal_year=?",
                (normalize_instrument_id(instrument_id), int(fiscal_year)),
            ).fetchone()
        return _decode_row(_require_row(row), json_fields=("checkpoint_json",))

    def list_period_reconciliation(
        self, *, due_at: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        clauses = ["status IN ('queued', 'retryable', 'running')"]
        params: list[Any] = []
        if due_at is not None:
            clauses.append("(next_retry_at IS NULL OR next_retry_at<=?)")
            params.append(due_at)
        params.append(max(1, min(int(limit), 1000)))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_asset_period_reconciliation WHERE "
                + " AND ".join(clauses)
                + " ORDER BY COALESCE(last_reconciled_at, '1970-01-01'), fiscal_year, instrument_id LIMIT ?",
                tuple(params),
            ).fetchall()
        return [_decode_row(row, json_fields=("checkpoint_json",)) for row in rows]

    def append_job_command_audit(
        self,
        *,
        operation_id: str | None,
        command: str,
        principal: str,
        effective_permission: str,
        trigger_kind: str,
        config_version: str,
        request_fingerprint: str,
        payload: Mapping[str, Any] | None = None,
    ) -> int:
        with self.transaction() as conn:
            cursor = conn.execute(
                """INSERT INTO official_asset_job_command_audit(
                       operation_id, command, principal, effective_permission,
                       trigger_kind, config_version, request_fingerprint,
                       payload_json, created_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    operation_id,
                    str(command),
                    str(principal),
                    str(effective_permission),
                    str(trigger_kind),
                    str(config_version),
                    str(request_fingerprint),
                    canonical_json(payload or {}),
                    utc_now_iso(),
                ),
            )
        return int(cursor.lastrowid)

    def list_job_command_audit(
        self, *, operation_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if operation_id:
            clauses.append("operation_id=?")
            params.append(operation_id)
        params.append(max(1, min(int(limit), 1000)))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_asset_job_command_audit"
                + where
                + " ORDER BY command_id DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [_decode_row(row, json_fields=("payload_json",)) for row in rows]

    def persist_operational_report(
        self,
        *,
        report_kind: str,
        schema_version: str,
        config_fingerprint: str,
        status: str,
        generated_at: str,
        payload: Mapping[str, Any],
        operation_id: str | None = None,
        scope_key: str = "global",
        report_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist one immutable, reloadable operational-report projection."""

        kind = str(report_kind or "").strip()
        version = str(schema_version or "").strip()
        fingerprint = str(config_fingerprint or "").strip()
        scope = str(scope_key or "global").strip()
        if not kind or not version or not fingerprint or not scope:
            raise ValueError("operational report identity fields are required")
        identity = report_id or stable_id(
            "announcement-asset-operational-report",
            kind,
            scope,
            str(operation_id or ""),
            str(generated_at),
            fingerprint,
        )
        created_at = utc_now_iso()
        encoded = canonical_json(payload)
        with self.transaction() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO official_asset_operational_reports(
                       report_id, schema_version, report_kind, operation_id,
                       scope_key, config_fingerprint, status, generated_at,
                       payload_json, created_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    identity,
                    version,
                    kind,
                    operation_id,
                    scope,
                    fingerprint,
                    str(status),
                    str(generated_at),
                    encoded,
                    created_at,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_operational_reports WHERE report_id=?",
                (identity,),
            ).fetchone()
        decoded = _decode_row(_require_row(row), json_fields=("payload_json",))
        if (
            decoded["schema_version"] != version
            or decoded["report_kind"] != kind
            or decoded["scope_key"] != scope
            or decoded["config_fingerprint"] != fingerprint
            or decoded["status"] != str(status)
            or decoded["generated_at"] != str(generated_at)
            or canonical_json(decoded["payload"]) != encoded
        ):
            raise ValueError("operational report identity collision")
        return decoded

    def list_operational_reports(
        self,
        *,
        report_kind: str | None = None,
        scope_key: str | None = None,
        operation_id: str | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if report_kind is not None:
            clauses.append("report_kind=?")
            params.append(str(report_kind))
        if scope_key is not None:
            clauses.append("scope_key=?")
            params.append(str(scope_key))
        if operation_id is not None:
            clauses.append("operation_id=?")
            params.append(str(operation_id))
        params.append(max(1, min(int(limit), 1000)))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM official_asset_operational_reports"
                + where
                + " ORDER BY generated_at DESC, report_id DESC LIMIT ?",
                tuple(params),
            ).fetchall()
        return [_decode_row(row, json_fields=("payload_json",)) for row in rows]

    def upsert_universe_snapshot(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        """Persist a versioned universe denominator and its freshness evidence."""
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_asset_universe_snapshots(
                    snapshot_id, schema_version, policy_version,
                    master_data_version, master_data_last_success_at,
                    snapshot_at, freshness_limit_seconds, status,
                    source_complete, indeterminate_count, eligible_count,
                    instrument_rows_json, indeterminate_rows_json, metadata_json,
                    paired_census_snapshot_id,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_id) DO UPDATE SET
                    master_data_version=excluded.master_data_version,
                    master_data_last_success_at=excluded.master_data_last_success_at,
                    snapshot_at=excluded.snapshot_at,
                    freshness_limit_seconds=excluded.freshness_limit_seconds,
                    status=excluded.status,
                    source_complete=excluded.source_complete,
                    indeterminate_count=excluded.indeterminate_count,
                    eligible_count=excluded.eligible_count,
                    instrument_rows_json=excluded.instrument_rows_json,
                    indeterminate_rows_json=excluded.indeterminate_rows_json,
                    metadata_json=CASE
                        WHEN excluded.paired_census_snapshot_id IS NULL
                         AND official_asset_universe_snapshots.paired_census_snapshot_id
                             IS NOT NULL
                        THEN official_asset_universe_snapshots.metadata_json
                        ELSE excluded.metadata_json
                    END,
                    paired_census_snapshot_id=COALESCE(
                        excluded.paired_census_snapshot_id,
                        official_asset_universe_snapshots.paired_census_snapshot_id
                    ),
                    updated_at=excluded.updated_at
                """,
                (
                    snapshot["snapshot_id"],
                    snapshot.get("schema_version", "official_asset_universe.v1"),
                    snapshot["policy_version"],
                    snapshot.get("master_data_version"),
                    snapshot.get("master_data_last_success_at"),
                    snapshot["snapshot_at"],
                    int(snapshot["freshness_limit_seconds"]),
                    snapshot["status"],
                    int(bool(snapshot.get("source_complete"))),
                    len(snapshot.get("indeterminate", ())),
                    len(snapshot.get("instruments", ())),
                    canonical_json({"items": list(snapshot.get("instruments", ()))}),
                    canonical_json({"items": list(snapshot.get("indeterminate", ()))}),
                    canonical_json(snapshot.get("metadata", {})),
                    snapshot.get("paired_census_snapshot_id"),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_universe_snapshots WHERE snapshot_id=?",
                (snapshot["snapshot_id"],),
            ).fetchone()
        return _decode_row(
            _require_row(row),
            json_fields=(
                "instrument_rows_json",
                "indeterminate_rows_json",
                "metadata_json",
            ),
        )

    def get_universe_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_universe_snapshots WHERE snapshot_id=?",
                (snapshot_id,),
            ).fetchone()
        return (
            None
            if row is None
            else _decode_row(
                row,
                json_fields=(
                    "instrument_rows_json",
                    "indeterminate_rows_json",
                    "metadata_json",
                ),
            )
        )

    def upsert_listed_security_census_snapshot(
        self, snapshot: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Persist an independently sourced listed-security census."""

        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_asset_listed_security_census_snapshots(
                    census_snapshot_id, schema_version, source,
                    query_boundary_json, completeness_watermark, source_version,
                    snapshot_at, raw_payload_hash, status, instrument_rows_json,
                    metadata_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(census_snapshot_id) DO UPDATE SET
                    source=excluded.source,
                    query_boundary_json=excluded.query_boundary_json,
                    completeness_watermark=excluded.completeness_watermark,
                    source_version=excluded.source_version,
                    snapshot_at=excluded.snapshot_at,
                    raw_payload_hash=excluded.raw_payload_hash,
                    status=excluded.status,
                    instrument_rows_json=excluded.instrument_rows_json,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    snapshot["census_snapshot_id"],
                    snapshot.get(
                        "schema_version", "official_listed_security_census.v1"
                    ),
                    snapshot["source"],
                    canonical_json(snapshot["query_boundary"]),
                    snapshot["completeness_watermark"],
                    snapshot["source_version"],
                    snapshot["snapshot_at"],
                    snapshot["raw_payload_hash"],
                    snapshot["status"],
                    canonical_json({"items": list(snapshot.get("instruments", ()))}),
                    canonical_json(snapshot.get("metadata", {})),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_listed_security_census_snapshots "
                "WHERE census_snapshot_id=?",
                (snapshot["census_snapshot_id"],),
            ).fetchone()
        return _decode_row(
            _require_row(row),
            json_fields=(
                "query_boundary_json",
                "instrument_rows_json",
                "metadata_json",
            ),
        )

    def get_listed_security_census_snapshot(
        self, census_snapshot_id: str
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_listed_security_census_snapshots "
                "WHERE census_snapshot_id=?",
                (str(census_snapshot_id),),
            ).fetchone()
        return (
            None
            if row is None
            else _decode_row(
                row,
                json_fields=(
                    "query_boundary_json",
                    "instrument_rows_json",
                    "metadata_json",
                ),
            )
        )

    def get_latest_complete_listed_security_census_snapshot(
        self,
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT *
                   FROM official_asset_listed_security_census_snapshots
                   WHERE status='complete'
                   ORDER BY snapshot_at DESC LIMIT 1"""
            ).fetchone()
        return (
            None
            if row is None
            else _decode_row(
                row,
                json_fields=(
                    "query_boundary_json",
                    "instrument_rows_json",
                    "metadata_json",
                ),
            )
        )

    def get_latest_complete_universe_snapshot(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_universe_snapshots
                   WHERE status='complete' AND source_complete=1
                   ORDER BY snapshot_at DESC LIMIT 1"""
            ).fetchone()
        return (
            None
            if row is None
            else _decode_row(
                row,
                json_fields=(
                    "instrument_rows_json",
                    "indeterminate_rows_json",
                    "metadata_json",
                ),
            )
        )

    def get_latest_full_market_universe_snapshot(self) -> dict[str, Any] | None:
        """Return the newest denominator with an independently paired census."""

        with self.connection() as conn:
            rows = conn.execute(
                """SELECT * FROM official_asset_universe_snapshots
                   WHERE status='complete' AND source_complete=1
                     AND paired_census_snapshot_id IS NOT NULL
                   ORDER BY snapshot_at DESC"""
            ).fetchall()
        for row in rows:
            item = _decode_row(
                row,
                json_fields=(
                    "instrument_rows_json",
                    "indeterminate_rows_json",
                    "metadata_json",
                ),
            )
            metadata = item.get("metadata") or item.get("metadata_json") or {}
            reconciliation = (
                metadata.get("census_reconciliation")
                if isinstance(metadata, Mapping)
                else None
            )
            if (
                isinstance(reconciliation, Mapping)
                and reconciliation.get("status") == "complete"
            ):
                return item
        return None

    def create_or_resume_bootstrap_run(
        self,
        *,
        operation_id: str,
        universe_snapshot_id: str,
        scope: Mapping[str, Any],
        as_of: str,
        evidence_visibility_cutoff: str,
        query_fingerprint: str,
    ) -> tuple[dict[str, Any], bool]:
        """Persist and validate the immutable identity of a bootstrap run."""
        values = {
            "operation_id": str(operation_id or "").strip(),
            "universe_snapshot_id": str(universe_snapshot_id or "").strip(),
            "scope": dict(scope),
            "as_of": str(as_of or "").strip(),
            "evidence_visibility_cutoff": str(evidence_visibility_cutoff or "").strip(),
            "query_fingerprint": str(query_fingerprint or "").strip(),
        }
        if not all(
            (
                values["operation_id"],
                values["universe_snapshot_id"],
                values["as_of"],
                values["evidence_visibility_cutoff"],
                values["query_fingerprint"],
            )
        ):
            raise ValueError("bootstrap run identity fields are required")
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_bootstrap_runs WHERE operation_id=?",
                (values["operation_id"],),
            ).fetchone()
            created = row is None
            if row is not None:
                mismatches: list[str] = []
                if row["universe_snapshot_id"] != values["universe_snapshot_id"]:
                    mismatches.append("universe_snapshot_id")
                if row["as_of"] != values["as_of"]:
                    mismatches.append("as_of")
                if (
                    row["evidence_visibility_cutoff"]
                    != values["evidence_visibility_cutoff"]
                ):
                    mismatches.append("evidence_visibility_cutoff")
                if row["query_fingerprint"] != values["query_fingerprint"]:
                    mismatches.append("query_fingerprint")
                if canonical_json(_json_load(row["scope_json"], {})) != canonical_json(
                    values["scope"]
                ):
                    mismatches.append("scope")
                if mismatches:
                    raise BootstrapRunIdentityError(
                        "bootstrap run identity conflict: " + ",".join(mismatches)
                    )
            else:
                row = conn.execute(
                    """SELECT * FROM official_asset_bootstrap_runs
                       WHERE universe_snapshot_id=? AND as_of=?
                         AND query_fingerprint=?""",
                    (
                        values["universe_snapshot_id"],
                        values["as_of"],
                        values["query_fingerprint"],
                    ),
                ).fetchone()
                if row is not None:
                    return _decode_row(
                        row, json_fields=("scope_json", "checkpoint_json")
                    ), False
                conn.execute(
                    """INSERT INTO official_asset_bootstrap_runs(
                           operation_id, schema_version, universe_snapshot_id,
                           scope_json, as_of, evidence_visibility_cutoff,
                           query_fingerprint, status, checkpoint_json,
                           started_at, created_at, updated_at
                       ) VALUES(?, ?, ?, ?, ?, ?, ?, 'running', '{}', ?, ?, ?)""",
                    (
                        values["operation_id"],
                        BOOTSTRAP_RUN_SCHEMA_VERSION,
                        values["universe_snapshot_id"],
                        canonical_json(values["scope"]),
                        values["as_of"],
                        values["evidence_visibility_cutoff"],
                        values["query_fingerprint"],
                        now,
                        now,
                        now,
                    ),
                )
            row = conn.execute(
                "SELECT * FROM official_asset_bootstrap_runs WHERE operation_id=?",
                (values["operation_id"],),
            ).fetchone()
        return _decode_row(
            _require_row(row), json_fields=("scope_json", "checkpoint_json")
        ), created

    def get_bootstrap_run(self, operation_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_bootstrap_runs WHERE operation_id=?",
                (str(operation_id),),
            ).fetchone()
        return (
            None
            if row is None
            else _decode_row(row, json_fields=("scope_json", "checkpoint_json"))
        )

    def update_bootstrap_run(
        self,
        operation_id: str,
        *,
        status: str | None = None,
        checkpoint: Mapping[str, Any] | None = None,
        expected_query_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        """Persist progress without allowing a worker to change run identity."""
        allowed = {"running", "partial", "success", "blocked", "failed"}
        if status is not None and status not in allowed:
            raise ValueError("invalid bootstrap run status")
        now = utc_now_iso()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM official_asset_bootstrap_runs WHERE operation_id=?",
                (str(operation_id),),
            ).fetchone()
            current = _require_row(row)
            if expected_query_fingerprint is not None and current[
                "query_fingerprint"
            ] != str(expected_query_fingerprint):
                raise BootstrapRunIdentityError(
                    "bootstrap query fingerprint does not match persisted run"
                )
            terminal = status in {"success", "blocked", "failed"}
            conn.execute(
                """UPDATE official_asset_bootstrap_runs
                   SET status=COALESCE(?, status), checkpoint_json=COALESCE(?, checkpoint_json),
                       completed_at=CASE WHEN ? THEN COALESCE(completed_at, ?) ELSE completed_at END,
                       updated_at=? WHERE operation_id=?""",
                (
                    status,
                    None if checkpoint is None else canonical_json(checkpoint),
                    int(terminal),
                    now,
                    now,
                    str(operation_id),
                ),
            )
            row = conn.execute(
                "SELECT * FROM official_asset_bootstrap_runs WHERE operation_id=?",
                (str(operation_id),),
            ).fetchone()
        return _decode_row(
            _require_row(row), json_fields=("scope_json", "checkpoint_json")
        )

    def upsert_asset_coverage(
        self,
        *,
        universe_snapshot_id: str,
        instrument_id: str,
        status: str,
        as_of: str,
        fiscal_year: int | None = None,
        expected_fiscal_year: int | None = None,
        earliest_search_year: int | None = None,
        evidence_expires_at: str | None = None,
        last_reconciled_at: str | None = None,
        retry_at: str | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if status == "confirmed_missing":
            evidence_mapping = dict(evidence or {})
            evidence_mapping.setdefault("evidence_expires_at", evidence_expires_at)
            evidence_expires_at = (
                evidence_expires_at
                or str(evidence_mapping.get("evidence_expires_at") or "").strip()
                or None
            )
            error = _confirmed_missing_evidence_error(
                evidence_mapping, evidence_expires_at
            )
            if error:
                raise ValueError(error)
            evidence = evidence_mapping
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO official_asset_coverage(
                    universe_snapshot_id, instrument_id, fiscal_year, status,
                    as_of, expected_fiscal_year, earliest_search_year,
                    evidence_expires_at, last_reconciled_at, retry_at,
                    evidence_json, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(universe_snapshot_id, instrument_id) DO UPDATE SET
                    fiscal_year=excluded.fiscal_year,
                    status=excluded.status,
                    as_of=excluded.as_of,
                    expected_fiscal_year=excluded.expected_fiscal_year,
                    earliest_search_year=excluded.earliest_search_year,
                    evidence_expires_at=excluded.evidence_expires_at,
                    last_reconciled_at=excluded.last_reconciled_at,
                    retry_at=excluded.retry_at,
                    evidence_json=excluded.evidence_json,
                    updated_at=excluded.updated_at
                """,
                (
                    universe_snapshot_id,
                    normalize_instrument_id(instrument_id),
                    fiscal_year,
                    status,
                    as_of,
                    expected_fiscal_year,
                    earliest_search_year,
                    evidence_expires_at,
                    last_reconciled_at,
                    retry_at,
                    canonical_json(evidence or {}),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                """SELECT * FROM official_asset_coverage
                   WHERE universe_snapshot_id=? AND instrument_id=?""",
                (universe_snapshot_id, normalize_instrument_id(instrument_id)),
            ).fetchone()
        return _decode_row(_require_row(row), json_fields=("evidence_json",))

    def list_asset_coverage(
        self,
        universe_snapshot_id: str,
        *,
        now: str | None = None,
        fingerprints: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """SELECT * FROM official_asset_coverage
                   WHERE universe_snapshot_id=? ORDER BY instrument_id""",
                (universe_snapshot_id,),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            item = _decode_row(row, json_fields=("evidence_json",))
            if item["status"] == "confirmed_missing":
                evidence = item.get("evidence") or item.get("evidence_json") or {}
                error = _confirmed_missing_evidence_error(
                    evidence, item.get("evidence_expires_at")
                )
                expired = bool(
                    now
                    and (
                        item.get("evidence_expires_at")
                        or evidence.get("evidence_expires_at")
                    )
                    and str(
                        item.get("evidence_expires_at")
                        or evidence.get("evidence_expires_at")
                    )
                    <= str(now)
                )
                fingerprint_changed = False
                if fingerprints:
                    fingerprint_changed = any(
                        str(evidence.get(key) or "") != str(value or "")
                        for key, value in fingerprints.items()
                    )
                if error or expired or fingerprint_changed:
                    # Do not erase the durable negative evidence.  Consumers
                    # receive a non-terminal projection and must schedule a
                    # bounded repair before granting missing credit again.
                    item["status"] = "incomplete"
                    item["terminal_restore_blocked"] = error or (
                        "confirmed_missing evidence expired"
                        if expired
                        else "confirmed_missing evidence fingerprint changed"
                    )
            output.append(item)
        return output

    def get_latest_asset_coverage_for_query(
        self,
        *,
        instrument_id: str,
        query_fingerprint: str,
    ) -> dict[str, Any] | None:
        """Return the newest coverage checkpoint for one stable bootstrap query."""

        with self.connection() as conn:
            row = conn.execute(
                """SELECT * FROM official_asset_coverage
                   WHERE instrument_id=?
                     AND json_extract(evidence_json, '$.query_fingerprint')=?
                   ORDER BY updated_at DESC, universe_snapshot_id DESC
                   LIMIT 1""",
                (
                    normalize_instrument_id(instrument_id),
                    str(query_fingerprint),
                ),
            ).fetchone()
        return None if row is None else _decode_row(row, json_fields=("evidence_json",))

    def list_latest_asset_coverage_for_query(
        self,
        query_fingerprint: str,
    ) -> list[dict[str, Any]]:
        """Return one newest durable coverage checkpoint per instrument."""

        with self.connection() as conn:
            rows = conn.execute(
                """WITH ranked AS (
                       SELECT coverage.*,
                              ROW_NUMBER() OVER (
                                  PARTITION BY instrument_id
                                  ORDER BY updated_at DESC,
                                           universe_snapshot_id DESC
                              ) AS query_rank
                       FROM official_asset_coverage AS coverage
                       WHERE json_extract(
                           evidence_json, '$.query_fingerprint'
                       )=?
                   )
                   SELECT * FROM ranked WHERE query_rank=1
                   ORDER BY instrument_id""",
                (str(query_fingerprint),),
            ).fetchall()
        return [_decode_row(row, json_fields=("evidence_json",)) for row in rows]

    @staticmethod
    def _append_effective_decision_conn(
        conn: sqlite3.Connection,
        *,
        decision_id: str,
        decision_kind: EffectiveDecisionKind,
        prior: EffectiveAnnualReport | None,
        replacement: EffectiveAnnualReport | None,
        decision_state: EffectiveDecisionState,
        classifier_version: str,
        decision_policy_version: str,
        decision_reasons: Sequence[str],
        decision_evidence: Mapping[str, Any],
        activated_at: str,
        outbox_event_key: str,
        created_at: str,
    ) -> None:
        prior_source, prior_source_announcement_id = (
            AnnouncementAssetRepository._legal_filing_identity_conn(conn, prior)
        )
        replacement_source, replacement_source_announcement_id = (
            AnnouncementAssetRepository._legal_filing_identity_conn(conn, replacement)
        )
        conn.execute(
            """INSERT INTO official_annual_report_decisions(
                   decision_id, schema_version, instrument_id, fiscal_year,
                   decision_kind, predecessor_asset_id,
                   predecessor_source, predecessor_source_announcement_id,
                   predecessor_announcement_id, predecessor_attachment_id,
                   predecessor_version_id, predecessor_content_hash,
                   replacement_asset_id, replacement_source,
                   replacement_source_announcement_id,
                   replacement_announcement_id,
                   replacement_attachment_id, replacement_version_id,
                   replacement_content_hash, decision_state,
                   classifier_version, decision_policy_version,
                   decision_reasons_json, decision_evidence_json,
                   activated_at, outbox_event_key, created_at
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                decision_id,
                EFFECTIVE_DECISION_SCHEMA_VERSION,
                (
                    replacement.instrument_id
                    if replacement is not None
                    else _require_effective(prior).instrument_id
                ),
                int(
                    replacement.fiscal_year
                    if replacement is not None
                    else _require_effective(prior).fiscal_year
                ),
                decision_kind.value,
                None if prior is None else prior.asset_id,
                prior_source,
                prior_source_announcement_id,
                None if prior is None else prior.announcement_id,
                None if prior is None else prior.attachment_id,
                None if prior is None else prior.version_id,
                None if prior is None else prior.content_hash,
                None if replacement is None else replacement.asset_id,
                replacement_source,
                replacement_source_announcement_id,
                None if replacement is None else replacement.announcement_id,
                None if replacement is None else replacement.attachment_id,
                None if replacement is None else replacement.version_id,
                None if replacement is None else replacement.content_hash,
                decision_state.value,
                str(classifier_version),
                str(decision_policy_version),
                json.dumps(
                    [str(reason) for reason in decision_reasons],
                    ensure_ascii=False,
                ),
                canonical_json(decision_evidence),
                activated_at,
                outbox_event_key,
                created_at,
            ),
        )

    @staticmethod
    def _legal_filing_identity_conn(
        conn: sqlite3.Connection,
        report: EffectiveAnnualReport | None,
    ) -> tuple[str | None, str | None]:
        if report is None:
            return None, None
        row = conn.execute(
            """SELECT source, source_announcement_id
               FROM official_announcements WHERE announcement_id=?""",
            (report.announcement_id,),
        ).fetchone()
        if row is None:
            raise ValueError(
                "effective decision references an unknown legal filing: "
                f"{report.announcement_id}"
            )
        return str(row["source"]), str(row["source_announcement_id"])

    @staticmethod
    def _upsert_effective_conn(
        conn: sqlite3.Connection,
        report: EffectiveAnnualReport,
        *,
        now: str,
    ) -> None:
        conn.execute(
            _UPSERT_EFFECTIVE_SQL,
            (
                report.asset_id,
                EFFECTIVE_ANNUAL_REPORT_SCHEMA_VERSION,
                normalize_instrument_id(report.instrument_id),
                int(report.fiscal_year),
                report.report_period,
                report.announcement_id,
                report.attachment_id,
                report.version_id,
                report.content_hash,
                normalize_source(report.source),
                report.source_announcement_id,
                report.published_at,
                report.document_family,
                report.variant.value,
                1 if report.is_full_report else 0,
                report.classifier_version,
                report.decision_state.value,
                report.availability.value,
                report.predecessor_asset_id,
                report.pending_candidate_id,
                report.activated_at,
                report.last_checked_at,
                json.dumps(list(report.decision_reasons), ensure_ascii=False),
                canonical_json(report.decision_evidence),
                json.dumps(
                    [item.as_dict() for item in report.equivalent_source_filings],
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                report.canonical_projection_policy_version,
                report.evidence_set_hash,
                report.visibility_state,
                now,
                now,
            ),
        )

    @staticmethod
    def _announcement_from_row(row: sqlite3.Row) -> OfficialAnnouncement:
        return OfficialAnnouncement(
            announcement_id=row["announcement_id"],
            source=row["source"],
            source_announcement_id=row["source_announcement_id"],
            title=row["title"],
            instrument_id=row["instrument_id"],
            exchange=row["exchange"],
            published_at=row["published_at"],
            published_at_raw=row["published_at_raw"],
            raw_payload_hash=row["raw_payload_hash"],
            first_observed_at=row["first_observed_at"],
            last_observed_at=row["last_observed_at"],
            status=row["status"],
            metadata=_json_load(row["metadata_json"], {}),
            source_category=row["source_category"],
            published_at_precision=row["published_at_precision"],
            provider_diagnostics=_json_load(row["provider_diagnostics_json"], {}),
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _attachment_from_row(row: sqlite3.Row) -> OfficialAnnouncementAttachment:
        return OfficialAnnouncementAttachment(
            attachment_id=row["attachment_id"],
            announcement_id=row["announcement_id"],
            attachment_identity=row["attachment_identity"],
            source_attachment_id=row["source_attachment_id"],
            source_url=row["source_url"],
            normalized_source_url=row["normalized_source_url"],
            name=row["name"],
            media_type=row["media_type"],
            content_length_hint=row["content_length_hint"],
            first_observed_at=row["first_observed_at"],
            last_observed_at=row["last_observed_at"],
            metadata=_json_load(row["metadata_json"], {}),
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _blob_from_row(row: sqlite3.Row) -> OfficialDocumentBlob:
        return OfficialDocumentBlob(
            content_hash=row["content_hash"],
            content_length=int(row["content_length"]),
            canonical_path=row["canonical_path"],
            signature_status=row["signature_status"],
            integrity_status=IntegrityStatus(row["integrity_status"]),
            first_available_at=row["first_available_at"],
            last_verified_at=row["last_verified_at"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _version_from_row(row: sqlite3.Row) -> OfficialAttachmentVersion:
        return OfficialAttachmentVersion(
            version_id=row["version_id"],
            attachment_id=row["attachment_id"],
            observation_key=row["observation_key"],
            content_hash=row["content_hash"],
            final_url=row["final_url"],
            retrieval_status=row["retrieval_status"],
            integrity_status=IntegrityStatus(row["integrity_status"]),
            attempt=int(row["attempt"]),
            next_retry_at=row["next_retry_at"],
            error_code=row["error_code"],
            observed_at=row["observed_at"],
            version_available_at=(row["version_available_at"] or row["observed_at"]),
            available_time_source=(row["available_time_source"] or "first_observed"),
            available_time_precision=(row["available_time_precision"] or "instant"),
            first_observed_at=row["first_observed_at"] or row["observed_at"],
            last_observed_at=row["last_observed_at"] or row["observed_at"],
            response_evidence=_json_load(row["response_evidence_json"], {}),
            content_length_observed=row["content_length_observed"],
            content_hash_observed=row["content_hash_observed"],
            lease_owner=row["lease_owner"],
            lease_generation=row["lease_generation"],
            max_attempts=int(row["max_attempts"]),
            temporary_path=row["temporary_path"],
            temporary_bytes=row["temporary_bytes"],
            quarantine_path=row["quarantine_path"],
            visibility_state=row["visibility_state"],
            metadata=_json_load(row["metadata_json"], {}),
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _effective_from_row(row: sqlite3.Row) -> EffectiveAnnualReport:
        return EffectiveAnnualReport(
            asset_id=row["asset_id"],
            instrument_id=row["instrument_id"],
            fiscal_year=int(row["fiscal_year"]),
            report_period=row["report_period"],
            announcement_id=row["announcement_id"],
            attachment_id=row["attachment_id"],
            version_id=row["version_id"],
            content_hash=row["content_hash"],
            source=row["source"],
            source_announcement_id=row["source_announcement_id"],
            published_at=row["published_at"],
            variant=AnnualReportVariant(row["variant"]),
            classifier_version=row["classifier_version"],
            decision_state=EffectiveDecisionState(row["decision_state"]),
            availability=AssetAvailability(row["availability"]),
            predecessor_asset_id=row["predecessor_asset_id"],
            pending_candidate_id=row["pending_candidate_id"],
            activated_at=row["activated_at"],
            last_checked_at=row["last_checked_at"],
            decision_reasons=tuple(_json_load(row["decision_reasons_json"], [])),
            equivalent_source_filings=tuple(
                SourceFilingEvidence(
                    source=str(item["source"]),
                    source_announcement_id=str(item["source_announcement_id"]),
                    attachment_id=str(item["attachment_id"]),
                    version_id=str(item["version_id"]),
                    content_hash=(
                        None
                        if item.get("content_hash") is None
                        else str(item["content_hash"])
                    ),
                )
                for item in _json_load(row["equivalent_source_filings_json"], [])
            ),
            canonical_projection_policy_version=row[
                "canonical_projection_policy_version"
            ],
            evidence_set_hash=row["evidence_set_hash"],
            decision_evidence=_json_load(row["decision_evidence_json"], {}),
            document_family=row["document_family"],
            visibility_state=row["visibility_state"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _effective_decision_from_row(
        row: sqlite3.Row,
    ) -> EffectiveAnnualReportDecision:
        return EffectiveAnnualReportDecision(
            decision_sequence=int(row["decision_sequence"]),
            decision_id=row["decision_id"],
            instrument_id=row["instrument_id"],
            fiscal_year=int(row["fiscal_year"]),
            decision_kind=EffectiveDecisionKind(row["decision_kind"]),
            predecessor_asset_id=row["predecessor_asset_id"],
            predecessor_source=row["predecessor_source"],
            predecessor_source_announcement_id=row[
                "predecessor_source_announcement_id"
            ],
            predecessor_announcement_id=row["predecessor_announcement_id"],
            predecessor_attachment_id=row["predecessor_attachment_id"],
            predecessor_version_id=row["predecessor_version_id"],
            predecessor_content_hash=row["predecessor_content_hash"],
            replacement_asset_id=row["replacement_asset_id"],
            replacement_source=row["replacement_source"],
            replacement_source_announcement_id=row[
                "replacement_source_announcement_id"
            ],
            replacement_announcement_id=row["replacement_announcement_id"],
            replacement_attachment_id=row["replacement_attachment_id"],
            replacement_version_id=row["replacement_version_id"],
            replacement_content_hash=row["replacement_content_hash"],
            decision_state=EffectiveDecisionState(row["decision_state"]),
            classifier_version=row["classifier_version"],
            decision_policy_version=row["decision_policy_version"],
            decision_reasons=tuple(_json_load(row["decision_reasons_json"], [])),
            decision_evidence=_json_load(row["decision_evidence_json"], {}),
            activated_at=row["activated_at"],
            outbox_event_key=row["outbox_event_key"],
            created_at=row["created_at"],
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> AssetOperation:
        return AssetOperation(
            operation_id=row["operation_id"],
            operation_type=row["operation_type"],
            idempotency_key=row["idempotency_key"],
            scope=_json_load(row["scope_json"], {}),
            policy_version=row["policy_version"],
            owner=row["owner"],
            status=OperationStatus(row["status"]),
            stage=OperationStage(row["stage"]) if row["stage"] else None,
            outcome=BatchOutcome(row["outcome"]) if row["outcome"] else None,
            attempt=int(row["attempt"]),
            next_retry_at=row["next_retry_at"],
            lease_owner=row["lease_owner"],
            lease_generation=int(row["lease_generation"]),
            lease_expires_at=row["lease_expires_at"],
            heartbeat_at=row["heartbeat_at"],
            progress=_json_load(row["progress_json"], {}),
            result_asset_id=row["result_asset_id"],
            result_origin=(
                ResultOrigin(row["result_origin"]) if row["result_origin"] else None
            ),
            reason_code=row["reason_code"],
            diagnostics=_json_load(row["diagnostics_json"], {}),
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            updated_at=row["updated_at"],
            bounds=_json_load(row["bounds_json"], {}),
            checkpoint=_json_load(row["checkpoint_json"], {}),
            max_attempts=int(row["max_attempts"]),
            resume_generation=int(row["resume_generation"]),
            config_version=row["config_version"],
            stage_schema_version=row["stage_schema_version"]
            or OPERATION_STAGE_SCHEMA_VERSION,
            schema_version=row["schema_version"],
        )

    @staticmethod
    def _subscription_from_row(row: sqlite3.Row) -> AssetOperationSubscription:
        return AssetOperationSubscription(
            asset_request_id=row["asset_request_id"],
            operation_id=row["operation_id"],
            principal=row["principal"],
            consumer=row["consumer"],
            idempotency_key=row["idempotency_key"],
            request_fingerprint=row["request_fingerprint"],
            status=AssetRequestStatus(row["status"]),
            consumer_continuation_id=row["consumer_continuation_id"],
            metadata=_json_load(row["metadata_json"], {}),
            authorized_projection=_json_load(row["authorized_projection_json"], {}),
            expires_at=row["expires_at"],
            expired_at=row["expired_at"],
            tombstone_until=row["tombstone_until"],
            retention_policy_version=row["retention_policy_version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            cancelled_at=row["cancelled_at"],
            schema_version=row["schema_version"],
        )


def _require_row(row: sqlite3.Row | None) -> sqlite3.Row:
    if row is None:
        raise KeyError("expected repository row was not found")
    return row


def _require_effective(report: EffectiveAnnualReport | None) -> EffectiveAnnualReport:
    if report is None:
        raise ValueError("effective decision requires a predecessor or replacement")
    return report


def _json_load(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _decode_row(row: sqlite3.Row, *, json_fields: Sequence[str] = ()) -> dict[str, Any]:
    output = dict(row)
    for field_name in json_fields:
        output[field_name.removesuffix("_json")] = _json_load(
            output.pop(field_name, None), {}
        )
    return output


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_after(left: str, right: str) -> bool:
    return _parse_iso_datetime(left) > _parse_iso_datetime(right)
