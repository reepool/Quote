from __future__ import annotations

import json
import sqlite3
from dataclasses import fields

import pytest

from research.announcement_assets import (
    AnnouncementAssetRepository,
    OfficialAssetBackupRecoveryJournalEntry,
    OfficialAssetConsumerProcessing,
    OfficialAssetConsumerRequest,
    OfficialAssetDeletionIntent,
    OfficialAssetRecoveryManifestEntry,
    OfficialAssetRecoveryPairClosure,
)
from research.announcement_assets.models import (
    BACKUP_RECOVERY_JOURNAL_SCHEMA_VERSION,
    CONSUMER_PROCESSING_SCHEMA_VERSION,
    CONSUMER_REQUEST_SCHEMA_VERSION,
    DELETION_INTENT_SCHEMA_VERSION,
    RECOVERY_PAIR_CLOSURE_SCHEMA_VERSION,
)
from research.announcement_assets.schema import OWNED_TABLES, SCHEMA_VERSION
from research.storage import ResearchStorageManager

NOW = "2026-08-10T00:00:00+00:00"


def _is_protected_business_table(name: str) -> bool:
    return name.startswith(("business_profile_", "company_", "financial_"))


def _protected_tables(conn: sqlite3.Connection) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        if _is_protected_business_table(str(row[0]))
    )


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _contract_snapshot(conn: sqlite3.Connection) -> dict[str, object]:
    tables = _protected_tables(conn)
    placeholders = ",".join("?" for _ in tables)
    objects = tuple(
        tuple(row)
        for row in conn.execute(
            f"""SELECT type, name, tbl_name, sql FROM sqlite_master
                WHERE tbl_name IN ({placeholders})
                  AND type IN ('table', 'index', 'trigger')
                ORDER BY type, name""",
            tables,
        ).fetchall()
    )
    table_contracts: dict[str, object] = {}
    rows: dict[str, object] = {}
    for table in tables:
        table_info = tuple(
            tuple(row)
            for row in conn.execute(f"PRAGMA table_info({_quoted(table)})").fetchall()
        )
        foreign_keys = tuple(
            tuple(row)
            for row in conn.execute(
                f"PRAGMA foreign_key_list({_quoted(table)})"
            ).fetchall()
        )
        indexes = tuple(
            tuple(row)
            for row in conn.execute(f"PRAGMA index_list({_quoted(table)})").fetchall()
        )
        index_contracts = []
        for index in indexes:
            index_name = str(index[1])
            index_contracts.append(
                (
                    index,
                    tuple(
                        tuple(row)
                        for row in conn.execute(
                            f"PRAGMA index_xinfo({_quoted(index_name)})"
                        ).fetchall()
                    ),
                )
            )
        table_contracts[table] = (table_info, foreign_keys, index_contracts)
        rows[table] = tuple(
            tuple(row)
            for row in conn.execute(
                f"SELECT * FROM {_quoted(table)} ORDER BY rowid"
            ).fetchall()
        )
    return {
        "tables": tables,
        "objects": objects,
        "table_contracts": table_contracts,
        "rows": rows,
    }


def _seed_real_existing_contracts(conn: sqlite3.Connection) -> None:
    ResearchStorageManager._create_tables(conn)
    conn.execute(
        """INSERT INTO business_profile_semantic_runs(
               run_id, instrument_id, source_document_id, field_family, status,
               bundle_hash, started_at, created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "semantic-run-1",
            "600000.SH",
            "document-1",
            "business_overview",
            "completed",
            "a" * 64,
            NOW,
            NOW,
            NOW,
        ),
    )
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
            "b" * 64,
            "2026-04-01",
            "exact",
            "c" * 64,
            "pdf_text",
            0.95,
            "approved",
            NOW,
            NOW,
        ),
    )
    conn.execute(
        """INSERT INTO company_business_profile_events(
               event_id, instrument_id, event_type, description, evidence_id,
               data_available_date, confidence, review_status, lineage_hash,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "profile-event-1",
            "600000.SH",
            "annual_report_update",
            "representative migration fixture",
            "evidence-1",
            "2026-04-01",
            0.95,
            "approved",
            "d" * 64,
            NOW,
            NOW,
        ),
    )
    for table in (
        "financial_numeric_facts",
        "financial_numeric_facts_hot",
        "financial_numeric_facts_history",
    ):
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
                NOW,
                NOW,
            ),
        )
    conn.execute(
        """INSERT INTO financial_facts(
               instrument_id, symbol, exchange, report_period, schema_version,
               total_assets, source, source_mode, data_as_of, facts_json,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "600000.SH",
            "600000",
            "SSE",
            "2025-12-31",
            "financial_facts.v1",
            100.0,
            "official_json",
            "structured",
            "2026-04-01",
            '{"total_assets":100.0}',
            NOW,
            NOW,
        ),
    )


def _seed_legacy_effective_projection(conn: sqlite3.Connection) -> None:
    digest = "a" * 64
    conn.execute(
        """INSERT INTO official_announcements(
               announcement_id, schema_version, source, source_announcement_id,
               title, instrument_id, exchange, published_at, published_at_raw,
               raw_payload_hash, first_observed_at, last_observed_at, status,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "announcement-correction",
            "official_announcement.v1",
            "cninfo",
            "correction-1",
            "2025 annual report correction",
            "600000.SH",
            "SSE",
            "2026-04-02T01:00:00+00:00",
            "2026-04-02 09:00:00",
            "b" * 64,
            NOW,
            NOW,
            "observed",
            NOW,
            NOW,
        ),
    )
    conn.execute(
        """INSERT INTO official_announcement_attachments(
               attachment_id, schema_version, announcement_id,
               attachment_identity, source_attachment_id, source_url,
               normalized_source_url, name, media_type, content_length_hint,
               first_observed_at, last_observed_at, created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "attachment-correction",
            "official_announcement_attachment.v1",
            "announcement-correction",
            "source:file-correction",
            "file-correction",
            "https://static.example/correction.pdf",
            "https://static.example/correction.pdf",
            "2025 annual report correction.pdf",
            "application/pdf",
            100,
            NOW,
            NOW,
            NOW,
            NOW,
        ),
    )
    conn.execute(
        """INSERT INTO official_document_blobs(
               content_hash, schema_version, content_length, canonical_path,
               signature_status, integrity_status, first_available_at,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            digest,
            "official_document_blob.v1",
            100,
            f"data/filings/announcements/sha256/{digest}.pdf",
            "valid_pdf",
            "valid",
            NOW,
            NOW,
            NOW,
        ),
    )
    conn.execute(
        """INSERT INTO official_attachment_versions(
               version_id, schema_version, attachment_id, observation_key,
               content_hash, retrieval_status, integrity_status, observed_at,
               first_observed_at, last_observed_at, version_available_at,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "version-correction",
            "official_attachment_version.v2",
            "attachment-correction",
            "observation-correction",
            digest,
            "available",
            "valid",
            NOW,
            NOW,
            NOW,
            NOW,
            NOW,
            NOW,
        ),
    )
    conn.execute(
        """INSERT INTO effective_annual_reports(
               asset_id, schema_version, instrument_id, fiscal_year,
               report_period, announcement_id, attachment_id, version_id,
               content_hash, source, source_announcement_id, published_at,
               variant, classifier_version, decision_state, availability,
               predecessor_asset_id, activated_at, last_checked_at,
               created_at, updated_at
           ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "asset-correction",
            "effective_annual_report.v1",
            "600000.SH",
            2025,
            "2025-12-31",
            "announcement-correction",
            "attachment-correction",
            "version-correction",
            digest,
            "cninfo",
            "correction-1",
            "2026-04-02T01:00:00+00:00",
            "correction",
            "formal_annual_report.v1",
            "current",
            "local_valid",
            "asset-original-legacy-hint",
            NOW,
            NOW,
            NOW,
            NOW,
        ),
    )


def _insert_outbox_event(conn: sqlite3.Connection, event_key: str) -> None:
    conn.execute(
        """INSERT INTO official_asset_change_events(
               event_key, event_type, instrument_id, fiscal_year, created_at
           ) VALUES(?, 'added', '600000.SH', 2025, ?)""",
        (event_key, NOW),
    )


def _insert_decision(
    conn: sqlite3.Connection,
    *,
    decision_id: str,
    event_key: str,
    decision_kind: str = "initial_activation",
    predecessor: bool = False,
) -> None:
    predecessor_values = (
        (
            "asset-old",
            "cninfo",
            "filing-old",
            "announcement-old",
            "attachment-old",
            "version-old",
            "a" * 64,
        )
        if predecessor
        else (None,) * 7
    )
    conn.execute(
        """INSERT INTO official_annual_report_decisions(
               decision_id, schema_version, instrument_id, fiscal_year,
               decision_kind, predecessor_asset_id, predecessor_source,
               predecessor_source_announcement_id, predecessor_announcement_id,
               predecessor_attachment_id, predecessor_version_id,
               predecessor_content_hash, replacement_asset_id,
               replacement_source, replacement_source_announcement_id,
               replacement_announcement_id, replacement_attachment_id,
               replacement_version_id, replacement_content_hash, decision_state,
               classifier_version, decision_policy_version, activated_at,
               outbox_event_key, created_at
           ) VALUES(?, ?, '600000.SH', 2025, ?, ?, ?, ?, ?, ?, ?, ?,
                    'asset-new', 'cninfo', 'filing-new', 'announcement-new',
                    'attachment-new', 'version-new', ?, 'current', ?, ?, ?, ?, ?)""",
        (
            decision_id,
            "official_annual_report_decision.v1",
            decision_kind,
            *predecessor_values,
            "b" * 64,
            "formal_annual_report.v1",
            "canonical_source_filing.v1",
            NOW,
            event_key,
            NOW,
        ),
    )


def test_model_contracts_expose_persisted_lineage_fields_and_schema_versions():
    deletion_fields = {item.name for item in fields(OfficialAssetDeletionIntent)}
    processing_fields = {item.name for item in fields(OfficialAssetConsumerProcessing)}
    request_fields = {item.name for item in fields(OfficialAssetConsumerRequest)}
    manifest_fields = {item.name for item in fields(OfficialAssetRecoveryManifestEntry)}
    closure_fields = {item.name for item in fields(OfficialAssetRecoveryPairClosure)}
    journal_fields = {
        item.name for item in fields(OfficialAssetBackupRecoveryJournalEntry)
    }

    assert {
        "decision_id",
        "outbox_event_key",
        "operation_mount_source",
        "operation_mount_point",
        "operation_mount_fs_type",
        "operation_mount_device_id",
        "operation_mount_filesystem_key",
        "operation_mount_captured_at",
        "schema_version",
    }.issubset(deletion_fields)
    assert {
        "canonical_projection_policy_version",
        "evidence_set_hash",
        "equivalent_source_filings",
        "schema_version",
    }.issubset(processing_fields)
    assert {
        "consumer_request_id",
        "principal",
        "consumer",
        "request_fingerprint",
        "processing_fingerprint",
        "selector",
        "asset_request_id",
        "asset_id",
        "processing_id",
        "status",
        "result_state",
        "resolved_observation_version",
        "resolved_content_hash",
        "expires_at",
        "expired_at",
        "tombstone_until",
        "schema_version",
    }.issubset(request_fields)
    assert OfficialAssetDeletionIntent.__dataclass_fields__[
        "schema_version"
    ].default == (DELETION_INTENT_SCHEMA_VERSION)
    assert (
        OfficialAssetConsumerProcessing.__dataclass_fields__["schema_version"].default
        == CONSUMER_PROCESSING_SCHEMA_VERSION
    )
    assert (
        OfficialAssetConsumerRequest.__dataclass_fields__["schema_version"].default
        == CONSUMER_REQUEST_SCHEMA_VERSION
    )
    assert "recovery_pair_id" in manifest_fields
    assert {
        "recovery_pair_id",
        "recovery_id",
        "catalog_snapshot_identity",
        "catalog_snapshot_hash",
        "file_manifest_watermark",
        "schema_version",
    }.issubset(closure_fields)
    assert {
        "journal_sequence",
        "increment_identity",
        "source_catalog_generation",
        "predecessor_watermark",
        "coverage_watermark",
        "integrity_hash",
        "payload",
        "schema_version",
    }.issubset(journal_fields)
    assert (
        OfficialAssetRecoveryPairClosure.__dataclass_fields__["schema_version"].default
        == RECOVERY_PAIR_CLOSURE_SCHEMA_VERSION
    )
    assert (
        OfficialAssetBackupRecoveryJournalEntry.__dataclass_fields__[
            "schema_version"
        ].default
        == BACKUP_RECOVERY_JOURNAL_SCHEMA_VERSION
    )


def test_clean_announcement_schema_preserves_all_real_business_contract_objects(
    tmp_path,
):
    db_path = tmp_path / "research.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _seed_real_existing_contracts(conn)
        before = _contract_snapshot(conn)
        conn.commit()

    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()
    repository.initialize_schema()

    with sqlite3.connect(db_path) as conn:
        after = _contract_snapshot(conn)
        all_tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert len(before["tables"]) >= 30
    assert before == after
    assert set(OWNED_TABLES).issubset(all_tables)


def test_clean_recovery_schema_has_pairing_and_replay_fields(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    expected = {
        "official_asset_recovery_manifest": {
            "recovery_id",
            "recovery_pair_id",
            "file_manifest_watermark",
            "catalog_snapshot_watermark",
            "active_indefinitely",
        },
        "official_asset_recovery_pair_closures": {
            "closure_id",
            "recovery_pair_id",
            "recovery_id",
            "catalog_snapshot_identity",
            "catalog_snapshot_hash",
            "file_manifest_watermark",
            "verified_at",
            "verified_by",
            "evidence_json",
        },
        "official_asset_backup_recovery_journal": {
            "journal_entry_id",
            "journal_sequence",
            "increment_kind",
            "increment_identity",
            "source_catalog_generation",
            "predecessor_watermark",
            "coverage_watermark",
            "integrity_hash",
            "payload_json",
            "created_at",
            "created_by",
        },
    }
    with repository.connection() as conn:
        for table, required_columns in expected.items():
            actual_columns = {
                str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")
            }
            assert required_columns.issubset(actual_columns), table


def test_in_place_decision_history_migration_preserves_real_business_contracts(
    tmp_path,
):
    db_path = tmp_path / "research.db"
    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _seed_real_existing_contracts(conn)
        _seed_legacy_effective_projection(conn)
        conn.execute("DROP TABLE official_annual_report_decisions")
        conn.execute(
            "UPDATE official_asset_schema_versions SET schema_version=7 "
            "WHERE component='announcement_assets'"
        )
        before = _contract_snapshot(conn)
        conn.commit()

    repository.initialize_schema()
    repository.initialize_schema()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        after = _contract_snapshot(conn)
        schema_version = conn.execute(
            "SELECT schema_version FROM official_asset_schema_versions "
            "WHERE component='announcement_assets'"
        ).fetchone()[0]
        event = conn.execute(
            "SELECT predecessor_asset_id, payload_json "
            "FROM official_asset_change_events WHERE trigger_origin='schema_migration'"
        ).fetchone()
        _insert_outbox_event(conn, "event-invalid-after-migration")
        with pytest.raises(sqlite3.IntegrityError, match="lineage shape|CHECK"):
            _insert_decision(
                conn,
                decision_id="decision-invalid-after-migration",
                event_key="event-invalid-after-migration",
                decision_kind="replacement",
                predecessor=False,
            )
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2025
    )
    assert before == after
    assert schema_version == SCHEMA_VERSION
    assert len(decisions) == 1
    assert decisions[0].decision_kind.value == "migration_snapshot"
    assert decisions[0].predecessor_asset_id is None
    assert decisions[0].predecessor_source is None
    assert decisions[0].predecessor_attachment_id is None
    assert decisions[0].decision_evidence["legacy_predecessor_asset_id_hint"] == (
        "asset-original-legacy-hint"
    )
    assert event["predecessor_asset_id"] is None
    assert (
        json.loads(event["payload_json"])["legacy_predecessor_asset_id_hint"]
        == "asset-original-legacy-hint"
    )


def test_clean_and_migrated_decision_tables_enforce_lineage_and_outbox_constraints(
    tmp_path,
):
    db_path = tmp_path / "research.db"
    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _insert_outbox_event(conn, "event-valid")
        _insert_decision(conn, decision_id="decision-valid", event_key="event-valid")

        _insert_outbox_event(conn, "event-invalid-replacement")
        with pytest.raises(sqlite3.IntegrityError, match="lineage shape|CHECK"):
            _insert_decision(
                conn,
                decision_id="decision-invalid-replacement",
                event_key="event-invalid-replacement",
                decision_kind="replacement",
                predecessor=False,
            )

        _insert_outbox_event(conn, "event-invalid-initial")
        with pytest.raises(sqlite3.IntegrityError, match="lineage shape|CHECK"):
            _insert_decision(
                conn,
                decision_id="decision-invalid-initial",
                event_key="event-invalid-initial",
                predecessor=True,
            )

        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_decision(
                conn,
                decision_id="decision-orphan-outbox",
                event_key="event-missing",
            )

        _insert_outbox_event(conn, "event-duplicate-decision-id")
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _insert_decision(
                conn,
                decision_id="decision-valid",
                event_key="event-duplicate-decision-id",
            )
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _insert_decision(
                conn,
                decision_id="decision-duplicate-outbox",
                event_key="event-valid",
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute(
                "UPDATE official_annual_report_decisions "
                "SET decision_state='blocked' WHERE decision_id='decision-valid'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute(
                "DELETE FROM official_annual_report_decisions "
                "WHERE decision_id='decision-valid'"
            )


def test_v10_recovery_manifest_migration_preserves_evidence_without_false_closure(
    tmp_path,
):
    db_path = tmp_path / "research.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE official_asset_schema_versions(
                   component TEXT PRIMARY KEY,
                   schema_version INTEGER NOT NULL,
                   applied_at TEXT NOT NULL
               )"""
        )
        conn.execute(
            """INSERT INTO official_asset_schema_versions
               VALUES('announcement_assets', 10, ?)""",
            (NOW,),
        )
        conn.execute(
            """CREATE TABLE official_asset_recovery_manifest(
                   recovery_id TEXT PRIMARY KEY,
                   schema_version TEXT NOT NULL,
                   manifest_kind TEXT NOT NULL,
                   manifest_version INTEGER NOT NULL,
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
                   catalog_snapshot_watermark TEXT NOT NULL,
                   consumer TEXT,
                   active_indefinitely INTEGER NOT NULL,
                   created_at TEXT NOT NULL,
                   created_by TEXT NOT NULL,
                   evidence_json TEXT NOT NULL,
                   UNIQUE(manifest_kind, prior_path, content_hash)
               )"""
        )
        conn.execute(
            """INSERT INTO official_asset_recovery_manifest VALUES(
                   'legacy-recovery', 'official_asset_recovery_manifest.v1',
                   'correction_predecessor', 1, 'asset-old', 'cninfo',
                   'filing-old', 'attachment-old', 'version-old',
                   'data/filings/legacy-old.pdf', ?, 'asset-new', ?,
                   'sha256/legacy-old.pdf', 'files-legacy', 'db-legacy',
                   NULL, 1, ?, 'legacy-backup', '{}'
               )""",
            ("a" * 64, "b" * 64, NOW),
        )
        conn.commit()

    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()
    repository.initialize_schema()

    entries = repository.list_recovery_manifest_entries()
    assert len(entries) == 1
    assert entries[0].recovery_pair_id
    assert entries[0].catalog_snapshot_watermark == "db-legacy"
    assert entries[0].schema_version == "official_asset_recovery_manifest.v1"
    assert repository.list_recovery_pair_closures() == []
    with sqlite3.connect(db_path) as conn:
        version = conn.execute(
            """SELECT schema_version FROM official_asset_schema_versions
               WHERE component='announcement_assets'"""
        ).fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute(
                """UPDATE official_asset_recovery_manifest
                   SET catalog_snapshot_watermark='forged-closure'"""
            )
        with pytest.raises(sqlite3.IntegrityError, match="invalid recovery manifest"):
            conn.execute(
                """INSERT INTO official_asset_recovery_manifest(
                       recovery_id, schema_version, manifest_kind, manifest_version,
                       prior_path, content_hash, backup_object,
                       file_manifest_watermark, catalog_snapshot_watermark,
                       active_indefinitely, created_at, created_by, evidence_json
                   ) VALUES(
                       'missing-pair', 'official_asset_recovery_manifest.v1',
                       'correction_predecessor', 1, 'data/filings/missing-pair.pdf',
                       ?, 'sha256/missing-pair.pdf', 'files-legacy', 'db-legacy',
                       1, ?, 'legacy-backup', '{}'
                   )""",
                ("c" * 64, NOW),
            )
    assert version == SCHEMA_VERSION
