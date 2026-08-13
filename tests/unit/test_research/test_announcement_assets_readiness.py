from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from research.announcement_assets.config import AnnouncementAssetConfig
from research.announcement_assets.readiness import AnnouncementAssetReadinessService
from research.announcement_assets.repository import AnnouncementAssetRepository
from research.announcement_assets.storage import ContentAddressedBlobStore
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)

PDF = b"%PDF-1.4\nreadiness fixture\n%%EOF\n"


def _full_market_pair() -> dict[str, object]:
    return {
        "paired_census_snapshot_id": "census-complete",
        "metadata": {"census_reconciliation": {"status": "complete"}},
    }


def _config(tmp_path: Path, *, relaxed: bool = False) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
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
            },
            "rollout_gates": {
                "require_bootstrap": True,
                "require_integrity": True,
                "require_storage": True,
                "require_backup": not relaxed,
                "require_consumer_migration": not relaxed,
                "consumer_dependency_policy": "completed_assets_only",
            },
        },
        project_root=tmp_path,
    )


def _add_attachment_hint(
    repository: AnnouncementAssetRepository,
    *,
    source_id: str,
    content_length: int,
) -> None:
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=f"{source_id} annual report",
        published_at="2026-04-01T00:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        raw_payload={"announcementId": source_id},
    )
    announcement = repository.upsert_announcement(
        record,
        instrument_id="600000.SH",
        observed_at="2026-08-10T00:00:00+00:00",
    )
    repository.upsert_attachment(
        announcement.announcement_id,
        AnnouncementAttachment(
            source_url=f"https://static.example/{source_id}.pdf",
            attachment_id=source_id,
            name=f"{source_id}.pdf",
            media_type="application/pdf",
            raw_metadata={"content_length": content_length},
        ),
        observed_at="2026-08-10T00:00:00+00:00",
    )


def _insert_effective_fixture(
    repository: AnnouncementAssetRepository,
    config: AnnouncementAssetConfig,
    *,
    pending_candidate_id: str | None = None,
    acquisition_origin: str = "download",
    adopted_from_path: str | None = None,
) -> str:
    digest = hashlib.sha256(PDF).hexdigest()
    path = config.blob_root / digest[:2] / f"{digest}.pdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(PDF)
    now = "2026-08-10T00:00:00+00:00"
    with repository.transaction() as conn:
        conn.execute(
            """INSERT INTO official_announcements(
                   announcement_id, schema_version, source, source_announcement_id,
                   title, instrument_id, exchange, raw_payload_hash,
                   first_observed_at, last_observed_at, status,
                   provider_diagnostics_json, metadata_json, created_at, updated_at
               ) VALUES('ann-ready', 'official_announcement.v1', 'cninfo',
                        'filing-ready', 'annual report', '600000.SH', 'SSE',
                        'payload', ?, ?, 'observed', '{}', '{}', ?, ?)""",
            (now, now, now, now),
        )
        conn.execute(
            """INSERT INTO official_announcement_attachments(
                   attachment_id, schema_version, announcement_id,
                   attachment_identity, source_attachment_id, source_url,
                   normalized_source_url, name, media_type, content_length_hint,
                   first_observed_at, last_observed_at, metadata_json,
                   created_at, updated_at
               ) VALUES('att-ready', 'official_announcement_attachment.v1',
                        'ann-ready', 'att-ready', 'att-ready',
                        'https://static.example/ready.pdf',
                        'https://static.example/ready.pdf', 'ready.pdf',
                        'application/pdf', ?, ?, ?, '{}', ?, ?)""",
            (len(PDF), now, now, now, now),
        )
        conn.execute(
            """INSERT INTO official_document_blobs(
                   content_hash, schema_version, content_length, canonical_path,
                   signature_status, integrity_status, first_available_at,
                   last_verified_at, backup_status, backup_verified_at,
                   acquisition_origin, adopted_from_path,
                   verification_evidence_json, backup_evidence_json,
                   created_at, updated_at
               ) VALUES(?, 'official_document_blob.v1', ?, ?, 'valid_pdf', 'valid',
                        ?, ?, 'verified', ?, ?, ?, '{}', '{}', ?, ?)""",
            (
                digest,
                len(PDF),
                str(path),
                now,
                now,
                now,
                acquisition_origin,
                adopted_from_path,
                now,
                now,
            ),
        )
        conn.execute(
            """INSERT INTO official_attachment_versions(
                   version_id, schema_version, attachment_id, observation_key,
                   content_hash, retrieval_status, integrity_status, observed_at,
                   created_at, updated_at
               ) VALUES('version-ready', 'official_attachment_version.v1',
                        'att-ready', 'observation-ready', ?, 'success', 'valid',
                        ?, ?, ?)""",
            (digest, now, now, now),
        )
        conn.execute(
            """INSERT INTO effective_annual_reports(
                   asset_id, schema_version, instrument_id, fiscal_year,
                   report_period, announcement_id, attachment_id, version_id,
                   content_hash, source, source_announcement_id, variant,
                   classifier_version, decision_state, availability,
                   pending_candidate_id, last_checked_at, created_at, updated_at
               ) VALUES('asset-ready', 'effective_annual_report.v1', '600000.SH',
                        2025, '2025-12-31', 'ann-ready', 'att-ready',
                        'version-ready', ?, 'cninfo', 'filing-ready', 'original',
                        'formal_annual_report.v1', 'current', 'local_valid',
                        ?, ?, ?, ?)""",
            (digest, pending_candidate_id, now, now, now),
        )
        conn.execute(
            """INSERT INTO official_asset_backup_state(
                   content_hash, config_fingerprint, destination_identity,
                   failure_domain, backup_path, content_length, status,
                   file_manifest_watermark, catalog_snapshot_watermark,
                   verified_at, created_at, updated_at
               ) VALUES(?, ?, 'backup-ready', 'independent', ?, ?, 'verified',
                        'file-watermark', 'catalog-watermark', ?, ?, ?)""",
            (digest, config.config_fingerprint, str(path), len(PDF), now, now, now),
        )
    return digest


def test_readiness_is_redacted_for_frontend_and_detailed_for_operator(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetReadinessService(
        repository=repository,
        config=_config(tmp_path),
    )

    frontend = service.report(now="2026-08-10T00:00:00+00:00")
    assert frontend.status == "blocked"
    assert "bootstrap_incomplete" in frontend.blockers
    assert frontend.operator_diagnostics is None

    operator = service.report(
        operator=True,
        now="2026-08-10T00:00:00+00:00",
    )
    assert operator.operator_diagnostics is not None
    assert "consumer_migration_complete" in operator.operator_diagnostics


def test_complete_empty_universe_can_reach_ready_under_relaxed_rollout(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-empty",
            "policy_version": "a_share_active.v1",
            "master_data_version": "master-v1",
            "master_data_last_success_at": "2026-08-10T00:00:00+00:00",
            "snapshot_at": "2026-08-10T00:00:00+00:00",
            "freshness_limit_seconds": 36 * 3600,
            "status": "complete",
            "source_complete": True,
            "instruments": (),
            "indeterminate": (),
            **_full_market_pair(),
        }
    )
    service = AnnouncementAssetReadinessService(
        repository=repository,
        config=_config(tmp_path, relaxed=True),
    )

    report = service.report(now="2026-08-10T01:00:00+00:00")
    assert report.status == "ready"
    assert report.ready_for_daily is True
    assert report.summary["active_universe_size"] == 0


def test_required_capacity_artifact_blocks_daily_readiness_when_missing(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-empty",
            "policy_version": "a_share_active.v1",
            "master_data_version": "master-v1",
            "master_data_last_success_at": "2026-08-10T00:00:00+00:00",
            "snapshot_at": "2026-08-10T00:00:00+00:00",
            "freshness_limit_seconds": 36 * 3600,
            "status": "complete",
            "source_complete": True,
            "instruments": (),
            "indeterminate": (),
            **_full_market_pair(),
        }
    )
    config = replace(
        _config(tmp_path, relaxed=True),
        capacity_artifact_required=True,
    )

    report = AnnouncementAssetReadinessService(
        repository=repository,
        config=config,
    ).report(now="2026-08-10T01:00:00+00:00")

    assert "capacity_artifact_not_ready" in report.blockers
    assert report.ready_for_daily is False


@pytest.mark.parametrize(
    "pair_fields",
    [
        {},
        {
            "paired_census_snapshot_id": "census-incomplete",
            "metadata": {"census_reconciliation": {"status": "partial"}},
        },
    ],
)
def test_latest_unpaired_or_incompletely_reconciled_snapshot_blocks_daily_only(
    tmp_path, pair_fields
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-not-full-market",
            "policy_version": "a_share_active.v1",
            "snapshot_at": "2026-08-10T00:00:00+00:00",
            "freshness_limit_seconds": 36 * 3600,
            "status": "complete",
            "source_complete": True,
            "instruments": (),
            "indeterminate": (),
            **pair_fields,
        }
    )

    report = AnnouncementAssetReadinessService(
        repository=repository,
        config=_config(tmp_path, relaxed=True),
    ).report(now="2026-08-10T01:00:00+00:00")

    assert "full_market_census_reconciliation_incomplete" in report.blockers
    assert report.ready_for_daily is False
    assert report.ready_for_reads is True
    assert report.summary["full_market_universe_complete"] is False
    assert report.summary["estimate_state"] == "indeterminate"


def test_indeterminate_universe_blocks_full_market_readiness(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-indeterminate",
            "policy_version": "a_share_active.v1",
            "snapshot_at": "2026-08-10T00:00:00+00:00",
            "freshness_limit_seconds": 36 * 3600,
            "status": "degraded",
            "source_complete": False,
            "instruments": (),
            "indeterminate": ({"instrument_id": "600000.SH"},),
        }
    )
    service = AnnouncementAssetReadinessService(
        repository=repository,
        config=_config(tmp_path, relaxed=True),
    )

    report = service.report(now="2026-08-10T01:00:00+00:00")
    assert "universe_denominator_indeterminate" in report.blockers
    assert report.ready_for_daily is False
    assert report.ready_for_reads is True
    assert report.summary["estimate_state"] == "indeterminate"


def test_incomplete_or_stale_snapshot_never_publishes_available_estimate(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-incomplete",
            "policy_version": "a_share_active.v1",
            "snapshot_at": "2026-08-01T00:00:00+00:00",
            "freshness_limit_seconds": 3600,
            "status": "degraded",
            "source_complete": False,
            "instruments": ({"instrument_id": "600000.SH"},),
            "indeterminate": (),
        }
    )
    report = AnnouncementAssetReadinessService(
        repository=repository,
        config=_config(tmp_path, relaxed=True),
    ).report(now="2026-08-10T01:00:00+00:00")

    assert report.summary["estimate_state"] == "indeterminate"
    assert report.summary["estimated_required_bytes"] is None


def test_capacity_estimate_uses_largest_hints_and_old_plus_new_replacement_peak(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-capacity",
            "policy_version": "a_share_active.v1",
            "snapshot_at": "2026-08-10T00:00:00+00:00",
            "freshness_limit_seconds": 36 * 3600,
            "status": "complete",
            "source_complete": True,
            "instruments": ({"instrument_id": "600000.SH"},),
            "indeterminate": (),
            **_full_market_pair(),
        }
    )
    _add_attachment_hint(repository, source_id="small", content_length=100)
    _add_attachment_hint(repository, source_id="large", content_length=300)
    config = _config(tmp_path, relaxed=True)
    _insert_effective_fixture(
        repository,
        config,
        pending_candidate_id="candidate-correction",
    )

    report = AnnouncementAssetReadinessService(
        repository=repository,
        config=config,
    ).report(now="2026-08-10T01:00:00+00:00")

    assert report.summary["known_content_length_bytes"] == 300
    assert report.summary["replacement_peak_bytes"] == (
        len(PDF) + config.storage.unknown_length_reservation_bytes
    )
    assert report.summary["estimate_basis"].endswith(".v2")


def test_cleanup_gates_require_canonical_consumers_adoption_closure_and_alias_release(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path, relaxed=True)
    digest = _insert_effective_fixture(repository, config)
    now = "2026-08-10T00:00:00+00:00"
    repository.upsert_universe_snapshot(
        {
            "snapshot_id": "snapshot-cleanup",
            "policy_version": "a_share_active.v1",
            "snapshot_at": now,
            "freshness_limit_seconds": 36 * 3600,
            "status": "complete",
            "source_complete": True,
            "instruments": ({"instrument_id": "600000.SH"},),
            "indeterminate": (),
            **_full_market_pair(),
        }
    )
    with repository.transaction() as conn:
        for consumer in ("business_profile", "broker_risk_control"):
            conn.execute(
                """INSERT INTO official_asset_consumer_processing(
                       processing_id, schema_version, asset_id, consumer,
                       parser_version, parameter_hash, status,
                       equivalent_source_filings_json, metadata_json,
                       created_at, updated_at
                   ) VALUES(?, 'official_asset_consumer_processing.v1',
                            'asset-ready', ?, 'parser-v1', ?, 'current',
                            '[]', '{}', ?, ?)""",
                (f"processing-{consumer}", consumer, consumer, now, now),
            )
    service = AnnouncementAssetReadinessService(repository=repository, config=config)
    baseline = service.report(now="2026-08-10T01:00:00+00:00")
    assert baseline.summary["consumer_migration_complete"] is True
    assert baseline.legacy_write_stop_allowed is False
    assert baseline.summary["shared_custody_adoption_complete"] is False
    assert baseline.summary["recovery_pair_closure_complete"] is False
    assert baseline.summary["cleanup_alias_pins_released"] is False

    with repository.transaction() as conn:
        conn.execute(
            """INSERT INTO official_asset_adoption_promotion_gates(
                   gate_id, schema_version, asset_id, inventory_fingerprint,
                   config_fingerprint, content_hash, content_length,
                   canonical_path, mount_filesystem_key, custody_state, status,
                   reconciled_at, expires_at, evidence_json, created_at, updated_at
               ) VALUES('gate-ready', 'official_asset_adoption_promotion_gate.v1',
                        'asset-ready', 'inventory-v1', ?, ?, ?, '/legacy/ready.pdf',
                        'mount-v1', 'shared_controlled_legacy', 'ready', ?,
                        '2026-08-11T00:00:00+00:00', '{}', ?, ?)""",
            (config.config_fingerprint, digest, len(PDF), now, now, now),
        )
    awaiting_adoption = service.report(now="2026-08-10T01:00:00+00:00")
    assert awaiting_adoption.legacy_write_stop_allowed is False
    assert awaiting_adoption.summary["shared_custody_adoption_complete"] is False

    with repository.transaction() as conn:
        conn.execute(
            """UPDATE official_asset_adoption_promotion_gates
               SET status='consumed', consumed_at=?, updated_at=?
               WHERE gate_id='gate-ready'""",
            (now, now),
        )
        conn.execute(
            """INSERT INTO official_asset_retention_pins(
                   pin_id, blob_hash, pin_type, pin_key, owner, created_at,
                   blocks_primary_unlink, required_set_hold, metadata_json
               ) VALUES('pin-alias', ?, 'managed_alias', 'legacy-path',
                        'business_profile', ?, 1, 0, '{}')""",
            (digest, now),
        )
    pinned = service.report(now="2026-08-10T01:00:00+00:00")
    assert pinned.legacy_write_stop_allowed is False
    assert pinned.duplicate_cleanup_allowed is False
    assert pinned.summary["cleanup_alias_pins_released"] is False

    with repository.transaction() as conn:
        conn.execute(
            "UPDATE official_asset_retention_pins SET released_at=? WHERE pin_id='pin-alias'",
            (now,),
        )
        conn.execute(
            """INSERT INTO official_asset_recovery_manifest(
                   recovery_id, schema_version, manifest_kind, manifest_version,
                   predecessor_asset_id, prior_path, content_hash,
                   backup_object, file_manifest_watermark, recovery_pair_id,
                   created_at, created_by, evidence_json
               ) VALUES('recovery-open', 'official_asset_recovery_manifest.v1',
                        'legacy_duplicate', 1, 'asset-ready', '/legacy/ready.pdf',
                        ?, 'backup/ready.pdf', 'file-watermark', 'pair-open',
                        ?, 'test', '{}')""",
            (digest, now),
        )
    unclosed = service.report(now="2026-08-10T01:00:00+00:00")
    assert unclosed.legacy_write_stop_allowed is False
    assert unclosed.summary["recovery_pair_closure_complete"] is False

    with repository.transaction() as conn:
        conn.execute(
            """INSERT INTO official_asset_recovery_pair_closures(
                   closure_id, schema_version, recovery_pair_id, recovery_id,
                   catalog_snapshot_identity, catalog_snapshot_hash,
                   file_manifest_watermark, verified_at, verified_by, evidence_json
               ) VALUES('closure-open', 'official_asset_recovery_pair_closure.v1',
                        'pair-open', 'recovery-open', 'catalog-snapshot',
                        'catalog-hash', 'file-watermark', ?, 'test', '{}')""",
            (now,),
        )
    closed = service.report(now="2026-08-10T01:00:00+00:00")
    assert closed.summary["recovery_pair_closure_complete"] is True
    assert closed.legacy_write_stop_allowed is True


def test_persisted_readiness_report_round_trips_typed_and_redacted(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetReadinessService(
        repository=repository,
        config=_config(tmp_path, relaxed=True),
    )
    persisted = service.report(
        operator=True,
        now="2026-08-10T01:00:00+00:00",
        persist=True,
        scope_key="daily",
    )

    redacted = service.load_latest_report(scope_key="daily")
    operator = service.load_latest_report(operator=True, scope_key="daily")
    assert redacted is not None and operator is not None
    assert redacted.report_id == persisted.report_id
    assert redacted.summary == persisted.summary
    assert redacted.operator_diagnostics is None
    assert json.dumps(operator.operator_diagnostics, sort_keys=True) == json.dumps(
        persisted.operator_diagnostics,
        sort_keys=True,
    )


def _write_part_evidence(config: AnnouncementAssetConfig, *, size: int, now: str) -> None:
    path = config.temp_root / "readiness.part"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    Path(f"{path}.json").write_text(
        json.dumps(
            {
                "artifact_type": "part",
                "managed_path": str(path),
                "created_at": now,
                "actual_bytes": size,
                "owner": "readiness-test",
                "lease_generation": "g1",
            }
        ),
        encoding="utf-8",
    )


def _threshold_config(
    tmp_path: Path,
    *,
    storage_overrides: dict[str, int],
) -> AnnouncementAssetConfig:
    base = _config(tmp_path, relaxed=True)
    normalized = base.normalized_mapping()
    return AnnouncementAssetConfig.from_mapping(
        {
            **normalized,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "storage": {**normalized["storage"], **storage_overrides},
            "rollout_gates": {
                **normalized["rollout_gates"],
                "require_bootstrap": False,
            },
        },
        project_root=tmp_path,
    )


def test_part_warning_degrades_but_does_not_block_readiness(tmp_path):
    config = _threshold_config(
        tmp_path,
        storage_overrides={
            "part_warning_bytes": 1,
            "part_max_bytes": 10,
            "part_warning_age_seconds": 10,
            "part_max_age_seconds": 100,
            "part_safety_grace_seconds": 1,
        },
    )
    _write_part_evidence(config, size=2, now="2026-08-10T00:00:00+00:00")
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetReadinessService(repository=repository, config=config)

    report = service.report(now="2026-08-10T00:00:01+00:00")
    assert report.status == "degraded"
    assert "temporary_part_threshold_warning" in report.warnings
    assert "temporary_part_threshold_exceeded" not in report.blockers
    assert report.ready_for_daily is True
    assert report.ready_for_deletion is True


def test_invalid_part_sidecar_blocks_writes_but_not_local_readiness(tmp_path):
    config = _config(tmp_path, relaxed=True)
    config.temp_root.mkdir(parents=True, exist_ok=True)
    path = config.temp_root / "invalid.part"
    path.write_bytes(b"x")
    Path(f"{path}.json").write_text("{}", encoding="utf-8")
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetReadinessService(repository=repository, config=config)

    report = service.report(now="2026-08-10T00:00:01+00:00")
    assert "artifact_sidecar_invalid" in report.blockers
    assert report.ready_for_reads is True
    assert report.ready_for_daily is False
    assert report.ready_for_deletion is False


def _write_quarantine_evidence(
    config: AnnouncementAssetConfig,
    *,
    size: int,
    now: str,
) -> Path:
    path = config.quarantine_root / "quarantine.pdf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    Path(f"{path}.json").write_text(
        json.dumps(
            {
                "artifact_type": "quarantine",
                "managed_path": str(path),
                "created_at": now,
                "actual_bytes": size,
                "content_hash": "a" * 64,
                "reason": "fixture",
            }
        ),
        encoding="utf-8",
    )
    return path


def test_quarantine_warning_and_hard_thresholds_are_deterministic(tmp_path):
    warning = _threshold_config(
        tmp_path,
        storage_overrides={
            "quarantine_warning_bytes": 1,
            "quarantine_hard_bytes": 10,
            "quarantine_warning_age_seconds": 10,
            "quarantine_hard_age_seconds": 100,
        },
    )
    _write_quarantine_evidence(
        warning, size=2, now="2026-08-10T00:00:00+00:00"
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    report = AnnouncementAssetReadinessService(
        repository=repository, config=warning
    ).report(now="2026-08-10T00:00:01+00:00")
    assert report.status == "degraded"
    assert "quarantine_warning_threshold_exceeded" in report.warnings

    hard = _threshold_config(
        tmp_path,
        storage_overrides={
            "quarantine_warning_bytes": 1,
            "quarantine_hard_bytes": 2,
            "quarantine_warning_age_seconds": 10,
            "quarantine_hard_age_seconds": 100,
        },
    )
    report = AnnouncementAssetReadinessService(
        repository=repository, config=hard
    ).report(now="2026-08-10T00:00:01+00:00")
    assert report.status == "blocked"
    assert "quarantine_hard_threshold_exceeded" in report.blockers
    assert report.ready_for_reads is True
    assert report.ready_for_daily is False
    assert report.ready_for_deletion is False


def test_quarantine_cleanup_requires_operator_and_records_audit(tmp_path):
    config = _threshold_config(tmp_path, storage_overrides={})
    _write_quarantine_evidence(
        config, size=2, now="2026-08-01T00:00:00+00:00"
    )
    store = ContentAddressedBlobStore(config)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    with pytest.raises(PermissionError, match="operator authorization"):
        store.cleanup_quarantine(
            authorized=False,
            actor="operator",
            audit=lambda _: None,
            now=None,
        )

    events: list[dict] = []

    def audit(event):
        evidence = dict(event)
        events.append(evidence)
        repository.append_storage_artifact_audit(evidence)

    cleaned = store.cleanup_quarantine(
        authorized=True,
        actor="operator",
        audit=audit,
        older_than_seconds=0,
    )
    assert cleaned == 1
    assert [event["status"] for event in events] == ["planned", "deleted"]
    assert [
        row["status"] for row in repository.list_storage_artifact_audit()
    ] == ["planned", "deleted"]
