from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

import research.announcement_assets.migration as migration_module
from research.announcement_assets import (
    AnnouncementArchiveInventory,
    AnnouncementAssetOutboxDispatcher,
    AnnouncementAssetRepository,
    AnnualReportVariant,
    ArchiveInventoryReport,
    AssetAvailability,
    EffectiveDecisionState,
    MountIdentity,
    probe_nfs_capabilities,
)
from research.announcement_assets.config import (
    AnnouncementAssetConfig,
    BackupConfig,
    LegacyArchiveRegistryConfig,
)

PDF_A = b"%PDF-1.4\nannual report A\n%%EOF\n"
PDF_B = b"%PDF-1.4\nannual report B\n%%EOF\n"
CONFIG_FINGERPRINT = "test-announcement-asset-config.v1"


def _config(tmp_path: Path, *, backup: bool = False) -> AnnouncementAssetConfig:
    legacy = LegacyArchiveRegistryConfig(
        business_profile_root=tmp_path / "business",
        broker_risk_control_root=tmp_path / "broker",
    )
    backup_config = BackupConfig(
        enabled=True,
        mount_root=tmp_path / "backup-mount",
        destination_root=tmp_path / "backup-mount" / "assets",
        expected_mount_source="backup-source",
        expected_failure_domain="backup-domain",
        freshness_hours=48,
    ) if backup else BackupConfig()
    return AnnouncementAssetConfig(
        project_root=tmp_path,
        filings_root=Path("."),
        archive_root=Path("canonical"),
        temp_root=Path("canonical/tmp"),
        quarantine_root=Path("canonical/quarantine"),
        adoption_roots=(Path("business"), Path("broker")),
        legacy_inventory=legacy,
        require_filings_mount=False,
        backup=backup_config,
    )


def _legacy_custody(path: Path, config: AnnouncementAssetConfig) -> dict:
    return {
        "path": str(path.resolve()),
        "content_hash": _digest(path.read_bytes()),
        "mount_filesystem_key": probe_nfs_capabilities(
            path, config.blob_root, perform_probe=False
        ).source_filesystem_key,
        "config_fingerprint": config.config_fingerprint,
        "legacy_writer_disabled": True,
        "legacy_cleaner_disabled": True,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "evidence_ref": "test-custody-shutdown.v1",
    }


def _legacy_path_exclusion_custody(
    path: Path, config: AnnouncementAssetConfig
) -> dict:
    evidence = _legacy_custody(path, config)
    evidence.update(
        {
            "custody_mode": "exact_path_excluded",
            "legacy_writer_disabled": False,
            "legacy_cleaner_disabled": False,
            "legacy_writer_excludes_exact_path": True,
            "legacy_cleaner_excludes_exact_path": True,
            "evidence_ref": "test-exact-path-exclusion.v1",
        }
    )
    return evidence


def _seed_backup(
    repository: AnnouncementAssetRepository,
    config: AnnouncementAssetConfig,
    path: Path,
    *,
    destination_identity: str | None = None,
) -> Path:
    assert config.backup.destination_root is not None
    backup_path = config.backup.destination_root / "blobs" / path.name
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.write_bytes(path.read_bytes())
    repository.upsert_backup_state(
        content_hash=_digest(path.read_bytes()),
        config_fingerprint=config.config_fingerprint,
        destination_identity=destination_identity
        or probe_nfs_capabilities(
            backup_path, config.backup.destination_root, perform_probe=False
        ).source_filesystem_key,
        failure_domain=str(config.backup.expected_failure_domain),
        backup_path=str(backup_path),
        content_length=path.stat().st_size,
        status="verified",
        file_manifest_watermark="files-test-watermark",
        catalog_snapshot_watermark="catalog-test-watermark",
        verified_at=datetime.now(timezone.utc).isoformat(),
    )
    return backup_path


def test_legacy_expired_operation_is_migrated_to_blocked_projection(tmp_path):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE official_asset_operations (
                operation_id TEXT PRIMARY KEY,
                schema_version TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                scope_json TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                status TEXT NOT NULL,
                next_retry_at TEXT,
                reason_code TEXT,
                diagnostics_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO official_asset_operations(
                operation_id, schema_version, operation_type, idempotency_key,
                scope_json, policy_version, status, updated_at
            ) VALUES ('legacy-op', 'v1', 'ensure', 'legacy-key', '{}', 'v1', 'expired', '2026-08-10T00:00:00+00:00')
            """
        )
        conn.commit()

    repository = AnnouncementAssetRepository(db_path)
    repository.initialize_schema()

    with repository.connection() as conn:
        row = conn.execute(
            "SELECT status, reason_code, diagnostics_json FROM official_asset_operations WHERE operation_id='legacy-op'"
        ).fetchone()
    assert row["status"] == "blocked"
    assert row["reason_code"] == "legacy_expired_operation"
    assert '"legacy_status":"expired"' in row["diagnostics_json"]


def test_clean_operation_schema_rejects_expired_durable_status(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "clean.db")
    repository.initialize_schema()
    with pytest.raises(sqlite3.IntegrityError), repository.transaction() as conn:
        conn.execute(
            """
            INSERT INTO official_asset_operations(
                operation_id, schema_version, operation_type, idempotency_key,
                scope_json, policy_version, status, updated_at
            ) VALUES ('bad-op', 'v1', 'ensure', 'bad-key', '{}', 'v1', 'expired', '2026-08-10T00:00:00+00:00')
            """
        )


def test_reconciliation_rejects_caller_supplied_fingerprint_without_active_config(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    inventory = ArchiveInventoryReport(
        items=(),
        counts={},
        files_seen=0,
        manifest_rows_seen=0,
        inventory_fingerprint="inventory-v1",
    )
    with pytest.raises(ValueError, match="active configuration"):
        AnnouncementArchiveInventory().reconcile_shadow_adoption(
            inventory,
            repository=repository,
            config_fingerprint="caller-controlled",
        )


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _bound_evidence(mapping: dict) -> tuple[dict, str]:
    evidence = {
        "evidence_id": mapping["evidence_id"],
        "source": mapping["source"],
        "source_announcement_id": mapping["source_announcement_id"],
        "attachment_id": mapping["attachment_id"],
        "instrument_id": mapping["instrument_id"],
        "exchange": mapping["exchange"],
        "report_period": mapping["report_period"],
        "fiscal_year": mapping["fiscal_year"],
        "content_hash": mapping["content_hash"],
        "content_length": mapping["content_length"],
    }
    return evidence, migration_module._orphan_evidence_hash(evidence)


def _manifest(
    path: Path,
    *,
    source_file_id: str,
    filing_id: str,
    instrument_id: str = "600000.SH",
    exchange: str = "SSE",
    report_period: str = "2025-12-31",
    content: bytes = PDF_A,
    report_type: str = "annual_report",
    source: str = "cninfo",
    published_at: str = "2026-03-20T01:00:00+00:00",
    supersedes_source_file_id: str | None = None,
) -> dict:
    result = {
        "source_file_id": source_file_id,
        "instrument_id": instrument_id,
        "exchange": exchange,
        "report_period": report_period,
        "report_type": report_type,
        "filing_id": filing_id,
        "source": source,
        "archive_path": str(path),
        "content_hash": _digest(content),
        "content_length": len(content),
        "published_at": published_at,
        "status": "archived",
    }
    if supersedes_source_file_id:
        result["supersedes_source_file_id"] = supersedes_source_file_id
    return result


def test_inventory_excludes_isolated_pdf_with_explicit_annual_summary_title(
    tmp_path, monkeypatch
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = broker / "SSE/601555"
    folder.mkdir(parents=True)
    summary = folder / "601555_2023-12-31_summary.pdf"
    summary.write_bytes(PDF_A)
    monkeypatch.setattr(
        migration_module,
        "_pdf_first_page_is_explicit_annual_report_summary",
        lambda path: path == summary,
    )

    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                summary,
                source_file_id="summary",
                filing_id="summary",
                instrument_id="601555.SH",
                report_period="2023-12-31",
            ),
        ],
    )

    by_path = {item.path: item for item in inventory.items}
    assert by_path[str(summary)].status == "out_of_scope"
    assert by_path[str(summary)].reason == "verified_pdf_summary_title"


def test_pdf_summary_helper_normalizes_extracted_title(monkeypatch, tmp_path):
    class Page:
        @staticmethod
        def extract_text():
            return "2023 年 年度报告 摘要"

    class Reader:
        def __init__(self, *args, **kwargs):
            self.pages = [Page()]

    import pypdf

    monkeypatch.setattr(pypdf, "PdfReader", Reader)

    assert migration_module._pdf_first_page_is_explicit_annual_report_summary(
        tmp_path / "summary.pdf"
    )


def test_pdf_summary_helper_treats_any_pypdf_error_as_no_evidence(
    monkeypatch, tmp_path
):
    from pypdf.errors import ParseError

    class Reader:
        def __init__(self, *args, **kwargs):
            raise ParseError("malformed xref")

    import pypdf

    monkeypatch.setattr(pypdf, "PdfReader", Reader)

    assert not migration_module._pdf_first_page_is_explicit_annual_report_summary(
        tmp_path / "broken.pdf"
    )


def test_pdf_summary_detection_failure_preserves_manifest_candidate(
    tmp_path, monkeypatch
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = broker / "SSE/600000"
    folder.mkdir(parents=True)
    report = folder / "600000_2024-12-31_report.pdf"
    report.write_bytes(PDF_A)
    monkeypatch.setattr(
        migration_module,
        "_pdf_first_page_is_explicit_annual_report_summary",
        lambda path: False,
    )

    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                report,
                source_file_id="report",
                filing_id="report",
                report_period="2024-12-31",
            )
        ],
    )

    assert inventory.items[0].status == "adoptable"


def test_inventory_classifies_mixed_archives_without_mutation(tmp_path):
    business = tmp_path / "data/filings/business_profile"
    broker = tmp_path / "data/filings/financial_statements/broker_risk_control"
    business_dir = business / "2025/SSE"
    broker_dir = broker / "SSE/600000"
    business_dir.mkdir(parents=True)
    broker_dir.mkdir(parents=True)
    digest = _digest(PDF_A)
    business_pdf = business_dir / f"600000_SH_2025Q4_filing-1_{digest}.pdf"
    broker_pdf = broker_dir / "600000_2025-12-31_filing-1.pdf"
    business_pdf.write_bytes(PDF_A)
    broker_pdf.write_bytes(PDF_A)
    semiannual = business_dir / f"600000_SH_2025Q2_half-1_{digest}.pdf"
    semiannual.write_bytes(PDF_A)
    derived = business_dir / "derived/pages.json"
    derived.parent.mkdir()
    derived.write_text("{}", encoding="utf-8")
    orphan = business_dir / f"600001_SH_2025Q4_orphan_{digest}.pdf"
    orphan.write_bytes(PDF_A)
    missing = broker_dir / "600000_2024-12-31_missing.pdf"
    manifests = [
        _manifest(
            business_pdf,
            source_file_id="bp-1",
            filing_id="filing-1",
        ),
        _manifest(
            broker_pdf,
            source_file_id="broker-1",
            filing_id="filing-1",
        ),
        _manifest(
            missing,
            source_file_id="missing-1",
            filing_id="missing",
            report_period="2024-12-31",
        ),
    ]
    before = {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in (business_pdf, broker_pdf, semiannual, orphan)
    }

    report = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=manifests,
    )

    assert report.counts == {
        "derived": 1,
        "duplicate": 2,
        "missing": 1,
        "orphan": 1,
        "out_of_scope": 1,
    }
    assert report.network_requests == 0
    assert report.files_moved == 0
    assert report.files_linked == 0
    assert report.files_quarantined == 0
    assert report.files_deleted == 0
    assert {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in before
    } == before


def test_orphan_reconciliation_uses_metadata_only_identity_and_keeps_ambiguous_files(
    tmp_path,
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "unclassified"
    folder.mkdir(parents=True)
    resolved = folder / "orphan-report.pdf"
    ambiguous = folder / "ambiguous-report.pdf"
    resolved.write_bytes(PDF_A)
    ambiguous.write_bytes(PDF_A)
    before = {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in (resolved, ambiguous)
    }
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=(),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    official_mapping = {
        "path": str(resolved),
        "evidence_id": "official-meta-1",
        "evidence_type": "official_metadata",
        "metadata_only": True,
        "source": "cninfo",
        "source_announcement_id": "official-2025-1",
        "attachment_id": "official-attachment-1",
        "instrument_id": "600000.SH",
        "exchange": "SSE",
        "report_period": "2025-12-31",
        "fiscal_year": 2025,
        "document_family": "annual_report",
        "variant": "original",
        "is_full_report": True,
        "content_hash": _digest(PDF_A),
        "content_length": len(PDF_A),
        "published_at": "2026-03-20T01:00:00+00:00",
    }
    official_mapping["evidence"], official_mapping["evidence_hash"] = (
        _bound_evidence(official_mapping)
    )
    report = AnnouncementArchiveInventory().reconcile_orphans(
        inventory,
        repository=repository,
        official_metadata=(official_mapping,),
        audited_operator_mappings=(
            {
                "path": str(ambiguous),
                "evidence_id": "operator-map-1",
                "evidence_type": "audited_operator_mapping",
                "source": "cninfo",
                "source_announcement_id": "operator-2025-1",
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "report_period": "2025-12-31",
                "fiscal_year": 2025,
                "document_family": "annual_report",
                "variant": "original",
                "is_full_report": True,
                "content_hash": _digest(PDF_A),
                "content_length": len(PDF_A),
                "audit_id": "audit-1",
                "audited_by": "operator:alice",
                "audited_at": "2026-08-10T00:00:00+00:00",
            },
        ),
        observed_at="2026-08-10T00:00:00+00:00",
    )

    assert report.network_requests == 0
    assert report.resolved_paths == (str(resolved.resolve()),)
    assert report.skipped["identity_incomplete:attachment_id"] == 1
    adopted = repository.get_effective_report(
        "600000.SH", 2025, include_shadow=True
    )
    assert adopted is not None
    assert adopted.visibility_state == "shadow"
    assert repository.get_effective_report("600000.SH", 2025) is None
    assert {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in before
    } == before


def test_orphan_operator_mapping_persists_audit_and_evidence_in_shadow_metadata(
    tmp_path,
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "unclassified"
    folder.mkdir(parents=True)
    orphan = folder / "operator-mapped-report.pdf"
    orphan.write_bytes(PDF_A)
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=(),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    mapping = {
        "path": str(orphan),
        "evidence_id": "operator-map-verified-1",
        "evidence_type": "audited_operator_mapping",
        "source": "cninfo",
        "source_announcement_id": "operator-2025-1",
        "attachment_id": "operator-attachment-1",
        "instrument_id": "600000.SH",
        "exchange": "SSE",
        "report_period": "2025-12-31",
        "fiscal_year": 2025,
        "document_family": "annual_report",
        "variant": "original",
        "is_full_report": True,
        "content_hash": _digest(PDF_A),
        "content_length": len(PDF_A),
        "audit_id": "audit-operator-2025-1",
        "audited_by": "operator:alice",
        "audited_at": "2026-08-10T00:00:00+00:00",
        "evidence": {
            "evidence_id": "operator-map-verified-1",
            "source": "cninfo",
            "source_announcement_id": "operator-2025-1",
            "attachment_id": "operator-attachment-1",
            "instrument_id": "600000.SH",
            "exchange": "SSE",
            "report_period": "2025-12-31",
            "fiscal_year": 2025,
            "content_hash": _digest(PDF_A),
            "content_length": len(PDF_A),
            "review_reason": "legacy manifest was lost during archive migration",
        },
    }
    report = AnnouncementArchiveInventory().reconcile_orphans(
        inventory,
        repository=repository,
        audited_operator_mappings=(mapping,),
        observed_at="2026-08-10T00:00:00+00:00",
    )

    assert report.network_requests == 0
    assert report.resolved_paths == (str(orphan.resolve()),)
    assert not report.skipped
    rows = repository.list_candidate_rows(
        instrument_id="600000.SH",
        fiscal_year=2025,
        include_shadow=True,
    )
    assert len(rows) == 1
    for metadata_key in ("attachment_metadata", "version_metadata"):
        evidence = rows[0][metadata_key]["orphan_reconciliation"]
        assert evidence["evidence_kind"] == "audited_operator_mapping"
        assert evidence["evidence_id"] == "operator-map-verified-1"
        assert evidence["audit_id"] == "audit-operator-2025-1"
        assert evidence["audited_by"] == "operator:alice"
        assert evidence["audited_at"] == "2026-08-10T00:00:00+00:00"
        assert evidence["evidence"]["review_reason"].startswith("legacy manifest")
        assert evidence["evidence_hash"] == migration_module._orphan_evidence_hash(
            evidence["evidence"]
        )
    assert repository.get_effective_report("600000.SH", 2025) is None


def test_orphan_official_metadata_without_bound_evidence_is_not_adopted(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "unclassified"
    folder.mkdir(parents=True)
    orphan = folder / "unbound-official-report.pdf"
    orphan.write_bytes(PDF_A)
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=(),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    mapping = {
        "path": str(orphan),
        "evidence_id": "official-unbound-1",
        "evidence_type": "official_metadata",
        "metadata_only": True,
        "source": "cninfo",
        "source_announcement_id": "official-2025-unbound",
        "attachment_id": "official-attachment-unbound",
        "instrument_id": "600000.SH",
        "exchange": "SSE",
        "report_period": "2025-12-31",
        "fiscal_year": 2025,
        "document_family": "annual_report",
        "content_hash": _digest(PDF_A),
        "content_length": len(PDF_A),
    }

    report = AnnouncementArchiveInventory().reconcile_orphans(
        inventory,
        repository=repository,
        official_metadata=(mapping,),
    )

    assert report.network_requests == 0
    assert report.resolved_paths == ()
    assert report.skipped["evidence_not_verifiable"] == 1
    assert repository.list_candidate_rows(include_shadow=True) == []


def test_orphan_same_path_multiple_mappings_fail_closed_without_shadow_mutation(
    tmp_path,
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "unclassified"
    folder.mkdir(parents=True)
    orphan = folder / "multiply-mapped-report.pdf"
    orphan.write_bytes(PDF_A)
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=(),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    base = {
        "path": str(orphan),
        "evidence_id": "official-map-1",
        "evidence_type": "official_metadata",
        "metadata_only": True,
        "source": "cninfo",
        "source_announcement_id": "official-2025-1",
        "attachment_id": "official-attachment-1",
        "instrument_id": "600000.SH",
        "exchange": "SSE",
        "report_period": "2025-12-31",
        "fiscal_year": 2025,
        "document_family": "annual_report",
        "variant": "original",
        "is_full_report": True,
        "content_hash": _digest(PDF_A),
        "content_length": len(PDF_A),
    }
    base["evidence"], base["evidence_hash"] = _bound_evidence(base)
    conflicting = dict(base)
    conflicting["evidence_id"] = "official-map-2"
    conflicting["source_announcement_id"] = "official-2025-2"
    conflicting["evidence"] = dict(base["evidence"])
    conflicting["evidence"]["evidence_id"] = "official-map-2"
    conflicting["evidence"]["source_announcement_id"] = "official-2025-2"
    conflicting["evidence_hash"] = migration_module._orphan_evidence_hash(
        conflicting["evidence"]
    )
    before = (orphan.stat().st_mtime_ns, orphan.stat().st_mode, orphan.read_bytes())

    report = AnnouncementArchiveInventory().reconcile_orphans(
        inventory,
        repository=repository,
        official_metadata=(base, conflicting),
        observed_at="2026-08-10T00:00:00+00:00",
    )

    assert report.network_requests == 0
    assert report.resolved_paths == ()
    assert report.skipped["mapping_conflict"] == 1
    assert repository.list_candidate_rows(include_shadow=True) == []
    assert (orphan.stat().st_mtime_ns, orphan.stat().st_mode, orphan.read_bytes()) == before


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("fiscal_year", 2024, "fiscal_year_report_period_mismatch"),
        ("exchange", "SZSE", "instrument_exchange_mismatch"),
    ],
)
def test_orphan_mapping_rejects_inconsistent_period_or_exchange(
    tmp_path, field, value, reason
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "unclassified"
    folder.mkdir(parents=True)
    orphan = folder / f"invalid-{field}.pdf"
    orphan.write_bytes(PDF_A)
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=(),
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    mapping = {
        "path": str(orphan),
        "evidence_id": f"invalid-{field}-1",
        "evidence_type": "official_metadata",
        "metadata_only": True,
        "source": "cninfo",
        "source_announcement_id": f"official-{field}-1",
        "attachment_id": f"official-{field}-attachment",
        "instrument_id": "600000.SH",
        "exchange": "SSE",
        "report_period": "2025-12-31",
        "fiscal_year": 2025,
        "document_family": "annual_report",
        "variant": "original",
        "is_full_report": True,
        "content_hash": _digest(PDF_A),
        "content_length": len(PDF_A),
    }
    mapping[field] = value
    mapping["evidence"], mapping["evidence_hash"] = _bound_evidence(mapping)
    report = AnnouncementArchiveInventory().reconcile_orphans(
        inventory,
        repository=repository,
        official_metadata=(mapping,),
    )

    assert report.network_requests == 0
    assert report.resolved_paths == ()
    assert report.skipped[reason] == 1
    assert repository.list_candidate_rows(include_shadow=True) == []


def test_inventory_fails_closed_for_identity_hash_and_legal_conflicts(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    business_dir = business / "2025/SSE"
    broker_dir = broker / "SSE/600000"
    business_dir.mkdir(parents=True)
    broker_dir.mkdir(parents=True)
    first = business_dir / f"600000_SH_2025Q4_filing-1_{_digest(PDF_A)}.pdf"
    second = broker_dir / "600000_2025-12-31_filing-1.pdf"
    first.write_bytes(PDF_A)
    second.write_bytes(PDF_B)
    corrupt = business_dir / f"600001_SH_2025Q4_bad_{_digest(PDF_A)}.pdf"
    corrupt.write_bytes(b"not a pdf")

    report = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(first, source_file_id="first", filing_id="filing-1"),
            _manifest(
                second,
                source_file_id="second",
                filing_id="filing-1",
                content=PDF_B,
            ),
            _manifest(
                corrupt,
                source_file_id="corrupt",
                filing_id="bad",
                instrument_id="600001.SH",
                content=PDF_A,
            ),
        ],
    )

    statuses = {Path(item.path).name: (item.status, item.reason) for item in report.items}
    assert statuses[first.name] == (
        "conflicting",
        "same_legal_filing_has_different_content",
    )
    assert statuses[second.name] == (
        "conflicting",
        "same_legal_filing_has_different_content",
    )
    assert statuses[corrupt.name] == ("corrupt", "invalid_pdf_signature")


def test_inventory_rejects_manifest_identity_mismatch(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2025/SSE"
    folder.mkdir(parents=True)
    path = folder / f"600000_SH_2025Q4_filing-1_{_digest(PDF_A)}.pdf"
    path.write_bytes(PDF_A)

    report = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                path,
                source_file_id="wrong",
                filing_id="filing-1",
                instrument_id="000001.SZ",
            )
        ],
    )

    assert report.items[0].status == "conflicting"
    assert report.items[0].reason == "manifest_instrument_id_mismatch"


def test_inventory_surfaces_manifest_superseded_lineage(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2025/SSE"
    folder.mkdir(parents=True)
    old = folder / f"600000_SH_2025Q4_old_{_digest(PDF_A)}.pdf"
    new = folder / f"600000_SH_2025Q4_new_{_digest(PDF_B)}.pdf"
    old.write_bytes(PDF_A)
    new.write_bytes(PDF_B)
    old_manifest = _manifest(old, source_file_id="old-id", filing_id="old")
    new_manifest = _manifest(
        new,
        source_file_id="new-id",
        filing_id="new",
        content=PDF_B,
    )
    new_manifest["supersedes_source_file_id"] = "old-id"

    report = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[old_manifest, new_manifest],
    )

    statuses = {item.source_file_id: item.status for item in report.items}
    assert statuses == {"old-id": "superseded", "new-id": "adoptable"}


def test_shadow_adoption_reuses_old_periods_and_selects_correction_without_file_mutation(
    tmp_path,
):
    business = tmp_path / "data/filings/business_profile"
    broker = tmp_path / "data/filings/financial_statements/broker_risk_control"
    old_dir = business / "2023/SSE"
    current_dir = business / "2024/SSE"
    old_dir.mkdir(parents=True)
    current_dir.mkdir(parents=True)
    old_period = old_dir / f"600000_SH_2023Q4_annual-2023_{_digest(PDF_A)}.pdf"
    original = current_dir / f"600000_SH_2024Q4_annual-2024_{_digest(PDF_A)}.pdf"
    correction = current_dir / f"600000_SH_2024Q4_corrected-2024_{_digest(PDF_B)}.pdf"
    old_period.write_bytes(PDF_A)
    original.write_bytes(PDF_A)
    correction.write_bytes(PDF_B)
    manifests = [
        _manifest(
            old_period,
            source_file_id="annual-2023",
            filing_id="annual-2023",
            report_period="2023-12-31",
            published_at="2024-03-20T01:00:00+00:00",
        ),
        _manifest(
            original,
            source_file_id="annual-2024",
            filing_id="annual-2024",
            report_period="2024-12-31",
            published_at="2025-03-20T01:00:00+00:00",
        ),
        _manifest(
            correction,
            source_file_id="corrected-2024",
            filing_id="corrected-2024",
            report_period="2024-12-31",
            content=PDF_B,
            report_type="annual_report_correction",
            published_at="2025-04-02T01:00:00+00:00",
            supersedes_source_file_id="annual-2024",
        ),
    ]
    before = {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in (old_period, original, correction)
    }
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=manifests,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()

    first = migration.shadow_adopt(
        inventory,
        repository=repository,
        observed_at="2026-08-10T00:00:00+00:00",
    )
    second = migration.shadow_adopt(
        inventory,
        repository=repository,
        observed_at="2026-08-10T00:00:00+00:00",
    )

    assert repository.get_effective_report("600000.SH", 2023) is None
    assert repository.get_effective_report("600000.SH", 2024) is None
    adopted_old = repository.get_effective_report(
        "600000.SH", 2023, include_shadow=True
    )
    adopted_current = repository.get_effective_report(
        "600000.SH", 2024, include_shadow=True
    )
    assert adopted_old is not None
    assert adopted_old.content_hash == _digest(PDF_A)
    assert adopted_old.variant is AnnualReportVariant.ORIGINAL
    assert adopted_old.availability is AssetAvailability.LOCAL_VALID
    assert adopted_current is not None
    assert adopted_current.content_hash == _digest(PDF_B)
    assert adopted_current.variant is AnnualReportVariant.CORRECTION
    assert adopted_current.decision_state is EffectiveDecisionState.CURRENT
    assert first.files_adopted == 3
    assert first.legal_attachments_registered == 3
    assert first.blobs_registered == 2
    assert first.coverage_updates == 0
    assert first.network_requests == 0
    assert first.files_moved == first.files_linked == first.files_deleted == 0
    assert second.periods == first.periods
    with repository.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_discovery_state"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM official_attachment_versions"
        ).fetchone()[0] == 3
        active_pins = conn.execute(
            "SELECT pin_type, pin_key, blob_hash FROM official_asset_retention_pins "
            "WHERE released_at IS NULL ORDER BY pin_type, pin_key"
        ).fetchall()
        # The correction-chain replay keeps the shared legacy aliases and
        # creates recovery protection for the distinct superseded bytes.
        assert len(active_pins) == 6
        assert any(
            row[0] == "recovery_predecessor" and row[2] == _digest(PDF_A)
            for row in active_pins
        )
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2024
    )
    assert [decision.decision_kind.value for decision in decisions] == [
        "initial_activation",
        "replacement",
    ]
    events = repository.list_change_events()
    assert [event["event_type"] for event in events] == [
        "shadow_added",
        "shadow_added",
        "shadow_replaced",
    ]
    delivered: list[dict] = []
    dispatch = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda event: delivered.append(dict(event)),
    ).replay_until_idle()
    assert dispatch.delivered == 0
    assert dispatch.skipped == 3
    assert delivered == []
    assert {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in before
    } == before


def test_shadow_adoption_does_not_fabricate_cross_source_replacement(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    business_dir = business / "2024/SSE"
    broker_dir = broker / "SSE/600000"
    business_dir.mkdir(parents=True)
    broker_dir.mkdir(parents=True)
    original = business_dir / (
        f"600000_SH_2024Q4_cninfo-original_{_digest(PDF_A)}.pdf"
    )
    correction = broker_dir / "600000_2024-12-31_sse-correction.pdf"
    original.write_bytes(PDF_A)
    correction.write_bytes(PDF_B)
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                original,
                source_file_id="cninfo-original",
                filing_id="cninfo-original",
                report_period="2024-12-31",
                source="cninfo",
            ),
            _manifest(
                correction,
                source_file_id="sse-correction",
                filing_id="sse-correction",
                report_period="2024-12-31",
                report_type="annual_report_correction",
                content=PDF_B,
                source="sse",
                published_at="2025-04-02T01:00:00+00:00",
            ),
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()

    adoption = migration.shadow_adopt(inventory, repository=repository)

    effective = repository.get_effective_report(
        "600000.SH", 2024, include_shadow=True
    )
    assert effective is not None
    assert effective.source == "sse"
    assert effective.variant is AnnualReportVariant.CORRECTION
    decisions = repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2024
    )
    assert [decision.decision_kind.value for decision in decisions] == [
        "initial_activation"
    ]
    assert decisions[0].predecessor_asset_id is None
    assert repository.list_deletions() == []
    assert adoption.periods[0].status == "current"


def test_shadow_adoption_fails_closed_for_cross_source_supersedes_chain(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    business_dir = business / "2024/SSE"
    broker_dir = broker / "SSE/600000"
    business_dir.mkdir(parents=True)
    broker_dir.mkdir(parents=True)
    original = business_dir / (
        f"600000_SH_2024Q4_cninfo-original_{_digest(PDF_A)}.pdf"
    )
    correction = broker_dir / "600000_2024-12-31_sse-correction.pdf"
    original.write_bytes(PDF_A)
    correction.write_bytes(PDF_B)
    correction_manifest = _manifest(
        correction,
        source_file_id="sse-correction",
        filing_id="sse-correction",
        report_period="2024-12-31",
        report_type="annual_report_correction",
        content=PDF_B,
        source="sse",
        published_at="2025-04-02T01:00:00+00:00",
        supersedes_source_file_id="cninfo-original",
    )
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                original,
                source_file_id="cninfo-original",
                filing_id="cninfo-original",
                report_period="2024-12-31",
                source="cninfo",
            ),
            correction_manifest,
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()

    adoption = migration.shadow_adopt(inventory, repository=repository)

    assert adoption.periods[0].status == "conflicting"
    assert adoption.periods[0].reason == "legacy_supersedes_chain_cross_source"
    assert repository.get_effective_report(
        "600000.SH", 2024, include_shadow=True
    ) is None
    assert repository.list_effective_decisions(
        instrument_id="600000.SH", fiscal_year=2024
    ) == []
    assert repository.list_change_events() == []


def test_notice_only_legacy_correction_uses_canonical_vocabulary_without_promotion(
    tmp_path,
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2025/SSE"
    folder.mkdir(parents=True)
    digest = _digest(PDF_A)
    notice = folder / f"600000_SH_2025Q4_notice_{digest}.pdf"
    notice.write_bytes(PDF_A)
    manifest = _manifest(
        notice,
        source_file_id="notice-2025",
        filing_id="notice",
        report_type="correction_notice",
    )
    manifest["asset_classification"] = {
        "document_family": "annual_report",
        "variant": "correction",
        "is_full_report": False,
        "correction_evidence": True,
        "notice_only": True,
        "vocabulary_version": "official_document_classification.v1",
    }

    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[manifest],
    )
    item = inventory.items[0]
    assert item.document_family == "annual_report"
    assert item.variant is AnnualReportVariant.CORRECTION
    assert item.is_full_report is False

    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    adoption = AnnouncementArchiveInventory().shadow_adopt(
        inventory,
        repository=repository,
        observed_at="2026-08-10T00:00:00+00:00",
    )

    assert repository.get_effective_report(
        "600000.SH", 2025, include_shadow=True
    ) is None
    rows = repository.list_candidate_rows(
        source="cninfo", source_announcement_id="notice", include_shadow=True
    )
    assert rows[0]["classification"]["document_family"] == "annual_report"
    assert rows[0]["classification"]["variant"] == "correction"
    assert rows[0]["classification"]["is_full_report"] is False
    assert adoption.network_requests == 0


def test_shadow_reconciliation_matches_catalog_and_blocks_mismatch(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    original = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    correction = folder / f"600000_SH_2024Q4_correction_{_digest(PDF_B)}.pdf"
    original.write_bytes(PDF_A)
    correction.write_bytes(PDF_B)
    manifests = [
        _manifest(
            original,
            source_file_id="original",
            filing_id="original",
            report_period="2024-12-31",
            published_at="2025-03-01T00:00:00+00:00",
        ),
        _manifest(
            correction,
            source_file_id="correction",
            filing_id="correction",
            report_period="2024-12-31",
            content=PDF_B,
            report_type="annual_report_correction",
            published_at="2025-04-01T00:00:00+00:00",
            supersedes_source_file_id="original",
        ),
    ]
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=manifests,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration.shadow_adopt(inventory, repository=repository)
    delivered: list[dict] = []
    dispatcher = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="business-profile",
        handler=lambda event: delivered.append(dict(event)),
    )
    before_promotion = dispatcher.replay_until_idle()
    assert before_promotion.delivered == 0
    assert before_promotion.skipped == 2
    assert delivered == []
    shadow = repository.get_effective_report(
        "600000.SH", 2024, include_shadow=True
    )
    assert shadow is not None
    assert repository.list_candidate_rows(
        instrument_id="600000.SH", fiscal_year=2024
    ) == []
    assert len(
        repository.list_candidate_rows(
            instrument_id="600000.SH",
            fiscal_year=2024,
            include_shadow=True,
        )
    ) == 2
    assert repository.get_latest_valid_attachment_version(
        shadow.attachment_id
    ) is None
    assert repository.get_latest_valid_attachment_version(
        shadow.attachment_id, include_shadow=True
    ) is not None
    matching_row = {
        "source_file_id": "correction",
        "instrument_id": "600000.SH",
        "report_period": "2024-12-31",
        "source": "cninfo",
        "filing_id": "correction",
        "content_hash": _digest(PDF_B),
        "integrity_status": "valid",
        "is_active": True,
    }

    class LegacyCatalog:
        def __init__(self, rows):
            self.rows = rows
            self.calls = []

        def list_assets(self, **kwargs):
            self.calls.append(kwargs)
            return self.rows

    catalog = LegacyCatalog([matching_row])
    config = _config(tmp_path)

    matched = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        legacy_catalog=catalog,
        config=config,
        legacy_custody_evidence_by_path={correction: _legacy_custody(correction, config)},
    )

    path_excluded = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        legacy_catalog=catalog,
        config=config,
        legacy_custody_evidence_by_path={
            correction: _legacy_path_exclusion_custody(correction, config)
        },
    )
    mismatched = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        legacy_catalog_rows=[
            {**matching_row, "filing_id": "original", "content_hash": _digest(PDF_A)}
        ],
        config=config,
    )

    assert matched.ready_for_cutover is True
    assert path_excluded.ready_for_cutover is True
    assert path_excluded.periods[0].status == "matched"
    assert matched.periods[0].status == "matched"
    assert catalog.calls == [
        {"active_only": True, "validate_files": True},
        {"active_only": True, "validate_files": True},
    ]
    assert matched.network_requests == 0
    assert mismatched.ready_for_cutover is False
    assert mismatched.conflict_count == 1
    assert mismatched.periods[0].reason == "legacy_winner_mismatch"
    assert repository.get_effective_report("600000.SH", 2024) is None
    gate_id = matched.periods[0].promotion_gate_id
    assert gate_id is not None
    assert repository.get_adoption_promotion_gate(gate_id)["status"] == "ready"
    with pytest.raises(RuntimeError, match="configuration fingerprint changed"):
        migration.promote_shadow_adoption(
            matched,
            repository=repository,
            config=replace(config, policy_version="drifted-policy.v1"),
        )
    assert repository.get_adoption_promotion_gate(gate_id)["status"] == "ready"

    promoted = migration.promote_shadow_adoption(
        matched,
        repository=repository,
        config=config,
    )
    assert len(promoted) == 1
    visible = repository.get_effective_report("600000.SH", 2024)
    assert visible is not None
    assert visible.content_hash == _digest(PDF_B)
    assert visible.visibility_state == "production"
    visible_candidates = repository.list_candidate_rows(
        instrument_id="600000.SH", fiscal_year=2024
    )
    assert [row["version_id"] for row in visible_candidates] == [visible.version_id]
    assert repository.get_latest_valid_attachment_version(
        visible.attachment_id
    ) is not None
    assert repository.get_adoption_promotion_gate(gate_id)["status"] == "consumed"
    after_promotion = dispatcher.replay_until_idle()
    assert after_promotion.delivered == 1
    assert after_promotion.skipped == 0
    assert len(delivered) == 1
    assert delivered[0]["event_type"] == "added"
    assert delivered[0]["asset_id"] == visible.asset_id
    assert delivered[0]["predecessor_asset_id"] is None
    assert delivered[0]["trigger_origin"] == "asset_adoption_promotion"
    assert delivered[0]["payload"]["consumer_deliverable"] is True
    assert delivered[0]["payload"]["shadow_predecessor_asset_id"] is not None
    assert len(delivered[0]["payload"]["deferred_shadow_event_keys"]) == 2
    event_count = len(repository.list_change_events())
    repeated = migration.promote_shadow_adoption(
        matched,
        repository=repository,
        config=config,
    )
    assert repeated[0].asset_id == visible.asset_id
    assert len(repository.list_change_events()) == event_count
    late_events: list[dict] = []
    late_dispatch = AnnouncementAssetOutboxDispatcher(
        repository=repository,
        consumer="broker-risk-control",
        handler=lambda event: late_events.append(dict(event)),
    ).replay_until_idle()
    assert late_dispatch.skipped == 2
    assert late_dispatch.delivered == 1
    assert [event["asset_id"] for event in late_events] == [visible.asset_id]
    with pytest.raises(RuntimeError, match="not ready"):
        migration.promote_shadow_adoption(
            mismatched,
            repository=repository,
            config=config,
        )


def test_shadow_adoption_fails_closed_for_cross_source_period_conflict(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    business_dir = business / "2024/SSE"
    broker_dir = broker / "SSE/600000"
    business_dir.mkdir(parents=True)
    broker_dir.mkdir(parents=True)
    first = business_dir / f"600000_SH_2024Q4_cninfo_{_digest(PDF_A)}.pdf"
    second = broker_dir / "600000_2024-12-31_exchange.pdf"
    first.write_bytes(PDF_A)
    second.write_bytes(PDF_B)
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                first,
                source_file_id="cninfo",
                filing_id="cninfo",
                report_period="2024-12-31",
                source="cninfo",
            ),
            _manifest(
                second,
                source_file_id="exchange",
                filing_id="exchange",
                report_period="2024-12-31",
                content=PDF_B,
                source="sse",
            ),
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()

    adoption = migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path)
    reconciliation = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )

    assert adoption.periods[0].status == "ambiguous"
    assert repository.get_effective_report("600000.SH", 2024) is None
    assert reconciliation.ready_for_cutover is False
    assert reconciliation.periods[0].reason == "canonical_winner_missing"


def test_shadow_promotion_revalidates_bytes_and_persists_failure(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    report_path = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    report_path.write_bytes(PDF_A)
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                report_path,
                source_file_id="original",
                filing_id="original",
                report_period="2024-12-31",
            )
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path)
    reconciliation = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
        legacy_custody_evidence_by_path={
            report_path: _legacy_custody(report_path, config)
        },
    )
    gate_id = reconciliation.periods[0].promotion_gate_id
    assert reconciliation.ready_for_cutover is True
    assert gate_id is not None

    report_path.write_bytes(PDF_B)
    with pytest.raises(RuntimeError, match="promotion_integrity_failure"):
        migration.promote_shadow_adoption(
            reconciliation,
            repository=repository,
            config=config,
        )

    gate = repository.get_adoption_promotion_gate(gate_id)
    assert gate is not None and gate["status"] == "invalidated"
    shadow = repository.get_effective_report(
        "600000.SH", 2024, include_shadow=True
    )
    assert shadow is not None
    assert shadow.visibility_state == "shadow"
    assert shadow.availability is AssetAvailability.CORRUPT
    assert repository.get_effective_report("600000.SH", 2024) is None


def test_shadow_adoption_keeps_original_provisional_when_correction_is_corrupt(
    tmp_path,
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    original = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    correction = folder / f"600000_SH_2024Q4_correction_{_digest(PDF_B)}.pdf"
    original.write_bytes(PDF_A)
    correction.write_bytes(b"not a pdf")
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                original,
                source_file_id="original",
                filing_id="original",
                report_period="2024-12-31",
                published_at="2025-03-01T00:00:00+00:00",
            ),
            _manifest(
                correction,
                source_file_id="correction",
                filing_id="correction",
                report_period="2024-12-31",
                content=PDF_B,
                report_type="annual_report_correction",
                published_at="2025-04-01T00:00:00+00:00",
                supersedes_source_file_id="original",
            ),
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()

    adoption = migration.shadow_adopt(inventory, repository=repository)

    assert repository.get_effective_report("600000.SH", 2024) is None
    effective = repository.get_effective_report(
        "600000.SH", 2024, include_shadow=True
    )
    assert effective is not None
    assert effective.variant is AnnualReportVariant.ORIGINAL
    assert effective.decision_state is EffectiveDecisionState.PROVISIONAL
    assert adoption.periods[0].status == "provisional"
    assert adoption.files_deleted == 0


def test_convergence_dry_run_is_zero_side_effect_and_emits_rollback_manifest(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    first = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    second = folder / f"600000_SH_2024Q4_duplicate_{_digest(PDF_A)}.pdf"
    first.write_bytes(PDF_A)
    second.write_bytes(PDF_A)
    manifests = [
        _manifest(
            first,
            source_file_id="first",
            filing_id="original",
            report_period="2024-12-31",
        ),
        _manifest(
            second,
            source_file_id="second",
            filing_id="duplicate",
            report_period="2024-12-31",
            published_at="2026-03-21T01:00:00+00:00",
        ),
    ]
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=manifests,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path, backup=True)
    reconciliation = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )
    assert reconciliation.ready_for_cutover is False
    canonical = config.blob_root
    before = {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in (first, second)
    }
    report = migration.converge(
        inventory,
        repository=repository,
        config=config,
        canonical_root=canonical,
        dry_run=True,
        approved_paths=[first],
    )
    assert report.files_linked == report.files_moved == report.files_deleted == 0
    assert report.network_requests == 0
    assert report.rollback_manifest
    assert {
        path: (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in before
    } == before
    assert not canonical.exists()


def test_convergence_requires_all_gates_before_mutation(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    first = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    second = folder / f"600000_SH_2024Q4_duplicate_{_digest(PDF_A)}.pdf"
    first.write_bytes(PDF_A)
    second.write_bytes(PDF_A)
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                first,
                source_file_id="first",
                filing_id="original",
                report_period="2024-12-31",
            ),
            _manifest(
                second,
                source_file_id="second",
                filing_id="duplicate",
                report_period="2024-12-31",
                published_at="2026-03-21T01:00:00+00:00",
            ),
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path, backup=True)
    migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )
    canonical = config.blob_root
    capability = probe_nfs_capabilities(first, canonical, perform_probe=True)
    with pytest.raises(RuntimeError, match="plan fingerprint"):
        migration.converge(
            inventory,
            repository=repository,
            config=config,
            canonical_root=canonical,
            dry_run=False,
            approved_paths=[first],
            capability=capability,
            operator_authorized=True,
        )
    assert first.exists() and second.exists()


@pytest.mark.parametrize("use_hardlinks", [True, False])
def test_convergence_executes_exact_approved_plan_and_promotes_after_reconciliation(
    tmp_path, use_hardlinks, monkeypatch
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    report_path = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    report_path.write_bytes(PDF_A)
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                report_path,
                source_file_id="original",
                filing_id="original",
                report_period="2024-12-31",
            )
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration = AnnouncementArchiveInventory()
    migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path, backup=True)
    reconciliation = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )
    assert reconciliation.ready_for_cutover is False
    def fake_mount_identity(path):
        resolved = Path(path).resolve(strict=False)
        if config.backup.mount_root and resolved.is_relative_to(
            config.backup.mount_root.resolve()
        ):
            return MountIdentity(resolved, config.backup.mount_root, "backup-source", "tmpfs", 2)
        return MountIdentity(resolved, tmp_path, "primary-source", "tmpfs", 1)

    monkeypatch.setattr(
        "research.announcement_assets.migration.probe_mount_identity",
        fake_mount_identity,
    )
    _seed_backup(
        repository,
        config,
        report_path,
        destination_identity="backup-source|" + str(config.backup.mount_root) + "|2",
    )
    capability = probe_nfs_capabilities(
        report_path, config.blob_root, perform_probe=True
    )
    alias_expires_at = "2099-01-01T00:00:00+00:00"
    primary_failure_domain = "primary-domain"
    plan = migration.converge(
        inventory,
        repository=repository,
        config=config,
        dry_run=True,
        approved_paths=[report_path],
        primary_failure_domain=primary_failure_domain,
        alias_expires_at=alias_expires_at,
        use_hardlinks=use_hardlinks,
        capability=capability,
    )
    before = (report_path.stat().st_mtime_ns, report_path.stat().st_mode, report_path.read_bytes())
    recovered_after_catalog_failure = False
    if use_hardlinks:
        original_revalidate = migration_module._revalidate_convergence_mounts
        revalidation_calls = 0

        def fail_after_link(**kwargs):
            nonlocal revalidation_calls
            revalidation_calls += 1
            original_revalidate(**kwargs)
            if revalidation_calls == 4:
                raise migration_module.ConvergenceMountRaceError(
                    "convergence filesystem identity changed"
                )

        monkeypatch.setattr(
            migration_module,
            "_revalidate_convergence_mounts",
            fail_after_link,
        )
        with pytest.raises(RuntimeError, match="filesystem identity changed"):
            migration.converge(
                inventory,
                repository=repository,
                config=config,
                dry_run=False,
                approved_paths=[report_path],
                approved_plan_fingerprint=plan.plan_fingerprint,
                primary_failure_domain=primary_failure_domain,
                alias_expires_at=alias_expires_at,
                use_hardlinks=use_hardlinks,
                capability=capability,
                operator_authorized=True,
            )
        canonical_after_race = (
            config.blob_root
            / _digest(PDF_A)[:2]
            / f"{_digest(PDF_A)}.pdf"
        )
        assert not canonical_after_race.exists()
        assert not canonical_after_race.parent.exists()
        monkeypatch.setattr(
            migration_module,
            "_revalidate_convergence_mounts",
            original_revalidate,
        )
    if not use_hardlinks:
        original_finalize = repository.finalize_legacy_path_convergence

        def fail_finalization_once(**_kwargs):
            raise RuntimeError("injected_catalog_finalization_failure")

        monkeypatch.setattr(
            repository,
            "finalize_legacy_path_convergence",
            fail_finalization_once,
        )
        with pytest.raises(RuntimeError, match="injected_catalog_finalization_failure"):
            migration.converge(
                inventory,
                repository=repository,
                config=config,
                dry_run=False,
                approved_paths=[report_path],
                approved_plan_fingerprint=plan.plan_fingerprint,
                primary_failure_domain=primary_failure_domain,
                alias_expires_at=alias_expires_at,
                use_hardlinks=use_hardlinks,
                capability=capability,
                operator_authorized=True,
            )
        assert Path(repository.get_blob(_digest(PDF_A)).canonical_path) == report_path
        assert not repository.list_recovery_manifest_entries(
            manifest_kind="legacy_path_rollback"
        )
        monkeypatch.setattr(
            repository,
            "finalize_legacy_path_convergence",
            original_finalize,
        )
        recovered_after_catalog_failure = True
    result = migration.converge(
        inventory,
        repository=repository,
        config=config,
        dry_run=False,
        approved_paths=[report_path],
        approved_plan_fingerprint=plan.plan_fingerprint,
        primary_failure_domain=primary_failure_domain,
        alias_expires_at=alias_expires_at,
        use_hardlinks=use_hardlinks,
        capability=capability,
        operator_authorized=True,
    )
    assert result.files_deleted == 0
    assert result.files_linked + result.files_copied == (
        0 if recovered_after_catalog_failure else 1
    )
    assert (report_path.stat().st_mtime_ns, report_path.stat().st_mode, report_path.read_bytes()) == before
    canonical = config.blob_root / _digest(PDF_A)[:2] / f"{_digest(PDF_A)}.pdf"
    assert canonical.is_file()
    assert _digest(canonical.read_bytes()) == _digest(PDF_A)
    assert repository.get_legacy_path_manifest(str(report_path))["status"] == "canonicalized_awaiting_cleanup"
    assert repository.list_recovery_manifest_entries(manifest_kind="legacy_path_rollback")
    assert repository.list_recovery_manifest_entries(manifest_kind="legacy_path_rollback")[0].catalog_snapshot_watermark is None
    assert repository.list_recovery_manifest_entries(manifest_kind="legacy_path_rollback")[0].prior_path == str(report_path)
    assert repository.list_recovery_manifest_entries(manifest_kind="legacy_path_rollback")[0].content_hash == _digest(PDF_A)
    assert repository.get_effective_report("600000.SH", 2024) is None

    ready = migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )
    assert ready.ready_for_cutover is True
    promoted = migration.promote_shadow_adoption(
        ready,
        repository=repository,
        config=config,
    )
    assert promoted and promoted[0].visibility_state == "production"
    assert repository.list_recovery_manifest_entries(manifest_kind="legacy_path_rollback")


def test_convergence_excludes_adopted_older_period_from_latest_only_cleanup(tmp_path):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2023/SSE"
    folder.mkdir(parents=True)
    older = folder / f"600000_SH_2023Q4_original_{_digest(PDF_A)}.pdf"
    older.write_bytes(PDF_A)
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=[
            _manifest(
                older,
                source_file_id="older",
                filing_id="original",
                report_period="2023-12-31",
            )
        ],
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration = AnnouncementArchiveInventory()
    migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path, backup=True)
    migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )
    report = migration.converge(
        inventory,
        repository=repository,
        config=config,
        canonical_root=config.blob_root,
        dry_run=True,
        approved_paths=[older],
    )
    assert report.files_deleted == 0
    assert report.entries[0]["status"] == "approved"
    assert older.exists()


def test_convergence_execution_preserves_all_excluded_mixed_archive_files(
    tmp_path, monkeypatch
):
    business = tmp_path / "business"
    broker = tmp_path / "broker"
    folder = business / "2024/SSE"
    folder.mkdir(parents=True)
    approved = folder / f"600000_SH_2024Q4_original_{_digest(PDF_A)}.pdf"
    semiannual = folder / f"600000_SH_2024Q2_half-1_{_digest(PDF_A)}.pdf"
    old_period = folder / f"600000_SH_2023Q4_old-1_{_digest(PDF_A)}.pdf"
    orphan = folder / "orphan-report.pdf"
    conflict = folder / f"600001_SH_2026Q4_conflict_{_digest(PDF_A)}.pdf"
    approved.write_bytes(PDF_A)
    semiannual.write_bytes(PDF_A)
    old_period.write_bytes(PDF_A)
    orphan.write_bytes(PDF_A)
    conflict.write_bytes(PDF_A)
    derived = folder / "derived/pages.json"
    derived.parent.mkdir()
    derived.write_text("{}", encoding="utf-8")
    manifests = [
        _manifest(
            approved,
            source_file_id="approved",
            filing_id="original",
            report_period="2024-12-31",
        ),
        _manifest(
            semiannual,
            source_file_id="half-1",
            filing_id="half-1",
            report_period="2024-06-30",
            report_type="semiannual",
        ),
        _manifest(
            old_period,
            source_file_id="old-1",
            filing_id="old-1",
            report_period="2023-12-31",
        ),
        _manifest(
            conflict,
            source_file_id="conflict-a",
            filing_id="conflict-a",
            instrument_id="600001.SH",
            report_period="2026-12-31",
        ),
        _manifest(
            conflict,
            source_file_id="conflict-b",
            filing_id="conflict-b",
            instrument_id="600001.SH",
            report_period="2026-12-31",
        ),
    ]
    migration = AnnouncementArchiveInventory()
    inventory = migration.inventory(
        business_profile_root=business,
        broker_root=broker,
        manifest_rows=manifests,
        fiscal_year_allowlist=[2024],
    )
    excluded_paths = (semiannual, old_period, orphan, conflict, derived)
    before = {
        path: (path.stat().st_mtime_ns, path.stat().st_mode, path.read_bytes())
        for path in excluded_paths
    }
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    migration.shadow_adopt(inventory, repository=repository)
    config = _config(tmp_path, backup=True)
    migration.reconcile_shadow_adoption(
        inventory,
        repository=repository,
        config=config,
    )

    def fake_mount_identity(path):
        resolved = Path(path).resolve(strict=False)
        if config.backup.mount_root and resolved.is_relative_to(
            config.backup.mount_root.resolve()
        ):
            return MountIdentity(
                resolved,
                config.backup.mount_root,
                "backup-source",
                "tmpfs",
                2,
            )
        return MountIdentity(resolved, tmp_path, "primary-source", "tmpfs", 1)

    monkeypatch.setattr(
        "research.announcement_assets.migration.probe_mount_identity",
        fake_mount_identity,
    )
    _seed_backup(repository, config, approved, destination_identity="backup-source|" + str(config.backup.mount_root) + "|2")
    capability = probe_nfs_capabilities(
        approved, config.blob_root, perform_probe=True
    )
    plan = migration.converge(
        inventory,
        repository=repository,
        config=config,
        dry_run=True,
        approved_paths=[approved],
        primary_failure_domain="primary-domain",
        alias_expires_at="2099-01-01T00:00:00+00:00",
        capability=capability,
    )
    result = migration.converge(
        inventory,
        repository=repository,
        config=config,
        dry_run=False,
        approved_paths=[approved],
        approved_plan_fingerprint=plan.plan_fingerprint,
        primary_failure_domain="primary-domain",
        alias_expires_at="2099-01-01T00:00:00+00:00",
        capability=capability,
        operator_authorized=True,
    )

    assert result.files_linked + result.files_copied == 1
    assert all(
        entry["status"] == "excluded"
        for entry in result.entries
        if entry["legacy_path"] in {str(path.resolve()) for path in excluded_paths}
    )
    assert {
        path: (path.stat().st_mtime_ns, path.stat().st_mode, path.read_bytes())
        for path in excluded_paths
    } == before
