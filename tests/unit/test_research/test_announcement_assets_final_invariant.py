from __future__ import annotations

import hashlib

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
)
from research.announcement_assets.integrity import (
    AnnouncementAssetIntegrityAuditService,
)
from research.announcement_assets.readiness import AnnouncementAssetReadinessService
from research.announcement_assets.storage import ContentAddressedBlobStore
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)


PDF = b"%PDF-1.4\nfinal invariant\n%%EOF\n"


class _Retriever:
    def retrieve(self, source, attachment, *, require_pdf=False):
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=PDF,
            content_hash=hashlib.sha256(PDF).hexdigest(),
            content_length=len(PDF),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-03-31T02:00:00+00:00",
            signature_status="valid_pdf",
        )


def _bundle(tmp_path):
    config = AnnouncementAssetConfig.from_mapping(
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
                "max_attachment_bytes": 1024 * 1024,
                "unknown_length_reservation_bytes": 4096,
            },
        },
        project_root=tmp_path,
    )
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
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="600000-2025-annual",
        announcement_key=build_announcement_key(
            "cninfo", "600000-2025-annual"
        ),
        title="测试公司2025年年度报告",
        published_at="2026-03-31T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/600000-2025.pdf",
                attachment_id="600000-2025-annual",
                name="600000-2025-annual.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": "600000-2025-annual"},
    )
    registered = service.register_discovered_record(
        record,
        instrument_id="600000.SH",
    )
    effective = service.acquire_attachment(registered[0].attachment_id)
    assert effective is not None
    return config, repository, effective


def test_final_catalog_filesystem_invariant_accepts_one_verified_winner(tmp_path):
    config, repository, _ = _bundle(tmp_path)

    result = AnnouncementAssetIntegrityAuditService(
        repository=repository,
        config=config,
    ).run()

    assert result.status == "success"
    assert result.findings == ()
    assert result.valid_count == 1


def test_final_invariant_rejects_nonconsumer_path_and_same_hash_unlink(tmp_path):
    config, repository, effective = _bundle(tmp_path)
    digest = str(effective.content_hash)
    quarantine_path = config.quarantine_root / f"{digest}.manual.pdf"
    repository.update_blob_path(digest, str(quarantine_path))
    with repository.transaction() as conn:
        conn.execute(
            """INSERT INTO official_asset_deletion_intents(
                   deletion_id, schema_version, blob_hash, managed_path,
                   predecessor_asset_id, replacement_asset_id,
                   replacement_blob_hash, status, reason, planned_at, updated_at
               ) VALUES (?, 'official_asset_deletion_intent.v1', ?, ?, ?, ?, ?,
                         'planned', 'replacement', ?, ?)""",
            (
                "same-hash-unlink",
                digest,
                str(quarantine_path),
                effective.asset_id,
                effective.asset_id,
                digest,
                "2026-08-12T00:00:00+00:00",
                "2026-08-12T00:00:00+00:00",
            ),
        )

    result = AnnouncementAssetIntegrityAuditService(
        repository=repository,
        config=config,
    ).run()
    statuses = {finding.status for finding in result.findings}

    assert "nonconsumer_storage_object_is_effective" in statuses
    assert "same_hash_physical_deletion_intent" in statuses
    readiness = AnnouncementAssetReadinessService(
        repository=repository,
        config=config,
    ).report(now="2026-08-12T00:01:00+00:00")
    assert readiness.summary["unique_storage_completion_allowed"] is False
