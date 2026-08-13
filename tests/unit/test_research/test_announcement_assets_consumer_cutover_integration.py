from __future__ import annotations

import hashlib
from dataclasses import asdict
from pathlib import Path

from research.announcement_assets import (
    AnnouncementAssetAccess,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    ContentAddressedBlobStore,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)
from research.broker_risk_control import (
    BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
    BrokerRiskControlReportSyncService,
    BrokerRiskControlSyncResult,
)
from research.business_profile_archive import (
    BusinessProfileDocumentArchiveService,
)
from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import classify_business_profile_document

PDF_BYTES = b"%PDF-1.4\nshared consumer cutover annual report\n%%EOF\n"


class _Retriever:
    def __init__(self) -> None:
        self.calls = 0

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls += 1
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=PDF_BYTES,
            content_hash=hashlib.sha256(PDF_BYTES).hexdigest(),
            content_length=len(PDF_BYTES),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-03-30T02:00:00+00:00",
            signature_status="valid_pdf",
        )


class _ManifestStorage:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def get_financial_source_file_manifests(self, **filters):
        return [
            row
            for row in self.rows
            if all(row.get(key) == value for key, value in filters.items() if value)
        ]

    def upsert_financial_source_file_manifest(self, manifest, *, ingestion_run_id=None):
        row = asdict(manifest)
        row["metadata"] = row.pop("metadata_json")
        row["ingestion_run_id"] = ingestion_run_id
        self.rows.append(row)
        return row["source_file_id"]


class _NoProvider:
    def acquire(self, *args, **kwargs):
        raise AssertionError("consumer cutover must not scan an announcement provider")


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
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
                "max_attachment_bytes": 1024 * 1024,
                "unknown_length_reservation_bytes": 4096,
            },
        },
        project_root=tmp_path,
    )


def _record(title: str = "中信证券2025年年度报告") -> AnnouncementRecord:
    source_id = "600030-2025-annual"
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=title,
        published_at="2026-03-30T01:00:00+00:00",
        exchange="SSE",
        symbols=("600030",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/600030-2025-annual.pdf",
                attachment_id=source_id,
                name="中信证券2025年年度报告.pdf",
                media_type="application/pdf",
            ),
        ),
    )


def _business_profile_candidate() -> BusinessProfileDocumentCandidate:
    title = "中信证券2025年年度报告"
    return BusinessProfileDocumentCandidate(
        announcement_id="600030-2025-annual",
        title=title,
        announcement_time="2026-03-30T09:00:00+08:00",
        symbols=["600030"],
        adjunct_url="https://static.example/600030-2025-annual.pdf",
        adjunct_type="PDF",
        classification=classify_business_profile_document(title, adjunct_type="PDF"),
        selection_reasons=["business_profile_document:annual_report"],
    )


def test_two_consumers_reuse_one_real_shared_pdf_without_network_or_archive_copy(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "catalog.db")
    repository.initialize_schema()
    blob_store = ContentAddressedBlobStore(config)
    blob_store.prepare()
    retriever = _Retriever()
    shared_service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=blob_store,
        attachment_retriever=retriever,
    )
    attachment = shared_service.register_discovered_record(
        _record(), instrument_id="600030.SH"
    )[0]
    asset = shared_service.acquire_attachment(attachment.attachment_id)
    assert asset is not None
    assert retriever.calls == 1
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=shared_service,
    )

    manifests = _ManifestStorage()
    business_profile = BusinessProfileDocumentArchiveService(
        storage=manifests,
        archive_root=tmp_path / "legacy-business-profile",
        shared_asset_access=access,
        downloader=lambda candidate: (_ for _ in ()).throw(
            AssertionError("business-profile legacy downloader must not run")
        ),
        annual_report_asset_mode="shared_only",
    )
    business_result = business_profile.archive_candidates(
        {"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE"},
        [_business_profile_candidate()],
    )

    broker = BrokerRiskControlReportSyncService(
        storage=object(),
        announcement_service=_NoProvider(),
        payload_fetcher=lambda record: (_ for _ in ()).throw(
            AssertionError("broker legacy downloader must not run")
        ),
        source_profile=BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        shared_asset_access=access,
        annual_report_asset_mode="shared_only",
        legacy_semiannual_enabled=False,
    )
    broker_shared = broker._shared_annual_report_asset(
        _record(), {"instrument_id": "600030.SH"}
    )

    assert broker_shared is not None
    broker_payload, broker_lineage, broker_content = broker_shared
    assert broker_payload == PDF_BYTES
    assert business_result.archived == 1
    assert business_result.records[0].content_hash == asset.content_hash
    assert Path(business_result.records[0].archive_path) == broker_content["path"]
    assert broker_lineage["asset_id"] == asset.asset_id
    assert broker_lineage["content_hash"] == asset.content_hash
    assert broker_lineage["observation_version"] == manifests.rows[0]["metadata"][
        "shared_asset_observation_version"
    ]
    assert manifests.rows[0]["source_mode"] == "shared_announcement_asset"
    assert retriever.calls == 1
    assert len(list(config.blob_root.rglob("*.pdf"))) == 1
    assert not (tmp_path / "legacy-business-profile").exists()
    assert broker_content["file_handle"].closed

    semiannual_result = BrokerRiskControlSyncResult(status="success", mode="test")
    broker._process_record(
        _record("中信证券2026年半年度报告"),
        {"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE"},
        semiannual_result,
        ingestion_run_id=None,
        tier="hot",
        dry_run=False,
    )
    assert semiannual_result.retryable_pending_reports == 1
    assert semiannual_result.errors == ["legacy broker semiannual acquisition is disabled"]
    assert retriever.calls == 1
