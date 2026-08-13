from __future__ import annotations

import ast
import hashlib
import importlib.util
import sys
from pathlib import Path

from api.announcement_asset_models import AnnualReportAssetResponse
from research.announcement_assets import (
    CLASSIFICATION_VOCABULARY_VERSION,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnualReportClassifier,
    AnnualReportVariant,
    ArchiveInventoryItem,
    ConsumerProcessingStatus,
    ContentAddressedBlobStore,
    DocumentFamily,
    IntegrityStatus,
    OfficialAttachmentVersion,
    OfficialDocumentBlob,
    normalize_annual_report_variant,
    normalize_document_family,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)
from research.announcements.categories import (
    ANNUAL_REPORT_CATEGORY,
    SEMIANNUAL_REPORT_CATEGORY,
    normalize_announcement_category,
)

PDF_BYTES = b"%PDF-1.4\nneutral semiannual fixture\n%%EOF\n"
FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "neutral_announcement_asset_consumer.py"
)
CHANGE_ROOT = (
    Path(__file__).resolve().parents[3]
    / "openspec"
    / "changes"
    / "establish-shared-announcement-asset-management"
)


def _load_neutral_consumer_module():
    spec = importlib.util.spec_from_file_location(
        "neutral_announcement_asset_consumer", FIXTURE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _scenario_contract(text: str, heading: str) -> str:
    marker = f"#### Scenario: {heading}"
    start = text.index(marker)
    end = text.find("\n#### Scenario:", start + len(marker))
    if end < 0:
        end = text.find("\n### Requirement:", start + len(marker))
    return text[start : len(text) if end < 0 else end]


def test_asset_and_api_specs_share_detach_only_cancellation_contract():
    asset_spec = (
        CHANGE_ROOT / "specs" / "official-announcement-assets" / "spec.md"
    ).read_text(encoding="utf-8")
    api_spec = (
        CHANGE_ROOT / "specs" / "research-data-engine" / "spec.md"
    ).read_text(encoding="utf-8")
    shared = _scenario_contract(asset_spec, "One subscriber cancels shared acquisition")
    last = _scenario_contract(asset_spec, "The last asset subscriber cancels")
    api_shared = _scenario_contract(
        api_spec,
        "Client cancels one shared acquisition request",
    )
    api_last = _scenario_contract(
        api_spec,
        "Client cancels the last shared acquisition request",
    )
    consumer = _scenario_contract(api_spec, "Client cancels a consumer request")

    assert "only the cancelling principal's subscription" in shared
    assert "SHALL NOT mutate the linked `consumer_request_id`" in shared
    assert "internal acquisition SHALL continue" in last
    assert "SHALL NOT stop consumer processing" in last
    assert "only that request subscription SHALL become cancelled" in api_shared
    assert "linked `consumer_request_id`" in api_shared
    assert "repeated DELETE SHALL return the same outcome" in api_shared
    assert "internal acquisition continues to a bounded terminal state" in api_last
    assert "consumer domain's authorized cooperative-stop contract" in consumer
    assert "deleting a consumer request SHALL NOT detach" in consumer


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
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
            "acquisition": {"normalized_categories": [ANNUAL_REPORT_CATEGORY]},
        },
        project_root=tmp_path,
    )


def _semiannual_record() -> AnnouncementRecord:
    source_id = "semiannual-600000-2025"
    attachment = AnnouncementAttachment(
        source_url=f"https://static.example/{source_id}.pdf",
        attachment_id=source_id,
        name="测试公司2025年半年度报告.pdf",
        media_type="application/pdf",
        raw_metadata={
            "asset_classification": {
                "document_family": DocumentFamily.SEMIANNUAL_REPORT.value,
                "variant": AnnualReportVariant.ORIGINAL.value,
                "is_full_report": True,
                "is_eligible": False,
                "vocabulary_version": CLASSIFICATION_VOCABULARY_VERSION,
                "policy_version": "semiannual-test-only.v1",
            }
        },
    )
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title="测试公司2025年半年度报告",
        published_at="2026-08-10T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(attachment,),
        raw_payload={
            "announcementId": source_id,
            "category": SEMIANNUAL_REPORT_CATEGORY,
        },
    )


def test_versioned_classification_vocabulary_is_shared_across_boundaries():
    assert normalize_announcement_category("annual_report_correction") == (
        ANNUAL_REPORT_CATEGORY
    )
    assert normalize_announcement_category("semiannual") == SEMIANNUAL_REPORT_CATEGORY
    assert normalize_document_family("annual_report_correction") == (
        DocumentFamily.ANNUAL_REPORT.value
    )
    assert normalize_document_family("semiannual") == (
        DocumentFamily.SEMIANNUAL_REPORT.value
    )
    assert normalize_annual_report_variant("annual_report_correction") is (
        AnnualReportVariant.CORRECTION
    )

    migrated = ArchiveInventoryItem(
        path="data/filings/business_profile/600000/2025/report.pdf",
        consumer="business_profile",
        status="adoptable",
        reason="fixture",
        report_type="annual_report_correction",
        manifest={
            "asset_classification": {
                "document_family": ANNUAL_REPORT_CATEGORY,
                "variant": AnnualReportVariant.CORRECTION.value,
                "is_full_report": True,
                "vocabulary_version": CLASSIFICATION_VOCABULARY_VERSION,
            }
        },
    )
    assert migrated.document_family == DocumentFamily.ANNUAL_REPORT.value
    assert migrated.variant is AnnualReportVariant.CORRECTION
    assert migrated.is_full_report is True

    api_projection = AnnualReportAssetResponse(
        asset_id="asset-1",
        instrument_id="600000.SH",
        fiscal_year=2025,
        report_period="2025-12-31",
        source="cninfo",
        source_announcement_id="filing-1",
        filing_id="filing-1",
        attachment_id="attachment-1",
        document_family=DocumentFamily.ANNUAL_REPORT.value,
        variant=AnnualReportVariant.CORRECTION.value,
        is_correction=True,
        classification_vocabulary_version=CLASSIFICATION_VOCABULARY_VERSION,
        asset_availability="local_valid",
        availability="local_valid",
        exact_content_state="local_valid",
        effective_decision_state="current",
        last_checked_at="2026-08-10T01:00:00+00:00",
        canonical_source_filing={
            "source": "cninfo",
            "source_announcement_id": "filing-1",
            "attachment_id": "attachment-1",
        },
    )
    assert api_projection.document_family == ANNUAL_REPORT_CATEGORY
    assert api_projection.classification_vocabulary_version == (
        CLASSIFICATION_VOCABULARY_VERSION
    )


def test_notice_only_correction_is_evidence_but_never_a_full_report():
    source_id = "notice-only"
    attachment = AnnouncementAttachment(
        source_url="https://static.example/notice-only.pdf",
        attachment_id=source_id,
        name="更正公告.pdf",
        media_type="application/pdf",
    )
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title="测试公司2025年年度报告更正公告",
        published_at="2026-08-10T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(attachment,),
        raw_payload={"announcementId": source_id},
    )

    classification = AnnualReportClassifier().classify(record, attachment)

    assert classification.document_family == DocumentFamily.ANNUAL_REPORT.value
    assert classification.variant is AnnualReportVariant.CORRECTION
    assert classification.correction_evidence is True
    assert classification.is_full_report is False
    assert classification.is_eligible is False


def test_generic_boundaries_accept_non_annual_asset_and_neutral_consumer(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    record = _semiannual_record()

    announcement = repository.upsert_announcement(
        record,
        instrument_id="600000.SH",
        observed_at="2026-08-10T01:00:00+00:00",
    )
    attachment = repository.upsert_attachment(
        announcement.announcement_id,
        record.attachments[0],
        observed_at="2026-08-10T01:00:00+00:00",
    )
    published = store.publish_bytes(PDF_BYTES)
    repository.register_blob(
        OfficialDocumentBlob(
            content_hash=published.content_hash,
            content_length=published.content_length,
            canonical_path=str(published.path),
            signature_status="valid_pdf",
            integrity_status=IntegrityStatus.VALID,
            first_available_at="2026-08-10T01:00:01+00:00",
            last_verified_at="2026-08-10T01:00:01+00:00",
            acquisition_origin="architecture_contract",
        )
    )
    version = repository.add_attachment_version(
        OfficialAttachmentVersion(
            version_id="semiannual-test-version-1",
            attachment_id=attachment.attachment_id,
            observation_key="semiannual-test-observation-1",
            content_hash=published.content_hash,
            final_url=record.attachments[0].source_url,
            retrieval_status="success",
            integrity_status=IntegrityStatus.VALID,
            attempt=1,
            next_retry_at=None,
            error_code=None,
            observed_at="2026-08-10T01:00:01+00:00",
            metadata={
                "asset_classification": record.attachments[0].raw_metadata[
                    "asset_classification"
                ]
            },
        )
    )
    asset_id = f"generic:{attachment.attachment_id}:{version.version_id}"

    neutral = _load_neutral_consumer_module()
    consumer = neutral.NeutralAnnouncementAssetConsumer()
    reference = neutral.SharedAssetReference(
        asset_id=asset_id,
        document_family=DocumentFamily.SEMIANNUAL_REPORT.value,
        variant=AnnualReportVariant.ORIGINAL.value,
        content_hash=published.content_hash,
    )
    result = consumer.process(
        reference,
        read_content=lambda content_hash: Path(
            repository.get_blob(content_hash).canonical_path
        ).read_bytes(),
    )
    processing_id = repository.upsert_consumer_processing(
        asset_id=asset_id,
        consumer=consumer.consumer_id,
        parser_version=consumer.parser_version,
        parameter_hash=hashlib.sha256(b"neutral-test-parameters").hexdigest(),
        status=ConsumerProcessingStatus.CURRENT,
        derived_identity=f"neutral:{published.content_hash}",
        metadata={
            "document_family": result["document_family"],
            "classification_vocabulary_version": CLASSIFICATION_VOCABULARY_VERSION,
        },
    )

    assert announcement.source_category == SEMIANNUAL_REPORT_CATEGORY
    assert repository.get_latest_valid_attachment_version(attachment.attachment_id) == (
        version
    )
    assert result["content_length"] == len(PDF_BYTES)
    rows = repository.list_consumer_processing(consumer=consumer.consumer_id)
    assert rows[0]["processing_id"] == processing_id
    assert rows[0]["asset_id"] == asset_id
    assert config.acquisition.normalized_categories == (ANNUAL_REPORT_CATEGORY,)
    annual_policy = AnnualReportClassifier().classify(record, record.attachments[0])
    assert annual_policy.is_eligible is False


def test_neutral_consumer_has_no_private_downloader_or_archive_dependencies():
    tree = ast.parse(FIXTURE_PATH.read_text(encoding="utf-8"))
    imported_modules = {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    imported_modules.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    forbidden_prefixes = (
        "research.announcements",
        "research.announcement_assets.storage",
        "research.announcement_assets.migration",
        "research.business_profile",
        "research.broker_risk_control",
    )

    assert not any(
        module.startswith(forbidden_prefixes) for module in imported_modules
    )
    source = FIXTURE_PATH.read_text(encoding="utf-8")
    assert "download" not in source.lower()
    assert "archive" not in source.lower()
    assert "revision" not in source.lower()
