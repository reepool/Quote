from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from research.announcement_assets.access import (
    AnnouncementAssetAccess,
    AssetContentGoneError,
    AssetContentIntegrityError,
    AssetContentMountError,
)
from research.announcement_assets.config import AnnouncementAssetConfig
from research.announcement_assets.models import (
    AssetAvailability,
    EnsureDisposition,
    EnsureRequest,
    IntegrityStatus,
    OperationStatus,
)
from research.announcement_assets.repository import AnnouncementAssetRepository
from research.announcement_assets.service import AnnouncementAssetService
from research.announcement_assets.storage import ContentAddressedBlobStore
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    AnnouncementRouteConfig,
    AnnouncementScanResult,
    build_announcement_key,
)

PDF_BYTES = b"%PDF-1.4\naccess annual report\n%%EOF\n"


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


class _Retriever:
    def __init__(self):
        self.calls = 0
        self.attachment_ids: list[str] = []

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls += 1
        self.attachment_ids.append(attachment.attachment_id)
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=PDF_BYTES,
            content_hash=hashlib.sha256(PDF_BYTES).hexdigest(),
            content_length=len(PDF_BYTES),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-03-20T02:00:00+00:00",
            signature_status="valid_pdf",
        )


class _SelectiveRetriever(_Retriever):
    def __init__(self, *, failing_ids=(), payloads=None):
        super().__init__()
        self.failing_ids = set(failing_ids)
        self.payloads = dict(payloads or {})

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls += 1
        self.attachment_ids.append(attachment.attachment_id)
        if attachment.attachment_id in self.failing_ids:
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="failed",
                content=None,
                content_hash=None,
                content_length=None,
                final_url=attachment.source_url,
                response_media_type="application/pdf",
                retrieved_at="2026-03-20T02:00:00+00:00",
                signature_status="unknown",
                error_code="test_failure",
            )
        payload = self.payloads.get(attachment.attachment_id, PDF_BYTES)
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=payload,
            content_hash=hashlib.sha256(payload).hexdigest(),
            content_length=len(payload),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-03-20T02:00:00+00:00",
            signature_status="valid_pdf",
        )


class _SlowRetriever(_Retriever):
    def retrieve(self, source, attachment, *, require_pdf=False):
        time.sleep(0.2)
        return super().retrieve(source, attachment, require_pdf=require_pdf)


@dataclass
class _DiscoveryProvider:
    records: tuple[AnnouncementRecord, ...]
    source_name: str = "cninfo"
    calls: int = 0
    queries: list[AnnouncementQuery] | None = None
    complete: bool = True

    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE"}),
        supports_instrument_scope=True,
        supports_date_filter=True,
        supports_category_filter=True,
    )

    def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
        self.calls += 1
        if self.queries is None:
            self.queries = []
        self.queries.append(query)
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status="success" if self.complete else "partial",
            records=self.records,
            is_complete=self.complete,
            pages_scanned=1,
            requests_made=1,
            announcements_seen=len(self.records),
            stop_reason="completed" if self.complete else "provider_scope_incomplete",
        )


def _record(
    *,
    source_id: str = "600000-2025-annual",
    title: str = "测试公司2025年年度报告",
    published_at: str = "2026-03-20T01:00:00+00:00",
) -> AnnouncementRecord:
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title=title,
        published_at=published_at,
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url=f"https://static.example/{source_id}.pdf",
                attachment_id=source_id,
                name=f"{source_id}.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": source_id},
    )


def test_access_facade_returns_safe_local_projection_and_controlled_content(tmp_path):
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
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    asset = service.acquire_attachment(registered[0].attachment_id)
    assert asset is not None
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    listed = access.list_assets(instrument_id="600000.SH", fiscal_year=2025)
    assert listed["returned"] == 1
    assert "path" not in listed["items"][0]
    effective = access.list_effective_assets(
        instrument_id="600000.SH",
        availability="local_valid",
    )
    assert effective["returned"] == 1
    assert effective["items"][0]["asset_id"] == asset.asset_id
    assert access.get_asset(asset.asset_id)["content_hash"] == asset.content_hash
    ensured = access.ensure(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
        )
    )
    assert ensured["disposition"] == "local_hit"
    assert ensured["asset_request_id"] is None
    content = access.content_handle(asset.asset_id)
    assert content["content_length"] == len(PDF_BYTES)
    assert content["path"].read_bytes() == PDF_BYTES
    handle = content["file_handle"]
    lease = repository.get_read_lease(content["read_lease_id"])
    assert lease is not None and lease["released_at"] is None
    assert lease["metadata"]["scope"] == "public_current"
    assert handle.lease_generation == 1
    assert handle.heartbeat()
    assert handle.lease_generation == 2
    assert (
        repository.heartbeat_read_lease(
            handle.lease_id,
            owner=lease["owner"],
            expected_generation=1,
            ttl_seconds=30,
        )
        is None
    )
    assert handle.read() == PDF_BYTES
    handle.close()
    assert repository.get_read_lease(handle.lease_id) is None

    audited = access.content_handle(asset.asset_id, audit_access=True)
    audited_lease_id = audited["read_lease_id"]
    audited["file_handle"].close()
    audited_lease = repository.get_read_lease(audited_lease_id)
    assert audited_lease is not None
    assert audited_lease["released_at"] is not None
    assert audited_lease["metadata"]["audit_access"] is True
    repository.initialize_schema()
    assert repository.get_read_lease(audited_lease_id) is not None


def test_asset_listing_includes_current_superseded_and_metadata_only_records(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    original_bytes = b"%PDF-1.4\noriginal annual report\n%%EOF\n"
    correction_bytes = b"%PDF-1.4\ncorrected annual report\n%%EOF\n"
    retriever = _SelectiveRetriever(
        payloads={
            "600000-2025-original": original_bytes,
            "600000-2025-correction": correction_bytes,
        }
    )
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    original = service.register_discovered_record(
        _record(
            source_id="600000-2025-original",
            published_at="2026-03-20T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )[0]
    service.acquire_attachment(original.attachment_id)
    correction = service.register_discovered_record(
        _record(
            source_id="600000-2025-correction",
            title="测试公司2025年年度报告（修订版）",
            published_at="2026-03-21T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )[0]
    service.acquire_attachment(correction.attachment_id)
    service.register_discovered_record(
        _record(
            source_id="600001-2025-metadata",
            published_at="2026-03-20T01:00:00+00:00",
        ),
        instrument_id="600001.SH",
    )
    service.register_discovered_record(
        _record(
            source_id="600002-2025-withdrawn",
            published_at="2026-03-19T01:00:00+00:00",
        ),
        instrument_id="600002.SH",
    )
    with repository.transaction() as conn:
        conn.execute(
            """UPDATE official_announcements SET status='withdrawn'
               WHERE source='cninfo' AND source_announcement_id=?""",
            ("600002-2025-withdrawn",),
        )
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    rows = access.list_assets(instrument_id="600000.SH")["items"]
    assert [row["effective_state"] for row in rows] == [
        "current",
        "superseded",
    ]
    assert rows[0]["asset_availability"] == "local_valid"
    assert rows[0]["content_url"] == (
        f"/api/v1/research/annual-report-assets/{rows[0]['asset_id']}/content"
    )
    assert rows[1]["asset_availability"] == "superseded"
    assert rows[1]["content_url"] is None
    assert rows[1]["effective_decision_state"] is None
    assert rows[1]["exact_content_state"] == "retained_internal_only"

    metadata = access.list_assets(
        source_announcement_id="600001-2025-metadata",
        integrity="unchecked",
        acquisition_status="metadata_only",
        effective_state="historical",
        asset_availability="metadata_only",
    )
    assert metadata["returned"] == 1
    assert metadata["items"][0]["asset_id"] is None
    assert metadata["items"][0]["content_url"] is None
    assert metadata["items"][0]["observation_version"] is None
    assert metadata["items"][0]["exact_content_state"] == ("local_content_unavailable")
    withdrawn = access.list_assets(
        source_announcement_id="600002-2025-withdrawn",
        effective_state="withdrawn",
    )
    assert withdrawn["returned"] == 1
    assert withdrawn["items"][0]["effective_state"] == "withdrawn"
    assert withdrawn["items"][0]["effective_decision_state"] == "withdrawn"
    assert retriever.calls == 2


def test_cutoff_effective_read_reconstructs_original_before_correction(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    original_bytes = b"%PDF-1.4\noriginal cutoff report\n%%EOF\n"
    correction_bytes = b"%PDF-1.4\ncorrected cutoff report\n%%EOF\n"
    retriever = _SelectiveRetriever(
        payloads={
            "600000-2025-original": original_bytes,
            "600000-2025-correction": correction_bytes,
        }
    )
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    original = service.register_discovered_record(
        _record(
            source_id="600000-2025-original",
            published_at="2026-03-20T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )[0]
    original_asset = service.acquire_attachment(original.attachment_id)
    correction = service.register_discovered_record(
        _record(
            source_id="600000-2025-correction",
            title="测试公司2025年年度报告（修订版）",
            published_at="2026-04-20T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )[0]
    correction_asset = service.acquire_attachment(correction.attachment_id)
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    historical = access.get_effective_asset(
        "600000.SH",
        fiscal_year=2025,
        knowledge_cutoff="2026-04-01",
    )
    current = access.get_effective_asset(
        "600000.SH",
        fiscal_year=2025,
    )

    assert original_asset is not None and correction_asset is not None
    assert historical is not None
    assert historical["asset_id"] == original_asset.asset_id
    assert historical["content_hash"] == hashlib.sha256(original_bytes).hexdigest()
    assert historical["content_url"] is None
    assert current is not None
    assert current["asset_id"] == correction_asset.asset_id
    assert current["content_hash"] == hashlib.sha256(correction_bytes).hexdigest()


def test_shared_access_classifies_and_selects_semiannual_report(tmp_path):
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
    annual = service.register_discovered_record(
        _record(
            source_id="600000-2025-annual",
            title="测试公司2025年年度报告",
            published_at="2026-03-20T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    annual_asset = service.acquire_attachment(annual[0].attachment_id)
    registered = service.register_discovered_record(
        _record(
            source_id="600000-2025-semiannual",
            title="测试公司2025年半年度报告",
            published_at="2025-08-20T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    assert registered[0].classification.document_family == "semiannual_report"
    assert registered[0].classification.report_period == "2025-06-30"
    service.acquire_attachment(registered[0].attachment_id)
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    projection = access.list_effective_assets(
        instrument_id="600000.SH",
        document_family="semiannual_report",
        availability="local_valid",
    )

    assert projection["returned"] == 1
    assert projection["items"][0]["document_family"] == "semiannual_report"
    assert projection["items"][0]["report_period"] == "2025-06-30"
    current_annual = access.get_effective_asset("600000.SH", fiscal_year=2025)
    assert annual_asset is not None and current_annual is not None
    assert current_annual["asset_id"] == annual_asset.asset_id
    assert current_annual["document_family"] == "annual_report"


@pytest.mark.parametrize(
    "availability",
    ["metadata_only", "missing", "ambiguous", "corrupt", "superseded", "blocked"],
)
def test_non_local_valid_asset_projections_never_expose_content_url(availability):
    projection = AnnouncementAssetAccess._asset_record_projection(
        {
            "asset_id": "asset-not-public",
            "instrument_id": "600000.SH",
            "fiscal_year": 2025,
            "report_period": "2025-12-31",
            "source": "cninfo",
            "source_announcement_id": "filing-2025",
            "attachment_id": "attachment-2025",
            "observation_version": "observation-1",
            "version_available_at": "2026-03-20T01:00:00+00:00",
            "published_at": "2026-03-20T01:00:00+00:00",
            "variant": "original",
            "content_hash": "a" * 64,
            "content_length": len(PDF_BYTES),
            "integrity": "valid",
            "asset_availability": availability,
            "acquisition_status": "success",
            "effective_state": "historical",
        }
    )

    assert projection["content_url"] is None
    assert projection["exact_content_state"] == "retained_internal_only"


def test_asset_listing_pagination_is_stable_for_tied_publication_times(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    service = AnnouncementAssetService(repository=repository, config=config)
    for source_id in (
        "600000-2025-c",
        "600000-2025-a",
        "600000-2025-b",
    ):
        service.register_discovered_record(
            _record(
                source_id=source_id,
                published_at="2026-03-20T01:00:00+00:00",
            ),
            instrument_id="600000.SH",
        )
    access = AnnouncementAssetAccess(
        repository=repository, config=config, service=service
    )

    first = access.list_assets(instrument_id="600000.SH", limit=2, offset=0)
    second = access.list_assets(instrument_id="600000.SH", limit=2, offset=2)
    repeated = access.list_assets(instrument_id="600000.SH", limit=2, offset=0)

    first_ids = [item["source_announcement_id"] for item in first["items"]]
    second_ids = [item["source_announcement_id"] for item in second["items"]]
    assert first_ids == ["600000-2025-a", "600000-2025-b"]
    assert second_ids == ["600000-2025-c"]
    assert [item["source_announcement_id"] for item in repeated["items"]] == (first_ids)


@pytest.mark.parametrize(
    ("mutation", "expected_integrity"),
    (("overwrite", "size_mismatch"), ("delete", "missing")),
)
def test_content_handle_persists_external_mutation_as_corrupt(
    tmp_path, mutation, expected_integrity
):
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
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    asset = service.acquire_attachment(registered[0].attachment_id)
    assert asset is not None and asset.content_hash
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )
    blob = repository.get_blob(asset.content_hash)
    assert blob is not None
    path = Path(blob.canonical_path)
    if mutation == "overwrite":
        path.write_bytes(b"%PDF-1.4\nexternally changed\n%%EOF\n")
    else:
        path.unlink()

    with pytest.raises(AssetContentIntegrityError, match="integrity failed"):
        access.content_handle(asset.asset_id)

    persisted = repository.get_effective_report_by_asset_id(asset.asset_id)
    assert persisted is not None
    assert persisted.availability is AssetAvailability.CORRUPT
    assert (
        repository.get_blob(asset.content_hash).integrity_status.value
        == expected_integrity
    )
    with repository.connection() as conn:
        active_readers = conn.execute(
            """SELECT COUNT(*) FROM official_asset_retention_pins
               WHERE pin_type='active_reader' AND released_at IS NULL"""
        ).fetchone()[0]
    assert active_readers == 0


def test_content_handle_rejects_mount_identity_change_and_releases_lease(
    tmp_path, monkeypatch
):
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
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    asset = service.acquire_attachment(registered[0].attachment_id)
    assert asset is not None
    approved = store.validate_mount()
    changed = replace(approved, source=approved.source + "-changed")
    monkeypatch.setattr(
        "research.announcement_assets.access.probe_mount_identity",
        lambda path: changed,
    )
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    with pytest.raises(AssetContentMountError, match="approved filings mount"):
        access.content_handle(asset.asset_id)

    with repository.connection() as conn:
        active_readers = conn.execute(
            """SELECT COUNT(*) FROM official_asset_retention_pins
               WHERE pin_type='active_reader' AND released_at IS NULL"""
        ).fetchone()[0]
    assert active_readers == 0


def test_content_handle_invalidates_every_effective_asset_sharing_corrupt_bytes(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    retriever = _Retriever()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    assets = []
    for instrument_id, source_id in (
        ("600000.SH", "600000-2025-shared"),
        ("600001.SH", "600001-2025-shared"),
    ):
        registered = service.register_discovered_record(
            _record(source_id=source_id),
            instrument_id=instrument_id,
        )
        assets.append(service.acquire_attachment(registered[0].attachment_id))
    assert all(asset is not None for asset in assets)
    assert assets[0].content_hash == assets[1].content_hash
    blob = repository.get_blob(assets[0].content_hash)
    assert blob is not None
    Path(blob.canonical_path).write_bytes(b"%PDF-1.4\ncorrupt shared bytes")
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    with pytest.raises(RuntimeError, match="integrity failed"):
        access.content_handle(assets[0].asset_id)

    assert (
        repository.get_effective_report("600000.SH", 2025).availability
        is AssetAvailability.CORRUPT
    )
    assert (
        repository.get_effective_report("600001.SH", 2025).availability
        is AssetAvailability.CORRUPT
    )


def test_access_facade_exposes_only_principal_request_handle(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    access = AnnouncementAssetAccess(repository=repository, config=config)

    created = access.ensure(
        EnsureRequest(
            instrument_id="600001.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
            consumer="business-profile",
            idempotency_key="alice-request",
        )
    )
    assert created["disposition"] == "operation_created"
    assert created["asset_request_id"]
    assert "operation_id" not in created["request"]
    assert (
        access.get_asset_request(created["asset_request_id"], principal="bob") is None
    )


def test_request_projection_redacts_unapproved_operation_details(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    subscription, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="redacted-operation",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="v1",
        principal="alice",
        request_idempotency_key="redacted-request",
        request_fingerprint="redacted-fingerprint",
        authorized_projection={
            "allowed_progress_fields": ["candidate_count"],
            "allowed_diagnostics_fields": ["error_type"],
        },
    )
    repository.transition_operation(
        operation.operation_id,
        OperationStatus.BLOCKED,
        progress={"candidate_count": 2, "internal_path": "/secret/file.pdf"},
        diagnostics={"error_type": "RuntimeError", "provider_cookie": "secret"},
    )
    projection = AnnouncementAssetAccess(
        repository=repository,
        config=config,
    ).get_asset_request(subscription.asset_request_id, principal="alice")

    assert projection is not None
    assert "operation_id" not in projection
    assert projection["progress"] == {"candidate_count": 2}
    assert projection["diagnostics"] == {"error_type": "RuntimeError"}
    assert projection["expires_at"]
    assert projection["retention_policy_version"] == "asset_request_retention.v1"


def test_positive_wait_completes_the_same_durable_request_when_work_finishes(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    retriever = _Retriever()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    service.register_discovered_record(_record(), instrument_id="600000.SH")

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="wait-for-local",
        )
    )

    assert result.disposition is EnsureDisposition.LOCAL_HIT
    assert result.asset is not None
    assert result.asset.availability is AssetAvailability.LOCAL_VALID
    assert result.asset_request is not None
    assert result.operation is not None
    assert result.operation.status.value == "completed"
    assert retriever.calls == 1


def test_wait_is_capped_and_timed_out_work_keeps_the_same_durable_handle(tmp_path):
    config = replace(
        _config(tmp_path),
        wait_seconds_default=0.05,
        wait_seconds_maximum=0.05,
    )
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=_SlowRetriever(),
    )
    service.register_discovered_record(_record(), instrument_id="600000.SH")

    started = time.monotonic()
    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            wait_seconds=5,
            principal="alice",
            idempotency_key="bounded-wait",
        )
    )

    assert time.monotonic() - started < 0.2
    assert result.disposition is EnsureDisposition.OPERATION_CREATED
    assert result.asset_request is not None
    request_id = result.asset_request.asset_request_id
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        operation = repository.get_operation(result.operation.operation_id)
        if operation is not None and operation.status.value == "completed":
            break
        time.sleep(0.02)
    assert operation is not None and operation.status.value == "completed"
    assert repository.get_asset_request(request_id, principal="alice") is not None


def test_absent_exact_filing_uses_bounded_source_qualified_discovery(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    requested = _record(source_id="requested-filing")
    provider = _DiscoveryProvider(
        records=(requested, _record(source_id="same-period-other-filing"))
    )
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry((provider,)),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        acquisition_service=acquisition,
        attachment_retriever=retriever,
    )

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            source="cninfo",
            source_announcement_id="requested-filing",
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="exact-request",
        )
    )

    assert result.disposition is EnsureDisposition.LOCAL_HIT
    assert result.asset is not None
    assert result.asset.source_announcement_id == "requested-filing"
    assert provider.calls == 1
    assert retriever.calls == 1
    assert (
        repository.list_candidate_rows(
            source="cninfo", source_announcement_id="same-period-other-filing"
        )
        == []
    )


def test_exact_discovery_never_substitutes_another_same_period_filing(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    provider = _DiscoveryProvider(records=(_record(source_id="other-filing"),))
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry((provider,)),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        acquisition_service=acquisition,
        attachment_retriever=retriever,
    )

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            source="cninfo",
            source_announcement_id="requested-filing",
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="exact-no-substitution",
        )
    )

    assert result.disposition is EnsureDisposition.LOCAL_MISS
    assert result.reason_code == "annual_report_not_found"
    assert result.asset_request is not None
    assert result.operation is not None
    assert result.operation.status.value == "missing"
    assert provider.calls == 1
    assert retriever.calls == 0
    assert (
        repository.list_candidate_rows(
            source="cninfo", source_announcement_id="other-filing"
        )
        == []
    )


def test_exact_filing_pins_match_one_observation_and_fail_closed(tmp_path):
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
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    asset = service.acquire_attachment(registered[0].attachment_id)
    assert asset is not None and asset.content_hash
    access = AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )

    pinned = access.ensure(
        EnsureRequest(
            source="cninfo",
            source_announcement_id="600000-2025-annual",
            attachment_id=registered[0].attachment_id,
            expected_content_hash=asset.content_hash,
            observation_version=asset.version_id,
            allow_network=True,
            principal="alice",
        )
    )
    assert pinned["disposition"] == "local_hit"
    assert pinned["asset"]["content_hash"] == asset.content_hash

    wrong_hash = "0" * 64 if asset.content_hash != "0" * 64 else "1" * 64
    rejected = access.ensure(
        EnsureRequest(
            source="cninfo",
            source_announcement_id="600000-2025-annual",
            attachment_id=registered[0].attachment_id,
            expected_content_hash=wrong_hash,
            allow_network=True,
            principal="alice",
        )
    )
    assert rejected["disposition"] == "local_miss"
    assert rejected["reason_code"] == "exact_filing_pin_unavailable"
    assert rejected["asset_request_id"] is None


def test_non_effective_exact_filing_is_metadata_only_without_network_or_work(tmp_path):
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
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    current = service.acquire_attachment(original[0].attachment_id)
    assert current is not None
    correction_id = "600000-2025-correction"
    service.register_discovered_record(
        _record(
            source_id=correction_id,
            title="测试公司2025年年度报告（修订版）",
            published_at="2026-04-01T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    calls_before = retriever.calls
    operations_before = repository.list_operations(limit=100)

    result = service.ensure_annual_report(
        EnsureRequest(
            source="cninfo",
            source_announcement_id=correction_id,
            allow_network=True,
            principal="alice",
        )
    )

    assert result.disposition is EnsureDisposition.LOCAL_MISS
    assert result.availability is AssetAvailability.METADATA_ONLY
    assert result.reason_code == "non_effective_exact_filing_content_unavailable"
    assert result.asset_request is None
    assert retriever.calls == calls_before
    assert repository.list_operations(limit=100) == operations_before
    assert len(list(config.blob_root.rglob("*.pdf"))) == 1


def test_period_ensure_acquires_only_prospective_correction(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    correction_id = "600000-2025-correction"
    retriever = _SelectiveRetriever(failing_ids={correction_id})
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    service.register_discovered_record(
        _record(
            source_id=correction_id,
            title="测试公司2025年年度报告（修订版）",
            published_at="2026-04-01T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="prospective-correction-only",
        )
    )

    assert result.operation is not None
    assert result.operation.status.value == "failed"
    assert retriever.calls == 1
    assert retriever.attachment_ids == [correction_id]
    assert (
        repository.get_latest_valid_attachment_version(original[0].attachment_id)
        is None
    )


def test_retained_predecessor_has_authorized_exact_observation_handle(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    correction_id = "600000-2025-correction"
    original_bytes = PDF_BYTES
    correction_bytes = b"%PDF-1.4\ncorrected annual report\n%%EOF\n"
    retriever = _SelectiveRetriever(
        payloads={correction_id: correction_bytes},
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    original = service.register_discovered_record(_record(), instrument_id="600000.SH")
    original_asset = service.acquire_attachment(original[0].attachment_id)
    assert original_asset is not None and original_asset.content_hash
    bound_version = repository.get_attachment_version(original_asset.version_id)
    assert bound_version is not None
    correction = service.register_discovered_record(
        _record(
            source_id=correction_id,
            title="测试公司2025年年度报告（修订版）",
            published_at="2026-04-01T01:00:00+00:00",
        ),
        instrument_id="600000.SH",
    )
    correction_asset = service.acquire_attachment(correction[0].attachment_id)
    assert correction_asset is not None
    assert correction_asset.source_announcement_id == correction_id
    access = AnnouncementAssetAccess(
        repository=repository, config=config, service=service
    )
    request = EnsureRequest(
        source="cninfo",
        source_announcement_id="600000-2025-annual",
        attachment_id=original[0].attachment_id,
        observation_version=original_asset.version_id,
        expected_content_hash=original_asset.content_hash,
        allow_network=True,
        principal="internal",
    )

    public = access.ensure(request)
    assert public["reason_code"] == "retained_internal_only"
    assert public["asset_request_id"] is None
    with pytest.raises(AssetContentGoneError) as gone:
        access.content_handle(original_asset.asset_id)
    assert gone.value.lifecycle_state == "superseded"
    with pytest.raises(PermissionError):
        access.exact_observation_handle(request)
    repository.add_attachment_version(
        replace(
            bound_version,
            version_id="newer-failed-version",
            observation_key="newer-failed-observation",
            content_hash=None,
            retrieval_status="failed",
            integrity_status=IntegrityStatus.UNREADABLE,
            error_code="test_failure",
            observed_at="2026-03-21T02:00:00+00:00",
            version_available_at="2026-03-21T02:00:00+00:00",
        )
    )
    content = access.exact_observation_handle(request, authorized=True)
    assert content["content_hash"] == original_asset.content_hash
    assert content["file_handle"].read() == original_bytes
    lease_id = content["read_lease_id"]
    assert repository.get_read_lease(lease_id)["released_at"] is None
    content["file_handle"].close()
    assert repository.get_read_lease(lease_id) is None

    repository.add_attachment_version(
        replace(
            bound_version,
            version_id="newer-valid-version",
            observation_key="newer-valid-observation",
            observed_at="2026-03-22T02:00:00+00:00",
            version_available_at="2026-03-22T02:00:00+00:00",
        )
    )
    duplicate_observation = access.exact_observation_handle(request, authorized=True)
    assert duplicate_observation["observation_version"] == original_asset.version_id
    duplicate_observation["file_handle"].close()

    normalized_source = access.exact_observation_handle(
        replace(request, source=" CNINFO "), authorized=True
    )
    normalized_source["file_handle"].close()

    with pytest.raises(FileNotFoundError):
        access.exact_observation_handle(
            replace(request, expected_content_hash="0" * 64), authorized=True
        )
    with pytest.raises(FileNotFoundError):
        access.exact_observation_handle(
            replace(request, observation_version="missing-version"), authorized=True
        )
    with pytest.raises(FileNotFoundError):
        access.exact_observation_handle(
            replace(
                request,
                source_announcement_id="wrong-filing",
                filing_id="wrong-filing",
            ),
            authorized=True,
        )


def test_inactive_instrument_discovery_is_bounded_and_does_not_touch_schedule_state(
    tmp_path,
):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    provider = _DiscoveryProvider(records=())
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry((provider,)),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        acquisition_service=acquisition,
        attachment_retriever=_Retriever(),
    )
    before_states = repository.list_discovery_states()
    before_snapshot = repository.get_latest_complete_universe_snapshot()
    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600999.SH",
            fiscal_year=2025,
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="inactive-bounded",
        )
    )
    assert result.operation is not None and result.operation.status.value == "missing"
    assert provider.calls == 1
    assert provider.queries and len(provider.queries) == 1
    query = provider.queries[0]
    assert query.scope.instrument_id == "600999.SH"
    assert query.scope.symbol == "600999"
    assert query.scope.max_pages == config.discovery.max_pages
    assert query.scope.page_size == config.discovery.page_size
    assert query.scope.start_date and query.scope.end_date
    assert repository.list_discovery_states() == before_states
    assert repository.get_latest_complete_universe_snapshot() == before_snapshot


def test_exact_selector_with_instrument_context_never_returns_other_effective_filing(
    tmp_path,
):
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
    registered = service.register_discovered_record(
        _record(), instrument_id="600000.SH"
    )
    effective = service.acquire_attachment(registered[0].attachment_id)
    assert effective is not None

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            source="cninfo",
            source_announcement_id="different-filing",
            allow_network=False,
            principal="alice",
        )
    )

    assert result.disposition is EnsureDisposition.LOCAL_MISS
    assert result.availability is AssetAvailability.MISSING
    assert result.asset is None
    assert result.reason_code == "network_disabled"


def test_incomplete_exact_provider_scope_fails_closed_without_download(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    provider = _DiscoveryProvider(
        records=(_record(source_id="requested-filing"),),
        complete=False,
    )
    acquisition = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry((provider,)),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",))
        ),
    )
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        acquisition_service=acquisition,
        attachment_retriever=retriever,
    )

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            source="cninfo",
            source_announcement_id="requested-filing",
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="incomplete-exact-scope",
        )
    )

    assert result.operation is not None
    assert result.operation.status.value == "failed"
    assert result.operation.reason_code == "ensure_execution_failed"
    assert provider.calls == 1
    assert retriever.calls == 0
    assert (
        repository.list_candidate_rows(
            source="cninfo", source_announcement_id="requested-filing"
        )
        == []
    )


def test_ambiguous_period_candidates_block_without_any_attachment_download(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    retriever = _Retriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        attachment_retriever=retriever,
    )
    for source_id in ("correction-a", "correction-b"):
        service.register_discovered_record(
            _record(
                source_id=source_id,
                title="测试公司2025年年度报告（修订版）",
                published_at="2026-04-01T01:00:00+00:00",
            ),
            instrument_id="600000.SH",
        )

    result = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            wait_seconds=10,
            principal="alice",
            idempotency_key="ambiguous-period",
        )
    )

    assert result.operation is not None
    assert result.operation.status.value == "blocked"
    assert result.operation.reason_code == "candidate_not_effective"
    assert retriever.calls == 0


def test_zero_wait_dispatches_background_work_and_keeps_same_handle(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    retriever = _SlowRetriever()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    service.register_discovered_record(_record(), instrument_id="600000.SH")

    accepted = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=True,
            wait_seconds=0,
            principal="alice",
            idempotency_key="zero-wait-dispatch",
        )
    )

    assert accepted.asset_request is not None
    request_id = accepted.asset_request.asset_request_id
    operation_id = accepted.operation.operation_id
    deadline = time.monotonic() + 5
    operation = repository.get_operation(operation_id)
    while operation is not None and operation.status.value in {"queued", "running"}:
        assert time.monotonic() < deadline
        time.sleep(0.02)
        operation = repository.get_operation(operation_id)
    assert operation is not None and operation.status.value == "completed"
    assert repository.get_asset_request(request_id, principal="alice") is not None
    assert retriever.calls == 1


def test_expired_lease_restart_recovery_resumes_same_durable_operation(tmp_path):
    config = _config(tmp_path)
    db_path = tmp_path / "research.db"
    repository = AnnouncementAssetRepository(db_path)
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
    service.register_discovered_record(_record(), instrument_id="600000.SH")
    operation, created = repository.create_or_reuse_operation(
        operation_type="ensure_annual_report",
        idempotency_key="restart-recovery-operation",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version=config.policy_version,
        stage=None,
    )
    assert created is True
    repository.claim_operation(
        operation.operation_id,
        lease_owner="stopped-worker",
        lease_expires_at="2026-01-01T00:00:00+00:00",
    )

    reopened_repository = AnnouncementAssetRepository(db_path)
    reopened_service = AnnouncementAssetService(
        repository=reopened_repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    dispatched = reopened_service.resume_pending_ensure_operations(
        now="2026-08-11T00:00:00+00:00"
    )
    assert dispatched == (operation.operation_id,)
    deadline = time.monotonic() + 5
    recovered = reopened_repository.get_operation(operation.operation_id)
    while recovered is not None and recovered.status.value in {"queued", "running"}:
        assert time.monotonic() < deadline
        time.sleep(0.02)
        recovered = reopened_repository.get_operation(operation.operation_id)
    assert recovered is not None and recovered.status.value == "completed"
    assert recovered.result_asset_id
    assert retriever.calls == 1
