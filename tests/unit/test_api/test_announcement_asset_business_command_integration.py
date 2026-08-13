from __future__ import annotations

import asyncio
import hashlib
import time
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from api.app import app
from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnualReportConsumerRequestCoordinator,
    ConsumerProcessingOutcome,
    ConsumerProcessingProfile,
    OperationStage,
    OperationStatus,
)
from research.announcement_assets.access import AnnouncementAssetAccess
from research.announcement_assets.service import AnnouncementAssetService
from research.announcement_assets.storage import ContentAddressedBlobStore
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)

PDF_BYTES = b"%PDF-1.4\napi consumer integration\n%%EOF\n"


class _Retriever:
    def retrieve(self, source, attachment, *, require_pdf=False):
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


def _trusted_config():
    return SimpleNamespace(
        modules={
            "official_announcement_assets": {
                "permissions": {
                    "trusted_identity_enabled": True,
                    "principals": [
                        {
                            "principal": "alice",
                            "token_env": "TEST_ANNUAL_REPORT_ASSET_TOKEN",
                            "scopes": [
                                "annual_report_assets:acquire",
                                "business_profile:process",
                            ],
                        }
                    ],
                }
            }
        }
    )


def _access(tmp_path) -> AnnouncementAssetAccess:
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
        published_at="2026-03-20T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/600000-2025-annual.pdf",
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
    service.acquire_attachment(registered[0].attachment_id)
    return AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )


@pytest.mark.asyncio
async def test_business_command_preserves_consumer_handle_until_asset_ready(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    access = _access(tmp_path)
    effective = access.get_effective_asset("600000.SH", fiscal_year=2025)
    ensure_calls: list[str] = []
    created_asset_work: dict[str, Any] = {}

    def pending_ensure(request):
        ensure_calls.append(str(request.instrument_id))
        consumer_rows = access.repository.list_consumer_requests(
            principal="alice"
        )
        assert len(consumer_rows) == 1
        assert consumer_rows[0].status.value == "not_started"
        assert consumer_rows[0].consumer_request_id == (
            request.consumer_continuation_id
        )
        asset_request, operation, _, _ = (
            access.repository.create_or_reuse_asset_request(
                operation_type="ensure_annual_report",
                operation_idempotency_key="integration-pending-work",
                scope={
                    "instrument_id": "600000.SH",
                    "fiscal_year": 2025,
                },
                policy_version="annual-report-v1",
                principal="alice",
                request_idempotency_key=str(request.idempotency_key),
                request_fingerprint="integration-pending-asset-fingerprint",
                consumer="business_profile",
                consumer_continuation_id=request.consumer_continuation_id,
                stage=OperationStage.DISCOVERING,
            )
        )
        created_asset_work["asset_request"] = asset_request
        created_asset_work["operation"] = operation
        return {
            "disposition": "operation_created",
            "asset_availability": "missing",
            "availability": "missing",
            "asset": None,
            "asset_request_id": asset_request.asset_request_id,
            "request": {},
            "reason_code": None,
        }

    monkeypatch.setattr(access, "ensure", pending_ensure)
    parser_calls: list[str] = []
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-integration-v1",
                parameters={"document_family": "annual_report"},
                processor=lambda asset, request: (
                    parser_calls.append(str(asset["asset_id"]))
                    or ConsumerProcessingOutcome(
                        status="completed",
                        result_identity="business-profile-integration-result",
                    )
                ),
            ),
        ),
    )

    async def process(request, **kwargs):
        result = coordinator.start(
            request,
            consumer="business_profile",
            profile_name=kwargs.get("profile_name", "default"),
            expected_processing_fingerprint=kwargs.get(
                "expected_processing_fingerprint"
            ),
        )
        return {**dict(result.projection), "_http_status": result.http_status}

    async def identity(request_id, *, principal):
        return access.repository.get_consumer_request_identity(
            request_id,
            principal=principal,
        )

    async def refresh(request_id, *, principal, operator=False):
        return coordinator.refresh(
            request_id,
            principal=principal,
            operator=operator,
        )

    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.process_shared_business_profile_annual_report",
        process,
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_consumer_request_identity",
        identity,
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_consumer_request",
        refresh,
    )
    headers = {
        "Authorization": "Bearer test-secret",
        "Idempotency-Key": "integration-command",
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post(
            "/api/v1/research/company/600000.SH/business-profile/annual-report-process",
            headers=headers,
            json={"fiscal_year": 2025, "allow_network": True},
        )
        repeated = await client.post(
            "/api/v1/research/company/600000.SH/business-profile/annual-report-process",
            headers=headers,
            json={"fiscal_year": 2025, "allow_network": True},
        )

        assert first.status_code == repeated.status_code == 202
        assert first.json()["consumer_request_id"] == repeated.json()[
            "consumer_request_id"
        ]
        asset_request = created_asset_work["asset_request"]
        operation = created_asset_work["operation"]
        assert first.json()["asset_request_id"] == asset_request.asset_request_id
        assert first.headers["location"].endswith(
            first.json()["consumer_request_id"]
        )
        assert first.headers["retry-after"] == "5"
        assert ensure_calls == ["600000.SH"]
        assert len(access.repository.list_consumer_requests(principal="alice")) == 1
        assert len(access.repository.list_asset_requests(principal="alice")) == 1

        claimed = access.repository.claim_operation(
            operation.operation_id,
            lease_owner="integration-worker",
            lease_expires_at="2099-01-01T00:00:00+00:00",
            stage=OperationStage.DISCOVERING,
        )
        access.repository.transition_operation(
            operation.operation_id,
            OperationStatus.COMPLETED,
            result_asset_id=effective["asset_id"],
            expected_lease_owner="integration-worker",
            expected_lease_generation=claimed.lease_generation,
        )

        consumer_request_id = first.json()["consumer_request_id"]
        deadline = time.monotonic() + 3
        polled = await client.get(
            "/api/v1/research/annual-report-consumer-requests/"
            + consumer_request_id,
            headers={"Authorization": "Bearer test-secret"},
        )
        while (
            polled.json()["consumer_request_status"] != "completed"
            and time.monotonic() < deadline
        ):
            await asyncio.sleep(0.05)
            polled = await client.get(
                "/api/v1/research/annual-report-consumer-requests/"
                + consumer_request_id,
                headers={"Authorization": "Bearer test-secret"},
            )

    assert polled.status_code == 200
    assert polled.json()["consumer_request_id"] == first.json()[
        "consumer_request_id"
    ]
    assert polled.json()["consumer_request_status"] == "completed"
    assert polled.json()["consumer_result_state"] == "current"
    assert polled.json()["result_identity"] == "business-profile-integration-result"
    assert polled.json()["resolved_content_hash"] == effective["content_hash"]
    assert parser_calls == [effective["asset_id"]]
    coordinator.close()
