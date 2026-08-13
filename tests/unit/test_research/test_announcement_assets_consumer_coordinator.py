from __future__ import annotations

import hashlib
import time

import pytest

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnualReportConsumerRequestCoordinator,
    ConsumerCommandResult,
    ConsumerProcessingOutcome,
    ConsumerProcessingProfile,
    ConsumerProcessingStatus,
    ConsumerRequestNotCancellableError,
    ConsumerRequestStatus,
    ConsumerResultState,
    EnsureRequest,
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

PDF_BYTES = b"%PDF-1.4\nconsumer coordinator\n%%EOF\n"


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
        record, instrument_id="600000.SH"
    )
    service.acquire_attachment(registered[0].attachment_id)
    return AnnouncementAssetAccess(
        repository=repository,
        config=config,
        service=service,
    )


def test_local_asset_business_command_completes_and_reuses_processing(tmp_path):
    access = _access(tmp_path)
    calls: list[str] = []

    def processor(asset, request):
        calls.append(str(asset["asset_id"]))
        return ConsumerProcessingOutcome(
            status="completed",
            result_identity="business-profile-result-1",
            metadata={"source": "business-profile"},
        )

    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-v1",
                parameters={"field_families": ["business_scope"]},
                processor=processor,
            ),
        ),
    )
    first = coordinator.start(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            principal="alice",
            idempotency_key="business-command-1",
            wait_seconds=2,
        ),
        consumer="business_profile",
        profile_name="default",
    )
    repeated = coordinator.start(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            principal="alice",
            idempotency_key="business-command-1",
            wait_seconds=2,
        ),
        consumer="business_profile",
        profile_name="default",
    )
    other_principal = coordinator.start(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            principal="bob",
            idempotency_key="business-command-2",
            wait_seconds=2,
        ),
        consumer="business_profile",
        profile_name="default",
    )

    assert first.http_status == 200
    assert first.projection["consumer_request_status"] == "completed"
    assert first.projection["consumer_result_state"] == "current"
    assert first.projection["resolved_content_hash"] == hashlib.sha256(
        PDF_BYTES
    ).hexdigest()
    assert first.projection["resolved_variant"] == "original"
    assert first.projection["resolved_effective_decision_state"] == "current"
    assert first.projection["resolved_canonical_source_filing"] == {
        "source": "cninfo",
        "source_announcement_id": "600000-2025-annual",
        "attachment_id": first.projection["resolved_attachment_id"],
    }
    assert repeated.projection["consumer_request_id"] == first.projection[
        "consumer_request_id"
    ]
    assert other_principal.projection["consumer_request_id"] != first.projection[
        "consumer_request_id"
    ]
    assert other_principal.projection["consumer_request_status"] == "completed"
    assert calls == [first.projection["asset_id"]]


def test_network_disabled_missing_creates_queryable_terminal_consumer_request(tmp_path):
    access = _access(tmp_path)
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="broker_risk_control",
                profile_name="default",
                parser_version="broker-v1",
                parameters={},
                processor=lambda asset, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
            ),
        ),
    )
    result = coordinator.start(
        EnsureRequest(
            instrument_id="600001.SH",
            fiscal_year=2025,
            principal="alice",
            idempotency_key="missing-command-1",
            allow_network=False,
        ),
        consumer="broker_risk_control",
        profile_name="default",
    )

    assert result.http_status == 200
    assert result.projection["consumer_request_status"] == "missing"
    assert result.projection["consumer_result_state"] == "unavailable"
    assert result.projection["asset_request_id"] is None
    assert coordinator.refresh(
        result.projection["consumer_request_id"], principal="alice"
    )["consumer_request_status"] == "missing"


def test_unknown_profile_and_expected_fingerprint_mismatch_create_no_request(tmp_path):
    access = _access(tmp_path)
    profile = ConsumerProcessingProfile(
        consumer="business_profile",
        profile_name="default",
        parser_version="business-profile-v1",
        parameters={},
        processor=lambda asset, request: ConsumerProcessingOutcome(
            status="completed"
        ),
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    request = EnsureRequest(
        instrument_id="600000.SH",
        fiscal_year=2025,
        principal="alice",
        idempotency_key="invalid-profile-command",
    )

    with pytest.raises(ValueError, match="unknown_consumer_processing_profile"):
        coordinator.start(
            request,
            consumer="business_profile",
            profile_name="unknown",
        )
    with pytest.raises(ValueError, match="processing_fingerprint_mismatch"):
        coordinator.start(
            request,
            consumer="business_profile",
            profile_name="default",
            expected_processing_fingerprint="wrong",
        )
    assert access.repository.list_consumer_requests(principal="alice") == []


def test_pre_acceptance_asset_blocker_leaves_no_consumer_request(
    tmp_path, monkeypatch
):
    access = _access(tmp_path)
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: (_ for _ in ()).throw(
            RuntimeError("archive_mount_unavailable")
        ),
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-v1",
                parameters={},
                processor=lambda asset, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
            ),
        ),
    )

    with pytest.raises(RuntimeError, match="archive_mount_unavailable"):
        coordinator.start(
            EnsureRequest(
                instrument_id="600001.SH",
                fiscal_year=2025,
                allow_network=True,
                principal="alice",
                idempotency_key="pre-acceptance-blocker",
            ),
            consumer="business_profile",
            profile_name="default",
        )

    assert access.repository.list_consumer_requests(principal="alice") == []


def test_pending_asset_continuation_advances_same_consumer_request(
    tmp_path, monkeypatch
):
    access = _access(tmp_path)
    asset = access.get_effective_asset("600000.SH", fiscal_year=2025)
    asset_request, operation, _, _ = access.repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="pending-asset-work",
        scope={"instrument_id": "600001.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="asset:pending-command",
        request_fingerprint="pending-asset-request",
        stage=OperationStage.DISCOVERING,
    )
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: {
            "disposition": "operation_created",
            "availability": "missing",
            "asset": None,
            "asset_request_id": asset_request.asset_request_id,
            "request": {},
            "reason_code": None,
        },
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-v1",
                parameters={},
                processor=lambda selected, request: ConsumerProcessingOutcome(
                    status="completed",
                    result_identity="continued-result",
                ),
            ),
        ),
    )
    started = coordinator.start(
        EnsureRequest(
            instrument_id="600001.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
            idempotency_key="pending-command",
        ),
        consumer="business_profile",
        profile_name="default",
    )
    assert started.projection["consumer_request_status"] == "pending_asset"
    consumer_request_id = started.projection["consumer_request_id"]

    claimed = access.repository.claim_operation(
        operation.operation_id,
        lease_owner="test-worker",
        lease_expires_at="2099-01-01T00:00:00+00:00",
        stage=OperationStage.DISCOVERING,
    )
    access.repository.transition_operation(
        operation.operation_id,
        OperationStatus.COMPLETED,
        result_asset_id=asset["asset_id"],
        expected_lease_owner="test-worker",
        expected_lease_generation=claimed.lease_generation,
    )
    deadline = time.monotonic() + 3
    projection = coordinator.refresh(consumer_request_id, principal="alice")
    while (
        projection["consumer_request_status"] != "completed"
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
        projection = coordinator.refresh(consumer_request_id, principal="alice")

    assert projection["consumer_request_id"] == consumer_request_id
    assert projection["consumer_request_status"] == "completed"
    assert projection["consumer_result_state"] == "current"
    assert projection["result_identity"] == "continued-result"
    coordinator.close()


def test_close_stops_pending_continuation_and_preserves_restart_state(
    tmp_path, monkeypatch
):
    access = _access(tmp_path)
    asset_request, _, _, _ = access.repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="pending-close-work",
        scope={"instrument_id": "600001.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="asset:pending-close",
        request_fingerprint="pending-close-asset-request",
        stage=OperationStage.DISCOVERING,
    )
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: {
            "disposition": "operation_created",
            "availability": "missing",
            "asset": None,
            "asset_request_id": asset_request.asset_request_id,
            "request": {},
            "reason_code": None,
        },
    )
    profile = ConsumerProcessingProfile(
        consumer="business_profile",
        profile_name="default",
        parser_version="business-profile-v1",
        parameters={},
        processor=lambda asset, request: ConsumerProcessingOutcome(status="completed"),
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    started = coordinator.start(
        EnsureRequest(
            instrument_id="600001.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
            idempotency_key="pending-close",
        ),
        consumer="business_profile",
        profile_name="default",
    )

    close_started = time.monotonic()
    coordinator.close()
    coordinator.close()

    assert time.monotonic() - close_started < 1.0
    persisted = access.repository.get_consumer_request(
        started.projection["consumer_request_id"]
    )
    assert persisted is not None
    assert persisted.status is ConsumerRequestStatus.PENDING_ASSET
    replacement = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    assert replacement.resume_pending() == (persisted.consumer_request_id,)
    replacement.close()


def test_refresh_projects_stale_processing_without_invalidating_asset(tmp_path):
    access = _access(tmp_path)
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="broker_risk_control",
                profile_name="default",
                parser_version="broker-v1",
                parameters={},
                processor=lambda asset, request: ConsumerProcessingOutcome(
                    status="completed",
                    result_identity="broker-result-1",
                ),
            ),
        ),
    )
    completed = coordinator.start(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            principal="alice",
            idempotency_key="stale-command",
            wait_seconds=2,
        ),
        consumer="broker_risk_control",
        profile_name="default",
    )
    request = access.repository.get_consumer_request(
        completed.projection["consumer_request_id"], principal="alice"
    )
    access.repository.transition_consumer_processing(
        request.processing_id,
        status=ConsumerProcessingStatus.STALE,
        error_code="effective_asset_replaced",
    )

    projection = coordinator.refresh(
        request.consumer_request_id,
        principal="alice",
    )

    assert projection["consumer_request_status"] == "completed"
    assert projection["consumer_result_state"] == "stale"
    assert projection["reason_code"] == "effective_asset_replaced"
    assert access.get_effective_asset("600000.SH", fiscal_year=2025)[
        "availability"
    ] == "local_valid"


def test_resume_pending_reclaims_expired_processing_with_generation_fence(
    tmp_path, monkeypatch
):
    access = _access(tmp_path)
    calls: list[str] = []
    profile = ConsumerProcessingProfile(
        consumer="business_profile",
        profile_name="default",
        parser_version="business-profile-restart-v1",
        parameters={},
        processor=lambda asset, request: (
            calls.append(str(asset["asset_id"]))
            or ConsumerProcessingOutcome(
                status="completed",
                result_identity="recovered-result",
            )
        ),
    )
    stopped = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    monkeypatch.setattr(stopped._executor, "submit", lambda *args, **kwargs: None)
    queued = stopped.start(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            principal="alice",
            idempotency_key="restart-processing-command",
        ),
        consumer="business_profile",
        profile_name="default",
    )
    request = access.repository.get_consumer_request(
        queued.projection["consumer_request_id"], principal="alice"
    )
    old_generation = access.repository.claim_consumer_processing(
        request.processing_id,
        lease_owner="dead-worker",
        lease_seconds=900,
        max_attempts=4,
    )
    with access.repository.transaction() as conn:
        conn.execute(
            """UPDATE official_asset_consumer_processing
               SET lease_expires_at='2000-01-01T00:00:00+00:00',
                   heartbeat_at='2000-01-01T00:00:00+00:00'
               WHERE processing_id=?""",
            (request.processing_id,),
        )

    restarted = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    assert restarted.resume_pending() == (request.consumer_request_id,)
    deadline = time.monotonic() + 3
    projection = restarted.refresh(request.consumer_request_id, principal="alice")
    while (
        projection["consumer_request_status"] != "completed"
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
        projection = restarted.refresh(
            request.consumer_request_id, principal="alice"
        )

    assert projection["consumer_request_status"] == "completed"
    assert projection["consumer_result_state"] == "current"
    assert projection["result_identity"] == "recovered-result"
    assert calls == [projection["asset_id"]]
    processing = access.repository.list_consumer_processing(
        asset_id=projection["asset_id"], consumer="business_profile"
    )[0]
    assert processing["lease_generation"] == old_generation + 1
    assert processing["attempt"] == 2
    assert processing["lease_owner"] is None
    with pytest.raises(ValueError):
        access.repository.transition_consumer_processing(
            request.processing_id,
            status=ConsumerProcessingStatus.FAILED,
            error_code="late-dead-worker",
            lease_owner="dead-worker",
            lease_generation=old_generation,
        )


def test_resume_pending_continues_asset_ready_request_after_restart(
    tmp_path, monkeypatch
):
    access = _access(tmp_path)
    asset = access.get_effective_asset("600000.SH", fiscal_year=2025)
    asset_request, operation, _, _ = access.repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="restart-pending-asset-work",
        scope={"instrument_id": "600001.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="asset:restart-pending-command",
        request_fingerprint="restart-pending-asset-request",
        stage=OperationStage.DISCOVERING,
    )
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: {
            "disposition": "operation_created",
            "availability": "missing",
            "asset": None,
            "asset_request_id": asset_request.asset_request_id,
            "request": {},
            "reason_code": None,
        },
    )
    profile = ConsumerProcessingProfile(
        consumer="broker_risk_control",
        profile_name="default",
        parser_version="broker-restart-v1",
        parameters={},
        processor=lambda selected, request: ConsumerProcessingOutcome(
            status="completed", result_identity="asset-recovered-result"
        ),
    )
    stopped = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    monkeypatch.setattr(stopped._executor, "submit", lambda *args, **kwargs: None)
    pending = stopped.start(
        EnsureRequest(
            instrument_id="600001.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
            idempotency_key="restart-pending-command",
        ),
        consumer="broker_risk_control",
        profile_name="default",
    )
    cancelled_asset_request = access.repository.cancel_asset_request(
        asset_request.asset_request_id,
        principal="alice",
    )
    assert cancelled_asset_request.status.value == "cancelled"
    claimed = access.repository.claim_operation(
        operation.operation_id,
        lease_owner="asset-worker",
        lease_expires_at="2099-01-01T00:00:00+00:00",
        stage=OperationStage.DISCOVERING,
    )
    access.repository.transition_operation(
        operation.operation_id,
        OperationStatus.COMPLETED,
        result_asset_id=asset["asset_id"],
        expected_lease_owner="asset-worker",
        expected_lease_generation=claimed.lease_generation,
    )

    restarted = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(profile,),
    )
    request_id = pending.projection["consumer_request_id"]
    assert restarted.resume_pending() == (request_id,)
    deadline = time.monotonic() + 3
    projection = restarted.refresh(request_id, principal="alice")
    while (
        projection["consumer_request_status"] != "completed"
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
        projection = restarted.refresh(request_id, principal="alice")

    assert projection["consumer_request_status"] == "completed"
    assert projection["consumer_result_state"] == "current"
    assert projection["result_identity"] == "asset-recovered-result"


def test_request_cancellation_survives_removed_processing_profile(tmp_path):
    access = _access(tmp_path)
    request, _ = access.repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="removed-profile-request",
        request_fingerprint="removed-profile-request-fingerprint",
        processing_fingerprint="retired-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.NOT_STARTED,
        metadata={"profile_name": "retired"},
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-v2",
                parameters={},
                processor=lambda asset, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
            ),
        ),
    )

    cancelled, disposition = coordinator.request_cancellation(
        request.consumer_request_id,
        principal="alice",
    )

    assert disposition == "cancelled"
    assert cancelled["consumer_request_status"] == "cancelled"


def test_started_blocked_request_uses_domain_cooperative_stop_contract(tmp_path):
    access = _access(tmp_path)
    stopped: list[str] = []
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="broker_risk_control",
                profile_name="default",
                parser_version="broker-v1",
                parameters={},
                processor=lambda asset, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
                cooperative_stop=lambda request_id: (
                    stopped.append(request_id) or True
                ),
            ),
        ),
    )
    request, _ = access.repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="broker_risk_control",
        request_idempotency_key="started-blocked-cooperative",
        request_fingerprint="started-blocked-cooperative-fingerprint",
        processing_fingerprint=coordinator.processing_fingerprint(
            "broker_risk_control", "default"
        ),
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.QUEUED,
        metadata={"profile_name": "default"},
    )
    access.repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.PROCESSING,
        result_state=ConsumerResultState.REPROCESSING,
    )
    access.repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.BLOCKED,
        result_state=ConsumerResultState.UNAVAILABLE,
        reason_code="consumer_processing_blocked",
    )

    projection, disposition = coordinator.request_cancellation(
        request.consumer_request_id,
        principal="alice",
    )

    assert disposition == "stop_requested"
    assert projection["consumer_request_status"] == "blocked"
    assert projection["stop_requested_at"] is not None
    assert stopped == [request.consumer_request_id]


def test_started_request_with_removed_profile_fails_closed_as_not_cancellable(
    tmp_path,
):
    access = _access(tmp_path)
    request, _ = access.repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="removed-started-profile",
        request_fingerprint="removed-started-profile-fingerprint",
        processing_fingerprint="retired-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.QUEUED,
        metadata={"profile_name": "retired"},
    )
    access.repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.PROCESSING,
        result_state=ConsumerResultState.REPROCESSING,
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-v2",
                parameters={},
                processor=lambda asset, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
            ),
        ),
    )

    with pytest.raises(
        ConsumerRequestNotCancellableError, match="request_not_cancellable"
    ):
        coordinator.request_cancellation(
            request.consumer_request_id,
            principal="alice",
        )


@pytest.mark.parametrize(
    ("status", "result_state", "expected_http_status"),
    [
        ("completed", "current", 200),
        ("missing", "unavailable", 200),
        ("completed", "unavailable", 202),
        ("completed", "stale", 202),
        ("processing", "reprocessing", 202),
        ("blocked", "unavailable", 202),
    ],
)
def test_business_command_http_status_requires_exact_terminal_state_pair(
    status,
    result_state,
    expected_http_status,
):
    result = ConsumerCommandResult(
        projection={
            "consumer_request_status": status,
            "consumer_result_state": result_state,
        },
        created=True,
    )

    assert result.http_status == expected_http_status


def test_provisional_default_effective_blocks_without_consumer_enqueue(
    tmp_path,
    monkeypatch,
):
    access = _access(tmp_path)
    asset = dict(access.get_effective_asset("600000.SH", fiscal_year=2025))
    asset["effective_state"] = "provisional"
    asset["effective_decision_state"] = "provisional"
    asset["pending_candidate_id"] = "candidate-correction"
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: {
            "disposition": "local_miss",
            "asset_availability": "local_valid",
            "availability": "local_valid",
            "asset": asset,
            "asset_request_id": None,
            "request": None,
            "reason_code": "pending_correction",
        },
    )
    processor_calls: list[str] = []
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="broker_risk_control",
                profile_name="default",
                parser_version="broker-provisional-v1",
                parameters={},
                processor=lambda selected, request: (
                    processor_calls.append(str(selected["asset_id"]))
                    or ConsumerProcessingOutcome(status="completed")
                ),
            ),
        ),
    )

    result = coordinator.start(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            principal="alice",
            idempotency_key="provisional-command",
        ),
        consumer="broker_risk_control",
        profile_name="default",
    )

    assert result.http_status == 202
    assert result.projection["consumer_request_status"] == "blocked"
    assert result.projection["consumer_result_state"] == "stale"
    assert result.projection["reason_code"] == "pending_correction"
    assert processor_calls == []
    assert access.repository.list_consumer_processing(
        asset_id=asset["asset_id"],
        consumer="broker_risk_control",
    ) == []


@pytest.mark.parametrize(
    ("decision_state", "availability", "expected_code"),
    [
        ("ambiguous", "ambiguous", "candidate_ambiguous"),
        ("blocked", "blocked", "effective_state_conflict"),
        ("withdrawn", "metadata_only", "effective_state_conflict"),
    ],
)
def test_local_default_effective_conflict_creates_no_consumer_or_asset_work(
    tmp_path,
    monkeypatch,
    decision_state,
    availability,
    expected_code,
):
    access = _access(tmp_path)
    asset = dict(access.get_effective_asset("600000.SH", fiscal_year=2025))
    asset["effective_state"] = decision_state
    asset["effective_decision_state"] = decision_state
    asset["asset_availability"] = availability
    asset["availability"] = availability
    ensure_calls: list[str] = []
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: (
            ensure_calls.append(request.instrument_id)
            or {
                "disposition": "local_miss",
                "asset_availability": availability,
                "availability": availability,
                "asset": asset,
                "asset_request_id": None,
                "request": None,
                "reason_code": expected_code,
            }
        ),
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-conflict-v1",
                parameters={},
                processor=lambda selected, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
            ),
        ),
    )

    with pytest.raises(ValueError, match=expected_code):
        coordinator.start(
            EnsureRequest(
                instrument_id="600000.SH",
                fiscal_year=2025,
                principal="alice",
                idempotency_key=f"conflict-{decision_state}",
            ),
            consumer="business_profile",
            profile_name="default",
        )

    assert ensure_calls == ["600000.SH"]
    assert access.repository.list_consumer_requests(principal="alice") == []
    assert access.repository.list_asset_requests(principal="alice") == []
    assert access.repository.list_consumer_processing(
        consumer="business_profile"
    ) == []


def test_post_acceptance_asset_blocker_is_exposed_by_consumer_polling(
    tmp_path,
    monkeypatch,
):
    access = _access(tmp_path)
    asset_request, operation, _, _ = access.repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="post-acceptance-blocked-work",
        scope={"instrument_id": "600001.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="asset:post-acceptance-blocked",
        request_fingerprint="post-acceptance-blocked-asset-fingerprint",
        stage=OperationStage.DISCOVERING,
    )
    monkeypatch.setattr(
        access,
        "ensure",
        lambda request: {
            "disposition": "operation_created",
            "asset_availability": "missing",
            "availability": "missing",
            "asset": None,
            "asset_request_id": asset_request.asset_request_id,
            "request": {},
            "reason_code": None,
        },
    )
    coordinator = AnnualReportConsumerRequestCoordinator(
        access=access,
        profiles=(
            ConsumerProcessingProfile(
                consumer="business_profile",
                profile_name="default",
                parser_version="business-profile-blocked-v1",
                parameters={},
                processor=lambda selected, request: ConsumerProcessingOutcome(
                    status="completed"
                ),
            ),
        ),
    )
    started = coordinator.start(
        EnsureRequest(
            instrument_id="600001.SH",
            fiscal_year=2025,
            allow_network=True,
            principal="alice",
            idempotency_key="post-acceptance-blocked",
        ),
        consumer="business_profile",
        profile_name="default",
    )
    assert started.http_status == 202
    consumer_request_id = started.projection["consumer_request_id"]
    claimed = access.repository.claim_operation(
        operation.operation_id,
        lease_owner="test-worker",
        lease_expires_at="2099-01-01T00:00:00+00:00",
        stage=OperationStage.DISCOVERING,
    )
    access.repository.transition_operation(
        operation.operation_id,
        OperationStatus.BLOCKED,
        reason_code="storage_reserve_exceeded",
        expected_lease_owner="test-worker",
        expected_lease_generation=claimed.lease_generation,
    )

    deadline = time.monotonic() + 3
    projection = coordinator.refresh(consumer_request_id, principal="alice")
    while (
        projection["consumer_request_status"] != "blocked"
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
        projection = coordinator.refresh(consumer_request_id, principal="alice")

    assert projection["consumer_request_status"] == "blocked"
    assert projection["consumer_result_state"] == "unavailable"
    assert projection["reason_code"] == "storage_reserve_exceeded"
    assert projection["asset_request_id"] == asset_request.asset_request_id
    assert projection["retry_metadata"]["operation_status"] == "blocked"
