from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from research.announcement_assets import (
    AnnouncementAssetRepository,
    AssetRequestStatus,
    ConsumerRequestNotCancellableError,
    ConsumerRequestStatus,
    ConsumerResultState,
    IdempotencyConflictError,
    OperationStage,
    OperationStatus,
)


def _repository(tmp_path) -> AnnouncementAssetRepository:
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    return repository


def test_consumer_request_idempotency_and_owner_isolation(tmp_path):
    repository = _repository(tmp_path)
    first, created = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="request-1",
        request_fingerprint="request-fingerprint-1",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.NOT_STARTED,
    )
    reused, reused_created = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="request-1",
        request_fingerprint="request-fingerprint-1",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.NOT_STARTED,
    )

    assert created is True
    assert reused_created is False
    assert reused.consumer_request_id == first.consumer_request_id
    assert repository.get_consumer_request(
        first.consumer_request_id, principal="bob"
    ) is None
    with pytest.raises(IdempotencyConflictError):
        repository.create_or_reuse_consumer_request(
            principal="alice",
            consumer="broker_risk_control",
            request_idempotency_key="request-1",
            request_fingerprint="different-request",
            processing_fingerprint="broker-parser-v1",
            selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
            status=ConsumerRequestStatus.NOT_STARTED,
        )


def test_consumer_cancellation_does_not_cancel_asset_request_or_operation(tmp_path):
    repository = _repository(tmp_path)
    asset_request, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="asset-work-1",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="asset-request-1",
        request_fingerprint="asset-request-fingerprint-1",
        stage=OperationStage.DISCOVERING,
    )
    consumer_request, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="consumer-request-1",
        request_fingerprint="consumer-request-fingerprint-1",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.PENDING_ASSET,
        asset_request_id=asset_request.asset_request_id,
    )

    cancelled, disposition = repository.cancel_consumer_request(
        consumer_request.consumer_request_id,
        principal="alice",
    )
    repeated, repeated_disposition = repository.cancel_consumer_request(
        consumer_request.consumer_request_id,
        principal="alice",
    )

    assert disposition == repeated_disposition == "cancelled"
    assert cancelled.status is ConsumerRequestStatus.CANCELLED
    assert repeated.consumer_request_id == cancelled.consumer_request_id
    assert (
        repository.get_asset_request(
            asset_request.asset_request_id, principal="alice"
        ).status
        is AssetRequestStatus.ACTIVE
    )
    assert repository.get_operation(operation.operation_id).status is OperationStatus.QUEUED


def test_second_principal_reuses_terminal_asset_work_during_retention(tmp_path):
    repository = _repository(tmp_path)
    first, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="shared-terminal-work",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="alice-terminal-request",
        request_fingerprint="alice-terminal-fingerprint",
        stage=OperationStage.DISCOVERING,
    )
    claimed = repository.claim_operation(
        operation.operation_id,
        lease_owner="consumer-request-worker",
        lease_expires_at="2099-01-01T00:00:00+00:00",
        stage=OperationStage.DISCOVERING,
    )
    repository.transition_operation(
        operation.operation_id,
        OperationStatus.MISSING,
        stage=OperationStage.NOT_APPLICABLE,
        reason_code="annual_report_not_found",
        expected_lease_owner="consumer-request-worker",
        expected_lease_generation=claimed.lease_generation,
    )
    second, reused_operation, created, operation_created = (
        repository.create_or_reuse_asset_request(
            operation_type="ensure_annual_report",
            operation_idempotency_key="shared-terminal-work",
            scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
            policy_version="annual-report-v1",
            principal="bob",
            request_idempotency_key="bob-terminal-request",
            request_fingerprint="bob-terminal-fingerprint",
            stage=OperationStage.DISCOVERING,
        )
    )

    assert second.asset_request_id != first.asset_request_id
    assert created is True
    assert operation_created is False
    assert reused_operation.operation_id == operation.operation_id
    assert reused_operation.status is OperationStatus.MISSING


def test_asset_request_detach_does_not_cancel_linked_consumer_continuation(tmp_path):
    repository = _repository(tmp_path)
    asset_request, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="linked-asset-work",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="linked-asset-request",
        request_fingerprint="linked-asset-request-fingerprint",
        consumer="business_profile",
        consumer_continuation_id="consumerreq-linked",
        stage=OperationStage.DISCOVERING,
    )
    consumer_request, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="linked-consumer-request",
        request_fingerprint="linked-consumer-request-fingerprint",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.PENDING_ASSET,
        asset_request_id=asset_request.asset_request_id,
    )

    first = repository.cancel_asset_request(
        asset_request.asset_request_id,
        principal="alice",
    )
    repeated = repository.cancel_asset_request(
        asset_request.asset_request_id,
        principal="alice",
    )

    assert first.status is repeated.status is AssetRequestStatus.CANCELLED
    assert first.consumer_continuation_id == "consumerreq-linked"
    linked = repository.get_consumer_request(
        consumer_request.consumer_request_id,
        principal="alice",
    )
    assert linked.status is ConsumerRequestStatus.PENDING_ASSET
    assert linked.asset_request_id == asset_request.asset_request_id
    assert repository.get_operation(operation.operation_id).status is OperationStatus.QUEUED


def test_started_consumer_request_requires_cooperative_stop(tmp_path):
    repository = _repository(tmp_path)
    request, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="broker_risk_control",
        request_idempotency_key="consumer-request-2",
        request_fingerprint="consumer-request-fingerprint-2",
        processing_fingerprint="broker-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.QUEUED,
    )
    processing = repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.PROCESSING,
        result_state=ConsumerResultState.REPROCESSING,
    )
    assert processing.processing_started_at is not None

    with pytest.raises(
        ConsumerRequestNotCancellableError, match="request_not_cancellable"
    ):
        repository.cancel_consumer_request(
            request.consumer_request_id,
            principal="alice",
        )

    stop_requested, disposition = repository.cancel_consumer_request(
        request.consumer_request_id,
        principal="alice",
        cooperative_stop_accepted=True,
    )
    assert disposition == "stop_requested"
    assert stop_requested.status is ConsumerRequestStatus.PROCESSING
    assert stop_requested.stop_requested_at is not None


def test_completed_result_and_expired_request_are_not_cancellable(tmp_path):
    repository = _repository(tmp_path)
    request, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="consumer-request-3",
        request_fingerprint="consumer-request-fingerprint-3",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.QUEUED,
    )
    completed = repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.COMPLETED,
        result_state=ConsumerResultState.CURRENT,
        result_identity="business-profile-result-1",
    )
    assert completed.result_identity == "business-profile-result-1"
    with pytest.raises(ConsumerRequestNotCancellableError):
        repository.cancel_consumer_request(
            request.consumer_request_id,
            principal="alice",
        )

    expiry = (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat()
    expiring, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="consumer-request-expiring",
        request_fingerprint="consumer-request-expiring-fingerprint",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600001.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.NOT_STARTED,
        expires_at=expiry,
    )
    with repository.transaction() as conn:
        conn.execute(
            "UPDATE official_asset_consumer_requests SET expires_at=? "
            "WHERE consumer_request_id=?",
            ("2000-01-01T00:00:00+00:00", expiring.consumer_request_id),
        )
    expired = repository.get_consumer_request(
        expiring.consumer_request_id,
        principal="alice",
    )
    assert expired.status is ConsumerRequestStatus.EXPIRED
    assert expired.result_state is ConsumerResultState.UNAVAILABLE
    assert expired.expired_at is not None
    assert expired.tombstone_until is not None
    with pytest.raises(ConsumerRequestNotCancellableError):
        repository.cancel_consumer_request(
            expiring.consumer_request_id,
            principal="alice",
        )


def test_not_started_blocked_request_cancels_without_losing_blocker_evidence(
    tmp_path,
):
    repository = _repository(tmp_path)
    asset_request, operation, _, _ = repository.create_or_reuse_asset_request(
        operation_type="ensure_annual_report",
        operation_idempotency_key="blocked-asset-work",
        scope={"instrument_id": "600000.SH", "fiscal_year": 2025},
        policy_version="annual-report-v1",
        principal="alice",
        request_idempotency_key="blocked-asset-request",
        request_fingerprint="blocked-asset-request-fingerprint",
        stage=OperationStage.DISCOVERING,
    )
    request, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="business_profile",
        request_idempotency_key="blocked-consumer-request",
        request_fingerprint="blocked-consumer-request-fingerprint",
        processing_fingerprint="profile-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.NOT_STARTED,
        asset_request_id=asset_request.asset_request_id,
    )
    blocked = repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.BLOCKED,
        result_state=ConsumerResultState.UNAVAILABLE,
        reason_code="storage_reserve_exceeded",
        retry_metadata={"resume_required": True, "attempt": 2},
        diagnostics={"operator_action_required": "free_space"},
    )
    assert blocked.processing_started_at is None

    cancelled, disposition = repository.cancel_consumer_request(
        request.consumer_request_id,
        principal="alice",
    )

    assert disposition == "cancelled"
    assert cancelled.status is ConsumerRequestStatus.CANCELLED
    assert cancelled.reason_code == "storage_reserve_exceeded"
    assert cancelled.retry_metadata == {"resume_required": True, "attempt": 2}
    assert cancelled.diagnostics == {"operator_action_required": "free_space"}
    assert (
        repository.get_asset_request(
            asset_request.asset_request_id,
            principal="alice",
        ).status
        is AssetRequestStatus.ACTIVE
    )
    assert repository.get_operation(operation.operation_id).status is OperationStatus.QUEUED


def test_started_blocked_request_requires_accepted_cooperative_stop(tmp_path):
    repository = _repository(tmp_path)
    request, _ = repository.create_or_reuse_consumer_request(
        principal="alice",
        consumer="broker_risk_control",
        request_idempotency_key="started-blocked-request",
        request_fingerprint="started-blocked-request-fingerprint",
        processing_fingerprint="broker-parser-v1",
        selector={"instrument_id": "600000.SH", "fiscal_year": 2025},
        status=ConsumerRequestStatus.QUEUED,
    )
    repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.PROCESSING,
        result_state=ConsumerResultState.REPROCESSING,
    )
    blocked = repository.transition_consumer_request(
        request.consumer_request_id,
        status=ConsumerRequestStatus.BLOCKED,
        result_state=ConsumerResultState.UNAVAILABLE,
        reason_code="consumer_processing_blocked",
    )
    assert blocked.processing_started_at is not None

    with pytest.raises(
        ConsumerRequestNotCancellableError, match="request_not_cancellable"
    ):
        repository.cancel_consumer_request(
            request.consumer_request_id,
            principal="alice",
        )

    stop_requested, disposition = repository.cancel_consumer_request(
        request.consumer_request_id,
        principal="alice",
        cooperative_stop_accepted=True,
    )
    assert disposition == "stop_requested"
    assert stop_requested.status is ConsumerRequestStatus.BLOCKED
    assert stop_requested.stop_requested_at is not None
    assert stop_requested.reason_code == "cooperative_stop_requested"
