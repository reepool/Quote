from __future__ import annotations

from pathlib import Path

import pytest

from research.announcement_assets.config import AnnouncementAssetConfig, RetryConfig
from research.announcement_assets.daily import AnnualReportDailyUpdater
from research.announcement_assets.models import OperationStatus
from research.announcement_assets.repository import (
    AnnouncementAssetRepository,
    DiscoveryRetryBlockedError,
    DiscoveryRetryNotDueError,
)
from research.announcement_assets.retry import (
    RetryFailureClass,
    RetryQueueStatus,
    classify_retry_failure,
)
from research.announcement_assets.service import AnnouncementAssetService
from research.announcement_assets.storage import ContentAddressedBlobStore
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)

NOW = "2026-08-10T00:00:00+00:00"
NEXT_MINUTE = "2026-08-10T00:01:00+00:00"


def _repository(tmp_path: Path) -> AnnouncementAssetRepository:
    repository = AnnouncementAssetRepository(tmp_path / "retry.db")
    repository.initialize_schema()
    return repository


def _daily_config(tmp_path: Path) -> AnnouncementAssetConfig:
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
            "discovery": {
                "overlap_days": 3,
                "initial_lookback_days": 30,
                "reconciliation_lookback_days": 30,
                "max_pages": 2,
                "page_size": 10,
                "max_requests": 20,
                "max_windows": 2,
                "max_instruments": 10,
                "max_elapsed_seconds": 60,
                "targeted_repair_lookback_years": 5,
                "provider_coverage_start_year": 2000,
            },
            "retry": {
                "max_attempts": 1,
                "initial_backoff_seconds": 60,
                "max_backoff_seconds": 60,
                "lease_seconds": 900,
                "heartbeat_seconds": 60,
            },
        },
        project_root=tmp_path,
    )


def _operation(repository: AnnouncementAssetRepository, suffix: str):
    operation, created = repository.create_or_reuse_operation(
        operation_type="retry_contract_test",
        idempotency_key=f"retry-contract-{suffix}",
        scope={"suffix": suffix},
        policy_version="v1",
    )
    assert created
    return operation


def _attachment(repository: AnnouncementAssetRepository) -> str:
    source_id = "annual-report-retry"
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id=source_id,
        announcement_key=build_announcement_key("cninfo", source_id),
        title="测试公司2025年年度报告",
        published_at="2026-03-20T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/annual-report-retry.pdf",
                attachment_id="annual-report-retry.pdf",
                name="annual-report-retry.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": source_id},
    )
    announcement = repository.upsert_announcement(
        record,
        instrument_id="600000.SH",
        observed_at=NOW,
    )
    attachment = repository.upsert_attachment(
        announcement.announcement_id,
        record.attachments[0],
        observed_at=NOW,
    )
    return attachment.attachment_id


def _finish_discovery(
    repository: AnnouncementAssetRepository,
    claimed: dict[str, object],
    *,
    status: str,
    decision=None,
):
    return repository.upsert_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="retry-policy-v1",
        status=status,
        is_complete=False,
        covered_until=None,
        run_cutoff=NOW,
        gap_reason=None if decision is None else decision.reason_code,
        checkpoint={"window_start": "2026-08-07T00:00:00+00:00"},
        expected_lease_owner=str(claimed["lease_owner"]),
        expected_lease_generation=int(claimed["lease_generation"]),
        expected_state_version=int(claimed["state_version"]),
        next_retry_at=None if decision is None else decision.next_retry_at,
        error_code=None if decision is None else decision.reason_code,
        failure_class=(None if decision is None else decision.failure_class.value),
        operator_action_required=(
            False if decision is None else decision.operator_action_required
        ),
        consumes_retry_budget=(
            True if decision is None else decision.consumes_retry_budget
        ),
    )


@pytest.mark.parametrize(
    ("message", "failure_class", "reason_code"),
    [
        ("provider timeout", RetryFailureClass.TRANSIENT, "transient_failure"),
        ("NAS unavailable", RetryFailureClass.TRANSIENT, "transient_failure"),
        ("identity conflict", RetryFailureClass.OPERATOR_ACTION, "identity_conflict"),
        ("unsafe path escapes root", RetryFailureClass.OPERATOR_ACTION, "unsafe_path"),
        ("invalid PDF signature", RetryFailureClass.OPERATOR_ACTION, "invalid_pdf"),
        (
            "persistent content length mismatch",
            RetryFailureClass.OPERATOR_ACTION,
            "persistent_length_mismatch",
        ),
        (
            "persistent hash mismatch",
            RetryFailureClass.OPERATOR_ACTION,
            "persistent_hash_mismatch",
        ),
        (
            "candidate ambiguous",
            RetryFailureClass.OPERATOR_ACTION,
            "candidate_ambiguous",
        ),
        (
            "unsplittable dense window",
            RetryFailureClass.OPERATOR_ACTION,
            "unsplittable_window",
        ),
    ],
)
def test_retry_classification_is_stable(message, failure_class, reason_code):
    decision = classify_retry_failure(
        message,
        attempt=1,
        config=RetryConfig(),
        now=NOW,
    )

    assert decision.failure_class is failure_class
    assert decision.reason_code == reason_code


def test_bounded_backoff_and_storage_block_do_not_share_retry_budget():
    config = RetryConfig(max_attempts=4, initial_backoff_seconds=60, max_backoff_seconds=120)

    first = classify_retry_failure("provider timeout", attempt=1, config=config, now=NOW)
    third = classify_retry_failure("provider timeout", attempt=3, config=config, now=NOW)
    exhausted = classify_retry_failure(
        "provider timeout", attempt=4, config=config, now=NOW
    )
    storage = classify_retry_failure(
        "hard free-space reserve would be violated",
        attempt=3,
        config=config,
        now=NOW,
    )

    assert first.next_retry_at == NEXT_MINUTE
    assert third.next_retry_at == "2026-08-10T00:02:00+00:00"
    assert exhausted.status is RetryQueueStatus.EXHAUSTED
    assert exhausted.next_retry_at is None
    assert storage.status is RetryQueueStatus.BLOCKED
    assert storage.consumes_retry_budget is False
    assert storage.next_retry_at is None


def test_attachment_retry_projects_terminal_substates_and_governs_reopen(tmp_path):
    repository = _repository(tmp_path)
    attachment_id = _attachment(repository)
    operation = _operation(repository, "attachment-exhausted")
    config = RetryConfig(max_attempts=1)
    repository.enqueue_attachment_retry(
        attachment_id=attachment_id,
        source="cninfo",
        operation_id=operation.operation_id,
        observation_key="observation-1",
        max_attempts=1,
    )
    claimed = repository.claim_attachment_retry(attachment_id, now=NOW)
    exhausted = classify_retry_failure(
        "provider timeout", attempt=int(claimed["attempt"]), config=config, now=NOW
    )
    item = repository.finish_attachment_retry(
        attachment_id,
        success=False,
        retryable=exhausted.retryable,
        error_code=exhausted.reason_code,
        failure_class=exhausted.failure_class.value,
        operator_action_required=exhausted.operator_action_required,
        consumes_retry_budget=exhausted.consumes_retry_budget,
        max_attempts=1,
    )

    assert item["status"] == "exhausted"
    assert repository.get_operation(operation.operation_id).status is OperationStatus.BLOCKED
    assert repository.get_operation(operation.operation_id).reason_code == "retry_exhausted"
    assert repository.enqueue_attachment_retry(
        attachment_id=attachment_id,
        source="cninfo",
        observation_key="observation-1",
    )["status"] == "exhausted"
    reopened = repository.enqueue_attachment_retry(
        attachment_id=attachment_id,
        source="cninfo",
        observation_key="observation-2",
    )
    assert reopened["status"] == "queued"
    assert reopened["attempt"] == 0

    storage_operation = _operation(repository, "attachment-storage")
    repository.enqueue_attachment_retry(
        attachment_id=attachment_id,
        source="cninfo",
        operation_id=storage_operation.operation_id,
        observation_key="observation-3",
        max_attempts=4,
    )
    claimed = repository.claim_attachment_retry(attachment_id, now=NOW)
    storage = classify_retry_failure(
        "storage reserve exceeded",
        attempt=int(claimed["attempt"]),
        config=RetryConfig(),
        now=NOW,
    )
    item = repository.finish_attachment_retry(
        attachment_id,
        success=False,
        retryable=storage.retryable,
        error_code=storage.reason_code,
        failure_class=storage.failure_class.value,
        operator_action_required=storage.operator_action_required,
        consumes_retry_budget=storage.consumes_retry_budget,
    )
    assert item["status"] == "blocked"
    assert item["attempt"] == 0
    assert repository.get_operation(storage_operation.operation_id).reason_code == (
        "storage_reserve_exceeded"
    )
    with pytest.raises(ValueError, match="requires an actor"):
        repository.enqueue_attachment_retry(
            attachment_id=attachment_id,
            source="cninfo",
            observation_key="observation-3",
            reopen_reason="audited_repair",
        )
    assert repository.enqueue_attachment_retry(
        attachment_id=attachment_id,
        source="cninfo",
        observation_key="observation-3",
        reopen_reason="audited_repair",
        repair_actor="operator-1",
    )["status"] == "queued"


def test_discovery_retry_is_due_bounded_and_parent_projected(tmp_path):
    repository = _repository(tmp_path)
    operation = _operation(repository, "discovery-exhausted")
    config = RetryConfig(max_attempts=2)
    claimed = repository.claim_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="retry-policy-v1",
        lease_owner="worker-1",
        lease_expires_at="2026-08-10T00:10:00+00:00",
        now=NOW,
        operation_id=operation.operation_id,
        observation_key="window-1",
        max_attempts=2,
    )
    transient = classify_retry_failure(
        "HTTP 503 temporarily unavailable",
        attempt=int(claimed["attempt"]),
        config=config,
        now=NOW,
    )
    item = _finish_discovery(
        repository, claimed, status=transient.status.value, decision=transient
    )
    assert item["status"] == "retryable"
    assert item["attempt"] == 1
    assert item["next_retry_at"] == NEXT_MINUTE
    with pytest.raises(DiscoveryRetryNotDueError):
        repository.claim_discovery_state(
            source="cninfo",
            exchange="SSE",
            category="annual_report",
            scope_key="market",
            config_fingerprint="retry-policy-v1",
            lease_owner="worker-2",
            lease_expires_at="2026-08-10T00:10:30+00:00",
            now="2026-08-10T00:00:30+00:00",
            operation_id=operation.operation_id,
            observation_key="window-1",
            max_attempts=2,
        )
    claimed = repository.claim_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="retry-policy-v1",
        lease_owner="worker-2",
        lease_expires_at="2026-08-10T00:11:00+00:00",
        now=NEXT_MINUTE,
        operation_id=operation.operation_id,
        observation_key="window-1",
        max_attempts=2,
    )
    exhausted = classify_retry_failure(
        "HTTP 503 temporarily unavailable",
        attempt=int(claimed["attempt"]),
        config=config,
        now=NEXT_MINUTE,
    )
    item = _finish_discovery(
        repository, claimed, status=exhausted.status.value, decision=exhausted
    )
    assert item["status"] == "exhausted"
    assert item["attempt"] == 2
    assert repository.get_operation(operation.operation_id).status is OperationStatus.BLOCKED
    assert repository.get_operation(operation.operation_id).reason_code == "retry_exhausted"
    with pytest.raises(DiscoveryRetryBlockedError):
        repository.claim_discovery_state(
            source="cninfo",
            exchange="SSE",
            category="annual_report",
            scope_key="market",
            config_fingerprint="retry-policy-v1",
            lease_owner="worker-3",
            lease_expires_at="2026-08-10T00:12:00+00:00",
            now="2026-08-10T00:02:00+00:00",
            observation_key="window-1",
            max_attempts=2,
        )
    reopened = repository.claim_discovery_state(
        source="cninfo",
        exchange="SSE",
        category="annual_report",
        scope_key="market",
        config_fingerprint="retry-policy-v1",
        lease_owner="worker-3",
        lease_expires_at="2026-08-10T00:12:00+00:00",
        now="2026-08-10T00:02:00+00:00",
        observation_key="window-2",
        max_attempts=2,
    )
    assert reopened["attempt"] == 1
    assert reopened["reopen_reason"] == "new_observation"


def test_discovery_operator_and_storage_blocks_have_distinct_budget_semantics(tmp_path):
    repository = _repository(tmp_path)
    config = RetryConfig(max_attempts=4)
    for suffix, observation, message, expected_attempt in (
        ("operator", "window-operator", "unsplittable dense window", 1),
        ("storage", "window-storage", "storage reserve exceeded", 0),
    ):
        operation = _operation(repository, f"discovery-{suffix}")
        claimed = repository.claim_discovery_state(
            source="cninfo",
            exchange="SSE",
            category="annual_report",
            scope_key="market",
            config_fingerprint="retry-policy-v1",
            lease_owner=f"worker-{suffix}",
            lease_expires_at="2026-08-10T00:10:00+00:00",
            now=NOW,
            operation_id=operation.operation_id,
            observation_key=observation,
            max_attempts=4,
        )
        decision = classify_retry_failure(
            message,
            attempt=int(claimed["attempt"]),
            config=config,
            now=NOW,
        )
        item = _finish_discovery(
            repository, claimed, status=decision.status.value, decision=decision
        )
        assert item["status"] == "blocked"
        assert item["attempt"] == expected_attempt
        assert repository.get_operation(operation.operation_id).status is (
            OperationStatus.BLOCKED
        )
        assert repository.get_operation(operation.operation_id).reason_code == (
            decision.reason_code
        )


def test_daily_discovery_exhaustion_projects_running_parent_as_blocked(tmp_path):
    repository = _repository(tmp_path)
    config = _daily_config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
    )
    operation = _operation(repository, "daily-discovery")
    claimed = repository.claim_operation(
        operation.operation_id,
        lease_owner="daily-worker",
        lease_expires_at="2099-01-01T00:00:00+00:00",
    )

    def unavailable(*_args):
        raise TimeoutError("provider timeout")

    result = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    ).run(
        run_cutoff=NOW,
        discover=unavailable,
        active_instrument_ids=("600000.SH",),
        operation_id=operation.operation_id,
        lease_owner="daily-worker",
        lease_generation=claimed.lease_generation,
    )

    parent = repository.get_operation(operation.operation_id)
    assert result.status == "blocked"
    assert parent is not None
    assert parent.status is OperationStatus.BLOCKED
    assert parent.reason_code == "retry_exhausted"
    states = repository.list_discovery_states(category="annual_report")
    assert states
    assert {state["status"] for state in states} == {"exhausted"}
