"""Business-neutral work orchestration models for LLM-assisted pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional


class OutcomeStatus(str, Enum):
    SUCCESS = "success"
    SKIPPED_IDEMPOTENT = "skipped_idempotent"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"
    CANCELLED = "cancelled"
    DEADLINE_EXCEEDED = "deadline_exceeded"


class OrchestrationError(RuntimeError):
    """Base error for local work orchestration failures."""


class StageQueueClosedError(OrchestrationError):
    """Raised when a closed stage queue cannot accept or return more work."""


class ResourceLeaseError(OrchestrationError):
    """Raised when a stage attempts an unsafe nested resource acquisition."""


@dataclass(frozen=True)
class WorkItem:
    work_id: str
    workload: str
    run_id: str
    business_item_key: str
    stage: str
    stage_sequence: int = 0
    attempt: int = 1
    idempotency_key: Optional[str] = None
    payload_ref: Optional[str] = None
    payload: Any = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def next_stage(
        self,
        stage: str,
        *,
        payload: Any = None,
        payload_ref: Optional[str] = None,
    ) -> "WorkItem":
        return WorkItem(
            work_id=self.work_id,
            workload=self.workload,
            run_id=self.run_id,
            business_item_key=self.business_item_key,
            stage=stage,
            stage_sequence=self.stage_sequence + 1,
            attempt=1,
            idempotency_key=self.idempotency_key,
            payload_ref=payload_ref,
            payload=payload,
            metadata=self.metadata,
        )


@dataclass(frozen=True)
class StageOutcome:
    item: WorkItem
    status: OutcomeStatus
    output: Any = None
    output_ref: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retryable: bool = False
    queue_wait_ms: int = 0
    execution_ms: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderSnapshot:
    resource_name: str
    active: int
    active_bulk: int
    active_by_workload: Mapping[str, int]
    waiting: int
    waiting_by_workload: Mapping[str, int]
    admitted: int
    admitted_by_workload: Mapping[str, int]
    completed: int
    completed_by_workload: Mapping[str, int]
    cancelled: int
    deadline_exceeded: int
    cooldown_remaining_seconds: float
    total_admission_wait_ms: int


@dataclass(frozen=True)
class ResourceSnapshot:
    resource_name: str
    limit: int
    active: int
    waiting: int
    acquired: int
    released: int


@dataclass(frozen=True)
class StageSnapshot:
    stage: str
    queue_depth: int
    active: int
    succeeded: int
    skipped: int
    retryable_failed: int
    terminal_failed: int
    cancelled: int
    deadline_exceeded: int
    total_queue_wait_ms: int
    total_execution_ms: int
