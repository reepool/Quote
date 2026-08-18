"""Deterministic retry classification for announcement-asset work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from .config import RetryConfig


class RetryFailureClass(str, Enum):
    """Stable failure classes persisted independently from raw diagnostics."""

    TRANSIENT = "transient"
    OPERATOR_ACTION = "operator_action"
    STORAGE_BLOCKED = "storage_blocked"


class RetryQueueStatus(str, Enum):
    """Durable queue outcomes used by attachment and repair workers."""

    RETRYABLE = "retryable"
    BLOCKED = "blocked"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True)
class RetryDecision:
    failure_class: RetryFailureClass
    status: RetryQueueStatus
    reason_code: str
    next_retry_at: str | None
    consumes_retry_budget: bool
    operator_action_required: bool

    @property
    def retryable(self) -> bool:
        return self.status is RetryQueueStatus.RETRYABLE


_STORAGE_BLOCK_PATTERNS = (
    "storage_reserve",
    "storage reserve",
    "hard reserve",
    "free-space reserve",
    "hard utilization",
    "reservation cannot be expanded",
    "storage reservation is blocked",
)

_OPERATOR_PATTERNS = (
    "identity_conflict",
    "identity conflict",
    "unsafe_path",
    "unsafe path",
    "path escapes",
    "not_pdf",
    "not pdf",
    "pdf signature",
    "persistent_hash",
    "persistent hash",
    "hash mismatch",
    "length mismatch",
    "content length mismatch",
    "candidate_ambiguous",
    "ambiguous",
    "unsplittable",
    "sidecar evidence is invalid",
    "mount source mismatch",
    "not a dedicated mounted filesystem",
)

_TRANSIENT_PATTERNS = (
    "timeout",
    "timed out",
    "rate_limit",
    "rate limit",
    "http_429",
    "http 429",
    "http_500",
    "http_502",
    "http_503",
    "http_504",
    "http 5",
    "temporarily unavailable",
    "temporary unavailable",
    "temporary_provider",
    "transient provider",
    "provider_unavailable",
    "connectionerror",
    "connection error",
    "connection reset",
    "archive_mount_unavailable",
    "nas unavailable",
)


def classify_retry_failure(
    error: BaseException | str,
    *,
    attempt: int,
    config: RetryConfig,
    now: datetime | str | None = None,
    failure_class: RetryFailureClass | str | None = None,
) -> RetryDecision:
    """Classify one failure and calculate a bounded durable retry decision.

    Unknown failures fail closed as operator-action items. Callers may supply an
    explicit class when the provider/transport already exposes structured error
    semantics; the persisted redacted reason still comes from this function.
    """

    current_attempt = max(0, int(attempt))
    resolved_class = (
        RetryFailureClass(failure_class)
        if failure_class is not None
        else _classify_text(_error_text(error))
    )
    if resolved_class is RetryFailureClass.STORAGE_BLOCKED:
        return RetryDecision(
            failure_class=resolved_class,
            status=RetryQueueStatus.BLOCKED,
            reason_code="storage_reserve_exceeded",
            next_retry_at=None,
            consumes_retry_budget=False,
            operator_action_required=True,
        )
    if resolved_class is RetryFailureClass.OPERATOR_ACTION:
        return RetryDecision(
            failure_class=resolved_class,
            status=RetryQueueStatus.BLOCKED,
            reason_code=_operator_reason(_error_text(error)),
            next_retry_at=None,
            consumes_retry_budget=True,
            operator_action_required=True,
        )
    if current_attempt >= config.max_attempts:
        return RetryDecision(
            failure_class=resolved_class,
            status=RetryQueueStatus.EXHAUSTED,
            reason_code="retry_attempts_exhausted",
            next_retry_at=None,
            consumes_retry_budget=True,
            operator_action_required=True,
        )
    timestamp = _parse_time(now)
    delay = min(
        config.max_backoff_seconds,
        config.initial_backoff_seconds * 2 ** max(0, current_attempt - 1),
    )
    return RetryDecision(
        failure_class=resolved_class,
        status=RetryQueueStatus.RETRYABLE,
        reason_code="transient_failure",
        next_retry_at=(timestamp + timedelta(seconds=delay)).isoformat(),
        consumes_retry_budget=True,
        operator_action_required=False,
    )


def _classify_text(text: str) -> RetryFailureClass:
    lowered = text.lower()
    if any(pattern in lowered for pattern in _STORAGE_BLOCK_PATTERNS):
        return RetryFailureClass.STORAGE_BLOCKED
    if any(pattern in lowered for pattern in _OPERATOR_PATTERNS):
        return RetryFailureClass.OPERATOR_ACTION
    if any(pattern in lowered for pattern in _TRANSIENT_PATTERNS):
        return RetryFailureClass.TRANSIENT
    return RetryFailureClass.OPERATOR_ACTION


def _operator_reason(text: str) -> str:
    lowered = text.lower()
    if "unsplittable" in lowered:
        return "unsplittable_window"
    if "ambiguous" in lowered:
        return "candidate_ambiguous"
    if "hash" in lowered:
        return "persistent_hash_mismatch"
    if "length" in lowered:
        return "persistent_length_mismatch"
    if "pdf" in lowered:
        return "invalid_pdf"
    if "path" in lowered:
        return "unsafe_path"
    if "identity" in lowered:
        return "identity_conflict"
    if "sidecar" in lowered:
        return "artifact_sidecar_invalid"
    return "operator_action_required"


def _error_text(error: BaseException | str) -> str:
    if isinstance(error, BaseException):
        return f"{type(error).__name__}:{error}"
    return str(error or "unknown_failure")


def _parse_time(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def retry_decision_payload(decision: RetryDecision) -> dict[str, Any]:
    """Return the stable persistence projection for a retry decision."""

    return {
        "failure_class": decision.failure_class.value,
        "status": decision.status.value,
        "reason_code": decision.reason_code,
        "next_retry_at": decision.next_retry_at,
        "consumes_retry_budget": decision.consumes_retry_budget,
        "operator_action_required": decision.operator_action_required,
    }
