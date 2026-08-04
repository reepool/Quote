"""Shared point-in-time availability contracts for source-published datasets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional


class ReleaseLifecycleStatus(str, Enum):
    NOT_DUE = "not_due"
    DUE_IN_GRACE = "due_in_grace"
    AVAILABLE = "available"
    DELAYED_AVAILABLE = "delayed_available"
    CANCELLED = "cancelled"
    RESCHEDULED = "rescheduled"
    UNRESOLVED_GAP = "unresolved_gap"
    SOURCE_FAILURE = "source_failure"


class ReleaseExceptionKind(str, Enum):
    CANCELLED = "cancelled"
    RESCHEDULED = "rescheduled"


def require_aware(value: datetime, *, field_name: str) -> datetime:
    """Reject host-timezone-dependent datetimes at domain boundaries."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


@dataclass(frozen=True)
class ReleasePlan:
    observation_start: date
    observation_end: date
    expected_release_at: datetime
    grace_deadline_at: datetime

    def __post_init__(self) -> None:
        require_aware(self.expected_release_at, field_name="expected_release_at")
        require_aware(self.grace_deadline_at, field_name="grace_deadline_at")
        if self.observation_start > self.observation_end:
            raise ValueError("observation_start must not follow observation_end")
        if self.grace_deadline_at < self.expected_release_at:
            raise ValueError("grace_deadline_at must not precede expected_release_at")


@dataclass(frozen=True)
class ReleaseException:
    kind: ReleaseExceptionKind
    reason: str
    evidence_url: str
    replacement_release_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if not self.reason.strip() or not self.evidence_url.strip():
            raise ValueError("release exception requires reason and evidence_url")
        if self.kind == ReleaseExceptionKind.RESCHEDULED:
            if self.replacement_release_at is None:
                raise ValueError("rescheduled release requires replacement_release_at")
            require_aware(
                self.replacement_release_at,
                field_name="replacement_release_at",
            )
        elif self.replacement_release_at is not None:
            raise ValueError("cancelled release must not define replacement_release_at")


@dataclass(frozen=True)
class AvailabilityEvidence:
    actual_published_at: Optional[datetime] = None
    first_seen_at: Optional[datetime] = None
    quality: Optional[str] = None
    evidence_url: Optional[str] = None
    source_failure: Optional[str] = None

    def __post_init__(self) -> None:
        if self.actual_published_at is not None:
            require_aware(
                self.actual_published_at,
                field_name="actual_published_at",
            )
        if self.first_seen_at is not None:
            require_aware(self.first_seen_at, field_name="first_seen_at")

    @property
    def available_at(self) -> Optional[datetime]:
        if self.actual_published_at is not None:
            return self.actual_published_at
        return self.first_seen_at

    @property
    def availability_quality(self) -> Optional[str]:
        if self.quality:
            return self.quality
        if self.actual_published_at is not None:
            return "actual_publication_timestamp"
        if self.first_seen_at is not None:
            return "local_first_seen_timestamp"
        return None


@dataclass(frozen=True)
class AvailabilityDecision:
    status: ReleaseLifecycleStatus
    expected_release_at: datetime
    grace_deadline_at: datetime
    available_at: Optional[datetime]
    availability_quality: Optional[str]


def evaluate_release(
    plan: ReleasePlan,
    *,
    evaluated_at: datetime,
    evidence: Optional[AvailabilityEvidence] = None,
    exception: Optional[ReleaseException] = None,
) -> AvailabilityDecision:
    """Evaluate one release deterministically at a timezone-aware instant."""
    require_aware(evaluated_at, field_name="evaluated_at")
    evidence = evidence or AvailabilityEvidence()
    expected_release_at = plan.expected_release_at
    grace_deadline_at = plan.grace_deadline_at

    if exception is not None and exception.kind == ReleaseExceptionKind.CANCELLED:
        return AvailabilityDecision(
            status=ReleaseLifecycleStatus.CANCELLED,
            expected_release_at=expected_release_at,
            grace_deadline_at=grace_deadline_at,
            available_at=None,
            availability_quality=None,
        )

    rescheduled = exception is not None
    if rescheduled:
        assert exception is not None and exception.replacement_release_at is not None
        grace_duration = plan.grace_deadline_at - plan.expected_release_at
        expected_release_at = exception.replacement_release_at
        grace_deadline_at = expected_release_at + grace_duration

    available_at = evidence.available_at
    if available_at is not None:
        status = (
            ReleaseLifecycleStatus.DELAYED_AVAILABLE
            if available_at > grace_deadline_at
            else ReleaseLifecycleStatus.AVAILABLE
        )
    elif evidence.source_failure:
        status = ReleaseLifecycleStatus.SOURCE_FAILURE
    elif evaluated_at < expected_release_at:
        status = (
            ReleaseLifecycleStatus.RESCHEDULED
            if rescheduled
            else ReleaseLifecycleStatus.NOT_DUE
        )
    elif evaluated_at <= grace_deadline_at:
        status = (
            ReleaseLifecycleStatus.RESCHEDULED
            if rescheduled
            else ReleaseLifecycleStatus.DUE_IN_GRACE
        )
    else:
        status = ReleaseLifecycleStatus.UNRESOLVED_GAP

    return AvailabilityDecision(
        status=status,
        expected_release_at=expected_release_at,
        grace_deadline_at=grace_deadline_at,
        available_at=available_at,
        availability_quality=evidence.availability_quality,
    )


def is_point_in_time_eligible(
    available_at: Optional[datetime],
    *,
    cutoff: datetime,
) -> bool:
    """Fail closed when governed availability is absent or after the cutoff."""
    require_aware(cutoff, field_name="cutoff")
    if available_at is None:
        return False
    require_aware(available_at, field_name="available_at")
    return available_at <= cutoff
