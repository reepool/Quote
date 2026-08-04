from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from research.temporal_data_availability import (
    AvailabilityEvidence,
    ReleaseException,
    ReleaseExceptionKind,
    ReleaseLifecycleStatus,
    ReleasePlan,
    evaluate_release,
    is_point_in_time_eligible,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


def _at(day: int, hour: int = 10) -> datetime:
    return datetime(2026, 8, day, hour, tzinfo=SHANGHAI)


def _plan() -> ReleasePlan:
    return ReleasePlan(
        observation_start=date(2026, 7, 21),
        observation_end=date(2026, 7, 30),
        expected_release_at=_at(4),
        grace_deadline_at=_at(4) + timedelta(hours=6),
    )


def test_temporal_contract_rejects_naive_datetimes():
    with pytest.raises(ValueError, match="timezone-aware"):
        ReleasePlan(
            observation_start=date(2026, 7, 21),
            observation_end=date(2026, 7, 30),
            expected_release_at=datetime(2026, 8, 4, 10),
            grace_deadline_at=_at(4, 16),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        is_point_in_time_eligible(_at(4), cutoff=datetime(2026, 8, 4, 10))


@pytest.mark.parametrize(
    ("evaluated_at", "expected_status"),
    [
        (_at(3), ReleaseLifecycleStatus.NOT_DUE),
        (_at(4, 12), ReleaseLifecycleStatus.DUE_IN_GRACE),
        (_at(5), ReleaseLifecycleStatus.UNRESOLVED_GAP),
    ],
)
def test_release_lifecycle_tracks_due_and_grace(evaluated_at, expected_status):
    decision = evaluate_release(_plan(), evaluated_at=evaluated_at)
    assert decision.status == expected_status
    assert decision.available_at is None


def test_release_lifecycle_preserves_actual_and_delayed_availability():
    on_time = evaluate_release(
        _plan(),
        evaluated_at=_at(4, 12),
        evidence=AvailabilityEvidence(actual_published_at=_at(4, 11)),
    )
    delayed = evaluate_release(
        _plan(),
        evaluated_at=_at(5),
        evidence=AvailabilityEvidence(
            first_seen_at=_at(5),
            quality="local_first_seen_timestamp",
        ),
    )
    assert on_time.status == ReleaseLifecycleStatus.AVAILABLE
    assert on_time.availability_quality == "actual_publication_timestamp"
    assert delayed.status == ReleaseLifecycleStatus.DELAYED_AVAILABLE
    assert delayed.available_at == _at(5)


def test_release_exceptions_require_evidence_and_govern_schedule():
    with pytest.raises(ValueError, match="reason and evidence_url"):
        ReleaseException(
            kind=ReleaseExceptionKind.CANCELLED,
            reason="",
            evidence_url="",
        )
    cancelled = evaluate_release(
        _plan(),
        evaluated_at=_at(5),
        exception=ReleaseException(
            kind=ReleaseExceptionKind.CANCELLED,
            reason="official cancellation",
            evidence_url="https://example.test/cancel",
        ),
    )
    replacement = _at(6)
    rescheduled = evaluate_release(
        _plan(),
        evaluated_at=_at(5),
        exception=ReleaseException(
            kind=ReleaseExceptionKind.RESCHEDULED,
            reason="official reschedule",
            evidence_url="https://example.test/reschedule",
            replacement_release_at=replacement,
        ),
    )
    assert cancelled.status == ReleaseLifecycleStatus.CANCELLED
    assert rescheduled.status == ReleaseLifecycleStatus.RESCHEDULED
    assert rescheduled.expected_release_at == replacement


def test_source_failure_is_distinct_from_missing_release():
    decision = evaluate_release(
        _plan(),
        evaluated_at=_at(5),
        evidence=AvailabilityEvidence(source_failure="official search rejected"),
    )
    assert decision.status == ReleaseLifecycleStatus.SOURCE_FAILURE


def test_point_in_time_eligibility_fails_closed():
    cutoff = _at(4, 12)
    assert is_point_in_time_eligible(_at(4, 11), cutoff=cutoff)
    assert not is_point_in_time_eligible(_at(4, 13), cutoff=cutoff)
    assert not is_point_in_time_eligible(None, cutoff=cutoff)
