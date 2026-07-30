from datetime import datetime, timezone
from email.utils import format_datetime

import pytest

from utils.adaptive_throttle import (
    AdaptiveSourceThrottle,
    AdaptiveThrottlePolicy,
    AdaptiveThrottleRegistry,
)


class _FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.epoch = 1_800_000_000.0
        self.sleeps = []

    def monotonic(self) -> float:
        return self.now

    def wall_clock(self) -> float:
        return self.epoch + self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _policy(**overrides) -> AdaptiveThrottlePolicy:
    values = {
        "min_interval_seconds": 1.0,
        "max_interval_seconds": 8.0,
        "outcome_window_size": 4,
        "slowdown_density_threshold": 0.5,
        "recovery_density_threshold": 0.25,
        "slowdown_factor": 2.0,
        "recovery_factor": 0.5,
        "stable_successes_for_recovery": 2,
        "cooldown_stages_seconds": (5.0, 15.0, 30.0),
        "max_cooldown_seconds": 40.0,
        "jitter_ratio": 0.0,
    }
    values.update(overrides)
    return AdaptiveThrottlePolicy(**values)


def _throttle(
    fake_time: _FakeTime,
    *,
    policy: AdaptiveThrottlePolicy | None = None,
    random_value: float = 0.0,
) -> AdaptiveSourceThrottle:
    return AdaptiveSourceThrottle(
        "Example",
        policy or _policy(),
        clock=fake_time.monotonic,
        wall_clock=fake_time.wall_clock,
        sleep_func=fake_time.sleep,
        random_func=lambda: random_value,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"min_interval_seconds": 0},
        {"max_interval_seconds": 0.5},
        {"outcome_window_size": 1},
        {"slowdown_density_threshold": 0},
        {"recovery_density_threshold": 0.5},
        {"slowdown_factor": 1},
        {"recovery_factor": 1},
        {"stable_successes_for_recovery": 0},
        {"cooldown_stages_seconds": ()},
        {"cooldown_stages_seconds": (15, 5)},
        {"max_cooldown_seconds": 10},
        {"circuit_density_threshold": 0},
        {"circuit_minimum_outcomes": 1},
        {"circuit_interval_ratio": 0},
        {"circuit_cooldown_seconds": (120, 60)},
        {"jitter_ratio": 1.1},
    ],
)
def test_policy_rejects_invalid_bounds(override):
    with pytest.raises(ValueError):
        _policy(**override)


def test_policy_normalizes_cooldown_stages_to_immutable_tuple():
    policy = _policy(cooldown_stages_seconds=[5, 15, 30])

    assert policy.cooldown_stages_seconds == (5.0, 15.0, 30.0)
    assert isinstance(policy.cooldown_stages_seconds, tuple)


def test_consecutive_throttles_raise_interval_and_cooldown_stages():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)

    throttle.record_throttle(403)
    first = throttle.snapshot()
    throttle.record_throttle(429)
    second = throttle.snapshot()
    throttle.record_throttle(403)
    third = throttle.snapshot()

    assert first.current_interval_seconds == 2.0
    assert first.cooldown_remaining_seconds == 5.0
    assert second.current_interval_seconds == 4.0
    assert second.cooldown_remaining_seconds == 15.0
    assert third.current_interval_seconds == 8.0
    assert third.cooldown_remaining_seconds == 30.0
    assert third.throttle_count == 3
    assert third.cooldown_count == 3


def test_interspersed_throttles_keep_reducing_frequency_by_density():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)

    throttle.record_throttle(403)
    throttle.record_success()
    throttle.record_throttle(403)

    snapshot = throttle.snapshot()
    assert snapshot.consecutive_throttles == 1
    assert snapshot.throttle_density == pytest.approx(2 / 3)
    assert snapshot.current_interval_seconds == 4.0


def test_retry_after_and_non_negative_jitter_set_cooldown_floor():
    fake_time = _FakeTime()
    throttle = _throttle(
        fake_time,
        policy=_policy(jitter_ratio=0.2),
        random_value=0.5,
    )

    throttle.record_throttle(429, retry_after="20")

    assert throttle.snapshot().cooldown_remaining_seconds == pytest.approx(22.0)


def test_http_date_retry_after_is_supported_and_bounded():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)
    retry_at = datetime.fromtimestamp(
        fake_time.wall_clock() + 20,
        tz=timezone.utc,
    )

    throttle.record_throttle(429, retry_after=format_datetime(retry_at))

    assert throttle.snapshot().cooldown_remaining_seconds == pytest.approx(20.0)


def test_admission_waits_for_cooldown_then_active_interval():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)
    throttle.record_throttle(403)

    first_delay = throttle.wait_before_request()
    second_delay = throttle.wait_before_request()

    assert first_delay == pytest.approx(5.0)
    assert second_delay == pytest.approx(2.0)
    assert fake_time.sleeps == pytest.approx([5.0, 2.0])
    assert throttle.snapshot().wait_count == 2


def test_stable_successes_recover_gradually_after_density_clears():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)
    throttle.record_throttle(403)

    throttle.record_success()
    assert throttle.snapshot().current_interval_seconds == 2.0
    throttle.record_success()
    assert throttle.snapshot().current_interval_seconds == 2.0
    throttle.record_success()

    recovered = throttle.snapshot()
    assert recovered.current_interval_seconds == 1.0
    assert recovered.recovery_count == 1
    assert recovered.stable_successes == 0


def test_non_throttle_failure_does_not_change_throttle_density():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)
    throttle.record_throttle(403)
    before = throttle.snapshot()

    throttle.record_failure()

    after = throttle.snapshot()
    assert after.throttle_density == before.throttle_density
    assert after.failure_count == 1
    assert after.stable_successes == 0


def test_registry_shares_same_key_and_isolates_other_sources():
    registry = AdaptiveThrottleRegistry()
    policy = _policy()

    first = registry.get("CNInfo", policy)
    same = registry.get("cninfo", policy)
    other = registry.get("sse", policy)
    first.record_throttle(403)

    assert same is first
    assert other is not first
    assert same.snapshot().throttle_count == 1
    assert other.snapshot().throttle_count == 0


def test_registry_rejects_conflicting_policy_for_existing_source():
    registry = AdaptiveThrottleRegistry()
    registry.get("cninfo", _policy())

    with pytest.raises(ValueError, match="policy conflict"):
        registry.get("cninfo", _policy(max_interval_seconds=10))


def test_dense_interspersed_throttles_open_long_shared_circuit():
    fake_time = _FakeTime()
    throttle = _throttle(
        fake_time,
        policy=_policy(
            circuit_density_threshold=0.5,
            circuit_minimum_outcomes=4,
            circuit_cooldown_seconds=(60, 120),
        ),
        random_value=0.5,
    )
    throttle.record_throttle(403)
    throttle.record_success()
    throttle.record_throttle(429)
    throttle.record_success()
    throttle.record_throttle(403)

    opened = throttle.snapshot()
    assert opened.circuit_open is True
    assert opened.circuit_remaining_seconds == pytest.approx(90)
    assert opened.circuit_trip_count == 1
    assert opened.http_403_count == 2
    assert opened.http_429_count == 1

    waited = throttle.wait_before_request()
    assert waited == pytest.approx(90)
    assert throttle.snapshot().circuit_wait_seconds == pytest.approx(90)


def test_admission_rechecks_circuit_opened_while_waiting():
    fake_time = _FakeTime()
    throttle = None
    sleep_calls = 0

    def sleep_with_concurrent_outcomes(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            fake_time.now += 40
            for _ in range(3):
                throttle.record_success()
            throttle.record_throttle(403)
            throttle.record_throttle(403)
            fake_time.now += seconds - 40
        else:
            fake_time.sleep(seconds)

    throttle = AdaptiveSourceThrottle(
        "Example",
        _policy(
            circuit_density_threshold=0.5,
            circuit_minimum_outcomes=4,
            circuit_cooldown_seconds=(60, 60),
        ),
        clock=fake_time.monotonic,
        wall_clock=fake_time.wall_clock,
        sleep_func=sleep_with_concurrent_outcomes,
        random_func=lambda: 0.0,
    )
    throttle.record_throttle(403)
    throttle.record_success()
    throttle.record_throttle(403)
    throttle.record_success()
    throttle.record_throttle(403)

    waited = throttle.wait_before_request()

    assert waited == pytest.approx(100)
    assert fake_time.sleeps == pytest.approx([40])


def test_one_success_does_not_close_circuit_and_recovery_is_gradual():
    fake_time = _FakeTime()
    throttle = _throttle(
        fake_time,
        policy=_policy(
            circuit_density_threshold=0.5,
            circuit_minimum_outcomes=4,
            circuit_cooldown_seconds=(60, 60),
        ),
    )
    throttle.record_throttle(403)
    throttle.record_success()
    throttle.record_throttle(403)
    throttle.record_success()
    throttle.record_throttle(403)
    throttle.wait_before_request()

    throttle.record_success()
    first_success = throttle.snapshot()
    assert first_success.circuit_open is True
    assert first_success.current_interval_seconds == 8

    throttle.record_success()
    recovered = throttle.snapshot()
    assert recovered.circuit_open is False
    assert recovered.current_interval_seconds == 4
    assert recovered.recovery_count == 1


def test_isolated_throttle_keeps_short_cooldown_without_circuit():
    fake_time = _FakeTime()
    throttle = _throttle(fake_time)

    throttle.record_throttle(403)

    snapshot = throttle.snapshot()
    assert snapshot.cooldown_remaining_seconds == 5
    assert snapshot.circuit_open is False
    assert snapshot.circuit_trip_count == 0
