"""Thread-safe adaptive pacing for source-scoped synchronous requests."""

from __future__ import annotations

import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Deque, Dict, Optional, Union


LOGGER = logging.getLogger(__name__)
RetryAfterValue = Union[str, int, float]


@dataclass(frozen=True)
class AdaptiveThrottlePolicy:
    """Validated bounds and transition rules for one upstream source."""

    min_interval_seconds: float = 0.5
    max_interval_seconds: float = 8.0
    outcome_window_size: int = 20
    slowdown_density_threshold: float = 0.2
    recovery_density_threshold: float = 0.1
    slowdown_factor: float = 1.75
    recovery_factor: float = 0.8
    stable_successes_for_recovery: int = 8
    cooldown_stages_seconds: tuple[float, ...] = (5.0, 15.0, 30.0)
    max_cooldown_seconds: float = 60.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.min_interval_seconds)
            or self.min_interval_seconds <= 0
        ):
            raise ValueError("min_interval_seconds must be finite and positive")
        if (
            not math.isfinite(self.max_interval_seconds)
            or self.max_interval_seconds < self.min_interval_seconds
        ):
            raise ValueError(
                "max_interval_seconds must be finite and at least the minimum"
            )
        if self.outcome_window_size < 2:
            raise ValueError("outcome_window_size must be at least 2")
        if not 0 < self.slowdown_density_threshold <= 1:
            raise ValueError("slowdown_density_threshold must be in (0, 1]")
        if not 0 <= self.recovery_density_threshold < self.slowdown_density_threshold:
            raise ValueError(
                "recovery_density_threshold must be non-negative and below "
                "slowdown_density_threshold"
            )
        if not math.isfinite(self.slowdown_factor) or self.slowdown_factor <= 1:
            raise ValueError("slowdown_factor must be finite and greater than 1")
        if not math.isfinite(self.recovery_factor) or not 0 < self.recovery_factor < 1:
            raise ValueError("recovery_factor must be finite and in (0, 1)")
        if self.stable_successes_for_recovery < 1:
            raise ValueError("stable_successes_for_recovery must be positive")
        if not self.cooldown_stages_seconds:
            raise ValueError("cooldown_stages_seconds must not be empty")
        stages = tuple(float(value) for value in self.cooldown_stages_seconds)
        if any(not math.isfinite(value) or value <= 0 for value in stages):
            raise ValueError("cooldown stages must be finite and positive")
        if any(current < previous for previous, current in zip(stages, stages[1:])):
            raise ValueError("cooldown stages must be ordered from low to high")
        object.__setattr__(self, "cooldown_stages_seconds", stages)
        if (
            not math.isfinite(self.max_cooldown_seconds)
            or self.max_cooldown_seconds < stages[-1]
        ):
            raise ValueError(
                "max_cooldown_seconds must be finite and cover every cooldown stage"
            )
        if not math.isfinite(self.jitter_ratio) or not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be finite and in [0, 1]")


@dataclass(frozen=True)
class AdaptiveThrottleSnapshot:
    """Consistent observable state for one source throttle."""

    source_key: str
    current_interval_seconds: float
    cooldown_remaining_seconds: float
    throttle_density: float
    consecutive_throttles: int
    stable_successes: int
    wait_count: int
    total_wait_seconds: float
    throttle_count: int
    cooldown_count: int
    recovery_count: int
    failure_count: int


class AdaptiveSourceThrottle:
    """Coordinate pacing and adaptive cooldowns for one logical source."""

    def __init__(
        self,
        source_key: str,
        policy: Optional[AdaptiveThrottlePolicy] = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        sleep_func: Callable[[float], None] = time.sleep,
        random_func: Callable[[], float] = random.random,
    ) -> None:
        normalized_key = str(source_key or "").strip().lower()
        if not normalized_key:
            raise ValueError("source_key must not be empty")
        self.source_key = normalized_key
        self.policy = policy or AdaptiveThrottlePolicy()
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleep = sleep_func
        self._random = random_func
        self._lock = threading.Lock()
        self._outcomes: Deque[bool] = deque(maxlen=self.policy.outcome_window_size)
        self._current_interval = self.policy.min_interval_seconds
        self._next_admission_at = 0.0
        self._cooldown_until = 0.0
        self._cooldown_version = 0
        self._cooldown_stage = 0
        self._consecutive_throttles = 0
        self._stable_successes = 0
        self._wait_count = 0
        self._total_wait_seconds = 0.0
        self._throttle_count = 0
        self._cooldown_count = 0
        self._recovery_count = 0
        self._failure_count = 0

    def wait_before_request(self) -> float:
        """Reserve and wait for one request admission, returning total delay."""
        total_delay = 0.0
        while True:
            with self._lock:
                now = self._clock()
                admission_at = max(
                    now,
                    self._next_admission_at,
                    self._cooldown_until,
                )
                delay = max(0.0, admission_at - now)
                self._next_admission_at = admission_at + self._current_interval
                cooldown_version = self._cooldown_version
                if delay > 0:
                    self._wait_count += 1
                    self._total_wait_seconds += delay
            if delay <= 0:
                return total_delay

            self._sleep(delay)
            total_delay += delay
            with self._lock:
                now = self._clock()
                cooldown_changed = self._cooldown_version != cooldown_version
                cooldown_active = self._cooldown_until > now
            if not (cooldown_changed and cooldown_active):
                return total_delay

    def record_success(self) -> None:
        """Record a stable HTTP response and recover by at most one step."""
        transition: Optional[tuple[float, float]] = None
        with self._lock:
            self._outcomes.append(False)
            self._consecutive_throttles = 0
            self._cooldown_stage = 0
            self._stable_successes += 1
            density = self._throttle_density_locked()
            if (
                self._stable_successes >= self.policy.stable_successes_for_recovery
                and density <= self.policy.recovery_density_threshold
                and self._current_interval > self.policy.min_interval_seconds
            ):
                previous = self._current_interval
                self._current_interval = max(
                    self.policy.min_interval_seconds,
                    self._current_interval * self.policy.recovery_factor,
                )
                self._stable_successes = 0
                self._recovery_count += 1
                transition = (previous, self._current_interval)
        if transition is not None:
            LOGGER.info(
                "[AdaptiveThrottle] Source recovering: source=%s interval=%.3f->%.3f",
                self.source_key,
                transition[0],
                transition[1],
            )

    def record_throttle(
        self,
        status_code: int,
        *,
        retry_after: Optional[RetryAfterValue] = None,
    ) -> None:
        """Record an HTTP 403/429 and apply adaptive pacing and cooldown."""
        if int(status_code) not in {403, 429}:
            raise ValueError("record_throttle only accepts HTTP 403 or 429")
        retry_after_seconds = self._parse_retry_after(retry_after)
        transition: Optional[tuple[float, float, float, int, float]] = None
        with self._lock:
            now = self._clock()
            self._outcomes.append(True)
            self._consecutive_throttles += 1
            self._stable_successes = 0
            self._throttle_count += 1
            density = self._throttle_density_locked()

            previous_interval = self._current_interval
            if (
                density >= self.policy.slowdown_density_threshold
                or self._consecutive_throttles > 1
            ):
                self._current_interval = min(
                    self.policy.max_interval_seconds,
                    max(
                        self.policy.min_interval_seconds,
                        self._current_interval * self.policy.slowdown_factor,
                    ),
                )

            stage = min(
                self._consecutive_throttles,
                len(self.policy.cooldown_stages_seconds),
            )
            stage_seconds = self.policy.cooldown_stages_seconds[stage - 1]
            cooldown_floor = max(stage_seconds, retry_after_seconds or 0.0)
            jittered_cooldown = min(
                self.policy.max_cooldown_seconds,
                cooldown_floor
                * (1.0 + self.policy.jitter_ratio * self._bounded_random()),
            )
            new_cooldown_until = max(
                self._cooldown_until,
                now + jittered_cooldown,
            )
            cooldown_extended = new_cooldown_until > self._cooldown_until
            stage_changed = stage != self._cooldown_stage
            self._cooldown_stage = stage
            if cooldown_extended:
                self._cooldown_until = new_cooldown_until
                self._cooldown_version += 1
                self._cooldown_count += 1
            if (
                self._current_interval != previous_interval
                or stage_changed
                or retry_after_seconds is not None
            ):
                transition = (
                    previous_interval,
                    self._current_interval,
                    max(0.0, self._cooldown_until - now),
                    stage,
                    density,
                )
        if transition is not None:
            LOGGER.warning(
                "[AdaptiveThrottle] Source throttled: source=%s status=%s "
                "interval=%.3f->%.3f cooldown=%.3f stage=%s density=%.3f",
                self.source_key,
                int(status_code),
                transition[0],
                transition[1],
                transition[2],
                transition[3],
                transition[4],
            )

    def record_failure(self) -> None:
        """Reset stable recovery without treating a failure as anti-crawl evidence."""
        with self._lock:
            self._stable_successes = 0
            self._failure_count += 1

    def snapshot(self) -> AdaptiveThrottleSnapshot:
        """Return an atomic state snapshot."""
        with self._lock:
            return AdaptiveThrottleSnapshot(
                source_key=self.source_key,
                current_interval_seconds=self._current_interval,
                cooldown_remaining_seconds=max(
                    0.0,
                    self._cooldown_until - self._clock(),
                ),
                throttle_density=self._throttle_density_locked(),
                consecutive_throttles=self._consecutive_throttles,
                stable_successes=self._stable_successes,
                wait_count=self._wait_count,
                total_wait_seconds=self._total_wait_seconds,
                throttle_count=self._throttle_count,
                cooldown_count=self._cooldown_count,
                recovery_count=self._recovery_count,
                failure_count=self._failure_count,
            )

    def _throttle_density_locked(self) -> float:
        if not self._outcomes:
            return 0.0
        return sum(self._outcomes) / len(self._outcomes)

    def _bounded_random(self) -> float:
        try:
            value = float(self._random())
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(value):
            return 0.0
        return min(1.0, max(0.0, value))

    def _parse_retry_after(
        self,
        value: Optional[RetryAfterValue],
    ) -> Optional[float]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            seconds = float(text)
        except ValueError:
            try:
                parsed = parsedate_to_datetime(text)
            except (TypeError, ValueError, OverflowError):
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            seconds = parsed.timestamp() - self._wall_clock()
        if not math.isfinite(seconds) or seconds < 0:
            return None
        return min(seconds, self.policy.max_cooldown_seconds)


class AdaptiveThrottleRegistry:
    """Own process-local adaptive throttle instances by source key."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._throttles: Dict[str, AdaptiveSourceThrottle] = {}

    def get(
        self,
        source_key: str,
        policy: Optional[AdaptiveThrottlePolicy] = None,
    ) -> AdaptiveSourceThrottle:
        """Return a stable source instance and reject conflicting policy reuse."""
        normalized_key = str(source_key or "").strip().lower()
        if not normalized_key:
            raise ValueError("source_key must not be empty")
        requested_policy = policy or AdaptiveThrottlePolicy()
        with self._lock:
            existing = self._throttles.get(normalized_key)
            if existing is not None:
                if existing.policy != requested_policy:
                    raise ValueError(
                        f"adaptive throttle policy conflict for source={normalized_key}"
                    )
                return existing
            throttle = AdaptiveSourceThrottle(
                normalized_key,
                requested_policy,
            )
            self._throttles[normalized_key] = throttle
            return throttle

    def clear(self) -> None:
        """Clear process-local instances, primarily for isolated tests."""
        with self._lock:
            self._throttles.clear()


_GLOBAL_REGISTRY = AdaptiveThrottleRegistry()


def get_adaptive_source_throttle(
    source_key: str,
    policy: Optional[AdaptiveThrottlePolicy] = None,
) -> AdaptiveSourceThrottle:
    """Return the process-shared adaptive throttle for one source."""
    return _GLOBAL_REGISTRY.get(source_key, policy)
