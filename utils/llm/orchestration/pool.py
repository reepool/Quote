"""Process-local weighted LLM pool admission and source coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import hashlib
import json
import logging
import time
from typing import Any, Callable, Deque, Mapping, Optional, Sequence

from ..errors import (
    LlmConfigurationError,
    LlmDeadlineExceededError,
    LlmProviderError,
    LlmRateLimitError,
)
from ..models import LlmConfig, LlmPoolConfig, LlmPoolMember


LOGGER = logging.getLogger("LLM")


@dataclass(frozen=True)
class LlmPoolSelection:
    pool: str
    logical_profile: str
    source_label: str
    selected_profile: str
    weight: int
    borrowed: bool
    half_open_probe: bool


@dataclass(frozen=True)
class LlmPoolMemberSnapshot:
    source_label: str
    weight: int
    configured_max_concurrency: int
    effective_max_concurrency: int
    active: int
    waiting: int
    dispatches: int
    dispatch_ratio: float
    borrowed_dispatches: int
    successes: int
    failures: int
    rate_limits: int
    provider_5xx: int
    timeouts: int
    parse_failures: int
    schema_failures: int
    circuit_state: str
    consecutive_failures: int
    cooldown_remaining_seconds: float
    half_open_active_probes: int


@dataclass(frozen=True)
class LlmPoolSnapshot:
    name: str
    identity: str
    configured_total_concurrency: int
    effective_total_concurrency: int
    active_bottleneck: str
    active: int
    waiting: int
    oldest_wait_seconds: float
    admitted: int
    completed: int
    cancelled: int
    deadline_exceeded: int
    queue_full: int
    failover_requested: int
    failover_succeeded: int
    failover_exhausted: int
    failover_by_error: Mapping[str, int]
    latency_ms: Mapping[str, int]
    correlations: Mapping[str, int]
    provider_snapshots: Mapping[str, Mapping[str, Any]]
    members: tuple[LlmPoolMemberSnapshot, ...]
    closed: bool


@dataclass
class _AdmissionWaiter:
    future: asyncio.Future[None]
    enqueued_at: float
    correlation: Mapping[str, str]
    admitted: bool = False


@dataclass
class _MemberState:
    member: LlmPoolMember
    current_weight: int = 0
    active: int = 0
    waiting: int = 0
    dispatches: int = 0
    borrowed_dispatches: int = 0
    successes: int = 0
    failures: int = 0
    rate_limits: int = 0
    provider_5xx: int = 0
    timeouts: int = 0
    parse_failures: int = 0
    schema_failures: int = 0
    consecutive_failures: int = 0
    circuit_state: str = "closed"
    open_until: float = 0.0
    half_open_active_probes: int = 0


class LlmPoolLease:
    """One logical execution permit held across retries and failover."""

    def __init__(
        self,
        coordinator: "LlmPoolCoordinator",
        *,
        correlation: Mapping[str, str],
        admitted_at: float,
        queue_wait_ms: int,
    ) -> None:
        self._coordinator = coordinator
        self.correlation = dict(correlation)
        self.admitted_at = admitted_at
        self.queue_wait_ms = queue_wait_ms
        self._selection: Optional[LlmPoolSelection] = None
        self._closed = False

    @property
    def selection(self) -> Optional[LlmPoolSelection]:
        return self._selection

    async def select_member(
        self,
        *,
        logical_profile: str,
        deadline: float,
        excluded_sources: Sequence[str] = (),
        failover: bool = False,
    ) -> LlmPoolSelection:
        if self._closed:
            raise LlmConfigurationError("LLM pool lease is closed")
        if self._selection is not None:
            raise LlmConfigurationError("LLM pool member lease is already active")
        self._selection = await self._coordinator._select_member(
            logical_profile=logical_profile,
            deadline=deadline,
            excluded_sources=frozenset(excluded_sources),
            failover=failover,
            correlation=self.correlation,
        )
        return self._selection

    async def finish_member(
        self,
        *,
        success: bool,
        error_code: Optional[str] = None,
        status_code: Optional[int] = None,
    ) -> None:
        selection = self._selection
        if selection is None:
            raise LlmConfigurationError("LLM pool member lease is not active")
        self._selection = None
        await self._coordinator._finish_member(
            selection,
            success=success,
            error_code=error_code,
            status_code=status_code,
            correlation=self.correlation,
        )

    async def record_failover(
        self,
        *,
        error_code: str,
        succeeded: bool,
        latency_ms: int = 0,
    ) -> None:
        await self._coordinator._record_failover(
            error_code=error_code,
            succeeded=succeeded,
            latency_ms=latency_ms,
        )

    async def close(self) -> None:
        if self._closed:
            return
        if self._selection is not None:
            selection = self._selection
            self._selection = None
            await self._coordinator._finish_member(
                selection,
                success=False,
                error_code="cancelled",
                status_code=None,
                correlation=self.correlation,
                affect_health=False,
            )
        self._closed = True
        await self._coordinator._release_route(self)

    async def __aenter__(self) -> "LlmPoolLease":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()


class LlmPoolCoordinator:
    """Bounded logical admission, weighted selection, and circuit state."""

    def __init__(
        self,
        config: LlmConfig,
        pool: LlmPoolConfig,
        *,
        identity: str,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.llm_config = config
        self.config = pool
        self.identity = identity
        self._clock = clock
        self._lock = asyncio.Lock()
        self._admission_queue: Deque[_AdmissionWaiter] = deque()
        self._member_waiters: set[asyncio.Future[None]] = set()
        self._members = {
            member.source_label: _MemberState(member=member)
            for member in pool.members
        }
        self._active = 0
        self._admitted = 0
        self._completed = 0
        self._cancelled = 0
        self._deadline_exceeded = 0
        self._queue_full = 0
        self._failover_requested = 0
        self._failover_succeeded = 0
        self._failover_exhausted = 0
        self._failover_by_error: dict[str, int] = {}
        self._latency_totals: dict[str, int] = {
            "queue": 0,
            "execution": 0,
            "failover": 0,
            "total": 0,
        }
        self._correlations: dict[str, int] = {}
        self._provider_snapshots: dict[str, Mapping[str, Any]] = {}
        self._closed = False
        LOGGER.info(
            "event=llm.pool.lifecycle.started pool=%s identity=%s total_concurrency=%s",
            pool.name,
            identity,
            pool.total_concurrency,
        )

    async def acquire(
        self,
        *,
        deadline: float,
        correlation: Optional[Mapping[str, Any]] = None,
    ) -> LlmPoolLease:
        safe_correlation = self._safe_correlation(correlation)
        loop = asyncio.get_running_loop()
        waiter = _AdmissionWaiter(
            future=loop.create_future(),
            enqueued_at=self._clock(),
            correlation=safe_correlation,
        )
        async with self._lock:
            if self._closed:
                raise LlmConfigurationError(f"LLM pool is closed: {self.config.name}")
            if self._active < self.config.total_concurrency and not self._admission_queue:
                self._admit_locked(waiter)
            else:
                if len(self._admission_queue) >= self.config.queue_size:
                    self._queue_full += 1
                    LOGGER.warning(
                        "event=llm.pool.queue.full pool=%s waiting=%s queue_size=%s",
                        self.config.name,
                        len(self._admission_queue),
                        self.config.queue_size,
                    )
                    raise LlmRateLimitError("LLM pool admission queue is full")
                self._admission_queue.append(waiter)
                LOGGER.debug(
                    "event=llm.pool.queue.enter pool=%s waiting=%s correlation=%s",
                    self.config.name,
                    len(self._admission_queue),
                    safe_correlation,
                )
        try:
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.wait_for(asyncio.shield(waiter.future), timeout=remaining)
        except asyncio.TimeoutError as exc:
            await self._withdraw_admission(waiter, deadline_exceeded=True)
            raise LlmDeadlineExceededError() from exc
        except BaseException:
            await self._withdraw_admission(waiter, deadline_exceeded=False)
            raise
        admitted_at = self._clock()
        queue_wait_ms = max(0, round((admitted_at - waiter.enqueued_at) * 1000))
        self._latency_totals["queue"] += queue_wait_ms
        LOGGER.info(
            "event=llm.route.admitted pool=%s queue_wait_ms=%s active=%s correlation=%s",
            self.config.name,
            queue_wait_ms,
            self._active,
            safe_correlation,
        )
        return LlmPoolLease(
            self,
            correlation=safe_correlation,
            admitted_at=admitted_at,
            queue_wait_ms=queue_wait_ms,
        )

    def _admit_locked(self, waiter: _AdmissionWaiter) -> None:
        waiter.admitted = True
        self._active += 1
        self._admitted += 1
        for key, value in waiter.correlation.items():
            self._correlations[f"{key}:{value}"] = (
                self._correlations.get(f"{key}:{value}", 0) + 1
            )
        if not waiter.future.done():
            waiter.future.set_result(None)

    async def _withdraw_admission(
        self, waiter: _AdmissionWaiter, *, deadline_exceeded: bool
    ) -> None:
        async with self._lock:
            if waiter.admitted:
                self._active -= 1
                self._dispatch_admission_locked()
            else:
                try:
                    self._admission_queue.remove(waiter)
                except ValueError:
                    pass
                if not waiter.future.done():
                    waiter.future.cancel()
            if deadline_exceeded:
                self._deadline_exceeded += 1
            else:
                self._cancelled += 1
            LOGGER.debug(
                "event=llm.pool.queue.withdraw pool=%s deadline=%s active=%s waiting=%s",
                self.config.name,
                deadline_exceeded,
                self._active,
                len(self._admission_queue),
            )

    async def _release_route(self, lease: LlmPoolLease) -> None:
        async with self._lock:
            if self._active <= 0:
                LOGGER.error(
                    "event=llm.pool.state.invalid pool=%s detail=release_without_active",
                    self.config.name,
                )
                raise RuntimeError("LLM pool release without active route lease")
            self._active -= 1
            self._completed += 1
            execution_ms = max(
                0, round((self._clock() - lease.admitted_at) * 1000)
            )
            self._latency_totals["execution"] += execution_ms
            self._latency_totals["total"] += lease.queue_wait_ms + execution_ms
            self._dispatch_admission_locked()
            LOGGER.debug(
                "event=llm.pool.lease.released pool=%s active=%s elapsed_ms=%s",
                self.config.name,
                self._active,
                execution_ms,
            )

    def _dispatch_admission_locked(self) -> None:
        while self._admission_queue and self._active < self.config.total_concurrency:
            waiter = self._admission_queue.popleft()
            if waiter.future.cancelled():
                continue
            self._admit_locked(waiter)

    async def _select_member(
        self,
        *,
        logical_profile: str,
        deadline: float,
        excluded_sources: frozenset[str],
        failover: bool,
        correlation: Mapping[str, str],
    ) -> LlmPoolSelection:
        while True:
            waiter: Optional[asyncio.Future[None]] = None
            async with self._lock:
                if self._closed:
                    raise LlmConfigurationError(
                        f"LLM pool is closed: {self.config.name}"
                    )
                selection, waiting_labels = self._select_locked(
                    logical_profile=logical_profile,
                    excluded_sources=excluded_sources,
                )
                if selection is not None:
                    LOGGER.info(
                        "event=llm.source.selected pool=%s logical_profile=%s "
                        "source_label=%s selected_profile=%s borrowed=%s failover=%s "
                        "correlation=%s",
                        self.config.name,
                        logical_profile,
                        selection.source_label,
                        selection.selected_profile,
                        selection.borrowed,
                        failover,
                        correlation,
                    )
                    return selection
                if not waiting_labels:
                    raise LlmProviderError(
                        "no eligible LLM pool member",
                        retryable=True,
                    )
                waiter = asyncio.get_running_loop().create_future()
                self._member_waiters.add(waiter)
                for label in waiting_labels:
                    self._members[label].waiting += 1
                LOGGER.debug(
                    "event=llm.pool.member.wait pool=%s logical_profile=%s "
                    "sources=%s excluded=%s",
                    self.config.name,
                    logical_profile,
                    sorted(waiting_labels),
                    sorted(excluded_sources),
                )
            try:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                await asyncio.wait_for(asyncio.shield(waiter), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise LlmDeadlineExceededError() from exc
            finally:
                async with self._lock:
                    self._member_waiters.discard(waiter)
                    for label in waiting_labels:
                        state = self._members[label]
                        state.waiting = max(0, state.waiting - 1)

    def _select_locked(
        self,
        *,
        logical_profile: str,
        excluded_sources: frozenset[str],
    ) -> tuple[Optional[LlmPoolSelection], set[str]]:
        now = self._clock()
        compatible: list[_MemberState] = []
        waiting_labels: set[str] = set()
        for member in self.config.members:
            state = self._members[member.source_label]
            if member.source_label in excluded_sources:
                continue
            concrete_name = member.profiles.get(logical_profile)
            if concrete_name is None:
                continue
            profile = self.llm_config.profiles.get(concrete_name)
            if profile is None or not profile.enabled:
                continue
            if state.circuit_state == "open":
                if now < state.open_until:
                    continue
                state.circuit_state = "half_open"
                LOGGER.info(
                    "event=llm.circuit.half_open pool=%s source_label=%s",
                    self.config.name,
                    member.source_label,
                )
            if (
                state.circuit_state == "half_open"
                and state.half_open_active_probes
                >= self.config.failover.half_open_max_probes
            ):
                waiting_labels.add(member.source_label)
                continue
            compatible.append(state)
            state.current_weight += member.weight
        if not compatible:
            return None, waiting_labels

        total_weight = sum(state.member.weight for state in compatible)
        preferred = max(
            compatible,
            key=lambda state: (
                state.current_weight,
                -self._member_index(state.member.source_label),
            ),
        )
        eligible = [
            state
            for state in compatible
            if state.active < self._member_limit(state.member, logical_profile)
        ]
        if not eligible:
            return None, {state.member.source_label for state in compatible}
        borrowed = False
        selected = preferred
        if preferred not in eligible:
            waiting_labels.add(preferred.member.source_label)
            if not self.config.borrow_idle_capacity:
                return None, waiting_labels
            selected = max(
                eligible,
                key=lambda state: (
                    state.current_weight,
                    -self._member_index(state.member.source_label),
                ),
            )
            borrowed = True
        selected.current_weight -= total_weight
        selected.active += 1
        selected.dispatches += 1
        if borrowed:
            selected.borrowed_dispatches += 1
        half_open_probe = selected.circuit_state == "half_open"
        if half_open_probe:
            selected.half_open_active_probes += 1
        concrete_name = selected.member.profiles[logical_profile]
        LOGGER.debug(
            "event=llm.pool.scheduler.selected pool=%s logical_profile=%s "
            "source_label=%s weights=%s active=%s borrowed=%s",
            self.config.name,
            logical_profile,
            selected.member.source_label,
            {label: state.current_weight for label, state in self._members.items()},
            selected.active,
            borrowed,
        )
        return LlmPoolSelection(
            pool=self.config.name,
            logical_profile=logical_profile,
            source_label=selected.member.source_label,
            selected_profile=concrete_name,
            weight=selected.member.weight,
            borrowed=borrowed,
            half_open_probe=half_open_probe,
        ), waiting_labels

    async def _finish_member(
        self,
        selection: LlmPoolSelection,
        *,
        success: bool,
        error_code: Optional[str],
        status_code: Optional[int],
        correlation: Mapping[str, str],
        affect_health: bool = True,
    ) -> None:
        async with self._lock:
            state = self._members[selection.source_label]
            if state.active <= 0:
                LOGGER.error(
                    "event=llm.pool.state.invalid pool=%s source_label=%s "
                    "detail=member_release_without_active",
                    self.config.name,
                    selection.source_label,
                )
                raise RuntimeError("LLM pool member release without active lease")
            state.active -= 1
            if selection.half_open_probe:
                state.half_open_active_probes = max(
                    0, state.half_open_active_probes - 1
                )
            if success:
                state.successes += 1
                state.consecutive_failures = 0
                if state.circuit_state != "closed":
                    state.circuit_state = "closed"
                    state.open_until = 0.0
                    LOGGER.info(
                        "event=llm.circuit.recovered pool=%s source_label=%s",
                        self.config.name,
                        selection.source_label,
                    )
            else:
                state.failures += 1
                self._classify_failure_locked(state, error_code, status_code)
                if affect_health and self._health_failure(error_code):
                    state.consecutive_failures += 1
                    if (
                        selection.half_open_probe
                        or state.consecutive_failures
                        >= self.config.failover.failure_threshold
                    ):
                        state.circuit_state = "open"
                        state.open_until = (
                            self._clock() + self.config.failover.open_seconds
                        )
                        LOGGER.info(
                            "event=llm.circuit.opened pool=%s source_label=%s "
                            "error_code=%s open_seconds=%s",
                            self.config.name,
                            selection.source_label,
                            error_code,
                            self.config.failover.open_seconds,
                        )
            self._wake_member_waiters_locked()
            LOGGER.debug(
                "event=llm.pool.member.released pool=%s source_label=%s success=%s "
                "error_code=%s active=%s correlation=%s",
                self.config.name,
                selection.source_label,
                success,
                error_code,
                state.active,
                correlation,
            )

    async def _record_failover(
        self,
        *,
        error_code: str,
        succeeded: bool,
        latency_ms: int = 0,
    ) -> None:
        async with self._lock:
            self._failover_requested += 1
            self._failover_by_error[error_code] = (
                self._failover_by_error.get(error_code, 0) + 1
            )
            if succeeded:
                self._failover_succeeded += 1
            else:
                self._failover_exhausted += 1
            self._latency_totals["failover"] += max(0, int(latency_ms))

    def _classify_failure_locked(
        self,
        state: _MemberState,
        error_code: Optional[str],
        status_code: Optional[int],
    ) -> None:
        if error_code == "rate_limit_error" or status_code == 429:
            state.rate_limits += 1
        if status_code is not None and 500 <= status_code <= 599:
            state.provider_5xx += 1
        if error_code in {"transient_transport_error", "deadline_exceeded"}:
            state.timeouts += 1
        if error_code == "response_parse_error":
            state.parse_failures += 1
        if error_code == "schema_validation_error":
            state.schema_failures += 1

    @staticmethod
    def _health_failure(error_code: Optional[str]) -> bool:
        return error_code in {
            "rate_limit_error",
            "transient_transport_error",
            "provider_error",
            "response_parse_error",
            "schema_validation_error",
        }

    def _member_limit(
        self, member: LlmPoolMember, logical_profile: Optional[str] = None
    ) -> int:
        profile_names = (
            (member.profiles[logical_profile],)
            if logical_profile is not None and logical_profile in member.profiles
            else tuple(member.profiles.values())
        )
        limits = []
        for profile_name in profile_names:
            profile = self.llm_config.profiles.get(profile_name)
            if profile is None:
                continue
            provider_limit = self.llm_config.resource_for_profile(
                profile
            ).hard_max_concurrency
            http_limit = self.llm_config.resource_for_profile(
                profile
            ).http_max_connections
            limits.append(
                min(profile.max_concurrency, provider_limit, http_limit)
            )
        profile_limit = min(limits) if limits else 1
        return min(member.max_concurrency or profile_limit, profile_limit)

    def _member_index(self, source_label: str) -> int:
        return next(
            index
            for index, member in enumerate(self.config.members)
            if member.source_label == source_label
        )

    def _wake_member_waiters_locked(self) -> None:
        for waiter in tuple(self._member_waiters):
            if not waiter.done():
                waiter.set_result(None)

    def update_provider_snapshots(
        self, snapshots: Mapping[str, Mapping[str, Any]]
    ) -> None:
        self._provider_snapshots = {
            str(name): dict(snapshot) for name, snapshot in snapshots.items()
        }

    def _member_local_limit(self, member: LlmPoolMember) -> tuple[int, str]:
        profiles = tuple(
            self.llm_config.profiles[name] for name in member.profiles.values()
        )
        profile_limit = min(profile.max_concurrency for profile in profiles)
        if member.max_concurrency and member.max_concurrency <= profile_limit:
            return member.max_concurrency, "member"
        return profile_limit, "profile"

    def _provider_runtime_limit(self, resource_name: str) -> tuple[int, str]:
        resource = self.llm_config.provider_resources[resource_name]
        snapshot = self._provider_snapshots.get(resource_name, {})
        if float(snapshot.get("cooldown_remaining_seconds") or 0.0) > 0:
            return 0, "provider_cooldown"
        configured_rpm = int(
            snapshot.get("configured_requests_per_minute")
            or resource.requests_per_minute
            or 0
        )
        if (
            configured_rpm > 0
            and int(snapshot.get("rpm_window_requests") or 0) >= configured_rpm
            and float(snapshot.get("rpm_next_admission_seconds") or 0.0) > 0
        ):
            return 0, "provider_rpm"
        adaptive_limit = int(
            snapshot.get("effective_bulk_concurrency")
            or resource.default_bulk_concurrency
        )
        candidates = (
            (resource.hard_max_concurrency, "provider"),
            (resource.http_max_connections, "http"),
            (adaptive_limit, "provider_adaptive"),
        )
        return min(candidates, key=lambda item: item[0])

    def _snapshot_capacity(self) -> tuple[int, str, Mapping[str, int]]:
        grouped: dict[str, list[tuple[int, str]]] = {}
        mixed_members: list[tuple[int, str]] = []
        member_limits: dict[str, int] = {}
        constraints: list[str] = []
        for member in self.config.members:
            local_limit, local_layer = self._member_local_limit(member)
            resource_names = {
                self.llm_config.profiles[name].provider_resource
                or self.llm_config.default_resource_name(
                    self.llm_config.profiles[name]
                )
                for name in member.profiles.values()
            }
            runtime_limits = tuple(
                self._provider_runtime_limit(name) for name in resource_names
            )
            runtime_limit, runtime_layer = min(
                runtime_limits, key=lambda item: item[0]
            )
            member_limits[member.source_label] = min(local_limit, runtime_limit)
            if len(resource_names) == 1:
                resource_name = next(iter(resource_names))
                grouped.setdefault(resource_name, []).append(
                    (local_limit, local_layer)
                )
            else:
                limit = min(local_limit, runtime_limit)
                mixed_members.append(
                    (
                        limit,
                        runtime_layer
                        if runtime_limit < local_limit
                        else local_layer,
                    )
                )

        capacity = sum(limit for limit, _ in mixed_members)
        constraints.extend(layer for _, layer in mixed_members)
        for resource_name, local_limits in grouped.items():
            local_capacity = sum(limit for limit, _ in local_limits)
            provider_capacity, provider_layer = self._provider_runtime_limit(
                resource_name
            )
            capacity += min(local_capacity, provider_capacity)
            if provider_capacity < local_capacity:
                constraints.append(provider_layer)
            else:
                constraints.extend(layer for _, layer in local_limits)

        effective = min(self.config.total_concurrency, capacity)
        if effective == self.config.total_concurrency:
            return effective, "pool", member_limits
        priority = (
            "provider_cooldown",
            "provider_rpm",
            "provider_adaptive",
            "http",
            "provider",
            "member",
            "profile",
        )
        bottleneck = next(
            (layer for layer in priority if layer in constraints),
            "member_or_profile",
        )
        return effective, bottleneck, member_limits

    def snapshot(self) -> LlmPoolSnapshot:
        now = self._clock()
        total_dispatches = sum(state.dispatches for state in self._members.values())
        oldest_wait = (
            max(0.0, now - self._admission_queue[0].enqueued_at)
            if self._admission_queue
            else 0.0
        )
        effective_total, bottleneck, runtime_member_limits = (
            self._snapshot_capacity()
        )
        member_snapshots = tuple(
            LlmPoolMemberSnapshot(
                source_label=member.source_label,
                weight=member.weight,
                configured_max_concurrency=member.max_concurrency,
                effective_max_concurrency=runtime_member_limits[
                    member.source_label
                ],
                active=state.active,
                waiting=state.waiting,
                dispatches=state.dispatches,
                dispatch_ratio=(
                    state.dispatches / total_dispatches if total_dispatches else 0.0
                ),
                borrowed_dispatches=state.borrowed_dispatches,
                successes=state.successes,
                failures=state.failures,
                rate_limits=state.rate_limits,
                provider_5xx=state.provider_5xx,
                timeouts=state.timeouts,
                parse_failures=state.parse_failures,
                schema_failures=state.schema_failures,
                circuit_state=state.circuit_state,
                consecutive_failures=state.consecutive_failures,
                cooldown_remaining_seconds=max(0.0, state.open_until - now),
                half_open_active_probes=state.half_open_active_probes,
            )
            for member in self.config.members
            for state in (self._members[member.source_label],)
        )
        return LlmPoolSnapshot(
            name=self.config.name,
            identity=self.identity,
            configured_total_concurrency=self.config.total_concurrency,
            effective_total_concurrency=effective_total,
            active_bottleneck=bottleneck,
            active=self._active,
            waiting=len(self._admission_queue),
            oldest_wait_seconds=oldest_wait,
            admitted=self._admitted,
            completed=self._completed,
            cancelled=self._cancelled,
            deadline_exceeded=self._deadline_exceeded,
            queue_full=self._queue_full,
            failover_requested=self._failover_requested,
            failover_succeeded=self._failover_succeeded,
            failover_exhausted=self._failover_exhausted,
            failover_by_error=dict(self._failover_by_error),
            latency_ms=dict(self._latency_totals),
            correlations=dict(self._correlations),
            provider_snapshots=dict(self._provider_snapshots),
            members=member_snapshots,
            closed=self._closed,
        )

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            for waiter in self._admission_queue:
                if not waiter.future.done():
                    waiter.future.set_exception(
                        LlmConfigurationError(f"LLM pool is closed: {self.config.name}")
                    )
            self._admission_queue.clear()
            for waiter in tuple(self._member_waiters):
                if not waiter.done():
                    waiter.set_exception(
                        LlmConfigurationError(f"LLM pool is closed: {self.config.name}")
                    )
            LOGGER.info(
                "event=llm.pool.lifecycle.stopped pool=%s active=%s",
                self.config.name,
                self._active,
            )

    @staticmethod
    def _safe_correlation(
        correlation: Optional[Mapping[str, Any]],
    ) -> dict[str, str]:
        allowed = {
            "logical_profile",
            "workload",
            "run_id",
            "stage",
            "business_item_key",
            "request_hash",
        }
        return {
            key: str(value)[:256]
            for key, value in (correlation or {}).items()
            if key in allowed and value is not None
        }


class LlmPoolCoordinatorRegistry:
    """Share coordinators by validated pool configuration identity."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._coordinators: dict[str, LlmPoolCoordinator] = {}
        self._pool_identities: dict[str, str] = {}

    @staticmethod
    def identity_for(config: LlmConfig, pool: LlmPoolConfig) -> str:
        profile_contracts = {
            profile_name: {
                "enabled": config.profiles[profile_name].enabled,
                "max_concurrency": config.profiles[profile_name].max_concurrency,
                "provider_resource": config.profiles[profile_name].provider_resource,
            }
            for member in pool.members
            for profile_name in member.profiles.values()
        }
        payload = json.dumps(
            {
                "pool": pool.safe_dict(),
                "profiles": profile_contracts,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def get(self, config: LlmConfig, pool_name: str) -> LlmPoolCoordinator:
        try:
            pool = config.pools[pool_name]
        except KeyError as exc:
            raise LlmConfigurationError(f"unknown LLM pool: {pool_name}") from exc
        identity = self.identity_for(config, pool)
        registered_identity = self._pool_identities.get(pool_name)
        if registered_identity is not None and registered_identity != identity:
            raise LlmConfigurationError(
                f"conflicting LLM pool configuration: {pool_name}"
            )
        coordinator = self._coordinators.get(identity)
        if coordinator is None:
            coordinator = LlmPoolCoordinator(
                config,
                pool,
                identity=identity,
                clock=self._clock,
            )
            self._coordinators[identity] = coordinator
        self._pool_identities[pool_name] = identity
        return coordinator

    def snapshots(self) -> Mapping[str, LlmPoolSnapshot]:
        return {
            identity: coordinator.snapshot()
            for identity, coordinator in self._coordinators.items()
        }

    async def close_all(self) -> None:
        coordinators = tuple(self._coordinators.values())
        self._coordinators.clear()
        self._pool_identities.clear()
        for coordinator in coordinators:
            await coordinator.close()

    async def clear(self) -> None:
        await self.close_all()
