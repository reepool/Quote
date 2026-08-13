"""At-least-once delivery for effective annual-report change events.

The repository transaction is the activation boundary.  This dispatcher is
deliberately outside that transaction: a process may stop before delivery or
between consumer handling and checkpoint advancement, in which case the same
immutable event is replayed.  Consumers must therefore make their handler
idempotent using ``event_key`` (and may persist their own processing record in
the same database transaction as their domain output).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .models import ChangeEventType
from .repository import AnnouncementAssetRepository

ChangeEventHandler = Callable[[Mapping[str, Any]], None]
BeforeEventHook = Callable[[Mapping[str, Any]], None]


@dataclass(frozen=True)
class OutboxDispatchResult:
    """Bounded result for one consumer delivery pass."""

    consumer: str
    events_seen: int
    delivered: int
    skipped: int
    failed: int
    failed_event_id: int | None
    last_event_id: int


class AnnouncementAssetOutboxDispatcher:
    """Deliver one consumer's events from its durable monotonic checkpoint."""

    _DELIVERABLE_TYPES = frozenset(item.value for item in ChangeEventType)
    _DEFERRED_EVENT_PREFIX = "shadow_"

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        consumer: str,
        handler: ChangeEventHandler,
        before_delivery: BeforeEventHook | None = None,
        before_checkpoint: BeforeEventHook | None = None,
    ) -> None:
        if not str(consumer or "").strip():
            raise ValueError("consumer is required")
        self.repository = repository
        self.consumer = str(consumer).strip()
        self.handler = handler
        self.before_delivery = before_delivery
        self.before_checkpoint = before_checkpoint

    def dispatch_once(self, *, limit: int = 100) -> OutboxDispatchResult:
        """Process at most ``limit`` events, stopping at the first normal error."""
        if int(limit) < 1:
            raise ValueError("limit must be positive")
        bounded_limit = min(int(limit), 1000)
        checkpoint = self.repository.ensure_consumer_checkpoint(self.consumer)
        previous_event_id = int(checkpoint["last_event_id"])
        events = self.repository.list_change_events(
            after_event_id=previous_event_id,
            limit=bounded_limit,
        )
        delivered = 0
        skipped = 0
        failed = 0
        failed_event_id: int | None = None
        last_event_id = previous_event_id
        for event in events:
            event_id = int(event["event_id"])
            if event_id <= previous_event_id:
                continue
            event_type = str(event.get("event_type") or "")
            payload = event.get("payload")
            explicitly_non_deliverable = bool(
                isinstance(payload, Mapping)
                and payload.get("consumer_deliverable") is False
            )
            # Unknown/internal events still advance the cursor.  They are not
            # exposed to consumers, but leaving them ahead of the cursor would
            # permanently block later supported effective-asset events.
            if explicitly_non_deliverable or event_type.startswith(
                self._DEFERRED_EVENT_PREFIX
            ) or (
                event_type not in self._DELIVERABLE_TYPES
            ):
                _, advanced = self.repository.advance_consumer_checkpoint(
                    self.consumer,
                    event_id=event_id,
                    event_key=str(event["event_key"]),
                    expected_previous_event_id=previous_event_id,
                )
                if advanced:
                    skipped += 1
                    previous_event_id = event_id
                    last_event_id = event_id
                else:
                    current = self.repository.get_consumer_checkpoint(self.consumer)
                    previous_event_id = int(
                        current["last_event_id"]
                        if current is not None
                        else previous_event_id
                    )
                    last_event_id = previous_event_id
                continue

            self.repository.record_consumer_delivery_attempt(
                self.consumer,
                event_id=event_id,
            )
            try:
                if self.before_delivery is not None:
                    self.before_delivery(event)
                self.handler(event)
                if self.before_checkpoint is not None:
                    self.before_checkpoint(event)
                _, advanced = self.repository.advance_consumer_checkpoint(
                    self.consumer,
                    event_id=event_id,
                    event_key=str(event["event_key"]),
                    expected_previous_event_id=previous_event_id,
                )
            except Exception as exc:  # noqa: BLE001 - persist and stop at one event
                self.repository.record_consumer_delivery_failure(
                    self.consumer,
                    event_id=event_id,
                    error_code=_error_code(exc),
                )
                failed = 1
                failed_event_id = event_id
                break
            if advanced:
                delivered += 1
                previous_event_id = event_id
                last_event_id = event_id
            else:
                # Another worker won the CAS.  Its checkpoint is authoritative;
                # reload it before deciding whether this pass can continue.
                current = self.repository.get_consumer_checkpoint(self.consumer)
                previous_event_id = int(
                    current["last_event_id"] if current is not None else previous_event_id
                )
                last_event_id = previous_event_id
        return OutboxDispatchResult(
            consumer=self.consumer,
            events_seen=len(events),
            delivered=delivered,
            skipped=skipped,
            failed=failed,
            failed_event_id=failed_event_id,
            last_event_id=last_event_id,
        )

    def replay_until_idle(
        self, *, limit: int = 100, max_passes: int = 100
    ) -> OutboxDispatchResult:
        """Replay bounded batches until no event remains or a handler fails."""
        if int(max_passes) < 1:
            raise ValueError("max_passes must be positive")
        total_seen = total_delivered = total_skipped = total_failed = 0
        failed_event_id: int | None = None
        last_event_id = int(
            self.repository.ensure_consumer_checkpoint(self.consumer)["last_event_id"]
        )
        for _ in range(int(max_passes)):
            result = self.dispatch_once(limit=limit)
            total_seen += result.events_seen
            total_delivered += result.delivered
            total_skipped += result.skipped
            total_failed += result.failed
            last_event_id = result.last_event_id
            if result.failed:
                failed_event_id = result.failed_event_id
                break
            if result.events_seen == 0:
                break
        return OutboxDispatchResult(
            consumer=self.consumer,
            events_seen=total_seen,
            delivered=total_delivered,
            skipped=total_skipped,
            failed=total_failed,
            failed_event_id=failed_event_id,
            last_event_id=last_event_id,
        )


def _error_code(exc: Exception) -> str:
    detail = str(exc).replace("\n", " ").strip()[:200]
    return f"{type(exc).__name__}:{detail}" if detail else type(exc).__name__
