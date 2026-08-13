"""Thread-local cooperative control for one claimed asset operation."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from threading import Event

from .models import OperationStatus
from .repository import AnnouncementAssetRepository


@dataclass(frozen=True)
class OperationExecutionControl:
    repository: AnnouncementAssetRepository
    operation_id: str
    lease_owner: str
    lease_generation: int
    heartbeat_lost: Event

    def stop_reason(self) -> str | None:
        if self.heartbeat_lost.is_set():
            return "operation_lease_lost"
        operation = self.repository.get_operation(self.operation_id)
        if operation is None:
            return "operation_lease_lost"
        if (
            operation.status is not OperationStatus.RUNNING
            or operation.lease_owner != self.lease_owner
            or operation.lease_generation != self.lease_generation
        ):
            return "operation_lease_lost"
        if operation.progress.get("stop_requested"):
            return "operator_stop_requested"
        return None


_CURRENT_CONTROL: ContextVar[OperationExecutionControl | None] = ContextVar(
    "announcement_asset_operation_control", default=None
)


@contextmanager
def activate_operation_control(
    control: OperationExecutionControl,
) -> Iterator[None]:
    token = _CURRENT_CONTROL.set(control)
    try:
        yield
    finally:
        _CURRENT_CONTROL.reset(token)


def operation_stop_reason(operation_id: str | None = None) -> str | None:
    control = _CURRENT_CONTROL.get()
    if control is None:
        return None
    if operation_id is not None and operation_id != control.operation_id:
        return None
    return control.stop_reason()


def current_operation_fence(
    operation_id: str,
) -> tuple[str, int] | None:
    control = _CURRENT_CONTROL.get()
    if control is None or control.operation_id != operation_id:
        return None
    return control.lease_owner, control.lease_generation
