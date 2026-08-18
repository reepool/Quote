"""Durable authenticated command plane for announcement-asset jobs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from typing import Any

from research.announcements import AnnouncementAcquisitionService

from .config import AnnouncementAssetConfig
from .models import (
    AssetOperation,
    BatchOutcome,
    OperationStage,
    OperationStatus,
    canonical_json,
    stable_id,
)
from .operation_control import (
    OperationExecutionControl,
    activate_operation_control,
)
from .repository import AnnouncementAssetRepository
from .service import acquisition_work_fingerprint

LATEST_BACKFILL_JOB = "annual_report_asset_latest_backfill"
DAILY_UPDATE_JOB = "annual_report_asset_daily_update"
SCHEDULER_SERVICE_PRINCIPAL = "service:annual-report-asset-scheduler"
SUPPORTED_JOBS = frozenset(
    {
        LATEST_BACKFILL_JOB,
        DAILY_UPDATE_JOB,
    }
)


class AuthorizationBoundaryUnavailable(RuntimeError):
    """The protected control plane has no trusted identity boundary."""


@dataclass(frozen=True)
class CommandPrincipal:
    principal_id: str
    permissions: frozenset[str]
    authenticated: bool = True
    service_identity: bool = False

    def __post_init__(self) -> None:
        principal = str(self.principal_id or "").strip()
        if not principal:
            raise ValueError("principal_id is required")
        object.__setattr__(self, "principal_id", principal)
        object.__setattr__(
            self,
            "permissions",
            frozenset(
                str(item).strip() for item in self.permissions if str(item).strip()
            ),
        )


@dataclass(frozen=True)
class JobStartResult:
    run_id: str
    job_name: str
    normalized_scope: Mapping[str, Any]
    accepted_bounds: Mapping[str, int]
    status: str
    reused: bool
    config_version: str


@dataclass(frozen=True)
class JobStartPreflightResult:
    """Normalized DB-independent inputs accepted for a job start."""

    job_name: str
    trigger_kind: str
    normalized_scope: Mapping[str, Any]
    accepted_bounds: Mapping[str, int]


@dataclass(frozen=True)
class JobHistory:
    runs: tuple[AssetOperation, ...]
    last_successful_cutoff: Mapping[str, str]
    active_heartbeat_age_seconds: Mapping[str, float]
    consecutive_failures: int
    cursor_lag_seconds: Mapping[str, float]
    oldest_retry_age_seconds: float | None
    alerts: tuple[str, ...] = ()


JobRunner = Callable[[AssetOperation], Any]
ReadinessGate = Callable[[str], tuple[bool, tuple[str, ...]]]


@dataclass
class AnnualReportSchedulerCommandService:
    repository: AnnouncementAssetRepository
    config: AnnouncementAssetConfig
    config_version: str
    runners: Mapping[str, JobRunner] = field(default_factory=dict)
    readiness_gate: ReadinessGate | None = None
    acquisition_service: AnnouncementAcquisitionService | None = None

    def preflight_start(
        self,
        job_name: str,
        *,
        principal: CommandPrincipal,
        trigger_kind: str,
        scope: Mapping[str, Any] | None = None,
        bounds: Mapping[str, int] | None = None,
        action_flags: Mapping[str, bool] | None = None,
    ) -> JobStartPreflightResult:
        """Validate and normalize a start request without repository access."""
        self._authorize(principal)
        job = _normalize_job(job_name)
        trigger = _normalize_trigger(trigger_kind)
        normalized_scope = _normalize_scope(scope or {})
        if action_flags:
            raise ValueError("announcement asset maintenance actions are not supported")
        self._validate_trigger(job, trigger, principal=principal)
        requested_exchanges = set(normalized_scope.get("exchanges", ()))
        requested_sources = set(normalized_scope.get("sources", ()))
        if requested_exchanges - set(self.config.exchanges):
            raise ValueError("job scope contains an unconfigured exchange")
        if requested_sources - set(self.config.acquisition.source_routes):
            raise ValueError("job scope contains an unconfigured source")
        accepted_bounds = self._normalize_bounds(job, bounds or {})
        if self.config.dry_run:
            raise RuntimeError("annual_report_asset_dry_run_blocks_job_execution")
        return JobStartPreflightResult(
            job_name=job,
            trigger_kind=trigger,
            normalized_scope=normalized_scope,
            accepted_bounds=accepted_bounds,
        )

    def start(
        self,
        job_name: str,
        *,
        principal: CommandPrincipal,
        trigger_kind: str,
        scope: Mapping[str, Any] | None = None,
        bounds: Mapping[str, int] | None = None,
        action_flags: Mapping[str, bool] | None = None,
    ) -> JobStartResult:
        preflight = self.preflight_start(
            job_name,
            principal=principal,
            trigger_kind=trigger_kind,
            scope=scope,
            bounds=bounds,
            action_flags=action_flags,
        )
        job = preflight.job_name
        trigger = preflight.trigger_kind
        normalized_scope = preflight.normalized_scope
        accepted_bounds = preflight.accepted_bounds
        if trigger == "cron" and self.readiness_gate is not None:
            ready, blockers = self.readiness_gate(job)
            if not ready:
                raise RuntimeError(
                    "scheduled_job_readiness_blocked:" + ",".join(blockers)
                )
        fingerprint = acquisition_work_fingerprint(
            operation_type=job,
            scope=normalized_scope,
            config=self.config,
            accepted_bounds=accepted_bounds,
            integrity_policy="hash_and_pdf_signature",
            configuration_version=self.config_version,
            acquisition_service=self.acquisition_service,
        )
        operation_key = stable_id("scheduled-asset-job", fingerprint)
        operation, created = self.repository.create_or_reuse_operation(
            operation_type=job,
            idempotency_key=operation_key,
            scope={
                **normalized_scope,
                "bounds": accepted_bounds,
                "trigger_kind": trigger,
                "config_version": self.config_version,
                "request_fingerprint": fingerprint,
            },
            policy_version=self.config.policy_version,
            owner=principal.principal_id,
            stage=OperationStage.DISCOVERING,
        )
        if created:
            operation = self.repository.transition_operation(
                operation.operation_id,
                OperationStatus.QUEUED,
                progress={
                    "resume_generation": 0,
                    "accepted_bounds": accepted_bounds,
                    "config_version": self.config_version,
                    "trigger_kind": trigger,
                },
            )
        self._audit(
            operation=operation,
            command="start" if created else "start_reused",
            principal=principal,
            trigger_kind=trigger,
            fingerprint=fingerprint,
            payload={"scope": normalized_scope, "bounds": accepted_bounds},
        )
        return JobStartResult(
            run_id=operation.operation_id,
            job_name=job,
            normalized_scope=normalized_scope,
            accepted_bounds=accepted_bounds,
            status=operation.status.value,
            reused=not created,
            config_version=self.config_version,
        )

    def execute(
        self,
        run_id: str,
        *,
        principal: CommandPrincipal,
    ) -> AssetOperation:
        self._authorize(principal)
        operation = self._require_operation(run_id)
        if self.config.dry_run:
            raise RuntimeError("annual_report_asset_dry_run_blocks_job_execution")
        runner = self.runners.get(operation.operation_type)
        if runner is None:
            raise RuntimeError(
                f"job runner is not registered: {operation.operation_type}"
            )
        now = datetime.now(timezone.utc)
        claimed = self.repository.claim_operation(
            operation.operation_id,
            lease_owner=principal.principal_id,
            lease_expires_at=(
                now + timedelta(seconds=self.config.retry.lease_seconds)
            ).isoformat(),
            stage=OperationStage.DISCOVERING,
        )
        self._audit(
            operation=claimed,
            command="execute",
            principal=principal,
            trigger_kind=str(claimed.scope.get("trigger_kind") or "manual"),
            fingerprint=str(claimed.scope.get("request_fingerprint") or ""),
        )
        stop_heartbeat = Event()
        heartbeat_lost = Event()

        def heartbeat() -> None:
            interval = max(1, int(self.config.retry.heartbeat_seconds))
            while not stop_heartbeat.wait(interval):
                try:
                    self.repository.heartbeat_operation(
                        run_id,
                        lease_owner=principal.principal_id,
                        lease_generation=claimed.lease_generation,
                        lease_expires_at=(
                            datetime.now(timezone.utc)
                            + timedelta(seconds=self.config.retry.lease_seconds)
                        ).isoformat(),
                    )
                except (KeyError, RuntimeError, ValueError):
                    heartbeat_lost.set()
                    return

        heartbeat_thread = Thread(
            target=heartbeat,
            name=f"annual-report-operation-heartbeat:{run_id}",
            daemon=True,
        )
        heartbeat_thread.start()
        try:
            with activate_operation_control(
                OperationExecutionControl(
                    repository=self.repository,
                    operation_id=run_id,
                    lease_owner=principal.principal_id,
                    lease_generation=claimed.lease_generation,
                    heartbeat_lost=heartbeat_lost,
                )
            ):
                result = runner(claimed)
            if heartbeat_lost.is_set():
                return self._require_operation(run_id)
            payload = _result_payload(result)
            latest = self._require_operation(run_id)
            if latest.progress.get("stop_requested"):
                return self.repository.transition_operation(
                    run_id,
                    OperationStatus.CANCELLED,
                    outcome=BatchOutcome.PARTIAL,
                    progress={**latest.progress, **payload},
                    reason_code="operator_stop",
                    expected_lease_owner=principal.principal_id,
                    expected_lease_generation=claimed.lease_generation,
                )
            result_status = str(payload.get("status") or "success")
            if result_status == "blocked":
                status = OperationStatus.BLOCKED
                outcome = BatchOutcome.BLOCKED
            elif result_status == "failed":
                status = OperationStatus.FAILED
                outcome = BatchOutcome.FAILED
            else:
                status = OperationStatus.COMPLETED
                outcome = (
                    BatchOutcome.PARTIAL
                    if result_status in {"partial", "degraded"}
                    else BatchOutcome.SUCCESS
                )
            return self.repository.transition_operation(
                run_id,
                status,
                outcome=outcome,
                progress={**latest.progress, **payload},
                reason_code=None,
                diagnostics={"errors": payload.get("errors", ())},
                expected_lease_owner=principal.principal_id,
                expected_lease_generation=claimed.lease_generation,
            )
        except Exception as exc:  # noqa: BLE001 - durable operation must not stay RUNNING
            latest = self._require_operation(run_id)
            if (
                latest.lease_owner != principal.principal_id
                or latest.lease_generation != claimed.lease_generation
            ):
                return latest
            return self.repository.transition_operation(
                run_id,
                OperationStatus.FAILED,
                outcome=BatchOutcome.FAILED,
                progress=latest.progress,
                reason_code="job_runner_exception",
                diagnostics={"error_type": type(exc).__name__, "error": str(exc)},
                expected_lease_owner=principal.principal_id,
                expected_lease_generation=claimed.lease_generation,
            )
        finally:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=1)

    def status(self, run_id: str, *, principal: CommandPrincipal) -> AssetOperation:
        self._authorize(principal)
        return self._require_operation(run_id)

    def stop(self, run_id: str, *, principal: CommandPrincipal) -> AssetOperation:
        self._authorize(principal)
        operation = self.repository.request_operation_stop(
            run_id, principal=principal.principal_id
        )
        self._audit(
            operation=operation,
            command="stop",
            principal=principal,
            trigger_kind="manual",
            fingerprint=str(operation.scope.get("request_fingerprint") or ""),
        )
        return operation

    def resume(self, run_id: str, *, principal: CommandPrincipal) -> AssetOperation:
        self._authorize(principal)
        operation = self.repository.resume_operation(
            run_id, principal=principal.principal_id
        )
        self._audit(
            operation=operation,
            command="resume",
            principal=principal,
            trigger_kind="manual",
            fingerprint=str(operation.scope.get("request_fingerprint") or ""),
            payload={"resume_generation": operation.progress.get("resume_generation")},
        )
        return operation

    def history(
        self,
        *,
        principal: CommandPrincipal,
        job_name: str | None = None,
        limit: int = 50,
        now: str | None = None,
    ) -> JobHistory:
        self._authorize(principal)
        job = None if job_name is None else _normalize_job(job_name)
        runs = tuple(self.repository.list_operations(operation_type=job, limit=limit))
        now_time = _parse_time(now) if now else datetime.now(timezone.utc)
        successful_cutoffs: dict[str, str] = {}
        heartbeat_ages: dict[str, float] = {}
        consecutive_failures = 0
        for operation in runs:
            scope_key = canonical_json(operation.scope)
            if (
                operation.status is OperationStatus.COMPLETED
                and operation.outcome is BatchOutcome.SUCCESS
            ):
                cutoff = str(operation.progress.get("run_cutoff") or "")
                if cutoff and scope_key not in successful_cutoffs:
                    successful_cutoffs[scope_key] = cutoff
            if operation.status is OperationStatus.RUNNING and operation.heartbeat_at:
                heartbeat_ages[operation.operation_id] = max(
                    0.0,
                    (now_time - _parse_time(operation.heartbeat_at)).total_seconds(),
                )
            if operation.status in {OperationStatus.FAILED, OperationStatus.BLOCKED}:
                consecutive_failures += 1
            elif operation.status is OperationStatus.COMPLETED:
                break
        cursor_lag: dict[str, float] = {}
        for state in self.repository.list_discovery_states(category="annual_report"):
            covered = state.get("covered_until")
            if covered:
                key = "/".join(
                    (
                        str(state["source"]),
                        str(state["exchange"]),
                        str(state["scope_key"]),
                    )
                )
                cursor_lag[key] = max(
                    0.0, (now_time - _parse_time(str(covered))).total_seconds()
                )
        retries = self.repository.list_attachment_retries(limit=1000)
        oldest_retry_age = None
        if retries:
            oldest = min(_parse_time(str(item["first_queued_at"])) for item in retries)
            oldest_retry_age = max(0.0, (now_time - oldest).total_seconds())
        alerts: list[str] = []
        if any(
            age > self.config.retry.lease_seconds for age in heartbeat_ages.values()
        ):
            alerts.append("stale_heartbeat")
        if consecutive_failures:
            alerts.append("consecutive_failures")
        if (
            oldest_retry_age
            and oldest_retry_age > self.config.retry.max_backoff_seconds
        ):
            alerts.append("old_attachment_retry")
        return JobHistory(
            runs=runs,
            last_successful_cutoff=successful_cutoffs,
            active_heartbeat_age_seconds=heartbeat_ages,
            consecutive_failures=consecutive_failures,
            cursor_lag_seconds=cursor_lag,
            oldest_retry_age_seconds=oldest_retry_age,
            alerts=tuple(alerts),
        )

    def _authorize(self, principal: CommandPrincipal) -> None:
        if not self.config.trusted_identity_enabled:
            raise AuthorizationBoundaryUnavailable("authorization_boundary_unavailable")
        if not principal.authenticated:
            raise PermissionError("authentication_required")
        if self.config.operator_permission not in principal.permissions:
            raise PermissionError("operator_permission_required")
        registered = next(
            (
                item
                for item in self.config.trusted_principals
                if item.principal == principal.principal_id
            ),
            None,
        )
        if registered is None:
            raise PermissionError("principal_not_registered")
        if self.config.operator_permission not in registered.scopes:
            raise PermissionError("principal_operator_scope_not_configured")

    def _validate_trigger(
        self,
        job_name: str,
        trigger_kind: str,
        *,
        principal: CommandPrincipal,
    ) -> None:
        if trigger_kind != "cron":
            if principal.service_identity:
                raise PermissionError("service_identity_requires_cron_trigger")
            return
        if not principal.service_identity:
            raise PermissionError("cron_trigger_requires_service_identity")
        if (
            job_name == LATEST_BACKFILL_JOB
            and self.config.jobs.latest_backfill_manual_only
        ):
            raise RuntimeError("latest_backfill_is_manual_only")
        if job_name == DAILY_UPDATE_JOB and not (
            self.config.enabled
            and self.config.scheduled_enabled
            and self.config.jobs.daily_enabled
        ):
            raise RuntimeError("daily_cron_disabled")

    def _normalize_bounds(
        self,
        job_name: str,
        bounds: Mapping[str, int],
    ) -> Mapping[str, int]:
        limits = {
            "max_pages": self.config.discovery.max_pages,
            "max_requests": self.config.discovery.max_requests,
            "max_windows": self.config.discovery.max_windows,
            "max_instruments": self.config.discovery.max_instruments,
            "max_elapsed_seconds": self.config.discovery.max_elapsed_seconds,
            "max_download_bytes": self.config.acquisition.max_task_download_bytes,
        }
        accepted: dict[str, int] = {}
        for name, ceiling in limits.items():
            requested = int(bounds.get(name, ceiling))
            if requested <= 0 or requested > ceiling:
                raise ValueError(f"{name} exceeds configured bound")
            accepted[name] = requested
        unknown = set(bounds) - set(limits)
        if unknown:
            raise ValueError(f"unsupported job bounds: {sorted(unknown)}")
        return accepted

    def _require_operation(self, run_id: str) -> AssetOperation:
        operation = self.repository.get_operation(str(run_id).strip())
        if operation is None or operation.operation_type not in SUPPORTED_JOBS:
            raise KeyError("annual-report job run was not found")
        return operation

    def _audit(
        self,
        *,
        operation: AssetOperation,
        command: str,
        principal: CommandPrincipal,
        trigger_kind: str,
        fingerprint: str,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        self.repository.append_job_command_audit(
            operation_id=operation.operation_id,
            command=command,
            principal=principal.principal_id,
            effective_permission=self.config.operator_permission,
            trigger_kind=trigger_kind,
            config_version=self.config_version,
            request_fingerprint=fingerprint,
            payload=payload,
        )


def _normalize_job(value: str) -> str:
    job = str(value or "").strip()
    if job not in SUPPORTED_JOBS:
        raise ValueError("unsupported annual-report asset job")
    return job


def _normalize_trigger(value: str) -> str:
    trigger = str(value or "").strip().lower()
    if trigger not in {"cron", "cli", "api", "manual"}:
        raise ValueError("unsupported job trigger")
    return trigger


def _normalize_scope(scope: Mapping[str, Any]) -> Mapping[str, Any]:
    # Scheduler adapters bind this immutable contract to the operation scope so
    # a run cannot be replayed under a different cadence/window policy. Keep
    # the allowlist explicit: arbitrary caller fields must still be rejected.
    allowed = {
        "as_of",
        "run_cutoff",
        "exchanges",
        "sources",
        "schedule_timezone",
        "schedule_cron",
        "overlap_days",
        "catch_up_max_days",
        "minimum_runs_per_calendar_day",
        "cadence_fingerprint",
        "manual_only",
        "cron",
    }
    unknown = set(scope) - allowed
    if unknown:
        raise ValueError(f"unsupported job scope: {sorted(unknown)}")
    normalized: dict[str, Any] = {}
    for name, value in scope.items():
        if name in {"exchanges", "sources"}:
            normalized[name] = sorted(
                {
                    str(item).strip().upper()
                    if name == "exchanges"
                    else str(item).strip().lower()
                    for item in value
                }
            )
        elif name in {
            "overlap_days",
            "catch_up_max_days",
            "minimum_runs_per_calendar_day",
        }:
            if value is None:
                continue
            try:
                normalized[name] = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be an integer") from exc
            if normalized[name] <= 0:
                raise ValueError(f"{name} must be positive")
        elif name in {"manual_only"}:
            if not isinstance(value, bool):
                raise ValueError(f"{name} must be boolean")
            normalized[name] = value
        elif name == "cron":
            normalized[name] = None if value is None else str(value).strip()
        elif value is not None:
            normalized[name] = str(value).strip()
    return normalized


def _result_payload(result: Any) -> dict[str, Any]:
    if is_dataclass(result):
        payload = asdict(result)
    elif isinstance(result, Mapping):
        payload = dict(result)
    else:
        payload = {"status": str(result or "success")}
    errors = payload.get("errors") or ()
    payload["errors"] = tuple(str(item) for item in errors)[:100]
    return payload


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
