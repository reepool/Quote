"""Scheduler/CLI/API adapters over the shared durable command service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .capacity_artifact import CapacityArtifactNotReadyError, validate_capacity_artifact
from .commands import (
    ARCHIVE_BACKUP_JOB,
    DAILY_UPDATE_JOB,
    INTEGRITY_AUDIT_JOB,
    LATEST_BACKFILL_JOB,
    AnnualReportSchedulerCommandService,
    CommandPrincipal,
    JobStartResult,
)
from .config import AnnouncementAssetConfig
from .models import canonical_json, stable_id


@dataclass(frozen=True)
class SchedulerJobDefinition:
    name: str
    enabled: bool
    cron: str | None
    manual_only: bool
    timezone: str | None = None
    overlap_days: int | None = None
    catch_up_max_days: int | None = None
    minimum_runs_per_calendar_day: int | None = None
    cadence_fingerprint: str | None = None


def annual_report_scheduler_job_definitions(
    config: AnnouncementAssetConfig,
) -> tuple[SchedulerJobDefinition, ...]:
    """Return consumer-independent registration metadata with no side effects."""
    daily_fingerprint = daily_schedule_fingerprint(config)
    execution_enabled = _execution_gate_ready(config)
    return (
        SchedulerJobDefinition(
            name=LATEST_BACKFILL_JOB,
            enabled=execution_enabled and config.jobs.latest_backfill_enabled,
            cron=config.jobs.latest_backfill_cron,
            manual_only=config.jobs.latest_backfill_manual_only,
            timezone=config.timezone,
            cadence_fingerprint=latest_backfill_schedule_fingerprint(config),
        ),
        SchedulerJobDefinition(
            name=DAILY_UPDATE_JOB,
            enabled=(
                execution_enabled
                and config.scheduled_enabled
                and config.jobs.daily_enabled
            ),
            cron=config.jobs.daily_cron,
            manual_only=config.jobs.daily_manual_only,
            timezone=config.timezone,
            overlap_days=config.discovery.overlap_days,
            catch_up_max_days=config.daily_catch_up_max_days,
            minimum_runs_per_calendar_day=config.daily_min_runs_per_calendar_day,
            cadence_fingerprint=daily_fingerprint,
        ),
        SchedulerJobDefinition(
            name=INTEGRITY_AUDIT_JOB,
            enabled=(
                execution_enabled
                and config.jobs.integrity_enabled
            ),
            cron=(
                config.jobs.integrity_cron
                if not config.jobs.integrity_manual_only
                else None
            ),
            manual_only=config.jobs.integrity_manual_only,
        ),
        SchedulerJobDefinition(
            name=ARCHIVE_BACKUP_JOB,
            enabled=(
                execution_enabled
                and config.scheduled_enabled
                and config.jobs.backup_enabled
                and config.backup.enabled
            ),
            cron=config.jobs.backup_cron,
            manual_only=config.jobs.backup_manual_only,
        ),
    )


def _execution_gate_ready(config: AnnouncementAssetConfig) -> bool:
    if not config.enabled or config.dry_run:
        return False
    try:
        validate_capacity_artifact(config)
    except (CapacityArtifactNotReadyError, OSError, RuntimeError):
        return False
    return True


@dataclass(frozen=True)
class AnnualReportCronAdapter:
    commands: AnnualReportSchedulerCommandService
    service_principal: CommandPrincipal

    def start_daily(
        self,
        *,
        scope: Mapping[str, Any] | None = None,
        bounds: Mapping[str, int] | None = None,
    ) -> JobStartResult:
        return self.commands.start(
            DAILY_UPDATE_JOB,
            principal=self.service_principal,
            trigger_kind="cron",
            scope=_daily_scope(self.commands.config, scope),
            bounds=bounds,
        )

    def start_backup(
        self,
        *,
        scope: Mapping[str, Any] | None = None,
        bounds: Mapping[str, int] | None = None,
    ) -> JobStartResult:
        return self.commands.start(
            ARCHIVE_BACKUP_JOB,
            principal=self.service_principal,
            trigger_kind="cron",
            scope=scope,
            bounds=bounds,
        )

    def start_integrity_audit(
        self,
        *,
        scope: Mapping[str, Any] | None = None,
        bounds: Mapping[str, int] | None = None,
    ) -> JobStartResult:
        return self.commands.start(
            INTEGRITY_AUDIT_JOB,
            principal=self.service_principal,
            trigger_kind="cron",
            scope=scope,
            bounds=bounds,
        )


@dataclass(frozen=True)
class AnnualReportOperatorAdapter:
    """One adapter used by both operator CLI and operator HTTP handlers."""

    commands: AnnualReportSchedulerCommandService

    def start(
        self,
        job_name: str,
        *,
        principal: CommandPrincipal,
        adapter_kind: str,
        scope: Mapping[str, Any] | None = None,
        bounds: Mapping[str, int] | None = None,
        action_flags: Mapping[str, bool] | None = None,
    ) -> JobStartResult:
        if adapter_kind not in {"cli", "api"}:
            raise ValueError("operator adapter_kind must be cli or api")
        normalized_scope = scope
        if job_name == DAILY_UPDATE_JOB:
            normalized_scope = _daily_scope(self.commands.config, scope)
        elif job_name == LATEST_BACKFILL_JOB:
            normalized_scope = _latest_backfill_scope(self.commands.config, scope)
        return self.commands.start(
            job_name,
            principal=principal,
            trigger_kind=adapter_kind,
            scope=normalized_scope,
            bounds=bounds,
            action_flags=action_flags,
        )

    def status(self, run_id: str, *, principal: CommandPrincipal):
        return self.commands.status(run_id, principal=principal)

    def stop(self, run_id: str, *, principal: CommandPrincipal):
        return self.commands.stop(run_id, principal=principal)

    def resume(self, run_id: str, *, principal: CommandPrincipal):
        return self.commands.resume(run_id, principal=principal)


def latest_backfill_schedule_fingerprint(config: AnnouncementAssetConfig) -> str:
    """Versioned identity for the manual-only latest backfill contract."""
    return stable_id(
        "annual-report-latest-backfill-schedule",
        canonical_json(
            {
                "version": "annual_report_asset_latest_backfill_schedule.v1",
                "job": LATEST_BACKFILL_JOB,
                "manual_only": True,
                "cron": None,
                "timezone": config.timezone,
                "config_fingerprint": config.config_fingerprint,
            }
        ),
    )


def daily_schedule_fingerprint(config: AnnouncementAssetConfig) -> str:
    """Versioned cadence identity persisted with each daily operation."""
    return stable_id(
        "annual-report-daily-schedule",
        canonical_json(
            {
                "version": "annual_report_asset_daily_schedule.v1",
                "job": DAILY_UPDATE_JOB,
                "cron": config.jobs.daily_cron,
                "timezone": config.timezone,
                "overlap_days": config.discovery.overlap_days,
                "catch_up_max_days": config.daily_catch_up_max_days,
                "minimum_runs_per_calendar_day": (
                    config.daily_min_runs_per_calendar_day
                ),
                "config_fingerprint": config.config_fingerprint,
            }
        ),
    )


def _daily_scope(
    config: AnnouncementAssetConfig,
    scope: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Attach an immutable schedule contract to a daily run scope."""
    result = dict(scope or {})
    result.setdefault(
        "run_cutoff",
        datetime.now(ZoneInfo(config.timezone)).isoformat(),
    )
    result["schedule_timezone"] = config.timezone
    result["schedule_cron"] = config.jobs.daily_cron
    result["overlap_days"] = config.discovery.overlap_days
    result["catch_up_max_days"] = config.daily_catch_up_max_days
    result["minimum_runs_per_calendar_day"] = config.daily_min_runs_per_calendar_day
    result["cadence_fingerprint"] = daily_schedule_fingerprint(config)
    return result


def _latest_backfill_scope(
    config: AnnouncementAssetConfig,
    scope: Mapping[str, Any] | None,
) -> dict[str, Any]:
    result = dict(scope or {})
    result["manual_only"] = True
    result["cron"] = None
    result["schedule_timezone"] = config.timezone
    result["cadence_fingerprint"] = latest_backfill_schedule_fingerprint(config)
    return result
