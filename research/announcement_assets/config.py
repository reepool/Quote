"""Validated configuration for shared announcement assets."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from research.announcements.categories import normalize_announcement_category
from utils.config_manager import ResearchConfig

from .path_segments import validate_path_segment

DEFAULT_MODULE_KEY = "official_announcement_assets"
CONFIG_SCHEMA_VERSION = "official_announcement_assets.config.v1"

def _bool_value(value: Any, field_name: str, default: bool) -> bool:
    if value is None:
        return default
    if type(value) is not bool:
        raise TypeError(f"{field_name} must be a boolean")
    return value


def _non_empty_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _positive_int(value: Any, field_name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _non_negative_int(value: Any, field_name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _non_negative_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _validate_positive_dataclass_ints(
    instance: Any,
    names: tuple[str, ...],
    *,
    prefix: str,
) -> None:
    for name in names:
        _positive_int(getattr(instance, name), f"{prefix}.{name}")


def _normalized_strings(
    values: Any,
    field_name: str,
    *,
    upper: bool = False,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple)):
        raise TypeError(f"{field_name} must be an array")
    normalized = tuple(
        dict.fromkeys(
            (str(item).strip().upper() if upper else str(item).strip().lower())
            for item in values
        )
    )
    if not normalized or any(not item for item in normalized):
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return value


def _relative_path(value: Any, field_name: str) -> Path:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    normalized_text = text.replace("\\", "/")
    raw_parts = normalized_text.split("/")
    if any(part == "" for part in raw_parts):
        raise ValueError(f"{field_name} contains an empty path component")
    pure = PurePosixPath(normalized_text)
    if pure.is_absolute() or ".." in pure.parts:
        raise ValueError(f"{field_name} must be a safe project-relative path")
    if any(part in {"", "."} for part in pure.parts):
        raise ValueError(f"{field_name} contains an unsafe path component")
    for part in pure.parts:
        validate_path_segment(
            part,
            kind="configured_name",
            field_name=field_name,
        )
    return Path(*pure.parts)


def _resolve_beneath(
    project_root: Path, relative: Path, parent: Path, field: str
) -> Path:
    resolved = (project_root / relative).resolve(strict=False)
    parent_resolved = (project_root / parent).resolve(strict=False)
    try:
        resolved.relative_to(parent_resolved)
    except ValueError as exc:
        raise ValueError(f"{field} must resolve beneath {parent.as_posix()}") from exc
    return resolved


def _validate_at_least_daily_cron(expression: str) -> None:
    """Reject schedules that can omit an entire project calendar day.

    The scheduler adapter uses ordinary five-field crontab syntax.  Requiring
    unrestricted day-of-month/month/day-of-week fields gives a deterministic
    minimum daily cadence while still permitting multiple runs per day through
    the minute/hour fields.
    """

    fields = str(expression).strip().split()
    if len(fields) != 5:
        raise ValueError("daily_cron must use five-field crontab syntax")
    _, _, day_of_month, month, day_of_week = fields
    daily_day_sets = {"*", "0-6", "0-7", "mon-sun", "sun-sat"}
    if day_of_month != "*" or month != "*" or day_of_week.lower() not in daily_day_sets:
        raise ValueError("daily_cron must admit at least one run every calendar day")


@dataclass(frozen=True)
class StorageGateConfig:
    warning_utilization: float = 0.80
    hard_stop_utilization: float = 0.90
    free_space_reserve_bytes: int = 100 * 1024**3
    max_attachment_bytes: int = 200 * 1024**2
    unknown_length_reservation_bytes: int = 64 * 1024**2
    stale_part_max_age_seconds: int = 3600
    stale_part_max_bytes: int = 10 * 1024**3
    part_warning_age_seconds: int = 1800
    part_warning_bytes: int = 5 * 1024**3
    part_safety_grace_seconds: int = 300
    quarantine_warning_age_seconds: int = 7 * 24 * 3600
    quarantine_max_age_seconds: int = 30 * 24 * 3600
    quarantine_warning_bytes: int = 1 * 1024**3
    quarantine_max_bytes: int = 10 * 1024**3
    quarantine_cleanup_policy: str = "operator_audited_only"
    predecessor_cleanup_warning_age_seconds: int = 7 * 24 * 3600
    predecessor_cleanup_hard_age_seconds: int = 30 * 24 * 3600
    # Candidate comparison is bounded separately from canonical downloads.
    candidate_verification_max_bytes: int = 10 * 1024**3
    unprotected_write_reservation_bytes: int = 64 * 1024**2

    def __post_init__(self) -> None:
        if (
            isinstance(self.warning_utilization, bool)
            or isinstance(self.hard_stop_utilization, bool)
            or not isinstance(self.warning_utilization, (int, float))
            or not isinstance(self.hard_stop_utilization, (int, float))
        ):
            raise TypeError("storage utilization thresholds must be numeric")
        if not 0 < self.warning_utilization < self.hard_stop_utilization < 1:
            raise ValueError(
                "storage utilization thresholds must satisfy 0 < warning < stop < 1"
            )
        _validate_positive_dataclass_ints(
            self,
            (
            "free_space_reserve_bytes",
            "max_attachment_bytes",
            "unknown_length_reservation_bytes",
            "stale_part_max_age_seconds",
            "stale_part_max_bytes",
            "part_warning_age_seconds",
            "part_warning_bytes",
            "part_safety_grace_seconds",
            "quarantine_warning_age_seconds",
            "quarantine_max_age_seconds",
            "quarantine_warning_bytes",
            "quarantine_max_bytes",
            "predecessor_cleanup_warning_age_seconds",
            "predecessor_cleanup_hard_age_seconds",
            "candidate_verification_max_bytes",
            "unprotected_write_reservation_bytes",
            ),
            prefix="storage",
        )
        if self.part_safety_grace_seconds >= self.stale_part_max_age_seconds:
            raise ValueError("part safety grace must be below stale part age")
        if self.part_warning_age_seconds > self.stale_part_max_age_seconds:
            raise ValueError("part warning age must not exceed hard maximum age")
        if self.part_warning_bytes > self.stale_part_max_bytes:
            raise ValueError("part warning bytes must not exceed hard maximum bytes")
        if self.quarantine_warning_age_seconds > self.quarantine_max_age_seconds:
            raise ValueError(
                "quarantine warning age must not exceed hard maximum age"
            )
        if self.quarantine_warning_bytes > self.quarantine_max_bytes:
            raise ValueError(
                "quarantine warning bytes must not exceed hard maximum bytes"
            )
        if (
            self.predecessor_cleanup_warning_age_seconds
            >= self.predecessor_cleanup_hard_age_seconds
        ):
            raise ValueError(
                "predecessor cleanup warning age must be below hard age"
            )
        if self.quarantine_cleanup_policy != "operator_audited_only":
            raise ValueError(
                "quarantine_cleanup_policy must be operator_audited_only"
            )

    @property
    def part_max_age_seconds(self) -> int:
        """Public vocabulary used by the storage/readiness contracts."""
        return self.stale_part_max_age_seconds

    @property
    def part_max_bytes(self) -> int:
        """Public vocabulary used by the storage/readiness contracts."""
        return self.stale_part_max_bytes

    @property
    def quarantine_hard_age_seconds(self) -> int:
        return self.quarantine_max_age_seconds

    @property
    def quarantine_hard_bytes(self) -> int:
        return self.quarantine_max_bytes

    @property
    def max_attachment_planned_bytes(self) -> int:
        return self.max_attachment_bytes

    @property
    def max_attachment_actual_bytes(self) -> int:
        return self.max_attachment_bytes








@dataclass(frozen=True)
class ProvisionalResultConfig:
    enabled: bool = True
    policy_version: str = "provisional_effective.v1"

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("provisional result policy_version must be non-empty")


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 4
    initial_backoff_seconds: int = 60
    max_backoff_seconds: int = 3600
    lease_seconds: int = 900
    heartbeat_seconds: int = 60
    lease_safety_grace_seconds: int = 30

    def __post_init__(self) -> None:
        _validate_positive_dataclass_ints(
            self,
            (
                "max_attempts",
                "initial_backoff_seconds",
                "max_backoff_seconds",
                "lease_seconds",
                "heartbeat_seconds",
                "lease_safety_grace_seconds",
            ),
            prefix="retry",
        )
        if not 0 < self.initial_backoff_seconds <= self.max_backoff_seconds:
            raise ValueError("retry backoff bounds are invalid")
        if not 0 < self.heartbeat_seconds < self.lease_seconds:
            raise ValueError("heartbeat_seconds must be below lease_seconds")
        if self.lease_safety_grace_seconds >= self.lease_seconds:
            raise ValueError("lease_safety_grace_seconds must be below lease_seconds")


@dataclass(frozen=True)
class DiscoveryConfig:
    overlap_days: int = 3
    initial_lookback_days: int = 180
    reconciliation_lookback_days: int = 400
    reconciliation_max_cycle_days: int = 30
    max_pages: int = 20
    page_size: int = 30
    max_requests: int = 300
    max_windows: int = 64
    max_instruments: int = 200
    max_elapsed_seconds: int = 1800
    targeted_repair_lookback_years: int = 5
    targeted_repair_max_requests: int = 50
    targeted_repair_max_instruments: int = 100
    targeted_repair_max_elapsed_seconds: int = 600
    provider_coverage_start_year: int = 2000

    def __post_init__(self) -> None:
        positive = (
            "initial_lookback_days",
            "reconciliation_lookback_days",
            "reconciliation_max_cycle_days",
            "max_pages",
            "page_size",
            "max_requests",
            "max_windows",
            "max_instruments",
            "max_elapsed_seconds",
            "targeted_repair_lookback_years",
            "targeted_repair_max_requests",
            "targeted_repair_max_instruments",
            "targeted_repair_max_elapsed_seconds",
            "provider_coverage_start_year",
        )
        _non_negative_int(self.overlap_days, "discovery.overlap_days")
        _validate_positive_dataclass_ints(self, positive, prefix="discovery")


@dataclass(frozen=True)
class AcquisitionConfig:
    source_routes: tuple[str, ...] = ("cninfo", "sse", "szse", "bse")
    normalized_categories: tuple[str, ...] = ("annual_report",)
    download_concurrency: int = 2
    per_source_concurrency: int = 1
    max_task_download_bytes: int = 50 * 1024**3
    source_requests_per_minute: Mapping[str, int] = field(
        default_factory=lambda: {
            "cninfo": 30,
            "sse": 20,
            "szse": 20,
            "bse": 20,
        }
    )

    def __post_init__(self) -> None:
        routes = tuple(
            dict.fromkeys(str(item).strip().lower() for item in self.source_routes)
        )
        categories = tuple(
            dict.fromkeys(
                str(normalize_announcement_category(item) or "").strip().lower()
                for item in self.normalized_categories
            )
        )
        if not routes or any(not item for item in routes):
            raise ValueError("source_routes must be non-empty")
        if categories != ("annual_report",):
            raise ValueError("version 1 supports only annual_report maintenance")
        _validate_positive_dataclass_ints(
            self,
            (
                "download_concurrency",
                "per_source_concurrency",
                "max_task_download_bytes",
            ),
            prefix="acquisition",
        )
        if self.per_source_concurrency > self.download_concurrency:
            raise ValueError(
                "per_source_concurrency cannot exceed download_concurrency"
            )
        rate_limits: dict[str, int] = {}
        for source, limit in self.source_requests_per_minute.items():
            normalized_source = str(source).strip().lower()
            if not normalized_source:
                raise ValueError("source request limit source must be non-empty")
            rate_limits[normalized_source] = _positive_int(
                limit,
                f"acquisition.source_requests_per_minute.{normalized_source}",
            )
        object.__setattr__(self, "source_routes", routes)
        object.__setattr__(self, "normalized_categories", categories)
        object.__setattr__(self, "source_requests_per_minute", rate_limits)

    @property
    def max_task_planned_bytes(self) -> int:
        return self.max_task_download_bytes

    @property
    def max_task_actual_bytes(self) -> int:
        return self.max_task_download_bytes




@dataclass(frozen=True)
class JobConfig:
    # The latest-only bootstrap is an operator entry point only.  Keeping an
    # explicit enable flag separate from ``module.enabled`` prevents an
    # application restart from accidentally registering a one-time backfill.
    latest_backfill_enabled: bool = False
    latest_backfill_manual_only: bool = True
    latest_backfill_cron: str | None = None
    daily_enabled: bool = False
    daily_manual_only: bool = False
    daily_cron: str = "15 3 * * *"

    def __post_init__(self) -> None:
        if not self.latest_backfill_manual_only:
            raise ValueError("latest backfill must remain manual-only")
        if self.latest_backfill_cron is not None:
            raise ValueError("latest backfill manual-only job cannot have a cron")
        if self.daily_manual_only and self.daily_cron:
            raise ValueError("daily job cannot be manual-only when a cron is configured")
        for name in (
            "daily_cron",
            "latest_backfill_cron",
        ):
            cron = getattr(self, name)
            if cron is not None and not str(cron).strip():
                raise ValueError(f"{name} must be a non-empty cron expression")
        if self.daily_cron is not None:
            _validate_at_least_daily_cron(self.daily_cron)


@dataclass(frozen=True)
class TrustedPrincipalConfig:
    principal: str
    token_env: str
    scopes: tuple[str, ...]

    def __post_init__(self) -> None:
        principal = _non_empty_text(self.principal, "permissions.principals.principal")
        token_env = _non_empty_text(self.token_env, "permissions.principals.token_env")
        if not token_env.replace("_", "a").isalnum() or token_env[0].isdigit():
            raise ValueError(
                "permissions.principals.token_env must be an environment variable name"
            )
        scopes = tuple(
            dict.fromkeys(str(item or "").strip() for item in self.scopes)
        )
        if not scopes or any(not item for item in scopes):
            raise ValueError("permissions.principals.scopes must be non-empty")
        object.__setattr__(self, "principal", principal)
        object.__setattr__(self, "token_env", token_env)
        object.__setattr__(self, "scopes", scopes)




@dataclass(frozen=True)
class AnnouncementAssetConfig:
    enabled: bool = False
    scheduled_enabled: bool = False
    dry_run: bool = True
    project_root: Path = Path(".")
    filings_root: Path = Path("data/filings")
    archive_root: Path = Path("data/filings/announcements")
    temp_root: Path = Path("data/filings/announcements/tmp")
    quarantine_root: Path = Path("data/filings/announcements/quarantine")
    expected_filings_mount_source: str | None = None
    require_filings_mount: bool = True
    exchanges: tuple[str, ...] = ("SSE", "SZSE", "BSE")
    instrument_type: str = "stock"
    active_only: bool = True
    universe_policy_version: str = "a_share_active.v1"
    universe_master_data_freshness_hours: int = 36
    master_data_max_age_hours: int = 36
    listed_security_census_max_age_hours: int = 36
    eligibility_indeterminate_policy: str = "retain_last_complete"
    overdue_missing_readiness_policy: str = "degraded"
    classifier_version: str = "formal_annual_report.v1"
    policy_version: str = "annual_report_asset_policy.v1"
    timezone: str = "Asia/Shanghai"
    bootstrap_scope: str = "latest_only_active_a_share"
    bootstrap_filing_season_start_month: int = 1
    bootstrap_filing_season_end_month: int = 5
    bootstrap_max_lookback_years: int = 5
    daily_catch_up_max_days: int = 14
    daily_min_runs_per_calendar_day: int = 1
    universe_refresh_cadence: str = "before_each_daily_run.v1"
    wait_seconds_default: float = 0.0
    wait_seconds_maximum: float = 30.0
    storage: StorageGateConfig = field(default_factory=StorageGateConfig)
    provisional_result: ProvisionalResultConfig = field(default_factory=ProvisionalResultConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    acquisition: AcquisitionConfig = field(default_factory=AcquisitionConfig)
    jobs: JobConfig = field(default_factory=JobConfig)
    trusted_identity_enabled: bool = False
    trusted_principals: tuple[TrustedPrincipalConfig, ...] = ()
    acquire_permission: str = "annual_report_assets:acquire"
    content_permission: str = "annual_report_assets:read_content"
    operator_permission: str = "annual_report_assets:operator"

    def __post_init__(self) -> None:
        root = self.project_root.resolve(strict=False)
        filings = _resolve_beneath(root, self.filings_root, Path("."), "filings_root")
        archive = _resolve_beneath(
            root, self.archive_root, self.filings_root, "archive_root"
        )
        temp = _resolve_beneath(root, self.temp_root, self.archive_root, "temp_root")
        quarantine = _resolve_beneath(
            root, self.quarantine_root, self.archive_root, "quarantine_root"
        )
        if self.trusted_identity_enabled and not self.trusted_principals:
            raise ValueError(
                "permissions.principals are required when trusted identity is enabled"
            )
        principal_names = [item.principal for item in self.trusted_principals]
        token_envs = [item.token_env for item in self.trusted_principals]
        if len(principal_names) != len(set(principal_names)):
            raise ValueError("permissions.principals principal values must be unique")
        if len(token_envs) != len(set(token_envs)):
            raise ValueError("permissions.principals token_env values must be unique")
        for name in (
            "acquire_permission",
            "content_permission",
            "operator_permission",
        ):
            _non_empty_text(getattr(self, name), f"permissions.{name}")
        object.__setattr__(self, "project_root", root)
        object.__setattr__(self, "filings_root", filings)
        object.__setattr__(self, "archive_root", archive)
        object.__setattr__(self, "temp_root", temp)
        object.__setattr__(self, "quarantine_root", quarantine)
        exchanges = tuple(
            dict.fromkeys(str(item).strip().upper() for item in self.exchanges)
        )
        if not exchanges or any(
            item not in {"SSE", "SZSE", "BSE"} for item in exchanges
        ):
            raise ValueError("exchanges must be a non-empty subset of SSE/SZSE/BSE")
        object.__setattr__(self, "exchanges", exchanges)
        if self.scheduled_enabled and not self.enabled:
            raise ValueError("scheduled_enabled requires module enabled")
        if self.jobs.daily_enabled and not self.scheduled_enabled:
            raise ValueError("daily job requires scheduled_enabled")
        if self.universe_master_data_freshness_hours <= 0:
            raise ValueError("universe master-data freshness must be positive")
        if self.master_data_max_age_hours <= 0:
            raise ValueError("master_data_max_age_hours must be positive")
        if self.listed_security_census_max_age_hours <= 0:
            raise ValueError("listed_security_census_max_age_hours must be positive")
        if not 1 <= self.bootstrap_filing_season_start_month <= 12:
            raise ValueError("bootstrap filing-season start month is invalid")
        if not 1 <= self.bootstrap_filing_season_end_month <= 12:
            raise ValueError("bootstrap filing-season end month is invalid")
        if self.bootstrap_filing_season_start_month > self.bootstrap_filing_season_end_month:
            raise ValueError("bootstrap filing-season month bounds are invalid")
        if self.bootstrap_max_lookback_years <= 0:
            raise ValueError("bootstrap_max_lookback_years must be positive")
        if self.bootstrap_scope != "latest_only_active_a_share":
            raise ValueError("bootstrap_scope is unsupported")
        if self.daily_catch_up_max_days <= 0:
            raise ValueError("daily_catch_up_max_days must be positive")
        if self.daily_min_runs_per_calendar_day < 1:
            raise ValueError("daily_min_runs_per_calendar_day must be positive")
        if self.universe_refresh_cadence != "before_each_daily_run.v1":
            raise ValueError("unsupported universe_refresh_cadence")
        wait_default = _non_negative_number(
            self.wait_seconds_default, "wait_seconds_default"
        )
        wait_maximum = _non_negative_number(
            self.wait_seconds_maximum, "wait_seconds_maximum"
        )
        if wait_maximum <= 0:
            raise ValueError("wait_seconds_maximum must be positive")
        if wait_default > wait_maximum:
            raise ValueError(
                "wait_seconds_default cannot exceed wait_seconds_maximum"
            )
        object.__setattr__(self, "wait_seconds_default", wait_default)
        object.__setattr__(self, "wait_seconds_maximum", wait_maximum)
        if self.eligibility_indeterminate_policy not in {
            "retain_last_complete",
            "block_full_market",
        }:
            raise ValueError("invalid eligibility_indeterminate_policy")
        if self.overdue_missing_readiness_policy not in {"degraded", "blocked"}:
            raise ValueError("invalid overdue_missing_readiness_policy")

    @property
    def blob_root(self) -> Path:
        return self.archive_root / "blobs"

    def normalized_mapping(self) -> dict[str, Any]:
        """Return a JSON-safe canonical mapping used for restart/fingerprint checks."""

        def path(value: Path | None) -> str | None:
            if value is None:
                return None
            try:
                return value.resolve(strict=False).relative_to(self.project_root).as_posix()
            except ValueError:
                return value.as_posix()

        return {
            "schema_version": CONFIG_SCHEMA_VERSION,
            "enabled": self.enabled,
            "scheduled_enabled": self.scheduled_enabled,
            "dry_run": self.dry_run,
            "active_exchanges": list(self.exchanges),
            "instrument_type": self.instrument_type,
            "active_only": self.active_only,
            "universe_policy_version": self.universe_policy_version,
            "master_data_max_age_hours": self.master_data_max_age_hours,
            "listed_security_census_max_age_hours": self.listed_security_census_max_age_hours,
            "eligibility_indeterminate_policy": self.eligibility_indeterminate_policy,
            "overdue_missing_readiness_policy": self.overdue_missing_readiness_policy,
            "classifier_version": self.classifier_version,
            "policy_version": self.policy_version,
            "timezone": self.timezone,
            "bootstrap_scope": self.bootstrap_scope,
            "bootstrap_filing_season_start_month": self.bootstrap_filing_season_start_month,
            "bootstrap_filing_season_end_month": self.bootstrap_filing_season_end_month,
            "bootstrap_max_lookback_years": self.bootstrap_max_lookback_years,
            "daily_catch_up_max_days": self.daily_catch_up_max_days,
            "daily_min_runs_per_calendar_day": self.daily_min_runs_per_calendar_day,
            "universe_refresh_cadence": self.universe_refresh_cadence,
            "wait_seconds_default": self.wait_seconds_default,
            "wait_seconds_maximum": self.wait_seconds_maximum,
            "paths": {
                "filings_root": path(self.filings_root),
                "archive_root": path(self.archive_root),
                "temp_root": path(self.temp_root),
                "quarantine_root": path(self.quarantine_root),
                "expected_mount_source": self.expected_filings_mount_source,
                "require_mount": self.require_filings_mount,
            },
            "storage": {
                "warning_utilization": self.storage.warning_utilization,
                "hard_stop_utilization": self.storage.hard_stop_utilization,
                "free_space_reserve_bytes": self.storage.free_space_reserve_bytes,
                "max_attachment_bytes": self.storage.max_attachment_bytes,
                "max_attachment_planned_bytes": self.storage.max_attachment_planned_bytes,
                "max_attachment_actual_bytes": self.storage.max_attachment_actual_bytes,
                "unknown_length_reservation_bytes": self.storage.unknown_length_reservation_bytes,
                "part_max_age_seconds": self.storage.part_max_age_seconds,
                "part_max_bytes": self.storage.part_max_bytes,
                "part_safety_grace_seconds": self.storage.part_safety_grace_seconds,
                "part_warning_age_seconds": self.storage.part_warning_age_seconds,
                "part_warning_bytes": self.storage.part_warning_bytes,
                "quarantine_warning_age_seconds": self.storage.quarantine_warning_age_seconds,
                "quarantine_hard_age_seconds": self.storage.quarantine_hard_age_seconds,
                "quarantine_warning_bytes": self.storage.quarantine_warning_bytes,
                "quarantine_hard_bytes": self.storage.quarantine_hard_bytes,
                "quarantine_cleanup_policy": self.storage.quarantine_cleanup_policy,
                "predecessor_cleanup_warning_age_seconds": (
                    self.storage.predecessor_cleanup_warning_age_seconds
                ),
                "predecessor_cleanup_hard_age_seconds": (
                    self.storage.predecessor_cleanup_hard_age_seconds
                ),
                "candidate_verification_max_bytes": self.storage.candidate_verification_max_bytes,
                "unprotected_write_reservation_bytes": self.storage.unprotected_write_reservation_bytes,
            },
            "provisional_result": {
                "enabled": self.provisional_result.enabled,
                "policy_version": self.provisional_result.policy_version,
            },
            "discovery": {
                name: getattr(self.discovery, name)
                for name in self.discovery.__dataclass_fields__
            },
            "acquisition": {
                "source_routes": list(self.acquisition.source_routes),
                "normalized_categories": list(self.acquisition.normalized_categories),
                "download_concurrency": self.acquisition.download_concurrency,
                "per_source_concurrency": self.acquisition.per_source_concurrency,
                "max_task_download_bytes": self.acquisition.max_task_download_bytes,
                "max_task_planned_bytes": self.acquisition.max_task_planned_bytes,
                "max_task_actual_bytes": self.acquisition.max_task_actual_bytes,
                "source_requests_per_minute": dict(self.acquisition.source_requests_per_minute),
            },
            "retry": {name: getattr(self.retry, name) for name in self.retry.__dataclass_fields__},
            "jobs": {name: getattr(self.jobs, name) for name in self.jobs.__dataclass_fields__},
            "permissions": {
                "trusted_identity_enabled": self.trusted_identity_enabled,
                "principals": [
                    {
                        "principal": item.principal,
                        "token_env": item.token_env,
                        "scopes": list(item.scopes),
                    }
                    for item in self.trusted_principals
                ],
                "acquire": self.acquire_permission,
                "read_content": self.content_permission,
                "operator": self.operator_permission,
            },
        }

    @property
    def config_fingerprint(self) -> str:
        payload = json.dumps(
            self.normalized_mapping(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @property
    def evidence_fingerprint(self) -> str:
        """Fingerprint policy while excluding runtime enable switches."""

        mapping = self.normalized_mapping()
        mapping["enabled"] = False
        mapping["scheduled_enabled"] = False
        mapping["dry_run"] = True
        # This bounds one execution batch without changing asset identity,
        # storage, or provider routing.
        mapping["discovery"]["max_requests"] = 300
        for name in (
            "latest_backfill_enabled",
            "daily_enabled",
        ):
            mapping["jobs"][name] = False
        mapping["permissions"]["trusted_identity_enabled"] = False
        mapping["permissions"]["principals"] = []
        payload = json.dumps(
            mapping,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @classmethod
    def from_research_config(
        cls,
        research_config: ResearchConfig,
        *,
        project_root: str | Path = ".",
    ) -> AnnouncementAssetConfig:
        modules = research_config.modules or {}
        raw = _mapping(modules.get(DEFAULT_MODULE_KEY), f"modules.{DEFAULT_MODULE_KEY}")
        return cls.from_mapping(raw, project_root=project_root)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        project_root: str | Path = ".",
    ) -> AnnouncementAssetConfig:
        raw = _mapping(value, DEFAULT_MODULE_KEY)
        schema_version = raw.get("schema_version")
        if schema_version is not None and schema_version != CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported announcement asset configuration schema_version")
        timezone_name = _non_empty_text(
            raw.get("timezone", "Asia/Shanghai"), "timezone"
        )
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be a valid IANA timezone") from exc
        master_data_max_age_hours = _positive_int(
            raw.get(
                "master_data_max_age_hours",
                raw.get("universe_master_data_freshness_hours", 36),
            ),
            "master_data_max_age_hours",
        )
        legacy_master_freshness_hours = _positive_int(
            raw.get(
                "universe_master_data_freshness_hours",
                master_data_max_age_hours,
            ),
            "universe_master_data_freshness_hours",
        )
        if legacy_master_freshness_hours != master_data_max_age_hours:
            raise ValueError(
                "master_data_max_age_hours conflicts with "
                "universe_master_data_freshness_hours"
            )
        paths = _mapping(raw.get("paths"), "paths")
        storage_raw = dict(_mapping(raw.get("storage"), "storage"))
        # Accept the contract vocabulary while retaining the original V1
        # ``stale_part``/``quarantine_max`` names for existing deployments.
        for public_name, legacy_name in (
            ("part_max_age_seconds", "stale_part_max_age_seconds"),
            ("part_max_bytes", "stale_part_max_bytes"),
            ("quarantine_hard_age_seconds", "quarantine_max_age_seconds"),
            ("quarantine_hard_bytes", "quarantine_max_bytes"),
            ("max_attachment_planned_bytes", "max_attachment_bytes"),
            ("max_attachment_actual_bytes", "max_attachment_bytes"),
        ):
            if public_name not in storage_raw:
                continue
            if (
                legacy_name in storage_raw
                and int(storage_raw[legacy_name]) != int(storage_raw[public_name])
            ):
                raise ValueError(
                    f"storage.{public_name} conflicts with storage.{legacy_name}"
                )
            storage_raw[legacy_name] = storage_raw.pop(public_name)
        provisional_result_raw = _mapping(
            raw.get("provisional_result"), "provisional_result"
        )
        retry_raw = _mapping(raw.get("retry"), "retry")
        discovery_raw = _mapping(raw.get("discovery"), "discovery")
        jobs_raw = _mapping(raw.get("jobs"), "jobs")
        acquisition_raw = dict(
            _mapping(raw.get("acquisition"), "acquisition")
        )
        for alias in ("max_task_planned_bytes", "max_task_actual_bytes"):
            if alias not in acquisition_raw:
                continue
            if (
                "max_task_download_bytes" in acquisition_raw
                and int(acquisition_raw["max_task_download_bytes"])
                != int(acquisition_raw[alias])
            ):
                raise ValueError(
                    f"acquisition.{alias} conflicts with acquisition.max_task_download_bytes"
                )
            acquisition_raw["max_task_download_bytes"] = acquisition_raw.pop(alias)
        permissions = _mapping(raw.get("permissions"), "permissions")
        principals_raw = permissions.get("principals", [])
        if not isinstance(principals_raw, list):
            raise TypeError("permissions.principals must be an array")
        if any(not isinstance(item, Mapping) for item in principals_raw):
            raise TypeError("permissions.principals entries must be mappings")
        if any(
            isinstance(item.get("scopes"), (str, bytes))
            or not isinstance(item.get("scopes"), (list, tuple))
            for item in principals_raw
        ):
            raise TypeError("permissions.principals.scopes must be an array")

        filings_rel = _relative_path(
            paths.get("filings_root", "data/filings"), "filings_root"
        )
        archive_rel = _relative_path(
            paths.get("archive_root", "data/filings/announcements"), "archive_root"
        )
        temp_rel = _relative_path(
            paths.get("temp_root", "data/filings/announcements/tmp"), "temp_root"
        )
        quarantine_rel = _relative_path(
            paths.get("quarantine_root", "data/filings/announcements/quarantine"),
            "quarantine_root",
        )
        project = Path(project_root)

        return cls(
            enabled=_bool_value(raw.get("enabled"), "enabled", False),
            scheduled_enabled=_bool_value(
                raw.get("scheduled_enabled"), "scheduled_enabled", False
            ),
            dry_run=_bool_value(raw.get("dry_run"), "dry_run", True),
            project_root=project,
            filings_root=filings_rel,
            archive_root=archive_rel,
            temp_root=temp_rel,
            quarantine_root=quarantine_rel,
            expected_filings_mount_source=(
                str(paths.get("expected_mount_source")).strip()
                if paths.get("expected_mount_source")
                else None
            ),
            require_filings_mount=_bool_value(
                paths.get("require_mount"), "paths.require_mount", True
            ),
            exchanges=_normalized_strings(
                raw.get("active_exchanges", ("SSE", "SZSE", "BSE")),
                "active_exchanges",
                upper=True,
            ),
            instrument_type=_non_empty_text(
                raw.get("instrument_type", "stock"), "instrument_type"
            ).lower(),
            active_only=_bool_value(raw.get("active_only"), "active_only", True),
            universe_policy_version=_non_empty_text(
                raw.get("universe_policy_version", "a_share_active.v1"),
                "universe_policy_version",
            ),
            universe_master_data_freshness_hours=legacy_master_freshness_hours,
            master_data_max_age_hours=master_data_max_age_hours,
            listed_security_census_max_age_hours=_positive_int(
                raw.get("listed_security_census_max_age_hours", 36),
                "listed_security_census_max_age_hours",
            ),
            eligibility_indeterminate_policy=str(
                raw.get("eligibility_indeterminate_policy", "retain_last_complete")
            ),
            overdue_missing_readiness_policy=str(
                raw.get("overdue_missing_readiness_policy", "degraded")
            ),
            classifier_version=_non_empty_text(
                raw.get("classifier_version", "formal_annual_report.v1"),
                "classifier_version",
            ),
            policy_version=_non_empty_text(
                raw.get("policy_version", "annual_report_asset_policy.v1"),
                "policy_version",
            ),
            timezone=timezone_name,
            bootstrap_scope=str(
                raw.get("bootstrap_scope", "latest_only_active_a_share")
            ),
            bootstrap_filing_season_start_month=int(
                raw.get("bootstrap_filing_season_start_month", 1)
            ),
            bootstrap_filing_season_end_month=int(
                raw.get("bootstrap_filing_season_end_month", 5)
            ),
            bootstrap_max_lookback_years=int(
                raw.get("bootstrap_max_lookback_years", 5)
            ),
            daily_catch_up_max_days=_positive_int(
                raw.get("daily_catch_up_max_days", 14),
                "daily_catch_up_max_days",
            ),
            daily_min_runs_per_calendar_day=_positive_int(
                raw.get("daily_min_runs_per_calendar_day", 1),
                "daily_min_runs_per_calendar_day",
            ),
            universe_refresh_cadence=_non_empty_text(
                raw.get(
                    "universe_refresh_cadence", "before_each_daily_run.v1"
                ),
                "universe_refresh_cadence",
            ),
            wait_seconds_default=_non_negative_number(
                raw.get("wait_seconds_default", 0.0), "wait_seconds_default"
            ),
            wait_seconds_maximum=_non_negative_number(
                raw.get("wait_seconds_maximum", 30.0), "wait_seconds_maximum"
            ),
            storage=StorageGateConfig(**storage_raw),
            provisional_result=ProvisionalResultConfig(
                enabled=_bool_value(
                    provisional_result_raw.get("enabled"),
                    "provisional_result.enabled",
                    True,
                ),
                policy_version=str(
                    provisional_result_raw.get(
                        "policy_version", "provisional_effective.v1"
                    )
                ),
            ),
            retry=RetryConfig(**retry_raw),
            discovery=DiscoveryConfig(**discovery_raw),
            acquisition=AcquisitionConfig(
                source_routes=tuple(
                    acquisition_raw.get(
                        "source_routes", ("cninfo", "sse", "szse", "bse")
                    )
                ),
                normalized_categories=tuple(
                    acquisition_raw.get(
                        "normalized_categories", ("annual_report",)
                    )
                ),
                download_concurrency=int(
                    acquisition_raw.get("download_concurrency", 2)
                ),
                per_source_concurrency=int(
                    acquisition_raw.get("per_source_concurrency", 1)
                ),
                max_task_download_bytes=int(
                    acquisition_raw.get(
                        "max_task_download_bytes", 50 * 1024**3
                    )
                ),
                source_requests_per_minute=_mapping(
                    acquisition_raw.get("source_requests_per_minute"),
                    "acquisition.source_requests_per_minute",
                )
                or {
                    "cninfo": 30,
                    "sse": 20,
                    "szse": 20,
                    "bse": 20,
                },
            ),
            jobs=JobConfig(**jobs_raw),
            trusted_identity_enabled=_bool_value(
                permissions.get("trusted_identity_enabled"),
                "permissions.trusted_identity_enabled",
                False,
            ),
            trusted_principals=tuple(
                TrustedPrincipalConfig(
                    principal=str(item.get("principal") or ""),
                    token_env=str(item.get("token_env") or ""),
                    scopes=tuple(item.get("scopes") or ()),
                )
                for item in principals_raw
            ),
            acquire_permission=str(
                permissions.get("acquire", "annual_report_assets:acquire")
            ),
            content_permission=str(
                permissions.get("read_content", "annual_report_assets:read_content")
            ),
            operator_permission=str(
                permissions.get("operator", "annual_report_assets:operator")
            ),
        )
