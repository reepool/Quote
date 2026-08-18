"""Validated configuration for shared announcement assets."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from research.announcements.categories import normalize_announcement_category
from utils.config_manager import ResearchConfig

from .path_segments import validate_path_segment

DEFAULT_MODULE_KEY = "official_announcement_assets"
CONFIG_SCHEMA_VERSION = "official_announcement_assets.config.v1"
BACKUP_PROTECTION_STATE_SCHEMA_VERSION = (
    "official_announcement_assets.backup_protection_state.v1"
)
LEGACY_ARCHIVE_REGISTRY_VERSION = "legacy_annual_report_roots.v1"
LEGACY_ARCHIVE_TEMPLATE_VERSION = "legacy_annual_report_paths.v1"
LEGACY_ARCHIVE_EXCLUSION_POLICY_VERSION = "legacy_annual_report_exclusions.v1"

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


def _unprotected_limit(value: Any, field_name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError("backup unprotected limits must be positive")
    return value


def _aware_datetime(value: Any, field_name: str) -> datetime:
    text = _non_empty_text(value, field_name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return parsed


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


def _relative_to_project(path: Path, project_root: Path, field_name: str) -> str:
    try:
        return path.resolve(strict=False).relative_to(
            project_root.resolve(strict=False)
        ).as_posix()
    except ValueError as exc:
        raise ValueError(f"{field_name} must remain beneath project_root") from exc


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


def _is_isolated_test_root(project_root: Path) -> bool:
    root = project_root.resolve(strict=False)
    return any(
        root == allowed or allowed in root.parents
        for allowed in (Path("/tmp"), Path("/dev/shm"))
    )


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
    # Candidate verification and backup-failure writes have their own byte
    # budget.  Keep the public aliases explicit so configuration fingerprints
    # do not depend on the original legacy field spelling.
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
class BackupConfig:
    enabled: bool = False
    scheduled_enabled: bool = False
    mount_root: Path | None = None
    destination_root: Path | None = None
    expected_mount_source: str | None = None
    expected_failure_domain: str | None = None
    warning_utilization: float = 0.80
    hard_stop_utilization: float = 0.90
    free_space_reserve_bytes: int = 50 * 1024**3
    freshness_hours: int = 48
    max_unprotected_bytes: int = 10 * 1024**3
    max_unprotected_age_seconds: int = 72 * 3600
    unprotected_accumulation_origin: str = "first_unprotected_at"
    reset_on_verified_backup: bool = True
    unblock_requires_verified_backup: bool = True
    recovery_journal_retention_policy: str = "append_only_no_automatic_gc.v1"
    recovery_journal_integrity_policy: str = "sha256_chain_with_watermarks.v1"

    def __post_init__(self) -> None:
        if self.scheduled_enabled and not self.enabled:
            raise ValueError("backup scheduling requires backup.enabled")
        if self.enabled and (self.mount_root is None or self.destination_root is None):
            raise ValueError("enabled backup requires mount_root and destination_root")
        if self.enabled and not self.expected_mount_source:
            raise ValueError("enabled backup requires expected_mount_source")
        if self.enabled and not self.expected_failure_domain:
            raise ValueError("enabled backup requires expected_failure_domain")
        if (
            isinstance(self.warning_utilization, bool)
            or isinstance(self.hard_stop_utilization, bool)
            or not isinstance(self.warning_utilization, (int, float))
            or not isinstance(self.hard_stop_utilization, (int, float))
        ):
            raise TypeError("backup utilization thresholds must be numeric")
        if not 0 < self.warning_utilization < self.hard_stop_utilization < 1:
            raise ValueError(
                "backup utilization thresholds must satisfy 0 < warning < stop < 1"
            )
        _validate_positive_dataclass_ints(
            self,
            (
                "free_space_reserve_bytes",
                "freshness_hours",
            ),
            prefix="backup",
        )
        for name in ("max_unprotected_bytes", "max_unprotected_age_seconds"):
            _unprotected_limit(getattr(self, name), f"backup.{name}")
        if self.unprotected_accumulation_origin not in {
            "first_unprotected_at",
            "first_failed_backup_at",
        }:
            raise ValueError("invalid unprotected_accumulation_origin")
        if not self.reset_on_verified_backup or not self.unblock_requires_verified_backup:
            raise ValueError(
                "runtime unprotected policy must reset and unblock only after verified backup"
            )
        if (
            self.recovery_journal_retention_policy
            != "append_only_no_automatic_gc.v1"
        ):
            raise ValueError("unsupported recovery journal retention policy")
        if (
            self.recovery_journal_integrity_policy
            != "sha256_chain_with_watermarks.v1"
        ):
            raise ValueError("unsupported recovery journal integrity policy")


@dataclass(frozen=True)
class BackupProtectionRuntimeState:
    """Restart-safe projection of the bounded unprotected-write policy.

    The repository or scheduler owns durable storage for this JSON-safe state.
    This value object owns the fail-closed transition rules so a restart cannot
    reset accumulated bytes, elapsed age, or an already-raised blocker.
    """

    config_fingerprint: str
    unprotected_bytes: int = 0
    accumulation_started_at: str | None = None
    blocked: bool = False
    blocker_reasons: tuple[str, ...] = ()
    last_backup_attempt_at: str | None = None
    last_verified_backup_at: str | None = None
    schema_version: str = BACKUP_PROTECTION_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BACKUP_PROTECTION_STATE_SCHEMA_VERSION:
            raise ValueError("unsupported backup protection state schema_version")
        _non_empty_text(self.config_fingerprint, "config_fingerprint")
        _non_negative_int(self.unprotected_bytes, "unprotected_bytes")
        for name in (
            "accumulation_started_at",
            "last_backup_attempt_at",
            "last_verified_backup_at",
        ):
            value = getattr(self, name)
            if value is not None:
                _aware_datetime(value, name)
        normalized_reasons = tuple(
            sorted(
                dict.fromkeys(
                    _non_empty_text(item, "blocker_reasons")
                    for item in self.blocker_reasons
                )
            )
        )
        if self.blocked != bool(normalized_reasons):
            raise ValueError("blocked state and blocker_reasons must agree")
        if self.unprotected_bytes == 0 and self.accumulation_started_at is not None:
            raise ValueError("zero unprotected bytes cannot retain an accumulation origin")
        object.__setattr__(self, "blocker_reasons", normalized_reasons)

    @classmethod
    def fresh(cls, config: AnnouncementAssetConfig) -> BackupProtectionRuntimeState:
        return cls(config_fingerprint=config.evidence_fingerprint)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        config: AnnouncementAssetConfig,
        now: str,
    ) -> BackupProtectionRuntimeState:
        raw = _mapping(value, "backup_protection_state")
        allowed = {
            "schema_version",
            "config_fingerprint",
            "unprotected_bytes",
            "accumulation_started_at",
            "blocked",
            "blocker_reasons",
            "last_backup_attempt_at",
            "last_verified_backup_at",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise ValueError(
                "backup protection state contains unknown fields: "
                + ", ".join(unknown)
            )
        fingerprint = _non_empty_text(
            raw.get("config_fingerprint"), "config_fingerprint"
        )
        if fingerprint != config.evidence_fingerprint:
            raise ValueError("backup protection state configuration fingerprint mismatch")
        reasons = raw.get("blocker_reasons", ())
        if isinstance(reasons, (str, bytes)) or not isinstance(
            reasons, (list, tuple)
        ):
            raise TypeError("blocker_reasons must be an array")
        state = cls(
            schema_version=str(raw.get("schema_version", "")),
            config_fingerprint=fingerprint,
            unprotected_bytes=_non_negative_int(
                raw.get("unprotected_bytes"), "unprotected_bytes"
            ),
            accumulation_started_at=raw.get("accumulation_started_at"),
            blocked=_bool_value(raw.get("blocked"), "blocked", False),
            blocker_reasons=tuple(reasons),
            last_backup_attempt_at=raw.get("last_backup_attempt_at"),
            last_verified_backup_at=raw.get("last_verified_backup_at"),
        )
        return state.evaluate(config=config, now=now)

    def normalized_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "config_fingerprint": self.config_fingerprint,
            "unprotected_bytes": self.unprotected_bytes,
            "accumulation_started_at": self.accumulation_started_at,
            "blocked": self.blocked,
            "blocker_reasons": list(self.blocker_reasons),
            "last_backup_attempt_at": self.last_backup_attempt_at,
            "last_verified_backup_at": self.last_verified_backup_at,
        }

    def record_unprotected_bytes(
        self,
        byte_count: int,
        *,
        observed_at: str,
        config: AnnouncementAssetConfig,
    ) -> BackupProtectionRuntimeState:
        self._validate_config(config)
        added = _positive_int(byte_count, "byte_count")
        _aware_datetime(observed_at, "observed_at")
        origin = self.accumulation_started_at
        if (
            origin is None
            and config.backup.unprotected_accumulation_origin
            == "first_unprotected_at"
        ):
            origin = observed_at
        updated = replace(
            self,
            unprotected_bytes=self.unprotected_bytes + added,
            accumulation_started_at=origin,
        )
        return updated.evaluate(config=config, now=observed_at)

    def record_backup_attempt(
        self,
        *,
        attempted_at: str,
        verified_closure: bool,
        config: AnnouncementAssetConfig,
    ) -> BackupProtectionRuntimeState:
        self._validate_config(config)
        _aware_datetime(attempted_at, "attempted_at")
        if type(verified_closure) is not bool:
            raise TypeError("verified_closure must be a boolean")
        if verified_closure:
            return replace(
                self,
                unprotected_bytes=0,
                accumulation_started_at=None,
                blocked=False,
                blocker_reasons=(),
                last_backup_attempt_at=attempted_at,
                last_verified_backup_at=attempted_at,
            )
        origin = self.accumulation_started_at
        if (
            origin is None
            and self.unprotected_bytes > 0
            and config.backup.unprotected_accumulation_origin
            == "first_failed_backup_at"
        ):
            origin = attempted_at
        updated = replace(
            self,
            accumulation_started_at=origin,
            last_backup_attempt_at=attempted_at,
        )
        return updated.evaluate(config=config, now=attempted_at)

    def evaluate(
        self,
        *,
        config: AnnouncementAssetConfig,
        now: str,
    ) -> BackupProtectionRuntimeState:
        self._validate_config(config)
        current = _aware_datetime(now, "now")
        reasons = set(self.blocker_reasons)
        if self.unprotected_bytes >= config.backup.max_unprotected_bytes:
            reasons.add("max_unprotected_bytes_reached")
        if self.accumulation_started_at is not None:
            started = _aware_datetime(
                self.accumulation_started_at, "accumulation_started_at"
            )
            elapsed = (current - started).total_seconds()
            if elapsed < 0:
                raise ValueError("now cannot precede accumulation_started_at")
            if elapsed >= config.backup.max_unprotected_age_seconds:
                reasons.add("max_unprotected_age_reached")
        normalized = tuple(sorted(reasons))
        return replace(
            self,
            blocked=bool(normalized),
            blocker_reasons=normalized,
        )

    def _validate_config(self, config: AnnouncementAssetConfig) -> None:
        if self.config_fingerprint != config.evidence_fingerprint:
            raise ValueError("backup protection state configuration fingerprint mismatch")


@dataclass(frozen=True)
class CapacityOverrideConfig:
    """Explicit, bounded operator override for a single acquisition operation."""

    enabled: bool = False
    max_bytes: int = 2 * 1024**3
    max_duration_seconds: int = 3600
    requires_operator: bool = True
    audit_required: bool = True
    scope_mode: str = "single_operation_and_target"

    def __post_init__(self) -> None:
        _validate_positive_dataclass_ints(
            self,
            ("max_bytes", "max_duration_seconds"),
            prefix="capacity_override",
        )
        if self.scope_mode != "single_operation_and_target":
            raise ValueError("capacity override scope_mode is unsupported")
        if not self.requires_operator or not self.audit_required:
            raise ValueError("capacity override requires operator authorization and audit")


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
    reconciliation_cohort_size: int = 200
    reconciliation_max_cycle_days: int = 30
    missing_repair_cohort_size: int = 100
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
            "reconciliation_cohort_size",
            "reconciliation_max_cycle_days",
            "missing_repair_cohort_size",
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
class RolloutGateConfig:
    require_bootstrap: bool = True
    require_integrity: bool = True
    require_storage: bool = True
    require_backup: bool = True
    require_consumer_migration: bool = True
    consumer_dependency_policy: str = "completed_assets_only"

    def __post_init__(self) -> None:
        if self.consumer_dependency_policy not in {
            "completed_assets_only",
            "wait_for_full_success",
            "disabled",
        }:
            raise ValueError("invalid consumer_dependency_policy")


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
    backup_manual_only: bool = False
    backup_cron: str = "45 4 * * *"
    integrity_enabled: bool = False
    integrity_manual_only: bool = True
    integrity_cron: str | None = None
    integrity_read_only: bool = True
    backup_enabled: bool = False

    def __post_init__(self) -> None:
        if not self.latest_backfill_manual_only:
            raise ValueError("latest backfill must remain manual-only")
        if self.latest_backfill_cron is not None:
            raise ValueError("latest backfill manual-only job cannot have a cron")
        if self.daily_manual_only and self.daily_cron:
            raise ValueError("daily job cannot be manual-only when a cron is configured")
        if self.backup_manual_only and self.backup_cron:
            raise ValueError("backup job cannot be manual-only when a cron is configured")
        if self.integrity_manual_only and self.integrity_cron:
            raise ValueError(
                "integrity audit cannot be manual-only when a cron is configured"
            )
        for name in (
            "daily_cron",
            "backup_cron",
            "latest_backfill_cron",
            "integrity_cron",
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
class LegacyArchiveRegistryConfig:
    """Versioned ownership and layout policy for legacy annual-report roots."""

    registry_version: str = LEGACY_ARCHIVE_REGISTRY_VERSION
    path_template_version: str = LEGACY_ARCHIVE_TEMPLATE_VERSION
    exclusion_policy_version: str = LEGACY_ARCHIVE_EXCLUSION_POLICY_VERSION
    business_profile_root: Path = Path("data/filings/business_profile")
    broker_risk_control_root: Path = Path(
        "data/filings/financial_statements/broker_risk_control"
    )
    business_profile_template: str = "business_profile/{fiscal_year}/{exchange}/"
    broker_risk_control_template: str = (
        "broker_risk_control/{exchange}/{symbol}/"
    )
    allowed_document_families: tuple[str, ...] = ("annual_report",)
    business_profile_excluded_subtrees: tuple[str, ...] = ("derived",)
    broker_excluded_document_families: tuple[str, ...] = ("semiannual_report",)

    def __post_init__(self) -> None:
        for name in (
            "registry_version",
            "path_template_version",
            "exclusion_policy_version",
        ):
            _non_empty_text(getattr(self, name), f"legacy_inventory.{name}")
        expected_placeholders = {
            "business_profile_template": ("{fiscal_year}", "{exchange}"),
            "broker_risk_control_template": ("{exchange}", "{symbol}"),
        }
        for name, placeholders in expected_placeholders.items():
            template = _non_empty_text(
                getattr(self, name), f"legacy_inventory.{name}"
            )
            if template.startswith("/") or ".." in PurePosixPath(template).parts:
                raise ValueError(f"legacy_inventory.{name} must be relative")
            if any(placeholder not in template for placeholder in placeholders):
                raise ValueError(
                    f"legacy_inventory.{name} is missing required placeholders"
                )
        allowed = tuple(
            dict.fromkeys(
                str(item or "").strip().lower()
                for item in self.allowed_document_families
            )
        )
        if allowed != ("annual_report",):
            raise ValueError("version 1 legacy inventory accepts only annual_report")
        excluded_subtrees = tuple(
            dict.fromkeys(
                str(item or "").strip().lower()
                for item in self.business_profile_excluded_subtrees
            )
        )
        if "derived" not in excluded_subtrees or any(
            not item or "/" in item or item in {".", ".."}
            for item in excluded_subtrees
        ):
            raise ValueError(
                "business-profile legacy exclusions must include derived"
            )
        broker_excluded = tuple(
            dict.fromkeys(
                str(item or "").strip().lower()
                for item in self.broker_excluded_document_families
            )
        )
        if "semiannual_report" not in broker_excluded:
            raise ValueError("broker legacy exclusions must include semiannual_report")
        object.__setattr__(self, "allowed_document_families", allowed)
        object.__setattr__(
            self, "business_profile_excluded_subtrees", excluded_subtrees
        )
        object.__setattr__(
            self, "broker_excluded_document_families", broker_excluded
        )

    @property
    def roots(self) -> tuple[tuple[str, Path], ...]:
        return (
            ("business_profile", self.business_profile_root),
            ("broker_risk_control", self.broker_risk_control_root),
        )

    def normalized_mapping(self, *, project_root: Path) -> dict[str, Any]:
        return {
            "registry_version": self.registry_version,
            "path_template_version": self.path_template_version,
            "exclusion_policy_version": self.exclusion_policy_version,
            "roots": {
                "business_profile": {
                    "base_root": _relative_to_project(
                        self.business_profile_root,
                        project_root,
                        "legacy_inventory.roots.business_profile.base_root",
                    ),
                    "path_template": self.business_profile_template,
                },
                "broker_risk_control": {
                    "base_root": _relative_to_project(
                        self.broker_risk_control_root,
                        project_root,
                        "legacy_inventory.roots.broker_risk_control.base_root",
                    ),
                    "path_template": self.broker_risk_control_template,
                },
            },
            "exclusions": {
                "allowed_document_families": list(
                    self.allowed_document_families
                ),
                "business_profile_subtrees": list(
                    self.business_profile_excluded_subtrees
                ),
                "broker_document_families": list(
                    self.broker_excluded_document_families
                ),
            },
        }

    def fingerprint(self, *, project_root: Path) -> str:
        payload = json.dumps(
            self.normalized_mapping(project_root=project_root),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class AnnouncementAssetConfig:
    enabled: bool = False
    scheduled_enabled: bool = False
    dry_run: bool = True
    project_root: Path = Path(".")
    capacity_artifact_required: bool = False
    capacity_artifact_path: Path = Path(
        "config/runtime_evidence/official_announcement_asset_capacity.json"
    )
    capacity_artifact_max_age_hours: int = 24
    filings_root: Path = Path("data/filings")
    archive_root: Path = Path("data/filings/announcements")
    temp_root: Path = Path("data/filings/announcements/tmp")
    quarantine_root: Path = Path("data/filings/announcements/quarantine")
    adoption_roots: tuple[Path, ...] = (
        Path("data/filings/business_profile"),
        Path("data/filings/financial_statements/broker_risk_control"),
    )
    legacy_inventory: LegacyArchiveRegistryConfig = field(
        default_factory=LegacyArchiveRegistryConfig
    )
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
    backup: BackupConfig = field(default_factory=BackupConfig)
    capacity_override: CapacityOverrideConfig = field(default_factory=CapacityOverrideConfig)
    provisional_result: ProvisionalResultConfig = field(default_factory=ProvisionalResultConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    acquisition: AcquisitionConfig = field(default_factory=AcquisitionConfig)
    jobs: JobConfig = field(default_factory=JobConfig)
    rollout: RolloutGateConfig = field(default_factory=RolloutGateConfig)
    trusted_identity_enabled: bool = False
    trusted_principals: tuple[TrustedPrincipalConfig, ...] = ()
    acquire_permission: str = "annual_report_assets:acquire"
    content_permission: str = "annual_report_assets:read_content"
    operator_permission: str = "annual_report_assets:operator"
    business_profile_process_permission: str = "business_profile:process"
    broker_risk_control_process_permission: str = "broker_risk_control:process"

    def __post_init__(self) -> None:
        root = self.project_root.resolve(strict=False)
        capacity_artifact = _resolve_beneath(
            root,
            self.capacity_artifact_path,
            Path("config/runtime_evidence"),
            "capacity_artifact_path",
        )
        filings = _resolve_beneath(root, self.filings_root, Path("."), "filings_root")
        archive = _resolve_beneath(
            root, self.archive_root, self.filings_root, "archive_root"
        )
        temp = _resolve_beneath(root, self.temp_root, self.archive_root, "temp_root")
        quarantine = _resolve_beneath(
            root, self.quarantine_root, self.archive_root, "quarantine_root"
        )
        adoption_roots = tuple(
            _resolve_beneath(root, path, self.filings_root, "adoption_roots")
            for path in self.adoption_roots
        )
        legacy_inventory = replace(
            self.legacy_inventory,
            business_profile_root=_resolve_beneath(
                root,
                self.legacy_inventory.business_profile_root,
                self.filings_root,
                "legacy_inventory.roots.business_profile.base_root",
            ),
            broker_risk_control_root=_resolve_beneath(
                root,
                self.legacy_inventory.broker_risk_control_root,
                self.filings_root,
                "legacy_inventory.roots.broker_risk_control.base_root",
            ),
        )
        registered_roots = tuple(path for _, path in legacy_inventory.roots)
        if adoption_roots != registered_roots:
            raise ValueError(
                "paths.adoption_roots must match the versioned legacy root registry"
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
            "business_profile_process_permission",
            "broker_risk_control_process_permission",
        ):
            _non_empty_text(getattr(self, name), f"permissions.{name}")
        object.__setattr__(self, "project_root", root)
        object.__setattr__(self, "capacity_artifact_path", capacity_artifact)
        object.__setattr__(self, "filings_root", filings)
        object.__setattr__(self, "archive_root", archive)
        object.__setattr__(self, "temp_root", temp)
        object.__setattr__(self, "quarantine_root", quarantine)
        object.__setattr__(self, "adoption_roots", adoption_roots)
        object.__setattr__(self, "legacy_inventory", legacy_inventory)
        _positive_int(
            self.capacity_artifact_max_age_hours,
            "capacity_artifact_max_age_hours",
        )
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
            "capacity_artifact_required": self.capacity_artifact_required,
            "capacity_artifact_path": path(self.capacity_artifact_path),
            "capacity_artifact_max_age_hours": self.capacity_artifact_max_age_hours,
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
                "adoption_roots": [path(item) for item in self.adoption_roots],
                "expected_mount_source": self.expected_filings_mount_source,
                "require_mount": self.require_filings_mount,
            },
            "legacy_inventory": self.legacy_inventory.normalized_mapping(
                project_root=self.project_root
            ),
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
            "backup": {
                "enabled": self.backup.enabled,
                "scheduled_enabled": self.backup.scheduled_enabled,
                "mount_root": path(self.backup.mount_root),
                "destination_root": path(self.backup.destination_root),
                "expected_mount_source": self.backup.expected_mount_source,
                "expected_failure_domain": self.backup.expected_failure_domain,
                "warning_utilization": self.backup.warning_utilization,
                "hard_stop_utilization": self.backup.hard_stop_utilization,
                "free_space_reserve_bytes": self.backup.free_space_reserve_bytes,
                "freshness_hours": self.backup.freshness_hours,
                "max_unprotected_bytes": self.backup.max_unprotected_bytes,
                "max_unprotected_age_seconds": self.backup.max_unprotected_age_seconds,
                "unprotected_accumulation_origin": self.backup.unprotected_accumulation_origin,
                "reset_on_verified_backup": self.backup.reset_on_verified_backup,
                "unblock_requires_verified_backup": self.backup.unblock_requires_verified_backup,
                "recovery_journal_retention_policy": self.backup.recovery_journal_retention_policy,
                "recovery_journal_integrity_policy": self.backup.recovery_journal_integrity_policy,
            },
            "capacity_override": {
                "enabled": self.capacity_override.enabled,
                "max_bytes": self.capacity_override.max_bytes,
                "max_duration_seconds": self.capacity_override.max_duration_seconds,
                "requires_operator": self.capacity_override.requires_operator,
                "audit_required": self.capacity_override.audit_required,
                "scope_mode": self.capacity_override.scope_mode,
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
            "rollout_gates": {
                name: getattr(self.rollout, name) for name in self.rollout.__dataclass_fields__
            },
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
                "business_profile_process": self.business_profile_process_permission,
                "broker_risk_control_process": self.broker_risk_control_process_permission,
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
        """Fingerprint policy while excluding pure rollout-control toggles."""

        mapping = self.normalized_mapping()
        mapping["enabled"] = False
        mapping["scheduled_enabled"] = False
        mapping["dry_run"] = True
        mapping["backup"]["enabled"] = False
        mapping["backup"]["scheduled_enabled"] = False
        # This bounds one execution batch without changing asset identity,
        # retention, storage, provider routing, or backup semantics.
        mapping["discovery"]["max_requests"] = 300
        for name in (
            "latest_backfill_enabled",
            "daily_enabled",
            "backup_enabled",
            "integrity_enabled",
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
        legacy_inventory_raw = _mapping(
            raw.get("legacy_inventory"), "legacy_inventory"
        )
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
        backup_raw = _mapping(raw.get("backup"), "backup")
        capacity_override_raw = _mapping(
            raw.get("capacity_override"), "capacity_override"
        )
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
        rollout_raw = _mapping(raw.get("rollout_gates"), "rollout_gates")
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
        adoption_roots_rel = tuple(
            _relative_path(item, "paths.adoption_roots")
            for item in paths.get(
                "adoption_roots",
                (
                    "data/filings/business_profile",
                    "data/filings/financial_statements/broker_risk_control",
                ),
            )
        )
        legacy_roots_raw = _mapping(
            legacy_inventory_raw.get("roots"), "legacy_inventory.roots"
        )
        required_legacy_consumers = {"business_profile", "broker_risk_control"}
        if (
            "roots" in legacy_inventory_raw
            and set(legacy_roots_raw) != required_legacy_consumers
        ):
            raise ValueError(
                "legacy_inventory.roots must define business_profile and "
                "broker_risk_control exactly"
            )
        business_root_raw = _mapping(
            legacy_roots_raw.get("business_profile"),
            "legacy_inventory.roots.business_profile",
        )
        broker_root_raw = _mapping(
            legacy_roots_raw.get("broker_risk_control"),
            "legacy_inventory.roots.broker_risk_control",
        )
        legacy_business_root = _relative_path(
            business_root_raw.get(
                "base_root", "data/filings/business_profile"
            ),
            "legacy_inventory.roots.business_profile.base_root",
        )
        legacy_broker_root = _relative_path(
            broker_root_raw.get(
                "base_root",
                "data/filings/financial_statements/broker_risk_control",
            ),
            "legacy_inventory.roots.broker_risk_control.base_root",
        )
        if adoption_roots_rel != (legacy_business_root, legacy_broker_root):
            raise ValueError(
                "paths.adoption_roots conflicts with legacy_inventory.roots"
            )
        exclusions_raw = _mapping(
            legacy_inventory_raw.get("exclusions"),
            "legacy_inventory.exclusions",
        )

        backup_mount = backup_raw.get("mount_root")
        backup_destination = backup_raw.get("destination_root")
        backup_mount_rel = (
            None
            if backup_mount in (None, "")
            else _relative_path(backup_mount, "backup.mount_root")
        )
        backup_destination_rel = (
            None
            if backup_destination in (None, "")
            else _relative_path(backup_destination, "backup.destination_root")
        )
        project = Path(project_root)
        if backup_mount_rel is not None and backup_destination_rel is not None:
            _resolve_beneath(
                project,
                backup_destination_rel,
                backup_mount_rel,
                "backup.destination_root",
            )

        return cls(
            enabled=_bool_value(raw.get("enabled"), "enabled", False),
            scheduled_enabled=_bool_value(
                raw.get("scheduled_enabled"), "scheduled_enabled", False
            ),
            dry_run=_bool_value(raw.get("dry_run"), "dry_run", True),
            project_root=project,
            capacity_artifact_required=_bool_value(
                raw.get("capacity_artifact_required"),
                "capacity_artifact_required",
                False,
            ),
            capacity_artifact_path=_relative_path(
                raw.get(
                    "capacity_artifact_path",
                    "config/runtime_evidence/official_announcement_asset_capacity.json",
                ),
                "capacity_artifact_path",
            ),
            capacity_artifact_max_age_hours=_positive_int(
                raw.get("capacity_artifact_max_age_hours", 24),
                "capacity_artifact_max_age_hours",
            ),
            filings_root=filings_rel,
            archive_root=archive_rel,
            temp_root=temp_rel,
            quarantine_root=quarantine_rel,
            adoption_roots=adoption_roots_rel,
            legacy_inventory=LegacyArchiveRegistryConfig(
                registry_version=str(
                    legacy_inventory_raw.get(
                        "registry_version", LEGACY_ARCHIVE_REGISTRY_VERSION
                    )
                ),
                path_template_version=str(
                    legacy_inventory_raw.get(
                        "path_template_version", LEGACY_ARCHIVE_TEMPLATE_VERSION
                    )
                ),
                exclusion_policy_version=str(
                    legacy_inventory_raw.get(
                        "exclusion_policy_version",
                        LEGACY_ARCHIVE_EXCLUSION_POLICY_VERSION,
                    )
                ),
                business_profile_root=legacy_business_root,
                broker_risk_control_root=legacy_broker_root,
                business_profile_template=str(
                    business_root_raw.get(
                        "path_template",
                        "business_profile/{fiscal_year}/{exchange}/",
                    )
                ),
                broker_risk_control_template=str(
                    broker_root_raw.get(
                        "path_template",
                        "broker_risk_control/{exchange}/{symbol}/",
                    )
                ),
                allowed_document_families=tuple(
                    exclusions_raw.get(
                        "allowed_document_families", ("annual_report",)
                    )
                ),
                business_profile_excluded_subtrees=tuple(
                    exclusions_raw.get(
                        "business_profile_subtrees", ("derived",)
                    )
                ),
                broker_excluded_document_families=tuple(
                    exclusions_raw.get(
                        "broker_document_families", ("semiannual_report",)
                    )
                ),
            ),
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
            backup=BackupConfig(
                enabled=_bool_value(
                    backup_raw.get("enabled"), "backup.enabled", False
                ),
                scheduled_enabled=_bool_value(
                    backup_raw.get("scheduled_enabled"),
                    "backup.scheduled_enabled",
                    False,
                ),
                mount_root=(
                    None
                    if backup_mount_rel is None
                    else (project / backup_mount_rel).resolve(strict=False)
                ),
                destination_root=(
                    None
                    if backup_destination_rel is None
                    else (project / backup_destination_rel).resolve(strict=False)
                ),
                expected_mount_source=(
                    str(backup_raw.get("expected_mount_source")).strip()
                    if backup_raw.get("expected_mount_source")
                    else None
                ),
                expected_failure_domain=(
                    str(backup_raw.get("expected_failure_domain")).strip()
                    if backup_raw.get("expected_failure_domain")
                    else None
                ),
                warning_utilization=float(backup_raw.get("warning_utilization", 0.80)),
                hard_stop_utilization=float(
                    backup_raw.get("hard_stop_utilization", 0.90)
                ),
                free_space_reserve_bytes=int(
                    backup_raw.get("free_space_reserve_bytes", 50 * 1024**3)
                ),
                freshness_hours=int(backup_raw.get("freshness_hours", 48)),
                max_unprotected_bytes=int(
                    _unprotected_limit(
                        backup_raw.get("max_unprotected_bytes", 10 * 1024**3),
                        "backup.max_unprotected_bytes",
                    )
                ),
                max_unprotected_age_seconds=int(
                    _unprotected_limit(
                        backup_raw.get(
                            "max_unprotected_age_seconds", 72 * 3600
                        ),
                        "backup.max_unprotected_age_seconds",
                    )
                ),
                unprotected_accumulation_origin=str(
                    backup_raw.get(
                        "unprotected_accumulation_origin", "first_unprotected_at"
                    )
                ),
                reset_on_verified_backup=_bool_value(
                    backup_raw.get("reset_on_verified_backup"),
                    "backup.reset_on_verified_backup",
                    True,
                ),
                unblock_requires_verified_backup=_bool_value(
                    backup_raw.get("unblock_requires_verified_backup"),
                    "backup.unblock_requires_verified_backup",
                    True,
                ),
                recovery_journal_retention_policy=_non_empty_text(
                    backup_raw.get(
                        "recovery_journal_retention_policy",
                        "append_only_no_automatic_gc.v1",
                    ),
                    "backup.recovery_journal_retention_policy",
                ),
                recovery_journal_integrity_policy=_non_empty_text(
                    backup_raw.get(
                        "recovery_journal_integrity_policy",
                        "sha256_chain_with_watermarks.v1",
                    ),
                    "backup.recovery_journal_integrity_policy",
                ),
            ),
            capacity_override=CapacityOverrideConfig(
                enabled=_bool_value(
                    capacity_override_raw.get("enabled"),
                    "capacity_override.enabled",
                    False,
                ),
                max_bytes=int(
                    capacity_override_raw.get("max_bytes", 2 * 1024**3)
                ),
                max_duration_seconds=int(
                    capacity_override_raw.get("max_duration_seconds", 3600)
                ),
                requires_operator=_bool_value(
                    capacity_override_raw.get("requires_operator"),
                    "capacity_override.requires_operator",
                    True,
                ),
                audit_required=_bool_value(
                    capacity_override_raw.get("audit_required"),
                    "capacity_override.audit_required",
                    True,
                ),
                scope_mode=str(
                    capacity_override_raw.get(
                        "scope_mode", "single_operation_and_target"
                    )
                ),
            ),
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
            rollout=RolloutGateConfig(**rollout_raw),
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
            business_profile_process_permission=str(
                permissions.get(
                    "business_profile_process", "business_profile:process"
                )
            ),
            broker_risk_control_process_permission=str(
                permissions.get(
                    "broker_risk_control_process", "broker_risk_control:process"
                )
            ),
        )
