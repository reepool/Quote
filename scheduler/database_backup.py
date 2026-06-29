"""Unified SQLite database backup workflow for scheduled production backups."""

from __future__ import annotations

import asyncio
import fnmatch
import glob
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional


BACKUP_STATUS_SUCCESS = "success"
BACKUP_STATUS_FAILED = "failed"
BACKUP_STATUS_SKIPPED = "skipped"


@dataclass(frozen=True)
class DatabaseBackupPerformanceConfig:
    mode: str = "sqlite_online_backup"
    max_parallel_databases: int = 1
    chunk_pages: int = 1000
    chunk_sleep_seconds: float = 0.05
    busy_timeout_seconds: float = 30.0
    min_free_space_multiplier: float = 1.5
    require_backup_mount: bool = False
    expected_mount_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatabaseBackupSource:
    name: str
    path: Path
    filename_pattern: str
    max_backup_files: int
    required: bool = True

    @property
    def stem(self) -> str:
        return self.path.stem

    def render_filename(self, timestamp: str) -> str:
        return self.filename_pattern.format(
            name=self.name,
            stem=self.stem,
            timestamp=timestamp,
        )

    def render_glob_pattern(self) -> str:
        return self.filename_pattern.format(
            name=self.name,
            stem=self.stem,
            timestamp="*",
        )


@dataclass
class DatabaseBackupCleanupResult:
    deleted_files: List[str] = field(default_factory=list)
    retained_count: int = 0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "deleted_files": list(self.deleted_files),
            "deleted_count": len(self.deleted_files),
            "retained_count": self.retained_count,
            "error": self.error,
        }


@dataclass
class DatabaseBackupResult:
    name: str
    source: str
    status: str
    required: bool = True
    backup_file: Optional[str] = None
    backup_path: Optional[str] = None
    file_size: int = 0
    backup_size: int = 0
    duration: float = 0.0
    validation_status: Optional[str] = None
    cleanup: DatabaseBackupCleanupResult = field(default_factory=DatabaseBackupCleanupResult)
    error: Optional[str] = None
    skipped_reason: Optional[str] = None
    continued_after_failure: bool = False

    @property
    def success(self) -> bool:
        return self.status == BACKUP_STATUS_SUCCESS

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "status": self.status,
            "required": self.required,
            "backup_file": self.backup_file,
            "backup_path": self.backup_path,
            "file_size": self.file_size,
            "backup_size": self.backup_size,
            "duration": self.duration,
            "validation_status": self.validation_status,
            "cleanup": self.cleanup.to_dict(),
            "error": self.error,
            "skipped_reason": self.skipped_reason,
            "continued_after_failure": self.continued_after_failure,
        }


@dataclass
class DatabaseBackupRunResult:
    status: str
    success: bool
    backup_directory: str
    started_at: str
    finished_at: str
    duration: float
    total_source_size: int
    results: List[DatabaseBackupResult]
    preflight_error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for result in self.results if result.status == BACKUP_STATUS_SUCCESS)

    @property
    def failure_count(self) -> int:
        return sum(1 for result in self.results if result.status == BACKUP_STATUS_FAILED)

    @property
    def skipped_count(self) -> int:
        return sum(1 for result in self.results if result.status == BACKUP_STATUS_SKIPPED)

    @property
    def cleanup_deleted_count(self) -> int:
        return sum(len(result.cleanup.deleted_files) for result in self.results)

    def to_report_data(self) -> Dict[str, Any]:
        backup_files = [result.to_dict() for result in self.results]
        return {
            "name": "数据库备份报告",
            "success": self.success,
            "status": self.status,
            "backup_file": f"{self.success_count} succeeded / {self.failure_count} failed / {self.skipped_count} skipped",
            "backup_files": backup_files,
            "file_size": self.total_source_size,
            "duration": self.duration,
            "backup_directory": self.backup_directory,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "skipped_count": self.skipped_count,
            "cleanup_deleted_count": self.cleanup_deleted_count,
            "preflight_error": self.preflight_error,
            "warnings": list(self.warnings),
            "timestamp": self.finished_at,
        }


@dataclass(frozen=True)
class DatabaseBackupWorkflowConfig:
    enabled: bool
    backup_directory: Path
    default_max_backup_files: int
    default_filename_pattern: str
    timestamp_format: str
    include_globs: tuple[str, ...]
    exclude_globs: tuple[str, ...]
    skip_missing: bool
    continue_on_database_failure: bool
    notification_enabled: bool
    per_database_notification: bool
    databases: tuple[Dict[str, Any], ...]
    performance: DatabaseBackupPerformanceConfig
    warnings: tuple[str, ...] = ()


DatabaseResultCallback = Callable[[DatabaseBackupResult], Awaitable[None]]


class DatabaseBackupService:
    """Resolve, execute, validate, and clean up configured SQLite backups."""

    def __init__(self, config: DatabaseBackupWorkflowConfig, logger: Any) -> None:
        self.config = config
        self.logger = logger

    @classmethod
    def from_config_manager(cls, config_manager: Any, logger: Any) -> "DatabaseBackupService":
        return cls(load_database_backup_config(config_manager, logger), logger)

    async def run(
        self,
        *,
        on_database_result: Optional[DatabaseResultCallback] = None,
    ) -> DatabaseBackupRunResult:
        started = time.monotonic()
        started_at = datetime.now().isoformat()
        results: List[DatabaseBackupResult] = []
        warnings = list(self.config.warnings)

        try:
            if not self.config.enabled:
                finished_at = datetime.now().isoformat()
                return DatabaseBackupRunResult(
                    status="disabled",
                    success=True,
                    backup_directory=str(self.config.backup_directory),
                    started_at=started_at,
                    finished_at=finished_at,
                    duration=time.monotonic() - started,
                    total_source_size=0,
                    results=[],
                    warnings=warnings,
                )

            planned, skipped = self.resolve_sources()
            results.extend(skipped)
            total_source_size = sum(source.path.stat().st_size for source in planned)
            self.logger.info(
                "[DatabaseBackup] plan resolved databases=%s skipped=%s total_size=%s target=%s",
                [source.name for source in planned],
                [result.name for result in skipped],
                total_source_size,
                self.config.backup_directory,
            )

            self.preflight(planned, total_source_size)

            for skipped_result in skipped:
                if on_database_result:
                    await on_database_result(skipped_result)

            for index, source in enumerate(planned, start=1):
                self.logger.info(
                    "[DatabaseBackup] database start %s/%s name=%s source=%s",
                    index,
                    len(planned),
                    source.name,
                    source.path,
                )
                result = await self.backup_one(source)
                should_continue = (
                    result.success
                    or self.config.continue_on_database_failure
                    or not source.required
                )
                result.continued_after_failure = (
                    result.status == BACKUP_STATUS_FAILED and should_continue
                )
                results.append(result)
                if on_database_result:
                    await on_database_result(result)
                if result.status == BACKUP_STATUS_FAILED and not should_continue:
                    self.logger.error(
                        "[DatabaseBackup] stopping after failed required database name=%s error=%s",
                        source.name,
                        result.error,
                    )
                    break

            failed_required = any(
                result.status == BACKUP_STATUS_FAILED and result.required
                for result in results
            )
            status = "success" if not failed_required else "failed"
            success = not failed_required
            finished_at = datetime.now().isoformat()
            self.logger.info(
                "[DatabaseBackup] run summary status=%s success=%s failed=%s skipped=%s deleted=%s duration=%.1fs",
                status,
                self._count_status(results, BACKUP_STATUS_SUCCESS),
                self._count_status(results, BACKUP_STATUS_FAILED),
                self._count_status(results, BACKUP_STATUS_SKIPPED),
                sum(len(result.cleanup.deleted_files) for result in results),
                time.monotonic() - started,
            )
            return DatabaseBackupRunResult(
                status=status,
                success=success,
                backup_directory=str(self.config.backup_directory),
                started_at=started_at,
                finished_at=finished_at,
                duration=time.monotonic() - started,
                total_source_size=total_source_size,
                results=results,
                warnings=warnings,
            )
        except Exception as exc:
            self.logger.error("[DatabaseBackup] preflight/run failed: %s", exc)
            finished_at = datetime.now().isoformat()
            return DatabaseBackupRunResult(
                status="failed",
                success=False,
                backup_directory=str(self.config.backup_directory),
                started_at=started_at,
                finished_at=finished_at,
                duration=time.monotonic() - started,
                total_source_size=0,
                results=results,
                preflight_error=str(exc),
                warnings=warnings,
            )

    def resolve_sources(self) -> tuple[List[DatabaseBackupSource], List[DatabaseBackupResult]]:
        sources: List[DatabaseBackupSource] = []
        skipped: List[DatabaseBackupResult] = []
        known_paths: set[str] = set()

        for item in self.config.databases:
            source = self._source_from_item(item)
            abs_path = str(source.path.resolve())
            if abs_path in known_paths:
                continue
            known_paths.add(abs_path)
            if not bool(item.get("enabled", True)):
                self.logger.info(
                    "[DatabaseBackup] configured database disabled name=%s source=%s",
                    source.name,
                    source.path,
                )
                continue
            if not source.path.exists():
                reason = f"source database missing: {source.path}"
                if self.config.skip_missing:
                    skipped.append(
                        DatabaseBackupResult(
                            name=source.name,
                            source=str(source.path),
                            status=BACKUP_STATUS_SKIPPED,
                            required=source.required,
                            skipped_reason=reason,
                        )
                    )
                    self.logger.warning("[DatabaseBackup] %s", reason)
                    continue
                raise FileNotFoundError(reason)
            sources.append(source)

        for pattern in self.config.include_globs:
            for path_str in sorted(glob.glob(pattern)):
                path = Path(path_str)
                if not path.is_file() or self._is_excluded(path):
                    continue
                abs_path = str(path.resolve())
                if abs_path in known_paths:
                    continue
                known_paths.add(abs_path)
                if not path.exists():
                    continue
                stem = path.stem
                sources.append(
                    DatabaseBackupSource(
                        name=stem,
                        path=path,
                        filename_pattern=self.config.default_filename_pattern,
                        max_backup_files=self.config.default_max_backup_files,
                    )
                )

        return sources, skipped

    def preflight(self, sources: Iterable[DatabaseBackupSource], total_source_size: int) -> None:
        backup_directory = self.config.backup_directory
        backup_directory.mkdir(parents=True, exist_ok=True)
        self._validate_target_mount(backup_directory)
        self._validate_free_space(backup_directory, total_source_size)
        if not list(sources):
            raise RuntimeError("no database files selected for backup")

    async def backup_one(self, source: DatabaseBackupSource) -> DatabaseBackupResult:
        started = time.monotonic()
        timestamp = datetime.now().strftime(self.config.timestamp_format)
        backup_file = source.render_filename(timestamp)
        backup_path = self.config.backup_directory / backup_file
        source_size = source.path.stat().st_size
        try:
            await asyncio.to_thread(self._backup_sqlite_sync, source, backup_path)
            backup_size = backup_path.stat().st_size if backup_path.exists() else 0
            validation_status = self._validate_backup(backup_path)
            cleanup = self._cleanup_source_backups(source)
            duration = time.monotonic() - started
            self.logger.info(
                "[DatabaseBackup] database done name=%s backup=%s size=%s duration=%.1fs validation=%s",
                source.name,
                backup_path,
                backup_size,
                duration,
                validation_status,
            )
            return DatabaseBackupResult(
                name=source.name,
                source=str(source.path),
                status=BACKUP_STATUS_SUCCESS,
                required=source.required,
                backup_file=backup_file,
                backup_path=str(backup_path),
                file_size=source_size,
                backup_size=backup_size,
                duration=duration,
                validation_status=validation_status,
                cleanup=cleanup,
            )
        except Exception as exc:
            duration = time.monotonic() - started
            self.logger.error(
                "[DatabaseBackup] database failed name=%s source=%s error=%s",
                source.name,
                source.path,
                exc,
            )
            return DatabaseBackupResult(
                name=source.name,
                source=str(source.path),
                status=BACKUP_STATUS_FAILED,
                required=source.required,
                backup_file=backup_file,
                backup_path=str(backup_path),
                file_size=source_size,
                duration=duration,
                error=str(exc),
            )

    def _source_from_item(self, item: Dict[str, Any]) -> DatabaseBackupSource:
        path = Path(str(item.get("path") or ""))
        if not str(path):
            raise ValueError("database backup entry path is required")
        name = str(item.get("name") or path.stem)
        filename_pattern = str(
            item.get("filename_pattern") or self.config.default_filename_pattern
        )
        max_backup_files = int(
            item.get("max_backup_files") or self.config.default_max_backup_files
        )
        required = bool(item.get("required", True))
        return DatabaseBackupSource(
            name=name,
            path=path,
            filename_pattern=filename_pattern,
            max_backup_files=max_backup_files,
            required=required,
        )

    def _is_excluded(self, path: Path) -> bool:
        value = str(path)
        return any(fnmatch.fnmatch(value, pattern) for pattern in self.config.exclude_globs)

    def _validate_target_mount(self, backup_directory: Path) -> None:
        performance = self.config.performance
        if not performance.require_backup_mount and not performance.expected_mount_paths:
            return
        if performance.expected_mount_paths:
            resolved = str(backup_directory.resolve())
            if not any(resolved.startswith(str(Path(path).resolve())) for path in performance.expected_mount_paths):
                raise RuntimeError(
                    f"backup directory {backup_directory} is outside expected mount paths"
                )
        if performance.require_backup_mount:
            candidates = [backup_directory, *backup_directory.parents]
            if not any(os.path.ismount(candidate) for candidate in candidates):
                raise RuntimeError(
                    f"backup directory {backup_directory} is not under a mounted backup target"
                )

    def _validate_free_space(self, backup_directory: Path, total_source_size: int) -> None:
        usage = shutil.disk_usage(backup_directory)
        required = int(total_source_size * self.config.performance.min_free_space_multiplier)
        if usage.free < required:
            raise RuntimeError(
                f"insufficient backup target space: required={required} available={usage.free}"
            )

    def _backup_sqlite_sync(self, source: DatabaseBackupSource, backup_path: Path) -> None:
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        source_uri = f"{source.path.resolve().as_uri()}?mode=ro"
        progress_state = {"last_log": time.monotonic(), "last_remaining": None}
        with sqlite3.connect(source_uri, uri=True, timeout=self.config.performance.busy_timeout_seconds) as src:
            src.execute(
                f"PRAGMA busy_timeout={int(self.config.performance.busy_timeout_seconds * 1000)}"
            )
            with sqlite3.connect(str(backup_path), timeout=self.config.performance.busy_timeout_seconds) as dst:
                dst.execute(
                    f"PRAGMA busy_timeout={int(self.config.performance.busy_timeout_seconds * 1000)}"
                )

                def progress(_status: int, remaining: int, total: int) -> None:
                    now = time.monotonic()
                    if now - progress_state["last_log"] >= 30:
                        self.logger.info(
                            "[DatabaseBackup] progress name=%s remaining_pages=%s total_pages=%s",
                            source.name,
                            remaining,
                            total,
                        )
                        progress_state["last_log"] = now
                        progress_state["last_remaining"] = remaining

                src.backup(
                    dst,
                    pages=self.config.performance.chunk_pages,
                    progress=progress,
                    sleep=self.config.performance.chunk_sleep_seconds,
                )

    def _validate_backup(self, backup_path: Path) -> str:
        if not backup_path.exists():
            raise RuntimeError(f"backup file was not created: {backup_path}")
        if backup_path.stat().st_size <= 0:
            raise RuntimeError(f"backup file is empty: {backup_path}")
        with sqlite3.connect(str(backup_path)) as conn:
            row = conn.execute("PRAGMA quick_check").fetchone()
        status = str(row[0]) if row else ""
        if status.lower() != "ok":
            raise RuntimeError(f"backup quick_check failed: {status}")
        return "ok"

    def _cleanup_source_backups(self, source: DatabaseBackupSource) -> DatabaseBackupCleanupResult:
        cleanup = DatabaseBackupCleanupResult()
        try:
            pattern = self.config.backup_directory / source.render_glob_pattern()
            files = [Path(path) for path in glob.glob(str(pattern))]
            files.sort(key=lambda item: item.stat().st_mtime, reverse=True)
            retained = files[: source.max_backup_files]
            for path in files[source.max_backup_files :]:
                path.unlink()
                cleanup.deleted_files.append(str(path))
                self.logger.info("[DatabaseBackup] deleted old backup %s", path)
            cleanup.retained_count = len(retained)
        except Exception as exc:
            cleanup.error = str(exc)
            self.logger.warning(
                "[DatabaseBackup] cleanup failed name=%s error=%s",
                source.name,
                exc,
            )
        return cleanup

    @staticmethod
    def _count_status(results: Iterable[DatabaseBackupResult], status: str) -> int:
        return sum(1 for result in results if result.status == status)


def load_database_backup_config(
    config_manager: Any,
    logger: Any = None,
) -> DatabaseBackupWorkflowConfig:
    raw = config_manager.get_nested("database_backup_config", None)
    warnings: List[str] = []
    if not isinstance(raw, dict) or not raw:
        legacy = config_manager.get_nested("backup_config", {}) or {}
        raw = _migrate_legacy_backup_config(legacy)
        if legacy:
            warnings.append(
                "legacy backup_config is deprecated for production database backups; use database_backup_config"
            )
            if logger:
                logger.warning("[DatabaseBackup] %s", warnings[-1])
    else:
        legacy = config_manager.get_nested("backup_config", {}) or {}
        if legacy:
            warnings.append(
                "legacy backup_config is ignored because database_backup_config is present"
            )
            if logger:
                logger.warning("[DatabaseBackup] %s", warnings[-1])

    database_config = config_manager.get_nested("database_config", {}) or {}
    if isinstance(database_config, dict) and "backup_enabled" in database_config:
        warnings.append(
            "database_config.backup_enabled is deprecated for production database backups"
        )
        if logger:
            logger.warning("[DatabaseBackup] %s", warnings[-1])

    config = _parse_database_backup_config(raw or {}, warnings)
    validate_database_backup_config(config)
    return config


def _migrate_legacy_backup_config(legacy: Dict[str, Any]) -> Dict[str, Any]:
    databases = []
    for item in legacy.get("source_databases") or []:
        if isinstance(item, dict):
            databases.append(
                {
                    "name": item.get("name"),
                    "path": item.get("path"),
                    "filename_pattern": item.get("filename_pattern"),
                    "max_backup_files": legacy.get("max_backup_files", 3),
                    "enabled": True,
                }
            )
    include_globs = []
    if legacy.get("include_extra_data_dbs"):
        include_globs.append(str(legacy.get("extra_db_glob") or "data/*.db"))
    if not databases and legacy.get("source_db_path"):
        path = str(legacy.get("source_db_path"))
        databases.append(
            {
                "name": Path(path).stem,
                "path": path,
                "filename_pattern": legacy.get("filename_pattern") or "{stem}_backup_{timestamp}.db",
            }
        )
    return {
        "enabled": legacy.get("enabled", True),
        "backup_directory": legacy.get("backup_directory", "data/PVE-Bak/QuoteBak"),
        "default_max_backup_files": legacy.get("max_backup_files", 3),
        "default_filename_pattern": legacy.get("filename_pattern", "{stem}_backup_{timestamp}.db"),
        "timestamp_format": "%Y%m%d_%H%M%S",
        "include_globs": include_globs,
        "exclude_globs": ["data/*-wal", "data/*-shm"],
        "skip_missing": True,
        "continue_on_database_failure": True,
        "notification_enabled": legacy.get("notification_enabled", True),
        "per_database_notification": True,
        "databases": databases,
        "performance": {},
    }


def _parse_database_backup_config(
    raw: Dict[str, Any],
    warnings: List[str],
) -> DatabaseBackupWorkflowConfig:
    performance_raw = raw.get("performance") or {}
    performance = DatabaseBackupPerformanceConfig(
        mode=str(performance_raw.get("mode") or "sqlite_online_backup"),
        max_parallel_databases=int(performance_raw.get("max_parallel_databases") or 1),
        chunk_pages=int(performance_raw.get("chunk_pages") or 1000),
        chunk_sleep_seconds=float(performance_raw.get("chunk_sleep_seconds", 0.05)),
        busy_timeout_seconds=float(performance_raw.get("busy_timeout_seconds") or 30.0),
        min_free_space_multiplier=float(performance_raw.get("min_free_space_multiplier") or 1.5),
        require_backup_mount=bool(performance_raw.get("require_backup_mount", False)),
        expected_mount_paths=tuple(
            str(item) for item in performance_raw.get("expected_mount_paths") or ()
        ),
    )
    return DatabaseBackupWorkflowConfig(
        enabled=bool(raw.get("enabled", True)),
        backup_directory=Path(str(raw.get("backup_directory") or "data/PVE-Bak/QuoteBak")),
        default_max_backup_files=int(raw.get("default_max_backup_files") or 3),
        default_filename_pattern=str(
            raw.get("default_filename_pattern") or "{stem}_backup_{timestamp}.db"
        ),
        timestamp_format=str(raw.get("timestamp_format") or "%Y%m%d_%H%M%S"),
        include_globs=tuple(str(item) for item in raw.get("include_globs") or ()),
        exclude_globs=tuple(str(item) for item in raw.get("exclude_globs") or ("data/*-wal", "data/*-shm")),
        skip_missing=bool(raw.get("skip_missing", True)),
        continue_on_database_failure=bool(raw.get("continue_on_database_failure", True)),
        notification_enabled=bool(raw.get("notification_enabled", True)),
        per_database_notification=bool(raw.get("per_database_notification", True)),
        databases=tuple(
            dict(item) for item in raw.get("databases") or () if isinstance(item, dict)
        ),
        performance=performance,
        warnings=tuple(warnings),
    )


def validate_database_backup_config(config: DatabaseBackupWorkflowConfig) -> None:
    if config.default_max_backup_files <= 0:
        raise ValueError("default_max_backup_files must be positive")
    if config.performance.mode != "sqlite_online_backup":
        raise ValueError("only sqlite_online_backup mode is supported")
    if config.performance.max_parallel_databases <= 0:
        raise ValueError("max_parallel_databases must be positive")
    if config.performance.chunk_pages <= 0:
        raise ValueError("chunk_pages must be positive")
    if config.performance.chunk_sleep_seconds < 0:
        raise ValueError("chunk_sleep_seconds must be non-negative")
    if config.performance.busy_timeout_seconds <= 0:
        raise ValueError("busy_timeout_seconds must be positive")
    if config.performance.min_free_space_multiplier <= 0:
        raise ValueError("min_free_space_multiplier must be positive")
    _validate_template(config.default_filename_pattern)

    seen_paths: set[str] = set()
    for item in config.databases:
        path = str(item.get("path") or "")
        if not path:
            raise ValueError("database backup entry path is required")
        abs_path = str(Path(path).resolve())
        if abs_path in seen_paths:
            raise ValueError(f"duplicate database backup path: {path}")
        seen_paths.add(abs_path)
        if int(item.get("max_backup_files") or config.default_max_backup_files) <= 0:
            raise ValueError(f"max_backup_files must be positive for {path}")
        _validate_template(str(item.get("filename_pattern") or config.default_filename_pattern))


def _validate_template(template: str) -> None:
    try:
        template.format(name="quotes", stem="quotes", timestamp="20260101_000000")
    except Exception as exc:
        raise ValueError(f"invalid backup filename template {template!r}: {exc}") from exc
