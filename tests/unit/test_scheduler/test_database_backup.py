import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

import scheduler.tasks as task_module
from scheduler.database_backup import (
    BACKUP_STATUS_FAILED,
    BACKUP_STATUS_SKIPPED,
    DatabaseBackupService,
    load_database_backup_config,
)
from scheduler.tasks import ScheduledTasks


def _create_sqlite_db(path: Path, value: str = "ok") -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE sample (value TEXT)")
        conn.execute("INSERT INTO sample VALUES (?)", (value,))


def _config_manager(raw_config: dict) -> Mock:
    config = Mock()

    def get_nested(path, default=None):
        if path == "database_backup_config":
            return raw_config
        if path in {"backup_config", "database_config"}:
            return {}
        return default

    config.get_nested.side_effect = get_nested
    return config


@pytest.mark.unit
def test_database_backup_config_resolves_defaults_and_sources(tmp_path):
    raw = {
        "backup_directory": str(tmp_path / "backups"),
        "databases": [
            {
                "name": "quotes",
                "path": str(tmp_path / "quotes.db"),
                "filename_pattern": "quotes_backup_{timestamp}.db",
            }
        ],
    }

    config = load_database_backup_config(_config_manager(raw), Mock())

    assert config.default_max_backup_files == 3
    assert config.performance.max_parallel_databases == 1
    assert config.databases[0]["name"] == "quotes"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_covers_configured_and_extra_databases(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    for name in ("quotes", "research", "fx"):
        _create_sqlite_db(data_dir / f"{name}.db", name)

    raw = {
        "backup_directory": str(backup_dir),
        "default_max_backup_files": 3,
        "default_filename_pattern": "{stem}_backup_{timestamp}.db",
        "include_globs": [str(data_dir / "*.db")],
        "skip_missing": True,
        "continue_on_database_failure": True,
        "notification_enabled": True,
        "per_database_notification": True,
        "performance": {"min_free_space_multiplier": 0.1},
        "databases": [
            {
                "name": "quotes",
                "path": str(data_dir / "quotes.db"),
                "filename_pattern": "quotes_backup_{timestamp}.db",
            },
            {
                "name": "research",
                "path": str(data_dir / "research.db"),
                "filename_pattern": "research_backup_{timestamp}.db",
            },
            {
                "name": "missing_legacy",
                "path": str(data_dir / "market_data.db"),
                "filename_pattern": "market_data_backup_{timestamp}.db",
            },
        ],
    }
    service = DatabaseBackupService.from_config_manager(_config_manager(raw), Mock())

    result = await service.run()

    assert result.success is True
    assert result.success_count == 3
    assert result.skipped_count == 1
    assert {item.name for item in result.results} == {
        "quotes",
        "research",
        "fx",
        "missing_legacy",
    }
    assert any(item.status == BACKUP_STATUS_SKIPPED for item in result.results)
    backup_names = sorted(path.name for path in backup_dir.glob("*_backup_*.db"))
    assert len(backup_names) == 3
    assert any(name.startswith("quotes_backup_") for name in backup_names)
    assert any(name.startswith("research_backup_") for name in backup_names)
    assert any(name.startswith("fx_backup_") for name in backup_names)
    for backup in backup_dir.glob("*_backup_*.db"):
        with sqlite3.connect(backup) as conn:
            assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_disabled_database_is_not_readded_by_glob(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    _create_sqlite_db(data_dir / "quotes.db", "quotes")
    _create_sqlite_db(data_dir / "research.db", "research")

    raw = {
        "backup_directory": str(backup_dir),
        "include_globs": [str(data_dir / "*.db")],
        "databases": [
            {
                "name": "quotes",
                "path": str(data_dir / "quotes.db"),
                "enabled": False,
            }
        ],
        "performance": {"min_free_space_multiplier": 0.1},
    }
    service = DatabaseBackupService.from_config_manager(_config_manager(raw), Mock())

    result = await service.run()

    assert result.success is True
    assert {item.name for item in result.results} == {"research"}
    assert not list(backup_dir.glob("quotes_backup_*.db"))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_retention_defaults_to_three(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    backup_dir.mkdir()
    _create_sqlite_db(data_dir / "quotes.db", "quotes")
    for index in range(4):
        old_file = backup_dir / f"quotes_backup_2026010{index}_000000.db"
        _create_sqlite_db(old_file, str(index))

    raw = {
        "backup_directory": str(backup_dir),
        "default_max_backup_files": 3,
        "databases": [
            {
                "name": "quotes",
                "path": str(data_dir / "quotes.db"),
                "filename_pattern": "quotes_backup_{timestamp}.db",
            }
        ],
        "performance": {"min_free_space_multiplier": 0.1},
    }
    service = DatabaseBackupService.from_config_manager(_config_manager(raw), Mock())

    result = await service.run()

    assert result.success is True
    assert len(list(backup_dir.glob("quotes_backup_*.db"))) == 3
    assert result.cleanup_deleted_count >= 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_retention_supports_per_database_override(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    backup_dir.mkdir()
    _create_sqlite_db(data_dir / "quotes.db", "quotes")
    for index in range(3):
        old_file = backup_dir / f"quotes_backup_2026010{index}_000000.db"
        _create_sqlite_db(old_file, str(index))

    raw = {
        "backup_directory": str(backup_dir),
        "default_max_backup_files": 3,
        "databases": [
            {
                "name": "quotes",
                "path": str(data_dir / "quotes.db"),
                "filename_pattern": "quotes_backup_{timestamp}.db",
                "max_backup_files": 2,
            }
        ],
        "performance": {"min_free_space_multiplier": 0.1},
    }
    service = DatabaseBackupService.from_config_manager(_config_manager(raw), Mock())

    result = await service.run()

    assert result.success is True
    assert len(list(backup_dir.glob("quotes_backup_*.db"))) == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_invalid_sqlite_reports_failure_and_continues(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    (data_dir / "bad.db").write_text("not sqlite", encoding="utf-8")
    _create_sqlite_db(data_dir / "good.db", "good")

    raw = {
        "backup_directory": str(backup_dir),
        "continue_on_database_failure": True,
        "databases": [
            {"name": "bad", "path": str(data_dir / "bad.db")},
            {"name": "good", "path": str(data_dir / "good.db")},
        ],
        "performance": {"min_free_space_multiplier": 0.1},
    }
    service = DatabaseBackupService.from_config_manager(_config_manager(raw), Mock())

    result = await service.run()

    assert result.success is False
    assert result.failure_count == 1
    assert result.success_count == 1
    assert any(item.name == "bad" and item.status == BACKUP_STATUS_FAILED for item in result.results)
    assert any(item.name == "good" for item in result.results)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_optional_failure_does_not_fail_run(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    (data_dir / "optional_bad.db").write_text("not sqlite", encoding="utf-8")
    _create_sqlite_db(data_dir / "good.db", "good")

    raw = {
        "backup_directory": str(backup_dir),
        "continue_on_database_failure": True,
        "databases": [
            {
                "name": "optional_bad",
                "path": str(data_dir / "optional_bad.db"),
                "required": False,
            },
            {"name": "good", "path": str(data_dir / "good.db")},
        ],
        "performance": {"min_free_space_multiplier": 0.1},
    }
    service = DatabaseBackupService.from_config_manager(_config_manager(raw), Mock())

    result = await service.run()

    assert result.success is True
    assert result.failure_count == 1
    optional = next(item for item in result.results if item.name == "optional_bad")
    assert optional.required is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_scheduler_database_backup_propagates_failed_result(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.telegram_enabled = False
    task.bot = None
    task._send_task_report = AsyncMock()

    failed_run = Mock()
    failed_run.success = False
    failed_run.success_count = 0
    failed_run.failure_count = 1
    failed_run.skipped_count = 0
    failed_run.cleanup_deleted_count = 0
    failed_run.preflight_error = None
    failed_run.results = []
    failed_run.to_report_data.return_value = {
        "name": "数据库备份报告",
        "success": False,
        "backup_files": [],
        "backup_file": "0 succeeded / 1 failed / 0 skipped",
    }

    service = Mock()
    service.config.notification_enabled = True
    service.config.per_database_notification = True
    service.run = AsyncMock(return_value=failed_run)
    monkeypatch.setattr(
        task_module.DatabaseBackupService,
        "from_config_manager",
        Mock(return_value=service),
    )

    result = await task.database_backup()

    assert result is False
    task._send_task_report.assert_awaited_once()
    assert task._send_task_report.await_args.args[0]["success"] is False
