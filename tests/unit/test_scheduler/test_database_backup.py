from unittest.mock import AsyncMock, Mock

import pytest

from scheduler.tasks import ScheduledTasks


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_backup_covers_configured_and_extra_databases(tmp_path):
    data_dir = tmp_path / "data"
    backup_dir = tmp_path / "backups"
    data_dir.mkdir()
    for name in ("quotes", "research", "financials", "market_data"):
        (data_dir / f"{name}.db").write_bytes(f"{name}-db".encode("ascii"))

    task = ScheduledTasks.__new__(ScheduledTasks)
    task.config = Mock()
    task.config.get_nested.return_value = {
        "source_databases": [
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
                "name": "financials",
                "path": str(data_dir / "financials.db"),
                "filename_pattern": "financials_backup_{timestamp}.db",
            },
        ],
        "include_extra_data_dbs": True,
        "extra_db_glob": str(data_dir / "*.db"),
        "backup_directory": str(backup_dir),
        "retention_days": 30,
        "notification_enabled": True,
        "filename_pattern": "quotes_backup_{timestamp}.db",
        "max_backup_files": 10,
    }
    task._send_task_report = AsyncMock()

    result = await task.database_backup()

    assert result is True
    backup_names = sorted(path.name for path in backup_dir.glob("*_backup_*.db"))
    assert len(backup_names) == 4
    assert any(name.startswith("quotes_backup_") for name in backup_names)
    assert any(name.startswith("research_backup_") for name in backup_names)
    assert any(name.startswith("financials_backup_") for name in backup_names)
    assert any(name.startswith("market_data_backup_") for name in backup_names)

    report_data = task._send_task_report.await_args.args[0]
    assert report_data["success"] is True
    assert report_data["backup_file"] == "4 files"
    assert {item["name"] for item in report_data["backup_files"]} == {
        "quotes",
        "research",
        "financials",
        "market_data",
    }
