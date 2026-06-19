import json
import logging

from utils.config_manager import UnifiedConfigManager
from utils.logging_manager import LogConfig, LoggingManager


def _flush_handlers(*logger_names: str) -> None:
    for logger_name in logger_names:
        logger = logging.getLogger(logger_name) if logger_name else logging.getLogger()
        for handler in logger.handlers:
            handler.flush()


def test_logging_config_accepts_domain_filenames(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "01_log.json").write_text(
        json.dumps({
            "logging_config": {
                "file_config": {
                    "enabled": True,
                    "directory": "log",
                    "filename": "legacy.log",
                    "system_filename": "system-custom.log",
                    "task_filename": "task-custom.log",
                    "access_filename": "access-custom.log",
                }
            }
        }),
        encoding="utf-8",
    )

    config = UnifiedConfigManager(str(config_dir)).get_logging_config()

    assert config.file_config.filename == "legacy.log"
    assert config.file_config.system_filename == "system-custom.log"
    assert config.file_config.task_filename == "task-custom.log"
    assert config.file_config.access_filename == "access-custom.log"


def test_logging_config_legacy_filename_does_not_collapse_domains(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "01_log.json").write_text(
        json.dumps({
            "logging_config": {
                "file_config": {
                    "enabled": True,
                    "directory": "log",
                    "filename": "legacy.log",
                }
            }
        }),
        encoding="utf-8",
    )

    config = UnifiedConfigManager(str(config_dir)).get_logging_config()

    assert config.file_config.filename == "legacy.log"
    assert config.file_config.system_filename == "sys.log"
    assert config.file_config.task_filename == "task.log"
    assert config.file_config.access_filename == "access.log"


def test_logging_manager_routes_system_task_and_access_logs(tmp_path):
    manager = LoggingManager()
    manager.configure(
        LogConfig(
            level="INFO",
            enable_console=False,
            enable_file=True,
            log_directory=str(tmp_path),
            system_log_filename="sys.log",
            task_log_filename="task.log",
            access_log_filename="access.log",
        )
    )

    logging.getLogger("API").info("system lifecycle marker")
    logging.getLogger("DataManager").info("task execution marker")
    logging.getLogger("API.Access").info("access request marker")
    _flush_handlers("", "API", "DataManager", "API.Access")

    sys_log = (tmp_path / "sys.log").read_text(encoding="utf-8")
    task_log = (tmp_path / "task.log").read_text(encoding="utf-8")
    access_log = (tmp_path / "access.log").read_text(encoding="utf-8")

    assert "system lifecycle marker" in sys_log
    assert "task execution marker" in task_log
    assert "access request marker" in access_log
    assert "task execution marker" not in sys_log
    assert "access request marker" not in sys_log
    assert "access request marker" not in task_log
    assert logging.getLogger("API.Access").propagate is False
