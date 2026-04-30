from unittest.mock import AsyncMock, Mock, patch

import pytest

from data_manager import DataManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _build_mock_config(tmp_path, *, research_enabled: bool = True):
    research_config = ResearchConfig(
        enabled=research_enabled,
        modules={"valuation": {"enabled": True}},
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(),
    )

    config = Mock()
    config.get_research_config.return_value = research_config

    def _get_nested(path, default=None):
        mapping = {
            "telegram_config.enabled": False,
            "data_config": {"data_dir": str(tmp_path)},
        }
        return mapping.get(path, default)

    config.get_nested.side_effect = _get_nested
    return config


@pytest.mark.asyncio
async def test_data_manager_initialize_can_skip_data_sources_and_progress(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.SessionLocal = object()
    manager.db_ops.initialize = AsyncMock()
    manager._initialize_research_storage = Mock()
    manager._load_progress = AsyncMock()

    with patch("data_sources.source_factory.get_data_source_factory", new=AsyncMock()) as factory_loader:
        await manager.initialize(
            include_data_sources=False,
            load_progress=False,
        )

    manager._initialize_research_storage.assert_called_once_with()
    manager._load_progress.assert_not_awaited()
    factory_loader.assert_not_awaited()
    assert manager.source_factory is None


@pytest.mark.asyncio
async def test_data_manager_initialize_default_path_still_loads_data_sources_and_progress(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.SessionLocal = object()
    manager.db_ops.initialize = AsyncMock()
    manager._initialize_research_storage = Mock()
    manager._load_progress = AsyncMock()

    factory = object()
    with patch(
        "data_sources.source_factory.get_data_source_factory",
        new=AsyncMock(return_value=factory),
    ) as factory_loader:
        await manager.initialize()

    manager._initialize_research_storage.assert_called_once_with()
    manager._load_progress.assert_awaited_once_with()
    factory_loader.assert_awaited_once_with(manager.db_ops)
    assert manager.source_factory is factory
