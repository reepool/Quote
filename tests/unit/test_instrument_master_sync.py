from datetime import date
from unittest.mock import AsyncMock, Mock, mock_open, patch

import pytest

from data_manager import DataManager
from database.operations import DatabaseOperations


def _build_config_manager() -> Mock:
    config = Mock()
    config.get_research_config.return_value = {}
    config.get_nested.side_effect = lambda key, default=None: {
        'telegram_config.enabled': False,
        'data_config': {
            'data_dir': 'data',
            'download_chunk_days': 7,
            'instrument_types': ['stock', 'index'],
            'instrument_master_sync': {
                'enabled': True,
                'run_before_daily_update': True,
                'skip_for_backfill': True,
                'continue_on_failure': True,
                'timeout_sec': 30,
                'freshness_threshold_hours': 48,
                'pytdx_validation_enabled': False,
                'exchanges': ['SSE', 'SZSE', 'BSE'],
            },
        },
    }.get(key, default)
    return config


def _manager() -> DataManager:
    with patch('data_manager.config_manager', _build_config_manager()):
        return DataManager()


def test_akshare_fallback_cannot_reactivate_existing_delisted_without_delist_date():
    assert DatabaseOperations._should_preserve_protected_inactive_status('delisted', 'akshare', None)
    assert DatabaseOperations._should_preserve_protected_inactive_status('auto_deactivated_zombie', 'akshare', None)
    assert not DatabaseOperations._should_preserve_protected_inactive_status('delisted', 'baostock', None)
    assert not DatabaseOperations._should_preserve_protected_inactive_status('active', 'akshare', None)


@pytest.mark.asyncio
async def test_sync_instrument_master_reports_added_and_deactivated_rows():
    manager = _manager()
    before_rows = [
        {'instrument_id': '600355.SH', 'status': 'active', 'is_active': 1, 'source': 'baostock', 'updated_at': '2026-05-20 20:00:00'},
        {'instrument_id': '600000.SH', 'status': 'active', 'is_active': 1, 'source': 'baostock', 'updated_at': '2026-05-20 20:00:00'},
    ]
    after_rows = [
        {'instrument_id': '600355.SH', 'status': 'delisted', 'is_active': 0, 'source': 'baostock', 'updated_at': '2026-05-21 20:00:00'},
        {'instrument_id': '600000.SH', 'status': 'active', 'is_active': 1, 'source': 'baostock', 'updated_at': '2026-05-21 20:00:00'},
        {'instrument_id': '688001.SH', 'status': 'active', 'is_active': 1, 'source': 'baostock', 'updated_at': '2026-05-21 20:00:00'},
    ]
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[before_rows, after_rows])
    manager.source_factory = Mock()
    manager.source_factory.get_instrument_list = AsyncMock(return_value=[
        {'instrument_id': '600355.SH', 'source': 'baostock', 'delisted_date': '2026-04-15', 'status': 'delisted', 'is_active': False},
        {'instrument_id': '600000.SH', 'source': 'baostock', 'status': 'active', 'is_active': True},
        {'instrument_id': '688001.SH', 'source': 'baostock', 'status': 'active', 'is_active': True},
    ])

    result = await manager.sync_instrument_master(
        ['SSE'],
        include_pytdx_validation=False,
        freshness_threshold_hours=None,
    )

    assert result['status'] == 'success'
    assert result['summary']['added_instruments'] == 1
    assert result['summary']['deactivated_instruments'] == 1
    assert result['exchanges']['SSE']['added_samples'] == ['688001.SH']
    assert result['exchanges']['SSE']['deactivated_samples'] == ['600355.SH']
    manager.source_factory.get_instrument_list.assert_awaited_once_with(
        'SSE',
        force_refresh=True,
        instrument_types=['stock'],
    )


@pytest.mark.asyncio
async def test_pytdx_validator_reports_discrepancies_without_master_write():
    manager = _manager()
    pytdx = Mock()
    pytdx.get_instrument_list = AsyncMock(return_value=[
        {'instrument_id': '600000.SH'},
        {'instrument_id': '600111.SH'},
    ])
    manager.source_factory = Mock()
    manager.source_factory._get_source_instance.return_value = pytdx
    manager.source_factory.get_instrument_list = AsyncMock()

    result = await manager._validate_instrument_master_with_pytdx(
        'SSE',
        {'600000.SH', '600355.SH'},
    )

    assert result['status'] == 'warning'
    assert result['missing_in_db_samples'] == ['600111.SH']
    assert result['missing_in_pytdx_samples'] == ['600355.SH']
    manager.source_factory.get_instrument_list.assert_not_called()


@pytest.mark.asyncio
async def test_daily_update_skips_master_sync_for_historical_backfill():
    manager = _manager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[])
    manager.source_factory = Mock()
    manager.sync_instrument_master = AsyncMock()

    with patch('builtins.open', mock_open()), patch('os.makedirs'):
        result = await manager.update_daily_data(
            exchanges=['SSE'],
            target_date=date(2026, 5, 1),
            progress_log_every=0,
            progress_log_interval_sec=0,
            instrument_types=['stock'],
        )

    assert result['instrument_master_sync']['status'] == 'skipped'
    assert result['instrument_master_sync']['reason'] == 'historical_backfill_current_master_sync_skipped'
    manager.sync_instrument_master.assert_not_awaited()
    manager.db_ops.get_active_instruments.assert_awaited_once_with('SSE', instrument_types=['stock'])


@pytest.mark.asyncio
async def test_daily_update_runs_master_sync_before_active_instrument_read():
    manager = _manager()
    events = []

    async def sync_master(*args, **kwargs):
        events.append('sync')
        return {'status': 'success', 'summary': {}, 'exchanges': {}, 'warnings': [], 'errors': []}

    async def get_active(*args, **kwargs):
        events.append('get_active')
        return []

    manager.sync_instrument_master = AsyncMock(side_effect=sync_master)
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(side_effect=get_active)
    manager.source_factory = Mock()

    with patch('builtins.open', mock_open()), patch('os.makedirs'):
        result = await manager.update_daily_data(
            exchanges=['SSE'],
            target_date=date.today(),
            progress_log_every=0,
            progress_log_interval_sec=0,
            instrument_types=['stock'],
        )

    assert events[:2] == ['sync', 'get_active']
    assert result['instrument_master_sync']['status'] == 'success'
    manager.sync_instrument_master.assert_awaited_once()
