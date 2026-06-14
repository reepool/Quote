from datetime import date, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from data_manager import DataGapInfo, DataManager


def _build_config_manager() -> Mock:
    data_config = {
        'data_dir': 'data',
        'download_chunk_days': 7,
        'instrument_types': ['stock', 'index'],
        'index_master_governance': {
            'enabled': True,
            'skip_stale_no_quote': True,
            'stale_no_quote_trading_days': 10,
        },
        'repair_universe_governance': {
            'enabled': True,
            'default_mode': 'historical_backfill',
            'sample_limit': 10,
            'allow_lifecycle_filter_override': True,
            'max_override_instruments': 2,
            'max_override_limit': 2,
            'skip_index_lifecycle_states': [
                'calculation_terminated',
                'inactive',
                'stale_no_quote',
            ],
            'enable_local_stale_no_quote': True,
            'stale_no_quote_trading_days': 10,
        },
    }
    config = Mock()
    config.get_nested.side_effect = lambda key, default=None: {
        'telegram_config.enabled': False,
        'data_config': data_config,
        'data_config.repair_universe_governance': data_config['repair_universe_governance'],
        'data_config.index_master_governance': data_config['index_master_governance'],
    }.get(key, default)
    return config


def _manager() -> DataManager:
    with patch('data_manager.config_manager', _build_config_manager()):
        return DataManager()


class FakeRepairDbOps:
    def __init__(self):
        self.instruments = {
            '005061.SZ': {
                'instrument_id': '005061.SZ',
                'symbol': '005061',
                'name': 'Stopped index',
                'exchange': 'SZSE',
                'type': 'index',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'listed_date': datetime(2020, 1, 1),
                'delisted_date': None,
                'source_symbol': '005061',
            },
            '399001.SZ': {
                'instrument_id': '399001.SZ',
                'symbol': '399001',
                'name': 'Active index',
                'exchange': 'SZSE',
                'type': 'index',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'listed_date': datetime(2020, 1, 1),
                'delisted_date': None,
                'source_symbol': '399001',
            },
            '000001.SZ': {
                'instrument_id': '000001.SZ',
                'symbol': '000001',
                'name': 'Ping An Bank',
                'exchange': 'SZSE',
                'type': 'stock',
                'status': 'delisted',
                'is_active': False,
                'trading_status': 0,
                'listed_date': datetime(1991, 4, 3),
                'delisted_date': datetime(2026, 6, 10),
                'source_symbol': '000001',
            },
        }
        self.latest_quotes = {
            '005061.SZ': datetime(2025, 5, 1),
            '399001.SZ': datetime(2026, 6, 13),
        }
        self.saved_quotes = []

    async def get_repair_universe_instruments(self, exchange, instrument_types=None):
        type_set = set(instrument_types or ['stock', 'index'])
        return [
            row.copy()
            for row in self.instruments.values()
            if row['exchange'] == exchange and row['type'] in type_set
        ]

    async def get_active_instruments(self, exchange, instrument_types=None, tradable_only=False):
        return await self.get_repair_universe_instruments(exchange, instrument_types)

    async def get_index_lifecycle_evidence(self, *, exchanges=None, states=None):
        return []

    async def get_latest_quote_date(self, instrument_id):
        return self.latest_quotes.get(instrument_id)

    async def get_existing_data_dates(self, instrument_id, start_date, end_date):
        return []

    async def get_instrument_info(self, instrument_id=None, symbol=None):
        if instrument_id:
            return self.instruments.get(instrument_id)
        for row in self.instruments.values():
            if row.get('symbol') == symbol:
                return row
        return None

    async def save_daily_quotes(self, rows):
        self.saved_quotes.extend(rows)
        return True


@pytest.mark.asyncio
async def test_stopped_index_is_skipped_before_gap_detection_source_calendar():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_trading_days = AsyncMock(return_value=[date(2026, 6, 10)])

    result = await manager.detect_data_gaps(
        ['SZSE'],
        date(2026, 6, 10),
        date(2026, 6, 10),
        instrument_types=['index'],
        include_diagnostics=True,
    )

    assert {gap.instrument_id for gap in result['gaps']} == {'399001.SZ'}
    diagnostics = result['repair_universe']
    assert diagnostics['skipped_instrument_count'] == 1
    assert diagnostics['reason_distribution']['index_lifecycle_stale_no_quote'] == 1
    assert diagnostics['samples'][0]['instrument_id'] == '005061.SZ'
    assert manager.source_factory.get_trading_days.await_count == 1


@pytest.mark.asyncio
async def test_stale_gap_record_is_rechecked_before_quote_source_request():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_daily_data = AsyncMock(return_value=[])

    gap = DataGapInfo(
        instrument_id='005061.SZ',
        symbol='005061',
        exchange='SZSE',
        gap_start=date(2026, 6, 10),
        gap_end=date(2026, 6, 10),
        gap_days=1,
        gap_type='missing_data',
        severity='low',
        recommendation='test',
        missing_dates=[date(2026, 6, 10)],
    )

    assert await manager._fill_single_gap(gap) is False
    manager.source_factory.get_daily_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_lifecycle_window_clips_pre_delisting_history():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()

    eligible, diagnostics = await manager.filter_repair_universe(
        [manager.db_ops.instruments['000001.SZ']],
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 14),
        mode='historical_backfill',
    )

    assert len(eligible) == 1
    assert eligible[0]['_repair_start_date'] == date(2026, 6, 1)
    assert eligible[0]['_repair_end_date'] == date(2026, 6, 10)
    assert eligible[0]['_repair_universe_clipped'] is True
    assert diagnostics['clipped_instrument_count'] == 1


@pytest.mark.asyncio
async def test_broad_lifecycle_override_is_rejected_before_source_requests():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()

    with pytest.raises(ValueError, match='requires explicit instrument_ids'):
        await manager.filter_repair_universe(
            list(manager.db_ops.instruments.values()),
            start_date=date(2026, 6, 1),
            end_date=date(2026, 6, 14),
            mode='override',
        )


@pytest.mark.asyncio
async def test_lifecycle_eligible_gap_still_uses_quote_source():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_daily_data = AsyncMock(return_value=[
        {
            'instrument_id': '399001.SZ',
            'time': datetime(2026, 6, 10),
            'open': 1,
            'high': 1,
            'low': 1,
            'close': 1,
            'volume': 1,
        }
    ])

    gap = DataGapInfo(
        instrument_id='399001.SZ',
        symbol='399001',
        exchange='SZSE',
        gap_start=date(2026, 6, 10),
        gap_end=date(2026, 6, 10),
        gap_days=1,
        gap_type='missing_data',
        severity='low',
        recommendation='test',
        missing_dates=[date(2026, 6, 10)],
    )

    assert await manager._fill_single_gap(gap) is True
    manager.source_factory.get_daily_data.assert_awaited_once()
    assert manager.db_ops.saved_quotes[0]['instrument_id'] == '399001.SZ'
