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
            '005125.SZ': {
                'instrument_id': '005125.SZ',
                'symbol': '005125',
                'name': 'No local quote index',
                'exchange': 'SZSE',
                'type': 'index',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'listed_date': datetime(2017, 1, 10),
                'delisted_date': None,
                'source_symbol': '005125',
            },
            '399999.SZ': {
                'instrument_id': '399999.SZ',
                'symbol': '399999',
                'name': 'New index',
                'exchange': 'SZSE',
                'type': 'index',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'listed_date': datetime(2026, 6, 9),
                'delisted_date': None,
                'source_symbol': '399999',
            },
            '399238.SZ': {
                'instrument_id': '399238.SZ',
                'symbol': '399238',
                'name': 'Delisted index without quote on terminal date',
                'exchange': 'SZSE',
                'type': 'index',
                'status': 'delisted',
                'is_active': False,
                'trading_status': 0,
                'listed_date': datetime(2013, 3, 4),
                'delisted_date': datetime(2025, 12, 8),
                'source_symbol': '399238',
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
            '00007.HK': {
                'instrument_id': '00007.HK',
                'symbol': '00007',
                'name': 'Suspended HK stock',
                'exchange': 'HKEX',
                'type': 'stock',
                'status': 'suspended',
                'is_active': True,
                'trading_status': 0,
                'listed_date': datetime(2000, 9, 8),
                'delisted_date': None,
                'source_symbol': '00007',
            },
            '01688.HK': {
                'instrument_id': '01688.HK',
                'symbol': '01688',
                'name': 'New HK stock without listed date',
                'exchange': 'HKEX',
                'type': 'stock',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'listed_date': None,
                'delisted_date': None,
                'source_symbol': '01688',
            },
            '01191.HK': {
                'instrument_id': '01191.HK',
                'symbol': '01191',
                'name': 'HK stock without listed date or quotes',
                'exchange': 'HKEX',
                'type': 'stock',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'listed_date': None,
                'delisted_date': None,
                'source_symbol': '01191',
            },
        }
        self.latest_quotes = {
            '005061.SZ': datetime(2025, 5, 1),
            '399001.SZ': datetime(2026, 6, 13),
            '399238.SZ': datetime(2025, 12, 5),
            '00007.HK': datetime(2024, 3, 28),
            '01688.HK': datetime(2026, 6, 26),
        }
        self.first_quotes = {
            '00007.HK': datetime(2000, 9, 8),
            '01688.HK': datetime(2026, 6, 26),
        }
        self.saved_quotes = []
        self.index_lifecycle_evidence = []

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
        exchange_set = set(exchanges or [])
        state_set = set(states or [])
        return [
            row.copy()
            for row in self.index_lifecycle_evidence
            if (not exchange_set or row.get('exchange') in exchange_set)
            and (not state_set or row.get('lifecycle_state') in state_set)
        ]

    async def get_latest_quote_date(self, instrument_id):
        return self.latest_quotes.get(instrument_id)

    async def execute_read_query(self, query, params=None):
        if 'MIN(date(time)) AS first_quote_date' not in query:
            return []
        requested_ids = set((params or {}).values())
        return [
            {
                'instrument_id': instrument_id,
                'first_quote_date': first_quote.date().isoformat(),
            }
            for instrument_id, first_quote in self.first_quotes.items()
            if instrument_id in requested_ids
        ]

    async def get_existing_data_dates(self, instrument_id, start_date, end_date):
        start = start_date.date() if isinstance(start_date, datetime) else start_date
        end = end_date.date() if isinstance(end_date, datetime) else end_date
        existing_dates = []
        for row in self.saved_quotes:
            if row.get('instrument_id') != instrument_id:
                continue
            row_date = row.get('time') or row.get('date') or row.get('trade_date')
            if isinstance(row_date, datetime):
                row_date = row_date.date()
            elif isinstance(row_date, str):
                row_date = date.fromisoformat(row_date[:10])
            if row_date and start <= row_date <= end:
                existing_dates.append(row_date)
        return existing_dates

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
        instrument_ids=['005061.SZ', '399001.SZ'],
        include_diagnostics=True,
    )

    assert {gap.instrument_id for gap in result['gaps']} == {'399001.SZ'}
    diagnostics = result['repair_universe']
    assert diagnostics['skipped_instrument_count'] == 1
    assert diagnostics['reason_distribution']['index_lifecycle_stale_no_quote_fallback'] == 1
    assert diagnostics['degraded_fallback_count'] == 1
    assert diagnostics['samples'][0]['instrument_id'] == '005061.SZ'
    assert manager.source_factory.get_trading_days.await_count == 1


@pytest.mark.asyncio
async def test_old_index_without_local_quotes_is_not_lifecycle_skipped():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_trading_days = AsyncMock(return_value=[date(2026, 6, 10)])

    result = await manager.detect_data_gaps(
        ['SZSE'],
        date(2026, 6, 10),
        date(2026, 6, 10),
        instrument_types=['index'],
        instrument_ids=['005125.SZ', '399001.SZ'],
        include_diagnostics=True,
    )

    assert {gap.instrument_id for gap in result['gaps']} == {'005125.SZ', '399001.SZ'}
    diagnostics = result['repair_universe']
    assert diagnostics['skipped_instrument_count'] == 0
    assert manager.source_factory.get_trading_days.await_count == 2


@pytest.mark.asyncio
async def test_new_index_without_local_quotes_remains_lifecycle_eligible():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()

    eligible, diagnostics = await manager.filter_repair_universe(
        [manager.db_ops.instruments['399999.SZ']],
        start_date=date(2026, 6, 10),
        end_date=date(2026, 6, 10),
        mode='historical_backfill',
    )

    assert [item['instrument_id'] for item in eligible] == ['399999.SZ']
    assert diagnostics['skipped_instrument_count'] == 0


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
async def test_delisted_index_window_clips_to_last_local_quote_before_terminal_date():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()

    eligible, diagnostics = await manager.filter_repair_universe(
        [manager.db_ops.instruments['399238.SZ']],
        start_date=date(2025, 1, 1),
        end_date=date(2026, 6, 28),
        mode='historical_backfill',
    )

    assert len(eligible) == 1
    assert eligible[0]['_repair_end_date'] == date(2025, 12, 5)
    assert eligible[0]['_repair_universe_reason'] == 'index_delisted_last_quote_fallback'
    assert eligible[0]['_repair_universe_clipped'] is True
    assert diagnostics['clipped_instrument_count'] == 1
    assert diagnostics['clip_reason_distribution']['index_delisted_last_quote_fallback'] == 1
    assert diagnostics['degraded_fallback_count'] == 1


@pytest.mark.asyncio
async def test_index_lifecycle_evidence_reader_prefers_official_last_quote_boundary():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.db_ops.index_lifecycle_evidence = [
        {
            'instrument_id': '399238.SZ',
            'symbol': '399238',
            'exchange': 'SZSE',
            'lifecycle_state': 'stale_no_quote',
            'event_type': 'stale_no_quote',
            'last_quote_date': '2025-12-05',
            'confidence': 'quote_gap',
            'source': 'local_quote_freshness',
            'updated_at': '2026-06-28T20:00:00',
        },
        {
            'instrument_id': '399238.SZ',
            'symbol': '399238',
            'exchange': 'SZSE',
            'lifecycle_state': 'calculation_terminated',
            'event_type': 'calculation_terminated',
            'effective_date': '2025-12-08',
            'last_quote_date': '2025-12-04',
            'confidence': 'direct',
            'source': 'cnindex_announcement',
            'updated_at': '2026-06-27T20:00:00',
        },
    ]

    evidence = await manager._load_index_lifecycle_evidence_by_instrument(
        ['SZSE'],
        ['calculation_terminated', 'stale_no_quote'],
    )

    assert evidence['399238.SZ']['source'] == 'cnindex_announcement'
    assert evidence['399238.SZ']['_terminal_boundary'] == date(2025, 12, 4)
    assert evidence['399238.SZ']['_terminal_boundary_reason'] == 'index_lifecycle_last_quote_date'


@pytest.mark.asyncio
async def test_delisted_index_window_prefers_governed_last_quote_metadata():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.db_ops.index_lifecycle_evidence = [
        {
            'instrument_id': '399238.SZ',
            'symbol': '399238',
            'exchange': 'SZSE',
            'lifecycle_state': 'calculation_terminated',
            'event_type': 'calculation_terminated',
            'effective_date': '2025-12-08',
            'last_quote_date': '2025-12-04',
            'confidence': 'direct',
            'source': 'cnindex_announcement',
        },
    ]

    eligible, diagnostics = await manager.filter_repair_universe(
        [manager.db_ops.instruments['399238.SZ']],
        start_date=date(2025, 1, 1),
        end_date=date(2026, 6, 28),
        mode='historical_backfill',
    )

    assert len(eligible) == 1
    assert eligible[0]['_repair_end_date'] == date(2025, 12, 4)
    assert eligible[0]['_repair_universe_reason'] == 'index_lifecycle_last_quote_date'
    assert diagnostics['clip_reason_distribution']['index_lifecycle_last_quote_date'] == 1
    assert diagnostics['clip_samples'][0]['evidence_source'] == 'cnindex_announcement'
    assert diagnostics['degraded_fallback_count'] == 0


@pytest.mark.asyncio
async def test_delisted_index_gap_after_governed_last_quote_is_metadata_skipped():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.db_ops.index_lifecycle_evidence = [
        {
            'instrument_id': '399238.SZ',
            'symbol': '399238',
            'exchange': 'SZSE',
            'lifecycle_state': 'calculation_terminated',
            'event_type': 'calculation_terminated',
            'effective_date': '2025-12-08',
            'last_quote_date': '2025-12-04',
            'confidence': 'direct',
            'source': 'cnindex_announcement',
        },
    ]
    manager.source_factory = Mock()
    manager.source_factory.get_daily_data = AsyncMock(return_value=[])

    gap = DataGapInfo(
        instrument_id='399238.SZ',
        symbol='399238',
        exchange='SZSE',
        gap_start=date(2025, 12, 8),
        gap_end=date(2025, 12, 8),
        gap_days=1,
        gap_type='missing_data',
        severity='low',
        recommendation='test',
        missing_dates=[date(2025, 12, 8)],
    )

    assert await manager._fill_single_gap(gap) is False
    manager.source_factory.get_daily_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_index_effective_date_boundary_is_distinguished_from_last_quote_metadata():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.db_ops.index_lifecycle_evidence = [
        {
            'instrument_id': '399238.SZ',
            'symbol': '399238',
            'exchange': 'SZSE',
            'lifecycle_state': 'calculation_terminated',
            'event_type': 'calculation_terminated',
            'effective_date': '2025-12-08',
            'confidence': 'direct',
            'source': 'cnindex_announcement',
        },
    ]

    eligible, diagnostics = await manager.filter_repair_universe(
        [manager.db_ops.instruments['399238.SZ']],
        start_date=date(2025, 1, 1),
        end_date=date(2026, 6, 28),
        mode='historical_backfill',
    )

    assert len(eligible) == 1
    assert eligible[0]['_repair_end_date'] == date(2025, 12, 8)
    assert eligible[0]['_repair_universe_reason'] == 'index_lifecycle_effective_date'
    assert diagnostics['clip_reason_distribution']['index_lifecycle_effective_date'] == 1


@pytest.mark.asyncio
async def test_delisted_index_gap_after_last_local_quote_is_lifecycle_skipped():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_daily_data = AsyncMock(return_value=[])

    gap = DataGapInfo(
        instrument_id='399238.SZ',
        symbol='399238',
        exchange='SZSE',
        gap_start=date(2025, 12, 8),
        gap_end=date(2025, 12, 8),
        gap_days=1,
        gap_type='missing_data',
        severity='low',
        recommendation='test',
        missing_dates=[date(2025, 12, 8)],
    )

    assert await manager._fill_single_gap(gap) is False
    manager.source_factory.get_daily_data.assert_not_awaited()


@pytest.mark.asyncio
async def test_hkex_suspended_stock_after_last_quote_is_skipped_before_gap_detection():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_trading_days = AsyncMock(return_value=[date(2026, 6, 10)])

    result = await manager.detect_data_gaps(
        ['HKEX'],
        date(2025, 1, 1),
        date(2026, 6, 10),
        instrument_types=['stock'],
        instrument_ids=['00007.HK'],
        include_diagnostics=True,
    )

    assert result['gaps'] == []
    diagnostics = result['repair_universe']
    assert diagnostics['skipped_instrument_count'] == 1
    assert diagnostics['reason_distribution']['hkex_suspended_after_last_quote'] == 1
    manager.source_factory.get_trading_days.assert_not_awaited()


@pytest.mark.asyncio
async def test_hkex_suspended_stock_window_clips_to_latest_local_quote():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()

    eligible, diagnostics = await manager.filter_repair_universe(
        [manager.db_ops.instruments['00007.HK']],
        start_date=date(2024, 3, 1),
        end_date=date(2025, 1, 31),
        mode='historical_backfill',
    )

    assert [item['instrument_id'] for item in eligible] == ['00007.HK']
    assert eligible[0]['_repair_end_date'] == date(2024, 3, 28)
    assert eligible[0]['_repair_universe_reason'] == 'hkex_suspended'
    assert eligible[0]['_repair_universe_clipped'] is True
    assert diagnostics['clipped_instrument_count'] == 1


@pytest.mark.asyncio
async def test_hkex_active_stock_without_listed_date_clips_to_first_local_quote():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_trading_days = AsyncMock(return_value=[date(2026, 6, 26)])
    manager.db_ops.get_existing_data_dates = AsyncMock(return_value=[date(2026, 6, 26)])

    result = await manager.detect_data_gaps(
        ['HKEX'],
        date(2025, 1, 1),
        date(2026, 6, 28),
        instrument_types=['stock'],
        instrument_ids=['01688.HK'],
        include_diagnostics=True,
    )

    assert result['gaps'] == []
    diagnostics = result['repair_universe']
    assert diagnostics['eligible_instrument_count'] == 1
    assert diagnostics['clipped_instrument_count'] == 1
    manager.source_factory.get_trading_days.assert_awaited_once()
    args = manager.source_factory.get_trading_days.await_args.args
    assert args[1] == date(2026, 6, 26)


@pytest.mark.asyncio
async def test_hkex_active_stock_without_listed_date_or_local_quote_is_skipped():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_trading_days = AsyncMock(return_value=[date(2026, 6, 10)])

    result = await manager.detect_data_gaps(
        ['HKEX'],
        date(2025, 1, 1),
        date(2026, 6, 10),
        instrument_types=['stock'],
        instrument_ids=['01191.HK'],
        include_diagnostics=True,
    )

    assert result['gaps'] == []
    diagnostics = result['repair_universe']
    assert diagnostics['skipped_instrument_count'] == 1
    assert diagnostics['reason_distribution']['hkex_missing_listed_date_no_local_quote'] == 1
    manager.source_factory.get_trading_days.assert_not_awaited()


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


@pytest.mark.asyncio
async def test_gap_fill_rejects_data_that_does_not_cover_requested_missing_dates():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.source_factory = Mock()
    manager.source_factory.get_daily_data = AsyncMock(return_value=[
        {
            'instrument_id': '399001.SZ',
            'time': datetime(2026, 6, 9),
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

    assert await manager._fill_single_gap(gap) is False
    assert manager.db_ops.saved_quotes == []


@pytest.mark.asyncio
async def test_gap_fill_rejects_successful_save_when_requested_dates_still_missing():
    manager = _manager()
    manager.db_ops = FakeRepairDbOps()
    manager.db_ops.get_existing_data_dates = AsyncMock(return_value=[])
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

    assert await manager._fill_single_gap(gap) is False
    assert manager.db_ops.saved_quotes
