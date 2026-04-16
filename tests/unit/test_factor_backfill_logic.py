from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


@pytest.mark.unit
class TestFactorBackfillLogic:
    def setup_method(self):
        self.manager = DataManager()
        self.manager.db_ops = Mock()
        self.manager.source_factory = Mock()
        self.manager.invalidate_factor_cache = Mock()

        self.instruments = [
            {
                'instrument_id': '00001.HK',
                'symbol': '00001',
                'exchange': 'HKEX',
                'type': 'stock',
                'is_active': True,
            },
            {
                'instrument_id': '00002.HK',
                'symbol': '00002',
                'exchange': 'HKEX',
                'type': 'stock',
                'is_active': True,
            },
        ]

        self.manager.db_ops.get_instruments_list = AsyncMock(return_value=self.instruments)
        self.manager.db_ops.execute_read_query = AsyncMock(
            return_value=[{'instrument_id': '00001.HK'}]
        )
        self.manager.db_ops.save_adjustment_factors = AsyncMock(return_value=3)
        self.manager.source_factory.get_adjustment_factors = AsyncMock(
            return_value=[
                {
                    'instrument_id': '00002.HK',
                    'ex_date': datetime(2026, 4, 13),
                    'factor': 1.1,
                    'cumulative_factor': 1.1,
                    'source': 'akshare',
                }
            ]
        )

    @pytest.mark.asyncio
    async def test_missing_mode_skips_existing_instruments(self):
        result = await self.manager.backfill_adjustment_factors(
            exchanges=['HKEX'],
            mode='missing',
            start_date=date(2026, 1, 1),
            end_date=date(2026, 4, 13),
            progress_log_every=0,
        )

        assert result['totals']['stocks_total'] == 2
        assert result['totals']['skipped_existing'] == 1
        assert result['totals']['synced_instruments'] == 1
        assert result['totals']['saved_records'] == 3
        self.manager.source_factory.get_adjustment_factors.assert_awaited_once_with(
            'HKEX',
            '00002.HK',
            '00002',
            datetime(2026, 1, 1, 0, 0, 0),
            datetime(2026, 4, 13, 23, 59, 59, 999999),
        )

    @pytest.mark.asyncio
    async def test_full_mode_processes_all_instruments(self):
        await self.manager.backfill_adjustment_factors(
            exchanges=['HKEX'],
            mode='full',
            start_date=date(2026, 1, 1),
            end_date=date(2026, 4, 13),
            progress_log_every=0,
        )

        assert self.manager.source_factory.get_adjustment_factors.await_count == 2
        self.manager.db_ops.execute_read_query.assert_not_awaited()
