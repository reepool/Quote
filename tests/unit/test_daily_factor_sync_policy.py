from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


@pytest.mark.unit
class TestDailyFactorSyncPolicy:
    @pytest.mark.asyncio
    async def test_daily_factor_sync_respects_disabled_exchange_policy(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(
            side_effect=lambda path, default=None: (
                False if path == 'routing.factor.HKEX.daily_sync_enabled' else default
            )
        )
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock(return_value=[])
        manager.db_ops = Mock()
        manager.db_ops.save_adjustment_factors = AsyncMock(return_value=0)

        result = await manager._batch_sync_adjustment_factors(
            exchange='HKEX',
            stocks=[{
                'instrument_id': '00001.HK',
                'symbol': '00001',
                'start_date': date(2026, 4, 13),
                'end_date': date(2026, 4, 13),
            }],
            skip_filter=True,
            progress_log_every=0,
            sync_reason='daily',
        )

        assert result['synced'] == 0
        assert result['skipped'] == 1
        manager.source_factory.get_adjustment_factors.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_maintenance_factor_sync_ignores_daily_policy(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(
            side_effect=lambda path, default=None: (
                False if path == 'routing.factor.HKEX.daily_sync_enabled' else default
            )
        )
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock(return_value=[{
            'instrument_id': '00001.HK',
            'ex_date': datetime(2026, 4, 13),
            'factor': 1.01,
            'cumulative_factor': 1.01,
            'source': 'akshare',
        }])
        manager.db_ops = Mock()
        manager.db_ops.save_adjustment_factors = AsyncMock(return_value=1)

        result = await manager._batch_sync_adjustment_factors(
            exchange='HKEX',
            stocks=[{
                'instrument_id': '00001.HK',
                'symbol': '00001',
                'start_date': date(2026, 4, 13),
                'end_date': date(2026, 4, 13),
            }],
            skip_filter=True,
            progress_log_every=0,
            sync_reason='maintenance',
        )

        assert result['synced'] == 1
        manager.source_factory.get_adjustment_factors.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_weekly_sync_respects_maintenance_policy(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(
            side_effect=lambda path, default=None: (
                False if path == 'routing.factor.HKEX.maintenance_sync_enabled' else True
            )
        )
        manager.db_ops = Mock()
        manager.db_ops.get_instruments = AsyncMock(return_value=[{
            'instrument_id': '00001.HK',
            'symbol': '00001',
            'type': 'stock',
        }])

        result = await manager.sync_all_adjustment_factors(exchanges=['HKEX'], days_back=7)

        assert result['HKEX']['skipped'] is True
        assert result['HKEX']['reason'] == 'maintenance_sync_disabled'
        manager.db_ops.get_instruments.assert_not_awaited()
