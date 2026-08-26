import sys
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

import data_manager as data_manager_module
from data_manager import DataManager


@pytest.mark.unit
class TestDailyFactorSyncPolicy:
    def test_factor_target_dates_expand_range(self):
        manager = DataManager()

        target_dates = manager._build_factor_target_dates([
            {
                'instrument_id': '000001.SZ',
                'symbol': '000001',
                'start_date': date(2026, 4, 9),
                'end_date': date(2026, 4, 12),
            }
        ])

        assert target_dates == {
            date(2026, 4, 9),
            date(2026, 4, 10),
            date(2026, 4, 11),
            date(2026, 4, 12),
        }

    def test_factor_target_dates_keep_internal_dates_for_long_history(self):
        manager = DataManager()

        target_dates = manager._build_factor_target_dates([{
            'instrument_id': '000001.SZ',
            'symbol': '000001',
            'start_date': date(2020, 1, 1),
            'end_date': date(2021, 1, 2),
        }])

        assert date(2020, 6, 1) in target_dates
        assert date(2020, 12, 31) in target_dates
        assert len(target_dates) == 368

    def test_dated_discovery_normalizes_cross_year_duplicates(self):
        result = DataManager._normalize_ex_dividend_discovery(
            {
                "000001": [
                    "2025-12-31",
                    date(2025, 12, 31),
                    datetime(2026, 1, 2, 9, 30),
                ],
                "600000": date(2026, 1, 2),
            },
            target_dates={date(2025, 12, 31), date(2026, 1, 2)},
        )

        assert result == {
            "000001": {date(2025, 12, 31), date(2026, 1, 2)},
            "600000": {date(2026, 1, 2)},
        }

    @pytest.mark.parametrize(
        "payload",
        [
            {"000001": []},
            {"000001": [None]},
            {"000001": ["not-a-date"]},
            {"": ["2026-01-02"]},
        ],
    )
    def test_dated_discovery_rejects_missing_or_invalid_evidence(self, payload):
        with pytest.raises(ValueError):
            DataManager._normalize_ex_dividend_discovery(
                payload,
                target_dates={date(2026, 1, 2)},
            )

    @pytest.mark.asyncio
    async def test_market_discovery_preserves_dates_and_empty_success(
        self,
        monkeypatch,
    ):
        frames = {
            "20251231": pd.DataFrame({
                "代码": [1, "000001", "600000"],
                "除权除息日": [
                    "2025-12-31",
                    "2026-01-02",
                    "2026-01-03",
                ],
            }),
            "20260630": pd.DataFrame(columns=["代码", "除权除息日"]),
        }

        def stock_fhps_em(*, date):
            return frames.get(
                date,
                pd.DataFrame(columns=["代码", "除权除息日"]),
            )

        monkeypatch.setitem(
            sys.modules,
            "akshare",
            SimpleNamespace(stock_fhps_em=stock_fhps_em),
        )

        async def run_inline(function, *args, **kwargs):
            return function(*args, **kwargs)

        monkeypatch.setattr(data_manager_module.asyncio, "to_thread", run_inline)
        manager = DataManager()

        result = await manager._query_ex_dividend_symbols({
            date(2025, 12, 31),
            date(2026, 1, 2),
        })
        empty = await manager._query_ex_dividend_symbols({date(2027, 1, 2)})

        assert result == {
            "000001": {date(2025, 12, 31), date(2026, 1, 2)},
        }
        assert empty == {}

    @pytest.mark.asyncio
    async def test_market_discovery_queries_quarterly_report_periods(self, monkeypatch):
        requested = []

        def stock_fhps_em(*, date):
            requested.append(date)
            return pd.DataFrame(columns=["代码", "除权除息日"])

        monkeypatch.setitem(
            sys.modules,
            "akshare",
            SimpleNamespace(stock_fhps_em=stock_fhps_em),
        )

        async def run_inline(function, *args, **kwargs):
            return function(*args, **kwargs)

        monkeypatch.setattr(data_manager_module.asyncio, "to_thread", run_inline)
        result = await DataManager()._query_ex_dividend_symbols({date(2026, 4, 1)})

        assert result == {}
        assert requested == ["20251231", "20260331", "20260630", "20260930", "20261231"]

    @pytest.mark.asyncio
    async def test_market_discovery_fails_closed_on_partial_periods(
        self,
        monkeypatch,
    ):
        def stock_fhps_em(*, date):
            if date == "20251231":
                raise RuntimeError("temporary source failure")
            return pd.DataFrame(columns=["代码", "除权除息日"])

        monkeypatch.setitem(
            sys.modules,
            "akshare",
            SimpleNamespace(stock_fhps_em=stock_fhps_em),
        )

        async def run_inline(function, *args, **kwargs):
            return function(*args, **kwargs)

        monkeypatch.setattr(data_manager_module.asyncio, "to_thread", run_inline)

        result = await DataManager()._query_ex_dividend_symbols({
            date(2026, 1, 2),
        })

        assert result is None

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
    async def test_factor_sync_reports_failed_when_persistence_is_incomplete(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(return_value=True)
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock(return_value=[{
            'instrument_id': '00001.HK',
            'ex_date': datetime(2026, 4, 13),
            'factor': 1.01,
            'cumulative_factor': 1.01,
            'source': 'akshare',
        }])
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
            sync_reason='maintenance',
        )

        assert result['status'] == 'partial'
        assert result['synced'] == 0
        assert result['failed'] == 1

    @pytest.mark.asyncio
    async def test_known_ex_dividend_with_empty_factor_result_is_partial(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(return_value=True)
        manager._query_ex_dividend_symbols = AsyncMock(return_value={
            "000001": {date(2026, 7, 31)},
        })
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock(return_value=[])

        result = await manager._batch_sync_adjustment_factors(
            exchange="SZSE",
            stocks=[{
                "instrument_id": "000001.SZ",
                "symbol": "000001",
                "start_date": date(2026, 7, 31),
                "end_date": date(2026, 7, 31),
            }],
            progress_log_every=0,
            sync_reason="daily",
        )

        assert result["status"] == "partial"
        assert result["failed"] == 1
        assert result["reason"] == "factor_download_failures"
        assert result["diagnostics"]["known_event_empty_count"] == 1

    @pytest.mark.asyncio
    async def test_invalid_factor_window_fails_before_discovery(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(return_value=True)
        manager._query_ex_dividend_symbols = AsyncMock(return_value={})
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock()

        result = await manager._batch_sync_adjustment_factors(
            exchange="SZSE",
            stocks=[{
                "instrument_id": "000001.SZ",
                "symbol": "000001",
                "start_date": date(2026, 8, 1),
                "end_date": date(2026, 7, 31),
            }],
            progress_log_every=0,
            sync_reason="daily",
        )

        assert result["status"] == "partial"
        assert result["reason"] == "invalid_factor_windows"
        assert result["failed"] == 1
        assert result["filtered_total"] == 0
        assert result["diagnostics"]["invalid_window_count"] == 1
        manager._query_ex_dividend_symbols.assert_not_awaited()
        manager.source_factory.get_adjustment_factors.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_union_window_excludes_seven_out_of_window_events(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(return_value=True)
        false_failure_symbols = [
            "000063", "000990", "000993", "300182",
            "300376", "300788", "300789",
        ]
        manager._query_ex_dividend_symbols = AsyncMock(return_value={
            "000001": {date(2026, 7, 29)},
            **{
                symbol: {
                    date(2026, 7, 29 + (index % 2))
                }
                for index, symbol in enumerate(false_failure_symbols)
            },
        })
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock(return_value=[{
            "instrument_id": "000001.SZ",
            "ex_date": datetime(2026, 7, 29),
            "factor": 1.01,
            "cumulative_factor": 1.01,
            "source": "akshare",
        }])
        manager._persist_adjustment_factor_batch = AsyncMock(return_value={
            "saved": 1,
        })
        stocks = [{
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "start_date": date(2026, 7, 29),
            "end_date": date(2026, 8, 3),
        }]
        stocks.extend({
            "instrument_id": f"{symbol}.SZ",
            "symbol": symbol,
            "start_date": date(2026, 7, 31),
            "end_date": date(2026, 8, 3),
        } for symbol in false_failure_symbols)

        result = await manager._batch_sync_adjustment_factors(
            exchange="SZSE",
            stocks=stocks,
            progress_log_every=0,
            sync_reason="daily",
        )

        assert result["status"] == "success"
        assert result["synced"] == 1
        assert result["failed"] == 0
        assert result["skipped"] == 0
        assert result["filtered_total"] == 1
        assert result["diagnostics"]["selected_instrument_count"] == 1
        assert result["diagnostics"]["excluded_out_of_window_count"] == 7
        assert len(
            result["diagnostics"]["samples"]["excluded_out_of_window"]
        ) == 7
        manager.source_factory.get_adjustment_factors.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_in_window_factor_without_event_coverage_is_partial(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(return_value=True)
        manager._query_ex_dividend_symbols = AsyncMock(return_value={
            "000001": {date(2026, 7, 31)},
        })
        manager.source_factory = Mock()
        manager.source_factory.get_adjustment_factors = AsyncMock(return_value=[{
            "instrument_id": "000001.SZ",
            "ex_date": datetime(2026, 7, 30),
            "factor": 1.01,
            "cumulative_factor": 1.01,
            "source": "akshare",
        }])
        manager._persist_adjustment_factor_batch = AsyncMock()

        result = await manager._batch_sync_adjustment_factors(
            exchange="SZSE",
            stocks=[{
                "instrument_id": "000001.SZ",
                "symbol": "000001",
                "start_date": date(2026, 7, 31),
                "end_date": date(2026, 7, 31),
            }],
            progress_log_every=0,
            sync_reason="daily",
        )

        assert result["status"] == "partial"
        assert result["failed"] == 1
        assert result["diagnostics"]["missing_event_coverage_count"] == 1
        manager._persist_adjustment_factor_batch.assert_not_awaited()

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
        manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
            'instrument_id': '00001.HK',
            'symbol': '00001',
            'type': 'stock',
        }])

        result = await manager.sync_all_adjustment_factors(exchanges=['HKEX'], days_back=7)

        assert result['HKEX']['skipped'] is True
        assert result['HKEX']['reason'] == 'maintenance_sync_disabled'
        manager.db_ops.get_instruments_list.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_weekly_sync_uses_existing_instruments_list_api(self):
        manager = DataManager()
        manager.config = Mock()
        manager.config.get_nested = Mock(return_value=True)
        manager.db_ops = Mock()
        manager.db_ops.get_instruments_list = AsyncMock(return_value=[{
            'instrument_id': '00001.HK',
            'symbol': '00001',
            'type': 'stock',
        }])
        manager._batch_sync_adjustment_factors = AsyncMock(
            return_value={'synced': 1, 'skipped': 0, 'failed': 0}
        )

        result = await manager.sync_all_adjustment_factors(exchanges=['HKEX'], days_back=7)

        manager.db_ops.get_instruments_list.assert_awaited_once_with(
            exchange='HKEX',
            type='stock',
            is_active=True,
        )
        manager._batch_sync_adjustment_factors.assert_awaited_once()
        assert result['HKEX']['synced'] == 1
