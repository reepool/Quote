"""BaostockSource 单元测试 (当前 API 的真实用例)。

历史遗留的 15 个 fixture 式用例测的是旧版 API (cleanup/旧行为断言),
对本代码库从未通过, 已随 2026-09-10 测试基建清理移除; 如需恢复
BaostockSource 主路径的单测覆盖, 请基于当前实现重写。
"""

from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from data_sources.baostock_source import BaostockSource
from data_sources.base_source import RateLimitConfig


class TestBaoStockSource:
    """BaostockSource 当前行为用例 (直接 mock _run_bs_call, 不触碰生产会话锁)。"""

    @pytest.mark.asyncio
    async def test_get_instrument_list_marks_past_outdate_delisted(self):
        """BaoStock outDate is authoritative for inactive/delisted A-share status."""
        class FakeResult:
            error_code = '0'
            error_msg = ''
            fields = ['code', 'code_name', 'industry', 'area', 'type', 'status', 'ipoDate', 'outDate']

            def __init__(self):
                self._rows = [
                    ['sh.600355', '精伦电子', '电子', '湖北', '1', '0', '2002-06-13', '2026-04-15'],
                    ['sh.600000', '浦发银行', '银行', '上海', '1', '1', '1999-11-10', ''],
                ]
                self._idx = -1

            def next(self):
                self._idx += 1
                return self._idx < len(self._rows)

            def get_row_data(self):
                return self._rows[self._idx]

        source = BaostockSource(
            'baostock',
            RateLimitConfig(max_requests_per_minute=10000, max_requests_per_hour=10000, max_requests_per_day=10000),
        )
        source._run_bs_call = AsyncMock(return_value=FakeResult())

        instruments = await source.get_instrument_list('SSE', instrument_types=['stock'])

        by_id = {item['instrument_id'] for item in instruments}
        assert '600355.SH' in by_id and '600000.SH' in by_id
        by_id = {item['instrument_id']: item for item in instruments}
        assert by_id['600355.SH']['is_active'] is False
        assert by_id['600355.SH']['status'] == 'delisted'
        assert by_id['600355.SH']['delisted_date'] == datetime(2026, 4, 15)
        assert by_id['600000.SH']['is_active'] is True

    def test_adjustment_factor_rows_derive_single_event_ratio_after_window_anchor(self):
        rows = [
            {
                'dividOperateDate': '1999-11-10',
                'foreAdjustFactor': '1',
                'backAdjustFactor': '1',
                'adjustFactor': '1',
            },
            {
                'dividOperateDate': '2000-07-06',
                'foreAdjustFactor': '0.99354',
                'backAdjustFactor': '1.006502',
                'adjustFactor': '1.006502',
            },
            {
                'dividOperateDate': '2002-08-22',
                'foreAdjustFactor': '0.65498',
                'backAdjustFactor': '1.526763',
                'adjustFactor': '1.526763',
            },
        ]

        factors = BaostockSource._normalize_adjustment_factor_rows(
            rows,
            instrument_id='600000.SH',
            start_date=datetime(2002, 1, 1),
            end_date=datetime(2002, 12, 31),
        )

        assert len(factors) == 1
        assert factors[0]['ex_date'] == datetime(2002, 8, 22)
        assert factors[0]['factor'] == pytest.approx(1.526763 / 1.006502)
        assert factors[0]['cumulative_factor'] == pytest.approx(1.526763)

    def test_adjustment_factor_rows_skip_baselines_and_invalid_values(self):
        rows = [
            {
                'dividOperateDate': '2020-01-01',
                'foreAdjustFactor': '1',
                'backAdjustFactor': '1',
                'adjustFactor': '1',
            },
            {
                'dividOperateDate': '2020-02-01',
                'foreAdjustFactor': '',
                'backAdjustFactor': '0',
                'adjustFactor': '1.2',
            },
            {
                'dividOperateDate': '2020-03-01',
                'foreAdjustFactor': '1',
                'backAdjustFactor': 'not-a-number',
                'adjustFactor': '',
            },
        ]

        factors = BaostockSource._normalize_adjustment_factor_rows(
            rows,
            instrument_id='000001.SZ',
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2020, 12, 31),
        )

        assert len(factors) == 1
        assert factors[0]['factor'] == pytest.approx(1.2)
        assert factors[0]['cumulative_factor'] == pytest.approx(1.2)
        assert factors[0]['fore_adjust_factor'] == pytest.approx(1.0)

    @pytest.mark.asyncio
    async def test_get_adjustment_factors_queries_full_history_before_windowing(self):
        class FakeResult:
            error_code = '0'
            error_msg = ''
            fields = [
                'code', 'dividOperateDate', 'foreAdjustFactor',
                'backAdjustFactor', 'adjustFactor',
            ]

            def __init__(self):
                self._rows = [
                    ['sh.600000', '2000-07-06', '1', '1.006502', '1.006502'],
                    ['sh.600000', '2002-08-22', '1', '1.526763', '1.526763'],
                ]
                self._idx = -1

            def next(self):
                self._idx += 1
                return self._idx < len(self._rows)

            def get_row_data(self):
                return self._rows[self._idx]

        source = BaostockSource(
            'baostock',
            RateLimitConfig(
                max_requests_per_minute=10000,
                max_requests_per_hour=10000,
                max_requests_per_day=10000,
            ),
        )
        source._ensure_login = AsyncMock()
        source._run_bs_call = AsyncMock(return_value=FakeResult())

        factors = await source.get_adjustment_factors(
            '600000.SH',
            '600000',
            datetime(2002, 1, 1),
            datetime(2002, 12, 31),
        )

        assert len(factors) == 1
        assert factors[0]['factor'] == pytest.approx(1.526763 / 1.006502)
        assert source._run_bs_call.await_args.kwargs['start_date'] == '1990-01-01'
        assert source._run_bs_call.await_args.kwargs['end_date'] == '2002-12-31'
