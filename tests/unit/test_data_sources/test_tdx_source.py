"""
通达信数据源单元测试

覆盖:
  1. _parse_instrument_id: 品种 ID 解析
  2. TdxIPManager: IP 管理 (探测 mock)
  3. TdxConnectionPool: 连接池 (mock API)
  4. TdxSource._convert_bars_to_quotes: 格式转换 + vol×100
  5. TdxSource._parse_bar_datetime: 日期解析
  6. TdxFactorEngine.calculate_day_factor: 单日因子计算
  7. TdxFactorValidator.validate: 交叉验证逻辑
  8. source_factory 路由验证 (pytdx → a_stock)
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import threading
import time

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

PARSEABLE_BAR = {
    "datetime": "2026-09-09",
    "open": 10.0,
    "high": 10.5,
    "low": 9.8,
    "close": 10.2,
    "vol": 100.0,
    "amount": 102000.0,
}


def _no_probe_tcp():
    from data_sources.tdx_source import TdxIPManager

    return patch.object(
        TdxIPManager,
        "_connect_registered_probe",
        lambda self, api, ip, port, tracker: None,
    )


def _entry(ip, status, latency=1.0, port=7709, name=""):
    from data_sources.tdx_source import IPEntry
    return IPEntry(ip=ip, port=port, name=name, latency_ms=latency, status=status)


# ===========================================================
# 1. _parse_instrument_id
# ===========================================================
class TestParseInstrumentId:
    """品种 ID → (market, code) 解析"""

    def test_sz_stock(self):
        from data_sources.tdx_source import _parse_instrument_id
        market, code = _parse_instrument_id("000001.SZ")
        assert market == 0
        assert code == "000001"

    def test_sh_stock(self):
        from data_sources.tdx_source import _parse_instrument_id
        market, code = _parse_instrument_id("600000.SH")
        assert market == 1
        assert code == "600000"

    def test_bj_stock(self):
        from data_sources.tdx_source import _parse_instrument_id
        market, code = _parse_instrument_id("430047.BJ")
        assert market == 2
        assert code == "430047"

    def test_case_insensitive(self):
        from data_sources.tdx_source import _parse_instrument_id
        market, code = _parse_instrument_id("000001.sz")
        # 根据 SUFFIX_TO_MARKET 使用 .upper(), 应正常解析
        assert market == 0

    def test_invalid_suffix(self):
        from data_sources.tdx_source import _parse_instrument_id
        with pytest.raises(ValueError, match="未知交易所后缀"):
            _parse_instrument_id("AAPL.US")

    def test_no_dot(self):
        from data_sources.tdx_source import _parse_instrument_id
        with pytest.raises(ValueError, match="无法解析"):
            _parse_instrument_id("000001")


class TestXdxrReconnect:
    @staticmethod
    def _event():
        return {
            "year": 2020,
            "month": 6,
            "day": 1,
            "category": 1,
            "fenhong": 2.0,
            "songzhuangu": 0.0,
            "peigu": 0.0,
            "peigujia": 0.0,
            "suogu": 0.0,
        }

    @staticmethod
    def _source(pool):
        from data_sources.tdx_source import TdxSource

        source = TdxSource.__new__(TdxSource)
        source.name = "test_pytdx"
        source.pool = pool
        return source

    def test_empty_xdxr_on_healthy_connection_does_not_reconnect(self):
        api = Mock()
        api.get_xdxr_info.return_value = []
        api.get_security_bars.return_value = [{"close": 10.0}]
        pool = Mock()
        pool.get_connection.return_value = api

        events = self._source(pool)._sync_get_xdxr_events("600000.SH")

        assert events == []
        pool.reconnect_current.assert_not_called()

    def test_empty_xdxr_and_bar_probe_reconnects_once(self):
        stale_api = Mock()
        stale_api.get_xdxr_info.return_value = []
        stale_api.get_security_bars.return_value = []
        refreshed_api = Mock()
        refreshed_api.get_xdxr_info.return_value = [self._event()]
        pool = Mock()
        pool.get_connection.return_value = stale_api
        pool.reconnect_current.return_value = refreshed_api

        events = self._source(pool)._sync_get_xdxr_events("600000.SH")

        assert len(events) == 1
        assert events[0]["fenhong"] == 2.0
        pool.reconnect_current.assert_called_once_with(mark_failure=False)

    def test_xdxr_exception_reconnects_at_most_once(self):
        failed_api = Mock()
        failed_api.get_xdxr_info.side_effect = ConnectionError("stale")
        refreshed_api = Mock()
        refreshed_api.get_xdxr_info.return_value = []
        refreshed_api.get_security_bars.return_value = []
        pool = Mock()
        pool.get_connection.return_value = failed_api
        pool.reconnect_current.return_value = refreshed_api

        events = self._source(pool)._sync_get_xdxr_events("600000.SH")

        assert events == []
        pool.reconnect_current.assert_called_once_with()


class TestDailyDataReconnect:
    @staticmethod
    def _bar(code_date="2026-07-15"):
        return {
            "datetime": code_date,
            "open": 10.0,
            "high": 10.5,
            "low": 9.8,
            "close": 10.2,
            "vol": 100.0,
            "amount": 102000.0,
        }

    @staticmethod
    def _source(pool):
        from data_sources.tdx_source import TdxSource

        source = TdxSource.__new__(TdxSource)
        source.name = "test_pytdx"
        source.pool = pool
        source._batch_size = 800
        return source

    def test_empty_delisted_series_on_healthy_connection_uses_backup(self):
        api = Mock()

        def get_bars(_category, _market, code, _offset, _count):
            if code == "300208":
                return []
            return [self._bar()]

        api.get_security_bars.side_effect = get_bars
        pool = Mock()
        pool.get_connection.return_value = api

        source = self._source(pool)
        quotes = source._sync_get_daily_data(
            "300208.SZ",
            "300208",
            datetime(1990, 12, 19),
            datetime(2026, 7, 15),
        )

        assert quotes == []
        pool.reconnect_current.assert_not_called()
        assert source.last_fetch_diagnostic.get("connection_unhealthy") is not True

    def test_dead_connection_rotates_endpoint_and_retries_target(self):
        stale_api = Mock()
        stale_api.get_security_bars.return_value = []
        refreshed_api = Mock()

        def get_refreshed_bars(_category, _market, code, _offset, _count):
            if code == "300208":
                return [self._bar()]
            return [self._bar()]

        refreshed_api.get_security_bars.side_effect = get_refreshed_bars
        pool = Mock()
        pool.get_connection.return_value = stale_api
        pool.reconnect_current.return_value = refreshed_api

        source = self._source(pool)
        quotes = source._sync_get_daily_data(
            "300208.SZ",
            "300208",
            datetime(2026, 7, 15),
            datetime(2026, 7, 15),
        )

        assert len(quotes) == 1
        assert quotes[0]["instrument_id"] == "300208.SZ"
        pool.reconnect_current.assert_called_once_with(mark_failure=True)
        assert source.last_fetch_diagnostic.get("connection_unhealthy") is not True

    def test_dead_connection_after_rotate_marks_source_unhealthy(self):
        stale_api = Mock()
        stale_api.get_security_bars.return_value = []
        refreshed_api = Mock()
        refreshed_api.get_security_bars.return_value = []
        pool = Mock()
        pool.get_connection.return_value = stale_api
        pool.reconnect_current.return_value = refreshed_api

        source = self._source(pool)
        quotes = source._sync_get_daily_data(
            "600000.SH",
            "600000",
            datetime(2026, 9, 9),
            datetime(2026, 9, 9),
        )

        assert quotes == []
        assert source.last_fetch_diagnostic["connection_unhealthy"] is True
        pool.reconnect_current.assert_called_once_with(mark_failure=True)


# ===========================================================
# 2. TdxIPManager
# ===========================================================
class TestTdxIPManager:
    """IP 管理器测试 (mock 日线探针)"""

    def _make_manager(self, **kwargs):
        from data_sources.tdx_source import TdxIPManager
        params = dict(
            hosts=[
                {"ip": "1.1.1.1", "port": 7709, "name": "测试1"},
                {"ip": "2.2.2.2", "port": 7709, "name": "测试2"},
                {"ip": "3.3.3.3", "port": 7709, "name": "测试3"},
            ],
            blacklist_duration_hours=0.01,
            probe_timeout=0.1,
            probe_workers=2,
            refresh_deadline_sec=8.0,
        )
        params.update(kwargs)
        return TdxIPManager(**params)

    def _patch_probes(self, status_by_ip):
        from data_sources.tdx_source import TdxIPManager

        def fake(self, host, deadline, tracker):
            ip = host["ip"]
            spec = status_by_ip[ip]
            if isinstance(spec, tuple):
                status, latency = spec
            else:
                status, latency = spec, float(100 * (ord(ip[0]) or 1))
            return _entry(ip, status, latency=latency, name=host.get("name", ""))

        return patch.object(TdxIPManager, "_probe_host", fake)

    def test_refresh_sorts_by_latency(self):
        """探测后按延迟排序"""
        mgr = self._make_manager()
        with self._patch_probes({
            "1.1.1.1": ("active", 500.0),
            "2.2.2.2": ("active", 100.0),
            "3.3.3.3": ("active", 300.0),
        }):
            mgr.refresh()
        assert len(mgr.ranked_ips) == 3
        assert mgr.ranked_ips[0].ip == "2.2.2.2"
        assert mgr.ranked_ips[1].ip == "3.3.3.3"
        assert mgr.ranked_ips[2].ip == "1.1.1.1"

    def test_faster_empty_bar_loses_to_slower_selectable(self):
        mgr = self._make_manager()
        with self._patch_probes({
            "1.1.1.1": ("empty_bars", 10.0),
            "2.2.2.2": ("active", 200.0),
            "3.3.3.3": ("unfinished", 9999.0),
        }):
            mgr.refresh()
        ip, port = mgr.get_ip()
        assert ip == "2.2.2.2"
        assert port == 7709
        assert any(e.status == "empty_bars" for e in mgr.ranked_ips)
        assert any(e.status == "unfinished" for e in mgr.ranked_ips)

    def test_refresh_log_separates_unfinished_from_unreachable(self):
        mgr = self._make_manager()
        with patch("data_sources.tdx_source.tdx_logger") as logger:
            with self._patch_probes({
                "1.1.1.1": ("active", 100.0),
                "2.2.2.2": ("empty_bars", 10.0),
                "3.3.3.3": ("unfinished", 9999.0),
            }):
                mgr.refresh()
        messages = " ".join(
            str(call.args[0]) for call in logger.info.call_args_list if call.args
        )
        assert "empty_bars=1" in messages
        assert "unfinished=1" in messages
        assert "unreachable=0" in messages
        assert "chosen=1.1.1.1:7709" in messages
        assert "duration_sec=" in messages

    def test_blacklist_and_failover(self):
        """IP 拉黑后自动切换到下一个"""
        mgr = self._make_manager()
        with self._patch_probes({
            "1.1.1.1": ("active", 100.0),
            "2.2.2.2": ("active", 200.0),
            "3.3.3.3": ("active", 300.0),
        }):
            mgr.refresh()

        ip, port = mgr.get_ip()
        assert ip == "1.1.1.1"

        new_ip, new_port = mgr.report_failure("1.1.1.1", 7709)
        assert new_ip == "2.2.2.2"

    def test_all_blacklisted_reset_only_selectable(self):
        """全部可选节点被拉黑后只解封可选节点，不复活空日线站"""
        mgr = self._make_manager()
        with self._patch_probes({
            "1.1.1.1": ("active", 100.0),
            "2.2.2.2": ("active", 200.0),
            "3.3.3.3": ("empty_bars", 1.0),
        }):
            mgr.refresh()

        mgr.report_failure("1.1.1.1", 7709)
        ip, port = mgr.report_failure("2.2.2.2", 7709)
        assert ip == "1.1.1.1"
        assert mgr.get_ip()[0] != "3.3.3.3"

    def test_zero_selectable_raises_connection_error(self):
        mgr = self._make_manager()
        with self._patch_probes({
            "1.1.1.1": ("empty_bars", 10.0),
            "2.2.2.2": ("unreachable", 9999.0),
            "3.3.3.3": ("unfinished", 9999.0),
        }):
            mgr.refresh()
        with pytest.raises(ConnectionError, match="无法连接任何服务器"):
            mgr.get_ip()

    def test_needs_refresh_initially(self):
        mgr = self._make_manager()
        assert mgr.needs_refresh() is True


# ===========================================================
# 3. TdxSource._parse_bar_datetime
# ===========================================================
class TestParseBarDatetime:
    """pytdx bar 日期解析"""

    def test_datetime_str_format(self):
        from data_sources.tdx_source import TdxSource
        bar = {"datetime": "2025-06-15 00:00:00"}
        result = TdxSource._parse_bar_datetime(bar)
        assert result == datetime(2025, 6, 15)

    def test_year_month_day_fallback(self):
        from data_sources.tdx_source import TdxSource
        bar = {"year": 2024, "month": 3, "day": 20}
        result = TdxSource._parse_bar_datetime(bar)
        assert result == datetime(2024, 3, 20)

    def test_empty_bar(self):
        from data_sources.tdx_source import TdxSource
        result = TdxSource._parse_bar_datetime({})
        assert result is None


# ===========================================================
# 4. TdxSource._convert_bars_to_quotes (vol×100 转换)
# ===========================================================
class TestConvertBarsToQuotes:
    """测试格式转换和 vol→volume 乘 100"""

    def _make_source(self):
        from data_sources.tdx_source import TdxSource
        source = TdxSource.__new__(TdxSource)
        source.name = "test_pytdx"
        return source

    @patch('utils.date_utils.get_shanghai_time')
    def test_vol_times_100(self, mock_time):
        """vol(手) × 100 → volume(股)"""
        mock_time.return_value = datetime(2025, 1, 1)
        source = self._make_source()
        bars = [
            {
                "datetime": "2025-06-15 00:00:00",
                "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
                "vol": 12345.0,  # 手
                "amount": 1234567.0,
            }
        ]
        start = datetime(2025, 6, 1)
        end = datetime(2025, 6, 30)
        quotes = source._convert_bars_to_quotes(bars, "000001.SZ", start, end)

        assert len(quotes) == 1
        q = quotes[0]
        assert q["volume"] == 1234500  # 12345 × 100
        assert q["source"] == "pytdx"
        assert q["factor"] == 1.0
        assert q["adjustment_type"] == "none"
        assert q["tradestatus"] == 1

    @patch('utils.date_utils.get_shanghai_time')
    def test_suspension_detection(self, mock_time):
        """成交量为 0 → tradestatus=0 (停牌)"""
        mock_time.return_value = datetime(2025, 1, 1)
        source = self._make_source()
        bars = [
            {
                "datetime": "2025-06-15 00:00:00",
                "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                "vol": 0.0,
                "amount": 0.0,
            }
        ]
        quotes = source._convert_bars_to_quotes(
            bars, "000001.SZ", datetime(2025, 6, 1), datetime(2025, 6, 30)
        )
        assert len(quotes) == 1
        assert quotes[0]["tradestatus"] == 0
        assert quotes[0]["volume"] == 0

    @patch('utils.date_utils.get_shanghai_time')
    def test_date_filtering(self, mock_time):
        """超出日期范围的 bar 被过滤"""
        mock_time.return_value = datetime(2025, 1, 1)
        source = self._make_source()
        bars = [
            {"datetime": "2025-06-10 00:00:00", "open": 1, "high": 1, "low": 1, "close": 1, "vol": 100, "amount": 100},
            {"datetime": "2025-06-15 00:00:00", "open": 2, "high": 2, "low": 2, "close": 2, "vol": 200, "amount": 200},
            {"datetime": "2025-06-20 00:00:00", "open": 3, "high": 3, "low": 3, "close": 3, "vol": 300, "amount": 300},
        ]
        quotes = source._convert_bars_to_quotes(
            bars, "000001.SZ",
            datetime(2025, 6, 14), datetime(2025, 6, 16)
        )
        assert len(quotes) == 1
        assert quotes[0]["close"] == 2.0


# ===========================================================
# 5. TdxFactorEngine.calculate_day_factor
# ===========================================================
class TestFactorEngine:
    """自研因子计算引擎测试"""

    def _engine(self):
        from data_sources.tdx_factor_engine import TdxFactorEngine
        return TdxFactorEngine()

    def test_pure_dividend(self):
        """纯分红: 每 10 股派 5 元, 前收盘 20"""
        engine = self._engine()
        # 除权价 = (20 - 0.5) / (1 + 0) = 19.5
        # 因子 = 20 / 19.5 ≈ 1.025641
        factor = engine.calculate_day_factor(
            pre_close=20.0,
            fenhong=5.0,  # 每 10 股派 5 元
            songzhuangu=0.0,
            peigu=0.0,
            peigujia=0.0,
        )
        expected = 20.0 / 19.5
        assert abs(factor - expected) < 0.0001

    def test_pure_bonus(self):
        """纯送股: 每 10 股送 5 股, 前收盘 20"""
        engine = self._engine()
        # 除权价 = 20 / (1 + 0.5) = 13.333...
        # 因子 = 20 / 13.333 = 1.5
        factor = engine.calculate_day_factor(
            pre_close=20.0,
            fenhong=0.0,
            songzhuangu=5.0,  # 每 10 股送 5 股
            peigu=0.0,
            peigujia=0.0,
        )
        assert abs(factor - 1.5) < 0.0001

    def test_mixed_event(self):
        """混合事件: 每 10 股派 3 元送 2 股配 1 股, 配股价 8 元, 前收盘 30"""
        engine = self._engine()
        # 每股: dividend=0.3, bonus=0.2, rights=0.1
        # 除权价 = (30 - 0.3 + 8 × 0.1) / (1 + 0.2 + 0.1)
        #        = (30 - 0.3 + 0.8) / 1.3
        #        = 30.5 / 1.3 ≈ 23.461538
        # 因子 = 30 / 23.461538 ≈ 1.278689
        factor = engine.calculate_day_factor(
            pre_close=30.0,
            fenhong=3.0,
            songzhuangu=2.0,
            peigu=1.0,
            peigujia=8.0,
        )
        expected = 30.0 / (30.5 / 1.3)
        assert abs(factor - expected) < 0.0001

    def test_zero_pre_close(self):
        """前收盘价为 0 → 返回 1.0"""
        engine = self._engine()
        factor = engine.calculate_day_factor(0.0, 5.0, 5.0, 0.0, 0.0)
        assert factor == 1.0

    def test_no_event(self):
        """无任何事件 → 因子为 1.0"""
        engine = self._engine()
        factor = engine.calculate_day_factor(20.0, 0.0, 0.0, 0.0, 0.0)
        assert factor == 1.0


# ===========================================================
# 6. TdxFactorValidator.validate
# ===========================================================
class TestFactorValidator:
    """交叉验证器测试"""

    def _validator(self):
        from data_sources.tdx_factor_validator import TdxFactorValidator
        return TdxFactorValidator(tolerance=0.001)

    def test_all_pass(self):
        """完全一致 → ALL_PASS"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.025641, "cumulative_factor": 1.025641},
        ]
        ref = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.025641, "cumulative_factor": 1.025641},
        ]
        report = val.validate("000001.SZ", tdx, ref)
        assert report.result == FactorValidationResult.ALL_PASS
        assert report.overlap_count == 1
        assert report.conflict_count == 0

    def test_conflict(self):
        """因子不一致 → CONFLICT"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5},
        ]
        ref = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.2, "cumulative_factor": 1.2},
        ]
        report = val.validate("000001.SZ", tdx, ref)
        assert report.result == FactorValidationResult.CONFLICT
        assert report.conflict_count == 1

    def test_no_overlap(self):
        """日期不重叠 → NO_OVERLAP"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5},
        ]
        ref = [
            {"ex_date": datetime(2024, 7, 10), "factor": 1.2, "cumulative_factor": 1.2},
        ]
        report = val.validate("000001.SZ", tdx, ref)
        assert report.result == FactorValidationResult.NO_OVERLAP

    def test_both_empty(self):
        """两侧都为空 → ALL_PASS"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        report = val.validate("000001.SZ", [], [])
        assert report.result == FactorValidationResult.ALL_PASS

    def test_partial(self):
        """部分重叠通过但有 tdx_only → PARTIAL"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.025641, "cumulative_factor": 1.025641},
            {"ex_date": datetime(2024, 9, 15), "factor": 1.1, "cumulative_factor": 1.128205},
        ]
        ref = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.025641, "cumulative_factor": 1.025641},
        ]
        report = val.validate("000001.SZ", tdx, ref)
        assert report.result == FactorValidationResult.PARTIAL
        assert report.tdx_only_count == 1
        assert report.pass_count == 1

    def test_within_tolerance(self):
        """微小差异在容差内 → ALL_PASS"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.025641, "cumulative_factor": 1.025641},
        ]
        ref = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.025640, "cumulative_factor": 1.025640},
        ]
        report = val.validate("000001.SZ", tdx, ref)
        assert report.result == FactorValidationResult.ALL_PASS

    def test_ref_cumulative_factor_is_normalized_to_day_factor(self):
        """权威源为累计因子时，先推导单日因子再比较。"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2026, 4, 8), "factor": 1.036244, "cumulative_factor": 1.036244},
        ]
        ref = [
            {"ex_date": datetime(2025, 9, 26), "factor": 1.283950, "cumulative_factor": 1.283950},
            {"ex_date": datetime(2026, 4, 8), "factor": 1.330485, "cumulative_factor": 1.330485},
        ]
        report = val.validate("603682.SH", tdx, ref)
        assert report.result == FactorValidationResult.ALL_PASS
        assert report.conflict_count == 0
        assert report.ref_only_count == 0
        assert report.details[0].ref_factor == pytest.approx(1.036244, rel=1e-6)

    def test_report_to_dict(self):
        """验证报告序列化"""
        val = self._validator()
        tdx = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5},
        ]
        ref = [
            {"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5},
        ]
        report = val.validate("000001.SZ", tdx, ref)
        d = report.to_dict()
        assert d["result"] == "all_pass"
        assert d["instrument_id"] == "000001.SZ"
        assert len(d["details"]) == 1

    def test_batch_validate(self):
        """批量验证"""
        val = self._validator()
        tdx_map = {
            "000001.SZ": [{"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5}],
            "000002.SZ": [{"ex_date": datetime(2024, 7, 1), "factor": 1.2, "cumulative_factor": 1.2}],
        }
        ref_map = {
            "000001.SZ": [{"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5}],
        }
        results = val.validate_batch(tdx_map, ref_map)
        assert len(results) == 2
        assert "000001.SZ" in results
        assert "000002.SZ" in results

    def test_summary_text(self):
        """摘要文本生成"""
        from data_sources.tdx_factor_validator import FactorValidationResult
        val = self._validator()
        tdx = [{"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5}]
        ref = [{"ex_date": datetime(2024, 6, 10), "factor": 1.5, "cumulative_factor": 1.5}]
        report = val.validate("000001.SZ", tdx, ref)
        text = report.summary_text()
        assert "000001.SZ" in text
        assert "✅" in text


# ===========================================================
# 7. 配置路由验证
# ===========================================================
class TestConfigRoutes:
    """验证配置文件路由正确性"""

    def test_config_json_valid(self):
        """03_data.json 可正确加载"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        assert "data_sources_config" in d
        assert "routing" in d

    def test_pytdx_is_primary_for_a_share_stock_daily_routes(self):
        """pytdx 是 A 股 stock 日线路由首选"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        daily = d["routing"]["daily"]
        assert daily["SSE"]["stock"][0] == "pytdx"
        assert daily["SZSE"]["stock"][0] == "pytdx"
        assert daily["BSE"]["stock"][0] == "pytdx"

    def test_baostock_is_backup_for_a_stock_master_and_calendar(self):
        """BaoStock 仅作为 A 股主数据和交易日历备源。"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        routing = d["routing"]
        assert routing["instrument_list"]["a_stock"][:2] == [
            "exchange_official",
            "baostock",
        ]
        assert routing["calendar"]["a_stock"] == ["akshare", "baostock"]

    def test_factor_routes_complete(self):
        """因子路由覆盖 SSE/SZSE/BSE"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        fs = d["routing"]["factor"]
        assert fs["SSE"]["primary"] == "akshare"
        assert fs["SSE"]["fallback"] == "baostock"
        assert fs["SSE"]["validator"] == "tdx_xdxr"
        assert fs["SZSE"]["primary"] == "akshare"
        assert fs["SZSE"]["fallback"] == "baostock"
        assert fs["BSE"]["primary"] == "akshare"
        assert fs["BSE"]["fallback"] is None

    def test_pytdx_not_in_instrument_or_calendar(self):
        """pytdx 不应出现在品种列表/日历路由中"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        routing = d["routing"]
        assert "pytdx" not in routing["instrument_list"]["a_stock"]
        assert "pytdx" not in routing["calendar"]["a_stock"]


class TestDailyBarProbeClassification:
    def test_shenzhen_only_success_is_selectable_and_healthy(self):
        from data_sources.tdx_source import TdxIPManager, TdxSource

        def get_bars(_category, _market, code, _offset, _count):
            if code == "600000":
                return []
            return [PARSEABLE_BAR]

        api = Mock()
        api.connect.return_value = api
        api.get_security_bars.side_effect = get_bars
        api.client = None

        with patch("data_sources.tdx_source.TdxHq_API", return_value=api), _no_probe_tcp():
            mgr = TdxIPManager(
                hosts=[{"ip": "8.8.8.8", "port": 7709, "name": "sz"}],
                probe_workers=1,
                refresh_deadline_sec=2.0,
            )
            mgr.refresh()
        assert mgr.ranked_ips[0].status == "active"
        assert TdxSource._is_connection_healthy(api)

        source = TdxSource.__new__(TdxSource)
        source.pool = Mock()
        source.pool.get_connection.return_value = api
        assert source._sync_test_connection() == [PARSEABLE_BAR]

    def test_malformed_primary_then_parseable_fallback(self):
        from data_sources.tdx_source import (
            TdxIPManager,
            TdxSource,
            is_parseable_daily_bar,
        )

        malformed = {
            "datetime": "not-a-date",
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
        }
        assert is_parseable_daily_bar(malformed) is False
        nan_bar = {
            "datetime": "2026-09-09",
            "open": float("nan"),
            "high": 1,
            "low": 1,
            "close": 1,
        }
        assert is_parseable_daily_bar(nan_bar) is False

        def get_bars(_category, _market, code, _offset, _count):
            if code == "600000":
                return [malformed]
            return [PARSEABLE_BAR]

        api = Mock()
        api.connect.return_value = api
        api.get_security_bars.side_effect = get_bars
        api.client = None
        with patch("data_sources.tdx_source.TdxHq_API", return_value=api), _no_probe_tcp():
            mgr = TdxIPManager(
                hosts=[{"ip": "8.8.8.8", "port": 7709}],
                probe_workers=1,
                refresh_deadline_sec=2.0,
            )
            mgr.refresh()
        assert mgr.ranked_ips[0].status == "active"
        assert TdxSource._is_connection_healthy(api)

    def test_transport_exception_is_not_empty_bar(self):
        from data_sources.tdx_source import TdxIPManager

        api = Mock()
        api.connect.return_value = api
        api.get_security_bars.side_effect = OSError("reset")
        api.client = None
        with patch("data_sources.tdx_source.TdxHq_API", return_value=api), _no_probe_tcp():
            mgr = TdxIPManager(
                hosts=[{"ip": "8.8.8.8", "port": 7709}],
                probe_workers=1,
                refresh_deadline_sec=2.0,
            )
            mgr.refresh()
        assert mgr.ranked_ips[0].status == "transport_failed"

    def test_swallowed_none_is_not_empty_bar(self):
        from data_sources.tdx_source import TdxIPManager

        api = Mock()
        api.connect.return_value = api
        api.get_security_bars.return_value = None
        api.client = None
        with patch("data_sources.tdx_source.TdxHq_API", return_value=api), _no_probe_tcp():
            mgr = TdxIPManager(
                hosts=[{"ip": "8.8.8.8", "port": 7709}],
                probe_workers=1,
                refresh_deadline_sec=2.0,
            )
            mgr.refresh()
        assert mgr.ranked_ips[0].status == "transport_failed"

    def test_plain_socket_cannot_run_real_pytdx_setup(self):
        from pytdx.hq import TdxHq_API

        class PlainSocket:
            def send(self, data, flags=0):
                return len(data)

            def recv(self, n, flags=0):
                return b"\x00" * n

        api = TdxHq_API(heartbeat=False, auto_retry=False, raise_exception=True)
        api.client = PlainSocket()
        with pytest.raises(AttributeError, match="send_pkg_num"):
            api.setup()

    def test_real_setup_and_bar_parser_use_probe_traffic_stat_socket(self):
        import struct
        from pytdx.base_socket_client import TrafficStatSocket
        from data_sources.tdx_source import TdxIPManager

        def reply(body: bytes) -> bytes:
            header = struct.pack("<IIIHH", 0, 0, 0, len(body), len(body))
            return header + body

        empty_bars = struct.pack("<H", 0)
        inbox = bytearray()
        for _ in range(5):
            inbox.extend(reply(empty_bars))

        created = []
        real_open = TdxIPManager._open_probe_socket

        def scripted_open(self, ip, port):
            sock = real_open(self, ip, port)
            created.append(sock)

            def send(data, flags=0):
                return len(data)

            def recv(bufsize, flags=0):
                n = min(bufsize, len(inbox))
                chunk = bytes(inbox[:n])
                del inbox[:n]
                return chunk

            sock.connect = lambda addr: None
            sock.send = send
            sock.recv = recv
            return sock

        mgr = TdxIPManager(
            hosts=[{"ip": "1.1.1.1", "port": 7709}],
            probe_workers=1,
            refresh_deadline_sec=2.0,
        )
        with patch.object(TdxIPManager, "_open_probe_socket", scripted_open):
            mgr.refresh()
        assert created
        assert all(isinstance(sock, TrafficStatSocket) for sock in created)
        assert mgr.ranked_ips[0].status == "empty_bars"
        assert mgr.ranked_ips[0].status != "unreachable"

    def test_duplicate_ip_port_is_probed_once(self):
        from data_sources.tdx_source import merge_hq_host_lists

        merged = merge_hq_host_lists(
            [{"ip": "1.1.1.1", "port": 7709, "name": "quote"}],
            [("pytdx", "1.1.1.1", 7709), ("broker", "2.2.2.2", 7709)],
        )
        assert [(h["ip"], h["port"]) for h in merged] == [
            ("1.1.1.1", 7709),
            ("2.2.2.2", 7709),
        ]

    def test_extra_hosts_are_appended_after_quote_and_pytdx(self):
        from data_sources.tdx_source import merge_hq_host_lists

        merged = merge_hq_host_lists(
            [{"ip": "1.1.1.1", "port": 7709, "name": "quote"}],
            [("pytdx", "2.2.2.2", 7709)],
            extra_hosts=[
                {"ip": "3.3.3.3", "port": 7709, "name": "cloud"},
                {"ip": "1.1.1.1", "port": 7709, "name": "dup"},
                {"ip": "hq.example.com", "port": 7709, "name": "dns"},
                {"ip": "4.4.4.4", "port": 7711, "name": "alt-port"},
            ],
        )
        assert [(h["ip"], h["port"]) for h in merged] == [
            ("1.1.1.1", 7709),
            ("2.2.2.2", 7709),
            ("3.3.3.3", 7709),
            ("4.4.4.4", 7711),
            ("hq.example.com", 7709),
        ]

    def test_default_hq_hosts_include_vendored_third_party_ips(self):
        from data_sources.tdx_hq_extra_hosts import SUPPLEMENTAL_HQ_HOSTS
        from data_sources.tdx_source import default_hq_hosts

        merged = default_hq_hosts()
        keys = {(h["ip"], h["port"]) for h in merged}
        extra_keys = {
            (str(h["ip"]), int(h["port"])) for h in SUPPLEMENTAL_HQ_HOSTS
        }
        assert extra_keys
        assert extra_keys <= keys
        assert ("110.41.147.114", 7709) in keys
        assert ("116.205.183.150", 7709) in keys
        assert len(merged) == len(keys)
        assert len(merged) >= 194

    def test_stale_thread_local_is_evicted(self):
        from data_sources.tdx_source import TdxConnectionPool, TdxIPManager

        mgr = TdxIPManager(
            hosts=[
                {"ip": "1.1.1.1", "port": 7709},
                {"ip": "2.2.2.2", "port": 7709},
            ],
            probe_workers=1,
            refresh_deadline_sec=2.0,
        )
        with patch.object(
            TdxIPManager,
            "_probe_host",
            lambda self, host, deadline, tracker: _entry(
                host["ip"],
                "active",
                latency=10 if host["ip"] == "1.1.1.1" else 20,
            ),
        ):
            mgr.refresh()

        first_api = Mock()
        second_api = Mock()
        created = {"n": 0}

        def api_factory(*_args, **_kwargs):
            created["n"] += 1
            return first_api if created["n"] == 1 else second_api

        with patch("data_sources.tdx_source.TdxHq_API", side_effect=api_factory):
            first_api.connect.return_value = first_api
            second_api.connect.return_value = second_api
            pool = TdxConnectionPool(mgr)
            assert pool.get_connection() is first_api
            mgr.ranked_ips = [_entry("2.2.2.2", "active", latency=20)]
            mgr._selectable_keys = {"2.2.2.2:7709"}
            mgr._last_refresh = datetime.now()
            with patch("data_sources.tdx_source.close_tdx_socket") as closer:
                assert pool.get_connection() is second_api
                closer.assert_called()


class TestRefreshDeadlineAndLocks:
    def test_slow_protocol_is_unfinished_within_budget(self):
        from data_sources.tdx_source import TdxIPManager

        deadline = 0.15

        def slow(self, host, deadline_ts, tracker):
            time.sleep(0.6)
            return _entry(host["ip"], "active", latency=1.0)

        mgr = TdxIPManager(
            hosts=[{"ip": "1.1.1.1", "port": 7709}],
            probe_workers=1,
            refresh_deadline_sec=deadline,
        )
        with patch.object(TdxIPManager, "_probe_host", slow):
            t0 = time.monotonic()
            mgr.refresh()
            elapsed = time.monotonic() - t0
        assert elapsed <= deadline + 0.2
        assert mgr.last_refresh_duration_sec <= deadline + 0.2
        assert mgr.ranked_ips[0].status == "unfinished"
        assert mgr.ranked_ips[0].status != "unreachable"

    def test_late_results_do_not_mutate_ranking(self):
        from data_sources.tdx_source import TdxIPManager

        deadline = 0.1

        def late(self, host, deadline_ts, tracker):
            time.sleep(0.25)
            return _entry(host["ip"], "active", latency=1.0)

        mgr = TdxIPManager(
            hosts=[{"ip": "1.1.1.1", "port": 7709}],
            probe_workers=1,
            refresh_deadline_sec=deadline,
        )
        with patch.object(TdxIPManager, "_probe_host", late):
            mgr.refresh()
            time.sleep(0.3)
        assert mgr.ranked_ips[0].status == "unfinished"

    def test_connect_in_progress_is_unfinished_and_socket_closed(self):
        from data_sources.tdx_source import TdxIPManager

        deadline = 0.15
        closed = []

        class SlowSock:
            def settimeout(self, *_args, **_kwargs):
                return None

            def connect(self, *_args, **_kwargs):
                time.sleep(0.6)

            def shutdown(self, *_args, **_kwargs):
                return None

            def close(self):
                closed.append("close")

        class ProbeAPI:
            def __init__(self, **_kwargs):
                self.client = None
                self.need_setup = False

            def get_security_bars(self, *_args, **_kwargs):
                return [PARSEABLE_BAR]

        mgr = TdxIPManager(
            hosts=[{"ip": "1.1.1.1", "port": 7709}],
            probe_workers=1,
            refresh_deadline_sec=deadline,
        )
        with patch("data_sources.tdx_source.TdxHq_API", ProbeAPI), patch.object(
            TdxIPManager, "_open_probe_socket", lambda self, ip, port: SlowSock()
        ):
            t0 = time.monotonic()
            mgr.refresh()
            elapsed = time.monotonic() - t0
        assert elapsed <= deadline + 0.2
        assert mgr.ranked_ips[0].status == "unfinished"
        assert "close" in closed

    def test_shutdown_error_still_closes_and_keeps_completed_result(self):
        from data_sources.tdx_source import TdxIPManager, close_tdx_socket

        client = Mock()
        client.shutdown.side_effect = OSError("disconnect err")
        api = Mock()
        api.client = client
        close_tdx_socket(api)
        client.close.assert_called_once()
        assert api.client is None

        deadline = 0.2
        closed = []
        boom_client = Mock()
        boom_client.settimeout = Mock()
        boom_client.shutdown.side_effect = OSError("disconnect err")
        boom_client.close.side_effect = lambda *_args, **_kwargs: closed.append("close")
        boom_client.connect.side_effect = lambda *_args, **_kwargs: time.sleep(0.5)

        class ProbeAPI:
            def __init__(self, **_kwargs):
                self.client = None
                self.need_setup = False

            def get_security_bars(self, *_args, **_kwargs):
                return [PARSEABLE_BAR]

        def open_sock(self, ip, port):
            if ip == "2.2.2.2":
                return boom_client
            fast = Mock()
            fast.settimeout = Mock()
            fast.connect = Mock()
            fast.shutdown = Mock()
            fast.close = Mock()
            return fast

        mgr = TdxIPManager(
            hosts=[
                {"ip": "1.1.1.1", "port": 7709},
                {"ip": "2.2.2.2", "port": 7709},
            ],
            probe_workers=2,
            refresh_deadline_sec=deadline,
        )
        with patch("data_sources.tdx_source.TdxHq_API", ProbeAPI), patch.object(
            TdxIPManager, "_open_probe_socket", open_sock
        ):
            mgr.refresh()
        statuses = {e.ip: e.status for e in mgr.ranked_ips}
        assert statuses["1.1.1.1"] == "active"
        assert statuses["2.2.2.2"] == "unfinished"
        assert "close" in closed

    def test_cleanup_cost_stays_inside_refresh_budget(self):
        from data_sources.tdx_source import TdxIPManager

        deadline = 0.1
        hosts = [{"ip": f"1.1.1.{i}", "port": 7709} for i in range(16)]

        def slow_close(api):
            time.sleep(0.03)
            client = getattr(api, "client", None)
            if client is None:
                return
            try:
                client.shutdown(2)
            except Exception:
                pass
            try:
                client.close()
            except Exception:
                pass
            try:
                api.client = None
            except Exception:
                pass

        def fake_probe(self, host, deadline_ts, tracker):
            api = Mock()
            sock = Mock()
            sock.shutdown = Mock()
            sock.close = Mock()
            if not tracker.register(api):
                return _entry(host["ip"], "unfinished")
            try:
                if not tracker.install_socket(api, sock):
                    return _entry(host["ip"], "unfinished")
                time.sleep(1.0)
                return _entry(host["ip"], "active")
            finally:
                tracker.unregister(api)

        mgr = TdxIPManager(
            hosts=hosts,
            probe_workers=16,
            refresh_deadline_sec=deadline,
        )
        with patch.object(TdxIPManager, "_probe_host", fake_probe), patch(
            "data_sources.tdx_source.close_tdx_socket", slow_close
        ):
            t0 = time.monotonic()
            mgr.refresh()
            elapsed = time.monotonic() - t0
        assert elapsed <= deadline + 0.2
        assert mgr.last_refresh_duration_sec <= deadline + 0.2
        assert all(entry.status == "unfinished" for entry in mgr.ranked_ips)

    def test_deadline_stop_closes_socket_created_after_register(self):
        from data_sources.tdx_source import TdxIPManager

        deadline = 0.1
        created = []
        registered = threading.Event()
        allow_create = threading.Event()

        class RecSock:
            def __init__(self):
                self.closed = False
                self.connect_called = False

            def settimeout(self, *_args, **_kwargs):
                return None

            def connect(self, *_args, **_kwargs):
                self.connect_called = True

            def shutdown(self, *_args, **_kwargs):
                return None

            def close(self):
                self.closed = True

        class ProbeAPI:
            def __init__(self, **_kwargs):
                self.client = None
                self.need_setup = False

            def get_security_bars(self, *_args, **_kwargs):
                return [PARSEABLE_BAR]

        def delayed_open(self, ip, port):
            registered.set()
            assert allow_create.wait(timeout=2.0)
            sock = RecSock()
            created.append(sock)
            return sock

        mgr = TdxIPManager(
            hosts=[{"ip": "1.1.1.1", "port": 7709}],
            probe_workers=1,
            refresh_deadline_sec=deadline,
        )
        elapsed = {}

        def run_refresh():
            t0 = time.monotonic()
            with patch("data_sources.tdx_source.TdxHq_API", ProbeAPI), patch.object(
                TdxIPManager, "_open_probe_socket", delayed_open
            ):
                mgr.refresh()
            elapsed["sec"] = time.monotonic() - t0

        worker = threading.Thread(target=run_refresh)
        worker.start()
        assert registered.wait(2.0)
        worker.join(2.0)
        assert not worker.is_alive()
        assert elapsed["sec"] <= deadline + 0.2
        allow_create.set()
        until = time.monotonic() + 1.0
        while time.monotonic() < until:
            if created and all(sock.closed for sock in created):
                break
            time.sleep(0.01)
        assert created
        assert all(sock.closed for sock in created)
        assert all(not sock.connect_called for sock in created)
        assert mgr.ranked_ips[0].status == "unfinished"

    def test_expired_and_forced_refresh_do_not_deadlock_or_scan_parallel(self):
        from data_sources.tdx_source import TdxConnectionPool, TdxIPManager

        in_scan = 0
        max_parallel = 0
        lock = threading.Lock()
        started = threading.Event()

        def slow(self, host, deadline, tracker):
            nonlocal in_scan, max_parallel
            with lock:
                in_scan += 1
                max_parallel = max(max_parallel, in_scan)
            started.set()
            time.sleep(0.2)
            with lock:
                in_scan -= 1
            return _entry(host["ip"], "active", latency=1.0)

        mgr = TdxIPManager(
            hosts=[{"ip": "1.1.1.1", "port": 7709}],
            probe_workers=1,
            refresh_deadline_sec=8.0,
            refresh_interval_hours=0.00001,
        )
        errors = []

        def run_expired():
            try:
                mgr.refresh(force=False)
            except Exception as exc:
                errors.append(exc)

        def run_forced():
            started.wait(2)
            try:
                mgr.refresh(force=True)
            except Exception as exc:
                errors.append(exc)

        with patch.object(TdxIPManager, "_probe_host", slow):
            t1 = threading.Thread(target=run_expired)
            t2 = threading.Thread(target=run_forced)
            t1.start()
            t2.start()
            t1.join(5)
            t2.join(5)

            pool = TdxConnectionPool(mgr)
            started.clear()
            mgr._last_refresh = datetime.now() - timedelta(hours=48)

            def run_get():
                try:
                    with patch("data_sources.tdx_source.TdxHq_API") as api_cls:
                        api_cls.return_value.connect.return_value = api_cls.return_value
                        pool.get_connection()
                except Exception as exc:
                    errors.append(exc)

            t3 = threading.Thread(target=run_get)
            t4 = threading.Thread(target=run_forced)
            t3.start()
            t4.start()
            t3.join(5)
            t4.join(5)

        assert not any(t.is_alive() for t in (t1, t2, t3, t4))
        assert errors == []
        assert max_parallel == 1


class TestPytdxZeroHostFactoryFallback:
    @pytest.mark.asyncio
    async def test_zero_selectable_pytdx_lets_baostock_serve_daily(self, monkeypatch):
        from data_sources.baostock_source import BaostockSource
        from data_sources.source_factory import DataSourceFactory
        from data_sources.tdx_source import TdxIPManager

        cfg = {
            "data_sources": {"a_stock": {"enabled": True}},
            "data_sources_config": {
                "pytdx": {
                    "enabled": True,
                    "exchanges_supported": ["a_stock"],
                    "instrument_types_supported": ["stock"],
                },
                "baostock": {
                    "enabled": True,
                    "exchanges_supported": ["a_stock"],
                    "instrument_types_supported": ["stock"],
                },
            },
            "routing": {
                "daily": {"SSE": {"stock": ["pytdx", "baostock"]}},
                "instrument_list": {"a_stock": ["baostock"]},
                "calendar": {"a_stock": ["baostock"]},
                "factor": {"SSE": {"primary": "baostock"}},
            },
        }

        class _Cfg:
            def get(self, key, default=None):
                return cfg.get(key, default)

        def fake_refresh(self, force=True):
            with self._lock:
                self.ranked_ips = []
                self._selectable_keys = set()
                self._last_refresh = datetime.now()

        async def skip_baostock_init(self):
            return None

        monkeypatch.setattr(TdxIPManager, "refresh", fake_refresh)
        monkeypatch.setattr(BaostockSource, "_initialize_impl", skip_baostock_init)

        factory = DataSourceFactory(Mock())
        factory.config = _Cfg()
        await factory.initialize()

        assert "pytdx_a_stock" not in factory.sources
        assert "baostock_a_stock" in factory.sources
        baostock = factory.sources["baostock_a_stock"]
        assert factory.get_primary_source("SSE", "stock") is baostock

        quote = [{
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "time": datetime(2026, 9, 9),
            "open": 9.0,
            "high": 9.3,
            "low": 9.0,
            "close": 9.23,
            "volume": 1000,
            "amount": 9000,
            "source": "baostock",
        }]
        baostock.get_daily_data = AsyncMock(return_value=quote)
        factory._validate_daily_data = Mock(return_value=True)
        factory._validate_daily_date_coverage = AsyncMock(return_value=True)
        factory._validate_yfinance_tradable_volume = AsyncMock(return_value=True)
        factory._drop_non_trading_day_daily_quotes = AsyncMock(
            side_effect=lambda data, **_kwargs: data
        )

        rows = await factory.get_daily_data(
            "SSE",
            "600000.SH",
            "600000",
            datetime(2026, 9, 9),
            datetime(2026, 9, 9),
            ignore_coverage_breaker=True,
        )
        assert rows == quote
        baostock.get_daily_data.assert_awaited()

