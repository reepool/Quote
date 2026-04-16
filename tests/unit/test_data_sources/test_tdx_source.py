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
from datetime import datetime, date, timedelta
from unittest.mock import Mock, MagicMock, patch, AsyncMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


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


# ===========================================================
# 2. TdxIPManager
# ===========================================================
class TestTdxIPManager:
    """IP 管理器测试 (mock socket 探测)"""

    def _make_manager(self):
        from data_sources.tdx_source import TdxIPManager
        return TdxIPManager(
            hosts=[
                {"ip": "1.1.1.1", "port": 7709, "name": "测试1"},
                {"ip": "2.2.2.2", "port": 7709, "name": "测试2"},
                {"ip": "3.3.3.3", "port": 7709, "name": "测试3"},
            ],
            blacklist_duration_hours=0.01,
            probe_timeout=0.1,
        )

    @patch.object(
        __import__('data_sources.tdx_source', fromlist=['TdxIPManager']).TdxIPManager,
        '_probe_single'
    )
    def test_refresh_sorts_by_latency(self, mock_probe):
        """探测后按延迟排序"""
        mock_probe.side_effect = [500.0, 100.0, 300.0]
        mgr = self._make_manager()
        mgr.refresh()
        assert len(mgr.ranked_ips) == 3
        assert mgr.ranked_ips[0].ip == "2.2.2.2"  # 最快
        assert mgr.ranked_ips[1].ip == "3.3.3.3"
        assert mgr.ranked_ips[2].ip == "1.1.1.1"

    @patch.object(
        __import__('data_sources.tdx_source', fromlist=['TdxIPManager']).TdxIPManager,
        '_probe_single'
    )
    def test_blacklist_and_failover(self, mock_probe):
        """IP 拉黑后自动切换到下一个"""
        mock_probe.side_effect = [100.0, 200.0, 300.0]
        mgr = self._make_manager()
        mgr.refresh()

        # 初始最佳
        ip, port = mgr.get_ip()
        assert ip == "1.1.1.1"

        # 报告故障 → 拉黑 → 切换
        new_ip, new_port = mgr.report_failure("1.1.1.1", 7709)
        assert new_ip == "2.2.2.2"

    @patch.object(
        __import__('data_sources.tdx_source', fromlist=['TdxIPManager']).TdxIPManager,
        '_probe_single'
    )
    def test_all_blacklisted_reset(self, mock_probe):
        """全部被拉黑后清空黑名单"""
        mock_probe.side_effect = [100.0, 200.0, 300.0]
        mgr = self._make_manager()
        mgr.refresh()

        mgr.report_failure("1.1.1.1", 7709)
        mgr.report_failure("2.2.2.2", 7709)
        mgr.report_failure("3.3.3.3", 7709)

        # 全部被封后应清空黑名单
        ip, port = mgr.get_ip()
        assert ip == "1.1.1.1"  # 回到最快的

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

    def test_baostock_routes_instrument_list_and_calendar_for_a_stock(self):
        """baostock 负责 a_stock 的品种列表和交易日历"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        routing = d["routing"]
        assert routing["instrument_list"]["a_stock"][0] == "baostock"
        assert routing["calendar"]["a_stock"][0] == "baostock"

    def test_factor_routes_complete(self):
        """因子路由覆盖 SSE/SZSE/BSE"""
        import json
        config_path = os.path.join(
            os.path.dirname(__file__), '..', '..', '..', 'config', '03_data.json'
        )
        with open(config_path) as f:
            d = json.load(f)
        fs = d["routing"]["factor"]
        assert fs["SSE"]["primary"] == "baostock"
        assert fs["SSE"]["validator"] == "tdx_xdxr"
        assert fs["SZSE"]["primary"] == "baostock"
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
