"""TencentSource 单元测试 (mock 共享传输层, 不打真实网络)。

覆盖 openspec change `add-tencent-a-share-daily-backup-source` 的 spec 场景:
11 列字段序、前缀量纲、万元 amount、有界翻页与 count+1 去重、pre_close
春节回看、健康空诊断、403/429 契约、快照字段位。
"""

import json
from datetime import datetime
from pathlib import Path

import pytest
import requests

from data_sources.base_source import RateLimitConfig
from data_sources.source_factory import DataSourceFactory
from data_sources.tencent_source import TencentSource, TencentHTTPStatusError

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _row(day, open_, close, high, low, volume_raw, amount_wan="100.00"):
    """构造 11 列日线行: [日期, 开, 收, 高, 低, 量, {}, 比率, 额(万元), 0, 0]"""
    return [day, str(open_), str(close), str(high), str(low), str(volume_raw),
            {}, "1.11", str(amount_wan), "0.00", "0.00"]


class _FakeResponse:
    def __init__(self, json_data=None, status_code=200, text=""):
        self._json = json_data
        self.status_code = status_code
        self.text = text

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class _FakeSession:
    """按队列依次返回响应或异常。"""

    def __init__(self, queue):
        self.queue = list(queue)
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        pass


def _kline_response(rows, code="sh600000"):
    return _FakeResponse(json_data={"data": {code: {"day": rows}}})


def _build_source(batch_size=800, queue=None) -> TencentSource:
    src = TencentSource(
        "tencent_test",
        RateLimitConfig(max_requests_per_minute=100000),
        config={"batch_size": batch_size, "connection_timeout_sec": 10},
    )
    src.session = _FakeSession(queue or [])
    return src


def _day(pages):
    """把多页行拼成 endpoint 响应队列 (每日一响应)。"""
    return [_kline_response(page) for page in pages]


# ---------- 字段映射与量纲 ----------

def test_close_sits_at_index2_not_ohlc():
    src = _build_source(queue=_day([[_row("2026-09-10", 9.22, 9.30, 9.33, 9.19, 356060)]]))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 10), datetime(2026, 9, 10),
    )
    assert len(bars) == 1
    bar = bars[0]
    assert (bar["open"], bar["close"], bar["high"], bar["low"]) == (9.22, 9.30, 9.33, 9.19)
    assert bar["low"] <= bar["close"] <= bar["high"]


def test_lot_market_volume_converted_by_100():
    src = _build_source(queue=_day([[_row("2026-09-03", 9.24, 9.27, 9.47, 9.22, 898172, "84197.29")]]))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000", datetime(2026, 9, 3), datetime(2026, 9, 3)
    )
    assert bars[0]["volume"] == 89817200
    # amount 万元 -> 元, 且消除二进制尘埃
    assert bars[0]["amount"] == 841972900.0
    assert bars[0]["turnover"] == 0.0


def test_star_and_cdr_volume_kept_as_shares():
    src = _build_source(queue=[
        _kline_response([
            _row("2026-09-07", 122.5, 124.12, 125.0, 122.0, 31314788),
        ], code="sh688981"),
        _kline_response([
            _row("2026-09-07", 40.1, 40.58, 40.9, 40.0, 11650323),
        ], code="sh689009"),
    ])
    star = src._sync_get_daily_data(
        "sh688981", "688981.SH", "688981", datetime(2026, 9, 7), datetime(2026, 9, 7)
    )
    cdr = src._sync_get_daily_data(
        "sh689009", "689009.SH", "689009", datetime(2026, 9, 7), datetime(2026, 9, 7)
    )
    assert star[0]["volume"] == 31314788  # 原始已是股, 不取整到手
    assert cdr[0]["volume"] == 11650323


def test_unsupported_code_raises_value_error():
    src = _build_source()
    with pytest.raises(ValueError):
        src._to_tencent_code("AAPL.US")


# ---------- 翻页与去重 ----------

def test_paging_covers_window_with_count_plus_one_dedupe():
    # batch_size=2 -> 每页实返 ≤3 根; 两页含重叠日 2026-09-03
    pages = [
        [_row("2026-09-07", 9.2, 9.3, 9.4, 9.1, 100),
         _row("2026-09-04", 9.1, 9.2, 9.3, 9.0, 200),
         _row("2026-09-03", 9.0, 9.1, 9.2, 8.9, 300)],
        [_row("2026-09-03", 9.0, 9.1, 9.2, 8.9, 300)],
    ]
    src = _build_source(batch_size=2, queue=_day(pages))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 3), datetime(2026, 9, 7),
    )
    days = [bar["time"].strftime("%Y-%m-%d") for bar in bars]
    assert days == ["2026-09-03", "2026-09-04", "2026-09-07"]


def test_paging_stops_on_empty_page_and_marks_healthy():
    pages = [
        [_row("2026-09-07", 9.2, 9.3, 9.4, 9.1, 100)],
        [],  # 已翻到上市日之前 -> 健康空终止
    ]
    src = _build_source(batch_size=2, queue=_day(pages))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 6), datetime(2026, 9, 7),
    )
    assert len(bars) == 1
    assert src.last_fetch_diagnostic == {}  # 健康非空无需诊断


def test_empty_day_node_returns_empty_with_diagnostic():
    src = _build_source(queue=_day([[]]))
    bars = src._sync_get_daily_data(
        "sh600033", "600033.SH", "600033",
        datetime(2026, 9, 1), datetime(2026, 9, 10),
    )
    assert bars == []
    assert src.last_fetch_diagnostic == {
        "connection_unhealthy": False, "reason": "empty_day_node",
    }


def test_rows_outside_window_yield_no_rows_in_window():
    src = _build_source(queue=[_kline_response(
        [_row("2002-04-26", 2.71, 2.71, 2.71, 2.71, 43972)], code="sz000003",
    )])
    bars = src._sync_get_daily_data(
        "sz000003", "000003.SZ", "000003",
        datetime(2026, 8, 1), datetime(2026, 9, 10),
    )
    assert bars == []
    assert src.last_fetch_diagnostic["reason"] == "no_rows_in_window"
    assert src.last_fetch_diagnostic["connection_unhealthy"] is False


def test_malformed_rows_skipped_with_diagnostic():
    pages = [[
        ["2026-09-07", "9.2", "9.3", "9.4"],  # 列数不足
        ["bad-day", "9.2", "9.3", "9.4", "9.1", "100", {}, "1", "1", "0", "0"],  # 日期非法
        _row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100),
    ], []]  # 翻页终止页
    src = _build_source(queue=_day(pages))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 8), datetime(2026, 9, 8),
    )
    assert len(bars) == 1
    assert src.last_fetch_diagnostic["reason"] == "malformed_rows"
    assert src.last_fetch_diagnostic["skipped"] == 2


def test_intraday_partial_bar_passthrough():
    src = _build_source(queue=_day([[_row("2026-09-10", 9.22, 9.30, 9.33, 9.19, 356060)]]))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 10), datetime(2026, 9, 10),
    )
    assert bars[0]["time"] == datetime(2026, 9, 10)
    assert bars[0]["tradestatus"] == 1


# ---------- pre_close 回看 ----------

def test_pre_close_bridges_spring_festival_gap():
    # 窗口 [2026-02-24, 2026-02-24]; 春节空档 02-13 -> 02-24 (11 个日历日)
    # 回看 15 天 (lookback_start=02-09) 必须覆盖 02-13
    pages = [
        [_row("2026-02-24", 10.0, 10.5, 10.6, 9.9, 100, "5000.00"),
         _row("2026-02-13", 10.2, 10.1, 10.3, 10.0, 100, "4900.00")],
        [],  # 02-12 之前无数据
    ]
    src = _build_source(batch_size=2, queue=_day(pages))
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 2, 24), datetime(2026, 2, 24),
    )
    assert len(bars) == 1
    bar = bars[0]
    assert bar["time"] == datetime(2026, 2, 24)
    assert bar["pre_close"] == 10.1
    assert bar["change"] == 0.4
    assert bar["pct_change"] == round((10.5 / 10.1 - 1) * 100, 4)


def test_pre_close_none_on_first_ever_bar():
    src = _build_source(queue=[_kline_response(
        [_row("2023-05-26", 12.0, 11.71, 12.11, 11.63, 10041)], code="bj920992",
    )])
    bars = src._sync_get_daily_data(
        "bj920992", "920992.BJ", "920992",
        datetime(2023, 5, 26), datetime(2023, 5, 26),
    )
    assert bars[0]["pre_close"] is None
    assert bars[0]["change"] is None and bars[0]["pct_change"] is None


# ---------- 失败语义 ----------

def test_http_403_raises_recognizable_status_error():
    src = _build_source(queue=[_FakeResponse(status_code=403)])
    with pytest.raises(TencentHTTPStatusError) as exc_info:
        src._sync_get_daily_data(
            "sh600000", "600000.SH", "600000",
            datetime(2026, 9, 7), datetime(2026, 9, 7),
        )
    assert exc_info.value.code == 403 and exc_info.value.status == 403
    # 工厂 throttle 熔断必须能识别, 且不误标为源不可用
    assert DataSourceFactory._is_daily_http_throttle_error(exc_info.value) is True
    assert DataSourceFactory._is_daily_source_unavailable_error(exc_info.value) is False


def test_http_403_not_retried():
    src = _build_source(queue=[
        _FakeResponse(status_code=403),
        _kline_response([_row("2026-09-07", 9.2, 9.3, 9.4, 9.1, 100)]),
    ])
    src.rate_limiter.config.retry_times = 3
    src.rate_limiter.config.retry_interval = 0.0
    with pytest.raises(TencentHTTPStatusError):
        src._sync_get_daily_data(
            "sh600000", "600000.SH", "600000",
            datetime(2026, 9, 7), datetime(2026, 9, 7),
        )
    assert len(src.session.calls) == 1  # 限流不重试


def test_http_5xx_retried_then_success():
    src = _build_source(queue=[
        _FakeResponse(status_code=502),
        _FakeResponse(status_code=502),
        _kline_response([_row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100)]),
    ])
    src.rate_limiter.config.retry_times = 3
    src.rate_limiter.config.retry_interval = 0.0
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 8), datetime(2026, 9, 8),
    )
    assert len(bars) == 1
    assert len(src.session.calls) == 3


def test_http_5xx_exhaustion_counts_as_transport_unavailable():
    from data_sources.tencent_source import TencentHTTPTransportStatusError

    src = _build_source(queue=[_FakeResponse(status_code=503)] * 3)
    src.rate_limiter.config.retry_times = 3
    src.rate_limiter.config.retry_interval = 0.0
    with pytest.raises(TencentHTTPTransportStatusError) as exc_info:
        src._sync_get_daily_data(
            "sh600000", "600000.SH", "600000",
            datetime(2026, 9, 8), datetime(2026, 9, 8),
        )
    assert len(src.session.calls) == 3
    assert isinstance(exc_info.value, ConnectionError)
    # 5xx 归 transport/unavailable, 不归 throttle
    assert DataSourceFactory._is_daily_source_unavailable_error(exc_info.value) is True
    assert DataSourceFactory._is_daily_http_throttle_error(exc_info.value) is False


def test_network_error_retried_then_success():
    src = _build_source(queue=[
        requests.exceptions.ConnectionError("connection reset"),
        _kline_response([_row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100)]),
    ])
    src.rate_limiter.config.retry_times = 2
    src.rate_limiter.config.retry_interval = 0.0
    bars = src._sync_get_daily_data(
        "sh600000", "600000.SH", "600000",
        datetime(2026, 9, 8), datetime(2026, 9, 8),
    )
    assert len(bars) == 1
    assert len(src.session.calls) == 2


def test_http_429_message_also_matches_factory_literals():
    exc = TencentHTTPStatusError("[tencent] http error 429 for sh600000", 429)
    assert DataSourceFactory._is_daily_http_throttle_error(exc) is True


def test_connection_error_wrapped_for_transport_breaker():
    src = _build_source(queue=[requests.exceptions.ConnectionError("connection reset")])
    src.rate_limiter.config.retry_times = 1
    with pytest.raises(ConnectionError):
        src._sync_get_daily_data(
            "sh600000", "600000.SH", "600000",
            datetime(2026, 9, 7), datetime(2026, 9, 7),
        )


def test_timeout_wrapped_as_connection_error():
    src = _build_source(queue=[requests.exceptions.Timeout("timed out")])
    src.rate_limiter.config.retry_times = 1
    with pytest.raises(ConnectionError):
        src._sync_get_latest_daily_data("sh600000", "600000.SH", "600000")


# ---------- 批量快照 ----------

def _snapshot_text(fields):
    return 'v_sh600000="' + "~".join(fields) + '";\n'


def _snapshot_fields(last="9.30", prev="9.23", open_="9.22", vol="356060", amount_wan="33040.12"):
    fields = ["0"] * 45
    fields[1], fields[2] = "浦发银行", "600000"
    fields[3], fields[4], fields[5] = last, prev, open_
    fields[6] = vol
    fields[30] = "20260910150003"
    fields[33], fields[34] = "9.33", "9.19"
    fields[37] = amount_wan
    return fields


def test_snapshot_field_mapping_main_board_scale():
    src = _build_source(queue=[_FakeResponse(text=_snapshot_text(_snapshot_fields()))])
    snap = src._sync_get_latest_daily_data("sh600000", "600000.SH", "600000")
    assert snap["close"] == 9.30 and snap["pre_close"] == 9.23 and snap["open"] == 9.22
    assert snap["high"] == 9.33 and snap["low"] == 9.19
    assert snap["volume"] == 35606000  # 手 -> 股
    assert snap["amount"] == 330401200.0
    assert snap["time"] == datetime(2026, 9, 10, 15, 0, 3)


def test_snapshot_star_volume_keeps_share_unit():
    fields = _snapshot_fields(last="119.35", vol="14307846")
    src = _build_source(queue=[_FakeResponse(text=_snapshot_text(fields))])
    snap = src._sync_get_latest_daily_data("sh688981", "688981.SH", "688981")
    assert snap["volume"] == 14307846  # sh688* 原始已是股


def test_snapshot_dead_code_returns_empty_dict():
    src = _build_source(queue=[_FakeResponse(text='pv_none="";\n')])
    snap = src._sync_get_latest_daily_data("sz000003", "000003.SZ", "000003")
    assert snap == {}


# ---------- 接线 ----------

def test_get_instrument_list_returns_empty_instead_of_raising():
    src = _build_source()
    assert src.get_instrument_list.__doc__  # 显式契约注释
    assert asyncio_run(src.get_instrument_list("SSE")) == []


def test_close_releases_session():
    import asyncio

    src = _build_source()
    src.session = _FakeSession([])
    asyncio.run(src.close())
    assert src.session is None


def test_factory_creates_tencent_source_and_route_order():
    factory = DataSourceFactory(None)
    rl = RateLimitConfig()
    inst = factory._create_source_instance(
        "tencent", "tencent_a_stock",
        {"enabled": True, "batch_size": 800}, rl,
    )
    assert isinstance(inst, TencentSource)

    config = json.loads((_REPO_ROOT / "config" / "03_data.json").read_text(encoding="utf-8"))
    daily = config["routing"]["daily"]
    assert daily["SSE"]["stock"] == ["pytdx", "tencent", "baostock", "akshare"]
    assert daily["SZSE"]["stock"] == ["pytdx", "tencent", "baostock", "akshare"]
    assert daily["BSE"]["stock"] == ["pytdx", "tencent", "akshare"]
    # 其余路由不得引用 tencent
    for section in ("instrument_list", "calendar", "factor"):
        assert "tencent" not in json.dumps(config["routing"].get(section, {}))
    assert "tencent" not in json.dumps(daily["SSE"]["index"])
    assert "tencent" not in json.dumps(daily["HKEX"])


def asyncio_run(coro):
    import asyncio
    return asyncio.run(coro)
