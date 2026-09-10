"""TencentSource 单元测试 (mock 共享传输层, 不打真实网络)。

覆盖 openspec change `add-tencent-a-share-daily-backup-source` 的 spec 场景:
11 列字段序、前缀量纲、万元 amount、有界翻页 (升序夹具, 空页终止)、
pre_close 春节回看、健康空诊断、403/429 与 5xx 契约、快照字段位。
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import pytest
import requests

from data_sources.base_source import RateLimitConfig
from data_sources.source_factory import DataSourceFactory
from data_sources.tencent_source import (
    TencentHTTPStatusError,
    TencentHTTPTransportStatusError,
    TencentSource,
)

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


def _day(pages, code="sh600000"):
    # 自动追加终止空页: 新终止逻辑下最早日 > 回看起点时会再翻一页取空
    return [_kline_response(page, code=code) for page in pages] + [
        _kline_response([], code=code)
    ]


# ---------- 字段映射与量纲 ----------

@pytest.mark.asyncio
async def test_close_sits_at_index2_not_ohlc():
    src = _build_source(queue=_day([[_row("2026-09-10", 9.22, 9.30, 9.33, 9.19, 356060)]]))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 10), datetime(2026, 9, 10)
    )
    assert len(bars) == 1
    bar = bars[0]
    assert (bar["open"], bar["close"], bar["high"], bar["low"]) == (9.22, 9.30, 9.33, 9.19)
    assert bar["low"] <= bar["close"] <= bar["high"]


@pytest.mark.asyncio
async def test_lot_market_volume_converted_by_100():
    src = _build_source(queue=_day([[_row("2026-09-03", 9.24, 9.27, 9.47, 9.22, 898172, "84197.29")]]))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 3), datetime(2026, 9, 3)
    )
    assert bars[0]["volume"] == 89817200
    # amount 万元 -> 元, 且消除二进制尘埃
    assert bars[0]["amount"] == 841972900.0
    assert bars[0]["turnover"] == 0.0


@pytest.mark.asyncio
async def test_bse_volume_converted_by_100():
    src = _build_source(queue=_day([[_row("2026-09-03", 14.66, 14.28, 14.96, 14.28, 11810, "1707.67")]],
                                   code="bj920000"))
    bars = await src.get_daily_data(
        "920000.BJ", "920000", datetime(2026, 9, 3), datetime(2026, 9, 3)
    )
    assert bars[0]["volume"] == 1181000
    assert bars[0]["amount"] == 17076700.0


@pytest.mark.asyncio
async def test_star_and_cdr_volume_kept_as_shares():
    src = _build_source(queue=_day([
        [_row("2026-09-07", 122.5, 124.12, 125.0, 122.0, 31314788)],
    ], code="sh688981") + _day([
        [_row("2026-09-07", 40.1, 40.58, 40.9, 40.0, 11650323)],
    ], code="sh689009"))
    star = await src.get_daily_data(
        "688981.SH", "688981", datetime(2026, 9, 7), datetime(2026, 9, 7)
    )
    cdr = await src.get_daily_data(
        "689009.SH", "689009", datetime(2026, 9, 7), datetime(2026, 9, 7)
    )
    assert star[0]["volume"] == 31314788  # 原始已是股, 不取整到手
    assert cdr[0]["volume"] == 11650323


def test_unsupported_code_raises_value_error():
    src = _build_source()
    with pytest.raises(ValueError):
        src._to_tencent_code("AAPL.US")


# ---------- 翻页、终止与去重 (升序夹具, 与生产一致) ----------

@pytest.mark.asyncio
async def test_paging_covers_window_with_real_cursor_and_empty_fq():
    # batch_size=2 -> 每页实返 ≤3 根 (count+1); 升序, 首页最早日推进 cursor
    pages = [
        [_row("2026-09-03", 9.0, 9.1, 9.2, 8.9, 300),
         _row("2026-09-04", 9.1, 9.2, 9.3, 9.0, 200),
         _row("2026-09-07", 9.2, 9.3, 9.4, 9.1, 100)],
        [_row("2026-08-19", 8.8, 8.9, 9.0, 8.7, 400),
         _row("2026-08-25", 8.9, 9.0, 9.1, 8.8, 500),
         _row("2026-09-02", 9.0, 9.05, 9.15, 8.95, 600)],
    ]
    src = _build_source(batch_size=2, queue=_day(pages))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 3), datetime(2026, 9, 7)
    )
    days = [bar["time"].strftime("%Y-%m-%d") for bar in bars]
    # 回看行 (08-19/08-25/09-02) 只用于 pre_close 推算, 输出必须滤回窗口
    assert days == ["2026-09-03", "2026-09-04", "2026-09-07"]
    # 跨页 pre_close: 首根 09-03 的昨收来自第二页的 09-02
    assert bars[0]["pre_close"] == 9.05
    # 空复权参数: URL 以空 fq 的逗号结尾, 绝不带 qfq/hfq
    for url in src.session.calls:
        assert url.endswith(",day,2026-08-19,2026-09-07,2,") or \
            url.endswith(",day,2026-08-19,2026-09-02,2,")
        assert "qfq" not in url and "hfq" not in url


@pytest.mark.asyncio
async def test_paging_terminates_on_empty_page():
    # 第二页为真正的空 day 节点: 空页 (而非根数) 终止翻页
    pages = [
        [_row("2026-09-06", 9.0, 9.1, 9.2, 8.9, 300),
         _row("2026-09-07", 9.1, 9.2, 9.3, 9.0, 200),
         _row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100)],
        [],
    ]
    src = _build_source(batch_size=2, queue=_day(pages))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 6), datetime(2026, 9, 8)
    )
    assert [bar["time"].strftime("%Y-%m-%d") for bar in bars] == \
        ["2026-09-06", "2026-09-07", "2026-09-08"]
    assert src.last_fetch_diagnostic == {}  # 健康非空无需诊断


@pytest.mark.asyncio
async def test_empty_day_node_returns_empty_with_diagnostic():
    src = _build_source(queue=_day([[]]))
    bars = await src.get_daily_data(
        "600033.SH", "600033", datetime(2026, 9, 1), datetime(2026, 9, 10)
    )
    assert bars == []
    assert src.last_fetch_diagnostic == {
        "connection_unhealthy": False, "reason": "empty_day_node",
    }


@pytest.mark.asyncio
async def test_rows_outside_window_yield_no_rows_in_window():
    src = _build_source(queue=[_kline_response(
        [_row("2002-04-26", 2.71, 2.71, 2.71, 2.71, 43972)], code="sz000003",
    )])
    bars = await src.get_daily_data(
        "000003.SZ", "000003", datetime(2026, 8, 1), datetime(2026, 9, 10)
    )
    assert bars == []
    assert src.last_fetch_diagnostic["reason"] == "no_rows_in_window"
    assert src.last_fetch_diagnostic["connection_unhealthy"] is False


@pytest.mark.asyncio
async def test_duplicate_days_within_page_deduplicated():
    # 端点异常导致同日重复行时, 以先到者为准, 不产生重复 bar
    pages = [[
        _row("2026-09-07", 9.2, 9.3, 9.4, 9.1, 100),
        _row("2026-09-07", 9.2, 9.9, 9.9, 9.9, 999),
    ]]
    src = _build_source(queue=_day(pages))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 7), datetime(2026, 9, 7)
    )
    assert len(bars) == 1
    assert bars[0]["close"] == 9.3  # 先到者为准


@pytest.mark.asyncio
async def test_malformed_rows_skipped_with_diagnostic():
    pages = [
        [
            ["2026-09-07", "9.2", "9.3", "9.4"],  # 列数不足
            ["bad-day", "9.2", "9.3", "9.4", "9.1", "100", {}, "1", "1", "0", "0"],  # 日期非法
            _row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100),
        ],
        [],  # 翻页终止页
    ]
    src = _build_source(queue=_day(pages))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 8), datetime(2026, 9, 8)
    )
    assert len(bars) == 1
    assert src.last_fetch_diagnostic["reason"] == "malformed_rows"
    assert src.last_fetch_diagnostic["skipped"] == 2


@pytest.mark.asyncio
async def test_intraday_partial_bar_passthrough():
    src = _build_source(queue=_day([[_row("2026-09-10", 9.22, 9.30, 9.33, 9.19, 356060)]]))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 10), datetime(2026, 9, 10)
    )
    assert bars[0]["time"] == datetime(2026, 9, 10)
    assert bars[0]["tradestatus"] == 1


# ---------- pre_close 回看 ----------

@pytest.mark.asyncio
async def test_pre_close_bridges_spring_festival_gap():
    # 窗口 [2026-02-24, 2026-02-24]; 春节空档 02-13 -> 02-24 (11 个日历日)
    # 回看 15 天 (lookback_start=02-09) 必须覆盖 02-13
    pages = [
        [_row("2026-02-13", 10.2, 10.1, 10.3, 10.0, 100, "4900.00"),
         _row("2026-02-24", 10.0, 10.5, 10.6, 9.9, 100, "5000.00")],
        [],  # 02-12 之前无数据
    ]
    src = _build_source(batch_size=2, queue=_day(pages))
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 2, 24), datetime(2026, 2, 24)
    )
    assert len(bars) == 1
    bar = bars[0]
    assert bar["time"] == datetime(2026, 2, 24)
    assert bar["pre_close"] == 10.1
    assert bar["change"] == 0.4
    assert bar["pct_change"] == round((10.5 / 10.1 - 1) * 100, 4)


@pytest.mark.asyncio
async def test_pre_close_none_on_first_ever_bar():
    src = _build_source(queue=_day([
        [_row("2023-05-26", 12.0, 11.71, 12.11, 11.63, 10041)],
    ], code="bj920992"))
    bars = await src.get_daily_data(
        "920992.BJ", "920992", datetime(2023, 5, 26), datetime(2023, 5, 26)
    )
    assert bars[0]["pre_close"] is None
    assert bars[0]["change"] is None and bars[0]["pct_change"] is None


# ---------- 失败语义 ----------

@pytest.mark.asyncio
async def test_http_403_raises_recognizable_status_error():
    src = _build_source(queue=[_FakeResponse(status_code=403)])
    with pytest.raises(TencentHTTPStatusError) as exc_info:
        await src.get_daily_data(
            "600000.SH", "600000", datetime(2026, 9, 7), datetime(2026, 9, 7)
        )
    assert exc_info.value.code == 403 and exc_info.value.status == 403
    # 工厂 throttle 熔断必须能识别, 且不误标为源不可用
    assert DataSourceFactory._is_daily_http_throttle_error(exc_info.value) is True
    assert DataSourceFactory._is_daily_source_unavailable_error(exc_info.value) is False


@pytest.mark.asyncio
async def test_http_403_not_retried():
    src = _build_source(queue=[
        _FakeResponse(status_code=403),
        _kline_response([_row("2026-09-07", 9.2, 9.3, 9.4, 9.1, 100)]),
    ])
    src.rate_limiter.config.retry_times = 3
    src.rate_limiter.config.retry_interval = 0.0
    with pytest.raises(TencentHTTPStatusError):
        await src.get_daily_data(
            "600000.SH", "600000", datetime(2026, 9, 7), datetime(2026, 9, 7)
        )
    assert len(src.session.calls) == 1  # 限流不重试


@pytest.mark.asyncio
async def test_http_5xx_retried_then_success():
    src = _build_source(queue=[
        _FakeResponse(status_code=502),
        _FakeResponse(status_code=502),
    ] + _day([[_row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100)]]))
    src.rate_limiter.config.retry_times = 3
    src.rate_limiter.config.retry_interval = 0.0
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 8), datetime(2026, 9, 8)
    )
    assert len(bars) == 1
    # 2 次 5xx + 1 次成功页 + 1 次翻页终止空页
    assert len(src.session.calls) == 4


@pytest.mark.asyncio
async def test_http_5xx_exhaustion_counts_as_transport_unavailable():
    src = _build_source(queue=[_FakeResponse(status_code=503)] * 3)
    src.rate_limiter.config.retry_times = 3
    src.rate_limiter.config.retry_interval = 0.0
    with pytest.raises(TencentHTTPTransportStatusError) as exc_info:
        await src.get_daily_data(
            "600000.SH", "600000", datetime(2026, 9, 8), datetime(2026, 9, 8)
        )
    assert len(src.session.calls) == 3
    assert isinstance(exc_info.value, ConnectionError)
    # 5xx 归 transport/unavailable, 不归 throttle
    assert DataSourceFactory._is_daily_source_unavailable_error(exc_info.value) is True
    assert DataSourceFactory._is_daily_http_throttle_error(exc_info.value) is False


@pytest.mark.asyncio
async def test_network_error_retried_then_success():
    src = _build_source(queue=[
        requests.exceptions.ConnectionError("connection reset"),
    ] + _day([[_row("2026-09-08", 9.2, 9.3, 9.4, 9.1, 100)]]))
    src.rate_limiter.config.retry_times = 2
    src.rate_limiter.config.retry_interval = 0.0
    bars = await src.get_daily_data(
        "600000.SH", "600000", datetime(2026, 9, 8), datetime(2026, 9, 8)
    )
    assert len(bars) == 1
    # 1 次网络失败 + 1 次成功页 + 1 次翻页终止空页
    assert len(src.session.calls) == 3


@pytest.mark.asyncio
async def test_timeout_wrapped_as_connection_error():
    src = _build_source(queue=[requests.exceptions.Timeout("timed out")])
    src.rate_limiter.config.retry_times = 1
    with pytest.raises(ConnectionError):
        await src.get_latest_daily_data("600000.SH", "600000")


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


@pytest.mark.asyncio
async def test_snapshot_field_mapping_main_board_scale():
    src = _build_source(queue=[_FakeResponse(text=_snapshot_text(_snapshot_fields()))])
    snap = await src.get_latest_daily_data("600000.SH", "600000")
    assert snap["close"] == 9.30 and snap["pre_close"] == 9.23 and snap["open"] == 9.22
    assert snap["high"] == 9.33 and snap["low"] == 9.19
    assert snap["volume"] == 35606000  # 手 -> 股
    assert snap["amount"] == 330401200.0
    assert snap["time"] == datetime(2026, 9, 10, 15, 0, 3)


@pytest.mark.asyncio
async def test_snapshot_star_volume_keeps_share_unit():
    fields = _snapshot_fields(last="119.35", vol="14307846")
    src = _build_source(queue=[_FakeResponse(text=_snapshot_text(fields))])
    snap = await src.get_latest_daily_data("688981.SH", "688981")
    assert snap["volume"] == 14307846  # sh688* 原始已是股


@pytest.mark.asyncio
async def test_snapshot_dead_code_returns_empty_dict():
    src = _build_source(queue=[_FakeResponse(text='pv_none="";\n')])
    snap = await src.get_latest_daily_data("000003.SZ", "000003")
    assert snap == {}


# ---------- 接线 ----------

@pytest.mark.asyncio
async def test_get_instrument_list_returns_empty_instead_of_raising():
    src = _build_source()
    assert await src.get_instrument_list("SSE") == []


@pytest.mark.asyncio
async def test_close_releases_session():
    src = _build_source()
    src.session = _FakeSession([])
    await src.close()
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
