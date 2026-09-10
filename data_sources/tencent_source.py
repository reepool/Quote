"""腾讯 A 股日线备源 (pytdx 之后的 routing.daily 第一备源)。

对接腾讯财经 newfqkline 日线接口与 qt.gtimg.cn 批量快照接口。口径合同见
openspec change `add-tencent-a-share-daily-backup-source`:

- 单一 URL, 空复权参数 (day 节点, 不复权原始价);
- 日线行 11 列: [日期, 开, 收, 高, 低, 量, {}, 换手类比率, 额(万元), 0, 0],
  收盘价在 idx2, idx6/idx7/idx9/idx10 不使用;
- 成交量量纲按代码前缀: sh688*/sh689* 原始值已是股 (×1), 其余为手 (×100);
- amount = idx8 (万元) × 10000; turnover 不猜 idx7, 恒 0.0;
- 窗口覆盖: 按批有界翻页 (每请求实返 ≤count+1 根, 按日期去重), 安全阀 100 次;
- pre_close: 自 start_date 回看 15 个日历日推算, 输出滤回 [start_date, end_date],
  回看内无更早 bar (IPO 首日) 时 pre_close=None;
- 盘中当日未收盘 bar 原样返回, 完整性由调度端收盘等待与 DataManager 处理;
- 数据级失败 (空节点/死代码/畸形行) 返回 [] + last_fetch_diagnostic;
  传输级故障抛 ConnectionError; HTTP 403/429 抛 TencentHTTPStatusError
  (携带 code/status 属性), 可被工厂 _is_daily_http_throttle_error 识别。
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from utils import ds_logger
from utils.http_transport import (
    create_requests_session,
    tls_config_from_source_config,
)

from .base_source import BaseDataSource, RateLimitConfig

KLINE_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
SNAPSHOT_URL = "https://qt.gtimg.cn/q="
_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"

_SUFFIX_TO_PREFIX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
# 原始成交量已经是股的代码前缀 (科创板/CDR), 其余为手
_SHARE_UNIT_PREFIXES = ("sh688", "sh689")
_SNAPSHOT_FIELD_COUNT = 38
_MAX_PAGING_ITERATIONS = 100
_PRE_CLOSE_LOOKBACK_DAYS = 15


class TencentHTTPStatusError(Exception):
    """腾讯接口返回非 200 状态码 (如 403/429)。

    携带 code/status 属性, 供工厂 _is_daily_http_throttle_error 识别并计入
    throttle 熔断, 避免把限流吞成空结果逐只空转。
    """

    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.code = status_code
        self.status = status_code


class TencentSource(BaseDataSource):
    """腾讯财经日线备源 (仅 A 股股票日线与最新快照)。"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None,
                 config: Dict[str, Any] = None):
        super().__init__(name, rate_limit_config)
        self.source_config = config or {}
        self.kline_url = KLINE_URL
        self.snapshot_url = SNAPSHOT_URL
        self.request_timeout = float(self.source_config.get("connection_timeout_sec", 10))
        self.batch_size = int(self.source_config.get("batch_size", 800))
        self.session: Optional[requests.Session] = None
        self.last_fetch_diagnostic: Dict[str, Any] = {}

    async def _initialize_impl(self):
        tls_config = tls_config_from_source_config(self.name, self.source_config)
        self.session = create_requests_session(
            tls_config=tls_config,
            headers={"User-Agent": _USER_AGENT},
        )
        ds_logger.info(
            "[%s] tencent source initialized (batch_size=%s, timeout=%ss)",
            self.name, self.batch_size, self.request_timeout,
        )

    async def close(self):
        if self.session:
            self.session.close()
            self.session = None
        ds_logger.info("[%s] tencent source closed", self.name)

    async def get_instrument_list(self, exchange: str = None) -> List[Dict[str, Any]]:
        """本源不提供股票池; routing.instrument_list 不引用, 返回空保持遍历安全。"""
        return []

    @staticmethod
    def _to_tencent_code(instrument_id: str) -> str:
        symbol, sep, suffix = instrument_id.partition(".")
        prefix = _SUFFIX_TO_PREFIX.get(suffix.upper()) if sep else None
        if not prefix or not symbol:
            raise ValueError(f"[tencent] 无法映射品种代码: {instrument_id}")
        return f"{prefix}{symbol}"

    @staticmethod
    def _volume_scale(code: str) -> int:
        return 1 if code.startswith(_SHARE_UNIT_PREFIXES) else 100

    async def get_daily_data(self, instrument_id: str, symbol: str,
                             start_date: datetime, end_date: datetime,
                             instrument_type: str = 'stock',
                             source_symbol: str = '') -> List[Dict[str, Any]]:
        code = self._to_tencent_code(instrument_id)
        await self.rate_limiter.acquire()
        return await asyncio.to_thread(
            self._sync_get_daily_data, code, instrument_id, symbol, start_date, end_date
        )

    def _fetch_kline_page(self, code: str, beg: str, end: str) -> List[list]:
        """拉取一页日线 (实返 ≤batch_size+1 根); 传输级失败抛异常。"""
        param = f"{code},day,{beg},{end},{self.batch_size},"
        try:
            response = self.session.get(
                f"{self.kline_url}?param={param}", timeout=self.request_timeout
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            raise ConnectionError(f"[tencent] 日线连接失败 {code}: {exc}") from exc
        if response.status_code != 200:
            raise TencentHTTPStatusError(
                f"[tencent] http error {response.status_code} for {code}",
                response.status_code,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ConnectionError(f"[tencent] 日线响应非 JSON {code}: {exc}") from exc
        return ((payload.get("data") or {}).get(code) or {}).get("day") or []

    def _sync_get_daily_data(self, code: str, instrument_id: str, symbol: str,
                             start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        self.last_fetch_diagnostic = {}
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        # pre_close 回看: 自 start_date 再往前 15 个日历日 (春节/国庆最大间隔 11 天)
        lookback_start = (start_date - timedelta(days=_PRE_CLOSE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        scale = self._volume_scale(code)
        start_day = start_date.date()
        end_day = end_date.date()

        rows_by_day: Dict[str, list] = {}
        skipped = 0
        cursor_end = end_str
        for _ in range(_MAX_PAGING_ITERATIONS):
            node = self._fetch_kline_page(code, lookback_start, cursor_end)
            if not node:
                # 页空: 覆盖完成、或已翻到上市日之前 (健康空), 终止翻页
                break
            for row in node:
                if not isinstance(row, (list, tuple)) or len(row) < 6:
                    skipped += 1
                    continue
                day = str(row[0])[:10]
                if day not in rows_by_day:
                    rows_by_day[day] = row
            earliest = str(node[0][0])[:10]
            if earliest <= lookback_start or len(node) < 2:
                break
            cursor_end = (datetime.strptime(earliest, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

        quotes: List[Dict[str, Any]] = []
        pre_close: Optional[float] = None
        for day in sorted(rows_by_day):
            row = rows_by_day[day]
            try:
                bar_time = datetime.strptime(day, "%Y-%m-%d")
                open_price = float(row[1])
                close_price = float(row[2])
                high_price = float(row[3])
                low_price = float(row[4])
                volume = int(round(float(row[5]) * scale))
                amount = round(float(row[8]) * 10000.0, 2) if len(row) > 8 else 0.0
            except (TypeError, ValueError):
                skipped += 1
                continue
            quote = {
                "time": bar_time,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "amount": amount,
                "turnover": 0.0,
                "pre_close": pre_close,
                "change": round(close_price - pre_close, 4) if pre_close is not None else None,
                "pct_change": (
                    round((close_price / pre_close - 1.0) * 100.0, 4)
                    if pre_close is not None else None
                ),
                "tradestatus": 1 if volume > 0 else 0,
                "factor": 1.0,
                "adjustment_type": "none",
                "source": self.name,
            }
            if start_day <= bar_time.date() <= end_day:
                quotes.append(quote)
            pre_close = close_price
        if skipped:
            self.last_fetch_diagnostic = {
                "connection_unhealthy": False,
                "reason": "malformed_rows",
                "skipped": skipped,
            }
        elif not rows_by_day:
            self.last_fetch_diagnostic = {
                "connection_unhealthy": False, "reason": "empty_day_node",
            }
        elif not quotes:
            self.last_fetch_diagnostic = {
                "connection_unhealthy": False, "reason": "no_rows_in_window",
            }
        ds_logger.info("[%s] %s: 获取 %s 根日线 [%s .. %s]",
                       self.name, code, len(quotes), start_day, end_day)
        return quotes

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        code = self._to_tencent_code(instrument_id)
        await self.rate_limiter.acquire()
        return await asyncio.to_thread(self._sync_get_latest_daily_data, code, instrument_id, symbol)

    def _sync_get_latest_daily_data(self, code: str, instrument_id: str, symbol: str) -> Dict[str, Any]:
        self.last_fetch_diagnostic = {}
        try:
            response = self.session.get(
                self.snapshot_url + code, timeout=self.request_timeout
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            raise ConnectionError(f"[tencent] 快照连接失败 {code}: {exc}") from exc
        if response.status_code != 200:
            raise TencentHTTPStatusError(
                f"[tencent] http error {response.status_code} for {code}",
                response.status_code,
            )
        response.encoding = "gbk"
        text = response.text
        if '="' not in text or '"' not in text[text.index('="') + 2:]:
            self.last_fetch_diagnostic = {
                "connection_unhealthy": False, "reason": "empty_snapshot",
            }
            return {}
        fields = text.split('"', 2)[1].split("~")
        if len(fields) < _SNAPSHOT_FIELD_COUNT or not fields[3]:
            self.last_fetch_diagnostic = {
                "connection_unhealthy": False, "reason": "incomplete_snapshot",
            }
            return {}
        try:
            volume = int(round(float(fields[6]) * self._volume_scale(code)))
            snapshot = {
                "time": datetime.strptime(fields[30], "%Y%m%d%H%M%S"),
                "instrument_id": instrument_id,
                "symbol": symbol,
                "open": float(fields[5]),
                "high": float(fields[33]),
                "low": float(fields[34]),
                "close": float(fields[3]),
                "volume": volume,
                "amount": round(float(fields[37]) * 10000.0, 2),
                "turnover": 0.0,
                "pre_close": float(fields[4]),
                "tradestatus": 1,
                "factor": 1.0,
                "adjustment_type": "none",
                "source": self.name,
            }
        except (TypeError, ValueError):
            self.last_fetch_diagnostic = {
                "connection_unhealthy": False, "reason": "malformed_snapshot",
            }
            return {}
        ds_logger.info("[%s] %s: 快照 close=%s volume=%s",
                       self.name, code, snapshot["close"], volume)
        return snapshot

    async def health_check(self) -> bool:
        try:
            probe_end = datetime.now()
            bars = await self.get_daily_data(
                "600000.SH", "600000", probe_end - timedelta(days=10), probe_end
            )
            if not bars:
                ds_logger.error("[%s] health check: 探针票无日线返回", self.name)
                return False
            last = bars[-1]
            required = ("time", "open", "high", "low", "close", "volume")
            healthy = all(last.get(field) is not None for field in required)
            if not healthy:
                ds_logger.error("[%s] health check: 探针票字段缺失", self.name)
            return healthy
        except Exception as exc:
            ds_logger.error("[%s] health check failed: %s", self.name, exc)
            return False
