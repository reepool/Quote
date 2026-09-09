"""
通达信(pytdx)数据源 - 高速 A 股日线数据获取

核心设计:
  1. IP 管理器 (TdxIPManager): 全网探测 → 延迟排序 → 故障黑名单 → 自动切换
  2. 连接池 (TdxConnectionPool): 线程安全的多连接管理
  3. 数据源 (TdxSource): BaseDataSource 实现, 含 run_in_executor 避免阻塞事件循环
  4. vol 单位: pytdx 返回「手」, 转换为「股」(×100) 以匹配系统标准
"""

import asyncio
import logging
import math
import socket
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait as wait_futures
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pytdx.hq import TdxHq_API

from .base_source import BaseDataSource, RateLimitConfig

tdx_logger = logging.getLogger("tdx_source")

# ---------------------------------------------------------------------------
# 内置 IP 候选列表（合并 pytdx 内置 stock_ip + config.hosts）
# ---------------------------------------------------------------------------
DEFAULT_HQ_HOSTS: list[dict[str, Any]] = [
    {"ip": "180.153.18.170", "port": 7709, "name": "上海电信主站Z1"},
    {"ip": "180.153.18.171", "port": 7709, "name": "上海电信主站Z2"},
    {"ip": "180.153.18.172", "port": 80,   "name": "上海电信主站Z80"},
    {"ip": "202.108.253.130", "port": 7709, "name": "北京电信主站A1"},
    {"ip": "202.108.253.131", "port": 7709, "name": "北京电信主站A2"},
    {"ip": "60.191.117.167", "port": 7709, "name": "杭州电信"},
    {"ip": "218.108.50.178", "port": 7709, "name": "杭州行情主站"},
    {"ip": "115.238.56.198", "port": 7709, "name": "杭州电信2"},
    {"ip": "124.160.88.183", "port": 7709, "name": "杭州电信3"},
    {"ip": "218.108.98.244", "port": 7709, "name": "杭州电信4"},
    {"ip": "218.75.126.9",  "port": 7709, "name": "杭州电信5"},
    {"ip": "119.147.171.206", "port": 7709, "name": "广州行情主站"},
    {"ip": "113.105.73.88",  "port": 7709, "name": "深圳行情主站"},
    {"ip": "119.147.164.60", "port": 7709, "name": "深圳电信"},
    {"ip": "112.95.140.74",  "port": 7709, "name": "深圳电信2"},
    {"ip": "112.95.140.92",  "port": 7709, "name": "深圳电信3"},
    {"ip": "112.95.140.93",  "port": 7709, "name": "深圳电信4"},
    {"ip": "114.80.149.19",  "port": 7709, "name": "上海电信"},
    {"ip": "114.80.149.22",  "port": 7709, "name": "上海电信2"},
    {"ip": "114.80.149.84",  "port": 7709, "name": "上海电信3"},
    {"ip": "114.80.80.222",  "port": 7709, "name": "上海行情主站"},
    {"ip": "123.125.108.23", "port": 7709, "name": "北京联通"},
    {"ip": "123.125.108.24", "port": 7709, "name": "北京联通2"},
    {"ip": "221.194.181.176", "port": 7709, "name": "北京行情主站2"},
    {"ip": "106.120.74.86",  "port": 7709, "name": "北京行情主站1"},
    {"ip": "61.135.142.73",  "port": 7709, "name": "北京联通3"},
    {"ip": "61.135.142.88",  "port": 7709, "name": "北京联通4"},
    {"ip": "117.184.140.156", "port": 7709, "name": "移动行情主站"},
    {"ip": "218.9.148.108",  "port": 7709, "name": "济南电信"},
    {"ip": "61.153.144.179", "port": 7709, "name": "温州电信"},
    {"ip": "61.153.209.138", "port": 7709, "name": "温州电信2"},
    {"ip": "61.153.209.139", "port": 7709, "name": "温州电信3"},
    {"ip": "114.67.61.70",   "port": 7709, "name": "北京电信"},
    {"ip": "121.14.104.70",  "port": 7709, "name": "深圳电信5"},
    {"ip": "121.14.104.72",  "port": 7709, "name": "深圳电信6"},
    {"ip": "60.28.29.69",    "port": 7709, "name": "天津联通"},
]

KLINE_TYPE_DAILY = 9
DAILY_PROBE_SYMBOLS: tuple[tuple[int, str], ...] = ((1, "600000"), (0, "000001"))
UNREACHABLE_LATENCY_MS = 9999.0
NO_SELECTABLE_HOST_ERROR = "pytdx 无法连接任何服务器"
_PROBE_STATUS_ORDER = (
    "active",
    "empty_bars",
    "unparseable",
    "transport_failed",
    "unreachable",
    "unfinished",
)


def load_pytdx_hq_hosts() -> list[tuple[Any, ...]]:
    """Return installed pytdx built-in HQ rows: (name, ip, port)."""
    try:
        from pytdx.config.hosts import hq_hosts
    except Exception:
        tdx_logger.warning("[TdxIPManager] 无法导入 pytdx.config.hosts.hq_hosts")
        return []
    if not hq_hosts:
        return []
    return list(hq_hosts)


def merge_hq_host_lists(
    quote_hosts: list[dict[str, Any]] | None,
    pytdx_hosts: list[tuple[Any, ...]] | None,
) -> list[dict[str, Any]]:
    """Unique ip:port union of Quote dicts and pytdx (name, ip, port) tuples."""
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for host in quote_hosts or []:
        ip = str(host.get("ip", "")).strip()
        if not ip:
            continue
        port = int(host.get("port", 7709))
        key = (ip, port)
        if key in seen:
            continue
        seen.add(key)
        merged.append({
            "ip": ip,
            "port": port,
            "name": str(host.get("name", "") or ""),
        })
    for row in pytdx_hosts or []:
        if row is None or len(row) < 3:
            continue
        name, ip, port = row[0], row[1], row[2]
        ip = str(ip).strip()
        if not ip:
            continue
        port = int(port)
        key = (ip, port)
        if key in seen:
            continue
        seen.add(key)
        merged.append({
            "ip": ip,
            "port": port,
            "name": str(name or ""),
        })
    return merged


def default_hq_hosts() -> list[dict[str, Any]]:
    return merge_hq_host_lists(DEFAULT_HQ_HOSTS, load_pytdx_hq_hosts())


def parse_tdx_bar_datetime(bar: dict) -> datetime | None:
    """解析 pytdx bar 的 datetime 字段。"""
    dt_str = bar.get("datetime", "")
    if dt_str:
        try:
            return datetime.strptime(str(dt_str)[:10], "%Y-%m-%d")
        except ValueError:
            pass
    try:
        return datetime(
            int(bar.get("year", 0)),
            int(bar.get("month", 1)),
            int(bar.get("day", 1)),
        )
    except (ValueError, TypeError):
        return None


def _finite_ohlc_price(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


def is_parseable_daily_bar(bar: Any) -> bool:
    if not isinstance(bar, dict):
        return False
    if parse_tdx_bar_datetime(bar) is None:
        return False
    return all(
        _finite_ohlc_price(bar.get(key)) for key in ("open", "high", "low", "close")
    )


def first_parseable_daily_bars(api: TdxHq_API) -> list | None:
    """Shared liquid-symbol probe: SSE 600000 then SZSE 000001.

    Empty or unparseable on the first symbol tries the second. A transport
    failure does not continue on this socket.
    """
    for market, code in DAILY_PROBE_SYMBOLS:
        try:
            bars = api.get_security_bars(KLINE_TYPE_DAILY, market, code, 0, 1)
        except Exception:
            return None
        if bars is None:
            return None
        if any(is_parseable_daily_bar(bar) for bar in bars):
            return bars
    return None


def close_tdx_socket(api: Any) -> None:
    """Best-effort closer: shutdown then close, each in its own try. Never raises."""
    if api is None:
        return
    client = getattr(api, "client", None)
    if client is None:
        return
    try:
        client.shutdown(socket.SHUT_RDWR)
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


def _host_key(ip: str, port: int) -> str:
    return f"{ip}:{int(port)}"


class _ProbeSocketTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._apis: list[Any] = []

    def register(self, api: Any) -> None:
        with self._lock:
            self._apis.append(api)

    def unregister(self, api: Any) -> None:
        with self._lock:
            try:
                self._apis.remove(api)
            except ValueError:
                pass

    def close_all(self) -> None:
        with self._lock:
            apis = list(self._apis)
        for api in apis:
            close_tdx_socket(api)


# ---------------------------------------------------------------------------
# TdxIPManager: IP 探测 + 故障转移 + 黑名单
# ---------------------------------------------------------------------------
@dataclass
class IPEntry:
    ip: str
    port: int
    name: str = ""
    latency_ms: float = UNREACHABLE_LATENCY_MS
    status: str = "unknown"


class TdxIPManager:
    """通达信行情服务器 IP 管理器

    功能:
      1. 启动时并发探测候选 IP, 按可解析日线延迟排序
      2. 请求失败时将可选 IP 加入黑名单, 自动切换到下一个可选节点
      3. 黑名单 IP 到期后自动解封（仅限最近一轮可选节点）
      4. 支持定期刷新 IP 延迟排名
    """

    def __init__(
        self,
        hosts: list[dict[str, Any]] | None = None,
        blacklist_duration_hours: float = 1.0,
        refresh_interval_hours: float = 24.0,
        probe_timeout: float = 0.7,
        probe_workers: int = 16,
        refresh_deadline_sec: float = 8.0,
    ):
        self._hosts = hosts if hosts is not None else default_hq_hosts()
        self._blacklist_duration = timedelta(hours=blacklist_duration_hours)
        self._refresh_interval = timedelta(hours=refresh_interval_hours)
        self._probe_timeout = float(probe_timeout)
        self._probe_workers = max(1, int(probe_workers))
        self._refresh_deadline_sec = max(0.0, float(refresh_deadline_sec))

        self.ranked_ips: list[IPEntry] = []
        self.blacklist: dict[str, datetime] = {}  # "ip:port" → 解封时间
        self._selectable_keys: set[str] = set()
        self._last_refresh: datetime | None = None
        self._lock = threading.Lock()
        self._refresh_mutex = threading.Lock()
        self.last_refresh_duration_sec: float = 0.0

    # ---- 公开接口 ----

    def refresh(self, force: bool = True) -> None:
        """并发探测候选 IP。force=False 时仅在排名到期后扫描。"""
        with self._refresh_mutex:
            if not force and not self.needs_refresh():
                return
            self._run_scan()

    def get_ip(self) -> Tuple[str, int]:
        """获取当前最佳可选 IP (跳过黑名单)。"""
        now = datetime.now()
        with self._lock:
            self._expire_blacklist_unlocked(now)
            for entry in self.ranked_ips:
                key = _host_key(entry.ip, entry.port)
                if key in self.blacklist:
                    continue
                if entry.status == "active" and key in self._selectable_keys:
                    return (entry.ip, entry.port)

            selectable = [
                entry for entry in self.ranked_ips
                if _host_key(entry.ip, entry.port) in self._selectable_keys
            ]
            if selectable:
                tdx_logger.warning("[TdxIPManager] 所有可选 IP 被封, 仅清空可选节点黑名单")
                for entry in selectable:
                    key = _host_key(entry.ip, entry.port)
                    self.blacklist.pop(key, None)
                    entry.status = "active"
                selectable.sort(key=lambda e: e.latency_ms)
                return (selectable[0].ip, selectable[0].port)

        tdx_logger.error("[TdxIPManager] 无日线可用 IP")
        raise ConnectionError(NO_SELECTABLE_HOST_ERROR)

    def report_failure(self, ip: str, port: int) -> Tuple[str, int]:
        """报告连接失败, 拉黑当前 IP, 返回下一个可用 IP"""
        key = _host_key(ip, port)
        with self._lock:
            self.blacklist[key] = datetime.now() + self._blacklist_duration
            for entry in self.ranked_ips:
                if entry.ip == ip and entry.port == port:
                    entry.status = "blacklisted"
                    break
        tdx_logger.warning(
            f"[TdxIPManager] 拉黑 {key}, 持续 {self._blacklist_duration}"
        )
        return self.get_ip()

    def report_success(self, ip: str, port: int) -> None:
        """报告连接成功"""
        key = _host_key(ip, port)
        with self._lock:
            self.blacklist.pop(key, None)
            if key in self._selectable_keys:
                for entry in self.ranked_ips:
                    if entry.ip == ip and entry.port == port:
                        entry.status = "active"
                        break

    def needs_refresh(self) -> bool:
        """是否需要刷新 IP 排名"""
        with self._lock:
            return self._needs_refresh_unlocked()

    def has_selectable_host(self) -> bool:
        with self._lock:
            return any(
                entry.status == "active"
                and _host_key(entry.ip, entry.port) in self._selectable_keys
                for entry in self.ranked_ips
            )

    def is_host_selectable(self, ip: str, port: int) -> bool:
        now = datetime.now()
        key = _host_key(ip, port)
        with self._lock:
            self._expire_blacklist_unlocked(now)
            if key not in self._selectable_keys:
                return False
            if key in self.blacklist:
                return False
            return True

    # ---- 内部 ----

    def _needs_refresh_unlocked(self) -> bool:
        if self._last_refresh is None:
            return True
        return datetime.now() - self._last_refresh > self._refresh_interval

    def _expire_blacklist_unlocked(self, now: datetime) -> None:
        expired = [key for key, until in self.blacklist.items() if now > until]
        for key in expired:
            del self.blacklist[key]
            if key in self._selectable_keys:
                for entry in self.ranked_ips:
                    if _host_key(entry.ip, entry.port) == key:
                        entry.status = "active"
                        break

    def _run_scan(self) -> None:
        hosts = list(self._hosts)
        t0 = time.monotonic()
        deadline = t0 + self._refresh_deadline_sec
        tdx_logger.info(f"[TdxIPManager] 开始探测 {len(hosts)} 个候选 IP...")

        accepted: dict[tuple[str, int], IPEntry] = {}
        accepted_lock = threading.Lock()
        tracker = _ProbeSocketTracker()
        workers = min(self._probe_workers, max(1, len(hosts))) if hosts else 1
        executor = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="tdx-probe",
        )
        futures = []
        try:
            for host in hosts:
                if time.monotonic() >= deadline:
                    break
                futures.append(
                    executor.submit(
                        self._record_probe,
                        host,
                        deadline,
                        tracker,
                        accepted,
                        accepted_lock,
                    )
                )
            remaining = deadline - time.monotonic()
            if remaining > 0 and futures:
                wait_futures(futures, timeout=remaining)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            tracker.close_all()

        results: list[IPEntry] = []
        for host in hosts:
            ip = str(host["ip"])
            port = int(host.get("port", 7709))
            name = str(host.get("name", "") or "")
            entry = accepted.get((ip, port))
            if entry is None:
                entry = IPEntry(
                    ip=ip,
                    port=port,
                    name=name,
                    latency_ms=UNREACHABLE_LATENCY_MS,
                    status="unfinished",
                )
            results.append(entry)

        results.sort(
            key=lambda e: (0 if e.status == "active" else 1, e.latency_ms)
        )
        selectable_keys = {
            _host_key(entry.ip, entry.port)
            for entry in results
            if entry.status == "active"
        }
        counts = Counter(entry.status for entry in results)
        duration = time.monotonic() - t0
        chosen = next((e for e in results if e.status == "active"), None)

        with self._lock:
            self.ranked_ips = results
            self._selectable_keys = selectable_keys
            self._last_refresh = datetime.now()
            self.last_refresh_duration_sec = duration
            stale_keys = [key for key in self.blacklist if key not in selectable_keys]
            for key in stale_keys:
                self.blacklist.pop(key, None)

        count_text = " ".join(
            f"{name}={counts.get(name, 0)}" for name in _PROBE_STATUS_ORDER
        )
        if chosen is not None:
            tdx_logger.info(
                f"[TdxIPManager] 探测完成: {count_text} "
                f"chosen={chosen.ip}:{chosen.port} "
                f"latency_ms={chosen.latency_ms:.1f} "
                f"duration_sec={duration:.3f}"
            )
        else:
            tdx_logger.info(
                f"[TdxIPManager] 探测完成: {count_text} "
                f"duration_sec={duration:.3f} 无日线可用 IP"
            )

    def _record_probe(
        self,
        host: dict[str, Any],
        deadline: float,
        tracker: _ProbeSocketTracker,
        accepted: dict[tuple[str, int], IPEntry],
        accepted_lock: threading.Lock,
    ) -> None:
        ip = str(host["ip"])
        port = int(host.get("port", 7709))
        try:
            entry = self._probe_host(host, deadline, tracker)
        except Exception:
            entry = IPEntry(
                ip=ip,
                port=port,
                name=str(host.get("name", "") or ""),
                latency_ms=UNREACHABLE_LATENCY_MS,
                status="unreachable",
            )
        finished_at = time.monotonic()
        if finished_at > deadline:
            return
        with accepted_lock:
            accepted[(entry.ip, entry.port)] = entry

    def _probe_host(
        self,
        host: dict[str, Any],
        deadline: float,
        tracker: _ProbeSocketTracker,
    ) -> IPEntry:
        ip = str(host["ip"])
        port = int(host.get("port", 7709))
        name = str(host.get("name", "") or "")
        if time.monotonic() >= deadline:
            return IPEntry(
                ip=ip, port=port, name=name,
                latency_ms=UNREACHABLE_LATENCY_MS, status="unfinished",
            )

        api = TdxHq_API(heartbeat=False, auto_retry=False, raise_exception=True)
        tracker.register(api)
        t0 = time.monotonic()
        saw_unparseable = False
        try:
            try:
                api.connect(ip, port, time_out=self._probe_timeout)
            except Exception:
                return IPEntry(
                    ip=ip, port=port, name=name,
                    latency_ms=UNREACHABLE_LATENCY_MS, status="unreachable",
                )

            for market, code in DAILY_PROBE_SYMBOLS:
                if time.monotonic() >= deadline:
                    return IPEntry(
                        ip=ip, port=port, name=name,
                        latency_ms=UNREACHABLE_LATENCY_MS, status="unfinished",
                    )
                try:
                    bars = api.get_security_bars(
                        KLINE_TYPE_DAILY, market, code, 0, 1
                    )
                except Exception:
                    return IPEntry(
                        ip=ip, port=port, name=name,
                        latency_ms=UNREACHABLE_LATENCY_MS, status="transport_failed",
                    )
                if bars is None:
                    return IPEntry(
                        ip=ip, port=port, name=name,
                        latency_ms=UNREACHABLE_LATENCY_MS, status="transport_failed",
                    )
                if any(is_parseable_daily_bar(bar) for bar in bars):
                    latency_ms = (time.monotonic() - t0) * 1000.0
                    return IPEntry(
                        ip=ip, port=port, name=name,
                        latency_ms=latency_ms, status="active",
                    )
                if bars:
                    saw_unparseable = True

            status = "unparseable" if saw_unparseable else "empty_bars"
            return IPEntry(
                ip=ip, port=port, name=name,
                latency_ms=UNREACHABLE_LATENCY_MS, status=status,
            )
        finally:
            close_tdx_socket(api)
            tracker.unregister(api)


# ---------------------------------------------------------------------------
# TdxConnectionPool: 线程安全连接池
# ---------------------------------------------------------------------------
class TdxConnectionPool:
    """管理多个 TdxHq_API 长连接

    - 线程安全: 每个线程获取独立的连接
    - 故障转移: 连接断开时自动重连到新 IP
    - 懒初始化: 按需创建连接
    """

    def __init__(self, ip_manager: TdxIPManager, pool_size: int = 3,
                 timeout: float = 10.0):
        self._ip_manager = ip_manager
        self._pool_size = pool_size
        self._timeout = timeout
        self._connections: list[tuple[TdxHq_API, str, int]] = []
        self._lock = threading.Lock()
        self._local = threading.local()

    def get_connection(self) -> TdxHq_API:
        """获取一个可用连接 (线程安全)

        如果当前线程已有连接且主机仍可选, 直接返回; 否则从池中分配或创建新连接.
        """
        if self._ip_manager.needs_refresh():
            self._ip_manager.refresh(force=False)

        api = getattr(self._local, "api", None)
        if api is not None:
            ip = getattr(self._local, "ip", None)
            port = getattr(self._local, "port", None)
            if (
                ip is not None
                and port is not None
                and self._ip_manager.is_host_selectable(ip, port)
            ):
                return api
            self._discard_local_connection()

        return self._open_connection()

    def _open_connection(self) -> TdxHq_API:
        ip, port = self._ip_manager.get_ip()
        api = TdxHq_API()
        try:
            api.connect(ip, port, time_out=self._timeout)
            self._store_local(api, ip, port)
            return api
        except Exception as e:
            tdx_logger.error(f"[TdxConnectionPool] 连接 {ip}:{port} 失败: {e}")
            new_ip, new_port = self._ip_manager.report_failure(ip, port)
            api2 = TdxHq_API()
            try:
                api2.connect(new_ip, new_port, time_out=self._timeout)
                self._store_local(api2, new_ip, new_port)
                return api2
            except Exception as e2:
                tdx_logger.error(
                    f"[TdxConnectionPool] 重试 {new_ip}:{new_port} 也失败: {e2}"
                )
                raise ConnectionError(
                    f"{NO_SELECTABLE_HOST_ERROR}: 首选 {ip}:{port}, 重试 {new_ip}:{new_port}"
                ) from e2

    def _store_local(self, api: TdxHq_API, ip: str, port: int) -> None:
        self._local.api = api
        self._local.ip = ip
        self._local.port = port
        with self._lock:
            self._connections.append((api, ip, port))

    def _discard_local_connection(self) -> None:
        old_api = getattr(self._local, "api", None)
        if old_api is not None:
            close_tdx_socket(old_api)
            with self._lock:
                self._connections = [
                    (a, i, p) for a, i, p in self._connections
                    if a is not old_api
                ]
        self._local.api = None
        self._local.ip = None
        self._local.port = None

    def reconnect_current(self, mark_failure: bool = True) -> TdxHq_API:
        """断线重连当前线程的连接
        
        Args:
            mark_failure: 是否将当前 IP 标记为故障（拉黑）。因为有时只是为了怀疑断连而主动重刷连接，并不代表服务器一定宕机。
        """
        old_ip = getattr(self._local, "ip", None)
        old_port = getattr(self._local, "port", None)
        self._discard_local_connection()
        if old_ip and old_port and mark_failure:
            self._ip_manager.report_failure(old_ip, old_port)
        return self.get_connection()

    def close_all(self) -> None:
        """关闭所有连接"""
        with self._lock:
            connections = list(self._connections)
            self._connections.clear()
        for api, ip, port in connections:
            close_tdx_socket(api)
        self._local.api = None
        self._local.ip = None
        self._local.port = None
        tdx_logger.info("[TdxConnectionPool] 所有连接已关闭")


# ---------------------------------------------------------------------------
# 交易所/市场映射
# ---------------------------------------------------------------------------
EXCHANGE_TO_MARKET: dict[str, int] = {"SZSE": 0, "SSE": 1, "BSE": 2}

# instrument_id 后缀 → market
SUFFIX_TO_MARKET: dict[str, int] = {
    ".SZ": 0,
    ".SH": 1,
    ".BJ": 2,
}

# market → 股票代码前缀 (用于从全量品种中过滤)
STOCK_PREFIXES: dict[int, tuple[str, ...]] = {
    0: ("000", "001", "002", "003", "300", "301"),     # 深圳
    1: ("600", "601", "603", "605", "688", "689"),     # 上海
    2: ("43", "83", "87", "920"),                       # 北交所
}

# 常用指数代码
INDEX_CODES: dict[int, tuple[str, ...]] = {
    1: ("999999", "000001", "000300", "000016", "000905", "000852"),
    0: ("399001", "399006", "399300", "399005", "399673"),
}

# K线类型常量见文件前部 KLINE_TYPE_DAILY


def _parse_instrument_id(instrument_id: str) -> Tuple[int, str]:
    """解析品种 ID → (market, code)

    例: '000001.SZ' → (0, '000001'), '600000.SH' → (1, '600000'), '430047.BJ' → (2, '430047')
    """
    parts = instrument_id.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"无法解析 instrument_id: {instrument_id}")
    code, suffix = parts[0], f".{parts[1]}"
    market = SUFFIX_TO_MARKET.get(suffix.upper())
    if market is None:
        raise ValueError(f"未知交易所后缀: {suffix} (instrument_id={instrument_id})")
    return market, code


# ---------------------------------------------------------------------------
# TdxSource: BaseDataSource 实现
# ---------------------------------------------------------------------------
class TdxSource(BaseDataSource):
    """通达信行情数据源

    特点:
      - 使用 socket 直连, 比 HTTP API 快 20~30 倍
      - 同步 API, 通过 run_in_executor 避免阻塞事件循环
      - 支持 SSE/SZSE/BSE (market=0/1/2)
      - vol 单位: 手 → 股 (×100)
    """

    def __init__(
        self,
        name: str,
        rate_limit_config: RateLimitConfig,
        pool_size: int = 3,
        connection_timeout: float = 10.0,
        ip_refresh_hours: float = 24.0,
        batch_size: int = 800,
        host_probe_workers: int = 16,
        host_probe_timeout_sec: float = 0.7,
        host_refresh_deadline_sec: float = 8.0,
    ):
        super().__init__(name, rate_limit_config)
        self.supported_exchanges = ['SSE', 'SZSE', 'BSE']  # pytdx 支持全部 A 股交易所
        self._pool_size = pool_size
        self._connection_timeout = connection_timeout
        self._batch_size = batch_size

        self.ip_manager = TdxIPManager(
            refresh_interval_hours=ip_refresh_hours,
            probe_timeout=host_probe_timeout_sec,
            probe_workers=host_probe_workers,
            refresh_deadline_sec=host_refresh_deadline_sec,
        )
        self.pool: TdxConnectionPool | None = None

        # 线程池: 限制并发数 = 连接池大小
        self._executor = ThreadPoolExecutor(
            max_workers=pool_size, thread_name_prefix="pytdx"
        )

        # 延迟导入, 避免循环依赖
        self.factor_engine: Any = None

    async def _initialize_impl(self) -> None:
        """初始化: 探测 IP + 创建连接池"""
        loop = asyncio.get_event_loop()

        # IP 探测 (在线程池中执行, 避免阻塞)
        tdx_logger.info(f"[{self.name}] 开始 IP 探测...")
        await loop.run_in_executor(self._executor, self.ip_manager.refresh)
        if not self.ip_manager.has_selectable_host():
            raise ConnectionError(NO_SELECTABLE_HOST_ERROR)

        # 创建连接池
        self.pool = TdxConnectionPool(
            self.ip_manager,
            pool_size=self._pool_size,
            timeout=self._connection_timeout,
        )

        # 验证连接: 获取一条可解析日线
        try:
            test_result = await loop.run_in_executor(
                self._executor,
                self._sync_test_connection,
            )
            if test_result:
                tdx_logger.info(f"[{self.name}] 连接验证成功")
            else:
                raise ConnectionError(NO_SELECTABLE_HOST_ERROR)
        except Exception as e:
            tdx_logger.error(f"[{self.name}] 连接验证失败: {e}")
            raise

        # 初始化因子引擎 (延迟导入)
        try:
            from .tdx_factor_engine import TdxFactorEngine
            self.factor_engine = TdxFactorEngine()
            tdx_logger.info(f"[{self.name}] 因子引擎已初始化")
        except ImportError:
            tdx_logger.warning(f"[{self.name}] 因子引擎模块未找到, 跳过")

    def _sync_test_connection(self) -> list | None:
        """同步连接测试: 使用与刷新相同的可解析日线探针。"""
        api = self.pool.get_connection()
        return first_parseable_daily_bars(api)

    @staticmethod
    def _is_connection_healthy(api: TdxHq_API) -> bool:
        """Use liquid A-share symbols to distinguish an empty series from a dead socket."""
        return first_parseable_daily_bars(api) is not None

    # ================================================================
    # BaseDataSource 抽象方法实现
    # ================================================================

    async def get_instrument_list(
        self, exchange: str = None, **kwargs
    ) -> List[Dict[str, Any]]:
        """获取交易品种列表 (辅助用途, 缺少 type/listed_date 等字段)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get_instrument_list,
            exchange,
        )

    def _sync_get_instrument_list(self, exchange: str = None) -> list[dict]:
        """同步获取品种列表"""
        api = self.pool.get_connection()
        results: list[dict] = []

        markets = (
            [EXCHANGE_TO_MARKET[exchange.upper()]]
            if exchange and exchange.upper() in EXCHANGE_TO_MARKET
            else list(EXCHANGE_TO_MARKET.values())
        )

        for market in markets:
            prefixes = STOCK_PREFIXES.get(market, ())
            count = api.get_security_count(market)
            if not count:
                continue

            for start in range(0, count, 1000):
                batch = api.get_security_list(market, start)
                if not batch:
                    break
                for item in batch:
                    code = item.get("code", "")
                    if any(code.startswith(p) for p in prefixes):
                        suffix = {0: ".SZ", 1: ".SH", 2: ".BJ"}[market]
                        results.append({
                            "instrument_id": f"{code}{suffix}",
                            "symbol": code,
                            "name": item.get("name", ""),
                            "exchange": {0: "SZSE", 1: "SSE", 2: "BSE"}[market],
                            "type": "stock",
                            "source": "pytdx",
                        })

        tdx_logger.info(f"[{self.name}] 获取到 {len(results)} 个品种")
        return results

    async def get_daily_data(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        instrument_type: str = "stock",
        source_symbol: str = "",
    ) -> List[Dict[str, Any]]:
        """获取日线数据 (run_in_executor 包装)"""
        await self.rate_limiter.acquire()
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get_daily_data,
            instrument_id, symbol, start_date, end_date, instrument_type,
        )

    def _sync_get_daily_data(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        instrument_type: str = "stock",
    ) -> list[dict]:
        """同步获取日线数据 (在线程池中执行)"""
        try:
            market, code = _parse_instrument_id(instrument_id)
        except ValueError as e:
            tdx_logger.warning(f"[{self.name}] 跳过无法解析的 ID: {instrument_id}: {e}")
            return []

        self.last_fetch_diagnostic: Dict[str, Any] = {}
        api = self.pool.get_connection()
        all_bars: list[dict] = []

        # pytdx 的 offset 是从最新日期往前数的偏移量
        # 我们需要从 offset=0 开始往前取, 直到超出 start_date 或无数据
        offset = 0
        max_iterations = 100  # 安全阀: 最多取 100 × batch_size 条
        _reconnected_this_call = False  # 防止同一次调用内无限重连

        for _ in range(max_iterations):
            try:
                bars = api.get_security_bars(
                    KLINE_TYPE_DAILY, market, code, offset, self._batch_size
                )
            except Exception as e:
                tdx_logger.warning(
                    f"[{self.name}] get_security_bars 失败 "
                    f"{instrument_id} offset={offset}: {e}"
                )
                # 尝试重连
                try:
                    api = self.pool.reconnect_current()
                    _reconnected_this_call = True
                    bars = api.get_security_bars(
                        KLINE_TYPE_DAILY, market, code, offset, self._batch_size
                    )
                except Exception as e2:
                    tdx_logger.error(
                        f"[{self.name}] 重连后仍失败 {instrument_id}: {e2}"
                    )
                    self.last_fetch_diagnostic = {
                        'connection_unhealthy': True,
                        'reason': 'reconnect_failed',
                    }
                    break

            if not bars:
                # pytdx 对退市股票也可能合法返回空，先用流动性基准判断连接状态。
                if offset == 0 and not _reconnected_this_call:
                    if self._is_connection_healthy(api):
                        tdx_logger.info(
                            f"[{self.name}] 目标日线为空但连接健康, "
                            f"可能不提供退市/历史行情, 交由备源处理 "
                            f"({instrument_id})"
                        )
                        self.last_fetch_diagnostic = {
                            'connection_unhealthy': False,
                            'reason': 'empty_on_healthy_connection',
                        }
                        break

                    tdx_logger.warning(
                        f"[{self.name}] 目标日线及连接健康探针均为空, "
                        f"切换 TDX 节点重试... ({instrument_id})"
                    )
                    try:
                        api = self.pool.reconnect_current(mark_failure=True)
                        _reconnected_this_call = True
                        bars = api.get_security_bars(
                            KLINE_TYPE_DAILY, market, code, offset, self._batch_size
                        )
                        if bars:
                            tdx_logger.info(
                                f"[{self.name}] 切换节点后成功获取数据 ({instrument_id})"
                            )
                        else:
                            probe_healthy = self._is_connection_healthy(api)
                            log = (
                                tdx_logger.info
                                if probe_healthy
                                else tdx_logger.error
                            )
                            log(
                                f"[{self.name}] 切换节点后目标日线仍为空; "
                                f"连接健康={probe_healthy}, 交由备源处理 "
                                f"({instrument_id})"
                            )
                            self.last_fetch_diagnostic = {
                                'connection_unhealthy': not probe_healthy,
                                'reason': (
                                    'empty_on_healthy_connection'
                                    if probe_healthy
                                    else 'empty_after_reconnect'
                                ),
                            }
                            break
                    except Exception as e:
                        tdx_logger.error(
                            f"[{self.name}] 切换 TDX 节点失败 {instrument_id}: {e}"
                        )
                        self.last_fetch_diagnostic = {
                            'connection_unhealthy': True,
                            'reason': 'reconnect_failed',
                        }
                        break
                else:
                    break

            all_bars.extend(bars)
            offset += len(bars)

            # 检查是否已超出 start_date
            earliest_bar = bars[-1]  # bars 按时间降序
            earliest_dt = self._parse_bar_datetime(earliest_bar)
            if earliest_dt and earliest_dt.date() < start_date.date():
                break

            # 如果返回的数据少于请求数, 说明已经没有更多数据
            if len(bars) < self._batch_size:
                break

        # 转换格式 + 日期过滤
        quotes = self._convert_bars_to_quotes(
            all_bars, instrument_id, start_date, end_date
        )
        return quotes

    async def get_daily_data_batch(
        self,
        instruments: List[Dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
        batch_size: int = 50,
    ) -> List[Dict[str, Any]]:
        """批量获取日线数据 (顺序执行, 避免 asyncio.gather 同步冲突)"""
        all_data: list[dict] = []
        total = len(instruments)

        for i, inst in enumerate(instruments):
            try:
                data = await self.get_daily_data(
                    inst["instrument_id"],
                    inst.get("symbol", ""),
                    start_date,
                    end_date,
                    instrument_type=inst.get("type", "stock"),
                    source_symbol=inst.get("source_symbol", ""),
                )
                if data:
                    all_data.extend(data)
            except Exception as e:
                tdx_logger.error(
                    f"[{self.name}] batch 获取失败 {inst.get('instrument_id')}: {e}"
                )

            # 每 200 只 yield 一次, 避免长时间阻塞事件循环
            if i > 0 and i % 200 == 0:
                tdx_logger.info(
                    f"[{self.name}] batch 进度: {i}/{total}, "
                    f"已获取 {len(all_data)} 条"
                )
                await asyncio.sleep(0)

        tdx_logger.info(
            f"[{self.name}] batch 完成: {total} 个品种, {len(all_data)} 条数据"
        )
        return all_data

    async def get_latest_daily_data(
        self, instrument_id: str, symbol: str
    ) -> Dict[str, Any]:
        """获取最新一条日线数据"""
        await self.rate_limiter.acquire()
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get_latest_daily_data,
            instrument_id, symbol,
        )

    def _sync_get_latest_daily_data(
        self, instrument_id: str, symbol: str
    ) -> dict:
        """同步获取最新日线"""
        try:
            market, code = _parse_instrument_id(instrument_id)
        except ValueError:
            return {}

        api = self.pool.get_connection()
        bars = api.get_security_bars(KLINE_TYPE_DAILY, market, code, 0, 2)
        if not bars:
            return {}

        quotes = self._convert_bars_to_quotes(
            bars, instrument_id, datetime.min, datetime.max
        )
        return quotes[0] if quotes else {}

    async def get_adjustment_factors(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        pre_close_overrides: Optional[Dict[Any, float]] = None,
    ) -> List[Dict[str, Any]]:
        """获取自研复权因子 (通过 TdxFactorEngine 计算)"""
        if not self.factor_engine:
            return []

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get_adjustment_factors,
            instrument_id, symbol, start_date, end_date, pre_close_overrides,
        )

    def _sync_get_adjustment_factors(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        pre_close_overrides: Optional[Dict[Any, float]] = None,
    ) -> list[dict]:
        """同步计算自研因子"""
        try:
            market, code = _parse_instrument_id(instrument_id)
        except ValueError:
            return []

        api = self.pool.get_connection()
        return self.factor_engine.compute_factors(
            api,
            market,
            code,
            instrument_id,
            start_date,
            end_date,
            pre_close_overrides=pre_close_overrides,
        )

    async def get_xdxr_events(
        self, instrument_id: str
    ) -> list[dict]:
        """获取原始 XDXR 除权除息事件"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get_xdxr_events,
            instrument_id,
        )

    def _sync_get_xdxr_events(self, instrument_id: str) -> list[dict]:
        """同步获取 XDXR"""
        try:
            market, code = _parse_instrument_id(instrument_id)
        except ValueError:
            return []

        api = self.pool.get_connection()
        reconnected = False
        try:
            xdxr = api.get_xdxr_info(market, code)
        except Exception as exc:
            tdx_logger.warning(
                f"[{self.name}] XDXR 请求失败, 尝试重连 ({instrument_id}): {exc}"
            )
            api = self.pool.reconnect_current()
            reconnected = True
            xdxr = api.get_xdxr_info(market, code)

        if not xdxr:
            try:
                connection_probe = api.get_security_bars(
                    KLINE_TYPE_DAILY, market, code, 0, 1
                )
            except Exception:
                connection_probe = None
            if not connection_probe and not reconnected:
                tdx_logger.warning(
                    f"[{self.name}] XDXR 与行情探针均为空, 怀疑连接已断开, "
                    f"尝试重连... ({instrument_id})"
                )
                api = self.pool.reconnect_current(mark_failure=False)
                reconnected = True
                xdxr = api.get_xdxr_info(market, code)
                if xdxr:
                    tdx_logger.info(
                        f"[{self.name}] 重连后成功获取 XDXR ({instrument_id})"
                    )
        if not xdxr:
            return []

        # 只返回除权除息事件 (category=1)
        return [
            {
                "date": datetime(
                    int(x.get("year", 0)),
                    int(x.get("month", 1)),
                    int(x.get("day", 1)),
                ),
                "category": x.get("category"),
                "fenhong": x.get("fenhong", 0.0),
                "songzhuangu": x.get("songzhuangu", 0.0),
                "peigu": x.get("peigu", 0.0),
                "peigujia": x.get("peigujia", 0.0),
                "suogu": x.get("suogu", 0.0),
                "instrument_id": instrument_id,
            }
            for x in xdxr
            if x.get("category") == 1
        ]

    async def health_check(self) -> bool:
        """健康检查: 能否获取到数据"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self._executor, self._sync_test_connection
            )
            return result is not None and len(result) > 0
        except Exception as e:
            tdx_logger.error(f"[{self.name}] 健康检查失败: {e}")
            return False

    async def _reconnect(self) -> bool:
        """自动修复: 重新探测 IP + 重建连接"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self._executor, self.ip_manager.refresh)
            if not self.ip_manager.has_selectable_host():
                return False
            if self.pool:
                self.pool.close_all()
            self.pool = TdxConnectionPool(
                self.ip_manager,
                pool_size=self._pool_size,
                timeout=self._connection_timeout,
            )
            # 验证
            return await self.health_check()
        except Exception as e:
            tdx_logger.error(f"[{self.name}] 重连失败: {e}")
            return False

    async def close(self) -> None:
        """关闭所有连接和线程池"""
        if self.pool:
            self.pool.close_all()
        self._executor.shutdown(wait=False)
        tdx_logger.info(f"[{self.name}] 数据源已关闭")

    # ================================================================
    # 内部工具方法
    # ================================================================

    @staticmethod
    def _parse_bar_datetime(bar: dict) -> datetime | None:
        """解析 pytdx bar 的 datetime 字段"""
        return parse_tdx_bar_datetime(bar)

    def _convert_bars_to_quotes(
        self,
        bars: list[dict],
        instrument_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """将 pytdx bar 列表转换为标准 quote dict 列表

        关键转换:
          - vol(手) × 100 → volume(股)
          - amount(元) → 直接使用
          - 追加 pre_close/change/pct_change 推算
        """
        from utils.date_utils import get_shanghai_time

        # 去重 + 按日期排序 (pytdx 可能返回重复)
        seen_dates: set[str] = set()
        unique_bars: list[dict] = []
        for bar in bars:
            dt = self._parse_bar_datetime(bar)
            if dt is None:
                continue
            date_key = dt.strftime("%Y-%m-%d")
            if date_key in seen_dates:
                continue
            seen_dates.add(date_key)
            unique_bars.append((dt, bar))

        # 按日期升序排序
        unique_bars.sort(key=lambda x: x[0])

        quotes: list[dict] = []
        prev_close: float = 0.0

        for dt, bar in unique_bars:
            # 日期过滤
            if dt.date() < start_date.date() or dt.date() > end_date.date():
                prev_close = float(bar.get("close", 0))
                continue

            open_price = float(bar.get("open", 0))
            high_price = float(bar.get("high", 0))
            low_price = float(bar.get("low", 0))
            close_price = float(bar.get("close", 0))
            vol_shou = float(bar.get("vol", 0))
            amount = float(bar.get("amount", 0))

            # ★ vol(手) × 100 → volume(股)
            volume = int(vol_shou * 100)

            # 推算 change / pct_change
            change = round(close_price - prev_close, 4) if prev_close > 0 else 0.0
            pct_change = (
                round(change / prev_close * 100, 4) if prev_close > 0 else 0.0
            )

            # 停牌判断: 成交量为 0
            tradestatus = 1 if volume > 0 else 0

            quote = {
                "time": dt,
                "instrument_id": instrument_id,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "amount": amount,
                "turnover": 0.0,  # pytdx 不提供换手率
                "pre_close": prev_close if prev_close > 0 else None,
                "change": change,
                "pct_change": pct_change,
                "tradestatus": tradestatus,
                "factor": 1.0,  # 非复权原始数据
                "adjustment_type": "none",
                "source": "pytdx",
                "created_at": get_shanghai_time(),
            }
            quotes.append(quote)

            prev_close = close_price

        return quotes
