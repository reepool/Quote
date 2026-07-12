"""无风险利率序列采集 (REQ-13)。

默认序列: china_treasury_10y —— 中国 10 年期国债到期收益率 (percent_annual)。
数据源: akshare.bond_zh_us_rate (含中国国债收益率10年列)。

设计:
- fetch 与 sync 分离, 便于单测 monkeypatch fetch。
- 采集失败 (网络/源漂移) 时降级返回空, 不抛出, 不阻塞消费端只读查询。
- 消费端只读; 本模块是唯一写入 risk_free_rate_* 表的入口。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

# 注: 不用 utils.db_logger ("Database" 模块日志器) —— config/01_log.json 将其
# 显式设为 WARNING(抑制高频 DB 层日志), info() 会被静默丢弃。本模块用独立的
# logging.getLogger(__name__), 与 research/fx_market_data.py 的惯例一致, 保证
# 中长程采集的阶段性进度日志真正可见。
logger = logging.getLogger(__name__)

_PROGRESS_LOG_EVERY = 200
_WRITE_CHUNK_SIZE = 500

CHINA_TREASURY_10Y = {
    "series_id": "china_treasury_10y",
    "name": "中国10年期国债到期收益率",
    "rate_type": "china_treasury_yield",
    "tenor": "10Y",
    "currency": "CNY",
    "unit": "percent_annual",
    "frequency": "daily",
    "timezone": "Asia/Shanghai",
    "source_profile": "akshare.bond_zh_us_rate",
    "source": "akshare",
    "source_mode": "shadow",
}

_DATE_COLUMNS = ("日期", "date")
_CN10Y_COLUMNS = ("中国国债收益率10年", "中国国债收益率10年%")


def fetch_china_treasury_10y() -> List[Dict[str, Any]]:
    """拉取中国 10Y 国债收益率序列; 失败返回空列表。"""
    started = time.monotonic()
    logger.info("[RiskFreeRate] fetch_china_treasury_10y: requesting akshare.bond_zh_us_rate() ...")
    try:
        import akshare as ak  # type: ignore

        df = ak.bond_zh_us_rate()
    except Exception as exc:  # pragma: no cover - 网络/源不可用
        logger.warning("fetch_china_treasury_10y failed: %s", exc)
        return []

    if df is None or getattr(df, "empty", True):
        logger.warning("fetch_china_treasury_10y: source returned empty frame")
        return []

    date_col = next((c for c in _DATE_COLUMNS if c in df.columns), None)
    value_col = next((c for c in _CN10Y_COLUMNS if c in df.columns), None)
    if date_col is None or value_col is None:
        logger.warning(
            "bond_zh_us_rate columns unexpected: %s", list(df.columns)
        )
        return []

    total_rows = len(df)
    logger.info(
        "[RiskFreeRate] fetch_china_treasury_10y: received %s rows in %.1fs, parsing ...",
        total_rows, time.monotonic() - started,
    )

    observations: List[Dict[str, Any]] = []
    for index, (_, row) in enumerate(df.iterrows(), start=1):
        raw_date = row.get(date_col)
        raw_value = row.get(value_col)
        if raw_date is not None and raw_value is not None:
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                value = float("nan")
            if value == value:  # skip NaN (early history has gaps)
                observations.append(
                    {"observation_date": str(raw_date)[:10], "value": value}
                )
        if index % _PROGRESS_LOG_EVERY == 0 or index == total_rows:
            logger.info(
                "[RiskFreeRate] fetch_china_treasury_10y: parsed %s/%s rows (%s valid so far)",
                index, total_rows, len(observations),
            )

    logger.info(
        "[RiskFreeRate] fetch_china_treasury_10y: done, %s valid observations in %.1fs total",
        len(observations), time.monotonic() - started,
    )
    return observations


class RiskFreeRateSyncService:
    """把无风险利率序列写入 research 存储。"""

    def __init__(
        self,
        storage: Any,
        *,
        fetcher: Callable[[], List[Dict[str, Any]]] = fetch_china_treasury_10y,
        series_meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._storage = storage
        self._fetcher = fetcher
        self._series_meta = dict(series_meta or CHINA_TREASURY_10Y)

    def sync(self, *, data_as_of: Optional[str] = None) -> Dict[str, Any]:
        series_id = self._series_meta["series_id"]
        started = time.monotonic()
        logger.info("[RiskFreeRate] sync start series_id=%s", series_id)

        meta = dict(self._series_meta)
        if data_as_of:
            meta["data_as_of"] = data_as_of
        self._storage.upsert_risk_free_rate_series(meta)

        observations = self._fetcher()
        for obs in observations:
            obs.setdefault("source", meta.get("source"))
            obs.setdefault("source_mode", meta.get("source_mode"))
            if data_as_of:
                obs.setdefault("data_as_of", data_as_of)

        logger.info(
            "[RiskFreeRate] sync series_id=%s fetched=%s, writing ...",
            series_id, len(observations),
        )
        written = 0
        total = len(observations)
        for start in range(0, total, _WRITE_CHUNK_SIZE):
            chunk = observations[start:start + _WRITE_CHUNK_SIZE]
            written += self._storage.upsert_risk_free_rate_observations(series_id, chunk)
            logger.info(
                "[RiskFreeRate] sync series_id=%s write progress %s/%s",
                series_id, written, total,
            )

        elapsed = time.monotonic() - started
        logger.info(
            "[RiskFreeRate] sync done series_id=%s fetched=%s written=%s elapsed=%.1fs",
            series_id, total, written, elapsed,
        )

        sorted_dates = sorted(obs["observation_date"] for obs in observations) if observations else []
        latest_obs = max(observations, key=lambda o: o["observation_date"]) if observations else None
        return {
            "series_id": series_id,
            "fetched": total,
            "written": written,
            "status": "ok" if observations else "empty",
            "elapsed_seconds": round(elapsed, 1),
            "earliest_date": sorted_dates[0] if sorted_dates else None,
            "latest_date": sorted_dates[-1] if sorted_dates else None,
            "latest_value": latest_obs.get("value") if latest_obs else None,
        }
