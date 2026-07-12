"""无风险利率序列采集 (REQ-13)。

默认序列: china_treasury_10y —— 中国 10 年期国债到期收益率 (percent_annual)。
数据源: akshare.bond_zh_us_rate (含中国国债收益率10年列)。

设计:
- fetch 与 sync 分离, 便于单测 monkeypatch fetch。
- 采集失败 (网络/源漂移) 时降级返回空, 不抛出, 不阻塞消费端只读查询。
- 消费端只读; 本模块是唯一写入 risk_free_rate_* 表的入口。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from utils import db_logger

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
    try:
        import akshare as ak  # type: ignore

        df = ak.bond_zh_us_rate()
    except Exception as exc:  # pragma: no cover - 网络/源不可用
        db_logger.warning("fetch_china_treasury_10y failed: %s", exc)
        return []

    if df is None or getattr(df, "empty", True):
        return []

    date_col = next((c for c in _DATE_COLUMNS if c in df.columns), None)
    value_col = next((c for c in _CN10Y_COLUMNS if c in df.columns), None)
    if date_col is None or value_col is None:
        db_logger.warning(
            "bond_zh_us_rate columns unexpected: %s", list(df.columns)
        )
        return []

    observations: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        raw_date = row.get(date_col)
        raw_value = row.get(value_col)
        if raw_date is None or raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        observations.append(
            {"observation_date": str(raw_date)[:10], "value": value}
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
        written = self._storage.upsert_risk_free_rate_observations(
            series_id, observations
        )
        return {
            "series_id": series_id,
            "fetched": len(observations),
            "written": written,
            "status": "ok" if observations else "empty",
        }
