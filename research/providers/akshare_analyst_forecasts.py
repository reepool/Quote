"""
AkShare-backed analyst forecast provider.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.date_utils import get_shanghai_time

from .akshare_support import load_akshare
from .base import AnalystForecastSnapshot, BaseAnalystForecastProvider


class AkshareAnalystForecastProvider(BaseAnalystForecastProvider):
    """Fetch analyst forecast summaries through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    _symbol_aliases = ("代码", "股票代码", "证券代码", "symbol", "SYMBOL")
    _name_aliases = ("名称", "股票简称", "证券简称", "name", "NAME")
    _date_aliases = ("日期", "更新时间", "发布日期", "截止日期", "报告日期")
    _rating_aliases = ("投资评级", "综合评级", "评级", "最新评级")
    _report_count_aliases = ("研报数", "报告数")
    _institution_count_aliases = ("机构数", "评级机构数", "覆盖机构数")
    _buy_aliases = ("买入",)
    _overweight_aliases = ("增持", "推荐")
    _neutral_aliases = ("中性", "持有")
    _underperform_aliases = ("减持", "回避")
    _sell_aliases = ("卖出",)

    async def fetch_analyst_forecasts(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[AnalystForecastSnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]

        if not target_instruments:
            return []

        return await asyncio.to_thread(
            self._fetch_analyst_forecasts_sync,
            target_instruments,
            mode,
        )

    def _fetch_analyst_forecasts_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        mode: str,
    ) -> List[AnalystForecastSnapshot]:
        akshare_module = self._akshare(mode)
        frame = akshare_module.stock_profit_forecast_em(symbol="")
        if frame is None or frame.empty:
            return []

        normalized_columns = {column: str(column).strip() for column in frame.columns}
        frame = frame.rename(columns=normalized_columns)
        symbol_column = self._find_column(frame.columns, self._symbol_aliases)
        if symbol_column is None:
            return []

        instrument_map = {
            self._normalize_symbol(instrument.get("symbol")): instrument
            for instrument in target_instruments
            if self._normalize_symbol(instrument.get("symbol"))
        }
        snapshots: List[AnalystForecastSnapshot] = []
        for _, row in frame.iterrows():
            row_dict = {str(key).strip(): value for key, value in row.items()}
            symbol = self._normalize_symbol(row_dict.get(symbol_column))
            instrument = instrument_map.get(symbol)
            if instrument is None:
                continue

            year_metrics = self._extract_year_metrics(row_dict)
            years = sorted(year_metrics.keys())
            fy1 = years[0] if years else None
            fy2 = years[1] if len(years) > 1 else None
            as_of_date = self._pick_date(row_dict) or get_shanghai_time().date().isoformat()

            normalized = {
                "rating_summary": self._pick_first_text(row_dict, self._rating_aliases),
                "report_count": self._pick_metric_by_alias(row_dict, self._report_count_aliases, kind="int"),
                "institution_count": self._pick_metric_by_alias(
                    row_dict,
                    self._institution_count_aliases,
                    kind="int",
                ),
                "buy_count": self._pick_metric_by_alias(row_dict, self._buy_aliases, kind="int"),
                "overweight_count": self._pick_metric_by_alias(
                    row_dict,
                    self._overweight_aliases,
                    kind="int",
                ),
                "neutral_count": self._pick_metric_by_alias(
                    row_dict,
                    self._neutral_aliases,
                    kind="int",
                ),
                "underperform_count": self._pick_metric_by_alias(
                    row_dict,
                    self._underperform_aliases,
                    kind="int",
                ),
                "sell_count": self._pick_metric_by_alias(row_dict, self._sell_aliases, kind="int"),
                "eps_fy1": year_metrics.get(fy1, {}).get("eps"),
                "eps_fy2": year_metrics.get(fy2, {}).get("eps"),
                "net_profit_fy1": year_metrics.get(fy1, {}).get("net_profit"),
                "net_profit_fy2": year_metrics.get(fy2, {}).get("net_profit"),
                "pe_fy1": year_metrics.get(fy1, {}).get("pe"),
                "pe_fy2": year_metrics.get(fy2, {}).get("pe"),
                "forward_years": years,
            }
            if normalized["institution_count"] is None:
                normalized["institution_count"] = normalized["report_count"]

            snapshots.append(
                AnalystForecastSnapshot(
                    instrument_id=instrument.get("instrument_id", ""),
                    symbol=instrument.get("symbol", ""),
                    exchange=instrument.get("exchange", ""),
                    as_of_date=as_of_date,
                    rating_summary=normalized["rating_summary"],
                    report_count=normalized["report_count"],
                    institution_count=normalized["institution_count"],
                    buy_count=normalized["buy_count"],
                    overweight_count=normalized["overweight_count"],
                    neutral_count=normalized["neutral_count"],
                    underperform_count=normalized["underperform_count"],
                    sell_count=normalized["sell_count"],
                    eps_fy1=normalized["eps_fy1"],
                    eps_fy2=normalized["eps_fy2"],
                    net_profit_fy1=normalized["net_profit_fy1"],
                    net_profit_fy2=normalized["net_profit_fy2"],
                    pe_fy1=normalized["pe_fy1"],
                    pe_fy2=normalized["pe_fy2"],
                    source=self.source_name,
                    source_mode=mode,
                    forecast_json={
                        "normalized": normalized,
                        "source_reported": self._compact_dict(row_dict),
                    },
                    raw_payload=self._compact_dict(row_dict),
                )
            )

        return snapshots

    @staticmethod
    def _akshare(mode: str = "direct"):
        return load_akshare(mode)

    @staticmethod
    def _find_column(columns: Any, aliases: tuple[str, ...]) -> Optional[str]:
        normalized = {str(column).strip().lower(): str(column).strip() for column in columns}
        for alias in aliases:
            match = normalized.get(alias.lower())
            if match:
                return match
        return None

    @classmethod
    def _pick_date(cls, row: Dict[str, Any]) -> Optional[str]:
        for alias in cls._date_aliases:
            value = row.get(alias)
            text = cls._format_date(value)
            if text:
                return text
        return None

    @classmethod
    def _pick_first_text(
        cls,
        row: Dict[str, Any],
        aliases: tuple[str, ...],
    ) -> Optional[str]:
        for alias in aliases:
            for key, value in row.items():
                if alias in str(key):
                    text = cls._to_text(value)
                    if text:
                        return text
        return None

    @classmethod
    def _pick_metric_by_alias(
        cls,
        row: Dict[str, Any],
        aliases: tuple[str, ...],
        *,
        kind: str,
    ) -> Optional[float | int]:
        for alias in aliases:
            for key, value in row.items():
                if alias in str(key):
                    if kind == "int":
                        parsed = cls._to_int(value)
                    else:
                        parsed = cls._to_float(value)
                    if parsed is not None:
                        return parsed
        return None

    @classmethod
    def _extract_year_metrics(cls, row: Dict[str, Any]) -> Dict[int, Dict[str, float]]:
        year_metrics: Dict[int, Dict[str, float]] = {}
        for key, value in row.items():
            column = str(key)
            year_match = re.search(r"(20\\d{2})", column)
            if not year_match:
                continue

            year = int(year_match.group(1))
            metric = None
            lower_name = column.lower()
            if "每股收益" in column or "eps" in lower_name:
                metric = "eps"
            elif "净利润" in column:
                metric = "net_profit"
            elif "市盈率" in column or "pe" in lower_name:
                metric = "pe"

            if metric is None:
                continue

            parsed = cls._to_float(value)
            if parsed is None:
                continue

            year_metrics.setdefault(year, {})[metric] = parsed
        return year_metrics

    @staticmethod
    def _normalize_symbol(value: Any) -> Optional[str]:
        if value in {None, ""}:
            return None
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        if not digits:
            return None
        return digits.zfill(6)

    @staticmethod
    def _to_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if text in {"", "nan", "None", "--"}:
            return None
        return text

    @classmethod
    def _format_date(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.date().isoformat()
        text = cls._to_text(value)
        if text is None:
            return None
        match = re.search(r"(20\\d{2})[-/]?(\\d{2})[-/]?(\\d{2})", text)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        return None

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).replace(",", "").replace("%", "").strip()
        if text in {"", "nan", "None", "--"}:
            return None
        try:
            return float(text)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _to_int(cls, value: Any) -> Optional[int]:
        parsed = cls._to_float(value)
        if parsed is None:
            return None
        return int(parsed)

    @staticmethod
    def _compact_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        compact: Dict[str, Any] = {}
        for key, value in row.items():
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            compact[str(key)] = value
        return compact
