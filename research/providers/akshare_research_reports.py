"""
AkShare-backed research report metadata provider.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.date_utils import get_shanghai_time

from .akshare_support import load_akshare
from .base import BaseResearchReportProvider, ResearchReportSnapshot


class AkshareResearchReportProvider(BaseResearchReportProvider):
    """Fetch per-instrument research report metadata through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    def __init__(self, *, max_reports_per_instrument: int = 20):
        self.max_reports_per_instrument = max(1, max_reports_per_instrument)

    async def fetch_research_reports(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[ResearchReportSnapshot]:
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
            self._fetch_research_reports_sync,
            target_instruments,
            mode,
        )

    def _fetch_research_reports_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        mode: str,
    ) -> List[ResearchReportSnapshot]:
        akshare_module = self._akshare(mode)
        snapshots: List[ResearchReportSnapshot] = []
        seen_ids: set[str] = set()

        for instrument in target_instruments:
            symbol = instrument.get("symbol", "")
            if not symbol:
                continue

            try:
                frame = akshare_module.stock_research_report_em(symbol=symbol)
            except Exception:
                continue

            if frame is None or frame.empty:
                continue

            frame = frame.rename(columns={column: str(column).strip() for column in frame.columns})
            for _, row in frame.head(self.max_reports_per_instrument).iterrows():
                row_dict = {str(key).strip(): value for key, value in row.items()}
                publish_date = self._pick_date(row_dict) or get_shanghai_time().date().isoformat()
                report_title = (
                    self._pick_first_text(
                        row_dict,
                        ("报告名称", "报告标题", "研报标题", "标题"),
                    )
                    or f"{symbol} research report"
                )
                institution_name = self._pick_first_text(row_dict, ("机构", "机构名称"))
                analyst_name = self._pick_first_text(row_dict, ("研究员", "分析师", "作者"))
                rating = self._pick_first_text(row_dict, ("东财评级", "评级", "最新评级"))
                rating_change = self._pick_first_text(row_dict, ("评级变动", "评级变化"))
                target_price = self._pick_first_float(row_dict, ("目标价",))
                report_url = self._pick_first_text(row_dict, ("网址", "链接", "报告地址", "url"))
                report_id = self._build_report_id(
                    instrument_id=instrument.get("instrument_id", ""),
                    publish_date=publish_date,
                    report_title=report_title,
                    institution_name=institution_name,
                    analyst_name=analyst_name,
                )
                if report_id in seen_ids:
                    continue
                seen_ids.add(report_id)

                normalized = {
                    "publish_date": publish_date,
                    "report_title": report_title,
                    "institution_name": institution_name,
                    "analyst_name": analyst_name,
                    "rating": rating,
                    "rating_change": rating_change,
                    "target_price": target_price,
                    "report_url": report_url,
                }
                snapshots.append(
                    ResearchReportSnapshot(
                        report_id=report_id,
                        instrument_id=instrument.get("instrument_id", ""),
                        symbol=instrument.get("symbol", ""),
                        exchange=instrument.get("exchange", ""),
                        publish_date=publish_date,
                        report_title=report_title,
                        institution_name=institution_name,
                        analyst_name=analyst_name,
                        rating=rating,
                        rating_change=rating_change,
                        target_price=target_price,
                        report_url=report_url,
                        source=self.source_name,
                        source_mode=mode,
                        report_json={
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
    def _build_report_id(
        *,
        instrument_id: str,
        publish_date: str,
        report_title: str,
        institution_name: Optional[str],
        analyst_name: Optional[str],
    ) -> str:
        payload = "|".join(
            [
                instrument_id,
                publish_date,
                report_title,
                institution_name or "",
                analyst_name or "",
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]

    @classmethod
    def _pick_first_text(
        cls,
        row: Dict[str, Any],
        aliases: tuple[str, ...],
    ) -> Optional[str]:
        for alias in aliases:
            for key, value in row.items():
                if alias.lower() in str(key).lower():
                    text = cls._to_text(value)
                    if text:
                        return text
        return None

    @classmethod
    def _pick_first_float(
        cls,
        row: Dict[str, Any],
        aliases: tuple[str, ...],
    ) -> Optional[float]:
        for alias in aliases:
            for key, value in row.items():
                if alias.lower() in str(key).lower():
                    parsed = cls._to_float(value)
                    if parsed is not None:
                        return parsed
        return None

    @classmethod
    def _pick_date(cls, row: Dict[str, Any]) -> Optional[str]:
        return cls._pick_first_text(row, ("日期", "报告日期", "发布时间", "发布日期"))

    @staticmethod
    def _to_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            return value.date().isoformat()
        text = str(value).strip()
        if text in {"", "nan", "None", "--"}:
            return None
        return text

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

    @staticmethod
    def _compact_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        compact: Dict[str, Any] = {}
        for key, value in row.items():
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            compact[str(key)] = value
        return compact
