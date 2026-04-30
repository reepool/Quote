"""
AkShare-backed event / sentiment provider.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.date_utils import get_shanghai_time

from .akshare_support import load_akshare
from .base import BaseSentimentEventProvider, SentimentEventSnapshot


class AkshareSentimentEventProvider(BaseSentimentEventProvider):
    """Fetch normalized notice and ownership-risk events through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    def __init__(
        self,
        *,
        lookback_days: int = 7,
        event_families: Optional[List[str]] = None,
    ):
        self.lookback_days = max(1, lookback_days)
        self.event_families = event_families or [
            "notice",
            "executive_share_change",
            "pledge_ratio",
        ]

    async def fetch_sentiment_events(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[SentimentEventSnapshot]:
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
            self._fetch_sentiment_events_sync,
            target_instruments,
            mode,
        )

    def _fetch_sentiment_events_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        mode: str,
    ) -> List[SentimentEventSnapshot]:
        akshare_module = self._akshare(mode)
        instrument_map = {
            self._normalize_symbol(instrument.get("symbol")): instrument
            for instrument in target_instruments
            if self._normalize_symbol(instrument.get("symbol"))
        }
        snapshots: List[SentimentEventSnapshot] = []
        seen_ids: set[str] = set()

        if "notice" in self.event_families:
            for snapshot in self._fetch_notice_events(akshare_module, instrument_map, mode):
                if snapshot.event_id in seen_ids:
                    continue
                seen_ids.add(snapshot.event_id)
                snapshots.append(snapshot)

        if "executive_share_change" in self.event_families:
            for snapshot in self._fetch_executive_share_change_events(
                akshare_module,
                instrument_map,
                mode,
            ):
                if snapshot.event_id in seen_ids:
                    continue
                seen_ids.add(snapshot.event_id)
                snapshots.append(snapshot)

        if "pledge_ratio" in self.event_families:
            for snapshot in self._fetch_pledge_ratio_events(akshare_module, instrument_map, mode):
                if snapshot.event_id in seen_ids:
                    continue
                seen_ids.add(snapshot.event_id)
                snapshots.append(snapshot)

        return snapshots

    def _fetch_notice_events(
        self,
        akshare_module: Any,
        instrument_map: Dict[str, Dict[str, Any]],
        mode: str,
    ) -> List[SentimentEventSnapshot]:
        snapshots: List[SentimentEventSnapshot] = []
        today = get_shanghai_time().date()
        for offset in range(self.lookback_days):
            target_date = today - timedelta(days=offset)
            date_key = target_date.strftime("%Y%m%d")
            try:
                frame = akshare_module.stock_notice_report(symbol="全部", date=date_key)
            except Exception:
                continue
            if frame is None or frame.empty:
                continue

            frame = frame.rename(columns={column: str(column).strip() for column in frame.columns})
            for _, row in frame.iterrows():
                row_dict = {str(key).strip(): value for key, value in row.items()}
                symbol = self._normalize_symbol(
                    self._pick_first_text(row_dict, ("代码", "股票代码", "证券代码"))
                )
                instrument = instrument_map.get(symbol)
                if instrument is None:
                    continue

                event_date = self._pick_first_text(
                    row_dict,
                    ("日期", "公告日期", "发布时间", "发布日期"),
                ) or target_date.isoformat()
                event_subtype = self._pick_first_text(
                    row_dict,
                    ("公告类型", "类型", "报告类型"),
                )
                title = self._pick_first_text(
                    row_dict,
                    ("公告标题", "公告名称", "标题", "名称"),
                ) or "公告事件"
                sentiment_score, severity = self._score_notice(event_subtype, title)
                snapshots.append(
                    self._build_snapshot(
                        instrument=instrument,
                        mode=mode,
                        event_date=event_date,
                        event_type="notice",
                        event_subtype=event_subtype,
                        title=title,
                        sentiment_score=sentiment_score,
                        severity=severity,
                        normalized_details={
                            "event_type": "notice",
                            "event_subtype": event_subtype,
                            "title": title,
                        },
                        raw_payload=row_dict,
                    )
                )
        return snapshots

    def _fetch_executive_share_change_events(
        self,
        akshare_module: Any,
        instrument_map: Dict[str, Dict[str, Any]],
        mode: str,
    ) -> List[SentimentEventSnapshot]:
        try:
            frame = akshare_module.stock_ggcg_em(symbol="全部")
        except Exception:
            return []
        if frame is None or frame.empty:
            return []

        frame = frame.rename(columns={column: str(column).strip() for column in frame.columns})
        snapshots: List[SentimentEventSnapshot] = []
        for _, row in frame.iterrows():
            row_dict = {str(key).strip(): value for key, value in row.items()}
            symbol = self._normalize_symbol(
                self._pick_first_text(row_dict, ("代码", "股票代码", "证券代码"))
            )
            instrument = instrument_map.get(symbol)
            if instrument is None:
                continue

            event_date = self._pick_first_text(
                row_dict,
                ("变动日期", "公告日", "公告日期", "开始日", "截止日期", "日期"),
            ) or get_shanghai_time().date().isoformat()
            event_subtype = self._pick_first_text(
                row_dict,
                ("变动方向", "变动类型", "方向", "类型"),
            )
            title = "高管持股变动"
            sentiment_score, severity = self._score_share_change(event_subtype, row_dict)
            snapshots.append(
                self._build_snapshot(
                    instrument=instrument,
                    mode=mode,
                    event_date=event_date,
                    event_type="executive_share_change",
                    event_subtype=event_subtype,
                    title=title,
                    sentiment_score=sentiment_score,
                    severity=severity,
                    normalized_details={
                        "event_type": "executive_share_change",
                        "event_subtype": event_subtype,
                    },
                    raw_payload=row_dict,
                )
            )
        return snapshots

    def _fetch_pledge_ratio_events(
        self,
        akshare_module: Any,
        instrument_map: Dict[str, Dict[str, Any]],
        mode: str,
    ) -> List[SentimentEventSnapshot]:
        today = get_shanghai_time().date()
        frame = None
        resolved_date = None
        for offset in range(self.lookback_days):
            target_date = today - timedelta(days=offset)
            date_key = target_date.strftime("%Y%m%d")
            try:
                candidate = akshare_module.stock_gpzy_pledge_ratio_em(date=date_key)
            except Exception:
                continue
            if candidate is not None and not candidate.empty:
                frame = candidate
                resolved_date = target_date.isoformat()
                break

        if frame is None or frame.empty:
            return []

        frame = frame.rename(columns={column: str(column).strip() for column in frame.columns})
        snapshots: List[SentimentEventSnapshot] = []
        for _, row in frame.iterrows():
            row_dict = {str(key).strip(): value for key, value in row.items()}
            symbol = self._normalize_symbol(
                self._pick_first_text(row_dict, ("代码", "股票代码", "证券代码"))
            )
            instrument = instrument_map.get(symbol)
            if instrument is None:
                continue

            event_date = self._pick_first_text(
                row_dict,
                ("日期", "公告日期", "统计日期"),
            ) or resolved_date or today.isoformat()
            pledge_ratio = self._pick_first_float(
                row_dict,
                ("质押比例", "累计质押比例", "质押总比例"),
            )
            sentiment_score, severity = self._score_pledge_ratio(pledge_ratio)
            snapshots.append(
                self._build_snapshot(
                    instrument=instrument,
                    mode=mode,
                    event_date=event_date,
                    event_type="pledge_ratio",
                    event_subtype=None,
                    title="股权质押比例更新",
                    sentiment_score=sentiment_score,
                    severity=severity,
                    normalized_details={
                        "event_type": "pledge_ratio",
                        "pledge_ratio": pledge_ratio,
                    },
                    raw_payload=row_dict,
                )
            )
        return snapshots

    def _build_snapshot(
        self,
        *,
        instrument: Dict[str, Any],
        mode: str,
        event_date: str,
        event_type: str,
        event_subtype: Optional[str],
        title: Optional[str],
        sentiment_score: Optional[float],
        severity: Optional[str],
        normalized_details: Dict[str, Any],
        raw_payload: Dict[str, Any],
    ) -> SentimentEventSnapshot:
        event_id = self._build_event_id(
            instrument_id=instrument.get("instrument_id", ""),
            event_date=event_date,
            event_type=event_type,
            event_subtype=event_subtype,
            title=title,
        )
        return SentimentEventSnapshot(
            event_id=event_id,
            instrument_id=instrument.get("instrument_id", ""),
            symbol=instrument.get("symbol", ""),
            exchange=instrument.get("exchange", ""),
            event_date=event_date,
            event_type=event_type,
            event_subtype=event_subtype,
            title=title,
            sentiment_score=sentiment_score,
            severity=severity,
            source=self.source_name,
            source_mode=mode,
            details_json={
                "normalized": normalized_details,
                "source_reported": self._compact_dict(raw_payload),
            },
            raw_payload=self._compact_dict(raw_payload),
        )

    @staticmethod
    def _akshare(mode: str = "direct"):
        return load_akshare(mode)

    @staticmethod
    def _build_event_id(
        *,
        instrument_id: str,
        event_date: str,
        event_type: str,
        event_subtype: Optional[str],
        title: Optional[str],
    ) -> str:
        payload = "|".join(
            [
                instrument_id,
                event_date,
                event_type,
                event_subtype or "",
                title or "",
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _normalize_symbol(value: Any) -> Optional[str]:
        if value in {None, ""}:
            return None
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        if not digits:
            return None
        return digits.zfill(6)

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
    def _score_notice(
        event_subtype: Optional[str],
        title: Optional[str],
    ) -> tuple[Optional[float], str]:
        label = f"{event_subtype or ''} {title or ''}"
        if "风险提示" in label:
            return -0.8, "high"
        if "重大事项" in label or "资产重组" in label:
            return 0.0, "medium"
        if "财务报告" in label:
            return 0.0, "low"
        if "持股变动" in label:
            return -0.2, "medium"
        return 0.0, "low"

    @staticmethod
    def _score_share_change(
        event_subtype: Optional[str],
        row: Dict[str, Any],
    ) -> tuple[Optional[float], str]:
        label = f"{event_subtype or ''} {row}"
        if "增持" in label:
            return 0.4, "medium"
        if "减持" in label:
            return -0.5, "high"
        return 0.0, "low"

    @staticmethod
    def _score_pledge_ratio(pledge_ratio: Optional[float]) -> tuple[Optional[float], str]:
        if pledge_ratio is None:
            return None, "low"
        if pledge_ratio >= 50:
            return -0.9, "high"
        if pledge_ratio >= 30:
            return -0.6, "high"
        if pledge_ratio >= 10:
            return -0.3, "medium"
        return -0.1, "low"

    @staticmethod
    def _compact_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        compact: Dict[str, Any] = {}
        for key, value in row.items():
            if value is None or (isinstance(value, float) and pd.isna(value)):
                continue
            compact[str(key)] = value
        return compact
