"""
Optional efinance-backed shareholder summary provider.
"""

from __future__ import annotations

import asyncio
import importlib
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .base import BaseShareholderProvider, ShareholderSnapshot


class EfinanceShareholdersProvider(BaseShareholderProvider):
    """Fetch latest shareholder summary snapshots through efinance."""

    source_name = "efinance"
    supported_modes = {"direct"}

    _holder_count_aliases = (
        "股东户数",
        "股东人数",
        "最新股东户数",
        "股东户数-本次",
        "本次户数",
    )
    _report_date_aliases = (
        "截止日期",
        "公告日期",
        "报告期",
        "日期",
        "更新日期",
    )
    _holder_name_aliases = (
        "股东名称",
        "股东姓名",
        "名称",
    )
    _holder_ratio_aliases = (
        "持股比例",
        "占总股本持股比例",
        "占总流通股本持股比例",
        "持股占比",
    )
    _holder_shares_aliases = (
        "持股数",
        "持股数量",
        "持股股数",
        "持有数量",
    )
    _holder_type_aliases = (
        "股东性质",
        "股份性质",
        "股东类型",
    )

    async def fetch_shareholder_snapshots(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[ShareholderSnapshot]:
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
            self._fetch_shareholder_snapshots_sync,
            target_instruments,
            exchange,
            mode,
        )

    def _fetch_shareholder_snapshots_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str,
    ) -> List[ShareholderSnapshot]:
        snapshots: List[ShareholderSnapshot] = []
        for instrument in target_instruments:
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                continue

            holder_count_payload = self._fetch_holder_count_payload(symbol)
            top_holders_payload = self._fetch_top_holders_payload(symbol)
            snapshot = self._build_snapshot(
                instrument=instrument,
                exchange=exchange,
                mode=mode,
                holder_count_payload=holder_count_payload,
                top_holders_payload=top_holders_payload,
            )
            if snapshot is not None:
                snapshots.append(snapshot)
        return snapshots

    def _fetch_holder_count_payload(self, symbol: str) -> Any:
        stock_api = getattr(self._efinance_module(), "stock", None)
        if stock_api is None:
            raise RuntimeError("efinance.stock is unavailable")
        fetcher = getattr(stock_api, "get_latest_holder_number", None)
        if fetcher is None:
            raise RuntimeError("efinance.stock.get_latest_holder_number is unavailable")
        return fetcher(symbol)

    def _fetch_top_holders_payload(self, symbol: str) -> Any:
        stock_api = getattr(self._efinance_module(), "stock", None)
        if stock_api is None:
            raise RuntimeError("efinance.stock is unavailable")
        fetcher = getattr(stock_api, "get_top10_stock_holder_info", None)
        if fetcher is None:
            raise RuntimeError("efinance.stock.get_top10_stock_holder_info is unavailable")
        return fetcher(symbol)

    def _build_snapshot(
        self,
        *,
        instrument: Dict[str, Any],
        exchange: str,
        mode: str,
        holder_count_payload: Any,
        top_holders_payload: Any,
    ) -> Optional[ShareholderSnapshot]:
        holder_count, holder_count_report_date = self._extract_holder_count_info(
            holder_count_payload
        )
        (
            top_holders_report_date,
            top_holders,
            top_holders_total_ratio,
            control_owner_name,
            control_owner_ratio,
        ) = self._extract_top_holders_info(top_holders_payload)

        if holder_count is None and not top_holders:
            return None

        snapshot_json = {
            "coverage_scope": [
                "holder_count",
                "top10_holders",
                "reference_only_ownership_clues",
            ],
            "holder_count": {
                "value": holder_count,
                "report_date": holder_count_report_date,
            },
            "top_holders": top_holders,
            "ownership_clues": {
                "control_owner_name": control_owner_name,
                "control_owner_ratio": control_owner_ratio,
            },
        }

        return ShareholderSnapshot(
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=str(instrument.get("symbol") or ""),
            exchange=exchange,
            coverage_status="reference_only",
            holder_count=holder_count,
            holder_count_report_date=holder_count_report_date,
            top_holders_report_date=top_holders_report_date,
            top_holders_count=len(top_holders),
            top_holders_total_ratio=top_holders_total_ratio,
            control_owner_name=control_owner_name,
            control_owner_ratio=control_owner_ratio,
            source=self.source_name,
            source_mode=mode,
            snapshot_json=snapshot_json,
            raw_payload={
                "holder_count": self._json_ready(holder_count_payload),
                "top_holders": self._json_ready(top_holders_payload),
            },
        )

    def _extract_holder_count_info(self, payload: Any) -> Tuple[Optional[int], Optional[str]]:
        rows = self._payload_rows(payload)
        if not rows:
            return None, None

        best_row = self._select_latest_row(rows)
        return (
            self._to_int(self._pick_first(best_row, self._holder_count_aliases)),
            self._pick_first(best_row, self._report_date_aliases),
        )

    def _extract_top_holders_info(
        self,
        payload: Any,
    ) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[float], Optional[str], Optional[float]]:
        frames_with_hint = self._flatten_payload_frames(payload)
        if not frames_with_hint:
            return None, [], None, None, None

        best_rows: List[Dict[str, Any]] = []
        best_report_date: Optional[str] = None
        for report_hint, frame in frames_with_hint:
            rows = self._dataframe_rows(frame)
            if not rows:
                continue
            report_date = report_hint or self._pick_first(rows[0], self._report_date_aliases)
            if best_report_date is None or self._date_sort_key(report_date) >= self._date_sort_key(best_report_date):
                best_rows = rows
                best_report_date = report_date

        if not best_rows:
            return None, [], None, None, None

        holders: List[Dict[str, Any]] = []
        for index, row in enumerate(best_rows[:10], start=1):
            holder_name = self._pick_first(row, self._holder_name_aliases)
            if not holder_name:
                continue
            ratio = self._to_float(self._pick_first(row, self._holder_ratio_aliases))
            holder = {
                "rank": index,
                "holder_name": holder_name,
                "holding_ratio": ratio,
                "holding_shares": self._to_float(self._pick_first(row, self._holder_shares_aliases)),
                "holder_type": self._pick_first(row, self._holder_type_aliases),
            }
            holders.append(holder)

        if not holders:
            return best_report_date, [], None, None, None

        total_ratio = sum(
            ratio for ratio in (item.get("holding_ratio") for item in holders) if ratio is not None
        )
        control_owner = holders[0]
        return (
            best_report_date,
            holders,
            total_ratio if total_ratio > 0 else None,
            control_owner.get("holder_name"),
            control_owner.get("holding_ratio"),
        )

    @staticmethod
    def _efinance_module() -> Any:
        try:
            return importlib.import_module("efinance")
        except ImportError as exc:
            raise RuntimeError("efinance is not installed") from exc

    @staticmethod
    def _to_dataframe(payload: Any) -> Optional[pd.DataFrame]:
        if isinstance(payload, pd.DataFrame):
            return payload.copy()
        if isinstance(payload, list):
            if not payload:
                return None
            if isinstance(payload[0], dict):
                return pd.DataFrame(payload)
        if isinstance(payload, dict):
            for value in payload.values():
                if isinstance(value, pd.DataFrame):
                    return value.copy()
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    return pd.DataFrame(value)
        return None

    def _flatten_payload_frames(self, payload: Any) -> List[Tuple[Optional[str], pd.DataFrame]]:
        if isinstance(payload, pd.DataFrame):
            return [(None, payload.copy())]
        if isinstance(payload, list):
            frame = self._to_dataframe(payload)
            return [] if frame is None else [(None, frame)]
        if isinstance(payload, dict):
            frames: List[Tuple[Optional[str], pd.DataFrame]] = []
            for key, value in payload.items():
                frame = self._to_dataframe(value)
                if frame is not None:
                    frames.append((str(key), frame))
            return frames
        return []

    def _payload_rows(self, payload: Any) -> List[Dict[str, Any]]:
        frame = self._to_dataframe(payload)
        return [] if frame is None else self._dataframe_rows(frame)

    @staticmethod
    def _dataframe_rows(frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame is None or frame.empty:
            return []
        normalized = frame.where(pd.notnull(frame), None)
        return normalized.to_dict(orient="records")

    def _select_latest_row(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        def sort_key(row: Dict[str, Any]) -> str:
            return self._date_sort_key(self._pick_first(row, self._report_date_aliases))

        return max(rows, key=sort_key)

    @staticmethod
    def _pick_first(row: Dict[str, Any], aliases: Iterable[str]) -> Optional[str]:
        for alias in aliases:
            if alias in row and row[alias] not in (None, ""):
                return str(row[alias]).strip()
        return None

    @staticmethod
    def _to_int(value: Optional[str]) -> Optional[int]:
        if value in (None, ""):
            return None
        text = str(value).strip().replace(",", "")
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_float(value: Optional[str]) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        text = str(value).strip().replace(",", "").replace("%", "")
        if text.lower() in {"nan", "none", "null"}:
            return None
        try:
            return float(text)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _date_sort_key(value: Optional[str]) -> str:
        return "" if value in (None, "") else str(value)

    @classmethod
    def _json_ready(cls, payload: Any) -> Any:
        if isinstance(payload, pd.DataFrame):
            return cls._json_ready(cls._dataframe_rows(payload))
        if isinstance(payload, (datetime, date)):
            return payload.isoformat()
        if isinstance(payload, dict):
            return {str(key): cls._json_ready(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [cls._json_ready(item) for item in payload]
        if pd.isna(payload):
            return None
        return payload
