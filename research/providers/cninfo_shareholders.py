"""
cninfo-backed shareholder fallback provider.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from .akshare_support import load_akshare
from .base import BaseShareholderProvider, ShareholderSnapshot


class CninfoShareholdersProvider(BaseShareholderProvider):
    """Fetch fallback shareholder snapshots through cninfo endpoints."""

    source_name = "cninfo"
    supported_modes = {"direct"}

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
        )

    def _fetch_shareholder_snapshots_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        exchange: str,
    ) -> List[ShareholderSnapshot]:
        akshare = load_akshare("direct")
        symbols = {
            symbol
            for instrument in target_instruments
            for symbol in self._request_symbol_candidates(instrument, exchange)
        }
        if not symbols:
            return []

        holder_count_rows = self._load_latest_holder_count_rows(akshare, symbols)
        control_rows = self._load_control_holder_rows(akshare, symbols)

        snapshots: List[ShareholderSnapshot] = []
        for instrument in target_instruments:
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                continue
            request_symbols = self._request_symbol_candidates(instrument, exchange)
            holder_count_row = self._first_row_for_symbols(
                holder_count_rows,
                request_symbols,
            )
            control_row = self._first_row_for_symbols(
                control_rows,
                request_symbols,
            )
            snapshot = self._build_snapshot(
                instrument=instrument,
                exchange=exchange,
                holder_count_row=holder_count_row,
                control_row=control_row,
                request_symbol_candidates=request_symbols,
            )
            if snapshot is not None:
                snapshots.append(snapshot)
        return snapshots

    def _load_latest_holder_count_rows(
        self,
        akshare: Any,
        symbols: set[str],
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        unresolved_symbols = set(symbols)
        for report_date in self._candidate_report_dates():
            if not unresolved_symbols:
                break
            try:
                frame = akshare.stock_hold_num_cninfo(date=report_date)
            except Exception:
                continue
            if frame is None or frame.empty:
                continue
            normalized = frame.where(pd.notnull(frame), None)
            rows = normalized.to_dict(orient="records")
            for row in rows:
                symbol = str(row.get("证券代码") or "").strip()
                if symbol and symbol in unresolved_symbols:
                    result[symbol] = row
                    unresolved_symbols.discard(symbol)
        return result

    def _load_control_holder_rows(
        self,
        akshare: Any,
        symbols: set[str],
    ) -> Dict[str, Dict[str, Any]]:
        try:
            frame = akshare.stock_hold_control_cninfo(symbol="全部")
        except Exception:
            return {}
        if frame is None or frame.empty:
            return {}

        rows = frame.where(pd.notnull(frame), None).to_dict(orient="records")
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("证券代码") or "").strip()
            if not symbol or symbol not in symbols:
                continue
            current = result.get(symbol)
            if current is None or self._date_sort_key(
                self._normalize_date(row.get("变动日期"))
            ) >= self._date_sort_key(self._normalize_date(current.get("变动日期"))):
                result[symbol] = row
        return result

    def _build_snapshot(
        self,
        *,
        instrument: Dict[str, Any],
        exchange: str,
        holder_count_row: Optional[Dict[str, Any]],
        control_row: Optional[Dict[str, Any]],
        request_symbol_candidates: Optional[List[str]] = None,
    ) -> Optional[ShareholderSnapshot]:
        holder_count = self._to_int(
            self._pick_first(holder_count_row, ("本期股东人数", "股东人数", "股东户数"))
        )
        holder_count_report_date = self._normalize_date(
            self._pick_first(holder_count_row, ("变动日期",))
        )
        control_owner_name = self._pick_first(
            control_row,
            ("实际控制人名称", "直接控制人名称"),
        )
        control_owner_ratio = self._to_float(
            self._pick_first(control_row, ("控股比例",))
        )
        control_owner_report_date = self._normalize_date(
            self._pick_first(control_row, ("变动日期",))
        )

        if holder_count is None and control_owner_name is None and control_owner_ratio is None:
            return None

        coverage_scope: List[str] = []
        if holder_count is not None:
            coverage_scope.append("holder_count")
        if control_owner_name or control_owner_ratio is not None:
            coverage_scope.append("reference_only_ownership_clues")

        snapshot_json = {
            "coverage_scope": coverage_scope,
            "holder_count": {
                "value": holder_count,
                "report_date": holder_count_report_date,
            },
            "top_holders": [],
            "ownership_clues": {
                "control_owner_name": control_owner_name,
                "control_owner_ratio": control_owner_ratio,
                "report_date": control_owner_report_date,
            },
        }

        return ShareholderSnapshot(
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=str(instrument.get("symbol") or ""),
            exchange=exchange,
            coverage_status="reference_only",
            holder_count=holder_count,
            holder_count_report_date=holder_count_report_date,
            top_holders_report_date=None,
            top_holders_count=0,
            top_holders_total_ratio=None,
            control_owner_name=control_owner_name,
            control_owner_ratio=control_owner_ratio,
            source=self.source_name,
            source_mode="direct",
            snapshot_json=snapshot_json,
            raw_payload={
                "request_symbol_candidates": request_symbol_candidates or [],
                "holder_count": self._json_ready(holder_count_row),
                "control_holder": self._json_ready(control_row),
            },
        )

    @classmethod
    def _request_symbol_candidates(
        cls,
        instrument: Dict[str, Any],
        exchange: str,
    ) -> List[str]:
        raw_candidates = [
            str(instrument.get("symbol") or "").strip(),
            str(instrument.get("instrument_id") or "").split(".")[0].strip(),
        ]
        candidates: List[str] = []
        for raw_symbol in raw_candidates:
            if not raw_symbol or raw_symbol in candidates:
                continue
            candidates.append(raw_symbol)
            bse_symbol = cls._normalize_bse_request_symbol(raw_symbol, exchange)
            if bse_symbol and bse_symbol not in candidates:
                candidates.append(bse_symbol)
        return candidates

    @staticmethod
    def _normalize_bse_request_symbol(symbol: str, exchange: str) -> Optional[str]:
        if exchange != "BSE" or len(symbol) != 6:
            return None
        if symbol.startswith("920"):
            return None
        if symbol.startswith(("43", "83", "87")):
            return f"920{symbol[-3:]}"
        return None

    @staticmethod
    def _first_row_for_symbols(
        rows_by_symbol: Dict[str, Dict[str, Any]],
        symbols: List[str],
    ) -> Optional[Dict[str, Any]]:
        for symbol in symbols:
            row = rows_by_symbol.get(symbol)
            if row:
                return row
        return None

    @staticmethod
    def _candidate_report_dates(limit: int = 8) -> List[str]:
        today = date.today()
        quarter = (today.month - 1) // 3
        year = today.year
        if quarter == 0:
            quarter = 4
            year -= 1

        dates: List[str] = []
        for _ in range(limit):
            if quarter == 1:
                report_date = date(year, 3, 31)
            elif quarter == 2:
                report_date = date(year, 6, 30)
            elif quarter == 3:
                report_date = date(year, 9, 30)
            else:
                report_date = date(year, 12, 31)
            dates.append(report_date.strftime("%Y%m%d"))
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1
        return dates

    @staticmethod
    def _pick_first(
        row: Optional[Dict[str, Any]],
        aliases: Iterable[str],
    ) -> Optional[str]:
        if not row:
            return None
        for alias in aliases:
            value = row.get(alias)
            if value not in (None, ""):
                return str(value).strip()
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

    @classmethod
    def _normalize_date(cls, value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        if isinstance(value, (datetime, date)):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip().replace("/", "-")
        return text or None

    @staticmethod
    def _date_sort_key(value: Optional[str]) -> str:
        return "" if value in (None, "") else str(value)

    @classmethod
    def _json_ready(cls, payload: Any) -> Any:
        if isinstance(payload, pd.DataFrame):
            if payload.empty:
                return []
            normalized = payload.where(pd.notnull(payload), None)
            return cls._json_ready(normalized.to_dict(orient="records"))
        if isinstance(payload, (datetime, date)):
            return payload.isoformat()
        if isinstance(payload, dict):
            return {str(key): cls._json_ready(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [cls._json_ready(item) for item in payload]
        if payload is None:
            return None
        try:
            if pd.isna(payload):
                return None
        except TypeError:
            pass
        return payload
