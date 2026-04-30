"""
AkShare-backed shareholder summary provider.
"""

from __future__ import annotations

import asyncio
import time
import warnings
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from .akshare_support import load_akshare
from .base import BaseShareholderProvider, ShareholderSnapshot


class AkshareShareholdersProvider(BaseShareholderProvider):
    """Fetch latest shareholder summary snapshots through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    _holder_count_aliases = (
        "股东户数-本次",
        "股东户数",
        "本期股东人数",
        "股东总数",
    )
    _holder_count_date_aliases = (
        "股东户数统计截止日",
        "截至日期",
        "变动日期",
        "股东户数公告日期",
        "公告日期",
    )
    _top_holder_name_aliases = (
        "股东名称",
        "名称",
    )
    _top_holder_ratio_aliases = (
        "持股比例",
        "占总股本持股比例",
        "占总流通股本持股比例",
    )
    _top_holder_shares_aliases = (
        "持股数量",
        "持股数",
    )
    _top_holder_type_aliases = (
        "股本性质",
        "股份类型",
        "股东性质",
    )
    _top_holder_date_aliases = (
        "截至日期",
        "截止日期",
        "公告日期",
    )

    def __init__(
        self,
        *,
        top_holders_request_interval_seconds: float = 0.0,
        top_holders_retry_attempts: int = 0,
        top_holders_retry_backoff_seconds: float = 0.0,
    ):
        self.top_holders_request_interval_seconds = max(
            0.0,
            float(top_holders_request_interval_seconds),
        )
        self.top_holders_retry_attempts = max(0, int(top_holders_retry_attempts))
        self.top_holders_retry_backoff_seconds = max(
            0.0,
            float(top_holders_retry_backoff_seconds),
        )
        self._last_top_holders_request_started_at = 0.0

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
        akshare = load_akshare(mode)
        snapshots: List[ShareholderSnapshot] = []
        for instrument in target_instruments:
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                continue

            request_symbols = self._request_symbol_candidates(instrument, exchange)
            for request_symbol in request_symbols:
                holder_count_payload: Any = None
                top_holders_payload: Any = None
                holder_count_error: Optional[str] = None
                top_holders_error: Optional[str] = None
                try:
                    holder_count_payload = self._fetch_holder_count_payload(
                        akshare,
                        request_symbol,
                    )
                except Exception as exc:
                    holder_count_payload = None
                    holder_count_error = str(exc)
                top_holders_payload, top_holders_error = self._fetch_top_holders_with_retry(
                    akshare,
                    request_symbol,
                )

                snapshot = self._build_snapshot(
                    instrument=instrument,
                    exchange=exchange,
                    mode=mode,
                    holder_count_payload=holder_count_payload,
                    top_holders_payload=top_holders_payload,
                    holder_count_error=holder_count_error,
                    top_holders_error=top_holders_error,
                    request_symbol=request_symbol,
                    request_symbol_candidates=request_symbols,
                )
                if snapshot is not None:
                    snapshots.append(snapshot)
                    break
        return snapshots

    def _fetch_holder_count_payload(self, akshare: Any, symbol: str) -> Any:
        return akshare.stock_zh_a_gdhs_detail_em(symbol=symbol)

    def _fetch_top_holders_payload(self, akshare: Any, symbol: str) -> Any:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Downcasting object dtype arrays on .*",
                category=FutureWarning,
            )
            return akshare.stock_main_stock_holder(stock=symbol)

    def _fetch_top_holders_with_retry(
        self,
        akshare: Any,
        symbol: str,
    ) -> tuple[Any, Optional[str]]:
        last_error: Optional[str] = None
        total_attempts = 1 + self.top_holders_retry_attempts

        for attempt_index in range(total_attempts):
            self._wait_for_top_holders_slot()
            try:
                payload = self._fetch_top_holders_payload(akshare, symbol)
            except Exception as exc:
                last_error = str(exc)
                payload = None
            else:
                if self._payload_rows(payload):
                    return payload, None
                last_error = (
                    f"empty payload after attempt {attempt_index + 1}/{total_attempts}"
                )

            if attempt_index >= total_attempts - 1:
                break

            backoff_seconds = self.top_holders_retry_backoff_seconds * (attempt_index + 1)
            if backoff_seconds > 0:
                time.sleep(backoff_seconds)

        return None, last_error

    def _wait_for_top_holders_slot(self) -> None:
        interval_seconds = self.top_holders_request_interval_seconds
        if interval_seconds <= 0:
            self._last_top_holders_request_started_at = time.monotonic()
            return

        now = time.monotonic()
        elapsed = now - self._last_top_holders_request_started_at
        if self._last_top_holders_request_started_at > 0 and elapsed < interval_seconds:
            time.sleep(interval_seconds - elapsed)
        self._last_top_holders_request_started_at = time.monotonic()

    def _build_snapshot(
        self,
        *,
        instrument: Dict[str, Any],
        exchange: str,
        mode: str,
        holder_count_payload: Any,
        top_holders_payload: Any,
        holder_count_error: Optional[str] = None,
        top_holders_error: Optional[str] = None,
        request_symbol: Optional[str] = None,
        request_symbol_candidates: Optional[List[str]] = None,
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
            fallback_holder_count,
        ) = self._extract_top_holders_info(top_holders_payload)

        if holder_count is None:
            holder_count = fallback_holder_count
            if holder_count_report_date is None:
                holder_count_report_date = top_holders_report_date

        if holder_count is None and not top_holders:
            return None

        coverage_scope: List[str] = []
        if holder_count is not None:
            coverage_scope.append("holder_count")
        if top_holders:
            coverage_scope.append("top10_holders")
        if control_owner_name or control_owner_ratio is not None:
            coverage_scope.append("reference_only_ownership_clues")
        elif top_holders:
            coverage_scope.append("reference_only_ownership_clues")

        raw_payload = {
            "request_symbol": request_symbol or str(instrument.get("symbol") or ""),
            "request_symbol_candidates": request_symbol_candidates or [],
            "holder_count": self._json_ready(holder_count_payload),
            "top_holders": self._json_ready(top_holders_payload),
        }
        fetch_errors = {
            key: value
            for key, value in {
                "holder_count": holder_count_error,
                "top_holders": top_holders_error,
            }.items()
            if value
        }
        if fetch_errors:
            raw_payload["fetch_errors"] = fetch_errors

        snapshot_json = {
            "coverage_scope": coverage_scope,
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
            raw_payload=raw_payload,
        )

    def _extract_holder_count_info(self, payload: Any) -> tuple[Optional[int], Optional[str]]:
        rows = self._payload_rows(payload)
        if not rows:
            return None, None

        best_row = max(
            rows,
            key=lambda row: self._date_sort_key(
                self._pick_first(row, self._holder_count_date_aliases)
            ),
        )
        report_date = self._pick_first(best_row, self._holder_count_date_aliases)
        report_date = self._normalize_date(report_date)
        holder_count = self._to_int(self._pick_first(best_row, self._holder_count_aliases))
        return holder_count, report_date

    def _extract_top_holders_info(
        self,
        payload: Any,
    ) -> tuple[
        Optional[str],
        List[Dict[str, Any]],
        Optional[float],
        Optional[str],
        Optional[float],
        Optional[int],
    ]:
        rows = self._payload_rows(payload)
        if not rows:
            return None, [], None, None, None, None

        best_date = max(
            (
                self._normalize_date(self._pick_first(row, self._top_holder_date_aliases))
                for row in rows
            ),
            default=None,
            key=self._date_sort_key,
        )
        latest_rows = [
            row
            for row in rows
            if self._normalize_date(self._pick_first(row, self._top_holder_date_aliases)) == best_date
        ]
        latest_rows.sort(key=self._row_rank)

        holders: List[Dict[str, Any]] = []
        fallback_holder_count: Optional[int] = None
        for index, row in enumerate(latest_rows[:10], start=1):
            if fallback_holder_count is None:
                fallback_holder_count = self._to_int(
                    self._pick_first(row, self._holder_count_aliases)
                )
            holder_name = self._pick_first(row, self._top_holder_name_aliases)
            if not holder_name:
                continue
            ratio = self._to_float(self._pick_first(row, self._top_holder_ratio_aliases))
            holders.append(
                {
                    "rank": index,
                    "holder_name": holder_name,
                    "holding_ratio": ratio,
                    "holding_shares": self._to_float(
                        self._pick_first(row, self._top_holder_shares_aliases)
                    ),
                    "holder_type": self._pick_first(row, self._top_holder_type_aliases),
                }
            )

        if not holders:
            return best_date, [], None, None, None, fallback_holder_count

        total_ratio = sum(
            ratio for ratio in (item.get("holding_ratio") for item in holders) if ratio is not None
        )
        control_owner = holders[0]
        return (
            best_date,
            holders,
            total_ratio if total_ratio > 0 else None,
            control_owner.get("holder_name"),
            control_owner.get("holding_ratio"),
            fallback_holder_count,
        )

    @staticmethod
    def _payload_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, pd.DataFrame):
            if payload.empty:
                return []
            normalized = payload.where(pd.notnull(payload), None)
            return normalized.to_dict(orient="records")
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            return [payload]
        return []

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
    def _pick_first(row: Dict[str, Any], aliases: Iterable[str]) -> Optional[str]:
        for alias in aliases:
            value = row.get(alias)
            if value not in (None, ""):
                return str(value).strip()
        return None

    @staticmethod
    def _row_rank(row: Dict[str, Any]) -> int:
        try:
            return int(float(str(row.get("编号") or row.get("名次") or 9999)))
        except (TypeError, ValueError):
            return 9999

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
