"""
AkShare/CNInfo-backed valuation input provider.

CNInfo exposes capital-change values in 10k-share units. This provider converts
them to raw share counts before they can be used by valuation history rebuilds.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .akshare_support import load_akshare
from .base import BaseValuationInputProvider, ValuationInputSnapshot


_logger = logging.getLogger("DataManager")


class AkshareCninfoValuationInputProvider(BaseValuationInputProvider):
    """Fetch total and float share-count inputs through AkShare CNInfo wrappers."""

    source_name = "cninfo"
    supported_modes = {"direct", "proxy_patch"}

    _code_columns = ("证券代码", "股票代码", "A股代码")
    _name_columns = ("证券简称", "股票简称", "公司简称")
    _market_columns = ("交易市场", "市场", "板块")
    _effective_date_columns = ("变动日期", "截止日期", "统计截止日")
    _announcement_date_columns = ("公告日期", "发布日期", "披露日期")
    _total_share_columns = ("总股本", "股份总数")
    _float_share_columns = ("已流通股份", "流通股本", "无限售条件流通股")
    _restricted_share_columns = ("流通受限股份", "限售股份")
    _reason_columns = ("变动原因", "变更原因")

    def __init__(
        self,
        *,
        request_interval_seconds: float = 0.2,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
    ):
        self.request_interval_seconds = max(0.0, float(request_interval_seconds))
        self.retry_attempts = max(0, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self._last_request_started_at = 0.0
        self._all_market_frame_cache: Dict[str, pd.DataFrame] = {}

    async def fetch_valuation_inputs(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        sync_mode: str = "incremental",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ValuationInputSnapshot]:
        if not self.supports_mode(mode):
            return []
        targets = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            targets = targets[:limit]
        if not targets:
            return []
        return await asyncio.to_thread(
            self._fetch_valuation_inputs_sync,
            targets,
            exchange,
            mode,
            sync_mode,
            start_date,
            end_date,
        )

    def _fetch_valuation_inputs_sync(
        self,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str,
        sync_mode: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> List[ValuationInputSnapshot]:
        akshare = load_akshare(mode)
        normalized_mode = str(sync_mode or "incremental").strip().lower()
        if normalized_mode in {"full", "backfill", "history", "historical"}:
            return self._fetch_history_snapshots(
                akshare,
                instruments,
                exchange,
                mode,
                start_date,
                end_date,
            )
        return self._fetch_latest_snapshots(
            akshare,
            instruments,
            exchange,
            mode,
            start_date,
            end_date,
        )

    def _fetch_latest_snapshots(
        self,
        akshare: Any,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> List[ValuationInputSnapshot]:
        frame = self._fetch_all_market_frame(akshare, mode)
        if frame.empty:
            _logger.warning(
                "[ValuationInputs] CNInfo latest all-market frame is empty: exchange=%s mode=%s instruments=%s",
                exchange,
                mode,
                len(instruments),
            )
            return []
        _logger.info(
            "[ValuationInputs] CNInfo latest all-market frame loaded: exchange=%s mode=%s raw_rows=%s instruments=%s",
            exchange,
            mode,
            len(frame),
            len(instruments),
        )
        by_code: Dict[str, Dict[str, Any]] = {}
        for _, row in frame.iterrows():
            row_dict = row.to_dict()
            code = self._normalize_symbol(self._first_value(row_dict, self._code_columns))
            if not code:
                continue
            by_code[code] = row_dict

        snapshots: List[ValuationInputSnapshot] = []
        for instrument in instruments:
            code = self._instrument_code(instrument)
            row = by_code.get(code)
            if row is None:
                continue
            snapshot = self._build_snapshot(
                row,
                instrument=instrument,
                exchange=exchange,
                mode=mode,
                input_kind="capital_snapshot",
                start_date=start_date,
                end_date=end_date,
            )
            if snapshot is not None:
                snapshots.append(snapshot)
        _logger.info(
            "[ValuationInputs] CNInfo latest snapshots prepared: exchange=%s requested=%s snapshots=%s",
            exchange,
            len(instruments),
            len(snapshots),
        )
        return snapshots

    def _fetch_history_snapshots(
        self,
        akshare: Any,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> List[ValuationInputSnapshot]:
        start_key = self._akshare_date(start_date) or "19900101"
        end_key = self._akshare_date(end_date) or self._akshare_date(date.today().isoformat())
        snapshots: List[ValuationInputSnapshot] = []
        for index, instrument in enumerate(instruments, start=1):
            code = self._instrument_code(instrument)
            if not code:
                continue
            if index == 1 or index % 100 == 0 or index == len(instruments):
                _logger.info(
                    "[ValuationInputs] CNInfo history fetch progress: exchange=%s index=%s/%s instrument_id=%s symbol=%s snapshots=%s",
                    exchange,
                    index,
                    len(instruments),
                    instrument.get("instrument_id"),
                    code,
                    len(snapshots),
                )
            try:
                frame = self._call_with_retry(
                    akshare.stock_share_change_cninfo,
                    symbol=code,
                    start_date=start_key,
                    end_date=end_key,
                )
            except Exception as exc:
                _logger.warning(
                    "[ValuationInputs] CNInfo history fetch failed: exchange=%s instrument_id=%s symbol=%s index=%s/%s error=%s",
                    exchange,
                    instrument.get("instrument_id"),
                    code,
                    index,
                    len(instruments),
                    exc,
                )
                continue
            if frame is None or frame.empty:
                continue
            for _, row in frame.iterrows():
                snapshot = self._build_snapshot(
                    row.to_dict(),
                    instrument=instrument,
                    exchange=exchange,
                    mode=mode,
                    input_kind="capital_change_event",
                    start_date=start_date,
                    end_date=end_date,
                )
                if snapshot is not None:
                    snapshots.append(snapshot)
        _logger.info(
            "[ValuationInputs] CNInfo history snapshots prepared: exchange=%s requested=%s snapshots=%s start=%s end=%s",
            exchange,
            len(instruments),
            len(snapshots),
            start_key,
            end_key,
        )
        return snapshots

    def _fetch_all_market_frame(self, akshare: Any, mode: str) -> pd.DataFrame:
        cached = self._all_market_frame_cache.get(mode)
        if cached is not None:
            return cached
        frame = self._call_with_retry(akshare.stock_hold_change_cninfo, symbol="全部")
        if frame is None:
            frame = pd.DataFrame()
        _logger.info(
            "[ValuationInputs] CNInfo all-market snapshot fetched: mode=%s rows=%s",
            mode,
            len(frame),
        )
        self._all_market_frame_cache[mode] = frame
        return frame

    def _call_with_retry(self, func: Any, **kwargs: Any) -> Any:
        attempts = self.retry_attempts + 1
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            self._throttle()
            try:
                return func(**kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt >= attempts - 1:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        return None

    def _throttle(self) -> None:
        if self.request_interval_seconds <= 0:
            self._last_request_started_at = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_request_started_at
        if elapsed < self.request_interval_seconds:
            time.sleep(self.request_interval_seconds - elapsed)
        self._last_request_started_at = time.monotonic()

    def _build_snapshot(
        self,
        row: Dict[str, Any],
        *,
        instrument: Dict[str, Any],
        exchange: str,
        mode: str,
        input_kind: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Optional[ValuationInputSnapshot]:
        effective_date = self._normalize_date(
            self._first_value(row, self._effective_date_columns)
        )
        announcement_date = self._normalize_date(
            self._first_value(row, self._announcement_date_columns)
        )
        as_of_date = effective_date or announcement_date
        if not as_of_date:
            return None
        if start_date and as_of_date < start_date:
            return None
        if end_date and as_of_date > end_date:
            return None

        total_shares_10k = self._safe_float(
            self._first_value(row, self._total_share_columns)
        )
        float_shares_10k = self._safe_float(
            self._first_value(row, self._float_share_columns)
        )
        if total_shares_10k is None and float_shares_10k is None:
            return None

        diagnostics = {
            "source_unit": "10k_share",
            "canonical_unit": "share",
            "unit_multiplier": 10000.0,
            "effective_date": effective_date,
            "announcement_date": announcement_date,
            "cninfo_market": self._first_value(row, self._market_columns),
            "cninfo_name": self._first_value(row, self._name_columns),
            "change_reason": self._first_value(row, self._reason_columns),
            "restricted_shares_10k": self._safe_float(
                self._first_value(row, self._restricted_share_columns)
            ),
        }
        return ValuationInputSnapshot(
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=str(instrument.get("symbol") or self._instrument_code(instrument)),
            exchange=exchange,
            as_of_date=as_of_date,
            currency="CNY",
            shares_outstanding=(
                None if total_shares_10k is None else total_shares_10k * 10000.0
            ),
            float_shares=(
                None if float_shares_10k is None else float_shares_10k * 10000.0
            ),
            source=self.source_name,
            source_mode=mode,
            input_kind=input_kind,
            unit="share",
            data_as_of=announcement_date or as_of_date,
            diagnostics_json=diagnostics,
        )

    @classmethod
    def _first_value(cls, row: Dict[str, Any], names: tuple[str, ...]) -> Any:
        for name in names:
            if name in row and pd.notna(row[name]):
                return row[name]
        return None

    @staticmethod
    def _instrument_code(instrument: Dict[str, Any]) -> str:
        for key in ("symbol", "instrument_id"):
            value = str(instrument.get(key) or "").strip()
            code = AkshareCninfoValuationInputProvider._normalize_symbol(value)
            if code:
                return code
        return ""

    @staticmethod
    def _normalize_symbol(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = text.split(".")[0]
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits.zfill(6) if digits else ""

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        text = str(value).strip().replace(",", "")
        if not text or text in {"-", "--", "nan", "None"}:
            return None
        if text.endswith("%"):
            text = text[:-1]
        try:
            number = float(text)
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number

    @staticmethod
    def _normalize_date(value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, (datetime, date)):
            return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
        text = str(value).strip()
        if not text or text in {"-", "--", "nan", "None"}:
            return None
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date().isoformat()

    @staticmethod
    def _akshare_date(value: Optional[str]) -> Optional[str]:
        normalized = AkshareCninfoValuationInputProvider._normalize_date(value)
        return None if normalized is None else normalized.replace("-", "")
