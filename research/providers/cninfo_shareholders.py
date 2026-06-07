"""
cninfo-backed shareholder fallback provider.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

from utils.http_transport import HttpTlsConfig, create_requests_session

from .akshare_support import load_akshare
from .base import BaseShareholderProvider, ShareholderSnapshot


_logger = logging.getLogger("DataManager")

_CNINFO_DATA20_BASE_URL = "https://www.cninfo.com.cn/data20/stockholderCapital"
_CNINFO_DATA20_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.cninfo.com.cn/new/disclosure/stock",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}


class CninfoShareholdersProvider(BaseShareholderProvider):
    """Fetch fallback shareholder snapshots through cninfo endpoints."""

    source_name = "cninfo"
    supported_modes = {"direct"}

    def __init__(
        self,
        *,
        request_timeout_seconds: float = 15.0,
        request_interval_seconds: float = 0.3,
        retry_attempts: int = 3,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        self.request_timeout_seconds = request_timeout_seconds
        self.request_interval_seconds = request_interval_seconds
        self.retry_attempts = max(0, retry_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.tls_config = HttpTlsConfig(source_name=self.source_name)

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
        started_at = time.monotonic()
        _logger.info(
            "[CninfoShareholders] Batch fetch started: exchange=%s instruments=%s",
            exchange,
            len(target_instruments),
        )
        akshare = load_akshare("direct")
        symbols = {
            symbol
            for instrument in target_instruments
            for symbol in self._request_symbol_candidates(instrument, exchange)
        }
        if not symbols:
            _logger.info(
                "[CninfoShareholders] Batch fetch skipped: exchange=%s reason=no_request_symbols",
                exchange,
            )
            return []

        session = create_requests_session(tls_config=self.tls_config)
        holder_count_rows = self._load_latest_holder_count_rows(akshare, symbols)
        control_rows = self._load_control_holder_rows(akshare, symbols)
        top_holder_bundles = self._load_top_holder_bundles(
            session,
            target_instruments,
            exchange,
            holder_count_rows,
        )
        _logger.info(
            "[CninfoShareholders] Source tables loaded: exchange=%s symbols=%s holder_count_rows=%s control_rows=%s top_holder_bundles=%s elapsed=%.1fs",
            exchange,
            len(symbols),
            len(holder_count_rows),
            len(control_rows),
            len(top_holder_bundles),
            time.monotonic() - started_at,
        )

        snapshots: List[ShareholderSnapshot] = []
        for index, instrument in enumerate(target_instruments, start=1):
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                _logger.debug(
                    "[CninfoShareholders] Instrument skipped: exchange=%s index=%s/%s instrument_id=%s reason=missing_symbol",
                    exchange,
                    index,
                    len(target_instruments),
                    instrument.get("instrument_id"),
                )
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
                top_holder_bundle=top_holder_bundles.get(
                    str(instrument.get("instrument_id") or "")
                ),
                request_symbol_candidates=request_symbols,
            )
            if snapshot is not None:
                snapshots.append(snapshot)
                _logger.debug(
                    "[CninfoShareholders] Instrument resolved: exchange=%s index=%s/%s instrument_id=%s request_symbols=%s coverage=%s",
                    exchange,
                    index,
                    len(target_instruments),
                    instrument.get("instrument_id"),
                    request_symbols,
                    snapshot.snapshot_json.get("coverage_scope", []),
                )
            else:
                _logger.debug(
                    "[CninfoShareholders] Instrument unresolved: exchange=%s index=%s/%s instrument_id=%s request_symbols=%s",
                    exchange,
                    index,
                    len(target_instruments),
                    instrument.get("instrument_id"),
                    request_symbols,
                )
            if index % 500 == 0:
                _logger.info(
                    "[CninfoShareholders] Batch progress: exchange=%s processed=%s/%s snapshots=%s elapsed=%.1fs",
                    exchange,
                    index,
                    len(target_instruments),
                    len(snapshots),
                    time.monotonic() - started_at,
                )
        _logger.info(
            "[CninfoShareholders] Batch fetch finished: exchange=%s instruments=%s snapshots=%s elapsed=%.1fs",
            exchange,
            len(target_instruments),
            len(snapshots),
            time.monotonic() - started_at,
        )
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

    def _load_top_holder_bundles(
        self,
        session: requests.Session,
        instruments: List[Dict[str, Any]],
        exchange: str,
        holder_count_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        started_at = time.monotonic()
        for index, instrument in enumerate(instruments, start=1):
            instrument_id = str(instrument.get("instrument_id") or "").strip()
            if not instrument_id:
                continue
            request_symbols = self._request_symbol_candidates(instrument, exchange)
            holder_count_known = (
                self._first_row_for_symbols(holder_count_rows or {}, request_symbols)
                is not None
            )
            bundle = self._fetch_first_top_holder_bundle(
                session,
                request_symbols,
                include_holder_count=not holder_count_known,
            )
            if bundle.get("top_holders"):
                result[instrument_id] = bundle
            elif bundle.get("fetch_errors"):
                result[instrument_id] = bundle

            if index % 100 == 0:
                _logger.info(
                    "[CninfoShareholders] Top-holder progress: exchange=%s processed=%s/%s resolved=%s elapsed=%.1fs",
                    exchange,
                    index,
                    len(instruments),
                    len(
                        [
                            item
                            for item in result.values()
                            if item.get("top_holders")
                        ]
                    ),
                    time.monotonic() - started_at,
                )
            if self.request_interval_seconds > 0:
                time.sleep(self.request_interval_seconds)
        return result

    def _fetch_first_top_holder_bundle(
        self,
        session: requests.Session,
        request_symbols: List[str],
        include_holder_count: bool = False,
    ) -> Dict[str, Any]:
        fetch_errors: Dict[str, str] = {}
        raw_records_by_symbol: Dict[str, Any] = {}
        for request_symbol in request_symbols:
            try:
                records, raw_payload = self._request_data20_records(
                    session,
                    "getTopTenStockholders",
                    request_symbol,
                )
            except Exception as exc:
                fetch_errors[request_symbol] = str(exc)
                _logger.debug(
                    "[CninfoShareholders] Top-holder request failed: request_symbol=%s error=%s",
                    request_symbol,
                    exc,
                )
                continue

            raw_records_by_symbol[request_symbol] = self._json_ready(raw_payload)
            top_holders_report_date, top_holders, top_holders_total_ratio = (
                self._extract_top_holder_info(records)
            )
            if top_holders:
                holder_count_info = None
                holder_count_raw_payload = None
                if include_holder_count:
                    try:
                        holder_count_records, holder_count_raw_payload = (
                            self._request_data20_records(
                                session,
                                "getStockholderNum",
                                request_symbol,
                            )
                        )
                        holder_count_info = self._extract_holder_count_from_data20(
                            holder_count_records
                        )
                    except Exception as exc:
                        fetch_errors[f"{request_symbol}:holder_count"] = str(exc)
                        _logger.debug(
                            "[CninfoShareholders] Holder-count data20 request failed: request_symbol=%s error=%s",
                            request_symbol,
                            exc,
                        )
                return {
                    "request_symbol": request_symbol,
                    "raw_records": records,
                    "raw_payload": raw_payload,
                    "holder_count": holder_count_info,
                    "holder_count_raw_payload": holder_count_raw_payload,
                    "top_holders": top_holders,
                    "top_holders_report_date": top_holders_report_date,
                    "top_holders_total_ratio": top_holders_total_ratio,
                    "fetch_errors": fetch_errors,
                }
            fetch_errors[request_symbol] = "empty top10 holder records"

        return {
            "request_symbol": None,
            "raw_records_by_symbol": raw_records_by_symbol,
            "holder_count": None,
            "holder_count_raw_payload": None,
            "top_holders": [],
            "top_holders_report_date": None,
            "top_holders_total_ratio": None,
            "fetch_errors": fetch_errors,
        }

    def _request_data20_records(
        self,
        session: requests.Session,
        endpoint: str,
        symbol: str,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        for attempt in range(self.retry_attempts + 1):
            try:
                return self._request_data20_records_once(session, endpoint, symbol)
            except Exception as exc:
                retryable = "resultCode=429" in str(exc) or "RateLimit" in str(exc)
                if not retryable or attempt >= self.retry_attempts:
                    raise
                sleep_seconds = self.retry_backoff_seconds * (attempt + 1)
                _logger.debug(
                    "[CninfoShareholders] Data20 request retrying: endpoint=%s symbol=%s attempt=%s/%s sleep=%.1fs error=%s",
                    endpoint,
                    symbol,
                    attempt + 1,
                    self.retry_attempts + 1,
                    sleep_seconds,
                    exc,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
        raise RuntimeError("CNInfo data20 request failed unexpectedly")

    def _request_data20_records_once(
        self,
        session: requests.Session,
        endpoint: str,
        symbol: str,
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        url = f"{_CNINFO_DATA20_BASE_URL}/{endpoint}"
        response = session.get(
            url,
            params={"scode": symbol},
            headers=_CNINFO_DATA20_HEADERS,
            timeout=self.request_timeout_seconds,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            snippet = response.text[:120].replace("\n", " ")
            raise RuntimeError(
                f"CNInfo data20 returned non-JSON response: http={response.status_code} body={snippet!r}"
            ) from exc

        if response.status_code >= 400:
            message = (
                payload.get("msg")
                or payload.get("message")
                or payload.get("error")
                or response.reason
            )
            raise RuntimeError(
                f"CNInfo data20 request failed: http={response.status_code} code={payload.get('code')} message={message}"
            )

        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            result_msg = data.get("resultMsg")
            if result_msg and result_msg != "success":
                raise RuntimeError(
                    "CNInfo data20 logical failure: "
                    f"resultCode={data.get('resultCode')} resultMsg={result_msg}"
                )

        records = self._extract_data20_records(payload)
        return records, payload

    @classmethod
    def _extract_data20_records(cls, payload: Any) -> List[Dict[str, Any]]:
        candidates: List[Any] = []
        if isinstance(payload, dict):
            candidates.extend(
                [
                    payload.get("records"),
                    payload.get("data"),
                    payload.get("result"),
                    payload.get("rows"),
                ]
            )
            data = payload.get("data")
            if isinstance(data, dict):
                candidates.extend(
                    [
                        data.get("records"),
                        data.get("list"),
                        data.get("rows"),
                    ]
                )
        for candidate in candidates:
            if isinstance(candidate, list):
                return [
                    item
                    for item in candidate
                    if isinstance(item, dict)
                ]
        if isinstance(payload, list):
            return [
                item
                for item in payload
                if isinstance(item, dict)
            ]
        return []

    def _extract_top_holder_info(
        self,
        records: List[Dict[str, Any]],
    ) -> tuple[Optional[str], List[Dict[str, Any]], Optional[float]]:
        if not records:
            return None, [], None

        best_date = max(
            (
                self._normalize_date(record.get("F001D"))
                for record in records
                if self._normalize_date(record.get("F001D"))
            ),
            default=None,
        )
        if best_date is None:
            return None, [], None

        latest_rows = [
            record
            for record in records
            if self._normalize_date(record.get("F001D")) == best_date
        ]
        top_holders: List[Dict[str, Any]] = []
        for row in latest_rows:
            holder_name = self._pick_first(row, ("F002V", "holderName", "股东名称"))
            if not holder_name:
                continue
            holding_ratio = self._to_float(
                self._pick_first(row, ("F004N", "holdingRatio", "持股比例"))
            )
            holding_shares_10k = self._to_float(
                self._pick_first(row, ("F003N", "holdingShares", "持股数量"))
            )
            holding_shares = (
                int(round(holding_shares_10k * 10000))
                if holding_shares_10k is not None
                else None
            )
            top_holders.append(
                {
                    "rank": self._to_int(
                        self._pick_first(row, ("F005N", "rank", "名次"))
                    ),
                    "holder_name": holder_name,
                    "holding_shares": holding_shares,
                    "holding_ratio": holding_ratio,
                    "holder_type": self._pick_first(
                        row,
                        ("F006V", "shareType", "股份性质"),
                    ),
                    "change": self._pick_first(row, ("F007V", "change", "增减")),
                    "report_date": best_date,
                }
            )

        top_holders.sort(
            key=lambda item: (
                item.get("rank") is None,
                item.get("rank") or 999,
                item.get("holder_name") or "",
            )
        )
        ratios = [
            ratio
            for ratio in (
                item.get("holding_ratio")
                for item in top_holders
            )
            if ratio is not None
        ]
        total_ratio = round(sum(ratios), 6) if ratios else None
        return best_date, top_holders, total_ratio

    def _extract_holder_count_from_data20(
        self,
        records: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not records:
            return None
        latest_row = max(
            records,
            key=lambda row: self._date_sort_key(
                self._normalize_date(row.get("ENDDATE"))
            ),
        )
        holder_count = self._to_int(latest_row.get("F001N"))
        report_date = self._normalize_date(latest_row.get("ENDDATE"))
        if holder_count is None and report_date is None:
            return None
        return {
            "value": holder_count,
            "report_date": report_date,
            "raw_row": latest_row,
        }

    def _build_snapshot(
        self,
        *,
        instrument: Dict[str, Any],
        exchange: str,
        holder_count_row: Optional[Dict[str, Any]],
        control_row: Optional[Dict[str, Any]],
        top_holder_bundle: Optional[Dict[str, Any]] = None,
        request_symbol_candidates: Optional[List[str]] = None,
    ) -> Optional[ShareholderSnapshot]:
        holder_count = self._to_int(
            self._pick_first(holder_count_row, ("本期股东人数", "股东人数", "股东户数"))
        )
        holder_count_report_date = self._normalize_date(
            self._pick_first(holder_count_row, ("变动日期",))
        )
        if holder_count is None:
            data20_holder_count = (top_holder_bundle or {}).get("holder_count") or {}
            holder_count = self._to_int(data20_holder_count.get("value"))
            holder_count_report_date = self._normalize_date(
                data20_holder_count.get("report_date")
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
        top_holders = list((top_holder_bundle or {}).get("top_holders") or [])
        top_holders_report_date = (top_holder_bundle or {}).get(
            "top_holders_report_date"
        )
        top_holders_total_ratio = (top_holder_bundle or {}).get(
            "top_holders_total_ratio"
        )

        if (
            holder_count is None
            and control_owner_name is None
            and control_owner_ratio is None
            and not top_holders
        ):
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
                "report_date": control_owner_report_date,
            },
        }
        raw_payload = {
            "request_symbol": (top_holder_bundle or {}).get("request_symbol"),
            "request_symbol_candidates": request_symbol_candidates or [],
            "holder_count": self._json_ready(holder_count_row),
            "holder_count_data20": self._json_ready(
                (top_holder_bundle or {}).get("holder_count")
            ),
            "holder_count_data20_raw": self._json_ready(
                (top_holder_bundle or {}).get("holder_count_raw_payload")
            ),
            "control_holder": self._json_ready(control_row),
            "top_holders": self._json_ready((top_holder_bundle or {}).get("raw_records")),
        }
        fetch_errors = (top_holder_bundle or {}).get("fetch_errors")
        if fetch_errors:
            raw_payload["fetch_errors"] = {"top_holders": self._json_ready(fetch_errors)}
        if (top_holder_bundle or {}).get("raw_records_by_symbol"):
            raw_payload["top_holders_by_symbol"] = self._json_ready(
                (top_holder_bundle or {}).get("raw_records_by_symbol")
            )

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
            source_mode="direct",
            snapshot_json=snapshot_json,
            raw_payload=raw_payload,
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
