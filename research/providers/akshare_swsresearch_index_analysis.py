"""
AkShare-backed historical Shenwan index-analysis provider.
"""

from __future__ import annotations

import asyncio
import math
import socket
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests
import urllib3

from .base import BaseIndustryIndexAnalysisProvider, IndustryIndexAnalysisSnapshot


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AkshareSWSResearchIndexAnalysisProvider(BaseIndustryIndexAnalysisProvider):
    """Fetch historical SWS index-analysis daily rows with AkShare semantics.

    AkShare's ``index_analysis_daily_sw`` wraps SWS ``index_analysis_report``.
    This provider uses the same upstream endpoint and field semantics directly
    so the research task can enforce request timeouts and retries. Values are
    already in SWS display units: percentages remain percent values, market cap
    is in CNY 100 million, and ``成交量`` is volume in 100 million shares. The
    normalized storage field ``bargain_volume`` is the traded volume field and
    does not mean traded amount.
    """

    source_name = "akshare_swsresearch_index_analysis"
    supported_modes = {"direct", "proxy_patch"}

    DEFAULT_INDEX_TYPES = ["市场表征", "一级行业", "二级行业", "三级行业", "风格指数"]
    REQUIRED_COLUMNS = ("指数代码", "指数名称", "发布日期")
    COLUMN_ALIASES = {
        "指数代码": ("指数代码", "swindexcode"),
        "指数名称": ("指数名称", "swindexname"),
        "发布日期": ("发布日期", "bargaindate"),
    }
    NUMERIC_COLUMNS = {
        "收盘指数": "close_index",
        "成交量": "bargain_volume",
        "涨跌幅": "markup",
        "换手率": "turnover_rate",
        "市盈率": "pe",
        "市净率": "pb",
        "均价": "mean_price",
        "成交额占比": "bargain_sum_rate",
        "流通市值": "negotiable_share_sum",
        "平均流通市值": "average_negotiable_share_sum",
        "股息率": "dividend_yield",
    }
    NUMERIC_ALIASES = {
        "收盘指数": ("收盘指数", "closeindex"),
        "成交量": ("成交量", "bargainamount"),
        "涨跌幅": ("涨跌幅", "markup"),
        "换手率": ("换手率", "turnoverrate"),
        "市盈率": ("市盈率", "pe"),
        "市净率": ("市净率", "pb"),
        "均价": ("均价", "meanprice"),
        "成交额占比": ("成交额占比", "bargainsumrate"),
        "流通市值": ("流通市值", "negotiablessharesum1"),
        "平均流通市值": ("平均流通市值", "negotiablessharesum2"),
        "股息率": ("股息率", "dp"),
    }

    def __init__(
        self,
        *,
        endpoint: str = (
            "https://www.swsresearch.com/institute-sw/api/index_analysis/"
            "index_analysis_report/"
        ),
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
        index_types: Optional[List[str]] = None,
        request_timeout_seconds: float = 20.0,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
        request_interval_seconds: float = 0.0,
        page_size: int = 50,
        max_pages_per_type: int = 200,
    ):
        self.endpoint = endpoint
        self.taxonomy_system = taxonomy_system
        self.taxonomy_version = taxonomy_version
        self.index_types = list(index_types or self.DEFAULT_INDEX_TYPES)
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.request_interval_seconds = max(0.0, float(request_interval_seconds))
        self.page_size = max(1, int(page_size))
        self.max_pages_per_type = max(1, int(max_pages_per_type))
        self._last_request_started_at = 0.0

    async def fetch_latest_index_analysis(
        self,
        *,
        mode: str = "direct",
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
    ) -> List[IndustryIndexAnalysisSnapshot]:
        if not self.supports_mode(mode):
            raise ValueError(f"Unsupported AkShare index-analysis mode: {mode}")
        return await asyncio.to_thread(
            self._fetch_index_analysis_sync,
            mode,
            list(index_types or self.index_types),
            limit_per_type,
            start_date,
            end_date,
            latest_date,
        )

    def _fetch_index_analysis_sync(
        self,
        mode: str,
        index_types: List[str],
        limit_per_type: Optional[int],
        start_date: Optional[str],
        end_date: Optional[str],
        latest_date: Optional[str],
    ) -> List[IndustryIndexAnalysisSnapshot]:
        requested_start = _normalize_akshare_date(latest_date or start_date)
        requested_end = _normalize_akshare_date(latest_date or end_date or start_date)
        if requested_start is None or requested_end is None:
            raise ValueError("AkShare index-analysis history requires start_date/end_date or latest_date")
        if requested_start > requested_end:
            raise ValueError("start_date must be earlier than or equal to end_date")

        snapshots: List[IndustryIndexAnalysisSnapshot] = []
        session = requests.Session()
        for index_type in index_types:
            records = self._fetch_records(
                session,
                index_type=index_type,
                start_date=requested_start,
                end_date=requested_end,
            )
            type_snapshots = [
                self._parse_record(record, index_type=index_type, mode=mode)
                for record in records
            ]
            if limit_per_type is not None:
                type_snapshots = type_snapshots[: max(0, int(limit_per_type))]
            snapshots.extend(type_snapshots)
        return snapshots

    def _fetch_records(
        self,
        session: requests.Session,
        *,
        index_type: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        page = 1
        while page <= self.max_pages_per_type:
            payload = self._fetch_page_with_retry(
                session,
                index_type=index_type,
                start_date=start_date,
                end_date=end_date,
                page=page,
            )
            data = payload.get("data") or {}
            page_rows = data.get("results") or []
            records.extend(page_rows)
            count = int(data.get("count") or len(records))
            if len(records) >= count or not page_rows:
                break
            page += 1
        return records

    def _fetch_page_with_retry(
        self,
        session: requests.Session,
        *,
        index_type: str,
        start_date: str,
        end_date: str,
        page: int,
    ) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retry_attempts + 1):
            self._wait_for_request_slot()
            try:
                response = self._request_page(
                    session,
                    index_type=index_type,
                    start_date=start_date,
                    end_date=end_date,
                    page=page,
                )
                response.raise_for_status()
                payload = response.json()
                if "data" not in payload:
                    raise ValueError(f"unexpected payload: {payload}")
                return payload
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(
            "AkShare index-analysis request failed "
            f"for {index_type} {start_date}-{end_date}: {last_error}"
        )

    def _request_page(
        self,
        session: requests.Session,
        *,
        index_type: str,
        start_date: str,
        end_date: str,
        page: int,
    ) -> requests.Response:
        old_default_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self.request_timeout_seconds)
        try:
            return session.get(
                self.endpoint,
                params={
                    "page": page,
                    "page_size": self.page_size,
                    "index_type": index_type,
                    "start_date": _format_remote_date(start_date),
                    "end_date": _format_remote_date(end_date),
                    "type": "DAY",
                    "swindexcode": "all",
                },
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0 Safari/537.36"
                    )
                },
                timeout=self.request_timeout_seconds,
                verify=False,
            )
        finally:
            socket.setdefaulttimeout(old_default_timeout)

    def _wait_for_request_slot(self) -> None:
        if self.request_interval_seconds <= 0:
            self._last_request_started_at = time.monotonic()
            return
        now = time.monotonic()
        elapsed = now - self._last_request_started_at
        if self._last_request_started_at > 0 and elapsed < self.request_interval_seconds:
            time.sleep(self.request_interval_seconds - elapsed)
        self._last_request_started_at = time.monotonic()

    def _parse_record(
        self,
        record: Dict[str, Any],
        *,
        index_type: str,
        mode: str = "direct",
    ) -> IndustryIndexAnalysisSnapshot:
        missing = [
            column
            for column in self.REQUIRED_COLUMNS
            if not str(_record_value(record, self.COLUMN_ALIASES[column]) or "").strip()
        ]
        if missing:
            raise ValueError(f"AkShare index-analysis row missing fields: {missing}")

        metrics = {
            target: _to_float(_record_value(record, self.NUMERIC_ALIASES[source]))
            for source, target in self.NUMERIC_COLUMNS.items()
        }
        return IndustryIndexAnalysisSnapshot(
            taxonomy_system=self.taxonomy_system,
            taxonomy_version=self.taxonomy_version,
            sw_index_code=str(_record_value(record, self.COLUMN_ALIASES["指数代码"])).strip(),
            sw_index_name=str(_record_value(record, self.COLUMN_ALIASES["指数名称"])).strip(),
            trade_date=_to_trade_date(_record_value(record, self.COLUMN_ALIASES["发布日期"])),
            index_type=index_type,
            source=self.source_name,
            source_mode=mode,
            raw_payload=dict(record),
            **metrics,
        )


def _frame_records(frame: Any) -> Iterable[Dict[str, Any]]:
    if frame is None:
        return []
    if hasattr(frame, "to_dict"):
        return frame.to_dict(orient="records")
    if isinstance(frame, list):
        return frame
    raise ValueError(f"Unsupported AkShare index-analysis payload type: {type(frame)!r}")


def _record_value(record: Dict[str, Any], aliases: Iterable[str]) -> Any:
    for alias in aliases:
        if alias in record:
            return record.get(alias)
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "nan", "NaN", "None", "NaT"}:
        return None
    return float(text)


def _to_trade_date(value: Any) -> str:
    if value is None:
        raise ValueError("AkShare index-analysis row has empty 发布日期")
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    text = str(value).strip()
    if not text:
        raise ValueError("AkShare index-analysis row has empty 发布日期")
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        return text[:10]


def _normalize_akshare_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return text
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().strftime("%Y%m%d")
    except ValueError:
        compact = text.replace("-", "")
        if len(compact) == 8 and compact.isdigit():
            return compact
        raise


def _format_remote_date(value: str) -> str:
    return "-".join([value[:4], value[4:6], value[6:]])
