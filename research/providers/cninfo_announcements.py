"""
Reusable CNInfo announcement metadata scanner.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests

from utils.http_transport import HttpTlsConfig, create_requests_session

_logger = logging.getLogger("DataManager")

_CNINFO_ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
_CNINFO_ANNOUNCEMENT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.cninfo.com.cn",
    "Referer": "https://www.cninfo.com.cn/new/disclosure/stock",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}


@dataclass(frozen=True)
class CninfoAnnouncementRecord:
    """Normalized CNInfo announcement metadata record."""

    announcement_id: str
    title: str
    announcement_time: Optional[str]
    market: str
    column: str
    symbols: List[str] = field(default_factory=list)
    sec_names: List[str] = field(default_factory=list)
    org_ids: List[str] = field(default_factory=list)
    adjunct_url: Optional[str] = None
    adjunct_type: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    selection_reasons: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class CninfoAnnouncementScanConfig:
    """Configuration for one market/column announcement scan."""

    purpose_key: str
    market: str
    column: str
    plate: Optional[str] = None
    tab_name: str = "fulltext"
    category: Optional[str] = None
    search_key: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    page_size: int = 30
    max_pages: int = 20
    stop_at_watermark: Optional[str] = None


@dataclass(frozen=True)
class CninfoAnnouncementScanResult:
    """Result of one reusable CNInfo announcement scan."""

    config: CninfoAnnouncementScanConfig
    records: List[CninfoAnnouncementRecord]
    selected_records: List[CninfoAnnouncementRecord]
    pages_scanned: int
    announcements_seen: int
    max_announcement_time: Optional[str]
    stopped_at_watermark: bool = False
    errors: List[str] = field(default_factory=list)


AnnouncementFilter = Callable[[CninfoAnnouncementRecord], List[str]]


class CninfoAnnouncementScanner:
    """Scan CNInfo announcement metadata with caller-provided filters."""

    def __init__(
        self,
        *,
        url: str = _CNINFO_ANNOUNCEMENT_URL,
        request_timeout_seconds: float = 20.0,
        request_interval_seconds: float = 0.2,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.url = url
        self.request_timeout_seconds = request_timeout_seconds
        self.request_interval_seconds = max(0.0, request_interval_seconds)
        self.retry_attempts = max(0, retry_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.tls_config = HttpTlsConfig(source_name="cninfo")
        self.session = session or create_requests_session(tls_config=self.tls_config)

    def scan(
        self,
        config: CninfoAnnouncementScanConfig,
        *,
        filters: Optional[Iterable[AnnouncementFilter]] = None,
    ) -> CninfoAnnouncementScanResult:
        """Scan one CNInfo market/column and return normalized records."""
        selected: List[CninfoAnnouncementRecord] = []
        records: List[CninfoAnnouncementRecord] = []
        errors: List[str] = []
        max_time: Optional[str] = None
        stopped_at_watermark = False
        filter_list = list(filters or [])
        pages_scanned = 0

        for page_num in range(1, max(1, config.max_pages) + 1):
            payload: Optional[Dict[str, Any]] = None
            try:
                payload = self._request_page(config, page_num)
            except Exception as exc:
                errors.append(str(exc))
                _logger.warning(
                    "[CninfoAnnouncements] Page request failed: purpose=%s market=%s column=%s page=%s error=%s",
                    config.purpose_key,
                    config.market,
                    config.column,
                    page_num,
                    exc,
                )
                break

            page_records = [
                self._normalize_record(item, config)
                for item in self._extract_records(payload)
            ]
            page_records = [record for record in page_records if record is not None]
            pages_scanned += 1
            if not page_records:
                break

            page_times = [
                record.announcement_time
                for record in page_records
                if record.announcement_time
            ]
            for record_time in page_times:
                if max_time is None or record_time > max_time:
                    max_time = record_time

            for record in page_records:
                records.append(record)
                reasons: List[str] = []
                for predicate in filter_list:
                    reasons.extend(predicate(record) or [])
                if reasons:
                    selected.append(
                        CninfoAnnouncementRecord(
                            announcement_id=record.announcement_id,
                            title=record.title,
                            announcement_time=record.announcement_time,
                            market=record.market,
                            column=record.column,
                            symbols=record.symbols,
                            sec_names=record.sec_names,
                            org_ids=record.org_ids,
                            adjunct_url=record.adjunct_url,
                            adjunct_type=record.adjunct_type,
                            raw_payload=record.raw_payload,
                            selection_reasons=sorted(set(reasons)),
                        )
                    )

            if self._page_reached_watermark(page_records, config.stop_at_watermark):
                stopped_at_watermark = True
                break
            if len(page_records) < max(1, config.page_size):
                break
            if self.request_interval_seconds > 0:
                time.sleep(self.request_interval_seconds)

        return CninfoAnnouncementScanResult(
            config=config,
            records=records,
            selected_records=selected,
            pages_scanned=pages_scanned,
            announcements_seen=len(records),
            max_announcement_time=max_time,
            stopped_at_watermark=stopped_at_watermark,
            errors=errors,
        )

    def _request_page(
        self,
        config: CninfoAnnouncementScanConfig,
        page_num: int,
    ) -> Dict[str, Any]:
        body = {
            "pageNum": str(page_num),
            "pageSize": str(max(1, config.page_size)),
            "column": config.column,
            "tabName": config.tab_name,
            "isHLtitle": "true",
        }
        if config.plate:
            body["plate"] = config.plate
        if config.category:
            body["category"] = config.category
        if config.search_key:
            body["searchkey"] = config.search_key
        if config.start_date and config.end_date:
            body["seDate"] = f"{config.start_date}~{config.end_date}"

        last_exc: Optional[Exception] = None
        for attempt in range(self.retry_attempts + 1):
            try:
                response = self.session.post(
                    self.url,
                    data=body,
                    headers=_CNINFO_ANNOUNCEMENT_HEADERS,
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("CNInfo announcement response is not a JSON object")
                return payload
            except Exception as exc:  # requests/json errors should share retry policy.
                last_exc = exc
                if attempt >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * (attempt + 1))
        raise RuntimeError(f"CNInfo announcement request failed: {last_exc}")

    @classmethod
    def _extract_records(cls, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates: List[Any] = [
            payload.get("announcements"),
            payload.get("data"),
            payload.get("records"),
            payload.get("rows"),
        ]
        data = payload.get("data")
        if isinstance(data, dict):
            candidates.extend(
                [
                    data.get("announcements"),
                    data.get("records"),
                    data.get("rows"),
                ]
            )
        classified = payload.get("classifiedAnnouncements")
        if isinstance(classified, list):
            flattened: List[Dict[str, Any]] = []
            for group in classified:
                if not isinstance(group, dict):
                    continue
                announcements = group.get("announcements")
                if isinstance(announcements, list):
                    flattened.extend(
                        item for item in announcements if isinstance(item, dict)
                    )
            if flattened:
                return flattened
        for candidate in candidates:
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
        return []

    @classmethod
    def _normalize_record(
        cls,
        row: Dict[str, Any],
        config: CninfoAnnouncementScanConfig,
    ) -> Optional[CninfoAnnouncementRecord]:
        announcement_id = str(
            row.get("announcementId")
            or row.get("id")
            or row.get("announcement_id")
            or ""
        ).strip()
        title = str(
            row.get("announcementTitle")
            or row.get("title")
            or row.get("announcement_title")
            or ""
        ).strip()
        if not announcement_id or not title:
            return None
        return CninfoAnnouncementRecord(
            announcement_id=announcement_id,
            title=title,
            announcement_time=cls._normalize_time(
                row.get("announcementTime")
                or row.get("announcement_time")
                or row.get("publishTime")
            ),
            market=config.market,
            column=config.column,
            symbols=cls._split_values(
                row.get("secCode") or row.get("stockCode") or row.get("symbol")
            ),
            sec_names=cls._split_values(row.get("secName") or row.get("stockName")),
            org_ids=cls._split_values(row.get("orgId") or row.get("org_id")),
            adjunct_url=cls._first_text(row.get("adjunctUrl") or row.get("url")),
            adjunct_type=cls._first_text(row.get("adjunctType") or row.get("fileType")),
            raw_payload=dict(row),
        )

    @staticmethod
    def _split_values(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            raw_values = value
        else:
            raw_values = str(value).replace(";", ",").split(",")
        result: List[str] = []
        for item in raw_values:
            text = str(item or "").strip()
            if text and text not in result:
                result.append(text)
        return result

    @staticmethod
    def _first_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_time(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            return CninfoAnnouncementScanner._normalize_time(int(text))
        return text

    @staticmethod
    def _page_reached_watermark(
        records: List[CninfoAnnouncementRecord],
        watermark: Optional[str],
    ) -> bool:
        if not watermark:
            return False
        times = [
            record.announcement_time
            for record in records
            if record.announcement_time
        ]
        return bool(times) and max(times) <= watermark
