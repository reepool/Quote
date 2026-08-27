"""Reusable HKEXnews title-search announcement provider."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

from research.announcements.base import (
    AnnouncementProviderCapabilities,
    AnnouncementQueryNotSupported,
)
from research.announcements.categories import hkexnews_category_options
from research.announcements.models import (
    AnnouncementAttachment,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementScanResult,
    build_announcement_key,
    build_derived_announcement_id,
    normalize_published_at,
)
from utils.http_transport import HttpTlsConfig, create_requests_session


LOGGER = logging.getLogger(__name__)
HONG_KONG_TZ = ZoneInfo("Asia/Hong_Kong")
_DEFAULT_ENDPOINT = "https://www1.hkexnews.hk/search/titleSearchServlet.do"
_DEFAULT_WARMUP = "https://www1.hkexnews.hk/search/titlesearch.xhtml"
_DEFAULT_REFERER = "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=EN"
_DEFAULT_ARTIFACT_BASE = "https://www1.hkexnews.hk"
_STOCK_CODE_SPLIT_RE = re.compile(r"<[^>]+>|[;,/|]+|\s+")


class HkexnewsAnnouncementProvider:
    """Normalize HKEXnews title-search metadata without business classification."""

    source_name = "hkexnews"

    def __init__(
        self,
        source_config: Optional[Mapping[str, Any]] = None,
        *,
        session: Optional[requests.Session] = None,
    ) -> None:
        config = dict(source_config or {})
        if not bool(config.get("enabled", True)):
            raise ValueError("official announcement provider is disabled: hkexnews")
        self.endpoint_url = str(config.get("endpoint_url") or _DEFAULT_ENDPOINT).strip()
        self.warmup_url = str(config.get("warmup_url") or _DEFAULT_WARMUP).strip()
        self.referer = str(config.get("referer") or _DEFAULT_REFERER).strip()
        self.artifact_base_url = str(
            config.get("artifact_base_url") or _DEFAULT_ARTIFACT_BASE
        ).strip()
        if not self.endpoint_url or not self.referer or not self.artifact_base_url:
            raise ValueError("hkexnews endpoint, referer, and artifact base URL are required")
        self.request_timeout_seconds = max(
            1.0, float(config.get("request_timeout_seconds", 20.0))
        )
        self.request_interval_seconds = max(
            0.0, float(config.get("request_interval_seconds", 0.15))
        )
        self.retry_attempts = max(0, int(config.get("retry_attempts", 2)))
        self.retry_backoff_seconds = max(
            0.0, float(config.get("retry_backoff_seconds", 0.5))
        )
        self.default_markets = tuple(
            str(item).strip().upper()
            for item in (config.get("markets") or ("SEHK", "GEM"))
            if str(item).strip()
        ) or ("SEHK", "GEM")
        self.capabilities = AnnouncementProviderCapabilities(
            exchanges=frozenset({"HKEX"}),
            supports_market_scope=True,
            supports_instrument_scope=True,
            supports_date_filter=True,
            supports_keyword_filter=True,
            supports_category_filter=True,
            max_page_size=max(1, int(config.get("max_page_size", 100))),
        )
        self.session = session or create_requests_session(
            tls_config=HttpTlsConfig(source_name="hkex"),
            headers={
                "Accept": "application/json, text/javascript, */*;q=0.1",
                "Referer": self.referer,
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                ),
            },
        )
        self._warmed_up = False

    def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
        self.capabilities.validate(query)
        scope = query.scope
        if not scope.is_instrument_scoped and not (scope.start_date and scope.end_date):
            raise AnnouncementQueryNotSupported(
                "hkexnews market-wide search requires a date range"
            )
        category_options = hkexnews_category_options(scope.category)
        if scope.category and category_options is None:
            raise AnnouncementQueryNotSupported(
                f"hkexnews does not support announcement category {scope.category}"
            )
        category_options = category_options or {}
        markets = self._resolve_markets(scope.market)
        windows = self._date_windows(scope.start_date, scope.end_date)
        records: List[AnnouncementRecord] = []
        errors: List[str] = []
        requests_made = 0
        pages_scanned = 0
        self._ensure_warmup()
        requests_made += 1
        stop_reason = "complete"
        page_size = min(scope.page_size, self.capabilities.max_page_size)
        max_pages = max(1, int(scope.max_pages))
        for market in markets:
            for window_start, window_end in windows:
                row_range = page_size
                window_pages = 0
                while True:
                    try:
                        payload = self._request_search(
                            market=market,
                            start_date=window_start,
                            end_date=window_end,
                            page_size=row_range,
                            keyword=scope.keyword,
                            category_options=category_options,
                            stock_id=str(scope.source_options.get("stock_id") or "-1"),
                        )
                    except Exception as exc:
                        errors.append(
                            f"hkexnews {market} {window_start}/{window_end} request failed: {exc}"
                        )
                        stop_reason = "request_failed"
                        break
                    requests_made += 1
                    pages_scanned += 1
                    window_pages += 1
                    try:
                        rows = self._extract_rows(payload)
                    except ValueError as exc:
                        errors.append(
                            f"hkexnews {market} {window_start}/{window_end} malformed payload: {exc}"
                        )
                        stop_reason = "malformed_payload"
                        break
                    for row in rows:
                        record = self._normalize_record(
                            row,
                            market=market,
                            headline_category=scope.category,
                        )
                        if record is None:
                            continue
                        if scope.symbol and scope.symbol not in record.symbols:
                            continue
                        records.append(record)
                    if not self._payload_has_more(
                        payload,
                        row_range=row_range,
                        row_count=len(rows),
                    ):
                        break
                    if window_pages >= max_pages:
                        stop_reason = "max_pages_exhausted"
                        break
                    next_range = self._next_row_range(
                        payload,
                        row_range=row_range,
                        page_size=page_size,
                        max_pages=max_pages,
                    )
                    if next_range <= row_range:
                        stop_reason = "max_pages_exhausted"
                        break
                    row_range = next_range
                    if self.request_interval_seconds:
                        time.sleep(self.request_interval_seconds)
                if stop_reason == "request_failed" or stop_reason == "malformed_payload":
                    break
                if self.request_interval_seconds and stop_reason != "max_pages_exhausted":
                    time.sleep(self.request_interval_seconds)
            if stop_reason in {"request_failed", "malformed_payload"}:
                break

        seen: Dict[str, AnnouncementRecord] = {}
        for record in records:
            seen[record.source_announcement_id] = record
        unique_records = tuple(seen.values())
        if errors and not unique_records:
            status = "failed"
        elif errors:
            status = "degraded"
        elif unique_records:
            status = "success"
        else:
            status = "success_empty"
        incomplete = stop_reason == "max_pages_exhausted"
        return AnnouncementScanResult(
            source=self.source_name,
            query=query.for_source(self.source_name),
            status=status,
            records=unique_records,
            selected_records=unique_records,
            pages_scanned=pages_scanned,
            requests_made=requests_made,
            announcements_seen=len(unique_records),
            max_published_at=max(
                (record.published_at for record in unique_records if record.published_at),
                default=None,
            ),
            is_complete=not errors and not incomplete,
            stop_reason=stop_reason if (errors or incomplete) else "complete",
            errors=tuple(errors),
            diagnostics={
                "markets": list(markets),
                "windows": [
                    [window_start.isoformat(), window_end.isoformat()]
                    for window_start, window_end in windows
                ],
            },
        )

    def _resolve_markets(self, market: Optional[str]) -> Tuple[str, ...]:
        normalized = str(market or "").strip().upper()
        if normalized in {"SEHK", "GEM"}:
            return (normalized,)
        return self.default_markets

    @staticmethod
    def _date_windows(
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> List[Tuple[date, date]]:
        if not start_date or not end_date:
            today = date.today()
            return [(today - timedelta(days=30), today)]
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        if start > end:
            raise AnnouncementQueryNotSupported("hkexnews start_date must be on or before end_date")
        windows: List[Tuple[date, date]] = []
        cursor = start
        while cursor <= end:
            month_end = (cursor.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(
                days=1
            )
            windows.append((cursor, min(month_end, end)))
            cursor = month_end + timedelta(days=1)
        return windows

    def _ensure_warmup(self) -> None:
        if self._warmed_up or not self.warmup_url:
            return
        self.session.get(
            self.warmup_url,
            params={"lang": "EN"},
            timeout=self.request_timeout_seconds,
        )
        self._warmed_up = True
        if self.request_interval_seconds:
            time.sleep(self.request_interval_seconds)

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes"}
        return bool(value)

    @classmethod
    def _payload_record_cnt(cls, payload: Mapping[str, Any]) -> Optional[int]:
        raw = payload.get("recordCnt")
        if raw in (None, ""):
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _payload_has_more(
        cls,
        payload: Mapping[str, Any],
        *,
        row_range: int,
        row_count: int,
    ) -> bool:
        if cls._truthy(payload.get("hasNextPage")) or cls._truthy(payload.get("hasNextRow")):
            return True
        record_cnt = cls._payload_record_cnt(payload)
        if record_cnt is None:
            return False
        return record_cnt > max(row_range, row_count)

    @classmethod
    def _next_row_range(
        cls,
        payload: Mapping[str, Any],
        *,
        row_range: int,
        page_size: int,
        max_pages: int,
    ) -> int:
        ceiling = page_size * max(1, max_pages)
        record_cnt = cls._payload_record_cnt(payload)
        if record_cnt is not None and record_cnt > row_range:
            return min(record_cnt, ceiling)
        return min(row_range + page_size, ceiling)

    def _request_search(
        self,
        *,
        market: str,
        start_date: date,
        end_date: date,
        page_size: int,
        keyword: Optional[str],
        category_options: Mapping[str, Any],
        stock_id: str,
    ) -> Dict[str, Any]:
        params = {
            "lang": "EN",
            "category": "0",
            "market": market,
            "stockId": stock_id or "-1",
            "fromDate": start_date.strftime("%Y%m%d"),
            "toDate": end_date.strftime("%Y%m%d"),
            "title": keyword or "",
            "searchType": "0",
            "documentType": "-1",
            "sortDir": "0",
            "sortByOptions": "DateTime",
            "rowRange": str(page_size),
        }
        params.update(
            {
                key: str(value)
                for key, value in category_options.items()
                if value not in (None, "")
            }
        )
        last_exc: Optional[Exception] = None
        for attempt in range(self.retry_attempts + 1):
            try:
                response = self.session.get(
                    self.endpoint_url,
                    params=params,
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("hkexnews title-search response is not an object")
                return payload
            except Exception as exc:
                last_exc = exc
                if attempt >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds:
                    time.sleep(self.retry_backoff_seconds * (attempt + 1))
        raise RuntimeError(last_exc)

    @staticmethod
    def _extract_rows(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
        raw_result = payload.get("result")
        if raw_result in (None, ""):
            return []
        if isinstance(raw_result, str):
            parsed = json.loads(raw_result)
        else:
            parsed = raw_result
        if parsed in (None, ""):
            return []
        if not isinstance(parsed, list):
            raise ValueError("hkexnews title-search result is not a list")
        rows: List[Dict[str, Any]] = []
        for index, item in enumerate(parsed):
            if not isinstance(item, Mapping):
                raise ValueError(f"hkexnews title-search result[{index}] is not an object")
            rows.append(dict(item))
        return rows

    def _normalize_record(
        self,
        row: Mapping[str, Any],
        *,
        market: str,
        headline_category: Optional[str],
    ) -> Optional[AnnouncementRecord]:
        title = str(row.get("TITLE") or "").strip()
        if not title:
            return None
        published_raw = str(row.get("DATE_TIME") or "").strip() or None
        symbols = self._stock_codes(row.get("STOCK_CODE"))
        raw_id = str(row.get("NEWS_ID") or "").strip()
        raw_url = str(row.get("FILE_LINK") or "").strip()
        resolved_url = urljoin(self.artifact_base_url, raw_url) if raw_url else None
        identity_is_derived = not bool(raw_id)
        source_id = raw_id or build_derived_announcement_id(
            source=self.source_name,
            title=title,
            published_at_raw=published_raw,
            symbols=symbols,
            source_urls=[resolved_url] if resolved_url else [],
        )
        published_at, diagnostics = normalize_published_at(
            published_raw,
            source_timezone=HONG_KONG_TZ,
        )
        if identity_is_derived:
            diagnostics.append("announcement_identity_derived")
        attachments: Tuple[AnnouncementAttachment, ...] = ()
        if raw_url:
            attachments = (
                AnnouncementAttachment(
                    source_url=raw_url,
                    resolved_url=resolved_url,
                    file_extension=raw_url.rsplit(".", 1)[-1] if "." in raw_url else None,
                    raw_metadata={"source_row": dict(row)},
                ),
            )
        payload = dict(row)
        if headline_category:
            payload["headline_category"] = headline_category
        names = tuple(
            name
            for name in (str(row.get("STOCK_NAME") or "").strip(),)
            if name
        )
        return AnnouncementRecord(
            source=self.source_name,
            source_announcement_id=source_id,
            announcement_key=build_announcement_key(self.source_name, source_id),
            title=title,
            published_at=published_at,
            published_at_raw=published_raw,
            exchange="HKEX",
            market=market,
            symbols=tuple(symbols),
            security_names=names,
            attachments=attachments,
            raw_payload=payload,
            diagnostics=tuple(diagnostics),
            identity_is_derived=identity_is_derived,
        )

    @classmethod
    def _stock_codes(cls, value: Any) -> List[str]:
        text = str(value or "").strip()
        if not text:
            return []
        codes: List[str] = []
        for token in _STOCK_CODE_SPLIT_RE.split(text):
            digits = "".join(ch for ch in token if ch.isdigit())
            if not digits:
                continue
            code = digits.zfill(5)
            if code not in codes:
                codes.append(code)
        return codes
