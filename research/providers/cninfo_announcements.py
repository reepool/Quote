"""
Reusable CNInfo announcement metadata scanner.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional
from urllib.parse import urljoin

import requests

from research.announcements.base import AnnouncementProviderCapabilities
from research.announcements.models import (
    AnnouncementAttachment,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
    normalize_published_at,
)
from utils.http_transport import HttpTlsConfig, create_requests_session

LOGGER = logging.getLogger(__name__)

_CNINFO_ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
_CNINFO_TOP_SEARCH_URL = "https://www.cninfo.com.cn/new/information/topSearch/query"
_CNINFO_MAX_PAGE_SIZE = 30
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
class _CninfoRawRecord:
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
class _CninfoRequest:
    """Configuration for one market/column announcement scan."""

    purpose_key: str
    market: str
    column: str
    plate: Optional[str] = None
    tab_name: str = "fulltext"
    category: Optional[str] = None
    search_key: Optional[str] = None
    stock: Optional[str] = None
    org_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    page_size: int = 30
    max_pages: int = 20
    stop_at_watermark: Optional[str] = None


@dataclass(frozen=True)
class _CninfoRawScanResult:
    """Result of one reusable CNInfo announcement scan."""

    config: _CninfoRequest
    records: List[_CninfoRawRecord]
    selected_records: List[_CninfoRawRecord]
    pages_scanned: int
    announcements_seen: int
    max_announcement_time: Optional[str]
    stopped_at_watermark: bool = False
    is_complete: bool = False
    stop_reason: Optional[str] = None
    errors: List[str] = field(default_factory=list)


_AnnouncementFilter = Callable[[_CninfoRawRecord], List[str]]


class _CninfoTransport:
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

    def resolve_stock_identity(self, symbol: str) -> Optional[Dict[str, str]]:
        """Resolve the CNInfo org id required for per-stock announcement scans."""
        normalized_symbol = str(symbol or "").strip().zfill(6)
        if not normalized_symbol.isdigit() or len(normalized_symbol) != 6:
            return None
        last_exc: Optional[Exception] = None
        for attempt in range(self.retry_attempts + 1):
            try:
                response = self.session.post(
                    _CNINFO_TOP_SEARCH_URL,
                    data={"keyWord": normalized_symbol, "maxNum": "10"},
                    headers={
                        **_CNINFO_ANNOUNCEMENT_HEADERS,
                        "Referer": (
                            "https://www.cninfo.com.cn/new/disclosure/stock"
                            f"?stockCode={normalized_symbol}"
                        ),
                    },
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("CNInfo top-search response is not a list")
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    code = str(item.get("code") or "").strip().zfill(6)
                    org_id = str(item.get("orgId") or "").strip()
                    if code == normalized_symbol and org_id:
                        return {
                            "symbol": code,
                            "org_id": org_id,
                            "stock": f"{code},{org_id}",
                        }
                return None
            except Exception as exc:
                last_exc = exc
                if attempt >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * (attempt + 1))
        raise RuntimeError(f"CNInfo stock identity request failed: {last_exc}")

    def scan(
        self,
        config: _CninfoRequest,
        *,
        filters: Optional[Iterable[_AnnouncementFilter]] = None,
    ) -> _CninfoRawScanResult:
        """Scan one CNInfo market/column and return normalized records."""
        selected: List[_CninfoRawRecord] = []
        records: List[_CninfoRawRecord] = []
        errors: List[str] = []
        max_time: Optional[str] = None
        stopped_at_watermark = False
        filter_list = list(filters or [])
        pages_scanned = 0
        effective_page_size = self._effective_page_size(config.page_size)
        is_complete = False
        stop_reason = "max_pages_exhausted"

        for page_num in range(1, max(1, config.max_pages) + 1):
            payload: Optional[Dict[str, Any]] = None
            page_started = time.monotonic()
            LOGGER.info(
                "CNInfo announcement page started: purpose=%s market=%s page=%s/%s effective_page_size=%s",
                config.purpose_key,
                config.market,
                page_num,
                max(1, config.max_pages),
                effective_page_size,
            )
            try:
                payload = self._request_page(config, page_num)
            except Exception as exc:
                errors.append(str(exc))
                stop_reason = "request_failed"
                LOGGER.warning(
                    "[CninfoAnnouncements] Page request failed: purpose=%s market=%s column=%s page=%s error=%s",
                    config.purpose_key,
                    config.market,
                    config.column,
                    page_num,
                    exc,
                )
                break

            try:
                raw_records = self._extract_records(payload)
            except ValueError as exc:
                errors.append(str(exc))
                stop_reason = "malformed_payload"
                LOGGER.warning(
                    "[CninfoAnnouncements] Page payload malformed: purpose=%s market=%s column=%s page=%s error=%s",
                    config.purpose_key,
                    config.market,
                    config.column,
                    page_num,
                    exc,
                )
                break
            page_records = [
                self._normalize_record(item, config) for item in raw_records
            ]
            page_records = [record for record in page_records if record is not None]
            if raw_records and not page_records:
                errors.append(
                    "CNInfo announcement rows could not be normalized"
                )
                stop_reason = "malformed_payload"
                LOGGER.warning(
                    "[CninfoAnnouncements] Page rows unrecognized: purpose=%s market=%s column=%s page=%s rows=%s",
                    config.purpose_key,
                    config.market,
                    config.column,
                    page_num,
                    len(raw_records),
                )
                break
            pages_scanned += 1
            LOGGER.info(
                "CNInfo announcement page completed: purpose=%s market=%s page=%s records=%s elapsed=%.3f",
                config.purpose_key,
                config.market,
                page_num,
                len(page_records),
                time.monotonic() - page_started,
            )
            if not page_records:
                is_complete = True
                stop_reason = "empty_page"
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
                        _CninfoRawRecord(
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
                is_complete = True
                stop_reason = "watermark_reached"
                break
            if len(page_records) < effective_page_size:
                is_complete = True
                stop_reason = "last_page"
                break
            if self.request_interval_seconds > 0:
                time.sleep(self.request_interval_seconds)

        return _CninfoRawScanResult(
            config=config,
            records=records,
            selected_records=selected,
            pages_scanned=pages_scanned,
            announcements_seen=len(records),
            max_announcement_time=max_time,
            stopped_at_watermark=stopped_at_watermark,
            is_complete=is_complete,
            stop_reason=stop_reason,
            errors=errors,
        )

    def _request_page(
        self,
        config: _CninfoRequest,
        page_num: int,
    ) -> Dict[str, Any]:
        body = {
            "pageNum": str(page_num),
            "pageSize": str(self._effective_page_size(config.page_size)),
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
        if config.stock:
            body["stock"] = config.stock
        if config.org_id:
            body["orgId"] = config.org_id
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

    @staticmethod
    def _effective_page_size(page_size: int) -> int:
        """Respect CNInfo's effective 30-row page limit."""
        return min(_CNINFO_MAX_PAGE_SIZE, max(1, int(page_size)))

    @classmethod
    def _extract_records(cls, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        for key in ("announcements", "records", "rows"):
            if key not in payload:
                continue
            candidate = payload[key]
            if not isinstance(candidate, list):
                raise ValueError(
                    f"CNInfo announcement container {key} is not a list"
                )
            return cls._dict_records(candidate, container=key)

        data = payload.get("data")
        if "data" in payload and isinstance(data, list):
            return cls._dict_records(data, container="data")
        if isinstance(data, dict):
            for key in ("announcements", "records", "rows"):
                if key not in data:
                    continue
                candidate = data[key]
                if not isinstance(candidate, list):
                    raise ValueError(
                        f"CNInfo announcement container data.{key} is not a list"
                    )
                return cls._dict_records(candidate, container=f"data.{key}")
        elif "data" in payload and data is not None:
            raise ValueError("CNInfo announcement container data has unsupported shape")

        if "classifiedAnnouncements" in payload:
            classified = payload["classifiedAnnouncements"]
            if not isinstance(classified, list):
                raise ValueError(
                    "CNInfo announcement container classifiedAnnouncements is not a list"
                )
            flattened: List[Dict[str, Any]] = []
            for index, group in enumerate(classified):
                if not isinstance(group, dict):
                    raise ValueError(
                        "CNInfo classified announcement group is not an object: "
                        f"index={index}"
                    )
                announcements = group.get("announcements")
                if not isinstance(announcements, list):
                    raise ValueError(
                        "CNInfo classified announcement group has no list container: "
                        f"index={index}"
                    )
                flattened.extend(
                    cls._dict_records(
                        announcements,
                        container=f"classifiedAnnouncements[{index}].announcements",
                    )
                )
            return flattened

        raise ValueError("CNInfo announcement response has no supported record container")

    @staticmethod
    def _dict_records(value: List[Any], *, container: str) -> List[Dict[str, Any]]:
        if any(not isinstance(item, dict) for item in value):
            raise ValueError(
                f"CNInfo announcement container {container} contains a non-object row"
            )
        return [dict(item) for item in value]

    @classmethod
    def _normalize_record(
        cls,
        row: Dict[str, Any],
        config: _CninfoRequest,
    ) -> Optional[_CninfoRawRecord]:
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
        return _CninfoRawRecord(
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
            return _CninfoTransport._normalize_time(int(text))
        return text

    @staticmethod
    def _page_reached_watermark(
        records: List[_CninfoRawRecord],
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


class CninfoAnnouncementProvider:
    """CNInfo implementation of the source-neutral announcement contract."""

    source_name = "cninfo"
    capabilities = AnnouncementProviderCapabilities(
        exchanges=frozenset({"SSE", "SZSE", "BSE"}),
        supports_market_scope=True,
        supports_instrument_scope=True,
        supports_date_filter=True,
        supports_keyword_filter=True,
        supports_category_filter=True,
        cursor_kind="published_at",
        max_page_size=_CNINFO_MAX_PAGE_SIZE,
        supports_attachment_retrieval=True,
        requires_provider_identity=True,
    )

    DEFAULT_MARKETS: Dict[str, Dict[str, str]] = {
        "SSE": {"market": "SSE", "column": "sse", "plate": "sh", "tab_name": "fulltext"},
        "SZSE": {"market": "SZSE", "column": "szse", "plate": "sz", "tab_name": "fulltext"},
        "BSE": {"market": "BSE", "column": "neeq", "plate": "bj", "tab_name": "fulltext"},
    }

    def __init__(
        self,
        *,
        source_config: Optional[Mapping[str, Any]] = None,
        transport: Optional[_CninfoTransport] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        config = dict(source_config or {})
        self.source_config = config
        self.market_configs = {
            **self.DEFAULT_MARKETS,
            **{
                str(exchange).upper(): dict(value)
                for exchange, value in (config.get("markets") or {}).items()
                if isinstance(value, Mapping)
            },
        }
        self.artifact_base_url = str(
            config.get("artifact_base_url") or "https://static.cninfo.com.cn/"
        ).strip()
        self.transport = transport or _CninfoTransport(
            url=str(config.get("endpoint_url") or _CNINFO_ANNOUNCEMENT_URL),
            request_timeout_seconds=float(config.get("request_timeout_seconds", 20.0)),
            request_interval_seconds=float(config.get("request_interval_seconds", 0.2)),
            retry_attempts=int(config.get("retry_attempts", 2)),
            retry_backoff_seconds=float(config.get("retry_backoff_seconds", 0.5)),
            session=session,
        )

    def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
        """Discover one bounded CNInfo query and normalize its evidence."""
        self.capabilities.validate(query)
        scope = query.scope
        market_config = self.market_configs.get(scope.exchange)
        if market_config is None:
            raise ValueError(f"CNInfo market config is missing: {scope.exchange}")

        identity: Optional[Dict[str, str]] = None
        if scope.is_instrument_scoped:
            if not scope.symbol:
                return AnnouncementScanResult(
                    source=self.source_name,
                    query=query,
                    status="identity_not_found",
                    is_complete=False,
                    stop_reason="symbol_missing",
                    errors=("instrument-scoped CNInfo query requires symbol",),
                )
            identity = self.transport.resolve_stock_identity(scope.symbol)
            if not identity:
                return AnnouncementScanResult(
                    source=self.source_name,
                    query=query,
                    status="identity_not_found",
                    is_complete=False,
                    stop_reason="provider_identity_not_found",
                    errors=("cninfo_stock_identity_not_found",),
                    diagnostics={"symbol": scope.symbol},
                )

        raw_result = self.transport.scan(
            _CninfoRequest(
                purpose_key=query.purpose_key,
                market=str(market_config.get("market") or scope.market or scope.exchange),
                column=str(market_config.get("column") or "").strip(),
                plate=self._text(market_config.get("plate")),
                tab_name=str(market_config.get("tab_name") or "fulltext"),
                category=scope.category,
                search_key=scope.keyword,
                stock=None if identity is None else identity.get("stock"),
                org_id=None if identity is None else identity.get("org_id"),
                start_date=scope.start_date,
                end_date=scope.end_date,
                page_size=min(scope.page_size, self.capabilities.max_page_size),
                max_pages=scope.max_pages,
                stop_at_watermark=(
                    None if scope.cursor is None else scope.cursor.value
                ),
            )
        )
        records = tuple(
            record
            for item in raw_result.records
            if (record := self._normalize_record(item, exchange=scope.exchange)) is not None
        )
        if raw_result.errors:
            status = "degraded" if records else "failed"
        elif not raw_result.is_complete:
            status = "degraded" if records else "indeterminate"
        elif not records:
            status = "success_empty"
        else:
            status = "success"
        max_published_at = max(
            (record.published_at for record in records if record.published_at),
            default=None,
        )
        provider_cursor = (
            ProviderCursor(kind="published_at", value=max_published_at)
            if max_published_at and raw_result.is_complete
            else None
        )
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status=status,
            records=records,
            pages_scanned=raw_result.pages_scanned,
            requests_made=raw_result.pages_scanned,
            announcements_seen=raw_result.announcements_seen,
            max_published_at=max_published_at,
            provider_cursor=provider_cursor,
            is_complete=raw_result.is_complete and not raw_result.errors,
            reached_prior_cursor=raw_result.stopped_at_watermark,
            stop_reason=raw_result.stop_reason,
            errors=tuple(raw_result.errors),
            diagnostics={
                "effective_page_size": min(
                    scope.page_size,
                    self.capabilities.max_page_size,
                ),
                "market_config": dict(market_config),
                "identity": identity or {},
            },
        )

    def _normalize_record(
        self,
        record: _CninfoRawRecord,
        *,
        exchange: str,
    ) -> Optional[AnnouncementRecord]:
        if not record.announcement_id or not record.title:
            return None
        published_at, time_diagnostics = normalize_published_at(
            record.announcement_time
        )
        attachments: List[AnnouncementAttachment] = []
        if record.adjunct_url:
            resolved_url = (
                record.adjunct_url
                if record.adjunct_url.startswith(("http://", "https://"))
                else urljoin(self.artifact_base_url, record.adjunct_url)
            )
            extension = self._text(record.adjunct_type)
            if not extension and "." in record.adjunct_url:
                extension = record.adjunct_url.rsplit(".", 1)[-1].split("?", 1)[0]
            attachments.append(
                AnnouncementAttachment(
                    source_url=record.adjunct_url,
                    resolved_url=resolved_url,
                    media_type=(
                        "application/pdf"
                        if extension and "pdf" in extension.lower()
                        else None
                    ),
                    file_extension=extension,
                    raw_metadata={
                        "adjunct_type": record.adjunct_type,
                    },
                )
            )
        source_id = str(record.announcement_id).strip()
        return AnnouncementRecord(
            source=self.source_name,
            source_announcement_id=source_id,
            announcement_key=build_announcement_key(self.source_name, source_id),
            title=record.title,
            published_at=published_at,
            published_at_raw=record.announcement_time,
            exchange=exchange,
            market=record.market,
            symbols=tuple(record.symbols),
            security_names=tuple(record.sec_names),
            organization_ids=tuple(record.org_ids),
            attachments=tuple(attachments),
            raw_payload=dict(record.raw_payload),
            diagnostics=tuple(time_diagnostics),
        )

    @staticmethod
    def _text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None
