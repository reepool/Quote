"""Reusable SSE, SZSE, and BSE official announcement providers."""

from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urljoin

import requests

from research.announcements.base import (
    AnnouncementProviderCapabilities,
    AnnouncementQueryNotSupported,
)
from research.announcements.categories import exchange_category_options
from research.announcements.models import (
    AnnouncementAttachment,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
    build_derived_announcement_id,
    normalize_published_at,
)
from utils.http_transport import HttpTlsConfig, create_requests_session


LOGGER = logging.getLogger(__name__)
SUPPORTED_EXCHANGE_SOURCES = {"SSE": "sse", "SZSE": "szse", "BSE": "bse"}
_JSONP_RE = re.compile(r"^[A-Za-z_$][\w$.]*\s*\((.*)\)\s*;?\s*$", re.S)


@dataclass(frozen=True)
class OfficialExchangeAnnouncementSourceConfig:
    """Validated endpoint and transport settings for one official exchange."""

    exchange: str
    source: str
    endpoint_url: str
    method: str
    referer: str
    artifact_base_url: str
    enabled: bool = True
    request_timeout_seconds: float = 20.0
    request_interval_seconds: float = 0.2
    retry_attempts: int = 2
    retry_backoff_seconds: float = 0.5
    max_page_size: int = 100
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(
        cls,
        exchange: str,
        value: Mapping[str, Any],
    ) -> "OfficialExchangeAnnouncementSourceConfig":
        normalized_exchange = str(exchange or "").strip().upper()
        expected_source = SUPPORTED_EXCHANGE_SOURCES.get(normalized_exchange)
        if expected_source is None:
            raise ValueError(f"unsupported official announcement exchange: {exchange}")
        source = str(value.get("source") or expected_source).strip().lower()
        if source != expected_source:
            raise ValueError(
                f"{normalized_exchange} official announcement source must be {expected_source}"
            )
        endpoint_url = str(value.get("endpoint_url") or "").strip()
        referer = str(value.get("referer") or "").strip()
        artifact_base_url = str(value.get("artifact_base_url") or "").strip()
        if not endpoint_url or not referer or not artifact_base_url:
            raise ValueError(
                f"{normalized_exchange} endpoint, referer, and artifact base URL are required"
            )
        method = str(value.get("method") or "GET").strip().upper()
        if method not in {"GET", "POST"}:
            raise ValueError("official announcement method must be GET or POST")
        return cls(
            exchange=normalized_exchange,
            source=source,
            endpoint_url=endpoint_url,
            method=method,
            referer=referer,
            artifact_base_url=artifact_base_url,
            enabled=bool(value.get("enabled", True)),
            request_timeout_seconds=max(
                1.0,
                float(value.get("request_timeout_seconds", 20.0)),
            ),
            request_interval_seconds=max(
                0.0,
                float(value.get("request_interval_seconds", 0.2)),
            ),
            retry_attempts=max(0, int(value.get("retry_attempts", 2))),
            retry_backoff_seconds=max(
                0.0,
                float(value.get("retry_backoff_seconds", 0.5)),
            ),
            max_page_size=max(1, int(value.get("max_page_size", 100))),
            options=dict(value.get("options") or {}),
        )


class OfficialExchangeAnnouncementProvider:
    """Normalize one official exchange endpoint without business classification."""

    def __init__(
        self,
        config: OfficialExchangeAnnouncementSourceConfig,
        *,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not config.enabled:
            raise ValueError(f"official announcement provider is disabled: {config.source}")
        self.config = config
        self.source_name = config.source
        self.endpoint_mode = str(
            config.options.get("endpoint_mode") or "instrument"
        ).strip().lower()
        if self.endpoint_mode not in {"instrument", "recent_market"}:
            raise ValueError(
                "official announcement endpoint_mode must be instrument or "
                "recent_market"
            )
        supports_market_scope = config.exchange == "SZSE" or (
            config.exchange == "BSE" and self.endpoint_mode == "recent_market"
        )
        supports_instrument_scope = not (
            config.exchange == "BSE" and self.endpoint_mode == "recent_market"
        )
        self.capabilities = AnnouncementProviderCapabilities(
            exchanges=frozenset({config.exchange}),
            supports_market_scope=supports_market_scope,
            supports_instrument_scope=supports_instrument_scope,
            supports_date_filter=True,
            supports_keyword_filter=True,
            supports_category_filter=config.exchange in {"SSE", "SZSE"},
            cursor_kind="published_at",
            max_page_size=config.max_page_size,
            supports_attachment_retrieval=True,
        )
        self.session = session or create_requests_session(
            tls_config=HttpTlsConfig(source_name=config.source),
            headers={
                "Accept": "application/json, text/plain, */*",
                "Referer": config.referer,
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                ),
            },
        )

    def discover(self, query: AnnouncementQuery) -> AnnouncementScanResult:
        self.capabilities.validate(query)
        scope = query.scope
        if not scope.symbol and not self.capabilities.supports_market_scope:
            raise ValueError(f"{self.source_name} instrument query requires symbol")
        page_size = min(scope.page_size, self.capabilities.max_page_size)
        records: List[AnnouncementRecord] = []
        errors: List[str] = []
        pages_scanned = 0
        stop_reason = "max_pages_exhausted"
        is_complete = False
        reached_prior_cursor = False

        for page_num in range(1, scope.max_pages + 1):
            page_started = time.monotonic()
            LOGGER.info(
                "official announcement page started: source=%s symbol=%s page=%s/%s effective_page_size=%s",
                self.source_name,
                scope.symbol,
                page_num,
                scope.max_pages,
                page_size,
            )
            try:
                payload = self._request_page(
                    symbol=scope.symbol,
                    page_num=page_num,
                    page_size=page_size,
                    start_date=scope.start_date,
                    end_date=scope.end_date,
                    keyword=scope.keyword,
                    category=scope.category,
                )
            except Exception as exc:
                errors.append(
                    f"{self.source_name} page {page_num} request failed: {exc}"
                )
                stop_reason = "request_failed"
                LOGGER.warning(
                    "official announcement page failed: source=%s symbol=%s page=%s error=%s",
                    self.source_name,
                    scope.symbol,
                    page_num,
                    exc,
                )
                break

            try:
                rows = self._extract_rows(payload)
            except ValueError as exc:
                errors.append(
                    f"{self.source_name} page {page_num} malformed payload: {exc}"
                )
                stop_reason = "malformed_payload"
                LOGGER.warning(
                    "official announcement payload malformed: source=%s symbol=%s page=%s error=%s",
                    self.source_name,
                    scope.symbol,
                    page_num,
                    exc,
                )
                break
            pages_scanned += 1
            normalized_page_records = [
                record
                for row in rows
                if (
                    record := self._normalize_record(
                        row,
                        expected_symbol=(
                            None
                            if self.config.exchange == "BSE"
                            and self.endpoint_mode == "recent_market"
                            else scope.symbol
                        ),
                    )
                )
                is not None
            ]
            if rows and not normalized_page_records:
                errors.append(
                    f"{self.source_name} page {page_num} rows could not be normalized"
                )
                stop_reason = "malformed_payload"
                LOGGER.warning(
                    "official announcement rows unrecognized: source=%s symbol=%s page=%s rows=%s",
                    self.source_name,
                    scope.symbol,
                    page_num,
                    len(rows),
                )
                break
            page_records = self._apply_local_filters(
                normalized_page_records,
                keyword=scope.keyword,
                symbol=scope.symbol,
                start_date=scope.start_date,
                end_date=scope.end_date,
            )
            records.extend(page_records)
            LOGGER.info(
                "official announcement page completed: source=%s symbol=%s page=%s rows=%s normalized=%s elapsed=%.3f",
                self.source_name,
                scope.symbol,
                page_num,
                len(rows),
                len(page_records),
                time.monotonic() - page_started,
            )
            if self._page_reached_cursor(normalized_page_records, scope.cursor):
                reached_prior_cursor = True
                is_complete = True
                stop_reason = "watermark_reached"
                break
            if self._page_reached_start_date(
                normalized_page_records, scope.start_date
            ):
                is_complete = True
                stop_reason = "requested_start_date_reached"
                break
            page_count = self._page_count(
                payload,
                page_num=page_num,
                page_size=page_size,
                row_count=len(rows),
            )
            if page_num >= page_count:
                is_complete = True
                stop_reason = "last_page" if rows else "empty_page"
                break
            if self.config.request_interval_seconds > 0:
                time.sleep(self.config.request_interval_seconds)

        records = self._deduplicate_records(records)
        if errors:
            status = "degraded" if records else "failed"
        elif not is_complete:
            status = "degraded" if records else "indeterminate"
        elif not records:
            status = "success_empty"
        else:
            status = "success"
        max_published_at = max(
            (record.published_at for record in records if record.published_at),
            default=None,
        )
        cursor = (
            ProviderCursor(kind="published_at", value=max_published_at)
            if is_complete and not errors and max_published_at
            else None
        )
        return AnnouncementScanResult(
            source=self.source_name,
            query=query,
            status=status,
            records=tuple(records),
            pages_scanned=pages_scanned,
            requests_made=pages_scanned + len(errors),
            announcements_seen=len(records),
            max_published_at=max_published_at,
            provider_cursor=cursor,
            is_complete=is_complete and not errors,
            reached_prior_cursor=reached_prior_cursor,
            stop_reason=stop_reason,
            errors=tuple(errors),
            diagnostics={
                "effective_page_size": page_size,
                "endpoint_url": self.config.endpoint_url,
                "endpoint_mode": self.endpoint_mode,
                "requested_start_date": scope.start_date,
                "requested_end_date": scope.end_date,
                "observed_earliest_published_at": min(
                    (
                        record.published_at
                        for record in records
                        if record.published_at
                    ),
                    default=None,
                ),
                "keyword_filter_mode": (
                    "local_exact"
                    if self.config.exchange in {"SSE", "BSE"} and scope.keyword
                    else "upstream" if scope.keyword else "none"
                ),
            },
        )

    def _request_page(
        self,
        *,
        symbol: Optional[str],
        page_num: int,
        page_size: int,
        start_date: Optional[str],
        end_date: Optional[str],
        keyword: Optional[str],
        category: Optional[str],
    ) -> Dict[str, Any]:
        kwargs = self._request_kwargs(
            symbol=symbol,
            page_num=page_num,
            page_size=page_size,
            start_date=start_date,
            end_date=end_date,
            keyword=keyword,
            category=category,
        )
        last_exc: Optional[Exception] = None
        for attempt in range(self.config.retry_attempts + 1):
            try:
                response = self.session.request(
                    self.config.method,
                    self.config.endpoint_url,
                    timeout=self.config.request_timeout_seconds,
                    **kwargs,
                )
                response.raise_for_status()
                return self._response_payload(response)
            except Exception as exc:
                last_exc = exc
                if attempt >= self.config.retry_attempts:
                    break
                if self.config.retry_backoff_seconds > 0:
                    time.sleep(self.config.retry_backoff_seconds * (attempt + 1))
        raise RuntimeError(last_exc)

    def _request_kwargs(
        self,
        *,
        symbol: Optional[str],
        page_num: int,
        page_size: int,
        start_date: Optional[str],
        end_date: Optional[str],
        keyword: Optional[str],
        category: Optional[str],
    ) -> Dict[str, Any]:
        category_options = exchange_category_options(self.config.exchange, category)
        if category and category_options is None:
            raise AnnouncementQueryNotSupported(
                f"{self.source_name} does not support announcement category {category}"
            )
        category_options = category_options or {}
        if self.config.exchange == "SSE":
            return {
                "params": {
                    "isPagination": "true",
                    "pageHelp.pageSize": str(page_size),
                    "pageHelp.pageNo": str(page_num),
                    "pageHelp.beginPage": str(page_num),
                    "pageHelp.cacheSize": "1",
                    "pageHelp.endPage": str(page_num),
                    "productId": symbol,
                    "securityType": self.config.options.get(
                        "security_type",
                        "0101,120100,020100,020200,120200",
                    ),
                    "reportType2": category_options.get(
                        "report_type2",
                        self.config.options.get("report_type2", "DQBG"),
                    ),
                    "reportType": category_options.get(
                        "report_type",
                        self.config.options.get("report_type", "ALL"),
                    ),
                    "beginDate": start_date or "",
                    "endDate": end_date or "",
                }
            }
        if self.config.exchange == "SZSE":
            body: Dict[str, Any] = {
                "stock": [symbol] if symbol else [],
                "channelCode": [self.config.options.get("channel_code", "fixed_disc")],
                "pageSize": page_size,
                "pageNum": page_num,
            }
            if start_date and end_date:
                body["seDate"] = [start_date, end_date]
            if keyword:
                body["keyword"] = keyword
            if category_options.get("big_category_id"):
                body["bigCategoryId"] = list(category_options["big_category_id"])
            return {"json": body}
        if self.config.exchange == "BSE":
            if self.endpoint_mode == "recent_market":
                need_fields = self.config.options.get("need_fields") or [
                    "companyCd", "companyName", "disclosureTitle",
                    "disclosurePostTitle", "destFilePath", "publishDate",
                    "xxfcbj", "fileExt", "xxzrlx",
                ]
                form_fields: List[tuple[str, Any]] = [
                    ("siteId", str(self.config.options.get("site_id", 6))),
                    ("flag", str(self.config.options.get("flag", 0))),
                    ("page", str(page_num - 1)),
                    ("companyCd", symbol or ""),
                    ("isNewThree", str(
                        self.config.options.get("is_new_three", "1")
                    )),
                    ("startTime", start_date or ""),
                    ("endTime", end_date or ""),
                    ("keyword", keyword or ""),
                    ("hyType", ""),
                ]
                for value in self.config.options.get("disclosure_type", []):
                    form_fields.append(("disclosureType[]", value))
                for value in self.config.options.get(
                    "disclosure_subtype", []
                ):
                    form_fields.append(("disclosureSubtype[]", value))
                for value in self.config.options.get("xxfcbj", ["2"]):
                    form_fields.append(("xxfcbj[]", value))
                for value in need_fields:
                    form_fields.append(("needFields[]", value))
                return {
                    "data": form_fields,
                }
            return {
                "data": {
                    "page": str(page_num - 1),
                    "companyCd": symbol,
                    "isNewThree": str(self.config.options.get("is_new_three", "1")),
                    "xxfcbj": str(self.config.options.get("xxfcbj", "1")),
                    "keyword": keyword or "",
                    "startTime": start_date or "",
                    "endTime": end_date or "",
                }
            }
        raise ValueError(f"unsupported exchange: {self.config.exchange}")

    @staticmethod
    def _response_payload(response: Any) -> Dict[str, Any]:
        try:
            payload = response.json()
        except Exception:
            text = str(getattr(response, "text", "") or "").strip()
            match = _JSONP_RE.match(text)
            if match:
                text = match.group(1)
            payload = json.loads(text)
        if (
            isinstance(payload, list)
            and len(payload) == 1
            and isinstance(payload[0], Mapping)
        ):
            payload = payload[0]
        if not isinstance(payload, dict):
            raise ValueError("exchange announcement response is not an object")
        return payload

    def _extract_rows(self, payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
        if self.config.exchange == "SSE":
            return self._required_dict_rows(payload, "result")
        if self.config.exchange == "SZSE":
            return self._required_dict_rows(payload, "data")
        if self.endpoint_mode == "recent_market":
            data = payload.get("data")
            if not isinstance(data, Mapping):
                raise ValueError(
                    "exchange announcement container data is not an object"
                )
            content = data.get("content")
            if not isinstance(content, list):
                raise ValueError(
                    "exchange announcement container data.content is not a list"
                )
            rows: List[Dict[str, Any]] = []
            for index, group in enumerate(content):
                if not isinstance(group, Mapping):
                    raise ValueError(
                        "exchange announcement data.content contains a "
                        "non-object row"
                    )
                disclosures = group.get("disclosures")
                if not isinstance(disclosures, list):
                    raise ValueError(
                        "exchange announcement container "
                        f"data.content[{index}].disclosures is not a list"
                    )
                rows.extend(self._dict_rows(
                    disclosures,
                    container=f"data.content[{index}].disclosures",
                ))
            return rows
        candidates: List[tuple[str, Any]] = [
            (key, payload[key])
            for key in ("content", "data", "rows")
            if key in payload
        ]
        list_info = payload.get("listInfo")
        if isinstance(list_info, Mapping):
            candidates.extend(
                (f"listInfo.{key}", list_info[key])
                for key in ("content", "rows", "data")
                if key in list_info
            )
        elif "listInfo" in payload and list_info is not None:
            raise ValueError("exchange announcement container listInfo is not an object")
        for container, candidate in candidates:
            if not isinstance(candidate, list):
                continue
            return self._dict_rows(candidate, container=container)
        if candidates:
            containers = ",".join(container for container, _candidate in candidates)
            raise ValueError(
                "exchange announcement containers have unsupported shapes: "
                f"{containers}"
            )
        raise ValueError("exchange announcement response has no supported row container")

    def _apply_local_filters(
        self,
        records: List[AnnouncementRecord],
        *,
        keyword: Optional[str],
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[AnnouncementRecord]:
        output = records
        if symbol:
            output = [record for record in output if symbol in record.symbols]
        if keyword and self.config.exchange in {"SSE", "BSE"}:
            output = [record for record in output if keyword in record.title]
        if self.config.exchange == "BSE" and self.endpoint_mode == "recent_market":
            if start_date:
                output = [
                    record for record in output
                    if self._published_date(record) is not None
                    and self._published_date(record) >= date.fromisoformat(start_date)
                ]
            if end_date:
                output = [
                    record for record in output
                    if self._published_date(record) is not None
                    and self._published_date(record) <= date.fromisoformat(end_date)
                ]
        return output

    def _normalize_record(
        self,
        row: Mapping[str, Any],
        *,
        expected_symbol: Optional[str],
    ) -> Optional[AnnouncementRecord]:
        if self.config.exchange == "SSE":
            title = self._text(row.get("TITLE"))
            published_raw = self._text(row.get("SSEDATE"))
            raw_url = self._text(row.get("URL"))
            symbols = self._values(row.get("SECURITY_CODE"))
            raw_id = self._text(row.get("BULLETIN_ID") or row.get("ANNOUNCEMENT_ID"))
            attachment_type = self._suffix(raw_url)
        elif self.config.exchange == "SZSE":
            title = self._text(row.get("title"))
            published_raw = self._text(row.get("publishTime"))
            raw_url = self._text(row.get("attachPath"))
            symbols = self._values(row.get("secCode"))
            raw_id = self._text(row.get("annId") or row.get("id"))
            attachment_type = self._text(row.get("attachFormat")) or self._suffix(raw_url)
        else:
            title = self._text(
                row.get("disclosureTitle") or row.get("title") or row.get("announcementTitle")
            )
            published_raw = self._text(
                row.get("publishTime") or row.get("disclosureTime") or row.get("publishDate")
            )
            raw_url = self._text(
                row.get("destFilePath") or row.get("attachPath") or row.get("url")
            )
            symbols = self._values(
                row.get("companyCd") or row.get("companyCode") or row.get("secCode")
            )
            raw_id = self._text(
                row.get("disclosureId")
                or row.get("disclosureCode")
                or row.get("announcementId")
                or row.get("infoId")
                or row.get("id")
            )
            attachment_type = self._suffix(raw_url)
        if not title or not raw_url:
            return None
        if symbols and expected_symbol and expected_symbol not in symbols:
            return None
        resolved_url = urljoin(self.config.artifact_base_url, raw_url)
        identity_is_derived = not bool(raw_id)
        source_id = raw_id or build_derived_announcement_id(
            source=self.source_name,
            title=title,
            published_at_raw=published_raw,
            symbols=symbols or ([expected_symbol] if expected_symbol else []),
            source_urls=[resolved_url],
        )
        published_at, diagnostics = normalize_published_at(published_raw)
        if identity_is_derived:
            diagnostics.append("announcement_identity_derived")
        attachment = AnnouncementAttachment(
            source_url=raw_url,
            resolved_url=resolved_url,
            media_type=(
                "application/pdf"
                if attachment_type and "pdf" in attachment_type.lower()
                else None
            ),
            file_extension=attachment_type,
            raw_metadata={"source_row": dict(row)},
        )
        return AnnouncementRecord(
            source=self.source_name,
            source_announcement_id=source_id,
            announcement_key=build_announcement_key(self.source_name, source_id),
            title=title,
            published_at=published_at,
            published_at_raw=published_raw,
            exchange=self.config.exchange,
            market=self.config.exchange,
            symbols=tuple(symbols or ([expected_symbol] if expected_symbol else [])),
            attachments=(attachment,),
            raw_payload=dict(row),
            diagnostics=tuple(diagnostics),
            identity_is_derived=identity_is_derived,
        )

    def _page_count(
        self,
        payload: Mapping[str, Any],
        *,
        page_num: int,
        page_size: int,
        row_count: int,
    ) -> int:
        if self.config.exchange == "SSE":
            page_help = payload.get("pageHelp")
            if isinstance(page_help, Mapping):
                return max(1, int(page_help.get("pageCount") or 1))
        if self.config.exchange == "SZSE":
            total = int(payload.get("announceCount") or row_count)
            return max(1, math.ceil(total / page_size))
        if self.endpoint_mode == "recent_market":
            data = payload.get("data")
            if isinstance(data, Mapping):
                for value in (data.get("totalPages"), data.get("pageCount")):
                    if value not in (None, ""):
                        return max(1, int(value))
        list_info = payload.get("listInfo")
        values: List[Any] = [payload.get("totalPages"), payload.get("pageCount")]
        if isinstance(list_info, Mapping):
            values.extend([list_info.get("totalPages"), list_info.get("pageCount")])
        for value in values:
            if value not in (None, ""):
                return max(1, int(value))
        return page_num if row_count < page_size else page_num + 1

    @staticmethod
    def _page_reached_cursor(
        records: List[AnnouncementRecord],
        cursor: Optional[ProviderCursor],
    ) -> bool:
        if cursor is None:
            return False
        times = [record.published_at for record in records if record.published_at]
        return bool(times) and max(times) <= cursor.value

    @classmethod
    def _page_reached_start_date(
        cls,
        records: List[AnnouncementRecord],
        start_date: Optional[str],
    ) -> bool:
        if not start_date:
            return False
        published_dates = [
            value for record in records
            if (value := cls._published_date(record)) is not None
        ]
        return bool(published_dates) and min(published_dates) < date.fromisoformat(
            start_date
        )

    @staticmethod
    def _published_date(record: AnnouncementRecord) -> Optional[date]:
        raw = str(record.published_at_raw or "").strip()
        if len(raw) >= 10:
            try:
                return date.fromisoformat(raw[:10].replace("/", "-"))
            except ValueError:
                pass
        if record.published_at:
            try:
                return date.fromisoformat(record.published_at[:10])
            except ValueError:
                return None
        return None

    @staticmethod
    def _deduplicate_records(records: List[AnnouncementRecord]) -> List[AnnouncementRecord]:
        output: Dict[str, AnnouncementRecord] = {}
        for record in records:
            output.setdefault(record.announcement_key, record)
        return list(output.values())

    @classmethod
    def _required_dict_rows(
        cls,
        payload: Mapping[str, Any],
        container: str,
    ) -> List[Dict[str, Any]]:
        if container not in payload:
            raise ValueError(
                f"exchange announcement response is missing {container}"
            )
        return cls._dict_rows(payload[container], container=container)

    @staticmethod
    def _dict_rows(value: Any, *, container: str) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            raise ValueError(
                f"exchange announcement container {container} is not a list"
            )
        if any(not isinstance(item, Mapping) for item in value):
            raise ValueError(
                f"exchange announcement container {container} contains a non-object row"
            )
        return [dict(item) for item in value]

    @staticmethod
    def _values(value: Any) -> List[str]:
        if value is None:
            return []
        raw = value if isinstance(value, list) else str(value).split(",")
        output: List[str] = []
        for item in raw:
            text = str(item or "").strip()
            if text and text not in output:
                output.append(text)
        return output

    @staticmethod
    def _text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    @staticmethod
    def _suffix(url: Optional[str]) -> Optional[str]:
        if not url or "." not in url:
            return None
        return url.rsplit(".", 1)[-1].split("?", 1)[0].upper()
