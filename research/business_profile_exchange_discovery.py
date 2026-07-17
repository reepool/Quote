"""Official exchange fallback discovery for business-profile documents."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urljoin

import requests

from research.business_profile_discovery import (
    BusinessProfileDiscoveryResult,
    BusinessProfileDocumentCandidate,
    CninfoBusinessProfileDiscoveryAdapter,
)
from research.business_profile_documents import classify_business_profile_document
from utils.http_transport import HttpTlsConfig, create_requests_session


LOGGER = logging.getLogger(__name__)
OFFICIAL_EXCHANGE_SOURCE_TIER = "official_backup"
SUPPORTED_EXCHANGE_SOURCES = {
    "SSE": "sse",
    "SZSE": "szse",
    "BSE": "bse",
}
_JSONP_RE = re.compile(r"^[A-Za-z_$][\w$.]*\s*\((.*)\)\s*;?\s*$", re.S)


@dataclass(frozen=True)
class ExchangeBusinessProfileSourceConfig:
    """Configuration for one official exchange announcement endpoint."""

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
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(
        cls,
        exchange: str,
        value: Mapping[str, Any],
    ) -> "ExchangeBusinessProfileSourceConfig":
        normalized_exchange = str(exchange or "").strip().upper()
        expected_source = SUPPORTED_EXCHANGE_SOURCES.get(normalized_exchange)
        if expected_source is None:
            raise ValueError(
                f"unsupported business-profile exchange backup: {exchange}"
            )
        source = str(value.get("source") or expected_source).strip().lower()
        if source != expected_source:
            raise ValueError(
                f"{normalized_exchange} backup source must be {expected_source}"
            )
        endpoint_url = str(value.get("endpoint_url") or "").strip()
        referer = str(value.get("referer") or "").strip()
        artifact_base_url = str(value.get("artifact_base_url") or "").strip()
        if not endpoint_url or not referer or not artifact_base_url:
            raise ValueError(
                f"{normalized_exchange} backup endpoint, referer, and artifact "
                "base URL are required"
            )
        method = str(value.get("method") or "GET").strip().upper()
        if method not in {"GET", "POST"}:
            raise ValueError(f"{normalized_exchange} backup method must be GET or POST")
        return cls(
            exchange=normalized_exchange,
            source=source,
            endpoint_url=endpoint_url,
            method=method,
            referer=referer,
            artifact_base_url=artifact_base_url,
            enabled=bool(value.get("enabled", True)),
            request_timeout_seconds=float(value.get("request_timeout_seconds", 20.0)),
            request_interval_seconds=max(
                0.0,
                float(value.get("request_interval_seconds", 0.2)),
            ),
            retry_attempts=max(0, int(value.get("retry_attempts", 2))),
            retry_backoff_seconds=max(
                0.0,
                float(value.get("retry_backoff_seconds", 0.5)),
            ),
            options=dict(value.get("options") or {}),
        )


@dataclass(frozen=True)
class BusinessProfileSourceAttempt:
    """Compact diagnostic for one source-priority attempt."""

    source: str
    source_tier: str
    status: str
    candidate_count: int
    pages_scanned: int
    announcements_seen: int
    errors: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class BusinessProfileDiscoveryResolution:
    """Chosen source result plus all attempted source diagnostics."""

    status: str
    selected_source: Optional[str]
    selected_source_tier: Optional[str]
    fallback_used: bool
    fallback_reason: Optional[str]
    candidates: List[BusinessProfileDocumentCandidate]
    attempts: List[BusinessProfileSourceAttempt]

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "candidates": [item.to_dict() for item in self.candidates],
            "attempts": [asdict(item) for item in self.attempts],
        }


class OfficialExchangeBusinessProfileDiscoveryAdapter:
    """Discover official exchange attachments without production writes."""

    def __init__(
        self,
        config: ExchangeBusinessProfileSourceConfig,
        *,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not config.enabled:
            raise ValueError(
                f"{config.exchange} business-profile backup source is disabled"
            )
        self.config = config
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

    def discover_instrument(
        self,
        instrument: Dict[str, Any],
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search_key: Optional[str] = None,
        category: Optional[str] = None,
        page_size: int = 30,
        max_pages: int = 20,
        dry_run: bool = True,
        ingestion_run_id: Optional[int] = None,
    ) -> BusinessProfileDiscoveryResult:
        """Run one bounded exchange query and normalize selected documents."""
        del category, ingestion_run_id
        if not dry_run:
            raise ValueError(
                "exchange backup discovery is read-only until orchestration "
                "persistence is implemented"
            )
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip().upper()
        if not instrument_id or not symbol:
            raise ValueError("instrument_id and symbol are required")
        if exchange != self.config.exchange:
            raise ValueError(
                f"{self.config.source} adapter does not support exchange {exchange}"
            )
        if bool(start_date) != bool(end_date):
            raise ValueError("start_date and end_date must be provided together")

        bounded_page_size = max(1, min(int(page_size), 100))
        bounded_max_pages = max(1, int(max_pages))
        candidates: List[BusinessProfileDocumentCandidate] = []
        errors: List[str] = []
        pages_scanned = 0
        announcements_seen = 0
        max_announcement_time: Optional[str] = None

        for page_num in range(1, bounded_max_pages + 1):
            try:
                payload = self._request_page(
                    symbol=symbol,
                    page_num=page_num,
                    page_size=bounded_page_size,
                    start_date=start_date,
                    end_date=end_date,
                    search_key=search_key,
                )
            except Exception as exc:
                message = f"{self.config.source} page {page_num} request failed: {exc}"
                errors.append(message)
                LOGGER.warning(
                    "business-profile exchange discovery failed: "
                    "source=%s instrument_id=%s page=%s error=%s",
                    self.config.source,
                    instrument_id,
                    page_num,
                    exc,
                )
                break

            rows = self._extract_rows(payload)
            pages_scanned += 1
            announcements_seen += len(rows)
            for row in rows:
                candidate = self._normalize_candidate(row, expected_symbol=symbol)
                if candidate is None:
                    continue
                announcement_time = candidate.announcement_time
                if announcement_time and (
                    max_announcement_time is None
                    or announcement_time > max_announcement_time
                ):
                    max_announcement_time = announcement_time
                if candidate.classification.selected:
                    candidates.append(candidate)

            if page_num >= self._page_count(
                payload,
                page_num=page_num,
                page_size=bounded_page_size,
                row_count=len(rows),
            ):
                break
            if self.config.request_interval_seconds > 0:
                time.sleep(self.config.request_interval_seconds)

        candidates = self._deduplicate_candidates(candidates)
        status = "degraded" if errors else "success" if candidates else "not_found"
        return BusinessProfileDiscoveryResult(
            status=status,
            purpose_key=f"business_profile_evidence:{instrument_id}",
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            pages_scanned=pages_scanned,
            announcements_seen=announcements_seen,
            candidates=candidates,
            max_announcement_time=max_announcement_time,
            stopped_at_watermark=False,
            errors=errors,
            source=self.config.source,
            source_tier=OFFICIAL_EXCHANGE_SOURCE_TIER,
        )

    def _request_page(
        self,
        *,
        symbol: str,
        page_num: int,
        page_size: int,
        start_date: Optional[str],
        end_date: Optional[str],
        search_key: Optional[str],
    ) -> Dict[str, Any]:
        kwargs = self._request_kwargs(
            symbol=symbol,
            page_num=page_num,
            page_size=page_size,
            start_date=start_date,
            end_date=end_date,
            search_key=search_key,
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
        symbol: str,
        page_num: int,
        page_size: int,
        start_date: Optional[str],
        end_date: Optional[str],
        search_key: Optional[str],
    ) -> Dict[str, Any]:
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
                    "reportType2": "DQBG",
                    "reportType": "ALL",
                    "beginDate": start_date or "",
                    "endDate": end_date or "",
                }
            }
        if self.config.exchange == "SZSE":
            body: Dict[str, Any] = {
                "stock": [symbol],
                "channelCode": [self.config.options.get("channel_code", "fixed_disc")],
                "pageSize": page_size,
                "pageNum": page_num,
            }
            if start_date and end_date:
                body["seDate"] = [start_date, end_date]
            if search_key:
                body["keyword"] = search_key
            return {"json": body}
        if self.config.exchange == "BSE":
            return {
                "data": {
                    "page": str(page_num - 1),
                    "companyCd": symbol,
                    "isNewThree": str(self.config.options.get("is_new_three", "1")),
                    "xxfcbj": str(self.config.options.get("xxfcbj", "1")),
                    "keyword": search_key or "",
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
        if not isinstance(payload, dict):
            raise ValueError("exchange announcement response is not an object")
        return payload

    def _extract_rows(self, payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
        if self.config.exchange == "SSE":
            return self._dict_rows(payload.get("result"))
        if self.config.exchange == "SZSE":
            return self._dict_rows(payload.get("data"))
        candidates = [
            payload.get("content"),
            payload.get("data"),
            payload.get("rows"),
        ]
        list_info = payload.get("listInfo")
        if isinstance(list_info, Mapping):
            candidates.extend(
                [
                    list_info.get("content"),
                    list_info.get("rows"),
                    list_info.get("data"),
                ]
            )
        for value in candidates:
            rows = self._dict_rows(value)
            if rows:
                return rows
        return []

    def _normalize_candidate(
        self,
        row: Mapping[str, Any],
        *,
        expected_symbol: str,
    ) -> Optional[BusinessProfileDocumentCandidate]:
        if self.config.exchange == "SSE":
            title = self._text(row.get("TITLE"))
            announcement_time = self._text(row.get("SSEDATE"))
            raw_url = self._text(row.get("URL"))
            symbols = self._values(row.get("SECURITY_CODE"))
            raw_id = None
            adjunct_type = self._suffix(raw_url)
        elif self.config.exchange == "SZSE":
            title = self._text(row.get("title"))
            announcement_time = self._text(row.get("publishTime"))
            raw_url = self._text(row.get("attachPath"))
            symbols = self._values(row.get("secCode"))
            raw_id = self._text(row.get("annId") or row.get("id"))
            adjunct_type = self._text(row.get("attachFormat")) or self._suffix(raw_url)
        else:
            title = self._text(
                row.get("disclosureTitle")
                or row.get("title")
                or row.get("announcementTitle")
            )
            announcement_time = self._text(
                row.get("publishTime")
                or row.get("disclosureTime")
                or row.get("publishDate")
            )
            raw_url = self._text(
                row.get("destFilePath") or row.get("attachPath") or row.get("url")
            )
            symbols = self._values(
                row.get("companyCd") or row.get("companyCode") or row.get("secCode")
            )
            raw_id = self._text(
                row.get("disclosureId") or row.get("announcementId") or row.get("id")
            )
            adjunct_type = self._suffix(raw_url)
        if not title or not raw_url:
            return None
        if symbols and expected_symbol not in symbols:
            return None
        classification = classify_business_profile_document(
            title,
            adjunct_type=adjunct_type,
        )
        absolute_url = urljoin(self.config.artifact_base_url, raw_url)
        announcement_id = raw_id or self._stable_announcement_id(
            source=self.config.source,
            url=absolute_url,
            title=title,
            announcement_time=announcement_time,
        )
        reasons: List[str] = []
        if classification.selected:
            reasons.append(f"business_profile_document:{classification.document_type}")
        if classification.is_correction:
            reasons.append("business_profile_document_correction")
        reasons.extend(
            f"profile_event_hint:{item}" for item in classification.profile_event_hints
        )
        reasons.append(f"official_exchange_backup:{self.config.source}")
        return BusinessProfileDocumentCandidate(
            announcement_id=f"{self.config.source}:{announcement_id}",
            title=title,
            announcement_time=announcement_time,
            symbols=symbols or [expected_symbol],
            adjunct_url=absolute_url,
            adjunct_type=adjunct_type,
            classification=classification,
            selection_reasons=sorted(set(reasons)),
            source=self.config.source,
            source_tier=OFFICIAL_EXCHANGE_SOURCE_TIER,
            raw_payload=dict(row),
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
        list_info = payload.get("listInfo")
        values: List[Any] = [
            payload.get("totalPages"),
            payload.get("pageCount"),
        ]
        if isinstance(list_info, Mapping):
            values.extend(
                [
                    list_info.get("totalPages"),
                    list_info.get("pageCount"),
                ]
            )
        for value in values:
            if value not in (None, ""):
                return max(1, int(value))
        return page_num if row_count < page_size else page_num + 1

    @staticmethod
    def _dict_rows(value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        return [dict(item) for item in value if isinstance(item, Mapping)]

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

    @staticmethod
    def _stable_announcement_id(
        *,
        source: str,
        url: str,
        title: str,
        announcement_time: Optional[str],
    ) -> str:
        payload = "|".join((source, url, title, announcement_time or ""))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _deduplicate_candidates(
        candidates: List[BusinessProfileDocumentCandidate],
    ) -> List[BusinessProfileDocumentCandidate]:
        output: Dict[str, BusinessProfileDocumentCandidate] = {}
        for candidate in candidates:
            output.setdefault(candidate.announcement_id, candidate)
        return list(output.values())


class BusinessProfileDiscoveryCoordinator:
    """Use CNInfo first and invoke only the matching official exchange backup."""

    def __init__(
        self,
        *,
        primary_adapter: Optional[CninfoBusinessProfileDiscoveryAdapter] = None,
        backup_adapters: Optional[
            Mapping[str, OfficialExchangeBusinessProfileDiscoveryAdapter]
        ] = None,
        fallback_on_empty: bool = True,
    ) -> None:
        self.primary_adapter = (
            primary_adapter or CninfoBusinessProfileDiscoveryAdapter()
        )
        self.backup_adapters = {
            str(exchange).upper(): adapter
            for exchange, adapter in (backup_adapters or {}).items()
        }
        self.fallback_on_empty = fallback_on_empty

    @classmethod
    def from_research_config(
        cls,
        research_config: Any,
        *,
        primary_adapter: Optional[CninfoBusinessProfileDiscoveryAdapter] = None,
    ) -> "BusinessProfileDiscoveryCoordinator":
        modules = getattr(research_config, "modules", {}) or {}
        module_cfg = modules.get("business_profile_evidence", {})
        discovery_cfg = (
            module_cfg.get("discovery", {}) if isinstance(module_cfg, Mapping) else {}
        )
        backup_cfg = (
            discovery_cfg.get("official_exchange_backups", {})
            if isinstance(discovery_cfg, Mapping)
            else {}
        )
        primary_source = (
            str(discovery_cfg.get("primary_source") or "cninfo").strip().lower()
            if isinstance(discovery_cfg, Mapping)
            else "cninfo"
        )
        if primary_source != "cninfo":
            raise ValueError("business-profile primary discovery source must be cninfo")
        adapters: Dict[str, OfficialExchangeBusinessProfileDiscoveryAdapter] = {}
        if isinstance(backup_cfg, Mapping):
            for exchange, raw_config in backup_cfg.items():
                if not isinstance(raw_config, Mapping):
                    continue
                source_config = ExchangeBusinessProfileSourceConfig.from_mapping(
                    str(exchange),
                    raw_config,
                )
                if source_config.enabled:
                    adapters[source_config.exchange] = (
                        OfficialExchangeBusinessProfileDiscoveryAdapter(source_config)
                    )
        return cls(
            primary_adapter=primary_adapter,
            backup_adapters=adapters,
            fallback_on_empty=bool(
                discovery_cfg.get("fallback_on_empty", True)
                if isinstance(discovery_cfg, Mapping)
                else True
            ),
        )

    def discover_instrument(
        self,
        instrument: Dict[str, Any],
        **kwargs: Any,
    ) -> BusinessProfileDiscoveryResolution:
        """Resolve one instrument through the configured source-priority chain."""
        attempts: List[BusinessProfileSourceAttempt] = []
        primary_result, primary_error = self._attempt(
            self.primary_adapter,
            instrument,
            kwargs,
            source="cninfo",
            source_tier="official_primary",
        )
        if primary_result is not None:
            attempts.append(self._source_attempt(primary_result))
        elif primary_error:
            attempts.append(
                BusinessProfileSourceAttempt(
                    source="cninfo",
                    source_tier="official_primary",
                    status="failed",
                    candidate_count=0,
                    pages_scanned=0,
                    announcements_seen=0,
                    errors=[primary_error],
                )
            )

        primary_usable = (
            primary_result is not None
            and primary_result.status == "success"
            and (bool(primary_result.candidates) or not self.fallback_on_empty)
        )
        if primary_usable:
            return self._resolution(
                selected=primary_result,
                attempts=attempts,
                fallback_used=False,
                fallback_reason=None,
            )

        fallback_reason = self._fallback_reason(
            primary_result,
            primary_error=primary_error,
        )
        exchange = str(instrument.get("exchange") or "").strip().upper()
        backup_adapter = self.backup_adapters.get(exchange)
        backup_result: Optional[BusinessProfileDiscoveryResult] = None
        if backup_adapter is not None:
            backup_result, backup_error = self._attempt(
                backup_adapter,
                instrument,
                kwargs,
                source=backup_adapter.config.source,
                source_tier=OFFICIAL_EXCHANGE_SOURCE_TIER,
            )
            if backup_result is not None:
                attempts.append(self._source_attempt(backup_result))
            elif backup_error:
                attempts.append(
                    BusinessProfileSourceAttempt(
                        source=backup_adapter.config.source,
                        source_tier=OFFICIAL_EXCHANGE_SOURCE_TIER,
                        status="failed",
                        candidate_count=0,
                        pages_scanned=0,
                        announcements_seen=0,
                        errors=[backup_error],
                    )
                )

        selected = (
            backup_result
            if backup_result is not None and backup_result.candidates
            else (
                primary_result
                if primary_result is not None and primary_result.candidates
                else backup_result or primary_result
            )
        )
        return self._resolution(
            selected=selected,
            attempts=attempts,
            fallback_used=backup_adapter is not None,
            fallback_reason=fallback_reason,
        )

    @staticmethod
    def _attempt(
        adapter: Any,
        instrument: Dict[str, Any],
        kwargs: Mapping[str, Any],
        *,
        source: str,
        source_tier: str,
    ) -> tuple[Optional[BusinessProfileDiscoveryResult], Optional[str]]:
        try:
            return adapter.discover_instrument(instrument, **dict(kwargs)), None
        except Exception as exc:
            return (
                None,
                f"{source}:{source_tier}:{type(exc).__name__}: {exc}",
            )

    @staticmethod
    def _source_attempt(
        result: BusinessProfileDiscoveryResult,
    ) -> BusinessProfileSourceAttempt:
        return BusinessProfileSourceAttempt(
            source=result.source,
            source_tier=result.source_tier,
            status=result.status,
            candidate_count=len(result.candidates),
            pages_scanned=result.pages_scanned,
            announcements_seen=result.announcements_seen,
            errors=list(result.errors),
        )

    @staticmethod
    def _fallback_reason(
        primary_result: Optional[BusinessProfileDiscoveryResult],
        *,
        primary_error: Optional[str],
    ) -> str:
        if primary_error:
            return "primary_failed"
        if primary_result is None:
            return "primary_unavailable"
        if primary_result.status == "degraded":
            return "primary_degraded"
        if primary_result.status == "not_found":
            return "primary_not_found"
        return "primary_empty"

    @staticmethod
    def _resolution(
        *,
        selected: Optional[BusinessProfileDiscoveryResult],
        attempts: List[BusinessProfileSourceAttempt],
        fallback_used: bool,
        fallback_reason: Optional[str],
    ) -> BusinessProfileDiscoveryResolution:
        candidates = [] if selected is None else list(selected.candidates)
        if selected is not None:
            status = selected.status
        elif any(item.status in {"degraded", "failed"} for item in attempts):
            status = "degraded"
        else:
            status = "not_found"
        return BusinessProfileDiscoveryResolution(
            status=status,
            selected_source=None if selected is None else selected.source,
            selected_source_tier=(None if selected is None else selected.source_tier),
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            candidates=candidates,
            attempts=attempts,
        )
