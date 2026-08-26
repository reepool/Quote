"""Source-neutral models for official company announcements."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

from .categories import normalize_announcement_category


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SUCCESSFUL_SCAN_STATUSES = frozenset({"success", "success_empty"})


def _stable_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _deduplicated_texts(values: Iterable[Any]) -> Tuple[str, ...]:
    output: List[str] = []
    for value in values:
        text = _clean_text(value)
        if text and text not in output:
            output.append(text)
    return tuple(output)


def build_announcement_key(source: str, source_announcement_id: str) -> str:
    """Build the stable source-qualified announcement identity."""
    source_name = str(source or "").strip().lower()
    source_id = str(source_announcement_id or "").strip()
    if not source_name or not source_id:
        raise ValueError("source and source_announcement_id are required")
    return f"{source_name}:{source_id}"


def build_derived_announcement_id(
    *,
    source: str,
    title: str,
    published_at_raw: Any = None,
    symbols: Iterable[str] = (),
    source_urls: Iterable[str] = (),
) -> str:
    """Build a deterministic provider-local fallback id from evidence fields."""
    payload = {
        "source": str(source or "").strip().lower(),
        "title": str(title or "").strip(),
        "published_at_raw": None
        if published_at_raw is None
        else str(published_at_raw).strip(),
        "symbols": sorted(_deduplicated_texts(symbols)),
        "source_urls": sorted(_deduplicated_texts(source_urls)),
    }
    return f"derived-{hashlib.sha256(_stable_json(payload).encode('utf-8')).hexdigest()[:32]}"


def normalize_published_at(
    value: Any,
    *,
    source_timezone: ZoneInfo = SHANGHAI_TZ,
) -> tuple[Optional[str], List[str]]:
    """Normalize a source timestamp while reporting any availability limitation."""
    if value in (None, ""):
        return None, ["published_at_missing"]
    diagnostics: List[str] = []
    parsed: Optional[datetime] = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
        diagnostics.append("published_at_date_only")
    elif isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        try:
            parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None, ["published_at_invalid"]
    else:
        text = str(value).strip()
        if text.isdigit():
            return normalize_published_at(int(text), source_timezone=source_timezone)
        candidate = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"):
                try:
                    parsed = datetime.strptime(text, fmt)
                    if fmt == "%Y-%m-%d":
                        diagnostics.append("published_at_date_only")
                    break
                except ValueError:
                    continue
        if parsed is None:
            return None, ["published_at_unparseable"]
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=source_timezone)
        diagnostics.append(f"published_at_assumed_timezone:{source_timezone.key}")
    return parsed.astimezone(timezone.utc).isoformat(), diagnostics


@dataclass(frozen=True)
class ProviderCursor:
    """Provider checkpoint cursor, which may be temporal or opaque."""

    kind: str
    value: str

    def __post_init__(self) -> None:
        if not str(self.kind or "").strip() or not str(self.value or "").strip():
            raise ValueError("cursor kind and value are required")


@dataclass(frozen=True)
class AnnouncementScope:
    """Stable discovery scope plus bounded per-run parameters."""

    exchange: str
    market: Optional[str] = None
    instrument_id: Optional[str] = None
    symbol: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    keyword: Optional[str] = None
    category: Optional[str] = None
    cursor: Optional[ProviderCursor] = None
    page_size: int = 30
    max_pages: int = 20
    overlap_days: int = 0
    start_page: int = 1
    preflight_page_bound: bool = False
    source_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        exchange = str(self.exchange or "").strip().upper()
        if not exchange:
            raise ValueError("exchange is required")
        object.__setattr__(self, "exchange", exchange)
        object.__setattr__(self, "market", _clean_text(self.market) or exchange)
        object.__setattr__(self, "instrument_id", _clean_text(self.instrument_id))
        object.__setattr__(self, "symbol", _clean_text(self.symbol))
        object.__setattr__(self, "start_date", _clean_text(self.start_date))
        object.__setattr__(self, "end_date", _clean_text(self.end_date))
        object.__setattr__(self, "keyword", _clean_text(self.keyword))
        object.__setattr__(
            self,
            "category",
            normalize_announcement_category(self.category),
        )
        object.__setattr__(self, "page_size", max(1, int(self.page_size)))
        object.__setattr__(self, "max_pages", max(1, int(self.max_pages)))
        object.__setattr__(self, "overlap_days", max(0, int(self.overlap_days)))
        object.__setattr__(self, "start_page", max(1, int(self.start_page)))
        object.__setattr__(
            self,
            "preflight_page_bound",
            bool(self.preflight_page_bound),
        )
        object.__setattr__(self, "source_options", dict(self.source_options or {}))
        if bool(self.start_date) != bool(self.end_date):
            raise ValueError("start_date and end_date must be provided together")

    def stable_scope_payload(self) -> Dict[str, Any]:
        """Return fields that identify the stream, excluding run windows/bounds."""
        # Pagination strategy changes how a stream is read, not which
        # announcement stream it represents. Keep operational source options
        # such as adaptive pagination from invalidating an existing cursor.
        stable_source_options = {
            key: value
            for key, value in self.source_options.items()
            if key != "adaptive_pagination"
        }
        return {
            "exchange": self.exchange,
            "market": self.market,
            "instrument_id": self.instrument_id,
            "symbol": self.symbol,
            "keyword": self.keyword,
            "category": self.category,
            "source_options": stable_source_options,
        }

    @property
    def scope_key(self) -> str:
        digest = hashlib.sha256(
            _stable_json(self.stable_scope_payload()).encode("utf-8")
        ).hexdigest()
        return digest

    @property
    def is_instrument_scoped(self) -> bool:
        return bool(self.instrument_id or self.symbol)


@dataclass(frozen=True)
class AnnouncementQuery:
    """One business-purpose request for official announcements."""

    purpose_key: str
    scope: AnnouncementScope
    source: Optional[str] = None

    def __post_init__(self) -> None:
        purpose = str(self.purpose_key or "").strip()
        if not purpose:
            raise ValueError("purpose_key is required")
        object.__setattr__(self, "purpose_key", purpose)
        object.__setattr__(self, "source", _clean_text(self.source.lower() if self.source else None))

    def for_source(self, source: str) -> "AnnouncementQuery":
        return replace(self, source=str(source or "").strip().lower())


@dataclass(frozen=True)
class AnnouncementAttachment:
    """Normalized metadata for one announcement attachment."""

    source_url: str
    resolved_url: Optional[str] = None
    attachment_id: Optional[str] = None
    name: Optional[str] = None
    media_type: Optional[str] = None
    file_extension: Optional[str] = None
    raw_metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.source_url or "").strip():
            raise ValueError("attachment source_url is required")
        object.__setattr__(self, "source_url", str(self.source_url).strip())
        object.__setattr__(self, "resolved_url", _clean_text(self.resolved_url))
        object.__setattr__(self, "attachment_id", _clean_text(self.attachment_id))
        object.__setattr__(self, "name", _clean_text(self.name))
        object.__setattr__(self, "media_type", _clean_text(self.media_type))
        object.__setattr__(self, "file_extension", _clean_text(self.file_extension))
        object.__setattr__(self, "raw_metadata", dict(self.raw_metadata or {}))


@dataclass(frozen=True)
class AnnouncementRecord:
    """Normalized source record without business-semantic interpretation."""

    source: str
    source_announcement_id: str
    announcement_key: str
    title: str
    published_at: Optional[str]
    published_at_raw: Any = None
    exchange: Optional[str] = None
    market: Optional[str] = None
    symbols: Tuple[str, ...] = ()
    security_names: Tuple[str, ...] = ()
    organization_ids: Tuple[str, ...] = ()
    attachments: Tuple[AnnouncementAttachment, ...] = ()
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    diagnostics: Tuple[str, ...] = ()
    identity_is_derived: bool = False
    selection_reasons: Tuple[str, ...] = ()
    provider_route_evidence: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        source = str(self.source or "").strip().lower()
        source_id = str(self.source_announcement_id or "").strip()
        title = str(self.title or "").strip()
        if not source or not source_id or not title:
            raise ValueError("source, source_announcement_id, and title are required")
        expected_key = build_announcement_key(source, source_id)
        if self.announcement_key != expected_key:
            raise ValueError("announcement_key does not match source identity")
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "source_announcement_id", source_id)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "exchange", _clean_text(self.exchange.upper() if self.exchange else None))
        object.__setattr__(self, "market", _clean_text(self.market))
        object.__setattr__(self, "symbols", _deduplicated_texts(self.symbols))
        object.__setattr__(self, "security_names", _deduplicated_texts(self.security_names))
        object.__setattr__(self, "organization_ids", _deduplicated_texts(self.organization_ids))
        object.__setattr__(self, "attachments", tuple(self.attachments or ()))
        object.__setattr__(self, "raw_payload", dict(self.raw_payload or {}))
        object.__setattr__(self, "diagnostics", _deduplicated_texts(self.diagnostics))
        object.__setattr__(self, "selection_reasons", _deduplicated_texts(self.selection_reasons))
        object.__setattr__(
            self,
            "provider_route_evidence",
            dict(self.provider_route_evidence or {}),
        )

    def with_selection_reasons(self, reasons: Iterable[str]) -> "AnnouncementRecord":
        return replace(self, selection_reasons=_deduplicated_texts(reasons))

    def with_provider_route_evidence(
        self,
        evidence: Mapping[str, Any],
    ) -> "AnnouncementRecord":
        """Attach source-neutral ordered route evidence to a selected record."""
        return replace(self, provider_route_evidence=dict(evidence or {}))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnnouncementScanResult:
    """Bounded result from one concrete source provider."""

    source: str
    query: AnnouncementQuery
    status: str
    records: Tuple[AnnouncementRecord, ...] = ()
    selected_records: Tuple[AnnouncementRecord, ...] = ()
    pages_scanned: int = 0
    requests_made: int = 0
    announcements_seen: int = 0
    max_published_at: Optional[str] = None
    provider_cursor: Optional[ProviderCursor] = None
    is_complete: bool = False
    reached_prior_cursor: bool = False
    stop_reason: Optional[str] = None
    errors: Tuple[str, ...] = ()
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        source = str(self.source or "").strip().lower()
        if not source:
            raise ValueError("scan result source is required")
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "status", str(self.status or "").strip().lower())
        object.__setattr__(self, "records", tuple(self.records or ()))
        object.__setattr__(self, "selected_records", tuple(self.selected_records or ()))
        object.__setattr__(self, "pages_scanned", max(0, int(self.pages_scanned)))
        object.__setattr__(self, "requests_made", max(0, int(self.requests_made)))
        object.__setattr__(self, "announcements_seen", max(0, int(self.announcements_seen)))
        object.__setattr__(self, "errors", _deduplicated_texts(self.errors))
        object.__setattr__(self, "diagnostics", dict(self.diagnostics or {}))
        if self.query.source and self.query.source != source:
            raise ValueError("scan result source does not match query source")
        if any(record.source != source for record in self.records):
            raise ValueError("scan result contains a record from another source")
        if any(record.source != source for record in self.selected_records):
            raise ValueError("selected scan record belongs to another source")

    @property
    def cursor_commit_allowed(self) -> bool:
        return self.is_complete and self.status in SUCCESSFUL_SCAN_STATUSES

    def with_selected_records(
        self,
        selected_records: Iterable[AnnouncementRecord],
    ) -> "AnnouncementScanResult":
        return replace(self, selected_records=tuple(selected_records))


@dataclass(frozen=True)
class AnnouncementRouteAttempt:
    """One provider attempt retained for routing diagnostics."""

    source: str
    status: str
    record_count: int
    selected_count: int
    pages_scanned: int
    stop_reason: Optional[str] = None
    errors: Tuple[str, ...] = ()
    requests_made: int = 0
    announcements_seen: int = 0
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        source = str(self.source or "").strip().lower()
        if not source:
            raise ValueError("route attempt source is required")
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "status", str(self.status or "").strip().lower())
        object.__setattr__(self, "record_count", max(0, int(self.record_count)))
        object.__setattr__(self, "selected_count", max(0, int(self.selected_count)))
        object.__setattr__(self, "pages_scanned", max(0, int(self.pages_scanned)))
        object.__setattr__(self, "requests_made", max(0, int(self.requests_made)))
        object.__setattr__(
            self,
            "announcements_seen",
            max(0, int(self.announcements_seen)),
        )
        object.__setattr__(self, "stop_reason", _clean_text(self.stop_reason))
        object.__setattr__(self, "errors", _deduplicated_texts(self.errors))
        object.__setattr__(self, "diagnostics", dict(self.diagnostics or {}))


@dataclass(frozen=True)
class AnnouncementRouteResult:
    """Selected provider result plus the full ordered attempt history."""

    query: AnnouncementQuery
    status: str
    selected_source: Optional[str]
    scan_result: Optional[AnnouncementScanResult]
    attempts: Tuple[AnnouncementRouteAttempt, ...] = ()
    fallback_used: bool = False
    fallback_reason: Optional[str] = None
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        status = str(self.status or "").strip().lower()
        selected_source = _clean_text(self.selected_source)
        if selected_source is not None:
            selected_source = selected_source.lower()
        attempts = tuple(self.attempts or ())
        if self.scan_result is not None:
            if selected_source != self.scan_result.source:
                raise ValueError("selected source does not match scan result source")
            if attempts and attempts[-1].source != selected_source:
                raise ValueError("selected source must be the final route attempt")
        elif selected_source is not None:
            raise ValueError("selected source requires a scan result")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "selected_source", selected_source)
        object.__setattr__(self, "attempts", attempts)
        object.__setattr__(self, "fallback_used", bool(self.fallback_used))
        object.__setattr__(self, "fallback_reason", _clean_text(self.fallback_reason))
        object.__setattr__(self, "diagnostics", dict(self.diagnostics or {}))


@dataclass(frozen=True)
class AnnouncementRetrievalResult:
    """Retrieved attachment bytes and transport evidence, without archive policy."""

    source: str
    attachment: AnnouncementAttachment
    status: str
    content: bytes = b""
    content_hash: Optional[str] = None
    content_length: int = 0
    final_url: Optional[str] = None
    response_media_type: Optional[str] = None
    retrieved_at: Optional[str] = None
    signature_status: Optional[str] = None
    errors: Tuple[str, ...] = ()
    diagnostics: Dict[str, Any] = field(default_factory=dict)
