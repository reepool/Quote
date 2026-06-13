"""Official-source helpers for futures trading-day governance."""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from research.futures_market_data import (
    FUTURES_TRADING_DAY_GOVERNANCE_VERSION,
    FuturesCalendarNotice,
    FuturesTradingCalendarDay,
    _date_key,
)
from utils.config_manager import ResearchConfig
from utils.date_utils import get_shanghai_time
from utils.http_transport import HttpTlsConfig, create_requests_session, request_get


class OfficialFuturesCalendarSourceUnavailable(RuntimeError):
    """Raised when an official calendar/notice source cannot be used."""


@dataclass(frozen=True)
class ParsedFuturesCalendarNotice:
    notice: FuturesCalendarNotice
    calendar_days: List[FuturesTradingCalendarDay] = field(default_factory=list)
    review_required: bool = False


class OfficialFuturesCalendarProvider:
    """Fetch and parse official futures calendar or holiday notice evidence.

    This provider deliberately supports fixture-style structured payloads first.
    Live exchange notice pages can be added per exchange without changing the
    governance service contract.
    """

    source_name = "exchange_official_calendar"
    parser_version = FUTURES_TRADING_DAY_GOVERNANCE_VERSION

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
        )
    }

    def __init__(self, research_config: ResearchConfig):
        self.research_config = research_config
        module_cfg = research_config.modules.get("commodity_market_data", {})
        governance_cfg = module_cfg.get("trading_day_governance") or {}
        source_cfg = (governance_cfg.get("calendar_sources") or {}).get("exchange_official_calendar") or {}
        self.timeout_seconds = float(source_cfg.get("timeout_seconds", 20))
        self.endpoint_templates = dict(source_cfg.get("endpoint_templates") or {})
        self.tls_config = HttpTlsConfig(source_name=self.source_name)

    async def fetch_notice(self, *, exchange: str, url: Optional[str] = None) -> FuturesCalendarNotice:
        return await asyncio.to_thread(self._fetch_notice_sync, exchange=exchange, url=url)

    def _fetch_notice_sync(self, *, exchange: str, url: Optional[str] = None) -> FuturesCalendarNotice:
        exchange_key = str(exchange or "").upper()
        target_url = url or self.endpoint_templates.get(exchange_key)
        if not target_url:
            raise OfficialFuturesCalendarSourceUnavailable(f"missing official calendar URL for {exchange_key}")
        session = create_requests_session(tls_config=self.tls_config, headers=self.DEFAULT_HEADERS)
        response = request_get(
            target_url,
            session=session,
            tls_config=self.tls_config,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            raw_payload: Dict[str, Any] = response.json()
        else:
            raw_payload = {"text": response.text}
        payload_hash = hashlib.sha256(str(raw_payload).encode("utf-8")).hexdigest()
        return FuturesCalendarNotice(
            notice_id=f"{exchange_key}:official_calendar:{payload_hash[:20]}",
            exchange=exchange_key,
            source_profile=self.source_name,
            notice_type="calendar_or_holiday_notice",
            title=f"{exchange_key} official futures calendar evidence",
            url=target_url,
            fetched_at=get_shanghai_time().isoformat(),
            raw_content_hash=payload_hash,
            raw_payload=raw_payload,
            parser_version=self.parser_version,
            parse_status="raw",
            confidence=0.0,
        )

    def parse_notice(self, notice: FuturesCalendarNotice) -> ParsedFuturesCalendarNotice:
        rows = _extract_structured_calendar_rows(notice.raw_payload)
        if not rows:
            rows = _extract_text_calendar_rows(notice.raw_payload.get("text") or "")
        if not rows:
            review_notice = FuturesCalendarNotice(
                **{
                    **notice.__dict__,
                    "parse_status": "review_required",
                    "confidence": 0.0,
                    "derived_changes": [],
                    "metadata": {**notice.metadata, "reason": "no_structured_calendar_dates_found"},
                }
            )
            return ParsedFuturesCalendarNotice(notice=review_notice, calendar_days=[], review_required=True)
        calendar_days = [
            FuturesTradingCalendarDay(
                exchange=notice.exchange.upper(),
                trade_date=_date_key(row["trade_date"]),
                is_trading_day=bool(row["is_trading_day"]),
                timezone=str(row.get("timezone") or "Asia/Shanghai"),
                session_type=str(row.get("session_type") or ("day_and_night" if row["is_trading_day"] else "closed")),
                source_profile=notice.source_profile,
                quality_flag=str(row.get("quality_flag") or "official_parsed"),
                parser_version=self.parser_version,
                evidence_url=notice.url,
                notice_id=notice.notice_id,
                metadata={"notice_title": notice.title, **dict(row.get("metadata") or {})},
            )
            for row in rows
        ]
        parsed_notice = FuturesCalendarNotice(
            **{
                **notice.__dict__,
                "parse_status": "parsed",
                "confidence": 0.8,
                "derived_changes": [
                    {
                        "trade_date": item.trade_date,
                        "is_trading_day": item.is_trading_day,
                        "quality_flag": item.quality_flag,
                    }
                    for item in calendar_days
                ],
            }
        )
        return ParsedFuturesCalendarNotice(notice=parsed_notice, calendar_days=calendar_days)


def _extract_structured_calendar_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = []
    for key in ("calendar", "trading_days", "dates", "data", "rows"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(item for item in value if isinstance(item, dict))
    rows: List[Dict[str, Any]] = []
    for item in candidates:
        raw_date = item.get("trade_date") or item.get("date") or item.get("calendar_date")
        if not raw_date:
            continue
        status = item.get("is_trading_day")
        if status is None:
            status_text = str(item.get("status") or item.get("trading_status") or "").lower()
            status = status_text in {"trading", "open", "1", "true", "交易", "交易日"}
        rows.append(
            {
                "trade_date": raw_date,
                "is_trading_day": bool(status),
                "timezone": item.get("timezone"),
                "session_type": item.get("session_type"),
                "quality_flag": item.get("quality_flag") or "official_parsed",
                "metadata": {key: value for key, value in item.items() if key not in {"trade_date", "date"}},
            }
        )
    return rows


def _extract_text_calendar_rows(text: str) -> List[Dict[str, Any]]:
    if not text:
        return []
    rows = []
    for match in re.finditer(r"(20\d{2})[-年/.](\d{1,2})[-月/.](\d{1,2})", text):
        trade_date = f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
        window = text[max(0, match.start() - 20): match.end() + 20]
        is_closed = any(token in window for token in ("休市", "暂停", "不交易", "闭市", "holiday", "closed"))
        is_open = any(token in window for token in ("交易", "开市", "恢复", "open", "trading"))
        if is_closed or is_open:
            rows.append(
                {
                    "trade_date": trade_date,
                    "is_trading_day": bool(is_open and not is_closed),
                    "quality_flag": "official_parsed",
                    "metadata": {"text_window": window.strip()},
                }
            )
    return rows
