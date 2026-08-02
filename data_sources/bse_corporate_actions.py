"""Deterministic recent BSE equity-distribution implementation parsing."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Mapping, Optional, Sequence

from data_sources.cninfo_corporate_action_documents import (
    CorporateActionPageText,
    normalize_page_text,
)
from research.announcements import AnnouncementRecord


BSE_SOURCE = "bse"
BSE_DIVIDEND_PROFILE = "bse_dividend_implementation"
BSE_IMPLEMENTATION_TITLE = "权益分派实施公告"
_NUMBER = r"([0-9]+(?:\.[0-9]+)?)"
_DATE_VALUE = (
    r"((?:19|20)\d{2}(?:年|[-/.])\s*\d{1,2}(?:月|[-/.])\s*"
    r"\d{1,2}日?)"
)


@dataclass(frozen=True)
class BseCorporateActionParseResult:
    """One auditable parse result; partial results never contain an event."""

    status: str
    observation: Optional[Dict[str, Any]]
    diagnostics: tuple[str, ...] = ()


def _date_value(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    iso_candidate = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate).date()
    except ValueError:
        pass
    if len(text) >= 10:
        try:
            return date.fromisoformat(text[:10].replace("/", "-").replace(".", "-"))
        except ValueError:
            pass
    normalized = re.sub(r"\s+", "", text)
    normalized = normalized.replace("年", "-").replace("月", "-")
    normalized = normalized.replace("日", "").replace("/", "-").replace(".", "-")
    try:
        year, month, day = (
            int(value) for value in normalized.split("-", maxsplit=2)
        )
        return date(year, month, day)
    except (TypeError, ValueError):
        return None


def _labeled_date(text: str, labels: Sequence[str]) -> Optional[date]:
    label_pattern = "|".join(re.escape(value) for value in labels)
    match = re.search(
        rf"(?:{label_pattern})\s*(?:为|是|：|:)?\s*{_DATE_VALUE}",
        text,
    )
    return _date_value(match.group(1)) if match else None


def _per_share(text: str, patterns: Sequence[str]) -> Optional[float]:
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return round(float(match.group(1)) / 10.0, 12)
    return None


def _action_type(
    cash: Optional[float],
    bonus: Optional[float],
    capitalization: Optional[float],
) -> str:
    effects = [
        name for name, value in (
            ("dividend", cash),
            ("bonus", bonus),
            ("capitalization", capitalization),
        )
        if value is not None and value > 0
    ]
    if len(effects) > 1:
        return "mixed_distribution"
    return effects[0] if effects else "distribution"


def parse_bse_dividend_implementation(
    *,
    record: AnnouncementRecord,
    instrument_id: str,
    pages: Sequence[CorporateActionPageText],
    document: Optional[Mapping[str, Any]] = None,
    as_of_date: Optional[date] = None,
) -> BseCorporateActionParseResult:
    """Parse explicit per-ten-share terms and labeled dates from a BSE PDF."""

    normalized_instrument = str(instrument_id or "").strip()
    if record.source != BSE_SOURCE or record.exchange != "BSE":
        return BseCorporateActionParseResult(
            status="not_applicable",
            observation=None,
            diagnostics=("not_bse_official_record",),
        )
    if BSE_IMPLEMENTATION_TITLE not in record.title:
        return BseCorporateActionParseResult(
            status="not_applicable",
            observation=None,
            diagnostics=("title_not_implementation_notice",),
        )
    if not normalized_instrument or not normalized_instrument.endswith(".BJ"):
        return BseCorporateActionParseResult(
            status="partial",
            observation=None,
            diagnostics=("instrument_identity_invalid",),
        )
    text = normalize_page_text(" ".join(page.text for page in pages))
    if not text:
        return BseCorporateActionParseResult(
            status="partial",
            observation=None,
            diagnostics=("document_text_empty",),
        )

    cash = _per_share(text, (
        rf"每\s*10\s*股[^。；;]{{0,40}}?派(?:发)?(?:人民币)?(?:现金)?(?:红利)?\s*{_NUMBER}\s*元",
        rf"每\s*10\s*股[^。；;]{{0,40}}?现金红利\s*{_NUMBER}\s*元",
    ))
    bonus = _per_share(text, (
        rf"每\s*10\s*股[^。；;]{{0,40}}?送(?:红股)?\s*{_NUMBER}\s*股",
    ))
    capitalization = _per_share(text, (
        rf"每\s*10\s*股[^。；;]{{0,50}}?转(?:增)?\s*{_NUMBER}\s*股",
    ))
    record_date = _labeled_date(text, ("股权登记日", "权益登记日"))
    ex_date = _labeled_date(text, (
        "除权除息日", "除权（息）日", "除权日", "除息日",
    ))
    pay_date = _labeled_date(text, (
        "现金红利发放日", "现金红利派发日", "红利发放日", "派息日",
    ))
    share_arrival_date = _labeled_date(text, (
        "新增股份上市日", "新增可流通股份上市日", "股份到账日",
        "转增股份到账日", "红股上市日",
    ))
    diagnostics = []
    if not any(
        value is not None and value > 0
        for value in (cash, bonus, capitalization)
    ):
        diagnostics.append("economic_terms_missing")
    if record_date is None:
        diagnostics.append("record_date_missing")
    if ex_date is None:
        diagnostics.append("ex_date_missing")
    if diagnostics:
        return BseCorporateActionParseResult(
            status="partial",
            observation=None,
            diagnostics=tuple(diagnostics),
        )

    announcement_date = _date_value(record.published_at_raw)
    anchor = "|".join((
        record.announcement_key,
        normalized_instrument,
    ))
    source_event_key = hashlib.sha256(anchor.encode("utf-8")).hexdigest()
    observation = {
        "instrument_id": normalized_instrument,
        "source": BSE_SOURCE,
        "source_profile": BSE_DIVIDEND_PROFILE,
        "source_event_key": source_event_key,
        "action_type": _action_type(cash, bonus, capitalization),
        "fiscal_period": None,
        "announcement_date": announcement_date,
        "record_date": record_date,
        "ex_date": ex_date,
        "pay_date": pay_date,
        "share_arrival_date": share_arrival_date,
        "cash_dividend_per_share": cash,
        "bonus_shares_per_share": bonus,
        "capitalization_shares_per_share": capitalization,
        "rights_shares_per_share": None,
        "rights_price": None,
        "currency": "CNY",
        "description": record.title,
        "event_status": (
            "implemented"
            if ex_date <= (as_of_date or date.today())
            else "scheduled"
        ),
        "quality_status": "official_document_complete",
        "raw_payload": {
            "announcement": record.to_dict(),
            "document": dict(document or {}),
            "parser": "bse_dividend_implementation.v1",
        },
    }
    return BseCorporateActionParseResult(
        status="success",
        observation=observation,
    )
