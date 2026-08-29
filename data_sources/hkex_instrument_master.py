"""HKEX instrument master source adapters and lifecycle policy.

The HKEX master policy keeps official HKEX/HKEXnews lifecycle evidence separate
from supplemental market-data sources. Supplemental rows may help discovery and
metadata fill, but they must not activate or delist instruments on their own.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from io import BytesIO, StringIO
from typing import AbstractSet, Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

import pandas as pd

from research.announcements.categories import (
    CAPITAL_REORGANISATION_CATEGORY,
    CIS_MATTERS_CATEGORY,
    LISTING_BY_INTRODUCTION_CATEGORY,
    TRADING_ARRANGEMENT_CATEGORY,
    TRADING_HALT_CATEGORY,
    TRADING_RESUMPTION_CATEGORY,
    TRADING_SUSPENSION_CATEGORY,
    WITHDRAWAL_OF_LISTING_CATEGORY,
    normalize_announcement_category,
)
from utils.http_transport import HttpTlsConfig, urlopen_bytes

HKEX_TRADING_HALT_SOURCE = "hkexnews_trading_halt"
HKEX_TRADING_RESUMPTION_SOURCE = "hkexnews_trading_resumption"
HKEX_TRADING_ARRANGEMENT_SOURCE = "hkexnews_trading_arrangement"
HKEX_PRODUCT_CESSATION_SOURCE = "hkexnews_product_cessation"
HKEX_TRADING_ELIGIBILITY_SOURCE = "hkexnews_trading_eligibility"
HKEX_LIFECYCLE_OVERLAY_FIELDS = (
    "status",
    "trading_status",
    "source",
    "official_lifecycle_source",
    "lifecycle_evidence",
    "lifecycle_evidence_at",
)
_HKEX_RESUMPTION_NEGATION_TOKENS = (
    "CONTINUED SUSPENSION",
    "CONTINUATION OF SUSPENSION",
    "REMAIN SUSPENDED",
    "REMAINS SUSPENDED",
    "STILL SUSPENDED",
)
_HKEX_RESUMPTION_ACTION_TOKENS = (
    "RESUMPTION OF TRADING",
    "RESUME TRADING",
    "TRADING WILL RESUME",
    "TRADING RESUMED",
    "FULFILMENT OF RESUMPTION GUIDANCE",
    "FULFILLMENT OF RESUMPTION GUIDANCE",
)
_HKEX_REPORT_AS_OF_RE = re.compile(
    r"(?:as at|Posted on)\s+(\d{1,2}\s+[A-Za-z]+\.?\s+\d{4})",
    re.IGNORECASE,
)
HKEX_UNTRADABLE_SOURCES = frozenset(
    {
        HKEX_TRADING_ARRANGEMENT_SOURCE,
        HKEX_PRODUCT_CESSATION_SOURCE,
    }
)
HKEX_MANUAL_REVIEW_SOURCE = "hkex_manual_review"
HKEX_LISTING_ACTIVE_SOURCES = frozenset(
    {
        "hkex_securities_list",
        "hkexnews_active_list",
        HKEX_MANUAL_REVIEW_SOURCE,
    }
)
HKEX_LISTING_PRESENCE_SOURCES = frozenset(
    {
        "hkex_securities_list",
        "hkexnews_active_list",
    }
)
HKEX_PROLONGED_SUSPENSION_SOURCE = "hkexnews_suspension_report"
HKEX_PROLONGED_SUSPENSION_MARKETS = ("Main Board", "GEM")
HKEX_STICKY_CESSATION_OVERRIDE_SOURCES = frozenset(
    {
        HKEX_MANUAL_REVIEW_SOURCE,
        HKEX_TRADING_RESUMPTION_SOURCE,
    }
)
HKEX_TRADING_STATUS_EVENT_CATEGORIES = frozenset(
    {
        TRADING_HALT_CATEGORY,
        TRADING_SUSPENSION_CATEGORY,
        TRADING_RESUMPTION_CATEGORY,
    }
)
HKEX_TRADING_ELIGIBILITY_EVENT_CATEGORIES = frozenset(
    {
        TRADING_ARRANGEMENT_CATEGORY,
        CAPITAL_REORGANISATION_CATEGORY,
        LISTING_BY_INTRODUCTION_CATEGORY,
        WITHDRAWAL_OF_LISTING_CATEGORY,
        CIS_MATTERS_CATEGORY,
    }
)
_HKEX_HEADLINE_TAG_MAP = {
    "trading halt": TRADING_HALT_CATEGORY,
    "suspension": TRADING_SUSPENSION_CATEGORY,
    "resumption": TRADING_RESUMPTION_CATEGORY,
}
_HKEX_HEADLINE_TAG_RE = re.compile(r"\[([^\]]+)\]")
_HKEX_CIS_FORM_TOKENS = (
    "CESSATION",
    "TERMINATION",
    "TERMINATING",
    "CEASE TO BE LISTED",
    "LAST DAY OF DEALING",
)
_HKEX_WITHDRAWAL_ACTUAL_TOKENS = (
    "LAST DAY OF DEALING",
    "CESSATION OF DEALING",
    "CEASE TO BE LISTED",
)
_HKEX_WITHDRAWAL_DECISION_TOKENS = (
    "WITHDRAWAL OF LISTING",
    "CANCELLATION OF LISTING",
)
_HKEX_WITHDRAWAL_PROCEDURAL_TOKENS = (
    "PROPOSED",
    "PRE-CONDITIONAL",
    "MONTHLY UPDATE",
    "DELAY IN DESPATCH",
    "APPOINTMENT OF INDEPENDENT FINANCIAL ADVISER",
    "PROFIT WARNING",
    "LAPSE OF THE PROPOSAL",
    "LAPSE OF THE SCHEME",
    "NEEQ",
)
_HKEX_NON_EQUITY_SUBJECT_TOKENS = (
    "IN THE NOTES",
    "IN THE BONDS",
    "IN THE WARRANTS",
    "IN THE CONVERTIBLE",
    "IN THE DEBENTURE",
)
_HKEX_PRODUCT_CESSATION_TOKENS = (
    "CESSATION OF TRADING",
    "TERMINATION OF THE SUB-FUND",
    "TERMINATION OF THE FUND",
    "TERMINATION OF THE ETF",
    "TERMINATING SUB-FUND",
    "VOLUNTARY DEAUTHORISATION",
)
_HKEX_COUNTER_CURRENCY_RE = re.compile(
    r"\b(HKD|USD|RMB|CNY)\s+TRADING COUNTER",
    re.IGNORECASE,
)
_HKEX_EFFECTIVE_DATE_HINTS = (
    "effective",
    "last day of dealing",
    "cease",
    "cessation",
    "with effect",
)
_HKEX_RESUME_DATE_HINTS = (
    "commence",
    "commencement",
    "expected",
    "resume",
    "resumption",
    "dealings expected",
)
_HKEX_MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_HKEX_NAMED_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|"
    r"Oct|Nov|Dec)\.?\s+(\d{4})\b",
    re.IGNORECASE,
)
_HKEX_NUMERIC_DATE_RE = re.compile(r"\b(\d{1,2})[./](\d{1,2})[./](\d{4})\b")


HKEX_MASTER_PARSER_VERSION = "hkex-instrument-master-v2"
OFFICIAL_SOURCES = {"hkex_securities_list", "hkexnews_active_list", "hkexnews_delisted_list"}


def normalize_hkex_code(value: Any) -> str:
    """Return a 5-digit HKEX stock code."""
    text = str(value or "").strip()
    if not text:
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(5)


def hkex_instrument_id(code: Any) -> str:
    normalized = normalize_hkex_code(code)
    return f"{normalized}.HK" if normalized else ""


def _parse_board_lot(value: Any) -> Optional[int]:
    """REQ-12: 解析港股每手股数字符串 (如 '500' / '1,000') 为 int; 无效返回 None。"""
    if value is None:
        return None
    text = str(value).replace(",", "").replace(" ", "").strip()
    if not text:
        return None
    try:
        lot = int(float(text))
    except (TypeError, ValueError):
        return None
    return lot if lot > 0 else None


def _snapshot_hash(raw_text: str) -> str:
    return hashlib.sha256((raw_text or "").encode("utf-8")).hexdigest()


def _snapshot_hash_bytes(raw_bytes: bytes) -> str:
    return hashlib.sha256(raw_bytes or b"").hexdigest()


def _fetch_url_bytes(
    source_url: str,
    *,
    timeout_sec: float,
    user_agent: str,
    attempts: int = 3,
) -> bytes:
    if not source_url:
        raise ValueError("source_url is required")
    last_error: Optional[Exception] = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            return urlopen_bytes(
                source_url,
                timeout_sec=timeout_sec,
                user_agent=user_agent,
                tls_config=HttpTlsConfig(source_name="hkex"),
            )
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(2.0 * attempt, 5.0))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"failed to fetch {source_url}")


def _normalized_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for column in frame.columns:
        key = str(column).strip().lower().replace(" ", "_").replace("-", "_")
        renamed[column] = key
    return frame.rename(columns=renamed)


def classify_hkex_product(row: Dict[str, Any]) -> Dict[str, Any]:
    """Classify HKEX products into research scope and derivative/debt buckets."""
    instrument_id = str(row.get("instrument_id") or "").strip().upper()
    symbol = str(row.get("symbol") or row.get("code") or "").strip()
    category = str(row.get("category") or row.get("hkex_category") or "").strip().lower()
    sub_category = str(row.get("sub_category") or row.get("hkex_sub_category") or "").strip().lower()
    name = str(row.get("name") or row.get("stock_name") or "").strip().lower()
    currency = str(row.get("currency") or row.get("trading_currency") or "").strip().upper()
    rmb_counter = str(row.get("rmb_counter") or "").strip().upper()
    combined = " ".join([category, sub_category, name])

    numeric_code = _hkex_numeric_code(instrument_id=instrument_id, symbol=symbol)

    code_range = _classify_hkex_code_range(numeric_code)

    if rmb_counter in {"Y", "YES", "TRUE", "1"} or currency in {"CNY", "RMB"}:
        product_type = "rmb_counter"
        research_scope = "exclude"
    elif code_range is not None:
        product_type = code_range["product_type"]
        research_scope = code_range["research_scope"]
    elif "trading only" in combined or "nasdaq-amex pilot" in combined or "nasdaq amex pilot" in combined:
        product_type = "trading_only"
        research_scope = "exclude"
    elif _is_hkex_temporary_counter_code(numeric_code):
        product_type = "temporary_counter"
        research_scope = "exclude"
    elif "old code" in combined or " old" in combined or "-old" in combined or "(旧)" in combined:
        product_type = "old_code"
        research_scope = "exclude"
    elif (
        "rights" in f"{category} {sub_category}"
        or "warrants for share rights" in f"{category} {sub_category}"
    ):
        product_type = "subscription_right"
        research_scope = "exclude"
    elif "callable bull/bear" in combined or "bull/bear" in combined or "cbbc" in combined:
        product_type = "cbbc"
        research_scope = "exclude"
    elif "inline warrant" in combined:
        product_type = "inline_warrant"
        research_scope = "exclude"
    elif "warrant" in combined:
        product_type = "warrant"
        research_scope = "exclude"
    elif "leveraged and inverse" in combined or "leveraged/inverse" in combined:
        product_type = "leveraged_inverse_product"
        research_scope = "exclude"
    elif "spac warrant" in combined:
        product_type = "spac_warrant"
        research_scope = "exclude"
    elif "debt" in combined or "bond" in combined or "note" in combined:
        product_type = "debt"
        research_scope = "exclude"
    elif "exchange traded fund" in combined or " etf" in combined or "tracker fund" in combined:
        product_type = "etf"
        research_scope = "fund"
    elif "real estate investment trust" in combined or " reit" in combined or "产业信托" in name or "房产基金" in name:
        product_type = "reit"
        research_scope = "fund"
    elif "equity" in category or "ordinary" in combined or not category:
        product_type = "ordinary_equity"
        research_scope = "equity"
    else:
        product_type = "unknown"
        research_scope = "review"

    return {
        "product_type": product_type,
        "research_scope": research_scope,
        "is_research_equity": product_type == "ordinary_equity",
    }


def _hkex_numeric_code(*, instrument_id: str, symbol: str) -> Optional[int]:
    raw = symbol or instrument_id.split(".")[0]
    raw = raw.strip()
    if raw.isdigit():
        return int(raw)
    return None


def _is_hkex_temporary_counter_code(numeric_code: Optional[int]) -> bool:
    if numeric_code is None:
        return False
    return (
        2900 <= numeric_code <= 2999
        or 8551 <= numeric_code <= 8600
        or 82900 <= numeric_code <= 82999
    )


def _classify_hkex_code_range(numeric_code: Optional[int]) -> Optional[Dict[str, str]]:
    """Classify official HKEX stock-code allocation ranges that are out of research scope."""
    if numeric_code is None:
        return None

    if _is_hkex_temporary_counter_code(numeric_code):
        return {"product_type": "temporary_counter", "research_scope": "exclude"}
    if 10000 <= numeric_code <= 29999 or 89200 <= numeric_code <= 89599:
        return {"product_type": "warrant", "research_scope": "exclude"}
    if 30000 <= numeric_code <= 39999 or 70000 <= numeric_code <= 79999 or 90000 <= numeric_code <= 99999:
        return {"product_type": "stock_connect_special_counter", "research_scope": "exclude"}
    if (
        4000 <= numeric_code <= 4329
        or 4400 <= numeric_code <= 4599
        or 4700 <= numeric_code <= 4799
        or 5000 <= numeric_code <= 6029
        or 6750 <= numeric_code <= 6799
        or 40000 <= numeric_code <= 40999
        or 84300 <= numeric_code <= 84329
        or 84400 <= numeric_code <= 84599
        or 85000 <= numeric_code <= 85743
        or 85744 <= numeric_code <= 86029
        or 86600 <= numeric_code <= 86799
        or 89000 <= numeric_code <= 89099
    ):
        return {"product_type": "debt", "research_scope": "exclude"}
    if 4330 <= numeric_code <= 4399:
        return {"product_type": "trading_only", "research_scope": "exclude"}
    if 4600 <= numeric_code <= 4699 or 84600 <= numeric_code <= 84699:
        return {"product_type": "professional_preference_share", "research_scope": "exclude"}
    if 4800 <= numeric_code <= 4999:
        return {"product_type": "spac_warrant", "research_scope": "exclude"}
    if 6200 <= numeric_code <= 6299:
        return {"product_type": "hdr", "research_scope": "exclude"}
    if 6300 <= numeric_code <= 6399 or 86300 <= numeric_code <= 86399:
        return {"product_type": "restricted_security", "research_scope": "exclude"}
    if (
        6400 <= numeric_code <= 6599
        or 7000 <= numeric_code <= 7199
        or 41000 <= numeric_code <= 41499
        or 41600 <= numeric_code <= 46999
        or 49000 <= numeric_code <= 49499
        or 84000 <= numeric_code <= 84299
        or 84330 <= numeric_code <= 84399
        or 84700 <= numeric_code <= 84999
        or 86200 <= numeric_code <= 86299
        or 86400 <= numeric_code <= 86599
        or 87100 <= numeric_code <= 87199
        or 87800 <= numeric_code <= 88999
        or 89100 <= numeric_code <= 89199
        or 89700 <= numeric_code <= 89849
    ):
        return {"product_type": "reserved_or_transition_counter", "research_scope": "exclude"}
    if (
        7200 <= numeric_code <= 7399
        or 7500 <= numeric_code <= 7599
        or 7700 <= numeric_code <= 7799
        or 9200 <= numeric_code <= 9399
        or 9500 <= numeric_code <= 9599
        or 9700 <= numeric_code <= 9799
        or 87200 <= numeric_code <= 87399
        or 87500 <= numeric_code <= 87599
        or 87700 <= numeric_code <= 87799
    ):
        return {"product_type": "leveraged_inverse_product", "research_scope": "exclude"}
    if 7800 <= numeric_code <= 7999:
        return {"product_type": "spac_share", "research_scope": "exclude"}
    if 47000 <= numeric_code <= 48999:
        return {"product_type": "inline_warrant", "research_scope": "exclude"}
    if 49500 <= numeric_code <= 69999:
        return {"product_type": "cbbc", "research_scope": "exclude"}
    if 80000 <= numeric_code <= 89999:
        return {"product_type": "rmb_counter", "research_scope": "exclude"}

    return None


@dataclass
class HKEXProviderSnapshot:
    source: str
    source_url: str
    parser_version: str
    raw_snapshot_hash: str
    rows: List[Dict[str, Any]]
    diagnostics: Dict[str, Any] = field(default_factory=dict)


def classify_hkex_trading_status_headline(record: Any) -> Optional[str]:
    """Classify a HKEXnews headline by category/tag, never by free-text title."""
    payload = getattr(record, "raw_payload", None) or {}
    if not isinstance(payload, dict):
        payload = {}
    stamped = normalize_announcement_category(payload.get("headline_category"))
    if stamped in HKEX_TRADING_STATUS_EVENT_CATEGORIES:
        return stamped
    for field in (payload.get("SHORT_TEXT"), payload.get("LONG_TEXT")):
        for tag in _HKEX_HEADLINE_TAG_RE.findall(str(field or "")):
            mapped = _HKEX_HEADLINE_TAG_MAP.get(tag.strip().lower())
            if mapped:
                return mapped
    return None


def build_hkex_trading_status_snapshot(
    records: Iterable[Any],
    *,
    source_url: str = "https://www1.hkexnews.hk/search/titlesearch.xhtml",
    as_of: Optional[date] = None,
) -> HKEXProviderSnapshot:
    """Keep the latest halt/suspension/resumption headline per instrument."""
    latest_by_id: Dict[str, Dict[str, Any]] = {}
    seen = 0
    for record in records or []:
        event = classify_hkex_trading_status_headline(record)
        if event is None:
            continue
        title = str(getattr(record, "title", "") or "")
        event = _hkex_effective_trading_status_event(event, title)
        seen += 1
        published_at = str(getattr(record, "published_at", None) or "")
        published_dt = parse_hkex_lifecycle_evidence_at(published_at)
        names = tuple(getattr(record, "security_names", ()) or ())
        for raw_symbol in getattr(record, "symbols", ()) or ():
            instrument_id = hkex_instrument_id(raw_symbol)
            if not instrument_id:
                continue
            current = latest_by_id.get(instrument_id)
            current_dt = parse_hkex_lifecycle_evidence_at(
                (current or {}).get("published_at")
            )
            if current is None or (
                published_dt is not None
                and (current_dt is None or published_dt > current_dt)
            ):
                latest_by_id[instrument_id] = {
                    "instrument_id": instrument_id,
                    "symbol": normalize_hkex_code(raw_symbol),
                    "name": names[0] if names else title,
                    "event": event,
                    "published_at": published_at,
                    "title": title,
                    "announcement_id": str(
                        getattr(record, "source_announcement_id", "") or ""
                    ),
                }

    effective_as_of = as_of or date.today()
    rows: List[Dict[str, Any]] = []
    for item in latest_by_id.values():
        is_resumption = item["event"] == TRADING_RESUMPTION_CATEGORY
        dates = extract_hkex_timetable_dates(item["title"])
        resume_date = dates["expected_resume_date"]
        pending_resume = (
            is_resumption
            and resume_date is not None
            and effective_as_of < resume_date
        )
        tradable = is_resumption and not pending_resume
        source = (
            HKEX_TRADING_RESUMPTION_SOURCE
            if is_resumption
            else HKEX_TRADING_HALT_SOURCE
        )
        rows.append(
            {
                "instrument_id": item["instrument_id"],
                "symbol": item["symbol"],
                "name": item["name"],
                "exchange": "HKEX",
                "type": "stock",
                "status": "active" if tradable else "suspended",
                "is_active": True,
                "trading_status": 1 if tradable else 0,
                "source": source,
                "source_symbol": item["symbol"],
                "official_lifecycle_source": source,
                "lifecycle_evidence_at": item["published_at"],
                "lifecycle_evidence": {
                    "source": source,
                    "headline_category": item["event"],
                    "published_at": item["published_at"],
                    "title": item["title"],
                    "announcement_id": item["announcement_id"],
                    "expected_resume_date": (
                        resume_date.isoformat() if resume_date else None
                    ),
                    "source_url": source_url,
                },
            }
        )
    rows.sort(key=lambda row: str(row.get("instrument_id") or ""))
    raw_snapshot = json.dumps(
        [
            {
                "instrument_id": row["instrument_id"],
                "announcement_id": (row.get("lifecycle_evidence") or {}).get(
                    "announcement_id"
                ),
            }
            for row in rows
        ],
        ensure_ascii=False,
        sort_keys=True,
    )
    return HKEXProviderSnapshot(
        source=HKEX_TRADING_HALT_SOURCE,
        source_url=source_url,
        parser_version=HKEX_MASTER_PARSER_VERSION,
        raw_snapshot_hash=_snapshot_hash(raw_snapshot),
        rows=rows,
        diagnostics={
            "row_count": len(rows),
            "classified_count": seen,
            "latest_event_count": len(latest_by_id),
            "as_of": effective_as_of.isoformat(),
        },
    )


_HKEX_FUND_LIKE_CODE_RANGES = (
    (2800, 2849),
    (3000, 3799),
    (7200, 7399),
    (7500, 7599),
    (7700, 7799),
    (82800, 82849),
)


def _hkex_symbol_product(symbol: str) -> Dict[str, Any]:
    """Classify one HKEX code without using a shared announcement name."""
    instrument_id = hkex_instrument_id(symbol)
    return classify_hkex_product(
        {
            "instrument_id": instrument_id,
            "symbol": normalize_hkex_code(symbol),
        }
    )


def _is_hkex_fund_like_symbol(symbol: str) -> bool:
    product = _hkex_symbol_product(symbol)
    if product.get("research_scope") == "fund" or product.get("product_type") in {
        "etf",
        "reit",
        "leveraged_inverse_product",
    }:
        return True
    numeric = _hkex_numeric_code(
        instrument_id=hkex_instrument_id(symbol),
        symbol=normalize_hkex_code(symbol),
    )
    if numeric is None:
        return False
    return any(start <= numeric <= end for start, end in _HKEX_FUND_LIKE_CODE_RANGES)


def select_hkex_eligibility_symbols(record: Any, event: str) -> Tuple[str, ...]:
    """Attach an eligibility event only to the securities it actually names.

    HKEXnews puts the issuer ordinary share, notes, warrants and share classes
    into one STOCK_CODE field. A note last-day or ETF class cessation must not
    turn the ordinary share untradable.
    """
    symbols: List[str] = []
    for raw in getattr(record, "symbols", ()) or ():
        code = normalize_hkex_code(raw)
        if code and code not in symbols:
            symbols.append(code)
    if not symbols:
        return ()

    excluded: List[str] = []
    fund_like: List[str] = []
    equity_like: List[str] = []
    for symbol in symbols:
        product = _hkex_symbol_product(symbol)
        if _is_hkex_fund_like_symbol(symbol):
            fund_like.append(symbol)
        elif product.get("research_scope") == "exclude":
            excluded.append(symbol)
        else:
            equity_like.append(symbol)

    title = str(getattr(record, "title", "") or "")
    if event == CIS_MATTERS_CATEGORY:
        selected = list(fund_like)
        currencies = _hkex_title_terminated_counter_currencies(title)
        if currencies and _is_hkex_counter_only_cessation(title):
            selected = [
                symbol
                for symbol in selected
                if _hkex_symbol_counter_currency(symbol) in currencies
            ]
        return tuple(selected)
    if event == WITHDRAWAL_OF_LISTING_CATEGORY:
        title_upper = title.upper()
        note_subject = any(token in title_upper for token in _HKEX_NON_EQUITY_SUBJECT_TOKENS)
        if excluded and equity_like:
            return tuple(excluded + fund_like)
        if note_subject:
            return tuple(excluded + fund_like)
    if (
        event
        in {
            TRADING_ARRANGEMENT_CATEGORY,
            CAPITAL_REORGANISATION_CATEGORY,
            LISTING_BY_INTRODUCTION_CATEGORY,
        }
        and excluded
        and (equity_like or fund_like)
    ):
        return tuple(equity_like + fund_like)
    return tuple(symbols)


def classify_hkex_trading_eligibility_headline(record: Any) -> Optional[str]:
    """Classify arrangement/ETF eligibility by category or tag, never free-text type."""
    payload = getattr(record, "raw_payload", None) or {}
    if not isinstance(payload, dict):
        payload = {}
    stamped = normalize_announcement_category(payload.get("headline_category"))
    if stamped in HKEX_TRADING_ELIGIBILITY_EVENT_CATEGORIES:
        if stamped == CIS_MATTERS_CATEGORY and not _hkex_title_has_tokens(
            record, _HKEX_CIS_FORM_TOKENS
        ):
            return None
        if stamped == WITHDRAWAL_OF_LISTING_CATEGORY and not _is_hkex_withdrawal_headline(
            record
        ):
            return None
        return stamped
    return None


def extract_hkex_timetable_dates(title: Any) -> Dict[str, Optional[date]]:
    """Extract effective/resume dates from an already-classified headline."""
    text = str(title or "")
    effective_date: Optional[date] = None
    resume_date: Optional[date] = None
    for parsed, start, end in _iter_hkex_title_dates(text):
        window = text[max(0, start - 48) : min(len(text), end + 16)].lower()
        is_resume = any(hint in window for hint in _HKEX_RESUME_DATE_HINTS)
        is_effective = any(hint in window for hint in _HKEX_EFFECTIVE_DATE_HINTS)
        if is_resume:
            resume_date = parsed
        elif is_effective and effective_date is None:
            effective_date = parsed
    return {
        "effective_date": effective_date,
        "expected_resume_date": resume_date,
    }


def hkex_row_expected_resume_date(row: Optional[Dict[str, Any]]) -> Optional[date]:
    """Read an explicit resume/commence date from a local or official lifecycle row."""
    item = row or {}
    candidates = [item.get("expected_resume_date")]
    evidence = item.get("lifecycle_evidence")
    if isinstance(evidence, dict):
        candidates.append(evidence.get("expected_resume_date"))
    for value in candidates:
        parsed = parse_hkex_lifecycle_evidence_at(value)
        if parsed is not None:
            return parsed.date()
    return None


def hkex_expected_resume_date_reached(
    row: Optional[Dict[str, Any]],
    as_of: Optional[date] = None,
) -> bool:
    """Return whether a dated untradable window has already reached its resume date."""
    resume_date = hkex_row_expected_resume_date(row)
    if resume_date is None:
        return False
    return (as_of or date.today()) >= resume_date


def is_hkex_sticky_untradable_local(
    row: Optional[Dict[str, Any]],
    *,
    official: Optional[Dict[str, Any]] = None,
    as_of: Optional[date] = None,
) -> bool:
    """Undated product cessation stays untradable after the scan window.

    Manual review ``active`` and later official resumption can end the state.
    A stored ``expected_resume_date`` that has already been reached also ends it.
    """
    item = row or {}
    source = str(item.get("source") or item.get("official_lifecycle_source") or "")
    if source != HKEX_PRODUCT_CESSATION_SOURCE:
        return False
    if item.get("trading_status") not in (0, "0", False):
        return False
    if _hkex_official_clears_sticky_cessation(official):
        return False
    if hkex_expected_resume_date_reached(item, as_of) or hkex_expected_resume_date_reached(
        official,
        as_of,
    ):
        return False
    return True


def is_hkex_untradable_window(
    category: str,
    *,
    title: Any,
    as_of: date,
) -> bool:
    """Return whether an eligibility headline currently blocks daily trading."""
    dates = extract_hkex_timetable_dates(title)
    effective_date = dates["effective_date"]
    resume_date = dates["expected_resume_date"]
    if resume_date is not None:
        if as_of >= resume_date:
            return False
        return effective_date is None or as_of >= effective_date
    if category == CIS_MATTERS_CATEGORY and _is_hkex_counter_only_cessation(title):
        return effective_date is not None and as_of >= effective_date
    if category in {CIS_MATTERS_CATEGORY, WITHDRAWAL_OF_LISTING_CATEGORY}:
        return effective_date is None or as_of >= effective_date
    return False


def build_hkex_trading_eligibility_snapshot(
    records: Iterable[Any],
    *,
    as_of: date,
    source_url: str = "https://www1.hkexnews.hk/search/titlesearch.xhtml",
) -> HKEXProviderSnapshot:
    """Keep current untradable arrangement/ETF windows without marking a halt."""
    latest_by_id: Dict[str, Dict[str, Any]] = {}
    seen = 0
    for record in records or []:
        event = classify_hkex_trading_eligibility_headline(record)
        if event is None:
            continue
        seen += 1
        published_at = str(getattr(record, "published_at", None) or "")
        names = tuple(getattr(record, "security_names", ()) or ())
        title = str(getattr(record, "title", "") or "")
        for raw_symbol in select_hkex_eligibility_symbols(record, event):
            instrument_id = hkex_instrument_id(raw_symbol)
            if not instrument_id:
                continue
            current = latest_by_id.get(instrument_id)
            if current is None or published_at > str(current.get("published_at") or ""):
                latest_by_id[instrument_id] = {
                    "instrument_id": instrument_id,
                    "symbol": normalize_hkex_code(raw_symbol),
                    "name": names[0] if names else title,
                    "event": event,
                    "published_at": published_at,
                    "title": title,
                    "announcement_id": str(
                        getattr(record, "source_announcement_id", "") or ""
                    ),
                }

    rows: List[Dict[str, Any]] = []
    for item in latest_by_id.values():
        if not is_hkex_untradable_window(
            item["event"],
            title=item["title"],
            as_of=as_of,
        ):
            continue
        dates = extract_hkex_timetable_dates(item["title"])
        source = (
            HKEX_PRODUCT_CESSATION_SOURCE
            if item["event"] in {CIS_MATTERS_CATEGORY, WITHDRAWAL_OF_LISTING_CATEGORY}
            else HKEX_TRADING_ARRANGEMENT_SOURCE
        )
        rows.append(
            {
                "instrument_id": item["instrument_id"],
                "symbol": item["symbol"],
                "name": item["name"],
                "exchange": "HKEX",
                "type": "stock",
                "status": "active",
                "is_active": True,
                "trading_status": 0,
                "source": source,
                "source_symbol": item["symbol"],
                "official_lifecycle_source": source,
                "lifecycle_evidence_at": item["published_at"],
                "expected_resume_date": (
                    dates["expected_resume_date"].isoformat()
                    if dates["expected_resume_date"]
                    else None
                ),
                "lifecycle_evidence": {
                    "source": source,
                    "headline_category": item["event"],
                    "published_at": item["published_at"],
                    "title": item["title"],
                    "announcement_id": item["announcement_id"],
                    "effective_date": (
                        dates["effective_date"].isoformat()
                        if dates["effective_date"]
                        else None
                    ),
                    "expected_resume_date": (
                        dates["expected_resume_date"].isoformat()
                        if dates["expected_resume_date"]
                        else None
                    ),
                    "source_url": source_url,
                },
            }
        )
    rows.sort(key=lambda row: str(row.get("instrument_id") or ""))
    raw_snapshot = json.dumps(
        [
            {
                "instrument_id": row["instrument_id"],
                "source": row.get("source"),
                "announcement_id": (row.get("lifecycle_evidence") or {}).get(
                    "announcement_id"
                ),
            }
            for row in rows
        ],
        ensure_ascii=False,
        sort_keys=True,
    )
    return HKEXProviderSnapshot(
        source=HKEX_TRADING_ELIGIBILITY_SOURCE,
        source_url=source_url,
        parser_version=HKEX_MASTER_PARSER_VERSION,
        raw_snapshot_hash=_snapshot_hash(raw_snapshot),
        rows=rows,
        diagnostics={
            "row_count": len(rows),
            "classified_count": seen,
            "latest_event_count": len(latest_by_id),
            "as_of": as_of.isoformat(),
        },
    )


def _hkex_record_title(record: Any) -> str:
    return str(getattr(record, "title", "") or "").upper()


def _hkex_title_has_tokens(record: Any, tokens: Tuple[str, ...]) -> bool:
    title = _hkex_record_title(record)
    return any(token in title for token in tokens)


def _is_hkex_withdrawal_headline(record: Any) -> bool:
    """Keep proposed privatisation / monthly updates out of the untradable set."""
    title = _hkex_record_title(record)
    if any(token in title for token in _HKEX_WITHDRAWAL_ACTUAL_TOKENS):
        return True
    if any(token in title for token in _HKEX_WITHDRAWAL_DECISION_TOKENS):
        return not any(token in title for token in _HKEX_WITHDRAWAL_PROCEDURAL_TOKENS)
    return False


def _hkex_title_terminated_counter_currencies(title: Any) -> Tuple[str, ...]:
    found: List[str] = []
    for match in _HKEX_COUNTER_CURRENCY_RE.finditer(str(title or "")):
        currency = match.group(1).upper()
        if currency == "CNY":
            currency = "RMB"
        if currency not in found:
            found.append(currency)
    return tuple(found)


def _hkex_symbol_counter_currency(symbol: str) -> str:
    numeric = _hkex_numeric_code(
        instrument_id=hkex_instrument_id(symbol),
        symbol=normalize_hkex_code(symbol),
    )
    if numeric is None:
        return "HKD"
    if 80000 <= numeric <= 89999:
        return "RMB"
    if 9000 <= numeric <= 9999:
        return "USD"
    return "HKD"


def _is_hkex_counter_only_cessation(title: Any) -> bool:
    if not _hkex_title_terminated_counter_currencies(title):
        return False
    text = str(title or "").upper()
    return not any(token in text for token in _HKEX_PRODUCT_CESSATION_TOKENS)


def _iter_hkex_title_dates(text: str) -> Iterable[Tuple[date, int, int]]:
    for match in _HKEX_NAMED_DATE_RE.finditer(text or ""):
        day = int(match.group(1))
        month = _HKEX_MONTH_MAP.get(match.group(2).rstrip(".").lower())
        year = int(match.group(3))
        parsed = _safe_hkex_date(year, month, day)
        if parsed is not None:
            yield parsed, match.start(), match.end()
    for match in _HKEX_NUMERIC_DATE_RE.finditer(text or ""):
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        parsed = _safe_hkex_date(year, month, day)
        if parsed is not None:
            yield parsed, match.start(), match.end()


def _safe_hkex_date(year: int, month: Optional[int], day: int) -> Optional[date]:
    if month is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_hkex_lifecycle_evidence_at(value: Any) -> Optional[datetime]:
    """Parse announcement or monthly-report timestamps for lifecycle overlays."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    text = html.unescape(str(value).strip())
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    for parsed_date, _start, _end in _iter_hkex_title_dates(text):
        return datetime(
            parsed_date.year,
            parsed_date.month,
            parsed_date.day,
            tzinfo=timezone.utc,
        )
    return None


def _hkex_incoming_lifecycle_wins(
    existing_at: Optional[datetime],
    incoming_at: Optional[datetime],
) -> bool:
    if incoming_at is None and existing_at is None:
        return True
    if incoming_at is None:
        return False
    if existing_at is None:
        return True
    return incoming_at >= existing_at


def hkex_official_row_is_untradable(row: Optional[Dict[str, Any]]) -> bool:
    """True when the merged official row is still an arrangement/cessation window."""
    item = row or {}
    source = str(item.get("source") or item.get("official_lifecycle_source") or "")
    if source not in HKEX_UNTRADABLE_SOURCES:
        return False
    return item.get("trading_status") in (0, "0", False)


def _hkex_row_is_untradable(row: Optional[Dict[str, Any]]) -> bool:
    return hkex_official_row_is_untradable(row)


def _hkex_row_makes_tradable(row: Optional[Dict[str, Any]]) -> bool:
    item = row or {}
    if str(item.get("status") or "").lower() == "suspended":
        return False
    return item.get("trading_status") not in (0, "0", False)


def _hkex_row_source(row: Optional[Dict[str, Any]]) -> str:
    item = row or {}
    return str(item.get("source") or item.get("official_lifecycle_source") or "")


def _hkex_official_clears_sticky_cessation(official: Optional[Dict[str, Any]]) -> bool:
    if official is None:
        return False
    if _hkex_row_source(official) not in HKEX_STICKY_CESSATION_OVERRIDE_SOURCES:
        return False
    return _hkex_row_makes_tradable(official)


def _hkex_incoming_overrides_untradable(
    existing: Dict[str, Any],
    incoming: Dict[str, Any],
    *,
    as_of: Optional[date] = None,
) -> bool:
    if not _hkex_row_makes_tradable(incoming):
        return False
    incoming_source = _hkex_row_source(incoming)
    if incoming_source == HKEX_MANUAL_REVIEW_SOURCE:
        return True
    if incoming_source == HKEX_TRADING_RESUMPTION_SOURCE:
        return _hkex_incoming_lifecycle_wins(
            parse_hkex_lifecycle_evidence_at((existing or {}).get("lifecycle_evidence_at")),
            parse_hkex_lifecycle_evidence_at((incoming or {}).get("lifecycle_evidence_at")),
        )
    return hkex_expected_resume_date_reached(existing, as_of) or hkex_expected_resume_date_reached(
        incoming,
        as_of,
    )


def overlay_hkex_lifecycle_fields(
    existing: Dict[str, Any],
    incoming: Dict[str, Any],
    *,
    as_of: Optional[date] = None,
) -> Dict[str, Any]:
    """Merge two official rows, letting later dated halt/resume/PDF evidence win."""
    combined = dict(existing or {})
    incoming_wins = _hkex_incoming_lifecycle_wins(
        parse_hkex_lifecycle_evidence_at((existing or {}).get("lifecycle_evidence_at")),
        parse_hkex_lifecycle_evidence_at((incoming or {}).get("lifecycle_evidence_at")),
    )
    if (
        _hkex_row_source(existing) == HKEX_MANUAL_REVIEW_SOURCE
        and _hkex_row_makes_tradable(existing)
        and _hkex_row_is_untradable(incoming)
    ):
        incoming_wins = False
    elif _hkex_row_is_untradable(existing) and _hkex_row_makes_tradable(incoming):
        incoming_wins = _hkex_incoming_overrides_untradable(
            existing,
            incoming,
            as_of=as_of,
        )
    for key, value in (incoming or {}).items():
        if value in (None, ""):
            continue
        if key in HKEX_LIFECYCLE_OVERLAY_FIELDS and not incoming_wins:
            continue
        combined[key] = value
    return combined


def _parse_hkex_suspension_report_as_of(raw_text: str) -> Optional[date]:
    as_at = None
    posted = None
    for match in _HKEX_REPORT_AS_OF_RE.finditer(raw_text or ""):
        parsed = next(_iter_hkex_title_dates(match.group(0)), None)
        if parsed is None:
            continue
        token = match.group(0).lower()
        if token.startswith("as at"):
            as_at = parsed[0]
        elif posted is None:
            posted = parsed[0]
    return as_at or posted


def _hkex_effective_trading_status_event(event: str, title: Any) -> str:
    if event != TRADING_RESUMPTION_CATEGORY:
        return event
    text = html.unescape(str(title or "")).upper()
    has_negation = any(token in text for token in _HKEX_RESUMPTION_NEGATION_TOKENS)
    has_action = any(token in text for token in _HKEX_RESUMPTION_ACTION_TOKENS)
    if has_negation and not has_action:
        return TRADING_SUSPENSION_CATEGORY
    return event


class HKEXSecuritiesListProvider:
    """Parser for official HKEX securities list snapshots."""

    source = "hkex_securities_list"

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def fetch_csv(self, *, timeout_sec: float = 20.0) -> HKEXProviderSnapshot:
        if not self.source_url:
            raise ValueError("source_url is required for HKEX securities-list fetch")
        raw_bytes = _fetch_url_bytes(
            self.source_url,
            timeout_sec=timeout_sec,
            user_agent="Quote-HKEX-InstrumentMaster/1.0",
        )
        if self.source_url.lower().endswith((".xlsx", ".xls")):
            return self.parse_excel(raw_bytes)
        raw = raw_bytes.decode("utf-8-sig", errors="replace")
        return self.parse_csv(raw)

    def parse_excel(self, raw_excel: bytes) -> HKEXProviderSnapshot:
        frame = pd.read_excel(BytesIO(raw_excel), dtype=str, header=None).fillna("")
        header_idx = None
        for idx, row in frame.iterrows():
            normalized_values = {str(value).strip().lower().replace(" ", "_") for value in row.tolist()}
            if "stock_code" in normalized_values and "name_of_securities" in normalized_values:
                header_idx = idx
                break
        if header_idx is None:
            raise ValueError("HKEX securities-list Excel header row not found")

        header = [str(value).strip() for value in frame.iloc[header_idx].tolist()]
        data = frame.iloc[header_idx + 1 :].copy()
        data.columns = header
        data = _normalized_columns(data)
        rows, skipped = self._rows_from_frame(data)
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash_bytes(raw_excel),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "excel"},
        )

    def parse_csv(self, raw_csv: str) -> HKEXProviderSnapshot:
        frame = _normalized_columns(pd.read_csv(StringIO(raw_csv), dtype=str).fillna(""))
        rows, skipped = self._rows_from_frame(frame)
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_csv),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "csv"},
        )

    def _rows_from_frame(self, frame: pd.DataFrame) -> tuple[List[Dict[str, Any]], int]:
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for item in frame.to_dict(orient="records"):
            code = normalize_hkex_code(item.get("stock_code") or item.get("code"))
            if not code:
                skipped += 1
                continue
            name = str(
                item.get("name_of_securities")
                or item.get("stock_name")
                or item.get("name")
                or ""
            ).strip()
            record = {
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": name,
                "exchange": "HKEX",
                "type": "stock",
                "currency": str(item.get("trading_currency") or "HKD").strip() or "HKD",
                "status": "active",
                "is_active": True,
                "trading_status": 1,
                "source": self.source,
                "source_symbol": code,
                "hkex_category": str(item.get("category") or "").strip(),
                "hkex_sub_category": str(item.get("sub_category") or "").strip(),
                "board_lot": str(item.get("board_lot") or "").strip(),
                "lot_size": _parse_board_lot(item.get("board_lot")),  # REQ-12: 港股每手股数
                "isin": str(item.get("isin") or "").strip(),
                "rmb_counter": str(item.get("rmb_counter") or "").strip(),
                "official_lifecycle_source": self.source,
                "source_url": self.source_url,
            }
            record.update(classify_hkex_product(record))
            rows.append(record)
        return rows, skipped


class HKEXNewsStockListProvider:
    """Parser for HKEXnews active and delisted lifecycle list snapshots."""

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def fetch_html(self, *, lifecycle_status: str, timeout_sec: float = 20.0) -> HKEXProviderSnapshot:
        if not self.source_url:
            raise ValueError("source_url is required for HKEXnews fetch")
        raw = _fetch_url_bytes(
            self.source_url,
            timeout_sec=timeout_sec,
            user_agent="Quote-HKEX-InstrumentMaster/1.0",
        ).decode("utf-8-sig", errors="replace")
        stripped = raw.lstrip()
        if stripped.startswith("[") or stripped.startswith("{"):
            return self.parse_json(raw, lifecycle_status=lifecycle_status)
        return self.parse_html(raw, lifecycle_status=lifecycle_status)

    def parse_json(self, raw_json: str, *, lifecycle_status: str) -> HKEXProviderSnapshot:
        payload = json.loads(raw_json)
        if isinstance(payload, dict):
            records = payload.get("data") or payload.get("rows") or payload.get("result") or []
        else:
            records = payload
        rows: List[Dict[str, Any]] = []
        skipped = 0
        status = lifecycle_status.lower().strip()
        for item in records or []:
            code = normalize_hkex_code(item.get("c") or item.get("code") or item.get("stock_code"))
            if not code:
                skipped += 1
                continue
            name = str(item.get("n") or item.get("name") or item.get("stock_name") or "").strip()
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": name,
                "exchange": "HKEX",
                "type": "stock",
                "status": "active" if status == "active" else "delisted",
                "is_active": status == "active",
                "trading_status": 1 if status == "active" else 0,
                "source": f"hkexnews_{status}_list",
                "source_symbol": code,
                "stock_id": item.get("i"),
                "security_id": item.get("s"),
                "delisted_date": str(item.get("delisting_date") or "").strip() or None,
                "lifecycle_evidence": {
                    "source": f"hkexnews_{status}_list",
                    "source_url": self.source_url,
                    "status": status,
                    "format": "json",
                },
            })

        source = f"hkexnews_{status}_list"
        return HKEXProviderSnapshot(
            source=source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_json),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": "json"},
        )

    def parse_html(self, raw_html: str, *, lifecycle_status: str) -> HKEXProviderSnapshot:
        stripped = raw_html.lstrip("\ufeff \n\r\t")
        if stripped.startswith("[") or stripped.startswith("{"):
            return self.parse_json(raw_html, lifecycle_status=lifecycle_status)
        tables = pd.read_html(StringIO(raw_html))
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for table in tables:
            frame = _normalized_columns(table.fillna(""))
            for item in frame.to_dict(orient="records"):
                code = normalize_hkex_code(item.get("stock_code") or item.get("code"))
                if not code:
                    skipped += 1
                    continue
                name = str(item.get("stock_name") or item.get("name") or "").strip()
                status = lifecycle_status.lower().strip()
                record = {
                    "instrument_id": hkex_instrument_id(code),
                    "symbol": code,
                    "name": name,
                    "exchange": "HKEX",
                    "type": "stock",
                    "status": "active" if status == "active" else "delisted",
                    "is_active": status == "active",
                    "trading_status": 1 if status == "active" else 0,
                    "source": f"hkexnews_{status}_list",
                    "source_symbol": code,
                    "market": str(item.get("market") or "").strip(),
                    "delisted_date": str(item.get("delisting_date") or "").strip() or None,
                    "lifecycle_evidence": {
                        "source": f"hkexnews_{status}_list",
                        "source_url": self.source_url,
                        "status": status,
                    },
                }
                rows.append(record)

        source = f"hkexnews_{lifecycle_status.lower().strip()}_list"
        return HKEXProviderSnapshot(
            source=source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_html),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "table_count": len(tables)},
        )


class HKEXSupplementalAdapter:
    """Normalize non-authoritative HKEX supplemental rows."""

    @staticmethod
    def parse_akshare_spot_csv(raw_csv: str, *, source_url: str = "") -> HKEXProviderSnapshot:
        frame = pd.read_csv(StringIO(raw_csv), dtype=str).fillna("")
        rows: List[Dict[str, Any]] = []
        for item in frame.to_dict(orient="records"):
            code = normalize_hkex_code(item.get("代码") or item.get("code"))
            if not code:
                continue
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": str(item.get("名称") or item.get("name") or "").strip(),
                "exchange": "HKEX",
                "type": "stock",
                "currency": "HKD",
                "source": "akshare_hk_spot_em",
                "source_symbol": code,
                "lifecycle_authoritative": False,
                "source_url": source_url,
            })
        return HKEXProviderSnapshot(
            source="akshare_hk_spot_em",
            source_url=source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_csv),
            rows=rows,
            diagnostics={"row_count": len(rows)},
        )

    @staticmethod
    def parse_eastmoney_profile_csv(raw_csv: str, *, source_url: str = "") -> HKEXProviderSnapshot:
        frame = _normalized_columns(pd.read_csv(StringIO(raw_csv), dtype=str).fillna(""))
        rows: List[Dict[str, Any]] = []
        for item in frame.to_dict(orient="records"):
            code = normalize_hkex_code(item.get("code"))
            if not code:
                continue
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": str(item.get("name") or "").strip(),
                "exchange": "HKEX",
                "type": "stock",
                "industry": str(item.get("industry") or "").strip() or None,
                "sector": str(item.get("sector") or "").strip() or None,
                "listed_date": str(item.get("listing_date") or "").strip() or None,
                "source": "eastmoney_hk_profile",
                "source_symbol": code,
                "lifecycle_authoritative": False,
                "source_url": source_url,
            })
        return HKEXProviderSnapshot(
            source="eastmoney_hk_profile",
            source_url=source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_csv),
            rows=rows,
            diagnostics={"row_count": len(rows)},
        )


class HKEXSuspensionReportProvider:
    """Parser for official HKEX prolonged-suspension reports.

    The live source is PDF. Text extraction goes through the shared PDF
    profile router; tests and operator fixtures can feed extracted text
    directly. Row parsing is engine-agnostic and keys off ticker/date blocks.
    """

    source = "hkexnews_suspension_report"

    def __init__(
        self,
        source_url: str = "",
        market: str = "",
        profile_name: Optional[str] = None,
    ):
        self.source_url = source_url
        self.market = market
        self.profile_name = profile_name

    def fetch_pdf(self, *, timeout_sec: float = 20.0) -> HKEXProviderSnapshot:
        if not self.source_url:
            raise ValueError("source_url is required for HKEX suspension-report fetch")
        raw_pdf = _fetch_url_bytes(
            self.source_url,
            timeout_sec=timeout_sec,
            user_agent="Quote-HKEX-InstrumentMaster/1.0",
        )
        return self.parse_pdf(raw_pdf)

    def parse_pdf(self, raw_pdf: bytes) -> HKEXProviderSnapshot:
        from research.document_processing.pdf import PdfParseRequest, build_router, resolve_profile

        profile = resolve_profile(self.profile_name)
        result = build_router(profile).parse(PdfParseRequest(content=raw_pdf, profile=profile))
        if result.status == "failed":
            raise RuntimeError("shared PDF parser failed")
        text = "\n".join(page.text for page in result.pages if page.text)
        snapshot = self.parse_text(text)
        snapshot.raw_snapshot_hash = _snapshot_hash_bytes(raw_pdf)
        snapshot.diagnostics["format"] = "pdf"
        snapshot.diagnostics["page_count"] = result.page_count
        snapshot.diagnostics["pdf_profile"] = profile.name
        snapshot.diagnostics["market"] = self.market
        return snapshot

    _DATE_RE = re.compile(r"\b\d{1,2}-[A-Za-z]{3}-\d{4}\b")
    _CODE_RE = re.compile(r"\((\d{1,5})\)")
    _HEADER_RE = re.compile(r"^\s*\d{1,3}\.?\s+\S")

    @classmethod
    def _is_report_row_start(cls, line: str) -> bool:
        return cls._HEADER_RE.match(line or "") is not None

    @classmethod
    def _extract_report_block(cls, block: List[str]) -> Optional[Dict[str, str]]:
        if not block:
            return None
        date_index = None
        for index, line in enumerate(block):
            if cls._DATE_RE.search(line or ""):
                date_index = index
                break
        if date_index is None or date_index == 0:
            return None

        start = 0
        for index in range(date_index - 1, -1, -1):
            if cls._is_report_row_start(block[index]):
                start = index
                break

        name_text = " ".join(line.strip() for line in block[start:date_index] if line.strip())
        code_matches = cls._CODE_RE.findall(name_text)
        if not code_matches:
            return None
        raw_code = code_matches[-1]
        code = normalize_hkex_code(raw_code)
        if not code:
            return None
        name = re.sub(rf"\(\s*{re.escape(raw_code)}\s*\)", "", name_text)
        name = re.sub(r"^\s*\d{1,3}\.?\s+", "", name).strip(" \t^*#")
        return {"code": code, "name": name}

    def parse_text(self, raw_text: str) -> HKEXProviderSnapshot:
        rows: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        skipped = 0
        current_block: List[str] = []
        report_as_of = _parse_hkex_suspension_report_as_of(raw_text)
        report_as_of_text = report_as_of.isoformat() if report_as_of else None

        def flush_block() -> None:
            nonlocal skipped
            parsed = self._extract_report_block(current_block)
            current_block.clear()
            if parsed is None:
                skipped += 1
                return
            code = parsed["code"]
            if code in seen:
                skipped += 1
                return
            seen.add(code)
            evidence = {
                "source": self.source,
                "source_url": self.source_url,
                "status": "suspended",
                "market": self.market,
            }
            if report_as_of_text:
                evidence["as_of"] = report_as_of_text
            rows.append({
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": parsed["name"],
                "exchange": "HKEX",
                "type": "stock",
                "status": "suspended",
                "is_active": True,
                "trading_status": 0,
                "source": self.source,
                "source_symbol": code,
                "market": self.market,
                "official_lifecycle_source": self.source,
                "source_url": self.source_url,
                "lifecycle_evidence_at": report_as_of_text,
                "lifecycle_evidence": evidence,
            })

        for line in (raw_text or "").splitlines():
            stripped = (line or "").strip()
            if not stripped:
                continue
            if stripped.startswith("Link to HKEXnews") or stripped.startswith("Posted on "):
                if current_block:
                    flush_block()
                continue
            current_block.append(stripped)
        if current_block:
            flush_block()
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_text),
            rows=rows,
            diagnostics={
                "row_count": len(rows),
                "skipped_count": skipped,
                "format": "text",
                "market": self.market,
            },
        )


class HKEXManualReviewProvider:
    """Parse operator-reviewed HKEX lifecycle evidence from JSON or CSV."""

    source = "hkex_manual_review"

    def __init__(self, source_url: str = ""):
        self.source_url = source_url

    def parse(self, raw_text: str) -> HKEXProviderSnapshot:
        stripped = (raw_text or "").lstrip("\ufeff \n\r\t")
        if stripped.startswith("[") or stripped.startswith("{"):
            return self.parse_json(raw_text)
        return self.parse_csv(raw_text)

    def parse_json(self, raw_json: str) -> HKEXProviderSnapshot:
        payload = json.loads(raw_json)
        if isinstance(payload, dict):
            records = payload.get("reviews") or payload.get("rows") or payload.get("data") or []
        else:
            records = payload
        return self._parse_records(records, raw_json, source_format="json")

    def parse_csv(self, raw_csv: str) -> HKEXProviderSnapshot:
        frame = _normalized_columns(pd.read_csv(StringIO(raw_csv), dtype=str).fillna(""))
        return self._parse_records(frame.to_dict(orient="records"), raw_csv, source_format="csv")

    def _parse_records(
        self,
        records: Iterable[Dict[str, Any]],
        raw_text: str,
        *,
        source_format: str,
    ) -> HKEXProviderSnapshot:
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for item in records or []:
            code = normalize_hkex_code(
                item.get("instrument_id")
                or item.get("stock_code")
                or item.get("code")
                or item.get("symbol")
            )
            if not code:
                skipped += 1
                continue
            action = str(item.get("action") or item.get("status") or "").strip().lower()
            if action in {"deactivate", "inactive", "delist", "delisted"}:
                status = "delisted"
            elif action in {"suspend", "suspended"}:
                status = "suspended"
            elif action in {"activate", "active", "reactivate"}:
                status = "active"
            else:
                skipped += 1
                continue

            effective_date = (
                str(item.get("effective_date") or item.get("delisted_date") or "").strip()
                or None
            )
            evidence_at = parse_hkex_lifecycle_evidence_at(
                item.get("lifecycle_evidence_at")
                or item.get("reviewed_at")
                or effective_date
            )
            row = {
                "instrument_id": hkex_instrument_id(code),
                "symbol": code,
                "name": str(item.get("name") or item.get("stock_name") or "").strip(),
                "exchange": "HKEX",
                "type": "stock",
                "status": status,
                "is_active": status != "delisted",
                "trading_status": 0 if status in {"delisted", "suspended"} else 1,
                "source": self.source,
                "source_symbol": code,
                "delisted_date": effective_date if status == "delisted" else None,
                "review_reason": str(item.get("reason") or "").strip(),
                "reviewed_by": str(item.get("reviewed_by") or "").strip(),
                "evidence_url": str(item.get("evidence_url") or item.get("source_url") or "").strip(),
                "official_lifecycle_source": self.source,
                "source_url": self.source_url,
                "lifecycle_evidence": {
                    "source": self.source,
                    "source_url": self.source_url,
                    "status": status,
                    "effective_date": effective_date,
                    "evidence_url": str(item.get("evidence_url") or item.get("source_url") or "").strip(),
                    "format": source_format,
                },
            }
            if evidence_at is not None:
                row["lifecycle_evidence_at"] = evidence_at.isoformat()
            rows.append(row)
        return HKEXProviderSnapshot(
            source=self.source,
            source_url=self.source_url,
            parser_version=HKEX_MASTER_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_text),
            rows=rows,
            diagnostics={"row_count": len(rows), "skipped_count": skipped, "format": source_format},
        )


def normalize_hkex_prolonged_suspension_market(value: Any) -> str:
    """Normalize Main Board / GEM labels used by prolonged-suspension PDFs."""
    text = str(value or "").strip()
    lowered = text.lower().replace("_", " ")
    if lowered in {"main board", "mainboard", "mb"}:
        return "Main Board"
    if lowered == "gem":
        return "GEM"
    return text


def prolonged_suspension_market_from_row(row: Optional[Mapping[str, Any]]) -> str:
    """Return the prolonged-suspension market stored on a local or official row."""
    payload = dict(row or {})
    evidence = payload.get("lifecycle_evidence")
    if not isinstance(evidence, Mapping):
        evidence = {}
    for candidate in (payload.get("market"), evidence.get("market")):
        market = normalize_hkex_prolonged_suspension_market(candidate)
        if market in HKEX_PROLONGED_SUSPENSION_MARKETS:
            return market
    return ""


def _snapshot_prolonged_suspension_market(snapshot: Any) -> str:
    diagnostics = getattr(snapshot, "diagnostics", None) or {}
    market = normalize_hkex_prolonged_suspension_market(diagnostics.get("market"))
    if market in HKEX_PROLONGED_SUSPENSION_MARKETS:
        return market
    rows = getattr(snapshot, "rows", None) or ()
    if rows:
        return prolonged_suspension_market_from_row(rows[0])
    return ""


def infer_prolonged_suspension_markets(
    snapshots: Iterable[Any],
) -> Dict[str, Dict[str, Any]]:
    """Build a Main Board / GEM ledger from parsed prolonged-suspension snapshots."""
    ledger: Dict[str, Dict[str, Any]] = {}
    for snapshot in snapshots or []:
        if getattr(snapshot, "source", None) != HKEX_PROLONGED_SUSPENSION_SOURCE:
            continue
        market = _snapshot_prolonged_suspension_market(snapshot)
        if not market:
            continue
        rows = list(getattr(snapshot, "rows", None) or [])
        ledger[market] = {
            "status": "success" if rows else "empty",
            "row_count": len(rows),
        }
    return ledger


def allow_prolonged_suspension_reactivation(
    local: Optional[Mapping[str, Any]],
    policy: Optional[Mapping[str, Any]],
) -> bool:
    """Return True when the successful prolonged-suspension list covers this name."""
    available = [
        normalize_hkex_prolonged_suspension_market(item)
        for item in (policy or {}).get("prolonged_suspension_available_markets") or []
    ]
    market = prolonged_suspension_market_from_row(local)
    if market:
        return market in available
    return bool((policy or {}).get("prolonged_suspension_all_configured_available"))


def should_skip_prolonged_suspension_reactivation(
    local: Optional[Mapping[str, Any]],
    official: Optional[Mapping[str, Any]],
    policy: Optional[Mapping[str, Any]],
) -> bool:
    """Skip listing-only clearance when that market's prolonged PDF is unavailable."""
    local_row = local or {}
    official_row = official or {}
    if str(local_row.get("status") or "") != "suspended":
        return False
    if str(local_row.get("source") or "") != HKEX_PROLONGED_SUSPENSION_SOURCE:
        return False
    if str(official_row.get("source") or "") not in HKEX_LISTING_PRESENCE_SOURCES:
        return False
    return not allow_prolonged_suspension_reactivation(local_row, policy)


def hkex_source_usage_key(snapshot: Any) -> str:
    """Return a report key that keeps Main Board / GEM PDFs distinct."""
    source = str(getattr(snapshot, "source", "") or "").strip()
    if source == HKEX_PROLONGED_SUSPENSION_SOURCE:
        market = _snapshot_prolonged_suspension_market(snapshot)
        if market:
            return f"{source}:{market}"
    return source


def should_write_hkex_reactivation(
    item: Optional[Mapping[str, Any]],
    policy: Optional[Mapping[str, Any]],
    allowed_lifecycle_ids: Optional[AbstractSet[str]] = None,
) -> bool:
    """Return True when a reactivation candidate would actually be written."""
    payload = item or {}
    instrument_id = payload.get("instrument_id")
    if not instrument_id:
        return False
    if allowed_lifecycle_ids is not None and instrument_id not in allowed_lifecycle_ids:
        return False
    policy_payload = policy or {}
    if not policy_payload.get("reactivation_write_allowed"):
        return False
    local = payload.get("local") or {}
    official = payload.get("official") or {}
    if (
        str(local.get("status") or "") == "suspended"
        and not policy_payload.get("suspension_source_available")
    ):
        return False
    return not should_skip_prolonged_suspension_reactivation(
        local,
        official,
        policy_payload,
    )


class HKEXSourceEvidencePolicy:
    """Summarize source quorum and write gates for HKEX sync modes."""

    @staticmethod
    def assess(
        *,
        snapshots: Iterable[HKEXProviderSnapshot],
        errors: Iterable[str],
        official_active_rows: Iterable[Dict[str, Any]],
        official_delisted_rows: Iterable[Dict[str, Any]],
        trading_status_scan: Optional[Dict[str, Any]] = None,
        prolonged_suspension_markets: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        snapshot_list = list(snapshots or [])
        sources = {snapshot.source for snapshot in snapshot_list}
        error_list = list(errors or [])
        primary_active_available = "hkex_securities_list" in sources
        fallback_active_available = "hkexnews_active_list" in sources
        delisted_available = "hkexnews_delisted_list" in sources or "hkex_manual_review" in sources
        prolonged_ledger = {
            normalize_hkex_prolonged_suspension_market(name): dict(info or {})
            for name, info in dict(prolonged_suspension_markets or {}).items()
            if normalize_hkex_prolonged_suspension_market(name)
        }
        if not prolonged_ledger:
            prolonged_ledger = infer_prolonged_suspension_markets(snapshot_list)
        available_markets = [
            market
            for market, info in prolonged_ledger.items()
            if str(info.get("status") or "") == "success"
            and int(info.get("row_count") or 0) > 0
        ]
        available_markets = [
            market
            for market in HKEX_PROLONGED_SUSPENSION_MARKETS
            if market in available_markets
        ] + [
            market
            for market in available_markets
            if market not in HKEX_PROLONGED_SUSPENSION_MARKETS
        ]
        configured_markets = [
            market
            for market, info in prolonged_ledger.items()
            if str(info.get("status") or "") != "not_configured"
        ]
        all_configured_available = bool(configured_markets) and all(
            str(prolonged_ledger[market].get("status") or "") == "success"
            and int(prolonged_ledger[market].get("row_count") or 0) > 0
            for market in configured_markets
        )
        prolonged_suspension_available = bool(available_markets) or any(
            snapshot.source == HKEX_PROLONGED_SUSPENSION_SOURCE and snapshot.rows
            for snapshot in snapshot_list
        )
        suspension_available = False
        for snapshot in snapshot_list:
            if (
                snapshot.source in {"hkexnews_suspension_report", HKEX_TRADING_HALT_SOURCE}
                and snapshot.rows
            ):
                suspension_available = True
                break
            if snapshot.source == "hkex_manual_review" and any(
                str(row.get("status") or "").lower() == "suspended"
                for row in snapshot.rows
            ):
                suspension_available = True
                break
        has_active_rows = any(row.get("instrument_id") for row in official_active_rows or [])
        has_delisted_rows = any(row.get("instrument_id") for row in official_delisted_rows or [])
        active_fallback_used = not primary_active_available and fallback_active_available and has_active_rows
        scan_status = str((trading_status_scan or {}).get("status") or "").strip()
        scan_complete = bool((trading_status_scan or {}).get("is_complete")) and scan_status in {
            "success",
            "success_empty",
        }

        return {
            "sources": sorted(sources),
            "source_error_count": len(error_list),
            "primary_active_source_available": primary_active_available,
            "fallback_active_source_available": fallback_active_available,
            "active_fallback_used": active_fallback_used,
            "delisted_source_available": delisted_available,
            "suspension_source_available": suspension_available,
            "prolonged_suspension_source_available": prolonged_suspension_available,
            "prolonged_suspension_markets": prolonged_ledger,
            "prolonged_suspension_available_markets": available_markets,
            "prolonged_suspension_all_configured_available": all_configured_available,
            "trading_status_scan_complete": scan_complete,
            "untradable_restore_allowed": scan_complete,
            "safe_write_allowed": primary_active_available and has_active_rows and not error_list,
            "reactivation_write_allowed": primary_active_available and has_active_rows and not error_list,
            "delisting_write_allowed": has_delisted_rows and delisted_available,
            "suspension_write_allowed": suspension_available and not error_list,
        }


def build_dual_counter_map(records: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group HKEX counters sharing the same ISIN and expose canonical HKD legs."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        isin = str(record.get("isin") or "").strip()
        if not isin:
            continue
        groups.setdefault(isin, []).append(record)

    mapping: Dict[str, Dict[str, Any]] = {}
    for isin, items in groups.items():
        if len(items) < 2:
            continue
        canonical = None
        for item in sorted(items, key=lambda row: str(row.get("symbol") or "")):
            if str(item.get("currency") or "").upper() == "HKD":
                canonical = item
                break
        canonical = canonical or sorted(items, key=lambda row: str(row.get("symbol") or ""))[0]
        canonical_id = canonical.get("instrument_id")
        for item in items:
            mapping[item.get("instrument_id")] = {
                "isin": isin,
                "canonical_instrument_id": canonical_id,
                "is_canonical": item.get("instrument_id") == canonical_id,
                "counter_currency": item.get("currency"),
                "dual_counter_ids": sorted(row.get("instrument_id") for row in items if row.get("instrument_id")),
            }
    return mapping


class HKEXLifecyclePolicy:
    """Apply HKEX source-authority rules without performing database writes."""

    @staticmethod
    def build_decisions(
        *,
        local_rows: Iterable[Dict[str, Any]],
        official_active_rows: Iterable[Dict[str, Any]],
        official_delisted_rows: Iterable[Dict[str, Any]],
        supplemental_rows: Iterable[Dict[str, Any]] = (),
    ) -> Dict[str, Any]:
        local_by_id = {
            row.get("instrument_id"): row
            for row in local_rows
            if row.get("instrument_id")
        }
        active_by_id = {
            row.get("instrument_id"): row
            for row in official_active_rows
            if row.get("instrument_id")
        }
        delisted_by_id = {
            row.get("instrument_id"): row
            for row in official_delisted_rows
            if row.get("instrument_id")
        }
        supplemental_ids: Set[str] = {
            row.get("instrument_id")
            for row in supplemental_rows
            if row.get("instrument_id")
        }
        listing_active_ids = {
            row.get("instrument_id")
            for row in official_active_rows
            if row.get("instrument_id")
            and (
                row.get("source") in HKEX_LISTING_ACTIVE_SOURCES
                or row.get("listing_source_present")
            )
        }

        inserts: List[Dict[str, Any]] = []
        metadata_updates: List[Dict[str, Any]] = []
        reactivations: List[Dict[str, Any]] = []
        suspensions: List[Dict[str, Any]] = []
        delistings: List[Dict[str, Any]] = []
        review_required: List[Dict[str, Any]] = []

        for instrument_id, active_row in active_by_id.items():
            if instrument_id in delisted_by_id:
                continue
            local = local_by_id.get(instrument_id)
            official_status = str(active_row.get("status") or "active").lower()
            if local is None:
                inserts.append(active_row)
                continue
            if official_status == "suspended":
                suspensions.append({
                    "instrument_id": instrument_id,
                    "reason": "official_suspension_evidence",
                    "official": active_row,
                    "local": local,
                })
                continue
            if local.get("is_active") in (False, 0, "0") or str(local.get("status") or "") != "active":
                reactivations.append({
                    "instrument_id": instrument_id,
                    "reason": "official_active_evidence_overrides_local_inactive",
                    "official": active_row,
                    "local": local,
                })
            else:
                metadata_updates.append(active_row)

        for instrument_id, local in local_by_id.items():
            local_classification = classify_hkex_product(local)
            if local_classification.get("research_scope") == "exclude":
                continue
            if instrument_id in delisted_by_id:
                if instrument_id in listing_active_ids:
                    review_required.append({
                        "instrument_id": instrument_id,
                        "reason": "official_active_and_delisted_evidence_conflict",
                        "official": active_by_id.get(instrument_id),
                        "local": local,
                    })
                    continue
                delistings.append({
                    "instrument_id": instrument_id,
                    "reason": "official_delisted_evidence",
                    "official": delisted_by_id[instrument_id],
                    "local": local,
                })
                continue
            if instrument_id in active_by_id:
                continue
            if (
                local.get("is_active") in (True, 1, "1")
                and instrument_id not in active_by_id
            ):
                review_required.append({
                    "instrument_id": instrument_id,
                    "reason": "local_active_missing_from_official_active_without_delisting_evidence",
                    "local": local,
                    "supplemental_seen": instrument_id in supplemental_ids,
                })

        supplemental_only = sorted(supplemental_ids - set(active_by_id) - set(local_by_id))
        for instrument_id in supplemental_only[:50]:
            review_required.append({
                "instrument_id": instrument_id,
                "reason": "supplemental_only_candidate_requires_official_confirmation",
                "supplemental_seen": True,
            })

        return {
            "insert_candidates": inserts,
            "metadata_update_candidates": metadata_updates,
            "reactivation_candidates": reactivations,
            "suspension_candidates": suspensions,
            "delisting_candidates": delistings,
            "review_required": review_required,
            "counts": {
                "official_active": len(active_by_id),
                "official_delisted": len(delisted_by_id),
                "local": len(local_by_id),
                "supplemental": len(supplemental_ids),
                "insert_candidates": len(inserts),
                "metadata_update_candidates": len(metadata_updates),
                "reactivation_candidates": len(reactivations),
                "suspension_candidates": len(suspensions),
                "delisting_candidates": len(delistings),
                "review_required": len(review_required),
            },
        }


def build_quote_availability_diagnostics(
    *,
    local_rows: Iterable[Dict[str, Any]],
    yfinance_rows: Iterable[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    """Build quote-availability diagnostics without lifecycle mutation output."""
    local_by_id = {
        row.get("instrument_id"): row
        for row in local_rows
        if row.get("instrument_id")
    }
    yf_by_id = {
        row.get("instrument_id"): row
        for row in yfinance_rows
        if row.get("instrument_id")
    }

    no_local_quote = []
    stale_local_quote = []
    yfinance_only_quote = []
    local_only_quote = []

    for instrument_id, local in local_by_id.items():
        last_quote = local.get("last_quote")
        if not last_quote:
            no_local_quote.append(instrument_id)
        if local.get("quote_stale"):
            stale_local_quote.append(instrument_id)
        if instrument_id not in yf_by_id:
            local_only_quote.append(instrument_id)

    for instrument_id in yf_by_id:
        if instrument_id not in local_by_id:
            yfinance_only_quote.append(instrument_id)

    return {
        "source": "local_quote_yfinance_diagnostics",
        "lifecycle_authoritative": False,
        "local_count": len(local_by_id),
        "yfinance_count": len(yf_by_id),
        "no_local_quote_count": len(no_local_quote),
        "stale_local_quote_count": len(stale_local_quote),
        "yfinance_only_quote_count": len(yfinance_only_quote),
        "local_only_quote_count": len(local_only_quote),
        "no_local_quote_samples": sorted(no_local_quote)[:20],
        "stale_local_quote_samples": sorted(stale_local_quote)[:20],
        "yfinance_only_quote_samples": sorted(yfinance_only_quote)[:20],
        "local_only_quote_samples": sorted(local_only_quote)[:20],
        "mutation_candidates": [],
    }
