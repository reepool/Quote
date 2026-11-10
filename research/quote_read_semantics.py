"""Public daily-bar projections; raw storage and technical volume stay intact."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from utils.date_utils import DateUtils

MISSING_TURNOVER_SOURCES = frozenset({"pytdx", "tencent"})


def finite_number(value: Any) -> bool:
    """Return whether a scalar is a finite numeric value, excluding missing data."""
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def normalize_quote_quality(record: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize known missing placeholders without guessing other providers' zeros."""
    row = dict(record)
    missing = list(row.get("missing_fields") or [])
    reasons = dict(row.get("missing_reason") or {})
    turnover = row.get("turnover")
    unavailable = not finite_number(turnover) or (
        row.get("source") in MISSING_TURNOVER_SOURCES and float(turnover) == 0
    )
    if unavailable:
        row["turnover"] = None
        if "turnover" not in missing:
            missing.append("turnover")
        reasons["turnover"] = (
            "source_unavailable"
            if row.get("source") in MISSING_TURNOVER_SOURCES
            else "not_available"
        )
        score = row.get("quality_score")
        row["quality_score"] = min(float(score), 0.9) if finite_number(score) else 0.0
        if not finite_number(score):
            reasons["quality_score"] = "not_available"
    row["missing_fields"] = missing
    row["missing_reason"] = reasons
    return row


def normalize_quote_frame(data: Any) -> pd.DataFrame:
    """Return a copy carrying the same public quality used by both quote queries."""
    frame = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    if frame.empty:
        return frame.copy()
    return pd.DataFrame(
        [normalize_quote_quality(row) for row in frame.to_dict("records")]
    )


def filter_quote_frame(data: Any, filters: Mapping[str, Any]) -> pd.DataFrame:
    """Evaluate declared filters by intersection and fail on unavailable inputs."""
    frame = normalize_quote_frame(data)
    if frame.empty:
        return frame
    checks = [
        ("tradestatus", filters.get("tradestatus"), "equal"),
        ("min_volume", filters.get("min_volume"), "minimum"),
        ("is_complete", filters.get("is_complete"), "equal"),
        ("min_quality_score", filters.get("min_quality_score"), "minimum"),
    ]
    if not filters.get("include_suspended", True):
        if "tradestatus" not in frame or frame["tradestatus"].isna().any():
            raise ValueError("tradestatus is unavailable for suspension filtering")
        frame = frame[frame["tradestatus"] != 0]
    for name, value, operation in checks:
        if value is None:
            continue
        column = {"min_volume": "volume", "min_quality_score": "quality_score"}.get(
            name, name
        )
        if column not in frame:
            raise ValueError(f"{column} is unavailable for quote filtering")
        series = (
            pd.to_numeric(frame[column], errors="coerce")
            if operation == "minimum"
            else frame[column]
        )
        mask = series >= value if operation == "minimum" else series == value
        frame = frame[mask.fillna(False)]
    return frame


def quote_units(currency: str | None) -> dict[str, str | None]:
    """Describe actual units, including legacy aliases and raw reference fields."""
    units = {
        name: f"{currency}/share" if currency else None
        for name in (
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "raw_pre_close",
            "raw_change",
        )
    }
    units.update({name: "share" for name in ("volume", "volume_raw", "volume_adj")})
    units.update(
        amount=currency,
        turnover="percent",
        pct_change="percent",
        pct_change_pct="percent",
        pct_change_ratio="ratio",
        factor="ratio",
        quality_score="score_0_1",
    )
    return units


def preserve_raw_quote_fields(data: Any) -> pd.DataFrame:
    """Capture input references before the existing adjustment engine is called."""
    frame = normalize_quote_frame(data)
    for source, target in (
        ("volume", "volume_raw"),
        ("pre_close", "raw_pre_close"),
        ("change", "raw_change"),
    ):
        frame[target] = frame.get(source)
    return frame


def project_quote_frame(data: Any, instrument: Mapping[str, Any]) -> pd.DataFrame:
    """Expose actual shares while retaining the engine's adjusted share values."""
    frame = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    currency = instrument.get("currency")
    if not currency and instrument.get("exchange") in {"SSE", "SZSE", "BSE"}:
        currency = "CNY"
    for name in ("volume", "pre_close", "change"):
        target = "volume_raw" if name == "volume" else f"raw_{name}"
        if target not in frame:
            frame[target] = frame.get(name)
    frame["volume_adj"] = frame.get("volume")
    frame["volume"] = frame["volume_raw"]
    frame["currency"] = currency
    frame["pct_change_pct"] = frame.get("pct_change")
    frame["pct_change_ratio"] = (
        pd.to_numeric(frame["pct_change_pct"], errors="coerce") / 100
    )
    adjustment = frame.get("adjustment_type", "none")
    basis = {"forward": "qfq", "backward": "hfq", "none": "none"}
    frame["price_basis"] = (
        adjustment.map(basis)
        if isinstance(adjustment, pd.Series)
        else basis.get(adjustment, "none")
    )
    frame["volume_basis"] = "raw"
    frame["return_basis"] = "source_reference"
    return frame


def batch_identity_kind(row: Mapping[str, Any]) -> str | None:
    """Do not infer a run's provenance from the visual shape of an old batch id."""
    if not row.get("batch_id"):
        return None
    # Historical rows have no persisted writer-kind marker. Future explicitly
    # marked rows can carry it; otherwise honesty is preferable to guessing.
    return row.get("batch_id_kind") or "unknown"


async def eod_timing(
    row: Mapping[str, Any],
    instrument: Mapping[str, Any],
    db_ops: Any,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Count completed sessions using stored calendars, with explicit unknowns."""
    exchange = str(instrument.get("exchange") or "")
    timezone = {
        "SSE": "Asia/Shanghai",
        "SZSE": "Asia/Shanghai",
        "BSE": "Asia/Shanghai",
        "HKEX": "Asia/Hong_Kong",
        "NYSE": "America/New_York",
        "NASDAQ": "America/New_York",
    }.get(exchange)
    tz = ZoneInfo(timezone or "UTC")
    current = (now or datetime.now(tz)).astimezone(tz)
    value = pd.Timestamp(row["time"])
    bar_day = value.date()
    result = {
        "quote_type": "eod",
        "is_intraday": False,
        "as_of": datetime.combine(bar_day, time.min, tzinfo=tz),
        "computed_at": current,
        "staleness_trading_days": None,
        "staleness_reason": "calendar_unavailable",
    }
    loader = getattr(db_ops, "get_trading_calendar_records", None)
    if not timezone or not callable(loader):
        return result
    calendar = await loader(exchange, bar_day, current.date())
    by_day = {pd.Timestamp(item["date"]).date(): item for item in calendar}
    days = [
        bar_day + timedelta(days=i) for i in range((current.date() - bar_day).days + 1)
    ]
    if not days or any(day not in by_day for day in days):
        return result
    # Use the established session boundary and require actual calendar coverage.
    _, close = DateUtils.get_market_open_time("SSE" if exchange == "BSE" else exchange)
    cutoff = time(close.hour, close.minute, close.second)
    result["staleness_trading_days"] = sum(
        bool(by_day[day]["is_trading_day"])
        and (day < current.date() or current.time().replace(tzinfo=None) >= cutoff)
        for day in days
        if day > bar_day
    )
    result["staleness_reason"] = None
    return result
