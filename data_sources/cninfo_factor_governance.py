"""Official-first A-share corporate-action factor derivation and reconciliation."""

from __future__ import annotations

import math
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


CNINFO_FACTOR_PROFILE = "cninfo_event_derived_v1"
TDX_FACTOR_PROFILE = "tdx_event_derived_v1"
FACTOR_NORMALIZATION_VERSION = "official_event_formula_v1"
RECONCILIATION_PRECISION_POLICY_VERSION = "tdx_xdxr_observed_precision_v2"

# TDX exposes float32 values without a decimal scale. Stable significant-digit
# normalization removes binary noise; field caps prevent integer-looking
# values from implying an unreasonably coarse rounding allowance.
DEFAULT_ROUNDED_FIELD_TOLERANCE_CAPS = {
    "cash_per_share": 0.0005,
    "bonus_per_share": 0.005,
    "rights_per_share": 0.005,
    "rights_price": 0.005,
}
TDX_FLOAT_SIGNIFICANT_DIGITS = 7
DEFAULT_FACTOR_RELATIVE_TOLERANCE = 0.0001
ARCHIVE_TDX_DATE_MATCH_WINDOW_DAYS = 550
ARCHIVE_TDX_ANNOUNCEMENT_WINDOW_DAYS = 120
ARCHIVE_TDX_OPERATIONAL_LOOKBACK_DAYS = 31


def _date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _positive(value: Any) -> Optional[float]:
    number = _number(value)
    return number if number > 0 else None


def _reviewed_economic_terms_complete(
    row: Mapping[str, Any],
) -> bool:
    """Return whether reviewed fields fully repair one partial event."""
    resolved_fields = {
        str(item)
        for item in (row.get("resolved_economic_fields") or [])
        if str(item)
    }
    if not resolved_fields:
        return False

    source_profile = str(row.get("source_profile") or "")
    action_type = str(row.get("action_type") or "")
    cash = _number(row.get("cash_dividend_per_share"))
    bonus = _number(row.get("bonus_shares_per_share"))
    capitalization = _number(row.get("capitalization_shares_per_share"))
    rights = _number(row.get("rights_shares_per_share"))
    rights_price = _number(row.get("rights_price"))

    if source_profile == "cninfo_allotment" or action_type == "rights":
        required = {"rights_shares_per_share", "rights_price"}
        return bool(resolved_fields & required) and rights > 0 and rights_price > 0

    distribution_fields = {
        "cash_dividend_per_share": cash,
        "bonus_shares_per_share": bonus,
        "capitalization_shares_per_share": capitalization,
    }
    return any(
        field_name in resolved_fields and value > 0
        for field_name, value in distribution_fields.items()
    )


def _economic_terms_shape_complete(row: Mapping[str, Any]) -> bool:
    source_profile = str(row.get("source_profile") or "")
    action_type = str(row.get("action_type") or "")
    rights = _number(row.get("rights_shares_per_share"))
    rights_price = _number(row.get("rights_price"))
    if source_profile == "cninfo_allotment" or action_type == "rights":
        return rights > 0 and rights_price > 0
    return any(
        _number(row.get(field_name)) > 0
        for field_name in (
            "cash_dividend_per_share",
            "bonus_shares_per_share",
            "capitalization_shares_per_share",
        )
    )


def _exchange(instrument_id: str) -> Optional[str]:
    normalized = str(instrument_id or "").upper()
    if normalized.endswith(".SH"):
        return "SSE"
    if normalized.endswith(".SZ"):
        return "SZSE"
    if normalized.endswith((".BJ", ".BSE")):
        return "BSE"
    return None


def build_quote_evidence_keys(
    cninfo_rows: Iterable[Mapping[str, Any]],
    tdx_rows: Iterable[Mapping[str, Any]],
) -> List[Tuple[str, date]]:
    """Return unique instrument/source-date pairs requiring quote evidence."""
    keys = set()
    for row in cninfo_rows:
        if not bool(row.get("is_current", True)):
            continue
        if str(row.get("event_status") or "") == "failed":
            continue
        if str(row.get("resolved_factor_effect") or "").strip().lower() == "none":
            continue
        instrument_id = str(row.get("instrument_id") or "").strip()
        raw_ex_date = _date(row.get("ex_date"))
        resolved_effective_date = _date(row.get("resolved_effective_date"))
        source_date = (
            resolved_effective_date
            if bool(row.get("resolved_date_authoritative"))
            and resolved_effective_date is not None
            else raw_ex_date or resolved_effective_date
        )
        if instrument_id and source_date:
            keys.add((instrument_id, source_date))
    for row in tdx_rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        ex_date = _date(row.get("ex_date"))
        if instrument_id and ex_date:
            keys.add((instrument_id, ex_date))
    return sorted(keys)


def _quote_map(
    quote_evidence: Iterable[Mapping[str, Any]],
) -> Dict[Tuple[str, date], Dict[str, Any]]:
    result = {}
    for row in quote_evidence:
        instrument_id = str(row.get("instrument_id") or "").strip()
        source_date = _date(row.get("source_date"))
        effective_date = _date(row.get("effective_date"))
        if instrument_id and source_date:
            result[(instrument_id, source_date)] = {
                "effective_date": effective_date,
                "pre_close": _positive(row.get("pre_close")),
                "close": _positive(row.get("close")),
            }
    return result


def _event_factor(
    *,
    pre_close: float,
    cash_per_share: float,
    bonus_per_share: float,
    rights_per_share: float,
    rights_proceeds_per_share: float,
) -> Optional[float]:
    denominator = pre_close - cash_per_share + rights_proceeds_per_share
    if pre_close <= 0 or denominator <= 0:
        return None
    factor = (
        pre_close
        * (1.0 + bonus_per_share + rights_per_share)
        / denominator
    )
    return round(factor, 12) if math.isfinite(factor) and factor > 0 else None


def derive_cninfo_factor_path(
    observations: Iterable[Mapping[str, Any]],
    quote_evidence: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Aggregate current CNInfo events and derive one factor per effective session."""
    quote_lookup = _quote_map(quote_evidence)
    pending: List[Dict[str, Any]] = []
    excluded_no_effect: List[Dict[str, Any]] = []
    historical_gaps: List[Dict[str, Any]] = []
    historical_gap_anchors: Dict[str, List[Optional[date]]] = defaultdict(list)
    grouped: Dict[Tuple[str, date], Dict[str, Any]] = {}
    pending_source_dates: Dict[str, List[date]] = defaultdict(list)
    unlocated_pending_instruments = set()
    for row in observations:
        if not bool(row.get("is_current", True)):
            continue
        if str(row.get("event_status") or "") == "failed":
            continue
        instrument_id = str(row.get("instrument_id") or "").strip()
        raw_ex_date = _date(row.get("ex_date"))
        resolved_effective_date = _date(row.get("resolved_effective_date"))
        source_date = (
            resolved_effective_date
            if bool(row.get("resolved_date_authoritative"))
            and resolved_effective_date is not None
            else raw_ex_date or resolved_effective_date
        )
        event_key = str(row.get("source_event_key") or "")
        if not instrument_id:
            continue
        factor_effect = str(
            row.get("resolved_factor_effect") or "normal"
        ).strip().lower()
        if factor_effect == "none":
            suppressed_dates = sorted({
                value
                for value in (
                    raw_ex_date,
                    resolved_effective_date,
                    source_date,
                )
                if value is not None
            })
            excluded_no_effect.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "reason": (
                    str(row.get("factor_exclusion_reason") or "").strip()
                    or "resolved_factor_effect_none"
                ),
                "effective_date": (
                    source_date.isoformat() if source_date is not None else None
                ),
                "suppressed_dates": [
                    value.isoformat() for value in suppressed_dates
                ],
            })
            continue
        historical_gap_reason = str(
            row.get("historical_gap_reason") or ""
        ).strip()
        if historical_gap_reason and source_date is None:
            operational_anchors = [
                parsed
                for field_name in ("record_date", "pay_date", "share_arrival_date")
                if (parsed := _date(row.get(field_name))) is not None
            ]
            ordering_anchor = (
                min(operational_anchors)
                if operational_anchors
                else _date(row.get("announcement_date"))
            )
            historical_gaps.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "reason": historical_gap_reason,
                "resolution_state": row.get("resolution_state"),
                "date_match": row.get("historical_date_match"),
                "ordering_anchor_date": (
                    ordering_anchor.isoformat()
                    if ordering_anchor is not None else None
                ),
            })
            historical_gap_anchors[instrument_id].append(ordering_anchor)
            continue
        factor_override = _positive(row.get("resolved_factor_override"))
        if (
            factor_effect == "official_reference_price"
            and factor_override is None
        ):
            pending.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "reason": "missing_official_factor_override",
            })
            if source_date is not None:
                pending_source_dates[instrument_id].append(source_date)
            else:
                unlocated_pending_instruments.add(instrument_id)
            continue
        if source_date is None:
            pending.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "reason": "missing_ex_date",
            })
            unlocated_pending_instruments.add(instrument_id)
            continue
        quality_status = str(row.get("quality_status") or "")
        resolved_date_available = (
            quality_status == "partial_missing_ex_date"
            and raw_ex_date is None
            and resolved_effective_date is not None
        )
        resolved_missing_date = (
            resolved_date_available and _economic_terms_shape_complete(row)
        )
        if resolved_date_available and not resolved_missing_date:
            pending.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "source_ex_date": source_date.isoformat(),
                "reason": "partial_missing_economic_fields",
            })
            pending_source_dates[instrument_id].append(source_date)
            continue
        resolved_missing_fields = (
            quality_status in {
                "partial_missing_fields",
                "partial_missing_economic_fields",
                "partial_zero_effect",
            }
            and _reviewed_economic_terms_complete(row)
        )
        if (
            quality_status.startswith("partial_")
            and not resolved_missing_date
            and not resolved_missing_fields
        ):
            pending.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "source_ex_date": source_date.isoformat(),
                "reason": quality_status,
            })
            pending_source_dates[instrument_id].append(source_date)
            continue
        quote = quote_lookup.get((instrument_id, source_date))
        if not quote or quote.get("effective_date") is None:
            pending.append({
                "instrument_id": instrument_id,
                "source_event_key": event_key,
                "source_ex_date": source_date.isoformat(),
                "reason": "missing_effective_trade_date",
            })
            pending_source_dates[instrument_id].append(source_date)
            continue
        effective_date = quote["effective_date"]
        key = (instrument_id, effective_date)
        aggregate = grouped.setdefault(key, {
            "instrument_id": instrument_id,
            "source_ex_dates": set(),
            "source_date_terms": {},
            "effective_date": effective_date,
            "cash_per_share": 0.0,
            "bonus_per_share": 0.0,
            "rights_per_share": 0.0,
            "rights_proceeds_per_share": 0.0,
            "event_keys": [],
            "authoritative_event_keys": [],
            "factor_overrides": [],
            "date_evidence": [],
            "pre_close": quote.get("pre_close"),
        })
        rights_per_share = _number(row.get("rights_shares_per_share"))
        rights_price = _number(row.get("rights_price"))
        cash_per_share = _number(row.get("cash_dividend_per_share"))
        bonus_per_share = (
            _number(row.get("bonus_shares_per_share"))
            + _number(row.get("capitalization_shares_per_share"))
        )
        rights_proceeds_per_share = rights_per_share * rights_price
        source_terms = aggregate["source_date_terms"].setdefault(source_date, {
            "source_ex_date": source_date,
            "cash_per_share": 0.0,
            "bonus_per_share": 0.0,
            "rights_per_share": 0.0,
            "rights_proceeds_per_share": 0.0,
        })
        aggregate["source_ex_dates"].add(source_date)
        aggregate["cash_per_share"] += cash_per_share
        aggregate["bonus_per_share"] += bonus_per_share
        aggregate["rights_per_share"] += rights_per_share
        aggregate["rights_proceeds_per_share"] += rights_proceeds_per_share
        source_terms["cash_per_share"] += cash_per_share
        source_terms["bonus_per_share"] += bonus_per_share
        source_terms["rights_per_share"] += rights_per_share
        source_terms[
            "rights_proceeds_per_share"
        ] += rights_proceeds_per_share
        aggregate["event_keys"].append(event_key)
        if factor_effect == "official_reference_price":
            aggregate["factor_overrides"].append({
                "source_event_key": event_key,
                "factor": factor_override,
            })
        if bool(row.get("resolved_authoritative_override")):
            aggregate["authoritative_event_keys"].append(event_key)
        if resolved_missing_date:
            aggregate["date_evidence"].append({
                "source_event_key": event_key,
                "effective_date": resolved_effective_date,
                "date_basis": row.get("resolved_date_basis"),
                "evidence_source": row.get("resolved_evidence_source"),
                "evidence_key": row.get("resolved_evidence_key"),
            })

    events: List[Dict[str, Any]] = []
    observations_out: List[Dict[str, Any]] = []
    cumulative_by_instrument: Dict[str, float] = defaultdict(lambda: 1.0)
    blocked_instruments = set()
    for (instrument_id, effective_date), aggregate in sorted(grouped.items()):
        if instrument_id in unlocated_pending_instruments:
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "source_event_keys": aggregate["event_keys"],
                "reason": "prior_unlocated_event_pending",
            })
            continue
        if any(
            effective_date >= source_date
            for source_date in pending_source_dates.get(instrument_id, [])
        ):
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "source_event_keys": aggregate["event_keys"],
                "reason": "prior_event_pending",
            })
            continue
        if instrument_id in blocked_instruments:
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "source_event_keys": aggregate["event_keys"],
                "reason": "prior_event_pending",
            })
            continue
        factor_overrides = aggregate["factor_overrides"]
        if factor_overrides:
            if (
                len(factor_overrides) != 1
                or len(aggregate["event_keys"]) != 1
            ):
                pending.append({
                    "instrument_id": instrument_id,
                    "effective_date": effective_date.isoformat(),
                    "source_event_keys": aggregate["event_keys"],
                    "reason": "ambiguous_official_factor_override",
                })
                blocked_instruments.add(instrument_id)
                continue
            factor = round(factor_overrides[0]["factor"], 12)
            factor_basis = "official_reference_price"
        else:
            pre_close = _positive(aggregate.get("pre_close"))
            if pre_close is None:
                pending.append({
                    "instrument_id": instrument_id,
                    "effective_date": effective_date.isoformat(),
                    "source_event_keys": aggregate["event_keys"],
                    "reason": "missing_pre_close",
                })
                blocked_instruments.add(instrument_id)
                continue
            factor = 1.0
            current_reference_price = pre_close
            source_date_terms = [
                aggregate["source_date_terms"][source_date]
                for source_date in sorted(aggregate["source_date_terms"])
            ]
            for source_terms in source_date_terms:
                source_factor = _event_factor(
                    pre_close=current_reference_price,
                    cash_per_share=source_terms["cash_per_share"],
                    bonus_per_share=source_terms["bonus_per_share"],
                    rights_per_share=source_terms["rights_per_share"],
                    rights_proceeds_per_share=source_terms[
                        "rights_proceeds_per_share"
                    ],
                )
                if source_factor is None:
                    factor = None
                    break
                factor = round(factor * source_factor, 12)
                current_reference_price /= source_factor
            factor_basis = (
                "ordinary_economic_terms_compounded"
                if len(source_date_terms) > 1
                else "ordinary_economic_terms"
            )
        pre_close = _positive(aggregate.get("pre_close"))
        if factor is None:
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "source_event_keys": aggregate["event_keys"],
                "reason": "invalid_factor_denominator",
            })
            blocked_instruments.add(instrument_id)
            continue
        cumulative_by_instrument[instrument_id] = round(
            cumulative_by_instrument[instrument_id] * factor,
            12,
        )
        source_dates = sorted(aggregate["source_ex_dates"])
        event = {
            "instrument_id": instrument_id,
            "source_ex_date": source_dates[0],
            "source_ex_dates": source_dates,
            "effective_date": effective_date,
            "cash_per_share": round(aggregate["cash_per_share"], 10),
            "bonus_per_share": round(aggregate["bonus_per_share"], 10),
            "rights_per_share": round(aggregate["rights_per_share"], 10),
            "rights_proceeds_per_share": round(
                aggregate["rights_proceeds_per_share"], 10
            ),
            "rights_price": round(
                aggregate["rights_proceeds_per_share"]
                / aggregate["rights_per_share"],
                10,
            ) if aggregate["rights_per_share"] > 0 else 0.0,
            "pre_close": pre_close,
            "factor": factor,
            "factor_basis": factor_basis,
            "cumulative_factor": cumulative_by_instrument[instrument_id],
            "source_event_keys": aggregate["event_keys"],
            "authoritative_override": bool(aggregate["event_keys"])
            and len(aggregate["authoritative_event_keys"])
            == len(aggregate["event_keys"]),
            "resolved_date_evidence": aggregate["date_evidence"],
            "source_date_terms": [
                {
                    **terms,
                    "source_ex_date": terms["source_ex_date"].isoformat(),
                }
                for terms in (
                    aggregate["source_date_terms"][source_date]
                    for source_date in sorted(aggregate["source_date_terms"])
                )
            ],
            "date_shifted": any(value != effective_date for value in source_dates),
            "path_has_prior_historical_gap": any(
                anchor is None or effective_date >= anchor
                for anchor in historical_gap_anchors.get(instrument_id, ())
            ),
        }
        events.append(event)
        observations_out.append({
            "instrument_id": instrument_id,
            "ex_date": datetime.combine(effective_date, datetime.min.time()),
            "source": "cninfo",
            "source_profile": CNINFO_FACTOR_PROFILE,
            "provider_factor": factor,
            "provider_cumulative_factor": cumulative_by_instrument[instrument_id],
            "normalized_factor": factor,
            "normalization_version": FACTOR_NORMALIZATION_VERSION,
            "quality_status": (
                "partial_prior_historical_gap"
                if event["path_has_prior_historical_gap"]
                else "valid"
            ),
            "raw_payload": event,
        })
    return {
        "observations": observations_out,
        "events": events,
        "pending": pending,
        "excluded_no_effect": excluded_no_effect,
        "historical_gaps": historical_gaps,
    }


def derive_tdx_factor_path(
    rows: Iterable[Mapping[str, Any]],
    quote_evidence: Iterable[Mapping[str, Any]],
    *,
    terminal_dates_by_instrument: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Rebuild a TDX event-product path while aligning source dates to sessions."""
    quote_lookup = _quote_map(quote_evidence)
    terminal_dates = {
        str(instrument_id).strip(): parsed
        for instrument_id, value in (
            terminal_dates_by_instrument or {}
        ).items()
        if str(instrument_id).strip()
        if (parsed := _date(value)) is not None
    }
    grouped: Dict[Tuple[str, date], Dict[str, Any]] = {}
    pending: List[Dict[str, Any]] = []
    excluded_terminal: List[Dict[str, Any]] = []
    pending_source_dates: Dict[str, List[date]] = defaultdict(list)
    unlocated_pending_instruments = set()
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        source_date = _date(row.get("ex_date"))
        factor = _positive(row.get("factor"))
        validation_result = str(row.get("validation_result") or "")
        if not instrument_id:
            continue
        if source_date is None:
            pending.append({
                "instrument_id": instrument_id,
                "reason": "missing_ex_date",
            })
            unlocated_pending_instruments.add(instrument_id)
            continue
        quote = quote_lookup.get((instrument_id, source_date))
        effective_date = quote.get("effective_date") if quote else None
        if effective_date is None:
            terminal_date = terminal_dates.get(instrument_id)
            if (
                quote is not None
                and terminal_date is not None
                and source_date <= terminal_date
            ):
                excluded_terminal.append({
                    "instrument_id": instrument_id,
                    "tdx_id": row.get("id"),
                    "source_ex_date": source_date,
                    "effective_date": None,
                    "factor": factor,
                    "cash_per_share": _number(row.get("fenhong")) / 10.0,
                    "bonus_per_share": _number(row.get("songzhuangu")) / 10.0,
                    "rights_per_share": _number(row.get("peigu")) / 10.0,
                    "rights_price": _number(row.get("peigujia")),
                    "reason": "terminal_no_post_event_trade",
                    "lifecycle": {
                        "terminal_date": terminal_date,
                        "terminal_type": "delisted",
                    },
                })
                continue
        if factor is None or validation_result.startswith("pending_"):
            pending.append({
                "instrument_id": instrument_id,
                "source_ex_date": source_date.isoformat(),
                "reason": validation_result or "missing_factor",
            })
            pending_source_dates[instrument_id].append(source_date)
            continue
        if effective_date is None:
            pending.append({
                "instrument_id": instrument_id,
                "source_ex_date": source_date.isoformat(),
                "reason": "missing_effective_trade_date",
            })
            pending_source_dates[instrument_id].append(source_date)
            continue
        key = (instrument_id, effective_date)
        aggregate = grouped.setdefault(key, {
            "instrument_id": instrument_id,
            "source_ex_dates": [],
            "effective_date": effective_date,
            "factor": 1.0,
            "cash_per_share": 0.0,
            "bonus_per_share": 0.0,
            "rights_per_share": 0.0,
            "rights_price": 0.0,
            "rights_proceeds_per_share": 0.0,
        })
        aggregate["source_ex_dates"].append(source_date)
        aggregate["factor"] *= factor
        aggregate["cash_per_share"] += _number(row.get("fenhong")) / 10.0
        aggregate["bonus_per_share"] += _number(row.get("songzhuangu")) / 10.0
        rights_per_share = _number(row.get("peigu")) / 10.0
        rights_price = _number(row.get("peigujia"))
        aggregate["rights_per_share"] += rights_per_share
        aggregate["rights_proceeds_per_share"] += (
            rights_per_share * rights_price
        )

    events: List[Dict[str, Any]] = []
    observations_out: List[Dict[str, Any]] = []
    cumulative_by_instrument: Dict[str, float] = defaultdict(lambda: 1.0)
    blocked_instruments = set()
    for (instrument_id, effective_date), aggregate in sorted(grouped.items()):
        if instrument_id in unlocated_pending_instruments:
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "reason": "prior_unlocated_event_pending",
            })
            continue
        if any(
            effective_date >= source_date
            for source_date in pending_source_dates.get(instrument_id, [])
        ):
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "reason": "prior_event_pending",
            })
            continue
        if instrument_id in blocked_instruments:
            pending.append({
                "instrument_id": instrument_id,
                "effective_date": effective_date.isoformat(),
                "reason": "prior_event_pending",
            })
            continue
        factor = round(aggregate["factor"], 12)
        aggregate["rights_price"] = (
            aggregate["rights_proceeds_per_share"]
            / aggregate["rights_per_share"]
            if aggregate["rights_per_share"] > 0
            else 0.0
        )
        cumulative_by_instrument[instrument_id] = round(
            cumulative_by_instrument[instrument_id] * factor,
            12,
        )
        event = {
            **aggregate,
            "source_ex_date": min(aggregate["source_ex_dates"]),
            "factor": factor,
            "cumulative_factor": cumulative_by_instrument[instrument_id],
            "date_shifted": any(
                value != effective_date for value in aggregate["source_ex_dates"]
            ),
        }
        events.append(event)
        observations_out.append({
            "instrument_id": instrument_id,
            "ex_date": datetime.combine(effective_date, datetime.min.time()),
            "source": "tdx_xdxr",
            "source_profile": TDX_FACTOR_PROFILE,
            "provider_factor": factor,
            "provider_cumulative_factor": cumulative_by_instrument[instrument_id],
            "normalized_factor": factor,
            "normalization_version": FACTOR_NORMALIZATION_VERSION,
            "quality_status": "valid",
            "raw_payload": event,
        })
    return {
        "observations": observations_out,
        "events": events,
        "pending": pending,
        "excluded_terminal": excluded_terminal,
    }


def _session_distance(left: date, right: date, sessions: Sequence[date]) -> Optional[int]:
    if left == right:
        return 0
    if not sessions:
        return None
    if right > left:
        return bisect_right(sessions, right) - bisect_right(sessions, left)
    return -(bisect_right(sessions, left) - bisect_right(sessions, right))


def evaluate_coverage_intervals(
    rows: Iterable[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
    accepted_statuses: Iterable[str],
) -> Dict[str, Any]:
    """Merge accepted inclusive status intervals and report uncovered gaps."""
    if end_date < start_date:
        raise ValueError("end_date must not be earlier than start_date")
    accepted = {
        str(status or "").strip().lower()
        for status in accepted_statuses
        if str(status or "").strip()
    }
    intervals: List[Tuple[date, date]] = []
    for row in rows:
        status = str(row.get("coverage_status") or "").strip().lower()
        interval_start = _date(row.get("requested_start_date"))
        interval_end = _date(row.get("requested_end_date"))
        if (
            status not in accepted
            or interval_start is None
            or interval_end is None
            or interval_end < interval_start
            or interval_end < start_date
            or interval_start > end_date
        ):
            continue
        intervals.append((
            max(start_date, interval_start),
            min(end_date, interval_end),
        ))

    merged: List[List[date]] = []
    for interval_start, interval_end in sorted(intervals):
        if (
            not merged
            or interval_start > merged[-1][1] + timedelta(days=1)
        ):
            merged.append([interval_start, interval_end])
        elif interval_end > merged[-1][1]:
            merged[-1][1] = interval_end

    gaps: List[Dict[str, date]] = []
    cursor = start_date
    for interval_start, interval_end in merged:
        if interval_start > cursor:
            gaps.append({
                "start_date": cursor,
                "end_date": interval_start - timedelta(days=1),
            })
        if interval_end >= end_date:
            cursor = end_date + timedelta(days=1)
            break
        cursor = max(cursor, interval_end + timedelta(days=1))
    if cursor <= end_date:
        gaps.append({"start_date": cursor, "end_date": end_date})

    return {
        "covered": not gaps,
        "accepted_interval_count": len(intervals),
        "merged_intervals": [
            {"start_date": item[0], "end_date": item[1]}
            for item in merged
        ],
        "gaps": gaps,
    }


def _economic_differences(
    cninfo_event: Mapping[str, Any],
    tdx_event: Mapping[str, Any],
) -> Dict[str, float]:
    def rights_price(event: Mapping[str, Any]) -> float:
        rights = _number(event.get("rights_per_share"))
        if rights <= 0:
            return 0.0
        if event.get("rights_price") is not None:
            return _number(event.get("rights_price"))
        proceeds = _number(event.get("rights_proceeds_per_share"))
        return proceeds / rights

    def rights_proceeds(event: Mapping[str, Any]) -> float:
        if event.get("rights_proceeds_per_share") is not None:
            return _number(event.get("rights_proceeds_per_share"))
        return (
            _number(event.get("rights_per_share"))
            * rights_price(event)
        )

    return {
        "cash_per_share": abs(
            _number(cninfo_event.get("cash_per_share"))
            - _number(tdx_event.get("cash_per_share"))
        ),
        "bonus_per_share": abs(
            _number(cninfo_event.get("bonus_per_share"))
            - _number(tdx_event.get("bonus_per_share"))
        ),
        "rights_per_share": abs(
            _number(cninfo_event.get("rights_per_share"))
            - _number(tdx_event.get("rights_per_share"))
        ),
        "rights_price": abs(
            rights_price(cninfo_event) - rights_price(tdx_event)
        ),
        "rights_proceeds_per_share": abs(
            rights_proceeds(cninfo_event) - rights_proceeds(tdx_event)
        ),
    }


def _factor_relative_difference(
    left: Any,
    right: Any,
) -> Tuple[Optional[float], Optional[float]]:
    """Return absolute and relative factor difference for audit output."""
    left_value = _positive(left)
    right_value = _positive(right)
    if left_value is None or right_value is None:
        return None, None
    absolute = abs(left_value - right_value)
    denominator = max(abs(left_value), abs(right_value), 1e-12)
    return absolute, absolute / denominator


def _rounded_match_policy(
    field_tolerances: Optional[Mapping[str, float]],
) -> Dict[str, Any]:
    overrides: Dict[str, float] = {}
    for field_name, value in (field_tolerances or {}).items():
        if field_name not in DEFAULT_ROUNDED_FIELD_TOLERANCE_CAPS:
            continue
        try:
            normalized = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(normalized) and normalized >= 0:
            overrides[field_name] = normalized
    return {
        "version": RECONCILIATION_PRECISION_POLICY_VERSION,
        "significant_digits": TDX_FLOAT_SIGNIFICANT_DIGITS,
        "field_tolerance_caps": dict(DEFAULT_ROUNDED_FIELD_TOLERANCE_CAPS),
        "field_tolerance_overrides": overrides,
    }


def _observed_precision_tolerance(value: Any) -> float:
    """Infer half of the stable decimal quantum from one TDX float32 value."""
    number = abs(_number(value))
    if number <= 0:
        return 0.0
    try:
        stable = Decimal(format(number, f".{TDX_FLOAT_SIGNIFICANT_DIGITS}g"))
    except (InvalidOperation, ValueError):
        return 0.0
    exponent = stable.normalize().as_tuple().exponent
    quantum = Decimal(1).scaleb(exponent)
    return float(abs(quantum) / 2)


def _rounded_field_tolerances(
    tdx_event: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> Dict[str, float]:
    """Expand source-field precision into normalized economic allowances."""
    caps = dict(policy.get("field_tolerance_caps") or {})
    overrides = dict(policy.get("field_tolerance_overrides") or {})
    source_values = {
        "cash_per_share": tdx_event.get("cash_per_share"),
        "bonus_per_share": tdx_event.get("bonus_per_share"),
        "rights_per_share": tdx_event.get("rights_per_share"),
        "rights_price": tdx_event.get("rights_price"),
    }
    base: Dict[str, float] = {}
    for field_name, source_value in source_values.items():
        if field_name in overrides:
            base[field_name] = float(overrides[field_name])
            continue
        inferred = _observed_precision_tolerance(source_value)
        cap = float(caps.get(field_name, 0.0) or 0.0)
        base[field_name] = min(inferred, cap) if inferred > 0 else 0.0
    rights_tolerance = float(base.get("rights_per_share", 0.0) or 0.0)
    price_tolerance = float(base.get("rights_price", 0.0) or 0.0)
    rights_per_share = abs(_number(tdx_event.get("rights_per_share")))
    rights_price = abs(_number(tdx_event.get("rights_price")))
    base["rights_proceeds_per_share"] = (
        rights_price * rights_tolerance
        + rights_per_share * price_tolerance
        + rights_tolerance * price_tolerance
    )
    return base


def match_cninfo_archive_tdx_date(
    observation: Mapping[str, Any],
    tdx_rows: Sequence[Mapping[str, Any]],
    *,
    field_tolerance: float = 0.0001,
    anchor_window_days: int = ARCHIVE_TDX_DATE_MATCH_WINDOW_DAYS,
    announcement_window_days: int = ARCHIVE_TDX_ANNOUNCEMENT_WINDOW_DAYS,
) -> Dict[str, Any]:
    """Return one bounded TDX date match without adopting TDX economics."""
    instrument_id = str(observation.get("instrument_id") or "").strip()
    cninfo_event = {
        "cash_per_share": _number(
            observation.get("cash_dividend_per_share")
        ),
        "bonus_per_share": (
            _number(observation.get("bonus_shares_per_share"))
            + _number(observation.get("capitalization_shares_per_share"))
        ),
        "rights_per_share": _number(
            observation.get("rights_shares_per_share")
        ),
        "rights_price": _number(observation.get("rights_price")),
    }
    if not instrument_id:
        return {"matched": False, "reason": "missing_instrument_id"}
    if not any(
        cninfo_event[field_name] > 0
        for field_name in (
            "cash_per_share",
            "bonus_per_share",
            "rights_per_share",
        )
    ):
        return {
            "matched": False,
            "reason": "cninfo_observation_has_no_positive_economic_term",
        }

    operational_anchors = {
        field_name: parsed
        for field_name in ("record_date", "pay_date", "share_arrival_date")
        if (parsed := _date(observation.get(field_name))) is not None
    }
    announcement_date = _date(observation.get("announcement_date"))
    anchors = (
        operational_anchors
        if operational_anchors
        else (
            {"announcement_date": announcement_date}
            if announcement_date is not None
            else {}
        )
    )
    if not anchors:
        return {"matched": False, "reason": "cninfo_date_anchor_missing"}

    normalized_tolerance = max(0.0, float(field_tolerance))
    normalized_window = max(0, int(anchor_window_days))
    normalized_announcement_window = max(0, int(announcement_window_days))
    precision_policy = _rounded_match_policy(None)
    candidates: List[Dict[str, Any]] = []
    for row in tdx_rows:
        if str(row.get("instrument_id") or "").strip() != instrument_id:
            continue
        validation_result = str(row.get("validation_result") or "")
        if validation_result.startswith("pending_"):
            continue
        tdx_date = _date(row.get("ex_date"))
        if tdx_date is None:
            continue
        matched_anchors: List[Dict[str, Any]] = []
        for role, anchor in anchors.items():
            distance_days = (tdx_date - anchor).days
            if role == "announcement_date":
                eligible = 0 <= distance_days <= normalized_announcement_window
            elif role == "record_date":
                eligible = 0 <= distance_days <= normalized_window
            else:
                eligible = (
                    -ARCHIVE_TDX_OPERATIONAL_LOOKBACK_DAYS
                    <= distance_days
                    <= normalized_window
                )
            if eligible:
                matched_anchors.append({
                    "role": role,
                    "date": anchor.isoformat(),
                    "distance_days": distance_days,
                })
        if not matched_anchors:
            continue
        tdx_event = {
            "cash_per_share": _number(row.get("fenhong")) / 10.0,
            "bonus_per_share": _number(row.get("songzhuangu")) / 10.0,
            "rights_per_share": _number(row.get("peigu")) / 10.0,
            "rights_price": _number(row.get("peigujia")),
        }
        differences = _economic_differences(cninfo_event, tdx_event)
        rounded_tolerances = _rounded_field_tolerances(
            tdx_event,
            precision_policy,
        )
        matches = all(
            difference <= max(
                normalized_tolerance,
                rounded_tolerances.get(field_name, 0.0),
            )
            for field_name, difference in differences.items()
        )
        if not matches:
            continue
        candidates.append({
            "tdx_id": row.get("id"),
            "tdx_ex_date": tdx_date.isoformat(),
            "anchor_distance_days": min(
                abs(item["distance_days"]) for item in matched_anchors
            ),
            "matched_anchors": matched_anchors,
            "differences": differences,
            "rounded_field_tolerances": rounded_tolerances,
        })

    anchor_policy = {
        "anchor_dates": {
            role: value.isoformat() for role, value in anchors.items()
        },
        "operational_forward_window_days": normalized_window,
        "operational_lookback_days": ARCHIVE_TDX_OPERATIONAL_LOOKBACK_DAYS,
        "announcement_forward_window_days": normalized_announcement_window,
    }
    if not candidates:
        return {
            "matched": False,
            "reason": "tdx_economic_event_not_found_in_anchor_window",
            **anchor_policy,
        }
    if len(candidates) != 1:
        return {
            "matched": False,
            "reason": "ambiguous_tdx_archive_date_match",
            **anchor_policy,
            "candidates": candidates,
        }
    selected = candidates[0]
    return {
        "matched": True,
        "reason": "unique_tdx_archive_date_match",
        "effective_date": selected["tdx_ex_date"],
        "date_basis": "tdx_xdxr_archive_date_reference",
        "selected_tdx_event": selected,
        **anchor_policy,
    }


def _excluded_cninfo_event_dates(
    events: Sequence[Mapping[str, Any]],
) -> set[Tuple[str, date]]:
    excluded_dates = set()
    for item in events:
        instrument_id = str(item.get("instrument_id") or "").strip()
        if not instrument_id:
            continue
        for value in [
            item.get("effective_date"),
            *(item.get("suppressed_dates") or []),
        ]:
            parsed = _date(value)
            if parsed is not None:
                excluded_dates.add((instrument_id, parsed))
    return excluded_dates


def partition_tdx_rows_by_lineage(
    rows: Sequence[Mapping[str, Any]],
    lineage_by_instrument: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    """Exclude explicit predecessor regimes from the current-issuer factor path."""
    included: List[Dict[str, Any]] = []
    suppressed: List[Dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        instrument_id = str(row.get("instrument_id") or "").strip()
        event_date = _date(row.get("ex_date"))
        lineage = lineage_by_instrument.get(instrument_id)
        if not isinstance(lineage, Mapping) or event_date is None:
            included.append(row)
            continue
        current_starts = [
            parsed
            for regime in lineage.get("issuer_regimes") or ()
            if isinstance(regime, Mapping)
            and str(regime.get("role") or "").strip().lower() == "current"
            if (parsed := _date(regime.get("start_date"))) is not None
        ]
        current_start = min(current_starts) if current_starts else None
        transition = next((
            item
            for item in lineage.get("transitions") or ()
            if isinstance(item, Mapping)
            and _date(item.get("effective_date")) == event_date
            and str(item.get("price_continuity") or "").strip().lower()
            == "non_continuous"
            and str(
                item.get("adjustment_factor_policy") or ""
            ).strip().lower() == "no_synthetic_factor"
        ), None)
        reason = None
        if transition is not None:
            reason = "lineage_non_continuous_transition"
        elif current_start is not None and event_date < current_start:
            reason = "lineage_predecessor_issuer_event"
        if reason is None:
            included.append(row)
            continue
        suppressed.append({
            "instrument_id": instrument_id,
            "tdx_id": row.get("id"),
            "source_ex_date": event_date,
            "effective_date": event_date,
            "factor": row.get("factor"),
            "cash_per_share": _number(row.get("fenhong")) / 10.0,
            "bonus_per_share": _number(row.get("songzhuangu")) / 10.0,
            "rights_per_share": _number(row.get("peigu")) / 10.0,
            "rights_price": _number(row.get("peigujia")),
            "reason": reason,
            "lineage": {
                "catalog_version": lineage.get("catalog_version"),
                "active_issuer_start": current_start,
                "transition_type": (
                    transition.get("event_type")
                    if isinstance(transition, Mapping)
                    else None
                ),
                "price_continuity": (
                    transition.get("price_continuity")
                    if isinstance(transition, Mapping)
                    else None
                ),
                "adjustment_factor_policy": (
                    transition.get("adjustment_factor_policy")
                    if isinstance(transition, Mapping)
                    else None
                ),
            },
        })
    return {
        "included_rows": included,
        "suppressed_reference_events": suppressed,
    }


def reconcile_cninfo_tdx_events(
    cninfo_events: Sequence[Mapping[str, Any]],
    tdx_events: Sequence[Mapping[str, Any]],
    *,
    excluded_cninfo_events: Sequence[Mapping[str, Any]] = (),
    pre_suppressed_reference_events: Sequence[Mapping[str, Any]] = (),
    sessions_by_exchange: Optional[Mapping[str, Sequence[date]]] = None,
    field_tolerance: float = 0.0001,
    rounded_field_tolerances: Optional[Mapping[str, float]] = None,
    factor_relative_tolerance: float = DEFAULT_FACTOR_RELATIVE_TOLERANCE,
    max_session_shift: int = 3,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Reconcile source dates and all supported economic fields."""
    sessions_by_exchange = sessions_by_exchange or {}
    cninfo = [dict(item) for item in cninfo_events]
    tdx = [dict(item) for item in tdx_events]
    used_cninfo: set[int] = set()
    used_tdx: set[int] = set()
    exact: List[Dict[str, Any]] = []
    shifted: List[Dict[str, Any]] = []
    rounded: List[Dict[str, Any]] = []
    accepted_overrides: List[Dict[str, Any]] = []
    suppressed_reference_events: List[Dict[str, Any]] = [
        dict(item) for item in pre_suppressed_reference_events
    ]
    conflicts: List[Dict[str, Any]] = []
    excluded_dates = _excluded_cninfo_event_dates(excluded_cninfo_events)
    for index, item in enumerate(tdx):
        instrument_id = str(item.get("instrument_id") or "").strip()
        reference_dates = {
            parsed
            for value in (
                item.get("source_ex_date"),
                item.get("effective_date"),
            )
            if (parsed := _date(value)) is not None
        }
        if any(
            (instrument_id, reference_date) in excluded_dates
            for reference_date in reference_dates
        ):
            used_tdx.add(index)
            suppressed_reference_events.append({
                **item,
                "tdx_index": index,
                "reason": "resolved_cninfo_factor_effect_none",
            })
    rounded_policy = _rounded_match_policy(rounded_field_tolerances)
    try:
        normalized_factor_tolerance = float(factor_relative_tolerance)
    except (TypeError, ValueError):
        normalized_factor_tolerance = DEFAULT_FACTOR_RELATIVE_TOLERANCE
    if (
        not math.isfinite(normalized_factor_tolerance)
        or normalized_factor_tolerance < 0
    ):
        normalized_factor_tolerance = DEFAULT_FACTOR_RELATIVE_TOLERANCE

    def detail(c_idx: int, t_idx: int, distance: int) -> Dict[str, Any]:
        left = cninfo[c_idx]
        right = tdx[t_idx]
        differences = _economic_differences(left, right)
        factor_difference, factor_relative_difference = _factor_relative_difference(
            left.get("factor"), right.get("factor")
        )
        return {
            "instrument_id": left["instrument_id"],
            "source_event_keys": list(left.get("source_event_keys") or []),
            "cninfo_source_date": _date(left.get("source_ex_date")),
            "tdx_source_date": _date(right.get("source_ex_date")),
            "cninfo_effective_date": _date(left.get("effective_date")),
            "tdx_effective_date": _date(right.get("effective_date")),
            "trading_session_distance": distance,
            "differences": differences,
            "factor_absolute_difference": factor_difference,
            "factor_relative_difference": factor_relative_difference,
            "cninfo_factor": left.get("factor"),
            "tdx_factor": right.get("factor"),
            "cninfo_index": c_idx,
            "tdx_index": t_idx,
        }

    for c_idx, left in enumerate(cninfo):
        for t_idx, right in enumerate(tdx):
            if t_idx in used_tdx or left["instrument_id"] != right["instrument_id"]:
                continue
            if _date(left.get("source_ex_date")) != _date(right.get("source_ex_date")):
                continue
            item = detail(c_idx, t_idx, 0)
            used_cninfo.add(c_idx)
            used_tdx.add(t_idx)
            if max(item["differences"].values(), default=0.0) <= field_tolerance:
                item["reason"] = "exact_event_match"
                exact.append(item)
            else:
                rounded_allowances = _rounded_field_tolerances(
                    right, rounded_policy
                )
                item["rounded_field_tolerances"] = rounded_allowances
                item["factor_relative_tolerance"] = normalized_factor_tolerance
                if (
                    all(
                        item["differences"].get(field_name, 0.0)
                        <= tolerance + max(1e-12, tolerance * 1e-9)
                        for field_name, tolerance in rounded_allowances.items()
                    )
                    and item["factor_relative_difference"] is not None
                    and item["factor_relative_difference"]
                    <= normalized_factor_tolerance
                ):
                    item["reason"] = "same_date_source_precision_match"
                    rounded.append(item)
                elif bool(left.get("authoritative_override")):
                    item["reason"] = "authoritative_cninfo_override"
                    accepted_overrides.append(item)
                else:
                    item["reason"] = "same_date_economic_conflict"
                    conflicts.append(item)
            break

    candidates = []
    for c_idx, left in enumerate(cninfo):
        if c_idx in used_cninfo:
            continue
        exchange = _exchange(str(left.get("instrument_id") or "")) or ""
        sessions = sessions_by_exchange.get(exchange, [])
        for t_idx, right in enumerate(tdx):
            if t_idx in used_tdx or left["instrument_id"] != right["instrument_id"]:
                continue
            left_date = _date(left.get("source_ex_date"))
            right_date = _date(right.get("source_ex_date"))
            if left_date is None or right_date is None:
                continue
            distance = _session_distance(left_date, right_date, sessions)
            if distance is None or abs(distance) > max_session_shift:
                continue
            item = detail(c_idx, t_idx, distance)
            candidates.append((
                max(item["differences"].values(), default=0.0),
                abs(distance),
                c_idx,
                t_idx,
                item,
            ))
    for difference, _, c_idx, t_idx, item in sorted(candidates):
        if c_idx in used_cninfo or t_idx in used_tdx:
            continue
        used_cninfo.add(c_idx)
        used_tdx.add(t_idx)
        if difference <= field_tolerance:
            item["reason"] = "shifted_event_match"
            shifted.append(item)
        elif bool(cninfo[c_idx].get("authoritative_override")):
            item["reason"] = "authoritative_cninfo_override_shifted"
            accepted_overrides.append(item)
        else:
            item["reason"] = "shifted_economic_conflict"
            conflicts.append(item)

    for index, item in enumerate(cninfo):
        if index in used_cninfo or not bool(item.get("authoritative_override")):
            continue
        accepted_overrides.append({
            **item,
            "cninfo_index": index,
            "reason": "authoritative_cninfo_event_only",
        })
        used_cninfo.add(index)
    cninfo_only = [
        {**item, "cninfo_index": index, "reason": "cninfo_event_only"}
        for index, item in enumerate(cninfo)
        if index not in used_cninfo
    ]
    tdx_only = [
        {**item, "tdx_index": index, "reason": "tdx_event_only"}
        for index, item in enumerate(tdx)
        if index not in used_tdx
    ]
    totals = {
        "cninfo_events": len(cninfo),
        "tdx_events": len(tdx),
        "exact_matches": len(exact),
        "rounded_matches": len(rounded),
        "shifted_matches": len(shifted),
        "accepted_authoritative_overrides": len(accepted_overrides),
        "suppressed_reference_events": len(suppressed_reference_events),
        "conflicts": len(conflicts),
        "cninfo_only": len(cninfo_only),
        "tdx_only": len(tdx_only),
    }
    return {
        "status": (
            "partial" if conflicts or cninfo_only or tdx_only else "success"
        ),
        "matching_policy": {
            "exact_field_tolerance": field_tolerance,
            "rounded_precision_policy": rounded_policy,
            "factor_relative_tolerance": normalized_factor_tolerance,
            "rounded_match_requires_same_source_date": True,
        },
        "totals": totals,
        "exact_matches": exact,
        "rounded_matches": rounded,
        "shifted_matches": shifted,
        "accepted_authoritative_overrides": accepted_overrides,
        "suppressed_reference_events": suppressed_reference_events,
        "conflicts": conflicts,
        "cninfo_only": cninfo_only,
        "tdx_only": tdx_only,
        "rounded_match_samples": rounded[:sample_limit],
        "samples": [
            *conflicts,
            *cninfo_only,
            *tdx_only,
            *accepted_overrides,
            *suppressed_reference_events,
            *shifted,
        ][:sample_limit],
    }


def build_cninfo_primary_candidate(
    cninfo_events: Sequence[Mapping[str, Any]],
    tdx_events: Sequence[Mapping[str, Any]],
    reconciliation: Mapping[str, Any],
    *,
    series_version: str,
    promotion_eligible: bool = False,
    excluded_cninfo_events: Sequence[Mapping[str, Any]] = (),
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build a staging candidate with CNInfo first and labelled TDX-only fallback."""
    conflict_indexes = {
        int(item["cninfo_index"])
        for item in reconciliation.get("conflicts", [])
        if item.get("cninfo_index") is not None
    }
    rows: List[Dict[str, Any]] = []
    cninfo_dates = set()
    for index, event in enumerate(cninfo_events):
        factor = _positive(event.get("factor"))
        effective_date = _date(event.get("effective_date"))
        if factor is None or effective_date is None:
            continue
        row = {
            "instrument_id": event["instrument_id"],
            "ex_date": effective_date,
            "factor": factor,
            "selected_source": "cninfo",
            "source_profile": CNINFO_FACTOR_PROFILE,
            "quality_status": (
                "source_conflict" if index in conflict_indexes else "valid"
            ),
            "evidence_count": 1,
        }
        rows.append(row)
        cninfo_dates.add((row["instrument_id"], row["ex_date"]))
    excluded_dates = _excluded_cninfo_event_dates(excluded_cninfo_events)
    fallback_dates = cninfo_dates | excluded_dates
    for item in reconciliation.get("tdx_only", []):
        index = item.get("tdx_index")
        if index is None or int(index) >= len(tdx_events):
            continue
        event = tdx_events[int(index)]
        factor = _positive(event.get("factor"))
        effective_date = _date(event.get("effective_date"))
        if factor is None or effective_date is None:
            continue
        key = (event["instrument_id"], effective_date)
        # CNInfo is primary for an effective session. A TDX-only event that
        # lands on an already occupied session must remain reconciliation data,
        # otherwise the staging table violates its one-row-per-session key.
        if key in fallback_dates:
            continue
        rows.append({
            "instrument_id": event["instrument_id"],
            "ex_date": effective_date,
            "factor": factor,
            "selected_source": "tdx_xdxr",
            "source_profile": TDX_FACTOR_PROFILE,
            "quality_status": "tdx_fallback_unverified",
            "evidence_count": 1,
        })
        fallback_dates.add(key)

    canonical: List[Dict[str, Any]] = []
    cumulative_by_instrument: Dict[str, float] = defaultdict(lambda: 1.0)
    for row in sorted(rows, key=lambda item: (item["instrument_id"], item["ex_date"])):
        instrument_id = row["instrument_id"]
        cumulative_by_instrument[instrument_id] = round(
            cumulative_by_instrument[instrument_id] * row["factor"],
            12,
        )
        canonical.append({
            **row,
            "ex_date": datetime.combine(row["ex_date"], datetime.min.time()),
            "series_version": series_version,
            "cumulative_factor": cumulative_by_instrument[instrument_id],
        })
    conflict_count = sum(
        row["quality_status"] != "valid" for row in canonical
    )
    return canonical, {
        "series_version": series_version,
        "row_count": len(canonical),
        "instrument_count": len({row["instrument_id"] for row in canonical}),
        "conflict_count": conflict_count,
        "tdx_fallback_count": sum(
            row["selected_source"] == "tdx_xdxr" for row in canonical
        ),
        "cninfo_no_effect_exclusion_count": len(excluded_dates),
        "promotion_eligible": bool(promotion_eligible),
    }
