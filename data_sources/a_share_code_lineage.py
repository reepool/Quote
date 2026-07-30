"""Govern reviewed A-share security-code lineage and missing quote repair."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "a_share_code_lineage.json"
)
ALLOWED_CONTINUITY_POLICIES = {"continuous", "non_continuous"}
ALLOWED_ADJUSTMENT_POLICIES = {"authoritative_factor", "no_synthetic_factor"}
ALLOWED_REVIEW_SOURCES = {"pytdx", "akshare_tx"}
OHLC_FIELDS = ("open", "high", "low", "close")


class LineageCatalogError(ValueError):
    """Raised when reviewed lineage configuration is invalid."""


class LineageReconciliationError(ValueError):
    """Raised when live source evidence is not fully covered by review decisions."""

    def __init__(self, message: str, diagnostics: Mapping[str, Any]):
        super().__init__(message)
        self.diagnostics = dict(diagnostics)


def _parse_date(value: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise LineageCatalogError(f"{field} must be an ISO date: {value!r}") from exc


def _finite_float(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric: {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field} must be finite: {value!r}")
    return result


@dataclass(frozen=True)
class IssuerRegime:
    issuer_name: str
    start_date: date
    end_date: date | None
    role: str


@dataclass(frozen=True)
class Transition:
    effective_date: date
    event_type: str
    from_regime_start: date
    to_regime_start: date
    price_continuity: str
    adjustment_factor_policy: str


@dataclass(frozen=True)
class ReviewedDecision:
    trade_date: date
    selected_source: str
    reason: str
    expected: Mapping[str, float]


@dataclass(frozen=True)
class LineageEntry:
    catalog_version: str
    reviewed_at: date
    instrument_id: str
    symbol: str
    exchange: str
    security_code_history_start: date
    repair_history_end: date
    issuer_regimes: tuple[IssuerRegime, ...]
    transitions: tuple[Transition, ...]
    reviewed_decisions: tuple[ReviewedDecision, ...]
    evidence: tuple[Mapping[str, Any], ...]

    @property
    def decisions_by_date(self) -> dict[date, ReviewedDecision]:
        return {item.trade_date: item for item in self.reviewed_decisions}


@dataclass(frozen=True)
class NormalizedQuote:
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    turnover: float | None
    source: str

    def values(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReconciliationResult:
    selected_rows: tuple[NormalizedQuote, ...]
    diagnostics: Mapping[str, Any]


def load_lineage_catalog(path: Path | str = CATALOG_PATH) -> dict[str, LineageEntry]:
    """Load and strictly validate the reviewed code-lineage catalog."""
    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LineageCatalogError(f"cannot load lineage catalog {catalog_path}: {exc}") from exc

    version = str(payload.get("catalog_version") or "").strip()
    if not version:
        raise LineageCatalogError("catalog_version is required")
    reviewed_at = _parse_date(payload.get("reviewed_at"), "reviewed_at")
    raw_instruments = payload.get("instruments")
    if not isinstance(raw_instruments, dict) or not raw_instruments:
        raise LineageCatalogError("instruments must be a non-empty object")

    entries: dict[str, LineageEntry] = {}
    for key, raw in raw_instruments.items():
        if not isinstance(raw, dict):
            raise LineageCatalogError(f"instruments.{key} must be an object")
        entry = _load_entry(version, reviewed_at, key, raw)
        if entry.instrument_id in entries:
            raise LineageCatalogError(f"duplicate instrument_id: {entry.instrument_id}")
        entries[entry.instrument_id] = entry
    return entries


def _load_entry(
    version: str,
    reviewed_at: date,
    key: str,
    raw: Mapping[str, Any],
) -> LineageEntry:
    instrument_id = str(raw.get("instrument_id") or "").strip().upper()
    if instrument_id != str(key).strip().upper() or "." not in instrument_id:
        raise LineageCatalogError(f"invalid instrument_id for catalog key {key!r}")
    symbol = str(raw.get("symbol") or "").strip()
    exchange = str(raw.get("exchange") or "").strip().upper()
    if not symbol or not exchange:
        raise LineageCatalogError(f"{instrument_id}: symbol and exchange are required")

    history_start = _parse_date(
        raw.get("security_code_history_start"),
        f"{instrument_id}.security_code_history_start",
    )
    repair_end = _parse_date(
        raw.get("repair_history_end"),
        f"{instrument_id}.repair_history_end",
    )
    if repair_end < history_start:
        raise LineageCatalogError(f"{instrument_id}: repair history ends before it starts")

    regimes = _load_regimes(instrument_id, raw.get("issuer_regimes"), history_start)
    transitions = _load_transitions(instrument_id, raw.get("transitions"), regimes)
    decisions = _load_decisions(
        instrument_id,
        raw.get("reviewed_decisions"),
        history_start,
        repair_end,
    )
    evidence = raw.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        raise LineageCatalogError(f"{instrument_id}: evidence must be non-empty")
    for index, item in enumerate(evidence):
        if not isinstance(item, dict) or not all(
            str(item.get(field) or "").strip()
            for field in ("source", "observed_at", "finding")
        ):
            raise LineageCatalogError(f"{instrument_id}: incomplete evidence at index {index}")
        _parse_date(item["observed_at"], f"{instrument_id}.evidence[{index}].observed_at")

    return LineageEntry(
        catalog_version=version,
        reviewed_at=reviewed_at,
        instrument_id=instrument_id,
        symbol=symbol,
        exchange=exchange,
        security_code_history_start=history_start,
        repair_history_end=repair_end,
        issuer_regimes=regimes,
        transitions=transitions,
        reviewed_decisions=decisions,
        evidence=tuple(dict(item) for item in evidence),
    )


def _load_regimes(
    instrument_id: str,
    raw_regimes: Any,
    history_start: date,
) -> tuple[IssuerRegime, ...]:
    if not isinstance(raw_regimes, list) or not raw_regimes:
        raise LineageCatalogError(f"{instrument_id}: issuer_regimes must be non-empty")
    regimes: list[IssuerRegime] = []
    for index, raw in enumerate(raw_regimes):
        if not isinstance(raw, dict):
            raise LineageCatalogError(f"{instrument_id}: invalid regime at index {index}")
        start = _parse_date(raw.get("start_date"), f"{instrument_id}.regime.start_date")
        end = (
            _parse_date(raw["end_date"], f"{instrument_id}.regime.end_date")
            if raw.get("end_date")
            else None
        )
        if end is not None and end < start:
            raise LineageCatalogError(f"{instrument_id}: regime ends before it starts")
        if regimes:
            previous = regimes[-1]
            if previous.end_date is None or start <= previous.end_date:
                raise LineageCatalogError(f"{instrument_id}: issuer regimes overlap or are unordered")
        regimes.append(
            IssuerRegime(
                issuer_name=str(raw.get("issuer_name") or "").strip(),
                start_date=start,
                end_date=end,
                role=str(raw.get("role") or "").strip(),
            )
        )
        if not regimes[-1].issuer_name or not regimes[-1].role:
            raise LineageCatalogError(f"{instrument_id}: incomplete issuer regime")
    if regimes[0].start_date != history_start:
        raise LineageCatalogError(f"{instrument_id}: first regime must start with code history")
    return tuple(regimes)


def _load_transitions(
    instrument_id: str,
    raw_transitions: Any,
    regimes: Sequence[IssuerRegime],
) -> tuple[Transition, ...]:
    expected_count = max(0, len(regimes) - 1)
    if not isinstance(raw_transitions, list) or len(raw_transitions) != expected_count:
        raise LineageCatalogError(
            f"{instrument_id}: transitions must cover every consecutive regime boundary"
        )
    regime_starts = {item.start_date for item in regimes}
    transitions: list[Transition] = []
    for index, raw in enumerate(raw_transitions):
        if not isinstance(raw, dict):
            raise LineageCatalogError(f"{instrument_id}: invalid transition at index {index}")
        effective = _parse_date(raw.get("effective_date"), f"{instrument_id}.transition.date")
        from_start = _parse_date(
            raw.get("from_regime_start"),
            f"{instrument_id}.transition.from_regime_start",
        )
        to_start = _parse_date(
            raw.get("to_regime_start"),
            f"{instrument_id}.transition.to_regime_start",
        )
        continuity = str(raw.get("price_continuity") or "").strip()
        factor_policy = str(raw.get("adjustment_factor_policy") or "").strip()
        if from_start not in regime_starts or to_start not in regime_starts:
            raise LineageCatalogError(f"{instrument_id}: transition references an unknown regime")
        if (
            from_start != regimes[index].start_date
            or to_start != regimes[index + 1].start_date
        ):
            raise LineageCatalogError(
                f"{instrument_id}: transition must connect consecutive regimes in order"
            )
        if effective != to_start:
            raise LineageCatalogError(f"{instrument_id}: transition must start the target regime")
        if continuity not in ALLOWED_CONTINUITY_POLICIES:
            raise LineageCatalogError(f"{instrument_id}: unsupported continuity policy")
        if factor_policy not in ALLOWED_ADJUSTMENT_POLICIES:
            raise LineageCatalogError(f"{instrument_id}: unsupported adjustment policy")
        transitions.append(
            Transition(
                effective_date=effective,
                event_type=str(raw.get("event_type") or "").strip(),
                from_regime_start=from_start,
                to_regime_start=to_start,
                price_continuity=continuity,
                adjustment_factor_policy=factor_policy,
            )
        )
        if not transitions[-1].event_type:
            raise LineageCatalogError(f"{instrument_id}: transition event_type is required")
    return tuple(transitions)


def _load_decisions(
    instrument_id: str,
    raw_decisions: Any,
    history_start: date,
    repair_end: date,
) -> tuple[ReviewedDecision, ...]:
    if not isinstance(raw_decisions, list) or not raw_decisions:
        raise LineageCatalogError(f"{instrument_id}: reviewed_decisions must be non-empty")
    decisions: list[ReviewedDecision] = []
    seen_dates: set[date] = set()
    for index, raw in enumerate(raw_decisions):
        if not isinstance(raw, dict):
            raise LineageCatalogError(f"{instrument_id}: invalid decision at index {index}")
        trade_date = _parse_date(raw.get("trade_date"), f"{instrument_id}.decision.trade_date")
        if trade_date in seen_dates:
            raise LineageCatalogError(f"{instrument_id}: duplicate decision date {trade_date}")
        if trade_date < history_start or trade_date > repair_end:
            raise LineageCatalogError(f"{instrument_id}: decision date outside repair history")
        seen_dates.add(trade_date)
        selected_source = str(raw.get("selected_source") or "").strip()
        reason = str(raw.get("reason") or "").strip()
        if selected_source not in ALLOWED_REVIEW_SOURCES or not reason:
            raise LineageCatalogError(f"{instrument_id}: invalid reviewed decision")
        raw_expected = raw.get("expected")
        if not isinstance(raw_expected, dict) or not raw_expected:
            raise LineageCatalogError(f"{instrument_id}: decision expected values are required")
        expected: dict[str, float] = {}
        for field, value in raw_expected.items():
            if field not in {*OHLC_FIELDS, "volume", "amount", "turnover"}:
                raise LineageCatalogError(f"{instrument_id}: unsupported expected field {field}")
            try:
                expected[field] = _finite_float(value, field)
            except ValueError as exc:
                raise LineageCatalogError(f"{instrument_id}: {exc}") from exc
        decisions.append(
            ReviewedDecision(
                trade_date=trade_date,
                selected_source=selected_source,
                reason=reason,
                expected=expected,
            )
        )
    return tuple(sorted(decisions, key=lambda item: item.trade_date))


def normalize_quote_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str,
) -> list[NormalizedQuote]:
    """Normalize already raw-price rows from pytdx or AkShare/Tencent."""
    if source not in ALLOWED_REVIEW_SOURCES:
        raise ValueError(f"unsupported quote source: {source}")
    normalized: list[NormalizedQuote] = []
    seen_dates: set[date] = set()
    for index, raw in enumerate(rows):
        raw_date = raw.get("trade_date") or raw.get("date") or raw.get("time")
        if isinstance(raw_date, datetime):
            trade_date = raw_date.date()
        elif isinstance(raw_date, date):
            trade_date = raw_date
        else:
            try:
                trade_date = date.fromisoformat(str(raw_date)[:10])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{source} row {index} has invalid date: {raw_date!r}") from exc
        if trade_date in seen_dates:
            raise ValueError(f"{source} has duplicate trade date {trade_date}")
        seen_dates.add(trade_date)

        open_price = _finite_float(raw.get("open"), "open")
        high_price = _finite_float(raw.get("high"), "high")
        low_price = _finite_float(raw.get("low"), "low")
        close_price = _finite_float(raw.get("close"), "close")
        if min(open_price, high_price, low_price, close_price) <= 0:
            raise ValueError(f"{source} {trade_date}: OHLC must be positive")
        if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
            raise ValueError(f"{source} {trade_date}: inconsistent OHLC range")
        volume = int(_finite_float(raw.get("volume", 0), "volume"))
        amount = _finite_float(raw.get("amount", 0), "amount")
        turnover_raw = raw.get("turnover")
        turnover = (
            _finite_float(turnover_raw, "turnover")
            if turnover_raw not in (None, "")
            else None
        )
        if volume < 0 or amount < 0 or (turnover is not None and turnover < 0):
            raise ValueError(f"{source} {trade_date}: volume/amount/turnover cannot be negative")
        normalized.append(
            NormalizedQuote(
                trade_date=trade_date,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                amount=amount,
                turnover=turnover,
                source=source,
            )
        )
    return sorted(normalized, key=lambda item: item.trade_date)


def reconcile_reviewed_history(
    entry: LineageEntry,
    primary_rows: Sequence[NormalizedQuote],
    independent_rows: Sequence[NormalizedQuote],
    *,
    price_tolerance: float = 1e-6,
) -> ReconciliationResult:
    """Reconcile source histories and fail closed on every unreviewed difference."""
    primary = _rows_by_date(entry, primary_rows, "pytdx")
    independent = _rows_by_date(entry, independent_rows, "akshare_tx")
    decisions = entry.decisions_by_date
    selected: list[NormalizedQuote] = []
    conflicts: list[dict[str, Any]] = []
    source_only: list[dict[str, Any]] = []
    applied_decisions: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []

    for trade_date in sorted(set(primary) | set(independent)):
        pytdx_row = primary.get(trade_date)
        tx_row = independent.get(trade_date)
        decision = decisions.get(trade_date)
        different_fields: list[str] = []
        issue_type: str | None = None
        if pytdx_row and tx_row:
            different_fields = [
                field
                for field in OHLC_FIELDS
                if not math.isclose(
                    getattr(pytdx_row, field),
                    getattr(tx_row, field),
                    rel_tol=0.0,
                    abs_tol=price_tolerance,
                )
            ]
            if different_fields:
                issue_type = "ohlc_conflict"
                conflicts.append(
                    {
                        "trade_date": trade_date.isoformat(),
                        "fields": different_fields,
                        "pytdx": pytdx_row.values(),
                        "akshare_tx": tx_row.values(),
                    }
                )
        else:
            issue_type = "source_only"
            only_row = pytdx_row or tx_row
            source_only.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "source": only_row.source if only_row else None,
                }
            )

        if issue_type and decision is None:
            unresolved.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "issue_type": issue_type,
                    "fields": different_fields,
                }
            )
            continue

        if decision is not None:
            candidate = primary.get(trade_date) if decision.selected_source == "pytdx" else independent.get(trade_date)
            if candidate is None:
                unresolved.append(
                    {
                        "trade_date": trade_date.isoformat(),
                        "issue_type": "reviewed_source_missing",
                        "selected_source": decision.selected_source,
                    }
                )
                continue
            required_review_fields = set(different_fields)
            if issue_type == "source_only":
                required_review_fields.update(
                    {*OHLC_FIELDS, "volume", "amount"}
                )
                if candidate.turnover is not None:
                    required_review_fields.add("turnover")
            uncovered_fields = sorted(
                required_review_fields - set(decision.expected)
            )
            if uncovered_fields:
                unresolved.append(
                    {
                        "trade_date": trade_date.isoformat(),
                        "issue_type": "reviewed_fields_not_covered",
                        "selected_source": decision.selected_source,
                        "fields": uncovered_fields,
                    }
                )
                continue
            mismatches = _expected_mismatches(candidate, decision, price_tolerance)
            if mismatches:
                unresolved.append(
                    {
                        "trade_date": trade_date.isoformat(),
                        "issue_type": "reviewed_values_changed",
                        "selected_source": decision.selected_source,
                        "mismatches": mismatches,
                    }
                )
                continue
            selected.append(candidate)
            applied_decisions.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "selected_source": decision.selected_source,
                    "reason": decision.reason,
                    "issue_type": issue_type or "provider_values_now_agree",
                }
            )
        else:
            selected.append(pytdx_row)

    missing_review_dates = sorted(set(decisions) - (set(primary) | set(independent)))
    unresolved.extend(
        {
            "trade_date": trade_date.isoformat(),
            "issue_type": "reviewed_date_missing_from_both_sources",
        }
        for trade_date in missing_review_dates
    )
    diagnostics = {
        "primary_count": len(primary),
        "independent_count": len(independent),
        "selected_count": len(selected),
        "common_date_count": len(set(primary) & set(independent)),
        "source_conflicts": conflicts,
        "source_only_dates": source_only,
        "applied_decisions": applied_decisions,
        "unresolved": unresolved,
    }
    if unresolved:
        raise LineageReconciliationError(
            f"{entry.instrument_id}: {len(unresolved)} unresolved source differences",
            diagnostics,
        )
    return ReconciliationResult(
        selected_rows=tuple(sorted(selected, key=lambda item: item.trade_date)),
        diagnostics=diagnostics,
    )


def _rows_by_date(
    entry: LineageEntry,
    rows: Sequence[NormalizedQuote],
    required_source: str,
) -> dict[date, NormalizedQuote]:
    result: dict[date, NormalizedQuote] = {}
    for row in rows:
        if row.source != required_source:
            raise ValueError(f"expected {required_source} row, got {row.source}")
        if row.trade_date < entry.security_code_history_start or row.trade_date > entry.repair_history_end:
            continue
        if row.trade_date in result:
            raise ValueError(f"{required_source} duplicate date: {row.trade_date}")
        result[row.trade_date] = row
    return result


def _expected_mismatches(
    row: NormalizedQuote,
    decision: ReviewedDecision,
    tolerance: float,
) -> dict[str, dict[str, float]]:
    mismatches: dict[str, dict[str, float]] = {}
    for field, expected in decision.expected.items():
        actual = float(getattr(row, field))
        field_tolerance = 0.5 if field == "volume" else tolerance
        if not math.isclose(actual, float(expected), rel_tol=0.0, abs_tol=field_tolerance):
            mismatches[field] = {"expected": float(expected), "actual": actual}
    return mismatches


def build_lineage_audit(
    entry: LineageEntry,
    *,
    existing_dates: Iterable[date],
    reconciliation: ReconciliationResult,
    first_current_quotes: Mapping[date, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build distinct leading-gap, missing-row, conflict, and boundary diagnostics."""
    local_dates = sorted(set(existing_dates))
    earliest = local_dates[0] if local_dates else None
    leading_gap = None
    if earliest is None or earliest > entry.security_code_history_start:
        gap_end = (
            min(entry.repair_history_end, earliest - timedelta(days=1))
            if earliest
            else entry.repair_history_end
        )
        leading_gap = {
            "start": entry.security_code_history_start.isoformat(),
            "end": gap_end.isoformat(),
            "earliest_local_quote": earliest.isoformat() if earliest else None,
        }
    selected_dates = {row.trade_date for row in reconciliation.selected_rows}
    missing_dates = sorted(selected_dates - set(local_dates))
    current_quotes = first_current_quotes or {}
    transitions: list[dict[str, Any]] = []
    for item in entry.transitions:
        predecessor_rows = [
            row
            for row in reconciliation.selected_rows
            if row.trade_date < item.effective_date
        ]
        last_predecessor = predecessor_rows[-1] if predecessor_rows else None
        transitions.append(
            {
                "effective_date": item.effective_date.isoformat(),
                "event_type": item.event_type,
                "price_continuity": item.price_continuity,
                "adjustment_factor_policy": item.adjustment_factor_policy,
                "last_predecessor_quote": (
                    last_predecessor.values() if last_predecessor else None
                ),
                "first_current_issuer_quote": dict(
                    current_quotes.get(item.effective_date) or {}
                )
                or None,
            }
        )
    return {
        "instrument_id": entry.instrument_id,
        "catalog_version": entry.catalog_version,
        "leading_gap": leading_gap,
        "missing_traded_row_count": len(missing_dates),
        "missing_traded_date_start": missing_dates[0].isoformat() if missing_dates else None,
        "missing_traded_date_end": missing_dates[-1].isoformat() if missing_dates else None,
        "suspension_row_count": sum(row.volume == 0 for row in reconciliation.selected_rows),
        "source_conflicts": reconciliation.diagnostics["source_conflicts"],
        "source_only_dates": reconciliation.diagnostics["source_only_dates"],
        "transitions": transitions,
    }


def build_missing_only_quotes(
    entry: LineageEntry,
    selected_rows: Sequence[NormalizedQuote],
    existing_dates: Iterable[date],
    *,
    batch_id: str,
) -> list[dict[str, Any]]:
    """Create database payloads only for dates not already persisted."""
    existing = set(existing_dates)
    ordered = sorted(selected_rows, key=lambda item: item.trade_date)
    payloads: list[dict[str, Any]] = []
    previous_close: float | None = None
    for row in ordered:
        change = row.close - previous_close if previous_close is not None else 0.0
        pct_change = change / previous_close * 100 if previous_close else 0.0
        if row.trade_date not in existing:
            payloads.append(
                {
                    "time": datetime.combine(row.trade_date, datetime.min.time()),
                    "instrument_id": entry.instrument_id,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "amount": row.amount,
                    "turnover": row.turnover,
                    "pre_close": previous_close,
                    "change": round(change, 4),
                    "pct_change": round(pct_change, 4),
                    "tradestatus": 1 if row.volume > 0 else 0,
                    "factor": 1.0,
                    "adjustment_type": "none",
                    "is_complete": True,
                    "quality_score": 1.0,
                    "source": row.source,
                    "batch_id": batch_id,
                }
            )
        previous_close = row.close
    return payloads


def build_lineage_metadata_row(
    entry: LineageEntry,
    *,
    reconciliation: ReconciliationResult,
    inserted_rows: Sequence[Mapping[str, Any]],
    existing_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge applied lineage evidence into the existing master metadata payload."""
    row = dict(existing_payload or {})
    raw_metadata = row.get("metadata")
    metadata = dict(raw_metadata) if isinstance(raw_metadata, Mapping) else {}
    inserted_dates = sorted(
        value.date() if isinstance(value, datetime) else value
        for value in (item.get("time") for item in inserted_rows)
        if isinstance(value, (date, datetime))
    )
    metadata["a_share_code_lineage"] = {
        "catalog_version": entry.catalog_version,
        "reviewed_at": entry.reviewed_at.isoformat(),
        "security_code_history_start": entry.security_code_history_start.isoformat(),
        "repair_history_end": entry.repair_history_end.isoformat(),
        "issuer_regimes": [
            {
                "issuer_name": item.issuer_name,
                "start_date": item.start_date.isoformat(),
                "end_date": item.end_date.isoformat() if item.end_date else None,
                "role": item.role,
            }
            for item in entry.issuer_regimes
        ],
        "transitions": [
            {
                "effective_date": item.effective_date.isoformat(),
                "event_type": item.event_type,
                "price_continuity": item.price_continuity,
                "adjustment_factor_policy": item.adjustment_factor_policy,
            }
            for item in entry.transitions
        ],
        "evidence": [dict(item) for item in entry.evidence],
        "reviewed_decisions": [
            {
                "trade_date": item.trade_date.isoformat(),
                "selected_source": item.selected_source,
                "reason": item.reason,
                "expected": dict(item.expected),
            }
            for item in entry.reviewed_decisions
        ],
        "source_comparison": dict(reconciliation.diagnostics),
        "reviewed_coverage": {
            "count": len(reconciliation.selected_rows),
            "start": (
                reconciliation.selected_rows[0].trade_date.isoformat()
                if reconciliation.selected_rows
                else None
            ),
            "end": (
                reconciliation.selected_rows[-1].trade_date.isoformat()
                if reconciliation.selected_rows
                else None
            ),
        },
        "last_apply": {
            "inserted_count": len(inserted_rows),
            "inserted_start": inserted_dates[0].isoformat() if inserted_dates else None,
            "inserted_end": inserted_dates[-1].isoformat() if inserted_dates else None,
        },
    }
    row.update(
        {
            "instrument_id": entry.instrument_id,
            "exchange": entry.exchange,
            "product_type": row.get("product_type") or "stock",
            "research_scope": row.get("research_scope") or "include",
            "canonical_instrument_id": row.get("canonical_instrument_id")
            or entry.instrument_id,
            "is_canonical": True,
            "counter_currency": row.get("counter_currency") or "CNY",
            "official_lifecycle_source": row.get("official_lifecycle_source")
            or "sse_official",
            "parser_version": "a_share_code_lineage_v1",
            "metadata": metadata,
        }
    )
    return row
