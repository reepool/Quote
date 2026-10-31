"""Materialized point-in-time financial cross-section panel.

Owns the ``financial_pit_panel`` table in the financials database: build,
availability derivation, incremental refresh, and read queries. This module is
the single write owner for the panel; API routes forward here.

Requirement source:
- ``docs/development/financial_pit_panel_requirements.md`` (v1.2)
- ``openspec/changes/add-financial-pit-panel-cross-section/``

Availability semantics (weak PIT): a row is visible from ``available_at``
(announcement semantics, possibly pushed later by delayed-filer events or
substantive value changes). ``announcement_available_at`` keeps the pre-push
chain result for diagnostics and for a future lenient guard if measured
thresholds are ever exceeded (2026-10-31 measurement: 2.47% < 5%, 0.39 < 1.0
periods -> the lenient parameter is intentionally NOT implemented, per the
spec scenario "when thresholds are not exceeded the parameter SHALL NOT be
implemented").

Per-share derivations prefer reported disclosure facts: ``eps`` uses the
cumulative reported basic EPS from the numeric-fact long table, ``bps`` /
``ocfps`` fall back to the period-end share capital fact
(``share_capital_amount``) when ``financial_facts.shares_outstanding`` is
absent (it is currently unpopulated in the wide table).
"""

from __future__ import annotations

import bisect
import json
import logging
import sqlite3
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from utils.date_utils import get_shanghai_time


def _today_iso() -> str:
    return get_shanghai_time().date().isoformat()

logger = logging.getLogger(__name__)

FINANCIAL_PIT_PANEL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS financial_pit_panel (
    instrument_id TEXT NOT NULL,
    symbol TEXT,
    exchange TEXT,
    report_period TEXT NOT NULL,
    report_type TEXT,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    currency TEXT,
    available_at TEXT NOT NULL,
    announcement_available_at TEXT NOT NULL,
    effective_at TEXT,
    available_at_quality TEXT NOT NULL,
    attribution_quality TEXT NOT NULL,
    is_delisted INTEGER NOT NULL DEFAULT 0,
    revenue REAL,
    gross_profit REAL,
    operating_profit REAL,
    pre_tax_profit REAL,
    net_income REAL,
    operating_cf REAL,
    total_cf REAL,
    total_assets REAL,
    total_liabilities REAL,
    equity REAL,
    current_assets REAL,
    current_liabilities REAL,
    inventory REAL,
    receivables REAL,
    fixed_assets REAL,
    intangible_assets REAL,
    shares_outstanding REAL,
    gross_margin REAL,
    operating_margin REAL,
    net_margin REAL,
    roe REAL,
    roa REAL,
    current_ratio REAL,
    quick_ratio REAL,
    asset_liability_ratio REAL,
    revenue_per_share REAL,
    book_value_per_share REAL,
    eps REAL,
    bps REAL,
    ocfps REAL,
    rev_yoy REAL,
    np_yoy REAL,
    ttm_revenue REAL,
    ttm_net_income REAL,
    source_table_updated_at TEXT,
    last_value_change_at TEXT,
    revised_after_publish INTEGER NOT NULL DEFAULT 0,
    panel_version TEXT NOT NULL,
    built_at TEXT NOT NULL,
    PRIMARY KEY (instrument_id, report_period)
);

CREATE INDEX IF NOT EXISTS idx_financial_pit_panel_available_at
ON financial_pit_panel(available_at);

CREATE INDEX IF NOT EXISTS idx_financial_pit_panel_instrument_available
ON financial_pit_panel(instrument_id, available_at);

CREATE INDEX IF NOT EXISTS idx_financial_pit_panel_report_period
ON financial_pit_panel(report_period);
"""

_BASE_FACT_COLUMNS: Tuple[str, ...] = (
    "revenue",
    "gross_profit",
    "operating_profit",
    "pre_tax_profit",
    "net_income",
    "operating_cf",
    "total_cf",
    "total_assets",
    "total_liabilities",
    "equity",
    "current_assets",
    "current_liabilities",
    "inventory",
    "receivables",
    "fixed_assets",
    "intangible_assets",
    "shares_outstanding",
)

_RATIO_COLUMNS: Tuple[str, ...] = (
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "current_ratio",
    "quick_ratio",
    "asset_liability_ratio",
    "revenue_per_share",
    "book_value_per_share",
)

_DERIVED_COLUMNS: Tuple[str, ...] = (
    "eps",
    "bps",
    "ocfps",
    "rev_yoy",
    "np_yoy",
    "ttm_revenue",
    "ttm_net_income",
)

_DELAY_GRACE_DAYS_DEFAULT = 90

_BULK_FIRST_PERIOD = "2023-12-31"

_CHANGE_LOG_DATASETS = ("financial_facts", "financial_numeric_facts")

_HIDDEN_ROW_SENTINEL = "9999-12-31"

_LONG_TABLE_EPS_FACT = "profit_sheet.basic_eps"
_LONG_TABLE_SHARE_FACT = "share_capital_amount"

_SELECT_FACT_COLUMNS = """
    instrument_id, symbol, exchange, report_period, report_type,
    currency, publish_date, updated_at, lineage_json,
    revenue, gross_profit, operating_profit, pre_tax_profit,
    net_income, operating_cf, total_cf, total_assets,
    total_liabilities, equity, current_assets, current_liabilities,
    inventory, receivables, fixed_assets, intangible_assets,
    shares_outstanding
"""


# ---------------------------------------------------------------- helpers


def statutory_deadline(report_period: str) -> Optional[str]:
    """Statutory disclosure deadline (ISO date) for an ISO report period.

    Q1 -> 04-30, semi-annual -> 08-31, Q3 -> 10-31, annual -> 04-30 next year.
    Any announcement must happen no later than this date, so falling back to it
    never uses data earlier than reality (conservative direction).
    """
    text = str(report_period or "")[:10]
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    month_day = (parsed.month, parsed.day)
    if month_day == (3, 31):
        return f"{parsed.year:04d}-04-30"
    if month_day == (6, 30):
        return f"{parsed.year:04d}-08-31"
    if month_day == (9, 30):
        return f"{parsed.year:04d}-10-31"
    if month_day == (12, 31):
        return f"{parsed.year + 1:04d}-04-30"
    return None


def prior_year_same_period(report_period: str) -> Optional[str]:
    """Same month-day one year earlier, as an ISO report period."""
    text = str(report_period or "")[:10]
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return None
    return f"{parsed.year - 1:04d}-{parsed.month:02d}-{parsed.day:02d}"


def _to_iso_date(value: Any) -> Optional[str]:
    """Normalize a date/datetime-ish value to an ISO date string."""
    if value is None:
        return None
    text = str(value).strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def _to_yyyymmdd(value: Optional[str]) -> Optional[str]:
    """ISO date (or datetime) text -> YYYYMMDD for the API layer."""
    if value is None:
        return None
    text = str(value)[:10].replace("-", "")
    return text if len(text) == 8 and text.isdigit() else None


def _from_yyyymmdd(value: Optional[str], *, field: str = "date") -> str:
    """YYYYMMDD (or ISO) query parameter -> ISO date text; raises on bad input."""
    if value is None:
        raise ValueError(f"{field} is required")
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYYMMDD or YYYY-MM-DD: {value!r}") from exc


def _shift_iso_date(iso_text: str, days: int) -> str:
    return (
        date.fromisoformat(iso_text[:10]) + timedelta(days=days)
    ).isoformat()


def _period_lag(later_period: str, earlier_period: str) -> Optional[int]:
    """Quarter distance between two ISO report periods; None when malformed."""
    try:
        later = date.fromisoformat(str(later_period)[:10])
        earlier = date.fromisoformat(str(earlier_period)[:10])
    except ValueError:
        return None
    return (later.year - earlier.year) * 4 + (later.month - earlier.month) // 3


# ------------------------------------------------------------------ store


class FinancialPitPanelStore:
    """Single write owner of the ``financial_pit_panel`` table."""

    def __init__(
        self,
        financials_db_path: str,
        quotes_db_path: Optional[str] = None,
        *,
        delay_grace_days: int = _DELAY_GRACE_DAYS_DEFAULT,
    ) -> None:
        self.db_path = str(financials_db_path)
        self.quotes_db_path = str(quotes_db_path) if quotes_db_path else None
        self.delay_grace_days = int(delay_grace_days)

    # ------------------------------------------------------------------ base

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=60)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=60000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(FINANCIAL_PIT_PANEL_SCHEMA_SQL)

    # ------------------------------------------------------- source reading

    def _load_indicator_rows(
        self, connection: sqlite3.Connection
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        ratio_columns = ", ".join(_RATIO_COLUMNS)
        rows = connection.execute(
            f"""
            SELECT instrument_id, report_period, {ratio_columns}
            FROM financial_indicator_snapshots
            """
        ).fetchall()
        return {
            (row["instrument_id"], str(row["report_period"])[:10]): dict(row)
            for row in rows
        }

    def _load_disclosure_events(
        self, connection: sqlite3.Connection
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT instrument_id, report_period, announcement_time,
                   classification, selection_reasons_json
            FROM financial_disclosure_event_state
            WHERE announcement_time IS NOT NULL AND announcement_time != ''
            """
        ).fetchall()
        events: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows:
            key = (row["instrument_id"], str(row["report_period"])[:10])
            reasons_text = str(row["selection_reasons_json"] or "")
            classification = str(row["classification"] or "")
            delayed = "delayed" in (classification + reasons_text).lower()
            candidate = {
                "announcement_date": _to_iso_date(row["announcement_time"]),
                "delayed": delayed,
            }
            existing = events.get(key)
            if (
                existing is None
                or (candidate["announcement_date"] or "") > (existing["announcement_date"] or "")
                or (candidate["delayed"] and not existing["delayed"])
            ):
                events[key] = candidate
        return events

    def _load_substantive_changes(
        self, connection: sqlite3.Connection
    ) -> Dict[Tuple[str, str], str]:
        placeholders = ",".join("?" for _ in _CHANGE_LOG_DATASETS)
        rows = connection.execute(
            f"""
            SELECT instrument_id, period, MAX(changed_at) AS last_changed_at
            FROM data_change_log
            WHERE domain = 'financial'
              AND dataset IN ({placeholders})
              AND old_hash IS NOT NULL
              AND old_hash != new_hash
              AND instrument_id IS NOT NULL AND instrument_id != ''
              AND period IS NOT NULL AND period != ''
            GROUP BY instrument_id, period
            """,
            _CHANGE_LOG_DATASETS,
        ).fetchall()
        result: Dict[Tuple[str, str], str] = {}
        for row in rows:
            changed_at = _to_iso_date(row["last_changed_at"])
            if changed_at:
                result[(row["instrument_id"], str(row["period"])[:10])] = changed_at
        return result

    def _load_share_facts(
        self, connection: sqlite3.Connection
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Reported per-share facts from the long table, keyed by (instrument, period)."""
        enrichment: Dict[Tuple[str, str], Dict[str, Any]] = {}
        instrument_ids = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT instrument_id FROM financial_facts"
            ).fetchall()
        ]
        for instrument_id in instrument_ids:
            rows = connection.execute(
                """
                SELECT report_period,
                       COALESCE(canonical_fact_name, fact_name) AS fact_name,
                       fact_value
                FROM financial_numeric_facts_hot
                WHERE instrument_id = ?
                  AND COALESCE(canonical_fact_name, fact_name) IN (?, ?)
                  AND fact_value IS NOT NULL
                ORDER BY updated_at DESC, rowid DESC
                """,
                (instrument_id, _LONG_TABLE_EPS_FACT, _LONG_TABLE_SHARE_FACT),
            ).fetchall()
            for row in rows:
                key = (instrument_id, str(row["report_period"])[:10])
                entry = enrichment.setdefault(key, {})
                value = float(row["fact_value"])
                if row["fact_name"] == _LONG_TABLE_EPS_FACT:
                    entry.setdefault("reported_eps", value)
                elif value > 0:
                    entry.setdefault("share_capital", value)
        return enrichment

    def _load_trading_days(self) -> Dict[str, List[str]]:
        """Trading-day lists per exchange for effective-date derivation."""
        trading: Dict[str, List[str]] = {}
        if not self.quotes_db_path:
            return trading
        try:
            with sqlite3.connect(
                f"file:{self.quotes_db_path}?mode=ro", uri=True, timeout=30
            ) as connection:
                rows = connection.execute(
                    """
                    SELECT exchange, date
                    FROM trading_calendar
                    WHERE is_trading_day = 1
                    ORDER BY exchange, date
                    """
                ).fetchall()
        except sqlite3.Error as exc:
            logger.warning(
                "financial_pit_panel: trading calendar unavailable (%s); "
                "effective_at will be null",
                exc,
            )
            return trading
        for exchange, calendar_date in rows:
            iso_day = _to_iso_date(calendar_date)
            if iso_day:
                trading.setdefault(str(exchange), []).append(iso_day)
        return trading

    @staticmethod
    def _next_trading_day(
        trading_days: Sequence[str], after_iso: str
    ) -> Optional[str]:
        """First trading day strictly after ``after_iso``; None beyond horizon."""
        index = bisect.bisect_right(trading_days, after_iso)
        if index >= len(trading_days):
            return None
        return trading_days[index]

    # ---------------------------------------------------------- derivation

    @staticmethod
    def _attribution_quality(lineage_json: Any, row: Dict[str, Any]) -> str:
        """parent / mixed / unverified from recorded lineage semantics.

        Rows without values inherit the parent contract default (nothing to
        flag); valued rows with missing semantics stay ``unverified`` rather
        than being nulled (dropping them would punch holes in the cross-section).
        """
        try:
            lineage = json.loads(lineage_json) if lineage_json else {}
        except (TypeError, json.JSONDecodeError):
            lineage = {}
        matches = lineage.get("core_fact_alias_matches") or {}
        net_income_semantic = (matches.get("net_income") or {}).get("alias_semantic")
        equity_semantic = (matches.get("equity") or {}).get("alias_semantic")
        has_values = row.get("net_income") is not None or row.get("equity") is not None
        if not has_values:
            return "parent"
        unmarked = False
        mixed = False
        if row.get("net_income") is not None:
            if net_income_semantic is None:
                unmarked = True
            elif net_income_semantic != "parent_attributable_net_profit":
                mixed = True
        if row.get("equity") is not None:
            if equity_semantic is None:
                unmarked = True
            elif equity_semantic != "parent_attributable_equity":
                mixed = True
        if mixed:
            return "mixed"
        if unmarked:
            return "unverified"
        return "parent"

    def derive_availability(
        self,
        *,
        publish_date: Any,
        report_period: str,
        disclosure_event: Optional[Dict[str, Any]],
    ) -> Tuple[str, str]:
        """Availability chain; returns (iso_date, quality).

        A delay-classified event's ``announcement_time`` is the date the delay
        was *announced*, not the date the financials were published, so such
        events never yield ``exact_event``; they yield
        ``delayed_estimate = max(statutory deadline, delay notice + grace)``.
        """
        exact = _to_iso_date(publish_date)
        if exact:
            return exact, "exact"
        deadline = statutory_deadline(report_period)
        if disclosure_event and disclosure_event.get("delayed"):
            event_date = disclosure_event.get("announcement_date")
            estimate = max(
                deadline or "", _shift_iso_date(event_date, self.delay_grace_days)
            ) if event_date else deadline
            if estimate:
                return estimate, "delayed_estimate"
        if disclosure_event and disclosure_event.get("announcement_date"):
            return disclosure_event["announcement_date"], "exact_event"
        if deadline:
            return deadline, "statutory_deadline"
        # Unknown period shape: hide the row rather than guess (conservative).
        return _HIDDEN_ROW_SENTINEL, "statutory_deadline"

    def build_row(
        self,
        row: Dict[str, Any],
        *,
        disclosure_event: Optional[Dict[str, Any]],
        last_change_at: Optional[str],
        panel_version: str,
        built_at: str,
    ) -> Dict[str, Any]:
        report_period = str(row["report_period"])[:10]
        announcement_available_at, quality = self.derive_availability(
            publish_date=row.get("publish_date"),
            report_period=report_period,
            disclosure_event=disclosure_event,
        )
        available_at = announcement_available_at
        revised = 0
        if (
            last_change_at
            and announcement_available_at != _HIDDEN_ROW_SENTINEL
            and last_change_at > announcement_available_at
        ):
            available_at = last_change_at
            revised = 1
        panel_row: Dict[str, Any] = {
            "instrument_id": row["instrument_id"],
            "symbol": row.get("symbol"),
            "exchange": row.get("exchange"),
            "report_period": report_period,
            "report_type": row.get("report_type"),
            "fiscal_year": int(report_period[:4]) if report_period[:4].isdigit() else None,
            "fiscal_quarter": {"03": 1, "06": 2, "09": 3, "12": 4}.get(report_period[5:7]),
            "currency": row.get("currency") or "CNY",
            "available_at": available_at,
            "announcement_available_at": announcement_available_at,
            "effective_at": None,
            "available_at_quality": quality,
            "attribution_quality": self._attribution_quality(
                row.get("lineage_json"), row
            ),
            "is_delisted": 0,
            "last_value_change_at": last_change_at if revised else None,
            "revised_after_publish": revised,
            "source_table_updated_at": _to_iso_date(row.get("updated_at")),
            "panel_version": panel_version,
            "built_at": built_at,
        }
        # Base facts and ratios carry straight through from the source rows;
        # derived columns are layered on by _apply_derivations afterwards.
        for column in (*_BASE_FACT_COLUMNS, *_RATIO_COLUMNS):
            panel_row[column] = row.get(column)
        return panel_row

    @staticmethod
    def _apply_delay_estimate(
        panel_row: Dict[str, Any],
        event: Optional[Dict[str, Any]],
        report_period: str,
        grace_days: int,
    ) -> None:
        """Kept for refresh-path symmetry; derivation lives in derive_availability."""
        return None

    @staticmethod
    def _apply_derivations(
        panel_row: Dict[str, Any],
        source_row: Dict[str, Any],
        sibling_periods: Dict[str, Dict[str, Any]],
        share_entry: Optional[Dict[str, Any]],
    ) -> None:
        """Compute eps/bps/ocfps and YoY/TTM in place (null outside boundaries)."""
        period = panel_row["report_period"]
        shares = source_row.get("shares_outstanding")
        if not shares and share_entry:
            shares = share_entry.get("share_capital")
        net_income = source_row.get("net_income")
        equity = source_row.get("equity")
        operating_cf = source_row.get("operating_cf")
        revenue = source_row.get("revenue")

        reported_eps = share_entry.get("reported_eps") if share_entry else None
        if reported_eps is not None:
            panel_row["eps"] = reported_eps
        elif shares and net_income is not None:
            panel_row["eps"] = net_income / shares
        else:
            panel_row["eps"] = None
        panel_row["bps"] = equity / shares if (shares and equity is not None) else None
        panel_row["ocfps"] = (
            operating_cf / shares if (shares and operating_cf is not None) else None
        )
        # Keep the resolved denominator visible for diagnostics; null stays null.
        panel_row["shares_outstanding"] = shares

        prior_key = prior_year_same_period(period)
        prior_row = sibling_periods.get(prior_key) if prior_key else None
        annual_key = f"{int(period[:4]) - 1:04d}-12-31"
        annual_row = sibling_periods.get(annual_key)

        if prior_row is not None and prior_row.get("revenue") not in (None, 0) and revenue is not None:
            panel_row["rev_yoy"] = (revenue / prior_row["revenue"] - 1.0) * 100.0
        else:
            panel_row["rev_yoy"] = None
        if prior_row is not None and prior_row.get("net_income") not in (None, 0) and net_income is not None:
            panel_row["np_yoy"] = (net_income / prior_row["net_income"] - 1.0) * 100.0
        else:
            panel_row["np_yoy"] = None

        panel_row["ttm_revenue"] = None
        panel_row["ttm_net_income"] = None
        if prior_row is not None and annual_row is not None:
            prior_cum_revenue = prior_row.get("revenue")
            prior_annual_revenue = annual_row.get("revenue")
            if None not in (revenue, prior_cum_revenue, prior_annual_revenue):
                panel_row["ttm_revenue"] = revenue + prior_annual_revenue - prior_cum_revenue
            prior_cum_ni = prior_row.get("net_income")
            prior_annual_ni = annual_row.get("net_income")
            if None not in (net_income, prior_cum_ni, prior_annual_ni):
                panel_row["ttm_net_income"] = net_income + prior_annual_ni - prior_cum_ni

    def _derive_all_rows(
        self,
        fact_rows: Sequence[sqlite3.Row],
        indicator_rows: Dict[Tuple[str, str], Dict[str, Any]],
        disclosure_events: Dict[Tuple[str, str], Dict[str, Any]],
        substantive_changes: Dict[Tuple[str, str], str],
        share_facts: Dict[Tuple[str, str], Dict[str, Any]],
        trading_days: Dict[str, List[str]],
        panel_version: str,
        built_at: str,
    ) -> List[Dict[str, Any]]:
        by_instrument: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in fact_rows:
            item = dict(row)
            period = str(item["report_period"])[:10]
            item.update(indicator_rows.get((item["instrument_id"], period), {}))
            by_instrument.setdefault(item["instrument_id"], {})[period] = item

        panel_rows: List[Dict[str, Any]] = []
        for instrument_id, periods in by_instrument.items():
            for period, row in periods.items():
                event = disclosure_events.get((instrument_id, period))
                last_change = substantive_changes.get((instrument_id, period))
                base = self.build_row(
                    row,
                    disclosure_event=event,
                    last_change_at=last_change,
                    panel_version=panel_version,
                    built_at=built_at,
                )
                self._apply_delay_estimate(base, event, period, self.delay_grace_days)
                exchange = str(base.get("exchange") or "SSE")
                base["effective_at"] = self._next_trading_day(
                    trading_days.get(exchange, trading_days.get("SSE", [])),
                    base["available_at"],
                )
                self._apply_derivations(
                    base, row, periods, share_facts.get((instrument_id, period))
                )
                panel_rows.append(base)
        return panel_rows

    # -------------------------------------------------------------- build

    def build_full(self, panel_version: str = "v1") -> Dict[str, Any]:
        """Rebuild the whole panel from source tables (idempotent per version)."""
        built_at = get_shanghai_time().isoformat()
        logger.info("financial_pit_panel: full build started (version=%s)", panel_version)
        with self.connect() as connection:
            indicator_rows = self._load_indicator_rows(connection)
            disclosure_events = self._load_disclosure_events(connection)
            logger.info(
                "financial_pit_panel: scanning data_change_log for substantive changes"
            )
            substantive_changes = self._load_substantive_changes(connection)
            logger.info(
                "financial_pit_panel: change index loaded (%d keys)",
                len(substantive_changes),
            )
            share_facts = self._load_share_facts(connection)
            fact_rows = connection.execute(
                f"SELECT {_SELECT_FACT_COLUMNS} FROM financial_facts"
            ).fetchall()
        trading_days = self._load_trading_days()
        panel_rows = self._derive_all_rows(
            fact_rows,
            indicator_rows,
            disclosure_events,
            substantive_changes,
            share_facts,
            trading_days,
            panel_version,
            built_at,
        )
        with self.connect() as connection:
            connection.execute("BEGIN")
            connection.execute("DELETE FROM financial_pit_panel")
            self._insert_rows(connection, panel_rows)
            connection.execute("COMMIT")
        stats = {
            "rows": len(panel_rows),
            "instruments": len({row["instrument_id"] for row in panel_rows}),
            "panel_version": panel_version,
            "built_at": built_at,
        }
        logger.info("financial_pit_panel: full build complete: %s", stats)
        return stats

    def refresh_pairs(
        self,
        pairs: Iterable[Tuple[str, str]],
        panel_version: str = "v1",
    ) -> Dict[str, Any]:
        """Recompute specific (instrument_id, report_period) rows in place."""
        built_at = get_shanghai_time().isoformat()
        normalized: List[Tuple[str, str]] = []
        seen = set()
        for instrument_id, report_period in pairs:
            period = str(report_period)[:10]
            key = (str(instrument_id), period)
            if len(period) == 10 and key not in seen:
                seen.add(key)
                normalized.append(key)
        if not normalized:
            return {"refreshed": 0}
        trading_days = self._load_trading_days()
        refreshed = 0
        with self.connect() as connection:
            for instrument_id, period in normalized:
                row = connection.execute(
                    f"""
                    SELECT {_SELECT_FACT_COLUMNS}
                    FROM financial_facts
                    WHERE instrument_id = ? AND report_period = ?
                    """,
                    (instrument_id, period),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        DELETE FROM financial_pit_panel
                        WHERE instrument_id = ? AND report_period = ?
                        """,
                        (instrument_id, period),
                    )
                    continue
                item = dict(row)
                indicator = connection.execute(
                    f"""
                    SELECT {', '.join(_RATIO_COLUMNS)}
                    FROM financial_indicator_snapshots
                    WHERE instrument_id = ? AND report_period = ?
                    """,
                    (instrument_id, period),
                ).fetchone()
                if indicator is not None:
                    item.update(dict(indicator))
                event = self._load_single_event(connection, instrument_id, period)
                last_change = self._load_single_change(connection, instrument_id, period)
                base = self.build_row(
                    item,
                    disclosure_event=event,
                    last_change_at=last_change,
                    panel_version=panel_version,
                    built_at=built_at,
                )
                self._apply_delay_estimate(base, event, period, self.delay_grace_days)
                exchange = str(base.get("exchange") or "SSE")
                base["effective_at"] = self._next_trading_day(
                    trading_days.get(exchange, trading_days.get("SSE", [])),
                    base["available_at"],
                )
                siblings = self._load_sibling_periods(connection, instrument_id)
                share_entry = self._load_single_share_facts(
                    connection, instrument_id, period
                )
                self._apply_derivations(base, item, siblings, share_entry)
                columns = list(base.keys())
                placeholders = ",".join("?" for _ in columns)
                column_list = ",".join(columns)
                updates = ",".join(
                    f"{name}=excluded.{name}"
                    for name in columns
                    if name not in ("instrument_id", "report_period")
                )
                connection.execute(
                    f"""
                    INSERT INTO financial_pit_panel ({column_list})
                    VALUES ({placeholders})
                    ON CONFLICT(instrument_id, report_period) DO UPDATE SET {updates}
                    """,
                    tuple(base.values()),
                )
                refreshed += 1
        return {"refreshed": refreshed, "built_at": built_at}

    def _load_sibling_periods(
        self, connection: sqlite3.Connection, instrument_id: str
    ) -> Dict[str, Dict[str, Any]]:
        siblings: Dict[str, Dict[str, Any]] = {}
        for row in connection.execute(
            f"SELECT {_SELECT_FACT_COLUMNS} FROM financial_facts WHERE instrument_id = ?",
            (instrument_id,),
        ).fetchall():
            key = str(row["report_period"])[:10]
            item = dict(row)
            indicator = connection.execute(
                f"""
                SELECT {', '.join(_RATIO_COLUMNS)}
                FROM financial_indicator_snapshots
                WHERE instrument_id = ? AND report_period = ?
                """,
                (instrument_id, key),
            ).fetchone()
            if indicator is not None:
                item.update(dict(indicator))
            siblings[key] = item
        return siblings

    @staticmethod
    def _load_single_event(
        connection: sqlite3.Connection, instrument_id: str, period: str
    ) -> Optional[Dict[str, Any]]:
        row = connection.execute(
            """
            SELECT announcement_time, classification, selection_reasons_json
            FROM financial_disclosure_event_state
            WHERE instrument_id = ? AND report_period = ?
              AND announcement_time IS NOT NULL AND announcement_time != ''
            ORDER BY announcement_time DESC LIMIT 1
            """,
            (instrument_id, period),
        ).fetchone()
        if row is None:
            return None
        reasons_text = str(row["selection_reasons_json"] or "")
        classification = str(row["classification"] or "")
        return {
            "announcement_date": _to_iso_date(row["announcement_time"]),
            "delayed": "delayed" in (classification + reasons_text).lower(),
        }

    @staticmethod
    def _load_single_change(
        connection: sqlite3.Connection, instrument_id: str, period: str
    ) -> Optional[str]:
        row = connection.execute(
            """
            SELECT MAX(changed_at) AS last_changed_at
            FROM data_change_log
            WHERE domain = 'financial'
              AND dataset IN ('financial_facts', 'financial_numeric_facts')
              AND instrument_id = ? AND period = ?
              AND old_hash IS NOT NULL AND old_hash != new_hash
            """,
            (instrument_id, period),
        ).fetchone()
        if row is None:
            return None
        return _to_iso_date(row["last_changed_at"])

    @staticmethod
    def _load_single_share_facts(
        connection: sqlite3.Connection, instrument_id: str, period: str
    ) -> Optional[Dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT COALESCE(canonical_fact_name, fact_name) AS fact_name, fact_value
            FROM financial_numeric_facts_hot
            WHERE instrument_id = ? AND report_period = ?
              AND COALESCE(canonical_fact_name, fact_name) IN (?, ?)
              AND fact_value IS NOT NULL
            ORDER BY updated_at DESC, rowid DESC
            """,
            (instrument_id, period, _LONG_TABLE_EPS_FACT, _LONG_TABLE_SHARE_FACT),
        ).fetchall()
        if not rows:
            return None
        entry: Dict[str, Any] = {}
        for row in rows:
            value = float(row["fact_value"])
            if row["fact_name"] == _LONG_TABLE_EPS_FACT:
                entry.setdefault("reported_eps", value)
            elif value > 0:
                entry.setdefault("share_capital", value)
        return entry

    @staticmethod
    def _insert_rows(
        connection: sqlite3.Connection, rows: Sequence[Dict[str, Any]]
    ) -> None:
        if not rows:
            return
        columns = list(rows[0].keys())
        placeholders = ",".join("?" for _ in columns)
        column_list = ",".join(columns)
        payload = [tuple(row.get(column) for column in columns) for row in rows]
        connection.executemany(
            f"INSERT INTO financial_pit_panel ({column_list}) VALUES ({placeholders})",
            payload,
        )

    # ------------------------------------------------------------ queries

    @staticmethod
    def _row_to_item(
        row: sqlite3.Row, fields: Optional[Sequence[str]] = None
    ) -> Dict[str, Any]:
        item = dict(row)
        for column in (
            "report_period",
            "available_at",
            "announcement_available_at",
            "effective_at",
            "last_value_change_at",
            "source_table_updated_at",
        ):
            item[column] = _to_yyyymmdd(item.get(column))
        item["is_delisted"] = bool(item.get("is_delisted"))
        item["revised_after_publish"] = bool(item.get("revised_after_publish"))
        if fields:
            allowed = set(fields) | {"instrument_id", "report_period"}
            item = {key: value for key, value in item.items() if key in allowed}
        return item

    def latest_complete_period(self) -> Optional[str]:
        """Newest bulk report period whose statutory deadline has passed."""
        today = _today_iso()
        with self.connect() as connection:
            periods = [
                row["report_period"]
                for row in connection.execute(
                    """
                    SELECT DISTINCT report_period FROM financial_pit_panel
                    WHERE report_period >= ?
                    ORDER BY report_period DESC
                    """,
                    (_BULK_FIRST_PERIOD,),
                ).fetchall()
            ]
        for period in periods:
            deadline = statutory_deadline(period)
            if deadline and deadline <= today:
                return period
        return None

    def _universe_note(self, latest_complete: Optional[str]) -> str:
        """Standard universe/limitation note shared by all panel endpoints.

        Includes the newest period's incomplete status while its disclosure
        window is still open (spec: universe limitation and completeness are
        disclosed).
        """
        note = (
            "universe=listed-only; delisted not covered; "
            f"latest_complete_period={_to_yyyymmdd(latest_complete) or 'none'}"
        )
        with self.connect() as connection:
            newest = connection.execute(
                "SELECT MAX(report_period) AS newest FROM financial_pit_panel"
            ).fetchone()["newest"]
        if newest:
            newest_deadline = statutory_deadline(newest)
            if newest_deadline and newest_deadline > _today_iso():
                note += (
                    f"; newest_period={_to_yyyymmdd(newest)} incomplete "
                    "(disclosure window open)"
                )
        return note

    def query_cross_section(
        self,
        as_of: str,
        *,
        report_period: Optional[str] = None,
        exchanges: Optional[Sequence[str]] = None,
        fields: Optional[Sequence[str]] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Latest visible report period per instrument at ``as_of`` (server-assembled).

        Strict revision guard only: rows whose substantive value changed after
        the original announcement stay invisible until the change date (see
        module docstring for why the lenient variant is not implemented).
        """
        as_of_iso = _from_yyyymmdd(as_of, field="as_of")
        visibility_column = "available_at"
        limit = max(1, min(int(limit), 2000))
        offset = max(0, int(offset))
        filter_params: List[Any] = [as_of_iso]
        clauses = [f"{visibility_column} <= ?"]
        clauses_prefixed = [f"p.{visibility_column} <= ?"]
        if report_period:
            clauses.append("report_period = ?")
            clauses_prefixed.append("p.report_period = ?")
            filter_params.append(_from_yyyymmdd(report_period, field="report_period"))
        if exchanges:
            if isinstance(exchanges, str):
                exchanges = exchanges.split(",")
            exchange_list = [
                str(item).strip().upper() for item in exchanges if str(item).strip()
            ]
            if exchange_list:
                placeholders = ",".join("?" for _ in exchange_list)
                clauses.append(f"exchange IN ({placeholders})")
                clauses_prefixed.append(f"p.exchange IN ({placeholders})")
                filter_params.extend(exchange_list)
        where = " AND ".join(clauses)
        where_prefixed = " AND ".join(clauses_prefixed)
        with self.connect() as connection:
            total = connection.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT instrument_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY instrument_id
                               ORDER BY report_period DESC, available_at DESC
                           ) AS rn
                    FROM financial_pit_panel
                    WHERE {where}
                ) WHERE rn = 1
                """,
                filter_params,
            ).fetchone()[0]
            rows = connection.execute(
                f"""
                SELECT * FROM (
                    SELECT p.*, ROW_NUMBER() OVER (
                        PARTITION BY p.instrument_id
                        ORDER BY p.report_period DESC, p.available_at DESC
                    ) AS rn
                    FROM financial_pit_panel p
                    WHERE {where_prefixed}
                ) WHERE rn = 1
                ORDER BY instrument_id
                LIMIT ? OFFSET ?
                """,
                [*filter_params, limit, offset],
            ).fetchall()
            # Expected period per instrument: latest period publicly announced
            # by as_of (announcement semantics, independent of revision push).
            expected_rows = connection.execute(
                """
                SELECT instrument_id, MAX(report_period) AS expected_period
                FROM financial_pit_panel
                WHERE announcement_available_at <= ?
                GROUP BY instrument_id
                """,
                (as_of_iso,),
            ).fetchall()
        expected_by_instrument = {
            row["instrument_id"]: row["expected_period"] for row in expected_rows
        }
        items = [self._row_to_item(row, fields) for row in rows]
        lag_distribution: Dict[str, int] = {}
        quality_counts: Dict[str, int] = {}
        attribution_counts: Dict[str, int] = {}
        for item in items:
            expected = expected_by_instrument.get(item["instrument_id"])
            actual = _to_iso_date(
                _from_yyyymmdd_safe(item.get("report_period")) or ""
            )
            if expected and actual:
                lag = str(_period_lag(expected, actual))
                lag_distribution[lag] = lag_distribution.get(lag, 0) + 1
            quality_key = item.get("available_at_quality") or "unknown"
            attribution_key = item.get("attribution_quality") or "unknown"
            quality_counts[quality_key] = quality_counts.get(quality_key, 0) + 1
            attribution_counts[attribution_key] = (
                attribution_counts.get(attribution_key, 0) + 1
            )
        latest_complete = self.latest_complete_period()
        return {
            "as_of": _to_yyyymmdd(as_of_iso),
            "report_period": _to_yyyymmdd(report_period) if report_period else None,
            "guard": "strict",
            "total": total,
            "limit": limit,
            "offset": offset,
            "universe_note": self._universe_note(latest_complete),
            "stats": {
                "period_lag_distribution": dict(
                    sorted(lag_distribution.items(), key=lambda kv: kv[0])
                ),
                "available_at_quality": quality_counts,
                "attribution_quality": attribution_counts,
            },
            "items": items,
        }

    def query_panel(
        self,
        *,
        report_period: Optional[str] = None,
        instrument_id: Optional[str] = None,
        available_at_lte: Optional[str] = None,
        limit: int = 2000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Raw panel rows for offline caching and client-side assembly."""
        limit = max(1, min(int(limit), 2000))
        offset = max(0, int(offset))
        clauses: List[str] = ["1 = 1"]
        params: List[Any] = []
        if report_period:
            clauses.append("report_period = ?")
            params.append(_from_yyyymmdd(report_period, field="report_period"))
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if available_at_lte:
            clauses.append("available_at <= ?")
            params.append(_from_yyyymmdd(available_at_lte, field="available_at_lte"))
        where = " AND ".join(clauses)
        with self.connect() as connection:
            total = connection.execute(
                f"SELECT COUNT(*) FROM financial_pit_panel WHERE {where}", params
            ).fetchone()[0]
            rows = connection.execute(
                f"""
                SELECT * FROM financial_pit_panel WHERE {where}
                ORDER BY report_period DESC, instrument_id
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "universe_note": self._universe_note(self.latest_complete_period()),
            "items": [self._row_to_item(row) for row in rows],
        }

    def coverage(self, report_period: Optional[str] = None) -> Dict[str, Any]:
        """Per-period actual/expected/gap with quality and completeness markers."""
        with self.connect() as connection:
            if report_period is None:
                periods = [
                    row["report_period"]
                    for row in connection.execute(
                        "SELECT DISTINCT report_period FROM financial_pit_panel "
                        "ORDER BY report_period"
                    ).fetchall()
                ]
            else:
                periods = [_from_yyyymmdd(report_period, field="report_period")]
            summary = [
                self._coverage_for_period(connection, period) for period in periods
            ]
            panel_instruments = {
                period: {
                    row["instrument_id"]
                    for row in connection.execute(
                        "SELECT instrument_id FROM financial_pit_panel "
                        "WHERE report_period = ?",
                        (period,),
                    ).fetchall()
                }
                for period in periods
            }
        lifecycle_sets = self._load_lifecycle_sets(periods)
        for item in summary:
            period = item["report_period"]
            lifecycle_set = lifecycle_sets.get(period)
            if item["is_bulk_period"] and lifecycle_set is not None:
                expected = len(lifecycle_set)
                item["expected_count"] = expected
                # Set-based gap: instruments expected but absent from the panel.
                # A count difference would be cancelled out by reverse rows
                # (pre-IPO prospectus financials) and hide true missing names
                # (real-DB example: 2024Q1 true gap 80 hidden by 161 extra rows).
                item["gap_count"] = len(
                    lifecycle_set - panel_instruments.get(period, set())
                )
                # Reverse discrepancy: panel rows outside the lifecycle universe
                # (e.g., pre-IPO prospectus financials; task 1.3 attribution).
                item["reverse_discrepancy_count"] = len(
                    panel_instruments.get(period, set()) - lifecycle_set
                )
            else:
                item["sparse"] = True
                item["expected_count"] = None
                item["gap_count"] = None
                item["reverse_discrepancy_count"] = None
        today = _today_iso()
        latest_complete = None
        for item in sorted(
            (x for x in summary if x["is_bulk_period"]),
            key=lambda x: x["report_period"],
            reverse=True,
        ):
            deadline = statutory_deadline(item["report_period"])
            if deadline and deadline <= today:
                latest_complete = item["report_period"]
                break
        return {
            "latest_complete_period": _to_yyyymmdd(latest_complete),
            "universe_note": self._universe_note(latest_complete),
            "expected_count_derivation": (
                "stock instruments on SSE/SZSE/BSE with listed_date <= period end "
                "and (delisted_date null or > period end), from instruments master; "
                "bulk periods only (2023-12-31 onward), sparse periods report "
                "actual counts with a sparse marker; reverse_discrepancy_count "
                "covers panel rows outside the lifecycle universe (pre-IPO rows)"
            ),
            "periods": [
                {**item, "report_period": _to_yyyymmdd(item["report_period"])}
                for item in summary
            ],
        }

    def _load_lifecycle_sets(
        self, periods: Sequence[str]
    ) -> Dict[str, set]:
        """Expected listed-stock instrument set per report period end.

        Derived from the instruments master lifecycle (listed on or before the
        period end, not delisted as of the period end). Sets (not counts) so
        coverage can also expose the reverse discrepancy.
        """
        sets: Dict[str, set] = {}
        if not self.quotes_db_path or not periods:
            return sets
        try:
            with sqlite3.connect(
                f"file:{self.quotes_db_path}?mode=ro", uri=True, timeout=30
            ) as connection:
                for period in periods:
                    if not statutory_deadline(period):
                        continue
                    sets[period] = {
                        row[0]
                        for row in connection.execute(
                            """
                            SELECT instrument_id FROM instruments
                            WHERE type = 'stock'
                              AND exchange IN ('SSE', 'SZSE', 'BSE')
                              AND listed_date IS NOT NULL AND listed_date != ''
                              AND substr(listed_date, 1, 10) <= ?
                              AND (delisted_date IS NULL OR delisted_date = ''
                                   OR substr(delisted_date, 1, 10) > ?)
                            """,
                            (period, period),
                        ).fetchall()
                    }
        except sqlite3.Error as exc:
            logger.warning(
                "financial_pit_panel: lifecycle universe unavailable (%s)", exc
            )
        return sets

    def _load_lifecycle_universe(self, periods: Sequence[str]) -> Dict[str, int]:
        """Expected listed-stock count per report period end (instruments master)."""
        counts: Dict[str, int] = {}
        if not self.quotes_db_path or not periods:
            return counts
        try:
            with sqlite3.connect(
                f"file:{self.quotes_db_path}?mode=ro", uri=True, timeout=30
            ) as connection:
                for period in periods:
                    if not statutory_deadline(period):
                        continue
                    counts[period] = connection.execute(
                        """
                        SELECT COUNT(*) FROM instruments
                        WHERE type = 'stock'
                          AND exchange IN ('SSE', 'SZSE', 'BSE')
                          AND listed_date IS NOT NULL AND listed_date != ''
                          AND substr(listed_date, 1, 10) <= ?
                          AND (delisted_date IS NULL OR delisted_date = ''
                               OR substr(delisted_date, 1, 10) > ?)
                        """,
                        (period, period),
                    ).fetchone()[0]
        except sqlite3.Error as exc:
            logger.warning(
                "financial_pit_panel: lifecycle universe unavailable (%s)", exc
            )
        return counts

    def _coverage_for_period(
        self, connection: sqlite3.Connection, period: str
    ) -> Dict[str, Any]:
        total = connection.execute(
            "SELECT COUNT(*) FROM financial_pit_panel WHERE report_period = ?",
            (period,),
        ).fetchone()[0]
        quality_rows = connection.execute(
            """
            SELECT available_at_quality, COUNT(*) AS n
            FROM financial_pit_panel WHERE report_period = ?
            GROUP BY available_at_quality
            """,
            (period,),
        ).fetchall()
        attribution_rows = connection.execute(
            """
            SELECT attribution_quality, COUNT(*) AS n
            FROM financial_pit_panel WHERE report_period = ?
            GROUP BY attribution_quality
            """,
            (period,),
        ).fetchall()
        revised = connection.execute(
            """
            SELECT COUNT(*) FROM financial_pit_panel
            WHERE report_period = ? AND revised_after_publish = 1
            """,
            (period,),
        ).fetchone()[0]
        derived_non_null = {
            column: connection.execute(
                f"""
                SELECT SUM(CASE WHEN {column} IS NOT NULL THEN 1 ELSE 0 END)
                FROM financial_pit_panel WHERE report_period = ?
                """,
                (period,),
            ).fetchone()[0]
            for column in ("eps", "bps", "ocfps", "rev_yoy", "np_yoy", "ttm_revenue")
        }
        is_bulk = period >= _BULK_FIRST_PERIOD
        today = _today_iso()
        deadline = statutory_deadline(period)
        recent_cutoff = _shift_iso_date(today, -7)
        recent_source_change = connection.execute(
            """
            SELECT COUNT(*) FROM financial_pit_panel
            WHERE report_period = ?
              AND source_table_updated_at IS NOT NULL
              AND source_table_updated_at >= ?
            """,
            (period, recent_cutoff),
        ).fetchone()[0]
        return {
            "report_period": period,
            "actual_count": total,
            "available_at_quality": {
                row["available_at_quality"]: row["n"] for row in quality_rows
            },
            "attribution_quality": {
                row["attribution_quality"]: row["n"] for row in attribution_rows
            },
            "revised_after_publish_count": revised,
            "recent_source_change_count": recent_source_change,
            "derived_non_null": derived_non_null,
            "is_bulk_period": is_bulk,
            "statutory_deadline": _to_yyyymmdd(deadline),
            "is_complete": (
                bool(deadline and deadline <= today) if is_bulk else None
            ),
        }

    def changes(
        self, *, cursor: Optional[int] = None, limit: int = 2000
    ) -> Dict[str, Any]:
        """Append-only changes feed keyed by the monotonic sequence_id cursor."""
        limit = max(1, min(int(limit), 5000))
        after = int(cursor) if cursor is not None else -1
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT sequence_id, dataset, change_type, instrument_id, period,
                       changed_at
                FROM data_change_log
                WHERE domain = 'financial'
                  AND dataset IN ({','.join('?' for _ in _CHANGE_LOG_DATASETS)})
                  AND sequence_id > ?
                  AND instrument_id IS NOT NULL AND instrument_id != ''
                  AND period IS NOT NULL AND period != ''
                ORDER BY sequence_id
                LIMIT ?
                """,
                [*_CHANGE_LOG_DATASETS, after, limit],
            ).fetchall()
            next_cursor = rows[-1]["sequence_id"] if rows else after
        entries = [
            {
                "sequence_id": row["sequence_id"],
                "instrument_id": row["instrument_id"],
                "report_period": _to_yyyymmdd(str(row["period"])[:10]),
                "change_type": row["change_type"],
                "dataset": row["dataset"],
                "changed_at": row["changed_at"],
            }
            for row in rows
        ]
        return {
            "cursor": after,
            "next_cursor": next_cursor,
            "limit": limit,
            "has_more": len(entries) == limit,
            "universe_note": self._universe_note(self.latest_complete_period()),
            "entries": entries,
        }

    def measure_revision_impact(self) -> Dict[str, Any]:
        """M2 measurement: substantive-change impact and average fallback depth.

        Fallback depth per revised row = how many statutory disclosure deadlines
        fall inside the pushed visibility window (announcement_available_at,
        available_at]; crossing k deadlines means an as-of inside that window
        falls back across k period boundaries. Drives the revision_guard
        threshold decision (acceptance #17 / task 2.5).
        """
        with self.connect() as connection:
            total = connection.execute(
                "SELECT COUNT(*) FROM financial_pit_panel"
            ).fetchone()[0]
            revised = connection.execute(
                "SELECT COUNT(*) FROM financial_pit_panel WHERE revised_after_publish = 1"
            ).fetchone()[0]
            rows = connection.execute(
                """
                SELECT report_period, announcement_available_at, available_at
                FROM financial_pit_panel WHERE revised_after_publish = 1
                """
            ).fetchall()
        fallback_lags: List[int] = []
        delay_days: List[int] = []
        for row in rows:
            start = row["announcement_available_at"][:10]
            end = row["available_at"][:10]
            if start >= end:
                continue
            delay_days.append(
                (
                    date.fromisoformat(end) - date.fromisoformat(start)
                ).days
            )
            year = int(start[:4])
            deadlines = [
                f"{year}-04-30",
                f"{year}-08-31",
                f"{year}-10-31",
                f"{year + 1}-04-30",
                f"{year + 1}-08-31",
                f"{year + 1}-10-31",
            ]
            crossed = sum(1 for d in deadlines if start < d <= end)
            fallback_lags.append(crossed)
        average_fallback = (
            sum(fallback_lags) / len(fallback_lags) if fallback_lags else 0.0
        )
        average_delay = sum(delay_days) / len(delay_days) if delay_days else 0.0
        revised_ratio = (revised / total) if total else 0.0
        return {
            "total_rows": total,
            "revised_rows": revised,
            "revised_ratio": revised_ratio,
            "average_fallback_periods": average_fallback,
            "average_delay_days": average_delay,
            "max_delay_days": max(delay_days) if delay_days else 0,
            "thresholds": {"revised_ratio": 0.05, "average_fallback_periods": 1.0},
            "guard_required": bool(revised_ratio > 0.05 or average_fallback > 1.0),
        }


def _from_yyyymmdd_safe(value: Any) -> Optional[str]:
    if not value:
        return None
    try:
        return _from_yyyymmdd(value)
    except ValueError:
        return None
