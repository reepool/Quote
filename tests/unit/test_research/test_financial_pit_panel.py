"""Financial PIT panel store tests: availability chain, as-of semantics, derivations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from research.financial_pit_panel import (
    FinancialPitPanelStore,
    statutory_deadline,
)

FINANCIALS_DDL = """
CREATE TABLE financial_facts (
    instrument_id TEXT, symbol TEXT, exchange TEXT, report_period TEXT,
    report_type TEXT, currency TEXT, publish_date TEXT, updated_at TEXT,
    lineage_json TEXT,
    revenue REAL, gross_profit REAL, operating_profit REAL, pre_tax_profit REAL,
    net_income REAL, operating_cf REAL, total_cf REAL, total_assets REAL,
    total_liabilities REAL, equity REAL, current_assets REAL,
    current_liabilities REAL, inventory REAL, receivables REAL,
    fixed_assets REAL, intangible_assets REAL, shares_outstanding REAL,
    PRIMARY KEY (instrument_id, report_period)
);
CREATE TABLE financial_indicator_snapshots (
    instrument_id TEXT, report_period TEXT,
    gross_margin REAL, operating_margin REAL, net_margin REAL, roe REAL,
    roa REAL, current_ratio REAL, quick_ratio REAL, asset_liability_ratio REAL,
    revenue_per_share REAL, book_value_per_share REAL
);
CREATE TABLE financial_disclosure_event_state (
    instrument_id TEXT, report_period TEXT, announcement_time TEXT,
    classification TEXT, selection_reasons_json TEXT
);
CREATE TABLE financial_numeric_facts_hot (
    instrument_id TEXT, report_period TEXT, canonical_fact_name TEXT,
    fact_name TEXT, fact_value REAL, updated_at TEXT
);
CREATE TABLE data_change_log (
    sequence_id INTEGER PRIMARY KEY, domain TEXT, dataset TEXT,
    change_type TEXT, business_key_json TEXT, instrument_id TEXT,
    series_id TEXT, observation_date TEXT, period TEXT,
    old_hash TEXT, new_hash TEXT, row_version INTEGER, source TEXT,
    source_mode TEXT, source_profile TEXT, ingestion_run_id TEXT,
    batch_id TEXT, changed_at TEXT
);
"""

QUOTES_DDL = """
CREATE TABLE instruments (
    instrument_id TEXT PRIMARY KEY, type TEXT, exchange TEXT,
    listed_date TEXT, delisted_date TEXT
);
CREATE TABLE trading_calendar (
    exchange TEXT, date TEXT, is_trading_day INTEGER
);
"""

LINEAGE_PARENT = (
    '{"core_fact_alias_matches": {'
    '"net_income": {"alias_semantic": "parent_attributable_net_profit"},'
    '"equity": {"alias_semantic": "parent_attributable_equity"}}}'
)
LINEAGE_UNMARKED = '{"core_fact_alias_matches": {}}'
LINEAGE_MIXED = (
    '{"core_fact_alias_matches": {'
    '"net_income": {"alias_semantic": "consolidated_net_profit"},'
    '"equity": {"alias_semantic": "parent_attributable_equity"}}}'
)


def _make_dbs(tmp_path: Path) -> tuple[Path, Path]:
    financials = tmp_path / "financials.db"
    quotes = tmp_path / "quotes.db"
    with sqlite3.connect(financials) as conn:
        conn.executescript(FINANCIALS_DDL)
    with sqlite3.connect(quotes) as conn:
        conn.executescript(QUOTES_DDL)
    return financials, quotes


def _insert_fact(
    conn: sqlite3.Connection,
    instrument_id: str,
    report_period: str,
    *,
    net_income: float | None = 100.0,
    revenue: float | None = 1000.0,
    equity: float | None = 500.0,
    operating_cf: float | None = 80.0,
    publish_date: str | None = None,
    lineage: str = LINEAGE_PARENT,
    exchange: str = "SSE",
    updated_at: str = "2026-01-01T00:00:00+08:00",
) -> None:
    conn.execute(
        """
        INSERT INTO financial_facts (
            instrument_id, symbol, exchange, report_period, report_type,
            currency, publish_date, updated_at, lineage_json,
            revenue, net_income, equity, operating_cf
        ) VALUES (?, ?, ?, ?, 'quarterly', 'CNY', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            instrument_id,
            instrument_id.split(".")[0],
            exchange,
            report_period,
            publish_date,
            updated_at,
            lineage,
            revenue,
            net_income,
            equity,
            operating_cf,
        ),
    )


def _add_change(
    conn: sqlite3.Connection,
    sequence_id: int,
    instrument_id: str,
    period: str,
    changed_at: str,
    *,
    substantive: bool = True,
    dataset: str = "financial_facts",
) -> None:
    old_hash = "aaa" if substantive else "aaa"
    new_hash = "bbb" if substantive else "aaa"
    conn.execute(
        """
        INSERT INTO data_change_log (
            sequence_id, domain, dataset, change_type, instrument_id, period,
            old_hash, new_hash, changed_at
        ) VALUES (?, 'financial', ?, 'update', ?, ?, ?, ?, ?)
        """,
        (sequence_id, dataset, instrument_id, period, old_hash, new_hash, changed_at),
    )


@pytest.fixture()
def store(tmp_path: Path) -> FinancialPitPanelStore:
    financials, quotes = _make_dbs(tmp_path)
    with sqlite3.connect(financials) as conn:
        # exact publish date
        _insert_fact(
            conn,
            "600001.SH",
            "2024-12-31",
            net_income=100.0,
            publish_date="2025-03-10",
        )
        _insert_fact(
            conn,
            "600001.SH",
            "2025-03-31",
            net_income=110.0,
            publish_date="2025-04-20",
        )
        # no publish date -> statutory deadline fallback (2024-06-30 -> 2025-08-31)
        _insert_fact(
            conn,
            "600002.SZ",
            "2024-06-30",
            net_income=200.0,
            publish_date=None,
            exchange="SZSE",
        )
        # unmarked lineage with values -> unverified
        _insert_fact(
            conn,
            "600003.SH",
            "2024-12-31",
            net_income=300.0,
            publish_date="2025-04-01",
            lineage=LINEAGE_UNMARKED,
        )
        # consolidated net_income semantic -> mixed
        _insert_fact(
            conn,
            "600004.SH",
            "2024-12-31",
            net_income=400.0,
            publish_date="2025-04-02",
            lineage=LINEAGE_MIXED,
        )
        # no values -> parent default
        _insert_fact(
            conn,
            "600005.SH",
            "2024-12-31",
            net_income=None,
            revenue=None,
            equity=None,
            operating_cf=None,
            publish_date="2025-04-03",
        )
        # sparse period before bulk window
        _insert_fact(
            conn,
            "600001.SH",
            "2022-12-31",
            net_income=90.0,
            publish_date="2023-04-28",
        )
        # revision: substantive change after announcement pushes availability
        _insert_fact(
            conn,
            "600001.SH",
            "2024-09-30",
            net_income=95.0,
            publish_date="2024-10-30",
        )
        _add_change(conn, 1, "600001.SH", "2024-09-30", "2024-11-20")
        # non-substantive update must NOT push
        _insert_fact(
            conn,
            "600001.SH",
            "2024-06-30",
            net_income=93.0,
            publish_date="2024-08-30",
        )
        _add_change(conn, 2, "600001.SH", "2024-06-30", "2024-09-15", substantive=False)
        # delayed filer: no publish date, delay event recorded
        _insert_fact(
            conn,
            "600006.SH",
            "2024-12-31",
            net_income=600.0,
            publish_date=None,
        )
        conn.execute(
            """
            INSERT INTO financial_disclosure_event_state (
                instrument_id, report_period, announcement_time,
                classification, selection_reasons_json
            ) VALUES ('600006.SH', '2024-12-31', '2026-05-10T16:00:00+00:00',
                      'periodic_report_delayed_or_suspended', '["periodic_report_delayed"]')
            """
        )
        # reported EPS + share capital facts in the long table
        conn.executemany(
            """
            INSERT INTO financial_numeric_facts_hot (
                instrument_id, report_period, canonical_fact_name, fact_name, fact_value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("600001.SH", "2024-12-31", "profit_sheet.basic_eps", "x", 1.25),
                ("600001.SH", "2024-12-31", "share_capital_amount", "x", 80.0),
                ("600001.SH", "2023-12-31", "profit_sheet.basic_eps", "x", 1.10),
            ],
        )
        # indicator rows store fractions; panel contract is percent
        conn.execute(
            """
            INSERT INTO financial_indicator_snapshots (
                instrument_id, report_period, gross_margin, roe, current_ratio
            ) VALUES ('600001.SH', '2024-12-31', 0.914, 0.0267, 1.9)
            """
        )
        # YoY/TTM history for 600001: 2023 annual + 2024 periods
        _insert_fact(
            conn,
            "600001.SH",
            "2023-12-31",
            net_income=90.0,
            revenue=900.0,
            publish_date="2024-04-26",
        )
        _insert_fact(
            conn,
            "600001.SH",
            "2024-03-31",
            net_income=20.0,
            revenue=210.0,
            publish_date="2024-04-26",
        )
        conn.commit()

    with sqlite3.connect(quotes) as conn:
        conn.executemany(
            """
            INSERT INTO instruments (instrument_id, type, exchange, listed_date, delisted_date)
            VALUES (?, 'stock', ?, ?, ?)
            """,
            [
                ("600001.SH", "SSE", "2000-01-01", None),
                ("600002.SZ", "SZSE", "2001-01-01", None),
                ("600003.SH", "SSE", "2002-01-01", None),
                ("600004.SH", "SSE", "2002-01-01", None),
                ("600005.SH", "SSE", "2002-01-01", None),
                ("600006.SH", "SSE", "2002-01-01", None),
                ("600100.SH", "SSE", "2025-06-01", None),
            ],
        )
        # Trading days around the announcement dates; horizon ends 2026-12-31.
        conn.executemany(
            "INSERT INTO trading_calendar (exchange, date, is_trading_day) VALUES (?, ?, 1)",
            [
                ("SSE", "2025-03-10"),
                ("SSE", "2025-03-11"),
                ("SSE", "2025-04-21"),
                ("SSE", "2025-04-22"),
                ("SSE", "2026-12-31"),
            ],
        )
        conn.commit()
    store = FinancialPitPanelStore(str(financials), str(quotes))
    store.initialize()
    return store


def test_statutory_deadline_mapping() -> None:
    assert statutory_deadline("2024-03-31") == "2024-04-30"
    assert statutory_deadline("2024-06-30") == "2024-08-31"
    assert statutory_deadline("2024-09-30") == "2024-10-31"
    assert statutory_deadline("2024-12-31") == "2025-04-30"
    assert statutory_deadline("bad") is None


def test_full_build_and_idempotency(store: FinancialPitPanelStore) -> None:
    stats = store.build_full("test-v1")
    assert stats["rows"] == 12
    with store.connect() as conn:
        first = conn.execute(
            "SELECT COUNT(*), SUM(revenue) FROM financial_pit_panel"
        ).fetchone()
    store.build_full("test-v1")
    with store.connect() as conn:
        second = conn.execute(
            "SELECT COUNT(*), SUM(revenue) FROM financial_pit_panel"
        ).fetchone()
    assert first == second


def test_availability_chain_qualities(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    with store.connect() as conn:
        rows = {
            (r["instrument_id"], r["report_period"]): r
            for r in conn.execute(
                "SELECT instrument_id, report_period, available_at, "
                "announcement_available_at, available_at_quality, "
                "revised_after_publish, last_value_change_at FROM financial_pit_panel"
            )
        }
    # exact publish date
    row = rows[("600001.SH", "2024-12-31")]
    assert row["available_at"] == "2025-03-10"
    assert row["available_at_quality"] == "exact"
    # statutory fallback never precedes the deadline
    row = rows[("600002.SZ", "2024-06-30")]
    assert row["available_at"] == "2024-08-31"
    assert row["available_at_quality"] == "statutory_deadline"
    # substantive change after announcement pushes availability
    row = rows[("600001.SH", "2024-09-30")]
    assert row["available_at"] == "2024-11-20"
    assert row["revised_after_publish"] == 1
    assert row["last_value_change_at"] == "2024-11-20"
    # non-substantive update does not push
    row = rows[("600001.SH", "2024-06-30")]
    assert row["available_at"] == "2024-08-30"
    assert row["revised_after_publish"] == 0
    # delayed filer: announcement_time is the delay-notice date, not the
    # financials publication date -> delayed_estimate, never exact_event
    row = rows[("600006.SH", "2024-12-31")]
    assert row["available_at_quality"] == "delayed_estimate"
    assert row["available_at"] >= "2025-04-30"


def test_delayed_estimate_uses_event_plus_grace(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    with store.connect() as conn:
        row = conn.execute(
            """
            SELECT available_at FROM financial_pit_panel
            WHERE instrument_id = '600006.SH' AND report_period = '2024-12-31'
            """
        ).fetchone()
    # event 2026-05-10 + 90 days grace, vs deadline 2025-04-30 -> max is 2026-08-08
    assert row["available_at"] == "2026-08-08"


def test_cross_section_visibility_and_fallback(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    result = store.query_cross_section("20250401", exchanges="SSE")
    by_instrument = {item["instrument_id"]: item for item in result["items"]}
    # 2024 annual visible from 2025-03-10
    assert by_instrument["600001.SH"]["report_period"] == "20241231"
    # revision-guarded row invisible on 2024-11-01: falls back to 2024-06-30
    strict = store.query_cross_section("20241101")
    strict_map = {item["instrument_id"]: item for item in strict["items"]}
    assert strict_map["600001.SH"]["report_period"] == "20240630"
    # guard is strict-only: measured thresholds were not exceeded, so the
    # lenient parameter is intentionally not implemented (spec scenario)
    assert strict["guard"] == "strict"
    with pytest.raises(TypeError):
        store.query_cross_section("20241101", revision_guard="lenient")
    # no future data: 2025Q1 invisible before its publish date
    assert "20250331" not in {
        item["report_period"] for item in result["items"]
    }


def test_cross_section_stats_and_universe_note(
    store: FinancialPitPanelStore,
) -> None:
    store.build_full("test-v1")
    result = store.query_cross_section("20260101")
    # 600006 delayed_estimate (2026-08-08) is correctly invisible at 2026-01-01
    assert result["total"] == 5
    assert "period_lag_distribution" in result["stats"]
    assert result["stats"]["available_at_quality"].get("exact", 0) >= 1
    assert "delisted not covered" in result["universe_note"]
    later = store.query_cross_section("20260901")
    assert later["total"] == 6  # delayed filer becomes visible after estimate


def test_attribution_quality_marking(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    with store.connect() as conn:
        rows = {
            (r["instrument_id"], r["report_period"]): r["attribution_quality"]
            for r in conn.execute(
                "SELECT instrument_id, report_period, attribution_quality "
                "FROM financial_pit_panel"
            )
        }
    assert rows[("600001.SH", "2024-12-31")] == "parent"
    assert rows[("600003.SH", "2024-12-31")] == "unverified"
    assert rows[("600004.SH", "2024-12-31")] == "mixed"
    assert rows[("600005.SH", "2024-12-31")] == "parent"  # no values -> default


def test_per_share_derivations_use_reported_facts(
    store: FinancialPitPanelStore,
) -> None:
    store.build_full("test-v1")
    with store.connect() as conn:
        row = conn.execute(
            """
            SELECT eps, bps, ocfps, shares_outstanding,
                   roe, gross_margin, current_ratio
            FROM financial_pit_panel
            WHERE instrument_id = '600001.SH' AND report_period = '2024-12-31'
            """
        ).fetchone()
    assert row["eps"] == 1.25  # reported basic EPS preferred
    assert row["bps"] == pytest.approx(500.0 / 80.0)
    assert row["ocfps"] == pytest.approx(80.0 / 80.0)
    # percent-semantic ratios rescaled to percent; multiples stay as-is
    assert row["roe"] == pytest.approx(2.67)
    assert row["gross_margin"] == pytest.approx(91.4)
    assert row["current_ratio"] == pytest.approx(1.9)
    # missing share facts -> null per-share, never zero placeholders
    with store.connect() as conn:
        row = conn.execute(
            """
            SELECT eps, bps, ocfps FROM financial_pit_panel
            WHERE instrument_id = '600003.SH' AND report_period = '2024-12-31'
            """
        ).fetchone()
    assert row["eps"] is None and row["bps"] is None and row["ocfps"] is None


def test_yoy_and_ttm_boundaries(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    with store.connect() as conn:
        rows = {
            r["report_period"]: r
            for r in conn.execute(
                """
                SELECT report_period, rev_yoy, np_yoy, ttm_revenue, ttm_net_income
                FROM financial_pit_panel WHERE instrument_id = '600001.SH'
                """
            )
        }
    # 2024 annual vs 2023 annual -> YoY computable, TTM computable
    annual = rows["2024-12-31"]
    assert annual["rev_yoy"] == pytest.approx((1000.0 / 900.0 - 1.0) * 100.0)
    assert annual["np_yoy"] == pytest.approx((100.0 / 90.0 - 1.0) * 100.0)
    assert annual["ttm_revenue"] == pytest.approx(1000.0 + 900.0 - 900.0)
    # 2022 annual has no 2021 prior -> null boundaries
    assert rows["2022-12-31"]["rev_yoy"] is None
    assert rows["2022-12-31"]["ttm_revenue"] is None


def test_effective_at_next_trading_day(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    with store.connect() as conn:
        rows = {
            r["report_period"]: r["effective_at"]
            for r in conn.execute(
                """
                SELECT report_period, effective_at FROM financial_pit_panel
                WHERE instrument_id = '600001.SH'
                """
            )
        }
    # published 2025-03-10 (trading day in fixture) -> next trading day after
    assert rows["2024-12-31"] == "2025-03-11"
    # published 2025-04-20 -> next trading day 2025-04-21 (fixture trading day)
    assert rows["2025-03-31"] == "2025-04-21"


def test_coverage_expected_gap_and_sparse(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    result = store.coverage()
    by_period = {p["report_period"]: p for p in result["periods"]}
    bulk = by_period["20241231"]
    assert bulk["is_bulk_period"] is True
    assert bulk["expected_count"] == 6  # lifecycle universe at 2024-12-31
    assert bulk["actual_count"] == 5
    assert bulk["gap_count"] == 1
    assert bulk["is_complete"] is True  # deadline 2025-04-30 already passed
    sparse = by_period["20221231"]
    assert sparse.get("sparse") is True
    assert sparse["expected_count"] is None
    assert sparse["reverse_discrepancy_count"] is None
    assert result["expected_count_derivation"]
    # reverse discrepancy: 600001.SH is in the lifecycle universe, but the
    # 2024-12-31 panel set also includes instruments listed later (none in
    # this fixture) -> 0; expected/gap/reverse all populated on bulk periods
    assert bulk["reverse_discrepancy_count"] == 0
    assert "recent_source_change_count" in bulk
    # every panel endpoint carries the universe note (spec: any panel API)
    assert "delisted not covered" in result["universe_note"]
    panel_resp = store.query_panel(limit=1)
    assert "delisted not covered" in panel_resp["universe_note"]
    changes_resp = store.changes(limit=1)
    assert "delisted not covered" in changes_resp["universe_note"]


def test_changes_cursor_is_monotonic_and_replayable(
    store: FinancialPitPanelStore,
) -> None:
    page = store.changes(limit=1)
    assert len(page["entries"]) == 1
    first = page["entries"][0]
    replay = store.changes(cursor=page["cursor"], limit=1)
    assert replay["entries"] == page["entries"]  # idempotent replay
    following = store.changes(cursor=page["next_cursor"], limit=10)
    assert all(
        entry["sequence_id"] > first["sequence_id"]
        for entry in following["entries"]
    )


def test_measure_revision_impact_thresholds(
    store: FinancialPitPanelStore,
) -> None:
    store.build_full("test-v1")
    impact = store.measure_revision_impact()
    assert impact["revised_rows"] == 1
    assert impact["total_rows"] == 12
    # 1/12 = 8.3% > 5% ratio threshold -> guard required even without quarter crossing
    assert impact["guard_required"] is True


def test_refresh_pairs_updates_single_row(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            UPDATE financial_facts SET net_income = 999.0
            WHERE instrument_id = '600002.SZ' AND report_period = '2024-06-30'
            """
        )
        conn.commit()
    result = store.refresh_pairs([("600002.SZ", "2024-06-30")])
    assert result["refreshed"] == 1
    with store.connect() as conn:
        value = conn.execute(
            """
            SELECT net_income FROM financial_pit_panel
            WHERE instrument_id = '600002.SZ' AND report_period = '2024-06-30'
            """
        ).fetchone()[0]
    assert value == 999.0


def test_refresh_pairs_deletes_rows_missing_from_source(
    store: FinancialPitPanelStore,
) -> None:
    store.build_full("test-v1")
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            DELETE FROM financial_facts
            WHERE instrument_id = '600002.SZ' AND report_period = '2024-06-30'
            """
        )
        conn.commit()
    store.refresh_pairs([("600002.SZ", "2024-06-30")])
    with store.connect() as conn:
        remaining = conn.execute(
            """
            SELECT COUNT(*) FROM financial_pit_panel
            WHERE instrument_id = '600002.SZ' AND report_period = '2024-06-30'
            """
        ).fetchone()[0]
    assert remaining == 0


def test_panel_raw_access_and_filters(store: FinancialPitPanelStore) -> None:
    store.build_full("test-v1")
    result = store.query_panel(instrument_id="600001.SH", limit=100)
    assert result["total"] >= 6
    periods = {item["report_period"] for item in result["items"]}
    assert "20241231" in periods
    filtered = store.query_panel(
        report_period="20240630", available_at_lte="20250101"
    )
    # 600001 (published 2024-08-30) + 600002 (statutory 2024-08-31) both visible
    assert filtered["total"] == 2
