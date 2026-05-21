from scripts.research_financial_l1_full_import import (
    DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES,
    DEFAULT_ACCEPTED_SOURCE_GAPS,
    resolve_report_periods,
    selected_batches,
    split_ready_existing_targets,
)


def test_selected_batches_supports_range_and_limit():
    batches = [{"batch_index": index} for index in range(1, 8)]

    selected = selected_batches(
        batches,
        start_batch=2,
        end_batch=6,
        max_batches=3,
    )

    assert [batch["batch_index"] for batch in selected] == [2, 3, 4]


def test_default_accepted_source_gaps_include_reviewed_bse_and_star_cases():
    joined = "\n".join(DEFAULT_ACCEPTED_SOURCE_GAPS)

    assert "920020.BJ:2024-09-30:total_assets,total_liabilities,equity_parent" in joined
    assert "920045.BJ:2024-09-30:total_assets,total_liabilities,equity_parent" in joined
    assert "688807.SH:2024-09-30" in joined
    assert "pre_listing_incomplete_structured_statement" in joined


def test_full_import_defaults_accept_bse_source_gaps():
    assert DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES == ("BSE",)


def test_resolve_report_periods_prefers_explicit_periods():
    periods = resolve_report_periods(
        report_periods="2024-12-31,2024-09-30",
        period_window="latest",
        rolling_quarters=10,
        baseline_report_period="2024Q1",
        latest_report_period="2026Q1",
        optional_anchor_period=None,
        include_optional_anchor=False,
    )

    assert periods == ["2024-12-31", "2024-09-30"]


def test_resolve_report_periods_builds_latest_rolling_window():
    periods = resolve_report_periods(
        report_periods=None,
        period_window="latest",
        rolling_quarters=10,
        baseline_report_period="2024Q1",
        latest_report_period="2026Q1",
        optional_anchor_period=None,
        include_optional_anchor=False,
    )

    assert periods == [
        "2023-12-31",
        "2024-03-31",
        "2024-06-30",
        "2024-09-30",
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
        "2026-03-31",
    ]


class _Target:
    def __init__(self, instrument_id):
        self.instrument_id = instrument_id


def test_split_ready_existing_targets_skips_only_fully_ready_targets(tmp_path):
    import sqlite3

    db_path = tmp_path / "financials.db"
    required = ["revenue", "net_income_parent"]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE financial_numeric_facts (
                instrument_id TEXT,
                report_period TEXT,
                canonical_fact_name TEXT,
                fact_value REAL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO financial_numeric_facts
                (instrument_id, report_period, canonical_fact_name, fact_value)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("600519.SH", "2025-12-31", "revenue", 1.0),
                ("600519.SH", "2025-12-31", "net_income_parent", 1.0),
                ("600519.SH", "2026-03-31", "revenue", 1.0),
                ("600519.SH", "2026-03-31", "net_income_parent", 1.0),
                ("600355.SH", "2025-12-31", "revenue", 1.0),
            ],
        )

    ready, pending = split_ready_existing_targets(
        db_path,
        targets=[_Target("600519.SH"), _Target("600355.SH")],
        report_periods=["2025-12-31", "2026-03-31"],
        required_canonical_facts=required,
    )

    assert [target.instrument_id for target in ready] == ["600519.SH"]
    assert [target.instrument_id for target in pending] == ["600355.SH"]
