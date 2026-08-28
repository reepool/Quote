import json
from datetime import datetime

from scripts.research_financial_l1_full_import import (
    DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES,
    DEFAULT_ACCEPTED_SOURCE_GAPS,
    accepted_source_gaps_from_manifest_lifecycle,
    apply_batch_status_to_progress,
    merge_accepted_source_gaps,
    resolve_full_import_log_dir,
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


def test_manifest_lifecycle_exclusions_become_accepted_source_gaps():
    accepted = accepted_source_gaps_from_manifest_lifecycle(
        {
            "targets": [
                {
                    "instrument_id": "600355.SH",
                    "excluded_report_periods": [
                        {
                            "report_period": "2026-03-31",
                            "classification": "post_delisting_or_no_disclosure",
                        }
                    ],
                }
            ]
        },
        required_canonical_facts=["revenue", "net_income_parent"],
    )

    entry = accepted[("600355.SH", "2026-03-31")]
    assert entry["facts"] == {"revenue", "net_income_parent"}
    assert entry["classification"] == "post_delisting_or_no_disclosure"


def test_manifest_financial_disclosure_events_become_accepted_source_gaps():
    accepted = accepted_source_gaps_from_manifest_lifecycle(
        {
            "targets": [
                {
                    "instrument_id": "002731.SZ",
                    "excluded_report_periods": [
                        {
                            "report_period": "2025-12-31",
                            "classification": "periodic_report_delayed_or_suspended",
                            "disclosure_events": [{"announcement_id": "a1"}],
                        }
                    ],
                }
            ]
        },
        required_canonical_facts=["revenue", "net_income_parent"],
    )

    entry = accepted[("002731.SZ", "2025-12-31")]
    assert entry["facts"] == {"revenue", "net_income_parent"}
    assert entry["classification"] == "periodic_report_delayed_or_suspended"


def test_merge_accepted_source_gaps_keeps_all_fact_names():
    merged = merge_accepted_source_gaps(
        {
            ("600355.SH", "2026-03-31"): {
                "facts": {"revenue"},
                "classification": "source_confirmed_missing",
            }
        },
        {
            ("600355.SH", "2026-03-31"): {
                "facts": {"net_income_parent"},
                "classification": "source_confirmed_missing",
            }
        },
    )

    assert merged[("600355.SH", "2026-03-31")]["facts"] == {
        "revenue",
        "net_income_parent",
    }


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


def test_needs_review_batch_is_not_marked_completed():
    progress = {"completed_batches": [1], "failed_batches": [], "review_batches": []}

    outcome = apply_batch_status_to_progress(
        progress,
        batch_index=2,
        status="needs_review",
        continue_on_needs_review=True,
        blocking_not_ready_read_count=3,
        evidence_path="batch_0002.json",
    )

    assert outcome == "review"
    assert progress["completed_batches"] == [1]
    assert progress["failed_batches"] == []
    assert progress["review_batches"] == [
        {
            "batch_index": 2,
            "status": "needs_review",
            "blocking_not_ready_read_count": 3,
            "evidence_path": "batch_0002.json",
        }
    ]


def test_successful_retry_clears_needs_review_batch():
    progress = {
        "completed_batches": [1],
        "failed_batches": [],
        "review_batches": [
            {
                "batch_index": 2,
                "status": "needs_review",
                "blocking_not_ready_read_count": 3,
                "evidence_path": "batch_0002.json",
            }
        ],
    }

    outcome = apply_batch_status_to_progress(
        progress,
        batch_index=2,
        status="success",
        continue_on_needs_review=True,
    )

    assert outcome == "completed"
    assert progress["completed_batches"] == [1, 2]
    assert progress["review_batches"] == []


def test_resolve_full_import_log_dir_reuses_incomplete_run(tmp_path):
    incomplete = tmp_path / "20260827_233000"
    incomplete.mkdir()
    (incomplete / "progress_state.json").write_text(
        json.dumps(
            {
                "completed_batches": [1],
                "failed_batches": [2],
                "batch_results": [],
            }
        ),
        encoding="utf-8",
    )
    (incomplete / "manifest.json").write_text(
        json.dumps({"batches": [{"batch_index": 1}, {"batch_index": 2}]}),
        encoding="utf-8",
    )

    resolved = resolve_full_import_log_dir(None, resume=True, root=tmp_path)

    assert resolved == incomplete


def test_resolve_full_import_log_dir_starts_new_after_completed_run(tmp_path):
    finished = tmp_path / "20260827_233000"
    finished.mkdir()
    (finished / "progress_state.json").write_text(
        json.dumps(
            {
                "completed_batches": [1, 2],
                "failed_batches": [],
                "review_batches": [],
                "batch_results": [],
            }
        ),
        encoding="utf-8",
    )
    (finished / "manifest.json").write_text(
        json.dumps({"batches": [{"batch_index": 1}, {"batch_index": 2}]}),
        encoding="utf-8",
    )
    (finished / "final_summary.json").write_text(
        json.dumps({"status": "success"}),
        encoding="utf-8",
    )

    resolved = resolve_full_import_log_dir(
        None,
        resume=True,
        root=tmp_path,
        now=datetime(2026, 8, 28, 9, 20, 0),
    )

    assert resolved == tmp_path / "20260828_092000"


def test_resolve_full_import_log_dir_reuses_review_batches_not_completed(tmp_path):
    review_run = tmp_path / "20260827_233000"
    review_run.mkdir()
    (review_run / "progress_state.json").write_text(
        json.dumps(
            {
                "completed_batches": [1],
                "failed_batches": [],
                "review_batches": [{"batch_index": 2, "status": "needs_review"}],
            }
        ),
        encoding="utf-8",
    )
    (review_run / "manifest.json").write_text(
        json.dumps({"batches": [{"batch_index": 1}, {"batch_index": 2}]}),
        encoding="utf-8",
    )
    (review_run / "final_summary.json").write_text(
        json.dumps({"status": "success_with_review"}),
        encoding="utf-8",
    )

    resolved = resolve_full_import_log_dir(None, resume=True, root=tmp_path)

    assert resolved == review_run
