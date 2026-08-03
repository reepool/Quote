import sqlite3

import pytest

from research.business_profile_rollout import (
    build_field_family_promotion_manifests,
    build_frozen_atomic_benchmark,
    evaluate_atomic_benchmark,
    evaluate_rollout_expansion,
    evaluate_scheduler_readiness,
    run_bounded_production_pilot,
    run_shadow_backfill_validation,
    select_first_industry_cohort,
)
from scripts.dev_validation.validate_business_profile_rollout_gates import (
    _run_governed_validation,
    _run_kill_switch_drill,
    _run_rollback_drill,
    _storage,
)


def _item(item_id, instrument_id, document_id, family="atomic_activities"):
    return {
        "item_id": item_id,
        "instrument_id": instrument_id,
        "source_document_id": document_id,
        "field_family": family,
        "temporal_class": "report_flow",
        "exact_spans": [{"page": 1, "quote": "issuer sells coking coal"}],
        "atomic_activities": [{"action": "sells", "object": "coking coal"}],
        "relationships": [],
        "exposure_facts": [{"fact_type": "sales_volume"}],
        "negative_assertions": [{"prohibited": "upstream_role"}],
    }


def _evaluation(item, **overrides):
    values = {
        "item_id": item["item_id"],
        "field_family": item["field_family"],
        "true_positive": 1,
        "false_positive": 0,
        "false_negative": 0,
        "unsupported_output": False,
        "evidence_valid": True,
        "temporal_correct": True,
        "stable_replay": True,
        "deterministic_complete": True,
        "llm_calls": 0,
        "cost": 0,
        "latency_seconds": 0.1,
        "exception_tier": None,
    }
    values.update(overrides)
    return values


def test_frozen_benchmark_keeps_all_issuer_reports_in_one_split():
    items = [
        _item("a1", "601088.SH", "annual-2025"),
        _item("a2", "601088.SH", "semi-2026"),
        _item("b1", "600362.SH", "annual-2025"),
    ]
    benchmark = build_frozen_atomic_benchmark(
        items,
        runtime_identities={"model": "model.v1", "schema": "schema.v1"},
    )

    issuer_splits = {
        item["split"]
        for item in benchmark["items"]
        if item["instrument_id"] == "601088.SH"
    }
    assert len(issuer_splits) == 1
    assert benchmark["benchmark_hash"]
    assert benchmark["counts"]["items"] == 3


def test_evaluation_and_manifest_fail_on_unsupported_output_despite_recall():
    items = [_item("a1", "601088.SH", "annual-2025")]
    benchmark = build_frozen_atomic_benchmark(
        items, runtime_identities={"model": "model.v1"}
    )
    evaluation = evaluate_atomic_benchmark(
        benchmark,
        [_evaluation(items[0], unsupported_output=True)],
    )
    manifests = build_field_family_promotion_manifests(
        evaluation,
        thresholds={
            "atomic_activities": {
                "min_precision": 0.99,
                "min_recall": 0.90,
                "min_evidence_validity": 1.0,
                "min_temporal_correctness": 1.0,
                "max_unsupported_output_rate": 0.0,
                "max_human_exception_rate": 0.05,
            }
        },
    )
    manifest = manifests["atomic_activities"]
    assert evaluation["field_families"]["atomic_activities"]["recall"] == 1.0
    assert manifest["enabled"] is False
    assert "threshold_failed:unsupported_output_rate:max" in manifest["reason_codes"]


def test_first_industry_cohort_is_selected_from_measured_coverage():
    selected = select_first_industry_cohort(
        [
            {
                "industry_group": "coal",
                "instrument_count": 20,
                "explicit_table_coverage": 0.9,
                "native_text_quality": 0.95,
                "catalog_coverage": 0.9,
                "semantic_difficulty": 0.5,
            },
            {
                "industry_group": "steel",
                "instrument_count": 30,
                "explicit_table_coverage": 0.7,
                "native_text_quality": 0.8,
                "catalog_coverage": 0.75,
                "semantic_difficulty": 0.7,
            },
        ]
    )
    assert selected["selected_industry_group"] == "coal"


def test_shadow_backfill_replays_on_copy_and_preserves_source_database(tmp_path):
    source = tmp_path / "research.db"
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE facts (id TEXT PRIMARY KEY, value INTEGER)")

    def runner(path):
        with sqlite3.connect(path) as conn:
            conn.execute("INSERT OR IGNORE INTO facts VALUES ('one', 1)")
            rows = conn.execute("SELECT * FROM facts ORDER BY id").fetchall()
        return {
            "governed_output": rows,
            "bulk_transaction_ok": True,
            "point_in_time_reads_ok": True,
            "machine_rework_recovery_ok": True,
            "zero_valuation_leakage": True,
        }

    result = run_shadow_backfill_validation(source, runner)
    assert result["passed"] is True
    with sqlite3.connect(source) as conn:
        assert conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 0


def test_bounded_pilot_requires_enabled_manifest_and_drills():
    pilot = run_bounded_production_pilot(
        instrument_ids=["601088.SH", "600362.SH"],
        enabled_manifests={"atomic_activities": {"enabled": True}},
        runner=lambda instruments: {
            "zero_candidate_valuation_leakage": True,
            "audited_system_promotion": True,
            "bounded_scope": len(instruments) <= 20,
        },
        rollback_drill=lambda: True,
        kill_switch_drill=lambda: True,
    )
    assert pilot["passed"] is True

    failed = run_bounded_production_pilot(
        instrument_ids=["601088.SH"],
        enabled_manifests={"atomic_activities": {"enabled": True}},
        runner=lambda instruments: {
            "zero_candidate_valuation_leakage": True,
            "audited_system_promotion": True,
            "bounded_scope": True,
        },
        rollback_drill=lambda: False,
        kill_switch_drill=lambda: True,
    )
    assert failed["passed"] is False
    assert "rollback_drill_failed" in failed["reason_codes"]


def test_shadow_and_pilot_drills_execute_real_governed_components(tmp_path):
    source = tmp_path / "production-shaped.db"
    _storage(source).initialize()
    shadow = run_shadow_backfill_validation(
        source,
        lambda path: _run_governed_validation(
            path,
            ("601088.SH",),
            exercise_machine_rework=True,
        ),
    )
    pilot = run_bounded_production_pilot(
        instrument_ids=["601088.SH", "600362.SH"],
        enabled_manifests={"structured_segments": {"enabled": True}},
        runner=lambda instruments: _run_governed_validation(
            tmp_path / "pilot.db",
            instruments,
            exercise_machine_rework=False,
        ),
        rollback_drill=lambda: _run_rollback_drill(tmp_path / "rollback.db"),
        kill_switch_drill=lambda: _run_kill_switch_drill(tmp_path / "kill-switch.json"),
    )

    assert shadow["passed"] is True
    assert shadow["first_run"]["audited_system_promotion"] is True
    assert shadow["first_run"]["machine_rework_recovery_ok"] is True
    assert pilot["passed"] is True
    assert pilot["result"]["point_in_time_reads_ok"] is True


def test_expansion_and_scheduler_require_quality_capacity_and_backlog_gates():
    expansion = evaluate_rollout_expansion(
        {
            "precision": 0.995,
            "drift_rate": 0.01,
            "average_cost": 0.2,
            "human_exception_rate": 0.02,
        },
        {
            "min_precision": 0.99,
            "max_drift_rate": 0.02,
            "max_average_cost": 0.5,
            "max_human_exception_rate": 0.05,
        },
    )
    assert expansion["expand"] is True

    capacity = evaluate_scheduler_readiness(
        issuer_count=5500,
        changed_issuer_rate=0.05,
        seconds_per_changed_issuer=10,
        available_window_seconds=3600,
        exception_backlog=20,
        maximum_exception_backlog=100,
    )
    assert capacity["scheduler_ready"] is True
    blocked = evaluate_scheduler_readiness(
        issuer_count=5500,
        changed_issuer_rate=0.5,
        seconds_per_changed_issuer=10,
        available_window_seconds=3600,
        exception_backlog=101,
        maximum_exception_backlog=100,
    )
    assert blocked["scheduler_ready"] is False
    assert set(blocked["reason_codes"]) == {
        "full_market_capacity_exceeded",
        "exception_backlog_policy_failed",
    }


def test_evaluation_rejects_items_outside_frozen_corpus():
    benchmark = build_frozen_atomic_benchmark(
        [_item("a1", "601088.SH", "annual-2025")],
        runtime_identities={"model": "model.v1"},
    )
    with pytest.raises(ValueError, match="outside frozen benchmark"):
        evaluate_atomic_benchmark(
            benchmark,
            [_evaluation(_item("other", "601088.SH", "annual-2025"))],
        )
