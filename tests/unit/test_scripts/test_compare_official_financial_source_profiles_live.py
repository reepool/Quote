from scripts.dev_validation.compare_official_financial_source_profiles_live import (
    build_assessment,
    compare_core_facts,
)


def test_compare_core_facts_reports_equity_only_mismatch():
    sse_facts = {
        "600000.SH|2023-12-31": {
            "fields": {
                "revenue": 173434000000.0,
                "net_income": 36702000000.0,
                "equity": 724749000000.0,
            }
        }
    }
    cninfo_facts = {
        "600000.SH|2023-12-31": {
            "fields": {
                "revenue": 173434000000.0,
                "net_income": 36702000000.0,
                "equity": 732884000000.0,
            }
        }
    }

    result = compare_core_facts(
        baseline_source="sse",
        other_source="cninfo",
        baseline_facts=sse_facts,
        other_facts=cninfo_facts,
        core_fields=["revenue", "net_income", "equity"],
        relative_tolerance=1e-6,
        absolute_tolerance=1e-3,
    )

    assert result["compared_field_count"] == 3
    assert result["matched_field_count"] == 2
    assert result["mismatch_count"] == 1
    assert result["mismatches"][0]["field"] == "equity"
    assert result["match_ratio"] == 0.666667


def test_build_assessment_keeps_sse_default_when_cninfo_equity_semantics_differ():
    profiles = {
        "sse": {
            "source_profile": "sse_commonquery",
            "elapsed_seconds_avg": 5.304,
            "throughput_instrument_periods_per_minute_avg": 56.558,
            "failed_instrument_period_count": 0,
            "total_numeric_facts_written_avg": 444.0,
            "latest_core_fact_coverage": {"coverage_ratio": 1.0},
        },
        "cninfo": {
            "source_profile": "cninfo_data20",
            "elapsed_seconds_avg": 10.211,
            "throughput_instrument_periods_per_minute_avg": 29.381,
            "failed_instrument_period_count": 0,
            "total_numeric_facts_written_avg": 102.0,
            "latest_core_fact_coverage": {"coverage_ratio": 1.0},
            "latest_numeric_fact_coverage": {"status": "needs_review"},
        },
    }
    comparisons = [
        {
            "baseline_source": "sse",
            "other_source": "cninfo",
            "match_ratio": 0.833333,
            "mismatch_count": 5,
            "mismatches": [{"field": "equity"}],
        }
    ]

    assessment = build_assessment(profiles=profiles, comparisons=comparisons)

    assert assessment["speed_rank"][0]["source"] == "sse"
    assert assessment["stability_rank"][0]["failed_instrument_period_count"] == 0
    assert assessment["warnings"] == [
        "cninfo_numeric_fact_coverage_requires_review",
        "equity_mismatch_likely_parent_vs_total_equity_semantics"
    ]
    assert (
        assessment["provisional_recommendation"]
        == "keep_sse_commonquery_as_sse_default_and_use_cninfo_for_cross_check_after_semantic_review"
    )
