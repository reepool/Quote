"""Report rendering tests for field-level fill and source reconciliation."""

from scheduler.tasks import _format_financial_disclosure_scheduler_report


def _base_result() -> dict:
    return {
        "status": "success",
        "db_path": "data/financials.db",
        "report_periods": ["2026-09-30"],
        "source_routing": {
            "cninfo_attempts": 1,
            "cninfo_parsed_instrument_periods": 1,
            "cninfo_successes": 0,
            "cninfo_missing_required_core_facts": [
                "revenue",
                "equity_parent",
            ],
            "cninfo_missing_or_ambiguous": 1,
            "fallback_attempts": 1,
            "fallback_successes": 1,
            "final_source": "mixed",
            "final_source_counts": {"cninfo": 0, "fallback": 1},
            "source_collection_complete": True,
            "source_fact_reconciliation": {"enabled": False},
        },
    }


def test_missing_fields_split_into_systematic_and_incidental():
    result = _base_result()
    result["source_routing"]["source_fact_reconciliation"] = {
        "enabled": True,
        "targets_with_official_rows": 100,
        "official_present_core_fact_counts": {
            "revenue": 100,
            "equity_parent": 0,
        },
    }
    report = _format_financial_disclosure_scheduler_report(result)
    assert "CNInfo系统性缺失字段: equity_parent（缺 100/100）" in report
    assert "CNInfo个别缺失字段: revenue" in report
    assert "CNInfo待补字段:" not in report


def test_fill_and_reconciliation_lines_rendered():
    result = _base_result()
    result["source_routing"]["official_core_fill"] = {
        "enabled": True,
        "official_sources": ["cninfo"],
    }
    result["source_routing"]["source_fact_reconciliation"].update(
        {
            "enabled": True,
            "merged_official_fields": 4,
            "filled_fallback_fields": 1,
            "cleaned_fallback_facts": 4,
            "compared_facts": 4,
            "consistent_facts": 3,
            "source_differences": 1,
            "sanity_reviews": 0,
            "samples": [
                {
                    "instrument_id": "002254.SZ",
                    "report_period": "2026-09-30",
                    "canonical_fact": "revenue",
                    "official_source": "cninfo",
                    "official_value": 100.0,
                    "fallback_source": "akshare",
                    "fallback_value": 150.0,
                }
            ],
            "sanity_samples": [],
        }
    )
    report = _format_financial_disclosure_scheduler_report(result)
    assert "字段级补缺: 官方优先来源（cninfo）保留官方值 4 项，第三方补缺 1 项，清理对照过程数据 4 项" in report
    assert "来源对照: 比较 4 项，一致 3 项，差异 1 项" in report
    assert (
        "对照差异: 002254.SZ@2026-09-30 revenue: 官方 100.0 (cninfo) vs 第三方 150.0 (akshare)"
        in report
    )


def test_sanity_samples_rendered_as_review_only():
    result = _base_result()
    result["source_routing"]["official_core_fill"] = {
        "enabled": True,
        "official_sources": ["cninfo"],
    }
    result["source_routing"]["source_fact_reconciliation"].update(
        {
            "enabled": True,
            "sanity_reviews": 1,
            "sanity_samples": [
                {
                    "instrument_id": "002254.SZ",
                    "report_period": "2026-09-30",
                    "canonical_fact": "equity_parent",
                    "official_value": 7_935_173_800.0,
                    "fallback_value": 7_900_000_000.0,
                }
            ],
        }
    )
    report = _format_financial_disclosure_scheduler_report(result)
    assert "复核提示 1 项" in report
    assert "复核提示: 002254.SZ@2026-09-30 equity_parent 定义校验异常" in report
    assert "仅提示不阻断" in report


def test_legacy_report_without_new_keys_unchanged():
    result = _base_result()
    report = _format_financial_disclosure_scheduler_report(result)
    assert "CNInfo待补字段: revenue，equity_parent" in report
    assert "字段级补缺" not in report
    assert "来源对照" not in report
