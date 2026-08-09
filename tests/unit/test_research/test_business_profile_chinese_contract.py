import pytest

from research.business_profile_fact_catalog import load_business_fact_catalog
from research.business_profile_llm import (
    LLM_REPORT_SCHEMA_VERSION,
    _parse_and_validate_report,
)
from research.business_profile_unit_conversions import load_unit_conversion_catalog


def _report(fact):
    return {
        "schema_version": LLM_REPORT_SCHEMA_VERSION,
        "fact_catalog_version": "business_profile_facts.2026.2",
        "instrument_id": "600403.SH",
        "report_period": "2025-12-31",
        "facts": [fact],
        "relationships": [],
        "warnings": [],
        "model_derived_hints": {"suggested_margin": 0.99},
    }


def test_chinese_source_fields_and_diagnostic_hints_are_accepted():
    report = _parse_and_validate_report(
        _report(
            {
                "field_id": "segment.revenue",
                "status": "candidate",
                "review_status": "candidate",
                "source_label_raw": "LED 产品收入",
                "source_value": "100",
                "source_unit_raw": "万元",
                "semantic_summary_zh": "LED 产品收入为一百万元",
                "model_derived_hints": {"translated_label": "LED revenue"},
                "evidence_section_ids": ["section-1"],
            }
        ),
        instrument_id="600403.SH",
        report_period="2025-12-31",
        valid_section_ids={"section-1"},
        fact_catalog=load_business_fact_catalog(),
        unit_catalog=load_unit_conversion_catalog(),
    )
    assert report["facts"][0]["source_label_raw"] == "LED 产品收入"
    assert report["model_derived_hints"]["suggested_margin"] == 0.99


def test_unknown_unit_is_preserved_as_pending_instead_of_rejecting_response():
    report = _parse_and_validate_report(
        _report(
            {
                "field_id": "segment.revenue",
                "status": "candidate",
                "review_status": "candidate",
                "source_label_raw": "主营业务收入",
                "source_value": "100",
                "source_unit_raw": "结算箱",
                "semantic_summary_zh": "主营业务收入按结算箱披露",
                "evidence_section_ids": ["section-1"],
            }
        ),
        instrument_id="600403.SH",
        report_period="2025-12-31",
        valid_section_ids={"section-1"},
        fact_catalog=load_business_fact_catalog(),
        unit_catalog=load_unit_conversion_catalog(),
    )
    assert report["facts"][0]["unit_resolution_status"] == "unit_resolution_pending"


def test_english_only_semantic_summary_is_a_language_contract_error():
    with pytest.raises(ValueError, match="Simplified Chinese"):
        _parse_and_validate_report(
            _report(
                {
                    "field_id": "segment.name",
                    "status": "candidate",
                    "review_status": "candidate",
                    "source_label_raw": "煤炭",
                    "source_value": "煤炭",
                    "semantic_summary_zh": "Coal production business",
                    "evidence_section_ids": ["section-1"],
                }
            ),
            instrument_id="600403.SH",
            report_period="2025-12-31",
            valid_section_ids={"section-1"},
            fact_catalog=load_business_fact_catalog(),
            unit_catalog=load_unit_conversion_catalog(),
        )
