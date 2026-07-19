import json

import pytest

from research.business_profile_llm import (
    BusinessProfileDocumentSection,
    OpenAICompatibleBusinessProfileExtractor,
    OpenAICompatibleLlmConfig,
)


@pytest.fixture(autouse=True)
def _llm_test_key(monkeypatch):
    monkeypatch.setenv("QUOTE_LLM_API_KEY", "unit-test-key")


def _section():
    return BusinessProfileDocumentSection.build(
        section_id="page-31-main-business",
        page_number=31,
        heading="主营业务",
        text="公司主要生产动力煤。",
    )


def test_llm_interface_is_disabled_by_default():
    extractor = OpenAICompatibleBusinessProfileExtractor(OpenAICompatibleLlmConfig())

    with pytest.raises(RuntimeError, match="disabled"):
        extractor.extract(
            instrument_id="601088.SH",
            report_period="2025-12-31",
            sections=[_section()],
        )


def test_llm_interface_accepts_strict_candidate_report():
    captured = {}

    def transport(url, headers, payload, timeout):
        captured.update({"url": url, "headers": headers, "payload": payload, "timeout": timeout})
        report = {
            "schema_version": "business_profile_llm_report.v1",
            "fact_catalog_version": "business_profile_facts.2026.1",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "facts": [
                {
                    "field_id": "segment.name",
                    "status": "candidate",
                    "review_status": "candidate",
                    "raw_value": "动力煤",
                    "evidence_section_ids": ["page-31-main-business"],
                }
            ],
            "relationships": [
                {
                    "relationship_type": "produces",
                    "subject": "公司",
                    "object": "动力煤",
                    "explicitly_stated": True,
                    "review_status": "candidate",
                    "evidence_section_ids": ["page-31-main-business"],
                }
            ],
            "warnings": [],
        }
        return {
            "model": "provider-model-v2",
            "choices": [{"message": {"content": json.dumps(report)}}],
        }

    extractor = OpenAICompatibleBusinessProfileExtractor(
        OpenAICompatibleLlmConfig(
            enabled=True,
            base_url="http://127.0.0.1:8000",
            model="future-local-model",
        ),
        transport=transport,
    )
    result = extractor.extract(
        instrument_id="601088.SH",
        report_period="2025-12-31",
        sections=[_section()],
    )

    assert captured["url"].endswith("/v1/chat/completions")
    user_message = next(
        message for message in captured["payload"]["messages"] if message.get("role") == "user"
    )
    user_payload = json.loads(user_message["content"])
    assert user_payload["fact_catalog_version"] == "business_profile_facts.2026.1"
    assert any(item["field_id"] == "segment.name" for item in user_payload["fact_fields"])
    assert result.report["facts"][0]["review_status"] == "candidate"
    assert result.fact_catalog_version == "business_profile_facts.2026.1"
    assert result.request_hash
    assert result.response_hash
    assert result.model == "provider-model-v2"


def test_business_profile_config_preserves_defaults_and_explicit_zeroes():
    defaults = OpenAICompatibleLlmConfig.from_mapping({})
    assert defaults.api_key_env == "QUOTE_LLM_API_KEY"
    assert defaults.max_retries == 2
    assert defaults.max_schema_repair_attempts == 1
    assert defaults.requests_per_minute == 20

    disabled_limits = OpenAICompatibleLlmConfig.from_mapping(
        {
            "max_retries": 0,
            "max_schema_repair_attempts": 0,
            "requests_per_minute": 0,
        }
    )
    assert disabled_limits.max_retries == 0
    assert disabled_limits.max_schema_repair_attempts == 0
    assert disabled_limits.requests_per_minute == 0


def test_llm_interface_rejects_unknown_evidence_and_inferred_relationship():
    def transport(url, headers, payload, timeout):
        report = {
            "schema_version": "business_profile_llm_report.v1",
            "fact_catalog_version": "business_profile_facts.2026.1",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "facts": [],
            "relationships": [
                {
                    "relationship_type": "produces",
                    "subject": "公司",
                    "object": "动力煤",
                    "explicitly_stated": False,
                    "review_status": "candidate",
                    "evidence_section_ids": ["unknown-section"],
                }
            ],
            "warnings": [],
        }
        return {"choices": [{"message": {"content": json.dumps(report)}}]}

    extractor = OpenAICompatibleBusinessProfileExtractor(
        OpenAICompatibleLlmConfig(
            enabled=True,
            base_url="http://127.0.0.1:8000",
            model="future-local-model",
        ),
        transport=transport,
    )
    with pytest.raises(ValueError, match="explicitly stated"):
        extractor.extract(
            instrument_id="601088.SH",
            report_period="2025-12-31",
            sections=[_section()],
        )


def test_llm_interface_rejects_unknown_fact_catalog_field():
    def transport(url, headers, payload, timeout):
        report = {
            "schema_version": "business_profile_llm_report.v1",
            "fact_catalog_version": "business_profile_facts.2026.1",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "facts": [
                {
                    "field_id": "segment.product",
                    "status": "candidate",
                    "review_status": "candidate",
                    "raw_value": "动力煤",
                    "evidence_section_ids": ["page-31-main-business"],
                }
            ],
            "relationships": [],
            "warnings": [],
        }
        return {"choices": [{"message": {"content": json.dumps(report)}}]}

    extractor = OpenAICompatibleBusinessProfileExtractor(
        OpenAICompatibleLlmConfig(
            enabled=True,
            base_url="http://127.0.0.1:8000",
            model="future-local-model",
        ),
        transport=transport,
    )

    with pytest.raises(ValueError, match="unknown field_id"):
        extractor.extract(
            instrument_id="601088.SH",
            report_period="2025-12-31",
            sections=[_section()],
        )


def test_llm_interface_rejects_invalid_section_hash_and_duplicate_ids():
    with pytest.raises(ValueError, match="text_hash"):
        BusinessProfileDocumentSection(
            section_id="page-31-main-business",
            page_number=31,
            heading="主营业务",
            text="公司主要生产动力煤。",
            text_hash="forged",
        )

    extractor = OpenAICompatibleBusinessProfileExtractor(
        OpenAICompatibleLlmConfig(
            enabled=True,
            base_url="http://127.0.0.1:8000",
            model="future-local-model",
        ),
        transport=lambda *_: {},
    )
    section = _section()
    with pytest.raises(ValueError, match="duplicate LLM section_id"):
        extractor.extract(
            instrument_id="601088.SH",
            report_period="2025-12-31",
            sections=[section, section],
        )


def test_llm_interface_rejects_candidate_numeric_fact_without_unit():
    def transport(url, headers, payload, timeout):
        report = {
            "schema_version": "business_profile_llm_report.v1",
            "fact_catalog_version": "business_profile_facts.2026.1",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "facts": [
                {
                    "field_id": "segment.revenue",
                    "status": "candidate",
                    "review_status": "candidate",
                    "raw_value": "1000",
                    "evidence_section_ids": ["page-31-main-business"],
                }
            ],
            "relationships": [],
            "warnings": [],
        }
        return {"choices": [{"message": {"content": json.dumps(report)}}]}

    extractor = OpenAICompatibleBusinessProfileExtractor(
        OpenAICompatibleLlmConfig(
            enabled=True,
            base_url="http://127.0.0.1:8000",
            model="future-local-model",
        ),
        transport=transport,
    )

    with pytest.raises(ValueError, match="raw_unit"):
        extractor.extract(
            instrument_id="601088.SH",
            report_period="2025-12-31",
            sections=[_section()],
        )
