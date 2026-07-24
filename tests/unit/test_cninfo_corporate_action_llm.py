import json
from copy import deepcopy
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_corporate_action_documents import (
    CorporateActionDocumentBundle,
    CorporateActionPageText,
    CninfoCorporateActionDocumentService,
    extract_html_pages,
    page_text_cache_key,
    select_relevant_pages,
)
from data_sources.cninfo_corporate_action_llm import (
    ANALYSIS_SCHEMA,
    AUTO_PROMOTION_POLICY_VERSION,
    CninfoCorporateActionLlmResolver,
    FACT_ANALYSIS_SCHEMA,
    FACT_SCHEMA_VERSION,
    LEGACY_ANALYSIS_SCHEMA,
    LEGACY_SCHEMA_VERSION,
    MAX_ANALYSIS_OUTPUT_TOKENS,
    MAX_DATE_FACTS,
    MAX_ECONOMIC_DERIVATIONS,
    MAX_EVENT_PAGES,
    MAX_EVENT_PROMPT_CHARACTERS,
    PARSER_VERSION,
    SCHEMA_VERSION,
    SEMANTIC_VERIFICATION_SCHEMA_VERSION,
    _derive_economic_terms,
    _economic_semantic_binding_supported,
    _semantic_verification_payload,
    analysis_schema_for_version,
    classify_auto_promotion_eligibility,
    normalize_analysis_result,
    validate_analysis,
)
from research.announcements import AnnouncementRetrievalResult
from utils.llm import LlmDeadlineExceededError


def _page(text="本次权益分派向全体股东每10股派2.36元，除权除息日为2026年6月12日。"):
    import hashlib

    return CorporateActionPageText(
        page_number=3,
        text=text,
        text_hash=hashlib.sha256(text.encode()).hexdigest(),
        announcement_id="ann-1",
    )


def _result(page, **overrides):
    result = {
        "schema_version": LEGACY_SCHEMA_VERSION,
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "event_match": True,
        "analysis_status": "resolved_candidate",
        "event_type": "dividend",
        "event_stage": "implemented",
        "effective_date": "2026-06-12",
        "effective_date_type": "ex_dividend_date",
        "date_basis": "official_announcement_explicit_statement",
        "economic_terms": {
            "cash_dividend": {"value": 2.36, "unit": "per_10_shares", "currency": "CNY"},
            "bonus_shares": None,
            "capitalization_shares": None,
            "rights_shares": None,
            "rights_price": None,
        },
        "evidence": [{
            "announcement_id": "ann-1",
            "section_id": "ann-1:p3",
            "page_number": 3,
            "text_hash": page.text_hash,
            "exact_quote": page.text,
            "supports_fields": ["effective_date", "cash_dividend"],
        }],
        "alternative_dates": [],
        "conflicts": [],
        "confidence": 0.99,
        "reason": "正文明确披露",
    }
    result.update(overrides)
    return result


def _v2_result(page, **overrides):
    result = _result(page)
    result["schema_version"] = FACT_SCHEMA_VERSION
    result["evidence"][0]["evidence_id"] = "ev-1"
    result["date_facts"] = [{
        "date": "2026-06-12",
        "date_type": "ex_dividend_date",
        "date_basis": "除权除息日",
        "evidence_ids": ["ev-1"],
    }]
    result["economic_primitives"] = [{
        "fact_id": "cash-ratio",
        "fact_type": "cash_ratio",
        "value": 2.36,
        "unit": "CNY_per_10_shares",
        "beneficiary_scope": "all_shareholders",
        "evidence_ids": ["ev-1"],
    }]
    result.update(overrides)
    return result


def _semantic_verification(result, **overrides):
    payload = _semantic_verification_payload(result)
    decisions = [
        {
            "assertion_id": item["assertion_id"],
            "assertion_kind": item["assertion_kind"],
            "assertion_hash": item["assertion_hash"],
            "semantic_supported": True,
            "type_or_role_supported": True,
            "scope_supported": True,
            "reason": "official quote supports the assertion",
        }
        for item in payload["assertions"]
    ]
    verification = {
        "schema_version": SEMANTIC_VERIFICATION_SCHEMA_VERSION,
        "instrument_id": result["instrument_id"],
        "source_event_key": result["source_event_key"],
        "event_claim_hash": payload["event_claim_hash"],
        "event_match_supported": True,
        "event_type_supported": True,
        "event_stage_supported": True,
        "unresolved_language": False,
        "decisions": decisions,
        "conflicts": [],
    }
    verification.update(overrides)
    return verification


def _v3_result(page, *, include_verification=True, **overrides):
    result = _v2_result(page)
    result["schema_version"] = SCHEMA_VERSION
    result["date_facts"][0].update({
        "fact_id": "date-ex",
        "semantic_evidence": [{
            "evidence_id": "ev-1",
            "role_text": "除权除息日",
            "date_text": "2026年6月12日",
        }],
    })
    result["economic_primitives"][0]["semantic_evidence"] = [{
        "evidence_id": "ev-1",
        "subject_text": "全体股东",
        "relation_text": "派",
        "value_text": "2.36",
        "unit_text": "元",
        "basis_text": "每10股",
    }]
    result.update(overrides)
    if include_verification:
        verification = _semantic_verification(result)
        result["semantic_event_verification"] = {
            key: verification[key]
            for key in (
                "schema_version", "instrument_id", "source_event_key",
                "event_claim_hash",
                "event_match_supported", "event_type_supported",
                "event_stage_supported", "unresolved_language",
            )
        }
        result["semantic_verifications"] = verification["decisions"]
        result["semantic_verifier_conflicts"] = verification["conflicts"]
    return result


def _gateway_response(
    data, *, suffix="1", latency_ms=10, usage=None, warnings=(),
):
    return SimpleNamespace(
        data=data,
        response_hash=f"response-hash-{suffix}",
        request_id=f"request-{suffix}",
        model="fake",
        latency_ms=latency_ms,
        attempt_count=1,
        usage=usage,
        warnings=warnings,
    )


def test_validated_candidate_requires_exact_official_quote():
    page = _page()
    status, gates, normalized = validate_analysis(
        _result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        allowed_start=date(2026, 1, 1),
        allowed_end=date(2026, 12, 31),
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["effective_date"] == "2026-06-12"


def _eligible_auto_promotion_case():
    page = _page()
    status, gates, normalized = validate_analysis(
        _v3_result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        allowed_start=date(2026, 1, 1),
        allowed_end=date(2026, 12, 31),
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    normalized["_semantic_verifier"] = {"status": "success"}
    normalized["_semantic_verification_complete"] = True
    normalized["_input_context"] = {"context_complete": True}
    return page, status, gates, normalized


def test_economic_binding_accepts_descriptive_unit_and_post_value_relation():
    ratio_quote = "流通股股东每持有 10 股流通股股份将获得转增的 8.85 股股份。"
    ratio = {
        "fact_type": "capitalization_ratio",
        "value": 8.85,
        "unit": "per_10_shares",
        "beneficiary_scope": "circulating_shareholders",
    }
    ratio_binding = {
        "evidence_id": "ratio",
        "subject_text": "流通股股东每持有 10 股流通股股份",
        "relation_text": "将获得转增的",
        "value_text": "8.85",
        "unit_text": "股股份",
        "basis_text": "每持有 10 股流通股股份",
    }
    assert _economic_semantic_binding_supported(
        ratio, ratio_binding, {"ratio": ratio_quote}
    )

    split_span_quote = (
        "以公司流通股本579,726,850股为基数，"
        "公积金转增部分流通股股东每10股可获1.379960流通股。"
    )
    split_span_ratio = {
        "fact_type": "capitalization_ratio",
        "value": 1.379960,
        "unit": "per_10_shares",
        "beneficiary_scope": "circulating_shareholders",
    }
    split_span_binding = {
        "evidence_id": "split",
        "subject_text": "公积金转增部分流通股股东",
        "relation_text": "每10股可获",
        "value_text": "1.379960",
        "unit_text": "流通股",
        "basis_text": "以公司流通股本579,726,850股为基数",
    }
    assert _economic_semantic_binding_supported(
        split_span_ratio,
        split_span_binding,
        {"split": split_span_quote},
    )


def test_v3_optional_invalid_auxiliary_primitive_does_not_veto_valid_terms():
    page = _page()
    result = _v3_result(page, include_verification=False)
    result["economic_primitives"].append({
        "fact_id": "auxiliary-base",
        "fact_type": "base_share_count",
        "value": 100,
        "unit": "shares",
        "beneficiary_scope": "all_shareholders",
        "semantic_evidence": [{
            "evidence_id": "ev-1",
            "subject_text": "全体股东",
            "relation_text": "派",
            "value_text": "2.36",
            "unit_text": "元",
            "basis_text": "每10股",
        }],
    })
    verification = _semantic_verification(result)
    result["semantic_event_verification"] = {
        key: verification[key]
        for key in (
            "schema_version", "instrument_id", "source_event_key",
            "event_claim_hash", "event_match_supported", "event_type_supported",
            "event_stage_supported", "unresolved_language",
        )
    }
    result["semantic_verifications"] = verification["decisions"]
    result["semantic_verifier_conflicts"] = verification["conflicts"]

    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    assert status == "validated_candidate"
    assert gates["economic_primitives_in_evidence"] is True
    assert normalized["economic_primitive_validation_warnings"] == [
        "unusable_economic_primitive:auxiliary-base"
    ]

    base_quote = "以现有流通股股本 160,800,000 股为基数实施本次方案。"
    base = {
        "fact_type": "base_share_count",
        "value": 160800000,
        "unit": "shares",
        "beneficiary_scope": "circulating_shareholders",
    }
    base_binding = {
        "evidence_id": "base",
        "subject_text": "现有流通股股本",
        "relation_text": "为基数",
        "value_text": "160,800,000",
        "unit_text": "股",
        "basis_text": None,
    }
    assert _economic_semantic_binding_supported(
        base, base_binding, {"base": base_quote}
    )


def test_resume_revalidates_successful_semantics_and_retries_failed_verifier():
    page = _page()
    result = _v3_result(page)
    result.update({
        "analysis_status": "manual_required",
        "_semantic_verifier": {"status": "success"},
    })
    analysis = {
        "analysis_id": 1,
        "analysis_key": "analysis-key",
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "validation_status": "manual_required",
        "result": result,
    }
    event = {
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "candidates": [{"announcement_title": "权益分派实施公告"}],
    }
    reused = DataManager()._revalidate_resumable_cninfo_analysis(
        analysis=analysis,
        event=event,
        pages=[page],
        allowed_start=date(2026, 1, 1),
        allowed_end=date(2026, 12, 31),
    )
    assert reused is not None
    assert reused["validation_status"] == "validated_candidate"
    assert reused["_resume_revalidated"] is True

    failed = deepcopy(analysis)
    failed["result"]["_semantic_verifier"] = {
        "status": "error",
        "error_code": "transient_transport_error",
    }
    assert DataManager()._revalidate_resumable_cninfo_analysis(
        analysis=failed,
        event=event,
        pages=[page],
        allowed_start=date(2026, 1, 1),
        allowed_end=date(2026, 12, 31),
    ) is None


def test_current_native_all_gate_candidate_is_auto_promotable():
    page, status, gates, normalized = _eligible_auto_promotion_case()
    decision = classify_auto_promotion_eligibility(
        result=normalized,
        gate_results=gates,
        validation_status=status,
        schema_version=SCHEMA_VERSION,
        parser_version=PARSER_VERSION,
        pages=[page],
    )
    assert decision == {
        "policy_version": AUTO_PROMOTION_POLICY_VERSION,
        "eligible": True,
        "reasons": [],
        "evidence_key": "ann-1",
        "minimum_confidence": "0.90",
    }


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("ocr", "non_native_or_unusable_page"),
        ("conflict", "conflicts_present:conflicts"),
        ("stale_parser", "stale_parser_version"),
        ("stale_schema", "stale_schema_version"),
        ("low_confidence", "confidence_below_threshold"),
        ("incomplete_context", "context_incomplete"),
        ("non_final_stage", "event_stage_not_implemented"),
        ("semantic_incomplete", "semantic_verification_incomplete"),
        ("failed_gate", "not_all_gates_passed"),
    ],
)
def test_auto_promotion_uncertainty_stays_in_review(case, reason):
    page, status, gates, normalized = _eligible_auto_promotion_case()
    schema_version = SCHEMA_VERSION
    parser_version = PARSER_VERSION
    pages = [page]
    if case == "ocr":
        pages = [CorporateActionPageText(
            page_number=page.page_number,
            text=page.text,
            text_hash=page.text_hash,
            announcement_id=page.announcement_id,
            extraction_method="ocr",
            quality_status="ocr_usable",
        )]
    elif case == "conflict":
        normalized["conflicts"] = ["official statements conflict"]
    elif case == "stale_parser":
        parser_version = "cninfo_corporate_action_resolution_validator.v7"
    elif case == "stale_schema":
        schema_version = FACT_SCHEMA_VERSION
    elif case == "low_confidence":
        normalized["confidence"] = 0.89
    elif case == "incomplete_context":
        normalized["_input_context"]["context_complete"] = False
    elif case == "non_final_stage":
        normalized["event_stage"] = "approved"
    elif case == "semantic_incomplete":
        normalized["_semantic_verification_complete"] = False
    elif case == "failed_gate":
        gates["no_conflict"] = False

    decision = classify_auto_promotion_eligibility(
        result=normalized,
        gate_results=gates,
        validation_status=status,
        schema_version=schema_version,
        parser_version=parser_version,
        pages=pages,
    )
    assert decision["eligible"] is False
    assert reason in decision["reasons"]


@pytest.mark.parametrize("change", [
    {"effective_date": "2026-06-13"},
    {"event_stage": "expected"},
    {"economic_terms": {
        "cash_dividend": {"value": 2.36, "unit": "unknown", "currency": "CNY"},
        "bonus_shares": None, "capitalization_shares": None,
        "rights_shares": None, "rights_price": None,
    }},
    {"economic_terms": {
        "cash_dividend": {
            "value": 2.36,
            "unit": "per_10_shares",
            "currency": "USD",
        },
        "bonus_shares": None, "capitalization_shares": None,
        "rights_shares": None, "rights_price": None,
    }},
])
def test_hallucinated_expected_or_invalid_unit_result_fails_closed(change):
    page = _page()
    status, _, normalized = validate_analysis(
        _result(page, **change),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert normalized["effective_date"] == change.get(
        "effective_date", "2026-06-12"
    )
    assert normalized["_review_classification"]["review_tier"] == "deep_review"


def test_analysis_schema_bounds_canonical_fields():
    properties = ANALYSIS_SCHEMA["properties"]
    cash_schema = properties["economic_terms"]["properties"]["cash_dividend"]
    cash_term = cash_schema["anyOf"][1]["properties"]
    cash_required = cash_schema["anyOf"][1]["required"]
    rights_schema = properties["economic_terms"]["properties"]["rights_price"]
    rights_term = rights_schema["anyOf"][1]["properties"]
    assert cash_term["unit"]["enum"] == ["per_share", "per_10_shares"]
    assert cash_required == ["value", "unit", "currency"]
    assert rights_term["unit"]["enum"] == ["currency_per_share"]
    assert properties["evidence"]["maxItems"] == 12
    assert properties["alternative_dates"]["items"]["required"] == [
        "date", "date_type", "date_basis", "reason",
    ]
    assert properties["alternative_dates"]["maxItems"] == MAX_DATE_FACTS
    assert "evidence_id" in properties["evidence"]["items"]["required"]
    assert {"date_facts", "economic_primitives"}.issubset(
        ANALYSIS_SCHEMA["required"]
    )


def test_schema_dispatch_preserves_legacy_contract():
    assert analysis_schema_for_version(LEGACY_SCHEMA_VERSION) is LEGACY_ANALYSIS_SCHEMA
    assert analysis_schema_for_version(FACT_SCHEMA_VERSION) is FACT_ANALYSIS_SCHEMA
    assert analysis_schema_for_version(SCHEMA_VERSION) is ANALYSIS_SCHEMA
    with pytest.raises(ValueError, match="unsupported"):
        analysis_schema_for_version("unknown")


def test_v2_direct_ratio_and_official_date_fact_validate():
    page = _page(
        "本次权益分派向全体股东每10股派2.36元，"
        "除权除息日为2026年6月12日。"
    )
    status, gates, normalized = validate_analysis(
        _v2_result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["economic_terms"]["cash_dividend"]["value"] == 2.36
    assert normalized["economic_derivations"][0]["formula_id"] == (
        "direct_cash_ratio_normalization_v1"
    )


def test_v3_unseen_wording_uses_semantic_evidence_not_keyword_enumeration():
    page = _page(
        "本公司面向全体股东办理本次权益安排：每10股兑现2.36元；"
        "权益处理基准日期为2026年6月12日。"
    )
    result = _v3_result(page, include_verification=False)
    result["date_facts"][0]["semantic_evidence"][0].update({
        "role_text": "权益处理基准日期",
        "date_text": "2026年6月12日",
    })
    result["economic_primitives"][0]["semantic_evidence"][0].update({
        "subject_text": "全体股东",
        "relation_text": "兑现",
    })
    verification = _semantic_verification(result)
    result["semantic_event_verification"] = {
        key: verification[key]
        for key in (
            "schema_version", "instrument_id", "source_event_key",
            "event_claim_hash", "event_match_supported", "event_type_supported",
            "event_stage_supported", "unresolved_language",
        )
    }
    result["semantic_verifications"] = verification["decisions"]
    result["semantic_verifier_conflicts"] = verification["conflicts"]
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["economic_terms"]["cash_dividend"]["value"] == 2.36


@pytest.mark.parametrize("decision_field", [
    "semantic_supported", "type_or_role_supported", "scope_supported",
])
def test_v3_rejected_semantic_decision_fails_closed(decision_field):
    page = _page()
    result = _v3_result(page)
    economic_decision = next(
        item for item in result["semantic_verifications"]
        if item["assertion_kind"] == "economic_primitive"
    )
    economic_decision[decision_field] = False
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["economic_primitives_in_evidence"] is False


@pytest.mark.parametrize(("field", "value"), [
    ("value_text", "2.35"),
    ("unit_text", "万元"),
    ("basis_text", "每股"),
])
def test_v3_altered_numeric_unit_or_basis_span_fails_closed(field, value):
    page = _page()
    result = _v3_result(page)
    result["economic_primitives"][0]["semantic_evidence"][0][field] = value
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["economic_primitives_in_evidence"] is False


def test_v3_altered_date_span_fails_closed():
    page = _page()
    result = _v3_result(page)
    result["date_facts"][0]["semantic_evidence"][0]["date_text"] = (
        "2026年6月13日"
    )
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["date_facts_in_evidence"] is False


def test_v3_rejects_fact_id_collision_across_assertion_kinds():
    page = _page()
    result = _v3_result(page, include_verification=False)
    result["date_facts"][0]["fact_id"] = "cash-ratio"
    verification = _semantic_verification(result)
    result["semantic_event_verification"] = {
        key: verification[key]
        for key in (
            "schema_version", "instrument_id", "source_event_key",
            "event_claim_hash", "event_match_supported", "event_type_supported",
            "event_stage_supported", "unresolved_language",
        )
    }
    result["semantic_verifications"] = verification["decisions"]
    result["semantic_verifier_conflicts"] = verification["conflicts"]
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["assertion_ids_unique"] is False
    assert gates["semantic_verification_complete"] is False


def test_v3_rejects_stale_semantic_verification_after_claim_change():
    page = _page()
    event_changed = _v3_result(page)
    event_changed["event_type"] = "mixed"
    status, gates, _ = validate_analysis(
        event_changed,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["semantic_verification_complete"] is False

    assertion_changed = _v3_result(page)
    assertion_changed["economic_primitives"][0]["beneficiary_scope"] = (
        "circulating_shareholders"
    )
    status, gates, _ = validate_analysis(
        assertion_changed,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["semantic_verification_complete"] is False


def test_v3_ignores_extra_verifier_decision_not_retained_by_extraction():
    page = _page()
    result = _v3_result(page)
    result["semantic_verifications"].append({
        "assertion_id": "not-retained-by-extraction",
        "assertion_kind": "date_fact",
        "assertion_hash": "unused",
        "semantic_supported": True,
        "type_or_role_supported": True,
        "scope_supported": True,
        "reason": "extra verifier output",
    })
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "validated_candidate"
    assert gates["semantic_verification_complete"] is True
    assert normalized["_semantic_verifier_warnings"] == [
        "ignored_extra_semantic_assertion:not-retained-by-extraction"
    ]


def test_v3_deduplicated_facts_remain_replayable():
    page = _page()
    result = _v3_result(page, include_verification=False)
    duplicate = deepcopy(result["economic_primitives"][0])
    duplicate["fact_id"] = "cash-ratio-duplicate"
    result["economic_primitives"].append(duplicate)
    verification = _semantic_verification(result)
    result["semantic_event_verification"] = {
        key: verification[key]
        for key in (
            "schema_version", "instrument_id", "source_event_key",
            "event_claim_hash", "event_match_supported", "event_type_supported",
            "event_stage_supported", "unresolved_language",
        )
    }
    result["semantic_verifications"] = verification["decisions"]
    result["semantic_verifier_conflicts"] = verification["conflicts"]
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert len(normalized["economic_primitives"]) == 1
    assert len(normalized["semantic_verifications"]) == 2

    replay_status, replay_gates, _ = validate_analysis(
        normalized,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert replay_status == "validated_candidate"
    assert all(replay_gates.values())


def _share_reform_v2_result(page):
    result = _v2_result(
        page,
        event_type="share_reform",
        effective_date="2006-06-14",
        effective_date_type="listing_date",
        date_basis="上市日",
        economic_terms={
            "cash_dividend": {
                "value": 0.3581058,
                "unit": "per_10_shares",
                "currency": "CNY",
            },
            "bonus_shares": {
                "value": 6.8, "unit": "per_10_shares", "currency": None,
            },
            "capitalization_shares": {
                "value": 3.4, "unit": "per_10_shares", "currency": None,
            },
            "rights_shares": None,
            "rights_price": None,
        },
    )
    result["date_facts"] = [
        {
            "date": "2006-06-12",
            "date_type": "record_date",
            "date_basis": "股权登记日",
            "evidence_ids": ["ev-1"],
        },
        {
            "date": "2006-06-14",
            "date_type": "listing_date",
            "date_basis": "上市日",
            "evidence_ids": ["ev-1"],
        },
        {
            "date": "2006-06-14",
            "date_type": "resumption_date",
            "date_basis": "复牌日",
            "evidence_ids": ["ev-1"],
        },
    ]
    result["economic_primitives"] = [
        {
            "fact_id": "bonus-total",
            "fact_type": "bonus_share_total",
            "value": 33574.8504,
            "unit": "10k_shares",
            "beneficiary_scope": "circulating_shareholders",
            "evidence_ids": ["ev-1"],
        },
        {
            "fact_id": "bonus-ratio",
            "fact_type": "bonus_ratio",
            "value": 6.8,
            "unit": "per_10_shares",
            "beneficiary_scope": "circulating_shareholders",
            "evidence_ids": ["ev-1"],
        },
        {
            "fact_id": "capitalization-ratio",
            "fact_type": "capitalization_ratio",
            "value": 3.4,
            "unit": "per_10_shares",
            "beneficiary_scope": "circulating_shareholders",
            "evidence_ids": ["ev-1"],
        },
        {
            "fact_id": "cash-total",
            "fact_type": "cash_total",
            "value": 1768.1397,
            "unit": "10k_CNY",
            "beneficiary_scope": "circulating_shareholders",
            "evidence_ids": ["ev-1"],
        },
    ]
    return result


def _share_reform_v3_result(page, *, include_verification=True):
    result = _share_reform_v2_result(page)
    result["schema_version"] = SCHEMA_VERSION
    date_bindings = {
        "record_date": ("股权登记日", "2006年6月12日"),
        "listing_date": ("上市日", "2006年6月14日"),
        "resumption_date": ("复牌日", "2006年6月14日"),
    }
    for index, fact in enumerate(result["date_facts"], start=1):
        role_text, date_text = date_bindings[fact["date_type"]]
        fact.update({
            "fact_id": f"date-{index}",
            "semantic_evidence": [{
                "evidence_id": "ev-1",
                "role_text": role_text,
                "date_text": date_text,
            }],
        })
    economic_bindings = {
        "bonus-total": {
            "relation_text": "送股总数", "value_text": "33,574.8504",
            "unit_text": "万股", "basis_text": None,
        },
        "bonus-ratio": {
            "relation_text": "送", "value_text": "6.8",
            "unit_text": "股", "basis_text": "每10股",
        },
        "capitalization-ratio": {
            "relation_text": "转增", "value_text": "3.4",
            "unit_text": "股", "basis_text": "每10股",
        },
        "cash-total": {
            "relation_text": "并派现金", "value_text": "1,768.1397",
            "unit_text": "万元", "basis_text": None,
        },
    }
    for primitive in result["economic_primitives"]:
        primitive["semantic_evidence"] = [{
            "evidence_id": "ev-1",
            "subject_text": "流通股股东",
            **economic_bindings[primitive["fact_id"]],
        }]
    if include_verification:
        verification = _semantic_verification(result)
        result["semantic_event_verification"] = {
            key: verification[key]
            for key in (
                "schema_version", "instrument_id", "source_event_key",
                "event_claim_hash",
                "event_match_supported", "event_type_supported",
                "event_stage_supported", "unresolved_language",
            )
        }
        result["semantic_verifications"] = verification["decisions"]
        result["semantic_verifier_conflicts"] = verification["conflicts"]
    return result


def test_v2_same_day_roles_and_chained_cash_derivation():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日为2006年6月14日，复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股，"
        "并派现金1,768.1397万元（含税）。"
    )
    status, gates, normalized = validate_analysis(
        _share_reform_v2_result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["effective_date"] == "2006-06-14"
    assert normalized["effective_date_type"] == "resumption_date"
    assert {item["date_type"] for item in normalized["alternative_dates"]} == {
        "record_date", "listing_date",
    }
    cash_derivation = next(
        item for item in normalized["economic_derivations"]
        if item["output_field"] == "cash_dividend"
    )
    assert cash_derivation["formula_id"] == "cash_total_over_derived_base_v1"
    assert float(cash_derivation["output_value"]) == pytest.approx(0.03581058)


def test_v3_share_reform_cash_derivation_does_not_require_action_keywords():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日为2006年6月14日，复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股，"
        "并派现金1,768.1397万元（含税）。"
    )
    status, gates, normalized = validate_analysis(
        _share_reform_v3_result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["effective_date"] == "2006-06-14"
    assert normalized["economic_terms"]["cash_dividend"] == {
        "value": pytest.approx(0.3581058),
        "unit": "per_10_shares",
        "currency": "CNY",
    }
    cash_derivation = next(
        item for item in normalized["economic_derivations"]
        if item["output_field"] == "cash_dividend"
    )
    assert float(cash_derivation["output_value"]) == pytest.approx(
        0.03581058386488
    )


def test_v3_currency_unit_accepts_bounded_tax_qualifier_only():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日为2006年6月14日，复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股，"
        "并派现金1,768.1397万元（含税）。"
    )
    result = _share_reform_v3_result(page, include_verification=False)

    def attach_semantic_verification():
        verification = _semantic_verification(result)
        result["semantic_event_verification"] = {
            key: verification[key]
            for key in (
                "schema_version", "instrument_id", "source_event_key",
                "event_claim_hash", "event_match_supported",
                "event_type_supported", "event_stage_supported",
                "unresolved_language",
            )
        }
        result["semantic_verifications"] = verification["decisions"]
        result["semantic_verifier_conflicts"] = verification["conflicts"]

    cash_binding = next(
        primitive["semantic_evidence"][0]
        for primitive in result["economic_primitives"]
        if primitive["fact_id"] == "cash-total"
    )
    cash_binding["unit_text"] = "万元（含税）"
    attach_semantic_verification()

    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
    )
    assert status == "validated_candidate"
    assert all(gates.values())

    cash_binding["unit_text"] = "万元（折合美元）"
    page = _page(page.text.replace("万元（含税）", "万元（折合美元）"))
    result["evidence"][0]["exact_quote"] = page.text
    attach_semantic_verification()
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
    )
    assert status == "manual_required"
    assert gates["economic_primitives_in_evidence"] is False


def test_v2_unsupported_date_role_cannot_be_selected():
    page = _page(
        "向全体股东每10股派2.36元，除权除息日为2026年6月12日。"
    )
    result = _v2_result(page)
    result["date_facts"].append({
        "date": "2026-06-13",
        "date_type": "listing_date",
        "date_basis": "上市日",
        "evidence_ids": ["ev-1"],
    })
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["date_facts_in_evidence"] is False
    assert normalized["effective_date"] == "2026-06-12"


def test_v2_date_role_must_be_bound_to_its_own_date():
    page = _page(
        "股权登记日为2006年6月12日，复牌日为2006年6月14日。"
        "向全体股东每10股派2.36元。"
    )
    result = _v2_result(
        page,
        event_type="share_reform",
        effective_date="2006-06-12",
        effective_date_type="resumption_date",
        date_basis="复牌日",
    )
    result["date_facts"] = [{
        "date": "2006-06-12",
        "date_type": "resumption_date",
        "date_basis": "复牌日",
        "evidence_ids": ["ev-1"],
    }]
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "manual_required"
    assert gates["date_facts_in_evidence"] is False
    assert normalized["effective_date"] is None


def test_v2_duplicate_date_facts_merge_official_evidence_ids():
    page = _page()
    result = _v2_result(page)
    second_evidence = deepcopy(result["evidence"][0])
    second_evidence["evidence_id"] = "ev-2"
    result["evidence"].append(second_evidence)
    result["date_facts"].append({
        "date": "2026-06-12",
        "date_type": "ex_dividend_date",
        "date_basis": "除权除息日",
        "evidence_ids": ["ev-2"],
    })
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["date_facts"] == [{
        "date": "2026-06-12",
        "date_type": "ex_dividend_date",
        "date_basis": "除权除息日",
        "evidence_ids": ["ev-1", "ev-2"],
    }]


def test_v2_same_role_date_conflict_leaves_no_canonical_date():
    page = _page(
        "向全体股东每10股派2.36元，除权除息日为2026年6月12日；"
        "更正后的除权除息日为2026年6月13日。"
    )
    result = _v2_result(page)
    result["date_facts"].append({
        "date": "2026-06-13",
        "date_type": "ex_dividend_date",
        "date_basis": "更正后的除权除息日",
        "evidence_ids": ["ev-1"],
    })
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["no_conflict"] is False
    assert normalized["effective_date"] is None
    assert normalized["_date_fact_conflicts"]


def test_v2_scope_mismatch_blocks_total_derivation():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日及复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股；"
        "向全体股东支付现金对价总额1,768.1397万元。"
    )
    result = _share_reform_v2_result(page)
    result["economic_primitives"][-1]["beneficiary_scope"] = "all_shareholders"
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "manual_required"
    assert gates["economic_terms_in_evidence"] is False
    assert not any(
        item["output_field"] == "cash_dividend"
        for item in normalized["economic_derivations"]
    )


def test_v2_primitive_scope_cannot_be_borrowed_from_another_clause():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日及复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股；"
        "向全体股东支付现金对价总额1,768.1397万元。"
    )
    result = _share_reform_v2_result(page)
    result["economic_primitives"][0]["beneficiary_scope"] = "all_shareholders"
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "manual_required"
    assert gates["economic_primitives_in_evidence"] is False


def test_v2_primitive_value_must_match_its_fact_type_and_unit():
    page = _page(
        "向全体股东每10股送6.8股，送股总数33,574万股，"
        "除权除息日为2026年6月12日。"
    )
    result = _v2_result(
        page,
        event_type="bonus_issue",
        economic_terms={
            "cash_dividend": None,
            "bonus_shares": {
                "value": 6.8, "unit": "per_10_shares", "currency": None,
            },
            "capitalization_shares": None,
            "rights_shares": None,
            "rights_price": None,
        },
    )
    result["economic_primitives"] = [
        {
            "fact_id": "wrong-total",
            "fact_type": "bonus_share_total",
            "value": 6.8,
            "unit": "shares",
            "beneficiary_scope": "all_shareholders",
            "evidence_ids": ["ev-1"],
        },
        {
            "fact_id": "bonus-ratio",
            "fact_type": "bonus_ratio",
            "value": 6.8,
            "unit": "per_10_shares",
            "beneficiary_scope": "all_shareholders",
            "evidence_ids": ["ev-1"],
        },
    ]
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="bonus",
    )
    assert status == "manual_required"
    assert gates["economic_primitives_in_evidence"] is False

    cash_page = _page()
    invalid_unit = _v2_result(cash_page)
    invalid_unit["economic_primitives"][0]["unit"] = "CNY"
    status, gates, _ = validate_analysis(
        invalid_unit,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[cash_page],
    )
    assert status == "manual_required"
    assert gates["economic_primitives_in_evidence"] is False


def test_v2_derivation_catalog_is_bounded_and_fails_closed():
    primitives = []
    for index in range(8):
        primitives.append({
            "fact_id": f"total-{index}",
            "fact_type": "bonus_share_total",
            "beneficiary_scope": "all_shareholders",
            "evidence_ids": ["ev-1"],
            "_normalized_value": Decimal(index + 1) * 100,
            "_normalized_unit": "shares",
        })
        primitives.append({
            "fact_id": f"ratio-{index}",
            "fact_type": "bonus_ratio",
            "beneficiary_scope": "all_shareholders",
            "evidence_ids": ["ev-1"],
            "_normalized_value": Decimal(index + 1) / 10,
            "_normalized_unit": "per_share",
        })
    derivations, resolved, conflicts = _derive_economic_terms(primitives)
    assert len(derivations) == MAX_ECONOMIC_DERIVATIONS
    assert resolved == {}
    assert any("exceeded the bounded limit" in item for item in conflicts)


def test_v2_model_arithmetic_mismatch_and_formula_conflict_fail_closed():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日及复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股，"
        "现金对价总额1,768.1397万元；另向流通股股东每10股派0.5元。"
    )
    result = _share_reform_v2_result(page)
    result["economic_primitives"].append({
        "fact_id": "cash-ratio-conflict",
        "fact_type": "cash_ratio",
        "value": 0.5,
        "unit": "CNY_per_10_shares",
        "beneficiary_scope": "circulating_shareholders",
        "evidence_ids": ["ev-1"],
    })
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "manual_required"
    assert gates["no_conflict"] is False
    assert gates["economic_terms_in_evidence"] is False
    assert normalized["economic_derivation_conflicts"]


def test_v2_model_arithmetic_mismatch_fails_without_formula_conflict():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日及复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股，"
        "现金对价总额1,768.1397万元。"
    )
    result = _share_reform_v2_result(page)
    result["economic_terms"]["cash_dividend"]["value"] = 0.5
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "manual_required"
    assert gates["no_conflict"] is True
    assert gates["economic_terms_in_evidence"] is False
    assert normalized["economic_derivation_conflicts"] == []


def test_safe_unit_and_currency_aliases_are_normalized_without_fact_changes():
    page = _page()
    result = _result(page)
    result["economic_terms"]["cash_dividend"] = {
        "value": 2.36,
        "unit": "每10股",
        "currency": "人民币",
    }
    normalized = normalize_analysis_result(result)
    assert normalized["economic_terms"]["cash_dividend"] == {
        "value": 2.36,
        "unit": "per_10_shares",
        "currency": "CNY",
    }
    assert normalized["effective_date"] == result["effective_date"]
    assert normalized["evidence"] == result["evidence"]


def test_share_reform_mixed_distribution_accepts_official_implementation_evidence():
    page = _page(
        "股权分置改革方案实施公告：复牌日为2006年6月14日，"
        "流通股股东每10股送6.8股转增3.4股派0.3581058元。"
    )
    result = _result(
        page,
        effective_date="2006-06-14",
        effective_date_type="resumption_date",
        event_type="share_reform",
        economic_terms={
            "cash_dividend": {
                "value": 0.3581058,
                "unit": "per_10_shares",
                "currency": "CNY",
            },
            "bonus_shares": {
                "value": 6.8, "unit": "per_10_shares", "currency": None,
            },
            "capitalization_shares": {
                "value": 3.4, "unit": "per_10_shares", "currency": None,
            },
            "rights_shares": None,
            "rights_price": None,
        },
        alternative_dates=[{
            "date": "2006-06-12",
            "date_type": "record_date",
            "date_basis": "official_announcement_explicit_statement",
            "reason": "股权登记日",
        }],
    )
    result["evidence"][0]["supports_fields"] = [
        "effective_date",
        "event_type",
        "cash_dividend",
        "bonus_shares",
        "capitalization_shares",
    ]
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "validated_candidate"
    assert gates["event_type_compatible"] is True
    assert gates["no_conflict"] is True
    assert normalized["_review_classification"]["review_tier"] == "quick_review"


def test_share_reform_pdf_spacing_and_same_day_roles_validate_candidate():
    page = _page(
        "股权分置改革方案实施公告。股权登记日为2006年6月1 2日，"
        "对价股份上市日为2006年6月1 4日，复牌日为2006年6月1 4日。"
        "流通股股东每10股定向送红股6.8股，实际可获得转增股份3.4股。"
    )
    result = _result(
        page,
        effective_date="2006-06-14",
        effective_date_type="listing_date",
        date_basis="对价股份上市日",
        event_type="share_reform",
        economic_terms={
            "cash_dividend": None,
            "bonus_shares": {
                "value": 6.8, "unit": "per_10_shares", "currency": None,
            },
            "capitalization_shares": {
                "value": 3.4, "unit": "per_10_shares", "currency": None,
            },
            "rights_shares": None,
            "rights_price": None,
        },
        alternative_dates=[
            {
                "date": "2006-06-12",
                "date_type": "record_date",
                "date_basis": "股权登记日",
                "reason": "公告明确披露",
            },
            {
                "date": "2006-06-14",
                "date_type": "resumption_date",
                "date_basis": "复牌日",
                "reason": "公告明确披露",
            },
        ],
        conflicts=[],
    )
    result["evidence"][0]["supports_fields"] = [
        "effective_date",
        "effective_date_type",
        "date_basis",
        "event_type",
        "bonus_shares",
        "capitalization_shares",
    ]
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "validated_candidate"
    assert all(gates.values())
    assert normalized["effective_date_type"] == "resumption_date"
    assert normalized["date_basis"] == "复牌日"
    assert normalized["conflicts"] == []


@pytest.mark.parametrize("conflict", [
    "Page 1 and page 3 state different ex-dividend dates.",
    "Page 1 and page 3 state different stock short-name change dates.",
])
def test_any_reported_conflict_requires_deep_review(conflict):
    page = _page()
    result = _result(
        page,
        conflicts=[conflict],
    )
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["no_conflict"] is False
    assert normalized["_review_classification"]["review_tier"] == "deep_review"


def test_same_day_role_normalization_requires_role_evidence():
    page = _page(
        "股权分置改革方案实施公告。对价股份上市日为2006年6月14日。"
    )
    result = _result(
        page,
        effective_date="2006-06-14",
        effective_date_type="listing_date",
        event_type="share_reform",
        alternative_dates=[{
            "date": "2006-06-14",
            "date_type": "resumption_date",
            "date_basis": "复牌日",
            "reason": "模型推测",
        }],
    )
    result["evidence"][0]["supports_fields"] = [
        "effective_date", "effective_date_type", "date_basis", "event_type",
        "cash_dividend",
    ]
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "manual_required"
    assert gates["effective_date_type_compatible"] is False
    assert normalized["effective_date_type"] == "listing_date"


def test_date_evidence_does_not_join_across_line_breaks():
    page = _page("除权除息日为2026年6月1\n4日后办理其他事项。每10股派2.36元。")
    status, gates, _ = validate_analysis(
        _result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["date_in_evidence"] is False


def test_ordinary_distribution_cannot_be_mislabeled_as_share_reform():
    page = _page()
    status, gates, normalized = validate_analysis(
        _result(page, event_type="share_reform"),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["年度权益分派实施公告"],
    )
    assert status == "manual_required"
    assert gates["event_type_compatible"] is False
    assert normalized["_review_classification"]["review_tier"] == "machine_rework"


def test_different_related_date_roles_are_not_conflicts():
    page = _page()
    result = _result(page, alternative_dates=[
        {
            "date": "2026-06-11",
            "date_type": "record_date",
            "date_basis": "official_announcement_explicit_statement",
            "reason": "股权登记日",
        },
        {
            "date": "2026-06-13",
            "date_type": "payment_date",
            "date_basis": "official_announcement_explicit_statement",
            "reason": "现金红利发放日",
        },
    ])
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "validated_candidate"
    assert gates["no_conflict"] is True


def test_same_semantic_date_role_conflict_requires_deep_review():
    page = _page()
    result = _result(page, alternative_dates=[{
        "date": "2026-06-13",
        "date_type": "ex_date",
        "date_basis": "official_announcement_explicit_statement",
        "reason": "另一处除权日",
    }])
    status, gates, normalized = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["no_conflict"] is False
    assert normalized["_review_classification"]["review_tier"] == "deep_review"


def test_date_missing_from_exact_quote_remains_deep_review():
    page = _page("本次权益分派每10股派2.36元。")
    status, gates, normalized = validate_analysis(
        _result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["date_in_evidence"] is False
    assert normalized["_review_classification"]["review_tier"] == "deep_review"


def test_approved_stage_and_out_of_window_date_require_deep_review():
    page = _page()
    status, gates, normalized = validate_analysis(
        _result(page, event_stage="approved"),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert normalized["_review_classification"]["review_tier"] == "deep_review"

    status, gates, normalized = validate_analysis(
        _result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        allowed_start=date(2027, 1, 1),
        allowed_end=date(2027, 12, 31),
    )
    assert status == "manual_required"
    assert gates["date_range"] is False
    assert normalized["_review_classification"]["review_tier"] == "deep_review"


def test_economic_term_requires_supported_numeric_official_quote():
    page = _page("本次权益分派除权除息日为2026年6月12日。")
    status, gates, _ = validate_analysis(
        _result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    assert status == "manual_required"
    assert gates["economic_terms_in_evidence"] is False


def test_economic_term_requires_matching_supports_field():
    page = _page()
    result = _result(page)
    result["evidence"][0]["supports_fields"] = ["effective_date"]
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "manual_required"
    assert gates["economic_terms_in_evidence"] is False


def test_per_share_value_can_match_explicit_per_ten_share_quote():
    page = _page()
    result = _result(page)
    result["economic_terms"]["cash_dividend"] = {
        "value": 0.236,
        "unit": "per_share",
        "currency": "CNY",
    }
    status, gates, _ = validate_analysis(
        result,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
    )
    assert status == "validated_candidate"
    assert gates["economic_terms_in_evidence"] is True


def test_incompatible_event_type_fails_closed():
    page = _page()
    status, gates, _ = validate_analysis(
        _result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_allotment",
        action_type="rights",
    )
    assert status == "manual_required"
    assert gates["event_type_compatible"] is False


def test_record_date_cannot_be_used_as_standard_dividend_factor_date():
    page = _page()
    status, gates, _ = validate_analysis(
        _result(page, effective_date_type="record_date"),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    assert status == "manual_required"
    assert gates["effective_date_type_compatible"] is False


def test_event_prompt_has_one_global_page_budget():
    resolver = CninfoCorporateActionLlmResolver(
        SimpleNamespace(complete=AsyncMock())
    )
    pages = [
        CorporateActionPageText(
            page_number=index,
            text=f"第{index}页权益分派内容",
            text_hash=str(index),
            announcement_id=f"ann-{index}",
        )
        for index in range(1, MAX_EVENT_PAGES + 3)
    ]
    payload = resolver.build_payload(
        {"instrument_id": "000001.SZ", "source_event_key": "event-1"},
        pages,
    )
    assert len(payload["pages"]) == MAX_EVENT_PAGES
    assert payload["context_window"]["context_complete"] is False
    assert len(payload["context_window"]["omitted_sections"]) == 2
    assert (
        len(json.dumps(payload, ensure_ascii=False, default=str))
        <= MAX_EVENT_PROMPT_CHARACTERS
    )


@pytest.mark.asyncio
async def test_truncated_event_context_cannot_be_validated():
    first_page = _page()
    pages = [first_page] + [
        CorporateActionPageText(
            page_number=index,
            text=f"第{index}页权益分派内容",
            text_hash=str(index),
            announcement_id=f"ann-{index}",
        )
        for index in range(2, MAX_EVENT_PAGES + 3)
    ]
    extraction = _v3_result(first_page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))
    analysis = await CninfoCorporateActionLlmResolver(client).analyze(
        event={"instrument_id": "000001.SZ", "source_event_key": "event-1"},
        pages=pages,
    )
    assert analysis.validation_status == "manual_required"
    assert analysis.gate_results["context_complete"] is False
    assert analysis.result["_input_context"]["omitted_sections"]


@pytest.mark.asyncio
async def test_resolver_uses_common_gateway_and_untrusted_content_guard():
    page = _page()
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))
    analysis = await CninfoCorporateActionLlmResolver(
        client,
        requests_per_minute=12,
    ).analyze(
        event={"instrument_id": "000001.SZ", "source_event_key": "event-1"},
        pages=[page],
    )
    extraction_request = client.complete.await_args_list[0].args[0]
    verification_request = client.complete.await_args_list[1].args[0]
    assert extraction_request.content_is_untrusted is True
    assert extraction_request.schema_version == SCHEMA_VERSION
    assert extraction_request.max_output_tokens == MAX_ANALYSIS_OUTPUT_TOKENS
    assert extraction_request.requests_per_minute == 12
    assert extraction_request.rate_limit_scope == "cninfo_corporate_action_resolution"
    assert verification_request.content_is_untrusted is True
    assert verification_request.schema_version == SEMANTIC_VERIFICATION_SCHEMA_VERSION
    assert verification_request.requests_per_minute == 12
    assert verification_request.rate_limit_scope == "cninfo_corporate_action_resolution"
    assert analysis.validation_status == "validated_candidate"
    assert analysis.latency_ms == 15
    assert analysis.attempt_count == 2


@pytest.mark.asyncio
async def test_semantic_verifier_failure_retains_extraction_for_manual_review():
    page = _page()
    extraction = _v3_result(page, include_verification=False)
    error = LlmDeadlineExceededError().with_context(
        request_id="verify-request", attempt_count=2,
    )
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        error,
    ]))
    analysis = await CninfoCorporateActionLlmResolver(client).analyze(
        event={"instrument_id": "000001.SZ", "source_event_key": "event-1"},
        pages=[page],
    )
    assert analysis.validation_status == "manual_required"
    assert analysis.gate_results["semantic_verification_complete"] is False
    assert analysis.result["_semantic_verifier"]["error_code"] == "deadline_exceeded"
    assert analysis.result["economic_primitives"][0]["fact_id"] == "cash-ratio"
    assert "semantic_verifier_deadline_exceeded" in analysis.warnings
    assert analysis.attempt_count == 3


def test_document_service_rejects_non_pdf_and_page_selection_is_bounded(tmp_path):
    service = CninfoCorporateActionDocumentService(
        archive_root=tmp_path,
        fetcher=lambda _url: b"not a pdf",
    )
    with pytest.raises(ValueError, match="unsupported_document_signature"):
        service.ingest(announcement_id="ann-1", source_url="https://example.test/a.pdf")
    pages = [
        CorporateActionPageText(
            index,
            "普通正文" if index != 3 else "除权除息日为2026年6月12日",
            str(index),
        )
        for index in range(1, 8)
    ]
    assert [item.page_number for item in select_relevant_pages(pages, max_pages=3)] == [2, 3, 4]
    assert page_text_cache_key("artifact-hash", pages[0]) == page_text_cache_key(
        "artifact-hash", pages[0]
    )


def test_document_service_archives_and_extracts_historical_html(tmp_path):
    html = """
    <html><head><style>hidden</style></head><body>
    <h1>1992年度分红派息公告</h1>
    <p>股权登记日为1993年5月20日，除权除息日为1993年5月21日。</p>
    <script>ignored()</script>
    </body></html>
    """.encode("gb18030")
    service = CninfoCorporateActionDocumentService(
        archive_root=tmp_path,
        fetcher=lambda _url: html,
    )
    bundle = service.ingest(
        announcement_id="12598339",
        source_url="https://static.cninfo.com.cn/finalpage/1993-05-15/12598339.html",
    )
    assert bundle.content_type == "text/html"
    assert bundle.archive_path.endswith(".html")
    assert bundle.pages[0].extraction_method == "html_text"
    assert "除权除息日为1993年5月21日" in bundle.pages[0].text
    assert "ignored" not in bundle.pages[0].text
    assert extract_html_pages(html)[0].text_hash == bundle.pages[0].text_hash


def test_document_service_uses_common_attachment_retriever(tmp_path, monkeypatch):
    class _Retriever:
        def __init__(self):
            self.calls = []

        def retrieve(self, source, attachment, *, require_pdf=False):
            self.calls.append((source, attachment, require_pdf))
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="success",
                content=b"%PDF-common",
                content_hash="retrieval-hash",
                content_length=11,
                final_url="https://static.cninfo.com.cn/finalpage/ann-1.pdf",
                response_media_type="application/pdf",
            )

    retriever = _Retriever()
    monkeypatch.setattr(
        "data_sources.cninfo_corporate_action_documents.extract_pdf_pages",
        lambda _content, **_kwargs: (_page(),),
    )
    service = CninfoCorporateActionDocumentService(
        archive_root=tmp_path,
        retriever=retriever,
    )

    artifact = service.retrieve_and_archive(
        announcement_id="ann-1",
        source_url="finalpage/ann-1.pdf",
    )
    bundle = service.parse_artifact(artifact)

    assert not hasattr(artifact, "content")
    assert artifact.content_hash == bundle.content_hash
    assert bundle.source == "cninfo"
    assert bundle.source_url == "https://static.cninfo.com.cn/finalpage/ann-1.pdf"
    assert retriever.calls[0][0] == "cninfo"
    assert retriever.calls[0][2] is False


@pytest.mark.asyncio
async def test_data_manager_dry_run_never_persists_documents_or_analysis(monkeypatch):
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ", "source_event_key": "event-1",
        "source_profile": "cninfo_dividend", "action_type": "dividend",
        "announcement_date": date(2026, 6, 1), "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1", "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1), "evidence_url": "https://example.test/a.pdf",
    }])
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value={"items": []}
    )
    manager.db_ops.save_corporate_action_document_bundle = AsyncMock()
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock()
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": []}
    )
    bundle = CorporateActionDocumentBundle(
        "ann-1", "https://example.test/a.pdf", "hash", "application/pdf", 10,
        "ann-1/hash.pdf", (page,), "extracted",
    )
    monkeypatch.setattr(CninfoCorporateActionDocumentService, "ingest", lambda self, **kwargs: bundle)
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(
            extraction,
            suffix="extract",
            usage=SimpleNamespace(
                input_tokens=100, output_tokens=50, total_tokens=150,
            ),
            warnings=("provider_output_budget_exceeded",),
        ),
        _gateway_response(
            _semantic_verification(extraction),
            suffix="verify",
            latency_ms=5,
            usage=SimpleNamespace(
                input_tokens=20, output_tokens=10, total_tokens=30,
            ),
        ),
    ]))
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01", end_date="2026-12-31",
        exchanges=["SZSE"], instrument_ids=["000001.SZ"], max_events=1,
        dry_run=True, llm_client=client,
    )
    assert result["status"] == "dry_run"
    assert result["counts"]["validated_candidates"] == 1
    assert result["auto_promotion"]["eligible"] == 1
    assert result["auto_promotion"]["dry_run_eligible"] == 1
    assert result["review_workload"]["tiers"]["auto_eligible"] == 1
    assert result["review_workload"]["tiers"]["quick_review"] == 0
    assert result["review_workload"]["remaining_manual_review"] == 0
    assert result["resolved_layer_write_allowed"] is False
    assert result["raw_observation_modified"] is False
    assert result["production_factor_modified"] is False
    assert result["llm_metrics"]["total_tokens"] == 180
    assert result["llm_metrics"]["provider_output_budget_overruns"] == 1
    assert result["llm_metrics"]["latency_ms"]["p95"] == 15
    candidate_query = manager.db_ops.execute_read_query.await_args.args[0]
    assert "corporate_action_resolution_states" in candidate_query
    assert "s.resolution_state IN" in candidate_query
    assert "s.is_terminal = 1" not in candidate_query
    manager.db_ops.save_corporate_action_document_bundle.assert_not_awaited()
    manager.db_ops.save_corporate_action_llm_analysis.assert_not_awaited()


@pytest.mark.asyncio
async def test_data_manager_async_pipeline_preserves_dry_run_business_result():
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }])
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.save_corporate_action_document_bundle = AsyncMock()
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock()
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": []}
    )
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        dry_run=True,
        download_documents=False,
        llm_client=client,
        pipeline={
            "mode": "async",
            "download_concurrency": 2,
            "document_parse_concurrency": 2,
            "llm_concurrency": 2,
            "progress_interval_seconds": 60,
        },
    )

    assert result["status"] == "dry_run"
    assert result["counts"]["analyzed"] == 1
    assert result["counts"]["validated_candidates"] == 1
    assert result["auto_promotion"]["dry_run_eligible"] == 1
    assert result["pipeline_runtime"]["mode"] == "async"
    assert result["pipeline_runtime"]["submitted"] == 1
    assert {
        item["stage"] for item in result["pipeline_runtime"]["stage_snapshots"]
    } == {
        "cninfo_document_preparation",
        "cninfo_semantic_resolution",
        "cninfo_serial_persistence",
    }
    manager.db_ops.save_corporate_action_document_bundle.assert_not_awaited()
    manager.db_ops.save_corporate_action_llm_analysis.assert_not_awaited()


@pytest.mark.asyncio
async def test_data_manager_async_pipeline_serializes_current_analysis_write():
    page = _page()
    candidate_row = {
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[candidate_row])
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        return_value={"analysis_id": 7, "status": "inserted"}
    )
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=False,
        dry_run=False,
        download_documents=False,
        auto_promote_validated=False,
        llm_client=client,
        pipeline={
            "mode": "async",
            "download_concurrency": 2,
            "document_parse_concurrency": 2,
            "llm_concurrency": 2,
            "progress_interval_seconds": 60,
        },
    )

    assert result["counts"]["persisted_analyses"] == 1
    assert result["pipeline_runtime"]["stage_snapshots"][-1]["active"] == 0
    manager.db_ops.save_corporate_action_llm_analysis.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_resume_counts_revalidated_analysis_persistence(monkeypatch):
    page, _, gates, normalized = _eligible_auto_promotion_case()
    candidate_row = _candidate_observation_row()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(
        side_effect=[
            [candidate_row],
            [candidate_row],
            [{"announcement_id": "ann-1"}],
        ]
    )
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{
            "analysis_id": 7,
            "analysis_key": "existing-analysis-key",
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "input_hash": "legacy-input-hash",
            "validation_status": "manual_required",
            "schema_version": SCHEMA_VERSION,
            "parser_version": "cninfo_corporate_action_resolution_validator.v8",
            "result": normalized,
            "gate_results": gates,
        }]
    })
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        return_value={"analysis_id": 8, "status": "updated"}
    )
    monkeypatch.setattr(
        CninfoCorporateActionLlmResolver,
        "input_hash",
        lambda self, event, pages: "matching-input-hash",
    )
    monkeypatch.setattr(
        CninfoCorporateActionLlmResolver,
        "input_hash_for_parser",
        lambda self, event, pages, *, parser_version: (
            "legacy-input-hash"
            if parser_version
            == "cninfo_corporate_action_resolution_validator.v8"
            else "matching-input-hash"
        ),
    )
    client = SimpleNamespace(complete=AsyncMock())

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=True,
        dry_run=False,
        auto_promote_validated=False,
        llm_client=client,
        pipeline={
            "mode": "async",
            "download_concurrency": 2,
            "document_parse_concurrency": 2,
            "llm_concurrency": 2,
            "progress_interval_seconds": 60,
        },
    )

    assert result["counts"]["resumed"] == 1
    assert result["counts"]["persisted_analyses"] == 1
    client.complete.assert_not_awaited()
    manager.db_ops.save_corporate_action_llm_analysis.assert_awaited_once()
    saved = manager.db_ops.save_corporate_action_llm_analysis.await_args.args[0]
    assert saved["analysis_key"] != "existing-analysis-key"
    assert saved["parser_version"] == PARSER_VERSION
    assert saved["input_hash"] == "matching-input-hash"


@pytest.mark.asyncio
async def test_direct_resolution_rejects_stale_period_candidate_before_llm():
    candidate_row = {
        **_candidate_observation_row(),
        "instrument_id": "000007.SZ",
        "source_event_key": "event-interim",
        "source_profile": "cninfo_dividend",
        "action_type": "mixed_distribution",
        "fiscal_period": "1992半年报",
        "announcement_id": "12598339",
        "announcement_title": "1992年度分红派息公告",
    }
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[candidate_row])
    client = SimpleNamespace(complete=AsyncMock())

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="1990-12-19",
        end_date="2026-07-24",
        exchanges=["SZSE"],
        instrument_ids=["000007.SZ"],
        max_events=1,
        dry_run=True,
        llm_client=client,
    )

    assert result["targets"]["candidate_events"] == 0
    assert result["targets"]["batch_events"] == 0
    assert result["targets"]["candidate_rows_rejected_by_current_policy"] == 1
    assert result["targets"]["candidate_policy_rejection_samples"][0][
        "reason"
    ].startswith("period_mismatch:")
    client.complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_data_manager_async_pipeline_rejects_superseded_event_before_write():
    page = _page()
    candidate_row = {
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(
        side_effect=[[candidate_row], []]
    )
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock()
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=False,
        dry_run=False,
        download_documents=False,
        auto_promote_validated=False,
        llm_client=client,
        pipeline={
            "mode": "async",
            "download_concurrency": 2,
            "document_parse_concurrency": 2,
            "llm_concurrency": 2,
            "progress_interval_seconds": 60,
        },
    )

    assert result["status"] == "partial"
    assert result["counts"]["errors"] == 1
    assert "stale or superseded" in result["errors"][0]["error"]
    manager.db_ops.save_corporate_action_llm_analysis.assert_not_awaited()


@pytest.mark.asyncio
async def test_review_promotes_only_archived_validated_official_evidence():
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{
            "analysis_id": 7,
            "validation_status": "validated_candidate",
            "gate_results": {
                "economic_terms_in_evidence": True,
                "event_type_compatible": True,
                "effective_date_type_compatible": True,
                "analysis_status_compatible": True,
                "context_complete": True,
            },
            "result": _result(page),
        }]
    })
    manager.db_ops.get_corporate_action_observations = AsyncMock(return_value={
        "items": [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
        }]
    })
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(return_value={
        "items": [{"pages": [{
            "page_number": 3, "text": page.text, "text_hash": page.text_hash,
        }]}]
    })
    manager.db_ops.get_corporate_action_effective_date_evidence = AsyncMock(return_value={
        "items": [{"announcement_id": "ann-1", "source_profile": "cninfo_dividend"}]
    })
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(return_value={
        "review": {"review_id": 9, "status": "inserted"},
        "terms_write": {"resolved_terms_id": 3, "status": "inserted"},
        "evidence_write": {
            "inserted": 1, "changed": 0, "unchanged": 0, "failed": 0,
        },
    })
    result = await manager.review_cninfo_corporate_action_resolution({
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "analysis_id": 7,
        "evidence_key": "ann-1",
        "decision": "resolved",
        "effective_date": "2026-06-12",
        "date_basis": "official_announcement_explicit_statement",
        "reviewer": "unit-reviewer",
        "_require_unreviewed_event": True,
    })
    bundle_kwargs = manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    saved = bundle_kwargs["evidence_row"]
    assert result["raw_observation_modified"] is False
    assert saved["evidence_source"] == "cninfo_reviewed_official_document"
    assert saved["resolution_status"] == "resolved"
    terms = bundle_kwargs["terms_row"]
    assert terms["cash_dividend_per_share"] == pytest.approx(0.236)
    assert terms["resolved_fields"] == ["cash_dividend_per_share"]
    assert terms["evidence"]["economic_field_evidence"]["cash_dividend"]
    assert bundle_kwargs["reject_if_prior_event_review"] is True


@pytest.mark.asyncio
async def test_v2_review_revalidates_and_persists_derived_term_lineage():
    page = _page(
        "股权分置改革方案实施公告。流通股股东股权登记日为2006年6月12日，"
        "对价股份上市日为2006年6月14日，复牌日为2006年6月14日。"
        "向流通股股东每10股送6.8股并转增3.4股，送股总数33,574.8504万股，"
        "现金对价总额1,768.1397万元。"
    )
    status, _, normalized = validate_analysis(
        _share_reform_v2_result(page),
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="mixed_distribution",
        candidate_titles=["股权分置改革方案实施公告"],
    )
    assert status == "validated_candidate"
    manager = _manual_review_manager(
        page, normalized, validation_status="validated_candidate"
    )
    manager.db_ops.get_corporate_action_observations.return_value = {
        "items": [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "mixed_distribution",
        }]
    }
    manager.db_ops.get_corporate_action_effective_date_evidence.return_value = {
        "items": [{
            "announcement_id": "ann-1",
            "announcement_title": "股权分置改革方案实施公告",
            "source_profile": "cninfo_dividend",
        }]
    }
    result = await manager.review_cninfo_corporate_action_resolution({
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "analysis_id": 7,
        "evidence_key": "ann-1",
        "decision": "resolved",
        "effective_date": "2006-06-14",
        "date_basis": "复牌日",
        "reviewer": "unit-reviewer",
    })
    assert result["status"] == "success"
    terms = manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs[
        "terms_row"
    ]
    assert terms["cash_dividend_per_share"] == pytest.approx(0.03581058)
    assert terms["bonus_shares_per_share"] == pytest.approx(0.68)
    assert terms["capitalization_shares_per_share"] == pytest.approx(0.34)
    assert terms["evidence"]["economic_field_evidence"]["cash_dividend"]


@pytest.mark.asyncio
async def test_v2_review_derives_canonical_date_after_fact_correction():
    page = _page()
    original = _v2_result(page)
    original["date_facts"] = [{
        "date": "2026-06-12",
        "date_type": "listing_date",
        "date_basis": "上市日",
        "evidence_ids": ["ev-1"],
    }]
    status, _, normalized = validate_analysis(
        original,
        instrument_id="000001.SZ",
        source_event_key="event-1",
        pages=[page],
        source_profile="cninfo_dividend",
        action_type="dividend",
    )
    assert status == "manual_required"
    assert normalized["effective_date"] is None
    manager = _manual_review_manager(
        page, normalized, validation_status="manual_required"
    )
    result = await manager.review_cninfo_corporate_action_resolution({
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "analysis_id": 7,
        "evidence_key": "ann-1",
        "decision": "resolved",
        "reviewer": "unit-reviewer",
        "corrected_result": {
            "date_facts": [{
                "date": "2026-06-12",
                "date_type": "ex_dividend_date",
                "date_basis": "除权除息日",
                "evidence_ids": ["ev-1"],
            }],
        },
    })
    assert result["status"] == "success"
    saved = manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs
    assert saved["evidence_row"]["effective_date"] == "2026-06-12"
    assert saved["evidence_row"]["date_basis"] == "除权除息日"


def _manual_review_manager(page, analysis_result, *, validation_status):
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{
            "analysis_id": 7,
            "validation_status": validation_status,
            "gate_results": {"date_in_evidence": validation_status == "validated_candidate"},
            "schema_version": analysis_result.get("schema_version"),
            "prompt_version": "prompt.v1",
            "parser_version": "parser.v1",
            "input_hash": "a" * 64,
            "response_hash": "b" * 64,
            "result": analysis_result,
        }]
    })
    manager.db_ops.get_corporate_action_observations = AsyncMock(return_value={
        "items": [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "action_type": "dividend",
        }]
    })
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(return_value={
        "items": [{"pages": [{
            "page_number": 3,
            "text": page.text,
            "text_hash": page.text_hash,
            "quality_status": "usable",
        }]}]
    })
    manager.db_ops.get_corporate_action_effective_date_evidence = AsyncMock(
        return_value={"items": [{
            "announcement_id": "ann-1",
            "announcement_title": "权益分派实施公告",
            "source_profile": "cninfo_dividend",
        }]}
    )
    manager.db_ops.save_corporate_action_review_bundle = AsyncMock(return_value={
        "review": {"review_id": 9, "status": "inserted"},
        "terms_write": {"resolved_terms_id": 3, "status": "inserted"},
        "evidence_write": {
            "inserted": 1, "changed": 0, "unchanged": 0, "failed": 0,
        },
    })
    return manager


@pytest.mark.asyncio
async def test_manual_required_correction_reruns_archived_evidence_gates():
    page = _page()
    original = _result(page, effective_date="2026-06-13")
    original["analysis_status"] = "manual_required"
    original["_review_classification"] = {
        "review_tier": "deep_review",
        "gate_signature": "date_in_evidence",
        "review_reasons": ["hard_gate:date_in_evidence"],
    }
    manager = _manual_review_manager(
        page, original, validation_status="manual_required"
    )
    result = await manager.review_cninfo_corporate_action_resolution({
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "analysis_id": 7,
        "evidence_key": "ann-1",
        "decision": "resolved",
        "reviewer": "unit-reviewer",
        "corrected_result": {
            "effective_date": "2026-06-12",
            "date_basis": "official_announcement_explicit_statement",
        },
    })
    review_row = (
        manager.db_ops.save_corporate_action_review_bundle.await_args.kwargs[
            "review_row"
        ]
    )
    lineage = review_row["review_payload"]
    assert result["status"] == "success"
    assert lineage["original_result"]["effective_date"] == "2026-06-13"
    assert lineage["corrected_result"]["effective_date"] == "2026-06-12"
    assert lineage["post_validation_status"] == "validated_candidate"
    assert all(lineage["post_gate_results"].values())
    assert result["raw_observation_modified"] is False


@pytest.mark.asyncio
async def test_manual_correction_rejects_date_unsupported_by_quote():
    page = _page()
    original = _result(page, effective_date="2026-06-13")
    original["analysis_status"] = "manual_required"
    manager = _manual_review_manager(
        page, original, validation_status="manual_required"
    )
    with pytest.raises(ValueError, match="failed evidence gates"):
        await manager.review_cninfo_corporate_action_resolution({
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "analysis_id": 7,
            "evidence_key": "ann-1",
            "decision": "resolved",
            "reviewer": "unit-reviewer",
            "corrected_result": {
                "effective_date": "2026-06-13",
                "date_basis": "official_announcement_explicit_statement",
            },
        })
    manager.db_ops.save_corporate_action_review_bundle.assert_not_awaited()


@pytest.mark.asyncio
async def test_manual_correction_rejects_evidence_from_another_event():
    page = _page()
    original = _result(page)
    manager = _manual_review_manager(
        page, original, validation_status="manual_required"
    )
    unrelated_evidence = deepcopy(original["evidence"])
    unrelated_evidence[0]["announcement_id"] = "ann-2"
    unrelated_evidence[0]["section_id"] = "ann-2:p3"
    with pytest.raises(ValueError, match="not linked to the requested event"):
        await manager.review_cninfo_corporate_action_resolution({
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "analysis_id": 7,
            "evidence_key": "ann-1",
            "decision": "resolved",
            "reviewer": "unit-reviewer",
            "corrected_result": {"evidence": unrelated_evidence},
        })
    manager.db_ops.save_corporate_action_review_bundle.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_review_is_item_isolated_and_rejects_deep_resolution():
    page = _page()
    quick = _result(page)
    quick["_review_classification"] = {
        "review_tier": "quick_review",
        "gate_signature": "all_gates_passed",
        "review_reasons": ["validated_candidate_requires_explicit_review"],
    }
    manager = _manual_review_manager(
        page, quick, validation_status="validated_candidate"
    )
    batch = await manager.review_cninfo_corporate_action_resolutions_batch({
        "reviewer": "batch-reviewer",
        "items": [
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "event-1",
                "analysis_id": 7,
                "evidence_key": "ann-1",
                "decision": "resolved",
            },
            {
                "instrument_id": "000001.SZ",
                "source_event_key": "event-1",
                "analysis_id": 7,
                "evidence_key": "ann-1",
                "decision": "resolved",
                "corrected_result": {"effective_date": "2026-06-13"},
            },
        ],
    })
    assert batch["status"] == "partial"
    assert batch["succeeded"] == 1
    assert batch["failed"] == 1
    assert [item["status"] for item in batch["items"]] == ["success", "failed"]

    deep = deepcopy(quick)
    deep["_review_classification"] = {
        "review_tier": "deep_review",
        "gate_signature": "no_conflict",
        "review_reasons": ["hard_gate:no_conflict"],
    }
    deep_manager = _manual_review_manager(
        page, deep, validation_status="manual_required"
    )
    deep_batch = await deep_manager.review_cninfo_corporate_action_resolutions_batch({
        "reviewer": "batch-reviewer",
        "items": [{
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "analysis_id": 7,
            "evidence_key": "ann-1",
            "decision": "resolved",
        }],
    })
    assert deep_batch["failed"] == 1
    assert "quick_review" in deep_batch["items"][0]["error"]
    deep_manager.db_ops.save_corporate_action_review_bundle.assert_not_awaited()

    with pytest.raises(ValueError, match="at most 100"):
        await manager.review_cninfo_corporate_action_resolutions_batch({
            "reviewer": "batch-reviewer",
            "items": [{} for _ in range(101)],
        })


@pytest.mark.asyncio
async def test_run_ocr_reports_missing_adapter_only_for_scanned_document(
    monkeypatch,
):
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "扫描版权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }])
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value={"items": []}
    )
    monkeypatch.setattr(
        CninfoCorporateActionDocumentService,
        "ingest",
        Mock(side_effect=ValueError("ocr_unavailable")),
    )
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        run_ocr=True,
        llm_client=SimpleNamespace(complete=AsyncMock()),
    )
    assert result["status"] == "partial"
    assert result["errors"][0]["code"] == "ocr_adapter_unconfigured"


@pytest.mark.asyncio
async def test_refresh_documents_rechecks_existing_announcement(monkeypatch):
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }])
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(return_value={
        "items": [{
            "artifact_id": 1,
            "pages": [{
                "page_number": 1,
                "text": "旧版正文",
                "text_hash": "old-hash",
            }],
        }]
    })
    manager.db_ops.save_corporate_action_document_bundle = AsyncMock(
        return_value={"artifact_id": 2, "artifact_status": "inserted"}
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        return_value={"analysis_id": 3, "status": "inserted"}
    )
    bundle = CorporateActionDocumentBundle(
        "ann-1", "https://example.test/a.pdf", "new-hash",
        "application/pdf", 10, "ann-1/new-hash.pdf", (page,), "extracted",
    )
    ingest = Mock(return_value=bundle)
    monkeypatch.setattr(CninfoCorporateActionDocumentService, "ingest", ingest)
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=False,
        dry_run=False,
        refresh_documents=True,
        auto_promote_validated=False,
        llm_client=client,
    )
    assert result["status"] == "partial"
    assert result["review_workload"]["remaining_manual_review"] == 1
    assert ingest.call_count == 1
    manager.db_ops.save_corporate_action_document_bundle.assert_awaited_once()


@pytest.mark.asyncio
async def test_data_manager_reports_specific_llm_error_code(monkeypatch):
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }])
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value={"items": []}
    )
    manager.db_ops.save_corporate_action_document_bundle = AsyncMock(
        return_value={"artifact_id": 1, "artifact_status": "inserted"}
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        return_value={"analysis_id": 1, "status": "inserted"}
    )
    bundle = CorporateActionDocumentBundle(
        "ann-1", "https://example.test/a.pdf", "hash", "application/pdf", 10,
        "ann-1/hash.pdf", (page,), "extracted",
    )
    monkeypatch.setattr(
        CninfoCorporateActionDocumentService,
        "ingest",
        Mock(return_value=bundle),
    )
    error = LlmDeadlineExceededError().with_context(
        request_id="request-1",
        attempt_count=2,
    )
    client = SimpleNamespace(complete=AsyncMock(side_effect=error))
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        resume=False,
        dry_run=False,
        llm_client=client,
    )
    assert result["status"] == "partial"
    assert result["errors"][0]["code"] == "deadline_exceeded"
    assert result["errors"][0]["attempt_count"] == 2
    assert result["review_workload"]["tiers"]["machine_rework"] == 1
    assert result["review_workload"]["gate_signatures"][
        "analysis_error:deadline_exceeded"
    ] == 1
    failure_row = manager.db_ops.save_corporate_action_llm_analysis.await_args.args[0]
    assert failure_row["error_code"] == "deadline_exceeded"
    assert failure_row["attempt_count"] == 2


def _stored_document_bundle(page):
    return {
        "items": [{
            "artifact_id": 1,
            "pages": [{
                "page_number": page.page_number,
                "text": page.text,
                "text_hash": page.text_hash,
                "extraction_method": "native_text",
                "quality_status": "usable",
            }],
        }]
    }


def _candidate_observation_row():
    return {
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "announcement_id": "ann-1",
        "announcement_title": "权益分派实施公告",
        "announcement_time": date(2026, 6, 1),
        "evidence_url": "https://example.test/a.pdf",
    }


@pytest.mark.asyncio
async def test_new_eligible_analysis_is_promoted_through_governed_review():
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(
        return_value=[_candidate_observation_row()]
    )
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        return_value={"analysis_id": 7, "status": "inserted"}
    )
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": []}
    )
    manager.review_cninfo_corporate_action_resolution = AsyncMock(return_value={
        "status": "success",
        "review": {"review_id": 9, "status": "inserted"},
        "raw_observation_modified": False,
        "production_factor_modified": False,
    })
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=False,
        dry_run=False,
        llm_client=client,
    )

    assert result["status"] == "success"
    assert result["auto_promotion"]["eligible"] == 1
    assert result["auto_promotion"]["promoted"] == 1
    assert result["review_workload"]["tiers"]["auto_promoted"] == 1
    assert result["review_workload"]["remaining_manual_review"] == 0
    payload = manager.review_cninfo_corporate_action_resolution.await_args.args[0]
    assert payload["analysis_id"] == 7
    assert payload["decision"] == "resolved"
    assert payload["reviewer"] == "system:cninfo_auto_promotion.v1"
    assert payload["_require_unreviewed_event"] is True


@pytest.mark.asyncio
async def test_resume_promotes_current_analysis_without_another_llm_call(
    monkeypatch,
):
    page, status, gates, normalized = _eligible_auto_promotion_case()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(
        return_value=[_candidate_observation_row()]
    )
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{
            "analysis_id": 7,
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "input_hash": "matching-input-hash",
            "validation_status": status,
            "schema_version": SCHEMA_VERSION,
            "parser_version": PARSER_VERSION,
            "result": normalized,
            "gate_results": gates,
        }]
    })
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": []}
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock()
    manager.review_cninfo_corporate_action_resolution = AsyncMock(return_value={
        "status": "success",
        "review": {"review_id": 9, "status": "inserted"},
    })
    monkeypatch.setattr(
        CninfoCorporateActionLlmResolver,
        "input_hash",
        lambda self, event, pages: "matching-input-hash",
    )
    client = SimpleNamespace(complete=AsyncMock())

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=True,
        dry_run=False,
        llm_client=client,
    )

    assert result["status"] == "success"
    assert result["counts"]["resumed"] == 1
    assert result["counts"]["analyzed"] == 0
    assert result["auto_promotion"]["promoted"] == 1
    client.complete.assert_not_awaited()
    manager.db_ops.save_corporate_action_llm_analysis.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_persistence_failure_does_not_overwrite_prior_analysis(
    monkeypatch,
):
    page, _, gates, normalized = _eligible_auto_promotion_case()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(
        return_value=[_candidate_observation_row()]
    )
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{
            "analysis_id": 7,
            "analysis_key": "existing-analysis-key",
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "input_hash": "matching-input-hash",
            "validation_status": "manual_required",
            "schema_version": SCHEMA_VERSION,
            "parser_version": PARSER_VERSION,
            "result": normalized,
            "gate_results": gates,
        }]
    })
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        side_effect=RuntimeError("database temporarily unavailable")
    )
    monkeypatch.setattr(
        CninfoCorporateActionLlmResolver,
        "input_hash",
        lambda self, event, pages: "matching-input-hash",
    )
    client = SimpleNamespace(complete=AsyncMock())

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=True,
        dry_run=False,
        auto_promote_validated=False,
        llm_client=client,
    )

    assert result["status"] == "partial"
    assert result["counts"]["errors"] == 1
    assert result["errors"][0]["code"] == (
        "revalidated_analysis_persistence_failed"
    )
    client.complete.assert_not_awaited()
    manager.db_ops.save_corporate_action_llm_analysis.assert_awaited_once()
    saved_payload = manager.db_ops.save_corporate_action_llm_analysis.await_args.args[0]
    assert saved_payload["analysis_key"] != "existing-analysis-key"
    assert saved_payload["validation_status"] == "validated_candidate"
    assert saved_payload["result"]


@pytest.mark.asyncio
async def test_prior_review_blocks_auto_promotion():
    page, status, gates, normalized = _eligible_auto_promotion_case()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": [{"review_id": 1, "decision": "resolved"}]}
    )
    manager.review_cninfo_corporate_action_resolution = AsyncMock()

    outcome = await manager._maybe_auto_promote_cninfo_analysis(
        analysis={
            "analysis_id": 7,
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "validation_status": status,
            "schema_version": SCHEMA_VERSION,
            "parser_version": PARSER_VERSION,
            "result": normalized,
            "gate_results": gates,
        },
        pages=[page],
        enabled=True,
        dry_run=False,
    )

    assert outcome["eligible"] is True
    assert outcome["status"] == "skipped"
    assert outcome["reason"] == "prior_event_review_exists"
    manager.review_cninfo_corporate_action_resolution.assert_not_awaited()


@pytest.mark.asyncio
async def test_governed_promotion_failure_is_reported_for_manual_review():
    page = _page()
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(
        return_value=[_candidate_observation_row()]
    )
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(
        return_value=_stored_document_bundle(page)
    )
    manager.db_ops.save_corporate_action_llm_analysis = AsyncMock(
        return_value={"analysis_id": 7, "status": "inserted"}
    )
    manager.db_ops.get_corporate_action_resolution_reviews = AsyncMock(
        return_value={"items": []}
    )
    manager.review_cninfo_corporate_action_resolution = AsyncMock(
        side_effect=ValueError("archived official pages are missing")
    )
    extraction = _v3_result(page, include_verification=False)
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _gateway_response(extraction, suffix="extract"),
        _gateway_response(
            _semantic_verification(extraction), suffix="verify", latency_ms=5,
        ),
    ]))

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=False,
        dry_run=False,
        llm_client=client,
    )

    assert result["status"] == "partial"
    assert result["auto_promotion"]["failed"] == 1
    assert result["auto_promotion"]["reason_counts"] == {
        "governed_review_failed": 1
    }
    assert result["review_workload"]["tiers"]["quick_review"] == 1
    assert result["review_workload"]["remaining_manual_review"] == 1


@pytest.mark.asyncio
async def test_incremental_candidate_query_can_exclude_reviewed_events():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[])

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exclude_reviewed_events=True,
        dry_run=True,
        llm_client=SimpleNamespace(complete=AsyncMock()),
    )

    query = manager.db_ops.execute_read_query.await_args.args[0]
    assert "NOT EXISTS" in query
    assert "corporate_action_resolution_reviews" in query
    assert "s.is_terminal = 1" in query
    assert result["parameters"]["exclude_reviewed_events"] is True
