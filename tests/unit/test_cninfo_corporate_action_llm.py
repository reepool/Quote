import json
from copy import deepcopy
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_corporate_action_documents import (
    CorporateActionDocumentBundle,
    CorporateActionPageText,
    CninfoCorporateActionDocumentService,
    select_relevant_pages,
)
from data_sources.cninfo_corporate_action_llm import (
    ANALYSIS_SCHEMA,
    CninfoCorporateActionLlmResolver,
    MAX_ANALYSIS_OUTPUT_TOKENS,
    MAX_EVENT_PAGES,
    MAX_EVENT_PROMPT_CHARACTERS,
    SCHEMA_VERSION,
    normalize_analysis_result,
    validate_analysis,
)
from utils.llm import LlmDeadlineExceededError


def _page(text="本次权益分派每10股派2.36元，除权除息日为2026年6月12日。"):
    import hashlib

    return CorporateActionPageText(
        page_number=3,
        text=text,
        text_hash=hashlib.sha256(text.encode()).hexdigest(),
        announcement_id="ann-1",
    )


def _result(page, **overrides):
    result = {
        "schema_version": SCHEMA_VERSION,
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
    client = SimpleNamespace(complete=AsyncMock(return_value=SimpleNamespace(
        data=_result(first_page), response_hash="response-hash",
        request_id="request-1", model="fake", latency_ms=10,
        attempt_count=1, usage=None,
    )))
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
    response = SimpleNamespace(
        data=_result(page), response_hash="response-hash", request_id="request-1",
        model="fake", latency_ms=10, attempt_count=1, usage=None,
    )
    client = SimpleNamespace(complete=AsyncMock(return_value=response))
    analysis = await CninfoCorporateActionLlmResolver(client).analyze(
        event={"instrument_id": "000001.SZ", "source_event_key": "event-1"},
        pages=[page],
    )
    request = client.complete.await_args.args[0]
    assert request.content_is_untrusted is True
    assert request.schema_version == SCHEMA_VERSION
    assert request.max_output_tokens == MAX_ANALYSIS_OUTPUT_TOKENS
    assert analysis.validation_status == "validated_candidate"


def test_document_service_rejects_non_pdf_and_page_selection_is_bounded(tmp_path):
    service = CninfoCorporateActionDocumentService(
        archive_root=tmp_path,
        fetcher=lambda _url: b"not a pdf",
    )
    with pytest.raises(ValueError, match="invalid_pdf_signature"):
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
    bundle = CorporateActionDocumentBundle(
        "ann-1", "https://example.test/a.pdf", "hash", "application/pdf", 10,
        "ann-1/hash.pdf", (page,), "extracted",
    )
    monkeypatch.setattr(CninfoCorporateActionDocumentService, "ingest", lambda self, **kwargs: bundle)
    client = SimpleNamespace(complete=AsyncMock(return_value=SimpleNamespace(
        data=_result(page), response_hash="response-hash", request_id="request-1",
        model="fake", latency_ms=10, attempt_count=1,
        usage=SimpleNamespace(input_tokens=100, output_tokens=50, total_tokens=150),
        warnings=("provider_output_budget_exceeded",),
    )))
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01", end_date="2026-12-31",
        exchanges=["SZSE"], instrument_ids=["000001.SZ"], max_events=1,
        dry_run=True, llm_client=client,
    )
    assert result["status"] == "dry_run"
    assert result["counts"]["validated_candidates"] == 1
    assert result["review_workload"]["tiers"]["quick_review"] == 1
    assert result["llm_metrics"]["total_tokens"] == 150
    assert result["llm_metrics"]["provider_output_budget_overruns"] == 1
    assert result["llm_metrics"]["latency_ms"]["p95"] == 10
    manager.db_ops.save_corporate_action_document_bundle.assert_not_awaited()
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


def _manual_review_manager(page, analysis_result, *, validation_status):
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_corporate_action_llm_analyses = AsyncMock(return_value={
        "items": [{
            "analysis_id": 7,
            "validation_status": validation_status,
            "gate_results": {"date_in_evidence": validation_status == "validated_candidate"},
            "schema_version": SCHEMA_VERSION,
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
    client = SimpleNamespace(complete=AsyncMock(return_value=SimpleNamespace(
        data=_result(page), response_hash="response-hash", request_id="request-1",
        model="fake", latency_ms=10, attempt_count=1, usage=None,
    )))
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01",
        end_date="2026-12-31",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        max_events=1,
        resume=False,
        dry_run=False,
        refresh_documents=True,
        llm_client=client,
    )
    assert result["status"] == "success"
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
