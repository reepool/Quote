import json
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
    CninfoCorporateActionLlmResolver,
    MAX_EVENT_PAGES,
    MAX_EVENT_PROMPT_CHARACTERS,
    SCHEMA_VERSION,
    validate_analysis,
)


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
    assert normalized["effective_date"] is None


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
        model="fake", latency_ms=10, attempt_count=1, usage=None,
    )))
    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2026-01-01", end_date="2026-12-31",
        exchanges=["SZSE"], instrument_ids=["000001.SZ"], max_events=1,
        dry_run=True, llm_client=client,
    )
    assert result["status"] == "dry_run"
    assert result["counts"]["validated_candidates"] == 1
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
    manager.db_ops.get_corporate_action_document_bundle = AsyncMock(return_value={
        "items": [{"pages": [{
            "page_number": 3, "text": page.text, "text_hash": page.text_hash,
        }]}]
    })
    manager.db_ops.get_corporate_action_effective_date_evidence = AsyncMock(return_value={
        "items": [{"announcement_id": "ann-1", "source_profile": "cninfo_dividend"}]
    })
    manager.db_ops.save_corporate_action_resolution_review = AsyncMock(
        return_value={"review_id": 9, "status": "inserted"}
    )
    manager.db_ops.save_corporate_action_resolved_terms = AsyncMock(
        return_value={"resolved_terms_id": 3, "status": "inserted"}
    )
    manager.db_ops.save_corporate_action_effective_date_evidence = AsyncMock(
        return_value={"inserted": 1, "changed": 0, "unchanged": 0, "failed": 0}
    )
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
    saved = manager.db_ops.save_corporate_action_effective_date_evidence.await_args.args[0][0]
    assert result["raw_observation_modified"] is False
    assert saved["evidence_source"] == "cninfo_reviewed_official_document"
    assert saved["resolution_status"] == "resolved"
    terms = manager.db_ops.save_corporate_action_resolved_terms.await_args.args[0]
    assert terms["cash_dividend_per_share"] == pytest.approx(0.236)
    assert terms["resolved_fields"] == ["cash_dividend_per_share"]
    assert terms["evidence"]["economic_field_evidence"]["cash_dividend"]


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
