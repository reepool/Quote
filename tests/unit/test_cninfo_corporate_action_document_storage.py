import json
from datetime import date

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database.models import (
    Base,
    CorporateActionDocumentArtifactDB,
    CorporateActionDocumentPageDB,
    CorporateActionEffectiveDateEvidenceDB,
    CorporateActionLlmAnalysisDB,
    CorporateActionObservationDB,
    CorporateActionResolutionReviewDB,
    CorporateActionResolvedTermsDB,
    InstrumentDB,
)
from database.operations import DatabaseOperations


@pytest.mark.asyncio
async def test_document_bundle_is_idempotent_and_event_filter_is_paginated():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionEffectiveDateEvidenceDB.__table__,
                CorporateActionDocumentArtifactDB.__table__,
                CorporateActionDocumentPageDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ", symbol="000001", name="Test",
            exchange="SZSE", type="stock", currency="CNY", is_active=True,
        ))
        await session.commit()
    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    await operations.save_corporate_action_effective_date_evidence([{
        "instrument_id": "000001.SZ", "source_event_key": "event-1",
        "source_profile": "cninfo_dividend", "evidence_source": "cninfo_announcement_metadata",
        "evidence_key": "ann-1", "resolution_status": "candidate",
        "announcement_id": "ann-1", "announcement_time": date(2026, 6, 1),
    }])
    artifact = {
        "announcement_id": "ann-1", "source_url": "https://example.test/ann-1.pdf",
        "content_hash": "a" * 64, "content_type": "application/pdf", "content_length": 10,
        "archive_path": "ann-1/" + "a" * 64 + ".pdf", "parser_version": "unit.v1",
    }
    pages = [{
        "page_number": 1, "text": "除权除息日为2026年6月12日。", "text_hash": "b" * 64,
    }]
    first = await operations.save_corporate_action_document_bundle(artifact, pages)
    second = await operations.save_corporate_action_document_bundle(artifact, pages)
    refreshed_artifact = {
        **artifact,
        "content_hash": "c" * 64,
        "content_length": 12,
        "archive_path": "ann-1/" + "c" * 64 + ".pdf",
    }
    refreshed = await operations.save_corporate_action_document_bundle(
        refreshed_artifact,
        [{
            "page_number": 1,
            "text": "更正版除权除息日为2026年6月13日。",
            "text_hash": "d" * 64,
        }],
    )
    page = await operations.get_corporate_action_document_bundle(
        source_event_key="event-1", limit=10, offset=0,
    )
    assert first["artifact_status"] == "inserted"
    assert second["artifact_status"] == "unchanged"
    assert refreshed["artifact_status"] == "inserted"
    assert page["total"] == 2
    assert page["returned"] == 2
    assert page["items"][0]["pages"][0]["text_hash"] == "b" * 64
    assert page["items"][1]["pages"][0]["text_hash"] == "d" * 64
    await engine.dispose()


@pytest.mark.asyncio
async def test_document_and_analysis_saves_return_ids_with_expiring_sessions():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=True)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionDocumentArtifactDB.__table__,
                CorporateActionDocumentPageDB.__table__,
                CorporateActionLlmAnalysisDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ", symbol="000001", name="Test",
            exchange="SZSE", type="stock", currency="CNY", is_active=True,
        ))
        await session.commit()
    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    artifact = await operations.save_corporate_action_document_bundle(
        {
            "announcement_id": "ann-1",
            "source_url": "https://example.test/ann-1.pdf",
            "content_hash": "a" * 64,
            "content_type": "application/pdf",
            "content_length": 10,
            "archive_path": "ann-1/a.pdf",
            "parser_version": "unit.v1",
        },
        [{
            "page_number": 1,
            "text": "除权除息日为2026年6月12日。",
            "text_hash": "b" * 64,
        }],
    )
    analysis = await operations.save_corporate_action_llm_analysis({
        "analysis_key": "analysis-key",
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "analysis_status": "resolved_candidate",
        "validation_status": "validated_candidate",
        "profile": "semantic_extraction",
        "schema_version": "v1",
        "prompt_version": "p1",
        "parser_version": "parser-v1",
        "input_hash": "c" * 64,
        "artifact_ids": [artifact["artifact_id"]],
        "result": {"effective_date": "2026-06-12"},
        "gate_results": {"date_in_evidence": True},
    })
    assert artifact["artifact_id"] == 1
    assert analysis["analysis_id"] == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_review_queue_uses_latest_analysis_and_compact_lineage():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionObservationDB.__table__,
                CorporateActionEffectiveDateEvidenceDB.__table__,
                CorporateActionDocumentArtifactDB.__table__,
                CorporateActionDocumentPageDB.__table__,
                CorporateActionLlmAnalysisDB.__table__,
                CorporateActionResolutionReviewDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ", symbol="000001", name="Test",
            exchange="SZSE", type="stock", currency="CNY", is_active=True,
        ))
        session.add_all([
            CorporateActionObservationDB(
                instrument_id="000001.SZ", source="cninfo",
                source_profile="cninfo_dividend", source_event_key=event_key,
                action_type="dividend", description="每10股派2.36元",
                event_status="implemented", quality_status="unvalidated",
                row_hash=("1" if event_key == "event-1" else "2") * 64,
            )
            for event_key in ("event-1", "event-2")
        ])
        artifact = CorporateActionDocumentArtifactDB(
            announcement_id="ann-1", source="cninfo",
            source_url="https://example.test/ann-1.pdf",
            announcement_title="权益分派实施公告",
            content_hash="a" * 64, content_type="application/pdf",
            content_length=10, archive_path="ann-1/a.pdf",
            download_status="downloaded", extraction_status="extracted",
            parser_version="unit.v1",
        )
        session.add(artifact)
        await session.flush()
        session.add(CorporateActionDocumentPageDB(
            artifact_id=artifact.id, page_number=1,
            extraction_method="native_text", quality_status="usable",
            text="整篇公告正文不应进入审核卡片",
            text_hash="b" * 64, character_count=14, parser_version="unit.v1",
        ))
        session.add(CorporateActionEffectiveDateEvidenceDB(
            instrument_id="000001.SZ", source_event_key="event-1",
            observation_source="cninfo", source_profile="cninfo_dividend",
            evidence_source="cninfo_announcement_metadata",
            evidence_key="ann-1", resolution_status="candidate",
            announcement_id="ann-1", announcement_title="权益分派实施公告",
            evidence_url="https://example.test/ann-1.pdf", row_hash="c" * 64,
        ))
        old_result = {
            "event_type": "dividend",
            "_review_classification": {
                "review_tier": "deep_review",
                "gate_signature": "date_in_evidence",
                "review_reasons": ["hard_gate:date_in_evidence"],
            },
        }
        latest_result = {
            "event_type": "dividend", "event_stage": "implemented",
            "effective_date": "2026-06-12",
            "effective_date_type": "ex_dividend_date",
            "date_basis": "official_announcement_explicit_statement",
            "economic_terms": {}, "alternative_dates": [], "conflicts": [],
            "confidence": 0.99, "reason": "正文明确披露",
            "evidence": [{
                "announcement_id": "ann-1", "section_id": "ann-1:p1",
                "page_number": 1, "text_hash": "b" * 64,
                "exact_quote": "除权除息日为2026年6月12日",
                "supports_fields": ["effective_date"],
            }],
            "_review_classification": {
                "review_tier": "quick_review",
                "gate_signature": "all_gates_passed",
                "review_reasons": ["validated_candidate_requires_explicit_review"],
            },
        }
        analyses = [
            CorporateActionLlmAnalysisDB(
                analysis_key="old", instrument_id="000001.SZ",
                source_event_key="event-1", analysis_status="manual_required",
                validation_status="manual_required", profile="semantic_extraction",
                schema_version="v1", prompt_version="p1", parser_version="p1",
                input_hash="d" * 64, artifact_ids_json=json.dumps([artifact.id]),
                result_json=json.dumps(old_result),
                gate_results_json=json.dumps({"date_in_evidence": False}),
                usage_json="{}", attempt_count=1,
            ),
            CorporateActionLlmAnalysisDB(
                analysis_key="latest", instrument_id="000001.SZ",
                source_event_key="event-1", analysis_status="resolved_candidate",
                validation_status="validated_candidate",
                profile="semantic_extraction", model="fake-model",
                schema_version="v1", prompt_version="p2", parser_version="p2",
                input_hash="e" * 64, response_hash="f" * 64,
                artifact_ids_json=json.dumps([artifact.id]),
                result_json=json.dumps(latest_result),
                gate_results_json=json.dumps({"date_in_evidence": True}),
                usage_json=json.dumps({"input_tokens": 100, "output_tokens": 50}),
                latency_ms=1200, attempt_count=1,
            ),
            CorporateActionLlmAnalysisDB(
                analysis_key="machine", instrument_id="000001.SZ",
                source_event_key="event-2", analysis_status="manual_required",
                validation_status="failed", profile="semantic_extraction",
                schema_version="v1", prompt_version="p2", parser_version="p2",
                input_hash="0" * 64, artifact_ids_json="[]",
                result_json="{}", gate_results_json="{}", usage_json="{}",
                attempt_count=1, error_code="schema_validation_failed",
            ),
        ]
        session.add_all(analyses)
        await session.flush()
        session.add(CorporateActionResolutionReviewDB(
            review_key="review-1", instrument_id="000001.SZ",
            source_event_key="event-1", analysis_id=analyses[1].id,
            evidence_key="ann-1", decision="manual_required",
            reviewer="reviewer", review_payload_json="{}",
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    page = await operations.get_corporate_action_review_queue(
        reviewed_state="reviewed", limit=10,
    )
    machine = await operations.get_corporate_action_review_queue(
        review_tier="machine_rework", include_machine_rework=True, limit=10,
    )
    second_page = await operations.get_corporate_action_review_queue(
        include_machine_rework=True, limit=1, offset=1,
    )

    assert page["total"] == 1
    assert page["items"][0]["analysis_id"] == analyses[1].id
    assert page["items"][0]["review_tier"] == "quick_review"
    assert page["items"][0]["evidence"][0]["exact_quote"].startswith("除权")
    assert "text" not in page["items"][0]["artifacts"][0]["pages"][0]
    assert page["items"][0]["usage"]["input_tokens"] == 100
    assert page["items"][0]["prior_reviews"][0]["reviewer"] == "reviewer"
    assert machine["total"] == 1
    assert machine["items"][0]["source_event_key"] == "event-2"
    assert machine["items"][0]["gate_signature"] == "schema_validation_failed"
    assert second_page["total"] == 2
    assert second_page["items"][0]["source_event_key"] == "event-2"
    await engine.dispose()


@pytest.mark.asyncio
async def test_failed_retry_does_not_overwrite_existing_candidate_analysis():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[InstrumentDB.__table__, CorporateActionLlmAnalysisDB.__table__],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ", symbol="000001", name="Test",
            exchange="SZSE", type="stock", currency="CNY", is_active=True,
        ))
        await session.commit()
    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    base = {
        "analysis_key": "analysis-key",
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "analysis_status": "manual_required",
        "profile": "semantic_extraction",
        "schema_version": "v1",
        "prompt_version": "p1",
        "parser_version": "parser-v1",
        "input_hash": "a" * 64,
        "artifact_ids": [],
    }
    await operations.save_corporate_action_llm_analysis({
        **base,
        "validation_status": "manual_required",
        "result": {"effective_date": "2026-06-12"},
        "gate_results": {"date_in_evidence": False},
    })
    retry = await operations.save_corporate_action_llm_analysis({
        **base,
        "validation_status": "failed",
        "result": {},
        "gate_results": {},
        "error_code": "deadline_exceeded",
    })
    page = await operations.get_corporate_action_llm_analyses(
        source_event_key="event-1"
    )
    assert retry["status"] == "unchanged"
    assert page["items"][0]["validation_status"] == "manual_required"
    assert page["items"][0]["result"]["effective_date"] == "2026-06-12"
    await engine.dispose()


@pytest.mark.asyncio
async def test_review_bundle_rolls_back_all_rows_when_terms_write_fails():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=True)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionLlmAnalysisDB.__table__,
                CorporateActionResolutionReviewDB.__table__,
                CorporateActionResolvedTermsDB.__table__,
                CorporateActionEffectiveDateEvidenceDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ", symbol="000001", name="Test",
            exchange="SZSE", type="stock", currency="CNY", is_active=True,
        ))
        analysis = CorporateActionLlmAnalysisDB(
            analysis_key="analysis-key", instrument_id="000001.SZ",
            source_event_key="event-1", analysis_status="resolved_candidate",
            validation_status="validated_candidate", profile="semantic_extraction",
            schema_version="v1", prompt_version="p1", parser_version="parser-v1",
            input_hash="a" * 64, artifact_ids_json="[]", result_json="{}",
            gate_results_json="{}", usage_json="{}", attempt_count=1,
        )
        session.add(analysis)
        await session.flush()
        analysis_id = analysis.id
        await session.commit()
    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    with pytest.raises(StatementError):
        await operations.save_corporate_action_review_bundle(
            review_row={
                "review_key": "review-key",
                "instrument_id": "000001.SZ",
                "source_event_key": "event-1",
                "analysis_id": analysis_id,
                "evidence_key": "ann-1",
                "decision": "resolved",
                "effective_date": "2026-06-12",
                "date_basis": "official_announcement_explicit_statement",
                "reviewer": "reviewer",
                "review_payload": {},
            },
            terms_row={
                "cash_dividend_per_share": {"invalid": "float"},
                "resolved_fields": ["cash_dividend_per_share"],
                "is_active": True,
            },
            evidence_row={
                "instrument_id": "000001.SZ",
                "source_event_key": "event-1",
                "source_profile": "cninfo_dividend",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "event-1",
                "resolution_status": "resolved",
                "effective_date": "2026-06-12",
                "date_basis": "official_announcement_explicit_statement",
            },
        )
    async with session_factory() as session:
        review_count = await session.scalar(
            select(func.count()).select_from(CorporateActionResolutionReviewDB)
        )
        terms_count = await session.scalar(
            select(func.count()).select_from(CorporateActionResolvedTermsDB)
        )
        evidence_count = await session.scalar(
            select(func.count()).select_from(CorporateActionEffectiveDateEvidenceDB)
        )
    assert review_count == 0
    assert terms_count == 0
    assert evidence_count == 0
    saved = await operations.save_corporate_action_review_bundle(
        review_row={
            "review_key": "review-key",
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "analysis_id": analysis_id,
            "evidence_key": "ann-1",
            "decision": "resolved",
            "effective_date": "2026-06-12",
            "date_basis": "official_announcement_explicit_statement",
            "reviewer": "reviewer",
            "review_payload": {},
        },
        terms_row={
            "cash_dividend_per_share": 0.236,
            "resolved_fields": ["cash_dividend_per_share"],
            "is_active": True,
        },
        evidence_row={
            "instrument_id": "000001.SZ",
            "source_event_key": "event-1",
            "source_profile": "cninfo_dividend",
            "evidence_source": "cninfo_reviewed_official_document",
            "evidence_key": "event-1",
            "resolution_status": "resolved",
            "effective_date": "2026-06-12",
            "date_basis": "official_announcement_explicit_statement",
        },
    )
    assert saved["review"]["review_id"] > 0
    assert saved["terms_write"]["resolved_terms_id"] > 0
    assert saved["evidence_write"]["inserted"] == 1
    await engine.dispose()
