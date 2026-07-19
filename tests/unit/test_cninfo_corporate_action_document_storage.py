from datetime import date

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database.models import (
    Base,
    CorporateActionDocumentArtifactDB,
    CorporateActionDocumentPageDB,
    CorporateActionEffectiveDateEvidenceDB,
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
