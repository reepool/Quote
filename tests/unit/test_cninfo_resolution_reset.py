from datetime import datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database.models import (
    Base,
    CorporateActionDocumentArtifactDB,
    CorporateActionDocumentPageDB,
    CorporateActionEffectiveDateEvidenceDB,
    CorporateActionLlmAnalysisDB,
    CorporateActionObservationDB,
    CorporateActionResolutionReviewDB,
    CorporateActionResolutionStateDB,
    CorporateActionResolvedTermsDB,
    InstrumentDB,
)
from database.operations import DatabaseOperations


@pytest.mark.asyncio
async def test_resolution_reset_preserves_resolved_lineage_and_supports_dry_run():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    tables = [
        InstrumentDB.__table__,
        CorporateActionObservationDB.__table__,
        CorporateActionEffectiveDateEvidenceDB.__table__,
        CorporateActionDocumentArtifactDB.__table__,
        CorporateActionDocumentPageDB.__table__,
        CorporateActionLlmAnalysisDB.__table__,
        CorporateActionResolutionReviewDB.__table__,
        CorporateActionResolvedTermsDB.__table__,
        CorporateActionResolutionStateDB.__table__,
    ]
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=tables)
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ",
            symbol="000001",
            name="Test",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        for event_key in (
            "event-resolved", "event-pending", "event-unanchored"
        ):
            session.add(CorporateActionObservationDB(
                instrument_id="000001.SZ",
                source="cninfo",
                source_profile="cninfo_dividend",
                source_event_key=event_key,
                action_type="dividend",
                announcement_date=(
                    None
                    if event_key == "event-unanchored"
                    else datetime(2001, 6, 1)
                ),
                cash_dividend_per_share=0.1,
                event_status="announced_incomplete",
                quality_status="partial_missing_ex_date",
                row_hash=event_key,
                is_current=True,
            ))
        session.add_all([
            CorporateActionEffectiveDateEvidenceDB(
                instrument_id="000001.SZ",
                source_event_key="event-resolved",
                observation_source="cninfo",
                source_profile="cninfo_dividend",
                evidence_source="cninfo_reviewed_official_document",
                evidence_key="ann-resolved",
                resolution_status="resolved",
                effective_date=datetime(2001, 6, 5),
                date_basis="official_announcement",
                announcement_id="ann-resolved",
                row_hash="resolved-evidence",
            ),
            CorporateActionEffectiveDateEvidenceDB(
                instrument_id="000001.SZ",
                source_event_key="event-pending",
                observation_source="cninfo",
                source_profile="cninfo_dividend",
                evidence_source="cninfo_announcement_metadata",
                evidence_key="ann-pending",
                resolution_status="candidate",
                announcement_id="ann-pending",
                row_hash="pending-evidence",
            ),
            CorporateActionEffectiveDateEvidenceDB(
                instrument_id="000001.SZ",
                source_event_key="event-unanchored",
                observation_source="cninfo",
                source_profile="cninfo_dividend",
                evidence_source="cninfo_announcement_metadata",
                evidence_key="ann-unanchored",
                resolution_status="candidate",
                announcement_id="ann-unanchored",
                row_hash="unanchored-evidence",
            ),
        ])
        session.add_all([
            CorporateActionDocumentArtifactDB(
                announcement_id="ann-resolved",
                source="cninfo",
                source_url="https://example.test/resolved.pdf",
                content_hash="a" * 64,
                content_length=10,
                archive_path="resolved.pdf",
                download_status="success",
                extraction_status="success",
                parser_version="test.v1",
            ),
            CorporateActionDocumentArtifactDB(
                announcement_id="ann-pending",
                source="cninfo",
                source_url="https://example.test/pending.pdf",
                content_hash="b" * 64,
                content_length=10,
                archive_path="pending.pdf",
                download_status="success",
                extraction_status="success",
                parser_version="test.v1",
            ),
        ])
        await session.flush()
        artifacts = (await session.execute(
            select(CorporateActionDocumentArtifactDB)
        )).scalars().all()
        artifact_by_announcement = {
            row.announcement_id: row for row in artifacts
        }
        for announcement_id, artifact in artifact_by_announcement.items():
            session.add(CorporateActionDocumentPageDB(
                artifact_id=artifact.id,
                page_number=1,
                extraction_method="text",
                quality_status="usable",
                text=announcement_id,
                text_hash=announcement_id,
                character_count=len(announcement_id),
                parser_version="test.v1",
            ))
        for event_key, announcement_id in (
            ("event-resolved", "ann-resolved"),
            ("event-pending", "ann-pending"),
        ):
            artifact_id = artifact_by_announcement[announcement_id].id
            analysis = CorporateActionLlmAnalysisDB(
                analysis_key=f"analysis-{event_key}",
                instrument_id="000001.SZ",
                source_event_key=event_key,
                analysis_status="manual_required",
                validation_status="manual_required",
                profile="semantic_extraction",
                schema_version="v1",
                prompt_version="v1",
                parser_version="v1",
                input_hash=event_key,
                artifact_ids_json=f"[{artifact_id}]",
                gate_results_json="{}",
                attempt_count=1,
            )
            session.add(analysis)
            await session.flush()
            session.add(CorporateActionResolutionStateDB(
                instrument_id="000001.SZ",
                source_event_key=event_key,
                source_profile="cninfo_dividend",
                action_type="dividend",
                exchange="SZSE",
                policy_version="test",
                state_version="test",
                resolution_state=(
                    "resolved_evidence"
                    if event_key == "event-resolved"
                    else "candidate_pending_analysis"
                ),
                is_terminal=event_key == "event-resolved",
                factor_blocking=event_key != "event-resolved",
                state_reason="test",
                next_action="none",
                candidate_count=1,
                latest_analysis_id=analysis.id,
                diagnostics_json="{}",
            ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    preview = await operations.reset_cninfo_corporate_action_resolution_data(
        start_date=datetime(1990, 12, 19),
        end_date=datetime(2026, 7, 23),
        exchanges=["SZSE"],
        dry_run=True,
    )
    assert preview["selected_events"] == 2
    assert preview["protected_resolved_events"] == 1
    assert preview["reset_events"] == 1
    assert preview["reset_source_event_keys"] == ["event-pending"]
    assert preview["reset_instrument_ids"] == ["000001.SZ"]
    assert preview["protected_instrument_ids"] == ["000001.SZ"]
    assert preview["deleted"]["effective_date_evidence"] == 1
    assert preview["deleted"]["llm_analyses"] == 1
    assert preview["deleted"]["resolution_states"] == 1
    assert preview["deleted"]["document_artifacts"] == 1
    assert preview["deleted"]["document_pages"] == 1

    preview_with_unanchored = (
        await operations.reset_cninfo_corporate_action_resolution_data(
            start_date=datetime(1990, 12, 19),
            end_date=datetime(2026, 7, 23),
            exchanges=["SZSE"],
            include_unanchored=True,
            dry_run=True,
        )
    )
    assert preview_with_unanchored["selected_events"] == 3
    assert preview_with_unanchored["reset_events"] == 2
    exact_event_preview = (
        await operations.reset_cninfo_corporate_action_resolution_data(
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2026, 7, 23),
            exchanges=["SZSE"],
            source_event_keys=["event-pending"],
            dry_run=True,
        )
    )
    assert exact_event_preview["selected_events"] == 1
    assert exact_event_preview["reset_source_event_keys"] == ["event-pending"]

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(
            CorporateActionEffectiveDateEvidenceDB
        )) == 3

    written = await operations.reset_cninfo_corporate_action_resolution_data(
        start_date=datetime(1990, 12, 19),
        end_date=datetime(2026, 7, 23),
        exchanges=["SZSE"],
        dry_run=False,
    )
    assert written["deleted"] == preview["deleted"]
    async with session_factory() as session:
        evidence = (await session.execute(select(
            CorporateActionEffectiveDateEvidenceDB
        ))).scalars().all()
        analyses = (await session.execute(select(
            CorporateActionLlmAnalysisDB
        ))).scalars().all()
        artifacts = (await session.execute(select(
            CorporateActionDocumentArtifactDB
        ))).scalars().all()
        observations = await session.scalar(select(func.count()).select_from(
            CorporateActionObservationDB
        ))
    assert sorted(row.source_event_key for row in evidence) == [
        "event-resolved",
        "event-unanchored",
    ]
    assert [row.source_event_key for row in analyses] == ["event-resolved"]
    assert [row.announcement_id for row in artifacts] == ["ann-resolved"]
    assert observations == 3
    await engine.dispose()
