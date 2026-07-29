from datetime import datetime

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database.models import (
    Base,
    CorporateActionObservationDB,
    CorporateActionResolutionStateDB,
    InstrumentDB,
)
from database.operations import DatabaseOperations


@pytest.mark.asyncio
async def test_resolution_state_upsert_is_idempotent_and_filterable():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionObservationDB.__table__,
                CorporateActionResolutionStateDB.__table__,
            ],
        )
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
        session.add(CorporateActionObservationDB(
            instrument_id="000001.SZ",
            source="cninfo",
            source_profile="cninfo_dividend",
            source_event_key="event-1",
            action_type="dividend",
            announcement_date=datetime(2026, 6, 1),
            cash_dividend_per_share=0.1,
            currency="CNY",
            event_status="announced_incomplete",
            quality_status="partial_missing_ex_date",
            row_hash="a" * 64,
            is_current=True,
        ))
        session.add(CorporateActionObservationDB(
            instrument_id="000001.SZ",
            source="cninfo",
            source_profile="cninfo_dividend",
            source_event_key="event-retired",
            action_type="dividend",
            announcement_date=datetime(2025, 6, 1),
            cash_dividend_per_share=0.1,
            currency="CNY",
            event_status="announced_incomplete",
            quality_status="partial_missing_ex_date",
            row_hash="b" * 64,
            is_current=False,
        ))
        await session.commit()
    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    attempted_at = datetime(2026, 7, 21, 8, 0, 0)
    row = {
        "instrument_id": "000001.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "exchange": "SZSE",
        "policy_version": "policy-v1",
        "state_version": "state-v1",
        "resolution_state": "candidate_pending_analysis",
        "is_terminal": False,
        "factor_blocking": True,
        "state_reason": "candidate_present",
        "next_action": "semantic_resolution",
        "candidate_count": 2,
        "last_attempt_at": attempted_at,
        "diagnostics": {"missing_required_date_roles": ["effective_date"]},
    }

    retired_row = {**row, "source_event_key": "event-retired"}
    first = await operations.upsert_corporate_action_resolution_states(
        [row, retired_row], ingestion_run_id="run-1"
    )
    second = await operations.upsert_corporate_action_resolution_states(
        [row, retired_row], ingestion_run_id="run-1"
    )
    without_attempt_time = await operations.upsert_corporate_action_resolution_states(
        [{**row, "last_attempt_at": None}], ingestion_run_id="run-1"
    )
    page = await operations.get_corporate_action_resolution_states(
        is_terminal=False,
        factor_blocking=True,
        next_action="semantic_resolution",
        limit=10,
        offset=0,
    )
    all_states = await operations.get_corporate_action_resolution_states(
        current_only=False,
        limit=10,
        offset=0,
    )

    assert first == {"inserted": 2, "changed": 0, "unchanged": 0, "failed": 0}
    assert second == {"inserted": 0, "changed": 0, "unchanged": 2, "failed": 0}
    assert without_attempt_time == {
        "inserted": 0, "changed": 0, "unchanged": 1, "failed": 0
    }
    assert page["total"] == 1
    assert all_states["total"] == 2
    assert page["items"][0]["candidate_count"] == 2
    assert page["items"][0]["diagnostics"] == {
        "missing_required_date_roles": ["effective_date"]
    }
    document_rework = await operations.upsert_corporate_action_resolution_states(
        [{
            **row,
            "resolution_state": "document_rework",
            "state_reason": "analysis_context_incomplete",
            "next_action": "repair_document_context",
        }],
        ingestion_run_id="run-2",
    )
    document_page = await operations.get_corporate_action_resolution_states(
        factor_blocking=True,
        next_action="repair_document_context",
        limit=10,
        offset=0,
    )
    assert document_rework == {
        "inserted": 0, "changed": 1, "unchanged": 0, "failed": 0
    }
    assert document_page["total"] == 1
    assert document_page["items"][0]["resolution_state"] == "document_rework"
    pre_listing = await operations.upsert_corporate_action_resolution_states(
        [{
            **row,
            "resolution_state": "pre_listing",
            "is_terminal": True,
            "factor_blocking": False,
            "state_reason": "review_confirmed_pre_listing_event",
            "next_action": "none",
        }],
        ingestion_run_id="run-3",
    )
    pre_listing_page = (
        await operations.get_corporate_action_resolution_states(
            resolution_state="pre_listing",
            is_terminal=True,
            factor_blocking=False,
            limit=10,
            offset=0,
        )
    )
    assert pre_listing == {
        "inserted": 0, "changed": 1, "unchanged": 0, "failed": 0
    }
    assert pre_listing_page["total"] == 1
    archive_gap_ignored = (
        await operations.upsert_corporate_action_resolution_states(
            [{
                **row,
                "resolution_state": "archive_gap_ignored",
                "is_terminal": True,
                "factor_blocking": False,
                "state_reason": (
                    "review_accepted_unrecoverable_historical_archive_gap"
                ),
                "next_action": "none",
            }],
            ingestion_run_id="run-4",
        )
    )
    archive_gap_page = (
        await operations.get_corporate_action_resolution_states(
            resolution_state="archive_gap_ignored",
            is_terminal=True,
            factor_blocking=False,
            limit=10,
            offset=0,
        )
    )
    assert archive_gap_ignored == {
        "inserted": 0, "changed": 1, "unchanged": 0, "failed": 0
    }
    assert archive_gap_page["total"] == 1
    await engine.dispose()
