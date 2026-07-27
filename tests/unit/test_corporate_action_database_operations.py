import asyncio
from datetime import date, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database.models import (
    Base,
    CorporateActionEffectiveDateEvidenceDB,
    CorporateActionInstrumentStatusDB,
    CorporateActionLlmAnalysisDB,
    CorporateActionObservationDB,
    CorporateActionResolutionReviewDB,
    CorporateActionResolvedTermsDB,
    InstrumentDB,
)
from database.operations import (
    DatabaseOperations,
    GOVERNED_CORPORATE_ACTION_EFFECTIVE_DATE_EVIDENCE_SOURCES,
)


def test_corporate_action_observation_revision_and_partial_coverage():
    asyncio.run(_exercise_observation_revision_and_partial_coverage())


async def _exercise_observation_revision_and_partial_coverage():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionObservationDB.__table__,
                CorporateActionInstrumentStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(
            InstrumentDB(
                instrument_id="000001.SZ",
                symbol="000001",
                name="Ping An Bank",
                exchange="SZSE",
                type="stock",
                currency="CNY",
                is_active=True,
            )
        )
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    observation = {
        "instrument_id": "000001.SZ",
        "source": "cninfo",
        "source_profile": "cninfo_dividend",
        "source_event_key": "event-1",
        "action_type": "dividend",
        "fiscal_period": "2025年报",
        "announcement_date": date(2026, 6, 1),
        "record_date": date(2026, 6, 11),
        "ex_date": date(2026, 6, 12),
        "cash_dividend_per_share": 0.2,
        "currency": "CNY",
        "event_status": "implemented",
        "quality_status": "structured_complete",
        "raw_payload": {"派息比例": 2.0},
    }
    first = await operations.save_corporate_action_observations(
        [observation], ingestion_run_id="run-1"
    )
    unchanged = await operations.save_corporate_action_observations(
        [observation], ingestion_run_id="run-2"
    )
    changed = await operations.save_corporate_action_observations(
        [{**observation, "cash_dividend_per_share": 0.25}],
        ingestion_run_id="run-3",
    )
    await operations.upsert_corporate_action_instrument_status(
        {
            "instrument_id": "000001.SZ",
            "source": "cninfo",
            "source_profile": "cninfo_dividend",
            "coverage_status": "partial_missing_fields",
            "event_count": 1,
            "missing_ex_date_count": 1,
            "requested_start_date": date(1990, 1, 1),
            "requested_end_date": date(2026, 7, 17),
            "ingestion_run_id": "run-3",
        }
    )

    async with session_factory() as session:
        row = (await session.execute(select(CorporateActionObservationDB))).scalar_one()
        status = (
            await session.execute(select(CorporateActionInstrumentStatusDB))
        ).scalar_one()

    assert first == {
        "inserted": 1,
        "changed": 0,
        "unchanged": 0,
        "reactivated": 0,
        "failed": 0,
    }
    assert unchanged == {
        "inserted": 0,
        "changed": 0,
        "unchanged": 1,
        "reactivated": 0,
        "failed": 0,
    }
    assert changed == {
        "inserted": 0,
        "changed": 1,
        "unchanged": 0,
        "reactivated": 0,
        "failed": 0,
    }
    assert row.cash_dividend_per_share == pytest.approx(0.25)
    assert row.row_version == 2
    assert row.ingestion_run_id == "run-3"
    assert status.coverage_status == "partial_missing_fields"
    assert status.missing_ex_date_count == 1
    assert isinstance(status.last_attempt_at, datetime)
    await engine.dispose()


def test_corporate_action_confirmed_empty_status_is_queryable():
    asyncio.run(_exercise_confirmed_empty_status_is_queryable())


async def _exercise_confirmed_empty_status_is_queryable():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionInstrumentStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(
            InstrumentDB(
                instrument_id="000003.SZ",
                symbol="000003",
                name="Delisted Sample",
                exchange="SZSE",
                type="stock",
                currency="CNY",
                is_active=False,
            )
        )
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    await operations.upsert_corporate_action_instrument_status(
        {
            "instrument_id": "000003.SZ",
            "source": "cninfo",
            "source_profile": "cninfo_allotment",
            "coverage_status": "complete_no_events",
            "event_count": 0,
            "requested_start_date": date(1990, 1, 1),
            "requested_end_date": date(2002, 12, 31),
        }
    )
    page = await operations.get_corporate_action_instrument_status_page(
        instrument_id="000003.SZ",
        coverage_status="complete_no_events",
    )

    assert page["total"] == 1
    assert page["items"][0]["source_profile"] == "cninfo_allotment"
    await engine.dispose()


def test_corporate_action_snapshot_retirement_and_reactivation():
    asyncio.run(_exercise_snapshot_retirement_and_reactivation())


async def _exercise_snapshot_retirement_and_reactivation():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionObservationDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(
            InstrumentDB(
                instrument_id="000001.SZ",
                symbol="000001",
                name="Ping An Bank",
                exchange="SZSE",
                type="stock",
                currency="CNY",
                is_active=True,
            )
        )
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    observations = [
        {
            "instrument_id": "000001.SZ",
            "source": "cninfo",
            "source_profile": "cninfo_dividend",
            "source_event_key": event_key,
            "action_type": "dividend",
            "ex_date": ex_date,
            "cash_dividend_per_share": amount,
            "quality_status": "structured_complete",
        }
        for event_key, ex_date, amount in (
            ("event-1", date(2025, 6, 10), 0.1),
            ("event-2", date(2026, 6, 10), 0.2),
        )
    ]
    await operations.save_corporate_action_observations(
        observations,
        ingestion_run_id="run-1",
    )
    retired = await operations.reconcile_corporate_action_observation_snapshot(
        instrument_id="000001.SZ",
        source="cninfo",
        source_profile="cninfo_dividend",
        requested_start_date=date(2020, 1, 1),
        requested_end_date=date(2026, 12, 31),
        seen_event_keys=["event-1"],
        ingestion_run_id="run-2",
    )

    current_page = await operations.get_corporate_action_observations(
        instrument_id="000001.SZ"
    )
    audit_page = await operations.get_corporate_action_observations(
        instrument_id="000001.SZ",
        include_inactive=True,
    )
    reactivated = await operations.save_corporate_action_observations(
        [observations[1]],
        ingestion_run_id="run-3",
    )
    restored_page = await operations.get_corporate_action_observations(
        instrument_id="000001.SZ"
    )

    assert retired == 1
    assert current_page["total"] == 1
    assert audit_page["total"] == 2
    retired_row = next(
        row for row in audit_page["items"] if row["source_event_key"] == "event-2"
    )
    assert retired_row["is_current"] is False
    assert retired_row["retirement_reason"] == ("missing_from_complete_source_snapshot")
    assert reactivated["reactivated"] == 1
    assert restored_page["total"] == 2
    await engine.dispose()


def test_corporate_action_coverage_is_versioned_by_requested_range():
    asyncio.run(_exercise_coverage_is_versioned_by_requested_range())


def test_effective_date_evidence_is_idempotent_and_queryable():
    asyncio.run(_exercise_effective_date_evidence_is_idempotent_and_queryable())


def test_effective_date_uses_active_superseding_review():
    asyncio.run(_exercise_effective_date_uses_active_superseding_review())


def test_operator_tdx_date_evidence_is_loaded_and_reset_protected():
    asyncio.run(_exercise_operator_tdx_date_evidence_is_governed())


async def _exercise_operator_tdx_date_evidence_is_governed():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionObservationDB.__table__,
                CorporateActionResolutionReviewDB.__table__,
                CorporateActionResolvedTermsDB.__table__,
                CorporateActionEffectiveDateEvidenceDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000897.SZ",
            symbol="000897",
            name="Test",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        session.add(CorporateActionObservationDB(
            instrument_id="000897.SZ",
            source="cninfo",
            source_profile="cninfo_dividend",
            source_event_key="event-operator-date",
            action_type="capitalization",
            record_date=date(2005, 11, 9),
            capitalization_shares_per_share=0.21,
            row_hash="observation-hash",
        ))
        session.add(CorporateActionEffectiveDateEvidenceDB(
            instrument_id="000897.SZ",
            source_event_key="event-operator-date",
            observation_source="cninfo",
            source_profile="cninfo_dividend",
            evidence_source="cninfo_tdx_xdxr_operator_review",
            evidence_key="tdx_xdxr:34700",
            resolution_status="resolved",
            effective_date=datetime(2005, 11, 11),
            date_basis="TDX XDXR除权交易日",
            raw_payload_json="{}",
            row_hash="evidence-hash",
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    resolved = await operations.get_resolved_corporate_action_effective_dates(
        ["event-operator-date"]
    )
    reset_preview = (
        await operations.reset_cninfo_corporate_action_resolution_data(
            start_date=date(2005, 1, 1),
            end_date=date(2005, 12, 31),
            exchanges=["SZSE"],
            source_event_keys=["event-operator-date"],
            dry_run=True,
        )
    )

    assert (
        "cninfo_tdx_xdxr_operator_review"
        in GOVERNED_CORPORATE_ACTION_EFFECTIVE_DATE_EVIDENCE_SOURCES
    )
    assert resolved["event-operator-date"]["effective_date"].date() == date(
        2005, 11, 11
    )
    assert resolved["event-operator-date"]["evidence_source"] == (
        "cninfo_tdx_xdxr_operator_review"
    )
    assert reset_preview["protected_resolved_events"] == 1
    assert reset_preview["reset_events"] == 0
    await engine.dispose()


async def _exercise_effective_date_evidence_is_idempotent_and_queryable():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
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
            instrument_id="000007.SZ",
            symbol="000007",
            name="Test",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    candidate = {
        "instrument_id": "000007.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "evidence_source": "cninfo_announcement",
        "evidence_key": "announcement-1",
        "resolution_status": "candidate",
        "announcement_id": "announcement-1",
        "announcement_title": "股权分置改革方案实施公告",
        "announcement_time": date(2006, 8, 10),
        "evidence_url": "https://example.test/announcement.pdf",
    }
    first = await operations.save_corporate_action_effective_date_evidence(
        [candidate], ingestion_run_id="run-1"
    )
    unchanged = await operations.save_corporate_action_effective_date_evidence(
        [candidate], ingestion_run_id="run-2"
    )
    resolved = {
        **candidate,
        "resolution_status": "resolved",
        "effective_date": date(2006, 8, 14),
        "date_basis": "official_resumption_date",
    }
    changed = await operations.save_corporate_action_effective_date_evidence(
        [resolved], ingestion_run_id="run-3"
    )
    rediscovered = await operations.save_corporate_action_effective_date_evidence(
        [candidate], ingestion_run_id="run-4"
    )
    invalid_resolved = await operations.save_corporate_action_effective_date_evidence(
        [{**candidate, "resolution_status": "resolved", "effective_date": date(2006, 8, 14)}],
        ingestion_run_id="run-5",
    )
    page = await operations.get_corporate_action_effective_date_evidence(
        instrument_id="000007.SZ",
        resolution_status="resolved",
    )
    resolved_map = await operations.get_resolved_corporate_action_effective_dates(
        ["event-1"]
    )

    assert first["inserted"] == 1
    assert unchanged["unchanged"] == 1
    assert changed["changed"] == 1
    assert rediscovered["unchanged"] == 1
    assert invalid_resolved["failed"] == 1
    assert page["total"] == 1
    assert page["items"][0]["date_basis"] == "official_resumption_date"
    assert resolved_map["event-1"]["effective_date"].date() == date(2006, 8, 14)
    await engine.dispose()


async def _exercise_effective_date_uses_active_superseding_review():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
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
            instrument_id="600449.SH",
            symbol="600449",
            name="Test",
            exchange="SSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        analysis = CorporateActionLlmAnalysisDB(
            analysis_key="analysis-1",
            instrument_id="600449.SH",
            source_event_key="event-1",
            analysis_status="completed",
            validation_status="manual_required",
            profile="semantic_extraction",
            schema_version="v1",
            prompt_version="v1",
            parser_version="v1",
            input_hash="input-1",
            artifact_ids_json="[]",
            gate_results_json="{}",
        )
        session.add(analysis)
        await session.flush()
        old_review = CorporateActionResolutionReviewDB(
            review_key="review-old",
            instrument_id="600449.SH",
            source_event_key="event-1",
            analysis_id=analysis.id,
            evidence_key="announcement-old",
            decision="resolved",
            effective_date=datetime(2006, 7, 14),
            date_basis="复牌日",
            reviewer="operator",
            review_payload_json="{}",
        )
        session.add(old_review)
        await session.flush()
        active_review = CorporateActionResolutionReviewDB(
            review_key="review-new",
            instrument_id="600449.SH",
            source_event_key="event-1",
            analysis_id=analysis.id,
            evidence_key="announcement-new",
            decision="resolved",
            effective_date=datetime(2006, 8, 15),
            date_basis="股份到账日",
            reviewer="operator",
            review_payload_json="{}",
            supersedes_review_id=old_review.id,
        )
        session.add(active_review)
        await session.flush()
        session.add(CorporateActionResolvedTermsDB(
            instrument_id="600449.SH",
            source_event_key="event-1",
            analysis_id=analysis.id,
            review_id=active_review.id,
            capitalization_shares_per_share=0.172488,
            currency="CNY",
            is_active=True,
            resolved_fields_json='["capitalization_shares_per_share"]',
            evidence_json="{}",
        ))
        for evidence_source, effective_date, review_key in (
            ("cninfo_announcement_review", datetime(2006, 7, 14), "review-old"),
            (
                "cninfo_reviewed_official_document",
                datetime(2006, 8, 15),
                "review-new",
            ),
        ):
            session.add(CorporateActionEffectiveDateEvidenceDB(
                instrument_id="600449.SH",
                source_event_key="event-1",
                observation_source="cninfo",
                source_profile="cninfo_dividend",
                evidence_source=evidence_source,
                evidence_key="announcement-new",
                resolution_status="resolved",
                effective_date=effective_date,
                date_basis=(
                    "股份到账日"
                    if review_key == "review-new"
                    else "复牌日"
                ),
                raw_payload_json=(
                    '{"review_key": "' + review_key + '"}'
                ),
                row_hash=review_key,
            ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    resolved_map = await operations.get_resolved_corporate_action_effective_dates(
        ["event-1"]
    )

    assert resolved_map["event-1"]["effective_date"].date() == date(2006, 8, 15)
    assert resolved_map["event-1"]["date_basis"] == "股份到账日"
    assert resolved_map["event-1"]["evidence_key"] == "announcement-new"

    async with session_factory() as session:
        terms = await session.scalar(
            select(CorporateActionResolvedTermsDB).where(
                CorporateActionResolvedTermsDB.source_event_key == "event-1"
            )
        )
        rejected_review = CorporateActionResolutionReviewDB(
            review_key="review-rejected",
            instrument_id="600449.SH",
            source_event_key="event-1",
            analysis_id=analysis.id,
            evidence_key="announcement-new",
            decision="rejected",
            reviewer="operator",
            review_payload_json="{}",
            supersedes_review_id=active_review.id,
        )
        session.add(rejected_review)
        await session.flush()
        terms.review_id = rejected_review.id
        terms.is_active = False
        await session.commit()

    assert (
        await operations.get_resolved_corporate_action_effective_dates(
            ["event-1"]
        )
    ) == {}
    await engine.dispose()


async def _exercise_coverage_is_versioned_by_requested_range():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                CorporateActionInstrumentStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(
            InstrumentDB(
                instrument_id="000001.SZ",
                symbol="000001",
                name="Ping An Bank",
                exchange="SZSE",
                type="stock",
                currency="CNY",
                is_active=True,
            )
        )
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    base_status = {
        "instrument_id": "000001.SZ",
        "source": "cninfo",
        "source_profile": "cninfo_dividend",
        "coverage_status": "complete_with_events",
        "event_count": 10,
    }
    await operations.upsert_corporate_action_instrument_status(
        {
            **base_status,
            "requested_start_date": date(1990, 1, 1),
            "requested_end_date": date(2026, 12, 31),
        }
    )
    await operations.upsert_corporate_action_instrument_status(
        {
            **base_status,
            "coverage_status": "complete_no_events",
            "event_count": 0,
            "requested_start_date": date(2020, 1, 1),
            "requested_end_date": date(2020, 12, 31),
        }
    )
    page = await operations.get_corporate_action_instrument_status_page(
        instrument_id="000001.SZ"
    )

    assert page["total"] == 2
    assert {
        (item["requested_start_date"].date(), item["requested_end_date"].date())
        for item in page["items"]
    } == {
        (date(1990, 1, 1), date(2026, 12, 31)),
        (date(2020, 1, 1), date(2020, 12, 31)),
    }
    await engine.dispose()
