import asyncio
import json
from datetime import date, datetime
import sqlite3

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from database.models import (
    AdjustmentFactorCanonicalDB,
    AdjustmentFactorInstrumentStatusDB,
    AdjustmentFactorObservationDB,
    AdjustmentFactorSeriesStatusDB,
    Base,
    InstrumentDB,
)
from database.connection import DatabaseManager
from database.operations import DatabaseOperations


def _candidate_report(*, full_market: bool, row_count: int, instrument_count: int):
    gates = {
        "full_market_scope": full_market,
        "no_cninfo_pending_factor_events": True,
        "no_historical_factor_gaps": True,
        "no_unverified_tdx_fallback": True,
        "no_blocked_selection_segments": True,
        "candidate_write_success": True,
    }
    return {
        "candidate_promotion_eligible": full_market,
        "incremental_merge_eligible": not full_market,
        "blocking_quality_gates": gates,
        "row_count": row_count,
        "instrument_count": instrument_count,
        "blocked_segment_count": 0,
        "decisions": [],
    }


def test_candidate_validation_accepts_truncated_prefix_for_long_target():
    target = "a" * 64
    token = "0123456789abcdef"
    staging = (
        f"{target[:64 - len('__staging__') - len(token)]}"
        f"__staging__{token}"
    )

    errors = DatabaseOperations._canonical_candidate_validation_errors(
        staging_series_version=staging,
        target_series_version=target,
        persisted_status="validated_staging",
        report={
            **_candidate_report(full_market=True, row_count=1, instrument_count=1),
            "candidate_promotion_eligible": True,
        },
        canonical_row_count=1,
        canonical_instrument_ids={"000001.SZ"},
        status_rows=[{
            "instrument_id": "000001.SZ",
            "coverage_status": "complete_with_events",
            "event_count": 1,
        }],
        require_full_market=True,
    )

    assert not any("staging version" in error for error in errors)


def test_candidate_validation_rejects_missing_persisted_gate():
    report = _candidate_report(full_market=True, row_count=1, instrument_count=1)
    report["blocking_quality_gates"].pop("candidate_write_success")

    errors = DatabaseOperations._canonical_candidate_validation_errors(
        staging_series_version="v1__staging__unit",
        target_series_version="v1",
        persisted_status="validated_staging",
        report=report,
        canonical_row_count=1,
        canonical_instrument_ids={"000001.SZ"},
        status_rows=[{
            "instrument_id": "000001.SZ",
            "coverage_status": "complete_with_events",
            "event_count": 1,
        }],
        require_full_market=True,
    )

    assert any("candidate_write_success" in error for error in errors)


def test_existing_database_bootstraps_adjustment_factor_governance_tables(tmp_path):
    database_path = tmp_path / "existing.db"
    sqlite3.connect(database_path).close()
    manager = DatabaseManager(str(database_path))

    manager.initialize()
    try:
        with sqlite3.connect(database_path) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert {
            "adjustment_factor_observations",
            "adjustment_factors_canonical",
            "adjustment_factor_series_status",
            "adjustment_factor_instrument_status",
        } <= tables
    finally:
        manager.close()


def test_replace_adjustment_factor_observations_removes_stale_dates():
    asyncio.run(_exercise_replace_adjustment_factor_observations())


def test_provider_snapshot_observations_and_status_commit_atomically():
    asyncio.run(_exercise_atomic_provider_snapshot())


def test_adjustment_factor_schema_removes_only_obsolete_price_ratio_state(
    tmp_path,
):
    database_path = tmp_path / "obsolete_factor_state.db"
    manager = DatabaseManager(str(database_path))
    manager.sync_engine = create_engine(f"sqlite:///{database_path}")

    with manager.sync_engine.begin() as connection:
        Base.metadata.create_all(
            bind=connection,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorObservationDB.__table__,
                AdjustmentFactorCanonicalDB.__table__,
                AdjustmentFactorSeriesStatusDB.__table__,
                AdjustmentFactorInstrumentStatusDB.__table__,
            ],
        )
        connection.execute(InstrumentDB.__table__.insert(), {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "name": "Ping An Bank",
            "exchange": "SZSE",
            "type": "stock",
            "currency": "CNY",
            "is_active": True,
        })
        connection.execute(
            AdjustmentFactorObservationDB.__table__.insert(),
            [
                {
                    "instrument_id": "000001.SZ",
                    "ex_date": datetime(2020, 5, 28),
                    "source": "akshare",
                    "source_profile": "akshare_tencent_price_ratio_v1",
                    "normalization_version": "event_ratio_v1",
                    "quality_status": "valid",
                },
                {
                    "instrument_id": "000001.SZ",
                    "ex_date": datetime(2021, 5, 28),
                    "source": "akshare",
                    "source_profile": "sina_hfq_factor",
                    "normalization_version": "event_ratio_v1",
                    "quality_status": "valid",
                },
            ],
        )
        connection.execute(
            AdjustmentFactorInstrumentStatusDB.__table__.insert(),
            [
                {
                    "instrument_id": "000001.SZ",
                    "series_version":
                        "akshare_market_price_ratio_snapshot_v1",
                    "source": "akshare",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                },
                {
                    "instrument_id": "000001.SZ",
                    "series_version": "sina_hfq_factor_snapshot_v1",
                    "source": "sina_hfq_factor",
                    "coverage_status": "complete_with_events",
                    "event_count": 1,
                },
            ],
        )

    manager._ensure_adjustment_factor_governance_schema()

    with manager.sync_engine.connect() as connection:
        observation_profiles = set(connection.execute(
            select(AdjustmentFactorObservationDB.source_profile)
        ).scalars())
        status_versions = set(connection.execute(
            select(AdjustmentFactorInstrumentStatusDB.series_version)
        ).scalars())

    assert observation_profiles == {"sina_hfq_factor"}
    assert status_versions == {"sina_hfq_factor_snapshot_v1"}
    manager.sync_engine.dispose()


async def _exercise_atomic_provider_snapshot():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorObservationDB.__table__,
                AdjustmentFactorInstrumentStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ",
            symbol="000001",
            name="Ping An Bank",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()

    def observation(factor):
        return {
            "instrument_id": "000001.SZ",
            "ex_date": datetime(2020, 5, 28),
            "source": "akshare",
            "source_profile": "sina_hfq_factor",
            "provider_factor": factor,
            "normalized_factor": factor,
            "normalization_version": "event_ratio_v1",
            "quality_status": "valid",
            "raw_payload": {"factor": factor},
        }

    async def save_snapshot(factor, run_id):
        return await operations.save_adjustment_factor_provider_snapshot(
            [observation(factor)],
            instrument_id="000001.SZ",
            source="akshare",
            source_profile="sina_hfq_factor",
            status_source="sina_hfq_factor",
            series_version="sina_hfq_factor_snapshot_v1",
            coverage_status="complete_with_events",
            start_date=date(1990, 12, 19),
            end_date=date(2026, 7, 29),
            ingestion_run_id=run_id,
        )

    first = await save_snapshot(1.02, "stable-run")
    assert first == {
        "inserted": 1,
        "changed": 0,
        "unchanged": 0,
        "failed": 0,
        "status_saved": 1,
    }

    def reject_status(_mapper, _connection, target):
        if target.ingestion_run_id == "failing-run":
            raise RuntimeError("injected status write failure")

    event.listen(
        AdjustmentFactorInstrumentStatusDB,
        "before_insert",
        reject_status,
    )
    try:
        with pytest.raises(
            RuntimeError, match="injected status write failure"
        ):
            await save_snapshot(1.03, "failing-run")
    finally:
        event.remove(
            AdjustmentFactorInstrumentStatusDB,
            "before_insert",
            reject_status,
        )

    async with session_factory() as session:
        stored_observation = (await session.execute(
            select(AdjustmentFactorObservationDB)
        )).scalar_one()
        stored_status = (await session.execute(
            select(AdjustmentFactorInstrumentStatusDB)
        )).scalar_one()

    assert stored_observation.normalized_factor == pytest.approx(1.02)
    assert stored_observation.ingestion_run_id == "stable-run"
    assert stored_status.ingestion_run_id == "stable-run"
    assert stored_status.event_count == 1
    await engine.dispose()


async def _exercise_replace_adjustment_factor_observations():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorObservationDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000035.SZ",
            symbol="000035",
            name="Test",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        session.add_all([
            AdjustmentFactorObservationDB(
                instrument_id="000035.SZ",
                ex_date=datetime(2012, 11, 30),
                source="cninfo",
                source_profile="cninfo_official_event_product_v1",
                provider_factor=1.2596,
                normalized_factor=1.2596,
                normalization_version="event_ratio_v1",
                quality_status="valid",
                raw_payload_json='{"source_event_keys": ["event-none"]}',
            ),
            AdjustmentFactorObservationDB(
                instrument_id="000035.SZ",
                ex_date=datetime(2011, 1, 1),
                source="cninfo",
                source_profile="cninfo_official_event_product_v1",
                provider_factor=1.1,
                normalized_factor=1.1,
                normalization_version="event_ratio_v1",
                quality_status="valid",
                raw_payload_json='{"source_event_keys": ["event-keep"]}',
            ),
        ])
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    replacement = {
        "instrument_id": "000035.SZ",
        "ex_date": datetime(2013, 1, 2),
        "source": "cninfo",
        "source_profile": "cninfo_official_event_product_v1",
        "provider_factor": 1.05,
        "normalized_factor": 1.05,
        "normalization_version": "event_ratio_v1",
        "quality_status": "valid",
        "raw_payload": {"source_event_keys": ["event-new"]},
    }
    result = await operations.replace_adjustment_factor_observations(
        [replacement],
        instrument_ids=["000035.SZ"],
        source="cninfo",
        source_profile="cninfo_official_event_product_v1",
        cleanup_source_event_keys=["event-none"],
        additional_keys=[("000035.SZ", date(2012, 11, 30))],
        ingestion_run_id="unit",
    )
    repeated = await operations.replace_adjustment_factor_observations(
        [replacement],
        instrument_ids=["000035.SZ"],
        source="cninfo",
        source_profile="cninfo_official_event_product_v1",
        cleanup_source_event_keys=["event-none"],
        additional_keys=[("000035.SZ", date(2012, 11, 30))],
        ingestion_run_id="unit-repeat",
    )

    async with session_factory() as session:
        rows = (await session.execute(
            select(AdjustmentFactorObservationDB).order_by(
                AdjustmentFactorObservationDB.ex_date
            )
        )).scalars().all()

    assert result == {"deleted": 1, "inserted": 1, "failed": 0}
    assert repeated == {"deleted": 1, "inserted": 1, "failed": 0}
    assert [row.ex_date.date() for row in rows] == [
        date(2011, 1, 1),
        date(2013, 1, 2),
    ]
    await engine.dispose()


@pytest.mark.asyncio
async def test_promote_canonical_series_atomically_copies_staging_rows_and_statuses():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorCanonicalDB.__table__,
                AdjustmentFactorInstrumentStatusDB.__table__,
                AdjustmentFactorSeriesStatusDB.__table__,
            ],
        )

    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ",
            symbol="000001",
            name="Ping An Bank",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        session.add(AdjustmentFactorCanonicalDB(
            instrument_id="000001.SZ",
            ex_date=datetime(2020, 5, 28),
            series_version="v1__staging__unit",
            factor=1.02,
            cumulative_factor=1.02,
            selected_source="akshare",
            source_profile="sina_hfq_factor",
            quality_status="valid",
            evidence_count=1,
        ))
        session.add(AdjustmentFactorInstrumentStatusDB(
            instrument_id="000001.SZ",
            series_version="v1__staging__unit",
            source="akshare",
            coverage_status="complete_with_events",
            event_count=1,
        ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1__staging__unit",
            status="validated_staging",
            instrument_count=1,
            row_count=1,
            report_json=json.dumps({
                "candidate_promotion_eligible": True,
                "blocking_quality_gates": {
                    "full_market_scope": True,
                    "no_cninfo_pending_factor_events": True,
                    "no_historical_factor_gaps": True,
                    "no_unverified_tdx_fallback": True,
                    "no_blocked_selection_segments": True,
                    "candidate_write_success": True,
                },
                "row_count": 1,
                "instrument_count": 1,
                "blocked_segment_count": 0,
            }),
        ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1",
            status="promoted",
            promotion_eligible=True,
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    result = await operations.promote_canonical_adjustment_factor_series(
        staging_series_version="v1__staging__unit",
        target_series_version="v1",
    )

    async with session_factory() as session:
        target_rows = (await session.execute(
            select(AdjustmentFactorCanonicalDB).where(
                AdjustmentFactorCanonicalDB.series_version == "v1"
            )
        )).scalars().all()
        target_status = await session.get(AdjustmentFactorSeriesStatusDB, "v1")
        instrument_status = (await session.execute(
            select(AdjustmentFactorInstrumentStatusDB).where(
                AdjustmentFactorInstrumentStatusDB.series_version == "v1"
            )
        )).scalar_one()

    assert result == {"canonical_rows": 1, "instrument_statuses": 1}
    assert len(target_rows) == 1
    assert target_rows[0].factor == pytest.approx(1.02)
    assert target_status.status == "promoted"
    assert target_status.promotion_eligible is True
    assert instrument_status.coverage_status == "complete_with_events"
    await engine.dispose()


@pytest.mark.asyncio
async def test_promote_canonical_series_rejects_persisted_count_mismatch():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorCanonicalDB.__table__,
                AdjustmentFactorInstrumentStatusDB.__table__,
                AdjustmentFactorSeriesStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ",
            symbol="000001",
            name="Ping An Bank",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            is_active=True,
        ))
        session.add(AdjustmentFactorCanonicalDB(
            instrument_id="000001.SZ",
            ex_date=datetime(2020, 5, 28),
            series_version="v1__staging__bad",
            factor=1.02,
            cumulative_factor=1.02,
            selected_source="cninfo",
            source_profile="unit",
            quality_status="high",
            evidence_count=2,
        ))
        session.add(AdjustmentFactorInstrumentStatusDB(
            instrument_id="000001.SZ",
            series_version="v1__staging__bad",
            source="cninfo",
            coverage_status="complete_with_events",
            event_count=1,
        ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1__staging__bad",
            status="validated_staging",
            report_json=json.dumps(_candidate_report(
                full_market=True,
                row_count=2,
                instrument_count=1,
            )),
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()

    with pytest.raises(RuntimeError, match="row count mismatch"):
        await operations.promote_canonical_adjustment_factor_series(
            staging_series_version="v1__staging__bad",
            target_series_version="v1",
        )
    await engine.dispose()


@pytest.mark.asyncio
async def test_incremental_merge_replaces_only_expected_instrument():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorCanonicalDB.__table__,
                AdjustmentFactorInstrumentStatusDB.__table__,
                AdjustmentFactorSeriesStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        for instrument_id in ("000001.SZ", "000002.SZ"):
            session.add(InstrumentDB(
                instrument_id=instrument_id,
                symbol=instrument_id[:6],
                name=instrument_id,
                exchange="SZSE",
                type="stock",
                currency="CNY",
                is_active=True,
            ))
        for instrument_id, factor in (
            ("000001.SZ", 1.01),
            ("000002.SZ", 1.02),
        ):
            session.add(AdjustmentFactorCanonicalDB(
                instrument_id=instrument_id,
                ex_date=datetime(2020, 5, 28),
                series_version="v1",
                factor=factor,
                cumulative_factor=factor,
                selected_source="cninfo",
                source_profile="unit",
                quality_status="high",
                evidence_count=2,
            ))
            session.add(AdjustmentFactorInstrumentStatusDB(
                instrument_id=instrument_id,
                series_version="v1",
                source="cninfo",
                coverage_status="complete_with_events",
                event_count=1,
            ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1",
            status="promoted",
            promotion_eligible=True,
            instrument_count=2,
            row_count=2,
            report_json=json.dumps({
                "status": "promoted",
                "promotion_eligible": True,
                "row_count": 2,
                "instrument_count": 2,
                "decisions": [],
            }),
        ))
        session.add(AdjustmentFactorCanonicalDB(
            instrument_id="000001.SZ",
            ex_date=datetime(2021, 5, 28),
            series_version="v1__staging__daily",
            factor=1.05,
            cumulative_factor=1.05,
            selected_source="tdx",
            source_profile="unit",
            quality_status="independent_consensus",
            evidence_count=2,
        ))
        session.add(AdjustmentFactorInstrumentStatusDB(
            instrument_id="000001.SZ",
            series_version="v1__staging__daily",
            source="tdx",
            coverage_status="complete_with_events",
            event_count=1,
        ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1__staging__daily",
            status="validated_incremental_staging",
            instrument_count=1,
            row_count=1,
            report_json=json.dumps(_candidate_report(
                full_market=False,
                row_count=1,
                instrument_count=1,
            )),
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    result = await operations.merge_canonical_adjustment_factor_subset(
        staging_series_version="v1__staging__daily",
        target_series_version="v1",
        expected_instrument_ids=["000001.SZ"],
    )

    async with session_factory() as session:
        rows = (await session.execute(
            select(AdjustmentFactorCanonicalDB).where(
                AdjustmentFactorCanonicalDB.series_version == "v1"
            ).order_by(AdjustmentFactorCanonicalDB.instrument_id)
        )).scalars().all()
        status = await session.get(AdjustmentFactorSeriesStatusDB, "v1")

    assert result["target_row_count"] == 2
    assert [(row.instrument_id, row.factor) for row in rows] == [
        ("000001.SZ", pytest.approx(1.05)),
        ("000002.SZ", pytest.approx(1.02)),
    ]
    assert status.status == "promoted"
    assert status.promotion_eligible is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_incremental_merge_rejects_coverage_end_regression():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[
                InstrumentDB.__table__,
                AdjustmentFactorCanonicalDB.__table__,
                AdjustmentFactorInstrumentStatusDB.__table__,
                AdjustmentFactorSeriesStatusDB.__table__,
            ],
        )
    async with session_factory() as session:
        session.add(AdjustmentFactorCanonicalDB(
            instrument_id="000001.SZ",
            ex_date=datetime(2025, 5, 28),
            series_version="v1",
            factor=1.02,
            cumulative_factor=1.02,
            selected_source="cninfo",
            source_profile="unit",
            quality_status="high",
            evidence_count=2,
        ))
        session.add(AdjustmentFactorInstrumentStatusDB(
            instrument_id="000001.SZ",
            series_version="v1",
            source="cninfo",
            coverage_status="complete_with_events",
            event_count=1,
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2025, 5, 28),
        ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1",
            status="promoted",
            promotion_eligible=True,
            instrument_count=1,
            row_count=1,
            report_json=json.dumps({
                "status": "promoted",
                "promotion_eligible": True,
                "row_count": 1,
                "instrument_count": 1,
            }),
        ))
        session.add(AdjustmentFactorCanonicalDB(
            instrument_id="000001.SZ",
            ex_date=datetime(2024, 5, 28),
            series_version="v1__staging__regression",
            factor=1.03,
            cumulative_factor=1.03,
            selected_source="cninfo",
            source_profile="unit",
            quality_status="high",
            evidence_count=2,
        ))
        session.add(AdjustmentFactorInstrumentStatusDB(
            instrument_id="000001.SZ",
            series_version="v1__staging__regression",
            source="cninfo",
            coverage_status="complete_with_events",
            event_count=1,
            start_date=datetime(2021, 1, 1),
            end_date=datetime(2024, 5, 28),
        ))
        session.add(AdjustmentFactorSeriesStatusDB(
            series_version="v1__staging__regression",
            status="validated_incremental_staging",
            instrument_count=1,
            row_count=1,
            report_json=json.dumps(_candidate_report(
                full_market=False,
                row_count=1,
                instrument_count=1,
            )),
        ))
        await session.commit()

    operations = DatabaseOperations(auto_initialize=False)
    operations.get_async_session = lambda: session_factory()
    with pytest.raises(RuntimeError, match="coverage end regresses") as exc_info:
        await operations.merge_canonical_adjustment_factor_subset(
            staging_series_version="v1__staging__regression",
            target_series_version="v1",
            expected_instrument_ids=["000001.SZ"],
        )
    assert "coverage start regresses" in str(exc_info.value)

    async with session_factory() as session:
        row = (await session.execute(
            select(AdjustmentFactorCanonicalDB).where(
                AdjustmentFactorCanonicalDB.series_version == "v1"
            )
        )).scalar_one()
        assert row.ex_date == datetime(2025, 5, 28)
    await engine.dispose()
