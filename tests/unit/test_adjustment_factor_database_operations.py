import asyncio
from datetime import date, datetime
import sqlite3

import pytest
from sqlalchemy import select
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
        report={
            "instrument_count": 1,
            "row_count": 1,
            "coverage_ratio": 1.0,
            "conflict_count": 0,
            "promotion_eligible": True,
        },
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
