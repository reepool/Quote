from sqlalchemy import create_engine, text

from database.connection import DatabaseManager
from database.models import InstrumentDB


def test_corporate_action_status_migration_adds_range_unique_key(tmp_path):
    db_path = tmp_path / "corporate_action_migration.db"
    manager = DatabaseManager(str(db_path))
    manager.sync_engine = create_engine(f"sqlite:///{db_path}")

    with manager.sync_engine.begin() as connection:
        InstrumentDB.__table__.create(bind=connection)
        connection.execute(
            text(
                """
            CREATE TABLE corporate_action_instrument_status (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                instrument_id VARCHAR(32) NOT NULL,
                source VARCHAR(32) NOT NULL,
                source_profile VARCHAR(64) NOT NULL,
                coverage_status VARCHAR(32) NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                missing_ex_date_count INTEGER NOT NULL DEFAULT 0,
                requested_start_date DATETIME,
                requested_end_date DATETIME,
                earliest_event_date DATETIME,
                latest_event_date DATETIME,
                error_message TEXT,
                ingestion_run_id VARCHAR(64),
                last_attempt_at DATETIME,
                created_at DATETIME,
                updated_at DATETIME,
                CONSTRAINT uq_corporate_action_instrument_source_profile
                    UNIQUE (instrument_id, source, source_profile),
                FOREIGN KEY(instrument_id) REFERENCES instruments (instrument_id)
            )
        """
            )
        )
        connection.execute(
            text(
                """
            INSERT INTO corporate_action_instrument_status (
                instrument_id, source, source_profile, coverage_status,
                requested_start_date, requested_end_date
            ) VALUES (
                '000001.SZ', 'cninfo', 'cninfo_dividend',
                'complete_with_events', '1990-01-01', '2026-12-31'
            )
        """
            )
        )

    manager._ensure_corporate_action_governance_schema()

    with manager.sync_engine.connect() as connection:
        unique_indexes = []
        for index_row in connection.execute(
            text("PRAGMA index_list(corporate_action_instrument_status)")
        ).fetchall():
            if index_row[2]:
                unique_indexes.append(
                    tuple(
                        row[2]
                        for row in connection.execute(
                            text(f"PRAGMA index_info('{index_row[1]}')")
                        ).fetchall()
                    )
                )
        migrated = (
            connection.execute(
                text(
                    "SELECT coverage_status, requested_start_date, requested_end_date "
                    "FROM corporate_action_instrument_status"
                )
            )
            .mappings()
            .one()
        )

    assert (
        "instrument_id",
        "source",
        "source_profile",
        "requested_start_date",
        "requested_end_date",
    ) in unique_indexes
    assert migrated["coverage_status"] == "complete_with_events"
    assert str(migrated["requested_start_date"]).startswith("1990-01-01")
    assert str(migrated["requested_end_date"]).startswith("2026-12-31")
    manager.sync_engine.dispose()
