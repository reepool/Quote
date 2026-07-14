from datetime import datetime

import pytest
from sqlalchemy import text

from database.connection import DatabaseManager
from database.models import AdjustmentFactorDB, DataChangeLogDB, DailyQuoteDB, InstrumentDB
from database.operations import DatabaseOperations


async def _ops_for_tmp_db(tmp_path):
    manager = DatabaseManager(str(tmp_path / "quote_cdc.db"))
    manager.initialize()
    manager.create_tables()
    ops = DatabaseOperations(auto_initialize=False)
    ops.db = manager
    ops.engine = manager.sync_engine
    ops.async_engine = manager.async_engine
    ops.SessionLocal = manager.SessionLocal
    ops.AsyncSessionLocal = manager.TaskAsyncSessionLocal
    return manager, ops


def _quote(close=10.5, batch_id="b1"):
    return {
        "time": datetime(2026, 7, 10),
        "instrument_id": "000001.SZ",
        "open": 10.0,
        "high": 11.0,
        "low": 9.8,
        "close": close,
        "volume": 1000,
        "amount": 10500.0,
        "turnover": 1.2,
        "pre_close": 10.0,
        "change": close - 10.0,
        "pct_change": (close - 10.0) / 10.0,
        "tradestatus": 1,
        "factor": 1.0,
        "adjustment_type": "none",
        "is_complete": True,
        "quality_score": 1.0,
        "source": "unit",
        "batch_id": batch_id,
    }


def _factor(cumulative_factor=1.0):
    return {
        "instrument_id": "000001.SZ",
        "ex_date": datetime(2026, 7, 10),
        "factor": 1.0,
        "cumulative_factor": cumulative_factor,
        "dividend": 0.0,
        "bonus_shares": 0.0,
        "rights_shares": 0.0,
        "rights_price": 0.0,
        "event_type": "dividend",
        "source": "unit",
    }


def _seed_instrument(manager):
    with manager.get_session() as session:
        session.add(
            InstrumentDB(
                instrument_id="000001.SZ",
                symbol="000001",
                name="Ping An Bank",
                exchange="SZSE",
                type="stock",
                currency="CNY",
                source="unit",
            )
        )
        session.commit()


def test_change_watermark_schema_guard_preserves_existing_rows(tmp_path):
    manager = DatabaseManager(str(tmp_path / "legacy.db"))
    manager.initialize()
    with manager.sync_engine.begin() as conn:
        conn.execute(text("CREATE TABLE daily_quotes (time DATETIME NOT NULL, instrument_id VARCHAR(32) NOT NULL, close FLOAT, PRIMARY KEY (time, instrument_id))"))
        conn.execute(text("CREATE TABLE adjustment_factors (id INTEGER PRIMARY KEY AUTOINCREMENT, instrument_id VARCHAR(32) NOT NULL, ex_date DATETIME NOT NULL, factor FLOAT)"))
        conn.execute(text("INSERT INTO daily_quotes(time, instrument_id, close) VALUES ('2026-07-10', '000001.SZ', 10.5)"))

    manager._ensure_change_watermark_schema()

    with manager.sync_engine.connect() as conn:
        quote_columns = {row[1] for row in conn.execute(text("PRAGMA table_info(daily_quotes)"))}
        factor_columns = {row[1] for row in conn.execute(text("PRAGMA table_info(adjustment_factors)"))}
        row_count = conn.execute(text("SELECT COUNT(*) FROM daily_quotes")).scalar()
        changelog_exists = conn.execute(
            text("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='data_change_log'")
        ).scalar()

    assert {"row_hash", "row_version"} <= quote_columns
    assert {"row_hash", "row_version"} <= factor_columns
    assert row_count == 1
    assert changelog_exists == 1
    manager.close()


@pytest.mark.asyncio
async def test_daily_quote_cdc_ignores_overlap_and_records_material_changes(tmp_path):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)

        insert_stats = await ops.save_daily_quotes([_quote()], return_stats=True)
        duplicate_stats = await ops.save_daily_quotes([_quote(batch_id="b2")], return_stats=True)
        changed_stats = await ops.save_daily_quotes([_quote(close=10.8, batch_id="b3")], return_stats=True)

        changes = await ops.get_data_changes(domain="quotes", dataset="daily_quotes", since_sequence=0)

        assert insert_stats["inserted"] == 1
        assert insert_stats["changelog_written"] == 1
        assert duplicate_stats["unchanged"] == 1
        assert duplicate_stats["changelog_written"] == 0
        assert changed_stats["changed"] == 1
        assert changed_stats["changelog_written"] == 1
        assert changes["count"] == 2
        assert [row["change_type"] for row in changes["changes"]] == ["insert", "update"]

        with manager.get_session() as session:
            quote = session.query(DailyQuoteDB).one()
            changelog_count = session.query(DataChangeLogDB).count()
            assert quote.row_version == 2
            assert quote.close == 10.8
            assert changelog_count == 2
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_daily_quote_cdc_rollback_reports_no_persisted_writes(
    tmp_path,
    monkeypatch,
):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        class EmptyScalars:
            @staticmethod
            def all():
                return []

        class EmptyResult:
            @staticmethod
            def scalars():
                return EmptyScalars()

        class FailingSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def execute(self, statement):
                return EmptyResult()

            def add(self, row):
                return None

            async def commit(self):
                raise RuntimeError("forced commit failure")

        monkeypatch.setattr(ops, "get_async_session", lambda: FailingSession())
        stats = await ops.save_daily_quotes([_quote()], return_stats=True)

        assert stats == {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "skipped": 0,
            "failed": 1,
            "changelog_written": 0,
        }
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_daily_quote_cdc_collapses_duplicate_batch_keys_and_keeps_valid_rows(tmp_path):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)
        malformed = {**_quote(), "instrument_id": "MALFORMED.SZ", "close": "bad"}

        stats = await ops.save_daily_quotes(
            [_quote(close=10.5), malformed, _quote(close=10.8, batch_id="last")],
            return_stats=True,
        )

        assert stats == {
            "inserted": 1,
            "changed": 0,
            "unchanged": 0,
            "skipped": 1,
            "failed": 1,
            "changelog_written": 1,
        }
        with manager.get_session() as session:
            quote = session.query(DailyQuoteDB).one()
            change = session.query(DataChangeLogDB).one()
        assert quote.close == 10.8
        assert quote.batch_id == "last"
        assert change.change_type == "insert"
        assert change.row_version == 1
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_adjustment_factor_cdc_uses_separate_domain(tmp_path):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)

        insert_stats = await ops.save_adjustment_factors([_factor()], return_stats=True)
        duplicate_stats = await ops.save_adjustment_factors([_factor()], return_stats=True)
        restated_stats = await ops.save_adjustment_factors(
            [_factor(cumulative_factor=1.2)],
            return_stats=True,
        )

        changes = await ops.get_data_changes(domain="adjustment_factor", since_sequence=0)

        assert insert_stats["inserted"] == 1
        assert duplicate_stats["unchanged"] == 1
        assert duplicate_stats["changelog_written"] == 0
        assert restated_stats["changed"] == 1
        assert changes["count"] == 2
        assert all(row["domain"] == "adjustment_factor" for row in changes["changes"])
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_adjustment_factor_cdc_rollback_reports_no_persisted_writes(
    tmp_path,
    monkeypatch,
):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        class EmptyResult:
            @staticmethod
            def scalar_one_or_none():
                return None

        class FailingSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def execute(self, statement):
                return EmptyResult()

            def add(self, row):
                return None

            async def commit(self):
                raise RuntimeError("forced commit failure")

        monkeypatch.setattr(ops, "get_async_session", lambda: FailingSession())
        stats = await ops.save_adjustment_factors([_factor()], return_stats=True)

        assert stats == {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "skipped": 0,
            "failed": 1,
            "changelog_written": 0,
        }
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_latest_watermark_supports_empty_and_multiple_domains(tmp_path):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        empty = await ops.get_change_watermark(domain="quotes")
        assert empty["latest_sequence"] == 0
        assert empty["is_empty"] is True

        _seed_instrument(manager)
        await ops.save_daily_quotes([_quote()], return_stats=True)
        await ops.save_adjustment_factors([_factor()], return_stats=True)

        quote_watermark = await ops.get_change_watermark(domain="quotes")
        factor_watermark = await ops.get_change_watermark(domain="adjustment_factor")
        all_watermark = await ops.get_change_watermark()

        assert quote_watermark["latest_sequence"] == 1
        assert quote_watermark["is_empty"] is False
        assert factor_watermark["latest_sequence"] == 2
        assert factor_watermark["is_empty"] is False
        assert all_watermark["latest_sequence"] == 2
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_latest_watermark_does_not_mask_database_failures(tmp_path, monkeypatch):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        def fail_session():
            raise RuntimeError("watermark database unavailable")

        monkeypatch.setattr(ops, "get_async_session", fail_session)

        with pytest.raises(RuntimeError, match="watermark database unavailable"):
            await ops.get_change_watermark(domain="quotes")
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_changelog_domain_can_be_disabled_without_blocking_source_writes(tmp_path, monkeypatch):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)
        monkeypatch.setattr(
            ops.config_manager,
            "get_nested",
            lambda path, default=None: {
                "enabled": True,
                "domains": {"quotes": False},
                "datasets": {"daily_quotes": True},
            } if path == "database_config.change_watermark" else default,
        )

        stats = await ops.save_daily_quotes([_quote()], return_stats=True)
        changes = await ops.get_data_changes(domain="quotes", dataset="daily_quotes", since_sequence=0)

        assert stats["inserted"] == 1
        assert stats["changelog_written"] == 0
        assert changes["count"] == 0
        with manager.get_session() as session:
            quote = session.query(DailyQuoteDB).one()
            assert quote.close == 10.5
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_changelog_global_disable_keeps_source_writes_without_watermark(tmp_path, monkeypatch):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)
        monkeypatch.setattr(
            ops.config_manager,
            "get_nested",
            lambda path, default=None: {"enabled": False}
            if path == "database_config.change_watermark" else default,
        )

        quote_stats = await ops.save_daily_quotes([_quote()], return_stats=True)
        factor_stats = await ops.save_adjustment_factors([_factor()], return_stats=True)
        changes = await ops.get_data_changes(since_sequence=0)

        assert quote_stats["inserted"] == 1
        assert quote_stats["changelog_written"] == 0
        assert factor_stats["inserted"] == 1
        assert factor_stats["changelog_written"] == 0
        assert changes["count"] == 0
        with manager.get_session() as session:
            assert session.query(DailyQuoteDB).count() == 1
            assert session.query(AdjustmentFactorDB).count() == 1
            assert session.query(DataChangeLogDB).count() == 0
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_change_watermark_invalid_config_falls_back_to_safe_defaults(tmp_path, monkeypatch):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)
        await ops.save_daily_quotes([_quote()], return_stats=True)
        monkeypatch.setattr(
            ops.config_manager,
            "get_nested",
            lambda path, default=None: {
                "enabled": True,
                "default_limit": "bad",
                "max_limit": "bad",
                "domains": "bad",
                "datasets": "bad",
            } if path == "database_config.change_watermark" else default,
        )

        changes = await ops.get_data_changes(since_sequence=0, limit=10000)
        health = await ops.get_change_watermark_health()

        assert changes["limit"] == 5000
        assert changes["count"] == 1
        assert health["default_limit"] == 1000
        assert health["max_limit"] == 5000
        assert health["domains"] == {"quotes": True, "adjustment_factor": True}
        assert health["datasets"] == {"daily_quotes": True, "adjustment_factors": True}
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_change_query_uses_configured_default_limit_when_omitted(tmp_path, monkeypatch):
    manager, ops = await _ops_for_tmp_db(tmp_path)
    try:
        _seed_instrument(manager)
        await ops.save_daily_quotes([_quote(close=10.5, batch_id="b1")], return_stats=True)
        await ops.save_daily_quotes([_quote(close=10.8, batch_id="b2")], return_stats=True)
        await ops.save_daily_quotes([_quote(close=10.9, batch_id="b3")], return_stats=True)
        monkeypatch.setattr(
            ops.config_manager,
            "get_nested",
            lambda path, default=None: {
                "enabled": True,
                "default_limit": 1,
                "max_limit": 2,
            } if path == "database_config.change_watermark" else default,
        )

        changes = await ops.get_data_changes(since_sequence=0)
        capped = await ops.get_data_changes(since_sequence=0, limit=10)

        assert changes["limit"] == 1
        assert changes["count"] == 1
        assert changes["has_more"] is True
        assert capped["limit"] == 2
        assert capped["count"] == 2
        assert capped["has_more"] is True
    finally:
        await manager.close_async()
