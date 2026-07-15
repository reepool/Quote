from contextlib import asynccontextmanager
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from database.operations import DatabaseOperations


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _FakeSession:
    def __init__(self, existing):
        self.existing = existing
        self.add = Mock()
        self.commit = AsyncMock()

    async def execute(self, _statement):
        return _ScalarResult(self.existing)


@pytest.mark.asyncio
async def test_raw_tdx_upsert_preserves_computed_fields():
    existing = SimpleNamespace(
        factor=1.25,
        cumulative_factor=2.5,
        pre_close=10.0,
        fenhong=0.0,
        songzhuangu=0.0,
        peigu=0.0,
        peigujia=0.0,
        validation_result="all_pass",
        ref_factor=1.24,
        ref_source="baostock",
        ratio_diff_pct=0.1,
        conflict_reason=None,
        source="tdx_xdxr",
    )
    session = _FakeSession(existing)
    operations = DatabaseOperations.__new__(DatabaseOperations)
    operations.db_logger = Mock()

    @asynccontextmanager
    async def get_session():
        yield session

    operations.get_async_session = get_session
    saved = await operations.save_tdx_audit_factors(
        [{
            "instrument_id": "600000.SH",
            "ex_date": datetime(2020, 6, 1),
            "factor": 1.0,
            "cumulative_factor": 1.0,
            "pre_close": 0.0,
            "fenhong": 2.0,
            "songzhuangu": 1.0,
            "peigu": 0.0,
            "peigujia": 0.0,
            "validation_result": "pending_factor_missing_pre_close",
        }],
        preserve_computed_fields=True,
    )

    assert saved == 1
    assert existing.fenhong == 2.0
    assert existing.songzhuangu == 1.0
    assert existing.factor == 1.25
    assert existing.cumulative_factor == 2.5
    assert existing.pre_close == 10.0
    assert existing.validation_result == "all_pass"


class _FakeTdxSource:
    def __init__(self, factors=None):
        self.factors = factors or []

    async def get_xdxr_events(self, instrument_id):
        return [{
            "instrument_id": instrument_id,
            "date": datetime(2020, 6, 1),
            "category": 1,
            "fenhong": 2.0,
            "songzhuangu": 0.0,
            "peigu": 0.0,
            "peigujia": 0.0,
        }]

    async def get_adjustment_factors(self, *_args):
        return list(self.factors)


class _FakeSourceFactory:
    def __init__(self, source):
        self.source = source

    def _find_source_by_base_name(self, name):
        return self.source if name == "pytdx" else None


class _FakeDbOps:
    def __init__(self):
        self.saved_calls = []

    async def get_repair_universe_instruments(self, exchange, instrument_types=None):
        return [{
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": exchange,
            "type": "stock",
            "is_active": False,
            "listed_date": date(1999, 11, 10),
            "delisted_date": date(2021, 1, 1),
        }]

    async def execute_read_query(self, _sql, _params):
        return []

    async def save_tdx_audit_factors(self, rows, preserve_computed_fields=False):
        self.saved_calls.append((list(rows), preserve_computed_fields))
        return len(rows)


def _build_manager(source):
    manager = DataManager.__new__(DataManager)
    manager.db_ops = _FakeDbOps()
    manager.source_factory = _FakeSourceFactory(source)
    manager.filter_repair_universe = AsyncMock(return_value=([{
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "type": "stock",
        "is_active": False,
        "_repair_start_date": date(2020, 1, 1),
        "_repair_end_date": date(2020, 12, 31),
    }], {"eligible_instrument_count": 1}))
    return manager


@pytest.mark.asyncio
async def test_xdxr_history_keeps_inactive_event_pending_without_pre_close():
    manager = _build_manager(_FakeTdxSource())

    result = await manager.backfill_tdx_xdxr_history(
        exchanges=["SSE"],
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        derive_factors=True,
    )

    assert result["status"] == "success"
    assert result["totals"]["eligible_instruments"] == 1
    assert result["totals"]["saved_events"] == 1
    assert result["totals"]["pending_factors"] == 1
    raw_rows, preserve = manager.db_ops.saved_calls[0]
    assert preserve is True
    assert raw_rows[0]["fenhong"] == 2.0
    assert raw_rows[0]["validation_result"] == "pending_factor_missing_pre_close"


@pytest.mark.asyncio
async def test_xdxr_history_derivation_updates_pending_event():
    factor = {
        "instrument_id": "600000.SH",
        "ex_date": datetime(2020, 6, 1),
        "factor": 1.02,
        "cumulative_factor": 1.02,
        "pre_close": 10.0,
        "fenhong": 2.0,
        "songzhuangu": 0.0,
        "peigu": 0.0,
        "peigujia": 0.0,
    }
    manager = _build_manager(_FakeTdxSource([factor]))

    result = await manager.backfill_tdx_xdxr_history(
        exchanges=["SSE"],
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        derive_factors=True,
    )

    assert result["totals"]["derived_factors"] == 1
    assert result["totals"]["pending_factors"] == 0
    assert manager.db_ops.saved_calls[1][1] is False
    assert manager.db_ops.saved_calls[1][0][0]["validation_result"] == "computed_unvalidated"


@pytest.mark.asyncio
async def test_xdxr_history_dry_run_counts_events_and_factors_without_saving():
    factor = {
        "instrument_id": "600000.SH",
        "ex_date": datetime(2020, 6, 1),
        "factor": 1.02,
        "cumulative_factor": 1.02,
        "pre_close": 10.0,
        "fenhong": 2.0,
        "songzhuangu": 0.0,
        "peigu": 0.0,
        "peigujia": 0.0,
    }
    manager = _build_manager(_FakeTdxSource([factor]))

    result = await manager.backfill_tdx_xdxr_history(
        exchanges=["SSE"],
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        derive_factors=True,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["totals"]["raw_events"] == 1
    assert result["totals"]["saved_events"] == 0
    assert result["totals"]["derived_factors"] == 1
    assert result["totals"]["pending_factors"] == 0
    assert manager.db_ops.saved_calls == []
