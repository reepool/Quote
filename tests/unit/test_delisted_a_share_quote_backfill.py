import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest

from data_manager import DataManager
from scheduler.tasks import ScheduledTasks


def _candidate(
    instrument_id: str,
    *,
    listed_date: date = date(2000, 1, 3),
    delisted_date: date = date(2001, 5, 8),
    quote_rows: int = 0,
    coverage_status: str = "missing",
) -> dict:
    return {
        "instrument_id": instrument_id,
        "symbol": instrument_id.split(".")[0],
        "name": f"退市样本{instrument_id}",
        "exchange": "SZSE" if instrument_id.endswith(".SZ") else "SSE",
        "listed_date": listed_date,
        "delisted_date": delisted_date,
        "status": "delisted",
        "is_active": False,
        "trading_status": 0,
        "source_symbol": instrument_id.split(".")[0],
        "quote_rows": quote_rows,
        "first_quote_date": None,
        "last_quote_date": None,
        "coverage_status": coverage_status,
    }


class FakeDelistedDbOps:
    def __init__(self, candidates: list[dict]):
        self.candidates = candidates
        self.candidate_kwargs = None
        self.coverage_kwargs = None
        self.saved_rows: list[dict] = []

    async def get_delisted_a_share_quote_backfill_candidates(self, **kwargs):
        self.candidate_kwargs = kwargs
        limit = kwargs.get("limit")
        if limit:
            return self.candidates[:limit]
        return list(self.candidates)

    async def get_delisted_a_share_quote_coverage_by_year(self, **kwargs):
        self.coverage_kwargs = kwargs
        return [
            {
                "delisted_year": "2001",
                "instrument_count": len(self.candidates),
                "with_quotes_count": 0,
                "no_quotes_count": len(self.candidates),
                "covered_count": 0,
                "uncovered_count": len(self.candidates),
                "first_quote_date": None,
                "last_quote_date": None,
            }
        ]

    async def save_daily_quotes(self, rows):
        self.saved_rows.extend(rows)
        return True


class FakeSourceFactory:
    def __init__(self, responses: dict[str, object]):
        self.responses = responses
        self.calls: list[dict] = []

    async def get_daily_data(
        self,
        exchange,
        instrument_id,
        symbol,
        start_date,
        end_date,
        *,
        instrument_type,
        source_symbol,
    ):
        self.calls.append(
            {
                "exchange": exchange,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "instrument_type": instrument_type,
                "source_symbol": source_symbol,
            }
        )
        response = self.responses.get(instrument_id, [])
        if isinstance(response, Exception):
            raise response
        if response == "sleep":
            await asyncio.sleep(0.05)
            return []
        return response


def _manager(candidates: list[dict], responses: dict[str, object] | None = None) -> DataManager:
    manager = DataManager.__new__(DataManager)
    manager.db_ops = FakeDelistedDbOps(candidates)
    manager.source_factory = FakeSourceFactory(responses or {})
    return manager


@pytest.mark.asyncio
async def test_delisted_backfill_dry_run_does_not_fetch_or_save():
    manager = _manager([_candidate("000508.SZ")])

    result = await manager.run_delisted_a_share_quote_backfill(
        delisted_year_start=1999,
        delisted_year_end=2024,
        limit=10,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["target_count"] == 1
    assert result["saved_rows"] == 0
    assert manager.source_factory.calls == []
    assert manager.db_ops.saved_rows == []
    assert manager.db_ops.candidate_kwargs["delisted_year_start"] == 1999
    assert manager.db_ops.candidate_kwargs["delisted_year_end"] == 2024


@pytest.mark.asyncio
async def test_delisted_backfill_fetches_lifecycle_window_and_saves():
    rows = [
        {"instrument_id": "000508.SZ", "time": datetime(2000, 1, 3), "close": 10.0},
        {"instrument_id": "000508.SZ", "time": datetime(2001, 5, 8), "close": 9.5},
    ]
    manager = _manager([_candidate("000508.SZ")], {"000508.SZ": rows})

    result = await manager.run_delisted_a_share_quote_backfill(dry_run=False)

    assert result["status"] == "success"
    assert result["processed_count"] == 1
    assert result["saved_rows"] == 2
    assert manager.db_ops.saved_rows == rows
    call = manager.source_factory.calls[0]
    assert call["instrument_id"] == "000508.SZ"
    assert call["start_date"].date() == date(2000, 1, 3)
    assert call["end_date"].date() == date(2001, 5, 8)
    assert call["instrument_type"] == "stock"


@pytest.mark.asyncio
async def test_delisted_backfill_reports_empty_failures_and_timeouts():
    manager = _manager(
        [
            _candidate("000001.SZ"),
            _candidate("600625.SH"),
            _candidate("000588.SZ"),
        ],
        {
            "000001.SZ": [],
            "600625.SH": RuntimeError("source rejected"),
            "000588.SZ": "sleep",
        },
    )

    result = await manager.run_delisted_a_share_quote_backfill(
        dry_run=False,
        per_instrument_timeout_sec=0.01,
    )

    assert result["status"] == "warning"
    assert result["processed_count"] == 3
    assert result["source_empty_count"] == 1
    assert result["failure_count"] == 2
    assert result["timeout_count"] == 1
    assert result["saved_rows"] == 0
    assert len(result["samples"]["source_empty"]) == 1
    assert len(result["samples"]["failures"]) == 2


@pytest.mark.asyncio
async def test_delisted_coverage_summary_counts_candidate_statuses():
    manager = _manager([
        _candidate("000508.SZ", coverage_status="missing"),
        _candidate("600625.SH", quote_rows=100, coverage_status="partial"),
    ])

    result = await manager.get_delisted_a_share_quote_backfill_coverage(
        exchanges=["SSE", "SZSE"],
        delisted_year_start=1999,
        delisted_year_end=2024,
        sample_limit=1,
    )

    assert result["status"] == "success"
    assert result["target_count"] == 2
    assert result["coverage_status_counts"] == {"missing": 1, "partial": 1}
    assert len(result["samples"]) == 1
    assert manager.db_ops.coverage_kwargs["delisted_year_end"] == 2024


@pytest.mark.asyncio
async def test_scheduler_delisted_backfill_delegates_to_data_manager(monkeypatch):
    task = ScheduledTasks.__new__(ScheduledTasks)
    task._active_tasks = set()
    task.telegram_enabled = False

    mock_run = AsyncMock(return_value={"status": "dry_run", "target_count": 288})
    monkeypatch.setattr("scheduler.tasks.data_manager.run_delisted_a_share_quote_backfill", mock_run)

    result = await task.delisted_a_share_quote_backfill(
        exchanges=["SSE", "SZSE"],
        delisted_year_start=1999,
        delisted_year_end=2024,
        delisted_start_date=None,
        delisted_end_date=None,
        instrument_ids=["000508.SZ"],
        limit=1,
        dry_run=True,
        per_instrument_timeout_sec=120,
        fail_fast=True,
    )

    assert result["status"] == "dry_run"
    mock_run.assert_awaited_once_with(
        exchanges=["SSE", "SZSE"],
        delisted_year_start=1999,
        delisted_year_end=2024,
        delisted_start_date=None,
        delisted_end_date=None,
        instrument_ids=["000508.SZ"],
        limit=1,
        dry_run=True,
        per_instrument_timeout_sec=120,
        fail_fast=True,
    )
    assert "delisted_a_share_quote_backfill" not in task._active_tasks
