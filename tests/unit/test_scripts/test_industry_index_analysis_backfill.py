from datetime import date

import pytest

from scripts.research_ops.industry_index_analysis_backfill import (
    iter_date_chunks,
    run_backfill,
)


class FakeIndexAnalysisBackfillManager:
    def __init__(self, *, fail_on=None):
        self.calls = []
        self.fail_on = fail_on or set()

    async def run_industry_index_analysis_backfill(self, **kwargs):
        self.calls.append(kwargs)
        key = (kwargs["start_date"], tuple(kwargs.get("index_types") or []))
        if key in self.fail_on:
            return {"status": "failed", "reason": "upstream timeout", "rows_written": 0}
        return {
            "status": "success",
            "rows_written": 3,
            "coverage": {
                "start_date": kwargs["start_date"],
                "end_date": kwargs["end_date"],
            },
        }


def test_iter_date_chunks_monthly_bounds():
    chunks = list(
        iter_date_chunks(
            date(2024, 1, 15),
            date(2024, 3, 5),
            frequency="month",
        )
    )

    assert [(start.isoformat(), end.isoformat()) for start, end in chunks] == [
        ("2024-01-15", "2024-01-31"),
        ("2024-02-01", "2024-02-29"),
        ("2024-03-01", "2024-03-05"),
    ]


def test_iter_date_chunks_daily_bounds():
    chunks = list(
        iter_date_chunks(
            date(2024, 1, 30),
            date(2024, 2, 1),
            frequency="day",
        )
    )

    assert [(start.isoformat(), end.isoformat()) for start, end in chunks] == [
        ("2024-01-30", "2024-01-30"),
        ("2024-01-31", "2024-01-31"),
        ("2024-02-01", "2024-02-01"),
    ]


@pytest.mark.asyncio
async def test_run_backfill_splits_by_month_and_index_type():
    manager = FakeIndexAnalysisBackfillManager()

    result = await run_backfill(
        manager,
        start_date="2024-01-30",
        end_date="2024-02-02",
        index_types=["一级行业", "二级行业"],
    )

    assert result["status"] == "success"
    assert result["chunks_total"] == 4
    assert result["rows_written"] == 12
    assert [call["start_date"] for call in manager.calls] == [
        "2024-01-30",
        "2024-01-30",
        "2024-02-01",
        "2024-02-01",
    ]
    assert [call["index_types"] for call in manager.calls] == [
        ["一级行业"],
        ["二级行业"],
        ["一级行业"],
        ["二级行业"],
    ]


@pytest.mark.asyncio
async def test_run_backfill_continues_after_chunk_failure_by_default():
    manager = FakeIndexAnalysisBackfillManager(
        fail_on={("2024-01-01", ("二级行业",))}
    )

    result = await run_backfill(
        manager,
        start_date="2024-01-01",
        end_date="2024-01-02",
        index_types=["一级行业", "二级行业", "三级行业"],
    )

    assert result["status"] == "partial_success"
    assert result["chunks_failed"] == 1
    assert result["rows_written"] == 6
    assert len(manager.calls) == 3


@pytest.mark.asyncio
async def test_run_backfill_can_stop_on_error():
    manager = FakeIndexAnalysisBackfillManager(
        fail_on={("2024-01-01", ("二级行业",))}
    )

    result = await run_backfill(
        manager,
        start_date="2024-01-01",
        end_date="2024-01-02",
        index_types=["一级行业", "二级行业", "三级行业"],
        stop_on_error=True,
    )

    assert result["status"] == "partial_success"
    assert result["chunks_failed"] == 1
    assert len(manager.calls) == 2
