"""Survey tests: windows, audit-only writes, abort/resume semantics."""
from dataclasses import dataclass, field, replace
from datetime import date
from typing import List, Tuple

import pytest

from research.financial_disclosure_date_survey import (
    SURVEY_PURPOSE_KEY,
    build_survey_windows,
    load_checkpoint,
    save_checkpoint,
    survey_historical_disclosure_dates,
)


@dataclass(frozen=True)
class _Record:
    source: str = "cninfo"
    announcement_key: str = "key-1"
    source_announcement_id: str = "src-1"
    symbols: Tuple[str, ...] = ("688981",)
    exchange: str = "SSE"
    market: str = "SSE"
    published_at: str = "2024-10-31T16:00:00+00:00"
    title: str = "中芯国际2024年第三季度报告"
    selection_reasons: Tuple[str, ...] = ()

    def with_selection_reasons(self, reasons) -> "_Record":
        return replace(self, selection_reasons=tuple(reasons))


class _Result:
    def __init__(self, status="success", records=(), errors=()):
        self.status = status
        self.records = records
        self.errors = errors


def _fake_acquire(by_start, probe_result=None):
    """Dispatch by scope.start_date; scope.max_pages == 1 hits probe_result."""
    calls = []

    async def acquire(query):
        calls.append(query)
        if getattr(query.scope, "max_pages", None) == 1 and probe_result is not None:
            return probe_result
        return by_start.get(query.scope.start_date, _Result())

    return acquire, calls


def _writer():
    written: List[Tuple[str, object, str]] = []

    def write(purpose_key, record, instrument_id):
        written.append((purpose_key, record, instrument_id))

    return write, written


def test_build_survey_windows_boundaries_and_tail():
    windows = build_survey_windows(
        ["SSE"],
        ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"],
        today=date(2026, 11, 4),
    )
    by_period = {w.report_period: w for w in windows}
    assert by_period["2024-03-31"].start_date == "2024-03-31"
    assert by_period["2024-03-31"].end_date == "2024-06-29"      # 04-30 + 60d
    assert by_period["2024-06-30"].start_date == "2024-06-30"
    assert by_period["2024-06-30"].end_date == "2024-10-30"      # 08-31 + 60d
    assert by_period["2024-09-30"].start_date == "2024-09-30"
    assert by_period["2024-09-30"].end_date == "2024-12-30"      # 10-31 + 60d
    assert by_period["2024-12-31"].start_date == "2024-12-31"
    assert by_period["2024-12-31"].end_date == "2025-06-29"      # next 04-30 + 60d


def test_build_survey_windows_drops_future_windows():
    windows = build_survey_windows(
        ["SSE"], ["2026-09-30"], today=date(2026, 11, 4)
    )
    # Q3 2026 deadline+tail = 2026-12-30, still in the future -> dropped
    assert windows == []


@pytest.mark.asyncio
async def test_survey_writes_audit_only_with_inferred_period(tmp_path):
    record = _Record(title="中芯国际2024年第三季度报告")          # -> 2024-09-30
    noise = _Record(title="中芯国际2024年半年度报告", announcement_key="k2")  # mismatch
    acquire, calls = _fake_acquire(
        {"2024-09-30": _Result(records=[record, noise])},
        probe_result=_Result(status="success_empty"),
    )
    write, written = _writer()
    cp = tmp_path / "checkpoint.json"

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-09-30"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={("SSE", "688981"): "688981.SH"},
        checkpoint_path=cp,
        probe_enabled=True,
    )

    assert report["aborted"] is False
    assert report["audit_written"] == 1
    assert len(written) == 1
    purpose, tagged, instrument_id = written[0]
    assert purpose == SURVEY_PURPOSE_KEY
    assert instrument_id == "688981.SH"
    reasons = tagged.selection_reasons
    assert "inferred_report_period=2024-09-30" in reasons
    # checkpoint marks the window completed
    assert load_checkpoint(cp)["completed_windows"] == ["SSE:2024-09-30"]
    # two acquires total: probe + one window (noise record not written)
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_survey_aborts_on_403_and_leaves_remaining_windows(tmp_path):
    acquire, calls = _fake_acquire(
        {
            "2024-03-31": _Result(records=[]),
            "2024-06-30": _Result(status="degraded", errors=["HTTP Error 403: Forbidden"]),
        }
    )
    write, written = _writer()
    cp = tmp_path / "checkpoint.json"

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-03-31", "2024-06-30", "2024-09-30"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={},
        checkpoint_path=cp,
        probe_enabled=False,
    )

    assert report["aborted"] is True
    assert "403" in (report["abort_reason"] or "")
    state = load_checkpoint(cp)
    assert state["completed_windows"] == ["SSE:2024-03-31"]
    assert state["failed_windows"] == ["SSE:2024-06-30"]
    # w3 never requested: w1 + w2 only
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_survey_resumes_from_checkpoint(tmp_path):
    save_checkpoint(
        tmp_path / "checkpoint.json",
        {"completed_windows": ["SSE:2024-03-31"], "failed_windows": []},
    )
    acquire, calls = _fake_acquire({})
    write, written = _writer()

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-03-31", "2024-06-30"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={},
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_enabled=False,
    )

    # the completed window was skipped: no acquire for it
    acquired_starts = [q.scope.start_date for q in calls]
    assert "2024-03-31" not in acquired_starts
    assert report["window_results"][0]["skipped"] == "checkpoint_completed"
    assert report["window_results"][1]["status"] == "success"


@pytest.mark.asyncio
async def test_survey_probe_failure_refuses_startup(tmp_path):
    acquire, calls = _fake_acquire(
        {}, probe_result=_Result(status="degraded", errors=["HTTP Error 403: Forbidden"])
    )
    write, written = _writer()

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-09-30"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={},
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_enabled=True,
    )

    assert report["aborted"] is True
    assert "probe failed" in (report["abort_reason"] or "")
    assert written == []
    # only the probe request was made; no window scans
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_survey_records_without_matching_symbol_skipped(tmp_path):
    record = _Record(symbols=("999999",))                      # not in map
    acquire, _ = _fake_acquire(
        {"2024-09-30": _Result(records=[record])}
    )
    write, written = _writer()

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-09-30"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={},                                    # empty map
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_enabled=False,
    )

    assert report["audit_written"] == 0
    assert written == []


@pytest.mark.asyncio
async def test_survey_degraded_window_is_incomplete_not_completed(tmp_path):
    """max_pages_exhausted (degraded) windows keep captured audit rows but
    are checkpointed as incomplete so a rerun with a higher page cap
    rescans them (truncation guard, found in implementation review)."""
    acquire, calls = _fake_acquire(
        {
            "2024-03-31": _Result(status="degraded", records=[_Record(title="某公司2024年第一季度报告")]),
            "2024-06-30": _Result(records=[]),
        }
    )
    write, written = _writer()
    cp = tmp_path / "checkpoint.json"

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-03-31", "2024-06-30"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={("SSE", "688981"): "688981.SH"},
        checkpoint_path=cp,
        probe_enabled=False,
    )

    # degraded window: not aborted, audit rows kept, but marked incomplete
    assert report["aborted"] is False
    assert report["audit_written"] == 1
    assert report["incomplete_windows"] == ["SSE:2024-03-31"]
    state = load_checkpoint(cp)
    assert state["completed_windows"] == ["SSE:2024-06-30"]
    assert state["incomplete_windows"] == ["SSE:2024-03-31"]
