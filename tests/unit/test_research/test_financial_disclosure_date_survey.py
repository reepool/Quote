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


def test_build_survey_windows_half_month_segments():
    windows = build_survey_windows(
        ["SSE"], ["2024-03-31"], today=date(2026, 11, 4)
    )
    # season 2024-03-31 .. 2024-06-29 splits into half-month segments,
    # each small enough for the cninfo effective-pagination ceiling
    spans = [(w.start_date, w.end_date) for w in windows]
    assert spans == [
        ("2024-03-31", "2024-03-31"),
        ("2024-04-01", "2024-04-15"),
        ("2024-04-16", "2024-04-30"),
        ("2024-05-01", "2024-05-15"),
        ("2024-05-16", "2024-05-31"),
        ("2024-06-01", "2024-06-15"),
        ("2024-06-16", "2024-06-29"),
    ]
    # keys are per-segment so checkpoints resume at half-month grain
    assert windows[0].key == "SSE:2024-03-31:2024-03-31~2024-03-31"
    assert windows[1].key == "SSE:2024-03-31:2024-04-01~2024-04-15"


def test_build_survey_windows_segment_never_crosses_season_tail():
    # FY season ends next-year 06-29; the last segment is clipped to it
    windows = build_survey_windows(
        ["SSE"], ["2023-12-31"], today=date(2026, 11, 4)
    )
    assert windows[-1].end_date == "2024-06-29"
    assert windows[0].start_date == "2023-12-31"
    # segment count sanity: Dec half + 6 months x 2 halves + clipped tail
    assert len(windows) == 13


def test_build_survey_windows_drops_future_windows():
    windows = build_survey_windows(
        ["SSE"], ["2026-09-30"], today=date(2026, 11, 4)
    )
    # Q3 2026 deadline+tail = 2026-12-30, still in the future -> dropped
    assert windows == []


@pytest.mark.asyncio
async def test_survey_writes_audit_only_with_inferred_period(tmp_path):
    record = _Record(title="中芯国际2024年第三季度报告")          # -> 2024-09-30
    # cross-season harvest: an annual report seen inside the Q3 window's
    # date range is tagged with its own period, not discarded
    cross = _Record(
        title="某公司2023年年度报告", announcement_key="k9", symbols=("600000",)
    )
    noise = _Record(title="中芯国际2024年半年度报告", announcement_key="k2")  # mismatch
    acquire, calls = _fake_acquire(
        {"2024-09-30": _Result(records=[record, cross, noise])},
        probe_result=_Result(status="success_empty"),
    )
    write, written = _writer()
    cp = tmp_path / "checkpoint.json"

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-09-30", "2023-12-31"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={
            ("SSE", "688981"): "688981.SH",
            ("SSE", "600000"): "600000.SH",
        },
        checkpoint_path=cp,
        probe_enabled=True,
    )

    assert report["aborted"] is False
    assert report["audit_written"] == 2
    assert len(written) == 2
    purpose, tagged, instrument_id = written[0]
    assert purpose == SURVEY_PURPOSE_KEY
    assert instrument_id == "688981.SH"
    reasons = tagged.selection_reasons
    assert "inferred_report_period=2024-09-30" in reasons
    # cross-season row keeps its own inferred period
    _, cross_tagged, cross_id = written[1]
    assert cross_id == "600000.SH"
    assert "inferred_report_period=2023-12-31" in cross_tagged.selection_reasons
    # checkpoint marks every half-month segment of the season completed
    completed = load_checkpoint(cp)["completed_windows"]
    assert completed
    assert any(k.startswith("SSE:2024-09-30:") for k in completed)
    assert any(k.startswith("SSE:2023-12-31:") for k in completed)
    # probe + one acquire per segment
    assert len(calls) == 1 + len(completed)


@pytest.mark.asyncio
async def test_survey_aborts_on_403_and_leaves_remaining_windows(tmp_path):
    acquire, calls = _fake_acquire(
        {
            "2024-09-30": _Result(records=[]),
            "2024-10-01": _Result(status="degraded", errors=["HTTP Error 403: Forbidden"]),
        }
    )
    write, written = _writer()
    cp = tmp_path / "checkpoint.json"

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-09-30", "2023-12-31"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={},
        checkpoint_path=cp,
        probe_enabled=False,
    )

    assert report["aborted"] is True
    assert "403" in (report["abort_reason"] or "")
    state = load_checkpoint(cp)
    assert state["completed_windows"] == ["SSE:2024-09-30:2024-09-30~2024-09-30"]
    assert state["failed_windows"] == ["SSE:2024-09-30:2024-10-01~2024-10-15"]
    # later segments never requested: first segment + refusing segment only
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_survey_resumes_from_checkpoint(tmp_path):
    from research.financial_disclosure_date_survey import build_survey_windows
    done_season_keys = [
        w.key for w in build_survey_windows(["SSE"], ["2024-03-31"])
    ]
    save_checkpoint(
        tmp_path / "checkpoint.json",
        {"completed_windows": done_season_keys, "failed_windows": []},
    )
    acquire, calls = _fake_acquire({})
    write, written = _writer()

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-03-31"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={},
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_enabled=False,
    )

    # every completed segment was skipped: no acquire inside its range
    acquired_starts = [q.scope.start_date for q in calls]
    for w in done_season_keys:
        seg_start = w.rsplit(":", 1)[1].split("~")[0]
        assert seg_start not in acquired_starts
    assert all(
        r["skipped"] == "checkpoint_completed"
        for r in report["window_results"][: len(done_season_keys)]
    )


@pytest.mark.asyncio
async def test_survey_probe_failure_refuses_startup(tmp_path):
    acquire, calls = _fake_acquire(
        {}, probe_result=_Result(status="degraded", errors=["HTTP Error 403: Forbidden"])
    )
    write, written = _writer()

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-09-30", "2023-12-31"],
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
        report_periods=["2024-09-30", "2023-12-31"],
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
            "2024-04-01": _Result(records=[]),
        }
    )
    write, written = _writer()
    cp = tmp_path / "checkpoint.json"

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-03-31"],
        acquire=acquire,
        audit_writer=write,
        instrument_map={("SSE", "688981"): "688981.SH"},
        checkpoint_path=cp,
        probe_enabled=False,
    )

    # degraded window: not aborted, audit rows kept, but marked incomplete
    assert report["aborted"] is False
    assert report["audit_written"] == 1
    assert report["incomplete_windows"] == ["SSE:2024-03-31:2024-03-31~2024-03-31"]
    state = load_checkpoint(cp)
    # every later segment of the season completed normally
    assert "SSE:2024-03-31:2024-04-01~2024-04-15" in state["completed_windows"]
    assert "SSE:2024-03-31:2024-06-16~2024-06-29" in state["completed_windows"]
    assert state["incomplete_windows"] == ["SSE:2024-03-31:2024-03-31~2024-03-31"]


@pytest.mark.asyncio
async def test_survey_bisects_page_exhausted_segments(tmp_path):
    """A half-month segment that exhausts the safe page cap is bisected
    into two halves which are scanned and checkpointed individually
    (deadline pile-ups exceed the cninfo rewind ceiling per query)."""
    acquire, calls = _fake_acquire(
        {
            # the first half-month segment is oversized -> bisect
            "2024-08-16": _Result(status="degraded", records=[]),
            # bisected halves succeed
            "2024-08-16x": None,  # placeholder replaced below by date math
        }
    )

    from datetime import date as _date, timedelta as _td

    start = _date(2024, 8, 16)
    end = _date(2024, 8, 31)
    mid = start + _td(days=(end - start).days // 2)          # 2024-08-23
    left_key = mid.isoformat()
    right_key = (mid + _td(days=1)).isoformat()

    async def acquire_dynamic(query):
        calls.append(query)
        sd, ed = query.scope.start_date, query.scope.end_date
        # only the full half-month segment is oversized; halves succeed
        if sd == "2024-08-16" and ed == "2024-08-31":
            return _Result(status="degraded", records=[])
        if sd in (left_key, right_key):
            return _Result(
                records=[_Record(title="某公司2024年半年度报告", symbols=("000001",))]
            )
        return _Result()

    write, written = _writer()

    report = await survey_historical_disclosure_dates(
        exchanges=["SSE"],
        report_periods=["2024-06-30"],
        acquire=acquire_dynamic,
        audit_writer=write,
        instrument_map={("SSE", "000001"): "000001.SZ"},
        checkpoint_path=tmp_path / "checkpoint.json",
        probe_enabled=False,
        today=_date(2024, 12, 31),
    )

    assert report["aborted"] is False
    assert report["incomplete_windows"] == []
    bisect_events = [
        r for r in report["window_results"] if "bisected_into" in r
    ]
    assert len(bisect_events) == 1
    state = load_checkpoint(tmp_path / "checkpoint.json")
    completed = set(state["completed_windows"])
    assert f"SSE:2024-06-30:2024-08-16~{mid.isoformat()}" in completed
    assert f"SSE:2024-06-30:{right_key}~2024-08-31" in completed
    # the oversized parent itself is NOT marked completed
    assert "SSE:2024-06-30:2024-08-16~2024-08-31" not in completed
    # the bisection record was harvested from a half
    assert report["audit_written"] == 1
