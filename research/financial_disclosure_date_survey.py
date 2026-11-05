"""Historical disclosure-date survey for financial fact availability backfill.

Part of change `backfill-financial-historical-availability-and-caliber`
(M2): scan periodic-report announcements per market x report-season window
via the existing announcement acquisition service, and record the results
in `announcement_audit` (purpose_key=`historical_disclosure_date_survey`)
so the availability-date backfill can use them as observed evidence.

Hard constraints (from the change spec):
- never write `financial_disclosure_event_state`;
- probe the announcement source with one bounded page before scanning;
  refuse to scan when the probe fails (endpoint-health contract);
- bounded requests: one acquire per (exchange, window) with page caps, a
  checkpoint per completed window, an abort on 403/limit refusals, and
  degraded (page-exhausted) windows marked incomplete so a rerun with a
  higher cap resumes them (2026-11-03 data20 403 incident).
"""
from __future__ import annotations

import calendar
import json
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from research.financial_available_date_backfill import (
    statutory_deadline_for_period_end,
)
from research.announcements.categories import (
    ANNUAL_REPORT_CATEGORY,
    FIRST_QUARTER_REPORT_CATEGORY,
    SEMIANNUAL_REPORT_CATEGORY,
    THIRD_QUARTER_REPORT_CATEGORY,
)
from research.financial_disclosure_events import infer_report_periods_from_title

# exact single-category per report period: quarter-sized queries keep the
# cninfo pagination far below the ~110-page rewind ceiling (April worst
# case ~49 pages per half-month segment on the Q1 category)
PERIOD_CATEGORY = {
    "03-31": FIRST_QUARTER_REPORT_CATEGORY,
    "06-30": SEMIANNUAL_REPORT_CATEGORY,
    "09-30": THIRD_QUARTER_REPORT_CATEGORY,
    "12-31": ANNUAL_REPORT_CATEGORY,
}

SURVEY_PURPOSE_KEY = "historical_disclosure_date_survey"

# selection-reason token carrying the title-inferred report period on each
# audit row; the availability backfill parses the same token (single
# contract between the two modules).
INFERRED_PERIOD_PREFIX = "inferred_report_period="


def inferred_period_reason(report_period: str) -> str:
    """Build the selection-reason token for a survey audit row."""
    return f"{INFERRED_PERIOD_PREFIX}{report_period}"


def parse_inferred_report_period(text: Any) -> str:
    """Extract the report period from survey selection reasons.

    Accepts the serialized shapes seen in ``selection_reasons_json``
    (JSON list of token strings), a bare token string, or a legacy dict
    carrying ``inferred_report_period``; empty string when absent.
    """
    prefix = INFERRED_PERIOD_PREFIX
    if not text:
        return ""
    try:
        parsed = json.loads(str(text))
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, dict):
        return str(parsed.get("inferred_report_period") or "").strip()
    candidates: list[Any] = parsed if isinstance(parsed, list) else [text]
    for item in candidates:
        token = str(item).strip()
        if token.startswith(prefix):
            return token[len(prefix):].strip().strip("'\"")
    return ""


# late-disclosure tail appended to each statutory deadline (design D1)
LATE_TAIL_DAYS = 60

# cninfo rewinds to page-one content beyond ~110 pages per query; keep
# segments under this ceiling and bisect any segment that exhausts it
# (deadline-week pile-ups reach 130+ pages even on a single category).
SAFE_MAX_PAGES = 90

_DEFAULT_PAGE_SIZE = 30

AcquireFn = Callable[[Any], Awaitable[Any]]
AuditWriterFn = Callable[[str, Any, Optional[str]], None]


@dataclass(frozen=True)
class SurveyWindow:
    """One half-month segment of an (exchange, report_period) season.

    Segments stay under the cninfo effective-pagination ceiling (~110
    pages, ~3,300 records): beyond it the endpoint rewinds to page one,
    silently hiding the older half of the date range (found while
    diagnosing missing annual-report evidence). Peak half-months (April,
    with annual tail + full Q1 season) stay at ~90 pages on the joint
    periodic category.
    """

    exchange: str
    report_period: str  # ISO 'YYYY-MM-DD' period end
    start_date: str
    end_date: str

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.report_period}:{self.start_date}~{self.end_date}"


def build_survey_windows(
    exchanges: Iterable[str],
    report_periods: Iterable[str],
    *,
    today: Optional[date] = None,
) -> List[SurveyWindow]:
    """Build season windows: period end -> statutory deadline + late tail.

    Q1 -> season starts 03-31, deadline 04-30; H1 -> 06-30 / 08-31;
    Q3 -> 09-30 / 10-31; FY -> 12-31 / next 04-30. Each deadline is
    extended by LATE_TAIL_DAYS for postponed disclosures. Windows whose
    end lies in the future (relative to ``today``) are dropped: the survey
    only covers already-elapsed seasons.
    """
    today = today or date.today()
    periods = [str(period) for period in report_periods]
    exchanges = list(exchanges)
    windows: List[SurveyWindow] = []
    for exchange in exchanges:
        for period_text in periods:
            period_end = _parse_period_end(period_text)
            if period_end is None:
                continue
            deadline = statutory_deadline_for_period_end(period_end)
            if deadline is None:
                continue
            season_end = deadline + timedelta(days=LATE_TAIL_DAYS)
            if season_end > today:
                continue
            windows.extend(
                SurveyWindow(
                    exchange=exchange,
                    report_period=period_text,
                    start_date=seg_start.isoformat(),
                    end_date=seg_end.isoformat(),
                )
                for seg_start, seg_end in _half_month_segments(
                    period_end, season_end, today
                )
            )
    return windows


def _half_month_segments(
    season_start: date, season_end: date, today: date
) -> List[Tuple[date, date]]:
    """Split [season_start, season_end] into half-month segments.

    Boundaries: the 1st..15th and 16th..month-end of each calendar month
    clipped to the season and to ``today`` (never scan into the future).
    """
    segments: List[Tuple[date, date]] = []
    cursor = season_start
    while cursor <= min(season_end, today):
        if cursor.day <= 15:
            seg_end = min(
                date(cursor.year, cursor.month, 15), season_end, today
            )
        else:
            month_last = calendar.monthrange(cursor.year, cursor.month)[1]
            seg_end = min(date(cursor.year, cursor.month, month_last), season_end, today)
        segments.append((cursor, seg_end))
        cursor = seg_end + timedelta(days=1)
    return segments


def _parse_period_end(text: str) -> Optional[date]:
    try:
        return date.fromisoformat(str(text).strip())
    except (TypeError, ValueError):
        return None


def load_checkpoint(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"completed_windows": [], "failed_windows": [], "incomplete_windows": []}


def save_checkpoint(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=False, indent=1, sort_keys=True),
        encoding="utf-8",
    )


def _window_probe_query(window: SurveyWindow):
    from research.announcements import AnnouncementQuery, AnnouncementScope

    one_day_end = min(
        date.fromisoformat(window.end_date),
        date.fromisoformat(window.start_date) + timedelta(days=1),
    )
    scope = AnnouncementScope(
        exchange=window.exchange,
        market=window.exchange,
        start_date=window.start_date,
        end_date=one_day_end.isoformat(),
        category=_window_category(window.report_period),
        page_size=5,
        max_pages=1,
    )
    return AnnouncementQuery(purpose_key=SURVEY_PURPOSE_KEY, scope=scope)


def _window_category(report_period: str) -> str:
    return PERIOD_CATEGORY.get(report_period[-5:], "periodic_report")


def _window_query(window: SurveyWindow, *, page_size: int, max_pages: int):
    from research.announcements import AnnouncementQuery, AnnouncementScope

    scope = AnnouncementScope(
        exchange=window.exchange,
        market=window.exchange,
        start_date=window.start_date,
        end_date=window.end_date,
        category=_window_category(window.report_period),
        page_size=page_size,
        max_pages=max_pages,
    )
    return AnnouncementQuery(purpose_key=SURVEY_PURPOSE_KEY, scope=scope)


def _result_errors(result: Any) -> Tuple[str, List[str]]:
    status = str(getattr(result, "status", "") or "")
    errors = [str(item) for item in (getattr(result, "errors", None) or ())]
    return status, errors


def _is_access_refusal(errors: List[str]) -> bool:
    joined = " ".join(errors).lower()
    return "403" in joined or "forbidden" in joined


async def survey_historical_disclosure_dates(
    *,
    exchanges: Iterable[str],
    report_periods: Iterable[str],
    acquire: AcquireFn,
    audit_writer: AuditWriterFn,
    instrument_map: Mapping[Tuple[str, str], str],
    checkpoint_path: Path,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_pages: int = SAFE_MAX_PAGES,
    probe_enabled: bool = True,
    write_enabled: bool = True,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """Run the survey. ``acquire``/``audit_writer`` are injected so tests can
    drive the loop without network or storage; the production runner wires
    the announcement acquisition service and ``store_announcement_audit``.
    """
    # materialize once: build_survey_windows and the harvest filter both
    # iterate the periods, and a generator input would silently exhaust
    periods = [str(period) for period in report_periods]
    windows = build_survey_windows(exchanges, periods, today=today)
    target_periods = set(periods)
    checkpoint = load_checkpoint(checkpoint_path)
    completed: List[str] = list(checkpoint.get("completed_windows", []))
    failed: List[str] = list(checkpoint.get("failed_windows", []))
    incomplete: List[str] = list(checkpoint.get("incomplete_windows", []))
    completed_set = set(completed)

    report: Dict[str, Any] = {
        "purpose_key": SURVEY_PURPOSE_KEY,
        "windows_total": len(windows),
        "windows_completed_before": len(completed_set),
        "window_results": [],
        "audit_written": 0,
        "records_seen": 0,
        "aborted": False,
        "abort_reason": None,
        "incomplete_windows": [],
    }

    if windows and probe_enabled:
        probe_status, probe_errors = _result_errors(
            await acquire(_window_probe_query(windows[0]))
        )
        if probe_status not in {"success", "success_empty"}:
            report["aborted"] = True
            report["abort_reason"] = (
                f"access probe failed (status={probe_status!r}, "
                f"errors={probe_errors[:3]})"
            )
            return report

    for window in windows:
        if window.key in completed_set:
            report["window_results"].append(
                {"window": window.key, "skipped": "checkpoint_completed"}
            )
            continue

        bisect_queue: List[SurveyWindow] = [window]
        window_keys_completed: List[str] = []
        while bisect_queue:
            active = bisect_queue.pop(0)
            result = await acquire(
                _window_query(active, page_size=page_size, max_pages=max_pages)
            )
            status, errors = _result_errors(result)
            records = list(getattr(result, "records", None) or [])

            if _is_access_refusal(errors) or status == "failed":
                report["window_results"].append(
                    {
                        "window": active.key,
                        "status": status,
                        "records": len(records),
                        "aborted_window": True,
                        "errors": errors[:3],
                    }
                )
                failed.append(active.key)
                report["aborted"] = True
                report["abort_reason"] = (
                    f"access refused on {active.key} (status={status!r}, "
                    f"errors={errors[:3]}); remaining windows left for the "
                    "next scheduled run"
                )
                save_checkpoint(
                    checkpoint_path,
                    {
                        "completed_windows": completed,
                        "failed_windows": failed,
                        "incomplete_windows": incomplete,
                    },
                )
                return report

            if (
                status not in {"success", "success_empty"}
                and _bisectable(active)
            ):
                # page exhaustion inside the safe ceiling means the segment
                # is bigger than one query can page: bisect and retry both
                # halves (deadline pile-ups can exceed 130 pages/category)
                left, right = _bisect_window(active)
                report["window_results"].append(
                    {
                        "window": active.key,
                        "status": status,
                        "records": len(records),
                        "bisected_into": [left.key, right.key],
                    }
                )
                bisect_queue.insert(0, right)
                bisect_queue.insert(0, left)
                continue

            window_result: Dict[str, Any] = {
                "window": active.key,
                "status": status,
                "records": len(records),
                "audit_written": 0,
            }
            if status not in {"success", "success_empty"}:
                # not exhaustible further (single day) - record as is
                window_result["incomplete_window"] = True
                incomplete.append(active.key)
                report["incomplete_windows"].append(active.key)

            if write_enabled:
                for record in records:
                    # Windows overlap the neighbouring disclosure season
                    # (deadline + late tail) and now carry a single report
                    # category: harvest every periodic report the scan
                    # sees, tagged with the period inferred from ITS OWN
                    # title. Filtering to the window period alone
                    # discarded ~half of each overlapping window's
                    # announcements (found in implementation review).
                    inferred_periods = sorted(
                        {
                            period
                            for period in infer_report_periods_from_title(
                                record.title or ""
                            )
                            if period in target_periods
                        }
                    )
                    if not inferred_periods:
                        continue
                    symbol = record.symbols[0] if record.symbols else None
                    instrument_id = instrument_map.get(
                        (active.exchange, symbol or "")
                    )
                    if not instrument_id:
                        continue
                    # selection_reasons is Tuple[str, ...] on the record
                    # model: carry the inferred period as plain tokens.
                    tagged = record.with_selection_reasons(
                        [
                            *(record.selection_reasons or ()),
                            *(inferred_period_reason(p) for p in inferred_periods),
                        ]
                    )
                    audit_writer(SURVEY_PURPOSE_KEY, tagged, instrument_id)
                    window_result["audit_written"] += 1
                    report["audit_written"] += 1
            report["records_seen"] += len(records)
            report["window_results"].append(window_result)

            if active.key not in incomplete:
                completed.append(active.key)
                completed_set.add(active.key)

        save_checkpoint(
            checkpoint_path,
            {
                "completed_windows": completed,
                "failed_windows": failed,
                "incomplete_windows": incomplete,
            },
        )

    return report


def _bisectable(window: SurveyWindow) -> bool:
    start = date.fromisoformat(window.start_date)
    end = date.fromisoformat(window.end_date)
    return (end - start).days >= 1


def _bisect_window(window: SurveyWindow) -> tuple[SurveyWindow, SurveyWindow]:
    start = date.fromisoformat(window.start_date)
    end = date.fromisoformat(window.end_date)
    span = (end - start).days
    mid = start + timedelta(days=span // 2)
    left = SurveyWindow(
        exchange=window.exchange,
        report_period=window.report_period,
        start_date=start.isoformat(),
        end_date=mid.isoformat(),
    )
    right = SurveyWindow(
        exchange=window.exchange,
        report_period=window.report_period,
        start_date=(mid + timedelta(days=1)).isoformat(),
        end_date=end.isoformat(),
    )
    return left, right
