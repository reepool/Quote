"""Historical disclosure-date survey for financial fact availability backfill.

Part of change `backfill-financial-historical-availability-and-caliber`
(M2): scan periodic-report announcements per market x report-season window
via the existing announcement acquisition service, and record the results
in `announcement_audit` (purpose_key=`historical_disclosure_date_survey`)
so the availability-date backfill can use them as observed evidence.

Hard constraints (from the change spec):
- never write `financial_disclosure_event_state`;
- refuse to start when the cninfo access stack is degraded (probe first);
- bounded requests: one acquire per (exchange, window) with page caps, a
  checkpoint per completed window, and an abort on repeated 403/limit
  responses (2026-11-03 data20 403 incident).
"""
from __future__ import annotations

import calendar
import json
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from research.financial_disclosure_events import infer_report_periods_from_title

SURVEY_PURPOSE_KEY = "historical_disclosure_date_survey"

# late-disclosure tail appended to each statutory deadline (design D1)
LATE_TAIL_DAYS = 60

_DEFAULT_PAGE_SIZE = 30
_DEFAULT_MAX_PAGES = 40

AcquireFn = Callable[[Any], Awaitable[Any]]
AuditWriterFn = Callable[[str, Any, Optional[str]], None]


@dataclass(frozen=True)
class SurveyWindow:
    """One (exchange, report_period) season window to scan."""

    exchange: str
    report_period: str  # ISO 'YYYY-MM-DD' period end
    start_date: str
    end_date: str

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.report_period}"


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
    windows: List[SurveyWindow] = []
    for exchange in exchanges:
        for period_text in report_periods:
            parsed = _parse_period_end(period_text)
            if parsed is None:
                continue
            period_end = parsed
            year, month, _ = (int(part) for part in period_text.split("-"))
            deadline_month = {3: 4, 6: 8, 9: 10, 12: 4}[month]
            deadline_year = year + 1 if month == 12 else year
            deadline = date(deadline_year, deadline_month, calendar.monthrange(deadline_year, deadline_month)[1])
            end = deadline + timedelta(days=LATE_TAIL_DAYS)
            if end > today:
                continue
            windows.append(
                SurveyWindow(
                    exchange=exchange,
                    report_period=period_text,
                    start_date=period_end.isoformat(),
                    end_date=end.isoformat(),
                )
            )
    return windows


def _parse_period_end(text: str) -> Optional[date]:
    try:
        return date.fromisoformat(str(text).strip())
    except (TypeError, ValueError):
        return None


def load_checkpoint(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"completed_windows": [], "failed_windows": []}


def save_checkpoint(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=False, indent=1, sort_keys=True),
        encoding="utf-8",
    )


class SurveyAccessError(RuntimeError):
    """Raised when the announcement access stack fails the startup probe.

    The operator guidance points at the scheduler-side entry: a degraded
    stack (headed Chrome down, source 403) must not be worked around with
    bare-HTTP bulk requests (2026-11-03 incident).
    """

    def __init__(self, detail: str) -> None:
        super().__init__(
            "announcement access stack probe failed; "
            "run the survey through the scheduler-side entry with the "
            "full cninfo access stack instead of a bare standalone "
            f"process. detail: {detail}"
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
        category="periodic_report",
        page_size=5,
        max_pages=1,
    )
    return AnnouncementQuery(purpose_key=SURVEY_PURPOSE_KEY, scope=scope)


def _window_query(window: SurveyWindow, *, page_size: int, max_pages: int):
    from research.announcements import AnnouncementQuery, AnnouncementScope

    scope = AnnouncementScope(
        exchange=window.exchange,
        market=window.exchange,
        start_date=window.start_date,
        end_date=window.end_date,
        category="periodic_report",
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
    request_interval_seconds: float = 1.0,
    page_size: int = _DEFAULT_PAGE_SIZE,
    max_pages: int = _DEFAULT_MAX_PAGES,
    probe_enabled: bool = True,
    write_enabled: bool = True,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """Run the survey. ``acquire``/``audit_writer`` are injected so tests can
    drive the loop without network or storage; the production runner wires
    the announcement acquisition service and ``store_announcement_audit``.
    """
    windows = build_survey_windows(exchanges, report_periods, today=today)
    checkpoint = load_checkpoint(checkpoint_path)
    completed: List[str] = list(checkpoint.get("completed_windows", []))
    failed: List[str] = list(checkpoint.get("failed_windows", []))
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

        result = await acquire(
            _window_query(window, page_size=page_size, max_pages=max_pages)
        )
        status, errors = _result_errors(result)
        records = list(getattr(result, "records", None) or [])
        window_result: Dict[str, Any] = {
            "window": window.key,
            "status": status,
            "records": len(records),
            "audit_written": 0,
        }

        if _is_access_refusal(errors) or status == "failed":
            window_result["aborted_window"] = True
            if errors:
                window_result["errors"] = errors[:3]
            failed.append(window.key)
            report["window_results"].append(window_result)
            report["aborted"] = True
            report["abort_reason"] = (
                f"access refused on {window.key} (status={status!r}, "
                f"errors={errors[:3]}); remaining windows left for the "
                "next scheduled run"
            )
            save_checkpoint(
                checkpoint_path,
                {"completed_windows": completed, "failed_windows": failed},
            )
            return report

        if write_enabled:
            for record in records:
                inferred = [
                    period
                    for period in infer_report_periods_from_title(record.title or "")
                    if period == window.report_period
                ]
                if not inferred:
                    continue
                symbol = record.symbols[0] if record.symbols else None
                instrument_id = instrument_map.get((window.exchange, symbol or ""))
                if not instrument_id:
                    continue
                # selection_reasons is Tuple[str, ...] on the record model:
                # carry the inferred period as a plain string token.
                tagged = record.with_selection_reasons(
                    [
                        *(record.selection_reasons or ()),
                        f"inferred_report_period={window.report_period}",
                    ]
                )
                audit_writer(SURVEY_PURPOSE_KEY, tagged, instrument_id)
                window_result["audit_written"] += 1
                report["audit_written"] += 1
        report["records_seen"] += len(records)
        report["window_results"].append(window_result)

        completed.append(window.key)
        completed_set.add(window.key)
        save_checkpoint(
            checkpoint_path,
            {"completed_windows": completed, "failed_windows": failed},
        )

    return report
