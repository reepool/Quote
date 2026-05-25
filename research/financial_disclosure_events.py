"""
Financial disclosure event filters for reusable CNInfo announcement scans.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from research.providers.cninfo_announcements import CninfoAnnouncementRecord


FINANCIAL_PERIODIC_REPORT_KEYWORDS = (
    "年度报告",
    "年报",
    "第一季度报告",
    "一季度报告",
    "半年度报告",
    "半年报",
    "第三季度报告",
    "三季度报告",
    "季度报告",
)

FORMAL_PERIODIC_REPORT_PHRASES = (
    "年度报告",
    "年报",
    "第一季度报告",
    "一季度报告",
    "半年度报告",
    "半年报",
    "第三季度报告",
    "三季度报告",
)

DISCLOSURE_DELAY_KEYWORDS = (
    "无法按期披露",
    "不能按期披露",
    "未能按期披露",
    "无法在法定期限内披露",
    "延期披露",
    "延迟披露",
    "无法披露",
)

DISCLOSURE_CORRECTION_KEYWORDS = (
    "更正",
    "修订",
    "补充",
)

NON_PRIMARY_ANNOUNCEMENT_KEYWORDS = (
    "业绩说明会",
    "说明会预告",
    "年度报告说明会",
    "年报说明会",
    "英文版",
    "图文版",
    "问询函",
    "问询函回复",
    "监管问询",
    "专项说明",
    "投资者网上集体接待日",
    "投资者接待日",
    "集体接待日",
    "摘要",
)

TRADING_RISK_KEYWORDS = (
    "停牌",
    "退市风险警示",
    "被实施退市风险警示",
    "可能被实施退市风险警示",
    "可能被终止上市",
    "终止上市风险",
    "终止上市",
    "股票可能被终止上市",
)

FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION = "periodic_report_delayed_or_suspended"
FINANCIAL_PERIODIC_REPORT_CLASSIFICATION = "periodic_report_available"
PENDING_DELISTING_RISK_CLASSIFICATION = "pending_delisting_risk"
ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS = frozenset(
    {
        FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION,
        PENDING_DELISTING_RISK_CLASSIFICATION,
    }
)


@dataclass
class FinancialDisclosureEvent:
    """Disclosure event that can explain a missing financial report period."""

    instrument_id: str
    report_period: str
    classification: str = FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
    reasons: List[str] = field(default_factory=list)
    announcement_id: Optional[str] = None
    announcement_time: Optional[str] = None
    title: Optional[str] = None
    source: str = "cninfo_announcement"

    def to_manifest_item(self) -> Dict[str, Any]:
        return {
            "instrument_id": self.instrument_id,
            "report_period": self.report_period,
            "classification": self.classification,
            "reason": "公告显示定期报告披露异常或相关停牌/退市风险，结构化财报缺失视为待补事项。",
            "event_reasons": list(self.reasons),
            "announcement_id": self.announcement_id,
            "announcement_time": self.announcement_time,
            "title": self.title,
            "source": self.source,
        }


def financial_disclosure_event_filter(record: CninfoAnnouncementRecord) -> List[str]:
    """Return financial disclosure event reasons for one announcement."""
    title = record.title or ""
    reasons: List[str] = []
    has_periodic_report = any(
        keyword in title for keyword in FINANCIAL_PERIODIC_REPORT_KEYWORDS
    )
    has_delay = any(keyword in title for keyword in DISCLOSURE_DELAY_KEYWORDS)
    has_trading_risk = any(keyword in title for keyword in TRADING_RISK_KEYWORDS)
    has_correction = any(keyword in title for keyword in DISCLOSURE_CORRECTION_KEYWORDS)
    is_non_primary = is_non_primary_financial_announcement_title(title)
    report_periods = infer_report_periods_from_title(title)

    if is_non_primary and not (has_delay or has_trading_risk):
        return []

    if has_periodic_report and not report_periods and not (has_delay or has_trading_risk):
        return []

    if has_periodic_report and any(keyword in title for keyword in DISCLOSURE_DELAY_KEYWORDS):
        reasons.append("periodic_report_delayed")
    if has_periodic_report and has_trading_risk:
        reasons.append("periodic_report_related_trading_risk")
    if has_periodic_report and has_correction:
        reasons.append("periodic_report_correction")
    if has_periodic_report and not reasons:
        reasons.append("periodic_report")
    if has_trading_risk:
        reasons.append("pending_delisting_risk")
    return reasons


def is_non_primary_financial_announcement_title(title: str) -> bool:
    """Return True for finance-related announcements that should not trigger repair."""
    text = str(title or "")
    return any(keyword in text for keyword in NON_PRIMARY_ANNOUNCEMENT_KEYWORDS)


def is_financial_disclosure_like_title(title: str) -> bool:
    """Return True when a title looks related to periodic reports or disclosure risk."""
    text = str(title or "")
    return any(keyword in text for keyword in FINANCIAL_PERIODIC_REPORT_KEYWORDS) or any(
        keyword in text for keyword in DISCLOSURE_DELAY_KEYWORDS + TRADING_RISK_KEYWORDS
    )


def infer_report_periods_from_title(title: str) -> List[str]:
    """Infer report periods mentioned by a Chinese periodic-report title."""
    text = str(title or "")
    periods: List[str] = []
    patterns = (
        (r"(20\d{2})\s*年\s*(?:第一季度报告|一季度报告)", "03-31"),
        (r"(20\d{2})\s*年\s*(?:半年度报告|半年报)", "06-30"),
        (r"(20\d{2})\s*年\s*(?:第三季度报告|三季度报告)", "09-30"),
        (r"(20\d{2})\s*年?\s*(?:年度报告|年报)", "12-31"),
    )
    for pattern, suffix in patterns:
        for year_text in re.findall(pattern, text):
            period = f"{int(year_text):04d}-{suffix}"
            if period not in periods:
                periods.append(period)
    return periods


def build_financial_disclosure_events(
    records: Iterable[CninfoAnnouncementRecord],
    symbol_index: Mapping[str, Mapping[str, Any]],
) -> List[FinancialDisclosureEvent]:
    """Map selected CNInfo announcements to financial disclosure events."""
    events_by_key: Dict[tuple[str, str, Optional[str]], FinancialDisclosureEvent] = {}
    for record in records:
        reasons = list(record.selection_reasons or financial_disclosure_event_filter(record))
        if not reasons:
            continue
        report_periods = infer_report_periods_from_title(record.title)
        if not report_periods:
            continue
        matched_instruments: List[Mapping[str, Any]] = []
        for symbol in record.symbols:
            instrument = symbol_index.get(symbol)
            if instrument is not None and instrument not in matched_instruments:
                matched_instruments.append(instrument)
        for instrument in matched_instruments:
            instrument_id = str(instrument.get("instrument_id") or "").strip()
            if not instrument_id:
                continue
            for report_period in report_periods:
                key = (instrument_id, report_period, record.announcement_id)
                event = events_by_key.get(key)
                if event is None:
                    classification = (
                        PENDING_DELISTING_RISK_CLASSIFICATION
                        if "pending_delisting_risk" in reasons
                        or "periodic_report_related_trading_risk" in reasons
                        else (
                            FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
                            if "periodic_report_delayed" in reasons
                            else FINANCIAL_PERIODIC_REPORT_CLASSIFICATION
                        )
                    )
                    event = FinancialDisclosureEvent(
                        instrument_id=instrument_id,
                        report_period=report_period,
                        classification=classification,
                        reasons=[],
                        announcement_id=record.announcement_id,
                        announcement_time=record.announcement_time,
                        title=record.title,
                    )
                    events_by_key[key] = event
                for reason in reasons:
                    if reason not in event.reasons:
                        event.reasons.append(reason)
    return list(events_by_key.values())


def build_financial_symbol_index(
    instruments: Iterable[Mapping[str, Any]],
) -> Dict[str, Mapping[str, Any]]:
    """Build CNInfo announcement symbol aliases for financial disclosure events."""
    index: Dict[str, Mapping[str, Any]] = {}
    for instrument in instruments:
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        for value in {symbol, instrument_id.split(".")[0] if instrument_id else ""}:
            if value:
                index.setdefault(value, instrument)
    return index


def build_financial_disclosure_event_index(
    events: Optional[Sequence[Mapping[str, Any]]],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Index manifest-ready disclosure events by instrument and report period."""
    index: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for raw_event in events or []:
        instrument_id = str(raw_event.get("instrument_id") or "").strip()
        classification = str(
            raw_event.get("classification") or FINANCIAL_DISCLOSURE_GAP_CLASSIFICATION
        ).strip()
        if (
            not instrument_id
            or classification not in ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS
        ):
            continue
        periods = _event_report_periods(raw_event)
        for report_period in periods:
            index[instrument_id][report_period].append(dict(raw_event))
    return {instrument: dict(periods) for instrument, periods in index.items()}


def _event_report_periods(raw_event: Mapping[str, Any]) -> List[str]:
    periods: List[str] = []
    for key in ("report_period", "period"):
        value = str(raw_event.get(key) or "").strip()
        if value and value not in periods:
            periods.append(value)
    raw_periods = raw_event.get("report_periods")
    if isinstance(raw_periods, str):
        candidates = [item.strip() for item in raw_periods.split(",")]
    elif isinstance(raw_periods, Sequence):
        candidates = [str(item).strip() for item in raw_periods]
    else:
        candidates = []
    for candidate in candidates:
        if candidate and candidate not in periods:
            periods.append(candidate)
    return periods
