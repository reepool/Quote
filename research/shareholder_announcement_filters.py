"""
Shareholder-specific filters for reusable CNInfo announcement scans.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from research.providers.cninfo_announcements import CninfoAnnouncementRecord


PERIODIC_REPORT_KEYWORDS = (
    "年度报告",
    "半年度报告",
    "季度报告",
    "第一季度报告",
    "第三季度报告",
)

OWNERSHIP_EVENT_KEYWORDS = (
    "权益变动报告书",
    "简式权益变动",
    "详式权益变动",
    "收购报告书",
    "要约收购",
    "股东持股变动",
    "股东权益变动",
    "控股股东",
    "实际控制人",
    "控制权",
    "股本变动",
    "股份变动",
)


@dataclass
class ShareholderAnnouncementCandidate:
    """Instrument selected for shareholder incremental refresh."""

    instrument_id: str
    symbol: str
    exchange: str
    reasons: List[str] = field(default_factory=list)
    announcement_ids: List[str] = field(default_factory=list)
    latest_announcement_time: Optional[str] = None
    announcements: List[CninfoAnnouncementRecord] = field(default_factory=list)


def shareholder_announcement_filter(
    record: CninfoAnnouncementRecord,
) -> List[str]:
    """Return shareholder refresh reasons for one announcement."""
    title = record.title or ""
    reasons: List[str] = []
    if any(keyword in title for keyword in PERIODIC_REPORT_KEYWORDS):
        reasons.append("periodic_report")
    if any(keyword in title for keyword in OWNERSHIP_EVENT_KEYWORDS):
        reasons.append("ownership_event")
    return reasons


def build_shareholder_symbol_index(
    instruments: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Build request-symbol to instrument map, including BSE 920 aliases."""
    index: Dict[str, Dict[str, Any]] = {}
    for instrument in instruments:
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip()
        values = {
            symbol,
            instrument_id.split(".")[0] if instrument_id else "",
        }
        for value in list(values):
            alias = _bse_920_alias(value, exchange)
            if alias:
                values.add(alias)
        for value in values:
            if value:
                index.setdefault(value, instrument)
    return index


def build_shareholder_announcement_candidates(
    records: Iterable[CninfoAnnouncementRecord],
    symbol_index: Dict[str, Dict[str, Any]],
) -> Dict[str, ShareholderAnnouncementCandidate]:
    """Map selected announcements to shareholder candidate instruments."""
    candidates: Dict[str, ShareholderAnnouncementCandidate] = {}
    for record in records:
        reasons = list(record.selection_reasons or shareholder_announcement_filter(record))
        if not reasons:
            continue
        matched_instruments = []
        for symbol in record.symbols:
            instrument = symbol_index.get(symbol)
            if instrument is not None and instrument not in matched_instruments:
                matched_instruments.append(instrument)
        for instrument in matched_instruments:
            instrument_id = str(instrument.get("instrument_id") or "").strip()
            if not instrument_id:
                continue
            candidate = candidates.get(instrument_id)
            if candidate is None:
                candidate = ShareholderAnnouncementCandidate(
                    instrument_id=instrument_id,
                    symbol=str(instrument.get("symbol") or "").strip(),
                    exchange=str(instrument.get("exchange") or "").strip(),
                )
                candidates[instrument_id] = candidate
            for reason in reasons:
                if reason not in candidate.reasons:
                    candidate.reasons.append(reason)
            if record.announcement_id not in candidate.announcement_ids:
                candidate.announcement_ids.append(record.announcement_id)
                candidate.announcements.append(record)
            if (
                record.announcement_time
                and (
                    candidate.latest_announcement_time is None
                    or record.announcement_time > candidate.latest_announcement_time
                )
            ):
                candidate.latest_announcement_time = record.announcement_time
    return candidates


def _bse_920_alias(symbol: str, exchange: str) -> Optional[str]:
    if exchange != "BSE" or len(symbol) != 6:
        return None
    if symbol.startswith("920"):
        return None
    if symbol.startswith(("43", "83", "87")):
        return f"920{symbol[-3:]}"
    return None
