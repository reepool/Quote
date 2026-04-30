"""
Config-driven manual industry-name supplement provider.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .base import BaseIndustryNameSupplementProvider, IndustryNameHintSnapshot


@dataclass(frozen=True)
class _ManualIndustrySupplementEntry:
    instrument_id: Optional[str]
    symbol: Optional[str]
    exchange: Optional[str]
    industry_name: str
    taxonomy_system: str
    taxonomy_version: Optional[str]
    source_classification: str
    raw_payload: Dict[str, Any]


class ManualIndustryNameSupplementProvider(BaseIndustryNameSupplementProvider):
    """Return manually curated Shenwan industry hints from repository config."""

    source_name = "manual"
    supported_modes = {"configured"}

    def __init__(
        self,
        *,
        entries: Optional[List[Dict[str, Any]]] = None,
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
    ):
        self.taxonomy_system = taxonomy_system
        self.taxonomy_version = taxonomy_version
        self._entries: List[_ManualIndustrySupplementEntry] = []
        for raw_entry in entries or []:
            entry = self._normalize_entry(raw_entry)
            if entry is not None:
                self._entries.append(entry)

    async def fetch_industry_name_hints(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "configured",
        limit: Optional[int] = None,
    ) -> List[IndustryNameHintSnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]
        if not target_instruments or not self._entries:
            return []

        hints: List[IndustryNameHintSnapshot] = []
        for instrument in target_instruments:
            entry = self._match_entry(instrument)
            if entry is None:
                continue

            symbol = str(instrument.get("symbol") or entry.symbol or "").strip()
            instrument_id = str(
                instrument.get("instrument_id") or entry.instrument_id or ""
            ).strip()
            if not instrument_id or not symbol:
                continue

            hints.append(
                IndustryNameHintSnapshot(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    exchange=str(instrument.get("exchange") or entry.exchange or exchange),
                    taxonomy_system=entry.taxonomy_system,
                    taxonomy_version=entry.taxonomy_version,
                    industry_name=entry.industry_name,
                    source_classification=entry.source_classification,
                    source=self.source_name,
                    source_mode=mode,
                    raw_payload=entry.raw_payload,
                )
            )
        return hints

    def _match_entry(
        self,
        instrument: Dict[str, Any],
    ) -> Optional[_ManualIndustrySupplementEntry]:
        instrument_id = str(instrument.get("instrument_id") or "").strip().upper()
        symbol = str(instrument.get("symbol") or "").strip()
        exchange = str(instrument.get("exchange") or "").strip().upper()
        if not symbol and instrument_id:
            symbol = instrument_id.split(".", 1)[0]
        for entry in self._entries:
            if entry.instrument_id and entry.instrument_id == instrument_id:
                return entry
            if entry.symbol and entry.exchange:
                if entry.symbol == symbol and entry.exchange == exchange:
                    return entry
        return None

    def _normalize_entry(
        self,
        raw_entry: Any,
    ) -> Optional[_ManualIndustrySupplementEntry]:
        if not isinstance(raw_entry, dict):
            return None

        industry_name = str(raw_entry.get("industry_name") or "").strip()
        if not industry_name:
            return None

        instrument_id = str(raw_entry.get("instrument_id") or "").strip().upper() or None
        symbol = str(raw_entry.get("symbol") or "").strip() or None
        exchange = str(raw_entry.get("exchange") or "").strip().upper() or None
        if instrument_id and "." in instrument_id and exchange is None:
            exchange = instrument_id.rsplit(".", 1)[-1]
        if not instrument_id and not symbol:
            return None

        raw_payload = {
            "manual_entry": {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "exchange": exchange,
                "industry_name": industry_name,
                "industry_code": raw_entry.get("industry_code"),
                "reason": raw_entry.get("reason"),
                "evidence": raw_entry.get("evidence"),
                "note": raw_entry.get("note"),
            }
        }
        return _ManualIndustrySupplementEntry(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            industry_name=industry_name,
            taxonomy_system=str(
                raw_entry.get("taxonomy_system") or self.taxonomy_system or "sw"
            ),
            taxonomy_version=str(
                raw_entry.get("taxonomy_version") or self.taxonomy_version or "sw_2021"
            ),
            source_classification=str(
                raw_entry.get("source_classification") or "人工确认申万行业补源"
            ),
            raw_payload=raw_payload,
        )
