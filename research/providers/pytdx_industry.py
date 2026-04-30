"""
PyTDX-backed industry provider.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from .base import BaseIndustryProvider, IndustrySnapshot


class PytdxIndustryProvider(BaseIndustryProvider):
    """Build industry memberships from existing instrument metadata."""

    source_name = "pytdx"
    supported_modes = {"direct"}

    async def fetch_industries(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[IndustrySnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]

        if not target_instruments:
            return []

        return await asyncio.to_thread(self._fetch_industries_sync, target_instruments)

    def _fetch_industries_sync(
        self,
        target_instruments: List[Dict[str, Any]],
    ) -> List[IndustrySnapshot]:
        snapshots: List[IndustrySnapshot] = []
        for instrument in target_instruments:
            industry_name = str(instrument.get("industry") or "").strip()
            if not industry_name:
                continue

            source_classification = str(instrument.get("sector") or "").strip() or None
            taxonomy_system, industry_level = self._normalize_taxonomy(source_classification)

            snapshots.append(
                IndustrySnapshot(
                    instrument_id=instrument["instrument_id"],
                    symbol=instrument.get("symbol", instrument["instrument_id"]),
                    exchange=instrument.get("exchange", ""),
                    taxonomy_system=taxonomy_system,
                    taxonomy_version=None,
                    industry_code=industry_name,
                    industry_name=industry_name,
                    industry_level=industry_level,
                    mapping_status="reference_only",
                    source_classification=source_classification,
                    source_industry_name=industry_name,
                    source=self.source_name,
                    source_mode="direct",
                    membership_json={
                        "normalized": {
                            "taxonomy_system": taxonomy_system,
                            "industry_code": industry_name,
                            "industry_name": industry_name,
                            "industry_level": industry_level,
                        },
                        "instrument_fields": {
                            "industry": instrument.get("industry"),
                            "sector": instrument.get("sector"),
                        },
                    },
                    raw_payload={
                        "instrument": {
                            "instrument_id": instrument.get("instrument_id"),
                            "industry": instrument.get("industry"),
                            "sector": instrument.get("sector"),
                        }
                    },
                )
            )

        return snapshots

    @staticmethod
    def _normalize_taxonomy(source_classification: Optional[str]) -> tuple[str, int]:
        text = str(source_classification or "").strip()
        if "申万一级" in text:
            return "sw_l1", 1
        if "申万二级" in text:
            return "sw_l2", 2
        if "申万三级" in text:
            return "sw_l3", 3
        if text:
            return f"source_{text}", 1
        return "source_raw", 1
