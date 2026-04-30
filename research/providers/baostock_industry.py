"""
BaoStock-backed industry provider.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Dict, List, Optional

import baostock as bs

from .base import BaseIndustryProvider, IndustrySnapshot


class BaostockIndustryProvider(BaseIndustryProvider):
    """Fetch latest industry memberships from BaoStock."""

    source_name = "baostock"
    supported_modes = {"direct"}
    _lock = threading.Lock()

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
        with self._lock:
            login_result = bs.login()
            if getattr(login_result, "error_code", "1") != "0":
                raise RuntimeError(f"BaoStock login failed: {login_result.error_msg}")

            try:
                industry_rows = self._query_rows(bs.query_stock_industry())
            finally:
                try:
                    bs.logout()
                except Exception:
                    pass

        industry_by_instrument: Dict[str, Dict[str, Any]] = {}
        for row in industry_rows:
            instrument_id = self._to_instrument_id(row.get("code", ""))
            if instrument_id:
                industry_by_instrument[instrument_id] = row

        snapshots: List[IndustrySnapshot] = []
        for instrument in target_instruments:
            instrument_id = instrument["instrument_id"]
            row = industry_by_instrument.get(instrument_id)
            if not row:
                continue

            industry_name = str(row.get("industry") or "").strip()
            if not industry_name:
                continue

            source_classification = str(row.get("industryClassification") or "").strip() or None
            taxonomy_system, industry_level = self._normalize_taxonomy(source_classification)
            raw_payload = {"industry": row}

            snapshots.append(
                IndustrySnapshot(
                    instrument_id=instrument_id,
                    symbol=instrument.get("symbol", instrument_id),
                    exchange=instrument.get("exchange", ""),
                    taxonomy_system=taxonomy_system,
                    taxonomy_version=None,
                    industry_code=industry_name,
                    industry_name=industry_name,
                    industry_level=industry_level,
                    source_classification=source_classification,
                    mapping_status="reference_only",
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
                        "source_fields": row,
                    },
                    raw_payload=raw_payload,
                )
            )

        return snapshots

    @staticmethod
    def _query_rows(result_set: Any) -> List[Dict[str, Any]]:
        if getattr(result_set, "error_code", "1") != "0":
            raise RuntimeError(getattr(result_set, "error_msg", "BaoStock query failed"))

        fields = list(getattr(result_set, "fields", []))
        rows: List[Dict[str, Any]] = []
        while result_set.error_code == "0" and result_set.next():
            row_data = result_set.get_row_data()
            rows.append(dict(zip(fields, row_data)))
        return rows

    @staticmethod
    def _to_instrument_id(code: str) -> Optional[str]:
        if code.startswith("sh."):
            return f"{code[3:]}.SH"
        if code.startswith("sz."):
            return f"{code[3:]}.SZ"
        if code.startswith("bj."):
            return f"{code[3:]}.BJ"
        return None

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
