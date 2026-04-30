"""
BaoStock-backed company profile provider.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Dict, List, Optional

import baostock as bs

from .base import BaseCompanyProfileProvider, CompanyProfileSnapshot


class BaostockCompanyProfileProvider(BaseCompanyProfileProvider):
    """Fetch company profile snapshots from BaoStock."""

    source_name = "baostock"
    supported_modes = {"direct"}
    _lock = threading.Lock()

    async def fetch_company_profiles(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[CompanyProfileSnapshot]:
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

        return await asyncio.to_thread(
            self._fetch_company_profiles_sync,
            target_instruments,
        )

    def _fetch_company_profiles_sync(
        self,
        target_instruments: List[Dict[str, Any]],
    ) -> List[CompanyProfileSnapshot]:
        with self._lock:
            login_result = bs.login()
            if getattr(login_result, "error_code", "1") != "0":
                raise RuntimeError(f"BaoStock login failed: {login_result.error_msg}")

            try:
                basic_rows = self._query_rows(bs.query_stock_basic())
                industry_rows = self._query_rows(bs.query_stock_industry())
            finally:
                try:
                    bs.logout()
                except Exception:
                    pass

        basic_by_instrument: Dict[str, Dict[str, Any]] = {}
        for row in basic_rows:
            instrument_id = self._to_instrument_id(row.get("code", ""))
            if instrument_id:
                basic_by_instrument[instrument_id] = row

        industry_by_instrument: Dict[str, Dict[str, Any]] = {}
        for row in industry_rows:
            instrument_id = self._to_instrument_id(row.get("code", ""))
            if instrument_id:
                industry_by_instrument[instrument_id] = row

        snapshots: List[CompanyProfileSnapshot] = []
        for instrument in target_instruments:
            instrument_id = instrument["instrument_id"]
            basic = basic_by_instrument.get(instrument_id)
            if not basic:
                continue

            industry = industry_by_instrument.get(instrument_id, {})
            company_name = basic.get("code_name") or instrument.get("name") or instrument_id
            raw_payload = {
                "basic": basic,
                "industry": industry,
            }

            snapshots.append(
                CompanyProfileSnapshot(
                    instrument_id=instrument_id,
                    symbol=instrument.get("symbol", instrument_id),
                    company_name=company_name,
                    short_name=company_name,
                    exchange=instrument.get("exchange", ""),
                    market=basic.get("type"),
                    listed_date=self._normalize_optional_date(basic.get("ipoDate")),
                    industry_raw=industry.get("industry") or instrument.get("industry"),
                    sector_raw=industry.get("industryClassification") or instrument.get("sector"),
                    status=self._derive_status(basic.get("outDate")),
                    source=self.source_name,
                    source_mode="direct",
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
    def _normalize_optional_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        if value in {"", "0000-00-00"}:
            return None
        return value

    @staticmethod
    def _derive_status(out_date: Optional[str]) -> str:
        if out_date and out_date not in {"", "0000-00-00"}:
            return "delisted"
        return "active"
