"""
AkShare-backed official Shenwan stock-classification history provider.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from .akshare_support import load_akshare
from .base import BaseOfficialIndustryHistoryProvider, OfficialIndustryHistorySnapshot


class AkshareOfficialShenwanHistoryProvider(BaseOfficialIndustryHistoryProvider):
    """Fetch latest official Shenwan stock-history classifications through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    async def fetch_latest_classifications(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[OfficialIndustryHistorySnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]

        if not target_instruments:
            return []

        return await asyncio.to_thread(
            self._fetch_latest_classifications_sync,
            target_instruments,
            mode,
        )

    async def fetch_all_latest_classifications(
        self,
        *,
        mode: str = "direct",
    ) -> List[OfficialIndustryHistorySnapshot]:
        if not self.supports_mode(mode):
            return []

        return await asyncio.to_thread(self._fetch_all_latest_classifications_sync, mode)

    def _fetch_latest_classifications_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        mode: str,
    ) -> List[OfficialIndustryHistorySnapshot]:
        frame = self._load_latest_frame(mode)
        if frame.empty:
            return []

        target_by_symbol = {
            str(instrument.get("symbol", "")).strip(): instrument
            for instrument in target_instruments
            if str(instrument.get("symbol", "")).strip()
        }
        matched = frame[frame["symbol"].isin(target_by_symbol.keys())].copy()
        if matched.empty:
            return []

        matched = matched.sort_values(
            by=["symbol", "update_time", "start_date"],
            ascending=[True, False, False],
            na_position="last",
        )
        latest_rows = matched.drop_duplicates(subset=["symbol"], keep="first")

        snapshots: List[OfficialIndustryHistorySnapshot] = []
        for row in latest_rows.to_dict("records"):
            instrument = target_by_symbol.get(str(row.get("symbol", "")).strip())
            if instrument is None:
                continue

            industry_code = str(row.get("industry_code") or "").strip()
            if not industry_code:
                continue

            snapshots.append(
                OfficialIndustryHistorySnapshot(
                    instrument_id=str(instrument.get("instrument_id", "")),
                    symbol=str(instrument.get("symbol", "")),
                    exchange=str(instrument.get("exchange", "")),
                    official_industry_code=industry_code,
                    start_date=self._format_timestamp(row.get("start_date")),
                    update_time=self._format_timestamp(row.get("update_time")),
                    source=self.source_name,
                    source_mode=mode,
                    raw_payload=self._compact_row(row),
                )
            )

        return snapshots

    def _fetch_all_latest_classifications_sync(
        self,
        mode: str,
    ) -> List[OfficialIndustryHistorySnapshot]:
        frame = self._load_latest_frame(mode)
        if frame.empty:
            return []

        latest_rows = frame.to_dict("records")
        snapshots: List[OfficialIndustryHistorySnapshot] = []
        for row in latest_rows:
            symbol = str(row.get("symbol", "")).strip()
            if not symbol:
                continue

            instrument_id = self._to_instrument_id(symbol)
            exchange = self._to_exchange(symbol)
            if instrument_id is None or exchange is None:
                continue

            industry_code = str(row.get("industry_code") or "").strip()
            if not industry_code:
                continue

            snapshots.append(
                OfficialIndustryHistorySnapshot(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    exchange=exchange,
                    official_industry_code=industry_code,
                    start_date=self._format_timestamp(row.get("start_date")),
                    update_time=self._format_timestamp(row.get("update_time")),
                    source=self.source_name,
                    source_mode=mode,
                    raw_payload=self._compact_row(row),
                )
            )

        return snapshots

    def _load_latest_frame(self, mode: str) -> pd.DataFrame:
        akshare_module = self._akshare(mode)
        frame = akshare_module.stock_industry_clf_hist_sw()
        if frame is None or frame.empty:
            return pd.DataFrame()

        frame = frame.rename(columns={column: str(column).strip() for column in frame.columns})
        frame["symbol"] = frame["symbol"].astype(str).str.strip()
        frame["industry_code"] = frame["industry_code"].astype(str).str.strip()
        frame["start_date"] = pd.to_datetime(frame["start_date"], errors="coerce")
        frame["update_time"] = pd.to_datetime(frame["update_time"], errors="coerce")
        frame = frame.sort_values(
            by=["symbol", "update_time", "start_date"],
            ascending=[True, False, False],
            na_position="last",
        )
        return frame.drop_duplicates(subset=["symbol"], keep="first")

    @staticmethod
    def _akshare(mode: str = "direct"):
        return load_akshare(mode)

    @staticmethod
    def _format_timestamp(value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        return pd.Timestamp(value).date().isoformat()

    @staticmethod
    def _compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
        compact: Dict[str, Any] = {}
        for key, value in row.items():
            if value is None or pd.isna(value):
                compact[str(key)] = None
            elif isinstance(value, pd.Timestamp):
                compact[str(key)] = value.isoformat()
            else:
                compact[str(key)] = value
        return compact

    @staticmethod
    def _to_instrument_id(symbol: str) -> Optional[str]:
        if symbol.startswith(("600", "601", "603", "605", "688", "689", "730")):
            return f"{symbol}.SH"
        if symbol.startswith(("000", "001", "002", "003", "300", "301")):
            return f"{symbol}.SZ"
        if symbol.startswith(
            (
                "920",
                "430",
                "831",
                "832",
                "833",
                "834",
                "835",
                "836",
                "837",
                "838",
                "839",
                "870",
                "871",
                "872",
                "873",
                "874",
                "875",
                "876",
                "877",
                "878",
                "879",
            )
        ):
            return f"{symbol}.BJ"
        return None

    @staticmethod
    def _to_exchange(symbol: str) -> Optional[str]:
        instrument_id = AkshareOfficialShenwanHistoryProvider._to_instrument_id(symbol)
        if instrument_id is None:
            return None
        suffix = instrument_id.rsplit(".", 1)[-1]
        if suffix == "SH":
            return "SSE"
        if suffix == "SZ":
            return "SZSE"
        if suffix == "BJ":
            return "BSE"
        return None
