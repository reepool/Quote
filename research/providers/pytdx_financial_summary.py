"""
pytdx-backed financial summary provider.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from .base import BaseFinancialSummaryProvider, FinancialSummarySnapshot
from .pytdx_support import EXCHANGE_TO_MARKET, PytdxProviderMixin


class PytdxFinancialSummaryProvider(PytdxProviderMixin, BaseFinancialSummaryProvider):
    """Fetch latest financial summary snapshots from pytdx finance info."""

    source_name = "pytdx"
    supported_modes = {"direct"}

    def __init__(
        self,
        *,
        connection_pool: Any = None,
        hosts: Optional[list[tuple[str, str, int]]] = None,
        connection_timeout: float = 10.0,
        max_connect_attempts: int = 5,
    ):
        super().__init__(
            connection_pool=connection_pool,
            hosts=hosts,
            connection_timeout=connection_timeout,
            max_connect_attempts=max_connect_attempts,
        )

    async def fetch_financial_summaries(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[FinancialSummarySnapshot]:
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
            self._fetch_financial_summaries_sync,
            target_instruments,
            exchange,
        )

    def _fetch_financial_summaries_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        exchange: str,
    ) -> List[FinancialSummarySnapshot]:
        api, managed_connection = self._get_api()
        expected_market = EXCHANGE_TO_MARKET.get(exchange)
        snapshots: List[FinancialSummarySnapshot] = []

        try:
            for instrument in target_instruments:
                instrument_id = instrument.get("instrument_id", "")
                parsed = self.parse_instrument_id(instrument_id)
                if parsed is None:
                    continue

                market, code = parsed
                if expected_market is not None and market != expected_market:
                    continue

                finance_info = self._normalize_mapping(api.get_finance_info(market, code))
                if not finance_info:
                    continue

                current_assets = self.to_float(finance_info.get("liudongzichan"))
                current_liabilities = self.to_float(finance_info.get("liudongfuzhai"))
                long_term_liabilities = self.to_float(finance_info.get("changqifuzhai"))
                total_assets = self.to_float(finance_info.get("zongzichan"))
                operating_revenue = self.to_float(finance_info.get("zhuyingshouru"))
                operating_cashflow = self.to_float(finance_info.get("jingyingxianjinliu"))
                net_profit = self.to_float(finance_info.get("jinglirun"))

                current_ratio = None
                if current_assets not in {None, 0.0} and current_liabilities not in {None, 0.0}:
                    current_ratio = current_assets / current_liabilities

                liability_to_asset = None
                total_liabilities = None
                if current_liabilities is not None or long_term_liabilities is not None:
                    total_liabilities = (current_liabilities or 0.0) + (long_term_liabilities or 0.0)
                if total_liabilities not in {None, 0.0} and total_assets not in {None, 0.0}:
                    liability_to_asset = total_liabilities / total_assets

                cfo_to_revenue = None
                if operating_cashflow is not None and operating_revenue not in {None, 0.0}:
                    cfo_to_revenue = operating_cashflow / operating_revenue

                cfo_to_net_profit = None
                if operating_cashflow is not None and net_profit not in {None, 0.0}:
                    cfo_to_net_profit = operating_cashflow / net_profit

                updated_date = self.normalize_numeric_date(finance_info.get("updated_date"))
                summary_json = {
                    "reporting": {
                        "updated_date": updated_date,
                        "ipo_date": self.normalize_numeric_date(finance_info.get("ipo_date")),
                    },
                    "normalized": {
                        "current_ratio": current_ratio,
                        "liability_to_asset": liability_to_asset,
                        "cfo_to_revenue": cfo_to_revenue,
                        "cfo_to_net_profit": cfo_to_net_profit,
                    },
                    "source_reported": self._compact_dict(finance_info),
                }

                snapshots.append(
                    FinancialSummarySnapshot(
                        instrument_id=instrument_id,
                        symbol=instrument.get("symbol", code),
                        exchange=instrument.get("exchange", exchange),
                        pub_date=updated_date,
                        current_ratio=current_ratio,
                        liability_to_asset=liability_to_asset,
                        cfo_to_revenue=cfo_to_revenue,
                        cfo_to_net_profit=cfo_to_net_profit,
                        source=self.source_name,
                        source_mode="direct",
                        summary_json=summary_json,
                        raw_payload={"finance_info": finance_info},
                    )
                )
        finally:
            if managed_connection:
                try:
                    api.disconnect()
                except Exception:
                    pass

        return snapshots

    @staticmethod
    def _normalize_mapping(value: Any) -> Dict[str, Any]:
        if not value:
            return {}
        if hasattr(value, "items"):
            return dict(value.items())
        if isinstance(value, dict):
            return dict(value)
        return {}

    @staticmethod
    def _compact_dict(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: value
            for key, value in row.items()
            if value not in {None, "", "--"}
        }
