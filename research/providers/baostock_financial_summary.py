"""
BaoStock-backed financial summary provider.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import baostock as bs

from .base import BaseFinancialSummaryProvider, FinancialSummarySnapshot


class BaostockFinancialSummaryProvider(BaseFinancialSummaryProvider):
    """Fetch latest quarterly financial summary snapshots from BaoStock."""

    source_name = "baostock"
    supported_modes = {"direct"}
    _lock = threading.Lock()

    def __init__(self, *, lookback_quarters: int = 6):
        self._lookback_quarters = max(1, lookback_quarters)

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
        )

    def _fetch_financial_summaries_sync(
        self,
        target_instruments: List[Dict[str, Any]],
    ) -> List[FinancialSummarySnapshot]:
        candidate_periods = self._candidate_periods()

        with self._lock:
            login_result = bs.login()
            if getattr(login_result, "error_code", "1") != "0":
                raise RuntimeError(f"BaoStock login failed: {login_result.error_msg}")

            try:
                snapshots: List[FinancialSummarySnapshot] = []
                for instrument in target_instruments:
                    snapshot = self._fetch_instrument_snapshot(
                        instrument=instrument,
                        candidate_periods=candidate_periods,
                    )
                    if snapshot is not None:
                        snapshots.append(snapshot)
            finally:
                try:
                    bs.logout()
                except Exception:
                    pass

        return snapshots

    def _fetch_instrument_snapshot(
        self,
        *,
        instrument: Dict[str, Any],
        candidate_periods: List[Tuple[int, int]],
    ) -> Optional[FinancialSummarySnapshot]:
        instrument_id = instrument.get("instrument_id", "")
        baostock_code = self._to_baostock_code(instrument_id)
        if baostock_code is None:
            return None

        for fiscal_year, fiscal_quarter in candidate_periods:
            profit = self._query_optional_row(bs.query_profit_data(baostock_code, fiscal_year, fiscal_quarter))
            operation = self._query_optional_row(bs.query_operation_data(baostock_code, fiscal_year, fiscal_quarter))
            growth = self._query_optional_row(bs.query_growth_data(baostock_code, fiscal_year, fiscal_quarter))
            balance = self._query_optional_row(bs.query_balance_data(baostock_code, fiscal_year, fiscal_quarter))
            cashflow = self._query_optional_row(bs.query_cash_flow_data(baostock_code, fiscal_year, fiscal_quarter))
            dupont = self._query_optional_row(bs.query_dupont_data(baostock_code, fiscal_year, fiscal_quarter))

            if not any((profit, operation, growth, balance, cashflow, dupont)):
                continue

            report_date = self._pick_first_value(
                "statDate",
                profit,
                operation,
                growth,
                balance,
                cashflow,
                dupont,
            )
            pub_date = self._pick_first_value(
                "pubDate",
                profit,
                operation,
                growth,
                balance,
                cashflow,
                dupont,
            )

            normalized = {
                "roe": self._to_float(profit, "roeAvg"),
                "gross_margin": self._to_float(profit, "gpMargin"),
                "net_margin": self._to_float(profit, "npMargin"),
                "current_ratio": self._to_float(balance, "currentRatio"),
                "quick_ratio": self._to_float(balance, "quickRatio"),
                "liability_to_asset": self._to_float(balance, "liabilityToAsset"),
                "yoy_asset": self._to_float(growth, "YOYAsset"),
                "yoy_equity": self._to_float(growth, "YOYEquity"),
                "yoy_net_profit": self._first_float(growth, "YOYPNI", "YOYNI"),
                "cfo_to_revenue": self._to_float(cashflow, "CFOToOR"),
                "cfo_to_net_profit": self._to_float(cashflow, "CFOToNP"),
                "asset_turnover": self._to_float(operation, "AssetTurnRatio"),
                "eps": self._to_float(profit, "epsTTM"),
            }

            summary_json = {
                "reporting": {
                    "report_date": report_date,
                    "pub_date": pub_date,
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": fiscal_quarter,
                },
                "normalized": normalized,
                "source_reported": {
                    "profit": self._compact_dict(profit),
                    "operation": self._compact_dict(operation),
                    "growth": self._compact_dict(growth),
                    "balance": self._compact_dict(balance),
                    "cashflow": self._compact_dict(cashflow),
                    "dupont": self._compact_dict(dupont),
                },
            }

            raw_payload = {
                "profit": profit or {},
                "operation": operation or {},
                "growth": growth or {},
                "balance": balance or {},
                "cashflow": cashflow or {},
                "dupont": dupont or {},
            }

            return FinancialSummarySnapshot(
                instrument_id=instrument_id,
                symbol=instrument.get("symbol", instrument_id),
                exchange=instrument.get("exchange", ""),
                report_date=report_date,
                pub_date=pub_date,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                roe=normalized["roe"],
                gross_margin=normalized["gross_margin"],
                net_margin=normalized["net_margin"],
                current_ratio=normalized["current_ratio"],
                quick_ratio=normalized["quick_ratio"],
                liability_to_asset=normalized["liability_to_asset"],
                yoy_asset=normalized["yoy_asset"],
                yoy_equity=normalized["yoy_equity"],
                yoy_net_profit=normalized["yoy_net_profit"],
                cfo_to_revenue=normalized["cfo_to_revenue"],
                cfo_to_net_profit=normalized["cfo_to_net_profit"],
                asset_turnover=normalized["asset_turnover"],
                eps=normalized["eps"],
                source=self.source_name,
                source_mode="direct",
                summary_json=summary_json,
                raw_payload=raw_payload,
            )

        return None

    def _candidate_periods(self) -> List[Tuple[int, int]]:
        today = date.today()
        quarter = ((today.month - 1) // 3) + 1
        year = today.year

        periods: List[Tuple[int, int]] = []
        for _ in range(self._lookback_quarters):
            periods.append((year, quarter))
            quarter -= 1
            if quarter == 0:
                year -= 1
                quarter = 4
        return periods

    @staticmethod
    def _query_optional_row(result_set: Any) -> Optional[Dict[str, Any]]:
        if getattr(result_set, "error_code", "1") != "0":
            return None

        fields = list(getattr(result_set, "fields", []))
        if not result_set.next():
            return None
        return dict(zip(fields, result_set.get_row_data()))

    @staticmethod
    def _to_baostock_code(instrument_id: str) -> Optional[str]:
        if instrument_id.endswith(".SH"):
            return f"sh.{instrument_id[:-3]}"
        if instrument_id.endswith(".SZ"):
            return f"sz.{instrument_id[:-3]}"
        if instrument_id.endswith(".BJ"):
            return f"bj.{instrument_id[:-3]}"
        return None

    @staticmethod
    def _pick_first_value(key: str, *rows: Optional[Dict[str, Any]]) -> Optional[str]:
        for row in rows:
            if not row:
                continue
            value = row.get(key)
            if value not in {None, "", "--"}:
                return str(value)
        return None

    @staticmethod
    def _to_float(row: Optional[Dict[str, Any]], key: str) -> Optional[float]:
        if not row:
            return None
        value = row.get(key)
        if value in {None, "", "--"}:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _first_float(cls, row: Optional[Dict[str, Any]], *keys: str) -> Optional[float]:
        for key in keys:
            value = cls._to_float(row, key)
            if value is not None:
                return value
        return None

    @staticmethod
    def _compact_dict(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not row:
            return {}
        return {
            key: value
            for key, value in row.items()
            if value not in {None, "", "--"}
        }
