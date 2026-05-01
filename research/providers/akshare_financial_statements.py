"""
AkShare-backed financial statements provider.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .akshare_support import load_akshare
from .base import (
    BaseFinancialStatementsProvider,
    FinancialFactsSnapshot,
    FinancialIndicatorSnapshot,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
)


class AkshareFinancialStatementsProvider(BaseFinancialStatementsProvider):
    """Fetch latest financial statement bundles through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    _report_period_aliases = (
        "REPORT_DATE",
        "REPORTDATE",
        "REPORT_PERIOD",
        "报告日期",
        "报告期",
    )
    _publish_date_aliases = (
        "NOTICE_DATE",
        "ANNOUNCE_DATE",
        "PUBLISH_DATE",
        "公告日期",
        "披露日期",
        "更新时间",
    )
    _revenue_aliases = (
        "TOTAL_OPERATE_INCOME",
        "OPERATE_INCOME",
        "TOTAL_OPERATE_REVENUE",
        "营业总收入",
        "营业收入",
    )
    _operating_cost_aliases = (
        "OPERATE_COST",
        "TOTAL_OPERATE_COST",
        "营业成本",
        "营业总成本",
    )
    _gross_profit_aliases = (
        "GROSS_PROFIT",
        "毛利润",
        "毛利",
    )
    _operating_profit_aliases = (
        "OPERATE_PROFIT",
        "营业利润",
    )
    _pre_tax_profit_aliases = (
        "TOTAL_PROFIT",
        "利润总额",
    )
    _net_income_aliases = (
        "PARENT_NETPROFIT",
        "NETPROFIT",
        "归属于母公司股东的净利润",
        "净利润",
    )
    _operating_cf_aliases = (
        "NETCASH_OPERATE",
        "经营活动产生的现金流量净额",
    )
    _total_cf_aliases = (
        "NETCASH_INCREASE_CASH",
        "现金及现金等价物净增加额",
        "总现金流",
    )
    _total_assets_aliases = (
        "TOTAL_ASSETS",
        "资产总计",
        "总资产",
    )
    _total_liabilities_aliases = (
        "TOTAL_LIABILITIES",
        "负债合计",
        "总负债",
    )
    _equity_aliases = (
        "TOTAL_EQUITY",
        "归属于母公司股东权益合计",
        "股东权益合计",
        "所有者权益合计",
        "净资产",
    )
    _current_assets_aliases = (
        "TOTAL_CURRENT_ASSETS",
        "流动资产合计",
        "流动资产",
    )
    _current_liabilities_aliases = (
        "TOTAL_CURRENT_LIAB",
        "流动负债合计",
        "流动负债",
    )
    _inventory_aliases = (
        "INVENTORY",
        "存货",
    )
    _receivables_aliases = (
        "ACCOUNTS_RECE",
        "应收账款",
    )
    _fixed_assets_aliases = (
        "FIXED_ASSET",
        "固定资产",
    )
    _intangible_assets_aliases = (
        "INTANGIBLE_ASSET",
        "无形资产",
    )
    _shares_outstanding_aliases = (
        "TOTAL_SHARE",
        "SHARE_CAPITAL",
        "总股本",
    )

    async def fetch_financial_statement_bundles(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
        report_periods: Optional[List[str]] = None,
    ) -> List[FinancialStatementBundle]:
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
            self._fetch_financial_statement_bundles_sync,
            target_instruments,
            mode,
            report_periods,
        )

    def _fetch_financial_statement_bundles_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        mode: str,
        report_periods: Optional[List[str]] = None,
    ) -> List[FinancialStatementBundle]:
        akshare_module = self._akshare(mode)
        bundles: List[FinancialStatementBundle] = []
        target_periods = set(report_periods or [])

        for instrument in target_instruments:
            symbol = self._to_akshare_symbol(instrument.get("instrument_id", ""))
            if symbol is None:
                continue

            balance_df = akshare_module.stock_balance_sheet_by_report_em(symbol=symbol)
            profit_df = akshare_module.stock_profit_sheet_by_report_em(symbol=symbol)
            cashflow_df = akshare_module.stock_cash_flow_sheet_by_report_em(symbol=symbol)

            instrument_bundles = self._build_bundles(
                instrument=instrument,
                mode=mode,
                balance_df=balance_df,
                profit_df=profit_df,
                cashflow_df=cashflow_df,
                report_periods=target_periods or None,
            )
            bundles.extend(instrument_bundles)

        return bundles

    def _build_bundles(
        self,
        *,
        instrument: Dict[str, Any],
        mode: str,
        balance_df: Optional[pd.DataFrame],
        profit_df: Optional[pd.DataFrame],
        cashflow_df: Optional[pd.DataFrame],
        report_periods: Optional[set[str]] = None,
    ) -> List[FinancialStatementBundle]:
        rows_by_type = {
            "balance_sheet": self._index_statement_rows(balance_df),
            "profit_sheet": self._index_statement_rows(profit_df),
            "cash_flow_sheet": self._index_statement_rows(cashflow_df),
        }
        candidate_periods = {
            report_period
            for rows in rows_by_type.values()
            for report_period in rows.keys()
        }
        if not candidate_periods:
            return []

        if report_periods is not None:
            candidate_periods = {
                period for period in candidate_periods if period in report_periods
            }
            if not candidate_periods:
                return []

        bundles: List[FinancialStatementBundle] = []
        for report_period in sorted(candidate_periods, reverse=True):
            bundle = self._build_bundle_for_period(
                instrument=instrument,
                mode=mode,
                rows_by_type=rows_by_type,
                report_period=report_period,
            )
            if bundle is not None:
                bundles.append(bundle)
        return bundles

    def _build_bundle_for_period(
        self,
        *,
        instrument: Dict[str, Any],
        mode: str,
        rows_by_type: Dict[str, Dict[str, Dict[str, Any]]],
        report_period: str,
    ) -> Optional[FinancialStatementBundle]:
        aligned_rows = {
            statement_type: rows.get(report_period)
            for statement_type, rows in rows_by_type.items()
        }

        publish_date = self._pick_first_text(
            aligned_rows["profit_sheet"],
            self._publish_date_aliases,
        ) or self._pick_first_text(
            aligned_rows["balance_sheet"],
            self._publish_date_aliases,
        ) or self._pick_first_text(
            aligned_rows["cash_flow_sheet"],
            self._publish_date_aliases,
        )

        fiscal_year, fiscal_quarter = self._derive_fiscal_period(report_period)
        symbol = instrument.get("symbol", "")
        instrument_id = instrument.get("instrument_id", "")
        exchange = instrument.get("exchange", "")

        raw_statements = [
            FinancialStatementRawSnapshot(
                instrument_id=instrument_id,
                symbol=symbol,
                exchange=exchange,
                statement_type=statement_type,
                report_period=report_period,
                publish_date=publish_date,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                source=self.source_name,
                source_mode=mode,
                statement_json=row or {},
            )
            for statement_type, row in aligned_rows.items()
            if row
        ]
        if not raw_statements:
            return None

        facts = self._build_facts_snapshot(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            publish_date=publish_date,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            mode=mode,
            balance_row=aligned_rows["balance_sheet"],
            profit_row=aligned_rows["profit_sheet"],
            cashflow_row=aligned_rows["cash_flow_sheet"],
        )
        indicators = self._build_indicator_snapshot(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            publish_date=publish_date,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            mode=mode,
            facts=facts,
        )

        return FinancialStatementBundle(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            publish_date=publish_date,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            source=self.source_name,
            source_mode=mode,
            raw_statements=raw_statements,
            facts=facts,
            indicators=indicators,
            raw_payload={
                "balance_sheet": aligned_rows["balance_sheet"] or {},
                "profit_sheet": aligned_rows["profit_sheet"] or {},
                "cash_flow_sheet": aligned_rows["cash_flow_sheet"] or {},
            },
        )

    def _build_facts_snapshot(
        self,
        *,
        instrument_id: str,
        symbol: str,
        exchange: str,
        report_period: str,
        publish_date: Optional[str],
        fiscal_year: Optional[int],
        fiscal_quarter: Optional[int],
        mode: str,
        balance_row: Optional[Dict[str, Any]],
        profit_row: Optional[Dict[str, Any]],
        cashflow_row: Optional[Dict[str, Any]],
    ) -> FinancialFactsSnapshot:
        revenue = self._pick_first_float(profit_row, self._revenue_aliases)
        gross_profit = self._pick_first_float(profit_row, self._gross_profit_aliases)
        if gross_profit is None:
            operating_cost = self._pick_first_float(profit_row, self._operating_cost_aliases)
            if revenue is not None and operating_cost is not None:
                gross_profit = revenue - operating_cost

        total_assets = self._pick_first_float(balance_row, self._total_assets_aliases)
        total_liabilities = self._pick_first_float(balance_row, self._total_liabilities_aliases)
        equity = self._pick_first_float(balance_row, self._equity_aliases)
        if equity is None and total_assets is not None and total_liabilities is not None:
            equity = total_assets - total_liabilities

        current_assets = self._pick_first_float(balance_row, self._current_assets_aliases)
        current_liabilities = self._pick_first_float(
            balance_row,
            self._current_liabilities_aliases,
        )

        facts_json = {
            "profit_sheet": profit_row or {},
            "balance_sheet": balance_row or {},
            "cash_flow_sheet": cashflow_row or {},
        }

        return FinancialFactsSnapshot(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            publish_date=publish_date,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            revenue=revenue,
            gross_profit=gross_profit,
            operating_profit=self._pick_first_float(
                profit_row,
                self._operating_profit_aliases,
            ),
            pre_tax_profit=self._pick_first_float(
                profit_row,
                self._pre_tax_profit_aliases,
            ),
            net_income=self._pick_first_float(profit_row, self._net_income_aliases),
            operating_cf=self._pick_first_float(cashflow_row, self._operating_cf_aliases),
            total_cf=self._pick_first_float(cashflow_row, self._total_cf_aliases),
            total_assets=total_assets,
            total_liabilities=total_liabilities,
            equity=equity,
            current_assets=current_assets,
            current_liabilities=current_liabilities,
            inventory=self._pick_first_float(balance_row, self._inventory_aliases),
            receivables=self._pick_first_float(balance_row, self._receivables_aliases),
            fixed_assets=self._pick_first_float(balance_row, self._fixed_assets_aliases),
            intangible_assets=self._pick_first_float(
                balance_row,
                self._intangible_assets_aliases,
            ),
            shares_outstanding=self._pick_first_float(
                balance_row,
                self._shares_outstanding_aliases,
            ) or self._pick_first_float(
                profit_row,
                self._shares_outstanding_aliases,
            ),
            source=self.source_name,
            source_mode=mode,
            facts_json=facts_json,
        )

    def _build_indicator_snapshot(
        self,
        *,
        instrument_id: str,
        symbol: str,
        exchange: str,
        report_period: str,
        publish_date: Optional[str],
        fiscal_year: Optional[int],
        fiscal_quarter: Optional[int],
        mode: str,
        facts: FinancialFactsSnapshot,
    ) -> FinancialIndicatorSnapshot:
        gross_margin = self._safe_div(facts.gross_profit, facts.revenue)
        operating_margin = self._safe_div(facts.operating_profit, facts.revenue)
        net_margin = self._safe_div(facts.net_income, facts.revenue)
        roe = self._safe_div(facts.net_income, facts.equity)
        roa = self._safe_div(facts.net_income, facts.total_assets)
        current_ratio = self._safe_div(facts.current_assets, facts.current_liabilities)

        quick_assets = None
        if facts.current_assets is not None:
            quick_assets = facts.current_assets - (facts.inventory or 0.0)
        quick_ratio = self._safe_div(quick_assets, facts.current_liabilities)

        indicators_json = {
            "derived_from": facts.facts_json,
            "calculated": {
                "gross_margin": gross_margin,
                "operating_margin": operating_margin,
                "net_margin": net_margin,
                "roe": roe,
                "roa": roa,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
            },
        }

        return FinancialIndicatorSnapshot(
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            publish_date=publish_date,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            gross_margin=gross_margin,
            operating_margin=operating_margin,
            net_margin=net_margin,
            roe=roe,
            roa=roa,
            current_ratio=current_ratio,
            quick_ratio=quick_ratio,
            asset_liability_ratio=self._safe_div(
                facts.total_liabilities,
                facts.total_assets,
            ),
            revenue_per_share=self._safe_div(facts.revenue, facts.shares_outstanding),
            operating_cf_to_revenue=self._safe_div(facts.operating_cf, facts.revenue),
            operating_cf_to_net_income=self._safe_div(
                facts.operating_cf,
                facts.net_income,
            ),
            book_value_per_share=self._safe_div(facts.equity, facts.shares_outstanding),
            source=self.source_name,
            source_mode=mode,
            indicators_json=indicators_json,
        )

    def _index_statement_rows(
        self,
        dataframe: Optional[pd.DataFrame],
    ) -> Dict[str, Dict[str, Any]]:
        if dataframe is None or dataframe.empty:
            return {}

        rows: Dict[str, Dict[str, Any]] = {}
        for row in dataframe.to_dict("records"):
            report_period = self._pick_first_text(row, self._report_period_aliases)
            if report_period is None:
                continue
            rows[report_period] = row
        return rows

    @staticmethod
    def _latest_row(rows: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not rows:
            return None
        return rows[max(rows.keys())]

    def _pick_first_text(
        self,
        row: Optional[Dict[str, Any]],
        aliases: tuple[str, ...],
    ) -> Optional[str]:
        if not row:
            return None

        for alias in aliases:
            value = row.get(alias)
            if value is None:
                continue
            if pd.isna(value):
                continue
            text = str(value).strip()
            if text and text not in {"--", "nan", "NaT"}:
                return text[:10] if len(text) >= 10 and text[4] == "-" else text
        return None

    def _pick_first_float(
        self,
        row: Optional[Dict[str, Any]],
        aliases: tuple[str, ...],
    ) -> Optional[float]:
        if not row:
            return None

        for alias in aliases:
            value = row.get(alias)
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            if pd.isna(value):
                continue
            numeric = self._to_float(value)
            if numeric is not None:
                return numeric
        return None

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value in {None, "", "--"}:
            return None
        if isinstance(value, str):
            normalized = value.replace(",", "").replace("%", "").strip()
        else:
            normalized = value
        try:
            return float(normalized)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
        if numerator is None or denominator in {None, 0.0}:
            return None
        return numerator / denominator

    @staticmethod
    def _derive_fiscal_period(report_period: str) -> tuple[Optional[int], Optional[int]]:
        try:
            value = datetime.fromisoformat(report_period)
        except ValueError:
            return None, None

        quarter = ((value.month - 1) // 3) + 1
        return value.year, quarter

    @staticmethod
    def _to_akshare_symbol(instrument_id: str) -> Optional[str]:
        parts = instrument_id.rsplit(".", 1)
        if len(parts) != 2:
            return None

        code, suffix = parts
        suffix = suffix.upper()
        if suffix == "SH":
            return f"SH{code}"
        if suffix == "SZ":
            return f"SZ{code}"
        if suffix in {"BJ", "BSE"}:
            return f"BJ{code}"
        return None

    @staticmethod
    def _akshare(mode: str = "direct"):
        return load_akshare(mode)
