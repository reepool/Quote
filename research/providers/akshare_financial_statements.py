"""
AkShare-backed financial statements provider.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from research.financial_fact_aliases import describe_core_financial_fact_alias
from utils import dm_logger
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
    _supported_statement_interfaces = {"sina_report", "ths_report", "eastmoney_report"}

    _report_period_aliases = (
        "REPORT_DATE",
        "REPORTDATE",
        "REPORT_PERIOD",
        "report_date",
        "report_period",
        "报告日期",
        "报告期",
        "报告日",
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
        "operating_income_total",
        "operating_income",
        "营业总收入",
        "营业收入",
    )
    _operating_cost_aliases = (
        "OPERATE_COST",
        "TOTAL_OPERATE_COST",
        "operating_cost_total",
        "operating_costs",
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
        "operating_profit",
        "营业利润",
    )
    _pre_tax_profit_aliases = (
        "TOTAL_PROFIT",
        "profit_total",
        "利润总额",
    )
    _net_income_aliases = (
        "PARENT_NETPROFIT",
        "parent_holder_net_profit",
        "归属于母公司股东的净利润",
        "归属于母公司的净利润",
        "归属于母公司所有者的净利润",
        "归属母公司净利润",
        "net_profit",
        "NETPROFIT",
        "净利润",
    )
    _minority_net_income_aliases = (
        "MINORITY_INTEREST_INCOME",
        "minority_holder_income_loss",
        "少数股东损益",
        "少数股东利润",
        "少数股东收益",
    )
    _operating_cf_aliases = (
        "NETCASH_OPERATE",
        "act_cash_flow_net",
        "经营活动产生的现金流量净额",
    )
    _total_cf_aliases = (
        "NETCASH_INCREASE_CASH",
        "cash_net_addition",
        "现金及现金等价物净增加额",
        "总现金流",
    )
    _total_assets_aliases = (
        "TOTAL_ASSETS",
        "assets_total",
        "资产总计",
        "总资产",
    )
    _total_liabilities_aliases = (
        "TOTAL_LIABILITIES",
        "total_debt",
        "负债合计",
        "总负债",
    )
    _equity_aliases = (
        "parent_holder_equity_total",
        "归属于母公司股东权益合计",
        "归属于母公司股东的权益",
        "归属于母公司所有者权益合计",
        "归属于母公司所有者权益",
        "PARENT_EQUITY",
        "TOTAL_PARENT_EQUITY",
        "TOTAL_EQUITY",
        "股东权益合计",
        "所有者权益合计",
        "所有者权益",
        "净资产",
    )
    _minority_equity_aliases = (
        "MINORITY_EQUITY",
        "MINORITY_INTEREST",
        "minority_equity",
        "少数股东权益",
        "少数股东权益合计",
    )
    _current_assets_aliases = (
        "TOTAL_CURRENT_ASSETS",
        "total_current_assets",
        "流动资产合计",
        "流动资产",
    )
    _current_liabilities_aliases = (
        "TOTAL_CURRENT_LIAB",
        "current_total_debt",
        "流动负债合计",
        "流动负债",
    )
    _inventory_aliases = (
        "INVENTORY",
        "inventories",
        "存货",
    )
    _receivables_aliases = (
        "ACCOUNTS_RECE",
        "account_receivable",
        "应收账款",
    )
    _fixed_assets_aliases = (
        "FIXED_ASSET",
        "fixed_assets",
        "固定资产",
    )
    _intangible_assets_aliases = (
        "INTANGIBLE_ASSET",
        "intangible_assets",
        "无形资产",
    )
    _shares_outstanding_aliases = (
        "SharesOutstanding",
        "TotalSharesOutstanding",
    )

    def __init__(self, provider_config: Optional[Dict[str, Any]] = None):
        self.provider_config = provider_config or {}
        self.statement_interface_order = self._resolve_statement_interface_order(
            self._configured_statement_interface_order()
        )

    def _configured_statement_interface_order(self) -> Any:
        service_layers = self.provider_config.get("service_layers") or {}
        local_core_cfg = service_layers.get("local_core") or {}
        if bool(local_core_cfg.get("enabled", False)):
            source_order = local_core_cfg.get("source_order")
            if source_order:
                return source_order
        return self.provider_config.get("statement_interface_order")

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
            if target_periods:
                bundles.extend(
                    self._fetch_target_period_bundles_with_statement_fallback(
                        akshare_module,
                        instrument=instrument,
                        mode=mode,
                        target_periods=target_periods,
                    )
                )
                continue

            for statement_interface in self.statement_interface_order:
                try:
                    balance_df, profit_df, cashflow_df = self._fetch_statement_frames(
                        akshare_module,
                        instrument=instrument,
                        statement_interface=statement_interface,
                    )
                except Exception as exc:
                    dm_logger.warning(
                        "[AkshareFinancialStatementsProvider] %s failed for %s: %s",
                        statement_interface,
                        instrument.get("instrument_id"),
                        exc,
                    )
                    continue

                instrument_bundles = self._build_bundles(
                    instrument=instrument,
                    mode=mode,
                    balance_df=balance_df,
                    profit_df=profit_df,
                    cashflow_df=cashflow_df,
                    report_periods=target_periods or None,
                    statement_interface=statement_interface,
                )
                if instrument_bundles:
                    bundles.extend(instrument_bundles)
                    break

        return bundles

    def _fetch_target_period_bundles_with_statement_fallback(
        self,
        akshare_module: Any,
        *,
        instrument: Dict[str, Any],
        mode: str,
        target_periods: set[str],
    ) -> List[FinancialStatementBundle]:
        rows_by_type: Dict[str, Dict[str, Dict[str, Any]]] = {
            "balance_sheet": {},
            "profit_sheet": {},
            "cash_flow_sheet": {},
        }
        statement_interfaces_by_period: Dict[str, Dict[str, str]] = {}

        for statement_interface in self.statement_interface_order:
            try:
                balance_df, profit_df, cashflow_df = self._fetch_statement_frames(
                    akshare_module,
                    instrument=instrument,
                    statement_interface=statement_interface,
                )
            except Exception as exc:
                dm_logger.warning(
                    "[AkshareFinancialStatementsProvider] %s failed for %s: %s",
                    statement_interface,
                    instrument.get("instrument_id"),
                    exc,
                )
                continue

            current_rows_by_type = {
                "balance_sheet": self._index_statement_rows(balance_df),
                "profit_sheet": self._index_statement_rows(profit_df),
                "cash_flow_sheet": self._index_statement_rows(cashflow_df),
            }
            for statement_type, rows in current_rows_by_type.items():
                for report_period, row in rows.items():
                    if report_period not in target_periods:
                        continue
                    if report_period in rows_by_type[statement_type]:
                        continue
                    rows_by_type[statement_type][report_period] = row
                    statement_interfaces_by_period.setdefault(report_period, {})[
                        statement_type
                    ] = statement_interface

            if self._target_statement_coverage_complete(rows_by_type, target_periods):
                break

        return self._build_bundles_from_indexed_rows(
            instrument=instrument,
            mode=mode,
            rows_by_type=rows_by_type,
            report_periods=target_periods,
            statement_interfaces_by_period=statement_interfaces_by_period,
        )

    @staticmethod
    def _target_statement_coverage_complete(
        rows_by_type: Dict[str, Dict[str, Dict[str, Any]]],
        target_periods: set[str],
    ) -> bool:
        if not target_periods:
            return False
        return all(
            report_period in rows_by_type.get(statement_type, {})
            for report_period in target_periods
            for statement_type in ("balance_sheet", "profit_sheet", "cash_flow_sheet")
        )

    def _fetch_statement_frames(
        self,
        akshare_module: Any,
        *,
        instrument: Dict[str, Any],
        statement_interface: str,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if statement_interface == "sina_report":
            symbol = self._to_sina_stock_symbol(str(instrument.get("instrument_id", "")))
            if symbol is None:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            return (
                akshare_module.stock_financial_report_sina(
                    stock=symbol,
                    symbol="资产负债表",
                ),
                akshare_module.stock_financial_report_sina(
                    stock=symbol,
                    symbol="利润表",
                ),
                akshare_module.stock_financial_report_sina(
                    stock=symbol,
                    symbol="现金流量表",
                ),
            )

        if statement_interface == "eastmoney_report":
            symbol = self._to_eastmoney_stock_symbol(
                str(instrument.get("instrument_id", ""))
            )
            if symbol is None:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            return (
                akshare_module.stock_balance_sheet_by_report_em(symbol=symbol),
                akshare_module.stock_profit_sheet_by_report_em(symbol=symbol),
                akshare_module.stock_cash_flow_sheet_by_report_em(symbol=symbol),
            )

        if statement_interface == "ths_report":
            symbol = self._to_ths_stock_symbol(str(instrument.get("instrument_id", "")))
            if symbol is None:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            return (
                akshare_module.stock_financial_debt_new_ths(
                    symbol=symbol,
                    indicator="按报告期",
                ),
                akshare_module.stock_financial_benefit_new_ths(
                    symbol=symbol,
                    indicator="按报告期",
                ),
                akshare_module.stock_financial_cash_new_ths(
                    symbol=symbol,
                    indicator="按报告期",
                ),
            )

        raise ValueError(f"unsupported AkShare statement interface: {statement_interface}")

    def _build_bundles(
        self,
        *,
        instrument: Dict[str, Any],
        mode: str,
        balance_df: Optional[pd.DataFrame],
        profit_df: Optional[pd.DataFrame],
        cashflow_df: Optional[pd.DataFrame],
        report_periods: Optional[set[str]] = None,
        statement_interface: Optional[str] = None,
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
                statement_interface=statement_interface,
            )
            if bundle is not None:
                bundles.append(bundle)
        return bundles

    def _build_bundles_from_indexed_rows(
        self,
        *,
        instrument: Dict[str, Any],
        mode: str,
        rows_by_type: Dict[str, Dict[str, Dict[str, Any]]],
        report_periods: set[str],
        statement_interfaces_by_period: Dict[str, Dict[str, str]],
    ) -> List[FinancialStatementBundle]:
        candidate_periods = {
            report_period
            for rows in rows_by_type.values()
            for report_period in rows.keys()
            if report_period in report_periods
        }
        bundles: List[FinancialStatementBundle] = []
        for report_period in sorted(candidate_periods, reverse=True):
            statement_interfaces = statement_interfaces_by_period.get(report_period, {})
            unique_interfaces = {
                source for source in statement_interfaces.values() if source
            }
            bundle = self._build_bundle_for_period(
                instrument=instrument,
                mode=mode,
                rows_by_type=rows_by_type,
                report_period=report_period,
                statement_interface=(
                    next(iter(unique_interfaces))
                    if len(unique_interfaces) == 1
                    else "mixed"
                ),
                statement_interfaces=statement_interfaces,
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
        statement_interface: Optional[str] = None,
        statement_interfaces: Optional[Dict[str, str]] = None,
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
        publish_date = self._normalize_date_text(publish_date)

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
                "akshare_statement_interface": statement_interface,
                "akshare_statement_interfaces": statement_interfaces or {},
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
        core_field_sources: Dict[str, Dict[str, Any]] = {}
        core_fact_warnings: List[Dict[str, Any]] = []
        revenue = self._pick_core_fact_float(
            profit_row,
            self._revenue_aliases,
            "revenue",
            core_field_sources,
            core_fact_warnings,
        )
        gross_profit = self._pick_core_fact_float(
            profit_row,
            self._gross_profit_aliases,
            "gross_profit",
            core_field_sources,
            core_fact_warnings,
        )
        if gross_profit is None:
            operating_cost = self._pick_first_float(profit_row, self._operating_cost_aliases)
            if revenue is not None and operating_cost is not None:
                gross_profit = revenue - operating_cost
                core_field_sources["gross_profit"] = {
                    "core_field": "gross_profit",
                    "derived_from": ["revenue", "operating_cost"],
                    "method": "revenue_minus_operating_cost",
                    "canonical_compatible": True,
                }

        total_assets = self._pick_core_fact_float(
            balance_row,
            self._total_assets_aliases,
            "total_assets",
            core_field_sources,
            core_fact_warnings,
        )
        total_liabilities = self._pick_core_fact_float(
            balance_row,
            self._total_liabilities_aliases,
            "total_liabilities",
            core_field_sources,
            core_fact_warnings,
        )
        equity = self._pick_core_fact_float(
            balance_row,
            self._equity_aliases,
            "equity",
            core_field_sources,
            core_fact_warnings,
        )
        if equity is None:
            equity = self._derive_difference_core_fact(
                balance_row,
                core_field="equity",
                total_aliases=self._equity_aliases,
                minority_aliases=self._minority_equity_aliases,
                method="total_owners_equity_minus_minority_interest",
                core_field_sources=core_field_sources,
            )
            if equity is not None:
                core_fact_warnings = [
                    warning
                    for warning in core_fact_warnings
                    if warning.get("core_field") != "equity"
                ]
        if equity is None and total_assets is not None and total_liabilities is not None:
            core_fact_warnings.append(
                {
                    "core_field": "equity",
                    "warning": "equity_total_vs_parent_ambiguous",
                    "canonical_semantic": "parent_attributable_equity",
                    "alias_semantic": "total_owners_equity",
                    "method": "total_assets_minus_total_liabilities",
                }
            )
            core_field_sources["equity"] = {
                "selected": None,
                "skipped_candidates": [
                    {
                        "core_field": "equity",
                        "method": "total_assets_minus_total_liabilities",
                        "fact_value": total_assets - total_liabilities,
                        "canonical_compatible": False,
                    }
                ],
            }

        current_assets = self._pick_core_fact_float(
            balance_row,
            self._current_assets_aliases,
            "current_assets",
            core_field_sources,
            core_fact_warnings,
        )
        current_liabilities = self._pick_core_fact_float(
            balance_row,
            self._current_liabilities_aliases,
            "current_liabilities",
            core_field_sources,
            core_fact_warnings,
        )
        net_income = self._pick_core_fact_float(
            profit_row,
            self._net_income_aliases,
            "net_income",
            core_field_sources,
            core_fact_warnings,
        )
        if net_income is None:
            net_income = self._derive_difference_core_fact(
                profit_row,
                core_field="net_income",
                total_aliases=self._net_income_aliases,
                minority_aliases=self._minority_net_income_aliases,
                method="total_net_profit_minus_minority_interest_income",
                core_field_sources=core_field_sources,
            )
            if net_income is not None:
                core_fact_warnings = [
                    warning
                    for warning in core_fact_warnings
                    if warning.get("core_field") != "net_income"
                ]

        facts_json = {
            "profit_sheet": profit_row or {},
            "balance_sheet": balance_row or {},
            "cash_flow_sheet": cashflow_row or {},
            "core_fact_alias_matches": core_field_sources,
            "core_fact_warnings": core_fact_warnings,
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
            operating_profit=self._pick_core_fact_float(
                profit_row,
                self._operating_profit_aliases,
                "operating_profit",
                core_field_sources,
                core_fact_warnings,
            ),
            pre_tax_profit=self._pick_core_fact_float(
                profit_row,
                self._pre_tax_profit_aliases,
                "pre_tax_profit",
                core_field_sources,
                core_fact_warnings,
            ),
            net_income=net_income,
            operating_cf=self._pick_core_fact_float(
                cashflow_row,
                self._operating_cf_aliases,
                "operating_cf",
                core_field_sources,
                core_fact_warnings,
            ),
            total_cf=self._pick_core_fact_float(
                cashflow_row,
                self._total_cf_aliases,
                "total_cf",
                core_field_sources,
                core_fact_warnings,
            ),
            total_assets=total_assets,
            total_liabilities=total_liabilities,
            equity=equity,
            current_assets=current_assets,
            current_liabilities=current_liabilities,
            inventory=self._pick_core_fact_float(
                balance_row,
                self._inventory_aliases,
                "inventory",
                core_field_sources,
                core_fact_warnings,
            ),
            receivables=self._pick_core_fact_float(
                balance_row,
                self._receivables_aliases,
                "receivables",
                core_field_sources,
                core_fact_warnings,
            ),
            fixed_assets=self._pick_core_fact_float(
                balance_row,
                self._fixed_assets_aliases,
                "fixed_assets",
                core_field_sources,
                core_fact_warnings,
            ),
            intangible_assets=self._pick_core_fact_float(
                balance_row,
                self._intangible_assets_aliases,
                "intangible_assets",
                core_field_sources,
                core_fact_warnings,
            ),
            shares_outstanding=self._pick_core_fact_float(
                balance_row,
                self._shares_outstanding_aliases,
                "shares_outstanding",
                core_field_sources,
                core_fact_warnings,
            ) or self._pick_core_fact_float(
                profit_row,
                self._shares_outstanding_aliases,
                "shares_outstanding",
                core_field_sources,
                core_fact_warnings,
            ),
            source=self.source_name,
            source_mode=mode,
            facts_json=facts_json,
            lineage_json={
                "core_fact_alias_matches": core_field_sources,
                "core_fact_warnings": core_fact_warnings,
            },
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

        if {"metric_name", "value"}.issubset({str(column) for column in dataframe.columns}):
            return self._index_ths_statement_rows(dataframe)

        rows: Dict[str, Dict[str, Any]] = {}
        for row in dataframe.to_dict("records"):
            report_period = self._pick_first_text(row, self._report_period_aliases)
            if report_period is None:
                continue
            report_period = self._normalize_date_text(report_period)
            if report_period is None:
                continue
            rows[report_period] = row
        return rows

    def _index_ths_statement_rows(
        self,
        dataframe: pd.DataFrame,
    ) -> Dict[str, Dict[str, Any]]:
        rows: Dict[str, Dict[str, Any]] = {}
        value_type_columns = ("single", "yoy", "mom", "single_yoy")

        for source_row in dataframe.to_dict("records"):
            report_period = self._pick_first_text(source_row, self._report_period_aliases)
            report_period = self._normalize_date_text(report_period)
            if report_period is None:
                continue

            metric_name = str(source_row.get("metric_name", "")).strip()
            if not metric_name:
                continue

            row = rows.setdefault(
                report_period,
                {
                    "report_date": report_period,
                    "report_period": report_period,
                    "ths_metrics": {},
                },
            )
            for key in ("report_name", "quarter_name"):
                value = source_row.get(key)
                if value is not None and not pd.isna(value):
                    row[key] = value

            metric_payload = {
                "value": source_row.get("value"),
                **{
                    column: source_row.get(column)
                    for column in value_type_columns
                    if column in source_row
                },
            }
            row["ths_metrics"][metric_name] = metric_payload
            row[metric_name] = source_row.get("value")
            for column in value_type_columns:
                if column in source_row:
                    row[f"{metric_name}__{column}"] = source_row.get(column)
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

    def _pick_core_fact_float(
        self,
        row: Optional[Dict[str, Any]],
        aliases: tuple[str, ...],
        core_field: str,
        core_field_sources: Dict[str, Dict[str, Any]],
        core_fact_warnings: List[Dict[str, Any]],
    ) -> Optional[float]:
        if not row:
            return None

        skipped_candidates: List[Dict[str, Any]] = []
        for alias in aliases:
            value = row.get(alias)
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            if pd.isna(value):
                continue
            numeric = self._to_float(value)
            if numeric is None:
                continue
            alias_info = describe_core_financial_fact_alias(core_field, alias)
            match_info = {
                **alias_info,
                "fact_name": alias,
                "fact_value": numeric,
            }
            if alias_info["canonical_compatible"]:
                core_field_sources[core_field] = match_info
                return numeric
            skipped_candidates.append({**match_info, "skipped": True})
            for warning in alias_info.get("warnings", []):
                core_fact_warnings.append(
                    {
                        "core_field": core_field,
                        "fact_name": alias,
                        "fact_value": numeric,
                        "warning": warning,
                        "canonical_semantic": alias_info.get("canonical_semantic"),
                        "alias_semantic": alias_info.get("alias_semantic"),
                    }
                )
        if skipped_candidates:
            core_field_sources[core_field] = {
                "selected": None,
                "skipped_candidates": skipped_candidates,
            }
        return None

    def _derive_difference_core_fact(
        self,
        row: Optional[Dict[str, Any]],
        *,
        core_field: str,
        total_aliases: tuple[str, ...],
        minority_aliases: tuple[str, ...],
        method: str,
        core_field_sources: Dict[str, Dict[str, Any]],
    ) -> Optional[float]:
        total = self._pick_first_float_with_alias(row, total_aliases)
        minority = self._pick_first_float_with_alias(row, minority_aliases)
        if total is None or minority is None:
            return None
        value = total["fact_value"] - minority["fact_value"]
        core_field_sources[core_field] = {
            "core_field": core_field,
            "fact_name": f"{total['fact_name']}-{minority['fact_name']}",
            "fact_value": value,
            "method": method,
            "canonical_compatible": True,
            "derived": True,
            "components": {
                "total": total,
                "minority": minority,
            },
        }
        return value

    def _pick_first_float_with_alias(
        self,
        row: Optional[Dict[str, Any]],
        aliases: tuple[str, ...],
    ) -> Optional[Dict[str, Any]]:
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
                return {"fact_name": alias, "fact_value": numeric}
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
    def _to_eastmoney_stock_symbol(instrument_id: str) -> Optional[str]:
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
    def _to_sina_stock_symbol(instrument_id: str) -> Optional[str]:
        parts = instrument_id.rsplit(".", 1)
        if len(parts) != 2:
            return None

        code, suffix = parts
        suffix = suffix.upper()
        if suffix == "SH":
            return f"sh{code}"
        if suffix == "SZ":
            return f"sz{code}"
        if suffix in {"BJ", "BSE"}:
            return f"bj{code}"
        return None

    @staticmethod
    def _to_ths_stock_symbol(instrument_id: str) -> Optional[str]:
        parts = instrument_id.rsplit(".", 1)
        if len(parts) != 2:
            return None

        code, suffix = parts
        if suffix.upper() in {"SH", "SZ", "BJ", "BSE"}:
            return code
        return None

    @classmethod
    def _resolve_statement_interface_order(cls, raw_value: Any) -> List[str]:
        if raw_value is None:
            return ["sina_report", "eastmoney_report"]
        if isinstance(raw_value, str):
            values = [item.strip() for item in raw_value.split(",")]
        else:
            values = [str(item).strip() for item in raw_value]
        resolved = [
            item
            for item in values
            if item and item in cls._supported_statement_interfaces
        ]
        return resolved or ["sina_report", "eastmoney_report"]

    @staticmethod
    def _normalize_date_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text in {"--", "nan", "NaT"}:
            return None
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        return text

    @staticmethod
    def _akshare(mode: str = "direct"):
        return load_akshare(mode)
