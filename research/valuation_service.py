"""
Valuation history, relative valuation, and DCF helpers for research APIs.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from copy import deepcopy
from statistics import fmean, median
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .providers.base import ValuationHistorySnapshot


DEFAULT_VALUATION_PARAMETERS: Dict[str, Any] = {
    "history": {
        "lookback_days": 252,
        "flow_input_mode": "cumulative_ytd",
        "require_availability_date": True,
        "forward_metrics_enabled": False,
    },
    "relative": {
        "taxonomy_system": "sw",
        "benchmark_level": 2,
        "benchmark_field": "sw_l2_code",
        "require_authoritative": True,
        "min_peer_count": 3,
        "max_peer_rows": 20,
        "metric_variants": ["pe_ttm", "pb_mrq", "ps_ttm"],
        "include_compatibility_metrics": True,
    },
    "dcf": {
        "projection_years": 5,
        "risk_free_rate": 0.02,
        "equity_risk_premium": 0.06,
        "default_beta": 1.0,
        "discount_rate": 0.10,
        "terminal_growth": 0.03,
        "base_growth_rate": 0.08,
        "bear_growth_rate": 0.04,
        "bull_growth_rate": 0.12,
        "sensitivity_growth_rates": [0.04, 0.06, 0.08],
        "sensitivity_discount_rates": [0.09, 0.10, 0.11],
    },
}

VALUATION_METRIC_FIELDS: Tuple[str, ...] = (
    "pe_static",
    "pe_ttm",
    "pe_forward",
    "pb_mrq",
    "ps_static",
    "ps_ttm",
    "ps_forward",
)

BENCHMARK_FIELD_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "sw_l1_code": {
        "level": 1,
        "name_field": "sw_l1_name",
        "missing_reason": "sw_l1_membership_not_available",
    },
    "sw_l2_code": {
        "level": 2,
        "name_field": "sw_l2_name",
        "missing_reason": "sw_l2_membership_not_available",
    },
    "sw_l3_code": {
        "level": 3,
        "name_field": "sw_l3_name",
        "missing_reason": "sw_l3_membership_not_available",
    },
}


class BaseDcfEngine(ABC):
    """Base abstraction for DCF implementations."""

    calc_method = "dcf_base"
    calc_version = "dcf_base.v1"

    @abstractmethod
    def run(
        self,
        *,
        instrument: Dict[str, Any],
        financial_bundle: Dict[str, Any],
        latest_close: Optional[float],
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Run a DCF valuation using one normalized financial bundle."""


class SimpleGrowthDcfEngine(BaseDcfEngine):
    """Simple FCF-proxy DCF implementation with scenario and sensitivity outputs."""

    calc_method = "dcf_simple_growth"
    calc_version = "dcf_simple_growth.v1"

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        merged = deepcopy(DEFAULT_VALUATION_PARAMETERS["dcf"])
        self._deep_update(merged, parameters or {})
        self.parameters = merged

    def run(
        self,
        *,
        instrument: Dict[str, Any],
        financial_bundle: Dict[str, Any],
        latest_close: Optional[float],
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        facts = financial_bundle or {}
        overrides = overrides or {}
        shares_outstanding = self._to_positive_float(facts.get("shares_outstanding"))
        base_cash_flow, base_cash_flow_source = self._select_base_cash_flow(facts)
        if base_cash_flow is None or base_cash_flow <= 0:
            return {
                "instrument_id": instrument.get("instrument_id"),
                "symbol": instrument.get("symbol"),
                "exchange": instrument.get("exchange"),
                "calc_method": self.calc_method,
                "calc_version": self.calc_version,
                "parameter_hash": self._build_parameter_hash(overrides),
                "status": "unavailable",
                "missing_reason": "positive_cash_flow_proxy_required",
                "base_cash_flow": None,
                "base_cash_flow_source": base_cash_flow_source,
                "projection_years": int(overrides.get("projection_years", self.parameters["projection_years"])),
                "shares_outstanding": shares_outstanding,
                "latest_close": latest_close,
                "scenarios": [],
                "sensitivity": [],
            }

        discount_rate = self._resolve_discount_rate(overrides)
        terminal_growth = float(
            overrides.get("terminal_growth", self.parameters["terminal_growth"])
        )
        projection_years = int(
            overrides.get("projection_years", self.parameters["projection_years"])
        )
        if discount_rate <= terminal_growth:
            return {
                "instrument_id": instrument.get("instrument_id"),
                "symbol": instrument.get("symbol"),
                "exchange": instrument.get("exchange"),
                "calc_method": self.calc_method,
                "calc_version": self.calc_version,
                "parameter_hash": self._build_parameter_hash(overrides),
                "status": "invalid_parameters",
                "missing_reason": "discount_rate_must_exceed_terminal_growth",
                "base_cash_flow": base_cash_flow,
                "base_cash_flow_source": base_cash_flow_source,
                "projection_years": projection_years,
                "shares_outstanding": shares_outstanding,
                "latest_close": latest_close,
                "scenarios": [],
                "sensitivity": [],
            }

        scenario_inputs = [
            ("bear", float(overrides.get("bear_growth_rate", self.parameters["bear_growth_rate"]))),
            ("base", float(overrides.get("growth_rate", self.parameters["base_growth_rate"]))),
            ("bull", float(overrides.get("bull_growth_rate", self.parameters["bull_growth_rate"]))),
        ]
        scenarios = [
            self._project_scenario(
                name=name,
                base_cash_flow=base_cash_flow,
                growth_rate=growth_rate,
                discount_rate=discount_rate,
                terminal_growth=terminal_growth,
                projection_years=projection_years,
                shares_outstanding=shares_outstanding,
                latest_close=latest_close,
            )
            for name, growth_rate in scenario_inputs
        ]

        sensitivity = []
        growth_grid = overrides.get(
            "sensitivity_growth_rates",
            self.parameters["sensitivity_growth_rates"],
        )
        discount_grid = overrides.get(
            "sensitivity_discount_rates",
            self.parameters["sensitivity_discount_rates"],
        )
        for growth_rate in growth_grid:
            for sensitivity_discount in discount_grid:
                if sensitivity_discount <= terminal_growth:
                    continue
                point = self._project_scenario(
                    name="sensitivity",
                    base_cash_flow=base_cash_flow,
                    growth_rate=float(growth_rate),
                    discount_rate=float(sensitivity_discount),
                    terminal_growth=terminal_growth,
                    projection_years=projection_years,
                    shares_outstanding=shares_outstanding,
                    latest_close=latest_close,
                )
                sensitivity.append(
                    {
                        "growth_rate": float(growth_rate),
                        "discount_rate": float(sensitivity_discount),
                        "intrinsic_value_per_share": point["intrinsic_value_per_share"],
                    }
                )

        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": self.calc_method,
            "calc_version": self.calc_version,
            "parameter_hash": self._build_parameter_hash(overrides),
            "status": "success",
            "missing_reason": None,
            "base_cash_flow": base_cash_flow,
            "base_cash_flow_source": base_cash_flow_source,
            "projection_years": projection_years,
            "shares_outstanding": shares_outstanding,
            "latest_close": latest_close,
            "scenarios": scenarios,
            "sensitivity": sensitivity,
        }

    def _project_scenario(
        self,
        *,
        name: str,
        base_cash_flow: float,
        growth_rate: float,
        discount_rate: float,
        terminal_growth: float,
        projection_years: int,
        shares_outstanding: Optional[float],
        latest_close: Optional[float],
    ) -> Dict[str, Any]:
        projected_cash_flows = []
        running_cash_flow = base_cash_flow
        discounted_sum = 0.0
        for year in range(1, projection_years + 1):
            running_cash_flow *= 1 + growth_rate
            discounted_cash_flow = running_cash_flow / ((1 + discount_rate) ** year)
            projected_cash_flows.append(
                {
                    "year": year,
                    "cash_flow": running_cash_flow,
                    "discounted_cash_flow": discounted_cash_flow,
                }
            )
            discounted_sum += discounted_cash_flow

        terminal_cash_flow = projected_cash_flows[-1]["cash_flow"]
        terminal_value = terminal_cash_flow * (1 + terminal_growth) / (
            discount_rate - terminal_growth
        )
        terminal_value_present = terminal_value / ((1 + discount_rate) ** projection_years)
        equity_value = discounted_sum + terminal_value_present
        intrinsic_value_per_share = (
            equity_value / shares_outstanding
            if shares_outstanding and shares_outstanding > 0
            else None
        )
        upside_to_last_close = None
        if intrinsic_value_per_share is not None and latest_close not in (None, 0):
            upside_to_last_close = intrinsic_value_per_share / latest_close - 1

        return {
            "scenario": name,
            "growth_rate": growth_rate,
            "discount_rate": discount_rate,
            "terminal_growth": terminal_growth,
            "equity_value": equity_value,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "upside_to_last_close": upside_to_last_close,
            "projected_cash_flows": projected_cash_flows,
        }

    def _resolve_discount_rate(self, overrides: Dict[str, Any]) -> float:
        if "discount_rate" in overrides:
            return float(overrides["discount_rate"])

        configured_discount = self.parameters.get("discount_rate")
        if configured_discount is not None:
            return float(configured_discount)

        risk_free_rate = float(self.parameters["risk_free_rate"])
        equity_risk_premium = float(self.parameters["equity_risk_premium"])
        beta = float(overrides.get("beta", self.parameters["default_beta"]))
        return risk_free_rate + beta * equity_risk_premium

    @staticmethod
    def _select_base_cash_flow(facts: Dict[str, Any]) -> tuple[Optional[float], str]:
        operating_cf = SimpleGrowthDcfEngine._to_positive_float(facts.get("operating_cf"))
        if operating_cf:
            return operating_cf, "operating_cf"

        net_income = SimpleGrowthDcfEngine._to_positive_float(facts.get("net_income"))
        if net_income:
            return net_income, "net_income"

        return None, "missing"

    def _build_parameter_hash(self, overrides: Dict[str, Any]) -> str:
        payload = {
            "parameters": self.parameters,
            "overrides": overrides,
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]

    @staticmethod
    def _to_positive_float(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if numeric <= 0:
            return None
        return numeric

    @staticmethod
    def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
        for key, value in overrides.items():
            if (
                isinstance(value, dict)
                and isinstance(target.get(key), dict)
            ):
                SimpleGrowthDcfEngine._deep_update(target[key], value)
            else:
                target[key] = deepcopy(value)


class ResearchValuationService:
    """Build valuation history, relative valuation, and DCF outputs."""

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        self.parameters = self._merge_parameters(parameters or {})
        self.dcf_engine = SimpleGrowthDcfEngine(self.parameters.get("dcf"))

    def build_history_snapshots(
        self,
        quotes: pd.DataFrame,
        instrument: Dict[str, Any],
        financial_bundle: Dict[str, Any],
    ) -> List[ValuationHistorySnapshot]:
        if quotes is None or quotes.empty:
            return []

        financial_facts = self._prepare_financial_fact_history(financial_bundle)
        valuation_inputs = self._prepare_valuation_inputs(financial_bundle)

        ordered = quotes.copy()
        ordered["time"] = pd.to_datetime(ordered["time"])
        ordered = ordered.sort_values("time").reset_index(drop=True)

        parameter_hash = self._build_parameter_hash(self.parameters.get("history", {}))
        snapshots: List[ValuationHistorySnapshot] = []
        for _, row in ordered.iterrows():
            close_price = self._safe_positive_float(row.get("close"))
            if close_price is None:
                continue

            as_of_date = row["time"].date().isoformat()
            eligible_facts = self._eligible_financial_facts(
                financial_facts,
                as_of_date=as_of_date,
            )

            valuation_input = self._resolve_valuation_input(
                valuation_inputs,
                as_of_date=as_of_date,
                close_price=close_price,
            )
            shares_outstanding = valuation_input.get("shares_outstanding")
            market_cap = valuation_input.get("market_cap")
            float_market_cap = valuation_input.get("float_market_cap")
            input_source = valuation_input.get("source")
            if not eligible_facts:
                if market_cap is None:
                    continue
                metric_details = self._build_unavailable_metric_details(
                    market_cap=market_cap,
                    missing_reason="financial_facts_not_available",
                )
                snapshots.append(
                    ValuationHistorySnapshot(
                        instrument_id=instrument.get("instrument_id", ""),
                        symbol=instrument.get("symbol", ""),
                        exchange=instrument.get("exchange", ""),
                        as_of_date=as_of_date,
                        close_price=close_price,
                        market_cap=market_cap,
                        float_market_cap=float_market_cap,
                        parameter_hash=parameter_hash,
                        details_json={
                            "valuation_scope": "market_cap_only",
                            "missing_reason": "financial_facts_not_available",
                            "valuation_input": self._compact_valuation_input_lineage(
                                input_source
                            ),
                            "shares_outstanding": shares_outstanding,
                            "float_market_cap": float_market_cap,
                            "metrics": metric_details,
                        },
                    )
                )
                continue

            latest_fact = eligible_facts[0]
            if market_cap is None:
                shares_outstanding = self._latest_positive_fact_value(
                    eligible_facts,
                    "shares_outstanding",
                )
                if shares_outstanding is None:
                    continue
                market_cap = close_price * shares_outstanding
                input_source = {
                    "source": "financial_core_facts",
                    "source_mode": "local",
                    "input_kind": "shares_outstanding",
                    "as_of_date": latest_fact.get("data_available_date"),
                        "unit": "share",
                        "resolution": "price_times_explicit_shares_outstanding",
                    }
            elif shares_outstanding is None:
                shares_outstanding = self._latest_positive_fact_value(
                    eligible_facts,
                    "shares_outstanding",
                )
            if market_cap is None:
                continue
            metric_details = self._build_metric_details(
                market_cap=market_cap,
                eligible_facts=eligible_facts,
            )
            pe_ttm = self._metric_value(metric_details, "pe_ttm")
            pe_static = self._metric_value(metric_details, "pe_static")
            pb_mrq = self._metric_value(metric_details, "pb_mrq")
            ps_ttm = self._metric_value(metric_details, "ps_ttm")
            ps_static = self._metric_value(metric_details, "ps_static")
            pe_forward = self._metric_value(metric_details, "pe_forward")
            ps_forward = self._metric_value(metric_details, "ps_forward")

            snapshots.append(
                ValuationHistorySnapshot(
                    instrument_id=instrument.get("instrument_id", ""),
                    symbol=instrument.get("symbol", ""),
                    exchange=instrument.get("exchange", ""),
                    as_of_date=as_of_date,
                    close_price=close_price,
                    market_cap=market_cap,
                    float_market_cap=float_market_cap,
                    pe_ratio=pe_ttm if pe_ttm is not None else pe_static,
                    pb_ratio=pb_mrq,
                    ps_ratio=ps_ttm if ps_ttm is not None else ps_static,
                    pe_static=pe_static,
                    pe_ttm=pe_ttm,
                    pe_forward=pe_forward,
                    pb_mrq=pb_mrq,
                    ps_static=ps_static,
                    ps_ttm=ps_ttm,
                    ps_forward=ps_forward,
                    parameter_hash=parameter_hash,
                    details_json={
                        "report_period": latest_fact.get("report_period"),
                        "latest_financial_report_period": latest_fact.get("report_period"),
                        "latest_financial_available_date": latest_fact.get(
                            "data_available_date"
                        ),
                        "valuation_input": self._compact_valuation_input_lineage(
                            input_source
                        ),
                        "shares_outstanding": shares_outstanding,
                        "float_market_cap": float_market_cap,
                        "revenue": latest_fact.get("revenue"),
                        "net_income": latest_fact.get("net_income"),
                        "equity": latest_fact.get("equity"),
                        "metrics": metric_details,
                    },
                )
            )

        return snapshots

    def history_identity(self) -> Dict[str, str]:
        """Return the storage identity for current valuation history parameters."""
        return {
            "calc_method": ValuationHistorySnapshot.calc_method,
            "calc_version": ValuationHistorySnapshot.calc_version,
            "parameter_hash": self._build_parameter_hash(
                self.parameters.get("history", {})
            ),
        }

    def candidate_history_dates(
        self,
        quotes: pd.DataFrame,
        financial_bundle: Dict[str, Any],
    ) -> List[str]:
        """Return dates that could produce valuation rows without full metric work."""
        if quotes is None or quotes.empty:
            return []

        financial_facts = self._prepare_financial_fact_history(financial_bundle)
        valuation_inputs = self._prepare_valuation_inputs(financial_bundle)

        ordered = quotes.copy()
        ordered["time"] = pd.to_datetime(ordered["time"])
        ordered = ordered.sort_values("time").reset_index(drop=True)

        dates: List[str] = []
        for _, row in ordered.iterrows():
            close_price = self._safe_positive_float(row.get("close"))
            if close_price is None:
                continue

            as_of_date = row["time"].date().isoformat()
            eligible_facts = self._eligible_financial_facts(
                financial_facts,
                as_of_date=as_of_date,
            )

            valuation_input = self._resolve_valuation_input(
                valuation_inputs,
                as_of_date=as_of_date,
                close_price=close_price,
            )
            market_cap = valuation_input.get("market_cap")
            if not eligible_facts:
                if market_cap is not None:
                    dates.append(as_of_date)
                continue
            if market_cap is None:
                shares_outstanding = self._latest_positive_fact_value(
                    eligible_facts,
                    "shares_outstanding",
                )
                if shares_outstanding is None:
                    continue
            dates.append(as_of_date)

        return dates

    def _build_unavailable_metric_details(
        self,
        *,
        market_cap: float,
        missing_reason: str,
    ) -> Dict[str, Dict[str, Any]]:
        return {
            metric_name: self._unavailable_metric(
                metric_name,
                missing_reason,
                numerator=market_cap,
            )
            for metric_name in VALUATION_METRIC_FIELDS
        }

    @staticmethod
    def _compact_valuation_input_lineage(source: Any) -> Dict[str, Any]:
        if not isinstance(source, dict):
            return {}
        allowed = {
            "source",
            "source_mode",
            "input_kind",
            "as_of_date",
            "data_as_of",
            "unit",
            "market_cap",
            "shares_outstanding",
            "float_market_cap",
            "float_shares",
            "resolution",
            "missing_reason",
        }
        return {
            key: value
            for key, value in source.items()
            if key in allowed and value is not None
        }

    def _prepare_financial_fact_history(
        self,
        financial_bundle: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        raw_facts = (
            financial_bundle.get("financial_history")
            or financial_bundle.get("core_facts")
            or financial_bundle.get("facts_history")
            or [financial_bundle]
        )
        if not isinstance(raw_facts, list):
            raw_facts = [raw_facts]

        facts: List[Dict[str, Any]] = []
        require_availability_date = bool(
            self.parameters.get("history", {}).get("require_availability_date", True)
        )
        for raw_fact in raw_facts:
            if not isinstance(raw_fact, dict):
                continue
            report_period = str(raw_fact.get("report_period") or "").strip()
            period_key = self._report_period_key(
                report_period,
                raw_fact.get("fiscal_year"),
                raw_fact.get("fiscal_quarter"),
            )
            if period_key is None:
                continue
            available_date = self._normalize_date(
                raw_fact.get("data_available_date") or raw_fact.get("publish_date")
            )
            if require_availability_date and not available_date:
                continue
            if not available_date:
                available_date = "0001-01-01"

            item = dict(raw_fact)
            item["report_period"] = report_period
            item["fiscal_year"] = period_key[0]
            item["fiscal_quarter"] = period_key[1]
            item["period_key"] = period_key
            item["data_available_date"] = available_date
            for field_name in (
                "revenue",
                "net_income",
                "equity",
                "shares_outstanding",
            ):
                item[field_name] = self._safe_float(raw_fact.get(field_name))
            facts.append(item)

        facts.sort(key=lambda item: item["period_key"], reverse=True)
        return facts

    def _eligible_financial_facts(
        self,
        financial_facts: List[Dict[str, Any]],
        *,
        as_of_date: str,
    ) -> List[Dict[str, Any]]:
        return [
            fact
            for fact in financial_facts
            if fact.get("data_available_date")
            and str(fact["data_available_date"]) <= as_of_date
        ]

    def _prepare_valuation_inputs(
        self,
        financial_bundle: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        raw_inputs = financial_bundle.get("valuation_inputs") or []
        if not isinstance(raw_inputs, list):
            raw_inputs = [raw_inputs]

        prepared: List[Dict[str, Any]] = []
        for raw_input in raw_inputs:
            if not isinstance(raw_input, dict):
                continue
            as_of_date = self._normalize_date(
                raw_input.get("as_of_date") or raw_input.get("data_as_of")
            )
            if not as_of_date:
                continue
            market_cap = self._safe_positive_float(raw_input.get("market_cap"))
            shares_outstanding = self._safe_positive_float(
                raw_input.get("shares_outstanding")
            )
            if market_cap is None and shares_outstanding is None:
                continue
            unit = str(raw_input.get("unit") or "").strip().lower()
            diagnostics = raw_input.get("diagnostics") or raw_input.get("diagnostics_json")
            item = dict(raw_input)
            item["as_of_date"] = as_of_date
            item["market_cap"] = market_cap
            item["shares_outstanding"] = shares_outstanding
            item["unit"] = unit or None
            item["diagnostics"] = diagnostics if isinstance(diagnostics, dict) else {}
            prepared.append(item)

        prepared.sort(
            key=lambda item: (
                str(item.get("as_of_date") or ""),
                str(item.get("updated_at") or ""),
            ),
            reverse=True,
        )
        return prepared

    def _resolve_valuation_input(
        self,
        valuation_inputs: List[Dict[str, Any]],
        *,
        as_of_date: str,
        close_price: float,
    ) -> Dict[str, Any]:
        for valuation_input in valuation_inputs:
            input_date = str(valuation_input.get("as_of_date") or "")
            if input_date > as_of_date:
                continue
            data_as_of = str(valuation_input.get("data_as_of") or input_date)
            if data_as_of > as_of_date:
                continue
            market_cap = self._safe_positive_float(valuation_input.get("market_cap"))
            shares_outstanding = self._safe_positive_float(
                valuation_input.get("shares_outstanding")
            )
            float_market_cap = self._safe_positive_float(
                valuation_input.get("float_market_cap")
            )
            float_shares = self._safe_positive_float(
                valuation_input.get("float_shares")
            )
            if market_cap is None and shares_outstanding is None:
                continue
            resolved_market_cap = (
                market_cap
                if market_cap is not None
                else close_price * shares_outstanding
            )
            resolved_float_market_cap = (
                float_market_cap
                if float_market_cap is not None
                else (
                    close_price * float_shares if float_shares is not None else None
                )
            )
            source = {
                "source": valuation_input.get("source"),
                "source_mode": valuation_input.get("source_mode"),
                "input_kind": valuation_input.get("input_kind"),
                "as_of_date": input_date,
                "data_as_of": data_as_of,
                "unit": valuation_input.get("unit"),
                "market_cap": market_cap,
                "shares_outstanding": shares_outstanding,
                "float_market_cap": float_market_cap,
                "float_shares": float_shares,
                "resolution": (
                    "explicit_market_cap"
                    if market_cap is not None
                    else "price_times_explicit_shares_outstanding"
                ),
                "diagnostics": valuation_input.get("diagnostics") or {},
            }
            return {
                "market_cap": resolved_market_cap,
                "float_market_cap": resolved_float_market_cap,
                "shares_outstanding": shares_outstanding,
                "source": source,
            }
        return {
            "market_cap": None,
            "float_market_cap": None,
            "shares_outstanding": None,
            "source": {
                "missing_reason": "valuation_input_not_available",
            },
        }

    def _build_metric_details(
        self,
        *,
        market_cap: float,
        eligible_facts: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        by_period = {fact["period_key"]: fact for fact in eligible_facts}
        latest_fact = eligible_facts[0]
        metrics = {
            "pe_static": self._build_static_metric(
                metric_name="pe_static",
                market_cap=market_cap,
                eligible_facts=eligible_facts,
                fact_field="net_income",
                allow_negative_denominator=True,
            ),
            "pe_ttm": self._build_ttm_metric(
                metric_name="pe_ttm",
                market_cap=market_cap,
                latest_fact=latest_fact,
                facts_by_period=by_period,
                fact_field="net_income",
                allow_negative_denominator=True,
            ),
            "pb_mrq": self._build_latest_metric(
                metric_name="pb_mrq",
                market_cap=market_cap,
                eligible_facts=eligible_facts,
                fact_field="equity",
            ),
            "ps_static": self._build_static_metric(
                metric_name="ps_static",
                market_cap=market_cap,
                eligible_facts=eligible_facts,
                fact_field="revenue",
            ),
            "ps_ttm": self._build_ttm_metric(
                metric_name="ps_ttm",
                market_cap=market_cap,
                latest_fact=latest_fact,
                facts_by_period=by_period,
                fact_field="revenue",
            ),
        }
        metrics["pe_forward"] = self._build_forward_metric("pe_forward")
        metrics["ps_forward"] = self._build_forward_metric("ps_forward")
        return metrics

    def _build_static_metric(
        self,
        *,
        metric_name: str,
        market_cap: float,
        eligible_facts: List[Dict[str, Any]],
        fact_field: str,
        allow_negative_denominator: bool = False,
    ) -> Dict[str, Any]:
        annual_fact = next(
            (
                fact
                for fact in eligible_facts
                if int(fact.get("fiscal_quarter") or 0) == 4
            ),
            None,
        )
        if annual_fact is None:
            return self._unavailable_metric(
                metric_name,
                "annual_report_period_not_available",
                numerator=market_cap,
            )
        return self._ratio_metric(
            metric_name=metric_name,
            numerator=market_cap,
            denominator=annual_fact.get(fact_field),
            denominator_fact=fact_field,
            report_periods=[annual_fact],
            calc_method="latest_annual",
            allow_negative_denominator=allow_negative_denominator,
        )

    def _build_ttm_metric(
        self,
        *,
        metric_name: str,
        market_cap: float,
        latest_fact: Dict[str, Any],
        facts_by_period: Dict[Tuple[int, int], Dict[str, Any]],
        fact_field: str,
        allow_negative_denominator: bool = False,
    ) -> Dict[str, Any]:
        year, quarter = latest_fact["period_key"]
        latest_value = latest_fact.get(fact_field)
        if latest_value is None:
            return self._unavailable_metric(
                metric_name,
                "latest_period_fact_missing",
                numerator=market_cap,
                report_periods=[latest_fact],
            )

        if quarter == 4:
            return self._ratio_metric(
                metric_name=metric_name,
                numerator=market_cap,
                denominator=latest_value,
                denominator_fact=fact_field,
                report_periods=[latest_fact],
                calc_method="annual_ttm",
                allow_negative_denominator=allow_negative_denominator,
            )

        prior_annual = facts_by_period.get((year - 1, 4))
        prior_same_quarter = facts_by_period.get((year - 1, quarter))
        if prior_annual is None or prior_same_quarter is None:
            return self._unavailable_metric(
                metric_name,
                "missing_ttm_comparison_period",
                numerator=market_cap,
                report_periods=[
                    item
                    for item in (latest_fact, prior_annual, prior_same_quarter)
                    if item is not None
                ],
            )

        prior_annual_value = prior_annual.get(fact_field)
        prior_same_value = prior_same_quarter.get(fact_field)
        if prior_annual_value is None or prior_same_value is None:
            return self._unavailable_metric(
                metric_name,
                "missing_ttm_denominator_fact",
                numerator=market_cap,
                report_periods=[latest_fact, prior_annual, prior_same_quarter],
            )

        denominator = latest_value + prior_annual_value - prior_same_value
        return self._ratio_metric(
            metric_name=metric_name,
            numerator=market_cap,
            denominator=denominator,
            denominator_fact=fact_field,
            report_periods=[latest_fact, prior_annual, prior_same_quarter],
            calc_method="cumulative_ytd_ttm",
            allow_negative_denominator=allow_negative_denominator,
        )

    def _build_latest_metric(
        self,
        *,
        metric_name: str,
        market_cap: float,
        eligible_facts: List[Dict[str, Any]],
        fact_field: str,
    ) -> Dict[str, Any]:
        latest_fact = next(
            (fact for fact in eligible_facts if fact.get(fact_field) is not None),
            None,
        )
        if latest_fact is None:
            return self._unavailable_metric(
                metric_name,
                "latest_period_fact_missing",
                numerator=market_cap,
            )
        return self._ratio_metric(
            metric_name=metric_name,
            numerator=market_cap,
            denominator=latest_fact.get(fact_field),
            denominator_fact=fact_field,
            report_periods=[latest_fact],
            calc_method="latest_mrq",
        )

    def _build_forward_metric(self, metric_name: str) -> Dict[str, Any]:
        history_config = self.parameters.get("history", {})
        forward_enabled = bool(
            history_config.get("forward_metrics_enabled", False)
            or self.parameters.get("forward", {}).get("enabled", False)
        )
        reason = (
            "analyst_forecast_missing"
            if forward_enabled
            else "analyst_forecast_disabled"
        )
        return self._unavailable_metric(metric_name, reason)

    def _ratio_metric(
        self,
        *,
        metric_name: str,
        numerator: float,
        denominator: Optional[float],
        denominator_fact: str,
        report_periods: List[Dict[str, Any]],
        calc_method: str,
        allow_negative_denominator: bool = False,
    ) -> Dict[str, Any]:
        if denominator is None:
            return self._unavailable_metric(
                metric_name,
                "denominator_fact_missing",
                numerator=numerator,
                denominator_fact=denominator_fact,
                report_periods=report_periods,
                calc_method=calc_method,
            )
        if denominator == 0 or (denominator < 0 and not allow_negative_denominator):
            return self._unavailable_metric(
                metric_name,
                "invalid_denominator",
                numerator=numerator,
                denominator=denominator,
                denominator_fact=denominator_fact,
                report_periods=report_periods,
                calc_method=calc_method,
            )
        return {
            "metric": metric_name,
            "status": "available",
            "value": numerator / denominator,
            "numerator": numerator,
            "denominator": denominator,
            "denominator_fact": denominator_fact,
            "report_periods": [
                str(item.get("report_period"))
                for item in report_periods
                if item.get("report_period")
            ],
            "availability_dates": [
                item.get("data_available_date")
                for item in report_periods
                if item.get("data_available_date")
            ],
            "calc_method": calc_method,
        }

    def _unavailable_metric(
        self,
        metric_name: str,
        missing_reason: str,
        *,
        numerator: Optional[float] = None,
        denominator: Optional[float] = None,
        denominator_fact: Optional[str] = None,
        report_periods: Optional[List[Dict[str, Any]]] = None,
        calc_method: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "metric": metric_name,
            "status": "unavailable",
            "value": None,
            "numerator": numerator,
            "denominator": denominator,
            "denominator_fact": denominator_fact,
            "report_periods": [
                str(item.get("report_period"))
                for item in report_periods or []
                if item.get("report_period")
            ],
            "availability_dates": [
                item.get("data_available_date")
                for item in report_periods or []
                if item.get("data_available_date")
            ],
            "calc_method": calc_method,
            "missing_reason": missing_reason,
        }

    @staticmethod
    def _metric_value(
        metrics: Dict[str, Dict[str, Any]],
        metric_name: str,
    ) -> Optional[float]:
        metric = metrics.get(metric_name) or {}
        value = metric.get("value")
        try:
            return None if value is None else float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _latest_positive_fact_value(
        eligible_facts: List[Dict[str, Any]],
        fact_field: str,
    ) -> Optional[float]:
        for fact in eligible_facts:
            value = ResearchValuationService._safe_positive_float(fact.get(fact_field))
            if value is not None:
                return value
        return None

    @staticmethod
    def _report_period_key(
        report_period: str,
        fiscal_year: Any = None,
        fiscal_quarter: Any = None,
    ) -> Optional[Tuple[int, int]]:
        try:
            if fiscal_year is not None and fiscal_quarter is not None:
                return int(fiscal_year), int(fiscal_quarter)
        except (TypeError, ValueError):
            pass

        normalized = str(report_period or "").strip()
        if len(normalized) == 6 and normalized[4].upper() == "Q":
            try:
                return int(normalized[:4]), int(normalized[5])
            except ValueError:
                return None

        timestamp = pd.to_datetime(normalized, errors="coerce")
        if pd.isna(timestamp):
            return None
        return int(timestamp.year), int((timestamp.month - 1) // 3 + 1)

    @staticmethod
    def _normalize_date(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        timestamp = pd.to_datetime(value, errors="coerce")
        if pd.isna(timestamp):
            return None
        return timestamp.date().isoformat()

    def build_history_response(
        self,
        rows: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None

        items = sorted(rows, key=lambda item: item["as_of_date"])
        latest = items[-1]
        return {
            "instrument_id": latest.get("instrument_id"),
            "symbol": latest.get("symbol"),
            "exchange": latest.get("exchange"),
            "calc_method": latest.get("calc_method"),
            "calc_version": latest.get("calc_version"),
            "parameter_hash": latest.get("parameter_hash"),
            "data_points": len(items),
            "window_start": items[0].get("as_of_date"),
            "window_end": latest.get("as_of_date"),
            "items": items,
        }

    def build_relative_valuation(
        self,
        *,
        instrument: Dict[str, Any],
        subject_row: Optional[Dict[str, Any]],
        industry_membership: Optional[Dict[str, Any]],
        peer_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        relative_config = self.parameters["relative"]
        require_authoritative = bool(relative_config.get("require_authoritative", True))
        min_peer_count = int(relative_config.get("min_peer_count", 3))
        max_peer_rows = int(relative_config.get("max_peer_rows", 20))
        metric_fields = self._relative_metric_fields(relative_config)

        if subject_row is None:
            return self._relative_unavailable(
                instrument,
                missing_reason="valuation_history_not_available",
            )

        if industry_membership is None:
            return self._relative_unavailable(
                instrument,
                missing_reason="industry_membership_not_available",
                subject_valuation=self._compact_valuation_row(subject_row),
            )

        benchmark_context = self.resolve_relative_benchmark_context(industry_membership)
        if not benchmark_context["supported"]:
            return self._relative_unavailable(
                instrument,
                missing_reason=benchmark_context["missing_reason"],
                subject_valuation=self._compact_valuation_row(subject_row),
                industry_membership=industry_membership,
            )

        if (
            require_authoritative
            and industry_membership.get("mapping_status") != "authoritative"
        ):
            return self._relative_unavailable(
                instrument,
                missing_reason="authoritative_sw_l2_membership_required",
                subject_valuation=self._compact_valuation_row(subject_row),
                industry_membership=industry_membership,
            )

        benchmark_code = benchmark_context["benchmark_code"]
        if not benchmark_code:
            return self._relative_unavailable(
                instrument,
                missing_reason=benchmark_context["missing_reason"],
                subject_valuation=self._compact_valuation_row(subject_row),
                industry_membership=industry_membership,
            )

        peer_rows = peer_rows[:max_peer_rows]
        peer_count = len(peer_rows)
        status = "success" if peer_count >= min_peer_count else "insufficient_peers"
        missing_reason = None if status == "success" else "minimum_peer_count_not_met"

        metrics = {}
        metric_exclusions: Dict[str, Any] = {}
        for field_name in metric_fields:
            benchmark, exclusions = self._build_metric_benchmark(
                subject_row=subject_row,
                peer_rows=peer_rows,
                field_name=field_name,
            )
            metrics[field_name] = benchmark
            metric_exclusions[field_name] = exclusions

        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "status": status,
            "missing_reason": missing_reason,
            "calc_method": "relative_valuation_builtin",
            "calc_version": "relative_valuation.v1",
            "parameter_hash": self._build_parameter_hash(relative_config),
            "benchmark_taxonomy_system": industry_membership.get("taxonomy_system"),
            "benchmark_taxonomy_version": industry_membership.get("taxonomy_version"),
            "benchmark_level": benchmark_context["benchmark_level"],
            "benchmark_field": benchmark_context["benchmark_field"],
            "benchmark_code": benchmark_code,
            "benchmark_name": benchmark_context["benchmark_name"],
            "benchmark_sw_l2_code": (
                benchmark_code
                if benchmark_context["benchmark_field"] == "sw_l2_code"
                else None
            ),
            "benchmark_sw_l2_name": (
                benchmark_context["benchmark_name"]
                if benchmark_context["benchmark_field"] == "sw_l2_code"
                else None
            ),
            "peer_count": peer_count,
            "subject_valuation": self._compact_valuation_row(subject_row),
            "benchmark_summary": metrics,
            "metric_variants": metric_fields,
            "diagnostics": {
                "metric_exclusions": metric_exclusions,
            },
            "peers": [self._compact_valuation_row(item) for item in peer_rows],
            "data_as_of": subject_row.get("data_as_of"),
        }

    def run_dcf(
        self,
        *,
        instrument: Dict[str, Any],
        financial_bundle: Dict[str, Any],
        latest_close: Optional[float],
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.dcf_engine.run(
            instrument=instrument,
            financial_bundle=financial_bundle,
            latest_close=latest_close,
            overrides=overrides,
        )

    def _relative_unavailable(
        self,
        instrument: Dict[str, Any],
        *,
        missing_reason: str,
        subject_valuation: Optional[Dict[str, Any]] = None,
        industry_membership: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        benchmark_context = (
            self.resolve_relative_benchmark_context(industry_membership)
            if industry_membership is not None
            else {}
        )
        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "status": "benchmark_unavailable",
            "missing_reason": missing_reason,
            "calc_method": "relative_valuation_builtin",
            "calc_version": "relative_valuation.v1",
            "parameter_hash": self._build_parameter_hash(self.parameters["relative"]),
            "benchmark_taxonomy_system": (
                None
                if industry_membership is None
                else industry_membership.get("taxonomy_system")
            ),
            "benchmark_taxonomy_version": (
                None
                if industry_membership is None
                else industry_membership.get("taxonomy_version")
            ),
            "benchmark_level": benchmark_context.get("benchmark_level"),
            "benchmark_field": benchmark_context.get("benchmark_field"),
            "benchmark_code": benchmark_context.get("benchmark_code"),
            "benchmark_name": benchmark_context.get("benchmark_name"),
            "benchmark_sw_l2_code": (
                benchmark_context.get("benchmark_code")
                if benchmark_context.get("benchmark_field") == "sw_l2_code"
                else None
            ),
            "benchmark_sw_l2_name": (
                benchmark_context.get("benchmark_name")
                if benchmark_context.get("benchmark_field") == "sw_l2_code"
                else None
            ),
            "peer_count": 0,
            "subject_valuation": subject_valuation,
            "benchmark_summary": {},
            "metric_variants": self._relative_metric_fields(self.parameters["relative"]),
            "diagnostics": {"metric_exclusions": {}},
            "peers": [],
            "data_as_of": (
                None
                if subject_valuation is None
                else subject_valuation.get("data_as_of")
            ),
        }

    def resolve_relative_benchmark_context(
        self,
        industry_membership: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Resolve the configured industry benchmark field for relative valuation."""
        relative_config = self.parameters["relative"]
        benchmark_field = self._configured_benchmark_field(relative_config)
        definition = BENCHMARK_FIELD_DEFINITIONS.get(benchmark_field)
        if definition is None:
            return {
                "supported": False,
                "missing_reason": "unsupported_benchmark_field",
                "benchmark_level": self._configured_benchmark_level(relative_config),
                "benchmark_field": benchmark_field,
                "benchmark_code": None,
                "benchmark_name": None,
            }

        benchmark_code = industry_membership.get(benchmark_field)
        benchmark_name = industry_membership.get(definition["name_field"])
        return {
            "supported": True,
            "missing_reason": None if benchmark_code else definition["missing_reason"],
            "benchmark_level": definition["level"],
            "benchmark_field": benchmark_field,
            "benchmark_code": benchmark_code,
            "benchmark_name": benchmark_name,
        }

    @staticmethod
    def _configured_benchmark_field(relative_config: Dict[str, Any]) -> str:
        benchmark_field = relative_config.get("benchmark_field")
        if benchmark_field:
            return str(benchmark_field)

        benchmark_level = ResearchValuationService._configured_benchmark_level(
            relative_config
        )
        for field_name, definition in BENCHMARK_FIELD_DEFINITIONS.items():
            if definition["level"] == benchmark_level:
                return field_name
        return "sw_l2_code"

    @staticmethod
    def _configured_benchmark_level(relative_config: Dict[str, Any]) -> int:
        try:
            return int(relative_config.get("benchmark_level", 2))
        except (TypeError, ValueError):
            return 2

    def _build_metric_benchmark(
        self,
        *,
        subject_row: Dict[str, Any],
        peer_rows: List[Dict[str, Any]],
        field_name: str,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        values: List[float] = []
        exclusions: List[Dict[str, Any]] = []
        for row in peer_rows:
            value, missing_reason = self._metric_value_from_row(row, field_name)
            if value is None:
                exclusions.append(
                    {
                        "instrument_id": row.get("instrument_id"),
                        "symbol": row.get("symbol"),
                        "reason": missing_reason,
                    }
                )
                continue
            values.append(value)

        subject_numeric, _ = self._metric_value_from_row(subject_row, field_name)
        if not values:
            return (
                {
                    "subject_value": subject_numeric,
                    "peer_mean": None,
                    "peer_median": None,
                    "peer_min": None,
                    "peer_max": None,
                    "peer_p25": None,
                    "peer_p75": None,
                    "valid_peer_count": 0,
                    "excluded_peer_count": len(exclusions),
                    "percentile_rank": None,
                    "premium_to_median": None,
                },
                exclusions,
            )

        benchmark_median = median(values)
        premium_to_median = None
        if subject_numeric is not None and benchmark_median not in (None, 0):
            premium_to_median = subject_numeric / benchmark_median - 1

        return (
            {
                "subject_value": subject_numeric,
                "peer_mean": fmean(values),
                "peer_median": benchmark_median,
                "peer_min": min(values),
                "peer_max": max(values),
                "peer_p25": self._quantile(values, 0.25),
                "peer_p75": self._quantile(values, 0.75),
                "valid_peer_count": len(values),
                "excluded_peer_count": len(exclusions),
                "percentile_rank": self._percentile_rank(subject_numeric, values),
                "premium_to_median": premium_to_median,
            },
            exclusions,
        )

    @staticmethod
    def _relative_metric_fields(relative_config: Dict[str, Any]) -> List[str]:
        configured = relative_config.get("metric_variants") or [
            "pe_ttm",
            "pb_mrq",
            "ps_ttm",
        ]
        fields = [str(item) for item in configured if str(item).strip()]
        if bool(relative_config.get("include_compatibility_metrics", True)):
            fields.extend(["pe_ratio", "pb_ratio", "ps_ratio"])
        unique_fields: List[str] = []
        for field_name in fields:
            if field_name not in unique_fields:
                unique_fields.append(field_name)
        return unique_fields

    @staticmethod
    def _metric_value_from_row(
        row: Dict[str, Any],
        field_name: str,
    ) -> Tuple[Optional[float], str]:
        value = row.get(field_name)
        fallback_fields = {
            "pe_ttm": "pe_ratio",
            "pb_mrq": "pb_ratio",
            "ps_ttm": "ps_ratio",
        }
        if value is None and field_name in fallback_fields:
            value = row.get(fallback_fields[field_name])
        if value is None:
            return None, "missing_value"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None, "non_numeric_value"
        if numeric <= 0:
            pe_fields = {"pe_ratio", "pe_static", "pe_ttm", "pe_forward"}
            if field_name in pe_fields and numeric != 0:
                return numeric, ""
            return None, "invalid_value"
        return numeric, ""

    @staticmethod
    def _quantile(values: List[float], q: float) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        position = (len(ordered) - 1) * q
        lower_index = int(position)
        upper_index = min(lower_index + 1, len(ordered) - 1)
        fraction = position - lower_index
        return ordered[lower_index] * (1 - fraction) + ordered[upper_index] * fraction

    @staticmethod
    def _percentile_rank(
        subject_value: Optional[float],
        peer_values: List[float],
    ) -> Optional[float]:
        if subject_value is None or not peer_values:
            return None
        return sum(1 for value in peer_values if value <= subject_value) / len(peer_values)

    @staticmethod
    def _compact_valuation_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "instrument_id": row.get("instrument_id"),
            "symbol": row.get("symbol"),
            "exchange": row.get("exchange"),
            "as_of_date": row.get("as_of_date"),
            "close_price": row.get("close_price"),
            "market_cap": row.get("market_cap"),
            "pe_ratio": row.get("pe_ratio"),
            "pb_ratio": row.get("pb_ratio"),
            "ps_ratio": row.get("ps_ratio"),
            "pe_static": row.get("pe_static"),
            "pe_ttm": row.get("pe_ttm"),
            "pe_forward": row.get("pe_forward"),
            "pb_mrq": row.get("pb_mrq"),
            "ps_static": row.get("ps_static"),
            "ps_ttm": row.get("ps_ttm"),
            "ps_forward": row.get("ps_forward"),
            "data_as_of": row.get("data_as_of"),
        }

    def _merge_parameters(self, overrides: Dict[str, Any]) -> Dict[str, Any]:
        merged = deepcopy(DEFAULT_VALUATION_PARAMETERS)
        self._deep_update(merged, overrides)
        return merged

    def _build_parameter_hash(self, payload: Dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]

    @staticmethod
    def _safe_ratio(
        numerator: Optional[float],
        denominator: Optional[float],
    ) -> Optional[float]:
        if numerator is None or denominator in (None, 0):
            return None
        if denominator <= 0:
            return None
        return numerator / denominator

    @staticmethod
    def _safe_positive_float(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if numeric <= 0:
            return None
        return numeric

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
        for key, value in overrides.items():
            if (
                isinstance(value, dict)
                and isinstance(target.get(key), dict)
            ):
                ResearchValuationService._deep_update(target[key], value)
            else:
                target[key] = deepcopy(value)
