"""
Valuation history, relative valuation, and DCF helpers for research APIs.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from copy import deepcopy
from statistics import fmean, median
from typing import Any, Dict, List, Optional

import pandas as pd

from .providers.base import ValuationHistorySnapshot


DEFAULT_VALUATION_PARAMETERS: Dict[str, Any] = {
    "history": {
        "lookback_days": 252,
    },
    "relative": {
        "taxonomy_system": "sw",
        "benchmark_level": 2,
        "benchmark_field": "sw_l2_code",
        "require_authoritative": True,
        "min_peer_count": 3,
        "max_peer_rows": 20,
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

        shares_outstanding = self._safe_positive_float(financial_bundle.get("shares_outstanding"))
        if shares_outstanding is None:
            return []

        ordered = quotes.copy()
        ordered["time"] = pd.to_datetime(ordered["time"])
        ordered = ordered.sort_values("time").reset_index(drop=True)

        report_period = financial_bundle.get("report_period")
        revenue = self._safe_positive_float(financial_bundle.get("revenue"))
        net_income = self._safe_positive_float(financial_bundle.get("net_income"))
        equity = self._safe_positive_float(financial_bundle.get("equity"))

        parameter_hash = self._build_parameter_hash(self.parameters.get("history", {}))
        snapshots: List[ValuationHistorySnapshot] = []
        for _, row in ordered.iterrows():
            close_price = self._safe_positive_float(row.get("close"))
            if close_price is None:
                continue

            market_cap = close_price * shares_outstanding
            snapshots.append(
                ValuationHistorySnapshot(
                    instrument_id=instrument.get("instrument_id", ""),
                    symbol=instrument.get("symbol", ""),
                    exchange=instrument.get("exchange", ""),
                    as_of_date=row["time"].date().isoformat(),
                    close_price=close_price,
                    market_cap=market_cap,
                    pe_ratio=self._safe_ratio(market_cap, net_income),
                    pb_ratio=self._safe_ratio(market_cap, equity),
                    ps_ratio=self._safe_ratio(market_cap, revenue),
                    parameter_hash=parameter_hash,
                    details_json={
                        "report_period": report_period,
                        "shares_outstanding": shares_outstanding,
                        "revenue": revenue,
                        "net_income": net_income,
                        "equity": equity,
                    },
                )
            )

        return snapshots

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
        for field_name in ("pe_ratio", "pb_ratio", "ps_ratio"):
            benchmark = self._build_metric_benchmark(
                subject_value=subject_row.get(field_name),
                peer_rows=peer_rows,
                field_name=field_name,
            )
            metrics[field_name] = benchmark

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
        subject_value: Any,
        peer_rows: List[Dict[str, Any]],
        field_name: str,
    ) -> Optional[Dict[str, Any]]:
        values = [
            float(item[field_name])
            for item in peer_rows
            if item.get(field_name) is not None and float(item[field_name]) > 0
        ]
        if not values:
            return None

        subject_numeric = self._safe_positive_float(subject_value)
        benchmark_median = median(values)
        premium_to_median = None
        if subject_numeric is not None and benchmark_median not in (None, 0):
            premium_to_median = subject_numeric / benchmark_median - 1

        return {
            "subject_value": subject_numeric,
            "peer_mean": fmean(values),
            "peer_median": benchmark_median,
            "peer_min": min(values),
            "peer_max": max(values),
            "premium_to_median": premium_to_median,
        }

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
    def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
        for key, value in overrides.items():
            if (
                isinstance(value, dict)
                and isinstance(target.get(key), dict)
            ):
                ResearchValuationService._deep_update(target[key], value)
            else:
                target[key] = deepcopy(value)
