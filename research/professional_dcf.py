"""Professional DCF model helpers.

This module keeps DCF model selection, input readiness, assumptions, and the
first non-financial FCFF implementation out of the API layer.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_PROFESSIONAL_DCF_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "default_model_profile": "nonfinancial_fcff.v1",
    "default_model_strategy": "auto",
    "score_gap_threshold": 0.15,
    "terminal_value_warning_threshold": 0.8,
    "model_profiles": {
        "nonfinancial_fcff.v1": {
            "candidate_type": "industry",
            "implementation_status": "implemented",
            "required_fields": ["revenue", "operating_profit", "capital_expenditure"],
            "optional_fields": [
                "depreciation_and_amortization",
                "change_in_working_capital",
                "cash_and_equivalents",
                "total_debt",
                "lease_liabilities",
                "minority_interest",
                "preferred_equity",
                "non_operating_assets",
            ],
            "supported_company_types": ["nonfinancial"],
        },
        "high_growth_staged_fcff.v1": {
            "candidate_type": "company_characteristic",
            "implementation_status": "guardrail",
            "required_fields": ["revenue", "research_and_development"],
            "optional_fields": ["operating_profit", "capital_expenditure"],
            "supported_company_types": ["high_growth", "high_r_and_d"],
        },
        "bank_residual_income.v1": {
            "candidate_type": "financial_sector",
            "implementation_status": "guardrail",
            "required_fields": ["equity", "net_income", "roe", "capital_adequacy_ratio"],
            "optional_fields": ["npl_ratio", "provision_coverage_ratio"],
            "supported_company_types": ["bank"],
        },
        "broker_excess_capital.v1": {
            "candidate_type": "financial_sector",
            "implementation_status": "guardrail",
            "required_fields": ["net_income", "equity", "net_capital"],
            "optional_fields": ["brokerage_revenue", "investment_income"],
            "supported_company_types": ["broker"],
        },
        "insurance_embedded_value_or_ddm.v1": {
            "candidate_type": "financial_sector",
            "implementation_status": "guardrail",
            "required_fields": ["embedded_value", "new_business_value"],
            "optional_fields": ["premium_income", "solvency_ratio"],
            "supported_company_types": ["insurance"],
        },
        "real_estate_nav_dcf.v1": {
            "candidate_type": "industry",
            "implementation_status": "guardrail",
            "required_fields": ["inventory", "contract_liabilities", "total_debt"],
            "optional_fields": ["land_bank", "project_sales"],
            "supported_company_types": ["real_estate"],
        },
        "cyclical_fcff_midcycle.v1": {
            "candidate_type": "industry",
            "implementation_status": "guardrail",
            "required_fields": ["revenue", "operating_profit", "commodity_price_assumption"],
            "optional_fields": ["production_volume", "unit_cost"],
            "supported_company_types": ["cyclical"],
        },
        "utility_fcfe_or_ddm.v1": {
            "candidate_type": "industry",
            "implementation_status": "guardrail",
            "required_fields": ["operating_cf", "dividend_payout_ratio"],
            "optional_fields": ["regulated_return", "total_debt"],
            "supported_company_types": ["utility"],
        },
        "holdco_sotp.v1": {
            "candidate_type": "company_characteristic",
            "implementation_status": "guardrail",
            "required_fields": ["segment_assets", "investment_income"],
            "optional_fields": ["holding_company_discount"],
            "supported_company_types": ["holding_company"],
        },
    },
    "assumptions": {
        "risk_free_rate_rmb_10y": {
            "assumption_key": "risk_free_rate_rmb_10y",
            "market": "CN",
            "currency": "CNY",
            "tenor": "10Y",
            "value": 0.02,
            "unit": "rate",
            "primary_source": "china_bond_10y",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
        },
        "risk_free_rate_usd_10y": {
            "assumption_key": "risk_free_rate_usd_10y",
            "market": "US",
            "currency": "USD",
            "tenor": "10Y",
            "value": 0.04,
            "unit": "rate",
            "primary_source": "us_treasury_10y",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
        },
        "risk_free_rate_hkd_10y": {
            "assumption_key": "risk_free_rate_hkd_10y",
            "market": "HK",
            "currency": "HKD",
            "tenor": "10Y",
            "value": 0.035,
            "unit": "rate",
            "primary_source": "hk_gov_bond_or_efn_10y",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
        },
        "equity_risk_premium": {
            "assumption_key": "equity_risk_premium",
            "market": "GLOBAL",
            "currency": None,
            "tenor": None,
            "value": 0.06,
            "unit": "rate",
            "primary_source": "versioned_research_config",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
        },
        "cost_of_debt_default": {
            "assumption_key": "cost_of_debt_default",
            "market": "GLOBAL",
            "currency": None,
            "tenor": None,
            "value": 0.045,
            "unit": "rate",
            "primary_source": "company_bond_or_interest_expense",
            "fallback_sources": ["industry_credit_spread", "manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
        },
    },
    "workbook": {
        "enabled": True,
        "default_style": "consulting_clean",
        "artifact_ttl_hours": 24,
    },
    "bounded_cache": {
        "enabled": False,
        "ttl_hours": 24,
    },
}


@dataclass(frozen=True)
class DcfInputBundle:
    """Normalized DCF inputs for one valuation run."""

    instrument: Dict[str, Any]
    financial_facts: Dict[str, Any]
    assumptions: Dict[str, Dict[str, Any]]
    valuation_date: str
    latest_close: Optional[float]
    beta_context: Dict[str, Any]
    data_available_cutoff: Optional[str]
    input_hash: str
    blockers: Tuple[str, ...]
    warnings: Tuple[str, ...]
    lineage: Dict[str, Any]


class ProfessionalDcfEngine:
    """Professional DCF engine with first-stage non-financial FCFF support."""

    calc_method = "professional_dcf"
    calc_version = "professional_dcf.v1"

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        merged = deepcopy(DEFAULT_PROFESSIONAL_DCF_CONFIG)
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
        overrides = overrides or {}
        valuation_date = str(
            overrides.get("valuation_date")
            or self._today_iso()
        )
        research_mode = bool(overrides.get("research_mode", False))
        bundle = self.build_input_bundle(
            instrument=instrument,
            financial_bundle=financial_bundle,
            latest_close=latest_close,
            valuation_date=valuation_date,
            overrides=overrides,
            research_mode=research_mode,
        )

        model_strategy = str(
            overrides.get("model_strategy")
            or self.parameters.get("default_model_strategy")
            or "auto"
        )
        include_comparison = bool(
            overrides.get("include_model_comparison")
            or model_strategy == "compare"
        )
        selector = self.select_model(
            instrument=instrument,
            input_bundle=bundle,
            overrides=overrides,
            model_strategy=model_strategy,
            include_comparison=include_comparison,
        )
        model_profile = selector["recommended_model"]

        if bundle.blockers and not research_mode:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="unavailable",
                missing_reason=bundle.blockers[0],
                research_mode=research_mode,
            )

        if model_profile != "nonfinancial_fcff.v1" and selector.get("include_model_comparison"):
            calculable_candidate = next(
                (
                    candidate
                    for candidate in selector.get("candidates", [])
                    if candidate.get("model_profile") == "nonfinancial_fcff.v1"
                ),
                None,
            )
            if calculable_candidate is not None:
                return self._run_nonfinancial_fcff(
                    instrument=instrument,
                    bundle=bundle,
                    overrides=overrides,
                    selector=selector,
                    research_mode=research_mode,
                )

        if model_profile != "nonfinancial_fcff.v1":
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="partial" if research_mode else "unavailable",
                missing_reason="model_profile_not_implemented",
                research_mode=research_mode,
            )

        return self._run_nonfinancial_fcff(
            instrument=instrument,
            bundle=bundle,
            overrides=overrides,
            selector=selector,
            research_mode=research_mode,
        )

    def build_input_bundle(
        self,
        *,
        instrument: Dict[str, Any],
        financial_bundle: Dict[str, Any],
        latest_close: Optional[float],
        valuation_date: str,
        overrides: Dict[str, Any],
        research_mode: bool = False,
    ) -> DcfInputBundle:
        facts = self._extract_financial_facts(financial_bundle)
        data_available_date = self._normalize_date(
            facts.get("data_available_date") or facts.get("publish_date")
        )
        blockers: List[str] = []
        warnings: List[str] = []
        if data_available_date and data_available_date > valuation_date:
            blockers.append("financial_fact_after_valuation_date")
        if not data_available_date:
            if research_mode:
                warnings.append("missing_data_available_date")
            else:
                blockers.append("missing_data_available_date")

        assumption_map = self.get_assumptions(
            market=str(instrument.get("exchange") or ""),
            currency=str(facts.get("currency") or instrument.get("currency") or "CNY"),
            overrides=overrides,
        )
        beta_context = self._resolve_beta_context(overrides)
        input_payload = {
            "instrument_id": instrument.get("instrument_id"),
            "facts": facts,
            "assumptions": assumption_map,
            "valuation_date": valuation_date,
            "latest_close": latest_close,
            "beta": beta_context,
        }
        input_hash = self._hash_payload(input_payload)
        lineage = {
            "financial": {
                "report_period": facts.get("report_period"),
                "data_available_date": data_available_date,
                "source": facts.get("source"),
                "source_mode": facts.get("source_mode"),
                "currency": facts.get("currency"),
            },
            "assumptions": {
                key: {
                    "source": value.get("source"),
                    "primary_source": value.get("primary_source"),
                    "as_of_date": value.get("as_of_date"),
                    "quality_flag": value.get("quality_flag"),
                }
                for key, value in assumption_map.items()
            },
            "beta": beta_context,
        }
        return DcfInputBundle(
            instrument=instrument,
            financial_facts=facts,
            assumptions=assumption_map,
            valuation_date=valuation_date,
            latest_close=latest_close,
            beta_context=beta_context,
            data_available_cutoff=data_available_date,
            input_hash=input_hash,
            blockers=tuple(blockers),
            warnings=tuple(warnings),
            lineage=lineage,
        )

    def get_assumptions(
        self,
        *,
        market: str = "",
        currency: str = "CNY",
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        overrides = overrides or {}
        assumptions = deepcopy(self.parameters.get("assumptions", {}))
        currency_upper = (currency or "").upper()
        market_upper = (market or "").upper()
        if currency_upper in {"USD", "US"} or market_upper in {"NYSE", "NASDAQ", "US"}:
            risk_key = "risk_free_rate_usd_10y"
        elif currency_upper == "HKD" or market_upper == "HKEX":
            risk_key = "risk_free_rate_hkd_10y"
        else:
            risk_key = "risk_free_rate_rmb_10y"

        if overrides.get("risk_free_rate") is not None:
            risk_free = deepcopy(assumptions.get(risk_key, {}))
            risk_free.update(
                {
                    "value": float(overrides["risk_free_rate"]),
                    "source": "manual_override",
                    "quality_flag": "manual_override",
                    "fallback_used": False,
                }
            )
            assumptions[risk_key] = risk_free
        if overrides.get("equity_risk_premium") is not None:
            erp = deepcopy(assumptions.get("equity_risk_premium", {}))
            erp.update(
                {
                    "value": float(overrides["equity_risk_premium"]),
                    "source": "manual_override",
                    "quality_flag": "manual_override",
                    "fallback_used": False,
                }
            )
            assumptions["equity_risk_premium"] = erp

        return {
            "risk_free_rate": assumptions[risk_key],
            "equity_risk_premium": assumptions["equity_risk_premium"],
            "cost_of_debt": assumptions["cost_of_debt_default"],
        }

    def list_model_profiles(self) -> List[Dict[str, Any]]:
        return [
            {"model_profile": key, **deepcopy(value)}
            for key, value in self.parameters.get("model_profiles", {}).items()
        ]

    def input_requirements(self) -> Dict[str, Any]:
        return {
            item["model_profile"]: {
                "required_fields": item.get("required_fields", []),
                "optional_fields": item.get("optional_fields", []),
                "implementation_status": item.get("implementation_status"),
            }
            for item in self.list_model_profiles()
        }

    def build_input_gaps(
        self,
        *,
        instrument: Dict[str, Any],
        financial_bundle: Dict[str, Any],
        model_profile: str = "nonfinancial_fcff.v1",
    ) -> Dict[str, Any]:
        facts = self._extract_financial_facts(financial_bundle)
        profile = self.parameters.get("model_profiles", {}).get(model_profile, {})
        missing = []
        for field_name in profile.get("required_fields", []):
            if self._safe_float(facts.get(field_name)) is None:
                missing.append(
                    {
                        "field": field_name,
                        "requiredness": "blocker",
                        "model_profile": model_profile,
                        "local_available": False,
                        "candidate_primary_source": self._source_candidate(field_name),
                        "candidate_fallback_sources": ["manual_override", "future_provider"],
                        "refresh_eligible": False,
                        "risk_notes": ["local_fact_not_available"],
                    }
                )
        return {
            "instrument_id": instrument.get("instrument_id"),
            "model_profile": model_profile,
            "missing_fields": missing,
            "ready": not missing,
        }

    def select_model(
        self,
        *,
        instrument: Dict[str, Any],
        input_bundle: DcfInputBundle,
        overrides: Dict[str, Any],
        model_strategy: str,
        include_comparison: bool,
    ) -> Dict[str, Any]:
        explicit = overrides.get("model_profile")
        if explicit:
            candidate = self._score_candidate(
                str(explicit),
                "forced",
                input_bundle,
                business_fit=1.0,
                financial_behavior_fit=1.0,
            )
            return self._selector_payload(
                recommended=candidate,
                candidates=[candidate],
                selection_policy="explicit_model_profile",
                include_comparison=False,
            )

        hard = self._hard_model_constraint(instrument, input_bundle)
        if hard:
            candidate = self._score_candidate(
                hard,
                "financial_sector",
                input_bundle,
                business_fit=1.0,
                financial_behavior_fit=0.8,
            )
            return self._selector_payload(
                recommended=candidate,
                candidates=[candidate],
                selection_policy="hard_model_constraint",
                include_comparison=False,
                hard_blockers=list(input_bundle.blockers),
            )

        industry_candidate = self._score_candidate(
            "nonfinancial_fcff.v1",
            "industry",
            input_bundle,
            business_fit=0.8,
            financial_behavior_fit=0.7,
        )
        characteristic_profile = self._characteristic_profile(input_bundle)
        characteristic_candidate = self._score_candidate(
            characteristic_profile,
            "company_characteristic",
            input_bundle,
            business_fit=0.7,
            financial_behavior_fit=0.9 if characteristic_profile != "nonfinancial_fcff.v1" else 0.65,
        )
        if model_strategy == "industry":
            candidates = [industry_candidate]
        elif model_strategy == "characteristic":
            candidates = [characteristic_candidate]
        else:
            candidates = [industry_candidate, characteristic_candidate]

        ranked = sorted(candidates, key=lambda item: item["score"], reverse=True)
        recommended = ranked[0]
        runner_up = ranked[1] if len(ranked) > 1 else None
        score_gap = (
            round(recommended["score"] - runner_up["score"], 4)
            if runner_up is not None
            else None
        )
        threshold = float(self.parameters.get("score_gap_threshold", 0.15))
        comparison = include_comparison or model_strategy == "compare" or (
            score_gap is not None and score_gap < threshold
        )
        policy = "score_gap_decisive"
        if comparison:
            policy = "score_gap_with_comparison"
        return self._selector_payload(
            recommended=recommended,
            candidates=ranked,
            selection_policy=policy,
            include_comparison=comparison,
            score_gap=score_gap,
        )

    def _run_nonfinancial_fcff(
        self,
        *,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
        overrides: Dict[str, Any],
        selector: Dict[str, Any],
        research_mode: bool,
    ) -> Dict[str, Any]:
        facts = bundle.financial_facts
        projection_years = int(overrides.get("projection_years", self.parameters.get("projection_years", 5)))
        terminal_growth = float(overrides.get("terminal_growth", self.parameters.get("terminal_growth", 0.03)))
        wacc_payload = self._build_wacc(bundle, overrides)
        wacc = wacc_payload["wacc"]
        if wacc <= terminal_growth:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="wacc_must_exceed_terminal_growth",
                research_mode=research_mode,
            )

        revenue = self._safe_float(facts.get("revenue"))
        operating_profit = self._safe_float(facts.get("operating_profit"))
        capex = self._safe_float(facts.get("capital_expenditure"))
        if capex is None:
            capex = self._safe_float(overrides.get("capital_expenditure"))
        blockers = []
        for field_name, value in (
            ("revenue", revenue),
            ("operating_profit", operating_profit),
            ("capital_expenditure", capex),
        ):
            if value is None:
                blockers.append(f"{field_name}_required")
        if blockers and not research_mode:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="unavailable",
                missing_reason=blockers[0],
                research_mode=research_mode,
                extra_blockers=blockers,
            )

        revenue = revenue or 0.0
        operating_profit = operating_profit or 0.0
        capex = abs(capex or 0.0)
        depreciation = self._safe_float(facts.get("depreciation_and_amortization")) or float(
            overrides.get("depreciation_and_amortization", capex * 0.7)
        )
        nwc_change = self._safe_float(facts.get("change_in_working_capital")) or float(
            overrides.get("change_in_working_capital", 0.0)
        )
        tax_rate = float(overrides.get("tax_rate", self.parameters.get("tax_rate", 0.25)))
        revenue_growth = float(overrides.get("growth_rate", self.parameters.get("base_growth_rate", 0.08)))
        operating_margin = (
            operating_profit / revenue
            if revenue
            else float(overrides.get("operating_margin", 0.1))
        )
        capex_to_sales = capex / revenue if revenue else float(overrides.get("capex_to_sales", 0.03))
        da_to_sales = depreciation / revenue if revenue else float(overrides.get("depreciation_to_sales", 0.02))
        nwc_to_sales = nwc_change / revenue if revenue else float(overrides.get("working_capital_to_sales", 0.0))

        scenarios = []
        scenario_specs = [
            ("bear", float(overrides.get("bear_growth_rate", max(revenue_growth - 0.04, -0.05)))),
            ("base", revenue_growth),
            ("bull", float(overrides.get("bull_growth_rate", revenue_growth + 0.04))),
            ("stress", float(overrides.get("stress_growth_rate", min(revenue_growth - 0.08, 0.0)))),
        ]
        for scenario_name, growth_rate in scenario_specs:
            scenarios.append(
                self._project_fcff_scenario(
                    scenario=scenario_name,
                    starting_revenue=revenue,
                    growth_rate=growth_rate,
                    operating_margin=operating_margin,
                    tax_rate=tax_rate,
                    depreciation_to_sales=da_to_sales,
                    capex_to_sales=capex_to_sales,
                    working_capital_to_sales=nwc_to_sales,
                    discount_rate=wacc,
                    terminal_growth=terminal_growth,
                    projection_years=projection_years,
                    latest_close=bundle.latest_close,
                    shares_outstanding=self._safe_positive_float(facts.get("shares_outstanding")),
                    net_debt_bridge=self._net_debt_bridge(facts),
                )
            )

        base_scenario = next(item for item in scenarios if item["scenario"] == "base")
        sensitivity = self._build_sensitivity(
            starting_revenue=revenue,
            operating_margin=operating_margin,
            tax_rate=tax_rate,
            depreciation_to_sales=da_to_sales,
            capex_to_sales=capex_to_sales,
            working_capital_to_sales=nwc_to_sales,
            terminal_growth=terminal_growth,
            projection_years=projection_years,
            latest_close=bundle.latest_close,
            shares_outstanding=self._safe_positive_float(facts.get("shares_outstanding")),
            net_debt_bridge=self._net_debt_bridge(facts),
            base_growth=revenue_growth,
            base_wacc=wacc,
        )
        warnings = list(bundle.warnings)
        if base_scenario.get("terminal_value_pct") is not None and base_scenario["terminal_value_pct"] > float(
            self.parameters.get("terminal_value_warning_threshold", 0.8)
        ):
            warnings.append("terminal_value_dominant")
        if wacc_payload.get("discount_rate_override"):
            warnings.append("discount_rate_override_used")

        result = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": "professional_dcf_fcff",
            "calc_version": "nonfinancial_fcff.v1",
            "parameter_hash": self._build_parameter_hash(overrides),
            "input_hash": bundle.input_hash,
            "status": "partial" if research_mode and blockers else "success",
            "missing_reason": blockers[0] if blockers else None,
            "model_profile": "nonfinancial_fcff.v1",
            "model_strategy": selector.get("model_strategy"),
            "recommended_model": selector.get("recommended_model"),
            "selection_confidence": selector.get("selection_confidence"),
            "selection_policy": selector.get("selection_policy"),
            "score_gap": selector.get("score_gap"),
            "model_suitability_candidates": selector.get("candidates", []),
            "selected_cash_flow_model": "fcff",
            "cash_flow_model_selection": self._select_cash_flow_model(facts),
            "readiness": self._readiness_payload(blockers, warnings, selector),
            "assumptions": {
                "wacc": wacc_payload,
                **bundle.assumptions,
            },
            "valuation_date": bundle.valuation_date,
            "data_available_cutoff": bundle.data_available_cutoff,
            "base_cash_flow": base_scenario.get("fcff"),
            "base_cash_flow_source": "fcff",
            "projection_years": projection_years,
            "shares_outstanding": self._safe_positive_float(facts.get("shares_outstanding")),
            "latest_close": bundle.latest_close,
            "beta": bundle.beta_context.get("beta"),
            "beta_source": bundle.beta_context.get("beta_source"),
            "beta_benchmark": bundle.beta_context.get("beta_benchmark"),
            "enterprise_value": base_scenario.get("enterprise_value"),
            "equity_value": base_scenario.get("equity_value"),
            "terminal_value": base_scenario.get("terminal_value"),
            "terminal_value_pct": base_scenario.get("terminal_value_pct"),
            "net_debt_adjustment": self._net_debt_bridge(facts),
            "forecast_rows": base_scenario.get("forecast_rows", []),
            "scenarios": scenarios,
            "sensitivity": sensitivity,
            "diagnostics": {
                "blockers": blockers,
                "warnings": warnings,
                "input_gaps": self.build_input_gaps(
                    instrument=instrument,
                    financial_bundle=facts,
                    model_profile="nonfinancial_fcff.v1",
                )["missing_fields"],
            },
            "warnings": warnings,
            "lineage": bundle.lineage,
            "model_comparison": None,
            "workbook": self._workbook_metadata(overrides),
        }
        if selector.get("include_model_comparison"):
            result["model_comparison"] = self._model_comparison_stub(result, selector)
        return result

    def _project_fcff_scenario(
        self,
        *,
        scenario: str,
        starting_revenue: float,
        growth_rate: float,
        operating_margin: float,
        tax_rate: float,
        depreciation_to_sales: float,
        capex_to_sales: float,
        working_capital_to_sales: float,
        discount_rate: float,
        terminal_growth: float,
        projection_years: int,
        latest_close: Optional[float],
        shares_outstanding: Optional[float],
        net_debt_bridge: Dict[str, Any],
    ) -> Dict[str, Any]:
        forecast_rows = []
        discounted_sum = 0.0
        revenue = starting_revenue
        fcff = 0.0
        for year in range(1, projection_years + 1):
            revenue *= 1 + growth_rate
            ebit = revenue * operating_margin
            nopat = ebit * (1 - tax_rate)
            depreciation = revenue * depreciation_to_sales
            capex = revenue * capex_to_sales
            nwc_change = revenue * working_capital_to_sales
            fcff = nopat + depreciation - capex - nwc_change
            discount_factor = 1 / ((1 + discount_rate) ** year)
            discounted_fcff = fcff * discount_factor
            discounted_sum += discounted_fcff
            forecast_rows.append(
                {
                    "year": year,
                    "revenue": revenue,
                    "revenue_growth": growth_rate,
                    "operating_margin": operating_margin,
                    "ebit": ebit,
                    "tax_rate": tax_rate,
                    "nopat": nopat,
                    "depreciation_and_amortization": depreciation,
                    "capital_expenditure": capex,
                    "change_in_working_capital": nwc_change,
                    "fcff": fcff,
                    "discount_factor": discount_factor,
                    "discounted_fcff": discounted_fcff,
                }
            )
        terminal_value = fcff * (1 + terminal_growth) / (discount_rate - terminal_growth)
        terminal_value_present = terminal_value / ((1 + discount_rate) ** projection_years)
        enterprise_value = discounted_sum + terminal_value_present
        equity_value = enterprise_value - float(net_debt_bridge.get("net_debt", 0.0))
        intrinsic_value_per_share = (
            equity_value / shares_outstanding
            if shares_outstanding and shares_outstanding > 0
            else None
        )
        upside_to_last_close = (
            intrinsic_value_per_share / latest_close - 1
            if intrinsic_value_per_share is not None and latest_close not in (None, 0)
            else None
        )
        denominator = enterprise_value if enterprise_value else None
        terminal_value_pct = (
            terminal_value_present / denominator
            if denominator
            else None
        )
        return {
            "scenario": scenario,
            "growth_rate": growth_rate,
            "discount_rate": discount_rate,
            "terminal_growth": terminal_growth,
            "enterprise_value": enterprise_value,
            "equity_value": equity_value,
            "terminal_value": terminal_value_present,
            "terminal_value_pct": terminal_value_pct,
            "fcff": fcff,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "upside_to_last_close": upside_to_last_close,
            "forecast_rows": forecast_rows,
            "projected_cash_flows": [
                {
                    "year": row["year"],
                    "cash_flow": row["fcff"],
                    "discounted_cash_flow": row["discounted_fcff"],
                }
                for row in forecast_rows
            ],
        }

    def _build_wacc(
        self,
        bundle: DcfInputBundle,
        overrides: Dict[str, Any],
    ) -> Dict[str, Any]:
        risk_free = float(bundle.assumptions["risk_free_rate"]["value"])
        erp = float(bundle.assumptions["equity_risk_premium"]["value"])
        beta = bundle.beta_context.get("beta")
        beta_value = float(beta if beta is not None else self.parameters.get("default_beta", 1.0))
        cost_of_equity = risk_free + beta_value * erp
        cost_of_debt = float(overrides.get("cost_of_debt", bundle.assumptions["cost_of_debt"]["value"]))
        tax_rate = float(overrides.get("tax_rate", self.parameters.get("tax_rate", 0.25)))
        debt_weight = float(overrides.get("debt_weight", self.parameters.get("target_debt_weight", 0.2)))
        equity_weight = 1 - debt_weight
        wacc = equity_weight * cost_of_equity + debt_weight * cost_of_debt * (1 - tax_rate)
        discount_rate_override = overrides.get("discount_rate")
        if discount_rate_override is not None:
            wacc = float(discount_rate_override)
        return {
            "risk_free_rate": risk_free,
            "equity_risk_premium": erp,
            "beta": beta_value,
            "beta_source": bundle.beta_context.get("beta_source"),
            "cost_of_equity": cost_of_equity,
            "cost_of_debt": cost_of_debt,
            "tax_rate": tax_rate,
            "debt_weight": debt_weight,
            "equity_weight": equity_weight,
            "wacc": wacc,
            "discount_rate_override": discount_rate_override,
            "assumption_lineage": {
                "risk_free_rate": bundle.assumptions["risk_free_rate"],
                "equity_risk_premium": bundle.assumptions["equity_risk_premium"],
                "cost_of_debt": bundle.assumptions["cost_of_debt"],
            },
        }

    def _build_sensitivity(self, **kwargs: Any) -> List[Dict[str, Any]]:
        base_wacc = float(kwargs.pop("base_wacc"))
        base_growth = float(kwargs.pop("base_growth"))
        points = []
        for growth_rate in (base_growth - 0.02, base_growth, base_growth + 0.02):
            for wacc in (base_wacc - 0.01, base_wacc, base_wacc + 0.01):
                terminal_growth = float(kwargs["terminal_growth"])
                if wacc <= terminal_growth:
                    continue
                scenario = self._project_fcff_scenario(
                    scenario="sensitivity",
                    growth_rate=growth_rate,
                    discount_rate=wacc,
                    **kwargs,
                )
                points.append(
                    {
                        "growth_rate": growth_rate,
                        "discount_rate": wacc,
                        "terminal_growth": terminal_growth,
                        "intrinsic_value_per_share": scenario["intrinsic_value_per_share"],
                    }
                )
        return points

    def _unavailable_result(
        self,
        *,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
        overrides: Dict[str, Any],
        selector: Dict[str, Any],
        status: str,
        missing_reason: str,
        research_mode: bool,
        extra_blockers: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        blockers = list(bundle.blockers)
        blockers.extend(extra_blockers or [])
        warnings = list(bundle.warnings)
        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": self.calc_method,
            "calc_version": self.calc_version,
            "parameter_hash": self._build_parameter_hash(overrides),
            "input_hash": bundle.input_hash,
            "status": status,
            "missing_reason": missing_reason,
            "model_profile": selector.get("recommended_model"),
            "model_strategy": selector.get("model_strategy"),
            "recommended_model": selector.get("recommended_model"),
            "selection_confidence": selector.get("selection_confidence"),
            "selection_policy": selector.get("selection_policy"),
            "score_gap": selector.get("score_gap"),
            "model_suitability_candidates": selector.get("candidates", []),
            "selected_cash_flow_model": None,
            "cash_flow_model_selection": None,
            "readiness": self._readiness_payload(blockers, warnings, selector),
            "assumptions": bundle.assumptions,
            "valuation_date": bundle.valuation_date,
            "data_available_cutoff": bundle.data_available_cutoff,
            "base_cash_flow": None,
            "base_cash_flow_source": "missing",
            "projection_years": int(overrides.get("projection_years", self.parameters.get("projection_years", 5))),
            "shares_outstanding": self._safe_positive_float(bundle.financial_facts.get("shares_outstanding")),
            "latest_close": bundle.latest_close,
            "beta": bundle.beta_context.get("beta"),
            "beta_source": bundle.beta_context.get("beta_source"),
            "beta_benchmark": bundle.beta_context.get("beta_benchmark"),
            "scenarios": [],
            "sensitivity": [],
            "forecast_rows": [],
            "diagnostics": {"blockers": blockers, "warnings": warnings},
            "warnings": warnings,
            "lineage": bundle.lineage,
            "model_comparison": None,
            "workbook": self._workbook_metadata(overrides),
            "research_mode": research_mode,
        }

    def _selector_payload(
        self,
        *,
        recommended: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        selection_policy: str,
        include_comparison: bool,
        score_gap: Optional[float] = None,
        hard_blockers: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return {
            "recommended_model": recommended["model_profile"],
            "selection_confidence": recommended["confidence"],
            "selection_policy": selection_policy,
            "score_gap": score_gap,
            "candidates": candidates,
            "include_model_comparison": include_comparison,
            "model_strategy": "compare" if include_comparison else "auto",
            "hard_blockers": hard_blockers or [],
        }

    def _score_candidate(
        self,
        model_profile: str,
        candidate_type: str,
        bundle: DcfInputBundle,
        *,
        business_fit: float,
        financial_behavior_fit: float,
    ) -> Dict[str, Any]:
        profile = self.parameters.get("model_profiles", {}).get(model_profile, {})
        required = profile.get("required_fields", [])
        available = sum(1 for field in required if self._safe_float(bundle.financial_facts.get(field)) is not None)
        input_readiness = available / len(required) if required else 1.0
        assumption_quality = 0.65
        if all(value.get("quality_flag") != "missing" for value in bundle.assumptions.values()):
            assumption_quality = 0.8
        lifecycle_quality = 0.4 if bundle.blockers else 0.85
        score = (
            0.30 * input_readiness
            + 0.25 * business_fit
            + 0.20 * financial_behavior_fit
            + 0.15 * assumption_quality
            + 0.10 * lifecycle_quality
        )
        input_gaps = [
            field for field in required
            if self._safe_float(bundle.financial_facts.get(field)) is None
        ]
        return {
            "model_profile": model_profile,
            "candidate_type": candidate_type,
            "score": round(score, 4),
            "confidence": round(score, 4),
            "probability": round(score, 4),
            "selection_reasons": [
                f"input_readiness={input_readiness:.2f}",
                f"business_fit={business_fit:.2f}",
                f"financial_behavior_fit={financial_behavior_fit:.2f}",
            ],
            "rejected_models": [],
            "input_gaps": input_gaps,
            "warnings": list(bundle.warnings),
            "implementation_status": profile.get("implementation_status", "unknown"),
        }

    def _extract_financial_facts(self, financial_bundle: Dict[str, Any]) -> Dict[str, Any]:
        facts = dict(financial_bundle or {})
        nested = facts.get("latest_facts") or facts.get("details") or facts.get("facts_json") or {}
        if isinstance(nested, dict):
            for key, value in nested.items():
                facts.setdefault(key, value)
        facts.setdefault("capital_expenditure", facts.get("capex"))
        facts.setdefault("depreciation_and_amortization", facts.get("depreciation"))
        facts.setdefault("change_in_working_capital", facts.get("working_capital_change"))
        facts.setdefault("cash_and_equivalents", facts.get("cash"))
        facts.setdefault("total_debt", facts.get("interest_bearing_debt") or facts.get("debt"))
        if facts.get("shares_outstanding") is None:
            valuation_inputs = facts.get("valuation_inputs") or []
            if isinstance(valuation_inputs, list) and valuation_inputs:
                latest_input = valuation_inputs[-1]
                if isinstance(latest_input, dict):
                    facts["shares_outstanding"] = latest_input.get("shares_outstanding")
        return facts

    def _hard_model_constraint(
        self,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
    ) -> Optional[str]:
        text = " ".join(
            str(value or "")
            for value in (
                instrument.get("industry"),
                instrument.get("industry_name"),
                instrument.get("sw_l1_name"),
                instrument.get("sw_l2_name"),
                instrument.get("type"),
                bundle.financial_facts.get("profile"),
            )
        ).lower()
        if "银行" in text or "bank" in text:
            return "bank_residual_income.v1"
        if "证券" in text or "broker" in text or "securities" in text:
            return "broker_excess_capital.v1"
        if "保险" in text or "insurance" in text:
            return "insurance_embedded_value_or_ddm.v1"
        return None

    def _characteristic_profile(self, bundle: DcfInputBundle) -> str:
        facts = bundle.financial_facts
        net_income = self._safe_float(facts.get("net_income"))
        rd = self._safe_float(facts.get("research_and_development") or facts.get("rd_expense"))
        revenue = self._safe_float(facts.get("revenue"))
        if net_income is not None and net_income < 0:
            return "high_growth_staged_fcff.v1"
        if rd is not None and revenue and rd / revenue > 0.15:
            return "high_growth_staged_fcff.v1"
        return "nonfinancial_fcff.v1"

    def _select_cash_flow_model(self, facts: Dict[str, Any]) -> Dict[str, Any]:
        debt = self._safe_float(facts.get("total_debt")) or 0.0
        equity = self._safe_float(facts.get("equity")) or 0.0
        leverage = debt / equity if equity > 0 else None
        if leverage is not None and leverage < 0.3 and self._safe_float(facts.get("dividend_payout_ratio")) is not None:
            selected = "fcfe"
            reasons = ["stable_low_leverage", "dividend_policy_available"]
        else:
            selected = "fcff"
            reasons = ["default_nonfinancial_enterprise_value_model"]
        return {
            "selected_cash_flow_model": selected,
            "candidate_models": ["fcff", "fcfe"],
            "selection_reasons": reasons,
            "rejected_models": ["fcfe"] if selected == "fcff" else ["fcff"],
            "input_gap_by_model": {},
            "confidence": 0.75,
            "warnings": [],
        }

    def _net_debt_bridge(self, facts: Dict[str, Any]) -> Dict[str, Any]:
        cash = self._safe_float(facts.get("cash_and_equivalents")) or 0.0
        debt = self._safe_float(facts.get("total_debt")) or 0.0
        lease = self._safe_float(facts.get("lease_liabilities")) or 0.0
        preferred = self._safe_float(facts.get("preferred_equity")) or 0.0
        minority = self._safe_float(facts.get("minority_interest")) or 0.0
        non_operating = self._safe_float(facts.get("non_operating_assets")) or 0.0
        net_debt = debt + lease + preferred + minority - cash - non_operating
        return {
            "cash_and_equivalents": cash,
            "total_debt": debt,
            "lease_liabilities": lease,
            "preferred_equity": preferred,
            "minority_interest": minority,
            "non_operating_assets": non_operating,
            "net_debt": net_debt,
            "warnings": [
                field
                for field in ("cash_and_equivalents", "total_debt")
                if self._safe_float(facts.get(field)) is None
            ],
        }

    def _readiness_payload(
        self,
        blockers: Iterable[str],
        warnings: Iterable[str],
        selector: Dict[str, Any],
    ) -> Dict[str, Any]:
        blockers_list = list(dict.fromkeys(blockers))
        warnings_list = list(dict.fromkeys(warnings))
        if blockers_list:
            level = "unavailable"
        elif warnings_list:
            level = "research_ready"
        else:
            level = "production_ready"
        return {
            "level": level,
            "ready_for_production": level == "production_ready",
            "blockers": blockers_list,
            "warnings": warnings_list,
            "available_model_profiles": [
                item["model_profile"] for item in self.list_model_profiles()
            ],
            "default_model_profile": selector.get("recommended_model"),
        }

    def _model_comparison_stub(
        self,
        result: Dict[str, Any],
        selector: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "recommended": selector.get("recommended_model"),
            "candidates": selector.get("candidates", []),
            "industry_model": result if result.get("model_profile") == "nonfinancial_fcff.v1" else None,
            "company_characteristic_model": None,
            "comparison_summary": {
                "selection_policy": selector.get("selection_policy"),
                "score_gap": selector.get("score_gap"),
            },
        }

    def _workbook_metadata(self, overrides: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not overrides.get("include_workbook"):
            return None
        return {
            "workbook_available": False,
            "workbook_artifact_id": None,
            "style": self.parameters.get("workbook", {}).get("default_style", "consulting_clean"),
            "warnings": ["workbook_builder_not_implemented"],
        }

    def _resolve_beta_context(self, overrides: Dict[str, Any]) -> Dict[str, Any]:
        beta = overrides.get("beta")
        return {
            "beta": float(beta) if beta is not None else float(self.parameters.get("default_beta", 1.0)),
            "beta_source": overrides.get("beta_source", "configured_default_beta" if beta is None else "override"),
            "beta_benchmark": overrides.get("beta_benchmark"),
            "used_in_discount_rate": overrides.get("discount_rate") is None,
        }

    def _source_candidate(self, field_name: str) -> str:
        mapping = {
            "capital_expenditure": "official_cash_flow_statement",
            "change_in_working_capital": "derived_working_capital_facts",
            "total_debt": "official_balance_sheet_or_bond_data",
            "embedded_value": "insurer_annual_report",
            "net_capital": "broker_annual_report",
        }
        return mapping.get(field_name, "official_financial_statement")

    def _build_parameter_hash(self, overrides: Dict[str, Any]) -> str:
        return self._hash_payload({"parameters": self.parameters, "overrides": overrides})

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _safe_positive_float(cls, value: Any) -> Optional[float]:
        numeric = cls._safe_float(value)
        if numeric is None or numeric <= 0:
            return None
        return numeric

    @staticmethod
    def _normalize_date(value: Any) -> Optional[str]:
        if value in (None, ""):
            return None
        text = str(value)
        return text[:10]

    @staticmethod
    def _today_iso() -> str:
        return date.today().isoformat()

    @staticmethod
    def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                ProfessionalDcfEngine._deep_update(target[key], value)
            else:
                target[key] = deepcopy(value)
