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
        "nonfinancial_fcfe.v1": {
            "candidate_type": "cash_flow_adapter",
            "implementation_status": "implemented",
            "required_fields": ["operating_cf", "net_debt_change"],
            "optional_fields": ["dividend_payout_ratio", "capital_expenditure", "maintenance_capex"],
            "supported_company_types": ["stable_low_leverage", "utility", "infrastructure"],
        },
        "asset_light_fcff.v1": {
            "candidate_type": "industry",
            "implementation_status": "guardrail",
            "required_fields": ["revenue", "operating_profit"],
            "optional_fields": ["research_and_development", "sbc_expense"],
            "supported_company_types": ["software", "internet", "asset_light"],
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
            "implementation_status": "implemented",
            "required_fields": ["equity", "net_income", "shares_outstanding"],
            "optional_fields": [
                "roe",
                "capital_adequacy_ratio",
                "npl_ratio",
                "provision_coverage_ratio",
                "dividend_payout_ratio",
            ],
            "supported_company_types": ["bank"],
        },
        "broker_excess_capital.v1": {
            "candidate_type": "financial_sector",
            "implementation_status": "implemented",
            "required_fields": ["net_income", "equity", "net_capital", "shares_outstanding"],
            "optional_fields": [
                "roe",
                "brokerage_revenue",
                "investment_income",
                "market_turnover",
                "index_level",
                "leverage_ratio",
            ],
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
            "implementation_status": "implemented",
            "required_fields": ["dividend_payout_ratio", "shares_outstanding"],
            "optional_fields": ["operating_cf", "net_income", "maintenance_capex", "regulated_return", "total_debt"],
            "supported_company_types": ["utility"],
        },
        "reit_ffo_affo_ddm.v1": {
            "candidate_type": "industry",
            "implementation_status": "implemented",
            "required_fields": ["dividend_payout_ratio", "shares_outstanding"],
            "optional_fields": ["affo", "ffo", "rental_income", "property_value", "occupancy_rate"],
            "supported_company_types": ["reit"],
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
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
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
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
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
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
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
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
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
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
        },
        "fx_usd_cny": {
            "assumption_key": "fx_usd_cny",
            "market": "FX",
            "currency": "CNY",
            "tenor": None,
            "value": 7.1,
            "unit": "fx_rate",
            "primary_source": "safe_or_pbo_fx_reference",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
        },
        "fx_hkd_cny": {
            "assumption_key": "fx_hkd_cny",
            "market": "FX",
            "currency": "CNY",
            "tenor": None,
            "value": 0.91,
            "unit": "fx_rate",
            "primary_source": "safe_or_pbo_fx_reference",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
        },
        "industry_beta_default": {
            "assumption_key": "industry_beta_default",
            "market": "GLOBAL",
            "currency": None,
            "tenor": None,
            "value": 1.0,
            "unit": "beta",
            "primary_source": "local_beta_service",
            "fallback_sources": ["manual_config"],
            "source": "manual_config",
            "quality_flag": "configured_fallback",
            "fallback_used": True,
            "as_of_date": "2026-06-04",
            "last_updated_at": "2026-06-04T00:00:00",
        },
        "commodity_price_default": {
            "assumption_key": "commodity_price_default",
            "market": "GLOBAL",
            "currency": "USD",
            "tenor": None,
            "value": None,
            "unit": "price",
            "primary_source": "commodity_exchange_or_industry_dataset",
            "fallback_sources": ["manual_config"],
            "source": None,
            "quality_flag": "missing",
            "fallback_used": False,
            "as_of_date": None,
            "last_updated_at": None,
        },
    },
    "assumption_sources": {
        "china_bond_10y": {
            "source_profile": "china_bond_10y",
            "assumption_keys": ["risk_free_rate_rmb_10y"],
            "provider": "china_bond_or_exchange_public_yield",
            "refresh_supported": False,
            "timeout_seconds": 10,
            "rate_limit_per_minute": 30,
        },
        "us_treasury_10y": {
            "source_profile": "us_treasury_10y",
            "assumption_keys": ["risk_free_rate_usd_10y"],
            "provider": "us_treasury_public_yield",
            "refresh_supported": False,
            "timeout_seconds": 10,
            "rate_limit_per_minute": 30,
        },
        "hk_gov_bond_or_efn_10y": {
            "source_profile": "hk_gov_bond_or_efn_10y",
            "assumption_keys": ["risk_free_rate_hkd_10y"],
            "provider": "hkma_or_gov_bond_public_yield",
            "refresh_supported": False,
            "timeout_seconds": 10,
            "rate_limit_per_minute": 30,
        },
        "manual_config": {
            "source_profile": "manual_config",
            "assumption_keys": ["*"],
            "provider": "versioned_research_config",
            "refresh_supported": False,
            "timeout_seconds": 0,
            "rate_limit_per_minute": None,
        },
    },
    "workbook": {
        "enabled": True,
        "default_style": "consulting_clean",
        "artifact_ttl_hours": 24,
        "artifact_dir": "data/reports/dcf_workbooks",
        "builder_dependency": "stdlib_ooxml",
    },
    "bounded_cache": {
        "enabled": True,
        "ttl_hours": 24,
        "max_entries": 128,
        "identity_fields": [
            "instrument_id",
            "valuation_date",
            "model_profile",
            "input_hash",
            "parameter_hash",
        ],
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
    company_characteristics: Tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class DcfAssumptionValue:
    """Auditable local DCF assumption value."""

    assumption_key: str
    value: Optional[float]
    unit: Optional[str]
    source: Optional[str]
    primary_source: Optional[str]
    fallback_sources: Tuple[str, ...]
    quality_flag: str
    fallback_used: bool
    as_of_date: Optional[str]
    last_updated_at: Optional[str]
    lineage_hash: str


@dataclass(frozen=True)
class DcfAssumptionSource:
    """Registered DCF assumption source profile."""

    source_profile: str
    assumption_keys: Tuple[str, ...]
    provider: str
    refresh_supported: bool
    timeout_seconds: int
    rate_limit_per_minute: Optional[int]


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

        special_guardrail = any(
            item.get("severity") in {"blocker", "guardrail"}
            for item in bundle.company_characteristics
        )
        if (
            model_profile
            not in {
                "nonfinancial_fcff.v1",
                "utility_fcfe_or_ddm.v1",
                "reit_ffo_affo_ddm.v1",
                "bank_residual_income.v1",
                "broker_excess_capital.v1",
            }
            and selector.get("include_model_comparison")
            and not (special_guardrail and not research_mode)
        ):
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

        if model_profile in {"utility_fcfe_or_ddm.v1", "reit_ffo_affo_ddm.v1"}:
            return self._run_distribution_dcf(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                research_mode=research_mode,
                model_profile=model_profile,
            )

        if model_profile == "bank_residual_income.v1":
            return self._run_bank_residual_income(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                research_mode=research_mode,
            )

        if model_profile == "broker_excess_capital.v1":
            return self._run_broker_excess_capital(
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
        company_characteristics = self._detect_company_characteristics(
            instrument,
            facts,
            valuation_date=valuation_date,
        )
        for characteristic in company_characteristics:
            code = characteristic["code"]
            severity = characteristic.get("severity")
            if severity == "blocker":
                blockers.append(code)
            elif severity == "warning":
                warnings.append(code)

        assumption_map = self.get_assumptions(
            market=str(instrument.get("exchange") or ""),
            currency=str(facts.get("currency") or instrument.get("currency") or "CNY"),
            overrides=overrides,
        )
        beta_context = self._resolve_beta_context(overrides)
        assumption_blockers, assumption_warnings = self._assumption_diagnostics(
            assumption_map,
            beta_context,
        )
        blockers.extend(assumption_blockers)
        warnings.extend(assumption_warnings)
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
                    "fallback_used": value.get("fallback_used"),
                    "lineage_hash": value.get("lineage_hash"),
                }
                for key, value in assumption_map.items()
            },
            "beta": beta_context,
            "company_characteristics": list(company_characteristics),
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
            company_characteristics=tuple(company_characteristics),
        )

    def get_assumptions(
        self,
        *,
        market: str = "",
        currency: str = "CNY",
        overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        overrides = overrides or {}
        assumptions = {
            key: self._assumption_to_dict(value)
            for key, value in deepcopy(self.parameters.get("assumptions", {})).items()
        }
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
            assumptions[risk_key] = self._assumption_to_dict(risk_free)
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
            assumptions["equity_risk_premium"] = self._assumption_to_dict(erp)

        return {
            "risk_free_rate": assumptions[risk_key],
            "equity_risk_premium": assumptions["equity_risk_premium"],
            "cost_of_debt": assumptions["cost_of_debt_default"],
            "fx_usd_cny": assumptions.get("fx_usd_cny"),
            "fx_hkd_cny": assumptions.get("fx_hkd_cny"),
            "industry_beta": assumptions.get("industry_beta_default"),
            "commodity_price": assumptions.get("commodity_price_default"),
        }

    def list_assumption_sources(self) -> List[Dict[str, Any]]:
        """Return configured local DCF assumption source profiles."""
        return [
            self._assumption_source_to_dict(value)
            for value in self.parameters.get("assumption_sources", {}).values()
        ]

    def refresh_assumptions(
        self,
        *,
        source_profile: str = "manual_config",
        timeout_seconds: Optional[int] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Explicit assumption refresh entry point.

        The first implementation is intentionally local-only. It exposes source
        profiles, timeout/rate-limit policy, and diagnostics without fetching
        remote data from valuation code.
        """
        sources = {
            item["source_profile"]: item
            for item in self.list_assumption_sources()
        }
        selected = list(sources.values()) if source_profile == "all" else [sources.get(source_profile)]
        if any(item is None for item in selected):
            return {
                "status": "invalid_source_profile",
                "source_profile": source_profile,
                "refreshed": False,
                "updated_assumption_keys": [],
                "diagnostics": {
                    "available_source_profiles": sorted(sources),
                    "remote_fetch_performed": False,
                },
            }

        results = []
        for source in selected:
            assert source is not None
            refresh_supported = bool(source.get("refresh_supported"))
            results.append(
                {
                    "source_profile": source["source_profile"],
                    "provider": source.get("provider"),
                    "assumption_keys": source.get("assumption_keys", []),
                    "timeout_seconds": int(
                        timeout_seconds
                        if timeout_seconds is not None
                        else source.get("timeout_seconds", 0) or 0
                    ),
                    "rate_limit_per_minute": source.get("rate_limit_per_minute"),
                    "refresh_supported": refresh_supported,
                    "status": "dry_run" if dry_run else ("unsupported" if not refresh_supported else "ready"),
                    "errors": [] if dry_run or refresh_supported else ["remote_refresh_adapter_not_configured"],
                }
            )
        return {
            "status": "dry_run" if dry_run else (
                "unsupported" if any(not item["refresh_supported"] for item in results) else "ready"
            ),
            "source_profile": source_profile,
            "refreshed": False,
            "updated_assumption_keys": [],
            "source_results": results,
            "diagnostics": {
                "remote_fetch_performed": False,
                "local_first": True,
                "hidden_refresh_inside_dcf": False,
            },
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
            candidate["warnings"].extend(
                self._forced_model_warnings(str(explicit), instrument, input_bundle)
            )
            return self._selector_payload(
                recommended=candidate,
                candidates=[candidate],
                selection_policy="explicit_model_profile",
                include_comparison=False,
                model_strategy=model_strategy,
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
                model_strategy=model_strategy,
                hard_blockers=list(input_bundle.blockers),
            )

        characteristic_profile = self._characteristic_profile(input_bundle)
        special_characteristic = characteristic_profile != "nonfinancial_fcff.v1"
        industry_profile = self._industry_profile(instrument, input_bundle)
        industry_candidate = self._score_candidate(
            industry_profile,
            "industry",
            input_bundle,
            business_fit=(
                0.9 if industry_profile != "nonfinancial_fcff.v1"
                else 0.45 if special_characteristic
                else 0.8
            ),
            financial_behavior_fit=0.40 if special_characteristic else 0.75,
        )
        characteristic_candidate = self._score_candidate(
            characteristic_profile,
            "company_characteristic",
            input_bundle,
            business_fit=0.82 if characteristic_profile != "nonfinancial_fcff.v1" else 0.7,
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
            model_strategy=model_strategy,
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
        terminal_method = str(overrides.get("terminal_method") or "gordon_growth")
        if terminal_method not in {"gordon_growth", "perpetual_growth"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_terminal_method",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_terminal_method:{terminal_method}"],
            )
        scenario_set = str(overrides.get("scenario_set") or "standard")
        if scenario_set not in {"standard", "downside_only"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_scenario_set",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_scenario_set:{scenario_set}"],
            )
        assumption_blockers = [blocker for blocker in bundle.blockers if blocker.startswith("assumption_")]
        if assumption_blockers:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="partial" if research_mode else "unavailable",
                missing_reason=assumption_blockers[0],
                research_mode=research_mode,
            )
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

        cash_flow_selection = self._select_cash_flow_model(
            instrument=instrument,
            facts=facts,
            overrides=overrides,
        )
        scenarios = []
        if cash_flow_selection["selected_cash_flow_model"] == "fcfe":
            return self._run_nonfinancial_fcfe(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                research_mode=research_mode,
                cash_flow_selection=cash_flow_selection,
            )

        scenario_specs = self._scenario_specs(scenario_set, revenue_growth, overrides)
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
        warnings.extend(
            warning
            for candidate in selector.get("candidates", [])
            for warning in candidate.get("warnings", [])
        )
        warnings.extend(cash_flow_selection.get("warnings", []))
        if not bool(overrides.get("include_sensitivity", True)):
            warnings.append("sensitivity_suppressed_by_request")
        if not bool(overrides.get("include_forecast_rows", True)):
            warnings.append("forecast_rows_suppressed_by_request")
        if not bool(overrides.get("include_lineage", True)):
            warnings.append("lineage_suppressed_by_request")
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
            "selected_cash_flow_model": cash_flow_selection["selected_cash_flow_model"],
            "cash_flow_model_selection": cash_flow_selection,
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
            "forecast_rows": base_scenario.get("forecast_rows", [])
            if bool(overrides.get("include_forecast_rows", True))
            else [],
            "scenarios": scenarios,
            "sensitivity": sensitivity if bool(overrides.get("include_sensitivity", True)) else [],
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
            "lineage": bundle.lineage if bool(overrides.get("include_lineage", True)) else None,
            "model_comparison": None,
            "workbook": None,
        }
        if selector.get("include_model_comparison"):
            result["model_comparison"] = self._model_comparison(result, selector)
        result["workbook"] = self._workbook_metadata(overrides, result)
        return result

    def _run_bank_residual_income(
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
        scenario_set = str(overrides.get("scenario_set") or "standard")
        if scenario_set not in {"standard", "downside_only"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_scenario_set",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_scenario_set:{scenario_set}"],
            )
        terminal_method = str(overrides.get("terminal_method") or "gordon_growth")
        if terminal_method not in {"gordon_growth", "perpetual_growth"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_terminal_method",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_terminal_method:{terminal_method}"],
            )
        assumption_blockers = [blocker for blocker in bundle.blockers if blocker.startswith("assumption_")]
        if assumption_blockers:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="partial" if research_mode else "unavailable",
                missing_reason=assumption_blockers[0],
                research_mode=research_mode,
            )
        wacc_payload = self._build_wacc(bundle, overrides)
        cost_of_equity = float(overrides.get("cost_of_equity", wacc_payload["cost_of_equity"]))
        if cost_of_equity <= terminal_growth:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="cost_of_equity_must_exceed_terminal_growth",
                research_mode=research_mode,
            )

        equity = self._safe_float(facts.get("equity") or facts.get("equity_parent") or overrides.get("equity"))
        net_income = self._safe_float(
            facts.get("net_income_parent") or facts.get("net_income") or overrides.get("net_income")
        )
        shares = self._safe_positive_float(facts.get("shares_outstanding"))
        blockers = [
            f"{field_name}_required"
            for field_name, value in (
                ("equity", equity),
                ("net_income", net_income),
                ("shares_outstanding", shares),
            )
            if value is None
        ]
        if blockers and not research_mode:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="unavailable",
                missing_reason="bank_core_inputs_missing",
                research_mode=research_mode,
                extra_blockers=["bank_core_inputs_missing", *blockers],
            )

        equity = equity or 0.0
        net_income = net_income or 0.0
        roe_raw = self._safe_float(facts.get("roe") or overrides.get("roe"))
        warnings = list(bundle.warnings)
        roe_source = "reported"
        if roe_raw is None and equity:
            roe_raw = net_income / equity
            roe_source = "derived_net_income_over_equity"
            warnings.append("bank_roe_derived_from_net_income_over_equity")
        roe = roe_raw if roe_raw is not None else 0.0
        payout_raw = self._safe_float(facts.get("dividend_payout_ratio") or overrides.get("dividend_payout_ratio"))
        payout_ratio = payout_raw
        if payout_ratio is None:
            payout_ratio = float(overrides.get("bank_default_payout_ratio", self.parameters.get("bank_default_payout_ratio", 0.3)))
            warnings.append("bank_payout_ratio_default_used")
        capital_adequacy = self._safe_float(
            facts.get("capital_adequacy_ratio") or overrides.get("capital_adequacy_ratio")
        )
        capital_threshold = float(self.parameters.get("bank_capital_adequacy_warning_threshold", 0.105))
        capital_diagnostics = {
            "capital_adequacy_ratio": capital_adequacy,
            "warning_threshold": capital_threshold,
            "status": "available",
        }
        if capital_adequacy is None:
            capital_diagnostics["status"] = "missing"
            warnings.append("bank_capital_adequacy_missing")
        elif capital_adequacy < capital_threshold:
            capital_diagnostics["status"] = "below_threshold"
            warnings.append("bank_capital_adequacy_below_threshold")

        scenario_specs = self._bank_roe_scenario_specs(scenario_set, roe, overrides)
        scenarios = [
            self._project_bank_residual_income_scenario(
                scenario=scenario_name,
                starting_equity=equity,
                roe=scenario_roe,
                payout_ratio=payout_ratio,
                cost_of_equity=cost_of_equity,
                terminal_growth=terminal_growth,
                projection_years=projection_years,
                latest_close=bundle.latest_close,
                shares_outstanding=shares,
            )
            for scenario_name, scenario_roe in scenario_specs
        ]
        base_scenario = next(item for item in scenarios if item["scenario"] == "base")
        sensitivity = self._build_bank_residual_income_sensitivity(
            starting_equity=equity,
            base_roe=roe,
            payout_ratio=payout_ratio,
            base_cost_of_equity=cost_of_equity,
            terminal_growth=terminal_growth,
            projection_years=projection_years,
            latest_close=bundle.latest_close,
            shares_outstanding=shares,
        )
        ddm_cross_check = self._bank_ddm_cross_check(
            net_income=net_income,
            payout_ratio=payout_raw,
            cost_of_equity=cost_of_equity,
            terminal_growth=terminal_growth,
            shares_outstanding=shares,
        )
        if ddm_cross_check is None:
            warnings.append("bank_ddm_cross_check_unavailable")
        if not bool(overrides.get("include_sensitivity", True)):
            warnings.append("sensitivity_suppressed_by_request")
        if not bool(overrides.get("include_forecast_rows", True)):
            warnings.append("forecast_rows_suppressed_by_request")
        if not bool(overrides.get("include_lineage", True)):
            warnings.append("lineage_suppressed_by_request")
        if blockers:
            warnings.append("research_mode_bank_core_inputs_incomplete")

        result = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": "professional_dcf_bank_residual_income",
            "calc_version": "bank_residual_income.v1",
            "parameter_hash": self._build_parameter_hash(overrides),
            "input_hash": bundle.input_hash,
            "status": "partial" if research_mode and blockers else "success",
            "missing_reason": "bank_core_inputs_missing" if blockers else None,
            "model_profile": "bank_residual_income.v1",
            "model_strategy": selector.get("model_strategy"),
            "recommended_model": selector.get("recommended_model"),
            "selection_confidence": selector.get("selection_confidence"),
            "selection_policy": selector.get("selection_policy"),
            "score_gap": selector.get("score_gap"),
            "model_suitability_candidates": selector.get("candidates", []),
            "selected_cash_flow_model": "residual_income",
            "cash_flow_model_selection": {
                "selected_cash_flow_model": "residual_income",
                "candidate_models": ["residual_income", "ddm"],
                "selection_reasons": ["bank_financial_sector_profile"],
                "rejected_models": ["fcff", "fcfe"],
                "input_gap_by_model": {"residual_income": blockers},
                "confidence": selector.get("selection_confidence"),
                "warnings": warnings,
            },
            "readiness": self._readiness_payload(blockers, warnings, selector),
            "assumptions": {"wacc": wacc_payload, **bundle.assumptions},
            "valuation_date": bundle.valuation_date,
            "data_available_cutoff": bundle.data_available_cutoff,
            "base_cash_flow": base_scenario.get("residual_income"),
            "base_cash_flow_source": "bank_residual_income",
            "projection_years": projection_years,
            "shares_outstanding": shares,
            "latest_close": bundle.latest_close,
            "beta": bundle.beta_context.get("beta"),
            "beta_source": bundle.beta_context.get("beta_source"),
            "beta_benchmark": bundle.beta_context.get("beta_benchmark"),
            "enterprise_value": None,
            "equity_value": base_scenario.get("equity_value"),
            "terminal_value": base_scenario.get("terminal_value"),
            "terminal_value_pct": base_scenario.get("terminal_value_pct"),
            "implied_pb": base_scenario.get("implied_pb"),
            "ddm_cross_check": ddm_cross_check,
            "financial_model_diagnostics": {
                "roe": roe,
                "roe_source": roe_source,
                "payout_ratio": payout_ratio,
                "payout_ratio_source": "reported" if payout_raw is not None else "configured_default",
                "capital": capital_diagnostics,
            },
            "net_debt_adjustment": None,
            "forecast_rows": base_scenario.get("forecast_rows", [])
            if bool(overrides.get("include_forecast_rows", True))
            else [],
            "scenarios": scenarios,
            "sensitivity": sensitivity if bool(overrides.get("include_sensitivity", True)) else [],
            "diagnostics": {
                "blockers": blockers,
                "warnings": warnings,
                "input_gaps": self.build_input_gaps(
                    instrument=instrument,
                    financial_bundle=facts,
                    model_profile="bank_residual_income.v1",
                )["missing_fields"],
            },
            "warnings": warnings,
            "lineage": bundle.lineage if bool(overrides.get("include_lineage", True)) else None,
            "model_comparison": None,
            "workbook": None,
        }
        if selector.get("include_model_comparison"):
            result["model_comparison"] = self._model_comparison(result, selector)
        result["workbook"] = self._workbook_metadata(overrides, result)
        return result

    def _run_broker_excess_capital(
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
        scenario_set = str(overrides.get("scenario_set") or "standard")
        if scenario_set not in {"standard", "downside_only"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_scenario_set",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_scenario_set:{scenario_set}"],
            )
        terminal_method = str(overrides.get("terminal_method") or "gordon_growth")
        if terminal_method not in {"gordon_growth", "perpetual_growth"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_terminal_method",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_terminal_method:{terminal_method}"],
            )
        assumption_blockers = [blocker for blocker in bundle.blockers if blocker.startswith("assumption_")]
        if assumption_blockers:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="partial" if research_mode else "unavailable",
                missing_reason=assumption_blockers[0],
                research_mode=research_mode,
            )
        wacc_payload = self._build_wacc(bundle, overrides)
        cost_of_equity = float(overrides.get("cost_of_equity", wacc_payload["cost_of_equity"]))
        if cost_of_equity <= terminal_growth:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="cost_of_equity_must_exceed_terminal_growth",
                research_mode=research_mode,
            )

        equity = self._safe_float(facts.get("equity") or facts.get("equity_parent") or overrides.get("equity"))
        net_income = self._safe_float(
            facts.get("net_income_parent") or facts.get("net_income") or overrides.get("net_income")
        )
        net_capital = self._safe_float(facts.get("net_capital") or overrides.get("net_capital"))
        shares = self._safe_positive_float(facts.get("shares_outstanding"))
        blockers = [
            f"{field_name}_required"
            for field_name, value in (
                ("net_income", net_income),
                ("equity", equity),
                ("net_capital", net_capital),
                ("shares_outstanding", shares),
            )
            if value is None
        ]
        if blockers and not research_mode:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="unavailable",
                missing_reason="broker_core_inputs_missing",
                research_mode=research_mode,
                extra_blockers=["broker_core_inputs_missing", *blockers],
            )

        equity = equity or 0.0
        net_income = net_income or 0.0
        net_capital = net_capital or 0.0
        roe_raw = self._safe_float(facts.get("roe") or overrides.get("roe"))
        warnings = list(bundle.warnings)
        net_capital_report_scope = str(
            facts.get("net_capital_report_scope")
            or facts.get("broker_net_capital_report_scope")
            or ""
        ).strip() or "unknown"
        if net_capital_report_scope == "unknown":
            warnings.append("broker_net_capital_report_scope_unknown")
        if net_capital_report_scope in {"parent_company", "regulatory"}:
            warnings.append("broker_net_capital_regulatory_scope_may_differ_from_accounting_equity")
        roe_source = "reported"
        if roe_raw is None and equity:
            roe_raw = net_income / equity
            roe_source = "derived_net_income_over_equity"
            warnings.append("broker_roe_derived_from_net_income_over_equity")
        reported_roe = roe_raw if roe_raw is not None else 0.0
        roe_cap = float(overrides.get("broker_normalized_roe_cap", self.parameters.get("broker_normalized_roe_cap", 0.15)))
        normalized_roe = min(reported_roe, roe_cap)
        if reported_roe > roe_cap:
            warnings.append("broker_roe_normalized_to_cap")
        payout_raw = self._safe_float(facts.get("dividend_payout_ratio") or overrides.get("dividend_payout_ratio"))
        payout_ratio = payout_raw
        if payout_ratio is None:
            payout_ratio = float(
                overrides.get("broker_default_payout_ratio", self.parameters.get("broker_default_payout_ratio", 0.25))
            )
            warnings.append("broker_payout_ratio_default_used")
        target_net_capital_to_equity = float(
            overrides.get(
                "broker_target_net_capital_to_equity",
                self.parameters.get("broker_target_net_capital_to_equity", 0.20),
            )
        )
        required_net_capital = max(equity * target_net_capital_to_equity, 0.0)
        excess_capital = max(net_capital - required_net_capital, 0.0)

        market_cycle_inputs = {
            "market_turnover": self._safe_float(facts.get("market_turnover") or overrides.get("market_turnover")),
            "index_level": self._safe_float(facts.get("index_level") or overrides.get("index_level")),
            "brokerage_revenue": self._safe_float(
                facts.get("brokerage_revenue") or overrides.get("brokerage_revenue")
            ),
            "investment_income": self._safe_float(
                facts.get("investment_income") or overrides.get("investment_income")
            ),
            "leverage_ratio": self._safe_float(facts.get("leverage_ratio") or overrides.get("leverage_ratio")),
        }
        missing_cycle_inputs = [
            field_name for field_name, value in market_cycle_inputs.items() if value is None
        ]
        if missing_cycle_inputs:
            warnings.append("broker_market_cycle_inputs_missing")
        if not bool(overrides.get("include_sensitivity", True)):
            warnings.append("sensitivity_suppressed_by_request")
        if not bool(overrides.get("include_forecast_rows", True)):
            warnings.append("forecast_rows_suppressed_by_request")
        if not bool(overrides.get("include_lineage", True)):
            warnings.append("lineage_suppressed_by_request")
        if blockers:
            warnings.append("research_mode_broker_core_inputs_incomplete")

        scenario_specs = self._broker_roe_scenario_specs(scenario_set, normalized_roe, overrides)
        scenarios = [
            self._project_broker_excess_capital_scenario(
                scenario=scenario_name,
                starting_equity=equity,
                normalized_roe=scenario_roe,
                payout_ratio=payout_ratio,
                cost_of_equity=cost_of_equity,
                terminal_growth=terminal_growth,
                projection_years=projection_years,
                latest_close=bundle.latest_close,
                shares_outstanding=shares,
                excess_capital=excess_capital,
            )
            for scenario_name, scenario_roe in scenario_specs
        ]
        base_scenario = next(item for item in scenarios if item["scenario"] == "base")
        sensitivity = self._build_broker_excess_capital_sensitivity(
            starting_equity=equity,
            base_roe=normalized_roe,
            payout_ratio=payout_ratio,
            base_cost_of_equity=cost_of_equity,
            terminal_growth=terminal_growth,
            projection_years=projection_years,
            latest_close=bundle.latest_close,
            shares_outstanding=shares,
            excess_capital=excess_capital,
        )

        result = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": "professional_dcf_broker_excess_capital",
            "calc_version": "broker_excess_capital.v1",
            "parameter_hash": self._build_parameter_hash(overrides),
            "input_hash": bundle.input_hash,
            "status": "partial" if research_mode and blockers else "success",
            "missing_reason": "broker_core_inputs_missing" if blockers else None,
            "model_profile": "broker_excess_capital.v1",
            "model_strategy": selector.get("model_strategy"),
            "recommended_model": selector.get("recommended_model"),
            "selection_confidence": selector.get("selection_confidence"),
            "selection_policy": selector.get("selection_policy"),
            "score_gap": selector.get("score_gap"),
            "model_suitability_candidates": selector.get("candidates", []),
            "selected_cash_flow_model": "broker_residual_income",
            "cash_flow_model_selection": {
                "selected_cash_flow_model": "broker_residual_income",
                "candidate_models": ["broker_residual_income", "excess_capital_adjustment"],
                "selection_reasons": ["broker_financial_sector_profile"],
                "rejected_models": ["fcff", "fcfe"],
                "input_gap_by_model": {"broker_residual_income": blockers},
                "confidence": selector.get("selection_confidence"),
                "warnings": warnings,
            },
            "readiness": self._readiness_payload(blockers, warnings, selector),
            "assumptions": {"wacc": wacc_payload, **bundle.assumptions},
            "valuation_date": bundle.valuation_date,
            "data_available_cutoff": bundle.data_available_cutoff,
            "base_cash_flow": base_scenario.get("residual_income"),
            "base_cash_flow_source": "broker_residual_income",
            "projection_years": projection_years,
            "shares_outstanding": shares,
            "latest_close": bundle.latest_close,
            "beta": bundle.beta_context.get("beta"),
            "beta_source": bundle.beta_context.get("beta_source"),
            "beta_benchmark": bundle.beta_context.get("beta_benchmark"),
            "enterprise_value": None,
            "equity_value": base_scenario.get("equity_value"),
            "terminal_value": base_scenario.get("terminal_value"),
            "terminal_value_pct": base_scenario.get("terminal_value_pct"),
            "implied_pb": base_scenario.get("implied_pb"),
            "normalized_roe": normalized_roe,
            "reported_roe": reported_roe,
            "excess_capital": excess_capital,
            "broker_model_diagnostics": {
                "reported_roe": reported_roe,
                "normalized_roe": normalized_roe,
                "roe_source": roe_source,
                "normalized_roe_cap": roe_cap,
                "payout_ratio": payout_ratio,
                "payout_ratio_source": "reported" if payout_raw is not None else "configured_default",
                "net_capital": net_capital,
                "net_capital_report_scope": net_capital_report_scope,
                "required_net_capital": required_net_capital,
                "target_net_capital_to_equity": target_net_capital_to_equity,
                "excess_capital": excess_capital,
                "market_cycle_inputs": market_cycle_inputs,
                "missing_market_cycle_inputs": missing_cycle_inputs,
            },
            "financial_model_diagnostics": {
                "model_type": "broker_excess_capital",
                "normalized_roe": normalized_roe,
                "excess_capital": excess_capital,
                "net_capital_report_scope": net_capital_report_scope,
                "missing_market_cycle_inputs": missing_cycle_inputs,
            },
            "net_debt_adjustment": None,
            "forecast_rows": base_scenario.get("forecast_rows", [])
            if bool(overrides.get("include_forecast_rows", True))
            else [],
            "scenarios": scenarios,
            "sensitivity": sensitivity if bool(overrides.get("include_sensitivity", True)) else [],
            "diagnostics": {
                "blockers": blockers,
                "warnings": warnings,
                "input_gaps": self.build_input_gaps(
                    instrument=instrument,
                    financial_bundle=facts,
                    model_profile="broker_excess_capital.v1",
                )["missing_fields"],
            },
            "warnings": warnings,
            "lineage": bundle.lineage if bool(overrides.get("include_lineage", True)) else None,
            "model_comparison": None,
            "workbook": None,
        }
        if selector.get("include_model_comparison"):
            result["model_comparison"] = self._model_comparison(result, selector)
        result["workbook"] = self._workbook_metadata(overrides, result)
        return result

    def _run_nonfinancial_fcfe(
        self,
        *,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
        overrides: Dict[str, Any],
        selector: Dict[str, Any],
        research_mode: bool,
        cash_flow_selection: Dict[str, Any],
    ) -> Dict[str, Any]:
        facts = bundle.financial_facts
        projection_years = int(overrides.get("projection_years", self.parameters.get("projection_years", 5)))
        terminal_growth = float(overrides.get("terminal_growth", self.parameters.get("terminal_growth", 0.03)))
        terminal_method = str(overrides.get("terminal_method") or "gordon_growth")
        if terminal_method not in {"gordon_growth", "perpetual_growth"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_terminal_method",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_terminal_method:{terminal_method}"],
            )
        scenario_set = str(overrides.get("scenario_set") or "standard")
        if scenario_set not in {"standard", "downside_only"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_scenario_set",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_scenario_set:{scenario_set}"],
            )
        assumption_blockers = [blocker for blocker in bundle.blockers if blocker.startswith("assumption_")]
        if assumption_blockers:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="partial" if research_mode else "unavailable",
                missing_reason=assumption_blockers[0],
                research_mode=research_mode,
            )

        wacc_payload = self._build_wacc(bundle, overrides)
        cost_of_equity = float(overrides.get("cost_of_equity", wacc_payload["cost_of_equity"]))
        if cost_of_equity <= terminal_growth:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="cost_of_equity_must_exceed_terminal_growth",
                research_mode=research_mode,
            )

        operating_cf = self._safe_float(facts.get("operating_cf") or overrides.get("operating_cf"))
        capex = self._safe_float(
            facts.get("maintenance_capex")
            or overrides.get("maintenance_capex")
            or facts.get("capital_expenditure")
            or overrides.get("capital_expenditure")
        )
        net_debt_change = self._safe_float(facts.get("net_debt_change") or overrides.get("net_debt_change"))
        blockers = [
            f"{field_name}_required"
            for field_name, value in (
                ("operating_cf", operating_cf),
                ("capital_expenditure", capex),
                ("net_debt_change", net_debt_change),
            )
            if value is None
        ]
        if blockers and not research_mode:
            result = self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="unavailable",
                missing_reason="fcfe_inputs_missing",
                research_mode=research_mode,
                extra_blockers=["fcfe_inputs_missing", *blockers],
            )
            result["selected_cash_flow_model"] = "fcfe"
            result["cash_flow_model_selection"] = cash_flow_selection
            result["base_cash_flow_source"] = "fcfe"
            return result

        operating_cf = operating_cf or 0.0
        capex = abs(capex or 0.0)
        net_debt_change = net_debt_change or 0.0
        starting_fcfe = operating_cf - capex + net_debt_change
        fcfe_growth = float(overrides.get("fcfe_growth_rate", overrides.get("growth_rate", self.parameters.get("base_growth_rate", 0.08))))
        scenario_specs = self._scenario_specs(scenario_set, fcfe_growth, overrides)
        scenarios = [
            self._project_fcfe_scenario(
                scenario=scenario_name,
                starting_fcfe=starting_fcfe,
                growth_rate=growth_rate,
                discount_rate=cost_of_equity,
                terminal_growth=terminal_growth,
                projection_years=projection_years,
                latest_close=bundle.latest_close,
                shares_outstanding=self._safe_positive_float(facts.get("shares_outstanding")),
            )
            for scenario_name, growth_rate in scenario_specs
        ]
        base_scenario = next(item for item in scenarios if item["scenario"] == "base")
        sensitivity = self._build_fcfe_sensitivity(
            starting_fcfe=starting_fcfe,
            base_growth=fcfe_growth,
            base_discount_rate=cost_of_equity,
            terminal_growth=terminal_growth,
            projection_years=projection_years,
            latest_close=bundle.latest_close,
            shares_outstanding=self._safe_positive_float(facts.get("shares_outstanding")),
        )
        warnings = list(bundle.warnings)
        warnings.extend(
            warning
            for candidate in selector.get("candidates", [])
            for warning in candidate.get("warnings", [])
        )
        warnings.extend(cash_flow_selection.get("warnings", []))
        if not bool(overrides.get("include_sensitivity", True)):
            warnings.append("sensitivity_suppressed_by_request")
        if not bool(overrides.get("include_forecast_rows", True)):
            warnings.append("forecast_rows_suppressed_by_request")
        if not bool(overrides.get("include_lineage", True)):
            warnings.append("lineage_suppressed_by_request")
        if wacc_payload.get("discount_rate_override"):
            warnings.append("discount_rate_override_used")
        if blockers:
            warnings.append("research_mode_fcfe_inputs_incomplete")

        result = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": "professional_dcf_fcfe",
            "calc_version": "nonfinancial_fcfe.v1",
            "parameter_hash": self._build_parameter_hash(overrides),
            "input_hash": bundle.input_hash,
            "status": "partial" if research_mode and blockers else "success",
            "missing_reason": "fcfe_inputs_missing" if blockers else None,
            "model_profile": "nonfinancial_fcff.v1",
            "model_strategy": selector.get("model_strategy"),
            "recommended_model": selector.get("recommended_model"),
            "selection_confidence": selector.get("selection_confidence"),
            "selection_policy": selector.get("selection_policy"),
            "score_gap": selector.get("score_gap"),
            "model_suitability_candidates": selector.get("candidates", []),
            "selected_cash_flow_model": "fcfe",
            "cash_flow_model_selection": cash_flow_selection,
            "readiness": self._readiness_payload(blockers, warnings, selector),
            "assumptions": {"wacc": wacc_payload, **bundle.assumptions},
            "valuation_date": bundle.valuation_date,
            "data_available_cutoff": bundle.data_available_cutoff,
            "base_cash_flow": base_scenario.get("fcfe"),
            "base_cash_flow_source": "fcfe",
            "projection_years": projection_years,
            "shares_outstanding": self._safe_positive_float(facts.get("shares_outstanding")),
            "latest_close": bundle.latest_close,
            "beta": bundle.beta_context.get("beta"),
            "beta_source": bundle.beta_context.get("beta_source"),
            "beta_benchmark": bundle.beta_context.get("beta_benchmark"),
            "enterprise_value": None,
            "equity_value": base_scenario.get("equity_value"),
            "terminal_value": base_scenario.get("terminal_value"),
            "terminal_value_pct": base_scenario.get("terminal_value_pct"),
            "net_debt_adjustment": None,
            "forecast_rows": base_scenario.get("forecast_rows", [])
            if bool(overrides.get("include_forecast_rows", True))
            else [],
            "scenarios": scenarios,
            "sensitivity": sensitivity if bool(overrides.get("include_sensitivity", True)) else [],
            "diagnostics": {
                "blockers": blockers,
                "warnings": warnings,
                "input_gaps": self.build_input_gaps(
                    instrument=instrument,
                    financial_bundle=facts,
                    model_profile="nonfinancial_fcfe.v1",
                )["missing_fields"],
            },
            "warnings": warnings,
            "lineage": bundle.lineage if bool(overrides.get("include_lineage", True)) else None,
            "model_comparison": None,
            "workbook": None,
        }
        if selector.get("include_model_comparison"):
            result["model_comparison"] = self._model_comparison(result, selector)
        result["workbook"] = self._workbook_metadata(overrides, result)
        return result

    def _run_distribution_dcf(
        self,
        *,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
        overrides: Dict[str, Any],
        selector: Dict[str, Any],
        research_mode: bool,
        model_profile: str,
    ) -> Dict[str, Any]:
        facts = bundle.financial_facts
        projection_years = int(overrides.get("projection_years", self.parameters.get("projection_years", 5)))
        terminal_growth = float(overrides.get("terminal_growth", self.parameters.get("terminal_growth", 0.03)))
        terminal_method = str(overrides.get("terminal_method") or "gordon_growth")
        if terminal_method not in {"gordon_growth", "perpetual_growth"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_terminal_method",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_terminal_method:{terminal_method}"],
            )
        scenario_set = str(overrides.get("scenario_set") or "standard")
        if scenario_set not in {"standard", "downside_only"}:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="unsupported_scenario_set",
                research_mode=research_mode,
                extra_blockers=[f"unsupported_scenario_set:{scenario_set}"],
            )
        assumption_blockers = [blocker for blocker in bundle.blockers if blocker.startswith("assumption_")]
        if assumption_blockers:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="partial" if research_mode else "unavailable",
                missing_reason=assumption_blockers[0],
                research_mode=research_mode,
            )
        wacc_payload = self._build_wacc(bundle, overrides)
        cost_of_equity = float(overrides.get("cost_of_equity", wacc_payload["cost_of_equity"]))
        if cost_of_equity <= terminal_growth:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="invalid_parameters",
                missing_reason="cost_of_equity_must_exceed_terminal_growth",
                research_mode=research_mode,
            )
        base_cash_flow, base_source, extra_warnings = self._distribution_cash_flow_base(
            facts=facts,
            overrides=overrides,
            model_profile=model_profile,
            research_mode=research_mode,
        )
        payout_ratio = self._safe_float(facts.get("dividend_payout_ratio") or overrides.get("dividend_payout_ratio"))
        shares = self._safe_positive_float(facts.get("shares_outstanding"))
        blockers = []
        if base_cash_flow is None:
            blockers.append("distribution_cash_flow_required")
        if payout_ratio is None:
            blockers.append("dividend_payout_ratio_required")
        if shares is None:
            blockers.append("shares_outstanding_required")
        if blockers and not research_mode:
            return self._unavailable_result(
                instrument=instrument,
                bundle=bundle,
                overrides=overrides,
                selector=selector,
                status="unavailable",
                missing_reason="distribution_inputs_missing",
                research_mode=research_mode,
                extra_blockers=["distribution_inputs_missing", *blockers],
            )
        base_cash_flow = base_cash_flow or 0.0
        payout_ratio = payout_ratio if payout_ratio is not None else 0.0
        distribution_base = base_cash_flow * payout_ratio
        growth_rate = float(overrides.get("distribution_growth_rate", overrides.get("growth_rate", terminal_growth)))
        scenario_specs = self._scenario_specs(scenario_set, growth_rate, overrides)
        scenarios = [
            self._project_distribution_scenario(
                scenario=scenario_name,
                starting_distribution=distribution_base,
                growth_rate=scenario_growth,
                discount_rate=cost_of_equity,
                terminal_growth=terminal_growth,
                projection_years=projection_years,
                latest_close=bundle.latest_close,
                shares_outstanding=shares,
            )
            for scenario_name, scenario_growth in scenario_specs
        ]
        base_scenario = next(item for item in scenarios if item["scenario"] == "base")
        warnings = list(bundle.warnings)
        warnings.extend(extra_warnings)
        if blockers:
            warnings.append("research_mode_distribution_inputs_incomplete")
        if not bool(overrides.get("include_sensitivity", True)):
            warnings.append("sensitivity_suppressed_by_request")
        if not bool(overrides.get("include_forecast_rows", True)):
            warnings.append("forecast_rows_suppressed_by_request")
        if not bool(overrides.get("include_lineage", True)):
            warnings.append("lineage_suppressed_by_request")
        result = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": "professional_dcf_distribution",
            "calc_version": model_profile,
            "parameter_hash": self._build_parameter_hash(overrides),
            "input_hash": bundle.input_hash,
            "status": "partial" if research_mode and blockers else "success",
            "missing_reason": "distribution_inputs_missing" if blockers else None,
            "model_profile": model_profile,
            "model_strategy": selector.get("model_strategy"),
            "recommended_model": selector.get("recommended_model"),
            "selection_confidence": selector.get("selection_confidence"),
            "selection_policy": selector.get("selection_policy"),
            "score_gap": selector.get("score_gap"),
            "model_suitability_candidates": selector.get("candidates", []),
            "selected_cash_flow_model": "ddm",
            "cash_flow_model_selection": {
                "selected_cash_flow_model": "ddm",
                "candidate_models": ["fcfe", "ddm"],
                "selection_reasons": [f"{model_profile}_distribution_profile"],
                "rejected_models": ["fcff"],
                "input_gap_by_model": {"ddm": blockers},
                "confidence": selector.get("selection_confidence"),
                "warnings": warnings,
            },
            "readiness": self._readiness_payload(blockers, warnings, selector),
            "assumptions": {"wacc": wacc_payload, **bundle.assumptions},
            "valuation_date": bundle.valuation_date,
            "data_available_cutoff": bundle.data_available_cutoff,
            "base_cash_flow": distribution_base,
            "base_cash_flow_source": base_source,
            "projection_years": projection_years,
            "shares_outstanding": shares,
            "latest_close": bundle.latest_close,
            "beta": bundle.beta_context.get("beta"),
            "beta_source": bundle.beta_context.get("beta_source"),
            "beta_benchmark": bundle.beta_context.get("beta_benchmark"),
            "enterprise_value": None,
            "equity_value": base_scenario.get("equity_value"),
            "terminal_value": base_scenario.get("terminal_value"),
            "terminal_value_pct": base_scenario.get("terminal_value_pct"),
            "net_debt_adjustment": None,
            "forecast_rows": base_scenario.get("forecast_rows", [])
            if bool(overrides.get("include_forecast_rows", True))
            else [],
            "scenarios": scenarios,
            "sensitivity": self._build_distribution_sensitivity(
                starting_distribution=distribution_base,
                base_growth=growth_rate,
                base_discount_rate=cost_of_equity,
                terminal_growth=terminal_growth,
                projection_years=projection_years,
                latest_close=bundle.latest_close,
                shares_outstanding=shares,
            )
            if bool(overrides.get("include_sensitivity", True))
            else [],
            "diagnostics": {
                "blockers": blockers,
                "warnings": warnings,
                "input_gaps": self.build_input_gaps(
                    instrument=instrument,
                    financial_bundle=facts,
                    model_profile=model_profile,
                )["missing_fields"],
            },
            "warnings": warnings,
            "lineage": bundle.lineage if bool(overrides.get("include_lineage", True)) else None,
            "model_comparison": None,
            "workbook": None,
        }
        if selector.get("include_model_comparison"):
            result["model_comparison"] = self._model_comparison(result, selector)
        result["workbook"] = self._workbook_metadata(overrides, result)
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

    def _project_fcfe_scenario(
        self,
        *,
        scenario: str,
        starting_fcfe: float,
        growth_rate: float,
        discount_rate: float,
        terminal_growth: float,
        projection_years: int,
        latest_close: Optional[float],
        shares_outstanding: Optional[float],
    ) -> Dict[str, Any]:
        forecast_rows = []
        discounted_sum = 0.0
        fcfe = starting_fcfe
        for year in range(1, projection_years + 1):
            fcfe *= 1 + growth_rate
            discount_factor = 1 / ((1 + discount_rate) ** year)
            discounted_fcfe = fcfe * discount_factor
            discounted_sum += discounted_fcfe
            forecast_rows.append(
                {
                    "year": year,
                    "fcfe_growth": growth_rate,
                    "fcfe": fcfe,
                    "discount_factor": discount_factor,
                    "discounted_fcfe": discounted_fcfe,
                }
            )
        terminal_value = fcfe * (1 + terminal_growth) / (discount_rate - terminal_growth)
        terminal_value_present = terminal_value / ((1 + discount_rate) ** projection_years)
        equity_value = discounted_sum + terminal_value_present
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
        return {
            "scenario": scenario,
            "growth_rate": growth_rate,
            "discount_rate": discount_rate,
            "terminal_growth": terminal_growth,
            "enterprise_value": None,
            "equity_value": equity_value,
            "terminal_value": terminal_value_present,
            "terminal_value_pct": terminal_value_present / equity_value if equity_value else None,
            "fcfe": fcfe,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "upside_to_last_close": upside_to_last_close,
            "forecast_rows": forecast_rows,
            "projected_cash_flows": [
                {
                    "year": row["year"],
                    "cash_flow": row["fcfe"],
                    "discounted_cash_flow": row["discounted_fcfe"],
                }
                for row in forecast_rows
            ],
        }

    def _project_distribution_scenario(
        self,
        *,
        scenario: str,
        starting_distribution: float,
        growth_rate: float,
        discount_rate: float,
        terminal_growth: float,
        projection_years: int,
        latest_close: Optional[float],
        shares_outstanding: Optional[float],
    ) -> Dict[str, Any]:
        forecast_rows = []
        discounted_sum = 0.0
        distribution = starting_distribution
        for year in range(1, projection_years + 1):
            distribution *= 1 + growth_rate
            discount_factor = 1 / ((1 + discount_rate) ** year)
            discounted_distribution = distribution * discount_factor
            discounted_sum += discounted_distribution
            forecast_rows.append(
                {
                    "year": year,
                    "distribution_growth": growth_rate,
                    "distribution": distribution,
                    "discount_factor": discount_factor,
                    "discounted_distribution": discounted_distribution,
                }
            )
        terminal_value = distribution * (1 + terminal_growth) / (discount_rate - terminal_growth)
        terminal_value_present = terminal_value / ((1 + discount_rate) ** projection_years)
        equity_value = discounted_sum + terminal_value_present
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
        dividend_yield = (
            starting_distribution / (latest_close * shares_outstanding)
            if latest_close not in (None, 0) and shares_outstanding
            else None
        )
        return {
            "scenario": scenario,
            "growth_rate": growth_rate,
            "discount_rate": discount_rate,
            "terminal_growth": terminal_growth,
            "enterprise_value": None,
            "equity_value": equity_value,
            "terminal_value": terminal_value_present,
            "terminal_value_pct": terminal_value_present / equity_value if equity_value else None,
            "distribution": distribution,
            "dividend_yield": dividend_yield,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "upside_to_last_close": upside_to_last_close,
            "forecast_rows": forecast_rows,
            "projected_cash_flows": [
                {
                    "year": row["year"],
                    "cash_flow": row["distribution"],
                    "discounted_cash_flow": row["discounted_distribution"],
                }
                for row in forecast_rows
            ],
        }

    def _project_bank_residual_income_scenario(
        self,
        *,
        scenario: str,
        starting_equity: float,
        roe: float,
        payout_ratio: float,
        cost_of_equity: float,
        terminal_growth: float,
        projection_years: int,
        latest_close: Optional[float],
        shares_outstanding: Optional[float],
    ) -> Dict[str, Any]:
        forecast_rows = []
        discounted_sum = 0.0
        beginning_book = starting_equity
        residual_income = 0.0
        for year in range(1, projection_years + 1):
            net_income = beginning_book * roe
            dividend = net_income * payout_ratio
            equity_charge = beginning_book * cost_of_equity
            residual_income = net_income - equity_charge
            discount_factor = 1 / ((1 + cost_of_equity) ** year)
            discounted_residual_income = residual_income * discount_factor
            discounted_sum += discounted_residual_income
            ending_book = beginning_book + net_income - dividend
            forecast_rows.append(
                {
                    "year": year,
                    "beginning_book_equity": beginning_book,
                    "roe": roe,
                    "net_income": net_income,
                    "payout_ratio": payout_ratio,
                    "dividend": dividend,
                    "cost_of_equity": cost_of_equity,
                    "equity_charge": equity_charge,
                    "residual_income": residual_income,
                    "discount_factor": discount_factor,
                    "discounted_residual_income": discounted_residual_income,
                    "ending_book_equity": ending_book,
                }
            )
            beginning_book = ending_book
        terminal_residual_income = residual_income * (1 + terminal_growth)
        terminal_value = terminal_residual_income / (cost_of_equity - terminal_growth)
        terminal_value_present = terminal_value / ((1 + cost_of_equity) ** projection_years)
        equity_value = starting_equity + discounted_sum + terminal_value_present
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
        return {
            "scenario": scenario,
            "roe": roe,
            "discount_rate": cost_of_equity,
            "cost_of_equity": cost_of_equity,
            "terminal_growth": terminal_growth,
            "enterprise_value": None,
            "equity_value": equity_value,
            "book_equity": starting_equity,
            "implied_pb": equity_value / starting_equity if starting_equity else None,
            "terminal_value": terminal_value_present,
            "terminal_value_pct": terminal_value_present / equity_value if equity_value else None,
            "residual_income": residual_income,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "upside_to_last_close": upside_to_last_close,
            "forecast_rows": forecast_rows,
            "projected_cash_flows": [
                {
                    "year": row["year"],
                    "cash_flow": row["residual_income"],
                    "discounted_cash_flow": row["discounted_residual_income"],
                }
                for row in forecast_rows
            ],
        }

    def _project_broker_excess_capital_scenario(
        self,
        *,
        scenario: str,
        starting_equity: float,
        normalized_roe: float,
        payout_ratio: float,
        cost_of_equity: float,
        terminal_growth: float,
        projection_years: int,
        latest_close: Optional[float],
        shares_outstanding: Optional[float],
        excess_capital: float,
    ) -> Dict[str, Any]:
        forecast_rows = []
        discounted_sum = 0.0
        beginning_book = starting_equity
        residual_income = 0.0
        for year in range(1, projection_years + 1):
            normalized_net_income = beginning_book * normalized_roe
            dividend = normalized_net_income * payout_ratio
            equity_charge = beginning_book * cost_of_equity
            residual_income = normalized_net_income - equity_charge
            discount_factor = 1 / ((1 + cost_of_equity) ** year)
            discounted_residual_income = residual_income * discount_factor
            discounted_sum += discounted_residual_income
            ending_book = beginning_book + normalized_net_income - dividend
            forecast_rows.append(
                {
                    "year": year,
                    "beginning_book_equity": beginning_book,
                    "normalized_roe": normalized_roe,
                    "normalized_net_income": normalized_net_income,
                    "payout_ratio": payout_ratio,
                    "dividend": dividend,
                    "cost_of_equity": cost_of_equity,
                    "equity_charge": equity_charge,
                    "residual_income": residual_income,
                    "discount_factor": discount_factor,
                    "discounted_residual_income": discounted_residual_income,
                    "ending_book_equity": ending_book,
                }
            )
            beginning_book = ending_book
        terminal_residual_income = residual_income * (1 + terminal_growth)
        terminal_value = terminal_residual_income / (cost_of_equity - terminal_growth)
        terminal_value_present = terminal_value / ((1 + cost_of_equity) ** projection_years)
        franchise_value = starting_equity + discounted_sum + terminal_value_present
        equity_value = franchise_value + excess_capital
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
        return {
            "scenario": scenario,
            "normalized_roe": normalized_roe,
            "discount_rate": cost_of_equity,
            "cost_of_equity": cost_of_equity,
            "terminal_growth": terminal_growth,
            "enterprise_value": None,
            "equity_value": equity_value,
            "franchise_value": franchise_value,
            "book_equity": starting_equity,
            "excess_capital": excess_capital,
            "implied_pb": equity_value / starting_equity if starting_equity else None,
            "terminal_value": terminal_value_present,
            "terminal_value_pct": terminal_value_present / equity_value if equity_value else None,
            "residual_income": residual_income,
            "intrinsic_value_per_share": intrinsic_value_per_share,
            "upside_to_last_close": upside_to_last_close,
            "forecast_rows": forecast_rows,
            "projected_cash_flows": [
                {
                    "year": row["year"],
                    "cash_flow": row["residual_income"],
                    "discounted_cash_flow": row["discounted_residual_income"],
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

    def _build_fcfe_sensitivity(self, **kwargs: Any) -> List[Dict[str, Any]]:
        base_discount_rate = float(kwargs.pop("base_discount_rate"))
        base_growth = float(kwargs.pop("base_growth"))
        points = []
        for growth_rate in (base_growth - 0.02, base_growth, base_growth + 0.02):
            for discount_rate in (base_discount_rate - 0.01, base_discount_rate, base_discount_rate + 0.01):
                terminal_growth = float(kwargs["terminal_growth"])
                if discount_rate <= terminal_growth:
                    continue
                scenario = self._project_fcfe_scenario(
                    scenario="sensitivity",
                    growth_rate=growth_rate,
                    discount_rate=discount_rate,
                    **kwargs,
                )
                points.append(
                    {
                        "growth_rate": growth_rate,
                        "discount_rate": discount_rate,
                        "terminal_growth": terminal_growth,
                        "intrinsic_value_per_share": scenario["intrinsic_value_per_share"],
                    }
                )
        return points

    def _build_distribution_sensitivity(self, **kwargs: Any) -> List[Dict[str, Any]]:
        base_discount_rate = float(kwargs.pop("base_discount_rate"))
        base_growth = float(kwargs.pop("base_growth"))
        points = []
        for growth_rate in (base_growth - 0.01, base_growth, base_growth + 0.01):
            for discount_rate in (base_discount_rate - 0.01, base_discount_rate, base_discount_rate + 0.01):
                terminal_growth = float(kwargs["terminal_growth"])
                if discount_rate <= terminal_growth:
                    continue
                scenario = self._project_distribution_scenario(
                    scenario="sensitivity",
                    growth_rate=growth_rate,
                    discount_rate=discount_rate,
                    **kwargs,
                )
                points.append(
                    {
                        "growth_rate": growth_rate,
                        "discount_rate": discount_rate,
                        "terminal_growth": terminal_growth,
                        "intrinsic_value_per_share": scenario["intrinsic_value_per_share"],
                    }
                )
        return points

    def _build_bank_residual_income_sensitivity(self, **kwargs: Any) -> List[Dict[str, Any]]:
        base_cost_of_equity = float(kwargs.pop("base_cost_of_equity"))
        base_roe = float(kwargs.pop("base_roe"))
        points = []
        for roe in (base_roe - 0.02, base_roe, base_roe + 0.02):
            for cost_of_equity in (base_cost_of_equity - 0.01, base_cost_of_equity, base_cost_of_equity + 0.01):
                terminal_growth = float(kwargs["terminal_growth"])
                if cost_of_equity <= terminal_growth:
                    continue
                scenario = self._project_bank_residual_income_scenario(
                    scenario="sensitivity",
                    roe=roe,
                    cost_of_equity=cost_of_equity,
                    **kwargs,
                )
                points.append(
                    {
                        "roe": roe,
                        "cost_of_equity": cost_of_equity,
                        "terminal_growth": terminal_growth,
                        "implied_pb": scenario["implied_pb"],
                        "intrinsic_value_per_share": scenario["intrinsic_value_per_share"],
                    }
                )
        return points

    def _build_broker_excess_capital_sensitivity(self, **kwargs: Any) -> List[Dict[str, Any]]:
        base_cost_of_equity = float(kwargs.pop("base_cost_of_equity"))
        base_roe = float(kwargs.pop("base_roe"))
        points = []
        for roe in (max(base_roe - 0.02, 0.0), base_roe, base_roe + 0.02):
            for cost_of_equity in (base_cost_of_equity - 0.01, base_cost_of_equity, base_cost_of_equity + 0.01):
                terminal_growth = float(kwargs["terminal_growth"])
                if cost_of_equity <= terminal_growth:
                    continue
                scenario = self._project_broker_excess_capital_scenario(
                    scenario="sensitivity",
                    normalized_roe=roe,
                    cost_of_equity=cost_of_equity,
                    **kwargs,
                )
                points.append(
                    {
                        "normalized_roe": roe,
                        "cost_of_equity": cost_of_equity,
                        "terminal_growth": terminal_growth,
                        "excess_capital": scenario["excess_capital"],
                        "implied_pb": scenario["implied_pb"],
                        "intrinsic_value_per_share": scenario["intrinsic_value_per_share"],
                    }
                )
        return points

    def _bank_roe_scenario_specs(
        self,
        scenario_set: str,
        roe: float,
        overrides: Dict[str, Any],
    ) -> List[Tuple[str, float]]:
        specs = [
            ("bear", float(overrides.get("bear_roe", max(roe - 0.02, 0.0)))),
            ("base", roe),
            ("bull", float(overrides.get("bull_roe", roe + 0.02))),
            ("stress", float(overrides.get("stress_roe", max(roe - 0.04, 0.0)))),
        ]
        if scenario_set == "downside_only":
            return [item for item in specs if item[0] in {"bear", "base", "stress"}]
        return specs

    def _broker_roe_scenario_specs(
        self,
        scenario_set: str,
        normalized_roe: float,
        overrides: Dict[str, Any],
    ) -> List[Tuple[str, float]]:
        specs = [
            ("bear", float(overrides.get("bear_normalized_roe", max(normalized_roe - 0.02, 0.0)))),
            ("base", normalized_roe),
            ("bull", float(overrides.get("bull_normalized_roe", normalized_roe + 0.02))),
            ("stress", float(overrides.get("stress_normalized_roe", max(normalized_roe - 0.04, 0.0)))),
        ]
        if scenario_set == "downside_only":
            return [item for item in specs if item[0] in {"bear", "base", "stress"}]
        return specs

    def _bank_ddm_cross_check(
        self,
        *,
        net_income: float,
        payout_ratio: Optional[float],
        cost_of_equity: float,
        terminal_growth: float,
        shares_outstanding: Optional[float],
    ) -> Optional[Dict[str, Any]]:
        if payout_ratio is None or shares_outstanding in (None, 0) or cost_of_equity <= terminal_growth:
            return None
        next_dividend = net_income * payout_ratio * (1 + terminal_growth)
        equity_value = next_dividend / (cost_of_equity - terminal_growth)
        return {
            "method": "bank_ddm_cross_check",
            "payout_ratio": payout_ratio,
            "next_dividend": next_dividend,
            "equity_value": equity_value,
            "intrinsic_value_per_share": equity_value / shares_outstanding,
        }

    def _distribution_cash_flow_base(
        self,
        *,
        facts: Dict[str, Any],
        overrides: Dict[str, Any],
        model_profile: str,
        research_mode: bool,
    ) -> Tuple[Optional[float], str, List[str]]:
        warnings: List[str] = []
        if model_profile == "reit_ffo_affo_ddm.v1":
            affo = self._safe_float(facts.get("affo") or overrides.get("affo"))
            if affo is not None:
                return affo, "affo_distribution", warnings
            ffo = self._safe_float(facts.get("ffo") or overrides.get("ffo"))
            if ffo is not None:
                warnings.append("reit_affo_missing_using_ffo")
                return ffo, "ffo_distribution", warnings
            rental_income = self._safe_float(facts.get("rental_income") or overrides.get("rental_income"))
            if rental_income is not None and research_mode:
                warnings.append("reit_ffo_affo_missing_using_rental_income_research_mode")
                return rental_income, "rental_income_research_proxy", warnings
            return None, "distribution_missing", warnings

        operating_cf = self._safe_float(facts.get("operating_cf") or overrides.get("operating_cf"))
        maintenance_capex = self._safe_float(
            facts.get("maintenance_capex")
            or overrides.get("maintenance_capex")
            or facts.get("capital_expenditure")
            or overrides.get("capital_expenditure")
        )
        net_debt_change = self._safe_float(facts.get("net_debt_change") or overrides.get("net_debt_change"))
        if operating_cf is not None and maintenance_capex is not None:
            return (
                operating_cf - abs(maintenance_capex) + (net_debt_change or 0.0),
                "fcfe_distribution",
                warnings,
            )
        net_income = self._safe_float(facts.get("net_income") or overrides.get("net_income"))
        if net_income is not None:
            warnings.append("utility_fcfe_missing_using_net_income_distribution")
            return net_income, "net_income_distribution", warnings
        return None, "distribution_missing", warnings

    def _scenario_specs(
        self,
        scenario_set: str,
        revenue_growth: float,
        overrides: Dict[str, Any],
    ) -> List[Tuple[str, float]]:
        specs = [
            ("bear", float(overrides.get("bear_growth_rate", max(revenue_growth - 0.04, -0.05)))),
            ("base", revenue_growth),
            ("bull", float(overrides.get("bull_growth_rate", revenue_growth + 0.04))),
            ("stress", float(overrides.get("stress_growth_rate", min(revenue_growth - 0.08, 0.0)))),
        ]
        if scenario_set == "downside_only":
            return [item for item in specs if item[0] in {"bear", "base", "stress"}]
        return specs

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
        result = {
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
            "workbook": None,
            "research_mode": research_mode,
        }
        result["workbook"] = self._workbook_metadata(overrides, result)
        return result

    def _selector_payload(
        self,
        *,
        recommended: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        selection_policy: str,
        include_comparison: bool,
        model_strategy: str,
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
            "model_strategy": model_strategy,
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
        characteristic_reasons = [
            item["code"] for item in bundle.company_characteristics
            if item.get("severity") in {"warning", "guardrail", "blocker"}
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
                *characteristic_reasons,
            ],
            "rejected_models": [],
            "input_gaps": input_gaps,
            "warnings": list(bundle.warnings),
            "implementation_status": profile.get("implementation_status", "unknown"),
        }

    def _assumption_to_dict(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        item = deepcopy(raw)
        value = self._safe_float(item.get("value"))
        quality_flag = str(item.get("quality_flag") or ("missing" if value is None else "configured"))
        source = item.get("source")
        fallback_sources = tuple(item.get("fallback_sources") or [])
        fallback_used = bool(
            item.get("fallback_used")
            if item.get("fallback_used") is not None
            else source in fallback_sources or quality_flag.endswith("fallback")
        )
        lineage_payload = {
            "assumption_key": item.get("assumption_key"),
            "value": value,
            "source": source,
            "primary_source": item.get("primary_source"),
            "as_of_date": item.get("as_of_date"),
            "quality_flag": quality_flag,
        }
        assumption = DcfAssumptionValue(
            assumption_key=str(item.get("assumption_key") or ""),
            value=value,
            unit=item.get("unit"),
            source=source,
            primary_source=item.get("primary_source"),
            fallback_sources=fallback_sources,
            quality_flag=quality_flag,
            fallback_used=fallback_used,
            as_of_date=item.get("as_of_date"),
            last_updated_at=item.get("last_updated_at"),
            lineage_hash=self._hash_payload(lineage_payload),
        )
        item.update(
            {
                "value": assumption.value,
                "quality_flag": assumption.quality_flag,
                "fallback_used": assumption.fallback_used,
                "as_of_date": assumption.as_of_date,
                "last_updated_at": assumption.last_updated_at,
                "lineage_hash": assumption.lineage_hash,
                "warnings": ["assumption_value_missing"] if assumption.value is None else [],
            }
        )
        return item

    def _assumption_source_to_dict(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        source = DcfAssumptionSource(
            source_profile=str(raw.get("source_profile") or ""),
            assumption_keys=tuple(raw.get("assumption_keys") or []),
            provider=str(raw.get("provider") or ""),
            refresh_supported=bool(raw.get("refresh_supported")),
            timeout_seconds=int(raw.get("timeout_seconds") or 0),
            rate_limit_per_minute=(
                int(raw["rate_limit_per_minute"])
                if raw.get("rate_limit_per_minute") is not None
                else None
            ),
        )
        return {
            "source_profile": source.source_profile,
            "assumption_keys": list(source.assumption_keys),
            "provider": source.provider,
            "refresh_supported": source.refresh_supported,
            "timeout_seconds": source.timeout_seconds,
            "rate_limit_per_minute": source.rate_limit_per_minute,
        }

    def _assumption_diagnostics(
        self,
        assumptions: Dict[str, Dict[str, Any]],
        beta_context: Dict[str, Any],
    ) -> Tuple[List[str], List[str]]:
        blockers: List[str] = []
        warnings: List[str] = []
        required_keys = ("risk_free_rate", "equity_risk_premium", "cost_of_debt")
        for key in required_keys:
            item = assumptions.get(key) or {}
            assumption_key = str(item.get("assumption_key") or key)
            if self._safe_float(item.get("value")) is None:
                blockers.append(f"assumption_{assumption_key}_missing")
                continue
            if item.get("fallback_used") or str(item.get("quality_flag") or "").endswith("fallback"):
                warnings.append(f"{assumption_key}_fallback_used")
            if item.get("source") == "manual_override" or item.get("quality_flag") == "manual_override":
                warnings.append(f"{assumption_key}_manual_override_used")
        for key in ("fx_usd_cny", "fx_hkd_cny", "industry_beta", "commodity_price"):
            item = assumptions.get(key)
            if not isinstance(item, dict):
                continue
            assumption_key = str(item.get("assumption_key") or key)
            if self._safe_float(item.get("value")) is None:
                warnings.append(f"{assumption_key}_missing")
            elif item.get("fallback_used") or str(item.get("quality_flag") or "").endswith("fallback"):
                warnings.append(f"{assumption_key}_fallback_used")
        if beta_context.get("beta_source") == "configured_default_beta":
            warnings.append("beta_fallback_used")
        return list(dict.fromkeys(blockers)), list(dict.fromkeys(warnings))

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

    def _industry_profile(
        self,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
    ) -> str:
        text = self._company_text(instrument, bundle.financial_facts)
        if any(token in text for token in ("reit", "基础设施公募", "不动产投资信托")):
            return "reit_ffo_affo_ddm.v1"
        if any(token in text for token in ("公用事业", "电力", "水务", "燃气", "高速", "utility", "infrastructure")):
            return "utility_fcfe_or_ddm.v1"
        if any(token in text for token in ("房地产", "地产", "物业开发", "real estate")):
            return "real_estate_nav_dcf.v1"
        if any(token in text for token in ("煤炭", "有色", "钢铁", "石油", "化工", "资源", "cyclical", "commodity")):
            return "cyclical_fcff_midcycle.v1"
        if any(token in text for token in ("软件", "互联网", "云", "平台", "software", "internet")):
            return "asset_light_fcff.v1"
        return "nonfinancial_fcff.v1"

    def _characteristic_profile(self, bundle: DcfInputBundle) -> str:
        codes = {item["code"] for item in bundle.company_characteristics}
        if "holding_company_structure" in codes:
            return "holdco_sotp.v1"
        if "strong_cyclicality" in codes:
            return "cyclical_fcff_midcycle.v1"
        if {"loss_making", "high_r_and_d", "early_growth", "listing_history_short"} & codes:
            return "high_growth_staged_fcff.v1"
        return "nonfinancial_fcff.v1"

    def _detect_company_characteristics(
        self,
        instrument: Dict[str, Any],
        facts: Dict[str, Any],
        *,
        valuation_date: str,
    ) -> List[Dict[str, Any]]:
        characteristics: List[Dict[str, Any]] = []
        text = self._company_text(instrument, facts)
        net_income = self._safe_float(facts.get("net_income"))
        operating_profit = self._safe_float(facts.get("operating_profit"))
        revenue = self._safe_float(facts.get("revenue"))
        rd = self._safe_float(facts.get("research_and_development") or facts.get("rd_expense"))
        debt = self._safe_float(facts.get("total_debt")) or 0.0
        equity = self._safe_float(facts.get("equity")) or 0.0
        leverage = debt / equity if equity > 0 else None

        if bool(instrument.get("is_st")) or "st" in text or "退市" in text or "delist" in text:
            characteristics.append(
                {
                    "code": "st_or_delisting_risk",
                    "severity": "blocker",
                    "evidence": ["instrument_status_or_name"],
                }
            )
        if str(instrument.get("trading_status") or "").lower() in {"suspended_long", "long_suspension"}:
            characteristics.append(
                {"code": "long_suspension", "severity": "blocker", "evidence": ["trading_status"]}
            )
        if net_income is not None and net_income < 0 or operating_profit is not None and operating_profit < 0:
            characteristics.append(
                {"code": "loss_making", "severity": "guardrail", "evidence": ["negative_profit"]}
            )
        if rd is not None and revenue and rd / revenue > 0.15:
            characteristics.append(
                {
                    "code": "high_r_and_d",
                    "severity": "guardrail",
                    "evidence": [f"rd_to_revenue={rd / revenue:.2f}"],
                }
            )
        if self._safe_float(facts.get("revenue_growth")) is not None and float(facts["revenue_growth"]) > 0.30:
            characteristics.append(
                {"code": "early_growth", "severity": "guardrail", "evidence": ["revenue_growth_gt_30pct"]}
            )
        if leverage is not None and leverage > 1.5:
            characteristics.append(
                {
                    "code": "high_leverage",
                    "severity": "warning",
                    "evidence": [f"debt_to_equity={leverage:.2f}"],
                }
            )
        listed_date = self._normalize_date(
            instrument.get("listed_date") or facts.get("listed_date")
        )
        listed_years = self._year_fraction(listed_date, valuation_date)
        if listed_years is not None and listed_years < 3:
            characteristics.append(
                {
                    "code": "listing_history_short",
                    "severity": "guardrail",
                    "evidence": [f"listed_years={listed_years:.1f}"],
                }
            )
        if any(token in text for token in ("控股", "holding", "投资控股")):
            characteristics.append(
                {"code": "holding_company_structure", "severity": "guardrail", "evidence": ["industry_or_name"]}
            )
        if any(token in text for token in ("煤炭", "有色", "钢铁", "石油", "化工", "资源", "cyclical", "commodity")):
            characteristics.append(
                {"code": "strong_cyclicality", "severity": "guardrail", "evidence": ["industry_or_name"]}
            )
        return characteristics

    def _select_cash_flow_model(
        self,
        *,
        instrument: Dict[str, Any],
        facts: Dict[str, Any],
        overrides: Dict[str, Any],
    ) -> Dict[str, Any]:
        explicit = overrides.get("cash_flow_model")
        if explicit in {"fcff", "fcfe"}:
            return {
                "selected_cash_flow_model": explicit,
                "candidate_models": ["fcff", "fcfe"],
                "selection_reasons": ["explicit_cash_flow_model_override"],
                "rejected_models": ["fcfe" if explicit == "fcff" else "fcff"],
                "input_gap_by_model": {},
                "confidence": 1.0,
                "warnings": [],
            }

        debt = self._safe_float(facts.get("total_debt")) or 0.0
        equity = self._safe_float(facts.get("equity")) or 0.0
        leverage = debt / equity if equity > 0 else None
        debt_change = abs(self._safe_float(facts.get("net_debt_change")) or 0.0)
        prior_debt = abs(self._safe_float(facts.get("prior_total_debt")) or debt)
        debt_change_ratio = debt_change / prior_debt if prior_debt else None
        interest_expense = abs(self._safe_float(facts.get("interest_expense")) or 0.0)
        operating_profit = self._safe_float(facts.get("operating_profit"))
        interest_coverage = (
            operating_profit / interest_expense
            if operating_profit is not None and interest_expense > 0
            else None
        )
        has_dividend_policy = self._safe_float(facts.get("dividend_payout_ratio")) is not None
        capex_ready = self._safe_float(facts.get("capital_expenditure")) is not None
        nwc_ready = self._safe_float(facts.get("change_in_working_capital")) is not None
        regulated_profile = self._industry_profile(instrument, DcfInputBundle(
            instrument=instrument,
            financial_facts=facts,
            assumptions={},
            valuation_date="",
            latest_close=None,
            beta_context={},
            data_available_cutoff=None,
            input_hash="",
            blockers=(),
            warnings=(),
            lineage={},
            company_characteristics=(),
        )) in {"utility_fcfe_or_ddm.v1", "reit_ffo_affo_ddm.v1"}

        warnings = []
        input_gap_by_model = {
            "fcff": [
                field for field in ("capital_expenditure", "change_in_working_capital")
                if self._safe_float(facts.get(field)) is None
            ],
            "fcfe": [
                field for field in ("operating_cf", "net_debt_change")
                if self._safe_float(facts.get(field)) is None
            ],
        }
        if leverage is not None and leverage > 1.5:
            warnings.append("high_leverage_fcfe_not_default")
        if interest_coverage is not None and interest_coverage < 3:
            warnings.append("weak_interest_coverage")
        stable_leverage = leverage is not None and leverage < 0.5
        stable_debt = debt_change_ratio is None or debt_change_ratio < 0.2
        sufficient_coverage = interest_coverage is None or interest_coverage >= 4
        fcfe_evidence = (
            stable_leverage
            and stable_debt
            and sufficient_coverage
            and has_dividend_policy
            and not input_gap_by_model["fcfe"]
            and (capex_ready or regulated_profile)
        )
        if fcfe_evidence:
            selected = "fcfe"
            reasons = [
                "stable_low_leverage",
                "stable_debt_change",
                "dividend_policy_available",
                "fcfe_inputs_available",
            ]
            if regulated_profile:
                reasons.append("regulated_or_distribution_profile")
            confidence = 0.82
        else:
            selected = "fcff"
            reasons = ["default_nonfinancial_enterprise_value_model"]
            if not stable_leverage:
                reasons.append("leverage_not_stably_low")
            if not stable_debt:
                reasons.append("debt_change_not_stable")
            if not has_dividend_policy:
                reasons.append("dividend_policy_missing")
            confidence = 0.72
        return {
            "selected_cash_flow_model": selected,
            "candidate_models": ["fcff", "fcfe"],
            "selection_reasons": reasons,
            "rejected_models": ["fcfe"] if selected == "fcff" else ["fcff"],
            "input_gap_by_model": input_gap_by_model,
            "confidence": confidence,
            "warnings": warnings,
        }

    def _forced_model_warnings(
        self,
        model_profile: str,
        instrument: Dict[str, Any],
        bundle: DcfInputBundle,
    ) -> List[str]:
        if model_profile != "nonfinancial_fcff.v1":
            return []
        if self._hard_model_constraint(instrument, bundle):
            return ["forced_model_warning", "financial_sector_mismatch"]
        return []

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

    def _model_comparison(
        self,
        result: Dict[str, Any],
        selector: Dict[str, Any],
    ) -> Dict[str, Any]:
        candidates = selector.get("candidates", [])
        industry_candidate = next(
            (candidate for candidate in candidates if candidate.get("candidate_type") == "industry"),
            None,
        )
        characteristic_candidate = next(
            (
                candidate
                for candidate in candidates
                if candidate.get("candidate_type") == "company_characteristic"
            ),
            None,
        )
        industry_result = self._comparison_result_for_candidate(industry_candidate, result)
        characteristic_result = self._comparison_result_for_candidate(characteristic_candidate, result)
        unavailable = [
            item
            for item in (industry_result, characteristic_result)
            if item and item.get("status") not in {"success", "partial"}
        ]
        return {
            "recommended": selector.get("recommended_model"),
            "candidates": selector.get("candidates", []),
            "industry_model_result": industry_result,
            "company_characteristic_model_result": characteristic_result,
            "industry_model": industry_result,
            "company_characteristic_model": characteristic_result,
            "unavailable_models": unavailable,
            "comparison_summary": {
                "selection_policy": selector.get("selection_policy"),
                "score_gap": selector.get("score_gap"),
            },
        }

    def _comparison_result_for_candidate(
        self,
        candidate: Optional[Dict[str, Any]],
        result: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not candidate:
            return None
        model_profile = candidate.get("model_profile")
        if model_profile == result.get("model_profile") and result.get("status") in {"success", "partial"}:
            return self._compact_comparison_result(result)
        return {
            "model_profile": model_profile,
            "candidate_type": candidate.get("candidate_type"),
            "status": "unavailable",
            "missing_reason": "model_profile_not_implemented",
            "readiness": {
                "level": "unavailable",
                "ready_for_production": False,
                "blockers": ["model_profile_not_implemented", *candidate.get("input_gaps", [])],
                "warnings": candidate.get("warnings", []),
            },
            "input_gaps": candidate.get("input_gaps", []),
            "score": candidate.get("score"),
            "selection_reasons": candidate.get("selection_reasons", []),
            "enterprise_value": None,
            "equity_value": None,
            "intrinsic_value_per_share": None,
        }

    @staticmethod
    def _compact_comparison_result(result: Dict[str, Any]) -> Dict[str, Any]:
        base_intrinsic = None
        for scenario in result.get("scenarios") or []:
            if scenario.get("scenario") == "base":
                base_intrinsic = scenario.get("intrinsic_value_per_share")
                break
        return {
            "model_profile": result.get("model_profile"),
            "status": result.get("status"),
            "missing_reason": result.get("missing_reason"),
            "calc_method": result.get("calc_method"),
            "calc_version": result.get("calc_version"),
            "base_cash_flow": result.get("base_cash_flow"),
            "base_cash_flow_source": result.get("base_cash_flow_source"),
            "enterprise_value": result.get("enterprise_value"),
            "equity_value": result.get("equity_value"),
            "implied_pb": result.get("implied_pb"),
            "normalized_roe": result.get("normalized_roe"),
            "excess_capital": result.get("excess_capital"),
            "intrinsic_value_per_share": base_intrinsic,
            "readiness": result.get("readiness"),
            "warnings": result.get("warnings", []),
            "diagnostics": result.get("diagnostics"),
            "financial_model_diagnostics": result.get("financial_model_diagnostics"),
            "broker_model_diagnostics": result.get("broker_model_diagnostics"),
        }

    def _workbook_metadata(
        self,
        overrides: Dict[str, Any],
        result: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not overrides.get("include_workbook"):
            return None
        workbook_cfg = self.parameters.get("workbook", {})
        style = str(overrides.get("workbook_style") or workbook_cfg.get("default_style", "consulting_clean"))
        artifact_dir = str(workbook_cfg.get("artifact_dir", "data/reports/dcf_workbooks"))
        ttl_hours = int(workbook_cfg.get("artifact_ttl_hours", 24))
        try:
            from .dcf_workbook import DcfWorkbookBuilder

            artifact = DcfWorkbookBuilder(
                artifact_dir=artifact_dir,
                style=style,
                ttl_hours=ttl_hours,
            ).build(result)
        except Exception as exc:
            return {
                "workbook_available": False,
                "workbook_artifact_id": None,
                "style": style,
                "warnings": [f"workbook_generation_failed:{exc.__class__.__name__}"],
            }
        return {
            "workbook_available": True,
            "workbook_artifact_id": artifact.artifact_id,
            "download_path": f"/api/v1/research/valuation/dcf/workbooks/{artifact.artifact_id}",
            "generated_at": artifact.generated_at,
            "expires_at": artifact.expires_at,
            "style": artifact.style,
            "sheets": list(artifact.sheets),
            "warnings": list(artifact.warnings),
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
            "net_debt_change": "official_cash_flow_statement_or_debt_schedule",
            "interest_expense": "official_income_statement",
            "capital_adequacy_ratio": "bank_capital_disclosure",
            "embedded_value": "insurer_annual_report",
            "new_business_value": "insurer_annual_report",
            "net_capital": "broker_annual_report",
            "brokerage_revenue": "broker_segment_or_income_statement",
            "investment_income": "broker_income_statement",
            "market_turnover": "exchange_market_statistics",
            "index_level": "exchange_or_index_provider",
            "leverage_ratio": "broker_annual_report",
            "commodity_price_assumption": "commodity_exchange_or_industry_dataset",
            "rental_income": "reit_report_or_property_dataset",
            "ffo": "reit_report_or_adjusted_cash_flow_statement",
            "segment_assets": "annual_report_segment_note",
        }
        return mapping.get(field_name, "official_financial_statement")

    @staticmethod
    def _company_text(instrument: Dict[str, Any], facts: Dict[str, Any]) -> str:
        return " ".join(
            str(value or "")
            for value in (
                instrument.get("name"),
                instrument.get("short_name"),
                instrument.get("company_name"),
                instrument.get("industry"),
                instrument.get("industry_name"),
                instrument.get("sw_l1_name"),
                instrument.get("sw_l2_name"),
                instrument.get("type"),
                instrument.get("status"),
                facts.get("profile"),
                facts.get("business_profile"),
            )
        ).lower()

    @staticmethod
    def _year_fraction(start_date: Optional[str], end_date: Optional[str]) -> Optional[float]:
        if not start_date or not end_date:
            return None
        try:
            start = date.fromisoformat(start_date[:10])
            end = date.fromisoformat(end_date[:10])
        except ValueError:
            return None
        if end < start:
            return None
        return (end - start).days / 365.25

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
