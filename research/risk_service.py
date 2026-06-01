"""
Risk snapshot helpers for research APIs.
"""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from typing import Any, Dict, Optional

import pandas as pd

from .providers.base import RiskSnapshot


DEFAULT_RISK_PARAMETERS: Dict[str, Any] = {
    "benchmark_instrument_id": "000300.SH",
    "volatility_window_short": 20,
    "volatility_window_long": 60,
    "beta_window": 60,
    "drawdown_window": 252,
    "liquidity_window": 20,
    "event_window_days": 30,
    "risk_level_thresholds": {
        "medium": 40.0,
        "high": 70.0,
    },
}


class ResearchRiskService:
    """Build derived risk snapshots from local quotes and research facts."""

    calc_method = "risk_snapshot_builtin"
    calc_version = "risk_snapshot.v1"

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        merged = deepcopy(DEFAULT_RISK_PARAMETERS)
        self._deep_update(merged, parameters or {})
        self.parameters = merged

    def build_snapshot(
        self,
        quotes: pd.DataFrame,
        instrument: Dict[str, Any],
        financial_bundle: Optional[Dict[str, Any]] = None,
        *,
        benchmark_quotes: Optional[pd.DataFrame] = None,
        negative_event_count_30d: int = 0,
    ) -> Optional[RiskSnapshot]:
        if quotes is None or quotes.empty:
            return None

        ordered = quotes.copy()
        ordered["time"] = pd.to_datetime(ordered["time"])
        ordered = ordered.sort_values("time").reset_index(drop=True)
        ordered["close"] = pd.to_numeric(ordered["close"], errors="coerce")
        ordered = ordered.dropna(subset=["close"])
        if ordered.empty:
            return None

        returns = ordered["close"].pct_change()
        as_of_date = ordered.iloc[-1]["time"].date().isoformat()
        vol_short = self._annualized_volatility(
            returns,
            int(self.parameters["volatility_window_short"]),
        )
        vol_long = self._annualized_volatility(
            returns,
            int(self.parameters["volatility_window_long"]),
        )
        max_drawdown = self._max_drawdown(
            ordered["close"],
            int(self.parameters["drawdown_window"]),
        )
        avg_turnover = self._rolling_mean(
            ordered.get("turnover"),
            int(self.parameters["liquidity_window"]),
        )
        avg_amount = self._rolling_mean(
            ordered.get("amount"),
            int(self.parameters["liquidity_window"]),
        )
        beta = self._beta(
            ordered,
            benchmark_quotes,
            int(self.parameters["beta_window"]),
        )

        facts = financial_bundle or {}
        total_assets = self._to_positive_float(facts.get("total_assets"))
        total_liabilities = self._to_positive_float(facts.get("total_liabilities"))
        current_assets = self._to_positive_float(facts.get("current_assets"))
        current_liabilities = self._to_positive_float(facts.get("current_liabilities"))
        operating_cf = self._to_float(facts.get("operating_cf"))
        net_income = self._to_float(facts.get("net_income"))

        liability_to_asset = self._safe_ratio(total_liabilities, total_assets)
        current_ratio = self._safe_ratio(current_assets, current_liabilities)
        operating_cf_to_net_income = self._safe_ratio(operating_cf, net_income)

        component_scores = {
            "volatility": self._volatility_score(vol_short),
            "drawdown": self._drawdown_score(max_drawdown),
            "leverage": self._leverage_score(liability_to_asset),
            "beta": self._beta_score(beta),
            "cashflow": self._cashflow_score(operating_cf_to_net_income),
            "liquidity": self._liquidity_score(avg_amount),
            "events": self._event_score(negative_event_count_30d),
        }
        risk_score = round(sum(component_scores.values()), 2)
        risk_level = self._classify_risk_level(risk_score)
        parameter_hash = self._build_parameter_hash(self.parameters)

        return RiskSnapshot(
            instrument_id=instrument.get("instrument_id", ""),
            symbol=instrument.get("symbol", ""),
            exchange=instrument.get("exchange", ""),
            as_of_date=as_of_date,
            benchmark_instrument_id=self.parameters.get("benchmark_instrument_id"),
            volatility_20d=vol_short,
            volatility_60d=vol_long,
            beta_60d=beta,
            max_drawdown_252d=max_drawdown,
            average_turnover_20d=avg_turnover,
            average_amount_20d=avg_amount,
            liability_to_asset=liability_to_asset,
            current_ratio=current_ratio,
            operating_cf_to_net_income=operating_cf_to_net_income,
            negative_event_count_30d=int(negative_event_count_30d),
            risk_score=risk_score,
            risk_level=risk_level,
            calc_method=self.calc_method,
            calc_version=self.calc_version,
            parameter_hash=parameter_hash,
            details_json={
                "component_scores": component_scores,
                "observation_count": int(len(ordered)),
                "windows": {
                    "volatility_short": int(self.parameters["volatility_window_short"]),
                    "volatility_long": int(self.parameters["volatility_window_long"]),
                    "beta": int(self.parameters["beta_window"]),
                    "drawdown": int(self.parameters["drawdown_window"]),
                    "liquidity": int(self.parameters["liquidity_window"]),
                    "events": int(self.parameters["event_window_days"]),
                },
                "financial_source_report_period": facts.get("report_period"),
                "beta_source": "calculated_inline",
            },
        )

    def build_response(
        self,
        snapshot: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if snapshot is None:
            return None

        response = dict(snapshot)
        response["status"] = "success"
        response["missing_reason"] = None
        return response

    def _build_parameter_hash(self, payload: Dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]

    @staticmethod
    def _annualized_volatility(returns: pd.Series, window: int) -> Optional[float]:
        if returns is None:
            return None
        series = pd.to_numeric(returns, errors="coerce").dropna()
        if len(series) < window:
            return None
        sample = series.tail(window)
        return float(sample.std(ddof=1) * math.sqrt(252))

    @staticmethod
    def _max_drawdown(close: pd.Series, window: int) -> Optional[float]:
        if close is None:
            return None
        series = pd.to_numeric(close, errors="coerce").dropna()
        if len(series) < 2:
            return None
        sample = series.tail(window)
        rolling_peak = sample.cummax()
        drawdown = sample / rolling_peak - 1.0
        return float(drawdown.min())

    @staticmethod
    def _rolling_mean(series: Any, window: int) -> Optional[float]:
        if series is None:
            return None
        sample = pd.to_numeric(series, errors="coerce").dropna()
        if len(sample) < window:
            return None
        return float(sample.tail(window).mean())

    @staticmethod
    def _beta(
        quotes: pd.DataFrame,
        benchmark_quotes: Optional[pd.DataFrame],
        window: int,
    ) -> Optional[float]:
        if benchmark_quotes is None or benchmark_quotes.empty:
            return None

        stock = quotes[["time", "close"]].copy()
        benchmark = benchmark_quotes[["time", "close"]].copy()
        stock["time"] = pd.to_datetime(stock["time"])
        benchmark["time"] = pd.to_datetime(benchmark["time"])
        stock["ret"] = pd.to_numeric(stock["close"], errors="coerce").pct_change()
        benchmark["ret"] = pd.to_numeric(benchmark["close"], errors="coerce").pct_change()
        merged = stock.merge(
            benchmark[["time", "ret"]],
            on="time",
            how="inner",
            suffixes=("_stock", "_benchmark"),
        ).dropna()
        if len(merged) < window:
            return None

        sample = merged.tail(window)
        benchmark_var = sample["ret_benchmark"].var(ddof=1)
        if benchmark_var in {None, 0} or pd.isna(benchmark_var):
            return None
        covariance = sample["ret_stock"].cov(sample["ret_benchmark"])
        if covariance is None or pd.isna(covariance):
            return None
        return float(covariance / benchmark_var)

    def _classify_risk_level(self, risk_score: float) -> str:
        thresholds = self.parameters["risk_level_thresholds"]
        if risk_score >= float(thresholds["high"]):
            return "high"
        if risk_score >= float(thresholds["medium"]):
            return "medium"
        return "low"

    @staticmethod
    def _volatility_score(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        return min(value / 0.60, 1.0) * 25.0

    @staticmethod
    def _drawdown_score(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        return min(abs(value) / 0.50, 1.0) * 20.0

    @staticmethod
    def _leverage_score(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        return min(value / 0.80, 1.0) * 20.0

    @staticmethod
    def _beta_score(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        return min(abs(value - 1.0) / 1.5, 1.0) * 10.0

    @staticmethod
    def _cashflow_score(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        if value >= 0.8:
            return 0.0
        return min((0.8 - value) / 0.8, 1.0) * 10.0

    @staticmethod
    def _liquidity_score(value: Optional[float]) -> float:
        if value is None:
            return 0.0
        if value < 5e7:
            return 5.0
        if value < 2e8:
            return 2.5
        return 0.0

    @staticmethod
    def _event_score(count: int) -> float:
        return min(max(count, 0) / 5.0, 1.0) * 10.0

    @staticmethod
    def _safe_ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
        if numerator is None or denominator in {None, 0}:
            return None
        return float(numerator / denominator)

    @staticmethod
    def _to_positive_float(value: Any) -> Optional[float]:
        parsed = ResearchRiskService._to_float(value)
        if parsed is None or parsed <= 0:
            return None
        return parsed

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        try:
            return None if value is None else float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                ResearchRiskService._deep_update(target[key], value)
            else:
                target[key] = deepcopy(value)
