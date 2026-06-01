"""
Benchmark-aware on-demand beta calculation helpers for research APIs.
"""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from typing import Any, Dict, List, Optional

import pandas as pd

from .providers.base import BetaResult


DEFAULT_BETA_PARAMETERS: Dict[str, Any] = {
    "windows": [60, 120, 252],
    "min_observation_ratio": 0.8,
    "min_observations_floor": 40,
    "stock_adjustment": "qfq",
    "benchmark_adjustment": "none",
    "annualization_days": 252,
}


class ResearchBetaService:
    """Build benchmark-aware beta results from local return series."""

    calc_method = "beta_ols_daily_return"
    calc_version = "beta_on_demand.v1"

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        merged = deepcopy(DEFAULT_BETA_PARAMETERS)
        self._deep_update(merged, parameters or {})
        self.parameters = merged

    def build_results(
        self,
        *,
        stock_quotes: pd.DataFrame,
        benchmark_quotes: Optional[pd.DataFrame],
        instrument: Dict[str, Any],
        benchmark: Dict[str, Any],
        windows: Optional[List[int]] = None,
        stock_adjustment: Optional[str] = None,
        benchmark_adjustment: Optional[str] = None,
    ) -> List[BetaResult]:
        """Build beta results for all requested windows."""
        target_windows = windows or list(self.parameters.get("windows", [60, 120, 252]))
        return [
            self.build_result(
                stock_quotes=stock_quotes,
                benchmark_quotes=benchmark_quotes,
                instrument=instrument,
                benchmark=benchmark,
                window_days=int(window),
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
            )
            for window in target_windows
        ]

    def build_result(
        self,
        *,
        stock_quotes: pd.DataFrame,
        benchmark_quotes: Optional[pd.DataFrame],
        instrument: Dict[str, Any],
        benchmark: Dict[str, Any],
        window_days: int,
        stock_adjustment: Optional[str] = None,
        benchmark_adjustment: Optional[str] = None,
    ) -> BetaResult:
        """Build one beta result for one benchmark/window."""
        stock_adjustment = stock_adjustment or str(
            self.parameters.get("stock_adjustment", "qfq")
        )
        benchmark_adjustment = benchmark_adjustment or str(
            self.parameters.get("benchmark_adjustment", "none")
        )
        parameter_hash = self._build_parameter_hash(
            {
                "window_days": int(window_days),
                "stock_adjustment": stock_adjustment,
                "benchmark_adjustment": benchmark_adjustment,
                "calc_method": self.calc_method,
                "calc_version": self.calc_version,
                "min_observation_ratio": self.parameters.get("min_observation_ratio"),
                "min_observations_floor": self.parameters.get("min_observations_floor"),
            }
        )
        min_observations = self._min_observations(window_days)

        if stock_quotes is None or stock_quotes.empty:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="stock_quotes_not_available",
                details={
                    "stock_quote_rows": 0,
                    "benchmark_quote_rows": (
                        0 if benchmark_quotes is None else int(len(benchmark_quotes))
                    ),
                },
            )
        if benchmark_quotes is None or benchmark_quotes.empty:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="benchmark_quotes_not_available",
                details={
                    "stock_quote_rows": int(len(stock_quotes)),
                    "benchmark_quote_rows": 0,
                },
            )

        stock = self._prepare_return_frame(stock_quotes, "stock")
        benchmark_frame = self._prepare_return_frame(benchmark_quotes, "benchmark")
        if stock.empty:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="stock_returns_not_available",
                details={
                    "stock_quote_rows": int(len(stock_quotes)),
                    "benchmark_quote_rows": int(len(benchmark_quotes)),
                },
            )
        if benchmark_frame.empty:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="benchmark_returns_not_available",
                details={
                    "stock_quote_rows": int(len(stock_quotes)),
                    "benchmark_quote_rows": int(len(benchmark_quotes)),
                    "stock_return_count": int(len(stock)),
                },
            )

        merged = stock.merge(benchmark_frame, on="time", how="inner").dropna()
        if len(merged) < min_observations:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="insufficient_aligned_observations",
                observation_count=int(len(merged)),
                details={
                    "stock_quote_rows": int(len(stock_quotes)),
                    "benchmark_quote_rows": int(len(benchmark_quotes)),
                    "stock_return_count": int(len(stock)),
                    "benchmark_return_count": int(len(benchmark_frame)),
                    "aligned_observation_count": int(len(merged)),
                },
            )

        sample = merged.tail(window_days)
        if len(sample) < min_observations:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="insufficient_window_observations",
                observation_count=int(len(sample)),
            )

        benchmark_var = sample["ret_benchmark"].var(ddof=1)
        if benchmark_var is None or pd.isna(benchmark_var) or benchmark_var == 0:
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="benchmark_return_variance_not_available",
                observation_count=int(len(sample)),
            )

        covariance = sample["ret_stock"].cov(sample["ret_benchmark"])
        if covariance is None or pd.isna(covariance):
            return self._unavailable(
                instrument=instrument,
                benchmark=benchmark,
                window_days=window_days,
                min_observations=min_observations,
                stock_adjustment=stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
                parameter_hash=parameter_hash,
                missing_reason="covariance_not_available",
                observation_count=int(len(sample)),
            )

        beta = float(covariance / benchmark_var)
        alpha = float(sample["ret_stock"].mean() - beta * sample["ret_benchmark"].mean())
        correlation = sample["ret_stock"].corr(sample["ret_benchmark"])
        correlation_value = None if pd.isna(correlation) else float(correlation)
        r_squared = None if correlation_value is None else float(correlation_value ** 2)
        annualization_days = int(self.parameters.get("annualization_days", 252))
        stock_volatility = float(sample["ret_stock"].std(ddof=1) * math.sqrt(annualization_days))
        benchmark_volatility = float(
            sample["ret_benchmark"].std(ddof=1) * math.sqrt(annualization_days)
        )
        fitted = alpha + beta * sample["ret_benchmark"]
        residual = sample["ret_stock"] - fitted
        active_return = sample["ret_stock"] - sample["ret_benchmark"]
        residual_volatility = float(residual.std(ddof=1) * math.sqrt(annualization_days))
        tracking_error = float(active_return.std(ddof=1) * math.sqrt(annualization_days))
        standard_error_beta = self._standard_error_beta(
            residual=residual,
            benchmark_return=sample["ret_benchmark"],
        )
        t_stat_beta = (
            None
            if standard_error_beta is None or standard_error_beta == 0
            else float(beta / standard_error_beta)
        )
        p_value_beta = (
            None
            if t_stat_beta is None
            else self._normal_two_tailed_p_value(t_stat_beta)
        )
        quality_flag, interpretation_flags = self._quality_assessment(
            r_squared=r_squared,
            p_value_beta=p_value_beta,
            observation_count=int(len(sample)),
            min_observations=min_observations,
            tracking_error=tracking_error,
            stock_volatility=stock_volatility,
        )
        as_of_date = pd.to_datetime(sample["time"].max()).date().isoformat()
        window_start = pd.to_datetime(sample["time"].min()).date().isoformat()

        return BetaResult(
            instrument_id=str(instrument.get("instrument_id", "")),
            symbol=str(instrument.get("symbol", "")),
            exchange=str(instrument.get("exchange", "")),
            as_of_date=as_of_date,
            benchmark_family=str(benchmark.get("benchmark_family", "custom")),
            benchmark_instrument_id=str(benchmark.get("benchmark_instrument_id", "")),
            benchmark_name=benchmark.get("benchmark_name"),
            window_days=int(window_days),
            status="success",
            missing_reason=None,
            beta=beta,
            alpha=alpha,
            correlation=correlation_value,
            r_squared=r_squared,
            stock_volatility=stock_volatility,
            benchmark_volatility=benchmark_volatility,
            residual_volatility=residual_volatility,
            tracking_error=tracking_error,
            standard_error_beta=standard_error_beta,
            t_stat_beta=t_stat_beta,
            p_value_beta=p_value_beta,
            quality_flag=quality_flag,
            interpretation_flags=interpretation_flags,
            observation_count=int(len(sample)),
            min_observation_count=min_observations,
            window_start=window_start,
            window_end=as_of_date,
            stock_adjustment=stock_adjustment,
            benchmark_adjustment=benchmark_adjustment,
            calc_method=self.calc_method,
            calc_version=self.calc_version,
            parameter_hash=parameter_hash,
            details_json={
                "stock_quote_rows": int(len(stock_quotes)),
                "benchmark_quote_rows": int(len(benchmark_quotes)),
                "stock_return_count": int(len(stock)),
                "benchmark_return_count": int(len(benchmark_frame)),
                "aligned_observation_count": int(len(merged)),
                "benchmark_selection_rule": benchmark.get("selection_rule"),
                "p_value_method": "normal_approximation",
            },
        )

    def _min_observations(self, window_days: int) -> int:
        ratio = float(self.parameters.get("min_observation_ratio", 0.8))
        floor = int(self.parameters.get("min_observations_floor", 40))
        return min(int(window_days), max(int(math.ceil(window_days * ratio)), floor))

    @staticmethod
    def _prepare_return_frame(quotes: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if quotes is None or quotes.empty:
            return pd.DataFrame(columns=["time", f"ret_{suffix}"])
        frame = quotes.copy()
        if "time" not in frame.columns:
            return pd.DataFrame(columns=["time", f"ret_{suffix}"])
        price_column = "close"
        if price_column not in frame.columns and "close_index" in frame.columns:
            price_column = "close_index"
        if price_column not in frame.columns:
            return pd.DataFrame(columns=["time", f"ret_{suffix}"])
        frame["time"] = pd.to_datetime(frame["time"])
        frame[price_column] = pd.to_numeric(frame[price_column], errors="coerce")
        frame = frame.dropna(subset=["time", price_column]).sort_values("time")
        frame[f"ret_{suffix}"] = frame[price_column].pct_change()
        return frame[["time", f"ret_{suffix}"]].dropna()

    @staticmethod
    def _standard_error_beta(
        *,
        residual: pd.Series,
        benchmark_return: pd.Series,
    ) -> Optional[float]:
        n = len(residual)
        if n <= 2:
            return None
        x = pd.to_numeric(benchmark_return, errors="coerce")
        e = pd.to_numeric(residual, errors="coerce")
        sxx = float(((x - x.mean()) ** 2).sum())
        if sxx <= 0:
            return None
        rss = float((e ** 2).sum())
        sigma2 = rss / (n - 2)
        if sigma2 < 0:
            return None
        return float(math.sqrt(sigma2 / sxx))

    @staticmethod
    def _normal_two_tailed_p_value(t_stat: float) -> float:
        return float(math.erfc(abs(float(t_stat)) / math.sqrt(2.0)))

    @staticmethod
    def _quality_assessment(
        *,
        r_squared: Optional[float],
        p_value_beta: Optional[float],
        observation_count: int,
        min_observations: int,
        tracking_error: Optional[float],
        stock_volatility: Optional[float],
    ) -> tuple[str, List[str]]:
        flags: List[str] = []
        r2 = 0.0 if r_squared is None else float(r_squared)
        p_value = 1.0 if p_value_beta is None else float(p_value_beta)

        if r2 < 0.2:
            flags.append("low_explanatory_power")
        elif r2 < 0.4:
            flags.append("moderate_explanatory_power")
        if p_value > 0.05:
            flags.append("beta_not_statistically_significant")
        if observation_count <= min_observations:
            flags.append("near_minimum_observation_count")
        if (
            tracking_error is not None
            and stock_volatility is not None
            and stock_volatility > 0
            and tracking_error / stock_volatility >= 0.75
        ):
            flags.append("active_risk_dominates")

        if r2 >= 0.5 and p_value <= 0.05:
            quality = "high"
        elif r2 >= 0.25 and p_value <= 0.10:
            quality = "medium"
        else:
            quality = "low"
        return quality, flags

    def _unavailable(
        self,
        *,
        instrument: Dict[str, Any],
        benchmark: Dict[str, Any],
        window_days: int,
        min_observations: int,
        stock_adjustment: str,
        benchmark_adjustment: str,
        parameter_hash: str,
        missing_reason: str,
        observation_count: int = 0,
        details: Optional[Dict[str, Any]] = None,
    ) -> BetaResult:
        return BetaResult(
            instrument_id=str(instrument.get("instrument_id", "")),
            symbol=str(instrument.get("symbol", "")),
            exchange=str(instrument.get("exchange", "")),
            as_of_date=str(benchmark.get("as_of_date") or ""),
            benchmark_family=str(benchmark.get("benchmark_family", "custom")),
            benchmark_instrument_id=str(benchmark.get("benchmark_instrument_id", "")),
            benchmark_name=benchmark.get("benchmark_name"),
            window_days=int(window_days),
            status="unavailable",
            missing_reason=missing_reason,
            observation_count=int(observation_count),
            min_observation_count=int(min_observations),
            quality_flag="unavailable",
            interpretation_flags=[missing_reason],
            stock_adjustment=stock_adjustment,
            benchmark_adjustment=benchmark_adjustment,
            calc_method=self.calc_method,
            calc_version=self.calc_version,
            parameter_hash=parameter_hash,
            details_json={
                **(details or {}),
                "benchmark_selection_rule": benchmark.get("selection_rule"),
            },
        )

    @staticmethod
    def _build_parameter_hash(payload: Dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]

    @staticmethod
    def _deep_update(target: Dict[str, Any], overrides: Dict[str, Any]) -> None:
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                ResearchBetaService._deep_update(target[key], value)
            else:
                target[key] = deepcopy(value)
