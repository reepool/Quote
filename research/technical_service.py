"""
Technical analysis helpers for research APIs.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


DEFAULT_TECHNICAL_PARAMETERS: Dict[str, Any] = {
    "lookback_bars": 180,
    "min_bars": 60,
    "sma_windows": {
        "short": 20,
        "long": 60,
    },
    "ema_windows": {
        "fast": 12,
        "slow": 26,
        "signal": 9,
    },
    "rsi_window": 14,
    "adx_window": 14,
    "stoch_window": 14,
    "stoch_smooth_k": 3,
    "stoch_smooth_d": 3,
    "cci_window": 20,
    "williams_r_window": 14,
    "boll_window": 20,
    "boll_stddev": 2.0,
    "atr_window": 14,
    "volume_ratio_window": 20,
    "signal_thresholds": {
        "bullish": 0.25,
        "bearish": -0.25,
    },
}


class ResearchTechnicalAnalysisService:
    """Compute technical summary from local quote history."""

    calc_method = "ta_builtin"
    calc_version = "technical_summary.v1"

    def __init__(self, parameters: Optional[Dict[str, Any]] = None):
        self.parameters = self._merge_parameters(parameters or {})

    def build_summary(
        self,
        quotes: pd.DataFrame,
        instrument: Dict[str, Any],
        *,
        requested_adjustment: str,
        applied_adjustment: str,
    ) -> Optional[Dict[str, Any]]:
        indicators = self._prepare_indicator_frame(quotes)
        if indicators is None:
            return None

        latest = indicators.iloc[-1]
        minimum_required = int(self.parameters["min_bars"])

        key_fields = [
            "sma_long",
            "macd_signal",
            "rsi",
            "boll_middle",
            "atr",
            "volume_ratio",
        ]
        has_key_indicators = len(indicators) >= minimum_required and all(
            pd.notna(latest.get(field))
            for field in key_fields
        )

        trend_score = self._calculate_trend_score(latest) if has_key_indicators else None
        signal = self._classify_signal(trend_score) if trend_score is not None else "insufficient_data"

        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "data_as_of": latest["time"].isoformat(),
            "calc_method": self.calc_method,
            "calc_version": self.calc_version,
            "parameter_hash": self._build_parameter_hash(),
            "status": "complete" if has_key_indicators else "insufficient_data",
            "missing_reason": None if has_key_indicators else "not_enough_quote_history",
            "signal": signal,
            "trend_score": self._clean_numeric(trend_score),
            "close": self._clean_numeric(latest.get("close")),
            "pct_change_1d": self._compute_period_return(indicators["close"], 1),
            "pct_change_20d": self._compute_period_return(indicators["close"], 20),
            "sma20": self._clean_numeric(latest.get("sma_short")),
            "sma60": self._clean_numeric(latest.get("sma_long")),
            "ema12": self._clean_numeric(latest.get("ema_fast")),
            "ema26": self._clean_numeric(latest.get("ema_slow")),
            "macd": self._clean_numeric(latest.get("macd")),
            "macd_signal": self._clean_numeric(latest.get("macd_signal")),
            "macd_hist": self._clean_numeric(latest.get("macd_hist")),
            "rsi14": self._clean_numeric(latest.get("rsi")),
            "adx": self._clean_numeric(latest.get("adx")),
            "plus_di": self._clean_numeric(latest.get("plus_di")),
            "minus_di": self._clean_numeric(latest.get("minus_di")),
            "stoch_k": self._clean_numeric(latest.get("stoch_k")),
            "stoch_d": self._clean_numeric(latest.get("stoch_d")),
            "cci": self._clean_numeric(latest.get("cci")),
            "williams_r": self._clean_numeric(latest.get("williams_r")),
            "boll_upper": self._clean_numeric(latest.get("boll_upper")),
            "boll_middle": self._clean_numeric(latest.get("boll_middle")),
            "boll_lower": self._clean_numeric(latest.get("boll_lower")),
            "atr14": self._clean_numeric(latest.get("atr")),
            "volume_ratio": self._clean_numeric(latest.get("volume_ratio")),
            "distance_to_sma20": self._compute_distance(
                latest.get("close"),
                latest.get("sma_short"),
            ),
            "distance_to_sma60": self._compute_distance(
                latest.get("close"),
                latest.get("sma_long"),
            ),
            "quote_summary": {
                "quote_source": "quotes_db",
                "data_points": int(len(indicators)),
                "window_start": indicators["time"].iloc[0].isoformat(),
                "window_end": indicators["time"].iloc[-1].isoformat(),
                "requested_adjustment": requested_adjustment,
                "applied_adjustment": applied_adjustment,
                "latest_quality_score": self._clean_numeric(latest.get("quality_score")),
            },
        }

    def build_indicator_series(
        self,
        quotes: pd.DataFrame,
        instrument: Dict[str, Any],
        *,
        requested_adjustment: str,
        applied_adjustment: str,
        limit: int,
    ) -> Optional[Dict[str, Any]]:
        indicators = self._prepare_indicator_frame(quotes)
        if indicators is None:
            return None

        minimum_required = int(self.parameters["min_bars"])
        if limit > 0:
            indicators = indicators.tail(limit).reset_index(drop=True)

        items = []
        for _, row in indicators.iterrows():
            has_complete_row = len(indicators) >= minimum_required and all(
                pd.notna(row.get(field))
                for field in ("sma_long", "macd_signal", "rsi", "boll_middle", "atr")
            )
            trend_score = self._calculate_trend_score(row) if has_complete_row else None
            signal = self._classify_signal(trend_score) if trend_score is not None else "insufficient_data"
            items.append(
                {
                    "time": row["time"].isoformat(),
                    "close": self._clean_numeric(row.get("close")),
                    "sma20": self._clean_numeric(row.get("sma_short")),
                    "sma60": self._clean_numeric(row.get("sma_long")),
                    "ema12": self._clean_numeric(row.get("ema_fast")),
                    "ema26": self._clean_numeric(row.get("ema_slow")),
                    "macd": self._clean_numeric(row.get("macd")),
                    "macd_signal": self._clean_numeric(row.get("macd_signal")),
                    "macd_hist": self._clean_numeric(row.get("macd_hist")),
                    "rsi14": self._clean_numeric(row.get("rsi")),
                    "adx": self._clean_numeric(row.get("adx")),
                    "plus_di": self._clean_numeric(row.get("plus_di")),
                    "minus_di": self._clean_numeric(row.get("minus_di")),
                    "stoch_k": self._clean_numeric(row.get("stoch_k")),
                    "stoch_d": self._clean_numeric(row.get("stoch_d")),
                    "cci": self._clean_numeric(row.get("cci")),
                    "williams_r": self._clean_numeric(row.get("williams_r")),
                    "boll_upper": self._clean_numeric(row.get("boll_upper")),
                    "boll_middle": self._clean_numeric(row.get("boll_middle")),
                    "boll_lower": self._clean_numeric(row.get("boll_lower")),
                    "atr14": self._clean_numeric(row.get("atr")),
                    "volume_ratio": self._clean_numeric(row.get("volume_ratio")),
                    "trend_score": self._clean_numeric(trend_score),
                    "signal": signal,
                }
            )

        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "calc_method": self.calc_method,
            "calc_version": self.calc_version,
            "parameter_hash": self._build_parameter_hash(),
            "requested_adjustment": requested_adjustment,
            "applied_adjustment": applied_adjustment,
            "data_points": len(items),
            "window_start": indicators["time"].iloc[0].isoformat(),
            "window_end": indicators["time"].iloc[-1].isoformat(),
            "items": items,
        }

    def _compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        sma_short_window = int(self.parameters["sma_windows"]["short"])
        sma_long_window = int(self.parameters["sma_windows"]["long"])
        ema_fast_window = int(self.parameters["ema_windows"]["fast"])
        ema_slow_window = int(self.parameters["ema_windows"]["slow"])
        ema_signal_window = int(self.parameters["ema_windows"]["signal"])
        rsi_window = int(self.parameters["rsi_window"])
        adx_window = int(self.parameters["adx_window"])
        stoch_window = int(self.parameters["stoch_window"])
        stoch_smooth_k = int(self.parameters["stoch_smooth_k"])
        stoch_smooth_d = int(self.parameters["stoch_smooth_d"])
        cci_window = int(self.parameters["cci_window"])
        williams_r_window = int(self.parameters["williams_r_window"])
        boll_window = int(self.parameters["boll_window"])
        boll_stddev = float(self.parameters["boll_stddev"])
        atr_window = int(self.parameters["atr_window"])
        volume_ratio_window = int(self.parameters["volume_ratio_window"])

        close = result["close"]

        result["sma_short"] = close.rolling(window=sma_short_window).mean()
        result["sma_long"] = close.rolling(window=sma_long_window).mean()

        result["ema_fast"] = close.ewm(span=ema_fast_window, adjust=False).mean()
        result["ema_slow"] = close.ewm(span=ema_slow_window, adjust=False).mean()
        result["macd"] = result["ema_fast"] - result["ema_slow"]
        result["macd_signal"] = result["macd"].ewm(span=ema_signal_window, adjust=False).mean()
        result["macd_hist"] = result["macd"] - result["macd_signal"]

        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        average_gain = gain.ewm(alpha=1 / rsi_window, adjust=False).mean()
        average_loss = loss.ewm(alpha=1 / rsi_window, adjust=False).mean()
        average_loss_safe = average_loss.replace(0, np.nan)
        rs = average_gain / average_loss_safe
        result["rsi"] = 100 - (100 / (1 + rs))
        result.loc[average_loss == 0, "rsi"] = 100.0

        rolling_mean = close.rolling(window=boll_window).mean()
        rolling_std = close.rolling(window=boll_window).std(ddof=0)
        result["boll_middle"] = rolling_mean
        result["boll_upper"] = rolling_mean + (rolling_std * boll_stddev)
        result["boll_lower"] = rolling_mean - (rolling_std * boll_stddev)

        previous_close = close.shift(1)
        true_range = pd.concat(
            [
                result["high"] - result["low"],
                (result["high"] - previous_close).abs(),
                (result["low"] - previous_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        result["atr"] = true_range.rolling(window=atr_window).mean()

        up_move = result["high"].diff()
        down_move = -result["low"].diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
        atr_wilder = true_range.ewm(alpha=1 / adx_window, adjust=False).mean()
        atr_wilder_safe = atr_wilder.replace(0, np.nan)
        plus_di = (
            100
            * plus_dm.ewm(alpha=1 / adx_window, adjust=False).mean()
            / atr_wilder_safe
        )
        minus_di = (
            100
            * minus_dm.ewm(alpha=1 / adx_window, adjust=False).mean()
            / atr_wilder_safe
        )
        plus_di = pd.to_numeric(plus_di, errors="coerce")
        minus_di = pd.to_numeric(minus_di, errors="coerce")
        dx_denominator = (plus_di + minus_di).replace(0, np.nan)
        dx = (
            100
            * (plus_di - minus_di).abs()
            / dx_denominator
        )
        dx = pd.to_numeric(dx, errors="coerce")
        result["plus_di"] = plus_di
        result["minus_di"] = minus_di
        result["adx"] = dx.ewm(alpha=1 / adx_window, adjust=False).mean()

        low_min = result["low"].rolling(window=stoch_window).min()
        high_max = result["high"].rolling(window=stoch_window).max()
        stoch_range = (high_max - low_min).replace(0, np.nan)
        stoch_k_raw = 100 * (close - low_min) / stoch_range
        stoch_k_raw = pd.to_numeric(stoch_k_raw, errors="coerce")
        result["stoch_k"] = stoch_k_raw.rolling(window=stoch_smooth_k).mean()
        result["stoch_d"] = result["stoch_k"].rolling(window=stoch_smooth_d).mean()

        typical_price = (result["high"] + result["low"] + result["close"]) / 3.0
        typical_sma = typical_price.rolling(window=cci_window).mean()
        mean_deviation = typical_price.rolling(window=cci_window).apply(
            lambda values: float(np.mean(np.abs(values - np.mean(values)))),
            raw=True,
        )
        cci_denominator = 0.015 * mean_deviation.replace(0, np.nan)
        result["cci"] = pd.to_numeric(
            (typical_price - typical_sma) / cci_denominator,
            errors="coerce",
        )

        williams_high = result["high"].rolling(window=williams_r_window).max()
        williams_low = result["low"].rolling(window=williams_r_window).min()
        williams_range = (williams_high - williams_low).replace(0, np.nan)
        result["williams_r"] = pd.to_numeric(
            -100 * (williams_high - close) / williams_range,
            errors="coerce",
        )

        average_volume = result["volume"].rolling(window=volume_ratio_window).mean()
        result["volume_ratio"] = result["volume"] / average_volume

        return result

    def _prepare_indicator_frame(self, quotes: pd.DataFrame) -> Optional[pd.DataFrame]:
        if quotes is None or quotes.empty:
            return None

        df = quotes.copy()
        df["time"] = pd.to_datetime(df["time"])
        df = df.sort_values("time").drop_duplicates("time", keep="last").reset_index(drop=True)

        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "quality_score",
        ]
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")

        return self._compute_indicators(df)

    def _calculate_trend_score(self, latest: pd.Series) -> Optional[float]:
        components = []

        close = latest.get("close")
        sma_short = latest.get("sma_short")
        sma_long = latest.get("sma_long")
        macd_hist = latest.get("macd_hist")
        rsi = latest.get("rsi")

        if pd.notna(close) and pd.notna(sma_short):
            components.append(1.0 if close > sma_short else -1.0 if close < sma_short else 0.0)
        if pd.notna(sma_short) and pd.notna(sma_long):
            components.append(1.0 if sma_short > sma_long else -1.0 if sma_short < sma_long else 0.0)
        if pd.notna(macd_hist):
            components.append(1.0 if macd_hist > 0 else -1.0 if macd_hist < 0 else 0.0)
        if pd.notna(rsi):
            normalized_rsi = max(-1.0, min(1.0, (float(rsi) - 50.0) / 20.0))
            components.append(normalized_rsi)

        if not components:
            return None

        return round(sum(components) / len(components), 4)

    def _classify_signal(self, trend_score: Optional[float]) -> str:
        if trend_score is None:
            return "insufficient_data"

        bullish_threshold = float(self.parameters["signal_thresholds"]["bullish"])
        bearish_threshold = float(self.parameters["signal_thresholds"]["bearish"])

        if trend_score >= bullish_threshold:
            return "bullish"
        if trend_score <= bearish_threshold:
            return "bearish"
        return "neutral"

    def _build_parameter_hash(self) -> str:
        payload = json.dumps(self.parameters, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _compute_period_return(series: pd.Series, periods: int) -> Optional[float]:
        if len(series) <= periods:
            return None

        current = series.iloc[-1]
        previous = series.iloc[-(periods + 1)]
        if pd.isna(current) or pd.isna(previous) or previous == 0:
            return None

        return round(float(current / previous - 1), 6)

    @staticmethod
    def _compute_distance(value: Any, baseline: Any) -> Optional[float]:
        if pd.isna(value) or pd.isna(baseline) or baseline == 0:
            return None
        return round(float(value / baseline - 1), 6)

    @staticmethod
    def _clean_numeric(value: Any) -> Optional[float]:
        if value is None or pd.isna(value):
            return None
        return round(float(value), 6)

    @staticmethod
    def _merge_parameters(override: Dict[str, Any]) -> Dict[str, Any]:
        merged = deepcopy(DEFAULT_TECHNICAL_PARAMETERS)
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key].update(value)
            else:
                merged[key] = value
        return merged
