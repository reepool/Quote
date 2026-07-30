"""Tencent-first AkShare adapter for independent A-share factor evidence."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Tuple

import pandas as pd


class PriceRatioFactorError(RuntimeError):
    """Raised when adjusted and raw prices cannot form a reliable factor path."""


@dataclass(frozen=True)
class AkshareFactorPathResult:
    events: List[Dict[str, Any]]
    source_profile: str
    diagnostics: Dict[str, Any]


def validate_price_ratio_snapshot_coverage(
    diagnostics: Mapping[str, Any],
    *,
    requested_start: date,
    requested_end: date,
    listed_date: Optional[date] = None,
    delisted_date: Optional[date] = None,
    tolerance_days: int = 10,
) -> Dict[str, Any]:
    """Validate provider overlap against the security's requested lifecycle."""

    def parsed(value: Any) -> Optional[date]:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if value in (None, ""):
            return None
        try:
            return datetime.fromisoformat(str(value)[:10]).date()
        except ValueError:
            return None

    required_start = max(
        value
        for value in (requested_start, listed_date)
        if value is not None
    )
    required_end = min(
        value
        for value in (requested_end, delisted_date)
        if value is not None
    )
    if required_end < required_start:
        raise PriceRatioFactorError(
            "requested range does not overlap the security lifecycle"
        )
    first_overlap = parsed(diagnostics.get("first_overlap_date"))
    last_overlap = parsed(diagnostics.get("last_overlap_date"))
    tolerance = max(0, int(tolerance_days))
    coverage_errors = []
    if (
        listed_date is not None
        and (
            first_overlap is None
            or first_overlap > required_start + timedelta(days=tolerance)
        )
    ):
        coverage_errors.append("leading_history_truncated")
    if (
        last_overlap is None
        or last_overlap < required_end - timedelta(days=tolerance)
    ):
        coverage_errors.append("trailing_history_truncated")
    if coverage_errors:
        raise PriceRatioFactorError(
            "provider snapshot coverage incomplete: "
            f"{','.join(coverage_errors)} "
            f"required={required_start}..{required_end} "
            f"observed={first_overlap}..{last_overlap}"
        )
    return {
        "coverage_validated": True,
        "required_coverage_start": required_start.isoformat(),
        "required_coverage_end": required_end.isoformat(),
        "coverage_tolerance_days": tolerance,
    }


def _price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        raise PriceRatioFactorError("empty price frame")
    date_column = next(
        (name for name in ("date", "日期") if name in frame.columns),
        None,
    )
    close_column = next(
        (name for name in ("close", "收盘") if name in frame.columns),
        None,
    )
    if date_column is None or close_column is None:
        raise PriceRatioFactorError("price frame lacks date/close columns")
    normalized = frame[[date_column, close_column]].copy()
    normalized.columns = ["date", "close"]
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["close"] = pd.to_numeric(
        normalized["close"], errors="coerce"
    )
    normalized = normalized.dropna(subset=["date", "close"])
    normalized = normalized[normalized["close"] > 0]
    normalized = normalized.drop_duplicates("date", keep="last")
    normalized = normalized.set_index("date").sort_index()
    if normalized.empty:
        raise PriceRatioFactorError("price frame has no positive dated closes")
    return normalized


def _relative_dispersion(values: pd.Series, center: float) -> float:
    if center <= 0 or values.empty:
        return math.inf
    return float((values / center - 1.0).abs().median())


def derive_price_ratio_factor_events(
    raw_frame: pd.DataFrame,
    adjusted_frame: pd.DataFrame,
    *,
    instrument_id: str,
    requested_start: date,
    requested_end: date,
    source_profile: str,
    persistence: int = 3,
    min_level_observations: int = 2,
    min_jump_relative: float = 0.003,
    max_level_dispersion: float = 0.002,
    noise_multiplier: float = 4.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Extract persistent event ratios from aligned adjusted/raw close prices."""

    persistence = max(2, int(persistence))
    min_level_observations = max(
        2, min(int(min_level_observations), persistence)
    )
    if requested_end < requested_start:
        raise ValueError("requested_end must not be earlier than requested_start")
    raw = _price_frame(raw_frame).rename(columns={"close": "raw_close"})
    adjusted = _price_frame(adjusted_frame).rename(
        columns={"close": "adjusted_close"}
    )
    aligned = raw.join(adjusted, how="inner")
    aligned = aligned[
        (aligned["raw_close"] > 0) & (aligned["adjusted_close"] > 0)
    ]
    if len(aligned) < min_level_observations * 2:
        raise PriceRatioFactorError(
            "insufficient overlapping raw/adjusted observations"
        )
    ratios = (
        aligned["adjusted_close"] / aligned["raw_close"]
    ).replace([math.inf, -math.inf], math.nan).dropna()
    ratios = ratios[ratios > 0].sort_index()
    if len(ratios) < min_level_observations * 2:
        raise PriceRatioFactorError("insufficient positive price ratios")

    candidates: List[Dict[str, Any]] = []
    reliable_window_count = 0
    for index in range(1, len(ratios)):
        left = ratios.iloc[max(0, index - persistence):index]
        right = ratios.iloc[index:min(len(ratios), index + persistence)]
        if (
            len(left) < min_level_observations
            or len(right) < min_level_observations
        ):
            continue
        left_level = float(left.median())
        right_level = float(right.median())
        if left_level <= 0 or right_level <= 0:
            continue
        left_noise = _relative_dispersion(left, left_level)
        right_noise = _relative_dispersion(right, right_level)
        if (
            left_noise > max_level_dispersion
            or right_noise > max_level_dispersion
        ):
            continue
        reliable_window_count += 1
        threshold = max(
            float(min_jump_relative),
            float(noise_multiplier) * max(left_noise, right_noise),
        )
        factor = right_level / left_level
        if abs(factor - 1.0) <= threshold:
            continue
        candidates.append({
            "index": index,
            "date": ratios.index[index],
            "factor": factor,
            "left_level": left_level,
            "right_level": right_level,
            "left_noise": left_noise,
            "right_noise": right_noise,
            "threshold": threshold,
        })

    clusters: List[List[Dict[str, Any]]] = []
    for candidate in candidates:
        if (
            not clusters
            or candidate["index"] - clusters[-1][-1]["index"] > 1
        ):
            clusters.append([candidate])
        else:
            clusters[-1].append(candidate)

    events: List[Dict[str, Any]] = []
    cumulative = 1.0
    for cluster in clusters:
        selected = max(
            cluster,
            key=lambda item: abs(
                float(ratios.iloc[int(item["index"])])
                / float(ratios.iloc[int(item["index"]) - 1])
                - 1.0
            ),
        )
        event_date = selected["date"].date()
        if not requested_start <= event_date <= requested_end:
            continue
        factor = float(selected["factor"])
        cumulative *= factor
        events.append({
            "instrument_id": instrument_id,
            "ex_date": selected["date"].to_pydatetime(),
            "factor": round(factor, 10),
            "cumulative_factor": round(cumulative, 10),
            "source": "akshare",
            "source_profile": source_profile,
            "quality_status": "valid",
            "raw_payload": {
                "extraction": "adjusted_raw_close_plateau_ratio",
                "left_level": selected["left_level"],
                "right_level": selected["right_level"],
                "left_noise": selected["left_noise"],
                "right_noise": selected["right_noise"],
                "threshold": selected["threshold"],
            },
        })

    material_transition_indexes = [
        index
        for index in range(1, len(ratios))
        if requested_start <= ratios.index[index].date() <= requested_end
        and abs(float(ratios.iloc[index] / ratios.iloc[index - 1]) - 1.0)
        > float(min_jump_relative)
    ]
    explained_transition_indexes = {
        index
        for candidate in candidates
        for index in range(
            max(1, int(candidate["index"]) - persistence),
            min(len(ratios), int(candidate["index"]) + persistence + 1),
        )
    }
    unresolved_transition_indexes = [
        index
        for index in material_transition_indexes
        if index not in explained_transition_indexes
    ]
    if reliable_window_count == 0:
        raise PriceRatioFactorError(
            "price ratios contain no reliable stable comparison window"
        )
    if unresolved_transition_indexes:
        unresolved_dates = ", ".join(
            ratios.index[index].date().isoformat()
            for index in unresolved_transition_indexes[:3]
        )
        raise PriceRatioFactorError(
            "price ratios contain material transitions without stable "
            f"two-sided evidence: {unresolved_dates}"
        )

    diagnostics = {
        "source_profile": source_profile,
        "raw_rows": len(raw),
        "adjusted_rows": len(adjusted),
        "overlap_rows": len(aligned),
        "first_overlap_date": ratios.index[0].date().isoformat(),
        "last_overlap_date": ratios.index[-1].date().isoformat(),
        "candidate_transitions": len(candidates),
        "reliable_windows": reliable_window_count,
        "event_count": len(events),
        "persistence": persistence,
        "min_jump_relative": float(min_jump_relative),
        "max_level_dispersion": float(max_level_dispersion),
    }
    return events, diagnostics


class AkshareAShareFactorAdapter:
    """Acquire one independent A-share path through AkShare provider adapters."""

    def __init__(
        self,
        *,
        akshare_module: Any,
        to_thread: Callable[..., Awaitable[Any]] = asyncio.to_thread,
        config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.akshare = akshare_module
        self.to_thread = to_thread
        self.config = dict(config or {})

    async def fetch(
        self,
        *,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> AkshareFactorPathResult:
        errors: List[Dict[str, str]] = []
        for provider in ("tencent", "eastmoney"):
            try:
                raw, adjusted = await self._fetch_pair(
                    provider=provider,
                    instrument_id=instrument_id,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                )
                profile = (
                    "akshare_tencent_price_ratio_v1"
                    if provider == "tencent"
                    else "akshare_eastmoney_price_ratio_v1"
                )
                events, diagnostics = derive_price_ratio_factor_events(
                    raw,
                    adjusted,
                    instrument_id=instrument_id,
                    requested_start=start_date.date(),
                    requested_end=end_date.date(),
                    source_profile=profile,
                    persistence=int(self.config.get("persistence", 3)),
                    min_level_observations=int(
                        self.config.get("min_level_observations", 2)
                    ),
                    min_jump_relative=float(
                        self.config.get("min_jump_relative", 0.003)
                    ),
                    max_level_dispersion=float(
                        self.config.get("max_level_dispersion", 0.002)
                    ),
                    noise_multiplier=float(
                        self.config.get("noise_multiplier", 4.0)
                    ),
                )
                diagnostics.update({
                    "provider": provider,
                    "fallback_errors": errors,
                    "requested_start": start_date.date().isoformat(),
                    "requested_end": end_date.date().isoformat(),
                })
                for event in events:
                    event["raw_payload"].update(diagnostics)
                return AkshareFactorPathResult(
                    events=events,
                    source_profile=profile,
                    diagnostics=diagnostics,
                )
            except Exception as exc:
                errors.append({
                    "provider": provider,
                    "error": f"{type(exc).__name__}: {exc}",
                })
        raise PriceRatioFactorError(
            "all AkShare A-share factor providers failed: "
            + "; ".join(
                f"{item['provider']}={item['error']}" for item in errors
            )
        )

    async def _fetch_pair(
        self,
        *,
        provider: str,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        lookback_days = max(7, int(self.config.get("lookback_days", 30)))
        lookahead_days = max(7, int(self.config.get("lookahead_days", 14)))
        fetch_start = (start_date.date() - timedelta(days=lookback_days))
        fetch_end = end_date.date() + timedelta(days=lookahead_days)
        timeout = float(self.config.get("timeout_seconds", 30))
        if provider == "tencent":
            tx_symbol = _tencent_symbol(instrument_id, symbol)
            common = {
                "symbol": tx_symbol,
                "start_date": fetch_start.strftime("%Y%m%d"),
                "end_date": fetch_end.strftime("%Y%m%d"),
                "timeout": timeout,
            }
            raw = await self.to_thread(
                self.akshare.stock_zh_a_hist_tx,
                adjust="",
                **common,
            )
            adjusted = await self.to_thread(
                self.akshare.stock_zh_a_hist_tx,
                adjust="hfq",
                **common,
            )
            return raw, adjusted
        if provider == "eastmoney":
            common = {
                "symbol": symbol,
                "period": "daily",
                "start_date": fetch_start.strftime("%Y%m%d"),
                "end_date": fetch_end.strftime("%Y%m%d"),
                "timeout": timeout,
            }
            raw = await self.to_thread(
                self.akshare.stock_zh_a_hist,
                adjust="",
                **common,
            )
            adjusted = await self.to_thread(
                self.akshare.stock_zh_a_hist,
                adjust="hfq",
                **common,
            )
            return raw, adjusted
        raise ValueError(f"unsupported AkShare factor provider: {provider}")


def _tencent_symbol(instrument_id: str, symbol: str) -> str:
    normalized = str(instrument_id or "").upper()
    if normalized.endswith(".SH"):
        return f"sh{symbol}"
    if normalized.endswith(".SZ"):
        return f"sz{symbol}"
    if normalized.endswith((".BJ", ".BSE")):
        return f"bj{symbol}"
    raise PriceRatioFactorError(
        f"unsupported A-share instrument identifier: {instrument_id}"
    )
