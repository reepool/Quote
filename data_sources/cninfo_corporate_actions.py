"""CNInfo structured corporate-action normalization and fetch helpers."""

from __future__ import annotations

import hashlib
import math
import re
from types import FunctionType
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Mapping, Optional


CNINFO_SOURCE = "cninfo"
DIVIDEND_PROFILE = "cninfo_dividend"
ALLOTMENT_PROFILE = "cninfo_allotment"


class _RequestsTimeoutProxy:
    """Delegate requests calls while enforcing a bounded POST timeout."""

    def __init__(self, delegate: Any, timeout_seconds: float) -> None:
        self._delegate = delegate
        self._timeout_seconds = timeout_seconds

    def post(self, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("timeout", self._timeout_seconds)
        return self._delegate.post(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def _bind_requests_timeout(
    loader: Callable[..., Any],
    timeout_seconds: float,
) -> Callable[..., Any]:
    """Clone an AkShare loader with an isolated timeout-aware requests proxy."""
    loader_globals = getattr(loader, "__globals__", None)
    requests_client = (
        loader_globals.get("requests") if isinstance(loader_globals, dict) else None
    )
    if requests_client is None or not hasattr(requests_client, "post"):
        raise RuntimeError("CNInfo AkShare loader does not expose requests.post")

    bound_globals = dict(loader_globals)
    bound_globals["requests"] = _RequestsTimeoutProxy(
        requests_client,
        max(1.0, float(timeout_seconds)),
    )
    bound = FunctionType(
        loader.__code__,
        bound_globals,
        name=loader.__name__,
        argdefs=loader.__defaults__,
        closure=loader.__closure__,
    )
    bound.__kwdefaults__ = getattr(loader, "__kwdefaults__", None)
    bound.__annotations__ = dict(getattr(loader, "__annotations__", {}))
    bound.__doc__ = loader.__doc__
    return bound


@dataclass(frozen=True)
class CninfoEndpointResult:
    """One bounded CNInfo endpoint result with explicit coverage semantics."""

    source_profile: str
    coverage_status: str
    observations: List[Dict[str, Any]]
    rows_received: int = 0
    error: Optional[str] = None


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text and text.lower() not in {"nan", "nat", "none"} else None


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _date(value: Any) -> Optional[date]:
    try:
        if value is not None and value != value:
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _clean_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def _per_share(value: Any) -> Optional[float]:
    number = _number(value)
    return number / 10.0 if number is not None else None


def parse_cninfo_distribution_description(
    description: Any,
) -> Dict[str, Optional[float]]:
    """Parse standard CNInfo descriptions such as ``10送2转增3股派1元``."""
    text = _clean_text(description)
    if not text:
        return {
            "cash_dividend_per_share": None,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
        }
    base_match = re.search(
        r"(?<![\d.])(?:每\s*)?(\d+(?:\.\d+)?)\s*股?\s*(?=送|转|派)",
        text,
    )
    if not base_match:
        return {
            "cash_dividend_per_share": None,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
        }
    base = _number(base_match.group(1))
    if not base or base <= 0:
        return {
            "cash_dividend_per_share": None,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
        }

    def extract(pattern: str) -> Optional[float]:
        match = re.search(pattern, text)
        value = _number(match.group(1)) if match else None
        return value / base if value is not None else None

    return {
        "cash_dividend_per_share": extract(
            r"派(?:发)?(?:现金)?(?:红利)?\s*(\d+(?:\.\d+)?)"
        ),
        "bonus_shares_per_share": extract(r"送(?:红股)?\s*(\d+(?:\.\d+)?)"),
        "capitalization_shares_per_share": extract(r"转(?:增)?\s*(\d+(?:\.\d+)?)"),
    }


def _action_type(
    cash: Optional[float],
    bonus: Optional[float],
    capitalization: Optional[float],
) -> str:
    effects = [
        name
        for name, value in (
            ("dividend", cash),
            ("bonus", bonus),
            ("capitalization", capitalization),
        )
        if value is not None and value > 0
    ]
    if len(effects) > 1:
        return "mixed_distribution"
    return effects[0] if effects else "distribution"


def _event_key(*parts: Any) -> str:
    identity = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _date_in_range(
    observation: Mapping[str, Any],
    start_date: date,
    end_date: date,
) -> bool:
    candidate = observation.get("ex_date") or observation.get("announcement_date")
    parsed = _date(candidate)
    if parsed is not None:
        return start_date <= parsed <= end_date
    fiscal_period = _clean_text(observation.get("fiscal_period")) or ""
    year_match = re.search(r"(19|20)\d{2}", fiscal_period)
    return bool(
        year_match and start_date.year - 1 <= int(year_match.group(0)) <= end_date.year
    )


def _has_temporal_anchor(observation: Mapping[str, Any]) -> bool:
    if _date(observation.get("ex_date") or observation.get("announcement_date")):
        return True
    fiscal_period = _clean_text(observation.get("fiscal_period")) or ""
    return re.search(r"(19|20)\d{2}", fiscal_period) is not None


def normalize_cninfo_dividend_rows(
    instrument_id: str,
    rows: List[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    """Normalize CNInfo dividend rows into source-neutral observations."""
    observations: List[Dict[str, Any]] = []
    for raw in rows:
        description = _clean_text(raw.get("实施方案分红说明"))
        parsed = parse_cninfo_distribution_description(description)
        structured_values = {
            "cash_dividend_per_share": _per_share(raw.get("派息比例")),
            "bonus_shares_per_share": _per_share(raw.get("送股比例")),
            "capitalization_shares_per_share": _per_share(raw.get("转增比例")),
        }
        used_description = False
        values: Dict[str, Optional[float]] = {}
        for key, structured_value in structured_values.items():
            fallback = parsed.get(key)
            values[key] = structured_value
            if structured_value is None and fallback is not None:
                values[key] = fallback
                used_description = True

        announcement_date = _date(raw.get("实施方案公告日期"))
        record_date = _date(raw.get("股权登记日"))
        ex_date = _date(raw.get("除权日"))
        pay_date = _date(raw.get("派息日"))
        fiscal_period = _clean_text(raw.get("报告时间"))
        distribution_type = _clean_text(raw.get("分红类型"))
        action_type = _action_type(
            values["cash_dividend_per_share"],
            values["bonus_shares_per_share"],
            values["capitalization_shares_per_share"],
        )
        has_effect = any(value is not None and value > 0 for value in values.values())
        if ex_date is None:
            quality_status = "partial_missing_ex_date"
        elif not has_effect:
            quality_status = "partial_zero_effect"
        elif used_description:
            quality_status = "parsed_description"
        else:
            quality_status = "structured_complete"

        if announcement_date:
            business_anchor = (
                f"ann:{announcement_date.isoformat()}:"
                f"period:{fiscal_period or ''}:type:{distribution_type or ''}"
            )
        elif ex_date:
            business_anchor = (
                f"ex:{ex_date.isoformat()}:"
                f"period:{fiscal_period or ''}:type:{distribution_type or ''}"
            )
        else:
            business_anchor = (
                f"period:{fiscal_period or ''}:type:{distribution_type or ''}:"
                f"description:{description or ''}"
            )
        observation = {
            "instrument_id": instrument_id,
            "source": CNINFO_SOURCE,
            "source_profile": DIVIDEND_PROFILE,
            "source_event_key": _event_key(
                instrument_id, DIVIDEND_PROFILE, business_anchor
            ),
            "action_type": action_type,
            "fiscal_period": fiscal_period,
            "announcement_date": announcement_date,
            "record_date": record_date,
            "ex_date": ex_date,
            "pay_date": pay_date,
            "share_arrival_date": _date(raw.get("股份到账日")),
            **values,
            "rights_shares_per_share": None,
            "rights_price": None,
            "currency": "CNY",
            "description": description,
            "event_status": (
                "announced_incomplete"
                if ex_date is None
                else ("scheduled" if ex_date > date.today() else "implemented")
            ),
            "quality_status": quality_status,
            "raw_payload": dict(raw),
        }
        if _date_in_range(observation, start_date, end_date):
            observations.append(observation)
    return observations


def normalize_cninfo_allotment_rows(
    instrument_id: str,
    rows: List[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    """Normalize CNInfo rights-issue implementation rows."""
    observations: List[Dict[str, Any]] = []
    for raw in rows:
        announcement_date = _date(raw.get("公告日期"))
        record_date = _date(raw.get("股权登记日"))
        ex_date = _date(raw.get("除权基准日"))
        rights_shares = _per_share(raw.get("配股比例"))
        rights_price = _number(raw.get("配股价格"))
        failure_refund_date = _date(raw.get("配股失败，退还申购款日期"))
        actual_allotted_shares = _number(raw.get("实际配股数量"))
        failed = failure_refund_date is not None
        zero_actual_allocation = (
            actual_allotted_shares is not None and actual_allotted_shares <= 0
        )
        source_record_id = _clean_text(raw.get("记录标识"))
        if failed:
            quality_status = "structured_non_effective"
        elif zero_actual_allocation:
            quality_status = "partial_zero_actual_allocation"
        elif ex_date is None:
            quality_status = "partial_missing_ex_date"
        elif rights_shares is None or rights_shares <= 0 or rights_price is None:
            quality_status = "partial_missing_economic_fields"
        else:
            quality_status = "structured_complete"
        business_anchor = source_record_id or (
            f"{ex_date or announcement_date or ''}:{record_date or ''}"
        )
        observation = {
            "instrument_id": instrument_id,
            "source": CNINFO_SOURCE,
            "source_profile": ALLOTMENT_PROFILE,
            "source_event_key": _event_key(
                instrument_id, ALLOTMENT_PROFILE, business_anchor
            ),
            "action_type": "rights",
            "fiscal_period": None,
            "announcement_date": announcement_date,
            "record_date": record_date,
            "ex_date": ex_date,
            "pay_date": None,
            "share_arrival_date": _date(raw.get("配股上市日")),
            "cash_dividend_per_share": None,
            "bonus_shares_per_share": None,
            "capitalization_shares_per_share": None,
            "rights_shares_per_share": rights_shares,
            "rights_price": rights_price,
            "currency": "CNY",
            "description": "配股失败" if failed else None,
            "event_status": (
                "failed"
                if failed
                else (
                    "announced_incomplete"
                    if ex_date is None or zero_actual_allocation
                    else ("scheduled" if ex_date > date.today() else "implemented")
                )
            ),
            "quality_status": quality_status,
            "raw_payload": dict(raw),
        }
        if _date_in_range(observation, start_date, end_date):
            observations.append(observation)
    return observations


class CninfoCorporateActionProvider:
    """Project-owned adapter for official CNInfo structured endpoints."""

    def __init__(
        self,
        *,
        dividend_loader: Optional[Callable[..., Any]] = None,
        allotment_loader: Optional[Callable[..., Any]] = None,
        request_timeout_seconds: float = 60.0,
    ) -> None:
        if dividend_loader is None or allotment_loader is None:
            import akshare as ak

            if dividend_loader is None:
                dividend_loader = _bind_requests_timeout(
                    ak.stock_dividend_cninfo,
                    request_timeout_seconds,
                )
            if allotment_loader is None:
                allotment_loader = _bind_requests_timeout(
                    ak.stock_allotment_cninfo,
                    request_timeout_seconds,
                )
        self.dividend_loader = dividend_loader
        self.allotment_loader = allotment_loader

    @staticmethod
    def _coverage_status(observations: List[Dict[str, Any]]) -> str:
        if not observations:
            return "complete_no_events"
        if any(
            str(item.get("quality_status") or "").startswith("partial_")
            for item in observations
        ):
            return "partial_missing_fields"
        return "complete_with_events"

    @staticmethod
    def _require_columns(frame: Any, required: set[str], profile: str) -> None:
        columns = {str(column) for column in getattr(frame, "columns", [])}
        missing = sorted(required - columns)
        if missing:
            raise ValueError(f"CNInfo {profile} response is missing columns: {missing}")

    def fetch_dividends(
        self,
        instrument_id: str,
        symbol: str,
        *,
        start_date: date,
        end_date: date,
    ) -> CninfoEndpointResult:
        try:
            frame = self.dividend_loader(symbol=symbol)
            if frame is None or not hasattr(frame, "to_dict"):
                raise ValueError("CNInfo dividend response is not a table")
            self._require_columns(
                frame,
                {"实施方案公告日期", "除权日", "实施方案分红说明"},
                DIVIDEND_PROFILE,
            )
            raw_rows = frame.to_dict(orient="records")
            anchored_rows = [
                raw
                for raw in raw_rows
                if _has_temporal_anchor(
                    {
                        "ex_date": raw.get("除权日"),
                        "announcement_date": raw.get("实施方案公告日期"),
                        "fiscal_period": raw.get("报告时间"),
                    }
                )
            ]
            observations = normalize_cninfo_dividend_rows(
                instrument_id,
                anchored_rows,
                start_date=start_date,
                end_date=end_date,
            )
            if len(anchored_rows) != len(raw_rows):
                return CninfoEndpointResult(
                    source_profile=DIVIDEND_PROFILE,
                    coverage_status="indeterminate",
                    observations=observations,
                    rows_received=len(raw_rows),
                    error=(
                        "CNInfo dividend response contains "
                        f"{len(raw_rows) - len(anchored_rows)} rows without a temporal anchor"
                    ),
                )
            return CninfoEndpointResult(
                source_profile=DIVIDEND_PROFILE,
                coverage_status=self._coverage_status(observations),
                observations=observations,
                rows_received=len(raw_rows),
            )
        except Exception as exc:
            return CninfoEndpointResult(
                source_profile=DIVIDEND_PROFILE,
                coverage_status="indeterminate",
                observations=[],
                error=str(exc),
            )

    def fetch_allotments(
        self,
        instrument_id: str,
        symbol: str,
        *,
        start_date: date,
        end_date: date,
    ) -> CninfoEndpointResult:
        try:
            frame = self.allotment_loader(
                symbol=symbol,
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
            )
            if frame is None or not hasattr(frame, "to_dict"):
                raise ValueError("CNInfo allotment response is not a table")
            self._require_columns(
                frame,
                {"记录标识", "除权基准日", "配股比例", "配股价格"},
                ALLOTMENT_PROFILE,
            )
            raw_rows = frame.to_dict(orient="records")
            anchored_rows = [
                raw
                for raw in raw_rows
                if _has_temporal_anchor(
                    {
                        "ex_date": raw.get("除权基准日"),
                        "announcement_date": raw.get("公告日期"),
                    }
                )
            ]
            observations = normalize_cninfo_allotment_rows(
                instrument_id,
                anchored_rows,
                start_date=start_date,
                end_date=end_date,
            )
            if len(anchored_rows) != len(raw_rows):
                return CninfoEndpointResult(
                    source_profile=ALLOTMENT_PROFILE,
                    coverage_status="indeterminate",
                    observations=observations,
                    rows_received=len(raw_rows),
                    error=(
                        "CNInfo allotment response contains "
                        f"{len(raw_rows) - len(anchored_rows)} rows without a temporal anchor"
                    ),
                )
            return CninfoEndpointResult(
                source_profile=ALLOTMENT_PROFILE,
                coverage_status=self._coverage_status(observations),
                observations=observations,
                rows_received=len(raw_rows),
            )
        except Exception as exc:
            return CninfoEndpointResult(
                source_profile=ALLOTMENT_PROFILE,
                coverage_status="indeterminate",
                observations=[],
                error=str(exc),
            )
