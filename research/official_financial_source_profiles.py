"""Official financial statement source profile helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class OfficialFinancialSourceProfile:
    """Stable metadata for one financial statement source profile."""

    profile_id: str
    source: str
    supported_exchanges: Tuple[str, ...]
    interface_type: str
    parser_profile: str
    parser_candidate: str
    source_unit: str
    source_unit_scale: float
    fallback_policy_profile: str
    anti_crawl_risk: str
    concurrency_assumption: str = "single_process_sequential"

    def as_metadata(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["supported_exchanges"] = list(self.supported_exchanges)
        return payload


@dataclass(frozen=True)
class OfficialFinancialSourceSelection:
    """Resolved source decision for one official financial backfill scope."""

    exchange: str
    requested_source: Optional[str]
    default_source: str
    resolved_source: str
    source_profile: str
    parser_profile: str
    auto_selected: bool
    reason: str
    diagnostics: Dict[str, Any]

    def as_metadata(self) -> Dict[str, Any]:
        return asdict(self)


OFFICIAL_FINANCIAL_SOURCE_PROFILES: Dict[str, OfficialFinancialSourceProfile] = {
    "sse_commonquery": OfficialFinancialSourceProfile(
        profile_id="sse_commonquery",
        source="sse",
        supported_exchanges=("SSE",),
        interface_type="exchange_hosted_structured_json_coded_fields",
        parser_profile="sse_commonquery_structured_json_facts.v1",
        parser_candidate="structured_financial_json.v1",
        source_unit="CNY",
        source_unit_scale=1.0,
        fallback_policy_profile="akshare_fallback",
        anti_crawl_risk="medium",
    ),
    "cninfo_data20": OfficialFinancialSourceProfile(
        profile_id="cninfo_data20",
        source="cninfo",
        supported_exchanges=("SSE", "SZSE", "BSE"),
        interface_type="cninfo_data20_structured_json_row_labels",
        parser_profile="cninfo_data20_structured_json_facts.v1",
        parser_candidate="structured_financial_json.v1",
        source_unit="CNY_10K",
        source_unit_scale=10000.0,
        fallback_policy_profile="akshare_fallback",
        anti_crawl_risk="medium",
    ),
    "akshare_fallback": OfficialFinancialSourceProfile(
        profile_id="akshare_fallback",
        source="akshare",
        supported_exchanges=("SSE", "SZSE", "BSE"),
        interface_type="third_party_financial_statement_fallback",
        parser_profile="akshare_financial_statement_bundle.v1",
        parser_candidate="financial_statement_bundle",
        source_unit="CNY",
        source_unit_scale=1.0,
        fallback_policy_profile="akshare_fallback",
        anti_crawl_risk="high",
    ),
}


_FINANCIAL_REPORT_TYPE_ID_BY_SUFFIX = {
    "Q1": "4000",
    "03-31": "4000",
    "Q2": "1000",
    "06-30": "1000",
    "Q3": "4400",
    "09-30": "4400",
    "Q4": "5000",
    "FY": "5000",
    "12-31": "5000",
}


_DEFAULT_OFFICIAL_SOURCE_BY_EXCHANGE = {
    "SSE": "sse",
    "SZSE": "cninfo",
    "BSE": "cninfo",
}


_PROFILE_BY_SOURCE_EXCHANGE = {
    ("sse", "SSE"): "sse_commonquery",
    ("cninfo", "SSE"): "cninfo_data20",
    ("cninfo", "SZSE"): "cninfo_data20",
    ("cninfo", "BSE"): "cninfo_data20",
    ("akshare", "SSE"): "akshare_fallback",
    ("akshare", "SZSE"): "akshare_fallback",
    ("akshare", "BSE"): "akshare_fallback",
}


def default_official_source_for_exchange(exchange: str) -> str:
    """Return the currently promoted official source for an exchange."""
    exchange_upper = str(exchange or "").upper()
    try:
        return _DEFAULT_OFFICIAL_SOURCE_BY_EXCHANGE[exchange_upper]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported exchange for official financial source profile: {exchange}"
        ) from exc


def source_profile_for(
    exchange: str,
    source: Optional[str],
    *,
    strict: bool = False,
) -> str:
    """Return source profile id for an exchange/source pair."""
    normalized_source = str(source or default_official_source_for_exchange(exchange)).lower()
    exchange_upper = str(exchange or "").upper()
    profile_id = _PROFILE_BY_SOURCE_EXCHANGE.get((normalized_source, exchange_upper))
    if profile_id is not None:
        return profile_id
    if strict:
        raise ValueError(
            "Unsupported official financial exchange/source profile combination: "
            f"exchange={exchange_upper or exchange}, source={normalized_source}"
        )
    return f"{normalized_source}:{exchange_upper.lower()}"


def official_source_profile(
    exchange: str,
    source: Optional[str],
    *,
    strict: bool = False,
) -> Optional[OfficialFinancialSourceProfile]:
    """Return the configured source profile for an exchange/source pair."""
    profile_id = source_profile_for(exchange, source, strict=strict)
    profile = OFFICIAL_FINANCIAL_SOURCE_PROFILES.get(profile_id)
    if profile is None and strict:
        raise ValueError(
            f"Official financial source profile is not configured: {profile_id}"
        )
    return profile


def parser_profile_for(
    exchange: str,
    source: Optional[str],
    *,
    fallback: str = "structured_financial_json.v1",
) -> str:
    """Return parser profile for an exchange/source pair."""
    profile = official_source_profile(exchange, source, strict=False)
    if profile is not None:
        return profile.parser_profile
    return fallback


def financial_report_type_id(report_period: str) -> str:
    """Return configured SSE/CNInfo report type id for a report-period label."""
    value = str(report_period or "").upper()
    for suffix, report_type_id in _FINANCIAL_REPORT_TYPE_ID_BY_SUFFIX.items():
        if value.endswith(suffix):
            return report_type_id
    return "5000"


def resolve_official_source_selection(
    exchange: str,
    requested_source: Optional[str],
    report_periods: List[str],
    *,
    module_config: Optional[Dict[str, Any]] = None,
) -> OfficialFinancialSourceSelection:
    """Resolve official source with config-driven period-availability overrides."""
    exchange_upper = str(exchange or "").upper()
    default_source = default_official_source_for_exchange(exchange_upper)
    normalized_request = (
        str(requested_source).strip().lower() if requested_source else None
    )
    explicit_source = normalized_request not in (None, "", "auto")
    resolved_source = normalized_request if explicit_source else default_source
    reason = "explicit_source" if explicit_source else "default_source"
    auto_selected = False
    diagnostics: Dict[str, Any] = {
        "report_periods": list(report_periods),
    }

    if not explicit_source:
        override = _matching_period_availability_override(
            exchange=exchange_upper,
            source=default_source,
            report_periods=report_periods,
            module_config=module_config or {},
        )
        if override is not None:
            resolved_source = str(override["alternate_source"]).lower()
            reason = str(override.get("reason") or "period_availability_override")
            auto_selected = True
            diagnostics.update(override)

    resolved_source_profile = source_profile_for(
        exchange_upper,
        resolved_source,
        strict=True,
    )
    return OfficialFinancialSourceSelection(
        exchange=exchange_upper,
        requested_source=normalized_request,
        default_source=default_source,
        resolved_source=resolved_source,
        source_profile=resolved_source_profile,
        parser_profile=parser_profile_for(exchange_upper, resolved_source),
        auto_selected=auto_selected,
        reason=reason,
        diagnostics=diagnostics,
    )


def _matching_period_availability_override(
    *,
    exchange: str,
    source: str,
    report_periods: List[str],
    module_config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    selection_cfg = module_config.get("official_source_selection") or {}
    if not bool(selection_cfg.get("enabled", False)):
        return None
    overrides = selection_cfg.get("period_unavailable_alternates") or []
    if not report_periods:
        return None
    for item in overrides:
        if not isinstance(item, dict) or not bool(item.get("enabled", True)):
            continue
        if str(item.get("exchange") or "").upper() != exchange:
            continue
        if str(item.get("source") or "").lower() != source:
            continue
        if item.get("switch_when") != "all_periods_beyond_report_type_max_year":
            continue
        max_year_by_id = item.get("report_type_max_year_by_id") or {}
        periods_beyond = [
            period
            for period in report_periods
            if _period_beyond_report_type_max_year(
                period,
                max_year_by_id=max_year_by_id,
            )
        ]
        if len(periods_beyond) != len(report_periods):
            continue
        return {
            "switch_when": item.get("switch_when"),
            "source": source,
            "source_profile": item.get("source_profile"),
            "alternate_source": item.get("alternate_source"),
            "alternate_source_profile": item.get("alternate_source_profile"),
            "reason": item.get("reason"),
            "observed_on": item.get("observed_on"),
            "report_type_max_year_by_id": dict(max_year_by_id),
            "periods_beyond_report_type_max_year": periods_beyond,
        }
    return None


def _period_beyond_report_type_max_year(
    report_period: str,
    *,
    max_year_by_id: Dict[str, Any],
) -> bool:
    report_type_id = financial_report_type_id(report_period)
    max_year = max_year_by_id.get(report_type_id)
    period_year = str(report_period or "")[:4]
    if max_year in (None, "") or not period_year.isdigit():
        return False
    try:
        return int(period_year) > int(str(max_year))
    except ValueError:
        return False


def source_profile_metadata(
    exchange: str,
    source: Optional[str],
    *,
    strict: bool = False,
) -> Dict[str, Any]:
    """Return JSON-ready source profile metadata for evidence and manifests."""
    profile = official_source_profile(exchange, source, strict=strict)
    if profile is None:
        profile_id = source_profile_for(exchange, source, strict=False)
        return {
            "profile_id": profile_id,
            "source": str(source or "").lower(),
            "supported_exchanges": [],
            "interface_type": "unsupported_or_unconfigured",
        }
    return profile.as_metadata()
