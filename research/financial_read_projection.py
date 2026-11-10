"""Bounded financial value projections and their explicit unit semantics."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

CORE_FACTS = (
    "revenue",
    "gross_profit",
    "operating_profit",
    "pre_tax_profit",
    "net_income",
    "operating_cf",
    "total_cf",
    "total_assets",
    "total_liabilities",
    "equity",
    "current_assets",
    "current_liabilities",
    "inventory",
    "receivables",
    "fixed_assets",
    "intangible_assets",
    "shares_outstanding",
)
PERCENT_RATIOS = (
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "asset_liability_ratio",
)
INDICATORS = (
    *PERCENT_RATIOS,
    "current_ratio",
    "quick_ratio",
    "revenue_per_share",
    "operating_cf_to_revenue",
    "operating_cf_to_net_income",
    "book_value_per_share",
)
METADATA = (
    "instrument_id",
    "symbol",
    "exchange",
    "report_period",
    "publish_date",
    "data_available_date",
    "fiscal_year",
    "fiscal_quarter",
    "currency",
    "schema_version",
    "source",
    "source_mode",
    "data_as_of",
    "ingestion_run_id",
    "created_at",
    "updated_at",
)


def parse_financial_fields(fields: str | Iterable[str] | None) -> list[str] | None:
    """Validate business projection keys before any query or optional extension."""
    if fields is None:
        return None
    values = fields.split(",") if isinstance(fields, str) else fields
    selected = list(
        dict.fromkeys(str(value).strip() for value in values if str(value).strip())
    )
    allowed = {
        *CORE_FACTS,
        *(f"indicators.{key}" for key in INDICATORS),
        *(f"ratios.{key}" for key in PERCENT_RATIOS),
    }
    bad = [key for key in selected if key not in allowed]
    if not selected or bad:
        raise ValueError(
            "Invalid financial fields: " + ", ".join(bad or ["empty projection"])
        )
    return selected


def financial_projection(
    bundle: Mapping[str, Any], *, view: str = "full", fields: list[str] | None = None
) -> dict[str, Any]:
    """Project existing canonical scalars; preserve detail only when requested."""
    indicators = bundle.get("indicators") or {}
    ratios = {key: indicators.get(key) for key in PERCENT_RATIOS}
    currency = bundle.get("currency")
    # The existing service-layer fallback has no canonical scalar row and
    # exposes its selected canonical values in facts. Do not use arbitrary raw
    # JSON to override stored scalar values, including an explicit null.
    fallback_facts = (
        (bundle.get("facts") or {}) if bundle.get("source") == "service_layers" else {}
    )
    if view == "full" and fields is None:
        result = dict(bundle)
    else:
        result = {key: bundle[key] for key in METADATA if key in bundle}
        selected = fields if fields is not None else list(CORE_FACTS)
        for key in selected:
            if key in CORE_FACTS:
                result[key] = bundle[key] if key in bundle else fallback_facts.get(key)
            else:
                group, field = key.split(".", 1)
                result.setdefault(group, {})[field] = (
                    ratios if group == "ratios" else indicators
                ).get(field)
        if fields is None:
            result["indicators"] = {key: indicators.get(key) for key in INDICATORS}
        if bundle.get("service_layers"):
            result["service_layers"] = bundle["service_layers"]
        result["view"] = "summary" if view == "summary" else "full"
    if fields is None:
        result["ratios"] = ratios
    units: dict[str, str | None] = {}
    for key in CORE_FACTS:
        if key in result:
            units[key] = "share" if key == "shares_outstanding" else currency
    for key in result.get("indicators") or {}:
        if key not in INDICATORS:
            continue
        units[f"indicators.{key}"] = (
            "multiple"
            if key in {"current_ratio", "quick_ratio"}
            else f"{currency}/share"
            if key.endswith("per_share")
            else "ratio"
        )
    units.update({f"ratios.{key}": "ratio" for key in result.get("ratios") or {}})
    result["units"] = units
    return result


def pit_ratio_projection(
    item: dict[str, Any], fields: Iterable[str] | None = None
) -> dict[str, Any]:
    """Add explicit percent aliases and decimal projections to an existing PIT row."""
    selected = set(fields) if fields is not None else None
    result = dict(item)
    if selected is not None:
        # Keep identity/availability/quality metadata from the existing panel
        # projection. Only append requested numeric aliases here.
        result = {
            key: value
            for key, value in result.items()
            if key not in {*CORE_FACTS, *INDICATORS} or key in selected
        }
    units = {}
    ratios = {}
    for key in PERCENT_RATIOS:
        value = item.get(key)
        if (selected is None or key in selected) and key in result:
            units[key] = "percent"
        alias = f"{key}_pct"
        if selected is None or alias in selected:
            result[alias] = value
            units[alias] = "percent"
        if selected is None or f"ratios.{key}" in selected or "ratios" in selected:
            ratios[key] = float(value) / 100 if value is not None else None
    if ratios:
        result["ratios"] = ratios
        units.update({f"ratios.{key}": "ratio" for key in ratios})
    currency = item.get("currency")
    for key in result:
        if key in CORE_FACTS:
            units[key] = "share" if key == "shares_outstanding" else currency
        elif key in {"current_ratio", "quick_ratio"}:
            units[key] = "multiple"
        elif key.endswith("_per_share") or key in {"eps", "bps", "ocfps"}:
            units[key] = f"{currency}/share" if currency else None
        elif key in {"rev_yoy", "np_yoy"}:
            units[key] = "percent"
    result["units"] = units
    return result
