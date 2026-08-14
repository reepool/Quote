"""Stable announcement categories and provider-owned parameter mappings."""

from __future__ import annotations

from typing import Any

ANNUAL_REPORT_CATEGORY = "annual_report"
SEMIANNUAL_REPORT_CATEGORY = "semiannual_report"
PERIODIC_REPORT_CATEGORY = "periodic_report"
PERIODIC_REPORT_ANOMALY_CATEGORY = "periodic_report_anomaly"

_CNINFO_PERIODIC_REPORT_VALUE = (
    "category_yjdbg_szsh;category_bndbg_szsh;"
    "category_sjdbg_szsh;category_ndbg_szsh"
)

_ALIASES = {
    ANNUAL_REPORT_CATEGORY: ANNUAL_REPORT_CATEGORY,
    "annual": ANNUAL_REPORT_CATEGORY,
    "annual_report_correction": ANNUAL_REPORT_CATEGORY,
    "annual_correction": ANNUAL_REPORT_CATEGORY,
    "correction_notice": ANNUAL_REPORT_CATEGORY,
    "category_ndbg_szsh": ANNUAL_REPORT_CATEGORY,
    SEMIANNUAL_REPORT_CATEGORY: SEMIANNUAL_REPORT_CATEGORY,
    "semiannual": SEMIANNUAL_REPORT_CATEGORY,
    "semiannual_correction": SEMIANNUAL_REPORT_CATEGORY,
    "category_bndbg_szsh": SEMIANNUAL_REPORT_CATEGORY,
    PERIODIC_REPORT_CATEGORY: PERIODIC_REPORT_CATEGORY,
    "periodic": PERIODIC_REPORT_CATEGORY,
    _CNINFO_PERIODIC_REPORT_VALUE: PERIODIC_REPORT_CATEGORY,
    PERIODIC_REPORT_ANOMALY_CATEGORY: PERIODIC_REPORT_ANOMALY_CATEGORY,
}

_CNINFO_VALUES = {
    ANNUAL_REPORT_CATEGORY: "category_ndbg_szsh",
    SEMIANNUAL_REPORT_CATEGORY: "category_bndbg_szsh",
    PERIODIC_REPORT_CATEGORY: _CNINFO_PERIODIC_REPORT_VALUE,
}

_EXCHANGE_OPTIONS: dict[str, dict[str, dict[str, Any]]] = {
    "SSE": {
        ANNUAL_REPORT_CATEGORY: {"report_type2": "DQBG", "report_type": "YEARLY"},
        SEMIANNUAL_REPORT_CATEGORY: {"report_type2": "DQBG", "report_type": "QUATER2"},
        PERIODIC_REPORT_CATEGORY: {"report_type2": "DQBG", "report_type": "ALL"},
    },
    "SZSE": {
        ANNUAL_REPORT_CATEGORY: {"big_category_id": ["010301"]},
        SEMIANNUAL_REPORT_CATEGORY: {"big_category_id": ["010303"]},
        PERIODIC_REPORT_CATEGORY: {
            "big_category_id": ["010301", "010302", "010303", "010304"]
        },
    },
    "BSE": {
        PERIODIC_REPORT_CATEGORY: {
            "disclosure_subtype": [
                "9503-1001",
                "9503-1002",
                "9503-1003",
                "9504-8001",
                "9504-2106",
                "9503-1004",
                "9504-2104",
            ]
        },
        PERIODIC_REPORT_ANOMALY_CATEGORY: {
            "disclosure_subtype": ["9504-2108"]
        },
    },
}


def normalize_announcement_category(value: Any) -> str | None:
    """Normalize known periodic-report aliases while preserving other categories."""

    text = str(value or "").strip()
    if not text:
        return None
    return _ALIASES.get(text.lower(), text)


def cninfo_category_value(value: Any) -> str | None:
    """Return the CNInfo category token for a normalized query value."""

    normalized = normalize_announcement_category(value)
    if normalized is None:
        return None
    return _CNINFO_VALUES.get(normalized, normalized)


def exchange_category_options(exchange: str, value: Any) -> dict[str, Any] | None:
    """Return official exchange parameters, or None for an unsupported category."""

    normalized = normalize_announcement_category(value)
    if normalized is None:
        return {}
    options = _EXCHANGE_OPTIONS.get(str(exchange or "").strip().upper(), {}).get(
        normalized
    )
    return None if options is None else dict(options)
