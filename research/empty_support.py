"""
Optional-empty exchange support for research domains.
"""

from __future__ import annotations

from typing import Any, Iterable, Set


EMPTY_PLACEHOLDER_SOURCE = "empty_placeholder"
EMPTY_PLACEHOLDER_MODE = "synthetic"
EMPTY_PLACEHOLDER_REASON = "optional_empty_exchange"


def get_optional_empty_exchanges(
    research_config: Any,
    module_name: str,
) -> Set[str]:
    module_cfg = research_config.modules.get(module_name, {})
    return {
        str(exchange).strip().upper()
        for exchange in module_cfg.get("optional_empty_exchanges", [])
        if str(exchange).strip()
    }


def allows_optional_empty_exchange(
    research_config: Any,
    module_name: str,
    exchange: str,
) -> bool:
    return str(exchange).strip().upper() in get_optional_empty_exchanges(
        research_config,
        module_name,
    )


def filter_required_exchanges(
    exchanges: Iterable[str],
    optional_empty_exchanges: Set[str],
) -> list[str]:
    return [
        str(exchange).strip().upper()
        for exchange in exchanges
        if str(exchange).strip().upper() not in optional_empty_exchanges
    ]
