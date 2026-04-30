#!/usr/bin/env python
"""Shared helpers for repository-level research CLI scripts."""

from __future__ import annotations

import inspect
from datetime import date, datetime
from typing import Any, List, Optional


def parse_exchanges(raw: Optional[str]) -> Optional[List[str]]:
    """Parse comma-separated exchanges into an uppercase list."""
    if raw is None:
        return None
    exchanges = [part.strip().upper() for part in raw.split(",") if part.strip()]
    return exchanges or None


def json_ready(value: Any) -> Any:
    """Convert dates and nested structures into JSON-serializable values."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    return value


async def initialize_manager_for_research_cli(manager: Any) -> None:
    """Initialize DataManager in lightweight research-only mode when supported."""
    initialize = getattr(manager, "initialize")
    try:
        parameters = inspect.signature(initialize).parameters
    except (TypeError, ValueError):
        parameters = {}

    supports_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    if (
        "include_data_sources" in parameters
        or "load_progress" in parameters
        or supports_kwargs
    ):
        await initialize(
            include_data_sources=False,
            load_progress=False,
        )
        return

    await initialize()
