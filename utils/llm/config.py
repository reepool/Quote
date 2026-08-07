"""Configuration and explicit local environment bootstrap for the LLM gateway."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .models import LlmConfig, project_root


def load_llm_config(value: Mapping[str, Any] | None) -> LlmConfig:
    return LlmConfig.from_mapping(value)


def load_project_environment(
    root: Path | None = None,
    *,
    override: bool = False,
) -> bool:
    """Explicitly load the ignored project `.env` for an application entrypoint.

    This function is intentionally not called at import time. Existing environment
    variables win over local files, which keeps service secret injection authoritative.
    """

    dotenv_path = (root or project_root()) / ".env"
    if not dotenv_path.is_file():
        return False
    try:
        from dotenv import load_dotenv
    except ImportError:
        return False
    if override:
        raise ValueError("project environment loading must not override process values")
    return bool(load_dotenv(dotenv_path=dotenv_path, override=False))
