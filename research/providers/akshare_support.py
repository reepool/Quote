"""
Shared AkShare runtime helpers for research-domain providers.
"""

from __future__ import annotations

import importlib
import sys
from typing import Any, Dict

from utils.proxy_patch_runtime import (
    get_akshare_proxy_patch_state,
    install_akshare_proxy_patch,
)


def load_akshare(mode: str = "direct") -> Any:
    """Load AkShare with explicit research-domain mode semantics."""
    normalized_mode = str(mode or "direct").strip().lower()
    if normalized_mode not in {"direct", "proxy_patch"}:
        raise ValueError(f"Unsupported AkShare mode: {mode}")

    if normalized_mode == "proxy_patch":
        _ensure_proxy_patch_installed()
        return _import_akshare(reload_module=True)

    return _import_akshare(reload_module=False)


def get_proxy_patch_state() -> Dict[str, Any]:
    """Return cached proxy-patch state for diagnostics and tests."""
    return get_akshare_proxy_patch_state()


def _ensure_proxy_patch_installed() -> None:
    install_akshare_proxy_patch(required=True)


def _import_akshare(*, reload_module: bool) -> Any:
    if "akshare" in sys.modules:
        module = sys.modules["akshare"]
        if reload_module:
            return importlib.reload(module)
        return module

    return importlib.import_module("akshare")
