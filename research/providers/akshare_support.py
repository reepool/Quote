"""
Shared AkShare runtime helpers for research-domain providers.
"""

from __future__ import annotations

import importlib
import logging
import sys
from typing import Any, Dict

from utils.proxy_patch_runtime import (
    get_akshare_proxy_patch_state,
    install_akshare_proxy_patch,
)
from proxy_patch_bootstrap import (
    get_akshare_proxy_patch_state as get_bootstrap_akshare_proxy_patch_state,
)


_logger = logging.getLogger("DataManager")


def load_akshare(mode: str = "direct") -> Any:
    """Load AkShare with explicit research-domain mode semantics."""
    normalized_mode = str(mode or "direct").strip().lower()
    if normalized_mode not in {"direct", "proxy_patch"}:
        raise ValueError(f"Unsupported AkShare mode: {mode}")

    if normalized_mode == "proxy_patch":
        already_loaded = {
            name: name in sys.modules
            for name in ("akshare", "efinance", "requests", "yfinance")
        }
        runtime_state_before = get_akshare_proxy_patch_state()
        bootstrap_state = get_bootstrap_akshare_proxy_patch_state()
        patch_ready_before = bool(
            runtime_state_before.get("ready") or bootstrap_state.get("ready")
        )
        _ensure_proxy_patch_installed()
        if any(already_loaded.values()) and not patch_ready_before:
            _logger.warning(
                "[AkShareSupport] proxy_patch requested after related modules were already loaded: %s. "
                "akshare_proxy_patch should be bootstrapped at process entry before akshare/efinance/requests/yfinance imports.",
                {name: loaded for name, loaded in already_loaded.items() if loaded},
            )
        else:
            _logger.debug(
                "[AkShareSupport] proxy_patch ready (loaded_before=%s, runtime_ready_before=%s, bootstrap_ready=%s)",
                {name: loaded for name, loaded in already_loaded.items() if loaded},
                runtime_state_before.get("ready"),
                bootstrap_state.get("ready"),
            )
        return _import_akshare(reload_module=True)

    _logger.debug(
        "[AkShareSupport] loading akshare in direct mode (akshare_loaded=%s, proxy_state=%s)",
        "akshare" in sys.modules,
        get_akshare_proxy_patch_state(),
    )
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
