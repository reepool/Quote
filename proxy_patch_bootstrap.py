"""
Early bootstrap for akshare_proxy_patch.

This module intentionally avoids importing project packages.  Newer
akshare_proxy_patch builds must be installed before modules that import
requests/akshare/yfinance, so runtime entry points should call this first.
"""

from __future__ import annotations

import importlib
import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


_logger = logging.getLogger("proxy_patch_bootstrap")


@dataclass
class ProxyPatchState:
    target: str
    attempted: bool = False
    ready: bool = False
    error: Optional[str] = None
    gateway: Optional[str] = None
    retry: Optional[int] = None
    hook_domains: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


_AKSHARE_STATE = ProxyPatchState(target="akshare")


def install_akshare_proxy_patch(*, required: bool = False) -> ProxyPatchState:
    defaults = {
        "gateway": "101.201.173.125",
        "retry": 30,
        "hook_domains": [
            "fund.eastmoney.com",
            "push2.eastmoney.com",
            "push2his.eastmoney.com",
            "emweb.securities.eastmoney.com",
        ],
    }
    return _install_patch(
        state=_AKSHARE_STATE,
        config=_load_proxy_patch_config("akshare"),
        defaults=defaults,
        installer_name="install_patch",
        required=required,
    )


def get_akshare_proxy_patch_state() -> Dict[str, Any]:
    return _AKSHARE_STATE.as_dict()


def _install_patch(
    *,
    state: ProxyPatchState,
    config: Dict[str, Any],
    defaults: Dict[str, Any],
    installer_name: str,
    required: bool,
) -> ProxyPatchState:
    if state.ready:
        return state
    if state.attempted and state.error:
        if required:
            raise RuntimeError(state.error)
        return state

    state.attempted = True
    if not config.get("enabled", False):
        state.error = "akshare proxy patch is disabled"
        _logger.info(state.error)
        if required:
            raise RuntimeError(state.error)
        return state

    gateway = str(config.get("gateway") or defaults.get("gateway") or "").strip()
    auth_token = str(config.get("auth_token") or "").strip()
    retry = int(config.get("retry", defaults.get("retry", 30)))
    hook_domains = [
        str(item).strip()
        for item in config.get("hook_domains", defaults.get("hook_domains", []))
        if str(item).strip()
    ]

    state.gateway = gateway
    state.retry = retry
    state.hook_domains = hook_domains

    if not gateway or not auth_token:
        state.error = "akshare proxy patch gateway/auth_token is not fully configured"
        if required:
            raise RuntimeError(state.error)
        _logger.warning(state.error)
        return state

    try:
        proxy_patch = importlib.import_module("akshare_proxy_patch")
    except ImportError as exc:
        state.error = "akshare_proxy_patch is not installed"
        if required:
            raise RuntimeError(state.error) from exc
        _logger.warning(state.error)
        return state

    installer = getattr(proxy_patch, installer_name)
    kwargs: Dict[str, Any] = {
        "auth_token": auth_token,
        "retry": retry,
    }
    if hook_domains:
        kwargs["hook_domains"] = hook_domains

    try:
        installer(gateway, **kwargs)
    except Exception as exc:
        state.error = f"Failed to install akshare proxy patch: {exc}"
        if required:
            raise RuntimeError(state.error) from exc
        _logger.warning(state.error)
        return state

    state.ready = True
    state.error = None
    _logger.info("akshare proxy patch installed early (token=%s***)", auth_token[:6])
    return state


def _load_proxy_patch_config(source_name: str) -> Dict[str, Any]:
    config_path = Path(__file__).resolve().parent / "config" / "03_data.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data_config = json.load(f)
    except Exception as exc:
        _logger.warning("Failed to read proxy patch config: %s", exc)
        return {}

    data_sources_cfg = data_config.get("data_sources_config", {}) or {}
    source_cfg = data_sources_cfg.get(source_name, {}) or {}
    return dict(source_cfg.get("proxy_patch", {}) or {})
