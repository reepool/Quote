"""
Runtime helpers for akshare_proxy_patch integration.

Patch installers must run before importing the target library. Keep this module
lightweight: it reads JSON config directly and intentionally avoids importing
the global config manager.
"""

from __future__ import annotations

import importlib
import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional


_logger = logging.getLogger("proxy_patch_runtime")


@dataclass
class ProxyPatchState:
    """State for one proxy patch target."""

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
_YFINANCE_STATE = ProxyPatchState(target="yfinance")


class ProxyResponseRejectedError(RuntimeError):
    """All acquired proxy exits returned a response rejected by the caller."""


def install_akshare_proxy_patch(*, required: bool = False) -> ProxyPatchState:
    """Install akshare proxy patch using data_sources_config.akshare.proxy_patch."""
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
        target="akshare",
        state=_AKSHARE_STATE,
        config=_load_proxy_patch_config("akshare"),
        defaults=defaults,
        installer_name="install_patch",
        required=required,
    )


def install_yfinance_proxy_patch(*, required: bool = False) -> ProxyPatchState:
    """Install yfinance proxy patch using data_sources_config.yfinance.proxy_patch."""
    config = _load_proxy_patch_config("yfinance")
    akshare_config = _load_proxy_patch_config("akshare")
    config.setdefault("gateway", akshare_config.get("gateway", "101.201.173.125"))
    config.setdefault("auth_token", akshare_config.get("auth_token", ""))
    config.setdefault("retry", akshare_config.get("retry", 30))
    # Do not force hook_domains here. The upstream yfinance patch default is
    # intentionally narrow and avoids breaking yfinance's consent flow.
    return _install_patch(
        target="yfinance",
        state=_YFINANCE_STATE,
        config=config,
        defaults={"gateway": "101.201.173.125", "retry": 30},
        installer_name="install_yfinance_patch",
        required=required,
    )


def get_akshare_proxy_patch_state() -> Dict[str, Any]:
    return _AKSHARE_STATE.as_dict()


def get_yfinance_proxy_patch_state() -> Dict[str, Any]:
    return _YFINANCE_STATE.as_dict()


def request_with_akshare_proxy(
    method: str,
    url: str,
    *,
    attempts: int = 3,
    timeout: float = 15.0,
    headers: Optional[Mapping[str, str]] = None,
    accept_response: Optional[Callable[[Any], bool]] = None,
    warning_logger: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    """Request one URL through freshly authorized proxy exits.

    This is an explicit fallback for source adapters that must inspect HTTP 200
    response bodies before deciding whether an exit is usable. It does not
    replace the normal domain hook and never logs credentials or proxy URLs.
    """
    config = _load_proxy_patch_config("akshare")
    gateway = str(config.get("gateway") or "").strip()
    auth_token = str(config.get("auth_token") or "").strip()
    if not config.get("enabled", False) or not gateway or not auth_token:
        raise RuntimeError("akshare proxy fallback is not fully configured")

    import requests

    try:
        proxy_patch = importlib.import_module("akshare_proxy_patch")
        patch_version = str(getattr(proxy_patch, "__version__", "0.5.0"))
    except ImportError:
        patch_version = "0.5.0"
    session_class = getattr(requests, "_OriginalSession", requests.Session)
    auth_url = f"http://{gateway}:47001/api/akshare-auth"
    last_error: Optional[Exception] = None
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            with session_class() as session:
                auth_response = session.get(
                    auth_url,
                    params={"token": auth_token, "version": patch_version},
                    timeout=(1.5, 5.0),
                )
                auth_response.raise_for_status()
                auth_payload = auth_response.json()
                proxy_url = str(auth_payload.get("proxy") or "").strip()
                user_agent = str(auth_payload.get("ua") or "").strip()
                if not proxy_url or not user_agent:
                    raise RuntimeError("proxy authorization returned incomplete data")
                request_headers = dict(headers or {})
                request_headers["User-Agent"] = user_agent
                cookie = str(auth_payload.get("cookie") or "").strip()
                if cookie:
                    request_headers["Cookie"] = cookie
                response = session.request(
                    method.upper(),
                    url,
                    headers=request_headers,
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=timeout,
                    **kwargs,
                )
                response.raise_for_status()
                if accept_response is not None and not accept_response(response):
                    raise ProxyResponseRejectedError(
                        "proxy exit returned a rejected response body"
                    )
                return response
        except Exception as exc:
            last_error = exc
            (warning_logger or _logger).warning(
                "akshare proxy fallback attempt failed attempt=%s/%s url_host=%s error_type=%s",
                attempt,
                max(1, int(attempts)),
                url.split("/", 3)[2] if "://" in url else "unknown",
                type(exc).__name__,
            )
    if isinstance(last_error, ProxyResponseRejectedError):
        raise ProxyResponseRejectedError(
            "akshare proxy exits returned rejected response bodies"
        ) from last_error
    raise RuntimeError("akshare proxy fallback exhausted") from last_error


def _install_patch(
    *,
    target: str,
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
        state.error = f"{target} proxy patch is disabled"
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
        state.error = f"{target} proxy patch gateway/auth_token is not fully configured"
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
        state.error = f"Failed to install {target} proxy patch: {exc}"
        if required:
            raise RuntimeError(state.error) from exc
        _logger.warning(state.error)
        return state

    state.ready = True
    state.error = None
    _logger.info("%s proxy patch installed (token=%s***)", target, auth_token[:6])
    return state


def _load_proxy_patch_config(source_name: str) -> Dict[str, Any]:
    config_path = Path(__file__).resolve().parent.parent / "config" / "03_data.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data_config = json.load(f)
    except Exception as exc:
        _logger.warning("Failed to read proxy patch config: %s", exc)
        return {}

    data_sources_cfg = data_config.get("data_sources_config", {}) or {}
    source_cfg = data_sources_cfg.get(source_name, {}) or {}
    return dict(source_cfg.get("proxy_patch", {}) or {})
