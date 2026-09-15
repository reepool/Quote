"""CNInfo HTTP access mux: headed Chrome or Chrome TLS, then proxy fallback.

``attach_cninfo_access`` is the only production factory. ``chrome_tls`` is a
``curl_cffi`` Chrome TLS fingerprint, not ``headless=true`` Chrome.
``wrap_cninfo_proxy_fallback`` is a test/internal TLS+proxy seam and requires
explicit ``impersonated_request`` and ``proxy_request`` hops; production
callers must not use it as a factory.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import threading
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urlparse

import requests

from utils.proxy_patch_runtime import request_with_akshare_proxy


LOGGER = logging.getLogger(__name__)
ProxyRequest = Callable[..., requests.Response]
_UNSET = object()
_THREAD_LOCAL = threading.local()
_ALLOWED_MODES = frozenset({"headed_chrome", "chrome_tls"})
_FIRST_PARTY_WWW_HOSTS = frozenset({"www.cninfo.com.cn", "cninfo.com.cn"})
_RUNTIME_GUARD = threading.Lock()
_RUNTIME: Optional["_CninfoAccessRuntime"] = None


def is_cninfo_url(url: str) -> bool:
    host = (urlparse(str(url or "")).hostname or "").lower()
    return host == "cninfo.com.cn" or host.endswith(".cninfo.com.cn")


def is_first_party_www_cninfo_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https":
        return False
    host = (parsed.hostname or "").lower()
    return host in _FIRST_PARTY_WWW_HOSTS


def _accept_cninfo_proxy_response(response: Any) -> bool:
    try:
        payload = response.json()
    except (TypeError, ValueError, requests.JSONDecodeError):
        return False
    return isinstance(payload, (dict, list))


def _accept_cninfo_www_proxy_response(url: str) -> Callable[[Any], bool]:
    from research.providers.cninfo_headed_chrome import _accept_proxy_response

    return _accept_proxy_response(url)


def _normalize_timeout(timeout: Any) -> float:
    if timeout is None:
        return 20.0
    if isinstance(timeout, (tuple, list)):
        if not timeout:
            return 20.0
        return float(timeout[-1] or 20.0)
    return float(timeout)


def dispatch_inner_session_request(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    """Call get/post/request on an inner session. Used for non-CNInfo hosts."""
    normalized = str(method or "GET").upper()
    if normalized == "GET" and hasattr(session, "get"):
        return session.get(url, **kwargs)
    if normalized == "POST" and hasattr(session, "post"):
        return session.post(url, **kwargs)
    request = getattr(session, "request", None)
    if request is None:
        raise AttributeError("inner session does not support HTTP requests")
    return request(normalized, url, **kwargs)


def _impersonated_session() -> Any:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is not None:
        return session
    # Import the real class from the submodule. akshare_proxy_patch replaces
    # curl_cffi.requests.Session with a stdlib requests wrapper that rejects
    # impersonate= and would otherwise crash CNInfo announcement scans.
    from curl_cffi.requests.session import Session as ChromeTlsSession

    session = ChromeTlsSession(impersonate="chrome")
    _THREAD_LOCAL.session = session
    return session


def request_cninfo_with_chrome_tls(
    method: str,
    url: str,
    **kwargs: Any,
) -> Any:
    """Issue one CNInfo request with a Chrome TLS fingerprint.

    This is curl_cffi impersonation, not a headless Chrome browser.
    """
    return _impersonated_session().request(
        method,
        url,
        timeout=_normalize_timeout(kwargs.get("timeout")),
        headers=kwargs.get("headers"),
        params=kwargs.get("params"),
        data=kwargs.get("data"),
        json=kwargs.get("json"),
    )


def _validate_preferred_mode(value: Any) -> str:
    mode = str(value or "").strip()
    if mode not in _ALLOWED_MODES:
        raise ValueError(
            "CNInfo access preferred_mode must be 'headed_chrome' or 'chrome_tls', "
            f"got {value!r}"
        )
    return mode


def _config_preferred_mode(sources: Optional[Mapping[str, Any]] = None) -> Optional[str]:
    if sources is None:
        try:
            from utils.config_manager import config_manager

            sources = getattr(config_manager.research_config, "sources", None) or {}
        except Exception:
            return None
    if not isinstance(sources, Mapping):
        return None
    cninfo = sources.get("cninfo")
    if not isinstance(cninfo, Mapping):
        return None
    access = cninfo.get("access")
    if not isinstance(access, Mapping):
        return None
    mode = access.get("preferred_mode")
    if mode is None or not str(mode).strip():
        return None
    return str(mode).strip()


def resolve_cninfo_access_mode(
    preferred_mode: Optional[str] = None,
    *,
    environ: Optional[Mapping[str, str]] = None,
    sources: Optional[Mapping[str, Any]] = None,
) -> str:
    """Resolve attach argument, then env, then config, then headed_chrome."""
    if preferred_mode is not None:
        return _validate_preferred_mode(preferred_mode)
    env = (environ if environ is not None else os.environ).get(
        "QUOTE_CNINFO_ACCESS_MODE",
        "",
    )
    if str(env).strip():
        return _validate_preferred_mode(env)
    config_mode = _config_preferred_mode(sources)
    if config_mode is not None:
        return _validate_preferred_mode(config_mode)
    return "headed_chrome"


class _CninfoAccessRuntime:
    """Process-scoped headed Chrome and www sticky fallback."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.www_sticky: Optional[str] = None
        self.headed: Any = _UNSET

    def install_headed(self, hop: Any) -> None:
        self.headed = hop

    def get_or_create_headed(self) -> Any:
        if self.headed is None:
            return None
        if self.headed is not _UNSET:
            return self.headed
        from research.providers.cninfo_headed_chrome import (
            CninfoHeadedChromeConfigError,
            create_cninfo_headed_chrome_access,
        )

        try:
            self.headed = create_cninfo_headed_chrome_access()
        except CninfoHeadedChromeConfigError:
            raise
        except Exception:
            LOGGER.warning(
                "[CninfoHttp] headed Chrome runtime could not be constructed",
                exc_info=True,
            )
            self.headed = None
            return None
        return self.headed

    def stop_headed(self) -> None:
        hop = None if self.headed is _UNSET else self.headed
        self.headed = None
        if hop is None:
            return
        closer = getattr(hop, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                LOGGER.debug("[CninfoHttp] headed Chrome close failed", exc_info=True)

    def close_quietly(self) -> None:
        try:
            self.stop_headed()
        except Exception:
            LOGGER.debug("[CninfoHttp] runtime close failed", exc_info=True)


def _runtime() -> _CninfoAccessRuntime:
    global _RUNTIME
    with _RUNTIME_GUARD:
        if _RUNTIME is None:
            _RUNTIME = _CninfoAccessRuntime()
        return _RUNTIME


def reset_cninfo_access_runtime() -> None:
    """Drop the shared mux runtime so tests do not share Chrome or sticky state."""
    global _RUNTIME
    with _RUNTIME_GUARD:
        previous = _RUNTIME
        _RUNTIME = _CninfoAccessRuntime()
    if previous is not None:
        previous.close_quietly()


def _decorate_response(response: Any, *, access_mode: str, url: str = "") -> Any:
    from research.providers.cninfo_headed_chrome import CninfoAccessResponse

    if isinstance(response, CninfoAccessResponse):
        response.access_mode = access_mode
        if not getattr(response, "reason", ""):
            from research.providers.cninfo_headed_chrome import http_reason_for_status

            response.reason = http_reason_for_status(int(getattr(response, "status_code", 0) or 0))
        return response
    return CninfoAccessResponse.from_raw(response, access_mode=access_mode, url=url)


class CninfoProxyFallbackSession:
    """CNInfo calls use Chrome TLS first; HTTP 403 retries through akshare proxy.

    Test/internal seam only. Production code must call ``attach_cninfo_access``.
    """

    def __init__(
        self,
        inner: Any,
        *,
        proxy_request: Optional[ProxyRequest] = None,
        impersonated_request: Any = _UNSET,
    ) -> None:
        self._inner = inner
        self._proxy_request = proxy_request or request_with_akshare_proxy
        self._impersonated_request = (
            request_cninfo_with_chrome_tls
            if impersonated_request is _UNSET
            else impersonated_request
        )
        self._prefer_proxy = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def get(self, url: str, **kwargs: Any) -> Any:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        return self.request("POST", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> Any:
        normalized_method = str(method or "GET").upper()
        if not is_cninfo_url(url):
            return dispatch_inner_session_request(self._inner, normalized_method, url, **kwargs)
        if self._prefer_proxy:
            return self._request_via_proxy(normalized_method, url, **kwargs)
        try:
            response = self._request_direct(normalized_method, url, **kwargs)
        except Exception as exc:
            if self._impersonated_request is None:
                raise
            LOGGER.warning(
                "[CninfoHttp] Chrome TLS direct request failed: %s",
                type(exc).__name__,
            )
            try:
                proxy_response = self._request_via_proxy(normalized_method, url, **kwargs)
            except Exception as proxy_exc:
                LOGGER.warning(
                    "[CninfoHttp] akshare proxy fallback failed after Chrome TLS error: %s",
                    type(proxy_exc).__name__,
                )
                raise exc
            self._prefer_proxy = True
            LOGGER.info(
                "[CninfoHttp] Chrome TLS unavailable; using akshare_proxy_patch"
            )
            return proxy_response
        if getattr(response, "status_code", 200) != 403:
            return response
        try:
            proxy_response = self._request_via_proxy(normalized_method, url, **kwargs)
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHttp] akshare proxy fallback failed after HTTP 403: %s",
                type(exc).__name__,
            )
            return response
        self._prefer_proxy = True
        LOGGER.info(
            "[CninfoHttp] direct CNInfo blocked with HTTP 403; using akshare_proxy_patch"
        )
        return proxy_response

    def _request_direct(self, method: str, url: str, **kwargs: Any) -> Any:
        if self._impersonated_request is not None:
            return self._impersonated_request(method, url, **kwargs)
        return dispatch_inner_session_request(self._inner, method, url, **kwargs)

    def _request_via_proxy(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        timeout = kwargs.get("timeout")
        return self._proxy_request(
            method,
            url,
            attempts=3,
            timeout=20.0 if timeout is None else _normalize_timeout(timeout),
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
            data=kwargs.get("data"),
            json=kwargs.get("json"),
            accept_response=_accept_cninfo_proxy_response,
            warning_logger=LOGGER,
        )


def wrap_cninfo_proxy_fallback(
    session: Any,
    *,
    proxy_request: Optional[ProxyRequest] = None,
    impersonated_request: Any = _UNSET,
) -> Any:
    """TLS-then-proxy test/internal seam. Production code must call attach()."""
    if isinstance(session, CninfoProxyFallbackSession):
        if proxy_request is not None:
            session._proxy_request = proxy_request
        if impersonated_request is not _UNSET:
            session._impersonated_request = impersonated_request
        return session
    if impersonated_request is _UNSET or proxy_request is None:
        raise TypeError(
            "wrap_cninfo_proxy_fallback is a test/internal seam; pass "
            "impersonated_request and proxy_request explicitly. "
            "Production code must call attach_cninfo_access."
        )
    return CninfoProxyFallbackSession(
        session,
        proxy_request=proxy_request,
        impersonated_request=impersonated_request,
    )


class CninfoAccessMux:
    """Session-shaped CNInfo access: host/path routing plus runtime fallback."""

    def __init__(
        self,
        inner: Any,
        *,
        preferred_mode: str,
        impersonated_request: Callable[..., Any],
        proxy_request: ProxyRequest,
    ) -> None:
        self._inner = inner
        self._preferred_mode = preferred_mode
        self._impersonated_request = impersonated_request
        self._proxy_request = proxy_request

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def get(self, url: str, **kwargs: Any) -> Any:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        return self.request("POST", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> Any:
        normalized = str(method or "GET").upper()
        if not is_cninfo_url(url):
            return dispatch_inner_session_request(self._inner, normalized, url, **kwargs)
        if is_first_party_www_cninfo_url(url):
            return self._request_first_party_www(normalized, url, **kwargs)
        return self._request_legacy(
            normalized,
            url,
            accept_response=_accept_cninfo_proxy_response,
            sticky_www=False,
            **kwargs,
        )

    def _request_first_party_www(self, method: str, url: str, **kwargs: Any) -> Any:
        runtime = _runtime()
        with runtime.lock:
            sticky = runtime.www_sticky
        if sticky == "proxy":
            return self._proxy_www(method, url, **kwargs)

        from research.providers.cninfo_headed_chrome import (
            is_allowed_cninfo_headed_chrome_url,
        )

        allowlisted = is_allowed_cninfo_headed_chrome_url(url)
        if (
            self._preferred_mode == "headed_chrome"
            and allowlisted
            and sticky is None
        ):
            outcome = self._headed_fetch(method, url, **kwargs)
            if outcome is None:
                with runtime.lock:
                    later_sticky = runtime.www_sticky
                if later_sticky == "proxy":
                    return self._proxy_www(method, url, **kwargs)
                return self._request_legacy_www(method, url, **kwargs)
            if outcome.status == "success" and outcome.response is not None:
                return _decorate_response(
                    outcome.response,
                    access_mode="headed_chrome",
                    url=url,
                )
            if outcome.status == "chrome_blocked":
                try:
                    return self._proxy_www(method, url, **kwargs)
                except Exception:
                    if outcome.response is not None:
                        return outcome.response
                    raise
            return self._request_legacy_www(method, url, **kwargs)
        return self._request_legacy_www(method, url, **kwargs)

    def _headed_fetch(self, method: str, url: str, **kwargs: Any) -> Any:
        from research.providers.cninfo_headed_chrome import CninfoHeadedChromeConfigError

        def run() -> Any:
            runtime = _runtime()
            with runtime.lock:
                if runtime.www_sticky == "proxy":
                    return None
                if runtime.www_sticky == "chrome_tls":
                    return None
                hop = runtime.get_or_create_headed()
                if hop is None:
                    runtime.www_sticky = "chrome_tls"
                    return None
                try:
                    outcome = hop.fetch_allowlisted(method, url, **kwargs)
                except CninfoHeadedChromeConfigError:
                    raise
                if outcome.status == "chrome_blocked":
                    runtime.www_sticky = "proxy"
                    runtime.stop_headed()
                    LOGGER.info(
                        "[CninfoHttp] headed Chrome blocked by Wangsu; "
                        "using akshare_proxy_patch for first-party www"
                    )
                    return outcome
                if outcome.status == "chrome_unavailable":
                    runtime.www_sticky = "chrome_tls"
                    runtime.stop_headed()
                    LOGGER.info(
                        "[CninfoHttp] headed Chrome unavailable; "
                        "using chrome_tls stack for first-party www"
                    )
                    return outcome
                return outcome

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return run()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(run).result()

    def _request_legacy_www(self, method: str, url: str, **kwargs: Any) -> Any:
        return self._request_legacy(
            method,
            url,
            accept_response=_accept_cninfo_www_proxy_response(url),
            sticky_www=True,
            **kwargs,
        )

    def _request_legacy(
        self,
        method: str,
        url: str,
        *,
        accept_response: Callable[[Any], bool],
        sticky_www: bool,
        **kwargs: Any,
    ) -> Any:
        try:
            response = self._impersonated_request(method, url, **kwargs)
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHttp] Chrome TLS request failed: %s",
                type(exc).__name__,
            )
            try:
                proxy_response = self._request_via_proxy(
                    method,
                    url,
                    accept_response=accept_response,
                    **kwargs,
                )
            except Exception as proxy_exc:
                LOGGER.warning(
                    "[CninfoHttp] akshare proxy fallback failed after Chrome TLS error: %s",
                    type(proxy_exc).__name__,
                )
                raise exc
            if sticky_www:
                _runtime().www_sticky = "proxy"
            LOGGER.info("[CninfoHttp] Chrome TLS unavailable; using akshare_proxy_patch")
            return _decorate_response(proxy_response, access_mode="proxy_patch", url=url)
        if getattr(response, "status_code", 200) != 403:
            return _decorate_response(response, access_mode="chrome_tls", url=url)
        try:
            proxy_response = self._request_via_proxy(
                method,
                url,
                accept_response=accept_response,
                **kwargs,
            )
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHttp] akshare proxy fallback failed after HTTP 403: %s",
                type(exc).__name__,
            )
            return _decorate_response(response, access_mode="chrome_tls", url=url)
        if sticky_www:
            _runtime().www_sticky = "proxy"
        LOGGER.info(
            "[CninfoHttp] Chrome TLS blocked with HTTP 403; using akshare_proxy_patch"
        )
        return _decorate_response(proxy_response, access_mode="proxy_patch", url=url)

    def _proxy_www(self, method: str, url: str, **kwargs: Any) -> Any:
        return _decorate_response(
            self._request_via_proxy(
                method,
                url,
                accept_response=_accept_cninfo_www_proxy_response(url),
                **kwargs,
            ),
            access_mode="proxy_patch",
            url=url,
        )

    def _request_via_proxy(
        self,
        method: str,
        url: str,
        *,
        accept_response: Callable[[Any], bool],
        **kwargs: Any,
    ) -> Any:
        timeout = kwargs.get("timeout")
        return self._proxy_request(
            method,
            url,
            attempts=3,
            timeout=20.0 if timeout is None else _normalize_timeout(timeout),
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
            data=kwargs.get("data"),
            json=kwargs.get("json"),
            accept_response=accept_response,
            warning_logger=LOGGER,
        )


def attach_cninfo_access(
    session: Optional[Any] = None,
    *,
    tls_config: Any = None,
    proxy_request: Optional[ProxyRequest] = None,
    preferred_mode: Optional[str] = None,
    impersonated_request: Any = _UNSET,
    headed_hop: Any = _UNSET,
    environ: Optional[Mapping[str, str]] = None,
    sources: Optional[Mapping[str, Any]] = None,
) -> Any:
    """Attach the unified CNInfo access mux to a new or caller-injected session."""
    from utils.http_transport import create_requests_session

    mode = resolve_cninfo_access_mode(
        preferred_mode,
        environ=environ,
        sources=sources,
    )
    inner = session if session is not None else create_requests_session(tls_config=tls_config)
    tls_hop = (
        request_cninfo_with_chrome_tls
        if impersonated_request is _UNSET or impersonated_request is None
        else impersonated_request
    )
    if headed_hop is not _UNSET:
        _runtime().install_headed(headed_hop)
    return CninfoAccessMux(
        inner,
        preferred_mode=mode,
        impersonated_request=tls_hop,
        proxy_request=proxy_request or request_with_akshare_proxy,
    )
