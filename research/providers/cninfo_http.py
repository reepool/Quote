"""Shared CNInfo HTTP helper: Chrome TLS first, then akshare proxy on 403."""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import requests

from utils.proxy_patch_runtime import request_with_akshare_proxy


LOGGER = logging.getLogger(__name__)
ProxyRequest = Callable[..., requests.Response]
_UNSET = object()
_THREAD_LOCAL = threading.local()


def is_cninfo_url(url: str) -> bool:
    host = (urlparse(str(url or "")).hostname or "").lower()
    return host == "cninfo.com.cn" or host.endswith(".cninfo.com.cn")


def _accept_cninfo_proxy_response(response: Any) -> bool:
    try:
        payload = response.json()
    except (TypeError, ValueError, requests.JSONDecodeError):
        return False
    return isinstance(payload, (dict, list))


def _normalize_timeout(timeout: Any) -> float:
    if timeout is None:
        return 20.0
    if isinstance(timeout, (tuple, list)):
        if not timeout:
            return 20.0
        return float(timeout[-1] or 20.0)
    return float(timeout)


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
    """Issue one CNInfo request with a Chrome TLS fingerprint."""
    return _impersonated_session().request(
        method,
        url,
        timeout=_normalize_timeout(kwargs.get("timeout")),
        headers=kwargs.get("headers"),
        params=kwargs.get("params"),
        data=kwargs.get("data"),
        json=kwargs.get("json"),
    )


class CninfoProxyFallbackSession:
    """CNInfo calls use Chrome TLS first; HTTP 403 retries through akshare proxy."""

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
            return self._inner_request(normalized_method, url, **kwargs)
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
        return self._inner_request(method, url, **kwargs)

    def _inner_request(self, method: str, url: str, **kwargs: Any) -> Any:
        if method == "GET" and hasattr(self._inner, "get"):
            return self._inner.get(url, **kwargs)
        if method == "POST" and hasattr(self._inner, "post"):
            return self._inner.post(url, **kwargs)
        request = getattr(self._inner, "request", None)
        if request is None:
            raise AttributeError("inner session does not support HTTP requests")
        return request(method, url, **kwargs)

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
    """Wrap a session so CNInfo uses Chrome TLS, then akshare_proxy_patch on 403."""
    if isinstance(session, CninfoProxyFallbackSession):
        if proxy_request is not None:
            session._proxy_request = proxy_request
        if impersonated_request is not _UNSET:
            session._impersonated_request = impersonated_request
        return session
    return CninfoProxyFallbackSession(
        session,
        proxy_request=proxy_request,
        impersonated_request=impersonated_request,
    )


def attach_cninfo_access(
    session: Optional[Any] = None,
    *,
    tls_config: Any = None,
    proxy_request: Optional[ProxyRequest] = None,
) -> Any:
    """Attach CNInfo access policy to a new or caller-injected session."""
    from utils.http_transport import create_requests_session

    if session is None:
        return wrap_cninfo_proxy_fallback(
            create_requests_session(tls_config=tls_config),
            proxy_request=proxy_request,
        )
    return wrap_cninfo_proxy_fallback(
        session,
        proxy_request=proxy_request,
        impersonated_request=None,
    )
