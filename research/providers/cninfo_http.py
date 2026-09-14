"""Shared CNInfo HTTP helper that falls back to akshare_proxy_patch on 403."""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import requests

from utils.proxy_patch_runtime import request_with_akshare_proxy


LOGGER = logging.getLogger(__name__)
ProxyRequest = Callable[..., requests.Response]


def is_cninfo_url(url: str) -> bool:
    host = (urlparse(str(url or "")).hostname or "").lower()
    return host == "cninfo.com.cn" or host.endswith(".cninfo.com.cn")


def _accept_cninfo_proxy_response(response: Any) -> bool:
    try:
        payload = response.json()
    except (TypeError, ValueError, requests.JSONDecodeError):
        return False
    return isinstance(payload, (dict, list))


class CninfoProxyFallbackSession:
    """Delegate HTTP calls; CNInfo 403 responses retry through akshare proxy."""

    def __init__(
        self,
        inner: Any,
        *,
        proxy_request: Optional[ProxyRequest] = None,
    ) -> None:
        self._inner = inner
        self._proxy_request = proxy_request or request_with_akshare_proxy
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
        response = self._inner_request(normalized_method, url, **kwargs)
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
            timeout=20.0 if timeout is None else float(timeout),
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
) -> Any:
    """Wrap a session so CNInfo 403s use akshare_proxy_patch."""
    if isinstance(session, CninfoProxyFallbackSession):
        if proxy_request is not None:
            session._proxy_request = proxy_request
        return session
    return CninfoProxyFallbackSession(session, proxy_request=proxy_request)
