"""Instance-owned headed Chrome hop for first-party www and static CNInfo HTTP.

Production jobs go through ``attach_cninfo_access``. This module reports
``success`` / ``chrome_blocked`` / ``chrome_unavailable`` via
``fetch_allowlisted``; the mux owns fallback. Standalone ``request()`` may
still jump headed-to-proxy for hop unit tests. HTTPS
``static.cninfo.com.cn`` attachments use an in-page binary fast path, then a
same-Chrome document-level read. In-page 403 / empty / CORS is not a finished
Wangsu block. The headed size ceiling is the official annual-report bound
(200 MiB), not the 50 MiB XDXR default.
"""

from __future__ import annotations

import asyncio
import base64
import http.client
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from typing import Any, Callable, Mapping, MutableMapping, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from utils.proxy_patch_runtime import request_with_akshare_proxy


LOGGER = logging.getLogger(__name__)

CNINFO_HOMEPAGE = "https://www.cninfo.com.cn/"
_ALLOWED_HOSTS = frozenset({"www.cninfo.com.cn", "cninfo.com.cn"})
_ALLOWED_EXACT_PATHS = frozenset(
    {
        "/",
        "/new/hisAnnouncement/query",
        "/new/information/topSearch/query",
    }
)
_ALLOWED_PREFIXES = ("/data20/", "/new/disclosure/")
_STATIC_HOST = "static.cninfo.com.cn"
_STATIC_MAX_BYTES = 200 * 1024 * 1024
_IN_PAGE_MAX_BYTES = 2 * 1024 * 1024
_DOCUMENT_TIMEOUT = 120.0
_DEFAULT_TIMEOUT = 20.0
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
ProxyRequest = Callable[..., Any]


class CninfoHeadedChromeConfigError(ValueError):
    """Raised when headed Chrome is misconfigured (for example headless=true)."""


class CninfoHeadedChromeUrlError(ValueError):
    """Raised when a URL is outside the headed CNInfo allowlist."""


class _ChromeUnusable(RuntimeError):
    """Headed Chrome cannot serve in-page requests on this instance."""


def _default_chrome_path() -> str:
    configured = str(os.environ.get("QUOTE_DCE_CHROME_PATH") or "").strip()
    if configured:
        return configured
    for path in (
        "/opt/google/chrome/chrome",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ):
        if os.path.exists(path):
            return path
    return ""


def _normalize_path(url: str) -> str:
    path = urlparse(str(url or "")).path or "/"
    if not path.startswith("/"):
        return "/" + path
    return path


def is_allowed_cninfo_headed_static_url(url: str) -> bool:
    """Return True for https://static.cninfo.com.cn/ (any path)."""
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https":
        return False
    return (parsed.hostname or "").lower() == _STATIC_HOST


def is_allowed_cninfo_headed_chrome_url(url: str) -> bool:
    """Return True for allowlisted www paths or HTTPS static CNInfo attachments."""
    if is_allowed_cninfo_headed_static_url(url):
        return True
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https":
        return False
    host = (parsed.hostname or "").lower()
    if host not in _ALLOWED_HOSTS:
        return False
    path = _normalize_path(url)
    if path in _ALLOWED_EXACT_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in _ALLOWED_PREFIXES)


def _is_homepage_url(url: str) -> bool:
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https":
        return False
    host = (parsed.hostname or "").lower()
    return host in _ALLOWED_HOSTS and _normalize_path(url) == "/"


def _is_disclosure_html_url(url: str) -> bool:
    return _normalize_path(url).startswith("/new/disclosure/")


def _static_media_type(headers: Mapping[str, Any]) -> str:
    if headers is None:
        return ""
    getter = getattr(headers, "get", None)
    raw = ""
    if callable(getter):
        raw = getter("Content-Type") or getter("content-type") or ""
    return str(raw).split(";", 1)[0].strip().lower()


def _is_trusted_static_html(body: bytes, media_type: str = "") -> bool:
    if not body:
        return False
    snippet = body[:4000].decode("utf-8", "replace")
    if _is_wangsu_block_page(text=snippet):
        return False
    prefix = body[:2048].lstrip().lower()
    if media_type in {"text/html", "application/xhtml+xml"}:
        return True
    return prefix.startswith((b"<!doctype html", b"<html", b"<head", b"<body"))


def _is_usable_static_attachment(body: bytes, media_type: str = "") -> bool:
    if body.startswith(b"%PDF-"):
        return True
    return _is_trusted_static_html(body, media_type)


def _is_in_page_cors_or_opaque(
    result: Optional[Mapping[str, Any]],
    exc: Optional[BaseException] = None,
) -> bool:
    markers = ("failed to fetch", "cors", "networkerror", "opaque", "access-control")
    if exc is not None:
        text = str(exc).lower()
        return any(marker in text for marker in markers)
    if result is None:
        return True
    try:
        status = int(result.get("status") or 0)
    except (TypeError, ValueError):
        status = 0
    if status <= 0:
        return True
    rtype = str(result.get("type") or "").lower()
    if rtype in {"opaque", "opaqueredirect", "error"}:
        return True
    text = str(result.get("text") or "").lower()
    return any(marker in text for marker in markers)


def _static_body_bytes(result: Mapping[str, Any]) -> bytes:
    body = result.get("body")
    if body is None:
        body = str(result.get("text") or "").encode("utf-8")
    return bytes(body or b"")


def _is_redirect_status(status: int) -> bool:
    return status in _REDIRECT_STATUSES or 300 <= status < 400


def _static_result_as_413(result: Mapping[str, Any], url: str) -> dict[str, Any]:
    return {
        "status": 413,
        "url": str(result.get("url") or url),
        "headers": dict(result.get("headers") or {}),
        "text": "",
        "body": b"",
        "path": str(result.get("path") or "document_fetch"),
        "reason": "Payload Too Large",
    }


def _static_off_host_redirect(url: str, final: str) -> dict[str, Any]:
    return {
        "status": 302,
        "url": url,
        "headers": {"Location": final},
        "text": "",
        "body": b"",
        "path": "document_fetch",
        "reason": "Found",
    }


def _static_in_page_needs_document_read(
    result: Optional[Mapping[str, Any]],
    *,
    allow_redirects: bool = True,
) -> bool:
    """True unless in-page already has a small usable body, 404/410, or 3xx."""
    if result is None or result.get("oversized") or _is_in_page_cors_or_opaque(result):
        return True
    try:
        status = int(result.get("status") or 0)
    except (TypeError, ValueError):
        return True
    if status in {404, 410}:
        return False
    if _is_redirect_status(status) and allow_redirects is False:
        return False
    content = _static_body_bytes(result)
    if len(content) > _IN_PAGE_MAX_BYTES:
        return True
    media = _static_media_type(dict(result.get("headers") or {}))
    text = str(result.get("text") or "") or content[:4000].decode("utf-8", "replace")
    if status in {200, 206} and _is_usable_static_attachment(content, media):
        return False
    if status in {200, 206} and not content:
        return True
    if status == 403 or _is_wangsu_block_page(status=status, text=text):
        return True
    return True


def _static_document_is_finished(result: Optional[Mapping[str, Any]]) -> bool:
    """True when Fetch already has a definitive status or a usable/blocked body."""
    if result is None:
        return False
    try:
        status = int(result.get("status") or 0)
    except (TypeError, ValueError):
        return False
    if status in {404, 410, 413} or _is_redirect_status(status) or status == 403:
        return True
    content = _static_body_bytes(result)
    media = _static_media_type(dict(result.get("headers") or {}))
    text = str(result.get("text") or "") or content[:4000].decode("utf-8", "replace")
    if result.get("oversized") or len(content) > _STATIC_MAX_BYTES:
        return True
    if _is_usable_static_attachment(content, media):
        return True
    if _is_wangsu_block_page(status=status, text=text):
        return True
    return False


def _classify_static_headed_result(
    result: Mapping[str, Any],
    *,
    url: str = "",
) -> "CninfoHeadedFetchOutcome":
    try:
        status = int(result.get("status") or 0)
    except (TypeError, ValueError):
        status = 0
    content = _static_body_bytes(result)
    media = _static_media_type(dict(result.get("headers") or {}))
    text = str(result.get("text") or "") or content[:4000].decode("utf-8", "replace")
    if result.get("oversized") or len(content) > _STATIC_MAX_BYTES:
        result = _static_result_as_413(result, url)
        status = 413
        content = b""
        text = ""
    response = CninfoAccessResponse.from_fetch(result, access_mode="headed_chrome")
    if status in {404, 410, 413} or _is_redirect_status(status):
        return CninfoHeadedFetchOutcome("success", response)
    if status == 403 or _is_wangsu_block_page(status=status, text=text):
        return CninfoHeadedFetchOutcome("chrome_blocked", response)
    if status in {200, 206}:
        if _is_usable_static_attachment(content, media):
            return CninfoHeadedFetchOutcome("success", response)
        return CninfoHeadedFetchOutcome("chrome_blocked", response)
    return CninfoHeadedFetchOutcome("chrome_blocked", response)


def _is_wangsu_block_page(
    *,
    status: Any = None,
    title: str = "",
    text: str = "",
) -> bool:
    try:
        if int(status or 0) == 403:
            return True
    except (TypeError, ValueError):
        pass
    blob = f"{title}\n{text}"[:4000].lower()
    return "403 forbidden" in blob or "ws-action" in blob


def _is_dead_session(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "target closed",
            "target navigated or closed",
            "browser has been closed",
            "session closed",
            "connection closed",
        )
    )


def _normalize_timeout(timeout: Any) -> float:
    if timeout is None:
        return _DEFAULT_TIMEOUT
    if isinstance(timeout, (tuple, list)):
        if not timeout:
            return _DEFAULT_TIMEOUT
        return float(timeout[-1] or _DEFAULT_TIMEOUT)
    return float(timeout)


def _document_timeout(timeout: Any) -> float:
    return max(_DOCUMENT_TIMEOUT, _normalize_timeout(timeout))


def _fetch_header_map(headers: Any) -> dict[str, str]:
    mapped: dict[str, str] = {}
    if not headers:
        return mapped
    if isinstance(headers, Mapping):
        return {str(key): str(value) for key, value in headers.items()}
    for item in headers:
        name = getattr(item, "name", None)
        value = getattr(item, "value", None)
        if name is None and isinstance(item, (tuple, list)) and len(item) >= 2:
            name, value = item[0], item[1]
        if name is not None:
            mapped[str(name)] = str(value or "")
    return mapped


def _apply_params(url: str, params: Optional[Mapping[str, Any]]) -> str:
    if not params:
        return url
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({str(key): value for key, value in params.items()})
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def _accept_proxy_response(url: str) -> Callable[[Any], bool]:
    def accept(response: Any) -> bool:
        if _is_disclosure_html_url(url):
            headers = getattr(response, "headers", None) or {}
            content_type = ""
            if hasattr(headers, "get"):
                content_type = str(
                    headers.get("Content-Type") or headers.get("content-type") or ""
                )
            text = str(getattr(response, "text", "") or "")
            snippet = text[:500].lower()
            if "html" in content_type.lower() or "<html" in snippet or "<!doctype" in snippet:
                return True
        try:
            payload = response.json()
        except (TypeError, ValueError):
            return False
        return isinstance(payload, (dict, list))

    return accept


def http_reason_for_status(status_code: int) -> str:
    try:
        return str(http.client.responses.get(int(status_code), "") or "")
    except (TypeError, ValueError):
        return ""


class _CaseInsensitiveHeaders(dict):
    """dict with case-insensitive ``get`` for Content-Type and similar headers."""

    def get(self, key, default=None):  # type: ignore[override]
        if key in self:
            return dict.get(self, key, default)
        needle = str(key).lower()
        for existing, value in self.items():
            if str(existing).lower() == needle:
                return value
        return default


class CninfoHeadedFetchOutcome:
    """Headed hop result. The production mux owns fallback from ``status``."""

    __slots__ = ("status", "response")

    def __init__(
        self,
        status: str,
        response: Optional["CninfoAccessResponse"] = None,
    ) -> None:
        if status not in {"success", "chrome_blocked", "chrome_unavailable"}:
            raise ValueError(f"unsupported headed fetch status: {status}")
        self.status = status
        self.response = response


class CninfoAccessResponse:
    """Session-shaped response with an explicit CNInfo access mode."""

    def __init__(
        self,
        *,
        status_code: int,
        url: str,
        headers: Mapping[str, Any],
        text: str,
        content: bytes,
        access_mode: str,
        raw: Any = None,
        reason: str = "",
    ) -> None:
        self.status_code = int(status_code)
        self.url = str(url or "")
        self.headers = _CaseInsensitiveHeaders(headers or {})
        self.text = str(text or "")
        self.content = bytes(content or b"")
        self.access_mode = str(access_mode)
        self.reason = str(reason or "") or http_reason_for_status(self.status_code)
        self._raw = raw

    @classmethod
    def from_fetch(cls, result: Mapping[str, Any], *, access_mode: str) -> "CninfoAccessResponse":
        text = str(result.get("text") or "")
        body = result.get("body")
        if body is None:
            body = text.encode("utf-8")
        status_code = int(result.get("status") or 0)
        return cls(
            status_code=status_code,
            url=str(result.get("url") or ""),
            headers=dict(result.get("headers") or {}),
            text=text,
            content=bytes(body),
            access_mode=access_mode,
            reason=str(result.get("reason") or "") or http_reason_for_status(status_code),
        )

    @classmethod
    def from_raw(cls, response: Any, *, access_mode: str, url: str) -> "CninfoAccessResponse":
        text = str(getattr(response, "text", "") or "")
        content = getattr(response, "content", None)
        if content is None:
            content = text.encode("utf-8")
        status_code = int(getattr(response, "status_code", 0) or 0)
        reason = str(getattr(response, "reason", "") or "")
        return cls(
            status_code=status_code,
            url=str(getattr(response, "url", "") or url),
            headers=dict(getattr(response, "headers", {}) or {}),
            text=text,
            content=bytes(content or b""),
            access_mode=access_mode,
            raw=response,
            reason=reason or http_reason_for_status(status_code),
        )

    def json(self) -> Any:
        if self._raw is not None:
            raw_json = getattr(self._raw, "json", None)
            if callable(raw_json):
                return raw_json()
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self._raw is not None:
            raw_raise = getattr(self._raw, "raise_for_status", None)
            if callable(raw_raise):
                raw_raise()
                return
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code} {self.reason}".strip())
            error.response = self
            raise error


class _NodriverPageSession:
    """Real headed Chrome homepage + in-page fetch. Not a DCE client."""

    def __init__(
        self,
        *,
        browser_executable_path: str = "",
        timeout_seconds: float = _DEFAULT_TIMEOUT,
    ) -> None:
        self.browser_executable_path = browser_executable_path
        self.timeout_seconds = timeout_seconds
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._browser: Any = None
        self._page: Any = None
        self._document_tab: Any = None
        self._document_capture: Optional[dict[str, Any]] = None
        self._document_event: Optional[asyncio.Event] = None
        self._document_allow_redirects = True
        self._document_target_url = ""
        self._document_token = 0
        self._download_dir: Optional[str] = None
        self._download_event: Optional[asyncio.Event] = None
        self._download_path: Optional[str] = None
        self._download_behavior_ready = False

    def _run(self, coro: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            coro.close()
            raise RuntimeError(
                "CNInfo headed Chrome must be called from a worker thread "
                "outside the running asyncio loop"
            )
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
        return self._loop.run_until_complete(coro)

    def start(self, homepage: str) -> dict[str, Any]:
        return self._run(self._async_start(homepage))

    def fetch(self, method: str, url: str, **kwargs: Any) -> Optional[dict[str, Any]]:
        return self._run(self._async_fetch(method, url, **kwargs))

    def fetch_binary_same_chrome(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Optional[dict[str, Any]]:
        return self._run(self._async_fetch_binary_same_chrome(method, url, **kwargs))

    def fetch_static_document(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Optional[dict[str, Any]]:
        return self._run(self._async_fetch_static_document(method, url, **kwargs))

    def close(self) -> None:
        if self._loop is None:
            return
        try:
            self._run(self._async_close())
        except Exception:
            LOGGER.warning("[CninfoHeadedChrome] browser close failed", exc_info=True)
        try:
            self._loop.close()
        except Exception:
            LOGGER.debug("[CninfoHeadedChrome] event loop close failed", exc_info=True)
        self._loop = None
        self._browser = None
        self._page = None
        self._document_tab = None
        self._document_capture = None
        self._document_event = None
        self._download_dir = None
        self._download_event = None
        self._download_path = None
        self._download_behavior_ready = False

    async def _async_start(self, homepage: str) -> dict[str, Any]:
        try:
            import nodriver as uc
        except Exception as exc:
            raise _ChromeUnusable(
                "CNInfo headed Chrome requires optional dependency nodriver"
            ) from exc
        kwargs: dict[str, Any] = {
            "headless": False,
            "browser_args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1920,1080",
            ],
            "sandbox": False,
            "no_sandbox": True,
            "lang": "zh-CN",
        }
        if self.browser_executable_path:
            kwargs["browser_executable_path"] = self.browser_executable_path
        self._browser = await asyncio.wait_for(
            uc.start(**kwargs),
            timeout=self.timeout_seconds,
        )
        self._page = await asyncio.wait_for(
            self._browser.get(homepage),
            timeout=self.timeout_seconds,
        )
        title = await self._evaluate_value("document.title")
        html = await self._evaluate_value(
            "document.documentElement ? document.documentElement.outerHTML.slice(0, 4000) : ''"
        )
        return {
            "status": 403 if _is_wangsu_block_page(title=str(title or ""), text=str(html or "")) else 200,
            "title": str(title or ""),
            "text": str(html or ""),
            "url": homepage,
        }

    async def _async_fetch(self, method: str, url: str, **kwargs: Any) -> Optional[dict[str, Any]]:
        if self._page is None:
            raise _ChromeUnusable("headed Chrome page is not started")
        timeout = _normalize_timeout(kwargs.get("timeout"))
        script = self._in_page_fetch_script(method, url, **kwargs)
        raw = await self._evaluate_json(script, timeout=timeout)
        return self._decode_in_page_fetch(raw, url=url, binary=bool(kwargs.get("binary")))

    async def _async_fetch_binary_same_chrome(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Optional[dict[str, Any]]:
        if self._browser is None or self._page is None:
            raise _ChromeUnusable("headed Chrome page is not started")
        timeout = _normalize_timeout(kwargs.get("timeout"))
        loaded = await self._try_cdp_network_resource(url, timeout=timeout)
        if loaded is not None:
            self._assert_static_final_host(loaded, fallback_url=url)
            return loaded
        if str(method or "GET").upper() != "GET":
            raise RuntimeError("same-Chrome static fallback only supports GET")
        extra = None
        try:
            getter = self._browser.get
            try:
                extra = await asyncio.wait_for(getter(url, new_tab=True), timeout=timeout)
            except TypeError:
                extra = await asyncio.wait_for(getter(url), timeout=timeout)
            kwargs = dict(kwargs)
            kwargs["binary"] = True
            script = self._in_page_fetch_script(method, url, **kwargs)
            raw = await self._evaluate_json_on(extra, script, timeout=timeout)
            result = self._decode_in_page_fetch(raw, url=url, binary=True)
            if result is None:
                raise RuntimeError("same-Chrome tab returned no static body")
            self._assert_static_final_host(result, fallback_url=url)
            return result
        finally:
            await self._close_extra_tab(extra)

    async def _async_fetch_static_document(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Optional[dict[str, Any]]:
        if self._browser is None or self._page is None:
            raise _ChromeUnusable("headed Chrome page is not started")
        if str(method or "GET").upper() != "GET":
            raise RuntimeError("document-level static read only supports GET")
        timeout = _document_timeout(kwargs.get("timeout"))
        allow_redirects = kwargs.get("allow_redirects", True) is not False
        extra = None
        download_dir: Optional[str] = None
        self._document_token += 1
        token = self._document_token
        self._document_capture = None
        self._document_event = asyncio.Event()
        self._document_allow_redirects = allow_redirects
        self._document_target_url = url
        self._download_event = asyncio.Event()
        self._download_path = None
        started = time.monotonic()
        try:
            download_dir = tempfile.mkdtemp(prefix="cninfo-headed-static-")
            self._download_dir = download_dir
            extra = await self._open_static_document_tab(url, timeout=timeout)
            await self._wait_static_document(timeout)
            captured = self._document_capture
            if token == self._document_token and not _static_document_is_finished(captured):
                remaining = max(0.1, timeout - (time.monotonic() - started))
                await self._wait_download_after_empty_fetch(remaining)
            downloaded = self._read_download_result(url)
            if downloaded is not None and (
                not _static_document_is_finished(captured)
                or _is_usable_static_attachment(_static_body_bytes(downloaded))
            ):
                return downloaded
            if captured is not None:
                return captured
            if downloaded is not None:
                return downloaded
            return {
                "status": 200,
                "url": url,
                "headers": {},
                "text": "",
                "body": b"",
                "path": "document_fetch",
            }
        finally:
            await self._close_extra_tab(extra)
            self._document_token += 1
            self._document_tab = None
            self._document_event = None
            self._document_capture = None
            self._download_event = None
            self._download_path = None
            self._download_dir = None
            if download_dir:
                shutil.rmtree(download_dir, ignore_errors=True)

    async def _open_static_document_tab(self, url: str, *, timeout: float) -> Any:
        getter = getattr(self._browser, "get", None)
        if not callable(getter):
            raise _ChromeUnusable("headed Chrome cannot open a static document tab")
        try:
            extra = await asyncio.wait_for(getter("about:blank", new_tab=True), timeout=timeout)
        except TypeError as exc:
            raise _ChromeUnusable(
                "headed Chrome cannot open a static document tab"
            ) from exc
        self._document_tab = extra
        await self._enable_static_fetch_on(extra)
        await self._ensure_download_behavior()
        try:
            from nodriver import cdp
        except Exception as exc:
            raise _ChromeUnusable("headed Chrome CDP is unavailable") from exc
        send = getattr(extra, "send", None)
        if callable(send):
            await asyncio.wait_for(
                send(cdp.page.navigate(url=url, referrer=CNINFO_HOMEPAGE)),
                timeout=timeout,
            )
        else:
            await asyncio.wait_for(extra.get(url), timeout=timeout)
        return extra

    async def _enable_static_fetch_on(self, tab: Any) -> None:
        try:
            from nodriver import cdp
        except Exception:
            return
        add_handler = getattr(tab, "add_handler", None)
        if callable(add_handler):
            add_handler(cdp.fetch.RequestPaused, self._on_static_fetch_paused)
        send = getattr(tab, "send", None)
        if not callable(send):
            return
        await send(
            cdp.fetch.enable(
                patterns=[
                    cdp.fetch.RequestPattern(
                        url_pattern="https://static.cninfo.com.cn/*",
                        request_stage=cdp.fetch.RequestStage.RESPONSE,
                    )
                ]
            )
        )

    async def _ensure_download_behavior(self) -> None:
        if not self._download_dir:
            return
        try:
            from nodriver import cdp
        except Exception:
            return
        send = getattr(self._browser, "send", None) or getattr(self._page, "send", None)
        if not callable(send):
            return
        try:
            await send(
                cdp.browser.set_download_behavior(
                    behavior="allow",
                    download_path=self._download_dir,
                    events_enabled=True,
                )
            )
        except Exception:
            LOGGER.debug(
                "[CninfoHeadedChrome] setDownloadBehavior failed",
                exc_info=True,
            )
            return
        if self._download_behavior_ready:
            return
        add_handler = getattr(self._browser, "add_handler", None) or getattr(
            self._page, "add_handler", None
        )
        if callable(add_handler):
            add_handler(cdp.browser.DownloadProgress, self._on_static_download_progress)
        self._download_behavior_ready = True

    async def _wait_static_document(self, timeout: float) -> None:
        event = self._document_event
        download_event = self._download_event
        if event is None:
            return
        waiters = [asyncio.create_task(event.wait())]
        if download_event is not None:
            waiters.append(asyncio.create_task(download_event.wait()))
        try:
            done, pending = await asyncio.wait(
                waiters,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if not done:
                return
        except Exception:
            LOGGER.debug("[CninfoHeadedChrome] document wait failed", exc_info=True)

    async def _wait_download_after_empty_fetch(self, timeout: float) -> None:
        download_event = self._download_event
        if download_event is None or download_event.is_set():
            return
        try:
            await asyncio.wait_for(download_event.wait(), timeout=max(0.1, float(timeout)))
        except Exception:
            LOGGER.debug("[CninfoHeadedChrome] download wait failed", exc_info=True)

    def _set_document_capture(
        self,
        result: Mapping[str, Any],
        *,
        token: int,
        finished: bool = True,
    ) -> None:
        if token != self._document_token:
            return
        if self._document_capture is None:
            self._document_capture = dict(result)
        if not finished:
            return
        if self._document_event is not None and not self._document_event.is_set():
            self._document_event.set()

    async def _on_static_fetch_paused(self, event: Any) -> None:
        token = self._document_token
        tab = self._document_tab
        if tab is None:
            return
        try:
            from nodriver import cdp
        except Exception:
            return
        request = getattr(event, "request", None)
        request_url = str(getattr(request, "url", "") or "")
        request_id = getattr(event, "request_id", None)
        send = getattr(tab, "send", None)
        if not callable(send) or request_id is None:
            return

        async def _continue() -> None:
            try:
                await send(cdp.fetch.continue_request(request_id=request_id))
            except Exception:
                try:
                    await send(cdp.fetch.continue_response(request_id=request_id))
                except Exception:
                    LOGGER.debug(
                        "[CninfoHeadedChrome] Fetch.continue failed",
                        exc_info=True,
                    )

        async def _abort() -> None:
            try:
                await send(
                    cdp.fetch.fail_request(
                        request_id=request_id,
                        error_reason=cdp.network.ErrorReason.ABORTED,
                    )
                )
            except Exception:
                await _continue()

        if token != self._document_token:
            await _continue()
            return
        if request_url and not is_allowed_cninfo_headed_static_url(request_url):
            if urlparse(request_url).hostname:
                self._set_document_capture(
                    _static_off_host_redirect(self._document_target_url, request_url),
                    token=token,
                )
            await _continue()
            return
        status = getattr(event, "response_status_code", None)
        if status is None:
            await _continue()
            return
        headers = _fetch_header_map(getattr(event, "response_headers", None))
        try:
            status_code = int(status)
        except (TypeError, ValueError):
            await _continue()
            return
        if _is_redirect_status(status_code) and self._document_allow_redirects is False:
            self._set_document_capture(
                {
                    "status": status_code,
                    "url": request_url or self._document_target_url,
                    "headers": headers,
                    "text": "",
                    "body": b"",
                    "path": "document_fetch",
                },
                token=token,
            )
            await _abort()
            return
        content_length = headers.get("Content-Length") or headers.get("content-length")
        try:
            announced = int(content_length) if content_length not in (None, "") else 0
        except (TypeError, ValueError):
            announced = 0
        if announced > _STATIC_MAX_BYTES:
            self._set_document_capture(
                _static_result_as_413(
                    {"headers": headers, "url": request_url},
                    request_url or self._document_target_url,
                ),
                token=token,
            )
            await _abort()
            return
        body = b""
        try:
            raw_body, encoded = await send(cdp.fetch.get_response_body(request_id=request_id))
            if encoded:
                body = base64.b64decode(raw_body or "")
            elif isinstance(raw_body, str):
                body = raw_body.encode("utf-8")
            else:
                body = bytes(raw_body or b"")
        except Exception:
            LOGGER.debug(
                "[CninfoHeadedChrome] Fetch.getResponseBody failed",
                exc_info=True,
            )
        if len(body) > _STATIC_MAX_BYTES:
            captured = _static_result_as_413(
                {"headers": headers, "url": request_url},
                request_url or self._document_target_url,
            )
            self._set_document_capture(captured, token=token)
            await _abort()
            return
        captured = {
            "status": status_code,
            "url": request_url or self._document_target_url,
            "headers": headers,
            "text": "",
            "body": body,
            "path": "document_fetch",
        }
        self._set_document_capture(
            captured,
            token=token,
            finished=_static_document_is_finished(captured),
        )
        await _continue()

    def _on_static_download_progress(self, event: Any) -> None:
        state = str(getattr(event, "state", "") or "").lower()
        if state not in {"completed", "complete"}:
            return
        path = str(getattr(event, "file_path", "") or "")
        download_dir = self._download_dir
        if path and download_dir and not os.path.abspath(path).startswith(
            os.path.abspath(download_dir) + os.sep
        ):
            return
        if path:
            self._download_path = path
        if self._download_event is not None and not self._download_event.is_set():
            self._download_event.set()

    def _read_download_result(self, url: str) -> Optional[dict[str, Any]]:
        path = self._download_path
        if not path and self._download_dir:
            try:
                names = [
                    name
                    for name in os.listdir(self._download_dir)
                    if not name.startswith(".") and not name.endswith(".crdownload")
                ]
            except OSError:
                names = []
            if names:
                path = os.path.join(self._download_dir, names[0])
        if not path or not os.path.isfile(path):
            return None
        try:
            size = os.path.getsize(path)
            if size > _STATIC_MAX_BYTES:
                return _static_result_as_413({"url": url}, url)
            with open(path, "rb") as handle:
                body = handle.read(_STATIC_MAX_BYTES + 1)
        except OSError:
            return None
        if len(body) > _STATIC_MAX_BYTES:
            return _static_result_as_413({"url": url}, url)
        return {
            "status": 200,
            "url": url,
            "headers": {},
            "text": "",
            "body": body,
            "path": "document_download",
        }

    def _in_page_fetch_script(self, method: str, url: str, **kwargs: Any) -> str:
        headers = dict(kwargs.get("headers") or {})
        content_type = kwargs.get("content_type")
        binary = bool(kwargs.get("binary"))
        redirect_mode = "follow" if kwargs.get("allow_redirects", True) is not False else "manual"
        max_bytes = int(kwargs.get("max_bytes") or _IN_PAGE_MAX_BYTES)
        return (
            "(async () => {\n"
            f"  const opt = {{ method: {method!r}, credentials: 'include', "
            f"headers: {json.dumps(headers, ensure_ascii=False)}, "
            f"redirect: {redirect_mode!r} }};\n"
            f"  const contentType = {json.dumps(content_type)};\n"
            f"  const jsonBody = {json.dumps(kwargs.get('json'), ensure_ascii=False)};\n"
            f"  const formBody = {json.dumps(kwargs.get('data'), ensure_ascii=False)};\n"
            f"  const binary = {json.dumps(binary)};\n"
            f"  const maxBytes = {max_bytes};\n"
            "  if (contentType === 'application/json' && jsonBody !== null) {\n"
            "    opt.headers['Content-Type'] = 'application/json';\n"
            "    opt.body = JSON.stringify(jsonBody);\n"
            "  } else if (contentType === 'application/x-www-form-urlencoded' && formBody !== null) {\n"
            "    opt.headers['Content-Type'] = 'application/x-www-form-urlencoded';\n"
            "    opt.body = new URLSearchParams(formBody).toString();\n"
            "  }\n"
            "  try {\n"
            f"    const r = await fetch({url!r}, opt);\n"
            "    const headers = {};\n"
            "    r.headers.forEach((value, key) => { headers[key] = value; });\n"
            "    if (binary) {\n"
            "      const announced = parseInt(r.headers.get('content-length') || '0', 10);\n"
            "      if (announced > maxBytes) {\n"
            "        if (r.body && r.body.cancel) { try { r.body.cancel(); } catch (e) {} }\n"
            "        return JSON.stringify({status: r.status, url: r.url, headers, "
            "text: '', body_b64: '', oversized: true, type: r.type});\n"
            "      }\n"
            "      const reader = r.body && r.body.getReader ? r.body.getReader() : null;\n"
            "      if (reader) {\n"
            "        let seen = 0;\n"
            "        const chunks = [];\n"
            "        while (true) {\n"
            "          const step = await reader.read();\n"
            "          if (step.done) break;\n"
            "          seen += step.value.length;\n"
            "          if (seen > maxBytes) {\n"
            "            try { await reader.cancel(); } catch (e) {}\n"
            "            return JSON.stringify({status: r.status, url: r.url, headers, "
            "text: '', body_b64: '', oversized: true, type: r.type});\n"
            "          }\n"
            "          chunks.push(step.value);\n"
            "        }\n"
            "        let binaryStr = '';\n"
            "        const chunk = 0x8000;\n"
            "        for (const part of chunks) {\n"
            "          for (let i = 0; i < part.length; i += chunk) {\n"
            "            binaryStr += String.fromCharCode.apply(null, part.subarray(i, i + chunk));\n"
            "          }\n"
            "        }\n"
            "        return JSON.stringify({status: r.status, url: r.url, headers, "
            "body_b64: btoa(binaryStr), type: r.type});\n"
            "      }\n"
            "      const buf = await r.arrayBuffer();\n"
            "      const bytes = new Uint8Array(buf);\n"
            "      if (bytes.length > maxBytes) {\n"
            "        return JSON.stringify({status: r.status, url: r.url, headers, "
            "text: '', body_b64: '', oversized: true, type: r.type});\n"
            "      }\n"
            "      let binaryStr = '';\n"
            "      const chunk = 0x8000;\n"
            "      for (let i = 0; i < bytes.length; i += chunk) {\n"
            "        binaryStr += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));\n"
            "      }\n"
            "      return JSON.stringify({status: r.status, url: r.url, headers, "
            "body_b64: btoa(binaryStr), type: r.type});\n"
            "    }\n"
            "    const text = await r.text();\n"
            "    return JSON.stringify({status: r.status, url: r.url, headers, text, type: r.type});\n"
            "  } catch (error) {\n"
            "    return JSON.stringify({status: -1, url: '', headers: {}, text: String(error), type: 'error'});\n"
            "  }\n"
            "})()"
        )

    def _decode_in_page_fetch(
        self,
        raw: Optional[Mapping[str, Any]],
        *,
        url: str,
        binary: bool,
    ) -> Optional[dict[str, Any]]:
        if raw is None:
            return None
        status = int(raw.get("status") or 0)
        text = str(raw.get("text") or "")
        if status <= 0:
            if binary:
                return {
                    "status": status,
                    "url": str(raw.get("url") or url),
                    "headers": dict(raw.get("headers") or {}),
                    "text": text,
                    "body": b"",
                    "type": str(raw.get("type") or ""),
                }
            raise TimeoutError(text or "in-page fetch failed")
        if binary:
            if raw.get("oversized"):
                return {
                    "status": status,
                    "url": str(raw.get("url") or url),
                    "headers": dict(raw.get("headers") or {}),
                    "text": "",
                    "body": b"",
                    "type": str(raw.get("type") or ""),
                    "oversized": True,
                    "path": "in_page",
                }
            body_b64 = raw.get("body_b64")
            if body_b64:
                try:
                    body = base64.b64decode(body_b64)
                except Exception:
                    body = b""
            else:
                body = b""
            return {
                "status": status,
                "url": str(raw.get("url") or url),
                "headers": dict(raw.get("headers") or {}),
                "text": "",
                "body": body,
                "type": str(raw.get("type") or ""),
                "path": "in_page",
            }
        return {
            "status": status,
            "url": str(raw.get("url") or url),
            "headers": dict(raw.get("headers") or {}),
            "text": text,
            "body": text.encode("utf-8"),
            "type": str(raw.get("type") or ""),
        }

    @staticmethod
    def _assert_static_final_host(result: Mapping[str, Any], *, fallback_url: str) -> None:
        final = str(result.get("url") or fallback_url or "")
        if not final:
            return
        if not is_allowed_cninfo_headed_static_url(final) and urlparse(final).hostname:
            raise RuntimeError(f"static headed fetch left approved host: {final}")

    async def _try_cdp_network_resource(
        self,
        url: str,
        *,
        timeout: float,
    ) -> Optional[dict[str, Any]]:
        send = getattr(self._page, "send", None)
        if not callable(send):
            return None
        try:
            from nodriver import cdp
        except Exception:
            return None
        try:
            frame_id = None
            try:
                tree = await asyncio.wait_for(
                    send(cdp.page.get_frame_tree()),
                    timeout=min(5.0, timeout),
                )
                frame = getattr(getattr(tree, "frame_tree", tree), "frame", None)
                frame_id = getattr(frame, "id", None)
            except Exception:
                frame_id = None
            options = cdp.network.LoadNetworkResourceOptions(
                disable_cache=False,
                include_credentials=True,
            )
            send_kwargs: dict[str, Any] = {"url": url, "options": options}
            if frame_id is not None:
                send_kwargs["frame_id"] = frame_id
            raw = await asyncio.wait_for(
                send(cdp.network.load_network_resource(**send_kwargs)),
                timeout=timeout,
            )
        except Exception:
            LOGGER.debug("[CninfoHeadedChrome] CDP loadNetworkResource failed", exc_info=True)
            return None
        return self._normalize_cdp_resource(raw, url)

    @staticmethod
    def _normalize_cdp_resource(raw: Any, url: str) -> Optional[dict[str, Any]]:
        resource = getattr(raw, "resource", raw)
        if isinstance(raw, dict):
            resource = raw.get("resource") or raw
        success = getattr(resource, "success", None)
        if isinstance(resource, dict):
            success = resource.get("success", success)
        if success is False:
            return None
        status = getattr(resource, "http_status_code", None)
        if status is None:
            status = getattr(resource, "httpStatusCode", None)
        headers = getattr(resource, "headers", None)
        body = getattr(resource, "body", None)
        if isinstance(resource, dict):
            status = resource.get("httpStatusCode") or resource.get("http_status_code") or status
            headers = resource.get("headers") if headers is None else headers
            body = resource.get("body") if body is None else body
        if body is None:
            return None
        if isinstance(body, str):
            try:
                content = base64.b64decode(body)
            except Exception:
                content = body.encode("latin-1")
        else:
            content = bytes(body)
        return {
            "status": int(status or 200),
            "url": url,
            "headers": dict(headers or {}),
            "text": "",
            "body": content,
        }

    async def _evaluate_json_on(
        self,
        page: Any,
        script: str,
        *,
        timeout: float,
    ) -> Optional[dict[str, Any]]:
        if page is None:
            return None
        try:
            raw = await asyncio.wait_for(
                page.evaluate(script, await_promise=True, return_by_value=True),
                timeout=timeout,
            )
        except TypeError:
            raw = await asyncio.wait_for(page.evaluate(script), timeout=timeout)
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if isinstance(payload, dict):
            return payload
        return None

    async def _close_extra_tab(self, tab: Any) -> None:
        if tab is None or tab is self._page:
            return
        closer = getattr(tab, "close", None)
        if not callable(closer):
            return
        try:
            maybe = closer()
            if asyncio.iscoroutine(maybe) or asyncio.isfuture(maybe):
                await maybe
        except Exception:
            LOGGER.debug("[CninfoHeadedChrome] extra tab close failed", exc_info=True)

    async def _evaluate_value(self, script: str) -> Any:
        if self._page is None:
            return ""
        try:
            return await asyncio.wait_for(
                self._page.evaluate(script),
                timeout=self.timeout_seconds,
            )
        except Exception:
            LOGGER.debug("[CninfoHeadedChrome] evaluate failed: %s", script, exc_info=True)
            return ""

    async def _evaluate_json(self, script: str, *, timeout: float) -> Optional[dict[str, Any]]:
        if self._page is None:
            return None
        try:
            raw = await asyncio.wait_for(
                self._page.evaluate(script, await_promise=True, return_by_value=True),
                timeout=timeout,
            )
        except TypeError:
            raw = await asyncio.wait_for(self._page.evaluate(script), timeout=timeout)
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if isinstance(payload, dict):
            return payload
        return None

    async def _async_close(self) -> None:
        browser = self._browser
        self._page = None
        self._browser = None
        if browser is None:
            return
        stop = getattr(browser, "stop", None)
        if callable(stop):
            maybe = stop()
            if asyncio.iscoroutine(maybe) or asyncio.isfuture(maybe):
                await maybe
            return
        close = getattr(browser, "close", None)
        if callable(close):
            maybe = close()
            if asyncio.isfuture(maybe) or asyncio.iscoroutine(maybe):
                await maybe


class CninfoHeadedChromeAccess:
    """Blocking session-shaped access: headed Chrome first, sticky proxy on failure."""

    def __init__(
        self,
        *,
        headless: bool = False,
        browser_executable_path: Optional[str] = None,
        proxy_request: Optional[ProxyRequest] = None,
        page_session: Optional[Any] = None,
        environ: Optional[MutableMapping[str, str]] = None,
        display_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        if headless:
            raise CninfoHeadedChromeConfigError(
                "CNInfo headed Chrome rejects headless=true; Wangsu still returns 403"
            )
        self.browser_executable_path = str(
            browser_executable_path or _default_chrome_path()
        ).strip()
        self._proxy_request = proxy_request or request_with_akshare_proxy
        self._injected_page_session = page_session
        self._page = page_session
        self._environ = environ
        self._display_factory = display_factory
        self._display: Any = None
        self._started_display = False
        self._display_ready = False
        self._prefer_proxy = False
        self._chrome_unusable = False
        self._session_started = False
        self._restarted_dead = False
        self._bootstrap: Optional[Mapping[str, Any]] = None
        self._lock = threading.RLock()

    def get(self, url: str, **kwargs: Any) -> CninfoAccessResponse:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> CninfoAccessResponse:
        return self.request("POST", url, **kwargs)

    def request(self, method: str, url: str, **kwargs: Any) -> CninfoAccessResponse:
        if not is_allowed_cninfo_headed_chrome_url(url):
            raise CninfoHeadedChromeUrlError(
                f"CNInfo headed Chrome rejects URL outside the headed allowlist: {url}"
            )
        with self._lock:
            return self._request_locked(str(method or "GET").upper(), url, **kwargs)

    def fetch_allowlisted(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> CninfoHeadedFetchOutcome:
        """Fetch an allowlisted www or static URL and report outcome only. No proxy."""
        if not is_allowed_cninfo_headed_chrome_url(url):
            raise CninfoHeadedChromeUrlError(
                f"CNInfo headed Chrome rejects URL outside the headed allowlist: {url}"
            )
        with self._lock:
            return self._fetch_allowlisted_locked(
                str(method or "GET").upper(),
                url,
                **kwargs,
            )

    def close(self) -> None:
        with self._lock:
            page = self._page
            self._page = self._injected_page_session
            self._session_started = False
            self._bootstrap = None
            if page is not None:
                closer = getattr(page, "close", None)
                if callable(closer):
                    closer()
            if self._started_display and self._display is not None:
                stopper = getattr(self._display, "stop", None)
                if callable(stopper):
                    stopper()
            self._display = None
            self._started_display = False
            self._display_ready = False

    def _request_locked(self, method: str, url: str, **kwargs: Any) -> CninfoAccessResponse:
        if self._prefer_proxy:
            return self._request_via_proxy(method, url, **kwargs)
        outcome = self._fetch_allowlisted_locked(method, url, **kwargs)
        if outcome.status == "success" and outcome.response is not None:
            return outcome.response
        try:
            response = self._request_via_proxy(method, url, **kwargs)
        except Exception:
            if outcome.response is not None:
                return outcome.response
            raise
        self._prefer_proxy = True
        return response

    def _fetch_allowlisted_locked(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> CninfoHeadedFetchOutcome:
        try:
            self._ensure_started()
        except CninfoHeadedChromeConfigError:
            raise
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHeadedChrome] Chrome start/bootstrap failed: %s",
                type(exc).__name__,
            )
            self._chrome_unusable = True
            return CninfoHeadedFetchOutcome("chrome_unavailable")

        if _is_homepage_url(url):
            bootstrap = dict(self._bootstrap or {})
            return CninfoHeadedFetchOutcome(
                "success",
                CninfoAccessResponse.from_fetch(
                    {
                        "status": int(bootstrap.get("status") or 200),
                        "url": CNINFO_HOMEPAGE,
                        "headers": {"content-type": "text/html"},
                        "text": str(bootstrap.get("text") or ""),
                        "body": str(bootstrap.get("text") or "").encode("utf-8"),
                    },
                    access_mode="headed_chrome",
                ),
            )

        if is_allowed_cninfo_headed_static_url(url):
            return self._static_binary_outcome(method, url, **kwargs)

        try:
            result = self._in_page_request(method, url, **kwargs)
        except CninfoHeadedChromeConfigError:
            raise
        except _ChromeUnusable:
            self._chrome_unusable = True
            return CninfoHeadedFetchOutcome("chrome_unavailable")
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHeadedChrome] in-page request failed: %s",
                type(exc).__name__,
            )
            if not self._session_started or (
                self._restarted_dead and _is_dead_session(exc)
            ):
                self._chrome_unusable = True
                return CninfoHeadedFetchOutcome("chrome_unavailable")
            return CninfoHeadedFetchOutcome("chrome_blocked")

        if result is None:
            LOGGER.warning("[CninfoHeadedChrome] in-page evaluate returned null")
            return CninfoHeadedFetchOutcome("chrome_blocked")
        if _is_wangsu_block_page(
            status=result.get("status"),
            text=str(result.get("text") or ""),
        ):
            return CninfoHeadedFetchOutcome(
                "chrome_blocked",
                CninfoAccessResponse.from_fetch(result, access_mode="headed_chrome"),
            )
        return CninfoHeadedFetchOutcome(
            "success",
            CninfoAccessResponse.from_fetch(result, access_mode="headed_chrome"),
        )

    def _static_binary_outcome(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> CninfoHeadedFetchOutcome:
        started = time.monotonic()
        path = "in_page"
        status_code = 0
        nbytes = 0
        try:
            result = self._static_binary_request(method, url, **kwargs)
        except CninfoHeadedChromeConfigError:
            raise
        except _ChromeUnusable:
            self._chrome_unusable = True
            LOGGER.info(
                "[CninfoHeadedChrome] static read path=document_fetch "
                "status=unavailable bytes=0 elapsed=%.3f",
                time.monotonic() - started,
            )
            return CninfoHeadedFetchOutcome("chrome_unavailable")
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHeadedChrome] static document request failed: %s",
                type(exc).__name__,
            )
            if not self._session_started or (
                self._restarted_dead and _is_dead_session(exc)
            ):
                self._chrome_unusable = True
                LOGGER.info(
                    "[CninfoHeadedChrome] static read path=document_fetch "
                    "status=unavailable bytes=0 elapsed=%.3f",
                    time.monotonic() - started,
                )
                return CninfoHeadedFetchOutcome("chrome_unavailable")
            LOGGER.info(
                "[CninfoHeadedChrome] static read path=document_fetch "
                "status=blocked bytes=0 elapsed=%.3f",
                time.monotonic() - started,
            )
            return CninfoHeadedFetchOutcome("chrome_blocked")
        if result is None:
            LOGGER.info(
                "[CninfoHeadedChrome] static read path=document_fetch "
                "status=blocked bytes=0 elapsed=%.3f",
                time.monotonic() - started,
            )
            return CninfoHeadedFetchOutcome("chrome_blocked")
        path = str(result.get("path") or "document_fetch")
        final = str(result.get("url") or url)
        if final and urlparse(final).hostname and not is_allowed_cninfo_headed_static_url(final):
            result = _static_off_host_redirect(url, final)
            path = str(result.get("path") or path)
        outcome = _classify_static_headed_result(result, url=url)
        if outcome.response is not None:
            status_code = int(outcome.response.status_code)
            nbytes = len(outcome.response.content)
        LOGGER.info(
            "[CninfoHeadedChrome] static read path=%s status=%s bytes=%s elapsed=%.3f",
            path,
            status_code or outcome.status,
            nbytes,
            time.monotonic() - started,
        )
        return outcome

    def _ensure_started(self) -> None:
        if self._session_started and self._page is not None:
            return
        if self._chrome_unusable:
            raise _ChromeUnusable("headed Chrome is unusable on this instance")
        self._ensure_display()
        if self._page is None:
            self._page = self._make_page_session()
        self._start_homepage_with_restart()

    def _start_homepage_with_restart(self) -> None:
        last_error: Optional[BaseException] = None
        for _attempt in range(2):
            self._page = self._page or self._make_page_session()
            try:
                bootstrap = self._page.start(CNINFO_HOMEPAGE)
            except Exception as exc:
                last_error = exc
                self._safe_close_page()
                continue
            if _is_wangsu_block_page(
                status=bootstrap.get("status"),
                title=str(bootstrap.get("title") or ""),
                text=str(bootstrap.get("text") or ""),
            ):
                last_error = _ChromeUnusable("homepage bootstrap is a Wangsu 403 block page")
                self._safe_close_page()
                continue
            self._bootstrap = bootstrap
            self._session_started = True
            return
        raise last_error or _ChromeUnusable("headed Chrome failed to bootstrap")

    def _make_page_session(self) -> Any:
        if self._injected_page_session is not None:
            return self._injected_page_session
        return _NodriverPageSession(
            browser_executable_path=self.browser_executable_path,
        )

    def _ensure_display(self) -> None:
        if self._display_ready:
            return
        self._display_ready = True
        env = self._environ if self._environ is not None else os.environ
        if str(env.get("DISPLAY") or "").strip():
            return
        if self._injected_page_session is not None and self._display_factory is None:
            return
        if self._display_factory is not None:
            self._display = self._display_factory()
            starter = getattr(self._display, "start", None)
            if callable(starter):
                starter()
            self._started_display = True
            return
        self._display = self._start_real_xvfb()
        self._started_display = self._display is not None

    def _start_real_xvfb(self) -> Any:
        old_displayfd = os.environ.get("PYVIRTUALDISPLAY_DISPLAYFD")
        os.environ["PYVIRTUALDISPLAY_DISPLAYFD"] = "0"
        try:
            from pyvirtualdisplay import Display
        except Exception as exc:
            raise _ChromeUnusable(
                "CNInfo headed Chrome needs pyvirtualdisplay when DISPLAY is unset"
            ) from exc
        try:
            display = Display(visible=False, size=(1920, 1080))
            display.start()
            return display
        finally:
            if old_displayfd is None:
                os.environ.pop("PYVIRTUALDISPLAY_DISPLAYFD", None)
            else:
                os.environ["PYVIRTUALDISPLAY_DISPLAYFD"] = old_displayfd

    def _in_page_request(self, method: str, url: str, **kwargs: Any) -> Optional[dict[str, Any]]:
        fetch_kwargs = self._fetch_kwargs(url, **kwargs)
        target_url = fetch_kwargs.pop("url")
        try:
            return self._page.fetch(method, target_url, **fetch_kwargs)
        except Exception as exc:
            if self._restarted_dead or not _is_dead_session(exc):
                if self._restarted_dead and _is_dead_session(exc):
                    self._chrome_unusable = True
                    raise _ChromeUnusable(
                        "headed Chrome dead after one restart"
                    ) from exc
                raise
            LOGGER.warning("[CninfoHeadedChrome] restarting dead Chrome session once")
            self._restarted_dead = True
            self._safe_close_page()
            self._session_started = False
            self._ensure_started()
            retry_kwargs = self._fetch_kwargs(url, **kwargs)
            retry_url = retry_kwargs.pop("url")
            try:
                return self._page.fetch(method, retry_url, **retry_kwargs)
            except Exception as retry_exc:
                if _is_dead_session(retry_exc):
                    self._chrome_unusable = True
                    raise _ChromeUnusable(
                        "headed Chrome dead after one restart"
                    ) from retry_exc
                raise

    def _static_binary_request(self, method: str, url: str, **kwargs: Any) -> Optional[dict[str, Any]]:
        def _once() -> Optional[dict[str, Any]]:
            fetch_kwargs = self._fetch_kwargs(url, **kwargs)
            target_url = fetch_kwargs.pop("url")
            fetch_kwargs["binary"] = True
            fetch_kwargs["max_bytes"] = _IN_PAGE_MAX_BYTES
            if "allow_redirects" in kwargs:
                fetch_kwargs["allow_redirects"] = kwargs["allow_redirects"]
            allow_redirects = fetch_kwargs.get("allow_redirects", True) is not False
            result: Optional[dict[str, Any]] = None
            in_page_exc: Optional[BaseException] = None
            try:
                result = self._page.fetch(method, target_url, **fetch_kwargs)
            except Exception as exc:
                if _is_dead_session(exc):
                    raise
                in_page_exc = exc
            if result is not None:
                result.setdefault("path", "in_page")
            if result is not None and not _static_in_page_needs_document_read(
                result,
                allow_redirects=allow_redirects,
            ):
                return result
            document = getattr(self._page, "fetch_static_document", None)
            if not callable(document):
                if in_page_exc is not None:
                    raise in_page_exc
                return result
            document_kwargs = dict(fetch_kwargs)
            document_kwargs["timeout"] = _document_timeout(kwargs.get("timeout"))
            try:
                captured = document(method, target_url, **document_kwargs)
            except Exception as document_exc:
                if _is_dead_session(document_exc):
                    raise
                if in_page_exc is not None:
                    raise RuntimeError(str(document_exc)) from document_exc
                raise
            if captured is not None:
                captured.setdefault("path", captured.get("path") or "document_fetch")
            return captured

        try:
            return _once()
        except Exception as exc:
            if self._restarted_dead or not _is_dead_session(exc):
                if self._restarted_dead and _is_dead_session(exc):
                    self._chrome_unusable = True
                    raise _ChromeUnusable(
                        "headed Chrome dead after one restart"
                    ) from exc
                raise
            LOGGER.warning("[CninfoHeadedChrome] restarting dead Chrome session once")
            self._restarted_dead = True
            self._safe_close_page()
            self._session_started = False
            self._ensure_started()
            try:
                return _once()
            except Exception as retry_exc:
                if _is_dead_session(retry_exc):
                    self._chrome_unusable = True
                    raise _ChromeUnusable(
                        "headed Chrome dead after one restart"
                    ) from retry_exc
                raise

    def _fetch_kwargs(self, url: str, **kwargs: Any) -> dict[str, Any]:
        data = kwargs.get("data")
        json_body = kwargs.get("json")
        if json_body is not None:
            content_type = "application/json"
        elif data is not None:
            content_type = "application/x-www-form-urlencoded"
        else:
            content_type = None
        return {
            "url": _apply_params(url, kwargs.get("params")),
            "params": kwargs.get("params"),
            "data": data,
            "json": json_body,
            "headers": dict(kwargs.get("headers") or {}),
            "timeout": _normalize_timeout(kwargs.get("timeout")),
            "content_type": content_type,
            "credentials": "include",
        }

    def _request_via_proxy(self, method: str, url: str, **kwargs: Any) -> CninfoAccessResponse:
        timeout = kwargs.get("timeout")
        raw = self._proxy_request(
            method,
            url,
            attempts=3,
            timeout=_DEFAULT_TIMEOUT if timeout is None else _normalize_timeout(timeout),
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
            data=kwargs.get("data"),
            json=kwargs.get("json"),
            accept_response=_accept_proxy_response(url),
            warning_logger=LOGGER,
        )
        return CninfoAccessResponse.from_raw(raw, access_mode="proxy_patch", url=url)

    def _safe_close_page(self) -> None:
        page = self._page
        self._session_started = False
        self._bootstrap = None
        if page is None:
            return
        closer = getattr(page, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                LOGGER.debug("[CninfoHeadedChrome] page close after failure failed", exc_info=True)
        if self._injected_page_session is None:
            self._page = None


def create_cninfo_headed_chrome_access(**kwargs: Any) -> CninfoHeadedChromeAccess:
    """Construct the headed-Chrome access session. Production factories do not call this."""
    return CninfoHeadedChromeAccess(**kwargs)
