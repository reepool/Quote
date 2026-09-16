"""Instance-owned headed Chrome hop for first-party www and static CNInfo HTTP.

Production jobs go through ``attach_cninfo_access``. This module reports
``success`` / ``chrome_blocked`` / ``chrome_unavailable`` via
``fetch_allowlisted``; the mux owns fallback. Standalone ``request()`` may
still jump headed-to-proxy for hop unit tests. HTTPS
``static.cninfo.com.cn`` attachments use a binary same-Chrome read, not the
www JSON ``r.text()`` path.
"""

from __future__ import annotations

import asyncio
import base64
import http.client
import json
import logging
import os
import threading
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
_STATIC_MAX_BYTES = 50 * 1024 * 1024
_DEFAULT_TIMEOUT = 20.0
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


def _static_result_needs_same_chrome_fallback(result: Optional[Mapping[str, Any]]) -> bool:
    if result is None or _is_in_page_cors_or_opaque(result):
        return True
    try:
        status = int(result.get("status") or 0)
    except (TypeError, ValueError):
        return True
    body = result.get("body")
    if body is None:
        body = str(result.get("text") or "").encode("utf-8")
    content = bytes(body or b"")
    return status in {200, 206} and not content


def _classify_static_headed_result(result: Mapping[str, Any]) -> "CninfoHeadedFetchOutcome":
    try:
        status = int(result.get("status") or 0)
    except (TypeError, ValueError):
        status = 0
    body = result.get("body")
    if body is None:
        body = str(result.get("text") or "").encode("utf-8")
    content = bytes(body or b"")
    media = _static_media_type(dict(result.get("headers") or {}))
    text = str(result.get("text") or "") or content[:4000].decode("utf-8", "replace")
    response = CninfoAccessResponse.from_fetch(result, access_mode="headed_chrome")
    if status in {404, 410}:
        return CninfoHeadedFetchOutcome("success", response)
    if status == 403 or _is_wangsu_block_page(status=status, text=text):
        return CninfoHeadedFetchOutcome("chrome_blocked", response)
    if status in {200, 206}:
        if len(content) > _STATIC_MAX_BYTES:
            return CninfoHeadedFetchOutcome("chrome_blocked", response)
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

    def _in_page_fetch_script(self, method: str, url: str, **kwargs: Any) -> str:
        headers = dict(kwargs.get("headers") or {})
        content_type = kwargs.get("content_type")
        binary = bool(kwargs.get("binary"))
        redirect_mode = "follow" if kwargs.get("allow_redirects", True) is not False else "manual"
        return (
            "(async () => {\n"
            f"  const opt = {{ method: {method!r}, credentials: 'include', "
            f"headers: {json.dumps(headers, ensure_ascii=False)}, "
            f"redirect: {redirect_mode!r} }};\n"
            f"  const contentType = {json.dumps(content_type)};\n"
            f"  const jsonBody = {json.dumps(kwargs.get('json'), ensure_ascii=False)};\n"
            f"  const formBody = {json.dumps(kwargs.get('data'), ensure_ascii=False)};\n"
            f"  const binary = {json.dumps(binary)};\n"
            f"  const maxBytes = {int(_STATIC_MAX_BYTES)};\n"
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
        try:
            result = self._static_binary_request(method, url, **kwargs)
        except CninfoHeadedChromeConfigError:
            raise
        except _ChromeUnusable:
            self._chrome_unusable = True
            return CninfoHeadedFetchOutcome("chrome_unavailable")
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHeadedChrome] static binary request failed: %s",
                type(exc).__name__,
            )
            if not self._session_started or (
                self._restarted_dead and _is_dead_session(exc)
            ):
                self._chrome_unusable = True
                return CninfoHeadedFetchOutcome("chrome_unavailable")
            return CninfoHeadedFetchOutcome("chrome_blocked")
        if result is None:
            LOGGER.warning("[CninfoHeadedChrome] static binary evaluate returned null")
            return CninfoHeadedFetchOutcome("chrome_blocked")
        final = str(result.get("url") or url)
        if final and urlparse(final).hostname and not is_allowed_cninfo_headed_static_url(final):
            return CninfoHeadedFetchOutcome(
                "chrome_blocked",
                CninfoAccessResponse.from_fetch(result, access_mode="headed_chrome"),
            )
        return _classify_static_headed_result(result)

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
            if "allow_redirects" in kwargs:
                fetch_kwargs["allow_redirects"] = kwargs["allow_redirects"]
            result: Optional[dict[str, Any]] = None
            cors_exc: Optional[BaseException] = None
            try:
                result = self._page.fetch(method, target_url, **fetch_kwargs)
            except Exception as exc:
                if _is_dead_session(exc):
                    raise
                if not _is_in_page_cors_or_opaque(None, exc):
                    raise
                cors_exc = exc
            if result is not None and not _static_result_needs_same_chrome_fallback(result):
                return result
            cdp = getattr(self._page, "fetch_binary_same_chrome", None)
            if not callable(cdp):
                if cors_exc is not None:
                    raise cors_exc
                return result
            try:
                return cdp(method, target_url, **fetch_kwargs)
            except Exception as cdp_exc:
                if _is_dead_session(cdp_exc):
                    raise
                if cors_exc is not None:
                    raise RuntimeError(str(cdp_exc)) from cdp_exc
                raise

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
