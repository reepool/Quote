"""Instance-owned headed Chrome access for first-party www.cninfo.com.cn HTTP.

This is not a wholesale replacement of ``attach_cninfo_access``. It only accepts
allowed www URLs. Official-filing PDF downloads on ``static.cninfo.com.cn`` must
stay on the current TLS/proxy pass-through.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from typing import Any, Callable, Mapping, MutableMapping, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

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
_DEFAULT_TIMEOUT = 20.0
ProxyRequest = Callable[..., Any]


class CninfoHeadedChromeConfigError(ValueError):
    """Raised when headed Chrome is misconfigured (for example headless=true)."""


class CninfoHeadedChromeUrlError(ValueError):
    """Raised when a URL is outside the first-party www CNInfo allowlist."""


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


def is_allowed_cninfo_headed_chrome_url(url: str) -> bool:
    """Return True for https www/cninfo.com.cn paths this access provider may fetch."""
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
    ) -> None:
        self.status_code = int(status_code)
        self.url = str(url or "")
        self.headers = dict(headers or {})
        self.text = str(text or "")
        self.content = bytes(content or b"")
        self.access_mode = str(access_mode)
        self._raw = raw

    @classmethod
    def from_fetch(cls, result: Mapping[str, Any], *, access_mode: str) -> "CninfoAccessResponse":
        text = str(result.get("text") or "")
        body = result.get("body")
        if body is None:
            body = text.encode("utf-8")
        return cls(
            status_code=int(result.get("status") or 0),
            url=str(result.get("url") or ""),
            headers=dict(result.get("headers") or {}),
            text=text,
            content=bytes(body),
            access_mode=access_mode,
        )

    @classmethod
    def from_raw(cls, response: Any, *, access_mode: str, url: str) -> "CninfoAccessResponse":
        text = str(getattr(response, "text", "") or "")
        content = getattr(response, "content", None)
        if content is None:
            content = text.encode("utf-8")
        return cls(
            status_code=int(getattr(response, "status_code", 0) or 0),
            url=str(getattr(response, "url", "") or url),
            headers=dict(getattr(response, "headers", {}) or {}),
            text=text,
            content=bytes(content or b""),
            access_mode=access_mode,
            raw=response,
        )

    def json(self) -> Any:
        if self._raw is not None:
            raw_json = getattr(self._raw, "json", None)
            if callable(raw_json):
                return raw_json()
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


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
        headers = dict(kwargs.get("headers") or {})
        content_type = kwargs.get("content_type")
        timeout = _normalize_timeout(kwargs.get("timeout"))
        script = (
            "(async () => {\n"
            f"  const opt = {{ method: {method!r}, credentials: 'include', headers: {json.dumps(headers, ensure_ascii=False)} }};\n"
            f"  const contentType = {json.dumps(content_type)};\n"
            f"  const jsonBody = {json.dumps(kwargs.get('json'), ensure_ascii=False)};\n"
            f"  const formBody = {json.dumps(kwargs.get('data'), ensure_ascii=False)};\n"
            "  if (contentType === 'application/json' && jsonBody !== null) {\n"
            "    opt.headers['Content-Type'] = 'application/json';\n"
            "    opt.body = JSON.stringify(jsonBody);\n"
            "  } else if (contentType === 'application/x-www-form-urlencoded' && formBody !== null) {\n"
            "    opt.headers['Content-Type'] = 'application/x-www-form-urlencoded';\n"
            "    opt.body = new URLSearchParams(formBody).toString();\n"
            "  }\n"
            "  try {\n"
            f"    const r = await fetch({url!r}, opt);\n"
            "    const text = await r.text();\n"
            "    const headers = {};\n"
            "    r.headers.forEach((value, key) => { headers[key] = value; });\n"
            "    return JSON.stringify({status: r.status, url: r.url, headers, text});\n"
            "  } catch (error) {\n"
            "    return JSON.stringify({status: -1, url: '', headers: {}, text: String(error)});\n"
            "  }\n"
            "})()"
        )
        raw = await self._evaluate_json(script, timeout=timeout)
        if raw is None:
            return None
        status = int(raw.get("status") or 0)
        text = str(raw.get("text") or "")
        if status <= 0:
            raise TimeoutError(text or "in-page fetch failed")
        return {
            "status": status,
            "url": str(raw.get("url") or url),
            "headers": dict(raw.get("headers") or {}),
            "text": text,
            "body": text.encode("utf-8"),
        }

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
                f"CNInfo headed Chrome rejects URL outside the www allowlist: {url}"
            )
        with self._lock:
            return self._request_locked(str(method or "GET").upper(), url, **kwargs)

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
        try:
            self._ensure_started()
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHeadedChrome] Chrome start/bootstrap failed: %s",
                type(exc).__name__,
            )
            self._chrome_unusable = True
            self._prefer_proxy = True
            return self._request_via_proxy(method, url, **kwargs)

        if _is_homepage_url(url):
            bootstrap = dict(self._bootstrap or {})
            return CninfoAccessResponse.from_fetch(
                {
                    "status": int(bootstrap.get("status") or 200),
                    "url": CNINFO_HOMEPAGE,
                    "headers": {"content-type": "text/html"},
                    "text": str(bootstrap.get("text") or ""),
                    "body": str(bootstrap.get("text") or "").encode("utf-8"),
                },
                access_mode="headed_chrome",
            )

        try:
            result = self._in_page_request(method, url, **kwargs)
        except Exception as exc:
            LOGGER.warning(
                "[CninfoHeadedChrome] in-page request failed: %s",
                type(exc).__name__,
            )
            return self._fallback_proxy_after_chrome(method, url, chrome_result=None, **kwargs)

        if result is None:
            LOGGER.warning("[CninfoHeadedChrome] in-page evaluate returned null")
            return self._fallback_proxy_after_chrome(method, url, chrome_result=None, **kwargs)
        if _is_wangsu_block_page(
            status=result.get("status"),
            text=str(result.get("text") or ""),
        ):
            return self._fallback_proxy_after_chrome(method, url, chrome_result=result, **kwargs)
        return CninfoAccessResponse.from_fetch(result, access_mode="headed_chrome")

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
                raise
            LOGGER.warning("[CninfoHeadedChrome] restarting dead Chrome session once")
            self._restarted_dead = True
            self._safe_close_page()
            self._session_started = False
            self._ensure_started()
            retry_kwargs = self._fetch_kwargs(url, **kwargs)
            retry_url = retry_kwargs.pop("url")
            return self._page.fetch(method, retry_url, **retry_kwargs)

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

    def _fallback_proxy_after_chrome(
        self,
        method: str,
        url: str,
        *,
        chrome_result: Optional[Mapping[str, Any]],
        **kwargs: Any,
    ) -> CninfoAccessResponse:
        try:
            response = self._request_via_proxy(method, url, **kwargs)
        except Exception:
            if chrome_result is not None:
                return CninfoAccessResponse.from_fetch(
                    chrome_result,
                    access_mode="headed_chrome",
                )
            raise
        self._prefer_proxy = True
        return response

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
