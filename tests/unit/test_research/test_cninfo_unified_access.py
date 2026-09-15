"""Unit tests for the unified CNInfo access mux."""

from __future__ import annotations

import asyncio
import json
import threading
import time

import pytest

from research.providers.cninfo_headed_chrome import (
    CninfoAccessResponse,
    CninfoHeadedChromeConfigError,
    CninfoHeadedFetchOutcome,
    create_cninfo_headed_chrome_access,
)
from research.providers.cninfo_http import (
    attach_cninfo_access,
    reset_cninfo_access_runtime,
    resolve_cninfo_access_mode,
)
from tests.unit.test_research.test_cninfo_headed_chrome import _FakePageSession


DATA20_URL = "https://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders"
DATA20_OTHER_URL = "https://www.cninfo.com.cn/data20/companyOverview/getCompanyInfo"
ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
DISCLOSURE_URL = "https://www.cninfo.com.cn/new/disclosure/stock?stockCode=000001"
UNALLOWLISTED_WWW = "https://www.cninfo.com.cn/new/js/app.js"
STATIC_URL = "https://static.cninfo.com.cn/finalpage/report.PDF"
WEBAPI_URL = "https://webapi.cninfo.com.cn/api/stock/p_stock2215"
HTTP_WWW = "http://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders"
SSE_URL = "https://query.sse.com.cn/commonQuery.do"


class _Resp:
    def __init__(
        self,
        payload=None,
        *,
        status_code=200,
        text="",
        reason="",
        headers=None,
        url="",
    ):
        self.payload = payload
        self.status_code = status_code
        self.text = text if text or payload is None else json.dumps(payload, ensure_ascii=False)
        self.content = self.text.encode("utf-8")
        self.reason = reason
        self.headers = headers or {"Content-Type": "application/json"}
        self.url = url or ""

    def json(self):
        if self.payload is None:
            raise ValueError("not json")
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Inner:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []
        self.marker = "inner-state"

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected inner request {method} {url}")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _CallableHop:
    def __init__(self, responses=None, *, error=None):
        self.responses = list(responses or [])
        self.calls = []
        self.error = error

    def __call__(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        if self.error is not None:
            raise self.error
        if not self.responses:
            raise AssertionError(f"unexpected hop {method} {url}")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _HeadedHop:
    def __init__(self, outcomes=None, *, error=None, delay=0.0):
        self.outcomes = list(outcomes or [])
        self.calls = []
        self.closed = 0
        self.error = error
        self.delay = delay
        self.in_flight = 0
        self.max_in_flight = 0
        self._lock = threading.Lock()
        self.threads = []

    def fetch_allowlisted(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        self.threads.append(threading.current_thread().ident)
        if self.error is not None:
            raise self.error
        with self._lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.delay:
                time.sleep(self.delay)
            if not self.outcomes:
                raise AssertionError(f"unexpected headed fetch {method} {url}")
            item = self.outcomes.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        finally:
            with self._lock:
                self.in_flight -= 1

    def close(self):
        self.closed += 1


def _headed_success(payload, *, url=DATA20_URL):
    text = json.dumps(payload, ensure_ascii=False)
    return CninfoHeadedFetchOutcome(
        "success",
        CninfoAccessResponse.from_fetch(
            {
                "status": 200,
                "url": url,
                "headers": {"content-type": "application/json"},
                "text": text,
                "body": text.encode("utf-8"),
            },
            access_mode="headed_chrome",
        ),
    )


def _attach(session=None, **kwargs):
    kwargs.setdefault("environ", {})
    kwargs.setdefault("sources", {"cninfo": {"access": {"preferred_mode": "headed_chrome"}}})
    return attach_cninfo_access(session, **kwargs)


def test_resolve_mode_precedence_argument_env_config_default():
    assert (
        resolve_cninfo_access_mode(
            "chrome_tls",
            environ={"QUOTE_CNINFO_ACCESS_MODE": "headed_chrome"},
            sources={"cninfo": {"access": {"preferred_mode": "headed_chrome"}}},
        )
        == "chrome_tls"
    )
    assert (
        resolve_cninfo_access_mode(
            environ={"QUOTE_CNINFO_ACCESS_MODE": "chrome_tls"},
            sources={"cninfo": {"access": {"preferred_mode": "headed_chrome"}}},
        )
        == "chrome_tls"
    )
    assert (
        resolve_cninfo_access_mode(
            environ={"QUOTE_CNINFO_ACCESS_MODE": ""},
            sources={"cninfo": {"access": {"preferred_mode": "chrome_tls"}}},
        )
        == "chrome_tls"
    )
    assert (
        resolve_cninfo_access_mode(environ={}, sources={"cninfo": {}})
        == "headed_chrome"
    )


def test_invalid_preferred_mode_raises_without_starting_chrome():
    headed = _HeadedHop(outcomes=[AssertionError("must not start")])
    with pytest.raises(ValueError, match="preferred_mode"):
        _attach(preferred_mode="headless", headed_hop=headed)
    with pytest.raises(ValueError, match="preferred_mode"):
        _attach(
            environ={"QUOTE_CNINFO_ACCESS_MODE": "browser"},
            headed_hop=headed,
        )
    with pytest.raises(ValueError, match="preferred_mode"):
        _attach(
            environ={},
            sources={"cninfo": {"access": {"preferred_mode": "headless"}}},
            headed_hop=headed,
        )
    assert headed.calls == []


def test_preferred_headed_chrome_uses_headed_hop_first():
    headed = _HeadedHop(outcomes=[_headed_success({"code": 200})])
    tls = _CallableHop(responses=[AssertionError("TLS must not run")])
    proxy = _CallableHop(responses=[AssertionError("proxy must not run")])
    session = _attach(
        preferred_mode="headed_chrome",
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=proxy,
    )
    response = session.get(DATA20_URL, params={"scode": "600000"})

    assert response.access_mode == "headed_chrome"
    assert response.json()["code"] == 200
    assert response.reason == "OK"
    assert headed.calls[0]["params"] == {"scode": "600000"}
    assert tls.calls == []
    assert proxy.calls == []


def test_preferred_chrome_tls_skips_headed():
    headed = _HeadedHop(outcomes=[AssertionError("headed must not run")])
    tls = _CallableHop(responses=[_Resp({"via": "tls"})])
    session = _attach(
        preferred_mode="chrome_tls",
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    response = session.get(DATA20_URL)

    assert response.access_mode == "chrome_tls"
    assert response.json()["via"] == "tls"
    assert headed.calls == []


def test_argument_overrides_env_and_config_for_chrome_tls():
    headed = _HeadedHop(outcomes=[AssertionError("headed must not run")])
    tls = _CallableHop(responses=[_Resp({"via": "tls"})])
    session = attach_cninfo_access(
        preferred_mode="chrome_tls",
        environ={"QUOTE_CNINFO_ACCESS_MODE": "headed_chrome"},
        sources={"cninfo": {"access": {"preferred_mode": "headed_chrome"}}},
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    assert session.get(DATA20_URL).access_mode == "chrome_tls"
    assert headed.calls == []


def test_data20_prefix_is_allowlisted():
    headed = _HeadedHop(outcomes=[_headed_success({"ok": True}, url=DATA20_OTHER_URL)])
    session = _attach(
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    response = session.get(DATA20_OTHER_URL)
    assert response.access_mode == "headed_chrome"
    assert headed.calls[0]["url"] == DATA20_OTHER_URL


@pytest.mark.parametrize(
    "url",
    [UNALLOWLISTED_WWW, STATIC_URL, WEBAPI_URL, HTTP_WWW],
)
def test_unallowlisted_static_webapi_http_never_use_headed(url):
    headed = _HeadedHop(outcomes=[AssertionError("headed must not run")])
    tls = _CallableHop(responses=[_Resp({"via": "tls"}, url=url)])
    session = _attach(
        preferred_mode="headed_chrome",
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    response = session.get(url)
    assert response.access_mode == "chrome_tls"
    assert headed.calls == []
    assert tls.calls[0]["url"] == url


def test_homepage_slash_is_not_a_prefix():
    headed = _HeadedHop(outcomes=[AssertionError("slash must not prefix-allow")])
    tls = _CallableHop(responses=[_Resp(text="js", headers={"Content-Type": "text/javascript"})])
    session = _attach(
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    session.get("https://www.cninfo.com.cn/anything")
    assert headed.calls == []
    assert tls.calls[0]["url"] == "https://www.cninfo.com.cn/anything"


def test_injected_session_does_not_disable_tls_or_headed():
    inner = _Inner(responses=[AssertionError("inner must not hit CNInfo")])
    headed = _HeadedHop(outcomes=[_headed_success({"via": "headed"})])
    session = _attach(
        inner,
        preferred_mode="headed_chrome",
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    response = session.get(DATA20_URL)
    assert response.access_mode == "headed_chrome"
    assert inner.calls == []
    assert session.marker == "inner-state"
    inner.responses = [_Resp({"via": "sse"})]
    sse = session.get(SSE_URL)
    assert sse.json()["via"] == "sse"
    assert inner.calls[0]["url"] == SSE_URL


def test_chrome_blocked_uses_proxy_only_and_is_runtime_sticky_for_www():
    headed = _HeadedHop(
        outcomes=[
            CninfoHeadedFetchOutcome("chrome_blocked"),
            AssertionError("headed must stop after blocked"),
        ]
    )
    tls = _CallableHop(responses=[AssertionError("TLS must not follow chrome_blocked")])
    proxy = _CallableHop(
        responses=[
            _Resp({"via": "proxy-1"}),
            _Resp({"via": "proxy-2"}),
            _Resp({"via": "proxy-unallowlisted"}),
        ]
    )
    first = _attach(
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=proxy,
    )
    first_response = first.get(DATA20_URL, params={"scode": "000001"})
    second = _attach(
        impersonated_request=tls,
        proxy_request=proxy,
    )
    second_response = second.get(ANNOUNCEMENT_URL, data={"pageNum": "1"})
    unallowlisted = second.get(UNALLOWLISTED_WWW)
    static_tls = _CallableHop(responses=[_Resp(text="%PDF", url=STATIC_URL)])
    static_session = _attach(
        impersonated_request=static_tls,
        proxy_request=_CallableHop(responses=[AssertionError("static must not use www sticky")]),
    )
    static_response = static_session.get(STATIC_URL)

    assert first_response.access_mode == "proxy_patch"
    assert first_response.json()["via"] == "proxy-1"
    assert second_response.access_mode == "proxy_patch"
    assert second_response.json()["via"] == "proxy-2"
    assert unallowlisted.access_mode == "proxy_patch"
    assert static_response.access_mode == "chrome_tls"
    assert headed.closed == 1
    assert len(headed.calls) == 1
    assert tls.calls == []
    assert static_tls.calls[0]["url"] == STATIC_URL


def test_dead_session_after_restart_uses_tls_stack_not_www_proxy_sticky():
    page = _FakePageSession(
        fetches=[
            RuntimeError("Target closed"),
            RuntimeError("session closed"),
        ]
    )
    hop = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("headed hop must not choose proxy")
        ),
    )
    tls = _CallableHop(responses=[_Resp({"via": "tls-1"}), _Resp({"via": "tls-2"})])
    proxy = _CallableHop(responses=[AssertionError("must not sticky www proxy")])
    first = _attach(
        headed_hop=hop,
        impersonated_request=tls,
        proxy_request=proxy,
    )
    first_response = first.get(DATA20_URL)
    second = _attach(impersonated_request=tls, proxy_request=proxy)
    second_response = second.get(DATA20_URL)

    assert first_response.access_mode == "chrome_tls"
    assert second_response.access_mode == "chrome_tls"
    assert first_response.json()["via"] == "tls-1"
    assert second_response.json()["via"] == "tls-2"
    assert page.started == 2
    assert page.closed >= 1


def test_chrome_unavailable_uses_tls_stack_and_does_not_restart_chrome():
    headed = _HeadedHop(
        outcomes=[
            CninfoHeadedFetchOutcome("chrome_unavailable"),
            AssertionError("must not restart headed after unavailable"),
        ]
    )
    tls = _CallableHop(responses=[_Resp({"via": "tls-1"}), _Resp({"via": "tls-2"})])
    first = _attach(
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    first_response = first.get(DATA20_URL)
    second = _attach(impersonated_request=tls, proxy_request=_CallableHop())
    second_response = second.get(DATA20_URL)

    assert first_response.access_mode == "chrome_tls"
    assert second_response.access_mode == "chrome_tls"
    assert first_response.json()["via"] == "tls-1"
    assert second_response.json()["via"] == "tls-2"
    assert len(headed.calls) == 1


def test_headed_hop_none_is_unavailable_without_constructing_chrome():
    tls = _CallableHop(responses=[_Resp({"via": "tls"})])
    session = _attach(
        preferred_mode="headed_chrome",
        headed_hop=None,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    assert session.get(DATA20_URL).access_mode == "chrome_tls"


def test_chrome_tls_403_stickies_www_proxy_not_static():
    tls = _CallableHop(
        responses=[
            _Resp(status_code=403, text="blocked", reason="Forbidden"),
            AssertionError("www TLS must not retry after proxy"),
        ]
    )
    proxy = _CallableHop(responses=[_Resp({"via": "proxy"}), _Resp({"via": "proxy-2"})])
    session = _attach(
        preferred_mode="chrome_tls",
        headed_hop=None,
        impersonated_request=tls,
        proxy_request=proxy,
    )
    first = session.get(DATA20_URL)
    second = session.get(DATA20_URL)
    assert first.access_mode == "proxy_patch"
    assert second.access_mode == "proxy_patch"
    assert first.json()["via"] == "proxy"
    static_tls = _CallableHop(responses=[_Resp(text="%PDF")])
    static = _attach(
        preferred_mode="chrome_tls",
        impersonated_request=static_tls,
        proxy_request=_CallableHop(responses=[AssertionError("static")]),
    )
    assert static.get(STATIC_URL).access_mode == "chrome_tls"
    assert len(tls.calls) == 1


def test_logical_429_is_transport_success_not_chrome_blocked():
    payload = {"data": {"resultCode": 429, "resultMsg": "too many requests"}}
    headed = _HeadedHop(outcomes=[_headed_success(payload)])
    proxy = _CallableHop(responses=[AssertionError("logical 429 is not proxy")])
    session = _attach(
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=proxy,
    )
    response = session.get(DATA20_URL)
    assert response.access_mode == "headed_chrome"
    assert response.status_code == 200
    assert response.json()["data"]["resultCode"] == 429
    assert proxy.calls == []


def test_www_proxy_accepts_disclosure_html():
    headed = _HeadedHop(outcomes=[CninfoHeadedFetchOutcome("chrome_blocked")])
    html = _Resp(
        payload=None,
        text="<!doctype html><html>stock</html>",
        headers={"Content-Type": "text/html; charset=utf-8"},
        url=DISCLOSURE_URL,
    )

    def fake_proxy(method, url, **kwargs):
        accept = kwargs["accept_response"]
        assert accept(html) is True
        assert accept(_Resp({"announcements": []})) is True
        return html

    session = _attach(
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=fake_proxy,
    )
    response = session.get(DISCLOSURE_URL)
    assert response.access_mode == "proxy_patch"
    assert "stock" in response.text
    assert response.headers.get("content-type") == "text/html; charset=utf-8"


def test_two_attach_sessions_share_one_headed_runtime():
    headed = _HeadedHop(
        outcomes=[
            _headed_success({"n": 1}),
            _headed_success({"n": 2}),
        ]
    )
    first = _attach(
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    second = _attach(
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    assert first.get(DATA20_URL).json()["n"] == 1
    assert second.get(DATA20_URL).json()["n"] == 2
    assert len(headed.calls) == 2


def test_in_page_requests_are_serialized():
    headed = _HeadedHop(
        outcomes=[_headed_success({"n": 1}), _headed_success({"n": 2})],
        delay=0.05,
    )
    first = _attach(
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    second = _attach(
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    results = []

    def worker(session):
        results.append(session.get(DATA20_URL).json()["n"])

    threads = [
        threading.Thread(target=worker, args=(first,)),
        threading.Thread(target=worker, args=(second,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(results) == [1, 2]
    assert headed.max_in_flight == 1


def test_request_from_running_loop_does_not_raise():
    headed = _HeadedHop(outcomes=[_headed_success({"ok": True})])
    session = _attach(
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=_CallableHop(),
    )
    main_ident = threading.current_thread().ident

    async def _call():
        return session.post(ANNOUNCEMENT_URL, data={"pageNum": "1"})

    response = asyncio.run(_call())
    assert response.access_mode == "headed_chrome"
    assert headed.threads
    assert headed.threads[0] != main_ident


def test_headless_true_is_refused_and_not_treated_as_unavailable():
    with pytest.raises(CninfoHeadedChromeConfigError, match="headless"):
        create_cninfo_headed_chrome_access(headless=True)

    headed = _HeadedHop(error=CninfoHeadedChromeConfigError("headless=true"))
    tls = _CallableHop(responses=[AssertionError("must not become chrome_tls")])
    session = _attach(
        headed_hop=headed,
        impersonated_request=tls,
        proxy_request=_CallableHop(),
    )
    with pytest.raises(CninfoHeadedChromeConfigError, match="headless"):
        session.get(DATA20_URL)
    assert tls.calls == []


def test_response_keeps_reason_and_access_mode():
    tls = _CallableHop(
        responses=[_Resp(status_code=403, text="blocked", reason="Forbidden")]
    )
    proxy = _CallableHop(
        responses=[_Resp({"ok": True}, reason="OK", url=DATA20_URL)]
    )
    session = _attach(
        preferred_mode="chrome_tls",
        headed_hop=None,
        impersonated_request=tls,
        proxy_request=proxy,
    )
    response = session.get(DATA20_URL)
    assert response.status_code == 200
    assert response.reason == "OK"
    assert response.access_mode == "proxy_patch"
    assert response.url == DATA20_URL
    assert response.content
    response.raise_for_status()
    assert response.headers.get("content-type") == "application/json"
    assert response.json()["ok"] is True


def test_factory_does_not_construct_headed_chrome():
    reset_cninfo_access_runtime()
    session = _attach(
        preferred_mode="chrome_tls",
        headed_hop=None,
        impersonated_request=_CallableHop(responses=[_Resp({})]),
        proxy_request=_CallableHop(),
    )
    assert session.__class__.__name__ == "CninfoAccessMux"
    assert session.__class__.__name__ != "CninfoHeadedChromeAccess"


def test_mux_does_not_read_supports_proxy_patch_or_allow_paid_proxy():
    headed = _HeadedHop(outcomes=[CninfoHeadedFetchOutcome("chrome_blocked")])
    proxy = _CallableHop(responses=[_Resp({"via": "proxy"})])
    session = attach_cninfo_access(
        environ={},
        sources={
            "cninfo": {
                "supports_proxy_patch": False,
                "access": {"preferred_mode": "headed_chrome"},
            }
        },
        headed_hop=headed,
        impersonated_request=_CallableHop(),
        proxy_request=proxy,
    )
    assert session.get(DATA20_URL).access_mode == "proxy_patch"
