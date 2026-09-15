"""Unit tests for the CNInfo headed-Chrome access provider."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from research.providers.cninfo_headed_chrome import (
    CninfoHeadedChromeConfigError,
    CninfoHeadedChromeUrlError,
    _NodriverPageSession,
    create_cninfo_headed_chrome_access,
)


DATA20_URL = "https://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders"
ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
TOP_SEARCH_URL = "https://www.cninfo.com.cn/new/information/topSearch/query"
DISCLOSURE_URL = "https://www.cninfo.com.cn/new/disclosure/stock?stockCode=000001"
HOMEPAGE = "https://www.cninfo.com.cn/"


class _FakeResponse:
    def __init__(self, payload=None, *, status_code=200, text="", headers=None):
        self.payload = payload
        self.status_code = status_code
        self.text = text if text or payload is None else str(payload)
        self.content = self.text.encode("utf-8")
        self.headers = headers or {"Content-Type": "application/json"}

    def json(self):
        if self.payload is None:
            raise ValueError("not json")
        return self.payload


class _FakePageSession:
    def __init__(
        self,
        *,
        bootstrap=None,
        fetches=None,
        start_error=None,
    ):
        self.started = 0
        self.closed = 0
        self.navigations = []
        self.in_page_calls = []
        self.start_error = start_error
        self.bootstrap = bootstrap or {
            "status": 200,
            "title": "巨潮资讯网",
            "text": "<html><title>巨潮资讯网</title></html>",
            "url": HOMEPAGE,
        }
        self.fetches = list(fetches or [])

    def start(self, homepage: str):
        if self.start_error is not None:
            raise self.start_error
        self.started += 1
        self.navigations.append(homepage)
        return dict(self.bootstrap)

    def fetch(self, method, url, **kwargs):
        self.in_page_calls.append({"method": method, "url": url, **kwargs})
        if not self.fetches:
            raise AssertionError("unexpected in-page fetch")
        result = self.fetches.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed += 1


class _FakeDisplay:
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True
        return self

    def stop(self):
        self.stopped = True


def _json_fetch(payload, *, status=200, url=DATA20_URL):
    import json

    text = json.dumps(payload, ensure_ascii=False)
    return {
        "status": status,
        "url": url,
        "headers": {"content-type": "application/json"},
        "text": text,
        "body": text.encode("utf-8"),
    }


def test_headless_true_is_hard_error_and_does_not_fall_back_to_proxy():
    proxy_calls = []

    with pytest.raises(CninfoHeadedChromeConfigError, match="headless"):
        create_cninfo_headed_chrome_access(
            headless=True,
            proxy_request=lambda *args, **kwargs: proxy_calls.append(True),
            page_session=_FakePageSession(),
        )

    assert proxy_calls == []


@pytest.mark.parametrize(
    "url",
    [
        "http://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders",
        "https://webapi.cninfo.com.cn/api/stock/p_stock2215",
        "https://static.cninfo.com.cn/finalpage/report.pdf",
        "https://query.sse.com.cn/commonQuery.do",
        "https://www.cninfo.com.cn/foo",
        "https://www.cninfo.com.cn/new/js/app.js",
    ],
)
def test_rejects_disallowed_hosts_and_paths_before_chrome(url):
    page = _FakePageSession(fetches=[AssertionError("Chrome must not start")])
    access = create_cninfo_headed_chrome_access(page_session=page)

    with pytest.raises(CninfoHeadedChromeUrlError):
        access.get(url)

    assert page.started == 0
    assert page.navigations == []


def test_homepage_slash_is_not_a_prefix_allow():
    page = _FakePageSession()
    access = create_cninfo_headed_chrome_access(page_session=page)

    with pytest.raises(CninfoHeadedChromeUrlError):
        access.get("https://www.cninfo.com.cn/anything")

    assert page.started == 0


def test_allows_www_data20_announcement_top_search_and_disclosure(monkeypatch):
    page = _FakePageSession(
        fetches=[
            _json_fetch({"code": 200}, url=DATA20_URL),
            _json_fetch({"announcements": []}, url=ANNOUNCEMENT_URL),
            _json_fetch([], url=TOP_SEARCH_URL),
            {
                "status": 200,
                "url": DISCLOSURE_URL,
                "headers": {"content-type": "text/html"},
                "text": "<html>disclosure</html>",
                "body": b"<html>disclosure</html>",
            },
        ]
    )
    access = create_cninfo_headed_chrome_access(page_session=page)

    assert access.get(DATA20_URL).json()["code"] == 200
    assert access.post(ANNOUNCEMENT_URL, data={"pageNum": "1"}).json()["announcements"] == []
    assert access.post(TOP_SEARCH_URL, data={"keyWord": "000001"}).json() == []
    disclosure = access.get(DISCLOSURE_URL)
    assert disclosure.status_code == 200
    assert "disclosure" in disclosure.text
    assert page.started == 1
    assert page.navigations == [HOMEPAGE]
    assert all(call["url"] != HOMEPAGE or call["method"] != "NAVIGATE" for call in page.in_page_calls)
    assert DATA20_URL not in page.navigations


def test_instances_do_not_share_browser_or_close():
    first_page = _FakePageSession(fetches=[_json_fetch({"n": 1})])
    second_page = _FakePageSession(fetches=[_json_fetch({"n": 2})])
    first = create_cninfo_headed_chrome_access(page_session=first_page)
    second = create_cninfo_headed_chrome_access(page_session=second_page)

    first.get(DATA20_URL)
    second.get(DATA20_URL)
    first.close()

    assert first_page.closed == 1
    assert second_page.closed == 0
    assert second_page.started == 1


def test_later_requests_reuse_one_browser_without_per_url_navigation():
    page = _FakePageSession(
        fetches=[
            _json_fetch({"page": 1}),
            _json_fetch({"page": 2}),
        ]
    )
    access = create_cninfo_headed_chrome_access(page_session=page)

    first = access.get(DATA20_URL, params={"scode": "000001"})
    second = access.get(
        "https://www.cninfo.com.cn/data20/stockholderCapital/getStockholderNum",
        params={"scode": "000001"},
    )

    assert first.json()["page"] == 1
    assert second.json()["page"] == 2
    assert page.started == 1
    assert page.navigations == [HOMEPAGE]
    assert [call["method"] for call in page.in_page_calls] == ["GET", "GET"]


def test_forwards_params_form_data_json_headers_timeout_and_credentials():
    page = _FakePageSession(
        fetches=[
            _json_fetch({"ok": True}, url=ANNOUNCEMENT_URL),
            _json_fetch({"ok": True}, url=DATA20_URL),
        ]
    )
    access = create_cninfo_headed_chrome_access(page_session=page)

    access.post(
        ANNOUNCEMENT_URL,
        data={"pageNum": "1", "pageSize": "1"},
        headers={"Origin": "https://www.cninfo.com.cn"},
        timeout=15,
    )
    access.get(
        DATA20_URL,
        params={"scode": "600000"},
        json=None,
        headers={"Accept": "application/json"},
        timeout=9,
    )

    form_call, json_call = page.in_page_calls
    assert form_call["data"] == {"pageNum": "1", "pageSize": "1"}
    assert form_call["content_type"] == "application/x-www-form-urlencoded"
    assert form_call["credentials"] == "include"
    assert form_call["headers"]["Origin"] == "https://www.cninfo.com.cn"
    assert form_call["timeout"] == 15
    assert json_call["params"] == {"scode": "600000"}
    assert json_call["credentials"] == "include"


def test_json_body_keeps_application_json():
    page = _FakePageSession(fetches=[_json_fetch({"ok": True}, url=DATA20_URL)])
    access = create_cninfo_headed_chrome_access(page_session=page)

    access.request("POST", DATA20_URL, json={"scode": "000001"}, timeout=5)

    assert page.in_page_calls[0]["json"] == {"scode": "000001"}
    assert page.in_page_calls[0]["content_type"] == "application/json"


def test_http_403_falls_back_to_proxy_without_requests_tls_and_stays():
    page = _FakePageSession(
        fetches=[
            {
                "status": 403,
                "url": DATA20_URL,
                "headers": {},
                "text": "<title>403 Forbidden</title>",
                "body": b"<title>403 Forbidden</title>",
            },
            AssertionError("headed Chrome must not be probed again after successful proxy"),
        ]
    )
    proxy_calls = []
    proxy_payloads = [
        _FakeResponse({"path": "first"}),
        _FakeResponse({"path": "second"}),
    ]

    def fake_proxy(method, url, **kwargs):
        proxy_calls.append({"method": method, "url": url, "params": kwargs.get("params")})
        assert kwargs.get("accept_response") is not None
        return proxy_payloads.pop(0)

    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=fake_proxy,
    )
    first = access.get(DATA20_URL, params={"scode": "000001"})
    second = access.get(DATA20_URL, params={"scode": "000002"})

    assert first.access_mode == "proxy_patch"
    assert first.json()["path"] == "first"
    assert second.access_mode == "proxy_patch"
    assert second.json()["path"] == "second"
    assert len(page.in_page_calls) == 1
    assert [call["url"] for call in proxy_calls] == [DATA20_URL, DATA20_URL]


def test_null_evaluate_is_chrome_failure_not_empty_success():
    page = _FakePageSession(fetches=[None])
    proxy_response = _FakeResponse({"code": 200, "data": {}})

    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: proxy_response,
    )
    response = access.get(DATA20_URL)

    assert response.access_mode == "proxy_patch"
    assert response.json()["code"] == 200
    assert response.text != ""


def test_chrome_exception_falls_back_to_proxy():
    page = _FakePageSession(fetches=[TimeoutError("evaluate timed out")])
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: _FakeResponse({"recovered": True}),
    )

    response = access.get(DATA20_URL)

    assert response.access_mode == "proxy_patch"
    assert response.json()["recovered"] is True


def test_chrome_start_failure_falls_back_to_proxy_for_instance():
    page = _FakePageSession(start_error=RuntimeError("xvfb missing"))
    proxy_calls = []

    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: proxy_calls.append(args) or _FakeResponse({"ok": 1}),
    )
    first = access.get(DATA20_URL)
    second = access.get(DATA20_URL)

    assert first.access_mode == "proxy_patch"
    assert second.access_mode == "proxy_patch"
    assert page.started == 0
    assert len(proxy_calls) == 2


def test_bootstrap_block_page_restarts_once_then_prefers_proxy():
    page = _FakePageSession(
        bootstrap={
            "status": 403,
            "title": "403 Forbidden",
            "text": "<html>403 Forbidden</html>",
            "url": HOMEPAGE,
        },
        fetches=[AssertionError("must not fetch APIs from a block page")],
    )
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: _FakeResponse({"via": "proxy"}),
    )

    response = access.get(DATA20_URL)
    second = access.get(DATA20_URL)

    assert response.access_mode == "proxy_patch"
    assert second.access_mode == "proxy_patch"
    assert page.started == 2
    assert page.in_page_calls == []


def test_proxy_accepts_disclosure_html_instead_of_json_only_gate():
    page = _FakePageSession(
        fetches=[
            {
                "status": 403,
                "url": DISCLOSURE_URL,
                "headers": {},
                "text": "403 Forbidden",
                "body": b"403 Forbidden",
            }
        ]
    )
    html = _FakeResponse(
        payload=None,
        text="<!doctype html><html>stock</html>",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )

    def fake_proxy(method, url, **kwargs):
        accept = kwargs["accept_response"]
        assert accept(html) is True
        assert accept(
            _FakeResponse(
                payload=None,
                text="<!doctype html><title>disclosure</title>",
                headers={"Content-Type": "text/html"},
            )
        )
        return html

    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=fake_proxy,
    )
    response = access.get(DISCLOSURE_URL)

    assert response.access_mode == "proxy_patch"
    assert "stock" in response.text


def test_response_exposes_session_fields_and_access_mode():
    page = _FakePageSession(fetches=[_json_fetch({"code": 200}, url=DATA20_URL)])
    access = create_cninfo_headed_chrome_access(page_session=page)

    response = access.get(DATA20_URL)

    assert response.status_code == 200
    assert response.url == DATA20_URL
    assert response.access_mode == "headed_chrome"
    assert response.reason == "OK"
    assert response.json()["code"] == 200
    assert response.content
    response.raise_for_status()


def test_fetch_allowlisted_reports_blocked_without_calling_proxy():
    page = _FakePageSession(
        fetches=[
            {
                "status": 403,
                "url": DATA20_URL,
                "headers": {},
                "text": "<title>403 Forbidden</title>",
                "body": b"<title>403 Forbidden</title>",
            }
        ]
    )
    proxy_calls = []
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: proxy_calls.append(True),
    )

    outcome = access.fetch_allowlisted("GET", DATA20_URL)

    assert outcome.status == "chrome_blocked"
    assert outcome.response is not None
    assert outcome.response.status_code == 403
    assert outcome.response.reason == "Forbidden"
    assert proxy_calls == []


def test_fetch_allowlisted_reports_unavailable_on_start_failure():
    page = _FakePageSession(start_error=RuntimeError("xvfb missing"))
    proxy_calls = []
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: proxy_calls.append(True),
    )

    outcome = access.fetch_allowlisted("GET", DATA20_URL)

    assert outcome.status == "chrome_unavailable"
    assert outcome.response is None
    assert proxy_calls == []


def test_fetch_allowlisted_treats_logical_429_as_success():
    page = _FakePageSession(
        fetches=[
            _json_fetch(
                {"data": {"resultCode": 429, "resultMsg": "too many requests"}},
                url=DATA20_URL,
            )
        ]
    )
    proxy_calls = []
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        proxy_request=lambda *args, **kwargs: proxy_calls.append(True),
    )

    outcome = access.fetch_allowlisted("GET", DATA20_URL)

    assert outcome.status == "success"
    assert outcome.response.access_mode == "headed_chrome"
    assert outcome.response.json()["data"]["resultCode"] == 429
    assert proxy_calls == []


def test_reuses_existing_display_and_close_stops_only_started_xvfb():
    existing_display = _FakeDisplay()
    created = []

    def factory():
        display = _FakeDisplay()
        created.append(display)
        return display

    page = _FakePageSession(fetches=[_json_fetch({"ok": True})])
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        environ={"DISPLAY": ":99"},
        display_factory=factory,
    )
    access.get(DATA20_URL)
    access.close()

    assert created == []
    assert existing_display.stopped is False
    assert page.closed == 1


def test_starts_xvfb_only_when_display_unset_and_stops_it_on_close():
    created = []

    def factory():
        display = _FakeDisplay()
        created.append(display)
        return display

    page = _FakePageSession(fetches=[_json_fetch({"ok": True})])
    access = create_cninfo_headed_chrome_access(
        page_session=page,
        environ={},
        display_factory=factory,
    )
    access.get(DATA20_URL)
    assert created[0].started is True
    access.close()
    assert created[0].stopped is True


def test_nodriver_close_accepts_sync_stop():
    session = _NodriverPageSession()
    session._loop = asyncio.new_event_loop()

    class _Browser:
        def stop(self):
            return None

    session._browser = _Browser()
    session.close()


def test_attach_cninfo_access_does_not_construct_headed_chrome_at_factory_time():
    from research.providers.cninfo_http import attach_cninfo_access
    from research.providers import cninfo_http
    from research.providers import cninfo_announcements
    from research.providers import cninfo_shareholders
    from research.providers import official_financial_filings

    session = attach_cninfo_access(
        preferred_mode="chrome_tls",
        headed_hop=None,
        impersonated_request=lambda *args, **kwargs: None,
        environ={},
        sources={"cninfo": {"access": {"preferred_mode": "chrome_tls"}}},
    )
    assert session.__class__.__name__ == "CninfoAccessMux"
    assert session.__class__.__name__ != "CninfoHeadedChromeAccess"
    assert "cninfo_headed_chrome" in inspect.getsource(cninfo_http)
    assert "cninfo_headed_chrome" not in inspect.getsource(cninfo_announcements)
    assert "cninfo_headed_chrome" not in inspect.getsource(cninfo_shareholders)
    assert "cninfo_headed_chrome" not in inspect.getsource(official_financial_filings)
    assert "probe_cninfo_headed_chrome_access" not in inspect.getsource(cninfo_announcements)
    assert "probe_cninfo_headed_chrome_access" not in inspect.getsource(cninfo_shareholders)
    assert "probe_cninfo_headed_chrome_access" not in inspect.getsource(official_financial_filings)
