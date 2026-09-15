from research.providers.cninfo_http import wrap_cninfo_proxy_fallback
from research.providers.cninfo_shareholders import CninfoShareholdersProvider


class _FakeResponse:
    def __init__(self, payload=None, *, status_code=200, text=""):
        self.payload = payload
        self.status_code = status_code
        self.text = text or ("" if payload is None else str(payload))
        self.headers = {"Content-Type": "application/json"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if self.payload is None:
            raise ValueError("not json")
        return self.payload


class _QueuedSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return self.responses.pop(0)

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)


def test_cninfo_wrapper_falls_back_to_proxy_on_http_403():
    inner = _QueuedSession([_FakeResponse(status_code=403, text="blocked")])
    proxy_calls = []
    proxy_response = _FakeResponse({"code": 200, "data": {"records": [{"SECCODE": "600000"}]}})

    def fake_proxy(method, url, **kwargs):
        proxy_calls.append({"method": method, "url": url, "params": kwargs.get("params")})
        return proxy_response

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=None,
        proxy_request=fake_proxy,
    )
    response = session.get(
        "https://www.cninfo.com.cn/data20/companyOverview/getCompanyInfo",
        params={"scode": "600000"},
    )

    assert response.json()["data"]["records"][0]["SECCODE"] == "600000"
    assert len(inner.calls) == 1
    assert proxy_calls == [
        {
            "method": "GET",
            "url": "https://www.cninfo.com.cn/data20/companyOverview/getCompanyInfo",
            "params": {"scode": "600000"},
        }
    ]


def test_cninfo_wrapper_stays_on_proxy_after_direct_403():
    inner = _QueuedSession(
        [
            _FakeResponse(status_code=403, text="blocked"),
            AssertionError("direct CNInfo must not be retried after proxy recovery"),
        ]
    )
    proxy_responses = [
        _FakeResponse({"path": "/first"}),
        _FakeResponse({"path": "/second"}),
    ]

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=None,
        proxy_request=lambda method, url, **kwargs: proxy_responses.pop(0),
    )

    first = session.get("https://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders")
    second = session.get("https://www.cninfo.com.cn/data20/stockholderCapital/getStockholderNum")

    assert first.json()["path"] == "/first"
    assert second.json()["path"] == "/second"
    assert len(inner.calls) == 1


def test_cninfo_wrapper_does_not_proxy_non_cninfo_hosts():
    blocked = _FakeResponse(status_code=403, text="sse blocked")
    inner = _QueuedSession([blocked])
    proxy_calls = []

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=None,
        proxy_request=lambda *args, **kwargs: proxy_calls.append(True) or _FakeResponse({}),
    )
    response = session.get("https://query.sse.com.cn/commonQuery.do")

    assert response is blocked
    assert proxy_calls == []


def test_cninfo_wrapper_keeps_403_when_proxy_unavailable():
    blocked = _FakeResponse(status_code=403, text="blocked")
    inner = _QueuedSession([blocked])

    def fake_proxy(*_args, **_kwargs):
        raise RuntimeError("akshare proxy fallback is not fully configured")

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=None,
        proxy_request=fake_proxy,
    )
    response = session.get(
        "https://www.cninfo.com.cn/data20/financialData/getIncomeStatement"
    )

    assert response is blocked
    assert response.status_code == 403


def test_cninfo_shareholders_data20_uses_proxy_on_http_403():
    inner = _QueuedSession(
        [_FakeResponse(status_code=403, text="<!doctype html><title>403 Forbidden</title>")]
    )
    proxy_response = _FakeResponse(
        {
            "code": 200,
            "data": {
                "records": [
                    {
                        "F001D": "2025-09-30",
                        "F002V": "测试股东",
                        "F003N": 100.0,
                        "F004N": 10.0,
                    }
                ]
            },
        }
    )
    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=None,
        proxy_request=lambda *args, **kwargs: proxy_response,
    )
    provider = CninfoShareholdersProvider(request_interval_seconds=0)

    records, payload = provider._request_data20_records_once(
        session,
        "getTopTenStockholders",
        "600000",
    )

    assert records[0]["F002V"] == "测试股东"
    assert payload["code"] == 200
    assert len(inner.calls) == 1


def test_cninfo_wrapper_uses_chrome_tls_before_requests_or_proxy():
    inner = _QueuedSession([AssertionError("Python requests TLS must not hit CNInfo")])
    impersonated = _QueuedSession(
        [_FakeResponse({"code": 200, "data": {"records": [{"SECCODE": "600000"}]}})]
    )
    proxy_calls = []

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=impersonated.request,
        proxy_request=lambda *args, **kwargs: proxy_calls.append(True) or _FakeResponse({}),
    )
    response = session.get(
        "https://www.cninfo.com.cn/data20/companyOverview/getCompanyInfo",
        params={"scode": "600000"},
    )

    assert response.json()["data"]["records"][0]["SECCODE"] == "600000"
    assert impersonated.calls == [
        {
            "method": "GET",
            "url": "https://www.cninfo.com.cn/data20/companyOverview/getCompanyInfo",
            "params": {"scode": "600000"},
        }
    ]
    assert inner.calls == []
    assert proxy_calls == []


def test_cninfo_wrapper_does_not_retry_requests_tls_after_chrome_tls_403():
    inner = _QueuedSession(
        [AssertionError("requests TLS after a 403 poisons the same egress IP")]
    )
    impersonated = _QueuedSession([_FakeResponse(status_code=403, text="blocked")])
    proxy_response = _FakeResponse({"code": 200, "data": {"records": [{"SECCODE": "600000"}]}})

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=impersonated.request,
        proxy_request=lambda *args, **kwargs: proxy_response,
    )
    response = session.get(
        "https://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders"
    )

    assert response.json()["data"]["records"][0]["SECCODE"] == "600000"
    assert len(impersonated.calls) == 1
    assert inner.calls == []
    assert session._prefer_proxy is True


def test_cninfo_wrapper_falls_back_to_proxy_when_chrome_tls_raises_typeerror():
    inner = _QueuedSession(
        [AssertionError("requests TLS must not run after chrome TLS setup failure")]
    )
    proxy_response = _FakeResponse({"code": 200, "data": {"records": [{"SECCODE": "600000"}]}})

    def broken_chrome_tls(*_args, **_kwargs):
        raise TypeError("Session.__init__() got an unexpected keyword argument 'impersonate'")

    session = wrap_cninfo_proxy_fallback(
        inner,
        impersonated_request=broken_chrome_tls,
        proxy_request=lambda *args, **kwargs: proxy_response,
    )
    response = session.get(
        "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    )

    assert response.json()["data"]["records"][0]["SECCODE"] == "600000"
    assert inner.calls == []
    assert session._prefer_proxy is True


def test_cninfo_chrome_tls_session_survives_akshare_proxy_session_replacement(monkeypatch):
    import curl_cffi.requests as curl_requests
    from curl_cffi.requests.session import Session as RealChromeSession

    from research.providers import cninfo_http

    class PatchedSession:
        def __init__(self, *args, **kwargs):
            if "impersonate" in kwargs:
                raise TypeError(
                    "Session.__init__() got an unexpected keyword argument 'impersonate'"
                )

    monkeypatch.setattr(curl_requests, "Session", PatchedSession)
    cninfo_http._THREAD_LOCAL.session = None
    try:
        session = cninfo_http._impersonated_session()
        assert isinstance(session, RealChromeSession)
        assert not isinstance(session, PatchedSession)
    finally:
        cninfo_http._THREAD_LOCAL.session = None
