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

    session = wrap_cninfo_proxy_fallback(inner, proxy_request=fake_proxy)
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

    session = wrap_cninfo_proxy_fallback(inner, proxy_request=fake_proxy)
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
