from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
import requests

from utils import proxy_patch_runtime


class _FakeResponse:
    def __init__(self, payload=None, *, text=""):
        self._payload = payload or {}
        self.text = text

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_request_with_akshare_proxy_uses_authorized_exit(monkeypatch):
    target_calls = []

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            return _FakeResponse(
                {
                    "proxy": "http://proxy.example:8080",
                    "ua": "unit-test-agent",
                    "cookie": "session=unit-test",
                }
            )

        def request(self, method, url, **kwargs):
            target_calls.append((method, url, kwargs))
            return _FakeResponse({"ok": True}, text="official content")

    monkeypatch.setattr(
        proxy_patch_runtime,
        "_load_proxy_patch_config",
        lambda source: {
            "enabled": True,
            "gateway": "proxy-auth.example",
            "auth_token": "unit-test-token",
        },
    )
    monkeypatch.setattr(requests, "_OriginalSession", FakeSession, raising=False)

    response = proxy_patch_runtime.request_with_akshare_proxy(
        "GET",
        "https://official.example/data",
        accept_response=lambda item: item.text == "official content",
    )

    assert response.json() == {"ok": True}
    assert len(target_calls) == 1
    method, _, kwargs = target_calls[0]
    assert method == "GET"
    assert kwargs["headers"]["User-Agent"] == "unit-test-agent"
    assert kwargs["headers"]["Cookie"] == "session=unit-test"
    assert kwargs["proxies"]["https"] == "http://proxy.example:8080"


def test_request_with_akshare_proxy_stops_after_rejected_exits(monkeypatch):
    target_calls = []
    warning_logger = SimpleNamespace(warning=lambda *args, **kwargs: None)

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            return _FakeResponse(
                {
                    "proxy": "http://proxy.example:8080",
                    "ua": "unit-test-agent",
                }
            )

        def request(self, method, url, **kwargs):
            target_calls.append((method, url, kwargs))
            return _FakeResponse(text="challenge")

    monkeypatch.setattr(
        proxy_patch_runtime,
        "_load_proxy_patch_config",
        lambda source: {
            "enabled": True,
            "gateway": "proxy-auth.example",
            "auth_token": "unit-test-token",
        },
    )
    monkeypatch.setattr(requests, "_OriginalSession", FakeSession, raising=False)

    with pytest.raises(proxy_patch_runtime.ProxyResponseRejectedError):
        proxy_patch_runtime.request_with_akshare_proxy(
            "GET",
            "https://official.example/data",
            attempts=2,
            accept_response=lambda item: item.text != "challenge",
            warning_logger=warning_logger,
        )

    assert len(target_calls) == 2


def test_acquire_akshare_proxy_lease_is_fresh_and_redacts_credentials(monkeypatch):
    calls = []

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            calls.append((url, kwargs))
            return _FakeResponse(
                {
                    "proxy": "http://lease-user:lease-secret@proxy.example:8080",
                    "ua": "lease-agent",
                    "cookie": "private-cookie",
                }
            )

    monkeypatch.setattr(
        proxy_patch_runtime,
        "_load_proxy_patch_config",
        lambda source: {
            "enabled": True,
            "gateway": "proxy-auth.example",
            "auth_token": "unit-test-token",
        },
    )
    monkeypatch.setattr(requests, "_OriginalSession", FakeSession, raising=False)

    first = proxy_patch_runtime.acquire_akshare_proxy_lease()
    second = proxy_patch_runtime.acquire_akshare_proxy_lease()

    assert len(calls) == 2
    assert first.proxy_url == second.proxy_url
    assert first.endpoint == "proxy.example:8080"
    assert "lease-secret" not in repr(first)
    assert "private-cookie" not in repr(first)


def test_restore_unpatched_curl_cffi_session_allows_yfinance_chrome_tls(monkeypatch):
    import curl_cffi.requests as curl_requests
    import yfinance._http as yfinance_http
    from curl_cffi.requests.session import Session as RealChromeSession

    from proxy_patch_bootstrap import restore_unpatched_curl_cffi_session

    class PatchedSession(requests.Session):
        pass

    monkeypatch.setattr(curl_requests, "Session", PatchedSession)

    with pytest.raises(TypeError, match="impersonate"):
        curl_requests.Session(impersonate="chrome")
    with pytest.raises(TypeError, match="impersonate"):
        yfinance_http.new_session()

    restore_unpatched_curl_cffi_session()

    assert isinstance(curl_requests.Session(impersonate="chrome"), RealChromeSession)
    assert isinstance(yfinance_http.new_session(), RealChromeSession)


def test_akshare_patch_install_restores_curl_cffi_session_for_yfinance(monkeypatch):
    import curl_cffi.requests as curl_requests
    import yfinance._http as yfinance_http
    from curl_cffi.requests.session import Session as RealChromeSession

    import proxy_patch_bootstrap as bootstrap

    class PatchedSession(requests.Session):
        pass

    def fake_installer(*_args, **_kwargs):
        curl_requests.Session = PatchedSession

    monkeypatch.setattr(
        bootstrap,
        "_load_proxy_patch_config",
        lambda _name: {
            "enabled": True,
            "gateway": "127.0.0.1",
            "auth_token": "unit-test-token",
            "retry": 1,
            "hook_domains": ["fund.eastmoney.com"],
        },
    )
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "akshare_proxy_patch":
            return SimpleNamespace(install_patch=fake_installer)
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    state = bootstrap.ProxyPatchState(target="akshare")
    result = bootstrap._install_patch(
        state=state,
        config={
            "enabled": True,
            "gateway": "127.0.0.1",
            "auth_token": "unit-test-token",
            "retry": 1,
            "hook_domains": ["fund.eastmoney.com"],
        },
        defaults={},
        installer_name="install_patch",
        required=True,
    )

    assert result.ready is True
    assert curl_requests.Session is RealChromeSession
    assert isinstance(yfinance_http.new_session(), RealChromeSession)

    curl_requests.Session = PatchedSession
    reused = bootstrap._install_patch(
        state=result,
        config={
            "enabled": True,
            "gateway": "127.0.0.1",
            "auth_token": "unit-test-token",
            "retry": 1,
            "hook_domains": ["fund.eastmoney.com"],
        },
        defaults={},
        installer_name="install_patch",
        required=True,
    )
    assert reused.ready is True
    assert curl_requests.Session is RealChromeSession
    assert isinstance(yfinance_http.new_session(), RealChromeSession)
