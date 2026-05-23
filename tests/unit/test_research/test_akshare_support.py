import pytest

from research.providers import akshare_support


def test_load_akshare_direct_skips_proxy_patch(monkeypatch):
    patch_calls = []
    import_calls = []

    monkeypatch.setattr(
        akshare_support,
        "_ensure_proxy_patch_installed",
        lambda: patch_calls.append("patch"),
    )
    monkeypatch.setattr(
        akshare_support,
        "_import_akshare",
        lambda *, reload_module: import_calls.append(reload_module) or "akshare-module",
    )

    result = akshare_support.load_akshare("direct")

    assert result == "akshare-module"
    assert patch_calls == []
    assert import_calls == [False]


def test_load_akshare_proxy_patch_requests_patch_install(monkeypatch):
    patch_calls = []
    import_calls = []

    monkeypatch.setattr(
        akshare_support,
        "_ensure_proxy_patch_installed",
        lambda: patch_calls.append("patch"),
    )
    monkeypatch.setattr(
        akshare_support,
        "_import_akshare",
        lambda *, reload_module: import_calls.append(reload_module) or "akshare-module",
    )

    result = akshare_support.load_akshare("proxy_patch")

    assert result == "akshare-module"
    assert patch_calls == ["patch"]
    assert import_calls == [True]


def test_proxy_patch_install_fails_explicitly_when_disabled(monkeypatch):
    monkeypatch.setattr(
        akshare_support,
        "install_akshare_proxy_patch",
        lambda *, required: (_ for _ in ()).throw(RuntimeError("disabled")),
    )

    with pytest.raises(RuntimeError, match="disabled"):
        akshare_support._ensure_proxy_patch_installed()
