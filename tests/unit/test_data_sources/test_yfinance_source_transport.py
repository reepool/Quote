"""yfinance 数据源直连优先 + akshare_proxy_patch 网关回落的传输策略测试。"""

from datetime import datetime
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from data_sources.base_source import RateLimitConfig
from data_sources.yfinance_source import YFinanceSource


def _sample_frame(rows=1):
    index = pd.to_datetime([f"2026-04-{10 + i:02d}" for i in range(rows)])
    return pd.DataFrame(
        {
            "Open": [1.0] * rows,
            "High": [1.1] * rows,
            "Low": [0.9] * rows,
            "Close": [1.0] * rows,
            "Adj Close": [1.0] * rows,
            "Volume": [100] * rows,
        },
        index=index,
    )


def _empty_frame():
    return pd.DataFrame()


def _make_source(name="yfinance_hk_stock"):
    return YFinanceSource(name, RateLimitConfig())


@pytest.mark.unit
class TestYFinanceTransportFallback:
    @pytest.mark.asyncio
    async def test_default_path_downloads_directly_without_patch(self, monkeypatch):
        """默认直连：latch 关闭时直接 yf.download，不安装 patch、不做探测。"""
        source = _make_source()
        assert source.direct_rejected is False

        calls = []

        def fake_download(*args, **kwargs):
            calls.append(kwargs)
            return _sample_frame()

        def fail_ensure(*args, **kwargs):
            raise AssertionError("patch install must not run when direct download succeeds")

        monkeypatch.setattr("data_sources.yfinance_source.yf.download", fake_download)
        monkeypatch.setattr("data_sources.yfinance_source.install_yfinance_proxy_patch", fail_ensure)

        data = await source._fetch_yahoo_data_library(
            "0700.HK",
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 30),
        )

        assert data is not None and not data.empty
        assert len(calls) == 1
        assert calls[0]["progress"] is False
        assert calls[0]["threads"] is False
        assert calls[0]["auto_adjust"] is False
        assert "proxy" not in calls[0]
        assert source.direct_rejected is False

    @pytest.mark.asyncio
    async def test_direct_rejection_falls_back_to_gateway_and_latches(self, monkeypatch):
        """直连被拒：探测确认后安装 patch 走网关重试，成功即锁定网关模式。"""
        source = _make_source()

        results = [_empty_frame(), _sample_frame()]

        def fake_download(*args, **kwargs):
            return results.pop(0)

        ensure_calls = []

        def fake_ensure():
            ensure_calls.append(1)
            source.proxy_patch_ready = True
            return True

        monkeypatch.setattr("data_sources.yfinance_source.yf.download", fake_download)
        monkeypatch.setattr(
            source, "_probe_direct", AsyncMock(return_value=False)
        )
        monkeypatch.setattr(source, "_ensure_proxy_patch", fake_ensure)

        data = await source._fetch_yahoo_data_library(
            "0700.HK",
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 30),
        )

        assert data is not None and not data.empty
        assert len(ensure_calls) == 1
        assert source.direct_rejected is True

    @pytest.mark.asyncio
    async def test_empty_symbol_keeps_direct_default(self, monkeypatch):
        """直连可达但代码无数据：不得动用网关，也不锁定回落模式。"""
        source = _make_source()

        def fake_download(*args, **kwargs):
            return _empty_frame()

        def fail_ensure(*args, **kwargs):
            raise AssertionError("patch install must not run when direct probe passes")

        monkeypatch.setattr("data_sources.yfinance_source.yf.download", fake_download)
        monkeypatch.setattr(source, "_probe_direct", AsyncMock(return_value=True))
        monkeypatch.setattr("data_sources.yfinance_source.install_yfinance_proxy_patch", fail_ensure)

        data = await source._fetch_yahoo_data_library(
            "0700.HK",
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 30),
        )

        assert data is None
        assert source.direct_rejected is False

    @pytest.mark.asyncio
    async def test_latched_source_uses_gateway_without_direct_probe(self, monkeypatch):
        """已锁定网关模式：直接走 patch 后的 yf.download，不再探测直连。"""
        source = _make_source()
        source.direct_rejected = True

        calls = []

        def fake_download(*args, **kwargs):
            calls.append(kwargs)
            return _sample_frame()

        def fail_probe(*args, **kwargs):
            raise AssertionError("direct probe must not run in gateway mode")

        monkeypatch.setattr("data_sources.yfinance_source.yf.download", fake_download)
        monkeypatch.setattr(source, "_probe_direct", AsyncMock(side_effect=fail_probe))
        monkeypatch.setattr(
            source, "_ensure_proxy_patch", lambda: source.proxy_patch_ready or True
        )

        data = await source._fetch_yahoo_data_library(
            "0700.HK",
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 30),
        )

        assert data is not None and not data.empty
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_health_check_direct_success_restores_direct(self, monkeypatch):
        """直连恢复：清除回落锁定并卸载 patch，恢复默认直连。"""
        source = _make_source()
        source.direct_rejected = True

        uninstall_calls = []

        def fake_uninstall():
            uninstall_calls.append(1)

        monkeypatch.setattr(source, "_probe_direct", AsyncMock(return_value=True))
        monkeypatch.setattr(
            "data_sources.yfinance_source.uninstall_yfinance_proxy_patch", fake_uninstall
        )

        assert await source.health_check() is True
        assert source.direct_rejected is False
        assert len(uninstall_calls) == 1

    @pytest.mark.asyncio
    async def test_health_check_direct_rejection_uses_gateway(self, monkeypatch):
        """直连被拒：装 patch 后经网关复测，健康且保持回落锁定。"""
        source = _make_source()

        monkeypatch.setattr(source, "_probe_direct", AsyncMock(return_value=False))
        monkeypatch.setattr(source, "_ensure_proxy_patch", lambda: True)
        monkeypatch.setattr(source, "_probe_gateway", AsyncMock(return_value=True))

        assert await source.health_check() is True
        assert source.direct_rejected is True

    @pytest.mark.asyncio
    async def test_health_check_fails_when_both_transports_down(self, monkeypatch):
        """直连与网关均不可用：如实报告不健康。"""
        source = _make_source()

        monkeypatch.setattr(source, "_probe_direct", AsyncMock(return_value=False))
        monkeypatch.setattr(source, "_ensure_proxy_patch", lambda: False)

        assert await source.health_check() is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("patch_ready", [True, False])
    async def test_init_latches_gateway_only_when_fallback_usable(
        self, monkeypatch, patch_ready
    ):
        """初始化直连被拒：仅当 patch 可用时才锁定网关模式，否则保留直连。"""
        from utils.proxy_patch_runtime import ProxyPatchState

        source = _make_source()
        monkeypatch.setattr(source, "_probe_direct", AsyncMock(return_value=False))

        state = ProxyPatchState(
            target="yfinance",
            attempted=True,
            ready=patch_ready,
            error=None if patch_ready else "proxy patch is disabled",
        )
        monkeypatch.setattr(
            "data_sources.yfinance_source.install_yfinance_proxy_patch",
            lambda **kwargs: state,
        )

        await source._initialize_impl()
        try:
            assert source.direct_rejected is patch_ready
        finally:
            await source.close()


@pytest.mark.unit
class TestYFinanceHkSymbolFormat:
    def test_five_digit_code_normalizes_to_yahoo_four_digit(self):
        """Yahoo 港股代码为 4 位有效数字：00700 → 0700.HK。"""
        source = _make_source()
        assert source._build_yf_symbol("00700", "HKEX") == "0700.HK"
        assert source._build_yf_symbol("08059", "HK") == "8059.HK"

    def test_four_digit_code_unchanged(self):
        source = _make_source()
        assert source._build_yf_symbol("9988", "HKEX") == "9988.HK"

    def test_yahoo_formatted_symbol_passthrough(self):
        source = _make_source()
        assert source._build_yf_symbol("0700.HK", None) == "0700.HK"
