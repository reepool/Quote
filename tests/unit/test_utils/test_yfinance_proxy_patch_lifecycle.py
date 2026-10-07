"""utils.proxy_patch_runtime yfinance patch 安装/卸载生命周期测试。"""

from curl_cffi import requests as curl_requests

from utils.proxy_patch_runtime import (
    get_yfinance_proxy_patch_state,
    install_yfinance_proxy_patch,
    uninstall_yfinance_proxy_patch,
)


def test_uninstall_restores_curl_cffi_transport():
    """卸载必须真正还原 curl_cffi 传输层，而不是只重置状态标志。

    akshare_proxy_patch 的 uninstall_yfinance_patch_main 定义在子模块中且未
    导出到顶层包，顶层查找会静默跳过卸载，导致“已恢复直连”时网关 hook 仍生效。
    """
    original_request = curl_requests.Session.request
    try:
        state = install_yfinance_proxy_patch(required=False, force=True)
        if not state.ready:
            # 环境未配置网关时跳过（本仓库默认配置应可用）
            assert state.error, "patch install failed without error info"
            return

        assert curl_requests.Session.request is not original_request

        uninstall_yfinance_proxy_patch()

        assert curl_requests.Session.request is original_request
        assert get_yfinance_proxy_patch_state()["ready"] is False
    finally:
        curl_requests.Session.request = original_request
