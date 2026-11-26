"""行情依赖的受控升级规格与单包升级决策。

安装、冒烟和回滚由调用方注入，便于单测；脚本负责真正的 pip 与子进程。
一次任务可以连续升级多个包，每个包独立冒烟、独立回滚，互不影响 pin。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class DependencyUpgradeSpec:
    """一个可自动升级的发行版及其冒烟入口。"""

    name: str
    distribution: str
    smoke_script: str
    smoke_args: Tuple[str, ...] = ()


# curl_cffi 在代理补丁和 yfinance 之下；补丁在 akshare 之前安装。
UPGRADE_ORDER: Tuple[str, ...] = (
    "curl_cffi",
    "akshare_proxy_patch",
    "akshare",
    "pytdx",
    "baostock",
    "yfinance",
)

DEPENDENCY_UPGRADE_SPECS: Dict[str, DependencyUpgradeSpec] = {
    "curl_cffi": DependencyUpgradeSpec(
        "curl_cffi",
        "curl_cffi",
        "scripts/dependency_smoke.py",
        ("--package", "curl_cffi"),
    ),
    "akshare_proxy_patch": DependencyUpgradeSpec(
        "akshare_proxy_patch",
        "akshare-proxy-patch",
        "scripts/dependency_smoke.py",
        ("--package", "akshare_proxy_patch"),
    ),
    "akshare": DependencyUpgradeSpec(
        "akshare",
        "akshare",
        "scripts/akshare_smoke.py",
    ),
    "pytdx": DependencyUpgradeSpec(
        "pytdx",
        "pytdx",
        "scripts/dependency_smoke.py",
        ("--package", "pytdx"),
    ),
    "baostock": DependencyUpgradeSpec(
        "baostock",
        "baostock",
        "scripts/dependency_smoke.py",
        ("--package", "baostock"),
    ),
    "yfinance": DependencyUpgradeSpec(
        "yfinance",
        "yfinance",
        "scripts/dependency_smoke.py",
        ("--package", "yfinance"),
    ),
}


def parse_package_spec(text: str) -> Tuple[str, Optional[str]]:
    """解析 `name` 或 `name==version`。"""
    raw = str(text or "").strip()
    if "==" in raw:
        name, version = raw.split("==", 1)
        return name.strip(), version.strip() or None
    return raw, None


def ordered_upgrade_names(names: Sequence[str]) -> List[str]:
    """按传输层到数据源的固定顺序排列，未知名称排在后面并保持原序。"""
    rank = {name: index for index, name in enumerate(UPGRADE_ORDER)}
    return sorted(
        list(dict.fromkeys(names)),
        key=lambda name: (rank.get(name, len(UPGRADE_ORDER)), name),
    )


def smoke_passed(summary: Dict[str, Any]) -> bool:
    return summary.get("status") == "success" and not summary.get("failed")


def upgrade_one_package(
    *,
    name: str,
    distribution: str,
    previous_version: str,
    target_version: str,
    install: Callable[[str, bool], int],
    run_smoke: Callable[[], Dict[str, Any]],
    pin: Callable[[str], Dict[str, Any]],
) -> Dict[str, Any]:
    """安装目标版本，冒烟通过才 pin；失败则强制装回旧版本。

    install(version, reinstall) 返回进程退出码。reinstall=True 表示回滚。
    """
    base = {
        "name": name,
        "distribution": distribution,
        "previous_version": previous_version,
        "target_version": target_version,
        "changed_disk": False,
    }
    install_code = install(target_version, False)
    if install_code != 0:
        rollback_code = install(previous_version, True)
        return {
            **base,
            "outcome": "failed",
            "stage": "install",
            "changed_disk": True,
            "rollback_exit": rollback_code,
            "smoke": {
                "status": "install_failed",
                "passed": 0,
                "failed": 0,
                "cases": [],
            },
        }

    smoke = run_smoke()
    if smoke_passed(smoke):
        pinned = pin(target_version)
        return {
            **base,
            "outcome": "success",
            "stage": "pinned",
            "changed_disk": True,
            "smoke": smoke,
            "requirements": pinned,
        }

    rollback_code = install(previous_version, True)
    rollback_smoke = (
        run_smoke() if rollback_code == 0 else {"status": "rollback_failed", "cases": []}
    )
    return {
        **base,
        "outcome": "failed",
        "stage": "smoke",
        "changed_disk": True,
        "rollback_exit": rollback_code,
        "smoke": smoke,
        "rollback_smoke": rollback_smoke,
    }
